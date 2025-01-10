/*-------------------------------------------------------------------------
 *
 * pagestore_smgr.c
 *
 *
 *
 * Temporary and unlogged rels
 * ---------------------------
 *
 * Temporary and unlogged tables are stored locally, by md.c. The functions
 * here just pass the calls through to corresponding md.c functions.
 *
 * Index build operations that use the buffer cache are also handled locally,
 * just like unlogged tables. Such operations must be marked by calling
 * smgr_start_unlogged_build() and friends.
 *
 * In order to know what relations are permanent and which ones are not, we
 * have added a 'smgr_relpersistence' field to SmgrRelationData, and it is set
 * by smgropen() callers, when they have the relcache entry at hand.  However,
 * sometimes we need to open an SmgrRelation for a relation without the
 * relcache. That is needed when we evict a buffer; we might not have the
 * SmgrRelation for that relation open yet. To deal with that, the
 * 'relpersistence' can be left to zero, meaning we don't know if it's
 * permanent or not. Most operations are not allowed with relpersistence==0,
 * but smgrwrite() does work, which is what we need for buffer eviction.  and
 * smgrunlink() so that a backend doesn't need to have the relcache entry at
 * transaction commit, where relations that were dropped in the transaction
 * are unlinked.
 *
 * If smgrwrite() is called and smgr_relpersistence == 0, we check if the
 * relation file exists locally or not. If it does exist, we assume it's an
 * unlogged relation and write the page there. Otherwise it must be a
 * permanent relation, WAL-logged and stored on the page server, and we ignore
 * the write like we do for permanent relations.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  contrib/neon/pagestore_smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "common/hashfn.h"
#include "executor/instrument.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/interrupt.h"
#include "port/pg_iovec.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/fsm_internals.h"
#include "storage/md.h"
#include "storage/smgr.h"

#include "neon_perf_counters.h"
#include "pagestore_client.h"
#include "bitmap.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif

/*
 * If DEBUG_COMPARE_LOCAL is defined, we pass through all the SMGR API
 * calls to md.c, and *also* do the calls to the Page Server. On every
 * read, compare the versions we read from local disk and Page Server,
 * and Assert that they are identical.
 */
/* #define DEBUG_COMPARE_LOCAL */

#ifdef DEBUG_COMPARE_LOCAL
#include "access/nbtree.h"
#include "storage/bufpage.h"
#include "access/xlog_internal.h"

static char *hexdump_page(char *page);
#endif

#define IS_LOCAL_REL(reln) (\
	NInfoGetDbOid(InfoFromSMgrRel(reln)) != 0 && \
		NInfoGetRelNumber(InfoFromSMgrRel(reln)) > FirstNormalObjectId \
)

const int	SmgrTrace = DEBUG5;

#define NEON_PANIC_CONNECTION_STATE(shard_no, elvl, message, ...) \
	neon_shard_log(shard_no, elvl, "Broken connection state: " message, \
				   ##__VA_ARGS__)

page_server_api *page_server;

/* unlogged relation build states */
typedef enum
{
	UNLOGGED_BUILD_NOT_IN_PROGRESS = 0,
	UNLOGGED_BUILD_PHASE_1,
	UNLOGGED_BUILD_PHASE_2,
	UNLOGGED_BUILD_NOT_PERMANENT
} UnloggedBuildPhase;

static SMgrRelation unlogged_build_rel = NULL;
static UnloggedBuildPhase unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;

static bool neon_redo_read_buffer_filter(XLogReaderState *record, uint8 block_id);
static bool (*old_redo_read_buffer_filter) (XLogReaderState *record, uint8 block_id) = NULL;

static BlockNumber neon_nblocks(SMgrRelation reln, ForkNumber forknum);

static uint32 local_request_counter;
#define GENERATE_REQUEST_ID() (((NeonRequestId)MyProcPid << 32) | ++local_request_counter)

/*
 * Prefetch implementation:
 *
 * Prefetch is performed locally by each backend.
 *
 * There can be up to readahead_buffer_size active IO requests registered at
 * any time. Requests using smgr_prefetch are sent to the pageserver, but we
 * don't wait on the response. Requests using smgr_read are either read from
 * the buffer, or (if that's not possible) we wait on the response to arrive -
 * this also will allow us to receive other prefetched pages.
 * Each request is immediately written to the output buffer of the pageserver
 * connection, but may not be flushed if smgr_prefetch is used: pageserver
 * flushes sent requests on manual flush, or every neon.flush_output_after
 * unflushed requests; which is not necessarily always and all the time.
 *
 * Once we have received a response, this value will be stored in the response
 * buffer, indexed in a hash table. This allows us to retain our buffered
 * prefetch responses even when we have cache misses.
 *
 * Reading of prefetch responses is delayed until them are actually needed
 * (smgr_read). In case of prefetch miss or any other SMGR request other than
 * smgr_read, all prefetch responses in the pipeline will need to be read from
 * the connection; the responses are stored for later use.
 *
 * NOTE: The current implementation of the prefetch system implements a ring
 * buffer of up to readahead_buffer_size requests. If there are more _read and
 * _prefetch requests between the initial _prefetch and the _read of a buffer,
 * the prefetch request will have been dropped from this prefetch buffer, and
 * your prefetch was wasted.
 */

/*
 * State machine:
 *
 * not in hash : in hash
 *             :
 * UNUSED ------> REQUESTED --> RECEIVED
 *   ^         :      |            |
 *   |         :      v            |
 *   |         : TAG_UNUSED        |
 *   |         :      |            |
 *   +----------------+------------+
 *             :
 */
typedef enum PrefetchStatus
{
	PRFS_UNUSED = 0,			/* unused slot */
	PRFS_REQUESTED,				/* request was written to the sendbuffer to
								 * PS, but not necessarily flushed. all fields
								 * except response valid */
	PRFS_RECEIVED,				/* all fields valid */
	PRFS_TAG_REMAINS,			/* only buftag and my_ring_index are still
								 * valid */
} PrefetchStatus;

/* must fit in uint8; bits 0x1 are used */
typedef enum {
	PRFSF_NONE	= 0x0,
	PRFSF_SEQ	= 0x1,
} PrefetchRequestFlags;

typedef struct PrefetchRequest
{
	BufferTag	buftag;			/* must be first entry in the struct */
	shardno_t	shard_no;
	uint8		status;		/* see PrefetchStatus for valid values */
	uint8		flags;		/* see PrefetchRequestFlags */
	neon_request_lsns request_lsns;
	NeonRequestId reqid;
	NeonResponse *response;		/* may be null */
	uint64		my_ring_index;
} PrefetchRequest;

/* prefetch buffer lookup hash table */

typedef struct PrfHashEntry
{
	PrefetchRequest *slot;
	uint32		status;
	uint32		hash;
} PrfHashEntry;

#define SH_PREFIX			prfh
#define SH_ELEMENT_TYPE		PrfHashEntry
#define SH_KEY_TYPE			PrefetchRequest *
#define SH_KEY				slot
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a)	((a)->hash)
#define SH_HASH_KEY(tb, key) hash_bytes( \
	((const unsigned char *) &(key)->buftag), \
	sizeof(BufferTag) \
)

#define SH_EQUAL(tb, a, b)	(BufferTagsEqual(&(a)->buftag, &(b)->buftag))
#define SH_SCOPE			static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"
#include "neon.h"

/*
 * PrefetchState maintains the state of (prefetch) getPage@LSN requests.
 * It maintains a (ring) buffer of in-flight requests and responses.
 *
 * We maintain several indexes into the ring buffer:
 * ring_unused >= ring_flush >= ring_receive >= ring_last >= 0
 *
 * ring_unused points to the first unused slot of the buffer
 * ring_receive is the next request that is to be received
 * ring_last is the oldest received entry in the buffer
 *
 * Apart from being an entry in the ring buffer of prefetch requests, each
 * PrefetchRequest that is not UNUSED is indexed in prf_hash by buftag.
 */
typedef struct PrefetchState
{
	MemoryContext bufctx;		/* context for prf_buffer[].response
								 * allocations */
	MemoryContext errctx;		/* context for prf_buffer[].response
								 * allocations */
	MemoryContext hashctx;		/* context for prf_buffer */

	/* buffer indexes */
	uint64		ring_unused;	/* first unused slot */
	uint64		ring_flush;		/* next request to flush */
	uint64		ring_receive;	/* next slot that is to receive a response */
	uint64		ring_last;		/* min slot with a response value */

	/* metrics / statistics  */
	int			n_responses_buffered;	/* count of PS responses not yet in
										 * buffers */
	int			n_requests_inflight;	/* count of PS requests considered in
										 * flight */
	int			n_unused;		/* count of buffers < unused, > last, that are
								 * also unused */

	/* the buffers */
	prfh_hash	*prf_hash;
	int			max_shard_no;
	/* Mark shards involved in prefetch */
	uint8		shard_bitmap[(MAX_SHARDS + 7)/8];
	PrefetchRequest prf_buffer[];	/* prefetch buffers */
} PrefetchState;

static PrefetchState *MyPState;

#define GetPrfSlotNoCheck(ring_index) ( \
	&MyPState->prf_buffer[((ring_index) % readahead_buffer_size)] \
)

#define GetPrfSlot(ring_index) ( \
	( \
		AssertMacro((ring_index) < MyPState->ring_unused && \
					(ring_index) >= MyPState->ring_last), \
		GetPrfSlotNoCheck(ring_index) \
	) \
)

#define ReceiveBufferNeedsCompaction() (\
	(MyPState->n_responses_buffered / 8) < ( \
		MyPState->ring_receive - \
			MyPState->ring_last - \
			MyPState->n_responses_buffered \
	) \
)

static bool compact_prefetch_buffers(void);
static void consume_prefetch_responses(void);
static bool prefetch_read(PrefetchRequest *slot);
static void prefetch_do_request(PrefetchRequest *slot, neon_request_lsns *force_request_lsns);
static bool prefetch_wait_for(uint64 ring_index);
static void prefetch_cleanup_trailing_unused(void);
static inline void prefetch_set_unused(uint64 ring_index);
#if PG_MAJORVERSION_NUM < 17
static void
GetLastWrittenLSNv(NRelFileInfo relfilenode, ForkNumber forknum,
				   BlockNumber blkno, int nblocks, XLogRecPtr *lsns);
#endif

static void
neon_get_request_lsns(NRelFileInfo rinfo, ForkNumber forknum,
					  BlockNumber blkno, neon_request_lsns *output,
					  BlockNumber nblocks, const bits8 *mask);
static bool neon_prefetch_response_usable(neon_request_lsns *request_lsns,
										  PrefetchRequest *slot);

static bool
compact_prefetch_buffers(void)
{
	uint64		empty_ring_index = MyPState->ring_last;
	uint64		search_ring_index = MyPState->ring_receive;
	int			n_moved = 0;

	if (MyPState->ring_receive == MyPState->ring_last)
		return false;

	while (search_ring_index > MyPState->ring_last)
	{
		search_ring_index--;
		if (GetPrfSlot(search_ring_index)->status == PRFS_UNUSED)
		{
			empty_ring_index = search_ring_index;
			break;
		}
	}

	/*
	 * Here we have established: slots < search_ring_index have an unknown
	 * state (not scanned) slots >= search_ring_index and <= empty_ring_index
	 * are unused slots > empty_ring_index are in use, or outside our buffer's
	 * range. ... unless search_ring_index <= ring_last
	 *
	 * Therefore, there is a gap of at least one unused items between
	 * search_ring_index and empty_ring_index (both inclusive), which grows as
	 * we hit more unused items while moving backwards through the array.
	 */

	while (search_ring_index > MyPState->ring_last)
	{
		PrefetchRequest *source_slot;
		PrefetchRequest *target_slot;
		bool		found;

		/* update search index to an unprocessed entry */
		search_ring_index--;

		source_slot = GetPrfSlot(search_ring_index);

		if (source_slot->status == PRFS_UNUSED)
			continue;

		/* slot is used -- start moving slot */
		target_slot = GetPrfSlot(empty_ring_index);

		Assert(source_slot->status == PRFS_RECEIVED);
		Assert(target_slot->status == PRFS_UNUSED);

		target_slot->buftag = source_slot->buftag;
		target_slot->shard_no = source_slot->shard_no;
		target_slot->status = source_slot->status;
		target_slot->response = source_slot->response;
		target_slot->reqid = source_slot->reqid;
		target_slot->request_lsns = source_slot->request_lsns;
		target_slot->my_ring_index = empty_ring_index;

		prfh_delete(MyPState->prf_hash, source_slot);
		prfh_insert(MyPState->prf_hash, target_slot, &found);

		Assert(!found);

		/* Adjust the location of our known-empty slot */
		empty_ring_index--;

		/* empty the moved slot */
		source_slot->status = PRFS_UNUSED;
		source_slot->buftag = (BufferTag)
		{
			0
		};
		source_slot->response = NULL;
		source_slot->my_ring_index = 0;
		source_slot->request_lsns = (neon_request_lsns) {
			InvalidXLogRecPtr, InvalidXLogRecPtr, InvalidXLogRecPtr
		};

		/* update bookkeeping */
		n_moved++;
	}

	/*
	 * Only when we've moved slots we can expect trailing unused slots, so
	 * only then we clean up trailing unused slots.
	 */
	if (n_moved > 0)
	{
		prefetch_cleanup_trailing_unused();
		return true;
	}

	return false;
}

void
readahead_buffer_resize(int newsize, void *extra)
{
	uint64		end,
				nfree = newsize;
	PrefetchState *newPState;
	Size		newprfs_size = offsetof(PrefetchState, prf_buffer) +
		(sizeof(PrefetchRequest) * newsize);

	/* don't try to re-initialize if we haven't initialized yet */
	if (MyPState == NULL)
		return;

	/*
	 * Make sure that we don't lose track of active prefetch requests by
	 * ensuring we have received all but the last n requests (n = newsize).
	 */
	if (MyPState->n_requests_inflight > newsize)
	{
		Assert(MyPState->ring_unused >= MyPState->n_requests_inflight - newsize);
		prefetch_wait_for(MyPState->ring_unused - (MyPState->n_requests_inflight - newsize));
		Assert(MyPState->n_requests_inflight <= newsize);
	}

	/* construct the new PrefetchState, and copy over the memory contexts */
	newPState = MemoryContextAllocZero(TopMemoryContext, newprfs_size);

	newPState->bufctx = MyPState->bufctx;
	newPState->errctx = MyPState->errctx;
	newPState->hashctx = MyPState->hashctx;
	newPState->prf_hash = prfh_create(MyPState->hashctx, newsize, NULL);
	newPState->n_unused = newsize;
	newPState->n_requests_inflight = 0;
	newPState->n_responses_buffered = 0;
	newPState->ring_last = newsize;
	newPState->ring_unused = newsize;
	newPState->ring_receive = newsize;
	newPState->max_shard_no = MyPState->max_shard_no;
	memcpy(newPState->shard_bitmap, MyPState->shard_bitmap, sizeof(MyPState->shard_bitmap));

	/*
	 * Copy over the prefetches.
	 *
	 * We populate the prefetch array from the end; to retain the most recent
	 * prefetches, but this has the benefit of only needing to do one
	 * iteration on the dataset, and trivial compaction.
	 */
	for (end = MyPState->ring_unused - 1;
		 end >= MyPState->ring_last && end != UINT64_MAX && nfree != 0;
		 end -= 1)
	{
		PrefetchRequest *slot = GetPrfSlot(end);
		PrefetchRequest *newslot;
		bool		found;

		if (slot->status == PRFS_UNUSED)
			continue;

		nfree -= 1;

		newslot = &newPState->prf_buffer[nfree];
		*newslot = *slot;
		newslot->my_ring_index = nfree;

		prfh_insert(newPState->prf_hash, newslot, &found);

		Assert(!found);

		switch (newslot->status)
		{
			case PRFS_UNUSED:
				pg_unreachable();
			case PRFS_REQUESTED:
				newPState->n_requests_inflight += 1;
				newPState->ring_receive -= 1;
				newPState->ring_last -= 1;
				break;
			case PRFS_RECEIVED:
				newPState->n_responses_buffered += 1;
				newPState->ring_last -= 1;
				break;
			case PRFS_TAG_REMAINS:
				newPState->ring_last -= 1;
				break;
		}
		newPState->n_unused -= 1;
	}
	newPState->ring_flush = newPState->ring_receive;

	MyNeonCounters->getpage_prefetches_buffered =
		MyPState->n_responses_buffered;
	MyNeonCounters->pageserver_open_requests =
		MyPState->n_requests_inflight;

	for (; end >= MyPState->ring_last && end != UINT64_MAX; end -= 1)
	{
		PrefetchRequest *slot = GetPrfSlot(end);
		Assert(slot->status != PRFS_REQUESTED);
		if (slot->status == PRFS_RECEIVED)
		{
			pfree(slot->response);
		}
	}

	prfh_destroy(MyPState->prf_hash);
	pfree(MyPState);
	MyPState = newPState;
}



/*
 * Make sure that there are no responses still in the buffer.
 *
 * NOTE: this function may indirectly update MyPState->pfs_hash; which
 * invalidates any active pointers into the hash table.
 */
static void
consume_prefetch_responses(void)
{
	if (MyPState->ring_receive < MyPState->ring_unused)
		prefetch_wait_for(MyPState->ring_unused - 1);
}

static void
prefetch_cleanup_trailing_unused(void)
{
	uint64		ring_index;
	PrefetchRequest *slot;

	while (MyPState->ring_last < MyPState->ring_receive)
	{
		ring_index = MyPState->ring_last;
		slot = GetPrfSlot(ring_index);

		if (slot->status == PRFS_UNUSED)
			MyPState->ring_last += 1;
		else
			break;
	}
}


static bool
prefetch_flush_requests(void)
{
	for (shardno_t shard_no = 0; shard_no < MyPState->max_shard_no; shard_no++)
	{
		if (BITMAP_ISSET(MyPState->shard_bitmap, shard_no))
		{
			if (!page_server->flush(shard_no))
				return false;
			BITMAP_CLR(MyPState->shard_bitmap, shard_no);
		}
	}
	MyPState->max_shard_no = 0;
	return true;
}

/*
 * Wait for slot of ring_index to have received its response.
 * The caller is responsible for making sure the request buffer is flushed.
 *
 * NOTE: this function may indirectly update MyPState->pfs_hash; which
 * invalidates any active pointers into the hash table.
 * NOTE: callers should make sure they can handle query cancellations in this
 * function's call path.
 */
static bool
prefetch_wait_for(uint64 ring_index)
{
	PrefetchRequest *entry;

	if (MyPState->ring_flush <= ring_index &&
		MyPState->ring_unused > MyPState->ring_flush)
	{
		if (!prefetch_flush_requests())
			return false;
		MyPState->ring_flush = MyPState->ring_unused;
	}

	Assert(MyPState->ring_unused > ring_index);

	while (MyPState->ring_receive <= ring_index)
	{
		entry = GetPrfSlot(MyPState->ring_receive);

		Assert(entry->status == PRFS_REQUESTED);
		if (!prefetch_read(entry))
			return false;
	}
	return true;
}

/*
 * Read the response of a prefetch request into its slot.
 *
 * The caller is responsible for making sure that the request for this buffer
 * was flushed to the PageServer.
 *
 * NOTE: this function may indirectly update MyPState->pfs_hash; which
 * invalidates any active pointers into the hash table.
 *
 * NOTE: this does IO, and can get canceled out-of-line.
 */
static bool
prefetch_read(PrefetchRequest *slot)
{
	NeonResponse *response;
	MemoryContext old;
	BufferTag	buftag;
	shardno_t	shard_no;
	uint64		my_ring_index;

	Assert(slot->status == PRFS_REQUESTED);
	Assert(slot->response == NULL);
	Assert(slot->my_ring_index == MyPState->ring_receive);

	if (slot->status != PRFS_REQUESTED ||
		slot->response != NULL ||
		slot->my_ring_index != MyPState->ring_receive)
		neon_shard_log(slot->shard_no, ERROR,
					   "Incorrect prefetch read: status=%d response=%p my=%lu receive=%lu",
					   slot->status, slot->response,
					   (long)slot->my_ring_index, (long)MyPState->ring_receive);

	/*
	 * Copy the request info so that if an error happens and the prefetch
	 * queue is flushed during the receive call, we can print the original
	 * values in the error message
	 */
	buftag = slot->buftag;
	shard_no = slot->shard_no;
	my_ring_index = slot->my_ring_index;

	old = MemoryContextSwitchTo(MyPState->errctx);
	response = (NeonResponse *) page_server->receive(shard_no);
	MemoryContextSwitchTo(old);
	if (response)
	{
		/* The slot should still be valid */
		if (slot->status != PRFS_REQUESTED ||
			slot->response != NULL ||
			slot->my_ring_index != MyPState->ring_receive)
			neon_shard_log(shard_no, ERROR,
						   "Incorrect prefetch slot state after receive: status=%d response=%p my=%lu receive=%lu",
						   slot->status, slot->response,
						   (long) slot->my_ring_index, (long) MyPState->ring_receive);

		/* update prefetch state */
		MyPState->n_responses_buffered += 1;
		MyPState->n_requests_inflight -= 1;
		MyPState->ring_receive += 1;
		MyNeonCounters->getpage_prefetches_buffered =
			MyPState->n_responses_buffered;

		/* update slot state */
		slot->status = PRFS_RECEIVED;
		slot->response = response;
		return true;
	}
	else
	{
		/*
		 * Note: The slot might no longer be valid, if the connection was lost
		 * and the prefetch queue was flushed during the receive call
		 */
		neon_shard_log(shard_no, LOG,
					   "No response from reading prefetch entry %lu: %u/%u/%u.%u block %u. This can be caused by a concurrent disconnect",
					   (long) my_ring_index,
					   RelFileInfoFmt(BufTagGetNRelFileInfo(buftag)),
					   buftag.forkNum, buftag.blockNum);
		return false;
	}
}

/*
 * Disconnect hook - drop prefetches when the connection drops
 *
 * If we don't remove the failed prefetches, we'd be serving incorrect
 * data to the smgr.
 */
void
prefetch_on_ps_disconnect(void)
{
	MyPState->ring_flush = MyPState->ring_unused;

	while (MyPState->ring_receive < MyPState->ring_unused)
	{
		PrefetchRequest *slot;
		uint64		ring_index = MyPState->ring_receive;

		slot = GetPrfSlot(ring_index);

		Assert(slot->status == PRFS_REQUESTED);
		Assert(slot->my_ring_index == ring_index);

		/*
		 * Drop connection to all shards which have prefetch requests.
		 * It is not a problem to call disconnect multiple times on the same connection
		 * because disconnect implementation in libpagestore.c will check if connection
		 * is alive and do nothing of connection was already dropped.
		 */
		page_server->disconnect(slot->shard_no);

		/* clean up the request */
		slot->status = PRFS_TAG_REMAINS;
		MyPState->n_requests_inflight -= 1;
		MyPState->ring_receive += 1;

		prefetch_set_unused(ring_index);
		pgBufferUsage.prefetch.expired += 1;
		MyNeonCounters->getpage_prefetch_discards_total += 1;
	}

	/*
	 * We can have gone into retry due to network error, so update stats with
	 * the latest available 
	 */
	MyNeonCounters->pageserver_open_requests =
		MyPState->n_requests_inflight;
	MyNeonCounters->getpage_prefetches_buffered =
		MyPState->n_responses_buffered;
}

/*
 * prefetch_set_unused() - clear a received prefetch slot
 *
 * The slot at ring_index must be a current member of the ring buffer,
 * and may not be in the PRFS_REQUESTED state.
 *
 * NOTE: this function will update MyPState->pfs_hash; which invalidates any
 * active pointers into the hash table.
 */
static inline void
prefetch_set_unused(uint64 ring_index)
{
	PrefetchRequest *slot;

	if (ring_index < MyPState->ring_last)
		return;					/* Should already be unused */

	slot = GetPrfSlot(ring_index);
	if (slot->status == PRFS_UNUSED)
		return;

	Assert(slot->status == PRFS_RECEIVED || slot->status == PRFS_TAG_REMAINS);

	if (slot->status == PRFS_RECEIVED)
	{
		pfree(slot->response);
		slot->response = NULL;

		MyPState->n_responses_buffered -= 1;
		MyPState->n_unused += 1;

		MyNeonCounters->getpage_prefetches_buffered =
			MyPState->n_responses_buffered;
	}
	else
	{
		Assert(slot->response == NULL);
	}

	prfh_delete(MyPState->prf_hash, slot);

	/* clear all fields */
	MemSet(slot, 0, sizeof(PrefetchRequest));
	slot->status = PRFS_UNUSED;

	/* run cleanup if we're holding back ring_last */
	if (MyPState->ring_last == ring_index)
		prefetch_cleanup_trailing_unused();

	/*
	 * ... and try to store the buffered responses more compactly if > 12.5%
	 * of the buffer is gaps
	 */
	else if (ReceiveBufferNeedsCompaction())
		compact_prefetch_buffers();
}

/*
 * Send one prefetch request to the pageserver. To wait for the response, call
 * prefetch_wait_for().
 */
static void
prefetch_do_request(PrefetchRequest *slot, neon_request_lsns *force_request_lsns)
{
	bool		found;
	uint64		mySlotNo PG_USED_FOR_ASSERTS_ONLY = slot->my_ring_index;

	NeonGetPageRequest request = {
		.hdr.tag = T_NeonGetPageRequest,
		.hdr.reqid = GENERATE_REQUEST_ID(),
		/* lsn and not_modified_since are filled in below */
		.rinfo = BufTagGetNRelFileInfo(slot->buftag),
		.forknum = slot->buftag.forkNum,
		.blkno = slot->buftag.blockNum,
	};

	Assert(mySlotNo == MyPState->ring_unused);

	slot->reqid = request.hdr.reqid;

	if (force_request_lsns)
		slot->request_lsns = *force_request_lsns;
	else
		neon_get_request_lsns(BufTagGetNRelFileInfo(slot->buftag),
							  slot->buftag.forkNum, slot->buftag.blockNum,
							  &slot->request_lsns, 1, NULL);
	request.hdr.lsn = slot->request_lsns.request_lsn;
	request.hdr.not_modified_since = slot->request_lsns.not_modified_since;

	Assert(slot->response == NULL);
	Assert(slot->my_ring_index == MyPState->ring_unused);

	while (!page_server->send(slot->shard_no, (NeonRequest *) &request))
	{
		Assert(mySlotNo == MyPState->ring_unused);
		/* loop */
	}

	/* update prefetch state */
	MyPState->n_requests_inflight += 1;
	MyPState->n_unused -= 1;
	MyPState->ring_unused += 1;
	BITMAP_SET(MyPState->shard_bitmap, slot->shard_no);
	MyPState->max_shard_no = Max(slot->shard_no+1, MyPState->max_shard_no);

	/* update slot state */
	slot->status = PRFS_REQUESTED;
	prfh_insert(MyPState->prf_hash, slot, &found);
	Assert(!found);
}

/*
 * prefetch_register_bufferv() - register and prefetch buffers
 *
 * Register that we may want the contents of BufferTag in the near future.
 * This is used when issuing a speculative prefetch request, but also when
 * performing a synchronous request and need the buffer right now.
 *
 * If force_request_lsns is not NULL, those values are sent to the
 * pageserver. If NULL, we utilize the lastWrittenLsn -infrastructure
 * to calculate the LSNs to send.
 *
 * When performing a prefetch rather than a synchronous request,
 * is_prefetch==true. Currently, it only affects how the request is accounted
 * in the perf counters.
 *
 * NOTE: this function may indirectly update MyPState->pfs_hash; which
 * invalidates any active pointers into the hash table.
 */
static uint64
prefetch_register_bufferv(BufferTag tag, neon_request_lsns *frlsns,
						  BlockNumber nblocks, const bits8 *mask,
						  bool is_prefetch)
{
	uint64		min_ring_index;
	PrefetchRequest hashkey;
#if USE_ASSERT_CHECKING
	bool		any_hits = false;
#endif
	/* We will never read further ahead than our buffer can store. */
	nblocks = Max(1, Min(nblocks, readahead_buffer_size));

	/*
	 * Use an intermediate PrefetchRequest struct as the hash key to ensure
	 * correct alignment and that the padding bytes are cleared.
	 */
	memset(&hashkey.buftag, 0, sizeof(BufferTag));
	hashkey.buftag = tag;

Retry:
	/*
	 * We can have gone into retry due to network error, so update stats with
	 * the latest available 
	 */
	MyNeonCounters->pageserver_open_requests =
		MyPState->ring_unused - MyPState->ring_receive;
	MyNeonCounters->getpage_prefetches_buffered =
		MyPState->n_responses_buffered;

	min_ring_index = UINT64_MAX;
	for (int i = 0; i < nblocks; i++)
	{
		PrefetchRequest *slot = NULL;
		PrfHashEntry *entry = NULL;
		uint64		ring_index;
		neon_request_lsns *lsns;

		if (PointerIsValid(mask) && !BITMAP_ISSET(mask, i))
			continue;

		if (frlsns)
			lsns = &frlsns[i];
		else
			lsns = NULL;

#if USE_ASSERT_CHECKING
		any_hits = true;
#endif

		slot = NULL;
		entry = NULL;

		hashkey.buftag.blockNum = tag.blockNum + i;
		entry = prfh_lookup(MyPState->prf_hash, &hashkey);

		if (entry != NULL)
		{
			slot = entry->slot;
			ring_index = slot->my_ring_index;
			Assert(slot == GetPrfSlot(ring_index));

			Assert(slot->status != PRFS_UNUSED);
			Assert(MyPState->ring_last <= ring_index &&
				   ring_index < MyPState->ring_unused);
			Assert(BufferTagsEqual(&slot->buftag, &hashkey.buftag));

			/*
			 * If the caller specified a request LSN to use, only accept
			 * prefetch responses that satisfy that request.
			 */
			if (lsns)
			{
				if (!neon_prefetch_response_usable(lsns, slot))
				{
					/* Wait for the old request to finish and discard it */
					if (!prefetch_wait_for(ring_index))
						goto Retry;
					prefetch_set_unused(ring_index);
					entry = NULL;
					slot = NULL;
					pgBufferUsage.prefetch.expired += 1;
					MyNeonCounters->getpage_prefetch_discards_total += 1;
				}
			}

			if (entry != NULL)
			{
				/*
				 * We received a prefetch for a page that was recently read
				 * and removed from the buffers. Remove that request from the
				 * buffers.
				 */
				if (slot->status == PRFS_TAG_REMAINS)
				{
					prefetch_set_unused(ring_index);
					entry = NULL;
					slot = NULL;
				}
				else
				{
					min_ring_index = Min(min_ring_index, ring_index);
					/* The buffered request is good enough, return that index */
					if (is_prefetch)
						pgBufferUsage.prefetch.duplicates++;
					else
						pgBufferUsage.prefetch.hits++;
					continue;
				}
			}
		}
		else if (!is_prefetch)
		{
			pgBufferUsage.prefetch.misses += 1;
			MyNeonCounters->getpage_prefetch_misses_total++;
		}
		/*
		 * We can only leave the block above by finding that there's
		 * no entry that can satisfy this request, either because there
		 * was no entry, or because the entry was invalid or didn't satisfy
		 * the LSNs provided.
		 *
		 * The code should've made sure to clear up the data.
		 */
		Assert(entry == NULL);
		Assert(slot == NULL);

		/* There should be no buffer overflow */
		Assert(MyPState->ring_last + readahead_buffer_size >= MyPState->ring_unused);

		/*
		 * If the prefetch queue is full, we need to make room by clearing the
		 * oldest slot. If the oldest slot holds a buffer that was already
		 * received, we can just throw it away; we fetched the page
		 * unnecessarily in that case. If the oldest slot holds a request that
		 * we haven't received a response for yet, we have to wait for the
		 * response to that before we can continue. We might not have even
		 * flushed the request to the pageserver yet, it might be just sitting
		 * in the output buffer. In that case, we flush it and wait for the
		 * response. (We could decide not to send it, but it's hard to abort
		 * when the request is already in the output buffer, and 'not sending'
		 * a prefetch request kind of goes against the principles of
		 * prefetching)
		 */
		if (MyPState->ring_last + readahead_buffer_size == MyPState->ring_unused)
		{
			uint64		cleanup_index = MyPState->ring_last;

			slot = GetPrfSlot(cleanup_index);

			Assert(slot->status != PRFS_UNUSED);

			/*
			 * If there is good reason to run compaction on the prefetch buffers,
			 * try to do that.
			 */
			if (ReceiveBufferNeedsCompaction() && compact_prefetch_buffers())
			{
				Assert(slot->status == PRFS_UNUSED);
			}
			else
			{
				/*
				 * We have the slot for ring_last, so that must still be in
				 * progress
				 */
				switch (slot->status)
				{
					case PRFS_REQUESTED:
						Assert(MyPState->ring_receive == cleanup_index);
						if (!prefetch_wait_for(cleanup_index))
							goto Retry;
						prefetch_set_unused(cleanup_index);
						pgBufferUsage.prefetch.expired += 1;
						MyNeonCounters->getpage_prefetch_discards_total += 1;
						break;
					case PRFS_RECEIVED:
					case PRFS_TAG_REMAINS:
						prefetch_set_unused(cleanup_index);
						pgBufferUsage.prefetch.expired += 1;
						MyNeonCounters->getpage_prefetch_discards_total += 1;
						break;
					default:
						pg_unreachable();
				}
			}
		}

		/*
		 * The next buffer pointed to by `ring_unused` is now definitely empty, so
		 * we can insert the new request to it.
		 */
		ring_index = MyPState->ring_unused;

		Assert(MyPState->ring_last <= ring_index &&
			   ring_index <= MyPState->ring_unused);

		slot = GetPrfSlotNoCheck(ring_index);

		Assert(slot->status == PRFS_UNUSED);

		/*
		 * We must update the slot data before insertion, because the hash
		 * function reads the buffer tag from the slot.
		 */
		slot->buftag = hashkey.buftag;
		slot->shard_no = get_shard_number(&tag);
		slot->my_ring_index = ring_index;

		min_ring_index = Min(min_ring_index, ring_index);

		if (is_prefetch)
			MyNeonCounters->getpage_prefetch_requests_total++;
		else
			MyNeonCounters->getpage_sync_requests_total++;

		prefetch_do_request(slot, lsns);
	}

	MyNeonCounters->pageserver_open_requests =
		MyPState->ring_unused - MyPState->ring_receive;

	Assert(any_hits);

	Assert(GetPrfSlot(min_ring_index)->status == PRFS_REQUESTED ||
		   GetPrfSlot(min_ring_index)->status == PRFS_RECEIVED);
	Assert(MyPState->ring_last <= min_ring_index &&
		   min_ring_index < MyPState->ring_unused);

	if (flush_every_n_requests > 0 &&
		MyPState->ring_unused - MyPState->ring_flush >= flush_every_n_requests)
	{
		if (!prefetch_flush_requests())
		{
			/*
			 * Prefetch set is reset in case of error, so we should try to
			 * register our request once again
			 */
			goto Retry;
		}
		MyPState->ring_flush = MyPState->ring_unused;
	}

	return min_ring_index;
}

static bool
equal_requests(NeonRequest* a, NeonRequest* b)
{
	return a->reqid == b->reqid && a->lsn == b->lsn && a->not_modified_since == b->not_modified_since;
}


/*
 * Note: this function can get canceled and use a long jump to the next catch
 * context. Take care.
 */
static NeonResponse *
page_server_request(void const *req)
{
	NeonResponse *resp;
	BufferTag tag = {0};
	shardno_t shard_no;

	switch (messageTag(req))
	{
		case T_NeonExistsRequest:
			CopyNRelFileInfoToBufTag(tag, ((NeonExistsRequest *) req)->rinfo);
			break;
		case T_NeonNblocksRequest:
			CopyNRelFileInfoToBufTag(tag, ((NeonNblocksRequest *) req)->rinfo);
			break;
		case T_NeonDbSizeRequest:
			NInfoGetDbOid(BufTagGetNRelFileInfo(tag)) = ((NeonDbSizeRequest *) req)->dbNode;
			break;
		case T_NeonGetPageRequest:
			CopyNRelFileInfoToBufTag(tag, ((NeonGetPageRequest *) req)->rinfo);
			tag.blockNum = ((NeonGetPageRequest *) req)->blkno;
			break;
		default:
			neon_log(ERROR, "Unexpected request tag: %d", messageTag(req));
	}
	shard_no = get_shard_number(&tag);

	/*
	 * Current sharding model assumes that all metadata is present only at shard 0.
	 * We still need to call get_shard_no() to check if shard map is up-to-date.
	 */
	if (((NeonRequest *) req)->tag != T_NeonGetPageRequest)
	{
		shard_no = 0;
	}

	do
	{
		PG_TRY();
		{
			while (!page_server->send(shard_no, (NeonRequest *) req)
				   || !page_server->flush(shard_no))
			{
				/* do nothing */
			}
			MyNeonCounters->pageserver_open_requests++;
			consume_prefetch_responses();
			resp = page_server->receive(shard_no);
			MyNeonCounters->pageserver_open_requests--;
		}
		PG_CATCH();
		{
			/*
			 * Cancellation in this code needs to be handled better at some
			 * point, but this currently seems fine for now.
			 */
			page_server->disconnect(shard_no);
			MyNeonCounters->pageserver_open_requests = 0;

			PG_RE_THROW();
		}
		PG_END_TRY();

	} while (resp == NULL);

	return resp;
}


StringInfoData
nm_pack_request(NeonRequest *msg)
{
	StringInfoData s;

	initStringInfo(&s);

	pq_sendbyte(&s, msg->tag);
	if (neon_protocol_version >= 3)
	{
		pq_sendint64(&s, msg->reqid);
	}
	pq_sendint64(&s, msg->lsn);
	pq_sendint64(&s, msg->not_modified_since);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_NeonExistsRequest:
			{
				NeonExistsRequest *msg_req = (NeonExistsRequest *) msg;

				pq_sendint32(&s, NInfoGetSpcOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetDbOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetRelNumber(msg_req->rinfo));
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_NeonNblocksRequest:
			{
				NeonNblocksRequest *msg_req = (NeonNblocksRequest *) msg;

				pq_sendint32(&s, NInfoGetSpcOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetDbOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetRelNumber(msg_req->rinfo));
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_NeonDbSizeRequest:
			{
				NeonDbSizeRequest *msg_req = (NeonDbSizeRequest *) msg;

				pq_sendint32(&s, msg_req->dbNode);

				break;
			}
		case T_NeonGetPageRequest:
			{
				NeonGetPageRequest *msg_req = (NeonGetPageRequest *) msg;

				pq_sendint32(&s, NInfoGetSpcOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetDbOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetRelNumber(msg_req->rinfo));
				pq_sendbyte(&s, msg_req->forknum);
				pq_sendint32(&s, msg_req->blkno);

				break;
			}

		case T_NeonGetSlruSegmentRequest:
			{
				NeonGetSlruSegmentRequest *msg_req = (NeonGetSlruSegmentRequest *) msg;

				pq_sendbyte(&s, msg_req->kind);
				pq_sendint32(&s, msg_req->segno);

				break;
			}

			/* pagestore -> pagestore_client. We never need to create these. */
		case T_NeonExistsResponse:
		case T_NeonNblocksResponse:
		case T_NeonGetPageResponse:
		case T_NeonErrorResponse:
		case T_NeonDbSizeResponse:
		case T_NeonGetSlruSegmentResponse:
		default:
			neon_log(ERROR, "unexpected neon message tag 0x%02x", msg->tag);
			break;
	}
	return s;
}

NeonResponse *
nm_unpack_response(StringInfo s)
{
	NeonMessageTag tag = pq_getmsgbyte(s);
	NeonResponse resp_hdr = {0}; /* make valgrind happy */
	NeonResponse *resp = NULL;

	resp_hdr.tag = tag;
	if (neon_protocol_version >= 3)
	{
		resp_hdr.reqid = pq_getmsgint64(s);
		resp_hdr.lsn = pq_getmsgint64(s);
		resp_hdr.not_modified_since = pq_getmsgint64(s);
	}
	switch (tag)
	{
			/* pagestore -> pagestore_client */
		case T_NeonExistsResponse:
			{
				NeonExistsResponse *msg_resp = palloc0(sizeof(NeonExistsResponse));

				if (neon_protocol_version >= 3)
				{
					NInfoGetSpcOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetDbOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetRelNumber(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					msg_resp->req.forknum = pq_getmsgbyte(s);
				}
				msg_resp->req.hdr = resp_hdr;
				msg_resp->exists = pq_getmsgbyte(s);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonNblocksResponse:
			{
				NeonNblocksResponse *msg_resp = palloc0(sizeof(NeonNblocksResponse));

				if (neon_protocol_version >= 3)
				{
					NInfoGetSpcOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetDbOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetRelNumber(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					msg_resp->req.forknum = pq_getmsgbyte(s);
				}
				msg_resp->req.hdr = resp_hdr;
				msg_resp->n_blocks = pq_getmsgint(s, 4);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonGetPageResponse:
			{
				NeonGetPageResponse *msg_resp;

				msg_resp = MemoryContextAllocZero(MyPState->bufctx, PS_GETPAGERESPONSE_SIZE);
				if (neon_protocol_version >= 3)
				{
					NInfoGetSpcOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetDbOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetRelNumber(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					msg_resp->req.forknum = pq_getmsgbyte(s);
					msg_resp->req.blkno = pq_getmsgint(s, 4);
				}
				msg_resp->req.hdr = resp_hdr;
				/* XXX:	should be varlena */
				memcpy(msg_resp->page, pq_getmsgbytes(s, BLCKSZ), BLCKSZ);
				pq_getmsgend(s);

				Assert(msg_resp->req.hdr.tag == T_NeonGetPageResponse);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse *msg_resp = palloc0(sizeof(NeonDbSizeResponse));

				if (neon_protocol_version >= 3)
				{
					msg_resp->req.dbNode = pq_getmsgint(s, 4);
				}
				msg_resp->req.hdr = resp_hdr;
				msg_resp->db_size = pq_getmsgint64(s);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonErrorResponse:
			{
				NeonErrorResponse *msg_resp;
				size_t		msglen;
				const char *msgtext;

				msgtext = pq_getmsgrawstring(s);
				msglen = strlen(msgtext);

				msg_resp = palloc0(sizeof(NeonErrorResponse) + msglen + 1);
				msg_resp->req = resp_hdr;
				memcpy(msg_resp->message, msgtext, msglen + 1);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonGetSlruSegmentResponse:
		    {
				NeonGetSlruSegmentResponse *msg_resp;
				int n_blocks;
				msg_resp = palloc0(sizeof(NeonGetSlruSegmentResponse));

				if (neon_protocol_version >= 3)
				{
					msg_resp->req.kind = pq_getmsgbyte(s);
					msg_resp->req.segno = pq_getmsgint(s, 4);
				}
				msg_resp->req.hdr = resp_hdr;

				n_blocks = pq_getmsgint(s, 4);
				msg_resp->n_blocks = n_blocks;
				memcpy(msg_resp->data, pq_getmsgbytes(s, n_blocks * BLCKSZ), n_blocks * BLCKSZ);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

			/*
			 * pagestore_client -> pagestore
			 *
			 * We create these ourselves, and don't need to decode them.
			 */
		case T_NeonExistsRequest:
		case T_NeonNblocksRequest:
		case T_NeonGetPageRequest:
		case T_NeonDbSizeRequest:
		case T_NeonGetSlruSegmentRequest:
		default:
			neon_log(ERROR, "unexpected neon message tag 0x%02x", tag);
			break;
	}

	return resp;
}

/* dump to json for debugging / error reporting purposes */
char *
nm_to_string(NeonMessage *msg)
{
	StringInfoData s;

	initStringInfo(&s);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_NeonExistsRequest:
			{
				NeonExistsRequest *msg_req = (NeonExistsRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonExistsRequest\"");
				appendStringInfo(&s, ", \"rinfo\": \"%u/%u/%u\"", RelFileInfoFmt(msg_req->rinfo));
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonNblocksRequest:
			{
				NeonNblocksRequest *msg_req = (NeonNblocksRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonNblocksRequest\"");
				appendStringInfo(&s, ", \"rinfo\": \"%u/%u/%u\"", RelFileInfoFmt(msg_req->rinfo));
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonGetPageRequest:
			{
				NeonGetPageRequest *msg_req = (NeonGetPageRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetPageRequest\"");
				appendStringInfo(&s, ", \"rinfo\": \"%u/%u/%u\"", RelFileInfoFmt(msg_req->rinfo));
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"blkno\": %u", msg_req->blkno);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonDbSizeRequest:
			{
				NeonDbSizeRequest *msg_req = (NeonDbSizeRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonDbSizeRequest\"");
				appendStringInfo(&s, ", \"dbnode\": \"%u\"", msg_req->dbNode);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonGetSlruSegmentRequest:
			{
				NeonGetSlruSegmentRequest *msg_req = (NeonGetSlruSegmentRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetSlruSegmentRequest\"");
				appendStringInfo(&s, ", \"kind\": %u", msg_req->kind);
				appendStringInfo(&s, ", \"segno\": %u", msg_req->segno);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}
			/* pagestore -> pagestore_client */
		case T_NeonExistsResponse:
			{
				NeonExistsResponse *msg_resp = (NeonExistsResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonExistsResponse\"");
				appendStringInfo(&s, ", \"exists\": %d}",
								 msg_resp->exists);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_NeonNblocksResponse:
			{
				NeonNblocksResponse *msg_resp = (NeonNblocksResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonNblocksResponse\"");
				appendStringInfo(&s, ", \"n_blocks\": %u}",
								 msg_resp->n_blocks);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_NeonGetPageResponse:
			{
#if 0
				NeonGetPageResponse *msg_resp = (NeonGetPageResponse *) msg;
#endif

				appendStringInfoString(&s, "{\"type\": \"NeonGetPageResponse\"");
				appendStringInfo(&s, ", \"page\": \"XXX\"}");
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonErrorResponse:
			{
				NeonErrorResponse *msg_resp = (NeonErrorResponse *) msg;

				/* FIXME: escape double-quotes in the message */
				appendStringInfoString(&s, "{\"type\": \"NeonErrorResponse\"");
				appendStringInfo(&s, ", \"message\": \"%s\"}", msg_resp->message);
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse *msg_resp = (NeonDbSizeResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonDbSizeResponse\"");
				appendStringInfo(&s, ", \"db_size\": %ld}",
								 msg_resp->db_size);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_NeonGetSlruSegmentResponse:
			{
				NeonGetSlruSegmentResponse *msg_resp = (NeonGetSlruSegmentResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetSlruSegmentResponse\"");
				appendStringInfo(&s, ", \"n_blocks\": %u}",
								 msg_resp->n_blocks);
				appendStringInfoChar(&s, '}');

				break;
			}

		default:
			appendStringInfo(&s, "{\"type\": \"unknown 0x%02x\"", msg->tag);
	}
	return s.data;
}

/*
 * Wrapper around log_newpage() that makes a temporary copy of the block and
 * WAL-logs that. This makes it safe to use while holding only a shared lock
 * on the page, see XLogSaveBufferForHint. We don't use XLogSaveBufferForHint
 * directly because it skips the logging if the LSN is new enough.
 */
static XLogRecPtr
log_newpage_copy(NRelFileInfo * rinfo, ForkNumber forkNum, BlockNumber blkno,
				 Page page, bool page_std)
{
	PGAlignedBlock copied_buffer;

	memcpy(copied_buffer.data, page, BLCKSZ);
	return log_newpage(rinfo, forkNum, blkno, copied_buffer.data, page_std);
}

#if PG_MAJORVERSION_NUM >= 17
/*
 * Wrapper around log_newpages() that makes a temporary copy of the block and
 * WAL-logs that. This makes it safe to use while holding only a shared lock
 * on the page, see XLogSaveBufferForHint. We don't use XLogSaveBufferForHint
 * directly because it skips the logging if the LSN is new enough.
 */
static XLogRecPtr
log_newpages_copy(NRelFileInfo * rinfo, ForkNumber forkNum, BlockNumber blkno,
				  BlockNumber nblocks, Page *pages, bool page_std)
{
	PGAlignedBlock copied_buffer[XLR_MAX_BLOCK_ID];
	BlockNumber	blknos[XLR_MAX_BLOCK_ID];
	Page		pageptrs[XLR_MAX_BLOCK_ID];
	int			nregistered = 0;

	for (int i = 0; i < nblocks; i++)
	{
		Page	page = copied_buffer[nregistered].data;
		memcpy(page, pages[i], BLCKSZ);
		pageptrs[nregistered] = page;
		blknos[nregistered] = blkno + i;

		++nregistered;

		if (nregistered >= XLR_MAX_BLOCK_ID)
		{
			log_newpages(rinfo, forkNum, nregistered, blknos, pageptrs,
						 page_std);
			nregistered = 0;
		}
	}

	if (nregistered != 0)
	{
		log_newpages(rinfo, forkNum, nregistered, blknos, pageptrs,
					 page_std);
	}

	return ProcLastRecPtr;
}
#endif /* PG_MAJORVERSION_NUM >= 17 */

/*
 * Is 'buffer' identical to a freshly initialized empty heap page?
 */
static bool
PageIsEmptyHeapPage(char *buffer)
{
	PGAlignedBlock empty_page;

	PageInit((Page) empty_page.data, BLCKSZ, 0);

	return memcmp(buffer, empty_page.data, BLCKSZ) == 0;
}

#if PG_MAJORVERSION_NUM >= 17
static void
neon_wallog_pagev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				  BlockNumber nblocks, const char **buffers, bool force)
{
#define BLOCK_BATCH_SIZE	16
	bool		log_pages;
	BlockNumber	batch_blockno = blocknum;
	XLogRecPtr	lsns[BLOCK_BATCH_SIZE];
	int			batch_size = 0;

	/*
	 * Whenever a VM or FSM page is evicted, WAL-log it. FSM and (some) VM
	 * changes are not WAL-logged when the changes are made, so this is our
	 * last chance to log them, otherwise they're lost. That's OK for
	 * correctness, the non-logged updates are not critical. But we want to
	 * have a reasonably up-to-date VM and FSM in the page server.
	 */
	log_pages = false;
	if (force)
	{
		Assert(XLogInsertAllowed());
		log_pages = true;
	}
	else if (XLogInsertAllowed() &&
			 !ShutdownRequestPending &&
			 (forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM))
	{
		log_pages = true;
	}

	if (log_pages)
	{
		XLogRecPtr	recptr;
		recptr = log_newpages_copy(&InfoFromSMgrRel(reln), forknum, blocknum,
								   nblocks, (Page *) buffers, false);

		for (int i = 0; i < nblocks; i++)
			PageSetLSN(unconstify(char *, buffers[i]), recptr);

		ereport(SmgrTrace,
				(errmsg(NEON_TAG "Page %u through %u of relation %u/%u/%u.%u "
								 "were force logged, lsn=%X/%X",
						blocknum, blocknum + nblocks,
						RelFileInfoFmt(InfoFromSMgrRel(reln)),
						forknum, LSN_FORMAT_ARGS(recptr))));
	}

	for (int i = 0; i < nblocks; i++)
	{
		Page		page = (Page) buffers[i];
		BlockNumber blkno = blocknum + i;
		XLogRecPtr	lsn = PageGetLSN(page);

		if (lsn == InvalidXLogRecPtr)
		{
			/*
			 * When PostgreSQL extends a relation, it calls smgrextend() with an
			 * all-zeros pages, and we can just ignore that in Neon. We do need to
			 * remember the new size, though, so that smgrnblocks() returns the
			 * right answer after the rel has been extended. We rely on the
			 * relsize cache for that.
			 *
			 * A completely empty heap page doesn't need to be WAL-logged, either.
			 * The heapam can leave such a page behind, if e.g. an insert errors
			 * out after initializing the page, but before it has inserted the
			 * tuple and WAL-logged the change. When we read the page from the
			 * page server, it will come back as all-zeros. That's OK, the heapam
			 * will initialize an all-zeros page on first use.
			 *
			 * In other scenarios, evicting a dirty page with no LSN is a bad
			 * sign: it implies that the page was not WAL-logged, and its contents
			 * will be lost when it's evicted.
			 */
			if (PageIsNew(page))
			{
				ereport(SmgrTrace,
						(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is all-zeros",
								blkno,
								RelFileInfoFmt(InfoFromSMgrRel(reln)),
								forknum)));
			}
			else if (PageIsEmptyHeapPage(page))
			{
				ereport(SmgrTrace,
						(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is an empty heap page with no LSN",
								blkno,
								RelFileInfoFmt(InfoFromSMgrRel(reln)),
								forknum)));
			}
			else if (forknum != FSM_FORKNUM && forknum != VISIBILITYMAP_FORKNUM)
			{
				/*
				 * Its a bad sign if there is a page with zero LSN in the buffer
				 * cache in a standby, too. However, PANICing seems like a cure
				 * worse than the disease, as the damage has likely already been
				 * done in the primary. So in a standby, make this an assertion,
				 * and in a release build just LOG the error and soldier on. We
				 * update the last-written LSN of the page with a conservative
				 * value in that case, which is the last replayed LSN.
				 */
				ereport(RecoveryInProgress() ? LOG : PANIC,
						(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is evicted with zero LSN",
								blkno,
								RelFileInfoFmt(InfoFromSMgrRel(reln)),
								forknum)));
				Assert(false);

				lsn = GetXLogReplayRecPtr(NULL); /* in standby mode, soldier on */
			}
		}
		else
		{
			ereport(SmgrTrace,
					(errmsg(NEON_TAG "Evicting page %u of relation %u/%u/%u.%u with lsn=%X/%X",
							blkno,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum, LSN_FORMAT_ARGS(lsn))));
		}

		/*
		 * Remember the LSN on this page. When we read the page again, we must
		 * read the same or newer version of it.
		 */
		lsns[batch_size++] = lsn;

		if (batch_size >= BLOCK_BATCH_SIZE)
		{
			SetLastWrittenLSNForBlockv(lsns, InfoFromSMgrRel(reln), forknum,
									   batch_blockno,
									   batch_size);
			batch_blockno += batch_size;
			batch_size = 0;
		}
	}

	if (batch_size != 0)
	{
		SetLastWrittenLSNForBlockv(lsns, InfoFromSMgrRel(reln), forknum,
								   batch_blockno,
								   batch_size);
	}
}
#endif

/*
 * A page is being evicted from the shared buffer cache. Update the
 * last-written LSN of the page, and WAL-log it if needed.
 */
#if PG_MAJORVERSION_NUM < 16
static void
neon_wallog_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool force)
#else
static void
neon_wallog_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool force)
#endif
{
	XLogRecPtr	lsn = PageGetLSN((Page) buffer);
	bool		log_page;

	/*
	 * Whenever a VM or FSM page is evicted, WAL-log it. FSM and (some) VM
	 * changes are not WAL-logged when the changes are made, so this is our
	 * last chance to log them, otherwise they're lost. That's OK for
	 * correctness, the non-logged updates are not critical. But we want to
	 * have a reasonably up-to-date VM and FSM in the page server.
	 */
	log_page = false;
	if (force)
	{
		Assert(XLogInsertAllowed());
		log_page = true;
	}
	else if (XLogInsertAllowed() &&
			 !ShutdownRequestPending &&
			 (forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM))
	{
		log_page = true;
	}

	if (log_page)
	{
		XLogRecPtr	recptr;

		recptr = log_newpage_copy(&InfoFromSMgrRel(reln), forknum, blocknum,
								  (Page) buffer, false);
		XLogFlush(recptr);
		lsn = recptr;
		ereport(SmgrTrace,
				(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u was force logged. Evicted at lsn=%X/%X",
						blocknum,
						RelFileInfoFmt(InfoFromSMgrRel(reln)),
						forknum, LSN_FORMAT_ARGS(lsn))));
	}

	if (lsn == InvalidXLogRecPtr)
	{
		/*
		 * When PostgreSQL extends a relation, it calls smgrextend() with an
		 * all-zeros pages, and we can just ignore that in Neon. We do need to
		 * remember the new size, though, so that smgrnblocks() returns the
		 * right answer after the rel has been extended. We rely on the
		 * relsize cache for that.
		 *
		 * A completely empty heap page doesn't need to be WAL-logged, either.
		 * The heapam can leave such a page behind, if e.g. an insert errors
		 * out after initializing the page, but before it has inserted the
		 * tuple and WAL-logged the change. When we read the page from the
		 * page server, it will come back as all-zeros. That's OK, the heapam
		 * will initialize an all-zeros page on first use.
		 *
		 * In other scenarios, evicting a dirty page with no LSN is a bad
		 * sign: it implies that the page was not WAL-logged, and its contents
		 * will be lost when it's evicted.
		 */
		if (PageIsNew((Page) buffer))
		{
			ereport(SmgrTrace,
					(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is all-zeros",
							blocknum,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum)));
		}
		else if (PageIsEmptyHeapPage((Page) buffer))
		{
			ereport(SmgrTrace,
					(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is an empty heap page with no LSN",
							blocknum,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum)));
		}
		else if (forknum != FSM_FORKNUM && forknum != VISIBILITYMAP_FORKNUM)
		{
			/*
			 * Its a bad sign if there is a page with zero LSN in the buffer
			 * cache in a standby, too. However, PANICing seems like a cure
			 * worse than the disease, as the damage has likely already been
			 * done in the primary. So in a standby, make this an assertion,
			 * and in a release build just LOG the error and soldier on. We
			 * update the last-written LSN of the page with a conservative
			 * value in that case, which is the last replayed LSN.
			 */
			ereport(RecoveryInProgress() ? LOG : PANIC,
					(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is evicted with zero LSN",
							blocknum,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum)));
			Assert(false);

			lsn = GetXLogReplayRecPtr(NULL); /* in standby mode, soldier on */
		}
	}
	else
	{
		ereport(SmgrTrace,
				(errmsg(NEON_TAG "Evicting page %u of relation %u/%u/%u.%u with lsn=%X/%X",
						blocknum,
						RelFileInfoFmt(InfoFromSMgrRel(reln)),
						forknum, LSN_FORMAT_ARGS(lsn))));
	}

	/*
	 * Remember the LSN on this page. When we read the page again, we must
	 * read the same or newer version of it.
	 */
	SetLastWrittenLSNForBlock(lsn, InfoFromSMgrRel(reln), forknum, blocknum);
}

/*
 *	neon_init() -- Initialize private state
 */
static void
neon_init(void)
{
	Size		prfs_size;

	if (MyPState != NULL)
		return;

	/*
	 * Sanity check that theperf counters array is sized correctly. We got
	 * this wrong once, and the formula for max number of backends and aux
	 * processes might well change in the future, so better safe than sorry.
	 * This is a very cheap check so we do it even without assertions.  On
	 * v14, this gets called before initializing MyProc, so we cannot perform
	 * the check here. That's OK, we don't expect the logic to change in old
	 * releases.
	 */
#if PG_VERSION_NUM>=150000
	if (MyNeonCounters >= &neon_per_backend_counters_shared[NUM_NEON_PERF_COUNTER_SLOTS])
		elog(ERROR, "MyNeonCounters points past end of array");
#endif

	prfs_size = offsetof(PrefetchState, prf_buffer) +
		sizeof(PrefetchRequest) * readahead_buffer_size;

	MyPState = MemoryContextAllocZero(TopMemoryContext, prfs_size);

	MyPState->n_unused = readahead_buffer_size;

	MyPState->bufctx = SlabContextCreate(TopMemoryContext,
										 "NeonSMGR/prefetch",
										 SLAB_DEFAULT_BLOCK_SIZE * 17,
										 PS_GETPAGERESPONSE_SIZE);
	MyPState->errctx = AllocSetContextCreate(TopMemoryContext,
											 "NeonSMGR/errors",
											 ALLOCSET_DEFAULT_SIZES);
	MyPState->hashctx = AllocSetContextCreate(TopMemoryContext,
											  "NeonSMGR/prefetch",
											  ALLOCSET_DEFAULT_SIZES);

	MyPState->prf_hash = prfh_create(MyPState->hashctx,
									 readahead_buffer_size, NULL);

	old_redo_read_buffer_filter = redo_read_buffer_filter;
	redo_read_buffer_filter = neon_redo_read_buffer_filter;

#ifdef DEBUG_COMPARE_LOCAL
	mdinit();
#endif
}

/*
 * GetXLogInsertRecPtr uses XLogBytePosToRecPtr to convert logical insert (reserved) position
 * to physical position in WAL. It always adds SizeOfXLogShortPHD:
 *		seg_offset += fullpages * XLOG_BLCKSZ + bytesleft + SizeOfXLogShortPHD;
 * so even if there are no records on the page, offset will be SizeOfXLogShortPHD.
 * It may cause problems with XLogFlush. So return pointer backward to the origin of the page.
 */
static XLogRecPtr
nm_adjust_lsn(XLogRecPtr lsn)
{
	/*
	 * If lsn points to the beging of first record on page or segment, then
	 * "return" it back to the page origin
	 */
	if ((lsn & (XLOG_BLCKSZ - 1)) == SizeOfXLogShortPHD)
	{
		lsn -= SizeOfXLogShortPHD;
	}
	else if ((lsn & (wal_segment_size - 1)) == SizeOfXLogLongPHD)
	{
		lsn -= SizeOfXLogLongPHD;
	}
	return lsn;
}


/*
 * Since PG17 we use vetorized version,
 * so add compatibility function for older versions
 */
#if PG_MAJORVERSION_NUM < 17
static void
GetLastWrittenLSNv(NRelFileInfo relfilenode, ForkNumber forknum,
				   BlockNumber blkno, int nblocks, XLogRecPtr *lsns)
{
	lsns[0] = GetLastWrittenLSN(relfilenode, forknum, blkno);
}
#endif

/*
 * Return LSN for requesting pages and number of blocks from page server
 */
static void
neon_get_request_lsns(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blkno,
					  neon_request_lsns *output, BlockNumber nblocks,
					  const bits8 *mask)
{
	XLogRecPtr	last_written_lsns[PG_IOV_MAX];

	Assert(nblocks <= PG_IOV_MAX);

	GetLastWrittenLSNv(rinfo, forknum, blkno, (int) nblocks, last_written_lsns);

	for (int i = 0; i < nblocks; i++)
	{
		last_written_lsns[i] = nm_adjust_lsn(last_written_lsns[i]);
		Assert(last_written_lsns[i] != InvalidXLogRecPtr);
	}

	if (RecoveryInProgress())
	{
		/*---
		 * In broad strokes, a replica always requests the page at the current
		 * replay LSN. But looking closer, what exactly is the replay LSN? Is
		 * it the last replayed record, or the record being replayed? And does
		 * the startup process performing the replay need to do something
		 * differently than backends running queries? Let's take a closer look
		 * at the different scenarios:
		 *
		 * 1. Startup process reads a page, last_written_lsn is old.
		 *
		 * Read the old version of the page. We will apply the WAL record on
		 * it to bring it up-to-date.
		 *
		 * We could read the new version, with the changes from this WAL
		 * record already applied, to offload the work of replaying the record
		 * to the pageserver. The pageserver might not have received the WAL
		 * record yet, though, so a read of the old page version and applying
		 * the record ourselves is likely faster. Also, the redo function
		 * might be surprised if the changes have already applied. That's
		 * normal during crash recovery, but not in hot standby.
		 *
		 * 2. Startup process reads a page, last_written_lsn == record we're
		 *    replaying.
		 *
		 * Can this happen? There are a few theoretical cases when it might:
		 *
		 * A) The redo function reads the same page twice. We had already read
		 *    and applied the changes once, and now we're reading it for the
		 *    second time.  That would be a rather silly thing for a redo
		 *    function to do, and I'm not aware of any that would do it.
		 *
		 * B) The redo function modifies multiple pages, and it already
		 *    applied the changes to one of the pages, released the lock on
		 *    it, and is now reading a second page.  Furthermore, the first
		 *    page was already evicted from the buffer cache, and also from
		 *    the last-written LSN cache, so that the per-relation or global
		 *    last-written LSN was already updated. All the WAL redo functions
		 *    hold the locks on pages that they modify, until all the changes
		 *    have been modified (?), which would make that impossible.
		 *    However, we skip the locking, if the page isn't currently in the
		 *    page cache (see neon_redo_read_buffer_filter below).
		 *
		 * Even if the one of the above cases were possible in theory, they
		 * would also require the pages being modified by the redo function to
		 * be immediately evicted from the page cache.
		 *
		 * So this probably does not happen in practice. But if it does, we
		 * request the new version, including the changes from the record
		 * being replayed. That seems like the correct behavior in any case.
		 *
		 * 3. Backend process reads a page with old last-written LSN
		 *
		 * Nothing special here. Read the old version.
		 *
		 * 4. Backend process reads a page with last_written_lsn == record being replayed
		 *
		 * This can happen, if the redo function has started to run, and saw
		 * that the page isn't present in the page cache (see
		 * neon_redo_read_buffer_filter below).  Normally, in a normal
		 * Postgres server, the redo function would hold a lock on the page,
		 * so we would get blocked waiting the redo function to release the
		 * lock. To emulate that, wait for the WAL replay of the record to
		 * finish.
		 */
		/* Request the page at the end of the last fully replayed LSN. */
		XLogRecPtr replay_lsn = GetXLogReplayRecPtr(NULL);

		for (int i = 0; i < nblocks; i++)
		{
			neon_request_lsns *result = &output[i];
			XLogRecPtr	last_written_lsn = last_written_lsns[i];

			if (PointerIsValid(mask) && !BITMAP_ISSET(mask, i))
				continue;

			if (last_written_lsn > replay_lsn)
			{
				/* GetCurrentReplayRecPtr was introduced in v15 */
#if PG_VERSION_NUM >= 150000
				Assert(last_written_lsn == GetCurrentReplayRecPtr(NULL));
#endif

				/*
				 * Cases 2 and 4. If this is a backend (case 4), the
				 * neon_read_at_lsn() call later will wait for the WAL record to be
				 * fully replayed.
				 */
				result->request_lsn = last_written_lsn;
			}
			else
			{
				/* cases 1 and 3 */
				result->request_lsn = replay_lsn;
			}

			result->not_modified_since = last_written_lsn;
			result->effective_request_lsn = result->request_lsn;
			Assert(last_written_lsn <= result->request_lsn);

			neon_log(DEBUG1, "neon_get_request_lsns request lsn %X/%X, not_modified_since %X/%X",
					 LSN_FORMAT_ARGS(result->request_lsn), LSN_FORMAT_ARGS(result->not_modified_since));
		}
	}
	else
	{
		XLogRecPtr	flushlsn;
#if PG_VERSION_NUM >= 150000
		flushlsn = GetFlushRecPtr(NULL);
#else
		flushlsn = GetFlushRecPtr();
#endif

		for (int i = 0; i < nblocks; i++)
		{
			neon_request_lsns *result = &output[i];
			XLogRecPtr	last_written_lsn = last_written_lsns[i];

			if (PointerIsValid(mask) && !BITMAP_ISSET(mask, i))
				continue;
			/*
			 * Use the latest LSN that was evicted from the buffer cache as the
			 * 'not_modified_since' hint. Any pages modified by later WAL records
			 * must still in the buffer cache, so our request cannot concern
			 * those.
			 */
			neon_log(DEBUG1, "neon_get_request_lsns GetLastWrittenLSN lsn %X/%X",
					 LSN_FORMAT_ARGS(last_written_lsn));

			/*
			 * Is it possible that the last-written LSN is ahead of last flush
			 * LSN? Generally not, we shouldn't evict a page from the buffer cache
			 * before all its modifications have been safely flushed. That's the
			 * "WAL before data" rule. However, such case does exist at index
			 * building, _bt_blwritepage logs the full page without flushing WAL
			 * before smgrextend (files are fsynced before build ends).
			 */
			if (last_written_lsn > flushlsn)
			{
				neon_log(DEBUG5, "last-written LSN %X/%X is ahead of last flushed LSN %X/%X",
						 LSN_FORMAT_ARGS(last_written_lsn),
						 LSN_FORMAT_ARGS(flushlsn));
				XLogFlush(last_written_lsn);
				flushlsn = last_written_lsn;
			}

			/*
			 * Request the very latest version of the page. In principle we
			 * want to read the page at the current insert LSN, and we could
			 * use that value in the request. However, there's a corner case
			 * with pageserver's garbage collection. If the GC horizon is
			 * set to a very small value, it's possible that by the time
			 * that the pageserver processes our request, the GC horizon has
			 * already moved past the LSN we calculate here. Standby servers
			 * always have that problem as the can always lag behind the
			 * primary, but for the primary we can avoid it by always
			 * requesting the latest page, by setting request LSN to
			 * UINT64_MAX.
			 *
			 * Remember the current LSN, however, so that we can later
			 * correctly determine if the response to the request is still
			 * valid. The most up-to-date LSN we could use for that purpose
			 * would be the current insert LSN, but to avoid the overhead of
			 * looking it up, use 'flushlsn' instead. This relies on the
			 * assumption that if the page was modified since the last WAL
			 * flush, it should still be in the buffer cache, and we
			 * wouldn't be requesting it.
			 */
			result->request_lsn = UINT64_MAX;
			result->not_modified_since = last_written_lsn;
			result->effective_request_lsn = flushlsn;
		}
	}
}

/*
 *  neon_prefetch_response_usable -- Can a new request be satisfied by old one?
 *
 * This is used to check if the response to a prefetch request can be used to
 * satisfy a page read now.
 */
static bool
neon_prefetch_response_usable(neon_request_lsns *request_lsns,
							  PrefetchRequest *slot)
{
	/* sanity check the LSN's on the old and the new request */
	Assert(request_lsns->request_lsn >= request_lsns->not_modified_since);
	Assert(request_lsns->effective_request_lsn >= request_lsns->not_modified_since);
	Assert(request_lsns->effective_request_lsn <= request_lsns->request_lsn);
	Assert(slot->request_lsns.request_lsn >= slot->request_lsns.not_modified_since);
	Assert(slot->request_lsns.effective_request_lsn >= slot->request_lsns.not_modified_since);
	Assert(slot->request_lsns.effective_request_lsn <= slot->request_lsns.request_lsn);
	Assert(slot->status != PRFS_UNUSED);

	/*
	 * The new request's LSN should never be older than the old one.  This
	 * could be an Assert, except that for testing purposes, we do provide an
	 * interface in neon_test_utils to fetch pages at arbitary LSNs, which
	 * violates this.
	 *
	 * Similarly, the not_modified_since value calculated for a page should
	 * never move backwards. This assumption is a bit fragile; if we updated
	 * the last-written cache when we read in a page, for example, then it
	 * might. But as the code stands, it should not.
	 *
	 * (If two backends issue a request at the same time, they might race and
	 * calculate LSNs "out of order" with each other, but the prefetch queue
	 * is backend-private at the moment.)
	 */
	if (request_lsns->effective_request_lsn < slot->request_lsns.effective_request_lsn ||
		request_lsns->not_modified_since < slot->request_lsns.not_modified_since)
	{
		ereport(LOG,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg(NEON_TAG "request with unexpected LSN after prefetch"),
				 errdetail("Request %X/%X not_modified_since %X/%X, prefetch %X/%X not_modified_since %X/%X)",
						   LSN_FORMAT_ARGS(request_lsns->effective_request_lsn),
						   LSN_FORMAT_ARGS(request_lsns->not_modified_since),
						   LSN_FORMAT_ARGS(slot->request_lsns.effective_request_lsn),
						   LSN_FORMAT_ARGS(slot->request_lsns.not_modified_since))));
		return false;
	}

	/*---
	 * Each request to the pageserver has three LSN values associated with it:
	 * `not_modified_since`, `request_lsn`, and 'effective_request_lsn'.
	 * `not_modified_since` and `request_lsn` are sent to the pageserver, but
	 * in the primary node, we always use UINT64_MAX as the `request_lsn`, so
	 * we remember `effective_request_lsn` separately. In a primary,
	 * `effective_request_lsn` is the last flush WAL position when the request
	 * was sent to the pageserver. That's logically the LSN that we are
	 * requesting the page at, but we send UINT64_MAX to the pageserver so
	 * that if the GC horizon advances past that position, we still get a
	 * valid response instead of an error.
	 *
	 * To determine whether a response to a GetPage request issued earlier is
	 * still valid to satisfy a new page read, we look at the
	 * (not_modified_since, effective_request_lsn] range of the request. It is
	 * effectively a claim that the page has not been modified between those
	 * LSNs.  If the range of the old request in the queue overlaps with the
	 * new request, we know that the page hasn't been modified in the union of
	 * the ranges. We can use the response to old request to satisfy the new
	 * request in that case. For example:
	 *
	 *              100      500
	 * Old request:  +--------+
	 *
	 *                     400      800
	 * New request:         +--------+
	 *
	 * The old request claims that the page was not modified between LSNs 100
	 * and 500, and the second claims that it was not modified between 400 and
	 * 800. Together they mean that the page was not modified between 100 and
	 * 800. Therefore the response to the old request is also valid for the
	 * new request.
	 *
	 * This logic also holds at the boundary case that the old request's LSN
	 * matches the new request's not_modified_since LSN exactly:
	 *
	 *              100      500
	 * Old request:  +--------+
	 *
	 *                       500      900
	 * New request:           +--------+
	 *
	 * The response to the old request is the page as it was at LSN 500, and
	 * the page hasn't been changed in the range (500, 900], therefore the
	 * response is valid also for the new request.
	 */

	/* this follows from the checks above */
	Assert(request_lsns->effective_request_lsn >= slot->request_lsns.not_modified_since);

	return request_lsns->not_modified_since <= slot->request_lsns.effective_request_lsn;
}

/*
 *	neon_exists() -- Does the physical file exist?
 */
static bool
neon_exists(SMgrRelation reln, ForkNumber forkNum)
{
	bool		exists;
	NeonResponse *resp;
	BlockNumber n_blocks;
	neon_request_lsns request_lsns;

	switch (reln->smgr_relpersistence)
	{
		case 0:

			/*
			 * We don't know if it's an unlogged rel stored locally, or
			 * permanent rel stored in the page server. First check if it
			 * exists locally. If it does, great. Otherwise check if it exists
			 * in the page server.
			 */
			if (mdexists(reln, forkNum))
				return true;
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdexists(reln, forkNum);

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (get_cached_relsize(InfoFromSMgrRel(reln), forkNum, &n_blocks))
	{
		return true;
	}

	/*
	 * \d+ on a view calls smgrexists with 0/0/0 relfilenode. The page server
	 * will error out if you check that, because the whole dbdir for
	 * tablespace 0, db 0 doesn't exists. We possibly should change the page
	 * server to accept that and return 'false', to be consistent with
	 * mdexists(). But we probably also should fix pg_table_size() to not call
	 * smgrexists() with bogus relfilenode.
	 *
	 * For now, handle that special case here.
	 */
#if PG_MAJORVERSION_NUM >= 16
	if (reln->smgr_rlocator.locator.spcOid == 0 &&
		reln->smgr_rlocator.locator.dbOid == 0 &&
		reln->smgr_rlocator.locator.relNumber == 0)
#else
	if (reln->smgr_rnode.node.spcNode == 0 &&
		reln->smgr_rnode.node.dbNode == 0 &&
		reln->smgr_rnode.node.relNode == 0)
#endif
	{
		return false;
	}

	neon_get_request_lsns(InfoFromSMgrRel(reln), forkNum,
						  REL_METADATA_PSEUDO_BLOCKNO, &request_lsns, 1, NULL);
	{
		NeonExistsRequest request = {
			.hdr.tag = T_NeonExistsRequest,
			.hdr.reqid = GENERATE_REQUEST_ID(),
			.hdr.lsn = request_lsns.request_lsn,
			.hdr.not_modified_since = request_lsns.not_modified_since,
			.rinfo = InfoFromSMgrRel(reln),
			.forknum = forkNum
		};

		resp = page_server_request(&request);

		switch (resp->tag)
		{
			case T_NeonExistsResponse:
			{
				NeonExistsResponse* exists_resp = (NeonExistsResponse *) resp;
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr) ||
						!RelFileInfoEquals(exists_resp->req.rinfo, request.rinfo) ||
						exists_resp->req.forknum != request.forknum)
					{
						NEON_PANIC_CONNECTION_STATE(-1, PANIC,
													"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u} to exits request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u}",
													resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), RelFileInfoFmt(exists_resp->req.rinfo), exists_resp->req.forknum,
													request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since), RelFileInfoFmt(request.rinfo), request.forknum);
					}
				}
				exists = exists_resp->exists;
				break;
			}
			case T_NeonErrorResponse:
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr))
					{
						elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match exists request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
							 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
							 request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since));
					}
				}
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg(NEON_TAG "[reqid %lx] could not read relation existence of rel %u/%u/%u.%u from page server at lsn %X/%08X",
								resp->reqid,
								RelFileInfoFmt(InfoFromSMgrRel(reln)),
								forkNum,
								LSN_FORMAT_ARGS(request_lsns.effective_request_lsn)),
						 errdetail("page server returned error: %s",
								   ((NeonErrorResponse *) resp)->message)));
				break;

			default:
				NEON_PANIC_CONNECTION_STATE(-1, PANIC,
											"Expected Exists (0x%02x) or Error (0x%02x) response to ExistsRequest, but got 0x%02x",
											T_NeonExistsResponse, T_NeonErrorResponse, resp->tag);
		}
		pfree(resp);
	}
	return exists;
}

/*
 *	neon_create() -- Create a new relation on neond storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
static void
neon_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrcreate() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdcreate(reln, forkNum, isRedo);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_log(SmgrTrace, "Create relation %u/%u/%u.%u",
		 RelFileInfoFmt(InfoFromSMgrRel(reln)),
		 forkNum);

	/*
	 * Newly created relation is empty, remember that in the relsize cache.
	 *
	 * Note that in REDO, this is called to make sure the relation fork
	 * exists, but it does not truncate the relation. So, we can only update
	 * the relsize if it didn't exist before.
	 *
	 * Also, in redo, we must make sure to update the cached size of the
	 * relation, as that is the primary source of truth for REDO's file length
	 * considerations, and as file extension isn't (perfectly) logged, we need
	 * to take care of that before we hit file size checks.
	 *
	 * FIXME: This is currently not just an optimization, but required for
	 * correctness. Postgres can call smgrnblocks() on the newly-created
	 * relation. Currently, we don't call SetLastWrittenLSN() when a new
	 * relation created, so if we didn't remember the size in the relsize
	 * cache, we might call smgrnblocks() on the newly-created relation before
	 * the creation WAL record hass been received by the page server.
	 */
	if (isRedo)
	{
		update_cached_relsize(InfoFromSMgrRel(reln), forkNum, 0);
		get_cached_relsize(InfoFromSMgrRel(reln), forkNum,
						   &reln->smgr_cached_nblocks[forkNum]);
	}
	else
		set_cached_relsize(InfoFromSMgrRel(reln), forkNum, 0);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdcreate(reln, forkNum, isRedo);
#endif
}

/*
 *	neon_unlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
static void
neon_unlink(NRelFileInfoBackend rinfo, ForkNumber forkNum, bool isRedo)
{
	/*
	 * Might or might not exist locally, depending on whether it's an unlogged
	 * or permanent relation (or if DEBUG_COMPARE_LOCAL is set). Try to
	 * unlink, it won't do any harm if the file doesn't exist.
	 */
	mdunlink(rinfo, forkNum, isRedo);
	if (!NRelFileInfoBackendIsTemp(rinfo))
	{
		forget_cached_relsize(InfoFromNInfoB(rinfo), forkNum);
	}
}

/*
 *	neon_extend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
static void
#if PG_MAJORVERSION_NUM < 16
neon_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
			char *buffer, bool skipFsync)
#else
neon_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
			const void *buffer, bool skipFsync)
#endif
{
	XLogRecPtr	lsn;
	BlockNumber n_blocks = 0;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrextend() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdextend(reln, forkNum, blkno, buffer, skipFsync);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/*
	 * Check that the cluster size limit has not been exceeded.
	 *
	 * Temporary and unlogged relations are not included in the cluster size
	 * measured by the page server, so ignore those. Autovacuum processes are
	 * also exempt.
	 */
	if (max_cluster_size > 0 &&
		reln->smgr_relpersistence == RELPERSISTENCE_PERMANENT &&
		!AmAutoVacuumWorkerProcess())
	{
		uint64		current_size = GetNeonCurrentClusterSize();

		if (current_size >= ((uint64) max_cluster_size) * 1024 * 1024)
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("could not extend file because project size limit (%d MB) has been exceeded",
							max_cluster_size),
					 errhint("This limit is defined externally by the project size limit, and internally by neon.max_cluster_size GUC")));
	}

	/*
	 * Usually Postgres doesn't extend relation on more than one page (leaving
	 * holes). But this rule is violated in PG-15 where
	 * CreateAndCopyRelationData call smgrextend for destination relation n
	 * using size of source relation
	 */
	n_blocks = neon_nblocks(reln, forkNum);
	while (n_blocks < blkno)
		neon_wallog_page(reln, forkNum, n_blocks++, buffer, true);

	neon_wallog_page(reln, forkNum, blkno, buffer, false);
	set_cached_relsize(InfoFromSMgrRel(reln), forkNum, blkno + 1);

	lsn = PageGetLSN((Page) buffer);
	neon_log(SmgrTrace, "smgrextend called for %u/%u/%u.%u blk %u, page LSN: %X/%08X",
		 RelFileInfoFmt(InfoFromSMgrRel(reln)),
		 forkNum, blkno,
		 (uint32) (lsn >> 32), (uint32) lsn);

	lfc_write(InfoFromSMgrRel(reln), forkNum, blkno, buffer);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdextend(reln, forkNum, blkno, buffer, skipFsync);
#endif

	/*
	 * smgr_extend is often called with an all-zeroes page, so
	 * lsn==InvalidXLogRecPtr. An smgr_write() call will come for the buffer
	 * later, after it has been initialized with the real page contents, and
	 * it is eventually evicted from the buffer cache. But we need a valid LSN
	 * to the relation metadata update now.
	 */
	if (lsn == InvalidXLogRecPtr)
	{
		lsn = GetXLogInsertRecPtr();
		SetLastWrittenLSNForBlock(lsn, InfoFromSMgrRel(reln), forkNum, blkno);
	}
	SetLastWrittenLSNForRelation(lsn, InfoFromSMgrRel(reln), forkNum);
}

#if PG_MAJORVERSION_NUM >= 16
static void
neon_zeroextend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blocknum,
				int nblocks, bool skipFsync)
{
	const PGAlignedBlock buffer = {0};
	int			remblocks = nblocks;
	XLogRecPtr	lsn = 0;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrextend() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdzeroextend(reln, forkNum, blocknum, nblocks, skipFsync);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (max_cluster_size > 0 &&
		reln->smgr_relpersistence == RELPERSISTENCE_PERMANENT &&
		!AmAutoVacuumWorkerProcess())
	{
		uint64		current_size = GetNeonCurrentClusterSize();

		if (current_size >= ((uint64) max_cluster_size) * 1024 * 1024)
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("could not extend file because project size limit (%d MB) has been exceeded",
							max_cluster_size),
					 errhint("This limit is defined by neon.max_cluster_size GUC")));
	}

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber or larger.
	 */
	if ((uint64) blocknum + nblocks >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg(NEON_TAG "cannot extend file \"%s\" beyond %u blocks",
						relpath(reln->smgr_rlocator, forkNum),
						InvalidBlockNumber)));

	/* Don't log any pages if we're not allowed to do so. */
	if (!XLogInsertAllowed())
		return;

	/* ensure we have enough xlog buffers to log max-sized records */
	XLogEnsureRecordSpace(Min(remblocks, (XLR_MAX_BLOCK_ID - 1)), 0);

	/*
	 * Iterate over all the pages. They are collected into batches of
	 * XLR_MAX_BLOCK_ID pages, and a single WAL-record is written for each
	 * batch.
	 */
	while (remblocks > 0)
	{
		int			count = Min(remblocks, XLR_MAX_BLOCK_ID);

		XLogBeginInsert();

		for (int i = 0; i < count; i++)
			XLogRegisterBlock(i, &InfoFromSMgrRel(reln), forkNum, blocknum + i,
							  (char *) buffer.data, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

		lsn = XLogInsert(RM_XLOG_ID, XLOG_FPI);

		for (int i = 0; i < count; i++)
		{
			lfc_write(InfoFromSMgrRel(reln), forkNum, blocknum + i, buffer.data);
			SetLastWrittenLSNForBlock(lsn, InfoFromSMgrRel(reln), forkNum,
									  blocknum + i);
		}

		blocknum += count;
		remblocks -= count;
	}

	Assert(lsn != 0);

	SetLastWrittenLSNForRelation(lsn, InfoFromSMgrRel(reln), forkNum);
	set_cached_relsize(InfoFromSMgrRel(reln), forkNum, blocknum);
}
#endif

/*
 *  neon_open() -- Initialize newly-opened relation.
 */
static void
neon_open(SMgrRelation reln)
{
	/*
	 * We don't have anything special to do here. Call mdopen() to let md.c
	 * initialize itself. That's only needed for temporary or unlogged
	 * relations, but it's dirt cheap so do it always to make sure the md
	 * fields are initialized, for debugging purposes if nothing else.
	 */
	mdopen(reln);

	/* no work */
	neon_log(SmgrTrace, "open noop");
}

/*
 *	neon_close() -- Close the specified relation, if it isn't closed already.
 */
static void
neon_close(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * Let md.c close it, if it had it open. Doesn't hurt to do this even for
	 * permanent relations that have no local storage.
	 */
	mdclose(reln, forknum);
}


#if PG_MAJORVERSION_NUM >= 17
/*
 *	neon_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
static bool
neon_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  int nblocks)
{
	uint64		ring_index PG_USED_FOR_ASSERTS_ONLY;
	BufferTag	tag;

	switch (reln->smgr_relpersistence)
	{
		case 0:					/* probably shouldn't happen, but ignore it */
		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdprefetch(reln, forknum, blocknum, nblocks);

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	tag.spcOid = reln->smgr_rlocator.locator.spcOid;
	tag.dbOid = reln->smgr_rlocator.locator.dbOid;
	tag.relNumber = reln->smgr_rlocator.locator.relNumber;
	tag.forkNum = forknum;

	while (nblocks > 0)
	{
		int		iterblocks = Min(nblocks, PG_IOV_MAX);
		bits8		lfc_present[PG_IOV_MAX / 8];
		memset(lfc_present, 0, sizeof(lfc_present));

		if (lfc_cache_containsv(InfoFromSMgrRel(reln), forknum, blocknum,
								iterblocks, lfc_present) == iterblocks)
		{
			nblocks -= iterblocks;
			blocknum += iterblocks;
			continue;
		}

		tag.blockNum = blocknum;
		
		for (int i = 0; i < PG_IOV_MAX / 8; i++)
			lfc_present[i] = ~(lfc_present[i]);

		ring_index = prefetch_register_bufferv(tag, NULL, iterblocks,
											   lfc_present, true);
		nblocks -= iterblocks;
		blocknum += iterblocks;

		Assert(ring_index < MyPState->ring_unused &&
			   MyPState->ring_last <= ring_index);
	}

	return false;
}


#else /* PG_MAJORVERSION_NUM >= 17 */
/*
 *	neon_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
static bool
neon_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	uint64		ring_index PG_USED_FOR_ASSERTS_ONLY;
	BufferTag	tag;

	switch (reln->smgr_relpersistence)
	{
		case 0:					/* probably shouldn't happen, but ignore it */
		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdprefetch(reln, forknum, blocknum);

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (lfc_cache_contains(InfoFromSMgrRel(reln), forknum, blocknum))
		return false;

	tag.forkNum = forknum;
	tag.blockNum = blocknum;

	CopyNRelFileInfoToBufTag(tag, InfoFromSMgrRel(reln));

	ring_index = prefetch_register_bufferv(tag, NULL, 1, NULL, true);

	Assert(ring_index < MyPState->ring_unused &&
		   MyPState->ring_last <= ring_index);

	return false;
}
#endif /* PG_MAJORVERSION_NUM < 17 */


/*
 * neon_writeback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
static void
neon_writeback(SMgrRelation reln, ForkNumber forknum,
			   BlockNumber blocknum, BlockNumber nblocks)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* mdwriteback() does nothing if the file doesn't exist */
			mdwriteback(reln, forknum, blocknum, nblocks);
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdwriteback(reln, forknum, blocknum, nblocks);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/*
	 * TODO: WAL sync up to lwLsn for the indicated blocks
	 * Without that sync, writeback doesn't actually guarantee the data is
	 * persistently written, which does seem to be one of the assumed
	 * properties of this smgr API call.
	 */
	neon_log(SmgrTrace, "writeback noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwriteback(reln, forknum, blocknum, nblocks);
#endif
}

static void
#if PG_MAJORVERSION_NUM < 16
neon_read_at_lsnv(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber base_blockno, neon_request_lsns *request_lsns,
				  char **buffers, BlockNumber nblocks, const bits8 *mask)
#else
neon_read_at_lsnv(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber base_blockno, neon_request_lsns *request_lsns,
				  void **buffers, BlockNumber nblocks, const bits8 *mask)
#endif
{
	NeonResponse *resp;
	uint64		ring_index;
	PrfHashEntry *entry;
	PrefetchRequest *slot;
	PrefetchRequest hashkey;

	Assert(PointerIsValid(request_lsns));
	Assert(nblocks >= 1);

	/*
	 * Use an intermediate PrefetchRequest struct as the hash key to ensure
	 * correct alignment and that the padding bytes are cleared.
	 */
	memset(&hashkey.buftag, 0, sizeof(BufferTag));
	CopyNRelFileInfoToBufTag(hashkey.buftag, rinfo);
	hashkey.buftag.forkNum = forkNum;
	hashkey.buftag.blockNum = base_blockno;

	/*
	 * The redo process does not lock pages that it needs to replay but are
	 * not in the shared buffers, so a concurrent process may request the page
	 * after redo has decided it won't redo that page and updated the LwLSN
	 * for that page. If we're in hot standby we need to take care that we
	 * don't return until after REDO has finished replaying up to that LwLSN,
	 * as the page should have been locked up to that point.
	 *
	 * See also the description on neon_redo_read_buffer_filter below.
	 *
	 * NOTE: It is possible that the WAL redo process will still do IO due to
	 * concurrent failed read IOs. Those IOs should never have a request_lsn
	 * that is as large as the WAL record we're currently replaying, if it
	 * weren't for the behaviour of the LwLsn cache that uses the highest
	 * value of the LwLsn cache when the entry is not found.
	 */
	prefetch_register_bufferv(hashkey.buftag, request_lsns, nblocks, mask, false);

	for (int i = 0; i < nblocks; i++)
	{
		void	   *buffer = buffers[i];
		BlockNumber blockno = base_blockno + i;
		neon_request_lsns *reqlsns = &request_lsns[i];
		TimestampTz		start_ts, end_ts;

		if (PointerIsValid(mask) && !BITMAP_ISSET(mask, i))
			continue;

		start_ts = GetCurrentTimestamp();

		if (RecoveryInProgress() && MyBackendType != B_STARTUP)
			XLogWaitForReplayOf(reqlsns[0].request_lsn);

		/*
		 * Try to find prefetched page in the list of received pages.
		 */
Retry:
		hashkey.buftag.blockNum = blockno;
		entry = prfh_lookup(MyPState->prf_hash, &hashkey);

		if (entry != NULL)
		{
			slot = entry->slot;
			if (neon_prefetch_response_usable(reqlsns, slot))
			{
				ring_index = slot->my_ring_index;
			}
			else
			{
				/*
				 * Cannot use this prefetch, discard it
				 *
				 * We can't drop cache for not-yet-received requested items. It is
				 * unlikely this happens, but it can happen if prefetch distance
				 * is large enough and a backend didn't consume all prefetch
				 * requests.
				 */
				if (slot->status == PRFS_REQUESTED)
				{
					if (!prefetch_wait_for(slot->my_ring_index))
						goto Retry;
				}
				/* drop caches */
				prefetch_set_unused(slot->my_ring_index);
				pgBufferUsage.prefetch.expired += 1;
				MyNeonCounters->getpage_prefetch_discards_total++;
				/* make it look like a prefetch cache miss */
				entry = NULL;
			}
		}

		do
		{
			if (entry == NULL)
			{
				ring_index = prefetch_register_bufferv(hashkey.buftag, reqlsns, 1, NULL, false);
				Assert(ring_index != UINT64_MAX);
				slot = GetPrfSlot(ring_index);
			}
			else
			{
				/*
				 * Empty our reference to the prefetch buffer's hash entry. When
				 * we wait for prefetches, the entry reference is invalidated by
				 * potential updates to the hash, and when we reconnect to the
				 * pageserver the prefetch we're waiting for may be dropped, in
				 * which case we need to retry and take the branch above.
				 */
				entry = NULL;
			}

			Assert(slot->my_ring_index == ring_index);
			Assert(MyPState->ring_last <= ring_index &&
				   MyPState->ring_unused > ring_index);
			Assert(slot->status != PRFS_UNUSED);
			Assert(GetPrfSlot(ring_index) == slot);

		} while (!prefetch_wait_for(ring_index));

		Assert(slot->status == PRFS_RECEIVED);
		Assert(memcmp(&hashkey.buftag, &slot->buftag, sizeof(BufferTag)) == 0);
		Assert(hashkey.buftag.blockNum == base_blockno + i);

		resp = slot->response;

		switch (resp->tag)
		{
			case T_NeonGetPageResponse:
			{
				NeonGetPageResponse* getpage_resp = (NeonGetPageResponse *) resp;
				if (neon_protocol_version >= 3)
				{
					if (resp->reqid != slot->reqid ||
						resp->lsn != slot->request_lsns.request_lsn ||
						resp->not_modified_since != slot->request_lsns.not_modified_since ||
						!RelFileInfoEquals(getpage_resp->req.rinfo, rinfo) ||
						getpage_resp->req.forknum != forkNum ||
						getpage_resp->req.blkno != base_blockno + i)
					{
						NEON_PANIC_CONNECTION_STATE(-1, PANIC,
													"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u, block=%u} to get page request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u, block=%u}",
													resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), RelFileInfoFmt(getpage_resp->req.rinfo), getpage_resp->req.forknum, getpage_resp->req.blkno,
													slot->reqid, LSN_FORMAT_ARGS(slot->request_lsns.request_lsn), LSN_FORMAT_ARGS(slot->request_lsns.not_modified_since), RelFileInfoFmt(rinfo), forkNum, base_blockno + i);
					}
				}
				memcpy(buffer, getpage_resp->page, BLCKSZ);
				lfc_write(rinfo, forkNum, blockno, buffer);
				break;
			}
			case T_NeonErrorResponse:
				if (neon_protocol_version >= 3)
				{
					if (resp->reqid != slot->reqid ||
						resp->lsn != slot->request_lsns.request_lsn ||
						resp->not_modified_since != slot->request_lsns.not_modified_since)
					{
						elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match get relsize request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
							 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
							 slot->reqid, LSN_FORMAT_ARGS(slot->request_lsns.request_lsn), LSN_FORMAT_ARGS(slot->request_lsns.not_modified_since));
					}
				}
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg(NEON_TAG "[shard %d, reqid %lx] could not read block %u in rel %u/%u/%u.%u from page server at lsn %X/%08X",
								slot->shard_no, resp->reqid, blockno, RelFileInfoFmt(rinfo),
								forkNum, LSN_FORMAT_ARGS(reqlsns->effective_request_lsn)),
						 errdetail("page server returned error: %s",
								   ((NeonErrorResponse *) resp)->message)));
				break;
			default:
				NEON_PANIC_CONNECTION_STATE(slot->shard_no, PANIC,
											"Expected GetPage (0x%02x) or Error (0x%02x) response to GetPageRequest, but got 0x%02x",
											T_NeonGetPageResponse, T_NeonErrorResponse, resp->tag);
		}

		/* buffer was used, clean up for later reuse */
		prefetch_set_unused(ring_index);
		prefetch_cleanup_trailing_unused();

		end_ts = GetCurrentTimestamp();
		inc_getpage_wait(end_ts >= start_ts ? (end_ts - start_ts) : 0);
	}
}

/*
 * While function is defined in the neon extension it's used within neon_test_utils directly.
 * To avoid breaking tests in the runtime please keep function signature in sync.
 */
void
#if PG_MAJORVERSION_NUM < 16
neon_read_at_lsn(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
				 neon_request_lsns request_lsns, char *buffer)
#else
neon_read_at_lsn(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
				 neon_request_lsns request_lsns, void *buffer)
#endif
{
	neon_read_at_lsnv(rinfo, forkNum, blkno, &request_lsns, &buffer, 1, NULL);
}

#if PG_MAJORVERSION_NUM < 17
/*
 *	neon_read() -- Read the specified block from a relation.
 */
#if PG_MAJORVERSION_NUM < 16
static void
neon_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno, char *buffer)
#else
static void
neon_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno, void *buffer)
#endif
{
	neon_request_lsns request_lsns;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrread() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdread(reln, forkNum, blkno, buffer);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/* Try to read from local file cache */
	if (lfc_read(InfoFromSMgrRel(reln), forkNum, blkno, buffer))
	{
		MyNeonCounters->file_cache_hits_total++;
		return;
	}

	neon_get_request_lsns(InfoFromSMgrRel(reln), forkNum, blkno, &request_lsns, 1, NULL);
	neon_read_at_lsn(InfoFromSMgrRel(reln), forkNum, blkno, request_lsns, buffer);

#ifdef DEBUG_COMPARE_LOCAL
	if (forkNum == MAIN_FORKNUM && IS_LOCAL_REL(reln))
	{
		char		pageserver_masked[BLCKSZ];
		char		mdbuf[BLCKSZ];
		char		mdbuf_masked[BLCKSZ];

		mdread(reln, forkNum, blkno, mdbuf);

		memcpy(pageserver_masked, buffer, BLCKSZ);
		memcpy(mdbuf_masked, mdbuf, BLCKSZ);

		if (PageIsNew((Page) mdbuf))
		{
			if (!PageIsNew((Page) pageserver_masked))
			{
				neon_log(PANIC, "page is new in MD but not in Page Server at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
					 blkno,
					 RelFileInfoFmt(InfoFromSMgrRel(reln)),
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(buffer));
			}
		}
		else if (PageIsNew((Page) buffer))
		{
			neon_log(PANIC, "page is new in Page Server but not in MD at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
				 blkno,
				 RelFileInfoFmt(InfoFromSMgrRel(reln)),
				 forkNum,
				 (uint32) (request_lsn >> 32), (uint32) request_lsn,
				 hexdump_page(mdbuf));
		}
		else if (PageGetSpecialSize(mdbuf) == 0)
		{
			/* assume heap */
			RmgrTable[RM_HEAP_ID].rm_mask(mdbuf_masked, blkno);
			RmgrTable[RM_HEAP_ID].rm_mask(pageserver_masked, blkno);

			if (memcmp(mdbuf_masked, pageserver_masked, BLCKSZ) != 0)
			{
				neon_log(PANIC, "heap buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
					 blkno,
					 RelFileInfoFmt(InfoFromSMgrRel(reln)),
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(mdbuf_masked),
					 hexdump_page(pageserver_masked));
			}
		}
		else if (PageGetSpecialSize(mdbuf) == MAXALIGN(sizeof(BTPageOpaqueData)))
		{
			if (((BTPageOpaqueData *) PageGetSpecialPointer(mdbuf))->btpo_cycleid < MAX_BT_CYCLE_ID)
			{
				/* assume btree */
				RmgrTable[RM_BTREE_ID].rm_mask(mdbuf_masked, blkno);
				RmgrTable[RM_BTREE_ID].rm_mask(pageserver_masked, blkno);

				if (memcmp(mdbuf_masked, pageserver_masked, BLCKSZ) != 0)
				{
					neon_log(PANIC, "btree buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
						 blkno,
						 RelFileInfoFmt(InfoFromSMgrRel(reln)),
						 forkNum,
						 (uint32) (request_lsn >> 32), (uint32) request_lsn,
						 hexdump_page(mdbuf_masked),
						 hexdump_page(pageserver_masked));
				}
			}
		}
	}
#endif
}
#endif /* PG_MAJORVERSION_NUM <= 16 */

#if PG_MAJORVERSION_NUM >= 17
static void
neon_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		void **buffers, BlockNumber nblocks)
{
	bits8		read[PG_IOV_MAX / 8];
	neon_request_lsns request_lsns[PG_IOV_MAX];
	int			lfc_result;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrread() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdreadv(reln, forknum, blocknum, buffers, nblocks);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (nblocks > PG_IOV_MAX)
		neon_log(ERROR, "Read request too large: %d is larger than max %d",
				 nblocks, PG_IOV_MAX);

	memset(read, 0, sizeof(read));

	/* Try to read from local file cache */
	lfc_result = lfc_readv_select(InfoFromSMgrRel(reln), forknum, blocknum, buffers,
								  nblocks, read);

	if (lfc_result > 0)
		MyNeonCounters->file_cache_hits_total += lfc_result;

	/* Read all blocks from LFC, so we're done */
	if (lfc_result == nblocks)
		return;

	if (lfc_result == -1)
	{
		/* can't use the LFC result, so read all blocks from PS */
		for (int i = 0; i < PG_IOV_MAX / 8; i++)
			read[i] = 0xFF;
	}
	else
	{
		/* invert the result: exclude blocks read from lfc */
		for (int i = 0; i < PG_IOV_MAX / 8; i++)
			read[i] = ~(read[i]);
	}

	neon_get_request_lsns(InfoFromSMgrRel(reln), forknum, blocknum,
						  request_lsns, nblocks, read);

	neon_read_at_lsnv(InfoFromSMgrRel(reln), forknum, blocknum, request_lsns,
					  buffers, nblocks, read);

#ifdef DEBUG_COMPARE_LOCAL
	if (forkNum == MAIN_FORKNUM && IS_LOCAL_REL(reln))
	{
		char		pageserver_masked[BLCKSZ];
		char		mdbuf[BLCKSZ];
		char		mdbuf_masked[BLCKSZ];

		for (int i = 0; i < nblocks; i++)
		{
#if PG_MAJORVERSION_NUM >= 17
			mdreadv(reln, forkNum, blkno + i, &mdbuf, 1);
#else
			mdread(reln, forkNum, blkno + i, mdbuf);
#endif

			memcpy(pageserver_masked, buffer, BLCKSZ);
			memcpy(mdbuf_masked, mdbuf, BLCKSZ);

			if (PageIsNew((Page) mdbuf))
			{
				if (!PageIsNew((Page) pageserver_masked))
				{
					neon_log(PANIC, "page is new in MD but not in Page Server at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
						 blkno,
						 RelFileInfoFmt(InfoFromSMgrRel(reln)),
						 forkNum,
						 (uint32) (request_lsn >> 32), (uint32) request_lsn,
						 hexdump_page(buffer));
				}
			}
			else if (PageIsNew((Page) buffer))
			{
				neon_log(PANIC, "page is new in Page Server but not in MD at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
					 blkno,
					 RelFileInfoFmt(InfoFromSMgrRel(reln)),
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(mdbuf));
			}
			else if (PageGetSpecialSize(mdbuf) == 0)
			{
				/* assume heap */
				RmgrTable[RM_HEAP_ID].rm_mask(mdbuf_masked, blkno);
				RmgrTable[RM_HEAP_ID].rm_mask(pageserver_masked, blkno);

				if (memcmp(mdbuf_masked, pageserver_masked, BLCKSZ) != 0)
				{
					neon_log(PANIC, "heap buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
						 blkno,
						 RelFileInfoFmt(InfoFromSMgrRel(reln)),
						 forkNum,
						 (uint32) (request_lsn >> 32), (uint32) request_lsn,
						 hexdump_page(mdbuf_masked),
						 hexdump_page(pageserver_masked));
				}
			}
			else if (PageGetSpecialSize(mdbuf) == MAXALIGN(sizeof(BTPageOpaqueData)))
			{
				if (((BTPageOpaqueData *) PageGetSpecialPointer(mdbuf))->btpo_cycleid < MAX_BT_CYCLE_ID)
				{
					/* assume btree */
					RmgrTable[RM_BTREE_ID].rm_mask(mdbuf_masked, blkno);
					RmgrTable[RM_BTREE_ID].rm_mask(pageserver_masked, blkno);
	
					if (memcmp(mdbuf_masked, pageserver_masked, BLCKSZ) != 0)
					{
						neon_log(PANIC, "btree buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
							 blkno,
							 RelFileInfoFmt(InfoFromSMgrRel(reln)),
							 forkNum,
							 (uint32) (request_lsn >> 32), (uint32) request_lsn,
							 hexdump_page(mdbuf_masked),
							 hexdump_page(pageserver_masked));
					}
				}
			}
		}
	}
#endif
}
#endif

#ifdef DEBUG_COMPARE_LOCAL
static char *
hexdump_page(char *page)
{
	StringInfoData result;

	initStringInfo(&result);

	for (int i = 0; i < BLCKSZ; i++)
	{
		if (i % 8 == 0)
			appendStringInfo(&result, " ");
		if (i % 40 == 0)
			appendStringInfo(&result, "\n");
		appendStringInfo(&result, "%02x", (unsigned char) (page[i]));
	}

	return result.data;
}
#endif

#if PG_MAJORVERSION_NUM < 17
/*
 *	neon_write() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
static void
#if PG_MAJORVERSION_NUM < 16
neon_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync)
#else
neon_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void *buffer, bool skipFsync)
#endif
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* This is a bit tricky. Check if the relation exists locally */
			if (mdexists(reln, forknum))
			{
				/* It exists locally. Guess it's unlogged then. */
#if PG_MAJORVERSION_NUM >= 17
				mdwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
#else
				mdwrite(reln, forknum, blocknum, buffer, skipFsync);
#endif
				/*
				 * We could set relpersistence now that we have determined
				 * that it's local. But we don't dare to do it, because that
				 * would immediately allow reads as well, which shouldn't
				 * happen. We could cache it with a different 'relpersistence'
				 * value, but this isn't performance critical.
				 */
				return;
			}
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			#if PG_MAJORVERSION_NUM >= 17
			mdwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
			#else
			mdwrite(reln, forknum, blocknum, buffer, skipFsync);
			#endif
			return;
		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_wallog_page(reln, forknum, blocknum, buffer, false);

	lsn = PageGetLSN((Page) buffer);
	neon_log(SmgrTrace, "smgrwrite called for %u/%u/%u.%u blk %u, page LSN: %X/%08X",
		 RelFileInfoFmt(InfoFromSMgrRel(reln)),
		 forknum, blocknum,
		 (uint32) (lsn >> 32), (uint32) lsn);

	lfc_write(InfoFromSMgrRel(reln), forknum, blocknum, buffer);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		#if PG_MAJORVERSION_NUM >= 17
		mdwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
		#else
		mdwrite(reln, forknum, blocknum, buffer, skipFsync);
		#endif
#endif
}
#endif



#if PG_MAJORVERSION_NUM >= 17
static void
neon_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* This is a bit tricky. Check if the relation exists locally */
			if (mdexists(reln, forknum))
			{
				/* It exists locally. Guess it's unlogged then. */
				mdwritev(reln, forknum, blkno, buffers, nblocks, skipFsync);

				/*
				 * We could set relpersistence now that we have determined
				 * that it's local. But we don't dare to do it, because that
				 * would immediately allow reads as well, which shouldn't
				 * happen. We could cache it with a different 'relpersistence'
				 * value, but this isn't performance critical.
				 */
				return;
			}
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdwritev(reln, forknum, blkno, buffers, nblocks, skipFsync);
			return;
		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_wallog_pagev(reln, forknum, blkno, nblocks, (const char **) buffers, false);

	lfc_writev(InfoFromSMgrRel(reln), forknum, blkno, buffers, nblocks);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
#endif
}

#endif

/*
 *	neon_nblocks() -- Get the number of blocks stored in a relation.
 */
static BlockNumber
neon_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	NeonResponse *resp;
	BlockNumber n_blocks;
	neon_request_lsns request_lsns;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrnblocks() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdnblocks(reln, forknum);

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (get_cached_relsize(InfoFromSMgrRel(reln), forknum, &n_blocks))
	{
		neon_log(SmgrTrace, "cached nblocks for %u/%u/%u.%u: %u blocks",
			 RelFileInfoFmt(InfoFromSMgrRel(reln)),
			 forknum, n_blocks);
		return n_blocks;
	}

	neon_get_request_lsns(InfoFromSMgrRel(reln), forknum,
						  REL_METADATA_PSEUDO_BLOCKNO, &request_lsns, 1, NULL);

	{
		NeonNblocksRequest request = {
			.hdr.tag = T_NeonNblocksRequest,
			.hdr.reqid = GENERATE_REQUEST_ID(),
			.hdr.lsn = request_lsns.request_lsn,
			.hdr.not_modified_since = request_lsns.not_modified_since,
			.rinfo = InfoFromSMgrRel(reln),
			.forknum = forknum,
		};

		resp = page_server_request(&request);

		switch (resp->tag)
		{
			case T_NeonNblocksResponse:
			{
				NeonNblocksResponse * relsize_resp = (NeonNblocksResponse *) resp;
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr) ||
						!RelFileInfoEquals(relsize_resp->req.rinfo, request.rinfo) ||
						relsize_resp->req.forknum != forknum)
					{
						NEON_PANIC_CONNECTION_STATE(-1, PANIC,
													"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u} to get relsize request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u}",
													resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), RelFileInfoFmt(relsize_resp->req.rinfo), relsize_resp->req.forknum,
													request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since), RelFileInfoFmt(request.rinfo), forknum);
					}
				}
				n_blocks = relsize_resp->n_blocks;
				break;
			}
			case T_NeonErrorResponse:
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr))
					{
						elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match get relsize request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
							 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
							 request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since));
					}
				}
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg(NEON_TAG "[reqid %lx] could not read relation size of rel %u/%u/%u.%u from page server at lsn %X/%08X",
								resp->reqid,
								RelFileInfoFmt(InfoFromSMgrRel(reln)),
								forknum,
								LSN_FORMAT_ARGS(request_lsns.effective_request_lsn)),
						 errdetail("page server returned error: %s",
								   ((NeonErrorResponse *) resp)->message)));
				break;

			default:
				NEON_PANIC_CONNECTION_STATE(-1, PANIC,
											"Expected Nblocks (0x%02x) or Error (0x%02x) response to NblocksRequest, but got 0x%02x",
											T_NeonNblocksResponse, T_NeonErrorResponse, resp->tag);
		}
		update_cached_relsize(InfoFromSMgrRel(reln), forknum, n_blocks);

		neon_log(SmgrTrace, "neon_nblocks: rel %u/%u/%u fork %u (request LSN %X/%08X): %u blocks",
				 RelFileInfoFmt(InfoFromSMgrRel(reln)),
				 forknum,
				 LSN_FORMAT_ARGS(request_lsns.effective_request_lsn),
				 n_blocks);

		pfree(resp);
	}
	return n_blocks;
}

/*
 *	neon_db_size() -- Get the size of the database in bytes.
 */
int64
neon_dbsize(Oid dbNode)
{
	NeonResponse *resp;
	int64		db_size;
	neon_request_lsns request_lsns;
	NRelFileInfo dummy_node = {0};

	neon_get_request_lsns(dummy_node, MAIN_FORKNUM,
						  REL_METADATA_PSEUDO_BLOCKNO, &request_lsns, 1, NULL);

	{
		NeonDbSizeRequest request = {
			.hdr.tag = T_NeonDbSizeRequest,
			.hdr.reqid = GENERATE_REQUEST_ID(),
			.hdr.lsn = request_lsns.request_lsn,
			.hdr.not_modified_since = request_lsns.not_modified_since,
			.dbNode = dbNode,
		};

		resp = page_server_request(&request);

		switch (resp->tag)
		{
			case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse* dbsize_resp = (NeonDbSizeResponse *) resp;
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr) ||
						dbsize_resp->req.dbNode != dbNode)
					{
						NEON_PANIC_CONNECTION_STATE(-1, PANIC,
													"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, dbNode=%u} to get DB size request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, dbNode=%u}",
													resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), dbsize_resp->req.dbNode,
													request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since), dbNode);
					}
				}
				db_size = dbsize_resp->db_size;
				break;
			}
			case T_NeonErrorResponse:
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr))
					{
						elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match get DB size request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
							 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
							 request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since));
					}
				}
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg(NEON_TAG "[reqid %lx] could not read db size of db %u from page server at lsn %X/%08X",
								resp->reqid,
								dbNode, LSN_FORMAT_ARGS(request_lsns.effective_request_lsn)),
						 errdetail("page server returned error: %s",
								   ((NeonErrorResponse *) resp)->message)));
				break;

			default:
				NEON_PANIC_CONNECTION_STATE(-1, PANIC,
											"Expected DbSize (0x%02x) or Error (0x%02x) response to DbSizeRequest, but got 0x%02x",
											T_NeonDbSizeResponse, T_NeonErrorResponse, resp->tag);
		}

		neon_log(SmgrTrace, "neon_dbsize: db %u (request LSN %X/%08X): %ld bytes",
				 dbNode, LSN_FORMAT_ARGS(request_lsns.effective_request_lsn), db_size);

		pfree(resp);
	}
	return db_size;
}

/*
 *	neon_truncate() -- Truncate relation to specified number of blocks.
 */
static void
neon_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrtruncate() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdtruncate(reln, forknum, nblocks);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	set_cached_relsize(InfoFromSMgrRel(reln), forknum, nblocks);

	/*
	 * Truncating a relation drops all its buffers from the buffer cache
	 * without calling smgrwrite() on them. But we must account for that in
	 * our tracking of last-written-LSN all the same: any future smgrnblocks()
	 * request must return the new size after the truncation. We don't know
	 * what the LSN of the truncation record was, so be conservative and use
	 * the most recently inserted WAL record's LSN.
	 */
	lsn = GetXLogInsertRecPtr();
	lsn = nm_adjust_lsn(lsn);

	/*
	 * Flush it, too. We don't actually care about it here, but let's uphold
	 * the invariant that last-written LSN <= flush LSN.
	 */
	XLogFlush(lsn);

	/*
	 * Truncate may affect several chunks of relations. So we should either
	 * update last written LSN for all of them, or update LSN for "dummy"
	 * metadata block. Second approach seems more efficient. If the relation
	 * is extended again later, the extension will update the last-written LSN
	 * for the extended pages, so there's no harm in leaving behind obsolete
	 * entries for the truncated chunks.
	 */
	SetLastWrittenLSNForRelation(lsn, InfoFromSMgrRel(reln), forknum);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdtruncate(reln, forknum, nblocks);
#endif
}

/*
 *	neon_immedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.  We
 * sync active and inactive segments; smgrDoPendingSyncs() relies on this.
 * Consider a relation skipping WAL.  Suppose a checkpoint syncs blocks of
 * some segment, then mdtruncate() renders that segment inactive.  If we
 * crash before the next checkpoint syncs the newly-inactive segment, that
 * segment may survive recovery, reintroducing unwanted data into the table.
 */
static void
neon_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrimmedsync() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdimmedsync(reln, forknum);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_log(SmgrTrace, "[NEON_SMGR] immedsync noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdimmedsync(reln, forknum);
#endif
}

#if PG_MAJORVERSION_NUM >= 17
static void
neon_registersync(SMgrRelation reln, ForkNumber forknum)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrregistersync() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdregistersync(reln, forknum);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_log(SmgrTrace, "[NEON_SMGR] registersync noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdimmedsync(reln, forknum);
#endif
}
#endif


/*
 * neon_start_unlogged_build() -- Starting build operation on a rel.
 *
 * Some indexes are built in two phases, by first populating the table with
 * regular inserts, using the shared buffer cache but skipping WAL-logging,
 * and WAL-logging the whole relation after it's done. Neon relies on the
 * WAL to reconstruct pages, so we cannot use the page server in the
 * first phase when the changes are not logged.
 */
static void
neon_start_unlogged_build(SMgrRelation reln)
{
	/*
	 * Currently, there can be only one unlogged relation build operation in
	 * progress at a time. That's enough for the current usage.
	 */
	if (unlogged_build_phase != UNLOGGED_BUILD_NOT_IN_PROGRESS)
		neon_log(ERROR, "unlogged relation build is already in progress");
	Assert(unlogged_build_rel == NULL);

	ereport(SmgrTrace,
			(errmsg(NEON_TAG "starting unlogged build of relation %u/%u/%u",
					RelFileInfoFmt(InfoFromSMgrRel(reln)))));

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgr_start_unlogged_build() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			unlogged_build_rel = reln;
			unlogged_build_phase = UNLOGGED_BUILD_NOT_PERMANENT;
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (smgrnblocks(reln, MAIN_FORKNUM) != 0)
		neon_log(ERROR, "cannot perform unlogged index build, index is not empty ");

	unlogged_build_rel = reln;
	unlogged_build_phase = UNLOGGED_BUILD_PHASE_1;

	/* Make the relation look like it's unlogged */
	reln->smgr_relpersistence = RELPERSISTENCE_UNLOGGED;

	/*
	 * Create the local file. In a parallel build, the leader is expected to
	 * call this first and do it.
	 *
	 * FIXME: should we pass isRedo true to create the tablespace dir if it
	 * doesn't exist? Is it needed?
	 */
	if (!IsParallelWorker())
		mdcreate(reln, MAIN_FORKNUM, false);
}

/*
 * neon_finish_unlogged_build_phase_1()
 *
 * Call this after you have finished populating a relation in unlogged mode,
 * before you start WAL-logging it.
 */
static void
neon_finish_unlogged_build_phase_1(SMgrRelation reln)
{
	Assert(unlogged_build_rel == reln);

	ereport(SmgrTrace,
			(errmsg(NEON_TAG "finishing phase 1 of unlogged build of relation %u/%u/%u",
					RelFileInfoFmt(InfoFromSMgrRel(reln)))));

	if (unlogged_build_phase == UNLOGGED_BUILD_NOT_PERMANENT)
		return;

	Assert(unlogged_build_phase == UNLOGGED_BUILD_PHASE_1);
	Assert(reln->smgr_relpersistence == RELPERSISTENCE_UNLOGGED);

	/*
	 * In a parallel build, (only) the leader process performs the 2nd
	 * phase.
	 */
	if (IsParallelWorker())
	{
		unlogged_build_rel = NULL;
		unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
	}
	else
		unlogged_build_phase = UNLOGGED_BUILD_PHASE_2;
}

/*
 * neon_end_unlogged_build() -- Finish an unlogged rel build.
 *
 * Call this after you have finished WAL-logging an relation that was
 * first populated without WAL-logging.
 *
 * This removes the local copy of the rel, since it's now been fully
 * WAL-logged and is present in the page server.
 */
static void
neon_end_unlogged_build(SMgrRelation reln)
{
	NRelFileInfoBackend rinfob = InfoBFromSMgrRel(reln);

	Assert(unlogged_build_rel == reln);

	ereport(SmgrTrace,
			(errmsg(NEON_TAG "ending unlogged build of relation %u/%u/%u",
					RelFileInfoFmt(InfoFromNInfoB(rinfob)))));

	if (unlogged_build_phase != UNLOGGED_BUILD_NOT_PERMANENT)
	{
		Assert(unlogged_build_phase == UNLOGGED_BUILD_PHASE_2);
		Assert(reln->smgr_relpersistence == RELPERSISTENCE_UNLOGGED);

		/* Make the relation look permanent again */
		reln->smgr_relpersistence = RELPERSISTENCE_PERMANENT;

		/* Remove local copy */
		rinfob = InfoBFromSMgrRel(reln);
		for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			neon_log(SmgrTrace, "forgetting cached relsize for %u/%u/%u.%u",
				 RelFileInfoFmt(InfoFromNInfoB(rinfob)),
				 forknum);

			forget_cached_relsize(InfoFromNInfoB(rinfob), forknum);
			mdclose(reln, forknum);
			/* use isRedo == true, so that we drop it immediately */
			mdunlink(rinfob, forknum, true);
		}
	}

	unlogged_build_rel = NULL;
	unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
}

#define STRPREFIX(str, prefix) (strncmp(str, prefix, strlen(prefix)) == 0)

static int
neon_read_slru_segment(SMgrRelation reln, const char* path, int segno, void* buffer)
{
	XLogRecPtr	request_lsn,
				not_modified_since;
	SlruKind	kind;
	int			n_blocks;
	shardno_t	shard_no = 0; /* All SLRUs are at shard 0 */
	NeonResponse *resp;
	NeonGetSlruSegmentRequest request;

	/*
	 * Compute a request LSN to use, similar to neon_get_request_lsns() but the
	 * logic is a bit simpler.
	 */
	if (RecoveryInProgress())
	{
		request_lsn = GetXLogReplayRecPtr(NULL);
		if (request_lsn == InvalidXLogRecPtr)
		{
			/*
			 * This happens in neon startup, we start up without replaying any
			 * records.
			 */
			request_lsn = GetRedoStartLsn();
		}
		request_lsn = nm_adjust_lsn(request_lsn);
	}
	else
		request_lsn = UINT64_MAX;

	/*
	 * GetRedoStartLsn() returns LSN of the basebackup. We know that the SLRU
	 * segment has not changed since the basebackup, because in order to
	 * modify it, we would have had to download it already. And once
	 * downloaded, we never evict SLRU segments from local disk.
	 */
	not_modified_since = nm_adjust_lsn(GetRedoStartLsn());

	if (STRPREFIX(path, "pg_xact"))
		kind = SLRU_CLOG;
	else if (STRPREFIX(path, "pg_multixact/members"))
		kind = SLRU_MULTIXACT_MEMBERS;
	else if (STRPREFIX(path, "pg_multixact/offsets"))
		kind = SLRU_MULTIXACT_OFFSETS;
	else
		return -1;

	request = (NeonGetSlruSegmentRequest) {
		.hdr.tag = T_NeonGetSlruSegmentRequest,
		.hdr.reqid = GENERATE_REQUEST_ID(),
		.hdr.lsn = request_lsn,
		.hdr.not_modified_since = not_modified_since,
		.kind = kind,
		.segno = segno
	};

	do
	{
		while (!page_server->send(shard_no, &request.hdr) || !page_server->flush(shard_no));

		consume_prefetch_responses();

		resp = page_server->receive(shard_no);
	} while (resp == NULL);

	switch (resp->tag)
	{
		case T_NeonGetSlruSegmentResponse:
		{
			NeonGetSlruSegmentResponse* slru_resp = (NeonGetSlruSegmentResponse *) resp;
			if (neon_protocol_version >= 3)
			{
				if (!equal_requests(resp, &request.hdr) ||
					slru_resp->req.kind != kind ||
					slru_resp->req.segno != segno)
				{
					NEON_PANIC_CONNECTION_STATE(-1, PANIC,
												"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, kind=%u, segno=%u} to get SLRU segment request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, kind=%u, segno=%u}",
												resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), slru_resp->req.kind, slru_resp->req.segno,
												request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since), kind, segno);
				}
			}
			n_blocks = slru_resp->n_blocks;
			memcpy(buffer, slru_resp->data, n_blocks*BLCKSZ);
			break;
		}
		case T_NeonErrorResponse:
			if (neon_protocol_version >= 3)
			{
				if (!equal_requests(resp, &request.hdr))
				{
					elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match get SLRU segment request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
						 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
						 request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since));
				}
			}
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg(NEON_TAG "[reqid %lx] could not read SLRU %d segment %d at lsn %X/%08X",
							resp->reqid,
							kind,
							segno,
							LSN_FORMAT_ARGS(request_lsn)),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			NEON_PANIC_CONNECTION_STATE(-1, PANIC,
										"Expected GetSlruSegment (0x%02x) or Error (0x%02x) response to GetSlruSegmentRequest, but got 0x%02x",
										T_NeonGetSlruSegmentResponse, T_NeonErrorResponse, resp->tag);
	}
	pfree(resp);

	return n_blocks;
}

static void
AtEOXact_neon(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:

			/*
			 * Forget about any build we might have had in progress. The local
			 * file will be unlinked by smgrDoPendingDeletes()
			 */
			unlogged_build_rel = NULL;
			unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			if (unlogged_build_phase != UNLOGGED_BUILD_NOT_IN_PROGRESS)
			{
				unlogged_build_rel = NULL;
				unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 (errmsg(NEON_TAG "unlogged index build was not properly finished"))));
			}
			break;
	}
}

static const struct f_smgr neon_smgr =
{
	.smgr_init = neon_init,
	.smgr_shutdown = NULL,
	.smgr_open = neon_open,
	.smgr_close = neon_close,
	.smgr_create = neon_create,
	.smgr_exists = neon_exists,
	.smgr_unlink = neon_unlink,
	.smgr_extend = neon_extend,
#if PG_MAJORVERSION_NUM >= 16
	.smgr_zeroextend = neon_zeroextend,
#endif
#if PG_MAJORVERSION_NUM >= 17
	.smgr_prefetch = neon_prefetch,
	.smgr_readv = neon_readv,
	.smgr_writev = neon_writev,
#else
	.smgr_prefetch = neon_prefetch,
	.smgr_read = neon_read,
	.smgr_write = neon_write,
#endif

	.smgr_writeback = neon_writeback,
	.smgr_nblocks = neon_nblocks,
	.smgr_truncate = neon_truncate,
	.smgr_immedsync = neon_immedsync,
#if PG_MAJORVERSION_NUM >= 17
	.smgr_registersync = neon_registersync,
#endif
	.smgr_start_unlogged_build = neon_start_unlogged_build,
	.smgr_finish_unlogged_build_phase_1 = neon_finish_unlogged_build_phase_1,
	.smgr_end_unlogged_build = neon_end_unlogged_build,

	.smgr_read_slru_segment = neon_read_slru_segment,
};

const f_smgr *
smgr_neon(ProcNumber backend, NRelFileInfo rinfo)
{

	/* Don't use page server for temp relations */
	if (backend != INVALID_PROC_NUMBER)
		return smgr_standard(backend, rinfo);
	else
		return &neon_smgr;
}

void
smgr_init_neon(void)
{
	RegisterXactCallback(AtEOXact_neon, NULL);

	smgr_init_standard();
	neon_init();
}


static void
neon_extend_rel_size(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blkno, XLogRecPtr end_recptr)
{
	BlockNumber relsize;

	/* This is only used in WAL replay */
	Assert(RecoveryInProgress());

	/* Extend the relation if we know its size */
	if (get_cached_relsize(rinfo, forknum, &relsize))
	{
		if (relsize < blkno + 1)
		{
			update_cached_relsize(rinfo, forknum, blkno + 1);
			SetLastWrittenLSNForRelation(end_recptr, rinfo, forknum);
		}
	}
	else
	{
		/*
		 * Size was not cached. We populate the cache now, with the size of
		 * the relation measured after this WAL record is applied.
		 *
		 * This length is later reused when we open the smgr to read the
		 * block, which is fine and expected.
		 */
		NeonResponse *response;
		NeonNblocksResponse *nbresponse;
		NeonNblocksRequest request = {
			.hdr = (NeonRequest) {
				.tag = T_NeonNblocksRequest,
				.reqid = GENERATE_REQUEST_ID(),
				.lsn = end_recptr,
				.not_modified_since = end_recptr,
			},
			.rinfo = rinfo,
			.forknum = forknum,
		};

		response = page_server_request(&request);

		Assert(response->tag == T_NeonNblocksResponse);
		nbresponse = (NeonNblocksResponse *) response;

		relsize = Max(nbresponse->n_blocks, blkno + 1);

		set_cached_relsize(rinfo, forknum, relsize);
		SetLastWrittenLSNForRelation(end_recptr, rinfo, forknum);

		neon_log(SmgrTrace, "Set length to %d", relsize);
	}
}

#define FSM_TREE_DEPTH	((SlotsPerFSMPage >= 1626) ? 3 : 4)

/*
 * TODO: May be it is better to make correspondent function from freespace.c public?
 */
static BlockNumber
get_fsm_physical_block(BlockNumber heapblk)
{
	BlockNumber pages;
	int			leafno;
	int			l;

	/*
	 * Calculate the logical page number of the first leaf page below the
	 * given page.
	 */
	leafno = heapblk / SlotsPerFSMPage;

	/* Count upper level nodes required to address the leaf page */
	pages = 0;
	for (l = 0; l < FSM_TREE_DEPTH; l++)
	{
		pages += leafno + 1;
		leafno /= SlotsPerFSMPage;
	}

	/* Turn the page count into 0-based block number */
	return pages - 1;
}


/*
 * Return whether we can skip the redo for this block.
 *
 * The conditions for skipping the IO are:
 *
 * - The block is not in the shared buffers, and
 * - The block is not in the local file cache
 *
 * ... because any subsequent read of the page requires us to read
 * the new version of the page from the PageServer. We do not
 * check the local file cache; we instead evict the page from LFC: it
 * is cheaper than going through the FS calls to read the page, and
 * limits the number of lock operations used in the REDO process.
 *
 * We have one exception to the rules for skipping IO: We always apply
 * changes to shared catalogs' pages. Although this is mostly out of caution,
 * catalog updates usually result in backends rebuilding their catalog snapshot,
 * which means it's quite likely the modified page is going to be used soon.
 *
 * It is important to note that skipping WAL redo for a page also means
 * the page isn't locked by the redo process, as there is no Buffer
 * being returned, nor is there a buffer descriptor to lock.
 * This means that any IO that wants to read this block needs to wait
 * for the WAL REDO process to finish processing the WAL record before
 * it allows the system to start reading the block, as releasing the
 * block early could lead to phantom reads.
 *
 * For example, REDO for a WAL record that modifies 3 blocks could skip
 * the first block, wait for a lock on the second, and then modify the
 * third block. Without skipping, all blocks would be locked and phantom
 * reads would not occur, but with skipping, a concurrent process could
 * read block 1 with post-REDO contents and read block 3 with pre-REDO
 * contents, where with REDO locking it would wait on block 1 and see
 * block 3 with post-REDO contents only.
 */
static bool
neon_redo_read_buffer_filter(XLogReaderState *record, uint8 block_id)
{
	XLogRecPtr	end_recptr = record->EndRecPtr;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
	BlockNumber blkno;
	BufferTag	tag;
	uint32		hash;
	LWLock	   *partitionLock;
	int			buf_id;
	bool		no_redo_needed;

	if (old_redo_read_buffer_filter && old_redo_read_buffer_filter(record, block_id))
		return true;

#if PG_VERSION_NUM < 150000
	if (!XLogRecGetBlockTag(record, block_id, &rinfo, &forknum, &blkno))
		neon_log(PANIC, "failed to locate backup block with ID %d", block_id);
#else
	XLogRecGetBlockTag(record, block_id, &rinfo, &forknum, &blkno);
#endif

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forknum;
	tag.blockNum = blkno;

	hash = BufTableHashCode(&tag);
	partitionLock = BufMappingPartitionLock(hash);

	/*
	 * Lock the partition of shared_buffers so that it can't be updated
	 * concurrently.
	 */
	LWLockAcquire(partitionLock, LW_SHARED);

	/*
	 * Out of an abundance of caution, we always run redo on shared catalogs,
	 * regardless of whether the block is stored in shared buffers. See also
	 * this function's top comment.
	 */
	if (!OidIsValid(NInfoGetDbOid(rinfo)))
	{
		no_redo_needed = false;
	}
	else
	{
		/* Try to find the relevant buffer */
		buf_id = BufTableLookup(&tag, hash);

		no_redo_needed = buf_id < 0;
	}

	/*
	 * we don't have the buffer in memory, update lwLsn past this record, also
	 * evict page from file cache
	 */
	if (no_redo_needed)
	{
		SetLastWrittenLSNForBlock(end_recptr, rinfo, forknum, blkno);
		lfc_evict(rinfo, forknum, blkno);
	}

	LWLockRelease(partitionLock);

	neon_extend_rel_size(rinfo, forknum, blkno, end_recptr);
	if (forknum == MAIN_FORKNUM)
	{
		neon_extend_rel_size(rinfo, FSM_FORKNUM, get_fsm_physical_block(blkno), end_recptr);
	}
	return no_redo_needed;
}
