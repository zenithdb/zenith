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
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/fsm_internals.h"
#include "storage/md.h"
#include "storage/smgr.h"

#include "pagestore_client.h"

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

typedef struct PrefetchRequest
{
	BufferTag	buftag;			/* must be first entry in the struct */
	XLogRecPtr	effective_request_lsn;
	XLogRecPtr	actual_request_lsn;
	NeonResponse *response;		/* may be null */
	PrefetchStatus status;
	shardno_t   shard_no;
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

#define SH_EQUAL(tb, a, b)	(BUFFERTAGS_EQUAL((a)->buftag, (b)->buftag))
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

#define BITMAP_ISSET(bm, bit) ((bm)[(bit) >> 3] & (1 << ((bit) & 7)))
#define BITMAP_SET(bm, bit) (bm)[(bit) >> 3] |= (1 << ((bit) & 7))
#define BITMAP_CLR(bm, bit) (bm)[(bit) >> 3] &= ~(1 << ((bit) & 7))

static PrefetchState *MyPState;

#define GetPrfSlot(ring_index) ( \
	( \
		AssertMacro((ring_index) < MyPState->ring_unused && \
					(ring_index) >= MyPState->ring_last), \
		&MyPState->prf_buffer[((ring_index) % readahead_buffer_size)] \
	) \
)

#define ReceiveBufferNeedsCompaction() (\
	(MyPState->n_responses_buffered / 8) < ( \
		MyPState->ring_receive - \
			MyPState->ring_last - \
			MyPState->n_responses_buffered \
	) \
)

static XLogRecPtr prefetch_lsn = 0;

static bool compact_prefetch_buffers(void);
static void consume_prefetch_responses(void);
static uint64 prefetch_register_buffer(BufferTag tag, bool *force_latest, XLogRecPtr *force_lsn);
static bool prefetch_read(PrefetchRequest *slot);
static void prefetch_do_request(PrefetchRequest *slot, bool *force_latest, XLogRecPtr *force_lsn);
static bool prefetch_wait_for(uint64 ring_index);
static void prefetch_cleanup_trailing_unused(void);
static inline void prefetch_set_unused(uint64 ring_index);

static XLogRecPtr neon_get_request_lsn(bool *latest, NRelFileInfo rinfo,
									   ForkNumber forknum, BlockNumber blkno);

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
		target_slot->effective_request_lsn = source_slot->effective_request_lsn;
		target_slot->actual_request_lsn = source_slot->actual_request_lsn;
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
		source_slot->effective_request_lsn = 0;

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
		prefetch_wait_for(MyPState->ring_unused - newsize);

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
	newPState->ring_flush = newsize;

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

	for (; end >= MyPState->ring_last && end != UINT64_MAX; end -= 1)
	{
		prefetch_set_unused(end);
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
 */
static bool
prefetch_read(PrefetchRequest *slot)
{
	NeonResponse *response;
	MemoryContext old;

	Assert(slot->status == PRFS_REQUESTED);
	Assert(slot->response == NULL);
	Assert(slot->my_ring_index == MyPState->ring_receive);

	old = MemoryContextSwitchTo(MyPState->errctx);
	response = (NeonResponse *) page_server->receive(slot->shard_no);
	MemoryContextSwitchTo(old);
	if (response)
	{
		/* update prefetch state */
		MyPState->n_responses_buffered += 1;
		MyPState->n_requests_inflight -= 1;
		MyPState->ring_receive += 1;

		/* update slot state */
		slot->status = PRFS_RECEIVED;
		slot->response = response;
		return true;
	}
	else
	{
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
	}
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

static void
prefetch_do_request(PrefetchRequest *slot, bool *force_latest, XLogRecPtr *force_lsn)
{
	bool		found;
	NeonGetPageRequest request = {
		.req.tag = T_NeonGetPageRequest,
		.req.latest = false,
		.req.lsn = 0,
		.rinfo = BufTagGetNRelFileInfo(slot->buftag),
		.forknum = slot->buftag.forkNum,
		.blkno = slot->buftag.blockNum,
	};

	if (force_lsn && force_latest)
	{
		request.req.lsn = *force_lsn;
		request.req.latest = *force_latest;
		slot->actual_request_lsn = slot->effective_request_lsn = *force_lsn;
	}
	else
	{
		XLogRecPtr	lsn = neon_get_request_lsn(
											   &request.req.latest,
											   BufTagGetNRelFileInfo(slot->buftag),
											   slot->buftag.forkNum,
											   slot->buftag.blockNum
			);

		/*
		 * Note: effective_request_lsn is potentially higher than the
		 * requested LSN, but still correct:
		 *
		 * We know there are no changes between the actual requested LSN and
		 * the value of effective_request_lsn: If there were, the page would
		 * have been in cache and evicted between those LSN values, which then
		 * would have had to result in a larger request LSN for this page.
		 *
		 * It is possible that a concurrent backend loads the page, modifies
		 * it and then evicts it again, but the LSN of that eviction cannot be
		 * smaller than the current WAL insert/redo pointer, which is already
		 * larger than this prefetch_lsn. So in any case, that would
		 * invalidate this cache.
		 *
		 * The best LSN to use for effective_request_lsn would be
		 * XLogCtl->Insert.RedoRecPtr, but that's expensive to access.
		 */
		slot->actual_request_lsn = request.req.lsn = lsn;
		prefetch_lsn = Max(prefetch_lsn, lsn);
		slot->effective_request_lsn = prefetch_lsn;
	}

	Assert(slot->response == NULL);
	Assert(slot->my_ring_index == MyPState->ring_unused);

	while (!page_server->send(slot->shard_no, (NeonRequest *) &request));

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
 * prefetch_register_buffer() - register and prefetch buffer
 *
 * Register that we may want the contents of BufferTag in the near future.
 *
 * If force_latest and force_lsn are not NULL, those values are sent to the
 * pageserver. If they are NULL, we utilize the lastWrittenLsn -infrastructure
 * to fill in these values manually.
 *
 * NOTE: this function may indirectly update MyPState->pfs_hash; which
 * invalidates any active pointers into the hash table.
 */

static uint64
prefetch_register_buffer(BufferTag tag, bool *force_latest, XLogRecPtr *force_lsn)
{
	uint64		ring_index;
	PrefetchRequest req;
	PrefetchRequest *slot;
	PrfHashEntry *entry;

	/* use an intermediate PrefetchRequest struct to ensure correct alignment */
	req.buftag = tag;
Retry:
	entry = prfh_lookup(MyPState->prf_hash, (PrefetchRequest *) &req);

	if (entry != NULL)
	{
		slot = entry->slot;
		ring_index = slot->my_ring_index;
		Assert(slot == GetPrfSlot(ring_index));

		Assert(slot->status != PRFS_UNUSED);
		Assert(MyPState->ring_last <= ring_index &&
			   ring_index < MyPState->ring_unused);
		Assert(BUFFERTAGS_EQUAL(slot->buftag, tag));

		/*
		 * If we want a specific lsn, we do not accept requests that were made
		 * with a potentially different LSN.
		 */
		if (force_latest && force_lsn)
		{
			/*
			 * if we want the latest version, any effective_request_lsn <
			 * request lsn is OK
			 */
			if (*force_latest)
			{
				if (*force_lsn > slot->effective_request_lsn)
				{
					if (!prefetch_wait_for(ring_index))
						goto Retry;
					prefetch_set_unused(ring_index);
					entry = NULL;
				}

			}

			/*
			 * if we don't want the latest version, only accept requests with
			 * the exact same LSN
			 */
			else
			{
				if (*force_lsn != slot->effective_request_lsn)
				{
					if (!prefetch_wait_for(ring_index))
						goto Retry;
					prefetch_set_unused(ring_index);
					entry = NULL;
				}
			}
		}

		if (entry != NULL)
		{
			/*
			 * We received a prefetch for a page that was recently read and
			 * removed from the buffers. Remove that request from the buffers.
			 */
			if (slot->status == PRFS_TAG_REMAINS)
			{
				prefetch_set_unused(ring_index);
				entry = NULL;
			}
			else
			{
				/* The buffered request is good enough, return that index */
				pgBufferUsage.prefetch.duplicates++;
				return ring_index;
			}
		}
	}

	/*
	 * If the prefetch queue is full, we need to make room by clearing the
	 * oldest slot. If the oldest slot holds a buffer that was already
	 * received, we can just throw it away; we fetched the page unnecessarily
	 * in that case. If the oldest slot holds a request that we haven't
	 * received a response for yet, we have to wait for the response to that
	 * before we can continue. We might not have even flushed the request to
	 * the pageserver yet, it might be just sitting in the output buffer. In
	 * that case, we flush it and wait for the response. (We could decide not
	 * to send it, but it's hard to abort when the request is already in the
	 * output buffer, and 'not sending' a prefetch request kind of goes
	 * against the principles of prefetching)
	 */
	if (MyPState->ring_last + readahead_buffer_size - 1 == MyPState->ring_unused)
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
					break;
				case PRFS_RECEIVED:
				case PRFS_TAG_REMAINS:
					prefetch_set_unused(cleanup_index);
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
	slot = &MyPState->prf_buffer[((ring_index) % readahead_buffer_size)];

	Assert(MyPState->ring_last <= ring_index);

	Assert(slot->status == PRFS_UNUSED);

	/*
	 * We must update the slot data before insertion, because the hash
	 * function reads the buffer tag from the slot.
	 */
	slot->buftag = tag;
	slot->shard_no = get_shard_number(&tag);
	slot->my_ring_index = ring_index;

	prefetch_do_request(slot, force_latest, force_lsn);
	Assert(slot->status == PRFS_REQUESTED);
	Assert(MyPState->ring_last <= ring_index &&
		   ring_index < MyPState->ring_unused);

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

	return ring_index;
}

static NeonResponse *
page_server_request(void const *req)
{
	NeonResponse *resp;
	BufferTag tag = {0};
	shardno_t shard_no;

	switch (((NeonRequest *) req)->tag)
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
			neon_log(ERROR, "Unexpected request tag: %d", ((NeonRequest *) req)->tag);
	}
	shard_no = get_shard_number(&tag);


	/*
	 * Current sharding model assumes that all metadata is present only at shard 0.
	 * We still need to call get_shard_no() to check if shard map is up-to-date.
	 */
	if (((NeonRequest *) req)->tag != T_NeonGetPageRequest || ((NeonGetPageRequest *) req)->forknum != MAIN_FORKNUM)
	{
		shard_no = 0;
	}

	do
	{
		while (!page_server->send(shard_no, (NeonRequest *) req) || !page_server->flush(shard_no));
		consume_prefetch_responses();
		resp = page_server->receive(shard_no);
	} while (resp == NULL);
	return resp;

}


StringInfoData
nm_pack_request(NeonRequest *msg)
{
	StringInfoData s;

	initStringInfo(&s);
	pq_sendbyte(&s, msg->tag);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_NeonExistsRequest:
			{
				NeonExistsRequest *msg_req = (NeonExistsRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint32(&s, NInfoGetSpcOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetDbOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetRelNumber(msg_req->rinfo));
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_NeonNblocksRequest:
			{
				NeonNblocksRequest *msg_req = (NeonNblocksRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint32(&s, NInfoGetSpcOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetDbOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetRelNumber(msg_req->rinfo));
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_NeonDbSizeRequest:
			{
				NeonDbSizeRequest *msg_req = (NeonDbSizeRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
				pq_sendint32(&s, msg_req->dbNode);

				break;
			}
		case T_NeonGetPageRequest:
			{
				NeonGetPageRequest *msg_req = (NeonGetPageRequest *) msg;

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
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

				pq_sendbyte(&s, msg_req->req.latest);
				pq_sendint64(&s, msg_req->req.lsn);
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
	NeonResponse *resp = NULL;

	switch (tag)
	{
			/* pagestore -> pagestore_client */
		case T_NeonExistsResponse:
			{
				NeonExistsResponse *msg_resp = palloc0(sizeof(NeonExistsResponse));

				msg_resp->tag = tag;
				msg_resp->exists = pq_getmsgbyte(s);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonNblocksResponse:
			{
				NeonNblocksResponse *msg_resp = palloc0(sizeof(NeonNblocksResponse));

				msg_resp->tag = tag;
				msg_resp->n_blocks = pq_getmsgint(s, 4);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonGetPageResponse:
			{
				NeonGetPageResponse *msg_resp;

				msg_resp = MemoryContextAllocZero(MyPState->bufctx, PS_GETPAGERESPONSE_SIZE);
				msg_resp->tag = tag;
				NInfoGetSpcOid(msg_resp->rinfo) = pq_getmsgint(s, 4);
				NInfoGetDbOid(msg_resp->rinfo) = pq_getmsgint(s, 4);
				NInfoGetRelNumber(msg_resp->rinfo) = pq_getmsgint(s, 4);
				msg_resp->forknum =  pq_getmsgbyte(s);
				msg_resp->blkno = pq_getmsgint(s, 4);
				/* XXX:	should be varlena */
				memcpy(msg_resp->page, pq_getmsgbytes(s, BLCKSZ), BLCKSZ);
				pq_getmsgend(s);

				Assert(msg_resp->tag == T_NeonGetPageResponse);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse *msg_resp = palloc0(sizeof(NeonDbSizeResponse));

				msg_resp->tag = tag;
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
				msg_resp->tag = tag;
				memcpy(msg_resp->message, msgtext, msglen + 1);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonGetSlruSegmentResponse:
		    {
				NeonGetSlruSegmentResponse *msg_resp;
				int n_blocks = pq_getmsgint(s, 4);
				msg_resp = palloc(sizeof(NeonGetSlruSegmentResponse));
				msg_resp->tag = tag;
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
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonNblocksRequest:
			{
				NeonNblocksRequest *msg_req = (NeonNblocksRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonNblocksRequest\"");
				appendStringInfo(&s, ", \"rinfo\": \"%u/%u/%u\"", RelFileInfoFmt(msg_req->rinfo));
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
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
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonDbSizeRequest:
			{
				NeonDbSizeRequest *msg_req = (NeonDbSizeRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonDbSizeRequest\"");
				appendStringInfo(&s, ", \"dbnode\": \"%u\"", msg_req->dbNode);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonGetSlruSegmentRequest:
			{
				NeonGetSlruSegmentRequest *msg_req = (NeonGetSlruSegmentRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetSlruSegmentRequest\"");
				appendStringInfo(&s, ", \"kind\": %u", msg_req->kind);
				appendStringInfo(&s, ", \"segno\": %u", msg_req->segno);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->req.lsn));
				appendStringInfo(&s, ", \"latest\": %d", msg_req->req.latest);
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

static void
#if PG_MAJORVERSION_NUM < 16
neon_wallog_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool force)
#else
neon_wallog_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool force)
#endif
{
	XLogRecPtr	lsn = PageGetLSN((Page) buffer);

	if (ShutdownRequestPending)
		return;
	/* Don't log any pages if we're not allowed to do so. */
	if (!XLogInsertAllowed())
		return;

	/*
	 * Whenever a VM or FSM page is evicted, WAL-log it. FSM and (some) VM
	 * changes are not WAL-logged when the changes are made, so this is our
	 * last chance to log them, otherwise they're lost. That's OK for
	 * correctness, the non-logged updates are not critical. But we want to
	 * have a reasonably up-to-date VM and FSM in the page server.
	 */
	if ((force || forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM) && !RecoveryInProgress())
	{
		/* FSM is never WAL-logged and we don't care. */
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
	else if (lsn == InvalidXLogRecPtr)
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
		else
		{
			ereport(PANIC,
					(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is evicted with zero LSN",
							blocknum,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum)));
		}
	}
	else
	{
		ereport(SmgrTrace,
				(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is already wal logged at lsn=%X/%X",
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
void
neon_init(void)
{
	Size		prfs_size;

	if (MyPState != NULL)
		return;

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
 * Return LSN for requesting pages and number of blocks from page server
 */
static XLogRecPtr
neon_get_request_lsn(bool *latest, NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blkno)
{
	XLogRecPtr	lsn;

	if (RecoveryInProgress())
	{
		/*
		 * We don't know if WAL has been generated but not yet replayed, so
		 * we're conservative in our estimates about latest pages.
		 */
		*latest = false;

		/*
		 * Get the last written LSN of this page.
		 */
		lsn = GetLastWrittenLSN(rinfo, forknum, blkno);
		lsn = nm_adjust_lsn(lsn);

		neon_log(DEBUG1, "neon_get_request_lsn GetXLogReplayRecPtr %X/%X request lsn 0 ",
			 (uint32) ((lsn) >> 32), (uint32) (lsn));
	}
	else
	{
		XLogRecPtr	flushlsn;

		/*
		 * Use the latest LSN that was evicted from the buffer cache. Any
		 * pages modified by later WAL records must still in the buffer cache,
		 * so our request cannot concern those.
		 */
		*latest = true;
		lsn = GetLastWrittenLSN(rinfo, forknum, blkno);
		Assert(lsn != InvalidXLogRecPtr);
		neon_log(DEBUG1, "neon_get_request_lsn GetLastWrittenLSN lsn %X/%X ",
			 (uint32) ((lsn) >> 32), (uint32) (lsn));

		lsn = nm_adjust_lsn(lsn);

		/*
		 * Is it possible that the last-written LSN is ahead of last flush
		 * LSN? Generally not, we shouldn't evict a page from the buffer cache
		 * before all its modifications have been safely flushed. That's the
		 * "WAL before data" rule. However, such case does exist at index
		 * building, _bt_blwritepage logs the full page without flushing WAL
		 * before smgrextend (files are fsynced before build ends).
		 */
#if PG_VERSION_NUM >= 150000
		flushlsn = GetFlushRecPtr(NULL);
#else
		flushlsn = GetFlushRecPtr();
#endif
		if (lsn > flushlsn)
		{
			neon_log(DEBUG5, "last-written LSN %X/%X is ahead of last flushed LSN %X/%X",
				 (uint32) (lsn >> 32), (uint32) lsn,
				 (uint32) (flushlsn >> 32), (uint32) flushlsn);
			XLogFlush(lsn);
		}
	}

	return lsn;
}

/*
 *	neon_exists() -- Does the physical file exist?
 */
bool
neon_exists(SMgrRelation reln, ForkNumber forkNum)
{
	bool		exists;
	NeonResponse *resp;
	BlockNumber n_blocks;
	bool		latest;
	XLogRecPtr	request_lsn;

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

	request_lsn = neon_get_request_lsn(&latest, InfoFromSMgrRel(reln), forkNum, REL_METADATA_PSEUDO_BLOCKNO);
	{
		NeonExistsRequest request = {
			.req.tag = T_NeonExistsRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.rinfo = InfoFromSMgrRel(reln),
		.forknum = forkNum};

		resp = page_server_request(&request);
	}

	switch (resp->tag)
	{
		case T_NeonExistsResponse:
			exists = ((NeonExistsResponse *) resp)->exists;
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg(NEON_TAG "could not read relation existence of rel %u/%u/%u.%u from page server at lsn %X/%08X",
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forkNum,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			neon_log(ERROR, "unexpected response from page server with tag 0x%02x in neon_exists", resp->tag);
	}
	pfree(resp);
	return exists;
}

/*
 *	neon_create() -- Create a new relation on neond storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
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
void
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
void
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
		!IsAutoVacuumWorkerProcess())
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
void
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
		!IsAutoVacuumWorkerProcess())
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
void
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
void
neon_close(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * Let md.c close it, if it had it open. Doesn't hurt to do this even for
	 * permanent relations that have no local storage.
	 */
	mdclose(reln, forknum);
}


/*
 *	neon_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
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

	ring_index = prefetch_register_buffer(tag, NULL, NULL);

	Assert(ring_index < MyPState->ring_unused &&
		   MyPState->ring_last <= ring_index);

	return false;
}

/*
 * neon_writeback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
void
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

	/* not implemented */
	neon_log(SmgrTrace, "writeback noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwriteback(reln, forknum, blocknum, nblocks);
#endif
}

/*
 * While function is defined in the neon extension it's used within neon_test_utils directly.
 * To avoid breaking tests in the runtime please keep function signature in sync.
 */
void
#if PG_MAJORVERSION_NUM < 16
neon_read_at_lsn(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
				 XLogRecPtr request_lsn, bool request_latest, char *buffer)
#else
neon_read_at_lsn(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
				 XLogRecPtr request_lsn, bool request_latest, void *buffer)
#endif
{
	NeonResponse *resp;
	uint64		ring_index;
	PrfHashEntry *entry;
	PrefetchRequest *slot;
	BufferTag	buftag =
	{
		.forkNum = forkNum,
		.blockNum = blkno,
	};

	CopyNRelFileInfoToBufTag(buftag, rinfo);

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
	if (RecoveryInProgress() && !(MyBackendType == B_STARTUP))
		XLogWaitForReplayOf(request_lsn);

	/*
	 * Try to find prefetched page in the list of received pages.
	 */
  Retry:
	entry = prfh_lookup(MyPState->prf_hash, (PrefetchRequest *) &buftag);

	if (entry != NULL)
	{
		slot = entry->slot;
		if (slot->effective_request_lsn >= request_lsn)
		{
			ring_index = slot->my_ring_index;
			pgBufferUsage.prefetch.hits += 1;
		}
		else					/* the current prefetch LSN is not large
								 * enough, so drop the prefetch */
		{
			/*
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
			/* make it look like a prefetch cache miss */
			entry = NULL;
		}
	}

	do
	{
		if (entry == NULL)
		{
			pgBufferUsage.prefetch.misses += 1;

			ring_index = prefetch_register_buffer(buftag, &request_latest,
												  &request_lsn);
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

	resp = slot->response;

	switch (resp->tag)
	{
		case T_NeonGetPageResponse:
		{
			NeonGetPageResponse* r = (NeonGetPageResponse *) resp;
			memcpy(buffer, r->page, BLCKSZ);
			if (memcmp(&r->rinfo, &rinfo, sizeof rinfo) != 0 && forkNum != r->forknum || blkno != r->blkno)
				ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg(NEON_TAG "[shard %d] get unexpected get page resonse for block %u in rel %u/%u/%u.%u instead of block block %u in rel %u/%u/%u.%u from page server at lsn %X/%08X",
							slot->shard_no,
							r->blkno, RelFileInfoFmt(r->rinfo), r->forknum,
							blkno, RelFileInfoFmt(rinfo), forkNum,
							(uint32) (request_lsn >> 32), (uint32) request_lsn)));
			lfc_write(rinfo, forkNum, blkno, buffer);
			break;
		}
		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg(NEON_TAG "[shard %d] could not read block %u in rel %u/%u/%u.%u from page server at lsn %X/%08X",
							slot->shard_no, blkno,
							RelFileInfoFmt(rinfo),
							forkNum,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;
		default:
			neon_log(ERROR, "unexpected response from page server with tag 0x%02x in neon_read_at_lsn", resp->tag);
	}

	/* buffer was used, clean up for later reuse */
	prefetch_set_unused(ring_index);
	prefetch_cleanup_trailing_unused();
}

/*
 *	neon_read() -- Read the specified block from a relation.
 */
void
#if PG_MAJORVERSION_NUM < 16
neon_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno, char *buffer)
#else
neon_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno, void *buffer)
#endif
{
	bool		latest;
	XLogRecPtr	request_lsn;

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
		return;
	}

	request_lsn = neon_get_request_lsn(&latest, InfoFromSMgrRel(reln), forkNum, blkno);
	neon_read_at_lsn(InfoFromSMgrRel(reln), forkNum, blkno, request_lsn, latest, buffer);

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

/*
 *	neon_write() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
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
				mdwrite(reln, forknum, blocknum, buffer, skipFsync);

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
			mdwrite(reln, forknum, blocknum, buffer, skipFsync);
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
		mdwrite(reln, forknum, blocknum, buffer, skipFsync);
#endif
}

/*
 *	neon_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
neon_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	NeonResponse *resp;
	BlockNumber n_blocks;
	bool		latest;
	XLogRecPtr	request_lsn;

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

	request_lsn = neon_get_request_lsn(&latest, InfoFromSMgrRel(reln), forknum, REL_METADATA_PSEUDO_BLOCKNO);
	{
		NeonNblocksRequest request = {
			.req.tag = T_NeonNblocksRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.rinfo = InfoFromSMgrRel(reln),
			.forknum = forknum,
		};

		resp = page_server_request(&request);
	}

	switch (resp->tag)
	{
		case T_NeonNblocksResponse:
			n_blocks = ((NeonNblocksResponse *) resp)->n_blocks;
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg(NEON_TAG "could not read relation size of rel %u/%u/%u.%u from page server at lsn %X/%08X",
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			neon_log(ERROR, "unexpected response from page server with tag 0x%02x in neon_nblocks", resp->tag);
	}
	update_cached_relsize(InfoFromSMgrRel(reln), forknum, n_blocks);

	neon_log(SmgrTrace, "neon_nblocks: rel %u/%u/%u fork %u (request LSN %X/%08X): %u blocks",
		 RelFileInfoFmt(InfoFromSMgrRel(reln)),
		 forknum,
		 (uint32) (request_lsn >> 32), (uint32) request_lsn,
		 n_blocks);

	pfree(resp);
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
	XLogRecPtr	request_lsn;
	bool		latest;
	NRelFileInfo dummy_node = {0};

	request_lsn = neon_get_request_lsn(&latest, dummy_node, MAIN_FORKNUM, REL_METADATA_PSEUDO_BLOCKNO);
	{
		NeonDbSizeRequest request = {
			.req.tag = T_NeonDbSizeRequest,
			.req.latest = latest,
			.req.lsn = request_lsn,
			.dbNode = dbNode,
		};

		resp = page_server_request(&request);
	}

	switch (resp->tag)
	{
		case T_NeonDbSizeResponse:
			db_size = ((NeonDbSizeResponse *) resp)->db_size;
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg(NEON_TAG "could not read db size of db %u from page server at lsn %X/%08X",
							dbNode,
							(uint32) (request_lsn >> 32), (uint32) request_lsn),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			neon_log(ERROR, "unexpected response from page server with tag 0x%02x in neon_dbsize", resp->tag);
	}

	neon_log(SmgrTrace, "neon_dbsize: db %u (request LSN %X/%08X): %ld bytes",
		 dbNode,
		 (uint32) (request_lsn >> 32), (uint32) request_lsn,
		 db_size);

	pfree(resp);
	return db_size;
}

/*
 *	neon_truncate() -- Truncate relation to specified number of blocks.
 */
void
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
void
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
	 * FIXME: should we pass isRedo true to create the tablespace dir if it
	 * doesn't exist? Is it needed?
	 */
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
	XLogRecPtr request_lsn;
	/*
	 * GetRedoStartLsn() returns LSN of basebackup.
	 * We need to download SLRU segments only once after node startup,
	 * then SLRUs are maintained locally.
	 */
	request_lsn = GetRedoStartLsn();
	request_lsn = nm_adjust_lsn(request_lsn);
	SlruKind kind;

    if (STRPREFIX(path, "pg_xact"))
        kind = SLRU_CLOG;
    else if (STRPREFIX(path, "pg_multixact/members"))
        kind = SLRU_MULTIXACT_MEMBERS;
    else if (STRPREFIX(path, "pg_multixact/offsets"))
        kind = SLRU_MULTIXACT_OFFSETS;
    else
        return -1;

	NeonResponse *resp;
	NeonGetSlruSegmentRequest request = {
		.req.tag = T_NeonGetSlruSegmentRequest,
		.req.latest = false,
		.req.lsn = request_lsn,

		.kind = kind,
		.segno = segno
	};
	int n_blocks;
	shardno_t shard_no = 0; /* All SLRUs are at shard 0 */
	do
	{
		while (!page_server->send(shard_no, &request.req) || !page_server->flush(shard_no));
		consume_prefetch_responses();
		resp = page_server->receive(shard_no);
	} while (resp == NULL);

	switch (resp->tag)
	{
		case T_NeonGetSlruSegmentResponse:
			n_blocks = ((NeonGetSlruSegmentResponse *) resp)->n_blocks;
			memcpy(buffer, ((NeonGetSlruSegmentResponse *) resp)->data, n_blocks*BLCKSZ);
			break;

		case T_NeonErrorResponse:
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg(NEON_TAG "could not read SLRU %d segment %d at lsn %X/%08X",
							kind,
							segno,
							LSN_FORMAT_ARGS(request_lsn)),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			neon_log(ERROR, "unexpected response from page server with tag 0x%02x in neon_read_slru_segment", resp->tag);
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
	.smgr_prefetch = neon_prefetch,
	.smgr_read = neon_read,
	.smgr_write = neon_write,
	.smgr_writeback = neon_writeback,
	.smgr_nblocks = neon_nblocks,
	.smgr_truncate = neon_truncate,
	.smgr_immedsync = neon_immedsync,

	.smgr_start_unlogged_build = neon_start_unlogged_build,
	.smgr_finish_unlogged_build_phase_1 = neon_finish_unlogged_build_phase_1,
	.smgr_end_unlogged_build = neon_end_unlogged_build,

	.smgr_read_slru_segment = neon_read_slru_segment,
};

const f_smgr *
smgr_neon(BackendId backend, NRelFileInfo rinfo)
{

	/* Don't use page server for temp relations */
	if (backend != InvalidBackendId)
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
			.req = (NeonRequest) {
				.lsn = end_recptr,
				.latest = false,
				.tag = T_NeonNblocksRequest,
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
	Buffer		buffer;
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
		buffer = BufTableLookup(&tag, hash);

		no_redo_needed = buffer < 0;
	}
	/* In both cases st lwlsn past this WAL record */
	SetLastWrittenLSNForBlock(end_recptr, rinfo, forknum, blkno);

	/*
	 * we don't have the buffer in memory, update lwLsn past this record, also
	 * evict page from file cache
	 */
	if (no_redo_needed)
		lfc_evict(rinfo, forknum, blkno);


	LWLockRelease(partitionLock);

	neon_extend_rel_size(rinfo, forknum, blkno, end_recptr);
	if (forknum == MAIN_FORKNUM)
	{
		neon_extend_rel_size(rinfo, FSM_FORKNUM, get_fsm_physical_block(blkno), end_recptr);
	}
	return no_redo_needed;
}
