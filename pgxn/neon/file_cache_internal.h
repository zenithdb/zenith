#ifndef FILE_CACHE_INTERNAL_H
#define FILE_CACHE_INTERNAL_H

#include "lib/ilist.h"
#include "storage/buf_internals.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "utils/dynahash.h"

#include "hll.h"

/*-----------
 * Prewarming
 * ----------
 *
 * LFC prewarming happens with the use of one or more prewarm workers, which
 * should never access the same LFC chunk at the same time.
 *
 * The prewarm worker works with these invariants:
 *		1. There are no concurrent writes to the pages that the prewarm worker
 *			is writing to the chunk.
 *			This is guaranteed by these mechanisms: 
 *			 - Before starting to write, a prewarm worker marks the chunk as
 *			   "prewarm is writing to this", and then waits for all current
 *			   accessors to finish their activities. Therefore, all users
 *			   know that prewarm is active on the chunk.
 *			 - Prewarm workers will only write to as-of-yet unwritten blocks
 *			   in the chunk.
 *			 - Writers to the chunk will wait for the prewarm worker to finish
 *			   writing in the chunk before starting their own writes into
 *			   as-of-yet unwritten blocks in the chunk.
 *			 - Readers won't be blocked (but can decide to wait for prewarm
 *			   in case they need to read pages not yet in LFC, and prewarm is
 *			   active on the chunk)
 *		2. Pages are never overwritten by LFC prewarm
 *		3. Readers are never blocked
 *		4. Only potentially conflicting writers are blocked, *but are not ignored*.
 *
 * The only consideration is that in hot-standby mode, we *can not* ignore
 * writes to pages that are being prewarmed. Usually, that's not much of an
 * issue, but here we don't have a buffer that ensures synchronization between
 * the redo process and the contents of the page/buffer (which we'd otherwise
 * have). So, in REDO mode we'd have to make sure that the recovery process
 * DOES do redo for pages in chunks that are being prewarmed, even if that
 * means doing PS reads, to make sure we don't drop changes for pages we're
 * fetching through prewarm processes.
 *
 * The algorithm used is as follows:
 *
 * 1.  Select a chunk to prewarm.
 *     This requires the chunk to be present in LFC, or an empty chunk to be
 *     available in the LFC that we can then fill.
 *     If there are no empty chunks left, and the chunk we selected isn't
 *     present in the LFC, we skip this chunk as evicting data to prewarm other
 *     data may be detrimental to the workload.
 * 2.  Prepare the chunk: with exclusively locked LFC:
 *     1.  Note down which blocks are already written to the chunk.
 *         We don't have to read those blocks, as they're already the latest
 *         version.
 *         If we don't have any blocks left to fetch, we're done: restart at 1
 *     2.  Unlink the selected chunk from the LRU.
 *         Now it can't be removed through LRU eviction, so we know any new
 *         blocks written to the chunk will be the latest version of said pages.
 * 3.  Determine which blocks to retrieve (= selection from 1, with selection
 *     from 2.1 removed)
 * 4.  Prefetch the to-be-retrieved blocks of the chunk.
 * 5.  Read all to-be-retrieved blocks of the chunk into local memory.
 * 6.  With exclusively locked LFC:
 *     1.  Mark chunk as LFC_PREWARM_STARTED
 *     2.  Copy this backend's prewarmer worker number (if relevant) into new
 *         `prewarm_worker_field` on chunk (this needs to be only a few bits,
 *         e.g. 8, as we only will likely have very few workers concurrently
 *         prewarming the LFC),
 *     3.  Copy the LFC entry's refcount into per-prewarmworker state's
 *         'wait_count', reduced by 1 (to account for the bgworker itself).
 *     Now the chunk we're going to put data into is pinned (has been removed from the LRU, can't be reclaimed) and no new backends will start write-IO on pages that aren't present in the chunk due to the LFC_PREWARM_STARTED flag.
 * 7.  Normal backends: Writes for not-yet-present pages that get to that chunk
 *     start waiting on (new) condition variable `prewarm_done` in the
 *     reported prewarm worker's worker state, until the lfc chunk doesn't have
 *     `LFC_PREWARM_STARTED` set or the page is present in the LFC chunk. All
 *     other writes and reads can continue as usual (those pages are not
 *     touched by the prewarm worker).
 * 8.  Normal backends: When the chunk is released:
 *     1.  If LFC_PREWARM_STARTED is not set, we're done.
 *     2.  If LFC_PREWARM_STARTED was already set when the chunk was acquired,
 *         we're done.
 *     3.  Reduce the current chunk's prewarm worker's wait count by 1.
 *     4.  If the current chunk's prewarm worker's wait count is 0, signal
 *         chunkrelease.
 * 9.  Prewarmer:
 *     1.  wait on new condition variable `chunkrelease` until the wait_count
 *         is 0 (no more active concurrent writes)
 *     2.  Write out own data into pages that aren't yet present in the chunk
 * 10. Prewarmer; holding LFC lock:
 *     1.  Set 'written' bit for valid data.
 *     2.  Unset LFC_PREWARM_STARTED
 *     3.  Release and link chunk as normal.
 * 11. Signal condition variable `prewarm_done`.
 *
 * For any chunk, valid operations are as follows:
 *
 * For IOs arriving through shared_buffers;
 * - Readers can always read from valid slots (unchanged by prewarm)
 * - Writers can always write to valid slots (unchanged by prewarm)
 * - Writers can write to invalid slots when 'prewarm_active' is not
 *    set when it acquires the chunk (if it is set, they have to wait for
 *    'prewarm_active' to be unset).
 *
 * For the recovery (startup) process:
 * - Recovery will always try to apply WAL to pages in slots that are
 *    detected as being prewarmed (the "not in local buffers" exception
 *    doesn't apply).
 *    This means recovery will update the LFC chunk with the latest
 *    version of the page, removing the opportunity for race conditions.
 *
 * For prewarm workers:
 * - Writes can only write to invalid slots, and only after making sure
 *   no other writers are active (through refcount/release checks, causing
 *   other writers to go into shared_buffers condition 3).
 *
 *
 * We depend on this critical ordering:
 *  1. Chunk unlink
 *       The chunk now can't be dropped from LRU, so we won't drop writes,
 *       so we have no undetected concurrent changes.
 *  2. LWLsn fetch / validation
 *       Any new writes would go into the LFC and would either overwrite our
 *       sideloaded data, or write data first and we wouldn't overwrite it.
 *  3. Set prewarm_active
 *       No new concurrent writers
 *  4. Wait for concurrent writers to finish
 *       No writers left that might write into the chunk; exclusive write access.
 *  5. Write to empty pages
 *       Changes since LwLsn may have been written to empty pages; but can't
 *       have been removed.
 *  6. Mark written pages & release prewarm_active & release concurrent writers
 *       Everything is normal again.
 */


/*
 * Local file storage allocation chunk.
 * Should be power of two. Using larger than page chunks can
 * 1. Reduce hash-map memory footprint: 8TB database contains billion pages
 *    and size of hash entry is 40 bytes, so we need 40Gb just for hash map.
 *    1Mb chunks can reduce hash map size to 320Mb.
 * 2. Improve access locality, subsequent pages will be allocated together improving seqscan speed
 */
#define BLOCKS_PER_CHUNK	128 /* 1Mb chunk */
#define MB					((uint64)1024*1024)

#define SIZE_MB_TO_CHUNKS(size) ((uint32)((size) * MB / BLCKSZ / BLOCKS_PER_CHUNK))
#define CHUNK_BITMAP_SIZE ((BLOCKS_PER_CHUNK + 31) / 32)

typedef struct FileCacheEntry
{
	BufferTag	key;
	uint32		hash;
	uint32		offset;
	uint32		access_count : 30;		/* limited to (2^18 - 1) by
										 * MAX_BACKENDS */
	bool		prewarm_selected : 1;	/* selected by prewarm worker,
										 * fetching has started */
	bool		prewarm_active : 1;		/* prewarm loading has started */
	uint32		bitmap[CHUNK_BITMAP_SIZE];
	dlist_node	list_node;		/* LRU/holes list node */
} FileCacheEntry;

/*----------
 * 		wait_count
 * 			Number of chunk accesses the backend is waiting for to complete
 *		wait_write
 *			Signaled whenever the prewarm process is done with a chunk
 *		backend_io
 *			Prewarmer sleeps on this CV when it is waiting for concurrent
 *			accesses to the chunk to finish their work.
 */
typedef struct FileCacheWorkerShared
{
	uint64				wait_time;		/* total time spent waiting for
										 * concurrent writes on LFC chunks */
	int					wait_count;		/* Number of other backends prewarm is
										 * still waiting for */
	ConditionVariable	prewarm_done;	/* Backends can wait on this for
										 * prewarm to complete */
	ConditionVariable	chunk_release;	/* Prewarm worker waits for signal on
										 * this CV for concurrent writes */
} FileCacheWorkerShared;

typedef struct FileCacheControl
{
	uint64		generation;		/* generation is needed to handle correct hash
								 * reenabling */
	uint32		size;			/* size of cache file in chunks */
	uint32		used;			/* number of used chunks */
	uint32		used_pages;		/* number of used pages */
	uint32		limit;			/* shared copy of lfc_size_limit */
	uint64		hits;
	uint64		misses;
	uint64		writes;			/* number of writes issued */
	uint64		time_read;		/* time spent reading (us) */
	uint64		time_write;		/* time spent writing (us) */
	uint64		time_prewarm;	/* time spent waiting for prewarm (us) */
	uint32		prewarm_total_chunks;
	uint32		prewarm_curr_chunk;
	uint32		prewarmed_pages;
	uint32		skipped_pages;
	dlist_head	lru;			/* double linked list for LRU replacement
								 * algorithm */
	dlist_head  holes;			/* double linked list of punched holes */
	FileCacheWorkerShared worker; /* future: support multiple parallel workers */
	HyperLogLogState wss_estimation; /* estimation of working set size */
} FileCacheControl;

typedef struct FileCacheStateEntry
{
	BufferTag	key;
	uint32		bitmap[CHUNK_BITMAP_SIZE];
} FileCacheStateEntry;

typedef struct LFCPrewarmChunk {
	FileCacheEntry	   *cacheEntry;		/* LFC entry of work item */
	FileCacheStateEntry *stateEntry;	/* prewarm request of work item */
	dlist_node	node;					/* entry in lpws_work */
	int			npages;					/* bit count in .stateEntry */
	int			prefetched;				/* num getpage requests already sent */
	int			received;				/* pages already in .pages */
	int			loaded;					/* pages loaded */
	PGAlignedBlock	*pages;				/* pages read from disk; IO_ALIGNed */
	void	   *alloc;					/* allocation containing *pages */
	BlockNumber	blknos[FLEXIBLE_ARRAY_MEMBER];	/* block numbers */
} LFCPrewarmChunk;

#define PREWARM_CHUNK_SIZE(nmemb) ( \
	offsetof(LFCPrewarmChunk, blknos) + (nmemb) * sizeof(BlockNumber) \
)
typedef struct LFCPrewarmWorkerState {
	Size		lpws_numrestore;		/* number of chunks to restore */
	FileCacheStateEntry *lpws_fcses;	/* chunks we have yet to restore */
	uint64		lpws_pages_prefetched;	/* pages in lpws_work which were neon_prefetch-ed */
	uint64		lpws_pages_read;		/* pages in lpws_work which were neon_read_at_lsn() */
	uint64		lpws_pages_loaded;		/* actually written to LFC (total) */
	uint64		lpws_pages_discarded;	/* fetched-but-ignored writes to LFC (total) */
	int			lpws_max_io_depth;		/* max diff between prefetched and read */
	dlist_head	lpws_work;				/* queue of prewarmer work */
} LFCPrewarmWorkerState;

extern FileCacheControl *lfc_ctl;
extern HTAB	   *lfc_hash;
extern int		lfc_desc;
extern LWLockId	lfc_lock;
extern int		lfc_prewarm_io_concurrency;

#define LFC_ENABLED() (lfc_ctl->limit != 0)

extern bool lfc_ensure_opened(void);

extern FileCacheEntry *lfc_entry_for_write(BufferTag *key, bool no_replace,
										   bool *prewarm_active,
										   const uint32 *bitmap, bool nodelay);
extern void release_entry(FileCacheEntry *entry, bool prewarm_was_active);

extern void lfc_prewarm(FileCacheStateEntry *fs, size_t n_entries);

#endif /* FILE_CACHE_INTERNAL_H */
