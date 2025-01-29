#include "postgres.h"

#include <sys/file.h>
#include <unistd.h>
#include <fcntl.h>

#include "neon_pgversioncompat.h"

#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "portability/instr_time.h"
#include "storage/ipc.h"

#include "bitmap.h"
#include "file_cache_internal.h"
#include "neon.h"
#include "pagestore_client.h"
#include "access/xlog.h"

static void lfcp_cleanup(int, Datum);
static LFCPrewarmChunk *lfcp_preprocess_chunk(FileCacheStateEntry *entry,
											  bool nowait, bool *load_needed);
static bool lfcp_pump_prefetch(LFCPrewarmWorkerState *state);
static bool lfcp_pump_read(LFCPrewarmWorkerState *state);
static void lfcp_pump_load(LFCPrewarmWorkerState *state);
static void lfcp_release_chunk(LFCPrewarmChunk *chunk, uint64 generation);


/*
 * Prewarm LFC cache to the specified state.
 *
 * Prewarming can interfere with accesses to the pages by other backends. Usually access to LFC is protected by shared buffers: when Postgres
 * is reading page, it pins shared buffer and enforces that only one backend is reading it, while other are waiting for read completion.
 *
 * But it is not true for prewarming: backend can fetch page itself, modify and then write it to LFC. At the
 * same time `lfc_prewarm` tries to write deteriorated image of this page in LFC. To increase concurrency, access to LFC files (both read and write)
 * is performed without holding locks. So it can happen that two or more processes write different content to the same location in the LFC file.
 * Certainly we can not rely on disk content in this case.
 *
 * To solve this problem we use two flags in LFC entry: `prewarm_requested` and `prewarm_started`. First is set before prewarm is actually started.
 * `lfc_prewarm` writes to LFC file only if this flag is set. This flag is cleared if any other backend performs write to this LFC chunk.
 * In this case data loaded by `lfc_prewarm` is considered to be deteriorated and should be just ignored.
 *
 * But as far as write to LFC is performed without holding lock, there is no guarantee that no such write is in progress.
 * This is why second flag is used: `prewarm_started`. It is set by `lfc_prewarm` when is starts writing page and cleared when write is completed.
 * Any other backend writing to LFC should abandon it's write to LFC file (just not mark page as loaded in bitmap) if this flag is set.
 * So neither `lfc_prewarm`, neither backend are saving page in LFC in this case - it is just skipped.
 */

void
lfc_prewarm(FileCacheStateEntry* fs, size_t numrestore)
{
	LFCPrewarmWorkerState state;
	lfc_ctl->prewarm_total_chunks += numrestore;

	if (!lfc_ensure_opened())
		return;

	if (fs == NULL)
	{
		Assert(numrestore == 0);
		elog(LOG, "LFC: no data to prewarm");
		return;
	}
	Assert(numrestore > 0);

	state.lpws_max_io_depth = Max(1, lfc_prewarm_io_concurrency);
	state.lpws_pages_prefetched = 0;
	state.lpws_pages_read = 0;
	state.lpws_pages_loaded = 0;
	state.lpws_pages_discarded = 0;
	state.lpws_numrestore = numrestore;
	state.lpws_fcses = fs;
	dlist_init(&state.lpws_work);

	PG_ENSURE_ERROR_CLEANUP(lfcp_cleanup, PointerGetDatum(&state));

	/* Pump until we don't have anything left to do */
	while (!(state.lpws_numrestore == 0 && dlist_is_empty(&state.lpws_work)))
	{
		CHECK_FOR_INTERRUPTS();

		/* Prewarming 4 */
		if (lfcp_pump_prefetch(&state))
		{
			/* Prewarming 5 */
			if (lfcp_pump_read(&state))
			{
				/* Prewarming steps 6, 9..11 */
				lfcp_pump_load(&state);
			}
		}
	}
	PG_END_ENSURE_ERROR_CLEANUP(lfcp_cleanup, PointerGetDatum(&state));
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	lfc_ctl->prewarmed_pages += state.lpws_pages_loaded;
	lfc_ctl->prewarm_curr_chunk += numrestore;
	lfc_ctl->skipped_pages += state.lpws_pages_discarded;
	LWLockRelease(lfc_lock);
}

static void
lfcp_cleanup(int code, Datum arg)
{
	LFCPrewarmWorkerState *state =
		(LFCPrewarmWorkerState *) DatumGetPointer(arg);
	dlist_mutable_iter iter;

	if (dlist_is_empty(&state->lpws_work))
		return;

	dlist_foreach_modify(iter, &state->lpws_work)
	{
		LFCPrewarmChunk *chunk = dlist_container(LFCPrewarmChunk, node,
												 iter.cur);
		FileCacheEntry *entry = chunk->cacheEntry;

		if (entry)
		{
			bool	had_waiters;
			/*
			 * We can have backends waiting on us if we failed during
			 * chunk loading. Signal those backends if needed.
			 */
			bool	maybe_waiters = chunk->npages != 0 &&
									chunk->prefetched == chunk->npages &&
									chunk->received == chunk->npages;

			LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
			had_waiters = entry->prewarm_active;
			entry->prewarm_active &= !maybe_waiters;
			entry->prewarm_selected = false;
			release_entry(chunk->cacheEntry, false);

			/* signal any waiters */
			if (maybe_waiters && had_waiters)
				ConditionVariableSignal(&lfc_ctl->worker.prewarm_done);
		}

		dlist_delete(&chunk->node);
		if (chunk->alloc)
			pfree(chunk->alloc);
		pfree(chunk);
	}
}

/* Takes care of steps 1 through 3 of the prewarm system */
static LFCPrewarmChunk *
lfcp_preprocess_chunk(FileCacheStateEntry *fcsentry, bool nowait,
					  bool *load_needed)
{
	uint32		bitmap[CHUNK_BITMAP_SIZE];
	bool		prewarm_conflict;
	int			j = 0;
	int			npages;
	LFCPrewarmChunk *pwchunk;
	FileCacheEntry *fcentry;

	if (load_needed)
		*load_needed = true;

	/* Prewarming step 1, enter step 2; 2.2 */
	fcentry = lfc_entry_for_write(&fcsentry->key, true, &prewarm_conflict,
								  fcsentry->bitmap, nowait);

	/* Chunk not found, and we're already at capacity */
	if (!fcentry)
	{
		LWLockRelease(lfc_lock);
		return NULL;
	}

	/* Prewarming 2.1 */
	for (int i = 0; i < CHUNK_BITMAP_SIZE; i++)
	{
		bitmap[i] = ~(fcentry->bitmap[i]) & fcsentry->bitmap[i];
	}

	npages = (int) pg_popcount((const char *) bitmap,
							   sizeof(uint32) * CHUNK_BITMAP_SIZE);

	/*
	 * Break out of the loop and release resources when we don't have any
	 * pages left to prewarm.
	 */
	if (unlikely(npages == 0))
	{
		release_entry(fcentry, prewarm_conflict);
		if (load_needed)
			*load_needed = false;
		return NULL;
	}

	fcentry->prewarm_selected = true;

	/* Prewarming exit step 2 */
	LWLockRelease(lfc_lock);

	pwchunk = palloc0(PREWARM_CHUNK_SIZE(npages));
	pwchunk->npages = npages;
	pwchunk->stateEntry = fcsentry;
	pwchunk->cacheEntry = fcentry;

	/* Prewarming 3 */
	for (int i = 0; i < BLOCKS_PER_CHUNK; i++)
	{
		if (bitmap[i >> 5] & 1 << (i % 32))
		{
			pwchunk->blknos[j++] = fcsentry->key.blockNum + i;
		}
	}

	Assert(j == pwchunk->npages);

	return pwchunk;
}

/*
 * Returns true if we've achieved maximum IO depth.
 *
 * Handles steps 1..4 of LFC pre-warming.
 */
static bool
lfcp_pump_prefetch(LFCPrewarmWorkerState *state)
{
	LFCPrewarmChunk *chunk = NULL;
	int			inflight_ios = (int) (state->lpws_pages_prefetched - state->lpws_pages_read);
	int			chunk_prefetches_remaining = 0;

	if (inflight_ios >= state->lpws_max_io_depth)
		return true;

	if (!dlist_is_empty(&state->lpws_work))
	{
		chunk = dlist_head_element(LFCPrewarmChunk, node, &state->lpws_work);
		chunk_prefetches_remaining = chunk->npages - chunk->prefetched;
		Assert(chunk_prefetches_remaining >= 0);
	}

	while (inflight_ios < state->lpws_max_io_depth &&
		   (chunk_prefetches_remaining > 0 || state->lpws_numrestore > 0))
	{
		BufferTag	tag;
		int			new_ios;

		while (chunk_prefetches_remaining == 0)
		{
			FileCacheStateEntry *fcentry = state->lpws_fcses;
			chunk = lfcp_preprocess_chunk(fcentry, false, NULL);
			state->lpws_fcses += 1;
			state->lpws_numrestore -= 1;
			if (!PointerIsValid(chunk))
			{
				if (state->lpws_numrestore == 0)
				{
					/* no new chunks to send prefetch requests for */
					return true;
				}
				continue;
			}

			ereport(DEBUG1,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[LFCP] prewarm starting on %u/%u/%u.%d page %u; %ld remaining",
							RelFileInfoFmt(BufTagGetNRelFileInfo(fcentry->key)),
							fcentry->key.forkNum,
							fcentry->key.blockNum,
							state->lpws_numrestore),
					 errhidecontext(true),
					 errhidestmt(true)));

			dlist_push_head(&state->lpws_work, &chunk->node);
			chunk_prefetches_remaining = chunk->npages - chunk->prefetched;
		}

		Assert(chunk_prefetches_remaining >= 0);
		Assert(PointerIsValid(chunk));
		tag = chunk->stateEntry->key;

		/* TODO: vectorize IOs */
		for (new_ios = 0
			 ; inflight_ios + new_ios < state->lpws_max_io_depth &&
			   new_ios < chunk_prefetches_remaining
			 ; new_ios++)
		{
			tag.blockNum = chunk->blknos[chunk->prefetched + new_ios];
			prefetch_page(BufTagGetNRelFileInfo(tag), tag.forkNum,
						  tag.blockNum);
		}

		Assert(new_ios > 0);
		chunk->prefetched += new_ios;
		state->lpws_pages_prefetched += new_ios;
		inflight_ios += new_ios;

		chunk_prefetches_remaining -= new_ios;
		Assert(chunk_prefetches_remaining == chunk->npages - chunk->prefetched);
	}

	if (chunk)
	{
		Assert(chunk->npages >= chunk->prefetched);
		Assert(chunk->prefetched >= chunk->received);
		Assert(chunk->received >= 0);
	}
	else
	{
		Assert(chunk_prefetches_remaining == 0);
	}

	return inflight_ios == state->lpws_max_io_depth ||
		   (chunk_prefetches_remaining == 0 && state->lpws_numrestore == 0);
}

/*
 * Read the next block from PS.
 * Returns true if the tail PrewarmChunk now has all requested pages.
 */
static bool
lfcp_pump_read(LFCPrewarmWorkerState *state)
{
	LFCPrewarmChunk *chunk;
	int			io_depth PG_USED_FOR_ASSERTS_ONLY =
		state->lpws_pages_prefetched - state->lpws_pages_read;
	int			chunk_ios_active;

	if (dlist_is_empty(&state->lpws_work))
		return false;

	chunk = dlist_tail_element(LFCPrewarmChunk, node, &state->lpws_work);
	chunk_ios_active = chunk->prefetched - chunk->received;

	if (chunk_ios_active == 0)
		elog(WARNING, "No io active");

	if (chunk_ios_active > 0)
	{
		BufferTag	tag;

		Assert(io_depth >= chunk_ios_active);

		tag = chunk->stateEntry->key;

		/*
		 * We're about to read the first page into memory, so allocate some
		 * space for that.
		 */
		if (chunk->received == 0)
		{
			Assert(chunk->alloc == NULL);

			ereport(DEBUG1,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("[LFCP] reads starting on %u/%u/%u..%d start page %u",
							RelFileInfoFmt(BufTagGetNRelFileInfo(chunk->stateEntry->key)),
							chunk->stateEntry->key.forkNum,
							chunk->stateEntry->key.blockNum),
					 errhidecontext(true),
					 errhidestmt(true)));

#if PG_MAJORVERSION_NUM > 15
			chunk->pages = chunk->alloc = 
				MemoryContextAllocAligned(CurrentMemoryContext,
										  chunk->npages * BLCKSZ,
										  PG_IO_ALIGN_SIZE,
										  0);
#else
			chunk->alloc = palloc((chunk->npages) * BLCKSZ + PG_IO_ALIGN_SIZE);
			chunk->pages = (PGAlignedBlock *)
				TYPEALIGN(PG_IO_ALIGN_SIZE, (Size) chunk->alloc);
#endif
		}

		Assert(chunk->pages != NULL);
		tag.blockNum = chunk->blknos[chunk->received];

		read_page(BufTagGetNRelFileInfo(tag), tag.forkNum, tag.blockNum,
				  chunk->pages[chunk->received].data);

		state->lpws_pages_read++;
		chunk->received++;
		chunk_ios_active--;
	}

	Assert(chunk_ios_active <= (state->lpws_pages_prefetched - state->lpws_pages_read));
	Assert(chunk->npages >= chunk->prefetched);
	Assert(chunk->prefetched >= chunk->received);
	Assert(chunk->received >= 0);

	/* Are all pages ready to load into the LFC now? */
	return chunk->npages == chunk->received;
}

/*
 * Load this chunk into LFC.
 * Handles prewarm 6 and 9..11
 */
static void
lfcp_pump_load(LFCPrewarmWorkerState *state)
{
	LFCPrewarmChunk *chunk = dlist_tail_element(LFCPrewarmChunk, node,
												&state->lpws_work);
	FileCacheEntry *fcentry = chunk->cacheEntry;
	uint64		generation;

	ereport(DEBUG1,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("[LFCP] write starting on %u/%u/%u.%d start page %u",
					RelFileInfoFmt(BufTagGetNRelFileInfo(chunk->stateEntry->key)),
					chunk->stateEntry->key.forkNum,
					chunk->stateEntry->key.blockNum),
			 errhidecontext(true),
			 errhidestmt(true)));

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	/* handle error condition */
	if (!LFC_ENABLED())
	{
		LWLockRelease(lfc_lock);
		lfcp_release_chunk(chunk, lfc_ctl->generation);
		return;
	}

	/* Prewarming 6.1 */
	fcentry->prewarm_active = true;
	/* Prewarming 6.3 */
	lfc_ctl->worker.wait_count = fcentry->access_count - 1;
	generation = lfc_ctl->generation;

	/* Prewarming exit 6 */
	LWLockRelease(lfc_lock);

	Assert(chunk->npages > 0);

	/* Prewarming 9.1 */
	if (lfc_ctl->worker.wait_count > 0)
	{
		instr_time	start, end;
		int		max_loops = lfc_ctl->worker.wait_count;

		INSTR_TIME_SET_CURRENT(start);
		ConditionVariablePrepareToSleep(&lfc_ctl->worker.chunk_release);

		while (lfc_ctl->worker.wait_count > 0 && max_loops > 0)
		{
			max_loops--;
			ConditionVariableTimedSleep(&lfc_ctl->worker.chunk_release,
										10, WAIT_EVENT_NEON_LFC_PREWARM_IO);
		}

		ConditionVariableCancelSleep();
		INSTR_TIME_SET_CURRENT(end);

		if (lfc_ctl->worker.wait_count > 0)
			elog(ERROR, "Prewarm waiting for too long; %d readers remaining after %0.3f seconds",
				 lfc_ctl->worker.wait_count, INSTR_TIME_GET_DOUBLE(end));

		INSTR_TIME_SUBTRACT(end, start);
		lfc_ctl->worker.wait_time += INSTR_TIME_GET_MICROSEC(end);
	}

	/* Prewarming 9.2 */
	do {
		int		i;
		int		start = chunk->loaded;
		int		writes_remaining = chunk->npages - chunk->loaded;
		BlockNumber base = chunk->blknos[chunk->loaded];

		/*
		 * Merge IOs
		 * 
		 * "i" will be the number of blocks written starting with `chunk->loaded`
		 */
		for (i = 0; i < writes_remaining; i++)
		{
			if (chunk->blknos[start + i] != base + i)
				break;

			/* block has already been written concurrently */
			if (fcentry->bitmap[(start + i) / 32] & 1 << ((start + i) % 32))
				break;
		}

		/*
		 * If the current block was written after we started fetching blocks,
		 * then i == 0. Else, write the combined pages in a single IO.
		 */
		if (i > 0)
		{
			size_t	len = i * BLCKSZ;
			off_t	file_off =
				((off_t) fcentry->offset) * BLOCKS_PER_CHUNK * BLCKSZ +
				((off_t) start * BLCKSZ);
			char   *dataptr = chunk->pages[start].data;

			do {
				/* no 'pwritev' necessary, nor possible */
				int	len_written = (int) pwrite(lfc_desc, dataptr, len,
											   file_off);

				if (len_written < 0)
				{
					elog(ERROR, "LFC Prewarm: Error writing data to LFC: %m");
				}
				file_off += len_written;
				dataptr += len_written;
				len -= len_written;
			} while (len > 0);

			chunk->loaded += i;
			state->lpws_pages_loaded += i;
		}
		else
		{
			/*
			 * Current page was written to after we started prewarming, but
			 * before we received all pages for this chunk.
			 */
			Assert(fcentry->bitmap[start / 32] & 1 << (start % 32));
			chunk->loaded += 1;
			state->lpws_pages_discarded += 1;
		}
	} while (chunk->loaded < chunk->npages);

	/* remove this chunk from io read stats */
	state->lpws_pages_prefetched -= chunk->npages;
	state->lpws_pages_read -= chunk->npages;

	/* Prewarming 10 and 11 */
	/* handles signalling & ready-marking of data */
	lfcp_release_chunk(chunk, generation);
}

/* Handles prewarming step 10 and 11  */
static void
lfcp_release_chunk(LFCPrewarmChunk *chunk, uint64 generation)
{
	FileCacheEntry *entry = chunk->cacheEntry;
	/* Prewarming 10 entry */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	/* Handle error condition */
	if (lfc_ctl->generation != generation)
	{
		release_entry(entry, false);
		goto cleanup_and_return;
	}

	/* Prewarming 10.2 */
	entry->prewarm_active = false;
	entry->prewarm_selected = false;

	/* Prewarming 10.1 */
	if (chunk->loaded == chunk->npages)
	{
		/* mark pages 'written' for this chunk */
		for (int i = 0; i < CHUNK_BITMAP_SIZE; i++)
		{
			entry->bitmap[i] |= chunk->stateEntry->bitmap[i];
		}
	}

	/* Prewarming 10.3; exit 10 */
	release_entry(entry, false);

	/* Prewarming 11 */
	ConditionVariableSignal(&lfc_ctl->worker.prewarm_done);

	ereport(DEBUG1,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("[LFCP] finished work on %u/%u/%u.%d start page %u",
					RelFileInfoFmt(BufTagGetNRelFileInfo(chunk->stateEntry->key)),
					chunk->stateEntry->key.forkNum,
					chunk->stateEntry->key.blockNum),
			 errhidecontext(true),
			 errhidestmt(true)));

	/* Cleanup, return */
cleanup_and_return:
	dlist_delete(&chunk->node);
	pfree(chunk->alloc);
	pfree(chunk);
}

/*
 * We assume we only get a small amount of buffers, where O(n^2) behaviour
 * doesn't matter much.
 *
 * bits in *stored are set for pages we won't need anymore, for any of
 * these reasons:
 * - The page was written to LFC
 * - The page was already present in the LFC
 * - The page version (per parameter lsns) was too old
 *
 * Note that we may not always set "already present" when the page is actually
 * present in the LFC.
 */
void
lfc_sideload_data(const Page *pages, const BufferTag *bufhdrs,
				  const XLogRecPtr *lsns, bits8 *removable, int npages,
				  int *n_added, int *n_discarded, int *n_expired)
{
	LFCPrewarmWorkerState state;
	FileCacheStateEntry stateEntry;
	bits8		   *processed;

	Assert(npages < INT16_MAX);

	if (npages == 0)
		return;

	Assert(npages > 0);

	memset(removable, 0, (npages + 7) / 8);
	processed = palloc0((npages + 7) / 8);

	/* we don't use these two fields */
	state.lpws_numrestore = 0;
	state.lpws_fcses = NULL;
	/* ... but do use these fields over here; only we fill them in later */
	state.lpws_pages_prefetched = 0;
	state.lpws_pages_read = 0;
	state.lpws_pages_loaded = 0;
	state.lpws_pages_discarded = 0;
	state.lpws_max_io_depth = INT_MAX;	/* not relevant in sideloading with
										 * pre-allocated pages */
	dlist_init(&state.lpws_work);

	for (int16 i = 0; i < (int16) npages; i++)
	{
		int16		input_offsets[BLOCKS_PER_CHUNK];
		int			nblocks = 0;
		bool		load_needed;
		LFCPrewarmChunk *chunk;

		if (BITMAP_ISSET(processed, i))
			continue;

		for (int off = 0; off < BLOCKS_PER_CHUNK; off++)
			input_offsets[off] = -1;
		memset(&stateEntry, 0, sizeof(FileCacheStateEntry));

		stateEntry.key = bufhdrs[i];
		stateEntry.key.blockNum = stateEntry.key.blockNum -
			(stateEntry.key.blockNum % BLOCKS_PER_CHUNK);

		for (int16 j = i; j < (int16) npages; j++)
		{
			int chunk_offset;

			if (memcmp(&BufTagGetNRelFileInfo(stateEntry.key),
					   &BufTagGetNRelFileInfo(bufhdrs[j]),
					   sizeof(NRelFileInfo)) != 0)
				continue;

			if (stateEntry.key.forkNum != bufhdrs[j].forkNum)
				continue;

			chunk_offset = (int) (bufhdrs[j].blockNum % BLOCKS_PER_CHUNK);

			if (stateEntry.key.blockNum != (bufhdrs[j].blockNum - chunk_offset))
				continue;

			/* save offset mapping */
			nblocks++;
			input_offsets[chunk_offset] = j;
			/* same chunk, no need for reprocessing */
			BITMAP_SET(processed, j);
			stateEntry.bitmap[chunk_offset / 32] |= 1 << chunk_offset % 32;
		}

		/*
		 * We've now completely filled the state entry with prefetched pages,
		 * allowing us to process the data as one (almost) normally would.
		 */

		Assert(nblocks > 0);

		/* Takes care of steps 1 through 3 of the prewarm system */
		chunk = lfcp_preprocess_chunk(&stateEntry, true, &load_needed);

		if (!chunk)
		{
			/*
			 * We didn't need to sideload data, so we can safely discard the
			 * pages.
			 */
			if (!load_needed)
			{
				for (int chunk_off = 0; chunk_off < BLOCKS_PER_CHUNK; chunk_off++)
				{
					int16	offset = input_offsets[chunk_off];

					if (offset != -1)
						BITMAP_SET(removable, offset);
				}
			}

			/*
			 * We didn't find space for the prewarm operation, or no operations
			 * were necessary for this chunk. Anyway, continue on with the
			 * next, as there are no resources that need to be cleaned up.
			 */
			continue;
		}

		Assert(chunk->npages <= nblocks && chunk->npages != 0);

		dlist_push_head(&state.lpws_work, &chunk->node);

		/* prepare some suitably aligned memory for IO operations */
#if PG_MAJORVERSION_NUM > 15
		chunk->pages = chunk->alloc = 
				MemoryContextAllocAligned(CurrentMemoryContext,
										  chunk->npages * BLCKSZ,
										  PG_IO_ALIGN_SIZE,
										  0);
#else
		chunk->alloc = palloc((chunk->npages) * BLCKSZ + PG_IO_ALIGN_SIZE);
		chunk->pages = (PGAlignedBlock *)
			TYPEALIGN(PG_IO_ALIGN_SIZE, (Size) chunk->alloc);
#endif

		for (int chunk_off = 0, pno = 0; chunk_off < BLOCKS_PER_CHUNK; chunk_off++)
		{
			int	input_offset = input_offsets[chunk_off];

			/* If we expected to process this page, do that now */
			if (stateEntry.bitmap[chunk_off / 32] & (1 << (chunk_off % 32)))
			{
				XLogRecPtr	lwlsn;
				Assert(input_offsets[chunk_off] != -1);

				lwlsn = GetLastWrittenLSN(BufTagGetNRelFileInfo(stateEntry.key),
										  bufhdrs[input_offset].forkNum,
										  bufhdrs[input_offset].blockNum);
				
				if (lwlsn > lsns[input_offset])
				{
					/* clear references to this struct */
					input_offsets[chunk_off] = -1;
					stateEntry.bitmap[chunk_off / 32] &= ~(1 << (chunk_off % 32));
				}

				memcpy(chunk->pages[pno].data,
					   pages[input_offset],
					   BLCKSZ);

				/* Step 4 and 5 are skipped */
				chunk->prefetched++; 
				chunk->received++;
				pno++;
				BITMAP_SET(removable, input_offset);
			}
			else if (chunk->cacheEntry->bitmap[chunk_off / 32] & (1 << (chunk_off % 32)))
			{
				BITMAP_SET(removable, input_offset);
			}
		}

		/* chunk and chunk->alloc are free-ed by pump_load */
		/* Steps 6 through 11 are handled in pump_load */
		lfcp_pump_load(&state);
	}

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	lfc_ctl->prewarmed_pages += state.lpws_pages_loaded;
	lfc_ctl->skipped_pages += state.lpws_pages_discarded;
	LWLockRelease(lfc_lock);
}

