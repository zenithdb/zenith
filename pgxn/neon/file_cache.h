/*-------------------------------------------------------------------------
 *
 * file_cache.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef file_cache_h
#define file_cache_h

#include "neon_pgversioncompat.h"

/* Local file storage allocation chunk.
 * Should be power of two. Using larger than page chunks can
 * 1. Reduce hash-map memory footprint: 8TB database contains billion pages
 *    and size of hash entry is 40 bytes, so we need 40Gb just for hash map.
 *    1Mb chunks can reduce hash map size to 320Mb.
 * 2. Improve access locality, subsequent pages will be allocated together improving seqscan speed
 */
#define BLOCKS_PER_CHUNK	128 /* 1Mb chunk */
#define CHUNK_BITMAP_SIZE   ((BLOCKS_PER_CHUNK + 31) / 32)

typedef struct
{
	BufferTag	key;
	uint32		bitmap[CHUNK_BITMAP_SIZE];
} FileCacheEntryDesc;

PGDLLEXPORT void FileCachePrewarmMain(Datum main_arg);

extern void lfc_writev(NRelFileInfo rinfo, ForkNumber forkNum,
					   BlockNumber blkno, const void *const *buffers,
					   BlockNumber nblocks);
/* returns number of blocks read, with one bit set in *read for each  */
extern int lfc_readv_select(NRelFileInfo rinfo, ForkNumber forkNum,
							BlockNumber blkno, void **buffers,
							BlockNumber nblocks, bits8 *mask);

extern bool lfc_cache_contains(NRelFileInfo rinfo, ForkNumber forkNum,
							   BlockNumber blkno);
extern int lfc_cache_containsv(NRelFileInfo rinfo, ForkNumber forkNum,
							   BlockNumber blkno, int nblocks, bits8 *bitmap);
extern void lfc_evict(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno);
extern void lfc_init(void);

static inline bool
lfc_read(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		 void *buffer)
{
	bits8		rv = 0;
	return lfc_readv_select(rinfo, forkNum, blkno, &buffer, 1, &rv) == 1;
}

static inline void
lfc_write(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		  const void *buffer)
{
	return lfc_writev(rinfo, forkNum, blkno, &buffer, 1);
}

#endif
