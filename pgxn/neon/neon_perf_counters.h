/*-------------------------------------------------------------------------
 *
 * neon_perf_counters.h
 *	  Performance counters for neon storage requests
 *-------------------------------------------------------------------------
 */

#ifndef NEON_PERF_COUNTERS_H
#define NEON_PERF_COUNTERS_H

#if PG_VERSION_NUM >= 170000
#include "storage/procnumber.h"
#else
#include "storage/backendid.h"
#include "storage/proc.h"
#endif

static const uint64 io_wait_bucket_thresholds[] = {
	       2,        3,        6,        10,  /* 0 us   - 10 us */
	      20,       30,       60,       100,  /* 10 us  - 100 us */
	     200,      300,      600,	   1000,  /* 100 us - 1 ms */
	    2000,     3000,     6000,     10000,  /* 1 ms   - 10 ms */
	   20000,    30000,    60000,    100000,  /* 10 ms  - 100 ms */
	  200000,   300000,   600000,   1000000,  /* 100 ms - 1 s */
	 2000000,  3000000,  6000000,  10000000,  /* 1 s - 10 s */
	UINT64_MAX,
};
#define NUM_IO_WAIT_BUCKETS (lengthof(io_wait_bucket_thresholds))

typedef struct IOHistogramData
{
	uint64		wait_us_count;
	uint64		wait_us_sum;
	uint64		wait_us_bucket[NUM_IO_WAIT_BUCKETS];
} IOHistogramData;

typedef IOHistogramData *IOHistogram;

static const uint64 qt_bucket_thresholds[] = {
	       2,        3,        6,        10,  /* 0 us   - 10 us */
	      20,       30,       60,       100,  /* 10 us  - 100 us */
	     200,      300,      600,	   1000,  /* 100 us - 1 ms */
	    2000,     3000,     6000,     10000,  /* 1 ms   - 10 ms */
	   20000,    30000,    60000,    100000,  /* 10 ms  - 100 ms */
	  200000,   300000,   600000,   1000000,  /* 100 ms - 1 s */
	 2000000,  3000000,  6000000,  10000000,  /* 1 s - 10 s */
	20000000, 30000000, 60000000, 100000000,  /* 10 s - 100 s */
	UINT64_MAX,
};
#define NUM_QT_BUCKETS (lengthof(qt_bucket_thresholds))

typedef struct QTHistogramData
{
	uint64		elapsed_us_count;
	uint64		elapsed_us_sum;
	uint64		elapsed_us_bucket[NUM_QT_BUCKETS];
} QTHistogramData;

typedef QTHistogramData *QTHistogram;

typedef struct
{
	/*
	 * Histogram for how long an smgrread() request needs to wait for response
	 * from pageserver. When prefetching is effective, these wait times can be
	 * lower than the network latency to the pageserver, even zero, if the
	 * page is already readily prefetched whenever we need to read a page.
	 *
	 * Note: we accumulate these in microseconds, because that's convenient in
	 * the backend, but the 'neon_backend_perf_counters' view will convert
	 * them to seconds, to make them more idiomatic as prometheus metrics.
	 */
	IOHistogramData getpage_hist;

	/*
	 * Total number of speculative prefetch Getpage requests and synchronous
	 * GetPage requests sent.
	 */
	uint64		getpage_prefetch_requests_total;
	uint64		getpage_sync_requests_total;

	/*
	 * Total number of readahead misses; consisting of either prefetches that
	 * don't satisfy the LSN bounds, or cases where no readahead was issued
	 * for the read.
	 */
	uint64		getpage_prefetch_misses_total;

	/*
	 * Number of prefetched responses that were discarded becuase the
	 * prefetched page was not needed or because it was concurrently fetched /
	 * modified by another backend.
	 */
	uint64		getpage_prefetch_discards_total;

	/*
	 * Total number of requests send to pageserver. (prefetch_requests_total
	 * and sync_request_total count only GetPage requests, this counts all
	 * request types.)
	 */
	uint64		pageserver_requests_sent_total;

	/*
	 * Number of times the connection to the pageserver was lost and the
	 * backend had to reconnect. Note that this doesn't count the first
	 * connection in each backend, only reconnects.
	 */
	uint64		pageserver_disconnects_total;

	/*
	 * Number of network flushes to the pageserver. Synchronous requests are
	 * flushed immediately, but when prefetching requests are sent in batches,
	 * this can be smaller than pageserver_requests_sent_total.
	 */
	uint64		pageserver_send_flushes_total;
	
	/*
	 * Number of open requests to PageServer.
	 */
	uint64		pageserver_open_requests;

	/*
	 * Number of unused prefetches currently cached in this backend.
	 */
	uint64		getpage_prefetches_buffered;

	/*
	 * Number of requests satisfied from the LFC.
	 *
	 * This is redundant with the server-wide file_cache_hits, but this gives
	 * per-backend granularity, and it's handy to have this in the same place
	 * as counters for requests that went to the pageserver. Maybe move all
	 * the LFC stats to this struct in the future?
	 */
	uint64		file_cache_hits_total;

	/* LFC I/O time buckets */
	IOHistogramData file_cache_read_hist;
	IOHistogramData file_cache_write_hist;

	/*
	 * Histogram of query execution time.
	 */
	QTHistogramData query_time_hist;
} neon_per_backend_counters;

/* Pointer to the shared memory array of neon_per_backend_counters structs */
extern neon_per_backend_counters *neon_per_backend_counters_shared;

/*
 * Size of the perf counters array in shared memory. One slot for each backend
 * and aux process. IOW one for each PGPROC slot, except for slots reserved
 * for prepared transactions, because they're not real processes and cannot do
 * I/O.
 */
#define NUM_NEON_PERF_COUNTER_SLOTS (MaxBackends + NUM_AUXILIARY_PROCS)

#if PG_VERSION_NUM >= 170000
#define MyNeonCounters (&neon_per_backend_counters_shared[MyProcNumber])
#else
#define MyNeonCounters (&neon_per_backend_counters_shared[MyProc->pgprocno])
#endif

extern void inc_getpage_wait(uint64 latency);
extern void inc_page_cache_read_wait(uint64 latency);
extern void inc_page_cache_write_wait(uint64 latency);
extern void inc_query_time(uint64 elapsed);

extern Size NeonPerfCountersShmemSize(void);
extern void NeonPerfCountersShmemInit(void);


#endif							/* NEON_PERF_COUNTERS_H */
