/*-------------------------------------------------------------------------
 *
 * neon_perf_counters.c
 *	  Collect statistics about Neon I/O
 *
 * Each backend has its own set of counters in shared memory.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"

#include "neon_perf_counters.h"
#include "neon_pgversioncompat.h"

neon_per_backend_counters *neon_per_backend_counters_shared;

Size
NeonPerfCountersShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, mul_size(NUM_NEON_PERF_COUNTER_SLOTS,
								   sizeof(neon_per_backend_counters)));

	return size;
}

void
NeonPerfCountersShmemInit(void)
{
	bool		found;

	neon_per_backend_counters_shared =
		ShmemInitStruct("Neon perf counters",
						mul_size(NUM_NEON_PERF_COUNTER_SLOTS,
								 sizeof(neon_per_backend_counters)),
						&found);
	Assert(found == IsUnderPostmaster);
	if (!found)
	{
		/* shared memory is initialized to zeros, so nothing to do here */
	}
}

static inline void
inc_iohist(IOHistogram hist, uint64 latency_us)
{
	int			lo = 0;
	int			hi = NUM_IO_WAIT_BUCKETS - 1;

	/* Find the right bucket with binary search */
	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (latency_us < io_wait_bucket_thresholds[mid])
			hi = mid;
		else
			lo = mid + 1;
	}
	hist->wait_us_bucket[lo]++;
	hist->wait_us_sum += latency_us;
	hist->wait_us_count++;
}

static inline void
inc_qthist(QTHistogram hist, uint64 elapsed_us)
{
	int			lo = 0;
	int			hi = NUM_QT_BUCKETS - 1;

	/* Find the right bucket with binary search */
	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (elapsed_us < qt_bucket_thresholds[mid])
			hi = mid;
		else
			lo = mid + 1;
	}
	hist->elapsed_us_bucket[lo]++;
	hist->elapsed_us_sum += elapsed_us;
	hist->elapsed_us_count++;
}

/*
 * Count a GetPage wait operation.
 */
void
inc_getpage_wait(uint64 latency)
{
	inc_iohist(&MyNeonCounters->getpage_hist, latency);
}

/*
 * Count an LFC read wait operation.
 */
void
inc_page_cache_read_wait(uint64 latency)
{
	inc_iohist(&MyNeonCounters->file_cache_read_hist, latency);
}

/*
 * Count an LFC write wait operation.
 */
void
inc_page_cache_write_wait(uint64 latency)
{
	inc_iohist(&MyNeonCounters->file_cache_write_hist, latency);
}


void
inc_query_time(uint64 elapsed)
{
	inc_qthist(&MyNeonCounters->query_time_hist, elapsed);
}

/*
 * Support functions for the views, neon_backend_perf_counters and
 * neon_perf_counters.
 */

typedef struct
{
	const char *name;
	bool		is_bucket;
	double		bucket_le;
	double		value;
} metric_t;

static int
io_histogram_to_metrics(IOHistogram histogram,
						metric_t *metrics,
						const char *count,
						const char *sum,
						const char *bucket)
{
	int		i = 0;
	uint64	bucket_accum = 0;

	metrics[i].name = count;
	metrics[i].is_bucket = false;
	metrics[i].value = (double) histogram->wait_us_count;
	i++;
	metrics[i].name = sum;
	metrics[i].is_bucket = false;
	metrics[i].value = (double) histogram->wait_us_sum / 1000000.0;
	i++;
	for (int bucketno = 0; bucketno < NUM_IO_WAIT_BUCKETS; bucketno++)
	{
		uint64		threshold = io_wait_bucket_thresholds[bucketno];

		bucket_accum += histogram->wait_us_bucket[bucketno];

		metrics[i].name = bucket;
		metrics[i].is_bucket = true;
		metrics[i].bucket_le = (threshold == UINT64_MAX) ? INFINITY : ((double) threshold) / 1000000.0;
		metrics[i].value = (double) bucket_accum;
		i++;
	}

	return i;
}

static int
qt_histogram_to_metrics(QTHistogram histogram,
						metric_t *metrics,
						const char *count,
						const char *sum,
						const char *bucket)
{
	int		i = 0;
	uint64	bucket_accum = 0;

	metrics[i].name = count;
	metrics[i].is_bucket = false;
	metrics[i].value = (double) histogram->elapsed_us_count;
	i++;
	metrics[i].name = sum;
	metrics[i].is_bucket = false;
	metrics[i].value = (double) histogram->elapsed_us_sum / 1000000.0;
	i++;
	for (int bucketno = 0; bucketno < NUM_QT_BUCKETS; bucketno++)
	{
		uint64		threshold = qt_bucket_thresholds[bucketno];

		bucket_accum += histogram->elapsed_us_bucket[bucketno];

		metrics[i].name = bucket;
		metrics[i].is_bucket = true;
		metrics[i].bucket_le = (threshold == UINT64_MAX) ? INFINITY : ((double) threshold) / 1000000.0;
		metrics[i].value = (double) bucket_accum;
		i++;
	}

	return i;
}

static metric_t *
neon_perf_counters_to_metrics(neon_per_backend_counters *counters)
{
#define NUM_METRICS ((2 + NUM_IO_WAIT_BUCKETS) * 4 + 10)
	metric_t   *metrics = palloc((NUM_METRICS + 1) * sizeof(metric_t));
	int			i = 0;

#define APPEND_METRIC(_name) do { \
		metrics[i].name = #_name; \
		metrics[i].is_bucket = false; \
		metrics[i].value = (double) counters->_name; \
		i++; \
	} while (false)

	i += io_histogram_to_metrics(&counters->getpage_hist, &metrics[i],
								 "getpage_wait_seconds_count",
								 "getpage_wait_seconds_sum",
								 "getpage_wait_seconds_bucket");

	APPEND_METRIC(getpage_prefetch_requests_total);
	APPEND_METRIC(getpage_sync_requests_total);
	APPEND_METRIC(getpage_prefetch_misses_total);
	APPEND_METRIC(getpage_prefetch_discards_total);
	APPEND_METRIC(pageserver_requests_sent_total);
	APPEND_METRIC(pageserver_disconnects_total);
	APPEND_METRIC(pageserver_send_flushes_total);
	APPEND_METRIC(pageserver_open_requests);
	APPEND_METRIC(getpage_prefetches_buffered);

	APPEND_METRIC(file_cache_hits_total);

	i += io_histogram_to_metrics(&counters->file_cache_read_hist, &metrics[i],
								 "file_cache_read_wait_seconds_count",
								 "file_cache_read_wait_seconds_sum",
								 "file_cache_read_wait_seconds_bucket");
	i += io_histogram_to_metrics(&counters->file_cache_write_hist, &metrics[i],
								 "file_cache_write_wait_seconds_count",
								 "file_cache_write_wait_seconds_sum",
								 "file_cache_write_wait_seconds_bucket");

	i += qt_histogram_to_metrics(&counters->query_time_hist, &metrics[i],
								 "query_time_seconds_count",
								 "query_time_seconds_sum",
								 "query_time_seconds_bucket");

	Assert(i == NUM_METRICS);

#undef APPEND_METRIC
#undef NUM_METRICS

	/* NULL entry marks end of array */
	metrics[i].name = NULL;
	metrics[i].value = 0;

	return metrics;
}

/*
 * Write metric to three output Datums
 */
static void
metric_to_datums(metric_t *m, Datum *values, bool *nulls)
{
	values[0] = CStringGetTextDatum(m->name);
	nulls[0] = false;
	if (m->is_bucket)
	{
		values[1] = Float8GetDatum(m->bucket_le);
		nulls[1] = false;
	}
	else
	{
		values[1] = (Datum) 0;
		nulls[1] = true;
	}
	values[2] = Float8GetDatum(m->value);
	nulls[2] = false;
}

PG_FUNCTION_INFO_V1(neon_get_backend_perf_counters);
Datum
neon_get_backend_perf_counters(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[5];
	bool		nulls[5];

	/* We put all the tuples into a tuplestore in one go. */
	InitMaterializedSRF(fcinfo, 0);

	for (int procno = 0; procno < NUM_NEON_PERF_COUNTER_SLOTS; procno++)
	{
		PGPROC	   *proc = GetPGProcByNumber(procno);
		int			pid = proc->pid;
		neon_per_backend_counters *counters = &neon_per_backend_counters_shared[procno];
		metric_t   *metrics = neon_perf_counters_to_metrics(counters);

		values[0] = Int32GetDatum(procno);
		nulls[0] = false;
		values[1] = Int32GetDatum(pid);
		nulls[1] = false;

		for (int i = 0; metrics[i].name != NULL; i++)
		{
			metric_to_datums(&metrics[i], &values[2], &nulls[2]);
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}

		pfree(metrics);
	}

	return (Datum) 0;
}

static inline void
io_histogram_merge_into(IOHistogram into, IOHistogram from)
{
	into->wait_us_count += from->wait_us_count;
	into->wait_us_sum += from->wait_us_sum;
	for (int bucketno = 0; bucketno < NUM_IO_WAIT_BUCKETS; bucketno++)
		into->wait_us_bucket[bucketno] += from->wait_us_bucket[bucketno];
}

static inline void
qt_histogram_merge_into(QTHistogram into, QTHistogram from)
{
	into->elapsed_us_count += from->elapsed_us_count;
	into->elapsed_us_sum += from->elapsed_us_sum;
	for (int bucketno = 0; bucketno < NUM_QT_BUCKETS; bucketno++)
		into->elapsed_us_bucket[bucketno] += from->elapsed_us_bucket[bucketno];
}

PG_FUNCTION_INFO_V1(neon_get_perf_counters);
Datum
neon_get_perf_counters(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[3];
	bool		nulls[3];
	neon_per_backend_counters totals = {0};
	metric_t   *metrics;

	/* We put all the tuples into a tuplestore in one go. */
	InitMaterializedSRF(fcinfo, 0);

	/* Aggregate the counters across all backends */
	for (int procno = 0; procno < NUM_NEON_PERF_COUNTER_SLOTS; procno++)
	{
		neon_per_backend_counters *counters = &neon_per_backend_counters_shared[procno];

		io_histogram_merge_into(&totals.getpage_hist, &counters->getpage_hist);
		totals.getpage_prefetch_requests_total += counters->getpage_prefetch_requests_total;
		totals.getpage_sync_requests_total += counters->getpage_sync_requests_total;
		totals.getpage_prefetch_misses_total += counters->getpage_prefetch_misses_total;
		totals.getpage_prefetch_discards_total += counters->getpage_prefetch_discards_total;
		totals.pageserver_requests_sent_total += counters->pageserver_requests_sent_total;
		totals.pageserver_disconnects_total += counters->pageserver_disconnects_total;
		totals.pageserver_send_flushes_total += counters->pageserver_send_flushes_total;
		totals.pageserver_open_requests += counters->pageserver_open_requests;
		totals.getpage_prefetches_buffered += counters->getpage_prefetches_buffered;
		totals.file_cache_hits_total += counters->file_cache_hits_total;
		io_histogram_merge_into(&totals.file_cache_read_hist, &counters->file_cache_read_hist);
		io_histogram_merge_into(&totals.file_cache_write_hist, &counters->file_cache_write_hist);
		qt_histogram_merge_into(&totals.query_time_hist, &counters->query_time_hist);
	}

	metrics = neon_perf_counters_to_metrics(&totals);
	for (int i = 0; metrics[i].name != NULL; i++)
	{
		metric_to_datums(&metrics[i], &values[0], &nulls[0]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	pfree(metrics);

	return (Datum) 0;
}
