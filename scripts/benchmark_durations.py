#! /usr/bin/env python3

import argparse
import json
import logging
from typing import Dict

import psycopg2
import psycopg2.extras

"""
The script fetches the durations of benchmarks from the database and stores it in a file compatible with pytest-split plugin.
"""


BENCHMARKS_DURATION_QUERY = """
    SELECT
        DISTINCT parent_suite, suite, test,
        PERCENTILE_DISC(%s) WITHIN GROUP (ORDER BY duration_ms) as percentile_ms
    FROM
        (
            SELECT
                jsonb_array_elements(data -> 'children') ->> 'name' as parent_suite,
                jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') ->> 'name' as suite,
                jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') ->> 'name' as test,
                jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') ->> 'status' as status,
                to_timestamp((jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') -> 'time' -> 'start')::bigint / 1000)::date as timestamp,
                (jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') -> 'time' -> 'duration')::int as duration_ms
            FROM
                regress_test_results
            WHERE
                reference = 'refs/heads/main'
        ) data
    WHERE
        timestamp > CURRENT_DATE - INTERVAL '%s' day
        AND parent_suite = 'test_runner.performance'
        AND status = 'passed'
    GROUP BY
        parent_suite, suite, test
    ;
"""

# For out benchmarks the default distribution for 4 worked produces pretty uneven chunks,
# the total duration varies from 8 to 40 minutes.
# We use some pre-collected durations as a fallback to have a better distribution.
FALLBACK_DURATION = {
    "test_runner/performance/test_branch_creation.py::test_branch_creation_heavy_write[20]": 57.0,
    "test_runner/performance/test_branch_creation.py::test_branch_creation_many_relations": 28.0,
    "test_runner/performance/test_branch_creation.py::test_branch_creation_many[1024]": 71.0,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_pgbench_perf": 27.0,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_read_perf": 11.0,
    "test_runner/performance/test_branching.py::test_compare_child_and_root_write_perf": 30.0,
    "test_runner/performance/test_bulk_insert.py::test_bulk_insert[neon]": 40.0,
    "test_runner/performance/test_bulk_insert.py::test_bulk_insert[vanilla]": 5.0,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[1]": 3.0,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[5]": 10.0,
    "test_runner/performance/test_bulk_tenant_create.py::test_bulk_tenant_create[10]": 19.0,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[10]": 66.0,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[50]": 30.0,
    "test_runner/performance/test_bulk_update.py::test_bulk_update[100]": 60.0,
    "test_runner/performance/test_compaction.py::test_compaction": 77.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_ro_with_pgbench_select_only[neon-5-10-100]": 11.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_ro_with_pgbench_select_only[vanilla-5-10-100]": 16.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_rw_with_pgbench_default[neon-5-10-100]": 11.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_rw_with_pgbench_default[vanilla-5-10-100]": 18.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wal_with_pgbench_default[neon-5-10-100]": 11.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wal_with_pgbench_default[vanilla-5-10-100]": 16.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[neon-10-1]": 11.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[neon-10-10]": 11.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[vanilla-10-1]": 10.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_heavy_write[vanilla-10-10]": 10.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_pgbench_simple_update[neon-5-10-100]": 11.0,
    "test_runner/performance/test_compare_pg_stats.py::test_compare_pg_stats_wo_with_pgbench_simple_update[vanilla-5-10-100]": 16.0,
    "test_runner/performance/test_copy.py::test_copy[neon]": 12.0,
    "test_runner/performance/test_copy.py::test_copy[vanilla]": 10.0,
    "test_runner/performance/test_gc_feedback.py::test_gc_feedback": 284.0,
    "test_runner/performance/test_gist_build.py::test_gist_buffering_build[neon]": 11.0,
    "test_runner/performance/test_gist_build.py::test_gist_buffering_build[vanilla]": 7.0,
    "test_runner/performance/test_latency.py::test_measure_read_latency_heavy_write_workload[neon-1]": 85.0,
    "test_runner/performance/test_latency.py::test_measure_read_latency_heavy_write_workload[vanilla-1]": 29.0,
    "test_runner/performance/test_layer_map.py::test_layer_map": 44.0,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_different_tables[neon]": 16.0,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_different_tables[vanilla]": 67.0,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_same_table[neon]": 67.0,
    "test_runner/performance/test_parallel_copy_to.py::test_parallel_copy_same_table[vanilla]": 80.0,
    "test_runner/performance/test_perf_pgbench.py::test_pgbench[neon-45-10]": 102.0,
    "test_runner/performance/test_perf_pgbench.py::test_pgbench[vanilla-45-10]": 99.0,
    "test_runner/performance/test_random_writes.py::test_random_writes[neon]": 9.0,
    "test_runner/performance/test_random_writes.py::test_random_writes[vanilla]": 2.0,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-100000-100-0]": 4.0,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-10000000-1-0]": 80.0,
    "test_runner/performance/test_seqscans.py::test_seqscans[neon-10000000-1-4]": 68.0,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-100000-100-0]": 0.0,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-10000000-1-0]": 11.0,
    "test_runner/performance/test_seqscans.py::test_seqscans[vanilla-10000000-1-4]": 10.0,
    "test_runner/performance/test_startup.py::test_startup_simple": 2.0,
    "test_runner/performance/test_startup.py::test_startup": 539.0,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[neon_off-10-5-5]": 375.0,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[neon_on-10-5-5]": 370.0,
    "test_runner/performance/test_wal_backpressure.py::test_heavy_write_workload[vanilla-10-5-5]": 94.0,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[neon_off-1000]": 164.0,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[neon_on-1000]": 274.0,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_intensive_init_workload[vanilla-1000]": 949.0,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[neon_off-45-100]": 142.0,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[neon_on-45-100]": 151.0,
    "test_runner/performance/test_wal_backpressure.py::test_pgbench_simple_update_workload[vanilla-45-100]": 182.0,
    "test_runner/performance/test_write_amplification.py::test_write_amplification[neon]": 13.0,
    "test_runner/performance/test_write_amplification.py::test_write_amplification[vanilla]": 16.0,
}


def main(args: argparse.Namespace):
    connstr = args.connstr
    interval_days = args.days
    output = args.output
    percentile = args.percentile

    res: Dict[str, float] = {}

    try:
        logging.info("connecting to the database...")
        with psycopg2.connect(connstr, connect_timeout=30) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                logging.info("fetching benchmarks...")
                cur.execute(BENCHMARKS_DURATION_QUERY, (percentile, interval_days))
                rows = cur.fetchall()
    except psycopg2.OperationalError as exc:
        logging.error("cannot fetch benchmarks duration from the DB due to an error", exc)
        rows = []
        res = FALLBACK_DURATION

    for row in rows:
        pytest_name = f"{row['parent_suite'].replace('.', '/')}/{row['suite']}.py::{row['test']}"
        duration = row["percentile_ms"] / 1000
        logging.info(f"\t{pytest_name}: {duration}")
        res[pytest_name] = duration

    logging.info(f"saving results to {output.name}")
    json.dump(res, output, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get <percentile> of benchmarks duration for the last <N> days"
    )
    parser.add_argument(
        "--output",
        type=argparse.FileType("w"),
        default=".test_durations",
        help="path to output json file (default: .test_durations)",
    )
    parser.add_argument(
        "--percentile",
        type=float,
        default="0.99",
        help="percentile (default: 0.99)",
    )
    parser.add_argument(
        "--days",
        required=False,
        default=10,
        type=int,
        help="how many days to look back for (default: 10)",
    )
    parser.add_argument(
        "connstr",
        help="connection string to the test results database",
    )
    args = parser.parse_args()

    level = logging.INFO
    logging.basicConfig(
        format="%(message)s",
        level=level,
    )

    main(args)
