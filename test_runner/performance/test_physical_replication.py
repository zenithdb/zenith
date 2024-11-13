from __future__ import annotations

import csv
import os
import subprocess
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg2
import psycopg2.extras
import pytest
from fixtures.benchmark_fixture import MetricReport
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_api import connection_parameters_to_env
from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    from typing import Any, Optional

    from fixtures.benchmark_fixture import NeonBenchmarker
    from fixtures.neon_api import NeonAPI
    from fixtures.neon_fixtures import PgBin


# Granularity of ~0.5 sec
def measure_replication_lag(master, replica, timeout_sec=600):
    start = time.time()
    master.execute("SELECT pg_current_wal_flush_lsn()")
    master_lsn = Lsn(master.fetchall()[0][0])
    while (time.time() - start) < timeout_sec:
        replica.execute("select pg_last_wal_replay_lsn()")
        replica_lsn = replica.fetchall()[0][0]
        if replica_lsn:
            if Lsn(replica_lsn) >= master_lsn:
                return time.time() - start
        time.sleep(0.5)
    raise TimeoutError(f"Replication sync took more than {timeout_sec} sec")


def check_pgbench_still_running(pgbench):
    rc = pgbench.poll()
    if rc is not None:
        raise RuntimeError(f"Pgbench terminated early with return code {rc}")


@pytest.mark.remote_cluster
@pytest.mark.timeout(2 * 60 * 60)
def test_ro_replica_lag(
    pg_bin: PgBin,
    neon_api: NeonAPI,
    pg_version: PgVersion,
    zenbenchmark: NeonBenchmarker,
):
    test_duration_min = 60
    sync_interval_min = 10

    pgbench_duration = f"-T{test_duration_min * 60 * 2}"

    project = neon_api.create_project(pg_version)
    project_id = project["project"]["id"]
    neon_api.wait_for_operation_to_finish(project_id)
    error_occurred = False
    try:
        branch_id = project["branch"]["id"]
        master_connstr = project["connection_uris"][0]["connection_uri"]
        master_env = connection_parameters_to_env(
            project["connection_uris"][0]["connection_parameters"]
        )

        replica = neon_api.create_endpoint(
            project_id,
            branch_id,
            endpoint_type="read_only",
            settings={"pg_settings": {"hot_standby_feedback": "on"}},
        )
        replica_env = master_env.copy()
        replica_env["PGHOST"] = replica["endpoint"]["host"]
        neon_api.wait_for_operation_to_finish(project_id)

        replica_connstr = neon_api.get_connection_uri(
            project_id,
            endpoint_id=replica["endpoint"]["id"],
        )["uri"]

        pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", "-s100"], env=master_env)

        master_workload = pg_bin.run_nonblocking(
            ["pgbench", "-c10", pgbench_duration, "-Mprepared"],
            env=master_env,
        )
        try:
            replica_workload = pg_bin.run_nonblocking(
                ["pgbench", "-c10", pgbench_duration, "-S"],
                env=replica_env,
            )
            try:
                start = time.time()
                while time.time() - start < test_duration_min * 60:
                    check_pgbench_still_running(master_workload)
                    check_pgbench_still_running(replica_workload)
                    time.sleep(sync_interval_min * 60)

                    conn_master = psycopg2.connect(master_connstr)
                    conn_replica = psycopg2.connect(replica_connstr)
                    conn_master.autocommit = True
                    conn_replica.autocommit = True

                    with (
                        conn_master.cursor() as cur_master,
                        conn_replica.cursor() as cur_replica,
                    ):
                        lag = measure_replication_lag(cur_master, cur_replica)

                    conn_master.close()
                    conn_replica.close()

                    log.info(f"Replica lagged behind master by {lag} seconds")
                    zenbenchmark.record("replica_lag", lag, "s", MetricReport.LOWER_IS_BETTER)
            finally:
                replica_workload.terminate()
        finally:
            master_workload.terminate()
    except Exception as e:
        error_occurred = True
        log.error(f"Caught exception: {e}")
        log.error(traceback.format_exc())
    finally:
        assert not error_occurred  # Fail the test if an error occurred
        neon_api.delete_project(project_id)


def report_pgbench_aggregate_intervals(
    output_dir: Path,
    prefix: str,
    zenbenchmark: NeonBenchmarker,
):
    for filename in os.listdir(output_dir):
        if filename.startswith(prefix):
            # The file will be in the form <prefix>_<node>.<pid>
            # So we first lop off the .<pid>, and then lop off the prefix and the _
            node = filename.split(".")[0][len(prefix) + 1 :]
            with open(output_dir / filename) as f:
                reader = csv.reader(f, delimiter=" ")
                for line in reader:
                    num_transactions = int(line[1])
                    if num_transactions == 0:
                        continue
                    sum_latency = int(line[2])
                    sum_lag = int(line[3])
                    zenbenchmark.record(
                        f"{node}_num_txns", num_transactions, "txns", MetricReport.HIGHER_IS_BETTER
                    )
                    zenbenchmark.record(
                        f"{node}_avg_latency",
                        sum_latency / num_transactions,
                        "s",
                        MetricReport.LOWER_IS_BETTER,
                    )
                    zenbenchmark.record(
                        f"{node}_avg_lag",
                        sum_lag / num_transactions,
                        "s",
                        MetricReport.LOWER_IS_BETTER,
                    )


@pytest.mark.remote_cluster
@pytest.mark.timeout(2 * 60 * 60)
def test_replication_start_stop(
    pg_bin: PgBin,
    test_output_dir: Path,
    neon_api: NeonAPI,
    pg_version: PgVersion,
    zenbenchmark: NeonBenchmarker,
):
    """
    Cycles through different configurations of read replicas being enabled disabled. The whole time,
    there's a pgbench read/write workload going on the master. For each replica, we either turn it
    on or off, and see how long it takes to catch up after some set amount of time of replicating
    the pgbench.
    """

    prefix = "pgbench_agg"
    num_replicas = 2
    configuration_test_time_sec = 10 * 60
    pgbench_duration = f"-T{2 ** num_replicas * configuration_test_time_sec}"
    error_occurred = False

    project = neon_api.create_project(pg_version)
    project_id = project["project"]["id"]
    neon_api.wait_for_operation_to_finish(project_id)
    try:
        branch_id = project["branch"]["id"]
        master_connstr = project["connection_uris"][0]["connection_uri"]
        master_env = connection_parameters_to_env(
            project["connection_uris"][0]["connection_parameters"]
        )

        replicas = []
        for _ in range(num_replicas):
            replicas.append(
                neon_api.create_endpoint(
                    project_id,
                    branch_id,
                    endpoint_type="read_only",
                    settings={"pg_settings": {"hot_standby_feedback": "on"}},
                )
            )
            neon_api.wait_for_operation_to_finish(project_id)

        replica_connstr = [
            neon_api.get_connection_uri(
                project_id,
                endpoint_id=replicas[i]["endpoint"]["id"],
            )["uri"]
            for i in range(num_replicas)
        ]
        replica_env = [master_env.copy() for _ in range(num_replicas)]
        for i in range(num_replicas):
            replica_env[i]["PGHOST"] = replicas[i]["endpoint"]["host"]

        pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", "-s10"], env=master_env)

        # Sync replicas
        conn_master = psycopg2.connect(master_connstr)
        conn_master.autocommit = True

        with conn_master.cursor() as cur_master:
            for i in range(num_replicas):
                conn_replica = psycopg2.connect(replica_connstr[i])
                measure_replication_lag(cur_master, conn_replica.cursor())

        conn_master.close()

        master_pgbench = pg_bin.run_nonblocking(
            [
                "pgbench",
                "-c10",
                pgbench_duration,
                "-Mprepared",
                "--log",
                f"--log-prefix={test_output_dir}/{prefix}_master",
                f"--aggregate-interval={configuration_test_time_sec}",
            ],
            env=master_env,
        )
        replica_pgbench: list[Optional[subprocess.Popen[Any]]] = [None for _ in range(num_replicas)]

        # Use the bits of iconfig to tell us which configuration we are on. For example
        # a iconfig of 2 is 10 in binary, indicating replica 0 is suspended and replica 1 is
        # alive.
        for iconfig in range((1 << num_replicas) - 1, -1, -1):

            def replica_enabled(iconfig: int = iconfig):
                return bool((iconfig >> 1) & 1)

            # Change configuration
            for ireplica in range(num_replicas):
                if replica_enabled() and replica_pgbench[ireplica] is None:
                    replica_pgbench[ireplica] = pg_bin.run_nonblocking(
                        [
                            "pgbench",
                            "-c10",
                            "-S",
                            pgbench_duration,
                            "--log",
                            f"--log-prefix={test_output_dir}/{prefix}_replica_{ireplica}",
                            f"--aggregate-interval={configuration_test_time_sec}",
                        ],
                        env=replica_env[ireplica],
                    )
                elif not replica_enabled() and replica_pgbench[ireplica] is not None:
                    pgb = replica_pgbench[ireplica]
                    assert pgb is not None
                    pgb.terminate()
                    pgb.wait()
                    replica_pgbench[ireplica] = None

                    neon_api.suspend_endpoint(
                        project_id,
                        replicas[ireplica]["endpoint"]["id"],
                    )
                    neon_api.wait_for_operation_to_finish(project_id)

            time.sleep(configuration_test_time_sec)

            conn_master = psycopg2.connect(master_connstr)
            conn_master.autocommit = True

            with conn_master.cursor() as cur_master:
                for ireplica in range(num_replicas):
                    replica_conn = psycopg2.connect(replica_connstr[ireplica])
                    lag = measure_replication_lag(cur_master, replica_conn.cursor())
                    zenbenchmark.record(
                        f"Replica {ireplica} lag", lag, "s", MetricReport.LOWER_IS_BETTER
                    )
                    log.info(
                        f"Replica {ireplica} lagging behind master by {lag} seconds after configuration {iconfig:>b}"
                    )

            conn_master.close()

        master_pgbench.terminate()
    except Exception as e:
        error_occurred = True
        log.error(f"Caught exception {e}")
        log.error(traceback.format_exc())
    finally:
        assert not error_occurred
        neon_api.delete_project(project_id)
        # Only report results if we didn't error out
        report_pgbench_aggregate_intervals(test_output_dir, prefix, zenbenchmark)
