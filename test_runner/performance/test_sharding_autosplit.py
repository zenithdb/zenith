from __future__ import annotations

import concurrent.futures
import re
import threading
from pathlib import Path

import pytest
from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    tenant_get_shards,
)


@pytest.mark.timeout(600)
def test_sharding_autosplit(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Check that sharding, including auto-splitting, "just works" under pgbench workloads.

    This is not a benchmark, but it lives in the same place as benchmarks in order to be run
    on a dedicated node that can sustain some significant throughput.

    Other tests validate the details of shard splitting, error cases etc.  This test is
    the sanity check that it all really works as expected with realistic amounts of data
    and under load.

    Success conditions:
    - Tenants auto-split when their capacity grows
    - Client workloads are not interrupted while that happens
    """

    neon_env_builder.num_pageservers = 8
    neon_env_builder.storage_controller_config = {
        # Split tenants at 500MB: it's up to the storage controller how it interprets this (logical
        # sizes, physical sizes, etc).  We will write this much data logically, therefore other sizes
        # will reliably be greater.
        "split_threshold": 1024 * 1024 * 500
    }

    tenant_conf = {
        # We want layer rewrites to happen as soon as possible (this is the most stressful
        # case for the system), so set PITR interval to something tiny.
        "pitr_interval": "5s",
        # Scaled down thresholds.  We will run at ~1GB scale but would like to emulate
        # the behavior of a system running at ~100GB scale.
        "checkpoint_distance": f"{1024 * 1024}",
        "compaction_threshold": "1",
        "compaction_target_size": f"{1024 * 1024}",
        "image_creation_threshold": "2",
        "image_layer_creation_check_threshold": "0",
    }

    env = neon_env_builder.init_start()

    for ps in env.pageservers:
        ps.allowed_errors.extend(
            [
                # We shut down pageservers while they might have some compaction work going on
                ".*Compaction failed.*shutting down.*"
            ]
        )

    env.storage_controller.allowed_errors.extend(
        [
            # The neon_local functionality for updating computes is flaky for unknown reasons
            ".*Local notification hook failed.*",
            ".*Marking shard.*for notification retry.*",
            ".*Failed to notify compute.*",
        ]
    )

    # Total tenants
    tenant_count = 4

    # Transaction rate: we set this rather than running at full-speed because we
    # might run on a slow node that doesn't cope well with many full-speed pgbenches running concurrently.
    transaction_rate = 100

    class TenantState:
        def __init__(self, timeline_id, endpoint):
            self.timeline_id = timeline_id
            self.endpoint = endpoint

    # Create tenants
    tenants = {}
    for tenant_id in set(TenantId.generate() for _i in range(0, tenant_count)):
        timeline_id = TimelineId.generate()
        env.create_tenant(tenant_id, timeline_id, conf=tenant_conf)
        endpoint = env.endpoints.create("main", tenant_id=tenant_id)
        tenants[tenant_id] = TenantState(timeline_id, endpoint)
        endpoint.start()

    def run_pgbench_init(endpoint):
        pg_bin.run_capture(
            [
                "pgbench",
                "-s50",
                "-i",
                f"postgres://cloud_admin@localhost:{endpoint.pg_port}/postgres",
            ]
        )

    def check_pgbench_output(out_path: str):
        """
        When we run pgbench, we want not just an absence of errors, but also continuous evidence
        of I/O progressing: our shard splitting and migration should not interrrupt the benchmark.
        """
        matched_lines = 0
        stderr = Path(f"{out_path}.stderr").read_text()

        low_watermark = None

        # Apply this as a threshold for what we consider an unacceptable interruption to I/O
        min_tps = transaction_rate // 10

        for line in stderr.split("\n"):
            match = re.match(r"progress: ([0-9\.]+) s, ([0-9\.]+) tps, .* ([0-9]+) failed", line)
            if match is None:
                # Fall back to older-version pgbench output (omits failure count)
                match = re.match(r"progress: ([0-9\.]+) s, ([0-9\.]+) tps, .*", line)
                if match is None:
                    continue
                else:
                    (_time, tps) = match.groups()
                    tps = float(tps)
                    failed = 0
            else:
                (_time, tps, failed) = match.groups()  # type: ignore
                tps = float(tps)
                failed = int(failed)

            matched_lines += 1

            if failed > 0:
                raise RuntimeError(
                    f"pgbench on tenant {endpoint.tenant_id} run at {out_path} has failed > 0"
                )

            if low_watermark is None or low_watermark > tps:
                low_watermark = tps

            # Temporarily disabled: have seen some 0 tps regions on Hetzner runners, but not
            # at the same time as a shard split.
            # if tps < min_tps:
            #     raise RuntimeError(
            #         f"pgbench on tenant {endpoint.tenant_id} run at {out_path} has tps < {min_tps}"
            #     )

        log.info(f"Checked {matched_lines} progress lines, lowest TPS was {min_tps}")

        if matched_lines == 0:
            raise RuntimeError(f"pgbench output at {out_path} contained no progress lines")

    def run_pgbench_main(endpoint):
        out_path = pg_bin.run_capture(
            [
                "pgbench",
                "-s50",
                "-T",
                "180",
                "-R",
                f"{transaction_rate}",
                "-P",
                "1",
                f"postgres://cloud_admin@localhost:{endpoint.pg_port}/postgres",
            ]
        )

        check_pgbench_output(out_path)

    def run_pgbench_read(endpoint):
        out_path = pg_bin.run_capture(
            [
                "pgbench",
                "-s50",
                "-T",
                "30",
                "-R",
                f"{transaction_rate}",
                "-S",
                "-P",
                "1",
                f"postgres://cloud_admin@localhost:{endpoint.pg_port}/postgres",
            ]
        )

        check_pgbench_output(out_path)

    stop_pump = threading.Event()

    def pump_controller():
        # Run a background loop to force the storage controller to run its
        # background work faster than it otherwise would: this helps
        # us:
        #  A) to create a test that runs in a shorter time
        #  B) to create a test that is more intensive by doing the shard migrations
        #     after splits happen more rapidly.
        while not stop_pump.is_set():
            env.storage_controller.reconcile_all()
            stop_pump.wait(0.1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=tenant_count + 1) as pgbench_threads:
        pgbench_futs = []
        for tenant_state in tenants.values():
            fut = pgbench_threads.submit(run_pgbench_init, tenant_state.endpoint)
            pgbench_futs.append(fut)

        log.info("Waiting for pgbench inits")
        for fut in pgbench_futs:
            fut.result()

        pump_fut = pgbench_threads.submit(pump_controller)

        pgbench_futs = []
        for tenant_state in tenants.values():
            fut = pgbench_threads.submit(run_pgbench_main, tenant_state.endpoint)
            pgbench_futs.append(fut)

        log.info("Waiting for pgbench read/write pass")
        for fut in pgbench_futs:
            fut.result()

        stop_pump.set()
        pump_fut.result()

    def assert_all_split():
        for tenant_id in tenants.keys():
            shards = tenant_get_shards(env, tenant_id)
            assert len(shards) == 8

    # This is not a wait_until, because we wanted the splits to happen _while_ pgbench is running: otherwise
    # this test is not properly doing its job of validating that splits work nicely under load.
    assert_all_split()

    env.storage_controller.assert_log_contains(".*Successful auto-split.*")

    # Log timeline sizes, useful for debug, and implicitly validates that the shards
    # are available in the places the controller thinks they should be.
    for tenant_id, tenant_state in tenants.items():
        (shard_zero_id, shard_zero_ps) = tenant_get_shards(env, tenant_id)[0]
        timeline_info = shard_zero_ps.http_client().timeline_detail(
            shard_zero_id, tenant_state.timeline_id
        )
        log.info(f"{shard_zero_id} timeline: {timeline_info}")

    # Run compaction for all tenants, restart endpoint so that on subsequent reads we will
    # definitely hit pageserver for reads.  This compaction passis expected to drop unwanted
    # layers but not do any rewrites (we're still in the same generation)
    for tenant_id, tenant_state in tenants.items():
        tenant_state.endpoint.stop()
        for shard_id, shard_ps in tenant_get_shards(env, tenant_id):
            shard_ps.http_client().timeline_gc(shard_id, tenant_state.timeline_id, gc_horizon=None)
            shard_ps.http_client().timeline_compact(shard_id, tenant_state.timeline_id)
        tenant_state.endpoint.start()

    with concurrent.futures.ThreadPoolExecutor(max_workers=tenant_count) as pgbench_threads:
        pgbench_futs = []
        for tenant_state in tenants.values():
            fut = pgbench_threads.submit(run_pgbench_read, tenant_state.endpoint)
            pgbench_futs.append(fut)

        log.info("Waiting for pgbench read pass")
        for fut in pgbench_futs:
            fut.result()

    env.storage_controller.consistency_check()

    # Restart the storage controller
    env.storage_controller.stop()
    env.storage_controller.start()

    env.storage_controller.consistency_check()

    # Restart all pageservers
    for ps in env.pageservers:
        ps.stop()
        ps.start()

    # Freshen gc_info in Timeline, so that when compaction runs in the background in the
    # subsequent pgbench period, the last_gc_cutoff is updated and enables the conditions for a rewrite to pass.
    for tenant_id, tenant_state in tenants.items():
        for shard_id, shard_ps in tenant_get_shards(env, tenant_id):
            shard_ps.http_client().timeline_gc(shard_id, tenant_state.timeline_id, gc_horizon=None)

    # One last check data remains readable after everything has restarted
    with concurrent.futures.ThreadPoolExecutor(max_workers=tenant_count) as pgbench_threads:
        pgbench_futs = []
        for tenant_state in tenants.values():
            fut = pgbench_threads.submit(run_pgbench_read, tenant_state.endpoint)
            pgbench_futs.append(fut)

        log.info("Waiting for pgbench read pass")
        for fut in pgbench_futs:
            fut.result()

    # Assert that some rewrites happened
    # TODO: uncomment this after https://github.com/neondatabase/neon/pull/7531 is merged
    # assert any(ps.log_contains(".*Rewriting layer after shard split.*") for ps in env.pageservers)
