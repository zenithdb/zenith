from __future__ import annotations

import json
import math
import random
import time
from enum import StrEnum

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PageserverWalReceiverProtocol,
    generate_uploads_and_deletions,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.utils import skip_in_debug_build, wait_until
from fixtures.workload import Workload

AGGRESSIVE_COMPACTION_TENANT_CONF = {
    # Disable gc and compaction. The test runs compaction manually.
    "gc_period": "0s",
    "compaction_period": "0s",
    # Small checkpoint distance to create many layers
    "checkpoint_distance": 1024**2,
    # Compact small layers
    "compaction_target_size": 1024**2,
    "image_creation_threshold": 2,
    # "lsn_lease_length": "0s", -- TODO: would cause branch creation errors, should fix later
}


@skip_in_debug_build("only run with release build")
@pytest.mark.parametrize(
    "wal_receiver_protocol",
    [PageserverWalReceiverProtocol.VANILLA, PageserverWalReceiverProtocol.INTERPRETED],
)
def test_pageserver_compaction_smoke(
    neon_env_builder: NeonEnvBuilder, wal_receiver_protocol: PageserverWalReceiverProtocol
):
    """
    This is a smoke test that compaction kicks in. The workload repeatedly churns
    a small number of rows and manually instructs the pageserver to run compaction
    between iterations. At the end of the test validate that the average number of
    layers visited to gather reconstruct data for a given key is within the empirically
    observed bounds.
    """

    neon_env_builder.pageserver_wal_receiver_protocol = wal_receiver_protocol

    # Effectively disable the page cache to rely only on image layers
    # to shorten reads.
    neon_env_builder.pageserver_config_override = """
page_cache_size=10
"""

    env = neon_env_builder.init_start(initial_tenant_conf=AGGRESSIVE_COMPACTION_TENANT_CONF)

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    row_count = 10000
    churn_rounds = 100

    ps_http = env.pageserver.http_client()

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageserver.id)

    log.info("Writing initial data ...")
    workload.write_rows(row_count, env.pageserver.id)

    for i in range(1, churn_rounds + 1):
        if i % 10 == 0:
            log.info(f"Running churn round {i}/{churn_rounds} ...")

        workload.churn_rows(row_count, env.pageserver.id)
        # Force L0 compaction to ensure the number of layers is within bounds; we don't want to count L0 layers
        # in this benchmark. In other words, this smoke test ensures number of L1 layers are bound.
        ps_http.timeline_compact(tenant_id, timeline_id, force_l0_compaction=True)
        assert ps_http.perf_info(tenant_id, timeline_id)[0]["num_of_l0"] <= 1

    log.info("Validating at workload end ...")
    workload.validate(env.pageserver.id)

    log.info("Checking layer access metrics ...")

    layer_access_metric_names = [
        "pageserver_layers_per_read_global_sum",
        "pageserver_layers_per_read_global_count",
        "pageserver_layers_per_read_global_bucket",
    ]

    metrics = env.pageserver.http_client().get_metrics()
    for name in layer_access_metric_names:
        layer_access_metrics = metrics.query_all(name)
        log.info(f"Got metrics: {layer_access_metrics}")

    vectored_sum = metrics.query_one("pageserver_layers_per_read_global_sum")
    vectored_count = metrics.query_one("pageserver_layers_per_read_global_count")
    if vectored_count.value > 0:
        assert vectored_sum.value > 0
        vectored_average = vectored_sum.value / vectored_count.value
    else:
        # special case: running local tests with default legacy configuration
        assert vectored_sum.value == 0
        vectored_average = 0

    log.info(f"{vectored_average=}")

    # The upper bound for average number of layer visits below (8)
    # was chosen empirically for this workload.
    assert vectored_average < 8


@skip_in_debug_build("only run with release build")
@pytest.mark.parametrize(
    "with_branches",
    ["with_branches", "no_branches"],
)
def test_pageserver_gc_compaction_smoke(neon_env_builder: NeonEnvBuilder, with_branches: str):
    SMOKE_CONF = {
        # Run both gc and gc-compaction.
        "gc_period": "5s",
        "compaction_period": "5s",
        # No PiTR interval and small GC horizon
        "pitr_interval": "0s",
        "gc_horizon": f"{1024 ** 2}",
        "lsn_lease_length": "0s",
    }

    env = neon_env_builder.init_start(initial_tenant_conf=SMOKE_CONF)
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    row_count = 10000
    churn_rounds = 50

    ps_http = env.pageserver.http_client()

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageserver.id)

    log.info("Writing initial data ...")
    workload.write_rows(row_count, env.pageserver.id)

    ps_http.timeline_gc(
        tenant_id, timeline_id, None
    )  # Force refresh gc info to have gc_cutoff generated

    child_workloads: list[Workload] = []

    for i in range(1, churn_rounds + 1):
        log.info(f"Running churn round {i}/{churn_rounds} ...")
        if i % 10 == 5 and with_branches == "with_branches":
            branch_name = f"child-{i}"
            branch_timeline_id = env.create_branch(branch_name)
            child_workloads.append(workload.branch(branch_timeline_id, branch_name))
        if (i - 1) % 10 == 0 or (i - 1) % 10 == 1:
            # Run gc-compaction twice every 10 rounds to ensure the test doesn't take too long time.
            ps_http.timeline_compact(
                tenant_id,
                timeline_id,
                enhanced_gc_bottom_most_compaction=True,
                body={
                    "scheduled": True,
                    "sub_compaction": True,
                    "compact_key_range": {
                        "start": "000000000000000000000000000000000000",
                        "end": "030000000000000000000000000000000000",
                    },
                    "sub_compaction_max_job_size_mb": 16,
                },
            )
        # do not wait for upload so that we can see if gc_compaction works well with data being ingested
        workload.churn_rows(row_count, env.pageserver.id, upload=False)
        time.sleep(1)
        workload.validate(env.pageserver.id)

    def compaction_finished():
        queue_depth = len(ps_http.timeline_compact_info(tenant_id, timeline_id))
        assert queue_depth == 0

    wait_until(compaction_finished, timeout=60)

    # ensure gc_compaction is scheduled and it's actually running (instead of skipping due to no layers picked)
    env.pageserver.assert_log_contains(
        "scheduled_compact_timeline.*picked .* layers for compaction"
    )

    log.info("Validating at workload end ...")
    workload.validate(env.pageserver.id)
    for child_workload in child_workloads:
        log.info(f"Validating at branch {child_workload.branch_name}")
        child_workload.validate(env.pageserver.id)

    # Run a legacy compaction+gc to ensure gc-compaction can coexist with legacy compaction.
    ps_http.timeline_checkpoint(tenant_id, timeline_id, wait_until_uploaded=True)
    ps_http.timeline_gc(tenant_id, timeline_id, None)


@pytest.mark.parametrize(
    "compaction_mode",
    ["before_restart", "after_restart"],
)
def test_pageserver_gc_compaction_idempotent(
    neon_env_builder: NeonEnvBuilder, compaction_mode: str
):
    """
    Do gc-compaction twice without writing any new data and see if anything breaks.
    We run this test in two modes:
    - before_restart: run two gc-compactions before pageserver restart
    - after_restart: run one gc-compaction before and one after pageserver restart
    """
    SMOKE_CONF = {
        # Run both gc and gc-compaction.
        "gc_period": "5s",
        "compaction_period": "5s",
        # No PiTR interval and small GC horizon
        "pitr_interval": "0s",
        "gc_horizon": 1024,
        "lsn_lease_length": "0s",
    }

    env = neon_env_builder.init_start(initial_tenant_conf=SMOKE_CONF)
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Only in testing mode: the warning is expected because we rewrite a layer file of different generations.
    # We could potentially patch the sanity-check code to not emit the warning in the future.
    env.pageserver.allowed_errors.append(".*was unlinked but was not dangling.*")

    row_count = 10000

    ps_http = env.pageserver.http_client()

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageserver.id)

    workload.write_rows(row_count, env.pageserver.id)

    child_workloads: list[Workload] = []

    def compaction_finished():
        queue_depth = len(ps_http.timeline_compact_info(tenant_id, timeline_id))
        assert queue_depth == 0

    workload.churn_rows(row_count, env.pageserver.id)
    env.create_branch("child_branch")  # so that we have a retain_lsn
    workload.churn_rows(row_count, env.pageserver.id)
    # compact 3 times if mode is before_restart
    n_compactions = 3 if compaction_mode == "before_restart" else 1
    ps_http.timeline_compact(
        tenant_id, timeline_id, force_l0_compaction=True, wait_until_uploaded=True
    )
    for _ in range(n_compactions):
        # Force refresh gc info to have gc_cutoff generated
        ps_http.timeline_gc(tenant_id, timeline_id, None)
        ps_http.timeline_compact(
            tenant_id,
            timeline_id,
            enhanced_gc_bottom_most_compaction=True,
            body={
                "scheduled": True,
                "sub_compaction": True,
                "compact_key_range": {
                    "start": "000000000000000000000000000000000000",
                    "end": "030000000000000000000000000000000000",
                },
                "sub_compaction_max_job_size_mb": 16,
            },
        )
        wait_until(compaction_finished, timeout=60)
    if compaction_mode == "after_restart":
        env.pageserver.restart(True)
        ps_http.timeline_gc(
            tenant_id, timeline_id, None
        )  # Force refresh gc info to have gc_cutoff generated
        for _ in range(3):
            ps_http.timeline_compact(
                tenant_id,
                timeline_id,
                enhanced_gc_bottom_most_compaction=True,
                body={
                    "scheduled": True,
                    "sub_compaction": True,
                    "compact_key_range": {
                        "start": "000000000000000000000000000000000000",
                        "end": "030000000000000000000000000000000000",
                    },
                    "sub_compaction_max_job_size_mb": 16,
                },
            )
            wait_until(compaction_finished, timeout=60)

    # ensure gc_compaction is scheduled and it's actually running (instead of skipping due to no layers picked)
    env.pageserver.assert_log_contains(
        "scheduled_compact_timeline.*picked .* layers for compaction"
    )

    # ensure we hit the duplicated layer key warning at least once: we did two compactions consecutively,
    # and the second one should have hit the duplicated layer key warning.
    if compaction_mode == "before_restart":
        env.pageserver.assert_log_contains("duplicated layer key in the same generation")
    else:
        env.pageserver.assert_log_contains("same layer key at different generation")

    log.info("Validating at workload end ...")
    workload.validate(env.pageserver.id)
    for child_workload in child_workloads:
        log.info(f"Validating at branch {child_workload.branch_name}")
        child_workload.validate(env.pageserver.id)

    # Run a legacy compaction+gc to ensure gc-compaction can coexist with legacy compaction.
    ps_http.timeline_checkpoint(tenant_id, timeline_id, wait_until_uploaded=True)
    ps_http.timeline_gc(tenant_id, timeline_id, None)


@skip_in_debug_build("only run with release build")
def test_pageserver_gc_compaction_interrupt(neon_env_builder: NeonEnvBuilder):
    """
    Force interrupt a gc-compaction and see if anything breaks.
    """
    SMOKE_CONF = {
        # Run both gc and gc-compaction.
        "gc_period": "5s",
        "compaction_period": "5s",
        # No PiTR interval and small GC horizon
        "pitr_interval": "0s",
        "gc_horizon": "1024",
        "lsn_lease_length": "0s",
    }

    env = neon_env_builder.init_start(initial_tenant_conf=SMOKE_CONF)
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Only in testing mode: the warning is expected because we rewrite a layer file of different generations.
    # We could potentially patch the sanity-check code to not emit the warning in the future.
    env.pageserver.allowed_errors.append(".*was unlinked but was not dangling.*")

    row_count = 10000
    churn_rounds = 20

    ps_http = env.pageserver.http_client()

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageserver.id)

    log.info("Writing initial data ...")
    workload.write_rows(row_count, env.pageserver.id)

    def compaction_finished():
        queue_depth = len(ps_http.timeline_compact_info(tenant_id, timeline_id))
        assert queue_depth == 0

    expected_compaction_time_seconds = 5.0
    ps_http.timeline_gc(
        tenant_id, timeline_id, None
    )  # Force refresh gc info to have gc_cutoff generated
    for i in range(1, churn_rounds + 1):
        log.info(f"Running churn round {i}/{churn_rounds} ...")
        workload.churn_rows(row_count, env.pageserver.id)
        ps_http.timeline_compact(
            tenant_id,
            timeline_id,
            enhanced_gc_bottom_most_compaction=True,
            body={
                "scheduled": True,
                "sub_compaction": True,
                "compact_key_range": {
                    "start": "000000000000000000000000000000000000",
                    "end": "030000000000000000000000000000000000",
                },
                "sub_compaction_max_job_size_mb": 16,
            },
        )
        # sleep random seconds between 0 and max(compaction_time); if the result is 0, wait until the compaction is complete
        # This would hopefully trigger the restart at different periods of the compaction:
        # - while we are doing the compaction
        # - while we finished the compaction but not yet uploaded the metadata
        # - after we uploaded the metadata
        time_to_sleep = random.randint(0, max(5, math.ceil(expected_compaction_time_seconds)))
        if time_to_sleep == 0 or i == 1:
            start = time.time()
            wait_until(compaction_finished, timeout=60)
            end = time.time()
            expected_compaction_time_seconds = end - start
            log.info(
                f"expected_compaction_time_seconds updated to {expected_compaction_time_seconds} seconds"
            )
        else:
            time.sleep(time_to_sleep)
        env.pageserver.restart(True)
        ps_http.timeline_gc(
            tenant_id, timeline_id, None
        )  # Force refresh gc info to have gc_cutoff generated
        ps_http.timeline_compact(
            tenant_id,
            timeline_id,
            enhanced_gc_bottom_most_compaction=True,
            body={
                "scheduled": True,
                "sub_compaction": True,
                "compact_key_range": {
                    "start": "000000000000000000000000000000000000",
                    "end": "030000000000000000000000000000000000",
                },
                "sub_compaction_max_job_size_mb": 16,
            },
        )
        workload.validate(env.pageserver.id)

    wait_until(compaction_finished, timeout=60)

    # ensure gc_compaction is scheduled and it's actually running (instead of skipping due to no layers picked)
    env.pageserver.assert_log_contains(
        "scheduled_compact_timeline.*picked .* layers for compaction"
    )

    log.info("Validating at workload end ...")
    workload.validate(env.pageserver.id)

    # Run a legacy compaction+gc to ensure gc-compaction can coexist with legacy compaction.
    ps_http.timeline_checkpoint(tenant_id, timeline_id, wait_until_uploaded=True)
    ps_http.timeline_gc(tenant_id, timeline_id, None)


# Stripe sizes in number of pages.
TINY_STRIPES = 16
LARGE_STRIPES = 32768


@pytest.mark.parametrize(
    "shard_count,stripe_size,gc_compaction",
    [
        (None, None, False),
        (4, TINY_STRIPES, False),
        (4, LARGE_STRIPES, False),
        (4, LARGE_STRIPES, True),
    ],
)
def test_sharding_compaction(
    neon_env_builder: NeonEnvBuilder,
    stripe_size: int,
    shard_count: int | None,
    gc_compaction: bool,
):
    """
    Use small stripes, small layers, and small compaction thresholds to exercise how compaction
    and image layer generation interacts with sharding.

    We are looking for bugs that might emerge from the way sharding uses sparse layer files that
    only contain some of the keys in the key range covered by the layer, such as errors estimating
    the size of layers that might result in too-small layer files.
    """

    compaction_target_size = 128 * 1024

    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": f"{128 * 1024}",
        "compaction_threshold": "1",
        "compaction_target_size": f"{compaction_target_size}",
        # no PITR horizon, we specify the horizon when we request on-demand GC
        "pitr_interval": "0s",
        # disable background compaction and GC. We invoke it manually when we want it to happen.
        "gc_period": "0s",
        "gc_horizon": f"{128 * 1024}",
        "compaction_period": "0s",
        "lsn_lease_length": "0s",
        # create image layers eagerly: we want to exercise image layer creation in this test.
        "image_creation_threshold": "1",
        "image_layer_creation_check_threshold": 0,
    }

    # Disable compression, as we can't estimate the size of layers with compression enabled
    # TODO: implement eager layer cutting during compaction
    neon_env_builder.pageserver_config_override = "image_compression='disabled'"

    neon_env_builder.num_pageservers = 1 if shard_count is None else shard_count
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF,
        initial_tenant_shard_count=shard_count,
        initial_tenant_shard_stripe_size=stripe_size,
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(64)
    for _i in range(0, 10):
        # Each of these does some writes then a checkpoint: because we set image_creation_threshold to 1,
        # these should result in image layers each time we write some data into a shard, and also shards
        # recieving less data hitting their "empty image layer" path (wherre they should skip writing the layer,
        # rather than asserting)
        workload.churn_rows(64)

    # Assert that we got some image layers: this is important because this test's purpose is to exercise the sharding changes
    # to Timeline::create_image_layers, so if we weren't creating any image layers we wouldn't be doing our job.
    shard_has_image_layers = []
    for shard in env.storage_controller.locate(tenant_id):
        pageserver = env.get_pageserver(shard["node_id"])
        shard_id = shard["shard_id"]
        layer_map = pageserver.http_client().layer_map_info(shard_id, timeline_id)
        image_layer_sizes = {}
        for layer in layer_map.historic_layers:
            if layer.kind == "Image":
                image_layer_sizes[layer.layer_file_name] = layer.layer_file_size

                # Pageserver should assert rather than emit an empty layer file, but double check here
                assert layer.layer_file_size > 0

        shard_has_image_layers.append(len(image_layer_sizes) > 1)
        log.info(f"Shard {shard_id} image layer sizes: {json.dumps(image_layer_sizes, indent=2)}")

        if stripe_size == TINY_STRIPES:
            # Checking the average size validates that our keyspace partitioning is  properly respecting sharding: if
            # it was not, we would tend to get undersized layers because the partitioning would overestimate the physical
            # data in a keyrange.
            #
            # We only do this check with tiny stripes, because large stripes may not give all shards enough
            # data to have statistically significant image layers
            avg_size = sum(v for v in image_layer_sizes.values()) / len(image_layer_sizes)
            log.info(f"Shard {shard_id} average image layer size: {avg_size}")
            assert avg_size > compaction_target_size / 2

    if stripe_size == TINY_STRIPES:
        # Expect writes were scattered across all pageservers: they should all have compacted some image layers
        assert all(shard_has_image_layers)
    else:
        # With large stripes, it is expected that most of our writes went to one pageserver, so we just require
        # that at least one of them has some image layers.
        assert any(shard_has_image_layers)

    # Assert that everything is still readable
    workload.validate()

    if gc_compaction:
        # trigger gc compaction to get more coverage for that, piggyback on the existing workload
        for shard in env.storage_controller.locate(tenant_id):
            pageserver = env.get_pageserver(shard["node_id"])
            tenant_shard_id = shard["shard_id"]
            # Force refresh gc info to have gc_cutoff generated
            pageserver.http_client().timeline_gc(tenant_shard_id, timeline_id, None)
            pageserver.http_client().timeline_compact(
                tenant_shard_id,
                timeline_id,
                enhanced_gc_bottom_most_compaction=True,
            )


class CompactionAlgorithm(StrEnum):
    LEGACY = "legacy"
    TIERED = "tiered"


@pytest.mark.parametrize(
    "compaction_algorithm", [CompactionAlgorithm.LEGACY, CompactionAlgorithm.TIERED]
)
def test_uploads_and_deletions(
    neon_env_builder: NeonEnvBuilder,
    compaction_algorithm: CompactionAlgorithm,
):
    """
    :param compaction_algorithm: the compaction algorithm to use.
    """

    tenant_conf = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": f"{128 * 1024}",
        "compaction_threshold": "1",
        "compaction_target_size": f"{128 * 1024}",
        # no PITR horizon, we specify the horizon when we request on-demand GC
        "pitr_interval": "0s",
        # disable background compaction and GC. We invoke it manually when we want it to happen.
        "gc_period": "0s",
        "compaction_period": "0s",
        # create image layers eagerly, so that GC can remove some layers
        "image_creation_threshold": "1",
        "image_layer_creation_check_threshold": "0",
        "compaction_algorithm": json.dumps({"kind": compaction_algorithm.value}),
        "lsn_lease_length": "0s",
    }
    env = neon_env_builder.init_start(initial_tenant_conf=tenant_conf)

    # TODO remove these allowed errors
    # https://github.com/neondatabase/neon/issues/7707
    # https://github.com/neondatabase/neon/issues/7759
    allowed_errors = [
        ".*/checkpoint.*rename temporary file as correct path for.*",  # EEXIST
        ".*delta layer created with.*duplicate values.*",
        ".*assertion failed: self.lsn_range.start <= lsn.*",
        ".*HTTP request handler task panicked: task.*panicked.*",
    ]
    if compaction_algorithm == CompactionAlgorithm.TIERED:
        env.pageserver.allowed_errors.extend(allowed_errors)

    try:
        generate_uploads_and_deletions(env, pageserver=env.pageserver)
    except PageserverApiException as e:
        log.info(f"Obtained PageserverApiException: {e}")

    # The errors occur flakily and no error is ensured to occur,
    # however at least one of them occurs.
    if compaction_algorithm == CompactionAlgorithm.TIERED:
        found_allowed_error = any(env.pageserver.log_contains(e) for e in allowed_errors)
        if not found_allowed_error:
            raise Exception("None of the allowed_errors occured in the log")


def test_pageserver_compaction_circuit_breaker(neon_env_builder: NeonEnvBuilder):
    """
    Check that repeated failures in compaction result in a circuit breaker breaking
    """
    TENANT_CONF = {
        # Very frequent runs to rack up failures quickly
        "compaction_period": "100ms",
        # Small checkpoint distance to create many layers
        "checkpoint_distance": 1024 * 128,
        # Compact small layers
        "compaction_target_size": 1024 * 128,
        "image_creation_threshold": 1,
    }

    FAILPOINT = "delta-layer-writer-fail-before-finish"
    BROKEN_LOG = ".*Circuit breaker broken!.*"

    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)

    workload = Workload(env, env.initial_tenant, env.initial_timeline)
    workload.init()

    # Set a failpoint that will prevent compaction succeeding
    env.pageserver.http_client().configure_failpoints((FAILPOINT, "return"))

    # Write some data to trigger compaction
    workload.write_rows(1024, upload=False)
    workload.write_rows(1024, upload=False)
    workload.write_rows(1024, upload=False)

    def assert_broken():
        env.pageserver.assert_log_contains(BROKEN_LOG)
        assert (
            env.pageserver.http_client().get_metric_value("pageserver_circuit_breaker_broken_total")
            or 0
        ) == 1
        assert (
            env.pageserver.http_client().get_metric_value(
                "pageserver_circuit_breaker_unbroken_total"
            )
            or 0
        ) == 0

    # Wait for enough failures to break the circuit breaker
    # This wait is fairly long because we back off on compaction failures, so 5 retries takes ~30s
    wait_until(assert_broken, timeout=60)

    # Sleep for a while, during which time we expect that compaction will _not_ be retried
    time.sleep(10)

    assert (
        env.pageserver.http_client().get_metric_value("pageserver_circuit_breaker_broken_total")
        or 0
    ) == 1
    assert (
        env.pageserver.http_client().get_metric_value("pageserver_circuit_breaker_unbroken_total")
        or 0
    ) == 0
    assert not env.pageserver.log_contains(".*Circuit breaker failure ended.*")


@pytest.mark.parametrize("enabled", [True, False])
def test_image_layer_compression(neon_env_builder: NeonEnvBuilder, enabled: bool):
    tenant_conf = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": f"{128 * 1024}",
        "compaction_threshold": "1",
        "compaction_target_size": f"{128 * 1024}",
        # no PITR horizon, we specify the horizon when we request on-demand GC
        "pitr_interval": "0s",
        # disable background compaction and GC. We invoke it manually when we want it to happen.
        "gc_period": "0s",
        "compaction_period": "0s",
        # create image layers as eagerly as possible
        "image_creation_threshold": "1",
        "image_layer_creation_check_threshold": "0",
    }

    # Explicitly enable/disable compression, rather than using default
    if enabled:
        neon_env_builder.pageserver_config_override = "image_compression='zstd'"
    else:
        neon_env_builder.pageserver_config_override = "image_compression='disabled'"

    env = neon_env_builder.init_start(initial_tenant_conf=tenant_conf)

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    pageserver = env.pageserver
    ps_http = env.pageserver.http_client()
    with env.endpoints.create_start(
        "main", tenant_id=tenant_id, pageserver_id=pageserver.id
    ) as endpoint:
        endpoint.safe_psql("CREATE TABLE foo (id INTEGER PRIMARY KEY, val text)")
        # Generate around 800k worth of easily compressible data to store
        for v in range(100):
            endpoint.safe_psql(
                f"INSERT INTO foo (id, val) VALUES ({v}, repeat('abcde{v:0>3}', 500))"
            )
    # run compaction to create image layers
    ps_http.timeline_checkpoint(tenant_id, timeline_id, wait_until_uploaded=True)

    layer_map = ps_http.layer_map_info(tenant_id, timeline_id)
    image_layer_count = 0
    delta_layer_count = 0
    for layer in layer_map.historic_layers:
        if layer.kind == "Image":
            image_layer_count += 1
        elif layer.kind == "Delta":
            delta_layer_count += 1
    assert image_layer_count > 0
    assert delta_layer_count > 0

    log.info(f"images: {image_layer_count}, deltas: {delta_layer_count}")

    bytes_in = pageserver.http_client().get_metric_value(
        "pageserver_compression_image_in_bytes_total"
    )
    bytes_out = pageserver.http_client().get_metric_value(
        "pageserver_compression_image_out_bytes_total"
    )
    assert bytes_in is not None
    assert bytes_out is not None
    log.info(f"Compression ratio: {bytes_out/bytes_in} ({bytes_out} in, {bytes_out} out)")

    if enabled:
        # We are writing high compressible repetitive plain text, expect excellent compression
        EXPECT_RATIO = 0.2
        assert bytes_out / bytes_in < EXPECT_RATIO
    else:
        # Nothing should be compressed if we disabled it.
        assert bytes_out >= bytes_in

    # Destroy the endpoint and create a new one to resetthe caches
    with env.endpoints.create_start(
        "main", tenant_id=tenant_id, pageserver_id=pageserver.id
    ) as endpoint:
        for v in range(100):
            res = endpoint.safe_psql(
                f"SELECT count(*) FROM foo WHERE id={v} and val=repeat('abcde{v:0>3}', 500)"
            )
            assert res[0][0] == 1
