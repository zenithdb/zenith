from __future__ import annotations

import concurrent.futures
import random
import time
from collections import defaultdict
from enum import StrEnum

import pytest
from fixtures.common_types import TenantId, TenantShardId, TimelineArchivalState, TimelineId
from fixtures.compute_reconfigure import ComputeReconfigure
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    NeonPageserver,
    PageserverAvailability,
    PageserverSchedulingPolicy,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.pg_version import PgVersion
from fixtures.utils import wait_until


def get_consistent_node_shard_counts(env: NeonEnv, total_shards) -> defaultdict[str, int]:
    """
    Get the number of shards attached to each node.
    This function takes into account the intersection of the intent and the observed state.
    If they do not match, it asserts out.
    """
    tenant_placement = env.storage_controller.get_tenants_placement()
    log.info(f"{tenant_placement=}")

    matching = {
        tid: tenant_placement[tid]["intent"]["attached"]
        for tid in tenant_placement
        if tenant_placement[tid]["intent"]["attached"]
        == tenant_placement[tid]["observed"]["attached"]
    }

    assert len(matching) == total_shards

    attached_per_node: defaultdict[str, int] = defaultdict(int)
    for node_id in matching.values():
        attached_per_node[node_id] += 1

    return attached_per_node


def assert_consistent_balanced_attachments(env: NeonEnv, total_shards):
    attached_per_node = get_consistent_node_shard_counts(env, total_shards)

    min_shard_count = min(attached_per_node.values())
    max_shard_count = max(attached_per_node.values())

    flake_factor = 5 / 100
    assert max_shard_count - min_shard_count <= int(total_shards * flake_factor)


@pytest.mark.timeout(3600)  # super long running test: should go down as we optimize
def test_storage_controller_many_tenants(
    neon_env_builder: NeonEnvBuilder, compute_reconfigure_listener: ComputeReconfigure
):
    """
    Check that we cope well with a not-totally-trivial number of tenants.

    This is checking for:
    - Obvious concurrency bugs from issuing many tenant creations/modifications
      concurrently.
    - Obvious scaling bugs like O(N^2) scaling that would be so slow that even
      a basic test starts failing from slowness.

    This is _not_ a comprehensive scale test: just a basic sanity check that
    we don't fall over for a thousand shards.
    """

    neon_env_builder.num_pageservers = 6
    neon_env_builder.storage_controller_config = {
        # Default neon_local uses a small timeout: use a longer one to tolerate longer pageserver restarts.
        # TODO: tune this down as restarts get faster (https://github.com/neondatabase/neon/pull/7553), to
        # guard against regressions in restart time.
        "max_offline": "30s",
        "max_warming_up": "300s",
    }
    neon_env_builder.control_plane_compute_hook_api = (
        compute_reconfigure_listener.control_plane_compute_hook_api
    )

    AZS = ["alpha", "bravo", "charlie"]

    def az_selector(node_id):
        return f"az-{AZS[(node_id - 1) % len(AZS)]}"

    neon_env_builder.pageserver_config_override = lambda ps_cfg: ps_cfg.update(
        {"availability_zone": az_selector(ps_cfg["id"])}
    )

    # A small sleep on each call into the notify hook, to simulate the latency of doing a database write
    compute_reconfigure_listener.register_on_notify(lambda body: time.sleep(0.01))

    env = neon_env_builder.init_configs()
    env.start()

    # We will intentionally stress reconciler concurrrency, which triggers a warning when lots
    # of shards are hitting the delayed path.
    env.storage_controller.allowed_errors.extend(
        [
            # We will intentionally stress reconciler concurrrency, which triggers a warning when lots
            # of shards are hitting the delayed path.
            ".*Many shards are waiting to reconcile",
            # We will create many timelines concurrently, so they might get slow enough to trip the warning
            # that timeline creation is holding a lock too long.
            ".*Shared lock by TimelineCreate.*was held.*",
        ]
    )

    for ps in env.pageservers:
        # Storage controller is allowed to drop pageserver requests when the cancellation token
        # for a Reconciler fires.
        ps.allowed_errors.append(".*request was dropped before completing.*")

    # Total tenants
    small_tenant_count = 7800
    large_tenant_count = 200
    tenant_count = small_tenant_count + large_tenant_count
    large_tenant_shard_count = 8
    total_shards = small_tenant_count + large_tenant_count * large_tenant_shard_count

    # A small stripe size to encourage all shards to get some data
    stripe_size = 1

    # We use a fixed seed to make the test somewhat reproducible: we want a randomly
    # chosen order in the sense that it's arbitrary, but not in the sense that it should change every run.
    rng = random.Random(1234)

    class Tenant:
        def __init__(self):
            # Tenants may optionally contain a timeline
            self.timeline_id = None

            # Tenants may be marked as 'large' to get multiple shard during creation phase
            self.large = False

    tenant_ids = list(TenantId.generate() for _i in range(0, tenant_count))
    tenants = dict((tid, Tenant()) for tid in tenant_ids)

    # We will create timelines in only a subset of tenants, because creating timelines
    # does many megabytes of IO, and we want to densely simulate huge tenant counts on
    # a single test node.
    tenant_timelines_count = 100

    # These lists are maintained for use with rng.choice
    tenants_with_timelines = list(rng.sample(list(tenants.keys()), tenant_timelines_count))
    tenants_without_timelines = list(
        tenant_id for tenant_id in tenants if tenant_id not in tenants_with_timelines
    )

    # For our sharded tenants, we will make half of them with timelines and half without
    assert large_tenant_count >= tenant_timelines_count / 2
    for tenant_id in tenants_with_timelines[0 : large_tenant_count // 2]:
        tenants[tenant_id].large = True

    for tenant_id in tenants_without_timelines[0 : large_tenant_count // 2]:
        tenants[tenant_id].large = True

    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    def check_memory():
        # Shards should be cheap_ in memory, as we will have very many of them
        expect_memory_per_shard = 128 * 1024

        rss = env.storage_controller.get_metric_value("process_resident_memory_bytes")
        assert rss is not None
        log.info(f"Resident memory: {rss} ({ rss / total_shards} per shard)")
        assert rss < expect_memory_per_shard * total_shards

    def assert_all_tenants_scheduled_in_home_az():
        for tenant_id in tenant_ids:
            desc = env.storage_controller.tenant_describe(tenant_id)
            preferred_az = None
            for shard in desc["shards"]:
                # All shards in a tenant should have the same preferred AZ
                if preferred_az is None:
                    preferred_az = shard["preferred_az_id"]
                else:
                    assert preferred_az == shard["preferred_az_id"]

                # Attachment should be in the preferred AZ
                assert shard["preferred_az_id"] == az_selector(
                    shard["node_attached"]
                ), f"Shard {shard['tenant_shard_id']} not in {shard['preferred_az_id']}"

                # Secondary locations should not be in the preferred AZ
                for node_secondary in shard["node_secondary"]:
                    assert (
                        shard["preferred_az_id"] != az_selector(node_secondary)
                    ), f"Shard {shard['tenant_shard_id']} secondary should be in {shard['preferred_az_id']}"

                # There should only be one secondary location (i.e. no migrations in flight)
                assert len(shard["node_secondary"]) == 1

    # Issue more concurrent operations than the storage controller's reconciler concurrency semaphore
    # permits, to ensure that we are exercising stressing that.
    api_concurrency = 135

    # A different concurrency limit for bulk tenant+timeline creations: these do I/O and will
    # start timing on test nodes if we aren't a bit careful.
    create_concurrency = 16

    class Operation(StrEnum):
        TIMELINE_OPS = "timeline_ops"
        SHARD_MIGRATE = "shard_migrate"
        TENANT_PASSTHROUGH = "tenant_passthrough"

    run_ops = api_concurrency * 4
    assert run_ops < len(tenants)

    # Creation phase: make a lot of tenants, and create timelines in a subset of them
    # This executor has concurrency set modestly, to avoid overloading pageservers with timeline creations.
    with concurrent.futures.ThreadPoolExecutor(max_workers=create_concurrency) as executor:
        tenant_create_futs = []
        t1 = time.time()

        for tenant_id, tenant in tenants.items():
            if tenant.large:
                shard_count = large_tenant_shard_count
            else:
                shard_count = 1

            # We will create tenants directly via API, not via neon_local, to avoid any false
            # serialization of operations in neon_local (it e.g. loads/saves a config file on each call)
            f = executor.submit(
                env.storage_controller.tenant_create,
                tenant_id,
                shard_count,
                stripe_size,
                # Upload heatmaps fast, so that secondary downloads happen promptly, enabling
                # the controller's optimization migrations to proceed promptly.
                tenant_config={"heatmap_period": "10s"},
                placement_policy={"Attached": 1},
            )
            tenant_create_futs.append(f)

        # Wait for tenant creations to finish
        for f in tenant_create_futs:
            f.result()
        log.info(
            f"Created {len(tenants)} tenants in {time.time() - t1}, {len(tenants) / (time.time() - t1)}/s"
        )

        # Waiting for optimizer to stabilize, if it disagrees with scheduling (the correct behavior
        # would be for original scheduling decisions to always match optimizer's preference)
        # (workaround for https://github.com/neondatabase/neon/issues/8969)
        env.storage_controller.reconcile_until_idle(max_interval=0.1, timeout_secs=120)

        # Create timelines in those tenants which are going to get one
        t1 = time.time()
        timeline_create_futs = []
        for tenant_id in tenants_with_timelines:
            timeline_id = TimelineId.generate()
            tenants[tenant_id].timeline_id = timeline_id
            f = executor.submit(
                env.storage_controller.pageserver_api().timeline_create,
                PgVersion.NOT_SET,
                tenant_id,
                timeline_id,
            )
            timeline_create_futs.append(f)

        for f in timeline_create_futs:
            f.result()
        log.info(
            f"Created {len(tenants_with_timelines)} timelines in {time.time() - t1}, {len(tenants_with_timelines) / (time.time() - t1)}/s"
        )

    # Check initial scheduling
    assert_all_tenants_scheduled_in_home_az()
    az_attached_counts: defaultdict[str, int] = defaultdict(int)
    az_secondary_counts: defaultdict[str, int] = defaultdict(int)
    node_attached_counts: defaultdict[str, int] = defaultdict(int)
    for tenant_id in tenants.keys():
        desc = env.storage_controller.tenant_describe(tenant_id)
        for shard in desc["shards"]:
            az_attached_counts[az_selector(shard["node_attached"])] += 1
            node_attached_counts[shard["node_attached"]] += 1
            for node_secondary in shard["node_secondary"]:
                az_secondary_counts[az_selector(node_secondary)] += 1

    log.info(f"Initial node attached counts: {node_attached_counts}")
    log.info(f"Initial AZ shard counts: {az_attached_counts}, {az_secondary_counts}")

    # Plan operations: ensure each tenant with a timeline gets at least
    # one of each operation type.  Then add other tenants to make up the
    # numbers.
    ops_plan = []
    for tenant_id in tenants_with_timelines:
        ops_plan.append((tenant_id, Operation.TIMELINE_OPS))
        ops_plan.append((tenant_id, Operation.SHARD_MIGRATE))
        ops_plan.append((tenant_id, Operation.TENANT_PASSTHROUGH))

    # Fill up remaining run_ops with migrations of tenants without timelines
    other_migrate_tenants = rng.sample(tenants_without_timelines, run_ops - len(ops_plan))

    for tenant_id in other_migrate_tenants:
        ops_plan.append(
            (
                tenant_id,
                rng.choice([Operation.SHARD_MIGRATE, Operation.TENANT_PASSTHROUGH]),
            )
        )

    # Exercise phase: pick pseudo-random operations to do on the tenants + timelines
    # This executor has concurrency high enough to stress the storage controller API.
    with concurrent.futures.ThreadPoolExecutor(max_workers=api_concurrency) as executor:

        def exercise_timeline_ops(tenant_id, timeline_id):
            # A read operation: this requires looking up shard zero and routing there
            detail = virtual_ps_http.timeline_detail(tenant_id, timeline_id)
            assert detail["timeline_id"] == str(timeline_id)

            # A fan-out write operation to all shards in a tenant.
            # - We use a metadata operation rather than something like a timeline create, because
            #   timeline creations are I/O intensive and this test isn't meant to be a stress test for
            #   doing lots of concurrent timeline creations.
            archival_state = rng.choice(
                [TimelineArchivalState.ARCHIVED, TimelineArchivalState.UNARCHIVED]
            )
            try:
                virtual_ps_http.timeline_archival_config(tenant_id, timeline_id, archival_state)
            except PageserverApiException as e:
                if e.status_code == 404:
                    # FIXME: there is an edge case where timeline ops can encounter a 404 during
                    # a very short time window between generating a new generation number and
                    # attaching this tenant to its new pageserver.
                    # See https://github.com/neondatabase/neon/issues/9471
                    pass
                else:
                    raise

        # Generate a mixture of operations and dispatch them all concurrently
        futs = []
        for tenant_id, op in ops_plan:
            if op == Operation.TIMELINE_OPS:
                op_timeline_id = tenants[tenant_id].timeline_id
                assert op_timeline_id is not None

                # Exercise operations that modify tenant scheduling state but require traversing
                # the fan-out-to-all-shards functionality.
                f = executor.submit(
                    exercise_timeline_ops,
                    tenant_id,
                    op_timeline_id,
                )
            elif op == Operation.SHARD_MIGRATE:
                # A reconciler operation: migrate a shard.
                desc = env.storage_controller.tenant_describe(tenant_id)

                shard_number = rng.randint(0, len(desc["shards"]) - 1)
                tenant_shard_id = TenantShardId(tenant_id, shard_number, len(desc["shards"]))

                # Migrate it to its secondary location
                dest_ps_id = desc["shards"][shard_number]["node_secondary"][0]

                f = executor.submit(
                    env.storage_controller.tenant_shard_migrate, tenant_shard_id, dest_ps_id
                )
            elif op == Operation.TENANT_PASSTHROUGH:
                # A passthrough read to shard zero
                f = executor.submit(virtual_ps_http.tenant_status, tenant_id)

            futs.append(f)

        # Wait for mixed ops to finish
        for f in futs:
            f.result()

    log.info("Completed mixed operations phase")

    # Some of the operations above (notably migrations) might leave the controller in a state where it has
    # some work to do, for example optimizing shard placement after we do a random migration. Wait for the system
    # to reach a quiescent state before doing following checks.
    #
    # - Set max_interval low because we probably have a significant number of optimizations to complete and would like
    #   the test to run quickly.
    # - Set timeout high because we might be waiting for optimizations that reuqire a secondary
    #   to warm up, and if we just started a secondary in the previous step, it might wait some time
    #   before downloading its heatmap
    env.storage_controller.reconcile_until_idle(max_interval=0.1, timeout_secs=120)

    env.storage_controller.consistency_check()
    check_memory()

    # This loop waits for reconcile_all to indicate no pending work, and then calls it once more to time
    # how long the call takes when idle: this iterates over shards while doing no I/O and should be reliably fast: if
    # it isn't, that's a sign that we have made some algorithmic mistake (e.g. O(N**2) scheduling)
    #
    # We do not require that the system is quiescent already here, although at present in this point in the test
    # that may be the case.
    log.info("Reconciling all & timing")
    while True:
        t1 = time.time()
        reconcilers = env.storage_controller.reconcile_all()
        if reconcilers == 0:
            # Time how long a no-op background reconcile takes: this measures how long it takes to
            # loop over all the shards looking for work to do.
            runtime = time.time() - t1
            log.info(f"No-op call to reconcile_all took {runtime}s")
            assert runtime < 1
            break

    # Restart the storage controller
    log.info("Restarting controller")
    env.storage_controller.stop()
    env.storage_controller.start()

    # See how long the controller takes to pass its readiness check.  This should be fast because
    # all the nodes are online: offline pageservers are the only thing that's allowed to delay
    # startup.
    readiness_period = env.storage_controller.wait_until_ready()
    assert readiness_period < 5

    # Consistency check is safe here: the storage controller's restart should not have caused any reconcilers
    # to run, as it was in a stable state before restart.  If it did, that's a bug.
    env.storage_controller.consistency_check()
    check_memory()

    shard_counts = get_consistent_node_shard_counts(env, total_shards)
    log.info(f"Shard counts before rolling restart: {shard_counts}")

    assert_consistent_balanced_attachments(env, total_shards)

    # Restart pageservers gracefully: this exercises the /re-attach pageserver API
    # and the storage controller drain and fill API
    log.info("Restarting pageservers...")

    # Parameters for how long we expect it to take to migrate all of the tenants from/to
    # a node during a drain/fill operation
    DRAIN_FILL_TIMEOUT = 240
    DRAIN_FILL_BACKOFF = 5

    for ps in env.pageservers:
        log.info(f"Draining pageserver {ps.id}")
        t1 = time.time()
        env.storage_controller.retryable_node_operation(
            lambda ps_id: env.storage_controller.node_drain(ps_id), ps.id, max_attempts=3, backoff=2
        )

        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.PAUSE_FOR_RESTART,
            max_attempts=DRAIN_FILL_TIMEOUT // DRAIN_FILL_BACKOFF,
            backoff=DRAIN_FILL_BACKOFF,
        )
        log.info(f"Drained pageserver {ps.id} in {time.time() - t1}s")

        shard_counts = get_consistent_node_shard_counts(env, total_shards)
        log.info(f"Shard counts after draining node {ps.id}: {shard_counts}")
        # Assert that we've drained the node
        assert shard_counts[str(ps.id)] == 0
        # Assert that those shards actually went somewhere
        assert sum(shard_counts.values()) == total_shards

        ps.restart()
        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.ACTIVE,
            max_attempts=24,
            backoff=1,
        )

        log.info(f"Filling pageserver {ps.id}")
        env.storage_controller.retryable_node_operation(
            lambda ps_id: env.storage_controller.node_fill(ps_id), ps.id, max_attempts=3, backoff=2
        )
        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.ACTIVE,
            max_attempts=DRAIN_FILL_TIMEOUT // DRAIN_FILL_BACKOFF,
            backoff=DRAIN_FILL_BACKOFF,
        )

        log.info(f"Filled pageserver {ps.id} in {time.time() - t1}s")

        # Waiting for optimizer to stabilize, if it disagrees with scheduling (the correct behavior
        # would be for original scheduling decisions to always match optimizer's preference)
        # (workaround for https://github.com/neondatabase/neon/issues/8969)
        env.storage_controller.reconcile_until_idle(max_interval=0.1, timeout_secs=120)

        shard_counts = get_consistent_node_shard_counts(env, total_shards)
        log.info(f"Shard counts after filling node {ps.id}: {shard_counts}")

        assert_consistent_balanced_attachments(env, total_shards)

        env.storage_controller.reconcile_until_idle(max_interval=0.1, timeout_secs=120)
        env.storage_controller.consistency_check()

    # Since we did `reconcile_until_idle` during the above loop, the system should be left in
    # an optimally scheduled state.  Validate that this includes all the tenants being scheduled
    # in their home AZ.
    assert_all_tenants_scheduled_in_home_az()

    # Consistency check is safe here: restarting pageservers should not have caused any Reconcilers to spawn,
    # as they were not offline long enough to trigger any scheduling changes.
    env.storage_controller.consistency_check()
    check_memory()

    # Simulate loss of an AZ
    victim_az = "az-alpha"
    killed_pageservers = []
    for ps in env.pageservers:
        if az_selector(ps.id) == victim_az:
            ps.stop(immediate=True)
            killed_pageservers.append(ps)
            log.info(f"Killed pageserver {ps.id}")

    assert killed_pageservers

    # Wait for the controller to notice the pageservers are dead
    def assert_pageservers_availability(
        pageservers: list[NeonPageserver], expected_availability: PageserverAvailability
    ):
        nodes = env.storage_controller.nodes()
        checked_any = False
        node_ids = [ps.id for ps in pageservers]
        for node in nodes:
            if node["id"] in node_ids:
                checked_any = True
                assert (
                    node["availability"] == expected_availability
                ), f"Node {node['id']} is not {expected_availability} yet: {node['availability']}"

        assert checked_any

    wait_until(
        lambda: assert_pageservers_availability(killed_pageservers, PageserverAvailability.OFFLINE),
        timeout=60,
    )

    # Let the controller finish all its rescheduling
    env.storage_controller.reconcile_until_idle(max_interval=0.1, timeout_secs=120)

    # Check that all the tenants are rescheduled to the remaining pageservers
    for tenant_id in tenant_ids:
        desc = env.storage_controller.tenant_describe(tenant_id)
        for shard in desc["shards"]:
            # Attachment should be outside the AZ where we killed the pageservers
            assert (
                az_selector(shard["node_attached"]) != victim_az
            ), f"Shard {shard['tenant_shard_id']} still in {victim_az} (node {shard['node_attached']})"

    # Bring back the pageservers
    for ps in killed_pageservers:
        ps.start()

    wait_until(
        lambda: assert_pageservers_availability(killed_pageservers, PageserverAvailability.ACTIVE),
        timeout=60,
    )

    # A very long timeout is required: we will be migrating all the tenants on all the pageservers
    # in the region that we just restored.  Assume it'll take up to twice as long as it took to fill
    # a single node
    env.storage_controller.reconcile_until_idle(
        max_interval=0.1, timeout_secs=DRAIN_FILL_TIMEOUT * 2
    )
    assert_all_tenants_scheduled_in_home_az()

    # Stop the storage controller before tearing down fixtures, because it otherwise might log
    # errors trying to call our `ComputeReconfigure`.
    env.storage_controller.stop()
