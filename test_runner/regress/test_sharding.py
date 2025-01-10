from __future__ import annotations

import os
import time
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import pytest
import requests
from fixtures.common_types import Lsn, TenantId, TenantShardId, TimelineArchivalState, TimelineId
from fixtures.compute_reconfigure import ComputeReconfigure
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    DEFAULT_AZ_ID,
    NeonEnv,
    NeonEnvBuilder,
    StorageControllerApiException,
    last_flush_lsn_upload,
    tenant_get_shards,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.utils import assert_prefix_empty, assert_prefix_not_empty
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind, s3_storage
from fixtures.utils import skip_in_debug_build, wait_until
from fixtures.workload import Workload
from pytest_httpserver import HTTPServer
from typing_extensions import override
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

if TYPE_CHECKING:
    from fixtures.httpserver import ListenAddress


def test_sharding_smoke(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test the basic lifecycle of a sharded tenant:
     - ingested data gets split up
     - page service reads
     - timeline creation and deletion
     - splits
    """

    shard_count = 4
    neon_env_builder.num_pageservers = shard_count

    # 1MiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.  The stripe size is given
    # in number of 8KiB pages.
    stripe_size = 128

    # Use S3-compatible remote storage so that we can scrub: this test validates
    # that the scrubber doesn't barf when it sees a sharded tenant.
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_shard_stripe_size=stripe_size
    )
    tenant_id = env.initial_tenant

    pageservers = dict((int(p.id), p) for p in env.pageservers)
    shards = env.storage_controller.locate(tenant_id)

    def get_sizes():
        sizes = {}
        for shard in shards:
            node_id = int(shard["node_id"])
            pageserver = pageservers[node_id]
            sizes[node_id] = pageserver.http_client().tenant_status(shard["shard_id"])[
                "current_physical_size"
            ]
        log.info(f"sizes = {sizes}")
        return sizes

    # The imported initdb for timeline creation should
    # not be fully imported on every shard.  We use a 1MB strripe size so expect
    # pretty good distribution: no one shard should have more than half the data
    sizes = get_sizes()
    physical_initdb_total = sum(sizes.values())
    expect_initdb_size = 20 * 1024 * 1024
    assert physical_initdb_total > expect_initdb_size
    assert all(s < expect_initdb_size // 2 for s in sizes.values())

    # Test that timeline creation works on a sharded tenant
    timeline_b = env.create_branch("branch_b", tenant_id=tenant_id)

    # Test that we can write data to a sharded tenant
    workload = Workload(env, tenant_id, timeline_b, branch_name="branch_b")
    workload.init()

    sizes_before = get_sizes()
    workload.write_rows(256)

    # Test that we can read data back from a sharded tenant
    workload.validate()

    # Validate that the data is spread across pageservers
    sizes_after = get_sizes()
    # Our sizes increased when we wrote data
    assert sum(sizes_after.values()) > sum(sizes_before.values())
    # That increase is present on all shards
    assert all(sizes_after[ps.id] > sizes_before[ps.id] for ps in env.pageservers)

    # Validate that timeline list API works properly on all shards
    for shard in shards:
        node_id = int(shard["node_id"])
        pageserver = pageservers[node_id]
        timelines = set(
            TimelineId(tl["timeline_id"])
            for tl in pageserver.http_client().timeline_list(shard["shard_id"])
        )
        assert timelines == {env.initial_timeline, timeline_b}

    env.storage_controller.consistency_check()

    # Validate that deleting a sharded tenant removes all files in the prefix

    # Before deleting, stop the client and check we have some objects to delete
    workload.stop()
    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )

    # Check the scrubber isn't confused by sharded content, then disable
    # it during teardown because we'll have deleted by then
    healthy, _ = env.storage_scrubber.scan_metadata()
    assert healthy

    env.storage_controller.pageserver_api().tenant_delete(tenant_id)
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )

    env.storage_controller.consistency_check()


def test_sharding_split_unsharded(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test that shard splitting works on a tenant created as unsharded (i.e. with
    ShardCount(0)).
    """
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Check that we created with an unsharded TenantShardId: this is the default,
    # but check it in case we change the default in future
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 0, 0)) is not None

    workload = Workload(env, tenant_id, timeline_id, branch_name="main")
    workload.init()
    workload.write_rows(256)
    workload.validate()

    # Split one shard into two
    env.storage_controller.tenant_shard_split(tenant_id, shard_count=2)

    # Check we got the shard IDs we expected
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 0, 2)) is not None
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 1, 2)) is not None

    workload.validate()

    env.storage_controller.consistency_check()


@pytest.mark.parametrize(
    "failpoint",
    [
        None,
        "compact-shard-ancestors-localonly",
        "compact-shard-ancestors-enqueued",
        "compact-shard-ancestors-persistent",
    ],
)
def test_sharding_split_compaction(
    neon_env_builder: NeonEnvBuilder, failpoint: str | None, build_type: str
):
    """
    Test that after a split, we clean up parent layer data in the child shards via compaction.
    """

    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": 128 * 1024,
        "compaction_threshold": 1,
        "compaction_target_size": 128 * 1024,
        # no PITR horizon, we specify the horizon when we request on-demand GC
        "pitr_interval": "3600s",
        # disable background compaction and GC. We invoke it manually when we want it to happen.
        "gc_period": "0s",
        "compaction_period": "0s",
        # Disable automatic creation of image layers, as we will create them explicitly when we want them
        "image_creation_threshold": 9999,
        "image_layer_creation_check_threshold": 0,
        "lsn_lease_length": "0s",
    }

    neon_env_builder.storage_controller_config = {
        # Default neon_local uses a small timeout: use a longer one to tolerate longer pageserver restarts.
        "max_offline": "30s",
        "max_warming_up": "300s",
    }

    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Check that we created with an unsharded TenantShardId: this is the default,
    # but check it in case we change the default in future
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 0, 0)) is not None

    workload = Workload(env, tenant_id, timeline_id, branch_name="main")
    workload.init()
    workload.write_rows(256)
    workload.validate()
    workload.stop()

    # Do a full image layer generation before splitting, so that when we compact after splitting
    # we should only see sizes decrease (from post-split drops/rewrites), not increase (from image layer generation)
    env.get_tenant_pageserver(tenant_id).http_client().timeline_checkpoint(
        tenant_id, timeline_id, force_image_layer_creation=True, wait_until_uploaded=True
    )

    # Split one shard into two
    shards = env.storage_controller.tenant_shard_split(tenant_id, shard_count=2)

    # Let all shards move into their stable locations, so that during subsequent steps we
    # don't have reconciles in progress (simpler to reason about what messages we expect in logs)
    env.storage_controller.reconcile_until_idle()

    # Check we got the shard IDs we expected
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 0, 2)) is not None
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 1, 2)) is not None

    workload.validate()
    workload.stop()

    env.storage_controller.consistency_check()

    # Cleanup part 1: while layers are still in PITR window, we should only drop layers that are fully redundant
    for shard in shards:
        ps = env.get_tenant_pageserver(shard)
        assert ps is not None

        # Invoke compaction: this should drop any layers that don't overlap with the shard's key stripes
        detail_before = ps.http_client().timeline_detail(shard, timeline_id)
        ps.http_client().timeline_compact(shard, timeline_id)
        detail_after = ps.http_client().timeline_detail(shard, timeline_id)

        # Physical size should shrink because some layers have been dropped
        assert detail_after["current_physical_size"] < detail_before["current_physical_size"]

    # Compaction shouldn't make anything unreadable
    workload.validate()

    # Force a generation increase: layer rewrites are a long-term thing and only happen after
    # the generation has increased.
    env.pageserver.stop()
    env.pageserver.start()

    # Cleanup part 2: once layers are outside the PITR window, they will be rewritten if they are partially redundant
    updated_conf = TENANT_CONF.copy()
    updated_conf["pitr_interval"] = "0s"
    env.storage_controller.pageserver_api().set_tenant_config(tenant_id, updated_conf)
    env.storage_controller.reconcile_until_idle()

    for shard in shards:
        ps = env.get_tenant_pageserver(shard)

        # Apply failpoints for the layer-rewriting phase: this is the area of code that has sensitive behavior
        # across restarts, as we will have local layer files that temporarily disagree with the remote metadata
        # for the same local layer file name.
        if failpoint is not None:
            ps.http_client().configure_failpoints((failpoint, "exit"))

        # Do a GC to update gc_info (compaction uses this to decide whether a layer is to be rewritten)
        # Set gc_horizon=0 to let PITR horizon control GC cutoff exclusively.
        ps.http_client().timeline_gc(shard, timeline_id, gc_horizon=0)

        # We will compare stats before + after compaction
        detail_before = ps.http_client().timeline_detail(shard, timeline_id)

        # Invoke compaction: this should rewrite layers that are behind the pitr horizon
        try:
            ps.http_client().timeline_compact(shard, timeline_id)
        except requests.ConnectionError as e:
            if failpoint is None:
                raise e
            else:
                log.info(f"Compaction failed (failpoint={failpoint}): {e}")

            if failpoint in (
                "compact-shard-ancestors-localonly",
                "compact-shard-ancestors-enqueued",
            ):
                # If we left local files that don't match remote metadata, we expect warnings on next startup
                env.pageserver.allowed_errors.append(
                    ".*removing local file .+ because it has unexpected length.*"
                )

            # Post-failpoint: we check that the pageserver comes back online happily.
            env.pageserver.running = False
            env.pageserver.start()
        else:
            assert failpoint is None  # We shouldn't reach success path if a failpoint was set

            detail_after = ps.http_client().timeline_detail(shard, timeline_id)

            # Physical size should shrink because layers are smaller
            assert detail_after["current_physical_size"] < detail_before["current_physical_size"]

    # Validate filtering compaction actually happened
    for shard in shards:
        ps = env.get_tenant_pageserver(shard)

        log.info("scan all layer files for disposable keys, there shouldn't be any")
        result = ps.timeline_scan_no_disposable_keys(shard, timeline_id)
        tally = result.tally
        raw_page_count = tally.not_disposable_count + tally.disposable_count
        assert tally.not_disposable_count > (
            raw_page_count // 2
        ), "compaction doesn't rewrite layers that are >=50pct local"

        log.info("check sizes")
        timeline_info = ps.http_client().timeline_detail(shard, timeline_id)
        reported_size = timeline_info["current_physical_size"]
        layer_paths = ps.list_layers(shard, timeline_id)
        measured_size = 0
        for p in layer_paths:
            abs_path = ps.timeline_dir(shard, timeline_id) / p
            measured_size += os.stat(abs_path).st_size

        log.info(
            f"shard {shard} reported size {reported_size}, measured size {measured_size} ({len(layer_paths)} layers)"
        )

        if failpoint in (
            "compact-shard-ancestors-localonly",
            "compact-shard-ancestors-enqueued",
        ):
            # If we injected a failure between local rewrite and remote upload, then after
            # restart we may end up with neither version of the file on local disk (the new file
            # is cleaned up because it doesn't matchc remote metadata).  So local size isn't
            # necessarily going to match remote physical size.
            continue

        assert measured_size == reported_size

    # Compaction shouldn't make anything unreadable
    workload.validate()


def test_sharding_split_offloading(neon_env_builder: NeonEnvBuilder):
    """
    Test that during a split, we don't miss archived and offloaded timelines.
    """

    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": 128 * 1024,
        "compaction_threshold": 1,
        "compaction_target_size": 128 * 1024,
        # no PITR horizon, we specify the horizon when we request on-demand GC
        "pitr_interval": "3600s",
        # disable background compaction, GC and offloading. We invoke it manually when we want it to happen.
        "gc_period": "0s",
        "compaction_period": "0s",
        # Disable automatic creation of image layers, as we will create them explicitly when we want them
        "image_creation_threshold": 9999,
        "image_layer_creation_check_threshold": 0,
        "lsn_lease_length": "0s",
    }

    neon_env_builder.storage_controller_config = {
        # Default neon_local uses a small timeout: use a longer one to tolerate longer pageserver restarts.
        "max_offline": "30s",
        "max_warming_up": "300s",
    }

    env = neon_env_builder.init_start(initial_tenant_conf=TENANT_CONF)
    tenant_id = env.initial_tenant
    timeline_id_main = env.initial_timeline

    # Check that we created with an unsharded TenantShardId: this is the default,
    # but check it in case we change the default in future
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 0, 0)) is not None

    workload_main = Workload(env, tenant_id, timeline_id_main, branch_name="main")
    workload_main.init()
    workload_main.write_rows(256)
    workload_main.validate()
    workload_main.stop()

    # Create two timelines, archive one, offload the other
    timeline_id_archived = env.create_branch("archived_not_offloaded")
    timeline_id_offloaded = env.create_branch("archived_offloaded")

    def timeline_id_set_for(list: list[dict[str, Any]]) -> set[TimelineId]:
        return set(
            map(
                lambda t: TimelineId(t["timeline_id"]),
                list,
            )
        )

    expected_offloaded_set = {timeline_id_offloaded}
    expected_timeline_set = {timeline_id_main, timeline_id_archived}

    with env.get_tenant_pageserver(tenant_id).http_client() as http_client:
        http_client.timeline_archival_config(
            tenant_id, timeline_id_archived, TimelineArchivalState.ARCHIVED
        )
        http_client.timeline_archival_config(
            tenant_id, timeline_id_offloaded, TimelineArchivalState.ARCHIVED
        )
        http_client.timeline_offload(tenant_id, timeline_id_offloaded)
        list = http_client.timeline_and_offloaded_list(tenant_id)
        assert timeline_id_set_for(list.offloaded) == expected_offloaded_set
        assert timeline_id_set_for(list.timelines) == expected_timeline_set

        # Do a full image layer generation before splitting
        http_client.timeline_checkpoint(
            tenant_id, timeline_id_main, force_image_layer_creation=True, wait_until_uploaded=True
        )

    # Split one shard into two
    shards = env.storage_controller.tenant_shard_split(tenant_id, shard_count=2)

    # Let all shards move into their stable locations, so that during subsequent steps we
    # don't have reconciles in progress (simpler to reason about what messages we expect in logs)
    env.storage_controller.reconcile_until_idle()

    # Check we got the shard IDs we expected
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 0, 2)) is not None
    assert env.storage_controller.inspect(TenantShardId(tenant_id, 1, 2)) is not None

    workload_main.validate()
    workload_main.stop()

    env.storage_controller.consistency_check()

    # Ensure each shard has the same list of timelines and offloaded timelines
    for shard in shards:
        ps = env.get_tenant_pageserver(shard)

        list = ps.http_client().timeline_and_offloaded_list(shard)
        assert timeline_id_set_for(list.offloaded) == expected_offloaded_set
        assert timeline_id_set_for(list.timelines) == expected_timeline_set

        ps.http_client().timeline_compact(shard, timeline_id_main)

    # Check that we can still read all the data
    workload_main.validate()

    # Force a restart, which requires the state to be persisted.
    env.pageserver.stop()
    env.pageserver.start()

    # Ensure each shard has the same list of timelines and offloaded timelines
    for shard in shards:
        ps = env.get_tenant_pageserver(shard)

        list = ps.http_client().timeline_and_offloaded_list(shard)
        assert timeline_id_set_for(list.offloaded) == expected_offloaded_set
        assert timeline_id_set_for(list.timelines) == expected_timeline_set

        ps.http_client().timeline_compact(shard, timeline_id_main)

    # Compaction shouldn't make anything unreadable
    workload_main.validate()

    # Do sharded unarchival
    env.storage_controller.timeline_archival_config(
        tenant_id, timeline_id_offloaded, TimelineArchivalState.UNARCHIVED
    )
    env.storage_controller.timeline_archival_config(
        tenant_id, timeline_id_archived, TimelineArchivalState.UNARCHIVED
    )

    for shard in shards:
        ps = env.get_tenant_pageserver(shard)

        list = ps.http_client().timeline_and_offloaded_list(shard)
        assert timeline_id_set_for(list.offloaded) == set()
        assert timeline_id_set_for(list.timelines) == {
            timeline_id_main,
            timeline_id_archived,
            timeline_id_offloaded,
        }


def test_sharding_split_smoke(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test the basics of shard splitting:
    - The API results in more shards than we started with
    - The tenant's data remains readable

    """

    # Shard count we start with
    shard_count = 2
    # Shard count we split into
    split_shard_count = 4
    # In preferred AZ & other AZ we will end up with one shard per pageserver
    neon_env_builder.num_pageservers = split_shard_count * 2

    # Two AZs
    def assign_az(ps_cfg):
        az = f"az-{(ps_cfg['id'] - 1) % 2}"
        ps_cfg["availability_zone"] = az

        # We will run more pageservers than tests usually do, so give them tiny page caches
        # in case we're on a test node under memory pressure.
        ps_cfg["page_cache_size"] = 128

    neon_env_builder.pageserver_config_override = assign_az

    # 1MiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.  The stripe size is given
    # in number of 8KiB pages.
    stripe_size = 128

    # Use S3-compatible remote storage so that we can scrub: this test validates
    # that the scrubber doesn't barf when it sees a sharded tenant.
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    non_default_tenant_config = {"gc_horizon": 77 * 1024 * 1024}

    env = neon_env_builder.init_configs(True)
    env.start()
    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.create_tenant(
        tenant_id,
        timeline_id,
        shard_count=shard_count,
        shard_stripe_size=stripe_size,
        placement_policy='{"Attached": 1}',
        conf=non_default_tenant_config,
    )

    workload = Workload(env, tenant_id, timeline_id, branch_name="main")
    workload.init()

    # Initial data
    workload.write_rows(256)

    # Note which pageservers initially hold a shard after tenant creation
    pre_split_pageserver_ids = dict()
    for loc in env.storage_controller.locate(tenant_id):
        shard_no = TenantShardId.parse(loc["shard_id"]).shard_number
        pre_split_pageserver_ids[loc["node_id"]] = shard_no
    log.info(f"Pre-split pageservers: {pre_split_pageserver_ids}")

    # For pageservers holding a shard, validate their ingest statistics
    # reflect a proper splitting of the WAL.

    observed_on_shard_zero = 0
    received_on_non_zero_shard = 0
    for pageserver in env.pageservers:
        if pageserver.id not in pre_split_pageserver_ids:
            continue

        metrics = pageserver.http_client().get_metrics_values(
            [
                "pageserver_wal_ingest_records_received_total",
                "pageserver_wal_ingest_records_observed_total",
            ]
        )

        log.info(f"Pageserver {pageserver.id} metrics: {metrics}")

        received = metrics["pageserver_wal_ingest_records_received_total"]
        observed = metrics["pageserver_wal_ingest_records_observed_total"]

        shard_number: int | None = pre_split_pageserver_ids.get(pageserver.id, None)
        if shard_number is None:
            assert received == 0
            assert observed == 0
        elif shard_number == 0:
            # Shard 0 receives its own records and observes records of other shards
            # for relation size tracking.
            assert observed > 0
            assert received > 0
            observed_on_shard_zero = int(observed)
        else:
            # Non zero shards do not observe any records, but only receive their own.
            assert observed == 0
            assert received > 0
            received_on_non_zero_shard += int(received)

    # Some records are sent to multiple shards and some shard 0 records include both value observations
    # and other metadata. Hence, we do a sanity check below that shard 0 observes the majority of records
    # received by other shards.
    assert (
        observed_on_shard_zero <= received_on_non_zero_shard
        and observed_on_shard_zero >= received_on_non_zero_shard // 2
    )

    # TODO: validate that shards have different sizes

    workload.validate()

    assert len(pre_split_pageserver_ids) == shard_count

    def shards_on_disk(shard_ids):
        for pageserver in env.pageservers:
            for shard_id in shard_ids:
                if pageserver.tenant_dir(shard_id).exists():
                    return True

        return False

    old_shard_ids = [TenantShardId(tenant_id, i, shard_count) for i in range(0, shard_count)]
    # Before split, old shards exist
    assert shards_on_disk(old_shard_ids)

    # Before split, we have done one reconcile for each shard
    assert (
        env.storage_controller.get_metric_value(
            "storage_controller_reconcile_complete_total", filter={"status": "ok"}
        )
        == shard_count
    )

    # Make secondary downloads slow: this exercises the storage controller logic for not migrating an attachment
    # during post-split optimization until the secondary is ready
    for ps in env.pageservers:
        ps.http_client().configure_failpoints([("secondary-layer-download-sleep", "return(1000)")])

    env.storage_controller.tenant_shard_split(tenant_id, shard_count=split_shard_count)

    post_split_pageserver_ids = [loc["node_id"] for loc in env.storage_controller.locate(tenant_id)]
    # We should have split into 8 shards, on the same 4 pageservers we started on.
    assert len(post_split_pageserver_ids) == split_shard_count
    assert len(set(post_split_pageserver_ids)) == shard_count
    assert set(post_split_pageserver_ids) == set(pre_split_pageserver_ids.keys())

    # The old parent shards should no longer exist on disk
    assert not shards_on_disk(old_shard_ids)

    # Enough background reconciliations should result in the shards being properly distributed.
    # Run this before the workload, because its LSN-waiting code presumes stable locations.
    env.storage_controller.reconcile_until_idle(timeout_secs=60)

    workload.validate()

    workload.churn_rows(256)

    workload.validate()

    # Run GC on all new shards, to check they don't barf or delete anything that breaks reads
    # (compaction was already run as part of churn_rows)
    all_shards = tenant_get_shards(env, tenant_id)
    for tenant_shard_id, pageserver in all_shards:
        pageserver.http_client().timeline_gc(tenant_shard_id, timeline_id, None)
    workload.validate()

    # Assert on how many reconciles happened during the process.  This is something of an
    # implementation detail, but it is useful to detect any bugs that might generate spurious
    # extra reconcile iterations.
    #
    # We'll have:
    # - shard_count reconciles for the original setup of the tenant
    # - shard_count reconciles for detaching the original secondary locations during split
    # - split_shard_count reconciles during shard splitting, for setting up secondaries.
    # - split_shard_count/2 reconciles to migrate shards to their temporary secondaries
    expect_reconciles = shard_count * 2 + split_shard_count + 3 * (split_shard_count / 2)

    reconcile_ok = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "ok"}
    )
    assert reconcile_ok == expect_reconciles

    # Check that no cancelled or errored reconciliations occurred: this test does no
    # failure injection and should run clean.
    cancelled_reconciles = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "cancel"}
    )
    errored_reconciles = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "error"}
    )
    assert cancelled_reconciles is not None and int(cancelled_reconciles) == 0
    assert errored_reconciles is not None and int(errored_reconciles) == 0

    # We should see that the migration of shards after the split waited for secondaries to warm up
    # before happening
    assert env.storage_controller.log_contains(".*Skipping.*because secondary isn't ready.*")

    env.storage_controller.consistency_check()

    def get_node_shard_counts(env: NeonEnv, tenant_ids):
        total: defaultdict[int, int] = defaultdict(int)
        attached: defaultdict[int, int] = defaultdict(int)
        for tid in tenant_ids:
            for shard in env.storage_controller.tenant_describe(tid)["shards"]:
                log.info(
                    f"{shard['tenant_shard_id']}: attached={shard['node_attached']}, secondary={shard['node_secondary']} "
                )
                for node in shard["node_secondary"]:
                    total[int(node)] += 1
                attached[int(shard["node_attached"])] += 1
                total[int(shard["node_attached"])] += 1

        return total, attached

    def check_effective_tenant_config():
        # Expect our custom tenant configs to have survived the split
        for shard in env.storage_controller.tenant_describe(tenant_id)["shards"]:
            node = env.get_pageserver(int(shard["node_attached"]))
            config = node.http_client().tenant_config(TenantShardId.parse(shard["tenant_shard_id"]))
            for k, v in non_default_tenant_config.items():
                assert config.effective_config[k] == v

            # Check that heatmap uploads remain enabled after shard split
            # (https://github.com/neondatabase/neon/issues/8189)
            assert (
                config.effective_config["heatmap_period"]
                and config.effective_config["heatmap_period"] != "0s"
            )

    # Validate pageserver state: expect every child shard to have an attached and secondary location
    (total, attached) = get_node_shard_counts(env, tenant_ids=[tenant_id])
    assert sum(attached.values()) == split_shard_count
    assert sum(total.values()) == split_shard_count * 2
    check_effective_tenant_config()

    # More specific check: that we are fully balanced.  It is deterministic that we will get exactly
    # one shard on each pageserver, because for these small shards the utilization metric is
    # dominated by shard count.
    log.info(f"total: {total}")
    assert total == {
        1: 1,
        2: 1,
        3: 1,
        4: 1,
        5: 1,
        6: 1,
        7: 1,
        8: 1,
    }

    # The controller is not required to lay out the attached locations in any particular way, but
    # all the pageservers that originally held an attached shard should still hold one, otherwise
    # it would indicate that we had done some unnecessary migration.
    log.info(f"attached: {attached}")
    for ps_id in pre_split_pageserver_ids.keys():
        log.info("Pre-split pageserver {ps_id} should still hold an attached location")
        assert ps_id in attached

    # Ensure post-split pageserver locations survive a restart (i.e. the child shards
    # correctly wrote config to disk, and the storage controller responds correctly
    # to /re-attach)
    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()

    # Validate pageserver state: expect every child shard to have an attached and secondary location
    (total, attached) = get_node_shard_counts(env, tenant_ids=[tenant_id])
    assert sum(attached.values()) == split_shard_count
    assert sum(total.values()) == split_shard_count * 2
    check_effective_tenant_config()

    workload.validate()


@pytest.mark.parametrize("initial_stripe_size", [None, 65536])
def test_sharding_split_stripe_size(
    neon_env_builder: NeonEnvBuilder,
    httpserver: HTTPServer,
    httpserver_listen_address: ListenAddress,
    initial_stripe_size: int,
):
    """
    Check that modifying stripe size inline with a shard split works as expected
    """
    (host, port) = httpserver_listen_address
    neon_env_builder.control_plane_compute_hook_api = f"http://{host}:{port}/notify"
    neon_env_builder.num_pageservers = 1

    # Set up fake HTTP notify endpoint: we will use this to validate that we receive
    # the correct stripe size after split.
    notifications = []

    def handler(request: Request):
        log.info(f"Notify request: {request}")
        notifications.append(request.json)
        return Response(status=200)

    httpserver.expect_request("/notify", method="PUT").respond_with_handler(handler)

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=1, initial_tenant_shard_stripe_size=initial_stripe_size
    )
    tenant_id = env.initial_tenant

    assert len(notifications) == 1
    expect: dict[str, list[dict[str, int]] | str | None | int] = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[0].id), "shard_number": 0}],
        "preferred_az": DEFAULT_AZ_ID,
    }
    assert notifications[0] == expect

    new_stripe_size = 2048
    env.storage_controller.tenant_shard_split(
        tenant_id, shard_count=2, shard_stripe_size=new_stripe_size
    )
    env.storage_controller.reconcile_until_idle()

    # Check that we ended up with the stripe size that we expected, both on the pageserver
    # and in the notifications to compute
    assert len(notifications) == 2
    expect_after: dict[str, list[dict[str, int]] | str | None | int] = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": new_stripe_size,
        "shards": [
            {"node_id": int(env.pageservers[0].id), "shard_number": 0},
            {"node_id": int(env.pageservers[0].id), "shard_number": 1},
        ],
        "preferred_az": DEFAULT_AZ_ID,
    }
    log.info(f"Got notification: {notifications[1]}")
    assert notifications[1] == expect_after

    # Inspect the stripe size on the pageserver
    shard_0_loc = (
        env.pageservers[0].http_client().tenant_get_location(TenantShardId(tenant_id, 0, 2))
    )
    assert shard_0_loc["shard_stripe_size"] == new_stripe_size
    shard_1_loc = (
        env.pageservers[0].http_client().tenant_get_location(TenantShardId(tenant_id, 1, 2))
    )
    assert shard_1_loc["shard_stripe_size"] == new_stripe_size

    # Ensure stripe size survives a pageserver restart
    env.pageservers[0].stop()
    env.pageservers[0].start()
    shard_0_loc = (
        env.pageservers[0].http_client().tenant_get_location(TenantShardId(tenant_id, 0, 2))
    )
    assert shard_0_loc["shard_stripe_size"] == new_stripe_size
    shard_1_loc = (
        env.pageservers[0].http_client().tenant_get_location(TenantShardId(tenant_id, 1, 2))
    )
    assert shard_1_loc["shard_stripe_size"] == new_stripe_size

    # Ensure stripe size survives a storage controller restart
    env.storage_controller.stop()
    env.storage_controller.start()

    def assert_restart_notification():
        assert len(notifications) == 3
        assert notifications[2] == expect_after

    wait_until(assert_restart_notification)


# The quantity of data isn't huge, but debug can be _very_ slow, and the things we're
# validating in this test don't benefit much from debug assertions.
@skip_in_debug_build("Avoid running bulkier ingest tests in debug mode")
def test_sharding_ingest_layer_sizes(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Check that when ingesting data to a sharded tenant, we properly respect layer size limts.
    """

    # Set a small stripe size and checkpoint distance, so that we can exercise rolling logic
    # without writing a lot of data.
    expect_layer_size = 131072
    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": f"{expect_layer_size}",
        "compaction_target_size": f"{expect_layer_size}",
        # aim to reduce flakyness, we are not doing explicit checkpointing
        "compaction_period": "0s",
        "gc_period": "0s",
    }
    shard_count = 4
    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF,
        initial_tenant_shard_count=shard_count,
        # A stripe size the same order of magnitude as layer size: this ensures that
        # within checkpoint_distance some shards will have no data to ingest, if LSN
        # contains sequential page writes.  This test checks that this kind of
        # scenario doesn't result in some shards emitting empty/tiny layers.
        initial_tenant_shard_stripe_size=expect_layer_size // 8192,
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # ignore the initdb layer(s) for the purposes of the size comparison as a initdb image layer optimization
    # will produce a lot more smaller layers.
    initial_layers_per_shard = {}
    log.info("initdb distribution (not asserted on):")
    for shard in env.storage_controller.locate(tenant_id):
        pageserver = env.get_pageserver(shard["node_id"])
        shard_id = shard["shard_id"]
        layers = (
            env.get_pageserver(shard["node_id"]).http_client().layer_map_info(shard_id, timeline_id)
        )
        for layer in layers.historic_layers:
            log.info(
                f"layer[{pageserver.id}]: {layer.layer_file_name} (size {layer.layer_file_size})"
            )

        initial_layers_per_shard[shard_id] = set(layers.historic_layers)

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)

    workload.validate()

    small_layer_count = 0
    ok_layer_count = 0
    huge_layer_count = 0

    # Inspect the resulting layer map, count how many layers are undersized.
    for shard in env.storage_controller.locate(tenant_id):
        pageserver = env.get_pageserver(shard["node_id"])
        shard_id = shard["shard_id"]
        layer_map = pageserver.http_client().layer_map_info(shard_id, timeline_id)

        historic_layers = sorted(layer_map.historic_layers, key=lambda layer: layer.lsn_start)

        initial_layers = initial_layers_per_shard[shard_id]

        for layer in historic_layers:
            if layer in initial_layers:
                # ignore the initdb image layers for the size histogram
                continue

            if layer.layer_file_size < expect_layer_size // 2:
                classification = "Small"
                small_layer_count += 1
            elif layer.layer_file_size > expect_layer_size * 2:
                classification = "Huge "
                huge_layer_count += 1
            else:
                classification = "OK   "
                ok_layer_count += 1

            if layer.kind == "Delta":
                assert layer.lsn_end is not None
                lsn_size = Lsn(layer.lsn_end) - Lsn(layer.lsn_start)
            else:
                lsn_size = 0

            log.info(
                f"{classification} layer[{pageserver.id}]: {layer.layer_file_name} (size {layer.layer_file_size}, LSN distance {lsn_size})"
            )

    # Why an inexact check?
    # - Because we roll layers on checkpoint_distance * shard_count, we expect to obey the target
    #   layer size on average, but it is still possible to write some tiny layers.
    log.info(f"Totals: {small_layer_count} small layers, {ok_layer_count} ok layers")
    if small_layer_count <= shard_count:
        # If each shard has <= 1 small layer
        pass
    else:
        # General case:
        # old limit was 0.25 but pg14 is right at the limit with 7/28
        assert float(small_layer_count) / float(ok_layer_count) < 0.3

    # Each shard may emit up to one huge layer, because initdb ingest doesn't respect checkpoint_distance.
    assert huge_layer_count <= shard_count


def test_sharding_ingest_gaps(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Check ingest behavior when the incoming data results in some shards having gaps where
    no data is ingested: they should advance their disk_consistent_lsn and remote_consistent_lsn
    even if they aren't writing out layers.
    """

    # Set a small stripe size and checkpoint distance, so that we can exercise rolling logic
    # without writing a lot of data.
    expect_layer_size = 131072
    checkpoint_interval_secs = 5
    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": f"{expect_layer_size}",
        "compaction_target_size": f"{expect_layer_size}",
        # Set a short checkpoint interval as we will wait for uploads to happen
        "checkpoint_timeout": f"{checkpoint_interval_secs}s",
        # Background checkpointing is done from compaction loop, so set that interval short too
        "compaction_period": "1s",
    }
    shard_count = 4
    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF,
        initial_tenant_shard_count=shard_count,
        initial_tenant_shard_stripe_size=128,
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Just a few writes: we aim to produce a situation where some shards are skipping
    # ingesting some records and thereby won't have layer files that advance their
    # consistent LSNs, to exercise the code paths that explicitly handle this case by
    # advancing consistent LSNs in the background if there is no open layer.
    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(128, upload=False)
    workload.churn_rows(128, upload=False)

    # Checkpoint, so that we won't get a background checkpoint happening during the next step
    workload.endpoint().safe_psql("checkpoint")
    # Freeze + flush, so that subsequent writes will start from a position of no open layers
    last_flush_lsn_upload(env, workload.endpoint(), tenant_id, timeline_id)

    # This write is tiny: at least some of the shards should find they don't have any
    # data to ingest.  This will exercise how they handle that.
    workload.churn_rows(1, upload=False)

    # The LSN that has reached pageservers, but may not have been flushed to historic layers yet
    expect_lsn = wait_for_last_flush_lsn(env, workload.endpoint(), tenant_id, timeline_id)

    # Don't leave the endpoint running, we don't want it writing in the background
    workload.stop()

    log.info(f"Waiting for shards' consistent LSNs to reach {expect_lsn}")

    shards = tenant_get_shards(env, tenant_id, None)

    def assert_all_disk_consistent():
        """
        Assert that all the shards' disk_consistent_lsns have reached expect_lsn
        """
        for tenant_shard_id, pageserver in shards:
            timeline_detail = pageserver.http_client().timeline_detail(tenant_shard_id, timeline_id)
            log.info(f"{tenant_shard_id} (ps {pageserver.id}) detail: {timeline_detail}")
            assert Lsn(timeline_detail["disk_consistent_lsn"]) >= expect_lsn

    # We set a short checkpoint timeout: expect things to get frozen+flushed within that
    wait_until(assert_all_disk_consistent, timeout=3 * checkpoint_interval_secs)

    def assert_all_remote_consistent():
        """
        Assert that all the shards' remote_consistent_lsns have reached expect_lsn
        """
        for tenant_shard_id, pageserver in shards:
            timeline_detail = pageserver.http_client().timeline_detail(tenant_shard_id, timeline_id)
            log.info(f"{tenant_shard_id} (ps {pageserver.id}) detail: {timeline_detail}")
            assert Lsn(timeline_detail["remote_consistent_lsn"]) >= expect_lsn

    # We set a short checkpoint timeout: expect things to get frozen+flushed within that
    wait_until(assert_all_remote_consistent, timeout=3 * checkpoint_interval_secs)

    workload.validate()


class Failure:
    pageserver_id: int | None

    def apply(self, env: NeonEnv):
        raise NotImplementedError()

    def clear(self, env: NeonEnv):
        """
        Clear the failure, in a way that should enable the system to proceed
        to a totally clean state (all nodes online and reconciled)
        """
        raise NotImplementedError()

    def expect_available(self):
        raise NotImplementedError()

    def can_mitigate(self):
        """Whether Self.mitigate is available for use"""
        return False

    def mitigate(self, env: NeonEnv):
        """
        Mitigate the failure in a way that should allow shard split to
        complete and service to resume, but does not guarantee to leave
        the whole world in a clean state (e.g. an Offline node might have
        junk LocationConfigs on it)
        """
        raise NotImplementedError()

    def fails_forward(self, env: NeonEnv):
        """
        If true, this failure results in a state that eventualy completes the split.
        """
        return False

    def expect_exception(self):
        """
        How do we expect a call to the split API to fail?
        """
        return StorageControllerApiException


class PageserverFailpoint(Failure):
    def __init__(self, failpoint, pageserver_id, mitigate):
        self.failpoint = failpoint
        self.pageserver_id = pageserver_id
        self._mitigate = mitigate

    @override
    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.allowed_errors.extend(
            [".*failpoint.*", ".*Resetting.*after shard split failure.*"]
        )
        pageserver.http_client().configure_failpoints((self.failpoint, "return(1)"))

    @override
    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.http_client().configure_failpoints((self.failpoint, "off"))
        if self._mitigate:
            env.storage_controller.node_configure(self.pageserver_id, {"availability": "Active"})

    @override
    def expect_available(self):
        return True

    @override
    def can_mitigate(self):
        return self._mitigate

    @override
    def mitigate(self, env: NeonEnv):
        env.storage_controller.node_configure(self.pageserver_id, {"availability": "Offline"})


class StorageControllerFailpoint(Failure):
    def __init__(self, failpoint, action):
        self.failpoint = failpoint
        self.pageserver_id = None
        self.action = action

    @override
    def apply(self, env: NeonEnv):
        env.storage_controller.configure_failpoints((self.failpoint, self.action))

    @override
    def clear(self, env: NeonEnv):
        if "panic" in self.action:
            log.info("Restarting storage controller after panic")
            env.storage_controller.stop()
            env.storage_controller.start()
        else:
            env.storage_controller.configure_failpoints((self.failpoint, "off"))

    @override
    def expect_available(self):
        # Controller panics _do_ leave pageservers available, but our test code relies
        # on using the locate API to update configurations in Workload, so we must skip
        # these actions when the controller has been panicked.
        return "panic" not in self.action

    @override
    def can_mitigate(self):
        return False

    @override
    def fails_forward(self, env: NeonEnv):
        # Edge case: the very last failpoint that simulates a DB connection error, where
        # the abort path will fail-forward and result in a complete split.
        fail_forward = self.failpoint == "shard-split-post-complete"

        # If the failure was a panic, then if we expect split to eventually (after restart)
        # complete, we must restart before checking that.
        if fail_forward and "panic" in self.action:
            log.info("Restarting storage controller after panic")
            env.storage_controller.stop()
            env.storage_controller.start()

        return fail_forward

    @override
    def expect_exception(self):
        if "panic" in self.action:
            return requests.exceptions.ConnectionError
        else:
            return StorageControllerApiException


class NodeKill(Failure):
    def __init__(self, pageserver_id, mitigate):
        self.pageserver_id = pageserver_id
        self._mitigate = mitigate

    @override
    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.stop(immediate=True)

    @override
    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.start()

    @override
    def expect_available(self):
        return False

    @override
    def mitigate(self, env: NeonEnv):
        env.storage_controller.node_configure(self.pageserver_id, {"availability": "Offline"})


class CompositeFailure(Failure):
    """
    Wrapper for failures in multiple components (e.g. a failpoint in the storage controller, *and*
    stop a pageserver to interfere with rollback)
    """

    def __init__(self, failures: list[Failure]):
        self.failures = failures

        self.pageserver_id = None
        for f in failures:
            if f.pageserver_id is not None:
                self.pageserver_id = f.pageserver_id
                break

    @override
    def apply(self, env: NeonEnv):
        for f in self.failures:
            f.apply(env)

    @override
    def clear(self, env: NeonEnv):
        for f in self.failures:
            f.clear(env)

    @override
    def expect_available(self):
        return all(f.expect_available() for f in self.failures)

    @override
    def mitigate(self, env: NeonEnv):
        for f in self.failures:
            f.mitigate(env)

    @override
    def expect_exception(self):
        expect = set(f.expect_exception() for f in self.failures)

        # We can't give a sensible response if our failures have different expectations
        assert len(expect) == 1

        return list(expect)[0]


@pytest.mark.parametrize(
    "failure",
    [
        PageserverFailpoint("api-500", 1, False),
        NodeKill(1, False),
        PageserverFailpoint("api-500", 1, True),
        NodeKill(1, True),
        PageserverFailpoint("shard-split-pre-prepare", 1, False),
        PageserverFailpoint("shard-split-post-prepare", 1, False),
        PageserverFailpoint("shard-split-pre-hardlink", 1, False),
        PageserverFailpoint("shard-split-post-hardlink", 1, False),
        PageserverFailpoint("shard-split-post-child-conf", 1, False),
        PageserverFailpoint("shard-split-lsn-wait", 1, False),
        PageserverFailpoint("shard-split-pre-finish", 1, False),
        StorageControllerFailpoint("shard-split-validation", "return(1)"),
        StorageControllerFailpoint("shard-split-post-begin", "return(1)"),
        StorageControllerFailpoint("shard-split-post-remote", "return(1)"),
        StorageControllerFailpoint("shard-split-post-complete", "return(1)"),
        StorageControllerFailpoint("shard-split-validation", "panic(failpoint)"),
        StorageControllerFailpoint("shard-split-post-begin", "panic(failpoint)"),
        StorageControllerFailpoint("shard-split-post-remote", "panic(failpoint)"),
        StorageControllerFailpoint("shard-split-post-complete", "panic(failpoint)"),
        CompositeFailure(
            [NodeKill(1, True), StorageControllerFailpoint("shard-split-post-begin", "return(1)")]
        ),
        CompositeFailure(
            [NodeKill(1, False), StorageControllerFailpoint("shard-split-post-begin", "return(1)")]
        ),
    ],
)
def test_sharding_split_failures(
    neon_env_builder: NeonEnvBuilder,
    compute_reconfigure_listener: ComputeReconfigure,
    failure: Failure,
):
    neon_env_builder.num_pageservers = 4
    neon_env_builder.control_plane_compute_hook_api = (
        compute_reconfigure_listener.control_plane_compute_hook_api
    )
    initial_shard_count = 2
    split_shard_count = 4

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()

    # Create a tenant with secondary locations enabled
    env.create_tenant(
        tenant_id, timeline_id, shard_count=initial_shard_count, placement_policy='{"Attached":1}'
    )

    env.storage_controller.allowed_errors.extend(
        [
            # All split failures log a warning when then enqueue the abort operation
            ".*Enqueuing background abort.*",
            # We exercise failure cases where abort itself will also fail (node offline)
            ".*abort_tenant_shard_split.*",
            ".*Failed to abort.*",
            # Tolerate any error lots that mention a failpoint
            ".*failpoint.*",
            # Node offline cases will fail to send requests
            ".*Reconcile error: receive body: error sending request for url.*",
            # Node offline cases will fail inside reconciler when detaching secondaries
            ".*Reconcile error on shard.*: receive body: error sending request for url.*",
            # Node offline cases may eventually cancel reconcilers when the heartbeater realizes nodes are offline
            ".*Reconcile error.*Cancelled.*",
            # While parent shard's client is stopped during split, flush loop updating LSNs will emit this warning
            ".*Failed to schedule metadata upload after updating disk_consistent_lsn.*",
        ]
    )

    for ps in env.pageservers:
        # If we're using a failure that will panic the storage controller, all background
        # upcalls from the pageserver can fail
        ps.allowed_errors.append(".*calling control plane generation validation API failed.*")

    # Make sure the node we're failing has a shard on it, otherwise the test isn't testing anything
    assert (
        failure.pageserver_id is None
        or len(
            env.get_pageserver(failure.pageserver_id)
            .http_client()
            .tenant_list_locations()["tenant_shards"]
        )
        > 0
    )

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(100)

    # Put the environment into a failing state (exact meaning depends on `failure`)
    failure.apply(env)

    with pytest.raises(failure.expect_exception()):
        env.storage_controller.tenant_shard_split(tenant_id, shard_count=4)

    # We expect that the overall operation will fail, but some split requests
    # will have succeeded: the net result should be to return to a clean state, including
    # detaching any child shards.
    def assert_rolled_back(exclude_ps_id=None) -> None:
        secondary_count = 0
        attached_count = 0
        for ps in env.pageservers:
            if exclude_ps_id is not None and ps.id == exclude_ps_id:
                continue

            locations = ps.http_client().tenant_list_locations()["tenant_shards"]
            for loc in locations:
                tenant_shard_id = TenantShardId.parse(loc[0])
                log.info(f"Shard {tenant_shard_id} seen on node {ps.id} in mode {loc[1]['mode']}")
                assert tenant_shard_id.shard_count == initial_shard_count
                if loc[1]["mode"] == "Secondary":
                    secondary_count += 1
                else:
                    attached_count += 1

        assert secondary_count == initial_shard_count
        assert attached_count == initial_shard_count

    def assert_split_done(exclude_ps_id: int | None = None) -> None:
        secondary_count = 0
        attached_count = 0
        for ps in env.pageservers:
            if exclude_ps_id is not None and ps.id == exclude_ps_id:
                continue

            locations = ps.http_client().tenant_list_locations()["tenant_shards"]
            for loc in locations:
                tenant_shard_id = TenantShardId.parse(loc[0])
                log.info(f"Shard {tenant_shard_id} seen on node {ps.id} in mode {loc[1]['mode']}")
                assert tenant_shard_id.shard_count == split_shard_count
                if loc[1]["mode"] == "Secondary":
                    secondary_count += 1
                else:
                    attached_count += 1
        assert attached_count == split_shard_count
        assert secondary_count == split_shard_count

    def finish_split():
        # Having failed+rolled back, we should be able to split again
        # No failures this time; it will succeed
        env.storage_controller.tenant_shard_split(tenant_id, shard_count=split_shard_count)
        env.storage_controller.reconcile_until_idle(timeout_secs=30)

        workload.churn_rows(10)
        workload.validate()

    if failure.expect_available():
        # Even though the split failed partway through, this should not leave the tenant in
        # an unavailable state.
        # - Disable waiting for pageservers in the workload helper, because our
        #   failpoints may prevent API access. This only applies for failure modes that
        #   leave pageserver page_service API available.
        # - This is a wait_until because clients may see transient errors in some split error cases,
        #   e.g. while waiting for a storage controller to re-attach a parent shard if we failed
        #   inside the pageserver and the storage controller responds by detaching children and attaching
        #   parents concurrently (https://github.com/neondatabase/neon/issues/7148)
        wait_until(lambda: workload.churn_rows(10, upload=False, ingest=False))

        workload.validate()

    if failure.fails_forward(env):
        log.info("Fail-forward failure, checking split eventually completes...")
        # A failure type which results in eventual completion of the split
        wait_until(assert_split_done)
    elif failure.can_mitigate():
        log.info("Mitigating failure...")
        # Mitigation phase: we expect to be able to proceed with a successful shard split
        failure.mitigate(env)

        # The split should appear to be rolled back from the point of view of all pageservers
        # apart from the one that is offline
        wait_until(lambda: assert_rolled_back(exclude_ps_id=failure.pageserver_id))

        finish_split()
        wait_until(lambda: assert_split_done(exclude_ps_id=failure.pageserver_id))

        # Having cleared the failure, everything should converge to a pristine state
        failure.clear(env)
        wait_until(assert_split_done)
    else:
        # Once we restore the faulty pageserver's API to good health, rollback should
        # eventually complete.
        log.info("Clearing failure...")
        failure.clear(env)

        wait_until(assert_rolled_back)

        # Having rolled back, the tenant should be working
        workload.churn_rows(10)
        workload.validate()

        # Splitting again should work, since we cleared the failure
        finish_split()
        assert_split_done()

    if isinstance(failure, StorageControllerFailpoint) and "post-complete" in failure.failpoint:
        # On a post-complete failure, the controller will recover the post-split state
        # after restart, but it will have missed the optimization part of the split function
        # where secondary downloads are kicked off.  This means that reconcile_until_idle
        # will take a very long time if we wait for all optimizations to complete, because
        # those optimizations will wait for secondary downloads.
        #
        # Avoid that by configuring the tenant into Essential scheduling mode, so that it will
        # skip optimizations when we're exercising this particular failpoint.
        env.storage_controller.tenant_policy_update(tenant_id, {"scheduling": "Essential"})

    # Having completed the split, pump the background reconciles to ensure that
    # the scheduler reaches an idle state
    env.storage_controller.reconcile_until_idle(timeout_secs=30)

    env.storage_controller.consistency_check()


def test_sharding_backpressure(neon_env_builder: NeonEnvBuilder):
    """
    Check a scenario when one of the shards is much slower than others.
    Without backpressure, this would lead to the slow shard falling behind
    and eventually causing WAL timeouts.
    """

    shard_count = 4
    neon_env_builder.num_pageservers = shard_count

    # 256KiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.  The stripe size is given
    # in number of 8KiB pages.
    stripe_size = 32

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_shard_stripe_size=stripe_size
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    pageservers = dict((int(p.id), p) for p in env.pageservers)
    shards = env.storage_controller.locate(tenant_id)

    # Slow down one of the shards, around ~1MB/s
    pageservers[4].http_client().configure_failpoints(("wal-ingest-record-sleep", "5%sleep(1)"))

    def shards_info():
        infos = []
        for shard in shards:
            node_id = int(shard["node_id"])
            pageserver = pageservers[node_id]
            shard_info = pageserver.http_client().timeline_detail(shard["shard_id"], timeline_id)
            infos.append(shard_info)
            last_record_lsn = shard_info["last_record_lsn"]
            current_physical_size = shard_info["current_physical_size"]
            log.info(
                f"Shard on pageserver {node_id}: lsn={last_record_lsn}, size={current_physical_size}"
            )
        return infos

    shards_info()

    workload = Workload(
        env,
        tenant_id,
        timeline_id,
        branch_name="main",
        endpoint_opts={
            "config_lines": [
                # Tip: set to 100MB to make the test fail
                "max_replication_write_lag=1MB",
            ],
        },
    )
    workload.init()

    endpoint = workload.endpoint()

    # on 2024-03-05, the default config on prod was [15MB, 10GB, null]
    res = endpoint.safe_psql_many(
        [
            "SHOW max_replication_write_lag",
            "SHOW max_replication_flush_lag",
            "SHOW max_replication_apply_lag",
        ]
    )
    log.info(f"backpressure config: {res}")

    last_flush_lsn = None
    last_timestamp = None

    def update_write_lsn():
        nonlocal last_flush_lsn
        nonlocal last_timestamp

        res = endpoint.safe_psql(
            """
            SELECT
                pg_wal_lsn_diff(pg_current_wal_flush_lsn(), received_lsn) as received_lsn_lag,
                received_lsn,
                pg_current_wal_flush_lsn() as flush_lsn,
                neon.backpressure_throttling_time() as throttling_time
            FROM neon.backpressure_lsns();
            """,
            dbname="postgres",
        )[0]
        log.info(
            f"received_lsn_lag = {res[0]}, received_lsn = {res[1]}, flush_lsn = {res[2]}, throttling_time = {res[3]}"
        )

        lsn = Lsn(res[2])
        now = time.time()

        if last_timestamp is not None:
            delta = now - last_timestamp
            delta_bytes = lsn - last_flush_lsn
            avg_speed = delta_bytes / delta / 1024 / 1024
            log.info(
                f"flush_lsn {lsn}, written {delta_bytes/1024}kb for {delta:.3f}s, avg_speed {avg_speed:.3f} MiB/s"
            )

        last_flush_lsn = lsn
        last_timestamp = now

    update_write_lsn()

    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.validate()

    update_write_lsn()
    shards_info()

    for _write_iter in range(30):
        # approximately 1MB of data
        workload.write_rows(8000, upload=False)
        update_write_lsn()
        infos = shards_info()
        min_lsn = min(Lsn(info["last_record_lsn"]) for info in infos)
        max_lsn = max(Lsn(info["last_record_lsn"]) for info in infos)
        diff = max_lsn - min_lsn
        assert diff < 2 * 1024 * 1024, f"LSN diff={diff}, expected diff < 2MB due to backpressure"


def test_sharding_unlogged_relation(neon_env_builder: NeonEnvBuilder):
    """
    Check that an unlogged relation is handled properly on a sharded tenant

    Reproducer for https://github.com/neondatabase/neon/issues/7451
    """

    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.create_tenant(tenant_id, timeline_id, shard_count=8)

    # We will create many tables to ensure it's overwhelmingly likely that at least one
    # of them doesn't land on shard 0
    table_names = [f"my_unlogged_{i}" for i in range(0, 16)]

    with env.endpoints.create_start("main", tenant_id=tenant_id) as ep:
        for table_name in table_names:
            ep.safe_psql(f"CREATE UNLOGGED TABLE {table_name} (id integer, value varchar(64));")
            ep.safe_psql(f"INSERT INTO {table_name} VALUES (1, 'foo')")
            result = ep.safe_psql(f"SELECT * from {table_name};")
            assert result == [(1, "foo")]
            ep.safe_psql(f"CREATE INDEX ON {table_name} USING btree (value);")

        wait_for_last_flush_lsn(env, ep, tenant_id, timeline_id)

    with env.endpoints.create_start("main", tenant_id=tenant_id) as ep:
        for table_name in table_names:
            # Check that table works: we can select and insert
            result = ep.safe_psql(f"SELECT * from {table_name};")
            assert result == []
            ep.safe_psql(f"INSERT INTO {table_name} VALUES (2, 'bar');")
            result = ep.safe_psql(f"SELECT * from {table_name};")
            assert result == [(2, "bar")]

        # Ensure that post-endpoint-restart modifications are ingested happily by pageserver
        wait_for_last_flush_lsn(env, ep, tenant_id, timeline_id)


def test_top_tenants(neon_env_builder: NeonEnvBuilder):
    """
    The top_tenants API is used in shard auto-splitting to find candidates.
    """

    env = neon_env_builder.init_configs()
    env.start()

    tenants = []
    n_tenants = 8
    for i in range(0, n_tenants):
        tenant_id = TenantId.generate()
        timeline_id = TimelineId.generate()
        env.create_tenant(tenant_id, timeline_id)

        # Write a different amount of data to each tenant
        w = Workload(env, tenant_id, timeline_id)
        w.init()
        w.write_rows(i * 1000)
        w.stop()

        logical_size = env.pageserver.http_client().timeline_detail(tenant_id, timeline_id)[
            "current_logical_size"
        ]
        tenants.append((tenant_id, timeline_id, logical_size))

        log.info(f"Created {tenant_id}/{timeline_id} with size {logical_size}")

    # Ask for 1 largest tenant
    top_1 = env.pageserver.http_client().top_tenants("max_logical_size", 1, 8, 0)
    assert len(top_1["shards"]) == 1
    assert top_1["shards"][0]["id"] == str(tenants[-1][0])
    assert top_1["shards"][0]["max_logical_size"] == tenants[-1][2]

    # Apply a lower bound limit
    top = env.pageserver.http_client().top_tenants(
        "max_logical_size", 100, 8, where_gt=tenants[3][2]
    )
    assert len(top["shards"]) == n_tenants - 4
    assert set(i["id"] for i in top["shards"]) == set(str(i[0]) for i in tenants[4:])


def test_sharding_gc(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Exercise GC in a sharded tenant: because only shard 0 holds SLRU content, it acts as
    the "leader" for GC, and other shards read its index to learn what LSN they should
    GC up to.
    """

    shard_count = 4
    neon_env_builder.num_pageservers = shard_count
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": 128 * 1024,
        "compaction_threshold": 1,
        "compaction_target_size": 128 * 1024,
        # A short PITR horizon, so that we won't have to sleep too long in the test to wait for it to
        # happen.
        "pitr_interval": "1s",
        # disable background compaction and GC. We invoke it manually when we want it to happen.
        "gc_period": "0s",
        "compaction_period": "0s",
        # Disable automatic creation of image layers, as we will create them explicitly when we want them
        "image_creation_threshold": 9999,
        "image_layer_creation_check_threshold": 0,
        "lsn_lease_length": "0s",
    }
    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_conf=TENANT_CONF
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Create a branch and write some data
    workload = Workload(env, tenant_id, timeline_id)
    initial_lsn = Lsn(workload.endpoint().safe_psql("SELECT pg_current_wal_lsn()")[0][0])
    log.info(f"Started at LSN: {initial_lsn}")

    workload.init()

    # Write enough data to generate multiple layers
    for _i in range(10):
        last_lsn = workload.write_rows(32)

    assert last_lsn > initial_lsn

    log.info(f"Wrote up to last LSN: {last_lsn}")

    # Do full image layer generation. When we subsequently wait for PITR, all historic deltas
    # should be GC-able
    for shard_number in range(shard_count):
        shard = TenantShardId(tenant_id, shard_number, shard_count)
        env.get_tenant_pageserver(shard).http_client().timeline_compact(
            shard, timeline_id, force_image_layer_creation=True
        )

    workload.churn_rows(32)

    time.sleep(5)

    # Invoke GC on a non-zero shard and verify its GC cutoff LSN does not advance
    shard_one = TenantShardId(tenant_id, 1, shard_count)
    env.get_tenant_pageserver(shard_one).http_client().timeline_gc(
        shard_one, timeline_id, gc_horizon=None
    )

    # Check shard 1's index - GC cutoff LSN should not have advanced
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)
    shard_1_index = env.pageserver_remote_storage.index_content(
        tenant_id=shard_one, timeline_id=timeline_id
    )
    shard_1_gc_cutoff_lsn = Lsn(shard_1_index["metadata_bytes"]["latest_gc_cutoff_lsn"])
    log.info(f"Shard 1 cutoff LSN: {shard_1_gc_cutoff_lsn}")
    assert shard_1_gc_cutoff_lsn <= last_lsn

    shard_zero = TenantShardId(tenant_id, 0, shard_count)
    env.get_tenant_pageserver(shard_zero).http_client().timeline_gc(
        shard_zero, timeline_id, gc_horizon=None
    )

    # TODO: observe that GC LSN of shard 0 has moved forward in remote storage
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)
    shard_0_index = env.pageserver_remote_storage.index_content(
        tenant_id=shard_zero, timeline_id=timeline_id
    )
    shard_0_gc_cutoff_lsn = Lsn(shard_0_index["metadata_bytes"]["latest_gc_cutoff_lsn"])
    log.info(f"Shard 0 cutoff LSN: {shard_0_gc_cutoff_lsn}")
    assert shard_0_gc_cutoff_lsn >= last_lsn

    # Invoke GC on all other shards and verify their GC cutoff LSNs
    for shard_number in range(1, shard_count):
        shard = TenantShardId(tenant_id, shard_number, shard_count)
        env.get_tenant_pageserver(shard).http_client().timeline_gc(
            shard, timeline_id, gc_horizon=None
        )

        # Verify GC cutoff LSN advanced to match shard 0
        shard_index = env.pageserver_remote_storage.index_content(
            tenant_id=shard, timeline_id=timeline_id
        )
        shard_gc_cutoff_lsn = Lsn(shard_index["metadata_bytes"]["latest_gc_cutoff_lsn"])
        log.info(f"Shard {shard_number} cutoff LSN: {shard_gc_cutoff_lsn}")
        assert shard_gc_cutoff_lsn == shard_0_gc_cutoff_lsn
