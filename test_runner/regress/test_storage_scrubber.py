from __future__ import annotations

import os
import pprint
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fixtures.common_types import TenantId, TenantShardId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pg_version import PgVersion
from fixtures.remote_storage import S3Storage, s3_storage
from fixtures.utils import wait_until
from fixtures.workload import Workload


@pytest.mark.parametrize("shard_count", [None, 4])
def test_scrubber_tenant_snapshot(neon_env_builder: NeonEnvBuilder, shard_count: int | None):
    """
    Test the `tenant-snapshot` subcommand, which grabs data from remote storage

    This is only a support/debug tool, but worth testing to ensure the tool does not regress.
    """

    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = shard_count if shard_count is not None else 1

    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    branch = "main"

    # Do some work
    workload = Workload(env, tenant_id, timeline_id, branch)
    workload.init()

    # Multiple write/flush passes to generate multiple layers
    for _n in range(0, 3):
        workload.write_rows(128)

    # Do some more work after a restart, so that we have multiple generations
    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()

    for _n in range(0, 3):
        workload.write_rows(128)

    # If we're doing multiple shards, split: this is important to exercise
    # the scrubber's ability to understand the references from child shards to parent shard's layers
    if shard_count is not None:
        tenant_shard_ids = env.storage_controller.tenant_shard_split(
            tenant_id, shard_count=shard_count
        )

        # Write after shard split: this will result in shards containing a mixture of owned
        # and parent layers in their index.
        workload.write_rows(128)
    else:
        tenant_shard_ids = [TenantShardId(tenant_id, 0, 0)]

    output_path = neon_env_builder.test_output_dir / "snapshot"
    os.makedirs(output_path)

    env.storage_scrubber.tenant_snapshot(tenant_id, output_path)

    assert len(os.listdir(output_path)) > 0

    workload.stop()

    # Stop pageservers
    for pageserver in env.pageservers:
        pageserver.stop()

    # Drop all shards' local storage
    for tenant_shard_id in tenant_shard_ids:
        pageserver = env.get_tenant_pageserver(tenant_shard_id)
        shutil.rmtree(pageserver.timeline_dir(tenant_shard_id, timeline_id))

    # Replace remote storage contents with the snapshot we downloaded
    assert isinstance(env.pageserver_remote_storage, S3Storage)

    remote_tenant_path = env.pageserver_remote_storage.tenant_path(tenant_id)

    # Delete current remote storage contents
    bucket = env.pageserver_remote_storage.bucket_name
    remote_client = env.pageserver_remote_storage.client
    deleted = 0
    for object in remote_client.list_objects_v2(Bucket=bucket, Prefix=remote_tenant_path)[
        "Contents"
    ]:
        key = object["Key"]
        remote_client.delete_object(Key=key, Bucket=bucket)
        deleted += 1
    assert deleted > 0

    # Upload from snapshot
    for root, _dirs, files in os.walk(output_path):
        for file in files:
            full_local_path = os.path.join(root, file)
            full_remote_path = (
                env.pageserver_remote_storage.tenants_path()
                + "/"
                + full_local_path.removeprefix(f"{output_path}/")
            )
            remote_client.upload_file(full_local_path, bucket, full_remote_path)

    for pageserver in env.pageservers:
        pageserver.start()

    # Check we can read everything
    workload.validate()


def drop_local_state(env: NeonEnv, tenant_id: TenantId):
    env.storage_controller.tenant_policy_update(tenant_id, {"placement": "Detached"})
    env.storage_controller.reconcile_until_idle()

    env.storage_controller.tenant_policy_update(tenant_id, {"placement": {"Attached": 0}})
    env.storage_controller.reconcile_until_idle()


@pytest.mark.parametrize("shard_count", [None, 4])
def test_scrubber_physical_gc(neon_env_builder: NeonEnvBuilder, shard_count: int | None):
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.create_tenant(tenant_id, timeline_id, shard_count=shard_count)

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()

    # We will end up with an index per shard, per cycle, plus one for the initial startup
    n_cycles = 4
    expect_indices_per_shard = n_cycles + 1
    shard_count = 1 if shard_count is None else shard_count

    # For each cycle, detach and attach the tenant to bump the generation, and do some writes to generate uploads
    for _i in range(0, n_cycles):
        drop_local_state(env, tenant_id)

        # This write includes remote upload, will generate an index in this generation
        workload.write_rows(1)

    # We will use a min_age_secs=1 threshold for deletion, let it pass
    time.sleep(2)

    # With a high min_age, the scrubber should decline to delete anything
    gc_summary = env.storage_scrubber.pageserver_physical_gc(min_age_secs=3600)
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0

    # If targeting a different tenant, the scrubber shouldn't do anything
    gc_summary = env.storage_scrubber.pageserver_physical_gc(
        min_age_secs=1, tenant_ids=[TenantId.generate()]
    )
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0

    #  With a low min_age, the scrubber should go ahead and clean up all but the latest 2 generations
    gc_summary = env.storage_scrubber.pageserver_physical_gc(min_age_secs=1)
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == (expect_indices_per_shard - 2) * shard_count


@pytest.mark.parametrize("shard_count", [None, 2])
def test_scrubber_physical_gc_ancestors(neon_env_builder: NeonEnvBuilder, shard_count: int | None):
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.create_tenant(
        tenant_id,
        timeline_id,
        shard_count=shard_count,
        conf={
            # Small layers and low compaction thresholds, so that when we split we can expect some to
            # be dropped by child shards
            "checkpoint_distance": f"{1024 * 1024}",
            "compaction_threshold": "1",
            "compaction_target_size": f"{1024 * 1024}",
            # Disable automatic creation of image layers, as future image layers can result in layers in S3 that
            # aren't referenced by children, earlier than the test expects such layers to exist
            "image_creation_threshold": "9999",
            "image_layer_creation_check_threshold": "0",
            # Disable background compaction, we will do it explicitly
            "compaction_period": "0s",
            # No PITR, so that as soon as child shards generate an image layer, it covers ancestor deltas
            # and makes them GC'able
            "pitr_interval": "0s",
            "lsn_lease_length": "0s",
        },
    )

    # Create an extra timeline, to ensure the scrubber isn't confused by multiple timelines
    env.storage_controller.pageserver_api().timeline_create(
        env.pg_version, tenant_id=tenant_id, new_timeline_id=TimelineId.generate()
    )

    # Make sure the original shard has some layers
    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(100)

    # Issue a deletion queue flush so that the parent shard can't leave behind layers
    # that will look like unexpected garbage to the scrubber
    for pre_split_shard in env.storage_controller.locate(tenant_id):
        env.get_pageserver(pre_split_shard["node_id"]).http_client().deletion_queue_flush(
            execute=True
        )

    new_shard_count = 4
    assert shard_count is None or new_shard_count > shard_count
    shards = env.storage_controller.tenant_shard_split(tenant_id, shard_count=new_shard_count)
    env.storage_controller.reconcile_until_idle()  # Move shards to their final locations immediately

    # Create a timeline after split, to ensure scrubber can handle timelines that exist in child shards but not ancestors
    env.storage_controller.pageserver_api().timeline_create(
        env.pg_version, tenant_id=tenant_id, new_timeline_id=TimelineId.generate()
    )

    # Make sure child shards have some layers.  Do not force upload, because the test helper calls checkpoint, which
    # compacts, and we only want to do tha explicitly later in the test.
    workload.write_rows(100, upload=False)
    for shard in shards:
        ps = env.get_tenant_pageserver(shard)
        assert ps is not None
        log.info(f"Waiting for shard {shard} on pageserver {ps.id}")
        ps.http_client().timeline_checkpoint(
            shard, timeline_id, compact=False, wait_until_uploaded=True
        )

    # Flush deletion queue so that we don't leave any orphan layers in the parent that will confuse subsequent checks: once
    # a shard is split, any layers in its prefix that aren't referenced by a child will be considered GC'able, even
    # if they were logically deleted before the shard split, just not physically deleted yet because of the queue.
    for ps in env.pageservers:
        ps.http_client().deletion_queue_flush(execute=True)

    # Before compacting, all the layers in the ancestor should still be referenced by the children: the scrubber
    # should not erase any ancestor layers
    gc_summary = env.storage_scrubber.pageserver_physical_gc(min_age_secs=1, mode="full")
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0
    assert gc_summary["ancestor_layers_deleted"] == 0

    # Write some data and compact: compacting, some ancestor layers should no longer be needed by children
    # (the compaction is part of the checkpoint that Workload does for us)
    workload.churn_rows(100)
    workload.churn_rows(100)
    workload.churn_rows(100)
    for shard in shards:
        ps = env.get_tenant_pageserver(shard)
        assert ps is not None
        ps.http_client().timeline_compact(
            shard, timeline_id, force_image_layer_creation=True, wait_until_uploaded=True
        )
        ps.http_client().timeline_gc(shard, timeline_id, 0)

    # We will use a min_age_secs=1 threshold for deletion, let it pass
    time.sleep(2)

    # Our time threshold should be respected: check that with a high threshold we delete nothing
    gc_summary = env.storage_scrubber.pageserver_physical_gc(min_age_secs=3600, mode="full")
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0
    assert gc_summary["ancestor_layers_deleted"] == 0

    # Now run with a low time threshold: deletions of ancestor layers should be executed
    gc_summary = env.storage_scrubber.pageserver_physical_gc(min_age_secs=1, mode="full")
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0
    assert gc_summary["ancestor_layers_deleted"] > 0

    # We deleted some layers: now check we didn't corrupt the tenant by doing so. Detach and
    # attach it, to drop any local state, then check it's still readable.
    workload.stop()
    drop_local_state(env, tenant_id)
    workload.validate()


def test_scrubber_physical_gc_timeline_deletion(neon_env_builder: NeonEnvBuilder):
    """
    When we delete a timeline after a shard split, the child shards do not directly delete the
    layers in the ancestor shards.  They rely on the scrubber to clean up.
    """
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.create_tenant(
        tenant_id,
        timeline_id,
        shard_count=None,
        conf={
            # Small layers and low compaction thresholds, so that when we split we can expect some to
            # be dropped by child shards
            "checkpoint_distance": f"{1024 * 1024}",
            "compaction_threshold": "1",
            "compaction_target_size": f"{1024 * 1024}",
            "image_creation_threshold": "2",
            "image_layer_creation_check_threshold": "0",
            # Disable background compaction, we will do it explicitly
            "compaction_period": "0s",
            # No PITR, so that as soon as child shards generate an image layer, it covers ancestor deltas
            # and makes them GC'able
            "pitr_interval": "0s",
        },
    )

    # Make sure the original shard has some layers
    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(100, upload=False)
    workload.stop()

    # Issue a deletion queue flush so that the parent shard can't leave behind layers
    # that will look like unexpected garbage to the scrubber
    ps = env.get_tenant_pageserver(tenant_id)
    assert ps is not None
    ps.http_client().deletion_queue_flush(execute=True)

    new_shard_count = 4
    shards = env.storage_controller.tenant_shard_split(tenant_id, shard_count=new_shard_count)
    for shard in shards:
        ps = env.get_tenant_pageserver(shard)
        assert ps is not None
        log.info(f"Waiting for shard {shard} on pageserver {ps.id}")
        ps.http_client().timeline_checkpoint(
            shard, timeline_id, compact=False, wait_until_uploaded=True
        )

        ps.http_client().deletion_queue_flush(execute=True)

    # Create a second timeline so that when we delete the first one, child shards still have some content in S3.
    #
    # This is a limitation of the scrubber: if a shard isn't in S3 (because it has no timelines), then the scrubber
    # doesn't know about it, and won't perceive its ancestors as ancestors.
    other_timeline_id = TimelineId.generate()
    env.storage_controller.pageserver_api().timeline_create(
        PgVersion.NOT_SET, tenant_id, other_timeline_id
    )

    # The timeline still exists in child shards and they reference its layers, so scrubbing
    # now shouldn't delete anything.
    gc_summary = env.storage_scrubber.pageserver_physical_gc(min_age_secs=0, mode="full")
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0
    assert gc_summary["ancestor_layers_deleted"] == 0

    # Delete the timeline
    env.storage_controller.pageserver_api().timeline_delete(tenant_id, timeline_id)

    # Subsequently doing physical GC should clean up the ancestor layers
    gc_summary = env.storage_scrubber.pageserver_physical_gc(min_age_secs=0, mode="full")
    assert gc_summary["remote_storage_errors"] == 0
    assert gc_summary["indices_deleted"] == 0
    assert gc_summary["ancestor_layers_deleted"] > 0


def test_scrubber_physical_gc_ancestors_split(neon_env_builder: NeonEnvBuilder):
    """
    Exercise ancestor GC while a tenant is partly split: this test ensures that if we have some child shards
    which don't reference an ancestor, but some child shards that don't exist yet, then we do not incorrectly
    GC any ancestor layers.
    """
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    initial_shard_count = 2
    env.create_tenant(
        tenant_id,
        timeline_id,
        shard_count=initial_shard_count,
        conf={
            # Small layers and low compaction thresholds, so that when we split we can expect some to
            # be dropped by child shards
            "checkpoint_distance": f"{1024 * 1024}",
            "compaction_threshold": "1",
            "compaction_target_size": f"{1024 * 1024}",
            "image_creation_threshold": "2",
            "image_layer_creation_check_threshold": "0",
            # Disable background compaction, we will do it explicitly
            "compaction_period": "0s",
            # No PITR, so that as soon as child shards generate an image layer, it covers ancestor deltas
            # and makes them GC'able
            "pitr_interval": "0s",
        },
    )

    unstuck = threading.Event()

    def stuck_split():
        # Pause our shard split after the first shard but before the second, such that when we run
        # the scrub, the S3 bucket contains shards 0002, 0101, 0004, 0204 (but not 0104, 0304).
        env.storage_controller.configure_failpoints(
            ("shard-split-post-remote-sleep", "return(3600000)")
        )
        try:
            split_response = env.storage_controller.tenant_shard_split(tenant_id, shard_count=4)
        except Exception as e:
            log.info(f"Split failed with {e}")
        else:
            if not unstuck.is_set():
                raise RuntimeError(f"Split succeeded unexpectedly ({split_response})")

    with ThreadPoolExecutor(max_workers=1) as threads:
        log.info("Starting hung shard split")
        stuck_split_fut = threads.submit(stuck_split)

        # Let the controller reach the failpoint
        wait_until(
            lambda: env.storage_controller.assert_log_contains(
                'failpoint "shard-split-post-remote-sleep": sleeping'
            ),
        )

        # Run compaction on the new child shards, so that they drop some refs to their parent
        child_shards = [
            TenantShardId(tenant_id, 0, 4),
            TenantShardId(tenant_id, 2, 4),
        ]
        log.info("Compacting first two children")
        for child in child_shards:
            env.get_tenant_pageserver(
                TenantShardId(tenant_id, 0, initial_shard_count)
            ).http_client().timeline_compact(child, timeline_id)

        # Check that the other child shards weren't created
        assert env.get_tenant_pageserver(TenantShardId(tenant_id, 1, 4)) is None
        assert env.get_tenant_pageserver(TenantShardId(tenant_id, 3, 4)) is None

        # Run scrubber: it should not incorrectly interpret the **04 shards' lack of refs to all
        # ancestor layers as a reason to GC them, because it should realize that a split is in progress.
        # (GC requires that controller does not indicate split in progress, and that if we see the highest
        #  shard count N, then there are N shards present with that shard count).
        gc_output = env.storage_scrubber.pageserver_physical_gc(min_age_secs=0, mode="full")
        log.info(f"Ran physical GC partway through split: {gc_output}")
        assert gc_output["ancestor_layers_deleted"] == 0
        assert gc_output["remote_storage_errors"] == 0
        assert gc_output["controller_api_errors"] == 0

        # Storage controller shutdown lets our split request client complete
        log.info("Stopping storage controller")
        unstuck.set()
        env.storage_controller.allowed_errors.append(".*Timed out joining HTTP server task.*")
        env.storage_controller.stop()
        stuck_split_fut.result()

        # Restart the controller and retry the split with the failpoint disabled, this should
        # complete successfully and result in an S3 state that allows the scrubber to proceed with removing ancestor layers
        log.info("Starting & retrying split")
        env.storage_controller.start()
        env.storage_controller.tenant_shard_split(tenant_id, shard_count=4)

        # The other child shards exist now, we can compact them to drop refs to ancestor
        log.info("Compacting second two children")
        for child in [
            TenantShardId(tenant_id, 1, 4),
            TenantShardId(tenant_id, 3, 4),
        ]:
            env.get_tenant_pageserver(child).http_client().timeline_compact(child, timeline_id)

        gc_output = env.storage_scrubber.pageserver_physical_gc(min_age_secs=0, mode="full")
        log.info(f"Ran physical GC after split completed: {gc_output}")
        assert gc_output["ancestor_layers_deleted"] > 0
        assert gc_output["remote_storage_errors"] == 0
        assert gc_output["controller_api_errors"] == 0


@pytest.mark.parametrize("shard_count", [None, 4])
def test_scrubber_scan_pageserver_metadata(
    neon_env_builder: NeonEnvBuilder, shard_count: int | None
):
    """
    Create some layers. Delete an object listed in index. Run scrubber and see if it detects the defect.
    """

    # Use s3_storage so we could test out scrubber.
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.num_pageservers = shard_count if shard_count is not None else 1
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    # Create some layers.

    workload = Workload(env, env.initial_tenant, env.initial_timeline)
    workload.init()

    for _ in range(3):
        workload.write_rows(128)

    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()

    for _ in range(3):
        workload.write_rows(128)

    # Get the latest index for a particular timeline.

    tenant_shard_id = TenantShardId(env.initial_tenant, 0, shard_count if shard_count else 0)

    assert isinstance(env.pageserver_remote_storage, S3Storage)
    timeline_path = env.pageserver_remote_storage.timeline_path(
        tenant_shard_id, env.initial_timeline
    )

    client = env.pageserver_remote_storage.client
    bucket = env.pageserver_remote_storage.bucket_name
    objects = client.list_objects_v2(Bucket=bucket, Prefix=f"{timeline_path}/", Delimiter="").get(
        "Contents", []
    )
    keys = [obj["Key"] for obj in objects]
    index_keys = list(filter(lambda s: s.startswith(f"{timeline_path}/index_part"), keys))
    assert len(index_keys) > 0

    latest_index_key = env.pageserver_remote_storage.get_latest_index_key(index_keys)
    log.info(f"{latest_index_key=}")

    index = env.pageserver_remote_storage.download_index_part(latest_index_key)

    assert len(index.layer_metadata) > 0
    it = iter(index.layer_metadata.items())

    healthy, scan_summary = env.storage_scrubber.scan_metadata(post_to_storage_controller=True)
    assert healthy

    assert env.storage_controller.metadata_health_is_healthy()

    # Delete a layer file that is listed in the index.
    layer, metadata = next(it)
    log.info(f"Deleting {timeline_path}/{layer.to_str()}")
    delete_response = client.delete_object(
        Bucket=bucket,
        Key=f"{timeline_path}/{layer.to_str()}-{metadata.generation:08x}",
    )
    log.info(f"delete response: {delete_response}")

    # Check scan summary without posting to storage controller. Expect it to be a L0 layer so only emit warnings.
    _, scan_summary = env.storage_scrubber.scan_metadata()
    log.info(f"{pprint.pformat(scan_summary)}")
    assert len(scan_summary["with_warnings"]) > 0

    assert env.storage_controller.metadata_health_is_healthy()

    # Now post to storage controller, expect seeing one unhealthy health record
    _, scan_summary = env.storage_scrubber.scan_metadata(post_to_storage_controller=True)
    log.info(f"{pprint.pformat(scan_summary)}")
    assert len(scan_summary["with_warnings"]) > 0

    unhealthy = env.storage_controller.metadata_health_list_unhealthy()["unhealthy_tenant_shards"]
    assert len(unhealthy) == 1 and unhealthy[0] == str(tenant_shard_id)

    healthy, _ = env.storage_scrubber.scan_metadata()
    assert not healthy
    env.storage_scrubber.allowed_errors.append(".*not present in remote storage.*")
    healthy, _ = env.storage_scrubber.scan_metadata()
    assert healthy

    neon_env_builder.disable_scrub_on_exit()  # We already ran scrubber, no need to do an extra run
