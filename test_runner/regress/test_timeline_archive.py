from __future__ import annotations

import json
from typing import Optional

import pytest
from fixtures.common_types import TenantId, TimelineArchivalState, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    last_flush_lsn_upload,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import assert_prefix_empty, assert_prefix_not_empty, list_prefix
from fixtures.remote_storage import S3Storage, s3_storage
from fixtures.utils import wait_until
from mypy_boto3_s3.type_defs import (
    ObjectTypeDef,
)


@pytest.mark.parametrize("shard_count", [0, 4])
def test_timeline_archive(neon_env_builder: NeonEnvBuilder, shard_count: int):
    unsharded = shard_count == 0
    if unsharded:
        env = neon_env_builder.init_start()
        # If we run the unsharded version, talk to the pageserver directly
        ps_http = env.pageserver.http_client()
    else:
        neon_env_builder.num_pageservers = shard_count
        env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)
        # If we run the unsharded version, talk to the storage controller
        ps_http = env.storage_controller.pageserver_api()

    # first try to archive a non existing timeline for an existing tenant:
    invalid_timeline_id = TimelineId.generate()
    with pytest.raises(PageserverApiException, match="timeline not found") as exc:
        ps_http.timeline_archival_config(
            env.initial_tenant,
            invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # for a non existing tenant:
    invalid_tenant_id = TenantId.generate()
    with pytest.raises(
        PageserverApiException,
        match="NotFound: [tT]enant",
    ) as exc:
        ps_http.timeline_archival_config(
            invalid_tenant_id,
            invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # construct a pair of branches to validate that pageserver prohibits
    # archival of ancestor timelines when they have non-archived child branches
    parent_timeline_id = env.create_branch("test_ancestor_branch_archive_parent")

    leaf_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_branch1",
        ancestor_branch_name="test_ancestor_branch_archive_parent",
    )

    with pytest.raises(
        PageserverApiException,
        match="Cannot archive timeline which has non-archived child timelines",
    ) as exc:
        ps_http.timeline_archival_config(
            env.initial_tenant,
            parent_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 412

    leaf_detail = ps_http.timeline_detail(
        env.initial_tenant,
        timeline_id=leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is False

    # Test that archiving the leaf timeline and then the parent works
    ps_http.timeline_archival_config(
        env.initial_tenant,
        leaf_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        env.initial_tenant,
        leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    ps_http.timeline_archival_config(
        env.initial_tenant,
        parent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    # Test that the leaf can't be unarchived
    with pytest.raises(
        PageserverApiException,
        match="ancestor is archived",
    ) as exc:
        ps_http.timeline_archival_config(
            env.initial_tenant,
            leaf_timeline_id,
            state=TimelineArchivalState.UNARCHIVED,
        )

    # Unarchive works for the leaf if the parent gets unarchived first
    ps_http.timeline_archival_config(
        env.initial_tenant,
        parent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )

    ps_http.timeline_archival_config(
        env.initial_tenant,
        leaf_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )


@pytest.mark.parametrize("manual_offload", [False, True])
def test_timeline_offloading(neon_env_builder: NeonEnvBuilder, manual_offload: bool):
    if not manual_offload:
        # (automatic) timeline offloading defaults to false for now
        neon_env_builder.pageserver_config_override = "timeline_offloading = true"

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    # Turn off gc and compaction loops: we want to issue them manually for better reliability
    tenant_id, initial_timeline_id = env.create_tenant(
        conf={
            "gc_period": "0s",
            "compaction_period": "0s" if manual_offload else "1s",
        }
    )

    # Create three branches that depend on each other, starting with two
    grandparent_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_grandparent", tenant_id
    )
    parent_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_parent", tenant_id, "test_ancestor_branch_archive_grandparent"
    )

    # write some stuff to the parent
    with env.endpoints.create_start(
        "test_ancestor_branch_archive_parent", tenant_id=tenant_id
    ) as endpoint:
        endpoint.safe_psql_many(
            [
                "CREATE TABLE foo(key serial primary key, t text default 'data_content')",
                "INSERT INTO foo SELECT FROM generate_series(1,1000)",
            ]
        )
        sum = endpoint.safe_psql("SELECT sum(key) from foo where key > 50")

    # create the third branch
    leaf_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_branch1", tenant_id, "test_ancestor_branch_archive_parent"
    )

    ps_http.timeline_archival_config(
        tenant_id,
        leaf_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        tenant_id,
        leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    ps_http.timeline_archival_config(
        tenant_id,
        parent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    ps_http.timeline_archival_config(
        tenant_id,
        grandparent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    def timeline_offloaded_logged(timeline_id: TimelineId) -> bool:
        return (
            env.pageserver.log_contains(f".*{timeline_id}.* offloading archived timeline.*")
            is not None
        )

    if manual_offload:
        with pytest.raises(
            PageserverApiException,
            match="timeline has attached children",
        ):
            # This only tests the (made for testing only) http handler,
            # but still demonstrates the constraints we have.
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=parent_timeline_id)

    def parent_offloaded():
        if manual_offload:
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=parent_timeline_id)
        assert timeline_offloaded_logged(parent_timeline_id)

    def leaf_offloaded():
        if manual_offload:
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=leaf_timeline_id)
        assert timeline_offloaded_logged(leaf_timeline_id)

    wait_until(30, 1, leaf_offloaded)
    wait_until(30, 1, parent_offloaded)

    # Offloaded child timelines should still prevent deletion
    with pytest.raises(
        PageserverApiException,
        match=f".* timeline which has child timelines: \\[{leaf_timeline_id}\\]",
    ):
        ps_http.timeline_delete(tenant_id, parent_timeline_id)

    ps_http.timeline_archival_config(
        tenant_id,
        grandparent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )
    ps_http.timeline_archival_config(
        tenant_id,
        parent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )
    parent_detail = ps_http.timeline_detail(
        tenant_id,
        parent_timeline_id,
    )
    assert parent_detail["is_archived"] is False

    with env.endpoints.create_start(
        "test_ancestor_branch_archive_parent", tenant_id=tenant_id
    ) as endpoint:
        sum_again = endpoint.safe_psql("SELECT sum(key) from foo where key > 50")
        assert sum == sum_again

    # Test that deletion of offloaded timelines works
    ps_http.timeline_delete(tenant_id, leaf_timeline_id)

    assert not timeline_offloaded_logged(initial_timeline_id)


@pytest.mark.parametrize("delete_timeline", [False, True])
def test_timeline_offload_persist(neon_env_builder: NeonEnvBuilder, delete_timeline: bool):
    """
    Test for persistence of timeline offload state
    """
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    # Turn off gc and compaction loops: we want to issue them manually for better reliability
    tenant_id, root_timeline_id = env.create_tenant(
        conf={
            "gc_period": "0s",
            "compaction_period": "0s",
            "checkpoint_distance": f"{1024 ** 2}",
        }
    )

    # Create a branch and archive it
    child_timeline_id = env.create_branch("test_archived_branch_persisted", tenant_id)

    with env.endpoints.create_start(
        "test_archived_branch_persisted", tenant_id=tenant_id
    ) as endpoint:
        endpoint.safe_psql_many(
            [
                "CREATE TABLE foo(key serial primary key, t text default 'data_content')",
                "INSERT INTO foo SELECT FROM generate_series(1,2048)",
            ]
        )
        sum = endpoint.safe_psql("SELECT sum(key) from foo where key < 500")
        last_flush_lsn_upload(env, endpoint, tenant_id, child_timeline_id)

    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=f"tenants/{str(tenant_id)}/",
    )
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=f"tenants/{str(tenant_id)}/tenant-manifest",
    )

    ps_http.timeline_archival_config(
        tenant_id,
        child_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        tenant_id,
        child_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    def timeline_offloaded_api(timeline_id: TimelineId) -> bool:
        # TODO add a proper API to check if a timeline has been offloaded or not
        return not any(
            timeline["timeline_id"] == str(timeline_id)
            for timeline in ps_http.timeline_list(tenant_id=tenant_id)
        )

    def child_offloaded():
        ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=child_timeline_id)
        assert timeline_offloaded_api(child_timeline_id)

    wait_until(30, 1, child_offloaded)

    assert timeline_offloaded_api(child_timeline_id)
    assert not timeline_offloaded_api(root_timeline_id)

    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=f"tenants/{str(tenant_id)}/tenant-manifest",
    )

    # Test persistence, is the timeline still offloaded?
    env.pageserver.stop()
    env.pageserver.start()

    assert timeline_offloaded_api(child_timeline_id)
    assert not timeline_offloaded_api(root_timeline_id)

    if delete_timeline:
        ps_http.timeline_delete(tenant_id, child_timeline_id)
        with pytest.raises(PageserverApiException, match="not found"):
            ps_http.timeline_detail(
                tenant_id,
                child_timeline_id,
            )
    else:
        ps_http.timeline_archival_config(
            tenant_id,
            child_timeline_id,
            state=TimelineArchivalState.UNARCHIVED,
        )
        child_detail = ps_http.timeline_detail(
            tenant_id,
            child_timeline_id,
        )
        assert child_detail["is_archived"] is False

        with env.endpoints.create_start(
            "test_archived_branch_persisted", tenant_id=tenant_id
        ) as endpoint:
            sum_again = endpoint.safe_psql("SELECT sum(key) from foo where key < 500")
            assert sum == sum_again

        assert_prefix_empty(
            neon_env_builder.pageserver_remote_storage,
            prefix=f"tenants/{str(env.initial_tenant)}/tenant-manifest",
        )

    assert not timeline_offloaded_api(root_timeline_id)

    ps_http.tenant_delete(tenant_id)

    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=f"tenants/{str(tenant_id)}/",
    )


@pytest.mark.parametrize("offload_child", ["offload", "offload-corrupt", "archive", None])
def test_timeline_retain_lsn(neon_env_builder: NeonEnvBuilder, offload_child: Optional[str]):
    """
    Ensure that retain_lsn functionality for timelines works, both for offloaded and non-offloaded ones
    """
    if offload_child == "offload-corrupt":
        # Our corruption code only works with S3 compatible storage
        neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    # Turn off gc and compaction loops: we want to issue them manually for better reliability
    tenant_id, root_timeline_id = env.create_tenant(
        conf={
            # small checkpointing and compaction targets to ensure we generate many upload operations
            "checkpoint_distance": 128 * 1024,
            "compaction_threshold": 1,
            "compaction_target_size": 128 * 1024,
            # set small image creation thresholds so that gc deletes data
            "image_creation_threshold": 2,
            # disable background compaction and GC. We invoke it manually when we want it to happen.
            "gc_period": "0s",
            "compaction_period": "0s",
            # Disable pitr, we only want the latest lsn
            "pitr_interval": "0s",
            # Don't rely on endpoint lsn leases
            "lsn_lease_length": "0s",
        }
    )

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        endpoint.safe_psql_many(
            [
                "CREATE TABLE foo(v int, key serial primary key, t text default 'data_content')",
                "SELECT setseed(0.4321)",
                "INSERT INTO foo SELECT v FROM (SELECT generate_series(1,2048), (random() * 409600)::int as v) as random_numbers",
            ]
        )
        pre_branch_sum = endpoint.safe_psql("SELECT sum(key) from foo where v < 51200")
        log.info(f"Pre branch sum: {pre_branch_sum}")
        last_flush_lsn_upload(env, endpoint, tenant_id, root_timeline_id)

    # Create a branch and write some additional data to the parent
    child_timeline_id = env.create_branch("test_archived_branch", tenant_id)

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        # Do some churn of the data. This is important so that we can overwrite image layers.
        for i in range(10):
            endpoint.safe_psql_many(
                [
                    f"SELECT setseed(0.23{i})",
                    "UPDATE foo SET v=(random() * 409600)::int WHERE v % 3 = 2",
                    "UPDATE foo SET v=(random() * 409600)::int WHERE v % 3 = 1",
                    "UPDATE foo SET v=(random() * 409600)::int WHERE v % 3 = 0",
                ]
            )
        post_branch_sum = endpoint.safe_psql("SELECT sum(key) from foo where v < 51200")
        log.info(f"Post branch sum: {post_branch_sum}")
        last_flush_lsn_upload(env, endpoint, tenant_id, root_timeline_id)

    if offload_child is not None:
        ps_http.timeline_archival_config(
            tenant_id,
            child_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )
        leaf_detail = ps_http.timeline_detail(
            tenant_id,
            child_timeline_id,
        )
        assert leaf_detail["is_archived"] is True
        if "offload" in offload_child:
            ps_http.timeline_offload(tenant_id, child_timeline_id)

    # Do a restart to get rid of any in-memory objects (we only init gc info once, at attach)
    env.pageserver.stop()
    if offload_child == "offload-corrupt":
        assert isinstance(env.pageserver_remote_storage, S3Storage)
        listing = list_prefix(
            env.pageserver_remote_storage, f"tenants/{str(tenant_id)}/tenant-manifest"
        )
        objects: list[ObjectTypeDef] = listing.get("Contents", [])
        assert len(objects) > 0
        remote_key: str = str(objects[0].get("Key", []))
        local_path = str(env.repo_dir / "tenant-manifest.json")

        log.info(f"Downloading {remote_key} -> {local_path}")
        env.pageserver_remote_storage.client.download_file(
            env.pageserver_remote_storage.bucket_name, remote_key, local_path
        )

        log.info(f"Corrupting {local_path}")
        with open(local_path) as manifest_json_file:
            manifest_json = json.load(manifest_json_file)
        for offloaded_timeline in manifest_json["offloaded_timelines"]:
            offloaded_timeline["ancestor_retain_lsn"] = None
        with open(local_path, "w") as manifest_json_file:
            json.dump(manifest_json, manifest_json_file)

        log.info(f"Uploading {local_path} -> {remote_key}")
        env.pageserver_remote_storage.client.upload_file(
            local_path, env.pageserver_remote_storage.bucket_name, remote_key
        )
        # The point of our earlier efforts was to provoke these
        env.pageserver.allowed_errors.extend(
            [
                ".*initial size calculation failed: PageRead.MissingKey.could not find data for key.*",
                ".*page_service_conn_main.*could not find data for key.*",
            ]
        )
    env.pageserver.start()

    # Do an agressive gc and compaction of the parent branch
    ps_http.timeline_gc(tenant_id=tenant_id, timeline_id=root_timeline_id, gc_horizon=0)
    ps_http.timeline_checkpoint(
        tenant_id,
        root_timeline_id,
        force_l0_compaction=True,
        force_repartition=True,
        wait_until_uploaded=True,
        compact=True,
    )

    if offload_child is not None:
        ps_http.timeline_archival_config(
            tenant_id,
            child_timeline_id,
            state=TimelineArchivalState.UNARCHIVED,
        )

    # Now, after unarchival, the child timeline should still have its data accessible (or corrupted)
    if offload_child == "offload-corrupt":
        with pytest.raises(RuntimeError, match=".*failed to get basebackup.*"):
            env.endpoints.create_start(
                "test_archived_branch", tenant_id=tenant_id, basebackup_request_tries=1
            )
    else:
        with env.endpoints.create_start("test_archived_branch", tenant_id=tenant_id) as endpoint:
            sum = endpoint.safe_psql("SELECT sum(key) from foo where v < 51200")
            assert sum == pre_branch_sum
