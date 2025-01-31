from __future__ import annotations

import os
import queue
import shutil
import threading
import time

import pytest
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.pageserver.utils import (
    timeline_delete_wait_completed,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_until_tenant_active,
    wait_until_tenant_state,
)
from fixtures.remote_storage import (
    LocalFsStorage,
    RemoteStorageKind,
    available_remote_storages,
)
from fixtures.utils import (
    assert_eq,
    assert_ge,
    assert_gt,
    print_gc_result,
    query_scalar,
    wait_until,
)
from requests import ReadTimeout


#
# Tests that a piece of data is backed up and restored correctly:
#
# 1. Initial pageserver
#   * starts a pageserver with remote storage, stores specific data in its tables
#   * triggers a checkpoint (which produces a local data scheduled for backup), gets the corresponding timeline id
#   * polls the timeline status to ensure it's copied remotely
#   * inserts more data in the pageserver and repeats the process, to check multiple checkpoints case
#   * stops the pageserver, clears all local directories
#
# 2. Second pageserver
#   * starts another pageserver, connected to the same remote storage
#   * timeline_attach is called for the same timeline id
#   * timeline status is polled until it's downloaded
#   * queries the specific data, ensuring that it matches the one stored before
#
# The tests are done for all types of remote storage pageserver supports.
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
@pytest.mark.parametrize("generations", [True, False])
def test_remote_storage_backup_and_restore(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind, generations: bool
):
    # Use this test to check more realistic SK ids: some etcd key parsing bugs were related,
    # and this test needs SK to write data to pageserver, so it will be visible
    neon_env_builder.safekeepers_id_start = 12

    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    # Exercise retry code path by making all uploads and downloads fail for the
    # first time. The retries print INFO-messages to the log; we will check
    # that they are present after the test.
    neon_env_builder.pageserver_config_override = "test_remote_failures=1"

    data_id = 1
    data = "just some data"

    ##### First start, insert data and upload it to the remote storage
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(
        [
            ".*Failed to get local tenant state.*",
            # FIXME retry downloads without throwing errors
            ".*failed to load remote timeline.*",
            # we have a bunch of pytest.raises for these below
            ".*tenant .*? already exists, state:.*",
            ".*tenant directory already exists.*",
            ".*simulated failure of remote operation.*",
        ]
    )

    pageserver_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Thats because of UnreliableWrapper's injected failures
    env.pageserver.allowed_errors.append(
        f".*failed to fetch tenant deletion mark at tenants/{tenant_id}/deleted attempt 1.*"
    )

    checkpoint_numbers = range(1, 3)

    for checkpoint_number in checkpoint_numbers:
        with endpoint.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE t{checkpoint_number}(id int primary key, data text);
                INSERT INTO t{checkpoint_number} VALUES ({data_id}, '{data}|{checkpoint_number}');
            """
            )
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

        # wait until pageserver successfully uploaded a checkpoint to remote storage
        log.info(f"waiting for checkpoint {checkpoint_number} upload")
        wait_for_upload(client, tenant_id, timeline_id, current_lsn)
        log.info(f"upload of checkpoint {checkpoint_number} is done")

    # Check that we had to retry the uploads
    env.pageserver.assert_log_contains(
        ".*failed to perform remote task UploadLayer.*, will retry.*"
    )
    env.pageserver.assert_log_contains(
        ".*failed to perform remote task UploadMetadata.*, will retry.*"
    )

    ##### Stop the first pageserver instance, erase all its data
    env.endpoints.stop_all()
    env.pageserver.stop()

    dir_to_clear = env.pageserver.tenant_dir()
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    # Introduce failpoint in list remote timelines code path to make tenant_attach fail.
    # This is before the failures injected by test_remote_failures, so it's a permanent error.
    pageserver_http.configure_failpoints(("storage-sync-list-remote-timelines", "return"))
    env.pageserver.allowed_errors.extend(
        [
            ".*attach failed.*: storage-sync-list-remote-timelines",
            ".*Tenant state is Broken: storage-sync-list-remote-timelines.*",
        ]
    )
    # Attach it. This HTTP request will succeed and launch a
    # background task to load the tenant. In that background task,
    # listing the remote timelines will fail because of the failpoint,
    # and the tenant will be marked as Broken.
    env.pageserver.tenant_attach(tenant_id)

    tenant_info = wait_until_tenant_state(pageserver_http, tenant_id, "Broken", 15)
    assert tenant_info["attachment_status"] == {
        "slug": "failed",
        "data": {"reason": "storage-sync-list-remote-timelines"},
    }

    # Even though the tenant is broken, subsequent calls to location_conf API will succeed, but
    # the tenant will always end up in a broken state as a result of the failpoint.
    # Ensure that even though the tenant is broken, retrying the attachment fails
    tenant_info = wait_until_tenant_state(pageserver_http, tenant_id, "Broken", 15)
    gen_state = env.storage_controller.inspect(tenant_id)
    assert gen_state is not None
    generation = gen_state[0]
    env.pageserver.tenant_attach(tenant_id, generation=generation)

    # Restart again, this implicitly clears the failpoint.
    # test_remote_failures=1 remains active, though, as it's in the pageserver config.
    # This means that any of the remote client operations after restart will exercise the
    # retry code path.
    #
    # The initiated attach operation should survive the restart, and continue from where it was.
    env.pageserver.stop()
    layer_download_failed_regex = r"Failed to download a remote file: simulated failure of remote operation Download.*[0-9A-F]+-[0-9A-F]+"
    assert not env.pageserver.log_contains(
        layer_download_failed_regex
    ), "we shouldn't have tried any layer downloads yet since list remote timelines has a failpoint"
    env.pageserver.start()

    # The attach should have got far enough that it recovers on restart (i.e. tenant's
    # config was written to local storage).
    log.info("waiting for tenant to become active. this should be quick with on-demand download")

    wait_until_tenant_active(
        pageserver_http=client,
        tenant_id=tenant_id,
        iterations=10,  # make it longer for real_s3 tests when unreliable wrapper is involved
    )

    detail = client.timeline_detail(tenant_id, timeline_id)
    log.info("Timeline detail after attach completed: %s", detail)
    assert (
        Lsn(detail["last_record_lsn"]) >= current_lsn
    ), "current db Lsn should should not be less than the one stored on remote storage"

    log.info("select some data, this will cause layers to be downloaded")
    endpoint = env.endpoints.create_start("main")
    with endpoint.cursor() as cur:
        for checkpoint_number in checkpoint_numbers:
            assert (
                query_scalar(cur, f"SELECT data FROM t{checkpoint_number} WHERE id = {data_id};")
                == f"{data}|{checkpoint_number}"
            )

    log.info("ensure that we needed to retry downloads due to test_remote_failures=1")
    assert env.pageserver.log_contains(layer_download_failed_regex)


# Exercises the upload queue retry code paths.
# - Use failpoints to cause all storage ops to fail
# - Churn on database to create layer & index uploads, and layer deletions
# - Check that these operations are queued up, using the appropriate metrics
# - Disable failpoints
# - Wait for all uploads to finish
# - Verify that remote is consistent and up-to-date (=all retries were done and succeeded)
def test_remote_storage_upload_queue_retries(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()

    # create tenant with config that will determinstically allow
    # compaction and gc
    tenant_id, timeline_id = env.create_tenant(
        conf={
            # small checkpointing and compaction targets to ensure we generate many upload operations
            "checkpoint_distance": f"{64 * 1024}",
            "compaction_threshold": "1",
            "compaction_target_size": f"{64 * 1024}",
            # no PITR horizon, we specify the horizon when we request on-demand GC
            "pitr_interval": "0s",
            # disable background compaction and GC. We invoke it manually when we want it to happen.
            "gc_period": "0s",
            "compaction_period": "0s",
            # create image layers eagerly, so that GC can remove some layers
            "image_creation_threshold": "1",
            "image_layer_creation_check_threshold": "0",
            "lsn_lease_length": "0s",
        }
    )

    client = env.pageserver.http_client()

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    endpoint.safe_psql("CREATE TABLE foo (id INTEGER PRIMARY KEY, val text)")

    def configure_storage_sync_failpoints(action):
        client.configure_failpoints(
            [
                ("before-upload-layer", action),
                ("before-upload-index", action),
                ("before-delete-layer", action),
            ]
        )

    FOO_ROWS_COUNT = 4000

    def overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data):
        # create initial set of layers & upload them with failpoints configured
        for _v in range(2):
            endpoint.safe_psql_many(
                [
                    f"""
                    INSERT INTO foo (id, val)
                    SELECT g, '{data}'
                    FROM generate_series(1, {FOO_ROWS_COUNT}) g
                    ON CONFLICT (id) DO UPDATE
                    SET val = EXCLUDED.val
                    """,
                    # to ensure that GC can actually remove some layers
                    "VACUUM foo",
                ]
            )
        wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    def get_queued_count(file_kind, op_kind):
        return client.get_remote_timeline_client_queue_count(
            tenant_id, timeline_id, file_kind, op_kind
        )

    # create some layers & wait for uploads to finish
    overwrite_data_and_wait_for_it_to_arrive_at_pageserver("a")
    client.timeline_checkpoint(tenant_id, timeline_id)
    client.timeline_compact(tenant_id, timeline_id)
    overwrite_data_and_wait_for_it_to_arrive_at_pageserver("b")
    client.timeline_checkpoint(tenant_id, timeline_id)
    client.timeline_compact(tenant_id, timeline_id)
    gc_result = client.timeline_gc(tenant_id, timeline_id, 0)
    print_gc_result(gc_result)
    assert gc_result["layers_removed"] > 0

    wait_until(lambda: assert_eq(get_queued_count(file_kind="layer", op_kind="upload"), 0))
    wait_until(lambda: assert_eq(get_queued_count(file_kind="index", op_kind="upload"), 0))
    wait_until(lambda: assert_eq(get_queued_count(file_kind="layer", op_kind="delete"), 0))

    # let all future operations queue up
    configure_storage_sync_failpoints("return")

    # Create more churn to generate all upload ops.
    # The checkpoint / compact / gc ops will block because they call remote_client.wait_completion().
    # So, run this in a different thread.
    churn_thread_result = [False]

    def churn_while_failpoints_active(result):
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver("c")
        # this call will wait for the failpoints to be turned off
        client.timeline_checkpoint(tenant_id, timeline_id)
        client.timeline_compact(tenant_id, timeline_id)
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver("d")
        client.timeline_checkpoint(tenant_id, timeline_id)
        client.timeline_compact(tenant_id, timeline_id)
        gc_result = client.timeline_gc(tenant_id, timeline_id, 0)
        print_gc_result(gc_result)
        assert gc_result["layers_removed"] > 0
        result[0] = True

    churn_while_failpoints_active_thread = threading.Thread(
        target=churn_while_failpoints_active, args=[churn_thread_result]
    )
    churn_while_failpoints_active_thread.start()

    # wait for churn thread's data to get stuck in the upload queue
    # Exponential back-off in upload queue, so, gracious timeouts.

    wait_until(
        lambda: assert_gt(get_queued_count(file_kind="layer", op_kind="upload"), 0), timeout=30
    )
    wait_until(
        lambda: assert_ge(get_queued_count(file_kind="index", op_kind="upload"), 1), timeout=30
    )
    wait_until(
        lambda: assert_eq(get_queued_count(file_kind="layer", op_kind="delete"), 0), timeout=30
    )

    # unblock churn operations
    configure_storage_sync_failpoints("off")

    wait_until(
        lambda: assert_eq(get_queued_count(file_kind="layer", op_kind="upload"), 0), timeout=30
    )
    wait_until(
        lambda: assert_eq(get_queued_count(file_kind="index", op_kind="upload"), 0), timeout=30
    )
    wait_until(
        lambda: assert_eq(get_queued_count(file_kind="layer", op_kind="delete"), 0), timeout=30
    )

    # The churn thread doesn't make progress once it blocks on the first wait_completion() call,
    # so, give it some time to wrap up.
    churn_while_failpoints_active_thread.join(60)
    assert not churn_while_failpoints_active_thread.is_alive()
    assert churn_thread_result[0]

    # try a restore to verify that the uploads worked
    # XXX: should vary this test to selectively fail just layer uploads, index uploads, deletions
    #      but how do we validate the result after restore?

    env.pageserver.stop(immediate=True)
    env.endpoints.stop_all()

    dir_to_clear = env.pageserver.tenant_dir()
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    env.pageserver.start()
    client = env.pageserver.http_client()

    env.pageserver.tenant_attach(tenant_id)

    wait_until_tenant_active(client, tenant_id)

    log.info("restarting postgres to validate")
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    with endpoint.cursor() as cur:
        assert query_scalar(cur, "SELECT COUNT(*) FROM foo WHERE val = 'd'") == FOO_ROWS_COUNT


def test_remote_timeline_client_calls_started_metric(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    # thinking about using a shared environment? the test assumes that global
    # metrics are for single tenant.
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # small checkpointing and compaction targets to ensure we generate many upload operations
            "checkpoint_distance": f"{128 * 1024}",
            # ensure each timeline_checkpoint() calls creates L1s
            "compaction_threshold": "1",
            "compaction_target_size": f"{128 * 1024}",
            # no PITR horizon, we specify the horizon when we request on-demand GC
            "pitr_interval": "0s",
            # disable background compaction and GC. We invoke it manually when we want it to happen.
            "gc_period": "0s",
            "compaction_period": "0s",
            "lsn_lease_length": "0s",
        }
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    client = env.pageserver.http_client()

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    endpoint.safe_psql("CREATE TABLE foo (id INTEGER PRIMARY KEY, val text)")

    def overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data):
        # create initial set of layers & upload them with failpoints configured
        endpoint.safe_psql_many(
            [
                f"""
               INSERT INTO foo (id, val)
               SELECT g, '{data}'
               FROM generate_series(1, 20000) g
               ON CONFLICT (id) DO UPDATE
               SET val = EXCLUDED.val
               """,
                # to ensure that GC can actually remove some layers
                "VACUUM foo",
            ]
        )
        assert timeline_id is not None
        wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    calls_started: dict[tuple[str, str], list[int]] = {
        ("layer", "upload"): [0],
        ("index", "upload"): [0],
        ("layer", "delete"): [0],
    }

    def fetch_calls_started():
        assert timeline_id is not None
        for (file_kind, op_kind), observations in calls_started.items():
            val = client.get_metric_value(
                name="pageserver_remote_timeline_client_calls_started_total",
                filter={
                    "file_kind": str(file_kind),
                    "op_kind": str(op_kind),
                },
            )
            assert val is not None, f"expecting metric to be present: {file_kind} {op_kind}"
            val = int(val)
            observations.append(val)

    def ensure_calls_started_grew():
        for (file_kind, op_kind), observations in calls_started.items():
            log.info(f"ensure_calls_started_grew: {file_kind} {op_kind}: {observations}")
            assert all(
                x < y for x, y in zip(observations, observations[1:], strict=False)
            ), f"observations for {file_kind} {op_kind} did not grow monotonically: {observations}"

    def churn(data_pass1, data_pass2):
        # overwrite the same data in place, vacuum inbetween, and
        # and create image layers; then run a gc().
        # this should
        # - create new layers
        # - delete some layers
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data_pass1)
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data_pass2)
        client.timeline_checkpoint(tenant_id, timeline_id, force_image_layer_creation=True)
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data_pass1)
        overwrite_data_and_wait_for_it_to_arrive_at_pageserver(data_pass2)
        client.timeline_checkpoint(tenant_id, timeline_id, force_image_layer_creation=True)
        gc_result = client.timeline_gc(tenant_id, timeline_id, 0)
        print_gc_result(gc_result)
        assert gc_result["layers_removed"] > 0

    # create some layers & wait for uploads to finish
    churn("a", "b")

    wait_upload_queue_empty(client, tenant_id, timeline_id)

    # ensure that we updated the calls_started metric
    fetch_calls_started()
    ensure_calls_started_grew()

    # more churn to cause more operations
    churn("c", "d")

    # ensure that the calls_started metric continued to be updated
    fetch_calls_started()
    ensure_calls_started_grew()

    ### now we exercise the download path
    calls_started.clear()
    calls_started.update(
        {
            ("index", "download"): [0],
            ("layer", "download"): [0],
        }
    )

    env.pageserver.stop(immediate=True)
    env.endpoints.stop_all()

    dir_to_clear = env.pageserver.tenant_dir()
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    env.pageserver.start()
    client = env.pageserver.http_client()

    env.pageserver.tenant_attach(tenant_id)

    wait_until_tenant_active(client, tenant_id)

    log.info("restarting postgres to validate")
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    with endpoint.cursor() as cur:
        assert query_scalar(cur, "SELECT COUNT(*) FROM foo WHERE val = 'd'") == 20000

    # ensure that we updated the calls_started download metric
    fetch_calls_started()
    ensure_calls_started_grew()


# Test that we correctly handle timeline with layers stuck in upload queue
def test_timeline_deletion_with_files_stuck_in_upload_queue(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # small checkpointing and compaction targets to ensure we generate many operations
            "checkpoint_distance": f"{64 * 1024}",
            "compaction_threshold": "1",
            "l0_flush_delay_threshold": "0",
            "l0_flush_stall_threshold": "0",
            "compaction_target_size": f"{64 * 1024}",
            # large horizon to avoid automatic GC (our assert on gc_result below relies on that)
            "gc_horizon": f"{1024 ** 4}",
            "gc_period": "1h",
            # disable PITR so that GC considers just gc_horizon
            "pitr_interval": "0s",
        }
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    timeline_path = env.pageserver.timeline_dir(tenant_id, timeline_id)

    client = env.pageserver.http_client()

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    client.configure_failpoints(("before-upload-layer", "return"))

    endpoint.safe_psql_many(
        [
            "CREATE TABLE foo (x INTEGER)",
            "INSERT INTO foo SELECT g FROM generate_series(1, 10000) g",
        ]
    )
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    # Kick off a checkpoint operation.
    # It will get stuck in remote_client.wait_completion(), since the select query will have
    # generated layer upload ops already.
    checkpoint_allowed_to_fail = threading.Event()

    def checkpoint_thread_fn():
        try:
            client.timeline_checkpoint(tenant_id, timeline_id)
        except PageserverApiException:
            assert (
                checkpoint_allowed_to_fail.is_set()
            ), "checkpoint op should only fail in response to timeline deletion"

    checkpoint_thread = threading.Thread(target=checkpoint_thread_fn)
    checkpoint_thread.start()

    # Wait for stuck uploads. NB: if there were earlier layer flushes initiated during `INSERT INTO`,
    # this will be their uploads. If there were none, it's the timeline_checkpoint()'s uploads.
    def assert_compacted_and_uploads_queued():
        assert timeline_path.exists()
        assert len(list(timeline_path.glob("*"))) >= 8
        assert (
            get_queued_count(client, tenant_id, timeline_id, file_kind="index", op_kind="upload")
            > 0
        )

    wait_until(assert_compacted_and_uploads_queued)

    # Regardless, give checkpoint some time to block for good.
    # Not strictly necessary, but might help uncover failure modes in the future.
    time.sleep(2)

    # Now delete the timeline. It should take priority over ongoing
    # checkpoint operations. Hence, checkpoint is allowed to fail now.
    log.info("sending delete request")
    checkpoint_allowed_to_fail.set()
    env.pageserver.allowed_errors.extend(
        [
            ".* ERROR .*Error processing HTTP request: InternalServerError\\(The timeline or pageserver is shutting down",
            ".* ERROR .*queue is in state Stopped.*",
            ".* ERROR .*[Cc]ould not flush frozen layer.*",
        ]
    )

    timeline_delete_wait_completed(client, tenant_id, timeline_id)

    assert not timeline_path.exists()

    # to please mypy
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)
    remote_timeline_path = env.pageserver_remote_storage.timeline_path(tenant_id, timeline_id)

    filtered = [
        path
        for path in remote_timeline_path.iterdir()
        if not (path.name.endswith("initdb.tar.zst"))
    ]
    assert len(filtered) == 0

    # timeline deletion should kill ongoing uploads, so, the metric will be gone
    assert (
        get_queued_count(client, tenant_id, timeline_id, file_kind="index", op_kind="upload")
        is None
    )

    # timeline deletion should be unblocking checkpoint ops
    checkpoint_thread.join(20.0)
    assert not checkpoint_thread.is_alive()

    # Just to be sure, unblock ongoing uploads. If the previous assert was incorrect, or the prometheus metric broken,
    # this would likely generate some ERROR level log entries that the NeonEnvBuilder would detect
    client.configure_failpoints(("before-upload-layer", "off"))
    # XXX force retry, currently we have to wait for exponential backoff
    time.sleep(10)


# Branches off a root branch, but does not write anything to the new branch, so it has a metadata file only.
# Ensures that such branch is still persisted on the remote storage, and can be restored during tenant (re)attach.
def test_empty_branch_remote_storage_upload(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    new_branch_name = "new_branch"
    new_branch_timeline_id = env.create_branch(
        new_branch_name, ancestor_branch_name="main", tenant_id=env.initial_tenant
    )
    assert_nothing_to_upload(client, env.initial_tenant, new_branch_timeline_id)

    timelines_before_detach = set(
        map(
            lambda t: TimelineId(t["timeline_id"]),
            client.timeline_list(env.initial_tenant),
        )
    )
    expected_timelines = set([env.initial_timeline, new_branch_timeline_id])
    assert (
        timelines_before_detach == expected_timelines
    ), f"Expected to have an initial timeline and the branch timeline only, but got {timelines_before_detach}"

    client.tenant_detach(env.initial_tenant)
    env.pageserver.tenant_attach(env.initial_tenant)
    wait_until_tenant_state(client, env.initial_tenant, "Active", 5)

    timelines_after_detach = set(
        map(
            lambda t: TimelineId(t["timeline_id"]),
            client.timeline_list(env.initial_tenant),
        )
    )

    assert (
        timelines_before_detach == timelines_after_detach
    ), f"Expected to have same timelines after reattach, but got {timelines_after_detach}"


def test_empty_branch_remote_storage_upload_on_restart(neon_env_builder: NeonEnvBuilder):
    """
    Branches off a root branch, but does not write anything to the new branch, so
    it has a metadata file only.

    Ensures the branch is not on the remote storage and restarts the pageserver
    — the upload should be scheduled by load, and create_timeline should await
    for it even though it gets 409 Conflict.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    client.configure_failpoints(("before-upload-index", "return"))

    new_branch_timeline_id = TimelineId.generate()

    with pytest.raises(ReadTimeout):
        client.timeline_create(
            tenant_id=env.initial_tenant,
            ancestor_timeline_id=env.initial_timeline,
            new_timeline_id=new_branch_timeline_id,
            pg_version=env.pg_version,
            timeout=4,
        )

    env.pageserver.allowed_errors.append(
        f".*POST.* path=/v1/tenant/{env.initial_tenant}/timeline.* request was dropped before completing"
    )

    # index upload is now hitting the failpoint, it should block the shutdown
    env.pageserver.stop(immediate=True)

    timeline_dir = env.pageserver.timeline_dir(env.initial_tenant, new_branch_timeline_id)
    assert timeline_dir.is_dir()

    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    new_branch_on_remote_storage = env.pageserver_remote_storage.timeline_path(
        env.initial_tenant, new_branch_timeline_id
    )
    assert (
        not new_branch_on_remote_storage.exists()
    ), "failpoint should had prohibited index_part.json upload"

    # during reconciliation we should had scheduled the uploads and on the
    # retried create_timeline, we will await for those to complete on next
    # client.timeline_create
    env.pageserver.start(extra_env_vars={"FAILPOINTS": "before-upload-index=return"})

    # sleep a bit to force the upload task go into exponential backoff
    time.sleep(1)

    q: queue.Queue[PageserverApiException | None] = queue.Queue()
    barrier = threading.Barrier(2)

    def create_in_background():
        barrier.wait()
        try:
            # retrying this kind of query makes no sense in real life as we do
            # not lock in the lsn. with the immediate stop, we could in real
            # life revert back the ancestor in startup, but most likely the lsn
            # would still be branchable.
            client.timeline_create(
                tenant_id=env.initial_tenant,
                ancestor_timeline_id=env.initial_timeline,
                new_timeline_id=new_branch_timeline_id,
                pg_version=env.pg_version,
            )
            q.put(None)
        except PageserverApiException as e:
            q.put(e)

    create_thread = threading.Thread(target=create_in_background)
    create_thread.start()

    try:
        # maximize chances of actually waiting for the uploads by create_timeline
        barrier.wait()

        assert not new_branch_on_remote_storage.exists(), "failpoint should had stopped uploading"

        client.configure_failpoints(("before-upload-index", "off"))
        exception = q.get()

        assert (
            exception is None
        ), "create_timeline should have succeeded, because we deleted unuploaded local state"

        # this is because creating a timeline always awaits for the uploads to complete
        assert_nothing_to_upload(client, env.initial_tenant, new_branch_timeline_id)

        assert env.pageserver_remote_storage.index_path(
            env.initial_tenant, new_branch_timeline_id
        ).is_file(), "uploads scheduled during initial load should had been awaited for"
    finally:
        barrier.abort()
        create_thread.join()


def test_paused_upload_stalls_checkpoint(
    neon_env_builder: NeonEnvBuilder,
):
    """
    This test checks that checkpoints block on uploads to remote storage.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # Set a small compaction threshold
            "compaction_threshold": "3",
            # Disable GC
            "gc_period": "0s",
            # disable PITR
            "pitr_interval": "0s",
        }
    )

    env.pageserver.allowed_errors.append(
        f".*PUT.* path=/v1/tenant/{env.initial_tenant}/timeline.* request was dropped before completing"
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    client = env.pageserver.http_client()
    layers_at_creation = client.layer_map_info(tenant_id, timeline_id)
    deltas_at_creation = len(layers_at_creation.delta_layers())
    assert (
        deltas_at_creation == 1
    ), "are you fixing #5863? make sure we end up with 2 deltas at the end of endpoint lifecycle"

    # Make new layer uploads get stuck.
    # Note that timeline creation waits for the initial layers to reach remote storage.
    # So at this point, the `layers_at_creation` are in remote storage.
    client.configure_failpoints(("before-upload-layer-pausable", "pause"))

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        # Build two tables with some data inside
        endpoint.safe_psql("CREATE TABLE foo AS SELECT x FROM generate_series(1, 10000) g(x)")
        wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

        with pytest.raises(ReadTimeout):
            client.timeline_checkpoint(tenant_id, timeline_id, timeout=5)
        client.configure_failpoints(("before-upload-layer-pausable", "off"))


def wait_upload_queue_empty(
    client: PageserverHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    wait_until(
        lambda: assert_eq(
            get_queued_count(client, tenant_id, timeline_id, file_kind="layer", op_kind="upload"), 0
        ),
    )
    wait_until(
        lambda: assert_eq(
            get_queued_count(client, tenant_id, timeline_id, file_kind="index", op_kind="upload"), 0
        ),
    )
    wait_until(
        lambda: assert_eq(
            get_queued_count(client, tenant_id, timeline_id, file_kind="layer", op_kind="delete"), 0
        ),
    )


def get_queued_count(
    client: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    file_kind: str,
    op_kind: str,
):
    """The most important aspect of this function is shorter name & no return type so asserts are more concise."""
    return client.get_remote_timeline_client_queue_count(tenant_id, timeline_id, file_kind, op_kind)


def assert_nothing_to_upload(
    client: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
):
    """
    Check last_record_lsn == remote_consistent_lsn. Assert works only for empty timelines, which
    do not have anything to compact or gc.
    """
    detail = client.timeline_detail(tenant_id, timeline_id)
    assert Lsn(detail["last_record_lsn"]) == Lsn(detail["remote_consistent_lsn"])


# TODO Test that we correctly handle GC of files that are stuck in upload queue.
