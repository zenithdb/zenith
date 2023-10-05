import enum
import os
import queue
import shutil
import threading

import pytest
import requests
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    MANY_SMALL_LAYERS_TENANT_CONFIG,
    assert_prefix_empty,
    assert_prefix_not_empty,
    poll_for_remote_storage_iterations,
    timeline_delete_wait_completed,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_timeline_detail_404,
    wait_until_tenant_active,
    wait_until_timeline_state,
)
from fixtures.remote_storage import (
    LocalFsStorage,
    RemoteStorageKind,
    available_remote_storages,
    available_s3_storages,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar, run_pg_bench_small, wait_until
from urllib3.util.retry import Retry


def test_timeline_delete(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.pageserver.allowed_errors.append(".*Timeline .* was not found.*")
    env.pageserver.allowed_errors.append(".*timeline not found.*")
    env.pageserver.allowed_errors.append(".*Cannot delete timeline which has child timelines.*")
    env.pageserver.allowed_errors.append(".*Precondition failed: Requested tenant is missing.*")

    ps_http = env.pageserver.http_client()

    # first try to delete non existing timeline
    # for existing tenant:
    invalid_timeline_id = TimelineId.generate()
    with pytest.raises(PageserverApiException, match="timeline not found"):
        ps_http.timeline_delete(tenant_id=env.initial_tenant, timeline_id=invalid_timeline_id)

    # for non existing tenant:
    invalid_tenant_id = TenantId.generate()
    with pytest.raises(
        PageserverApiException,
        match="Precondition failed: Requested tenant is missing",
    ) as exc:
        ps_http.timeline_delete(tenant_id=invalid_tenant_id, timeline_id=invalid_timeline_id)

    assert exc.value.status_code == 412

    # construct pair of branches to validate that pageserver prohibits
    # deletion of ancestor timelines when they have child branches
    parent_timeline_id = env.neon_cli.create_branch("test_ancestor_branch_delete_parent", "empty")

    leaf_timeline_id = env.neon_cli.create_branch(
        "test_ancestor_branch_delete_branch1", "test_ancestor_branch_delete_parent"
    )

    timeline_path = env.pageserver.timeline_dir(env.initial_tenant, parent_timeline_id)

    with pytest.raises(
        PageserverApiException, match="Cannot delete timeline which has child timelines"
    ) as exc:
        assert timeline_path.exists()

        ps_http.timeline_delete(env.initial_tenant, parent_timeline_id)

    assert exc.value.status_code == 412

    timeline_path = env.pageserver.timeline_dir(env.initial_tenant, leaf_timeline_id)
    assert timeline_path.exists()

    # retry deletes when compaction or gc is running in pageserver
    wait_until(
        number_of_iterations=3,
        interval=0.2,
        func=lambda: timeline_delete_wait_completed(ps_http, env.initial_tenant, leaf_timeline_id),
    )

    assert not timeline_path.exists()

    # check 404
    with pytest.raises(
        PageserverApiException,
        match=f"Timeline {env.initial_tenant}/{leaf_timeline_id} was not found",
    ) as exc:
        ps_http.timeline_detail(env.initial_tenant, leaf_timeline_id)
    assert exc.value.status_code == 404

    wait_until(
        number_of_iterations=3,
        interval=0.2,
        func=lambda: timeline_delete_wait_completed(
            ps_http, env.initial_tenant, parent_timeline_id
        ),
    )

    # Check that we didn't pick up the timeline again after restart.
    # See https://github.com/neondatabase/neon/issues/3560
    env.pageserver.stop(immediate=True)
    env.pageserver.start()

    wait_until_tenant_active(ps_http, env.initial_tenant)

    with pytest.raises(
        PageserverApiException,
        match=f"Timeline {env.initial_tenant}/{leaf_timeline_id} was not found",
    ) as exc:
        ps_http.timeline_detail(env.initial_tenant, leaf_timeline_id)


class Check(enum.Enum):
    RETRY_WITHOUT_RESTART = enum.auto()
    RETRY_WITH_RESTART = enum.auto()


DELETE_FAILPOINTS = [
    "timeline-delete-before-index-deleted-at",
    "timeline-delete-before-schedule",
    "timeline-delete-before-rm",
    "timeline-delete-during-rm",
    "timeline-delete-after-rm",
    "timeline-delete-before-index-delete",
    "timeline-delete-after-index-delete",
    "timeline-delete-after-rm-metadata",
    "timeline-delete-after-rm-dir",
]


def combinations():
    result = []

    remotes = [RemoteStorageKind.MOCK_S3]
    if os.getenv("ENABLE_REAL_S3_REMOTE_STORAGE"):
        remotes.append(RemoteStorageKind.REAL_S3)

    for remote_storage_kind in remotes:
        for delete_failpoint in DELETE_FAILPOINTS:
            result.append((remote_storage_kind, delete_failpoint))
    return result


# cover the two cases: remote storage configured vs not configured
@pytest.mark.parametrize("remote_storage_kind, failpoint", combinations())
@pytest.mark.parametrize("check", list(Check))
def test_delete_timeline_exercise_crash_safety_failpoints(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    failpoint: str,
    check: Check,
    pg_bin: PgBin,
):
    """
    If there is a failure during deletion in one of the associated failpoints (or crash restart happens at this point) the delete operation
    should be retryable and should be successfully resumed.

    We iterate over failpoints list, changing failpoint to the next one.

    1. Set settings to generate many layers
    2. Create branch.
    3. Insert something
    4. Go with the test.
    5. Iterate over failpoints
    6. Execute delete for each failpoint
    7. Ensure failpoint is hit
    8. Retry or restart without the failpoint and check the result.
    """

    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
            "compaction_period": "0s",
            "checkpoint_distance": f"{1024 ** 2}",
            "image_creation_threshold": "100",
        }
    )

    ps_http = env.pageserver.http_client()

    timeline_id = env.neon_cli.create_timeline("delete")
    with env.endpoints.create_start("delete") as endpoint:
        # generate enough layers
        run_pg_bench_small(pg_bin, endpoint.connstr())

        last_flush_lsn_upload(env, endpoint, env.initial_tenant, timeline_id)

        if remote_storage_kind in available_s3_storages():
            assert_prefix_not_empty(
                neon_env_builder,
                prefix="/".join(
                    (
                        "tenants",
                        str(env.initial_tenant),
                        "timelines",
                        str(timeline_id),
                    )
                ),
            )

    env.pageserver.allowed_errors.append(f".*{timeline_id}.*failpoint: {failpoint}")
    # It appears when we stopped flush loop during deletion and then pageserver is stopped
    env.pageserver.allowed_errors.append(
        ".*shutdown_all_tenants:shutdown.*tenant_id.*shutdown.*timeline_id.*: failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
    )
    # This happens when we fail before scheduling background operation.
    # Timeline is left in stopping state and retry tries to stop it again.
    env.pageserver.allowed_errors.append(
        ".*Ignoring new state, equal to the existing one: Stopping"
    )
    # This happens when we retry delete requests for broken timelines
    env.pageserver.allowed_errors.append(".*Ignoring state update Stopping for broken timeline")
    # This happens when timeline remains are cleaned up during loading
    env.pageserver.allowed_errors.append(".*Timeline dir entry become invalid.*")
    # In one of the branches we poll for tenant to become active. Polls can generate this log message:
    env.pageserver.allowed_errors.append(f".*Tenant {env.initial_tenant} is not active*")

    ps_http.configure_failpoints((failpoint, "return"))

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    # These failpoints are earlier than background task is spawned.
    # so they result in api request failure.
    if failpoint in (
        "timeline-delete-before-index-deleted-at",
        "timeline-delete-before-schedule",
    ):
        with pytest.raises(PageserverApiException, match=failpoint):
            ps_http.timeline_delete(env.initial_tenant, timeline_id)

    else:
        ps_http.timeline_delete(env.initial_tenant, timeline_id)
        timeline_info = wait_until_timeline_state(
            pageserver_http=ps_http,
            tenant_id=env.initial_tenant,
            timeline_id=timeline_id,
            expected_state="Broken",
            iterations=iterations,
        )

        reason = timeline_info["state"]["Broken"]["reason"]
        log.info(f"timeline broken: {reason}")

        # failpoint may not be the only error in the stack
        assert reason.endswith(f"failpoint: {failpoint}"), reason

    if check is Check.RETRY_WITH_RESTART:
        env.pageserver.stop()
        env.pageserver.start()

        wait_until_tenant_active(ps_http, env.initial_tenant, iterations=iterations)

        if failpoint == "timeline-delete-before-index-deleted-at":
            # We crashed before persisting this to remote storage, need to retry delete request
            timeline_delete_wait_completed(ps_http, env.initial_tenant, timeline_id)
        else:
            # Pageserver should've resumed deletion after restart.
            wait_timeline_detail_404(
                ps_http, env.initial_tenant, timeline_id, iterations=iterations
            )

            if failpoint == "timeline-delete-after-index-delete":
                m = ps_http.get_metrics()
                assert (
                    m.query_one(
                        "remote_storage_s3_request_seconds_count",
                        filter={"request_type": "get_object", "result": "err"},
                    ).value
                    == 2  # One is missing tenant deletion mark, second is missing index part
                )
                assert (
                    m.query_one(
                        "remote_storage_s3_request_seconds_count",
                        filter={"request_type": "get_object", "result": "ok"},
                    ).value
                    == 1  # index part for initial timeline
                )
    elif check is Check.RETRY_WITHOUT_RESTART:
        # this should succeed
        # this also checks that delete can be retried even when timeline is in Broken state
        ps_http.configure_failpoints((failpoint, "off"))

        timeline_delete_wait_completed(
            ps_http, env.initial_tenant, timeline_id, iterations=iterations
        )

    # Check remote is empty
    if remote_storage_kind is RemoteStorageKind.MOCK_S3:
        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(env.initial_tenant),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )

    timeline_dir = env.pageserver.timeline_dir(env.initial_tenant, timeline_id)
    # Check local is empty
    assert not timeline_dir.exists()
    # Check no delete mark present
    assert not (timeline_dir.parent / f"{timeline_id}.___deleted").exists()


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
@pytest.mark.parametrize("fill_branch", [True, False])
def test_timeline_resurrection_on_attach(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    fill_branch: bool,
):
    """
    After deleting a timeline it should never appear again.
    This test ensures that this invariant holds for detach+attach.
    Original issue: https://github.com/neondatabase/neon/issues/3560
    """

    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    ##### First start, insert data and upload it to the remote storage
    env = neon_env_builder.init_start()

    ps_http = env.pageserver.http_client()
    pg = env.endpoints.create_start("main")

    tenant_id = env.initial_tenant
    main_timeline_id = env.initial_timeline

    with pg.cursor() as cur:
        cur.execute("CREATE TABLE f (i integer);")
        cur.execute("INSERT INTO f VALUES (generate_series(1,1000));")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(ps_http, tenant_id, main_timeline_id, current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        ps_http.timeline_checkpoint(tenant_id, main_timeline_id)

        # wait until pageserver successfully uploaded a checkpoint to remote storage
        log.info("waiting for checkpoint upload")
        wait_for_upload(ps_http, tenant_id, main_timeline_id, current_lsn)
        log.info("upload of checkpoint is done")

    branch_timeline_id = env.neon_cli.create_branch("new", "main")

    # Two variants of this test:
    # - In fill_branch=True, the deleted branch has layer files.
    # - In fill_branch=False, it doesn't, it just has the metadata file.
    # A broken implementation is conceivable that tries to "optimize" handling of empty branches, e.g.,
    # by skipping IndexPart uploads if the layer file set doesn't change. That would be wrong, catch those.
    if fill_branch:
        with env.endpoints.create_start("new") as new_pg:
            with new_pg.cursor() as cur:
                cur.execute("INSERT INTO f VALUES (generate_series(1,1000));")
                current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

                # wait until pageserver receives that data
                wait_for_last_record_lsn(ps_http, tenant_id, branch_timeline_id, current_lsn)

                # run checkpoint manually to be sure that data landed in remote storage
                ps_http.timeline_checkpoint(tenant_id, branch_timeline_id)

                # wait until pageserver successfully uploaded a checkpoint to remote storage
                log.info("waiting for checkpoint upload")
                wait_for_upload(ps_http, tenant_id, branch_timeline_id, current_lsn)
                log.info("upload of checkpoint is done")
    else:
        pass

    # delete new timeline
    timeline_delete_wait_completed(ps_http, tenant_id=tenant_id, timeline_id=branch_timeline_id)

    ##### Stop the pageserver instance, erase all its data
    env.endpoints.stop_all()
    env.pageserver.stop()

    dir_to_clear = env.pageserver.tenant_dir()
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure that we see only timeline that wasnt deleted
    env.pageserver.start()

    ps_http.tenant_attach(tenant_id=tenant_id)

    wait_until_tenant_active(ps_http, tenant_id=tenant_id, iterations=10, period=0.5)

    timelines = ps_http.timeline_list(tenant_id=tenant_id)
    assert {TimelineId(tl["timeline_id"]) for tl in timelines} == {
        main_timeline_id
    }, "the deleted timeline should not have been resurrected"
    assert all([tl["state"] == "Active" for tl in timelines])


def test_timeline_delete_fail_before_local_delete(neon_env_builder: NeonEnvBuilder):
    """
    When deleting a timeline, if we succeed in setting the deleted flag remotely
    but fail to delete the local state, restarting the pageserver should resume
    the deletion of the local state.
    """

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.append(".*failpoint: timeline-delete-before-rm")
    env.pageserver.allowed_errors.append(
        ".*Ignoring new state, equal to the existing one: Stopping"
    )
    # this happens, because the stuck timeline is visible to shutdown
    env.pageserver.allowed_errors.append(
        ".*shutdown_all_tenants:shutdown.*tenant_id.*shutdown.*timeline_id.*: failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
    )

    ps_http = env.pageserver.http_client()
    ps_http.configure_failpoints(("timeline-delete-before-rm", "return"))

    # construct pair of branches
    intermediate_timeline_id = env.neon_cli.create_branch(
        "test_timeline_delete_fail_before_local_delete"
    )

    leaf_timeline_id = env.neon_cli.create_branch(
        "test_timeline_delete_fail_before_local_delete1",
        "test_timeline_delete_fail_before_local_delete",
    )

    leaf_timeline_path = env.pageserver.timeline_dir(env.initial_tenant, leaf_timeline_id)

    ps_http.timeline_delete(env.initial_tenant, leaf_timeline_id)
    timeline_info = wait_until_timeline_state(
        pageserver_http=ps_http,
        tenant_id=env.initial_tenant,
        timeline_id=leaf_timeline_id,
        expected_state="Broken",
        iterations=2,  # effectively try immediately and retry once in one second
    )

    assert timeline_info["state"]["Broken"]["reason"] == "failpoint: timeline-delete-before-rm"

    assert leaf_timeline_path.exists(), "the failpoint didn't work"

    env.pageserver.stop()
    env.pageserver.start()

    # Wait for tenant to finish loading.
    wait_until_tenant_active(ps_http, tenant_id=env.initial_tenant, iterations=10, period=1)

    wait_timeline_detail_404(ps_http, env.initial_tenant, leaf_timeline_id, iterations=4)

    assert (
        not leaf_timeline_path.exists()
    ), "timeline load procedure should have resumed the deletion interrupted by the failpoint"
    timelines = ps_http.timeline_list(env.initial_tenant)
    assert {TimelineId(tl["timeline_id"]) for tl in timelines} == {
        intermediate_timeline_id,
        env.initial_timeline,
    }, "other timelines should not have been affected"
    assert all([tl["state"] == "Active" for tl in timelines])

    assert_prefix_empty(
        neon_env_builder,
        prefix="/".join(
            (
                "tenants",
                str(env.initial_tenant),
                "timelines",
                str(leaf_timeline_id),
            )
        ),
    )

    for timeline_id in (intermediate_timeline_id, env.initial_timeline):
        timeline_delete_wait_completed(
            ps_http, tenant_id=env.initial_tenant, timeline_id=timeline_id
        )

        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(env.initial_tenant),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )

    # for some reason the check above doesn't immediately take effect for the below.
    # Assume it is mock server incosistency and check twice.
    wait_until(
        2,
        0.5,
        lambda: assert_prefix_empty(neon_env_builder),
    )


@pytest.mark.parametrize(
    "stuck_failpoint",
    ["persist_deleted_index_part", "in_progress_delete"],
)
def test_concurrent_timeline_delete_stuck_on(
    neon_env_builder: NeonEnvBuilder, stuck_failpoint: str
):
    """
    If delete is stuck console will eventually retry deletion.
    So we need to be sure that these requests won't interleave with each other.
    In this tests we check two places where we can spend a lot of time.
    This is a regression test because there was a bug when DeletionGuard wasnt propagated
    to the background task.

    Ensure that when retry comes if we're still stuck request will get an immediate error response,
    signalling to console that it should retry later.
    """

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    env = neon_env_builder.init_start()

    child_timeline_id = env.neon_cli.create_branch("child", "main")

    ps_http = env.pageserver.http_client()

    # make the first call sleep practically forever
    ps_http.configure_failpoints((stuck_failpoint, "pause"))

    def first_call(result_queue):
        try:
            log.info("first call start")
            timeline_delete_wait_completed(
                ps_http, env.initial_tenant, child_timeline_id, timeout=20
            )
            log.info("first call success")
            result_queue.put("success")
        except Exception:
            log.exception("first call failed")
            result_queue.put("failure, see log for stack trace")

    first_call_result: queue.Queue[str] = queue.Queue()
    first_call_thread = threading.Thread(target=first_call, args=(first_call_result,))
    first_call_thread.start()

    try:

        def first_call_hit_failpoint():
            assert env.pageserver.log_contains(
                f".*{child_timeline_id}.*at failpoint {stuck_failpoint}"
            )

        wait_until(50, 0.1, first_call_hit_failpoint)

        # make the second call and assert behavior
        log.info("second call start")
        error_msg_re = "Timeline deletion is already in progress"
        with pytest.raises(PageserverApiException, match=error_msg_re) as second_call_err:
            ps_http.timeline_delete(env.initial_tenant, child_timeline_id)
        assert second_call_err.value.status_code == 409
        env.pageserver.allowed_errors.append(f".*{child_timeline_id}.*{error_msg_re}.*")
        # the second call will try to transition the timeline into Stopping state as well
        env.pageserver.allowed_errors.append(
            f".*{child_timeline_id}.*Ignoring new state, equal to the existing one: Stopping"
        )
        log.info("second call failed as expected")

        # ensure it is not 404 and stopping
        detail = ps_http.timeline_detail(env.initial_tenant, child_timeline_id)
        assert detail["state"] == "Stopping"

        # by now we know that the second call failed, let's ensure the first call will finish
        ps_http.configure_failpoints((stuck_failpoint, "off"))

        result = first_call_result.get()
        assert result == "success"

    finally:
        log.info("joining first call thread")
        # in any case, make sure the lifetime of the thread is bounded to this test
        first_call_thread.join()


def test_delete_timeline_client_hangup(neon_env_builder: NeonEnvBuilder):
    """
    If the client hangs up before we start the index part upload but after deletion is scheduled
    we mark it
    deleted in local memory, a subsequent delete_timeline call should be able to do

    another delete timeline operation.

    This tests cancel safety up to the given failpoint.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    env = neon_env_builder.init_start()

    child_timeline_id = env.neon_cli.create_branch("child", "main")

    ps_http = env.pageserver.http_client(retries=Retry(0, read=False))

    failpoint_name = "persist_deleted_index_part"
    ps_http.configure_failpoints((failpoint_name, "pause"))

    with pytest.raises(requests.exceptions.Timeout):
        ps_http.timeline_delete(env.initial_tenant, child_timeline_id, timeout=2)

    env.pageserver.allowed_errors.append(
        f".*{child_timeline_id}.*Timeline deletion is already in progress.*"
    )
    with pytest.raises(PageserverApiException, match="Timeline deletion is already in progress"):
        ps_http.timeline_delete(env.initial_tenant, child_timeline_id, timeout=2)

    # make sure the timeout was due to the failpoint
    at_failpoint_log_message = f".*{child_timeline_id}.*at failpoint {failpoint_name}.*"

    def hit_failpoint():
        assert env.pageserver.log_contains(at_failpoint_log_message)

    wait_until(50, 0.1, hit_failpoint)

    # we log this error if a client hangs up
    # might as well use it as another indicator that the test works
    hangup_log_message = f".*DELETE.*{child_timeline_id}.*request was dropped before completing"
    env.pageserver.allowed_errors.append(hangup_log_message)

    def got_hangup_log_message():
        assert env.pageserver.log_contains(hangup_log_message)

    wait_until(50, 0.1, got_hangup_log_message)

    # check that the timeline is still present
    ps_http.timeline_detail(env.initial_tenant, child_timeline_id)

    # ok, disable the failpoint to let the deletion finish
    ps_http.configure_failpoints((failpoint_name, "off"))

    def first_request_finished():
        message = f".*DELETE.*{child_timeline_id}.*Cancelled request finished"
        assert env.pageserver.log_contains(message)

    wait_until(50, 0.1, first_request_finished)

    # check that the timeline is gone
    wait_timeline_detail_404(ps_http, env.initial_tenant, child_timeline_id, iterations=2)


@pytest.mark.parametrize(
    "remote_storage_kind",
    list(
        filter(
            lambda s: s in (RemoteStorageKind.MOCK_S3, RemoteStorageKind.REAL_S3),
            available_remote_storages(),
        )
    ),
)
def test_timeline_delete_works_for_remote_smoke(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()

    ps_http = env.pageserver.http_client()
    pg = env.endpoints.create_start("main")

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    timeline_ids = [env.initial_timeline]
    for i in range(2):
        branch_timeline_id = env.neon_cli.create_branch(f"new{i}", "main")
        pg = env.endpoints.create_start(f"new{i}")

        with pg.cursor() as cur:
            cur.execute("CREATE TABLE f (i integer);")
            cur.execute("INSERT INTO f VALUES (generate_series(1,1000));")
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

            # wait until pageserver receives that data
            wait_for_last_record_lsn(ps_http, tenant_id, branch_timeline_id, current_lsn)

            # run checkpoint manually to be sure that data landed in remote storage
            ps_http.timeline_checkpoint(tenant_id, branch_timeline_id)

            # wait until pageserver successfully uploaded a checkpoint to remote storage
            log.info("waiting for checkpoint upload")
            wait_for_upload(ps_http, tenant_id, branch_timeline_id, current_lsn)
            log.info("upload of checkpoint is done")

        timeline_ids.append(branch_timeline_id)

    for timeline_id in timeline_ids:
        assert_prefix_not_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(env.initial_tenant),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )

    for timeline_id in reversed(timeline_ids):
        # note that we need to finish previous deletion before scheduling next one
        # otherwise we can get an "HasChildren" error if deletion is not fast enough (real_s3)
        timeline_delete_wait_completed(ps_http, tenant_id=tenant_id, timeline_id=timeline_id)

        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(env.initial_tenant),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )

    # for some reason the check above doesn't immediately take effect for the below.
    # Assume it is mock server inconsistency and check twice.
    wait_until(2, 0.5, lambda: assert_prefix_empty(neon_env_builder))


def test_delete_orphaned_objects(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    remote_storage_kind = RemoteStorageKind.LOCAL_FS
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
            "compaction_period": "0s",
            "checkpoint_distance": f"{1024 ** 2}",
            "image_creation_threshold": "100",
        }
    )

    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    ps_http = env.pageserver.http_client()

    timeline_id = env.neon_cli.create_timeline("delete")
    with env.endpoints.create_start("delete") as endpoint:
        # generate enough layers
        run_pg_bench_small(pg_bin, endpoint.connstr())
        last_flush_lsn_upload(env, endpoint, env.initial_tenant, timeline_id)

    # write orphaned file that is missing from the index
    remote_timeline_path = env.pageserver_remote_storage.timeline_path(
        env.initial_tenant, timeline_id
    )
    orphans = [remote_timeline_path / f"orphan_{i}" for i in range(3)]
    for orphan in orphans:
        orphan.write_text("I shouldnt be there")

    # trigger failpoint after orphaned file deletion to check that index_part is not deleted as well.
    failpoint = "timeline-delete-before-index-delete"
    ps_http.configure_failpoints((failpoint, "return"))

    env.pageserver.allowed_errors.append(f".*failpoint: {failpoint}")

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    ps_http.timeline_delete(env.initial_tenant, timeline_id)
    timeline_info = wait_until_timeline_state(
        pageserver_http=ps_http,
        tenant_id=env.initial_tenant,
        timeline_id=timeline_id,
        expected_state="Broken",
        iterations=iterations,
    )

    reason = timeline_info["state"]["Broken"]["reason"]
    assert reason.endswith(f"failpoint: {failpoint}"), reason

    ps_http.deletion_queue_flush(execute=True)

    for orphan in orphans:
        assert not orphan.exists()
        assert env.pageserver.log_contains(
            f"deleting a file not referenced from index_part.json name={orphan.stem}"
        )

    assert env.pageserver_remote_storage.index_path(env.initial_tenant, timeline_id).exists()


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_timeline_delete_resumed_on_attach(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_bin: PgBin,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)

    tenant_id = env.initial_tenant

    ps_http = env.pageserver.http_client()

    timeline_id = env.neon_cli.create_timeline("delete")
    with env.endpoints.create_start("delete") as endpoint:
        # generate enough layers
        run_pg_bench_small(pg_bin, endpoint.connstr())
        last_flush_lsn_upload(env, endpoint, env.initial_tenant, timeline_id)

        if remote_storage_kind in available_s3_storages():
            assert_prefix_not_empty(
                neon_env_builder,
                prefix="/".join(
                    (
                        "tenants",
                        str(env.initial_tenant),
                        "timelines",
                        str(timeline_id),
                    )
                ),
            )

    # failpoint before we remove index_part from s3
    failpoint = "timeline-delete-during-rm"
    ps_http.configure_failpoints((failpoint, "return"))

    env.pageserver.allowed_errors.extend(
        (
            # allow errors caused by failpoints
            f".*failpoint: {failpoint}",
            # It appears when we stopped flush loop during deletion (attempt) and then pageserver is stopped
            ".*shutdown_all_tenants:shutdown.*tenant_id.*shutdown.*timeline_id.*: failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
            # error from http response is also logged
            ".*InternalServerError\\(Tenant is marked as deleted on remote storage.*",
            # Polling after attach may fail with this
            ".*Resource temporarily unavailable.*Tenant not yet active",
            '.*shutdown_pageserver{exit_code=0}: stopping left-over name="remote upload".*',
        )
    )

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    ps_http.timeline_delete(tenant_id, timeline_id)

    timeline_info = wait_until_timeline_state(
        pageserver_http=ps_http,
        tenant_id=env.initial_tenant,
        timeline_id=timeline_id,
        expected_state="Broken",
        iterations=iterations,
    )

    reason = timeline_info["state"]["Broken"]["reason"]
    log.info(f"timeline broken: {reason}")

    # failpoint may not be the only error in the stack
    assert reason.endswith(f"failpoint: {failpoint}"), reason

    if remote_storage_kind in available_s3_storages():
        assert_prefix_not_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(tenant_id),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )

    # now we stop pageserver and remove local tenant state
    env.endpoints.stop_all()
    env.pageserver.stop()

    dir_to_clear = env.pageserver.tenant_dir()
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    env.pageserver.start()

    # now we call attach
    ps_http.tenant_attach(tenant_id=tenant_id)

    # delete should be resumed
    wait_timeline_detail_404(ps_http, env.initial_tenant, timeline_id, iterations=iterations)

    tenant_path = env.pageserver.timeline_dir(tenant_id, timeline_id)
    assert not tenant_path.exists()

    if remote_storage_kind in available_s3_storages():
        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(timeline_id),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )
