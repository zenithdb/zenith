from __future__ import annotations

import concurrent.futures
import os

import pytest
from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.pg_version import PgVersion


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_local_corruption(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(
        [
            ".*get_values_reconstruct_data for layer .*",
            ".*could not find data for key.*",
            ".*is not active. Current state: Broken.*",
            ".*will not become active. Current state: Broken.*",
            ".*failed to load metadata.*",
            ".*load failed.*load local timeline.*",
            ".*: layer load failed, assuming permanent failure:.*",
            ".*failed to get checkpoint bytes.*",
            ".*failed get control bytes.*",
        ]
    )

    tenant_timelines: list[tuple[TenantId, TimelineId, Endpoint]] = []

    for _ in range(3):
        tenant_id, timeline_id = env.create_tenant()

        endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,100), 'payload'")
            wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
        endpoint.stop()
        tenant_timelines.append((tenant_id, timeline_id, endpoint))

    # Stop the pageserver -- this has to be not immediate or we need to wait for uploads
    env.pageserver.stop()

    # Leave the first timeline alone, but corrupt the others in different ways
    (tenant0, timeline0, pg0) = tenant_timelines[0]
    log.info(f"Timeline {tenant0}/{timeline0} is left intact")

    (tenant1, timeline1, pg1) = tenant_timelines[2]
    timeline_path = f"{env.pageserver.workdir}/tenants/{tenant1}/timelines/{timeline1}/"
    for filename in os.listdir(timeline_path):
        if filename.startswith("00000"):
            # Looks like a layer file. Corrupt it
            p = f"{timeline_path}/{filename}"
            size = os.path.getsize(p)
            with open(p, "wb") as f:
                f.truncate(0)
                f.truncate(size)
    log.info(f"Timeline {tenant1}/{timeline1} got its local layer files spoiled")

    env.pageserver.start()

    # Un-damaged tenant works
    pg0.start()
    assert pg0.safe_psql("SELECT COUNT(*) FROM t")[0][0] == 100

    # Second timeline will fail during basebackup, because the local layer file is corrupt.
    # It will fail when we try to read (and reconstruct) a page from it, ergo the error message.
    # (We don't check layer file contents on startup, when loading the timeline)
    #
    # This will change when we implement checksums for layers
    with pytest.raises(Exception, match="failed to get checkpoint bytes") as err:
        pg1.start()
    log.info(
        f"As expected, compute startup failed for timeline {tenant1}/{timeline1} with corrupt layers: {err}"
    )


def test_create_multiple_timelines_parallel(neon_simple_env: NeonEnv):
    env = neon_simple_env

    tenant_id, _ = env.create_tenant()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(env.create_timeline, f"test-create-multiple-timelines-{i}", tenant_id)
            for i in range(4)
        ]
        for future in futures:
            future.result()


def test_timeline_init_break_before_checkpoint(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.extend(
        [
            ".*Failed to process timeline dir contents.*Timeline has no ancestor and no layer files.*",
        ]
    )

    tenant_id = env.initial_tenant

    timelines_dir = env.pageserver.timeline_dir(tenant_id)
    old_tenant_timelines = env.neon_cli.timeline_list(tenant_id)
    initial_timeline_dirs = [d for d in timelines_dir.iterdir()]

    # Introduce failpoint during timeline init (some intermediate files are on disk), before it's checkpointed.
    pageserver_http.configure_failpoints(("before-checkpoint-new-timeline", "return"))
    with pytest.raises(Exception, match="before-checkpoint-new-timeline"):
        _ = pageserver_http.timeline_create(PgVersion.NOT_SET, tenant_id, TimelineId.generate())

    # Restart the page server
    env.pageserver.restart(immediate=True)

    # Creating the timeline didn't finish. The other timelines on tenant should still be present and work normally.
    new_tenant_timelines = env.neon_cli.timeline_list(tenant_id)
    assert (
        new_tenant_timelines == old_tenant_timelines
    ), f"Pageserver after restart should ignore non-initialized timelines for tenant {tenant_id}"

    timeline_dirs = [d for d in timelines_dir.iterdir()]
    assert (
        timeline_dirs == initial_timeline_dirs
    ), "pageserver should clean its temp timeline files on timeline creation failure"


# The "exit" case is for a reproducer of issue 6007: an unclean shutdown where we can't do local fs cleanups
@pytest.mark.parametrize("exit_or_return", ["return", "exit"])
def test_timeline_init_break_before_checkpoint_recreate(
    neon_env_builder: NeonEnvBuilder, exit_or_return: str
):
    env = neon_env_builder.init_configs()
    env.start()
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.extend(
        [
            ".*Failed to process timeline dir contents.*Timeline has no ancestor and no layer files.*",
            ".*Failed to load index_part from remote storage, failed creation?.*",
        ]
    )

    env.create_tenant(env.initial_tenant)
    tenant_id = env.initial_tenant

    timelines_dir = env.pageserver.timeline_dir(tenant_id)
    old_tenant_timelines = env.neon_cli.timeline_list(tenant_id)
    initial_timeline_dirs = [d for d in timelines_dir.iterdir()]

    # Some fixed timeline ID (like control plane does)
    timeline_id = TimelineId("1080243c1f76fe3c5147266663c9860b")

    # Introduce failpoint during timeline init (some intermediate files are on disk), before it's checkpointed.
    failpoint = "before-checkpoint-new-timeline"
    pattern = failpoint
    if exit_or_return == "exit":
        # in reality a read error happens, but there are automatic retries which now fail because pageserver is dead
        pattern = "Connection aborted."

    pageserver_http.configure_failpoints((failpoint, exit_or_return))
    with pytest.raises(Exception, match=pattern):
        _ = pageserver_http.timeline_create(env.pg_version, tenant_id, timeline_id)

    # Restart the page server (with the failpoint disabled)
    env.pageserver.restart(immediate=True)

    # Creating the timeline didn't finish. The other timelines on tenant should still be present and work normally.
    new_tenant_timelines = env.neon_cli.timeline_list(tenant_id)
    assert (
        new_tenant_timelines == old_tenant_timelines
    ), f"Pageserver after restart should ignore non-initialized timelines for tenant {tenant_id}"

    timeline_dirs = [d for d in timelines_dir.iterdir()]
    assert (
        timeline_dirs == initial_timeline_dirs
    ), "pageserver should clean its temp timeline files on timeline creation failure"

    # creating the branch should have worked now
    new_timeline_id = TimelineId(
        pageserver_http.timeline_create(env.pg_version, tenant_id, timeline_id)["timeline_id"]
    )

    assert timeline_id == new_timeline_id


def test_timeline_create_break_after_dir_creation(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    tenant_id = env.initial_tenant

    timelines_dir = env.pageserver.timeline_dir(tenant_id)
    old_tenant_timelines = env.neon_cli.timeline_list(tenant_id)
    initial_timeline_dirs = [d for d in timelines_dir.iterdir()]

    # Introduce failpoint when creating a new timeline, right after creating its directory
    pageserver_http.configure_failpoints(("after-timeline-dir-creation", "return"))
    with pytest.raises(Exception, match="after-timeline-dir-creation"):
        _ = pageserver_http.timeline_create(PgVersion.NOT_SET, tenant_id, TimelineId.generate())

    # Creating the timeline didn't finish. The other timelines on tenant should still be present and work normally.
    # "New" timeline is not present in the list, allowing pageserver to retry the same request
    new_tenant_timelines = env.neon_cli.timeline_list(tenant_id)
    assert (
        new_tenant_timelines == old_tenant_timelines
    ), f"Pageserver after restart should ignore non-initialized timelines for tenant {tenant_id}"

    timeline_dirs = [d for d in timelines_dir.iterdir()]
    assert (
        timeline_dirs == initial_timeline_dirs
    ), "pageserver should clean its temp timeline files on timeline creation failure"
