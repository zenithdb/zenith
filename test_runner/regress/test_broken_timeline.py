import concurrent.futures
import os
from typing import List, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, NeonEnvBuilder
from fixtures.types import TenantId, TimelineId


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_broken_timeline(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(
        [
            ".*Failed to load delta layer.*",
            ".*could not find data for key.*",
            ".*is not active. Current state: Broken.*",
            ".*will not become active. Current state: Broken.*",
            ".*failed to load metadata.*",
            ".*load failed.*load local timeline.*",
            ".*load failed, setting tenant state to Broken.*",
        ]
    )

    tenant_timelines: List[Tuple[TenantId, TimelineId, Endpoint]] = []

    for n in range(4):
        tenant_id, timeline_id = env.neon_cli.create_tenant()

        endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,100), 'payload'")
        endpoint.stop()
        tenant_timelines.append((tenant_id, timeline_id, endpoint))

    # Stop the pageserver
    env.pageserver.stop()

    # Leave the first timeline alone, but corrupt the others in different ways
    (tenant0, timeline0, pg0) = tenant_timelines[0]
    log.info(f"Timeline {tenant0}/{timeline0} is left intact")

    (tenant1, timeline1, pg1) = tenant_timelines[1]
    metadata_path = f"{env.repo_dir}/tenants/{tenant1}/timelines/{timeline1}/metadata"
    f = open(metadata_path, "w")
    f.write("overwritten with garbage!")
    f.close()
    log.info(f"Timeline {tenant1}/{timeline1} got its metadata spoiled")

    (tenant2, timeline2, pg2) = tenant_timelines[2]
    timeline_path = f"{env.repo_dir}/tenants/{tenant2}/timelines/{timeline2}/"
    for filename in os.listdir(timeline_path):
        if filename.startswith("00000"):
            # Looks like a layer file. Remove it
            os.remove(f"{timeline_path}/{filename}")
    log.info(
        f"Timeline {tenant2}/{timeline2} got its layer files removed (no remote storage enabled)"
    )

    (tenant3, timeline3, pg3) = tenant_timelines[3]
    timeline_path = f"{env.repo_dir}/tenants/{tenant3}/timelines/{timeline3}/"
    for filename in os.listdir(timeline_path):
        if filename.startswith("00000"):
            # Looks like a layer file. Corrupt it
            f = open(f"{timeline_path}/{filename}", "w")
            f.write("overwritten with garbage!")
            f.close()
    log.info(f"Timeline {tenant3}/{timeline3} got its layer files spoiled")

    env.pageserver.start()

    # Tenant 0 should still work
    pg0.start()
    assert pg0.safe_psql("SELECT COUNT(*) FROM t")[0][0] == 100

    # But all others are broken

    # First timeline would not get loaded into pageserver due to corrupt metadata file
    with pytest.raises(
        Exception, match=f"Tenant {tenant1} will not become active. Current state: Broken"
    ) as err:
        pg1.start()
    log.info(
        f"As expected, compute startup failed eagerly for timeline with corrupt metadata: {err}"
    )

    # Second timeline has no ancestors, only the metadata file and no layer files locally,
    # and we don't have the remote storage enabled. It is loaded into memory, but getting
    # the basebackup from it will fail.
    with pytest.raises(
        Exception, match=f"Tenant {tenant2} will not become active. Current state: Broken"
    ) as err:
        pg2.start()
    log.info(f"As expected, compute startup failed for timeline with missing layers: {err}")

    # Third timeline will also fail during basebackup, because the layer file is corrupt.
    # It will fail when we try to read (and reconstruct) a page from it, ergo the error message.
    # (We don't check layer file contents on startup, when loading the timeline)
    with pytest.raises(Exception, match="Failed to load delta layer") as err:
        pg3.start()
    log.info(
        f"As expected, compute startup failed for timeline {tenant3}/{timeline3} with corrupt layers: {err}"
    )


def test_create_multiple_timelines_parallel(neon_simple_env: NeonEnv):
    env = neon_simple_env

    tenant_id, _ = env.neon_cli.create_tenant()

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(
                env.neon_cli.create_timeline, f"test-create-multiple-timelines-{i}", tenant_id
            )
            for i in range(4)
        ]
        for future in futures:
            future.result()


def test_timeline_init_break_before_checkpoint(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.extend(
        [
            ".*Failed to process timeline dir contents.*Timeline has no ancestor and no layer files.*",
            ".*Timeline got dropped without initializing, cleaning its files.*",
        ]
    )

    tenant_id, _ = env.neon_cli.create_tenant()

    timelines_dir = env.repo_dir / "tenants" / str(tenant_id) / "timelines"
    old_tenant_timelines = env.neon_cli.list_timelines(tenant_id)
    initial_timeline_dirs = [d for d in timelines_dir.iterdir()]

    # Introduce failpoint during timeline init (some intermediate files are on disk), before it's checkpointed.
    pageserver_http.configure_failpoints(("before-checkpoint-new-timeline", "return"))
    with pytest.raises(Exception, match="before-checkpoint-new-timeline"):
        _ = env.neon_cli.create_timeline("test_timeline_init_break_before_checkpoint", tenant_id)

    # Restart the page server
    env.neon_cli.pageserver_stop(immediate=True)
    env.neon_cli.pageserver_start()

    # Creating the timeline didn't finish. The other timelines on tenant should still be present and work normally.
    new_tenant_timelines = env.neon_cli.list_timelines(tenant_id)
    assert (
        new_tenant_timelines == old_tenant_timelines
    ), f"Pageserver after restart should ignore non-initialized timelines for tenant {tenant_id}"

    timeline_dirs = [d for d in timelines_dir.iterdir()]
    assert (
        timeline_dirs == initial_timeline_dirs
    ), "pageserver should clean its temp timeline files on timeline creation failure"


def test_timeline_create_break_after_uninit_mark(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()

    tenant_id, _ = env.neon_cli.create_tenant()

    timelines_dir = env.repo_dir / "tenants" / str(tenant_id) / "timelines"
    old_tenant_timelines = env.neon_cli.list_timelines(tenant_id)
    initial_timeline_dirs = [d for d in timelines_dir.iterdir()]

    # Introduce failpoint when creating a new timeline uninit mark, before any other files were created
    pageserver_http.configure_failpoints(("after-timeline-uninit-mark-creation", "return"))
    with pytest.raises(Exception, match="after-timeline-uninit-mark-creation"):
        _ = env.neon_cli.create_timeline("test_timeline_create_break_after_uninit_mark", tenant_id)

    # Creating the timeline didn't finish. The other timelines on tenant should still be present and work normally.
    # "New" timeline is not present in the list, allowing pageserver to retry the same request
    new_tenant_timelines = env.neon_cli.list_timelines(tenant_id)
    assert (
        new_tenant_timelines == old_tenant_timelines
    ), f"Pageserver after restart should ignore non-initialized timelines for tenant {tenant_id}"

    timeline_dirs = [d for d in timelines_dir.iterdir()]
    assert (
        timeline_dirs == initial_timeline_dirs
    ), "pageserver should clean its temp timeline files on timeline creation failure"
