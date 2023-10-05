import asyncio
import random
import time
from threading import Thread
from typing import List, Optional

import asyncpg
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_until_tenant_state,
)
from fixtures.remote_storage import RemoteStorageKind, available_remote_storages
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar, wait_until
from prometheus_client.samples import Sample


def do_gc_target(
    pageserver_http: PageserverHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    """Hack to unblock main, see https://github.com/neondatabase/neon/issues/2211"""
    try:
        log.info("sending gc http request")
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
        pageserver_http.timeline_gc(tenant_id, timeline_id, 0)
    except Exception as e:
        log.error("do_gc failed: %s", e)
    finally:
        log.info("gc http thread returning")


# Basic detach and re-attach test
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_tenant_reattach(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    # Exercise retry code path by making all uploads and downloads fail for the
    # first time. The retries print INFO-messages to the log; we will check
    # that they are present after the test.
    neon_env_builder.pageserver_config_override = "test_remote_failures=1"

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # create new nenant
    tenant_id, timeline_id = env.neon_cli.create_tenant()

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )
    # That's because of UnreliableWrapper's injected failures
    env.pageserver.allowed_errors.append(
        f".*failed to fetch tenant deletion mark at tenants/({tenant_id}|{env.initial_tenant})/deleted attempt 1.*"
    )

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # Wait for the all data to be processed by the pageserver and uploaded in remote storage
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)

    # Check that we had to retry the uploads
    assert env.pageserver.log_contains(
        ".*failed to perform remote task UploadLayer.*, will retry.*"
    )
    assert env.pageserver.log_contains(
        ".*failed to perform remote task UploadMetadata.*, will retry.*"
    )

    ps_metrics = pageserver_http.get_metrics()
    tenant_metric_filter = {
        "tenant_id": str(tenant_id),
        "timeline_id": str(timeline_id),
    }
    pageserver_last_record_lsn_before_detach = int(
        ps_metrics.query_one("pageserver_last_record_lsn", filter=tenant_metric_filter).value
    )

    pageserver_http.tenant_detach(tenant_id)
    pageserver_http.tenant_attach(tenant_id)

    time.sleep(1)  # for metrics propagation

    ps_metrics = pageserver_http.get_metrics()
    pageserver_last_record_lsn = int(
        ps_metrics.query_one("pageserver_last_record_lsn", filter=tenant_metric_filter).value
    )

    assert pageserver_last_record_lsn_before_detach == pageserver_last_record_lsn

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            assert query_scalar(cur, "SELECT count(*) FROM t") == 100000

        # Check that we had to retry the downloads
        assert env.pageserver.log_contains(".*list prefixes.*failed, will retry.*")
        assert env.pageserver.log_contains(".*download.*failed, will retry.*")


num_connections = 10
num_rows = 100000

# Detach and re-attach tenant, while compute is busy running queries.
#
# Some of the queries may fail, in the window that the tenant has been
# detached but not yet re-attached. But Postgres itself should keep
# running, and when we retry the queries, they should start working
# after the attach has finished.


# FIXME:
#
# This is pretty unstable at the moment. I've seen it fail with a warning like this:
#
# AssertionError: assert not ['2023-01-05T13:09:40.708303Z  WARN remote_upload{tenant=c3fc41f6cf29a7626b90316e3518cd4b timeline=7978246f85faa71ab03...1282b/000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001716699-0000000001736681"\n']
#
# (https://neon-github-public-dev.s3.amazonaws.com/reports/pr-3232/debug/3846817847/index.html#suites/f9eba3cfdb71aa6e2b54f6466222829b/470fc62b5db7d7d7/)
# I believe that failure happened because there is a race condition
# between detach and starting remote upload tasks:
#
# 1. detach_timeline calls task_mgr::shutdown_tasks(), sending shutdown
#    signal to all in-progress tasks associated with the tenant.
# 2. Just after shutdown_tasks() has collected the list of tasks,
#    a new remote-upload task is spawned.
#
# See https://github.com/neondatabase/neon/issues/3273
#
#
# I also saw this failure:
#
# test_runner/regress/test_tenant_detach.py:194: in test_tenant_reattach_while_busy
#     asyncio.run(reattach_while_busy(env, pg, pageserver_http, tenant_id))
# /home/nonroot/.pyenv/versions/3.9.2/lib/python3.9/asyncio/runners.py:44: in run
#     return loop.run_until_complete(main)
# /home/nonroot/.pyenv/versions/3.9.2/lib/python3.9/asyncio/base_events.py:642: in run_until_complete
#     return future.result()
# test_runner/regress/test_tenant_detach.py:151: in reattach_while_busy
#     assert updates_finished == updates_to_perform
# E   assert 5010 == 10010
# E     +5010
# E     -10010
#
# I don't know what's causing that...
@pytest.mark.skip(reason="fixme")
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_tenant_reattach_while_busy(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    updates_started = 0
    updates_finished = 0
    updates_to_perform = 0

    # Run random UPDATEs on test table. On failure, try again.
    async def update_table(pg_conn: asyncpg.Connection):
        nonlocal updates_started, updates_finished, updates_to_perform

        while updates_started < updates_to_perform or updates_to_perform == 0:
            updates_started += 1
            id = random.randrange(1, num_rows)

            # Loop to retry until the UPDATE succeeds
            while True:
                try:
                    await pg_conn.fetchrow(f"UPDATE t SET counter = counter + 1 WHERE id = {id}")
                    updates_finished += 1
                    if updates_finished % 1000 == 0:
                        log.info(f"update {updates_finished} / {updates_to_perform}")
                    break
                except asyncpg.PostgresError as e:
                    # Received error from Postgres. Log it, sleep a little, and continue
                    log.info(f"UPDATE error: {e}")
                    await asyncio.sleep(0.1)

    async def sleep_and_reattach(pageserver_http: PageserverHttpClient, tenant_id: TenantId):
        nonlocal updates_started, updates_finished, updates_to_perform

        # Wait until we have performed some updates
        wait_until(20, 0.5, lambda: updates_finished > 500)

        log.info("Detaching tenant")
        pageserver_http.tenant_detach(tenant_id)
        await asyncio.sleep(1)
        log.info("Re-attaching tenant")
        pageserver_http.tenant_attach(tenant_id)
        log.info("Re-attach finished")

        # Continue with 5000 more updates
        updates_to_perform = updates_started + 5000

    # async guts of test_tenant_reattach_while_bysy test
    async def reattach_while_busy(
        env: NeonEnv, endpoint: Endpoint, pageserver_http: PageserverHttpClient, tenant_id: TenantId
    ):
        nonlocal updates_to_perform, updates_finished
        workers = []
        for _ in range(num_connections):
            pg_conn = await endpoint.connect_async()
            workers.append(asyncio.create_task(update_table(pg_conn)))

        workers.append(asyncio.create_task(sleep_and_reattach(pageserver_http, tenant_id)))
        await asyncio.gather(*workers)

        assert updates_finished == updates_to_perform

    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start()

    pageserver_http = env.pageserver.http_client()

    # create new nenant
    tenant_id, timeline_id = env.neon_cli.create_tenant(
        # Create layers aggressively
        conf={"checkpoint_distance": "100000"}
    )

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    cur = endpoint.connect().cursor()

    cur.execute("CREATE TABLE t(id int primary key, counter int)")
    cur.execute(f"INSERT INTO t SELECT generate_series(1,{num_rows}), 0")

    # Run the test
    asyncio.run(reattach_while_busy(env, endpoint, pageserver_http, tenant_id))

    # Verify table contents
    assert query_scalar(cur, "SELECT count(*) FROM t") == num_rows
    assert query_scalar(cur, "SELECT sum(counter) FROM t") == updates_to_perform


def test_tenant_detach_smoke(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.append(".*NotFound: Tenant .*")

    # first check for non existing tenant
    tenant_id = TenantId.generate()
    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"NotFound: tenant {tenant_id}",
    ) as excinfo:
        pageserver_http.tenant_detach(tenant_id)

    assert excinfo.value.status_code == 404

    # the error will be printed to the log too
    env.pageserver.allowed_errors.append(".*NotFound: tenant *")

    # create new nenant
    tenant_id, timeline_id = env.neon_cli.create_tenant()

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )

    # assert tenant exists on disk
    assert env.pageserver.tenant_dir(tenant_id).exists()

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    # we rely upon autocommit after each statement
    endpoint.safe_psql_many(
        queries=[
            "CREATE TABLE t(key int primary key, value text)",
            "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
        ]
    )

    # gc should not try to even start on a timeline that doesn't exist
    with pytest.raises(
        expected_exception=PageserverApiException, match="gc target timeline does not exist"
    ):
        bogus_timeline_id = TimelineId.generate()
        pageserver_http.timeline_gc(tenant_id, bogus_timeline_id, 0)

    # the error will be printed to the log too
    env.pageserver.allowed_errors.append(".*gc target timeline does not exist.*")
    # Timelines get stopped during detach, ignore the gc calls that error, witnessing that
    env.pageserver.allowed_errors.append(".*InternalServerError\\(timeline is Stopping.*")

    # Detach while running manual GC.
    # It should wait for manual GC to finish because it runs in a task associated with the tenant.
    pageserver_http.configure_failpoints(
        ("gc_iteration_internal_after_getting_gc_timelines", "return(2000)")
    )
    gc_thread = Thread(target=lambda: do_gc_target(pageserver_http, tenant_id, timeline_id))
    gc_thread.start()
    time.sleep(1)
    # By now the gc task is spawned but in sleep for another second due to the failpoint.

    log.info("detaching tenant")
    pageserver_http.tenant_detach(tenant_id)
    log.info("tenant detached without error")

    log.info("wait for gc thread to return")
    gc_thread.join(timeout=10)
    assert not gc_thread.is_alive()
    log.info("gc thread returned")

    # check that nothing is left on disk for deleted tenant
    assert not env.pageserver.tenant_dir(tenant_id).exists()

    with pytest.raises(
        expected_exception=PageserverApiException, match=f"NotFound: tenant {tenant_id}"
    ):
        pageserver_http.timeline_gc(tenant_id, timeline_id, 0)


# Creates and ignores a tenant, then detaches it: first, with no parameters (should fail),
# then with parameters to force ignored tenant detach (should not fail).
def test_tenant_detach_ignored_tenant(neon_simple_env: NeonEnv):
    env = neon_simple_env
    client = env.pageserver.http_client()

    # create a new tenant
    tenant_id, _ = env.neon_cli.create_tenant()

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )

    # assert tenant exists on disk
    assert env.pageserver.tenant_dir(tenant_id).exists()

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    # we rely upon autocommit after each statement
    endpoint.safe_psql_many(
        queries=[
            "CREATE TABLE t(key int primary key, value text)",
            "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
        ]
    )

    # ignore tenant
    client.tenant_ignore(tenant_id)
    env.pageserver.allowed_errors.append(".*NotFound: tenant .*")
    # ensure tenant couldn't be detached without the special flag for ignored tenant
    log.info("detaching ignored tenant WITHOUT required flag")
    with pytest.raises(
        expected_exception=PageserverApiException, match=f"NotFound: tenant {tenant_id}"
    ):
        client.tenant_detach(tenant_id)

    log.info("tenant detached failed as expected")

    # ensure tenant is detached with ignore state
    log.info("detaching ignored tenant with required flag")
    client.tenant_detach(tenant_id, True)
    log.info("ignored tenant detached without error")

    # check that nothing is left on disk for deleted tenant
    assert not env.pageserver.tenant_dir(tenant_id).exists()

    # assert the tenant does not exists in the Pageserver
    tenants_after_detach = [tenant["id"] for tenant in client.tenant_list()]
    assert (
        tenant_id not in tenants_after_detach
    ), f"Ignored and then detached tenant {tenant_id} \
        should not be present in pageserver's memory"


# Creates a tenant, and detaches it with extra parameter that forces ignored tenant detach.
# Tenant should be detached without issues.
def test_tenant_detach_regular_tenant(neon_simple_env: NeonEnv):
    env = neon_simple_env
    client = env.pageserver.http_client()

    # create a new tenant
    tenant_id, _ = env.neon_cli.create_tenant()

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )

    # assert tenant exists on disk
    assert env.pageserver.tenant_dir(tenant_id).exists()

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    # we rely upon autocommit after each statement
    endpoint.safe_psql_many(
        queries=[
            "CREATE TABLE t(key int primary key, value text)",
            "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
        ]
    )

    log.info("detaching regular tenant with detach ignored flag")
    client.tenant_detach(tenant_id, True)

    log.info("regular tenant detached without error")

    # check that nothing is left on disk for deleted tenant
    assert not env.pageserver.tenant_dir(tenant_id).exists()

    # assert the tenant does not exists in the Pageserver
    tenants_after_detach = [tenant["id"] for tenant in client.tenant_list()]
    assert (
        tenant_id not in tenants_after_detach
    ), f"Ignored and then detached tenant {tenant_id} \
        should not be present in pageserver's memory"


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_detach_while_attaching(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    with endpoint.cursor() as cur:
        cur.execute("CREATE TABLE foo (t text)")
        cur.execute(
            """
            INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
            """
        )
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # wait until pageserver receives that data
    wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

    # run checkpoint manually to be sure that data landed in remote storage
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    log.info("waiting for upload")

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(client, tenant_id, timeline_id, current_lsn)
    log.info("upload is done")

    # Detach it
    pageserver_http.tenant_detach(tenant_id)

    # And re-attach
    pageserver_http.configure_failpoints([("attach-before-activate", "return(5000)")])

    pageserver_http.tenant_attach(tenant_id)

    # Before it has chance to finish, detach it again
    pageserver_http.tenant_detach(tenant_id)

    # is there a better way to assert that failpoint triggered?
    time.sleep(10)

    # Attach it again. If the GC and compaction loops from the previous attach/detach
    # cycle are still running, things could get really confusing..
    pageserver_http.tenant_attach(tenant_id)

    with endpoint.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM foo")


# Tests that `ignore` and `get` operations' combination is able to remove and restore the tenant in pageserver's memory.
# * writes some data into tenant's timeline
# * ensures it's synced with the remote storage
# * `ignore` the tenant
# * verify that ignored tenant files are generally unchanged, only an ignored mark had appeared
# * verify the ignored tenant is gone from pageserver's memory
# * restart the pageserver and verify that ignored tenant is still not loaded
# * `load` the same tenant
# * ensure that it's status is `Active` and it's present in pageserver's memory with all timelines
def test_ignored_tenant_reattach(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    ignored_tenant_id, _ = env.neon_cli.create_tenant()
    tenant_dir = env.pageserver.tenant_dir(ignored_tenant_id)
    tenants_before_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_before_ignore.sort()
    timelines_before_ignore = [
        timeline["timeline_id"]
        for timeline in pageserver_http.timeline_list(tenant_id=ignored_tenant_id)
    ]
    files_before_ignore = [tenant_path for tenant_path in tenant_dir.glob("**/*")]

    # ignore the tenant and veirfy it's not present in pageserver replies, with its files still on disk
    pageserver_http.tenant_ignore(ignored_tenant_id)

    files_after_ignore_with_retain = [tenant_path for tenant_path in tenant_dir.glob("**/*")]
    new_files = set(files_after_ignore_with_retain) - set(files_before_ignore)
    disappeared_files = set(files_before_ignore) - set(files_after_ignore_with_retain)
    assert (
        len(disappeared_files) == 0
    ), f"Tenant ignore should not remove files from disk, missing: {disappeared_files}"
    assert (
        len(new_files) == 1
    ), f"Only tenant ignore file should appear on disk but got: {new_files}"

    tenants_after_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    assert ignored_tenant_id not in tenants_after_ignore, "Ignored tenant should be missing"
    assert len(tenants_after_ignore) + 1 == len(
        tenants_before_ignore
    ), "Only ignored tenant should be missing"

    # restart the pageserver to ensure we don't load the ignore timeline
    env.pageserver.stop()
    env.pageserver.start()
    tenants_after_restart = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_after_restart.sort()
    assert (
        tenants_after_restart == tenants_after_ignore
    ), "Ignored tenant should not be reloaded after pageserver restart"

    # now, load it from the local files and expect it works
    pageserver_http.tenant_load(tenant_id=ignored_tenant_id)
    wait_until_tenant_state(pageserver_http, ignored_tenant_id, "Active", 5)

    tenants_after_attach = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_after_attach.sort()
    assert tenants_after_attach == tenants_before_ignore, "Should have all tenants back"

    timelines_after_ignore = [
        timeline["timeline_id"]
        for timeline in pageserver_http.timeline_list(tenant_id=ignored_tenant_id)
    ]
    assert timelines_before_ignore == timelines_after_ignore, "Should have all timelines back"


# Tests that it's possible to `load` tenants with missing layers and get them restored:
# * writes some data into tenant's timeline
# * ensures it's synced with the remote storage
# * `ignore` the tenant
# * removes all timeline's local layers
# * `load` the same tenant
# * ensure that it's status is `Active`
# * check that timeline data is restored
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_ignored_tenant_download_missing_layers(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main")

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )

    data_id = 1
    data_secret = "very secret secret"
    insert_test_data(pageserver_http, tenant_id, timeline_id, data_id, data_secret, endpoint)

    tenants_before_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_before_ignore.sort()
    timelines_before_ignore = [
        timeline["timeline_id"] for timeline in pageserver_http.timeline_list(tenant_id=tenant_id)
    ]

    # ignore the tenant and remove its layers
    pageserver_http.tenant_ignore(tenant_id)
    timeline_dir = env.pageserver.timeline_dir(tenant_id, timeline_id)
    layers_removed = False
    for dir_entry in timeline_dir.iterdir():
        if dir_entry.name.startswith("00000"):
            # Looks like a layer file. Remove it
            dir_entry.unlink()
            layers_removed = True
    assert layers_removed, f"Found no layers for tenant {timeline_dir}"

    # now, load it from the local files and expect it to work due to remote storage restoration
    pageserver_http.tenant_load(tenant_id=tenant_id)
    wait_until_tenant_state(pageserver_http, tenant_id, "Active", 5)

    tenants_after_attach = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_after_attach.sort()
    assert tenants_after_attach == tenants_before_ignore, "Should have all tenants back"

    timelines_after_ignore = [
        timeline["timeline_id"] for timeline in pageserver_http.timeline_list(tenant_id=tenant_id)
    ]
    assert timelines_before_ignore == timelines_after_ignore, "Should have all timelines back"

    endpoint.stop()
    endpoint.start()
    ensure_test_data(data_id, data_secret, endpoint)


# Tests that it's possible to `load` broken tenants:
# * `ignore` a tenant
# * removes its `metadata` file locally
# * `load` the same tenant
# * ensure that it's status is `Broken`
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_ignored_tenant_stays_broken_without_metadata(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    env.endpoints.create_start("main")

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: (Broken|Stopping).*"
    )

    # ignore the tenant and remove its metadata
    pageserver_http.tenant_ignore(tenant_id)
    timeline_dir = env.pageserver.timeline_dir(tenant_id, timeline_id)
    metadata_removed = False
    for dir_entry in timeline_dir.iterdir():
        if dir_entry.name == "metadata":
            # Looks like a layer file. Remove it
            dir_entry.unlink()
            metadata_removed = True
    assert metadata_removed, f"Failed to find metadata file in {timeline_dir}"

    env.pageserver.allowed_errors.append(
        f".*{tenant_id}.*: load failed.*: failed to load metadata.*"
    )

    # now, load it from the local files and expect it to be broken due to inability to load tenant files into memory
    pageserver_http.tenant_load(tenant_id=tenant_id)
    wait_until_tenant_state(pageserver_http, tenant_id, "Broken", 5)


# Tests that attach is never working on a tenant, ignored or not, as long as it's not absent locally
# Similarly, tests that it's not possible to schedule a `load` for tenat that's not ignored.
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_load_attach_negatives(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    env.endpoints.create_start("main")

    tenant_id = env.initial_tenant

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )

    env.pageserver.allowed_errors.append(".*tenant .*? already exists, state:.*")
    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"tenant {tenant_id} already exists, state: Active",
    ):
        pageserver_http.tenant_load(tenant_id)

    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"tenant {tenant_id} already exists, state: Active",
    ):
        pageserver_http.tenant_attach(tenant_id)

    pageserver_http.tenant_ignore(tenant_id)

    env.pageserver.allowed_errors.append(".*tenant directory already exists.*")
    with pytest.raises(
        expected_exception=PageserverApiException,
        match="tenant directory already exists",
    ):
        pageserver_http.tenant_attach(tenant_id)


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_ignore_while_attaching(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main")

    pageserver_http = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.append(f".*Tenant {tenant_id} not found.*")
    env.pageserver.allowed_errors.append(
        f".*Tenant {tenant_id} will not become active\\. Current state: Stopping.*"
    )

    data_id = 1
    data_secret = "very secret secret"
    insert_test_data(pageserver_http, tenant_id, timeline_id, data_id, data_secret, endpoint)

    tenants_before_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]

    # Detach it
    pageserver_http.tenant_detach(tenant_id)
    # And re-attach, but stop attach task_mgr task from completing
    pageserver_http.configure_failpoints([("attach-before-activate", "return(5000)")])
    pageserver_http.tenant_attach(tenant_id)
    # Run ignore on the task, thereby cancelling the attach.
    # XXX This should take priority over attach, i.e., it should cancel the attach task.
    # But neither the failpoint, nor the proper remote_timeline_client download functions,
    # are sensitive to task_mgr::shutdown.
    # This problem is tracked in https://github.com/neondatabase/neon/issues/2996 .
    # So, for now, effectively, this ignore here will block until attach task completes.
    pageserver_http.tenant_ignore(tenant_id)

    # Cannot attach it due to some local files existing
    env.pageserver.allowed_errors.append(".*tenant directory already exists.*")
    with pytest.raises(
        expected_exception=PageserverApiException,
        match="tenant directory already exists",
    ):
        pageserver_http.tenant_attach(tenant_id)

    tenants_after_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    assert tenant_id not in tenants_after_ignore, "Ignored tenant should be missing"
    assert len(tenants_after_ignore) + 1 == len(
        tenants_before_ignore
    ), "Only ignored tenant should be missing"

    # But can load it from local files, that will restore attach.
    pageserver_http.tenant_load(tenant_id)

    wait_until_tenant_state(pageserver_http, tenant_id, "Active", 5)

    endpoint.stop()
    endpoint.start()
    ensure_test_data(data_id, data_secret, endpoint)


def insert_test_data(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    data_id: int,
    data: str,
    endpoint: Endpoint,
):
    with endpoint.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE test(id int primary key, secret text);
            INSERT INTO test VALUES ({data_id}, '{data}');
        """
        )
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # wait until pageserver receives that data
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)

    # run checkpoint manually to be sure that data landed in remote storage
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    log.info("waiting for to be ignored tenant data checkpoint upload")
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)


def ensure_test_data(data_id: int, data: str, endpoint: Endpoint):
    with endpoint.cursor() as cur:
        assert (
            query_scalar(cur, f"SELECT secret FROM test WHERE id = {data_id};") == data
        ), "Should have timeline data back"


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_metrics_while_ignoring_broken_tenant_and_reloading(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()

    client = env.pageserver.http_client()
    env.pageserver.allowed_errors.append(
        r".* Changing Active tenant to Broken state, reason: broken from test"
    )

    def only_int(samples: List[Sample]) -> Optional[int]:
        if len(samples) == 1:
            return int(samples[0].value)
        assert len(samples) == 0
        return None

    wait_until_tenant_state(client, env.initial_tenant, "Active", 10, 0.5)

    client.tenant_break(env.initial_tenant)

    found_broken = False
    active, broken, broken_set = ([], [], [])
    for _ in range(10):
        m = client.get_metrics()
        active = m.query_all("pageserver_tenant_states_count", {"state": "Active"})
        broken = m.query_all("pageserver_tenant_states_count", {"state": "Broken"})
        broken_set = m.query_all(
            "pageserver_broken_tenants_count", {"tenant_id": str(env.initial_tenant)}
        )
        found_broken = only_int(active) == 0 and only_int(broken) == 1 and only_int(broken_set) == 1

        if found_broken:
            break
        log.info(f"active: {active}, broken: {broken}, broken_set: {broken_set}")
        time.sleep(0.5)
    assert (
        found_broken
    ), f"tenant shows up as broken; active={active}, broken={broken}, broken_set={broken_set}"

    client.tenant_ignore(env.initial_tenant)

    found_broken = False
    broken, broken_set = ([], [])
    for _ in range(10):
        m = client.get_metrics()
        broken = m.query_all("pageserver_tenant_states_count", {"state": "Broken"})
        broken_set = m.query_all(
            "pageserver_broken_tenants_count", {"tenant_id": str(env.initial_tenant)}
        )
        found_broken = only_int(broken) == 0 and only_int(broken_set) == 1

        if found_broken:
            break
        time.sleep(0.5)
    assert (
        found_broken
    ), f"broken should still be in set, but it is not in the tenant state count: broken={broken}, broken_set={broken_set}"

    client.tenant_load(env.initial_tenant)

    found_active = False
    active, broken_set = ([], [])
    for _ in range(10):
        m = client.get_metrics()
        active = m.query_all("pageserver_tenant_states_count", {"state": "Active"})
        broken_set = m.query_all(
            "pageserver_broken_tenants_count", {"tenant_id": str(env.initial_tenant)}
        )
        found_active = only_int(active) == 1 and len(broken_set) == 0

        if found_active:
            break
        time.sleep(0.5)

    assert (
        found_active
    ), f"reloaded tenant should be active, and broken tenant set item removed: active={active}, broken_set={broken_set}"
