import os
import shutil
import threading
import time
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pytest
from fixtures.broker import NeonBroker
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import (
    assert_tenant_state,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_tenant_status_404,
)
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import (
    LocalFsStorage,
    RemoteStorageKind,
    available_remote_storages,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import (
    query_scalar,
    start_in_background,
    subprocess_capture,
    wait_until,
)


def assert_abs_margin_ratio(a: float, b: float, margin_ratio: float):
    assert abs(a - b) / a < margin_ratio, abs(a - b) / a


@contextmanager
def new_pageserver_service(
    new_pageserver_dir: Path,
    pageserver_bin: Path,
    remote_storage_mock_path: Path,
    pg_port: int,
    http_port: int,
    broker: Optional[NeonBroker],
    pg_distrib_dir: Path,
):
    """
    cannot use NeonPageserver yet because it depends on neon cli
    which currently lacks support for multiple pageservers
    """
    # actually run new pageserver
    cmd = [
        str(pageserver_bin),
        "--workdir",
        str(new_pageserver_dir),
        "--update-config",
        f"-c listen_pg_addr='localhost:{pg_port}'",
        f"-c listen_http_addr='localhost:{http_port}'",
        f"-c pg_distrib_dir='{pg_distrib_dir}'",
        "-c id=2",
        f"-c remote_storage={{local_path='{remote_storage_mock_path}'}}",
    ]
    if broker is not None:
        cmd.append(
            f"-c broker_endpoint='{broker.client_url()}'",
        )
    pageserver_client = PageserverHttpClient(
        port=http_port,
        auth_token=None,
        is_testing_enabled_or_skip=lambda: True,  # TODO: check if testing really enabled
    )
    try:
        pageserver_process = start_in_background(
            cmd, new_pageserver_dir, "pageserver.log", pageserver_client.check_status
        )
    except Exception as e:
        log.error(e)
        pageserver_process.kill()
        raise Exception(f"Failed to start pageserver as {cmd}, reason: {e}") from e

    log.info("new pageserver started")
    try:
        yield pageserver_process
    finally:
        log.info("stopping new pageserver")
        pageserver_process.kill()


@contextmanager
def pg_cur(endpoint):
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            yield cur


def load(endpoint: Endpoint, stop_event: threading.Event, load_ok_event: threading.Event):
    log.info("load started")

    inserted_ctr = 0
    failed = False
    while not stop_event.is_set():
        try:
            with pg_cur(endpoint) as cur:
                cur.execute("INSERT INTO load VALUES ('some payload')")
                inserted_ctr += 1
        except:  # noqa: E722
            if not failed:
                log.info("load failed")
            failed = True
            load_ok_event.clear()
        else:
            if failed:
                with pg_cur(endpoint) as cur:
                    # if we recovered after failure verify that we have correct number of rows
                    log.info("recovering at %s", inserted_ctr)
                    cur.execute("SELECT count(*) FROM load")
                    # it seems that sometimes transaction gets committed before we can acknowledge
                    # the result, so sometimes selected value is larger by one than we expect
                    assert cur.fetchone()[0] - inserted_ctr <= 1
                    log.info("successfully recovered %s", inserted_ctr)
                    failed = False
                    load_ok_event.set()
    log.info("load thread stopped")


def populate_branch(
    endpoint: Endpoint,
    tenant_id: TenantId,
    ps_http: PageserverHttpClient,
    create_table: bool,
    expected_sum: Optional[int],
) -> Tuple[TimelineId, Lsn]:
    # insert some data
    with pg_cur(endpoint) as cur:
        cur.execute("SHOW neon.timeline_id")
        timeline_id = TimelineId(cur.fetchone()[0])
        log.info("timeline to relocate %s", timeline_id)

        log.info(
            "pg_current_wal_flush_lsn(): %s",
            Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()")),
        )
        log.info(
            "timeline detail %s",
            ps_http.timeline_detail(tenant_id=tenant_id, timeline_id=timeline_id),
        )

        # we rely upon autocommit after each statement
        # as waiting for acceptors happens there
        if create_table:
            cur.execute("CREATE TABLE t(key int, value text)")
        cur.execute("INSERT INTO t SELECT generate_series(1,1000), 'some payload'")
        if expected_sum is not None:
            cur.execute("SELECT sum(key) FROM t")
            assert cur.fetchone() == (expected_sum,)
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

        return timeline_id, current_lsn


def ensure_checkpoint(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    current_lsn: Lsn,
):
    # run checkpoint manually to be sure that data landed in remote storage
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)


def check_timeline_attached(
    new_pageserver_http_client: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    old_timeline_detail: Dict[str, Any],
    old_current_lsn: Lsn,
):
    # new pageserver should be in sync (modulo wal tail or vacuum activity) with the old one because there was no new writes since checkpoint
    new_timeline_detail = new_pageserver_http_client.timeline_detail(tenant_id, timeline_id)

    # when load is active these checks can break because lsns are not static
    # so let's check with some margin
    assert_abs_margin_ratio(
        int(Lsn(new_timeline_detail["disk_consistent_lsn"])),
        int(Lsn(old_timeline_detail["disk_consistent_lsn"])),
        0.03,
    )

    assert_abs_margin_ratio(
        int(Lsn(new_timeline_detail["disk_consistent_lsn"])), int(old_current_lsn), 0.03
    )


def switch_pg_to_new_pageserver(
    env: NeonEnv,
    endpoint: Endpoint,
    new_pageserver_port: int,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Path:
    endpoint.stop()

    pg_config_file_path = Path(endpoint.config_file_path())
    pg_config_file_path.open("a").write(
        f"\nneon.pageserver_connstring = 'postgresql://no_user:@localhost:{new_pageserver_port}'"
    )

    endpoint.start()

    timeline_to_detach_local_path = env.pageserver.timeline_dir(tenant_id, timeline_id)
    files_before_detach = os.listdir(timeline_to_detach_local_path)
    assert (
        "metadata" in files_before_detach
    ), f"Regular timeline {timeline_to_detach_local_path} should have the metadata file,\
            but got: {files_before_detach}"
    assert (
        len(files_before_detach) >= 2
    ), f"Regular timeline {timeline_to_detach_local_path} should have at least one layer file,\
            but got {files_before_detach}"

    return timeline_to_detach_local_path


def post_migration_check(endpoint: Endpoint, sum_before_migration: int, old_local_path: Path):
    with pg_cur(endpoint) as cur:
        # check that data is still there
        cur.execute("SELECT sum(key) FROM t")
        assert cur.fetchone() == (sum_before_migration,)
        # check that we can write new data
        cur.execute("INSERT INTO t SELECT generate_series(1001,2000), 'some payload'")
        cur.execute("SELECT sum(key) FROM t")
        assert cur.fetchone() == (sum_before_migration + 1500500,)

    assert not os.path.exists(
        old_local_path
    ), f"After detach, local timeline dir {old_local_path} should be removed"


@pytest.mark.parametrize(
    "method",
    [
        # A minor migration involves no storage breaking changes.
        # It is done by attaching the tenant to a new pageserver.
        "minor",
        # A major migration involves exporting a postgres datadir
        # basebackup and importing it into the new pageserver.
        # This kind of migration can tolerate breaking changes
        # to storage format
        "major",
    ],
)
@pytest.mark.parametrize("with_load", ["with_load", "without_load"])
def test_tenant_relocation(
    neon_env_builder: NeonEnvBuilder,
    port_distributor: PortDistributor,
    test_output_dir: Path,
    neon_binpath: Path,
    base_dir: Path,
    method: str,
    with_load: str,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()

    tenant_id = TenantId("74ee8b079a0e437eb0afea7d26a07209")

    # FIXME: Is this expected?
    env.pageserver.allowed_errors.append(
        ".*init_tenant_mgr: marking .* as locally complete, while it doesn't exist in remote index.*"
    )

    # Needed for detach polling.
    env.pageserver.allowed_errors.append(f".*NotFound: tenant {tenant_id}.*")

    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)
    remote_storage_mock_path = env.pageserver_remote_storage.root

    # we use two branches to check that they are both relocated
    # first branch is used for load, compute for second one is used to
    # check that data is not lost

    pageserver_http = env.pageserver.http_client()

    _, initial_timeline_id = env.neon_cli.create_tenant(tenant_id)
    log.info("tenant to relocate %s initial_timeline_id %s", tenant_id, initial_timeline_id)

    env.neon_cli.create_branch("test_tenant_relocation_main", tenant_id=tenant_id)
    ep_main = env.endpoints.create_start(
        branch_name="test_tenant_relocation_main", tenant_id=tenant_id
    )

    timeline_id_main, current_lsn_main = populate_branch(
        ep_main,
        tenant_id=tenant_id,
        ps_http=pageserver_http,
        create_table=True,
        expected_sum=500500,
    )

    env.neon_cli.create_branch(
        new_branch_name="test_tenant_relocation_second",
        ancestor_branch_name="test_tenant_relocation_main",
        ancestor_start_lsn=current_lsn_main,
        tenant_id=tenant_id,
    )
    ep_second = env.endpoints.create_start(
        branch_name="test_tenant_relocation_second", tenant_id=tenant_id
    )

    timeline_id_second, current_lsn_second = populate_branch(
        ep_second,
        tenant_id=tenant_id,
        ps_http=pageserver_http,
        create_table=False,
        expected_sum=1001000,
    )

    # wait until pageserver receives that data
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id_main, current_lsn_main)
    timeline_detail_main = pageserver_http.timeline_detail(tenant_id, timeline_id_main)

    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id_second, current_lsn_second)
    timeline_detail_second = pageserver_http.timeline_detail(tenant_id, timeline_id_second)

    if with_load == "with_load":
        # create load table
        with pg_cur(ep_main) as cur:
            cur.execute("CREATE TABLE load(value text)")

        load_stop_event = threading.Event()
        load_ok_event = threading.Event()
        load_thread = threading.Thread(
            target=load,
            args=(ep_main, load_stop_event, load_ok_event),
            daemon=True,  # To make sure the child dies when the parent errors
        )
        load_thread.start()

    # this requirement introduces a problem
    # if user creates a branch during migration
    # it won't appear on the new pageserver
    ensure_checkpoint(
        pageserver_http=pageserver_http,
        tenant_id=tenant_id,
        timeline_id=timeline_id_main,
        current_lsn=current_lsn_main,
    )

    ensure_checkpoint(
        pageserver_http=pageserver_http,
        tenant_id=tenant_id,
        timeline_id=timeline_id_second,
        current_lsn=current_lsn_second,
    )

    log.info("initializing new pageserver")
    # bootstrap second pageserver
    new_pageserver_dir = env.repo_dir / "new_pageserver"
    new_pageserver_dir.mkdir()

    new_pageserver_pg_port = port_distributor.get_port()
    new_pageserver_http_port = port_distributor.get_port()
    log.info("new pageserver ports pg %s http %s", new_pageserver_pg_port, new_pageserver_http_port)
    pageserver_bin = neon_binpath / "pageserver"

    new_pageserver_http = PageserverHttpClient(
        port=new_pageserver_http_port,
        auth_token=None,
        is_testing_enabled_or_skip=env.pageserver.is_testing_enabled_or_skip,
    )

    with new_pageserver_service(
        new_pageserver_dir,
        pageserver_bin,
        remote_storage_mock_path,
        new_pageserver_pg_port,
        new_pageserver_http_port,
        neon_env_builder.broker,
        neon_env_builder.pg_distrib_dir,
    ):
        # Migrate either by attaching from s3 or import/export basebackup
        if method == "major":
            cmd = [
                "poetry",
                "run",
                "python",
                str(base_dir / "scripts/export_import_between_pageservers.py"),
                "--tenant-id",
                str(tenant_id),
                "--from-host",
                "localhost",
                "--from-http-port",
                str(pageserver_http.port),
                "--from-pg-port",
                str(env.pageserver.service_port.pg),
                "--to-host",
                "localhost",
                "--to-http-port",
                str(new_pageserver_http_port),
                "--to-pg-port",
                str(new_pageserver_pg_port),
                "--pg-distrib-dir",
                str(neon_env_builder.pg_distrib_dir),
                "--work-dir",
                str(test_output_dir),
                "--tmp-pg-port",
                str(port_distributor.get_port()),
            ]
            subprocess_capture(test_output_dir, cmd, check=True)
        elif method == "minor":
            # call to attach timeline to new pageserver
            new_pageserver_http.tenant_attach(tenant_id)

            # wait for tenant to finish attaching
            wait_until(
                number_of_iterations=10,
                interval=1,
                func=lambda: assert_tenant_state(new_pageserver_http, tenant_id, "Active"),
            )

            check_timeline_attached(
                new_pageserver_http,
                tenant_id,
                timeline_id_main,
                timeline_detail_main,
                current_lsn_main,
            )

            check_timeline_attached(
                new_pageserver_http,
                tenant_id,
                timeline_id_second,
                timeline_detail_second,
                current_lsn_second,
            )

        # rewrite neon cli config to use new pageserver for basebackup to start new compute
        lines = (env.repo_dir / "config").read_text().splitlines()
        for i, line in enumerate(lines):
            if line.startswith("listen_http_addr"):
                lines[i] = f"listen_http_addr = 'localhost:{new_pageserver_http_port}'"
            if line.startswith("listen_pg_addr"):
                lines[i] = f"listen_pg_addr = 'localhost:{new_pageserver_pg_port}'"
        (env.repo_dir / "config").write_text("\n".join(lines))

        old_local_path_main = switch_pg_to_new_pageserver(
            env,
            ep_main,
            new_pageserver_pg_port,
            tenant_id,
            timeline_id_main,
        )

        old_local_path_second = switch_pg_to_new_pageserver(
            env,
            ep_second,
            new_pageserver_pg_port,
            tenant_id,
            timeline_id_second,
        )

        # detach tenant from old pageserver before we check
        # that all the data is there to be sure that old pageserver
        # is no longer involved, and if it is, we will see the error
        pageserver_http.tenant_detach(tenant_id)

        # Wait a little, so that the detach operation has time to finish.
        wait_tenant_status_404(pageserver_http, tenant_id, iterations=100, interval=1)

        post_migration_check(ep_main, 500500, old_local_path_main)
        post_migration_check(ep_second, 1001000, old_local_path_second)

        # ensure that we can successfully read all relations on the new pageserver
        with pg_cur(ep_second) as cur:
            cur.execute(
                """
                DO $$
                DECLARE
                r RECORD;
                BEGIN
                FOR r IN
                SELECT relname FROM pg_class WHERE relkind='r'
                LOOP
                    RAISE NOTICE '%', r.relname;
                    EXECUTE 'SELECT count(*) FROM quote_ident($1)' USING r.relname;
                END LOOP;
                END$$;
                """
            )

        if with_load == "with_load":
            assert load_ok_event.wait(3)
            log.info("stopping load thread")
            load_stop_event.set()
            load_thread.join(timeout=10)
            log.info("load thread stopped")

        # bring old pageserver back for clean shutdown via neon cli
        # new pageserver will be shut down by the context manager
        lines = (env.repo_dir / "config").read_text().splitlines()
        for i, line in enumerate(lines):
            if line.startswith("listen_http_addr"):
                lines[i] = f"listen_http_addr = 'localhost:{env.pageserver.service_port.http}'"
            if line.startswith("listen_pg_addr"):
                lines[i] = f"listen_pg_addr = 'localhost:{env.pageserver.service_port.pg}'"
        (env.repo_dir / "config").write_text("\n".join(lines))


# Simulate hard crash of pageserver and re-attach a tenant with a branch
#
# This exercises a race condition after tenant attach, where the
# branch point on the ancestor timeline is greater than the ancestor's
# last-record LSN. We had a bug where GetPage incorrectly followed the
# timeline to the ancestor without waiting for the missing WAL to
# arrive.
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_emergency_relocate_with_branches_slow_replay(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    env.pageserver.is_testing_enabled_or_skip()
    pageserver_http = env.pageserver.http_client()

    # Prepare for the test:
    #
    # - Main branch, with a table and two inserts to it.
    # - A logical replication message between the inserts, so that we can conveniently
    #   pause the WAL ingestion between the two inserts.
    # - Child branch, created after the inserts
    tenant_id, _ = env.neon_cli.create_tenant()

    main_endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    with main_endpoint.cursor() as cur:
        cur.execute("CREATE TABLE test_reattach (t text)")
        cur.execute("INSERT INTO test_reattach VALUES ('before pause')")

        cur.execute("SELECT pg_logical_emit_message(false, 'neon-test', 'between inserts')")

        cur.execute("INSERT INTO test_reattach VALUES ('after pause')")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    main_endpoint.stop()
    env.neon_cli.create_branch("child", tenant_id=tenant_id, ancestor_start_lsn=current_lsn)

    # Now kill the pageserver, remove the tenant directory, and restart. This simulates
    # the scenario that a pageserver dies unexpectedly and cannot be recovered, so we relocate
    # the tenant to a different pageserver. We reuse the same pageserver because it's
    # simpler than initializing a new one from scratch, but the effect on the single tenant
    # is the same.
    env.pageserver.stop(immediate=True)
    shutil.rmtree(env.pageserver.tenant_dir(tenant_id))
    env.pageserver.start()

    # This fail point will pause the WAL ingestion on the main branch, after the
    # the first insert
    pageserver_http.configure_failpoints([("wal-ingest-logical-message-sleep", "return(5000)")])

    # Attach and wait a few seconds to give it time to load the tenants, attach to the
    # safekeepers, and to stream and ingest the WAL up to the pause-point.
    before_attach_time = time.time()
    pageserver_http.tenant_attach(tenant_id)
    time.sleep(3)

    # The wal ingestion on the main timeline should now be paused at the fail point.
    # Run a query on the child branch. The GetPage requests for this should recurse to the
    # parent timeline, and wait for the WAL to be ingested there. Otherwise it won't see
    # the second insert.
    child_endpoint = env.endpoints.create_start("child", tenant_id=tenant_id)
    with child_endpoint.cursor() as cur:
        cur.execute("SELECT * FROM test_reattach")
        assert cur.fetchall() == [("before pause",), ("after pause",)]

    # Sanity check that the failpoint was reached
    assert env.pageserver.log_contains('failpoint "wal-ingest-logical-message-sleep": sleep done')
    assert time.time() - before_attach_time > 5

    # Clean up
    pageserver_http.configure_failpoints(("wal-ingest-logical-message-sleep", "off"))


# Simulate hard crash of pageserver and re-attach a tenant with a branch
#
# This exercises the same race condition after as
# 'test_emergency_relocate_with_branches_slow_replay', but this test case
# is closer to the original scenario where we originally found the
# issue.
#
# In this scenario, the incorrect result to get-request leads to
# *permanent damage* in the child timeline, because ingesting the WAL
# on the child timeline depended on incorrect view of the parent. This
# test reproduced one such case; the symptom was an error on the child, when
# trying to connect to the child endpoint after re-attaching the tenant:
#
# FATAL: database "neondb" does not exist
#
# In the original case where we bumped into this, the error was slightly
# different:
#
# FATAL:  "base/16385" is not a valid data directory
# DETAIL:  File "base/16385/PG_VERSION" is missing.
#
### Detailed explanation of the original bug and why it lead to that error:
#
# The WAL on the main and the child branches look like this:
#
#    Main                                  Child
# 1. CREATE DATABASE
#           <child branch is created>
# 2. CREATE TABLE AS SELECT ...            3. CREATE TABLE AS SELECT ...
#
# None of these WAL records have been flushed to disk or uploaded to remote
# storage in the pageserver yet, when the tenant is detached.
#
# After detach and re-attach, a walreceiver is spawned on both timelines.
# They will connect to the safekeepers and start ingesting the WAL
# from their respective IndexParts' `disk_consistent_lsn` onward.
#
# The bug occurs if the child branch's walreceiver runs before the
# main's.  It receives the SMGR_CREATE WAL record emitted by the
# CREATE TABLE statement (3.), and applies it, without seeing the
# effect of the CREATE DATABASE statement.
#
# To understand why that leads to a 'File "base/16385/PG_VERSION" is
# missing' error, let's look at what the handlers for the WAL records
# do:
#
# CREATE DATABASE WAL record is handled by ingest_xlog_dbase_create:
#
#    ingest_xlog_dbase_create:
#     put_relmap_file()
#       // NOTE 'true': It means that there is a relmapper and PG_VERSION file
# 1:    let r = dbdir.dbdirs.insert((spcnode, dbnode), true);
#
#
# CREATE TABLE emits an SMGR_CREATE record, which is handled by:
#
#    ingest_xlog_smgr_create:
#      put_rel_creation:
#        ...
#        let mut rel_dir = if dbdir.dbdirs.get(&(rel.spcnode, rel.dbnode)).is_none() {
# 2:         // Didn't exist. Update dbdir
#            dbdir.dbdirs.insert((rel.spcnode, rel.dbnode), false);
#            let buf = DbDirectory::ser(&dbdir)?;
#            self.put(DBDIR_KEY, Value::Image(buf.into()));
#
#            // and create the RelDirectory
#            RelDirectory::default()
#        } else {
# 3:         // reldir already exists, fetch it
#            RelDirectory::des(&self.get(rel_dir_key, ctx).await?)?
#        };
#
#
# In the correct ordering, the SMGR_CREATE record is applied after the
# CREATE DATABASE record. The CREATE DATABASE creates the entry in the
# 'dbdir', with the 'true' flag that indicates that PG_VERSION exists
# (1). The SMGR_CREATE handler calls put_rel_creation, which finds the
# dbdir entry that the CREATE DATABASE record created, and takes the
# "reldir already exists, fetch it" else-branch at the if statement (3).
#
# In the incorrect ordering, the child walreceiver applies the
# SMGR_CREATE record without seeing the effects of the CREATE
# DATABASE. In that case, put_rel_creation takes the "Didn't
# exist. Update dbir" path (2), and inserts an entry in the
# DbDirectory with 'false' to indicate there is no PG_VERSION file.
#
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_emergency_relocate_with_branches_createdb(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # create new nenant
    tenant_id, _ = env.neon_cli.create_tenant()

    main_endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    with main_endpoint.cursor() as cur:
        cur.execute("SELECT pg_logical_emit_message(false, 'neon-test', 'between inserts')")

        cur.execute("CREATE DATABASE neondb")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
    env.neon_cli.create_branch("child", tenant_id=tenant_id, ancestor_start_lsn=current_lsn)

    with main_endpoint.cursor(dbname="neondb") as cur:
        cur.execute("CREATE TABLE test_migrate_one AS SELECT generate_series(1,100)")
    main_endpoint.stop()

    child_endpoint = env.endpoints.create_start("child", tenant_id=tenant_id)
    with child_endpoint.cursor(dbname="neondb") as cur:
        cur.execute("CREATE TABLE test_migrate_one AS SELECT generate_series(1,200)")
    child_endpoint.stop()

    # Kill the pageserver, remove the tenant directory, and restart
    env.pageserver.stop(immediate=True)
    shutil.rmtree(env.pageserver.tenant_dir(tenant_id))
    env.pageserver.start()

    # Wait before ingesting the WAL for CREATE DATABASE on the main branch. The original
    # bug reproduced easily even without this, as there is always some delay between
    # loading the timeline and establishing the connection to the safekeeper to stream and
    # ingest the WAL, but let's make this less dependent on accidental timing.
    pageserver_http.configure_failpoints([("wal-ingest-logical-message-sleep", "return(5000)")])
    before_attach_time = time.time()
    pageserver_http.tenant_attach(tenant_id)

    child_endpoint.start()
    with child_endpoint.cursor(dbname="neondb") as cur:
        assert query_scalar(cur, "SELECT count(*) FROM test_migrate_one") == 200

    # Sanity check that the failpoint was reached
    assert env.pageserver.log_contains('failpoint "wal-ingest-logical-message-sleep": sleep done')
    assert time.time() - before_attach_time > 5

    # Clean up
    pageserver_http.configure_failpoints(("wal-ingest-logical-message-sleep", "off"))
