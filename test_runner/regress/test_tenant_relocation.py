import os
import threading
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Etcd,
    NeonEnv,
    NeonEnvBuilder,
    PageserverHttpClient,
    PortDistributor,
    Postgres,
    assert_no_in_progress_downloads_for_tenant,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar, start_in_background, subprocess_capture, wait_until


def assert_abs_margin_ratio(a: float, b: float, margin_ratio: float):
    assert abs(a - b) / a < margin_ratio, abs(a - b) / a


@contextmanager
def new_pageserver_service(
    new_pageserver_dir: Path,
    pageserver_bin: Path,
    remote_storage_mock_path: Path,
    pg_port: int,
    http_port: int,
    broker: Optional[Etcd],
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
            f"-c broker_endpoints=['{broker.client_url()}']",
        )
    pageserver_client = PageserverHttpClient(
        port=http_port,
        auth_token=None,
    )
    try:
        pageserver_process = start_in_background(
            cmd, new_pageserver_dir, "pageserver.log", pageserver_client.check_status
        )
    except Exception as e:
        log.error(e)
        pageserver_process.kill()
        raise Exception(f"Failed to start pageserver as {cmd}, reason: {e}")

    log.info("new pageserver started")
    try:
        yield pageserver_process
    finally:
        log.info("stopping new pageserver")
        pageserver_process.kill()


@contextmanager
def pg_cur(pg):
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            yield cur


def load(pg: Postgres, stop_event: threading.Event, load_ok_event: threading.Event):
    log.info("load started")

    inserted_ctr = 0
    failed = False
    while not stop_event.is_set():
        try:
            with pg_cur(pg) as cur:
                cur.execute("INSERT INTO load VALUES ('some payload')")
                inserted_ctr += 1
        except:  # noqa: E722
            if not failed:
                log.info("load failed")
            failed = True
            load_ok_event.clear()
        else:
            if failed:
                with pg_cur(pg) as cur:
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
    pg: Postgres,
    tenant_id: TenantId,
    ps_http: PageserverHttpClient,
    create_table: bool,
    expected_sum: Optional[int],
) -> Tuple[TimelineId, Lsn]:
    # insert some data
    with pg_cur(pg) as cur:
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
    pg: Postgres,
    new_pageserver_port: int,
    tenant_id: TenantId,
    timeline_id: TimelineId,
) -> Path:
    pg.stop()

    pg_config_file_path = Path(pg.config_file_path())
    pg_config_file_path.open("a").write(
        f"\nneon.pageserver_connstring = 'postgresql://no_user:@localhost:{new_pageserver_port}'"
    )

    pg.start()

    timeline_to_detach_local_path = (
        env.repo_dir / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)
    )
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


def post_migration_check(pg: Postgres, sum_before_migration: int, old_local_path: Path):
    with pg_cur(pg) as cur:
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
    neon_env_builder.enable_local_fs_remote_storage()

    env = neon_env_builder.init_start()

    # FIXME: Is this expected?
    env.pageserver.allowed_errors.append(
        ".*init_tenant_mgr: marking .* as locally complete, while it doesnt exist in remote index.*"
    )

    # create folder for remote storage mock
    remote_storage_mock_path = env.repo_dir / "local_fs_remote_storage"

    # we use two branches to check that they are both relocated
    # first branch is used for load, compute for second one is used to
    # check that data is not lost

    pageserver_http = env.pageserver.http_client()

    tenant_id, initial_timeline_id = env.neon_cli.create_tenant(
        TenantId("74ee8b079a0e437eb0afea7d26a07209")
    )
    log.info("tenant to relocate %s initial_timeline_id %s", tenant_id, initial_timeline_id)

    env.neon_cli.create_branch("test_tenant_relocation_main", tenant_id=tenant_id)
    pg_main = env.postgres.create_start(
        branch_name="test_tenant_relocation_main", tenant_id=tenant_id
    )

    timeline_id_main, current_lsn_main = populate_branch(
        pg_main,
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
    pg_second = env.postgres.create_start(
        branch_name="test_tenant_relocation_second", tenant_id=tenant_id
    )

    timeline_id_second, current_lsn_second = populate_branch(
        pg_second,
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
        with pg_cur(pg_main) as cur:
            cur.execute("CREATE TABLE load(value text)")

        load_stop_event = threading.Event()
        load_ok_event = threading.Event()
        load_thread = threading.Thread(
            target=load,
            args=(pg_main, load_stop_event, load_ok_event),
            daemon=True,  # To make sure the child dies when the parent errors
        )
        load_thread.start()

    # this requirement introduces a problem
    # if user creates a branch during migration
    # it wont appear on the new pageserver
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

    log.info("inititalizing new pageserver")
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

            # check that it shows that download is in progress
            tenant_status = new_pageserver_http.tenant_status(tenant_id=tenant_id)
            assert tenant_status.get("has_in_progress_downloads"), tenant_status

            # wait until tenant is downloaded
            wait_until(
                number_of_iterations=10,
                interval=1,
                func=lambda: assert_no_in_progress_downloads_for_tenant(
                    new_pageserver_http, tenant_id
                ),
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
        cli_config_lines = (env.repo_dir / "config").read_text().splitlines()
        cli_config_lines[-2] = f"listen_http_addr = 'localhost:{new_pageserver_http_port}'"
        cli_config_lines[-1] = f"listen_pg_addr = 'localhost:{new_pageserver_pg_port}'"
        (env.repo_dir / "config").write_text("\n".join(cli_config_lines))

        old_local_path_main = switch_pg_to_new_pageserver(
            env,
            pg_main,
            new_pageserver_pg_port,
            tenant_id,
            timeline_id_main,
        )

        old_local_path_second = switch_pg_to_new_pageserver(
            env,
            pg_second,
            new_pageserver_pg_port,
            tenant_id,
            timeline_id_second,
        )

        # detach tenant from old pageserver before we check
        # that all the data is there to be sure that old pageserver
        # is no longer involved, and if it is, we will see the errors
        pageserver_http.tenant_detach(tenant_id)

        post_migration_check(pg_main, 500500, old_local_path_main)
        post_migration_check(pg_second, 1001000, old_local_path_second)

        # ensure that we can successfully read all relations on the new pageserver
        with pg_cur(pg_second) as cur:
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
        cli_config_lines = (env.repo_dir / "config").read_text().splitlines()
        cli_config_lines[-2] = f"listen_http_addr = 'localhost:{env.pageserver.service_port.http}'"
        cli_config_lines[-1] = f"listen_pg_addr = 'localhost:{env.pageserver.service_port.pg}'"
        (env.repo_dir / "config").write_text("\n".join(cli_config_lines))
