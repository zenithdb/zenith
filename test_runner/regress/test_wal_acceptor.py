import os
import pathlib
import random
import shutil
import signal
import subprocess
import sys
import threading
import time
from contextlib import closing
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, List, Optional

import psycopg2
import psycopg2.errors
import psycopg2.extras
import pytest
from fixtures.broker import NeonBroker
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    NeonPageserver,
    PgBin,
    PgProtocol,
    Safekeeper,
    SafekeeperHttpClient,
    SafekeeperPort,
)
from fixtures.pageserver.utils import (
    timeline_delete_wait_completed,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import (
    RemoteStorageKind,
    available_remote_storages,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import get_dir_size, query_scalar, start_in_background


def wait_lsn_force_checkpoint(
    tenant_id: TenantId,
    timeline_id: TimelineId,
    endpoint: Endpoint,
    ps: NeonPageserver,
    pageserver_conn_options=None,
):
    pageserver_conn_options = pageserver_conn_options or {}
    lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
    log.info(f"pg_current_wal_flush_lsn is {lsn}, waiting for it on pageserver")

    auth_token = None
    if "password" in pageserver_conn_options:
        auth_token = pageserver_conn_options["password"]

    # wait for the pageserver to catch up
    wait_for_last_record_lsn(
        ps.http_client(auth_token=auth_token),
        tenant_id,
        timeline_id,
        lsn,
    )

    # force checkpoint to advance remote_consistent_lsn
    ps.http_client(auth_token).timeline_checkpoint(tenant_id, timeline_id)

    # ensure that remote_consistent_lsn is advanced
    wait_for_upload(
        ps.http_client(auth_token=auth_token),
        tenant_id,
        timeline_id,
        lsn,
    )


@dataclass
class TimelineMetrics:
    timeline_id: TimelineId
    last_record_lsn: Lsn
    # One entry per each Safekeeper, order is the same
    flush_lsns: List[Lsn] = field(default_factory=list)
    commit_lsns: List[Lsn] = field(default_factory=list)


# Run page server and multiple acceptors, and multiple compute nodes running
# against different timelines.
def test_many_timelines(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    n_timelines = 3

    branch_names = [
        "test_safekeepers_many_timelines_{}".format(tlin) for tlin in range(n_timelines)
    ]
    # pageserver, safekeeper operate timelines via their ids (can be represented in hex as 'ad50847381e248feaac9876cc71ae418')
    # that's not really human readable, so the branch names are introduced in Neon CLI.
    # Neon CLI stores its branch <-> timeline mapping in its internals,
    # but we need this to collect metrics from other servers, related to the timeline.
    branch_names_to_timeline_ids = {}

    # start postgres on each timeline
    endpoints = []
    for branch_name in branch_names:
        new_timeline_id = env.neon_cli.create_branch(branch_name)
        endpoints.append(env.endpoints.create_start(branch_name))
        branch_names_to_timeline_ids[branch_name] = new_timeline_id

    tenant_id = env.initial_tenant

    def collect_metrics(message: str) -> List[TimelineMetrics]:
        with env.pageserver.http_client() as pageserver_http:
            timeline_details = [
                pageserver_http.timeline_detail(
                    tenant_id=tenant_id, timeline_id=branch_names_to_timeline_ids[branch_name]
                )
                for branch_name in branch_names
            ]
        # All changes visible to pageserver (last_record_lsn) should be
        # confirmed by safekeepers first. As we cannot atomically get
        # state of both pageserver and safekeepers, we should start with
        # pageserver. Looking at outdated data from pageserver is ok.
        # Asking safekeepers first is not ok because new commits may arrive
        # to both safekeepers and pageserver after we've already obtained
        # safekeepers' state, it will look contradictory.
        sk_metrics = [sk.http_client().get_metrics() for sk in env.safekeepers]

        timeline_metrics = []
        for timeline_detail in timeline_details:
            timeline_id = TimelineId(timeline_detail["timeline_id"])

            m = TimelineMetrics(
                timeline_id=timeline_id,
                last_record_lsn=Lsn(timeline_detail["last_record_lsn"]),
            )
            for sk_m in sk_metrics:
                m.flush_lsns.append(Lsn(sk_m.flush_lsn_inexact[(tenant_id, timeline_id)]))
                m.commit_lsns.append(Lsn(sk_m.commit_lsn_inexact[(tenant_id, timeline_id)]))

            for flush_lsn, commit_lsn in zip(m.flush_lsns, m.commit_lsns):
                # Invariant. May be < when transaction is in progress.
                assert (
                    commit_lsn <= flush_lsn
                ), f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            # We only call collect_metrics() after a transaction is confirmed by
            # the compute node, which only happens after a consensus of safekeepers
            # has confirmed the transaction. We assume majority consensus here.
            assert (
                2 * sum(m.last_record_lsn <= lsn for lsn in m.flush_lsns)
                > neon_env_builder.num_safekeepers
            ), f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            assert (
                2 * sum(m.last_record_lsn <= lsn for lsn in m.commit_lsns)
                > neon_env_builder.num_safekeepers
            ), f"timeline_id={timeline_id}, timeline_detail={timeline_detail}, sk_metrics={sk_metrics}"
            timeline_metrics.append(m)
        log.info(f"{message}: {timeline_metrics}")
        return timeline_metrics

    # TODO: https://github.com/neondatabase/neon/issues/809
    # collect_metrics("before CREATE TABLE")

    # Do everything in different loops to have actions on different timelines
    # interleaved.
    # create schema
    for endpoint in endpoints:
        endpoint.safe_psql("CREATE TABLE t(key int primary key, value text)")
    init_m = collect_metrics("after CREATE TABLE")

    # Populate data for 2/3 timelines
    class MetricsChecker(threading.Thread):
        def __init__(self) -> None:
            super().__init__(daemon=True)
            self.should_stop = threading.Event()
            self.exception: Optional[BaseException] = None

        def run(self) -> None:
            try:
                while not self.should_stop.is_set():
                    collect_metrics("during INSERT INTO")
                    time.sleep(1)
            except:  # noqa: E722
                log.error(
                    "MetricsChecker's thread failed, the test will be failed on .stop() call",
                    exc_info=True,
                )
                # We want to preserve traceback as well as the exception
                exc_type, exc_value, exc_tb = sys.exc_info()
                assert exc_type
                e = exc_type(exc_value)
                e.__traceback__ = exc_tb
                self.exception = e

        def stop(self) -> None:
            self.should_stop.set()
            self.join()
            if self.exception:
                raise self.exception

    metrics_checker = MetricsChecker()
    metrics_checker.start()

    for endpoint in endpoints[:-1]:
        endpoint.safe_psql("INSERT INTO t SELECT generate_series(1,100000), 'payload'")

    metrics_checker.stop()

    collect_metrics("after INSERT INTO")

    # Check data for 2/3 timelines
    for endpoint in endpoints[:-1]:
        res = endpoint.safe_psql("SELECT sum(key) FROM t")
        assert res[0] == (5000050000,)

    final_m = collect_metrics("after SELECT")
    # Assume that LSNs (a) behave similarly in all timelines; and (b) INSERT INTO alters LSN significantly.
    # Also assume that safekeepers will not be significantly out of sync in this test.
    middle_lsn = Lsn((int(init_m[0].last_record_lsn) + int(final_m[0].last_record_lsn)) // 2)
    assert max(init_m[0].flush_lsns) < middle_lsn < min(final_m[0].flush_lsns)
    assert max(init_m[0].commit_lsns) < middle_lsn < min(final_m[0].commit_lsns)
    assert max(init_m[1].flush_lsns) < middle_lsn < min(final_m[1].flush_lsns)
    assert max(init_m[1].commit_lsns) < middle_lsn < min(final_m[1].commit_lsns)
    assert max(init_m[2].flush_lsns) <= min(final_m[2].flush_lsns) < middle_lsn
    assert max(init_m[2].commit_lsns) <= min(final_m[2].commit_lsns) < middle_lsn


# Check that dead minority doesn't prevent the commits: execute insert n_inserts
# times, with fault_probability chance of getting a wal acceptor down or up
# along the way. 2 of 3 are always alive, so the work keeps going.
def test_restarts(neon_env_builder: NeonEnvBuilder):
    fault_probability = 0.01
    n_inserts = 1000
    n_acceptors = 3

    neon_env_builder.num_safekeepers = n_acceptors
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch("test_safekeepers_restarts")
    endpoint = env.endpoints.create_start("test_safekeepers_restarts")

    # we rely upon autocommit after each statement
    # as waiting for acceptors happens there
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    failed_node = None
    cur.execute("CREATE TABLE t(key int primary key, value text)")
    for i in range(n_inserts):
        cur.execute("INSERT INTO t values (%s, 'payload');", (i + 1,))

        if random.random() <= fault_probability:
            if failed_node is None:
                failed_node = env.safekeepers[random.randrange(0, n_acceptors)]
                failed_node.stop()
            else:
                failed_node.start()
                failed_node = None
    assert query_scalar(cur, "SELECT sum(key) FROM t") == (n_inserts * (n_inserts + 1)) // 2


# Test that safekeepers push their info to the broker and learn peer status from it
def test_broker(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_broker", "main")

    # FIXME: Is this expected?
    env.pageserver.allowed_errors.append(
        ".*init_tenant_mgr: marking .* as locally complete, while it doesn't exist in remote index.*"
    )

    endpoint = env.endpoints.create_start("test_broker")
    endpoint.safe_psql("CREATE TABLE t(key int primary key, value text)")

    # wait until remote_consistent_lsn gets advanced on all safekeepers
    clients = [sk.http_client() for sk in env.safekeepers]
    stat_before = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
    log.info(f"statuses is {stat_before}")

    endpoint.safe_psql("INSERT INTO t SELECT generate_series(1,100), 'payload'")

    # force checkpoint in pageserver to advance remote_consistent_lsn
    wait_lsn_force_checkpoint(tenant_id, timeline_id, endpoint, env.pageserver)

    # and wait till remote_consistent_lsn propagates to all safekeepers
    started_at = time.time()
    while True:
        stat_after = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
        if all(
            s_after.remote_consistent_lsn > s_before.remote_consistent_lsn
            for s_after, s_before in zip(stat_after, stat_before)
        ):
            break
        elapsed = time.time() - started_at
        if elapsed > 20:
            raise RuntimeError(
                f"timed out waiting {elapsed:.0f}s for remote_consistent_lsn propagation: status before {stat_before}, status current {stat_after}"
            )
        time.sleep(1)


# Test that old WAL consumed by peers and pageserver is removed from safekeepers.
@pytest.mark.parametrize("auth_enabled", [False, True])
def test_wal_removal(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.num_safekeepers = 2
    # to advance remote_consistent_lsn
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    # FIXME: Is this expected?
    env.pageserver.allowed_errors.append(
        ".*init_tenant_mgr: marking .* as locally complete, while it doesn't exist in remote index.*"
    )

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_safekeepers_wal_removal")
    endpoint = env.endpoints.create_start("test_safekeepers_wal_removal")

    # Note: it is important to insert at least two segments, as currently
    # control file is synced roughly once in segment range and WAL is not
    # removed until all horizons are persisted.
    endpoint.safe_psql_many(
        [
            "CREATE TABLE t(key int primary key, value text)",
            "INSERT INTO t SELECT generate_series(1,200000), 'payload'",
        ]
    )

    # force checkpoint to advance remote_consistent_lsn
    pageserver_conn_options = {}
    if auth_enabled:
        pageserver_conn_options["password"] = env.auth_keys.generate_tenant_token(tenant_id)
    wait_lsn_force_checkpoint(
        tenant_id, timeline_id, endpoint, env.pageserver, pageserver_conn_options
    )

    # We will wait for first segment removal. Make sure they exist for starter.
    first_segments = [
        os.path.join(sk.data_dir(), str(tenant_id), str(timeline_id), "000000010000000000000001")
        for sk in env.safekeepers
    ]
    assert all(os.path.exists(p) for p in first_segments)

    if not auth_enabled:
        http_cli = env.safekeepers[0].http_client()
    else:
        http_cli = env.safekeepers[0].http_client(
            auth_token=env.auth_keys.generate_tenant_token(tenant_id)
        )
        http_cli_other = env.safekeepers[0].http_client(
            auth_token=env.auth_keys.generate_tenant_token(TenantId.generate())
        )
        http_cli_noauth = env.safekeepers[0].http_client()

    # Pretend WAL is offloaded to s3.
    if auth_enabled:
        old_backup_lsn = http_cli.timeline_status(
            tenant_id=tenant_id, timeline_id=timeline_id
        ).backup_lsn
        assert "FFFFFFFF/FEFFFFFF" != old_backup_lsn
        for cli in [http_cli_other, http_cli_noauth]:
            with pytest.raises(cli.HTTPError, match="Forbidden|Unauthorized"):
                cli.record_safekeeper_info(
                    tenant_id, timeline_id, {"backup_lsn": "FFFFFFFF/FEFFFFFF"}
                )
        assert (
            old_backup_lsn
            == http_cli.timeline_status(tenant_id=tenant_id, timeline_id=timeline_id).backup_lsn
        )
    http_cli.record_safekeeper_info(tenant_id, timeline_id, {"backup_lsn": "FFFFFFFF/FEFFFFFF"})
    assert (
        Lsn("FFFFFFFF/FEFFFFFF")
        == http_cli.timeline_status(tenant_id=tenant_id, timeline_id=timeline_id).backup_lsn
    )

    # wait till first segment is removed on all safekeepers
    wait(
        lambda first_segments=first_segments: all(not os.path.exists(p) for p in first_segments),
        "first segment get removed",
        wait_f=lambda http_cli=http_cli, tenant_id=tenant_id, timeline_id=timeline_id: log.info(
            f"waiting for segments removal, sk info: {http_cli.timeline_status(tenant_id=tenant_id, timeline_id=timeline_id)}"
        ),
    )


# Wait for something, defined as f() returning True, raising error if this
# doesn't happen without timeout seconds, and calling wait_f while waiting.
def wait(f, desc, timeout=30, wait_f=None):
    started_at = time.time()
    while True:
        if f():
            break
        elapsed = time.time() - started_at
        if elapsed > timeout:
            raise RuntimeError(f"timed out waiting {elapsed:.0f}s for {desc}")
        time.sleep(0.5)
        if wait_f is not None:
            wait_f()


def is_segment_offloaded(
    sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, seg_end: Lsn
):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    log.info(f"sk status is {tli_status}")
    return tli_status.backup_lsn >= seg_end


def is_flush_lsn_caught_up(sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, lsn: Lsn):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    log.info(f"sk status is {tli_status}")
    return tli_status.flush_lsn >= lsn


def is_wal_trimmed(sk: Safekeeper, tenant_id: TenantId, timeline_id: TimelineId, target_size_mb):
    http_cli = sk.http_client()
    tli_status = http_cli.timeline_status(tenant_id, timeline_id)
    sk_wal_size = get_dir_size(os.path.join(sk.data_dir(), str(tenant_id), str(timeline_id)))
    sk_wal_size_mb = sk_wal_size / 1024 / 1024
    log.info(f"Safekeeper id={sk.id} wal_size={sk_wal_size_mb:.2f}MB status={tli_status}")
    return sk_wal_size_mb <= target_size_mb


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_wal_backup(neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind):
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_safekeeper_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_safekeepers_wal_backup")
    endpoint = env.endpoints.create_start("test_safekeepers_wal_backup")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()
    cur.execute("create table t(key int, value text)")

    # Shut down subsequently each of safekeepers and fill a segment while sk is
    # down; ensure segment gets offloaded by others.
    offloaded_seg_end = [Lsn("0/2000000"), Lsn("0/3000000"), Lsn("0/4000000")]
    for victim, seg_end in zip(env.safekeepers, offloaded_seg_end):
        victim.stop()
        # roughly fills one segment
        cur.execute("insert into t select generate_series(1,250000), 'payload'")
        live_sk = [sk for sk in env.safekeepers if sk != victim][0]

        wait(
            partial(is_segment_offloaded, live_sk, tenant_id, timeline_id, seg_end),
            f"segment ending at {seg_end} get offloaded",
        )

        victim.start()

    # put one of safekeepers down again
    env.safekeepers[0].stop()
    # restart postgres
    endpoint.stop_and_destroy().create_start("test_safekeepers_wal_backup")
    # and ensure offloading still works
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("insert into t select generate_series(1,250000), 'payload'")
    seg_end = Lsn("0/5000000")
    wait(
        partial(is_segment_offloaded, env.safekeepers[1], tenant_id, timeline_id, seg_end),
        f"segment ending at {seg_end} get offloaded",
    )


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_s3_wal_replay(neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind):
    neon_env_builder.num_safekeepers = 3

    neon_env_builder.enable_safekeeper_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_s3_wal_replay")

    endpoint = env.endpoints.create_start("test_s3_wal_replay")

    expected_sum = 0

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table t(key int, value text)")
            cur.execute("insert into t values (1, 'payload')")
            expected_sum += 1

            offloaded_seg_end = Lsn("0/3000000")
            # roughly fills two segments
            cur.execute("insert into t select generate_series(1,500000), 'payload'")
            expected_sum += 500000 * 500001 // 2

            assert query_scalar(cur, "select sum(key) from t") == expected_sum

            for sk in env.safekeepers:
                wait(
                    partial(is_segment_offloaded, sk, tenant_id, timeline_id, offloaded_seg_end),
                    f"segment ending at {offloaded_seg_end} get offloaded",
                )

            # advance remote_consistent_lsn to trigger WAL trimming
            # this LSN should be less than commit_lsn, so timeline will be active=true in safekeepers, to push broker updates
            env.safekeepers[0].http_client().record_safekeeper_info(
                tenant_id, timeline_id, {"remote_consistent_lsn": str(offloaded_seg_end)}
            )

            last_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

            for sk in env.safekeepers:
                # require WAL to be trimmed, so no more than one segment is left
                # on disk
                # TODO: WAL removal uses persistent values and control
                # file is fsynced roughly once in a segment, so there is a small
                # chance that two segments are left on disk, not one. We can
                # force persist cf and have 16 instead of 32 here.
                target_size_mb = 32 * 1.5
                wait(
                    partial(is_wal_trimmed, sk, tenant_id, timeline_id, target_size_mb),
                    f"sk_id={sk.id} to trim WAL to {target_size_mb:.2f}MB",
                )
                # wait till everyone puts data up to last_lsn on disk, we are
                # going to recreate state on safekeepers claiming they have data till last_lsn.
                wait(
                    partial(is_flush_lsn_caught_up, sk, tenant_id, timeline_id, last_lsn),
                    f"sk_id={sk.id} to flush {last_lsn}",
                )

    ps_http = env.pageserver.http_client()
    pageserver_lsn = Lsn(ps_http.timeline_detail(tenant_id, timeline_id)["last_record_lsn"])
    lag = last_lsn - pageserver_lsn
    log.info(
        f"Pageserver last_record_lsn={pageserver_lsn}; flush_lsn={last_lsn}; lag before replay is {lag / 1024}kb"
    )

    endpoint.stop_and_destroy()
    timeline_delete_wait_completed(ps_http, tenant_id, timeline_id)

    # Also delete and manually create timeline on safekeepers -- this tests
    # scenario of manual recovery on different set of safekeepers.

    # save the last (partial) file to put it back after recreation; others will be fetched from s3
    sk = env.safekeepers[0]
    tli_dir = Path(sk.data_dir()) / str(tenant_id) / str(timeline_id)
    f_partial = Path([f for f in os.listdir(tli_dir) if f.endswith(".partial")][0])
    f_partial_path = tli_dir / f_partial
    f_partial_saved = Path(sk.data_dir()) / f_partial.name
    f_partial_path.rename(f_partial_saved)

    pg_version = sk.http_client().timeline_status(tenant_id, timeline_id).pg_version

    # Terminate first all safekeepers to prevent communication unexpectantly
    # advancing peer_horizon_lsn.
    for sk in env.safekeepers:
        cli = sk.http_client()
        cli.timeline_delete_force(tenant_id, timeline_id)
        # restart safekeeper to clear its in-memory state
        sk.stop()
    # wait all potenital in flight pushes to broker arrive before starting
    # safekeepers (even without sleep, it is very unlikely they are not
    # delivered yet).
    time.sleep(1)

    for sk in env.safekeepers:
        sk.start()
        cli = sk.http_client()
        cli.timeline_create(tenant_id, timeline_id, pg_version, last_lsn)
        f_partial_path = (
            Path(sk.data_dir()) / str(tenant_id) / str(timeline_id) / f_partial_saved.name
        )
        shutil.copy(f_partial_saved, f_partial_path)

    # recreate timeline on pageserver from scratch
    ps_http.timeline_create(
        pg_version=PgVersion(pg_version),
        tenant_id=tenant_id,
        new_timeline_id=timeline_id,
    )

    wait_lsn_timeout = 60 * 3
    started_at = time.time()
    last_debug_print = 0.0

    while True:
        elapsed = time.time() - started_at
        if elapsed > wait_lsn_timeout:
            raise RuntimeError("Timed out waiting for WAL redo")

        tenant_status = ps_http.tenant_status(tenant_id)
        if tenant_status["state"]["slug"] == "Loading":
            log.debug(f"Tenant {tenant_id} is still loading, retrying")
        else:
            pageserver_lsn = Lsn(
                env.pageserver.http_client().timeline_detail(tenant_id, timeline_id)[
                    "last_record_lsn"
                ]
            )
            lag = last_lsn - pageserver_lsn

            if time.time() > last_debug_print + 10 or lag <= 0:
                last_debug_print = time.time()
                log.info(f"Pageserver last_record_lsn={pageserver_lsn}; lag is {lag / 1024}kb")

                if lag <= 0:
                    break

        time.sleep(1)

    log.info(f"WAL redo took {elapsed} s")

    # verify data
    endpoint.create_start("test_s3_wal_replay")

    assert endpoint.safe_psql("select sum(key) from t")[0][0] == expected_sum


class ProposerPostgres(PgProtocol):
    """Object for running postgres without NeonEnv"""

    def __init__(
        self,
        pgdata_dir: str,
        pg_bin,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        listen_addr: str,
        port: int,
    ):
        super().__init__(host=listen_addr, port=port, user="cloud_admin", dbname="postgres")

        self.pgdata_dir: str = pgdata_dir
        self.pg_bin: PgBin = pg_bin
        self.tenant_id: TenantId = tenant_id
        self.timeline_id: TimelineId = timeline_id
        self.listen_addr: str = listen_addr
        self.port: int = port

    def pg_data_dir_path(self) -> str:
        """Path to data directory"""
        return self.pgdata_dir

    def config_file_path(self) -> str:
        """Path to postgresql.conf"""
        return os.path.join(self.pgdata_dir, "postgresql.conf")

    def create_dir_config(self, safekeepers: str):
        """Create dir and config for running --sync-safekeepers"""

        pathlib.Path(self.pg_data_dir_path()).mkdir(exist_ok=True)
        with open(self.config_file_path(), "w") as f:
            cfg = [
                "synchronous_standby_names = 'walproposer'\n",
                "shared_preload_libraries = 'neon'\n",
                f"neon.timeline_id = '{self.timeline_id}'\n",
                f"neon.tenant_id = '{self.tenant_id}'\n",
                "neon.pageserver_connstring = ''\n",
                f"neon.safekeepers = '{safekeepers}'\n",
                f"listen_addresses = '{self.listen_addr}'\n",
                f"port = '{self.port}'\n",
            ]

            f.writelines(cfg)

    def sync_safekeepers(self) -> Lsn:
        """
        Run 'postgres --sync-safekeepers'.
        Returns execution result, which is commit_lsn after sync.
        """

        command = ["postgres", "--sync-safekeepers"]
        env = {
            "PGDATA": self.pg_data_dir_path(),
        }

        basepath = self.pg_bin.run_capture(command, env)

        log.info(f"postgres --sync-safekeepers output: {basepath}")

        stdout_filename = basepath + ".stdout"

        with open(stdout_filename, "r") as stdout_f:
            stdout = stdout_f.read()
            return Lsn(stdout.strip("\n "))

    def initdb(self):
        """Run initdb"""

        args = ["initdb", "-U", "cloud_admin", "-D", self.pg_data_dir_path()]
        self.pg_bin.run(args)

    def start(self):
        """Start postgres with pg_ctl"""

        log_path = os.path.join(self.pg_data_dir_path(), "pg.log")
        args = ["pg_ctl", "-D", self.pg_data_dir_path(), "-l", log_path, "-w", "start"]
        self.pg_bin.run(args)

    def stop(self):
        """Stop postgres with pg_ctl"""

        args = ["pg_ctl", "-D", self.pg_data_dir_path(), "-m", "immediate", "-w", "stop"]
        self.pg_bin.run(args)


# insert wal in all safekeepers and run sync on proposer
def test_sync_safekeepers(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    port_distributor: PortDistributor,
):
    # We don't really need the full environment for this test, just the
    # safekeepers would be enough.
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()

    # write config for proposer
    pgdata_dir = os.path.join(env.repo_dir, "proposer_pgdata")
    pg = ProposerPostgres(
        pgdata_dir, pg_bin, tenant_id, timeline_id, "127.0.0.1", port_distributor.get_port()
    )
    pg.create_dir_config(env.get_safekeeper_connstrs())

    # valid lsn, which is not in the segment start, nor in zero segment
    epoch_start_lsn = Lsn("0/16B9188")
    begin_lsn = epoch_start_lsn

    # append and commit WAL
    lsn_after_append = []
    for i in range(3):
        res = env.safekeepers[i].append_logical_message(
            tenant_id,
            timeline_id,
            {
                "lm_prefix": "prefix",
                "lm_message": "message",
                "set_commit_lsn": True,
                "send_proposer_elected": True,
                "term": 2,
                "begin_lsn": int(begin_lsn),
                "epoch_start_lsn": int(epoch_start_lsn),
                "truncate_lsn": int(epoch_start_lsn),
                "pg_version": int(env.pg_version) * 10000,
            },
        )
        lsn = Lsn(res["inserted_wal"]["end_lsn"])
        lsn_after_append.append(lsn)
        log.info(f"safekeeper[{i}] lsn after append: {lsn}")

    # run sync safekeepers
    lsn_after_sync = pg.sync_safekeepers()
    log.info(f"lsn after sync = {lsn_after_sync}")

    assert all(lsn_after_sync == lsn for lsn in lsn_after_append)


@pytest.mark.parametrize("auth_enabled", [False, True])
def test_timeline_status(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_timeline_status")
    endpoint = env.endpoints.create_start("test_timeline_status")

    wa = env.safekeepers[0]

    if not auth_enabled:
        wa_http_cli = wa.http_client()
        wa_http_cli.check_status()

        wa_http_cli_debug = wa.http_client()
        wa_http_cli_debug.check_status()
    else:
        wa_http_cli = wa.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))
        wa_http_cli.check_status()
        wa_http_cli_bad = wa.http_client(
            auth_token=env.auth_keys.generate_tenant_token(TenantId.generate())
        )
        wa_http_cli_bad.check_status()
        wa_http_cli_noauth = wa.http_client()
        wa_http_cli_noauth.check_status()

        # debug endpoint requires safekeeper scope
        wa_http_cli_debug = wa.http_client(auth_token=env.auth_keys.generate_safekeeper_token())
        wa_http_cli_debug.check_status()

    # create a dummy table to wait for timeline initialization in safekeeper
    endpoint.safe_psql("create table wait_for_sk()")

    # fetch something sensible from status
    tli_status = wa_http_cli.timeline_status(tenant_id, timeline_id)
    epoch = tli_status.acceptor_epoch
    timeline_start_lsn = tli_status.timeline_start_lsn

    if auth_enabled:
        for cli in [wa_http_cli_bad, wa_http_cli_noauth]:
            with pytest.raises(cli.HTTPError, match="Forbidden|Unauthorized"):
                cli.timeline_status(tenant_id, timeline_id)

    # fetch debug_dump endpoint
    debug_dump_0 = wa_http_cli_debug.debug_dump({"dump_all": "true"})
    log.info(f"debug_dump before reboot {debug_dump_0}")
    assert debug_dump_0["timelines_count"] == 1
    assert debug_dump_0["timelines"][0]["timeline_id"] == str(timeline_id)

    endpoint.safe_psql("create table t(i int)")

    # ensure epoch goes up after reboot
    endpoint.stop().start()
    endpoint.safe_psql("insert into t values(10)")

    tli_status = wa_http_cli.timeline_status(tenant_id, timeline_id)
    epoch_after_reboot = tli_status.acceptor_epoch
    assert epoch_after_reboot > epoch

    # and timeline_start_lsn stays the same
    assert tli_status.timeline_start_lsn == timeline_start_lsn

    # fetch debug_dump after reboot
    debug_dump_1 = wa_http_cli_debug.debug_dump({"dump_all": "true"})
    log.info(f"debug_dump after reboot {debug_dump_1}")
    assert debug_dump_1["timelines_count"] == 1
    assert debug_dump_1["timelines"][0]["timeline_id"] == str(timeline_id)

    # check that commit_lsn and flush_lsn not decreased
    assert (
        debug_dump_1["timelines"][0]["memory"]["mem_state"]["commit_lsn"]
        >= debug_dump_0["timelines"][0]["memory"]["mem_state"]["commit_lsn"]
    )
    assert (
        debug_dump_1["timelines"][0]["memory"]["flush_lsn"]
        >= debug_dump_0["timelines"][0]["memory"]["flush_lsn"]
    )

    # check .config in response
    assert debug_dump_1["config"]["id"] == env.safekeepers[0].id


class DummyConsumer(object):
    def __call__(self, msg):
        pass


def test_start_replication_term(neon_env_builder: NeonEnvBuilder):
    """
    Test START_REPLICATION of uncommitted part specifying leader term. It must
    error if safekeeper switched to different term.
    """

    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_start_replication_term")
    endpoint = env.endpoints.create_start("test_start_replication_term")

    endpoint.safe_psql("CREATE TABLE t(key int primary key, value text)")

    sk = env.safekeepers[0]
    sk_http_cli = sk.http_client()
    tli_status = sk_http_cli.timeline_status(tenant_id, timeline_id)
    timeline_start_lsn = tli_status.timeline_start_lsn

    conn_opts = {
        "host": "127.0.0.1",
        "options": f"-c timeline_id={timeline_id} tenant_id={tenant_id}",
        "port": sk.port.pg,
        "connection_factory": psycopg2.extras.PhysicalReplicationConnection,
    }
    sk_pg_conn = psycopg2.connect(**conn_opts)  # type: ignore
    with sk_pg_conn.cursor() as cur:
        # should fail, as first start has term 2
        cur.start_replication_expert(f"START_REPLICATION {timeline_start_lsn} (term='3')")
        dummy_consumer = DummyConsumer()
        with pytest.raises(psycopg2.errors.InternalError_) as excinfo:
            cur.consume_stream(dummy_consumer)
        assert "failed to acquire term 3" in str(excinfo.value)


# Test auth on all ports: WAL service (postgres protocol), WAL service tenant only and http.
def test_sk_auth(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_sk_auth")
    env.endpoints.create_start("test_sk_auth")

    sk = env.safekeepers[0]

    tenant_token = env.auth_keys.generate_tenant_token(tenant_id)
    full_token = env.auth_keys.generate_safekeeper_token()

    conn_opts = {
        "host": "127.0.0.1",
        "options": f"-c timeline_id={timeline_id} tenant_id={tenant_id}",
    }
    connector = PgProtocol(**conn_opts)
    # no password, should fail
    with pytest.raises(psycopg2.OperationalError):
        connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg)
    # giving password, should be ok with either token on main pg port
    connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg, password=tenant_token)
    connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg, password=full_token)
    # on tenant only port tenant only token should work
    connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg_tenant_only, password=tenant_token)
    # but full token should fail
    with pytest.raises(psycopg2.OperationalError):
        connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg_tenant_only, password=full_token)

    # Now test that auth on http/pg can be enabled separately.

    # By default, neon_local enables auth on all services if auth is configured,
    # so http must require the token.
    sk_http_cli_noauth = sk.http_client()
    sk_http_cli_auth = sk.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))
    with pytest.raises(sk_http_cli_noauth.HTTPError, match="Forbidden|Unauthorized"):
        sk_http_cli_noauth.timeline_status(tenant_id, timeline_id)
    sk_http_cli_auth.timeline_status(tenant_id, timeline_id)

    # now, disable auth on http
    sk.stop()
    sk.start(extra_opts=["--http-auth-public-key-path="])
    sk_http_cli_noauth.timeline_status(tenant_id, timeline_id)  # must work without token
    # but pg should still require the token
    with pytest.raises(psycopg2.OperationalError):
        connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg)
    connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg, password=tenant_token)

    # now also disable auth on pg, but leave on pg tenant only
    sk.stop()
    sk.start(extra_opts=["--http-auth-public-key-path=", "--pg-auth-public-key-path="])
    sk_http_cli_noauth.timeline_status(tenant_id, timeline_id)  # must work without token
    connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg)  # must work without token
    # but pg tenant only should still require the token
    with pytest.raises(psycopg2.OperationalError):
        connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg_tenant_only)
    connector.safe_psql("IDENTIFY_SYSTEM", port=sk.port.pg_tenant_only, password=tenant_token)


# Try restarting endpoint with enabled auth.
def test_restart_endpoint(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch("test_sk_auth_restart_endpoint")
    endpoint = env.endpoints.create_start("test_sk_auth_restart_endpoint")

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table t(i int)")

    # Restarting endpoints and random safekeepers, to trigger recovery.
    for _i in range(3):
        random_sk = random.choice(env.safekeepers)
        random_sk.stop()

        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                start = random.randint(1, 100000)
                end = start + random.randint(1, 10000)
                cur.execute("insert into t select generate_series(%s,%s)", (start, end))

        endpoint.stop()
        random_sk.start()
        endpoint.start()


class SafekeeperEnv:
    def __init__(
        self,
        repo_dir: Path,
        port_distributor: PortDistributor,
        pg_bin: PgBin,
        neon_binpath: Path,
        num_safekeepers: int = 1,
    ):
        self.repo_dir = repo_dir
        self.port_distributor = port_distributor
        self.broker = NeonBroker(
            logfile=Path(self.repo_dir) / "storage_broker.log",
            port=self.port_distributor.get_port(),
            neon_binpath=neon_binpath,
        )
        self.pg_bin = pg_bin
        self.num_safekeepers = num_safekeepers
        self.bin_safekeeper = str(neon_binpath / "safekeeper")
        self.safekeepers: Optional[List[subprocess.CompletedProcess[Any]]] = None
        self.postgres: Optional[ProposerPostgres] = None
        self.tenant_id: Optional[TenantId] = None
        self.timeline_id: Optional[TimelineId] = None

    def init(self) -> "SafekeeperEnv":
        assert self.postgres is None, "postgres is already initialized"
        assert self.safekeepers is None, "safekeepers are already initialized"

        self.tenant_id = TenantId.generate()
        self.timeline_id = TimelineId.generate()
        self.repo_dir.mkdir(exist_ok=True)

        # Create config and a Safekeeper object for each safekeeper
        self.safekeepers = []
        for i in range(1, self.num_safekeepers + 1):
            self.safekeepers.append(self.start_safekeeper(i))

        # Create and start postgres
        self.postgres = self.create_postgres()
        self.postgres.start()

        return self

    def start_safekeeper(self, i):
        port = SafekeeperPort(
            pg=self.port_distributor.get_port(),
            pg_tenant_only=self.port_distributor.get_port(),
            http=self.port_distributor.get_port(),
        )

        safekeeper_dir = self.repo_dir / f"sk{i}"
        safekeeper_dir.mkdir(exist_ok=True)

        cmd = [
            self.bin_safekeeper,
            "-l",
            f"127.0.0.1:{port.pg}",
            "--listen-http",
            f"127.0.0.1:{port.http}",
            "-D",
            str(safekeeper_dir),
            "--id",
            str(i),
            "--broker-endpoint",
            self.broker.client_url(),
        ]
        log.info(f'Running command "{" ".join(cmd)}"')

        safekeeper_client = SafekeeperHttpClient(
            port=port.http,
            auth_token=None,
        )
        try:
            safekeeper_process = start_in_background(
                cmd, safekeeper_dir, "safekeeper.log", safekeeper_client.check_status
            )
            return safekeeper_process
        except Exception as e:
            log.error(e)
            safekeeper_process.kill()
            raise Exception(f"Failed to start safekepeer as {cmd}, reason: {e}") from e

    def get_safekeeper_connstrs(self):
        assert self.safekeepers is not None, "safekeepers are not initialized"
        return ",".join([sk_proc.args[2] for sk_proc in self.safekeepers])

    def create_postgres(self):
        assert self.tenant_id is not None, "tenant_id is not initialized"
        assert self.timeline_id is not None, "tenant_id is not initialized"
        pgdata_dir = os.path.join(self.repo_dir, "proposer_pgdata")
        pg = ProposerPostgres(
            pgdata_dir,
            self.pg_bin,
            self.tenant_id,
            self.timeline_id,
            "127.0.0.1",
            self.port_distributor.get_port(),
        )
        pg.initdb()
        pg.create_dir_config(self.get_safekeeper_connstrs())
        return pg

    def kill_safekeeper(self, sk_dir):
        """Read pid file and kill process"""
        pid_file = os.path.join(sk_dir, "safekeeper.pid")
        with open(pid_file, "r") as f:
            pid = int(f.read())
            log.info(f"Killing safekeeper with pid {pid}")
            os.kill(pid, signal.SIGKILL)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        log.info("Cleaning up all safekeeper and compute nodes")

        # Stop all the nodes
        if self.postgres is not None:
            self.postgres.stop()
        if self.safekeepers is not None:
            for sk_proc in self.safekeepers:
                self.kill_safekeeper(sk_proc.args[6])


def test_safekeeper_without_pageserver(
    test_output_dir: str,
    port_distributor: PortDistributor,
    pg_bin: PgBin,
    neon_binpath: Path,
):
    # Create the environment in the test-specific output dir
    repo_dir = Path(os.path.join(test_output_dir, "repo"))

    env = SafekeeperEnv(
        repo_dir,
        port_distributor,
        pg_bin,
        neon_binpath,
    )

    with env:
        env.init()
        assert env.postgres is not None

        env.postgres.safe_psql("create table t(i int)")
        env.postgres.safe_psql("insert into t select generate_series(1, 100)")
        res = env.postgres.safe_psql("select sum(i) from t")[0][0]
        assert res == 5050


def test_replace_safekeeper(neon_env_builder: NeonEnvBuilder):
    def execute_payload(endpoint: Endpoint):
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute("CREATE TABLE IF NOT EXISTS t(key int, value text)")
                cur.execute("INSERT INTO t VALUES (0, 'something')")
                sum_before = query_scalar(cur, "SELECT SUM(key) FROM t")

                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                sum_after = query_scalar(cur, "SELECT SUM(key) FROM t")
                assert sum_after == sum_before + 5000050000

    def show_statuses(safekeepers: List[Safekeeper], tenant_id: TenantId, timeline_id: TimelineId):
        for sk in safekeepers:
            http_cli = sk.http_client()
            try:
                status = http_cli.timeline_status(tenant_id, timeline_id)
                log.info(f"Safekeeper {sk.id} status: {status}")
            except Exception as e:
                log.info(f"Safekeeper {sk.id} status error: {e}")

    neon_env_builder.num_safekeepers = 4
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_replace_safekeeper")

    log.info("Use only first 3 safekeepers")
    env.safekeepers[3].stop()
    endpoint = env.endpoints.create("test_replace_safekeeper")
    endpoint.active_safekeepers = [1, 2, 3]
    endpoint.start()

    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Restart all safekeepers to flush everything")
    env.safekeepers[0].stop(immediate=True)
    execute_payload(endpoint)
    env.safekeepers[0].start()
    env.safekeepers[1].stop(immediate=True)
    execute_payload(endpoint)
    env.safekeepers[1].start()
    env.safekeepers[2].stop(immediate=True)
    execute_payload(endpoint)
    env.safekeepers[2].start()

    env.safekeepers[0].stop(immediate=True)
    env.safekeepers[1].stop(immediate=True)
    env.safekeepers[2].stop(immediate=True)
    env.safekeepers[0].start()
    env.safekeepers[1].start()
    env.safekeepers[2].start()

    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Stop sk1 (simulate failure) and use only quorum of sk2 and sk3")
    env.safekeepers[0].stop(immediate=True)
    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Recreate postgres to replace failed sk1 with new sk4")
    endpoint.stop_and_destroy().create("test_replace_safekeeper")
    env.safekeepers[3].start()
    endpoint.active_safekeepers = [2, 3, 4]
    endpoint.start()

    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Stop sk2 to require quorum of sk3 and sk4 for normal work")
    env.safekeepers[1].stop(immediate=True)
    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)


# We have `wal_keep_size=0`, so postgres should trim WAL once it's broadcasted
# to all safekeepers. This test checks that compute WAL can fit into small number
# of WAL segments.
def test_wal_deleted_after_broadcast(neon_env_builder: NeonEnvBuilder):
    # used to calculate delta in collect_stats
    last_lsn = Lsn(0)

    # returns pg_wal size in MB
    def collect_stats(endpoint: Endpoint, cur, enable_logs=True):
        nonlocal last_lsn
        assert endpoint.pgdata_dir is not None

        log.info("executing INSERT to generate WAL")
        current_lsn = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        pg_wal_size_mb = get_dir_size(os.path.join(endpoint.pgdata_dir, "pg_wal")) / 1024 / 1024
        if enable_logs:
            lsn_delta_mb = (current_lsn - last_lsn) / 1024 / 1024
            log.info(f"LSN delta: {lsn_delta_mb} MB, current WAL size: {pg_wal_size_mb} MB")
        last_lsn = current_lsn
        return pg_wal_size_mb

    # generates about ~20MB of WAL, to create at least one new segment
    def generate_wal(cur):
        cur.execute("INSERT INTO t SELECT generate_series(1,300000), 'payload'")

    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch("test_wal_deleted_after_broadcast")
    # Adjust checkpoint config to prevent keeping old WAL segments
    endpoint = env.endpoints.create_start(
        "test_wal_deleted_after_broadcast",
        config_lines=["min_wal_size=32MB", "max_wal_size=32MB", "log_checkpoints=on"],
    )

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()
    cur.execute("CREATE TABLE t(key int, value text)")

    collect_stats(endpoint, cur)

    # generate WAL to simulate normal workload
    for _ in range(5):
        generate_wal(cur)
        collect_stats(endpoint, cur)

    log.info("executing checkpoint")
    cur.execute("CHECKPOINT")
    wal_size_after_checkpoint = collect_stats(endpoint, cur)

    # there shouldn't be more than 2 WAL segments (but dir may have archive_status files)
    assert wal_size_after_checkpoint < 16 * 2.5


@pytest.mark.parametrize("auth_enabled", [False, True])
def test_delete_force(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    # FIXME: are these expected?
    env.pageserver.allowed_errors.extend(
        [
            ".*Timeline .* was not found in global map.*",
            ".*Timeline .* was cancelled and cannot be used anymore.*",
        ]
    )

    # Create two tenants: one will be deleted, other should be preserved.
    tenant_id = env.initial_tenant
    timeline_id_1 = env.neon_cli.create_branch("br1")  # Active, delete explicitly
    timeline_id_2 = env.neon_cli.create_branch("br2")  # Inactive, delete explicitly
    timeline_id_3 = env.neon_cli.create_branch("br3")  # Active, delete with the tenant
    timeline_id_4 = env.neon_cli.create_branch("br4")  # Inactive, delete with the tenant

    tenant_id_other, timeline_id_other = env.neon_cli.create_tenant()

    # Populate branches
    endpoint_1 = env.endpoints.create_start("br1")
    endpoint_2 = env.endpoints.create_start("br2")
    endpoint_3 = env.endpoints.create_start("br3")
    endpoint_4 = env.endpoints.create_start("br4")
    endpoint_other = env.endpoints.create_start("main", tenant_id=tenant_id_other)
    for endpoint in [endpoint_1, endpoint_2, endpoint_3, endpoint_4, endpoint_other]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key)")
    sk = env.safekeepers[0]
    sk_data_dir = Path(sk.data_dir())
    if not auth_enabled:
        sk_http = sk.http_client()
        sk_http_other = sk_http
    else:
        sk_http = sk.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))
        sk_http_other = sk.http_client(
            auth_token=env.auth_keys.generate_tenant_token(tenant_id_other)
        )
        sk_http_noauth = sk.http_client()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_1)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_2)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Stop branches which should be inactive and restart Safekeeper to drop its in-memory state.
    endpoint_2.stop_and_destroy()
    endpoint_4.stop_and_destroy()
    sk.stop()
    sk.start()

    # Ensure connections to Safekeeper are established
    for endpoint in [endpoint_1, endpoint_3, endpoint_other]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO t (key) VALUES (1)")

    # Stop all computes gracefully before safekeepers stop responding to them
    endpoint_1.stop_and_destroy()
    endpoint_3.stop_and_destroy()

    # Remove initial tenant's br1 (active)
    assert sk_http.timeline_delete_force(tenant_id, timeline_id_1)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_2)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Ensure repeated deletion succeeds
    assert not sk_http.timeline_delete_force(tenant_id, timeline_id_1)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_2)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    if auth_enabled:
        # Ensure we cannot delete the other tenant
        for sk_h in [sk_http, sk_http_noauth]:
            with pytest.raises(sk_h.HTTPError, match="Forbidden|Unauthorized"):
                assert sk_h.timeline_delete_force(tenant_id_other, timeline_id_other)
            with pytest.raises(sk_h.HTTPError, match="Forbidden|Unauthorized"):
                assert sk_h.tenant_delete_force(tenant_id_other)
        assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove initial tenant's br2 (inactive)
    assert sk_http.timeline_delete_force(tenant_id, timeline_id_2)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_2)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove non-existing branch, should succeed
    assert not sk_http.timeline_delete_force(tenant_id, TimelineId("00" * 16))["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_2)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove initial tenant fully (two branches are active)
    response = sk_http.tenant_delete_force(tenant_id)
    assert response[str(timeline_id_3)]["dir_existed"]
    assert not (sk_data_dir / str(tenant_id)).exists()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove initial tenant again.
    response = sk_http.tenant_delete_force(tenant_id)
    # assert response == {}
    assert not (sk_data_dir / str(tenant_id)).exists()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Ensure the other tenant still works
    sk_http_other.timeline_status(tenant_id_other, timeline_id_other)
    with closing(endpoint_other.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO t (key) VALUES (123)")


def test_pull_timeline(neon_env_builder: NeonEnvBuilder):
    def safekeepers_guc(env: NeonEnv, sk_names: List[int]) -> str:
        return ",".join([f"localhost:{sk.port.pg}" for sk in env.safekeepers if sk.id in sk_names])

    def execute_payload(endpoint: Endpoint):
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute("CREATE TABLE IF NOT EXISTS t(key int, value text)")
                cur.execute("INSERT INTO t VALUES (0, 'something')")
                sum_before = query_scalar(cur, "SELECT SUM(key) FROM t")

                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                sum_after = query_scalar(cur, "SELECT SUM(key) FROM t")
                assert sum_after == sum_before + 5000050000

    def show_statuses(safekeepers: List[Safekeeper], tenant_id: TenantId, timeline_id: TimelineId):
        for sk in safekeepers:
            http_cli = sk.http_client()
            try:
                status = http_cli.timeline_status(tenant_id, timeline_id)
                log.info(f"Safekeeper {sk.id} status: {status}")
            except Exception as e:
                log.info(f"Safekeeper {sk.id} status error: {e}")

    neon_env_builder.num_safekeepers = 4
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_pull_timeline")

    log.info("Use only first 3 safekeepers")
    env.safekeepers[3].stop()
    endpoint = env.endpoints.create("test_pull_timeline")
    endpoint.active_safekeepers = [1, 2, 3]
    endpoint.start()

    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Kill safekeeper 2, continue with payload")
    env.safekeepers[1].stop(immediate=True)
    execute_payload(endpoint)

    log.info("Initialize new safekeeper 4, pull data from 1 & 3")
    env.safekeepers[3].start()

    res = (
        env.safekeepers[3]
        .http_client()
        .pull_timeline(
            {
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
                "http_hosts": [
                    f"http://localhost:{env.safekeepers[0].port.http}",
                    f"http://localhost:{env.safekeepers[2].port.http}",
                ],
            }
        )
    )
    log.info("Finished pulling timeline")
    log.info(res)

    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Restarting compute with new config to verify that it works")
    endpoint.stop_and_destroy().create("test_pull_timeline")
    endpoint.active_safekeepers = [1, 3, 4]
    endpoint.start()

    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Stop sk1 (simulate failure) and use only quorum of sk3 and sk4")
    env.safekeepers[0].stop(immediate=True)
    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Restart sk4 and and use quorum of sk1 and sk4")
    env.safekeepers[3].stop()
    env.safekeepers[2].stop()
    env.safekeepers[0].start()
    env.safekeepers[3].start()

    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)
