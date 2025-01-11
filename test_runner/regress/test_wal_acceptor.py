from __future__ import annotations

import filecmp
import logging
import os
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
from typing import TYPE_CHECKING

import psycopg2
import psycopg2.errors
import psycopg2.extras
import pytest
import requests
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnvBuilder,
    NeonPageserver,
    PgBin,
    PgProtocol,
    Safekeeper,
    SafekeeperPort,
    last_flush_lsn_upload,
)
from fixtures.pageserver.utils import (
    assert_prefix_empty,
    assert_prefix_not_empty,
    timeline_delete_wait_completed,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import (
    RemoteStorageKind,
    default_remote_storage,
    s3_storage,
)
from fixtures.safekeeper.http import (
    Configuration,
    SafekeeperHttpClient,
    SafekeeperId,
    TimelineCreateRequest,
)
from fixtures.safekeeper.utils import wait_walreceivers_absent
from fixtures.utils import (
    PropagatingThread,
    get_dir_size,
    query_scalar,
    run_only_on_default_postgres,
    skip_in_debug_build,
    start_in_background,
    wait_until,
)

if TYPE_CHECKING:
    from typing import Any, Self


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

    wait_lsn_force_checkpoint_at(lsn, tenant_id, timeline_id, ps, pageserver_conn_options)


def wait_lsn_force_checkpoint_at_sk(
    safekeeper: Safekeeper,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    ps: NeonPageserver,
    pageserver_conn_options=None,
):
    sk_flush_lsn = safekeeper.get_flush_lsn(tenant_id, timeline_id)
    wait_lsn_force_checkpoint_at(sk_flush_lsn, tenant_id, timeline_id, ps, pageserver_conn_options)


def wait_lsn_force_checkpoint_at(
    lsn: Lsn,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    ps: NeonPageserver,
    pageserver_conn_options=None,
):
    """
    Wait until pageserver receives given lsn, force checkpoint and wait for
    upload, i.e. remote_consistent_lsn advancement.
    """
    pageserver_conn_options = pageserver_conn_options or {}

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
    flush_lsns: list[Lsn] = field(default_factory=list)
    commit_lsns: list[Lsn] = field(default_factory=list)


# Run page server and multiple acceptors, and multiple compute nodes running
# against different timelines.
def test_many_timelines(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    n_timelines = 3

    branch_names = [f"test_safekeepers_many_timelines_{tlin}" for tlin in range(n_timelines)]
    # pageserver, safekeeper operate timelines via their ids (can be represented in hex as 'ad50847381e248feaac9876cc71ae418')
    # that's not really human readable, so the branch names are introduced in Neon CLI.
    # Neon CLI stores its branch <-> timeline mapping in its internals,
    # but we need this to collect metrics from other servers, related to the timeline.
    branch_names_to_timeline_ids = {}

    # start postgres on each timeline
    endpoints = []
    for branch_name in branch_names:
        new_timeline_id = env.create_branch(branch_name)
        endpoints.append(env.endpoints.create_start(branch_name))
        branch_names_to_timeline_ids[branch_name] = new_timeline_id

    tenant_id = env.initial_tenant

    def collect_metrics(message: str) -> list[TimelineMetrics]:
        with env.pageserver.http_client() as pageserver_http:
            timeline_details = [
                pageserver_http.timeline_detail(
                    tenant_id=tenant_id,
                    timeline_id=branch_names_to_timeline_ids[branch_name],
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
                m.flush_lsns.append(Lsn(int(sk_m.flush_lsn_inexact(tenant_id, timeline_id))))
                m.commit_lsns.append(Lsn(int(sk_m.commit_lsn_inexact(tenant_id, timeline_id))))

            for flush_lsn, commit_lsn in zip(m.flush_lsns, m.commit_lsns, strict=False):
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
            self.exception: BaseException | None = None

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

    # Test timeline_list endpoint.
    http_cli = env.safekeepers[0].http_client()
    assert len(http_cli.timeline_list()) == 3


# Check that dead minority doesn't prevent the commits: execute insert n_inserts
# times, with fault_probability chance of getting a wal acceptor down or up
# along the way. 2 of 3 are always alive, so the work keeps going.
def test_restarts(neon_env_builder: NeonEnvBuilder):
    fault_probability = 0.01
    n_inserts = 1000
    n_acceptors = 3

    neon_env_builder.num_safekeepers = n_acceptors
    env = neon_env_builder.init_start()

    env.create_branch("test_safekeepers_restarts")
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
    timeline_id = env.create_branch("test_broker", ancestor_branch_name="main")

    endpoint = env.endpoints.create_start("test_broker")
    endpoint.safe_psql("CREATE TABLE t(key int primary key, value text)")

    # wait until remote_consistent_lsn gets advanced on all safekeepers
    clients = [sk.http_client() for sk in env.safekeepers]
    stat_before = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
    log.info(f"statuses before insert: {stat_before}")

    endpoint.safe_psql("INSERT INTO t SELECT generate_series(1,100), 'payload'")

    # wait for remote_consistent_lsn to reach flush_lsn, forcing it with checkpoint
    new_rcl = last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)
    log.info(f"new_rcl: {new_rcl}")
    endpoint.stop()

    # and wait till remote_consistent_lsn propagates to all safekeepers
    #
    # This timeout is long: safekeepers learn about remote_consistent_lsn updates when a pageserver
    # connects, receives a PrimaryKeepAlive, and sends a PageserverFeedback.  So the timeout has to encompass:
    # - pageserver deletion_queue to validate + publish the remote_consistent_lsn
    # - pageserver to reconnect to all safekeepers one by one, with multi-second delays between
    #
    # TODO: timeline status on safekeeper should take into account peers state as well.
    rcl_propagate_secs = 60

    started_at = time.time()
    while True:
        stat_after = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
        if all([s_after.remote_consistent_lsn >= new_rcl for s_after in stat_after]):
            break
        elapsed = time.time() - started_at
        if elapsed > rcl_propagate_secs:
            raise RuntimeError(
                f"timed out waiting {elapsed:.0f}s for remote_consistent_lsn propagation: status before {stat_before}, status current {stat_after}"
            )
        time.sleep(1)

    # Ensure that safekeepers don't lose remote_consistent_lsn on restart.
    for sk in env.safekeepers:
        # force persist cfile
        sk.http_client().checkpoint(tenant_id, timeline_id)
        sk.stop()
        sk.start()
    stat_after_restart = [cli.timeline_status(tenant_id, timeline_id) for cli in clients]
    log.info(f"statuses after {stat_after_restart}")
    assert all([s.remote_consistent_lsn >= new_rcl for s in stat_after_restart])


# Test that old WAL consumed by peers and pageserver is removed from safekeepers.
@pytest.mark.parametrize("auth_enabled", [False, True])
def test_wal_removal(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.num_safekeepers = 2
    # to advance remote_consistent_lsn
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_safekeepers_wal_removal")
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
        sk.timeline_dir(tenant_id, timeline_id) / "000000010000000000000001"
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
        http_cli_noauth = env.safekeepers[0].http_client(gen_sk_wide_token=False)

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
        try:
            if f():
                break
        except Exception as e:
            log.info(f"got exception while waiting for {desc}: {e}")
            pass
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
    sk_wal_size = get_dir_size(sk.timeline_dir(tenant_id, timeline_id))
    sk_wal_size_mb = sk_wal_size / 1024 / 1024
    log.info(f"Safekeeper id={sk.id} wal_size={sk_wal_size_mb:.2f}MB status={tli_status}")
    return sk_wal_size_mb <= target_size_mb


def test_wal_backup(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_safekeeper_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()

    # These are expected after timeline deletion on safekeepers.
    env.pageserver.allowed_errors.extend(
        [
            ".*Timeline .* was not found in global map.*",
            ".*Timeline .* was cancelled and cannot be used anymore.*",
        ]
    )

    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_safekeepers_wal_backup")
    endpoint = env.endpoints.create_start("test_safekeepers_wal_backup")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()
    cur.execute("create table t(key int, value text)")

    # Shut down subsequently each of safekeepers and fill a segment while sk is
    # down; ensure segment gets offloaded by others.
    offloaded_seg_end = [Lsn("0/2000000"), Lsn("0/3000000"), Lsn("0/4000000")]
    for victim, seg_end in zip(env.safekeepers, offloaded_seg_end, strict=False):
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
    endpoint.stop()
    endpoint = env.endpoints.create_start("test_safekeepers_wal_backup")
    # and ensure offloading still works
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("insert into t select generate_series(1,250000), 'payload'")
    seg_end = Lsn("0/5000000")
    wait(
        partial(is_segment_offloaded, env.safekeepers[1], tenant_id, timeline_id, seg_end),
        f"segment ending at {seg_end} get offloaded",
    )
    env.safekeepers[0].start()
    endpoint.stop()

    # Test that after timeline deletion remote objects are gone.
    prefix = "/".join([str(tenant_id), str(timeline_id)])
    assert_prefix_not_empty(neon_env_builder.safekeepers_remote_storage, prefix)

    for sk in env.safekeepers:
        sk_http = sk.http_client()
        sk_http.timeline_delete(tenant_id, timeline_id)
    assert_prefix_empty(neon_env_builder.safekeepers_remote_storage, prefix)


def test_s3_wal_replay(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3

    neon_env_builder.enable_safekeeper_remote_storage(default_remote_storage())

    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_s3_wal_replay")

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

    endpoint.stop()
    timeline_delete_wait_completed(ps_http, tenant_id, timeline_id)

    # Also delete and manually create timeline on safekeepers -- this tests
    # scenario of manual recovery on different set of safekeepers.

    # save the last (partial) file to put it back after recreation; others will be fetched from s3
    sk = env.safekeepers[0]
    tli_dir = Path(sk.data_dir) / str(tenant_id) / str(timeline_id)
    f_partial = Path([f for f in os.listdir(tli_dir) if f.endswith(".partial")][0])
    f_partial_path = tli_dir / f_partial
    f_partial_saved = Path(sk.data_dir) / f_partial.name
    f_partial_path.rename(f_partial_saved)

    pg_version = sk.http_client().timeline_status(tenant_id, timeline_id).pg_version

    # Terminate first all safekeepers to prevent communication unexpectantly
    # advancing peer_horizon_lsn.
    for sk in env.safekeepers:
        cli = sk.http_client()
        cli.timeline_delete(tenant_id, timeline_id, only_local=True)
        # restart safekeeper to clear its in-memory state
        sk.stop()
    # wait all potenital in flight pushes to broker arrive before starting
    # safekeepers (even without sleep, it is very unlikely they are not
    # delivered yet).
    time.sleep(1)

    for sk in env.safekeepers:
        sk.start()
        cli = sk.http_client()
        mconf = Configuration(generation=0, members=[], new_members=None)
        # set start_lsn to the beginning of the first segment to allow reading
        # WAL from there (could you intidb LSN as well).
        r = TimelineCreateRequest(
            tenant_id, timeline_id, mconf, pg_version, Lsn("0/1000000"), commit_lsn=last_lsn
        )
        cli.timeline_create(r)
        f_partial_path = (
            Path(sk.data_dir) / str(tenant_id) / str(timeline_id) / f_partial_saved.name
        )
        shutil.copy(f_partial_saved, f_partial_path)

    # recreate timeline on pageserver from scratch
    ps_http.timeline_create(
        pg_version=PgVersion(str(pg_version)),
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
        pg_bin: PgBin,
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

        Path(self.pg_data_dir_path()).mkdir(exist_ok=True)
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

        basepath = self.pg_bin.run_capture(command, env, with_command_header=False)

        log.info(f"postgres --sync-safekeepers output: {basepath}")

        stdout_filename = basepath + ".stdout"

        with open(stdout_filename) as stdout_f:
            stdout = stdout_f.read()
            return Lsn(stdout.strip("\n "))

    def initdb(self):
        """Run initdb"""

        args = ["initdb", "--username", "cloud_admin", "--pgdata", self.pg_data_dir_path()]
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
    timeline_id = env.create_branch("test_timeline_status")
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
        wa_http_cli_noauth = wa.http_client(gen_sk_wide_token=False)
        wa_http_cli_noauth.check_status()

        # debug endpoint requires safekeeper scope
        wa_http_cli_debug = wa.http_client(auth_token=env.auth_keys.generate_safekeeper_token())
        wa_http_cli_debug.check_status()

    # create a dummy table to wait for timeline initialization in safekeeper
    endpoint.safe_psql("create table wait_for_sk()")

    # fetch something sensible from status
    tli_status = wa_http_cli.timeline_status(tenant_id, timeline_id)
    term = tli_status.term
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
    assert debug_dump_0["timelines"][0]["wal_last_modified"] != ""

    # debug dump non existing tenant, should return no timelines.
    debug_dump_non_existent = wa_http_cli_debug.debug_dump(
        {"tenant_id": "deadbeefdeadbeefdeadbeefdeadbeef"}
    )
    log.info(f"debug_dump_non_existend: {debug_dump_non_existent}")
    assert len(debug_dump_non_existent["timelines"]) == 0

    endpoint.safe_psql("create table t(i int)")

    # ensure epoch goes up after reboot
    endpoint.stop().start()
    endpoint.safe_psql("insert into t values(10)")

    tli_status = wa_http_cli.timeline_status(tenant_id, timeline_id)
    term_after_reboot = tli_status.term
    assert term_after_reboot > term

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


class DummyConsumer:
    def __call__(self, msg):
        pass


def test_start_replication_term(neon_env_builder: NeonEnvBuilder):
    """
    Test START_REPLICATION of uncommitted part specifying leader term. It must
    error if safekeeper switched to different term.
    """

    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_start_replication_term")
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
    timeline_id = env.create_branch("test_sk_auth")
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
    sk_http_cli_noauth = sk.http_client(gen_sk_wide_token=False)
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

    env.create_branch("test_sk_auth_restart_endpoint")
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


# Try restarting endpoint immediately after xlog switch.
# https://github.com/neondatabase/neon/issues/8911
def test_restart_endpoint_after_switch_wal(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    timeline_id = env.initial_timeline

    endpoint = env.endpoints.create_start("main")

    endpoint.safe_psql("create table t (i int)")

    endpoint.safe_psql("SELECT pg_switch_wal()")

    # we want immediate shutdown to have endpoint restart on xlog switch record,
    # so prevent shutdown checkpoint.
    endpoint.stop(mode="immediate", sks_wait_walreceiver_gone=(env.safekeepers, timeline_id))
    endpoint = env.endpoints.create_start("main")
    endpoint.safe_psql("SELECT 'works'")


# Test restarting compute at WAL page boundary.
def test_restart_endpoint_wal_page_boundary(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    ep = env.endpoints.create_start("main")
    ep.safe_psql("create table t (i int)")

    with ep.cursor() as cur:
        # measure how much space logical message takes. Sometimes first attempt
        # creates huge message and then it stabilizes, have no idea why.
        for _ in range(3):
            lsn_before = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
            log.info(f"current_lsn={lsn_before}")
            # Non-transactional logical message doesn't write WAL, only XLogInsert's
            # it, so use transactional. Which is a bit problematic as transactional
            # necessitates commit record. Alternatively we can do smth like
            #   select neon_xlogflush(pg_current_wal_insert_lsn());
            # but isn't much better + that particular call complains on 'xlog flush
            # request 0/282C018 is not satisfied' as pg_current_wal_insert_lsn skips
            # page headers.
            payload = "blahblah"
            cur.execute(f"select pg_logical_emit_message(true, 'pref', '{payload}')")
            lsn_after_by_curr_wal_lsn = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
            lsn_diff = lsn_after_by_curr_wal_lsn - lsn_before
            logical_message_base = lsn_after_by_curr_wal_lsn - lsn_before - len(payload)
            log.info(
                f"before {lsn_before}, after {lsn_after_by_curr_wal_lsn}, lsn diff is {lsn_diff}, base {logical_message_base}"
            )

        # and write logical message spanning exactly as we want
        lsn_before = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        log.info(f"current_lsn={lsn_before}")
        curr_lsn = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        offs = int(curr_lsn) % 8192
        till_page = 8192 - offs
        target_lsn = curr_lsn + till_page
        payload_len = (
            till_page - logical_message_base - 8
        )  # not sure why 8 is here, it is deduced from experiments
        log.info(
            f"current_lsn={curr_lsn}, offs {offs}, till_page {till_page}, target_lsn {target_lsn}"
        )

        cur.execute(f"select pg_logical_emit_message(true, 'pref', 'f{'a' * payload_len}')")
        supposedly_contrecord_end = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        log.info(f"supposedly_page_boundary={supposedly_contrecord_end}")
        # The calculations to hit the page boundary are very fuzzy, so just
        # ignore test if we fail to reach it.
        if not (int(supposedly_contrecord_end) % 8192 == 0):
            pytest.skip(f"missed page boundary, bad luck: lsn is {supposedly_contrecord_end}")

    ep.stop(mode="immediate")
    ep = env.endpoints.create_start("main")
    ep.safe_psql("insert into t values (42)")  # should be ok


# Context manager which logs passed time on exit.
class DurationLogger:
    def __init__(self, desc):
        self.desc = desc

    def __enter__(self):
        self.ts_before = time.time()

    def __exit__(self, *exc):
        log.info(f"{self.desc} finished in {time.time() - self.ts_before}s")


# Context manager which logs WAL position change on exit.
class WalChangeLogger:
    def __init__(self, ep, desc_before):
        self.ep = ep
        self.desc_before = desc_before

    def __enter__(self):
        self.ts_before = time.time()
        self.lsn_before = Lsn(self.ep.safe_psql_scalar("select pg_current_wal_lsn()"))
        log.info(f"{self.desc_before}, lsn_before={self.lsn_before}")

    def __exit__(self, *exc):
        lsn_after = Lsn(self.ep.safe_psql_scalar("select pg_current_wal_lsn()"))
        log.info(
            f"inserted {((lsn_after - self.lsn_before) / 1024 / 1024):.3f} MB of WAL in {(time.time() - self.ts_before):.3f}s"
        )


# Test that we can create timeline with one safekeeper down and initialize it
# later when some data already had been written. It is strictly weaker than
# test_lagging_sk, but also is the simplest test to trigger WAL sk -> compute
# download (recovery) and as such useful for development/testing.
def test_late_init(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    sk1 = env.safekeepers[0]
    sk1.stop()

    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_late_init")
    endpoint = env.endpoints.create_start("test_late_init")
    # create and insert smth while safekeeper is down...
    endpoint.safe_psql("create table t(key int, value text)")
    with WalChangeLogger(endpoint, "doing insert with sk1 down"):
        endpoint.safe_psql("insert into t select generate_series(1, 1000), 'payload'")
    endpoint.stop()  # stop compute

    # stop another safekeeper, and start one which missed timeline creation
    sk2 = env.safekeepers[1]
    sk2.stop()
    sk1.start()

    # insert some more
    with DurationLogger("recovery"):
        endpoint = env.endpoints.create_start("test_late_init")
    endpoint.safe_psql("insert into t select generate_series(1,100), 'payload'")

    wait_flush_lsn_align_by_ep(
        env, "test_late_init", tenant_id, timeline_id, endpoint, [sk1, env.safekeepers[2]]
    )
    # Check that WALs are the same.
    cmp_sk_wal([sk1, env.safekeepers[2]], tenant_id, timeline_id)


# is timeline flush_lsn equal on provided safekeepers?
def is_flush_lsn_aligned(sk_http_clis, tenant_id, timeline_id):
    flush_lsns = [
        sk_http_cli.timeline_status(tenant_id, timeline_id).flush_lsn
        for sk_http_cli in sk_http_clis
    ]
    log.info(f"waiting for flush_lsn alignment, flush_lsns={flush_lsns}")
    return all([flush_lsns[0] == flsn for flsn in flush_lsns])


# Assert by xxd that WAL on given safekeepers is identical. No compute must be
# running for this to be reliable.
def cmp_sk_wal(sks: list[Safekeeper], tenant_id: TenantId, timeline_id: TimelineId):
    assert len(sks) >= 2, "cmp_sk_wal makes sense with >= 2 safekeepers passed"
    sk_http_clis = [sk.http_client() for sk in sks]

    # First check that term / flush_lsn are the same: it is easier to
    # report/understand if WALs are different due to that.
    statuses = [sk_http_cli.timeline_status(tenant_id, timeline_id) for sk_http_cli in sk_http_clis]
    term_flush_lsns = [(s.last_log_term, s.flush_lsn) for s in statuses]
    for tfl, sk in zip(term_flush_lsns[1:], sks[1:], strict=False):
        assert (
            term_flush_lsns[0] == tfl
        ), f"(last_log_term, flush_lsn) are not equal on sks {sks[0].id} and {sk.id}: {term_flush_lsns[0]} != {tfl}"

    # check that WALs are identic.
    segs = [sk.list_segments(tenant_id, timeline_id) for sk in sks]
    for cmp_segs, sk in zip(segs[1:], sks[1:], strict=False):
        assert (
            segs[0] == cmp_segs
        ), f"lists of segments on sks {sks[0].id} and {sk.id} are not identic: {segs[0]} and {cmp_segs}"
    log.info(f"comparing segs {segs[0]}")

    sk0 = sks[0]
    for sk in sks[1:]:
        (_, mismatch, not_regular) = filecmp.cmpfiles(
            sk0.timeline_dir(tenant_id, timeline_id),
            sk.timeline_dir(tenant_id, timeline_id),
            segs[0],
            shallow=False,
        )
        log.info(
            f"filecmp result mismatch and not regular files:\n\t mismatch={mismatch}\n\t not_regular={not_regular}"
        )

        for f in mismatch:
            f1 = sk0.timeline_dir(tenant_id, timeline_id) / f
            f2 = sk.timeline_dir(tenant_id, timeline_id) / f
            stdout_filename = f"{f2}.filediff"

            with open(stdout_filename, "w") as stdout_f:
                subprocess.run(f"xxd {f1} > {f1}.hex ", shell=True)
                subprocess.run(f"xxd {f2} > {f2}.hex ", shell=True)

                cmd = f"diff {f1}.hex {f2}.hex"
                subprocess.run([cmd], stdout=stdout_f, shell=True)

            assert (mismatch, not_regular) == (
                [],
                [],
            ), f"WAL segs {f1} and {f2} on sks {sks[0].id} and {sk.id} are not identic"


# Wait until flush_lsn on given sks becomes equal, assuming endpoint ep is
# running. ep is stopped by this function. This is used in tests which check
# binary equality of WAL segments on safekeepers; which is inherently racy as
# shutting down endpoint might always write some WAL which can get to only one
# safekeeper. So here we recheck flush_lsn again after ep shutdown and retry if
# it has changed.
def wait_flush_lsn_align_by_ep(env, branch, tenant_id, timeline_id, ep, sks):
    sk_http_clis = [sk.http_client() for sk in sks]
    # First wait for the alignment.
    wait(
        partial(is_flush_lsn_aligned, sk_http_clis, tenant_id, timeline_id),
        "flush_lsn to get aligned",
    )
    ep.stop()  # then stop endpoint
    # Even if there is no compute, there might be some in flight data; ensure
    # all walreceivers die before rechecking.
    for sk_http_cli in sk_http_clis:
        wait_walreceivers_absent(sk_http_cli, tenant_id, timeline_id)
    # Now recheck again flush_lsn and exit if it is good
    if is_flush_lsn_aligned(sk_http_clis, tenant_id, timeline_id):
        return
    # Otherwise repeat.
    log.info("flush_lsn changed during endpoint shutdown; retrying alignment")
    ep = env.endpoints.create_start(branch)


# Test behaviour with one safekeeper down and missing a lot of WAL, exercising
# neon_walreader and checking that pg_wal never bloats. Namely, ensures that
# compute doesn't keep many WAL for lagging sk, but still can recover it with
# neon_walreader, in two scenarious: a) WAL never existed on compute (it started
# on basebackup LSN later than lagging sk position) though segment file exists
# b) WAL had been recycled on it and segment file doesn't exist.
#
# Also checks along the way that whenever there are two sks alive, compute
# should be able to commit.
def test_lagging_sk(neon_env_builder: NeonEnvBuilder):
    # inserts ~20MB of WAL, a bit more than a segment.
    def fill_segment(ep):
        ep.safe_psql("insert into t select generate_series(1, 180000), 'payload'")

    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    (sk1, sk2, sk3) = env.safekeepers

    # create and insert smth while safekeeper is down...
    sk1.stop()
    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_lagging_sk")
    ep = env.endpoints.create_start("test_lagging_sk")
    ep.safe_psql("create table t(key int, value text)")
    # make small insert to be on the same segment
    ep.safe_psql("insert into t select generate_series(1, 1000), 'payload'")
    log.info("insert with safekeeper down done")
    ep.stop()  # stop compute

    # Stop another safekeeper, and start one which missed timeline creation.
    sk2.stop()
    sk1.start()

    # Start new ep and insert some more. neon_walreader should download WAL for
    # sk1 because it should be filled since the horizon (initial LSN) which is
    # earlier than basebackup LSN.
    ep = env.endpoints.create_start("test_lagging_sk")
    ep.safe_psql("insert into t select generate_series(1,100), 'payload'")
    # stop ep and ensure WAL is identical after recovery.
    wait_flush_lsn_align_by_ep(env, "test_lagging_sk", tenant_id, timeline_id, ep, [sk1, sk3])
    # Check that WALs are the same.
    cmp_sk_wal([sk1, sk3], tenant_id, timeline_id)

    # Now repeat insertion with sk1 down, but with inserting more data to check
    # that WAL on compute is removed.
    sk1.stop()
    sk2.start()

    # min_wal_size must be at least 2x segment size.
    min_wal_config = [
        "min_wal_size=32MB",
        "max_wal_size=32MB",
        "wal_keep_size=0",
        "log_checkpoints=on",
    ]
    ep = env.endpoints.create_start(
        "test_lagging_sk",
        config_lines=min_wal_config,
    )
    with WalChangeLogger(ep, "doing large insert with sk1 down"):
        for _ in range(0, 5):
            fill_segment(ep)
    # there shouldn't be more than 2 WAL segments (but dir may have archive_status files)
    assert ep.get_pg_wal_size() < 16 * 2.5

    sk2.stop()  # stop another sk to ensure sk1 and sk3 can work
    sk1.start()
    with DurationLogger("recovery"):
        ep.safe_psql("insert into t select generate_series(1,100), 'payload'")  # forces recovery
    # stop ep and ensure WAL is identical after recovery.
    wait_flush_lsn_align_by_ep(env, "test_lagging_sk", tenant_id, timeline_id, ep, [sk1, sk3])
    # Check that WALs are the same.
    cmp_sk_wal([sk1, sk3], tenant_id, timeline_id)

    # Now do the same with different safekeeper sk2 down, and restarting ep
    # before recovery (again scenario when recovery starts below basebackup_lsn,
    # but multi segment now).
    ep = env.endpoints.create_start(
        "test_lagging_sk",
        config_lines=["min_wal_size=32MB", "max_wal_size=32MB", "log_checkpoints=on"],
    )
    with WalChangeLogger(ep, "doing large insert with sk2 down"):
        for _ in range(0, 5):
            fill_segment(ep)
    # there shouldn't be more than 2 WAL segments (but dir may have archive_status files)
    assert ep.get_pg_wal_size() < 16 * 2.5

    ep.stop()
    ep = env.endpoints.create_start(
        "test_lagging_sk",
        config_lines=min_wal_config,
    )
    sk2.start()
    with DurationLogger("recovery"):
        wait_flush_lsn_align_by_ep(env, "test_lagging_sk", tenant_id, timeline_id, ep, [sk2, sk3])
    # Check that WALs are the same.
    cmp_sk_wal([sk1, sk2, sk3], tenant_id, timeline_id)

    env.stop(immediate=True)


# Smaller version of test_one_sk_down testing peer recovery in isolation: that
# it works without compute at all.
def test_peer_recovery(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_peer_recovery")
    endpoint = env.endpoints.create_start("test_peer_recovery")

    endpoint.safe_psql("create table t(key int, value text)")
    sk1 = env.safekeepers[0]
    sk2 = env.safekeepers[1]
    sk1_http_cli = sk1.http_client()
    sk2_http_cli = sk2.http_client()
    # ensure tli gets created on sk1, peer recovery won't do that
    wait(
        partial(is_flush_lsn_aligned, [sk1_http_cli, sk2_http_cli], tenant_id, timeline_id),
        "flush_lsn to get aligned",
    )

    sk1 = env.safekeepers[0]
    sk1.stop()

    # roughly fills one segment
    endpoint.safe_psql("insert into t select generate_series(1,250000), 'payload'")

    endpoint.stop()  # stop compute

    # now start safekeeper, but with peer recovery disabled; it should lag for about a segment
    sk1.start(extra_opts=["--peer-recovery=false"])
    sk1_tli_status = sk1_http_cli.timeline_status(tenant_id, timeline_id)
    sk2_tli_status = sk2_http_cli.timeline_status(tenant_id, timeline_id)
    log.info(
        f"flush_lsns after insertion: sk1={sk1_tli_status.flush_lsn}, sk2={sk2_tli_status.flush_lsn}"
    )
    assert sk2_tli_status.flush_lsn - sk1_tli_status.flush_lsn >= 16 * 1024 * 1024

    # wait a bit, lsns shouldn't change
    time.sleep(2)
    sk1_tli_status = sk1_http_cli.timeline_status(tenant_id, timeline_id)
    sk2_tli_status = sk2_http_cli.timeline_status(tenant_id, timeline_id)
    log.info(
        f"flush_lsns after waiting: sk1={sk1_tli_status.flush_lsn}, sk2={sk2_tli_status.flush_lsn}"
    )
    assert sk2_tli_status.flush_lsn - sk1_tli_status.flush_lsn >= 16 * 1024 * 1024

    # now restart safekeeper with peer recovery enabled and wait for recovery
    sk1.stop().start(extra_opts=["--peer-recovery=true"])
    wait(
        partial(is_flush_lsn_aligned, [sk1_http_cli, sk2_http_cli], tenant_id, timeline_id),
        "flush_lsn to get aligned",
    )

    cmp_sk_wal([sk1, sk2], tenant_id, timeline_id)

    # stop one of safekeepers which weren't recovering and insert a bit more to check we can commit
    env.safekeepers[2].stop()
    endpoint = env.endpoints.create_start("test_peer_recovery")
    endpoint.safe_psql("insert into t select generate_series(1,100), 'payload'")


# Test that when compute is terminated in fast (or smart) mode, walproposer is
# allowed to run and self terminate after shutdown checkpoint is written, so it
# commits it to safekeepers before exiting. This not required for correctness,
# but needed for tests using check_restored_datadir_content.
def test_wp_graceful_shutdown(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_wp_graceful_shutdown")
    ep = env.endpoints.create_start("test_wp_graceful_shutdown")
    ep.safe_psql("create table t(key int, value text)")
    ep.stop()

    # figure out checkpoint lsn
    ckpt_lsn = pg_bin.get_pg_controldata_checkpoint_lsn(ep.pg_data_dir_path())

    sk_http_cli = env.safekeepers[0].http_client()
    commit_lsn = sk_http_cli.timeline_status(tenant_id, timeline_id).commit_lsn
    # Note: this is in memory value. Graceful shutdown of walproposer currently
    # doesn't guarantee persisted value, which is ok as we need it only for
    # tests. Persisting it without risking too many cf flushes needs a wp -> sk
    # protocol change. (though in reality shutdown sync-safekeepers does flush
    # of cf, so most of the time persisted value wouldn't lag)
    log.info(f"sk commit_lsn {commit_lsn}")
    # note that ckpt_lsn is the *beginning* of checkpoint record, so commit_lsn
    # must be actually higher
    assert commit_lsn > ckpt_lsn, "safekeeper must have checkpoint record"


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
        self.fake_broker_endpoint = f"http://127.0.0.1:{port_distributor.get_port()}"
        self.pg_bin = pg_bin
        self.num_safekeepers = num_safekeepers
        self.bin_safekeeper = str(neon_binpath / "safekeeper")
        self.safekeepers: list[subprocess.CompletedProcess[Any]] | None = None
        self.postgres: ProposerPostgres | None = None
        self.tenant_id: TenantId | None = None
        self.timeline_id: TimelineId | None = None

    def init(self) -> Self:
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
            self.fake_broker_endpoint,
        ]
        log.info(f'Running command "{" ".join(cmd)}"')

        safekeeper_client = SafekeeperHttpClient(
            port=port.http,
            auth_token=None,
        )
        safekeeper_process = start_in_background(
            cmd, safekeeper_dir, "safekeeper.log", safekeeper_client.check_status
        )
        return safekeeper_process

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
        with open(pid_file) as f:
            pid = int(f.read())
            log.info(f"Killing safekeeper with pid {pid}")
            os.kill(pid, signal.SIGKILL)

    def __enter__(self) -> Self:
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

    def show_statuses(safekeepers: list[Safekeeper], tenant_id: TenantId, timeline_id: TimelineId):
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
    timeline_id = env.create_branch("test_replace_safekeeper")

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
    timeline_id_1 = env.create_branch("br1")  # Active, delete explicitly
    timeline_id_2 = env.create_branch("br2")  # Inactive, delete explicitly
    timeline_id_3 = env.create_branch("br3")  # Active, delete with the tenant
    timeline_id_4 = env.create_branch("br4")  # Inactive, delete with the tenant

    tenant_id_other, timeline_id_other = env.create_tenant()

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
    sk_data_dir = sk.data_dir
    if not auth_enabled:
        sk_http = sk.http_client()
        sk_http_other = sk_http
    else:
        sk_http = sk.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))
        sk_http_other = sk.http_client(
            auth_token=env.auth_keys.generate_tenant_token(tenant_id_other)
        )
        sk_http_noauth = sk.http_client(gen_sk_wide_token=False)
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
    assert sk_http.timeline_delete(tenant_id, timeline_id_1)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_2)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Ensure repeated deletion succeeds
    assert not sk_http.timeline_delete(tenant_id, timeline_id_1)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_2)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    if auth_enabled:
        # Ensure we cannot delete the other tenant
        for sk_h in [sk_http, sk_http_noauth]:
            with pytest.raises(sk_h.HTTPError, match="Forbidden|Unauthorized"):
                assert sk_h.timeline_delete(tenant_id_other, timeline_id_other)
            with pytest.raises(sk_h.HTTPError, match="Forbidden|Unauthorized"):
                assert sk_h.tenant_delete_force(tenant_id_other)
        assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove initial tenant's br2 (inactive)
    assert sk_http.timeline_delete(tenant_id, timeline_id_2)["dir_existed"]
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_1)).exists()
    assert not (sk_data_dir / str(tenant_id) / str(timeline_id_2)).exists()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_3)).is_dir()
    assert (sk_data_dir / str(tenant_id) / str(timeline_id_4)).is_dir()
    assert (sk_data_dir / str(tenant_id_other) / str(timeline_id_other)).is_dir()

    # Remove non-existing branch, should succeed
    assert not sk_http.timeline_delete(tenant_id, TimelineId("00" * 16))["dir_existed"]
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


def test_delete_timeline_under_load(neon_env_builder: NeonEnvBuilder):
    """
    Test deleting timelines on a safekeeper while they're under load.

    This should not happen under normal operation, but it can happen if
    there is some rogue compute/pageserver that is writing/reading to a
    safekeeper that we're migrating a timeline away from, or if the timeline
    is being deleted while such a rogue client is running.
    """
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()

    # Create two endpoints that will generate load
    timeline_id_a = env.create_branch("deleteme_a")
    timeline_id_b = env.create_branch("deleteme_b")

    endpoint_a = env.endpoints.create("deleteme_a")
    endpoint_a.start()
    endpoint_b = env.endpoints.create("deleteme_b")
    endpoint_b.start()

    # Get tenant and timeline IDs
    tenant_id = env.initial_tenant

    # Start generating load on both timelines
    def generate_load(endpoint: Endpoint):
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS t(key int, value text)")
                while True:
                    try:
                        cur.execute("INSERT INTO t SELECT generate_series(1,1000), 'data'")
                    except:  # noqa
                        # Ignore errors since timeline may be deleted
                        break

    t_a = threading.Thread(target=generate_load, args=(endpoint_a,))
    t_b = threading.Thread(target=generate_load, args=(endpoint_b,))
    try:
        t_a.start()
        t_b.start()

        # Let the load run for a bit
        log.info("Warming up...")
        time.sleep(2)

        # Safekeeper errors will propagate to the pageserver: it is correct that these are
        # logged at error severity because they indicate the pageserver is trying to read
        # a timeline that it shouldn't.
        env.pageserver.allowed_errors.extend(
            [
                ".*Timeline.*was cancelled.*",
                ".*Timeline.*was not found.*",
            ]
        )

        # Try deleting timelines while under load
        sk = env.safekeepers[0]
        sk_http = sk.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))

        # Delete first timeline
        log.info(f"Deleting {timeline_id_a}...")
        assert sk_http.timeline_delete(tenant_id, timeline_id_a, only_local=True)["dir_existed"]

        # Delete second timeline
        log.info(f"Deleting {timeline_id_b}...")
        assert sk_http.timeline_delete(tenant_id, timeline_id_b, only_local=True)["dir_existed"]

        # Verify timelines are gone from disk
        sk_data_dir = sk.data_dir
        assert not (sk_data_dir / str(tenant_id) / str(timeline_id_a)).exists()
        # assert not (sk_data_dir / str(tenant_id) / str(timeline_id_b)).exists()

    finally:
        log.info("Stopping endpoints...")
        # Stop endpoints with immediate mode because we deleted the timeline out from under the compute, which may cause it to hang
        endpoint_a.stop(mode="immediate")
        endpoint_b.stop(mode="immediate")
        log.info("Joining threads...")
        t_a.join()
        t_b.join()


# Basic pull_timeline test.
# When live_sk_change is False, compute is restarted to change set of
# safekeepers; otherwise it is live reload.
@pytest.mark.parametrize("live_sk_change", [False, True])
def test_pull_timeline(neon_env_builder: NeonEnvBuilder, live_sk_change: bool):
    neon_env_builder.auth_enabled = True

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

    def show_statuses(safekeepers: list[Safekeeper], tenant_id: TenantId, timeline_id: TimelineId):
        for sk in safekeepers:
            http_cli = sk.http_client(auth_token=env.auth_keys.generate_tenant_token(tenant_id))
            try:
                status = http_cli.timeline_status(tenant_id, timeline_id)
                log.info(f"Safekeeper {sk.id} status: {status}")
            except Exception as e:
                log.info(f"Safekeeper {sk.id} status error: {e}")

    neon_env_builder.num_safekeepers = 4
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    log.info("Use only first 3 safekeepers")
    env.safekeepers[3].stop()
    endpoint = env.endpoints.create("main")
    endpoint.start(safekeepers=[1, 2, 3])

    execute_payload(endpoint)
    show_statuses(env.safekeepers, tenant_id, timeline_id)

    log.info("Kill safekeeper 2, continue with payload")
    env.safekeepers[1].stop(immediate=True)
    execute_payload(endpoint)

    log.info("Initialize new safekeeper 4, pull data from 1 & 3")
    env.safekeepers[3].start()

    res = env.safekeepers[3].pull_timeline(
        [env.safekeepers[0], env.safekeepers[2]], tenant_id, timeline_id
    )
    log.info("Finished pulling timeline")
    log.info(res)

    show_statuses(env.safekeepers, tenant_id, timeline_id)

    action = "reconfiguing" if live_sk_change else "restarting"
    log.info(f"{action} compute with new config to verify that it works")
    new_sks = [1, 3, 4]
    if not live_sk_change:
        endpoint.stop_and_destroy().create("main")
        endpoint.start(safekeepers=new_sks)
    else:
        endpoint.reconfigure(safekeepers=new_sks)

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


# Test pull_timeline while concurrently gc'ing WAL on safekeeper:
# 1) Start pull_timeline, listing files to fetch.
# 2) Write segment, do gc.
# 3) Finish pull_timeline.
# 4) Do some write, verify integrity with timeline_digest.
# Expected to fail while holding off WAL gc plus fetching commit_lsn WAL
# segment is not implemented.
def test_pull_timeline_gc(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_safekeeper_remote_storage(default_remote_storage())
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    (src_sk, dst_sk) = (env.safekeepers[0], env.safekeepers[2])

    log.info("use only first 2 safekeepers, 3rd will be seeded")
    endpoint = env.endpoints.create("main")
    endpoint.active_safekeepers = [1, 2]
    endpoint.start()
    endpoint.safe_psql("create table t(key int, value text)")
    endpoint.safe_psql("insert into t select generate_series(1, 1000), 'pear'")

    src_flush_lsn = src_sk.get_flush_lsn(tenant_id, timeline_id)
    log.info(f"flush_lsn on src before pull_timeline: {src_flush_lsn}")

    src_http = src_sk.http_client()
    # run pull_timeline which will halt before downloading files
    src_http.configure_failpoints(("sk-snapshot-after-list-pausable", "pause"))
    pt_handle = PropagatingThread(
        target=dst_sk.pull_timeline, args=([src_sk], tenant_id, timeline_id)
    )
    pt_handle.start()
    src_sk.wait_until_paused("sk-snapshot-after-list-pausable")

    # ensure segment exists
    endpoint.safe_psql("insert into t select generate_series(1, 180000), 'papaya'")
    lsn = last_flush_lsn_upload(
        env,
        endpoint,
        tenant_id,
        timeline_id,
        auth_token=env.auth_keys.generate_tenant_token(tenant_id),
    )
    assert lsn > Lsn("0/2000000")
    # Checkpoint timeline beyond lsn.
    src_sk.checkpoint_up_to(tenant_id, timeline_id, lsn, wait_wal_removal=False)
    first_segment_p = src_sk.timeline_dir(tenant_id, timeline_id) / "000000010000000000000001"
    log.info(f"first segment exist={os.path.exists(first_segment_p)}")

    src_http.configure_failpoints(("sk-snapshot-after-list-pausable", "off"))
    pt_handle.join()

    # after pull_timeline is finished WAL should be removed on donor
    src_sk.checkpoint_up_to(tenant_id, timeline_id, lsn, wait_wal_removal=True)

    timeline_start_lsn = src_sk.get_timeline_start_lsn(tenant_id, timeline_id)
    dst_flush_lsn = dst_sk.get_flush_lsn(tenant_id, timeline_id)
    log.info(f"flush_lsn on dst after pull_timeline: {dst_flush_lsn}")
    assert dst_flush_lsn >= src_flush_lsn
    digests = [
        sk.http_client().timeline_digest(tenant_id, timeline_id, timeline_start_lsn, dst_flush_lsn)
        for sk in [src_sk, dst_sk]
    ]
    assert digests[0] == digests[1], f"digest on src is {digests[0]} but on dst is {digests[1]}"


# Test pull_timeline while concurrently changing term on the donor:
# 1) Start pull_timeline, listing files to fetch.
# 2) Change term on the donor
# 3) Finish pull_timeline.
#
# Currently (until proper membership change procedure), we want to pull_timeline
# to fetch the log up to <last_log_term, flush_lsn>. This is unsafe if term
# changes during the procedure (unless timeline is locked all the time but we
# don't want that): recepient might end up with mix of WAL from different
# histories. Thus the schedule above is expected to fail. Later we'd allow
# pull_timeline to only initialize timeline to any valid state (up to
# commit_lsn), holding switch to fully new configuration until it recovers
# enough, so it won't be affected by term change anymore.
#
# Expected to fail while term check is not implemented.
def test_pull_timeline_term_change(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_safekeeper_remote_storage(default_remote_storage())
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    (src_sk, dst_sk) = (env.safekeepers[0], env.safekeepers[2])

    log.info("use only first 2 safekeepers, 3rd will be seeded")
    ep = env.endpoints.create("main")
    ep.active_safekeepers = [1, 2]
    ep.start()
    ep.safe_psql("create table t(key int, value text)")
    ep.safe_psql("insert into t select generate_series(1, 1000), 'pear'")

    src_http = src_sk.http_client()
    # run pull_timeline which will halt before downloading files
    src_http.configure_failpoints(("sk-snapshot-after-list-pausable", "pause"))
    pt_handle = PropagatingThread(
        target=dst_sk.pull_timeline, args=([src_sk], tenant_id, timeline_id)
    )
    pt_handle.start()
    src_sk.wait_until_paused("sk-snapshot-after-list-pausable")

    src_http = src_sk.http_client()
    term_before = src_http.timeline_status(tenant_id, timeline_id).term

    # restart compute to bump term
    ep.stop()
    ep = env.endpoints.create("main")
    ep.active_safekeepers = [1, 2]
    ep.start()
    ep.safe_psql("insert into t select generate_series(1, 100), 'pear'")

    term_after = src_http.timeline_status(tenant_id, timeline_id).term
    assert term_after > term_before, f"term_after={term_after}, term_before={term_before}"

    src_http.configure_failpoints(("sk-snapshot-after-list-pausable", "off"))
    with pytest.raises(requests.exceptions.HTTPError):
        pt_handle.join()


def test_pull_timeline_while_evicted(neon_env_builder: NeonEnvBuilder):
    """
    Verify that when pull_timeline is used on an evicted timeline, it does not result in
    promoting any segments to local disk on the source, and the timeline is correctly instantiated
    in evicted state on the destination.  This behavior is important to avoid ballooning disk
    usage when doing mass migration of timelines.
    """
    neon_env_builder.num_safekeepers = 4
    neon_env_builder.enable_safekeeper_remote_storage(default_remote_storage())

    # Configure safekeepers with ultra-fast eviction policy
    neon_env_builder.safekeeper_extra_opts = [
        "--enable-offload",
        "--partial-backup-timeout",
        "50ms",
        "--control-file-save-interval",
        "1s",
        # Safekeepers usually wait a while before evicting something: for this test we want them to
        # evict things as soon as they are inactive.
        "--eviction-min-resident=100ms",
        "--delete-offloaded-wal",
    ]

    initial_tenant_conf = {"lagging_wal_timeout": "1s", "checkpoint_timeout": "100ms"}
    env = neon_env_builder.init_start(initial_tenant_conf=initial_tenant_conf)
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    (src_sk, dst_sk) = (env.safekeepers[0], env.safekeepers[-1])
    log.info(f"Will pull_timeline on destination {dst_sk.id} from source {src_sk.id}")

    ep = env.endpoints.create("main")
    ep.active_safekeepers = [s.id for s in env.safekeepers if s.id != dst_sk.id]
    log.info(f"Compute writing initially to safekeepers: {ep.active_safekeepers}")
    ep.active_safekeepers = [1, 2, 3]  # Exclude dst_sk from set written by compute initially
    ep.start()
    ep.safe_psql("CREATE TABLE t(i int)")
    ep.safe_psql("INSERT INTO t VALUES (0)")
    ep.stop()

    wait_lsn_force_checkpoint_at_sk(src_sk, tenant_id, timeline_id, env.pageserver)

    src_http = src_sk.http_client()
    dst_http = dst_sk.http_client()

    def evicted_on_source():
        # Wait for timeline to go into evicted state
        assert src_http.get_eviction_state(timeline_id) != "Present"
        assert (
            src_http.get_metric_value(
                "safekeeper_eviction_events_completed_total", {"kind": "evict"}
            )
            or 0 > 0
        )
        assert src_http.get_metric_value("safekeeper_evicted_timelines") or 0 > 0
        # Check that on source no segment files are present
        assert src_sk.list_segments(tenant_id, timeline_id) == []

    wait_until(evicted_on_source, timeout=60)

    # Invoke pull_timeline: source should serve snapshot request without promoting anything to local disk,
    # destination should import the control file only & go into evicted mode immediately
    dst_sk.pull_timeline([src_sk], tenant_id, timeline_id)

    # Check that on source and destination no segment files are present
    assert src_sk.list_segments(tenant_id, timeline_id) == []
    assert dst_sk.list_segments(tenant_id, timeline_id) == []

    # Check that the timeline on the destination is in the expected evicted state.
    evicted_on_source()  # It should still be evicted on the source

    def evicted_on_destination():
        assert dst_http.get_eviction_state(timeline_id) != "Present"
        assert dst_http.get_metric_value("safekeeper_evicted_timelines") or 0 > 0

    # This should be fast, it is a wait_until because eviction state is updated
    # in the background wrt pull_timeline.
    wait_until(evicted_on_destination, timeout=1.0, interval=0.1)

    # Delete the timeline on the source, to prove that deletion works on an
    # evicted timeline _and_ that the final compute test is really not using
    # the original location
    src_sk.http_client().timeline_delete(tenant_id, timeline_id, only_local=True)

    # Check that using the timeline correctly un-evicts it on the new location
    ep.active_safekeepers = [2, 3, 4]
    ep.start()
    ep.safe_psql("INSERT INTO t VALUES (0)")
    ep.stop()

    def unevicted_on_dest():
        assert (
            dst_http.get_metric_value(
                "safekeeper_eviction_events_completed_total", {"kind": "restore"}
            )
            or 0 > 0
        )
        n_evicted = dst_sk.http_client().get_metric_value("safekeeper_evicted_timelines")
        assert n_evicted == 0

    wait_until(unevicted_on_dest, interval=0.1, timeout=1.0)


# Basic test for http API membership related calls: create timeline and switch
# configuration. Normally these are called by storage controller, but this
# allows to test them separately.
@run_only_on_default_postgres("tests only safekeeper API")
def test_membership_api(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    sk = env.safekeepers[0]
    http_cli = sk.http_client()

    sk_id_1 = SafekeeperId(env.safekeepers[0].id, "localhost", sk.port.pg_tenant_only)
    sk_id_2 = SafekeeperId(11, "localhost", 5434)  # just a mock

    # Request to switch before timeline creation should fail.
    init_conf = Configuration(generation=1, members=[sk_id_1], new_members=None)
    with pytest.raises(requests.exceptions.HTTPError):
        http_cli.membership_switch(tenant_id, timeline_id, init_conf)

    # Create timeline.
    create_r = TimelineCreateRequest(
        tenant_id, timeline_id, init_conf, 150002, Lsn("0/1000000"), commit_lsn=None
    )
    log.info(f"sending {create_r.to_json()}")
    http_cli.timeline_create(create_r)

    # Switch into some conf.
    joint_conf = Configuration(generation=4, members=[sk_id_1], new_members=[sk_id_2])
    resp = http_cli.membership_switch(tenant_id, timeline_id, joint_conf)
    log.info(f"joint switch resp: {resp}")
    assert resp.previous_conf.generation == 1
    assert resp.current_conf.generation == 4

    # Restart sk, conf should be preserved.
    sk.stop().start()
    after_restart = http_cli.get_membership(tenant_id, timeline_id)
    log.info(f"conf after restart: {after_restart}")
    assert after_restart.generation == 4

    # Switch into disjoint conf.
    non_joint = Configuration(generation=5, members=[sk_id_2], new_members=None)
    resp = http_cli.membership_switch(tenant_id, timeline_id, non_joint)
    log.info(f"non joint switch resp: {resp}")
    assert resp.previous_conf.generation == 4
    assert resp.current_conf.generation == 5

    # Switch request to lower conf should be ignored.
    lower_conf = Configuration(generation=3, members=[], new_members=None)
    resp = http_cli.membership_switch(tenant_id, timeline_id, lower_conf)
    log.info(f"lower switch resp: {resp}")
    assert resp.previous_conf.generation == 5
    assert resp.current_conf.generation == 5


# In this test we check for excessive START_REPLICATION and START_WAL_PUSH queries
# when compute is active, but there are no writes to the timeline. In that case
# pageserver should maintain a single connection to safekeeper and don't attempt
# to reconnect extra times.
#
# The only way to verify this without manipulating time is to sleep for a while.
# In this test we sleep for 60 seconds, so this test takes at least 1 minute to run.
# This is longer than most other tests, we run it only for v16 to save CI resources.
@run_only_on_default_postgres("run only on release build to save CI resources")
@skip_in_debug_build("run only on release build to save CI resources")
def test_idle_reconnections(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_idle_reconnections")

    def collect_stats() -> dict[str, float]:
        # we need to collect safekeeper_pg_queries_received_total metric from all safekeepers
        sk_metrics = [
            parse_metrics(sk.http_client().get_metrics_str(), f"safekeeper_{sk.id}")
            for sk in env.safekeepers
        ]

        total: dict[str, float] = {}

        for sk in sk_metrics:
            queries_received = sk.query_all("safekeeper_pg_queries_received_total")
            log.info(f"{sk.name} queries received: {queries_received}")
            for sample in queries_received:
                total[sample.labels["query"]] = total.get(sample.labels["query"], 0) + sample.value

        log.info(f"Total queries received: {total}")

        # in the perfect world, we should see only one START_REPLICATION query,
        # here we check for 5 to prevent flakiness
        assert total.get("START_REPLICATION", 0) <= 5

        # in the perfect world, we should see ~6 START_WAL_PUSH queries,
        # here we check for 15 to prevent flakiness
        assert total.get("START_WAL_PUSH", 0) <= 15

        return total

    collect_stats()

    endpoint = env.endpoints.create_start("test_idle_reconnections")
    # just write something to the timeline
    endpoint.safe_psql("create table t(i int)")
    collect_stats()

    # sleep a bit
    time.sleep(30)

    # force checkpoint in pageserver to advance remote_consistent_lsn
    wait_lsn_force_checkpoint(tenant_id, timeline_id, endpoint, env.pageserver)

    collect_stats()

    time.sleep(30)

    final_stats = collect_stats()
    # pageserver should connect to safekeepers at least once
    assert final_stats.get("START_REPLICATION", 0) >= 1
    # walproposer should connect to each safekeeper at least once
    assert final_stats.get("START_WAL_PUSH", 0) >= 3


@pytest.mark.parametrize("insert_rows", [0, 100, 100000, 500000])
def test_timeline_copy(neon_env_builder: NeonEnvBuilder, insert_rows: int):
    target_percents = [10, 50, 90, 100]

    neon_env_builder.num_safekeepers = 3
    # we need remote storage that supports copy_object S3 API
    neon_env_builder.enable_safekeeper_remote_storage(RemoteStorageKind.MOCK_S3)
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    endpoint = env.endpoints.create_start("main")

    lsns = []

    def remember_lsn():
        lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
        lsns.append(lsn)
        return lsn

    # remember LSN right after timeline creation
    lsn = remember_lsn()
    log.info(f"LSN after timeline creation: {lsn}")

    endpoint.safe_psql("create table t(key int, value text)")

    # Note: currently timelines on sks are created by compute and commit of
    # transaction above is finished when 2/3 sks received it, so there is a
    # small chance that timeline on this sk is not created/initialized yet,
    # hence the usage of waiting function to prevent flakiness.
    timeline_start_lsn = (
        env.safekeepers[0].http_client().get_non_zero_timeline_start_lsn(tenant_id, timeline_id)
    )
    log.info(f"Timeline start LSN: {timeline_start_lsn}")

    current_percent = 0.0
    for new_percent in target_percents:
        new_rows = insert_rows * (new_percent - current_percent) / 100
        current_percent = new_percent

        if new_rows == 0:
            continue

        endpoint.safe_psql(
            f"insert into t select generate_series(1, {new_rows}), repeat('payload!', 10)"
        )

        # remember LSN right after reaching new_percent
        lsn = remember_lsn()
        log.info(f"LSN after inserting {new_rows} rows: {lsn}")

    # TODO: would be also good to test cases where not all segments are uploaded to S3

    for lsn in lsns:
        new_timeline_id = TimelineId.generate()
        log.info(f"Copying branch for LSN {lsn}, to timeline {new_timeline_id}")

        orig_digest = (
            env.safekeepers[0]
            .http_client()
            .timeline_digest(tenant_id, timeline_id, timeline_start_lsn, lsn)
        )
        log.info(f"Original digest: {orig_digest}")

        for sk in env.safekeepers:
            wait(
                partial(is_flush_lsn_caught_up, sk, tenant_id, timeline_id, lsn),
                f"sk_id={sk.id} to flush {lsn}",
            )

            sk.http_client().copy_timeline(
                tenant_id,
                timeline_id,
                {
                    "target_timeline_id": str(new_timeline_id),
                    "until_lsn": str(lsn),
                },
            )

            new_digest = sk.http_client().timeline_digest(
                tenant_id, new_timeline_id, timeline_start_lsn, lsn
            )
            log.info(f"Digest after timeline copy on safekeeper {sk.id}: {new_digest}")

            assert orig_digest == new_digest

    # TODO: test timelines can start after copy


def test_patch_control_file(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    endpoint = env.endpoints.create_start("main")
    # initialize safekeeper
    endpoint.safe_psql("create table t(key int, value text)")

    # update control file
    res = (
        env.safekeepers[0]
        .http_client()
        .patch_control_file(
            tenant_id,
            timeline_id,
            {
                "timeline_start_lsn": "0/1",
            },
        )
    )

    timeline_start_lsn_before = res["old_control_file"]["timeline_start_lsn"]
    timeline_start_lsn_after = res["new_control_file"]["timeline_start_lsn"]

    log.info(f"patch_control_file response: {res}")
    log.info(
        f"updated control file timeline_start_lsn, before {timeline_start_lsn_before}, after {timeline_start_lsn_after}"
    )

    assert timeline_start_lsn_after == "0/1"
    env.safekeepers[0].stop().start()

    # wait/check that safekeeper is alive
    endpoint.safe_psql("insert into t values (1, 'payload')")

    # check that timeline_start_lsn is updated
    res = (
        env.safekeepers[0]
        .http_client()
        .debug_dump({"dump_control_file": "true", "timeline_id": str(timeline_id)})
    )
    log.info(f"dump_control_file response: {res}")
    assert res["timelines"][0]["control_file"]["timeline_start_lsn"] == "0/1"


def test_term_bump(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    endpoint = env.endpoints.create_start("main")
    # initialize safekeeper
    endpoint.safe_psql("create table t(key int, value text)")

    http_cli = env.safekeepers[0].http_client()

    # check that bump up to specific term works
    curr_term = http_cli.timeline_status(tenant_id, timeline_id).term
    bump_to = curr_term + 3
    res = http_cli.term_bump(tenant_id, timeline_id, bump_to)
    log.info(f"bump to {bump_to} res: {res}")
    assert res.current_term >= bump_to

    # check that bump to none increments current term
    res = http_cli.term_bump(tenant_id, timeline_id, None)
    log.info(f"bump to None res: {res}")
    assert res.current_term > bump_to
    assert res.current_term > res.previous_term

    # check that bumping doesn't work downward
    res = http_cli.term_bump(tenant_id, timeline_id, 2)
    log.info(f"bump to 2 res: {res}")
    assert res.current_term > bump_to
    assert res.current_term == res.previous_term

    # check that this doesn't kill endpoint because last WAL flush was his and
    # thus its basebackup is still good
    endpoint.safe_psql("insert into t values (1, 'payload')")


# Test disables periodic pushes from safekeeper to the broker and checks that
# pageserver can still discover safekeepers with discovery requests.
def test_broker_discovery(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_safekeeper_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    env.create_branch("test_broker_discovery")

    endpoint = env.endpoints.create_start(
        "test_broker_discovery",
        config_lines=["shared_buffers=1MB"],
    )
    endpoint.safe_psql("create table t(i int, payload text)")
    # Install extension containing function needed to clear buffer
    endpoint.safe_psql("CREATE EXTENSION neon_test_utils")

    def do_something():
        time.sleep(1)
        # generate some data to commit WAL on safekeepers
        endpoint.safe_psql("insert into t select generate_series(1,100), 'action'")
        # clear the buffers
        endpoint.clear_buffers()
        # read data to fetch pages from pageserver
        endpoint.safe_psql("select sum(i) from t")

    do_something()
    do_something()

    for sk in env.safekeepers:
        # Disable periodic broker push, so pageserver won't be able to discover
        # safekeepers without sending a discovery request
        sk.stop().start(extra_opts=["--disable-periodic-broker-push"])

    do_something()
    do_something()

    # restart pageserver and check how everything works
    env.pageserver.stop().start()

    do_something()
    do_something()


# Test creates 5 endpoints and tries to wake them up randomly. All timeouts are
# configured to be very short, so that we expect that:
# - pageserver will update remote_consistent_lsn very often
# - safekeepers will upload partial WAL segments very often
# - safekeeper will try to evict and unevict timelines
#
# Test checks that there are no critical errors while doing this. Also it checks
# that every safekeeper has at least one successful eviction.
@pytest.mark.parametrize("delete_offloaded_wal", [False, True])
@pytest.mark.parametrize("restart_chance", [0.0, 0.2])
def test_s3_eviction(
    neon_env_builder: NeonEnvBuilder, delete_offloaded_wal: bool, restart_chance: float
):
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_safekeeper_remote_storage(RemoteStorageKind.LOCAL_FS)

    neon_env_builder.safekeeper_extra_opts = [
        "--enable-offload",
        "--partial-backup-timeout",
        "50ms",
        "--control-file-save-interval",
        "1s",
        # Safekeepers usually wait a while before evicting something: for this test we want them to
        # evict things as soon as they are inactive.
        "--eviction-min-resident=100ms",
    ]
    if delete_offloaded_wal:
        neon_env_builder.safekeeper_extra_opts.append("--delete-offloaded-wal")
    # make lagging_wal_timeout small to force pageserver quickly forget about
    # safekeeper after it stops sending updates (timeline is deactivated) to
    # make test faster. Won't be needed with
    # https://github.com/neondatabase/neon/issues/8148 fixed.
    initial_tenant_conf = {"lagging_wal_timeout": "1s", "checkpoint_timeout": "100ms"}
    env = neon_env_builder.init_start(initial_tenant_conf=initial_tenant_conf)

    n_timelines = 5

    branch_names = [f"branch{tlin}" for tlin in range(n_timelines)]
    timelines = []
    ps_client = env.pageservers[0].http_client()

    # start postgres on each timeline
    endpoints: list[Endpoint] = []
    for branch_name in branch_names:
        timeline_id = env.create_branch(branch_name)
        timelines.append(timeline_id)

        endpoints.append(env.endpoints.create_start(branch_name))
        endpoints[-1].safe_psql("CREATE TABLE t(i int)")
        endpoints[-1].safe_psql("INSERT INTO t VALUES (0)")

        lsn = endpoints[-1].safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0]
        log.info(f"{branch_name}: LSN={lsn}")

        endpoints[-1].stop()

        # update remote_consistent_lsn on pageserver
        ps_client.timeline_checkpoint(env.initial_tenant, timelines[-1], wait_until_uploaded=True)

    check_values = [0] * n_timelines

    event_metrics_seen = False

    n_iters = 20
    for _ in range(n_iters):
        if log.isEnabledFor(logging.DEBUG):
            for j in range(n_timelines):
                detail = ps_client.timeline_detail(env.initial_tenant, timelines[j])
                log.debug(
                    f'{branch_names[j]}: RCL={detail["remote_consistent_lsn"]}, LRL={detail["last_record_lsn"]}'
                )

        i = random.randint(0, n_timelines - 1)
        log.info(f"Starting endpoint {i}")
        endpoints[i].start()
        check_values[i] += 1
        res = endpoints[i].safe_psql("UPDATE t SET i = i + 1 RETURNING i")
        assert res[0][0] == check_values[i]

        lsn = endpoints[i].safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0]
        log.info(f"{branch_names[i]}: LSN={lsn}")

        endpoints[i].stop()

        # update remote_consistent_lsn on pageserver
        ps_client.timeline_checkpoint(env.initial_tenant, timelines[i], wait_until_uploaded=True)

        # Do metrics check before restarts, since these will reset to zero across a restart
        event_metrics_seen |= any(
            sk.http_client().get_metric_value(
                "safekeeper_eviction_events_started_total", {"kind": "evict"}
            )
            or 0 > 0
            and sk.http_client().get_metric_value(
                "safekeeper_eviction_events_completed_total", {"kind": "evict"}
            )
            or 0 > 0
            and sk.http_client().get_metric_value(
                "safekeeper_eviction_events_started_total", {"kind": "restore"}
            )
            or 0 > 0
            and sk.http_client().get_metric_value(
                "safekeeper_eviction_events_completed_total", {"kind": "restore"}
            )
            or 0 > 0
            for sk in env.safekeepers
        )

        # restarting random safekeepers
        for sk in env.safekeepers:
            if random.random() < restart_chance:
                sk.stop().start()
        time.sleep(0.5)

    # require at least one successful eviction in at least one safekeeper
    # TODO: require eviction in each safekeeper after https://github.com/neondatabase/neon/issues/8148 is fixed
    assert any(
        sk.log_contains("successfully evicted timeline")
        and sk.log_contains("successfully restored evicted timeline")
        for sk in env.safekeepers
    )
    assert event_metrics_seen

    # test safekeeper_evicted_timelines metric
    log.info("testing safekeeper_evicted_timelines metric")
    # checkpoint pageserver to force remote_consistent_lsn update
    for i in range(n_timelines):
        ps_client.timeline_checkpoint(env.initial_tenant, timelines[i], wait_until_uploaded=True)
    for ep in endpoints:
        log.info(ep.is_running())
    sk = env.safekeepers[0]

    # all timelines must be evicted eventually
    def all_evicted():
        n_evicted = sk.http_client().get_metric_value("safekeeper_evicted_timelines")
        assert n_evicted  # make mypy happy
        assert int(n_evicted) == n_timelines

    wait_until(all_evicted, timeout=30)
    # restart should preserve the metric value
    sk.stop().start()
    wait_until(all_evicted)
    # and endpoint start should reduce is
    endpoints[0].start()

    def one_unevicted():
        n_evicted = sk.http_client().get_metric_value("safekeeper_evicted_timelines")
        assert n_evicted  # make mypy happy
        assert int(n_evicted) < n_timelines

    wait_until(one_unevicted)


# Test resetting uploaded partial segment state.
def test_backup_partial_reset(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 1
    neon_env_builder.enable_safekeeper_remote_storage(default_remote_storage())
    # We want to upload/evict quickly, but not too quickly to check that s3 is
    # empty before next round of upload happens.
    # Note: this test fails with --delete-offloaded-wal, this is expected.
    neon_env_builder.safekeeper_extra_opts = [
        "--enable-offload",
        "--partial-backup-timeout",
        "1s",
        "--control-file-save-interval",
        "1s",
        "--eviction-min-resident=1s",
    ]
    # XXX: pageserver currently connects to safekeeper as long as connection
    # manager doesn't remove its entry (default lagging_wal_timeout is 10s),
    # causing uneviction. It should be fixed to not reconnect if last
    # remote_consistent_lsn is communicated and there is nothing to fetch. Make
    # value lower to speed up the test.
    initial_tenant_conf = {
        "lagging_wal_timeout": "1s",
    }
    env = neon_env_builder.init_start(initial_tenant_conf)

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    endpoint = env.endpoints.create("main")
    endpoint.start()
    endpoint.safe_psql("create table t(key int, value text)")
    endpoint.stop()
    sk = env.safekeepers[0]
    # eviction won't happen until remote_consistent_lsn catches up.
    wait_lsn_force_checkpoint_at_sk(sk, tenant_id, timeline_id, env.pageserver)

    http_cli = env.safekeepers[0].http_client()

    # wait until eviction happens
    def evicted():
        eviction_state = http_cli.get_eviction_state(timeline_id)
        log.info(f"eviction_state: {eviction_state}")
        if isinstance(eviction_state, str) and eviction_state == "Present":
            raise Exception("eviction didn't happen yet")

    wait_until(evicted)
    # it must have uploaded something
    uploaded_segs = sk.list_uploaded_segments(tenant_id, timeline_id)
    log.info(f"uploaded segments before reset: {uploaded_segs}")
    assert len(uploaded_segs) > 0

    reset_res = http_cli.backup_partial_reset(tenant_id, timeline_id)
    log.info(f"reset res: {reset_res}")

    # Backup_partial_reset must have reset the state and dropped s3 segment.
    #
    # Note: if listing takes more than --partial-backup-timeout test becomes
    # flaky because file might be reuploaded. With local fs it shouldn't be an
    # issue, but can add retry if this appears.
    uploaded_segs = sk.list_uploaded_segments(tenant_id, timeline_id)
    log.info(f"uploaded segments after reset: {uploaded_segs}")
    assert len(uploaded_segs) == 0

    # calling second time should be ok
    http_cli.backup_partial_reset(tenant_id, timeline_id)

    # inserting data should be ok
    endpoint.start()
    endpoint.safe_psql("insert into t values(1, 'hehe')")


def test_pull_timeline_partial_segment_integrity(neon_env_builder: NeonEnvBuilder):
    """
    Verify that pulling timeline from a SK with an uploaded partial segment
    does not lead to consistency issues:
    1. Start 3 SKs - only use two
    2. Ingest a bit of WAL
    3. Wait for partial to be uploaded
    4. Pull timeline to the third SK
    6. Replace source with destination SK and start compute
    5. Wait for source SK to evict timeline
    6. Go back to initial compute SK config and validate that
    source SK can unevict the timeline (S3 state is consistent)
    """
    neon_env_builder.auth_enabled = True
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_safekeeper_remote_storage(default_remote_storage())

    neon_env_builder.safekeeper_extra_opts = [
        "--enable-offload",
        "--delete-offloaded-wal",
        "--partial-backup-timeout",
        "500ms",
        "--control-file-save-interval",
        "500ms",
        "--eviction-min-resident=500ms",
    ]

    # XXX: pageserver currently connects to safekeeper as long as connection
    # manager doesn't remove its entry (default lagging_wal_timeout is 10s),
    # causing uneviction. It should be fixed to not reconnect if last
    # remote_consistent_lsn is communicated and there is nothing to fetch. Until
    # this is fixed make value lower to speed up the test.
    initial_tenant_conf = {
        "lagging_wal_timeout": "1s",
        "checkpoint_timeout": "100ms",
    }
    env = neon_env_builder.init_start(initial_tenant_conf=initial_tenant_conf)
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    (src_sk, dst_sk) = (env.safekeepers[0], env.safekeepers[2])

    log.info("use only first 2 safekeepers, 3rd will be seeded")
    endpoint = env.endpoints.create("main")
    endpoint.active_safekeepers = [1, 2]
    endpoint.start()
    endpoint.safe_psql("create table t(key int, value text)")
    endpoint.safe_psql("insert into t select generate_series(1, 180000), 'papaya'")

    endpoint.stop()

    def source_partial_segment_uploaded():
        first_segment_name = "000000010000000000000001"
        segs = src_sk.list_uploaded_segments(tenant_id, timeline_id)

        candidate_seg = None
        for seg in segs:
            if "partial" in seg and "sk1" in seg and not seg.startswith(first_segment_name):
                candidate_seg = seg

        if candidate_seg is not None:
            # The term might change, causing the segment to be gc-ed shortly after,
            # so give it a bit of time to make sure it's stable.
            time.sleep(2)

            segs = src_sk.list_uploaded_segments(tenant_id, timeline_id)
            assert candidate_seg in segs
            return candidate_seg

        raise Exception("Partial segment not uploaded yet")

    source_partial_segment = wait_until(source_partial_segment_uploaded)
    log.info(
        f"Uploaded segments before pull are {src_sk.list_uploaded_segments(tenant_id, timeline_id)}"
    )
    log.info(f"Tracking source partial segment: {source_partial_segment}")

    src_flush_lsn = src_sk.get_flush_lsn(tenant_id, timeline_id)
    log.info(f"flush_lsn on src before pull_timeline: {src_flush_lsn}")

    pageserver_conn_options = {"password": env.auth_keys.generate_tenant_token(tenant_id)}
    wait_lsn_force_checkpoint_at(
        src_flush_lsn, tenant_id, timeline_id, env.pageserver, pageserver_conn_options
    )

    dst_sk.pull_timeline([src_sk], tenant_id, timeline_id)

    def evicted():
        evictions = src_sk.http_client().get_metric_value(
            "safekeeper_eviction_events_completed_total", {"kind": "evict"}
        )

        if evictions is None or evictions == 0:
            raise Exception("Eviction did not happen on source safekeeper yet")

    wait_until(evicted)

    endpoint.start(safekeepers=[2, 3])

    def new_partial_segment_uploaded():
        segs = dst_sk.list_uploaded_segments(tenant_id, timeline_id)
        for seg in segs:
            if "partial" in seg and "sk3" in seg:
                return seg

        raise Exception("Partial segment not uploaded yet")

    log.info(
        f"Uploaded segments before post-pull ingest are {src_sk.list_uploaded_segments(tenant_id, timeline_id)}"
    )

    endpoint.safe_psql("insert into t select generate_series(1, 1000), 'pear'")
    wait_until(new_partial_segment_uploaded)

    log.info(
        f"Uploaded segments after post-pull ingest are {src_sk.list_uploaded_segments(tenant_id, timeline_id)}"
    )

    # Allow for some gc iterations to happen and assert that the original
    # uploaded partial segment remains in place.
    time.sleep(5)
    segs = src_sk.list_uploaded_segments(tenant_id, timeline_id)
    assert source_partial_segment in segs

    log.info(
        f"Uploaded segments at the end are {src_sk.list_uploaded_segments(tenant_id, timeline_id)}"
    )

    # Restart the endpoint in order to check that the source safekeeper
    # can unevict the timeline
    endpoint.stop()
    endpoint.start(safekeepers=[1, 2])

    def unevicted():
        unevictions = src_sk.http_client().get_metric_value(
            "safekeeper_eviction_events_completed_total", {"kind": "restore"}
        )

        if unevictions is None or unevictions == 0:
            raise Exception("Uneviction did not happen on source safekeeper yet")

    wait_until(unevicted)
