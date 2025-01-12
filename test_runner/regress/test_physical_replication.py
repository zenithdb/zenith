from __future__ import annotations

import random
import time
from typing import TYPE_CHECKING

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import wait_replica_caughtup

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def test_physical_replication(neon_simple_env: NeonEnv):
    env = neon_simple_env
    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        with primary.connect() as p_con:
            with p_con.cursor() as p_cur:
                p_cur.execute(
                    "CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))"
                )
        with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
            wait_replica_caughtup(primary, secondary)
            with primary.connect() as p_con:
                with p_con.cursor() as p_cur:
                    with secondary.connect() as s_con:
                        with s_con.cursor() as s_cur:
                            runtime_secs = 30
                            started_at = time.time()
                            pk = 0
                            while True:
                                pk += 1
                                now = time.time()
                                if now - started_at > runtime_secs:
                                    break
                                p_cur.execute("insert into t (pk) values (%s)", (pk,))
                                # an earlier version of this test was based on a fixed number of loop iterations
                                # and selected for pk=(random.randrange(1, fixed number of loop iterations)).
                                # => the probability of selection for a value that was never inserted changed from 99.9999% to 0% over the course of the test.
                                #
                                # We changed the test to where=(random.randrange(1, 2*pk)), which means the probability is now fixed to 50%.
                                s_cur.execute(
                                    "select * from t where pk=%s", (random.randrange(1, 2 * pk),)
                                )


def test_physical_replication_config_mismatch_max_connections(neon_simple_env: NeonEnv):
    """
    Test for primary and replica with different configuration settings (max_connections).
    PostgreSQL enforces that settings that affect how many transactions can be open at the same time
    have values equal to or higher in a hot standby replica than in the primary. If they don't, the replica refuses
    to start up. If the settings are changed in the primary, it emits a WAL record with the new settings, and
    when the replica sees that record it pauses the replay.

    PostgreSQL enforces this to ensure that the replica can hold all the XIDs in the so-called
    "known-assigned XIDs" array, which is a fixed size array that needs to be allocated
    upfront and server startup. That's pretty pessimistic, though; usually you can get
    away with smaller settings, because we allocate space for 64 subtransactions per
    transaction too. If you get unlucky and you run out of space, WAL redo dies with
    "ERROR: too many KnownAssignedXids". It's better to take the chances than refuse
    to start up, especially in Neon: if the WAL redo dies, the server is restarted, which is
    no worse than refusing to start up in the first place. Furthermore, the control plane
    tries to ensure that on restart, the settings are set high enough, so most likely it will
    work after restart. Because of that, we have patched Postgres to disable to checks when
    the `recovery_pause_on_misconfig` setting is set to `false` (which is the default on neon).

    This test tests all those cases of running out of space in known-assigned XIDs array that
    we can hit with `recovery_pause_on_misconfig=false`, which are unreachable in unpatched
    Postgres.
    There's a similar check for `max_locks_per_transactions` too, which is related to running out
    of space in the lock manager rather than known-assigned XIDs. Similar story with that, although
    running out of space in the lock manager is possible in unmodified Postgres too. Enforcing the
    check for `max_locks_per_transactions` ensures  that you don't run out of space in the lock manager
    when there are no read-only queries holding locks in the replica, but you can still run out if you have
    those.
    """
    env = neon_simple_env
    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        with primary.connect() as p_con:
            with p_con.cursor() as p_cur:
                p_cur.execute(
                    "CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))"
                )
        with env.endpoints.new_replica_start(
            origin=primary,
            endpoint_id="secondary",
            config_lines=["max_connections=5"],
        ) as secondary:
            wait_replica_caughtup(primary, secondary)
            with secondary.connect() as s_con:
                with s_con.cursor() as s_cur:
                    cursors = []
                    for i in range(10):
                        p_con = primary.connect()
                        p_cur = p_con.cursor()
                        p_cur.execute("begin")
                        p_cur.execute("insert into t (pk) values (%s)", (i,))
                        cursors.append(p_cur)

                    for p_cur in cursors:
                        p_cur.execute("commit")

                    wait_replica_caughtup(primary, secondary)
                    s_cur.execute("select count(*) from t")
                    assert s_cur.fetchall()[0][0] == 10


def test_physical_replication_config_mismatch_max_prepared(neon_simple_env: NeonEnv):
    """
    Test for primary and replica with different configuration settings (max_prepared_transactions).
    If number of transactions at primary exceeds its limit at replica then WAL replay is terminated.
    """
    env = neon_simple_env
    primary = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
        config_lines=["max_prepared_transactions=10"],
    )
    p_con = primary.connect()
    p_cur = p_con.cursor()
    p_cur.execute("CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))")

    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
        config_lines=["max_prepared_transactions=5"],
    )
    wait_replica_caughtup(primary, secondary)

    s_con = secondary.connect()
    s_cur = s_con.cursor()
    cursors = []
    for i in range(10):
        p_con = primary.connect()
        p_cur = p_con.cursor()
        p_cur.execute("begin")
        p_cur.execute("insert into t (pk) values (%s)", (i,))
        p_cur.execute(f"prepare transaction 't{i}'")
        cursors.append(p_cur)

    for i in range(10):
        cursors[i].execute(f"commit prepared 't{i}'")

    time.sleep(5)
    with pytest.raises(Exception) as e:
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchall()[0][0] == 10
        secondary.stop()

    log.info(f"Replica crashed with {e}")
    assert secondary.log_contains("maximum number of prepared transactions reached")


def connect(ep):
    max_reconnect_attempts = 10
    for _ in range(max_reconnect_attempts):
        try:
            return ep.connect()
        except Exception as e:
            log.info(f"Failed to connect with primary: {e}")
            time.sleep(1)


def test_physical_replication_config_mismatch_too_many_known_xids(neon_simple_env: NeonEnv):
    """
    Test for primary and replica with different configuration settings (max_connections).
    In this case large difference in this setting and larger number of concurrent transactions at primary
    # cause too many known xids error  at replica.
    """
    env = neon_simple_env
    primary = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
        config_lines=[
            "max_connections=1000",
            "shared_buffers=128MB",  # prevent "no unpinned buffers available" error
        ],
    )
    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
        config_lines=[
            "max_connections=5",
            "autovacuum_max_workers=1",
            "max_worker_processes=5",
            "max_wal_senders=1",
            "superuser_reserved_connections=0",
        ],
    )

    p_con = primary.connect()
    p_cur = p_con.cursor()
    p_cur.execute("CREATE TABLE t(x integer)")

    n_connections = 990
    cursors = []
    for i in range(n_connections):
        p_con = connect(primary)
        p_cur = p_con.cursor()
        p_cur.execute("begin")
        p_cur.execute(f"insert into t values({i})")
        cursors.append(p_cur)

    for cur in cursors:
        cur.execute("commit")

    time.sleep(5)
    with pytest.raises(Exception) as e:
        s_con = secondary.connect()
        s_cur = s_con.cursor()
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchall()[0][0] == n_connections
        secondary.stop()

    log.info(f"Replica crashed with {e}")
    assert secondary.log_contains("too many KnownAssignedXids")


def test_physical_replication_config_mismatch_max_locks_per_transaction(neon_simple_env: NeonEnv):
    """
    Test for primary and replica with different configuration settings (max_locks_per_transaction).
    In  conjunction with different number of max_connections at primary and standby it can cause "out of shared memory"
    error if the primary obtains more AccessExclusiveLocks than the standby can hold.
    """
    env = neon_simple_env
    primary = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
        config_lines=[
            "max_locks_per_transaction = 100",
        ],
    )
    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
        config_lines=[
            "max_connections=10",
            "max_locks_per_transaction = 10",
        ],
    )

    n_tables = 1000

    p_con = primary.connect()
    p_cur = p_con.cursor()
    p_cur.execute("begin")
    for i in range(n_tables):
        p_cur.execute(f"CREATE TABLE t_{i}(x integer)")
    p_cur.execute("commit")

    with pytest.raises(Exception) as e:
        wait_replica_caughtup(primary, secondary)
        secondary.stop()

    log.info(f"Replica crashed with {e}")
    assert secondary.log_contains("You might need to increase")
