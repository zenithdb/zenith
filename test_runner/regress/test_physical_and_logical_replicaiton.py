from __future__ import annotations

import time

from fixtures.neon_fixtures import NeonEnv, logical_replication_sync, wait_replica_caughtup


def test_physical_and_logical_replication_slot_not_copied(neon_simple_env: NeonEnv, vanilla_pg):
    """Test read replica of a primary which has a logical replication publication"""
    env = neon_simple_env

    n_records = 100000

    primary = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    )
    p_con = primary.connect()
    p_cur = p_con.cursor()
    p_cur.execute("CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))")
    p_cur.execute("create publication pub1 for table t")

    # start subscriber to primary
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE t(pk bigint primary key, payload text)")
    connstr = primary.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    time.sleep(1)
    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
    )

    s_con = secondary.connect()
    s_cur = s_con.cursor()

    for pk in range(n_records):
        p_cur.execute("insert into t (pk) values (%s)", (pk,))

    wait_replica_caughtup(primary, secondary)

    s_cur.execute("select count(*) from t")
    assert s_cur.fetchall()[0][0] == n_records

    logical_replication_sync(vanilla_pg, primary)
    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == n_records

    # Check that LR slot is not copied to replica
    s_cur.execute("select count(*) from pg_replication_slots")
    assert s_cur.fetchall()[0][0] == 0


def test_aux_not_logged_at_replica(neon_simple_env: NeonEnv, vanilla_pg):
    """Test that AUX files are not saved at replica"""
    env = neon_simple_env

    n_records = 20000

    primary = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    )
    p_con = primary.connect()
    p_cur = p_con.cursor()
    p_cur.execute("CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))")
    p_cur.execute("create publication pub1 for table t")

    # start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE t(pk bigint primary key, payload text)")
    connstr = primary.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    for pk in range(n_records):
        p_cur.execute("insert into t (pk) values (%s)", (pk,))

    # LR snapshot is stored each 15 seconds
    time.sleep(16)

    # start replica
    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
    )

    s_con = secondary.connect()
    s_cur = s_con.cursor()

    logical_replication_sync(vanilla_pg, primary)

    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == n_records
    s_cur.execute("select count(*) from t")
    assert s_cur.fetchall()[0][0] == n_records

    vanilla_pg.stop()
    secondary.stop()
    primary.stop()
    assert not secondary.log_contains("cannot make new WAL entries during recovery")
