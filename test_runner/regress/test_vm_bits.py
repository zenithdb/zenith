from __future__ import annotations

import time
from contextlib import closing

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, PgBin, fork_at_current_lsn
from fixtures.utils import query_scalar


#
# Test that the VM bit is cleared correctly at a HEAP_DELETE and
# HEAP_UPDATE record.
#
def test_vm_bit_clear(neon_simple_env: NeonEnv):
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Install extension containing function needed for test
    cur.execute("CREATE EXTENSION neon_test_utils")

    # Create a test table for a few different scenarios and freeze it to set the VM bits.
    cur.execute("CREATE TABLE vmtest_delete (id integer PRIMARY KEY)")
    cur.execute("INSERT INTO vmtest_delete VALUES (1)")
    cur.execute("VACUUM FREEZE vmtest_delete")

    cur.execute("CREATE TABLE vmtest_hot_update (id integer PRIMARY KEY, filler text)")
    cur.execute("INSERT INTO vmtest_hot_update VALUES (1, 'x')")
    cur.execute("VACUUM FREEZE vmtest_hot_update")

    cur.execute("CREATE TABLE vmtest_cold_update (id integer PRIMARY KEY)")
    cur.execute("INSERT INTO vmtest_cold_update SELECT g FROM generate_series(1, 1000) g")
    cur.execute("VACUUM FREEZE vmtest_cold_update")

    cur.execute(
        "CREATE TABLE vmtest_cold_update2 (id integer PRIMARY KEY, filler text) WITH (fillfactor=100)"
    )
    cur.execute("INSERT INTO vmtest_cold_update2 SELECT g, '' FROM generate_series(1, 1000) g")
    cur.execute("VACUUM FREEZE vmtest_cold_update2")

    # DELETE and UPDATE the rows.
    cur.execute("DELETE FROM vmtest_delete WHERE id = 1")
    cur.execute("UPDATE vmtest_hot_update SET filler='x' WHERE id = 1")
    cur.execute("UPDATE vmtest_cold_update SET id = 5000 WHERE id = 1")

    # Clear the VM bit on the last page with an INSERT. Then clear the VM bit on
    # the page where row 1 is (block 0), by doing an UPDATE. The UPDATE is a
    # cold update, and the new tuple goes to the last page, which already had
    # its VM bit cleared. The point is that the UPDATE *only* clears the VM bit
    # on the page containing the old tuple. We had a bug where we got the old
    # and new pages mixed up, and that only shows up when one of the bits is
    # cleared, but not the other one.
    cur.execute("INSERT INTO vmtest_cold_update2 VALUES (9999, 'x')")
    # Clears the VM bit on the old page
    cur.execute("UPDATE vmtest_cold_update2 SET id = 5000, filler=repeat('x', 200) WHERE id = 1")

    # Branch at this point, to test that later
    fork_at_current_lsn(env, endpoint, "test_vm_bit_clear_new", "main")

    # Clear the buffer cache, to force the VM page to be re-fetched from
    # the page server
    endpoint.clear_buffers(cursor=cur)

    # Check that an index-only scan doesn't see the deleted row. If the
    # clearing of the VM bit was not replayed correctly, this would incorrectly
    # return deleted row.
    cur.execute(
        """
    set enable_seqscan=off;
    set enable_indexscan=on;
    set enable_bitmapscan=off;
    """
    )

    cur.execute("SELECT id FROM vmtest_delete WHERE id = 1")
    assert cur.fetchall() == []
    cur.execute("SELECT id FROM vmtest_hot_update WHERE id = 1")
    assert cur.fetchall() == [(1,)]
    cur.execute("SELECT id FROM vmtest_cold_update WHERE id = 1")
    assert cur.fetchall() == []
    cur.execute("SELECT id FROM vmtest_cold_update2 WHERE id = 1")
    assert cur.fetchall() == []

    cur.close()

    # Check the same thing on the branch that we created right after the DELETE
    #
    # As of this writing, the code in smgrwrite() creates a full-page image whenever
    # a dirty VM page is evicted. If the VM bit was not correctly cleared by the
    # earlier WAL record, the full-page image hides the problem. Starting a new
    # server at the right point-in-time avoids that full-page image.
    endpoint_new = env.endpoints.create_start("test_vm_bit_clear_new")

    pg_new_conn = endpoint_new.connect()
    cur_new = pg_new_conn.cursor()

    cur_new.execute(
        """
    set enable_seqscan=off;
    set enable_indexscan=on;
    set enable_bitmapscan=off;
    """
    )

    cur_new.execute("SELECT id FROM vmtest_delete WHERE id = 1")
    assert cur_new.fetchall() == []
    cur_new.execute("SELECT id FROM vmtest_hot_update WHERE id = 1")
    assert cur_new.fetchall() == [(1,)]
    cur_new.execute("SELECT id FROM vmtest_cold_update WHERE id = 1")
    assert cur_new.fetchall() == []
    cur_new.execute("SELECT id FROM vmtest_cold_update2 WHERE id = 1")
    assert cur_new.fetchall() == []


def test_vm_bit_clear_on_heap_lock_whitebox(neon_env_builder: NeonEnvBuilder):
    """
    Test that the ALL_FROZEN VM bit is cleared correctly at a HEAP_LOCK record.

    This is a repro for the bug fixed in commit 66fa176cc8.
    """
    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            # If auto-analyze runs at the same time that we run VACUUM FREEZE, it
            # can hold a snasphot that prevent the tuples from being frozen.
            "autovacuum=off",
            "log_checkpoints=on",
        ],
    )

    # Run the tests in a dedicated database, because the activity monitor
    # periodically runs some queries on to the 'postgres' database. If that
    # happens at the same time that we're trying to freeze, the activity
    # monitor's queries can hold back the xmin horizon and prevent freezing.
    with closing(endpoint.connect()) as pg_conn:
        pg_conn.cursor().execute("CREATE DATABASE vmbitsdb")
    pg_conn = endpoint.connect(dbname="vmbitsdb")
    cur = pg_conn.cursor()

    # Install extension containing function needed for test
    cur.execute("CREATE EXTENSION neon_test_utils")
    cur.execute("CREATE EXTENSION pageinspect")

    # Create a test table and freeze it to set the all-frozen VM bit on all pages.
    cur.execute("CREATE TABLE vmtest_lock (id integer PRIMARY KEY)")
    cur.execute("BEGIN")
    cur.execute("INSERT INTO vmtest_lock SELECT g FROM generate_series(1, 50000) g")
    xid = int(query_scalar(cur, "SELECT txid_current()"))
    cur.execute("COMMIT")
    cur.execute("VACUUM (FREEZE, DISABLE_PAGE_SKIPPING true, VERBOSE) vmtest_lock")
    for notice in pg_conn.notices:
        log.info(f"{notice}")

    # This test has been flaky in the past, because background activity like
    # auto-analyze and compute_ctl's activity monitor queries have prevented the
    # tuples from being frozen. Check that they were frozen.
    relfrozenxid = int(
        query_scalar(cur, "SELECT relfrozenxid FROM pg_class WHERE relname='vmtest_lock'")
    )
    assert (
        relfrozenxid > xid
    ), f"Inserted rows were not frozen. This can be caused by concurrent activity in the database. (XID {xid}, relfrozenxid {relfrozenxid}"

    # Lock a row. This clears the all-frozen VM bit for that page.
    cur.execute("BEGIN")
    cur.execute("SELECT * FROM vmtest_lock WHERE id = 40000 FOR UPDATE")
    cur.execute("COMMIT")

    # The VM page in shared buffer cache, and the same page as reconstructed by
    # the pageserver, should be equal. Except for the LSN: Clearing a bit in the
    # VM doesn't bump the LSN in PostgreSQL, but the pageserver updates the LSN
    # when it replays the VM-bit clearing record (since commit 387a36874c)
    #
    # This is a bit fragile, we've had lot of flakiness in this test before. For
    # example, because all the VM bits were not set because concurrent
    # autoanalyze prevented the VACUUM FREEZE from freezing the tuples. Or
    # because autoavacuum kicked in and re-froze the page between the
    # get_raw_page() and get_raw_page_at_lsn() calls. We disable autovacuum now,
    # which should make this deterministic.
    cur.execute("select get_raw_page( 'vmtest_lock', 'vm', 0 )")
    vm_page_in_cache = (cur.fetchall()[0][0])[8:100].hex()
    cur.execute(
        "select get_raw_page_at_lsn( 'vmtest_lock', 'vm', 0, pg_current_wal_insert_lsn(), NULL )"
    )
    vm_page_at_pageserver = (cur.fetchall()[0][0])[8:100].hex()

    assert vm_page_at_pageserver == vm_page_in_cache


def test_vm_bit_clear_on_heap_lock_blackbox(neon_env_builder: NeonEnvBuilder):
    """
    The previous test is enough to verify the bug that was fixed in
    commit 66fa176cc8. But for good measure, we also reproduce the
    original problem that the missing VM page update caused.
    """
    tenant_conf = {
        "checkpoint_distance": f"{128 * 1024}",
        "compaction_target_size": f"{128 * 1024}",
        "compaction_threshold": "1",
        # create image layers eagerly, so that GC can remove some layers
        "image_creation_threshold": "1",
        # set PITR interval to be small, so we can do GC
        "pitr_interval": "0 s",
    }
    env = neon_env_builder.init_start(initial_tenant_conf=tenant_conf)

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "log_autovacuum_min_duration = 0",
            # Perform anti-wraparound vacuuming aggressively
            "autovacuum_naptime='1 s'",
            "autovacuum_freeze_max_age = 1000000",
        ],
    )

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Install extension containing function needed for test
    cur.execute("CREATE EXTENSION neon_test_utils")

    # Create a test table and freeze it to set the all-frozen VM bit on all pages.
    cur.execute("CREATE TABLE vmtest_lock (id integer PRIMARY KEY)")
    cur.execute("INSERT INTO vmtest_lock SELECT g FROM generate_series(1, 50000) g")
    cur.execute("VACUUM (FREEZE, DISABLE_PAGE_SKIPPING true) vmtest_lock")

    # Lock a row. This clears the all-frozen VM bit for that page.
    cur.execute("BEGIN")
    cur.execute("SELECT * FROM vmtest_lock WHERE id = 40000 FOR UPDATE")

    # Remember the XID. We will use it later to verify that we have consumed a lot of
    # XIDs after this.
    cur.execute("select pg_current_xact_id()")
    locking_xid = int(cur.fetchall()[0][0])

    cur.execute("COMMIT")

    # Kill and restart postgres, to clear the buffer cache.
    #
    # NOTE: clear_buffer_cache() will not do, because it evicts the dirty pages
    # in a "clean" way. Our neon extension will write a full-page image of the VM
    # page, and we want to avoid that. A clean shutdown will also not do, for the
    # same reason.
    endpoint.stop(mode="immediate", sks_wait_walreceiver_gone=(env.safekeepers, timeline_id))

    endpoint.start()
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Consume a lot of XIDs, so that anti-wraparound autovacuum kicks
    # in and the clog gets truncated. We set autovacuum_freeze_max_age to a very
    # low value, so it doesn't take all that many XIDs for autovacuum to kick in.
    #
    # We could use test_consume_xids() to consume XIDs much faster,
    # but it wouldn't speed up the overall test, because we'd still
    # need to wait for autovacuum to run.
    for _ in range(1000):
        cur.execute("select test_consume_xids(10000);")
    for _ in range(1000):
        cur.execute("select min(datfrozenxid::text::int) from pg_database")
        datfrozenxid = int(cur.fetchall()[0][0])
        log.info(f"datfrozenxid {datfrozenxid} locking_xid: {locking_xid}")
        if datfrozenxid > locking_xid + 3000000:
            break
        time.sleep(0.5)

    cur.execute("select pg_current_xact_id()")
    curr_xid = int(cur.fetchall()[0][0])
    assert curr_xid - locking_xid >= 100000

    # Perform GC in the pageserver. Otherwise the compute might still
    # be able to download the already-deleted SLRU segment from the
    # pageserver. That masks the original bug.
    env.pageserver.http_client().timeline_checkpoint(tenant_id, timeline_id)
    env.pageserver.http_client().timeline_compact(tenant_id, timeline_id)
    env.pageserver.http_client().timeline_gc(tenant_id, timeline_id, 0)

    # Now, if the VM all-frozen bit was not correctly cleared on
    # replay, we will try to fetch the status of the XID that was
    # already truncated away.
    #
    # ERROR: could not access status of transaction 1027
    cur.execute("select xmin, xmax, * from vmtest_lock where id = 40000 for update")
    tup = cur.fetchall()
    log.info(f"tuple = {tup}")
    cur.execute("commit transaction")


@pytest.mark.timeout(600)  # slow in debug builds
def test_check_visibility_map(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Runs pgbench across a few databases on a sharded tenant, then performs a visibility map
    consistency check. Regression test for https://github.com/neondatabase/neon/issues/9914.
    """

    # Use a large number of shards with small stripe sizes, to ensure the visibility
    # map will end up on non-zero shards.
    SHARD_COUNT = 8
    STRIPE_SIZE = 32  # in 8KB pages
    PGBENCH_RUNS = 4

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=SHARD_COUNT, initial_tenant_shard_stripe_size=STRIPE_SIZE
    )
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "shared_buffers = 64MB",
        ],
    )

    # Run pgbench in 4 different databases, to exercise different shards.
    dbnames = [f"pgbench{i}" for i in range(PGBENCH_RUNS)]
    for i, dbname in enumerate(dbnames):
        log.info(f"pgbench run {i+1}/{PGBENCH_RUNS}")
        endpoint.safe_psql(f"create database {dbname}")
        connstr = endpoint.connstr(dbname=dbname)
        # pgbench -i will automatically vacuum the tables. This creates the visibility map.
        pg_bin.run(["pgbench", "-i", "-s", "10", connstr])
        # Freeze the tuples to set the initial frozen bit.
        endpoint.safe_psql("vacuum freeze", dbname=dbname)
        # Run pgbench.
        pg_bin.run(["pgbench", "-c", "32", "-j", "8", "-T", "10", connstr])

    # Restart the endpoint to flush the compute page cache. We want to make sure we read VM pages
    # from storage, not cache.
    endpoint.stop()
    endpoint.start()

    # Check that the visibility map matches the heap contents for pg_accounts (the main table).
    for dbname in dbnames:
        log.info(f"Checking visibility map for {dbname}")
        with endpoint.cursor(dbname=dbname) as cur:
            cur.execute("create extension pg_visibility")

            cur.execute("select count(*) from pg_check_visible('pgbench_accounts')")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0, f"{row[0]} inconsistent VM pages (visible)"

            cur.execute("select count(*) from pg_check_frozen('pgbench_accounts')")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0, f"{row[0]} inconsistent VM pages (frozen)"

    # Vacuum and freeze the tables, and check that the visibility map is still accurate.
    for dbname in dbnames:
        log.info(f"Vacuuming and checking visibility map for {dbname}")
        with endpoint.cursor(dbname=dbname) as cur:
            cur.execute("vacuum freeze")

            cur.execute("select count(*) from pg_check_visible('pgbench_accounts')")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0, f"{row[0]} inconsistent VM pages (visible)"

            cur.execute("select count(*) from pg_check_frozen('pgbench_accounts')")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 0, f"{row[0]} inconsistent VM pages (frozen)"
