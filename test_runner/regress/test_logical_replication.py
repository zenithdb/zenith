import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    logical_replication_sync,
    wait_for_last_flush_lsn,
)


def test_logical_replication(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_logical_replication", "empty")
    endpoint = env.endpoints.create_start("test_logical_replication")

    log.info("postgres is running on 'test_logical_replication' branch")
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("create table t(pk integer primary key, payload integer)")
    cur.execute("create publication pub1 for table t")

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key, payload integer)")
    connstr = endpoint.connstr().replace("'", "''")
    print(f"connstr='{connstr}'")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Wait logical replication channel to be established
    logical_replication_sync(vanilla_pg, endpoint)

    # insert some data
    cur.execute("insert into t values (generate_series(1,1000), 0)")

    # Wait logical replication to sync
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == 1000

    # now stop subscriber...
    vanilla_pg.stop()

    # ... and insert some more data which should be delivered to subscriber after restart
    cur.execute("insert into t values (generate_series(1001,2000), 0)")

    # Restart compute
    endpoint.stop()
    endpoint.start()

    # start subscriber
    vanilla_pg.start()

    # Wait logical replication to sync
    logical_replication_sync(vanilla_pg, endpoint)

    # Check that subscribers receives all data
    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == 2000

    sp = endpoint.safe_psql("table pg_replication_slots;")
    log.info(f"slots are {sp}, connstr {endpoint.connstr()}")
    # test that removal of repl slots works across restart
    vanilla_pg.stop()
    time.sleep(1)  # wait for conn termination; active slots can't be dropped
    endpoint.safe_psql("select pg_drop_replication_slot('sub1');")
    endpoint.safe_psql("insert into t values (2001, 1);")  # forces WAL flush
    # wait for drop message to reach safekeepers (it is not transactional)
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    endpoint.stop()
    endpoint.start()
    # time.sleep(43432423)
    # it must be gone (but walproposer slot still exists, hence 1)
    assert endpoint.safe_psql("select count(*) from pg_replication_slots")[0][0] == 1
