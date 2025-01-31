import pytest
from fixtures.neon_fixtures import NeonEnv
from fixtures.pg_version import PgVersion


#
# Test that pgstat statistic is preserved across sessions
#
def test_pgstat(neon_simple_env: NeonEnv):
    env = neon_simple_env
    if env.pg_version == PgVersion.V14:
        pytest.skip("PG14 doesn't support pgstat statistic persistence")

    n = 10000
    endpoint = env.endpoints.create_start(
        "main", config_lines=["neon_pgstat_file_size_limit=100kB"]
    )

    con = endpoint.connect()
    cur = con.cursor()

    cur.execute("create table t(x integer)")
    cur.execute(f"insert into t values (generate_series(1,{n}))")
    cur.execute("vacuum analyze t")
    cur.execute("select sum(x) from t")
    cur.execute("update t set x=x+1")

    cur.execute("select pg_stat_force_next_flush()")

    cur.execute(
        "select seq_scan,seq_tup_read,n_tup_ins,n_tup_upd,n_live_tup,n_dead_tup, vacuum_count,analyze_count from pg_stat_user_tables"
    )
    rec = cur.fetchall()[0]
    assert rec == (2, n * 2, n, n, n * 2, n, 1, 1)

    endpoint.stop()
    endpoint.start()

    con = endpoint.connect()
    cur = con.cursor()

    cur.execute(
        "select seq_scan,seq_tup_read,n_tup_ins,n_tup_upd,n_live_tup,n_dead_tup, vacuum_count,analyze_count from pg_stat_user_tables"
    )
    rec = cur.fetchall()[0]
    assert rec == (2, n * 2, n, n, n * 2, n, 1, 1)

    cur.execute("update t set x=x+1")

    # stop without checkpoint
    endpoint.stop(mode="immediate")
    endpoint.start()

    con = endpoint.connect()
    cur = con.cursor()

    cur.execute(
        "select seq_scan,seq_tup_read,n_tup_ins,n_tup_upd,n_live_tup,n_dead_tup, vacuum_count,analyze_count from pg_stat_user_tables"
    )
    rec = cur.fetchall()[0]
    # pgstat information should be discarded in case of abnormal termination
    assert rec == (0, 0, 0, 0, 0, 0, 0, 0)

    cur.execute("select sum(x) from t")

    # create more relations to increase size of statistics
    for i in range(1, 1000):
        cur.execute(f"create table t{i}(pk integer primary key)")

    cur.execute("select pg_stat_force_next_flush()")

    endpoint.stop()
    endpoint.start()

    con = endpoint.connect()
    cur = con.cursor()

    cur.execute(
        "select seq_scan,seq_tup_read,n_tup_ins,n_tup_upd,n_live_tup,n_dead_tup, vacuum_count,analyze_count from pg_stat_user_tables"
    )
    rec = cur.fetchall()[0]
    # pgstat information is not restored because it's size exeeds 100k threshold
    assert rec == (0, 0, 0, 0, 0, 0, 0, 0)
