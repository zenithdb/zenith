import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import USE_LFC


def test_lfc_prewarm(neon_simple_env: NeonEnv):
    if not USE_LFC:
        return

    env = neon_simple_env
    n_records = 1000000

    endpoint = env.endpoints.create_start(
        branch_name="main",
        config_lines=[
            "autovacuum = off",
            "shared_buffers=1MB",
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
            "neon.file_cache_prewarm_limit=1000",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("create extension neon version '1.6'")
    cur.execute("create table t(pk integer primary key, payload text default repeat('?', 128))")
    cur.execute(f"insert into t (pk) values (generate_series(1,{n_records}))")
    cur.execute("select get_local_cache_state()")
    lfc_state = cur.fetchall()[0][0]

    endpoint.stop()
    endpoint.start()

    conn = endpoint.connect()
    cur = conn.cursor()
    time.sleep(1)  # wait until compute_ctl complete downgrade of extension to default version
    cur.execute("alter extension neon update to '1.6'")
    cur.execute("select prewarm_local_cache(%s)", (lfc_state,))

    cur.execute("select lfc_value from neon_lfc_stats where lfc_key='file_cache_used_pages'")
    lfc_used_pages = cur.fetchall()[0][0]
    log.info(f"Used LFC size: {lfc_used_pages}")
    cur.execute("select * from get_prewarm_info()")
    prewarm_info = cur.fetchall()[0]
    log.info(f"Prewarm info: {prewarm_info}")
    log.info(f"Prewarm progress: {prewarm_info[1]*100//prewarm_info[0]}%")

    assert lfc_used_pages > 10000
    assert prewarm_info[0] > 0 and prewarm_info[0] == prewarm_info[1]

    cur.execute("select sum(pk) from t")
    assert cur.fetchall()[0][0] == n_records * (n_records + 1) / 2

    assert prewarm_info[1] > 0
