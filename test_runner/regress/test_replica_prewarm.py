from __future__ import annotations

import pytest
from fixtures.neon_fixtures import NeonEnv, wait_replica_caughtup
from fixtures.pg_version import PgVersion


def test_replica_prewarm(neon_simple_env: NeonEnv):
    env = neon_simple_env
    n_records = 1000000
    if env.pg_version < PgVersion.V16:
        pytest.skip("NEON_RM is available only in PG16")
    primary = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
        config_lines=[
            "autovacuum = off",
            "shared_buffers=1MB",
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
            "neon.file_cache_prewarm_rate=1s",
        ],
    )
    p_con = primary.connect()
    p_cur = p_con.cursor()

    p_cur.execute("create extension neon")
    p_cur.execute("create table t(pk integer primary key, payload text default repeat('?', 128))")

    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
        config_lines=[
            "autovacuum = off",
            "shared_buffers=1MB",
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
            "neon.file_cache_prewarm_limit=1000",
        ],
    )
    p_cur.execute(f"insert into t (pk) values (generate_series(1,{n_records}))")

    wait_replica_caughtup(primary, secondary)

    s_con = secondary.connect()
    s_cur = s_con.cursor()

    s_cur.execute("select lfc_value from neon_lfc_stats where lfc_key='file_cache_used_pages'")
    lfc_used_pages = s_cur.fetchall()[0][0]
    assert lfc_used_pages > 10000

    s_cur.execute("select sum(pk) from t")
    assert s_cur.fetchall()[0][0] == n_records * (n_records + 1) / 2
