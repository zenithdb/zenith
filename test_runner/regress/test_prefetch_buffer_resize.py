from __future__ import annotations

import random

import pytest
from fixtures.neon_fixtures import NeonEnvBuilder


@pytest.mark.parametrize("shard_count", [None, 4])
def test_prefetch(neon_env_builder: NeonEnvBuilder, shard_count: int | None):
    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count,
    )
    n_iter = 10
    n_rec = 100000

    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "shared_buffers=10MB",
        ],
    )

    cur = endpoint.connect().cursor()

    cur.execute("CREATE TABLE t(pk integer, filler text default repeat('?', 200))")
    cur.execute(f"insert into t (pk) values (generate_series(1,{n_rec}))")

    cur.execute("set statement_timeout=0")
    cur.execute("set effective_io_concurrency=20")
    cur.execute("set max_parallel_workers_per_gather=0")

    for _ in range(n_iter):
        buf_size = random.randrange(16, 32)
        cur.execute(f"set neon.readahead_buffer_size={buf_size}")
        limit = random.randrange(1, n_rec)
        cur.execute(f"select sum(pk) from (select pk from t limit {limit}) s")
