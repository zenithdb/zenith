from __future__ import annotations

import time

from fixtures.neon_fixtures import NeonEnvBuilder, flush_ep_to_pageserver


def test_layer_map(neon_env_builder: NeonEnvBuilder, zenbenchmark):
    """Benchmark searching the layer map, when there are a lot of small layer files."""

    env = neon_env_builder.init_configs()
    n_iters = 10
    n_records = 100000

    env.start()

    # We want to have a lot of lot of layer files to exercise the layer map. Disable
    # GC, and make checkpoint_distance very small, so that we get a lot of small layer
    # files.
    tenant, timeline = env.create_tenant(
        conf={
            "gc_period": "0s",
            "checkpoint_distance": "16384",
            "compaction_period": "1 s",
            "compaction_threshold": "1",
            "l0_flush_delay_threshold": "0",
            "l0_flush_stall_threshold": "0",
            "compaction_target_size": "16384",
        }
    )

    endpoint = env.endpoints.create_start("main", tenant_id=tenant)
    cur = endpoint.connect().cursor()
    cur.execute("set log_statement = 'all'")
    cur.execute("create table t(x integer)")
    for _ in range(n_iters):
        cur.execute(f"insert into t values (generate_series(1,{n_records}))")
        time.sleep(1)

    cur.execute("vacuum t")

    with zenbenchmark.record_duration("test_query"):
        cur.execute("SELECT count(*) from t")
        assert cur.fetchone() == (n_iters * n_records,)

    flush_ep_to_pageserver(env, endpoint, tenant, timeline)
    env.pageserver.http_client().timeline_checkpoint(
        tenant, timeline, compact=False, wait_until_uploaded=True
    )
