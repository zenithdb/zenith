from __future__ import annotations

import random
from contextlib import closing
from typing import Optional

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.remote_storage import s3_storage
from fixtures.utils import skip_in_debug_build, wait_until


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_pageserver_restart(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    # We inject a delay of 15 seconds for tenant activation below.
    # Hence, bump the max delay here to not skip over the activation.
    neon_env_builder.pageserver_config_override = 'background_task_maximum_delay="20s"'

    env = neon_env_builder.init_start()

    endpoint = env.endpoints.create_start("main")
    pageserver_http = env.pageserver.http_client()

    assert (
        pageserver_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"})
        == 1
    )

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    cur.execute("CREATE TABLE foo (t text)")
    cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    """
    )

    # Verify that the table is larger than shared_buffers
    cur.execute(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
    """
    )
    row = cur.fetchone()
    assert row is not None
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    # Stop the pageserver gracefully and restart it.
    env.pageserver.stop()
    env.pageserver.start()

    # We reloaded our tenant
    assert (
        pageserver_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"})
        == 1
    )

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)

    # Restart the server again, but delay the loading of tenants, and test what the
    # pageserver does if a compute node connects and sends a request for the tenant
    # while it's still in Loading state. (It waits for the loading to finish, and then
    # processes the request.)
    tenant_load_delay_ms = 15000
    env.pageserver.stop()
    env.pageserver.start(
        extra_env_vars={"FAILPOINTS": f"before-attaching-tenant=return({tenant_load_delay_ms})"}
    )

    # Check that it's in Attaching state
    client = env.pageserver.http_client()
    tenant_status = client.tenant_status(env.initial_tenant)
    log.info("Tenant status : %s", tenant_status)
    assert tenant_status["state"]["slug"] == "Attaching"

    # Try to read. This waits until the loading finishes, and then return normally.
    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)

    # Wait for metrics to indicate startup complete, so that we can know all
    # startup phases will be reflected in the subsequent checks
    def assert_complete():
        for sample in pageserver_http.get_metrics().query_all(
            "pageserver_startup_duration_seconds"
        ):
            labels = dict(sample.labels)
            log.info(f"metric {labels['phase']}={sample.value}")
            if labels["phase"] == "complete" and sample.value > 0:
                return

        raise AssertionError("No 'complete' metric yet")

    wait_until(30, 1.0, assert_complete)

    # Expectation callbacks: arg t is sample value, arg p is the previous phase's sample value
    expectations = [
        (
            "initial",
            lambda t, p: True,
        ),  # make no assumptions about the initial time point, it could be 0 in theory
        # Remote phase of initial_tenant_load should happen before overall phase is complete
        ("initial_tenant_load_remote", lambda t, p: t >= 0.0 and t >= p),
        # Initial tenant load should reflect the delay we injected
        ("initial_tenant_load", lambda t, p: t >= (tenant_load_delay_ms / 1000.0) and t >= p),
        # Subsequent steps should occur in expected order
        ("background_jobs_can_start", lambda t, p: t > 0 and t >= p),
        ("complete", lambda t, p: t > 0 and t >= p),
    ]

    # Accumulate the runtime of each startup phase
    values = {}
    metrics = pageserver_http.get_metrics()
    prev_value = None
    for sample in metrics.query_all("pageserver_startup_duration_seconds"):
        phase = sample.labels["phase"]
        log.info(f"metric {phase}={sample.value}")
        assert phase in [e[0] for e in expectations], f"Unexpected phase {phase}"
        values[phase] = sample

    # Apply expectations to the metrics retrieved
    for phase, expectation in expectations:
        assert phase in values, f"No data for phase {phase}"
        sample = values[phase]
        assert expectation(
            sample.value, prev_value
        ), f"Unexpected value for {phase}: {sample.value}"
        prev_value = sample.value

    # Startup is complete, this metric should exist but be zero
    assert metrics.query_one("pageserver_startup_is_loading").value == 0

    # This histogram should have been populated, although we aren't specific about exactly
    # which bucket values: just nonzero
    assert any(
        bucket.value > 0
        for bucket in metrics.query_all("pageserver_tenant_activation_seconds_bucket")
    )


# Test that repeatedly kills and restarts the page server, while the
# safekeeper and compute node keep running.
@pytest.mark.timeout(540)
@pytest.mark.parametrize("shard_count", [None, 4])
@skip_in_debug_build("times out in debug builds")
def test_pageserver_chaos(neon_env_builder: NeonEnvBuilder, shard_count: Optional[int]):
    # same rationale as with the immediate stop; we might leave orphan layers behind.
    neon_env_builder.disable_scrub_on_exit()
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count

    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    # Use a tiny checkpoint distance, to create a lot of layers quickly.
    # That allows us to stress the compaction and layer flushing logic more.
    tenant, _ = env.create_tenant(
        conf={
            "checkpoint_distance": "5000000",
        }
    )
    endpoint = env.endpoints.create_start("main", tenant_id=tenant)

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE foo (id int, t text, updates int)")
            cur.execute("CREATE INDEX ON foo (id)")
            cur.execute(
                """
            INSERT INTO foo
            SELECT g, 'long string to consume some space' || g, 0
            FROM generate_series(1, 100000) g
            """
            )

            # Verify that the table is larger than shared_buffers
            cur.execute(
                """
            select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
            from pg_settings where name = 'shared_buffers'
            """
            )
            row = cur.fetchone()
            assert row is not None
            log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
            assert int(row[0]) < int(row[1])

    # We run "random" kills using a fixed seed, to improve reproducibility if a test
    # failure is related to a particular order of operations.
    seed = 0xDEADBEEF
    rng = random.Random(seed)

    # Update the whole table, then immediately kill and restart the pageserver
    for i in range(1, 15):
        endpoint.safe_psql("UPDATE foo set updates = updates + 1")

        # This kills the pageserver immediately, to simulate a crash
        to_kill = rng.choice(env.pageservers)
        to_kill.stop(immediate=True)
        to_kill.start()

        # Check that all the updates are visible
        num_updates = endpoint.safe_psql("SELECT sum(updates) FROM foo")[0][0]
        assert num_updates == i * 100000

    # currently pageserver cannot tolerate the fact that "s3" goes away, and if
    # we succeeded in a compaction before shutdown, there might be a lot of
    # uploads pending, certainly more than what we can ingest with MOCK_S3
    #
    # so instead, do a fast shutdown for this one test.
    # See https://github.com/neondatabase/neon/issues/8709
    env.stop(immediate=True)
