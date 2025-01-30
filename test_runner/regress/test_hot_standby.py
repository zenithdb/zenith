from __future__ import annotations

import asyncio
import os
import threading
import time
from functools import partial

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    log_replica_lag,
    tenant_get_shards,
    wait_replica_caughtup,
)
from fixtures.utils import wait_until


def test_hot_standby(neon_simple_env: NeonEnv):
    env = neon_simple_env

    # We've had a bug caused by WAL records split across multiple XLogData
    # messages resulting in corrupted WAL complains on standby. It reproduced
    # only when sending from safekeeper is slow enough to grab full
    # MAX_SEND_SIZE messages. So insert sleep through failpoints, but only in
    # one conf to decrease test time.
    slow_down_send = "[debug-pg16]" in os.environ.get("PYTEST_CURRENT_TEST", "")
    if slow_down_send:
        sk_http = env.safekeepers[0].http_client()
        sk_http.configure_failpoints([("sk-send-wal-replica-sleep", "return(100)")])

    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        time.sleep(1)
        with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
            queries = [
                "SHOW neon.timeline_id",
                "SHOW neon.tenant_id",
                "SELECT relname FROM pg_class WHERE relnamespace = current_schema()::regnamespace::oid",
                "SELECT COUNT(*), SUM(i) FROM test",
            ]
            responses = dict()

            with primary.connect() as p_con:
                with p_con.cursor() as p_cur:
                    p_cur.execute("CREATE TABLE test AS SELECT generate_series(1, 100) AS i")

                for query in queries:
                    with p_con.cursor() as p_cur:
                        p_cur.execute(query)
                        res = p_cur.fetchone()
                        assert res is not None
                        response = res
                        responses[query] = response

                # insert more data to make safekeeper send MAX_SEND_SIZE messages
                if slow_down_send:
                    primary.safe_psql("create table t(key int, value text)")
                    primary.safe_psql("insert into t select generate_series(1, 100000), 'payload'")

            wait_replica_caughtup(primary, secondary)

            with secondary.connect() as s_con:
                with s_con.cursor() as s_cur:
                    s_cur.execute("SELECT 1 WHERE pg_is_in_recovery()")
                    res = s_cur.fetchone()
                    assert res is not None

                for query in queries:
                    with s_con.cursor() as secondary_cursor:
                        secondary_cursor.execute(query)
                        response = secondary_cursor.fetchone()
                        assert response is not None
                        assert response == responses[query]

            # Check for corrupted WAL messages which might otherwise go unnoticed if
            # reconnection fixes this.
            assert not secondary.log_contains(
                "incorrect resource manager data|record with incorrect|invalid magic number|unexpected pageaddr"
            )

    # clean up
    if slow_down_send:
        sk_http.configure_failpoints(("sk-send-wal-replica-sleep", "off"))


def test_2_replicas_start(neon_simple_env: NeonEnv):
    env = neon_simple_env

    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        time.sleep(1)
        with env.endpoints.new_replica_start(
            origin=primary, endpoint_id="secondary1"
        ) as secondary1:
            with env.endpoints.new_replica_start(
                origin=primary, endpoint_id="secondary2"
            ) as secondary2:
                wait_replica_caughtup(primary, secondary1)
                wait_replica_caughtup(primary, secondary2)


# Test two different scenarios related to gc of data needed by hot standby.
#
# When pause_apply is False, standby is mostly caught up with the primary.
# However, in compute <-> pageserver protocol version 1 only one LSN had been
# sent to the pageserver in page request, and to avoid waits in the pageserver
# it was last-written LSN cache value. If page hasn't been updated for a long
# time that resulted in an error from the pageserver: "Bad request: tried to
# request a page version that was garbage collected". For primary this wasn't a
# problem because pageserver always bumped LSN to the newest one; for standy
# that would be incorrect since we might get page fresher then apply LSN. Hence,
# in protocol version v2 two LSNs were introduced: main request_lsn (apply LSN
# in case of standby) and not_modified_since which could be used as an
# optimization to avoid waiting.
#
# https://github.com/neondatabase/neon/issues/6211
#
# When pause_apply is True we model standby lagging behind primary (e.g. due to
# high max_standby_streaming_delay). To prevent pageserver from removing data
# still needed by the standby apply LSN is propagated in standby -> safekeepers
# -> broker -> pageserver flow so that pageserver could hold off gc for it.
@pytest.mark.parametrize("pause_apply", [False, True])
def test_hot_standby_gc(neon_env_builder: NeonEnvBuilder, pause_apply: bool):
    tenant_conf = {
        # set PITR interval to be small, so we can do GC
        "pitr_interval": "0 s",
    }

    # Make sure that standby_horizon feedback still works when the standby and
    # the pageserver might be connected to different safekeepers
    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start(initial_tenant_conf=tenant_conf)
    timeline_id = env.initial_timeline
    tenant_id = env.initial_tenant

    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        with env.endpoints.new_replica_start(
            origin=primary,
            endpoint_id="secondary",
            # Protocol version 2 was introduced to fix the issue
            # that this test exercises. With protocol version 1 it
            # fails.
            config_lines=["neon.protocol_version=2"],
        ) as secondary:
            p_cur = primary.connect().cursor()
            p_cur.execute("CREATE EXTENSION neon_test_utils")
            p_cur.execute("CREATE TABLE test (id int primary key) WITH (autovacuum_enabled=false)")
            p_cur.execute("INSERT INTO test SELECT generate_series(1, 10000) AS g")

            wait_replica_caughtup(primary, secondary)

            s_cur = secondary.connect().cursor()

            s_cur.execute("SELECT 1 WHERE pg_is_in_recovery()")
            res = s_cur.fetchone()
            assert res is not None

            s_cur.execute("SELECT COUNT(*) FROM test")
            res = s_cur.fetchone()
            assert res[0] == 10000

            # Clear the cache in the standby, so that when we
            # re-execute the query, it will make GetPage
            # requests. This does not clear the last-written LSN cache
            # so we still remember the LSNs of the pages.
            secondary.clear_buffers(cursor=s_cur)

            if pause_apply:
                s_cur.execute("SELECT pg_wal_replay_pause()")

            # Do other stuff on the primary, to advance the WAL
            p_cur.execute("CREATE TABLE test2 AS SELECT generate_series(1, 1000000) AS g")

            # Run GC. The PITR interval is very small, so this advances the GC cutoff LSN
            # very close to the primary's current insert LSN.
            shards = tenant_get_shards(env, tenant_id, None)
            for tenant_shard_id, pageserver in shards:
                client = pageserver.http_client()
                client.timeline_checkpoint(tenant_shard_id, timeline_id)
                client.timeline_compact(tenant_shard_id, timeline_id)
                client.timeline_gc(tenant_shard_id, timeline_id, 0)

            # Re-execute the query. The GetPage requests that this
            # generates use old not_modified_since LSNs, older than
            # the GC cutoff, but new request LSNs. (In protocol
            # version 1 there was only one LSN, and this failed.)
            secondary.clear_shared_buffers(cursor=s_cur)
            log_replica_lag(primary, secondary)
            s_cur.execute("SELECT COUNT(*) FROM test")
            log_replica_lag(primary, secondary)
            res = s_cur.fetchone()
            assert res[0] == 10000

            env.safekeepers[0].stop()

            # Restart the pageserver and run GC again: the standby_horizon should still be enforced
            env.pageserver.stop()
            env.pageserver.start()
            for tenant_shard_id, pageserver in shards:
                client = pageserver.http_client()
                client.timeline_checkpoint(tenant_shard_id, timeline_id)
                client.timeline_compact(tenant_shard_id, timeline_id)
                client.timeline_gc(tenant_shard_id, timeline_id, 0)

            # Re-execute the query. The GetPage requests that this
            # generates use old not_modified_since LSNs, older than
            # the GC cutoff, but new request LSNs. (In protocol
            # version 1 there was only one LSN, and this failed.)
            secondary.clear_shared_buffers(cursor=s_cur)
            log_replica_lag(primary, secondary)
            s_cur.execute("SELECT COUNT(*) FROM test")
            log_replica_lag(primary, secondary)
            res = s_cur.fetchone()
            assert res[0] == 10000


def run_pgbench(connstr: str, pg_bin: PgBin):
    log.info(f"Start a pgbench workload on pg {connstr}")
    pg_bin.run_capture(["pgbench", "-T60", connstr])


# assert that pgbench_accounts and its index are created.
def pgbench_accounts_initialized(ep):
    ep.safe_psql_scalar("select 'pgbench_accounts_pkey'::regclass")


# Test that hot_standby_feedback works in neon (it is forwarded through
# safekeepers). That is, ensure queries on standby don't fail during load on
# primary under the following conditions:
# - pgbench bombards primary with updates.
# - On the secondary we run long select of the updated table.
# - Set small max_standby_streaming_delay: hs feedback should prevent conflicts
#   so apply doesn't need to wait.
# - Do agressive vacuum on primary which still shouldn't create conflicts.
#   Actually this appears to be redundant due to microvacuum existence.
#
# Without hs feedback enabled we'd see 'User query might have needed to see row
# versions that must be removed.' errors.
def test_hot_standby_feedback(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    env = neon_env_builder.init_start(initial_tenant_conf={"lsn_lease_length": "0s"})
    agressive_vacuum_conf = [
        "log_autovacuum_min_duration = 0",
        "autovacuum_naptime = 10s",
        "autovacuum_vacuum_threshold = 25",
        "autovacuum_vacuum_scale_factor = 0.1",
        "autovacuum_vacuum_cost_delay = -1",
    ]
    with env.endpoints.create_start(
        branch_name="main", endpoint_id="primary", config_lines=agressive_vacuum_conf
    ) as primary:
        # It would be great to have more strict max_standby_streaming_delay=0s here, but then sometimes it fails with
        # 'User was holding shared buffer pin for too long.'.
        with env.endpoints.new_replica_start(
            origin=primary,
            endpoint_id="secondary",
            config_lines=[
                "max_standby_streaming_delay=2s",
                "neon.protocol_version=2",
                "hot_standby_feedback=true",
            ],
        ) as secondary:
            log.info(
                f"primary connstr is {primary.connstr()}, secondary connstr {secondary.connstr()}"
            )

            # s10 is about 150MB of data. In debug mode init takes about 15s on SSD.
            pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", "-s10", primary.connstr()])
            log.info("pgbench init done in primary")

            t = threading.Thread(target=run_pgbench, args=(primary.connstr(), pg_bin))
            t.start()

            # Wait until we see that the pgbench_accounts is created + filled on replica *and*
            # index is created. Otherwise index creation would conflict with
            # read queries and hs feedback won't save us.
            wait_until(partial(pgbench_accounts_initialized, secondary), timeout=60)

            # Test should fail if hs feedback is disabled anyway, but cross
            # check that walproposer sets some xmin.
            def xmin_is_not_null():
                slot_xmin = primary.safe_psql_scalar(
                    "select xmin from pg_replication_slots where slot_name = 'wal_proposer_slot'",
                    log_query=False,
                )
                log.info(f"xmin is {slot_xmin}")
                assert int(slot_xmin) > 0

            wait_until(xmin_is_not_null)
            for _ in range(1, 5):
                # in debug mode takes about 5-7s
                balance = secondary.safe_psql_scalar("select sum(abalance) from pgbench_accounts")
                log.info(f"balance={balance}")
                log_replica_lag(primary, secondary)
            t.join()

        # check xmin is reset when standby is gone
        def xmin_is_null():
            slot_xmin = primary.safe_psql_scalar(
                "select xmin from pg_replication_slots where slot_name = 'wal_proposer_slot'",
                log_query=False,
            )
            log.info(f"xmin is {slot_xmin}")
            assert slot_xmin is None

        wait_until(xmin_is_null)


# Test race condition between WAL replay and backends performing queries
# https://github.com/neondatabase/neon/issues/7791
def test_replica_query_race(neon_simple_env: NeonEnv):
    env = neon_simple_env

    primary_ep = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    )

    with primary_ep.connect() as p_con:
        with p_con.cursor() as p_cur:
            p_cur.execute("CREATE EXTENSION neon_test_utils")
            p_cur.execute("CREATE TABLE test AS SELECT 0 AS counter")

    standby_ep = env.endpoints.new_replica_start(origin=primary_ep, endpoint_id="standby")
    wait_replica_caughtup(primary_ep, standby_ep)

    # In primary, run a lot of UPDATEs on a single page
    finished = False
    writecounter = 1

    async def primary_workload():
        nonlocal writecounter, finished
        conn = await primary_ep.connect_async()
        while writecounter < 10000:
            writecounter += 1
            await conn.execute(f"UPDATE test SET counter = {writecounter}")
        finished = True

    # In standby, at the same time, run queries on it. And repeatedly drop caches
    async def standby_workload():
        nonlocal writecounter, finished
        conn = await standby_ep.connect_async()
        reads = 0
        while not finished:
            readcounter = await conn.fetchval("SELECT counter FROM test")

            # Check that the replica is keeping up with the primary. In local
            # testing, the lag between primary and standby is much smaller, in
            # the ballpark of 2-3 counter values. But be generous in case there's
            # some hiccup.
            # assert(writecounter - readcounter < 1000)
            assert readcounter <= writecounter
            if reads % 100 == 0:
                log.info(f"read {reads}: counter {readcounter}, last update {writecounter}")
            reads += 1

            # FIXME: what about LFC clearing?
            await conn.execute("SELECT clear_buffer_cache()")

    async def both():
        await asyncio.gather(
            primary_workload(),
            standby_workload(),
        )

    asyncio.run(both())
