from __future__ import annotations

import os

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    logical_replication_sync,
    wait_for_last_flush_lsn,
)
from fixtures.pg_version import PgVersion
from fixtures.utils import skip_on_postgres


@skip_on_postgres(
    PgVersion.V14,
    reason="pg_log_standby_snapshot() function is available since Postgres 16",
)
@skip_on_postgres(
    PgVersion.V15,
    reason="pg_log_standby_snapshot() function is available since Postgres 16",
)
def test_layer_bloating(neon_env_builder: NeonEnvBuilder, vanilla_pg):
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
            "compaction_period": "0s",
            "compaction_threshold": 99999,
            "image_creation_threshold": 99999,
        }
    )

    timeline = env.initial_timeline
    endpoint = env.endpoints.create_start("main", config_lines=["log_statement=all"])

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # create table...
    cur.execute("create table t(pk integer primary key)")
    cur.execute("create publication pub1 for table t")
    # Create slot to hold WAL
    cur.execute("select pg_create_logical_replication_slot('my_slot', 'pgoutput')")

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key)")

    connstr = endpoint.connstr().replace("'", "''")
    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    cur.execute(
        """create or replace function create_snapshots(n integer) returns void as $$
                   declare
                     i integer;
                   begin
                     for i in 1..n loop
                       perform pg_log_standby_snapshot();
                     end loop;
                   end; $$ language plpgsql"""
    )
    cur.execute("set statement_timeout=0")
    cur.execute("select create_snapshots(10000)")
    # Wait logical replication to sync
    logical_replication_sync(vanilla_pg, endpoint)
    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, timeline)
    env.pageserver.http_client().timeline_checkpoint(env.initial_tenant, timeline, compact=False)

    # Check layer file sizes
    timeline_path = f"{env.pageserver.workdir}/tenants/{env.initial_tenant}/timelines/{timeline}/"
    log.info(f"Check {timeline_path}")
    for filename in os.listdir(timeline_path):
        if filename.startswith("00000"):
            log.info(f"layer {filename} size is {os.path.getsize(timeline_path + filename)}")
            assert os.path.getsize(timeline_path + filename) < 512_000_000

    env.stop(immediate=True)
