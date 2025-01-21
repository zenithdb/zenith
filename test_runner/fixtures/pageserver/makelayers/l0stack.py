from dataclasses import dataclass

from psycopg2.extensions import connection as PgConnection

from fixtures.common_types import Lsn, ShardIndex, TenantShardId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import wait_for_last_record_lsn


@dataclass
class L0StackShape:
    logical_table_size_mib: int = 50
    delta_stack_height: int = 20


def make_l0_stack(endpoint: Endpoint, shape: L0StackShape):
    env = endpoint.env

    # TDOO: wait for storcon to finish any reonciles before jumping to action here?
    shards = env.storage_controller.tenant_describe(endpoint.tenant_id)

    shard_idx = ShardIndex.parse(shards[0]["shard_id"])
    assert shard_idx.shard_count == 1, "see above"
    tenant_shard_id = TenantShardId(
        endpoint.tenant_id, shard_idx.shard_number, shard_idx.shard_count
    )

    ps = env.get_pageserver(shards[0]["node_id"])

    timeline_id = endpoint.show_timeline_id()

    vps_http = env.storage_controller.pageserver_api()
    ps_http = ps.http_client()
    endpoint_conn = endpoint.connect()
    make_l0_stack_standalone(vps_http, ps_http, tenant_shard_id, timeline_id, endpoint_conn, shape)


def make_l0_stack_standalone(
    vps_http: PageserverHttpClient,
    ps_http: PageserverHttpClient,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    endpoint_conn: PgConnection,
    shape: L0StackShape,
):
    """
    Creates stack of L0 deltas each of which should have 1 Value::Delta per page in `data`.

    Usable from scripts (ad-hoc manual testing in neon_local) and pytest code alike.
    """

    assert (
        not tenant_shard_id.shard_index.is_sharded
    ), "the current implementation only supports unsharded tenants"

    tenant_id = tenant_shard_id.tenant_id
    conn = endpoint_conn
    desired_size = shape.logical_table_size_mib * 1024 * 1024

    config = {
        "gc_period": "0s",  # disable periodic gc
        "checkpoint_timeout": "10 years",
        "compaction_period": "1h",  # doesn't matter, but 0 value will kill walredo every 10s
        "compaction_threshold": 100000,  # we just want L0s
        "compaction_target_size": 134217728,
        "checkpoint_distance": 268435456,
        "image_creation_threshold": 100000,  # we just want L0s
    }

    vps_http.set_tenant_config(tenant_id, config)

    conn.autocommit = True
    cur = conn.cursor()

    # each tuple is 23 (header) + 100 bytes = 123 bytes
    # page header si 24 bytes
    # 8k page size
    # (8k-24bytes) / 123 bytes = 63 tuples per page
    # set fillfactor to 10 to have 6 tuples per page
    cur.execute("DROP TABLE IF EXISTS data")
    cur.execute("CREATE TABLE data(id bigint, row char(92)) with (fillfactor=10)")
    need_pages = desired_size // 8192
    need_rows = need_pages * 6
    log.info(f"Need {need_pages} pages, {need_rows} rows")
    cur.execute(f"INSERT INTO data SELECT i,'row'||i FROM generate_series(1, {need_rows}) as i")

    def settle_and_flush():
        cur.execute("SELECT pg_current_wal_flush_lsn()")
        flush_lsn = Lsn(cur.fetchall()[0][0])
        wait_for_last_record_lsn(ps_http, tenant_shard_id, timeline_id, flush_lsn)
        ps_http.timeline_checkpoint(tenant_id, timeline_id)
        ps_http.timeline_compact(tenant_id, timeline_id)

    # create an L0 for the initial data we just inserted
    settle_and_flush()
    # every iteration updates one tuple in each page
    delta_stack_height = shape.delta_stack_height
    for i in range(0, delta_stack_height):
        log.info(i)
        cur.execute(f"UPDATE data set row = row||',u' where id % 6 = {i%6}")
        log.info("modified rows", cur.rowcount)
        settle_and_flush()
