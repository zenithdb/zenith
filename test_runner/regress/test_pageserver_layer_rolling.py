from __future__ import annotations

import asyncio
import time

import psutil
import pytest
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    tenant_get_shards,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import wait_for_last_record_lsn, wait_for_upload
from fixtures.utils import skip_in_debug_build, wait_until

TIMELINE_COUNT = 10
ENTRIES_PER_TIMELINE = 10_000
CHECKPOINT_TIMEOUT_SECONDS = 60


async def run_worker_for_tenant(
    env: NeonEnv,
    entries: int,
    tenant: TenantId,
    offset: int | None = None,
) -> Lsn:
    if offset is None:
        offset = 0

    with env.endpoints.create_start("main", tenant_id=tenant) as ep:
        conn = await ep.connect_async()
        try:
            await conn.execute("CREATE TABLE IF NOT EXISTS t(key serial primary key, value text)")
            await conn.execute(
                f"INSERT INTO t SELECT i, CONCAT('payload_', i) FROM generate_series({offset},{entries}) as i"
            )
        finally:
            await conn.close(timeout=10)

        loop = asyncio.get_running_loop()
        sql = await loop.run_in_executor(
            None, lambda ep: ep.safe_psql("SELECT pg_current_wal_flush_lsn()"), ep
        )
        last_flush_lsn = Lsn(sql[0][0])
        return last_flush_lsn


async def run_worker(env: NeonEnv, tenant_conf, entries: int) -> tuple[TenantId, TimelineId, Lsn]:
    loop = asyncio.get_running_loop()
    # capture tenant_conf by specifying `tenant_conf=tenant_conf`, otherwise it will be evaluated to some random value
    tenant, timeline = await loop.run_in_executor(
        None, lambda tenant_conf, env: env.create_tenant(conf=tenant_conf), tenant_conf, env
    )
    last_flush_lsn = await run_worker_for_tenant(env, entries, tenant)
    return tenant, timeline, last_flush_lsn


async def workload(
    env: NeonEnv, tenant_conf, timelines: int, entries: int
) -> list[tuple[TenantId, TimelineId, Lsn]]:
    workers = [asyncio.create_task(run_worker(env, tenant_conf, entries)) for _ in range(timelines)]
    return await asyncio.gather(*workers)


def wait_until_pageserver_is_caught_up(
    env: NeonEnv, last_flush_lsns: list[tuple[TenantId, TimelineId, Lsn]]
):
    for tenant, timeline, last_flush_lsn in last_flush_lsns:
        shards = tenant_get_shards(env, tenant)
        for tenant_shard_id, pageserver in shards:
            waited = wait_for_last_record_lsn(
                pageserver.http_client(), tenant_shard_id, timeline, last_flush_lsn
            )
            assert waited >= last_flush_lsn


def wait_until_pageserver_has_uploaded(
    env: NeonEnv, last_flush_lsns: list[tuple[TenantId, TimelineId, Lsn]]
):
    for tenant, timeline, last_flush_lsn in last_flush_lsns:
        shards = tenant_get_shards(env, tenant)
        for tenant_shard_id, pageserver in shards:
            wait_for_upload(pageserver.http_client(), tenant_shard_id, timeline, last_flush_lsn)


def wait_for_wal_ingest_metric(pageserver_http: PageserverHttpClient) -> float:
    def query():
        value = pageserver_http.get_metric_value("pageserver_wal_ingest_records_received_total")
        assert value is not None
        return value

    # The metric gets initialised on the first update.
    # Retry a few times, but return 0 if it's stable.
    try:
        return float(wait_until(query, timeout=2, interval=0.5))
    except Exception:
        return 0


def get_dirty_bytes(env):
    v = env.pageserver.http_client().get_metric_value("pageserver_timeline_ephemeral_bytes") or 0
    log.info(f"dirty_bytes: {v}")
    return v


def assert_dirty_bytes(env, v):
    assert get_dirty_bytes(env) == v


def assert_dirty_bytes_nonzero(env):
    dirty_bytes = get_dirty_bytes(env)
    assert dirty_bytes > 0
    return dirty_bytes


@pytest.mark.parametrize("immediate_shutdown", [True, False])
def test_pageserver_small_inmemory_layers(
    neon_env_builder: NeonEnvBuilder, immediate_shutdown: bool
):
    """
    Test that open layers get flushed after the `checkpoint_timeout` config
    and do not require WAL reingest upon restart.

    The workload creates a number of timelines and writes some data to each,
    but not enough to trigger flushes via the `checkpoint_distance` config.
    """
    tenant_conf = {
        # Large `checkpoint_distance` effectively disables size
        # based checkpointing.
        "checkpoint_distance": f"{2 * 1024 ** 3}",
        "checkpoint_timeout": f"{CHECKPOINT_TIMEOUT_SECONDS}s",
        "compaction_period": "1s",
    }

    env = neon_env_builder.init_configs()
    env.start()

    last_flush_lsns = asyncio.run(workload(env, tenant_conf, TIMELINE_COUNT, ENTRIES_PER_TIMELINE))
    wait_until_pageserver_is_caught_up(env, last_flush_lsns)

    # We didn't write enough data to trigger a size-based checkpoint: we should see dirty data.
    wait_until(lambda: assert_dirty_bytes_nonzero(env))

    ps_http_client = env.pageserver.http_client()
    total_wal_ingested_before_restart = wait_for_wal_ingest_metric(ps_http_client)

    # Within ~ the checkpoint interval, all the ephemeral layers should be frozen and flushed,
    # such that there are zero bytes of ephemeral layer left on the pageserver
    log.info("Waiting for background checkpoints...")
    wait_until(lambda: assert_dirty_bytes(env, 0), timeout=2 * CHECKPOINT_TIMEOUT_SECONDS)

    # Zero ephemeral layer bytes does not imply that all the frozen layers were uploaded: they
    # must be uploaded to remain visible to the pageserver after restart.
    wait_until_pageserver_has_uploaded(env, last_flush_lsns)

    env.pageserver.restart(immediate=immediate_shutdown)
    wait_until_pageserver_is_caught_up(env, last_flush_lsns)

    # Catching up with WAL ingest should have resulted in zero bytes of ephemeral layers, since
    # we froze, flushed and uploaded everything before restarting.  There can be no more WAL writes
    # because we shut down compute endpoints before flushing.
    assert get_dirty_bytes(env) == 0

    total_wal_ingested_after_restart = wait_for_wal_ingest_metric(ps_http_client)

    log.info(f"WAL ingested before restart: {total_wal_ingested_before_restart}")
    log.info(f"WAL ingested after restart: {total_wal_ingested_after_restart}")

    assert total_wal_ingested_after_restart == 0


def test_idle_checkpoints(neon_env_builder: NeonEnvBuilder):
    """
    Test that `checkpoint_timeout` is enforced even if there is no safekeeper input.
    """
    tenant_conf = {
        # Large `checkpoint_distance` effectively disables size
        # based checkpointing.
        "checkpoint_distance": f"{2 * 1024 ** 3}",
        "checkpoint_timeout": f"{CHECKPOINT_TIMEOUT_SECONDS}s",
        "compaction_period": "1s",
    }

    env = neon_env_builder.init_configs()
    env.start()

    last_flush_lsns = asyncio.run(workload(env, tenant_conf, TIMELINE_COUNT, ENTRIES_PER_TIMELINE))
    wait_until_pageserver_is_caught_up(env, last_flush_lsns)

    # We didn't write enough data to trigger a size-based checkpoint: we should see dirty data.
    wait_until(lambda: assert_dirty_bytes_nonzero(env))

    # Stop the safekeepers, so that we cannot have any more WAL receiver connections
    for sk in env.safekeepers:
        sk.stop()

    # We should have got here fast enough that we didn't hit the background interval yet,
    # and the teardown of SK connections shouldn't prompt any layer freezing.
    assert get_dirty_bytes(env) > 0

    # Within ~ the checkpoint interval, all the ephemeral layers should be frozen and flushed,
    # such that there are zero bytes of ephemeral layer left on the pageserver
    log.info("Waiting for background checkpoints...")
    wait_until(lambda: assert_dirty_bytes(env, 0), timeout=2 * CHECKPOINT_TIMEOUT_SECONDS)

    # The code below verifies that we do not flush on the first write
    # after an idle period longer than the checkpoint timeout.

    # Sit quietly for longer than the checkpoint timeout
    time.sleep(CHECKPOINT_TIMEOUT_SECONDS + CHECKPOINT_TIMEOUT_SECONDS / 2)

    # Restart the safekeepers and write a bit of extra data into one tenant
    for sk in env.safekeepers:
        sk.start()

    tenant_with_extra_writes = last_flush_lsns[0][0]
    asyncio.run(
        run_worker_for_tenant(env, 5, tenant_with_extra_writes, offset=ENTRIES_PER_TIMELINE)
    )

    dirty_after_write = wait_until(lambda: assert_dirty_bytes_nonzero(env))

    # We shouldn't flush since we've just opened a new layer
    waited_for = 0
    while waited_for < CHECKPOINT_TIMEOUT_SECONDS // 4:
        time.sleep(5)
        waited_for += 5

        assert get_dirty_bytes(env) >= dirty_after_write


# We have to use at least ~100MB of data to hit the lowest limit we can configure, which is
# prohibitively slow in debug mode
@skip_in_debug_build("Avoid running bulkier ingest tests in debug mode")
def test_total_size_limit(neon_env_builder: NeonEnvBuilder):
    """
    Test that checkpoints are done based on total ephemeral layer size, even if no one timeline is
    individually exceeding checkpoint thresholds.
    """

    system_memory = psutil.virtual_memory().total

    # The smallest total size limit we can configure is 1/1024th of the system memory (e.g. 128MB on
    # a system with 128GB of RAM).  We will then write enough data to violate this limit.
    max_dirty_data = 128 * 1024 * 1024
    ephemeral_bytes_per_memory_kb = (max_dirty_data * 1024) // system_memory
    assert ephemeral_bytes_per_memory_kb > 0

    neon_env_builder.pageserver_config_override = f"""
        ephemeral_bytes_per_memory_kb={ephemeral_bytes_per_memory_kb}
        """

    compaction_period_s = 10

    checkpoint_distance = 1024**3
    tenant_conf = {
        # Large space + time thresholds: effectively disable these limits
        "checkpoint_distance": f"{checkpoint_distance}",
        "checkpoint_timeout": "3600s",
        "compaction_period": f"{compaction_period_s}s",
    }

    env = neon_env_builder.init_configs()
    env.start()

    timeline_count = 10

    # This is about 2MiB of data per timeline
    entries_per_timeline = 100_000

    last_flush_lsns = asyncio.run(workload(env, tenant_conf, timeline_count, entries_per_timeline))
    wait_until_pageserver_is_caught_up(env, last_flush_lsns)

    total_bytes_ingested = 0
    for tenant, timeline, last_flush_lsn in last_flush_lsns:
        http_client = env.pageserver.http_client()
        initdb_lsn = Lsn(http_client.timeline_detail(tenant, timeline)["initdb_lsn"])
        this_timeline_ingested = last_flush_lsn - initdb_lsn
        assert (
            this_timeline_ingested < checkpoint_distance * 0.8
        ), "this test is supposed to fill InMemoryLayer"
        total_bytes_ingested += this_timeline_ingested

    log.info(f"Ingested {total_bytes_ingested} bytes since initdb (vs max dirty {max_dirty_data})")
    assert total_bytes_ingested > max_dirty_data

    # Expected end state: the total physical size of all the tenants is in excess of the max dirty
    # data, but the total amount of dirty data is less than the limit: this demonstrates that we
    # have exceeded the threshold but then rolled layers in response
    def get_total_historic_layers():
        total_ephemeral_layers = 0
        total_historic_bytes = 0
        for tenant, timeline, _last_flush_lsn in last_flush_lsns:
            http_client = env.pageserver.http_client()
            initdb_lsn = Lsn(http_client.timeline_detail(tenant, timeline)["initdb_lsn"])
            layer_map = http_client.layer_map_info(tenant, timeline)
            total_historic_bytes += sum(
                layer.layer_file_size
                for layer in layer_map.historic_layers
                if Lsn(layer.lsn_start) > initdb_lsn
            )
            total_ephemeral_layers += len(layer_map.in_memory_layers)

        log.info(
            f"Total historic layer bytes: {total_historic_bytes} ({total_ephemeral_layers} ephemeral layers)"
        )

        return total_historic_bytes

    def assert_bytes_rolled():
        assert total_bytes_ingested - get_total_historic_layers() <= max_dirty_data

    # Wait until enough layers have rolled that the amount of dirty data is under the threshold.
    # We do this indirectly via layer maps, rather than the dirty bytes metric, to avoid false-passing
    # if that metric isn't updated quickly enough to reflect the dirty bytes exceeding the limit.
    wait_until(assert_bytes_rolled, timeout=2 * compaction_period_s)

    # The end state should also have the reported metric under the limit
    def assert_dirty_data_limited():
        dirty_bytes = get_dirty_bytes(env)
        assert dirty_bytes < max_dirty_data

    wait_until(lambda: assert_dirty_data_limited(), timeout=2 * compaction_period_s)
