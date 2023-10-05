import time
from dataclasses import dataclass
from typing import Dict, Tuple

import pytest
import toml
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import wait_for_upload_queue_empty
from fixtures.remote_storage import RemoteStorageKind
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import wait_until

GLOBAL_LRU_LOG_LINE = "tenant_min_resident_size-respecting LRU would not relieve pressure, evicting more following global LRU policy"


@pytest.mark.parametrize("config_level_override", [None, 400])
def test_min_resident_size_override_handling(
    neon_env_builder: NeonEnvBuilder, config_level_override: int
):
    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    def assert_config(tenant_id, expect_override, expect_effective):
        config = ps_http.tenant_config(tenant_id)
        assert config.tenant_specific_overrides.get("min_resident_size_override") == expect_override
        assert config.effective_config.get("min_resident_size_override") == expect_effective

    def assert_overrides(tenant_id, default_tenant_conf_value):
        ps_http.set_tenant_config(tenant_id, {"min_resident_size_override": 200})
        assert_config(tenant_id, 200, 200)

        ps_http.set_tenant_config(tenant_id, {"min_resident_size_override": 0})
        assert_config(tenant_id, 0, 0)

        ps_http.set_tenant_config(tenant_id, {})
        assert_config(tenant_id, None, default_tenant_conf_value)

    env.pageserver.stop()
    if config_level_override is not None:
        env.pageserver.start(
            overrides=(
                "--pageserver-config-override=tenant_config={ min_resident_size_override =  "
                + str(config_level_override)
                + " }",
            )
        )
    else:
        env.pageserver.start()

    tenant_id, _ = env.neon_cli.create_tenant()
    assert_overrides(tenant_id, config_level_override)

    # Also ensure that specifying the parameter to create_tenant works, in addition to http-level recconfig.
    tenant_id, _ = env.neon_cli.create_tenant(conf={"min_resident_size_override": "100"})
    assert_config(tenant_id, 100, 100)
    ps_http.set_tenant_config(tenant_id, {})
    assert_config(tenant_id, None, config_level_override)


@dataclass
class EvictionEnv:
    timelines: list[Tuple[TenantId, TimelineId]]
    neon_env: NeonEnv
    pg_bin: PgBin
    pageserver_http: PageserverHttpClient
    layer_size: int
    pgbench_init_lsns: Dict[TenantId, Lsn]

    def timelines_du(self) -> Tuple[int, int, int]:
        return poor_mans_du(
            self.neon_env, [(tid, tlid) for tid, tlid in self.timelines], verbose=False
        )

    def du_by_timeline(self) -> Dict[Tuple[TenantId, TimelineId], int]:
        return {
            (tid, tlid): poor_mans_du(self.neon_env, [(tid, tlid)], verbose=True)[0]
            for tid, tlid in self.timelines
        }

    def warm_up_tenant(self, tenant_id: TenantId):
        """
        Start a read-only compute at the LSN after pgbench -i, and run pgbench -S against it.
        This assumes that the tenant is still at the state after pbench -i.
        """
        lsn = self.pgbench_init_lsns[tenant_id]
        with self.neon_env.endpoints.create_start("main", tenant_id=tenant_id, lsn=lsn) as endpoint:
            # instead of using pgbench --select-only which does point selects,
            # run full table scans for all tables
            with endpoint.connect() as conn:
                cur = conn.cursor()

                tables_cols = {
                    "pgbench_accounts": "abalance",
                    "pgbench_tellers": "tbalance",
                    "pgbench_branches": "bbalance",
                    "pgbench_history": "delta",
                }

                for table, column in tables_cols.items():
                    cur.execute(f"select avg({column}) from {table}")
                    _avg = cur.fetchone()

    def pageserver_start_with_disk_usage_eviction(
        self, period, max_usage_pct, min_avail_bytes, mock_behavior
    ):
        disk_usage_config = {
            "period": period,
            "max_usage_pct": max_usage_pct,
            "min_avail_bytes": min_avail_bytes,
            "mock_statvfs": mock_behavior,
        }

        enc = toml.TomlEncoder()

        self.neon_env.pageserver.start(
            overrides=(
                "--pageserver-config-override=disk_usage_based_eviction="
                + enc.dump_inline_table(disk_usage_config).replace("\n", " "),
                # Disk usage based eviction runs as a background task.
                # But pageserver startup delays launch of background tasks for some time, to prioritize initial logical size calculations during startup.
                # But, initial logical size calculation may not be triggered if safekeepers don't publish new broker messages.
                # But, we only have a 10-second-timeout in this test.
                # So, disable the delay for this test.
                "--pageserver-config-override=background_task_maximum_delay='0s'",
            ),
        )

        def statvfs_called():
            assert self.neon_env.pageserver.log_contains(".*running mocked statvfs.*")

        wait_until(10, 1, statvfs_called)

        # these can sometimes happen during startup before any tenants have been
        # loaded, so nothing can be evicted, we just wait for next iteration which
        # is able to evict.
        self.neon_env.pageserver.allowed_errors.append(".*WARN.* disk usage still high.*")


def human_bytes(amt: float) -> str:
    suffixes = ["", "Ki", "Mi", "Gi"]

    last = suffixes[-1]

    for name in suffixes:
        if amt < 1024 or name == last:
            return f"{int(round(amt))} {name}B"
        amt = amt / 1024

    raise RuntimeError("unreachable")


@pytest.fixture
def eviction_env(request, neon_env_builder: NeonEnvBuilder, pg_bin: PgBin) -> EvictionEnv:
    """
    Creates two tenants, one somewhat larger than the other.
    """

    log.info(f"setting up eviction_env for test {request.node.name}")

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    # initial tenant will not be present on this pageserver
    env = neon_env_builder.init_configs()
    env.start()
    pageserver_http = env.pageserver.http_client()

    # allow because we are invoking this manually; we always warn on executing disk based eviction
    env.pageserver.allowed_errors.append(r".* running disk usage based eviction due to pressure.*")

    # Choose small layer_size so that we can use low pgbench_scales and still get a large count of layers.
    # Large count of layers and small layer size is good for testing because it makes evictions predictable.
    # Predictable in the sense that many layer evictions will be required to reach the eviction target, because
    # each eviction only makes small progress. That means little overshoot, and thereby stable asserts.
    pgbench_scales = [4, 6]
    layer_size = 5 * 1024**2

    pgbench_init_lsns = {}

    timelines = []
    for scale in pgbench_scales:
        tenant_id, timeline_id = env.neon_cli.create_tenant(
            conf={
                "gc_period": "0s",
                "compaction_period": "0s",
                "checkpoint_distance": f"{layer_size}",
                "image_creation_threshold": "100",
                "compaction_target_size": f"{layer_size}",
            }
        )

        with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
            pg_bin.run(["pgbench", "-i", f"-s{scale}", endpoint.connstr()])
            wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

        timelines.append((tenant_id, timeline_id))

    # stop the safekeepers to avoid on-demand downloads caused by
    # initial logical size calculation triggered by walreceiver connection status
    # when we restart the pageserver process in any of the tests
    env.neon_cli.safekeeper_stop()

    # after stopping the safekeepers, we know that no new WAL will be coming in
    for tenant_id, timeline_id in timelines:
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
        wait_for_upload_queue_empty(pageserver_http, tenant_id, timeline_id)
        tl_info = pageserver_http.timeline_detail(tenant_id, timeline_id)
        assert tl_info["last_record_lsn"] == tl_info["disk_consistent_lsn"]
        assert tl_info["disk_consistent_lsn"] == tl_info["remote_consistent_lsn"]
        pgbench_init_lsns[tenant_id] = Lsn(tl_info["last_record_lsn"])

        layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
        log.info(f"{layers}")
        assert (
            len(layers.historic_layers) >= 10
        ), "evictions happen at layer granularity, but we often assert at byte-granularity"

    eviction_env = EvictionEnv(
        timelines=timelines,
        neon_env=env,
        pageserver_http=pageserver_http,
        layer_size=layer_size,
        pg_bin=pg_bin,
        pgbench_init_lsns=pgbench_init_lsns,
    )

    return eviction_env


def test_broken_tenants_are_skipped(eviction_env: EvictionEnv):
    env = eviction_env

    env.neon_env.pageserver.allowed_errors.append(
        r".* Changing Active tenant to Broken state, reason: broken from test"
    )
    broken_tenant_id, broken_timeline_id = env.timelines[0]
    env.pageserver_http.tenant_break(broken_tenant_id)

    healthy_tenant_id, healthy_timeline_id = env.timelines[1]

    broken_size_pre, _, _ = poor_mans_du(
        env.neon_env, [(broken_tenant_id, broken_timeline_id)], verbose=True
    )
    healthy_size_pre, _, _ = poor_mans_du(
        env.neon_env, [(healthy_tenant_id, healthy_timeline_id)], verbose=True
    )

    # try to evict everything, then validate that broken tenant wasn't touched
    target = broken_size_pre + healthy_size_pre

    response = env.pageserver_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    broken_size_post, _, _ = poor_mans_du(
        env.neon_env, [(broken_tenant_id, broken_timeline_id)], verbose=True
    )
    healthy_size_post, _, _ = poor_mans_du(
        env.neon_env, [(healthy_tenant_id, healthy_timeline_id)], verbose=True
    )

    assert broken_size_pre == broken_size_post, "broken tenant should not be touched"
    assert healthy_size_post < healthy_size_pre
    assert healthy_size_post == 0
    env.neon_env.pageserver.allowed_errors.append(".*" + GLOBAL_LRU_LOG_LINE)


def test_pageserver_evicts_until_pressure_is_relieved(eviction_env: EvictionEnv):
    """
    Basic test to ensure that we evict enough to relieve pressure.
    """
    env = eviction_env
    pageserver_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du()

    target = total_on_disk // 2

    response = pageserver_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du()

    actual_change = total_on_disk - later_total_on_disk

    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "must evict more than half"
    assert (
        response["Finished"]["assumed"]["projected_after"]["freed_bytes"] >= actual_change
    ), "report accurately evicted bytes"
    assert response["Finished"]["assumed"]["failed"]["count"] == 0, "zero failures expected"


def test_pageserver_respects_overridden_resident_size(eviction_env: EvictionEnv):
    """
    Override tenant min resident and ensure that it will be respected by eviction.
    """
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du()
    du_by_timeline = env.du_by_timeline()
    log.info("du_by_timeline: %s", du_by_timeline)

    assert len(du_by_timeline) == 2, "this test assumes two tenants"
    large_tenant = max(du_by_timeline, key=du_by_timeline.__getitem__)
    small_tenant = min(du_by_timeline, key=du_by_timeline.__getitem__)
    assert du_by_timeline[large_tenant] > du_by_timeline[small_tenant]
    assert (
        du_by_timeline[large_tenant] - du_by_timeline[small_tenant] > 5 * env.layer_size
    ), "ensure this test will do more than 1 eviction"

    # Give the larger tenant a haircut while preventing the smaller tenant from getting one.
    # To prevent the smaller from getting a haircut, we set min_resident_size to its current size.
    # To ensure the larger tenant is getting a haircut, any non-zero `target` will do.
    min_resident_size = du_by_timeline[small_tenant]
    target = 1
    assert (
        du_by_timeline[large_tenant] > min_resident_size
    ), "ensure the larger tenant will get a haircut"
    ps_http.patch_tenant_config_client_side(
        small_tenant[0], {"min_resident_size_override": min_resident_size}
    )
    ps_http.patch_tenant_config_client_side(
        large_tenant[0], {"min_resident_size_override": min_resident_size}
    )

    # Make the large tenant more-recently used. An incorrect implementation would try to evict
    # the smaller tenant completely first, before turning to the larger tenant,
    # since the smaller tenant's layers are least-recently-used.
    env.warm_up_tenant(large_tenant[0])

    # do one run
    response = ps_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    time.sleep(1)  # give log time to flush
    assert not env.neon_env.pageserver.log_contains(
        GLOBAL_LRU_LOG_LINE,
    ), "this test is pointless if it fell back to global LRU"

    (later_total_on_disk, _, _) = env.timelines_du()
    later_du_by_timeline = env.du_by_timeline()
    log.info("later_du_by_timeline: %s", later_du_by_timeline)

    actual_change = total_on_disk - later_total_on_disk
    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "eviction must always evict more than target"
    assert (
        response["Finished"]["assumed"]["projected_after"]["freed_bytes"] >= actual_change
    ), "report accurately evicted bytes"
    assert response["Finished"]["assumed"]["failed"]["count"] == 0, "zero failures expected"

    assert (
        later_du_by_timeline[small_tenant] == du_by_timeline[small_tenant]
    ), "small tenant sees no haircut"
    assert (
        later_du_by_timeline[large_tenant] < du_by_timeline[large_tenant]
    ), "large tenant gets a haircut"
    assert du_by_timeline[large_tenant] - later_du_by_timeline[large_tenant] >= target


def test_pageserver_falls_back_to_global_lru(eviction_env: EvictionEnv):
    """
    If we can't relieve pressure using tenant_min_resident_size-respecting eviction,
    we should continue to evict layers following global LRU.
    """
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du()
    target = total_on_disk

    response = ps_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du()
    actual_change = total_on_disk - later_total_on_disk
    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "eviction must always evict more than target"

    time.sleep(1)  # give log time to flush
    assert env.neon_env.pageserver.log_contains(GLOBAL_LRU_LOG_LINE)
    env.neon_env.pageserver.allowed_errors.append(".*" + GLOBAL_LRU_LOG_LINE)


def test_partial_evict_tenant(eviction_env: EvictionEnv):
    """
    Warm up a tenant, then build up pressure to cause in evictions in both.
    We expect
    * the default min resident size to be respect (largest layer file size)
    * the warmed-up tenants layers above min resident size to be evicted after the cold tenant's.
    """
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du()
    du_by_timeline = env.du_by_timeline()

    # pick any tenant
    [warm, cold] = list(du_by_timeline.keys())
    (tenant_id, timeline_id) = warm

    # make picked tenant more recently used than the other one
    env.warm_up_tenant(tenant_id)

    # Build up enough pressure to require evictions from both tenants,
    # but not enough to fall into global LRU.
    # So, set target to all occupied space, except 2*env.layer_size per tenant
    target = du_by_timeline[cold] + (du_by_timeline[warm] // 2) - 2 * 2 * env.layer_size
    response = ps_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du()
    actual_change = total_on_disk - later_total_on_disk
    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "eviction must always evict more than target"

    later_du_by_timeline = env.du_by_timeline()
    for tenant, later_tenant_usage in later_du_by_timeline.items():
        assert (
            later_tenant_usage < du_by_timeline[tenant]
        ), "all tenants should have lost some layers"

    warm_size = later_du_by_timeline[warm]

    # bounds for warmed_size
    warm_lower = 0.5 * du_by_timeline[warm]

    # We don't know exactly whether the cold tenant needs 2 or just 1 env.layer_size wiggle room.
    # So, check for up to 3 here.
    warm_upper = warm_lower + 3 * env.layer_size

    cold_size = later_du_by_timeline[cold]
    cold_upper = 2 * env.layer_size

    log.info(
        f"expecting for warm tenant: {human_bytes(warm_lower)} < {human_bytes(warm_size)} < {human_bytes(warm_upper)}"
    )
    log.info(f"expecting for cold tenant: {human_bytes(cold_size)} < {human_bytes(cold_upper)}")

    assert warm_size > warm_lower, "warmed up tenant should be at about half size (lower)"
    assert warm_size < warm_upper, "warmed up tenant should be at about half size (upper)"

    assert (
        cold_size < cold_upper
    ), "the cold tenant should be evicted to its min_resident_size, i.e., max layer file size"


def poor_mans_du(
    env: NeonEnv, timelines: list[Tuple[TenantId, TimelineId]], verbose: bool = False
) -> Tuple[int, int, int]:
    """
    Disk usage, largest, smallest layer for layer files over the given (tenant, timeline) tuples;
    this could be done over layers endpoint just as well.
    """
    total_on_disk = 0
    largest_layer = 0
    smallest_layer = None
    for tenant_id, timeline_id in timelines:
        timeline_dir = env.pageserver.timeline_dir(tenant_id, timeline_id)
        assert timeline_dir.exists(), f"timeline dir does not exist: {timeline_dir}"
        total = 0
        for file in timeline_dir.iterdir():
            if "__" not in file.name:
                continue
            size = file.stat().st_size
            total += size
            largest_layer = max(largest_layer, size)
            if smallest_layer:
                smallest_layer = min(smallest_layer, size)
            else:
                smallest_layer = size
            if verbose:
                log.info(f"{tenant_id}/{timeline_id} => {file.name} {size} ({human_bytes(size)})")

        if verbose:
            log.info(f"{tenant_id}/{timeline_id}: sum {total} ({human_bytes(total)})")
        total_on_disk += total

    assert smallest_layer is not None or total_on_disk == 0 and largest_layer == 0
    return (total_on_disk, largest_layer, smallest_layer or 0)


def test_statvfs_error_handling(eviction_env: EvictionEnv):
    """
    We should log an error that statvfs fails.
    """
    env = eviction_env
    env.neon_env.pageserver.stop()
    env.pageserver_start_with_disk_usage_eviction(
        period="1s",
        max_usage_pct=90,
        min_avail_bytes=0,
        mock_behavior={
            "type": "Failure",
            "mocked_error": "EIO",
        },
    )

    assert env.neon_env.pageserver.log_contains(".*statvfs failed.*EIO")
    env.neon_env.pageserver.allowed_errors.append(".*statvfs failed.*EIO")


def test_statvfs_pressure_usage(eviction_env: EvictionEnv):
    """
    If statvfs data shows 100% usage, the eviction task will drive it down to
    the configured max_usage_pct.
    """
    env = eviction_env

    env.neon_env.pageserver.stop()

    # make it seem like we're at 100% utilization by setting total bytes to the used bytes
    total_size, _, _ = env.timelines_du()
    blocksize = 512
    total_blocks = (total_size + (blocksize - 1)) // blocksize

    env.pageserver_start_with_disk_usage_eviction(
        period="1s",
        max_usage_pct=33,
        min_avail_bytes=0,
        mock_behavior={
            "type": "Success",
            "blocksize": blocksize,
            "total_blocks": total_blocks,
            # Only count layer files towards used bytes in the mock_statvfs.
            # This avoids accounting for metadata files & tenant conf in the tests.
            "name_filter": ".*__.*",
        },
    )

    def relieved_log_message():
        assert env.neon_env.pageserver.log_contains(".*disk usage pressure relieved")

    wait_until(10, 1, relieved_log_message)

    post_eviction_total_size, _, _ = env.timelines_du()

    assert post_eviction_total_size <= 0.33 * total_size, "we requested max 33% usage"


def test_statvfs_pressure_min_avail_bytes(eviction_env: EvictionEnv):
    """
    If statvfs data shows 100% usage, the eviction task will drive it down to
    at least the configured min_avail_bytes.
    """
    env = eviction_env

    env.neon_env.pageserver.stop()

    # make it seem like we're at 100% utilization by setting total bytes to the used bytes
    total_size, _, _ = env.timelines_du()
    blocksize = 512
    total_blocks = (total_size + (blocksize - 1)) // blocksize

    min_avail_bytes = total_size // 3

    env.pageserver_start_with_disk_usage_eviction(
        period="1s",
        max_usage_pct=100,
        min_avail_bytes=min_avail_bytes,
        mock_behavior={
            "type": "Success",
            "blocksize": blocksize,
            "total_blocks": total_blocks,
            # Only count layer files towards used bytes in the mock_statvfs.
            # This avoids accounting for metadata files & tenant conf in the tests.
            "name_filter": ".*__.*",
        },
    )

    def relieved_log_message():
        assert env.neon_env.pageserver.log_contains(".*disk usage pressure relieved")

    wait_until(10, 1, relieved_log_message)

    post_eviction_total_size, _, _ = env.timelines_du()

    assert (
        total_size - post_eviction_total_size >= min_avail_bytes
    ), "we requested at least min_avail_bytes worth of free space"
