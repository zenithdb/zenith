from __future__ import annotations

import enum
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

import pytest
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    NeonPageserver,
    PgBin,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import wait_for_upload_queue_empty
from fixtures.remote_storage import RemoteStorageKind
from fixtures.utils import human_bytes, wait_until

if TYPE_CHECKING:
    from typing import Any


GLOBAL_LRU_LOG_LINE = "tenant_min_resident_size-respecting LRU would not relieve pressure, evicting more following global LRU policy"

# access times in the pageserver are stored at a very low resolution: to generate meaningfully different
# values, tests must inject sleeps
ATIME_RESOLUTION = 2


@pytest.mark.parametrize("config_level_override", [None, 400])
def test_min_resident_size_override_handling(
    neon_env_builder: NeonEnvBuilder, config_level_override: int
):
    env = neon_env_builder.init_start()
    vps_http = env.storage_controller.pageserver_api()
    ps_http = env.pageserver.http_client()

    def assert_config(tenant_id, expect_override, expect_effective):
        # talk to actual pageserver to _get_ the config, workaround for
        # https://github.com/neondatabase/neon/issues/9621
        config = ps_http.tenant_config(tenant_id)
        assert config.tenant_specific_overrides.get("min_resident_size_override") == expect_override
        assert config.effective_config.get("min_resident_size_override") == expect_effective

    def assert_overrides(tenant_id, default_tenant_conf_value):
        vps_http.set_tenant_config(tenant_id, {"min_resident_size_override": 200})
        assert_config(tenant_id, 200, 200)

        vps_http.set_tenant_config(tenant_id, {"min_resident_size_override": 0})
        assert_config(tenant_id, 0, 0)

        vps_http.set_tenant_config(tenant_id, {})
        assert_config(tenant_id, None, default_tenant_conf_value)

    if config_level_override is not None:

        def set_min_resident_size(config):
            tenant_config = config.setdefault("tenant_config", {})
            tenant_config["min_resident_size_override"] = config_level_override

        env.pageserver.edit_config_toml(set_min_resident_size)
    env.pageserver.stop()
    env.pageserver.start()

    tenant_id, _ = env.create_tenant()
    assert_overrides(tenant_id, config_level_override)

    # Also ensure that specifying the paramter to create_tenant works, in addition to http-level recconfig.
    tenant_id, _ = env.create_tenant(conf={"min_resident_size_override": "100"})
    assert_config(tenant_id, 100, 100)
    vps_http.set_tenant_config(tenant_id, {})
    assert_config(tenant_id, None, config_level_override)


@enum.unique
class EvictionOrder(StrEnum):
    RELATIVE_ORDER_EQUAL = "relative_equal"
    RELATIVE_ORDER_SPARE = "relative_spare"

    def config(self) -> dict[str, Any]:
        if self == EvictionOrder.RELATIVE_ORDER_EQUAL:
            return {
                "type": "RelativeAccessed",
                "args": {"highest_layer_count_loses_first": False},
            }
        elif self == EvictionOrder.RELATIVE_ORDER_SPARE:
            return {
                "type": "RelativeAccessed",
                "args": {"highest_layer_count_loses_first": True},
            }
        else:
            raise RuntimeError(f"not implemented: {self}")


@dataclass
class EvictionEnv:
    timelines: list[tuple[TenantId, TimelineId]]
    neon_env: NeonEnv
    pg_bin: PgBin
    pageserver_http: PageserverHttpClient
    layer_size: int
    pgbench_init_lsns: dict[TenantId, Lsn]

    @property
    def pageserver(self):
        """
        Shortcut for tests that only use one pageserver.
        """
        return self.neon_env.pageserver

    def timelines_du(self, pageserver: NeonPageserver) -> tuple[int, int, int]:
        return poor_mans_du(
            self.neon_env,
            [(tid, tlid) for tid, tlid in self.timelines],
            pageserver,
            verbose=False,
        )

    def du_by_timeline(self, pageserver: NeonPageserver) -> dict[tuple[TenantId, TimelineId], int]:
        return {
            (tid, tlid): poor_mans_du(self.neon_env, [(tid, tlid)], pageserver, verbose=True)[0]
            for tid, tlid in self.timelines
        }

    def count_layers_per_tenant(self, pageserver: NeonPageserver) -> dict[TenantId, int]:
        return count_layers_per_tenant(pageserver, self.timelines)

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
        self,
        pageserver: NeonPageserver,
        period,
        max_usage_pct,
        min_avail_bytes,
        mock_behavior,
        eviction_order: EvictionOrder,
    ):
        """
        Starts pageserver up with mocked statvfs setup. The startup is
        problematic because of dueling initial logical size calculations
        requiring layers and disk usage based task evicting.

        Returns after initial logical sizes are complete, but the phase of disk
        usage eviction task is unknown; it might need to run one more iteration
        before assertions can be made.
        """

        # these can sometimes happen during startup before any tenants have been
        # loaded, so nothing can be evicted, we just wait for next iteration which
        # is able to evict.
        pageserver.allowed_errors.append(".*WARN.* disk usage still high.*")

        pageserver.patch_config_toml_nonrecursive(
            {
                "disk_usage_based_eviction": {
                    "period": period,
                    "max_usage_pct": max_usage_pct,
                    "min_avail_bytes": min_avail_bytes,
                    "mock_statvfs": mock_behavior,
                    "eviction_order": eviction_order.config(),
                },
                # Disk usage based eviction runs as a background task.
                # But pageserver startup delays launch of background tasks for some time, to prioritize initial logical size calculations during startup.
                # But, initial logical size calculation may not be triggered if safekeepers don't publish new broker messages.
                # But, we only have a 10-second-timeout in this test.
                # So, disable the delay for this test.
                "background_task_maximum_delay": "0s",
            }
        )

        pageserver.start()

        # we now do initial logical size calculation on startup, which on debug builds can fight with disk usage based eviction
        for tenant_id, timeline_id in self.timelines:
            tenant_ps = self.neon_env.get_tenant_pageserver(tenant_id)
            # Pageserver may be none if we are currently not attached anywhere, e.g. during secondary eviction test
            if tenant_ps is not None:
                tenant_ps.http_client().timeline_wait_logical_size(tenant_id, timeline_id)

        def statvfs_called():
            pageserver.assert_log_contains(".*running mocked statvfs.*")

        # we most likely have already completed multiple runs
        wait_until(statvfs_called)


def count_layers_per_tenant(
    pageserver: NeonPageserver, timelines: Iterable[tuple[TenantId, TimelineId]]
) -> dict[TenantId, int]:
    ret: Counter[TenantId] = Counter()

    for tenant_id, timeline_id in timelines:
        timeline_dir = pageserver.timeline_dir(tenant_id, timeline_id)
        assert timeline_dir.exists()
        for file in timeline_dir.iterdir():
            if "__" not in file.name:
                continue
            ret[tenant_id] += 1

    return dict(ret)


def _eviction_env(
    request, neon_env_builder: NeonEnvBuilder, pg_bin: PgBin, num_pageservers: int
) -> EvictionEnv:
    """
    Creates two tenants, one somewhat larger than the other.
    """

    log.info(f"setting up eviction_env for test {request.node.name}")

    neon_env_builder.num_pageservers = num_pageservers
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    # Disable compression support for EvictionEnv to get larger layer sizes
    neon_env_builder.pageserver_config_override = "image_compression='disabled'"

    # initial tenant will not be present on this pageserver
    env = neon_env_builder.init_configs()
    env.start()

    # allow because we are invoking this manually; we always warn on executing disk based eviction
    for ps in env.pageservers:
        ps.allowed_errors.append(r".* running disk usage based eviction due to pressure.*")

    # Choose small layer_size so that we can use low pgbench_scales and still get a large count of layers.
    # Large count of layers and small layer size is good for testing because it makes evictions predictable.
    # Predictable in the sense that many layer evictions will be required to reach the eviction target, because
    # each eviction only makes small progress. That means little overshoot, and thereby stable asserts.
    pgbench_scales = [4, 6]
    layer_size = 5 * 1024**2

    pgbench_init_lsns = {}

    timelines = []
    for scale in pgbench_scales:
        timelines.append(pgbench_init_tenant(layer_size, scale, env, pg_bin))

    # stop the safekeepers to avoid on-demand downloads caused by
    # initial logical size calculation triggered by walreceiver connection status
    # when we restart the pageserver process in any of the tests
    env.neon_cli.safekeeper_stop()

    # after stopping the safekeepers, we know that no new WAL will be coming in
    for tenant_id, timeline_id in timelines:
        pgbench_init_lsns[tenant_id] = finish_tenant_creation(env, tenant_id, timeline_id, 10)

    eviction_env = EvictionEnv(
        timelines=timelines,
        neon_env=env,
        # this last tenant http client works for num_pageservers=1
        pageserver_http=env.get_tenant_pageserver(timelines[-1][0]).http_client(),
        layer_size=layer_size,
        pg_bin=pg_bin,
        pgbench_init_lsns=pgbench_init_lsns,
    )

    return eviction_env


def pgbench_init_tenant(
    layer_size: int, scale: int, env: NeonEnv, pg_bin: PgBin
) -> tuple[TenantId, TimelineId]:
    tenant_id, timeline_id = env.create_tenant(
        conf={
            "gc_period": "0s",
            "compaction_period": "0s",
            "checkpoint_distance": f"{layer_size}",
            "image_creation_threshold": "999999",
            "compaction_target_size": f"{layer_size}",
        }
    )

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        pg_bin.run(["pgbench", "-i", "-I", "dtGvp", f"-s{scale}", endpoint.connstr()])
        wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    return (tenant_id, timeline_id)


def finish_tenant_creation(
    env: NeonEnv,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    min_expected_layers: int,
) -> Lsn:
    pageserver_http = env.get_tenant_pageserver(tenant_id).http_client()
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload_queue_empty(pageserver_http, tenant_id, timeline_id)
    tl_info = pageserver_http.timeline_detail(tenant_id, timeline_id)
    assert tl_info["last_record_lsn"] == tl_info["disk_consistent_lsn"]
    assert tl_info["disk_consistent_lsn"] == tl_info["remote_consistent_lsn"]
    pgbench_init_lsn = Lsn(tl_info["last_record_lsn"])

    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    # log.info(f"{layers}")
    assert (
        len(layers.historic_layers) >= min_expected_layers
    ), "evictions happen at layer granularity, but we often assert at byte-granularity"

    return pgbench_init_lsn


@pytest.fixture
def eviction_env(request, neon_env_builder: NeonEnvBuilder, pg_bin: PgBin) -> EvictionEnv:
    return _eviction_env(request, neon_env_builder, pg_bin, num_pageservers=1)


@pytest.fixture
def eviction_env_ha(request, neon_env_builder: NeonEnvBuilder, pg_bin: PgBin) -> EvictionEnv:
    """
    Variant of the eviction environment with two pageservers for testing eviction on
    HA configurations with a secondary location.
    """
    return _eviction_env(request, neon_env_builder, pg_bin, num_pageservers=2)


def test_broken_tenants_are_skipped(eviction_env: EvictionEnv):
    env = eviction_env

    env.neon_env.pageserver.allowed_errors.append(
        r".* Changing Active tenant to Broken state, reason: broken from test"
    )
    broken_tenant_id, broken_timeline_id = env.timelines[0]
    env.pageserver_http.tenant_break(broken_tenant_id)

    healthy_tenant_id, healthy_timeline_id = env.timelines[1]

    broken_size_pre, _, _ = poor_mans_du(
        env.neon_env,
        [(broken_tenant_id, broken_timeline_id)],
        env.pageserver,
        verbose=True,
    )
    healthy_size_pre, _, _ = poor_mans_du(
        env.neon_env,
        [(healthy_tenant_id, healthy_timeline_id)],
        env.pageserver,
        verbose=True,
    )

    # try to evict everything, then validate that broken tenant wasn't touched
    target = broken_size_pre + healthy_size_pre

    response = env.pageserver_http.disk_usage_eviction_run({"evict_bytes": target})
    log.info(f"{response}")

    broken_size_post, _, _ = poor_mans_du(
        env.neon_env,
        [(broken_tenant_id, broken_timeline_id)],
        env.pageserver,
        verbose=True,
    )
    healthy_size_post, _, _ = poor_mans_du(
        env.neon_env,
        [(healthy_tenant_id, healthy_timeline_id)],
        env.pageserver,
        verbose=True,
    )

    assert broken_size_pre == broken_size_post, "broken tenant should not be touched"
    assert healthy_size_post < healthy_size_pre
    assert healthy_size_post == 0
    env.neon_env.pageserver.allowed_errors.append(".*" + GLOBAL_LRU_LOG_LINE)


@pytest.mark.parametrize(
    "order",
    [EvictionOrder.RELATIVE_ORDER_EQUAL],
)
def test_pageserver_evicts_until_pressure_is_relieved(
    eviction_env: EvictionEnv, order: EvictionOrder
):
    """
    Basic test to ensure that we evict enough to relieve pressure.
    """
    env = eviction_env
    pageserver_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du(env.pageserver)

    target = total_on_disk // 2

    response = pageserver_http.disk_usage_eviction_run(
        {"evict_bytes": target, "eviction_order": order.config()}
    )
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du(env.pageserver)

    actual_change = total_on_disk - later_total_on_disk

    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "must evict more than half"
    assert (
        response["Finished"]["assumed"]["projected_after"]["freed_bytes"] >= actual_change
    ), "report accurately evicted bytes"
    assert response["Finished"]["assumed"]["failed"]["count"] == 0, "zero failures expected"


@pytest.mark.parametrize(
    "order",
    [EvictionOrder.RELATIVE_ORDER_EQUAL],
)
def test_pageserver_respects_overridden_resident_size(
    eviction_env: EvictionEnv, order: EvictionOrder
):
    """
    Override tenant min resident and ensure that it will be respected by eviction.
    """
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du(env.pageserver)
    du_by_timeline = env.du_by_timeline(env.pageserver)
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
    env.neon_env.storage_controller.pageserver_api().update_tenant_config(
        small_tenant[0], {"min_resident_size_override": min_resident_size}
    )
    env.neon_env.storage_controller.pageserver_api().update_tenant_config(
        large_tenant[0], {"min_resident_size_override": min_resident_size}
    )

    # Make the large tenant more-recently used. An incorrect implemention would try to evict
    # the smaller tenant completely first, before turning to the larger tenant,
    # since the smaller tenant's layers are least-recently-used.
    env.warm_up_tenant(large_tenant[0])

    # do one run
    response = ps_http.disk_usage_eviction_run(
        {"evict_bytes": target, "eviction_order": order.config()}
    )
    log.info(f"{response}")

    time.sleep(1)  # give log time to flush
    assert not env.neon_env.pageserver.log_contains(
        GLOBAL_LRU_LOG_LINE,
    ), "this test is pointless if it fell back to global LRU"

    (later_total_on_disk, _, _) = env.timelines_du(env.pageserver)
    later_du_by_timeline = env.du_by_timeline(env.pageserver)
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


@pytest.mark.parametrize(
    "order",
    [EvictionOrder.RELATIVE_ORDER_EQUAL],
)
def test_pageserver_falls_back_to_global_lru(eviction_env: EvictionEnv, order: EvictionOrder):
    """
    If we can't relieve pressure using tenant_min_resident_size-respecting eviction,
    we should continue to evict layers following global LRU.
    """
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du(env.pageserver)
    target = total_on_disk

    response = ps_http.disk_usage_eviction_run(
        {"evict_bytes": target, "eviction_order": order.config()}
    )
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du(env.pageserver)
    actual_change = total_on_disk - later_total_on_disk
    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "eviction must always evict more than target"

    time.sleep(1)  # give log time to flush
    env.neon_env.pageserver.assert_log_contains(GLOBAL_LRU_LOG_LINE)
    env.neon_env.pageserver.allowed_errors.append(".*" + GLOBAL_LRU_LOG_LINE)


@pytest.mark.parametrize(
    "order",
    [
        EvictionOrder.RELATIVE_ORDER_EQUAL,
        EvictionOrder.RELATIVE_ORDER_SPARE,
    ],
)
def test_partial_evict_tenant(eviction_env: EvictionEnv, order: EvictionOrder):
    """
    Warm up a tenant, then build up pressure to cause in evictions in both.
    We expect
    * the default min resident size to be respect (largest layer file size)
    * the warmed-up tenants layers above min resident size to be evicted after the cold tenant's.
    """
    env = eviction_env
    ps_http = env.pageserver_http

    (total_on_disk, _, _) = env.timelines_du(env.pageserver)
    du_by_timeline = env.du_by_timeline(env.pageserver)
    tenant_layers = env.count_layers_per_tenant(env.pageserver)

    # pick smaller or greater (iteration order is insertion order of scale=4 and scale=6)
    [warm, cold] = list(du_by_timeline.keys())
    (tenant_id, timeline_id) = warm

    # make picked tenant more recently used than the other one
    time.sleep(ATIME_RESOLUTION)
    env.warm_up_tenant(tenant_id)

    # Build up enough pressure to require evictions from both tenants,
    # but not enough to fall into global LRU.
    # So, set target to all occupied space, except 2*env.layer_size per tenant
    target = du_by_timeline[cold] + (du_by_timeline[warm] // 2) - 2 * 2 * env.layer_size
    response = ps_http.disk_usage_eviction_run(
        {"evict_bytes": target, "eviction_order": order.config()}
    )
    log.info(f"{response}")

    (later_total_on_disk, _, _) = env.timelines_du(env.pageserver)
    actual_change = total_on_disk - later_total_on_disk
    assert 0 <= actual_change, "nothing can load layers during this test"
    assert actual_change >= target, "eviction must always evict more than target"

    later_du_by_timeline = env.du_by_timeline(env.pageserver)
    for tenant, later_tenant_usage in later_du_by_timeline.items():
        assert (
            later_tenant_usage < du_by_timeline[tenant]
        ), "all tenants should have lost some layers"

    # with relative order what matters is the amount of layers, with a
    # fudge factor of whether the eviction bothers tenants with highest
    # layer count the most. last accessed times between tenants does not
    # matter.
    assert order in [EvictionOrder.RELATIVE_ORDER_EQUAL, EvictionOrder.RELATIVE_ORDER_SPARE]
    layers_now = env.count_layers_per_tenant(env.pageserver)

    expected_ratio = later_total_on_disk / total_on_disk
    log.info(
        f"freed up {100 * expected_ratio}%, expecting the layer counts to decrease in similar ratio"
    )

    for tenant_id, original_count in tenant_layers.items():
        count_now = layers_now[tenant_id]
        ratio = count_now / original_count
        abs_diff = abs(ratio - expected_ratio)
        assert original_count > count_now

        expectation = 0.065
        log.info(
            f"tenant {tenant_id} layer count {original_count} -> {count_now}, ratio: {ratio}, expecting {abs_diff} < {expectation}"
        )
        # in this test case both relative_spare and relative_equal produce
        # the same outcomes; this must be a quantization effect of similar
        # sizes (-s4 and -s6) and small (5MB) layer size.
        # for pg15 and pg16 the absdiff is < 0.01, for pg14 it is closer to 0.02
        assert abs_diff < expectation


@pytest.mark.parametrize(
    "order",
    [
        EvictionOrder.RELATIVE_ORDER_EQUAL,
        EvictionOrder.RELATIVE_ORDER_SPARE,
    ],
)
def test_fast_growing_tenant(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin, order: EvictionOrder):
    """
    Create in order first smaller tenants and finally a single larger tenant.
    Assert that with relative order modes, the disk usage based eviction is
    more fair towards the smaller tenants.
    """
    env = neon_env_builder.init_configs()
    env.start()
    env.pageserver.allowed_errors.append(r".* running disk usage based eviction due to pressure.*")

    # initial_tenant and initial_timeline do not exist

    # create N tenants the same fashion as EvictionEnv
    layer_size = 5 * 1024**2
    timelines = []
    for scale in [1, 1, 1, 4]:
        timelines.append((pgbench_init_tenant(layer_size, scale, env, pg_bin), scale))

        # Eviction times are stored at a low resolution.  We must ensure that the time between
        # tenants is long enough for the pageserver to distinguish them.
        time.sleep(ATIME_RESOLUTION)

    env.neon_cli.safekeeper_stop()

    for (tenant_id, timeline_id), scale in timelines:
        min_expected_layers = 4 if scale == 1 else 10
        finish_tenant_creation(env, tenant_id, timeline_id, min_expected_layers)

    tenant_layers = count_layers_per_tenant(env.pageserver, map(lambda x: x[0], timelines))
    (total_on_disk, _, _) = poor_mans_du(env, map(lambda x: x[0], timelines), env.pageserver, True)

    response = env.pageserver.http_client().disk_usage_eviction_run(
        {"evict_bytes": total_on_disk // 5, "eviction_order": order.config()}
    )
    log.info(f"{response}")

    after_tenant_layers = count_layers_per_tenant(env.pageserver, map(lambda x: x[0], timelines))

    ratios = []
    for i, ((tenant_id, _timeline_id), _scale) in enumerate(timelines):
        # we expect the oldest to suffer most
        originally, after = tenant_layers[tenant_id], after_tenant_layers[tenant_id]
        log.info(f"{i + 1}th tenant went from {originally} -> {after}")
        ratio = after / originally
        ratios.append(ratio)

    assert (
        len(ratios) == 4
    ), "rest of the assertions expect 3 + 1 timelines, ratios, scales, all in order"
    log.info(f"{ratios}")

    if order == EvictionOrder.RELATIVE_ORDER_EQUAL:
        assert all([x for x in ratios if x < 1.0]), "all tenants lose layers"
    elif order == EvictionOrder.RELATIVE_ORDER_SPARE:
        # with different layer sizes and pg versions, there are different combinations
        assert len([x for x in ratios if x < 1.0]) >= 2, "require 2..4 tenants to lose layers"
        assert ratios[3] < 1.0, "largest tenant always loses layers"
    else:
        raise RuntimeError(f"unimplemented {order}")


def poor_mans_du(
    env: NeonEnv,
    timelines: Iterable[tuple[TenantId, TimelineId]],
    pageserver: NeonPageserver,
    verbose: bool = False,
) -> tuple[int, int, int]:
    """
    Disk usage, largest, smallest layer for layer files over the given (tenant, timeline) tuples;
    this could be done over layers endpoint just as well.
    """
    total_on_disk = 0
    largest_layer = 0
    smallest_layer = None
    for tenant_id, timeline_id in timelines:
        timeline_dir = pageserver.timeline_dir(tenant_id, timeline_id)
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
        env.pageserver,
        period="1s",
        max_usage_pct=90,
        min_avail_bytes=0,
        mock_behavior={
            "type": "Failure",
            "mocked_error": "EIO",
        },
        eviction_order=EvictionOrder.RELATIVE_ORDER_SPARE,
    )

    env.neon_env.pageserver.assert_log_contains(".*statvfs failed.*EIO")
    env.neon_env.pageserver.allowed_errors.append(".*statvfs failed.*EIO")


def test_statvfs_pressure_usage(eviction_env: EvictionEnv):
    """
    If statvfs data shows 100% usage, the eviction task will drive it down to
    the configured max_usage_pct.
    """
    env = eviction_env

    env.neon_env.pageserver.stop()

    # make it seem like we're at 100% utilization by setting total bytes to the used bytes
    total_size, _, _ = env.timelines_du(env.pageserver)
    blocksize = 512
    total_blocks = (total_size + (blocksize - 1)) // blocksize

    env.pageserver_start_with_disk_usage_eviction(
        env.pageserver,
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
        eviction_order=EvictionOrder.RELATIVE_ORDER_SPARE,
    )

    wait_until(
        lambda: env.neon_env.pageserver.assert_log_contains(".*disk usage pressure relieved")
    )

    def less_than_max_usage_pct():
        post_eviction_total_size, _, _ = env.timelines_du(env.pageserver)
        assert post_eviction_total_size < 0.33 * total_size, "we requested max 33% usage"

    wait_until(less_than_max_usage_pct, timeout=5)

    # Disk usage candidate collection only takes into account active tenants.
    # However, the statvfs call takes into account the entire tenants directory,
    # which includes tenants which haven't become active yet.
    #
    # After re-starting the pageserver, disk usage eviction may kick in *before*
    # both tenants have become active. Hence, the logic will try to satisfy the
    # disk usage requirements by evicting everything belonging to the active tenant,
    # and hence violating the tenant minimum resident size.
    env.neon_env.pageserver.allowed_errors.append(".*" + GLOBAL_LRU_LOG_LINE)


def test_statvfs_pressure_min_avail_bytes(eviction_env: EvictionEnv):
    """
    If statvfs data shows 100% usage, the eviction task will drive it down to
    at least the configured min_avail_bytes.
    """
    env = eviction_env

    env.neon_env.pageserver.stop()

    # make it seem like we're at 100% utilization by setting total bytes to the used bytes
    total_size, _, _ = env.timelines_du(env.pageserver)
    blocksize = 512
    total_blocks = (total_size + (blocksize - 1)) // blocksize

    min_avail_bytes = total_size // 3

    env.pageserver_start_with_disk_usage_eviction(
        env.pageserver,
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
        eviction_order=EvictionOrder.RELATIVE_ORDER_SPARE,
    )

    wait_until(
        lambda: env.neon_env.pageserver.assert_log_contains(".*disk usage pressure relieved"),
    )

    def more_than_min_avail_bytes_freed():
        post_eviction_total_size, _, _ = env.timelines_du(env.pageserver)
        assert (
            total_size - post_eviction_total_size >= min_avail_bytes
        ), f"we requested at least {min_avail_bytes} worth of free space"

    wait_until(more_than_min_avail_bytes_freed, timeout=5)


def test_secondary_mode_eviction(eviction_env_ha: EvictionEnv):
    env = eviction_env_ha

    tenant_ids = [t[0] for t in env.timelines]

    # Set up a situation where one pageserver _only_ has secondary locations on it,
    # so that when we release space we are sure it is via secondary locations.
    log.info("Setting up secondary locations...")
    ps_secondary = env.neon_env.pageservers[1]
    for tenant_id in tenant_ids:
        # Find where it is attached
        pageserver = env.neon_env.get_tenant_pageserver(tenant_id)
        pageserver.http_client().tenant_heatmap_upload(tenant_id)

        # Detach it
        pageserver.tenant_detach(tenant_id)

        # Create a secondary mode location for the tenant, all tenants on one pageserver that will only
        # contain secondary locations: this is the one where we will exercise disk usage eviction
        ps_secondary.tenant_location_configure(
            tenant_id,
            {
                "mode": "Secondary",
                "secondary_conf": {"warm": True},
                "tenant_conf": {},
            },
        )
        readback_conf = ps_secondary.read_tenant_location_conf(tenant_id)
        log.info(f"Read back conf: {readback_conf}")

        # Request secondary location to download all layers that the attached location indicated
        # in its heatmap
        ps_secondary.http_client().tenant_secondary_download(tenant_id)

    total_size, _, _ = env.timelines_du(ps_secondary)
    evict_bytes = total_size // 3

    response = ps_secondary.http_client().disk_usage_eviction_run({"evict_bytes": evict_bytes})
    log.info(f"{response}")

    post_eviction_total_size, _, _ = env.timelines_du(ps_secondary)

    assert (
        total_size - post_eviction_total_size >= evict_bytes
    ), "we requested at least evict_bytes worth of free space"
