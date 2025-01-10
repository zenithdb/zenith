from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from fixtures.common_types import Lsn
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pageserver.utils import assert_tenant_state, wait_for_upload
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.utils import run_only_on_default_postgres, wait_until
from fixtures.workload import Workload

if TYPE_CHECKING:
    from typing import Any


def test_tenant_config(neon_env_builder: NeonEnvBuilder):
    """Test per tenant configuration"""

    def set_some_nondefault_global_config(ps_cfg: dict[str, Any]):
        ps_cfg["page_cache_size"] = 444
        ps_cfg["wait_lsn_timeout"] = "111 s"

        tenant_config = ps_cfg.setdefault("tenant_config", {})
        tenant_config["checkpoint_distance"] = 10000
        tenant_config["compaction_target_size"] = 1048576
        tenant_config["evictions_low_residence_duration_metric_threshold"] = "2 days"
        tenant_config["eviction_policy"] = {
            "kind": "LayerAccessThreshold",
            "period": "20s",
            "threshold": "23 hours",
        }

    neon_env_builder.pageserver_config_override = set_some_nondefault_global_config

    env = neon_env_builder.init_start()
    # we configure eviction but no remote storage, there might be error lines
    env.pageserver.allowed_errors.append(".* no remote storage configured, cannot evict layers .*")
    http_client = env.pageserver.http_client()

    # Check that we raise on misspelled configs
    invalid_conf_key = "some_invalid_setting_name_blah_blah_123"
    try:
        env.create_tenant(
            conf={
                invalid_conf_key: "20000",
            }
        )
    except Exception as e:
        assert invalid_conf_key in str(e)
    else:
        raise AssertionError("Expected validation error")

    new_conf = {
        "checkpoint_distance": "20000",
        "gc_period": "30sec",
        "evictions_low_residence_duration_metric_threshold": "42s",
        "eviction_policy": json.dumps({"kind": "NoEviction"}),
    }
    tenant, _ = env.create_tenant(conf=new_conf)

    env.create_timeline("test_tenant_conf", tenant_id=tenant)
    env.endpoints.create_start("test_tenant_conf", "main", tenant)

    # check the configuration of the default tenant
    # it should match global configuration
    default_tenant_config = http_client.tenant_config(tenant_id=env.initial_tenant)
    assert (
        not default_tenant_config.tenant_specific_overrides
    ), "Should have no specific settings yet"
    effective_config = default_tenant_config.effective_config
    assert effective_config["checkpoint_distance"] == 10000
    assert effective_config["compaction_target_size"] == 1048576
    assert effective_config["compaction_period"] == "20s"
    assert effective_config["compaction_threshold"] == 10
    assert effective_config["gc_horizon"] == 67108864
    assert effective_config["gc_period"] == "1h"
    assert effective_config["image_creation_threshold"] == 3
    assert effective_config["pitr_interval"] == "7days"
    assert effective_config["evictions_low_residence_duration_metric_threshold"] == "2days"
    assert effective_config["eviction_policy"] == {
        "kind": "LayerAccessThreshold",
        "period": "20s",
        "threshold": "23h",
    }

    # check the configuration of the new tenant
    new_tenant_config = http_client.tenant_config(tenant_id=tenant)
    new_specific_config = new_tenant_config.tenant_specific_overrides
    assert new_specific_config["checkpoint_distance"] == 20000
    assert new_specific_config["gc_period"] == "30s"
    assert len(new_specific_config) == len(
        new_conf
    ), f"No more specific properties were expected, but got: {new_specific_config}"
    new_effective_config = new_tenant_config.effective_config
    assert (
        new_effective_config["checkpoint_distance"] == 20000
    ), "Specific 'checkpoint_distance' config should override the default value"
    assert (
        new_effective_config["gc_period"] == "30s"
    ), "Specific 'gc_period' config should override the default value"
    assert (
        new_effective_config["evictions_low_residence_duration_metric_threshold"] == "42s"
    ), "Should override default value"
    assert new_effective_config["eviction_policy"] == {
        "kind": "NoEviction"
    }, "Specific 'eviction_policy' config should override the default value"
    assert new_effective_config["compaction_target_size"] == 1048576
    assert new_effective_config["compaction_period"] == "20s"
    assert new_effective_config["compaction_threshold"] == 10
    assert new_effective_config["gc_horizon"] == 67108864
    assert new_effective_config["image_creation_threshold"] == 3
    assert new_effective_config["pitr_interval"] == "7days"

    # update the config and ensure that it has changed
    conf_update = {
        "checkpoint_distance": "15000",
        "gc_period": "80sec",
        "compaction_period": "80sec",
        "image_creation_threshold": "2",
        "evictions_low_residence_duration_metric_threshold": "23h",
        "eviction_policy": json.dumps(
            {"kind": "LayerAccessThreshold", "period": "80s", "threshold": "42h"}
        ),
        "max_lsn_wal_lag": "13000000",
    }
    env.config_tenant(tenant_id=tenant, conf=conf_update)

    updated_tenant_config = http_client.tenant_config(tenant_id=tenant)
    updated_specific_config = updated_tenant_config.tenant_specific_overrides
    assert updated_specific_config["checkpoint_distance"] == 15000
    assert updated_specific_config["gc_period"] == "1m 20s"
    assert updated_specific_config["compaction_period"] == "1m 20s"
    assert len(updated_specific_config) == len(
        conf_update
    ), f"No more specific properties were expected, but got: {updated_specific_config}"
    updated_effective_config = updated_tenant_config.effective_config
    assert (
        updated_effective_config["checkpoint_distance"] == 15000
    ), "Specific 'checkpoint_distance' config should override the default value"
    assert (
        updated_effective_config["gc_period"] == "1m 20s"
    ), "Specific 'gc_period' config should override the default value"
    assert (
        updated_effective_config["compaction_period"] == "1m 20s"
    ), "Specific 'compaction_period' config should override the default value"
    assert (
        updated_effective_config["evictions_low_residence_duration_metric_threshold"] == "23h"
    ), "Should override default value"
    assert updated_effective_config["eviction_policy"] == {
        "kind": "LayerAccessThreshold",
        "period": "1m 20s",
        "threshold": "1day 18h",
    }, "Specific 'eviction_policy' config should override the default value"
    assert updated_effective_config["compaction_target_size"] == 1048576
    assert updated_effective_config["compaction_threshold"] == 10
    assert updated_effective_config["gc_horizon"] == 67108864
    assert updated_effective_config["image_creation_threshold"] == 2
    assert updated_effective_config["pitr_interval"] == "7days"
    assert updated_effective_config["max_lsn_wal_lag"] == 13000000

    # restart the pageserver and ensure that the config is still correct
    env.pageserver.stop()
    env.pageserver.start()

    restarted_tenant_config = http_client.tenant_config(tenant_id=tenant)
    assert (
        restarted_tenant_config == updated_tenant_config
    ), "Updated config should not change after the restart"

    # update the config with very short config and make sure no trailing chars are left from previous config
    final_conf = {
        "pitr_interval": "1 min",
    }
    env.config_tenant(tenant_id=tenant, conf=final_conf)

    final_tenant_config = http_client.tenant_config(tenant_id=tenant)
    final_specific_config = final_tenant_config.tenant_specific_overrides
    assert final_specific_config["pitr_interval"] == "1m"
    assert len(final_specific_config) == len(
        final_conf
    ), f"No more specific properties were expected, but got: {final_specific_config}"
    final_effective_config = final_tenant_config.effective_config
    assert (
        final_effective_config["pitr_interval"] == "1m"
    ), "Specific 'pitr_interval' config should override the default value"
    assert final_effective_config["checkpoint_distance"] == 10000
    assert final_effective_config["compaction_target_size"] == 1048576
    assert final_effective_config["compaction_period"] == "20s"
    assert final_effective_config["compaction_threshold"] == 10
    assert final_effective_config["gc_horizon"] == 67108864
    assert final_effective_config["gc_period"] == "1h"
    assert final_effective_config["image_creation_threshold"] == 3
    assert final_effective_config["evictions_low_residence_duration_metric_threshold"] == "2days"
    assert final_effective_config["eviction_policy"] == {
        "kind": "LayerAccessThreshold",
        "period": "20s",
        "threshold": "23h",
    }
    assert final_effective_config["max_lsn_wal_lag"] == 1024 * 1024 * 1024

    # restart the pageserver and ensure that the config is still correct
    env.pageserver.stop()
    env.pageserver.start()

    restarted_final_tenant_config = http_client.tenant_config(tenant_id=tenant)
    assert (
        restarted_final_tenant_config == final_tenant_config
    ), "Updated config should not change after the restart"


def test_creating_tenant_conf_after_attach(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    # tenant is created with defaults, as in without config file
    (tenant_id, timeline_id) = env.create_tenant()
    config_path = env.pageserver.tenant_dir(tenant_id) / "config-v1"

    http_client = env.pageserver.http_client()

    detail = http_client.timeline_detail(tenant_id, timeline_id)
    last_record_lsn = Lsn(detail["last_record_lsn"])
    assert last_record_lsn.lsn_int != 0, "initdb must have executed"

    wait_for_upload(http_client, tenant_id, timeline_id, last_record_lsn)

    http_client.tenant_detach(tenant_id)

    assert not config_path.exists(), "detach did not remove config file"

    env.pageserver.tenant_attach(tenant_id)
    wait_until(lambda: assert_tenant_state(http_client, tenant_id, "Active"))

    env.config_tenant(tenant_id, {"gc_horizon": "1000000"})
    contents_first = config_path.read_text()
    env.config_tenant(tenant_id, {"gc_horizon": "0"})
    contents_later = config_path.read_text()

    # dont test applying the setting here, we have that another test case to show it
    # we just care about being able to create the file
    assert len(contents_first) > len(contents_later)


def test_live_reconfig_get_evictions_low_residence_duration_metric_threshold(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # disable compaction so that it will not download the layer for repartitioning
            "compaction_period": "0s"
        }
    )
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    (tenant_id, timeline_id) = env.initial_tenant, env.initial_timeline
    ps_http = env.pageserver.http_client()

    # When we evict/download layers, we will use this Workload to generate getpage requests
    # that touch some layers, as otherwise the pageserver doesn't report totally unused layers
    # as problems when they have short residence duration.
    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(100)

    def get_metric():
        metrics = ps_http.get_metrics()
        metric = metrics.query_one(
            "pageserver_evictions_with_low_residence_duration_total",
            {
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
            },
        )
        return metric

    default_value = ps_http.tenant_config(tenant_id).effective_config[
        "evictions_low_residence_duration_metric_threshold"
    ]
    metric = get_metric()
    assert int(metric.value) == 0, "metric is present with default value"

    assert default_value == "1day"

    ps_http.download_all_layers(tenant_id, timeline_id)
    workload.validate()
    ps_http.evict_all_layers(tenant_id, timeline_id)
    metric = get_metric()
    assert int(metric.value) > 0, "metric is updated"

    env.config_tenant(
        tenant_id, {"evictions_low_residence_duration_metric_threshold": default_value}
    )
    updated_metric = get_metric()
    assert int(updated_metric.value) == int(
        metric.value
    ), "metric is unchanged when setting same value"

    env.config_tenant(tenant_id, {"evictions_low_residence_duration_metric_threshold": "2day"})
    metric = get_metric()
    assert int(metric.labels["low_threshold_secs"]) == 2 * 24 * 60 * 60
    assert int(metric.value) == 0

    ps_http.download_all_layers(tenant_id, timeline_id)
    workload.validate()
    ps_http.evict_all_layers(tenant_id, timeline_id)
    metric = get_metric()
    assert int(metric.labels["low_threshold_secs"]) == 2 * 24 * 60 * 60
    assert int(metric.value) > 0

    env.config_tenant(tenant_id, {"evictions_low_residence_duration_metric_threshold": "2h"})
    metric = get_metric()
    assert int(metric.labels["low_threshold_secs"]) == 2 * 60 * 60
    assert int(metric.value) == 0, "value resets if label changes"

    ps_http.download_all_layers(tenant_id, timeline_id)
    workload.validate()
    ps_http.evict_all_layers(tenant_id, timeline_id)
    metric = get_metric()
    assert int(metric.labels["low_threshold_secs"]) == 2 * 60 * 60
    assert int(metric.value) > 0, "set a non-zero value for next step"

    env.config_tenant(tenant_id, {})
    metric = get_metric()
    assert int(metric.labels["low_threshold_secs"]) == 24 * 60 * 60, "label resets to default"
    assert int(metric.value) == 0, "value resets to default"


@run_only_on_default_postgres("Test does not start a compute")
@pytest.mark.parametrize("ps_managed_by", ["storcon", "cplane"])
def test_tenant_config_patch(neon_env_builder: NeonEnvBuilder, ps_managed_by: str):
    """
    Test tenant config patching (i.e. additive updates)

    The flow is different for storage controller and cplane managed pageserver.
    1. Storcon managed: /v1/tenant/config request lands on storcon, which generates
    location_config calls containing the update to the pageserver
    2. Cplane managed: /v1/tenant/config is called directly on the pageserver
    """

    def assert_tenant_conf_semantically_equal(lhs, rhs):
        """
        Storcon returns None for fields that are not set while the pageserver does not.
        Compare two tenant's config overrides semantically, by dropping the None values.
        """
        lhs = {k: v for k, v in lhs.items() if v is not None}
        rhs = {k: v for k, v in rhs.items() if v is not None}

        assert lhs == rhs

    env = neon_env_builder.init_start()

    if ps_managed_by == "storcon":
        api = env.storage_controller.pageserver_api()
    elif ps_managed_by == "cplane":
        # Disallow storcon from sending location_configs to the pageserver.
        # These would overwrite the manually set tenant configs.
        env.storage_controller.reconcile_until_idle()
        env.storage_controller.tenant_policy_update(env.initial_tenant, {"scheduling": "Stop"})
        env.storage_controller.allowed_errors.append(".*Scheduling is disabled by policy Stop.*")

        api = env.pageserver.http_client()
    else:
        raise Exception(f"Unexpected value of ps_managed_by param: {ps_managed_by}")

    crnt_tenant_conf = api.tenant_config(env.initial_tenant).tenant_specific_overrides

    patch: dict[str, Any | None] = {
        "gc_period": "3h",
        "wal_receiver_protocol_override": {
            "type": "interpreted",
            "args": {"format": "bincode", "compression": {"zstd": {"level": 1}}},
        },
    }
    api.patch_tenant_config(env.initial_tenant, patch)
    tenant_conf_after_patch = api.tenant_config(env.initial_tenant).tenant_specific_overrides
    if ps_managed_by == "storcon":
        # Check that the config was propagated to the PS.
        overrides_on_ps = (
            env.pageserver.http_client().tenant_config(env.initial_tenant).tenant_specific_overrides
        )
        assert_tenant_conf_semantically_equal(overrides_on_ps, tenant_conf_after_patch)
    assert_tenant_conf_semantically_equal(tenant_conf_after_patch, crnt_tenant_conf | patch)
    crnt_tenant_conf = tenant_conf_after_patch

    patch = {"gc_period": "5h", "wal_receiver_protocol_override": None}
    api.patch_tenant_config(env.initial_tenant, patch)
    tenant_conf_after_patch = api.tenant_config(env.initial_tenant).tenant_specific_overrides
    if ps_managed_by == "storcon":
        overrides_on_ps = (
            env.pageserver.http_client().tenant_config(env.initial_tenant).tenant_specific_overrides
        )
        assert_tenant_conf_semantically_equal(overrides_on_ps, tenant_conf_after_patch)
    assert_tenant_conf_semantically_equal(tenant_conf_after_patch, crnt_tenant_conf | patch)
    crnt_tenant_conf = tenant_conf_after_patch

    put = {"pitr_interval": "1m 1s"}
    api.set_tenant_config(env.initial_tenant, put)
    tenant_conf_after_put = api.tenant_config(env.initial_tenant).tenant_specific_overrides
    if ps_managed_by == "storcon":
        overrides_on_ps = (
            env.pageserver.http_client().tenant_config(env.initial_tenant).tenant_specific_overrides
        )
        assert_tenant_conf_semantically_equal(overrides_on_ps, tenant_conf_after_put)
    assert_tenant_conf_semantically_equal(tenant_conf_after_put, put)
    crnt_tenant_conf = tenant_conf_after_put
