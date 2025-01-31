from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass

import pytest
from fixtures.common_types import TenantId
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.http import TenantConfig
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.utils import wait_until


@pytest.fixture
def positive_env(neon_env_builder: NeonEnvBuilder) -> NeonEnv:
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(
        [
            # eviction might be the first one after an attach to access the layers
            ".*unexpectedly on-demand downloading remote layer .* for task kind Eviction",
        ]
    )
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)
    return env


@dataclass
class NegativeTests:
    neon_env: NeonEnv
    tenant_id: TenantId
    config_pre_detach: TenantConfig


@pytest.fixture
def negative_env(neon_env_builder: NeonEnvBuilder) -> Generator[NegativeTests, None, None]:
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    ps_http = env.pageserver.http_client()
    (tenant_id, _) = env.create_tenant()
    assert ps_http.tenant_config(tenant_id).tenant_specific_overrides == {}
    config_pre_detach = ps_http.tenant_config(tenant_id)
    assert tenant_id in [TenantId(t["id"]) for t in ps_http.tenant_list()]
    ps_http.tenant_detach(tenant_id)
    assert tenant_id not in [TenantId(t["id"]) for t in ps_http.tenant_list()]

    yield NegativeTests(env, tenant_id, config_pre_detach)

    assert tenant_id not in [
        TenantId(t["id"]) for t in ps_http.tenant_list()
    ], "tenant should not be attached after negative test"

    env.pageserver.allowed_errors.extend(
        [
            # This fixture is for tests that will intentionally generate 400 responses
            ".*Error processing HTTP request: Bad request",
        ]
    )

    wait_until(
        lambda: env.pageserver.assert_log_contains(".*Error processing HTTP request: Bad request"),
    )


def test_null_body(negative_env: NegativeTests):
    """
    If we send `null` in the body, the request should be rejected with status 400.
    """
    env = negative_env.neon_env
    tenant_id = negative_env.tenant_id
    ps_http = env.pageserver.http_client()

    res = ps_http.put(
        f"{ps_http.base_url}/v1/tenant/{tenant_id}/location_config",
        data=b"null",
        headers={"Content-Type": "application/json"},
    )
    assert res.status_code == 400


def test_null_config(negative_env: NegativeTests):
    """
    If the `config` field is `null`, the request should be rejected with status 400.
    """

    env = negative_env.neon_env
    tenant_id = negative_env.tenant_id
    ps_http = env.pageserver.http_client()

    res = ps_http.put(
        f"{ps_http.base_url}/v1/tenant/{tenant_id}/location_config",
        json={"mode": "AttachedSingle", "generation": 1, "tenant_conf": None},
        headers={"Content-Type": "application/json"},
    )
    assert res.status_code == 400


@pytest.mark.parametrize("content_type", [None, "application/json"])
def test_empty_config(positive_env: NeonEnv, content_type: str | None):
    """
    When the 'config' body attribute is omitted, the request should be accepted
    and the tenant should use the default configuration
    """
    env = positive_env
    ps_http = env.pageserver.http_client()
    (tenant_id, _) = env.create_tenant()
    assert ps_http.tenant_config(tenant_id).tenant_specific_overrides == {}
    config_pre_detach = ps_http.tenant_config(tenant_id)
    assert tenant_id in [TenantId(t["id"]) for t in ps_http.tenant_list()]
    ps_http.tenant_detach(tenant_id)
    assert tenant_id not in [TenantId(t["id"]) for t in ps_http.tenant_list()]

    ps_http.put(
        f"{ps_http.base_url}/v1/tenant/{tenant_id}/location_config",
        json={
            "mode": "AttachedSingle",
            "generation": env.storage_controller.attach_hook_issue(tenant_id, env.pageserver.id),
            "tenant_conf": {},
        },
        headers=None if content_type else {"Content-Type": "application/json"},
    ).raise_for_status()

    assert ps_http.tenant_config(tenant_id).tenant_specific_overrides == {}
    assert ps_http.tenant_config(tenant_id).effective_config == config_pre_detach.effective_config


def test_fully_custom_config(positive_env: NeonEnv):
    """
    If we send a valid config in the body, the request should be accepted and the config should be applied.
    """
    env = positive_env

    fully_custom_config = {
        "compaction_period": "1h",
        "compaction_threshold": 13,
        "compaction_upper_limit": 100,
        "l0_flush_delay_threshold": 25,
        "l0_flush_stall_threshold": 42,
        "l0_flush_wait_upload": True,
        "compaction_target_size": 1048576,
        "checkpoint_distance": 10000,
        "checkpoint_timeout": "13m",
        "compaction_algorithm": {
            "kind": "tiered",
        },
        "eviction_policy": {
            "kind": "LayerAccessThreshold",
            "period": "20s",
            "threshold": "23h",
        },
        "evictions_low_residence_duration_metric_threshold": "2days",
        "gc_horizon": 23 * (1024 * 1024),
        "gc_period": "2h 13m",
        "heatmap_period": "10m",
        "image_creation_threshold": 7,
        "pitr_interval": "1m",
        "lagging_wal_timeout": "23m",
        "lazy_slru_download": True,
        "max_lsn_wal_lag": 230000,
        "min_resident_size_override": 23,
        "timeline_get_throttle": {
            "task_kinds": ["PageRequestHandler"],
            "initial": 0,
            "refill_interval": "1s",
            "refill_amount": 1000,
            "max": 1000,
        },
        "walreceiver_connect_timeout": "13m",
        "image_layer_creation_check_threshold": 1,
        "lsn_lease_length": "1m",
        "lsn_lease_length_for_ts": "5s",
        "timeline_offloading": True,
        "wal_receiver_protocol_override": {
            "type": "interpreted",
            "args": {"format": "bincode", "compression": {"zstd": {"level": 1}}},
        },
        "rel_size_v2_enabled": True,
        "gc_compaction_enabled": True,
        "gc_compaction_initial_threshold_kb": 1024000,
        "gc_compaction_ratio_percent": 200,
    }

    vps_http = env.storage_controller.pageserver_api()

    initial_tenant_config = vps_http.tenant_config(env.initial_tenant)
    assert [
        (key, val)
        for key, val in initial_tenant_config.tenant_specific_overrides.items()
        if val is not None
    ] == []
    assert set(initial_tenant_config.effective_config.keys()) == set(
        fully_custom_config.keys()
    ), "ensure we cover all config options"

    (tenant_id, _) = env.create_tenant()
    vps_http.set_tenant_config(tenant_id, fully_custom_config)
    our_tenant_config = vps_http.tenant_config(tenant_id)
    assert our_tenant_config.tenant_specific_overrides == fully_custom_config
    assert set(our_tenant_config.effective_config.keys()) == set(
        fully_custom_config.keys()
    ), "ensure we cover all config options"
    assert (
        {
            k: initial_tenant_config.effective_config[k] != our_tenant_config.effective_config[k]
            for k in fully_custom_config.keys()
        }
        == {k: True for k in fully_custom_config.keys()}
    ), "ensure our custom config has different values than the default config for all config options, so we know we overrode everything"

    env.pageserver.tenant_detach(tenant_id)
    env.pageserver.tenant_attach(tenant_id, config=fully_custom_config)

    assert vps_http.tenant_config(tenant_id).tenant_specific_overrides == fully_custom_config
    assert set(vps_http.tenant_config(tenant_id).effective_config.keys()) == set(
        fully_custom_config.keys()
    ), "ensure we cover all config options"
