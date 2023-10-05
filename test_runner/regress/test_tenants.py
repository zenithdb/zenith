import os
import shutil
import time
from contextlib import closing
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import List

import pytest
from fixtures.log_helper import log
from fixtures.metrics import (
    PAGESERVER_GLOBAL_METRICS,
    PAGESERVER_PER_TENANT_METRICS,
    parse_metrics,
)
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.utils import timeline_delete_wait_completed
from fixtures.remote_storage import RemoteStorageKind, available_remote_storages
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import wait_until
from prometheus_client.samples import Sample


def test_tenant_creation_fails(neon_simple_env: NeonEnv):
    tenants_dir = neon_simple_env.pageserver.tenant_dir()
    initial_tenants = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.list_tenants().stdout.splitlines())
    )
    initial_tenant_dirs = [d for d in tenants_dir.iterdir()]

    neon_simple_env.pageserver.allowed_errors.extend(
        [
            ".*Failed to create directory structure for tenant .*, cleaning tmp data.*",
            ".*Failed to fsync removed temporary tenant directory .*",
        ]
    )

    pageserver_http = neon_simple_env.pageserver.http_client()
    pageserver_http.configure_failpoints(("tenant-creation-before-tmp-rename", "return"))
    with pytest.raises(Exception, match="tenant-creation-before-tmp-rename"):
        _ = neon_simple_env.neon_cli.create_tenant()

    new_tenants = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.list_tenants().stdout.splitlines())
    )
    assert initial_tenants == new_tenants, "should not create new tenants"

    new_tenant_dirs = [d for d in tenants_dir.iterdir()]
    assert (
        new_tenant_dirs == initial_tenant_dirs
    ), "pageserver should clean its temp tenant dirs on tenant creation failure"


def test_tenants_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    """Tests tenants with and without wal acceptors"""
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    env.neon_cli.create_timeline("test_tenants_normal_work", tenant_id=tenant_1)
    env.neon_cli.create_timeline("test_tenants_normal_work", tenant_id=tenant_2)

    endpoint_tenant1 = env.endpoints.create_start(
        "test_tenants_normal_work",
        tenant_id=tenant_1,
    )
    endpoint_tenant2 = env.endpoints.create_start(
        "test_tenants_normal_work",
        tenant_id=tenant_2,
    )

    for endpoint in [endpoint_tenant1, endpoint_tenant2]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)


def test_metrics_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.pageserver_config_override = "availability_zone='test_ps_az'"

    env = neon_env_builder.init_start()
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    timeline_1 = env.neon_cli.create_timeline("test_metrics_normal_work", tenant_id=tenant_1)
    timeline_2 = env.neon_cli.create_timeline("test_metrics_normal_work", tenant_id=tenant_2)

    endpoint_tenant1 = env.endpoints.create_start("test_metrics_normal_work", tenant_id=tenant_1)
    endpoint_tenant2 = env.endpoints.create_start("test_metrics_normal_work", tenant_id=tenant_2)

    for endpoint in [endpoint_tenant1, endpoint_tenant2]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)

    collected_metrics = {
        "pageserver": env.pageserver.http_client().get_metrics_str(),
    }
    for sk in env.safekeepers:
        collected_metrics[f"safekeeper{sk.id}"] = sk.http_client().get_metrics_str()

    for name in collected_metrics:
        basepath = os.path.join(neon_env_builder.repo_dir, f"{name}.metrics")

        with open(basepath, "w") as stdout_f:
            print(collected_metrics[name], file=stdout_f, flush=True)

    all_metrics = [parse_metrics(m, name) for name, m in collected_metrics.items()]
    ps_metrics = all_metrics[0]
    sk_metrics = all_metrics[1:]

    # Find all metrics among all safekeepers, accepts the same arguments as query_all()
    def query_all_safekeepers(name, filter):
        return list(
            chain.from_iterable(
                map(
                    lambda sk: sk.query_all(name, filter),
                    sk_metrics,
                )
            )
        )

    ttids = [
        {"tenant_id": str(tenant_1), "timeline_id": str(timeline_1)},
        {"tenant_id": str(tenant_2), "timeline_id": str(timeline_2)},
    ]

    # Test metrics per timeline
    for tt in ttids:
        log.info(f"Checking metrics for {tt}")

        ps_lsn = Lsn(int(ps_metrics.query_one("pageserver_last_record_lsn", filter=tt).value))
        sk_lsns = [
            Lsn(int(sk.query_one("safekeeper_commit_lsn", filter=tt).value)) for sk in sk_metrics
        ]

        log.info(f"ps_lsn: {ps_lsn}")
        log.info(f"sk_lsns: {sk_lsns}")

        assert ps_lsn <= max(sk_lsns)
        assert ps_lsn > Lsn(0)

    # Test common metrics
    for metrics in all_metrics:
        log.info(f"Checking common metrics for {metrics.name}")

        log.info(
            f"process_cpu_seconds_total: {metrics.query_one('process_cpu_seconds_total').value}"
        )
        log.info(f"process_threads: {int(metrics.query_one('process_threads').value)}")
        log.info(
            f"process_resident_memory_bytes (MB): {metrics.query_one('process_resident_memory_bytes').value / 1024 / 1024}"
        )
        log.info(
            f"process_virtual_memory_bytes (MB): {metrics.query_one('process_virtual_memory_bytes').value / 1024 / 1024}"
        )
        log.info(f"process_open_fds: {int(metrics.query_one('process_open_fds').value)}")
        log.info(f"process_max_fds: {int(metrics.query_one('process_max_fds').value)}")
        log.info(
            f"process_start_time_seconds (UTC): {datetime.fromtimestamp(metrics.query_one('process_start_time_seconds').value)}"
        )

    for io_direction in ["read", "write"]:
        # Querying all metrics for number of bytes read/written by pageserver in another AZ
        io_metrics = query_all_safekeepers(
            "safekeeper_pg_io_bytes_total",
            {
                "app_name": "pageserver",
                "client_az": "test_ps_az",
                "dir": io_direction,
                "same_az": "false",
            },
        )
        total_bytes = sum(int(metric.value) for metric in io_metrics)
        log.info(f"Pageserver {io_direction} bytes from another AZ: {total_bytes}")
        # We expect some bytes to be read/written, to make sure metrics are working
        assert total_bytes > 0

    # Test (a subset of) safekeeper global metrics
    for sk_m in sk_metrics:
        # Test that every safekeeper has read some bytes
        assert any(
            map(
                lambda x: x.value > 0,
                sk_m.query_all("safekeeper_pg_io_bytes_total", {"dir": "read"}),
            )
        ), f"{sk_m.name} has not read bytes"

        # Test that every safekeeper has written some bytes
        assert any(
            map(
                lambda x: x.value > 0,
                sk_m.query_all("safekeeper_pg_io_bytes_total", {"dir": "write"}),
            )
        ), f"{sk_m.name} has not written bytes"

    # Test (a subset of) pageserver global metrics
    for metric in PAGESERVER_GLOBAL_METRICS:
        if metric.startswith("pageserver_remote"):
            continue

        ps_samples = ps_metrics.query_all(metric, {})
        assert len(ps_samples) > 0, f"expected at least one sample for {metric}"
        for sample in ps_samples:
            labels = ",".join([f'{key}="{value}"' for key, value in sample.labels.items()])
            log.info(f"{sample.name}{{{labels}}} {sample.value}")

    # Test that we gather tenant create metric
    storage_operation_metrics = [
        "pageserver_storage_operations_seconds_global_bucket",
        "pageserver_storage_operations_seconds_global_sum",
        "pageserver_storage_operations_seconds_global_count",
    ]
    for metric in storage_operation_metrics:
        value = ps_metrics.query_all(metric, filter={"operation": "create tenant"})
        assert value


def test_pageserver_metrics_removed_after_detach(neon_env_builder: NeonEnvBuilder):
    """Tests that when a tenant is detached, the tenant specific metrics are not left behind"""

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    env.neon_cli.create_timeline("test_metrics_removed_after_detach", tenant_id=tenant_1)
    env.neon_cli.create_timeline("test_metrics_removed_after_detach", tenant_id=tenant_2)

    endpoint_tenant1 = env.endpoints.create_start(
        "test_metrics_removed_after_detach", tenant_id=tenant_1
    )
    endpoint_tenant2 = env.endpoints.create_start(
        "test_metrics_removed_after_detach", tenant_id=tenant_2
    )

    for endpoint in [endpoint_tenant1, endpoint_tenant2]:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)
        endpoint.stop()

    def get_ps_metric_samples_for_tenant(tenant_id: TenantId) -> List[Sample]:
        ps_metrics = env.pageserver.http_client().get_metrics()
        samples = []
        for metric_name in ps_metrics.metrics:
            for sample in ps_metrics.query_all(
                name=metric_name, filter={"tenant_id": str(tenant_id)}
            ):
                samples.append(sample)
        return samples

    for tenant in [tenant_1, tenant_2]:
        pre_detach_samples = set([x.name for x in get_ps_metric_samples_for_tenant(tenant)])
        expected = set(PAGESERVER_PER_TENANT_METRICS)
        assert pre_detach_samples == expected

        env.pageserver.http_client().tenant_detach(tenant)

        post_detach_samples = set([x.name for x in get_ps_metric_samples_for_tenant(tenant)])
        assert post_detach_samples == set()


# Check that empty tenants work with or without the remote storage
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_pageserver_with_empty_tenants(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.append(
        ".*marking .* as locally complete, while it doesn't exist in remote index.*"
    )
    env.pageserver.allowed_errors.append(".*load failed.*list timelines directory.*")

    client = env.pageserver.http_client()

    tenant_with_empty_timelines = TenantId.generate()
    client.tenant_create(tenant_with_empty_timelines)
    temp_timelines = client.timeline_list(tenant_with_empty_timelines)
    for temp_timeline in temp_timelines:
        timeline_delete_wait_completed(
            client, tenant_with_empty_timelines, TimelineId(temp_timeline["timeline_id"])
        )

    files_in_timelines_dir = sum(
        1 for _p in Path.iterdir(env.pageserver.timeline_dir(tenant_with_empty_timelines))
    )
    assert (
        files_in_timelines_dir == 0
    ), f"Tenant {tenant_with_empty_timelines} should have an empty timelines/ directory"

    # Trigger timeline re-initialization after pageserver restart
    env.endpoints.stop_all()
    env.pageserver.stop()

    tenant_without_timelines_dir = env.initial_tenant
    shutil.rmtree(env.pageserver.timeline_dir(tenant_without_timelines_dir))

    env.pageserver.start()

    client = env.pageserver.http_client()

    def not_loading():
        tenants = client.tenant_list()
        assert len(tenants) == 2
        assert all(t["state"]["slug"] != "Loading" for t in tenants)

    wait_until(10, 0.2, not_loading)

    tenants = client.tenant_list()

    [broken_tenant] = [t for t in tenants if t["id"] == str(tenant_without_timelines_dir)]
    assert (
        broken_tenant["state"]["slug"] == "Broken"
    ), f"Tenant {tenant_without_timelines_dir} without timelines dir should be broken"

    broken_tenant_status = client.tenant_status(tenant_without_timelines_dir)
    assert (
        broken_tenant_status["state"]["slug"] == "Broken"
    ), f"Tenant {tenant_without_timelines_dir} without timelines dir should be broken"

    assert env.pageserver.log_contains(".*load failed, setting tenant state to Broken:.*")

    [loaded_tenant] = [t for t in tenants if t["id"] == str(tenant_with_empty_timelines)]
    assert (
        loaded_tenant["state"]["slug"] == "Active"
    ), "Tenant {tenant_with_empty_timelines} with empty timelines dir should be active and ready for timeline creation"

    loaded_tenant_status = client.tenant_status(tenant_with_empty_timelines)
    assert (
        loaded_tenant_status["state"]["slug"] == "Active"
    ), f"Tenant {tenant_with_empty_timelines} without timelines dir should be active"

    time.sleep(1)  # to allow metrics propagation

    ps_metrics = client.get_metrics()
    broken_tenants_metric_filter = {
        "tenant_id": str(tenant_without_timelines_dir),
    }
    active_tenants_metric_filter = {
        "state": "Active",
    }

    tenant_active_count = int(
        ps_metrics.query_one(
            "pageserver_tenant_states_count", filter=active_tenants_metric_filter
        ).value
    )

    assert (
        tenant_active_count == 1
    ), f"Tenant {tenant_with_empty_timelines} should have metric as active"

    tenant_broken_count = int(
        ps_metrics.query_one(
            "pageserver_broken_tenants_count", filter=broken_tenants_metric_filter
        ).value
    )

    assert (
        tenant_broken_count == 1
    ), f"Tenant {tenant_without_timelines_dir} should have metric as broken"
