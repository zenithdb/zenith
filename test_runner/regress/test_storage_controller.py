from __future__ import annotations

import concurrent.futures
import json
import threading
import time
from collections import defaultdict
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING

import fixtures.utils
import pytest
from fixtures.auth_tokens import TokenScope
from fixtures.common_types import TenantId, TenantShardId, TimelineId
from fixtures.compute_reconfigure import ComputeReconfigure
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    DEFAULT_AZ_ID,
    LogCursor,
    NeonEnv,
    NeonEnvBuilder,
    NeonPageserver,
    PageserverAvailability,
    PageserverSchedulingPolicy,
    PgBin,
    StorageControllerApiException,
    StorageControllerLeadershipStatus,
    last_flush_lsn_upload,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.pageserver.utils import (
    assert_prefix_empty,
    assert_prefix_not_empty,
    enable_remote_storage_versioning,
    list_prefix,
    many_small_layers_tenant_config,
    remote_storage_delete_key,
    timeline_delete_wait_completed,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import RemoteStorageKind, s3_storage
from fixtures.storage_controller_proxy import StorageControllerProxy
from fixtures.utils import (
    run_only_on_default_postgres,
    run_pg_bench_small,
    subprocess_capture,
    wait_until,
)
from fixtures.workload import Workload
from mypy_boto3_s3.type_defs import (
    ObjectTypeDef,
)
from pytest_httpserver import HTTPServer
from urllib3 import Retry
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

if TYPE_CHECKING:
    from typing import Any

    from fixtures.httpserver import ListenAddress


def get_node_shard_counts(env: NeonEnv, tenant_ids):
    counts: defaultdict[int, int] = defaultdict(int)
    for tid in tenant_ids:
        for shard in env.storage_controller.locate(tid):
            counts[shard["node_id"]] += 1
    return counts


@pytest.mark.parametrize(**fixtures.utils.allpairs_versions())
def test_storage_controller_smoke(neon_env_builder: NeonEnvBuilder, combination):
    """
    Test the basic lifecycle of a storage controller:
    - Restarting
    - Restarting a pageserver
    - Creating and deleting tenants and timelines
    - Marking a pageserver offline
    """

    neon_env_builder.num_pageservers = 3
    env = neon_env_builder.init_configs()

    # Start services by hand so that we can skip a pageserver (this will start + register later)
    env.broker.start()
    env.storage_controller.start()
    env.pageservers[0].start()
    env.pageservers[1].start()
    for sk in env.safekeepers:
        sk.start()

    # The pageservers we started should have registered with the sharding service on startup
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2
    assert set(n["id"] for n in nodes) == {env.pageservers[0].id, env.pageservers[1].id}

    # Starting an additional pageserver should register successfully
    env.pageservers[2].start()
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 3
    assert set(n["id"] for n in nodes) == {ps.id for ps in env.pageservers}

    # Use a multiple of pageservers to get nice even number of shards on each one
    tenant_shard_count = len(env.pageservers) * 4
    tenant_count = len(env.pageservers) * 2
    shards_per_tenant = tenant_shard_count // tenant_count
    tenant_ids = set(TenantId.generate() for i in range(0, tenant_count))

    # Creating several tenants should spread out across the pageservers
    for tid in tenant_ids:
        env.create_tenant(tid, shard_count=shards_per_tenant)

    # Tenant listing API should work
    listed_tenants = env.storage_controller.tenant_list()
    log.info(f"listed_tenants: {listed_tenants}")
    assert set(t["tenant_id"] for t in listed_tenants) == set(str(t) for t in tenant_ids)
    paged = env.storage_controller.tenant_list(limit=2, start_after=listed_tenants[0]["tenant_id"])
    assert len(paged) == 2
    assert paged[0] == listed_tenants[1]
    assert paged[1] == listed_tenants[2]
    paged = env.storage_controller.tenant_list(
        limit=1000, start_after="ffffffffffffffffffffffffffffffff"
    )
    assert paged == []

    # Validate high level metrics
    assert (
        env.storage_controller.get_metric_value("storage_controller_tenant_shards")
        == len(tenant_ids) * shards_per_tenant
    )
    assert env.storage_controller.get_metric_value("storage_controller_pageserver_nodes") == len(
        env.storage_controller.node_list()
    )

    # Repeating a creation should be idempotent (we are just testing it doesn't return an error)
    env.storage_controller.tenant_create(
        tenant_id=next(iter(tenant_ids)), shard_count=shards_per_tenant
    )

    for node_id, count in get_node_shard_counts(env, tenant_ids).items():
        # we used a multiple of pagservers for the total shard count,
        # so expect equal number on all pageservers
        assert count == tenant_shard_count / len(
            env.pageservers
        ), f"Node {node_id} has bad count {count}"

    # Creating and deleting timelines should work, using identical API to pageserver
    timeline_crud_tenant = next(iter(tenant_ids))
    timeline_id = TimelineId.generate()
    env.storage_controller.pageserver_api().timeline_create(
        pg_version=PgVersion.NOT_SET, tenant_id=timeline_crud_tenant, new_timeline_id=timeline_id
    )
    timelines = env.storage_controller.pageserver_api().timeline_list(timeline_crud_tenant)
    assert len(timelines) == 2
    assert timeline_id in set(TimelineId(t["timeline_id"]) for t in timelines)
    #    virtual_ps_http.timeline_delete(tenant_id=timeline_crud_tenant, timeline_id=timeline_id)
    timeline_delete_wait_completed(
        env.storage_controller.pageserver_api(), timeline_crud_tenant, timeline_id
    )
    timelines = env.storage_controller.pageserver_api().timeline_list(timeline_crud_tenant)
    assert len(timelines) == 1
    assert timeline_id not in set(TimelineId(t["timeline_id"]) for t in timelines)

    # Marking a pageserver offline should migrate tenants away from it.
    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Offline"})

    def node_evacuated(node_id: int) -> None:
        counts = get_node_shard_counts(env, tenant_ids)
        assert counts[node_id] == 0

    wait_until(lambda: node_evacuated(env.pageservers[0].id))

    # Let all the reconciliations after marking the node offline complete
    env.storage_controller.reconcile_until_idle()

    # Marking pageserver active should not migrate anything to it
    # immediately
    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Active"})
    time.sleep(1)
    assert get_node_shard_counts(env, tenant_ids)[env.pageservers[0].id] == 0

    # Restarting a pageserver should not detach any tenants (i.e. /re-attach works)
    before_restart = env.pageservers[1].http_client().tenant_list_locations()
    env.pageservers[1].stop()
    env.pageservers[1].start()
    after_restart = env.pageservers[1].http_client().tenant_list_locations()
    assert len(after_restart) == len(before_restart)

    # Locations should be the same before & after restart, apart from generations
    for _shard_id, tenant in after_restart["tenant_shards"]:
        del tenant["generation"]
    for _shard_id, tenant in before_restart["tenant_shards"]:
        del tenant["generation"]
    assert before_restart == after_restart

    # Delete all the tenants
    for tid in tenant_ids:
        env.storage_controller.pageserver_api().tenant_delete(tid)

    env.storage_controller.consistency_check()

    # Set a scheduling policy on one node, create all the tenants, observe
    # that the scheduling policy is respected.
    env.storage_controller.node_configure(env.pageservers[1].id, {"scheduling": "Draining"})

    # Create some fresh tenants
    tenant_ids = set(TenantId.generate() for i in range(0, tenant_count))
    for tid in tenant_ids:
        env.create_tenant(tid, shard_count=shards_per_tenant)

    counts = get_node_shard_counts(env, tenant_ids)
    # Nothing should have been scheduled on the node in Draining
    assert counts[env.pageservers[1].id] == 0
    assert counts[env.pageservers[0].id] == tenant_shard_count // 2
    assert counts[env.pageservers[2].id] == tenant_shard_count // 2

    env.storage_controller.consistency_check()


def test_node_status_after_restart(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    # Initially we have two online pageservers
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2

    env.pageservers[1].stop()
    env.storage_controller.allowed_errors.extend([".*Could not scan node"])

    env.storage_controller.stop()
    env.storage_controller.start()

    def is_ready():
        assert env.storage_controller.ready() is True

    wait_until(is_ready)

    # We loaded nodes from database on restart
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2

    # We should still be able to create a tenant, because the pageserver which is still online
    # should have had its availabilty state set to Active.
    env.storage_controller.tenant_create(TenantId.generate())

    env.storage_controller.consistency_check()


def test_storage_controller_passthrough(
    neon_env_builder: NeonEnvBuilder,
):
    """
    For simple timeline/tenant GET APIs that don't require coordination across
    shards, the sharding service implements a proxy to shard zero.  This test
    calls those APIs.
    """
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    # We will talk to storage controller as if it was a pageserver, using the pageserver
    # HTTP client
    client = PageserverHttpClient(env.storage_controller_port, lambda: True)
    timelines = client.timeline_list(tenant_id=env.initial_tenant)
    assert len(timelines) == 1

    status = client.tenant_status(env.initial_tenant)
    assert TenantId(status["id"]) == env.initial_tenant
    assert set(TimelineId(t) for t in status["timelines"]) == {
        env.initial_timeline,
    }
    assert status["state"]["slug"] == "Active"

    (synthetic_size, size_inputs) = client.tenant_size_and_modelinputs(env.initial_tenant)
    assert synthetic_size > 0
    assert "segments" in size_inputs

    env.storage_controller.consistency_check()


def test_storage_controller_restart(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    tenant_a = env.initial_tenant
    tenant_b = TenantId.generate()
    env.storage_controller.tenant_create(tenant_b)
    env.pageserver.tenant_detach(tenant_a)

    # TODO: extend this test to use multiple pageservers, and check that locations don't move around
    # on restart.

    # Storage controller restart
    env.storage_controller.stop()
    env.storage_controller.start()

    observed = set(TenantId(tenant["id"]) for tenant in env.pageserver.http_client().tenant_list())

    # Tenant A should still be attached
    assert tenant_a not in observed

    # Tenant B should remain detached
    assert tenant_b in observed

    # Pageserver restart
    env.pageserver.stop()
    env.pageserver.start()

    # Same assertions as above: restarting either service should not perturb things
    observed = set(TenantId(tenant["id"]) for tenant in env.pageserver.http_client().tenant_list())
    assert tenant_a not in observed
    assert tenant_b in observed

    env.storage_controller.consistency_check()


def prepare_onboarding_env(
    neon_env_builder: NeonEnvBuilder,
) -> tuple[NeonEnv, NeonPageserver, TenantId, int]:
    """
    For tests that do onboarding of a tenant to the storage controller, a small dance to
    set up one pageserver that won't be managed by the storage controller and create
    a tenant there.
    """
    # One pageserver to simulate legacy environment, two to be managed by storage controller
    neon_env_builder.num_pageservers = 3

    # Enable tests to use methods that require real S3 API
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    # Start services by hand so that we can skip registration on one of the pageservers
    env = neon_env_builder.init_configs()
    env.broker.start()
    env.storage_controller.start()

    # This is the pageserver where we'll initially create the tenant.  Run it in emergency
    # mode so that it doesn't talk to storage controller, and do not register it.
    env.pageservers[0].allowed_errors.append(".*Emergency mode!.*")
    env.pageservers[0].patch_config_toml_nonrecursive(
        {
            "control_plane_emergency_mode": True,
        }
    )
    env.pageservers[0].start()
    origin_ps = env.pageservers[0]

    # These are the pageservers managed by the sharding service, where the tenant
    # will be attached after onboarding
    env.pageservers[1].start()
    env.pageservers[2].start()

    for sk in env.safekeepers:
        sk.start()

    # Create a tenant directly via pageserver HTTP API, skipping the storage controller
    tenant_id = TenantId.generate()
    generation = 123
    origin_ps.tenant_create(tenant_id, generation=generation)

    origin_ps.http_client().timeline_create(PgVersion.NOT_SET, tenant_id, TimelineId.generate())

    return (env, origin_ps, tenant_id, generation)


@pytest.mark.parametrize("warm_up", [True, False])
def test_storage_controller_onboarding(neon_env_builder: NeonEnvBuilder, warm_up: bool):
    """
    We onboard tenants to the sharding service by treating it as a 'virtual pageserver'
    which provides the /location_config API.  This is similar to creating a tenant,
    but imports the generation number.
    """

    neon_env_builder.num_azs = 3
    env, origin_ps, tenant_id, generation = prepare_onboarding_env(neon_env_builder)

    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    # As if doing a live migration, first configure origin into stale mode
    r = origin_ps.http_client().tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedStale",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 1

    if warm_up:
        origin_ps.http_client().tenant_heatmap_upload(tenant_id)

        # We expect to be called via live migration code, which may try to configure the tenant into secondary
        # mode before attaching it.
        virtual_ps_http.tenant_location_conf(
            tenant_id,
            {
                "mode": "Secondary",
                "secondary_conf": {"warm": True},
                "tenant_conf": {},
                "generation": None,
            },
        )

        virtual_ps_http.tenant_secondary_download(tenant_id)
        warm_up_ps = env.storage_controller.tenant_describe(tenant_id)["shards"][0][
            "node_secondary"
        ][0]

        # Check that the secondary's scheduling is stable
        assert env.storage_controller.reconcile_all() == 0

    # Call into storage controller to onboard the tenant
    generation += 1
    r = virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedMulti",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 1

    describe = env.storage_controller.tenant_describe(tenant_id)["shards"][0]
    dest_ps_id = describe["node_attached"]
    dest_ps = env.get_pageserver(dest_ps_id)
    if warm_up:
        # The storage controller should have attached the tenant to the same placce
        # it had a secondary location, otherwise there was no point warming it up
        assert dest_ps_id == warm_up_ps

        # It should have been given a new secondary location as well
        assert len(describe["node_secondary"]) == 1
        assert describe["node_secondary"][0] != warm_up_ps

    # As if doing a live migration, detach the original pageserver
    origin_ps.http_client().tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )

    # As if doing a live migration, call into the storage controller to
    # set it to AttachedSingle: this is a no-op, but we test it because the
    # cloud control plane may call this for symmetry with live migration to
    # an individual pageserver
    r = virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 1

    # Check that onboarding did not result in an unstable scheduling state
    assert env.storage_controller.reconcile_all() == 0

    # We should see the tenant is now attached to the pageserver managed
    # by the sharding service
    origin_tenants = origin_ps.http_client().tenant_list()
    assert len(origin_tenants) == 0
    dest_tenants = dest_ps.http_client().tenant_list()
    assert len(dest_tenants) == 1
    assert TenantId(dest_tenants[0]["id"]) == tenant_id

    # sharding service advances generation by 1 when it first attaches.  We started
    # with a nonzero generation so this equality also proves that the generation
    # was properly carried over during onboarding.
    assert dest_tenants[0]["generation"] == generation + 1

    # The onboarded tenant should survive a restart of sharding service
    env.storage_controller.stop()
    env.storage_controller.start()

    # The onboarded tenant should surviev a restart of pageserver
    dest_ps.stop()
    dest_ps.start()

    # Having onboarded via /location_config, we should also be able to update the
    # TenantConf part of LocationConf, without inadvertently resetting the generation
    modified_tenant_conf = {"max_lsn_wal_lag": 1024 * 1024 * 1024 * 100}
    dest_tenant_before_conf_change = dest_ps.http_client().tenant_status(tenant_id)

    # The generation has moved on since we onboarded
    assert generation != dest_tenant_before_conf_change["generation"]

    r = virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": modified_tenant_conf,
            # This is intentionally a stale generation
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 1
    dest_tenant_after_conf_change = dest_ps.http_client().tenant_status(tenant_id)
    assert (
        dest_tenant_after_conf_change["generation"] == dest_tenant_before_conf_change["generation"]
    )
    dest_tenant_conf_after = dest_ps.http_client().tenant_config(tenant_id)

    # Storage controller auto-sets heatmap period, ignore it for the comparison
    del dest_tenant_conf_after.tenant_specific_overrides["heatmap_period"]
    assert dest_tenant_conf_after.tenant_specific_overrides == modified_tenant_conf

    env.storage_controller.consistency_check()


@run_only_on_default_postgres("this test doesn't start an endpoint")
def test_storage_controller_onboard_detached(neon_env_builder: NeonEnvBuilder):
    """
    Sometimes, the control plane wants to delete a tenant that wasn't attached to any pageserver,
    and also wasn't ever registered with the storage controller.

    It may do this by calling /location_conf in mode Detached and then calling the delete API
    as normal.
    """

    env, origin_ps, tenant_id, generation = prepare_onboarding_env(neon_env_builder)

    remote_prefix = "/".join(
        (
            "tenants",
            str(tenant_id),
        )
    )

    # Detach it from its original pageserver.
    origin_ps.http_client().tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )

    # Since we will later assert that remote data is gone, as a control also check it was ever there
    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=remote_prefix,
    )

    # Register with storage controller in Detached state
    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)
    generation += 1
    r = virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": generation,
        },
    )
    assert len(r["shards"]) == 0  # location_conf tells us there are no attached shards

    # Onboarding in Detached state shouldn't have attached it to any pageserver
    for ps in env.pageservers:
        assert ps.http_client().tenant_list() == []

    # Delete it via the storage controller
    virtual_ps_http.tenant_delete(tenant_id)

    # Check that we really deleted it
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=remote_prefix,
    )


def test_storage_controller_compute_hook(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
):
    """
    Test that the sharding service calls out to the configured HTTP endpoint on attachment changes
    """

    # We will run two pageserver to migrate and check that the storage controller sends notifications
    # when migrating.
    neon_env_builder.num_pageservers = 2
    (host, port) = httpserver_listen_address
    neon_env_builder.control_plane_compute_hook_api = f"http://{host}:{port}/notify"

    # Set up fake HTTP notify endpoint
    notifications = []

    handle_params = {"status": 200}

    def handler(request: Request):
        status = handle_params["status"]
        log.info(f"Notify request[{status}]: {request}")
        notifications.append(request.json)
        return Response(status=status)

    httpserver.expect_request("/notify", method="PUT").respond_with_handler(handler)

    # Start running
    env = neon_env_builder.init_start(initial_tenant_conf={"lsn_lease_length": "0s"})

    # Initial notification from tenant creation
    assert len(notifications) == 1
    expect: dict[str, list[dict[str, int]] | str | None | int] = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[0].id), "shard_number": 0}],
        "preferred_az": DEFAULT_AZ_ID,
    }
    assert notifications[0] == expect

    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Offline"})

    def node_evacuated(node_id: int) -> None:
        counts = get_node_shard_counts(env, [env.initial_tenant])
        assert counts[node_id] == 0

    wait_until(lambda: node_evacuated(env.pageservers[0].id))

    # Additional notification from migration
    log.info(f"notifications: {notifications}")
    expect = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[1].id), "shard_number": 0}],
        "preferred_az": DEFAULT_AZ_ID,
    }

    def received_migration_notification():
        assert len(notifications) == 2
        assert notifications[1] == expect

    wait_until(received_migration_notification)

    # When we restart, we should re-emit notifications for all tenants
    env.storage_controller.stop()
    env.storage_controller.start()

    def received_restart_notification():
        assert len(notifications) == 3
        assert notifications[2] == expect

    wait_until(received_restart_notification)

    # Splitting a tenant should cause its stripe size to become visible in the compute notification
    env.storage_controller.tenant_shard_split(env.initial_tenant, shard_count=2)
    expect = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": 32768,
        "shards": [
            {"node_id": int(env.pageservers[1].id), "shard_number": 0},
            {"node_id": int(env.pageservers[1].id), "shard_number": 1},
        ],
        "preferred_az": DEFAULT_AZ_ID,
    }

    def received_split_notification():
        assert len(notifications) == 4
        assert notifications[3] == expect

    wait_until(received_split_notification)

    # If the compute hook is unavailable, that should not block creating a tenant and
    # creating a timeline.  This simulates a control plane refusing to accept notifications
    handle_params["status"] = 423
    degraded_tenant_id = TenantId.generate()
    degraded_timeline_id = TimelineId.generate()
    env.storage_controller.tenant_create(degraded_tenant_id)
    env.storage_controller.pageserver_api().timeline_create(
        PgVersion.NOT_SET, degraded_tenant_id, degraded_timeline_id
    )

    # Ensure we hit the handler error path
    env.storage_controller.allowed_errors.append(
        ".*Failed to notify compute of attached pageserver.*tenant busy.*"
    )
    env.storage_controller.allowed_errors.append(".*Reconcile error.*tenant busy.*")
    assert notifications[-1] is not None
    assert notifications[-1]["tenant_id"] == str(degraded_tenant_id)

    env.storage_controller.consistency_check()


NOTIFY_BLOCKED_LOG = ".*Live migration blocked.*"
NOTIFY_FAILURE_LOGS = [
    ".*Failed to notify compute.*",
    ".*Reconcile error.*Cancelled",
    ".*Reconcile error.*Control plane tenant busy",
]


def test_storage_controller_stuck_compute_hook(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
):
    """
    Test the migration process's behavior when the compute hook does not enable it to proceed
    """

    neon_env_builder.num_pageservers = 2
    (host, port) = httpserver_listen_address
    neon_env_builder.control_plane_compute_hook_api = f"http://{host}:{port}/notify"

    handle_params = {"status": 200}

    notifications = []

    def handler(request: Request):
        status = handle_params["status"]
        log.info(f"Notify request[{status}]: {request}")
        notifications.append(request.json)
        return Response(status=status)

    httpserver.expect_request("/notify", method="PUT").respond_with_handler(handler)

    # Start running
    env = neon_env_builder.init_start(initial_tenant_conf={"lsn_lease_length": "0s"})

    # Initial notification from tenant creation
    assert len(notifications) == 1
    expect: dict[str, list[dict[str, int]] | str | None | int] = {
        "tenant_id": str(env.initial_tenant),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[0].id), "shard_number": 0}],
        "preferred_az": DEFAULT_AZ_ID,
    }
    assert notifications[0] == expect

    # Do a migration while the compute hook is returning 423 status
    tenant_id = env.initial_tenant
    origin_pageserver = env.get_tenant_pageserver(tenant_id)
    dest_ps_id = [p.id for p in env.pageservers if p.id != origin_pageserver.id][0]
    dest_pageserver = env.get_pageserver(dest_ps_id)
    shard_0_id = TenantShardId(tenant_id, 0, 0)

    env.storage_controller.allowed_errors.append(NOTIFY_BLOCKED_LOG)
    env.storage_controller.allowed_errors.extend(NOTIFY_FAILURE_LOGS)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # We expect the controller to hit the 423 (locked) and retry.  Migration shouldn't complete until that
        # status is cleared.
        handle_params["status"] = 423
        migrate_fut = executor.submit(
            env.storage_controller.tenant_shard_migrate, shard_0_id, dest_ps_id
        )

        def logged_stuck():
            env.storage_controller.assert_log_contains(NOTIFY_BLOCKED_LOG)

        wait_until(logged_stuck)
        contains_r = env.storage_controller.log_contains(NOTIFY_BLOCKED_LOG)
        assert contains_r is not None  # Appease mypy
        (_, log_cursor) = contains_r
        assert migrate_fut.running()

        # Permit the compute hook to proceed
        handle_params["status"] = 200
        migrate_fut.result(timeout=10)

        # Advance log cursor past the last 'stuck' message (we already waited for one, but
        # there could be more than one)
        while True:
            contains_r = env.storage_controller.log_contains(NOTIFY_BLOCKED_LOG, offset=log_cursor)
            if contains_r is None:
                break
            else:
                (_, log_cursor) = contains_r

        # Now, do a migration in the opposite direction
        handle_params["status"] = 423
        migrate_fut = executor.submit(
            env.storage_controller.tenant_shard_migrate, shard_0_id, origin_pageserver.id
        )

        def logged_stuck_again():
            env.storage_controller.assert_log_contains(NOTIFY_BLOCKED_LOG, offset=log_cursor)

        wait_until(logged_stuck_again)
        assert migrate_fut.running()

        # This time, the compute hook remains stuck, but we mark the origin node offline: this should
        # also allow the migration to complete -- we only wait for the compute hook as long as we think
        # the old location is still usable for computes.
        # This is a regression test for issue https://github.com/neondatabase/neon/issues/8901
        dest_pageserver.stop()
        env.storage_controller.node_configure(dest_ps_id, {"availability": "Offline"})

        try:
            migrate_fut.result(timeout=10)
        except StorageControllerApiException as e:
            # The reconciler will fail because it can't detach from the origin: the important
            # thing is that it finishes, rather than getting stuck in the compute notify loop.
            assert "Reconcile error" in str(e)

        # A later background reconciliation will clean up and leave things in a neat state, even
        # while the compute hook is still blocked
        try:
            env.storage_controller.reconcile_all()
        except StorageControllerApiException as e:
            # We expect that the reconciler will do its work, but be unable to fully succeed
            # because it can't send a compute notification.  It will complete, but leave
            # the internal flag set for "retry compute notification later"
            assert "Control plane tenant busy" in str(e)

        # Confirm that we are AttachedSingle on the node we last called the migrate API for
        loc = origin_pageserver.http_client().tenant_get_location(shard_0_id)
        assert loc["mode"] == "AttachedSingle"

        # When the origin node comes back, it should get cleaned up
        dest_pageserver.start()
        try:
            env.storage_controller.reconcile_all()
        except StorageControllerApiException as e:
            # Compute hook is still blocked: reconciler will configure PS but not fully succeed
            assert "Control plane tenant busy" in str(e)

        with pytest.raises(PageserverApiException, match="Tenant shard not found"):
            dest_pageserver.http_client().tenant_get_location(shard_0_id)

        # Once the compute hook is unblocked, we should be able to get into a totally
        # quiescent state again
        handle_params["status"] = 200
        env.storage_controller.reconcile_until_idle()

    env.storage_controller.consistency_check()


@run_only_on_default_postgres("postgres behavior is not relevant")
def test_storage_controller_compute_hook_retry(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
):
    """
    Test that when a reconciler can't do its compute hook notification, it will keep
    trying until it succeeds.

    Reproducer for https://github.com/neondatabase/cloud/issues/22612
    """

    neon_env_builder.num_pageservers = 2
    (host, port) = httpserver_listen_address
    neon_env_builder.control_plane_compute_hook_api = f"http://{host}:{port}/notify"

    handle_params = {"status": 200}

    notifications = []

    def handler(request: Request):
        status = handle_params["status"]
        log.info(f"Notify request[{status}]: {request}")
        notifications.append(request.json)
        return Response(status=status)

    httpserver.expect_request("/notify", method="PUT").respond_with_handler(handler)

    # Start running
    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    env.create_tenant(tenant_id, placement_policy='{"Attached": 1}')

    # Initial notification from tenant creation
    assert len(notifications) == 1
    expect: dict[str, list[dict[str, int]] | str | None | int] = {
        "tenant_id": str(tenant_id),
        "stripe_size": None,
        "shards": [{"node_id": int(env.pageservers[0].id), "shard_number": 0}],
        "preferred_az": DEFAULT_AZ_ID,
    }
    assert notifications[0] == expect

    # Block notifications, and fail a node
    handle_params["status"] = 423
    env.pageservers[0].stop()
    env.storage_controller.allowed_errors.append(NOTIFY_BLOCKED_LOG)
    env.storage_controller.allowed_errors.extend(NOTIFY_FAILURE_LOGS)

    # Avoid waiting for heartbeats
    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Offline"})

    # Make reconciler run and fail: it should leave itself in a state where the shard will retry notification later,
    # and we will check that that happens
    notifications = []
    try:
        assert env.storage_controller.reconcile_all() == 1
    except StorageControllerApiException as e:
        assert "Control plane tenant busy" in str(e)
    assert len(notifications) == 1
    assert (
        env.storage_controller.tenant_describe(tenant_id)["shards"][0][
            "is_pending_compute_notification"
        ]
        is True
    )

    # Try reconciling again, it should try notifying again
    notifications = []
    try:
        assert env.storage_controller.reconcile_all() == 1
    except StorageControllerApiException as e:
        assert "Control plane tenant busy" in str(e)
    assert len(notifications) == 1
    assert (
        env.storage_controller.tenant_describe(tenant_id)["shards"][0][
            "is_pending_compute_notification"
        ]
        is True
    )

    # The describe API should indicate that a notification is pending
    assert (
        env.storage_controller.tenant_describe(tenant_id)["shards"][0][
            "is_pending_compute_notification"
        ]
        is True
    )

    # Unblock notifications: reconcile should work now
    handle_params["status"] = 200
    notifications = []
    assert env.storage_controller.reconcile_all() == 1
    assert len(notifications) == 1
    assert (
        env.storage_controller.tenant_describe(tenant_id)["shards"][0][
            "is_pending_compute_notification"
        ]
        is False
    )

    # Reconciler should be idle now that it succeeded in its compute notification
    notifications = []
    assert env.storage_controller.reconcile_all() == 0
    assert len(notifications) == 0
    assert (
        env.storage_controller.tenant_describe(tenant_id)["shards"][0][
            "is_pending_compute_notification"
        ]
        is False
    )


@run_only_on_default_postgres("this test doesn't start an endpoint")
def test_storage_controller_compute_hook_revert(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
):
    """
    'revert' in the sense of a migration which gets reversed shortly after, as may happen during
    a rolling upgrade.

    This is a reproducer for https://github.com/neondatabase/neon/issues/9417

    The buggy behavior was that when the compute hook gave us errors, we assumed our last successfully
    sent state was still in effect, so when migrating back to the original pageserver we didn't bother
    notifying of that.  This is wrong because even a failed request might mutate the state on the server.
    """

    # We will run two pageserver to migrate and check that the storage controller sends notifications
    # when migrating.
    neon_env_builder.num_pageservers = 2
    (host, port) = httpserver_listen_address
    neon_env_builder.control_plane_compute_hook_api = f"http://{host}:{port}/notify"

    # Set up fake HTTP notify endpoint
    notifications = []

    handle_params = {"status": 200}

    def handler(request: Request):
        status = handle_params["status"]
        log.info(f"Notify request[{status}]: {request}")
        notifications.append(request.json)
        return Response(status=status)

    httpserver.expect_request("/notify", method="PUT").respond_with_handler(handler)

    # Start running
    env = neon_env_builder.init_start(initial_tenant_conf={"lsn_lease_length": "0s"})
    tenant_id = env.initial_tenant
    tenant_shard_id = TenantShardId(tenant_id, 0, 0)

    pageserver_a = env.get_tenant_pageserver(tenant_id)
    pageserver_b = [p for p in env.pageservers if p.id != pageserver_a.id][0]

    def notified_ps(ps_id: int) -> None:
        latest = notifications[-1]
        log.info(f"Waiting for {ps_id}, have {latest}")
        assert latest is not None
        assert latest["shards"] is not None
        assert latest["shards"][0]["node_id"] == ps_id

    wait_until(lambda: notified_ps(pageserver_a.id))

    env.storage_controller.allowed_errors.append(NOTIFY_BLOCKED_LOG)
    env.storage_controller.allowed_errors.extend(NOTIFY_FAILURE_LOGS)

    # Migrate A -> B, and make notifications fail while this is happening
    handle_params["status"] = 423

    with pytest.raises(StorageControllerApiException, match="Timeout waiting for shard"):
        # We expect the controller to give us an error because its reconciliation timed out
        # waiting for the compute hook.
        env.storage_controller.tenant_shard_migrate(tenant_shard_id, pageserver_b.id)

    # Although the migration API failed, the hook should still see pageserver B (it remembers what
    # was posted even when returning an error code)
    wait_until(lambda: notified_ps(pageserver_b.id))

    # Although the migration API failed, the tenant should still have moved to the right pageserver
    assert len(pageserver_b.http_client().tenant_list()) == 1

    # Before we clear the failure on the migration hook, we need the controller to give up
    # trying to notify about B -- the bug case we're reproducing is when the controller
    # _never_ successfully notified for B, then tries to notify for A.
    #
    # The controller will give up notifying if the origin of a migration becomes unavailable.
    pageserver_a.stop()

    # Preempt heartbeats for a faster test
    env.storage_controller.node_configure(pageserver_a.id, {"availability": "Offline"})

    def logged_giving_up():
        env.storage_controller.assert_log_contains(".*Giving up on compute notification.*")

    wait_until(logged_giving_up)

    pageserver_a.start()

    # Preempt heartbeats for determinism
    env.storage_controller.node_configure(pageserver_a.id, {"availability": "Active"})
    # Starting node will prompt a reconcile to clean up old AttachedStale location, for a deterministic test
    # we want that complete before we start our migration.  Tolerate failure because our compute hook is
    # still configured to fail
    try:
        env.storage_controller.reconcile_all()
    except StorageControllerApiException as e:
        # This exception _might_ be raised: it depends if our reconcile_all hit the on-node-activation
        # Reconciler lifetime or ran after it already completed.
        log.info(f"Expected error from reconcile_all: {e}")

    # Migrate B -> A, with a working compute hook: the controller should notify the hook because the
    # last update it made that was acked (423) by the compute was for node B.
    handle_params["status"] = 200
    env.storage_controller.tenant_shard_migrate(tenant_shard_id, pageserver_a.id)

    wait_until(lambda: notified_ps(pageserver_a.id))


def test_storage_controller_debug_apis(neon_env_builder: NeonEnvBuilder):
    """
    Verify that occasional-use debug APIs work as expected.  This is a lightweight test
    that just hits the endpoints to check that they don't bitrot.
    """

    neon_env_builder.num_pageservers = 3
    env = neon_env_builder.init_start()

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(tenant_id, shard_count=2, shard_stripe_size=8192)

    # Check that the consistency check passes on a freshly setup system
    env.storage_controller.consistency_check()

    # These APIs are intentionally not implemented as methods on NeonStorageController, as
    # they're just for use in unanticipated circumstances.

    # Initial tenant (1 shard) and the one we just created (2 shards) should be visible
    response = env.storage_controller.request(
        "GET",
        f"{env.storage_controller_api}/debug/v1/tenant",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )
    assert len(response.json()) == 3

    # Scheduler should report the expected nodes and shard counts
    response = env.storage_controller.request(
        "GET", f"{env.storage_controller_api}/debug/v1/scheduler"
    )
    # Two nodes, in a dict of node_id->node
    assert len(response.json()["nodes"]) == 3
    assert sum(v["shard_count"] for v in response.json()["nodes"].values()) == 3
    assert all(v["may_schedule"] for v in response.json()["nodes"].values())

    # Reconciler cancel API should be a no-op when nothing is in flight
    env.storage_controller.request(
        "PUT",
        f"{env.storage_controller_api}/control/v1/tenant/{tenant_id}-0102/cancel_reconcile",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )

    # Secondary migration API: superficial check that it migrates
    secondary_dest = env.pageservers[2].id
    env.storage_controller.request(
        "PUT",
        f"{env.storage_controller_api}/control/v1/tenant/{tenant_id}-0002/migrate_secondary",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
        json={"tenant_shard_id": f"{tenant_id}-0002", "node_id": secondary_dest},
    )
    assert env.storage_controller.tenant_describe(tenant_id)["shards"][0]["node_secondary"] == [
        secondary_dest
    ]

    # Node unclean drop API
    response = env.storage_controller.request(
        "POST",
        f"{env.storage_controller_api}/debug/v1/node/{env.pageservers[1].id}/drop",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )
    assert len(env.storage_controller.node_list()) == 2

    # Tenant unclean drop API
    response = env.storage_controller.request(
        "POST",
        f"{env.storage_controller_api}/debug/v1/tenant/{tenant_id}/drop",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )

    # Tenant drop should be reflected in dump output
    response = env.storage_controller.request(
        "GET",
        f"{env.storage_controller_api}/debug/v1/tenant",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )
    assert len(response.json()) == 1
    # Check that the 'drop' APIs didn't leave things in a state that would fail a consistency check: they're
    # meant to be unclean wrt the pageserver state, but not leave a broken storage controller behind.
    env.storage_controller.consistency_check()


def test_storage_controller_s3_time_travel_recovery(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    """
    Test for S3 time travel
    """

    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    # Mock S3 doesn't have versioning enabled by default, enable it
    # (also do it before there is any writes to the bucket)
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        remote_storage = neon_env_builder.pageserver_remote_storage
        assert remote_storage, "remote storage not configured"
        enable_remote_storage_versioning(remote_storage)

    neon_env_builder.num_pageservers = 1

    env = neon_env_builder.init_start()
    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        tenant_id,
        shard_count=2,
        shard_stripe_size=8192,
        tenant_config=many_small_layers_tenant_config(),
    )

    # Check that the consistency check passes
    env.storage_controller.consistency_check()

    branch_name = "main"
    timeline_id = env.create_timeline(branch_name, tenant_id=tenant_id)
    # Write some nontrivial amount of data into the endpoint and wait until it is uploaded
    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        run_pg_bench_small(pg_bin, endpoint.connstr())
        endpoint.safe_psql("CREATE TABLE created_foo(id integer);")
        # last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)

    # Give the data time to be uploaded
    time.sleep(4)

    # Detach the tenant
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )

    time.sleep(4)
    ts_before_disaster = datetime.now(tz=UTC).replace(tzinfo=None)
    time.sleep(4)

    # Simulate a "disaster": delete some random files from remote storage for one of the shards
    assert env.pageserver_remote_storage
    shard_id_for_list = "0002"
    objects: list[ObjectTypeDef] = list_prefix(
        env.pageserver_remote_storage,
        f"tenants/{tenant_id}-{shard_id_for_list}/timelines/{timeline_id}/",
    ).get("Contents", [])
    assert len(objects) > 1
    log.info(f"Found {len(objects)} objects in remote storage")
    should_delete = False
    for obj in objects:
        obj_key = obj["Key"]
        should_delete = not should_delete
        if not should_delete:
            log.info(f"Keeping key on remote storage: {obj_key}")
            continue
        log.info(f"Deleting key from remote storage: {obj_key}")
        remote_storage_delete_key(env.pageserver_remote_storage, obj_key)
        pass

    time.sleep(4)
    ts_after_disaster = datetime.now(tz=UTC).replace(tzinfo=None)
    time.sleep(4)

    # Do time travel recovery
    virtual_ps_http.tenant_time_travel_remote_storage(
        tenant_id, ts_before_disaster, ts_after_disaster, shard_counts=[2]
    )
    time.sleep(4)

    # Attach the tenant again
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": 100,
        },
    )

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        endpoint.safe_psql("SELECT * FROM created_foo;")

    env.storage_controller.consistency_check()


def test_storage_controller_auth(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()
    svc = env.storage_controller
    api = env.storage_controller_api

    tenant_id = TenantId.generate()
    body: dict[str, Any] = {"new_tenant_id": str(tenant_id)}

    env.storage_controller.allowed_errors.append(".*Unauthorized.*")
    env.storage_controller.allowed_errors.append(".*Forbidden.*")

    # No token
    with pytest.raises(
        StorageControllerApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("POST", f"{env.storage_controller_api}/v1/tenant", json=body)

    # Token with incorrect scope
    with pytest.raises(
        StorageControllerApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request(
            "POST", f"{api}/v1/tenant", json=body, headers=svc.headers(TokenScope.SAFEKEEPER_DATA)
        )

    # Token with correct scope
    svc.request(
        "POST", f"{api}/v1/tenant", json=body, headers=svc.headers(TokenScope.PAGE_SERVER_API)
    )

    # Token with admin scope should also be permitted
    svc.request("POST", f"{api}/v1/tenant", json=body, headers=svc.headers(TokenScope.ADMIN))

    # No token
    with pytest.raises(
        StorageControllerApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("GET", f"{api}/debug/v1/tenant")

    # Token with incorrect scope
    with pytest.raises(
        StorageControllerApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request(
            "GET", f"{api}/debug/v1/tenant", headers=svc.headers(TokenScope.GENERATIONS_API)
        )

    # No token
    with pytest.raises(
        StorageControllerApiException,
        match="Unauthorized: missing authorization header",
    ):
        svc.request("POST", f"{api}/upcall/v1/re-attach")

    # Token with incorrect scope
    with pytest.raises(
        StorageControllerApiException,
        match="Forbidden: JWT authentication error",
    ):
        svc.request(
            "POST", f"{api}/upcall/v1/re-attach", headers=svc.headers(TokenScope.PAGE_SERVER_API)
        )


def test_storage_controller_tenant_conf(neon_env_builder: NeonEnvBuilder):
    """
    Validate the pageserver-compatible API endpoints for setting and getting tenant conf, without
    supplying the whole LocationConf.
    """

    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant

    http = env.storage_controller.pageserver_api()

    default_value = "7days"
    new_value = "1h"
    http.set_tenant_config(tenant_id, {"pitr_interval": new_value})

    # Ensure the change landed on the storage controller
    readback_controller = http.tenant_config(tenant_id)
    assert readback_controller.effective_config["pitr_interval"] == new_value
    assert readback_controller.tenant_specific_overrides["pitr_interval"] == new_value

    # Ensure the change made it down to the pageserver
    readback_ps = env.pageservers[0].http_client().tenant_config(tenant_id)
    assert readback_ps.effective_config["pitr_interval"] == new_value
    assert readback_ps.tenant_specific_overrides["pitr_interval"] == new_value

    # Omitting a value clears it.  This looks different in storage controller
    # vs. pageserver API calls, because pageserver has defaults.
    http.set_tenant_config(tenant_id, {})
    readback_controller = http.tenant_config(tenant_id)
    assert readback_controller.effective_config["pitr_interval"] is None
    assert readback_controller.tenant_specific_overrides["pitr_interval"] is None
    readback_ps = env.pageservers[0].http_client().tenant_config(tenant_id)
    assert readback_ps.effective_config["pitr_interval"] == default_value
    assert "pitr_interval" not in readback_ps.tenant_specific_overrides

    env.storage_controller.consistency_check()


def test_storage_controller_tenant_deletion(
    neon_env_builder: NeonEnvBuilder,
    compute_reconfigure_listener: ComputeReconfigure,
):
    """
    Validate that:
    - Deleting a tenant deletes all its shards
    - Deletion does not require the compute notification hook to be responsive
    - Deleting a tenant also removes all secondary locations
    """
    neon_env_builder.num_pageservers = 4
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.control_plane_compute_hook_api = (
        compute_reconfigure_listener.control_plane_compute_hook_api
    )

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.create_tenant(tenant_id, timeline_id, shard_count=2, placement_policy='{"Attached":1}')

    # Ensure all the locations are configured, including secondaries
    env.storage_controller.reconcile_until_idle()

    shard_ids = [
        TenantShardId.parse(shard["shard_id"]) for shard in env.storage_controller.locate(tenant_id)
    ]

    # Assert attachments all have local content
    for shard_id in shard_ids:
        pageserver = env.get_tenant_pageserver(shard_id)
        assert pageserver is not None
        assert pageserver.tenant_dir(shard_id).exists()

    # Assert all shards have some content in remote storage
    for shard_id in shard_ids:
        assert_prefix_not_empty(
            neon_env_builder.pageserver_remote_storage,
            prefix="/".join(
                (
                    "tenants",
                    str(shard_id),
                )
            ),
        )

    # Break the compute hook: we are checking that deletion does not depend on the compute hook being available
    def break_hook(_body: Any):
        raise RuntimeError("Unexpected call to compute hook")

    compute_reconfigure_listener.register_on_notify(break_hook)

    # No retry loop: deletion should complete in one shot without polling for 202 responses, because
    # it cleanly detaches all the shards first, and then deletes them in remote storage
    env.storage_controller.pageserver_api().tenant_delete(tenant_id)

    # Assert no pageservers have any local content
    for pageserver in env.pageservers:
        for shard_id in shard_ids:
            assert not pageserver.tenant_dir(shard_id).exists()

    for shard_id in shard_ids:
        assert_prefix_empty(
            neon_env_builder.pageserver_remote_storage,
            prefix="/".join(
                (
                    "tenants",
                    str(shard_id),
                )
            ),
        )

    # Assert the tenant is not visible in storage controller API
    with pytest.raises(StorageControllerApiException):
        env.storage_controller.tenant_describe(tenant_id)


class Failure:
    pageserver_id: int
    offline_timeout: int
    must_detect_after: int

    def apply(self, env: NeonEnv):
        raise NotImplementedError()

    def clear(self, env: NeonEnv):
        raise NotImplementedError()

    def nodes(self):
        raise NotImplementedError()


class NodeStop(Failure):
    def __init__(self, pageserver_ids, immediate, offline_timeout, must_detect_after):
        self.pageserver_ids = pageserver_ids
        self.immediate = immediate
        self.offline_timeout = offline_timeout
        self.must_detect_after = must_detect_after

    def apply(self, env: NeonEnv):
        for ps_id in self.pageserver_ids:
            pageserver = env.get_pageserver(ps_id)
            pageserver.stop(immediate=self.immediate)

    def clear(self, env: NeonEnv):
        for ps_id in self.pageserver_ids:
            pageserver = env.get_pageserver(ps_id)
            pageserver.start()

    def nodes(self):
        return self.pageserver_ids


class NodeRestartWithSlowReattach(Failure):
    def __init__(self, pageserver_id, offline_timeout, must_detect_after):
        self.pageserver_id = pageserver_id
        self.offline_timeout = offline_timeout
        self.must_detect_after = must_detect_after
        self.thread = None

    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.stop(immediate=False)

        def start_ps():
            pageserver.start(
                extra_env_vars={"FAILPOINTS": "control-plane-client-re-attach=return(30000)"}
            )

        self.thread = threading.Thread(target=start_ps)
        self.thread.start()

    def clear(self, env: NeonEnv):
        if self.thread is not None:
            self.thread.join()

        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.http_client().configure_failpoints(("control-plane-client-re-attach", "off"))

    def nodes(self):
        return [self.pageserver_id]


class PageserverFailpoint(Failure):
    def __init__(self, failpoint, pageserver_id, offline_timeout, must_detect_after):
        self.failpoint = failpoint
        self.pageserver_id = pageserver_id
        self.offline_timeout = offline_timeout
        self.must_detect_after = must_detect_after

    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.http_client().configure_failpoints((self.failpoint, "return(1)"))

    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.http_client().configure_failpoints((self.failpoint, "off"))

    def nodes(self):
        return [self.pageserver_id]


def build_node_to_tenants_map(env: NeonEnv) -> dict[int, list[TenantId]]:
    tenants = env.storage_controller.tenant_shard_dump()

    node_to_tenants: dict[int, list[TenantId]] = {}
    for t in tenants:
        for node_id, loc_state in t["observed"]["locations"].items():
            if (
                loc_state is not None
                and "conf" in loc_state
                and loc_state["conf"] is not None
                and loc_state["conf"]["mode"] == "AttachedSingle"
            ):
                crnt = node_to_tenants.get(int(node_id), [])
                crnt.append(TenantId(t["tenant_shard_id"]))
                node_to_tenants[int(node_id)] = crnt

    return node_to_tenants


@pytest.mark.parametrize(
    "failure",
    [
        NodeStop(pageserver_ids=[1], immediate=False, offline_timeout=20, must_detect_after=5),
        NodeStop(pageserver_ids=[1], immediate=True, offline_timeout=20, must_detect_after=5),
        NodeStop(pageserver_ids=[1, 2], immediate=True, offline_timeout=20, must_detect_after=5),
        PageserverFailpoint(
            pageserver_id=1,
            failpoint="get-utilization-http-handler",
            offline_timeout=20,
            must_detect_after=5,
        ),
        # Instrument a scenario where the node is slow to re-attach. The re-attach request itself
        # should serve as a signal to the storage controller to use a more lenient heartbeat timeout.
        NodeRestartWithSlowReattach(pageserver_id=1, offline_timeout=60, must_detect_after=15),
    ],
)
def test_storage_controller_heartbeats(
    neon_env_builder: NeonEnvBuilder, pg_bin: PgBin, failure: Failure
):
    neon_env_builder.storage_controller_config = {
        "max_offline": "10s",
        "max_warming_up": "20s",
    }

    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    # Default log allow list permits connection errors, but this test will use error responses on
    # the utilization endpoint.
    env.storage_controller.allowed_errors.append(
        ".*Call to node.*management API.*failed.*failpoint.*"
    )

    # Initially we have two online pageservers
    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2
    assert all([n["availability"] == "Active" for n in nodes])

    # ... then we create two tenants and write some data into them
    def create_tenant(tid: TenantId):
        env.storage_controller.tenant_create(tid)

        branch_name = "main"
        env.create_timeline(branch_name, tenant_id=tid)

        with env.endpoints.create_start("main", tenant_id=tid) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            endpoint.safe_psql("CREATE TABLE created_foo(id integer);")

    tenant_ids = [TenantId.generate(), TenantId.generate()]
    for tid in tenant_ids:
        create_tenant(tid)

    # ... expecting that each tenant will be placed on a different node
    def tenants_placed():
        node_to_tenants = build_node_to_tenants_map(env)
        log.info(f"{node_to_tenants=}")

        # Check that all the tenants have been attached
        assert sum(len(ts) for ts in node_to_tenants.values()) == len(tenant_ids)
        # Check that each node got one tenant
        assert all(len(ts) == 1 for ts in node_to_tenants.values())

    wait_until(tenants_placed)

    # ... then we apply the failure
    offline_node_ids = set(failure.nodes())
    online_node_ids = set(range(1, len(env.pageservers) + 1)) - offline_node_ids

    for node_id in offline_node_ids:
        if len(offline_node_ids) > 1:
            env.get_pageserver(node_id).allowed_errors.append(
                ".*Scheduling error when marking pageserver.*offline.*",
            )

    failure.apply(env)

    # ... expecting the heartbeats to mark it offline
    def nodes_offline():
        nodes = env.storage_controller.node_list()
        log.info(f"{nodes=}")
        for node in nodes:
            if node["id"] in offline_node_ids:
                assert node["availability"] == "Offline"

    start = time.time()
    wait_until(nodes_offline, timeout=failure.offline_timeout)
    detected_after = time.time() - start
    log.info(f"Detected node failures after {detected_after}s")

    assert detected_after >= failure.must_detect_after

    # .. expecting the tenant on the offline node to be migrated
    def tenant_migrated():
        if len(online_node_ids) == 0:
            time.sleep(5)
            return

        node_to_tenants = build_node_to_tenants_map(env)
        log.info(f"{node_to_tenants=}")

        observed_tenants = set()
        for node_id in online_node_ids:
            observed_tenants |= set(node_to_tenants[node_id])

        assert observed_tenants == set(tenant_ids)

    wait_until(tenant_migrated)

    # ... then we clear the failure
    failure.clear(env)

    # ... expecting the offline node to become active again
    def nodes_online():
        nodes = env.storage_controller.node_list()
        for node in nodes:
            if node["id"] in online_node_ids:
                assert node["availability"] == "Active"

    wait_until(nodes_online)

    time.sleep(5)

    node_to_tenants = build_node_to_tenants_map(env)
    log.info(f"Back online: {node_to_tenants=}")

    # ... background reconciliation may need to run to clean up the location on the node that was offline
    env.storage_controller.reconcile_until_idle()

    # ... expecting the storage controller to reach a consistent state
    env.storage_controller.consistency_check()


def test_storage_controller_re_attach(neon_env_builder: NeonEnvBuilder):
    """
    Exercise the behavior of the /re-attach endpoint on pageserver startup when
    pageservers have a mixture of attached and secondary locations
    """

    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    # We'll have two tenants.
    tenant_a = TenantId.generate()
    env.create_tenant(tenant_a, placement_policy='{"Attached":1}')
    tenant_b = TenantId.generate()
    env.create_tenant(tenant_b, placement_policy='{"Attached":1}')

    # Each pageserver will have one attached and one secondary location
    env.storage_controller.tenant_shard_migrate(
        TenantShardId(tenant_a, 0, 0), env.pageservers[0].id
    )
    env.storage_controller.tenant_shard_migrate(
        TenantShardId(tenant_b, 0, 0), env.pageservers[1].id
    )

    # Hard-fail a pageserver
    victim_ps = env.pageservers[1]
    survivor_ps = env.pageservers[0]
    victim_ps.stop(immediate=True)

    # Heatbeater will notice it's offline, and consequently attachments move to the other pageserver
    def failed_over():
        locations = survivor_ps.http_client().tenant_list_locations()["tenant_shards"]
        log.info(f"locations: {locations}")
        assert len(locations) == 2
        assert all(loc[1]["mode"] == "AttachedSingle" for loc in locations)

    # We could pre-empty this by configuring the node to Offline, but it's preferable to test
    # the realistic path we would take when a node restarts uncleanly.
    # The delay here will be ~NEON_LOCAL_MAX_UNAVAILABLE_INTERVAL in neon_local
    wait_until(failed_over)

    reconciles_before_restart = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "ok"}
    )

    # Restart the failed pageserver
    victim_ps.start()

    # We expect that the re-attach call correctly tipped off the pageserver that its locations
    # are all secondaries now.
    locations = victim_ps.http_client().tenant_list_locations()["tenant_shards"]
    assert len(locations) == 2
    assert all(loc[1]["mode"] == "Secondary" for loc in locations)

    # We expect that this situation resulted from the re_attach call, and not any explicit
    # Reconciler runs: assert that the reconciliation count has not gone up since we restarted.
    reconciles_after_restart = env.storage_controller.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "ok"}
    )
    assert reconciles_after_restart == reconciles_before_restart


def test_storage_controller_shard_scheduling_policy(neon_env_builder: NeonEnvBuilder):
    """
    Check that emergency hooks for disabling rogue tenants' reconcilers work as expected.
    """
    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()

    env.storage_controller.allowed_errors.extend(
        [
            # We will intentionally cause reconcile errors
            ".*Reconcile error.*",
            # Message from using a scheduling policy
            ".*Scheduling is disabled by policy.*",
            ".*Skipping reconcile for policy.*",
            # Message from a node being offline
            ".*Call to node .* management API .* failed",
        ]
    )

    # Stop pageserver so that reconcile cannot complete
    env.pageserver.stop()

    env.storage_controller.tenant_create(tenant_id, placement_policy="Detached")

    # Try attaching it: we should see reconciles failing
    env.storage_controller.tenant_policy_update(
        tenant_id,
        {
            "placement": {"Attached": 0},
        },
    )

    def reconcile_errors() -> int:
        return int(
            env.storage_controller.get_metric_value(
                "storage_controller_reconcile_complete_total", filter={"status": "error"}
            )
            or 0
        )

    def reconcile_ok() -> int:
        return int(
            env.storage_controller.get_metric_value(
                "storage_controller_reconcile_complete_total", filter={"status": "ok"}
            )
            or 0
        )

    def assert_errors_gt(n) -> int:
        e = reconcile_errors()
        assert e > n
        return e

    errs = wait_until(lambda: assert_errors_gt(0))

    # Try reconciling again, it should fail again
    with pytest.raises(StorageControllerApiException):
        env.storage_controller.reconcile_all()
    errs = wait_until(lambda: assert_errors_gt(errs))

    # Configure the tenant to disable reconciles
    env.storage_controller.tenant_policy_update(
        tenant_id,
        {
            "scheduling": "Stop",
        },
    )

    # Try reconciling again, it should not cause an error (silently skip)
    env.storage_controller.reconcile_all()
    assert reconcile_errors() == errs

    # Start the pageserver and re-enable reconciles
    env.pageserver.start()
    env.storage_controller.tenant_policy_update(
        tenant_id,
        {
            "scheduling": "Active",
        },
    )

    def assert_ok_gt(n) -> int:
        o = reconcile_ok()
        assert o > n
        return o

    # We should see a successful reconciliation
    wait_until(lambda: assert_ok_gt(0))

    # And indeed the tenant should be attached
    assert len(env.pageserver.http_client().tenant_list_locations()["tenant_shards"]) == 1


def test_storcon_cli(neon_env_builder: NeonEnvBuilder):
    """
    The storage controller command line interface (storcon-cli) is an internal tool.  Most tests
    just use the APIs directly: this test exercises some basics of the CLI as a regression test
    that the client remains usable as the server evolves.
    """
    output_dir = neon_env_builder.test_output_dir
    shard_count = 4
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    env.create_tenant(tenant_id, placement_policy='{"Attached":1}', shard_count=shard_count)

    base_args = [env.neon_binpath / "storcon_cli", "--api", env.storage_controller_api]

    def storcon_cli(args):
        """
        CLI wrapper: returns stdout split into a list of non-empty strings
        """
        (output_path, stdout, status_code) = subprocess_capture(
            output_dir,
            [str(s) for s in base_args + args],
            echo_stderr=True,
            echo_stdout=True,
            env={},
            check=False,
            capture_stdout=True,
            timeout=10,
        )
        if status_code:
            log.warning(f"Command {args} failed")
            log.warning(f"Output at: {output_path}")

            raise RuntimeError("CLI failure (check logs for stderr)")

        assert stdout is not None
        return [line.strip() for line in stdout.split("\n") if line.strip()]

    # List nodes
    node_lines = storcon_cli(["nodes"])
    # Table header, footer, and one line of data
    assert len(node_lines) == 7
    assert "localhost" in node_lines[3]

    # Pause scheduling onto a node
    storcon_cli(["node-configure", "--node-id", "1", "--scheduling", "pause"])
    assert "Pause" in storcon_cli(["nodes"])[3]

    # We will simulate a node death and then marking it offline
    env.pageservers[0].stop(immediate=True)
    # Sleep to make it unlikely that the controller's heartbeater will race handling
    # a /utilization response internally, such that it marks the node back online.  IRL
    # there would always be a longer delay than this before a node failing and a human
    # intervening.
    time.sleep(2)

    storcon_cli(["node-configure", "--node-id", "1", "--availability", "offline"])
    assert "Offline" in storcon_cli(["nodes"])[3]

    # Restore node, verify status changes in CLI output
    env.pageservers[0].start()

    def is_online():
        assert "Offline" not in storcon_cli(["nodes"])

    wait_until(is_online)

    # Let everything stabilize after node failure to avoid interfering with subsequent steps
    env.storage_controller.reconcile_until_idle(timeout_secs=10)

    # List tenants
    tenant_lines = storcon_cli(["tenants"])
    assert len(tenant_lines) == 5
    assert str(tenant_id) in tenant_lines[3]

    # Setting scheduling policies intentionally result in warnings, they're for rare use.
    env.storage_controller.allowed_errors.extend(
        [".*Skipping reconcile for policy.*", ".*Scheduling is disabled by policy.*"]
    )

    # Describe a tenant
    tenant_lines = storcon_cli(["tenant-describe", "--tenant-id", str(tenant_id)])
    assert len(tenant_lines) >= 3 + shard_count * 2
    assert str(tenant_id) in tenant_lines[0]

    # Migrate an attached location
    def other_ps_id(current_ps_id):
        return (
            env.pageservers[0].id
            if current_ps_id == env.pageservers[1].id
            else env.pageservers[1].id
        )

    storcon_cli(
        [
            "tenant-shard-migrate",
            "--tenant-shard-id",
            f"{tenant_id}-0004",
            "--node",
            str(
                other_ps_id(
                    env.storage_controller.tenant_describe(tenant_id)["shards"][0]["node_attached"]
                )
            ),
        ]
    )

    # Migrate a secondary location
    storcon_cli(
        [
            "tenant-shard-migrate-secondary",
            "--tenant-shard-id",
            f"{tenant_id}-0004",
            "--node",
            str(
                other_ps_id(
                    env.storage_controller.tenant_describe(tenant_id)["shards"][0][
                        "node_secondary"
                    ][0]
                )
            ),
        ]
    )

    # Pause changes on a tenant
    storcon_cli(["tenant-policy", "--tenant-id", str(tenant_id), "--scheduling", "stop"])
    assert "Stop" in storcon_cli(["tenants"])[3]

    # Cancel ongoing reconcile on a tenant
    storcon_cli(["tenant-shard-cancel-reconcile", "--tenant-shard-id", f"{tenant_id}-0104"])

    # Change a tenant's placement
    storcon_cli(["tenant-policy", "--tenant-id", str(tenant_id), "--placement", "secondary"])
    assert "Secondary" in storcon_cli(["tenants"])[3]

    # Modify a tenant's config
    storcon_cli(
        [
            "patch-tenant-config",
            "--tenant-id",
            str(tenant_id),
            "--config",
            json.dumps({"pitr_interval": "1m"}),
        ]
    )

    # Quiesce any background reconciliation before doing consistency check
    env.storage_controller.reconcile_until_idle(timeout_secs=10)
    env.storage_controller.consistency_check()


def test_lock_time_tracing(neon_env_builder: NeonEnvBuilder):
    """
    Check that when lock on resource (tenants, nodes) is held for too long it is
    traced in logs.
    """
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    env.storage_controller.allowed_errors.extend(
        [
            ".*Exclusive lock by.*",
            ".*Shared lock by.*",
            ".*Scheduling is disabled by policy.*",
            f".*Operation TimelineCreate on key {tenant_id} has waited.*",
        ]
    )

    # Apply failpoint
    env.storage_controller.configure_failpoints(
        ("tenant-update-policy-exclusive-lock", "return(35000)")
    )

    # This will hold the exclusive for enough time to cause an warning
    def update_tenent_policy():
        env.storage_controller.tenant_policy_update(
            tenant_id=tenant_id,
            body={
                "scheduling": "Stop",
            },
        )

    thread_update_tenant_policy = threading.Thread(target=update_tenent_policy)
    thread_update_tenant_policy.start()

    # Make sure the update policy thread has started
    time.sleep(1)
    # This will not be able to access and will log a warning
    timeline_id = TimelineId.generate()
    env.storage_controller.pageserver_api().timeline_create(
        pg_version=PgVersion.NOT_SET, tenant_id=tenant_id, new_timeline_id=timeline_id
    )
    thread_update_tenant_policy.join()

    env.storage_controller.assert_log_contains("Exclusive lock by UpdatePolicy was held for")
    _, last_log_cursor = env.storage_controller.assert_log_contains(
        f"Operation TimelineCreate on key {tenant_id} has waited"
    )

    # Test out shared lock
    env.storage_controller.configure_failpoints(
        ("tenant-create-timeline-shared-lock", "return(31000)")
    )

    timeline_id = TimelineId.generate()
    # This will hold the shared lock for enough time to cause an warning
    env.storage_controller.pageserver_api().timeline_create(
        pg_version=PgVersion.NOT_SET, tenant_id=tenant_id, new_timeline_id=timeline_id
    )
    env.storage_controller.assert_log_contains(
        "Shared lock by TimelineCreate was held for", offset=last_log_cursor
    )


@pytest.mark.parametrize("remote_storage", [RemoteStorageKind.LOCAL_FS, s3_storage()])
@pytest.mark.parametrize("shard_count", [None, 4])
def test_tenant_import(neon_env_builder: NeonEnvBuilder, shard_count, remote_storage):
    """
    Tenant import is a support/debug tool for recovering a tenant from remote storage
    if we don't have any metadata for it in the storage controller.
    """

    # This test is parametrized on remote storage because it exercises the relatively rare
    # code path of listing with a prefix that is not a directory name: this helps us notice
    # quickly if local_fs or s3_bucket implementations diverge.
    neon_env_builder.enable_pageserver_remote_storage(remote_storage)

    # Use multiple pageservers because some test helpers assume single sharded tenants
    # if there is only one pageserver.
    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)
    tenant_id = env.initial_tenant

    # Create a second timeline to ensure that import finds both
    timeline_a = env.initial_timeline
    timeline_b = env.create_branch("branch_b", tenant_id=tenant_id)

    workload_a = Workload(env, tenant_id, timeline_a, branch_name="main")
    workload_a.init()

    workload_b = Workload(env, tenant_id, timeline_b, branch_name="branch_b")
    workload_b.init()

    # Write some data
    workload_a.write_rows(72)
    expect_rows_a = workload_a.expect_rows
    workload_a.stop()
    del workload_a

    # Bump generation to make sure generation recovery works properly
    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()

    # Write some data in the higher generation into the other branch
    workload_b.write_rows(107)
    expect_rows_b = workload_b.expect_rows
    workload_b.stop()
    del workload_b

    # Detach from pageservers
    env.storage_controller.tenant_policy_update(
        tenant_id,
        {
            "placement": "Detached",
        },
    )
    env.storage_controller.reconcile_until_idle(timeout_secs=10)

    # Force-drop it from the storage controller
    env.storage_controller.request(
        "POST",
        f"{env.storage_controller_api}/debug/v1/tenant/{tenant_id}/drop",
        headers=env.storage_controller.headers(TokenScope.ADMIN),
    )

    # Now import it again
    env.neon_cli.tenant_import(tenant_id)

    # Check we found the shards
    describe = env.storage_controller.tenant_describe(tenant_id)
    literal_shard_count = 1 if shard_count is None else shard_count
    assert len(describe["shards"]) == literal_shard_count

    nodes = env.storage_controller.nodes()
    assert len(nodes) == 2
    describe1 = env.storage_controller.node_shards(nodes[0]["id"])
    describe2 = env.storage_controller.node_shards(nodes[1]["id"])
    assert len(describe1["shards"]) + len(describe2["shards"]) == literal_shard_count

    # Check the data is still there: this implicitly proves that we recovered generation numbers
    # properly, for the timeline which was written to after a generation bump.
    for timeline, branch, expect_rows in [
        (timeline_a, "main", expect_rows_a),
        (timeline_b, "branch_1", expect_rows_b),
    ]:
        workload = Workload(env, tenant_id, timeline, branch_name=branch)
        workload.expect_rows = expect_rows
        workload.validate()


def test_graceful_cluster_restart(neon_env_builder: NeonEnvBuilder):
    """
    Graceful reststart of storage controller clusters use the drain and
    fill hooks in order to migrate attachments away from pageservers before
    restarting. In practice, Ansible will drive this process.
    """
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    tenant_count = 5
    shard_count_per_tenant = 8
    total_shards = tenant_count * shard_count_per_tenant
    tenant_ids = []

    for _ in range(0, tenant_count):
        tid = TenantId.generate()
        tenant_ids.append(tid)
        env.create_tenant(
            tid, placement_policy='{"Attached":1}', shard_count=shard_count_per_tenant
        )

    # Give things a chance to settle.
    env.storage_controller.reconcile_until_idle(timeout_secs=30)

    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2

    def assert_shard_counts_balanced(env: NeonEnv, shard_counts, total_shards):
        # Assert that all nodes have some attached shards
        assert len(shard_counts) == len(env.pageservers)

        min_shard_count = min(shard_counts.values())
        max_shard_count = max(shard_counts.values())

        flake_factor = 5 / 100
        assert max_shard_count - min_shard_count <= int(total_shards * flake_factor)

    # Perform a graceful rolling restart
    for ps in env.pageservers:
        env.storage_controller.warm_up_all_secondaries()

        env.storage_controller.retryable_node_operation(
            lambda ps_id: env.storage_controller.node_drain(ps_id), ps.id, max_attempts=3, backoff=2
        )
        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.PAUSE_FOR_RESTART,
            max_attempts=6,
            backoff=5,
        )

        shard_counts = get_node_shard_counts(env, tenant_ids)
        log.info(f"Shard counts after draining node {ps.id}: {shard_counts}")
        # Assert that we've drained the node
        assert shard_counts[ps.id] == 0
        # Assert that those shards actually went somewhere
        assert sum(shard_counts.values()) == total_shards

        ps.restart()
        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.ACTIVE,
            max_attempts=10,
            backoff=1,
        )

        env.storage_controller.retryable_node_operation(
            lambda ps_id: env.storage_controller.node_fill(ps_id), ps.id, max_attempts=3, backoff=2
        )
        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.ACTIVE,
            max_attempts=6,
            backoff=5,
        )

        shard_counts = get_node_shard_counts(env, tenant_ids)
        log.info(f"Shard counts after filling node {ps.id}: {shard_counts}")
        assert_shard_counts_balanced(env, shard_counts, total_shards)

    # Now check that shards are reasonably balanced
    shard_counts = get_node_shard_counts(env, tenant_ids)
    log.info(f"Shard counts after rolling restart: {shard_counts}")
    assert_shard_counts_balanced(env, shard_counts, total_shards)


def test_skip_drain_on_secondary_lag(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Artificially make a tenant shard's secondary location lag behind the primary
    and check that storage controller driven node drains skip the lagging tenant shard.
    Finally, validate that the tenant shard is migrated when a new drain request comes
    in and it's no longer lagging.
    """
    neon_env_builder.num_pageservers = 2
    neon_env_builder.storage_controller_config = {
        "max_secondary_lag_bytes": 1 * 1024 * 1024,
    }

    env = neon_env_builder.init_configs()
    env.start()

    tid, timeline_id = env.create_tenant(placement_policy='{"Attached":1}')

    # Give things a chance to settle.
    env.storage_controller.reconcile_until_idle(timeout_secs=30)

    locations = env.storage_controller.locate(tid)
    assert len(locations) == 1
    primary: int = locations[0]["node_id"]
    not_primary = [ps.id for ps in env.pageservers if ps.id != primary]
    assert len(not_primary) == 1
    secondary = not_primary[0]

    log.info(f"Paused secondary downloads on {secondary}")
    env.get_pageserver(secondary).http_client().configure_failpoints(
        ("secondary-layer-download-pausable", "pause")
    )

    log.info(f"Ingesting some data for {tid}")

    with env.endpoints.create_start("main", tenant_id=tid) as endpoint:
        run_pg_bench_small(pg_bin, endpoint.connstr())
        endpoint.safe_psql("CREATE TABLE created_foo(id integer);")
        last_flush_lsn_upload(env, endpoint, tid, timeline_id)

    log.info(f"Uploading heatmap from {primary} and requesting download from {secondary}")

    env.get_pageserver(primary).http_client().tenant_heatmap_upload(tid)
    env.get_pageserver(secondary).http_client().tenant_secondary_download(tid, wait_ms=100)

    def secondary_is_lagging():
        resp = env.get_pageserver(secondary).http_client().tenant_secondary_status(tid)
        lag = resp["bytes_total"] - resp["bytes_downloaded"]

        if lag <= 1 * 1024 * 1024:
            raise Exception(f"Secondary lag not big enough: {lag}")

    log.info(f"Looking for lag to develop on the secondary {secondary}")
    wait_until(secondary_is_lagging)

    log.info(f"Starting drain of primary {primary} with laggy secondary {secondary}")
    env.storage_controller.retryable_node_operation(
        lambda ps_id: env.storage_controller.node_drain(ps_id), primary, max_attempts=3, backoff=2
    )

    env.storage_controller.poll_node_status(
        primary,
        PageserverAvailability.ACTIVE,
        PageserverSchedulingPolicy.PAUSE_FOR_RESTART,
        max_attempts=6,
        backoff=5,
    )

    locations = env.storage_controller.locate(tid)
    assert len(locations) == 1
    assert locations[0]["node_id"] == primary

    log.info(f"Unpausing secondary downloads on {secondary}")
    env.get_pageserver(secondary).http_client().configure_failpoints(
        ("secondary-layer-download-pausable", "off")
    )
    env.get_pageserver(secondary).http_client().tenant_secondary_download(tid, wait_ms=100)

    log.info(f"Waiting for lag to reduce on {secondary}")

    def lag_is_acceptable():
        resp = env.get_pageserver(secondary).http_client().tenant_secondary_status(tid)
        lag = resp["bytes_total"] - resp["bytes_downloaded"]

        if lag > 1 * 1024 * 1024:
            raise Exception(f"Secondary lag not big enough: {lag}")

    wait_until(lag_is_acceptable)

    env.storage_controller.node_configure(primary, {"scheduling": "Active"})

    log.info(f"Starting drain of primary {primary} with non-laggy secondary {secondary}")

    env.storage_controller.retryable_node_operation(
        lambda ps_id: env.storage_controller.node_drain(ps_id), primary, max_attempts=3, backoff=2
    )

    env.storage_controller.poll_node_status(
        primary,
        PageserverAvailability.ACTIVE,
        PageserverSchedulingPolicy.PAUSE_FOR_RESTART,
        max_attempts=6,
        backoff=5,
    )

    locations = env.storage_controller.locate(tid)
    assert len(locations) == 1
    assert locations[0]["node_id"] == secondary


def test_background_operation_cancellation(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    tenant_count = 10
    shard_count_per_tenant = 16
    tenant_ids = []

    for _ in range(0, tenant_count):
        tid = TenantId.generate()
        tenant_ids.append(tid)
        env.create_tenant(
            tid, placement_policy='{"Attached":1}', shard_count=shard_count_per_tenant
        )

    # See sleep comment in the test above.
    time.sleep(2)

    nodes = env.storage_controller.node_list()
    assert len(nodes) == 2

    env.storage_controller.configure_failpoints(("sleepy-drain-loop", "return(2000)"))

    ps_id_to_drain = env.pageservers[0].id

    env.storage_controller.warm_up_all_secondaries()
    env.storage_controller.retryable_node_operation(
        lambda ps_id: env.storage_controller.node_drain(ps_id),
        ps_id_to_drain,
        max_attempts=3,
        backoff=2,
    )

    env.storage_controller.poll_node_status(
        ps_id_to_drain,
        PageserverAvailability.ACTIVE,
        PageserverSchedulingPolicy.DRAINING,
        max_attempts=6,
        backoff=2,
    )

    env.storage_controller.cancel_node_drain(ps_id_to_drain)

    env.storage_controller.poll_node_status(
        ps_id_to_drain,
        PageserverAvailability.ACTIVE,
        PageserverSchedulingPolicy.ACTIVE,
        max_attempts=6,
        backoff=2,
    )


@pytest.mark.parametrize("while_offline", [True, False])
def test_storage_controller_node_deletion(
    neon_env_builder: NeonEnvBuilder,
    compute_reconfigure_listener: ComputeReconfigure,
    while_offline: bool,
):
    """
    Test that deleting a node works & properly reschedules everything that was on the node.
    """
    neon_env_builder.num_pageservers = 3
    neon_env_builder.num_azs = 3
    env = neon_env_builder.init_configs()
    env.start()

    tenant_count = 10
    shard_count_per_tenant = 8
    tenant_ids = []
    for _ in range(0, tenant_count):
        tid = TenantId.generate()
        tenant_ids.append(tid)
        env.create_tenant(
            tid, placement_policy='{"Attached":1}', shard_count=shard_count_per_tenant
        )

    # Sanity check: initial creations should not leave the system in an unstable scheduling state
    assert env.storage_controller.reconcile_all() == 0

    victim = env.pageservers[-1]

    # The procedure a human would follow is:
    # 1. Mark pageserver scheduling=pause
    # 2. Mark pageserver availability=offline to trigger migrations away from it
    # 3. Wait for attachments to all move elsewhere
    # 4. Call deletion API
    # 5. Stop the node.

    env.storage_controller.node_configure(victim.id, {"scheduling": "Pause"})

    if while_offline:
        victim.stop(immediate=True)
        env.storage_controller.node_configure(victim.id, {"availability": "Offline"})

        def assert_shards_migrated():
            counts = get_node_shard_counts(env, tenant_ids)
            elsewhere = sum(v for (k, v) in counts.items() if k != victim.id)
            log.info(f"Shards on nodes other than on victim: {elsewhere}")
            assert elsewhere == tenant_count * shard_count_per_tenant

        wait_until(assert_shards_migrated)

    log.info(f"Deleting pageserver {victim.id}")
    env.storage_controller.node_delete(victim.id)

    if not while_offline:

        def assert_victim_evacuated():
            counts = get_node_shard_counts(env, tenant_ids)
            count = counts[victim.id]
            log.info(f"Shards on node {victim.id}: {count}")
            assert count == 0

        wait_until(assert_victim_evacuated)

    # The node should be gone from the list API
    assert victim.id not in [n["id"] for n in env.storage_controller.node_list()]

    # No tenants should refer to the node in their intent
    for tenant_id in tenant_ids:
        describe = env.storage_controller.tenant_describe(tenant_id)
        for shard in describe["shards"]:
            assert shard["node_attached"] != victim.id
            assert victim.id not in shard["node_secondary"]

    # Reconciles running during deletion should all complete
    env.storage_controller.reconcile_until_idle()

    # Controller should pass its own consistency checks
    env.storage_controller.consistency_check()

    # The node should stay gone across a restart
    env.storage_controller.stop()
    env.storage_controller.start()
    assert victim.id not in [n["id"] for n in env.storage_controller.node_list()]
    env.storage_controller.consistency_check()


@pytest.mark.parametrize("shard_count", [None, 2])
def test_storage_controller_metadata_health(
    neon_env_builder: NeonEnvBuilder,
    shard_count: int | None,
):
    """
    Create three tenants A, B, C.

    Phase 1:
    - A: Post healthy status.
    - B: Post unhealthy status.
    - C: No updates.

    Phase 2:
    - B: Post healthy status.
    - C: Post healthy status.

    Phase 3:
    - A: Post unhealthy status.

    Phase 4:
    - Delete tenant A, metadata health status should be deleted as well.
    """

    def update_and_query_metadata_health(
        env: NeonEnv,
        healthy: list[TenantShardId],
        unhealthy: list[TenantShardId],
        outdated_duration: str = "1h",
    ) -> tuple[set[str], set[str]]:
        """
        Update metadata health. Then list tenant shards with unhealthy and
        outdated metadata health status.
        """
        if healthy or unhealthy:
            env.storage_controller.metadata_health_update(healthy, unhealthy)
        result = env.storage_controller.metadata_health_list_unhealthy()
        unhealthy_res = set(result["unhealthy_tenant_shards"])
        result = env.storage_controller.metadata_health_list_outdated(outdated_duration)
        outdated_res = set(record["tenant_shard_id"] for record in result["health_records"])

        return unhealthy_res, outdated_res

    neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    # Mock tenant (`initial_tenant``) with healthy scrubber scan result
    tenant_a_shard_ids = (
        env.storage_controller.tenant_shard_split(env.initial_tenant, shard_count=shard_count)
        if shard_count is not None
        else [TenantShardId(env.initial_tenant, 0, 0)]
    )

    # Mock tenant with unhealthy scrubber scan result
    tenant_b, _ = env.create_tenant(shard_count=shard_count)
    tenant_b_shard_ids = (
        env.storage_controller.tenant_shard_split(tenant_b, shard_count=shard_count)
        if shard_count is not None
        else [TenantShardId(tenant_b, 0, 0)]
    )

    # Mock tenant that never gets a health update from scrubber
    tenant_c, _ = env.create_tenant(shard_count=shard_count)

    tenant_c_shard_ids = (
        env.storage_controller.tenant_shard_split(tenant_c, shard_count=shard_count)
        if shard_count is not None
        else [TenantShardId(tenant_c, 0, 0)]
    )

    # Metadata health table also updated as tenant shards are created.
    assert env.storage_controller.metadata_health_is_healthy()

    # post "fake" updates to storage controller db

    unhealthy, outdated = update_and_query_metadata_health(
        env, healthy=tenant_a_shard_ids, unhealthy=tenant_b_shard_ids
    )

    log.info(f"After Phase 1: {unhealthy=}, {outdated=}")
    assert len(unhealthy) == len(tenant_b_shard_ids)
    for t in tenant_b_shard_ids:
        assert str(t) in unhealthy
    assert len(outdated) == 0

    unhealthy, outdated = update_and_query_metadata_health(
        env, healthy=tenant_b_shard_ids + tenant_c_shard_ids, unhealthy=[]
    )

    log.info(f"After Phase 2: {unhealthy=}, {outdated=}")
    assert len(unhealthy) == 0
    assert len(outdated) == 0

    unhealthy, outdated = update_and_query_metadata_health(
        env, healthy=[], unhealthy=tenant_a_shard_ids
    )

    log.info(f"After Phase 3: {unhealthy=}, {outdated=}")
    assert len(unhealthy) == len(tenant_a_shard_ids)
    for t in tenant_a_shard_ids:
        assert str(t) in unhealthy
    assert len(outdated) == 0

    # Phase 4: Delete A
    env.storage_controller.pageserver_api().tenant_delete(env.initial_tenant)

    # A's unhealthy metadata health status should be deleted as well.
    assert env.storage_controller.metadata_health_is_healthy()

    # All shards from B and C are not fresh if set outdated duration to 0 seconds.
    unhealthy, outdated = update_and_query_metadata_health(
        env, healthy=[], unhealthy=tenant_a_shard_ids, outdated_duration="0s"
    )
    assert len(unhealthy) == 0
    for t in tenant_b_shard_ids + tenant_c_shard_ids:
        assert str(t) in outdated


def test_storage_controller_step_down(neon_env_builder: NeonEnvBuilder):
    """
    Test the `/control/v1/step_down` storage controller API. Upon receiving such
    a request, the storage controller cancels any on-going reconciles and replies
    with 503 to all requests apart from `/control/v1/step_down`, `/status` and `/metrics`.
    """
    env = neon_env_builder.init_configs()
    env.start()

    tid = TenantId.generate()
    tsid = str(TenantShardId(tid, shard_number=0, shard_count=0))
    env.storage_controller.tenant_create(tid)

    env.storage_controller.reconcile_until_idle()
    env.storage_controller.configure_failpoints(("reconciler-epilogue", "pause"))

    def unpause_failpoint():
        time.sleep(2)
        env.storage_controller.configure_failpoints(("reconciler-epilogue", "off"))

    thread = threading.Thread(target=unpause_failpoint)
    thread.start()

    # Make a change to the tenant config to trigger a slow reconcile
    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)
    virtual_ps_http.update_tenant_config(tid, {"compaction_threshold": 5}, None)
    env.storage_controller.allowed_errors.extend(
        [
            ".*Accepted configuration update but reconciliation failed.*",
            ".*Leader is stepped down instance",
        ]
    )

    observed_state = env.storage_controller.step_down()
    log.info(f"Storage controller stepped down with {observed_state=}")

    thread.join()

    # Validate that we waited for the slow reconcile to complete
    # and updated the observed state in the storcon before stepping down.
    node_id = str(env.pageserver.id)
    assert tsid in observed_state
    assert node_id in observed_state[tsid]["locations"]
    assert "conf" in observed_state[tsid]["locations"][node_id]
    assert "tenant_conf" in observed_state[tsid]["locations"][node_id]["conf"]

    tenant_conf = observed_state[tsid]["locations"][node_id]["conf"]["tenant_conf"]
    assert "compaction_threshold" in tenant_conf
    assert tenant_conf["compaction_threshold"] == 5

    # Validate that we propagated the change to the pageserver
    ps_tenant_conf = env.pageserver.http_client().tenant_config(tid)
    assert "compaction_threshold" in ps_tenant_conf.effective_config
    assert ps_tenant_conf.effective_config["compaction_threshold"] == 5

    # Validate that the storcon attempts to forward the request, but stops.
    # when it realises it is still the current leader.
    with pytest.raises(StorageControllerApiException, match="Leader is stepped down instance"):
        env.storage_controller.tenant_shard_dump()

    # Validate that we can step down multiple times and the observed state
    # doesn't change.
    observed_state_again = env.storage_controller.step_down()
    assert observed_state == observed_state_again

    assert (
        env.storage_controller.get_metric_value(
            "storage_controller_leadership_status", filter={"status": "leader"}
        )
        == 0
    )

    assert (
        env.storage_controller.get_metric_value(
            "storage_controller_leadership_status", filter={"status": "stepped_down"}
        )
        == 1
    )

    assert (
        env.storage_controller.get_metric_value(
            "storage_controller_leadership_status", filter={"status": "candidate"}
        )
        == 0
    )


# This is a copy of NeonEnv.start which injects the instance id and port
# into the call to NeonStorageController.start
def start_env(env: NeonEnv, storage_controller_port: int):
    timeout_in_seconds = 30

    # Storage controller starts first, so that pageserver /re-attach calls don't
    # bounce through retries on startup
    env.storage_controller.start(timeout_in_seconds, 1, storage_controller_port)

    # Wait for storage controller readiness to prevent unnecessary post start-up
    # reconcile.
    env.storage_controller.wait_until_ready()

    # Start up broker, pageserver and all safekeepers
    futs = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=2 + len(env.pageservers) + len(env.safekeepers)
    ) as executor:
        futs.append(
            executor.submit(lambda: env.broker.start() or None)
        )  # The `or None` is for the linter

        for pageserver in env.pageservers:
            futs.append(
                executor.submit(
                    lambda ps=pageserver: ps.start(timeout_in_seconds=timeout_in_seconds)  # type: ignore[misc]
                )
            )

        for safekeeper in env.safekeepers:
            futs.append(
                executor.submit(
                    lambda sk=safekeeper: sk.start(timeout_in_seconds=timeout_in_seconds)  # type: ignore[misc]
                )
            )

    for f in futs:
        f.result()


@pytest.mark.parametrize("step_down_times_out", [False, True])
def test_storage_controller_leadership_transfer(
    neon_env_builder: NeonEnvBuilder,
    storage_controller_proxy: StorageControllerProxy,
    port_distributor: PortDistributor,
    step_down_times_out: bool,
):
    neon_env_builder.auth_enabled = True

    neon_env_builder.num_pageservers = 3

    neon_env_builder.storage_controller_config = {
        "database_url": f"127.0.0.1:{port_distributor.get_port()}",
        "start_as_candidate": True,
    }

    neon_env_builder.storage_controller_port_override = storage_controller_proxy.port()

    storage_controller_1_port = port_distributor.get_port()
    storage_controller_2_port = port_distributor.get_port()

    storage_controller_proxy.route_to(f"http://127.0.0.1:{storage_controller_1_port}")

    env = neon_env_builder.init_configs()
    start_env(env, storage_controller_1_port)

    assert (
        env.storage_controller.get_leadership_status() == StorageControllerLeadershipStatus.LEADER
    )
    leader = env.storage_controller.get_leader()
    assert leader["address"] == f"http://127.0.0.1:{storage_controller_1_port}/"

    if step_down_times_out:
        env.storage_controller.configure_failpoints(
            ("sleep-on-step-down-handling", "return(10000)")
        )
        env.storage_controller.allowed_errors.append(".*request was dropped before completing.*")

    tenant_count = 2
    shard_count = 4
    tenants = set(TenantId.generate() for _ in range(0, tenant_count))

    for tid in tenants:
        env.storage_controller.tenant_create(
            tid, shard_count=shard_count, placement_policy={"Attached": 1}
        )
    env.storage_controller.reconcile_until_idle()

    env.storage_controller.start(
        timeout_in_seconds=30, instance_id=2, base_port=storage_controller_2_port
    )

    if not step_down_times_out:

        def previous_stepped_down():
            assert (
                env.storage_controller.get_leadership_status()
                == StorageControllerLeadershipStatus.STEPPED_DOWN
            )

        wait_until(previous_stepped_down)

    storage_controller_proxy.route_to(f"http://127.0.0.1:{storage_controller_2_port}")

    def new_becomes_leader():
        assert (
            env.storage_controller.get_leadership_status()
            == StorageControllerLeadershipStatus.LEADER
        )

    wait_until(new_becomes_leader)
    leader = env.storage_controller.get_leader()
    assert leader["address"] == f"http://127.0.0.1:{storage_controller_2_port}/"

    env.storage_controller.wait_until_ready()
    env.storage_controller.consistency_check()

    if not step_down_times_out:
        # Check that the stepped down instance forwards requests
        # to the new leader while it's still running.
        storage_controller_proxy.route_to(f"http://127.0.0.1:{storage_controller_1_port}")
        env.storage_controller.tenant_shard_dump()
        env.storage_controller.node_configure(env.pageservers[0].id, {"scheduling": "Pause"})
        status = env.storage_controller.node_status(env.pageservers[0].id)
        assert status["scheduling"] == "Pause"

    if step_down_times_out:
        env.storage_controller.allowed_errors.extend(
            [
                ".*Leader.*did not respond to step-down request.*",
                ".*Send step down request failed.*",
                ".*Send step down request still failed.*",
            ]
        )


def test_storage_controller_ps_restarted_during_drain(neon_env_builder: NeonEnvBuilder):
    # single unsharded tenant, two locations
    neon_env_builder.num_pageservers = 2

    env = neon_env_builder.init_start()

    env.storage_controller.tenant_policy_update(env.initial_tenant, {"placement": {"Attached": 1}})
    env.storage_controller.reconcile_until_idle()

    attached_id = int(env.storage_controller.locate(env.initial_tenant)[0]["node_id"])
    attached = next(ps for ps in env.pageservers if ps.id == attached_id)

    def attached_is_draining():
        details = env.storage_controller.node_status(attached.id)
        assert details["scheduling"] == "Draining"

    env.storage_controller.configure_failpoints(("sleepy-drain-loop", "return(10000)"))
    env.storage_controller.node_drain(attached.id)

    wait_until(attached_is_draining)

    attached.restart()

    # we are unable to reconfigure node while the operation is still ongoing
    with pytest.raises(
        StorageControllerApiException,
        match="Precondition failed: Ongoing background operation forbids configuring: drain.*",
    ):
        env.storage_controller.node_configure(attached.id, {"scheduling": "Pause"})
    with pytest.raises(
        StorageControllerApiException,
        match="Precondition failed: Ongoing background operation forbids configuring: drain.*",
    ):
        env.storage_controller.node_configure(attached.id, {"availability": "Offline"})

    env.storage_controller.cancel_node_drain(attached.id)

    def reconfigure_node_again():
        env.storage_controller.node_configure(attached.id, {"scheduling": "Pause"})

    # allow for small delay between actually having cancelled and being able reconfigure again
    wait_until(reconfigure_node_again)


def test_storage_controller_timeline_crud_race(neon_env_builder: NeonEnvBuilder):
    """
    The storage controller is meant to handle the case where a timeline CRUD operation races
    with a generation-incrementing change to the tenant: this should trigger a retry so that
    the operation lands on the highest-generation'd tenant location.
    """
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()
    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(tenant_id)

    # Set up a failpoint so that a timeline creation will be very slow
    failpoint = "timeline-creation-after-uninit"
    for ps in env.pageservers:
        ps.http_client().configure_failpoints((failpoint, "sleep(10000)"))

    # Start a timeline creation in the background
    create_timeline_id = TimelineId.generate()
    futs = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=2 + len(env.pageservers) + len(env.safekeepers)
    ) as executor:
        futs.append(
            executor.submit(
                env.storage_controller.pageserver_api(
                    retries=Retry(
                        status=0,
                        connect=0,  # Disable retries: we want to see the 503
                    )
                ).timeline_create,
                PgVersion.NOT_SET,
                tenant_id,
                create_timeline_id,
            )
        )

        def has_hit_failpoint():
            assert any(
                ps.log_contains(f"at failpoint {failpoint}") is not None for ps in env.pageservers
            )

        wait_until(has_hit_failpoint)

        # Migrate the tenant while the timeline creation is in progress: this migration will complete once it
        # can detach from the old pageserver, which will happen once the failpoint completes.
        env.storage_controller.tenant_shard_migrate(
            TenantShardId(tenant_id, 0, 0), env.pageservers[1].id
        )

        with pytest.raises(PageserverApiException, match="Tenant attachment changed, please retry"):
            futs[0].result(timeout=20)

    # Timeline creation should work when there isn't a concurrent migration, even though it's
    # slow (our failpoint is still enabled)
    env.storage_controller.pageserver_api(
        retries=Retry(
            status=0,
            connect=0,  # Disable retries: we want to see the 503
        )
    ).timeline_create(PgVersion.NOT_SET, tenant_id, create_timeline_id)


def test_storage_controller_validate_during_migration(neon_env_builder: NeonEnvBuilder):
    """
    A correctness edge case: while we are live migrating and a shard's generation is
    visible to the Reconciler but not to the central Service, the generation validation
    API should still prevent stale generations from doing deletions.
    """
    neon_env_builder.num_pageservers = 2
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    env = neon_env_builder.init_configs()
    env.start()

    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": 128 * 1024,
        "compaction_threshold": 1,
        "compaction_target_size": 128 * 1024,
        # disable background compaction and GC. We invoke it manually when we want it to happen.
        "gc_period": "0s",
        "compaction_period": "0s",
    }

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    env.create_tenant(tenant_id, timeline_id)
    env.storage_controller.pageserver_api().set_tenant_config(tenant_id, TENANT_CONF)

    # Write enough data that a compaction would do some work (deleting some L0s)
    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(64)
    for _i in range(0, 2):
        workload.churn_rows(64, upload=False)

    # Upload but don't compact
    origin_pageserver = env.get_tenant_pageserver(tenant_id)
    assert origin_pageserver is not None
    dest_ps_id = [p.id for p in env.pageservers if p.id != origin_pageserver.id][0]
    origin_pageserver.http_client().timeline_checkpoint(
        tenant_id, timeline_id, wait_until_uploaded=True, compact=False
    )

    # Start a compaction that will pause on a failpoint.
    compaction_failpoint = "before-upload-index-pausable"
    origin_pageserver.http_client().configure_failpoints((compaction_failpoint, "pause"))

    # This failpoint can also cause migration code to time out trying to politely flush
    # during migrations
    origin_pageserver.allowed_errors.append(".*Timed out waiting for flush to remote storage.*")

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            compact_fut = executor.submit(
                origin_pageserver.http_client().timeline_compact,
                tenant_id,
                timeline_id,
                wait_until_uploaded=True,
            )

            # Let the compaction start and then get stuck uploading an index: when we live migrate, the new generation's
            # index will be initialized from the pre-compaction index, referencing layers that the compaction will try to delete
            def has_hit_compaction_failpoint():
                assert origin_pageserver.log_contains(f"at failpoint {compaction_failpoint}")

            wait_until(has_hit_compaction_failpoint)

            # While the compaction is running, start a live migration which will pause long enough for the compaction to sleep,
            # after incrementing generation and attaching the new location
            migration_failpoint = "reconciler-live-migrate-post-notify"
            env.storage_controller.configure_failpoints((migration_failpoint, "pause"))
            migrate_fut = executor.submit(
                env.storage_controller.tenant_shard_migrate,
                TenantShardId(tenant_id, 0, 0),
                dest_ps_id,
            )

            def has_hit_migration_failpoint():
                assert env.storage_controller.log_contains(f"at failpoint {migration_failpoint}")

            # Long wait because the migration will have to time out during transition to AttachedStale
            # before it reaches this point.  The timeout is because the AttachedStale transition includes
            # a flush of remote storage, and if the compaction already enqueued an index upload this cannot
            # make progress.
            wait_until(has_hit_migration_failpoint, timeout=60)

            # Origin pageserver has succeeded with compaction before the migration completed. It has done all the writes it wanted to do in its own (stale) generation
            origin_pageserver.http_client().configure_failpoints((compaction_failpoint, "off"))
            compact_fut.result()
            origin_pageserver.http_client().deletion_queue_flush(execute=True)

            # Eventually migration completes
            env.storage_controller.configure_failpoints((migration_failpoint, "off"))
            migrate_fut.result()
    except:
        # Always disable 'pause' failpoints, even on failure, to avoid hanging in shutdown
        env.storage_controller.configure_failpoints((migration_failpoint, "off"))
        origin_pageserver.http_client().configure_failpoints((compaction_failpoint, "off"))
        raise

    # Ensure the destination of the migration writes an index, so that if it has corrupt state that is
    # visible to the scrubber.
    workload.write_rows(1, upload=False)
    env.get_pageserver(dest_ps_id).http_client().timeline_checkpoint(
        tenant_id, timeline_id, wait_until_uploaded=True, compact=False
    )

    # The destination of the live migration would now have a corrupt index (referencing deleted L0s) if
    # the controller had not properly applied validation rules.
    healthy, _summary = env.storage_scrubber.scan_metadata()
    try:
        log.info(f"scrubbed, healthy={healthy}")
        assert healthy
    except:
        # On failures, we want to report them FAIL during the test, not as ERROR during teardown
        neon_env_builder.enable_scrub_on_exit = False
        raise


class MigrationFailpoints(Enum):
    # While only the origin is attached
    PRE_GENERATION_INC = "reconciler-live-migrate-pre-generation-inc"
    # While only the origin is attached and the db was updated to
    # point to the new location
    PRE_AWAIT_LSN = "reconciler-live-migrate-pre-await-lsn"
    # While both locations are attached
    POST_NOTIFY = "reconciler-live-migrate-post-notify"
    # While only the destination is attached
    POST_DETACH = "reconciler-live-migrate-post-detach"


@pytest.mark.parametrize(
    "migration_failpoint",
    [
        MigrationFailpoints.PRE_GENERATION_INC,
        MigrationFailpoints.POST_NOTIFY,
        MigrationFailpoints.POST_DETACH,
    ],
)
def test_storage_controller_proxy_during_migration(
    neon_env_builder: NeonEnvBuilder, migration_failpoint: MigrationFailpoints
):
    """
    If we send a proxied GET request to the controller during a migration, it should route
    the request to whichever pageserver was most recently issued a generation.

    Reproducer for https://github.com/neondatabase/neon/issues/9062
    """
    neon_env_builder.num_pageservers = 2
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    neon_env_builder.storage_controller_config = {
        # Publish long reconcile metric early
        "long_reconcile_threshold": "5s",
    }

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    env.create_tenant(tenant_id, timeline_id)

    # The test stalls a reconcile on purpose to check if the long running
    # reconcile alert fires.
    env.storage_controller.allowed_errors.extend(
        [".*Reconcile passed the long running threshold.*"]
    )

    # Activate a failpoint that will cause live migration to get stuck _after_ the generation has been issued
    # to the new pageserver: this should result in requests routed to the new pageserver.
    env.storage_controller.configure_failpoints((migration_failpoint.value, "pause"))

    origin_pageserver = env.get_tenant_pageserver(tenant_id)
    dest_ps_id = [p.id for p in env.pageservers if p.id != origin_pageserver.id][0]

    def long_migration_metric_published():
        assert (
            env.storage_controller.get_metric_value(
                "storage_controller_reconcile_long_running_total",
                filter={"tenant_id": str(tenant_id), "shard_number": "0"},
            )
            == 1
        )

    def assert_long_migration_metric_not_published():
        assert (
            env.storage_controller.get_metric_value(
                "storage_controller_reconcile_long_running_total",
                filter={"tenant_id": str(tenant_id), "shard_number": "0"},
            )
            is None
        )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            migrate_fut = executor.submit(
                env.storage_controller.tenant_shard_migrate,
                TenantShardId(tenant_id, 0, 0),
                dest_ps_id,
            )

            def has_hit_migration_failpoint():
                expr = f"at failpoint {str(migration_failpoint.value)}"
                log.info(expr)
                assert env.storage_controller.log_contains(expr)

            wait_until(has_hit_migration_failpoint)

            # This request should be routed to whichever pageserver holds the highest generation
            tenant_info = env.storage_controller.pageserver_api().tenant_status(
                tenant_id,
            )

            if migration_failpoint in (
                MigrationFailpoints.POST_NOTIFY,
                MigrationFailpoints.POST_DETACH,
            ):
                # We expect request to land on the destination
                assert tenant_info["generation"] == 2
            elif migration_failpoint == MigrationFailpoints.PRE_GENERATION_INC:
                # We expect request to land on the origin
                assert tenant_info["generation"] == 1

            wait_until(long_migration_metric_published)

            # Eventually migration completes
            env.storage_controller.configure_failpoints((migration_failpoint.value, "off"))
            migrate_fut.result()

            assert_long_migration_metric_not_published()

    except:
        # Always disable 'pause' failpoints, even on failure, to avoid hanging in shutdown
        env.storage_controller.configure_failpoints((migration_failpoint.value, "off"))
        raise


@run_only_on_default_postgres("this is like a 'unit test' against storcon db")
def test_safekeeper_deployment_time_update(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_configs()
    env.start()

    fake_id = 5

    target = env.storage_controller

    assert target.get_safekeeper(fake_id) is None

    assert len(target.get_safekeepers()) == 0

    body = {
        "active": True,
        "id": fake_id,
        "created_at": "2023-10-25T09:11:25Z",
        "updated_at": "2024-08-28T11:32:43Z",
        "region_id": "aws-us-east-2",
        "host": "safekeeper-333.us-east-2.aws.neon.build",
        "port": 6401,
        "http_port": 7676,
        "version": 5957,
        "availability_zone_id": "us-east-2b",
    }

    target.on_safekeeper_deploy(fake_id, body)

    inserted = target.get_safekeeper(fake_id)
    assert inserted is not None
    assert target.get_safekeepers() == [inserted]
    assert eq_safekeeper_records(body, inserted)

    # error out if pk is changed (unexpected)
    with pytest.raises(StorageControllerApiException) as exc:
        different_pk = dict(body)
        different_pk["id"] = 4
        assert different_pk["id"] != body["id"]
        target.on_safekeeper_deploy(fake_id, different_pk)
    assert exc.value.status_code == 400

    inserted_again = target.get_safekeeper(fake_id)
    assert target.get_safekeepers() == [inserted_again]
    assert inserted_again is not None
    assert eq_safekeeper_records(inserted, inserted_again)

    # the most common case, version goes up:
    assert isinstance(body["version"], int)
    body["version"] += 1
    target.on_safekeeper_deploy(fake_id, body)
    inserted_now = target.get_safekeeper(fake_id)
    assert target.get_safekeepers() == [inserted_now]
    assert inserted_now is not None

    assert eq_safekeeper_records(body, inserted_now)

    # some small tests for the scheduling policy querying and returning APIs
    newest_info = target.get_safekeeper(inserted["id"])
    assert newest_info
    assert newest_info["scheduling_policy"] == "Pause"
    target.safekeeper_scheduling_policy(inserted["id"], "Decomissioned")
    newest_info = target.get_safekeeper(inserted["id"])
    assert newest_info
    assert newest_info["scheduling_policy"] == "Decomissioned"
    # Ensure idempotency
    target.safekeeper_scheduling_policy(inserted["id"], "Decomissioned")


def eq_safekeeper_records(a: dict[str, Any], b: dict[str, Any]) -> bool:
    compared = [dict(a), dict(b)]

    masked_keys = ["created_at", "updated_at", "active", "scheduling_policy"]

    for d in compared:
        # keep deleting these in case we are comparing the body as it will be uploaded by real scripts
        for key in masked_keys:
            if key in d:
                del d[key]

    return compared[0] == compared[1]


@run_only_on_default_postgres("this is like a 'unit test' against storcon db")
def test_shard_preferred_azs(neon_env_builder: NeonEnvBuilder):
    def assign_az(ps_cfg):
        az = f"az-{ps_cfg['id'] % 2}"
        log.info("Assigned AZ {az}")
        ps_cfg["availability_zone"] = az

    neon_env_builder.pageserver_config_override = assign_az
    neon_env_builder.num_pageservers = 4
    env = neon_env_builder.init_configs()
    env.start()

    tids = [TenantId.generate() for _ in range(0, 3)]
    for tid in tids:
        env.storage_controller.tenant_create(tid)

        shards = env.storage_controller.tenant_describe(tid)["shards"]
        assert len(shards) == 1
        attached_to = shards[0]["node_attached"]
        expected_az = env.get_pageserver(attached_to).az_id

        assert shards[0]["preferred_az_id"] == expected_az

    # When all other schedule scoring parameters are equal, tenants should round-robin on AZs
    assert env.storage_controller.tenant_describe(tids[0])["shards"][0]["preferred_az_id"] == "az-0"
    assert env.storage_controller.tenant_describe(tids[1])["shards"][0]["preferred_az_id"] == "az-1"
    assert env.storage_controller.tenant_describe(tids[2])["shards"][0]["preferred_az_id"] == "az-0"

    # Try modifying preferred AZ
    updated = env.storage_controller.set_preferred_azs(
        {TenantShardId(tid, 0, 0): "az-0" for tid in tids}
    )

    assert set(updated) == set([TenantShardId(tid, 0, 0) for tid in tids])

    for tid in tids:
        shards = env.storage_controller.tenant_describe(tid)["shards"]
        assert len(shards) == 1
        assert shards[0]["preferred_az_id"] == "az-0"

    # Having modified preferred AZ, we should get moved there
    env.storage_controller.reconcile_until_idle(max_interval=0.1)
    for tid in tids:
        shard = env.storage_controller.tenant_describe(tid)["shards"][0]
        attached_to = shard["node_attached"]
        attached_in_az = env.get_pageserver(attached_to).az_id
        assert shard["preferred_az_id"] == attached_in_az == "az-0"

    env.storage_controller.tenant_shard_split(tids[0], shard_count=2)
    env.storage_controller.reconcile_until_idle(max_interval=0.1)
    shards = env.storage_controller.tenant_describe(tids[0])["shards"]
    assert len(shards) == 2
    for shard in shards:
        attached_to = shard["node_attached"]
        attached_in_az = env.get_pageserver(attached_to).az_id
        assert shard["preferred_az_id"] == attached_in_az == "az-0"


@run_only_on_default_postgres("Postgres version makes no difference here")
@pytest.mark.parametrize(
    "migration_failpoint",
    [
        MigrationFailpoints.PRE_GENERATION_INC,
        MigrationFailpoints.PRE_AWAIT_LSN,
        MigrationFailpoints.POST_NOTIFY,
        MigrationFailpoints.POST_DETACH,
    ],
)
def test_timeline_delete_mid_live_migration(neon_env_builder: NeonEnvBuilder, migration_failpoint):
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.storage_controller.tenant_create(tenant_id, placement_policy={"Attached": 1})
    env.storage_controller.pageserver_api().timeline_create(
        pg_version=PgVersion.NOT_SET, tenant_id=tenant_id, new_timeline_id=timeline_id
    )

    shard_zero = TenantShardId(tenant_id, 0, 0)
    locations = env.storage_controller.get_tenants_placement()[str(shard_zero)]

    assert locations["observed"] == locations["intent"]
    assert locations["observed"]["attached"] is not None
    assert len(locations["observed"]["secondary"]) > 0

    attached_location = locations["observed"]["attached"]
    secondary_location = locations["observed"]["secondary"][0]

    env.storage_controller.configure_failpoints((migration_failpoint.value, "pause"))

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            migrate_fut = executor.submit(
                env.storage_controller.tenant_shard_migrate,
                shard_zero,
                secondary_location,
            )

            def has_hit_migration_failpoint():
                expr = f"at failpoint {migration_failpoint.value}"
                log.info(expr)
                assert env.storage_controller.log_contains(expr)

            wait_until(has_hit_migration_failpoint)

            env.storage_controller.pageserver_api().timeline_delete(
                tenant_id=tenant_id, timeline_id=timeline_id
            )

            # Eventually migration completes
            env.storage_controller.configure_failpoints((migration_failpoint.value, "off"))
            migrate_fut.result()

            # Ensure that we detached from the old attached location
            with pytest.raises(PageserverApiException) as exc:
                env.get_pageserver(attached_location).http_client().timeline_list(tenant_id)
            assert exc.value.status_code == 404

            # Ensure the timeline is not present on the new attached location
            client = env.get_pageserver(secondary_location).http_client()
            assert timeline_id not in {
                TimelineId(b["timeline_id"]) for b in client.timeline_list(tenant_id)
            }, f"deleted timeline found on {secondary_location}"

    except:
        # Always disable 'pause' failpoints, even on failure, to avoid hanging in shutdown
        env.storage_controller.configure_failpoints((migration_failpoint.value, "off"))
        raise


@run_only_on_default_postgres("Postgres version makes no difference here")
@pytest.mark.parametrize(
    "migration_failpoint",
    [
        MigrationFailpoints.PRE_GENERATION_INC,
        MigrationFailpoints.POST_NOTIFY,
        MigrationFailpoints.POST_DETACH,
    ],
)
def test_multi_attached_timeline_creation(neon_env_builder: NeonEnvBuilder, migration_failpoint):
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(tenant_id, placement_policy={"Attached": 1})

    shard_zero = TenantShardId(tenant_id, 0, 0)
    locations = env.storage_controller.get_tenants_placement()[str(shard_zero)]

    assert locations["observed"] == locations["intent"]
    assert locations["observed"]["attached"] is not None
    assert len(locations["observed"]["secondary"]) > 0

    attached_location = locations["observed"]["attached"]
    secondary_location = locations["observed"]["secondary"][0]

    env.storage_controller.configure_failpoints((migration_failpoint.value, "pause"))

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            migrate_fut = executor.submit(
                env.storage_controller.tenant_shard_migrate,
                shard_zero,
                secondary_location,
            )

            def has_hit_migration_failpoint():
                expr = f"at failpoint {migration_failpoint.value}"
                log.info(expr)
                assert env.storage_controller.log_contains(expr)

            wait_until(has_hit_migration_failpoint)

            timeline_id = TimelineId.generate()
            env.storage_controller.pageserver_api().timeline_create(
                pg_version=PgVersion.NOT_SET, tenant_id=tenant_id, new_timeline_id=timeline_id
            )

            # Timeline creation only goes to the origin.
            if migration_failpoint == MigrationFailpoints.PRE_GENERATION_INC:
                client = env.get_pageserver(attached_location).http_client()
                assert timeline_id in {
                    TimelineId(b["timeline_id"]) for b in client.timeline_list(tenant_id)
                }, f"new timeline not found on {attached_location}"

                with pytest.raises(PageserverApiException) as exc:
                    env.get_pageserver(secondary_location).http_client().timeline_list(tenant_id)
                assert exc.value.status_code == 404

            # Timeline creations goes to both attached locations
            if migration_failpoint == MigrationFailpoints.POST_NOTIFY:
                for node_id in [attached_location, secondary_location]:
                    client = env.get_pageserver(node_id).http_client()
                    assert timeline_id in {
                        TimelineId(b["timeline_id"]) for b in client.timeline_list(tenant_id)
                    }, f"new timeline not found on {node_id}"

            # Timeline creation goes both locations, but storcon gets a 404 from the origin
            # which it ignores.
            if migration_failpoint == MigrationFailpoints.POST_DETACH:
                client = env.get_pageserver(secondary_location).http_client()
                assert timeline_id in {
                    TimelineId(b["timeline_id"]) for b in client.timeline_list(tenant_id)
                }, f"new timeline not found on {attached_location}"

                with pytest.raises(PageserverApiException) as exc:
                    env.get_pageserver(attached_location).http_client().timeline_list(tenant_id)
                assert exc.value.status_code == 404

            # Eventually migration completes
            env.storage_controller.configure_failpoints((migration_failpoint.value, "off"))
            migrate_fut.result()

            # Ensure that we detached from the old attached location
            with pytest.raises(PageserverApiException) as exc:
                env.get_pageserver(attached_location).http_client().timeline_list(tenant_id)
            assert exc.value.status_code == 404
    except:
        # Always disable 'pause' failpoints, even on failure, to avoid hanging in shutdown
        env.storage_controller.configure_failpoints((migration_failpoint.value, "off"))
        raise


@run_only_on_default_postgres("Postgres version makes no difference here")
def test_storage_controller_detached_stopped(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test that detaching a tenant while it has scheduling policy set to Paused or Stop works
    """

    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    neon_env_builder.num_pageservers = 1

    env = neon_env_builder.init_configs()
    env.start()
    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        tenant_id,
        shard_count=1,
    )

    assert len(env.pageserver.http_client().tenant_list_locations()["tenant_shards"]) == 1

    # Disable scheduling: ordinarily this would prevent the tenant's configuration being
    # reconciled to pageservers, but this should be overridden when detaching.
    env.storage_controller.allowed_errors.append(".*Scheduling is disabled by policy.*")
    env.storage_controller.tenant_policy_update(
        tenant_id,
        {"scheduling": "Stop"},
    )

    env.storage_controller.consistency_check()

    # Detach the tenant
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )
    env.storage_controller.reconcile_until_idle()
    env.storage_controller.consistency_check()

    # Confirm the detach happened
    assert env.pageserver.http_client().tenant_list_locations()["tenant_shards"] == []


@run_only_on_default_postgres("Postgres version makes no difference here")
def test_storage_controller_detach_lifecycle(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test that detached tenants are handled properly through their lifecycle: getting dropped
    from memory when detached, then getting loaded back on-demand.
    """

    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    neon_env_builder.num_pageservers = 1

    env = neon_env_builder.init_configs()
    env.start()
    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()
    env.storage_controller.tenant_create(
        tenant_id,
        shard_count=1,
    )
    virtual_ps_http.timeline_create(PgVersion.NOT_SET, tenant_id, timeline_id)

    remote_prefix = "/".join(
        (
            "tenants",
            str(tenant_id),
        )
    )
    # We will later check data is gone after deletion, so as a control check that it is present to begin with
    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=remote_prefix,
    )

    assert len(env.pageserver.http_client().tenant_list_locations()["tenant_shards"]) == 1
    assert len(env.storage_controller.tenant_list()) == 1

    # Detach the tenant
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )
    # Ensure reconciles are done (the one we do inline in location_conf is advisory and if it takes too long that API just succeeds anyway)
    env.storage_controller.reconcile_until_idle()
    env.storage_controller.consistency_check()

    # Confirm the detach happened on pageserver
    assert env.pageserver.http_client().tenant_list_locations()["tenant_shards"] == []
    # Confirm the tenant is not in memory on the controller
    assert env.storage_controller.tenant_list() == []

    # The detached tenant does not get loaded into memory across a controller restart
    env.storage_controller.stop()
    env.storage_controller.start()
    assert env.storage_controller.tenant_list() == []
    env.storage_controller.consistency_check()

    # The detached tenant can be re-attached
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "AttachedSingle",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )
    assert len(env.pageserver.http_client().tenant_list_locations()["tenant_shards"]) == 1
    assert len(env.storage_controller.tenant_list()) == 1
    env.storage_controller.consistency_check()

    # Detach it again before doing deletion
    virtual_ps_http.tenant_location_conf(
        tenant_id,
        {
            "mode": "Detached",
            "secondary_conf": None,
            "tenant_conf": {},
            "generation": None,
        },
    )
    env.storage_controller.reconcile_until_idle()
    env.storage_controller.consistency_check()

    # A detached tenant can be deleted
    virtual_ps_http.tenant_delete(tenant_id)

    # Such deletions really work (empty remote storage)
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix=remote_prefix,
    )


@run_only_on_default_postgres("Postgres version makes no difference here")
def test_storage_controller_node_flap_detach_race(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Reproducer for https://github.com/neondatabase/neon/issues/10253.

    When a node's availability flaps, the reconciliations spawned by the node
    going offline may race with the reconciliation done when then node comes
    back online.
    """
    neon_env_builder.num_pageservers = 4

    env = neon_env_builder.init_configs()
    env.start()

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        tenant_id,
        shard_count=2,
    )
    env.storage_controller.reconcile_until_idle()

    stopped_nodes = [s["node_id"] for s in env.storage_controller.locate(tenant_id)]

    def has_hit_failpoint(failpoint: str, offset: LogCursor | None = None) -> LogCursor:
        res = env.storage_controller.log_contains(f"at failpoint {failpoint}", offset=offset)
        assert res
        return res[1]

    # Stop the nodes which host attached shards.
    # This will trigger reconciliations which pause before incrmenenting the generation,
    # and, more importantly, updating the `generation_pageserver` of the shards.
    env.storage_controller.configure_failpoints(("reconciler-pre-increment-generation", "pause"))
    for node_id in stopped_nodes:
        env.get_pageserver(node_id).stop(immediate=True)

    def failure_handled() -> LogCursor:
        stop_offset = None

        for node_id in stopped_nodes:
            res = env.storage_controller.log_contains(f"node {node_id} going offline")
            assert res
            stop_offset = res[1]

        assert stop_offset
        return stop_offset

    offset = wait_until(failure_handled)

    # Now restart the nodes and make them pause before marking themselves as available
    # or running the activation reconciliation.
    env.storage_controller.configure_failpoints(("heartbeat-pre-node-state-configure", "pause"))

    for node_id in stopped_nodes:
        env.get_pageserver(node_id).start(await_active=False)

    offset = wait_until(
        lambda: has_hit_failpoint("heartbeat-pre-node-state-configure", offset=offset)
    )

    # The nodes have restarted and are waiting to perform activaction reconciliation.
    # Unpause the initial reconciliation triggered by the nodes going offline.
    # It will attempt to detach from the old location, but notice that the old location
    # is not yet available, and then stop before processing the results of the reconciliation.
    env.storage_controller.configure_failpoints(("reconciler-epilogue", "pause"))
    env.storage_controller.configure_failpoints(("reconciler-pre-increment-generation", "off"))

    offset = wait_until(lambda: has_hit_failpoint("reconciler-epilogue", offset=offset))

    # Let the nodes perform activation reconciliation while still holding up processing the result
    # from the initial reconcile triggered by going offline.
    env.storage_controller.configure_failpoints(("heartbeat-pre-node-state-configure", "off"))

    def activate_reconciliation_done():
        for node_id in stopped_nodes:
            assert env.storage_controller.log_contains(
                f"Node {node_id} transition to active", offset=offset
            )

    wait_until(activate_reconciliation_done)

    # Finally, allow the initial reconcile to finish up.
    env.storage_controller.configure_failpoints(("reconciler-epilogue", "off"))

    # Give things a chance to settle and validate that no stale locations exist
    env.storage_controller.reconcile_until_idle()

    def validate_locations():
        shard_locations = defaultdict(list)
        for ps in env.pageservers:
            locations = ps.http_client().tenant_list_locations()["tenant_shards"]
            for loc in locations:
                shard_locations[loc[0]].append(
                    {"generation": loc[1]["generation"], "mode": loc[1]["mode"], "node": ps.id}
                )

        log.info(f"Shard locations: {shard_locations}")

        attached_locations = {
            k: list(filter(lambda loc: loc["mode"] == "AttachedSingle", v))
            for k, v in shard_locations.items()
        }

        for shard, locs in attached_locations.items():
            assert len(locs) == 1, f"{shard} has {len(locs)} attached locations"

    wait_until(validate_locations, timeout=10)
