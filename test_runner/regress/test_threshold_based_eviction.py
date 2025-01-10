from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload,
)
from fixtures.pageserver.http import LayerMapInfo
from fixtures.remote_storage import RemoteStorageKind
from pytest_httpserver import HTTPServer

if TYPE_CHECKING:
    from fixtures.httpserver import ListenAddress

# NB: basic config change tests are in test_tenant_conf.py


def test_threshold_based_eviction(
    httpserver: HTTPServer,
    httpserver_listen_address: ListenAddress,
    pg_bin: PgBin,
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    # Start with metrics collection enabled, so that the eviction task
    # imitates its accesses. We'll use a non-existent endpoint to make it fail.
    # The synthetic size calculation will run regardless.
    host, port = httpserver_listen_address
    neon_env_builder.pageserver_config_override = f"""
        metric_collection_interval="1s"
        synthetic_size_calculation_interval="2s"
        metric_collection_endpoint="http://{host}:{port}/nonexistent"
    """
    metrics_refused_log_line = (
        ".*metrics_collection:.* upload consumption_metrics (still failed|failed, will retry).*"
    )
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(
        [
            metrics_refused_log_line,
            # these can happen whenever we run consumption metrics collection
            r".*failed to calculate logical size at \S+: cancelled",
            r".*failed to calculate synthetic size for tenant \S+: failed to calculate some logical_sizes",
        ]
    )

    tenant_id, timeline_id = env.initial_tenant, env.initial_timeline

    ps_http = env.pageserver.http_client()
    vps_http = env.storage_controller.pageserver_api()
    assert vps_http.tenant_config(tenant_id).effective_config["eviction_policy"] is None

    eviction_threshold = 10
    eviction_period = 2
    vps_http.set_tenant_config(
        tenant_id,
        {
            "eviction_policy": {
                "kind": "LayerAccessThreshold",
                "threshold": f"{eviction_threshold}s",
                "period": f"{eviction_period}s",
            },
        },
    )
    assert vps_http.tenant_config(tenant_id).effective_config["eviction_policy"] == {
        "kind": "LayerAccessThreshold",
        "threshold": f"{eviction_threshold}s",
        "period": f"{eviction_period}s",
    }

    # restart because changing tenant config is not instant
    env.pageserver.restart()

    assert vps_http.tenant_config(tenant_id).effective_config["eviction_policy"] == {
        "kind": "LayerAccessThreshold",
        "threshold": f"{eviction_threshold}s",
        "period": f"{eviction_period}s",
    }

    # create a bunch of L1s, only the least of which will need to be resident
    compaction_threshold = 3  # create L1 layers quickly
    vps_http.update_tenant_config(
        tenant_id,
        inserts={
            # Disable gc and compaction to avoid on-demand downloads from their side.
            # The only on-demand downloads should be from the eviction tasks's "imitate access" functions.
            "gc_period": "0s",
            "compaction_period": "0s",
            # low checkpoint_distance so that pgbench creates many layers
            "checkpoint_distance": 1024**2,
            # Low compaction target size to create many L1's with tight key ranges.
            # This is so that the "imitate access" don't download all the layers.
            "compaction_target_size": 1 * 1024**2,  # all keys into one L1
            # Turn L0's into L1's fast.
            "compaction_threshold": compaction_threshold,
            # Prevent compaction from collapsing L1 delta layers into image layers. We want many layers here.
            "image_creation_threshold": 100,
            # Much larger so that synthetic size caluclation worker, which is part of metric collection,
            # computes logical size for initdb_lsn every time, instead of some moving lsn as we insert data.
            # This makes the set of downloaded layers predictable,
            # thereby allowing the residence statuses to stabilize below.
            "gc_horizon": 1024**4,
        },
    )

    # create a bunch of layers
    with env.endpoints.create_start("main", tenant_id=tenant_id) as pg:
        pg_bin.run(["pgbench", "-i", "-I", "dtGvp", "-s", "3", pg.connstr()])
        last_flush_lsn_upload(env, pg, tenant_id, timeline_id)
    # wrap up and shutdown safekeepers so that no more layers will be created after the final checkpoint
    for sk in env.safekeepers:
        sk.stop()
    ps_http.timeline_checkpoint(tenant_id, timeline_id)

    # wait for evictions and assert that they stabilize
    @dataclass
    class ByLocalAndRemote:
        remote_layers: set[str]
        local_layers: set[str]

    class MapInfoProjection:
        def __init__(self, info: LayerMapInfo):
            self.info = info

        def by_local_and_remote(self) -> ByLocalAndRemote:
            return ByLocalAndRemote(
                remote_layers={
                    layer.layer_file_name for layer in self.info.historic_layers if layer.remote
                },
                local_layers={
                    layer.layer_file_name for layer in self.info.historic_layers if not layer.remote
                },
            )

        def __eq__(self, other):
            if not isinstance(other, MapInfoProjection):
                return False
            return self.by_local_and_remote() == other.by_local_and_remote()

        def __repr__(self) -> str:
            out = ["MapInfoProjection:"]
            for layer in sorted(self.info.historic_layers, key=lambda layer: layer.layer_file_name):
                remote = "R" if layer.remote else "L"
                out += [f"  {remote} {layer.layer_file_name}"]
            return "\n".join(out)

    stable_for: float = 0
    observation_window = 8 * eviction_threshold
    consider_stable_when_no_change_for_seconds = 3 * eviction_threshold
    poll_interval = eviction_threshold / 3
    started_waiting_at = time.time()
    map_info_changes: list[tuple[float, MapInfoProjection]] = []
    while time.time() - started_waiting_at < observation_window:
        current = (
            time.time(),
            MapInfoProjection(vps_http.layer_map_info(tenant_id, timeline_id)),
        )
        last = map_info_changes[-1] if map_info_changes else (0, None)
        if last[1] is None or current[1] != last[1]:
            map_info_changes.append(current)
            log.info("change in layer map\n before: %s\n after: %s", last, current)
        else:
            stable_for = current[0] - last[0]
            log.info("residencies stable for %s", stable_for)
            if stable_for > consider_stable_when_no_change_for_seconds:
                break
        time.sleep(poll_interval)

    log.info("len(map_info_changes)=%s", len(map_info_changes))

    # TODO: can we be more precise here? E.g., require we're stable _within_ X*threshold,
    # instead of what we do here, i.e., stable _for at least_ X*threshold toward the end of the observation window
    assert (
        stable_for > consider_stable_when_no_change_for_seconds
    ), "layer residencies did not become stable within the observation window"

    post = map_info_changes[-1][1].by_local_and_remote()
    assert len(post.remote_layers) > 0, "some layers should be evicted once it's stabilized"
    assert len(post.local_layers) > 0, "the imitate accesses should keep some layers resident"

    assert (
        env.pageserver.log_contains(metrics_refused_log_line) is not None
    ), "ensure the metrics collection worker ran"
