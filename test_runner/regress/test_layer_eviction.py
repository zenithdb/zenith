import time

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.utils import wait_for_last_record_lsn, wait_for_upload
from fixtures.remote_storage import RemoteStorageKind
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar


# Crates a few layers, ensures that we can evict them (removing locally but keeping track of them anyway)
# and then download them back.
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_basic_eviction(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_download_remote_layers_api",
    )

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # disable gc and compaction background loops because they perform on-demand downloads
            "gc_period": "0s",
            "compaction_period": "0s",
        }
    )
    client = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main")

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Create a number of layers in the tenant
    with endpoint.cursor() as cur:
        cur.execute("CREATE TABLE foo (t text)")
        cur.execute(
            """
            INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 5000000) g
            """
        )
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)
    client.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(client, tenant_id, timeline_id, current_lsn)

    # disable compute & sks to avoid on-demand downloads by walreceiver / getpage
    endpoint.stop()
    for sk in env.safekeepers:
        sk.stop()

    timeline_path = env.timeline_dir(tenant_id, timeline_id)
    initial_local_layers = sorted(
        list(filter(lambda path: path.name != "metadata", timeline_path.glob("*")))
    )
    assert (
        len(initial_local_layers) > 1
    ), f"Should create multiple layers for timeline, but got {initial_local_layers}"

    # Compare layer map dump with the local layers, ensure everything's present locally and matches
    initial_layer_map_info = client.layer_map_info(tenant_id=tenant_id, timeline_id=timeline_id)
    assert (
        not initial_layer_map_info.in_memory_layers
    ), "Should have no in memory layers after flushing"
    assert len(initial_local_layers) == len(
        initial_layer_map_info.historic_layers
    ), "Should have the same layers in memory and on disk"
    for returned_layer in initial_layer_map_info.historic_layers:
        assert (
            returned_layer.kind == "Delta"
        ), f"Did not create and expect image layers, but got {returned_layer}"
        assert (
            not returned_layer.remote
        ), f"All created layers should be present locally, but got {returned_layer}"

        local_layers = list(
            filter(lambda layer: layer.name == returned_layer.layer_file_name, initial_local_layers)
        )
        assert (
            len(local_layers) == 1
        ), f"Did not find returned layer {returned_layer} in local layers {initial_local_layers}"
        local_layer = local_layers[0]
        assert (
            returned_layer.layer_file_size == local_layer.stat().st_size
        ), f"Returned layer {returned_layer} has a different file size than local layer {local_layer}"

    # Detach all layers, ensre they are not in the local FS, but are still dumped as part of the layer map
    for local_layer in initial_local_layers:
        client.evict_layer(
            tenant_id=tenant_id, timeline_id=timeline_id, layer_name=local_layer.name
        )
        assert not any(
            new_local_layer.name == local_layer.name for new_local_layer in timeline_path.glob("*")
        ), f"Did not expect to find {local_layer} layer after evicting"

    empty_layers = list(filter(lambda path: path.name != "metadata", timeline_path.glob("*")))
    assert (
        not empty_layers
    ), f"After evicting all layers, timeline {tenant_id}/{timeline_id} should have no layers locally, but got: {empty_layers}"

    evicted_layer_map_info = client.layer_map_info(tenant_id=tenant_id, timeline_id=timeline_id)
    assert (
        not evicted_layer_map_info.in_memory_layers
    ), "Should have no in memory layers after flushing and evicting"
    assert len(initial_local_layers) == len(
        evicted_layer_map_info.historic_layers
    ), "Should have the same layers in memory and on disk initially"
    for returned_layer in evicted_layer_map_info.historic_layers:
        assert (
            returned_layer.kind == "Delta"
        ), f"Did not create and expect image layers, but got {returned_layer}"
        assert (
            returned_layer.remote
        ), f"All layers should be evicted and not present locally, but got {returned_layer}"
        assert any(
            local_layer.name == returned_layer.layer_file_name
            for local_layer in initial_local_layers
        ), f"Did not find returned layer {returned_layer} in local layers {initial_local_layers}"

    # redownload all evicted layers and ensure the initial state is restored
    for local_layer in initial_local_layers:
        client.download_layer(
            tenant_id=tenant_id, timeline_id=timeline_id, layer_name=local_layer.name
        )
    client.timeline_download_remote_layers(
        tenant_id,
        timeline_id,
        # allow some concurrency to unveil potential concurrency bugs
        max_concurrent_downloads=10,
        errors_ok=False,
        at_least_one_download=False,
    )

    redownloaded_layers = sorted(
        list(filter(lambda path: path.name != "metadata", timeline_path.glob("*")))
    )
    assert (
        redownloaded_layers == initial_local_layers
    ), "Should have the same layers locally after redownloading the evicted layers"
    redownloaded_layer_map_info = client.layer_map_info(
        tenant_id=tenant_id, timeline_id=timeline_id
    )
    assert (
        redownloaded_layer_map_info == initial_layer_map_info
    ), "Should have the same layer map after redownloading the evicted layers"


def test_gc_of_remote_layers(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.LOCAL_FS,
        test_name="test_gc_of_remote_layers",
    )

    env = neon_env_builder.init_start()

    tenant_config = {
        "pitr_interval": "1s",  # set to non-zero, so GC actually does something
        "gc_period": "0s",  # we want to control when GC runs
        "compaction_period": "0s",  # we want to control when compaction runs
        "checkpoint_timeout": "24h",  # something we won't reach
        "checkpoint_distance": f"{50 * (1024**2)}",  # something we won't reach, we checkpoint manually
        "compaction_threshold": "3",
        # "image_creation_threshold": set at runtime
        "compaction_target_size": f"{128 * (1024**2)}",  # make it so that we only have 1 partition => image coverage for delta layers => enables gc of delta layers
    }

    def tenant_update_config(changes):
        tenant_config.update(changes)
        env.neon_cli.config_tenant(tenant_id, tenant_config)

    tenant_id, timeline_id = env.neon_cli.create_tenant(conf=tenant_config)
    log.info("tenant id is %s", tenant_id)
    env.initial_tenant = tenant_id  # update_and_gc relies on this
    ps_http = env.pageserver.http_client()

    endpoint = env.endpoints.create_start("main")

    log.info("fill with data, creating delta & image layers, some of which are GC'able after")
    # no particular reason to create the layers like this, but we are sure
    # not to hit the image_creation_threshold here.
    with endpoint.cursor() as cur:
        cur.execute("create table a (id bigserial primary key, some_value bigint not null)")
        cur.execute("insert into a(some_value) select i from generate_series(1, 10000) s(i)")
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    ps_http.timeline_checkpoint(tenant_id, timeline_id)

    # Create delta layers, then turn them into image layers.
    # Do it multiple times so that there's something to GC.
    for k in range(0, 2):
        # produce delta layers => disable image layer creation by setting high threshold
        tenant_update_config({"image_creation_threshold": "100"})
        for i in range(0, 2):
            for j in range(0, 3):
                # create a minimal amount of "delta difficulty" for this table
                with endpoint.cursor() as cur:
                    cur.execute("update a set some_value = -some_value + %s", (j,))

                with endpoint.cursor() as cur:
                    # vacuuming should aid to reuse keys, though it's not really important
                    # with image_creation_threshold=1 which we will use on the last compaction
                    cur.execute("vacuum")

                last_lsn = wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

                if i == 1 and j == 2 and k == 1:
                    # last iteration; stop before checkpoint to avoid leaving an inmemory layer
                    endpoint.stop_and_destroy()

                ps_http.timeline_checkpoint(tenant_id, timeline_id)

            # images should not yet be created, because threshold is too high,
            # but these will be reshuffled to L1 layers
            ps_http.timeline_compact(tenant_id, timeline_id)

        for _ in range(0, 20):
            # loop in case flushing is still in progress
            layers = ps_http.layer_map_info(tenant_id, timeline_id)
            if not layers.in_memory_layers:
                break
            time.sleep(0.2)

        # now that we've grown some delta layers, turn them into image layers
        tenant_update_config({"image_creation_threshold": "1"})
        ps_http.timeline_compact(tenant_id, timeline_id)

    # wait for all uploads to finish (checkpoint has been done above)
    wait_for_upload(ps_http, tenant_id, timeline_id, last_lsn)

    # shutdown safekeepers to avoid on-demand downloads from walreceiver
    for sk in env.safekeepers:
        sk.stop()

    ps_http.timeline_checkpoint(tenant_id, timeline_id)

    log.info("ensure the code above produced image and delta layers")
    pre_evict_info = ps_http.layer_map_info(tenant_id, timeline_id)
    log.info("layer map dump: %s", pre_evict_info)
    by_kind = pre_evict_info.kind_count()
    log.info("by kind: %s", by_kind)
    assert by_kind["Image"] > 0
    assert by_kind["Delta"] > 0
    assert by_kind["InMemory"] == 0
    resident_layers = list(env.timeline_dir(tenant_id, timeline_id).glob("*-*_*"))
    log.info("resident layers count before eviction: %s", len(resident_layers))

    log.info("evict all layers")
    ps_http.evict_all_layers(tenant_id, timeline_id)

    def ensure_resident_and_remote_size_metrics():
        log.info("ensure that all the layers are gone")
        resident_layers = list(env.timeline_dir(tenant_id, timeline_id).glob("*-*_*"))
        # we have disabled all background loops, so, this should hold
        assert len(resident_layers) == 0

        info = ps_http.layer_map_info(tenant_id, timeline_id)
        log.info("layer map dump: %s", info)

        log.info("ensure that resident_physical_size metric is zero")
        resident_physical_size_metric = ps_http.get_timeline_metric(
            tenant_id, timeline_id, "pageserver_resident_physical_size"
        )
        assert resident_physical_size_metric == 0
        log.info("ensure that resident_physical_size metric corresponds to layer map dump")
        assert resident_physical_size_metric == sum(
            [layer.layer_file_size or 0 for layer in info.historic_layers if not layer.remote]
        )

        log.info("ensure that remote_physical_size metric matches layer map")
        remote_physical_size_metric = ps_http.get_timeline_metric(
            tenant_id, timeline_id, "pageserver_remote_physical_size"
        )
        log.info("ensure that remote_physical_size metric corresponds to layer map dump")
        assert remote_physical_size_metric == sum(
            layer.layer_file_size or 0 for layer in info.historic_layers if layer.remote
        )

    log.info("before runnning GC, ensure that remote_physical size is zero")
    ensure_resident_and_remote_size_metrics()

    log.info("run GC")
    time.sleep(2)  # let pitr_interval + 1 second pass
    ps_http.timeline_gc(tenant_id, timeline_id, 0)
    time.sleep(1)
    assert not env.pageserver.log_contains("Nothing to GC")

    log.info("ensure GC deleted some layers, otherwise this test is pointless")
    post_gc_info = ps_http.layer_map_info(tenant_id, timeline_id)
    log.info("layer map dump: %s", post_gc_info)
    log.info("by kind: %s", post_gc_info.kind_count())
    pre_evict_layers = set([layer.layer_file_name for layer in pre_evict_info.historic_layers])
    post_gc_layers = set([layer.layer_file_name for layer in post_gc_info.historic_layers])
    assert post_gc_layers.issubset(pre_evict_layers)
    assert len(post_gc_layers) < len(pre_evict_layers)

    log.info("update_gc_info might download some layers. Evict them again.")
    ps_http.evict_all_layers(tenant_id, timeline_id)

    log.info("after running GC, ensure that resident size is still zero")
    ensure_resident_and_remote_size_metrics()
