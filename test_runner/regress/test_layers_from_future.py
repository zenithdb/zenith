from __future__ import annotations

import time

import pytest
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, flush_ep_to_pageserver
from fixtures.pageserver.common_types import (
    DeltaLayerName,
    ImageLayerName,
    is_future_layer,
)
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
    wait_for_upload_queue_empty,
    wait_until_tenant_active,
)
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.utils import query_scalar, wait_until


@pytest.mark.parametrize(
    "attach_mode",
    ["default_generation", "same_generation"],
)
def test_issue_5878(neon_env_builder: NeonEnvBuilder, attach_mode: str):
    """
    Regression test for issue https://github.com/neondatabase/neon/issues/5878 .

    Create a situation where IndexPart contains an image layer from a future
    (i.e., image layer > IndexPart::disk_consistent_lsn).
    Detach.
    Attach.
    Wait for tenant to finish load_layer_map (by waiting for it to become active).
    Wait for any remote timeline client ops to finish that the attach started.
    Integrity-check the index part.

    Before fixing the issue, load_layer_map would schedule removal of the future
    image layer. A compaction run could later re-create the image layer with
    the same file name, scheduling a PUT.
    Due to lack of an upload queue barrier, the PUT and DELETE could be re-ordered.
    The result was IndexPart referencing a non-existent object.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_configs()
    env.start()

    ps_http = env.pageserver.http_client()

    l0_l1_threshold = 3
    image_creation_threshold = 1

    tenant_config = {
        "gc_period": "0s",  # disable GC (shouldn't matter for this test but still)
        "compaction_period": "0s",  # we want to control when compaction runs
        "checkpoint_timeout": "24h",  # something we won't reach
        "checkpoint_distance": f"{50 * (1024**2)}",  # something we won't reach, we checkpoint manually
        "image_creation_threshold": "100",  # we want to control when image is created
        "image_layer_creation_check_threshold": "0",
        "compaction_threshold": f"{l0_l1_threshold}",
        "compaction_target_size": f"{128 * (1024**3)}",  # make it so that we only have 1 partition => image coverage for delta layers => enables gc of delta layers
    }

    tenant_id, timeline_id = env.create_tenant(conf=tenant_config)

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    def get_index_part():
        assert isinstance(env.pageserver_remote_storage, LocalFsStorage)
        ip_path = env.pageserver_remote_storage.index_path(tenant_id, timeline_id)
        return env.pagectl.dump_index_part(ip_path)

    def get_future_layers():
        ip = get_index_part()
        future_layers = [
            layer_file_name
            for layer_file_name in ip.layer_metadata.keys()
            if is_future_layer(layer_file_name, ip.disk_consistent_lsn)
        ]
        return future_layers

    assert len(get_future_layers()) == 0

    current = get_index_part()
    assert len(set(current.layer_metadata.keys())) == 1
    layer_file_name = list(current.layer_metadata.keys())[0]
    assert isinstance(layer_file_name, DeltaLayerName)
    assert layer_file_name.is_l0(), f"{layer_file_name}"

    log.info("force image layer creation in the future by writing some data into in-memory layer")

    # Create a number of layers in the tenant
    with endpoint.cursor() as cur:
        cur.execute("CREATE TABLE foo (t text)")
        iters = l0_l1_threshold * image_creation_threshold
        for i in range(0, iters):
            cur.execute(
                f"""
                INSERT INTO foo
                SELECT '{i}' || g
                FROM generate_series(1, 10000) g
                """
            )
            last_record_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
            wait_for_last_record_lsn(ps_http, tenant_id, timeline_id, last_record_lsn)
            # 0..iters-1: create a stack of delta layers
            # iters: leave a non-empty in-memory layer which we'll use for image layer generation
            if i < iters - 1:
                ps_http.timeline_checkpoint(tenant_id, timeline_id, force_repartition=True)
                assert (
                    len(
                        [
                            layer
                            for layer in ps_http.layer_map_info(
                                tenant_id, timeline_id
                            ).historic_layers
                            if layer.kind == "Image"
                        ]
                    )
                    == 0
                )
    last_record_lsn = flush_ep_to_pageserver(env, endpoint, tenant_id, timeline_id)

    wait_for_upload_queue_empty(ps_http, tenant_id, timeline_id)

    ip = get_index_part()
    assert len(ip.layer_metadata.keys())
    assert (
        ip.disk_consistent_lsn < last_record_lsn
    ), "sanity check for what above loop is supposed to do"

    # create the image layer from the future
    env.storage_controller.pageserver_api().update_tenant_config(
        tenant_id, {"image_creation_threshold": image_creation_threshold}, None
    )
    assert ps_http.tenant_config(tenant_id).effective_config["image_creation_threshold"] == 1
    ps_http.timeline_compact(tenant_id, timeline_id, force_repartition=True)
    assert (
        len(
            [
                layer
                for layer in ps_http.layer_map_info(tenant_id, timeline_id).historic_layers
                if layer.kind == "Image"
            ]
        )
        == 1
    )
    wait_for_upload_queue_empty(ps_http, tenant_id, timeline_id)
    future_layers = get_future_layers()
    assert len(future_layers) == 1
    future_layer = future_layers[0]
    assert isinstance(future_layer, ImageLayerName)
    assert future_layer.lsn == last_record_lsn
    log.info(
        f"got layer from the future: lsn={future_layer.lsn} disk_consistent_lsn={ip.disk_consistent_lsn} last_record_lsn={last_record_lsn}"
    )
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)
    future_layer_path = env.pageserver_remote_storage.remote_layer_path(
        tenant_id, timeline_id, future_layer.to_str()
    )
    log.info(f"future layer path: {future_layer_path}")
    pre_stat = future_layer_path.stat()
    time.sleep(1.1)  # so that we can use change in pre_stat.st_mtime to detect overwrites

    def get_generation_number():
        attachment = env.storage_controller.inspect(tenant_id)
        assert attachment is not None
        return attachment[0]

    # force removal of layers from the future
    tenant_conf = ps_http.tenant_config(tenant_id)
    generation_before_detach = get_generation_number()
    env.pageserver.tenant_detach(tenant_id)
    failpoint_deletion_queue = "deletion-queue-before-execute-pause"

    ps_http.configure_failpoints((failpoint_deletion_queue, "pause"))

    if attach_mode == "default_generation":
        env.pageserver.tenant_attach(tenant_id, tenant_conf.tenant_specific_overrides)
    elif attach_mode == "same_generation":
        # Attach with the same generation number -- this is possible with timeline offload and detach ancestor
        env.pageserver.tenant_attach(
            tenant_id,
            tenant_conf.tenant_specific_overrides,
            generation=generation_before_detach,
            # We want to avoid the generation bump and don't want to talk with the storcon
            override_storage_controller_generation=False,
        )
    else:
        raise AssertionError(f"Unknown attach_mode: {attach_mode}")

    # Get it from pageserver API instead of storcon API b/c we might not have attached using the storcon
    # API if attach_mode == "same_generation"
    tenant_location = env.pageserver.http_client().tenant_get_location(tenant_id)
    generation_after_reattach = tenant_location["generation"]

    if attach_mode == "same_generation":
        # The generation number should be the same as before the detach
        assert generation_before_detach == generation_after_reattach
    wait_until_tenant_active(ps_http, tenant_id)

    # Ensure the IndexPart upload that unlinks the layer file finishes, i.e., doesn't clog the queue.
    def future_layer_is_gone_from_index_part():
        future_layers = set(get_future_layers())
        assert future_layer not in future_layers

    wait_until(future_layer_is_gone_from_index_part)

    # We already make deletion stuck here, but we don't necessarily hit the failpoint
    # because deletions are batched.
    future_layer_path = env.pageserver_remote_storage.remote_layer_path(
        tenant_id, timeline_id, future_layer.to_str(), generation=generation_before_detach
    )
    log.info(f"future layer path: {future_layer_path}")
    assert future_layer_path.exists()

    # wait for re-ingestion of the WAL from safekeepers into the in-memory layer
    # (this happens in parallel to the above)
    wait_for_last_record_lsn(ps_http, tenant_id, timeline_id, last_record_lsn)

    # re-do image layer generation
    # This will produce the same image layer and queue an upload.
    # However, we still have the deletion for the layer queued, stuck on the failpoint.
    # An incorrect implementation would let the PUT execute before the DELETE.
    # The later code in this test asserts that this doesn't happen.
    ps_http.timeline_compact(tenant_id, timeline_id, force_repartition=True)

    # Let things sit for some time; a good implementation makes no progress because
    # we can't execute the PUT before the DELETE. A bad implementation would do that.
    max_race_opportunity_window = 4
    start = time.monotonic()
    while True:
        post_stat = future_layer_path.stat()
        assert (
            pre_stat.st_mtime == post_stat.st_mtime
        ), "observed PUT overtake the stucked DELETE => bug isn't fixed yet"
        if time.monotonic() - start > max_race_opportunity_window:
            log.info(
                "a correct implementation would never let the later PUT overtake the earlier DELETE"
            )
            break
        time.sleep(1)

    # Window has passed, unstuck the delete, let deletion queue drain; the upload queue should
    # have drained because we put these layer deletion operations into the deletion queue and
    # have consumed the operation from the upload queue.
    log.info("unstuck the DELETE")
    ps_http.configure_failpoints((failpoint_deletion_queue, "off"))
    wait_for_upload_queue_empty(ps_http, tenant_id, timeline_id)
    env.pageserver.http_client().deletion_queue_flush(True)

    # Examine the resulting S3 state.
    log.info("integrity-check the remote storage")
    ip = get_index_part()
    for layer_file_name, layer_metadata in ip.layer_metadata.items():
        log.info(f"Layer metadata {layer_file_name.to_str()}: {layer_metadata}")
        layer_path = env.pageserver_remote_storage.remote_layer_path(
            tenant_id, timeline_id, layer_file_name.to_str(), layer_metadata.generation
        )
        assert layer_path.exists(), f"{layer_file_name.to_str()}"

    log.info("assert that the overwritten layer won")
    future_layer_path = env.pageserver_remote_storage.remote_layer_path(
        tenant_id, timeline_id, future_layer.to_str(), generation=generation_after_reattach
    )
    final_stat = future_layer_path.stat()
    log.info(f"future layer path: {future_layer_path}")
    assert final_stat.st_mtime != pre_stat.st_mtime

    # Ensure no weird errors in the end...
    wait_for_upload_queue_empty(ps_http, tenant_id, timeline_id)

    if attach_mode == "same_generation":
        # we should have detected a race upload and deferred it
        env.pageserver.assert_log_contains(
            "waiting for deletion queue flush to complete before uploading layer"
        )
