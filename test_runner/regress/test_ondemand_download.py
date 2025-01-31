# It's possible to run any regular test with the local fs remote storage via
# env ZENITH_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/neon_zzz/'}" poetry ......

from __future__ import annotations

import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import pytest
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    flush_ep_to_pageserver,
    last_flush_lsn_upload,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.pageserver.utils import (
    assert_tenant_state,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_for_upload_queue_empty,
    wait_until_tenant_active,
)
from fixtures.remote_storage import RemoteStorageKind, S3Storage, s3_storage
from fixtures.utils import query_scalar, wait_until
from urllib3 import Retry

if TYPE_CHECKING:
    from typing import Any


def get_num_downloaded_layers(client: PageserverHttpClient):
    """
    This assumes that the pageserver only has a single tenant.
    """
    value = client.get_metric_value(
        "pageserver_remote_operation_seconds_count",
        {
            "file_kind": "layer",
            "op_kind": "download",
            "status": "success",
        },
    )
    if value is None:
        return 0
    return int(value)


#
# If you have a large relation, check that the pageserver downloads parts of it as
# require by queries.
#
def test_ondemand_download_large_rel(neon_env_builder: NeonEnvBuilder):
    # thinking about using a shared environment? the test assumes that global
    # metrics are for single tenant.
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # disable background GC
            "gc_period": "0s",
            "gc_horizon": f"{10 * 1024 ** 3}",  # 10 GB
            # small checkpoint distance to create more delta layer files
            "checkpoint_distance": f"{10 * 1024 ** 2}",  # 10 MB
            # allow compaction with the checkpoint
            "compaction_threshold": "3",
            "compaction_target_size": f"{10 * 1024 ** 2}",  # 10 MB
            # but don't run compaction in background or on restart
            "compaction_period": "0s",
        }
    )

    endpoint = env.endpoints.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # We want to make sure that the data is large enough that the keyspace is partitioned.
    num_rows = 1000000

    with endpoint.cursor() as cur:
        # data loading may take a while, so increase statement timeout
        cur.execute("SET statement_timeout='300s'")
        cur.execute(
            f"""CREATE TABLE tbl AS SELECT g as id, 'long string to consume some space' || g
        from generate_series(1,{num_rows}) g"""
        )
        cur.execute("CREATE INDEX ON tbl (id)")
        cur.execute("VACUUM tbl")

        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

    # stop endpoint before checkpoint to stop wal generation
    endpoint.stop()

    # stopping of safekeepers now will help us not to calculate logical size
    # after startup, so page requests should be the only one on-demand
    # downloading the layers
    for sk in env.safekeepers:
        sk.stop()

    # run checkpoint manually to be sure that data landed in remote storage
    client.timeline_checkpoint(tenant_id, timeline_id)

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(client, tenant_id, timeline_id, current_lsn)
    log.info("uploads have finished")

    ##### Stop the first pageserver instance, erase all its data
    env.pageserver.stop()

    # remove all the layer files
    for layer in env.pageserver.tenant_dir().glob("*/timelines/*/*-*_*"):
        log.info(f"unlinking layer {layer}")
        layer.unlink()

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    # start a readonly endpoint which we'll use to check the database.
    # readonly (with lsn=) is required so that we don't try to connect to
    # safekeepers, that have now been shut down.
    endpoint = env.endpoints.create_start("main", lsn=current_lsn)

    before_downloads = get_num_downloaded_layers(client)
    assert before_downloads != 0, "basebackup should on-demand non-zero layers"

    # Probe in the middle of the table. There's a high chance that the beginning
    # and end of the table was stored together in the same layer files with data
    # from other tables, and with the entry that stores the size of the
    # relation, so they are likely already downloaded. But the middle of the
    # table should not have been needed by anything yet.
    with endpoint.cursor() as cur:
        assert query_scalar(cur, "select count(*) from tbl where id = 500000") == 1

    after_downloads = get_num_downloaded_layers(client)
    log.info(f"layers downloaded before {before_downloads} and after {after_downloads}")
    assert after_downloads > before_downloads


#
# If you have a relation with a long history of updates, the pageserver downloads the layer
# files containing the history as needed by timetravel queries.
#
def test_ondemand_download_timetravel(neon_env_builder: NeonEnvBuilder):
    # thinking about using a shared environment? the test assumes that global
    # metrics are for single tenant.

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # Disable background GC & compaction
            # We don't want GC, that would break the assertion about num downloads.
            # We don't want background compaction, we force a compaction every time we do explicit checkpoint.
            "gc_period": "0s",
            "compaction_period": "0s",
            # small checkpoint distance to create more delta layer files
            "checkpoint_distance": f"{1 * 1024 ** 2}",  # 1 MB
            "compaction_threshold": "1",
            "image_creation_threshold": "1",
            "compaction_target_size": f"{1 * 1024 ** 2}",  # 1 MB
        }
    )
    pageserver_http = env.pageserver.http_client()

    endpoint = env.endpoints.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    ####
    # Produce layers
    ####

    lsns = []

    table_len = 10000
    with endpoint.cursor() as cur:
        cur.execute(
            f"""
        CREATE TABLE testtab(id serial primary key, checkpoint_number int, data text);
        INSERT INTO testtab (checkpoint_number, data) SELECT 0, 'data' FROM generate_series(1, {table_len});
        """
        )
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
    # wait until pageserver receives that data
    wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)
    # run checkpoint manually to be sure that data landed in remote storage
    client.timeline_checkpoint(tenant_id, timeline_id)
    lsns.append((0, current_lsn))

    for checkpoint_number in range(1, 20):
        with endpoint.cursor() as cur:
            cur.execute(f"UPDATE testtab SET checkpoint_number = {checkpoint_number}")
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
        lsns.append((checkpoint_number, current_lsn))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        client.timeline_checkpoint(tenant_id, timeline_id)

    # prevent new WAL from being produced, wait for layers to reach remote storage
    env.endpoints.stop_all()
    for sk in env.safekeepers:
        sk.stop()
    # NB: the wait_for_upload returns as soon as remote_consistent_lsn == current_lsn.
    # But the checkpoint also triggers a compaction
    # => image layer generation =>
    # => doesn't advance LSN
    # => but we want the remote state to deterministic, so additionally, wait for upload queue to drain
    wait_for_upload(client, tenant_id, timeline_id, current_lsn)
    wait_for_upload_queue_empty(pageserver_http, env.initial_tenant, timeline_id)
    client.deletion_queue_flush(execute=True)
    env.pageserver.stop()
    env.pageserver.start()
    # We've shut down the SKs, then restarted the PSes to sever all walreceiver connections;
    # This means pageserver's remote_consistent_lsn is now frozen to whatever it was after the pageserver.stop() call.
    wait_until_tenant_active(client, tenant_id)

    ###
    # Produce layers complete;
    # Start the actual testing.
    ###

    def get_api_current_physical_size():
        d = client.timeline_detail(tenant_id, timeline_id)
        return d["current_physical_size"]

    def get_resident_physical_size():
        return client.get_timeline_metric(
            tenant_id, timeline_id, "pageserver_resident_physical_size"
        )

    filled_current_physical = get_api_current_physical_size()
    log.info(filled_current_physical)
    filled_size = get_resident_physical_size()
    log.info(filled_size)
    assert filled_current_physical == filled_size, "we don't yet do layer eviction"

    # Stop the first pageserver instance, erase all its data
    env.pageserver.stop()

    # remove all the layer files
    for layer in env.pageserver.tenant_dir().glob("*/timelines/*/*-*_*"):
        log.info(f"unlinking layer {layer}")
        layer.unlink()

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    wait_until(lambda: assert_tenant_state(client, tenant_id, "Active"))

    # The current_physical_size reports the sum of layers loaded in the layer
    # map, regardless of where the layer files are located. So even though we
    # just removed the local files, they still count towards
    # current_physical_size because they are loaded as `RemoteLayer`s.
    assert filled_current_physical == get_api_current_physical_size()

    # Run queries at different points in time
    num_layers_downloaded = [0]
    resident_size = [get_resident_physical_size()]
    for checkpoint_number, lsn in lsns:
        endpoint_old = env.endpoints.create_start(
            branch_name="main", endpoint_id=f"ep-old_lsn_{checkpoint_number}", lsn=lsn
        )
        with endpoint_old.cursor() as cur:
            # assert query_scalar(cur, f"select count(*) from testtab where checkpoint_number={checkpoint_number}") == 100000
            assert (
                query_scalar(
                    cur,
                    f"select count(*) from testtab where checkpoint_number<>{checkpoint_number}",
                )
                == 0
            )
            assert (
                query_scalar(
                    cur,
                    f"select count(*) from testtab where checkpoint_number={checkpoint_number}",
                )
                == table_len
            )

        after_downloads = get_num_downloaded_layers(client)
        num_layers_downloaded.append(after_downloads)
        log.info(f"num_layers_downloaded[-1]={num_layers_downloaded[-1]}")

        # Check that on each query, we need to download at least one more layer file. However in
        # practice, thanks to compaction and the fact that some requests need to download
        # more history, some points-in-time are covered by earlier downloads already. But
        # in broad strokes, as we query more points-in-time, more layers need to be downloaded.
        #
        # Do a fuzzy check on that, by checking that after each point-in-time, we have downloaded
        # more files than we had three iterations ago.
        log.info(f"layers downloaded after checkpoint {checkpoint_number}: {after_downloads}")
        if len(num_layers_downloaded) > 4:
            assert after_downloads > num_layers_downloaded[len(num_layers_downloaded) - 4]

        # Likewise, assert that the resident_physical_size metric grows as layers are downloaded
        resident_size.append(get_resident_physical_size())
        log.info(f"resident_size[-1]={resident_size[-1]}")
        if len(resident_size) > 4:
            assert resident_size[-1] > resident_size[len(resident_size) - 4]

        # current_physical_size reports the total size of all layer files, whether
        # they are present only in the remote storage, only locally, or both.
        # It should not change.
        assert filled_current_physical == get_api_current_physical_size()
        endpoint_old.stop()


#
# Ensure that the `download_remote_layers` API works
#
def test_download_remote_layers_api(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    ##### First start, insert data and upload it to the remote storage
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # Disable background GC & compaction
            # We don't want GC, that would break the assertion about num downloads.
            # We don't want background compaction, we force a compaction every time we do explicit checkpoint.
            "gc_period": "0s",
            "compaction_period": "0s",
            # small checkpoint distance to create more delta layer files
            "checkpoint_distance": f"{1 * 1024 ** 2}",  # 1 MB
            "compaction_threshold": "999999",
            "image_creation_threshold": "999999",
            "compaction_target_size": f"{1 * 1024 ** 2}",  # 1 MB
        }
    )

    # This test triggers layer download failures on demand. It is possible to modify the failpoint
    # during a `Timeline::get_vectored` right between the vectored read and it's validation read.
    # This means that one of the reads can fail while the other one succeeds and vice versa.
    # TODO(vlad): Remove this block once the vectored read path validation goes away.
    env.pageserver.allowed_errors.extend(
        [
            ".*initial_size_calculation.*Vectored get failed with downloading evicted layer file failed, but sequential get did not.*"
            ".*initial_size_calculation.*Sequential get failed with downloading evicted layer file failed, but vectored get did not.*"
        ]
    )

    endpoint = env.endpoints.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    table_len = 10000
    with endpoint.cursor() as cur:
        cur.execute(
            f"""
        CREATE TABLE testtab(id serial primary key, checkpoint_number int, data text);
        INSERT INTO testtab (checkpoint_number, data) SELECT 0, 'data' FROM generate_series(1, {table_len});
        """
        )

    last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)
    env.endpoints.stop_all()

    def get_api_current_physical_size():
        d = client.timeline_detail(tenant_id, timeline_id)
        return d["current_physical_size"]

    def get_resident_physical_size():
        return client.get_timeline_metric(
            tenant_id, timeline_id, "pageserver_resident_physical_size"
        )

    # Shut down safekeepers before starting the pageserver.
    # If we don't, they might stream us more WAL.
    for sk in env.safekeepers:
        sk.stop()

    # it is sad we cannot do a flush inmem layer without compaction, but
    # working around with very high layer0 count and image layer creation
    # threshold
    client.timeline_checkpoint(tenant_id, timeline_id)

    wait_for_upload_queue_empty(client, tenant_id, timeline_id)

    filled_current_physical = get_api_current_physical_size()
    log.info(f"filled_current_physical: {filled_current_physical}")
    filled_size = get_resident_physical_size()
    log.info(f"filled_size: {filled_size}")
    assert filled_current_physical == filled_size, "we don't yet do layer eviction"

    env.pageserver.stop()

    # remove all the layer files
    for layer in env.pageserver.tenant_dir().glob("*/timelines/*/*-*_*"):
        log.info(f"unlinking layer {layer.name}")
        layer.unlink()

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start(extra_env_vars={"FAILPOINTS": "remote-storage-download-pre-rename=return"})
    env.pageserver.allowed_errors.extend(
        [
            ".*download failed: downloading evicted layer file failed.*",
            f".*initial_size_calculation.*{tenant_id}.*{timeline_id}.*initial size calculation failed.*downloading evicted layer file failed",
        ]
    )

    wait_until(lambda: assert_tenant_state(client, tenant_id, "Active"))

    ###### Phase 1: exercise download error code path

    this_time = get_api_current_physical_size()
    assert (
        filled_current_physical == this_time
    ), "current_physical_size is sum of loaded layer sizes, independent of whether local or remote"

    post_unlink_size = get_resident_physical_size()
    log.info(f"post_unlink_size: {post_unlink_size}")
    assert (
        post_unlink_size < filled_size
    ), "we just deleted layers and didn't cause anything to re-download them yet"

    # issue downloads that we know will fail
    info = client.timeline_download_remote_layers(
        tenant_id,
        timeline_id,
        max_concurrent_downloads=10,
        errors_ok=True,
        at_least_one_download=False,
    )
    log.info(f"info={info}")
    assert info["state"] == "Completed"
    assert info["total_layer_count"] > 0
    assert info["successful_download_count"] == 0
    # can't assert == total_layer_count because timeline_detail also tries to
    # download layers for logical size, but this might not always hold.
    assert info["failed_download_count"] > 0
    assert (
        info["total_layer_count"]
        == info["successful_download_count"] + info["failed_download_count"]
    )
    assert get_api_current_physical_size() == filled_current_physical
    assert (
        get_resident_physical_size() == post_unlink_size
    ), "didn't download anything new due to failpoint"

    ##### Retry, this time without failpoints
    client.configure_failpoints(("remote-storage-download-pre-rename", "off"))
    info = client.timeline_download_remote_layers(
        tenant_id,
        timeline_id,
        # allow some concurrency to unveil potential concurrency bugs
        max_concurrent_downloads=10,
        errors_ok=False,
    )
    log.info(f"info={info}")

    assert info["state"] == "Completed"
    assert info["total_layer_count"] > 0
    assert info["successful_download_count"] > 0
    assert info["failed_download_count"] == 0
    assert (
        info["total_layer_count"]
        == info["successful_download_count"] + info["failed_download_count"]
    )

    refilled_size = get_resident_physical_size()
    log.info(refilled_size)

    assert filled_size == refilled_size, "we redownloaded all the layers"
    assert get_api_current_physical_size() == filled_current_physical

    for sk in env.safekeepers:
        sk.start()

    # ensure that all the data is back
    endpoint_old = env.endpoints.create_start(branch_name="main")
    with endpoint_old.cursor() as cur:
        assert query_scalar(cur, "select count(*) from testtab") == table_len


def test_compaction_downloads_on_demand_without_image_creation(neon_env_builder: NeonEnvBuilder):
    """
    Create a few layers, then evict, then make sure compaction runs successfully.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    conf = {
        # Disable background GC & compaction
        "gc_period": "0s",
        "compaction_period": "0s",
        # unused, because manual will be called after each table
        "checkpoint_distance": 100 * 1024**2,
        # this will be updated later on to allow manual compaction outside of checkpoints
        "compaction_threshold": 100,
        # repartitioning parameter, not required here
        "image_creation_threshold": 100,
        # repartitioning parameter, not required here
        "compaction_target_size": 128 * 1024**2,
        # pitr_interval and gc_horizon are not interesting because we dont run gc
    }

    env = neon_env_builder.init_start(initial_tenant_conf=stringify(conf))

    def downloaded_bytes_and_count(pageserver_http: PageserverHttpClient) -> tuple[int, int]:
        m = pageserver_http.get_metrics()
        # these are global counters
        total_bytes = m.query_one("pageserver_remote_ondemand_downloaded_bytes_total").value
        assert (
            total_bytes < 2**53 and total_bytes.is_integer()
        ), "bytes should still be safe integer-in-f64"
        count = m.query_one("pageserver_remote_ondemand_downloaded_layers_total").value
        assert count < 2**53 and count.is_integer(), "count should still be safe integer-in-f64"
        return (int(total_bytes), int(count))

    pageserver_http = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    with env.endpoints.create_start("main") as endpoint:
        # no particular reason to create the layers like this, but we are sure
        # not to hit the image_creation_threshold here.
        with endpoint.cursor() as cur:
            cur.execute("create table a as select id::bigint from generate_series(1, 204800) s(id)")
        wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

        with endpoint.cursor() as cur:
            cur.execute("update a set id = -id")
        flush_ep_to_pageserver(env, endpoint, tenant_id, timeline_id)
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    assert not layers.in_memory_layers, "no inmemory layers expected after post-commit checkpoint"
    assert len(layers.historic_layers) == 1 + 2, "should have initdb layer and 2 deltas"

    layer_sizes = 0

    for layer in layers.historic_layers:
        log.info(f"pre-compact:  {layer}")
        layer_sizes += layer.layer_file_size
        pageserver_http.evict_layer(tenant_id, timeline_id, layer.layer_file_name)

    env.config_tenant(tenant_id, {"compaction_threshold": "3"})

    pageserver_http.timeline_compact(tenant_id, timeline_id)
    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    for layer in layers.historic_layers:
        log.info(f"post compact: {layer}")
    assert len(layers.historic_layers) == 1, "should have compacted to single layer"

    post_compact = downloaded_bytes_and_count(pageserver_http)

    # use gte to allow pageserver to do other random stuff; this test could be run on a shared pageserver
    assert post_compact[0] >= layer_sizes
    assert post_compact[1] >= 3, "should had downloaded the three layers"


def test_compaction_downloads_on_demand_with_image_creation(neon_env_builder: NeonEnvBuilder):
    """
    Create layers, compact with high image_creation_threshold, then run final compaction with all layers evicted.

    Due to current implementation, this will make image creation on-demand download layers, but we cannot really
    directly test for it.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    conf = {
        # Disable background GC & compaction
        "gc_period": "0s",
        "compaction_period": "0s",
        # repartitioning threshold is this / 10, but it doesn't really seem to matter
        "checkpoint_distance": 50 * 1024**2,
        "compaction_threshold": 3,
        # important: keep this high for the data ingestion
        "image_creation_threshold": 100,
        # repartitioning parameter, unused
        "compaction_target_size": 128 * 1024**2,
        # Always check if a new image layer can be created
        "image_layer_creation_check_threshold": 0,
        # pitr_interval and gc_horizon are not interesting because we dont run gc
    }

    env = neon_env_builder.init_start(initial_tenant_conf=stringify(conf))
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    pageserver_http = env.pageserver.http_client()

    endpoint = env.endpoints.create_start("main")

    # no particular reason to create the layers like this, but we are sure
    # not to hit the image_creation_threshold here.
    with endpoint.cursor() as cur:
        cur.execute("create table a (id bigserial primary key, some_value bigint not null)")
        cur.execute("insert into a(some_value) select i from generate_series(1, 10000) s(i)")
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    for i in range(0, 2):
        for j in range(0, 3):
            # create a minimal amount of "delta difficulty" for this table
            with endpoint.cursor() as cur:
                cur.execute("update a set some_value = -some_value + %s", (j,))

            with endpoint.cursor() as cur:
                # vacuuming should aid to reuse keys, though it's not really important
                # with image_creation_threshold=1 which we will use on the last compaction
                cur.execute("vacuum")

            wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

            if i == 1 and j == 2:
                # last iteration; stop before checkpoint to avoid leaving an inmemory layer
                endpoint.stop_and_destroy()

            pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

        # images should not yet be created, because threshold is too high,
        # but these will be reshuffled to L1 layers
        pageserver_http.timeline_compact(tenant_id, timeline_id)

    for _ in range(0, 20):
        # loop in case flushing is still in progress
        layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
        if not layers.in_memory_layers:
            break
        time.sleep(0.2)

    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    assert not layers.in_memory_layers, "no inmemory layers expected after post-commit checkpoint"

    kinds_before: defaultdict[str, int] = defaultdict(int)

    for layer in layers.historic_layers:
        kinds_before[layer.kind] += 1
        pageserver_http.evict_layer(tenant_id, timeline_id, layer.layer_file_name)

    assert dict(kinds_before) == {"Delta": 4}

    # now having evicted all layers, reconfigure to have lower image creation
    # threshold to expose image creation to downloading all of the needed
    # layers -- threshold of 2 would sound more reasonable, but keeping it as 1
    # to be less flaky
    conf["image_creation_threshold"] = "1"
    env.config_tenant(tenant_id, {k: str(v) for k, v in conf.items()})

    pageserver_http.timeline_compact(tenant_id, timeline_id)
    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    kinds_after: defaultdict[str, int] = defaultdict(int)
    for layer in layers.historic_layers:
        kinds_after[layer.kind] += 1

    assert dict(kinds_after) == {"Delta": 4, "Image": 1}


def test_layer_download_cancelled_by_config_location(neon_env_builder: NeonEnvBuilder):
    """
    Demonstrates that tenant shutdown will cancel on-demand download and secondary doing warmup.
    """
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())

    # turn off background tasks so that they don't interfere with the downloads
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
            "compaction_period": "0s",
        }
    )

    # Disable retries, because we'll hit code paths that can give us
    # 503 and want to see that directly
    client = env.pageserver.http_client(retries=Retry(status=0))

    failpoint = "before-downloading-layer-stream-pausable"
    client.configure_failpoints((failpoint, "pause"))

    info = client.layer_map_info(env.initial_tenant, env.initial_timeline)
    assert len(info.delta_layers()) == 1

    layer = info.delta_layers()[0]

    client.tenant_heatmap_upload(env.initial_tenant)

    # evict the initdb layer so we can download it
    client.evict_layer(env.initial_tenant, env.initial_timeline, layer.layer_file_name)

    with ThreadPoolExecutor(max_workers=2) as exec:
        download = exec.submit(
            client.download_layer,
            env.initial_tenant,
            env.initial_timeline,
            layer.layer_file_name,
        )

        _, offset = wait_until(
            lambda: env.pageserver.assert_log_contains(f"at failpoint {failpoint}")
        )

        location_conf = {"mode": "Detached", "tenant_conf": {}}
        # assume detach removes the layers
        detach = exec.submit(client.tenant_location_conf, env.initial_tenant, location_conf)

        _, offset = wait_until(
            lambda: env.pageserver.assert_log_contains(
                "closing is taking longer than expected", offset
            ),
        )

        client.configure_failpoints((failpoint, "off"))

        with pytest.raises(PageserverApiException, match="Shutting down"):
            download.result()

        detach.result()

        client.configure_failpoints((failpoint, "pause"))

        _, offset = wait_until(
            lambda: env.pageserver.assert_log_contains(f"cfg failpoint: {failpoint} pause", offset),
        )

        location_conf = {
            "mode": "Secondary",
            "secondary_conf": {"warm": True},
            "tenant_conf": {},
        }

        client.tenant_location_conf(env.initial_tenant, location_conf)

        warmup = exec.submit(client.tenant_secondary_download, env.initial_tenant, wait_ms=30000)

        _, offset = wait_until(
            lambda: env.pageserver.assert_log_contains(f"at failpoint {failpoint}", offset),
        )

        client.configure_failpoints((failpoint, "off"))
        location_conf = {"mode": "Detached", "tenant_conf": {}}
        client.tenant_location_conf(env.initial_tenant, location_conf)

        client.configure_failpoints((failpoint, "off"))

        # here we have nothing in the log, but we see that the warmup and conf location update worked
        warmup.result()


def test_layer_download_timeouted(neon_env_builder: NeonEnvBuilder):
    """
    Pause using a pausable_failpoint longer than the client timeout to simulate the timeout happening.
    """
    # running this test is not reliable against REAL_S3, because operations can
    # take longer than 1s we want to use as a timeout
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)
    assert isinstance(neon_env_builder.pageserver_remote_storage, S3Storage)
    neon_env_builder.pageserver_remote_storage.custom_timeout = "1s"

    # turn off background tasks so that they don't interfere with the downloads
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
            "compaction_period": "0s",
        }
    )
    client = env.pageserver.http_client()
    failpoint = "before-downloading-layer-stream-pausable"
    client.configure_failpoints((failpoint, "pause"))

    info = client.layer_map_info(env.initial_tenant, env.initial_timeline)
    assert len(info.delta_layers()) == 1

    layer = info.delta_layers()[0]

    client.tenant_heatmap_upload(env.initial_tenant)

    # evict so we can download it
    client.evict_layer(env.initial_tenant, env.initial_timeline, layer.layer_file_name)

    with ThreadPoolExecutor(max_workers=2) as exec:
        download = exec.submit(
            client.download_layer,
            env.initial_tenant,
            env.initial_timeline,
            layer.layer_file_name,
        )

        _, offset = wait_until(
            lambda: env.pageserver.assert_log_contains(f"at failpoint {failpoint}")
        )
        # ensure enough time while paused to trip the timeout
        time.sleep(2)

        client.configure_failpoints((failpoint, "off"))
        download.result()

        _, offset = env.pageserver.assert_log_contains(
            ".*failed, will retry \\(attempt 0\\): timeout.*"
        )
        _, offset = env.pageserver.assert_log_contains(".*succeeded after [0-9]+ retries.*", offset)

        client.evict_layer(env.initial_tenant, env.initial_timeline, layer.layer_file_name)

        client.configure_failpoints((failpoint, "pause"))

        # capture the next offset for a new synchronization with the failpoint
        _, offset = wait_until(
            lambda: env.pageserver.assert_log_contains(f"cfg failpoint: {failpoint} pause", offset),
        )

        location_conf = {
            "mode": "Secondary",
            "secondary_conf": {"warm": True},
            "tenant_conf": {},
        }

        client.tenant_location_conf(
            env.initial_tenant,
            location_conf,
        )

        started = time.time()

        warmup = exec.submit(client.tenant_secondary_download, env.initial_tenant, wait_ms=30000)
        # ensure enough time while paused to trip the timeout
        time.sleep(2)

        client.configure_failpoints((failpoint, "off"))

        warmup.result()

        elapsed = time.time() - started

        _, offset = env.pageserver.assert_log_contains(
            ".*failed, will retry \\(attempt 0\\): timeout.*", offset
        )
        _, offset = env.pageserver.assert_log_contains(".*succeeded after [0-9]+ retries.*", offset)

        assert elapsed < 30, "too long passed: {elapsed=}"


def stringify(conf: dict[str, Any]) -> dict[str, str]:
    return dict(map(lambda x: (x[0], str(x[1])), conf.items()))
