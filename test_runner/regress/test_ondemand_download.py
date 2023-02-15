# It's possible to run any regular test with the local fs remote storage via
# env ZENITH_PAGESERVER_OVERRIDES="remote_storage={local_path='/tmp/neon_zzz/'}" poetry ......

import time
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
    assert_tenant_status,
    available_remote_storages,
    wait_for_last_flush_lsn,
    wait_for_last_record_lsn,
    wait_for_sk_commit_lsn_to_reach_remote_storage,
    wait_for_upload,
    wait_until,
)
from fixtures.types import Lsn
from fixtures.utils import query_scalar


def get_num_downloaded_layers(client, tenant_id, timeline_id):
    value = client.get_metric_value(
        f'pageserver_remote_operation_seconds_count{{file_kind="layer",op_kind="download",status="success",tenant_id="{tenant_id}",timeline_id="{timeline_id}"}}'
    )
    if value is None:
        return 0
    return int(value)


#
# If you have a large relation, check that the pageserver downloads parts of it as
# require by queries.
#
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_ondemand_download_large_rel(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_ondemand_download_large_rel",
    )

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()

    # Override defaults, to create more layers
    tenant, _ = env.neon_cli.create_tenant(
        conf={
            # disable background GC
            "gc_period": "10 m",
            "gc_horizon": f"{10 * 1024 ** 3}",  # 10 GB
            # small checkpoint distance to create more delta layer files
            "checkpoint_distance": f"{10 * 1024 ** 2}",  # 10 MB
            "compaction_threshold": "3",
            "compaction_target_size": f"{10 * 1024 ** 2}",  # 10 MB
        }
    )
    env.initial_tenant = tenant

    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    # We want to make sure that the data is large enough that the keyspace is partitioned.
    num_rows = 1000000

    with pg.cursor() as cur:
        # data loading may take a while, so increase statement timeout
        cur.execute("SET statement_timeout='300s'")
        cur.execute(
            f"""CREATE TABLE tbl AS SELECT g as id, 'long string to consume some space' || g
        from generate_series(1,{num_rows}) g"""
        )
        cur.execute("CREATE INDEX ON tbl (id)")
        cur.execute("VACUUM tbl")

        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # wait until pageserver receives that data
    wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

    # run checkpoint manually to be sure that data landed in remote storage
    client.timeline_checkpoint(tenant_id, timeline_id)

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(client, tenant_id, timeline_id, current_lsn)
    log.info("uploads have finished")

    ##### Stop the first pageserver instance, erase all its data
    pg.stop()
    env.pageserver.stop()

    # remove all the layer files
    for layer in (Path(env.repo_dir) / "tenants").glob("*/timelines/*/*-*_*"):
        log.info(f"unlinking layer {layer}")
        layer.unlink()

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    pg.start()
    before_downloads = get_num_downloaded_layers(client, tenant_id, timeline_id)

    # Probe in the middle of the table. There's a high chance that the beginning
    # and end of the table was stored together in the same layer files with data
    # from other tables, and with the entry that stores the size of the
    # relation, so they are likely already downloaded. But the middle of the
    # table should not have been needed by anything yet.
    with pg.cursor() as cur:
        assert query_scalar(cur, "select count(*) from tbl where id = 500000") == 1

    after_downloads = get_num_downloaded_layers(client, tenant_id, timeline_id)
    log.info(f"layers downloaded before {before_downloads} and after {after_downloads}")
    assert after_downloads > before_downloads


#
# If you have a relation with a long history of updates, the pageserver downloads the layer
# files containing the history as needed by timetravel queries.
#
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_ondemand_download_timetravel(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_ondemand_download_timetravel",
    )

    ##### First start, insert data and upload it to the remote storage
    env = neon_env_builder.init_start()

    # Override defaults, to create more layers
    tenant, _ = env.neon_cli.create_tenant(
        conf={
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
    env.initial_tenant = tenant

    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    lsns = []

    table_len = 10000
    with pg.cursor() as cur:
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
        with pg.cursor() as cur:
            cur.execute(f"UPDATE testtab SET checkpoint_number = {checkpoint_number}")
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
        lsns.append((checkpoint_number, current_lsn))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        client.timeline_checkpoint(tenant_id, timeline_id)

    ##### Stop the first pageserver instance, erase all its data
    env.postgres.stop_all()

    # wait until pageserver has successfully uploaded all the data to remote storage
    wait_for_sk_commit_lsn_to_reach_remote_storage(
        tenant_id, timeline_id, env.safekeepers, env.pageserver
    )

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

    # Wait until generated image layers are uploaded to S3
    time.sleep(3)

    env.pageserver.stop()

    # remove all the layer files
    for layer in (Path(env.repo_dir) / "tenants").glob("*/timelines/*/*-*_*"):
        log.info(f"unlinking layer {layer}")
        layer.unlink()

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start()

    wait_until(10, 0.2, lambda: assert_tenant_status(client, tenant_id, "Active"))

    # The current_physical_size reports the sum of layers loaded in the layer
    # map, regardless of where the layer files are located. So even though we
    # just removed the local files, they still count towards
    # current_physical_size because they are loaded as `RemoteLayer`s.
    assert filled_current_physical == get_api_current_physical_size()

    # Run queries at different points in time
    num_layers_downloaded = [0]
    resident_size = [get_resident_physical_size()]
    for (checkpoint_number, lsn) in lsns:
        pg_old = env.postgres.create_start(
            branch_name="main", node_name=f"test_old_lsn_{checkpoint_number}", lsn=lsn
        )
        with pg_old.cursor() as cur:
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

        after_downloads = get_num_downloaded_layers(client, tenant_id, timeline_id)
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


#
# Ensure that the `download_remote_layers` API works
#
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_download_remote_layers_api(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_download_remote_layers_api",
    )

    ##### First start, insert data and upload it to the remote storage
    env = neon_env_builder.init_start()

    # Override defaults, to create more layers
    tenant, _ = env.neon_cli.create_tenant(
        conf={
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
    env.initial_tenant = tenant

    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    table_len = 10000
    with pg.cursor() as cur:
        cur.execute(
            f"""
        CREATE TABLE testtab(id serial primary key, checkpoint_number int, data text);
        INSERT INTO testtab (checkpoint_number, data) SELECT 0, 'data' FROM generate_series(1, {table_len});
        """
        )

    env.postgres.stop_all()

    wait_for_sk_commit_lsn_to_reach_remote_storage(
        tenant_id, timeline_id, env.safekeepers, env.pageserver
    )

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

    env.pageserver.stop()

    # remove all the layer files
    # XXX only delete some of the layer files, to show that it really just downloads all the layers
    for layer in (Path(env.repo_dir) / "tenants").glob("*/timelines/*/*-*_*"):
        log.info(f"unlinking layer {layer}")
        layer.unlink()

    # Shut down safekeepers before starting the pageserver.
    # If we don't, the tenant's walreceiver handler will trigger the
    # the logical size computation task, and that downloads layes,
    # which makes our assertions on size fail.
    for sk in env.safekeepers:
        sk.stop(immediate=True)

    ##### Second start, restore the data and ensure it's the same
    env.pageserver.start(extra_env_vars={"FAILPOINTS": "remote-storage-download-pre-rename=return"})
    env.pageserver.allowed_errors.extend(
        [
            f".*download_all_remote_layers.*{tenant_id}.*{timeline_id}.*layer download failed.*remote-storage-download-pre-rename failpoint",
            f".*initial size calculation.*{tenant_id}.*{timeline_id}.*Failed to calculate logical size",
        ]
    )

    wait_until(10, 0.2, lambda: assert_tenant_status(client, tenant_id, "Active"))

    ###### Phase 1: exercise download error code path
    assert (
        filled_current_physical == get_api_current_physical_size()
    ), "current_physical_size is sum of loaded layer sizes, independent of whether local or remote"
    post_unlink_size = get_resident_physical_size()
    log.info(post_unlink_size)
    assert (
        post_unlink_size < filled_size
    ), "we just deleted layers and didn't cause anything to re-download them yet"
    assert filled_size - post_unlink_size > 5 * (
        1024**2
    ), "we may be downloading some layers as part of tenant activation"

    # issue downloads that we know will fail
    info = client.timeline_download_remote_layers(
        tenant_id,
        timeline_id,
        # allow some concurrency to unveil potential concurrency bugs
        max_concurrent_downloads=10,
        errors_ok=True,
        at_least_one_download=False,
    )
    log.info(f"info={info}")
    assert info["state"] == "Completed"
    assert info["total_layer_count"] > 0
    assert info["successful_download_count"] == 0
    assert (
        info["failed_download_count"] > 0
    )  # can't assert == total_layer_count because attach + tenant status downloads some layers
    assert (
        info["total_layer_count"]
        == info["successful_download_count"] + info["failed_download_count"]
    )
    assert get_api_current_physical_size() == filled_current_physical
    assert (
        get_resident_physical_size() == post_unlink_size
    ), "didn't download anything new due to failpoint"
    # would be nice to assert that the layers in the layer map are still RemoteLayer

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
    pg_old = env.postgres.create_start(branch_name="main")
    with pg_old.cursor() as cur:
        assert query_scalar(cur, "select count(*) from testtab") == table_len


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.MOCK_S3])
def test_compaction_downloads_on_demand_without_image_creation(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    """
    Create a few layers, then evict, then make sure compaction runs successfully.
    """
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_compaction_downloads_on_demand_without_image_creation",
    )

    env = neon_env_builder.init_start()

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

    # Override defaults, to create more layers
    tenant_id, timeline_id = env.neon_cli.create_tenant(conf=stringify(conf))
    env.initial_tenant = tenant_id
    pageserver_http = env.pageserver.http_client()

    with env.postgres.create_start("main") as pg:
        # no particular reason to create the layers like this, but we are sure
        # not to hit the image_creation_threshold here.
        with pg.cursor() as cur:
            cur.execute("create table a as select id::bigint from generate_series(1, 204800) s(id)")
        wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

        with pg.cursor() as cur:
            cur.execute("update a set id = -id")
        wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    assert not layers.in_memory_layers, "no inmemory layers expected after post-commit checkpoint"
    assert len(layers.historic_layers) == 1 + 2, "should have inidb layer and 2 deltas"

    for layer in layers.historic_layers:
        log.info(f"pre-compact:  {layer}")
        pageserver_http.evict_layer(tenant_id, timeline_id, layer.layer_file_name)

    env.neon_cli.config_tenant(tenant_id, {"compaction_threshold": "3"})

    pageserver_http.timeline_compact(tenant_id, timeline_id)
    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    for layer in layers.historic_layers:
        log.info(f"post compact: {layer}")
    assert len(layers.historic_layers) == 1, "should have compacted to single layer"


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.MOCK_S3])
def test_compaction_downloads_on_demand_with_image_creation(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    """
    Create layers, compact with high image_creation_threshold, then run final compaction with all layers evicted.

    Due to current implementation, this will make image creation on-demand download layers, but we cannot really
    directly test for it.
    """
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_compaction_downloads_on_demand",
    )

    env = neon_env_builder.init_start()

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
        # pitr_interval and gc_horizon are not interesting because we dont run gc
    }

    # Override defaults, to create more layers
    tenant_id, timeline_id = env.neon_cli.create_tenant(conf=stringify(conf))
    env.initial_tenant = tenant_id
    pageserver_http = env.pageserver.http_client()

    with env.postgres.create_start("main") as pg:
        # no particular reason to create the layers like this, but we are sure
        # not to hit the image_creation_threshold here.
        with pg.cursor() as cur:
            cur.execute("create table a (id bigserial primary key, some_value bigint not null)")
            cur.execute("insert into a(some_value) select i from generate_series(1, 10000) s(i)")
        wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

        for _ in range(0, 2):
            for i in range(0, 3):
                # create a minimal amount of "delta difficulty" for this table
                with pg.cursor() as cur:
                    cur.execute("update a set some_value = -some_value + %s", (i,))

                with pg.cursor() as cur:
                    # vacuuming should aid to reuse keys, though it's not really important
                    # with image_creation_threshold=1 which we will use on the last compaction
                    cur.execute("vacuum")

                wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)
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

    kinds_before: DefaultDict[str, int] = defaultdict(int)

    for layer in layers.historic_layers:
        kinds_before[layer.kind] += 1
        pageserver_http.evict_layer(tenant_id, timeline_id, layer.layer_file_name)

    assert dict(kinds_before) == {"Delta": 4}

    # now having evicted all layers, reconfigure to have lower image creation
    # threshold to expose image creation to downloading all of the needed
    # layers -- threshold of 2 would sound more reasonable, but keeping it as 1
    # to be less flaky
    env.neon_cli.config_tenant(tenant_id, {"image_creation_threshold": "1"})

    pageserver_http.timeline_compact(tenant_id, timeline_id)
    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    kinds_after: DefaultDict[str, int] = defaultdict(int)
    for layer in layers.historic_layers:
        kinds_after[layer.kind] += 1

    assert dict(kinds_after) == {"Delta": 4, "Image": 1}


def stringify(conf: Dict[str, Any]) -> Dict[str, str]:
    return dict(map(lambda x: (x[0], str(x[1])), conf.items()))
