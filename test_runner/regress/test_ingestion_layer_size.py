from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_last_flush_lsn
from fixtures.pageserver.http import HistoricLayerInfo, LayerMapInfo
from fixtures.utils import human_bytes, skip_in_debug_build


@skip_in_debug_build("debug run is unnecessarily slow")
def test_ingesting_large_batches_of_images(neon_env_builder: NeonEnvBuilder):
    """
    Build a non-small GIN index which includes similarly batched up images in WAL stream as does pgvector
    to show that we no longer create oversized layers.
    """

    minimum_initdb_size = 20 * 1024**2
    checkpoint_distance = 32 * 1024**2
    minimum_good_layer_size = checkpoint_distance * 0.9
    minimum_too_large_layer_size = 2 * checkpoint_distance

    # index size: 99MiB
    rows = 2_500_000

    # bucket lower limits
    buckets = [0, minimum_initdb_size, minimum_good_layer_size, minimum_too_large_layer_size]

    assert (
        minimum_initdb_size < minimum_good_layer_size
    ), "keep checkpoint_distance higher than the initdb size (find it by experimenting)"

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "checkpoint_distance": f"{checkpoint_distance}",
            "compaction_target_size": f"{checkpoint_distance}",
            # this test is primarly interested in L0 sizes but we'll compact after ingestion to ensure sizes are good even then
            "compaction_period": "0s",
            "gc_period": "0s",
            "compaction_threshold": "255",
            "image_creation_threshold": "99999",
        }
    )

    # build a larger than 3*checkpoint_distance sized gin index.
    # gin index building exhibits the same behaviour as the pgvector with the two phase build
    with env.endpoints.create_start("main") as ep, ep.cursor() as cur:
        cur.execute(
            f"create table int_array_test as select array_agg(g) as int_array from generate_series(1, {rows}) g group by g / 10;"
        )
        cur.execute(
            "create index int_array_test_gin_index on int_array_test using gin (int_array);"
        )
        cur.execute("select pg_table_size('int_array_test_gin_index')")
        size = cur.fetchone()
        assert size is not None
        assert isinstance(size[0], int)
        log.info(f"gin index size: {human_bytes(size[0])}")
        assert (
            size[0] > checkpoint_distance * 3
        ), f"gin index is not large enough: {human_bytes(size[0])}"
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    ps_http = env.pageserver.http_client()
    ps_http.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

    infos = ps_http.layer_map_info(env.initial_tenant, env.initial_timeline)
    assert len(infos.in_memory_layers) == 0, "should had flushed open layers"
    post_ingest = histogram_historic_layers(infos, buckets)

    # describe first, assert later for easier debugging
    log.info("non-cumulative layer size distribution after ingestion:")
    print_layer_size_histogram(post_ingest)

    # since all we have are L0s, we should be getting nice L1s and images out of them now
    env.storage_controller.pageserver_api().update_tenant_config(
        env.initial_tenant,
        {
            "compaction_threshold": 1,
            "image_creation_threshold": 1,
        },
    )

    ps_http.timeline_compact(env.initial_tenant, env.initial_timeline, True, True)

    infos = ps_http.layer_map_info(env.initial_tenant, env.initial_timeline)
    assert len(infos.in_memory_layers) == 0, "no new inmem layers expected"
    post_compact = histogram_historic_layers(infos, buckets)

    log.info("non-cumulative layer size distribution after compaction:")
    print_layer_size_histogram(post_compact)

    assert (
        post_ingest.counts[3] == 0
    ), f"there should be no layers larger than 2*checkpoint_distance ({human_bytes(2*checkpoint_distance)})"
    assert post_ingest.counts[1] == 1, "expect one smaller layer for initdb"
    assert (
        post_ingest.counts[0] <= 1
    ), "expect at most one tiny layer from shutting down the endpoint"

    # just make sure we don't have trouble splitting the layers apart
    assert post_compact.counts[3] == 0


@dataclass
class Histogram:
    buckets: list[int | float]
    counts: list[int]
    sums: list[int]


def histogram_historic_layers(infos: LayerMapInfo, minimum_sizes: list[int | float]) -> Histogram:
    def log_layer(layer: HistoricLayerInfo) -> HistoricLayerInfo:
        log.info(
            f"{layer.layer_file_name} {human_bytes(layer.layer_file_size)} ({layer.layer_file_size} bytes)"
        )
        return layer

    layers = map(log_layer, infos.historic_layers)
    sizes = (x.layer_file_size for x in layers)
    return histogram(sizes, minimum_sizes)


def histogram(sizes: Iterable[int], minimum_sizes: list[int | float]) -> Histogram:
    assert all(minimum_sizes[i] < minimum_sizes[i + 1] for i in range(len(minimum_sizes) - 1))
    buckets = list(enumerate(minimum_sizes))
    counts = [0 for _ in buckets]
    sums = [0 for _ in buckets]

    for size in sizes:
        found = False
        for index, min_size in reversed(buckets):
            if size >= min_size:
                counts[index] += 1
                sums[index] += size
                found = True
                break
        assert found

    return Histogram(minimum_sizes, counts, sums)


def print_layer_size_histogram(h: Histogram):
    for index, min_size in enumerate(h.buckets):
        log.info(
            f">= {human_bytes(min_size)}: {h.counts[index]} layers total {human_bytes(h.sums[index])}"
        )
