from __future__ import annotations

from contextlib import closing

import pytest
from fixtures.compare_fixtures import NeonCompare
from fixtures.log_helper import log
from fixtures.neon_fixtures import wait_for_last_flush_lsn


#
# Test compaction and image layer creation performance.
#
# This creates a few tables and runs some simple INSERTs and UPDATEs on them to generate
# some delta layers. Then it runs manual compaction, measuring how long it takes.
#
@pytest.mark.timeout(1000)
def test_compaction(neon_compare: NeonCompare):
    env = neon_compare.env
    pageserver_http = env.pageserver.http_client()

    tenant_id, timeline_id = env.create_tenant(
        conf={
            # Disable background GC and compaction, we'll run compaction manually.
            "gc_period": "0s",
            "compaction_period": "0s",
            # Make checkpoint distance somewhat smaller than default, to create
            # more delta layers quicker, to trigger compaction.
            "checkpoint_distance": "25000000",  # 25 MB
            # Force image layer creation when we run compaction.
            "image_creation_threshold": "1",
        }
    )
    neon_compare.tenant = tenant_id
    neon_compare.timeline = timeline_id

    # Create some tables, and run a bunch of INSERTs and UPDATes on them,
    # to generate WAL and layers
    endpoint = env.endpoints.create_start(
        "main", tenant_id=tenant_id, config_lines=["shared_buffers=512MB"]
    )

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            for i in range(100):
                cur.execute(f"create table tbl{i} (i int, j int);")
                cur.execute(f"insert into tbl{i} values (generate_series(1, 1000), 0);")
                for j in range(100):
                    cur.execute(f"update tbl{i} set j = {j};")

    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    # First compaction generates L1 layers
    with neon_compare.zenbenchmark.record_duration("compaction"):
        pageserver_http.timeline_compact(tenant_id, timeline_id)

    # And second compaction triggers image layer creation
    with neon_compare.zenbenchmark.record_duration("image_creation"):
        pageserver_http.timeline_compact(tenant_id, timeline_id)

    neon_compare.report_size()


def test_compaction_l0_memory(neon_compare: NeonCompare):
    """
    Generate a large stack of L0s pending compaction into L1s, and
    measure the pageserver's peak RSS while doing so
    """

    env = neon_compare.env
    pageserver_http = env.pageserver.http_client()

    tenant_id, timeline_id = env.create_tenant(
        conf={
            # Initially disable compaction so that we will build up a stack of L0s
            "compaction_period": "0s",
            "gc_period": "0s",
            "compaction_upper_limit": 12,
        }
    )
    neon_compare.tenant = tenant_id
    neon_compare.timeline = timeline_id

    endpoint = env.endpoints.create_start(
        "main", tenant_id=tenant_id, config_lines=["shared_buffers=512MB"]
    )

    # Read tenant effective config and assert on checkpoint_distance and compaction_threshold,
    # as we do want to test with defaults (to be same as the field), but this test's workload size makes assumptions about them.
    #
    # If these assertions fail, it probably means we changed the default.
    tenant_conf = pageserver_http.tenant_config(tenant_id)
    assert tenant_conf.effective_config["checkpoint_distance"] == 256 * 1024 * 1024
    assert tenant_conf.effective_config["compaction_threshold"] == 10
    assert tenant_conf.effective_config["compaction_upper_limit"] == 12

    # Aim to write about 20 L0s, so that we will hit the limit on how many
    # to compact at once
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            for i in range(200):
                cur.execute(f"create table tbl{i} (i int, j int);")
                cur.execute(f"insert into tbl{i} values (generate_series(1, 1000), 0);")
                for j in range(100):
                    cur.execute(f"update tbl{i} set j = {j};")

    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    pageserver_http.timeline_checkpoint(
        tenant_id, timeline_id, compact=False
    )  # ^1: flush all in-memory layers
    endpoint.stop()

    # Check we have generated the L0 stack we expected
    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    initial_l0s = len(layers.delta_l0_layers())
    initial_l0s_size = sum(x.layer_file_size for x in layers.delta_l0_layers())
    log.info(f"l0s before compaction {initial_l0s} ({initial_l0s_size})")

    def rss_hwm():
        v = pageserver_http.get_metric_value("libmetrics_maxrss_kb")
        assert v is not None
        assert v > 0
        return v * 1024

    before = rss_hwm()
    pageserver_http.timeline_compact(
        tenant_id, timeline_id
    )  # ^1: we must ensure during this process no new L0 layers are flushed
    after = rss_hwm()

    log.info(f"RSS across compaction: {before} -> {after} (grew {after - before})")

    layers = pageserver_http.layer_map_info(tenant_id, timeline_id)
    final_l0s_size = sum(x.layer_file_size for x in layers.delta_l0_layers())
    log.info(f"l0s after compaction {len(layers.delta_l0_layers())} ({final_l0s_size})")

    assert after > before  # If we didn't use some memory the test is probably buggy
    compaction_mapped_rss = after - before

    # During L0 compaction, we require as much memory as the physical size of what we compacted, and then some,
    # because the key->value mapping in L0s compaction is exhaustive, non-streaming, and does not de-duplicate
    # repeated references to the same key.
    #
    # To be fixed in https://github.com/neondatabase/neon/issues/8184, after which
    # this memory estimate can be revised far downwards to something that doesn't scale
    # linearly with the layer sizes.
    MEMORY_ESTIMATE = (initial_l0s_size - final_l0s_size) * 1.25

    # If we find that compaction is using more memory, this may indicate a regression
    assert compaction_mapped_rss < MEMORY_ESTIMATE

    # If we find that compaction is using <0.5 the expected memory then:
    # - maybe we made a big efficiency improvement, in which case update the test
    # - maybe something is functionally wrong with the test and it's not driving the system as expected
    assert compaction_mapped_rss > MEMORY_ESTIMATE / 2

    # We should have compacted some but not all of the l0s, based on the limit on how much
    # l0 to compact in one go
    assert len(layers.delta_l0_layers()) > 0
    assert len(layers.delta_l0_layers()) < initial_l0s

    # The pageserver should have logged when it hit the compaction size limit
    env.pageserver.assert_log_contains(".*hit max delta layer size limit.*")
