from __future__ import annotations

import json

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


def gc_feedback_impl(neon_env_builder: NeonEnvBuilder, zenbenchmark: NeonBenchmarker, mode: str):
    assert mode == "normal" or mode == "with_snapshots"
    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    tenant_id, _ = env.create_tenant(
        conf={
            # disable default GC and compaction
            "gc_period": "1000 m",
            "compaction_period": "0 s",
            "gc_horizon": f"{1024 ** 2}",
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            # set PITR interval to be small, so we can do GC
            "pitr_interval": "10 s",
            # "compaction_threshold": "3",
            # "image_creation_threshold": "2",
        }
    )
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    timeline_id = endpoint.safe_psql("show neon.timeline_id")[0][0]
    n_steps = 10
    n_update_iters = 100
    step_size = 10000
    branch_created = 0
    with endpoint.cursor() as cur:
        cur.execute("SET statement_timeout='1000s'")
        cur.execute(
            "CREATE TABLE t(step bigint, count bigint default 0, payload text default repeat(' ', 100))  with (fillfactor=50)"
        )
        cur.execute("CREATE INDEX ON t(step)")
        # In each step, we insert 'step_size' new rows, and update the newly inserted rows
        # 'n_update_iters' times. This creates a lot of churn and generates lots of WAL at the end of the table,
        # without modifying the earlier parts of the table.
        for step in range(n_steps):
            cur.execute(f"INSERT INTO t (step) SELECT {step} FROM generate_series(1, {step_size})")
            for _ in range(n_update_iters):
                cur.execute(f"UPDATE t set count=count+1 where step = {step}")
                cur.execute("vacuum t")

            # cur.execute("select pg_table_size('t')")
            # logical_size = cur.fetchone()[0]
            logical_size = client.timeline_detail(tenant_id, timeline_id)["current_logical_size"]
            log.info(f"Logical storage size  {logical_size}")

            client.timeline_checkpoint(tenant_id, timeline_id)

            # Do compaction and GC
            client.timeline_gc(tenant_id, timeline_id, 0)
            client.timeline_compact(tenant_id, timeline_id)
            # One more iteration to check that no excessive image layers are generated
            client.timeline_gc(tenant_id, timeline_id, 0)
            client.timeline_compact(tenant_id, timeline_id)

            physical_size = client.timeline_detail(tenant_id, timeline_id)["current_physical_size"]
            log.info(f"Physical storage size {physical_size}")
        if mode == "with_snapshots":
            if step == n_steps / 2:
                env.create_branch("child")
                branch_created += 1

    max_num_of_deltas_above_image = 0
    max_total_num_of_deltas = 0
    for key_range in client.perf_info(tenant_id, timeline_id):
        max_total_num_of_deltas = max(max_total_num_of_deltas, key_range["total_num_of_deltas"])
        max_num_of_deltas_above_image = max(
            max_num_of_deltas_above_image, key_range["num_of_deltas_above_image"]
        )

    MB = 1024 * 1024
    zenbenchmark.record("logical_size", logical_size // MB, "Mb", MetricReport.LOWER_IS_BETTER)
    zenbenchmark.record("physical_size", physical_size // MB, "Mb", MetricReport.LOWER_IS_BETTER)
    zenbenchmark.record(
        "physical/logical ratio", physical_size / logical_size, "", MetricReport.LOWER_IS_BETTER
    )
    zenbenchmark.record(
        "max_total_num_of_deltas", max_total_num_of_deltas, "", MetricReport.LOWER_IS_BETTER
    )
    zenbenchmark.record(
        "max_num_of_deltas_above_image",
        max_num_of_deltas_above_image,
        "",
        MetricReport.LOWER_IS_BETTER,
    )

    client.timeline_compact(tenant_id, timeline_id, enhanced_gc_bottom_most_compaction=True)
    tline_detail = client.timeline_detail(tenant_id, timeline_id)
    logical_size = tline_detail["current_logical_size"]
    physical_size = tline_detail["current_physical_size"]

    max_num_of_deltas_above_image = 0
    max_total_num_of_deltas = 0
    for key_range in client.perf_info(tenant_id, timeline_id):
        max_total_num_of_deltas = max(max_total_num_of_deltas, key_range["total_num_of_deltas"])
        max_num_of_deltas_above_image = max(
            max_num_of_deltas_above_image, key_range["num_of_deltas_above_image"]
        )
    zenbenchmark.record(
        "logical_size_after_bottom_most_compaction",
        logical_size // MB,
        "Mb",
        MetricReport.LOWER_IS_BETTER,
    )
    zenbenchmark.record(
        "physical_size_after_bottom_most_compaction",
        physical_size // MB,
        "Mb",
        MetricReport.LOWER_IS_BETTER,
    )
    zenbenchmark.record(
        "physical/logical ratio after bottom_most_compaction",
        physical_size / logical_size,
        "",
        MetricReport.LOWER_IS_BETTER,
    )
    zenbenchmark.record(
        "max_total_num_of_deltas_after_bottom_most_compaction",
        max_total_num_of_deltas,
        "",
        MetricReport.LOWER_IS_BETTER,
    )
    zenbenchmark.record(
        "max_num_of_deltas_above_image_after_bottom_most_compaction",
        max_num_of_deltas_above_image,
        "",
        MetricReport.LOWER_IS_BETTER,
    )

    with endpoint.cursor() as cur:
        cur.execute("SELECT * FROM t")  # ensure data is not corrupted

    layer_map_path = env.repo_dir / "layer-map.json"
    log.info(f"Writing layer map to {layer_map_path}")
    with layer_map_path.open("w") as f:
        f.write(json.dumps(client.timeline_layer_map_info(tenant_id, timeline_id)))

    # We should have collected all garbage
    if mode == "normal":
        # in theory we should get physical size ~= logical size, but given that gc interval is 10s,
        # and the layer has indexes that might contribute to the fluctuation, we allow a small margin
        # of 1 here, and the end ratio we are asserting is 1 (margin) + 1 (expected) = 2.
        assert physical_size / logical_size < 2
    elif mode == "with_snapshots":
        assert physical_size / logical_size < (2 + branch_created)


@pytest.mark.timeout(10000)
def test_gc_feedback(neon_env_builder: NeonEnvBuilder, zenbenchmark: NeonBenchmarker):
    """
    Test that GC is able to collect all old layers even if them are forming
    "stairs" and there are not three delta layers since last image layer.

    Information about image layers needed to collect old layers should
    be propagated by GC to compaction task which should take in in account
    when make a decision which new image layers needs to be created.

    NB: this test demonstrates the problem. The source tree contained the
    `gc_feedback` mechanism for about 9 months, but, there were problems
    with it and it wasn't enabled at runtime.
    This PR removed the code: https://github.com/neondatabase/neon/pull/6863

    And the bottom-most GC-compaction epic resolves the problem.
    https://github.com/neondatabase/neon/issues/8002
    """
    gc_feedback_impl(neon_env_builder, zenbenchmark, "normal")


@pytest.mark.timeout(10000)
def test_gc_feedback_with_snapshots(
    neon_env_builder: NeonEnvBuilder, zenbenchmark: NeonBenchmarker
):
    """
    Compared with `test_gc_feedback`, we create a branch without written data (=snapshot) in the middle
    of the benchmark, and the   bottom-most compaction should collect as much garbage as possible below the GC
    horizon. Ideally, there should be images (in an image layer) covering the full range at the branch point,
    and images covering the full key range (in a delta layer) at the GC horizon.
    """
    gc_feedback_impl(neon_env_builder, zenbenchmark, "with_snapshots")
