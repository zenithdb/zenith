from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    flush_ep_to_pageserver,
    wait_for_last_flush_lsn,
    wait_for_wal_insert_lsn,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.pageserver.utils import (
    timeline_delete_wait_completed,
    wait_until_tenant_active,
)
from fixtures.pg_version import PgVersion
from fixtures.utils import skip_in_debug_build, wait_until


def test_empty_tenant_size(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_configs()
    env.start()

    (tenant_id, timeline_id) = env.create_tenant()
    http_client = env.pageserver.http_client()
    initial_size = http_client.tenant_size(tenant_id)

    # we should never have zero, because there should be the initdb "changes"
    assert initial_size > 0, "initial implementation returns ~initdb tenant_size"

    endpoint = env.endpoints.create_start(
        "main",
        tenant_id=tenant_id,
        config_lines=["autovacuum=off", "checkpoint_timeout=10min"],
    )

    with endpoint.cursor() as cur:
        cur.execute("SELECT 1")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

    # The transaction above will make the compute generate a checkpoint.
    # In turn, the pageserver persists the checkpoint. This should only be
    # one key with a size of a couple hundred bytes.
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    size = http_client.tenant_size(tenant_id)

    assert size >= initial_size and size - initial_size < 1024


def test_branched_empty_timeline_size(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    Issue found in production. Because the ancestor branch was under
    gc_horizon, the branchpoint was "dangling" and the computation could not be
    done.

    Assuming gc_horizon = 50
    root:    I      0---10------>20
    branch:              |-------------------I---------->150
                                   gc_horizon
    """
    env = neon_simple_env
    (tenant_id, _) = env.create_tenant()
    http_client = env.pageserver.http_client()

    initial_size = http_client.tenant_size(tenant_id)

    first_branch_timeline_id = env.create_branch("first-branch", tenant_id=tenant_id)

    with env.endpoints.create_start("first-branch", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute(
                "CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
            )
        wait_for_last_flush_lsn(env, endpoint, tenant_id, first_branch_timeline_id)

    size_after_branching = http_client.tenant_size(tenant_id)
    log.info(f"size_after_branching: {size_after_branching}")

    assert size_after_branching > initial_size

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


def test_branched_from_many_empty_parents_size(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    More general version of test_branched_empty_timeline_size

    Assuming gc_horizon = 50

    root:  I 0------10
    first: I        10
    nth_0: I        10
    nth_1: I        10
    nth_n:          10------------I--------100
    """
    env = neon_simple_env
    (tenant_id, _) = env.create_tenant()
    http_client = env.pageserver.http_client()

    initial_size = http_client.tenant_size(tenant_id)

    first_branch_name = "first"
    env.create_branch(first_branch_name, tenant_id=tenant_id)

    size_after_branching = http_client.tenant_size(tenant_id)

    # this might be flaky like test_get_tenant_size_with_multiple_branches
    # https://github.com/neondatabase/neon/issues/2962
    assert size_after_branching == initial_size

    last_branch_name = first_branch_name
    last_branch = None

    for i in range(0, 4):
        latest_branch_name = f"nth_{i}"
        last_branch = env.create_branch(
            latest_branch_name, ancestor_branch_name=last_branch_name, tenant_id=tenant_id
        )
        last_branch_name = latest_branch_name

        size_after_branching = http_client.tenant_size(tenant_id)
        assert size_after_branching == initial_size

    assert last_branch is not None

    with env.endpoints.create_start(last_branch_name, tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute(
                "CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
            )
        wait_for_last_flush_lsn(env, endpoint, tenant_id, last_branch)

    size_after_writes = http_client.tenant_size(tenant_id)
    assert size_after_writes > initial_size

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


def test_branch_point_within_horizon(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    gc_horizon = 15

    main:          0--I-10------>20
    branch:              |-------------------I---------->150
                                   gc_horizon
    """

    env = neon_simple_env
    gc_horizon = 20_000
    (tenant_id, main_id) = env.create_tenant(conf={"gc_horizon": str(gc_horizon)})
    http_client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        initdb_lsn = wait_for_last_flush_lsn(env, endpoint, tenant_id, main_id)
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000) s(i)")
        flushed_lsn = wait_for_last_flush_lsn(env, endpoint, tenant_id, main_id)

    size_before_branching = http_client.tenant_size(tenant_id)

    assert flushed_lsn.lsn_int - gc_horizon > initdb_lsn.lsn_int

    branch_id = env.create_branch("branch", tenant_id=tenant_id, ancestor_start_lsn=flushed_lsn)

    with env.endpoints.create_start("branch", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 1000) s(i)")
        wait_for_last_flush_lsn(env, endpoint, tenant_id, branch_id)

    size_after = http_client.tenant_size(tenant_id)

    assert size_before_branching < size_after

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


def test_parent_within_horizon(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    gc_horizon = 5

    main:          0----10----I->20
    branch:              |-------------------I---------->150
                                   gc_horizon
    """

    env = neon_simple_env
    gc_horizon = 5_000
    (tenant_id, main_id) = env.create_tenant(conf={"gc_horizon": str(gc_horizon)})
    http_client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        initdb_lsn = wait_for_last_flush_lsn(env, endpoint, tenant_id, main_id)
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000) s(i)")

        flushed_lsn = wait_for_last_flush_lsn(env, endpoint, tenant_id, main_id)

        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t00 AS SELECT i::bigint n FROM generate_series(0, 2000) s(i)")

        wait_for_last_flush_lsn(env, endpoint, tenant_id, main_id)

    size_before_branching = http_client.tenant_size(tenant_id)

    assert flushed_lsn.lsn_int - gc_horizon > initdb_lsn.lsn_int

    branch_id = env.create_branch("branch", tenant_id=tenant_id, ancestor_start_lsn=flushed_lsn)

    with env.endpoints.create_start("branch", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 10000) s(i)")
        wait_for_last_flush_lsn(env, endpoint, tenant_id, branch_id)

    size_after = http_client.tenant_size(tenant_id)

    assert size_before_branching < size_after

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


def test_only_heads_within_horizon(neon_simple_env: NeonEnv, test_output_dir: Path):
    """
    gc_horizon = small

    main: 0--------10-----I>20
    first:         |-----------------------------I>150
    second:        |---------I>30
    """

    env = neon_simple_env
    (tenant_id, main_id) = env.create_tenant(conf={"gc_horizon": "1024"})
    http_client = env.pageserver.http_client()

    initial_size = http_client.tenant_size(tenant_id)

    first_id = env.create_branch("first", tenant_id=tenant_id)
    second_id = env.create_branch("second", tenant_id=tenant_id)

    ids = {"main": main_id, "first": first_id, "second": second_id}

    latest_size = None

    # gc is not expected to change the results

    for branch_name, amount in [("main", 2000), ("first", 15000), ("second", 3000)]:
        with env.endpoints.create_start(branch_name, tenant_id=tenant_id) as endpoint:
            with endpoint.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, {amount}) s(i)"
                )
            wait_for_last_flush_lsn(env, endpoint, tenant_id, ids[branch_name])
            size_now = http_client.tenant_size(tenant_id)
            if latest_size is not None:
                assert size_now > latest_size
            else:
                assert size_now > initial_size

            latest_size = size_now

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


@skip_in_debug_build("only run with release build")
def test_single_branch_get_tenant_size_grows(
    neon_env_builder: NeonEnvBuilder, test_output_dir: Path, pg_version: PgVersion
):
    """
    Operate on single branch reading the tenants size after each transaction.
    """

    # Disable automatic compaction and GC, and set a long PITR interval: we will expect
    # size to always increase with writes as all writes remain within the PITR
    tenant_config = {
        "compaction_period": "0s",
        "gc_period": "0s",
        "pitr_interval": "3600s",
    }

    env = neon_env_builder.init_start(initial_tenant_conf=tenant_config)

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    branch_name = "main"

    http_client = env.pageserver.http_client()

    collected_responses: list[tuple[str, Lsn, int]] = []

    size_debug_file = open(test_output_dir / "size_debug.html", "w")

    def get_current_consistent_size(
        env: NeonEnv,
        endpoint: Endpoint,
        size_debug_file,  # apparently there is no public signature for open()...
        http_client: PageserverHttpClient,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> tuple[Lsn, int]:
        size = 0
        consistent = False
        size_debug = None

        current_lsn = wait_for_wal_insert_lsn(env, endpoint, tenant_id, timeline_id)
        # We want to make sure we have a self-consistent set of values.
        # Size changes with WAL, so only if both before and after getting
        # the size of the tenant reports the same WAL insert LSN, we're OK
        # to use that (size, LSN) combination.
        # Note that 'wait_for_wal_flush_lsn' is not accurate enough: There
        # can be more wal after the flush LSN that can arrive on the
        # pageserver before we're requesting the page size.
        # Anyway, in general this is only one iteration, so in general
        # this is fine.
        while not consistent:
            size, sizes = http_client.tenant_size_and_modelinputs(tenant_id)
            size_debug = http_client.tenant_size_debug(tenant_id)

            after_lsn = wait_for_wal_insert_lsn(env, endpoint, tenant_id, timeline_id)
            consistent = current_lsn == after_lsn
            current_lsn = after_lsn
        size_debug_file.write(size_debug)
        assert size > 0
        log.info(f"size: {size} at lsn {current_lsn}")
        return (current_lsn, size)

    with env.endpoints.create_start(
        branch_name,
        tenant_id=tenant_id,
        ### autovacuum is disabled to limit WAL logging.
        config_lines=["autovacuum=off"],
    ) as endpoint:
        (initdb_lsn, size) = get_current_consistent_size(
            env, endpoint, size_debug_file, http_client, tenant_id, timeline_id
        )
        collected_responses.append(("INITDB", initdb_lsn, size))

        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t0 (i BIGINT NOT NULL) WITH (fillfactor = 40)")

        (current_lsn, size) = get_current_consistent_size(
            env, endpoint, size_debug_file, http_client, tenant_id, timeline_id
        )
        collected_responses.append(("CREATE", current_lsn, size))

        batch_size = 100
        prev_size = 0
        for i in range(3):
            with endpoint.cursor() as cur:
                cur.execute(
                    f"INSERT INTO t0(i) SELECT i FROM generate_series({batch_size} * %s, ({batch_size} * (%s + 1)) - 1) s(i)",
                    (i, i),
                )

            i += 1

            (current_lsn, size) = get_current_consistent_size(
                env, endpoint, size_debug_file, http_client, tenant_id, timeline_id
            )

            prev_size = collected_responses[-1][2]
            assert size > prev_size

            collected_responses.append(("INSERT", current_lsn, size))

        while True:
            with endpoint.cursor() as cur:
                cur.execute(
                    f"UPDATE t0 SET i = -i WHERE i IN (SELECT i FROM t0 WHERE i > 0 LIMIT {batch_size})"
                )
                updated = cur.rowcount

            if updated == 0:
                break

            (current_lsn, size) = get_current_consistent_size(
                env, endpoint, size_debug_file, http_client, tenant_id, timeline_id
            )

            prev_size = collected_responses[-1][2]
            assert size > prev_size

            collected_responses.append(("UPDATE", current_lsn, size))

        while True:
            with endpoint.cursor() as cur:
                cur.execute(f"DELETE FROM t0 WHERE i IN (SELECT i FROM t0 LIMIT {batch_size})")
                deleted = cur.rowcount

            if deleted == 0:
                break

            (current_lsn, size) = get_current_consistent_size(
                env, endpoint, size_debug_file, http_client, tenant_id, timeline_id
            )

            prev_size = collected_responses[-1][2]
            assert size > prev_size

            collected_responses.append(("DELETE", current_lsn, size))

        size_before_drop = get_current_consistent_size(
            env, endpoint, size_debug_file, http_client, tenant_id, timeline_id
        )[1]

        with endpoint.cursor() as cur:
            cur.execute("DROP TABLE t0")

        # Dropping the table doesn't reclaim any space
        # from the user's point of view, because the DROP transaction is still
        # within pitr_interval.
        (current_lsn, size) = get_current_consistent_size(
            env, endpoint, size_debug_file, http_client, tenant_id, timeline_id
        )
        assert size >= prev_size
        prev_size = size

        # Set a zero PITR interval to allow the DROP to impact the synthetic size
        # Because synthetic size calculation uses pitr interval when available,
        # when our tenant is configured with a tiny pitr interval, dropping a table should
        # cause synthetic size to go down immediately
        tenant_config["pitr_interval"] = "0s"
        env.pageserver.http_client().set_tenant_config(tenant_id, tenant_config)
        (current_lsn, size) = get_current_consistent_size(
            env, endpoint, size_debug_file, http_client, tenant_id, timeline_id
        )
        assert size < size_before_drop

        # The size of the tenant should still be as large as before we dropped
        # the table, because the drop operation can still be undone in the PITR
        # defined by gc_horizon.
        collected_responses.append(("DROP", current_lsn, size))

    # this isn't too many lines to forget for a while. observed while
    # developing these tests that locally the value is a bit more than what we
    # get in the ci.
    for phase, lsn, size in collected_responses:
        log.info(f"collected: {phase}, {lsn}, {size}")

    env.pageserver.stop()
    env.pageserver.start()

    wait_until_tenant_active(http_client, tenant_id)

    size_after = http_client.tenant_size(tenant_id)
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)
    size_debug_file.close()

    prev = collected_responses[-1][2]

    assert size_after == prev, "size after restarting pageserver should not have changed"


def assert_size_approx_equal(size_a, size_b):
    """
    Tests that evaluate sizes are checking the pageserver space consumption
    that sits many layers below the user input.  The exact space needed
    varies slightly depending on postgres behavior.

    Rather than expecting postgres to be determinstic and occasionally
    failing the test, we permit sizes for the same data to vary by a few pages.
    """

    # Determined empirically from examples of equality failures: they differ
    # by page multiples of 8272, and usually by 1-3 pages.  Tolerate 6 to avoid
    # failing on outliers from that observed range.
    threshold = 6 * 8272

    assert size_a == pytest.approx(size_b, abs=threshold)


def test_get_tenant_size_with_multiple_branches(
    neon_env_builder: NeonEnvBuilder, test_output_dir: Path
):
    """
    Reported size goes up while branches or rows are being added, goes down after removing branches.
    """

    gc_horizon = 128 * 1024

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "compaction_period": "0s",
            "gc_period": "0s",
            "pitr_interval": "0sec",
            "gc_horizon": gc_horizon,
        }
    )

    # FIXME: we have a race condition between GC and delete timeline. GC might fail with this
    # error. Similar to https://github.com/neondatabase/neon/issues/2671
    env.pageserver.allowed_errors.append(".*InternalServerError\\(No such file or directory.*")

    tenant_id = env.initial_tenant
    main_timeline_id = env.initial_timeline
    main_branch_name = "main"

    http_client = env.pageserver.http_client()

    main_endpoint = env.endpoints.create_start(main_branch_name, tenant_id=tenant_id)

    batch_size = 10000

    with main_endpoint.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, {batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, main_endpoint, tenant_id, main_timeline_id)
    size_at_branch = http_client.tenant_size(tenant_id)
    assert size_at_branch > 0

    first_branch_timeline_id = env.create_branch(
        "first-branch", ancestor_branch_name=main_branch_name, tenant_id=tenant_id
    )

    size_after_first_branch = http_client.tenant_size(tenant_id)
    assert_size_approx_equal(size_after_first_branch, size_at_branch)

    first_branch_endpoint = env.endpoints.create_start("first-branch", tenant_id=tenant_id)

    with first_branch_endpoint.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, {batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, first_branch_endpoint, tenant_id, first_branch_timeline_id)
    size_after_growing_first_branch = http_client.tenant_size(tenant_id)
    assert size_after_growing_first_branch > size_after_first_branch

    with main_endpoint.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 2*{batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, main_endpoint, tenant_id, main_timeline_id)
    size_after_continuing_on_main = http_client.tenant_size(tenant_id)
    assert size_after_continuing_on_main > size_after_growing_first_branch

    second_branch_timeline_id = env.create_branch(
        "second-branch", ancestor_branch_name=main_branch_name, tenant_id=tenant_id
    )
    size_after_second_branch = http_client.tenant_size(tenant_id)
    assert_size_approx_equal(size_after_second_branch, size_after_continuing_on_main)

    second_branch_endpoint = env.endpoints.create_start("second-branch", tenant_id=tenant_id)

    with second_branch_endpoint.cursor() as cur:
        cur.execute(
            f"CREATE TABLE t2 AS SELECT i::bigint n FROM generate_series(0, 3*{batch_size}) s(i)"
        )

    wait_for_last_flush_lsn(env, second_branch_endpoint, tenant_id, second_branch_timeline_id)
    size_after_growing_second_branch = http_client.tenant_size(tenant_id)
    assert size_after_growing_second_branch > size_after_second_branch

    with second_branch_endpoint.cursor() as cur:
        cur.execute("DROP TABLE t0")
        cur.execute("DROP TABLE t1")
        cur.execute("VACUUM FULL")

    wait_for_last_flush_lsn(env, second_branch_endpoint, tenant_id, second_branch_timeline_id)
    size_after_thinning_branch = http_client.tenant_size(tenant_id)
    assert (
        size_after_thinning_branch > size_after_growing_second_branch
    ), "tenant_size should grow with dropped tables and full vacuum"

    first_branch_endpoint.stop_and_destroy()
    second_branch_endpoint.stop_and_destroy()
    main_endpoint.stop()
    env.pageserver.stop()
    env.pageserver.start()

    wait_until_tenant_active(http_client, tenant_id)

    # chance of compaction and gc on startup might have an effect on the
    # tenant_size but so far this has been reliable, even though at least gc
    # and tenant_size race for the same locks
    size_after = http_client.tenant_size(tenant_id)
    assert_size_approx_equal(size_after, size_after_thinning_branch)

    size_debug_file_before = open(test_output_dir / "size_debug_before.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file_before.write(size_debug)

    # teardown, delete branches, and the size should be going down
    timeline_delete_wait_completed(http_client, tenant_id, first_branch_timeline_id)

    size_after_deleting_first = http_client.tenant_size(tenant_id)
    assert size_after_deleting_first < size_after_thinning_branch

    timeline_delete_wait_completed(http_client, tenant_id, second_branch_timeline_id)
    size_after_deleting_second = http_client.tenant_size(tenant_id)
    assert size_after_deleting_second < size_after_deleting_first

    assert size_after_deleting_second < size_after_continuing_on_main
    assert size_after_deleting_second > size_after_first_branch

    size_debug_file = open(test_output_dir / "size_debug.html", "w")
    size_debug = http_client.tenant_size_debug(tenant_id)
    size_debug_file.write(size_debug)


def test_synthetic_size_while_deleting(neon_env_builder: NeonEnvBuilder):
    """
    Makes sure synthetic size can still be calculated even if one of the
    timelines is deleted or the tenant is deleted.
    """

    env = neon_env_builder.init_start()
    failpoint = "Timeline::find_gc_cutoffs-pausable"
    client = env.pageserver.http_client()

    orig_size = client.tenant_size(env.initial_tenant)

    branch_id = env.create_branch(
        "branch", ancestor_branch_name="main", tenant_id=env.initial_tenant
    )
    client.configure_failpoints((failpoint, "pause"))

    with ThreadPoolExecutor(max_workers=1) as exec:
        completion = exec.submit(client.tenant_size, env.initial_tenant)
        _, last_offset = wait_until(
            lambda: env.pageserver.assert_log_contains(f"at failpoint {failpoint}")
        )

        timeline_delete_wait_completed(client, env.initial_tenant, branch_id)

        client.configure_failpoints((failpoint, "off"))
        size = completion.result()

        assert_size_approx_equal(orig_size, size)

    branch_id = env.create_branch(
        "branch2", ancestor_branch_name="main", tenant_id=env.initial_tenant
    )
    client.configure_failpoints((failpoint, "pause"))

    with ThreadPoolExecutor(max_workers=1) as exec:
        completion = exec.submit(client.tenant_size, env.initial_tenant)
        wait_until(
            lambda: env.pageserver.assert_log_contains(
                f"at failpoint {failpoint}", offset=last_offset
            ),
        )

        client.tenant_delete(env.initial_tenant)

        client.configure_failpoints((failpoint, "off"))

        # accept both, because the deletion might still complete before
        matcher = "(Failed to refresh gc_info before gathering inputs|NotFound: tenant|Conflict: will not become active.  Current state: Stopping)"
        with pytest.raises(PageserverApiException, match=matcher):
            completion.result()

    # this happens only in the case of deletion (http response logging)
    env.pageserver.allowed_errors.append(".*Failed to refresh gc_info before gathering inputs.*")


# Helper for tests that compare timeline_inputs
# We don't want to compare the exact values, because they can be unstable
# and cause flaky tests. So replace the values with useful invariants.
def mask_model_inputs(x):
    if isinstance(x, dict):
        newx = {}
        for k, v in x.items():
            if k == "size":
                if v is None or v == 0:
                    # no change
                    newx[k] = v
                elif v < 0:
                    newx[k] = "<0"
                else:
                    newx[k] = ">0"
            elif k.endswith("lsn") or k.endswith("cutoff") or k == "last_record":
                if v is None or v == 0 or v == "0/0":
                    # no change
                    newx[k] = v
                else:
                    newx[k] = "masked"
            else:
                newx[k] = mask_model_inputs(v)
        return newx
    elif isinstance(x, list):
        newlist = [mask_model_inputs(v) for v in x]
        return newlist
    else:
        return x


@pytest.mark.parametrize("zero_gc", [True, False])
def test_lsn_lease_size(neon_env_builder: NeonEnvBuilder, test_output_dir: Path, zero_gc: bool):
    """
    Compare a LSN lease to a read-only branch for synthetic size calculation.
    They should have the same effect.
    """

    def assert_size_approx_equal_for_lease_test(size_lease, size_branch):
        """
        Tests that evaluate sizes are checking the pageserver space consumption
        that sits many layers below the user input.  The exact space needed
        varies slightly depending on postgres behavior.

        Rather than expecting postgres to be determinstic and occasionally
        failing the test, we permit sizes for the same data to vary by a few pages.
        """

        # FIXME(yuchen): The delta is too large, used as temp solution to pass the test reliably.
        # Investigate and reduce the threshold.
        threshold = 22 * 8272

        log.info(
            f"delta: size_branch({size_branch}) -  size_lease({size_lease}) = {size_branch - size_lease}"
        )

        assert size_lease == pytest.approx(size_branch, abs=threshold)

    conf = {
        "pitr_interval": "0s" if zero_gc else "3600s",
        "gc_period": "0s",
        "compaction_period": "0s",
    }

    env = neon_env_builder.init_start(initial_tenant_conf=conf)

    ro_branch_res = insert_with_action(
        env, env.initial_tenant, env.initial_timeline, test_output_dir, action="branch"
    )

    tenant, timeline = env.create_tenant(conf=conf)
    lease_res = insert_with_action(env, tenant, timeline, test_output_dir, action="lease")

    assert_size_approx_equal_for_lease_test(lease_res, ro_branch_res)

    # we are writing a lot, and flushing all of that to disk is not important for this test
    env.stop(immediate=True)


def insert_with_action(
    env: NeonEnv,
    tenant: TenantId,
    timeline: TimelineId,
    test_output_dir: Path,
    action: str,
) -> int:
    """
    Inserts some data on the timeline, perform an action, and insert more data on the same timeline.
    Returns the size at the end of the insertion.

    Valid actions:
     - "lease": Acquires a lease.
     - "branch": Creates a child branch but never writes to it.
    """

    client = env.pageserver.http_client()
    with env.endpoints.create_start(
        "main",
        tenant_id=tenant,
        config_lines=["autovacuum=off"],
    ) as ep:
        initial_size = client.tenant_size(tenant)
        log.info(f"initial size: {initial_size}")

        with ep.cursor() as cur:
            cur.execute(
                "CREATE TABLE t0 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
            )
        last_flush_lsn = wait_for_last_flush_lsn(env, ep, tenant, timeline)

        if action == "lease":
            res = client.timeline_lsn_lease(tenant, timeline, last_flush_lsn)
            log.info(f"result from lsn_lease api: {res}")
        elif action == "branch":
            ro_branch = env.create_branch(
                "ro_branch", ancestor_start_lsn=last_flush_lsn, tenant_id=tenant
            )
            log.info(f"{ro_branch=} created")
        else:
            raise AssertionError("Invalid action type, only `lease` and `branch`are accepted")

        with ep.cursor() as cur:
            cur.execute(
                "CREATE TABLE t1 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
            )
            cur.execute(
                "CREATE TABLE t2 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
            )
            cur.execute(
                "CREATE TABLE t3 AS SELECT i::bigint n FROM generate_series(0, 1000000) s(i)"
            )

        last_flush_lsn = wait_for_last_flush_lsn(env, ep, tenant, timeline)

        # Avoid flakiness when calculating logical size.
        flush_ep_to_pageserver(env, ep, tenant, timeline)

        size_after_action_and_insert = client.tenant_size(tenant)
        log.info(f"{size_after_action_and_insert=}")

        size_debug_file = open(test_output_dir / f"size_debug_{action}.html", "w")
        size_debug = client.tenant_size_debug(tenant)
        size_debug_file.write(size_debug)
        return size_after_action_and_insert
