import enum
from concurrent.futures import ThreadPoolExecutor
from typing import List

import pytest
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import HistoricLayerInfo, PageserverApiException
from fixtures.pageserver.utils import wait_timeline_detail_404
from fixtures.types import Lsn, TimelineId
from fixtures.utils import wait_until


def by_end_lsn(info: HistoricLayerInfo) -> Lsn:
    assert info.lsn_end is not None
    return Lsn(info.lsn_end)


def layer_name(info: HistoricLayerInfo) -> str:
    return info.layer_file_name


@enum.unique
class Branchpoint(str, enum.Enum):
    """
    Have branches at these Lsns possibly relative to L0 layer boundary.
    """

    EARLIER = "earlier"
    AT_L0 = "at"
    AFTER_L0 = "after"
    LAST_RECORD_LSN = "head"

    def __str__(self) -> str:
        return self.value

    @staticmethod
    def all() -> List["Branchpoint"]:
        return [
            Branchpoint.EARLIER,
            Branchpoint.AT_L0,
            Branchpoint.AFTER_L0,
            Branchpoint.LAST_RECORD_LSN,
        ]


@pytest.mark.parametrize("branchpoint", Branchpoint.all())
@pytest.mark.parametrize("restart_after", [True, False])
def test_ancestor_detach_branched_from(
    neon_env_builder: NeonEnvBuilder, branchpoint: Branchpoint, restart_after: bool
):
    """
    Creates a branch relative to L0 lsn boundary according to Branchpoint. Later the timeline is detached.
    """
    # TODO: parametrize; currently unimplemented over at pageserver
    write_to_branch_first = True

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
        }
    )

    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo (i BIGINT);")

        after_first_tx = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(0, 8191) g(i);")

        # create a single layer for us to remote copy
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(8192, 16383) g(i);")
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    deltas = client.layer_map_info(env.initial_tenant, env.initial_timeline).delta_layers()
    # there is also the in-mem layer, but ignore it for now
    assert len(deltas) == 2, "expecting there to be two deltas: initdb and checkpointed"
    later_delta = max(deltas, key=by_end_lsn)
    assert later_delta.lsn_end is not None

    # -1 as the lsn_end is exclusive.
    last_lsn = Lsn(later_delta.lsn_end).lsn_int - 1

    if branchpoint == Branchpoint.EARLIER:
        branch_at = after_first_tx
        rows = 0
        truncated_layers = 1
    elif branchpoint == Branchpoint.AT_L0:
        branch_at = Lsn(last_lsn)
        rows = 8192
        truncated_layers = 0
    elif branchpoint == Branchpoint.AFTER_L0:
        branch_at = Lsn(last_lsn + 8)
        rows = 8192
        # as there is no 8 byte walrecord, nothing should get copied from the straddling layer
        truncated_layers = 0
    else:
        # this case also covers the implicit flush of ancestor as the inmemory hasn't been flushed yet
        assert branchpoint == Branchpoint.LAST_RECORD_LSN
        branch_at = None
        rows = 16384
        truncated_layers = 0

    name = "new main"

    timeline_id = env.neon_cli.create_branch(
        name, "main", env.initial_tenant, ancestor_start_lsn=branch_at
    )

    recorded = Lsn(client.timeline_detail(env.initial_tenant, timeline_id)["ancestor_lsn"])
    if branch_at is None:
        # fix it up if we need it later (currently unused)
        branch_at = recorded
    else:
        assert branch_at == recorded, "the test should not use unaligned lsns"

    if write_to_branch_first:
        with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
            # make sure the ep is writable
            # with BEFORE_L0, AFTER_L0 there will be a gap in Lsns caused by accurate end_lsn on straddling layers
            ep.safe_psql("CREATE TABLE audit AS SELECT 1 as starts;")
            wait_for_last_flush_lsn(env, ep, env.initial_tenant, timeline_id)

        # branch must have a flush for "PREV_LSN: none"
        client.timeline_checkpoint(env.initial_tenant, timeline_id)
        branch_layers = set(
            map(layer_name, client.layer_map_info(env.initial_tenant, timeline_id).historic_layers)
        )
    else:
        branch_layers = set()

    all_reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
    assert all_reparented == set()

    if restart_after:
        env.pageserver.stop()
        env.pageserver.start()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384

    with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    old_main = client.layer_map_info(env.initial_tenant, env.initial_timeline)
    old_main = set(map(layer_name, old_main.historic_layers))

    new_main = client.layer_map_info(env.initial_tenant, timeline_id)
    new_main = set(map(layer_name, new_main.historic_layers))

    new_main_copied_or_truncated = new_main - branch_layers
    new_main_truncated = new_main_copied_or_truncated - old_main

    assert len(new_main_truncated) == truncated_layers
    # could additionally check that the symmetric difference has layers starting at the same lsn
    # but if nothing was copied, then there is no nice rule.
    # there could be a hole in LSNs between copied from the "old main" and the first branch layer.

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline, 10, 1.0)


@pytest.mark.parametrize("restart_after", [True, False])
def test_ancestor_detach_reparents_earlier(neon_env_builder: NeonEnvBuilder, restart_after: bool):
    """
    The case from RFC:

                              +-> another branch with same ancestor_lsn as new main
                              |
    old main -------|---------X--------->
                    |         |         |
                    |         |         +-> after
                    |         |
                    |         +-> new main
                    |
                    +-> reparented

    Ends up as:

    old main --------------------------->
                                        |
                                        +-> after

                              +-> another branch with same ancestor_lsn as new main
                              |
    new main -------|---------|->
                    |
                    +-> reparented

    We confirm the end result by being able to delete "old main" after deleting "after".
    """

    # TODO: support not yet implemented for these
    write_to_branch_first = True

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
        }
    )

    env.pageserver.allowed_errors.append(
        ".*initial size calculation failed: downloading failed, possibly for shutdown"
    )

    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo (i BIGINT);")
        ep.safe_psql("CREATE TABLE audit AS SELECT 1 as starts;")

        branchpoint_pipe = wait_for_last_flush_lsn(
            env, ep, env.initial_tenant, env.initial_timeline
        )

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(0, 8191) g(i);")

        branchpoint_x = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(8192, 16383) g(i);")
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    # as this only gets reparented, we don't need to write to it like new main
    reparented = env.neon_cli.create_branch(
        "reparented", "main", env.initial_tenant, ancestor_start_lsn=branchpoint_pipe
    )

    same_branchpoint = env.neon_cli.create_branch(
        "same_branchpoint", "main", env.initial_tenant, ancestor_start_lsn=branchpoint_x
    )

    timeline_id = env.neon_cli.create_branch(
        "new main", "main", env.initial_tenant, ancestor_start_lsn=branchpoint_x
    )

    after = env.neon_cli.create_branch("after", "main", env.initial_tenant, ancestor_start_lsn=None)

    if write_to_branch_first:
        with env.endpoints.create_start("new main", tenant_id=env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 8192
            with ep.cursor() as cur:
                cur.execute("UPDATE audit SET starts = starts + 1")
                assert cur.rowcount == 1
            wait_for_last_flush_lsn(env, ep, env.initial_tenant, timeline_id)

        client.timeline_checkpoint(env.initial_tenant, timeline_id)

    all_reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
    assert all_reparented == set([reparented, same_branchpoint])

    if restart_after:
        env.pageserver.stop()
        env.pageserver.start()

    env.pageserver.quiesce_tenants()

    # checking the ancestor after is much faster than waiting for the endpoint not start
    expected_result = [
        ("main", env.initial_timeline, None, 16384, 1),
        ("after", after, env.initial_timeline, 16384, 1),
        ("new main", timeline_id, None, 8192, 2),
        ("same_branchpoint", same_branchpoint, timeline_id, 8192, 1),
        ("reparented", reparented, timeline_id, 0, 1),
    ]

    for _, timeline_id, expected_ancestor, _, _ in expected_result:
        details = client.timeline_detail(env.initial_tenant, timeline_id)
        ancestor_timeline_id = details["ancestor_timeline_id"]
        if expected_ancestor is None:
            assert ancestor_timeline_id is None
        else:
            assert TimelineId(ancestor_timeline_id) == expected_ancestor

    for name, _, _, rows, starts in expected_result:
        with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
            assert ep.safe_psql(f"SELECT count(*) FROM audit WHERE starts = {starts}")[0][0] == 1

    client.timeline_delete(env.initial_tenant, after)
    wait_timeline_detail_404(client, env.initial_tenant, after, 10, 1.0)

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline, 10, 1.0)


@pytest.mark.parametrize("restart_after", [True, False])
def test_detached_receives_flushes_while_being_detached(
    neon_env_builder: NeonEnvBuilder, restart_after: bool
):
    """
    Specifically the flush is received before restart, after making the remote storage change.
    This requires that layer file flushes do not overwrite ancestor_timeline_id.
    """
    write_to_branch_first = True

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
        },
    )

    client = env.pageserver.http_client()

    # row counts have been manually verified to cause reconnections and getpage
    # requests when restart_after=False with pg16
    def insert_rows(n: int, ep) -> int:
        ep.safe_psql(
            f"INSERT INTO foo SELECT i::bigint, 'more info!! this is a long string' || i FROM generate_series(0, {n - 1}) g(i);"
        )
        return n

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo (i BIGINT, aux TEXT NOT NULL);")

        rows = insert_rows(256, ep)

        branchpoint = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    timeline_id = env.neon_cli.create_branch(
        "new main", "main", tenant_id=env.initial_tenant, ancestor_start_lsn=branchpoint
    )

    ep = env.endpoints.create_start("new main", tenant_id=env.initial_tenant)
    assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    if write_to_branch_first:
        rows += insert_rows(256, ep)
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, timeline_id)
        client.timeline_checkpoint(env.initial_tenant, timeline_id)

    failpoint_name = "timeline-ancestor-detach-before-restart-pausable"
    client.configure_failpoints((failpoint_name, "pause"))

    with ThreadPoolExecutor(max_workers=1) as exec:
        completion = exec.submit(client.detach_ancestor, env.initial_tenant, timeline_id)

        wait_until(
            10,
            0.5,
            lambda: env.pageserver.assert_log_contains(f".*at failpoint.*{failpoint_name}"),
        )

        historic_before = len(
            client.layer_map_info(env.initial_tenant, timeline_id).historic_layers
        )

        # just a quick write to make sure we have something to flush
        rows += insert_rows(256, ep)
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, timeline_id)
        client.timeline_checkpoint(env.initial_tenant, timeline_id)

        historic_after = len(client.layer_map_info(env.initial_tenant, timeline_id).historic_layers)
        assert historic_before < historic_after, "actually flushed something"

        client.configure_failpoints((failpoint_name, "off"))

        reparented = completion.result()
        assert len(reparented) == 0

    if restart_after:
        # ep is kept alive on purpose
        env.pageserver.stop()
        env.pageserver.start()

    env.pageserver.quiesce_tenants()

    assert client.timeline_detail(env.initial_tenant, timeline_id)["ancestor_timeline_id"] is None

    # before restart this might all come from shared buffers
    assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
    assert ep.safe_psql("SELECT SUM(LENGTH(aux)) FROM foo")[0][0] != 0
    ep.stop()

    # finally restart the endpoint and make sure we still have the same answer
    with env.endpoints.create_start("new main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows


# @pytest.mark.parametrize("failmode", ["return", "exit"])
def test_retried_ancestor_detach(neon_env_builder: NeonEnvBuilder):
    """
    Fail or restart the pageserver, and retry detaching the ancestor.
    """
    write_to_branch_first = True

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
            "pitr_interval": "0s",
            "compaction_period": "0s",
            "checkpoint_distance": f"{128 * 1024}",
            "compaction_target_size": f"{96 * 1024}",
            "image_layer_creation_check_threshold": "0",
            "compaction_threshold": "3",
        },
    )

    def insert_rows(n: int, ep, i_from: int = 0) -> int:
        start = i_from
        end = i_from + n - 1
        ep.safe_psql(
            f"INSERT INTO foo SELECT i::bigint, 'more info!! this is a long string' || i FROM generate_series({start}, {end}) g(i);"
        )
        return n

    rows = 0
    batch = 128
    assert (batch // 2) > 0

    client = env.pageserver.http_client()
    waited_lsns = []

    # try to create interesting enough
    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo (i BIGINT, aux TEXT NOT NULL);")

        # batch size is 10, so hopefully we have more; this creates about 300 layers on pg16 in single iteration
        for _ in range(4):
            rows += insert_rows(batch, ep, rows)

            # this attempts to write rows, delete half of them, update the remaining half, then vacuum
            ep.safe_psql(f"DELETE FROM foo WHERE i >= {rows - batch} AND i < {rows - (batch // 2)}")
            rows -= batch // 2
            ep.safe_psql(
                f"UPDATE foo SET i = i - {batch // 2} WHERE i >= {rows} AND i < {rows + (batch // 2)}"
            )
            ep.safe_psql("VACUUM FULL")

            lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
            waited_lsns.append(lsn)
            client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        assert (
            len(client.layer_map_info(env.initial_tenant, env.initial_timeline).historic_layers)
            > 200
        )
        assert len(waited_lsns) == 4

        reparented_old = env.neon_cli.create_branch(
            "reparented_old",
            "main",
            tenant_id=env.initial_tenant,
            ancestor_start_lsn=waited_lsns[0],
        )

        reparented_newer = env.neon_cli.create_branch(
            "reparented_newer",
            "main",
            tenant_id=env.initial_tenant,
            ancestor_start_lsn=waited_lsns[2],
        )

        detached_timeline = env.neon_cli.create_branch(
            "detached", "main", tenant_id=env.initial_tenant, ancestor_start_lsn=waited_lsns[3]
        )

    if write_to_branch_first:
        with env.endpoints.create_start("detached", tenant_id=env.initial_tenant) as ep:
            rows += insert_rows(batch // 2, ep, rows)
            wait_for_last_flush_lsn(env, ep, env.initial_tenant, detached_timeline)
            client.timeline_checkpoint(env.initial_tenant, detached_timeline)

    # FIXME: a compaction during detach is ok, because we will always get a consistent view of the layers, but we might need to compact again...?

    failpoints = [
        ("timeline-ancestor-after-one-rewritten", "return"),
        ("timeline-ancestor-after-rewrite-batch", "return"),
        ("timeline-ancestor-after-rewrite-fsync", "return"),
        ("timeline-ancestor-after-copy-batch", "return"),
        ("timeline-ancestor-reparent", "return"),
        ("timeline-ancestor-detach-before-detach", "return"),
        ("timeline-ancestor-detach-before-restart", "return"),
    ]

    client.configure_failpoints(failpoints)

    for index, (name, _) in enumerate(failpoints):
        if name == "timeline-ancestor-reparent":
            matcher = "some reparentings failed, please retry"
        else:
            matcher = f"failpoint: {failpoints[index][0]}"

        with pytest.raises(PageserverApiException, match=matcher):
            client.detach_ancestor(env.initial_tenant, detached_timeline, batch_size=10)

        client.configure_failpoints((failpoints[index][0], "off"))

    reparented = client.detach_ancestor(env.initial_tenant, detached_timeline)
    assert reparented == set([reparented_old, reparented_newer])

    env.pageserver.allowed_errors.append(
        ".* request\\{method=POST path=/v1/tenant/[0-9a-f]+/timeline/[0-9a-f]+/detach_ancestor request_id=[-0-9a-f]+\\}: Error processing HTTP request: InternalServerError\\(failpoint:.*"
    )
    env.pageserver.allowed_errors.append(
        ".* request\\{method=POST path=/v1/tenant/[0-9a-f]+/timeline/[0-9a-f]+/detach_ancestor request_id=[-0-9a-f]+\\}: Error processing HTTP request: InternalServerError\\(some reparentings failed, please retry"
    )


# TODO:
# - after starting the operation, tenant is deleted
# - after starting the operation, pageserver is shutdown, restarted
# - after starting the operation, bottom-most timeline is deleted, pageserver is restarted, gc is inhibited
# - deletion of reparented while reparenting should fail once, then succeed (?)
# - branch near existing L1 boundary, image layers?
# - investigate: why are layers started at uneven lsn? not just after branching, but in general.
