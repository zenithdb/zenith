from __future__ import annotations

import datetime
import enum
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from enum import StrEnum
from queue import Empty, Queue
from threading import Barrier

import pytest
from fixtures.common_types import Lsn, TimelineArchivalState, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    LogCursor,
    NeonEnvBuilder,
    PgBin,
    flush_ep_to_pageserver,
    last_flush_lsn_upload,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import HistoricLayerInfo, PageserverApiException
from fixtures.pageserver.utils import wait_for_last_record_lsn, wait_timeline_detail_404
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from fixtures.utils import assert_pageserver_backups_equal, skip_in_debug_build, wait_until
from fixtures.workload import Workload
from requests import ReadTimeout


def by_end_lsn(info: HistoricLayerInfo) -> Lsn:
    assert info.lsn_end is not None
    return Lsn(info.lsn_end)


def layer_name(info: HistoricLayerInfo) -> str:
    return info.layer_file_name


@enum.unique
class Branchpoint(StrEnum):
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
    def all() -> list[Branchpoint]:
        return [
            Branchpoint.EARLIER,
            Branchpoint.AT_L0,
            Branchpoint.AFTER_L0,
            Branchpoint.LAST_RECORD_LSN,
        ]


SHUTDOWN_ALLOWED_ERRORS = [
    ".*initial size calculation failed: downloading failed, possibly for shutdown",
    ".*failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
    ".*logical_size_calculation_task:panic.*: Sequential get failed with Bad state \\(not active\\).*",
    ".*Task 'initial size calculation' .* panicked.*",
]


@pytest.mark.parametrize("branchpoint", Branchpoint.all())
@pytest.mark.parametrize("restart_after", [True, False])
@pytest.mark.parametrize("write_to_branch_first", [True, False])
def test_ancestor_detach_branched_from(
    test_output_dir,
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    branchpoint: Branchpoint,
    restart_after: bool,
    write_to_branch_first: bool,
):
    """
    Creates a branch relative to L0 lsn boundary according to Branchpoint. Later the timeline is detached.
    """
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo (i BIGINT);")

        after_first_tx = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(0, 8191) g(i);")

        # create a single layer for us to remote copy
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(8192, 16383) g(i);")
        flush_ep_to_pageserver(env, ep, env.initial_tenant, env.initial_timeline)

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
        # make sure the branch point is not on a page header
        if 0 < (branch_at.lsn_int % 8192) < 40:
            branch_at += 40
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

    timeline_id = env.create_branch(name, ancestor_branch_name="main", ancestor_start_lsn=branch_at)

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

    # run fullbackup to make sure there are no off by one errors
    # take this on the parent
    fullbackup_before = test_output_dir / "fullbackup-before.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, env.initial_timeline, branch_at, fullbackup_before
    )

    all_reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
    assert all_reparented == set()

    if restart_after:
        env.pageserver.stop()
        env.pageserver.start()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384

    with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    old_main_info = client.layer_map_info(env.initial_tenant, env.initial_timeline)
    old_main = set(map(layer_name, old_main_info.historic_layers))

    new_main_info = client.layer_map_info(env.initial_tenant, timeline_id)
    new_main = set(map(layer_name, new_main_info.historic_layers))

    new_main_copied_or_truncated = new_main - branch_layers
    new_main_truncated = new_main_copied_or_truncated - old_main

    assert len(new_main_truncated) == truncated_layers
    # could additionally check that the symmetric difference has layers starting at the same lsn
    # but if nothing was copied, then there is no nice rule.
    # there could be a hole in LSNs between copied from the "old main" and the first branch layer.

    # take this on the detached, at same lsn
    fullbackup_after = test_output_dir / "fullbackup-after.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, timeline_id, branch_at, fullbackup_after
    )

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline)

    # because we do the fullbackup from ancestor at the branch_lsn, the zenith.signal is always different
    # as there is always "PREV_LSN: invalid" for "before"
    skip_files = {"zenith.signal"}

    assert_pageserver_backups_equal(fullbackup_before, fullbackup_after, skip_files)


def test_ancestor_detach_reparents_earlier(neon_env_builder: NeonEnvBuilder):
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

    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

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
    reparented = env.create_branch(
        "reparented", ancestor_branch_name="main", ancestor_start_lsn=branchpoint_pipe
    )

    same_branchpoint = env.create_branch(
        "same_branchpoint", ancestor_branch_name="main", ancestor_start_lsn=branchpoint_x
    )

    timeline_id = env.create_branch(
        "new main", ancestor_branch_name="main", ancestor_start_lsn=branchpoint_x
    )

    after = env.create_branch("after", ancestor_branch_name="main", ancestor_start_lsn=None)

    all_reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
    assert set(all_reparented) == {reparented, same_branchpoint}

    env.pageserver.quiesce_tenants()

    # checking the ancestor after is much faster than waiting for the endpoint not start
    expected_result = [
        ("main", env.initial_timeline, None, 16384, 1),
        ("after", after, env.initial_timeline, 16384, 1),
        ("new main", timeline_id, None, 8192, 1),
        ("same_branchpoint", same_branchpoint, timeline_id, 8192, 1),
        ("reparented", reparented, timeline_id, 0, 1),
    ]

    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    for _, queried_timeline, expected_ancestor, _, _ in expected_result:
        details = client.timeline_detail(env.initial_tenant, queried_timeline)
        ancestor_timeline_id = details["ancestor_timeline_id"]
        if expected_ancestor is None:
            assert ancestor_timeline_id is None
        else:
            assert TimelineId(ancestor_timeline_id) == expected_ancestor

        index_part = env.pageserver_remote_storage.index_content(
            env.initial_tenant, queried_timeline
        )
        lineage = index_part["lineage"]
        assert lineage is not None

        assert lineage.get("reparenting_history_overflown", "false") == "false"

        if queried_timeline == timeline_id:
            original_ancestor = lineage["original_ancestor"]
            assert original_ancestor is not None
            assert original_ancestor[0] == str(env.initial_timeline)
            assert original_ancestor[1] == str(branchpoint_x)

            # this does not contain Z in the end, so fromisoformat accepts it
            # it is to be in line with the deletion timestamp.. well, almost.
            when = original_ancestor[2][:26]
            when_ts = datetime.datetime.fromisoformat(when)
            assert when_ts < datetime.datetime.now()
            assert len(lineage.get("reparenting_history", [])) == 0
        elif expected_ancestor == timeline_id:
            assert len(lineage.get("original_ancestor", [])) == 0
            assert lineage["reparenting_history"] == [str(env.initial_timeline)]
        else:
            assert len(lineage.get("original_ancestor", [])) == 0
            assert len(lineage.get("reparenting_history", [])) == 0

    for name, _, _, rows, starts in expected_result:
        with env.endpoints.create_start(name, tenant_id=env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
            assert ep.safe_psql(f"SELECT count(*) FROM audit WHERE starts = {starts}")[0][0] == 1

    # delete the timelines to confirm detach actually worked
    client.timeline_delete(env.initial_tenant, after)
    wait_timeline_detail_404(client, env.initial_tenant, after)

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(client, env.initial_tenant, env.initial_timeline)


def test_detached_receives_flushes_while_being_detached(neon_env_builder: NeonEnvBuilder):
    """
    Makes sure that the timeline is able to receive writes through-out the detach process.
    """

    env = neon_env_builder.init_start()

    client = env.pageserver.http_client()

    # row counts have been manually verified to cause reconnections and getpage
    # requests when restart_after=False with pg16
    def insert_rows(n: int, ep) -> int:
        ep.safe_psql(
            f"INSERT INTO foo SELECT i::bigint, 'more info!! this is a long string' || i FROM generate_series(0, {n - 1}) g(i);"
        )
        return n

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("CREATE EXTENSION neon_test_utils;")
        ep.safe_psql("CREATE TABLE foo (i BIGINT, aux TEXT NOT NULL);")

        rows = insert_rows(256, ep)

        branchpoint = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    timeline_id = env.create_branch(
        "new main", ancestor_branch_name="main", ancestor_start_lsn=branchpoint
    )

    log.info("starting the new main endpoint")
    ep = env.endpoints.create_start("new main", tenant_id=env.initial_tenant)
    assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    def small_txs(ep, queue: Queue[str], barrier):
        extra_rows = 0

        with ep.connect() as conn:
            while True:
                try:
                    queue.get_nowait()
                    break
                except Empty:
                    pass

                if barrier is not None:
                    barrier.wait()
                    barrier = None

                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO foo(i, aux) VALUES (1, 'more info!! this is a long string' || 1);"
                )
                extra_rows += 1
        return extra_rows

    with ThreadPoolExecutor(max_workers=1) as exec:
        queue: Queue[str] = Queue()
        barrier = Barrier(2)

        completion = exec.submit(small_txs, ep, queue, barrier)
        barrier.wait()

        reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
        assert len(reparented) == 0

        env.pageserver.quiesce_tenants()

        queue.put("done")
        extra_rows = completion.result()
        assert extra_rows > 0, "some rows should had been written"
        rows += extra_rows

    assert client.timeline_detail(env.initial_tenant, timeline_id)["ancestor_timeline_id"] is None

    ep.clear_buffers()
    assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows
    assert ep.safe_psql("SELECT SUM(LENGTH(aux)) FROM foo")[0][0] != 0
    ep.stop()

    # finally restart the endpoint and make sure we still have the same answer
    with env.endpoints.create_start("new main", tenant_id=env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == rows

    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)


def test_compaction_induced_by_detaches_in_history(
    neon_env_builder: NeonEnvBuilder, test_output_dir, pg_bin: PgBin
):
    """
    Assuming the tree of timelines:

    root
    |- child1
       |- ...
          |- wanted_detached_child

    Each detach can add N more L0 per level, this is actually unbounded because
    compaction can be arbitrarily delayed (or detach happen right before one
    starts). If "wanted_detached_child" has already made progress and compacted
    L1s, we want to make sure "compaction in the history" does not leave the
    timeline broken.
    """

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # we want to create layers manually so we don't branch on arbitrary
            # Lsn, but we also do not want to compact L0 -> L1.
            "compaction_threshold": "99999",
            "compaction_period": "0s",
            # shouldn't matter, but just in case
            "gc_period": "0s",
        }
    )
    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)
    env.pageserver.allowed_errors.append(
        ".*await_initial_logical_size: can't get semaphore cancel token, skipping"
    )
    client = env.pageserver.http_client()

    def delta_layers(timeline_id: TimelineId):
        # shorthand for more readable formatting
        return client.layer_map_info(env.initial_tenant, timeline_id).delta_layers()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("create table integers (i bigint not null);")
        ep.safe_psql("insert into integers (i) values (42)")
        branch_lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        assert len(delta_layers(env.initial_timeline)) == 2

    more_good_numbers = range(0, 3)

    branches: list[tuple[str, TimelineId]] = [("main", env.initial_timeline)]

    for num in more_good_numbers:
        branch_name = f"br-{len(branches)}"
        branch_timeline_id = env.create_branch(
            branch_name,
            ancestor_branch_name=branches[-1][0],
            ancestor_start_lsn=branch_lsn,
        )
        branches.append((branch_name, branch_timeline_id))

        with env.endpoints.create_start(branches[-1][0], tenant_id=env.initial_tenant) as ep:
            ep.safe_psql(
                f"insert into integers (i) select i from generate_series({num}, {num + 100}) as s(i)"
            )
            branch_lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, branch_timeline_id)
            client.timeline_checkpoint(env.initial_tenant, branch_timeline_id)

        assert len(delta_layers(branch_timeline_id)) == 1

    # now fill in the final, most growing timeline

    branch_name, branch_timeline_id = branches[-1]
    with env.endpoints.create_start(branch_name, tenant_id=env.initial_tenant) as ep:
        ep.safe_psql("insert into integers (i) select i from generate_series(50, 500) s(i)")

        last_suffix = None
        for suffix in range(0, 4):
            ep.safe_psql(f"create table other_table_{suffix} as select * from integers")
            wait_for_last_flush_lsn(env, ep, env.initial_tenant, branch_timeline_id)
            client.timeline_checkpoint(env.initial_tenant, branch_timeline_id)
            last_suffix = suffix

        assert last_suffix is not None

        assert len(delta_layers(branch_timeline_id)) == 5

        env.storage_controller.pageserver_api().update_tenant_config(
            env.initial_tenant, {"compaction_threshold": 5}, None
        )

        client.timeline_compact(env.initial_tenant, branch_timeline_id)

        # one more layer
        ep.safe_psql(f"create table other_table_{last_suffix + 1} as select * from integers")
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, branch_timeline_id)

        # we need to wait here, because the detaches will do implicit tenant restart,
        # and we could get unexpected layer counts
        client.timeline_checkpoint(env.initial_tenant, branch_timeline_id, wait_until_uploaded=True)

    assert len([filter(lambda x: x.l0, delta_layers(branch_timeline_id))]) == 1

    skip_main = branches[1:]

    branch_lsn = client.timeline_detail(env.initial_tenant, branch_timeline_id)["ancestor_lsn"]

    # take the fullbackup before and after inheriting the new L0s
    fullbackup_before = test_output_dir / "fullbackup-before.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, branch_timeline_id, branch_lsn, fullbackup_before
    )

    # force initial logical sizes, so we can evict all layers from all
    # timelines and exercise on-demand download for copy lsn prefix
    client.timeline_detail(
        env.initial_tenant, env.initial_timeline, force_await_initial_logical_size=True
    )
    client.evict_all_layers(env.initial_tenant, env.initial_timeline)

    for _, timeline_id in skip_main:
        reparented = client.detach_ancestor(env.initial_tenant, timeline_id)
        assert reparented == set(), "we have no earlier branches at any level"

    post_detach_l0s = list(filter(lambda x: x.l0, delta_layers(branch_timeline_id)))
    assert len(post_detach_l0s) == 5, "should had inherited 4 L0s, have 5 in total"

    # checkpoint does compaction, which in turn decides to run, because
    # there is now in total threshold number L0s even if they are not
    # adjacent in Lsn space:
    #
    # inherited  flushed during this checkpoint
    #       \\\\ /
    #       1234X5---> lsn
    #           |
    #       l1 layers from "fill in the final, most growing timeline"
    #
    # branch_lsn is between 4 and first X.
    client.timeline_checkpoint(env.initial_tenant, branch_timeline_id)

    post_compact_l0s = list(filter(lambda x: x.l0, delta_layers(branch_timeline_id)))
    assert len(post_compact_l0s) == 1, "only the consecutive inherited L0s should be compacted"

    fullbackup_after = test_output_dir / "fullbackup_after.tar"
    pg_bin.take_fullbackup(
        env.pageserver, env.initial_tenant, branch_timeline_id, branch_lsn, fullbackup_after
    )

    # we don't need to skip any files, because zenith.signal will be identical
    assert_pageserver_backups_equal(fullbackup_before, fullbackup_after, set())


@pytest.mark.parametrize("shards_initial_after", [(1, 1), (2, 2), (1, 4)])
def test_timeline_ancestor_detach_idempotent_success(
    neon_env_builder: NeonEnvBuilder, shards_initial_after: tuple[int, int]
):
    shards_initial = shards_initial_after[0]
    shards_after = shards_initial_after[1]

    neon_env_builder.num_pageservers = shards_after
    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shards_initial if shards_initial > 1 else None,
        initial_tenant_conf={
            # small checkpointing and compaction targets to ensure we generate many upload operations
            "checkpoint_distance": 512 * 1024,
            "compaction_threshold": 1,
            "compaction_target_size": 512 * 1024,
            # disable background compaction and GC. We invoke it manually when we want it to happen.
            "gc_period": "0s",
            "compaction_period": "0s",
        },
    )

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    for ps in pageservers.values():
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    if shards_after > 1:
        # FIXME: should this be in the neon_env_builder.init_start?
        env.storage_controller.reconcile_until_idle()
        client = env.storage_controller.pageserver_api()
    else:
        client = env.pageserver.http_client()

    # Write some data so that we have some layers to copy
    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as endpoint:
        endpoint.safe_psql_many(
            [
                "CREATE TABLE foo(key serial primary key, t text default 'data_content')",
                "INSERT INTO foo SELECT FROM generate_series(1,1024)",
            ]
        )
        last_flush_lsn_upload(env, endpoint, env.initial_tenant, env.initial_timeline)

    first_branch = env.create_branch("first_branch")

    _ = env.create_branch("second_branch", ancestor_branch_name="first_branch")

    # these two will be reparented, and they should be returned in stable order
    # from pageservers OR otherwise there will be an `error!` logging from
    # storage controller
    reparented1 = env.create_branch("first_reparented", ancestor_branch_name="main")
    reparented2 = env.create_branch("second_reparented", ancestor_branch_name="main")

    if shards_after > shards_initial:
        # Do a shard split
        # This is a reproducer for https://github.com/neondatabase/neon/issues/9667
        env.storage_controller.tenant_shard_split(env.initial_tenant, shards_after)
        env.storage_controller.reconcile_until_idle()

    first_reparenting_response = client.detach_ancestor(env.initial_tenant, first_branch)
    assert set(first_reparenting_response) == {reparented1, reparented2}

    # FIXME: this should be done by the http req handler
    for ps in pageservers.values():
        ps.quiesce_tenants()

    for _ in range(5):
        # once completed, we can retry this how many times
        assert (
            client.detach_ancestor(env.initial_tenant, first_branch) == first_reparenting_response
        )

    client.tenant_delete(env.initial_tenant)

    with pytest.raises(PageserverApiException) as e:
        client.detach_ancestor(env.initial_tenant, first_branch)
    assert e.value.status_code == 404


@pytest.mark.parametrize("sharded", [True, False])
def test_timeline_ancestor_detach_errors(neon_env_builder: NeonEnvBuilder, sharded: bool):
    # the test is split from test_timeline_ancestor_detach_idempotent_success as only these error cases should create "request was dropped before completing",
    # given the current first error handling
    shards = 2 if sharded else 1

    neon_env_builder.num_pageservers = shards
    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shards if sharded else None,
        initial_tenant_conf={
            # turn off gc, we want to do manual offloading here.
            "gc_period": "0s",
        },
    )

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    for ps in pageservers.values():
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)
        ps.allowed_errors.extend(
            [
                ".* WARN .* path=/v1/tenant/.*/timeline/.*/detach_ancestor request_id=.*: request was dropped before completing",
                # rare error logging, which is hard to reproduce without instrumenting responding with random sleep
                '.* ERROR .* path=/v1/tenant/.*/timeline/.*/detach_ancestor request_id=.*: Cancelled request finished with an error: Conflict\\("no ancestors"\\)',
            ]
        )

    client = (
        env.pageserver.http_client() if not sharded else env.storage_controller.pageserver_api()
    )

    with pytest.raises(PageserverApiException, match=".* no ancestors") as info:
        client.detach_ancestor(env.initial_tenant, env.initial_timeline)
    assert info.value.status_code == 409

    early_branch = env.create_branch("early_branch")

    first_branch = env.create_branch("first_branch")

    second_branch = env.create_branch("second_branch", ancestor_branch_name="first_branch")

    # funnily enough this does not have a prefix
    with pytest.raises(PageserverApiException, match="too many ancestors") as info:
        client.detach_ancestor(env.initial_tenant, second_branch)
    assert info.value.status_code == 400

    client.timeline_archival_config(
        env.initial_tenant, second_branch, TimelineArchivalState.ARCHIVED
    )

    client.timeline_archival_config(
        env.initial_tenant, early_branch, TimelineArchivalState.ARCHIVED
    )

    with pytest.raises(PageserverApiException, match=f".*archived: {early_branch}") as info:
        client.detach_ancestor(env.initial_tenant, first_branch)
    assert info.value.status_code == 400

    if not sharded:
        client.timeline_offload(env.initial_tenant, early_branch)

    client.timeline_archival_config(
        env.initial_tenant, first_branch, TimelineArchivalState.ARCHIVED
    )

    with pytest.raises(PageserverApiException, match=f".*archived: {first_branch}") as info:
        client.detach_ancestor(env.initial_tenant, first_branch)
    assert info.value.status_code == 400


def test_sharded_timeline_detach_ancestor(neon_env_builder: NeonEnvBuilder):
    """
    Sharded timeline detach ancestor; 4 nodes: 1 stuck, 1 restarted, 2 normal.

    Stuck node gets stuck on a pause failpoint for first storage controller request.
    Restarted node remains stuck until explicit restart from test code.

    We retry the request until storage controller gets 200 OK from all nodes.
    """
    branch_name = "soon_detached"
    shard_count = 4
    neon_env_builder.num_pageservers = shard_count
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)

    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)
    for ps in env.pageservers:
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    # FIXME: should this be in the neon_env_builder.init_start?
    env.storage_controller.reconcile_until_idle()
    # as we will stop a node, make sure there is no clever rebalancing
    env.storage_controller.tenant_policy_update(env.initial_tenant, body={"scheduling": "Stop"})
    env.storage_controller.allowed_errors.append(".*: Scheduling is disabled by policy Stop .*")

    shards = env.storage_controller.locate(env.initial_tenant)

    utilized_pageservers = {x["node_id"] for x in shards}
    assert len(utilized_pageservers) > 1, "all shards got placed on single pageserver?"

    branch_timeline_id = env.create_branch(branch_name)

    with env.endpoints.create_start(branch_name, tenant_id=env.initial_tenant) as ep:
        ep.safe_psql(
            "create table foo as select 1::bigint, i::bigint from generate_series(1, 10000) v(i)"
        )
        lsn = flush_ep_to_pageserver(env, ep, env.initial_tenant, branch_timeline_id)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    for shard_info in shards:
        node_id = int(shard_info["node_id"])
        shard_id = shard_info["shard_id"]
        detail = pageservers[node_id].http_client().timeline_detail(shard_id, branch_timeline_id)

        assert Lsn(detail["last_record_lsn"]) >= lsn
        assert Lsn(detail["initdb_lsn"]) < lsn
        assert TimelineId(detail["ancestor_timeline_id"]) == env.initial_timeline

    # make one of the nodes get stuck, but continue the initial operation
    # make another of the nodes get stuck, then restart

    stuck = pageservers[int(shards[0]["node_id"])]
    log.info(f"stuck pageserver is id={stuck.id}")
    stuck_http = stuck.http_client()
    stuck_http.configure_failpoints(
        ("timeline-detach-ancestor::before_starting_after_locking-pausable", "pause")
    )

    restarted = pageservers[int(shards[1]["node_id"])]
    log.info(f"restarted pageserver is id={restarted.id}")
    # this might be hit; see `restart_restarted`
    restarted.allowed_errors.append(".*: Cancelled request finished with an error: ShuttingDown")
    assert restarted.id != stuck.id
    restarted_http = restarted.http_client()
    restarted_http.configure_failpoints(
        [
            ("timeline-detach-ancestor::before_starting_after_locking-pausable", "pause"),
        ]
    )

    for info in shards:
        pageserver = pageservers[int(info["node_id"])]
        # the first request can cause these, but does not repeatedly
        pageserver.allowed_errors.append(".*: request was dropped before completing")

    # first request again
    env.storage_controller.allowed_errors.append(".*: request was dropped before completing")

    target = env.storage_controller.pageserver_api()

    with pytest.raises(ReadTimeout):
        target.detach_ancestor(env.initial_tenant, branch_timeline_id, timeout=1)

    stuck_http.configure_failpoints(
        ("timeline-detach-ancestor::before_starting_after_locking-pausable", "off")
    )

    barrier = threading.Barrier(2)

    def restart_restarted():
        barrier.wait()
        # graceful shutdown should just work, because simultaneously unpaused
        restarted.stop()
        # this does not happen always, depends how fast we exit after unpausing
        # restarted.assert_log_contains("Cancelled request finished with an error: ShuttingDown")
        restarted.start()

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(restart_restarted)
        barrier.wait()
        # we have 10s, lets use 1/2 of that to help the shutdown start
        time.sleep(5)
        restarted_http.configure_failpoints(
            ("timeline-detach-ancestor::before_starting_after_locking-pausable", "off")
        )
        fut.result()

    # detach ancestor request handling is not sensitive to http cancellation.
    # this means that the "stuck" is on its way to complete the detach, but the restarted is off
    # now it can either be complete on all nodes, or still in progress with
    # one.
    without_retrying = target.without_status_retrying()

    # this retry loop will be long enough that the tenant can always activate
    reparented = None
    for _ in range(10):
        try:
            reparented = without_retrying.detach_ancestor(env.initial_tenant, branch_timeline_id)
        except PageserverApiException as info:
            assert info.status_code == 503
            time.sleep(2)
        else:
            break

    assert reparented == set(), "too many retries (None) or unexpected reparentings"

    for shard_info in shards:
        node_id = int(shard_info["node_id"])
        shard_id = shard_info["shard_id"]

        # TODO: ensure quescing is done on pageserver?
        pageservers[node_id].quiesce_tenants()
        detail = pageservers[node_id].http_client().timeline_detail(shard_id, branch_timeline_id)
        wait_for_last_record_lsn(
            pageservers[node_id].http_client(), shard_id, branch_timeline_id, lsn
        )
        assert detail.get("ancestor_timeline_id") is None

    with env.endpoints.create_start(branch_name, tenant_id=env.initial_tenant) as ep:
        count = int(ep.safe_psql("select count(*) from foo")[0][0])
        assert count == 10000


@pytest.mark.parametrize(
    "mode, sharded",
    [
        ("delete_timeline", False),
        ("delete_timeline", True),
        ("delete_tenant", False),
        # the shared/exclusive lock for tenant is blocking this:
        # timeline detach ancestor takes shared, delete tenant takes exclusive
        # ("delete_tenant", True)
    ],
)
def test_timeline_detach_ancestor_interrupted_by_deletion(
    neon_env_builder: NeonEnvBuilder, mode: str, sharded: bool
):
    """
    Timeline ancestor detach interrupted by deleting either:
    - the detached timeline
    - the whole tenant

    after starting the detach.

    What remains not tested by this:
    - shutdown winning over complete, see test_timeline_is_deleted_before_timeline_detach_ancestor_completes
    """

    shard_count = 2 if sharded else 1

    neon_env_builder.num_pageservers = shard_count

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count if sharded else None,
        initial_tenant_conf={
            "gc_period": "1s",
            "lsn_lease_length": "0s",
        },
    )

    for ps in env.pageservers:
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    detached_timeline = env.create_branch("detached soon", ancestor_branch_name="main")

    pausepoint = "timeline-detach-ancestor::before_starting_after_locking-pausable"

    env.storage_controller.reconcile_until_idle()
    shards = env.storage_controller.locate(env.initial_tenant)

    assert len(set(info["node_id"] for info in shards)) == shard_count

    target = env.storage_controller.pageserver_api() if sharded else env.pageserver.http_client()
    target = target.without_status_retrying()

    victim = pageservers[int(shards[-1]["node_id"])]
    victim_http = victim.http_client()
    victim_http.configure_failpoints((pausepoint, "pause"))

    def detach_ancestor():
        target.detach_ancestor(env.initial_tenant, detached_timeline)

    def at_failpoint() -> LogCursor:
        msg, offset = victim.assert_log_contains(f"at failpoint {pausepoint}")
        log.info(f"found {msg}")
        msg, offset = victim.assert_log_contains(
            ".* gc_loop.*: Skipping GC: .*",
            offset,
        )
        log.info(f"found {msg}")
        return offset

    def start_delete():
        if mode == "delete_timeline":
            target.timeline_delete(env.initial_tenant, detached_timeline)
        elif mode == "delete_tenant":
            target.tenant_delete(env.initial_tenant)
        else:
            raise RuntimeError(f"unimplemented mode {mode}")

    def at_waiting_on_gate_close(start_offset: LogCursor) -> LogCursor:
        _, offset = victim.assert_log_contains(
            "closing is taking longer than expected", offset=start_offset
        )
        return offset

    def is_deleted():
        try:
            if mode == "delete_timeline":
                target.timeline_detail(env.initial_tenant, detached_timeline)
            elif mode == "delete_tenant":
                target.tenant_status(env.initial_tenant)
            else:
                return False
        except PageserverApiException as e:
            assert e.status_code == 404
            return True
        else:
            raise RuntimeError("waiting for 404")

    with ThreadPoolExecutor(max_workers=2) as pool:
        try:
            fut = pool.submit(detach_ancestor)
            offset = wait_until(at_failpoint)

            delete = pool.submit(start_delete)

            offset = wait_until(lambda: at_waiting_on_gate_close(offset))

            victim_http.configure_failpoints((pausepoint, "off"))

            delete.result()

            assert wait_until(is_deleted), f"unimplemented mode {mode}"

            # TODO: match the error
            with pytest.raises(PageserverApiException) as exc:
                fut.result()
            log.info(f"TODO: match this error: {exc.value}")
            assert exc.value.status_code == 503
        finally:
            victim_http.configure_failpoints((pausepoint, "off"))

    if mode != "delete_timeline":
        return

    # make sure the gc is unblocked
    time.sleep(2)
    victim.assert_log_contains(".* gc_loop.*: 1 timelines need GC", offset)

    if not sharded:
        # we have the other node only while sharded
        return

    other = pageservers[int(shards[0]["node_id"])]
    log.info(f"other is {other.id}")
    _, offset = other.assert_log_contains(
        ".*INFO request\\{method=PUT path=/v1/tenant/\\S+/timeline/\\S+/detach_ancestor .*\\}: Request handled, status: 200 OK",
    )
    # this might be a lot earlier than the victims line, but that is okay.
    _, offset = other.assert_log_contains(".* gc_loop.*: 1 timelines need GC", offset)


@pytest.mark.parametrize("mode", ["delete_reparentable_timeline", "create_reparentable_timeline"])
def test_sharded_tad_interleaved_after_partial_success(neon_env_builder: NeonEnvBuilder, mode: str):
    """
    Technically possible storage controller concurrent interleaving timeline
    deletion with timeline detach.

    Deletion is fine, as any sharded pageservers reach the same end state, but
    creating reparentable timeline would create an issue as the two nodes would
    never agree. There is a solution though: the created reparentable timeline
    must be detached.
    """

    shard_count = 2
    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    for ps in env.pageservers:
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    env.storage_controller.reconcile_until_idle()
    shards = env.storage_controller.locate(env.initial_tenant)
    assert len(set(x["node_id"] for x in shards)) == shard_count

    with env.endpoints.create_start("main") as ep:
        ep.safe_psql("create table foo as select i::bigint from generate_series(1, 1000) t(i)")

        # as the interleaved operation, we will delete this timeline, which was reparenting candidate
        first_branch_lsn = wait_for_last_flush_lsn(
            env, ep, env.initial_tenant, env.initial_timeline
        )
        for ps, shard_id in [(pageservers[int(x["node_id"])], x["shard_id"]) for x in shards]:
            ps.http_client().timeline_checkpoint(shard_id, env.initial_timeline)

        ep.safe_psql("create table bar as select i::bigint from generate_series(1, 2000) t(i)")
        detached_branch_lsn = flush_ep_to_pageserver(
            env, ep, env.initial_tenant, env.initial_timeline
        )

    for ps, shard_id in [(pageservers[int(x["node_id"])], x["shard_id"]) for x in shards]:
        ps.http_client().timeline_checkpoint(shard_id, env.initial_timeline)

    def create_reparentable_timeline() -> TimelineId:
        return env.create_branch(
            "first_branch", ancestor_branch_name="main", ancestor_start_lsn=first_branch_lsn
        )

    if mode == "delete_reparentable_timeline":
        first_branch = create_reparentable_timeline()
    else:
        first_branch = None

    detached_branch = env.create_branch(
        "detached_branch", ancestor_branch_name="main", ancestor_start_lsn=detached_branch_lsn
    )

    pausepoint = "timeline-detach-ancestor::before_starting_after_locking-pausable"

    stuck = pageservers[int(shards[0]["node_id"])]
    stuck_http = stuck.http_client().without_status_retrying()
    stuck_http.configure_failpoints((pausepoint, "pause"))

    victim = pageservers[int(shards[-1]["node_id"])]
    victim_http = victim.http_client().without_status_retrying()
    victim_http.configure_failpoints(
        (pausepoint, "pause"),
    )

    # interleaving a create_timeline which could be reparented will produce two
    # permanently different reparentings: one node has reparented, other has
    # not
    #
    # with deletion there is no such problem
    def detach_timeline():
        env.storage_controller.pageserver_api().detach_ancestor(env.initial_tenant, detached_branch)

    def paused_at_failpoint():
        stuck.assert_log_contains(f"at failpoint {pausepoint}")
        victim.assert_log_contains(f"at failpoint {pausepoint}")

    def first_completed():
        detail = stuck_http.timeline_detail(shards[0]["shard_id"], detached_branch)
        log.info(detail)
        assert detail.get("ancestor_lsn") is None

    def first_branch_gone():
        assert first_branch is not None
        try:
            env.storage_controller.pageserver_api().timeline_detail(
                env.initial_tenant, first_branch
            )
        except PageserverApiException as e:
            log.info(f"error {e}")
            assert e.status_code == 404
        else:
            log.info("still ok")
            raise RuntimeError("not done yet")

    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            fut = pool.submit(detach_timeline)
            wait_until(paused_at_failpoint)

            # let stuck complete
            stuck_http.configure_failpoints((pausepoint, "off"))
            wait_until(first_completed)

            if mode == "delete_reparentable_timeline":
                assert first_branch is not None
                env.storage_controller.pageserver_api().timeline_delete(
                    env.initial_tenant, first_branch
                )
                victim_http.configure_failpoints((pausepoint, "off"))
                wait_until(first_branch_gone)
            elif mode == "create_reparentable_timeline":
                first_branch = create_reparentable_timeline()
                victim_http.configure_failpoints((pausepoint, "off"))
            else:
                raise RuntimeError("{mode}")

            # it now passes, and we should get an error messages about mixed reparenting as the stuck still had something to reparent
            mixed_results = "pageservers returned mixed results for ancestor detach; manual intervention is required."
            with pytest.raises(PageserverApiException, match=mixed_results):
                fut.result()

            msg, offset = env.storage_controller.assert_log_contains(
                ".*/timeline/\\S+/detach_ancestor.*: shards returned different results matching=0 .*"
            )
            log.info(f"expected error message: {msg.rstrip()}")
            env.storage_controller.allowed_errors.extend(
                [
                    ".*: shards returned different results matching=0 .*",
                    f".*: InternalServerError\\({mixed_results}",
                ]
            )

            if mode == "create_reparentable_timeline":
                with pytest.raises(PageserverApiException, match=mixed_results):
                    detach_timeline()
            else:
                # it is a bit shame to flag it and then it suceeds, but most
                # likely there would be a retry loop which would take care of
                # this in cplane
                detach_timeline()

            retried = env.storage_controller.log_contains(
                ".*/timeline/\\S+/detach_ancestor.*: shards returned different results matching=0 .*",
                offset,
            )
            if mode == "delete_reparentable_timeline":
                assert (
                    retried is None
                ), "detaching should had converged after both nodes saw the deletion"
            elif mode == "create_reparentable_timeline":
                assert retried is not None, "detaching should not have converged"
                _, offset = retried
        finally:
            stuck_http.configure_failpoints((pausepoint, "off"))
            victim_http.configure_failpoints((pausepoint, "off"))

    if mode == "create_reparentable_timeline":
        assert first_branch is not None
        # now we have mixed ancestry
        assert (
            TimelineId(
                stuck_http.timeline_detail(shards[0]["shard_id"], first_branch)[
                    "ancestor_timeline_id"
                ]
            )
            == env.initial_timeline
        )
        assert (
            TimelineId(
                victim_http.timeline_detail(shards[-1]["shard_id"], first_branch)[
                    "ancestor_timeline_id"
                ]
            )
            == detached_branch
        )

        # make sure we are still able to repair this by detaching the ancestor on the storage controller in case it ever happens
        # if the ancestor would be deleted, we would partially fail, making deletion stuck.
        env.storage_controller.pageserver_api().detach_ancestor(env.initial_tenant, first_branch)

        # and we should now have good results
        not_found = env.storage_controller.log_contains(
            ".*/timeline/\\S+/detach_ancestor.*: shards returned different results matching=0 .*",
            offset,
        )

        assert not_found is None
        assert (
            stuck_http.timeline_detail(shards[0]["shard_id"], first_branch)["ancestor_timeline_id"]
            is None
        )
        assert (
            victim_http.timeline_detail(shards[-1]["shard_id"], first_branch)[
                "ancestor_timeline_id"
            ]
            is None
        )


def test_retryable_500_hit_through_storcon_during_timeline_detach_ancestor(
    neon_env_builder: NeonEnvBuilder,
):
    shard_count = 2
    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    for ps in env.pageservers:
        ps.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    pageservers = dict((int(p.id), p) for p in env.pageservers)

    env.storage_controller.reconcile_until_idle()
    shards = env.storage_controller.locate(env.initial_tenant)
    assert len(set(x["node_id"] for x in shards)) == shard_count

    detached_branch = env.create_branch("detached_branch", ancestor_branch_name="main")

    pausepoint = "timeline-detach-ancestor::before_starting_after_locking-pausable"
    failpoint = "timeline-detach-ancestor::before_starting_after_locking"

    stuck = pageservers[int(shards[0]["node_id"])]
    stuck_http = stuck.http_client().without_status_retrying()
    stuck_http.configure_failpoints(
        (pausepoint, "pause"),
    )

    env.storage_controller.allowed_errors.append(
        f".*Error processing HTTP request: .* failpoint: {failpoint}"
    )
    http = env.storage_controller.pageserver_api()

    victim = pageservers[int(shards[-1]["node_id"])]
    victim.allowed_errors.append(
        f".*Error processing HTTP request: InternalServerError\\(failpoint: {failpoint}"
    )
    victim_http = victim.http_client().without_status_retrying()
    victim_http.configure_failpoints([(pausepoint, "pause"), (failpoint, "return")])

    def detach_timeline():
        http.detach_ancestor(env.initial_tenant, detached_branch)

    def paused_at_failpoint():
        stuck.assert_log_contains(f"at failpoint {pausepoint}")
        victim.assert_log_contains(f"at failpoint {pausepoint}")

    def first_completed():
        detail = stuck_http.timeline_detail(shards[0]["shard_id"], detached_branch)
        log.info(detail)
        assert detail.get("ancestor_lsn") is None

    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            fut = pool.submit(detach_timeline)
            wait_until(paused_at_failpoint)

            # let stuck complete
            stuck_http.configure_failpoints((pausepoint, "off"))
            wait_until(first_completed)

            victim_http.configure_failpoints((pausepoint, "off"))

            with pytest.raises(
                PageserverApiException,
                match=f".*failpoint: {failpoint}",
            ) as exc:
                fut.result()
            assert exc.value.status_code == 500

        finally:
            stuck_http.configure_failpoints((pausepoint, "off"))
            victim_http.configure_failpoints((pausepoint, "off"))

    victim_http.configure_failpoints((failpoint, "off"))
    detach_timeline()


def test_retried_detach_ancestor_after_failed_reparenting(neon_env_builder: NeonEnvBuilder):
    """
    Using a failpoint, force the completion step of timeline ancestor detach to
    fail after reparenting a single timeline.

    Retrying should try reparenting until all reparentings are done, all the
    time blocking gc even across restarts (first round).

    A completion failpoint is used to inhibit completion on second to last
    round.

    On last round, the completion uses a path where no reparentings can happen
    because original ancestor is deleted, and there is a completion to unblock
    gc without restart.
    """

    # to get the remote storage metrics
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "1s",
            "lsn_lease_length": "0s",
        }
    )

    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    env.pageserver.allowed_errors.extend(
        [
            ".* reparenting failed: failpoint: timeline-detach-ancestor::allow_one_reparented",
            ".* Error processing HTTP request: InternalServerError\\(failed to reparent all candidate timelines, please retry",
            ".* Error processing HTTP request: InternalServerError\\(failpoint: timeline-detach-ancestor::complete_before_uploading",
        ]
    )

    http = env.pageserver.http_client()

    def remote_storage_copy_requests():
        return http.get_metric_value(
            "remote_storage_s3_request_seconds_count",
            {"request_type": "copy_object", "result": "ok"},
        )

    def reparenting_progress(timelines: list[TimelineId]) -> tuple[int, set[TimelineId]]:
        reparented = 0
        not_reparented = set()
        for timeline in timelines:
            detail = http.timeline_detail(env.initial_tenant, timeline)
            ancestor = TimelineId(detail["ancestor_timeline_id"])
            if ancestor == detached:
                reparented += 1
            else:
                not_reparented.add(timeline)
        return (reparented, not_reparented)

    # main ------A-----B-----C-----D-----E> lsn
    timelines = []
    with env.endpoints.create_start("main") as ep:
        for counter in range(5):
            ep.safe_psql(
                f"create table foo_{counter} as select i::bigint from generate_series(1, 10000) t(i)"
            )
            branch_lsn = wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
            http.timeline_checkpoint(env.initial_tenant, env.initial_timeline)
            branch = env.create_branch(
                f"branch_{counter}", ancestor_branch_name="main", ancestor_start_lsn=branch_lsn
            )
            timelines.append(branch)

        flush_ep_to_pageserver(env, ep, env.initial_tenant, env.initial_timeline)

    # detach "E" which has most reparentable timelines under it
    detached = timelines.pop()
    assert len(timelines) == 4

    http = http.without_status_retrying()

    http.configure_failpoints(("timeline-detach-ancestor::allow_one_reparented", "return"))

    not_reparented: set[TimelineId] = set()
    # tracked offset in the pageserver log which is at least at the most recent activation
    offset = None

    def try_detach():
        with pytest.raises(
            PageserverApiException,
            match=".*failed to reparent all candidate timelines, please retry",
        ) as exc:
            http.detach_ancestor(env.initial_tenant, detached)
        assert exc.value.status_code == 503

    # first round -- do more checking to make sure the gc gets paused
    try_detach()

    assert (
        http.timeline_detail(env.initial_tenant, detached)["ancestor_timeline_id"] is None
    ), "first round should had detached 'detached'"

    reparented, not_reparented = reparenting_progress(timelines)
    assert reparented == 1

    time.sleep(2)
    _, offset = env.pageserver.assert_log_contains(
        ".*INFO request\\{method=PUT path=/v1/tenant/[0-9a-f]{32}/timeline/[0-9a-f]{32}/detach_ancestor .*\\}: Handling request",
        offset,
    )
    _, offset = env.pageserver.assert_log_contains(".*: attach finished, activating", offset)
    _, offset = env.pageserver.assert_log_contains(
        ".* gc_loop.*: Skipping GC: .*",
        offset,
    )
    metric = remote_storage_copy_requests()
    assert metric != 0
    # make sure the gc blocking is persistent over a restart
    env.pageserver.restart()
    env.pageserver.quiesce_tenants()
    time.sleep(2)
    _, offset = env.pageserver.assert_log_contains(".*: attach finished, activating", offset)
    assert env.pageserver.log_contains(".* gc_loop.*: [0-9] timelines need GC", offset) is None
    _, offset = env.pageserver.assert_log_contains(
        ".* gc_loop.*: Skipping GC: .*",
        offset,
    )
    # restore failpoint for the next reparented
    http.configure_failpoints(("timeline-detach-ancestor::allow_one_reparented", "return"))

    reparented_before = reparented

    # do two more rounds
    for _ in range(2):
        try_detach()

        assert (
            http.timeline_detail(env.initial_tenant, detached)["ancestor_timeline_id"] is None
        ), "first round should had detached 'detached'"

        reparented, not_reparented = reparenting_progress(timelines)
        assert reparented == reparented_before + 1
        reparented_before = reparented

        _, offset = env.pageserver.assert_log_contains(".*: attach finished, activating", offset)
        metric = remote_storage_copy_requests()
        assert metric == 0, "copies happen in the first round"

    assert offset is not None
    assert len(not_reparented) == 1

    http.configure_failpoints(("timeline-detach-ancestor::complete_before_uploading", "return"))

    # almost final round, the failpoint is hit no longer as there is only one reparented and one always gets to succeed.
    # the tenant is restarted once more, but we fail during completing.
    with pytest.raises(
        PageserverApiException, match=".* timeline-detach-ancestor::complete_before_uploading"
    ) as exc:
        http.detach_ancestor(env.initial_tenant, detached)
    assert exc.value.status_code == 500
    _, offset = env.pageserver.assert_log_contains(".*: attach finished, activating", offset)

    # delete the previous ancestor to take a different path to completion. all
    # other tests take the "detach? reparent complete", but this only hits
    # "complete".
    http.timeline_delete(env.initial_tenant, env.initial_timeline)
    wait_timeline_detail_404(http, env.initial_tenant, env.initial_timeline)

    http.configure_failpoints(("timeline-detach-ancestor::complete_before_uploading", "off"))

    reparented_resp = http.detach_ancestor(env.initial_tenant, detached)
    assert reparented_resp == set(timelines)
    # no need to quiesce_tenants anymore, because completion does that

    reparented, not_reparented = reparenting_progress(timelines)
    assert reparented == len(timelines)

    time.sleep(2)
    assert (
        env.pageserver.log_contains(".*: attach finished, activating", offset) is None
    ), "there should be no restart with the final detach_ancestor as it only completed"

    # gc is unblocked
    env.pageserver.assert_log_contains(".* gc_loop.*: 5 timelines need GC", offset)

    metric = remote_storage_copy_requests()
    assert metric == 0


def test_timeline_is_deleted_before_timeline_detach_ancestor_completes(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Make sure that a timeline deleted after restart will unpause gc blocking.
    """
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "1s",
            "lsn_lease_length": "0s",
        }
    )

    env.pageserver.allowed_errors.extend(SHUTDOWN_ALLOWED_ERRORS)

    http = env.pageserver.http_client()

    detached = env.create_branch("detached")

    failpoint = "timeline-detach-ancestor::after_activating_before_finding-pausable"

    http.configure_failpoints((failpoint, "pause"))

    def detach_and_get_stuck():
        return http.detach_ancestor(env.initial_tenant, detached)

    def request_processing_noted_in_log():
        _, offset = env.pageserver.assert_log_contains(
            ".*INFO request\\{method=PUT path=/v1/tenant/[0-9a-f]{32}/timeline/[0-9a-f]{32}/detach_ancestor .*\\}: Handling request",
        )
        return offset

    def delete_detached():
        return http.timeline_delete(env.initial_tenant, detached)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            detach = pool.submit(detach_and_get_stuck)

            offset = wait_until(request_processing_noted_in_log)

            # make this named fn tor more clear failure test output logging
            def pausepoint_hit_with_gc_paused() -> LogCursor:
                env.pageserver.assert_log_contains(f"at failpoint {failpoint}")
                _, at = env.pageserver.assert_log_contains(
                    ".* gc_loop.*: Skipping GC: .*",
                    offset,
                )
                return at

            offset = wait_until(pausepoint_hit_with_gc_paused)

            delete_detached()

            wait_timeline_detail_404(http, env.initial_tenant, detached)

            http.configure_failpoints((failpoint, "off"))

            with pytest.raises(
                PageserverApiException, match="NotFound: Timeline .* was not found"
            ) as exc:
                detach.result()
            assert exc.value.status_code == 404
    finally:
        http.configure_failpoints((failpoint, "off"))

    # make sure gc has been unblocked
    time.sleep(2)

    env.pageserver.assert_log_contains(".* gc_loop.*: 1 timelines need GC", offset)


@skip_in_debug_build("only run with release build")
def test_pageserver_compaction_detach_ancestor_smoke(neon_env_builder: NeonEnvBuilder):
    SMOKE_CONF = {
        # Run both gc and gc-compaction.
        "gc_period": "5s",
        "compaction_period": "5s",
        # No PiTR interval and small GC horizon
        "pitr_interval": "0s",
        "gc_horizon": f"{1024 ** 2}",
        "lsn_lease_length": "0s",
        # Small checkpoint distance to create many layers
        "checkpoint_distance": 1024**2,
        # Compact small layers
        "compaction_target_size": 1024**2,
        "image_creation_threshold": 2,
    }

    env = neon_env_builder.init_start(initial_tenant_conf=SMOKE_CONF)

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    row_count = 10000
    churn_rounds = 50

    ps_http = env.pageserver.http_client()

    workload_parent = Workload(env, tenant_id, timeline_id)
    workload_parent.init(env.pageserver.id)
    log.info("Writing initial data ...")
    workload_parent.write_rows(row_count, env.pageserver.id)
    branch_id = env.create_branch("child")
    workload_child = Workload(env, tenant_id, branch_id, branch_name="child")
    workload_child.init(env.pageserver.id, allow_recreate=True)
    log.info("Writing initial data on child...")
    workload_child.write_rows(row_count, env.pageserver.id)

    for i in range(1, churn_rounds + 1):
        if i % 10 == 0:
            log.info(f"Running churn round {i}/{churn_rounds} ...")

        workload_parent.churn_rows(row_count, env.pageserver.id)
        workload_child.churn_rows(row_count, env.pageserver.id)

    ps_http.detach_ancestor(tenant_id, branch_id)

    log.info("Validating at workload end ...")
    workload_parent.validate(env.pageserver.id)
    workload_child.validate(env.pageserver.id)


# TODO:
# - branch near existing L1 boundary, image layers?
# - investigate: why are layers started at uneven lsn? not just after branching, but in general.
#
# TEST: 1. tad which partially succeeds, one returns 500
#       2. create branch below timeline? ~or delete reparented timeline~ (done)
#       3. on retry all should report the same reparented timelines
