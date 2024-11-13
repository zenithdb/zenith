from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import asyncpg
import pytest
import toml
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import getLogger
from fixtures.neon_fixtures import Endpoint, NeonEnv, NeonEnvBuilder, Safekeeper
from fixtures.remote_storage import RemoteStorageKind
from fixtures.utils import skip_in_debug_build

if TYPE_CHECKING:
    from typing import Optional


log = getLogger("root.safekeeper_async")


class BankClient:
    def __init__(self, conn: asyncpg.Connection, n_accounts, init_amount):
        self.conn: asyncpg.Connection = conn
        self.n_accounts = n_accounts
        self.init_amount = init_amount

    async def initdb(self):
        await self.conn.execute("DROP TABLE IF EXISTS bank_accs")
        await self.conn.execute("CREATE TABLE bank_accs(uid int primary key, amount int)")
        await self.conn.execute(
            """
            INSERT INTO bank_accs
            SELECT *, $1 FROM generate_series(0, $2)
        """,
            self.init_amount,
            self.n_accounts - 1,
        )
        await self.conn.execute("DROP TABLE IF EXISTS bank_log")
        await self.conn.execute("CREATE TABLE bank_log(from_uid int, to_uid int, amount int)")

    async def check_invariant(self):
        row = await self.conn.fetchrow("SELECT sum(amount) AS sum FROM bank_accs")
        assert row["sum"] == self.n_accounts * self.init_amount


async def bank_transfer(conn: asyncpg.Connection, from_uid, to_uid, amount):
    # avoid deadlocks by sorting uids
    if from_uid > to_uid:
        from_uid, to_uid, amount = to_uid, from_uid, -amount

    async with conn.transaction():
        await conn.execute(
            "UPDATE bank_accs SET amount = amount + ($1) WHERE uid = $2",
            amount,
            to_uid,
        )
        await conn.execute(
            "UPDATE bank_accs SET amount = amount - ($1) WHERE uid = $2",
            amount,
            from_uid,
        )
        await conn.execute(
            "INSERT INTO bank_log VALUES ($1, $2, $3)",
            from_uid,
            to_uid,
            amount,
        )


class WorkerStats:
    def __init__(self, n_workers):
        self.counters = [0] * n_workers
        self.running = True

    def reset(self):
        self.counters = [0] * len(self.counters)

    def inc_progress(self, worker_id):
        self.counters[worker_id] += 1

    def check_progress(self):
        log.debug(f"Workers progress: {self.counters}")

        # every worker should finish at least one tx
        assert all(cnt > 0 for cnt in self.counters)

        progress = sum(self.counters)
        log.info(f"All workers made {progress} transactions")


async def run_random_worker(
    stats: WorkerStats, endpoint: Endpoint, worker_id, n_accounts, max_transfer
):
    pg_conn = await endpoint.connect_async()
    log.debug(f"Started worker {worker_id}")

    while stats.running:
        from_uid = random.randint(0, n_accounts - 1)
        to_uid = (from_uid + random.randint(1, n_accounts - 1)) % n_accounts
        amount = random.randint(1, max_transfer)

        await bank_transfer(pg_conn, from_uid, to_uid, amount)
        stats.inc_progress(worker_id)

        log.debug(f"Executed transfer({amount}) {from_uid} => {to_uid}")

    log.debug(f"Finished worker {worker_id}")

    await pg_conn.close()


async def wait_for_lsn(
    safekeeper: Safekeeper,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    wait_lsn: Lsn,
    polling_interval=1,
    timeout=60,
):
    """
    Poll flush_lsn from safekeeper until it's greater or equal than
    provided wait_lsn. To do that, timeline_status is fetched from
    safekeeper every polling_interval seconds.
    """

    started_at = time.time()
    client = safekeeper.http_client()

    flush_lsn = client.timeline_status(tenant_id, timeline_id).flush_lsn
    log.info(
        f"Safekeeper at port {safekeeper.port.pg} has flush_lsn {flush_lsn}, waiting for lsn {wait_lsn}"
    )

    while wait_lsn > flush_lsn:
        elapsed = time.time() - started_at
        if elapsed > timeout:
            raise RuntimeError(
                f"timed out waiting for safekeeper at port {safekeeper.port.pg} to reach {wait_lsn}, current lsn is {flush_lsn}"
            )

        await asyncio.sleep(polling_interval)
        flush_lsn = client.timeline_status(tenant_id, timeline_id).flush_lsn
        log.debug(f"safekeeper port={safekeeper.port.pg} flush_lsn={flush_lsn} wait_lsn={wait_lsn}")


# This test will run several iterations and check progress in each of them.
# On each iteration 1 acceptor is stopped, and 2 others should allow
# background workers execute transactions. In the end, state should remain
# consistent.
async def run_restarts_under_load(
    env: NeonEnv,
    endpoint: Endpoint,
    acceptors: list[Safekeeper],
    n_workers=10,
    n_accounts=100,
    init_amount=100000,
    max_transfer=100,
    period_time=4,
    iterations=10,
):
    # Set timeout for this test at 5 minutes. It should be enough for test to complete,
    # taking into account that this timeout is checked only at the beginning of every iteration.
    test_timeout_at = time.monotonic() + 5 * 60

    pg_conn = await endpoint.connect_async()
    tenant_id = TenantId(await pg_conn.fetchval("show neon.tenant_id"))
    timeline_id = TimelineId(await pg_conn.fetchval("show neon.timeline_id"))

    bank = BankClient(pg_conn, n_accounts=n_accounts, init_amount=init_amount)
    # create tables and initial balances
    await bank.initdb()

    stats = WorkerStats(n_workers)
    workers = []
    for worker_id in range(n_workers):
        worker = run_random_worker(stats, endpoint, worker_id, bank.n_accounts, max_transfer)
        workers.append(asyncio.create_task(worker))

    for it in range(iterations):
        assert time.monotonic() < test_timeout_at, "test timed out"

        victim_idx = it % len(acceptors)
        victim = acceptors[victim_idx]
        victim.stop()

        flush_lsn = Lsn(await pg_conn.fetchval("SELECT pg_current_wal_flush_lsn()"))
        log.info(f"Postgres flush_lsn {flush_lsn}")

        pageserver_lsn = Lsn(
            env.pageserver.http_client().timeline_detail(tenant_id, timeline_id)["last_record_lsn"]
        )
        sk_ps_lag = flush_lsn - pageserver_lsn
        log.info(f"Pageserver last_record_lsn={pageserver_lsn} lag={sk_ps_lag / 1024}kb")

        # Wait until alive safekeepers catch up with postgres
        for idx, safekeeper in enumerate(acceptors):
            if idx != victim_idx:
                await wait_for_lsn(safekeeper, tenant_id, timeline_id, flush_lsn)

        stats.reset()
        await asyncio.sleep(period_time)
        # assert that at least one transaction has completed in every worker
        stats.check_progress()

        # testing #6530
        victim.start(extra_opts=["--partial-backup-timeout=2s"])

    log.info("Iterations are finished, exiting coroutines...")
    stats.running = False
    # await all workers
    await asyncio.gather(*workers)
    # assert balances sum hasn't changed
    await bank.check_invariant()
    await pg_conn.close()


# Restart acceptors one by one, while executing and validating bank transactions
def test_restarts_under_load(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_safekeeper_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    env.create_branch("test_safekeepers_restarts_under_load")
    # Enable backpressure with 1MB maximal lag, because we don't want to block on `wait_for_lsn()` for too long
    endpoint = env.endpoints.create_start(
        "test_safekeepers_restarts_under_load", config_lines=["max_replication_write_lag=1MB"]
    )

    asyncio.run(run_restarts_under_load(env, endpoint, env.safekeepers))


# Restart acceptors one by one and test that everything is working as expected
# when checkpoins are triggered frequently by max_wal_size=32MB. Because we have
# wal_keep_size=0, there will be aggressive WAL segments recycling.
def test_restarts_frequent_checkpoints(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.create_branch("test_restarts_frequent_checkpoints")
    # Enable backpressure with 1MB maximal lag, because we don't want to block on `wait_for_lsn()` for too long
    endpoint = env.endpoints.create_start(
        "test_restarts_frequent_checkpoints",
        config_lines=[
            "max_replication_write_lag=1MB",
            "min_wal_size=32MB",
            "max_wal_size=32MB",
            "log_checkpoints=on",
        ],
    )

    # we try to simulate large (flush_lsn - truncate_lsn) lag, to test that WAL segments
    # are not removed before broadcasted to all safekeepers, with the help of replication slot
    asyncio.run(
        run_restarts_under_load(env, endpoint, env.safekeepers, period_time=15, iterations=4)
    )


def endpoint_create_start(
    env: NeonEnv, branch: str, pgdir_name: Optional[str], allow_multiple: bool = False
):
    endpoint = Endpoint(
        env,
        tenant_id=env.initial_tenant,
        pg_port=env.port_distributor.get_port(),
        http_port=env.port_distributor.get_port(),
        # In these tests compute has high probability of terminating on its own
        # before our stop() due to lost consensus leadership.
        check_stop_result=False,
    )

    # embed current time in endpoint ID
    endpoint_id = pgdir_name or f"ep-{time.time()}"
    return endpoint.create_start(
        branch_name=branch,
        endpoint_id=endpoint_id,
        config_lines=["log_statement=all"],
        allow_multiple=allow_multiple,
    )


async def exec_compute_query(
    env: NeonEnv,
    branch: str,
    query: str,
    pgdir_name: Optional[str] = None,
    allow_multiple: bool = False,
):
    with endpoint_create_start(
        env, branch=branch, pgdir_name=pgdir_name, allow_multiple=allow_multiple
    ) as endpoint:
        before_conn = time.time()
        conn = await endpoint.connect_async()
        res = await conn.fetch(query)
        await conn.close()
        after_conn = time.time()
        log.info(f"{query} took {after_conn - before_conn}s")
        return res


async def run_compute_restarts(
    env: NeonEnv, queries=16, batch_insert=10000, branch="test_compute_restarts"
):
    cnt = 0
    sum = 0

    await exec_compute_query(env, branch, "CREATE TABLE t (i int)")

    for i in range(queries):
        if i % 4 == 0:
            await exec_compute_query(
                env, branch, f"INSERT INTO t SELECT 1 FROM generate_series(1, {batch_insert})"
            )
            sum += batch_insert
            cnt += batch_insert
        elif (i % 4 == 1) or (i % 4 == 3):
            # Note that select causes lots of FPI's and increases probability of safekeepers
            # standing at different LSNs after compute termination.
            actual_sum = (await exec_compute_query(env, branch, "SELECT SUM(i) FROM t"))[0][0]
            assert actual_sum == sum, f"Expected sum={sum}, actual={actual_sum}"
        elif i % 4 == 2:
            await exec_compute_query(env, branch, "UPDATE t SET i = i + 1")
            sum += cnt


# Add a test which creates compute for every query, and then destroys it right after.
def test_compute_restarts(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.create_branch("test_compute_restarts")
    asyncio.run(run_compute_restarts(env))


class BackgroundCompute:
    MAX_QUERY_GAP_SECONDS = 2

    def __init__(self, index: int, env: NeonEnv, branch: str):
        self.index = index
        self.env = env
        self.branch = branch
        self.running = False
        self.stopped = False
        self.total_tries = 0
        self.successful_queries: list[int] = []

    async def run(self):
        if self.running:
            raise Exception("BackgroundCompute is already running")

        self.running = True
        i = 0
        while not self.stopped:
            try:
                verify_key = (self.index << 16) + i
                i += 1
                self.total_tries += 1
                res = await exec_compute_query(
                    self.env,
                    self.branch,
                    f"INSERT INTO query_log(index, verify_key) VALUES ({self.index}, {verify_key}) RETURNING verify_key",
                    pgdir_name=f"bgcompute{self.index}_key{verify_key}",
                    allow_multiple=True,
                )
                log.info(f"result: {res}")
                if len(res) != 1:
                    raise Exception("No result returned")
                if res[0][0] != verify_key:
                    raise Exception("Wrong result returned")
                self.successful_queries.append(verify_key)
            except Exception as e:
                log.info(f"BackgroundCompute {self.index} query failed: {e}")

            # With less sleep, there is a very big chance of not committing
            # anything or only 1 xact during test run.
            await asyncio.sleep(random.uniform(0, self.MAX_QUERY_GAP_SECONDS))
        self.running = False


async def run_concurrent_computes(
    env: NeonEnv, num_computes=10, run_seconds=20, branch="test_concurrent_computes"
):
    await exec_compute_query(
        env, branch, "CREATE TABLE query_log (t timestamp default now(), index int, verify_key int)"
    )

    computes = [BackgroundCompute(i, env, branch) for i in range(num_computes)]
    background_tasks = [asyncio.create_task(compute.run()) for compute in computes]

    await asyncio.sleep(run_seconds)
    log.info("stopping all tasks but one")
    for compute in computes[1:]:
        compute.stopped = True
    await asyncio.gather(*background_tasks[1:])
    log.info("stopped all tasks but one")

    # work for some time with only one compute -- it should be able to make some xacts
    TIMEOUT_SECONDS = computes[0].MAX_QUERY_GAP_SECONDS + 3
    initial_queries_by_0 = len(computes[0].successful_queries)
    log.info(
        f"Waiting for another query by computes[0], "
        f"it already had {initial_queries_by_0}, timeout is {TIMEOUT_SECONDS}s"
    )
    for _ in range(10 * TIMEOUT_SECONDS):
        current_queries_by_0 = len(computes[0].successful_queries) - initial_queries_by_0
        if current_queries_by_0 >= 1:
            log.info(
                f"Found {current_queries_by_0} successful queries "
                f"by computes[0], completing the test"
            )
            break
        await asyncio.sleep(0.1)
    else:
        raise AssertionError("Timed out while waiting for another query by computes[0]")
    computes[0].stopped = True

    await asyncio.gather(background_tasks[0])

    result = await exec_compute_query(env, branch, "SELECT * FROM query_log")
    # we should have inserted something while single compute was running
    log.info(
        f"Executed {len(result)} queries, {current_queries_by_0} of them "
        f"by computes[0] after we started stopping the others"
    )
    for row in result:
        log.info(f"{row[0]} {row[1]} {row[2]}")

    # ensure everything reported as committed wasn't lost
    for compute in computes:
        for verify_key in compute.successful_queries:
            assert verify_key in [row[2] for row in result]


# Run multiple computes concurrently, creating-destroying them after single
# query. Ensure we don't lose any xacts reported as committed and be able to
# progress once only one compute remains.
def test_concurrent_computes(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.create_branch("test_concurrent_computes")
    asyncio.run(run_concurrent_computes(env))


# Stop safekeeper and check that query cannot be executed while safekeeper is down.
# Query will insert a single row into a table.
async def check_unavailability(
    sk: Safekeeper, conn: asyncpg.Connection, key: int, start_delay_sec: int = 2
):
    # shutdown one of two acceptors, that is, majority
    sk.stop()

    bg_query = asyncio.create_task(conn.execute(f"INSERT INTO t values ({key}, 'payload')"))

    await asyncio.sleep(start_delay_sec)
    # ensure that the query has not been executed yet
    assert not bg_query.done()

    # start safekeeper and await the query
    sk.start()
    await bg_query
    assert bg_query.done()


async def run_unavailability(env: NeonEnv, endpoint: Endpoint):
    conn = await endpoint.connect_async()

    # check basic work with table
    await conn.execute("CREATE TABLE t(key int primary key, value text)")
    await conn.execute("INSERT INTO t values (1, 'payload')")

    # stop safekeeper and check that query cannot be executed while safekeeper is down
    await check_unavailability(env.safekeepers[0], conn, 2)

    # for the world's balance, do the same with second safekeeper
    await check_unavailability(env.safekeepers[1], conn, 3)

    # check that we can execute queries after restart
    await conn.execute("INSERT INTO t values (4, 'payload')")

    result_sum = await conn.fetchval("SELECT sum(key) FROM t")
    assert result_sum == 10


# When majority of acceptors is offline, commits are expected to be frozen
def test_unavailability(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 2
    env = neon_env_builder.init_start()

    env.create_branch("test_safekeepers_unavailability")
    endpoint = env.endpoints.create_start("test_safekeepers_unavailability")

    asyncio.run(run_unavailability(env, endpoint))


async def run_recovery_uncommitted(env: NeonEnv):
    (sk1, sk2, _) = env.safekeepers

    env.create_branch("test_recovery_uncommitted")
    ep = env.endpoints.create_start("test_recovery_uncommitted")
    ep.safe_psql("create table t(key int, value text)")
    ep.safe_psql("insert into t select generate_series(1, 100), 'payload'")

    # insert with only one safekeeper up to create tail of flushed but not committed WAL
    sk1.stop()
    sk2.stop()
    conn = await ep.connect_async()
    # query should hang, so execute in separate task
    bg_query = asyncio.create_task(
        conn.execute("insert into t select generate_series(1, 2000), 'payload'")
    )
    sleep_sec = 2
    await asyncio.sleep(sleep_sec)
    # it must still be not finished
    assert not bg_query.done()
    # note: destoy will kill compute_ctl, preventing it waiting for hanging sync-safekeepers.
    ep.stop_and_destroy()

    # Start one of sks to make quorum online plus compute and ensure they can
    # sync.
    sk2.start()
    ep = env.endpoints.create_start(
        "test_recovery_uncommitted",
    )
    ep.safe_psql("insert into t select generate_series(1, 2000), 'payload'")


# Test pulling uncommitted WAL (up to flush_lsn) during recovery.
def test_recovery_uncommitted(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    asyncio.run(run_recovery_uncommitted(env))


async def run_wal_truncation(env: NeonEnv):
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    (sk1, sk2, sk3) = env.safekeepers

    ep = env.endpoints.create_start("main")
    ep.safe_psql("create table t (key int, value text)")
    ep.safe_psql("insert into t select generate_series(1, 100), 'payload'")

    # insert with only one sk3 up to create tail of flushed but not committed WAL on it
    sk1.stop()
    sk2.stop()
    conn = await ep.connect_async()
    # query should hang, so execute in separate task
    bg_query = asyncio.create_task(
        conn.execute("insert into t select generate_series(1, 180000), 'Papaya'")
    )
    sleep_sec = 2
    await asyncio.sleep(sleep_sec)
    # it must still be not finished
    assert not bg_query.done()
    # note: destoy will kill compute_ctl, preventing it waiting for hanging sync-safekeepers.
    ep.stop_and_destroy()

    # stop sk3 as well
    sk3.stop()

    # now start sk1 and sk2 and make them commit something
    sk1.start()
    sk2.start()
    ep = env.endpoints.create_start(
        "main",
    )
    ep.safe_psql("insert into t select generate_series(1, 200), 'payload'")

    # start sk3 and wait for it to catch up
    sk3.start()
    flush_lsn = Lsn(ep.safe_psql_scalar("SELECT pg_current_wal_flush_lsn()"))
    await wait_for_lsn(sk3, tenant_id, timeline_id, flush_lsn)

    timeline_start_lsn = sk1.get_timeline_start_lsn(tenant_id, timeline_id)
    digests = [
        sk.http_client().timeline_digest(tenant_id, timeline_id, timeline_start_lsn, flush_lsn)
        for sk in [sk1, sk2]
    ]
    assert digests[0] == digests[1], f"digest on sk1 is {digests[0]} but on sk3 is {digests[1]}"


# Simple deterministic test creating tail of WAL on safekeeper which is
# truncated when majority without this sk elects walproposer starting earlier.
def test_wal_truncation(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    asyncio.run(run_wal_truncation(env))


async def run_segment_init_failure(env: NeonEnv):
    env.create_branch("test_segment_init_failure")
    ep = env.endpoints.create_start("test_segment_init_failure")
    ep.safe_psql("create table t(key int, value text)")
    ep.safe_psql("insert into t select generate_series(1, 100), 'payload'")

    sk = env.safekeepers[0]
    sk_http = sk.http_client()
    sk_http.configure_failpoints([("sk-zero-segment", "return")])
    conn = await ep.connect_async()
    ep.safe_psql("select pg_switch_wal()")  # jump to the segment boundary
    # next insertion should hang until failpoint is disabled.
    bg_query = asyncio.create_task(
        conn.execute("insert into t select generate_series(1,1), 'payload'")
    )
    sleep_sec = 2
    await asyncio.sleep(sleep_sec)
    # it must still be not finished
    assert not bg_query.done()
    # Also restart ep at segment boundary to make test more interesting. Do it in immediate mode;
    # fast will hang because it will try to gracefully finish sending WAL.
    ep.stop(mode="immediate")
    # Without segment rename during init (#6402) previous statement created
    # partially initialized 16MB segment, so sk restart also triggers #6401.
    sk.stop().start()
    ep = env.endpoints.create_start("test_segment_init_failure")
    ep.safe_psql("insert into t select generate_series(1,1), 'payload'")  # should be ok now


# Test (injected) failure during WAL segment init.
# https://github.com/neondatabase/neon/issues/6401
# https://github.com/neondatabase/neon/issues/6402
def test_segment_init_failure(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    asyncio.run(run_segment_init_failure(env))


@dataclass
class RaceConditionTest:
    iteration: int
    is_stopped: bool


# shut down random subset of safekeeper, sleep, wake them up, rinse, repeat
async def xmas_garland(safekeepers: list[Safekeeper], data: RaceConditionTest):
    while not data.is_stopped:
        data.iteration += 1
        victims = []
        for sk in safekeepers:
            if random.random() >= 0.5:
                victims.append(sk)
        log.info(
            f"Iteration {data.iteration}: stopping {list(map(lambda sk: sk.id, victims))} safekeepers"
        )
        for v in victims:
            v.stop()
        await asyncio.sleep(1)
        for v in victims:
            v.start()
        log.info(f"Iteration {data.iteration} finished")
        await asyncio.sleep(1)


async def run_race_conditions(env: NeonEnv, endpoint: Endpoint):
    conn = await endpoint.connect_async()
    await conn.execute("CREATE TABLE t(key int primary key, value text)")

    data = RaceConditionTest(0, False)
    bg_xmas = asyncio.create_task(xmas_garland(env.safekeepers, data))

    n_iterations = 5
    expected_sum = 0
    i = 1

    while data.iteration <= n_iterations:
        await asyncio.sleep(0.005)
        await conn.execute(f"INSERT INTO t values ({i}, 'payload')")
        expected_sum += i
        i += 1

    log.info(f"Executed {i-1} queries")

    res = await conn.fetchval("SELECT sum(key) FROM t")
    assert res == expected_sum

    data.is_stopped = True
    await bg_xmas


# do inserts while concurrently getting up/down subsets of acceptors
def test_race_conditions(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.create_branch("test_safekeepers_race_conditions")
    endpoint = env.endpoints.create_start("test_safekeepers_race_conditions")

    asyncio.run(run_race_conditions(env, endpoint))


# Check that pageserver can select safekeeper with largest commit_lsn
# and switch if LSN is not updated for some time (NoWalTimeout).
async def run_wal_lagging(env: NeonEnv, endpoint: Endpoint, test_output_dir: Path):
    def adjust_safekeepers(env: NeonEnv, active_sk: list[bool]):
        # Change the pg ports of the inactive safekeepers in the config file to be
        # invalid, to make them unavailable to the endpoint.  We use
        # ports 10, 11 and 12 to simulate unavailable safekeepers.
        config = toml.load(test_output_dir / "repo" / "config")
        for i, (_sk, active) in enumerate(zip(env.safekeepers, active_sk)):
            if active:
                config["safekeepers"][i]["pg_port"] = env.safekeepers[i].port.pg
            else:
                config["safekeepers"][i]["pg_port"] = 10 + i

        with open(test_output_dir / "repo" / "config", "w") as f:
            toml.dump(config, f)

    conn = await endpoint.connect_async()
    await conn.execute("CREATE TABLE t(key int primary key, value text)")
    await conn.close()
    endpoint.stop()

    n_iterations = 20
    n_txes = 10000
    expected_sum = 0
    i = 1
    quorum = len(env.safekeepers) // 2 + 1

    for it in range(n_iterations):
        active_sk = list(map(lambda _: random.random() >= 0.5, env.safekeepers))
        active_count = sum(active_sk)

        if active_count < quorum:
            it -= 1
            continue

        adjust_safekeepers(env, active_sk)
        log.info(f"Iteration {it}: {active_sk}")

        endpoint.start()
        conn = await endpoint.connect_async()

        for _ in range(n_txes):
            await conn.execute(f"INSERT INTO t values ({i}, 'payload')")
            expected_sum += i
            i += 1

        await conn.close()
        endpoint.stop()

    adjust_safekeepers(env, [True] * len(env.safekeepers))
    endpoint.start()
    conn = await endpoint.connect_async()

    log.info(f"Executed {i-1} queries")

    res = await conn.fetchval("SELECT sum(key) FROM t")
    assert res == expected_sum


# Do inserts while restarting postgres and messing with safekeeper addresses.
# The test takes more than default 5 minutes on Postgres 16,
# see https://github.com/neondatabase/neon/issues/5305
@pytest.mark.timeout(600)
@skip_in_debug_build("times out in debug builds")
def test_wal_lagging(neon_env_builder: NeonEnvBuilder, test_output_dir: Path, build_type: str):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.create_branch("test_wal_lagging")
    endpoint = env.endpoints.create_start("test_wal_lagging")

    asyncio.run(run_wal_lagging(env, endpoint, test_output_dir))
