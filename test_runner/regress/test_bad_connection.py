from __future__ import annotations

import asyncio
import random
import time
from asyncio import TaskGroup

import psycopg2.errors
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin
from fixtures.pageserver_mitm import BreakConnectionException, PageserverProxy
from fixtures.port_distributor import PortDistributor


@pytest.mark.timeout(600)
def test_compute_pageserver_connection_stress(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.append(".*simulated connection error.*")  # this is never hit

    # the real reason (Simulated Connection Error) is on the next line, and we cannot filter this out.
    env.pageserver.allowed_errors.append(
        ".*ERROR error in page_service connection task: Postgres query error"
    )

    # Enable failpoint before starting everything else up so that we exercise the retry
    # on fetching basebackup
    pageserver_http = env.pageserver.http_client()
    pageserver_http.configure_failpoints(("simulated-bad-compute-connection", "50%return(15)"))

    env.create_branch("test_compute_pageserver_connection_stress")
    endpoint = env.endpoints.create_start("test_compute_pageserver_connection_stress")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    def execute_retry_on_timeout(query):
        while True:
            try:
                cur.execute(query)
                return
            except psycopg2.errors.QueryCanceled:
                log.info(f"Query '{query}' timed out - retrying")

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    execute_retry_on_timeout("CREATE TABLE foo (t text)")
    execute_retry_on_timeout(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
        """
    )

    # Verify that the table is larger than shared_buffers
    execute_retry_on_timeout(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
        """
    )
    row = cur.fetchone()
    assert row is not None
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    execute_retry_on_timeout("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)

    end_time = time.time() + 30
    times_executed = 0
    while time.time() < end_time:
        if random.random() < 0.5:
            execute_retry_on_timeout("INSERT INTO foo VALUES ('stas'), ('heikki')")
        else:
            execute_retry_on_timeout("SELECT t FROM foo ORDER BY RANDOM() LIMIT 10")
            cur.fetchall()
        times_executed += 1
    log.info(f"Workload executed {times_executed} times")

    # do a graceful shutdown which would had caught the allowed_errors before
    # https://github.com/neondatabase/neon/pull/8632
    env.pageserver.stop()


#
# Observations:
#
# 1. If the backend is waiting for response to GetPage request, and the client disconnects,
#    the backend will not immediately abort the GetPage request. It will not notice that the
#    client is gone, until it tries to send something back to the client, or if a timeout
#    kills the query.
#
# So to reproduce the traffic jam, you need:
#
# - A network glitch, which causes one GetPage request/response to be lost or delayed.
#   It might get lost at IP level, and TCP retransmits might take a long time. Or there might
#   be a glitch in the pageserver or compute, which causes the request to be "stuck".
#
# - An application with a application level timeout and retry. If the
#   query doesn't return in a timely a fashion, the application kills the connection and
#   retries the query, or a runs similar query that needs the same page.
#
# The first time the GetPage request is stuck and it disconnects, it leaves behind a
# backend that's still waiting for the GetPage response, and is holding the buffer lock.
# The client has closed the connection, but the server doesn't get the memo.
# On each subsequent retry, the connection will block waiting for the buffer lock, give up,
# and leave behind another backend blocked indefinitely.
#
# The situation unravels when the original backend doing the GetPage request finally
# gets a response, or it gets confirmation that the TCP connection is lost.
#
# This test reproduces the traffic jam using a MITM proxy between pageserver and compute,
# and forcing one GetPage request to get stuck.
#
# Recommendations:
# - set client_connection_check_interval = '10s'. This makes Postgres wake up and check
#   for client connection loss. It's not perfect, it might not notice if the client has
#   e.g rebooted without sending a RST packet, but there's no downside
#
# - Add a timeout to GetPage requests. If no response is received from the pageserver
#   in, say, 10 s, terminate the pageserver connection and retry. XXX: Negotiate this
#   behavior with the storage team
#
#
@pytest.mark.timeout(120)
def test_compute_pageserver_connection_stress2(
    neon_env_builder: NeonEnvBuilder, port_distributor: PortDistributor, pg_bin: PgBin
):
    env = neon_env_builder.init_start()

    # Set up the MITM proxy

    global error_fraction
    global delay_fraction

    error_fraction = 0
    delay_fraction = 0

    async def response_cb(conn_id):
        global delay_fraction
        global error_fraction

        if random.random() < error_fraction:
            raise BreakConnectionException("unlucky")

        orig_delay_fraction = delay_fraction
        if random.random() < delay_fraction:
            delay_fraction = 0
            log.info(f"[{conn_id}] making getpage request STUCK")
            try:
                await asyncio.sleep(300)
            finally:
                delay_fraction = orig_delay_fraction
                log.info(f"[{conn_id}] delay finished")

    mitm_listen_port = port_distributor.get_port()
    mitm = PageserverProxy(mitm_listen_port, env.pageserver.service_port.pg, response_cb)

    def main():
        global error_fraction, delay_fraction
        endpoint = env.endpoints.create(
            "main",
            config_lines=[
                "max_connections=1000",
                "shared_buffers=8MB",
                "log_connections=on",
                "log_disconnections=on",
            ],
        )
        endpoint.start()

        with open(endpoint.pg_data_dir_path() / "postgresql.conf", "a") as conf:
            conf.write(
                f"neon.pageserver_connstring='postgres://localhost:{mitm_listen_port}'  # MITM proxy\n"
            )

        pg_conn = endpoint.connect()
        cur = pg_conn.cursor()

        cur.execute("select pg_reload_conf()")

        scale = 5
        connstr = endpoint.connstr()
        log.info(f"Start a pgbench workload on pg {connstr}")

        error_fraction = 0.001

        pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", f"-s{scale}", connstr])
        error_fraction = 0.00
        delay_fraction = 0.001

        cur.execute("select max(aid) from pgbench_accounts")
        num_accounts = 100000 * scale

        num_clients = 200

        app = WalkingApplication(num_accounts, num_clients, endpoint, 1000000)
        asyncio.run(app.run())

        mitm.shutdown()

    async def mm():
        await asyncio.gather(asyncio.to_thread(main), mitm.run_server())

    asyncio.run(mm())

    # do a graceful shutdown which would had caught the allowed_errors before
    # https://github.com/neondatabase/neon/pull/8632
    env.pageserver.stop()


class WalkingApplication:
    """
    A test application with following characteristics:

    - It performs single-row lookups in pgbench_accounts table, just like pgbench -S

    - Whenever a query takes longer than 10s, the application disconnects, reconnects,
      and retries the query, with the same parameter. This way, if there's a problem
      with a single page, the application will keep retrying it rather than work
      around it.

    - The lookups are not randomly distributed, but form a "walking herd" pattern,
      where the queries walk through all accounts, with some randomness. This way,
      there's a lot of locality of access, but the locality moves throughout the
      table.

    """

    def __init__(self, num_accounts, num_clients, endpoint, num_xacts):
        self.num_accounts = num_accounts
        self.num_clients = num_clients
        self.endpoint = endpoint
        self.running = True
        self.num_xacts = num_xacts

        self.xacts_started = 0
        self.xacts_performed = 0
        self.xacts_failed = 0

    async def run(self):
        async with TaskGroup() as group:
            for i in range(1, self.num_clients):
                group.create_task(self.walking_client(i))

    async def walking_client(self, client_id):
        local_xacts_performed = 0

        conn = None
        stmt = None
        failed = False
        while self.running and self.xacts_started < self.num_xacts:
            self.xacts_started += 1
            if not failed:
                aid = (self.xacts_started * 100 + random.randint(0, 100)) % self.num_accounts + 1

            if conn is None:
                conn = await self.endpoint.connect_async()
                await conn.execute("set statement_timeout=0")
                stmt = await conn.prepare("SELECT abalance FROM pgbench_accounts WHERE aid = $1")

            try:
                async with asyncio.timeout(10):
                    res = await stmt.fetchval(aid)
                    if local_xacts_performed % 1000 == 0:
                        log.info(
                            f"[{client_id}] result {self.xacts_performed}/{self.num_xacts}: balance of account {aid}: {res}"
                        )
                    self.xacts_performed += 1
                    local_xacts_performed += 1
                    failed = False
            except TimeoutError:
                log.info(f"[{client_id}] query on aid {aid} timed out. Reconnecting")
                conn.terminate()
                conn = None
                failed = True
