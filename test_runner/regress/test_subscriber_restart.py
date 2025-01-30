from __future__ import annotations

import threading
import time

import pytest
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import wait_until


# This test checks of logical replication subscriber is able to correctly restart replication without receiving duplicates.
# It requires tracking information about replication origins at page server side
@pytest.mark.timeout(900)  # This test is slow with sanitizers enabled, especially on ARM
def test_subscriber_restart(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.create_branch("publisher")
    pub = env.endpoints.create("publisher")
    pub.start()

    sub_timeline_id = env.create_branch("subscriber")
    sub = env.endpoints.create("subscriber")
    sub.start()

    n_records = 100000
    n_restarts = 100

    def check_that_changes_propagated():
        scur.execute("SELECT count(*) FROM t")
        res = scur.fetchall()
        assert res[0][0] == n_records

    def insert_data(pub):
        with pub.cursor() as pcur:
            for i in range(0, n_records):
                pcur.execute("INSERT into t values (%s,random()*100000)", (i,))

    with pub.cursor() as pcur:
        with sub.cursor() as scur:
            pcur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
            pcur.execute("CREATE PUBLICATION pub FOR TABLE t")
            scur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
            # scur.execute("CREATE INDEX on t(sk)") # slowdown applying WAL at replica
            pub_conn = f"host=localhost port={pub.pg_port} dbname=postgres user=cloud_admin"
            query = f"CREATE SUBSCRIPTION sub CONNECTION '{pub_conn}' PUBLICATION pub"
            scur.execute(query)
            time.sleep(2)  # let initial table sync complete

        thread = threading.Thread(target=insert_data, args=(pub,), daemon=True)
        thread.start()

        for _ in range(n_restarts):
            # restart subscriber
            # time.sleep(2)
            sub.stop("immediate", sks_wait_walreceiver_gone=(env.safekeepers, sub_timeline_id))
            sub.start()

        thread.join()
        pcur.execute(f"INSERT into t values ({n_records}, 0)")
        n_records += 1
        with sub.cursor() as scur:
            wait_until(check_that_changes_propagated)
