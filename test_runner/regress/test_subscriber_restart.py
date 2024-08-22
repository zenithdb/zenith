import threading
import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import wait_until


# This test checks of logical replication subscriber is able to correctly restart replication without receiving duplicates.
# It requires tracking information about replication origins at page server side
def test_subscriber_restart(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("publisher")
    pub = env.endpoints.create("publisher")
    pub.start()

    env.neon_cli.create_branch("subscriber")
    sub = env.endpoints.create("subscriber")
    sub.start()

    n_records = 100000
    n_restarts = 100

    def check_that_changes_propagated():
        scur.execute("SELECT received_lsn from pg_stat_subscription")
        received_lsn = scur.fetchall()[0][0]
        log.info(f"received_lsn={received_lsn}")
        scur.execute("SELECT count(*) FROM t")
        count = scur.fetchall()[0][0]
        log.info(f"count={count}")
        assert count == n_records

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
            # synchronous_commit=on to test a hypothesis for why this test has been flaky.
            # XXX: Add link to the issue
            query = f"CREATE SUBSCRIPTION sub CONNECTION '{pub_conn}' PUBLICATION pub with (synchronous_commit=on)"
            scur.execute(query)
            time.sleep(2)  # let initial table sync complete

        thread = threading.Thread(target=insert_data, args=(pub,), daemon=True)
        thread.start()

        for _ in range(n_restarts):
            # restart subscriber
            # time.sleep(2)
            sub.stop("immediate")
            sub.start()

        thread.join()
        pcur.execute(f"INSERT into t values ({n_records}, 0)")
        n_records += 1
        pcur.execute("SELECT pg_current_wal_flush_lsn()")
        flush_lsn = pcur.fetchall()[0][0]
        log.info(f"flush_lsn={flush_lsn}")
        with sub.cursor() as scur:
            wait_until(60, 0.5, check_that_changes_propagated)
