import random
import threading
from threading import Thread

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, check_restored_datadir_content
from fixtures.utils import query_scalar


#
# Test multixact state after branching
# Now this test is very minimalistic -
# it only checks next_multixact_id field in restored pg_control,
# since we don't have functions to check multixact internals.
#
def test_multixact_conc(neon_simple_env: NeonEnv, test_output_dir):
    env = neon_simple_env
    env.neon_cli.create_branch("test_multixact", "empty")
    endpoint = env.endpoints.create_start("test_multixact")

    log.info("postgres is running on 'test_multixact' branch")

    n_records = 100
    n_threads =   5
    n_iters =  1000
    n_restarts = 10

    cur = endpoint.connect().cursor()
    cur.execute(
        f"""
        CREATE TABLE t1(pk int primary key, val integer);
        INSERT INTO t1 values (generate_series(1, {n_records}), 0);
    """
    )

    next_multixact_id_old = query_scalar(
        cur, "SELECT next_multixact_id FROM pg_control_checkpoint()"
    )

    # Lock entries using parallel connections in a round-robin fashion.
    def do_updates():
        conn = endpoint.connect(autocommit=False)
        for i in range(n_iters):
            pk = random.randrange(1, n_records)
            conn.cursor().execute(f"update t1 set val=val+1 where pk={pk}")
            conn.cursor().execute("select * from t1 for key share")
            conn.commit()
        conn.close()

    for iter in range(n_restarts):
        threads: List[threading.Thread] = []
        for i in range(n_threads):
            threads.append(threading.Thread(target=do_updates, args=(), daemon=False))
            threads[-1].start()

        for thread in threads:
            thread.join()

        # Restart endpoint
        endpoint.stop()
        endpoint.start()

        conn = endpoint.connect()
        cur = conn.cursor()
        cur.execute("select count(*) from t1")
        assert cur.fetchone() == (n_records,)

    # force wal flush
    cur.execute("checkpoint")

    cur.execute(
        "SELECT next_multixact_id, pg_current_wal_insert_lsn() FROM pg_control_checkpoint()"
    )
    res = cur.fetchone()
    assert res is not None
    next_multixact_id = res[0]
    lsn = res[1]

    # Ensure that we did lock some tuples
    assert int(next_multixact_id) > int(next_multixact_id_old)

    # Branch at this point
    env.neon_cli.create_branch("test_multixact_new", "test_multixact", ancestor_start_lsn=lsn)
    endpoint_new = env.endpoints.create_start("test_multixact_new")

    log.info("postgres is running on 'test_multixact_new' branch")
    next_multixact_id_new = endpoint_new.safe_psql(
        "SELECT next_multixact_id FROM pg_control_checkpoint()"
    )[0][0]

    # Check that we restored pg_controlfile correctly
    assert next_multixact_id_new == next_multixact_id
