import os
import queue
import random
import threading
import time
from typing import List

from fixtures.neon_fixtures import DEFAULT_BRANCH_NAME, NeonEnvBuilder
from fixtures.utils import query_scalar


def test_local_file_cache_unlink(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    cache_dir = os.path.join(env.repo_dir, "file_cache")
    os.mkdir(cache_dir)

    env.neon_cli.create_branch("empty", ancestor_branch_name=DEFAULT_BRANCH_NAME)
    env.neon_cli.create_branch("test_local_file_cache_unlink", "empty")

    endpoint = env.endpoints.create_start(
        "test_local_file_cache_unlink",
        config_lines=[
            "shared_buffers='1MB'",
            f"neon.file_cache_path='{cache_dir}/file.cache'",
            "neon.max_file_cache_size='64MB'",
            "neon.file_cache_size_limit='10MB'",
        ],
    )

    cur = endpoint.connect().cursor()

    stop = threading.Event()
    n_rows = 100000
    n_threads = 20
    n_updates_per_connection = 1000

    cur.execute("CREATE TABLE lfctest (id int4 PRIMARY KEY, n int) WITH (fillfactor=10)")
    cur.execute(f"INSERT INTO lfctest SELECT g, 1 FROM generate_series(1, {n_rows}) g")

    # Start threads that will perform random UPDATEs. Each UPDATE
    # increments the counter on the row, so that we can check at the
    # end that the sum of all the counters match the number of updates
    # performed (plus the initial 1 on each row).
    #
    # Furthermore, each thread will reconnect between every 1000 updates.
    def run_updates(n_updates_performed_q: queue.Queue[int]):
        n_updates_performed = 0
        conn = endpoint.connect()
        cur = conn.cursor()
        while not stop.is_set():
            id = random.randint(1, n_rows)
            cur.execute(f"UPDATE lfctest SET n = n + 1 WHERE id = {id}")
            n_updates_performed += 1
            if n_updates_performed % n_updates_per_connection == 0:
                cur.close()
                conn.close()
                conn = endpoint.connect()
                cur = conn.cursor()
        n_updates_performed_q.put(n_updates_performed)

    n_updates_performed_q: queue.Queue[int] = queue.Queue()
    threads: List[threading.Thread] = []
    for _i in range(n_threads):
        thread = threading.Thread(target=run_updates, args=(n_updates_performed_q,), daemon=True)
        thread.start()
        threads.append(thread)

    time.sleep(5)

    # unlink, this is what we're actually testing
    new_cache_dir = os.path.join(env.repo_dir, "file_cache_new")
    os.rename(cache_dir, new_cache_dir)

    time.sleep(10)

    stop.set()

    n_updates_performed = 0
    for thread in threads:
        thread.join()
        n_updates_performed += n_updates_performed_q.get()

    assert query_scalar(cur, "SELECT SUM(n) FROM lfctest") == n_rows + n_updates_performed
