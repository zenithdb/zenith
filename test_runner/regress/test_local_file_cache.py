import os
import random
import threading
import time
from typing import List

from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import query_scalar


def test_local_file_cache_unlink(neon_simple_env: NeonEnv):
    env = neon_simple_env

    cache_dir = os.path.join(env.repo_dir, "file_cache")
    os.mkdir(cache_dir)

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

    n_rows = 100000
    n_threads = 20
    n_updates_per_thread = 10000
    n_updates_per_connection = 1000
    n_total_updates = n_threads * n_updates_per_thread

    cur.execute("CREATE TABLE lfctest (id int4 PRIMARY KEY, n int) WITH (fillfactor=10)")
    cur.execute(f"INSERT INTO lfctest SELECT g, 1 FROM generate_series(1, {n_rows}) g")

    # Start threads that will perform random UPDATEs. Each UPDATE
    # increments the counter on the row, so that we can check at the
    # end that the sum of all the counters match the number of updates
    # performed (plus the initial 1 on each row).
    #
    # Furthermore, each thread will reconnect between every 1000 updates.
    def run_updates():
        n_updates_performed = 0
        conn = endpoint.connect()
        cur = conn.cursor()
        for _ in range(n_updates_per_thread):
            id = random.randint(1, n_rows)
            cur.execute(f"UPDATE lfctest SET n = n + 1 WHERE id = {id}")
            n_updates_performed += 1
            if n_updates_performed % n_updates_per_connection == 0:
                cur.close()
                conn.close()
                conn = endpoint.connect()
                cur = conn.cursor()

    threads: List[threading.Thread] = []
    for _i in range(n_threads):
        thread = threading.Thread(target=run_updates, args=(), daemon=True)
        thread.start()
        threads.append(thread)

    time.sleep(5)

    new_cache_dir = os.path.join(env.repo_dir, "file_cache_new")
    os.rename(cache_dir, new_cache_dir)

    for thread in threads:
        thread.join()

    assert query_scalar(cur, "SELECT SUM(n) FROM lfctest") == n_total_updates + n_rows
