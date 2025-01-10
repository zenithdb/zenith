from __future__ import annotations

import os

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.utils import query_scalar, wait_until


#
# Test compute node start after clog truncation
#
def test_clog_truncate(neon_env_builder: NeonEnvBuilder):
    # Use a multi-sharded tenant because WAL ingest logic is shard-dependent, and
    # this test is one of the very few that exercises a CLogTruncate WAL record.
    env = neon_env_builder.init_start(initial_tenant_shard_count=2)

    # set aggressive autovacuum to make sure that truncation will happen
    config = [
        "autovacuum_max_workers=10",
        "autovacuum_vacuum_threshold=0",
        "autovacuum_vacuum_insert_threshold=0",
        "autovacuum_vacuum_cost_delay=0",
        "autovacuum_vacuum_cost_limit=10000",
        "autovacuum_naptime =1s",
        "autovacuum_freeze_max_age=100000",
    ]

    endpoint = env.endpoints.create_start("main", config_lines=config)

    # Install extension containing function needed for test
    endpoint.safe_psql("CREATE EXTENSION neon_test_utils")

    # Consume many xids to advance clog
    log.info("Consuming xids...")
    with endpoint.cursor() as cur:
        cur.execute("select test_consume_xids(1000*1000*10);")
        log.info("xids consumed")

        # call a checkpoint to trigger TruncateSubtrans
        cur.execute("CHECKPOINT;")

        # ensure WAL flush
        cur.execute("select txid_current()")
        log.info(cur.fetchone())

    # wait for autovacuum to truncate the pg_xact
    # XXX Is it worth to add a timeout here?
    pg_xact_0000_path = os.path.join(endpoint.pg_xact_dir_path(), "0000")
    log.info(f"pg_xact_0000_path = {pg_xact_0000_path}")

    def assert_file_removed():
        exists = os.path.isfile(pg_xact_0000_path)
        if exists:
            log.info(f"file exists. wait for truncation: {pg_xact_0000_path=}")
        assert not exists

    log.info("Waiting for truncation...")
    wait_until(assert_file_removed)

    # checkpoint to advance latest lsn
    log.info("Checkpointing...")
    with endpoint.cursor() as cur:
        cur.execute("CHECKPOINT;")
        lsn_after_truncation = query_scalar(cur, "select pg_current_wal_insert_lsn()")

    # create new branch after clog truncation and start a compute node on it
    log.info(f"create branch at lsn_after_truncation {lsn_after_truncation}")
    env.create_branch(
        "test_clog_truncate_new",
        ancestor_branch_name="main",
        ancestor_start_lsn=lsn_after_truncation,
    )
    endpoint2 = env.endpoints.create_start("test_clog_truncate_new")

    # check that new node doesn't contain truncated segment
    pg_xact_0000_path_new = os.path.join(endpoint2.pg_xact_dir_path(), "0000")
    log.info(f"pg_xact_0000_path_new = {pg_xact_0000_path_new}")
    assert os.path.isfile(pg_xact_0000_path_new) is False
