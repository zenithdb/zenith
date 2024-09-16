import os
from pathlib import Path

from fixtures.common_types import TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    PgBin,
    fork_at_current_lsn,
    import_timeline_from_vanilla_postgres,
)


#
# Test branching, when a transaction is in prepared state
#
def twophase_test_on_timeline(env: NeonEnv):
    endpoint = env.endpoints.create_start(
        "test_twophase", config_lines=["max_prepared_transactions=5"]
    )

    conn = endpoint.connect()
    cur = conn.cursor()

    # FIXME: Switch to the next WAL segment, to work around the bug fixed in
    # https://github.com/neondatabase/neon/pull/8914.  When that is merged, this can be
    # removed.
    cur.execute("select pg_switch_wal()")

    cur.execute("CREATE TABLE foo (t text)")

    # Prepare a transaction that will insert a row
    cur.execute("BEGIN")
    cur.execute("INSERT INTO foo VALUES ('one')")
    cur.execute("PREPARE TRANSACTION 'insert_one'")

    # Prepare another transaction that will insert a row
    cur.execute("BEGIN")
    cur.execute("INSERT INTO foo VALUES ('two')")
    cur.execute("PREPARE TRANSACTION 'insert_two'")

    # Prepare a transaction that will insert a row
    cur.execute("BEGIN")
    cur.execute("INSERT INTO foo VALUES ('three')")
    cur.execute("PREPARE TRANSACTION 'insert_three'")

    # Prepare another transaction that will insert a row
    cur.execute("BEGIN")
    cur.execute("INSERT INTO foo VALUES ('four')")
    cur.execute("PREPARE TRANSACTION 'insert_four'")

    # On checkpoint state data copied to files in
    # pg_twophase directory and fsynced
    cur.execute("CHECKPOINT")

    twophase_files = os.listdir(endpoint.pg_twophase_dir_path())
    log.info(twophase_files)
    assert len(twophase_files) == 4

    cur.execute("COMMIT PREPARED 'insert_three'")
    cur.execute("ROLLBACK PREPARED 'insert_four'")
    cur.execute("CHECKPOINT")

    twophase_files = os.listdir(endpoint.pg_twophase_dir_path())
    log.info(twophase_files)
    assert len(twophase_files) == 2

    # Create a branch with the transaction in prepared state
    fork_at_current_lsn(env, endpoint, "test_twophase_prepared", "test_twophase")

    # Start compute on the new branch
    endpoint2 = env.endpoints.create_start(
        "test_twophase_prepared",
        config_lines=["max_prepared_transactions=5"],
    )

    # Check that we restored only needed twophase files
    twophase_files2 = os.listdir(endpoint2.pg_twophase_dir_path())
    log.info(twophase_files2)
    assert twophase_files2.sort() == twophase_files.sort()

    conn2 = endpoint2.connect()
    cur2 = conn2.cursor()

    # On the new branch, commit one of the prepared transactions,
    # abort the other one.
    cur2.execute("COMMIT PREPARED 'insert_one'")
    cur2.execute("ROLLBACK PREPARED 'insert_two'")

    cur2.execute("SELECT * FROM foo")
    assert cur2.fetchall() == [("one",), ("three",)]

    # Only one committed insert is visible on the original branch
    cur.execute("SELECT * FROM foo")
    assert cur.fetchall() == [("three",)]


def test_twophase(neon_simple_env: NeonEnv):
    """
    Test branching, when a transaction is in prepared state
    """
    env = neon_simple_env
    env.neon_cli.create_branch("test_twophase")

    twophase_test_on_timeline(env)


def test_twophase_nonzero_epoch(
    neon_simple_env: NeonEnv,
    test_output_dir: Path,
    pg_bin: PgBin,
    vanilla_pg,
):
    """
    Same as 'test_twophase' test, but with a non-zero XID epoch, i.e. after 4 billion XIDs
    have been consumed. (This is to ensure that we correctly use the full 64-bit XIDs in
    pg_twophase filenames with PostgreSQL v17.)
    """
    env = neon_simple_env

    # Reset the vanilla Postgres instance with a higher XID epoch
    pg_resetwal_path = os.path.join(pg_bin.pg_bin_path, "pg_resetwal")
    cmd = [pg_resetwal_path, "--epoch=1000000000", "-D", str(vanilla_pg.pgdatadir)]
    pg_bin.run_capture(cmd)

    timeline_id = TimelineId.generate()

    # Import the cluster to Neon
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")
    import_timeline_from_vanilla_postgres(
        test_output_dir,
        env,
        pg_bin,
        env.initial_tenant,
        timeline_id,
        "test_twophase",
        vanilla_pg.connstr(),
    )
    vanilla_pg.stop()  # don't need the original server anymore

    twophase_test_on_timeline(env)
