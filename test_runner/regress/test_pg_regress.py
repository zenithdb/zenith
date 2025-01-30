#
# This file runs pg_regress-based tests.
#
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    check_restored_datadir_content,
    tenant_get_shards,
)
from fixtures.pg_version import PgVersion
from fixtures.remote_storage import s3_storage
from fixtures.utils import skip_in_debug_build

if TYPE_CHECKING:
    from fixtures.neon_fixtures import PgBin
    from pytest import CaptureFixture


TENANT_CONF = {
    # Scaled down thresholds so that we are exercising the pageserver beyond just writing
    # ephemeral/L0 layers, and because debug-mode code is slow to read from full sized ephemeral layer files.
    "pitr_interval": "60s",
    "checkpoint_distance": f"{8 * 1024 * 1024}",
    "compaction_target_size": f"{8 * 1024 * 1024}",
}

# # Ensure that compaction works, on a timeline containing all the diversity that postgres regression tests create.
# # There should have been compactions mid-test as well, this final check is in addition those.
# for (shard, pageserver) in tenant_get_shards(env, env.initial_tenant):
#     pageserver.http_client().timeline_checkpoint(env.initial_tenant, env.initial_timeline, force_repartition=True, force_image_layer_creation=True)


def post_checks(env: NeonEnv, test_output_dir: Path, db_name: str, endpoint: Endpoint):
    """
    After running some opaque tests that create interesting content in a timeline, run
    some generic integrity checks that the storage stack is able to reproduce the written
    data properly.
    """

    ignored_files: list[str] | None = None

    # Neon handles unlogged relations in a special manner. During a
    # basebackup, we ship the init fork as the main fork. This presents a
    # problem in that the endpoint's data directory and the basebackup will
    # have differences and will fail the eventual file comparison.
    #
    # Unlogged tables were introduced in version 9.1. ALTER TABLE grew
    # support for setting the persistence of a table in 9.5. The reason that
    # this doesn't affect versions < 15 (but probably would between 9.1 and
    # 9.5) is that all the regression tests that deal with unlogged tables
    # up until that point dropped the unlogged tables or set them to logged
    # at some point during the test.
    #
    # In version 15, Postgres grew support for unlogged sequences, and with
    # that came a few more regression tests. These tests did not all drop
    # the unlogged tables/sequences prior to finishing.
    #
    # But unlogged sequences came with a bug in that, sequences didn't
    # inherit the persistence of their "parent" tables if they had one. This
    # was fixed and backported to 15, thus exacerbating our problem a bit.
    #
    # So what we can do is just ignore file differences between the data
    # directory and basebackup for unlogged relations.
    results = cast(
        "list[tuple[str, str]]",
        endpoint.safe_psql(
            """
        SELECT
            relkind,
            pg_relation_filepath(
                pg_filenode_relation(reltablespace, relfilenode)
            ) AS unlogged_relation_paths
        FROM pg_class
        WHERE relpersistence = 'u'
        """,
            dbname=db_name,
        ),
    )

    unlogged_relation_files: list[str] = []
    for r in results:
        unlogged_relation_files.append(r[1])
        # This is related to the following Postgres commit:
        #
        # commit ccadf73163ca88bdaa74b8223d4dde05d17f550b
        # Author: Heikki Linnakangas <heikki.linnakangas@iki.fi>
        # Date:   2023-08-23 09:21:31 -0500
        #
        # Use the buffer cache when initializing an unlogged index.
        #
        # This patch was backpatched to 16. Without it, the LSN in the
        # page header would be 0/0 in the data directory, which wouldn't
        # match the LSN generated during the basebackup, thus creating
        # a difference.
        if env.pg_version <= PgVersion.V15 and r[0] == "i":
            unlogged_relation_files.append(f"{r[1]}_init")

    ignored_files = unlogged_relation_files

    check_restored_datadir_content(test_output_dir, env, endpoint, ignored_files=ignored_files)

    # Ensure that compaction/GC works, on a timeline containing all the diversity that postgres regression tests create.
    # There should have been compactions mid-test as well, this final check is in addition those.
    for shard, pageserver in tenant_get_shards(env, env.initial_tenant):
        pageserver.http_client().timeline_checkpoint(
            shard, env.initial_timeline, force_repartition=True, force_image_layer_creation=True
        )

        pageserver.http_client().timeline_gc(shard, env.initial_timeline, None)


# Run the main PostgreSQL regression tests, in src/test/regress.
#
@pytest.mark.timeout(3000)  # Contains many sub-tests, is slow in debug builds
@pytest.mark.parametrize("shard_count", [None, 4])
def test_pg_regress(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_bin: PgBin,
    capsys: CaptureFixture[str],
    base_dir: Path,
    pg_distrib_dir: Path,
    shard_count: int | None,
):
    DBNAME = "regression"

    """
    :param shard_count: if None, create an unsharded tenant.  Otherwise create a tenant with this
                        many shards.
    """
    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count

    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF,
        initial_tenant_shard_count=shard_count,
    )

    # Connect to postgres and create a database called "regression".
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            # Enable the test mode, so that we don't need to patch the test cases.
            "neon.regress_test_mode = true",
        ],
    )
    endpoint.safe_psql(f"CREATE DATABASE {DBNAME}")

    # Create some local directories for pg_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_regress will need.
    build_path = pg_distrib_dir / f"build/{env.pg_version.v_prefixed}/src/test/regress"
    src_path = base_dir / f"vendor/postgres-{env.pg_version.v_prefixed}/src/test/regress"
    bindir = pg_distrib_dir / f"v{env.pg_version}/bin"
    schedule = src_path / "parallel_schedule"
    pg_regress = build_path / "pg_regress"

    pg_regress_command = [
        str(pg_regress),
        '--bindir=""',
        "--use-existing",
        f"--bindir={bindir}",
        f"--dlpath={build_path}",
        f"--schedule={schedule}",
        f"--inputdir={src_path}",
    ]

    env_vars = {
        "PGPORT": str(endpoint.default_options["port"]),
        "PGUSER": endpoint.default_options["user"],
        "PGHOST": endpoint.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_regress_command, env=env_vars, cwd=runpath)

    post_checks(env, test_output_dir, DBNAME, endpoint)


# Run the PostgreSQL "isolation" tests, in src/test/isolation.
#
@pytest.mark.timeout(1500)  # Contains many sub-tests, is slow in debug builds
@pytest.mark.parametrize("shard_count", [None, 4])
def test_isolation(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_bin: PgBin,
    capsys: CaptureFixture[str],
    base_dir: Path,
    pg_distrib_dir: Path,
    shard_count: int | None,
):
    DBNAME = "isolation_regression"

    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF, initial_tenant_shard_count=shard_count
    )

    # Connect to postgres and create a database called "regression".
    # isolation tests use prepared transactions, so enable them
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "max_prepared_transactions=100",
            # Enable the test mode, so that we don't need to patch the test cases.
            "neon.regress_test_mode = true",
            # Stack size should be increased for tests to pass with asan.
            "max_stack_depth = 4MB",
        ],
    )
    endpoint.safe_psql(f"CREATE DATABASE {DBNAME}")

    # Create some local directories for pg_isolation_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_isolation_regress will need.
    build_path = pg_distrib_dir / f"build/{env.pg_version.v_prefixed}/src/test/isolation"
    src_path = base_dir / f"vendor/postgres-{env.pg_version.v_prefixed}/src/test/isolation"
    bindir = pg_distrib_dir / f"v{env.pg_version}/bin"
    schedule = src_path / "isolation_schedule"
    pg_isolation_regress = build_path / "pg_isolation_regress"

    pg_isolation_regress_command = [
        str(pg_isolation_regress),
        "--use-existing",
        f"--bindir={bindir}",
        f"--dlpath={build_path}",
        f"--inputdir={src_path}",
        f"--schedule={schedule}",
    ]

    env_vars = {
        "PGPORT": str(endpoint.default_options["port"]),
        "PGUSER": endpoint.default_options["user"],
        "PGHOST": endpoint.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_isolation_regress_command, env=env_vars, cwd=runpath)

    # This fails with a mismatch on `pg_multixact/offsets/0000`
    # post_checks(env, test_output_dir, DBNAME, endpoint)


# Run extra Neon-specific pg_regress-based tests. The tests and their
# schedule file are in the sql_regress/ directory.
@pytest.mark.parametrize("shard_count", [None, 4])
def test_sql_regress(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_bin: PgBin,
    capsys: CaptureFixture[str],
    base_dir: Path,
    pg_distrib_dir: Path,
    shard_count: int | None,
):
    DBNAME = "regression"

    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF, initial_tenant_shard_count=shard_count
    )

    # Connect to postgres and create a database called "regression".
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            # Enable the test mode, so that we don't need to patch the test cases.
            "neon.regress_test_mode = true",
        ],
    )
    endpoint.safe_psql(f"CREATE DATABASE {DBNAME}")

    # Create some local directories for pg_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_regress will need.
    # This test runs neon specific tests
    build_path = pg_distrib_dir / f"build/v{env.pg_version}/src/test/regress"
    src_path = base_dir / "test_runner/sql_regress"
    bindir = pg_distrib_dir / f"v{env.pg_version}/bin"
    schedule = src_path / "parallel_schedule"
    pg_regress = build_path / "pg_regress"

    pg_regress_command = [
        str(pg_regress),
        "--use-existing",
        f"--bindir={bindir}",
        f"--dlpath={build_path}",
        f"--schedule={schedule}",
        f"--inputdir={src_path}",
    ]

    env_vars = {
        "PGPORT": str(endpoint.default_options["port"]),
        "PGUSER": endpoint.default_options["user"],
        "PGHOST": endpoint.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_regress_command, env=env_vars, cwd=runpath)

    post_checks(env, test_output_dir, DBNAME, endpoint)


@skip_in_debug_build("only run with release build")
def test_tx_abort_with_many_relations(
    neon_env_builder: NeonEnvBuilder,
):
    """
    This is not a pg_regress test as such, but perhaps it should be -- this test exercises postgres
    behavior when aborting a transaction with lots of relations.

    Reproducer for https://github.com/neondatabase/neon/issues/9505
    """

    env = neon_env_builder.init_start()
    ep = env.endpoints.create_start(
        "main",
        tenant_id=env.initial_tenant,
        config_lines=[
            "shared_buffers=1000MB",
            "max_locks_per_transaction=16384",
        ],
    )

    # How many relations: this number is tuned to be long enough to take tens of seconds
    # if the rollback code path is buggy, tripping the test's timeout.
    n = 4000

    def create():
        # Create many relations
        log.info(f"Creating {n} relations...")
        ep.safe_psql_many(
            [
                "BEGIN",
                f"""DO $$
            DECLARE
                i INT;
                table_name TEXT;
            BEGIN
                FOR i IN 1..{n} LOOP
                    table_name := 'table_' || i;
                    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || table_name || ' (id SERIAL PRIMARY KEY, data TEXT)';
                END LOOP;
            END $$;
            """,
                "COMMIT",
            ]
        )

    def truncate():
        # Truncate relations, then roll back the transaction containing the truncations
        log.info(f"Truncating {n} relations...")
        ep.safe_psql_many(
            [
                "BEGIN",
                f"""DO $$
            DECLARE
                i INT;
                table_name TEXT;
            BEGIN
                FOR i IN 1..{n} LOOP
                    table_name := 'table_' || i;
                    EXECUTE 'TRUNCATE ' || table_name ;
                END LOOP;
            END $$;
            """,
            ]
        )

    def rollback_and_wait():
        log.info(f"Rolling back after truncating {n} relations...")
        ep.safe_psql("ROLLBACK")

        # Restart the endpoint: this ensures that we can read back what we just wrote, i.e. pageserver
        # ingest has caught up.
        ep.stop()
        log.info(f"Starting endpoint after truncating {n} relations...")
        ep.start()
        log.info(f"Started endpoint after truncating {n} relations...")

    # Actual create & truncate phases may be slow, these involves lots of WAL records.  We do not
    # apply a special timeout, they are expected to complete within general test timeout
    create()
    truncate()

    # Run in a thread because the failure case is to take pathologically long time, and we don't want
    # to block the test executor on that.
    with ThreadPoolExecutor(max_workers=1) as exec:
        try:
            # Rollback phase should be fast: this is one WAL record that we should process efficiently
            fut = exec.submit(rollback_and_wait)
            fut.result(timeout=15)
        except:
            exec.shutdown(wait=False, cancel_futures=True)
            raise
