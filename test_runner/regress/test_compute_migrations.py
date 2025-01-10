from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from fixtures.compute_migrations import COMPUTE_MIGRATIONS, NUM_COMPUTE_MIGRATIONS

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def test_compute_migrations_retry(neon_simple_env: NeonEnv, compute_migrations_dir: Path):
    """
    Test that compute_ctl can recover from migration failures next time it
    starts, and that the persisted migration ID is correct in such cases.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create("main")
    endpoint.respec(skip_pg_catalog_updates=False)

    for i in range(1, NUM_COMPUTE_MIGRATIONS + 1):
        endpoint.start(env={"FAILPOINTS": f"compute-migration=return({i})"})

        # Make sure that the migrations ran
        endpoint.wait_for_migrations(wait_for=i - 1)

        # Confirm that we correctly recorded that in the
        # neon_migration.migration_id table
        with endpoint.cursor() as cur:
            cur.execute("SELECT id FROM neon_migration.migration_id")
            migration_id = cast("int", cur.fetchall()[0][0])
            assert migration_id == i - 1

        endpoint.stop()

    endpoint.start()

    # Now wait for the rest of the migrations
    endpoint.wait_for_migrations()

    with endpoint.cursor() as cur:
        cur.execute("SELECT id FROM neon_migration.migration_id")
        migration_id = cast("int", cur.fetchall()[0][0])
        assert migration_id == NUM_COMPUTE_MIGRATIONS

    for i, m in enumerate(COMPUTE_MIGRATIONS, start=1):
        migration_query = (compute_migrations_dir / m).read_text(encoding="utf-8")
        if not migration_query.startswith("-- SKIP"):
            pattern = rf"Skipping migration id={i}"
        else:
            pattern = rf"Running migration id={i}"

        endpoint.log_contains(pattern)


@pytest.mark.parametrize(
    "migration",
    (pytest.param((i, m), id=str(i)) for i, m in enumerate(COMPUTE_MIGRATIONS, start=1)),
)
def test_compute_migrations_e2e(
    neon_simple_env: NeonEnv,
    compute_migrations_dir: Path,
    compute_migrations_test_dir: Path,
    migration: tuple[int, str],
):
    """
    Test that the migrations perform as advertised.
    """
    env = neon_simple_env

    migration_id = migration[0]
    migration_filename = migration[1]

    migration_query = (compute_migrations_dir / migration_filename).read_text(encoding="utf-8")
    if migration_query.startswith("-- SKIP"):
        pytest.skip("The migration is marked as SKIP")

    endpoint = env.endpoints.create("main")
    endpoint.respec(skip_pg_catalog_updates=False)

    # Stop applying migrations after the one we want to test, so that we can
    # test the state of the cluster at the given migration ID
    endpoint.start(env={"FAILPOINTS": f"compute-migration=return({migration_id + 1})"})

    endpoint.wait_for_migrations(wait_for=migration_id)

    check_query = (compute_migrations_test_dir / migration_filename).read_text(encoding="utf-8")
    endpoint.safe_psql(check_query)
