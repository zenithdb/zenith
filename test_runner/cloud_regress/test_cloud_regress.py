"""
Run the regression tests on the cloud instance of Neon
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fixtures.neon_fixtures import RemotePostgres
from fixtures.pg_version import PgVersion


@pytest.mark.timeout(7200)
@pytest.mark.remote_cluster
def test_cloud_regress(
    remote_pg: RemotePostgres,
    pg_version: PgVersion,
    pg_distrib_dir: Path,
    base_dir: Path,
    test_output_dir: Path,
):
    """
    Run the regression tests
    """
    regress_bin = (
        pg_distrib_dir / f"{pg_version.v_prefixed}/lib/postgresql/pgxs/src/test/regress/pg_regress"
    )
    test_path = base_dir / f"vendor/postgres-{pg_version.v_prefixed}/src/test/regress"

    env_vars = {
        "PGHOST": remote_pg.default_options["host"],
        "PGPORT": str(
            remote_pg.default_options["port"] if "port" in remote_pg.default_options else 5432
        ),
        "PGUSER": remote_pg.default_options["user"],
        "PGPASSWORD": remote_pg.default_options["password"],
        "PGDATABASE": remote_pg.default_options["dbname"],
    }
    regress_cmd = [
        str(regress_bin),
        f"--inputdir={test_path}",
        f"--bindir={pg_distrib_dir}/{pg_version.v_prefixed}/bin",
        "--dlpath=/usr/local/lib",
        "--max-concurrent-tests=20",
        f"--schedule={test_path}/parallel_schedule",
        "--max-connections=5",
    ]
    remote_pg.pg_bin.run(regress_cmd, env=env_vars, cwd=test_output_dir)
