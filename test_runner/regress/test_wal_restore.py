import os
import sys
import tarfile
from pathlib import Path

import pytest
import tempfile
import zstandard
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    VanillaPostgres,
)
from fixtures.port_distributor import PortDistributor
from fixtures.types import TenantId, TimelineId


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="restore_from_wal.sh supports only Linux",
)
def test_wal_restore(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    base_dir: Path,
    pg_distrib_dir: Path,
):
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_wal_restore")
    endpoint = env.endpoints.create_start("test_wal_restore")
    endpoint.safe_psql("create table t as select generate_series(1,300000)")
    tenant_id = TenantId(endpoint.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(endpoint.safe_psql("show neon.timeline_id")[0][0])
    env.pageserver.stop()
    port = port_distributor.get_port()
    data_dir = test_output_dir / "pgsql.restored"
    with VanillaPostgres(
        data_dir, PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version), port
    ) as restored:
        pg_bin.run_capture(
            [
                str(base_dir / "libs" / "utils" / "scripts" / "restore_from_wal.sh"),
                str(pg_distrib_dir / f"v{env.pg_version}/bin"),
                str(
                    test_output_dir
                    / "repo"
                    / "safekeepers"
                    / "sk1"
                    / str(tenant_id)
                    / str(timeline_id)
                ),
                str(data_dir),
                str(port),
            ]
        )
        restored.start()
        assert restored.safe_psql("select count(*) from t", user="cloud_admin") == [(300000,)]


def decompress_zstd(
    input_file_name: Path,
    output_dir: Path,
):
    log.info(f"decompressing zstd to: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(suffix=".tar") as temp:
        decompressor = zstandard.ZstdDecompressor()
        with open(input_file_name, "rb") as input_file:
            decompressor.copy_stream(input_file, temp)
        with tarfile.open(fileobj=temp) as tfile:
            tfile.extractall(path=output_dir)


def test_wal_restore_initdb(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    base_dir: Path,
    pg_distrib_dir: Path,
):
    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")
    endpoint.safe_psql("create table t as select generate_series(1,300000)")
    tenant_id = TenantId(endpoint.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(endpoint.safe_psql("show neon.timeline_id")[0][0])
    env.pageserver.stop()
    port = port_distributor.get_port()
    data_dir = test_output_dir / "pgsql.restored"

    initdb_path = Path(
        test_output_dir
        / "repo"
        / "local_fs_remote_storage"
        / "pageserver"
        / "tenants"
        / str(tenant_id)
        / "timelines"
        / str(timeline_id)
        / "initdb.tar.zst"
    )

    decompress_zstd(initdb_path, data_dir)
    with VanillaPostgres(
        data_dir, PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version), port
    ) as restored:
        pg_bin.run_capture(
            [
                str(base_dir / "libs" / "utils" / "scripts" / "restore_from_wal_initdb.sh"),
                str(pg_distrib_dir / f"v{env.pg_version}/bin"),
                str(
                    test_output_dir
                    / "repo"
                    / "safekeepers"
                    / "sk1"
                    / str(tenant_id)
                    / str(timeline_id)
                ),
                str(data_dir),
                str(port),
            ]
        )
        restored.start()
        assert restored.safe_psql("select count(*) from t", user="cloud_admin") == [(300000,)]
