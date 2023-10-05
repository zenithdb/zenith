import copy
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, List, Optional

import pytest
import toml  # TODO: replace with tomllib for Python >= 3.11
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonCli,
    NeonEnvBuilder,
    PgBin,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import (
    timeline_delete_wait_completed,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind, RemoteStorageUser
from fixtures.types import Lsn
from pytest import FixtureRequest

#
# A test suite that help to prevent unintentionally breaking backward or forward compatibility between Neon releases.
# - `test_create_snapshot` a script wrapped in a test that creates a data snapshot.
# - `test_backward_compatibility` checks that the current version of Neon can start/read/interact with a data snapshot created by the previous version.
#   The path to the snapshot is configured by COMPATIBILITY_SNAPSHOT_DIR environment variable.
#   If the breakage is intentional, the test can be xfaild with setting ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE=true.
# - `test_forward_compatibility` checks that a snapshot created by the current version can be started/read/interacted by the previous version of Neon.
#   Paths to Neon and Postgres are configured by COMPATIBILITY_NEON_BIN and COMPATIBILITY_POSTGRES_DISTRIB_DIR environment variables.
#   If the breakage is intentional, the test can be xfaild with setting ALLOW_FORWARD_COMPATIBILITY_BREAKAGE=true.
#
# The file contains a couple of helper functions:
# - prepare_snapshot copies the snapshot, cleans it up and makes it ready for the current version of Neon (replaces paths and ports in config files).
# - check_neon_works performs the test itself, feel free to add more checks there.
#
#
# How to run `test_backward_compatibility` locally:
#
#    export DEFAULT_PG_VERSION=15
#    export BUILD_TYPE=release
#    export CHECK_ONDISK_DATA_COMPATIBILITY=true
#
#    # Build previous version of binaries and create a data snapshot:
#    rm -rf pg_install target
#    git checkout <previous version>
#    CARGO_BUILD_FLAGS="--features=testing" make -s -j`nproc`
#    ./scripts/pytest -k test_create_snapshot
#
#    # Build current version of binaries
#    rm -rf pg_install target
#    git checkout <current version>
#    CARGO_BUILD_FLAGS="--features=testing" make -s -j`nproc`
#
#    # Run backward compatibility test
#    COMPATIBILITY_SNAPSHOT_DIR=test_output/compatibility_snapshot_pgv${DEFAULT_PG_VERSION} \
#       ./scripts/pytest -k test_backward_compatibility
#
#
# How to run `test_forward_compatibility` locally:
#
#    export DEFAULT_PG_VERSION=15
#    export BUILD_TYPE=release
#    export CHECK_ONDISK_DATA_COMPATIBILITY=true
#
#    # Build previous version of binaries and store them somewhere:
#    rm -rf pg_install target
#    git checkout <previous version>
#    CARGO_BUILD_FLAGS="--features=testing" make -s -j`nproc`
#    mkdir -p neon_previous/target
#    cp -a target/${BUILD_TYPE} ./neon_previous/target/${BUILD_TYPE}
#    cp -a pg_install ./neon_previous/pg_install
#
#    # Build current version of binaries and create a data snapshot:
#    rm -rf pg_install target
#    git checkout <current version>
#    CARGO_BUILD_FLAGS="--features=testing" make -s -j`nproc`
#    ./scripts/pytest -k test_create_snapshot
#
#    # Run forward compatibility test
#    COMPATIBILITY_NEON_BIN=neon_previous/target/${BUILD_TYPE} \
#    COMPATIBILITY_POSTGRES_DISTRIB_DIR=neon_previous/pg_install \
#       ./scripts/pytest -k test_forward_compatibility
#

check_ondisk_data_compatibility_if_enabled = pytest.mark.skipif(
    os.environ.get("CHECK_ONDISK_DATA_COMPATIBILITY") is None,
    reason="CHECK_ONDISK_DATA_COMPATIBILITY env is not set",
)


@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(before="test_forward_compatibility")
def test_create_snapshot(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    top_output_dir: Path,
    test_output_dir: Path,
    pg_version: PgVersion,
):
    # The test doesn't really test anything
    # it creates a new snapshot for releases after we tested the current version against the previous snapshot in `test_backward_compatibility`.
    #
    # There's no cleanup here, it allows to adjust the data in `test_backward_compatibility` itself without re-collecting it.
    neon_env_builder.pg_version = pg_version
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")

    # FIXME: Is this expected?
    env.pageserver.allowed_errors.append(
        ".*init_tenant_mgr: marking .* as locally complete, while it doesn't exist in remote index.*"
    )

    pg_bin.run_capture(["pgbench", "--initialize", "--scale=10", endpoint.connstr()])
    pg_bin.run_capture(["pgbench", "--time=60", "--progress=2", endpoint.connstr()])
    pg_bin.run_capture(
        ["pg_dumpall", f"--dbname={endpoint.connstr()}", f"--file={test_output_dir / 'dump.sql'}"]
    )

    snapshot_config = toml.load(test_output_dir / "repo" / "config")
    tenant_id = snapshot_config["default_tenant_id"]
    timeline_id = dict(snapshot_config["branch_name_mappings"]["main"])[tenant_id]

    pageserver_http = env.pageserver.http_client()
    lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])

    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, lsn)

    env.endpoints.stop_all()
    for sk in env.safekeepers:
        sk.stop()
    env.pageserver.stop()

    # Directory `compatibility_snapshot_dir` is uploaded to S3 in a workflow, keep the name in sync with it
    compatibility_snapshot_dir = (
        top_output_dir / f"compatibility_snapshot_pg{pg_version.v_prefixed}"
    )
    if compatibility_snapshot_dir.exists():
        shutil.rmtree(compatibility_snapshot_dir)
    shutil.copytree(test_output_dir, compatibility_snapshot_dir)


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(after="test_create_snapshot")
def test_backward_compatibility(
    pg_bin: PgBin,
    port_distributor: PortDistributor,
    test_output_dir: Path,
    neon_binpath: Path,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    request: FixtureRequest,
):
    """
    Test that the new binaries can read old data
    """
    compatibility_snapshot_dir_env = os.environ.get("COMPATIBILITY_SNAPSHOT_DIR")
    assert (
        compatibility_snapshot_dir_env is not None
    ), f"COMPATIBILITY_SNAPSHOT_DIR is not set. It should be set to `compatibility_snapshot_pg{pg_version.v_prefixed}` path generateted by test_create_snapshot (ideally generated by the previous version of Neon)"
    compatibility_snapshot_dir = Path(compatibility_snapshot_dir_env).resolve()

    breaking_changes_allowed = (
        os.environ.get("ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE", "false").lower() == "true"
    )

    try:
        # Copy the snapshot to current directory, and prepare for the test
        prepare_snapshot(
            from_dir=compatibility_snapshot_dir,
            to_dir=test_output_dir / "compatibility_snapshot",
            port_distributor=port_distributor,
        )

        check_neon_works(
            test_output_dir / "compatibility_snapshot" / "repo",
            neon_binpath,
            neon_binpath,
            pg_distrib_dir,
            pg_version,
            port_distributor,
            test_output_dir,
            pg_bin,
            request,
        )
    except Exception:
        if breaking_changes_allowed:
            pytest.xfail(
                "Breaking changes are allowed by ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE env var"
            )
        else:
            raise

    assert (
        not breaking_changes_allowed
    ), "Breaking changes are allowed by ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE, but the test has passed without any breakage"


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(after="test_create_snapshot")
def test_forward_compatibility(
    test_output_dir: Path,
    top_output_dir: Path,
    port_distributor: PortDistributor,
    pg_version: PgVersion,
    request: FixtureRequest,
    neon_binpath: Path,
):
    """
    Test that the old binaries can read new data
    """
    compatibility_neon_bin_env = os.environ.get("COMPATIBILITY_NEON_BIN")
    assert compatibility_neon_bin_env is not None, (
        "COMPATIBILITY_NEON_BIN is not set. It should be set to a path with Neon binaries "
        "(ideally generated by the previous version of Neon)"
    )
    compatibility_neon_bin = Path(compatibility_neon_bin_env).resolve()

    compatibility_postgres_distrib_dir_env = os.environ.get("COMPATIBILITY_POSTGRES_DISTRIB_DIR")
    assert (
        compatibility_postgres_distrib_dir_env is not None
    ), "COMPATIBILITY_POSTGRES_DISTRIB_DIR is not set. It should be set to a pg_install directory (ideally generated by the previous version of Neon)"
    compatibility_postgres_distrib_dir = Path(compatibility_postgres_distrib_dir_env).resolve()

    compatibility_snapshot_dir = (
        top_output_dir / f"compatibility_snapshot_pg{pg_version.v_prefixed}"
    )

    breaking_changes_allowed = (
        os.environ.get("ALLOW_FORWARD_COMPATIBILITY_BREAKAGE", "false").lower() == "true"
    )

    try:
        # Copy the snapshot to current directory, and prepare for the test
        prepare_snapshot(
            from_dir=compatibility_snapshot_dir,
            to_dir=test_output_dir / "compatibility_snapshot",
            port_distributor=port_distributor,
            pg_distrib_dir=compatibility_postgres_distrib_dir,
        )

        check_neon_works(
            test_output_dir / "compatibility_snapshot" / "repo",
            compatibility_neon_bin,
            neon_binpath,
            compatibility_postgres_distrib_dir,
            pg_version,
            port_distributor,
            test_output_dir,
            PgBin(test_output_dir, compatibility_postgres_distrib_dir, pg_version),
            request,
        )
    except Exception:
        if breaking_changes_allowed:
            pytest.xfail(
                "Breaking changes are allowed by ALLOW_FORWARD_COMPATIBILITY_BREAKAGE env var"
            )
        else:
            raise

    assert (
        not breaking_changes_allowed
    ), "Breaking changes are allowed by ALLOW_FORWARD_COMPATIBILITY_BREAKAGE, but the test has passed without any breakage"


def prepare_snapshot(
    from_dir: Path,
    to_dir: Path,
    port_distributor: PortDistributor,
    pg_distrib_dir: Optional[Path] = None,
):
    assert from_dir.exists(), f"Snapshot '{from_dir}' doesn't exist"
    assert (from_dir / "repo").exists(), f"Snapshot '{from_dir}' doesn't contain a repo directory"
    assert (from_dir / "dump.sql").exists(), f"Snapshot '{from_dir}' doesn't contain a dump.sql"

    log.info(f"Copying snapshot from {from_dir} to {to_dir}")
    shutil.copytree(from_dir, to_dir)

    repo_dir = to_dir / "repo"

    snapshot_config_toml = repo_dir / "config"
    snapshot_config = toml.load(snapshot_config_toml)

    # Remove old logs to avoid confusion in test artifacts
    for logfile in repo_dir.glob("**/*.log"):
        logfile.unlink()

    # Remove old computes in 'endpoints'. Old versions of the control plane used a directory
    # called "pgdatadirs". Delete it, too.
    if (repo_dir / "endpoints").exists():
        shutil.rmtree(repo_dir / "endpoints")
    if (repo_dir / "pgdatadirs").exists():
        shutil.rmtree(repo_dir / "pgdatadirs")
    os.mkdir(repo_dir / "endpoints")

    # Update paths and ports in config files
    legacy_pageserver_toml = repo_dir / "pageserver.toml"
    legacy_bundle = os.path.exists(legacy_pageserver_toml)

    path_to_config: dict[Path, dict[Any, Any]] = {}
    if legacy_bundle:
        os.mkdir(repo_dir / "pageserver_1")
        path_to_config[repo_dir / "pageserver_1" / "pageserver.toml"] = toml.load(
            legacy_pageserver_toml
        )
        os.remove(legacy_pageserver_toml)
        os.rename(repo_dir / "tenants", repo_dir / "pageserver_1" / "tenants")
    else:
        for ps_conf in snapshot_config["pageservers"]:
            config_path = repo_dir / f"pageserver_{ps_conf['id']}" / "pageserver.toml"
            path_to_config[config_path] = toml.load(config_path)

    # For each pageserver config, edit it and rewrite
    for config_path, pageserver_config in path_to_config.items():
        pageserver_config["remote_storage"]["local_path"] = str(
            LocalFsStorage.component_path(repo_dir, RemoteStorageUser.PAGESERVER)
        )

        for param in ("listen_http_addr", "listen_pg_addr", "broker_endpoint"):
            pageserver_config[param] = port_distributor.replace_with_new_port(
                pageserver_config[param]
            )

        # We don't use authentication in compatibility tests
        # so just remove authentication related settings.
        pageserver_config.pop("pg_auth_type", None)
        pageserver_config.pop("http_auth_type", None)

        if pg_distrib_dir:
            pageserver_config["pg_distrib_dir"] = str(pg_distrib_dir)

        with config_path.open("w") as f:
            toml.dump(pageserver_config, f)

    # neon_local config doesn't have to be backward compatible.  If we're using a dump from before
    # it supported multiple pageservers, fix it up.
    if "pageservers" not in snapshot_config:
        snapshot_config["pageservers"] = [snapshot_config["pageserver"]]
        del snapshot_config["pageserver"]

    for param in ("listen_http_addr", "listen_pg_addr"):
        for pageserver in snapshot_config["pageservers"]:
            pageserver[param] = port_distributor.replace_with_new_port(pageserver[param])
    snapshot_config["broker"]["listen_addr"] = port_distributor.replace_with_new_port(
        snapshot_config["broker"]["listen_addr"]
    )
    for sk in snapshot_config["safekeepers"]:
        for param in ("http_port", "pg_port", "pg_tenant_only_port"):
            sk[param] = port_distributor.replace_with_new_port(sk[param])

    if pg_distrib_dir:
        snapshot_config["pg_distrib_dir"] = str(pg_distrib_dir)

    with snapshot_config_toml.open("w") as f:
        toml.dump(snapshot_config, f)

    # Ensure that snapshot doesn't contain references to the original path
    rv = subprocess.run(
        [
            "grep",
            "--recursive",
            "--binary-file=without-match",
            "--files-with-matches",
            "test_create_snapshot/repo",
            str(repo_dir),
        ],
        capture_output=True,
        text=True,
    )
    assert (
        rv.returncode != 0
    ), f"there're files referencing `test_create_snapshot/repo`, this path should be replaced with {repo_dir}:\n{rv.stdout}"


def check_neon_works(
    repo_dir: Path,
    neon_target_binpath: Path,
    neon_current_binpath: Path,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    port_distributor: PortDistributor,
    test_output_dir: Path,
    pg_bin: PgBin,
    request: FixtureRequest,
):
    snapshot_config_toml = repo_dir / "config"
    snapshot_config = toml.load(snapshot_config_toml)
    snapshot_config["neon_distrib_dir"] = str(neon_target_binpath)
    snapshot_config["postgres_distrib_dir"] = str(pg_distrib_dir)
    with (snapshot_config_toml).open("w") as f:
        toml.dump(snapshot_config, f)

    # TODO: replace with NeonEnvBuilder / NeonEnv
    config: Any = type("NeonEnvStub", (object,), {})
    config.rust_log_override = None
    config.repo_dir = repo_dir
    config.pg_version = pg_version
    config.initial_tenant = snapshot_config["default_tenant_id"]
    config.pg_distrib_dir = pg_distrib_dir
    config.remote_storage = None
    config.ext_remote_storage = None
    config.sk_remote_storage = None

    # Use the "target" binaries to launch the storage nodes
    config_target = config
    config_target.neon_binpath = neon_target_binpath
    # We are using maybe-old binaries for neon services, but want to use current
    # binaries for test utilities like neon_local
    config_target.neon_local_binpath = neon_current_binpath
    cli_target = NeonCli(config_target)

    # And the current binaries to launch computes
    snapshot_config["neon_distrib_dir"] = str(neon_current_binpath)
    with (snapshot_config_toml).open("w") as f:
        toml.dump(snapshot_config, f)
    config_current = copy.copy(config)
    config_current.neon_binpath = neon_current_binpath
    cli_current = NeonCli(config_current)

    cli_target.raw_cli(["start"])
    request.addfinalizer(lambda: cli_target.raw_cli(["stop"]))

    pg_port = port_distributor.get_port()
    http_port = port_distributor.get_port()
    cli_current.endpoint_start("main", pg_port=pg_port, http_port=http_port)
    request.addfinalizer(lambda: cli_current.endpoint_stop("main"))

    connstr = f"host=127.0.0.1 port={pg_port} user=cloud_admin dbname=postgres"
    pg_bin.run_capture(
        ["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump.sql'}"]
    )
    initial_dump_differs = dump_differs(
        repo_dir.parent / "dump.sql",
        test_output_dir / "dump.sql",
        test_output_dir / "dump.filediff",
    )

    # Check that project can be recovered from WAL
    # loosely based on https://github.com/neondatabase/cloud/wiki/Recovery-from-WAL
    tenant_id = snapshot_config["default_tenant_id"]
    timeline_id = dict(snapshot_config["branch_name_mappings"]["main"])[tenant_id]
    pageserver_port = snapshot_config["pageservers"][0]["listen_http_addr"].split(":")[-1]
    pageserver_http = PageserverHttpClient(
        port=pageserver_port,
        is_testing_enabled_or_skip=lambda: True,  # TODO: check if testing really enabled
    )

    shutil.rmtree(repo_dir / "local_fs_remote_storage")
    timeline_delete_wait_completed(pageserver_http, tenant_id, timeline_id)
    pageserver_http.timeline_create(pg_version, tenant_id, timeline_id)
    pg_bin.run_capture(
        ["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump-from-wal.sql'}"]
    )
    # The assert itself deferred to the end of the test
    # to allow us to perform checks that change data before failing
    dump_from_wal_differs = dump_differs(
        test_output_dir / "dump.sql",
        test_output_dir / "dump-from-wal.sql",
        test_output_dir / "dump-from-wal.filediff",
    )

    pg_bin.run_capture(["pg_amcheck", connstr, "--install-missing", "--verbose"])

    # Check that we can interact with the data
    pg_bin.run_capture(["pgbench", "--time=10", "--progress=2", connstr])

    assert not dump_from_wal_differs, "dump from WAL differs"
    assert not initial_dump_differs, "initial dump differs"


def dump_differs(
    first: Path, second: Path, output: Path, allowed_diffs: Optional[List[str]] = None
) -> bool:
    """
    Runs diff(1) command on two SQL dumps and write the output to the given output file.
    The function supports allowed diffs, if the diff is in the allowed_diffs list, it's not considered as a difference.
    See the example of it in https://github.com/neondatabase/neon/pull/4425/files#diff-15c5bfdd1d5cc1411b9221091511a60dd13a9edf672bdfbb57dd2ef8bb7815d6

    Returns True if the dumps differ and produced diff is not allowed, False otherwise (in most cases we want it to return False).
    """

    with output.open("w") as stdout:
        res = subprocess.run(
            [
                "diff",
                "--unified",  # Make diff output more readable
                "--ignore-matching-lines=^--",  # Ignore changes in comments
                "--ignore-blank-lines",
                str(first),
                str(second),
            ],
            stdout=stdout,
        )

    differs = res.returncode != 0

    allowed_diffs = allowed_diffs or []
    if differs and len(allowed_diffs) > 0:
        for allowed_diff in allowed_diffs:
            with tempfile.NamedTemporaryFile(mode="w") as tmp:
                tmp.write(allowed_diff)
                tmp.flush()

                allowed = subprocess.run(
                    [
                        "diff",
                        "--unified",  # Make diff output more readable
                        r"--ignore-matching-lines=^---",  # Ignore diff headers
                        r"--ignore-matching-lines=^\+\+\+",  # Ignore diff headers
                        "--ignore-matching-lines=^@@",  # Ignore diff blocks location
                        "--ignore-matching-lines=^ *$",  # Ignore lines with only spaces
                        "--ignore-matching-lines=^ --.*",  # Ignore SQL comments in diff
                        "--ignore-blank-lines",
                        str(output),
                        str(tmp.name),
                    ],
                )

                differs = allowed.returncode != 0
                if not differs:
                    break

    return differs
