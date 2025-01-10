from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

import fixtures.utils
import pytest
import toml
from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    flush_ep_to_pageserver,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    timeline_delete_wait_completed,
)
from fixtures.pg_version import PgVersion
from fixtures.remote_storage import RemoteStorageKind, S3Storage, s3_storage
from fixtures.workload import Workload

#
# A test suite that help to prevent unintentionally breaking backward or forward compatibility between Neon releases.
# - `test_create_snapshot` a script wrapped in a test that creates a data snapshot.
# - `test_backward_compatibility` checks that the current version of Neon can start/read/interract with a data snapshot created by the previous version.
#   The path to the snapshot is configured by COMPATIBILITY_SNAPSHOT_DIR environment variable.
#   If the breakage is intentional, the test can be xfaild with setting ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE=true.
# - `test_forward_compatibility` checks that a snapshot created by the current version can be started/read/interracted by the previous version of Neon.
#   Paths to Neon and Postgres are configured by COMPATIBILITY_NEON_BIN and COMPATIBILITY_POSTGRES_DISTRIB_DIR environment variables.
#   If the breakage is intentional, the test can be xfaild with setting ALLOW_FORWARD_COMPATIBILITY_BREAKAGE=true.
#
# The file contains a couple of helper functions:
# - check_neon_works performs the test itself, feel free to add more checks there.
# - dump_differs compares two SQL dumps and writes the diff to a file.
#
#
# How to run `test_backward_compatibility` locally:
#
#    export DEFAULT_PG_VERSION=16
#    export BUILD_TYPE=release
#    export CHECK_ONDISK_DATA_COMPATIBILITY=true
#    export COMPATIBILITY_SNAPSHOT_DIR=test_output/compatibility_snapshot_pgv${DEFAULT_PG_VERSION}
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
#    ./scripts/pytest -k test_backward_compatibility
#
#
# How to run `test_forward_compatibility` locally:
#
#    export DEFAULT_PG_VERSION=16
#    export BUILD_TYPE=release
#    export CHECK_ONDISK_DATA_COMPATIBILITY=true
#    export COMPATIBILITY_NEON_BIN=neon_previous/target/${BUILD_TYPE}
#    export COMPATIBILITY_POSTGRES_DISTRIB_DIR=neon_previous/pg_install
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
#    ./scripts/pytest -k test_forward_compatibility
#
#
# How to run `test_version_mismatch` locally:
#
#    export DEFAULT_PG_VERSION=16
#    export BUILD_TYPE=release
#    export CHECK_ONDISK_DATA_COMPATIBILITY=true
#    export COMPATIBILITY_NEON_BIN=neon_previous/target/${BUILD_TYPE}
#    export COMPATIBILITY_POSTGRES_DISTRIB_DIR=neon_previous/pg_install
#    export NEON_BIN=target/release
#    export POSTGRES_DISTRIB_DIR=pg_install
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
#   # Run the version mismatch test
#    ./scripts/pytest -k test_version_mismatch


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

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            # Miniature layers to enable generating non-trivial layer map without writing lots of data
            "checkpoint_distance": f"{128 * 1024}",
            "compaction_threshold": "1",
            "compaction_target_size": f"{128 * 1024}",
        }
    )
    endpoint = env.endpoints.create_start("main")

    pg_bin.run_capture(["pgbench", "--initialize", "--scale=1", endpoint.connstr()])
    pg_bin.run_capture(["pgbench", "--time=30", "--progress=2", endpoint.connstr()])
    pg_bin.run_capture(
        ["pg_dumpall", f"--dbname={endpoint.connstr()}", f"--file={test_output_dir / 'dump.sql'}"]
    )

    snapshot_config = toml.load(test_output_dir / "repo" / "config")
    tenant_id = snapshot_config["default_tenant_id"]
    timeline_id = dict(snapshot_config["branch_name_mappings"]["main"])[tenant_id]

    pageserver_http = env.pageserver.http_client()

    flush_ep_to_pageserver(env, endpoint, tenant_id, timeline_id)
    pageserver_http.timeline_checkpoint(
        tenant_id, timeline_id, wait_until_uploaded=True, force_image_layer_creation=True
    )

    env.endpoints.stop_all()
    for sk in env.safekeepers:
        sk.stop()
    env.pageserver.stop()
    env.storage_controller.stop()

    # Directory `compatibility_snapshot_dir` is uploaded to S3 in a workflow, keep the name in sync with it
    compatibility_snapshot_dir = (
        top_output_dir / f"compatibility_snapshot_pg{pg_version.v_prefixed}"
    )
    if compatibility_snapshot_dir.exists():
        shutil.rmtree(compatibility_snapshot_dir)

    shutil.copytree(
        test_output_dir,
        compatibility_snapshot_dir,
        ignore=shutil.ignore_patterns("pg_dynshmem"),
    )


# check_neon_works does recovery from WAL => the compatibility snapshot's WAL is old => will log this warning
ingest_lag_log_line = ".*ingesting record with timestamp lagging more than wait_lsn_timeout.*"


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(after="test_create_snapshot")
def test_backward_compatibility(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_version: PgVersion,
    compatibility_snapshot_dir: Path,
):
    """
    Test that the new binaries can read old data
    """
    breaking_changes_allowed = (
        os.environ.get("ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE", "false").lower() == "true"
    )

    try:
        neon_env_builder.num_safekeepers = 3
        env = neon_env_builder.from_repo_dir(compatibility_snapshot_dir / "repo")
        env.pageserver.allowed_errors.append(ingest_lag_log_line)
        env.start()

        check_neon_works(
            env,
            test_output_dir=test_output_dir,
            sql_dump_path=compatibility_snapshot_dir / "dump.sql",
            repo_dir=env.repo_dir,
        )

        env.pageserver.assert_log_contains(ingest_lag_log_line)

    except Exception:
        if breaking_changes_allowed:
            pytest.xfail(
                "Breaking changes are allowed by ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE env var"
            )
        else:
            raise

    assert not breaking_changes_allowed, "Breaking changes are allowed by ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE, but the test has passed without any breakage"


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(after="test_create_snapshot")
def test_forward_compatibility(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    top_output_dir: Path,
    pg_version: PgVersion,
    compatibility_snapshot_dir: Path,
):
    """
    Test that the old binaries can read new data
    """
    breaking_changes_allowed = (
        os.environ.get("ALLOW_FORWARD_COMPATIBILITY_BREAKAGE", "false").lower() == "true"
    )

    try:
        neon_env_builder.num_safekeepers = 3

        # Use previous version's production binaries (pageserver, safekeeper, pg_distrib_dir, etc.).
        # But always use the current version's neon_local binary.
        # This is because we want to test the compatibility of the data format, not the compatibility of the neon_local CLI.
        assert (
            neon_env_builder.compatibility_neon_binpath is not None
        ), "the environment variable COMPATIBILITY_NEON_BIN is required"
        assert (
            neon_env_builder.compatibility_pg_distrib_dir is not None
        ), "the environment variable COMPATIBILITY_POSTGRES_DISTRIB_DIR is required"
        neon_env_builder.neon_binpath = neon_env_builder.compatibility_neon_binpath
        neon_env_builder.pg_distrib_dir = neon_env_builder.compatibility_pg_distrib_dir

        env = neon_env_builder.from_repo_dir(
            compatibility_snapshot_dir / "repo",
        )
        # there may be an arbitrary number of unrelated tests run between create_snapshot and here
        env.pageserver.allowed_errors.append(ingest_lag_log_line)

        # not using env.pageserver.version because it was initialized before
        prev_pageserver_version_str = env.get_binary_version("pageserver")
        prev_pageserver_version_match = re.search(
            "Neon page server git(?:-env)?:(.*) failpoints: (.*), features: (.*)",
            prev_pageserver_version_str,
        )
        if prev_pageserver_version_match is not None:
            prev_pageserver_version = prev_pageserver_version_match.group(1)
        else:
            raise AssertionError(
                "cannot find git hash in the version string: " + prev_pageserver_version_str
            )

        # does not include logs from previous runs
        assert not env.pageserver.log_contains(f"git(-env)?:{prev_pageserver_version}")

        env.start()

        # ensure the specified pageserver is running
        assert env.pageserver.log_contains(f"git(-env)?:{prev_pageserver_version}")

        check_neon_works(
            env,
            test_output_dir=test_output_dir,
            sql_dump_path=compatibility_snapshot_dir / "dump.sql",
            repo_dir=env.repo_dir,
        )

    except Exception:
        if breaking_changes_allowed:
            pytest.xfail(
                "Breaking changes are allowed by ALLOW_FORWARD_COMPATIBILITY_BREAKAGE env var"
            )
        else:
            raise

    assert not breaking_changes_allowed, "Breaking changes are allowed by ALLOW_FORWARD_COMPATIBILITY_BREAKAGE, but the test has passed without any breakage"


def check_neon_works(env: NeonEnv, test_output_dir: Path, sql_dump_path: Path, repo_dir: Path):
    ep = env.endpoints.create_start("main")
    connstr = ep.connstr()

    pg_bin = PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version)

    pg_bin.run_capture(
        ["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump.sql'}"]
    )
    initial_dump_differs = dump_differs(
        sql_dump_path,
        test_output_dir / "dump.sql",
        test_output_dir / "dump.filediff",
    )

    # Check that project can be recovered from WAL
    # loosely based on https://www.notion.so/neondatabase/Storage-Recovery-from-WAL-d92c0aac0ebf40df892b938045d7d720
    pageserver_http = env.pageserver.http_client()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    pg_version = env.pg_version

    # Stop endpoint while we recreate timeline
    flush_ep_to_pageserver(env, ep, tenant_id, timeline_id)

    try:
        pageserver_http.timeline_preserve_initdb_archive(tenant_id, timeline_id)
    except PageserverApiException as e:
        # Allow the error as we might be running the old pageserver binary
        log.info(f"Got allowed error: '{e}'")

    # Delete all files from local_fs_remote_storage except initdb-preserved.tar.zst,
    # the file is required for `timeline_create` with `existing_initdb_timeline_id`.
    #
    # TODO: switch to Path.walk() in Python 3.12
    # for dirpath, _dirnames, filenames in (repo_dir / "local_fs_remote_storage").walk():
    for dirpath, _dirnames, filenames in os.walk(repo_dir / "local_fs_remote_storage"):
        for filename in filenames:
            if filename != "initdb-preserved.tar.zst" and filename != "initdb.tar.zst":
                (Path(dirpath) / filename).unlink()

    timeline_delete_wait_completed(pageserver_http, tenant_id, timeline_id)
    pageserver_http.timeline_create(
        pg_version=pg_version,
        tenant_id=tenant_id,
        new_timeline_id=timeline_id,
        existing_initdb_timeline_id=timeline_id,
    )

    # Timeline exists again: restart the endpoint
    ep.start()

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

    # Check that we can interract with the data
    pg_bin.run_capture(["pgbench", "--time=10", "--progress=2", connstr])

    assert not dump_from_wal_differs, "dump from WAL differs"
    assert not initial_dump_differs, "initial dump differs"

    flush_ep_to_pageserver(env, ep, tenant_id, timeline_id)
    pageserver_http.timeline_checkpoint(
        tenant_id, timeline_id, compact=False, wait_until_uploaded=True
    )


def dump_differs(
    first: Path, second: Path, output: Path, allowed_diffs: list[str] | None = None
) -> bool:
    """
    Runs diff(1) command on two SQL dumps and write the output to the given output file.
    The function supports allowed diffs, if the diff is in the allowed_diffs list, it's not considered as a difference.
    See the example of it in https://github.com/neondatabase/neon/pull/4425/files#diff-15c5bfdd1d5cc1411b9221091511a60dd13a9edf672bdfbb57dd2ef8bb7815d6

    Returns True if the dumps differ and produced diff is not allowed, False otherwise (in most cases we want it to return False).
    """

    if not first.exists():
        raise FileNotFoundError(f"{first} doesn't exist")
    if not second.exists():
        raise FileNotFoundError(f"{second} doesn't exist")

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


@dataclass
class HistoricDataSet:
    name: str
    tenant_id: TenantId
    pg_version: PgVersion
    url: str

    def __str__(self):
        return self.name


HISTORIC_DATA_SETS = [
    # From before we enabled image layer compression.
    # - IndexPart::LATEST_VERSION 7
    # - STORAGE_FORMAT_VERSION 3
    HistoricDataSet(
        "2024-07-18",
        TenantId("17bf64a53509714687664b3a84e9b3ba"),
        PgVersion.V16,
        "https://neon-github-public-dev.s3.eu-central-1.amazonaws.com/compatibility-data-snapshots/2024-07-18-pgv16.tar.zst",
    ),
]


@pytest.mark.parametrize("dataset", HISTORIC_DATA_SETS)
@pytest.mark.xdist_group("compatibility")
def test_historic_storage_formats(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_version: PgVersion,
    dataset: HistoricDataSet,
):
    """
    This test is like test_backward_compatibility, but it looks back further to examples of our storage format from long ago.
    """

    ARTIFACT_CACHE_DIR = "./artifact_cache"

    import tarfile
    from contextlib import closing

    import requests
    import zstandard

    artifact_unpack_path = ARTIFACT_CACHE_DIR / Path("unpacked") / Path(dataset.name)

    # Note: we assume that when running across a matrix of PG versions, the matrix includes all the versions needed by
    # HISTORIC_DATA_SETS. If we ever remove a PG version from the matrix, then historic datasets built using that version
    # will no longer be covered by this test.
    if pg_version != dataset.pg_version:
        pytest.skip(f"Dataset {dataset} is for different PG version, skipping")

    with closing(requests.get(dataset.url, stream=True)) as r:
        unzstd = zstandard.ZstdDecompressor()
        with unzstd.stream_reader(r.raw) as stream:
            with tarfile.open(mode="r|", fileobj=stream) as tf:
                tf.extractall(artifact_unpack_path)

    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.pg_version = dataset.pg_version
    env = neon_env_builder.init_configs()
    env.start()
    assert isinstance(env.pageserver_remote_storage, S3Storage)

    # Link artifact data into test's remote storage.  We don't want the whole repo dir, just the remote storage part: we are not testing
    # compat of local disk data across releases (test_backward_compat does that), we're testing really long-lived data in S3 like layer files and indices.
    #
    # The code generating the snapshot uses local_fs, but this test uses S3Storage, so we are copying a tree of files into a bucket.  We use
    # S3Storage so that the scrubber can run (the scrubber doesn't speak local_fs)
    artifact_pageserver_path = (
        artifact_unpack_path / Path("repo") / Path("local_fs_remote_storage") / Path("pageserver")
    )
    for root, _dirs, files in os.walk(artifact_pageserver_path):
        for file in files:
            local_path = os.path.join(root, file)
            remote_key = (
                env.pageserver_remote_storage.prefix_in_bucket
                + str(local_path)[len(str(artifact_pageserver_path)) :]
            )
            log.info(f"Uploading {local_path} -> {remote_key}")
            env.pageserver_remote_storage.client.upload_file(
                local_path, env.pageserver_remote_storage.bucket_name, remote_key
            )

    # Check the scrubber handles this old data correctly (can read it and doesn't consider it corrupt)
    #
    # Do this _before_ importing to the pageserver, as that import may start writing immediately
    healthy, metadata_summary = env.storage_scrubber.scan_metadata()
    assert healthy
    assert metadata_summary["tenant_count"] >= 1
    assert metadata_summary["timeline_count"] >= 1

    env.neon_cli.tenant_import(dataset.tenant_id)

    # Discover timelines
    timelines = env.pageserver.http_client().timeline_list(dataset.tenant_id)
    # All our artifacts should contain at least one timeline
    assert len(timelines) > 0

    # TODO: ensure that the snapshots we're importing contain a sensible variety of content, at the very
    # least they should include a mixture of deltas and image layers.  Preferably they should also
    # contain some "exotic" stuff like aux files from logical replication.

    # Check we can start an endpoint and read the SQL that the artifact is meant to contain
    reference_sql_dump = artifact_unpack_path / Path("dump.sql")
    ep = env.endpoints.create_start("main", tenant_id=dataset.tenant_id)
    pg_bin = PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version)
    pg_bin.run_capture(
        ["pg_dumpall", f"--dbname={ep.connstr()}", f"--file={test_output_dir / 'dump.sql'}"]
    )
    assert not dump_differs(
        reference_sql_dump,
        test_output_dir / "dump.sql",
        test_output_dir / "dump.filediff",
    )
    ep.stop()

    # Check we can also do writes to the database
    existing_timeline_id = TimelineId(timelines[0]["timeline_id"])
    workload = Workload(env, dataset.tenant_id, existing_timeline_id)
    workload.init()
    workload.write_rows(100)

    # Check that compaction works
    env.pageserver.http_client().timeline_compact(
        dataset.tenant_id, existing_timeline_id, force_image_layer_creation=True
    )


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.parametrize(**fixtures.utils.allpairs_versions())
def test_versions_mismatch(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_version: PgVersion,
    compatibility_snapshot_dir,
    combination,
):
    """
    Checks compatibility of different combinations of versions of the components
    """
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.from_repo_dir(
        compatibility_snapshot_dir / "repo",
    )
    env.pageserver.allowed_errors.extend(
        [".*ingesting record with timestamp lagging more than wait_lsn_timeout.+"]
    )
    env.start()
    check_neon_works(
        env, test_output_dir, compatibility_snapshot_dir / "dump.sql", test_output_dir / "repo"
    )
