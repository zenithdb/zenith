import os
import shutil
import threading
from contextlib import closing
from pathlib import Path

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pg_version import PgVersion, skip_on_postgres
from fixtures.remote_storage import (
    RemoteStorageKind,
    S3Storage,
    available_s3_storages,
)


# Cleaning up downloaded files is important for local tests
# or else one test could reuse the files from another test or another test run
def cleanup(pg_version):
    PGDIR = Path(f"pg_install/v{pg_version}")

    LIB_DIR = PGDIR / Path("lib/postgresql")
    cleanup_lib_globs = ["anon*", "postgis*", "pg_buffercache*"]
    cleanup_lib_glob_paths = [LIB_DIR.glob(x) for x in cleanup_lib_globs]

    SHARE_DIR = PGDIR / Path("share/postgresql/extension")
    cleanup_ext_globs = [
        "anon*",
        "address_standardizer*",
        "postgis*",
        "pageinspect*",
        "pg_buffercache*",
        "pgrouting*",
    ]
    cleanup_ext_glob_paths = [SHARE_DIR.glob(x) for x in cleanup_ext_globs]

    all_glob_paths = cleanup_lib_glob_paths + cleanup_ext_glob_paths
    all_cleanup_files = []
    for file_glob in all_glob_paths:
        for file in file_glob:
            all_cleanup_files.append(file)

    for file in all_cleanup_files:
        try:
            os.remove(file)
            log.info(f"removed file {file}")
        except Exception as err:
            log.info(
                f"skipping remove of file {file} because it doesn't exist.\
                      this may be expected or unexpected depending on the test {err}"
            )

    cleanup_folders = [SHARE_DIR / Path("anon"), PGDIR / Path("download_extensions")]
    for folder in cleanup_folders:
        try:
            shutil.rmtree(folder)
            log.info(f"removed folder {folder}")
        except Exception as err:
            log.info(
                f"skipping remove of folder {folder} because it doesn't exist.\
                      this may be expected or unexpected depending on the test {err}"
            )


def upload_files(env):
    log.info("Uploading test files to mock bucket")
    os.chdir("test_runner/regress/data/extension_test")
    for path in os.walk("."):
        prefix, _, files = path
        for file in files:
            # the [2:] is to remove the leading "./"
            full_path = os.path.join(prefix, file)[2:]

            with open(full_path, "rb") as f:
                log.info(f"UPLOAD {full_path} to ext/{full_path}")
                assert isinstance(env.pageserver_remote_storage, S3Storage)
                env.pageserver_remote_storage.client.upload_fileobj(
                    f,
                    env.ext_remote_storage.bucket_name,
                    f"ext/{full_path}",
                )
    os.chdir("../../../..")


# Test downloading remote extension.
@skip_on_postgres(PgVersion.V16, reason="TODO: PG16 extension building")
@pytest.mark.parametrize("remote_storage_kind", available_s3_storages())
@pytest.mark.skip(reason="https://github.com/neondatabase/neon/issues/4949")
def test_remote_extensions(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_version: PgVersion,
):
    neon_env_builder.enable_extensions_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_extensions", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy

    # For MOCK_S3 we upload test files.
    # For REAL_S3 we use the files already in the bucket
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        upload_files(env)

    # Start a compute node and check that it can download the extensions
    # and use them to CREATE EXTENSION and LOAD
    endpoint = env.endpoints.create_start(
        "test_remote_extensions",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        # config_lines=["log_min_messages=debug3"],
    )
    try:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # Check that appropriate control files were downloaded
                cur.execute("SELECT * FROM pg_available_extensions")
                all_extensions = [x[0] for x in cur.fetchall()]
                log.info(all_extensions)
                assert "anon" in all_extensions

                # postgis is on real s3 but not mock s3.
                # it's kind of a big file, would rather not upload to github
                if remote_storage_kind == RemoteStorageKind.REAL_S3:
                    assert "postgis" in all_extensions
                    # this may fail locally if dependency is missing
                    # we don't really care about the error,
                    # we just want to make sure it downloaded
                    try:
                        cur.execute("CREATE EXTENSION postgis")
                    except Exception as err:
                        log.info(f"(expected) error creating postgis extension: {err}")
                        # we do not check the error, so this is basically a NO-OP
                        # however checking the log you can make sure that it worked
                        # and also get valuable information about how long loading the extension took

                # this is expected to fail on my computer because I don't have the pgcrypto extension
                try:
                    cur.execute("CREATE EXTENSION anon")
                except Exception as err:
                    log.info("error creating anon extension")
                    assert "pgcrypto" in str(err), "unexpected error creating anon extension"
    finally:
        cleanup(pg_version)


# Test downloading remote library.
@skip_on_postgres(PgVersion.V16, reason="TODO: PG16 extension building")
@pytest.mark.parametrize("remote_storage_kind", available_s3_storages())
@pytest.mark.skip(reason="https://github.com/neondatabase/neon/issues/4949")
def test_remote_library(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_version: PgVersion,
):
    neon_env_builder.enable_extensions_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_library", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy

    # For MOCK_S3 we upload test files.
    # For REAL_S3 we use the files already in the bucket
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        upload_files(env)

    # and use them to run LOAD library
    endpoint = env.endpoints.create_start(
        "test_remote_library",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        # config_lines=["log_min_messages=debug3"],
    )
    try:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # try to load library
                try:
                    cur.execute("LOAD 'anon'")
                except Exception as err:
                    log.info(f"error loading anon library: {err}")
                    raise AssertionError("unexpected error loading anon library") from err

                # test library which name is different from extension name
                # this may fail locally if dependency is missing
                # however, it does successfully download the postgis archive
                if remote_storage_kind == RemoteStorageKind.REAL_S3:
                    try:
                        cur.execute("LOAD 'postgis_topology-3'")
                    except Exception as err:
                        log.info("error loading postgis_topology-3")
                        assert "No such file or directory" in str(
                            err
                        ), "unexpected error loading postgis_topology-3"
    finally:
        cleanup(pg_version)


# Here we test a complex extension
# which has multiple extensions in one archive
# using postgis as an example
# @pytest.mark.skipif(
#    RemoteStorageKind.REAL_S3 not in available_s3_storages(),
#    reason="skipping test because real s3 not enabled",
# )
@skip_on_postgres(PgVersion.V16, reason="TODO: PG16 extension building")
@pytest.mark.skip(reason="https://github.com/neondatabase/neon/issues/4949")
def test_multiple_extensions_one_archive(
    neon_env_builder: NeonEnvBuilder,
    pg_version: PgVersion,
):
    neon_env_builder.enable_extensions_remote_storage(RemoteStorageKind.REAL_S3)
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_multiple_extensions_one_archive", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy

    endpoint = env.endpoints.create_start(
        "test_multiple_extensions_one_archive",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
    )
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION address_standardizer;")
            cur.execute("CREATE EXTENSION address_standardizer_data_us;")
            # execute query to ensure that it works
            cur.execute(
                "SELECT house_num, name, suftype, city, country, state, unit \
                        FROM standardize_address('us_lex', 'us_gaz', 'us_rules', \
                        'One Rust Place, Boston, MA 02109');"
            )
            res = cur.fetchall()
            log.info(res)
            assert len(res) > 0

    cleanup(pg_version)


# Test that extension is downloaded after endpoint restart,
# when the library is used in the query.
#
# Run the test with multiple simultaneous connections to an endpoint.
# to ensure that the extension is downloaded only once.
#
@pytest.mark.skip(reason="https://github.com/neondatabase/neon/issues/4949")
def test_extension_download_after_restart(
    neon_env_builder: NeonEnvBuilder,
    pg_version: PgVersion,
):
    # TODO: PG15 + PG16 extension building
    if "v14" not in pg_version:  # test set only has extension built for v14
        return None

    neon_env_builder.enable_extensions_remote_storage(RemoteStorageKind.MOCK_S3)
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_extension_download_after_restart", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy

    # For MOCK_S3 we upload test files.
    upload_files(env)

    endpoint = env.endpoints.create_start(
        "test_extension_download_after_restart",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        config_lines=["log_min_messages=debug3"],
    )
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE extension pg_buffercache;")
            cur.execute("SELECT * from pg_buffercache;")
            res = cur.fetchall()
            assert len(res) > 0
            log.info(res)

    # shutdown compute node
    endpoint.stop()
    # remove extension files locally
    cleanup(pg_version)

    # spin up compute node again (there are no extension files available, because compute is stateless)
    endpoint = env.endpoints.create_start(
        "test_extension_download_after_restart",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        config_lines=["log_min_messages=debug3"],
    )

    # connect to compute node and run the query
    # that will trigger the download of the extension
    def run_query(endpoint, thread_id: int):
        log.info("thread_id {%d} starting", thread_id)
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * from pg_buffercache;")
                res = cur.fetchall()
                assert len(res) > 0
                log.info("thread_id {%d}, res = %s", thread_id, res)

    threads = [threading.Thread(target=run_query, args=(endpoint, i)) for i in range(2)]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    cleanup(pg_version)
