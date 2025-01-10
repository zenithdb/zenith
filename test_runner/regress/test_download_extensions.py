from __future__ import annotations

import os
import shutil
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pg_version import PgVersion
from fixtures.utils import skip_on_postgres
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

if TYPE_CHECKING:
    from typing import Any

    from fixtures.httpserver import ListenAddress


# use neon_env_builder_local fixture to override the default neon_env_builder fixture
# and use a test-specific pg_install instead of shared one
@pytest.fixture(scope="function")
def neon_env_builder_local(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
) -> NeonEnvBuilder:
    test_local_pginstall = test_output_dir / "pg_install"
    log.info(f"copy {pg_distrib_dir} to {test_local_pginstall}")
    shutil.copytree(
        pg_distrib_dir / pg_version.v_prefixed, test_local_pginstall / pg_version.v_prefixed
    )

    neon_env_builder.pg_distrib_dir = test_local_pginstall
    log.info(f"local neon_env_builder.pg_distrib_dir: {neon_env_builder.pg_distrib_dir}")

    return neon_env_builder


@skip_on_postgres(PgVersion.V16, reason="TODO: PG16 extension building")
@skip_on_postgres(PgVersion.V17, reason="TODO: PG17 extension building")
def test_remote_extensions(
    httpserver: HTTPServer,
    neon_env_builder_local: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
    pg_version: PgVersion,
):
    # setup mock http server
    # that expects request for anon.tar.zst
    # and returns the requested file
    (host, port) = httpserver_listen_address
    extensions_endpoint = f"http://{host}:{port}/pg-ext-s3-gateway"

    build_tag = os.environ.get("BUILD_TAG", "latest")
    archive_path = f"{build_tag}/v{pg_version}/extensions/anon.tar.zst"

    def endpoint_handler_build_tag(request: Request) -> Response:
        log.info(f"request: {request}")

        file_name = "anon.tar.zst"
        file_path = f"test_runner/regress/data/extension_test/5670669815/v{pg_version}/extensions/anon.tar.zst"
        file_size = os.path.getsize(file_path)
        fh = open(file_path, "rb")

        return Response(
            fh,
            mimetype="application/octet-stream",
            headers=[
                ("Content-Length", str(file_size)),
                ("Content-Disposition", f'attachment; filename="{file_name}"'),
            ],
            direct_passthrough=True,
        )

    httpserver.expect_request(
        f"/pg-ext-s3-gateway/{archive_path}", method="GET"
    ).respond_with_handler(endpoint_handler_build_tag)

    # Start a compute node with remote_extension spec
    # and check that it can download the extensions and use them to CREATE EXTENSION.
    env = neon_env_builder_local.init_start()
    env.create_branch("test_remote_extensions")
    endpoint = env.endpoints.create(
        "test_remote_extensions",
        config_lines=["log_min_messages=debug3"],
    )

    # mock remote_extensions spec
    spec: dict[str, Any] = {
        "library_index": {
            "anon": "anon",
        },
        "extension_data": {
            "anon": {
                "archive_path": "",
                "control_data": {
                    "anon.control": "# PostgreSQL Anonymizer (anon) extension\ncomment = 'Data anonymization tools'\ndefault_version = '1.1.0'\ndirectory='extension/anon'\nrelocatable = false\nrequires = 'pgcrypto'\nsuperuser = false\nmodule_pathname = '$libdir/anon'\ntrusted = true\n"
                },
            },
        },
    }
    spec["extension_data"]["anon"]["archive_path"] = archive_path

    endpoint.create_remote_extension_spec(spec)

    endpoint.start(
        remote_ext_config=extensions_endpoint,
    )

    # this is expected to fail if there's no pgcrypto extension, that's ok
    # we just want to check that the extension was downloaded
    try:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # Check that appropriate files were downloaded
                cur.execute("CREATE EXTENSION anon")
                res = [x[0] for x in cur.fetchall()]
                log.info(res)
    except Exception as err:
        assert "pgcrypto" in str(err), f"unexpected error creating anon extension {err}"

    httpserver.check()


# TODO
# 1. Test downloading remote library.
#
# 2. Test a complex extension, which has multiple extensions in one archive
# using postgis as an example
#
# 3.Test that extension is downloaded after endpoint restart,
# when the library is used in the query.
# Run the test with mutliple simultaneous connections to an endpoint.
# to ensure that the extension is downloaded only once.
#
# 4. Test that private extensions are only downloaded when they are present in the spec.
#
