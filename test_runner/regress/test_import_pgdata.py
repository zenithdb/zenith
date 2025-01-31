import base64
import json
import re
import time
from enum import Enum
from pathlib import Path

import psycopg2
import psycopg2.errors
import pytest
from fixtures.common_types import Lsn, TenantId, TenantShardId, TimelineId
from fixtures.fast_import import FastImport
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, PgProtocol, VanillaPostgres
from fixtures.pageserver.http import (
    ImportPgdataIdemptencyKey,
    PageserverApiException,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import MockS3Server, RemoteStorageKind
from mypy_boto3_kms import KMSClient
from mypy_boto3_kms.type_defs import EncryptResponseTypeDef
from mypy_boto3_s3 import S3Client
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

num_rows = 1000


class RelBlockSize(Enum):
    ONE_STRIPE_SIZE = 1
    TWO_STRPES_PER_SHARD = 2
    MULTIPLE_RELATION_SEGMENTS = 3


smoke_params = [
    # unsharded (the stripe size needs to be given for rel block size calculations)
    *[(None, 1024, s) for s in RelBlockSize],
    # many shards, small stripe size to speed up test
    *[(8, 1024, s) for s in RelBlockSize],
]


@pytest.mark.parametrize("shard_count,stripe_size,rel_block_size", smoke_params)
def test_pgdata_import_smoke(
    vanilla_pg: VanillaPostgres,
    neon_env_builder: NeonEnvBuilder,
    shard_count: int | None,
    stripe_size: int,
    rel_block_size: RelBlockSize,
    make_httpserver: HTTPServer,
):
    #
    # Setup fake control plane for import progress
    #
    def handler(request: Request) -> Response:
        log.info(f"control plane request: {request.json}")
        return Response(json.dumps({}), status=200)

    cplane_mgmt_api_server = make_httpserver
    cplane_mgmt_api_server.expect_request(re.compile(".*")).respond_with_handler(handler)

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    # The test needs LocalFs support, which is only built in testing mode.
    env.pageserver.is_testing_enabled_or_skip()

    env.pageserver.patch_config_toml_nonrecursive(
        {
            "import_pgdata_upcall_api": f"http://{cplane_mgmt_api_server.host}:{cplane_mgmt_api_server.port}/path/to/mgmt/api"
        }
    )
    env.pageserver.stop()
    env.pageserver.start()

    # By default our tests run with a tiny shared_buffers=1MB setting. That
    # doesn't allow any prefetching on v17 and above, where the new streaming
    # read machinery keeps buffers pinned while prefetching them.  Use a higher
    # setting to enable prefetching and speed up the tests
    ep_config = ["shared_buffers=64MB"]

    #
    # Put data in vanilla pg
    #

    vanilla_pg.start()
    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")

    log.info("create relblock data")
    if rel_block_size == RelBlockSize.ONE_STRIPE_SIZE:
        target_relblock_size = stripe_size * 8192
    elif rel_block_size == RelBlockSize.TWO_STRPES_PER_SHARD:
        target_relblock_size = (shard_count or 1) * stripe_size * 8192 * 2
    elif rel_block_size == RelBlockSize.MULTIPLE_RELATION_SEGMENTS:
        # Postgres uses a 1GiB segment size, fixed at compile time, so we must use >2GB of data
        # to exercise multiple segments.
        target_relblock_size = int(((2.333 * 1024 * 1024 * 1024) // 8192) * 8192)
    else:
        raise ValueError

    # fillfactor so we don't need to produce that much data
    # 900 byte per row is > 10% => 1 row per page
    vanilla_pg.safe_psql("""create table t (data char(900)) with (fillfactor = 10)""")

    nrows = 0
    while True:
        relblock_size = vanilla_pg.safe_psql_scalar("select pg_relation_size('t')")
        log.info(
            f"relblock size: {relblock_size / 8192} pages (target: {target_relblock_size // 8192}) pages"
        )
        if relblock_size >= target_relblock_size:
            break
        addrows = int((target_relblock_size - relblock_size) // 8192)
        assert addrows >= 1, "forward progress"
        vanilla_pg.safe_psql(
            f"insert into t select generate_series({nrows + 1}, {nrows + addrows})"
        )
        nrows += addrows
    expect_nrows = nrows
    expect_sum = (
        (nrows) * (nrows + 1) // 2
    )  # https://stackoverflow.com/questions/43901484/sum-of-the-integers-from-1-to-n

    def validate_vanilla_equivalence(ep):
        # TODO: would be nicer to just compare pgdump

        # Enable IO concurrency for batching on large sequential scan, to avoid making
        # this test unnecessarily onerous on CPU. Especially on debug mode, it's still
        # pretty onerous though, so increase statement_timeout to avoid timeouts.
        assert ep.safe_psql_many(
            [
                "set effective_io_concurrency=32;",
                "SET statement_timeout='300s';",
                "select count(*), sum(data::bigint)::bigint from t",
            ]
        ) == [[], [], [(expect_nrows, expect_sum)]]

    validate_vanilla_equivalence(vanilla_pg)

    vanilla_pg.stop()

    #
    # We have a Postgres data directory now.
    # Make a localfs remote storage that looks like how after `fast_import` ran.
    # TODO: actually exercise fast_import here
    # TODO: test s3 remote storage
    #
    importbucket = neon_env_builder.repo_dir / "importbucket"
    importbucket.mkdir()
    # what cplane writes before scheduling fast_import
    specpath = importbucket / "spec.json"
    specpath.write_text(json.dumps({"branch_id": "somebranch", "project_id": "someproject"}))
    # what fast_import writes
    vanilla_pg.pgdatadir.rename(importbucket / "pgdata")
    statusdir = importbucket / "status"
    statusdir.mkdir()
    (statusdir / "pgdata").write_text(json.dumps({"done": True}))

    #
    # Do the import
    #

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        tenant_id, shard_count=shard_count, shard_stripe_size=stripe_size
    )

    timeline_id = TimelineId.generate()
    log.info("starting import")
    start = time.monotonic()

    idempotency = ImportPgdataIdemptencyKey.random()
    log.info(f"idempotency key {idempotency}")
    # TODO: teach neon_local CLI about the idempotency & 429 error so we can run inside the loop
    # and check for 429

    import_branch_name = "imported"
    env.storage_controller.timeline_create(
        tenant_id,
        {
            "new_timeline_id": str(timeline_id),
            "import_pgdata": {
                "idempotency_key": str(idempotency),
                "location": {"LocalFs": {"path": str(importbucket.absolute())}},
            },
        },
    )
    env.neon_cli.mappings_map_branch(import_branch_name, tenant_id, timeline_id)

    while True:
        locations = env.storage_controller.locate(tenant_id)
        active_count = 0
        for location in locations:
            shard_id = TenantShardId.parse(location["shard_id"])
            ps = env.get_pageserver(location["node_id"])
            try:
                detail = ps.http_client().timeline_detail(shard_id, timeline_id)
                state = detail["state"]
                log.info(f"shard {shard_id} state: {state}")
                if state == "Active":
                    active_count += 1
            except PageserverApiException as e:
                if e.status_code == 404:
                    log.info("not found, import is in progress")
                    continue
                elif e.status_code == 429:
                    log.info("import is in progress")
                    continue
                else:
                    raise

            shard_status_file = statusdir / f"shard-{shard_id.shard_index}"
            if state == "Active":
                shard_status_file_contents = (
                    shard_status_file.read_text()
                )  # Active state implies import is done
                shard_status = json.loads(shard_status_file_contents)
                assert shard_status["done"] is True

        if active_count == len(locations):
            log.info("all shards are active")
            break
        time.sleep(1)

    import_duration = time.monotonic() - start
    log.info(f"import complete; duration={import_duration:.2f}s")

    #
    # Get some timeline details for later.
    #
    locations = env.storage_controller.locate(tenant_id)
    [shard_zero] = [
        loc for loc in locations if TenantShardId.parse(loc["shard_id"]).shard_number == 0
    ]
    shard_zero_ps = env.get_pageserver(shard_zero["node_id"])
    shard_zero_http = shard_zero_ps.http_client()
    shard_zero_timeline_info = shard_zero_http.timeline_detail(shard_zero["shard_id"], timeline_id)
    initdb_lsn = Lsn(shard_zero_timeline_info["initdb_lsn"])
    latest_gc_cutoff_lsn = Lsn(shard_zero_timeline_info["latest_gc_cutoff_lsn"])
    last_record_lsn = Lsn(shard_zero_timeline_info["last_record_lsn"])
    disk_consistent_lsn = Lsn(shard_zero_timeline_info["disk_consistent_lsn"])
    _remote_consistent_lsn = Lsn(shard_zero_timeline_info["remote_consistent_lsn"])
    remote_consistent_lsn_visible = Lsn(shard_zero_timeline_info["remote_consistent_lsn_visible"])
    # assert remote_consistent_lsn_visible == remote_consistent_lsn TODO: this fails initially and after restart, presumably because `UploadQueue::clean.1` is still `None`
    assert remote_consistent_lsn_visible == disk_consistent_lsn
    assert initdb_lsn == latest_gc_cutoff_lsn
    assert disk_consistent_lsn == initdb_lsn + 8
    assert last_record_lsn == disk_consistent_lsn
    # TODO: assert these values are the same everywhere

    #
    # Validate the resulting remote storage state.
    #

    #
    # Validate the imported data
    #

    ro_endpoint = env.endpoints.create_start(
        branch_name=import_branch_name,
        endpoint_id="ro",
        tenant_id=tenant_id,
        lsn=last_record_lsn,
        config_lines=ep_config,
    )

    validate_vanilla_equivalence(ro_endpoint)

    # ensure the import survives restarts
    ro_endpoint.stop()
    env.pageserver.stop(immediate=True)
    env.pageserver.start()
    ro_endpoint.start()
    validate_vanilla_equivalence(ro_endpoint)

    #
    # validate the layer files in each shard only have the shard-specific data
    # (the implementation would be functional but not efficient without this characteristic)
    #

    shards = env.storage_controller.locate(tenant_id)
    for shard in shards:
        shard_ps = env.get_pageserver(shard["node_id"])
        result = shard_ps.timeline_scan_no_disposable_keys(shard["shard_id"], timeline_id)
        assert result.tally.disposable_count == 0
        assert (
            result.tally.not_disposable_count > 0
        ), "sanity check, each shard should have some data"

    #
    # validate that we can write
    #
    rw_endpoint = env.endpoints.create_start(
        branch_name=import_branch_name,
        endpoint_id="rw",
        tenant_id=tenant_id,
        config_lines=ep_config,
    )
    rw_endpoint.safe_psql("create table othertable(values text)")
    rw_lsn = Lsn(rw_endpoint.safe_psql_scalar("select pg_current_wal_flush_lsn()"))

    # TODO: consider using `class Workload` here
    # to do compaction and whatnot?

    #
    # validate that we can branch (important use case)
    #

    # ... at the tip
    _ = env.create_branch(
        new_branch_name="br-tip",
        ancestor_branch_name=import_branch_name,
        tenant_id=tenant_id,
        ancestor_start_lsn=rw_lsn,
    )
    br_tip_endpoint = env.endpoints.create_start(
        branch_name="br-tip", endpoint_id="br-tip-ro", tenant_id=tenant_id, config_lines=ep_config
    )
    validate_vanilla_equivalence(br_tip_endpoint)
    br_tip_endpoint.safe_psql("select * from othertable")

    # ... at the initdb lsn
    _ = env.create_branch(
        new_branch_name="br-initdb",
        ancestor_branch_name=import_branch_name,
        tenant_id=tenant_id,
        ancestor_start_lsn=initdb_lsn,
    )
    br_initdb_endpoint = env.endpoints.create_start(
        branch_name="br-initdb",
        endpoint_id="br-initdb-ro",
        tenant_id=tenant_id,
        config_lines=ep_config,
    )
    validate_vanilla_equivalence(br_initdb_endpoint)
    with pytest.raises(psycopg2.errors.UndefinedTable):
        br_initdb_endpoint.safe_psql("select * from othertable")


def test_fast_import_binary(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
):
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    pg_port = port_distributor.get_port()
    fast_import.run(pg_port, vanilla_pg.connstr())
    vanilla_pg.stop()

    pgbin = PgBin(test_output_dir, fast_import.pg_distrib_dir, fast_import.pg_version)
    with VanillaPostgres(
        fast_import.workdir / "pgdata", pgbin, pg_port, False
    ) as new_pgdata_vanilla_pg:
        new_pgdata_vanilla_pg.start()

        # database name and user are hardcoded in fast_import binary, and they are different from normal vanilla postgres
        conn = PgProtocol(dsn=f"postgresql://cloud_admin@localhost:{pg_port}/neondb")
        res = conn.safe_psql("SELECT count(*) FROM foo;")
        log.info(f"Result: {res}")
        assert res[0][0] == 10


def test_fast_import_restore_to_connstring(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
):
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    pgdatadir = test_output_dir / "restore-pgdata"
    pg_bin = PgBin(test_output_dir, pg_distrib_dir, pg_version)
    port = port_distributor.get_port()
    with VanillaPostgres(pgdatadir, pg_bin, port) as restore_vanilla_pg:
        restore_vanilla_pg.configure(["shared_preload_libraries='neon_rmgr'"])
        restore_vanilla_pg.start()

        fast_import.run(
            source_connection_string=vanilla_pg.connstr(),
            restore_connection_string=restore_vanilla_pg.connstr(),
        )
        vanilla_pg.stop()

        res = restore_vanilla_pg.safe_psql("SELECT count(*) FROM foo;")
        log.info(f"Result: {res}")
        assert res[0][0] == 10


def test_fast_import_restore_to_connstring_from_s3_spec(
    test_output_dir,
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    fast_import: FastImport,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    mock_s3_server: MockS3Server,
    mock_kms: KMSClient,
    mock_s3_client: S3Client,
):
    # Prepare KMS and S3
    key_response = mock_kms.create_key(
        Description="Test key",
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
    )
    key_id = key_response["KeyMetadata"]["KeyId"]

    def encrypt(x: str) -> EncryptResponseTypeDef:
        return mock_kms.encrypt(KeyId=key_id, Plaintext=x)

    # Start source postgres and ingest data
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE foo (a int); INSERT INTO foo SELECT generate_series(1, 10);")

    # Start target postgres
    pgdatadir = test_output_dir / "restore-pgdata"
    pg_bin = PgBin(test_output_dir, pg_distrib_dir, pg_version)
    port = port_distributor.get_port()
    with VanillaPostgres(pgdatadir, pg_bin, port) as restore_vanilla_pg:
        restore_vanilla_pg.configure(["shared_preload_libraries='neon_rmgr'"])
        restore_vanilla_pg.start()

        # Encrypt connstrings and put spec into S3
        source_connstring_encrypted = encrypt(vanilla_pg.connstr())
        restore_connstring_encrypted = encrypt(restore_vanilla_pg.connstr())
        spec = {
            "encryption_secret": {"KMS": {"key_id": key_id}},
            "source_connstring_ciphertext_base64": base64.b64encode(
                source_connstring_encrypted["CiphertextBlob"]
            ).decode("utf-8"),
            "restore_connstring_ciphertext_base64": base64.b64encode(
                restore_connstring_encrypted["CiphertextBlob"]
            ).decode("utf-8"),
        }

        mock_s3_client.create_bucket(Bucket="test-bucket")
        mock_s3_client.put_object(
            Bucket="test-bucket", Key="test-prefix/spec.json", Body=json.dumps(spec)
        )

        # Run fast_import
        if fast_import.extra_env is None:
            fast_import.extra_env = {}
        fast_import.extra_env["AWS_ACCESS_KEY_ID"] = mock_s3_server.access_key()
        fast_import.extra_env["AWS_SECRET_ACCESS_KEY"] = mock_s3_server.secret_key()
        fast_import.extra_env["AWS_SESSION_TOKEN"] = mock_s3_server.session_token()
        fast_import.extra_env["AWS_REGION"] = mock_s3_server.region()
        fast_import.extra_env["AWS_ENDPOINT_URL"] = mock_s3_server.endpoint()
        fast_import.extra_env["RUST_LOG"] = "aws_config=debug,aws_sdk_kms=debug"
        fast_import.run(s3prefix="s3://test-bucket/test-prefix")
        vanilla_pg.stop()

        res = restore_vanilla_pg.safe_psql("SELECT count(*) FROM foo;")
        log.info(f"Result: {res}")
        assert res[0][0] == 10


# TODO: Maybe test with pageserver?
# 1. run whole neon env
# 2. create timeline with some s3 path???
# 3. run fast_import with s3 prefix
# 4. ??? mock http where pageserver will report progress
# 5. run compute on this timeline and check if data is there
