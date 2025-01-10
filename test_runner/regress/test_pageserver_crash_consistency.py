from __future__ import annotations

import pytest
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.pageserver.common_types import ImageLayerName, parse_layer_file_name
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
    wait_until_tenant_active,
)
from fixtures.remote_storage import LocalFsStorage, RemoteStorageKind
from requests.exceptions import ConnectionError


def test_local_only_layers_after_crash(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Test case for docs/rfcs/027-crash-consistent-layer-map-through-index-part.md.

    Simulate crash after compaction has written layers to disk
    but before they have been uploaded/linked into remote index_part.json.

    Startup handles this situation by deleting the not yet uploaded L1 layer files.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "checkpoint_distance": f"{10 * 1024**2}",
            "compaction_period": "0 s",
            "compaction_threshold": "999999",
        }
    )
    pageserver_http = env.pageserver.http_client()

    tenant_id, timeline_id = env.initial_tenant, env.initial_timeline

    pageserver_http.configure_failpoints(("after-timeline-compacted-first-L1", "exit"))

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    connstr = endpoint.connstr(options="-csynchronous_commit=off")
    pg_bin.run_capture(["pgbench", "-i", "-s1", connstr])

    lsn = wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    # make sure we receive no new wal after this, so that we'll write over the same L1 file.
    endpoint.stop()
    for sk in env.safekeepers:
        sk.stop()

    env.storage_controller.pageserver_api().update_tenant_config(
        tenant_id, {"compaction_threshold": 3}
    )
    # hit the exit failpoint
    with pytest.raises(ConnectionError, match="Remote end closed connection without response"):
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    env.pageserver.stop()

    # now the duplicate L1 has been created, but is not yet uploaded
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)

    # path = env.remote_storage.timeline_path(tenant_id, timeline_id)
    l1_found = None
    for path in env.pageserver.list_layers(tenant_id, timeline_id):
        [key_range, lsn_range] = path.name.split("__", maxsplit=1)

        if "-" not in lsn_range:
            # image layer
            continue

        [key_start, key_end] = key_range.split("-", maxsplit=1)

        if key_start == "0" * 36 and key_end == "F" * 36:
            # L0
            continue

        candidate = parse_layer_file_name(path.name)

        if isinstance(candidate, ImageLayerName):
            continue

        if l1_found is not None:
            raise RuntimeError(f"found multiple L1: {l1_found.to_str()} and {path.name}")

        l1_found = candidate

    assert l1_found is not None, "failed to find L1 locally"

    uploaded = env.pageserver_remote_storage.remote_layer_path(
        tenant_id, timeline_id, l1_found.to_str()
    )
    assert not uploaded.exists(), "to-be-overwritten should not yet be uploaded"

    env.pageserver.start()
    wait_until_tenant_active(pageserver_http, tenant_id)

    assert not env.pageserver.layer_exists(
        tenant_id, timeline_id, l1_found
    ), "partial compaction result should had been removed during startup"

    # wait for us to catch up again
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, lsn)

    pageserver_http.timeline_compact(tenant_id, timeline_id, wait_until_uploaded=True)

    assert env.pageserver.layer_exists(tenant_id, timeline_id, l1_found), "the L1 reappears"

    uploaded = env.pageserver_remote_storage.remote_layer_path(
        tenant_id, timeline_id, l1_found.to_str()
    )
    assert uploaded.exists(), "the L1 is uploaded"


# TODO: same test for L0s produced by ingest.
