from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    last_flush_lsn_upload,
    tenant_get_shards,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.utils import wait_for_last_record_lsn

if TYPE_CHECKING:
    from typing import Any

# neon_local doesn't handle creating/modifying endpoints concurrently, so we use a mutex
# to ensure we don't do that: this enables running lots of Workloads in parallel safely.
ENDPOINT_LOCK = threading.Lock()


class Workload:
    """
    This is not a general purpose load generator: it exists for storage tests that need to inject some
    high level types of storage work via the postgres interface:
    - layer writes (`write_rows`)
    - work for compaction (`churn_rows`)
    - reads, checking we get the right data (`validate`)
    """

    def __init__(
        self,
        env: NeonEnv,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        branch_name: str | None = None,
        endpoint_opts: dict[str, Any] | None = None,
    ):
        self.env = env
        self.tenant_id = tenant_id
        self.timeline_id = timeline_id
        self.table = "foo"

        # By default, use the default branch name for initial tenant in NeonEnv
        self.branch_name = branch_name or "main"

        self.expect_rows = 0
        self.churn_cursor = 0

        self._endpoint: Endpoint | None = None
        self._endpoint_opts = endpoint_opts or {}

    def branch(
        self,
        timeline_id: TimelineId,
        branch_name: str | None = None,
        endpoint_opts: dict[str, Any] | None = None,
    ) -> Workload:
        """
        Checkpoint the current status of the workload in case of branching
        """
        branch_workload = Workload(
            self.env, self.tenant_id, timeline_id, branch_name, endpoint_opts
        )
        branch_workload.expect_rows = self.expect_rows
        branch_workload.churn_cursor = self.churn_cursor
        return branch_workload

    def reconfigure(self) -> None:
        """
        Request the endpoint to reconfigure based on location reported by storage controller
        """
        if self._endpoint is not None:
            with ENDPOINT_LOCK:
                self._endpoint.reconfigure()

    def endpoint(self, pageserver_id: int | None = None) -> Endpoint:
        # We may be running alongside other Workloads for different tenants.  Full TTID is
        # obnoxiously long for use here, but a cut-down version is still unique enough for tests.
        endpoint_id = f"ep-workload-{str(self.tenant_id)[0:4]}-{str(self.timeline_id)[0:4]}"

        with ENDPOINT_LOCK:
            if self._endpoint is None:
                self._endpoint = self.env.endpoints.create(
                    self.branch_name,
                    tenant_id=self.tenant_id,
                    pageserver_id=pageserver_id,
                    endpoint_id=endpoint_id,
                    **self._endpoint_opts,
                )
                self._endpoint.start(pageserver_id=pageserver_id)
            else:
                self._endpoint.reconfigure(pageserver_id=pageserver_id)

        connstring = self._endpoint.safe_psql(
            "SELECT setting FROM pg_settings WHERE name='neon.pageserver_connstring'"
        )
        log.info(f"Workload.endpoint: connstr={connstring}")

        return self._endpoint

    def stop(self):
        if self._endpoint is not None:
            self._endpoint.stop()
            self._endpoint = None

    def __del__(self):
        self.stop()

    def init(self, pageserver_id: int | None = None, allow_recreate=False):
        endpoint = self.endpoint(pageserver_id)
        if allow_recreate:
            endpoint.safe_psql(f"DROP TABLE IF EXISTS {self.table};")
        endpoint.safe_psql(f"CREATE TABLE {self.table} (id INTEGER PRIMARY KEY, val text);")
        endpoint.safe_psql("CREATE EXTENSION IF NOT EXISTS neon_test_utils;")
        last_flush_lsn_upload(
            self.env, endpoint, self.tenant_id, self.timeline_id, pageserver_id=pageserver_id
        )

    def write_rows(self, n: int, pageserver_id: int | None = None, upload: bool = True):
        endpoint = self.endpoint(pageserver_id)
        start = self.expect_rows
        end = start + n - 1
        self.expect_rows += n
        dummy_value = "blah"
        endpoint.safe_psql(
            f"""
            INSERT INTO {self.table} (id, val)
            SELECT g, '{dummy_value}'
            FROM generate_series({start}, {end}) g
            """
        )

        if upload:
            return last_flush_lsn_upload(
                self.env, endpoint, self.tenant_id, self.timeline_id, pageserver_id=pageserver_id
            )
        else:
            return False

    def churn_rows(
        self, n: int, pageserver_id: int | None = None, upload: bool = True, ingest: bool = True
    ):
        assert self.expect_rows >= n

        max_iters = 10
        endpoint = self.endpoint(pageserver_id)
        todo = n
        i = 0
        while todo > 0:
            i += 1
            if i > max_iters:
                raise RuntimeError("oops")
            start = self.churn_cursor % self.expect_rows
            n_iter = min((self.expect_rows - start), todo)
            todo -= n_iter

            end = start + n_iter - 1

            log.info(
                f"start,end = {start},{end}, cursor={self.churn_cursor}, expect_rows={self.expect_rows}"
            )

            assert end < self.expect_rows

            self.churn_cursor += n_iter
            dummy_value = "blah"
            endpoint.safe_psql_many(
                [
                    f"""
                INSERT INTO {self.table} (id, val)
                SELECT g, '{dummy_value}'
                FROM generate_series({start}, {end}) g
                ON CONFLICT (id) DO UPDATE
                SET val = EXCLUDED.val
                """,
                    f"VACUUM {self.table}",
                ]
            )

        if ingest:
            # Wait for written data to be ingested by the pageserver
            for tenant_shard_id, pageserver in tenant_get_shards(
                self.env, self.tenant_id, pageserver_id
            ):
                last_flush_lsn = wait_for_last_flush_lsn(
                    self.env,
                    endpoint,
                    self.tenant_id,
                    self.timeline_id,
                    pageserver_id=pageserver_id,
                )
                ps_http = pageserver.http_client()
                wait_for_last_record_lsn(ps_http, tenant_shard_id, self.timeline_id, last_flush_lsn)

                if upload:
                    # Wait for written data to be uploaded to S3 (force a checkpoint to trigger upload)
                    ps_http.timeline_checkpoint(
                        tenant_shard_id,
                        self.timeline_id,
                        wait_until_uploaded=True,
                    )
                    log.info(f"Churn: waiting for remote LSN {last_flush_lsn}")
                else:
                    log.info(f"Churn: not waiting for upload, disk LSN {last_flush_lsn}")

    def validate(self, pageserver_id: int | None = None):
        endpoint = self.endpoint(pageserver_id)
        endpoint.clear_buffers()
        result = endpoint.safe_psql(f"SELECT COUNT(*) FROM {self.table}")

        log.info(f"validate({self.expect_rows}): {result}")
        assert result == [(self.expect_rows,)]
