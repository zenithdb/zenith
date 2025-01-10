from __future__ import annotations

import gzip
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from queue import SimpleQueue
from typing import TYPE_CHECKING

from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.remote_storage import (
    LocalFsStorage,
    RemoteStorageKind,
    remote_storage_to_toml_inline_table,
)
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

if TYPE_CHECKING:
    from typing import Any

    from fixtures.httpserver import ListenAddress


# TODO: collect all of the env setup *AFTER* removal of RemoteStorageKind.NOOP


def test_metric_collection(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
):
    (host, port) = httpserver_listen_address
    metric_collection_endpoint = f"http://{host}:{port}/billing/api/v1/usage_events"

    # this should be Union[str, Tuple[List[Any], bool]], but it will make unpacking much more verbose
    uploads: SimpleQueue[Any] = SimpleQueue()

    def metrics_handler(request: Request) -> Response:
        if request.json is None:
            return Response(status=400)

        events = request.json["events"]
        is_last = request.headers["pageserver-metrics-last-upload-in-batch"]
        assert is_last in ["true", "false"]
        uploads.put((events, is_last == "true"))
        return Response(status=200)

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    assert neon_env_builder.pageserver_remote_storage is not None

    # Require collecting metrics frequently, since we change
    # the timeline and want something to be logged about it.
    #
    # Disable time-based pitr, we will use the manual GC calls
    # to trigger remote storage operations in a controlled way
    neon_env_builder.pageserver_config_override = f"""
        metric_collection_interval="1s"
        metric_collection_endpoint="{metric_collection_endpoint}"
        metric_collection_bucket={remote_storage_to_toml_inline_table(neon_env_builder.pageserver_remote_storage)}
        synthetic_size_calculation_interval="3s"
        """

    log.info(f"test_metric_collection endpoint is {metric_collection_endpoint}")

    # mock http server that returns OK for the metrics
    httpserver.expect_request("/billing/api/v1/usage_events", method="POST").respond_with_handler(
        metrics_handler
    )

    # spin up neon,  after http server is ready
    env = neon_env_builder.init_start(initial_tenant_conf={"pitr_interval": "0 sec"})
    # httpserver is shut down before pageserver during passing run
    env.pageserver.allowed_errors.extend(
        [
            ".*metrics endpoint refused the sent metrics*",
            ".*metrics_collection: failed to upload to remote storage: Failed to upload data of length .* to storage path.*",
        ]
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("CREATE TABLE foo (id int, counter int, t text)")
    cur.execute(
        """
        INSERT INTO foo
        SELECT g, 0, 'long string to consume some space' || g
        FROM generate_series(1, 100000) g
        """
    )

    # Helper function that gets the number of given kind of remote ops from the metrics
    def get_num_remote_ops(file_kind: str, op_kind: str) -> int:
        ps_metrics = env.pageserver.http_client().get_metrics()
        total = 0.0
        for sample in ps_metrics.query_all(
            name="pageserver_remote_operation_seconds_count",
            filter={
                "file_kind": str(file_kind),
                "op_kind": str(op_kind),
            },
        ):
            total += sample[2]
        return int(total)

    # upload some data to remote storage
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    pageserver_http = env.pageserver.http_client()
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    pageserver_http.timeline_gc(tenant_id, timeline_id, 10000)

    remote_uploaded = get_num_remote_ops("index", "upload")
    assert remote_uploaded > 0

    # we expect uploads at 1Hz, on busy runners this could be too optimistic,
    # so give 5s we only want to get the following upload after "ready" value.
    timeout = 5

    # these strings in the upload queue allow synchronizing with the uploads
    # and the main test execution
    uploads.put("ready")

    # note that this verifier graph should live across restarts as long as the
    # cache file lives
    v = MetricsVerifier()

    while True:
        events = uploads.get(timeout=timeout)

        if events == "ready":
            (events, is_last) = uploads.get(timeout=timeout)
            v.ingest(events, is_last)
            break
        else:
            (events, is_last) = events
            v.ingest(events, is_last)

    if "synthetic_storage_size" not in v.accepted_event_names():
        log.info("waiting for synthetic storage size to be calculated and uploaded...")

    rounds = 0
    while "synthetic_storage_size" not in v.accepted_event_names():
        (events, is_last) = uploads.get(timeout=timeout)
        v.ingest(events, is_last)
        rounds += 1
        assert rounds < 10, "did not get synthetic_storage_size in 10 uploads"
        # once we have it in verifiers, it will assert that future batches will contain it

    env.pageserver.stop()
    time.sleep(1)
    uploads.put("ready")
    env.pageserver.start()

    while True:
        events = uploads.get(timeout=timeout)

        if events == "ready":
            (events, is_last) = uploads.get(timeout=timeout * 3)
            v.ingest(events, is_last)
            (events, is_last) = uploads.get(timeout=timeout)
            v.ingest(events, is_last)
            break
        else:
            (events, is_last) = events
            v.ingest(events, is_last)

    httpserver.check()

    # Check that at least one bucket output object is present, and that all
    # can be decompressed and decoded.
    bucket_dumps = {}
    assert isinstance(env.pageserver_remote_storage, LocalFsStorage)
    for dirpath, _dirs, files in os.walk(env.pageserver_remote_storage.root):
        for file in files:
            file_path = os.path.join(dirpath, file)
            log.info(file_path)
            if file.endswith(".gz"):
                bucket_dumps[file_path] = json.load(gzip.open(file_path))

    assert len(bucket_dumps) >= 1
    assert all("events" in data for data in bucket_dumps.values())


def test_metric_collection_cleans_up_tempfile(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
):
    (host, port) = httpserver_listen_address
    metric_collection_endpoint = f"http://{host}:{port}/billing/api/v1/usage_events"

    # this should be Union[str, Tuple[List[Any], bool]], but it will make unpacking much more verbose
    uploads: SimpleQueue[Any] = SimpleQueue()

    def metrics_handler(request: Request) -> Response:
        if request.json is None:
            return Response(status=400)

        events = request.json["events"]
        is_last = request.headers["pageserver-metrics-last-upload-in-batch"]
        assert is_last in ["true", "false"]
        uploads.put((events, is_last == "true"))
        return Response(status=200)

    # Require collecting metrics frequently, since we change
    # the timeline and want something to be logged about it.
    #
    # Disable time-based pitr, we will use the manual GC calls
    # to trigger remote storage operations in a controlled way
    neon_env_builder.pageserver_config_override = f"""
        metric_collection_interval="1s"
        metric_collection_endpoint="{metric_collection_endpoint}"
        synthetic_size_calculation_interval="3s"
        """

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    # mock http server that returns OK for the metrics
    httpserver.expect_request("/billing/api/v1/usage_events", method="POST").respond_with_handler(
        metrics_handler
    )

    # spin up neon,  after http server is ready
    env = neon_env_builder.init_start(initial_tenant_conf={"pitr_interval": "0 sec"})
    pageserver_http = env.pageserver.http_client()

    # httpserver is shut down before pageserver during passing run
    env.pageserver.allowed_errors.extend(
        [
            ".*metrics endpoint refused the sent metrics*",
        ]
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("CREATE TABLE foo (id int, counter int, t text)")
    cur.execute(
        """
        INSERT INTO foo
        SELECT g, 0, 'long string to consume some space' || g
        FROM generate_series(1, 100000) g
        """
    )

    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    # we expect uploads at 1Hz, on busy runners this could be too optimistic,
    # so give 5s we only want to get the following upload after "ready" value.
    timeout = 5

    # these strings in the upload queue allow synchronizing with the uploads
    # and the main test execution
    uploads.put("ready")

    while True:
        events = uploads.get(timeout=timeout)

        if events == "ready":
            (events, _) = uploads.get(timeout=timeout)
            break

    # should really configure an env?
    pageserver_http.configure_failpoints(("before-persist-last-metrics-collected", "exit"))

    time.sleep(3)

    env.pageserver.stop()

    initially = iterate_pageserver_workdir(env.pageserver.workdir, "last_consumption_metrics.json")

    assert (
        len(initially.matching) == 2
    ), f"expecting actual file and tempfile, but not found: {initially.matching}"

    uploads.put("ready")
    env.pageserver.start()

    while True:
        events = uploads.get(timeout=timeout * 3)

        if events == "ready":
            (events, _) = uploads.get(timeout=timeout)
            break

    env.pageserver.stop()

    later = iterate_pageserver_workdir(env.pageserver.workdir, "last_consumption_metrics.json")

    # it is possible we shutdown the pageserver right at the correct time, so the old tempfile
    # is gone, but we also have a new one.
    only = set(["last_consumption_metrics.json"])
    assert (
        initially.matching.intersection(later.matching) == only
    ), "only initial tempfile should had been removed"
    assert initially.other.issuperset(later.other), "no other files should had been removed"


@dataclass
class PrefixPartitionedFiles:
    matching: set[str]
    other: set[str]


def iterate_pageserver_workdir(path: Path, prefix: str) -> PrefixPartitionedFiles:
    """
    Iterates the files in the workdir, returns two sets:
    - files with the prefix
    - files without the prefix
    """

    matching = set()
    other = set()
    for entry in path.iterdir():
        if not entry.is_file():
            continue

        if not entry.name.startswith(prefix):
            other.add(entry.name)
        else:
            matching.add(entry.name)

    return PrefixPartitionedFiles(matching, other)


class MetricsVerifier:
    """
    A graph of per tenant per timeline verifiers, allowing one for each
    metric
    """

    def __init__(self):
        self.tenants: dict[TenantId, TenantMetricsVerifier] = {}
        pass

    def ingest(self, events, is_last):
        stringified = json.dumps(events, indent=2)
        log.info(f"ingesting: {stringified}")
        for event in events:
            id = TenantId(event["tenant_id"])
            if id not in self.tenants:
                self.tenants[id] = TenantMetricsVerifier(id)

            self.tenants[id].ingest(event)

        if is_last:
            for t in self.tenants.values():
                t.post_batch()

    def accepted_event_names(self) -> set[str]:
        names: set[str] = set()
        for t in self.tenants.values():
            names = names.union(t.accepted_event_names())
        return names


class TenantMetricsVerifier:
    def __init__(self, id: TenantId):
        self.id = id
        self.timelines: dict[TimelineId, TimelineMetricsVerifier] = {}
        self.state: dict[str, Any] = {}

    def ingest(self, event):
        assert TenantId(event["tenant_id"]) == self.id

        if "timeline_id" in event:
            id = TimelineId(event["timeline_id"])
            if id not in self.timelines:
                self.timelines[id] = TimelineMetricsVerifier(self.id, id)

            self.timelines[id].ingest(event)
        else:
            name = event["metric"]
            if name not in self.state:
                self.state[name] = PER_METRIC_VERIFIERS[name]()
            self.state[name].ingest(event, self)

    def post_batch(self):
        for v in self.state.values():
            v.post_batch(self)

        for tl in self.timelines.values():
            tl.post_batch(self)

    def accepted_event_names(self) -> set[str]:
        names = set(self.state.keys())
        for t in self.timelines.values():
            names = names.union(t.accepted_event_names())
        return names


class TimelineMetricsVerifier:
    def __init__(self, tenant_id: TenantId, timeline_id: TimelineId):
        self.id = timeline_id
        self.state: dict[str, Any] = {}

    def ingest(self, event):
        name = event["metric"]
        if name not in self.state:
            self.state[name] = PER_METRIC_VERIFIERS[name]()
        self.state[name].ingest(event, self)

    def post_batch(self, parent):
        for v in self.state.values():
            v.post_batch(self)

    def accepted_event_names(self) -> set[str]:
        return set(self.state.keys())


class CannotVerifyAnything:
    """We can only assert types, but rust already has types, so no need."""

    def __init__(self):
        pass

    def ingest(self, event, parent):
        pass

    def post_batch(self, parent):
        pass


class WrittenDataVerifier:
    def __init__(self):
        self.values = []
        pass

    def ingest(self, event, parent):
        self.values.append(event["value"])

    def post_batch(self, parent):
        pass


class WrittenDataDeltaVerifier:
    def __init__(self):
        self.value = None
        self.sum = 0
        self.timerange = None
        pass

    def ingest(self, event, parent):
        assert event["type"] == "incremental"
        self.value = event["value"]
        self.sum += event["value"]
        start = event["start_time"]
        stop = event["stop_time"]
        timerange = (start, stop)
        if self.timerange is not None:
            # this holds across restarts
            assert self.timerange[1] == timerange[0], "time ranges should be continious"
        self.timerange = timerange

    def post_batch(self, parent):
        absolute = parent.state["written_size"]
        if len(absolute.values) == 1:
            # in tests this comes up as initdb execution, so we can have 0 or
            # about 30MB on the first event. it is not consistent.
            assert self.value is not None
        else:
            assert self.value == absolute.values[-1] - absolute.values[-2]
            # sounds like this should hold, but it will not for branches -- probably related to timing
            # assert self.sum == absolute.latest


class SyntheticSizeVerifier:
    def __init__(self):
        self.prev = None
        self.value = None
        pass

    def ingest(self, event, parent):
        assert isinstance(parent, TenantMetricsVerifier)
        assert event["type"] == "absolute"
        value = event["value"]
        self.value = value

    def post_batch(self, parent):
        if self.prev is not None:
            # this is assuming no one goes and deletes the cache file
            assert (
                self.value is not None
            ), "after calculating first synthetic size, cached or more recent should be sent"
        self.prev = self.value
        self.value = None


PER_METRIC_VERIFIERS = {
    "remote_storage_size": CannotVerifyAnything,
    "resident_size": CannotVerifyAnything,
    "written_size": WrittenDataVerifier,
    "written_data_bytes_delta": WrittenDataDeltaVerifier,
    "timeline_logical_size": CannotVerifyAnything,
    "synthetic_storage_size": SyntheticSizeVerifier,
}
