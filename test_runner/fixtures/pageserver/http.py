from __future__ import annotations

import dataclasses
import json
import random
import string
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from fixtures.common_types import (
    Lsn,
    TenantId,
    TenantShardId,
    TimelineArchivalState,
    TimelineId,
)
from fixtures.log_helper import log
from fixtures.metrics import Metrics, MetricsGetter, parse_metrics
from fixtures.pg_version import PgVersion
from fixtures.utils import EnhancedJSONEncoder, Fn


class PageserverApiException(Exception):
    def __init__(self, message, status_code: int):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


@dataclass
class ImportPgdataIdemptencyKey:
    key: str

    @staticmethod
    def random() -> ImportPgdataIdemptencyKey:
        return ImportPgdataIdemptencyKey(
            "".join(random.choices(string.ascii_letters + string.digits, k=20))
        )


@dataclass
class LocalFs:
    path: str


@dataclass
class AwsS3:
    region: str
    bucket: str
    key: str


@dataclass
class ImportPgdataLocation:
    LocalFs: None | LocalFs = None
    AwsS3: None | AwsS3 = None


@dataclass
class TimelineCreateRequestModeImportPgdata:
    location: ImportPgdataLocation
    idempotency_key: ImportPgdataIdemptencyKey


@dataclass
class TimelineCreateRequestMode:
    Branch: None | dict[str, Any] = None
    Bootstrap: None | dict[str, Any] = None
    ImportPgdata: None | TimelineCreateRequestModeImportPgdata = None


@dataclass
class TimelineCreateRequest:
    new_timeline_id: TimelineId
    mode: TimelineCreateRequestMode

    def to_json(self) -> str:
        # mode is flattened
        this = dataclasses.asdict(self)
        mode = this.pop("mode")
        this.update(mode)
        return json.dumps(self, cls=EnhancedJSONEncoder)


class TimelineCreate406(PageserverApiException):
    def __init__(self, res: requests.Response):
        assert res.status_code == 406
        super().__init__(res.json()["msg"], res.status_code)


class TimelineCreate409(PageserverApiException):
    def __init__(self, res: requests.Response):
        assert res.status_code == 409
        super().__init__(res.json()["msg"], res.status_code)


@dataclass
class InMemoryLayerInfo:
    kind: str
    lsn_start: str
    lsn_end: str | None

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> InMemoryLayerInfo:
        return InMemoryLayerInfo(
            kind=d["kind"],
            lsn_start=d["lsn_start"],
            lsn_end=d.get("lsn_end"),
        )


@dataclass(frozen=True)
class HistoricLayerInfo:
    kind: str
    layer_file_name: str
    layer_file_size: int
    lsn_start: str
    lsn_end: str | None
    remote: bool
    # None for image layers, true if pageserver thinks this is an L0 delta layer
    l0: bool | None
    visible: bool

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> HistoricLayerInfo:
        # instead of parsing the key range lets keep the definition of "L0" in pageserver
        l0_ness = d.get("l0")
        assert l0_ness is None or isinstance(l0_ness, bool)

        size = d["layer_file_size"]
        assert isinstance(size, int)

        return HistoricLayerInfo(
            kind=d["kind"],
            layer_file_name=d["layer_file_name"],
            layer_file_size=size,
            lsn_start=d["lsn_start"],
            lsn_end=d.get("lsn_end"),
            remote=d["remote"],
            l0=l0_ness,
            visible=d["access_stats"]["visible"],
        )


@dataclass
class LayerMapInfo:
    in_memory_layers: list[InMemoryLayerInfo]
    historic_layers: list[HistoricLayerInfo]

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> LayerMapInfo:
        info = LayerMapInfo(in_memory_layers=[], historic_layers=[])

        json_in_memory_layers = d["in_memory_layers"]
        assert isinstance(json_in_memory_layers, list)
        for json_in_memory_layer in json_in_memory_layers:
            info.in_memory_layers.append(InMemoryLayerInfo.from_json(json_in_memory_layer))

        json_historic_layers = d["historic_layers"]
        assert isinstance(json_historic_layers, list)
        for json_historic_layer in json_historic_layers:
            info.historic_layers.append(HistoricLayerInfo.from_json(json_historic_layer))

        return info

    def kind_count(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for inmem_layer in self.in_memory_layers:
            counts[inmem_layer.kind] += 1
        for hist_layer in self.historic_layers:
            counts[hist_layer.kind] += 1
        return counts

    def delta_layers(self) -> list[HistoricLayerInfo]:
        return [x for x in self.historic_layers if x.kind == "Delta"]

    def image_layers(self) -> list[HistoricLayerInfo]:
        return [x for x in self.historic_layers if x.kind == "Image"]

    def delta_l0_layers(self) -> list[HistoricLayerInfo]:
        return [x for x in self.historic_layers if x.kind == "Delta" and x.l0]

    def historic_by_name(self) -> set[str]:
        return set(x.layer_file_name for x in self.historic_layers)


@dataclass
class ScanDisposableKeysResponse:
    disposable_count: int
    not_disposable_count: int

    def __add__(self, b):
        a = self
        assert isinstance(a, ScanDisposableKeysResponse)
        assert isinstance(b, ScanDisposableKeysResponse)
        return ScanDisposableKeysResponse(
            a.disposable_count + b.disposable_count, a.not_disposable_count + b.not_disposable_count
        )

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> ScanDisposableKeysResponse:
        disposable_count = d["disposable_count"]
        not_disposable_count = d["not_disposable_count"]
        return ScanDisposableKeysResponse(disposable_count, not_disposable_count)


@dataclass
class TenantConfig:
    tenant_specific_overrides: dict[str, Any]
    effective_config: dict[str, Any]

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> TenantConfig:
        return TenantConfig(
            tenant_specific_overrides=d["tenant_specific_overrides"],
            effective_config=d["effective_config"],
        )


@dataclass
class TimelinesInfoAndOffloaded:
    timelines: list[dict[str, Any]]
    offloaded: list[dict[str, Any]]

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> TimelinesInfoAndOffloaded:
        return TimelinesInfoAndOffloaded(
            timelines=d["timelines"],
            offloaded=d["offloaded"],
        )


class PageserverHttpClient(requests.Session, MetricsGetter):
    def __init__(
        self,
        port: int,
        is_testing_enabled_or_skip: Fn,
        auth_token: str | None = None,
        retries: Retry | None = None,
    ):
        super().__init__()
        self.port = port
        self.auth_token = auth_token
        self.is_testing_enabled_or_skip = is_testing_enabled_or_skip

        if retries is None:
            # We apply a retry policy that is different to the default `requests` behavior,
            # because the pageserver has various transiently unavailable states that benefit
            # from a client retrying on 503

            retries = Retry(
                # Status retries are for retrying on 503 while e.g. waiting for tenants to activate
                status=5,
                # Connection retries are for waiting for the pageserver to come up and listen
                connect=5,
                # No read retries: if a request hangs that is not expected behavior
                # (this may change in future if we do fault injection of a kind that causes
                #  requests TCP flows to stick)
                read=False,
                backoff_factor=0.2,
                status_forcelist=[503],
                allowed_methods=None,
                remove_headers_on_redirect=[],
            )

        self.mount("http://", HTTPAdapter(max_retries=retries))

        if auth_token is not None:
            self.headers["Authorization"] = f"Bearer {auth_token}"

    def without_status_retrying(self) -> PageserverHttpClient:
        retries = Retry(
            status=0,
            connect=5,
            read=False,
            backoff_factor=0.2,
            status_forcelist=[],
            allowed_methods=None,
            remove_headers_on_redirect=[],
        )

        return PageserverHttpClient(
            self.port, self.is_testing_enabled_or_skip, self.auth_token, retries
        )

    @property
    def base_url(self) -> str:
        return f"http://localhost:{self.port}"

    def verbose_error(self, res: requests.Response):
        try:
            res.raise_for_status()
        except requests.RequestException as e:
            try:
                msg = res.json()["msg"]
            except:  # noqa: E722
                msg = ""
            raise PageserverApiException(msg, res.status_code) from e

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def configure_failpoints(self, config_strings: tuple[str, str] | list[tuple[str, str]]):
        self.is_testing_enabled_or_skip()

        if isinstance(config_strings, tuple):
            pairs = [config_strings]
        else:
            pairs = config_strings

        log.info(f"Requesting config failpoints: {repr(pairs)}")

        res = self.put(
            f"http://localhost:{self.port}/v1/failpoints",
            json=[{"name": name, "actions": actions} for name, actions in pairs],
        )
        log.info(f"Got failpoints request response code {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None
        return res_json

    def reload_auth_validation_keys(self):
        res = self.post(f"http://localhost:{self.port}/v1/reload_auth_validation_keys")
        self.verbose_error(res)

    def tenant_list(self) -> list[dict[Any, Any]]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def tenant_attach(
        self,
        tenant_id: TenantId | TenantShardId,
        generation: int,
        config: None | dict[str, Any] = None,
    ):
        config = config or {}

        return self.tenant_location_conf(
            tenant_id,
            location_conf={
                "mode": "AttachedSingle",
                "secondary_conf": None,
                "tenant_conf": config,
                "generation": generation,
            },
        )

    def tenant_detach(self, tenant_id: TenantId):
        return self.tenant_location_conf(
            tenant_id,
            location_conf={
                "mode": "Detached",
                "secondary_conf": None,
                "tenant_conf": {},
                "generation": None,
            },
        )

    def tenant_reset(self, tenant_id: TenantId | TenantShardId, drop_cache: bool):
        params = {}
        if drop_cache:
            params["drop_cache"] = "true"

        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/reset", params=params)
        self.verbose_error(res)

    def tenant_location_conf(
        self,
        tenant_id: TenantId | TenantShardId,
        location_conf: dict[str, Any],
        flush_ms=None,
        lazy: bool | None = None,
    ):
        body = location_conf.copy()

        params = {}
        if flush_ms is not None:
            params["flush_ms"] = str(flush_ms)

        if lazy is not None:
            params["lazy"] = "true" if lazy else "false"

        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/location_config",
            json=body,
            params=params,
        )
        self.verbose_error(res)
        return res.json()

    def tenant_list_locations(self):
        res = self.get(
            f"http://localhost:{self.port}/v1/location_config",
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json["tenant_shards"], list)
        return res_json

    def tenant_get_location(self, tenant_id: TenantId | TenantShardId):
        res = self.get(
            f"http://localhost:{self.port}/v1/location_config/{tenant_id}",
        )
        self.verbose_error(res)
        return res.json()

    def tenant_delete(self, tenant_id: TenantId | TenantShardId):
        res = self.delete(f"http://localhost:{self.port}/v1/tenant/{tenant_id}")
        self.verbose_error(res)
        return res

    def tenant_status(
        self, tenant_id: TenantId | TenantShardId, activate: bool = False
    ) -> dict[Any, Any]:
        """
        :activate: hint the server not to accelerate activation of this tenant in response
        to this query.  False by default for tests, because they generally want to observed the
        system rather than interfering with it.  This is true  by default on the server side,
        because in the field if the control plane is GET'ing a tenant it's a sign that it wants
        to do something with it.
        """
        params = {}
        if not activate:
            params["activate"] = "false"

        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}", params=params)
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def tenant_config(self, tenant_id: TenantId | TenantShardId) -> TenantConfig:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/config")
        self.verbose_error(res)
        return TenantConfig.from_json(res.json())

    def tenant_heatmap_upload(self, tenant_id: TenantId | TenantShardId):
        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/heatmap_upload")
        self.verbose_error(res)

    def tenant_secondary_download(
        self, tenant_id: TenantId | TenantShardId, wait_ms: int | None = None
    ) -> tuple[int, dict[Any, Any]]:
        url = f"http://localhost:{self.port}/v1/tenant/{tenant_id}/secondary/download"
        if wait_ms is not None:
            url = url + f"?wait_ms={wait_ms}"
        res = self.post(url)
        self.verbose_error(res)
        return (res.status_code, res.json())

    def tenant_secondary_status(self, tenant_id: TenantId | TenantShardId):
        url = f"http://localhost:{self.port}/v1/tenant/{tenant_id}/secondary/status"
        res = self.get(url)
        self.verbose_error(res)
        return res.json()

    def set_tenant_config(self, tenant_id: TenantId | TenantShardId, config: dict[str, Any]):
        """
        Only use this via storage_controller.pageserver_api().

        Storcon is the authority on tenant config - changes you make directly
        against pageserver may be reconciled away at any time.
        """
        assert "tenant_id" not in config.keys()
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/config",
            json={**config, "tenant_id": str(tenant_id)},
        )
        self.verbose_error(res)

    def patch_tenant_config(self, tenant_id: TenantId | TenantShardId, updates: dict[str, Any]):
        """
        Only use this via storage_controller.pageserver_api().

        See `set_tenant_config` for more information.
        """
        assert "tenant_id" not in updates.keys()
        res = self.patch(
            f"http://localhost:{self.port}/v1/tenant/config",
            json={**updates, "tenant_id": str(tenant_id)},
        )
        self.verbose_error(res)

    def update_tenant_config(
        self,
        tenant_id: TenantId,
        inserts: dict[str, Any] | None = None,
        removes: list[str] | None = None,
    ):
        """
        Only use this via storage_controller.pageserver_api().

        See `set_tenant_config` for more information.
        """
        if inserts is None:
            inserts = {}
        if removes is None:
            removes = []

        patch = inserts | {remove: None for remove in removes}
        self.patch_tenant_config(tenant_id, patch)

    def tenant_size(self, tenant_id: TenantId | TenantShardId) -> int:
        return self.tenant_size_and_modelinputs(tenant_id)[0]

    def tenant_size_and_modelinputs(
        self, tenant_id: TenantId | TenantShardId
    ) -> tuple[int, dict[str, Any]]:
        """
        Returns the tenant size, together with the model inputs as the second tuple item.
        """
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/synthetic_size")
        self.verbose_error(res)
        res = res.json()
        assert isinstance(res, dict)
        assert TenantId(res["id"]) == tenant_id
        size = res["size"]
        assert isinstance(size, int)
        inputs = res["inputs"]
        assert isinstance(inputs, dict)
        return (size, inputs)

    def tenant_size_debug(self, tenant_id: TenantId | TenantShardId) -> str:
        """
        Returns the tenant size debug info, as an HTML string
        """
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/synthetic_size",
            headers={"Accept": "text/html"},
        )
        return res.text

    def tenant_time_travel_remote_storage(
        self,
        tenant_id: TenantId | TenantShardId,
        timestamp: datetime,
        done_if_after: datetime,
        shard_counts: list[int] | None = None,
    ):
        """
        Issues a request to perform time travel operations on the remote storage
        """

        if shard_counts is None:
            shard_counts = []
        body: dict[str, Any] = {
            "shard_counts": shard_counts,
        }
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/time_travel_remote_storage?travel_to={timestamp.isoformat()}Z&done_if_after={done_if_after.isoformat()}Z",
            json=body,
        )
        self.verbose_error(res)

    def timeline_list(
        self,
        tenant_id: TenantId | TenantShardId,
        include_non_incremental_logical_size: bool = False,
        include_timeline_dir_layer_file_size_sum: bool = False,
    ) -> list[dict[str, Any]]:
        params = {}
        if include_non_incremental_logical_size:
            params["include-non-incremental-logical-size"] = "true"
        if include_timeline_dir_layer_file_size_sum:
            params["include-timeline-dir-layer-file-size-sum"] = "true"

        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline", params=params
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def timeline_and_offloaded_list(
        self,
        tenant_id: TenantId | TenantShardId,
    ) -> TimelinesInfoAndOffloaded:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline_and_offloaded",
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return TimelinesInfoAndOffloaded.from_json(res_json)

    def timeline_create(
        self,
        pg_version: PgVersion,
        tenant_id: TenantId | TenantShardId,
        new_timeline_id: TimelineId,
        ancestor_timeline_id: TimelineId | None = None,
        ancestor_start_lsn: Lsn | None = None,
        existing_initdb_timeline_id: TimelineId | None = None,
        **kwargs,
    ) -> dict[Any, Any]:
        body: dict[str, Any] = {
            "new_timeline_id": str(new_timeline_id),
        }
        if ancestor_timeline_id:
            body["ancestor_timeline_id"] = str(ancestor_timeline_id)
        if ancestor_start_lsn:
            body["ancestor_start_lsn"] = str(ancestor_start_lsn)
        if existing_initdb_timeline_id:
            body["existing_initdb_timeline_id"] = str(existing_initdb_timeline_id)
        if pg_version != PgVersion.NOT_SET:
            body["pg_version"] = int(pg_version)

        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline", json=body, **kwargs
        )
        if res.status_code == 409:
            raise TimelineCreate409(res)
        if res.status_code == 406:
            raise TimelineCreate406(res)

        self.verbose_error(res)

        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def timeline_detail(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        include_non_incremental_logical_size: bool = False,
        include_timeline_dir_layer_file_size_sum: bool = False,
        force_await_initial_logical_size: bool = False,
        **kwargs,
    ) -> dict[Any, Any]:
        params = {}
        if include_non_incremental_logical_size:
            params["include-non-incremental-logical-size"] = "true"
        if include_timeline_dir_layer_file_size_sum:
            params["include-timeline-dir-layer-file-size-sum"] = "true"
        if force_await_initial_logical_size:
            params["force-await-initial-logical-size"] = "true"

        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}",
            params=params,
            **kwargs,
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def timeline_delete(
        self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId, **kwargs
    ):
        """
        Note that deletion is not instant, it is scheduled and performed mostly in the background.
        So if you need to wait for it to complete use `timeline_delete_wait_completed`.
        For longer description consult with pageserver openapi spec.
        """
        res = self.delete(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}", **kwargs
        )
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_gc(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        gc_horizon: int | None,
    ) -> dict[str, Any]:
        """
        Unlike most handlers, this will wait for the layers to be actually
        complete registering themselves to the deletion queue.
        """
        self.is_testing_enabled_or_skip()

        log.info(
            f"Requesting GC: tenant {tenant_id}, timeline {timeline_id}, gc_horizon {repr(gc_horizon)}"
        )
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/do_gc",
            json={"gc_horizon": gc_horizon},
        )
        log.info(f"Got GC request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is not None
        assert isinstance(res_json, dict)
        return res_json

    def timeline_block_gc(self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/block_gc",
        )
        log.info(f"Got GC request response code: {res.status_code}")
        self.verbose_error(res)

    def timeline_unblock_gc(self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/unblock_gc",
        )
        log.info(f"Got GC request response code: {res.status_code}")
        self.verbose_error(res)

    def timeline_offload(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
    ):
        self.is_testing_enabled_or_skip()

        log.info(f"Requesting offload: tenant {tenant_id}, timeline {timeline_id}")
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/offload",
        )
        log.info(f"Got offload request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_compact_info(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
    ) -> Any:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/compact",
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_compact(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        force_repartition=False,
        force_image_layer_creation=False,
        force_l0_compaction=False,
        wait_until_uploaded=False,
        enhanced_gc_bottom_most_compaction=False,
        body: dict[str, Any] | None = None,
    ):
        query = {}
        if force_repartition:
            query["force_repartition"] = "true"
        if force_image_layer_creation:
            query["force_image_layer_creation"] = "true"
        if force_l0_compaction:
            query["force_l0_compaction"] = "true"
        if wait_until_uploaded:
            query["wait_until_uploaded"] = "true"
        if enhanced_gc_bottom_most_compaction:
            query["enhanced_gc_bottom_most_compaction"] = "true"

        log.info(f"Requesting compact: tenant {tenant_id}, timeline {timeline_id}")
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/compact",
            params=query,
            json=body,
        )
        log.info(f"Got compact request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_preserve_initdb_archive(
        self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId
    ):
        log.info(
            f"Requesting initdb archive preservation for tenant {tenant_id} and timeline {timeline_id}"
        )
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/preserve_initdb_archive",
        )
        self.verbose_error(res)

    def timeline_archival_config(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        state: TimelineArchivalState,
    ):
        config = {"state": state.value}
        log.info(
            f"requesting timeline archival config {config} for tenant {tenant_id} and timeline {timeline_id}"
        )
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/archival_config",
            json=config,
        )
        self.verbose_error(res)

    def timeline_get_lsn_by_timestamp(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        timestamp: datetime,
        with_lease: bool = False,
        **kwargs,
    ):
        log.info(
            f"Requesting lsn by timestamp {timestamp}, tenant {tenant_id}, timeline {timeline_id}, {with_lease=}"
        )
        with_lease_query = f"{with_lease=}".lower()
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/get_lsn_by_timestamp?timestamp={timestamp.isoformat()}Z&{with_lease_query}",
            **kwargs,
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_lsn_lease(
        self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId, lsn: Lsn
    ):
        data = {
            "lsn": str(lsn),
        }

        log.info(f"Requesting lsn lease for {lsn=}, {tenant_id=}, {timeline_id=}")
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/lsn_lease",
            json=data,
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_get_timestamp_of_lsn(
        self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId, lsn: Lsn
    ):
        log.info(f"Requesting time range of lsn {lsn}, tenant {tenant_id}, timeline {timeline_id}")
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/get_timestamp_of_lsn?lsn={lsn}",
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_layer_map_info(self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId):
        log.info(f"Requesting layer map info of tenant {tenant_id}, timeline {timeline_id}")
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer",
        )
        self.verbose_error(res)
        res_json = res.json()
        return res_json

    def timeline_checkpoint(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        force_repartition=False,
        force_image_layer_creation=False,
        force_l0_compaction=False,
        wait_until_flushed=True,
        wait_until_uploaded=False,
        compact: bool | None = None,
        **kwargs,
    ):
        self.is_testing_enabled_or_skip()
        query = {}
        if force_repartition:
            query["force_repartition"] = "true"
        if force_image_layer_creation:
            query["force_image_layer_creation"] = "true"
        if force_l0_compaction:
            query["force_l0_compaction"] = "true"
        if not wait_until_flushed:
            query["wait_until_flushed"] = "false"
        if wait_until_uploaded:
            query["wait_until_uploaded"] = "true"

        if compact is not None:
            query["compact"] = "true" if compact else "false"

        log.info(
            f"Requesting checkpoint: tenant={tenant_id} timeline={timeline_id} wait_until_flushed={wait_until_flushed} wait_until_uploaded={wait_until_uploaded} compact={compact}"
        )
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/checkpoint",
            params=query,
            **kwargs,
        )
        log.info(f"Got checkpoint request response code: {res.status_code}")
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is None

    def timeline_spawn_download_remote_layers(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        max_concurrent_downloads: int,
    ) -> dict[str, Any]:
        body = {
            "max_concurrent_downloads": max_concurrent_downloads,
        }
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/download_remote_layers",
            json=body,
        )
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is not None
        assert isinstance(res_json, dict)
        return res_json

    def timeline_poll_download_remote_layers_status(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        spawn_response: dict[str, Any],
        poll_state=None,
    ) -> None | dict[str, Any]:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/download_remote_layers",
        )
        self.verbose_error(res)
        res_json = res.json()
        assert res_json is not None
        assert isinstance(res_json, dict)

        # assumption in this API client here is that nobody else spawns the task
        assert res_json["task_id"] == spawn_response["task_id"]

        if poll_state is None or res_json["state"] == poll_state:
            return res_json
        return None

    def timeline_download_remote_layers(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        max_concurrent_downloads: int,
        errors_ok=False,
        at_least_one_download=True,
    ):
        res = self.timeline_spawn_download_remote_layers(
            tenant_id, timeline_id, max_concurrent_downloads
        )
        while True:
            completed = self.timeline_poll_download_remote_layers_status(
                tenant_id, timeline_id, res, poll_state="Completed"
            )
            if not completed:
                time.sleep(0.1)
                continue
            if not errors_ok:
                assert completed["failed_download_count"] == 0
            if at_least_one_download:
                assert completed["successful_download_count"] > 0
            return completed

    def get_metrics_str(self) -> str:
        """You probably want to use get_metrics() instead."""
        res = self.get(f"http://localhost:{self.port}/metrics")
        self.verbose_error(res)
        return res.text

    def get_metrics(self) -> Metrics:
        res = self.get_metrics_str()
        return parse_metrics(res)

    def get_timeline_metric(
        self, tenant_id: TenantId, timeline_id: TimelineId, metric_name: str
    ) -> float:
        metrics = self.get_metrics()
        return metrics.query_one(
            metric_name,
            filter={
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
            },
        ).value

    def get_remote_timeline_client_queue_count(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        file_kind: str,
        op_kind: str,
    ) -> int | None:
        metrics = [
            "pageserver_remote_timeline_client_calls_started_total",
            "pageserver_remote_timeline_client_calls_finished_total",
        ]
        res = self.get_metrics_values(
            metrics,
            filter={
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
                "file_kind": str(file_kind),
                "op_kind": str(op_kind),
            },
            absence_ok=True,
        )
        if len(res) != 2:
            return None
        inc, dec = (res[metric] for metric in metrics)
        queue_count = int(inc) - int(dec)
        assert queue_count >= 0
        return queue_count

    def layer_map_info(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
    ) -> LayerMapInfo:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/",
        )
        self.verbose_error(res)
        return LayerMapInfo.from_json(res.json())

    def timeline_layer_scan_disposable_keys(
        self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId, layer_name: str
    ) -> ScanDisposableKeysResponse:
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/{layer_name}/scan_disposable_keys",
        )
        self.verbose_error(res)
        assert res.status_code == 200
        return ScanDisposableKeysResponse.from_json(res.json())

    def download_layer(
        self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId, layer_name: str
    ):
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/{layer_name}",
        )
        self.verbose_error(res)

        assert res.status_code == 200

    def download_all_layers(self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId):
        info = self.layer_map_info(tenant_id, timeline_id)
        for layer in info.historic_layers:
            if not layer.remote:
                continue
            self.download_layer(tenant_id, timeline_id, layer.layer_file_name)

    def detach_ancestor(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
        batch_size: int | None = None,
        **kwargs,
    ) -> set[TimelineId]:
        params = {}
        if batch_size is not None:
            params["batch_size"] = batch_size
        res = self.put(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/detach_ancestor",
            params=params,
            **kwargs,
        )
        self.verbose_error(res)
        json = res.json()
        return set(map(TimelineId, json["reparented_timelines"]))

    def evict_layer(
        self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId, layer_name: str
    ):
        res = self.delete(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/layer/{layer_name}",
        )
        self.verbose_error(res)

        assert res.status_code in (200, 304)

    def evict_all_layers(self, tenant_id: TenantId | TenantShardId, timeline_id: TimelineId):
        info = self.layer_map_info(tenant_id, timeline_id)
        for layer in info.historic_layers:
            self.evict_layer(tenant_id, timeline_id, layer.layer_file_name)

    def disk_usage_eviction_run(self, request: dict[str, Any]):
        res = self.put(
            f"http://localhost:{self.port}/v1/disk_usage_eviction/run",
            json=request,
        )
        self.verbose_error(res)
        return res.json()

    def tenant_break(self, tenant_id: TenantId | TenantShardId):
        res = self.put(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/break")
        self.verbose_error(res)

    def post_tracing_event(self, level: str, message: str):
        res = self.post(
            f"http://localhost:{self.port}/v1/tracing/event",
            json={
                "level": level,
                "message": message,
            },
        )
        self.verbose_error(res)

    def deletion_queue_flush(self, execute: bool = False):
        self.put(
            f"http://localhost:{self.port}/v1/deletion_queue/flush?execute={'true' if execute else 'false'}"
        ).raise_for_status()

    def timeline_wait_logical_size(self, tenant_id: TenantId, timeline_id: TimelineId) -> int:
        detail = self.timeline_detail(
            tenant_id,
            timeline_id,
            include_non_incremental_logical_size=True,
            force_await_initial_logical_size=True,
        )
        current_logical_size = detail["current_logical_size"]
        non_incremental = detail["current_logical_size_non_incremental"]
        assert current_logical_size == non_incremental
        assert isinstance(current_logical_size, int)
        return current_logical_size

    def top_tenants(
        self, order_by: str, limit: int, where_shards_lt: int, where_gt: int
    ) -> dict[Any, Any]:
        res = self.post(
            f"http://localhost:{self.port}/v1/top_tenants",
            json={
                "order_by": order_by,
                "limit": limit,
                "where_shards_lt": where_shards_lt,
                "where_gt": where_gt,
            },
        )
        self.verbose_error(res)
        return res.json()  # type: ignore

    def perf_info(
        self,
        tenant_id: TenantId | TenantShardId,
        timeline_id: TimelineId,
    ):
        self.is_testing_enabled_or_skip()

        log.info(f"Requesting perf info: tenant {tenant_id}, timeline {timeline_id}")
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/perf_info",
        )
        log.info(f"Got perf info response code: {res.status_code}")
        self.verbose_error(res)
        return res.json()
