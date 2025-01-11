from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
import requests

from fixtures.common_types import Lsn, TenantId, TenantTimelineId, TimelineId
from fixtures.log_helper import log
from fixtures.metrics import Metrics, MetricsGetter, parse_metrics
from fixtures.utils import EnhancedJSONEncoder, wait_until

if TYPE_CHECKING:
    from typing import Any


# Walreceiver as returned by sk's timeline status endpoint.
@dataclass
class Walreceiver:
    conn_id: int
    state: str


@dataclass
class SafekeeperTimelineStatus:
    mconf: Configuration | None
    term: int
    last_log_term: int
    pg_version: int  # Not exactly a PgVersion, safekeeper returns version as int, for example 150002 for 15.2
    flush_lsn: Lsn
    commit_lsn: Lsn
    timeline_start_lsn: Lsn
    backup_lsn: Lsn
    peer_horizon_lsn: Lsn
    remote_consistent_lsn: Lsn
    walreceivers: list[Walreceiver]


class SafekeeperMetrics(Metrics):
    # Helpers to get metrics from tests without hardcoding the metric names there.
    # These are metrics from Prometheus which uses float64 internally.
    # As a consequence, values may differ from real original int64s.

    def __init__(self, m: Metrics):
        self.metrics = m.metrics

    def flush_lsn_inexact(self, tenant_id: TenantId, timeline_id: TimelineId):
        return self.query_one(
            "safekeeper_flush_lsn", {"tenant_id": str(tenant_id), "timeline_id": str(timeline_id)}
        ).value

    def commit_lsn_inexact(self, tenant_id: TenantId, timeline_id: TimelineId):
        return self.query_one(
            "safekeeper_commit_lsn", {"tenant_id": str(tenant_id), "timeline_id": str(timeline_id)}
        ).value


@dataclass
class TermBumpResponse:
    previous_term: int
    current_term: int

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> TermBumpResponse:
        return TermBumpResponse(
            previous_term=d["previous_term"],
            current_term=d["current_term"],
        )


@dataclass
class SafekeeperId:
    id: int
    host: str
    pg_port: int


@dataclass
class Configuration:
    generation: int
    members: list[SafekeeperId]
    new_members: list[SafekeeperId] | None

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> Configuration:
        generation = d["generation"]
        members = d["members"]
        new_members = d.get("new_members")
        return Configuration(generation, members, new_members)

    def to_json(self) -> str:
        return json.dumps(self, cls=EnhancedJSONEncoder)


@dataclass
class TimelineCreateRequest:
    tenant_id: TenantId
    timeline_id: TimelineId
    mconf: Configuration
    # not exactly PgVersion, for example 150002 for 15.2
    pg_version: int
    start_lsn: Lsn
    commit_lsn: Lsn | None

    def to_json(self) -> str:
        return json.dumps(self, cls=EnhancedJSONEncoder)


@dataclass
class TimelineMembershipSwitchResponse:
    previous_conf: Configuration
    current_conf: Configuration

    @classmethod
    def from_json(cls, d: dict[str, Any]) -> TimelineMembershipSwitchResponse:
        previous_conf = Configuration.from_json(d["previous_conf"])
        current_conf = Configuration.from_json(d["current_conf"])
        return TimelineMembershipSwitchResponse(previous_conf, current_conf)


class SafekeeperHttpClient(requests.Session, MetricsGetter):
    HTTPError = requests.HTTPError

    def __init__(self, port: int, auth_token: str | None = None, is_testing_enabled=False):
        super().__init__()
        self.port = port
        self.auth_token = auth_token
        self.is_testing_enabled = is_testing_enabled

        if auth_token is not None:
            self.headers["Authorization"] = f"Bearer {auth_token}"

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def get_metrics_str(self) -> str:
        """You probably want to use get_metrics() instead."""
        request_result = self.get(f"http://localhost:{self.port}/metrics")
        request_result.raise_for_status()
        return request_result.text

    def get_metrics(self) -> SafekeeperMetrics:
        res = self.get_metrics_str()
        return SafekeeperMetrics(parse_metrics(res))

    def is_testing_enabled_or_skip(self):
        if not self.is_testing_enabled:
            pytest.skip("safekeeper was built without 'testing' feature")

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
        res.raise_for_status()
        res_json = res.json()
        assert res_json is None
        return res_json

    def tenant_delete_force(self, tenant_id: TenantId) -> dict[Any, Any]:
        res = self.delete(f"http://localhost:{self.port}/v1/tenant/{tenant_id}")
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def timeline_list(self) -> list[TenantTimelineId]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/timeline")
        res.raise_for_status()
        resj = res.json()
        return [TenantTimelineId.from_json(ttidj) for ttidj in resj]

    def timeline_create(self, r: TimelineCreateRequest):
        res = self.post(f"http://localhost:{self.port}/v1/tenant/timeline", data=r.to_json())
        res.raise_for_status()

    def timeline_status(
        self, tenant_id: TenantId, timeline_id: TimelineId
    ) -> SafekeeperTimelineStatus:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}")
        res.raise_for_status()
        resj = res.json()
        walreceivers = [Walreceiver(wr["conn_id"], wr["status"]) for wr in resj["walreceivers"]]
        # It is always normally not None, it is allowed only to make forward compat tests happy.
        mconf = Configuration.from_json(resj["mconf"]) if "mconf" in resj else None
        return SafekeeperTimelineStatus(
            mconf=mconf,
            term=resj["acceptor_state"]["term"],
            last_log_term=resj["acceptor_state"]["epoch"],
            pg_version=resj["pg_info"]["pg_version"],
            flush_lsn=Lsn(resj["flush_lsn"]),
            commit_lsn=Lsn(resj["commit_lsn"]),
            timeline_start_lsn=Lsn(resj["timeline_start_lsn"]),
            backup_lsn=Lsn(resj["backup_lsn"]),
            peer_horizon_lsn=Lsn(resj["peer_horizon_lsn"]),
            remote_consistent_lsn=Lsn(resj["remote_consistent_lsn"]),
            walreceivers=walreceivers,
        )

    # Get timeline_start_lsn, waiting until it's nonzero. It is a way to ensure
    # that the timeline is fully initialized at the safekeeper.
    def get_non_zero_timeline_start_lsn(self, tenant_id: TenantId, timeline_id: TimelineId) -> Lsn:
        def timeline_start_lsn_non_zero() -> Lsn:
            s = self.timeline_status(tenant_id, timeline_id).timeline_start_lsn
            assert s > Lsn(0)
            return s

        return wait_until(timeline_start_lsn_non_zero)

    def get_commit_lsn(self, tenant_id: TenantId, timeline_id: TimelineId) -> Lsn:
        return self.timeline_status(tenant_id, timeline_id).commit_lsn

    # Get timeline membership configuration.
    def get_membership(self, tenant_id: TenantId, timeline_id: TimelineId) -> Configuration:
        # make mypy happy
        return self.timeline_status(tenant_id, timeline_id).mconf  # type: ignore

    # only_local doesn't remove segments in the remote storage.
    def timeline_delete(
        self, tenant_id: TenantId, timeline_id: TimelineId, only_local: bool = False
    ) -> dict[Any, Any]:
        res = self.delete(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}",
            params={
                "only_local": str(only_local).lower(),
            },
        )
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def debug_dump(self, params: dict[str, str] | None = None) -> dict[str, Any]:
        params = params or {}
        res = self.get(f"http://localhost:{self.port}/v1/debug_dump", params=params)
        res.raise_for_status()
        res_json = json.loads(res.text)
        assert isinstance(res_json, dict)
        return res_json

    def debug_dump_timeline(
        self, timeline_id: TimelineId, params: dict[str, str] | None = None
    ) -> Any:
        params = params or {}
        params["timeline_id"] = str(timeline_id)
        dump = self.debug_dump(params)
        return dump["timelines"][0]

    def get_partial_backup(self, timeline_id: TimelineId) -> Any:
        dump = self.debug_dump_timeline(timeline_id, {"dump_control_file": "true"})
        return dump["control_file"]["partial_backup"]

    def get_eviction_state(self, timeline_id: TimelineId) -> Any:
        dump = self.debug_dump_timeline(timeline_id, {"dump_control_file": "true"})
        return dump["control_file"]["eviction_state"]

    def pull_timeline(self, body: dict[str, Any]) -> dict[str, Any]:
        res = self.post(f"http://localhost:{self.port}/v1/pull_timeline", json=body)
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def membership_switch(
        self, tenant_id: TenantId, timeline_id: TimelineId, to: Configuration
    ) -> TimelineMembershipSwitchResponse:
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/membership",
            data=to.to_json(),
        )
        res.raise_for_status()
        return TimelineMembershipSwitchResponse.from_json(res.json())

    def copy_timeline(self, tenant_id: TenantId, timeline_id: TimelineId, body: dict[str, Any]):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/copy",
            json=body,
        )
        res.raise_for_status()

    def patch_control_file(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        patch: dict[str, Any],
    ) -> dict[str, Any]:
        res = self.patch(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/control_file",
            json={
                "updates": patch,
                "apply_fields": list(patch.keys()),
            },
        )
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def checkpoint(self, tenant_id: TenantId, timeline_id: TimelineId):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/checkpoint",
            json={},
        )
        res.raise_for_status()

    def timeline_digest(
        self, tenant_id: TenantId, timeline_id: TimelineId, from_lsn: Lsn, until_lsn: Lsn
    ) -> dict[str, Any]:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/digest",
            params={
                "from_lsn": str(from_lsn),
                "until_lsn": str(until_lsn),
            },
        )
        res.raise_for_status()
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def backup_partial_reset(self, tenant_id: TenantId, timeline_id: TimelineId):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/backup_partial_reset",
            json={},
        )
        res.raise_for_status()
        return res.json()

    def term_bump(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        term: int | None,
    ) -> TermBumpResponse:
        body = {}
        if term is not None:
            body["term"] = term
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id}/timeline/{timeline_id}/term_bump",
            json=body,
        )
        res.raise_for_status()
        return TermBumpResponse.from_json(res.json())

    def record_safekeeper_info(self, tenant_id: TenantId, timeline_id: TimelineId, body):
        res = self.post(
            f"http://localhost:{self.port}/v1/record_safekeeper_info/{tenant_id}/{timeline_id}",
            json=body,
        )
        res.raise_for_status()
