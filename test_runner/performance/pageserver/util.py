"""
Utilities used by all code in this sub-directory
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import fixtures.pageserver.many_tenants as many_tenants
from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.utils import wait_until_all_tenants_state

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


def ensure_pageserver_ready_for_benchmarking(env: NeonEnv, n_tenants: int):
    """
    Helper function.
    """
    ps_http = env.pageserver.http_client()

    log.info("wait for all tenants to become active")
    wait_until_all_tenants_state(
        ps_http, "Active", iterations=10 + n_tenants, period=1, http_error_ok=False
    )

    # ensure all layers are resident for predictiable performance
    tenants = [info["id"] for info in ps_http.tenant_list()]
    for tenant in tenants:
        for timeline in ps_http.tenant_status(tenant)["timelines"]:
            info = ps_http.layer_map_info(tenant, timeline)
            for layer in info.historic_layers:
                assert not layer.remote

    log.info("ready")


def setup_pageserver_with_tenants(
    neon_env_builder: NeonEnvBuilder,
    name: str,
    n_tenants: int,
    setup: Callable[[NeonEnv], tuple[TenantId, TimelineId, dict[str, Any]]],
    timeout_in_seconds: int | None = None,
    extra_ps_env_vars: dict[str, str] | None = None,
) -> NeonEnv:
    """
    Utility function to set up a pageserver with a given number of identical tenants.
    """

    def doit(neon_env_builder: NeonEnvBuilder) -> NeonEnv:
        return many_tenants.single_timeline(neon_env_builder, setup, n_tenants)

    env = neon_env_builder.build_and_use_snapshot(name, doit)
    env.start(timeout_in_seconds=timeout_in_seconds, extra_ps_env_vars=extra_ps_env_vars)
    ensure_pageserver_ready_for_benchmarking(env, n_tenants)
    return env
