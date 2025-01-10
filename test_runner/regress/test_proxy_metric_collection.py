from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    PSQL,
    NeonProxy,
    VanillaPostgres,
)
from fixtures.port_distributor import PortDistributor
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

if TYPE_CHECKING:
    from fixtures.httpserver import ListenAddress


def proxy_metrics_handler(request: Request) -> Response:
    if request.json is None:
        return Response(status=400)

    events = request.json["events"]
    log.info("received events:")
    log.info(events)

    # perform basic sanity checks
    for event in events:
        assert event["metric"] == "proxy_io_bytes_per_client"
        assert event["endpoint_id"] == "test_endpoint_id"
        assert event["value"] >= 0
        assert event["stop_time"] >= event["start_time"]

    return Response(status=200)


@pytest.fixture(scope="function")
def proxy_with_metric_collector(
    port_distributor: PortDistributor,
    neon_binpath: Path,
    httpserver_listen_address: ListenAddress,
    test_output_dir: Path,
) -> Iterator[NeonProxy]:
    """Neon proxy that routes through link auth and has metric collection enabled."""

    http_port = port_distributor.get_port()
    proxy_port = port_distributor.get_port()
    mgmt_port = port_distributor.get_port()
    external_http_port = port_distributor.get_port()

    (host, port) = httpserver_listen_address
    metric_collection_endpoint = f"http://{host}:{port}/billing/api/v1/usage_events"
    metric_collection_interval = "5s"

    with NeonProxy(
        neon_binpath=neon_binpath,
        test_output_dir=test_output_dir,
        proxy_port=proxy_port,
        http_port=http_port,
        mgmt_port=mgmt_port,
        external_http_port=external_http_port,
        metric_collection_endpoint=metric_collection_endpoint,
        metric_collection_interval=metric_collection_interval,
        auth_backend=NeonProxy.Link(),
    ) as proxy:
        proxy.start()
        yield proxy


@pytest.mark.asyncio
async def test_proxy_metric_collection(
    httpserver: HTTPServer,
    proxy_with_metric_collector: NeonProxy,
    vanilla_pg: VanillaPostgres,
):
    # mock http server that returns OK for the metrics
    httpserver.expect_request("/billing/api/v1/usage_events", method="POST").respond_with_handler(
        proxy_metrics_handler
    )

    # do something to generate load to generate metrics
    # sleep for 5 seconds to give metric collector time to collect metrics
    psql = await PSQL(
        host=proxy_with_metric_collector.host, port=proxy_with_metric_collector.proxy_port
    ).run(
        "create table tbl as select * from generate_series(0,1000); select pg_sleep(5); select 42"
    )

    base_uri = proxy_with_metric_collector.link_auth_uri
    link = await NeonProxy.find_auth_link(base_uri, psql)

    psql_session_id = NeonProxy.get_session_id(base_uri, link)
    await NeonProxy.activate_link_auth(vanilla_pg, proxy_with_metric_collector, psql_session_id)

    assert psql.stdout is not None
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "42"

    # do something to generate load to generate metrics
    # sleep for 5 seconds to give metric collector time to collect metrics
    psql = await PSQL(
        host=proxy_with_metric_collector.host, port=proxy_with_metric_collector.proxy_port
    ).run("insert into tbl select * from generate_series(0,1000);  select pg_sleep(5); select 42")

    link = await NeonProxy.find_auth_link(base_uri, psql)
    psql_session_id = NeonProxy.get_session_id(base_uri, link)
    await NeonProxy.activate_link_auth(
        vanilla_pg, proxy_with_metric_collector, psql_session_id, create_user=False
    )

    assert psql.stdout is not None
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "42"

    httpserver.check()
