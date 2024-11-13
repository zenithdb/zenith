from __future__ import annotations

import uuid

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.utils import run_only_on_default_postgres, wait_until


@pytest.mark.parametrize("level", ["trace", "debug", "info", "warn", "error"])
@run_only_on_default_postgres("it does not use any postgres functionality")
def test_logging_event_count(neon_env_builder: NeonEnvBuilder, level: str):
    # self-test: make sure the event is logged (i.e., our testing endpoint works)
    log_expected = {
        "trace": False,
        "debug": False,
        "info": True,
        "warn": True,
        "error": True,
    }[level]

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()
    msg_id = uuid.uuid4().hex

    # NB: the _total suffix is added by our prometheus client
    before = ps_http.get_metric_value("libmetrics_tracing_event_count_total", {"level": level})

    # post the event
    ps_http.post_tracing_event(level, msg_id)
    if log_expected:
        env.pageserver.allowed_errors.append(f".*{msg_id}.*")

    def assert_logged():
        if not log_expected:
            return
        env.pageserver.assert_log_contains(f".*{msg_id}.*")

    wait_until(10, 0.5, assert_logged)

    # make sure it's counted
    def assert_metric_value():
        if not log_expected:
            return
        # NB: the _total suffix is added by our prometheus client
        val = ps_http.get_metric_value("libmetrics_tracing_event_count_total", {"level": level})
        val = val or 0.0
        log.info("libmetrics_tracing_event_count: %s", val)
        assert val > (before or 0.0)

    wait_until(10, 1, assert_metric_value)
