from __future__ import annotations

pytest_plugins = (
    "fixtures.pg_version",
    "fixtures.parametrize",
    "fixtures.h2server",
    "fixtures.httpserver",
    "fixtures.compute_reconfigure",
    "fixtures.storage_controller_proxy",
    "fixtures.paths",
    "fixtures.compute_migrations",
    "fixtures.neon_fixtures",
    "fixtures.benchmark_fixture",
    "fixtures.pg_stats",
    "fixtures.compare_fixtures",
    "fixtures.slow",
    "fixtures.reruns",
)
