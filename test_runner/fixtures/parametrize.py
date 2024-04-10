import os
from typing import Optional

import allure
import pytest
from _pytest.python import Metafunc

from fixtures.pg_version import PgVersion

"""
Dynamically parametrize tests by different parameters
"""


@pytest.fixture(scope="function", autouse=True)
def pg_version() -> Optional[PgVersion]:
    return None


@pytest.fixture(scope="function", autouse=True)
def build_type() -> Optional[str]:
    return None


@pytest.fixture(scope="function", autouse=True)
def platform() -> Optional[str]:
    return None


@pytest.fixture(scope="function", autouse=True)
def pageserver_virtual_file_io_engine() -> Optional[str]:
    return os.getenv("PAGESERVER_VIRTUAL_FILE_IO_ENGINE")


def pytest_generate_tests(metafunc: Metafunc):
    if (bt := os.getenv("BUILD_TYPE")) is None:
        build_types = ["debug", "release"]
    else:
        build_types = [bt.lower()]

    metafunc.parametrize("build_type", build_types)

    if (v := os.getenv("DEFAULT_PG_VERSION")) is None:
        pg_versions = [version for version in PgVersion if version != PgVersion.NOT_SET]
    else:
        pg_versions = [PgVersion(v)]

    metafunc.parametrize("pg_version", pg_versions, ids=map(lambda v: f"pg{v}", pg_versions))

    # A hacky way to parametrize tests only for `pageserver_virtual_file_io_engine=std-fs`
    # And do not change test name for default `pageserver_virtual_file_io_engine=tokio-epoll-uring` to keep tests statistics
    if (io_engine := os.getenv("PAGESERVER_VIRTUAL_FILE_IO_ENGINE", "")) not in (
        "",
        "tokio-epoll-uring",
    ):
        metafunc.parametrize("pageserver_virtual_file_io_engine", [io_engine])

    # For performance tests, parametrize also by platform
    if (
        "test_runner/performance" in metafunc.definition._nodeid
        and (platform := os.getenv("PLATFORM")) is not None
    ):
        metafunc.parametrize("platform", [platform.lower()])


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(*args, **kwargs):
    if (build_type := os.getenv("BUILD_TYPE")) is not None:
        allure.dynamic.parameter("__BUILD_TYPE", build_type)

    if (pg_version := os.getenv("DEFAULT_PG_VERSION")) is not None:
        allure.dynamic.parameter("__DEFAULT_PG_VERSION", int(pg_version))

    if (io_engine := os.getenv("PAGESERVER_VIRTUAL_FILE_IO_ENGINE")) is not None:
        allure.dynamic.parameter("__PAGESERVER_VIRTUAL_FILE_IO_ENGINE", io_engine)

    if (platform := os.getenv("PLATFORM")) is not None:
        allure.dynamic.parameter("__PLATFORM", platform)

    if (github_run_id := os.getenv("GITHUB_RUN_ID")) is not None:
        allure.dynamic.parameter(
            "__GITHUB_RUN_ID",
            int(github_run_id),
            excluded=True,
        )
    if (github_run_attempt := os.getenv("GITHUB_RUN_ATTEMPT")) is not None:
        allure.dynamic.parameter(
            "__GITHUB_RUN_ATTEMPT",
            int(github_run_attempt),
            excluded=True,
        )

    yield
