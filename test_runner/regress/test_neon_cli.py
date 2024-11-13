from __future__ import annotations

import subprocess
from pathlib import Path
from typing import cast

import pytest
import requests
from fixtures.common_types import TenantId, TimelineId
from fixtures.neon_fixtures import (
    DEFAULT_BRANCH_NAME,
    NeonEnv,
    NeonEnvBuilder,
    parse_project_git_version_output,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.utils import run_only_on_default_postgres, skip_in_debug_build


def helper_compare_timeline_list(
    pageserver_http_client: PageserverHttpClient, env: NeonEnv, initial_tenant: TenantId
):
    """
    Compare timelines list returned by CLI and directly via API.
    Filters out timelines created by other tests.
    """

    timelines_api = sorted(
        map(
            lambda t: TimelineId(t["timeline_id"]),
            pageserver_http_client.timeline_list(initial_tenant),
        )
    )

    timelines_cli = env.neon_cli.timeline_list(initial_tenant)
    cli_timeline_ids = sorted([timeline_id for (_, timeline_id) in timelines_cli])
    assert timelines_api == cli_timeline_ids


def test_cli_timeline_list(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http_client = env.pageserver.http_client()

    # Initial sanity check
    helper_compare_timeline_list(pageserver_http_client, env, env.initial_tenant)

    # Create a branch for us
    main_timeline_id = env.create_branch("test_cli_branch_list_main")
    helper_compare_timeline_list(pageserver_http_client, env, env.initial_tenant)

    # Create a nested branch
    nested_timeline_id = env.create_branch(
        "test_cli_branch_list_nested", ancestor_branch_name="test_cli_branch_list_main"
    )
    helper_compare_timeline_list(pageserver_http_client, env, env.initial_tenant)

    # Check that all new branches are visible via CLI
    timelines_cli = [
        timeline_id for (_, timeline_id) in env.neon_cli.timeline_list(env.initial_tenant)
    ]

    assert main_timeline_id in timelines_cli
    assert nested_timeline_id in timelines_cli


def helper_compare_tenant_list(pageserver_http_client: PageserverHttpClient, env: NeonEnv):
    tenants = pageserver_http_client.tenant_list()
    tenants_api = sorted(map(lambda t: cast(str, t["id"]), tenants))

    res = env.neon_cli.tenant_list()
    tenants_cli = sorted(map(lambda t: t.split()[0], res.stdout.splitlines()))

    assert tenants_api == tenants_cli


def test_cli_tenant_list(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http_client = env.pageserver.http_client()
    # Initial sanity check
    helper_compare_tenant_list(pageserver_http_client, env)

    # Create new tenant
    tenant1, _ = env.create_tenant()

    # check tenant1 appeared
    helper_compare_tenant_list(pageserver_http_client, env)

    # Create new tenant
    tenant2, _ = env.create_tenant()

    # check tenant2 appeared
    helper_compare_tenant_list(pageserver_http_client, env)

    res = env.neon_cli.tenant_list()
    tenants = sorted(map(lambda t: TenantId(t.split()[0]), res.stdout.splitlines()))

    assert env.initial_tenant in tenants
    assert tenant1 in tenants
    assert tenant2 in tenants


def test_cli_tenant_create(neon_simple_env: NeonEnv):
    env = neon_simple_env
    tenant_id, _ = env.create_tenant()
    timelines = env.neon_cli.timeline_list(tenant_id)

    # an initial timeline should be created upon tenant creation
    assert len(timelines) == 1
    assert timelines[0][0] == DEFAULT_BRANCH_NAME


def test_cli_ipv4_listeners(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    # Connect to sk port on v4 loopback
    res = requests.get(f"http://127.0.0.1:{env.safekeepers[0].port.http}/v1/status")
    assert res.ok

    # FIXME Test setup is using localhost:xx in ps config.
    # Perhaps consider switching test suite to v4 loopback.

    # Connect to ps port on v4 loopback
    # res = requests.get(f'http://127.0.0.1:{env.pageserver.service_port.http}/v1/status')
    # assert res.ok


def test_cli_start_stop(neon_env_builder: NeonEnvBuilder):
    """
    Basic start/stop with default single-instance config for
    safekeeper and pageserver
    """
    env = neon_env_builder.init_start()

    # Stop default ps/sk
    env.neon_cli.pageserver_stop(env.pageserver.id)
    env.neon_cli.safekeeper_stop()
    env.neon_cli.storage_controller_stop(False)
    env.neon_cli.storage_broker_stop()

    # Keep NeonEnv state up to date, it usually owns starting/stopping services
    env.pageserver.running = False

    # Default start
    res = env.neon_cli.raw_cli(["start"])
    res.check_returncode()

    # Default stop
    res = env.neon_cli.raw_cli(["stop"])
    res.check_returncode()


def test_cli_start_stop_multi(neon_env_builder: NeonEnvBuilder):
    """
    Basic start/stop with explicitly configured counts of pageserver
    and safekeeper
    """
    neon_env_builder.num_pageservers = 2
    neon_env_builder.num_safekeepers = 2
    env = neon_env_builder.init_start()

    env.neon_cli.pageserver_stop(env.BASE_PAGESERVER_ID)
    env.neon_cli.pageserver_stop(env.BASE_PAGESERVER_ID + 1)

    # We will stop the storage controller while it may have requests in
    # flight, and the pageserver complains when requests are abandoned.
    for ps in env.pageservers:
        ps.allowed_errors.append(".*request was dropped before completing.*")

    # Keep NeonEnv state up to date, it usually owns starting/stopping services
    env.pageservers[0].running = False
    env.pageservers[1].running = False

    # Addressing a nonexistent ID throws
    with pytest.raises(RuntimeError):
        env.neon_cli.pageserver_stop(env.BASE_PAGESERVER_ID + 100)

    # Using the single-pageserver shortcut property throws when there are multiple pageservers
    with pytest.raises(AssertionError):
        _drop = env.pageserver

    env.neon_cli.safekeeper_stop(neon_env_builder.safekeepers_id_start + 1)
    env.neon_cli.safekeeper_stop(neon_env_builder.safekeepers_id_start + 2)

    # Stop this to get out of the way of the following `start`
    env.neon_cli.storage_controller_stop(False)
    env.neon_cli.storage_broker_stop()

    # Default start
    res = env.neon_cli.raw_cli(["start"])
    res.check_returncode()

    # Default stop
    res = env.neon_cli.raw_cli(["stop"])
    res.check_returncode()


@run_only_on_default_postgres(reason="does not use postgres")
@skip_in_debug_build("unit test for test support, either build works")
def test_parse_project_git_version_output_positive():
    commit = "b6f77b5816cf1dba12a3bc8747941182ce220846"

    positive = [
        # most likely when developing locally
        f"Neon CLI git:{commit}-modified",
        # when developing locally
        f"Neon CLI git:{commit}",
        # this is not produced in practice, but the impl supports it
        f"Neon CLI git-env:{commit}-modified",
        # most likely from CI or docker build
        f"Neon CLI git-env:{commit}",
    ]

    for example in positive:
        assert parse_project_git_version_output(example) == commit


@run_only_on_default_postgres(reason="does not use postgres")
@skip_in_debug_build("unit test for test support, either build works")
def test_parse_project_git_version_output_local_docker():
    """
    Makes sure the tests don't accept the default version in Dockerfile one gets without providing
    a commit lookalike in --build-arg GIT_VERSION=XXX
    """
    input = "Neon CLI git-env:local"

    with pytest.raises(ValueError) as e:
        parse_project_git_version_output(input)

    assert input in str(e)


@run_only_on_default_postgres(reason="does not use postgres")
@skip_in_debug_build("unit test for test support, either build works")
def test_binaries_version_parses(neon_binpath: Path):
    """
    Ensures that we can parse the actual outputs of --version from a set of binaries.

    The list is not meant to be exhaustive, and compute_ctl has a different way for example.
    """

    binaries = [
        "neon_local",
        "pageserver",
        "safekeeper",
        "proxy",
        "pg_sni_router",
        "storage_broker",
    ]
    for bin in binaries:
        out = subprocess.check_output([neon_binpath / bin, "--version"]).decode("utf-8")
        parse_project_git_version_output(out)
