from __future__ import annotations

import abc
import asyncio
import concurrent.futures
import filecmp
import json
import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import threading
import time
import uuid
from collections import defaultdict
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from fcntl import LOCK_EX, LOCK_UN, flock
from functools import cached_property, partial
from itertools import chain, product
from pathlib import Path
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import quote, urlparse

import asyncpg
import backoff
import httpx
import psycopg2
import psycopg2.sql
import pytest
import requests
import toml
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.fixtures import FixtureRequest

# Type-related stuff
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor
from psycopg2.extensions import make_dsn, parse_dsn
from urllib3.util.retry import Retry

from fixtures import overlayfs
from fixtures.auth_tokens import AuthKeys, TokenScope
from fixtures.broker import NeonBroker
from fixtures.common_types import Lsn, NodeId, TenantId, TenantShardId, TimelineId
from fixtures.endpoint.http import EndpointHttpClient
from fixtures.log_helper import log
from fixtures.metrics import Metrics, MetricsGetter, parse_metrics
from fixtures.pageserver.allowed_errors import (
    DEFAULT_PAGESERVER_ALLOWED_ERRORS,
    DEFAULT_STORAGE_CONTROLLER_ALLOWED_ERRORS,
)
from fixtures.pageserver.common_types import IndexPartDump, LayerName, parse_layer_file_name
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import (
    LocalFsStorage,
    MockS3Server,
    RemoteStorage,
    RemoteStorageKind,
    RemoteStorageUser,
    S3Storage,
    default_remote_storage,
    remote_storage_to_toml_dict,
)
from fixtures.safekeeper.http import SafekeeperHttpClient
from fixtures.safekeeper.utils import are_walreceivers_absent
from fixtures.utils import (
    ATTACHMENT_NAME_REGEX,
    allure_add_grafana_links,
    allure_attach_from_dir,
    assert_no_errors,
    get_dir_size,
    get_self_dir,
    print_gc_result,
    subprocess_capture,
    wait_until,
)
from fixtures.utils import AuxFileStore as AuxFileStore  # reexport

from .neon_api import NeonAPI, NeonApiEndpoint

T = TypeVar("T")

"""
This file contains pytest fixtures. A fixture is a test resource that can be
summoned by placing its name in the test's arguments.

A fixture is created with the decorator @pytest.fixture decorator.
See docs: https://docs.pytest.org/en/6.2.x/fixture.html

There are several environment variables that can control the running of tests:
NEON_BIN, POSTGRES_DISTRIB_DIR, etc. See README.md for more information.

There's no need to import this file to use it. It should be declared as a plugin
inside conftest.py, and that makes it available to all tests.

Don't import functions from this file, or pytest will emit warnings. Instead
put directly-importable functions into utils.py or another separate file.
"""

Env = Dict[str, str]

DEFAULT_OUTPUT_DIR: str = "test_output"
DEFAULT_BRANCH_NAME: str = "main"

BASE_PORT: int = 15000


@pytest.fixture(scope="session")
def base_dir() -> Iterator[Path]:
    # find the base directory (currently this is the git root)
    base_dir = get_self_dir().parent.parent
    log.info(f"base_dir is {base_dir}")

    yield base_dir


@pytest.fixture(scope="function")
def neon_binpath(base_dir: Path, build_type: str) -> Iterator[Path]:
    if os.getenv("REMOTE_ENV"):
        # we are in remote env and do not have neon binaries locally
        # this is the case for benchmarks run on self-hosted runner
        return

    # Find the neon binaries.
    if env_neon_bin := os.environ.get("NEON_BIN"):
        binpath = Path(env_neon_bin)
    else:
        binpath = base_dir / "target" / build_type
    log.info(f"neon_binpath is {binpath}")

    if not (binpath / "pageserver").exists():
        raise Exception(f"neon binaries not found at '{binpath}'")

    yield binpath


@pytest.fixture(scope="session")
def pg_distrib_dir(base_dir: Path) -> Iterator[Path]:
    if env_postgres_bin := os.environ.get("POSTGRES_DISTRIB_DIR"):
        distrib_dir = Path(env_postgres_bin).resolve()
    else:
        distrib_dir = base_dir / "pg_install"

    log.info(f"pg_distrib_dir is {distrib_dir}")
    yield distrib_dir


@pytest.fixture(scope="session")
def top_output_dir(base_dir: Path) -> Iterator[Path]:
    # Compute the top-level directory for all tests.
    if env_test_output := os.environ.get("TEST_OUTPUT"):
        output_dir = Path(env_test_output).resolve()
    else:
        output_dir = base_dir / DEFAULT_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True)

    log.info(f"top_output_dir is {output_dir}")
    yield output_dir


@pytest.fixture(scope="session")
def neon_api_key() -> str:
    api_key = os.getenv("NEON_API_KEY")
    if not api_key:
        raise AssertionError("Set the NEON_API_KEY environment variable")

    return api_key


@pytest.fixture(scope="session")
def neon_api_base_url() -> str:
    return os.getenv("NEON_API_BASE_URL", "https://console-stage.neon.build/api/v2")


@pytest.fixture(scope="session")
def neon_api(neon_api_key: str, neon_api_base_url: str) -> NeonAPI:
    return NeonAPI(neon_api_key, neon_api_base_url)


@pytest.fixture(scope="session")
def worker_port_num():
    return (32768 - BASE_PORT) // int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1"))


@pytest.fixture(scope="session")
def worker_seq_no(worker_id: str) -> int:
    # worker_id is a pytest-xdist fixture
    # it can be master or gw<number>
    # parse it to always get a number
    if worker_id == "master":
        return 0
    assert worker_id.startswith("gw")
    return int(worker_id[2:])


@pytest.fixture(scope="session")
def worker_base_port(worker_seq_no: int, worker_port_num: int) -> int:
    # so we divide ports in ranges of ports
    # so workers have disjoint set of ports for services
    return BASE_PORT + worker_seq_no * worker_port_num


@pytest.fixture(scope="session")
def port_distributor(worker_base_port: int, worker_port_num: int) -> PortDistributor:
    return PortDistributor(base_port=worker_base_port, port_number=worker_port_num)


@pytest.fixture(scope="function")
def default_broker(
    port_distributor: PortDistributor,
    test_output_dir: Path,
    neon_binpath: Path,
) -> Iterator[NeonBroker]:
    # multiple pytest sessions could get launched in parallel, get them different ports/datadirs
    client_port = port_distributor.get_port()
    broker_logfile = test_output_dir / "repo" / "storage_broker.log"

    broker = NeonBroker(logfile=broker_logfile, port=client_port, neon_binpath=neon_binpath)
    yield broker
    broker.stop()


@pytest.fixture(scope="session")
def run_id() -> Iterator[uuid.UUID]:
    yield uuid.uuid4()


@pytest.fixture(scope="session")
def mock_s3_server(port_distributor: PortDistributor) -> Iterator[MockS3Server]:
    mock_s3_server = MockS3Server(port_distributor.get_port())
    yield mock_s3_server
    mock_s3_server.kill()


class PgProtocol:
    """Reusable connection logic"""

    def __init__(self, **kwargs: Any):
        self.default_options = kwargs

    def connstr(self, **kwargs: Any) -> str:
        """
        Build a libpq connection string for the Postgres instance.
        """
        return str(make_dsn(**self.conn_options(**kwargs)))

    def conn_options(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Construct a dictionary of connection options from default values and extra parameters.
        An option can be dropped from the returning dictionary by None-valued extra parameter.
        """
        result = self.default_options.copy()
        if "dsn" in kwargs:
            result.update(parse_dsn(kwargs["dsn"]))
        result.update(kwargs)
        result = {k: v for k, v in result.items() if v is not None}

        # Individual statement timeout in seconds. 2 minutes should be
        # enough for our tests, but if you need a longer, you can
        # change it by calling "SET statement_timeout" after
        # connecting.
        options = result.get("options", "")
        if "statement_timeout" not in options:
            options = f"-cstatement_timeout=120s {options}"
        result["options"] = options
        return result

    # autocommit=True here by default because that's what we need most of the time
    def connect(self, autocommit: bool = True, **kwargs: Any) -> PgConnection:
        """
        Connect to the node.
        Returns psycopg2's connection object.
        This method passes all extra params to connstr.
        """
        conn: PgConnection = psycopg2.connect(**self.conn_options(**kwargs))

        # WARNING: this setting affects *all* tests!
        conn.autocommit = autocommit
        return conn

    @contextmanager
    def cursor(self, autocommit: bool = True, **kwargs: Any) -> Iterator[PgCursor]:
        """
        Shorthand for pg.connect().cursor().
        The cursor and connection are closed when the context is exited.
        """
        with closing(self.connect(autocommit=autocommit, **kwargs)) as conn:
            yield conn.cursor()

    async def connect_async(self, **kwargs: Any) -> asyncpg.Connection:
        """
        Connect to the node from async python.
        Returns asyncpg's connection object.
        """

        # asyncpg takes slightly different options than psycopg2. Try
        # to convert the defaults from the psycopg2 format.

        # The psycopg2 option 'dbname' is called 'database' is asyncpg
        conn_options = self.conn_options(**kwargs)
        if "dbname" in conn_options:
            conn_options["database"] = conn_options.pop("dbname")

        # Convert options='-c<key>=<val>' to server_settings
        if "options" in conn_options:
            options = conn_options.pop("options")
            for match in re.finditer(r"-c(\w*)=(\w*)", options):
                key = match.group(1)
                val = match.group(2)
                if "server_options" in conn_options:
                    conn_options["server_settings"].update({key: val})
                else:
                    conn_options["server_settings"] = {key: val}
        return await asyncpg.connect(**conn_options)

    def safe_psql(self, query: str, **kwargs: Any) -> List[Tuple[Any, ...]]:
        """
        Execute query against the node and return all rows.
        This method passes all extra params to connstr.
        """
        return self.safe_psql_many([query], **kwargs)[0]

    def safe_psql_many(
        self, queries: Iterable[str], log_query=True, **kwargs: Any
    ) -> List[List[Tuple[Any, ...]]]:
        """
        Execute queries against the node and return all rows.
        This method passes all extra params to connstr.
        """
        result: List[List[Any]] = []
        with closing(self.connect(**kwargs)) as conn:
            with conn.cursor() as cur:
                for query in queries:
                    if log_query:
                        log.info(f"Executing query: {query}")
                    cur.execute(query)

                    if cur.description is None:
                        result.append([])  # query didn't return data
                    else:
                        result.append(cur.fetchall())
        return result

    def safe_psql_scalar(self, query, log_query=True) -> Any:
        """
        Execute query returning single row with single column.
        """
        return self.safe_psql(query, log_query=log_query)[0][0]


class NeonEnvBuilder:
    """
    Builder object to create a Neon runtime environment

    You should use the `neon_env_builder` or `neon_simple_env` pytest
    fixture to create the NeonEnv object. That way, the repository is
    created in the right directory, based on the test name, and it's properly
    cleaned up after the test has finished.
    """

    def __init__(
        self,
        repo_dir: Path,
        port_distributor: PortDistributor,
        broker: NeonBroker,
        run_id: uuid.UUID,
        mock_s3_server: MockS3Server,
        neon_binpath: Path,
        pg_distrib_dir: Path,
        pg_version: PgVersion,
        test_name: str,
        top_output_dir: Path,
        test_output_dir: Path,
        test_overlay_dir: Optional[Path] = None,
        pageserver_remote_storage: Optional[RemoteStorage] = None,
        # toml that will be decomposed into `--config-override` flags during `pageserver --init`
        pageserver_config_override: Optional[str | Callable[[Dict[str, Any]], None]] = None,
        num_safekeepers: int = 1,
        num_pageservers: int = 1,
        # Use non-standard SK ids to check for various parsing bugs
        safekeepers_id_start: int = 0,
        # fsync is disabled by default to make the tests go faster
        safekeepers_enable_fsync: bool = False,
        auth_enabled: bool = False,
        rust_log_override: Optional[str] = None,
        default_branch_name: str = DEFAULT_BRANCH_NAME,
        preserve_database_files: bool = False,
        initial_tenant: Optional[TenantId] = None,
        initial_timeline: Optional[TimelineId] = None,
        pageserver_virtual_file_io_engine: Optional[str] = None,
        pageserver_aux_file_policy: Optional[AuxFileStore] = None,
        pageserver_default_tenant_config_compaction_algorithm: Optional[Dict[str, Any]] = None,
        safekeeper_extra_opts: Optional[list[str]] = None,
        storage_controller_port_override: Optional[int] = None,
        pageserver_io_buffer_alignment: Optional[int] = None,
    ):
        self.repo_dir = repo_dir
        self.rust_log_override = rust_log_override
        self.port_distributor = port_distributor

        # Pageserver remote storage
        self.pageserver_remote_storage = pageserver_remote_storage
        # Safekeepers remote storage
        self.safekeepers_remote_storage: Optional[RemoteStorage] = None

        self.broker = broker
        self.run_id = run_id
        self.mock_s3_server: MockS3Server = mock_s3_server
        self.pageserver_config_override = pageserver_config_override
        self.num_safekeepers = num_safekeepers
        self.num_pageservers = num_pageservers
        self.safekeepers_id_start = safekeepers_id_start
        self.safekeepers_enable_fsync = safekeepers_enable_fsync
        self.auth_enabled = auth_enabled
        self.default_branch_name = default_branch_name
        self.env: Optional[NeonEnv] = None
        self.keep_remote_storage_contents: bool = True
        self.neon_binpath = neon_binpath
        self.neon_local_binpath = neon_binpath
        self.pg_distrib_dir = pg_distrib_dir
        self.pg_version = pg_version
        self.preserve_database_files = preserve_database_files
        self.initial_tenant = initial_tenant or TenantId.generate()
        self.initial_timeline = initial_timeline or TimelineId.generate()
        self.enable_scrub_on_exit = True
        self.test_output_dir = test_output_dir
        self.test_overlay_dir = test_overlay_dir
        self.overlay_mounts_created_by_us: List[Tuple[str, Path]] = []
        self.config_init_force: Optional[str] = None
        self.top_output_dir = top_output_dir
        self.control_plane_compute_hook_api: Optional[str] = None
        self.storage_controller_config: Optional[dict[Any, Any]] = None

        self.pageserver_virtual_file_io_engine: Optional[str] = pageserver_virtual_file_io_engine

        self.pageserver_default_tenant_config_compaction_algorithm: Optional[
            Dict[str, Any]
        ] = pageserver_default_tenant_config_compaction_algorithm
        if self.pageserver_default_tenant_config_compaction_algorithm is not None:
            log.debug(
                f"Overriding pageserver default compaction algorithm to {self.pageserver_default_tenant_config_compaction_algorithm}"
            )

        self.pageserver_aux_file_policy = pageserver_aux_file_policy

        self.safekeeper_extra_opts = safekeeper_extra_opts

        self.storage_controller_port_override = storage_controller_port_override

        self.pageserver_io_buffer_alignment = pageserver_io_buffer_alignment

        assert test_name.startswith(
            "test_"
        ), "Unexpectedly instantiated from outside a test function"
        self.test_name = test_name

    def init_configs(self, default_remote_storage_if_missing: bool = True) -> NeonEnv:
        # Cannot create more than one environment from one builder
        assert self.env is None, "environment already initialized"
        if default_remote_storage_if_missing and self.pageserver_remote_storage is None:
            self.enable_pageserver_remote_storage(default_remote_storage())
        self.env = NeonEnv(self)
        return self.env

    def init_start(
        self,
        initial_tenant_conf: Optional[Dict[str, Any]] = None,
        default_remote_storage_if_missing: bool = True,
        initial_tenant_shard_count: Optional[int] = None,
        initial_tenant_shard_stripe_size: Optional[int] = None,
    ) -> NeonEnv:
        """
        Default way to create and start NeonEnv. Also creates the initial_tenant with root initial_timeline.

        To avoid creating initial_tenant, call init_configs to setup the environment.

        Configuring pageserver with remote storage is now the default. There will be a warning if pageserver is created without one.
        """
        env = self.init_configs(default_remote_storage_if_missing=default_remote_storage_if_missing)
        env.start()

        # Prepare the default branch to start the postgres on later.
        # Pageserver itself does not create tenants and timelines, until started first and asked via HTTP API.
        log.debug(
            f"Services started, creating initial tenant {env.initial_tenant} and its initial timeline"
        )
        initial_tenant, initial_timeline = env.neon_cli.create_tenant(
            tenant_id=env.initial_tenant,
            conf=initial_tenant_conf,
            timeline_id=env.initial_timeline,
            shard_count=initial_tenant_shard_count,
            shard_stripe_size=initial_tenant_shard_stripe_size,
            aux_file_policy=self.pageserver_aux_file_policy,
        )
        assert env.initial_tenant == initial_tenant
        assert env.initial_timeline == initial_timeline
        log.info(f"Initial timeline {initial_tenant}/{initial_timeline} created successfully")

        return env

    def build_and_use_snapshot(
        self, global_ident: str, create_env_for_snapshot: Callable[[NeonEnvBuilder], NeonEnv]
    ) -> NeonEnv:
        if os.getenv("CI", "false") == "true":
            log.info("do not use snapshots in ephemeral CI environment")
            env = create_env_for_snapshot(self)
            env.stop(immediate=True, ps_assert_metric_no_errors=False)
            return env

        with shared_snapshot_dir(self.top_output_dir, global_ident) as snapshot_dir:
            if not snapshot_dir.is_initialized():
                self._build_and_use_snapshot_impl(snapshot_dir, create_env_for_snapshot)
                assert snapshot_dir.is_initialized()

            return self.from_repo_dir(snapshot_dir.path)

    def _build_and_use_snapshot_impl(
        self,
        snapshot_dir: SnapshotDirLocked,
        create_env_for_snapshot: Callable[[NeonEnvBuilder], NeonEnv],
    ):
        if snapshot_dir.path.exists():
            shutil.rmtree(snapshot_dir.path)

        if self.test_overlay_dir is not None:
            # Make repo_dir an overlayfs mount with lowerdir being the empty snapshot_dir.
            # When we're done filling up repo_dir, tear everything down, unmount the overlayfs, and use
            # the upperdir as the snapshot. This is equivalent to docker `FROM scratch`.
            assert not self.repo_dir.exists()
            assert self.repo_dir.parent.exists()
            snapshot_dir.path.mkdir()
            self.overlay_mount("create-snapshot-repo-dir", snapshot_dir.path, self.repo_dir)
            self.config_init_force = "empty-dir-ok"

        env = create_env_for_snapshot(self)
        assert self.env is not None
        assert self.env == env

        # shut down everything for snapshot
        env.stop(immediate=True, ps_assert_metric_no_errors=True)

        # TODO: all kinds of assertions to ensure the env is unused

        if self.test_overlay_dir is None:
            log.info("take snapshot by moving repo dir")
            env.repo_dir.rename(snapshot_dir.path)
        else:
            log.info("take snapshot by using overlayfs upperdir")
            self.overlay_unmount_and_move("create-snapshot-repo-dir", snapshot_dir.path)
            log.info("remove empty repo_dir (previously mountpoint) for snapshot overlay_mount")
            env.repo_dir.rmdir()
            # TODO from here on, we should be able to reset / goto top where snapshot_dir.is_initialized()
            log.info("make repo_dir an overlayfs mount of the snapshot we just created")
        assert not env.repo_dir.exists(), "both branches above should remove it"
        snapshot_dir.set_initialized()

        self.env = None  # so that from_repo_dir works again

    def from_repo_dir(
        self,
        repo_dir: Path,
    ) -> NeonEnv:
        """
        A simple method to import data into the current NeonEnvBuilder from a snapshot of a repo dir.
        """

        # Get the initial tenant and timeline from the snapshot config
        snapshot_config_toml = repo_dir / "config"
        with snapshot_config_toml.open("r") as f:
            snapshot_config = toml.load(f)

        self.initial_tenant = TenantId(snapshot_config["default_tenant_id"])
        self.initial_timeline = TimelineId(
            dict(snapshot_config["branch_name_mappings"][DEFAULT_BRANCH_NAME])[
                str(self.initial_tenant)
            ]
        )
        self.env = self.init_configs()

        for ps_dir in repo_dir.glob("pageserver_*"):
            tenants_from_dir = ps_dir / "tenants"
            tenants_to_dir = self.repo_dir / ps_dir.name / "tenants"

            if self.test_overlay_dir is None:
                log.info(
                    f"Copying pageserver tenants directory {tenants_from_dir} to {tenants_to_dir}"
                )
                shutil.copytree(tenants_from_dir, tenants_to_dir)
            else:
                log.info(
                    f"Creating overlayfs mount of pageserver tenants directory {tenants_from_dir} to {tenants_to_dir}"
                )
                self.overlay_mount(f"{ps_dir.name}:tenants", tenants_from_dir, tenants_to_dir)

        for sk_from_dir in (repo_dir / "safekeepers").glob("sk*"):
            sk_to_dir = self.repo_dir / "safekeepers" / sk_from_dir.name
            log.info(f"Copying safekeeper directory {sk_from_dir} to {sk_to_dir}")
            sk_to_dir.rmdir()
            shutil.copytree(sk_from_dir, sk_to_dir, ignore=shutil.ignore_patterns("*.log", "*.pid"))

        shutil.rmtree(self.repo_dir / "local_fs_remote_storage", ignore_errors=True)
        if self.test_overlay_dir is None:
            log.info("Copying local_fs_remote_storage directory from snapshot")
            shutil.copytree(
                repo_dir / "local_fs_remote_storage", self.repo_dir / "local_fs_remote_storage"
            )
        else:
            log.info("Creating overlayfs mount of local_fs_remote_storage directory from snapshot")
            self.overlay_mount(
                "local_fs_remote_storage",
                repo_dir / "local_fs_remote_storage",
                self.repo_dir / "local_fs_remote_storage",
            )

        # restore storage controller (the db is small, don't bother with overlayfs)
        storcon_db_from_dir = repo_dir / "storage_controller_db"
        storcon_db_to_dir = self.repo_dir / "storage_controller_db"
        log.info(f"Copying storage_controller_db from {storcon_db_from_dir} to {storcon_db_to_dir}")
        assert storcon_db_from_dir.is_dir()
        assert not storcon_db_to_dir.exists()

        def ignore_postgres_log(path: str, _names):
            if Path(path) == storcon_db_from_dir:
                return {"postgres.log"}
            return set()

        shutil.copytree(storcon_db_from_dir, storcon_db_to_dir, ignore=ignore_postgres_log)
        assert not (storcon_db_to_dir / "postgres.log").exists()
        # NB: neon_local rewrites postgresql.conf on each start based on neon_local config. No need to patch it.
        # However, in this new NeonEnv, the pageservers listen on different ports, and the storage controller
        # will currently reject re-attach requests from them because the NodeMetadata isn't identical.
        # So, from_repo_dir patches up the the storcon database.
        patch_script_path = self.repo_dir / "storage_controller_db.startup.sql"
        assert not patch_script_path.exists()
        patch_script = ""
        for ps in self.env.pageservers:
            patch_script += f"UPDATE nodes SET listen_http_port={ps.service_port.http}, listen_pg_port={ps.service_port.pg}  WHERE node_id = '{ps.id}';"
            # This is a temporary to get the backward compat test happy
            # since the compat snapshot was generated with an older version of neon local
            patch_script += f"UPDATE nodes SET availability_zone_id='{ps.az_id}'  WHERE node_id = '{ps.id}' AND availability_zone_id IS NULL;"
        patch_script_path.write_text(patch_script)

        # Update the config with info about tenants and timelines
        with (self.repo_dir / "config").open("r") as f:
            config = toml.load(f)

        config["default_tenant_id"] = snapshot_config["default_tenant_id"]
        config["branch_name_mappings"] = snapshot_config["branch_name_mappings"]

        # Update the config with new neon + postgres path in case of compat test
        config["pg_distrib_dir"] = str(self.pg_distrib_dir)
        config["neon_distrib_dir"] = str(self.neon_binpath)

        with (self.repo_dir / "config").open("w") as f:
            toml.dump(config, f)

        return self.env

    def overlay_mount(self, ident: str, srcdir: Path, dstdir: Path):
        """
        Mount `srcdir` as an overlayfs mount at `dstdir`.
        The overlayfs `upperdir` and `workdir` will be placed in test_overlay_dir.
        """
        assert self.test_overlay_dir
        assert (
            self.test_output_dir in dstdir.parents
        )  # so that teardown & test_overlay_dir fixture work
        assert srcdir.is_dir()
        dstdir.mkdir(exist_ok=False, parents=False)
        ident_state_dir = self.test_overlay_dir / ident
        upper = ident_state_dir / "upper"
        work = ident_state_dir / "work"
        ident_state_dir.mkdir(
            exist_ok=False, parents=False
        )  # exists_ok=False also checks uniqueness in self.overlay_mounts
        upper.mkdir()
        work.mkdir()
        cmd = [
            "sudo",
            "mount",
            "-t",
            "overlay",
            "overlay",
            "-o",
            f"lowerdir={srcdir},upperdir={upper},workdir={work}",
            str(dstdir),
        ]
        log.info(f"Mounting overlayfs srcdir={srcdir} dstdir={dstdir}: {cmd}")
        subprocess_capture(
            self.test_output_dir, cmd, check=True, echo_stderr=True, echo_stdout=True
        )
        self.overlay_mounts_created_by_us.append((ident, dstdir))

    def _overlay_umount(self, mountpoint: Path):
        cmd = ["sudo", "umount", str(mountpoint)]
        assert mountpoint.is_mount()
        subprocess_capture(
            self.test_output_dir, cmd, check=True, echo_stderr=True, echo_stdout=True
        )

    def overlay_unmount_and_move(self, ident: str, dst: Path):
        """
        Unmount previously established overlayfs mount at `dstdir` and move the upperdir contents to `dst`.
        If `dst` is an empty directory, it gets replaced.
        Caller is responsible for ensuring the unmount will succeed, i.e., that there aren't any nested mounts.

        Raises exception if self.test_overlay_dir is None
        """
        assert self.test_overlay_dir is not None
        # not mutating state yet, make checks
        ident_state_dir = self.test_overlay_dir / ident
        assert ident_state_dir.is_dir()
        upper = ident_state_dir / "upper"
        work = ident_state_dir / "work"
        assert upper.is_dir()
        assert work.is_dir()
        assert (
            self.test_overlay_dir not in dst.parents
        ), "otherwise workdir cleanup below wouldn't work"
        # find index, still not mutating state
        idxmap = {
            existing_ident: idx
            for idx, (existing_ident, _) in enumerate(self.overlay_mounts_created_by_us)
        }
        idx = idxmap.get(ident)
        if idx is None:
            raise RuntimeError(f"cannot find mount for ident {ident}")

        if dst.is_dir():
            dst.rmdir()  # raises exception if not empty, which is what we want

        _, mountpoint = self.overlay_mounts_created_by_us.pop(idx)
        self._overlay_umount(mountpoint)
        upper.rename(dst)
        # we moved the upperdir, clean up workdir and then its parent ident_state_dir
        cmd = ["sudo", "rm", "-rf", str(work)]
        subprocess_capture(
            self.test_output_dir, cmd, check=True, echo_stderr=True, echo_stdout=True
        )
        ident_state_dir.rmdir()  # should be empty since we moved `upper` out

    def disable_scrub_on_exit(self):
        """
        Some tests intentionally leave the remote storage contents empty or corrupt,
        so it doesn't make sense to do the usual scrub at the end of the test.
        """
        self.enable_scrub_on_exit = False

    def overlay_cleanup_teardown(self):
        """
        Unmount the overlayfs mounts created by `self.overlay_mount()`.
        Supposed to be called during env teardown.
        """
        if self.test_overlay_dir is None:
            return
        while len(self.overlay_mounts_created_by_us) > 0:
            (ident, mountpoint) = self.overlay_mounts_created_by_us.pop()
            ident_state_dir = self.test_overlay_dir / ident
            log.info(
                f"Unmounting overlayfs mount created during setup for ident {ident} at {mountpoint}"
            )
            self._overlay_umount(mountpoint)
            log.info(
                f"Cleaning up overlayfs state dir (owned by root user) for ident {ident} at {ident_state_dir}"
            )
            cmd = ["sudo", "rm", "-rf", str(ident_state_dir)]
            subprocess_capture(
                self.test_output_dir, cmd, check=True, echo_stderr=True, echo_stdout=True
            )

        # assert all overlayfs mounts in our test directory are gone
        assert [] == list(overlayfs.iter_mounts_beneath(self.test_overlay_dir))

    def enable_pageserver_remote_storage(
        self,
        remote_storage_kind: RemoteStorageKind,
    ):
        assert self.pageserver_remote_storage is None, "remote storage is enabled already"
        ret = self._configure_and_create_remote_storage(
            remote_storage_kind, RemoteStorageUser.PAGESERVER
        )
        self.pageserver_remote_storage = ret

    def enable_safekeeper_remote_storage(self, kind: RemoteStorageKind):
        assert (
            self.safekeepers_remote_storage is None
        ), "safekeepers_remote_storage already configured"

        self.safekeepers_remote_storage = self._configure_and_create_remote_storage(
            kind, RemoteStorageUser.SAFEKEEPER
        )

    def _configure_and_create_remote_storage(
        self,
        kind: RemoteStorageKind,
        user: RemoteStorageUser,
        bucket_name: Optional[str] = None,
        bucket_region: Optional[str] = None,
    ) -> RemoteStorage:
        ret = kind.configure(
            self.repo_dir,
            self.mock_s3_server,
            str(self.run_id),
            self.test_name,
            user,
            bucket_name=bucket_name,
            bucket_region=bucket_region,
        )

        if kind == RemoteStorageKind.MOCK_S3:
            assert isinstance(ret, S3Storage)
            ret.client.create_bucket(Bucket=ret.bucket_name)
        elif kind == RemoteStorageKind.REAL_S3:
            assert isinstance(ret, S3Storage)
            assert ret.cleanup, "we should not leave files in REAL_S3"

        return ret

    def cleanup_local_storage(self):
        if self.preserve_database_files:
            return

        overlayfs_mounts = {mountpoint for _, mountpoint in self.overlay_mounts_created_by_us}

        directories_to_clean: List[Path] = []
        for test_entry in Path(self.repo_dir).glob("**/*"):
            if test_entry in overlayfs_mounts:
                continue
            for parent in test_entry.parents:
                if parent in overlayfs_mounts:
                    continue
            if test_entry.is_file():
                test_file = test_entry
                if ATTACHMENT_NAME_REGEX.fullmatch(test_file.name):
                    continue
                if SMALL_DB_FILE_NAME_REGEX.fullmatch(test_file.name):
                    continue
                log.debug(f"Removing large database {test_file} file")
                test_file.unlink()
            elif test_entry.is_dir():
                directories_to_clean.append(test_entry)

        for directory_to_clean in reversed(directories_to_clean):
            if not os.listdir(directory_to_clean):
                log.info(f"Removing empty directory {directory_to_clean}")
                try:
                    directory_to_clean.rmdir()
                except Exception as e:
                    log.error(f"Error removing empty directory {directory_to_clean}: {e}")

    def cleanup_remote_storage(self):
        for x in [self.pageserver_remote_storage, self.safekeepers_remote_storage]:
            if isinstance(x, S3Storage):
                x.do_cleanup()

    def __enter__(self) -> "NeonEnvBuilder":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ):
        # Stop all the nodes.
        if self.env:
            log.info("Cleaning up all storage and compute nodes")
            self.env.stop(
                immediate=False,
                # if the test threw an exception, don't check for errors
                # as a failing assertion would cause the cleanup below to fail
                ps_assert_metric_no_errors=(exc_type is None),
                # do not fail on endpoint errors to allow the rest of cleanup to proceed
                fail_on_endpoint_errors=False,
            )
            cleanup_error = None

            # If we are running with S3Storage (required by the scrubber), check that whatever the test
            # did does not generate any corruption
            if (
                isinstance(self.env.pageserver_remote_storage, S3Storage)
                and self.enable_scrub_on_exit
            ):
                try:
                    healthy, _ = self.env.storage_scrubber.scan_metadata()
                    if not healthy:
                        e = Exception("Remote storage metadata corrupted")
                        cleanup_error = e
                except Exception as e:
                    log.error(f"Error during remote storage scrub: {e}")
                    cleanup_error = e

            try:
                self.cleanup_remote_storage()
            except Exception as e:
                log.error(f"Error during remote storage cleanup: {e}")
                if cleanup_error is not None:
                    cleanup_error = e

            try:
                self.cleanup_local_storage()
            except Exception as e:
                log.error(f"Error during local storage cleanup: {e}")
                if cleanup_error is not None:
                    cleanup_error = e

            if cleanup_error is not None:
                raise cleanup_error

            for pageserver in self.env.pageservers:
                pageserver.assert_no_errors()

            for safekeeper in self.env.safekeepers:
                safekeeper.assert_no_errors()

            self.env.storage_controller.assert_no_errors()

        try:
            self.overlay_cleanup_teardown()
        except Exception as e:
            log.error(f"Error cleaning up overlay state: {e}")
            if cleanup_error is not None:
                cleanup_error = e


class NeonEnv:
    """
    An object representing the Neon runtime environment. It consists of
    the page server, 0-N safekeepers, and the compute nodes.

    NeonEnv contains functions for stopping/starting nodes in the
    environment, checking their status, creating tenants, connecting to the
    nodes, creating and destroying compute nodes, etc. The page server and
    the safekeepers are considered fixed in the environment, you cannot
    create or destroy them after the environment is initialized. (That will
    likely change in the future, as we start supporting multiple page
    servers and adding/removing safekeepers on the fly).

    Some notable functions and fields in NeonEnv:

    endpoints - A factory object for creating postgres compute nodes.

    pageservers - An array containing objects representing the pageservers

    safekeepers - An array containing objects representing the safekeepers

    pg_bin - pg_bin.run() can be used to execute Postgres client binaries,
        like psql or pg_dump

    initial_tenant - tenant ID of the initial tenant created in the repository

    neon_cli - can be used to run the 'neon' CLI tool

    create_tenant() - initializes a new tenant in the page server, returns
        the tenant id
    """

    BASE_PAGESERVER_ID = 1
    storage_controller: NeonStorageController | NeonProxiedStorageController

    def __init__(self, config: NeonEnvBuilder):
        self.repo_dir = config.repo_dir
        self.rust_log_override = config.rust_log_override
        self.port_distributor = config.port_distributor
        self.s3_mock_server = config.mock_s3_server
        self.neon_cli = NeonCli(env=self)
        self.pagectl = Pagectl(env=self)
        self.endpoints = EndpointFactory(self)
        self.safekeepers: List[Safekeeper] = []
        self.pageservers: List[NeonPageserver] = []
        self.broker = config.broker
        self.pageserver_remote_storage = config.pageserver_remote_storage
        self.safekeepers_remote_storage = config.safekeepers_remote_storage
        self.pg_version = config.pg_version
        # Binary path for pageserver, safekeeper, etc
        self.neon_binpath = config.neon_binpath
        # Binary path for neon_local test-specific binaries
        self.neon_local_binpath = config.neon_local_binpath
        if self.neon_local_binpath is None:
            self.neon_local_binpath = self.neon_binpath
        self.pg_distrib_dir = config.pg_distrib_dir
        self.endpoint_counter = 0
        self.storage_controller_config = config.storage_controller_config
        self.initial_tenant = config.initial_tenant
        self.initial_timeline = config.initial_timeline

        # The URL for the pageserver to use as its control_plane_api config
        if config.storage_controller_port_override is not None:
            log.info(
                f"Using storage controller api override {config.storage_controller_port_override}"
            )

            self.storage_controller_port = config.storage_controller_port_override
            self.storage_controller = NeonProxiedStorageController(
                self, config.storage_controller_port_override, config.auth_enabled
            )
        else:
            # Find two adjacent ports for storage controller and its postgres DB.  This
            # loop would eventually throw from get_port() if we run out of ports (extremely
            # unlikely): usually we find two adjacent free ports on the first iteration.
            while True:
                storage_controller_port = self.port_distributor.get_port()
                storage_controller_pg_port = self.port_distributor.get_port()
                if storage_controller_pg_port == storage_controller_port + 1:
                    break

            self.storage_controller_port = storage_controller_port
            self.storage_controller = NeonStorageController(
                self, storage_controller_port, config.auth_enabled
            )

            log.info(
                f"Using generated control_plane_api: {self.storage_controller.upcall_api_endpoint()}"
            )

        self.storage_controller_api: str = self.storage_controller.api_root()
        self.control_plane_api: str = self.storage_controller.upcall_api_endpoint()

        # For testing this with a fake HTTP server, enable passing through a URL from config
        self.control_plane_compute_hook_api = config.control_plane_compute_hook_api

        self.pageserver_virtual_file_io_engine = config.pageserver_virtual_file_io_engine
        self.pageserver_aux_file_policy = config.pageserver_aux_file_policy
        self.pageserver_io_buffer_alignment = config.pageserver_io_buffer_alignment

        # Create the neon_local's `NeonLocalInitConf`
        cfg: Dict[str, Any] = {
            "default_tenant_id": str(self.initial_tenant),
            "broker": {
                "listen_addr": self.broker.listen_addr(),
            },
            "safekeepers": [],
            "pageservers": [],
        }

        if self.control_plane_api is not None:
            cfg["control_plane_api"] = self.control_plane_api

        if self.control_plane_compute_hook_api is not None:
            cfg["control_plane_compute_hook_api"] = self.control_plane_compute_hook_api

        if self.storage_controller_config is not None:
            cfg["storage_controller"] = self.storage_controller_config

        # Create config for pageserver
        http_auth_type = "NeonJWT" if config.auth_enabled else "Trust"
        pg_auth_type = "NeonJWT" if config.auth_enabled else "Trust"
        for ps_id in range(
            self.BASE_PAGESERVER_ID, self.BASE_PAGESERVER_ID + config.num_pageservers
        ):
            pageserver_port = PageserverPort(
                pg=self.port_distributor.get_port(),
                http=self.port_distributor.get_port(),
            )

            ps_cfg: Dict[str, Any] = {
                "id": ps_id,
                "listen_pg_addr": f"localhost:{pageserver_port.pg}",
                "listen_http_addr": f"localhost:{pageserver_port.http}",
                "pg_auth_type": pg_auth_type,
                "http_auth_type": http_auth_type,
                # Default which can be overriden with `NeonEnvBuilder.pageserver_config_override`
                "availability_zone": "us-east-2a",
            }
            if self.pageserver_virtual_file_io_engine is not None:
                ps_cfg["virtual_file_io_engine"] = self.pageserver_virtual_file_io_engine
            if config.pageserver_default_tenant_config_compaction_algorithm is not None:
                tenant_config = ps_cfg.setdefault("tenant_config", {})
                tenant_config[
                    "compaction_algorithm"
                ] = config.pageserver_default_tenant_config_compaction_algorithm

            if self.pageserver_remote_storage is not None:
                ps_cfg["remote_storage"] = remote_storage_to_toml_dict(
                    self.pageserver_remote_storage
                )

            if config.pageserver_config_override is not None:
                if callable(config.pageserver_config_override):
                    config.pageserver_config_override(ps_cfg)
                else:
                    assert isinstance(config.pageserver_config_override, str)
                    for o in config.pageserver_config_override.split(";"):
                        override = toml.loads(o)
                        for key, value in override.items():
                            ps_cfg[key] = value

            ps_cfg["io_buffer_alignment"] = self.pageserver_io_buffer_alignment

            # Create a corresponding NeonPageserver object
            self.pageservers.append(
                NeonPageserver(self, ps_id, port=pageserver_port, az_id=ps_cfg["availability_zone"])
            )
            cfg["pageservers"].append(ps_cfg)

        # Create config and a Safekeeper object for each safekeeper
        for i in range(1, config.num_safekeepers + 1):
            port = SafekeeperPort(
                pg=self.port_distributor.get_port(),
                pg_tenant_only=self.port_distributor.get_port(),
                http=self.port_distributor.get_port(),
            )
            id = config.safekeepers_id_start + i  # assign ids sequentially
            sk_cfg: Dict[str, Any] = {
                "id": id,
                "pg_port": port.pg,
                "pg_tenant_only_port": port.pg_tenant_only,
                "http_port": port.http,
                "sync": config.safekeepers_enable_fsync,
            }
            if config.auth_enabled:
                sk_cfg["auth_enabled"] = True
            if self.safekeepers_remote_storage is not None:
                sk_cfg[
                    "remote_storage"
                ] = self.safekeepers_remote_storage.to_toml_inline_table().strip()
            self.safekeepers.append(
                Safekeeper(env=self, id=id, port=port, extra_opts=config.safekeeper_extra_opts)
            )
            cfg["safekeepers"].append(sk_cfg)

        # Scrubber instance for tests that use it, and for use during teardown checks
        self.storage_scrubber = StorageScrubber(self, log_dir=config.test_output_dir)

        log.info(f"Config: {cfg}")
        self.neon_cli.init(
            cfg,
            force=config.config_init_force,
        )

    def start(self, timeout_in_seconds: Optional[int] = None):
        # Storage controller starts first, so that pageserver /re-attach calls don't
        # bounce through retries on startup
        self.storage_controller.start(timeout_in_seconds=timeout_in_seconds)

        # Wait for storage controller readiness to prevent unnecessary post start-up
        # reconcile.
        self.storage_controller.wait_until_ready()

        # Start up broker, pageserver and all safekeepers
        futs = []
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=2 + len(self.pageservers) + len(self.safekeepers)
        ) as executor:
            futs.append(
                executor.submit(lambda: self.broker.try_start() or None)
            )  # The `or None` is for the linter

            for pageserver in self.pageservers:
                futs.append(
                    executor.submit(
                        lambda ps=pageserver: ps.start(timeout_in_seconds=timeout_in_seconds)
                    )
                )

            for safekeeper in self.safekeepers:
                futs.append(
                    executor.submit(
                        lambda sk=safekeeper: sk.start(timeout_in_seconds=timeout_in_seconds)
                    )
                )

        for f in futs:
            f.result()

    def stop(self, immediate=False, ps_assert_metric_no_errors=False, fail_on_endpoint_errors=True):
        """
        After this method returns, there should be no child processes running.

        Unless of course, some stopping failed, in that case, all remaining child processes are leaked.
        """

        # the commonly failing components have special try-except behavior,
        # trying to get us to actually shutdown all processes over easier error
        # reporting.

        raise_later = None
        try:
            self.endpoints.stop_all(fail_on_endpoint_errors)
        except Exception as e:
            raise_later = e

        # Stop storage controller before pageservers: we don't want it to spuriously
        # detect a pageserver "failure" during test teardown
        self.storage_controller.stop(immediate=immediate)

        stop_later = []
        metric_errors = []

        for sk in self.safekeepers:
            sk.stop(immediate=immediate)
        for pageserver in self.pageservers:
            if ps_assert_metric_no_errors:
                try:
                    pageserver.assert_no_metric_errors()
                except Exception as e:
                    metric_errors.append(e)
                    log.error(f"metric validation failed on {pageserver.id}: {e}")
            try:
                pageserver.stop(immediate=immediate)
            except RuntimeError:
                stop_later.append(pageserver)
        self.broker.stop(immediate=immediate)

        # TODO: for nice logging we need python 3.11 ExceptionGroup
        for ps in stop_later:
            ps.stop(immediate=True)

        if raise_later is not None:
            raise raise_later

        for error in metric_errors:
            raise error

        if len(stop_later) > 0:
            raise RuntimeError(
                f"{len(stop_later)} out of {len(self.pageservers)} pageservers failed to stop gracefully"
            )

    @property
    def pageserver(self) -> NeonPageserver:
        """
        For tests that are naive to multiple pageservers: give them the 1st in the list, and
        assert that there is only one. Tests with multiple pageservers should always use
        get_pageserver with an explicit ID.
        """
        assert (
            len(self.pageservers) == 1
        ), "env.pageserver must only be used with single pageserver NeonEnv"
        return self.pageservers[0]

    def get_pageserver(self, id: Optional[int]) -> NeonPageserver:
        """
        Look up a pageserver by its node ID.

        As a convenience for tests that do not use multiple pageservers, passing None
        will yield the same default pageserver as `self.pageserver`.
        """

        if id is None:
            return self.pageserver

        for ps in self.pageservers:
            if ps.id == id:
                return ps

        raise RuntimeError(f"Pageserver with ID {id} not found")

    def get_tenant_pageserver(self, tenant_id: Union[TenantId, TenantShardId]):
        """
        Get the NeonPageserver where this tenant shard is currently attached, according
        to the storage controller.
        """
        meta = self.storage_controller.inspect(tenant_id)
        if meta is None:
            return None
        pageserver_id = meta[1]
        return self.get_pageserver(pageserver_id)

    def get_safekeeper_connstrs(self) -> str:
        """Get list of safekeeper endpoints suitable for safekeepers GUC"""
        return ",".join(f"localhost:{wa.port.pg}" for wa in self.safekeepers)

    def get_binary_version(self, binary_name: str) -> str:
        bin_pageserver = str(self.neon_binpath / binary_name)
        res = subprocess.run(
            [bin_pageserver, "--version"],
            check=True,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return res.stdout

    @cached_property
    def auth_keys(self) -> AuthKeys:
        priv = (Path(self.repo_dir) / "auth_private_key.pem").read_text()
        return AuthKeys(priv=priv)

    def regenerate_keys_at(self, privkey_path: Path, pubkey_path: Path):
        # compare generate_auth_keys() in local_env.rs
        subprocess.run(
            ["openssl", "genpkey", "-algorithm", "ed25519", "-out", privkey_path],
            cwd=self.repo_dir,
            check=True,
        )

        subprocess.run(
            [
                "openssl",
                "pkey",
                "-in",
                privkey_path,
                "-pubout",
                "-out",
                pubkey_path,
            ],
            cwd=self.repo_dir,
            check=True,
        )
        del self.auth_keys

    def generate_endpoint_id(self) -> str:
        """
        Generate a unique endpoint ID
        """
        self.endpoint_counter += 1
        return "ep-" + str(self.endpoint_counter)


@pytest.fixture(scope="function")
def neon_simple_env(
    request: FixtureRequest,
    pytestconfig: Config,
    port_distributor: PortDistributor,
    mock_s3_server: MockS3Server,
    default_broker: NeonBroker,
    run_id: uuid.UUID,
    top_output_dir: Path,
    test_output_dir: Path,
    neon_binpath: Path,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    pageserver_virtual_file_io_engine: str,
    pageserver_aux_file_policy: Optional[AuxFileStore],
    pageserver_default_tenant_config_compaction_algorithm: Optional[Dict[str, Any]],
    pageserver_io_buffer_alignment: Optional[int],
) -> Iterator[NeonEnv]:
    """
    Simple Neon environment, with no authentication and no safekeepers.

    This fixture will use RemoteStorageKind.LOCAL_FS with pageserver.
    """

    # Create the environment in the per-test output directory
    repo_dir = get_test_repo_dir(request, top_output_dir)

    with NeonEnvBuilder(
        top_output_dir=top_output_dir,
        repo_dir=repo_dir,
        port_distributor=port_distributor,
        broker=default_broker,
        mock_s3_server=mock_s3_server,
        neon_binpath=neon_binpath,
        pg_distrib_dir=pg_distrib_dir,
        pg_version=pg_version,
        run_id=run_id,
        preserve_database_files=cast(bool, pytestconfig.getoption("--preserve-database-files")),
        test_name=request.node.name,
        test_output_dir=test_output_dir,
        pageserver_virtual_file_io_engine=pageserver_virtual_file_io_engine,
        pageserver_aux_file_policy=pageserver_aux_file_policy,
        pageserver_default_tenant_config_compaction_algorithm=pageserver_default_tenant_config_compaction_algorithm,
        pageserver_io_buffer_alignment=pageserver_io_buffer_alignment,
    ) as builder:
        env = builder.init_start()

        yield env


@pytest.fixture(scope="function")
def neon_env_builder(
    pytestconfig: Config,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    mock_s3_server: MockS3Server,
    neon_binpath: Path,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    default_broker: NeonBroker,
    run_id: uuid.UUID,
    request: FixtureRequest,
    test_overlay_dir: Path,
    top_output_dir: Path,
    pageserver_virtual_file_io_engine: str,
    pageserver_default_tenant_config_compaction_algorithm: Optional[Dict[str, Any]],
    pageserver_aux_file_policy: Optional[AuxFileStore],
    record_property: Callable[[str, object], None],
    pageserver_io_buffer_alignment: Optional[int],
) -> Iterator[NeonEnvBuilder]:
    """
    Fixture to create a Neon environment for test.

    To use, define 'neon_env_builder' fixture in your test to get access to the
    builder object. Set properties on it to describe the environment.
    Finally, initialize and start up the environment by calling
    neon_env_builder.init_start().

    After the initialization, you can launch compute nodes by calling
    the functions in the 'env.endpoints' factory object, stop/start the
    nodes, etc.
    """

    # Create the environment in the test-specific output dir
    repo_dir = os.path.join(test_output_dir, "repo")

    # Return the builder to the caller
    with NeonEnvBuilder(
        top_output_dir=top_output_dir,
        repo_dir=Path(repo_dir),
        port_distributor=port_distributor,
        mock_s3_server=mock_s3_server,
        neon_binpath=neon_binpath,
        pg_distrib_dir=pg_distrib_dir,
        pg_version=pg_version,
        broker=default_broker,
        run_id=run_id,
        preserve_database_files=cast(bool, pytestconfig.getoption("--preserve-database-files")),
        pageserver_virtual_file_io_engine=pageserver_virtual_file_io_engine,
        test_name=request.node.name,
        test_output_dir=test_output_dir,
        test_overlay_dir=test_overlay_dir,
        pageserver_aux_file_policy=pageserver_aux_file_policy,
        pageserver_default_tenant_config_compaction_algorithm=pageserver_default_tenant_config_compaction_algorithm,
        pageserver_io_buffer_alignment=pageserver_io_buffer_alignment,
    ) as builder:
        yield builder
        # Propogate `preserve_database_files` to make it possible to use in other fixtures,
        # like `test_output_dir` fixture for attaching all database files to Allure report.
        record_property("preserve_database_files", builder.preserve_database_files)


@dataclass
class PageserverPort:
    pg: int
    http: int


class AbstractNeonCli(abc.ABC):
    """
    A typed wrapper around an arbitrary Neon CLI tool.
    Supports a way to run arbitrary command directly via CLI.
    Do not use directly, use specific subclasses instead.
    """

    def __init__(self, env: NeonEnv):
        self.env = env

    COMMAND: str = cast(str, None)  # To be overwritten by the derived class.

    def raw_cli(
        self,
        arguments: List[str],
        extra_env_vars: Optional[Dict[str, str]] = None,
        check_return_code=True,
        timeout=None,
        local_binpath=False,
    ) -> "subprocess.CompletedProcess[str]":
        """
        Run the command with the specified arguments.

        Arguments must be in list form, e.g. ['pg', 'create']

        Return both stdout and stderr, which can be accessed as

        >>> result = env.neon_cli.raw_cli(...)
        >>> assert result.stderr == ""
        >>> log.info(result.stdout)

        If `check_return_code`, on non-zero exit code logs failure and raises.

        If `local_binpath` is true, then we are invoking a test utility
        """

        assert isinstance(arguments, list)
        assert isinstance(self.COMMAND, str)

        if local_binpath:
            # Test utility
            bin_neon = str(self.env.neon_local_binpath / self.COMMAND)
        else:
            # Normal binary
            bin_neon = str(self.env.neon_binpath / self.COMMAND)

        args = [bin_neon] + arguments
        log.info('Running command "{}"'.format(" ".join(args)))

        env_vars = os.environ.copy()
        env_vars["NEON_REPO_DIR"] = str(self.env.repo_dir)
        env_vars["POSTGRES_DISTRIB_DIR"] = str(self.env.pg_distrib_dir)
        if self.env.rust_log_override is not None:
            env_vars["RUST_LOG"] = self.env.rust_log_override
        for extra_env_key, extra_env_value in (extra_env_vars or {}).items():
            env_vars[extra_env_key] = extra_env_value

        # Pass coverage settings
        var = "LLVM_PROFILE_FILE"
        val = os.environ.get(var)
        if val:
            env_vars[var] = val

        # Intercept CalledProcessError and print more info
        try:
            res = subprocess.run(
                args,
                env=env_vars,
                check=False,
                universal_newlines=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            if e.stderr:
                stderr = e.stderr.decode(errors="replace")
            else:
                stderr = ""

            if e.stdout:
                stdout = e.stdout.decode(errors="replace")
            else:
                stdout = ""

            log.warn(f"CLI timeout: stderr={stderr}, stdout={stdout}")
            raise

        indent = "  "
        if not res.returncode:
            stripped = res.stdout.strip()
            lines = stripped.splitlines()
            if len(lines) < 2:
                log.debug(f"Run {res.args} success: {stripped}")
            else:
                log.debug("Run %s success:\n%s" % (res.args, textwrap.indent(stripped, indent)))
        elif check_return_code:
            # this way command output will be in recorded and shown in CI in failure message
            indent = indent * 2
            msg = textwrap.dedent(
                """\
            Run %s failed:
              stdout:
            %s
              stderr:
            %s
            """
            )
            msg = msg % (
                res.args,
                textwrap.indent(res.stdout.strip(), indent),
                textwrap.indent(res.stderr.strip(), indent),
            )
            log.info(msg)
            raise RuntimeError(msg) from subprocess.CalledProcessError(
                res.returncode, res.args, res.stdout, res.stderr
            )
        return res


class NeonCli(AbstractNeonCli):
    """
    A typed wrapper around the `neon` CLI tool.
    Supports main commands via typed methods and a way to run arbitrary command directly via CLI.
    """

    COMMAND = "neon_local"

    def raw_cli(self, *args, **kwargs) -> subprocess.CompletedProcess[str]:
        kwargs["local_binpath"] = True
        return super().raw_cli(*args, **kwargs)

    def create_tenant(
        self,
        tenant_id: Optional[TenantId] = None,
        timeline_id: Optional[TimelineId] = None,
        conf: Optional[Dict[str, Any]] = None,
        shard_count: Optional[int] = None,
        shard_stripe_size: Optional[int] = None,
        placement_policy: Optional[str] = None,
        set_default: bool = False,
        aux_file_policy: Optional[AuxFileStore] = None,
    ) -> Tuple[TenantId, TimelineId]:
        """
        Creates a new tenant, returns its id and its initial timeline's id.
        """
        tenant_id = tenant_id or TenantId.generate()
        timeline_id = timeline_id or TimelineId.generate()

        args = [
            "tenant",
            "create",
            "--tenant-id",
            str(tenant_id),
            "--timeline-id",
            str(timeline_id),
            "--pg-version",
            self.env.pg_version,
        ]
        if conf is not None:
            args.extend(
                chain.from_iterable(
                    product(["-c"], (f"{key}:{value}" for key, value in conf.items()))
                )
            )

        if aux_file_policy is AuxFileStore.V2:
            args.extend(["-c", "switch_aux_file_policy:v2"])
        elif aux_file_policy is AuxFileStore.V1:
            args.extend(["-c", "switch_aux_file_policy:v1"])
        elif aux_file_policy is AuxFileStore.CrossValidation:
            args.extend(["-c", "switch_aux_file_policy:cross-validation"])

        if set_default:
            args.append("--set-default")

        if shard_count is not None:
            args.extend(["--shard-count", str(shard_count)])

        if shard_stripe_size is not None:
            args.extend(["--shard-stripe-size", str(shard_stripe_size)])

        if placement_policy is not None:
            args.extend(["--placement-policy", str(placement_policy)])

        res = self.raw_cli(args)
        res.check_returncode()
        return tenant_id, timeline_id

    def import_tenant(self, tenant_id: TenantId):
        args = ["tenant", "import", "--tenant-id", str(tenant_id)]
        res = self.raw_cli(args)
        res.check_returncode()

    def set_default(self, tenant_id: TenantId):
        """
        Update default tenant for future operations that require tenant_id.
        """
        res = self.raw_cli(["tenant", "set-default", "--tenant-id", str(tenant_id)])
        res.check_returncode()

    def config_tenant(self, tenant_id: TenantId, conf: Dict[str, str]):
        """
        Update tenant config.
        """

        args = ["tenant", "config", "--tenant-id", str(tenant_id)]
        if conf is not None:
            args.extend(
                chain.from_iterable(
                    product(["-c"], (f"{key}:{value}" for key, value in conf.items()))
                )
            )

        res = self.raw_cli(args)
        res.check_returncode()

    def list_tenants(self) -> "subprocess.CompletedProcess[str]":
        res = self.raw_cli(["tenant", "list"])
        res.check_returncode()
        return res

    def create_timeline(
        self,
        new_branch_name: str,
        tenant_id: Optional[TenantId] = None,
        timeline_id: Optional[TimelineId] = None,
    ) -> TimelineId:
        if timeline_id is None:
            timeline_id = TimelineId.generate()

        cmd = [
            "timeline",
            "create",
            "--branch-name",
            new_branch_name,
            "--tenant-id",
            str(tenant_id or self.env.initial_tenant),
            "--timeline-id",
            str(timeline_id),
            "--pg-version",
            self.env.pg_version,
        ]

        res = self.raw_cli(cmd)
        res.check_returncode()

        return timeline_id

    def create_branch(
        self,
        new_branch_name: str = DEFAULT_BRANCH_NAME,
        ancestor_branch_name: Optional[str] = None,
        tenant_id: Optional[TenantId] = None,
        ancestor_start_lsn: Optional[Lsn] = None,
        new_timeline_id: Optional[TimelineId] = None,
    ) -> TimelineId:
        if new_timeline_id is None:
            new_timeline_id = TimelineId.generate()
        cmd = [
            "timeline",
            "branch",
            "--branch-name",
            new_branch_name,
            "--timeline-id",
            str(new_timeline_id),
            "--tenant-id",
            str(tenant_id or self.env.initial_tenant),
        ]
        if ancestor_branch_name is not None:
            cmd.extend(["--ancestor-branch-name", ancestor_branch_name])
        if ancestor_start_lsn is not None:
            cmd.extend(["--ancestor-start-lsn", str(ancestor_start_lsn)])

        res = self.raw_cli(cmd)
        res.check_returncode()

        return TimelineId(str(new_timeline_id))

    def list_timelines(self, tenant_id: Optional[TenantId] = None) -> List[Tuple[str, TimelineId]]:
        """
        Returns a list of (branch_name, timeline_id) tuples out of parsed `neon timeline list` CLI output.
        """

        # main [b49f7954224a0ad25cc0013ea107b54b]
        # ┣━ @0/16B5A50: test_cli_branch_list_main [20f98c79111b9015d84452258b7d5540]
        TIMELINE_DATA_EXTRACTOR: re.Pattern = re.compile(  # type: ignore[type-arg]
            r"\s?(?P<branch_name>[^\s]+)\s\[(?P<timeline_id>[^\]]+)\]", re.MULTILINE
        )
        res = self.raw_cli(
            ["timeline", "list", "--tenant-id", str(tenant_id or self.env.initial_tenant)]
        )
        timelines_cli = sorted(
            map(
                lambda branch_and_id: (branch_and_id[0], TimelineId(branch_and_id[1])),
                TIMELINE_DATA_EXTRACTOR.findall(res.stdout),
            )
        )
        return timelines_cli

    def init(
        self,
        init_config: Dict[str, Any],
        force: Optional[str] = None,
    ) -> "subprocess.CompletedProcess[str]":
        with tempfile.NamedTemporaryFile(mode="w+") as init_config_tmpfile:
            init_config_tmpfile.write(toml.dumps(init_config))
            init_config_tmpfile.flush()

            cmd = [
                "init",
                f"--config={init_config_tmpfile.name}",
            ]

            if force is not None:
                cmd.extend(["--force", force])

            res = self.raw_cli(cmd)
            res.check_returncode()
        return res

    def storage_controller_start(
        self,
        timeout_in_seconds: Optional[int] = None,
        instance_id: Optional[int] = None,
        base_port: Optional[int] = None,
    ):
        cmd = ["storage_controller", "start"]
        if timeout_in_seconds is not None:
            cmd.append(f"--start-timeout={timeout_in_seconds}s")
        if instance_id is not None:
            cmd.append(f"--instance-id={instance_id}")
        if base_port is not None:
            cmd.append(f"--base-port={base_port}")
        return self.raw_cli(cmd)

    def storage_controller_stop(self, immediate: bool, instance_id: Optional[int] = None):
        cmd = ["storage_controller", "stop"]
        if immediate:
            cmd.extend(["-m", "immediate"])
        if instance_id is not None:
            cmd.append(f"--instance-id={instance_id}")
        return self.raw_cli(cmd)

    def pageserver_start(
        self,
        id: int,
        extra_env_vars: Optional[Dict[str, str]] = None,
        timeout_in_seconds: Optional[int] = None,
    ) -> "subprocess.CompletedProcess[str]":
        start_args = ["pageserver", "start", f"--id={id}"]
        if timeout_in_seconds is not None:
            start_args.append(f"--start-timeout={timeout_in_seconds}s")
        storage = self.env.pageserver_remote_storage

        if isinstance(storage, S3Storage):
            s3_env_vars = storage.access_env_vars()
            extra_env_vars = (extra_env_vars or {}) | s3_env_vars

        return self.raw_cli(start_args, extra_env_vars=extra_env_vars)

    def pageserver_stop(self, id: int, immediate=False) -> "subprocess.CompletedProcess[str]":
        cmd = ["pageserver", "stop", f"--id={id}"]
        if immediate:
            cmd.extend(["-m", "immediate"])

        log.info(f"Stopping pageserver with {cmd}")
        return self.raw_cli(cmd)

    def safekeeper_start(
        self,
        id: int,
        extra_opts: Optional[List[str]] = None,
        timeout_in_seconds: Optional[int] = None,
    ) -> "subprocess.CompletedProcess[str]":
        s3_env_vars = None
        if isinstance(self.env.safekeepers_remote_storage, S3Storage):
            s3_env_vars = self.env.safekeepers_remote_storage.access_env_vars()

        if extra_opts is not None:
            extra_opts = [f"-e={opt}" for opt in extra_opts]
        else:
            extra_opts = []
        if timeout_in_seconds is not None:
            extra_opts.append(f"--start-timeout={timeout_in_seconds}s")
        return self.raw_cli(
            ["safekeeper", "start", str(id), *extra_opts], extra_env_vars=s3_env_vars
        )

    def safekeeper_stop(
        self, id: Optional[int] = None, immediate=False
    ) -> "subprocess.CompletedProcess[str]":
        args = ["safekeeper", "stop"]
        if id is not None:
            args.append(str(id))
        if immediate:
            args.extend(["-m", "immediate"])
        return self.raw_cli(args)

    def endpoint_create(
        self,
        branch_name: str,
        pg_port: int,
        http_port: int,
        endpoint_id: Optional[str] = None,
        tenant_id: Optional[TenantId] = None,
        hot_standby: bool = False,
        lsn: Optional[Lsn] = None,
        pageserver_id: Optional[int] = None,
        allow_multiple=False,
    ) -> "subprocess.CompletedProcess[str]":
        args = [
            "endpoint",
            "create",
            "--tenant-id",
            str(tenant_id or self.env.initial_tenant),
            "--branch-name",
            branch_name,
            "--pg-version",
            self.env.pg_version,
        ]
        if lsn is not None:
            args.extend(["--lsn", str(lsn)])
        if pg_port is not None:
            args.extend(["--pg-port", str(pg_port)])
        if http_port is not None:
            args.extend(["--http-port", str(http_port)])
        if endpoint_id is not None:
            args.append(endpoint_id)
        if hot_standby:
            args.extend(["--hot-standby", "true"])
        if pageserver_id is not None:
            args.extend(["--pageserver-id", str(pageserver_id)])
        if allow_multiple:
            args.extend(["--allow-multiple"])

        res = self.raw_cli(args)
        res.check_returncode()
        return res

    def endpoint_start(
        self,
        endpoint_id: str,
        safekeepers: Optional[List[int]] = None,
        remote_ext_config: Optional[str] = None,
        pageserver_id: Optional[int] = None,
        allow_multiple=False,
        basebackup_request_tries: Optional[int] = None,
    ) -> "subprocess.CompletedProcess[str]":
        args = [
            "endpoint",
            "start",
        ]
        extra_env_vars = {}
        if basebackup_request_tries is not None:
            extra_env_vars["NEON_COMPUTE_TESTING_BASEBACKUP_TRIES"] = str(basebackup_request_tries)
        if remote_ext_config is not None:
            args.extend(["--remote-ext-config", remote_ext_config])

        if safekeepers is not None:
            args.extend(["--safekeepers", (",".join(map(str, safekeepers)))])
        if endpoint_id is not None:
            args.append(endpoint_id)
        if pageserver_id is not None:
            args.extend(["--pageserver-id", str(pageserver_id)])
        if allow_multiple:
            args.extend(["--allow-multiple"])

        res = self.raw_cli(args, extra_env_vars)
        res.check_returncode()
        return res

    def endpoint_reconfigure(
        self,
        endpoint_id: str,
        tenant_id: Optional[TenantId] = None,
        pageserver_id: Optional[int] = None,
        safekeepers: Optional[List[int]] = None,
        check_return_code=True,
    ) -> "subprocess.CompletedProcess[str]":
        args = ["endpoint", "reconfigure", endpoint_id]
        if tenant_id is not None:
            args.extend(["--tenant-id", str(tenant_id)])
        if pageserver_id is not None:
            args.extend(["--pageserver-id", str(pageserver_id)])
        if safekeepers is not None:
            args.extend(["--safekeepers", (",".join(map(str, safekeepers)))])
        return self.raw_cli(args, check_return_code=check_return_code)

    def endpoint_stop(
        self,
        endpoint_id: str,
        destroy=False,
        check_return_code=True,
        mode: Optional[str] = None,
    ) -> "subprocess.CompletedProcess[str]":
        args = [
            "endpoint",
            "stop",
        ]
        if destroy:
            args.append("--destroy")
        if mode is not None:
            args.append(f"--mode={mode}")
        if endpoint_id is not None:
            args.append(endpoint_id)

        return self.raw_cli(args, check_return_code=check_return_code)

    def map_branch(
        self, name: str, tenant_id: TenantId, timeline_id: TimelineId
    ) -> "subprocess.CompletedProcess[str]":
        """
        Map tenant id and timeline id to a neon_local branch name. They do not have to exist.
        Usually needed when creating branches via PageserverHttpClient and not neon_local.

        After creating a name mapping, you can use EndpointFactory.create_start
        with this registered branch name.
        """
        args = [
            "mappings",
            "map",
            "--branch-name",
            name,
            "--tenant-id",
            str(tenant_id),
            "--timeline-id",
            str(timeline_id),
        ]

        return self.raw_cli(args, check_return_code=True)

    def start(self, check_return_code=True) -> "subprocess.CompletedProcess[str]":
        return self.raw_cli(["start"], check_return_code=check_return_code)

    def stop(self, check_return_code=True) -> "subprocess.CompletedProcess[str]":
        return self.raw_cli(["stop"], check_return_code=check_return_code)


class WalCraft(AbstractNeonCli):
    """
    A typed wrapper around the `wal_craft` CLI tool.
    Supports main commands via typed methods and a way to run arbitrary command directly via CLI.
    """

    COMMAND = "wal_craft"

    def postgres_config(self) -> List[str]:
        res = self.raw_cli(["print-postgres-config"])
        res.check_returncode()
        return res.stdout.split("\n")

    def in_existing(self, type: str, connection: str) -> None:
        res = self.raw_cli(["in-existing", type, connection])
        res.check_returncode()


class ComputeCtl(AbstractNeonCli):
    """
    A typed wrapper around the `compute_ctl` CLI tool.
    """

    COMMAND = "compute_ctl"


class Pagectl(AbstractNeonCli):
    """
    A typed wrapper around the `pagectl` utility CLI tool.
    """

    COMMAND = "pagectl"

    def dump_index_part(self, path: Path) -> IndexPartDump:
        res = self.raw_cli(["index-part", "dump", str(path)])
        res.check_returncode()
        parsed = json.loads(res.stdout)
        return IndexPartDump.from_json(parsed)


class LogUtils:
    """
    A mixin class which provides utilities for inspecting the logs of a service.
    """

    def __init__(self, logfile: Path) -> None:
        self.logfile = logfile

    def assert_log_contains(
        self, pattern: str, offset: None | LogCursor = None
    ) -> Tuple[str, LogCursor]:
        """Convenient for use inside wait_until()"""

        res = self.log_contains(pattern, offset=offset)
        assert res is not None
        return res

    def log_contains(
        self, pattern: str, offset: None | LogCursor = None
    ) -> Optional[Tuple[str, LogCursor]]:
        """Check that the log contains a line that matches the given regex"""
        logfile = self.logfile
        if not logfile.exists():
            log.warning(f"Skipping log check: {logfile} does not exist")
            return None

        contains_re = re.compile(pattern)

        # XXX: Our rust logging machinery buffers the messages, so if you
        # call this function immediately after it's been logged, there is
        # no guarantee it is already present in the log file. This hasn't
        # been a problem in practice, our python tests are not fast enough
        # to hit that race condition.
        skip_until_line_no = 0 if offset is None else offset._line_no
        cur_line_no = 0
        with logfile.open("r") as f:
            for line in f:
                if cur_line_no < skip_until_line_no:
                    cur_line_no += 1
                    continue
                elif contains_re.search(line):
                    # found it!
                    cur_line_no += 1
                    return (line, LogCursor(cur_line_no))
                else:
                    cur_line_no += 1
        return None


class StorageControllerApiException(Exception):
    def __init__(self, message, status_code: int):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


# See libs/pageserver_api/src/controller_api.rs
# for the rust definitions of the enums below
# TODO: Replace with `StrEnum` when we upgrade to python 3.11
class PageserverAvailability(str, Enum):
    ACTIVE = "Active"
    UNAVAILABLE = "Unavailable"
    OFFLINE = "Offline"


class PageserverSchedulingPolicy(str, Enum):
    ACTIVE = "Active"
    DRAINING = "Draining"
    FILLING = "Filling"
    PAUSE = "Pause"
    PAUSE_FOR_RESTART = "PauseForRestart"


class StorageControllerLeadershipStatus(str, Enum):
    LEADER = "leader"
    STEPPED_DOWN = "stepped_down"
    CANDIDATE = "candidate"


class NeonStorageController(MetricsGetter, LogUtils):
    def __init__(self, env: NeonEnv, port: int, auth_enabled: bool):
        self.env = env
        self.port: int = port
        self.api: str = f"http://127.0.0.1:{port}"
        self.running = False
        self.auth_enabled = auth_enabled
        self.allowed_errors: list[str] = DEFAULT_STORAGE_CONTROLLER_ALLOWED_ERRORS
        self.logfile = self.env.repo_dir / "storage_controller_1" / "storage_controller.log"

    def start(
        self,
        timeout_in_seconds: Optional[int] = None,
        instance_id: Optional[int] = None,
        base_port: Optional[int] = None,
    ):
        assert not self.running
        self.env.neon_cli.storage_controller_start(timeout_in_seconds, instance_id, base_port)
        self.running = True
        return self

    def stop(self, immediate: bool = False) -> "NeonStorageController":
        if self.running:
            self.env.neon_cli.storage_controller_stop(immediate)
            self.running = False
        return self

    def upcall_api_endpoint(self) -> str:
        return f"{self.api}/upcall/v1"

    def api_root(self) -> str:
        return self.api

    @staticmethod
    def retryable_node_operation(op, ps_id, max_attempts, backoff):
        while max_attempts > 0:
            try:
                op(ps_id)
                return
            except StorageControllerApiException as e:
                max_attempts -= 1
                log.info(f"Operation failed ({max_attempts} attempts left): {e}")

                if max_attempts == 0:
                    raise e

                time.sleep(backoff)

    @staticmethod
    def raise_api_exception(res: requests.Response):
        try:
            res.raise_for_status()
        except requests.RequestException as e:
            try:
                msg = res.json()["msg"]
            except:  # noqa: E722
                msg = ""
            raise StorageControllerApiException(msg, res.status_code) from e

    def assert_no_errors(self):
        assert_no_errors(
            self.logfile,
            "storage_controller",
            self.allowed_errors,
        )

    def pageserver_api(self, *args, **kwargs) -> PageserverHttpClient:
        """
        The storage controller implements a subset of the pageserver REST API, for mapping
        per-tenant actions into per-shard actions (e.g. timeline creation).  Tests should invoke those
        functions via the HttpClient, as an implicit check that these APIs remain compatible.
        """
        auth_token = None
        if self.auth_enabled:
            auth_token = self.env.auth_keys.generate_token(scope=TokenScope.PAGE_SERVER_API)
        return PageserverHttpClient(self.port, lambda: True, auth_token, *args, **kwargs)

    def request(self, method, *args, **kwargs) -> requests.Response:
        resp = requests.request(method, *args, **kwargs)
        NeonStorageController.raise_api_exception(resp)

        return resp

    def headers(self, scope: Optional[TokenScope]) -> Dict[str, str]:
        headers = {}
        if self.auth_enabled and scope is not None:
            jwt_token = self.env.auth_keys.generate_token(scope=scope)
            headers["Authorization"] = f"Bearer {jwt_token}"

        return headers

    def get_metrics(self) -> Metrics:
        res = self.request("GET", f"{self.api}/metrics")
        return parse_metrics(res.text)

    def ready(self) -> bool:
        status = None
        try:
            resp = self.request("GET", f"{self.api}/ready")
            status = resp.status_code
        except StorageControllerApiException as e:
            status = e.status_code

        if status == 503:
            return False
        elif status == 200:
            return True
        else:
            raise RuntimeError(f"Unexpected status {status} from readiness endpoint")

    def wait_until_ready(self):
        t1 = time.time()

        def storage_controller_ready():
            assert self.ready() is True

        wait_until(30, 1, storage_controller_ready)
        return time.time() - t1

    def attach_hook_issue(
        self,
        tenant_shard_id: Union[TenantId, TenantShardId],
        pageserver_id: int,
        generation_override: Optional[int] = None,
    ) -> int:
        body = {"tenant_shard_id": str(tenant_shard_id), "node_id": pageserver_id}
        if generation_override is not None:
            body["generation_override"] = generation_override

        response = self.request(
            "POST",
            f"{self.api}/debug/v1/attach-hook",
            json=body,
            headers=self.headers(TokenScope.ADMIN),
        )
        gen = response.json()["gen"]
        assert isinstance(gen, int)
        return gen

    def attach_hook_drop(self, tenant_shard_id: Union[TenantId, TenantShardId]):
        self.request(
            "POST",
            f"{self.api}/debug/v1/attach-hook",
            json={"tenant_shard_id": str(tenant_shard_id), "node_id": None},
            headers=self.headers(TokenScope.ADMIN),
        )

    def inspect(self, tenant_shard_id: Union[TenantId, TenantShardId]) -> Optional[tuple[int, int]]:
        """
        :return: 2-tuple of (generation, pageserver id), or None if unknown
        """
        response = self.request(
            "POST",
            f"{self.api}/debug/v1/inspect",
            json={"tenant_shard_id": str(tenant_shard_id)},
            headers=self.headers(TokenScope.ADMIN),
        )
        json = response.json()
        log.info(f"Response: {json}")
        if json["attachment"]:
            # Explicit int() to make python type linter happy
            return (int(json["attachment"][0]), int(json["attachment"][1]))
        else:
            return None

    def node_register(self, node: NeonPageserver):
        body = {
            "node_id": int(node.id),
            "listen_http_addr": "localhost",
            "listen_http_port": node.service_port.http,
            "listen_pg_addr": "localhost",
            "listen_pg_port": node.service_port.pg,
            "availability_zone_id": node.az_id,
        }
        log.info(f"node_register({body})")
        self.request(
            "POST",
            f"{self.api}/control/v1/node",
            json=body,
            headers=self.headers(TokenScope.ADMIN),
        )

    def node_delete(self, node_id):
        log.info(f"node_delete({node_id})")
        self.request(
            "DELETE",
            f"{self.api}/control/v1/node/{node_id}",
            headers=self.headers(TokenScope.ADMIN),
        )

    def node_drain(self, node_id):
        log.info(f"node_drain({node_id})")
        self.request(
            "PUT",
            f"{self.api}/control/v1/node/{node_id}/drain",
            headers=self.headers(TokenScope.ADMIN),
        )

    def cancel_node_drain(self, node_id):
        log.info(f"cancel_node_drain({node_id})")
        self.request(
            "DELETE",
            f"{self.api}/control/v1/node/{node_id}/drain",
            headers=self.headers(TokenScope.ADMIN),
        )

    def node_fill(self, node_id):
        log.info(f"node_fill({node_id})")
        self.request(
            "PUT",
            f"{self.api}/control/v1/node/{node_id}/fill",
            headers=self.headers(TokenScope.ADMIN),
        )

    def cancel_node_fill(self, node_id):
        log.info(f"cancel_node_fill({node_id})")
        self.request(
            "DELETE",
            f"{self.api}/control/v1/node/{node_id}/fill",
            headers=self.headers(TokenScope.ADMIN),
        )

    def node_status(self, node_id):
        response = self.request(
            "GET",
            f"{self.api}/control/v1/node/{node_id}",
            headers=self.headers(TokenScope.ADMIN),
        )
        return response.json()

    def get_leader(self):
        response = self.request(
            "GET",
            f"{self.api}/control/v1/leader",
            headers=self.headers(TokenScope.ADMIN),
        )
        return response.json()

    def node_list(self):
        response = self.request(
            "GET",
            f"{self.api}/control/v1/node",
            headers=self.headers(TokenScope.ADMIN),
        )
        return response.json()

    def tenant_list(self):
        response = self.request(
            "GET",
            f"{self.api}/debug/v1/tenant",
            headers=self.headers(TokenScope.ADMIN),
        )
        return response.json()

    def node_configure(self, node_id, body: dict[str, Any]):
        log.info(f"node_configure({node_id}, {body})")
        body["node_id"] = node_id
        self.request(
            "PUT",
            f"{self.api}/control/v1/node/{node_id}/config",
            json=body,
            headers=self.headers(TokenScope.ADMIN),
        )

    def tenant_create(
        self,
        tenant_id: TenantId,
        shard_count: Optional[int] = None,
        shard_stripe_size: Optional[int] = None,
        tenant_config: Optional[Dict[Any, Any]] = None,
        placement_policy: Optional[Union[Dict[Any, Any] | str]] = None,
    ):
        """
        Use this rather than pageserver_api() when you need to include shard parameters
        """
        body: Dict[str, Any] = {"new_tenant_id": str(tenant_id)}

        if shard_count is not None:
            shard_params = {"count": shard_count}
            if shard_stripe_size is not None:
                shard_params["stripe_size"] = shard_stripe_size
            else:
                shard_params["stripe_size"] = 32768

            body["shard_parameters"] = shard_params

        if tenant_config is not None:
            for k, v in tenant_config.items():
                body[k] = v

        body["placement_policy"] = placement_policy

        response = self.request(
            "POST",
            f"{self.api}/v1/tenant",
            json=body,
            headers=self.headers(TokenScope.PAGE_SERVER_API),
        )
        response.raise_for_status()
        log.info(f"tenant_create success: {response.json()}")

    def locate(self, tenant_id: TenantId) -> list[dict[str, Any]]:
        """
        :return: list of {"shard_id": "", "node_id": int, "listen_pg_addr": str, "listen_pg_port": int, "listen_http_addr": str, "listen_http_port": int}
        """
        response = self.request(
            "GET",
            f"{self.api}/debug/v1/tenant/{tenant_id}/locate",
            headers=self.headers(TokenScope.ADMIN),
        )
        body = response.json()
        shards: list[dict[str, Any]] = body["shards"]
        return shards

    def tenant_describe(self, tenant_id: TenantId):
        """
        :return: list of {"shard_id": "", "node_id": int, "listen_pg_addr": str, "listen_pg_port": int, "listen_http_addr: str, "listen_http_port: int, preferred_az_id: str}
        """
        response = self.request(
            "GET",
            f"{self.api}/control/v1/tenant/{tenant_id}",
            headers=self.headers(TokenScope.ADMIN),
        )
        response.raise_for_status()
        return response.json()

    def nodes(self):
        """
        :return: list of {"id": ""}
        """
        response = self.request(
            "GET",
            f"{self.api}/control/v1/node",
            headers=self.headers(TokenScope.ADMIN),
        )
        response.raise_for_status()
        return response.json()

    def node_shards(self, node_id: NodeId):
        """
        :return: list of {"shard_id": "", "is_secondary": bool}
        """
        response = self.request(
            "GET",
            f"{self.api}/control/v1/node/{node_id}/shards",
            headers=self.headers(TokenScope.ADMIN),
        )
        response.raise_for_status()
        return response.json()

    def tenant_shard_split(
        self, tenant_id: TenantId, shard_count: int, shard_stripe_size: Optional[int] = None
    ) -> list[TenantShardId]:
        response = self.request(
            "PUT",
            f"{self.api}/control/v1/tenant/{tenant_id}/shard_split",
            json={"new_shard_count": shard_count, "new_stripe_size": shard_stripe_size},
            headers=self.headers(TokenScope.ADMIN),
        )
        body = response.json()
        log.info(f"tenant_shard_split success: {body}")
        shards: list[TenantShardId] = body["new_shards"]
        return shards

    def tenant_shard_migrate(self, tenant_shard_id: TenantShardId, dest_ps_id: int):
        self.request(
            "PUT",
            f"{self.api}/control/v1/tenant/{tenant_shard_id}/migrate",
            json={"tenant_shard_id": str(tenant_shard_id), "node_id": dest_ps_id},
            headers=self.headers(TokenScope.ADMIN),
        )
        log.info(f"Migrated tenant {tenant_shard_id} to pageserver {dest_ps_id}")
        assert self.env.get_tenant_pageserver(tenant_shard_id).id == dest_ps_id

    def tenant_policy_update(self, tenant_id: TenantId, body: dict[str, Any]):
        log.info(f"tenant_policy_update({tenant_id}, {body})")
        self.request(
            "PUT",
            f"{self.api}/control/v1/tenant/{tenant_id}/policy",
            json=body,
            headers=self.headers(TokenScope.ADMIN),
        )

    def tenant_import(self, tenant_id: TenantId):
        self.request(
            "POST",
            f"{self.api}/debug/v1/tenant/{tenant_id}/import",
            headers=self.headers(TokenScope.ADMIN),
        )

    def reconcile_all(self):
        r = self.request(
            "POST",
            f"{self.api}/debug/v1/reconcile_all",
            headers=self.headers(TokenScope.ADMIN),
        )
        r.raise_for_status()
        n = r.json()
        log.info(f"reconcile_all waited for {n} shards")
        return n

    def reconcile_until_idle(self, timeout_secs=30):
        start_at = time.time()
        n = 1
        delay_sec = 0.5
        delay_max = 5
        while n > 0:
            n = self.reconcile_all()
            if n == 0:
                break
            elif time.time() - start_at > timeout_secs:
                raise RuntimeError("Timeout in reconcile_until_idle")
            else:
                # Don't call again right away: if we're waiting for many reconciles that
                # are blocked on the concurrency limit, it slows things down to call
                # reconcile_all frequently.
                time.sleep(delay_sec)
                delay_sec *= 2
                delay_sec = min(delay_sec, delay_max)

    def consistency_check(self):
        """
        Throw an exception if the service finds any inconsistencies in its state
        """
        self.request(
            "POST",
            f"{self.api}/debug/v1/consistency_check",
            headers=self.headers(TokenScope.ADMIN),
        )
        log.info("storage controller passed consistency check")

    def node_registered(self, node_id: int) -> bool:
        """
        Returns true if the storage controller can confirm
        it knows of pageserver with 'node_id'
        """
        try:
            self.node_status(node_id)
        except StorageControllerApiException as e:
            if e.status_code == 404:
                return False
            else:
                raise e

        return True

    def poll_node_status(
        self,
        node_id: int,
        desired_availability: Optional[PageserverAvailability],
        desired_scheduling_policy: Optional[PageserverSchedulingPolicy],
        max_attempts: int,
        backoff: int,
    ):
        """
        Poll the node status until it reaches 'desired_scheduling_policy' and 'desired_availability'
        or 'max_attempts' have been exhausted
        """
        log.info(
            f"Polling {node_id} for {desired_scheduling_policy} scheduling policy and {desired_availability} availability"
        )
        while max_attempts > 0:
            try:
                status = self.node_status(node_id)
                policy = status["scheduling"]
                availability = status["availability"]
                if (desired_scheduling_policy is None or policy == desired_scheduling_policy) and (
                    desired_availability is None or availability == desired_availability
                ):
                    return
                else:
                    max_attempts -= 1
                    log.info(
                        f"Status call returned {policy=} {availability=} ({max_attempts} attempts left)"
                    )

                    if max_attempts == 0:
                        raise AssertionError(
                            f"Status for {node_id=} did not reach {desired_scheduling_policy=} {desired_availability=}"
                        )

                    time.sleep(backoff)
            except StorageControllerApiException as e:
                max_attempts -= 1
                log.info(f"Status call failed ({max_attempts} retries left): {e}")

                if max_attempts == 0:
                    raise e

                time.sleep(backoff)

    def metadata_health_update(self, healthy: List[TenantShardId], unhealthy: List[TenantShardId]):
        body: Dict[str, Any] = {
            "healthy_tenant_shards": [str(t) for t in healthy],
            "unhealthy_tenant_shards": [str(t) for t in unhealthy],
        }

        self.request(
            "POST",
            f"{self.api}/control/v1/metadata_health/update",
            json=body,
            headers=self.headers(TokenScope.SCRUBBER),
        )

    def metadata_health_list_unhealthy(self):
        response = self.request(
            "GET",
            f"{self.api}/control/v1/metadata_health/unhealthy",
            headers=self.headers(TokenScope.ADMIN),
        )
        return response.json()

    def metadata_health_list_outdated(self, duration: str):
        body: Dict[str, Any] = {"not_scrubbed_for": duration}

        response = self.request(
            "POST",
            f"{self.api}/control/v1/metadata_health/outdated",
            json=body,
            headers=self.headers(TokenScope.ADMIN),
        )
        return response.json()

    def metadata_health_is_healthy(self, outdated_duration: str = "1h") -> bool:
        """Metadata is healthy if there is no unhealthy or outdated health records."""

        unhealthy = self.metadata_health_list_unhealthy()
        outdated = self.metadata_health_list_outdated(outdated_duration)

        healthy = (
            len(unhealthy["unhealthy_tenant_shards"]) == 0 and len(outdated["health_records"]) == 0
        )
        if not healthy:
            log.info(f"{unhealthy=}, {outdated=}")
        return healthy

    def step_down(self):
        log.info("Asking storage controller to step down")
        response = self.request(
            "PUT",
            f"{self.api}/control/v1/step_down",
            headers=self.headers(TokenScope.ADMIN),
        )

        response.raise_for_status()
        return response.json()

    def configure_failpoints(self, config_strings: Tuple[str, str] | List[Tuple[str, str]]):
        if isinstance(config_strings, tuple):
            pairs = [config_strings]
        else:
            pairs = config_strings

        log.info(f"Requesting config failpoints: {repr(pairs)}")

        res = self.request(
            "PUT",
            f"{self.api}/debug/v1/failpoints",
            json=[{"name": name, "actions": actions} for name, actions in pairs],
            headers=self.headers(TokenScope.ADMIN),
        )
        log.info(f"Got failpoints request response code {res.status_code}")
        res.raise_for_status()

    def get_tenants_placement(self) -> defaultdict[str, Dict[str, Any]]:
        """
        Get the intent and observed placements of all tenants known to the storage controller.
        """
        tenants = self.tenant_list()

        tenant_placement: defaultdict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "observed": {"attached": None, "secondary": []},
                "intent": {"attached": None, "secondary": []},
            }
        )

        for t in tenants:
            for node_id, loc_state in t["observed"]["locations"].items():
                if (
                    loc_state is not None
                    and "conf" in loc_state
                    and loc_state["conf"] is not None
                    and loc_state["conf"]["mode"]
                    in set(["AttachedSingle", "AttachedMulti", "AttachedStale"])
                ):
                    tenant_placement[t["tenant_shard_id"]]["observed"]["attached"] = int(node_id)

                if (
                    loc_state is not None
                    and "conf" in loc_state
                    and loc_state["conf"] is not None
                    and loc_state["conf"]["mode"] == "Secondary"
                ):
                    tenant_placement[t["tenant_shard_id"]]["observed"]["secondary"].append(
                        int(node_id)
                    )

            if "attached" in t["intent"]:
                tenant_placement[t["tenant_shard_id"]]["intent"]["attached"] = t["intent"][
                    "attached"
                ]

            if "secondary" in t["intent"]:
                tenant_placement[t["tenant_shard_id"]]["intent"]["secondary"] += t["intent"][
                    "secondary"
                ]

        return tenant_placement

    def warm_up_all_secondaries(self):
        log.info("Warming up all secondary locations")

        tenant_placement = self.get_tenants_placement()
        for tid, placement in tenant_placement.items():
            assert placement["observed"]["attached"] is not None
            primary_id = placement["observed"]["attached"]

            assert len(placement["observed"]["secondary"]) == 1
            secondary_id = placement["observed"]["secondary"][0]

            parsed_tid = TenantShardId.parse(tid)
            self.env.get_pageserver(primary_id).http_client().tenant_heatmap_upload(parsed_tid)
            self.env.get_pageserver(secondary_id).http_client().tenant_secondary_download(
                parsed_tid, wait_ms=250
            )

    def get_leadership_status(self) -> StorageControllerLeadershipStatus:
        metric_values = {}
        for status in StorageControllerLeadershipStatus:
            metric_value = self.get_metric_value(
                "storage_controller_leadership_status", filter={"status": status}
            )
            metric_values[status] = metric_value

        assert list(metric_values.values()).count(1) == 1

        for status, metric_value in metric_values.items():
            if metric_value == 1:
                return status

        raise AssertionError("unreachable")

    def on_safekeeper_deploy(self, id: int, body: dict[str, Any]):
        self.request(
            "POST",
            f"{self.api}/control/v1/safekeeper/{id}",
            headers=self.headers(TokenScope.ADMIN),
            json=body,
        )

    def get_safekeeper(self, id: int) -> Optional[dict[str, Any]]:
        try:
            response = self.request(
                "GET",
                f"{self.api}/control/v1/safekeeper/{id}",
                headers=self.headers(TokenScope.ADMIN),
            )
            json = response.json()
            assert isinstance(json, dict)
            return json
        except StorageControllerApiException as e:
            if e.status_code == 404:
                return None
            raise e

    def set_preferred_azs(self, preferred_azs: dict[TenantShardId, str]) -> list[TenantShardId]:
        response = self.request(
            "PUT",
            f"{self.api}/control/v1/preferred_azs",
            headers=self.headers(TokenScope.ADMIN),
            json={str(tid): az for tid, az in preferred_azs.items()},
        )

        response.raise_for_status()
        return [TenantShardId.parse(tid) for tid in response.json()["updated"]]

    def __enter__(self) -> "NeonStorageController":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        self.stop(immediate=True)


class NeonProxiedStorageController(NeonStorageController):
    def __init__(self, env: NeonEnv, proxy_port: int, auth_enabled: bool):
        super(NeonProxiedStorageController, self).__init__(env, proxy_port, auth_enabled)
        self.instances: dict[int, dict[str, Any]] = {}

    def start(
        self,
        timeout_in_seconds: Optional[int] = None,
        instance_id: Optional[int] = None,
        base_port: Optional[int] = None,
    ):
        assert instance_id is not None and base_port is not None

        self.env.neon_cli.storage_controller_start(timeout_in_seconds, instance_id, base_port)
        self.instances[instance_id] = {"running": True}

        self.running = True
        return self

    def stop_instance(
        self, immediate: bool = False, instance_id: Optional[int] = None
    ) -> "NeonStorageController":
        assert instance_id in self.instances
        if self.instances[instance_id]["running"]:
            self.env.neon_cli.storage_controller_stop(immediate, instance_id)
            self.instances[instance_id]["running"] = False

        self.running = any(meta["running"] for meta in self.instances.values())
        return self

    def stop(self, immediate: bool = False) -> "NeonStorageController":
        for iid, details in self.instances.items():
            if details["running"]:
                self.env.neon_cli.storage_controller_stop(immediate, iid)
                self.instances[iid]["running"] = False

        self.running = False
        return self

    def assert_no_errors(self):
        for instance_id in self.instances.keys():
            assert_no_errors(
                self.env.repo_dir / f"storage_controller_{instance_id}" / "storage_controller.log",
                "storage_controller",
                self.allowed_errors,
            )

    def log_contains(
        self, pattern: str, offset: None | LogCursor = None
    ) -> Optional[Tuple[str, LogCursor]]:
        raise NotImplementedError()


@dataclass
class LogCursor:
    _line_no: int


class NeonPageserver(PgProtocol, LogUtils):
    """
    An object representing a running pageserver.
    """

    TEMP_FILE_SUFFIX = "___temp"

    def __init__(self, env: NeonEnv, id: int, port: PageserverPort, az_id: str):
        super().__init__(host="localhost", port=port.pg, user="cloud_admin")
        self.env = env
        self.id = id
        self.az_id = az_id
        self.running = False
        self.service_port = port
        self.version = env.get_binary_version("pageserver")
        self.logfile = self.workdir / "pageserver.log"
        # After a test finishes, we will scrape the log to see if there are any
        # unexpected error messages. If your test expects an error, add it to
        # 'allowed_errors' in the test with something like:
        #
        # env.pageserver.allowed_errors.append(".*could not open garage door.*")
        #
        # The entries in the list are regular experessions.
        self.allowed_errors: List[str] = list(DEFAULT_PAGESERVER_ALLOWED_ERRORS)

    def timeline_dir(
        self,
        tenant_shard_id: Union[TenantId, TenantShardId],
        timeline_id: Optional[TimelineId] = None,
    ) -> Path:
        """Get a timeline directory's path based on the repo directory of the test environment"""
        if timeline_id is None:
            return self.tenant_dir(tenant_shard_id) / "timelines"
        return self.tenant_dir(tenant_shard_id) / "timelines" / str(timeline_id)

    def tenant_dir(
        self,
        tenant_shard_id: Optional[Union[TenantId, TenantShardId]] = None,
    ) -> Path:
        """Get a tenant directory's path based on the repo directory of the test environment"""
        if tenant_shard_id is None:
            return self.workdir / "tenants"
        return self.workdir / "tenants" / str(tenant_shard_id)

    @property
    def config_toml_path(self) -> Path:
        return self.workdir / "pageserver.toml"

    def edit_config_toml(self, edit_fn: Callable[[Dict[str, Any]], T]) -> T:
        """
        Edit the pageserver's config toml file in place.
        """
        path = self.config_toml_path
        with open(path, "r") as f:
            config = toml.load(f)
        res = edit_fn(config)
        with open(path, "w") as f:
            toml.dump(config, f)
        return res

    def patch_config_toml_nonrecursive(self, patch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Non-recursively merge the given `patch` dict into the existing config toml, using `dict.update()`.
        Returns the replaced values.
        If there was no previous value, the key is mapped to None.
        This allows to restore the original value by calling this method with the returned dict.
        """
        replacements = {}

        def doit(config: Dict[str, Any]):
            while len(patch) > 0:
                key, new = patch.popitem()
                old = config.get(key, None)
                config[key] = new
                replacements[key] = old

        self.edit_config_toml(doit)
        return replacements

    def start(
        self,
        extra_env_vars: Optional[Dict[str, str]] = None,
        timeout_in_seconds: Optional[int] = None,
    ) -> "NeonPageserver":
        """
        Start the page server.
        `overrides` allows to add some config to this pageserver start.
        Returns self.
        """
        assert self.running is False

        self.env.neon_cli.pageserver_start(
            self.id, extra_env_vars=extra_env_vars, timeout_in_seconds=timeout_in_seconds
        )
        self.running = True

        if self.env.storage_controller.running and self.env.storage_controller.node_registered(
            self.id
        ):
            self.env.storage_controller.poll_node_status(
                self.id, PageserverAvailability.ACTIVE, None, max_attempts=20, backoff=1
            )

        return self

    def stop(self, immediate: bool = False) -> "NeonPageserver":
        """
        Stop the page server.
        Returns self.
        """
        if self.running:
            self.env.neon_cli.pageserver_stop(self.id, immediate)
            self.running = False
        return self

    def restart(
        self,
        immediate: bool = False,
        timeout_in_seconds: Optional[int] = None,
    ):
        """
        High level wrapper for restart: restarts the process, and waits for
        tenant state to stabilize.
        """
        self.stop(immediate=immediate)
        self.start(timeout_in_seconds=timeout_in_seconds)
        self.quiesce_tenants()

    def quiesce_tenants(self):
        """
        Wait for all tenants to enter a stable state (Active or Broken)

        Call this after restarting the pageserver, or after attaching a tenant,
        to ensure that it is ready for use.
        """

        stable_states = {"Active", "Broken"}

        client = self.http_client()

        def complete():
            log.info("Checking tenants...")
            tenants = client.tenant_list()
            log.info(f"Tenant list: {tenants}...")
            any_unstable = any((t["state"]["slug"] not in stable_states) for t in tenants)
            if any_unstable:
                for t in tenants:
                    log.info(f"Waiting for tenant {t['id']} in state {t['state']['slug']}")
            log.info(f"any_unstable={any_unstable}")
            assert not any_unstable

        wait_until(20, 0.5, complete)

    def __enter__(self) -> "NeonPageserver":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        self.stop(immediate=True)

    def is_testing_enabled_or_skip(self):
        if '"testing"' not in self.version:
            pytest.skip("pageserver was built without 'testing' feature")

    def http_client(
        self, auth_token: Optional[str] = None, retries: Optional[Retry] = None
    ) -> PageserverHttpClient:
        return PageserverHttpClient(
            port=self.service_port.http,
            auth_token=auth_token,
            is_testing_enabled_or_skip=self.is_testing_enabled_or_skip,
            retries=retries,
        )

    @property
    def workdir(self) -> Path:
        return self.env.repo_dir / f"pageserver_{self.id}"

    def assert_no_errors(self):
        assert_no_errors(
            self.workdir / "pageserver.log", f"pageserver_{self.id}", self.allowed_errors
        )

    def assert_no_metric_errors(self):
        """
        Certain metrics should _always_ be zero: they track conditions that indicate a bug.
        """
        if not self.running:
            log.info(f"Skipping metrics check on pageserver {self.id}, it is not running")
            return

        for metric in [
            "pageserver_tenant_manager_unexpected_errors_total",
            "pageserver_deletion_queue_unexpected_errors_total",
        ]:
            value = self.http_client().get_metric_value(metric)
            assert value == 0, f"Nonzero {metric} == {value}"

    def tenant_attach(
        self,
        tenant_id: TenantId,
        config: None | Dict[str, Any] = None,
        generation: Optional[int] = None,
        override_storage_controller_generation: bool = False,
    ):
        """
        Tenant attachment passes through here to acquire a generation number before proceeding
        to call into the pageserver HTTP client.
        """
        client = self.http_client()
        if generation is None:
            generation = self.env.storage_controller.attach_hook_issue(tenant_id, self.id)
        elif override_storage_controller_generation:
            generation = self.env.storage_controller.attach_hook_issue(
                tenant_id, self.id, generation
            )
        return client.tenant_attach(
            tenant_id,
            generation,
            config,
        )

    def tenant_detach(self, tenant_id: TenantId):
        self.env.storage_controller.attach_hook_drop(tenant_id)

        client = self.http_client()
        return client.tenant_detach(tenant_id)

    def tenant_location_configure(self, tenant_id: TenantId, config: dict[str, Any], **kwargs):
        if config["mode"].startswith("Attached") and "generation" not in config:
            config["generation"] = self.env.storage_controller.attach_hook_issue(tenant_id, self.id)

        client = self.http_client()
        return client.tenant_location_conf(tenant_id, config, **kwargs)

    def read_tenant_location_conf(
        self, tenant_shard_id: Union[TenantId, TenantShardId]
    ) -> dict[str, Any]:
        path = self.tenant_dir(tenant_shard_id) / "config-v1"
        log.info(f"Reading location conf from {path}")
        bytes = open(path, "r").read()
        try:
            decoded: dict[str, Any] = toml.loads(bytes)
            return decoded
        except:
            log.error(f"Failed to decode LocationConf, raw content ({len(bytes)} bytes): {bytes}")
            raise

    def tenant_create(
        self,
        tenant_id: TenantId,
        conf: Optional[Dict[str, Any]] = None,
        auth_token: Optional[str] = None,
        generation: Optional[int] = None,
    ) -> TenantId:
        if generation is None:
            generation = self.env.storage_controller.attach_hook_issue(tenant_id, self.id)
        client = self.http_client(auth_token=auth_token)

        conf = conf or {}

        client.tenant_location_conf(
            tenant_id,
            {
                "mode": "AttachedSingle",
                "generation": generation,
                "tenant_conf": conf,
                "secondary_conf": None,
            },
        )
        return tenant_id

    def list_layers(
        self, tenant_id: Union[TenantId, TenantShardId], timeline_id: TimelineId
    ) -> list[Path]:
        """
        Inspect local storage on a pageserver to discover which layer files are present.

        :return: list of relative paths to layers, from the timeline root.
        """
        timeline_path = self.timeline_dir(tenant_id, timeline_id)

        def relative(p: Path) -> Path:
            return p.relative_to(timeline_path)

        return sorted(
            list(
                map(
                    relative,
                    filter(
                        lambda path: path.name != "metadata"
                        and "ephemeral" not in path.name
                        and "temp" not in path.name,
                        timeline_path.glob("*"),
                    ),
                )
            )
        )

    def layer_exists(
        self, tenant_id: TenantId, timeline_id: TimelineId, layer_name: LayerName
    ) -> bool:
        layers = self.list_layers(tenant_id, timeline_id)
        return layer_name in [parse_layer_file_name(p.name) for p in layers]


class PgBin:
    """A helper class for executing postgres binaries"""

    def __init__(self, log_dir: Path, pg_distrib_dir: Path, pg_version: PgVersion):
        self.log_dir = log_dir
        self.pg_version = pg_version
        self.pg_bin_path = pg_distrib_dir / pg_version.v_prefixed / "bin"
        self.pg_lib_dir = pg_distrib_dir / pg_version.v_prefixed / "lib"
        self.env = os.environ.copy()
        self.env["LD_LIBRARY_PATH"] = str(self.pg_lib_dir)

    def _fixpath(self, command: List[str]):
        if "/" not in str(command[0]):
            command[0] = str(self.pg_bin_path / command[0])

    def _build_env(self, env_add: Optional[Env]) -> Env:
        if env_add is None:
            return self.env
        env = self.env.copy()
        env.update(env_add)
        return env

    def _log_env(self, env: dict[str, str]) -> None:
        env_s = {}
        for k, v in env.items():
            if k.startswith("PG") and k != "PGPASSWORD":
                env_s[k] = v
        log.debug(f"Environment: {env_s}")

    def run_nonblocking(
        self,
        command: List[str],
        env: Optional[Env] = None,
        cwd: Optional[Union[str, Path]] = None,
    ) -> subprocess.Popen[Any]:
        """
        Run one of the postgres binaries, not waiting for it to finish

        The command should be in list form, e.g. ['pgbench', '-p', '55432']

        All the necessary environment variables will be set.

        If the first argument (the command name) doesn't include a path (no '/'
        characters present), then it will be edited to include the correct path.

        If you want stdout/stderr captured to files, use `run_capture` instead.
        """
        self._fixpath(command)
        log.info(f"Running command '{' '.join(command)}'")
        env = self._build_env(env)
        self._log_env(env)
        return subprocess.Popen(command, env=env, cwd=cwd, stdout=subprocess.PIPE, text=True)

    def run(
        self,
        command: List[str],
        env: Optional[Env] = None,
        cwd: Optional[Union[str, Path]] = None,
    ) -> None:
        """
        Run one of the postgres binaries, waiting for it to finish

        The command should be in list form, e.g. ['pgbench', '-p', '55432']

        All the necessary environment variables will be set.

        If the first argument (the command name) doesn't include a path (no '/'
        characters present), then it will be edited to include the correct path.

        If you want stdout/stderr captured to files, use `run_capture` instead.
        """
        proc = self.run_nonblocking(command, env, cwd)
        proc.wait()
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, proc.args)

    def run_capture(
        self,
        command: List[str],
        env: Optional[Env] = None,
        cwd: Optional[str] = None,
        with_command_header=True,
        **popen_kwargs: Any,
    ) -> str:
        """
        Run one of the postgres binaries, with stderr and stdout redirected to a file.

        This is just like `run`, but for chatty programs. Returns basepath for files
        with captured output.
        """

        self._fixpath(command)
        log.info(f"Running command '{' '.join(command)}'")
        env = self._build_env(env)
        self._log_env(env)
        base_path, _, _ = subprocess_capture(
            self.log_dir,
            command,
            env=env,
            cwd=cwd,
            check=True,
            with_command_header=with_command_header,
            **popen_kwargs,
        )
        return base_path

    def get_pg_controldata_checkpoint_lsn(self, pgdata: Path) -> Lsn:
        """
        Run pg_controldata on given datadir and extract checkpoint lsn.
        """

        pg_controldata_path = self.pg_bin_path / "pg_controldata"
        cmd = f"{pg_controldata_path} -D {pgdata}"
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        checkpoint_lsn = re.findall(
            "Latest checkpoint location:\\s+([0-9A-F]+/[0-9A-F]+)", result.stdout
        )[0]
        log.info(f"last checkpoint at {checkpoint_lsn}")
        return Lsn(checkpoint_lsn)

    def take_fullbackup(
        self,
        pageserver: NeonPageserver,
        tenant: TenantId,
        timeline: TimelineId,
        lsn: Lsn,
        output: Path,
    ):
        """
        Request fullbackup from pageserver, store it at 'output'.
        """
        cmd = [
            "psql",
            "--no-psqlrc",
            pageserver.connstr(),
            "-c",
            f"fullbackup {tenant} {timeline} {lsn}",
            "-o",
            str(output),
        ]
        self.run_capture(cmd)


@pytest.fixture(scope="function")
def pg_bin(test_output_dir: Path, pg_distrib_dir: Path, pg_version: PgVersion) -> PgBin:
    return PgBin(test_output_dir, pg_distrib_dir, pg_version)


# TODO make port an optional argument
class VanillaPostgres(PgProtocol):
    def __init__(self, pgdatadir: Path, pg_bin: PgBin, port: int, init: bool = True):
        super().__init__(host="localhost", port=port, dbname="postgres")
        self.pgdatadir = pgdatadir
        self.pg_bin = pg_bin
        self.running = False
        if init:
            self.pg_bin.run_capture(["initdb", "-D", str(pgdatadir)])
        self.configure([f"port = {port}\n"])

    def enable_tls(self):
        assert not self.running
        # generate self-signed certificate
        subprocess.run(
            [
                "openssl",
                "req",
                "-new",
                "-x509",
                "-days",
                "365",
                "-nodes",
                "-text",
                "-out",
                self.pgdatadir / "server.crt",
                "-keyout",
                self.pgdatadir / "server.key",
                "-subj",
                "/CN=localhost",
            ]
        )
        # configure postgresql.conf
        self.configure(
            [
                "ssl = on",
                "ssl_cert_file = 'server.crt'",
                "ssl_key_file = 'server.key'",
            ]
        )

    def configure(self, options: List[str]):
        """Append lines into postgresql.conf file."""
        assert not self.running
        with open(os.path.join(self.pgdatadir, "postgresql.conf"), "a") as conf_file:
            conf_file.write("\n".join(options))
            conf_file.write("\n")

    def edit_hba(self, hba: List[str]):
        """Prepend hba lines into pg_hba.conf file."""
        assert not self.running
        with open(os.path.join(self.pgdatadir, "pg_hba.conf"), "r+") as conf_file:
            data = conf_file.read()
            conf_file.seek(0)
            conf_file.write("\n".join(hba) + "\n")
            conf_file.write(data)

    def start(self, log_path: Optional[str] = None):
        assert not self.running
        self.running = True

        log_path = log_path or os.path.join(self.pgdatadir, "pg.log")

        self.pg_bin.run_capture(
            ["pg_ctl", "-w", "-D", str(self.pgdatadir), "-l", log_path, "start"]
        )

    def stop(self):
        assert self.running
        self.running = False
        self.pg_bin.run_capture(["pg_ctl", "-w", "-D", str(self.pgdatadir), "stop"])

    def get_subdir_size(self, subdir: Path) -> int:
        """Return size of pgdatadir subdirectory in bytes."""
        return get_dir_size(self.pgdatadir / subdir)

    def __enter__(self) -> "VanillaPostgres":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        if self.running:
            self.stop()


@pytest.fixture(scope="function")
def vanilla_pg(
    test_output_dir: Path,
    port_distributor: PortDistributor,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
) -> Iterator[VanillaPostgres]:
    pgdatadir = test_output_dir / "pgdata-vanilla"
    pg_bin = PgBin(test_output_dir, pg_distrib_dir, pg_version)
    port = port_distributor.get_port()
    with VanillaPostgres(pgdatadir, pg_bin, port) as vanilla_pg:
        vanilla_pg.configure(["shared_preload_libraries='neon_rmgr'"])
        yield vanilla_pg


class RemotePostgres(PgProtocol):
    def __init__(self, pg_bin: PgBin, remote_connstr: str):
        super().__init__(**parse_dsn(remote_connstr))
        self.pg_bin = pg_bin
        # The remote server is assumed to be running already
        self.running = True

    def configure(self, options: List[str]):
        raise Exception("cannot change configuration of remote Posgres instance")

    def start(self):
        raise Exception("cannot start a remote Postgres instance")

    def stop(self):
        raise Exception("cannot stop a remote Postgres instance")

    def get_subdir_size(self, subdir) -> int:
        # TODO: Could use the server's Generic File Access functions if superuser.
        # See https://www.postgresql.org/docs/14/functions-admin.html#FUNCTIONS-ADMIN-GENFILE
        raise Exception("cannot get size of a Postgres instance")

    def __enter__(self) -> "RemotePostgres":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        # do nothing
        pass


@pytest.fixture(scope="function")
def benchmark_project_pub(neon_api: NeonAPI, pg_version: PgVersion) -> NeonApiEndpoint:
    project_id = os.getenv("BENCHMARK_PROJECT_ID_PUB")
    return NeonApiEndpoint(neon_api, pg_version, project_id)


@pytest.fixture(scope="function")
def benchmark_project_sub(neon_api: NeonAPI, pg_version: PgVersion) -> NeonApiEndpoint:
    project_id = os.getenv("BENCHMARK_PROJECT_ID_SUB")
    return NeonApiEndpoint(neon_api, pg_version, project_id)


@pytest.fixture(scope="function")
def remote_pg(
    test_output_dir: Path, pg_distrib_dir: Path, pg_version: PgVersion
) -> Iterator[RemotePostgres]:
    pg_bin = PgBin(test_output_dir, pg_distrib_dir, pg_version)

    connstr = os.getenv("BENCHMARK_CONNSTR")
    if connstr is None:
        raise ValueError("no connstr provided, use BENCHMARK_CONNSTR environment variable")

    host = parse_dsn(connstr).get("host", "")
    is_neon = host.endswith(".neon.build")

    start_ms = int(datetime.utcnow().timestamp() * 1000)
    with RemotePostgres(pg_bin, connstr) as remote_pg:
        if is_neon:
            timeline_id = TimelineId(remote_pg.safe_psql("SHOW neon.timeline_id")[0][0])

        yield remote_pg

    end_ms = int(datetime.utcnow().timestamp() * 1000)
    if is_neon:
        # Add 10s margin to the start and end times
        allure_add_grafana_links(
            host,
            timeline_id,
            start_ms - 10_000,
            end_ms + 10_000,
        )


class PSQL:
    """
    Helper class to make it easier to run psql in the proxy tests.
    Copied and modified from PSQL from cloud/tests_e2e/common/psql.py
    """

    path: str
    database_url: str

    def __init__(
        self,
        path: str = "psql",
        host: str = "127.0.0.1",
        port: int = 5432,
    ):
        search_path = None
        if (d := os.getenv("POSTGRES_DISTRIB_DIR")) is not None and (
            v := os.getenv("DEFAULT_PG_VERSION")
        ) is not None:
            search_path = Path(d) / f"v{v}" / "bin"

        full_path = shutil.which(path, path=search_path)
        assert full_path is not None

        self.path = full_path
        self.database_url = f"postgres://{host}:{port}/main?options=project%3Dgeneric-project-name"

    async def run(self, query: Optional[str] = None) -> asyncio.subprocess.Process:
        run_args = [self.path, "--no-psqlrc", "--quiet", "--tuples-only", self.database_url]
        if query is not None:
            run_args += ["--command", query]

        log.info(f"Run psql: {subprocess.list2cmdline(run_args)}")
        return await asyncio.create_subprocess_exec(
            *run_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={"LC_ALL": "C", **os.environ},  # one locale to rule them all
        )


class NeonProxy(PgProtocol):
    link_auth_uri: str = "http://dummy-uri"

    class AuthBackend(abc.ABC):
        """All auth backends must inherit from this class"""

        @property
        def default_conn_url(self) -> Optional[str]:
            return None

        @abc.abstractmethod
        def extra_args(self) -> list[str]:
            pass

    class Link(AuthBackend):
        def extra_args(self) -> list[str]:
            return [
                # Link auth backend params
                *["--auth-backend", "link"],
                *["--uri", NeonProxy.link_auth_uri],
                *["--allow-self-signed-compute", "true"],
            ]

    class Console(AuthBackend):
        def __init__(self, endpoint: str, fixed_rate_limit: Optional[int] = None):
            self.endpoint = endpoint
            self.fixed_rate_limit = fixed_rate_limit

        def extra_args(self) -> list[str]:
            args = [
                # Console auth backend params
                *["--auth-backend", "console"],
                *["--auth-endpoint", self.endpoint],
                *["--sql-over-http-pool-opt-in", "false"],
            ]
            if self.fixed_rate_limit is not None:
                args += [
                    *["--disable-dynamic-rate-limiter", "false"],
                    *["--rate-limit-algorithm", "aimd"],
                    *["--initial-limit", str(1)],
                    *["--rate-limiter-timeout", "1s"],
                    *["--aimd-min-limit", "0"],
                    *["--aimd-increase-by", "1"],
                    *["--wake-compute-cache", "size=0"],  # Disable cache to test rate limiter.
                ]
            return args

    @dataclass(frozen=True)
    class Postgres(AuthBackend):
        pg_conn_url: str

        @property
        def default_conn_url(self) -> Optional[str]:
            return self.pg_conn_url

        def extra_args(self) -> list[str]:
            return [
                # Postgres auth backend params
                *["--auth-backend", "postgres"],
                *["--auth-endpoint", self.pg_conn_url],
            ]

    def __init__(
        self,
        neon_binpath: Path,
        test_output_dir: Path,
        proxy_port: int,
        http_port: int,
        mgmt_port: int,
        external_http_port: int,
        auth_backend: NeonProxy.AuthBackend,
        metric_collection_endpoint: Optional[str] = None,
        metric_collection_interval: Optional[str] = None,
    ):
        host = "127.0.0.1"
        domain = "proxy.localtest.me"  # resolves to 127.0.0.1
        super().__init__(dsn=auth_backend.default_conn_url, host=domain, port=proxy_port)

        self.domain = domain
        self.host = host
        self.http_port = http_port
        self.external_http_port = external_http_port
        self.neon_binpath = neon_binpath
        self.test_output_dir = test_output_dir
        self.proxy_port = proxy_port
        self.mgmt_port = mgmt_port
        self.auth_backend = auth_backend
        self.metric_collection_endpoint = metric_collection_endpoint
        self.metric_collection_interval = metric_collection_interval
        self.http_timeout_seconds = 15
        self._popen: Optional[subprocess.Popen[bytes]] = None

    def start(self) -> NeonProxy:
        assert self._popen is None

        # generate key of it doesn't exist
        crt_path = self.test_output_dir / "proxy.crt"
        key_path = self.test_output_dir / "proxy.key"

        if not key_path.exists():
            r = subprocess.run(
                [
                    "openssl",
                    "req",
                    "-new",
                    "-x509",
                    "-days",
                    "365",
                    "-nodes",
                    "-text",
                    "-out",
                    str(crt_path),
                    "-keyout",
                    str(key_path),
                    "-subj",
                    "/CN=*.localtest.me",
                    "-addext",
                    "subjectAltName = DNS:*.localtest.me",
                ]
            )
            assert r.returncode == 0

        args = [
            str(self.neon_binpath / "proxy"),
            *["--http", f"{self.host}:{self.http_port}"],
            *["--proxy", f"{self.host}:{self.proxy_port}"],
            *["--mgmt", f"{self.host}:{self.mgmt_port}"],
            *["--wss", f"{self.host}:{self.external_http_port}"],
            *["--sql-over-http-timeout", f"{self.http_timeout_seconds}s"],
            *["-c", str(crt_path)],
            *["-k", str(key_path)],
            *self.auth_backend.extra_args(),
        ]

        if (
            self.metric_collection_endpoint is not None
            and self.metric_collection_interval is not None
        ):
            args += [
                *["--metric-collection-endpoint", self.metric_collection_endpoint],
                *["--metric-collection-interval", self.metric_collection_interval],
            ]

        logfile = open(self.test_output_dir / "proxy.log", "w")
        self._popen = subprocess.Popen(args, stdout=logfile, stderr=logfile)
        self._wait_until_ready()
        return self

    # Sends SIGTERM to the proxy if it has been started
    def terminate(self):
        if self._popen:
            self._popen.terminate()

    # Waits for proxy to exit if it has been opened with a default timeout of
    # two seconds. Raises subprocess.TimeoutExpired if the proxy does not exit in time.
    def wait_for_exit(self, timeout=2):
        if self._popen:
            self._popen.wait(timeout=2)

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=10)
    def _wait_until_ready(self):
        requests.get(f"http://{self.host}:{self.http_port}/v1/status")

    def http_query(self, query, args, **kwargs):
        # TODO maybe use default values if not provided
        user = quote(kwargs["user"])
        password = quote(kwargs["password"])
        expected_code = kwargs.get("expected_code")
        timeout = kwargs.get("timeout")

        log.info(f"Executing http query: {query}")

        connstr = f"postgresql://{user}:{password}@{self.domain}:{self.proxy_port}/postgres"
        response = requests.post(
            f"https://{self.domain}:{self.external_http_port}/sql",
            data=json.dumps({"query": query, "params": args}),
            headers={
                "Content-Type": "application/sql",
                "Neon-Connection-String": connstr,
                "Neon-Pool-Opt-In": "true",
            },
            verify=str(self.test_output_dir / "proxy.crt"),
            timeout=timeout,
        )

        if expected_code is not None:
            assert response.status_code == expected_code, f"response: {response.json()}"
        return response.json()

    async def http2_query(self, query, args, **kwargs):
        # TODO maybe use default values if not provided
        user = kwargs["user"]
        password = kwargs["password"]
        expected_code = kwargs.get("expected_code")

        log.info(f"Executing http2 query: {query}")

        connstr = f"postgresql://{user}:{password}@{self.domain}:{self.proxy_port}/postgres"
        async with httpx.AsyncClient(
            http2=True, verify=str(self.test_output_dir / "proxy.crt")
        ) as client:
            response = await client.post(
                f"https://{self.domain}:{self.external_http_port}/sql",
                json={"query": query, "params": args},
                headers={
                    "Content-Type": "application/sql",
                    "Neon-Connection-String": connstr,
                    "Neon-Pool-Opt-In": "true",
                },
            )
            assert response.http_version == "HTTP/2"

            if expected_code is not None:
                assert response.status_code == expected_code, f"response: {response.json()}"
            return response.json()

    def get_metrics(self) -> str:
        request_result = requests.get(f"http://{self.host}:{self.http_port}/metrics")
        return request_result.text

    @staticmethod
    def get_session_id(uri_prefix, uri_line):
        assert uri_prefix in uri_line

        url_parts = urlparse(uri_line)
        psql_session_id = url_parts.path[1:]
        assert psql_session_id.isalnum(), "session_id should only contain alphanumeric chars"

        return psql_session_id

    @staticmethod
    async def find_auth_link(link_auth_uri, proc):
        for _ in range(100):
            line = (await proc.stderr.readline()).decode("utf-8").strip()
            log.info(f"psql line: {line}")
            if link_auth_uri in line:
                log.info(f"SUCCESS, found auth url: {line}")
                return line

    def __enter__(self) -> NeonProxy:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        if self._popen is not None:
            self._popen.terminate()
            try:
                self._popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log.warning("failed to gracefully terminate proxy; killing")
                self._popen.kill()

    @staticmethod
    async def activate_link_auth(
        local_vanilla_pg, proxy_with_metric_collector, psql_session_id, create_user=True
    ):
        pg_user = "proxy"

        if create_user:
            log.info("creating a new user for link auth test")
            local_vanilla_pg.enable_tls()
            local_vanilla_pg.start()
            local_vanilla_pg.safe_psql(f"create user {pg_user} with login superuser")

        db_info = json.dumps(
            {
                "session_id": psql_session_id,
                "result": {
                    "Success": {
                        "host": local_vanilla_pg.default_options["host"],
                        "port": local_vanilla_pg.default_options["port"],
                        "dbname": local_vanilla_pg.default_options["dbname"],
                        "user": pg_user,
                        "aux": {
                            "project_id": "test_project_id",
                            "endpoint_id": "test_endpoint_id",
                            "branch_id": "test_branch_id",
                        },
                    }
                },
            }
        )

        log.info("sending session activation message")
        psql = await PSQL(
            host=proxy_with_metric_collector.host,
            port=proxy_with_metric_collector.mgmt_port,
        ).run(db_info)
        assert psql.stdout is not None
        out = (await psql.stdout.read()).decode("utf-8").strip()
        assert out == "ok"


@pytest.fixture(scope="function")
def link_proxy(
    port_distributor: PortDistributor, neon_binpath: Path, test_output_dir: Path
) -> Iterator[NeonProxy]:
    """Neon proxy that routes through link auth."""

    http_port = port_distributor.get_port()
    proxy_port = port_distributor.get_port()
    mgmt_port = port_distributor.get_port()
    external_http_port = port_distributor.get_port()

    with NeonProxy(
        neon_binpath=neon_binpath,
        test_output_dir=test_output_dir,
        proxy_port=proxy_port,
        http_port=http_port,
        mgmt_port=mgmt_port,
        external_http_port=external_http_port,
        auth_backend=NeonProxy.Link(),
    ) as proxy:
        proxy.start()
        yield proxy


@pytest.fixture(scope="function")
def static_proxy(
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    neon_binpath: Path,
    test_output_dir: Path,
) -> Iterator[NeonProxy]:
    """Neon proxy that routes directly to vanilla postgres."""

    port = vanilla_pg.default_options["port"]
    host = vanilla_pg.default_options["host"]
    dbname = vanilla_pg.default_options["dbname"]
    auth_endpoint = f"postgres://proxy:password@{host}:{port}/{dbname}"

    # require password for 'http_auth' user
    vanilla_pg.edit_hba([f"host {dbname} http_auth {host} password"])

    # For simplicity, we use the same user for both `--auth-endpoint` and `safe_psql`
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user proxy with login superuser password 'password'")
    vanilla_pg.safe_psql("CREATE SCHEMA IF NOT EXISTS neon_control_plane")
    vanilla_pg.safe_psql(
        "CREATE TABLE neon_control_plane.endpoints (endpoint_id VARCHAR(255) PRIMARY KEY, allowed_ips VARCHAR(255))"
    )

    proxy_port = port_distributor.get_port()
    mgmt_port = port_distributor.get_port()
    http_port = port_distributor.get_port()
    external_http_port = port_distributor.get_port()

    with NeonProxy(
        neon_binpath=neon_binpath,
        test_output_dir=test_output_dir,
        proxy_port=proxy_port,
        http_port=http_port,
        mgmt_port=mgmt_port,
        external_http_port=external_http_port,
        auth_backend=NeonProxy.Postgres(auth_endpoint),
    ) as proxy:
        proxy.start()
        yield proxy


class Endpoint(PgProtocol, LogUtils):
    """An object representing a Postgres compute endpoint managed by the control plane."""

    def __init__(
        self,
        env: NeonEnv,
        tenant_id: TenantId,
        pg_port: int,
        http_port: int,
        check_stop_result: bool = True,
    ):
        super().__init__(host="localhost", port=pg_port, user="cloud_admin", dbname="postgres")
        self.env = env
        self.branch_name: Optional[str] = None  # dubious
        self.endpoint_id: Optional[str] = None  # dubious, see asserts below
        self.pgdata_dir: Optional[Path] = None  # Path to computenode PGDATA
        self.tenant_id = tenant_id
        self.pg_port = pg_port
        self.http_port = http_port
        self.check_stop_result = check_stop_result
        # passed to endpoint create and endpoint reconfigure
        self.active_safekeepers: List[int] = list(map(lambda sk: sk.id, env.safekeepers))
        # path to conf is <repo_dir>/endpoints/<endpoint_id>/pgdata/postgresql.conf

        # Semaphore is set to 1 when we start, and acquire'd back to zero when we stop
        #
        # We use a semaphore rather than a bool so that racing calls to stop() don't
        # try and stop the same process twice, as stop() is called by test teardown and
        # potentially by some __del__ chains in other threads.
        self._running = threading.Semaphore(0)

    def http_client(
        self, auth_token: Optional[str] = None, retries: Optional[Retry] = None
    ) -> EndpointHttpClient:
        return EndpointHttpClient(
            port=self.http_port,
        )

    def create(
        self,
        branch_name: str,
        endpoint_id: Optional[str] = None,
        hot_standby: bool = False,
        lsn: Optional[Lsn] = None,
        config_lines: Optional[List[str]] = None,
        pageserver_id: Optional[int] = None,
        allow_multiple: bool = False,
    ) -> "Endpoint":
        """
        Create a new Postgres endpoint.
        Returns self.
        """

        if not config_lines:
            config_lines = []

        endpoint_id = endpoint_id or self.env.generate_endpoint_id()
        self.endpoint_id = endpoint_id
        self.branch_name = branch_name

        self.env.neon_cli.endpoint_create(
            branch_name,
            endpoint_id=self.endpoint_id,
            tenant_id=self.tenant_id,
            lsn=lsn,
            hot_standby=hot_standby,
            pg_port=self.pg_port,
            http_port=self.http_port,
            pageserver_id=pageserver_id,
            allow_multiple=allow_multiple,
        )
        path = Path("endpoints") / self.endpoint_id / "pgdata"
        self.pgdata_dir = self.env.repo_dir / path
        self.logfile = self.endpoint_path() / "compute.log"

        config_lines = config_lines or []

        # set small 'max_replication_write_lag' to enable backpressure
        # and make tests more stable.
        config_lines = ["max_replication_write_lag=15MB"] + config_lines

        self.config(config_lines)

        return self

    def start(
        self,
        remote_ext_config: Optional[str] = None,
        pageserver_id: Optional[int] = None,
        safekeepers: Optional[List[int]] = None,
        allow_multiple: bool = False,
        basebackup_request_tries: Optional[int] = None,
    ) -> "Endpoint":
        """
        Start the Postgres instance.
        Returns self.
        """

        assert self.endpoint_id is not None

        # If `safekeepers` is not None, they are remember them as active and use
        # in the following commands.
        if safekeepers is not None:
            self.active_safekeepers = safekeepers

        log.info(f"Starting postgres endpoint {self.endpoint_id}")

        self.env.neon_cli.endpoint_start(
            self.endpoint_id,
            safekeepers=self.active_safekeepers,
            remote_ext_config=remote_ext_config,
            pageserver_id=pageserver_id,
            allow_multiple=allow_multiple,
            basebackup_request_tries=basebackup_request_tries,
        )
        self._running.release(1)

        return self

    def endpoint_path(self) -> Path:
        """Path to endpoint directory"""
        assert self.endpoint_id
        path = Path("endpoints") / self.endpoint_id
        return self.env.repo_dir / path

    def pg_data_dir_path(self) -> Path:
        """Path to Postgres data directory"""
        return self.endpoint_path() / "pgdata"

    def pg_xact_dir_path(self) -> Path:
        """Path to pg_xact dir"""
        return self.pg_data_dir_path() / "pg_xact"

    def pg_twophase_dir_path(self) -> Path:
        """Path to pg_twophase dir"""
        return self.pg_data_dir_path() / "pg_twophase"

    def config_file_path(self) -> Path:
        """Path to the postgresql.conf in the endpoint directory (not the one in pgdata)"""
        return self.endpoint_path() / "postgresql.conf"

    def config(self, lines: List[str]) -> "Endpoint":
        """
        Add lines to postgresql.conf.
        Lines should be an array of valid postgresql.conf rows.
        Returns self.
        """

        with open(self.config_file_path(), "a") as conf:
            for line in lines:
                conf.write(line)
                conf.write("\n")

        return self

    def edit_hba(self, hba: List[str]):
        """Prepend hba lines into pg_hba.conf file."""
        with open(os.path.join(self.pg_data_dir_path(), "pg_hba.conf"), "r+") as conf_file:
            data = conf_file.read()
            conf_file.seek(0)
            conf_file.write("\n".join(hba) + "\n")
            conf_file.write(data)

        if self.is_running():
            self.safe_psql("SELECT pg_reload_conf()")

    def is_running(self):
        return self._running._value > 0

    def reconfigure(
        self, pageserver_id: Optional[int] = None, safekeepers: Optional[List[int]] = None
    ):
        assert self.endpoint_id is not None
        # If `safekeepers` is not None, they are remember them as active and use
        # in the following commands.
        if safekeepers is not None:
            self.active_safekeepers = safekeepers
        self.env.neon_cli.endpoint_reconfigure(
            self.endpoint_id, self.tenant_id, pageserver_id, self.active_safekeepers
        )

    def respec(self, **kwargs: Any) -> None:
        """Update the endpoint.json file used by control_plane."""
        # Read config
        config_path = os.path.join(self.endpoint_path(), "endpoint.json")
        with open(config_path, "r") as f:
            data_dict: dict[str, Any] = json.load(f)

        # Write it back updated
        with open(config_path, "w") as file:
            log.info(json.dumps(dict(data_dict, **kwargs)))
            json.dump(dict(data_dict, **kwargs), file, indent=4)

    # Please note: Migrations only run if pg_skip_catalog_updates is false
    def wait_for_migrations(self, num_migrations: int = 11):
        with self.cursor() as cur:

            def check_migrations_done():
                cur.execute("SELECT id FROM neon_migration.migration_id")
                migration_id: int = cur.fetchall()[0][0]
                assert migration_id >= num_migrations

            wait_until(20, 0.5, check_migrations_done)

    # Mock the extension part of spec passed from control plane for local testing
    # endpooint.rs adds content of this file as a part of the spec.json
    def create_remote_extension_spec(self, spec: dict[str, Any]):
        """Create a remote extension spec file for the endpoint."""
        remote_extensions_spec_path = os.path.join(
            self.endpoint_path(), "remote_extensions_spec.json"
        )

        with open(remote_extensions_spec_path, "w") as file:
            json.dump(spec, file, indent=4)

    def stop(self, mode: str = "fast") -> "Endpoint":
        """
        Stop the Postgres instance if it's running.

        Because test teardown might try and stop an endpoint concurrently with test code
        stopping the endpoint, this method is thread safe

        Returns self.
        """

        running = self._running.acquire(blocking=False)
        if running:
            assert self.endpoint_id is not None
            self.env.neon_cli.endpoint_stop(
                self.endpoint_id, check_return_code=self.check_stop_result, mode=mode
            )

        return self

    def stop_and_destroy(self, mode: str = "immediate") -> "Endpoint":
        """
        Stop the Postgres instance, then destroy the endpoint.
        Returns self.
        """

        running = self._running.acquire(blocking=False)
        if running:
            assert self.endpoint_id is not None
            self.env.neon_cli.endpoint_stop(
                self.endpoint_id, True, check_return_code=self.check_stop_result, mode=mode
            )
            self.endpoint_id = None

        return self

    def create_start(
        self,
        branch_name: str,
        endpoint_id: Optional[str] = None,
        hot_standby: bool = False,
        lsn: Optional[Lsn] = None,
        config_lines: Optional[List[str]] = None,
        remote_ext_config: Optional[str] = None,
        pageserver_id: Optional[int] = None,
        allow_multiple=False,
        basebackup_request_tries: Optional[int] = None,
    ) -> "Endpoint":
        """
        Create an endpoint, apply config, and start Postgres.
        Returns self.
        """

        started_at = time.time()

        self.create(
            branch_name=branch_name,
            endpoint_id=endpoint_id,
            config_lines=config_lines,
            hot_standby=hot_standby,
            lsn=lsn,
            pageserver_id=pageserver_id,
            allow_multiple=allow_multiple,
        ).start(
            remote_ext_config=remote_ext_config,
            pageserver_id=pageserver_id,
            allow_multiple=allow_multiple,
            basebackup_request_tries=basebackup_request_tries,
        )

        log.info(f"Postgres startup took {time.time() - started_at} seconds")

        return self

    def __enter__(self) -> "Endpoint":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        self.stop()

    # Checkpoints running endpoint and returns pg_wal size in MB.
    def get_pg_wal_size(self):
        log.info(f'checkpointing at LSN {self.safe_psql("select pg_current_wal_lsn()")[0][0]}')
        self.safe_psql("checkpoint")
        assert self.pgdata_dir is not None  # please mypy
        return get_dir_size(self.pgdata_dir / "pg_wal") / 1024 / 1024

    def clear_shared_buffers(self, cursor: Optional[Any] = None):
        """
        Best-effort way to clear postgres buffers. Pinned buffers will not be 'cleared.'

        Might also clear LFC.
        """
        if cursor is not None:
            cursor.execute("select clear_buffer_cache()")
        else:
            self.safe_psql("select clear_buffer_cache()")


class EndpointFactory:
    """An object representing multiple compute endpoints."""

    def __init__(self, env: NeonEnv):
        self.env = env
        self.num_instances: int = 0
        self.endpoints: List[Endpoint] = []

    def create_start(
        self,
        branch_name: str,
        endpoint_id: Optional[str] = None,
        tenant_id: Optional[TenantId] = None,
        lsn: Optional[Lsn] = None,
        hot_standby: bool = False,
        config_lines: Optional[List[str]] = None,
        remote_ext_config: Optional[str] = None,
        pageserver_id: Optional[int] = None,
        basebackup_request_tries: Optional[int] = None,
    ) -> Endpoint:
        ep = Endpoint(
            self.env,
            tenant_id=tenant_id or self.env.initial_tenant,
            pg_port=self.env.port_distributor.get_port(),
            http_port=self.env.port_distributor.get_port(),
        )
        self.num_instances += 1
        self.endpoints.append(ep)

        return ep.create_start(
            branch_name=branch_name,
            endpoint_id=endpoint_id,
            hot_standby=hot_standby,
            config_lines=config_lines,
            lsn=lsn,
            remote_ext_config=remote_ext_config,
            pageserver_id=pageserver_id,
            basebackup_request_tries=basebackup_request_tries,
        )

    def create(
        self,
        branch_name: str,
        endpoint_id: Optional[str] = None,
        tenant_id: Optional[TenantId] = None,
        lsn: Optional[Lsn] = None,
        hot_standby: bool = False,
        config_lines: Optional[List[str]] = None,
        pageserver_id: Optional[int] = None,
    ) -> Endpoint:
        ep = Endpoint(
            self.env,
            tenant_id=tenant_id or self.env.initial_tenant,
            pg_port=self.env.port_distributor.get_port(),
            http_port=self.env.port_distributor.get_port(),
        )

        endpoint_id = endpoint_id or self.env.generate_endpoint_id()

        self.num_instances += 1
        self.endpoints.append(ep)

        return ep.create(
            branch_name=branch_name,
            endpoint_id=endpoint_id,
            lsn=lsn,
            hot_standby=hot_standby,
            config_lines=config_lines,
            pageserver_id=pageserver_id,
        )

    def stop_all(self, fail_on_error=True) -> "EndpointFactory":
        exception = None
        for ep in self.endpoints:
            try:
                ep.stop()
            except Exception as e:
                log.error(f"Failed to stop endpoint {ep.endpoint_id}: {e}")
                exception = e

        if fail_on_error and exception is not None:
            raise exception

        return self

    def new_replica(
        self, origin: Endpoint, endpoint_id: str, config_lines: Optional[List[str]] = None
    ):
        branch_name = origin.branch_name
        assert origin in self.endpoints
        assert branch_name is not None

        return self.create(
            branch_name=branch_name,
            endpoint_id=endpoint_id,
            tenant_id=origin.tenant_id,
            lsn=None,
            hot_standby=True,
            config_lines=config_lines,
        )

    def new_replica_start(
        self, origin: Endpoint, endpoint_id: str, config_lines: Optional[List[str]] = None
    ):
        branch_name = origin.branch_name
        assert origin in self.endpoints
        assert branch_name is not None

        return self.create_start(
            branch_name=branch_name,
            endpoint_id=endpoint_id,
            tenant_id=origin.tenant_id,
            lsn=None,
            hot_standby=True,
            config_lines=config_lines,
        )


@dataclass
class SafekeeperPort:
    pg: int
    pg_tenant_only: int
    http: int


@dataclass
class Safekeeper(LogUtils):
    """An object representing a running safekeeper daemon."""

    env: NeonEnv
    port: SafekeeperPort
    id: int
    running: bool = False

    def __init__(
        self,
        env: NeonEnv,
        port: SafekeeperPort,
        id: int,
        running: bool = False,
        extra_opts: Optional[List[str]] = None,
    ):
        self.env = env
        self.port = port
        self.id = id
        self.running = running
        self.logfile = Path(self.data_dir) / f"safekeeper-{id}.log"

        if extra_opts is None:
            # Testing defaults: enable everything, and set short timeouts so that background
            # work will happen during short tests.
            # **Note**: Any test that explicitly sets extra_opts will not get these defaults.
            extra_opts = [
                "--enable-offload",
                "--delete-offloaded-wal",
                "--partial-backup-timeout",
                "10s",
                "--control-file-save-interval",
                "1s",
                "--eviction-min-resident",
                "10s",
            ]

        self.extra_opts = extra_opts

    def start(
        self, extra_opts: Optional[List[str]] = None, timeout_in_seconds: Optional[int] = None
    ) -> "Safekeeper":
        if extra_opts is None:
            # Apply either the extra_opts passed in, or the ones from our constructor: we do not merge the two.
            extra_opts = self.extra_opts

        assert self.running is False
        self.env.neon_cli.safekeeper_start(
            self.id, extra_opts=extra_opts, timeout_in_seconds=timeout_in_seconds
        )
        self.running = True
        # wait for wal acceptor start by checking its status
        started_at = time.time()
        while True:
            try:
                with self.http_client() as http_cli:
                    http_cli.check_status()
            except Exception as e:
                elapsed = time.time() - started_at
                if elapsed > 3:
                    raise RuntimeError(
                        f"timed out waiting {elapsed:.0f}s for wal acceptor start: {e}"
                    ) from e
                time.sleep(0.5)
            else:
                break  # success
        return self

    def stop(self, immediate: bool = False) -> "Safekeeper":
        log.info(f"Stopping safekeeper {self.id}")
        self.env.neon_cli.safekeeper_stop(self.id, immediate)
        self.running = False
        return self

    def assert_no_errors(self):
        assert not self.log_contains("manager task finished prematurely")
        assert not self.log_contains("error while acquiring WalResidentTimeline guard")
        assert not self.log_contains("timeout while acquiring WalResidentTimeline guard")

    def append_logical_message(
        self, tenant_id: TenantId, timeline_id: TimelineId, request: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Send JSON_CTRL query to append LogicalMessage to WAL and modify
        safekeeper state. It will construct LogicalMessage from provided
        prefix and message, and then will write it to WAL.
        """

        # "replication=0" hacks psycopg not to send additional queries
        # on startup, see https://github.com/psycopg/psycopg2/pull/482
        token = self.env.auth_keys.generate_tenant_token(tenant_id)
        connstr = f"host=localhost port={self.port.pg} password={token} replication=0 options='-c timeline_id={timeline_id} tenant_id={tenant_id}'"

        with closing(psycopg2.connect(connstr)) as conn:
            # server doesn't support transactions
            conn.autocommit = True
            with conn.cursor() as cur:
                request_json = json.dumps(request)
                log.info(f"JSON_CTRL request on port {self.port.pg}: {request_json}")
                cur.execute("JSON_CTRL " + request_json)
                all = cur.fetchall()
                log.info(f"JSON_CTRL response: {all[0][0]}")
                res = json.loads(all[0][0])
                assert isinstance(res, dict)
                return res

    def http_client(
        self, auth_token: Optional[str] = None, gen_sk_wide_token: bool = True
    ) -> SafekeeperHttpClient:
        """
        When auth_token is None but gen_sk_wide is True creates safekeeper wide
        token, which is a reasonable default.
        """
        if auth_token is None and gen_sk_wide_token:
            auth_token = self.env.auth_keys.generate_safekeeper_token()
        is_testing_enabled = '"testing"' in self.env.get_binary_version("safekeeper")
        return SafekeeperHttpClient(
            port=self.port.http, auth_token=auth_token, is_testing_enabled=is_testing_enabled
        )

    def get_timeline_start_lsn(self, tenant_id: TenantId, timeline_id: TimelineId) -> Lsn:
        timeline_status = self.http_client().timeline_status(tenant_id, timeline_id)
        timeline_start_lsn = timeline_status.timeline_start_lsn
        log.info(f"sk {self.id} timeline start LSN: {timeline_start_lsn}")
        return timeline_start_lsn

    def get_flush_lsn(self, tenant_id: TenantId, timeline_id: TimelineId) -> Lsn:
        timeline_status = self.http_client().timeline_status(tenant_id, timeline_id)
        flush_lsn = timeline_status.flush_lsn
        log.info(f"sk {self.id} flush LSN: {flush_lsn}")
        return flush_lsn

    def pull_timeline(
        self, srcs: list[Safekeeper], tenant_id: TenantId, timeline_id: TimelineId
    ) -> Dict[str, Any]:
        """
        pull_timeline from srcs to self.
        """
        src_https = [f"http://localhost:{sk.port.http}" for sk in srcs]
        res = self.http_client().pull_timeline(
            {"tenant_id": str(tenant_id), "timeline_id": str(timeline_id), "http_hosts": src_https}
        )
        src_ids = [sk.id for sk in srcs]
        log.info(f"finished pulling timeline from {src_ids} to {self.id}")
        return res

    @property
    def data_dir(self) -> Path:
        return self.env.repo_dir / "safekeepers" / f"sk{self.id}"

    def timeline_dir(self, tenant_id, timeline_id) -> Path:
        return self.data_dir / str(tenant_id) / str(timeline_id)

    # List partial uploaded segments of this safekeeper. Works only for
    # RemoteStorageKind.LOCAL_FS.
    def list_uploaded_segments(self, tenant_id: TenantId, timeline_id: TimelineId):
        tline_path = (
            self.env.repo_dir
            / "local_fs_remote_storage"
            / "safekeeper"
            / str(tenant_id)
            / str(timeline_id)
        )
        assert isinstance(self.env.safekeepers_remote_storage, LocalFsStorage)
        segs = self._list_segments_in_dir(
            tline_path, lambda name: ".metadata" not in name and ".___temp" not in name
        )
        mysegs = [s for s in segs if f"sk{self.id}" in s]
        return mysegs

    def list_segments(self, tenant_id, timeline_id) -> List[str]:
        """
        Get list of segment names of the given timeline.
        """
        tli_dir = self.timeline_dir(tenant_id, timeline_id)
        return self._list_segments_in_dir(
            tli_dir, lambda name: not name.startswith("safekeeper.control")
        )

    def _list_segments_in_dir(self, path: Path, keep_filter: Callable[[str], bool]) -> list[str]:
        segments = []
        for _, _, filenames in os.walk(path):
            segments.extend([f for f in filenames if keep_filter(f)])
        segments.sort()
        return segments

    def checkpoint_up_to(
        self, tenant_id: TenantId, timeline_id: TimelineId, lsn: Lsn, wait_wal_removal=True
    ):
        """
        Assuming pageserver(s) uploaded to s3 up to `lsn`,
        1) wait for remote_consistent_lsn and wal_backup_lsn on safekeeper to reach it.
        2) checkpoint timeline on safekeeper, which should remove WAL before this LSN; optionally wait for that.
        """
        cli = self.http_client()

        target_segment_file = lsn.segment_name()

        def are_segments_removed():
            segments = self.list_segments(tenant_id, timeline_id)
            log.info(
                f"waiting for all segments before {target_segment_file} to be removed from sk {self.id}, current segments: {segments}"
            )
            assert all(target_segment_file <= s for s in segments)

        def are_lsns_advanced():
            stat = cli.timeline_status(tenant_id, timeline_id)
            log.info(
                f"waiting for remote_consistent_lsn and backup_lsn on sk {self.id} to reach {lsn}, currently remote_consistent_lsn={stat.remote_consistent_lsn}, backup_lsn={stat.backup_lsn}"
            )
            assert stat.remote_consistent_lsn >= lsn and stat.backup_lsn >= lsn.segment_lsn()

        # xxx: max wait is long because we might be waiting for reconnection from
        # pageserver to this safekeeper
        wait_until(30, 1, are_lsns_advanced)
        cli.checkpoint(tenant_id, timeline_id)
        if wait_wal_removal:
            wait_until(30, 1, are_segments_removed)

    def wait_until_paused(self, failpoint: str):
        msg = f"at failpoint {failpoint}"

        def paused():
            log.info(f"waiting for hitting failpoint {failpoint}")
            self.assert_log_contains(msg)

        wait_until(20, 0.5, paused)


# TODO: Replace with `StrEnum` when we upgrade to python 3.11
class NodeKind(str, Enum):
    PAGESERVER = "pageserver"
    SAFEKEEPER = "safekeeper"


class StorageScrubber:
    def __init__(self, env: NeonEnv, log_dir: Path):
        self.env = env
        self.log_dir = log_dir

    def scrubber_cli(
        self, args: list[str], timeout, extra_env: Optional[Dict[str, str]] = None
    ) -> str:
        assert isinstance(self.env.pageserver_remote_storage, S3Storage)
        s3_storage = self.env.pageserver_remote_storage

        env = {
            "REGION": s3_storage.bucket_region,
            "BUCKET": s3_storage.bucket_name,
            "BUCKET_PREFIX": s3_storage.prefix_in_bucket,
            "RUST_LOG": "DEBUG",
        }
        env.update(s3_storage.access_env_vars())

        if s3_storage.endpoint is not None:
            env.update({"AWS_ENDPOINT_URL": s3_storage.endpoint})

        if extra_env is not None:
            env.update(extra_env)

        base_args = [
            str(self.env.neon_binpath / "storage_scrubber"),
            f"--controller-api={self.env.storage_controller.api_root()}",
        ]
        args = base_args + args

        log.info(f"Invoking scrubber command {args} with env: {env}")
        (output_path, stdout, status_code) = subprocess_capture(
            self.log_dir,
            args,
            echo_stderr=True,
            echo_stdout=True,
            env=env,
            check=False,
            capture_stdout=True,
            timeout=timeout,
        )
        if status_code:
            log.warning(f"Scrub command {args} failed")
            log.warning(f"Scrub environment: {env}")
            log.warning(f"Output at: {output_path}")

            raise RuntimeError(f"Scrubber failed while running {args}")

        assert stdout is not None
        return stdout

    def scan_metadata_safekeeper(
        self,
        timeline_lsns: List[Dict[str, Any]],
        cloud_admin_api_url: str,
        cloud_admin_api_token: str,
    ) -> Tuple[bool, Any]:
        extra_env = {
            "CLOUD_ADMIN_API_URL": cloud_admin_api_url,
            "CLOUD_ADMIN_API_TOKEN": cloud_admin_api_token,
        }
        return self.scan_metadata(
            node_kind=NodeKind.SAFEKEEPER, timeline_lsns=timeline_lsns, extra_env=extra_env
        )

    def scan_metadata(
        self,
        post_to_storage_controller: bool = False,
        node_kind: NodeKind = NodeKind.PAGESERVER,
        timeline_lsns: Optional[List[Dict[str, Any]]] = None,
        extra_env: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, Any]:
        """
        Returns the health status and the metadata summary.
        """
        args = ["scan-metadata", "--node-kind", node_kind.value, "--json"]
        if post_to_storage_controller:
            args.append("--post")
        if timeline_lsns is not None:
            args.append("--timeline-lsns")
            args.append(json.dumps(timeline_lsns))
        stdout = self.scrubber_cli(args, timeout=30, extra_env=extra_env)

        try:
            summary = json.loads(stdout)
            # summary does not contain "with_warnings" if node_kind is the safekeeper
            no_warnings = "with_warnings" not in summary or not summary["with_warnings"]
            healthy = not summary["with_errors"] and no_warnings
            return healthy, summary
        except:
            log.error("Failed to decode JSON output from `scan-metadata`.  Dumping stdout:")
            log.error(stdout)
            raise

    def tenant_snapshot(self, tenant_id: TenantId, output_path: Path):
        stdout = self.scrubber_cli(
            ["tenant-snapshot", "--tenant-id", str(tenant_id), "--output-path", str(output_path)],
            timeout=30,
        )
        log.info(f"tenant-snapshot output: {stdout}")

    def pageserver_physical_gc(
        self,
        min_age_secs: int,
        tenant_ids: Optional[list[TenantId]] = None,
        mode: Optional[str] = None,
    ):
        args = ["pageserver-physical-gc", "--min-age", f"{min_age_secs}s"]

        if tenant_ids is None:
            tenant_ids = []

        for tenant_id in tenant_ids:
            args.extend(["--tenant-id", str(tenant_id)])

        if mode is not None:
            args.extend(["--mode", mode])

        stdout = self.scrubber_cli(
            args,
            timeout=30,
        )
        try:
            return json.loads(stdout)
        except:
            log.error(
                "Failed to decode JSON output from `pageserver-physical_gc`.  Dumping stdout:"
            )
            log.error(stdout)
            raise


def _get_test_dir(request: FixtureRequest, top_output_dir: Path, prefix: str) -> Path:
    """Compute the path to a working directory for an individual test."""
    test_name = request.node.name
    test_dir = top_output_dir / f"{prefix}{test_name.replace('/', '-')}"

    # We rerun flaky tests multiple times, use a separate directory for each run.
    if (suffix := getattr(request.node, "execution_count", None)) is not None:
        test_dir = test_dir.parent / f"{test_dir.name}-{suffix}"

    log.info(f"get_test_output_dir is {test_dir}")
    # make mypy happy
    assert isinstance(test_dir, Path)
    return test_dir


def get_test_output_dir(request: FixtureRequest, top_output_dir: Path) -> Path:
    """
    The working directory for a test.
    """
    return _get_test_dir(request, top_output_dir, "")


def get_test_overlay_dir(request: FixtureRequest, top_output_dir: Path) -> Path:
    """
    Directory that contains `upperdir` and `workdir` for overlayfs mounts
    that a test creates. See `NeonEnvBuilder.overlay_mount`.
    """
    return _get_test_dir(request, top_output_dir, "overlay-")


def get_shared_snapshot_dir_path(top_output_dir: Path, snapshot_name: str) -> Path:
    return top_output_dir / "shared-snapshots" / snapshot_name


def get_test_repo_dir(request: FixtureRequest, top_output_dir: Path) -> Path:
    return get_test_output_dir(request, top_output_dir) / "repo"


def pytest_addoption(parser: Parser):
    parser.addoption(
        "--preserve-database-files",
        action="store_true",
        default=False,
        help="Preserve timeline files after the test suite is over",
    )


SMALL_DB_FILE_NAME_REGEX: re.Pattern = re.compile(  # type: ignore[type-arg]
    r"config-v1|heatmap-v1|metadata|.+\.(?:toml|pid|json|sql|conf)"
)


# This is autouse, so the test output directory always gets created, even
# if a test doesn't put anything there.
#
# NB: we request the overlay dir fixture so the fixture does its cleanups
@pytest.fixture(scope="function", autouse=True)
def test_output_dir(
    request: FixtureRequest, top_output_dir: Path, test_overlay_dir: Path
) -> Iterator[Path]:
    """Create the working directory for an individual test."""

    # one directory per test
    test_dir = get_test_output_dir(request, top_output_dir)
    log.info(f"test_output_dir is {test_dir}")
    shutil.rmtree(test_dir, ignore_errors=True)
    test_dir.mkdir()

    yield test_dir

    # Allure artifacts creation might involve the creation of `.tar.zst` archives,
    # which aren't going to be used if Allure results collection is not enabled
    # (i.e. --alluredir is not set).
    # Skip `allure_attach_from_dir` in this case
    if not request.config.getoption("--alluredir"):
        return

    preserve_database_files = False
    for k, v in request.node.user_properties:
        # NB: the neon_env_builder fixture uses this fixture (test_output_dir).
        # So, neon_env_builder's cleanup runs before here.
        # The cleanup propagates NeonEnvBuilder.preserve_database_files into this user property.
        if k == "preserve_database_files":
            assert isinstance(v, bool)
            preserve_database_files = v

    allure_attach_from_dir(test_dir, preserve_database_files)


class FileAndThreadLock:
    def __init__(self, path: Path):
        self.path = path
        self.thread_lock = threading.Lock()
        self.fd: Optional[int] = None

    def __enter__(self):
        self.fd = os.open(self.path, os.O_CREAT | os.O_WRONLY)
        # lock thread lock before file lock so that there's no race
        # around flocking / funlocking the file lock
        self.thread_lock.acquire()
        flock(self.fd, LOCK_EX)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        assert self.fd is not None
        assert self.thread_lock.locked()  # ... by us
        flock(self.fd, LOCK_UN)
        self.thread_lock.release()
        os.close(self.fd)
        self.fd = None


class SnapshotDirLocked:
    def __init__(self, parent: SnapshotDir):
        self._parent = parent

    def is_initialized(self):
        # TODO: in the future, take a `tag` as argument and store it in the marker in set_initialized.
        # Then, in this function, compare marker file contents with the tag to invalidate the snapshot if the tag changed.
        return self._parent._marker_file_path.exists()

    def set_initialized(self):
        self._parent._marker_file_path.write_text("")

    @property
    def path(self) -> Path:
        return self._parent._path / "snapshot"


class SnapshotDir:
    _path: Path

    def __init__(self, path: Path):
        self._path = path
        assert self._path.is_dir()
        self._lock = FileAndThreadLock(self._lock_file_path)

    @property
    def _lock_file_path(self) -> Path:
        return self._path / "initializing.flock"

    @property
    def _marker_file_path(self) -> Path:
        return self._path / "initialized.marker"

    def __enter__(self) -> SnapshotDirLocked:
        self._lock.__enter__()
        return SnapshotDirLocked(self)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._lock.__exit__(exc_type, exc_value, exc_traceback)


def shared_snapshot_dir(top_output_dir, ident: str) -> SnapshotDir:
    snapshot_dir_path = get_shared_snapshot_dir_path(top_output_dir, ident)
    snapshot_dir_path.mkdir(exist_ok=True, parents=True)
    return SnapshotDir(snapshot_dir_path)


@pytest.fixture(scope="function")
def test_overlay_dir(request: FixtureRequest, top_output_dir: Path) -> Optional[Path]:
    """
    Idempotently create a test's overlayfs mount state directory.
    If the functionality isn't enabled via env var, returns None.

    The procedure cleans up after previous runs that were aborted (e.g. due to Ctrl-C, OOM kills, etc).
    """

    if os.getenv("NEON_ENV_BUILDER_USE_OVERLAYFS_FOR_SNAPSHOTS") is None:
        return None

    overlay_dir = get_test_overlay_dir(request, top_output_dir)
    log.info(f"test_overlay_dir is {overlay_dir}")

    overlay_dir.mkdir(exist_ok=True)
    # unmount stale overlayfs mounts which subdirectories of `overlay_dir/*` as the overlayfs `upperdir` and `workdir`
    for mountpoint in overlayfs.iter_mounts_beneath(get_test_output_dir(request, top_output_dir)):
        cmd = ["sudo", "umount", str(mountpoint)]
        log.info(
            f"Unmounting stale overlayfs mount probably created during earlier test run: {cmd}"
        )
        subprocess.run(cmd, capture_output=True, check=True)
    # the overlayfs `workdir`` is owned by `root`, shutil.rmtree won't work.
    cmd = ["sudo", "rm", "-rf", str(overlay_dir)]
    subprocess.run(cmd, capture_output=True, check=True)

    overlay_dir.mkdir()

    return overlay_dir

    # no need to clean up anything: on clean shutdown,
    # NeonEnvBuilder.overlay_cleanup_teardown takes care of cleanup
    # and on unclean shutdown, this function will take care of it
    # on the next test run


SKIP_DIRS = frozenset(
    (
        "pg_wal",
        "pg_stat",
        "pg_stat_tmp",
        "pg_subtrans",
        "pg_logical",
        "pg_replslot/wal_proposer_slot",
    )
)

SKIP_FILES = frozenset(
    (
        "pg_internal.init",
        "pg.log",
        "zenith.signal",
        "pg_hba.conf",
        "postgresql.conf",
        "postmaster.opts",
        "postmaster.pid",
        "pg_control",
    )
)


def should_skip_dir(dirname: str) -> bool:
    return dirname in SKIP_DIRS


def should_skip_file(filename: str) -> bool:
    if filename in SKIP_FILES:
        return True
    # check for temp table files according to https://www.postgresql.org/docs/current/storage-file-layout.html
    # i e "tBBB_FFF"
    if not filename.startswith("t"):
        return False

    tmp_name = filename[1:].split("_")
    if len(tmp_name) != 2:
        return False

    try:
        list(map(int, tmp_name))
    except:  # noqa: E722
        return False
    return True


#
# Test helpers
#
def list_files_to_compare(pgdata_dir: Path) -> List[str]:
    pgdata_files = []
    for root, _dirs, filenames in os.walk(pgdata_dir):
        for filename in filenames:
            rel_dir = os.path.relpath(root, pgdata_dir)
            # Skip some dirs and files we don't want to compare
            if should_skip_dir(rel_dir) or should_skip_file(filename):
                continue
            rel_file = os.path.join(rel_dir, filename)
            pgdata_files.append(rel_file)

    pgdata_files.sort()
    log.info(pgdata_files)
    return pgdata_files


# pg is the existing and running compute node, that we want to compare with a basebackup
def check_restored_datadir_content(
    test_output_dir: Path,
    env: NeonEnv,
    endpoint: Endpoint,
    ignored_files: Optional[list[str]] = None,
):
    pg_bin = PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version)

    # Get the timeline ID. We need it for the 'basebackup' command
    timeline_id = TimelineId(endpoint.safe_psql("SHOW neon.timeline_id")[0][0])

    # stop postgres to ensure that files won't change
    endpoint.stop()

    # Read the shutdown checkpoint's LSN
    checkpoint_lsn = pg_bin.get_pg_controldata_checkpoint_lsn(endpoint.pg_data_dir_path())

    # Take a basebackup from pageserver
    restored_dir_path = env.repo_dir / f"{endpoint.endpoint_id}_restored_datadir"
    restored_dir_path.mkdir(exist_ok=True)

    psql_path = os.path.join(pg_bin.pg_bin_path, "psql")

    pageserver_id = env.storage_controller.locate(endpoint.tenant_id)[0]["node_id"]
    cmd = rf"""
        {psql_path}                                    \
            --no-psqlrc                                \
            postgres://localhost:{env.get_pageserver(pageserver_id).service_port.pg}  \
            -c 'basebackup {endpoint.tenant_id} {timeline_id} {checkpoint_lsn}'  \
         | tar -x -C {restored_dir_path}
    """

    # Set LD_LIBRARY_PATH in the env properly, otherwise we may use the wrong libpq.
    # PgBin sets it automatically, but here we need to pipe psql output to the tar command.
    psql_env = {"LD_LIBRARY_PATH": pg_bin.pg_lib_dir}
    result = subprocess.run(cmd, env=psql_env, capture_output=True, text=True, shell=True)

    # Print captured stdout/stderr if basebackup cmd failed.
    if result.returncode != 0:
        log.error("Basebackup shell command failed with:")
        log.error(result.stdout)
        log.error(result.stderr)
    assert result.returncode == 0

    # list files we're going to compare
    assert endpoint.pgdata_dir
    pgdata_files = list_files_to_compare(Path(endpoint.pgdata_dir))

    restored_files = list_files_to_compare(restored_dir_path)

    if pgdata_files != restored_files:
        # filter pg_xact and multixact files which are downloaded on demand
        pgdata_files = [
            f
            for f in pgdata_files
            if not f.startswith("pg_xact") and not f.startswith("pg_multixact")
        ]

    if ignored_files:
        pgdata_files = [f for f in pgdata_files if f not in ignored_files]
        restored_files = [f for f in restored_files if f not in ignored_files]

    # check that file sets are equal
    assert pgdata_files == restored_files

    # compare content of the files
    # filecmp returns (match, mismatch, error) lists
    # We've already filtered all mismatching files in list_files_to_compare(),
    # so here expect that the content is identical
    (match, mismatch, error) = filecmp.cmpfiles(
        endpoint.pgdata_dir, restored_dir_path, pgdata_files, shallow=False
    )
    log.info(f"filecmp result mismatch and error lists:\n\t mismatch={mismatch}\n\t error={error}")

    for f in mismatch:
        f1 = os.path.join(endpoint.pgdata_dir, f)
        f2 = os.path.join(restored_dir_path, f)
        stdout_filename = f"{f2}.filediff"

        with open(stdout_filename, "w") as stdout_f:
            subprocess.run(f"xxd -b {f1} > {f1}.hex ", shell=True)
            subprocess.run(f"xxd -b {f2} > {f2}.hex ", shell=True)

            cmd = f"diff {f1}.hex {f2}.hex"
            subprocess.run([cmd], stdout=stdout_f, shell=True)

    assert (mismatch, error) == ([], [])


def logical_replication_sync(subscriber: PgProtocol, publisher: PgProtocol) -> Lsn:
    """Wait logical replication subscriber to sync with publisher."""
    publisher_lsn = Lsn(publisher.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
    while True:
        res = subscriber.safe_psql("select latest_end_lsn from pg_catalog.pg_stat_subscription")[0][
            0
        ]
        if res:
            log.info(f"subscriber_lsn={res}")
            subscriber_lsn = Lsn(res)
            log.info(f"Subscriber LSN={subscriber_lsn}, publisher LSN={ publisher_lsn}")
            if subscriber_lsn >= publisher_lsn:
                return subscriber_lsn
        time.sleep(0.5)


def tenant_get_shards(
    env: NeonEnv, tenant_id: TenantId, pageserver_id: Optional[int] = None
) -> list[tuple[TenantShardId, NeonPageserver]]:
    """
    Helper for when you want to talk to one or more pageservers, and the
    caller _might_ have specified a pageserver, or they might leave it to
    us to figure out the shards for a tenant.

    If the caller provides `pageserver_id`, it will be used for all shards, even
    if the shard is indicated by storage controller to be on some other pageserver.

    Caller should over the response to apply their per-pageserver action to
    each shard
    """
    if pageserver_id is not None:
        override_pageserver = [p for p in env.pageservers if p.id == pageserver_id][0]
    else:
        override_pageserver = None

    if len(env.pageservers) > 1:
        return [
            (
                TenantShardId.parse(s["shard_id"]),
                override_pageserver or env.get_pageserver(s["node_id"]),
            )
            for s in env.storage_controller.locate(tenant_id)
        ]
    else:
        # Assume an unsharded tenant
        return [(TenantShardId(tenant_id, 0, 0), override_pageserver or env.pageserver)]


def wait_replica_caughtup(primary: Endpoint, secondary: Endpoint):
    primary_lsn = Lsn(
        primary.safe_psql_scalar("SELECT pg_current_wal_flush_lsn()", log_query=False)
    )
    while True:
        secondary_lsn = Lsn(
            secondary.safe_psql_scalar("SELECT pg_last_wal_replay_lsn()", log_query=False)
        )
        caught_up = secondary_lsn >= primary_lsn
        log.info(f"caughtup={caught_up}, primary_lsn={primary_lsn}, secondary_lsn={secondary_lsn}")
        if caught_up:
            return
        time.sleep(1)


def log_replica_lag(primary: Endpoint, secondary: Endpoint):
    last_replay_lsn = Lsn(
        secondary.safe_psql_scalar("SELECT pg_last_wal_replay_lsn()", log_query=False)
    )
    primary_lsn = Lsn(
        primary.safe_psql_scalar("SELECT pg_current_wal_flush_lsn()", log_query=False)
    )
    lag = primary_lsn - last_replay_lsn
    log.info(f"primary_lsn={primary_lsn}, replay_lsn={last_replay_lsn}, lag={lag}")


def wait_for_last_flush_lsn(
    env: NeonEnv,
    endpoint: Endpoint,
    tenant: TenantId,
    timeline: TimelineId,
    pageserver_id: Optional[int] = None,
    auth_token: Optional[str] = None,
) -> Lsn:
    """Wait for pageserver to catch up the latest flush LSN, returns the last observed lsn."""

    shards = tenant_get_shards(env, tenant, pageserver_id)

    last_flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])

    results = []
    for tenant_shard_id, pageserver in shards:
        log.info(
            f"wait_for_last_flush_lsn: waiting for {last_flush_lsn} on shard {tenant_shard_id} on pageserver {pageserver.id})"
        )
        waited = wait_for_last_record_lsn(
            pageserver.http_client(auth_token=auth_token), tenant_shard_id, timeline, last_flush_lsn
        )

        assert waited >= last_flush_lsn
        results.append(waited)

    # Return the lowest LSN that has been ingested by all shards
    return min(results)


def flush_ep_to_pageserver(
    env: NeonEnv,
    ep: Endpoint,
    tenant: TenantId,
    timeline: TimelineId,
    pageserver_id: Optional[int] = None,
) -> Lsn:
    """
    Stop endpoint and wait until all committed WAL reaches the pageserver
    (last_record_lsn). This is for use by tests which want everything written so
    far to reach pageserver *and* expecting that no more data will arrive until
    endpoint starts again, so unlike wait_for_last_flush_lsn it polls
    safekeepers instead of compute to learn LSN.

    Returns the catch up LSN.
    """
    ep.stop()

    commit_lsn: Lsn = Lsn(0)
    # In principle in the absense of failures polling single sk would be enough.
    for sk in env.safekeepers:
        cli = sk.http_client()
        # wait until compute connections are gone
        wait_until(30, 0.5, partial(are_walreceivers_absent, cli, tenant, timeline))
        commit_lsn = max(cli.get_commit_lsn(tenant, timeline), commit_lsn)

    # Note: depending on WAL filtering implementation, probably most shards
    # won't be able to reach commit_lsn (unless gaps are also ack'ed), so this
    # is broken in sharded case.
    shards = tenant_get_shards(env, tenant, pageserver_id)
    for tenant_shard_id, pageserver in shards:
        log.info(
            f"flush_ep_to_pageserver: waiting for {commit_lsn} on shard {tenant_shard_id} on pageserver {pageserver.id})"
        )
        waited = wait_for_last_record_lsn(
            pageserver.http_client(), tenant_shard_id, timeline, commit_lsn
        )

        assert waited >= commit_lsn

    return commit_lsn


def wait_for_wal_insert_lsn(
    env: NeonEnv,
    endpoint: Endpoint,
    tenant: TenantId,
    timeline: TimelineId,
    pageserver_id: Optional[int] = None,
) -> Lsn:
    """Wait for pageserver to catch up the latest flush LSN, returns the last observed lsn."""
    last_flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_insert_lsn()")[0][0])
    result = None
    for tenant_shard_id, pageserver in tenant_get_shards(env, tenant, pageserver_id):
        shard_r = wait_for_last_record_lsn(
            pageserver.http_client(), tenant_shard_id, timeline, last_flush_lsn
        )
        if result is None:
            result = shard_r

    assert result is not None
    return result


def fork_at_current_lsn(
    env: NeonEnv,
    endpoint: Endpoint,
    new_branch_name: str,
    ancestor_branch_name: str,
    tenant_id: Optional[TenantId] = None,
) -> TimelineId:
    """
    Create new branch at the last LSN of an existing branch.
    The "last LSN" is taken from the given Postgres instance. The pageserver will wait for all the
    the WAL up to that LSN to arrive in the pageserver before creating the branch.
    """
    current_lsn = endpoint.safe_psql("SELECT pg_current_wal_lsn()")[0][0]
    return env.neon_cli.create_branch(new_branch_name, ancestor_branch_name, tenant_id, current_lsn)


def import_timeline_from_vanilla_postgres(
    test_output_dir: Path,
    env: NeonEnv,
    pg_bin: PgBin,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    branch_name: str,
    vanilla_pg_connstr: str,
):
    """
    Create a new timeline, by importing an existing PostgreSQL cluster.

    This works by taking a physical backup of the running PostgreSQL cluster, and importing that.
    """

    # Take backup of the existing PostgreSQL server with pg_basebackup
    basebackup_dir = os.path.join(test_output_dir, "basebackup")
    base_tar = os.path.join(basebackup_dir, "base.tar")
    wal_tar = os.path.join(basebackup_dir, "pg_wal.tar")
    os.mkdir(basebackup_dir)
    pg_bin.run(
        [
            "pg_basebackup",
            "-F",
            "tar",
            "-d",
            vanilla_pg_connstr,
            "-D",
            basebackup_dir,
        ]
    )

    # Extract start_lsn and end_lsn form the backup manifest file
    with open(os.path.join(basebackup_dir, "backup_manifest")) as f:
        manifest = json.load(f)
        start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
        end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]

    # Import the backup tarballs into the pageserver
    env.neon_cli.raw_cli(
        [
            "timeline",
            "import",
            "--tenant-id",
            str(tenant_id),
            "--timeline-id",
            str(timeline_id),
            "--branch-name",
            branch_name,
            "--base-lsn",
            start_lsn,
            "--base-tarfile",
            base_tar,
            "--end-lsn",
            end_lsn,
            "--wal-tarfile",
            wal_tar,
            "--pg-version",
            env.pg_version,
        ]
    )
    wait_for_last_record_lsn(env.pageserver.http_client(), tenant_id, timeline_id, Lsn(end_lsn))


def last_flush_lsn_upload(
    env: NeonEnv,
    endpoint: Endpoint,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    pageserver_id: Optional[int] = None,
    auth_token: Optional[str] = None,
) -> Lsn:
    """
    Wait for pageserver to catch to the latest flush LSN of given endpoint,
    checkpoint pageserver, and wait for it to be uploaded (remote_consistent_lsn
    reaching flush LSN).
    """
    last_flush_lsn = wait_for_last_flush_lsn(
        env, endpoint, tenant_id, timeline_id, pageserver_id=pageserver_id, auth_token=auth_token
    )
    shards = tenant_get_shards(env, tenant_id, pageserver_id)
    for tenant_shard_id, pageserver in shards:
        ps_http = pageserver.http_client(auth_token=auth_token)
        wait_for_last_record_lsn(ps_http, tenant_shard_id, timeline_id, last_flush_lsn)
        ps_http.timeline_checkpoint(tenant_shard_id, timeline_id, wait_until_uploaded=True)
    return last_flush_lsn


def parse_project_git_version_output(s: str) -> str:
    """
    Parses the git commit hash out of the --version output supported at least by neon_local.

    The information is generated by utils::project_git_version!
    """
    res = re.search(r"git(-env)?:([0-9a-fA-F]{8,40})(-\S+)?", s)
    if res and (commit := res.group(2)):
        return commit

    raise ValueError(f"unable to parse --version output: '{s}'")


def generate_uploads_and_deletions(
    env: NeonEnv,
    *,
    init: bool = True,
    tenant_id: Optional[TenantId] = None,
    timeline_id: Optional[TimelineId] = None,
    data: Optional[str] = None,
    pageserver: NeonPageserver,
):
    """
    Using the environment's default tenant + timeline, generate a load pattern
    that results in some uploads and some deletions to remote storage.
    """

    if tenant_id is None:
        tenant_id = env.initial_tenant
    assert tenant_id is not None

    if timeline_id is None:
        timeline_id = env.initial_timeline
    assert timeline_id is not None

    ps_http = pageserver.http_client()

    with env.endpoints.create_start(
        "main", tenant_id=tenant_id, pageserver_id=pageserver.id
    ) as endpoint:
        if init:
            endpoint.safe_psql("CREATE TABLE foo (id INTEGER PRIMARY KEY, val text)")
            last_flush_lsn_upload(
                env, endpoint, tenant_id, timeline_id, pageserver_id=pageserver.id
            )

        def churn(data):
            endpoint.safe_psql_many(
                [
                    f"""
                INSERT INTO foo (id, val)
                SELECT g, '{data}'
                FROM generate_series(1, 200) g
                ON CONFLICT (id) DO UPDATE
                SET val = EXCLUDED.val
                """,
                    # to ensure that GC can actually remove some layers
                    "VACUUM foo",
                ]
            )
            assert tenant_id is not None
            assert timeline_id is not None
            # We are waiting for uploads as well as local flush, in order to avoid leaving the system
            # in a state where there are "future layers" in remote storage that will generate deletions
            # after a restart.
            last_flush_lsn_upload(
                env, endpoint, tenant_id, timeline_id, pageserver_id=pageserver.id
            )

        # Compaction should generate some GC-elegible layers
        for i in range(0, 2):
            churn(f"{i if data is None else data}")

        gc_result = ps_http.timeline_gc(tenant_id, timeline_id, 0)
        print_gc_result(gc_result)
        assert gc_result["layers_removed"] > 0

        # Stop endpoint and flush all data to pageserver, then checkpoint it: this
        # ensures that the pageserver is in a fully idle state: there will be no more
        # background ingest, no more uploads pending, and therefore no non-determinism
        # in subsequent actions like pageserver restarts.
        flush_ep_to_pageserver(env, endpoint, tenant_id, timeline_id, pageserver.id)
        ps_http.timeline_checkpoint(tenant_id, timeline_id, wait_until_uploaded=True)
