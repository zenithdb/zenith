from __future__ import annotations

from dataclasses import field
import textwrap
from cached_property import cached_property
import asyncpg
import os
import boto3
import pathlib
import uuid
import warnings
import jwt
import json
import psycopg2
import pytest
import re
import shutil
import socket
import subprocess
import time
import filecmp
import tempfile

from contextlib import closing
from pathlib import Path
from dataclasses import dataclass

# Type-related stuff
from psycopg2.extensions import connection as PgConnection
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, TypeVar, cast, Union, Tuple
from typing_extensions import Literal

import requests
import backoff  # type: ignore

from .utils import (etcd_path, get_self_dir, mkdir_if_needed, subprocess_capture, lsn_from_hex)
from fixtures.log_helper import log
"""
This file contains pytest fixtures. A fixture is a test resource that can be
summoned by placing its name in the test's arguments.

A fixture is created with the decorator @zenfixture, which is a wrapper around
the standard pytest.fixture with some extra behavior.

There are several environment variables that can control the running of tests:
ZENITH_BIN, POSTGRES_DISTRIB_DIR, etc. See README.md for more information.

There's no need to import this file to use it. It should be declared as a plugin
inside conftest.py, and that makes it available to all tests.

Don't import functions from this file, or pytest will emit warnings. Instead
put directly-importable functions into utils.py or another separate file.
"""

Env = Dict[str, str]
Fn = TypeVar('Fn', bound=Callable[..., Any])

DEFAULT_OUTPUT_DIR = 'test_output'
DEFAULT_POSTGRES_DIR = 'tmp_install'
DEFAULT_BRANCH_NAME = 'main'

BASE_PORT = 15000
WORKER_PORT_NUM = 100


def pytest_addoption(parser):
    parser.addoption(
        "--skip-interfering-proc-check",
        dest="skip_interfering_proc_check",
        action="store_true",
        help="skip check for interferring processes",
    )


# These are set in pytest_configure()
base_dir = ""
zenith_binpath = ""
pg_distrib_dir = ""
top_output_dir = ""


def check_interferring_processes(config):
    if config.getoption("skip_interfering_proc_check"):
        warnings.warn("interferring process check is skipped")
        return

    # does not use -c as it is not supported on macOS
    cmd = ['pgrep', 'pageserver|postgres|safekeeper']
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL)
    if result.returncode == 0:
        # returncode of 0 means it found something.
        # This is bad; we don't want any of those processes polluting the
        # result of the test.
        # NOTE this shows as an internal pytest error, there might be a better way
        raise Exception(
            'Found interfering processes running. Stop all Zenith pageservers, nodes, safekeepers, as well as stand-alone Postgres.'
        )


def pytest_configure(config):
    """
    Ensure that no unwanted daemons are running before we start testing.
    Check that we do not owerflow available ports range.
    """
    check_interferring_processes(config)

    numprocesses = config.getoption('numprocesses')
    if numprocesses is not None and BASE_PORT + numprocesses * WORKER_PORT_NUM > 32768:  # do not use ephemeral ports
        raise Exception('Too many workers configured. Cannot distribute ports for services.')

    # find the base directory (currently this is the git root)
    global base_dir
    base_dir = os.path.normpath(os.path.join(get_self_dir(), '../..'))
    log.info(f'base_dir is {base_dir}')

    # Compute the top-level directory for all tests.
    global top_output_dir
    env_test_output = os.environ.get('TEST_OUTPUT')
    if env_test_output is not None:
        top_output_dir = env_test_output
    else:
        top_output_dir = os.path.join(base_dir, DEFAULT_OUTPUT_DIR)
    mkdir_if_needed(top_output_dir)

    if os.getenv("REMOTE_ENV"):
        # we are in remote env and do not have zenith binaries locally
        # this is the case for benchmarks run on self-hosted runner
        return
    # Find the zenith binaries.
    global zenith_binpath
    env_zenith_bin = os.environ.get('ZENITH_BIN')
    if env_zenith_bin:
        zenith_binpath = env_zenith_bin
    else:
        zenith_binpath = os.path.join(base_dir, 'target/debug')
    log.info(f'zenith_binpath is {zenith_binpath}')
    if not os.path.exists(os.path.join(zenith_binpath, 'pageserver')):
        raise Exception('zenith binaries not found at "{}"'.format(zenith_binpath))

    # Find the postgres installation.
    global pg_distrib_dir
    env_postgres_bin = os.environ.get('POSTGRES_DISTRIB_DIR')
    if env_postgres_bin:
        pg_distrib_dir = env_postgres_bin
    else:
        pg_distrib_dir = os.path.normpath(os.path.join(base_dir, DEFAULT_POSTGRES_DIR))
    log.info(f'pg_distrib_dir is {pg_distrib_dir}')
    if not os.path.exists(os.path.join(pg_distrib_dir, 'bin/postgres')):
        raise Exception('postgres not found at "{}"'.format(pg_distrib_dir))


def zenfixture(func: Fn) -> Fn:
    """
    This is a python decorator for fixtures with a flexible scope.

    By default every test function will set up and tear down a new
    database. In pytest, this is called fixtures "function" scope.

    If the environment variable TEST_SHARED_FIXTURES is set, then all
    tests will share the same database. State, logs, etc. will be
    stored in a directory called "shared".
    """

    scope: Literal['session', 'function'] = \
        'function' if os.environ.get('TEST_SHARED_FIXTURES') is None else 'session'

    return pytest.fixture(func, scope=scope)


@zenfixture
def worker_seq_no(worker_id: str):
    # worker_id is a pytest-xdist fixture
    # it can be master or gw<number>
    # parse it to always get a number
    if worker_id == 'master':
        return 0
    assert worker_id.startswith('gw')
    return int(worker_id[2:])


@zenfixture
def worker_base_port(worker_seq_no: int):
    # so we divide ports in ranges of 100 ports
    # so workers have disjoint set of ports for services
    return BASE_PORT + worker_seq_no * WORKER_PORT_NUM


def get_dir_size(path: str) -> int:
    """Return size in bytes."""
    totalbytes = 0
    for root, dirs, files in os.walk(path):
        for name in files:
            totalbytes += os.path.getsize(os.path.join(root, name))

    return totalbytes


def can_bind(host: str, port: int) -> bool:
    """
    Check whether a host:port is available to bind for listening

    Inspired by the can_bind() perl function used in Postgres tests, in
    vendor/postgres/src/test/perl/PostgresNode.pm
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        # TODO: The pageserver and safekeepers don't use SO_REUSEADDR at the
        # moment. If that changes, we should use start using SO_REUSEADDR here
        # too, to allow reusing ports more quickly.
        # See https://github.com/zenithdb/zenith/issues/801
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            sock.bind((host, port))
            sock.listen()
            return True
        except socket.error:
            log.info(f"Port {port} is in use, skipping")
            return False


class PortDistributor:
    def __init__(self, base_port: int, port_number: int):
        self.iterator = iter(range(base_port, base_port + port_number))

    def get_port(self) -> int:
        for port in self.iterator:
            if can_bind("localhost", port):
                return port
        else:
            raise RuntimeError(
                'port range configured for test is exhausted, consider enlarging the range')


@zenfixture
def port_distributor(worker_base_port):
    return PortDistributor(base_port=worker_base_port, port_number=WORKER_PORT_NUM)


class PgProtocol:
    """ Reusable connection logic """
    def __init__(self,
                 host: str,
                 port: int,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 dbname: Optional[str] = None,
                 schema: Optional[str] = None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.dbname = dbname
        self.schema = schema

    def connstr(self,
                *,
                dbname: Optional[str] = None,
                schema: Optional[str] = None,
                username: Optional[str] = None,
                password: Optional[str] = None,
                statement_timeout_ms: Optional[int] = None) -> str:
        """
        Build a libpq connection string for the Postgres instance.
        """

        username = username or self.username
        password = password or self.password
        dbname = dbname or self.dbname or "postgres"
        schema = schema or self.schema
        res = f'host={self.host} port={self.port} dbname={dbname}'

        if username:
            res = f'{res} user={username}'

        if password:
            res = f'{res} password={password}'

        if schema:
            res = f"{res} options='-c search_path={schema}'"

        if statement_timeout_ms:
            res = f"{res} options='-c statement_timeout={statement_timeout_ms}'"

        return res

    # autocommit=True here by default because that's what we need most of the time
    def connect(
        self,
        *,
        autocommit=True,
        dbname: Optional[str] = None,
        schema: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        # individual statement timeout in seconds, 2 minutes should be enough for our tests
        statement_timeout: Optional[int] = 120
    ) -> PgConnection:
        """
        Connect to the node.
        Returns psycopg2's connection object.
        This method passes all extra params to connstr.
        """

        conn = psycopg2.connect(
            self.connstr(dbname=dbname,
                         schema=schema,
                         username=username,
                         password=password,
                         statement_timeout_ms=statement_timeout *
                         1000 if statement_timeout else None))
        # WARNING: this setting affects *all* tests!
        conn.autocommit = autocommit
        return conn

    async def connect_async(self,
                            *,
                            dbname: str = 'postgres',
                            username: Optional[str] = None,
                            password: Optional[str] = None) -> asyncpg.Connection:
        """
        Connect to the node from async python.
        Returns asyncpg's connection object.
        """

        conn = await asyncpg.connect(
            host=self.host,
            port=self.port,
            database=dbname,
            user=username or self.username,
            password=password,
        )
        return conn

    def safe_psql(self, query: str, **kwargs: Any) -> List[Any]:
        """
        Execute query against the node and return all rows.
        This method passes all extra params to connstr.
        """

        with closing(self.connect(**kwargs)) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                if cur.description is None:
                    return []  # query didn't return data
                return cast(List[Any], cur.fetchall())


@dataclass
class AuthKeys:
    pub: bytes
    priv: bytes

    def generate_management_token(self):
        token = jwt.encode({"scope": "pageserverapi"}, self.priv, algorithm="RS256")

        # jwt.encode can return 'bytes' or 'str', depending on Python version or type
        # hinting or something (not sure what). If it returned 'bytes', convert it to 'str'
        # explicitly.
        if isinstance(token, bytes):
            token = token.decode()

        return token

    def generate_tenant_token(self, tenant_id):
        token = jwt.encode({
            "scope": "tenant", "tenant_id": tenant_id
        },
                           self.priv,
                           algorithm="RS256")

        if isinstance(token, bytes):
            token = token.decode()

        return token


class MockS3Server:
    """
    Starts a mock S3 server for testing on a port given, errors if the server fails to start or exits prematurely.
    Relies that `poetry` and `moto` server are installed, since it's the way the tests are run.

    Also provides a set of methods to derive the connection properties from and the method to kill the underlying server.
    """
    def __init__(
        self,
        port: int,
    ):
        self.port = port

        self.subprocess = subprocess.Popen([f'poetry run moto_server s3 -p{port}'], shell=True)
        error = None
        try:
            return_code = self.subprocess.poll()
            if return_code is not None:
                error = f"expected mock s3 server to run but it exited with code {return_code}. stdout: '{self.subprocess.stdout}', stderr: '{self.subprocess.stderr}'"
        except Exception as e:
            error = f"expected mock s3 server to start but it failed with exception: {e}. stdout: '{self.subprocess.stdout}', stderr: '{self.subprocess.stderr}'"
        if error is not None:
            log.error(error)
            self.subprocess.kill()
            raise RuntimeError("failed to start s3 mock server")

    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def region(self) -> str:
        return 'us-east-1'

    def access_key(self) -> str:
        return 'test'

    def secret_key(self) -> str:
        return 'test'

    def kill(self):
        self.subprocess.kill()


class ZenithEnvBuilder:
    """
    Builder object to create a Zenith runtime environment

    You should use the `zenith_env_builder` or `zenith_simple_env` pytest
    fixture to create the ZenithEnv object. That way, the repository is
    created in the right directory, based on the test name, and it's properly
    cleaned up after the test has finished.
    """
    def __init__(self,
                 repo_dir: Path,
                 port_distributor: PortDistributor,
                 pageserver_remote_storage: Optional[RemoteStorage] = None,
                 pageserver_config_override: Optional[str] = None,
                 num_safekeepers: int = 0,
                 pageserver_auth_enabled: bool = False,
                 rust_log_override: Optional[str] = None,
                 default_branch_name=DEFAULT_BRANCH_NAME,
                 broker: bool = False):
        self.repo_dir = repo_dir
        self.rust_log_override = rust_log_override
        self.port_distributor = port_distributor
        self.pageserver_remote_storage = pageserver_remote_storage
        self.pageserver_config_override = pageserver_config_override
        self.num_safekeepers = num_safekeepers
        self.pageserver_auth_enabled = pageserver_auth_enabled
        self.default_branch_name = default_branch_name
        self.broker = broker
        self.env: Optional[ZenithEnv] = None

        self.s3_mock_server: Optional[MockS3Server] = None

        if os.getenv('FORCE_MOCK_S3') is not None:
            bucket_name = f'{repo_dir.name}_bucket'
            log.warning(f'Unconditionally initializing mock S3 server for bucket {bucket_name}')
            self.enable_s3_mock_remote_storage(bucket_name)

    def init(self) -> ZenithEnv:
        # Cannot create more than one environment from one builder
        assert self.env is None, "environment already initialized"
        self.env = ZenithEnv(self)
        return self.env

    def start(self):
        self.env.start()

    def init_start(self) -> ZenithEnv:
        env = self.init()
        self.start()
        return env

    """
    Sets up the pageserver to use the local fs at the `test_dir/local_fs_remote_storage` path.
    Errors, if the pageserver has some remote storage configuration already, unless `force_enable` is not set to `True`.
    """

    def enable_local_fs_remote_storage(self, force_enable=True):
        assert force_enable or self.pageserver_remote_storage is None, "remote storage is enabled already"
        self.pageserver_remote_storage = LocalFsStorage(
            Path(self.repo_dir / 'local_fs_remote_storage'))

    """
    Sets up the pageserver to use the S3 mock server, creates the bucket, if it's not present already.
    Starts up the mock server, if that does not run yet.
    Errors, if the pageserver has some remote storage configuration already, unless `force_enable` is not set to `True`.
    """

    def enable_s3_mock_remote_storage(self, bucket_name: str, force_enable=True):
        assert force_enable or self.pageserver_remote_storage is None, "remote storage is enabled already"
        if not self.s3_mock_server:
            self.s3_mock_server = MockS3Server(self.port_distributor.get_port())

        mock_endpoint = self.s3_mock_server.endpoint()
        mock_region = self.s3_mock_server.region()
        mock_access_key = self.s3_mock_server.access_key()
        mock_secret_key = self.s3_mock_server.secret_key()
        boto3.client(
            's3',
            endpoint_url=mock_endpoint,
            region_name=mock_region,
            aws_access_key_id=mock_access_key,
            aws_secret_access_key=mock_secret_key,
        ).create_bucket(Bucket=bucket_name)
        self.pageserver_remote_storage = S3Storage(bucket=bucket_name,
                                                   endpoint=mock_endpoint,
                                                   region=mock_region,
                                                   access_key=mock_access_key,
                                                   secret_key=mock_secret_key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):

        # Stop all the nodes.
        if self.env:
            log.info('Cleaning up all storage and compute nodes')
            self.env.postgres.stop_all()
            for sk in self.env.safekeepers:
                sk.stop(immediate=True)
            self.env.pageserver.stop(immediate=True)
            if self.s3_mock_server:
                self.s3_mock_server.kill()
            if self.env.broker is not None:
                self.env.broker.stop()


class ZenithEnv:
    """
    An object representing the Zenith runtime environment. It consists of
    the page server, 0-N safekeepers, and the compute nodes.

    ZenithEnv contains functions for stopping/starting nodes in the
    environment, checking their status, creating tenants, connecting to the
    nodes, creating and destroying compute nodes, etc. The page server and
    the safekeepers are considered fixed in the environment, you cannot
    create or destroy them after the environment is initialized. (That will
    likely change in the future, as we start supporting multiple page
    servers and adding/removing safekeepers on the fly).

    Some notable functions and fields in ZenithEnv:

    postgres - A factory object for creating postgres compute nodes.

    pageserver - An object that contains functions for manipulating and
        connecting to the pageserver

    safekeepers - An array containing objects representing the safekeepers

    pg_bin - pg_bin.run() can be used to execute Postgres client binaries,
        like psql or pg_dump

    initial_tenant - tenant ID of the initial tenant created in the repository

    zenith_cli - can be used to run the 'zenith' CLI tool

    create_tenant() - initializes a new tenant in the page server, returns
        the tenant id
    """
    def __init__(self, config: ZenithEnvBuilder):
        self.repo_dir = config.repo_dir
        self.rust_log_override = config.rust_log_override
        self.port_distributor = config.port_distributor
        self.s3_mock_server = config.s3_mock_server
        self.zenith_cli = ZenithCli(env=self)
        self.postgres = PostgresFactory(self)
        self.safekeepers: List[Safekeeper] = []

        # generate initial tenant ID here instead of letting 'zenith init' generate it,
        # so that we don't need to dig it out of the config file afterwards.
        self.initial_tenant = uuid.uuid4()

        # Create a config file corresponding to the options
        toml = textwrap.dedent(f"""
            default_tenant_id = '{self.initial_tenant.hex}'
        """)

        self.broker = None
        if config.broker:
            # keep etcd datadir inside 'repo'
            self.broker = Etcd(datadir=os.path.join(self.repo_dir, "etcd"),
                               port=self.port_distributor.get_port(),
                               peer_port=self.port_distributor.get_port())
            toml += textwrap.dedent(f"""
            broker_endpoints = 'http://127.0.0.1:{self.broker.port}'
        """)

        # Create config for pageserver
        pageserver_port = PageserverPort(
            pg=self.port_distributor.get_port(),
            http=self.port_distributor.get_port(),
        )
        pageserver_auth_type = "ZenithJWT" if config.pageserver_auth_enabled else "Trust"

        toml += textwrap.dedent(f"""
            [pageserver]
            id=1
            listen_pg_addr = 'localhost:{pageserver_port.pg}'
            listen_http_addr = 'localhost:{pageserver_port.http}'
            auth_type = '{pageserver_auth_type}'
        """)

        # Create a corresponding ZenithPageserver object
        self.pageserver = ZenithPageserver(self,
                                           port=pageserver_port,
                                           remote_storage=config.pageserver_remote_storage,
                                           config_override=config.pageserver_config_override)

        # Create config and a Safekeeper object for each safekeeper
        for i in range(1, config.num_safekeepers + 1):
            port = SafekeeperPort(
                pg=self.port_distributor.get_port(),
                http=self.port_distributor.get_port(),
            )
            id = i  # assign ids sequentially
            toml += textwrap.dedent(f"""
                [[safekeepers]]
                id = {id}
                pg_port = {port.pg}
                http_port = {port.http}
                sync = false # Disable fsyncs to make the tests go faster
            """)
            safekeeper = Safekeeper(env=self, id=id, port=port)
            self.safekeepers.append(safekeeper)

        log.info(f"Config: {toml}")
        self.zenith_cli.init(toml)

    def start(self):
        # Start up the page server, all the safekeepers and the broker
        self.pageserver.start()

        for safekeeper in self.safekeepers:
            safekeeper.start()

        if self.broker is not None:
            self.broker.start()

    def get_safekeeper_connstrs(self) -> str:
        """ Get list of safekeeper endpoints suitable for wal_acceptors GUC  """
        return ','.join([f'localhost:{wa.port.pg}' for wa in self.safekeepers])

    @cached_property
    def auth_keys(self) -> AuthKeys:
        pub = (Path(self.repo_dir) / 'auth_public_key.pem').read_bytes()
        priv = (Path(self.repo_dir) / 'auth_private_key.pem').read_bytes()
        return AuthKeys(pub=pub, priv=priv)


@zenfixture
def _shared_simple_env(request: Any, port_distributor) -> Iterator[ZenithEnv]:
    """
    Internal fixture backing the `zenith_simple_env` fixture. If TEST_SHARED_FIXTURES
    is set, this is shared by all tests using `zenith_simple_env`.
    """

    if os.environ.get('TEST_SHARED_FIXTURES') is None:
        # Create the environment in the per-test output directory
        repo_dir = os.path.join(get_test_output_dir(request), "repo")
    else:
        # We're running shared fixtures. Share a single directory.
        repo_dir = os.path.join(str(top_output_dir), "shared_repo")
        shutil.rmtree(repo_dir, ignore_errors=True)

    with ZenithEnvBuilder(Path(repo_dir), port_distributor) as builder:
        env = builder.init_start()

        # For convenience in tests, create a branch from the freshly-initialized cluster.
        env.zenith_cli.create_branch('empty', ancestor_branch_name=DEFAULT_BRANCH_NAME)

        yield env


@pytest.fixture(scope='function')
def zenith_simple_env(_shared_simple_env: ZenithEnv) -> Iterator[ZenithEnv]:
    """
    Simple Zenith environment, with no authentication and no safekeepers.

    If TEST_SHARED_FIXTURES environment variable is set, we reuse the same
    environment for all tests that use 'zenith_simple_env', keeping the
    page server and safekeepers running. Any compute nodes are stopped after
    each the test, however.
    """
    yield _shared_simple_env

    _shared_simple_env.postgres.stop_all()
    if _shared_simple_env.s3_mock_server:
        _shared_simple_env.s3_mock_server.kill()


@pytest.fixture(scope='function')
def zenith_env_builder(test_output_dir, port_distributor) -> Iterator[ZenithEnvBuilder]:
    """
    Fixture to create a Zenith environment for test.

    To use, define 'zenith_env_builder' fixture in your test to get access to the
    builder object. Set properties on it to describe the environment.
    Finally, initialize and start up the environment by calling
    zenith_env_builder.init_start().

    After the initialization, you can launch compute nodes by calling
    the functions in the 'env.postgres' factory object, stop/start the
    nodes, etc.
    """

    # Create the environment in the test-specific output dir
    repo_dir = os.path.join(test_output_dir, "repo")

    # Return the builder to the caller
    with ZenithEnvBuilder(Path(repo_dir), port_distributor) as builder:
        yield builder


class ZenithPageserverApiException(Exception):
    pass


class ZenithPageserverHttpClient(requests.Session):
    def __init__(self, port: int, auth_token: Optional[str] = None):
        super().__init__()
        self.port = port
        self.auth_token = auth_token

        if auth_token is not None:
            self.headers['Authorization'] = f'Bearer {auth_token}'

    def verbose_error(self, res: requests.Response):
        try:
            res.raise_for_status()
        except requests.RequestException as e:
            try:
                msg = res.json()['msg']
            except:
                msg = ''
            raise ZenithPageserverApiException(msg) from e

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def timeline_attach(self, tenant_id: uuid.UUID, timeline_id: uuid.UUID):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id.hex}/timeline/{timeline_id.hex}/attach",
        )
        self.verbose_error(res)

    def timeline_detach(self, tenant_id: uuid.UUID, timeline_id: uuid.UUID):
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id.hex}/timeline/{timeline_id.hex}/detach",
        )
        self.verbose_error(res)

    def timeline_create(
        self,
        tenant_id: uuid.UUID,
        new_timeline_id: Optional[uuid.UUID] = None,
        ancestor_timeline_id: Optional[uuid.UUID] = None,
        ancestor_start_lsn: Optional[str] = None,
    ) -> Dict[Any, Any]:
        res = self.post(f"http://localhost:{self.port}/v1/tenant/{tenant_id.hex}/timeline",
                        json={
                            'new_timeline_id':
                            new_timeline_id.hex if new_timeline_id else None,
                            'ancestor_start_lsn':
                            ancestor_start_lsn,
                            'ancestor_timeline_id':
                            ancestor_timeline_id.hex if ancestor_timeline_id else None,
                        })
        self.verbose_error(res)
        if res.status_code == 409:
            raise Exception(f'could not create timeline: already exists for id {new_timeline_id}')

        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def tenant_list(self) -> List[Dict[Any, Any]]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def tenant_create(self, new_tenant_id: Optional[uuid.UUID] = None) -> uuid.UUID:
        res = self.post(
            f"http://localhost:{self.port}/v1/tenant",
            json={
                'new_tenant_id': new_tenant_id.hex if new_tenant_id else None,
            },
        )
        self.verbose_error(res)
        if res.status_code == 409:
            raise Exception(f'could not create tenant: already exists for id {new_tenant_id}')
        new_tenant_id = res.json()
        assert isinstance(new_tenant_id, str)
        return uuid.UUID(new_tenant_id)

    def timeline_list(self, tenant_id: uuid.UUID) -> List[Dict[Any, Any]]:
        res = self.get(f"http://localhost:{self.port}/v1/tenant/{tenant_id.hex}/timeline")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def timeline_detail(self, tenant_id: uuid.UUID, timeline_id: uuid.UUID) -> Dict[Any, Any]:
        res = self.get(
            f"http://localhost:{self.port}/v1/tenant/{tenant_id.hex}/timeline/{timeline_id.hex}?include-non-incremental-logical-size=1"
        )
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, dict)
        return res_json

    def get_metrics(self) -> str:
        res = self.get(f"http://localhost:{self.port}/metrics")
        self.verbose_error(res)
        return res.text


@dataclass
class PageserverPort:
    pg: int
    http: int


@dataclass
class LocalFsStorage:
    root: Path


@dataclass
class S3Storage:
    bucket: str
    region: str
    access_key: Optional[str]
    secret_key: Optional[str]
    endpoint: Optional[str]


RemoteStorage = Union[LocalFsStorage, S3Storage]

CREATE_TIMELINE_ID_EXTRACTOR = re.compile(r"^Created timeline '(?P<timeline_id>[^']+)'",
                                          re.MULTILINE)
CREATE_TIMELINE_ID_EXTRACTOR = re.compile(r"^Created timeline '(?P<timeline_id>[^']+)'",
                                          re.MULTILINE)
TIMELINE_DATA_EXTRACTOR = re.compile(r"\s(?P<branch_name>[^\s]+)\s\[(?P<timeline_id>[^\]]+)\]",
                                     re.MULTILINE)


class ZenithCli:
    """
    A typed wrapper around the `zenith` CLI tool.
    Supports main commands via typed methods and a way to run arbitrary command directly via CLI.
    """
    def __init__(self, env: ZenithEnv):
        self.env = env
        pass

    def create_tenant(self, tenant_id: Optional[uuid.UUID] = None) -> uuid.UUID:
        """
        Creates a new tenant, returns its id and its initial timeline's id.
        """
        if tenant_id is None:
            tenant_id = uuid.uuid4()
        res = self.raw_cli(['tenant', 'create', '--tenant-id', tenant_id.hex])
        res.check_returncode()
        return tenant_id

    def list_tenants(self) -> 'subprocess.CompletedProcess[str]':
        res = self.raw_cli(['tenant', 'list'])
        res.check_returncode()
        return res

    def create_timeline(self,
                        new_branch_name: str,
                        tenant_id: Optional[uuid.UUID] = None) -> uuid.UUID:
        cmd = [
            'timeline',
            'create',
            '--branch-name',
            new_branch_name,
            '--tenant-id',
            (tenant_id or self.env.initial_tenant).hex,
        ]

        res = self.raw_cli(cmd)
        res.check_returncode()

        matches = CREATE_TIMELINE_ID_EXTRACTOR.search(res.stdout)

        created_timeline_id = None
        if matches is not None:
            created_timeline_id = matches.group('timeline_id')

        return uuid.UUID(created_timeline_id)

    def create_root_branch(self, branch_name: str, tenant_id: Optional[uuid.UUID] = None):
        cmd = [
            'timeline',
            'create',
            '--branch-name',
            branch_name,
            '--tenant-id',
            (tenant_id or self.env.initial_tenant).hex,
        ]

        res = self.raw_cli(cmd)
        res.check_returncode()

        matches = CREATE_TIMELINE_ID_EXTRACTOR.search(res.stdout)

        created_timeline_id = None
        if matches is not None:
            created_timeline_id = matches.group('timeline_id')

        if created_timeline_id is None:
            raise Exception('could not find timeline id after `zenith timeline create` invocation')
        else:
            return uuid.UUID(created_timeline_id)

    def create_branch(self,
                      new_branch_name: str = DEFAULT_BRANCH_NAME,
                      ancestor_branch_name: Optional[str] = None,
                      tenant_id: Optional[uuid.UUID] = None,
                      ancestor_start_lsn: Optional[str] = None) -> uuid.UUID:
        cmd = [
            'timeline',
            'branch',
            '--branch-name',
            new_branch_name,
            '--tenant-id',
            (tenant_id or self.env.initial_tenant).hex,
        ]
        if ancestor_branch_name is not None:
            cmd.extend(['--ancestor-branch-name', ancestor_branch_name])
        if ancestor_start_lsn is not None:
            cmd.extend(['--ancestor-start-lsn', ancestor_start_lsn])

        res = self.raw_cli(cmd)
        res.check_returncode()

        matches = CREATE_TIMELINE_ID_EXTRACTOR.search(res.stdout)

        created_timeline_id = None
        if matches is not None:
            created_timeline_id = matches.group('timeline_id')

        if created_timeline_id is None:
            raise Exception('could not find timeline id after `zenith timeline create` invocation')
        else:
            return uuid.UUID(created_timeline_id)

    def list_timelines(self, tenant_id: Optional[uuid.UUID] = None) -> List[Tuple[str, str]]:
        """
        Returns a list of (branch_name, timeline_id) tuples out of parsed `zenith timeline list` CLI output.
        """

        # (L) main [b49f7954224a0ad25cc0013ea107b54b]
        # (L) ┣━ @0/16B5A50: test_cli_branch_list_main [20f98c79111b9015d84452258b7d5540]
        res = self.raw_cli(
            ['timeline', 'list', '--tenant-id', (tenant_id or self.env.initial_tenant).hex])
        timelines_cli = sorted(
            map(lambda branch_and_id: (branch_and_id[0], branch_and_id[1]),
                TIMELINE_DATA_EXTRACTOR.findall(res.stdout)))
        return timelines_cli

    def init(self,
             config_toml: str,
             initial_timeline_id: Optional[uuid.UUID] = None) -> 'subprocess.CompletedProcess[str]':
        with tempfile.NamedTemporaryFile(mode='w+') as tmp:
            tmp.write(config_toml)
            tmp.flush()

            cmd = ['init', f'--config={tmp.name}']
            if initial_timeline_id:
                cmd.extend(['--timeline-id', initial_timeline_id.hex])
            append_pageserver_param_overrides(cmd,
                                              self.env.pageserver.remote_storage,
                                              self.env.pageserver.config_override)

            res = self.raw_cli(cmd)
            res.check_returncode()
            return res

    def pageserver_start(self, overrides=()) -> 'subprocess.CompletedProcess[str]':
        start_args = ['pageserver', 'start', *overrides]
        append_pageserver_param_overrides(start_args,
                                          self.env.pageserver.remote_storage,
                                          self.env.pageserver.config_override)
        return self.raw_cli(start_args)

    def pageserver_stop(self, immediate=False) -> 'subprocess.CompletedProcess[str]':
        cmd = ['pageserver', 'stop']
        if immediate:
            cmd.extend(['-m', 'immediate'])

        log.info(f"Stopping pageserver with {cmd}")
        return self.raw_cli(cmd)

    def safekeeper_start(self, id: int) -> 'subprocess.CompletedProcess[str]':
        return self.raw_cli(['safekeeper', 'start', str(id)])

    def safekeeper_stop(self,
                        id: Optional[int] = None,
                        immediate=False) -> 'subprocess.CompletedProcess[str]':
        args = ['safekeeper', 'stop']
        if id is not None:
            args.extend(str(id))
        if immediate:
            args.extend(['-m', 'immediate'])
        return self.raw_cli(args)

    def pg_create(
        self,
        branch_name: str,
        node_name: Optional[str] = None,
        tenant_id: Optional[uuid.UUID] = None,
        lsn: Optional[str] = None,
        port: Optional[int] = None,
    ) -> 'subprocess.CompletedProcess[str]':
        args = [
            'pg',
            'create',
            '--tenant-id',
            (tenant_id or self.env.initial_tenant).hex,
            '--branch-name',
            branch_name,
        ]
        if lsn is not None:
            args.extend(['--lsn', lsn])
        if port is not None:
            args.extend(['--port', str(port)])
        if node_name is not None:
            args.append(node_name)

        res = self.raw_cli(args)
        res.check_returncode()
        return res

    def pg_start(
        self,
        node_name: str,
        tenant_id: Optional[uuid.UUID] = None,
        lsn: Optional[str] = None,
        port: Optional[int] = None,
    ) -> 'subprocess.CompletedProcess[str]':
        args = [
            'pg',
            'start',
            '--tenant-id',
            (tenant_id or self.env.initial_tenant).hex,
        ]
        if lsn is not None:
            args.append(f'--lsn={lsn}')
        if port is not None:
            args.append(f'--port={port}')
        if node_name is not None:
            args.append(node_name)

        res = self.raw_cli(args)
        res.check_returncode()
        return res

    def pg_stop(
        self,
        node_name: str,
        tenant_id: Optional[uuid.UUID] = None,
        destroy=False,
    ) -> 'subprocess.CompletedProcess[str]':
        args = [
            'pg',
            'stop',
            '--tenant-id',
            (tenant_id or self.env.initial_tenant).hex,
        ]
        if destroy:
            args.append('--destroy')
        if node_name is not None:
            args.append(node_name)

        return self.raw_cli(args)

    def raw_cli(self,
                arguments: List[str],
                check_return_code=True) -> 'subprocess.CompletedProcess[str]':
        """
        Run "zenith" with the specified arguments.

        Arguments must be in list form, e.g. ['pg', 'create']

        Return both stdout and stderr, which can be accessed as

        >>> result = env.zenith_cli.raw_cli(...)
        >>> assert result.stderr == ""
        >>> log.info(result.stdout)
        """

        assert type(arguments) == list

        bin_zenith = os.path.join(str(zenith_binpath), 'zenith')

        args = [bin_zenith] + arguments
        log.info('Running command "{}"'.format(' '.join(args)))
        log.info(f'Running in "{self.env.repo_dir}"')

        env_vars = os.environ.copy()
        env_vars['ZENITH_REPO_DIR'] = str(self.env.repo_dir)
        env_vars['POSTGRES_DISTRIB_DIR'] = str(pg_distrib_dir)

        if self.env.rust_log_override is not None:
            env_vars['RUST_LOG'] = self.env.rust_log_override

        # Pass coverage settings
        var = 'LLVM_PROFILE_FILE'
        val = os.environ.get(var)
        if val:
            env_vars[var] = val

        # Intercept CalledProcessError and print more info
        try:
            res = subprocess.run(args,
                                 env=env_vars,
                                 check=True,
                                 universal_newlines=True,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            log.info(f"Run success: {res.stdout}")
        except subprocess.CalledProcessError as exc:
            # this way command output will be in recorded and shown in CI in failure message
            msg = f"""\
            Run failed: {exc}
              stdout: {exc.stdout}
              stderr: {exc.stderr}
            """
            log.info(msg)

            raise Exception(msg) from exc

        if check_return_code:
            res.check_returncode()
        return res


class ZenithPageserver(PgProtocol):
    """
    An object representing a running pageserver.

    Initializes the repository via `zenith init`.
    """
    def __init__(self,
                 env: ZenithEnv,
                 port: PageserverPort,
                 remote_storage: Optional[RemoteStorage] = None,
                 config_override: Optional[str] = None):
        super().__init__(host='localhost', port=port.pg, username='zenith_admin')
        self.env = env
        self.running = False
        self.service_port = port  # do not shadow PgProtocol.port which is just int
        self.remote_storage = remote_storage
        self.config_override = config_override

    def start(self, overrides=()) -> 'ZenithPageserver':
        """
        Start the page server.
        `overrides` allows to add some config to this pageserver start.
        Returns self.
        """
        assert self.running == False

        self.env.zenith_cli.pageserver_start(overrides=overrides)
        self.running = True
        return self

    def stop(self, immediate=False) -> 'ZenithPageserver':
        """
        Stop the page server.
        Returns self.
        """
        if self.running:
            self.env.zenith_cli.pageserver_stop(immediate)
            self.running = False
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop(True)

    def http_client(self, auth_token: Optional[str] = None) -> ZenithPageserverHttpClient:
        return ZenithPageserverHttpClient(
            port=self.service_port.http,
            auth_token=auth_token,
        )


def append_pageserver_param_overrides(
    params_to_update: List[str],
    pageserver_remote_storage: Optional[RemoteStorage],
    pageserver_config_override: Optional[str] = None,
):
    if pageserver_remote_storage is not None:
        if isinstance(pageserver_remote_storage, LocalFsStorage):
            pageserver_storage_override = f"local_path='{pageserver_remote_storage.root}'"
        elif isinstance(pageserver_remote_storage, S3Storage):
            pageserver_storage_override = f"bucket_name='{pageserver_remote_storage.bucket}',\
                bucket_region='{pageserver_remote_storage.region}'"

            if pageserver_remote_storage.access_key is not None:
                pageserver_storage_override += f",access_key_id='{pageserver_remote_storage.access_key}'"
            if pageserver_remote_storage.secret_key is not None:
                pageserver_storage_override += f",secret_access_key='{pageserver_remote_storage.secret_key}'"
            if pageserver_remote_storage.endpoint is not None:
                pageserver_storage_override += f",endpoint='{pageserver_remote_storage.endpoint}'"

        else:
            raise Exception(f'Unknown storage configuration {pageserver_remote_storage}')
        params_to_update.append(
            f'--pageserver-config-override=remote_storage={{{pageserver_storage_override}}}')

    env_overrides = os.getenv('ZENITH_PAGESERVER_OVERRIDES')
    if env_overrides is not None:
        params_to_update += [
            f'--pageserver-config-override={o.strip()}' for o in env_overrides.split(';')
        ]

    if pageserver_config_override is not None:
        params_to_update += [
            f'--pageserver-config-override={o.strip()}'
            for o in pageserver_config_override.split(';')
        ]


class PgBin:
    """ A helper class for executing postgres binaries """
    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        self.pg_bin_path = os.path.join(str(pg_distrib_dir), 'bin')
        self.env = os.environ.copy()
        self.env['LD_LIBRARY_PATH'] = os.path.join(str(pg_distrib_dir), 'lib')

    def _fixpath(self, command: List[str]):
        if '/' not in command[0]:
            command[0] = os.path.join(self.pg_bin_path, command[0])

    def _build_env(self, env_add: Optional[Env]) -> Env:
        if env_add is None:
            return self.env
        env = self.env.copy()
        env.update(env_add)
        return env

    def run(self, command: List[str], env: Optional[Env] = None, cwd: Optional[str] = None):
        """
        Run one of the postgres binaries.

        The command should be in list form, e.g. ['pgbench', '-p', '55432']

        All the necessary environment variables will be set.

        If the first argument (the command name) doesn't include a path (no '/'
        characters present), then it will be edited to include the correct path.

        If you want stdout/stderr captured to files, use `run_capture` instead.
        """

        self._fixpath(command)
        log.info('Running command "{}"'.format(' '.join(command)))
        env = self._build_env(env)
        subprocess.run(command, env=env, cwd=cwd, check=True)

    def run_capture(self,
                    command: List[str],
                    env: Optional[Env] = None,
                    cwd: Optional[str] = None,
                    **kwargs: Any) -> str:
        """
        Run one of the postgres binaries, with stderr and stdout redirected to a file.

        This is just like `run`, but for chatty programs. Returns basepath for files
        with captured output.
        """

        self._fixpath(command)
        log.info('Running command "{}"'.format(' '.join(command)))
        env = self._build_env(env)
        return subprocess_capture(self.log_dir, command, env=env, cwd=cwd, check=True, **kwargs)


@pytest.fixture(scope='function')
def pg_bin(test_output_dir: str) -> PgBin:
    return PgBin(test_output_dir)


class VanillaPostgres(PgProtocol):
    def __init__(self, pgdatadir: str, pg_bin: PgBin, port: int):
        super().__init__(host='localhost', port=port)
        self.pgdatadir = pgdatadir
        self.pg_bin = pg_bin
        self.running = False
        self.pg_bin.run_capture(['initdb', '-D', pgdatadir])

    def configure(self, options: List[str]):
        """Append lines into postgresql.conf file."""
        assert not self.running
        with open(os.path.join(self.pgdatadir, 'postgresql.conf'), 'a') as conf_file:
            conf_file.writelines(options)

    def start(self):
        assert not self.running
        self.running = True
        self.pg_bin.run_capture(['pg_ctl', '-D', self.pgdatadir, 'start'])

    def stop(self):
        assert self.running
        self.running = False
        self.pg_bin.run_capture(['pg_ctl', '-D', self.pgdatadir, 'stop'])

    def get_subdir_size(self, subdir) -> int:
        """Return size of pgdatadir subdirectory in bytes."""
        return get_dir_size(os.path.join(self.pgdatadir, subdir))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.running:
            self.stop()


@pytest.fixture(scope='function')
def vanilla_pg(test_output_dir: str) -> Iterator[VanillaPostgres]:
    pgdatadir = os.path.join(test_output_dir, "pgdata-vanilla")
    pg_bin = PgBin(test_output_dir)
    with VanillaPostgres(pgdatadir, pg_bin, 5432) as vanilla_pg:
        yield vanilla_pg


class ZenithProxy(PgProtocol):
    def __init__(self, port: int):
        super().__init__(host="127.0.0.1", username="pytest", password="pytest", port=port)
        self.http_port = 7001
        self._popen: Optional[subprocess.Popen[bytes]] = None

    def start_static(self, addr="127.0.0.1:5432") -> None:
        assert self._popen is None

        # Start proxy
        bin_proxy = os.path.join(str(zenith_binpath), 'proxy')
        args = [bin_proxy]
        args.extend(["--http", f"{self.host}:{self.http_port}"])
        args.extend(["--proxy", f"{self.host}:{self.port}"])
        args.extend(["--auth-method", "password"])
        args.extend(["--static-router", addr])
        self._popen = subprocess.Popen(args)
        self._wait_until_ready()

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_time=10)
    def _wait_until_ready(self):
        requests.get(f"http://{self.host}:{self.http_port}/v1/status")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._popen is not None:
            # NOTE the process will die when we're done with tests anyway, because
            # it's a child process. This is mostly to clean up in between different tests.
            self._popen.kill()


@pytest.fixture(scope='function')
def static_proxy(vanilla_pg) -> Iterator[ZenithProxy]:
    """Zenith proxy that routes directly to vanilla postgres."""
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user pytest with password 'pytest';")

    with ZenithProxy(4432) as proxy:
        proxy.start_static()
        yield proxy


class Postgres(PgProtocol):
    """ An object representing a running postgres daemon. """
    def __init__(self, env: ZenithEnv, tenant_id: uuid.UUID, port: int):
        super().__init__(host='localhost', port=port, username='zenith_admin')

        self.env = env
        self.running = False
        self.node_name: Optional[str] = None  # dubious, see asserts below
        self.pgdata_dir: Optional[str] = None  # Path to computenode PGDATA
        self.tenant_id = tenant_id
        # path to conf is <repo_dir>/pgdatadirs/tenants/<tenant_id>/<node_name>/postgresql.conf

    def create(
        self,
        branch_name: str,
        node_name: Optional[str] = None,
        lsn: Optional[str] = None,
        config_lines: Optional[List[str]] = None,
    ) -> 'Postgres':
        """
        Create the pg data directory.
        Returns self.
        """

        if not config_lines:
            config_lines = []

        self.node_name = node_name or f'{branch_name}_pg_node'
        self.env.zenith_cli.pg_create(branch_name,
                                      node_name=self.node_name,
                                      tenant_id=self.tenant_id,
                                      lsn=lsn,
                                      port=self.port)
        path = pathlib.Path('pgdatadirs') / 'tenants' / self.tenant_id.hex / self.node_name
        self.pgdata_dir = os.path.join(self.env.repo_dir, path)

        if config_lines is None:
            config_lines = []

        # set small 'max_replication_write_lag' to enable backpressure
        # and make tests more stable.
        config_lines = ['max_replication_write_lag=15MB'] + config_lines
        self.config(config_lines)

        return self

    def start(self) -> 'Postgres':
        """
        Start the Postgres instance.
        Returns self.
        """

        assert self.node_name is not None

        log.info(f"Starting postgres node {self.node_name}")

        run_result = self.env.zenith_cli.pg_start(self.node_name,
                                                  tenant_id=self.tenant_id,
                                                  port=self.port)
        self.running = True

        log.info(f"stdout: {run_result.stdout}")

        return self

    def pg_data_dir_path(self) -> str:
        """ Path to data directory """
        assert self.node_name
        path = pathlib.Path('pgdatadirs') / 'tenants' / self.tenant_id.hex / self.node_name
        return os.path.join(self.env.repo_dir, path)

    def pg_xact_dir_path(self) -> str:
        """ Path to pg_xact dir """
        return os.path.join(self.pg_data_dir_path(), 'pg_xact')

    def pg_twophase_dir_path(self) -> str:
        """ Path to pg_twophase dir """
        return os.path.join(self.pg_data_dir_path(), 'pg_twophase')

    def config_file_path(self) -> str:
        """ Path to postgresql.conf """
        return os.path.join(self.pg_data_dir_path(), 'postgresql.conf')

    def adjust_for_wal_acceptors(self, wal_acceptors: str) -> 'Postgres':
        """
        Adjust instance config for working with wal acceptors instead of
        pageserver (pre-configured by CLI) directly.
        """

        # TODO: reuse config()
        with open(self.config_file_path(), "r") as f:
            cfg_lines = f.readlines()
        with open(self.config_file_path(), "w") as f:
            for cfg_line in cfg_lines:
                # walproposer uses different application_name
                if ("synchronous_standby_names" in cfg_line or
                        # don't ask pageserver to fetch WAL from compute
                        "callmemaybe_connstring" in cfg_line or
                        # don't repeat wal_acceptors multiple times
                        "wal_acceptors" in cfg_line):
                    continue
                f.write(cfg_line)
            f.write("synchronous_standby_names = 'walproposer'\n")
            f.write("wal_acceptors = '{}'\n".format(wal_acceptors))
        return self

    def config(self, lines: List[str]) -> 'Postgres':
        """
        Add lines to postgresql.conf.
        Lines should be an array of valid postgresql.conf rows.
        Returns self.
        """

        with open(self.config_file_path(), 'a') as conf:
            for line in lines:
                conf.write(line)
                conf.write('\n')

        return self

    def stop(self) -> 'Postgres':
        """
        Stop the Postgres instance if it's running.
        Returns self.
        """

        if self.running:
            assert self.node_name is not None
            self.env.zenith_cli.pg_stop(self.node_name, self.tenant_id)
            self.running = False

        return self

    def stop_and_destroy(self) -> 'Postgres':
        """
        Stop the Postgres instance, then destroy it.
        Returns self.
        """

        assert self.node_name is not None
        self.env.zenith_cli.pg_stop(self.node_name, self.tenant_id, True)
        self.node_name = None

        return self

    def create_start(
        self,
        branch_name: str,
        node_name: Optional[str] = None,
        lsn: Optional[str] = None,
        config_lines: Optional[List[str]] = None,
    ) -> 'Postgres':
        """
        Create a Postgres instance, apply config
        and then start it.
        Returns self.
        """

        self.create(
            branch_name=branch_name,
            node_name=node_name,
            config_lines=config_lines,
            lsn=lsn,
        ).start()

        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


class PostgresFactory:
    """ An object representing multiple running postgres daemons. """
    def __init__(self, env: ZenithEnv):
        self.env = env
        self.num_instances = 0
        self.instances: List[Postgres] = []

    def create_start(self,
                     branch_name: str,
                     node_name: Optional[str] = None,
                     tenant_id: Optional[uuid.UUID] = None,
                     lsn: Optional[str] = None,
                     config_lines: Optional[List[str]] = None) -> Postgres:

        pg = Postgres(
            self.env,
            tenant_id=tenant_id or self.env.initial_tenant,
            port=self.env.port_distributor.get_port(),
        )
        self.num_instances += 1
        self.instances.append(pg)

        return pg.create_start(
            branch_name=branch_name,
            node_name=node_name,
            config_lines=config_lines,
            lsn=lsn,
        )

    def create(self,
               branch_name: str,
               node_name: Optional[str] = None,
               tenant_id: Optional[uuid.UUID] = None,
               lsn: Optional[str] = None,
               config_lines: Optional[List[str]] = None) -> Postgres:

        pg = Postgres(
            self.env,
            tenant_id=tenant_id or self.env.initial_tenant,
            port=self.env.port_distributor.get_port(),
        )

        self.num_instances += 1
        self.instances.append(pg)

        return pg.create(
            branch_name=branch_name,
            node_name=node_name,
            lsn=lsn,
            config_lines=config_lines,
        )

    def stop_all(self) -> 'PostgresFactory':
        for pg in self.instances:
            pg.stop()

        return self


def read_pid(path: Path) -> int:
    """ Read content of file into number """
    return int(path.read_text())


@dataclass
class SafekeeperPort:
    pg: int
    http: int


@dataclass
class Safekeeper:
    """ An object representing a running safekeeper daemon. """
    env: ZenithEnv
    port: SafekeeperPort
    id: int
    auth_token: Optional[str] = None
    running: bool = False

    def start(self) -> 'Safekeeper':
        assert self.running == False
        self.env.zenith_cli.safekeeper_start(self.id)
        self.running = True
        # wait for wal acceptor start by checking its status
        started_at = time.time()
        while True:
            try:
                http_cli = self.http_client()
                http_cli.check_status()
            except Exception as e:
                elapsed = time.time() - started_at
                if elapsed > 3:
                    raise RuntimeError(
                        f"timed out waiting {elapsed:.0f}s for wal acceptor start: {e}")
                time.sleep(0.5)
            else:
                break  # success
        return self

    def stop(self, immediate=False) -> 'Safekeeper':
        log.info('Stopping safekeeper {}'.format(self.id))
        self.env.zenith_cli.safekeeper_stop(self.id, immediate)
        self.running = False
        return self

    def append_logical_message(self,
                               tenant_id: uuid.UUID,
                               timeline_id: uuid.UUID,
                               request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send JSON_CTRL query to append LogicalMessage to WAL and modify
        safekeeper state. It will construct LogicalMessage from provided
        prefix and message, and then will write it to WAL.
        """

        # "replication=0" hacks psycopg not to send additional queries
        # on startup, see https://github.com/psycopg/psycopg2/pull/482
        connstr = f"host=localhost port={self.port.pg} replication=0 options='-c ztimelineid={timeline_id.hex} ztenantid={tenant_id.hex}'"

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

    def http_client(self) -> SafekeeperHttpClient:
        return SafekeeperHttpClient(port=self.port.http)


@dataclass
class SafekeeperTimelineStatus:
    acceptor_epoch: int
    flush_lsn: str
    remote_consistent_lsn: str


@dataclass
class SafekeeperMetrics:
    # These are metrics from Prometheus which uses float64 internally.
    # As a consequence, values may differ from real original int64s.
    flush_lsn_inexact: Dict[Tuple[str, str], int] = field(default_factory=dict)
    commit_lsn_inexact: Dict[Tuple[str, str], int] = field(default_factory=dict)
    flush_wal_count: Dict[Tuple[str, str], int] = field(default_factory=dict)


class SafekeeperHttpClient(requests.Session):
    def __init__(self, port: int):
        super().__init__()
        self.port = port

    def check_status(self):
        self.get(f"http://localhost:{self.port}/v1/status").raise_for_status()

    def timeline_status(self, tenant_id: str, timeline_id: str) -> SafekeeperTimelineStatus:
        res = self.get(f"http://localhost:{self.port}/v1/timeline/{tenant_id}/{timeline_id}")
        res.raise_for_status()
        resj = res.json()
        return SafekeeperTimelineStatus(acceptor_epoch=resj['acceptor_state']['epoch'],
                                        flush_lsn=resj['flush_lsn'],
                                        remote_consistent_lsn=resj['remote_consistent_lsn'])

    def get_metrics(self) -> SafekeeperMetrics:
        request_result = self.get(f"http://localhost:{self.port}/metrics")
        request_result.raise_for_status()
        all_metrics_text = request_result.text

        metrics = SafekeeperMetrics()
        for match in re.finditer(
                r'^safekeeper_flush_lsn{tenant_id="([0-9a-f]+)",timeline_id="([0-9a-f]+)"} (\S+)$',
                all_metrics_text,
                re.MULTILINE):
            metrics.flush_lsn_inexact[(match.group(1), match.group(2))] = int(match.group(3))
        for match in re.finditer(
                r'^safekeeper_commit_lsn{tenant_id="([0-9a-f]+)",timeline_id="([0-9a-f]+)"} (\S+)$',
                all_metrics_text,
                re.MULTILINE):
            metrics.commit_lsn_inexact[(match.group(1), match.group(2))] = int(match.group(3))
        for match in re.finditer(
                r'^safekeeper_flush_wal_seconds_count{tenant_id="([0-9a-f]+)",timeline_id="([0-9a-f]+)"} (\S+)$',
                all_metrics_text,
                re.MULTILINE):
            metrics.flush_wal_count[(match.group(1), match.group(2))] = int(match.group(3))
        return metrics


@dataclass
class Etcd:
    """ An object managing etcd instance """
    datadir: str
    port: int
    peer_port: int
    handle: Optional[subprocess.Popen[Any]] = None  # handle of running daemon

    def check_status(self):
        s = requests.Session()
        s.mount('http://', requests.adapters.HTTPAdapter(max_retries=1))  # do not retry
        s.get(f"http://localhost:{self.port}/health").raise_for_status()

    def start(self):
        pathlib.Path(self.datadir).mkdir(exist_ok=True)
        etcd_full_path = etcd_path()
        if etcd_full_path is None:
            raise Exception('etcd not found')

        with open(os.path.join(self.datadir, "etcd.log"), "wb") as log_file:
            args = [
                etcd_full_path,
                f"--data-dir={self.datadir}",
                f"--listen-client-urls=http://localhost:{self.port}",
                f"--advertise-client-urls=http://localhost:{self.port}",
                f"--listen-peer-urls=http://localhost:{self.peer_port}"
            ]
            self.handle = subprocess.Popen(args, stdout=log_file, stderr=log_file)

        # wait for start
        started_at = time.time()
        while True:
            try:
                self.check_status()
            except Exception as e:
                elapsed = time.time() - started_at
                if elapsed > 5:
                    raise RuntimeError(f"timed out waiting {elapsed:.0f}s for etcd start: {e}")
                time.sleep(0.5)
            else:
                break  # success

    def stop(self):
        if self.handle is not None:
            self.handle.terminate()
            self.handle.wait()


def get_test_output_dir(request: Any) -> str:
    """ Compute the working directory for an individual test. """
    test_name = request.node.name
    test_dir = os.path.join(str(top_output_dir), test_name)
    log.info(f'get_test_output_dir is {test_dir}')
    return test_dir


# This is autouse, so the test output directory always gets created, even
# if a test doesn't put anything there. It also solves a problem with the
# zenith_simple_env fixture: if TEST_SHARED_FIXTURES is not set, it
# creates the repo in the test output directory. But it cannot depend on
# 'test_output_dir' fixture, because when TEST_SHARED_FIXTURES is not set,
# it has 'session' scope and cannot access fixtures with 'function'
# scope. So it uses the get_test_output_dir() function to get the path, and
# this fixture ensures that the directory exists.  That works because
# 'autouse' fixtures are run before other fixtures.
@pytest.fixture(scope='function', autouse=True)
def test_output_dir(request: Any) -> str:
    """ Create the working directory for an individual test. """

    # one directory per test
    test_dir = get_test_output_dir(request)
    log.info(f'test_output_dir is {test_dir}')
    shutil.rmtree(test_dir, ignore_errors=True)
    mkdir_if_needed(test_dir)
    return test_dir


SKIP_DIRS = frozenset(('pg_wal', 'pg_stat', 'pg_stat_tmp', 'pg_subtrans', 'pg_logical'))

SKIP_FILES = frozenset(('pg_internal.init',
                        'pg.log',
                        'zenith.signal',
                        'postgresql.conf',
                        'postmaster.opts',
                        'postmaster.pid',
                        'pg_control'))


def should_skip_dir(dirname: str) -> bool:
    return dirname in SKIP_DIRS


def should_skip_file(filename: str) -> bool:
    if filename in SKIP_FILES:
        return True
    # check for temp table files according to https://www.postgresql.org/docs/current/storage-file-layout.html
    # i e "tBBB_FFF"
    if not filename.startswith('t'):
        return False

    tmp_name = filename[1:].split('_')
    if len(tmp_name) != 2:
        return False

    try:
        list(map(int, tmp_name))
    except:
        return False
    return True


#
# Test helpers
#
def list_files_to_compare(pgdata_dir: str):
    pgdata_files = []
    for root, _file, filenames in os.walk(pgdata_dir):
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
def check_restored_datadir_content(test_output_dir: str, env: ZenithEnv, pg: Postgres):

    # Get the timeline ID. We need it for the 'basebackup' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

    # stop postgres to ensure that files won't change
    pg.stop()

    # Take a basebackup from pageserver
    restored_dir_path = os.path.join(env.repo_dir, f"{pg.node_name}_restored_datadir")
    mkdir_if_needed(restored_dir_path)

    pg_bin = PgBin(test_output_dir)
    psql_path = os.path.join(pg_bin.pg_bin_path, 'psql')

    cmd = rf"""
        {psql_path}                                    \
            --no-psqlrc                                \
            postgres://localhost:{env.pageserver.service_port.pg}  \
            -c 'basebackup {pg.tenant_id.hex} {timeline}'  \
         | tar -x -C {restored_dir_path}
    """

    # Set LD_LIBRARY_PATH in the env properly, otherwise we may use the wrong libpq.
    # PgBin sets it automatically, but here we need to pipe psql output to the tar command.
    psql_env = {'LD_LIBRARY_PATH': os.path.join(str(pg_distrib_dir), 'lib')}
    result = subprocess.run(cmd, env=psql_env, capture_output=True, text=True, shell=True)

    # Print captured stdout/stderr if basebackup cmd failed.
    if result.returncode != 0:
        log.error('Basebackup shell command failed with:')
        log.error(result.stdout)
        log.error(result.stderr)
    assert result.returncode == 0

    # list files we're going to compare
    assert pg.pgdata_dir
    pgdata_files = list_files_to_compare(pg.pgdata_dir)
    restored_files = list_files_to_compare(restored_dir_path)

    # check that file sets are equal
    assert pgdata_files == restored_files

    # compare content of the files
    # filecmp returns (match, mismatch, error) lists
    # We've already filtered all mismatching files in list_files_to_compare(),
    # so here expect that the content is identical
    (match, mismatch, error) = filecmp.cmpfiles(pg.pgdata_dir,
                                                restored_dir_path,
                                                pgdata_files,
                                                shallow=False)
    log.info(f'filecmp result mismatch and error lists:\n\t mismatch={mismatch}\n\t error={error}')

    for f in mismatch:

        f1 = os.path.join(pg.pgdata_dir, f)
        f2 = os.path.join(restored_dir_path, f)
        stdout_filename = "{}.filediff".format(f2)

        with open(stdout_filename, 'w') as stdout_f:
            subprocess.run("xxd -b {} > {}.hex ".format(f1, f1), shell=True)
            subprocess.run("xxd -b {} > {}.hex ".format(f2, f2), shell=True)

            cmd = 'diff {}.hex {}.hex'.format(f1, f2)
            subprocess.run([cmd], stdout=stdout_f, shell=True)

    assert (mismatch, error) == ([], [])


def wait_for(number_of_iterations: int, interval: int, func):
    last_exception = None
    for i in range(number_of_iterations):
        try:
            res = func()
        except Exception as e:
            log.info("waiting for %s iteration %s failed", func, i + 1)
            last_exception = e
            time.sleep(interval)
            continue
        return res
    raise Exception("timed out while waiting for %s" % func) from last_exception


def assert_local(pageserver_http_client: ZenithPageserverHttpClient,
                 tenant: uuid.UUID,
                 timeline: uuid.UUID):
    timeline_detail = pageserver_http_client.timeline_detail(tenant, timeline)
    assert timeline_detail.get('local', {}).get("disk_consistent_lsn"), timeline_detail
    return timeline_detail


def remote_consistent_lsn(pageserver_http_client: ZenithPageserverHttpClient,
                          tenant: uuid.UUID,
                          timeline: uuid.UUID) -> int:
    detail = pageserver_http_client.timeline_detail(tenant, timeline)

    lsn_str = detail['remote']['remote_consistent_lsn']
    assert isinstance(lsn_str, str)
    return lsn_from_hex(lsn_str)


def wait_for_upload(pageserver_http_client: ZenithPageserverHttpClient,
                    tenant: uuid.UUID,
                    timeline: uuid.UUID,
                    lsn: int):
    """waits for local timeline upload up to specified lsn"""

    wait_for(10, 1, lambda: remote_consistent_lsn(pageserver_http_client, tenant, timeline) >= lsn)


def last_record_lsn(pageserver_http_client: ZenithPageserverHttpClient,
                    tenant: uuid.UUID,
                    timeline: uuid.UUID) -> int:
    detail = pageserver_http_client.timeline_detail(tenant, timeline)

    lsn_str = detail['local']['last_record_lsn']
    assert isinstance(lsn_str, str)
    return lsn_from_hex(lsn_str)


def wait_for_last_record_lsn(pageserver_http_client: ZenithPageserverHttpClient,
                             tenant: uuid.UUID,
                             timeline: uuid.UUID,
                             lsn: int):
    """waits for pageserver to catch up to a certain lsn"""

    wait_for(10, 1, lambda: last_record_lsn(pageserver_http_client, tenant, timeline) >= lsn)
