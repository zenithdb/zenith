from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, VanillaPostgres
from psycopg2.errors import UndefinedObject
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

if TYPE_CHECKING:
    from typing import Any, Self

    from fixtures.httpserver import ListenAddress


def handle_db(dbs, roles, operation):
    if operation["op"] == "set":
        if "old_name" in operation and operation["old_name"] in dbs:
            dbs[operation["name"]] = dbs[operation["old_name"]]
            dbs.pop(operation["old_name"])
        if "owner" in operation:
            dbs[operation["name"]] = operation["owner"]
    elif operation["op"] == "del":
        dbs.pop(operation["name"])
    else:
        raise ValueError("Invalid op")


def handle_role(dbs, roles, operation):
    if operation["op"] == "set":
        if "old_name" in operation and operation["old_name"] in roles:
            roles[operation["name"]] = roles[operation["old_name"]]
            roles.pop(operation["old_name"])
            for db, owner in dbs.items():
                if owner == operation["old_name"]:
                    dbs[db] = operation["name"]
        if "password" in operation:
            roles[operation["name"]] = operation["password"]
            assert "encrypted_password" in operation
    elif operation["op"] == "del":
        if "old_name" in operation:
            roles.pop(operation["old_name"])
        roles.pop(operation["name"])
    else:
        raise ValueError("Invalid op")


def ddl_forward_handler(
    request: Request, dbs: dict[str, str], roles: dict[str, str], ddl: DdlForwardingContext
) -> Response:
    log.info(f"Received request with data {request.get_data(as_text=True)}")
    if ddl.fail:
        log.info("FAILING")
        return Response(status=500, response="Failed just cuz")
    if request.json is None:
        log.info("Received invalid JSON")
        return Response(status=400)
    json: dict[str, list[str]] = request.json
    # Handle roles first
    for operation in json.get("roles", []):
        handle_role(dbs, roles, operation)
    for operation in json.get("dbs", []):
        handle_db(dbs, roles, operation)
    return Response(status=200)


class DdlForwardingContext:
    def __init__(self, httpserver: HTTPServer, vanilla_pg: VanillaPostgres, host: str, port: int):
        self.server = httpserver
        self.pg = vanilla_pg
        self.host = host
        self.port = port
        self.dbs: dict[str, str] = {}
        self.roles: dict[str, str] = {}
        self.fail = False
        endpoint = "/test/roles_and_databases"
        ddl_url = f"http://{host}:{port}{endpoint}"
        self.pg.configure(
            [
                f"neon.console_url={ddl_url}",
                "shared_preload_libraries = 'neon'",
            ]
        )
        log.info(f"Listening on {ddl_url}")
        self.server.expect_request(endpoint, method="PATCH").respond_with_handler(
            lambda request: ddl_forward_handler(request, self.dbs, self.roles, self)
        )

    def __enter__(self) -> Self:
        self.pg.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ):
        self.pg.stop()

    def send(self, query: str) -> list[tuple[Any, ...]]:
        return self.pg.safe_psql(query)

    def wait(self, timeout=3):
        self.server.wait(timeout=timeout)

    def failures(self, bool):
        self.fail = bool

    def send_and_wait(self, query: str, timeout=3) -> list[tuple[Any, ...]]:
        res = self.send(query)
        self.wait(timeout=timeout)
        return res


@pytest.fixture(scope="function")
def ddl(
    httpserver: HTTPServer, vanilla_pg: VanillaPostgres, httpserver_listen_address: ListenAddress
):
    (host, port) = httpserver_listen_address
    with DdlForwardingContext(httpserver, vanilla_pg, host, port) as ddl:
        yield ddl


def test_ddl_forwarding(ddl: DdlForwardingContext):
    curr_user = ddl.send("SELECT current_user")[0][0]
    log.info(f"Current user is {curr_user}")
    ddl.send_and_wait("CREATE DATABASE bork")
    assert ddl.dbs == {"bork": curr_user}
    ddl.send_and_wait("CREATE ROLE volk WITH PASSWORD 'nu_zayats'")
    ddl.send_and_wait("ALTER DATABASE bork RENAME TO nu_pogodi")
    assert ddl.dbs == {"nu_pogodi": curr_user}
    ddl.send_and_wait("ALTER DATABASE nu_pogodi OWNER TO volk")
    assert ddl.dbs == {"nu_pogodi": "volk"}
    ddl.send_and_wait("DROP DATABASE nu_pogodi")
    assert ddl.dbs == {}
    ddl.send_and_wait("DROP ROLE volk")
    assert ddl.roles == {}

    ddl.send_and_wait("CREATE ROLE tarzan WITH PASSWORD 'of_the_apes'")
    assert ddl.roles == {"tarzan": "of_the_apes"}
    ddl.send_and_wait("DROP ROLE tarzan")
    assert ddl.roles == {}
    ddl.send_and_wait("CREATE ROLE tarzan WITH PASSWORD 'of_the_apes'")
    assert ddl.roles == {"tarzan": "of_the_apes"}
    ddl.send_and_wait("ALTER ROLE tarzan WITH PASSWORD 'jungle_man'")
    assert ddl.roles == {"tarzan": "jungle_man"}
    ddl.send_and_wait("ALTER ROLE tarzan RENAME TO mowgli")
    assert ddl.roles == {"mowgli": "jungle_man"}
    ddl.send_and_wait("DROP ROLE mowgli")
    assert ddl.roles == {}

    conn = ddl.pg.connect()
    cur = conn.cursor()

    cur.execute("BEGIN")
    cur.execute("CREATE ROLE bork WITH PASSWORD 'cork'")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {"bork": "cork"}
    cur.execute("BEGIN")
    cur.execute("CREATE ROLE stork WITH PASSWORD 'pork'")
    cur.execute("ABORT")
    ddl.wait()
    assert ("stork", "pork") not in ddl.roles.items()
    cur.execute("BEGIN")
    cur.execute("ALTER ROLE bork WITH PASSWORD 'pork'")
    cur.execute("ALTER ROLE bork RENAME TO stork")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {"stork": "pork"}
    cur.execute("BEGIN")
    cur.execute("CREATE ROLE dork WITH PASSWORD 'york'")
    cur.execute("SAVEPOINT point")
    cur.execute("ALTER ROLE dork WITH PASSWORD 'zork'")
    cur.execute("ALTER ROLE dork RENAME TO fork")
    cur.execute("ROLLBACK TO SAVEPOINT point")
    cur.execute("ALTER ROLE dork WITH PASSWORD 'fork'")
    cur.execute("ALTER ROLE dork RENAME TO zork")
    cur.execute("RELEASE SAVEPOINT point")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {"stork": "pork", "zork": "fork"}

    cur.execute("DROP ROLE stork")
    cur.execute("DROP ROLE zork")
    ddl.wait()
    assert ddl.roles == {}

    cur.execute("CREATE ROLE bork WITH PASSWORD 'dork'")
    cur.execute("CREATE ROLE stork WITH PASSWORD 'cork'")
    cur.execute("BEGIN")
    cur.execute("DROP ROLE bork")
    cur.execute("ALTER ROLE stork RENAME TO bork")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {"bork": "cork"}

    cur.execute("DROP ROLE bork")
    ddl.wait()
    assert ddl.roles == {}

    cur.execute("CREATE ROLE bork WITH PASSWORD 'newyork'")
    cur.execute("BEGIN")
    cur.execute("SAVEPOINT point")
    cur.execute("DROP ROLE bork")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {}

    cur.execute("CREATE ROLE bork WITH PASSWORD 'oldyork'")
    cur.execute("BEGIN")
    cur.execute("SAVEPOINT point")
    cur.execute("ALTER ROLE bork PASSWORD NULL")
    cur.execute("COMMIT")
    cur.execute("DROP ROLE bork")
    ddl.wait()
    assert ddl.roles == {}

    cur.execute("CREATE ROLE bork WITH PASSWORD 'dork'")
    cur.execute("CREATE DATABASE stork WITH OWNER=bork")
    cur.execute("ALTER ROLE bork RENAME TO cork")
    ddl.wait()
    assert ddl.dbs == {"stork": "cork"}

    cur.execute("DROP DATABASE stork")
    ddl.wait()
    assert ddl.dbs == {}

    with pytest.raises(psycopg2.InternalError):
        ddl.failures(True)
        cur.execute("CREATE DATABASE failure WITH OWNER=cork")
        ddl.wait()

    ddl.failures(False)
    cur.execute("CREATE DATABASE failure WITH OWNER=cork")
    ddl.wait()
    with pytest.raises(psycopg2.InternalError):
        ddl.failures(True)
        cur.execute("DROP DATABASE failure")
        ddl.wait()
    assert ddl.dbs == {"failure": "cork"}
    ddl.failures(False)

    # Check that db is still in the Postgres after failure
    cur.execute("SELECT datconnlimit FROM pg_database WHERE datname = 'failure'")
    result = cur.fetchone()
    if not result:
        raise AssertionError("Database 'failure' not found")
    # -2 means invalid database
    # It should be invalid because cplane request failed
    assert result[0] == -2, "Database 'failure' is not invalid"

    # Check that repeated drop succeeds
    cur.execute("DROP DATABASE failure")
    ddl.wait()
    assert ddl.dbs == {}

    # DB should be absent in the Postgres
    cur.execute("SELECT count(*) FROM pg_database WHERE datname = 'failure'")
    result = cur.fetchone()
    if not result:
        raise AssertionError("Could not count databases")
    assert result[0] == 0, "Database 'failure' still exists after drop"

    # We don't have compute_ctl, so here, so create neon_superuser here manually
    cur.execute("CREATE ROLE neon_superuser NOLOGIN CREATEDB CREATEROLE")

    # Contrary to popular belief, being superman does not make you superuser
    cur.execute("CREATE ROLE superman LOGIN NOSUPERUSER PASSWORD 'jungle_man'")

    with ddl.pg.cursor(user="superman", password="jungle_man") as superman_cur:
        # We allow real SUPERUSERs to ALTER neon_superuser
        with pytest.raises(psycopg2.InternalError):
            superman_cur.execute("ALTER ROLE neon_superuser LOGIN")

    cur.execute("ALTER ROLE neon_superuser LOGIN")

    with pytest.raises(psycopg2.InternalError):
        cur.execute("CREATE DATABASE trololobus WITH OWNER neon_superuser")

    cur.execute("CREATE DATABASE trololobus")
    with pytest.raises(psycopg2.InternalError):
        cur.execute("ALTER DATABASE trololobus OWNER TO neon_superuser")

    conn.close()


# Assert that specified database has a specific connlimit, throwing an AssertionError otherwise
# -2 means invalid database
# -1 means no specific per-db limit (default)
def assert_db_connlimit(endpoint: Any, db_name: str, connlimit: int, msg: str):
    with endpoint.cursor() as cur:
        cur.execute("SELECT datconnlimit FROM pg_database WHERE datname = %s", (db_name,))
        result = cur.fetchone()
        if not result:
            raise AssertionError(f"Database '{db_name}' not found")
        assert result[0] == connlimit, msg


# Test that compute_ctl can deal with invalid databases (drop them).
# If Postgres extension cannot reach cplane, then DROP will be aborted
# and database will be marked as invalid. Then there are two recovery
# flows:
# 1. User can just repeat DROP DATABASE command until it succeeds
# 2. User can ignore, then compute_ctl will drop invalid databases
#    automatically during full configuration
# Here we test the latter. The first one is tested in test_ddl_forwarding
def test_ddl_forwarding_invalid_db(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        # Some non-existent url
        config_lines=["neon.console_url=http://localhost:9999/unknown/api/v0/roles_and_databases"],
    )

    with endpoint.cursor() as cur:
        cur.execute("SET neon.forward_ddl = false")
        cur.execute("CREATE DATABASE failure")
        cur.execute("COMMIT")

    assert_db_connlimit(
        endpoint, "failure", -1, "Database 'failure' doesn't have a valid connlimit"
    )

    with pytest.raises(psycopg2.InternalError):
        with endpoint.cursor() as cur:
            cur.execute("DROP DATABASE failure")
            cur.execute("COMMIT")

    # Should be invalid after failed drop
    assert_db_connlimit(endpoint, "failure", -2, "Database 'failure' ins't invalid")

    endpoint.stop()
    endpoint.start()

    # Still invalid after restart without full configuration
    assert_db_connlimit(endpoint, "failure", -2, "Database 'failure' ins't invalid")

    endpoint.stop()
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    # Should be cleaned up by compute_ctl during full configuration
    with endpoint.cursor() as cur:
        cur.execute("SELECT count(*) FROM pg_database WHERE datname = 'failure'")
        result = cur.fetchone()
        if not result:
            raise AssertionError("Could not count databases")
        assert result[0] == 0, "Database 'failure' still exists after restart"


def test_ddl_forwarding_role_specs(neon_simple_env: NeonEnv):
    """
    Postgres has a concept of role specs:

        ROLESPEC_CSTRING: ALTER ROLE xyz
        ROLESPEC_CURRENT_USER: ALTER ROLE current_user
        ROLESPEC_CURRENT_ROLE: ALTER ROLE current_role
        ROLESPEC_SESSION_USER: ALTER ROLE session_user
        ROLESPEC_PUBLIC: ALTER ROLE public

    The extension is required to serialize these special role spec into
    usernames for the purpose of DDL forwarding.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")

    with endpoint.cursor() as cur:
        # ROLESPEC_CSTRING
        cur.execute("ALTER ROLE cloud_admin WITH PASSWORD 'york'")
        # ROLESPEC_CURRENT_USER
        cur.execute("ALTER ROLE current_user WITH PASSWORD 'pork'")
        # ROLESPEC_CURRENT_ROLE
        cur.execute("ALTER ROLE current_role WITH PASSWORD 'cork'")
        # ROLESPEC_SESSION_USER
        cur.execute("ALTER ROLE session_user WITH PASSWORD 'bork'")
        # ROLESPEC_PUBLIC
        with pytest.raises(UndefinedObject):
            cur.execute("ALTER ROLE public WITH PASSWORD 'dork'")
