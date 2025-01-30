from __future__ import annotations

import asyncio
import json
import subprocess
import time
import urllib.parse
from contextlib import closing
from typing import TYPE_CHECKING

import psycopg2
import pytest
import requests
from fixtures.neon_fixtures import PSQL, NeonProxy, VanillaPostgres

if TYPE_CHECKING:
    from typing import Any


GET_CONNECTION_PID_QUERY = "SELECT pid FROM pg_stat_activity WHERE state = 'active'"


@pytest.mark.asyncio
async def test_http_pool_begin_1(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user http_auth with password 'http' superuser")

    def query(*args) -> Any:
        static_proxy.http_query(
            "SELECT pg_sleep(10);",
            args,
            user="http_auth",
            password="http",
            expected_code=200,
        )

    query()
    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(None, query) for _ in range(10)]
    # Wait for all the tasks to complete
    completed, pending = await asyncio.wait(tasks)
    # Get the results
    results = [task.result() for task in completed]
    print(results)


def test_proxy_select_1(static_proxy: NeonProxy, vanilla_pg: VanillaPostgres):
    """
    A simplest smoke test: check proxy against a local postgres instance.
    """

    # Establish the default compute for this endpoint
    vanilla_pg.safe_psql(
        "INSERT INTO neon_control_plane.endpoints (endpoint_id) VALUES ('generic-project-name')"
    )

    # no SNI, deprecated `options=project` syntax (before we had several endpoint in project)
    out = static_proxy.safe_psql("select 1", sslsni=0, options="project=generic-project-name")
    assert out[0][0] == 1

    # no SNI, new `options=endpoint` syntax
    out = static_proxy.safe_psql("select 1", sslsni=0, options="endpoint=generic-project-name")
    assert out[0][0] == 1

    # with SNI
    out = static_proxy.safe_psql("select 42", host="generic-project-name.localtest.me")
    assert out[0][0] == 42


def test_password_hack(static_proxy: NeonProxy, vanilla_pg: VanillaPostgres):
    """
    Check the PasswordHack auth flow: an alternative to SCRAM auth for
    clients which can't provide the project/endpoint name via SNI or `options`.
    """

    # Establish the default compute for this endpoint
    vanilla_pg.safe_psql(
        "INSERT INTO neon_control_plane.endpoints (endpoint_id) VALUES ('irrelevant')"
    )

    user = "borat"
    password = "password"
    static_proxy.safe_psql(f"create role {user} with login password '{password}'")

    # Note the format of `magic`!
    magic = f"project=irrelevant;{password}"
    out = static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)
    assert out[0][0] == 1

    magic = f"endpoint=irrelevant;{password}"
    out = static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)
    assert out[0][0] == 1

    # Must also check that invalid magic won't be accepted.
    with pytest.raises(psycopg2.OperationalError):
        magic = "broken"
        static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)


@pytest.mark.asyncio
async def test_link_auth(vanilla_pg: VanillaPostgres, link_proxy: NeonProxy):
    """
    Check the Link auth flow: a lightweight auth method which delegates
    all necessary checks to the console by sending client an auth URL.
    """

    psql = await PSQL(host=link_proxy.host, port=link_proxy.proxy_port).run("select 42")

    base_uri = link_proxy.link_auth_uri
    link = await NeonProxy.find_auth_link(base_uri, psql)

    psql_session_id = NeonProxy.get_session_id(base_uri, link)
    await NeonProxy.activate_link_auth(vanilla_pg, link_proxy, psql_session_id)

    assert psql.stdout is not None
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "42"


@pytest.mark.parametrize("option_name", ["project", "endpoint"])
def test_proxy_options(static_proxy: NeonProxy, vanilla_pg: VanillaPostgres, option_name: str):
    """
    Check that we pass extra `options` to the PostgreSQL server:
    * `project=...` and `endpoint=...` shouldn't be passed at all
    * (otherwise postgres will raise an error).
    * everything else should be passed as-is.
    """

    # Establish the default compute for this endpoint
    vanilla_pg.safe_psql(
        "INSERT INTO neon_control_plane.endpoints (endpoint_id) VALUES ('irrelevant')"
    )

    options = f"{option_name}=irrelevant -cproxytest.option=value"
    out = static_proxy.safe_psql("show proxytest.option", options=options, sslsni=0)
    assert out[0][0] == "value"

    options = f"-c proxytest.foo=\\ str {option_name}=irrelevant"
    out = static_proxy.safe_psql("show proxytest.foo", options=options, sslsni=0)
    assert out[0][0] == " str"

    options = "-cproxytest.option=value"
    out = static_proxy.safe_psql("show proxytest.option", options=options)
    assert out[0][0] == "value"

    options = "-c proxytest.foo=\\ str"
    out = static_proxy.safe_psql("show proxytest.foo", options=options)
    assert out[0][0] == " str"


@pytest.mark.asyncio
async def test_proxy_arbitrary_params(static_proxy: NeonProxy):
    with closing(
        await static_proxy.connect_async(server_settings={"IntervalStyle": "iso_8601"})
    ) as conn:
        out = await conn.fetchval("select to_json('0 seconds'::interval)")
        assert out == '"00:00:00"'

    options = "neon_proxy_params_compat:true"
    with closing(
        await static_proxy.connect_async(
            server_settings={"IntervalStyle": "iso_8601", "options": options}
        )
    ) as conn:
        out = await conn.fetchval("select to_json('0 seconds'::interval)")
        assert out == '"PT0S"'


def test_auth_errors(static_proxy: NeonProxy):
    """
    Check that we throw very specific errors in some unsuccessful auth scenarios.
    """

    # User does not exist
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio")
    text = str(exprinfo.value).strip()
    assert text.find("password authentication failed for user 'pinocchio'") != -1

    static_proxy.safe_psql(
        "create role pinocchio with login password 'magic'",
    )

    # User exists, but password is missing
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", password=None)
    text = str(exprinfo.value).strip()
    assert text.find("password authentication failed for user 'pinocchio'") != -1

    # User exists, but password is wrong
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", password="bad")
    text = str(exprinfo.value).strip()
    assert text.find("password authentication failed for user 'pinocchio'") != -1

    # Finally, check that the user can connect
    with static_proxy.connect(user="pinocchio", password="magic"):
        pass


def test_forward_params_to_client(static_proxy: NeonProxy):
    """
    Check that we forward all necessary PostgreSQL server params to client.
    """

    # A subset of parameters (GUCs) which postgres
    # sends to the client during connection setup.
    # Unfortunately, `GUC_REPORT` can't be queried.
    # Proxy *should* forward them, otherwise client library
    # might misbehave (e.g. parse timestamps incorrectly).
    reported_params_subset = [
        "client_encoding",
        "integer_datetimes",
        "is_superuser",
        "server_encoding",
        "server_version",
        "session_authorization",
        "standard_conforming_strings",
    ]

    query = """
        select name, setting
        from pg_catalog.pg_settings
        where name = any(%s)
    """

    with static_proxy.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (reported_params_subset,))
            for name, value in cur.fetchall():
                # Check that proxy has forwarded this parameter.
                assert conn.get_parameter_status(name) == value


def test_close_on_connections_exit(static_proxy: NeonProxy):
    # Open two connections, send SIGTERM, then ensure that proxy doesn't exit
    # until after connections close.
    with static_proxy.connect(), static_proxy.connect():
        static_proxy.terminate()
        with pytest.raises(subprocess.TimeoutExpired):
            static_proxy.wait_for_exit(timeout=2)
        # Ensure we don't accept any more connections
        with pytest.raises(psycopg2.OperationalError):
            static_proxy.connect()
    static_proxy.wait_for_exit()


def test_sql_over_http_serverless_driver(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http with login password 'http' superuser")

    connstr = f"postgresql://http:http@{static_proxy.domain}:{static_proxy.proxy_port}/postgres"
    response = requests.post(
        f"https://api.localtest.me:{static_proxy.external_http_port}/sql",
        data=json.dumps({"query": "select 42 as answer", "params": []}),
        headers={"Content-Type": "application/sql", "Neon-Connection-String": connstr},
        verify=str(static_proxy.test_output_dir / "proxy.crt"),
    )
    assert response.status_code == 200, response.text
    rows = response.json()["rows"]
    assert rows == [{"answer": 42}]


def test_sql_over_http(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http with login password 'http' superuser")

    def q(sql: str, params: list[Any] | None = None) -> Any:
        params = params or []
        connstr = f"postgresql://http:http@{static_proxy.domain}:{static_proxy.proxy_port}/postgres"
        response = requests.post(
            f"https://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
            data=json.dumps({"query": sql, "params": params}),
            headers={"Content-Type": "application/sql", "Neon-Connection-String": connstr},
            verify=str(static_proxy.test_output_dir / "proxy.crt"),
        )
        assert response.status_code == 200, response.text
        return response.json()

    rows = q("select 42 as answer")["rows"]
    assert rows == [{"answer": 42}]

    rows = q("select $1 as answer", [42])["rows"]
    assert rows == [{"answer": "42"}]

    rows = q("select $1 * 1 as answer", [42])["rows"]
    assert rows == [{"answer": 42}]

    rows = q("select $1::int[] as answer", [[1, 2, 3]])["rows"]
    assert rows == [{"answer": [1, 2, 3]}]

    rows = q("select $1::json->'a' as answer", [{"a": {"b": 42}}])["rows"]
    assert rows == [{"answer": {"b": 42}}]

    rows = q("select $1::jsonb[] as answer", [[{}]])["rows"]
    assert rows == [{"answer": [{}]}]

    rows = q("select $1::jsonb[] as answer", [[{"foo": 1}, {"bar": 2}]])["rows"]
    assert rows == [{"answer": [{"foo": 1}, {"bar": 2}]}]

    rows = q("select * from pg_class limit 1")["rows"]
    assert len(rows) == 1

    res = q("create table t(id serial primary key, val int)")
    assert res["command"] == "CREATE"
    assert res["rowCount"] is None

    res = q("insert into t(val) values (10), (20), (30) returning id")
    assert res["command"] == "INSERT"
    assert res["rowCount"] == 3
    assert res["rows"] == [{"id": 1}, {"id": 2}, {"id": 3}]

    res = q("select * from t")
    assert res["command"] == "SELECT"
    assert res["rowCount"] == 3

    res = q("drop table t")
    assert res["command"] == "DROP"
    assert res["rowCount"] is None


def test_sql_over_http_db_name_with_space(static_proxy: NeonProxy):
    db = "db with spaces"
    static_proxy.safe_psql_many(
        (
            f'create database "{db}"',
            "create role http with login password 'http' superuser",
        )
    )

    def q(sql: str, params: list[Any] | None = None) -> Any:
        params = params or []
        connstr = f"postgresql://http:http@{static_proxy.domain}:{static_proxy.proxy_port}/{urllib.parse.quote(db)}"
        response = requests.post(
            f"https://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
            data=json.dumps({"query": sql, "params": params}),
            headers={"Content-Type": "application/sql", "Neon-Connection-String": connstr},
            verify=str(static_proxy.test_output_dir / "proxy.crt"),
        )
        assert response.status_code == 200, response.text
        return response.json()

    rows = q("select 42 as answer")["rows"]
    assert rows == [{"answer": 42}]


def test_sql_over_http_output_options(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http2 with login password 'http2' superuser")

    def q(sql: str, raw_text: bool, array_mode: bool, params: list[Any] | None = None) -> Any:
        params = params or []
        connstr = (
            f"postgresql://http2:http2@{static_proxy.domain}:{static_proxy.proxy_port}/postgres"
        )
        response = requests.post(
            f"https://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
            data=json.dumps({"query": sql, "params": params}),
            headers={
                "Content-Type": "application/sql",
                "Neon-Connection-String": connstr,
                "Neon-Raw-Text-Output": "true" if raw_text else "false",
                "Neon-Array-Mode": "true" if array_mode else "false",
            },
            verify=str(static_proxy.test_output_dir / "proxy.crt"),
        )
        assert response.status_code == 200
        return response.json()

    rows = q("select 1 as n, 'a' as s, '{1,2,3}'::int4[] as arr", False, False)["rows"]
    assert rows == [{"arr": [1, 2, 3], "n": 1, "s": "a"}]

    rows = q("select 1 as n, 'a' as s, '{1,2,3}'::int4[] as arr", False, True)["rows"]
    assert rows == [[1, "a", [1, 2, 3]]]

    rows = q("select 1 as n, 'a' as s, '{1,2,3}'::int4[] as arr", True, False)["rows"]
    assert rows == [{"arr": "{1,2,3}", "n": "1", "s": "a"}]

    rows = q("select 1 as n, 'a' as s, '{1,2,3}'::int4[] as arr", True, True)["rows"]
    assert rows == [["1", "a", "{1,2,3}"]]


def test_sql_over_http_batch(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http with login password 'http' superuser")

    def qq(
        queries: list[tuple[str, list[Any] | None]],
        read_only: bool = False,
        deferrable: bool = False,
    ) -> Any:
        connstr = f"postgresql://http:http@{static_proxy.domain}:{static_proxy.proxy_port}/postgres"
        response = requests.post(
            f"https://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
            data=json.dumps(
                {"queries": list(map(lambda x: {"query": x[0], "params": x[1] or []}, queries))}
            ),
            headers={
                "Content-Type": "application/sql",
                "Neon-Connection-String": connstr,
                "Neon-Batch-Isolation-Level": "Serializable",
                "Neon-Batch-Read-Only": "true" if read_only else "false",
                "Neon-Batch-Deferrable": "true" if deferrable else "false",
            },
            verify=str(static_proxy.test_output_dir / "proxy.crt"),
        )
        assert response.status_code == 200
        return response.json()["results"], response.headers

    result, headers = qq(
        [
            ("select 42 as answer", None),
            ("select $1 as answer", [42]),
            ("select $1 * 1 as answer", [42]),
            ("select $1::int[] as answer", [[1, 2, 3]]),
            ("select $1::json->'a' as answer", [{"a": {"b": 42}}]),
            ("select * from pg_class limit 1", None),
            ("create table t(id serial primary key, val int)", None),
            ("insert into t(val) values (10), (20), (30) returning id", None),
            ("select * from t", None),
            ("drop table t", None),
        ]
    )

    assert headers["Neon-Batch-Isolation-Level"] == "Serializable"
    assert "Neon-Batch-Read-Only" not in headers
    assert "Neon-Batch-Deferrable" not in headers

    assert result[0]["rows"] == [{"answer": 42}]
    assert result[1]["rows"] == [{"answer": "42"}]
    assert result[2]["rows"] == [{"answer": 42}]
    assert result[3]["rows"] == [{"answer": [1, 2, 3]}]
    assert result[4]["rows"] == [{"answer": {"b": 42}}]
    assert len(result[5]["rows"]) == 1
    res = result[6]
    assert res["command"] == "CREATE"
    assert res["rowCount"] is None
    res = result[7]
    assert res["command"] == "INSERT"
    assert res["rowCount"] == 3
    assert res["rows"] == [{"id": 1}, {"id": 2}, {"id": 3}]
    res = result[8]
    assert res["command"] == "SELECT"
    assert res["rowCount"] == 3
    res = result[9]
    assert res["command"] == "DROP"
    assert res["rowCount"] is None
    assert len(result) == 10

    result, headers = qq(
        [
            ("select 42 as answer", None),
        ],
        True,
        True,
    )
    assert headers["Neon-Batch-Isolation-Level"] == "Serializable"
    assert headers["Neon-Batch-Read-Only"] == "true"
    assert headers["Neon-Batch-Deferrable"] == "true"

    assert result[0]["rows"] == [{"answer": 42}]


def test_sql_over_http_batch_output_options(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http with login password 'http' superuser")

    connstr = f"postgresql://http:http@{static_proxy.domain}:{static_proxy.proxy_port}/postgres"
    response = requests.post(
        f"https://{static_proxy.domain}:{static_proxy.external_http_port}/sql",
        data=json.dumps(
            {
                "queries": [
                    {"query": "select $1 as answer", "params": [42], "arrayMode": True},
                    {"query": "select $1 as answer", "params": [42], "arrayMode": False},
                ]
            }
        ),
        headers={
            "Content-Type": "application/sql",
            "Neon-Connection-String": connstr,
            "Neon-Batch-Isolation-Level": "Serializable",
            "Neon-Batch-Read-Only": "false",
            "Neon-Batch-Deferrable": "false",
        },
        verify=str(static_proxy.test_output_dir / "proxy.crt"),
    )
    assert response.status_code == 200
    results = response.json()["results"]

    assert results[0]["rowAsArray"]
    assert results[0]["rows"] == [["42"]]

    assert not results[1]["rowAsArray"]
    assert results[1]["rows"] == [{"answer": "42"}]


def test_sql_over_http_pool(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user http_auth with password 'http' superuser")

    def get_pid(status: int, pw: str, user="http_auth") -> Any:
        return static_proxy.http_query(
            GET_CONNECTION_PID_QUERY,
            [],
            user=user,
            password=pw,
            expected_code=status,
        )

    pid1 = get_pid(200, "http")["rows"][0]["pid"]

    time.sleep(0.02)

    # query should be on the same connection
    rows = get_pid(200, "http")["rows"]
    assert rows == [{"pid": pid1}]

    time.sleep(0.02)

    # incorrect password should not work
    res = get_pid(400, "foobar")
    assert "password authentication failed for user" in res["message"]

    static_proxy.safe_psql("alter user http_auth with password 'http2'")

    # after password change, shouldn't open a new connection because it checks password in proxy.
    rows = get_pid(200, "http2")["rows"]
    assert rows == [{"pid": pid1}]

    time.sleep(0.02)

    # incorrect user shouldn't reveal that the user doesn't exists
    res = get_pid(400, "http", user="http_auth2")
    assert "password authentication failed for user" in res["message"]


def test_sql_over_http_urlencoding(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user \"http+auth$$\" with password '%+$^&*@!' superuser")

    static_proxy.http_query(
        "select 1",
        [],
        user="http+auth$$",
        password="%+$^&*@!",
        expected_code=200,
    )


# Beginning a transaction should not impact the next query,
# which might come from a completely different client.
def test_http_pool_begin(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user http_auth with password 'http' superuser")

    def query(status: int, query: str, *args) -> Any:
        static_proxy.http_query(
            query,
            args,
            user="http_auth",
            password="http",
            expected_code=status,
        )

    query(200, "BEGIN;")
    query(400, "garbage-lol(&(&(&(&")  # Intentional error to break the transaction
    query(200, "SELECT 1;")  # Query that should succeed regardless of the transaction


def test_sql_over_http_pool_idle(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user http_auth2 with password 'http' superuser")

    def query(status: int, query: str) -> Any:
        return static_proxy.http_query(
            query,
            [],
            user="http_auth2",
            password="http",
            expected_code=status,
        )

    pid1 = query(200, GET_CONNECTION_PID_QUERY)["rows"][0]["pid"]
    time.sleep(0.02)
    query(200, "BEGIN")
    pid2 = query(200, GET_CONNECTION_PID_QUERY)["rows"][0]["pid"]
    assert pid1 != pid2


@pytest.mark.timeout(60)
def test_sql_over_http_pool_dos(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user http_auth with password 'http' superuser")

    static_proxy.safe_psql("CREATE TYPE foo AS ENUM ('foo')")

    def query(status: int, query: str) -> Any:
        return static_proxy.http_query(
            query,
            [],
            user="http_auth",
            password="http",
            expected_code=status,
        )

    # query generates a million rows - should hit the 10MB reponse limit quickly
    response = query(
        507,
        "select * from generate_series(1, 5000) a cross join generate_series(1, 5000) b cross join (select 'foo'::foo) c;",
    )
    assert "response is too large (max is 10485760 bytes)" in response["message"]


def test_sql_over_http_pool_custom_types(static_proxy: NeonProxy):
    static_proxy.safe_psql("create user http_auth with password 'http' superuser")

    static_proxy.safe_psql("CREATE TYPE foo AS ENUM ('foo','bar','baz')")

    def query(status: int, query: str) -> Any:
        return static_proxy.http_query(
            query,
            [],
            user="http_auth",
            password="http",
            expected_code=status,
        )

    response = query(
        200,
        "select array['foo'::foo, 'bar'::foo, 'baz'::foo] as data",
    )
    assert response["rows"][0]["data"] == ["foo", "bar", "baz"]


@pytest.mark.asyncio
async def test_sql_over_http2(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http with login password 'http' superuser")

    resp = await static_proxy.http2_query(
        "select 42 as answer", [], user="http", password="http", expected_code=200
    )
    assert resp["rows"] == [{"answer": 42}]


def test_sql_over_http_connection_cancel(static_proxy: NeonProxy):
    static_proxy.safe_psql("create role http with login password 'http' superuser")

    static_proxy.safe_psql("create table test_table ( id int primary key )")

    # insert into a table, with a unique constraint, after sleeping for n seconds
    query = "WITH temp AS ( \
        SELECT pg_sleep($1) as sleep, $2::int as id \
    ) INSERT INTO test_table (id) SELECT id FROM temp"

    try:
        # The request should complete before the proxy HTTP timeout triggers.
        # Timeout and cancel the request on the client side before the query completes.
        static_proxy.http_query(
            query,
            [static_proxy.http_timeout_seconds - 1, 1],
            user="http",
            password="http",
            timeout=2,
        )
    except requests.exceptions.ReadTimeout:
        pass

    # wait until the query _would_ have been complete
    time.sleep(static_proxy.http_timeout_seconds)

    res = static_proxy.http_query(query, [1, 1], user="http", password="http", expected_code=200)
    assert res["command"] == "INSERT", "HTTP query should insert"
    assert res["rowCount"] == 1, "HTTP query should insert"

    res = static_proxy.http_query(query, [0, 1], user="http", password="http", expected_code=400)
    assert (
        "duplicate key value violates unique constraint" in res["message"]
    ), "HTTP query should conflict"
