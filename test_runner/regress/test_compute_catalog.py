from __future__ import annotations

import logging

import requests
from fixtures.neon_fixtures import NeonEnv, logical_replication_sync

TEST_DB_NAMES = [
    {
        "name": "neondb",
        "owner": "cloud_admin",
    },
    {
        "name": "db with spaces",
        "owner": "cloud_admin",
    },
    {
        "name": "db with%20spaces ",
        "owner": "cloud_admin",
    },
    {
        "name": "db with whitespaces	",
        "owner": "cloud_admin",
    },
    {
        "name": "injective db with spaces'; SELECT pg_sleep(10);",
        "owner": "cloud_admin",
    },
    {
        "name": "db with #pound-sign and &ampersands=true",
        "owner": "cloud_admin",
    },
    {
        "name": "db with emoji 🌍",
        "owner": "cloud_admin",
    },
]


def test_compute_catalog(neon_simple_env: NeonEnv):
    """
    Create a bunch of databases with tricky names and test that we can list them
    and dump via API.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")

    # Update the spec.json file to include new databases
    # and reconfigure the endpoint to create some test databases.
    endpoint.respec_deep(
        **{
            "skip_pg_catalog_updates": False,
            "cluster": {
                "databases": TEST_DB_NAMES,
            },
        }
    )
    endpoint.reconfigure()

    client = endpoint.http_client()
    objects = client.dbs_and_roles()

    # Assert that 'cloud_admin' role exists in the 'roles' list
    assert any(
        role["name"] == "cloud_admin" for role in objects["roles"]
    ), "The 'cloud_admin' role is missing"

    # Assert that 'postgres' database exists in the 'databases' list
    assert any(
        db["name"] == "postgres" for db in objects["databases"]
    ), "The 'postgres' database is missing"

    # Check other databases
    for test_db in TEST_DB_NAMES:
        db = next((db for db in objects["databases"] if db["name"] == test_db["name"]), None)
        assert db is not None, f"The '{test_db['name']}' database is missing"
        assert (
            db["owner"] == test_db["owner"]
        ), f"The '{test_db['name']}' database has incorrect owner"

        ddl = client.database_schema(database=test_db["name"])

        # Check that it looks like a valid PostgreSQL dump
        assert "-- PostgreSQL database dump" in ddl

        # Check that it doesn't contain health_check and migration traces.
        # They are only created in system `postgres` database, so by checking
        # that we ensure that we dump right databases.
        assert "health_check" not in ddl, f"The '{test_db['name']}' database contains health_check"
        assert "migration" not in ddl, f"The '{test_db['name']}' database contains migrations data"

    try:
        client.database_schema(database="nonexistentdb")
        raise AssertionError("Expected HTTPError was not raised")
    except requests.exceptions.HTTPError as e:
        assert (
            e.response.status_code == 404
        ), f"Expected 404 status code, but got {e.response.status_code}"


def test_compute_create_databases(neon_simple_env: NeonEnv):
    """
    Test that compute_ctl can create and work with databases with special
    characters (whitespaces, %, tabs, etc.) in the name.
    """
    env = neon_simple_env

    # Create and start endpoint so that neon_local put all the generated
    # stuff into the spec.json file.
    endpoint = env.endpoints.create_start("main")

    # Update the spec.json file to include new databases
    # and reconfigure the endpoint to apply the changes.
    endpoint.respec_deep(
        **{
            "skip_pg_catalog_updates": False,
            "cluster": {
                "databases": TEST_DB_NAMES,
            },
        }
    )
    endpoint.reconfigure()

    for db in TEST_DB_NAMES:
        # Check that database has a correct name in the system catalog
        with endpoint.cursor() as cursor:
            cursor.execute("SELECT datname FROM pg_database WHERE datname = %s", (db["name"],))
            catalog_db = cursor.fetchone()
            assert catalog_db is not None
            assert len(catalog_db) == 1
            assert catalog_db[0] == db["name"]

        # Check that we can connect to this database without any issues
        with endpoint.cursor(dbname=db["name"]) as cursor:
            cursor.execute("SELECT * FROM current_database()")
            curr_db = cursor.fetchone()
            assert curr_db is not None
            assert len(curr_db) == 1
            assert curr_db[0] == db["name"]


def test_dropdb_with_subscription(neon_simple_env: NeonEnv):
    """
    Test that compute_ctl can drop a database that has a logical replication subscription.
    """
    env = neon_simple_env

    # Create and start endpoint so that neon_local put all the generated
    # stuff into the spec.json file.
    endpoint = env.endpoints.create_start("main")

    TEST_DB_NAMES = [
        {
            "name": "neondb",
            "owner": "cloud_admin",
        },
        {
            "name": "subscriber_db",
            "owner": "cloud_admin",
        },
        {
            "name": "publisher_db",
            "owner": "cloud_admin",
        },
    ]

    # Update the spec.json file to create the databases
    # and reconfigure the endpoint to apply the changes.
    endpoint.respec_deep(
        **{
            "skip_pg_catalog_updates": False,
            "cluster": {
                "databases": TEST_DB_NAMES,
            },
        }
    )
    endpoint.reconfigure()

    # connect to the publisher_db and create a publication
    with endpoint.cursor(dbname="publisher_db") as cursor:
        cursor.execute("CREATE PUBLICATION mypub FOR ALL TABLES")
        cursor.execute("select pg_catalog.pg_create_logical_replication_slot('mysub', 'pgoutput');")
        cursor.execute("CREATE TABLE t(a int)")
        cursor.execute("INSERT INTO t VALUES (1)")

    # connect to the subscriber_db and create a subscription
    # Note that we need to create subscription with
    connstr = endpoint.connstr(dbname="publisher_db").replace("'", "''")
    with endpoint.cursor(dbname="subscriber_db") as cursor:
        cursor.execute("CREATE TABLE t(a int)")
        cursor.execute(
            f"CREATE SUBSCRIPTION mysub CONNECTION '{connstr}' PUBLICATION mypub  WITH (create_slot = false) "
        )

    # wait for the subscription to be active
    logical_replication_sync(
        endpoint, endpoint, sub_dbname="subscriber_db", pub_dbname="publisher_db"
    )

    # Check that replication is working
    with endpoint.cursor(dbname="subscriber_db") as cursor:
        cursor.execute("SELECT * FROM t")
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 1

    # drop the subscriber_db from the list
    TEST_DB_NAMES_NEW = [
        {
            "name": "neondb",
            "owner": "cloud_admin",
        },
        {
            "name": "publisher_db",
            "owner": "cloud_admin",
        },
    ]
    # Update the spec.json file to drop the database
    # and reconfigure the endpoint to apply the changes.
    endpoint.respec_deep(
        **{
            "skip_pg_catalog_updates": False,
            "cluster": {
                "databases": TEST_DB_NAMES_NEW,
            },
            "delta_operations": [
                {"action": "delete_db", "name": "subscriber_db"},
                # also test the case when we try to delete a non-existent database
                # shouldn't happen in normal operation,
                # but can occur when failed operations are retried
                {"action": "delete_db", "name": "nonexistent_db"},
            ],
        }
    )

    logging.info("Reconfiguring the endpoint to drop the subscriber_db")
    endpoint.reconfigure()

    # Check that the subscriber_db is dropped
    with endpoint.cursor() as cursor:
        cursor.execute("SELECT datname FROM pg_database WHERE datname = %s", ("subscriber_db",))
        catalog_db = cursor.fetchone()
        assert catalog_db is None

    # Check that we can still connect to the publisher_db
    with endpoint.cursor(dbname="publisher_db") as cursor:
        cursor.execute("SELECT * FROM current_database()")
        curr_db = cursor.fetchone()
        assert curr_db is not None
        assert len(curr_db) == 1
        assert curr_db[0] == "publisher_db"


def test_compute_drop_role(neon_simple_env: NeonEnv):
    """
    Test that compute_ctl can drop a role even if it has some depending objects
    like permissions in one of the databases.
    Reproduction test for https://github.com/neondatabase/cloud/issues/13582
    """
    env = neon_simple_env
    TEST_DB_NAME = "db_with_permissions"

    endpoint = env.endpoints.create_start("main")

    endpoint.respec_deep(
        **{
            "skip_pg_catalog_updates": False,
            "cluster": {
                "roles": [
                    {
                        # We need to create role via compute_ctl, because in this case it will receive
                        # additional grants equivalent to our real environment, so we can repro some
                        # issues.
                        "name": "neon",
                        # Some autocomplete-suggested hash, no specific meaning.
                        "encrypted_password": "SCRAM-SHA-256$4096:hBT22QjqpydQWqEulorfXA==$miBogcoj68JWYdsNB5PW1X6PjSLBEcNuctuhtGkb4PY=:hxk2gxkwxGo6P7GCtfpMlhA9zwHvPMsCz+NQf2HfvWk=",
                        "options": [],
                    },
                ],
                "databases": [
                    {
                        "name": TEST_DB_NAME,
                        "owner": "neon",
                    },
                ],
            },
        }
    )
    endpoint.reconfigure()

    with endpoint.cursor(dbname=TEST_DB_NAME) as cursor:
        # Create table and view as `cloud_admin`. This is the case when, for example,
        # PostGIS extensions creates tables in `public` schema.
        cursor.execute("create table test_table (id int)")
        cursor.execute("create view test_view as select * from test_table")

    with endpoint.cursor(dbname=TEST_DB_NAME, user="neon") as cursor:
        cursor.execute("create role readonly")
        # We (`compute_ctl`) make 'neon' the owner of schema 'public' in the owned database.
        # Postgres has all sorts of permissions and grants that we may not handle well,
        # but this is the shortest repro grant for the issue
        # https://github.com/neondatabase/cloud/issues/13582
        cursor.execute("grant select on all tables in schema public to readonly")

    # Check that role was created
    with endpoint.cursor() as cursor:
        cursor.execute("SELECT rolname FROM pg_roles WHERE rolname = 'readonly'")
        role = cursor.fetchone()
        assert role is not None

    # Confirm that we actually have some permissions for 'readonly' role
    # that may block our ability to drop the role.
    with endpoint.cursor(dbname=TEST_DB_NAME) as cursor:
        cursor.execute(
            "select grantor from information_schema.role_table_grants where grantee = 'readonly'"
        )
        res = cursor.fetchall()
        assert len(res) == 2, f"Expected 2 table grants, got {len(res)}"
        for row in res:
            assert row[0] == "neon_superuser"

    # Drop role via compute_ctl
    endpoint.respec_deep(
        **{
            "skip_pg_catalog_updates": False,
            "delta_operations": [
                {
                    "action": "delete_role",
                    "name": "readonly",
                },
            ],
        }
    )
    endpoint.reconfigure()

    # Check that role is dropped
    with endpoint.cursor() as cursor:
        cursor.execute("SELECT rolname FROM pg_roles WHERE rolname = 'readonly'")
        role = cursor.fetchone()
        assert role is None

    #
    # Drop schema 'public' and check that we can still drop the role
    #
    with endpoint.cursor(dbname=TEST_DB_NAME) as cursor:
        cursor.execute("create role readonly2")
        cursor.execute("grant select on all tables in schema public to readonly2")
        cursor.execute("drop schema public cascade")

    endpoint.respec_deep(
        **{
            "skip_pg_catalog_updates": False,
            "delta_operations": [
                {
                    "action": "delete_role",
                    "name": "readonly2",
                },
            ],
        }
    )
    endpoint.reconfigure()

    with endpoint.cursor() as cursor:
        cursor.execute("SELECT rolname FROM pg_roles WHERE rolname = 'readonly2'")
        role = cursor.fetchone()
        assert role is None
