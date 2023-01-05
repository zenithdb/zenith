from contextlib import closing

import pytest
from fixtures.neon_fixtures import NeonEnvBuilder, PageserverApiException, PgProtocol
from fixtures.types import Lsn, TenantId


def test_pageserver_auth(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()

    ps = env.pageserver

    tenant_token = env.auth_keys.generate_tenant_token(env.initial_tenant)
    tenant_http_client = env.pageserver.http_client(tenant_token)
    invalid_tenant_token = env.auth_keys.generate_tenant_token(TenantId.generate())
    invalid_tenant_http_client = env.pageserver.http_client(invalid_tenant_token)

    pageserver_token = env.auth_keys.generate_pageserver_token()
    pageserver_http_client = env.pageserver.http_client(pageserver_token)

    # this does not invoke auth check and only decodes jwt and checks it for validity
    # check both tokens
    ps.safe_psql("set FOO", password=tenant_token)
    ps.safe_psql("set FOO", password=pageserver_token)

    new_timeline_id = env.neon_cli.create_branch(
        "test_pageserver_auth", tenant_id=env.initial_tenant
    )

    # tenant can create branches
    branching_lsn = Lsn(
        tenant_http_client.timeline_detail(
            tenant_id=env.initial_tenant, timeline_id=new_timeline_id
        )["last_record_lsn"]
    )
    tenant_http_client.timeline_create(
        tenant_id=env.initial_tenant,
        ancestor_timeline_id=new_timeline_id,
        ancestor_start_lsn=branching_lsn,
    )
    # console can create branches for tenant
    pageserver_http_client.timeline_create(
        tenant_id=env.initial_tenant,
        ancestor_timeline_id=new_timeline_id,
        ancestor_start_lsn=branching_lsn,
    )

    # fail to create branch using token with different tenant_id
    with pytest.raises(
        PageserverApiException, match="Forbidden: Tenant id mismatch. Permission denied"
    ):
        invalid_tenant_http_client.timeline_create(
            tenant_id=env.initial_tenant,
            ancestor_timeline_id=new_timeline_id,
            ancestor_start_lsn=branching_lsn,
        )

    # create tenant using management token
    pageserver_http_client.tenant_create()

    # fail to create tenant using tenant token
    with pytest.raises(
        PageserverApiException,
        match="Forbidden: Attempt to access management api with tenant scope. Permission denied",
    ):
        tenant_http_client.tenant_create()


def test_compute_auth_to_pageserver(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    branch = "test_compute_auth_to_pageserver"
    env.neon_cli.create_branch(branch)
    pg = env.postgres.create_start(branch)

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute("SELECT sum(key) FROM t")
            assert cur.fetchone() == (5000050000,)


@pytest.mark.parametrize("auth_enabled", [False, True])
def test_auth_failures(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    branch = f"test_auth_failures_auth_enabled_{auth_enabled}"
    timeline_id = env.neon_cli.create_branch(branch)
    env.postgres.create_start(branch)

    tenant_token = env.auth_keys.generate_tenant_token(env.initial_tenant)
    invalid_tenant_token = env.auth_keys.generate_tenant_token(TenantId.generate())
    pageserver_token = env.auth_keys.generate_pageserver_token()
    safekeeper_token = env.auth_keys.generate_safekeeper_token()

    def check_connection(
        pg_protocol: PgProtocol, command: str, expect_success: bool, **conn_kwargs
    ):
        def op():
            with closing(pg_protocol.connect(**conn_kwargs)) as conn:
                with conn.cursor() as cur:
                    cur.execute(command)

        if expect_success:
            op()
        else:
            with pytest.raises(Exception):
                op()

    def check_pageserver(expect_success: bool, **conn_kwargs):
        check_connection(
            env.pageserver,
            f"get_last_record_rlsn {env.initial_tenant} {timeline_id}",
            expect_success,
            **conn_kwargs,
        )

    check_pageserver(not auth_enabled)
    if auth_enabled:
        check_pageserver(True, password=tenant_token)

        env.pageserver.allowed_errors.append(".*Tenant id mismatch. Permission denied.*")
        check_pageserver(False, password=invalid_tenant_token)

        check_pageserver(True, password=pageserver_token)

        env.pageserver.allowed_errors.append(
            ".*SafekeeperData scope makes no sense for Pageserver.*"
        )
        check_pageserver(False, password=safekeeper_token)

    def check_safekeeper(expect_success: bool, **conn_kwargs):
        check_connection(
            PgProtocol(
                host="localhost",
                port=env.safekeepers[0].port.pg,
                options=f"ztenantid={env.initial_tenant} ztimelineid={timeline_id}",
            ),
            "IDENTIFY_SYSTEM",
            expect_success,
            **conn_kwargs,
        )

    check_safekeeper(not auth_enabled)
    if auth_enabled:
        check_safekeeper(True, password=tenant_token)
        check_safekeeper(False, password=invalid_tenant_token)
        check_safekeeper(False, password=pageserver_token)
        check_safekeeper(True, password=safekeeper_token)
