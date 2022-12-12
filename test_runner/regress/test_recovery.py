import time
from contextlib import closing

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


#
# Test pageserver recovery after crash
#
def test_pageserver_recovery(neon_env_builder: NeonEnvBuilder):
    # Override default checkpointer settings to run it more often
    neon_env_builder.pageserver_config_override = "tenant_config={checkpoint_distance = 1048576}"

    env = neon_env_builder.init()

    neon_env_builder.start()

    # These warnings are expected, when the pageserver is restarted abruptly
    env.pageserver.allowed_errors.append(".*found future delta layer.*")
    env.pageserver.allowed_errors.append(".*found future image layer.*")

    # Create a branch for us
    env.neon_cli.create_branch("test_pageserver_recovery", "main")

    pg = env.postgres.create_start("test_pageserver_recovery")
    log.info("postgres is running on 'test_pageserver_recovery' branch")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            with env.pageserver.http_client() as pageserver_http:
                # Create and initialize test table
                cur.execute("CREATE TABLE foo(x bigint)")
                cur.execute("INSERT INTO foo VALUES (generate_series(1,100000))")

                # Sleep for some time to let checkpoint create image layers
                time.sleep(2)

                # Configure failpoints
                pageserver_http.configure_failpoints(
                    [
                        ("flush-frozen-before-sync", "sleep(2000)"),
                        ("checkpoint-after-sync", "exit"),
                    ]
                )

                # Do some updates until pageserver is crashed
                try:
                    while True:
                        cur.execute("update foo set x=x+1")
                except Exception as err:
                    log.info(f"Expected server crash {err}")

    log.info("Wait before server restart")
    env.pageserver.stop()
    env.pageserver.start()

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from foo")
            assert cur.fetchone() == (100000,)
