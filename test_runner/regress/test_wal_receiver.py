from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fixtures.common_types import Lsn, TenantId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from typing import Any


# Checks that pageserver's walreceiver state is printed in the logs during WAL wait timeout.
# Ensures that walreceiver does not run without any data inserted and only starts after the insertion.
def test_pageserver_lsn_wait_error_start(neon_env_builder: NeonEnvBuilder):
    # Trigger WAL wait timeout faster
    neon_env_builder.pageserver_config_override = "wait_lsn_timeout = '1s'"
    env = neon_env_builder.init_start()
    env.pageserver.http_client()

    # In this test we force 'Timed out while waiting for WAL record error' while
    # fetching basebackup and don't want any retries.
    os.environ["NEON_COMPUTE_TESTING_BASEBACKUP_RETRIES"] = "1"

    tenant_id, timeline_id = env.create_tenant()
    expected_timeout_error = f"Timed out while waiting for WAL record at LSN {future_lsn} to arrive"
    env.pageserver.allowed_errors.append(f".*{expected_timeout_error}.*")

    try:
        trigger_wait_lsn_timeout(env, tenant_id)
    except Exception as e:
        exception_string = str(e)
        assert expected_timeout_error in exception_string, "Should time out during waiting for WAL"
        assert (
            "WalReceiver status: Not active" in exception_string
        ), "Walreceiver should not be active before any data writes"

    insert_test_elements(env, tenant_id, start=0, count=1_000)
    try:
        trigger_wait_lsn_timeout(env, tenant_id)
    except Exception as e:
        exception_string = str(e)
        assert expected_timeout_error in exception_string, "Should time out during waiting for WAL"
        assert (
            "WalReceiver status: Not active" not in exception_string
        ), "Should not be inactive anymore after INSERTs are made"
        assert "WalReceiver status" in exception_string, "But still should have some other status"


# Checks that all active safekeepers are shown in pageserver's walreceiver state printed on WAL wait timeout.
# Kills one of the safekeepers and ensures that only the active ones are printed in the state.
def test_pageserver_lsn_wait_error_safekeeper_stop(neon_env_builder: NeonEnvBuilder):
    # Trigger WAL wait timeout faster
    def customize_pageserver_toml(ps_cfg: dict[str, Any]):
        ps_cfg["wait_lsn_timeout"] = "2s"
        tenant_config = ps_cfg.setdefault("tenant_config", {})
        tenant_config["walreceiver_connect_timeout"] = "2s"
        tenant_config["lagging_wal_timeout"] = "2s"

    # In this test we force 'Timed out while waiting for WAL record error' while
    # fetching basebackup and don't want any retries.
    os.environ["NEON_COMPUTE_TESTING_BASEBACKUP_RETRIES"] = "1"
    neon_env_builder.pageserver_config_override = customize_pageserver_toml

    # Have notable SK ids to ensure we check logs for their presence, not some other random numbers
    neon_env_builder.safekeepers_id_start = 12345
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    env.pageserver.http_client()

    tenant_id, timeline_id = env.create_tenant()

    expected_timeout_error = f"Timed out while waiting for WAL record at LSN {future_lsn} to arrive"
    env.pageserver.allowed_errors.append(f".*{expected_timeout_error}.*")
    # we configure wait_lsn_timeout to a shorter value than the lagging_wal_timeout / walreceiver_connect_timeout
    # => after we run into a timeout and reconnect to a different SK, more time than wait_lsn_timeout has passed
    # ==> we log this error
    env.pageserver.allowed_errors.append(
        ".*ingesting record with timestamp lagging more than wait_lsn_timeout.*"
    )

    insert_test_elements(env, tenant_id, start=0, count=1)

    def all_sks_in_wareceiver_state():
        try:
            trigger_wait_lsn_timeout(env, tenant_id)
        except Exception as e:
            exception_string = str(e)
            assert (
                expected_timeout_error in exception_string
            ), "Should time out during waiting for WAL"

            for safekeeper in env.safekeepers:
                assert (
                    str(safekeeper.id) in exception_string
                ), f"Should have safekeeper {safekeeper.id} printed in walreceiver state after WAL wait timeout"

    wait_until(all_sks_in_wareceiver_state, timeout=30)

    stopped_safekeeper = env.safekeepers[-1]
    stopped_safekeeper_id = stopped_safekeeper.id
    log.info(f"Stopping safekeeper {stopped_safekeeper.id}")
    stopped_safekeeper.stop()

    def all_but_stopped_sks_in_wareceiver_state():
        try:
            trigger_wait_lsn_timeout(env, tenant_id)
        except Exception as e:
            # Strip out the part before stdout, as it contains full command with the list of all safekeepers
            exception_string = str(e).split("stdout", 1)[-1]
            assert (
                expected_timeout_error in exception_string
            ), "Should time out during waiting for WAL"

            for safekeeper in env.safekeepers:
                if safekeeper.id == stopped_safekeeper_id:
                    assert (
                        str(safekeeper.id) not in exception_string
                    ), f"Should not have stopped safekeeper {safekeeper.id} printed in walreceiver state after 2nd WAL wait timeout"
                else:
                    assert (
                        str(safekeeper.id) in exception_string
                    ), f"Should have safekeeper {safekeeper.id} printed in walreceiver state after 2nd WAL wait timeout"

    wait_until(all_but_stopped_sks_in_wareceiver_state, timeout=30)


def insert_test_elements(env: NeonEnv, tenant_id: TenantId, start: int, count: int):
    first_element_id = start
    last_element_id = first_element_id + count
    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS t(key serial primary key, value text)")
            cur.execute(
                f"INSERT INTO t SELECT i, CONCAT('payload_', i) FROM generate_series({first_element_id},{last_element_id}) as i"
            )


future_lsn = Lsn("0/FFFFFFFF")


def trigger_wait_lsn_timeout(env: NeonEnv, tenant_id: TenantId):
    with env.endpoints.create_start(
        "main",
        tenant_id=tenant_id,
        lsn=future_lsn,
    ) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute("SELECT 1")
