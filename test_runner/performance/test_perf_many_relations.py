import os
from pathlib import Path

import pytest
from fixtures.compare_fixtures import RemoteCompare
from fixtures.log_helper import log


def get_num_relations(default: int = 1000) -> list[int]:
    # We parametrize each run with scale specifying the number of wanted child partitions.
    # Databases are pre-created and passed through BENCHMARK_CONNSTR env variable.
    scales = os.getenv("TEST_NUM_RELATIONS", default=str(default))
    rv = []
    for s in scales.split(","):
        scale = int(s)
        rv.append(scale)
    return rv


@pytest.mark.parametrize("num_relations", get_num_relations())
@pytest.mark.remote_cluster
def test_perf_many_relations(remote_compare: RemoteCompare, num_relations: int):
    """
    Test creating many relations in a single database.
    We use partitioned tables with child tables, indexes and constraints to have a realistic schema.
    Also we include some common data types like text, uuid, timestamp, JSONB, etc.

    see many_relations/create_many_relations.sql
    """
    env = remote_compare

    # prepare some base tables and the plpgsql procedures that we use to create the tables
    sql_file = Path(__file__).parent / "many_relations" / "create_many_relations.sql"
    env.pg_bin.run_capture(["psql", env.pg.connstr(), "-f", str(sql_file)])

    num_parent_tables = num_relations // 500 + 1
    log.info(f"Creating {num_relations} relations in {num_parent_tables} parent tables")

    log.info(f"Creating {num_parent_tables} parent tables")
    sql = f"CALL create_partitioned_tables('operations_scale_{num_relations}', {num_parent_tables})"
    log.info(sql)
    env.pg_bin.run_capture(["psql", env.pg.connstr(), "-c", sql])

    current_table = 0
    num_relations_remaining = num_relations

    # now run and measure the actual relation creation
    while num_relations_remaining > 0:
        current_table += 1
        parent_table_name = f"operations_scale_{num_relations}_{current_table}"
        if num_relations_remaining > 500:
            num_relations_to_create = 500
        else:
            num_relations_to_create = num_relations_remaining
        num_relations_remaining -= num_relations_to_create
        log.info(
            f"Creating {num_relations_to_create} child tables in partitioned parent table '{parent_table_name}'"
        )
        sql = f"CALL create_operations_partitions( '{parent_table_name}', '2000-01-01', ('2000-01-01'::DATE + INTERVAL '1 day' * {num_relations_to_create})::DATE)"
        log.info(sql)
        with env.zenbenchmark.record_duration(
            f"CREATE_TABLE/{current_table}/{num_relations_to_create}"
        ):
            env.pg_bin.run_capture(
                ["psql", env.pg.connstr(options="-cstatement_timeout=1000s "), "-c", sql]
            )
