from pathlib import Path
import os
from time import sleep
from typing import Callable, Dict
import uuid

from fixtures.zenith_fixtures import PgBin, PgProtocol, PortDistributor
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from batch_others.test_wal_acceptor import ProposerPostgres, SafekeeperEnv
from fixtures.utils import lsn_from_hex, mkdir_if_needed

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


def test_walproposer_pgbench(test_output_dir: str,
                             port_distributor: PortDistributor,
                             pg_bin: PgBin,
                             zenbenchmark: ZenithBenchmarker):
    # Create the environment in the test-specific output dir
    repo_dir = Path(os.path.join(test_output_dir, "repo"))

    env = SafekeeperEnv(
        repo_dir,
        port_distributor,
        pg_bin,
        num_safekeepers=1,
    )

    with env:
        env.init()
        assert env.postgres is not None

        def calc_flushes() -> int:
            assert env.safekeepers is not None
            assert env.tenant_id is not None
            assert env.timeline_id is not None
            metrics = env.safekeepers[0].http_client().get_metrics()
            return int(metrics.flush_wal_count[(env.tenant_id.hex, env.timeline_id.hex)])

        pgbench_env = {
            "PGHOST": env.postgres.listen_addr,
            "PGPORT": str(env.postgres.port),
            "PGDATA": env.postgres.pg_data_dir_path(),
            "PGUSER": "zenith_admin",
            "PGDATABASE": "postgres"
        }

        run_pgbench(pg_bin, zenbenchmark, pgbench_env, calc_flushes, env.postgres)


def test_vanilla_pgbench(test_output_dir: str,
                         port_distributor: PortDistributor,
                         pg_bin: PgBin,
                         zenbenchmark: ZenithBenchmarker):
    # Create the environment in the test-specific output dir
    repo_dir = os.path.join(test_output_dir, "repo")
    mkdir_if_needed(repo_dir)

    pgdata_master = os.path.join(repo_dir, "pgdata_master")
    pgdata_replica = os.path.join(repo_dir, "pgdata_replica")

    master = ProposerPostgres(pgdata_master,
                              pg_bin,
                              uuid.uuid4(),
                              uuid.uuid4(),
                              "127.0.0.1",
                              port_distributor.get_port())

    common_config = [
        "wal_keep_size=10TB\n",
        "shared_preload_libraries=zenith\n",
        "zenith.page_server_connstring=''\n",
        "synchronous_commit=on\n",
        "max_wal_senders=10\n",
        "wal_log_hints=on\n",
        "max_replication_slots=10\n",
        "hot_standby=on\n",
        "min_wal_size=20GB\n",
        "max_wal_size=40GB\n",
        "checkpoint_timeout=60min\n",
        "log_checkpoints=on\n",
        "max_connections=100\n",
        "wal_sender_timeout=0\n",
        "wal_level=replica\n",
    ]

    master.initdb()
    mkdir_if_needed(master.pg_data_dir_path())
    with open(master.config_file_path(), "w") as f:
        cfg = [
            "fsync=off\n",
            f"listen_addresses = '{master.listen_addr}'\n",
            f"port = '{master.port}'\n",
            "synchronous_standby_names = 'ANY 1 (s1)'\n",
        ] + common_config

        f.writelines(cfg)

    with open(os.path.join(pgdata_master, "pg_hba.conf"), "w") as f:
        pg_hba = """
host     all             all             0.0.0.0/0               trust
host     all             all             ::/0                    trust
local    all             all                                     trust
host     all             all        127.0.0.1/32                 trust
host     all             all        ::1/128                      trust
host      replication     all             0.0.0.0/0               trust
host      replication     all             ::/0                    trust
# Allow replication connections from localhost, by a user with the
# replication privilege.
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
"""
        f.writelines(pg_hba)

    master_env = {
        "PGHOST": master.listen_addr,
        "PGPORT": str(master.port),
        "PGDATA": master.pg_data_dir_path(),
        "PGUSER": "zenith_admin",
        "PGDATABASE": "postgres"
    }

    master.start()
    master.safe_psql("SELECT pg_create_physical_replication_slot('s1');")
    pg_bin.run(["pg_basebackup", "-D", pgdata_replica], env=master_env)

    replica = ProposerPostgres(pgdata_replica,
                               pg_bin,
                               uuid.uuid4(),
                               uuid.uuid4(),
                               "127.0.0.1",
                               port_distributor.get_port())

    with open(replica.config_file_path(), "w") as f:
        cfg = [
            "fsync=on\n",
            f"listen_addresses = '{replica.listen_addr}'\n",
            f"port = '{replica.port}'\n",
            "primary_slot_name = 's1'\n",
            f"primary_conninfo = 'application_name=s1 user=zenith_admin host={master.listen_addr} channel_binding=disable port={master.port} sslmode=disable sslcompression=0 sslsni=1 ssl_min_protocol_version=TLSv1.2 gssencmode=disable krbsrvname=postgres target_session_attrs=any'\n",
        ] + common_config

        f.writelines(cfg)

    with open(os.path.join(pgdata_replica, "standby.signal"), "w") as f:
        pass

    replica.start()

    def calc_flushes() -> int:
        return int(replica.safe_psql("SELECT wal_sync FROM pg_stat_wal")[0][0])

    run_pgbench(pg_bin, zenbenchmark, master_env, calc_flushes, master)

    replica.stop()
    master.stop()


def run_pgbench(pg_bin: PgBin,
                zenbenchmark: ZenithBenchmarker,
                pgbench_env: Dict[str, str],
                calc_flushes: Callable[[], int],
                postgres: PgProtocol,
                scale=200,
                conns_count=32,
                pgbench_time=60):

    step0_lsn = lsn_from_hex(postgres.safe_psql('select pg_current_wal_insert_lsn()')[0][0])
    step0_flush_cnt = calc_flushes()
    log.info(f"step0_lsn: {step0_lsn}")
    log.info(f"step0_flush_cnt: {step0_flush_cnt}")

    cmd = ["pgbench", "-i", "-s", str(scale)]
    basepath = pg_bin.run_capture(cmd, pgbench_env)
    pgbench_init = basepath + '.stderr'

    with open(pgbench_init, 'r') as stdout_f:
        stdout = stdout_f.readlines()
        stats_str = stdout[-1]
        log.info(stats_str)

    init_seconds = float(stats_str.split()[2])

    zenbenchmark.record("pgbench_init", init_seconds, unit="s", report=MetricReport.LOWER_IS_BETTER)

    wal_init_size = get_dir_size(os.path.join(pgbench_env["PGDATA"], 'pg_wal'))
    zenbenchmark.record('wal_init_size',
                        wal_init_size / (1024 * 1024),
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)

    step1_lsn = lsn_from_hex(postgres.safe_psql('select pg_current_wal_insert_lsn()')[0][0])
    step1_flush_cnt = calc_flushes()
    log.info(f"step1_lsn: {step1_lsn}")
    log.info(f"step1_flush_cnt: {step1_flush_cnt}")

    zenbenchmark.record("init_wal_bytes_per_fsync",
                        (step1_lsn - step0_lsn) / (step1_flush_cnt - step0_flush_cnt),
                        unit="b",
                        report=MetricReport.HIGHER_IS_BETTER)

    cmd = [
        "pgbench",
        "-c",
        str(conns_count),
        "-N",
        "-P",
        "1",
        "-T",
        str(pgbench_time),
    ]
    basepath = pg_bin.run_capture(cmd, pgbench_env)
    pgbench_run = basepath + '.stdout'

    with open(pgbench_run, 'r') as stdout_f:
        stdout = stdout_f.readlines()
        for line in stdout:
            if "number of transactions actually processed:" in line:
                transactions_processed = int(line.split()[-1])
        stats_str = stdout[-1]
        log.info(stats_str)

    pgbench_tps = float(stats_str.split()[2])

    step2_lsn = lsn_from_hex(postgres.safe_psql('select pg_current_wal_insert_lsn()')[0][0])
    step2_flush_cnt = calc_flushes()
    log.info(f"step2_lsn: {step2_lsn}")
    log.info(f"step2_flush_cnt: {step2_flush_cnt}")

    zenbenchmark.record("tps_pgbench", pgbench_tps, unit="", report=MetricReport.HIGHER_IS_BETTER)

    zenbenchmark.record("tx_wal_bytes_per_fsync",
                        (step2_lsn - step1_lsn) / (step2_flush_cnt - step1_flush_cnt),
                        unit="b",
                        report=MetricReport.HIGHER_IS_BETTER)

    zenbenchmark.record("txes_per_fsync",
                        transactions_processed / (step2_flush_cnt - step1_flush_cnt),
                        unit="",
                        report=MetricReport.HIGHER_IS_BETTER)

    wal_size = get_dir_size(os.path.join(pgbench_env["PGDATA"], 'pg_wal'))
    zenbenchmark.record('wal_size',
                        wal_size / (1024 * 1024),
                        'MB',
                        report=MetricReport.LOWER_IS_BETTER)


def get_dir_size(path: str) -> int:
    """Return size in bytes."""
    totalbytes = 0
    for root, dirs, files in os.walk(path):
        for name in files:
            totalbytes += os.path.getsize(os.path.join(root, name))

    return totalbytes
