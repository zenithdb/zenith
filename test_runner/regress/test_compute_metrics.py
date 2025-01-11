from __future__ import annotations

import enum
import os
import shutil
import sys
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
import requests
import yaml
from fixtures.log_helper import log
from fixtures.paths import BASE_DIR, COMPUTE_CONFIG_DIR

if TYPE_CHECKING:
    from types import TracebackType
    from typing import Self, TypedDict

    from fixtures.neon_fixtures import NeonEnv
    from fixtures.pg_version import PgVersion
    from fixtures.port_distributor import PortDistributor

    class Metric(TypedDict):
        metric_name: str
        type: str
        help: str
        key_labels: list[str] | None
        values: list[str] | None
        query: str | None
        query_ref: str | None

    class Collector(TypedDict):
        collector_name: str
        metrics: list[Metric]
        queries: list[Query] | None

    class Query(TypedDict):
        query_name: str
        query: str


JSONNET_IMPORT_CACHE: dict[str, bytes] = {}
JSONNET_PATH: list[Path] = [BASE_DIR / "compute" / "jsonnet", COMPUTE_CONFIG_DIR]


def __import_callback(dir: str, rel: str) -> tuple[str, bytes]:
    """
    dir: The directory of the Jsonnet file which tried to import a file
    rel: The actual import path from Jsonnet
    """
    if not rel:
        raise RuntimeError("Empty filename")

    full_path: str | None = None
    if os.path.isabs(rel):
        full_path = rel
    else:
        for p in (dir, *JSONNET_PATH):
            assert isinstance(p, str | Path), "for mypy"
            full_path = os.path.join(p, rel)

            assert isinstance(full_path, str), "for mypy"
            if not os.path.exists(full_path):
                full_path = None
                continue

            break

        if not full_path:
            raise RuntimeError(f"Could not resolve import ({rel}) in {dir}")

    if os.path.isdir(full_path):
        raise RuntimeError(f"Attempted to import directory: {full_path}")

    if full_path not in JSONNET_IMPORT_CACHE:
        with open(full_path, encoding="utf-8") as f:
            JSONNET_IMPORT_CACHE[full_path] = f.read().encode()

    return full_path, JSONNET_IMPORT_CACHE[full_path]


def jsonnet_evaluate_file(
    jsonnet_file: str | Path,
    ext_vars: str | dict[str, str] | None = None,
    tla_vars: str | dict[str, str] | None = None,
) -> str:
    # Jsonnet doesn't support Python 3.13 yet
    # Docs are available at https://jsonnet.org/ref/bindings.html#python_api
    import _jsonnet

    return cast(
        "str",
        _jsonnet.evaluate_file(
            str(jsonnet_file),
            ext_vars=ext_vars,
            tla_vars=tla_vars,
            import_callback=__import_callback,
        ),
    )


def evaluate_collector(jsonnet_file: Path, pg_version: PgVersion) -> str:
    return jsonnet_evaluate_file(jsonnet_file, ext_vars={"pg_version": str(pg_version)})


def evaluate_config(
    jsonnet_file: Path, collector_name: str, collector_file: str | Path, connstr: str
) -> str:
    return jsonnet_evaluate_file(
        jsonnet_file,
        tla_vars={
            "collector_name": collector_name,
            "collector_file": str(collector_file),
            "connection_string": connstr,
        },
    )


@enum.unique
class SqlExporterProcess(StrEnum):
    COMPUTE = "compute"
    AUTOSCALING = "autoscaling"


@pytest.mark.xfail(sys.version_info >= (3, 13), reason="Jsonnet doesn't support Python 3.13 yet")
@pytest.mark.parametrize(
    "collector_name",
    ["neon_collector", "neon_collector_autoscaling"],
    ids=[SqlExporterProcess.COMPUTE, SqlExporterProcess.AUTOSCALING],
)
def test_sql_exporter_metrics_smoke(
    pg_version: PgVersion,
    neon_simple_env: NeonEnv,
    compute_config_dir: Path,
    collector_name: str,
):
    """
    This is a smoke test to ensure the metrics SQL queries for sql_exporter
    work without errors.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create("main")
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    # Extract all the SQL queries from the sql_exporter config files, and run
    # them.
    collector = cast(
        "Collector",
        yaml.safe_load(
            jsonnet_evaluate_file(
                str(compute_config_dir / f"{collector_name}.jsonnet"),
                ext_vars={"pg_version": pg_version},
            )
        ),
    )

    for metric in collector["metrics"]:
        query = metric.get("query")
        if query is not None:
            log.info("Checking query for metric %s in %s", metric["metric_name"], collector_name)
            endpoint.safe_psql(query)

    queries = collector.get("queries")
    if queries is not None:
        # This variable is named q because mypy is too silly to understand it is
        # different from the query above.
        #
        # query: Optional[str]
        # q: Metric
        for q in queries:
            log.info("Checking query %s in %s", q["query_name"], collector_name)
            endpoint.safe_psql(q["query"])


class SqlExporterRunner:
    def __init__(self, test_output_dir: Path, sql_exporter_port: int) -> None:
        self._log_file_name = test_output_dir / "sql_exporter.stderr"
        self._sql_exporter_port = sql_exporter_port

        log.info(f"Starting sql_exporter at http://localhost:{self._sql_exporter_port}")

    def start(self) -> None:
        raise NotImplementedError()

    def stop(self) -> None:
        raise NotImplementedError()

    def __enter__(self) -> Self:
        self.start()

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ):
        self.stop()


SQL_EXPORTER = shutil.which("sql_exporter")

if SQL_EXPORTER is None:
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs
    from typing_extensions import override

    class SqlExporterContainer(DockerContainer):  # type: ignore
        def __init__(
            self, logs_dir: Path, config_file: Path, collector_file: Path, port: int
        ) -> None:
            # NOTE: Keep the version the same as in
            # compute/Dockerfile.compute-node and Dockerfile.build-tools.
            #
            # The "host" network mode allows sql_exporter to talk to the
            # endpoint which is running on the host.
            super().__init__("docker.io/burningalchemist/sql_exporter:0.17.0", network_mode="host")

            self.__logs_dir = logs_dir
            self.__port = port

            config_file_name = config_file.name
            collector_file_name = collector_file.name

            self.with_command(f"-config.file=/etc/{config_file_name} -web.listen-address=:{port}")

            container_config_file = f"/etc/{config_file_name}"
            container_collector_file = f"/etc/{collector_file_name}"
            log.info(
                "Mapping %s to %s in sql_exporter container", config_file, container_config_file
            )
            log.info(
                "Mapping %s to %s in sql_exporter container",
                collector_file,
                container_collector_file,
            )

            # NOTE: z allows Podman to work with SELinux. Please don't change it.
            # Ideally this would be a ro (read-only) mount, but I couldn't seem to
            # get it to work.
            self.with_volume_mapping(str(config_file), container_config_file, "z")
            self.with_volume_mapping(str(collector_file), container_collector_file, "z")

        def start(self) -> Self:
            super().start()

            log.info("Waiting for sql_exporter to be ready")
            wait_for_logs(
                self,
                rf'msg="Listening on" address=\[::\]:{self.__port}',
                timeout=5,
            )

            return self

    class SqlExporterContainerRunner(SqlExporterRunner):
        def __init__(
            self,
            test_output_dir: Path,
            config_file: Path,
            collector_file: Path,
            sql_exporter_port: int,
        ) -> None:
            super().__init__(test_output_dir, sql_exporter_port)

            self.__container = SqlExporterContainer(
                test_output_dir, config_file, collector_file, sql_exporter_port
            )

        @override
        def start(self) -> None:
            self.__container.start()

        @override
        def stop(self) -> None:
            try:
                # sql_exporter doesn't print anything to stdout
                with open(self._log_file_name, "w", encoding="utf-8") as f:
                    f.write(self.__container.get_logs()[1].decode())
            except Exception:
                log.exception("Failed to write sql_exporter logs")

            # Stop the container *after* getting the logs
            self.__container.stop()

else:
    import subprocess
    import time
    from signal import Signals

    from typing_extensions import override

    if TYPE_CHECKING:
        from collections.abc import Mapping

    class SqlExporterNativeRunner(SqlExporterRunner):
        def __init__(
            self,
            test_output_dir: Path,
            config_file: Path,
            collector_file: Path,
            sql_exporter_port: int,
        ) -> None:
            super().__init__(test_output_dir, sql_exporter_port)

            self.__config_file = config_file
            self.__collector_file = collector_file
            self.__proc: subprocess.Popen[str]

        @override
        def start(self) -> None:
            assert SQL_EXPORTER is not None

            log_file = open(self._log_file_name, "w", encoding="utf-8")
            self.__proc = subprocess.Popen(
                [
                    os.path.realpath(SQL_EXPORTER),
                    f"-config.file={self.__config_file}",
                    f"-web.listen-address=:{self._sql_exporter_port}",
                ],
                # If PGSERVICEFILE is set, sql_exporter won't launch.
                env=cast("Mapping[str, str]", {}),
                stderr=log_file,
                bufsize=0,
                text=True,
            )

            log.info("Waiting for sql_exporter to be ready")

            with open(self._log_file_name, encoding="utf-8") as f:
                started = time.time()
                while True:
                    if time.time() - started > 5:
                        self.__proc.kill()
                        raise RuntimeError("sql_exporter did not start up properly")

                    line = f.readline()
                    if not line:
                        time.sleep(0.5)
                        continue

                    if f'msg="Listening on" address=[::]:{self._sql_exporter_port}' in line:
                        break

        @override
        def stop(self) -> None:
            self.__proc.send_signal(Signals.SIGINT)
            self.__proc.wait()


@pytest.mark.xfail(sys.version_info >= (3, 13), reason="Jsonnet doesn't support Python 3.13 yet")
@pytest.mark.parametrize(
    "exporter",
    [SqlExporterProcess.COMPUTE, SqlExporterProcess.AUTOSCALING],
)
def test_sql_exporter_metrics_e2e(
    pg_version: PgVersion,
    neon_simple_env: NeonEnv,
    test_output_dir: Path,
    compute_config_dir: Path,
    exporter: SqlExporterProcess,
    port_distributor: PortDistributor,
):
    """
    This is a full E2E test of the sql_exporter setup to make sure it works
    without error.

    If you use Podman instead of Docker, you may run into issues. If you run
    rootful Podman, you may need to add a ~/.testcontainers.properties file
    with the following content:

        ryuk.container.privileged=true

    If you are not running rootful Podman, set the following environment
    variable:

        TESTCONTAINERS_RYUK_DISABLED=true

    Note that you will need the Podman socket to be running. On a systemd-based
    system, that command will look something like:

        # Use `enable --now` to start the socket on login and immediately.
        systemctl --user start podman.socket

    Whether you use the user service manager or the system service manager is
    up to you, but may have implications on the above ryuk related steps. Note
    that you may also need the docker(1) Podman frontend. I am unsure if the
    docker Python package supports Podman natively.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create("main")
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    if exporter == SqlExporterProcess.COMPUTE:
        stem_suffix = ""
    elif exporter == SqlExporterProcess.AUTOSCALING:
        stem_suffix = "_autoscaling"

    # Write the collector file
    collector_file = test_output_dir / f"neon_collector{stem_suffix}.yml"
    with open(collector_file, "w", encoding="utf-8") as o:
        collector = evaluate_collector(
            compute_config_dir / f"neon_collector{stem_suffix}.jsonnet", pg_version
        )
        o.write(collector)

    conn_options = endpoint.conn_options()
    pg_host = conn_options["host"]
    pg_port = conn_options["port"]
    pg_user = conn_options["user"]
    pg_dbname = conn_options["dbname"]
    pg_application_name = f"sql_exporter{stem_suffix}"
    connstr = f"postgresql://{pg_user}@{pg_host}:{pg_port}/{pg_dbname}?sslmode=disable&application_name={pg_application_name}"

    def escape_go_filepath_match_characters(s: str) -> str:
        """
        Unfortunately sql_exporter doesn't use plain file paths, so we need to
        escape special characters. pytest encodes the parameters of a test using
        [ and ], so we need to escape them with backslashes.
        See https://pkg.go.dev/path/filepath#Match.
        """
        return s.replace("[", r"\[").replace("]", r"\]")

    # Write the config file
    config_file = test_output_dir / f"sql_exporter{stem_suffix}.yml"
    with open(config_file, "w", encoding="utf-8") as o:
        config = evaluate_config(
            compute_config_dir / "sql_exporter.jsonnet",
            collector_name=collector_file.stem,
            collector_file=escape_go_filepath_match_characters(str(collector_file))
            if SQL_EXPORTER
            else collector_file.name,
            connstr=connstr,
        )
        o.write(config)

    sql_exporter_port = port_distributor.get_port()
    with (SqlExporterNativeRunner if SQL_EXPORTER else SqlExporterContainerRunner)(
        test_output_dir, config_file, collector_file, sql_exporter_port
    ) as _runner:
        resp = requests.get(f"http://localhost:{sql_exporter_port}/metrics")
        resp.raise_for_status()


def test_perf_counters(neon_simple_env: NeonEnv):
    """
    Test compute metrics, exposed in the neon_backend_perf_counters and
    neon_perf_counters views
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()

    # We don't check that the values make sense, this is just a very
    # basic check that the server doesn't crash or something like that.
    #
    # 1.5 is the minimum version to contain these views.
    cur.execute("CREATE EXTENSION neon VERSION '1.5'")
    cur.execute("SELECT * FROM neon_perf_counters")
    cur.execute("SELECT * FROM neon_backend_perf_counters")
