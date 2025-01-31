import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from fixtures.log_helper import log
from fixtures.neon_cli import AbstractNeonCli
from fixtures.pg_version import PgVersion


class FastImport(AbstractNeonCli):
    COMMAND = "fast_import"
    cmd: subprocess.CompletedProcess[str] | None = None

    def __init__(
        self,
        extra_env: dict[str, str] | None,
        binpath: Path,
        pg_distrib_dir: Path,
        pg_version: PgVersion,
        workdir: Path,
    ):
        if extra_env is None:
            env_vars = {}
        else:
            env_vars = extra_env.copy()

        if not (binpath / self.COMMAND).exists():
            raise Exception(f"{self.COMMAND} binary not found at '{binpath}'")
        super().__init__(env_vars, binpath)

        pg_dir = pg_distrib_dir / pg_version.v_prefixed
        self.pg_distrib_dir = pg_distrib_dir
        self.pg_version = pg_version
        self.pg_bin = pg_dir / "bin"
        if not (self.pg_bin / "postgres").exists():
            raise Exception(f"postgres binary was not found at '{self.pg_bin}'")
        self.pg_lib = pg_dir / "lib"
        if env_vars.get("LD_LIBRARY_PATH") is not None:
            self.pg_lib = Path(env_vars["LD_LIBRARY_PATH"])
        elif os.getenv("LD_LIBRARY_PATH") is not None:
            self.pg_lib = Path(str(os.getenv("LD_LIBRARY_PATH")))
        if not workdir.exists():
            raise Exception(f"Working directory '{workdir}' does not exist")
        self.workdir = workdir

    def run(
        self,
        pg_port: int | None = None,
        source_connection_string: str | None = None,
        restore_connection_string: str | None = None,
        s3prefix: str | None = None,
        interactive: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        if self.cmd is not None:
            raise Exception("Command already executed")
        args = [
            f"--pg-bin-dir={self.pg_bin}",
            f"--pg-lib-dir={self.pg_lib}",
            f"--working-directory={self.workdir}",
        ]
        if pg_port is not None:
            args.append(f"--pg-port={pg_port}")
        if source_connection_string is not None:
            args.append(f"--source-connection-string={source_connection_string}")
        if restore_connection_string is not None:
            args.append(f"--restore-connection-string={restore_connection_string}")
        if s3prefix is not None:
            args.append(f"--s3-prefix={s3prefix}")
        if interactive:
            args.append("--interactive")

        self.cmd = self.raw_cli(args)
        return self.cmd

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.workdir.exists():
            shutil.rmtree(self.workdir)


@pytest.fixture(scope="function")
def fast_import(
    pg_version: PgVersion,
    test_output_dir: Path,
    neon_binpath: Path,
    pg_distrib_dir: Path,
) -> Iterator[FastImport]:
    workdir = Path(tempfile.mkdtemp())
    with FastImport(None, neon_binpath, pg_distrib_dir, pg_version, workdir) as fi:
        yield fi

        if fi.cmd is None:
            return

        # dump stdout & stderr into test log dir
        with open(test_output_dir / "fast_import.stdout", "w") as f:
            f.write(fi.cmd.stdout)
        with open(test_output_dir / "fast_import.stderr", "w") as f:
            f.write(fi.cmd.stderr)

        log.info("Written logs to %s", test_output_dir)
