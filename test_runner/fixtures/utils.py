from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import re
import subprocess
import tarfile
import threading
import time
from collections.abc import Callable, Iterable
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import urlencode

import allure
import pytest
import zstandard
from psycopg2.extensions import cursor
from typing_extensions import override

from fixtures.common_types import Id, Lsn
from fixtures.log_helper import log
from fixtures.pageserver.common_types import (
    parse_delta_layer,
    parse_image_layer,
)
from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import IO

    from fixtures.common_types import TimelineId
    from fixtures.neon_fixtures import PgBin

    WaitUntilRet = TypeVar("WaitUntilRet")


Fn = TypeVar("Fn", bound=Callable[..., Any])

COMPONENT_BINARIES = {
    "storage_controller": ("storage_controller",),
    "storage_broker": ("storage_broker",),
    "compute": ("compute_ctl",),
    "safekeeper": ("safekeeper",),
    "pageserver": ("pageserver", "pagectl"),
}
# Disable auto-formatting for better readability
# fmt: off
VERSIONS_COMBINATIONS = (
    {"storage_controller": "new", "storage_broker": "new", "compute": "new", "safekeeper": "new", "pageserver": "new"},
    {"storage_controller": "new", "storage_broker": "new", "compute": "old", "safekeeper": "old", "pageserver": "old"},
    {"storage_controller": "new", "storage_broker": "new", "compute": "old", "safekeeper": "old", "pageserver": "new"},
    {"storage_controller": "new", "storage_broker": "new", "compute": "old", "safekeeper": "new", "pageserver": "new"},
    {"storage_controller": "old", "storage_broker": "old", "compute": "new", "safekeeper": "new", "pageserver": "new"},
)
# fmt: on

# If the environment variable USE_LFC is set and its value is "false", then LFC is disabled for tests.
# If it is not set or set to a value not equal to "false", LFC is enabled by default.
USE_LFC = os.environ.get("USE_LFC") != "false"


def subprocess_capture(
    capture_dir: Path,
    cmd: list[str],
    *,
    check: bool = False,
    echo_stderr: bool = False,
    echo_stdout: bool = False,
    capture_stdout: bool = False,
    timeout: float | None = None,
    with_command_header: bool = True,
    **popen_kwargs: Any,
) -> tuple[str, str | None, int]:
    """Run a process and bifurcate its output to files and the `log` logger

    stderr and stdout are always captured in files.  They are also optionally
    echoed to the log (echo_stderr, echo_stdout), and/or captured and returned
    (capture_stdout).

    File output will go to files named "cmd_NNN.stdout" and "cmd_NNN.stderr"
    where "cmd" is the name of the program and NNN is an incrementing
    counter.

    If those files already exist, we will overwrite them.

    Returns 3-tuple of:
     - The base path for output files
     - Captured stdout, or None
     - The exit status of the process
    """
    assert isinstance(cmd, list)
    base_cmd = os.path.basename(cmd[0])
    base = f"{base_cmd}_{global_counter()}"
    basepath = os.path.join(capture_dir, base)
    stdout_filename = f"{basepath}.stdout"
    stderr_filename = f"{basepath}.stderr"

    # Since we will stream stdout and stderr concurrently, need to do it in a thread.
    class OutputHandler(threading.Thread):
        def __init__(self, in_file, out_file, echo: bool, capture: bool):
            super().__init__()
            self.in_file = in_file
            self.out_file = out_file
            self.echo = echo
            self.capture = capture
            self.captured = ""

        @override
        def run(self):
            first = with_command_header
            for line in self.in_file:
                if first:
                    # do this only after receiving any input so that we can
                    # keep deleting empty files, or leave it out completly if
                    # it was unwanted (using the file as input later for example)
                    first = False
                    # prefix the files with the command line so that we can
                    # later understand which file is for what command
                    self.out_file.write((f"# {' '.join(cmd)}\n\n").encode())

                # Only bother decoding if we are going to do something more than stream to a file
                if self.echo or self.capture:
                    string = line.decode(encoding="utf-8", errors="replace")

                    if self.echo:
                        log.info(string.strip())

                    if self.capture:
                        self.captured += string

                self.out_file.write(line)

    captured = None
    try:
        with open(stdout_filename, "wb") as stdout_f:
            with open(stderr_filename, "wb") as stderr_f:
                log.info(f'Capturing stdout to "{base}.stdout" and stderr to "{base}.stderr"')

                p = subprocess.Popen(
                    cmd,
                    **popen_kwargs,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                stdout_handler = OutputHandler(
                    p.stdout, stdout_f, echo=echo_stdout, capture=capture_stdout
                )
                stdout_handler.start()
                stderr_handler = OutputHandler(p.stderr, stderr_f, echo=echo_stderr, capture=False)
                stderr_handler.start()

                r = p.wait(timeout=timeout)

                stdout_handler.join()
                stderr_handler.join()

                if check and r != 0:
                    raise subprocess.CalledProcessError(r, " ".join(cmd))

                if capture_stdout:
                    captured = stdout_handler.captured
    finally:
        # Remove empty files if there is no output
        for filename in (stdout_filename, stderr_filename):
            if os.stat(filename).st_size == 0:
                os.remove(filename)

    return (basepath, captured, r)


_global_counter = 0
_global_counter_lock = threading.Lock()


def global_counter() -> int:
    """A really dumb but thread-safe global counter.

    This is useful for giving output files a unique number, so if we run the
    same command multiple times we can keep their output separate.
    """
    global _global_counter, _global_counter_lock
    with _global_counter_lock:
        _global_counter += 1
        return _global_counter


def print_gc_result(row: dict[str, Any]):
    log.info("GC duration {elapsed} ms".format_map(row))
    log.info(
        (
            "  total: {layers_total}, needed_by_cutoff {layers_needed_by_cutoff}, needed_by_pitr {layers_needed_by_pitr}"
            " needed_by_branches: {layers_needed_by_branches}, not_updated: {layers_not_updated}, removed: {layers_removed}"
        ).format_map(row)
    )


def query_scalar(cur: cursor, query: str) -> Any:
    """
    It is a convenience wrapper to avoid repetitions
    of cur.execute(); cur.fetchone()[0]

    And this is mypy friendly, because without None
    check mypy says that Optional is not indexable.
    """
    cur.execute(query)
    var = cur.fetchone()
    assert var is not None
    return var[0]


# Traverse directory to get total size.
def get_dir_size(path: Path) -> int:
    """Return size in bytes."""
    totalbytes = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                totalbytes += os.path.getsize(os.path.join(root, name))
            except FileNotFoundError:
                pass  # file could be concurrently removed

    return totalbytes


def get_timeline_dir_size(path: Path) -> int:
    """Get the timeline directory's total size, which only counts the layer files' size."""
    sz = 0
    for dir_entry in path.iterdir():
        with contextlib.suppress(Exception):
            # file is an image layer
            _ = parse_image_layer(dir_entry.name)
            sz += dir_entry.stat().st_size
            continue

        with contextlib.suppress(Exception):
            # file is a delta layer
            _ = parse_delta_layer(dir_entry.name)
            sz += dir_entry.stat().st_size
    return sz


def get_scale_for_db(size_mb: int) -> int:
    """Returns pgbench scale factor for given target db size in MB.

    Ref https://www.cybertec-postgresql.com/en/a-formula-to-calculate-pgbench-scaling-factor-for-target-db-size/
    """

    return round(0.06689 * size_mb - 0.5)


ATTACHMENT_NAME_REGEX: re.Pattern[str] = re.compile(
    r"regression\.(diffs|out)|.+\.(?:log|stderr|stdout|filediff|metrics|html|walredo)"
)


def allure_attach_from_dir(dir: Path, preserve_database_files: bool = False):
    """Attach all non-empty files from `dir` that matches `ATTACHMENT_NAME_REGEX` to Allure report"""

    if preserve_database_files:
        zst_file = dir.with_suffix(".tar.zst")
        with zst_file.open("wb") as zst:
            cctx = zstandard.ZstdCompressor()
            with cctx.stream_writer(zst) as compressor:
                with tarfile.open(fileobj=compressor, mode="w") as tar:
                    tar.add(dir, arcname="")
        allure.attach.file(zst_file, "everything.tar.zst", "application/zstd", "tar.zst")

    for attachment in Path(dir).glob("**/*"):
        if ATTACHMENT_NAME_REGEX.fullmatch(attachment.name) and attachment.stat().st_size > 0:
            name = str(attachment.relative_to(dir))

            # compress files that are larger than 1Mb, they're hardly readable in a browser
            if attachment.stat().st_size > 1024**2:
                compressed = attachment.with_suffix(".zst")

                cctx = zstandard.ZstdCompressor()
                with attachment.open("rb") as fin, compressed.open("wb") as fout:
                    cctx.copy_stream(fin, fout)

                name = f"{name}.zst"
                attachment = compressed

            source = str(attachment)
            if source.endswith(".gz"):
                attachment_type = "application/gzip"
                extension = "gz"
            elif source.endswith(".zst"):
                attachment_type = "application/zstd"
                extension = "zst"
            elif source.endswith(".svg"):
                attachment_type = "image/svg+xml"
                extension = "svg"
            elif source.endswith(".html"):
                attachment_type = "text/html"
                extension = "html"
            elif source.endswith(".walredo"):
                attachment_type = "application/octet-stream"
                extension = "walredo"
            else:
                attachment_type = "text/plain"
                extension = attachment.suffix.removeprefix(".")

            allure.attach.file(source, name, attachment_type, extension)


GRAFANA_URL = "https://neonprod.grafana.net"
GRAFANA_EXPLORE_URL = f"{GRAFANA_URL}/explore"
GRAFANA_TIMELINE_INSPECTOR_DASHBOARD_URL = f"{GRAFANA_URL}/d/8G011dlnk/timeline-inspector"
LOGS_STAGING_DATASOURCE_ID = "xHHYY0dVz"


def allure_add_grafana_links(host: str, timeline_id: TimelineId, start_ms: int, end_ms: int):
    """Add links to server logs in Grafana to Allure report"""
    links: dict[str, str] = {}
    # We expect host to be in format like ep-divine-night-159320.us-east-2.aws.neon.build
    endpoint_id, region_id, _ = host.split(".", 2)

    expressions = {
        "compute logs": f'{{app="compute-node-{endpoint_id}", neon_region="{region_id}"}}',
        "k8s events": f'{{job="integrations/kubernetes/eventhandler"}} |~ "name=compute-node-{endpoint_id}-"',
        "console logs": f'{{neon_service="console", neon_region="{region_id}"}} | json | endpoint_id = "{endpoint_id}"',
        "proxy logs": f'{{neon_service="proxy-scram", neon_region="{region_id}"}}',
    }

    params: dict[str, Any] = {
        "datasource": LOGS_STAGING_DATASOURCE_ID,
        "queries": [
            {
                "expr": "<PUT AN EXPRESSION HERE>",
                "refId": "A",
                "datasource": {"type": "loki", "uid": LOGS_STAGING_DATASOURCE_ID},
                "editorMode": "code",
                "queryType": "range",
            }
        ],
        "range": {
            "from": str(start_ms),
            "to": str(end_ms),
        },
    }
    for name, expr in expressions.items():
        params["queries"][0]["expr"] = expr
        query_string = urlencode({"orgId": 1, "left": json.dumps(params)})
        links[name] = f"{GRAFANA_EXPLORE_URL}?{query_string}"

    timeline_qs = urlencode(
        {
            "orgId": 1,
            "var-environment": "victoria-metrics-aws-dev",
            "var-timeline_id": timeline_id,
            "var-endpoint_id": endpoint_id,
            "var-log_datasource": "grafanacloud-neonstaging-logs",
            "from": start_ms,
            "to": end_ms,
        }
    )
    link = f"{GRAFANA_TIMELINE_INSPECTOR_DASHBOARD_URL}?{timeline_qs}"
    links["Timeline Inspector"] = link

    for name, link in links.items():
        allure.dynamic.link(link, name=name)
        log.info(f"{name}: {link}")


def start_in_background(
    command: list[str], cwd: Path, log_file_name: str, is_started: Callable[[], WaitUntilRet]
) -> subprocess.Popen[bytes]:
    """Starts a process, creates the logfile and redirects stderr and stdout there. Runs the start checks before the process is started, or errors."""

    log.info(f'Running command "{" ".join(command)}"')

    with open(cwd / log_file_name, "wb") as log_file:
        spawned_process = subprocess.Popen(command, stdout=log_file, stderr=log_file, cwd=cwd)
        error = None
        try:
            return_code = spawned_process.poll()
            if return_code is not None:
                error = f"expected subprocess to run but it exited with code {return_code}"
            else:
                try:
                    wait_until(is_started, timeout=10)
                except Exception:
                    error = "Failed to get correct status from subprocess"
        except Exception as e:
            error = f"expected subprocess to start but it failed with exception: {e}"

        if error is not None:
            log.error(error)
            spawned_process.kill()
            raise Exception(f"Failed to run subprocess as {command}, reason: {error}")

        log.info("subprocess spawned")
        return spawned_process


def wait_until(
    func: Callable[[], WaitUntilRet],
    name: str | None = None,
    timeout: float = 20.0,  # seconds
    interval: float = 0.5,  # seconds
    status_interval: float = 1.0,  # seconds
) -> WaitUntilRet:
    """
    Wait until 'func' returns successfully, without exception. Returns the
    last return value from the function.
    """
    if name is None:
        name = getattr(func, "__name__", repr(func))
    deadline = datetime.now() + timedelta(seconds=timeout)
    next_status = datetime.now()
    last_exception = None
    while datetime.now() <= deadline:
        try:
            return func()
        except Exception as e:
            if datetime.now() >= next_status:
                log.info("waiting for %s: %s", name, e)
                next_status = datetime.now() + timedelta(seconds=status_interval)
            last_exception = e
            time.sleep(interval)
    raise Exception(f"timed out while waiting for {name}") from last_exception


def assert_eq(a, b) -> None:
    assert a == b


def assert_gt(a, b) -> None:
    assert a > b


def assert_ge(a, b) -> None:
    assert a >= b


def run_pg_bench_small(pg_bin: PgBin, connstr: str):
    """
    Fast way to populate data.
    For more layers consider combining with these tenant settings:
    {
        "checkpoint_distance": 1024 ** 2,
        "image_creation_threshold": 100,
    }
    """
    pg_bin.run(["pgbench", "-i", "-I dtGvp", "-s1", connstr])


def humantime_to_ms(humantime: str) -> float:
    """
    Converts Rust humantime's output string to milliseconds.

    humantime_to_ms("1h 1ms 406us") -> 3600001.406
    """

    unit_multiplier_map = {
        "ns": 1e-6,
        "us": 1e-3,
        "ms": 1,
        "s": 1e3,
        "m": 1e3 * 60,
        "h": 1e3 * 60 * 60,
    }
    matcher = re.compile(rf"^(\d+)({'|'.join(unit_multiplier_map.keys())})$")
    total_ms = 0.0

    if humantime == "0":
        return total_ms

    for item in humantime.split():
        if (match := matcher.search(item)) is not None:
            n, unit = match.groups()
            total_ms += int(n) * unit_multiplier_map[unit]
        else:
            raise ValueError(
                f"can't parse '{item}' (from string '{humantime}'), known units are {', '.join(unit_multiplier_map.keys())}."
            )

    return round(total_ms, 3)


def scan_log_for_errors(input: Iterable[str], allowed_errors: list[str]) -> list[tuple[int, str]]:
    # FIXME: this duplicates test_runner/fixtures/pageserver/allowed_errors.py
    error_or_warn = re.compile(r"\s(ERROR|WARN)")
    errors: list[tuple[int, str]] = []
    for lineno, line in enumerate(input, start=1):
        if len(line) == 0:
            continue

        if error_or_warn.search(line):
            # Is this a torn log line?  This happens when force-killing a process and restarting
            # Example: "2023-10-25T09:38:31.752314Z  WARN deletion executo2023-10-25T09:38:31.875947Z  INFO version: git-env:0f9452f76e8ccdfc88291bccb3f53e3016f40192"
            if re.match("\\d{4}-\\d{2}-\\d{2}T.+\\d{4}-\\d{2}-\\d{2}T.+INFO version.+", line):
                continue

            # It's an ERROR or WARN. Is it in the allow-list?
            for a in allowed_errors:
                try:
                    if re.match(a, line):
                        break
                # We can switch `re.error` with `re.PatternError` after 3.13
                # https://docs.python.org/3/library/re.html#re.PatternError
                except re.error:
                    log.error(f"Invalid regex: '{a}'")
                    raise
            else:
                errors.append((lineno, line))
    return errors


def assert_no_errors(log_file: Path, service: str, allowed_errors: list[str]):
    if not log_file.exists():
        log.warning(f"Skipping {service} log check: {log_file} does not exist")
        return

    with log_file.open("r") as f:
        errors = scan_log_for_errors(f, allowed_errors)

    for _lineno, error in errors:
        log.info(f"not allowed {service} error: {error.strip()}")

    assert not errors, f"First log error on {service}: {errors[0]}\nHint: use scripts/check_allowed_errors.sh to test any new allowed_error you add"


def assert_pageserver_backups_equal(left: Path, right: Path, skip_files: set[str]):
    """
    This is essentially:

    lines=$(comm -3 \
        <(mkdir left  && cd left  && tar xf "$left"  && find . -type f -print0 | xargs sha256sum | sort -k2) \
        <(mkdir right && cd right && tar xf "$right" && find . -type f -print0 | xargs sha256sum | sort -k2) \
        | wc -l)
    [ "$lines" = "0" ]

    But in a more mac friendly fashion.
    """
    started_at = time.time()

    def hash_extracted(reader: IO[bytes] | None) -> bytes:
        assert reader is not None
        digest = sha256(usedforsecurity=False)
        while True:
            buf = reader.read(64 * 1024)
            if not buf:
                break
            digest.update(buf)
        return digest.digest()

    def build_hash_list(p: Path) -> list[tuple[str, bytes]]:
        with tarfile.open(p) as f:
            matching_files = (info for info in f if info.isreg() and info.name not in skip_files)
            ret = list(
                map(lambda info: (info.name, hash_extracted(f.extractfile(info))), matching_files)
            )
            ret.sort(key=lambda t: t[0])
            return ret

    left_list, right_list = map(build_hash_list, [left, right])

    assert len(left_list) == len(
        right_list
    ), f"unexpected number of files on tar files, {len(left_list)} != {len(right_list)}"

    mismatching: set[str] = set()

    for left_tuple, right_tuple in zip(left_list, right_list, strict=False):
        left_path, left_hash = left_tuple
        right_path, right_hash = right_tuple
        assert (
            left_path == right_path
        ), f"file count matched, expected these to be same paths: {left_path}, {right_path}"
        if left_hash != right_hash:
            mismatching.add(left_path)

    assert len(mismatching) == 0, f"files with hash mismatch: {mismatching}"

    elapsed = time.time() - started_at
    log.info(f"assert_pageserver_backups_equal completed in {elapsed}s")


class PropagatingThread(threading.Thread):
    _target: Any
    _args: Any
    _kwargs: Any
    """
    Simple Thread wrapper with join() propagating the possible exception in the thread.
    """

    @override
    def run(self):
        self.exc = None
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e

    @override
    def join(self, timeout: float | None = None) -> Any:
        super().join(timeout)
        if self.exc:
            raise self.exc
        return self.ret


class EnhancedJSONEncoder(json.JSONEncoder):
    """
    Default json.JSONEncoder works only on primitive builtins. Extend it to any
    dataclass plus our custom types.
    """

    def default(self, o):
        if dataclasses.is_dataclass(o) and not isinstance(o, type):
            return dataclasses.asdict(o)
        elif isinstance(o, Id):
            return o.id.hex()
        elif isinstance(o, Lsn):
            return str(o)  # standard hex notation
        return super().default(o)


def human_bytes(amt: float) -> str:
    """
    Render a bytes amount into nice IEC bytes string.
    """

    suffixes = ["", "Ki", "Mi", "Gi"]

    last = suffixes[-1]

    for name in suffixes:
        if amt < 1024 or name == last:
            return f"{int(round(amt))} {name}B"
        amt = amt / 1024

    raise RuntimeError("unreachable")


def allpairs_versions():
    """
    Returns a dictionary with arguments for pytest parametrize
    to test the compatibility with the previous version of Neon components
    combinations were pre-computed to test all the pairs of the components with
    the different versions.
    """
    ids = []
    argvalues = []
    compat_not_defined = (
        os.getenv("COMPATIBILITY_POSTGRES_DISTRIB_DIR") is None
        or os.getenv("COMPATIBILITY_NEON_BIN") is None
    )
    for pair in VERSIONS_COMBINATIONS:
        cur_id = []
        all_new = all(v == "new" for v in pair.values())
        for component in sorted(pair.keys()):
            cur_id.append(pair[component][0])
        # Adding None if all versions are new, sof no need to mix at all
        # If COMPATIBILITY_NEON_BIN or COMPATIBILITY_POSTGRES_DISTRIB_DIR are not defined,
        # we will skip all the tests which include the versions mix.
        argvalues.append(
            pytest.param(
                None if all_new else pair,
                marks=pytest.mark.skipif(
                    compat_not_defined and not all_new,
                    reason="COMPATIBILITY_NEON_BIN or COMPATIBILITY_POSTGRES_DISTRIB_DIR is not set",
                ),
            )
        )
        ids.append(f"combination_{''.join(cur_id)}")
    return {"argnames": "combination", "argvalues": tuple(argvalues), "ids": ids}


def size_to_bytes(hr_size: str) -> int:
    """
    Gets human-readable size from postgresql.conf (e.g. 512kB, 10MB)
    returns size in bytes
    """
    units = {"B": 1, "kB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4, "PB": 1024**5}
    match = re.search(r"^\'?(\d+)\s*([kMGTP]?B)?\'?$", hr_size)
    assert match is not None, f'"{hr_size}" is not a well-formatted human-readable size'
    number, unit = match.groups()

    if unit:
        amp = units[unit]
    else:
        amp = 8192
    return int(number) * amp


def skip_on_postgres(version: PgVersion, reason: str):
    return pytest.mark.skipif(
        PgVersion(os.getenv("DEFAULT_PG_VERSION", PgVersion.DEFAULT)) is version,
        reason=reason,
    )


def xfail_on_postgres(version: PgVersion, reason: str):
    return pytest.mark.xfail(
        PgVersion(os.getenv("DEFAULT_PG_VERSION", PgVersion.DEFAULT)) is version,
        reason=reason,
    )


def run_only_on_default_postgres(reason: str):
    return pytest.mark.skipif(
        PgVersion(os.getenv("DEFAULT_PG_VERSION", PgVersion.DEFAULT)) is not PgVersion.DEFAULT,
        reason=reason,
    )


def run_only_on_postgres(versions: Iterable[PgVersion], reason: str):
    return pytest.mark.skipif(
        PgVersion(os.getenv("DEFAULT_PG_VERSION", PgVersion.DEFAULT)) not in versions,
        reason=reason,
    )


def skip_in_debug_build(reason: str):
    return pytest.mark.skipif(
        os.getenv("BUILD_TYPE", "debug") == "debug",
        reason=reason,
    )


def skip_on_ci(reason: str):
    # `CI` variable is always set to `true` on GitHub
    # Ref: https://docs.github.com/en/actions/writing-workflows/choosing-what-your-workflow-does/store-information-in-variables#default-environment-variables
    return pytest.mark.skipif(
        os.getenv("CI", "false") == "true",
        reason=reason,
    )
