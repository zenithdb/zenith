import argparse
import asyncio
import enum
import json
import os
import pprint
import tempfile
from asyncio import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

"""
This is the automation tool that was mostly helpful during our big aws account migration,
but may be helpful in other day to day tasks and concentrate knowledge about operations
that can help during on-call.


This script executes commands on remote using ssh multiplexing. See referenes:
    https://blog.scottlowe.org/2015/12/11/using-ssh-multiplexing/
    https://github.com/openssh-rust/openssh/blob/master/src/builder.rs
    https://github.com/openssh-rust/openssh/blob/master/src/process_impl/session.rs
    https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing
    https://docs.rs/openssh/0.9.8/openssh/

For use with teleport you'll need to setup nsh script mentioned here:
https://github.com/neondatabase/cloud/wiki/Cloud%3A-access#3-access-the-nodes-with-ssm
"""


def show_line(output_label: Optional[str], line: str):
    if output_label is not None:
        print(f"({output_label})", line, end="")
    else:
        print("    ", line, end="")
    if not line:
        print()


async def exec_checked(
    program: str,
    args: List[str],
    err_msg: Optional[str] = None,
    output_label: Optional[str] = None,
    show_output: bool = True,
    expected_exit_codes=frozenset((0,)),
) -> List[str]:
    if show_output:
        print("+", program, *args)
    proc = await subprocess.create_subprocess_exec(
        program,
        *args,
        stdout=asyncio.subprocess.PIPE,
        limit=10 << 20,
    )

    assert proc.stdout is not None

    out = []

    line = (await proc.stdout.readline()).decode()
    if show_output:
        show_line(output_label, line)

    out.append(line)

    while line:
        line = (await proc.stdout.readline()).decode()
        # empty line means eof, actual empty line from the program is represented by "\n"
        if not line:
            continue

        if show_output:
            show_line(output_label, line)
        out.append(line)
    exit_code = await proc.wait()
    assert exit_code in expected_exit_codes, err_msg or f"{program} failed with {exit_code}"
    return out


class Connection:
    def __init__(
        self,
        tempdir: tempfile.TemporaryDirectory,  # type: ignore
        target: str,
    ):
        self.tempdir = tempdir
        self.target = target

    def get_args(self, extra_args: List[str]):
        ctl_path = os.path.join(self.tempdir.name, "master")
        return ["-S", ctl_path, "-o", "BatchMode=yes", *extra_args, "none"]

    async def check(self):
        args = self.get_args(["-O", "check"])
        await exec_checked("ssh", args, err_msg="master check operation failed")

    async def spawn(self, cmd: str):
        # https://github.com/openssh-rust/openssh/blob/cd8f174fafc530d8e55c2aa63add14a24cb2b94c/src/process_impl/session.rs#L72
        local_args = self.get_args(["-T", "-p", "9"])
        local_args.extend(["--", f"bash -c '{cmd}'"])
        return await exec_checked(
            "ssh", local_args, err_msg="spawn failed", output_label=self.target
        )

    async def close(self):
        args = self.get_args(["-O", "exit"])
        await exec_checked("ssh", args, err_msg="master exit operation failed")


async def connect(target: str) -> Connection:
    """
    target is directly passed to ssh command
    """
    # NOTE: it is mentioned that this setup is not secure
    #     For better security it should be placed somewhere in ~/.ssh
    #     or in other directory with proper permissions
    #     openssh-rust does it the same way
    #     https://github.com/openssh-rust/openssh/blob/master/src/builder.rs
    connection_dir = tempfile.TemporaryDirectory(suffix=".ssh-multiplexed")
    # "-E logfile"
    await exec_checked(
        "ssh",
        [
            "-S",
            os.path.join(connection_dir.name, "master"),
            "-M",  # Places the ssh client into “master” mode for connection sharing.
            "-f",  # Requests ssh to go to background just before command execution.
            "-N",  # Do not execute a remote command. This is useful for just forwarding ports.
            "-o",
            "BatchMode=yes",
            target,
        ],
        err_msg="starting master process failed",
    )
    return Connection(tempdir=connection_dir, target=target)


class Timer:
    def __init__(self, msg: str) -> None:
        self.t0 = datetime.now()
        self.msg = msg

    def __enter__(self):
        return None

    def __exit__(self, *_):
        print(self.msg, datetime.now() - self.t0)


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def write_line(f, line: str):
    f.write(line)
    f.write("\n")


async def pageserver_tenant_sizes(
    pageserver_target: str, tenants_of_interest: Optional[List[str]] = None
) -> Dict[str, int]:
    """
    With ondemand it should rather look at physical size api
    For old projects since we dont have eviction yet,
    we can look at local fs state.
    """
    if tenants_of_interest is not None:
        tenants_of_interest = set(tenants_of_interest)  # type: ignore

    ps_connection = await connect(pageserver_target)
    out = await ps_connection.spawn("du -sb /storage/pageserver/data/tenants/* | sort -rh")

    tenants = {}

    for line in out:
        if line.startswith("du: cannot read directory"):
            continue

        size, tenant_path = map(str.strip, line.split())
        tenant = Path(tenant_path).stem
        if tenants_of_interest is not None:
            if tenant not in tenants_of_interest:
                continue

        tenants[tenant] = int(size)
    return tenants


async def fetch_ps_size(args):
    if args.input is not None:
        tenants = Path(args.input).read_text().splitlines()
    else:
        tenants = None

    sizes = await pageserver_tenant_sizes(args.target, tenants_of_interest=tenants)

    total = 0
    for tenant, size in sorted(sizes.items(), key=lambda x: x[1], reverse=True):
        total += size
        print(tenant, size)
    print("total", total)


@enum.unique
class Env(enum.Enum):
    STAGING = "staging"
    PRODUCTION = "production"


class ConsoleAdminShortcuts:
    def __init__(self, env: Env, verbose: bool = False):
        if env is Env.STAGING:
            self.admin_base_url = "https://console.neon.tech/api/v1"
            self.management_base_url = "http://console-staging.local:3440/management/api/v2"
        elif env is Env.PRODUCTION:
            self.admin_base_url = "https://console.neon.tech"
            self.management_base_url = "http://console-release.local:3441/management/api/v2"

        self.api_token = os.getenv("CONSOLE_ADMIN_API_TOKEN")
        assert self.api_token, '"CONSOLE_ADMIN_API_TOKEN" is missing in env'

        self.verbose = verbose

    async def check_availability(self, project_id: str):
        url = f"{self.admin_base_url}/admin/projects/{project_id}/check_availability"
        output = await exec_checked(
            "curl",
            [
                "--silent",
                "--fail",
                "-XPOST",
                url,
                "-H",
                f"Authorization: Bearer {self.api_token}",
                "-H",
                "Accept: application/json",
            ],
            show_output=self.verbose,
        )
        assert len(output) == 1  # output should be one line of json
        return json.loads(output.pop())

    async def get_operation(self, operation_id: str):
        url = f"{self.admin_base_url}/admin/operations/{operation_id}"
        output = await exec_checked(
            "curl",
            [
                "--silent",
                "--fail",
                url,
                "-H",
                f"Authorization: Bearer {self.api_token}",
                "-H",
                "Accept: application/json",
            ],
            show_output=self.verbose,
        )
        assert len(output) == 1  # output should be one line of json
        return json.loads(output.pop())

    async def get_pageservers(self):
        url = f"{self.admin_base_url}/admin/pageservers"
        output = await exec_checked(
            "curl",
            [
                "--silent",
                "--fail",
                url,
                "-H",
                f"Authorization: Bearer {self.api_token}",
                "-H",
                "Accept: application/json",
            ],
            show_output=self.verbose,
        )
        assert len(output) == 1  # output should be one line of json
        return json.loads(output.pop())

    async def set_maintenance(self, project_id: str, maintenance: bool) -> Dict[str, Any]:
        """
        Example response:
        {
            "project": {
                "id": "tight-wood-864662",
                "maintenance_set_at": "2023-01-31T13:36:45.90346Z"
            },
            "operations": [
                {
                "id": "216142e0-fbb7-4f41-a470-e63408d4d6b4"
                }
            ]
        }
        """
        url = f"{self.management_base_url}/projects/{project_id}/maintenance"
        data = json.dumps({"maintenance": maintenance})
        if not self.verbose:
            args = ["--silent"]
        else:
            args = []
        args.extend(
            [
                "--fail",
                "-XPUT",
                url,
                "-H",
                f"Authorization: Bearer {self.api_token}",
                "-H",
                "Accept: application/json",
                "-d",
                data,
            ]
        )
        output = await exec_checked(
            "curl",
            [],
            show_output=self.verbose,
        )
        assert len(output) == 1  # output should be one line of json
        ret = json.loads(output.pop())
        assert isinstance(ret, Dict)
        return ret

    async def fetch_branches(self, project_id: str):
        url = f"{self.admin_base_url}/admin/branches?project_id={project_id}"
        output = await exec_checked(
            "curl",
            [
                "--silent",
                "--fail",
                url,
                "-H",
                f"Authorization: Bearer {self.api_token}",
                "-H",
                "Accept: application/json",
            ],
            show_output=self.verbose,
        )
        assert len(output) == 1  # output should be one line of json
        return json.loads(output.pop())


async def poll_pending_ops(console: ConsoleAdminShortcuts, pending_ops: Set[str]):
    finished = set()  # needed because sets cannot be changed during iteration
    for pending_op in pending_ops:
        data = await console.get_operation(pending_op)
        operation = data["operation"]
        status = operation["status"]
        if status == "failed":
            print(f"ERROR: operation {pending_op} failed")
            continue

        if operation["failures_count"] != 0:
            print(f"WARN: operation {pending_op} has failures != 0")
            continue

        if status == "finished":
            print(f"operation {pending_op} finished")
            finished.add(pending_op)
        else:
            print(f"operation {pending_op} is still pending: {status}")

    pending_ops.difference_update(finished)


async def check_availability(args):
    console = ConsoleAdminShortcuts(env=Env(args.env))
    max_concurrent_checks = args.max_concurrent_checks

    # reverse to keep the order because we will be popping from the end
    projects: List[str] = list(reversed(Path(args.input).read_text().splitlines()))
    print("n_projects", len(projects))

    pending_ops: Set[str] = set()
    while projects:
        # walk through pending ops
        if pending_ops:
            print("pending", len(pending_ops), pending_ops)
            await poll_pending_ops(console, pending_ops)

        # schedule new ops if limit allows
        while len(pending_ops) < max_concurrent_checks and len(projects) > 0:
            project = projects.pop()
            print("starting:", project, len(projects))
            # there can be many operations, one for each endpoint
            data = await console.check_availability(project)
            for operation in data["operations"]:
                pending_ops.add(operation["ID"])
            # wait a bit before starting next one
            await asyncio.sleep(2)

        if projects:
            # sleep a little bit to give operations time to finish
            await asyncio.sleep(5)

    print("all scheduled, poll pending", len(pending_ops), pending_ops, projects)
    while pending_ops:
        await poll_pending_ops(console, pending_ops)
        await asyncio.sleep(5)


async def maintain(args):
    console = ConsoleAdminShortcuts(env=Env(args.env))
    finish_flag = args.finish

    projects: List[str] = Path(args.input).read_text().splitlines()
    print("n_projects", len(projects))

    pending_ops: Set[str] = set()

    for project in projects:
        data = await console.set_maintenance(project, maintenance=not finish_flag)
        print(project, len(data["operations"]))
        for operation in data["operations"]:
            pending_ops.add(operation["id"])

    if finish_flag:
        assert len(pending_ops) == 0
        return

    print("all scheduled, poll pending", len(pending_ops), pending_ops)
    while pending_ops:
        await poll_pending_ops(console, pending_ops)
        print("n pending ops:", len(pending_ops))
        if pending_ops:
            await asyncio.sleep(5)


SOURCE_BUCKET = "zenith-storage-oregon"
AWS_REGION = "us-west-2"
SAFEKEEPER_SOURCE_PREFIX_IN_BUCKET = "prod-1/wal"


async def fetch_sk_s3_size(args):
    tenants: List[str] = Path(args.input).read_text().splitlines()

    total_objects = 0
    total_size = 0
    for tenant in tenants:
        wal_prefix = f"s3://{SOURCE_BUCKET}/{SAFEKEEPER_SOURCE_PREFIX_IN_BUCKET}/{tenant}"
        result = await exec_checked(
            "aws",
            [
                "--profile",
                "neon_main",
                "s3",
                "ls",
                "--recursive",
                "--summarize",
                wal_prefix,
            ],
            expected_exit_codes={0, 1},
            show_output=False,
        )
        objects = int(result[-2].rsplit(maxsplit=1).pop())
        total_objects += objects

        size = int(result[-1].rsplit(maxsplit=1).pop())
        total_size += size

        print(tenant, "objects", objects, "size", size)

    print("total_objects", total_objects, "total_size", total_size)


async def fetch_branches(args):
    console = ConsoleAdminShortcuts(env=Env(args.env))
    project_id = args.project_id

    pprint.pprint(await console.fetch_branches(project_id=project_id))


async def get_pageservers(args):
    console = ConsoleAdminShortcuts(env=Env(args.env))

    pprint.pprint(await console.get_pageservers())


async def main():
    parser = argparse.ArgumentParser("migrator")
    sub = parser.add_subparsers(title="commands", dest="subparser_name")

    split_parser = sub.add_parser(
        "split",
    )
    split_parser.add_argument(
        "--input",
        help="CSV file with results from snowflake query mentioned in README.",
        required=True,
    )
    split_parser.add_argument(
        "--out",
        help="Directory to store groups of projects. Directory name is pageserver id.",
        required=True,
    )
    split_parser.add_argument(
        "--last-usage-cutoff",
        dest="last_usage_cutoff",
        help="Projects which do not have compute time starting from passed date (e g 2022-12-01) wil be considered not used recently",
        required=True,
    )
    split_parser.add_argument(
        "--select-pageserver-id",
        help="Filter input for this pageserver id",
        required=True,
    )

    fetch_ps_size_parser = sub.add_parser("fetch-ps-size")
    fetch_ps_size_parser.add_argument(
        "--target",
        help="Target pageserver host as resolvable by ssh",
        required=True,
    )
    fetch_ps_size_parser.add_argument(
        "--input",
        help="File containing list of tenants to include",
    )

    check_availability_parser = sub.add_parser("check-availability")
    check_availability_parser.add_argument(
        "--input",
        help="File containing list of projects to run availability checks for",
    )
    check_availability_parser.add_argument(
        "--env", choices=["staging", "production"], default="staging"
    )
    check_availability_parser.add_argument(
        "--max-concurrent-checks",
        help="Max number of simultaneously active availability checks",
        type=int,
        default=50,
    )

    maintain_parser = sub.add_parser("maintain")
    maintain_parser.add_argument(
        "--input",
        help="File containing list of projects",
    )
    maintain_parser.add_argument("--env", choices=["staging", "production"], default="staging")
    maintain_parser.add_argument(
        "--finish",
        action="store_true",
    )

    fetch_sk_s3_size_parser = sub.add_parser("fetch-sk-s3-size")
    fetch_sk_s3_size_parser.add_argument(
        "--input",
        help="File containing list of tenants",
    )

    fetch_branches_parser = sub.add_parser("fetch-branches")
    fetch_branches_parser.add_argument("--project-id")
    fetch_branches_parser.add_argument(
        "--env", choices=["staging", "production"], default="staging"
    )

    get_pageservers_parser = sub.add_parser("get-pageservers")
    get_pageservers_parser.add_argument(
        "--env", choices=["staging", "production"], default="staging"
    )

    args = parser.parse_args()

    handlers = {
        "fetch-ps-size": fetch_ps_size,
        "check-availability": check_availability,
        "maintain": maintain,
        "fetch-sk-s3-size": fetch_sk_s3_size,
        "fetch-branches": fetch_branches,
        "get-pageservers": get_pageservers,
    }

    handler = handlers.get(args.subparser_name)
    if handler:
        await handler(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
