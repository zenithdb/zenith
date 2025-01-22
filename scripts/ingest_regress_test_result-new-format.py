#! /usr/bin/env python3

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import backoff
import psycopg2
from psycopg2.extras import execute_values

CREATE_TABLE = """
CREATE TYPE arch AS ENUM ('ARM64', 'X64', 'UNKNOWN');
CREATE TABLE IF NOT EXISTS results (
    id           BIGSERIAL PRIMARY KEY,
    parent_suite TEXT NOT NULL,
    suite        TEXT NOT NULL,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    stopped_at   TIMESTAMPTZ NOT NULL,
    duration     INT NOT NULL,
    flaky        BOOLEAN NOT NULL,
    arch         arch DEFAULT 'X64',
    lfc          BOOLEAN DEFAULT false NOT NULL,
    build_type   TEXT NOT NULL,
    pg_version   INT NOT NULL,
    run_id       BIGINT NOT NULL,
    run_attempt  INT NOT NULL,
    reference    TEXT NOT NULL,
    revision     CHAR(40) NOT NULL,
    raw          JSONB COMPRESSION lz4 NOT NULL,
    UNIQUE (parent_suite, suite, name, arch, build_type, pg_version, started_at, stopped_at, run_id)
);
"""


@dataclass
class Row:
    parent_suite: str
    suite: str
    name: str
    status: str
    started_at: datetime
    stopped_at: datetime
    duration: int
    flaky: bool
    arch: str
    lfc: bool
    build_type: str
    pg_version: int
    run_id: int
    run_attempt: int
    reference: str
    revision: str
    raw: str


def err(msg):
    print(f"error: {msg}")
    sys.exit(1)


@contextmanager
def get_connection_cursor(connstr: str):
    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_time=150)
    def connect(connstr):
        conn = psycopg2.connect(connstr, connect_timeout=30)
        conn.autocommit = True
        return conn

    conn = connect(connstr)
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        if conn is not None:
            conn.close()


def create_table(cur):
    cur.execute(CREATE_TABLE)


def ingest_test_result(
    cur,
    reference: str,
    revision: str,
    run_id: int,
    run_attempt: int,
    test_cases_dir: Path,
):
    rows = []
    for f in test_cases_dir.glob("*.json"):
        test = json.loads(f.read_text())
        # Drop unneded fields from raw data
        raw = test.copy()
        raw.pop("parameterValues")
        raw.pop("labels")
        raw.pop("extra")

        # All allure parameters are prefixed with "__", see test_runner/fixtures/parametrize.py
        parameters = {
            p["name"].removeprefix("__"): p["value"]
            for p in test["parameters"]
            if p["name"].startswith("__")
        }
        arch = parameters.get("arch", "UNKNOWN").strip("'")
        lfc = parameters.get("lfc", "False") == "True"
        pg_version = int(parameters.get("pg_version", 17))
        build_type = parameters.get("build_type", "debug").strip("'")

        name = test["name"]
        labels = {label["name"]: label["value"] for label in test["labels"]}
        row = Row(
            parent_suite=labels["parentSuite"],
            suite=labels["suite"],
            name=name,
            status=test["status"],
            started_at=datetime.fromtimestamp(test["time"]["start"] / 1000, tz=UTC),
            stopped_at=datetime.fromtimestamp(test["time"]["stop"] / 1000, tz=UTC),
            duration=test["time"]["duration"],
            flaky=test["flaky"] or test["retriesStatusChange"],
            arch=arch,
            lfc=lfc,
            build_type=build_type,
            pg_version=pg_version,
            run_id=run_id,
            run_attempt=run_attempt,
            reference=reference,
            revision=revision,
            raw=json.dumps(raw),
        )
        rows.append(dataclasses.astuple(row))

    columns = ",".join(f.name for f in dataclasses.fields(Row))
    query = f"INSERT INTO results ({columns}) VALUES %s ON CONFLICT DO NOTHING"
    execute_values(cur, query, rows)


def main():
    parser = argparse.ArgumentParser(
        description="Regress test result uploader. \
            Database connection string should be provided via DATABASE_URL environment variable",
    )
    parser.add_argument("--initdb", action="store_true", help="Initialuze database")
    parser.add_argument(
        "--reference", type=str, required=True, help="git reference, for example refs/heads/main"
    )
    parser.add_argument("--revision", type=str, required=True, help="git revision")
    parser.add_argument("--run-id", type=int, required=True, help="GitHub Workflow run id")
    parser.add_argument(
        "--run-attempt", type=int, required=True, help="GitHub Workflow run attempt"
    )
    parser.add_argument(
        "--test-cases-dir",
        type=Path,
        required=True,
        help="Path to a dir with extended test cases data",
    )

    connstr = os.getenv("DATABASE_URL", "")
    if not connstr:
        err("DATABASE_URL environment variable is not set")

    args = parser.parse_args()
    with get_connection_cursor(connstr) as cur:
        if args.initdb:
            create_table(cur)

        if not args.test_cases_dir.exists():
            err(f"test-cases dir {args.test_cases_dir} does not exist")

        if not args.test_cases_dir.is_dir():
            err(f"test-cases dir {args.test_cases_dir} it not a directory")

        ingest_test_result(
            cur,
            reference=args.reference,
            revision=args.revision,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
            test_cases_dir=args.test_cases_dir,
        )


if __name__ == "__main__":
    logging.getLogger("backoff").addHandler(logging.StreamHandler())
    main()
