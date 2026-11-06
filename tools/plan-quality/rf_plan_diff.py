#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Collect FE-vs-NovaRocks runtime-filter EXPLAIN VERBOSE differences."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, NamedTuple

from trino_query import run_text_column


RF_PATTERNS = (
    "runtime filter",
    "runtime filters",
    "build runtime filters",
    "probe runtime filters",
    "build_expr",
    "probe_expr",
)


DEFAULT_CASES = (
    "tpc-h/q4",
    "tpc-h/q22",
    "tpc-ds/q41",
    "tpc-ds/q72",
    "ssb/q1.1",
    "ssb/q1.2",
    "ssb/q1.3",
    "ssb/q2.1",
    "ssb/q2.2",
    "ssb/q2.3",
    "ssb/q3.1",
    "ssb/q3.2",
    "ssb/q3.3",
    "ssb/q3.4",
    "ssb/q4.1",
    "ssb/q4.2",
    "ssb/q4.3",
)


DEFAULT_DATABASES = {"ssb": "ssb", "tpc-h": "tpch", "tpc-ds": "tpcds"}


class Endpoint(NamedTuple):
    name: str
    host: str
    port: str
    user: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def case_to_sql_path(case_id: str) -> Path:
    suite, case_name = case_id.split("/", 1)
    return repo_root() / "tests" / "sql" / "correctness" / suite / "sql" / f"{case_name}.sql"


def case_default_database(case_id: str) -> str | None:
    suite = case_id.split("/", 1)[0]
    return DEFAULT_DATABASES.get(suite)


def explain_sql(raw_sql: str) -> str:
    non_comment_lines = [
        line for line in raw_sql.splitlines() if not line.lstrip().startswith("--")
    ]
    stripped = "\n".join(non_comment_lines).strip().rstrip(";").strip()
    if not stripped:
        raise ValueError("SQL body is empty after removing full-line comments")
    return f"EXPLAIN VERBOSE {stripped};"


def run_mysql(endpoint: Endpoint, sql: str, timeout: int, database: str | None) -> str:
    cmd = [
        "mysql",
        "-h",
        endpoint.host,
        "-P",
        endpoint.port,
        "-u",
        endpoint.user,
        "--batch",
        "--raw",
        "--skip-column-names",
    ]
    if database is not None:
        cmd.extend(["--database", database])
    cmd.extend(["-e", sql])
    env = os.environ.copy()
    env.update(
        {
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
            "HTTP_PROXY": "",
            "HTTPS_PROXY": "",
            "ALL_PROXY": "",
            "http_proxy": "",
            "https_proxy": "",
            "all_proxy": "",
        }
    )
    result = subprocess.run(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{endpoint.name} mysql failed for port {endpoint.port}: {result.stderr.strip()}"
        )
    return result.stdout


def rf_lines(explain: str) -> list[str]:
    lines = []
    for line in explain.splitlines():
        lowered = line.lower()
        if any(pattern in lowered for pattern in RF_PATTERNS):
            lines.append(line)
    return lines


def safe_file_name(case_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "__", case_id)


def run_novarocks(endpoint: Endpoint, raw_sql: str, timeout: float, database: str | None = None, *,
                   artifact_receipt: str | None = None, nr_catalog: str | None = None) -> str:
    return run_text_column(endpoint, explain_sql(raw_sql), timeout, database,
                           receipt=artifact_receipt, catalog=nr_catalog)


def collect_case(
    case_id: str,
    endpoints: Iterable[Endpoint],
    output_dir: Path,
    timeout: int,
    *, artifact_receipt: str | None = None, nr_catalog: str | None = None,
) -> dict[str, object]:
    sql_path = case_to_sql_path(case_id)
    raw_sql = sql_path.read_text()
    sql = explain_sql(raw_sql)
    database = case_default_database(case_id)
    entry: dict[str, object] = {
        "case": case_id,
        "sql_path": str(sql_path),
        "database": database,
    }
    for endpoint in endpoints:
        explain = (run_novarocks(endpoint, raw_sql, timeout, database,
                                  artifact_receipt=artifact_receipt, nr_catalog=nr_catalog)
                   if endpoint.name == "nr" else run_mysql(endpoint, sql, timeout, database))
        out_path = output_dir / endpoint.name / f"{safe_file_name(case_id)}.out"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(explain)
        lines = rf_lines(explain)
        entry[endpoint.name] = {
            "file": str(out_path),
            "runtime_filter_line_count": len(lines),
            "runtime_filter_lines": lines,
        }
    return entry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fe-host", default="127.0.0.1")
    parser.add_argument("--fe-port", required=True)
    parser.add_argument("--nr-host", default="127.0.0.1")
    parser.add_argument("--nr-port", required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--case", action="append", dest="cases")
    parser.add_argument("--artifact-receipt", default=None)
    parser.add_argument("--nr-catalog", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cases = args.cases or list(DEFAULT_CASES)
    output_dir = Path(args.output_dir)
    endpoints = (
        Endpoint("fe", args.fe_host, args.fe_port, args.user),
        Endpoint("nr", args.nr_host, args.nr_port, args.user),
    )
    summary = []
    for case_id in cases:
        summary.append(collect_case(case_id, endpoints, output_dir, args.timeout,
                                    artifact_receipt=args.artifact_receipt, nr_catalog=args.nr_catalog))
    status_dir = output_dir / "status"
    status_dir.mkdir(parents=True, exist_ok=True)
    (status_dir / "aggregate_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n"
    )
    rows = [
        "| case | FE RF lines | NR RF lines |",
        "|---|---:|---:|",
    ]
    for item in summary:
        fe_count = item["fe"]["runtime_filter_line_count"]  # type: ignore[index]
        nr_count = item["nr"]["runtime_filter_line_count"]  # type: ignore[index]
        rows.append(f"| {item['case']} | {fe_count} | {nr_count} |")
    (status_dir / "representative_queries.md").write_text("\n".join(rows) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
