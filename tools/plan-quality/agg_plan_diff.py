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

"""Collect FE-vs-NovaRocks aggregate split-shape EXPLAIN VERBOSE differences."""

from __future__ import annotations

import re


# NovaRocks EXPLAIN aggregate-mode markers (see src/sql/explain.rs:581).
_NR_PATTERNS = {
    "single": re.compile(r"HASH AGGREGATE \(SINGLE"),
    "local": re.compile(r"HASH AGGREGATE \(LOCAL"),
    "global": re.compile(r"HASH AGGREGATE \(GLOBAL"),
}

# StarRocks FE EXPLAIN aggregate-phase markers.
_FE_PATTERNS = {
    "single": re.compile(r"AGGREGATE \((?:update|merge) finalize\)"),
    "update": re.compile(r"AGGREGATE \(update (?:serialize|finalize)\)"),
    "merge": re.compile(r"AGGREGATE \(merge (?:serialize|finalize)\)"),
}


def agg_counts(explain: str, dialect: str) -> dict[str, int]:
    """Count aggregate phase markers per dialect.

    NR keys: single/local/global. FE keys: single/update/merge.
    FE 'single' (a lone `update finalize` with no matching merge) is resolved
    by the caller via update-vs-merge balance; here we report raw marker hits
    for update/merge and reserve 'single' for the NR dialect only.
    """
    if dialect == "nr":
        return {k: len(p.findall(explain)) for k, p in _NR_PATTERNS.items()}
    if dialect == "fe":
        update = len(_FE_PATTERNS["update"].findall(explain))
        merge = len(_FE_PATTERNS["merge"].findall(explain))
        # An FE aggregate that is NOT split shows a single `update finalize`
        # with no paired merge; treat unpaired update-finalize as 'single'.
        single = max(0, len(re.findall(r"AGGREGATE \(update finalize\)", explain)))
        return {"single": single if merge == 0 else 0, "update": update, "merge": merge}
    raise ValueError(f"unknown dialect: {dialect!r}")


import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

from trino_query import run_text_column


DEFAULT_CASES = (
    "tpc-h/q1", "tpc-h/q7", "tpc-h/q8", "tpc-h/q9",
    "tpc-ds/q28", "tpc-ds/q44", "tpc-ds/q54", "tpc-ds/q67", "tpc-ds/q75", "tpc-ds/q85",
    "ssb/q1.1", "ssb/q2.1", "ssb/q3.1", "ssb/q4.1",
)
DEFAULT_DATABASES = {"ssb": "ssb", "tpc-h": "tpch", "tpc-ds": "tpcds"}


class Endpoint(NamedTuple):
    name: str
    host: str
    port: str
    user: str
    dialect: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def case_to_sql_path(case_id: str) -> Path:
    suite, case = case_id.split("/", 1)
    return repo_root() / "tests" / "sql" / "correctness" / suite / "sql" / f"{case}.sql"


def case_default_database(case_id: str) -> str | None:
    return DEFAULT_DATABASES.get(case_id.split("/", 1)[0])


def explain_sql(raw_sql: str) -> str:
    stripped = raw_sql.strip().rstrip(";").strip()
    return f"EXPLAIN VERBOSE {stripped};"


def run_mysql(endpoint: Endpoint, sql: str, timeout: int, database: str | None) -> str:
    cmd = ["mysql", "-h", endpoint.host, "-P", endpoint.port, "-u", endpoint.user, "--batch", "--raw"]
    if database:
        cmd += ["-D", database]
    cmd += ["-e", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"{endpoint.name} mysql failed: {proc.stderr.strip()}")
    return proc.stdout


def safe_file_name(case_id: str) -> str:
    return case_id.replace("/", "__")


def run_novarocks(endpoint: Endpoint, raw_sql: str, timeout: float, database: str | None = None, *,
                   artifact_receipt: str | None = None, nr_catalog: str | None = None) -> str:
    return run_text_column(endpoint, explain_sql(raw_sql), timeout, database,
                           receipt=artifact_receipt, catalog=nr_catalog)


def collect_case(endpoint: Endpoint, case_id: str, out_dir: Path, timeout: int, *,
                 artifact_receipt: str | None = None, nr_catalog: str | None = None) -> dict[str, int]:
    raw = case_to_sql_path(case_id).read_text()
    explain = (run_novarocks(endpoint, raw, timeout, case_default_database(case_id),
                              artifact_receipt=artifact_receipt, nr_catalog=nr_catalog)
               if endpoint.dialect == "nr" else
               run_mysql(endpoint, explain_sql(raw), timeout, case_default_database(case_id)))
    out_path = out_dir / endpoint.name / f"{safe_file_name(case_id)}.out"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(explain)
    return agg_counts(explain, endpoint.dialect)


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
    out_dir = Path(args.output_dir)
    fe = Endpoint("fe", args.fe_host, args.fe_port, args.user, "fe")
    nr = Endpoint("nr", args.nr_host, args.nr_port, args.user, "nr")
    cases = tuple(args.cases) if args.cases else DEFAULT_CASES

    rows = []
    summary = []
    failures = []
    for case_id in cases:
        try:
            fe_counts = collect_case(fe, case_id, out_dir, args.timeout)
            nr_counts = collect_case(nr, case_id, out_dir, args.timeout,
                                     artifact_receipt=args.artifact_receipt, nr_catalog=args.nr_catalog)
        except Exception as exc:  # fail-loud per case, keep going
            failures.append({"case": case_id, "error": str(exc)})
            print(f"[fail] {case_id}: {exc}", file=sys.stderr)
            continue
        fe_split = fe_counts["update"] + fe_counts["merge"]
        nr_split = nr_counts["local"] + nr_counts["global"]
        rows.append(f"| {case_id} | {fe_split} | {nr_split} |")
        summary.append({"case": case_id, "fe": fe_counts, "nr": nr_counts})

    status = out_dir / "status"
    status.mkdir(parents=True, exist_ok=True)
    (status / "aggregate_split_failures.json").write_text(json.dumps(failures, indent=2) + "\n")
    (status / "aggregate_split_summary.json").write_text(json.dumps(summary, indent=2))
    table = ["| case | FE split markers | NR split markers |", "|---|---:|---:|", *rows]
    (status / "aggregate_split_table.md").write_text("\n".join(table) + "\n")
    print("\n".join(table))
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
