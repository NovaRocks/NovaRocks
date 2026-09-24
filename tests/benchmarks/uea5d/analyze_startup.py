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

"""Compare two independent native startup baseline runs before freezing gates."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from pathlib import Path


STEM = "uea5d-startup-baseline"
EXPECTED_WARMUPS = 5
EXPECTED_MEASURED = 200
EXPECTED_FIXTURES = {
    "shallow",
    "shuffle-join-chain-v1",
    "union-width-4",
    "union-width-8",
    "union-width-16",
}


def nearest_rank(values: list[int], percentile: float) -> int:
    ordered = sorted(values)
    return ordered[math.ceil(percentile * len(ordered)) - 1]


def read_run(root: Path) -> dict:
    csv_path = root / f"{STEM}.csv"
    identity_path = root / f"{STEM}-fixtures.json"
    identity_bytes = identity_path.read_bytes()
    identity = json.loads(identity_bytes)
    if identity.get("schema_version") != 1:
        raise ValueError(f"{identity_path}: unsupported schema")
    for field in ("measured_binary_sha256", "runner_binary_sha256"):
        value = identity.get(field)
        if not isinstance(value, str) or len(value) != 64 or any(
            character not in "0123456789abcdef" for character in value
        ):
            raise ValueError(f"{identity_path}: missing or malformed {field}")
    fixtures = {item["fixture_id"]: item for item in identity["fixtures"]}
    if len(fixtures) != len(identity["fixtures"]) or set(fixtures) != EXPECTED_FIXTURES:
        raise ValueError(f"{identity_path}: duplicate or incomplete fixture set")
    manifest_bytes = (root / "run-manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    if (
        manifest.get("schema_version") != 6
        or manifest.get("kind") != "startup-baseline"
        or manifest.get("status") != "completed"
        or manifest.get("exit_code") != 0
        or manifest.get("formal") is not True
        or manifest.get("source_dirty") is not False
        or manifest.get("build_profile") != "release"
        or manifest.get("scenario") != "task-execution/uea5d-startup-baseline"
        or manifest.get("run_id") != identity["run_id"]
        or hashlib.sha256(manifest_bytes).hexdigest() != identity["run_manifest_sha256"]
    ):
        raise ValueError(f"{root}: incomplete or mismatched formal run manifest")
    if manifest.get("native_build_identity") != manifest.get("source_revision"):
        raise ValueError(f"{root}: native build identity differs from source revision")
    processes = manifest.get("process_identities")
    if not isinstance(processes, list) or [item.get("role") for item in processes] != [
        "fe", "be-0", "be-1", "be-2"
    ]:
        raise ValueError(f"{root}: expected native 1FE+3BE process identities")
    if any(
        item.get("build_identity") != manifest["source_revision"]
        or not isinstance(item.get("os_pid"), int)
        or item["os_pid"] <= 0
        for item in processes
    ) or len({item["os_pid"] for item in processes}) != 4:
        raise ValueError(f"{root}: invalid native process identities")

    fixture_bytes = (root / "fixture-spec.json").read_bytes()
    if hashlib.sha256(fixture_bytes).hexdigest() != manifest.get("fixture_sha256"):
        raise ValueError(f"{root}: frozen fixture bytes differ from manifest")
    fixture_spec = json.loads(fixture_bytes)
    if not isinstance(fixture_spec, list) or len(fixture_spec) != len(EXPECTED_FIXTURES):
        raise ValueError(f"{root}: incomplete frozen fixture specification")
    frozen = {}
    for item in fixture_spec:
        if not isinstance(item, list) or len(item) != 3:
            raise ValueError(f"{root}: invalid frozen fixture entry")
        name, sql, expected_rows = item
        if name in frozen or name not in EXPECTED_FIXTURES or not isinstance(sql, str):
            raise ValueError(f"{root}: duplicate or invalid frozen fixture")
        if name.startswith("union-width-") and expected_rows != int(name.rsplit("-", 1)[1]):
            raise ValueError(f"{root}: wrong width fixture row oracle")
        if not name.startswith("union-width-") and expected_rows is not None:
            raise ValueError(f"{root}: unexpected row oracle")
        if hashlib.sha256(sql.encode()).hexdigest() != fixtures[name]["query_sha256"]:
            raise ValueError(f"{root}: fixture SQL differs from reported identity")
        frozen[name] = sql
    if set(frozen) != EXPECTED_FIXTURES:
        raise ValueError(f"{root}: incomplete frozen fixture set")

    rows: dict[str, dict[str, list[tuple[int, int, int]]]] = {
        name: {"warmup": [], "measured": []} for name in fixtures
    }
    with csv_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != [
            "fixture", "phase", "run", "first_row_micros", "total_micros"
        ]:
            raise ValueError(f"{csv_path}: unexpected columns")
        for row in reader:
            fixture = row["fixture"]
            phase = row["phase"]
            if fixture not in rows or phase not in ("warmup", "measured"):
                raise ValueError(f"{csv_path}: unknown fixture or phase")
            sample = (
                int(row["run"]),
                int(row["first_row_micros"]),
                int(row["total_micros"]),
            )
            if sample[1] <= 0 or sample[2] < sample[1]:
                raise ValueError(f"{csv_path}: invalid latency sample")
            rows[fixture][phase].append(sample)

    for fixture, phases in rows.items():
        for phase, expected in (
            ("warmup", EXPECTED_WARMUPS),
            ("measured", EXPECTED_MEASURED),
        ):
            samples = phases[phase]
            if len(samples) != expected or sorted(index for index, _, _ in samples) != list(
                range(expected)
            ):
                raise ValueError(f"{csv_path}: incomplete {fixture}/{phase} samples")
    return {
        "identity": identity,
        "manifest": manifest,
        "fixture_spec_sha256": hashlib.sha256(fixture_bytes).hexdigest(),
        "identity_sha256": hashlib.sha256(identity_bytes).hexdigest(),
        "csv_sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
        "rows": rows,
    }


def describe(samples: list[tuple[int, int, int]], column: int) -> dict[str, int | float]:
    values = [sample[column] for sample in samples]
    return {
        "median_us": nearest_rank(values, 0.5),
        "p95_us": nearest_rank(values, 0.95),
        "p99_us": nearest_rank(values, 0.99),
        "maximum_us": max(values),
    }


def analyze(first: dict, second: dict) -> dict:
    a = first["identity"]
    b = second["identity"]
    if a["run_id"] == b["run_id"]:
        raise ValueError("A/A requires two independent run identities")
    stable_fields = (
        "source_revision",
        "source_tree_sha256",
        "native_build_identity",
        "config_sha256",
        "fixture_sha256",
        "tool_tree_sha256",
        "cargo_lock_sha256",
        "third_party_build_graph_sha256",
        "effective_launch_config_semantics_sha256",
        "build_profile",
        "platform",
    )
    if any(first["manifest"].get(field) != second["manifest"].get(field) for field in stable_fields):
        raise ValueError("A/A source, binary identity, fixture or configuration differs")
    fixtures_a = {item["fixture_id"]: item for item in a["fixtures"]}
    fixtures_b = {item["fixture_id"]: item for item in b["fixtures"]}
    if fixtures_a != fixtures_b:
        raise ValueError("A/A fixture SQL, plan shape or plan hashes differ")
    for field in ("measured_binary_sha256", "runner_binary_sha256"):
        if a[field] != b[field]:
            raise ValueError(f"A/A {field} differs")
    output = {
        "kind": "uea5d-startup-aa",
        "analyzer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "runs": [],
        "fixtures": {},
    }
    for run in (first, second):
        output["runs"].append(
            {
                "run_id": run["identity"]["run_id"],
                "run_manifest_sha256": run["identity"]["run_manifest_sha256"],
                "fixtures_sha256": run["identity_sha256"],
                "samples_sha256": run["csv_sha256"],
                "fixture_spec_sha256": run["fixture_spec_sha256"],
                "measured_binary_sha256": run["identity"]["measured_binary_sha256"],
                "runner_binary_sha256": run["identity"]["runner_binary_sha256"],
            }
        )
    for fixture in sorted(fixtures_a):
        pair = [first["rows"][fixture]["measured"], second["rows"][fixture]["measured"]]
        metrics = {}
        for name, column in (("first_row", 1), ("total", 2)):
            summaries = [describe(samples, column) for samples in pair]
            metrics[name] = {
                "runs": summaries,
                "median_spread_ratio": abs(
                    summaries[0]["median_us"] - summaries[1]["median_us"]
                ) / statistics.median(s["median_us"] for s in summaries),
                "p95_spread_ratio": abs(
                    summaries[0]["p95_us"] - summaries[1]["p95_us"]
                ) / statistics.median(s["p95_us"] for s in summaries),
                "p99_spread_ratio": abs(
                    summaries[0]["p99_us"] - summaries[1]["p99_us"]
                ) / statistics.median(s["p99_us"] for s in summaries),
            }
        output["fixtures"][fixture] = metrics
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--a", type=Path, required=True, help="first scenario artifact directory")
    parser.add_argument("--b", type=Path, required=True, help="second scenario artifact directory")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = analyze(read_run(args.a), read_run(args.b))
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
