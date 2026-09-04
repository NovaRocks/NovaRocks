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

"""Validate and combine the two owner-local NCP-8 AC32 metric reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ICEBERG_PREFIX = "NCP8_AC32_ICEBERG_IO="
CASES = {"empty_auxiliary", "theta_auxiliary"}
EXECUTION_OBSERVATIONS = {
    "throughput",
    "cpu",
    "peak_memory",
    "writer_queue_blocked_time",
    "final_aggregate_blocked_time",
    "exchange_encoded_payload_bytes",
    "exchange_sent_payload_bytes",
    "provider_calls_on_driver_thread",
}
ICEBERG_POSITIVE_OBSERVATIONS = {
    "puffin_write_calls",
    "puffin_write_bytes",
    "puffin_read_calls",
    "puffin_read_bytes",
}
ICEBERG_ZERO_OBSERVATIONS = {
    "data_input_opens",
    "data_exists_calls",
    "data_metadata_calls",
    "data_read_calls",
    "data_reader_calls",
    "data_read_bytes",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-jsonl", type=Path, required=True)
    parser.add_argument("--iceberg-test-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--execution-exit", type=int, required=True)
    parser.add_argument("--iceberg-exit", type=int, required=True)
    parser.add_argument("--source-clean", choices=("true", "false"), required=True)
    return parser.parse_args()


def read_execution(path: Path, issues: list[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not path.is_file():
        issues.append(f"execution report is missing: {path}")
        return records
    for number, line in enumerate(path.read_text().splitlines(), 1):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError as error:
            issues.append(f"execution JSON line {number} is invalid: {error}")
            continue
        if isinstance(value, dict) and value.get("record") in {
            "config",
            "sample",
            "summary",
        }:
            records.append(value)
    if not records:
        issues.append("execution report contains no benchmark records")
    return records


def read_iceberg(path: Path, issues: list[str]) -> dict[str, Any] | None:
    if not path.is_file():
        issues.append(f"Iceberg owner report is missing: {path}")
        return None
    reports = []
    for line in path.read_text().splitlines():
        if ICEBERG_PREFIX not in line:
            continue
        encoded = line.split(ICEBERG_PREFIX, 1)[1].strip()
        try:
            reports.append(json.loads(encoded))
        except json.JSONDecodeError as error:
            issues.append(f"Iceberg owner report is invalid JSON: {error}")
    if len(reports) != 1:
        issues.append(f"expected exactly one Iceberg owner report, got {len(reports)}")
        return reports[0] if reports else None
    return reports[0]


def require_observation(
    observations: Any,
    name: str,
    owner: str,
    issues: list[str],
) -> Any:
    if not isinstance(observations, dict) or name not in observations:
        issues.append(f"{owner} observer {name} is missing")
        return None
    observation = observations[name]
    if not isinstance(observation, dict):
        issues.append(f"{owner} observer {name} is not an object")
        return None
    if observation.get("observed") is not True:
        issues.append(f"{owner} observer {name} is not observed")
    if not isinstance(observation.get("source"), str) or not observation["source"]:
        issues.append(f"{owner} observer {name} has no source")
    if "value" not in observation:
        issues.append(f"{owner} observer {name} has no value")
        return None
    return observation["value"]


def validate_execution(records: list[dict[str, Any]], issues: list[str]) -> None:
    configs = [record for record in records if record.get("record") == "config"]
    if len(configs) != 1:
        issues.append(f"expected exactly one execution config, got {len(configs)}")
        return
    config = configs[0]
    if config.get("schema_version") != 3:
        issues.append("execution report schema_version must be 3")
    if config.get("measurement_scope") != "execution_writer_exchange_decode_finish":
        issues.append("execution measurement scope is missing or unexpected")
    warmup_rounds = config.get("warmup_rounds")
    measurement_rounds = config.get("measurement_rounds")
    if not isinstance(warmup_rounds, int) or isinstance(warmup_rounds, bool) or warmup_rounds < 1:
        issues.append("execution warmup_rounds must be a positive integer")
        warmup_rounds = 0
    if (
        not isinstance(measurement_rounds, int)
        or isinstance(measurement_rounds, bool)
        or measurement_rounds < 2
    ):
        issues.append("execution measurement_rounds must be at least two")
        measurement_rounds = 0
    publication = config.get("publication_io_observer")
    if not isinstance(publication, dict) or publication.get("observed") is not False:
        issues.append("execution report must explicitly leave publication I/O unobserved")

    samples = [record for record in records if record.get("record") == "sample"]
    sample_cases = {sample.get("case") for sample in samples}
    if sample_cases != CASES:
        issues.append(
            f"execution samples cover {sorted(str(case) for case in sample_cases)}, "
            f"expected {sorted(CASES)}"
        )
    expected_per_case = warmup_rounds + measurement_rounds
    for case in CASES:
        case_samples = [sample for sample in samples if sample.get("case") == case]
        count = len(case_samples)
        if count != expected_per_case:
            issues.append(
                f"execution sample count for {case} is {count}, expected {expected_per_case}"
            )
        for phase, expected in (
            ("warmup", warmup_rounds),
            ("measurement", measurement_rounds),
        ):
            actual = sum(sample.get("phase") == phase for sample in case_samples)
            if actual != expected:
                issues.append(
                    f"execution {phase} count for {case} is {actual}, expected {expected}"
                )
    for index, sample in enumerate(samples):
        owner = f"execution sample[{index}]"
        if sample.get("schema_version") != 3:
            issues.append(f"{owner} schema_version must be 3")
        observations = sample.get("observations")
        values = {
            name: require_observation(observations, name, owner, issues)
            for name in EXECUTION_OBSERVATIONS
        }
        encoded = values["exchange_encoded_payload_bytes"]
        sent = values["exchange_sent_payload_bytes"]
        throughput = values["throughput"]
        cpu = values["cpu"]
        peak_memory = values["peak_memory"]
        writer_blocked = values["writer_queue_blocked_time"]
        final_blocked = values["final_aggregate_blocked_time"]
        if (
            not isinstance(throughput, (int, float))
            or isinstance(throughput, bool)
            or throughput <= 0
        ):
            issues.append(f"{owner} throughput must be a positive number")
        if not isinstance(cpu, int) or isinstance(cpu, bool) or cpu <= 0:
            issues.append(f"{owner} CPU time must be a positive integer")
        if (
            not isinstance(peak_memory, int)
            or isinstance(peak_memory, bool)
            or peak_memory < 0
        ):
            issues.append(f"{owner} peak memory must be a non-negative integer")
        for name, value in (
            ("writer_queue_blocked_time", writer_blocked),
            ("final_aggregate_blocked_time", final_blocked),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                issues.append(f"{owner} {name} must be a non-negative integer")
        if not isinstance(encoded, int) or isinstance(encoded, bool) or encoded <= 0:
            issues.append(f"{owner} exchange encoded payload bytes must be positive")
        if not isinstance(sent, int) or isinstance(sent, bool) or sent <= 0:
            issues.append(f"{owner} exchange sent payload bytes must be positive")
        if encoded != sent:
            issues.append(f"{owner} exchange encoded/sent payload bytes differ")
        driver_calls = values["provider_calls_on_driver_thread"]
        if not isinstance(driver_calls, int) or isinstance(driver_calls, bool):
            issues.append(f"{owner} provider driver-thread call count must be an integer")
        elif driver_calls != 0:
            issues.append(f"{owner} polled a provider future on the driver thread")
    summary_records = [record for record in records if record.get("record") == "summary"]
    summaries = {record.get("case") for record in summary_records}
    if summaries != CASES or len(summary_records) != len(CASES):
        issues.append(f"execution summaries cover {sorted(summaries)}, expected {sorted(CASES)}")
    for summary in summary_records:
        if summary.get("measurement_rounds") != measurement_rounds:
            issues.append(
                f"execution summary for {summary.get('case')} has unexpected measurement_rounds"
            )


def validate_iceberg(report: dict[str, Any] | None, issues: list[str]) -> None:
    if report is None:
        return
    if report.get("record") != "iceberg_publication_io" or report.get(
        "schema_version"
    ) != 1:
        issues.append("Iceberg owner report identity or schema is invalid")
    if report.get("scope") != "one_eager_collect_on_write_attempt":
        issues.append("Iceberg owner report scope is missing or unexpected")
    observations = report.get("observations")
    for name in ICEBERG_POSITIVE_OBSERVATIONS:
        value = require_observation(observations, name, "Iceberg", issues)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            issues.append(f"Iceberg observer {name} must be positive")
    for name in ICEBERG_ZERO_OBSERVATIONS:
        value = require_observation(observations, name, "Iceberg", issues)
        if not isinstance(value, int) or isinstance(value, bool):
            issues.append(f"Iceberg observer {name} must be an integer")
        elif value != 0:
            issues.append(f"Iceberg observer {name} proves a data reread: {value}")
    for name in ("puffin_input_opens", "puffin_output_opens", "puffin_metadata_calls"):
        value = require_observation(observations, name, "Iceberg", issues)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            issues.append(f"Iceberg observer {name} must be positive")


def main() -> int:
    args = parse_args()
    issues: list[str] = []
    if args.execution_exit != 0:
        issues.append(f"execution benchmark exited with {args.execution_exit}")
    if args.iceberg_exit != 0:
        issues.append(f"Iceberg owner test exited with {args.iceberg_exit}")
    source_clean = args.source_clean == "true"
    if not source_clean:
        issues.append("source worktree is dirty; recorded revision is not an exact source identity")
    execution = read_execution(args.execution_jsonl, issues)
    iceberg = read_iceberg(args.iceberg_test_log, issues)
    validate_execution(execution, issues)
    validate_iceberg(iceberg, issues)

    report = {
        "record": "ncp8_ac32_composite_report",
        "schema_version": 1,
        "status": "complete" if not issues else "incomplete",
        "revision": args.revision,
        "source_clean": source_clean,
        "issues": issues,
        "sources": {
            "execution": {
                "observed": bool(execution),
                "source": str(args.execution_jsonl),
                "records": execution,
            },
            "iceberg_publication_io": {
                "observed": iceberg is not None,
                "source": str(args.iceberg_test_log),
                "record": iceberg,
            },
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if issues:
        for issue in issues:
            print(f"incomplete: {issue}")
        return 1
    print(f"complete AC32 report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
