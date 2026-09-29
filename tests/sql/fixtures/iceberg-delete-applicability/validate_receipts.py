#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Validate independent generation, planning, and row-read receipts.

Anomaly success/failure is an observation, never an expectation inferred from
writer behavior. Every planned task retains its original delete-list order and
multiplicity. The exported bytes permit another reader to reproduce the input.
"""

import argparse
import base64
import collections
import hashlib
import json
import pathlib
import re


def row_bag(rows: list) -> collections.Counter:
    return collections.Counter(json.dumps(row, separators=(",", ":"), sort_keys=True) for row in rows)


def validate(log: pathlib.Path, output: pathlib.Path, mode: str) -> dict:
    text = re.sub(r"\x1b\[[0-9;]*m", "", log.read_text())
    if "UEA4G_COMPLETE" not in text.splitlines():
        raise ValueError("Missing completion marker; Scala compilation or execution failed")
    records = [json.loads(line.removeprefix("UEA4G_RECEIPT "))
               for line in text.splitlines() if line.startswith("UEA4G_RECEIPT ")]
    runtime = [r for r in records if r["record"] == "runtime"]
    if len(runtime) != 1 or "1.11.0" not in runtime[0]["iceberg"]:
        raise ValueError("Expected exactly one Iceberg 1.11.0 runtime receipt")
    cases = [r for r in records if r["record"] == "case"]
    names = [r["case"] for r in cases]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate case names")
    expected = {f"same_commit_{kind}" for kind in ("position", "equality", "dv")}
    if mode == "promotion":
        expected = {f"promotion_{partition}_{scan}" for partition in ("partitioned", "unpartitioned")
                    for scan in ("before", "snapshot", "current_projection")}
    elif mode == "projection":
        expected = {"equality_promoted_type_fields_current_projection"}
    elif mode == "corpus":
        expected = {"same_commit_dv_equality", "same_commit_position_equality", "multi_target_position", "multi_blob_dv",
                    "legacy_position_before_upgrade", "legacy_position_dv_equality",
                    "cumulative_dv_from", "cumulative_dv_to", "data_sequence_rewrite",
                    "global_partition_field_groups", "same_puffin_endpoint_from", "same_puffin_endpoint_to",
                    "equivalent_position_from", "equivalent_dv_to",
                    "position_ranges_full", "position_ranges_missing_stats", "position_ranges_long_prefix",
                    "equality_null", "equality_nan_signed_zero", "equality_promoted_int",
                    "equality_promoted_float", "equality_disjoint_bounds", "equality_missing_bounds"}
    if mode == "anomalies":
        expected |= {f"{kind}_{variant}" for kind in ("position", "equality", "dv")
                     for variant in ("duplicate_same_manifest", "duplicate_cross_manifest",
                                     "duplicate_manifest_reference", "same_address_sequence",
                                     "same_address_record_count", "same_address_scope",
                                     "added_null_sequence", "existing_null_sequence", "deleted_null_sequence")}
        expected |= {f"{kind}_{variant}" for kind in ("position", "dv")
                     for variant in ("cross_partition", "cross_spec")}
        expected |= {"dv_older_than_exact_target", "dv_distinct_same_target",
                     "dv_same_address_reference", "dv_unmatched_target_control",
                     "position_explicit_reference_control", "position_explicit_cross_partition",
                     "position_explicit_cross_spec"}
        expected |= {"data_existing_null_sequence", "v1_sequence_zero", "same_commit_equality_wide",
                     "equality_same_address_fields", "equality_promoted_type_fields"}
    if set(names) != expected:
        raise ValueError(f"Incomplete matrix: missing={expected-set(names)}, extra={set(names)-expected}")
    for case in cases:
        for phase in ("java_planFiles", "java_row_read"):
            if case[phase]["status"] not in ("success", "error"):
                raise ValueError(f"{case['case']}: missing actual {phase} outcome")
            if case[phase]["status"] == "error" and not case[phase].get("error", {}).get("class"):
                raise ValueError(f"{case['case']}: missing exception identity")
        independent = case["independent_expected_rows"]
        if independent is not None:
            if case["java_planFiles"]["status"] != "success" or case["java_row_read"]["status"] != "success":
                raise ValueError(f"{case['case']}: compliant control failed")
            matches = row_bag(independent) == row_bag(case["java_row_read"]["rows"])
            case["independent_oracle_matched"] = matches
            if mode != "promotion" and not matches:
                raise ValueError(f"{case['case']}: independent row-bag mismatch")
            # A positive fixture must prove same-commit sequence semantics from
            # actual planned file facts, not merely use addRows/addDeletes calls.
            tasks = case["java_planFiles"]["tasks"]
            if case["case"] == "v1_sequence_zero":
                if any(t["data"]["data_sequence"] != 0 or t["data"]["file_sequence"] != 0 for t in tasks):
                    raise ValueError("v1 files must inherit sequence zero")
                continue
            if case["case"] not in {"same_commit_position", "same_commit_equality", "same_commit_dv",
                                    "same_commit_equality_wide"}:
                continue
            new_tasks = [t for t in tasks if t["data"]["data_sequence"] == 2]
            old_tasks = [t for t in tasks if t["data"]["data_sequence"] == 1]
            if len(new_tasks) != 1 or len(old_tasks) != 1:
                raise ValueError(f"{case['case']}: missing actual sequence 1/2 files")
            if case["case"].startswith("same_commit_equality"):
                if new_tasks[0]["deletes"] or not old_tasks[0]["deletes"]:
                    raise ValueError("Equality must apply to the old file only")
            elif len(new_tasks[0]["deletes"]) != 1 or new_tasks[0]["deletes"][0]["data_sequence"] != 2:
                raise ValueError("Position/DV must apply at equal data sequence")
    artifacts = []
    artifact_dir = output / "artifacts"
    artifact_dir.mkdir(exist_ok=True)
    for source in (r for r in records if r["record"] == "artifact"):
        item = source.copy()
        content = base64.b64decode(item.pop("base64"), validate=True)
        if len(content) != item["size"] or hashlib.sha256(content).hexdigest() != item["sha256"]:
            raise ValueError(f"Artifact digest mismatch: {item['path']}")
        suffix = ".metadata.json" if item["kind"] == "metadata" else pathlib.PurePosixPath(item["path"]).suffix
        relative = pathlib.Path("artifacts") / (item["sha256"] + suffix)
        (output / relative).write_bytes(content)
        item["local_path"] = str(relative)
        artifacts.append(item)
    registrations = [r["result"] for r in records if r["record"] == "registration"]
    artifact_paths = {artifact["path"] for artifact in artifacts}
    for case in cases:
        required_paths = {case["metadata"]}
        for task in case["java_planFiles"].get("tasks", case["java_planFiles"].get("partial_tasks", [])):
            required_paths.add(task["data"]["path"])
            required_paths.update(member["path"] for member in task["deletes"])
        if required_paths - artifact_paths:
            raise ValueError(f"{case['case']}: missing exported metadata/content artifacts")
    if mode == "anomalies" and len(registrations) != 1:
        raise ValueError("Missing REST registration capability observation")
    endpoints = [r for r in records if r["record"] == "endpoint-oracle"]
    if mode == "corpus" and {r["case"] for r in endpoints} != {
            "cumulative_dv", "same_puffin_endpoint_blobs", "equivalent_position_to_dv"}:
        raise ValueError("Incomplete endpoint oracle matrix")
    by_snapshot = {(case["table"], case["snapshot"]): case for case in cases}
    for endpoint in endpoints:
        lower = by_snapshot[(endpoint["table"], endpoint["from_snapshot"])]
        upper = by_snapshot[(endpoint["table"], endpoint["to_snapshot"])]
        old_rows, new_rows = (row_bag(case["java_row_read"]["rows"]) for case in (lower, upper))
        if old_rows - new_rows != row_bag(endpoint["independent_removed_rows"]):
            raise ValueError(f"{endpoint['case']}: endpoint removed row-bag mismatch")
        if new_rows - old_rows != row_bag(endpoint["independent_added_rows"]):
            raise ValueError(f"{endpoint['case']}: endpoint added row-bag mismatch")
    receipt = {"runtime": runtime[0], "mode": mode, "cases": cases,
               "artifacts": artifacts, "registrations": registrations, "endpoint_oracles": endpoints}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    for case in cases:
        planned = case["java_planFiles"]
        rows = case["java_row_read"]
        print(f"{case['case']}: plan={planned['status']} rows={rows['status']} independent={case.get('independent_oracle_matched')}")
    return receipt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--mode", choices=("positive", "anomalies", "corpus", "promotion", "projection"), required=True)
    args = parser.parse_args()
    validate(args.log, args.output, args.mode)
