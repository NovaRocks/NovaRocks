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

"""Run and audit the frozen UEA-4G two-pass native performance experiment.

Requires an already built runner and two release servers. Credentials are inherited
without being copied into receipts. No input generation, recording, retry, cache
flushing, threshold adjustment, or server build is performed here.

Exit codes: 0 passed, 1 correctness/latency failure, 2 invalid/incomplete evidence,
3 repeat_required (noisy controls). --prepare-only freezes commands without running
them. --compare-only audits an existing completed experiment without launching it.
"""

import argparse
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import signal
import statistics
import struct
import subprocess
import sys
import time


SCENARIO = "connector/iceberg-delete-performance"
SCENARIO_DIR = SCENARIO.replace("/", "-")
FIELDS = ("row_count", "sum_row_id", "sum_eq_key", "sum_partition_key", "sum_payload")
ORDER = tuple(
    (pass_id, cache, side)
    for pass_id, sides in ((1, ("baseline", "candidate")), (2, ("candidate", "baseline")))
    for cache in ("cold", "warm")
    for side in sides
)
CONTROL_NAMES = {"no_delete_n1", "no_delete_n10", "no_delete_n100", "no_delete_n1000", "equality_m1"}
WAVE_KEYS = set("case snapshot metadata cache side pass concurrency run warmup wave_elapsed_ns resource_window_start_ms resource_window_end_ms expected expected_aggregate_sha256 samples".split())
SAMPLE_KEYS = set("actor query_elapsed_ns aggregate aggregate_sha256 matched error_class".split())
SUMMARY_KEYS = set("min median p95_nearest_rank max mean coefficient_of_variation all_samples_retained".split())
CACHE_FLAGS = ("page_cache_enable", "parquet_page_cache_enable", "datacache_enable")
FIXTURE_ENV = {"NOVAROCKS_UEA4G_FIXTURE_ACCESS", "NOVAROCKS_UEA4G_FIXTURE_SECRET"}
RESOURCE_ROLES = ("fe", "be-0", "be-1", "be-2")
OUTPUT_FILENAMES = {
    "evidence": "delete-performance.json", "samples": "delete-performance-samples.jsonl",
    "objects": "delete-performance-objects.jsonl", "scenario": "scenario-evidence.json",
    "resource_envelope": "delete-performance-resources.json",
    "resource_samples": "delete-performance-resources.jsonl",
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def exact_keys(value, keys, label):
    require(isinstance(value, dict) and set(value) == keys, f"{label}: schema mismatch")


def integer(value, label, minimum=0):
    require(type(value) is int and value >= minimum, f"{label}: invalid integer")
    return value


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def json_no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def parse_json(text):
    return json.loads(text, object_pairs_hook=json_no_duplicates,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"non-finite JSON: {value}")))


def read_json(path):
    return parse_json(Path(path).read_text())


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def source_identity(directory, artifact_root):
    """Hash actual tracked and non-ignored source bytes, not only Git HEAD."""
    artifact_root = artifact_root.resolve()
    def git(*args):
        return subprocess.check_output(["git", "-C", str(directory), *args], stderr=subprocess.DEVNULL)
    root = Path(git("rev-parse", "--show-toplevel").decode().strip())
    paths = sorted(set(git("ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0")) - {b""})
    paths = [relative for relative in paths if not (root / os.fsdecode(relative)).is_relative_to(artifact_root)]
    status_args = ["status", "--porcelain", "--untracked-files=normal"]
    if artifact_root.is_relative_to(root):
        status_args.extend(["--", ".", ":(exclude)" + str(artifact_root.relative_to(root))])
    digest = hashlib.sha256()
    for relative in paths:
        path = root / os.fsdecode(relative)
        digest.update(relative + b"\0")
        if path.is_symlink():
            digest.update(b"symlink\0" + os.fsencode(os.readlink(path)))
        elif path.is_file():
            digest.update(b"file\0" + bytes.fromhex(sha256(path)))
        elif not path.exists():
            digest.update(b"deleted\0")
        else:
            raise ValueError(f"unsupported source entry: {path}")
    return {"root": str(root), "revision": git("rev-parse", "HEAD").decode().strip(),
            "dirty": bool(git(*status_args).strip()),
            "source_files_sha256": digest.hexdigest(), "file_count": len(paths)}


def file_identity(path):
    path = Path(path).resolve(strict=True)
    return {"path": str(path), "size": path.stat().st_size, "sha256": sha256(path)}


def aggregate_digest(value):
    exact_keys(value, set(FIELDS), "aggregate")
    integer(value["row_count"], "row_count")
    require(value["row_count"] < 2**64, "row_count overflows u64")
    for field in FIELDS[1:]:
        require(type(value[field]) is int and -(2**63) <= value[field] < 2**63, f"{field} overflows i64")
    return hashlib.sha256(struct.pack(">Qqqqq", *(value[field] for field in FIELDS))).hexdigest()


def summarize(values):
    require(bool(values), "empty latency sample")
    require(all(type(value) is int and value > 0 for value in values), "invalid elapsed nanoseconds")
    ordered = sorted(values)
    mean = statistics.fmean(values)
    return {"count": len(values), "min": ordered[0], "median": statistics.median(ordered),
            "p95_nearest_rank": ordered[math.ceil(.95 * len(ordered)) - 1], "max": ordered[-1],
            "mean": mean, "coefficient_of_variation": statistics.pstdev(values) / mean,
            "all_samples_retained": True}


def compare_summary(actual, expected):
    exact_keys(actual, SUMMARY_KEYS, "scenario latency summary")
    for key in SUMMARY_KEYS:
        if key == "all_samples_retained":
            require(actual[key] is True, "summary does not retain all samples")
        else:
            require(type(actual[key]) in (float, int) and math.isfinite(actual[key]) and
                    math.isclose(actual[key], expected[key], rel_tol=1e-10, abs_tol=1e-10),
                    f"summary {key} differs from raw samples")


def load_inputs(args):
    files = {name: file_identity(getattr(args, name)) for name in
             ("runner", "baseline_binary", "candidate_binary", "config", "native_manifest", "frozen_contract")}
    for name in ("runner", "baseline_binary", "candidate_binary"):
        require(os.access(files[name]["path"], os.X_OK), f"{name} is not executable")
    contract = read_json(args.frozen_contract)
    params = contract["parameters"]
    require(contract["production_topology"] == "1FE+3BE" and contract["performance_profile"] == "release",
            "frozen topology/profile differs")
    require(params["warmup_runs"] == 2 and params["measured_runs"] == 9 and params["query_concurrency"] == [1, 4],
            "frozen repetition/concurrency parameters differ")
    gates = contract["latency_gates"]
    require(gates == {"correct_baseline_no_delete_median_ratio_max": 1.15,
                      "correct_baseline_no_delete_p95_ratio_max": 1.25,
                      "correct_baseline_small_equality_median_ratio_max": 1.2}, "frozen latency gates differ")
    require(files["baseline_binary"]["sha256"] == contract["baseline_release_binary_sha256"],
            "baseline binary differs from G00")
    manifest = read_json(args.native_manifest)
    exact_keys(manifest, set("version corpus corpus_sha256 anomaly anomaly_sha256 scale scale_sha256 scale_artifacts_sha256 scale_java_plans_sha256 scale_java_delete_members_sha256 scale_cases rest_uri warehouse s3_endpoint".split()), "native manifest")
    require(manifest["version"] == 1, "unsupported native manifest version")
    base = Path(args.native_manifest).parent
    for name in ("corpus", "anomaly", "scale"):
        files[name] = file_identity(base / manifest[name])
        require(files[name]["sha256"] == manifest[name + "_sha256"], f"{name} SHA differs")
    scale_path = Path(files["scale"]["path"])
    for name, filename in (("scale_artifacts", "artifacts.jsonl"),
                           ("scale_java_plans", "java-plans.jsonl"),
                           ("scale_java_delete_members", "java-delete-members.jsonl")):
        files[name] = file_identity(scale_path.parent / filename)
        require(files[name]["sha256"] == manifest[name + "_sha256"], f"{name} SHA differs")
    scale = read_json(scale_path)
    files["scale_matrix"] = file_identity(scale_path.parent / "manifest.json")
    require(files["scale_matrix"]["sha256"] == scale["matrix_sha256"], "scale matrix SHA differs")
    matrix = read_json(files["scale_matrix"]["path"])
    require(matrix["rows_per_file"] == params["rows_per_file"] == 2048 and
            matrix["rows_per_equality_file"] == params["rows_per_equality_artifact"] == 256,
            "row counts differ from frozen parameters")
    require([c["data_files"] for c in matrix["cases"] if c["family"] == "no_delete"] == params["data_file_count"] and
            [c["equality_per_partition"] for c in matrix["cases"] if c["family"] == "equality_ladder"] ==
            [n for n in params["equality_artifacts_per_bucket"] if n != 0] and
            [c["suffixes"] for c in matrix["cases"] if c["family"] == "suffix"] == params["equality_checkpoint_suffixes"] and
            [c["partitions"] for c in matrix["cases"] if c["family"] == "buckets"] == params["partition_count"],
            "scale ladder parameters differ from G00")
    require(matrix["row_schema"] == [{"id": i, "name": name, "type": "long"}
                                    for i, name in enumerate(("row_id", "eq_key", "partition_key", "payload"), 1)],
            "scale row schema differs")
    cases = {case["case"]: case for case in scale["cases"]}
    require(len(scale["cases"]) == len(cases) == scale["case_count"] == 16, "missing/duplicate scale cases")
    require([c["configuration"] for c in scale["cases"]] == matrix["cases"], "scale parameters differ from matrix")
    require(CONTROL_NAMES <= cases.keys(), "missing control cases")
    require(len(set(manifest["scale_cases"])) == len(manifest["scale_cases"]) and
            set(manifest["scale_cases"]) <= cases.keys(), "invalid native case selectors")
    for case in cases.values():
        require(case["exact_bag_checked"] is True and case["java_oracle"] == case["independent_oracle"] and
                case["java_planFiles"] == case["java_row_read"] == "success", "scale oracle is unverified")
        case["expected_aggregate"] = {field: case["independent_oracle"][field] for field in FIELDS}
        aggregate_digest(case["expected_aggregate"])
    sources = {side: source_identity(Path(files[side + "_binary"]["path"]).parent, args.artifact_root)
               for side in ("baseline", "candidate")}
    require(sources["baseline"]["revision"] == contract["baseline_sha"], "baseline source revision differs from G00")
    files["driver_source"] = file_identity(__file__)
    return {"schema_version": 1, "files": files, "sources": sources,
            "platform": {"system": platform.system(), "machine": platform.machine(), "logical_cpus": os.cpu_count()},
            "parameters": params, "latency_gates": gates,
            "resource_sampling": {"sample_interval_ms": 50, "clock_resolution_ms": 1,
                                  "roles": list(RESOURCE_ROLES), "raw_envelope_schema": 3,
                                  "scope": "whole process; no per-index attribution or RSS release gate",
                                  "implementation": "cluster-harness ProcessResourceMonitor: four-role OS reads, with exact birth-identity checks before/after each role; macOS libproc and Linux procfs; no per-query subprocess",
                                  "observer_cost": "Fixed four-role OS sampling work plus 50 ms sleep per sweep on both sides; actual gaps are retained, and observation is not zero-cost."},
            "source_provenance_limit": "Source checkout hashes exclude the experiment artifact root and accompany supplied binaries; the driver does not prove which source bytes compiled a binary."}, cases, manifest


def run_spec(args, inputs, ordinal, labels):
    pass_id, cache, side = labels
    path = Path(args.artifact_root) / f"{ordinal:02d}-pass{pass_id}-{cache}-{side}"
    command = [inputs["files"]["runner"]["path"], "--only", SCENARIO,
               "--binary", inputs["files"][side + "_binary"]["path"],
               "--config", inputs["files"]["config"]["path"], "--artifact-root", str(path),
               "--cluster-size", "3", "--timeout-secs", "900", "--launch-profile", "performance"]
    environment = {"NOVAROCKS_UEA4G_PERF_CACHE": cache, "NOVAROCKS_UEA4G_PERF_SIDE": side,
                   "NOVAROCKS_UEA4G_PERF_PASS": str(pass_id),
                   "NOVAROCKS_UEA4G_NATIVE_MANIFEST": inputs["files"]["native_manifest"]["path"]}
    return {"ordinal": ordinal, "pass": pass_id, "cache": cache, "side": side,
            "artifact_root": str(path), "command": command, "environment_overrides": environment}


def audit_wave(wave, spec, cases):
    exact_keys(wave, WAVE_KEYS, "sample wave")
    for label in ("side", "pass", "cache"):
        require(wave[label] == spec[label], f"wrong {label} in sample")
    require(wave["case"] in cases, "unknown sample case")
    case = cases[wave["case"]]
    require(wave["snapshot"] == case["snapshot"] and wave["metadata"] == case["metadata"], "sample endpoint differs")
    concurrency = integer(wave["concurrency"], "concurrency", 1)
    require(concurrency in (1, 4), "unsupported concurrency")
    run = integer(wave["run"], "run")
    require(run < 11 and wave["warmup"] is (run < 2), "wrong warmup/measured index")
    integer(wave["wave_elapsed_ns"], "wave elapsed", 1)
    start = integer(wave["resource_window_start_ms"], "resource window start")
    end = integer(wave["resource_window_end_ms"], "resource window end")
    require(start <= end and wave["wave_elapsed_ns"] < (end - start + 1) * 1_000_000,
            "resource window does not enclose the timed wave at millisecond resolution")
    require(wave["expected"] == case["expected_aggregate"], "sample expected aggregate differs")
    require(wave["expected_aggregate_sha256"] == aggregate_digest(wave["expected"]), "expected aggregate hash differs")
    samples = wave["samples"]
    require(isinstance(samples, list) and len(samples) == concurrency, "missing actor samples")
    require({s["actor"] for s in samples} == set(range(concurrency)), "duplicate/missing actor IDs")
    for sample in samples:
        exact_keys(sample, SAMPLE_KEYS, "actor sample")
        integer(sample["actor"], "actor")
        integer(sample["query_elapsed_ns"], "query elapsed", 1)
        require(type(sample["matched"]) is bool, "non-boolean matched")
        if sample["aggregate"] is None:
            require(sample["aggregate_sha256"] is None and not sample["matched"] and
                    sample["error_class"] in ("query_failed", "aggregate_shape"), "invalid failed sample")
        else:
            require(sample["aggregate_sha256"] == aggregate_digest(sample["aggregate"]), "aggregate hash differs")
            matched = sample["aggregate"] == wave["expected"]
            require(sample["matched"] is matched and sample["error_class"] == (None if matched else "aggregate_mismatch"),
                    "sample correctness flag differs from aggregate")
    return (wave["case"], concurrency, run)


def decode_canonical_toml(node):
    exact_keys(node, {"type", "value"}, "canonical TOML")
    kind, value = node["type"], node["value"]
    if kind == "table":
        require(isinstance(value, dict), "invalid canonical TOML table")
        return {key: decode_canonical_toml(child) for key, child in value.items()}
    if kind == "array":
        require(isinstance(value, list), "invalid canonical TOML array")
        return [decode_canonical_toml(child) for child in value]
    expected = {"string": str, "datetime": str, "integer": int, "float": float, "boolean": bool}
    require(kind in expected and type(value) is expected[kind], "invalid canonical TOML scalar")
    require(kind != "float" or math.isfinite(value), "non-finite TOML scalar")
    return value


def canonical_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def audit_effective_config(scenario, cache):
    artifact = scenario["effective_launch_config"]
    exact_keys(artifact, {"schema_version", "semantics"}, "effective configuration artifact")
    require(artifact["schema_version"] == 1, "unknown effective configuration schema")
    semantics = artifact["semantics"]
    exact_keys(semantics, set("launch_profile cluster_size expected_eligible_backend_count fixed_environment child_environment native_proxy roles".split()), "effective configuration semantics")
    require(semantics["launch_profile"] == "performance" and semantics["cluster_size"] == 3 and
            semantics["expected_eligible_backend_count"] == 3, "effective native topology/profile differs")
    require(semantics["fixed_environment"] == {"NO_PROXY": "127.0.0.1,localhost"}, "fixed child environment differs")
    require(semantics["native_proxy"] == {"backends": []}, "performance native fault proxy is enabled")
    environment = semantics["child_environment"]
    exact_keys(environment, {"frontend", "backends"}, "child environment contract")
    require(len(environment["backends"]) == 3, "missing backend environment contracts")
    for bindings in [environment["frontend"], *environment["backends"]]:
        names = set()
        for binding in bindings:
            exact_keys(binding, {"name", "classification", "present"}, "environment binding")
            require(isinstance(binding["name"], str) and binding["name"] and binding["name"] not in names,
                    "duplicate/invalid environment binding")
            require(binding["classification"] == "secret-presence" and binding["present"] is True,
                    "unsealed or absent performance environment binding")
            names.add(binding["name"])
        require(FIXTURE_ENV <= names, "missing sealed fixture credentials")
    roles = semantics["roles"]
    require(len(roles) == 4 and {(r["role"], r["backend_index"]) for r in roles} ==
            {("fe", None), ("be", 0), ("be", 1), ("be", 2)}, "missing/duplicate effective roles")
    normalized = copy.deepcopy(artifact)
    for index, role in enumerate(roles):
        exact_keys(role, {"role", "backend_index", "effective_config", "attachments"}, "effective role")
        config = decode_canonical_toml(role["effective_config"])
        for flag in CACHE_FLAGS:
            require(config["runtime"]["cache"][flag] is (cache == "warm"), "effective cache flag differs from run label")
            # Only these three switches may differ between the cold and warm
            # experiments. Preserve every other typed TOML field and attachment.
            normalized["semantics"]["roles"][index]["effective_config"]["value"]["runtime"]["value"]["cache"]["value"][flag]["value"] = False
        credentials = [entry for entry in config["connector"]["credentials"] if entry.get("name") == "uea4g-fixture"]
        purpose = "object-store-metadata" if role["role"] == "fe" else "object-store-data"
        require(len(credentials) == 1 and credentials[0]["purpose"] == purpose,
                "fixture credential purpose differs from native role ownership")
        for credential in credentials:
            require(credential["generation"] == "v1" and credential["kind"] == "s3" and
                    credential["access_key_id"] == "${ENV:NOVAROCKS_UEA4G_FIXTURE_ACCESS}" and
                    credential["access_key_secret"] == "${ENV:NOVAROCKS_UEA4G_FIXTURE_SECRET}",
                    "fixture credential generation/reference differs")
    native_sha = scenario["effective_launch_config_sha256"]
    require(isinstance(native_sha, str) and len(native_sha) == 64 and all(c in "0123456789abcdef" for c in native_sha) and
            native_sha == scenario["effective_launch_config_semantics_sha256"], "schema-1 effective configuration hashes differ")
    # serde_json's struct field order is lost in the nested Value stored by the
    # runner. Compare a separately defined canonical body digest, not a guessed
    # reconstruction of Rust pretty-printed bytes or floating-point formatting.
    return {"canonical_body_sha256": canonical_digest(artifact),
            "without_cache_switches_sha256": canonical_digest(normalized), "native_sha256": native_sha}


def audit_resources(report, envelope_path, samples_path, identities, status, spec, waves):
    exact_keys(report, set("schema_version run_id sample_interval_ms clock_resolution_ms started_wall_ns ended_wall_ns queries_finished_ms convergence_finished_ms envelope_sha256 samples_sha256 sample_count roles scope sampling".split()), "resource evidence")
    run_id = f"uea4g-{spec['pass']}-{spec['cache']}-{spec['side']}"
    require(report["schema_version"] == 1 and report["run_id"] == run_id and
            report["sample_interval_ms"] == 50 and report["clock_resolution_ms"] == 1,
            "resource sampling contract differs")
    require(report["envelope_sha256"] == sha256(envelope_path) and
            report["samples_sha256"] == sha256(samples_path), "resource evidence hash mismatch")
    for field in ("started_wall_ns", "ended_wall_ns", "queries_finished_ms", "convergence_finished_ms", "sample_count"):
        integer(report[field], "resource " + field)
    require(status["started_wall_ns"] <= report["started_wall_ns"] <= report["ended_wall_ns"] <= status["ended_wall_ns"],
            "resource wall-clock window escapes the runner")
    require(report["queries_finished_ms"] <= report["convergence_finished_ms"], "resource convergence precedes queries")
    envelope = read_json(envelope_path)
    exact_keys(envelope, {"schema_version", "run_id", "processes", "samples"}, "resource envelope")
    require(envelope["schema_version"] == 3 and envelope["run_id"] == run_id, "resource envelope identity/schema differs")
    expected_processes = {identity["role"]: {key: identity[key] for key in ("pid", "process_start_token")}
                          for identity in identities}
    require(set(expected_processes) == set(RESOURCE_ROLES) and len(identities) == 4 and
            len({identity["pid"] for identity in identities}) == 4,
            "resource launch roles are missing or duplicated")
    require(envelope["processes"] == expected_processes, "resource identities differ from exact native launches")
    samples = envelope["samples"]
    require(isinstance(samples, list) and len(samples) == report["sample_count"], "resource sample count differs")
    require(len(samples) % 4 == 0 and all([sample["role"] for sample in samples[offset:offset + 4]] == sorted(RESOURCE_ROLES)
                                       for offset in range(0, len(samples), 4)), "incomplete/reordered four-role resource sweep")
    with Path(samples_path).open() as stream:
        require([parse_json(line) for line in stream] == samples, "resource JSONL differs from raw envelope")
    by_role = {role: [] for role in RESOURCE_ROLES}
    previous_elapsed = 0
    for sample in samples:
        exact_keys(sample, set("elapsed_millis role pid process_start_token rss_bytes threads cpu_user_nanos cpu_system_nanos unavailable_reason".split()), "resource sample")
        role = sample["role"]
        require(role in by_role and {key: sample[key] for key in ("pid", "process_start_token")} == expected_processes[role],
                "resource sample belongs to another process lifetime")
        elapsed = integer(sample["elapsed_millis"], "resource sample time")
        require(previous_elapsed <= elapsed <= status["elapsed_monotonic_ns"] // 1_000_000 + 1,
                "resource samples are unordered or outside runner duration")
        previous_elapsed = elapsed
        require(sample["unavailable_reason"] is None, "resource counters unavailable; never substitute zero")
        integer(sample["rss_bytes"], "process RSS", 1)
        for counter in ("cpu_user_nanos", "cpu_system_nanos"):
            integer(sample[counter], counter)
            require(not by_role[role] or by_role[role][-1][counter] <= sample[counter], "process CPU counter moved backwards")
        if sample["threads"] is not None:
            integer(sample["threads"], "process threads", 1)
        by_role[role].append(sample)
    require(waves, "missing resource wave windows")
    previous_end = 0
    for wave in waves:
        require(previous_end <= wave["resource_window_start_ms"] <= wave["resource_window_end_ms"] <= report["queries_finished_ms"],
                "resource wave windows overlap, reorder, or escape query phase")
        previous_end = wave["resource_window_end_ms"]
    measured = [wave for wave in waves if not wave["warmup"]]
    require(measured, "missing measured resource window")
    first_measured_start = measured[0]["resource_window_start_ms"]
    summaries = []
    for role, role_samples in by_role.items():
        require(len(role_samples) >= 2, "insufficient resource samples for " + role)
        first, last = role_samples[0], role_samples[-1]
        require(first["elapsed_millis"] <= first_measured_start and
                last["elapsed_millis"] >= report["convergence_finished_ms"],
                "resource samples do not bracket measurements and terminal convergence")
        threads = [sample["threads"] for sample in role_samples if sample["threads"] is not None]
        summaries.append({"role": role, **expected_processes[role], "sample_count": len(role_samples),
                          "first_sample_elapsed_ms": first["elapsed_millis"], "last_sample_elapsed_ms": last["elapsed_millis"],
                          "max_sample_gap_ms": max(b["elapsed_millis"] - a["elapsed_millis"] for a, b in zip(role_samples, role_samples[1:])),
                          "rss_high_water_bytes": max(sample["rss_bytes"] for sample in role_samples),
                          "threads_high_water": max(threads) if threads else None,
                          "cpu_user_delta_nanos": last["cpu_user_nanos"] - first["cpu_user_nanos"],
                          "cpu_system_delta_nanos": last["cpu_system_nanos"] - first["cpu_system_nanos"]})
    require(report["roles"] == summaries, "resource summary differs from raw process samples")
    return report


def audit_run(spec, inputs, cases, manifest):
    run_root = Path(spec["artifact_root"])
    status = read_json(run_root / "driver-run.json")
    require(status["spec"] == spec, "run command/order/environment differs")
    require(status["status"] in ("completed", "failed") and not status.get("interrupted"), "run did not complete")
    require(type(status["exit_code"]) is int and status["exit_code"] in (0, 1), "runner crashed or was interrupted")
    for field in ("started_wall_ns", "ended_wall_ns", "started_monotonic_ns", "elapsed_monotonic_ns"):
        integer(status[field], field, 1)
    require(status["ended_wall_ns"] >= status["started_wall_ns"] and status["elapsed_monotonic_ns"] > 0,
            "invalid driver timing")
    require(status["runner_log"] == file_identity(run_root / "runner.log"), "runner log changed")
    root = run_root / SCENARIO_DIR
    paths = {name: root / filename for name, filename in OUTPUT_FILENAMES.items()}
    hashes = {name: file_identity(path) for name, path in paths.items()}
    require(status["output_files"] == hashes, "run output files changed after completion")
    evidence = read_json(paths["evidence"])
    expected_keys = set("manifest_sha256 corpus_sha256 scale_sha256 scale_artifacts_sha256 binary_sha256 cache side pass warmup_runs measured_runs concurrencies timing cold_definition warm_definition aggregate_checksum_definition samples_sha256 object_evidence resource_evidence resource_error summary succeeded comparison_owner".split())
    exact_keys(evidence, expected_keys, "performance evidence")
    for label in ("side", "pass", "cache"):
        require(evidence[label] == spec[label], f"wrong evidence {label}")
    for key in ("corpus_sha256", "scale_sha256", "scale_artifacts_sha256"):
        require(evidence[key] == manifest[key], f"evidence {key} differs")
    require(evidence["manifest_sha256"] == inputs["files"]["native_manifest"]["sha256"] and
            evidence["binary_sha256"] == inputs["files"][spec["side"] + "_binary"]["sha256"], "binary/manifest mismatch")
    require(evidence["samples_sha256"] == hashes["samples"]["sha256"], "samples hash mismatch")
    require(evidence["warmup_runs"] == 2 and evidence["measured_runs"] == 9 and
            evidence["concurrencies"] == [1, 4] and type(evidence["succeeded"]) is bool, "incomplete/different experiment")
    objects = evidence["object_evidence"]
    exact_keys(objects, set("sha256 object_event_count gets heads upstream_bytes_read completed_response_bytes peak_inflight_reads event_overflow".split()), "object counters")
    require(objects["sha256"] == hashes["objects"]["sha256"] and objects["event_overflow"] == 0, "object evidence incomplete")
    for key in set(objects) - {"sha256"}:
        integer(objects[key], key)
    object_count = 0
    with paths["objects"].open() as stream:
        for line in stream:
            event = parse_json(line)
            require(event["record"] in ("object", "connection"), "unknown object event schema")
            exact_keys(event, set("record kind request_id connection_id elapsed_ms method object_id range bytes".split())
                       if event["record"] == "object" else set("record kind connection_id elapsed_ms".split()),
                       "object/connection event")
            object_count += event["record"] == "object"
    require(object_count == objects["object_event_count"], "object event count differs")
    scenario = read_json(paths["scenario"])
    require(scenario["schema_version"] == 5 and scenario["scenario"] == SCENARIO and
            scenario["launch_profile"] == "performance" and scenario["cluster_size"] == 3,
            "invalid native scenario evidence")
    require(scenario["base_config_sha256"] == inputs["files"]["config"]["sha256"] and
            scenario["command"] == spec["command"] and
            Path(scenario["primary_binary"]).resolve() == Path(inputs["files"][spec["side"] + "_binary"]["path"]),
            "native configuration/command/binary differs")
    identities = scenario["process_launch_identities"]
    require(len(identities) == 4 and {p["role"] for p in identities} == {"fe", "be-0", "be-1", "be-2"} and
            len({p["pid"] for p in identities}) == 4, "missing/duplicate native process identities")
    for identity in identities:
        integer(identity["pid"], "native PID", 1)
        require(isinstance(identity["process_start_token"], str) and identity["process_start_token"], "missing native start identity")
    effective = audit_effective_config(scenario, spec["cache"])
    rows = {}
    with paths["samples"].open() as stream:
        for line in stream:
            wave = parse_json(line)
            key = audit_wave(wave, spec, cases)
            require(key not in rows, "duplicate sample wave")
            rows[key] = wave
    expected = {(name, concurrency, run) for name in cases for concurrency in (1, 4) for run in range(11)}
    require(rows.keys() == expected, "missing/extra case, concurrency, or repetition samples")
    require(list(rows) == [(name, concurrency, run) for name in cases for concurrency in (1, 4) for run in range(11)],
            "sample wave order differs from the frozen matrix")
    require(evidence["resource_error"] is None, "resource sampler failed")
    resources = audit_resources(evidence["resource_evidence"], paths["resource_envelope"], paths["resource_samples"],
                                identities, status, spec, list(rows.values()))
    summary = evidence["summary"]
    exact_keys(summary, {"groups", "all_input_aggregates_match", "baseline_controls_match", "wrong_case_policy"}, "experiment summary")
    groups = {}
    for group in summary["groups"]:
        exact_keys(group, set("case cache concurrency correct comparable baseline_control incorrect_actor_samples query_sample_count query_ns wave_sample_count wave_ns".split()), "group summary")
        key = (group["case"], group["concurrency"])
        require(key not in groups and key[0] in cases and key[1] in (1, 4), "duplicate/unknown group summary")
        require(group["cache"] == spec["cache"] and group["query_sample_count"] == 9 * key[1] and
                group["wave_sample_count"] == 9, "summary sample count differs")
        waves = [rows[(*key, run)] for run in range(11)]
        query_ns = [sample["query_elapsed_ns"] for wave in waves[2:] for sample in wave["samples"]]
        wave_ns = [wave["wave_elapsed_ns"] for wave in waves[2:]]
        compare_summary(group["query_ns"], summarize(query_ns))
        compare_summary(group["wave_ns"], summarize(wave_ns))
        correct = all(s["matched"] for w in waves for s in w["samples"])
        require(group["correct"] is correct and group["comparable"] is correct and
                group["baseline_control"] is (key[0] in CONTROL_NAMES) and
                group["incorrect_actor_samples"] == sum(not s["matched"] for w in waves for s in w["samples"]),
                "group correctness summary differs from raw samples")
        groups[key] = {"query_ns": query_ns, "wave_ns": wave_ns, "correct": correct,
                       "resource_windows": [{k: w[k] for k in ("run", "warmup", "resource_window_start_ms", "resource_window_end_ms")}
                                            for w in waves],
                       "errors": [dict(run=w["run"], warmup=w["warmup"], **s)
                                  for w in waves for s in w["samples"] if not s["matched"]]}
    require(groups.keys() == {(name, c) for name in cases for c in (1, 4)}, "missing summary groups")
    all_correct = all(group["correct"] for group in groups.values())
    controls_correct = all(group["correct"] for key, group in groups.items() if key[0] in CONTROL_NAMES)
    require(summary["all_input_aggregates_match"] is all_correct and
            summary["baseline_controls_match"] is controls_correct, "aggregate acceptance summary differs")
    accepted = all_correct if spec["side"] == "candidate" else controls_correct
    require(evidence["succeeded"] is accepted and status["exit_code"] == (0 if accepted else 1) and
            status["status"] == ("completed" if accepted else "failed") and
            scenario["outcome"] == ("passed" if accepted else "failed") and
            scenario["exit_code"] == (0 if accepted else 1),
            "scenario exit differs from complete aggregate acceptance; possible infrastructure/cleanup failure")
    return {"spec": spec, "groups": groups, "files": hashes, "object_counters": objects, "resources": resources,
            "driver_run": file_identity(run_root / "driver-run.json"), "runner_log": status["runner_log"],
            "started_monotonic_ns": status["started_monotonic_ns"],
            "elapsed_monotonic_ns": status["elapsed_monotonic_ns"],
            "native_source": {key: scenario[key] for key in
                              ("source_revision", "source_dirty", "source_tree_sha256", "runner_native_build_identity")},
            "effective_configuration": effective}


def compare_runs(audits, cases):
    indexed = {(r["spec"]["pass"], r["spec"]["cache"], r["spec"]["side"]): r for r in audits}
    require(len(audits) == 8 and set(indexed) == set(ORDER), "missing/duplicate experiment runs")
    ordered = [indexed[key] for key in ORDER]
    require(len({audit["effective_configuration"]["without_cache_switches_sha256"] for audit in ordered}) == 1,
            "effective configuration differs across sides/passes beyond the three cache switches")
    for cache in ("cold", "warm"):
        same_cache = [audit["effective_configuration"] for audit in ordered if audit["spec"]["cache"] == cache]
        require(len({item["canonical_body_sha256"] for item in same_cache}) == 1 and
                len({item["native_sha256"] for item in same_cache}) == 1,
                "same-cache effective configuration differs across sides/passes")
    for previous, current in zip(ordered, ordered[1:]):
        require(previous["started_monotonic_ns"] + previous["elapsed_monotonic_ns"] <= current["started_monotonic_ns"],
                "actual run order differs from the frozen sequential order")
    comparisons, failures, noise = [], [], []
    for name in cases:
        control = name in CONTROL_NAMES
        for cache in ("cold", "warm"):
            for concurrency in (1, 4):
                samples = {(p, side): indexed[p, cache, side]["groups"][name, concurrency]
                           for p in (1, 2) for side in ("baseline", "candidate")}
                label = {"case": name, "cache": cache, "concurrency": concurrency}
                correct = {side: all(samples[p, side]["correct"] for p in (1, 2))
                           for side in ("baseline", "candidate")}
                item = dict(label, correct=correct, comparable=all(correct.values()),
                            control=control, passes=[], pooled={}, latency_gate=None)
                for p in (1, 2):
                    part = {"pass": p, "sides": {}}
                    for side in ("baseline", "candidate"):
                        group = samples[p, side]
                        query = summarize(group["query_ns"])
                        part["sides"][side] = {"query_ns": query, "wave_ns": summarize(group["wave_ns"]),
                                              "correct": group["correct"], "errors": group["errors"],
                                              "resource_windows": group["resource_windows"]}
                        if control and query["coefficient_of_variation"] > .10:
                            noise.append(dict(label, side=side, pass_id=p, coefficient_of_variation=query["coefficient_of_variation"]))
                    item["passes"].append(part)
                for side in ("baseline", "candidate"):
                    item["pooled"][side] = {
                        kind: summarize([v for p in (1, 2) for v in samples[p, side][kind]])
                        for kind in ("query_ns", "wave_ns")}
                    if control and item["pooled"][side]["query_ns"]["coefficient_of_variation"] > .10:
                        noise.append(dict(label, side=side, pass_id="pooled", coefficient_of_variation=item["pooled"][side]["query_ns"]["coefficient_of_variation"]))
                if not correct["candidate"] or (control and not correct["baseline"]):
                    failures.append(dict(label, reason="candidate_correctness" if not correct["candidate"] else "baseline_control_incorrect"))
                if item["comparable"]:
                    for part in item["passes"]:
                        part["ratios"] = {metric: part["sides"]["candidate"]["query_ns"][metric] /
                                          part["sides"]["baseline"]["query_ns"][metric]
                                          for metric in ("median", "p95_nearest_rank")}
                    item["pooled_ratios"] = {metric: item["pooled"]["candidate"]["query_ns"][metric] /
                                              item["pooled"]["baseline"]["query_ns"][metric]
                                              for metric in ("median", "p95_nearest_rank")}
                    if control:
                        limits = {"median": 1.2} if name == "equality_m1" else {"median": 1.15, "p95_nearest_rank": 1.25}
                        exceeded = {metric: ratio for metric, ratio in item["pooled_ratios"].items()
                                    if metric in limits and ratio > limits[metric]}
                        item["latency_gate"] = {"scope": "pooled measured actor query samples across both reversed-order passes",
                                                "limits": limits, "passed": not exceeded, "exceeded": exceeded}
                        if exceeded:
                            failures.append(dict(label, reason="latency_gate_exceeded", exceeded=exceeded))
                else:
                    item["noncomparable_reason"] = "At least one side returned incorrect aggregates; no latency ratio is computed."
                comparisons.append(item)
    correctness_failures = [f for f in failures if f["reason"] != "latency_gate_exceeded"]
    status = "failed" if correctness_failures else "repeat_required" if noise else "failed" if failures else "passed"
    return {"schema_version": 1, "status": status, "comparisons": comparisons, "failures": failures,
            "noisy_controls": noise, "noise_policy": "Control CV > 10% requires a new complete frozen experiment; thresholds never change and this driver never retries.",
            "percentile_definition": "Nearest rank ceil(0.95*n); median averages middle values; CV uses population standard deviation.",
            "aggregation": "Actor query_elapsed_ns samples are pooled across both passes; per-pass and wave summaries remain visible. Warmups never enter latency summaries.",
            "correctness_limit": "COUNT/SUM checksum is not an exact row-bag hash; frozen Java fixture evidence supplies independent exact row-bag verification.",
            "runs": [{k: v for k, v in audit.items() if k != "groups"} for audit in audits]}


def execute(spec):
    root = Path(spec["artifact_root"])
    root.mkdir()
    status = {"spec": spec, "started_wall_ns": time.time_ns(), "started_monotonic_ns": time.monotonic_ns(),
              "exit_code": None, "status": "running"}
    write_json(root / "driver-run.json", status)
    env = os.environ.copy()
    env.update(spec["environment_overrides"])
    with (root / "runner.log").open("wb") as log:
        process = subprocess.Popen(spec["command"], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            status["exit_code"] = process.wait()
        except KeyboardInterrupt:
            os.killpg(process.pid, signal.SIGINT)
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            status["exit_code"] = process.returncode
            status["interrupted"] = True
        finally:
            status["ended_wall_ns"] = time.time_ns()
            status["elapsed_monotonic_ns"] = time.monotonic_ns() - status["started_monotonic_ns"]
            status["status"] = "completed" if status["exit_code"] == 0 else "failed"
            status["runner_log"] = file_identity(root / "runner.log")
            status["output_files"] = {name: file_identity(root / SCENARIO_DIR / filename)
                                      for name, filename in OUTPUT_FILENAMES.items() if (root / SCENARIO_DIR / filename).is_file()}
            write_json(root / "driver-run.json", status)
    require(not status.get("interrupted"), "experiment interrupted; do not reuse its incomplete run order")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for flag in ("runner", "baseline-binary", "candidate-binary", "config", "native-manifest", "frozen-contract", "artifact-root"):
        parser.add_argument("--" + flag, type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--prepare-only", action="store_true", help="freeze inputs and commands without starting a process")
    mode.add_argument("--execute-prepared", action="store_true", help="execute an unchanged preparation; reject any prior run")
    mode.add_argument("--compare-only", action="store_true", help="revalidate completed receipts without starting a process")
    args = parser.parse_args()
    for key, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, key, value.resolve())
    inputs, cases, manifest = load_inputs(args)
    specs = [run_spec(args, inputs, i, labels) for i, labels in enumerate(ORDER, 1)]
    freeze = {"inputs": inputs, "run_order": specs}
    root = args.artifact_root
    if args.compare_only or args.execute_prepared:
        require(read_json(root / "experiment.json") == freeze, "current inputs/source/order differ from frozen experiment")
    else:
        require(not root.exists(), "artifact root already exists; use a fresh root or --compare-only")
        root.mkdir(parents=True)
        write_json(root / "experiment.json", freeze)
        if args.prepare_only:
            print(f"Prepared eight commands without execution: {root / 'experiment.json'}")
            return 0
    if not args.compare_only:
        require(all(not Path(spec["artifact_root"]).exists() for spec in specs), "a run already exists; no resumption/retry is allowed")
        for spec in specs:
            require(load_inputs(args)[0] == inputs, "inputs/source changed before next run; experiment stopped")
            print(f"Run {spec['ordinal']}/8: pass {spec['pass']} {spec['cache']} {spec['side']}", flush=True)
            execute(spec)
        require(load_inputs(args)[0] == inputs, "inputs/source changed during experiment")
    audits, errors = [], []
    for spec in specs:
        try:
            audits.append(audit_run(spec, inputs, cases, manifest))
        except (ValueError, KeyError, TypeError, OSError) as error:
            errors.append({"ordinal": spec["ordinal"], "side": spec["side"], "pass": spec["pass"],
                           "cache": spec["cache"], "error": str(error)})
    if errors:
        result = {"schema_version": 1, "status": "invalid_or_incomplete", "errors": errors,
                  "comparison": "No gate result is issued for incomplete or incomparable experiment inputs."}
    else:
        result = compare_runs(audits, cases)
    result["experiment_sha256"] = sha256(root / "experiment.json")
    result["generated_wall_ns"] = time.time_ns()
    write_json(root / "comparison.json", result)
    print(f"{result['status']}: {root / 'comparison.json'}")
    return {"passed": 0, "failed": 1, "invalid_or_incomplete": 2, "repeat_required": 3}[result["status"]]


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print(f"UEA-4G experiment rejected: {error}", file=sys.stderr)
        sys.exit(2)
