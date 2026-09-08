#!/usr/bin/env python3
"""Extract and validate traceable UEA-1 performance comparison inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


INPUT_SCHEMA_VERSION = 1
INPUT_KIND = "uea1-performance-comparison-input"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SOURCE_RE = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
QUERY_RELATIVE_METRICS = {
    "first_row_p50_micros": ("microseconds", "lower_is_better"),
    "first_row_p95_micros": ("microseconds", "lower_is_better"),
    "first_row_p99_micros": ("microseconds", "lower_is_better"),
    "total_p50_micros": ("microseconds", "lower_is_better"),
    "total_p95_micros": ("microseconds", "lower_is_better"),
    "total_p99_micros": ("microseconds", "lower_is_better"),
    "throughput_queries_per_second": ("queries_per_second", "higher_is_better"),
}
RESOURCE_RELATIVE_METRICS = {
    "fe_peak_rss_bytes": ("bytes", "lower_is_better"),
    "max_be_peak_rss_bytes": ("bytes", "lower_is_better"),
}
RELATIVE_METRICS = QUERY_RELATIVE_METRICS | RESOURCE_RELATIVE_METRICS
WINDOW_QUERY_WORKLOADS = {
    "short": {"short"},
    "mixed": {"mixed-foreground"},
    "slow-output": {"slow-output-control", "slow-output-foreground"},
}
QUERY_WINDOW_WORKLOAD = {
    query_workload: window_workload
    for window_workload, query_workloads in WINDOW_QUERY_WORKLOADS.items()
    for query_workload in query_workloads
}
KNOWN_ABSOLUTE_METRICS = {
    "fe_peak_threads": "threads",
    "max_be_peak_threads": "threads",
}
FORMAL_ROLES = ["fe", "be-0", "be-1", "be-2"]


class ProtocolError(ValueError):
    """An artifact cannot prove that it is valid comparison evidence."""


def _expect_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError(f"{context} must be an object")
    return value


def _expect_exact_keys(value: dict[str, Any], keys: set[str], context: str) -> None:
    missing = sorted(keys - value.keys())
    unknown = sorted(value.keys() - keys)
    if missing or unknown:
        raise ProtocolError(f"{context} keys mismatch: missing={missing}, unknown={unknown}")


def _positive_number(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProtocolError(f"{context} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ProtocolError(f"{context} must be finite and positive")
    return result


def _nonnegative_number(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProtocolError(f"{context} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ProtocolError(f"{context} must be finite and nonnegative")
    return result


def _nonempty_string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProtocolError(f"{context} must be a nonempty string")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise ProtocolError(f"cannot hash {path}: {error}") from error
    return digest.hexdigest()


def _load_json(path: Path, context: str) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ProtocolError(f"cannot read {context} {path}: {error}") from error


def _percentile(values: list[float], percentile: int) -> float:
    if not values:
        raise ProtocolError("cannot derive a percentile from an empty window")
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile * len(ordered) / 100))
    return ordered[rank - 1]


def _cohort_key(window: dict[str, Any]) -> tuple[str, int]:
    return str(window["workload"]), int(window["configured_concurrency"])


def _metric_id(metric: str, cohort: tuple[str, int]) -> str:
    return f"{cohort[0]}@concurrency={cohort[1]}/{metric}"


def _validate_preparation_events(performance: dict[str, Any], scenario: str) -> None:
    if performance.get("preparation_events_status") != "available":
        raise ProtocolError("formal evidence requires available preparation events")
    events = performance.get("preparation_events")
    if not isinstance(events, list) or not events:
        raise ProtocolError("formal evidence requires nonempty preparation events")
    required_fields = {
        "work_id",
        "logical_execution_id",
        "attempt_id",
        "phase",
        "operation",
        "capability_path",
        "call_count",
        "elapsed_ns",
        "io_wait_ns",
        "cache_hit",
        "outcome",
    }
    known_phases = {
        "metadata_observation",
        "connector_planning_negotiation",
        "compile",
        "attempt_instantiation",
        "credential_refresh",
    }
    capability_paths = {
        "not-applicable",
        "static",
        "credentials-endpoint",
        "load-delegation",
        "staged",
    }
    successful_by_execution: dict[tuple[str, str], set[str]] = defaultdict(set)
    for offset, raw in enumerate(events):
        event = _expect_object(raw, f"preparation_events[{offset}]")
        _expect_exact_keys(event, required_fields, f"preparation_events[{offset}]")
        work_id = _nonempty_string(
            event["work_id"], f"preparation_events[{offset}].work_id"
        )
        logical_execution_id = _nonempty_string(
            event["logical_execution_id"],
            f"preparation_events[{offset}].logical_execution_id",
        )
        phase = _nonempty_string(event["phase"], f"preparation_events[{offset}].phase")
        if phase not in known_phases:
            raise ProtocolError(f"preparation_events[{offset}] has an unknown phase")
        _nonempty_string(event["operation"], f"preparation_events[{offset}].operation")
        if event["capability_path"] not in capability_paths:
            raise ProtocolError(
                f"preparation_events[{offset}] has an unknown capability path"
            )
        _positive_number(event["call_count"], f"preparation_events[{offset}].call_count")
        _positive_number(event["elapsed_ns"], f"preparation_events[{offset}].elapsed_ns")
        if event["io_wait_ns"] is not None:
            _nonnegative_number(
                event["io_wait_ns"], f"preparation_events[{offset}].io_wait_ns"
            )
        if event["cache_hit"] is not None and not isinstance(event["cache_hit"], bool):
            raise ProtocolError(f"preparation_events[{offset}].cache_hit must be boolean or null")
        attempt_id = event["attempt_id"]
        if phase in {"attempt_instantiation", "credential_refresh"}:
            _nonempty_string(attempt_id, f"preparation_events[{offset}].attempt_id")
        elif attempt_id is not None:
            raise ProtocolError(
                f"preparation_events[{offset}] preparation phase must not claim an attempt"
            )
        if event["outcome"] == "success":
            successful_by_execution[(work_id, logical_execution_id)].add(phase)
        elif event["outcome"] not in {"missing", "rejected", "failed", "cancelled"}:
            raise ProtocolError(f"preparation_events[{offset}] has an unknown outcome")
    required_by_scenario = {
        "performance/uea1-short-concurrent": {"compile", "attempt_instantiation"},
        "performance/uea1-mixed": {
            "metadata_observation",
            "connector_planning_negotiation",
            "compile",
            "attempt_instantiation",
        },
        "performance/uea1-slow-output": {"compile", "attempt_instantiation"},
    }
    required = required_by_scenario.get(scenario)
    if required is None:
        raise ProtocolError("formal evidence has an unsupported scenario")
    instantiated = [
        (identity, phases)
        for identity, phases in successful_by_execution.items()
        if "attempt_instantiation" in phases
    ]
    if not instantiated:
        raise ProtocolError("formal evidence contains no successfully instantiated execution")
    for identity, phases in instantiated:
        missing = sorted(required - phases)
        if missing:
            raise ProtocolError(
                f"formal execution {identity} is missing successful preparation phases {missing}"
            )


def extract_comparison_input(descriptor_path: Path) -> dict[str, Any]:
    descriptor = _expect_object(_load_json(descriptor_path, "descriptor"), "descriptor")
    _expect_exact_keys(
        descriptor,
        {
            "schema_version",
            "artifacts",
            "expected",
            "metric_resolutions",
            "absolute_gates",
        },
        "descriptor",
    )
    if descriptor["schema_version"] != INPUT_SCHEMA_VERSION:
        raise ProtocolError("unsupported descriptor schema_version")

    artifacts = _expect_object(descriptor["artifacts"], "artifacts")
    _expect_exact_keys(artifacts, {"run_manifest", "performance", "resources"}, "artifacts")
    artifact_paths: dict[str, Path] = {}
    for name in ("run_manifest", "performance", "resources"):
        artifact_path = Path(_nonempty_string(artifacts[name], f"artifacts.{name}"))
        if not artifact_path.is_absolute():
            artifact_path = descriptor_path.parent / artifact_path
        artifact_paths[name] = artifact_path
    performance = _expect_object(
        _load_json(artifact_paths["performance"], "performance artifact"),
        "performance artifact",
    )
    resources = _load_json(artifact_paths["resources"], "resource artifact")
    run_manifest = _expect_object(
        _load_json(artifact_paths["run_manifest"], "run manifest"),
        "run manifest",
    )
    if not isinstance(resources, list):
        raise ProtocolError("resource artifact must be an array")
    if performance.get("schema_version") != 3:
        raise ProtocolError("performance artifact must use schema_version 3")
    run_manifest_sha256 = _sha256_file(artifact_paths["run_manifest"])
    if (
        run_manifest.get("formal") is not True
        or run_manifest.get("status") != "completed"
        or run_manifest.get("exit_code") != 0
        or not run_manifest.get("ended_unix_millis")
        or run_manifest.get("source_dirty") is not False
    ):
        raise ProtocolError("run manifest does not describe a clean completed successful run")
    run_id = _nonempty_string(run_manifest.get("run_id"), "run manifest run_id")
    source_sha = _nonempty_string(
        run_manifest.get("source_revision"), "run manifest source_revision"
    )
    if not SOURCE_RE.fullmatch(source_sha):
        raise ProtocolError("run manifest source_revision must be a full lowercase Git SHA")
    hash_fields = {
        "binary": "binary_sha256",
        "manifest": "workload_manifest_sha256",
        "fixture": "fixture_sha256",
        "config": "config_sha256",
        "tool": "tool_tree_sha256",
        "cargo_lock": "cargo_lock_sha256",
    }
    input_hashes = {
        name: _nonempty_string(run_manifest.get(field), f"run manifest {field}")
        for name, field in hash_fields.items()
    }
    for name, value in input_hashes.items():
        if not SHA256_RE.fullmatch(value):
            raise ProtocolError(f"run manifest {hash_fields[name]} must be lowercase SHA-256")
    run_platform = _expect_object(run_manifest.get("platform"), "run manifest platform")
    platform = {
        "system": run_platform.get("os"),
        "release": run_platform.get("os_version"),
        "machine": run_platform.get("architecture"),
        "processor": run_platform.get("cpu_model"),
        "cpu_count": run_platform.get("logical_cpu_count"),
        "memory_bytes": run_platform.get("physical_memory_bytes"),
        "power_mode": run_platform.get("power_mode"),
    }
    for field in ("system", "release", "machine", "processor", "power_mode"):
        _nonempty_string(platform[field], f"run manifest platform.{field}")
    _positive_number(platform["cpu_count"], "run manifest platform.cpu_count")
    _positive_number(platform["memory_bytes"], "run manifest platform.memory_bytes")
    toolchain = json.dumps(
        {
            "rustc": _nonempty_string(
                run_manifest.get("rustc_version"), "run manifest rustc_version"
            ),
            "cargo": _nonempty_string(
                run_manifest.get("cargo_version"), "run manifest cargo_version"
            ),
        },
        sort_keys=True,
    )
    if performance.get("run_id") != run_id or run_manifest.get("run_id") != run_id:
        raise ProtocolError("performance and run manifest identity mismatch")
    if performance.get("run_manifest_sha256") != run_manifest_sha256:
        raise ProtocolError("performance artifact does not reference its exact run manifest")
    if performance.get("manifest_sha256") != input_hashes["manifest"]:
        raise ProtocolError("performance artifact manifest hash does not match run identity")
    expected = _expect_object(descriptor["expected"], "expected")
    _expect_exact_keys(expected, {"scenario", "window_count", "roles"}, "expected")
    scenario = _nonempty_string(expected["scenario"], "expected.scenario")
    if performance.get("scenario") != scenario or run_manifest.get("scenario") != scenario:
        raise ProtocolError("performance scenario does not match expected.scenario")
    if not isinstance(expected["window_count"], int) or expected["window_count"] <= 0:
        raise ProtocolError("expected.window_count must be a positive integer")
    roles = expected["roles"]
    if roles != FORMAL_ROLES:
        raise ProtocolError("formal evidence requires the exact 1FE+3BE role set")
    _validate_preparation_events(performance, scenario)

    windows_raw = performance.get("measurement_windows")
    samples_raw = performance.get("query_samples")
    if not isinstance(windows_raw, list) or not isinstance(samples_raw, list):
        raise ProtocolError("performance artifact is missing window or query samples")
    if len(windows_raw) != expected["window_count"]:
        raise ProtocolError(
            f"window count mismatch: expected {expected['window_count']}, got {len(windows_raw)}"
        )
    windows: list[dict[str, Any]] = []
    seen_window_indices: set[int] = set()
    for offset, raw in enumerate(windows_raw):
        window = _expect_object(raw, f"measurement_windows[{offset}]")
        required = {
            "workload",
            "window_index",
            "configured_concurrency",
            "started_elapsed_millis",
            "ended_elapsed_millis",
        }
        if not required.issubset(window):
            raise ProtocolError(f"measurement_windows[{offset}] is incomplete")
        workload = _nonempty_string(window["workload"], f"measurement_windows[{offset}].workload")
        index = window["window_index"]
        concurrency = window["configured_concurrency"]
        start = window["started_elapsed_millis"]
        end = window["ended_elapsed_millis"]
        if any(isinstance(value, bool) or not isinstance(value, int) for value in (index, concurrency, start, end)):
            raise ProtocolError(f"measurement_windows[{offset}] has non-integer coordinates")
        if index < 0 or concurrency <= 0 or start < 0 or end <= start:
            raise ProtocolError(f"measurement_windows[{offset}] has invalid coordinates")
        if index in seen_window_indices:
            raise ProtocolError(f"duplicate measurement window index {index}")
        seen_window_indices.add(index)
        windows.append(
            {
                "workload": workload,
                "window_index": index,
                "configured_concurrency": concurrency,
                "duration_millis": end - start,
                "started_elapsed_millis": start,
                "ended_elapsed_millis": end,
            }
        )

    windows_by_index = {window["window_index"]: window for window in windows}
    query_by_window: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    slow_observers_by_window: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for offset, raw in enumerate(samples_raw):
        sample = _expect_object(raw, f"query_samples[{offset}]")
        try:
            sample_workload = str(sample["workload"])
            window_index = int(sample["window_index"])
        except (KeyError, TypeError, ValueError) as error:
            raise ProtocolError(f"query_samples[{offset}] has no valid window identity") from error
        window = windows_by_index.get(window_index)
        if window is None:
            raise ProtocolError(f"query_samples[{offset}] refers to an unknown window")
        allowed = {
            **WINDOW_QUERY_WORKLOADS,
            "slow-output": WINDOW_QUERY_WORKLOADS["slow-output"]
            | {"slow-output-client"},
        }
        if sample_workload not in allowed.get(window["workload"], set()):
            raise ProtocolError(f"query_samples[{offset}] has the wrong workload for its window")
        sample_concurrency = sample.get("configured_concurrency")
        if (
            isinstance(sample_concurrency, bool)
            or not isinstance(sample_concurrency, int)
            or sample_concurrency != window["configured_concurrency"]
        ):
            raise ProtocolError(
                f"query_samples[{offset}] configured_concurrency does not match its window"
            )
        outcome = sample.get("outcome")
        end_micros = sample.get("ended_elapsed_micros")
        start_micros = sample.get("started_elapsed_micros")
        if not isinstance(start_micros, int) or not isinstance(end_micros, int):
            raise ProtocolError(f"query_samples[{offset}] has no monotonic start/end")
        window_start = window["started_elapsed_millis"] * 1000
        window_end = window["ended_elapsed_millis"] * 1000
        if outcome == "success":
            if sample_workload == "slow-output-client":
                raise ProtocolError(
                    f"query_samples[{offset}] slow-output client is diagnostic only"
                )
            if start_micros < window_start or end_micros > window_end:
                raise ProtocolError(f"query_samples[{offset}] claims success outside its window")
            key = (window_index, sample_workload)
            query_by_window[key].append(sample)
        elif outcome == "drained-after-window":
            if end_micros <= window_end:
                raise ProtocolError(f"query_samples[{offset}] drain completed inside its window")
        elif outcome == "window-observation" and sample_workload == "slow-output-client":
            if _positive_number(sample.get("bytes_read"), f"query_samples[{offset}].bytes_read") <= 0:
                raise ProtocolError(f"query_samples[{offset}] has no delivered bytes")
            slow_observers_by_window[window_index].append(sample)
        else:
            raise ProtocolError(f"query_samples[{offset}] has an unsupported outcome")

    resource_by_role: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for offset, raw in enumerate(resources):
        sample = _expect_object(raw, f"resources[{offset}]")
        role = sample.get("role")
        elapsed = sample.get("elapsed_millis")
        if not isinstance(role, str) or isinstance(elapsed, bool) or not isinstance(elapsed, int):
            raise ProtocolError(f"resources[{offset}] has invalid role or timestamp")
        resource_by_role[role].append(sample)

    resolutions = _expect_object(descriptor["metric_resolutions"], "metric_resolutions")
    if set(resolutions) != set(RELATIVE_METRICS):
        raise ProtocolError("metric_resolutions must cover the exact relative metric set")
    normalized_resolutions = {
        metric: _positive_number(value, f"metric_resolutions.{metric}")
        for metric, value in resolutions.items()
    }

    query_metric_values: dict[tuple[str, int], dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    resource_metric_values: dict[tuple[str, int], dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    per_window_threads: list[dict[str, float]] = []
    for window in windows:
        window_key = (window["workload"], window["window_index"])
        required_query_workloads = WINDOW_QUERY_WORKLOADS.get(window["workload"])
        if required_query_workloads is None:
            raise ProtocolError(f"window {window_key} has an unsupported workload")
        if window["workload"] == "slow-output" and len(
            slow_observers_by_window.get(window["window_index"], [])
        ) != 1:
            raise ProtocolError(
                f"window {window_key} must contain exactly one slow-output client observation"
            )
        duration_seconds = window["duration_millis"] / 1000
        for query_workload in sorted(required_query_workloads):
            query_samples = query_by_window.get(
                (window["window_index"], query_workload), []
            )
            if not query_samples:
                raise ProtocolError(
                    f"window {window_key} has no successful {query_workload} query samples"
                )
            first_rows = [
                _positive_number(
                    sample.get("first_row_micros"),
                    f"window {window_key} {query_workload} first row",
                )
                for sample in query_samples
            ]
            totals = [
                _positive_number(
                    sample.get("total_micros"),
                    f"window {window_key} {query_workload} total",
                )
                for sample in query_samples
            ]
            cohort = (query_workload, window["configured_concurrency"])
            derived = {
                "first_row_p50_micros": _percentile(first_rows, 50),
                "first_row_p95_micros": _percentile(first_rows, 95),
                "first_row_p99_micros": _percentile(first_rows, 99),
                "total_p50_micros": _percentile(totals, 50),
                "total_p95_micros": _percentile(totals, 95),
                "total_p99_micros": _percentile(totals, 99),
                "throughput_queries_per_second": len(query_samples) / duration_seconds,
            }
            for metric, value in derived.items():
                query_metric_values[cohort][metric].append(value)

        selected_resources: dict[str, list[dict[str, Any]]] = {}
        for role in roles:
            selected = [
                sample
                for sample in resource_by_role.get(role, [])
                if window["started_elapsed_millis"]
                <= sample["elapsed_millis"]
                <= window["ended_elapsed_millis"]
            ]
            if not selected:
                raise ProtocolError(f"window {window_key} has no resource sample for {role}")
            for sample in selected:
                if sample.get("unavailable_reason") is not None:
                    raise ProtocolError(
                        f"window {window_key} has unavailable resource data for {role}"
                    )
                _positive_number(
                    sample.get("rss_bytes"), f"window {window_key} {role} rss_bytes"
                )
                _positive_number(
                    sample.get("threads"), f"window {window_key} {role} threads"
                )
            selected_resources[role] = selected
        be_roles = [role for role in roles if role.startswith("be-")]
        resource_cohort = _cohort_key(window)
        resource_metric_values[resource_cohort]["fe_peak_rss_bytes"].append(
            max(float(sample["rss_bytes"]) for sample in selected_resources["fe"])
        )
        resource_metric_values[resource_cohort]["max_be_peak_rss_bytes"].append(
            max(
                float(sample["rss_bytes"])
                for role in be_roles
                for sample in selected_resources[role]
            )
        )
        per_window_threads.append(
            {
                "fe_peak_threads": max(float(sample["threads"]) for sample in selected_resources["fe"]),
                "max_be_peak_threads": max(
                    float(sample["threads"])
                    for role in be_roles
                    for sample in selected_resources[role]
                ),
            }
        )

    metrics: list[dict[str, Any]] = []
    for metric_values, metric_specs in (
        (query_metric_values, QUERY_RELATIVE_METRICS),
        (resource_metric_values, RESOURCE_RELATIVE_METRICS),
    ):
        for cohort in sorted(metric_values):
            for metric, (unit, direction) in metric_specs.items():
                values = metric_values[cohort].get(metric)
                if values is None:
                    raise ProtocolError(f"cohort {cohort} is missing metric {metric}")
                metrics.append(
                    {
                        "id": _metric_id(metric, cohort),
                        "metric": metric,
                        "unit": unit,
                        "direction": direction,
                        "resolution": normalized_resolutions[metric],
                        "cohort": {
                            "workload": cohort[0],
                            "configured_concurrency": cohort[1],
                        },
                        "values": values,
                    }
                )

    gates_raw = _expect_object(descriptor["absolute_gates"], "absolute_gates")
    if set(gates_raw) != set(KNOWN_ABSOLUTE_METRICS):
        raise ProtocolError("absolute_gates must contain the exact sampled gate set")
    gates: list[dict[str, Any]] = []
    for metric in sorted(gates_raw):
        gate = _expect_object(gates_raw[metric], f"absolute_gates.{metric}")
        allowed = {"unit", "direction", "limit"}
        if set(gate) != allowed:
            raise ProtocolError(f"absolute_gates.{metric} has invalid keys")
        unit = _nonempty_string(gate["unit"], f"absolute_gates.{metric}.unit")
        if gate["direction"] != "at_most":
            raise ProtocolError(f"absolute_gates.{metric}.direction must be at_most")
        limit = _nonnegative_number(gate["limit"], f"absolute_gates.{metric}.limit")
        if unit != KNOWN_ABSOLUTE_METRICS[metric]:
            raise ProtocolError(
                f"absolute_gates.{metric} must use sampled {KNOWN_ABSOLUTE_METRICS[metric]} values"
            )
        values = [sample[metric] for sample in per_window_threads]
        gates.append(
            {
                "metric": metric,
                "unit": unit,
                "direction": "at_most",
                "limit": limit,
                "values": values,
                "passed": all(value <= limit for value in values),
            }
        )

    result = {
        "schema_version": INPUT_SCHEMA_VERSION,
        "kind": INPUT_KIND,
        "provenance": {
            "run_id": run_id,
            "source_sha": source_sha,
            "binary_sha256": input_hashes["binary"],
            "descriptor_sha256": _sha256_file(descriptor_path),
            "performance_sha256": _sha256_file(artifact_paths["performance"]),
            "resources_sha256": _sha256_file(artifact_paths["resources"]),
            "run_manifest_sha256": run_manifest_sha256,
            "started_unix_millis": run_manifest["started_unix_millis"],
            "ended_unix_millis": run_manifest["ended_unix_millis"],
        },
        "compatibility": {
            "scenario": scenario,
            "manifest_sha256": input_hashes["manifest"],
            "fixture_sha256": input_hashes["fixture"],
            "config_sha256": input_hashes["config"],
            "tool_sha256": input_hashes["tool"],
            "cargo_lock_sha256": input_hashes["cargo_lock"],
            "platform": platform,
            "toolchain": toolchain,
            "build_profile": _nonempty_string(
                run_manifest.get("build_profile"), "run manifest build_profile"
            ),
        },
        "windows": windows,
        "metrics": metrics,
        "absolute_gates": gates,
    }
    validate_comparison_input(result)
    return result


def validate_comparison_input(document: Any) -> dict[str, Any]:
    model = _expect_object(document, "comparison input")
    _expect_exact_keys(
        model,
        {"schema_version", "kind", "provenance", "compatibility", "windows", "metrics", "absolute_gates"},
        "comparison input",
    )
    if model["schema_version"] != INPUT_SCHEMA_VERSION or model["kind"] != INPUT_KIND:
        raise ProtocolError("unsupported comparison input protocol")
    provenance = _expect_object(model["provenance"], "provenance")
    _expect_exact_keys(
        provenance,
        {"run_id", "source_sha", "binary_sha256", "descriptor_sha256", "performance_sha256", "resources_sha256", "run_manifest_sha256", "started_unix_millis", "ended_unix_millis"},
        "provenance",
    )
    _nonempty_string(provenance["run_id"], "provenance.run_id")
    if not SOURCE_RE.fullmatch(str(provenance["source_sha"])):
        raise ProtocolError("provenance.source_sha must be a full lowercase Git SHA")
    for field in ("binary_sha256", "descriptor_sha256", "performance_sha256", "resources_sha256", "run_manifest_sha256"):
        if not SHA256_RE.fullmatch(str(provenance[field])):
            raise ProtocolError(f"provenance.{field} must be lowercase SHA-256")
    started = provenance["started_unix_millis"]
    ended = provenance["ended_unix_millis"]
    if (
        isinstance(started, bool)
        or isinstance(ended, bool)
        or not isinstance(started, int)
        or not isinstance(ended, int)
        or started <= 0
        or ended < started
    ):
        raise ProtocolError("provenance start/end timestamps are invalid")

    compatibility = _expect_object(model["compatibility"], "compatibility")
    _expect_exact_keys(
        compatibility,
        {"scenario", "manifest_sha256", "fixture_sha256", "config_sha256", "tool_sha256", "cargo_lock_sha256", "platform", "toolchain", "build_profile"},
        "compatibility",
    )
    _nonempty_string(compatibility["scenario"], "compatibility.scenario")
    _nonempty_string(compatibility["toolchain"], "compatibility.toolchain")
    _nonempty_string(compatibility["build_profile"], "compatibility.build_profile")
    for field in ("manifest_sha256", "fixture_sha256", "config_sha256", "tool_sha256", "cargo_lock_sha256"):
        if not SHA256_RE.fullmatch(str(compatibility[field])):
            raise ProtocolError(f"compatibility.{field} must be lowercase SHA-256")
    platform = _expect_object(compatibility["platform"], "compatibility.platform")
    _expect_exact_keys(platform, {"system", "release", "machine", "processor", "cpu_count", "memory_bytes", "power_mode"}, "compatibility.platform")
    for field in ("system", "release", "machine", "processor", "power_mode"):
        _nonempty_string(platform[field], f"compatibility.platform.{field}")
    _positive_number(platform["cpu_count"], "compatibility.platform.cpu_count")
    _positive_number(platform["memory_bytes"], "compatibility.platform.memory_bytes")

    windows = model["windows"]
    if not isinstance(windows, list) or not windows:
        raise ProtocolError("comparison input requires measurement windows")
    cohort_counts: dict[tuple[str, int], int] = defaultdict(int)
    for offset, raw in enumerate(windows):
        window = _expect_object(raw, f"windows[{offset}]")
        _expect_exact_keys(
            window,
            {"workload", "window_index", "configured_concurrency", "duration_millis", "started_elapsed_millis", "ended_elapsed_millis"},
            f"windows[{offset}]",
        )
        workload = _nonempty_string(window["workload"], f"windows[{offset}].workload")
        if workload not in WINDOW_QUERY_WORKLOADS:
            raise ProtocolError(f"windows[{offset}].workload is unsupported")
        concurrency = window["configured_concurrency"]
        if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency <= 0:
            raise ProtocolError(f"windows[{offset}].configured_concurrency is invalid")
        _positive_number(window["duration_millis"], f"windows[{offset}].duration_millis")
        cohort_counts[(workload, concurrency)] += 1

    metrics = model["metrics"]
    if not isinstance(metrics, list) or not metrics:
        raise ProtocolError("comparison input requires relative metrics")
    metric_ids: set[str] = set()
    seen_by_cohort: dict[tuple[str, int], set[str]] = defaultdict(set)
    expected_resource_cohorts = set(cohort_counts)
    expected_query_cohorts = {
        (query_workload, concurrency)
        for (window_workload, concurrency) in cohort_counts
        for query_workload in WINDOW_QUERY_WORKLOADS[window_workload]
    }
    for offset, raw in enumerate(metrics):
        metric = _expect_object(raw, f"metrics[{offset}]")
        _expect_exact_keys(metric, {"id", "metric", "unit", "direction", "resolution", "cohort", "values"}, f"metrics[{offset}]")
        name = metric.get("metric")
        if name not in RELATIVE_METRICS:
            raise ProtocolError(f"metrics[{offset}] has an unknown metric")
        unit, direction = RELATIVE_METRICS[name]
        cohort = _expect_object(metric["cohort"], f"metrics[{offset}].cohort")
        _expect_exact_keys(cohort, {"workload", "configured_concurrency"}, f"metrics[{offset}].cohort")
        cohort_key = (str(cohort["workload"]), int(cohort["configured_concurrency"]))
        expected_id = _metric_id(name, cohort_key)
        if metric["id"] != expected_id or expected_id in metric_ids:
            raise ProtocolError(f"metrics[{offset}] has a duplicate or invalid id")
        metric_ids.add(expected_id)
        if metric["unit"] != unit or metric["direction"] != direction:
            raise ProtocolError(f"metrics[{offset}] unit or direction mismatch")
        _positive_number(metric["resolution"], f"metrics[{offset}].resolution")
        values = metric["values"]
        if name in QUERY_RELATIVE_METRICS:
            if cohort_key not in expected_query_cohorts:
                raise ProtocolError(f"metrics[{offset}] has an unexpected query cohort")
            window_workload = QUERY_WINDOW_WORKLOAD[cohort_key[0]]
            expected_value_count = cohort_counts[(window_workload, cohort_key[1])]
        else:
            if cohort_key not in expected_resource_cohorts:
                raise ProtocolError(f"metrics[{offset}] has an unexpected resource cohort")
            expected_value_count = cohort_counts[cohort_key]
        if not isinstance(values, list) or len(values) != expected_value_count:
            raise ProtocolError(f"metrics[{offset}] values do not match cohort windows")
        for value in values:
            _positive_number(value, f"metrics[{offset}].values")
        seen_by_cohort[cohort_key].add(name)
    for cohort in expected_query_cohorts:
        if seen_by_cohort[cohort] & set(QUERY_RELATIVE_METRICS) != set(
            QUERY_RELATIVE_METRICS
        ):
            raise ProtocolError(
                f"query cohort {cohort} does not contain the exact query metric set"
            )
    for cohort in expected_resource_cohorts:
        if seen_by_cohort[cohort] & set(RESOURCE_RELATIVE_METRICS) != set(
            RESOURCE_RELATIVE_METRICS
        ):
            raise ProtocolError(
                f"resource cohort {cohort} does not contain the exact resource metric set"
            )

    gates = model["absolute_gates"]
    if not isinstance(gates, list) or not gates:
        raise ProtocolError("comparison input requires absolute gates")
    gate_names: set[str] = set()
    for offset, raw in enumerate(gates):
        gate = _expect_object(raw, f"absolute_gates[{offset}]")
        _expect_exact_keys(gate, {"metric", "unit", "direction", "limit", "values", "passed"}, f"absolute_gates[{offset}]")
        name = _nonempty_string(gate["metric"], f"absolute_gates[{offset}].metric")
        if name in gate_names:
            raise ProtocolError(f"duplicate absolute gate {name}")
        gate_names.add(name)
        _nonempty_string(gate["unit"], f"absolute_gates[{offset}].unit")
        if gate["direction"] != "at_most":
            raise ProtocolError(f"absolute_gates[{offset}] direction must be at_most")
        limit = _nonnegative_number(gate["limit"], f"absolute_gates[{offset}].limit")
        values = gate["values"]
        if not isinstance(values, list) or len(values) != len(windows):
            raise ProtocolError(f"absolute_gates[{offset}] values must align with windows")
        normalized = [_nonnegative_number(value, f"absolute_gates[{offset}].values") for value in values]
        if gate["passed"] is not all(value <= limit for value in normalized):
            raise ProtocolError(f"absolute_gates[{offset}] passed flag is inconsistent")
    return model


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="operation", required=True)
    extract = subparsers.add_parser("extract")
    extract.add_argument("--descriptor", type=Path, required=True)
    extract.add_argument("--output", type=Path, required=True)
    check = subparsers.add_parser("check")
    check.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.operation == "extract":
            result = extract_comparison_input(args.descriptor)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        else:
            validate_comparison_input(_load_json(args.input, "comparison input"))
    except ProtocolError as error:
        print(json.dumps({"valid": False, "reason": str(error)}, sort_keys=True))
        return 1
    print(json.dumps({"valid": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
