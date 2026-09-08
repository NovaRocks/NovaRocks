#!/usr/bin/env python3
"""Compare positive UEA-1 metrics using the approved median/MAD protocol."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).parent))
from artifact_protocol import (  # noqa: E402
    ProtocolError,
    extract_comparison_input,
    validate_comparison_input,
)


def derive_epsilon(values: list[float], resolution: float) -> float:
    if not values or any(not math.isfinite(value) or value <= 0 for value in values):
        raise ValueError("positive metrics require finite non-zero samples")
    median = statistics.median(values)
    mad = statistics.median(abs(value - median) for value in values)
    return max(2 * resolution / median, 3 * 1.4826 * mad / median)


def compare(
    baseline_a: list[float],
    baseline_b: list[float],
    candidate: list[float],
    resolution: float,
    larger_is_better: bool,
) -> dict[str, float | bool | str]:
    pooled = baseline_a + baseline_b
    epsilon = derive_epsilon(pooled, resolution)
    if epsilon > 0.05:
        return {"valid": False, "reason": "baseline noise exceeds five percent", "epsilon": epsilon}
    if not candidate or any(not math.isfinite(value) or value <= 0 for value in candidate):
        raise ValueError("candidate positive metrics require finite non-zero samples")
    baseline_median = statistics.median(pooled)
    candidate_median = statistics.median(candidate)
    limit = baseline_median * (1 - epsilon if larger_is_better else 1 + epsilon)
    passed = candidate_median >= limit if larger_is_better else candidate_median <= limit
    return {
        "valid": True,
        "passed": passed,
        "epsilon": epsilon,
        "baseline_median": baseline_median,
        "candidate_median": candidate_median,
        "limit": limit,
    }


def load_values(path: Path) -> list[float]:
    value = json.loads(path.read_text())
    if not isinstance(value, list):
        raise ValueError(f"{path} must contain a JSON array")
    return [float(item) for item in value]


def _window_signature(document: dict) -> list[tuple[str, int, int, int]]:
    return [
        (
            window["workload"],
            window["window_index"],
            window["configured_concurrency"],
            window["duration_millis"],
        )
        for window in document["windows"]
    ]


def _metric_map(document: dict) -> dict[str, dict]:
    return {metric["id"]: metric for metric in document["metrics"]}


def _gate_map(document: dict) -> dict[str, dict]:
    return {gate["metric"]: gate for gate in document["absolute_gates"]}


def _validate_baseline_pair(baseline_a: dict, baseline_b: dict) -> tuple[dict, dict]:
    baseline_a = validate_comparison_input(baseline_a)
    baseline_b = validate_comparison_input(baseline_b)
    provenance_a = baseline_a["provenance"]
    provenance_b = baseline_b["provenance"]
    if provenance_a["run_id"] == provenance_b["run_id"]:
        raise ProtocolError("baseline A/A run identities must be distinct")
    if provenance_a["ended_unix_millis"] > provenance_b["started_unix_millis"]:
        raise ProtocolError("baseline A/A runs must be non-overlapping and ordered A then B")
    for field in (
        "source_sha",
        "binary_sha256",
        "runner_binary_sha256",
        "cargo_lock_sha256",
    ):
        if provenance_a[field] != provenance_b[field]:
            raise ProtocolError(f"baseline A/A {field} mismatch")
    if provenance_a["descriptor_sha256"] != provenance_b["descriptor_sha256"]:
        raise ProtocolError("baseline A/A descriptor_sha256 mismatch")
    if baseline_a["compatibility"] != baseline_b["compatibility"]:
        raise ProtocolError(
            "baseline A/A manifest, fixture, config, tool, third-party build graph, toolchain, scenario, or platform mismatch"
        )
    if _window_signature(baseline_a) != _window_signature(baseline_b):
        raise ProtocolError("baseline A/A measurement window count or shape mismatch")
    return baseline_a, baseline_b


def compare_baseline_noise(baseline_a: dict, baseline_b: dict) -> dict[str, object]:
    """Validate one baseline-only A/A pair and report per-metric noise."""

    baseline_a, baseline_b = _validate_baseline_pair(baseline_a, baseline_b)
    metric_a = _metric_map(baseline_a)
    metric_b = _metric_map(baseline_b)
    if set(metric_a) != set(metric_b):
        raise ProtocolError("baseline A/A relative metric set mismatch")

    results: list[dict[str, object]] = []
    stable = True
    for metric_id in sorted(metric_a):
        entry_a = metric_a[metric_id]
        entry_b = metric_b[metric_id]
        definition = lambda entry: (
            entry["metric"],
            entry["unit"],
            entry["direction"],
            entry["resolution"],
            entry["cohort"],
        )
        if definition(entry_a) != definition(entry_b):
            raise ProtocolError(f"baseline A/A metric definition mismatch for {metric_id}")
        pooled = entry_a["values"] + entry_b["values"]
        epsilon = derive_epsilon(pooled, entry_a["resolution"])
        metric_stable = epsilon <= 0.05
        stable = stable and metric_stable
        pooled_median = statistics.median(pooled)
        pooled_mad = statistics.median(abs(value - pooled_median) for value in pooled)
        results.append(
            {
                "id": metric_id,
                "unit": entry_a["unit"],
                "direction": entry_a["direction"],
                "resolution": entry_a["resolution"],
                "baseline_a_median": statistics.median(entry_a["values"]),
                "baseline_b_median": statistics.median(entry_b["values"]),
                "pooled_median": pooled_median,
                "pooled_mad": pooled_mad,
                "epsilon": epsilon,
                "stable": metric_stable,
                "reason": None
                if metric_stable
                else "baseline noise exceeds five percent",
            }
        )

    gate_a = _gate_map(baseline_a)
    gate_b = _gate_map(baseline_b)
    if set(gate_a) != set(gate_b):
        raise ProtocolError("baseline A/A absolute gate set mismatch")
    absolute_results: list[dict[str, object]] = []
    absolute_passed = True
    for metric in sorted(gate_a):
        entry_a = gate_a[metric]
        entry_b = gate_b[metric]
        definition_a = (entry_a["unit"], entry_a["direction"], entry_a["limit"])
        definition_b = (entry_b["unit"], entry_b["direction"], entry_b["limit"])
        if definition_a != definition_b:
            raise ProtocolError(
                f"baseline A/A absolute gate definition mismatch for {metric}"
            )
        passed = bool(entry_a["passed"]) and bool(entry_b["passed"])
        absolute_passed = absolute_passed and passed
        absolute_results.append(
            {
                "metric": metric,
                "unit": entry_a["unit"],
                "direction": entry_a["direction"],
                "limit": entry_a["limit"],
                "baseline_a_worst": max(entry_a["values"]),
                "baseline_b_worst": max(entry_b["values"]),
                "baseline_a_passed": bool(entry_a["passed"]),
                "baseline_b_passed": bool(entry_b["passed"]),
                "passed": passed,
            }
        )
    return {
        "mode": "baseline-noise",
        "valid": stable,
        "passed": stable and absolute_passed,
        "runs": {
            "baseline_a": baseline_a["provenance"]["run_id"],
            "baseline_b": baseline_b["provenance"]["run_id"],
        },
        "relative_metrics": results,
        "absolute_gates": absolute_results,
    }


def compare_protocol_inputs(
    baseline_a: dict, candidate_a: dict, baseline_b: dict, candidate_b: dict
) -> dict[str, object]:
    """Compare the required interleaved B0/A/B and candidate/A/B runs."""

    inputs = [
        validate_comparison_input(baseline_a),
        validate_comparison_input(candidate_a),
        validate_comparison_input(baseline_b),
        validate_comparison_input(candidate_b),
    ]
    baseline_a, candidate_a, baseline_b, candidate_b = inputs
    run_ids = [document["provenance"]["run_id"] for document in inputs]
    if len(set(run_ids)) != len(run_ids):
        raise ProtocolError("all four run identities must be distinct")
    for previous, following in zip(inputs, inputs[1:]):
        if previous["provenance"]["ended_unix_millis"] > following["provenance"][
            "started_unix_millis"
        ]:
            raise ProtocolError(
                "runs must be non-overlapping and ordered B0-A, candidate-A, B0-B, candidate-B"
            )
    for field in ("source_sha", "binary_sha256"):
        if baseline_a["provenance"][field] != baseline_b["provenance"][field]:
            raise ProtocolError(f"baseline A/A {field} mismatch")
        if candidate_a["provenance"][field] != candidate_b["provenance"][field]:
            raise ProtocolError(f"candidate A/A {field} mismatch")
        if candidate_a["provenance"][field] == baseline_a["provenance"][field]:
            raise ProtocolError(f"candidate {field} unexpectedly matches the baseline")
    if (
        baseline_a["provenance"]["runner_binary_sha256"]
        != baseline_b["provenance"]["runner_binary_sha256"]
    ):
        raise ProtocolError("baseline A/A runner_binary_sha256 mismatch")
    if (
        candidate_a["provenance"]["runner_binary_sha256"]
        != candidate_b["provenance"]["runner_binary_sha256"]
    ):
        raise ProtocolError("candidate A/A runner_binary_sha256 mismatch")
    if (
        baseline_a["provenance"]["cargo_lock_sha256"]
        != baseline_b["provenance"]["cargo_lock_sha256"]
    ):
        raise ProtocolError("baseline A/A cargo_lock_sha256 mismatch")
    if (
        candidate_a["provenance"]["cargo_lock_sha256"]
        != candidate_b["provenance"]["cargo_lock_sha256"]
    ):
        raise ProtocolError("candidate A/A cargo_lock_sha256 mismatch")
    if any(
        document["provenance"]["descriptor_sha256"]
        != baseline_a["provenance"]["descriptor_sha256"]
        for document in inputs[1:]
    ):
        raise ProtocolError("all four runs must use the same descriptor_sha256")
    if any(document["compatibility"] != baseline_a["compatibility"] for document in inputs[1:]):
        raise ProtocolError(
            "manifest, fixture, config, tool, third-party build graph, toolchain, scenario, or platform mismatch"
        )
    signatures = [_window_signature(document) for document in inputs]
    if any(signature != signatures[0] for signature in signatures[1:]):
        raise ProtocolError("measurement window count or shape mismatch")

    metric_maps = [_metric_map(document) for document in inputs]
    if any(set(mapping) != set(metric_maps[0]) for mapping in metric_maps[1:]):
        raise ProtocolError("relative metric set mismatch")
    relative_results: list[dict[str, object]] = []
    all_relative_valid = True
    all_relative_passed = True
    for metric_id in sorted(metric_maps[0]):
        entries = [mapping[metric_id] for mapping in metric_maps]
        spec = lambda entry: (
            entry["metric"],
            entry["unit"],
            entry["direction"],
            entry["resolution"],
            entry["cohort"],
        )
        if any(spec(entry) != spec(entries[0]) for entry in entries[1:]):
            raise ProtocolError(f"metric definition mismatch for {metric_id}")
        result = compare(
            entries[0]["values"],
            entries[2]["values"],
            entries[1]["values"] + entries[3]["values"],
            entries[0]["resolution"],
            entries[0]["direction"] == "higher_is_better",
        )
        relative_results.append({"id": metric_id, **result})
        all_relative_valid = all_relative_valid and bool(result.get("valid"))
        all_relative_passed = all_relative_passed and bool(result.get("passed"))

    gate_maps = [_gate_map(document) for document in inputs]
    if any(set(mapping) != set(gate_maps[0]) for mapping in gate_maps[1:]):
        raise ProtocolError("absolute gate set mismatch")
    absolute_results: list[dict[str, object]] = []
    all_absolute_passed = True
    for metric in sorted(gate_maps[0]):
        entries = [mapping[metric] for mapping in gate_maps]
        spec = lambda entry: (entry["unit"], entry["direction"], entry["limit"])
        if any(spec(entry) != spec(entries[0]) for entry in entries[1:]):
            raise ProtocolError(f"absolute gate definition mismatch for {metric}")
        passed = all(bool(entry["passed"]) for entry in entries)
        all_absolute_passed = all_absolute_passed and passed
        absolute_results.append(
            {
                "metric": metric,
                "unit": entries[2]["unit"],
                "limit": entries[2]["limit"],
                "baseline_a_worst": max(entries[0]["values"]),
                "baseline_b_worst": max(entries[2]["values"]),
                "candidate_a_worst": max(entries[1]["values"]),
                "candidate_b_worst": max(entries[3]["values"]),
                "baseline_a_passed": bool(entries[0]["passed"]),
                "candidate_a_passed": bool(entries[1]["passed"]),
                "baseline_b_passed": bool(entries[2]["passed"]),
                "candidate_b_passed": bool(entries[3]["passed"]),
                "passed": passed,
            }
        )
    return {
        "valid": all_relative_valid,
        "passed": all_relative_valid and all_relative_passed and all_absolute_passed,
        "runs": {
            "baseline_a": run_ids[0],
            "candidate_a": run_ids[1],
            "baseline_b": run_ids[2],
            "candidate_b": run_ids[3],
        },
        "relative_metrics": relative_results,
        "absolute_gates": absolute_results,
    }


def load_protocol_input(path: Path) -> dict:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ProtocolError(f"{path} must contain a structured comparison input")
    return validate_comparison_input(value)


def load_descriptor_input(path: Path) -> dict:
    """Re-extract a comparison input from raw, hash-bound artifacts."""

    return extract_comparison_input(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-a", type=Path, required=True)
    parser.add_argument("--baseline-b", type=Path, required=True)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--candidate-a", type=Path)
    parser.add_argument("--candidate-b", type=Path)
    parser.add_argument("--resolution", type=float)
    parser.add_argument("--larger-is-better", action="store_true")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--structured",
        action="store_true",
        help="compare the interleaved B0-A/candidate-A/B0-B/candidate-B inputs",
    )
    mode.add_argument(
        "--baseline-noise",
        action="store_true",
        help="validate baseline A/A identity and calculate the per-metric noise gate",
    )
    args = parser.parse_args()
    try:
        if args.baseline_noise:
            if (
                args.resolution is not None
                or args.larger_is_better
                or args.candidate is not None
                or args.candidate_a is not None
                or args.candidate_b is not None
            ):
                parser.error(
                    "baseline noise comparison accepts only --baseline-a and --baseline-b"
                )
            result = compare_baseline_noise(
                load_descriptor_input(args.baseline_a),
                load_descriptor_input(args.baseline_b),
            )
        elif args.structured:
            if (
                args.resolution is not None
                or args.larger_is_better
                or args.candidate is not None
                or args.candidate_a is None
                or args.candidate_b is None
            ):
                parser.error(
                    "structured comparison requires --candidate-a and --candidate-b and carries its own resolution and direction"
                )
            result = compare_protocol_inputs(
                load_descriptor_input(args.baseline_a),
                load_descriptor_input(args.candidate_a),
                load_descriptor_input(args.baseline_b),
                load_descriptor_input(args.candidate_b),
            )
        else:
            if args.resolution is None or args.candidate is None:
                parser.error("--resolution and --candidate are required for legacy array inputs")
            result = compare(
                load_values(args.baseline_a),
                load_values(args.baseline_b),
                load_values(args.candidate),
                args.resolution,
                args.larger_is_better,
            )
    except (OSError, json.JSONDecodeError, ProtocolError, ValueError) as error:
        result = {"valid": False, "passed": False, "reason": str(error)}
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("valid") and result.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
