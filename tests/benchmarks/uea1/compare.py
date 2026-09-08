#!/usr/bin/env python3
"""Compare positive UEA-1 metrics using the approved median/MAD protocol."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path


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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-a", type=Path, required=True)
    parser.add_argument("--baseline-b", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--resolution", type=float, required=True)
    parser.add_argument("--larger-is-better", action="store_true")
    args = parser.parse_args()
    result = compare(
        load_values(args.baseline_a),
        load_values(args.baseline_b),
        load_values(args.candidate),
        args.resolution,
        args.larger_is_better,
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("valid") and result.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
