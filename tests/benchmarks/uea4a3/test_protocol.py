#!/usr/bin/env python3
"""Offline checks of the UEA-4A-3 comparison on synthetic receipts."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import compare  # noqa: E402

MANIFEST = Path(__file__).resolve().parent / "workload.json"


def samples(latency_ms: float, count: int, digest: str = "d0", error_every: int = 0,
            odd_digest: str | None = None) -> list[dict]:
    """One closed-loop client whose queries each take `latency_ms`; with
    `odd_digest`, its last query returns that result instead."""
    rows, clock = [], 0
    for index in range(count):
        failed = error_every and index % error_every == 0
        result = odd_digest if odd_digest and index == count - 1 else digest
        rows.append({"client": 0, "submitted_micros": clock,
                     "finished_micros": clock + int(latency_ms * 1000),
                     "rows": None if failed else 1,
                     "result_digest": None if failed else result,
                     "result_preview": None if failed else f"1 rows: {result}",
                     "error": "boom" if failed else None})
        clock += int(latency_ms * 1000)
    return rows


def workload(name: str, gated: bool, latency_ms: float, digest: str = "d0",
             error_every: int = 0, odd_digest: str | None = None) -> dict:
    windows = [{"repetition": repetition, "summary": {},
                "resources": [{"role": "be-0", "peak_rss_bytes": 1 << 30, "cpu_millis": 5000}],
                "backends": [{"backend": 0, "dispatch": None, "scan_pending": None}],
                "samples": samples(latency_ms, 50, digest, error_every,
                                   odd_digest if repetition == 1 else None)}
               for repetition in (1, 2, 3)]
    return {"name": name, "sql": "", "clients": 1, "session": [], "gated": gated,
            "warmup": {"errors": 0}, "windows": windows}


def control(loaded_ms: float) -> dict:
    view = lambda value: {"short_scan_ms": [value] * 60, "short_scan_errors": [],
                          "kill_ack_ms": [1.0] * 20, "kill_delivery_ms": [value] * 20,
                          "kill_invalid": []}
    return {"kill_query_uncancelled_ms": 5000.0, "idle": view(10.0), "loaded": view(loaded_ms),
            "background_samples": samples(100, 10), "heartbeat": "not measured",
            "runtime_filter": "not measured"}


class ComparisonTest(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = json.loads(MANIFEST.read_text())
        self.directory = tempfile.TemporaryDirectory()
        self.output = Path(self.directory.name)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def write(self, candidate_latency_ms: float, loaded_ms: float = 20.0,
              candidate_digest: str = "d0", candidate_errors: int = 0,
              b0_fixture: str = "f", b0_odd_digest: str | None = None) -> dict:
        manifest_sha = compare.sha256(MANIFEST)
        hashes = {"smoke": False, "manifest_path": str(MANIFEST), "manifest_sha256": manifest_sha,
                  "binary_sha256_by_side": {"b0": "b0bin", "candidate": "candbin"},
                  "b0_revision": "b0", "candidate_revision": "cand",
                  "order": [["default", "b0"]]}
        (self.output / "input-hashes.json").write_text(json.dumps(hashes))
        for group in self.manifest["config_groups"]:
            for side in compare.SIDES:
                root = self.output / group["name"] / side / compare.SCENARIO_DIR
                root.mkdir(parents=True)
                specs = {item["name"]: item for item in self.manifest["workloads"]}
                latency = 100.0 if side == "b0" else candidate_latency_ms
                digest = "d0" if side == "b0" else candidate_digest
                errors = 0 if side == "b0" else candidate_errors
                odd = b0_odd_digest if side == "b0" else None
                receipt = {
                    "side": side, "group": group["name"], "manifest_sha256": manifest_sha,
                    "binary_sha256": "b0bin" if side == "b0" else "candbin",
                    "fixture_sha256": b0_fixture if side == "b0" else "f",
                    "driver_workers": group["driver_workers"],
                    "workloads": [workload(name, specs[name]["gated"], latency, digest, errors, odd)
                                  for name in group["workloads"]],
                    "control": (control(20.0 if side == "b0" else loaded_ms)
                                if group["control"] else None),
                }
                (root / compare.RECEIPT).write_text(json.dumps(receipt))
                (root / "scenario-evidence.json").write_text(json.dumps({
                    "scenario": compare.SCENARIO, "outcome": "passed", "cluster_size": 3,
                    "launch_profile": "performance",
                    "effective_launch_config_semantics_sha256": group["name"]}))
        return compare.compare(self.output, MANIFEST)

    def test_equal_sides_pass(self) -> None:
        result = self.write(candidate_latency_ms=100.0)
        self.assertEqual(result["invalid"], [])
        self.assertTrue(result["passed"], result["verdict_line"])

    def test_throughput_regression_fails_the_gate(self) -> None:
        result = self.write(candidate_latency_ms=130.0)
        self.assertFalse(result["passed"])
        failed = {check["check"] for group in result["groups"] for row in group["workloads"]
                  for check in row["checks"] if not check["passed"]}
        self.assertEqual(failed, {"throughput_regression", "p95_growth"})

    def test_any_candidate_error_fails(self) -> None:
        result = self.write(candidate_latency_ms=100.0, candidate_errors=25)
        self.assertFalse(result["passed"])

    def test_a_different_result_fails(self) -> None:
        result = self.write(candidate_latency_ms=100.0, candidate_digest="d1")
        self.assertFalse(result["passed"])

    def test_control_ratio_to_idle_is_gated(self) -> None:
        result = self.write(candidate_latency_ms=100.0, loaded_ms=40.0)
        control = result["groups"][0]["control"]
        failed = {check["check"] for check in control["checks"] if not check["passed"]}
        self.assertEqual(failed, {"short_scan_p99_ratio_to_idle", "kill_p99_ratio_to_idle"})

    def test_a_b0_minority_result_is_reported_not_invalidating(self) -> None:
        result = self.write(candidate_latency_ms=100.0, b0_odd_digest="bad")
        self.assertEqual(result["invalid"], [])
        self.assertTrue(result["passed"], result["verdict_line"])
        row = result["groups"][0]["workloads"][0]
        self.assertEqual(row["reference_result"], "d0")
        self.assertEqual(row["b0_divergent_results"], {"bad": {"count": 1, "preview": "1 rows: bad"}})

    def test_different_inputs_invalidate_the_run(self) -> None:
        result = self.write(candidate_latency_ms=100.0, b0_fixture="other")
        self.assertEqual(result["verdict"], "作废")
        self.assertFalse(result["passed"])

    def test_nearest_rank(self) -> None:
        values = [float(value) for value in range(1, 21)]
        self.assertEqual(compare.nearest_rank(values, 0.95), 19.0)
        self.assertEqual(compare.nearest_rank(values, 0.99), 20.0)
        self.assertIsNone(compare.nearest_rank([], 0.5))


if __name__ == "__main__":
    unittest.main()
