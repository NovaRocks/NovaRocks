import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import unittest
from unittest import mock


MODULE_PATH = pathlib.Path(__file__).with_name("compare.py")
SPEC = importlib.util.spec_from_file_location("uea1_compare", MODULE_PATH)
COMPARE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(COMPARE)


HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def comparison_input(
    run_id,
    started,
    source_sha=HASH_A,
    binary_sha=HASH_B,
    runner_binary_sha=HASH_B,
    cargo_lock_sha=HASH_C,
    third_party_build_graph_sha=HASH_C,
):
    query_metrics = {
        "first_row_p50_micros": ("microseconds", "lower_is_better"),
        "first_row_p95_micros": ("microseconds", "lower_is_better"),
        "first_row_p99_micros": ("microseconds", "lower_is_better"),
        "total_p50_micros": ("microseconds", "lower_is_better"),
        "total_p95_micros": ("microseconds", "lower_is_better"),
        "total_p99_micros": ("microseconds", "lower_is_better"),
        "throughput_queries_per_second": (
            "queries_per_second",
            "higher_is_better",
        ),
    }
    resource_metrics = {
        "fe_peak_rss_bytes": ("bytes", "lower_is_better"),
        "max_be_peak_rss_bytes": ("bytes", "lower_is_better"),
    }
    metrics = []
    for name, (unit, direction) in (query_metrics | resource_metrics).items():
        metrics.append(
            {
                "id": f"short@concurrency=1/{name}",
                "metric": name,
                "unit": unit,
                "direction": direction,
                "resolution": 1.0,
                "cohort": {"workload": "short", "configured_concurrency": 1},
                "values": [100.0],
            }
        )
    return {
        "schema_version": 1,
        "kind": "uea1-performance-comparison-input",
        "provenance": {
            "run_id": run_id,
            "source_sha": source_sha,
            "binary_sha256": binary_sha,
            "runner_binary_sha256": runner_binary_sha,
            "cargo_lock_sha256": cargo_lock_sha,
            "descriptor_sha256": HASH_C,
            "performance_sha256": HASH_C,
            "resources_sha256": HASH_C,
            "run_manifest_sha256": HASH_C,
            "effective_launch_config_sha256": HASH_C,
            "fixture_realization_sha256": HASH_C,
            "completion_sha256": HASH_C,
            "started_unix_millis": started,
            "ended_unix_millis": started + 50,
        },
        "compatibility": {
            "scenario": "performance/uea1-short-concurrent",
            "manifest_sha256": HASH_C,
            "fixture_spec_sha256": HASH_C,
            "effective_launch_config_semantics_sha256": HASH_C,
            "fixture_realization_semantics_sha256": HASH_C,
            "tool_sha256": HASH_C,
            "third_party_build_graph_sha256": third_party_build_graph_sha,
            "platform": {
                "system": "TestOS",
                "release": "1",
                "machine": "test-machine",
                "processor": "test-cpu",
                "cpu_count": 8,
                "memory_bytes": 1024,
                "power_mode": "fixed",
            },
            "toolchain": "rustc-test",
            "build_profile": "release",
        },
        "windows": [
            {
                "workload": "short",
                "window_index": 0,
                "configured_concurrency": 1,
                "duration_millis": 1000,
                "started_elapsed_millis": 0,
                "ended_elapsed_millis": 1000,
            }
        ],
        "metrics": metrics,
        "absolute_gates": [
            {
                "metric": "fe_peak_threads",
                "unit": "threads",
                "direction": "at_most",
                "limit": 200,
                "values": [100],
                "passed": True,
            }
        ],
    }


class CompareTest(unittest.TestCase):
    def test_zero_noise_baseline_accepts_equal_candidate(self):
        result = COMPARE.compare([100.0, 100.0], [100.0, 100.0], [100.0], 1.0, False)
        self.assertTrue(result["valid"])
        self.assertTrue(result["passed"])
        self.assertEqual(result["epsilon"], 0.02)

    def test_unstable_baseline_is_invalid(self):
        result = COMPARE.compare([80.0, 120.0], [80.0, 120.0], [100.0], 0.001, False)
        self.assertFalse(result["valid"])
        self.assertIn("noise", result["reason"])

    def test_positive_metric_rejects_zero(self):
        with self.assertRaises(ValueError):
            COMPARE.compare([0.0], [1.0], [1.0], 0.001, False)

    def test_baseline_noise_accepts_matching_stable_runs(self):
        baseline_a = comparison_input("baseline-a", 100)
        baseline_b = comparison_input("baseline-b", 200)
        result = COMPARE.compare_baseline_noise(baseline_a, baseline_b)
        self.assertTrue(result["valid"])
        self.assertTrue(result["passed"])
        self.assertEqual(result["mode"], "baseline-noise")
        self.assertEqual(len(result["relative_metrics"]), 9)
        self.assertTrue(all(metric["stable"] for metric in result["relative_metrics"]))
        self.assertEqual(result["relative_metrics"][0]["epsilon"], 0.02)
        self.assertEqual(result["absolute_gates"][0]["baseline_a_worst"], 100)
        self.assertEqual(result["absolute_gates"][0]["baseline_b_worst"], 100)
        self.assertTrue(result["absolute_gates"][0]["passed"])

    def test_baseline_noise_rejects_high_noise(self):
        baseline_a = comparison_input("baseline-a", 100)
        baseline_b = comparison_input("baseline-b", 200)
        baseline_a["metrics"][0]["values"] = [80.0]
        baseline_b["metrics"][0]["values"] = [120.0]
        result = COMPARE.compare_baseline_noise(baseline_a, baseline_b)
        self.assertFalse(result["valid"])
        unstable = [metric for metric in result["relative_metrics"] if not metric["stable"]]
        self.assertEqual(len(unstable), 1)
        self.assertIn("five percent", unstable[0]["reason"])

    def test_baseline_noise_rejects_reversed_or_overlapping_runs(self):
        with self.assertRaisesRegex(COMPARE.ProtocolError, "ordered A then B"):
            COMPARE.compare_baseline_noise(
                comparison_input("baseline-a", 300),
                comparison_input("baseline-b", 100),
            )
        overlapping_a = comparison_input("baseline-a", 100)
        overlapping_a["provenance"]["ended_unix_millis"] = 250
        with self.assertRaisesRegex(COMPARE.ProtocolError, "non-overlapping"):
            COMPARE.compare_baseline_noise(
                overlapping_a,
                comparison_input("baseline-b", 200),
            )

    def test_baseline_noise_rejects_source_binary_and_compatibility_mismatch(self):
        baseline_a = comparison_input("baseline-a", 100)
        cases = [
            ("source_sha", HASH_C, "source_sha mismatch"),
            ("binary_sha256", HASH_C, "binary_sha256 mismatch"),
            ("runner_binary_sha256", HASH_C, "runner_binary_sha256 mismatch"),
            ("cargo_lock_sha256", HASH_B, "cargo_lock_sha256 mismatch"),
        ]
        for field, value, message in cases:
            with self.subTest(field=field):
                baseline_b = comparison_input("baseline-b", 200)
                baseline_b["provenance"][field] = value
                with self.assertRaisesRegex(COMPARE.ProtocolError, message):
                    COMPARE.compare_baseline_noise(baseline_a, baseline_b)
        baseline_b = comparison_input("baseline-b", 200)
        baseline_b["compatibility"]["tool_sha256"] = "d" * 64
        with self.assertRaisesRegex(COMPARE.ProtocolError, "manifest, fixture, config"):
            COMPARE.compare_baseline_noise(baseline_a, baseline_b)

    def test_baseline_noise_rejects_zero_and_missing_metric(self):
        baseline_a = comparison_input("baseline-a", 100)
        baseline_b = comparison_input("baseline-b", 200)
        baseline_b["metrics"][0]["values"] = [0.0]
        with self.assertRaisesRegex(COMPARE.ProtocolError, "finite and positive"):
            COMPARE.compare_baseline_noise(baseline_a, baseline_b)
        baseline_b = comparison_input("baseline-b", 200)
        baseline_b["metrics"].pop()
        with self.assertRaisesRegex(COMPARE.ProtocolError, "exact resource metric set"):
            COMPARE.compare_baseline_noise(baseline_a, baseline_b)

    def test_baseline_noise_requires_identical_descriptor(self):
        baseline_a = comparison_input("baseline-a", 100)
        baseline_b = comparison_input("baseline-b", 200)
        baseline_b["provenance"]["descriptor_sha256"] = "d" * 64
        with self.assertRaisesRegex(COMPARE.ProtocolError, "descriptor_sha256 mismatch"):
            COMPARE.compare_baseline_noise(baseline_a, baseline_b)

    def test_baseline_noise_checks_absolute_gate_set_and_definition(self):
        baseline_a = comparison_input("baseline-a", 100)
        baseline_b = comparison_input("baseline-b", 200)
        baseline_b["absolute_gates"].append(
            {
                "metric": "queue_entries",
                "unit": "entries",
                "direction": "at_most",
                "limit": 10,
                "values": [1],
                "passed": True,
            }
        )
        with self.assertRaisesRegex(COMPARE.ProtocolError, "gate set mismatch"):
            COMPARE.compare_baseline_noise(baseline_a, baseline_b)

        baseline_b = comparison_input("baseline-b", 200)
        baseline_b["absolute_gates"][0]["limit"] = 201
        with self.assertRaisesRegex(COMPARE.ProtocolError, "gate definition mismatch"):
            COMPARE.compare_baseline_noise(baseline_a, baseline_b)

    def test_baseline_absolute_gate_failure_is_valid_but_does_not_pass(self):
        baseline_a = comparison_input("baseline-a", 100)
        baseline_b = comparison_input("baseline-b", 200)
        baseline_b["absolute_gates"][0]["values"] = [201]
        baseline_b["absolute_gates"][0]["passed"] = False
        result = COMPARE.compare_baseline_noise(baseline_a, baseline_b)
        self.assertTrue(result["valid"])
        self.assertFalse(result["passed"])
        self.assertTrue(result["absolute_gates"][0]["baseline_a_passed"])
        self.assertFalse(result["absolute_gates"][0]["baseline_b_passed"])

    def test_four_run_structured_comparison_remains_available(self):
        baseline_a = comparison_input("baseline-a", 100)
        candidate_a = comparison_input(
            "candidate-a",
            200,
            source_sha="d" * 64,
            binary_sha="e" * 64,
            runner_binary_sha="f" * 64,
        )
        baseline_b = comparison_input("baseline-b", 300)
        candidate_b = comparison_input(
            "candidate-b",
            400,
            source_sha="d" * 64,
            binary_sha="e" * 64,
            runner_binary_sha="f" * 64,
        )
        result = COMPARE.compare_protocol_inputs(
            baseline_a, candidate_a, baseline_b, candidate_b
        )
        self.assertTrue(result["valid"])
        self.assertTrue(result["passed"])
        self.assertEqual(result["runs"]["candidate_b"], "candidate-b")

    def test_four_run_structured_comparison_requires_identical_descriptor(self):
        baseline_a = comparison_input("baseline-a", 100)
        candidate_a = comparison_input(
            "candidate-a", 200, source_sha="d" * 64, binary_sha="e" * 64
        )
        baseline_b = comparison_input("baseline-b", 300)
        candidate_b = comparison_input(
            "candidate-b", 400, source_sha="d" * 64, binary_sha="e" * 64
        )
        candidate_b["provenance"]["descriptor_sha256"] = "f" * 64
        with self.assertRaisesRegex(COMPARE.ProtocolError, "same descriptor_sha256"):
            COMPARE.compare_protocol_inputs(
                baseline_a, candidate_a, baseline_b, candidate_b
            )

    def test_four_run_requires_runner_identity_within_each_source(self):
        baseline_a = comparison_input("baseline-a", 100)
        candidate_a = comparison_input(
            "candidate-a",
            200,
            source_sha="d" * 64,
            binary_sha="e" * 64,
            runner_binary_sha="f" * 64,
        )
        baseline_b = comparison_input("baseline-b", 300)
        candidate_b = comparison_input(
            "candidate-b",
            400,
            source_sha="d" * 64,
            binary_sha="e" * 64,
            runner_binary_sha=HASH_C,
        )
        with self.assertRaisesRegex(
            COMPARE.ProtocolError, "candidate A/A runner_binary_sha256 mismatch"
        ):
            COMPARE.compare_protocol_inputs(
                baseline_a, candidate_a, baseline_b, candidate_b
            )

    def test_four_run_requires_raw_cargo_lock_within_each_source(self):
        baseline_a = comparison_input("baseline-a", 100)
        candidate_a = comparison_input(
            "candidate-a", 200, source_sha="d" * 64, binary_sha="e" * 64
        )
        baseline_b = comparison_input("baseline-b", 300, cargo_lock_sha=HASH_B)
        candidate_b = comparison_input(
            "candidate-b", 400, source_sha="d" * 64, binary_sha="e" * 64
        )
        with self.assertRaisesRegex(
            COMPARE.ProtocolError, "baseline A/A cargo_lock_sha256 mismatch"
        ):
            COMPARE.compare_protocol_inputs(
                baseline_a, candidate_a, baseline_b, candidate_b
            )

    def test_four_run_requires_cross_source_third_party_build_graph(self):
        baseline_a = comparison_input("baseline-a", 100)
        candidate_a = comparison_input(
            "candidate-a",
            200,
            source_sha="d" * 64,
            binary_sha="e" * 64,
            third_party_build_graph_sha=HASH_B,
        )
        baseline_b = comparison_input("baseline-b", 300)
        candidate_b = comparison_input(
            "candidate-b",
            400,
            source_sha="d" * 64,
            binary_sha="e" * 64,
            third_party_build_graph_sha=HASH_B,
        )
        with self.assertRaisesRegex(COMPARE.ProtocolError, "build graph, toolchain"):
            COMPARE.compare_protocol_inputs(
                baseline_a, candidate_a, baseline_b, candidate_b
            )

    def test_four_run_absolute_gate_requires_baseline_preflight(self):
        baseline_a = comparison_input("baseline-a", 100)
        baseline_a["absolute_gates"][0]["values"] = [201]
        baseline_a["absolute_gates"][0]["passed"] = False
        candidate_a = comparison_input(
            "candidate-a", 200, source_sha="d" * 64, binary_sha="e" * 64
        )
        baseline_b = comparison_input("baseline-b", 300)
        candidate_b = comparison_input(
            "candidate-b", 400, source_sha="d" * 64, binary_sha="e" * 64
        )
        result = COMPARE.compare_protocol_inputs(
            baseline_a, candidate_a, baseline_b, candidate_b
        )
        self.assertTrue(result["valid"])
        self.assertFalse(result["passed"])
        self.assertFalse(result["absolute_gates"][0]["baseline_a_passed"])

    def test_structured_cli_routes_all_four_descriptors_through_extraction(self):
        documents = [
            comparison_input("baseline-a", 100),
            comparison_input(
                "candidate-a", 200, source_sha="d" * 64, binary_sha="e" * 64
            ),
            comparison_input("baseline-b", 300),
            comparison_input(
                "candidate-b", 400, source_sha="d" * 64, binary_sha="e" * 64
            ),
        ]
        arguments = [
            "compare.py",
            "--structured",
            "--baseline-a",
            "b0-a/descriptor.json",
            "--candidate-a",
            "candidate-a/descriptor.json",
            "--baseline-b",
            "b0-b/descriptor.json",
            "--candidate-b",
            "candidate-b/descriptor.json",
        ]
        output = io.StringIO()
        with mock.patch.object(
            COMPARE, "load_descriptor_input", side_effect=documents
        ) as loader, mock.patch.object(
            sys, "argv", arguments
        ), contextlib.redirect_stdout(output):
            exit_code = COMPARE.main()
        self.assertEqual(exit_code, 0)
        self.assertEqual(loader.call_count, 4)
        self.assertTrue(json.loads(output.getvalue())["passed"])


if __name__ == "__main__":
    unittest.main()
