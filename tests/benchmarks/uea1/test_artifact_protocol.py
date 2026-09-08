import copy
import hashlib
import importlib.util
import json
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).parent


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


PROTOCOL = load_module("uea1_artifact_protocol", "artifact_protocol.py")
COMPARE = load_module("uea1_structured_compare", "compare.py")


def sha(value):
    return hashlib.sha256(value).hexdigest()


class ArtifactFixture:
    def __init__(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary.name)
        self.run_id = "1" * 64
        self.source = "a" * 40
        self.hashes = {name: sha(name.encode()) for name in (
            "binary", "runner", "manifest", "fixture", "config", "tool", "lock", "tree"
        )}
        self.run_manifest = {
            "schema_version": 2,
            "formal": True,
            "run_id": self.run_id,
            "scenario": "performance/uea1-short-concurrent",
            "command": ["novarocks-system-tests"],
            "started_unix_millis": 1,
            "ended_unix_millis": 2,
            "exit_code": 0,
            "status": "completed",
            "source_revision": self.source,
            "native_build_identity": self.source,
            "source_tree_sha256": self.hashes["tree"],
            "source_dirty": False,
            "binary_sha256": self.hashes["binary"],
            "runner_executable_path": "/checkout/target/release/novarocks-system-tests",
            "runner_executable_sha256": self.hashes["runner"],
            "config_sha256": self.hashes["config"],
            "workload_manifest_sha256": self.hashes["manifest"],
            "fixture_sha256": self.hashes["fixture"],
            "tool_tree_sha256": self.hashes["tool"],
            "cargo_lock_sha256": self.hashes["lock"],
            "rustc_version": "rustc test",
            "cargo_version": "cargo test",
            "build_profile": "release",
            "platform": {
                "os": "test-os",
                "os_version": "1",
                "architecture": "test-arch",
                "cpu_model": "test-cpu",
                "logical_cpu_count": 8,
                "physical_memory_bytes": 16_000,
                "power_mode": "fixed",
            },
        }
        self.performance = {
            "schema_version": 4,
            "run_id": self.run_id,
            "run_manifest_sha256": "",
            "manifest_sha256": self.hashes["manifest"],
            "scenario": "performance/uea1-short-concurrent",
            "query_samples": [{
                "workload": "short",
                "window_index": 0,
                "configured_concurrency": 1,
                "client": 0,
                "started_elapsed_micros": 110_000,
                "ended_elapsed_micros": 190_000,
                "first_row_micros": 100,
                "total_micros": 200,
                "rows": 1,
                "bytes_read": None,
                "outcome": "success",
            }],
            "measurement_windows": [{
                "workload": "short",
                "window_index": 0,
                "configured_concurrency": 1,
                "started_elapsed_millis": 100,
                "ended_elapsed_millis": 200,
                "drain_ended_elapsed_millis": 201,
            }],
            "preparation_diagnostic": {
                "schema_version": 1,
                "run_token": self.run_id,
                "started_elapsed_micros": 10_000,
                "ended_elapsed_micros": 90_000,
            },
            "preparation_events_status": "available",
            "preparation_events": [
                {
                    "work_id": "work-1",
                    "logical_execution_id": "logical-1",
                    "attempt_id": None,
                    "phase": "compile",
                    "operation": "compile-query",
                    "capability_path": "not-applicable",
                    "call_count": 1,
                    "elapsed_ns": 10,
                    "io_wait_ns": None,
                    "cache_hit": None,
                    "outcome": "success",
                },
                {
                    "work_id": "work-1",
                    "logical_execution_id": "logical-1",
                    "attempt_id": "attempt-1",
                    "phase": "attempt_instantiation",
                    "operation": "instantiate-attempt",
                    "capability_path": "not-applicable",
                    "call_count": 1,
                    "elapsed_ns": 10,
                    "io_wait_ns": None,
                    "cache_hit": None,
                    "outcome": "success",
                },
            ],
        }
        self.resources = [
            {"elapsed_millis": 150, "role": "fe", "pid": 1, "rss_bytes": 1000, "threads": 4, "unavailable_reason": None},
            {"elapsed_millis": 150, "role": "be-0", "pid": 2, "rss_bytes": 2000, "threads": 5, "unavailable_reason": None},
            {"elapsed_millis": 150, "role": "be-1", "pid": 3, "rss_bytes": 2100, "threads": 5, "unavailable_reason": None},
            {"elapsed_millis": 150, "role": "be-2", "pid": 4, "rss_bytes": 2200, "threads": 5, "unavailable_reason": None},
        ]
        self.descriptor = {
            "schema_version": 1,
            "artifacts": {
                "run_manifest": "run-manifest.json",
                "performance": "uea1-performance.json",
                "resources": "process-resources.json",
            },
            "expected": {
                "scenario": "performance/uea1-short-concurrent",
                "window_count": 1,
                "roles": list(PROTOCOL.FORMAL_ROLES),
            },
            "metric_resolutions": {
                name: (0.001 if "throughput" in name else 0.1)
                for name in PROTOCOL.RELATIVE_METRICS
            },
            "absolute_gates": {
                "fe_peak_threads": {"unit": "threads", "direction": "at_most", "limit": 10},
                "max_be_peak_threads": {"unit": "threads", "direction": "at_most", "limit": 10},
            },
        }

    def write(self):
        run_path = self.root / "run-manifest.json"
        run_path.write_text(json.dumps(self.run_manifest))
        self.performance["run_manifest_sha256"] = hashlib.sha256(run_path.read_bytes()).hexdigest()
        (self.root / "uea1-performance.json").write_text(json.dumps(self.performance))
        (self.root / "process-resources.json").write_text(json.dumps(self.resources))
        descriptor = self.root / "descriptor.json"
        descriptor.write_text(json.dumps(self.descriptor))
        return descriptor

    def extract(self):
        return PROTOCOL.extract_comparison_input(self.write())

    def close(self):
        self.temporary.cleanup()


class ArtifactProtocolTest(unittest.TestCase):
    def setUp(self):
        self.fixture = ArtifactFixture()

    def tearDown(self):
        self.fixture.close()

    def test_formal_descriptors_freeze_scenario_shape_and_thread_ceilings(self):
        expected = {
            "short.json": ("performance/uea1-short-concurrent", 15),
            "mixed.json": ("performance/uea1-mixed", 5),
            "slow-output.json": ("performance/uea1-slow-output", 5),
        }
        for name, (scenario, window_count) in expected.items():
            descriptor = json.loads((ROOT / "descriptors" / name).read_text())
            self.assertEqual(
                set(descriptor),
                {
                    "schema_version",
                    "artifacts",
                    "expected",
                    "metric_resolutions",
                    "absolute_gates",
                },
            )
            self.assertEqual(descriptor["expected"]["scenario"], scenario)
            self.assertEqual(descriptor["expected"]["window_count"], window_count)
            self.assertEqual(descriptor["expected"]["roles"], PROTOCOL.FORMAL_ROLES)
            self.assertEqual(
                set(descriptor["metric_resolutions"]), set(PROTOCOL.RELATIVE_METRICS)
            )
            self.assertEqual(
                descriptor["absolute_gates"]["fe_peak_threads"]["limit"], 1114
            )
            self.assertEqual(
                descriptor["absolute_gates"]["max_be_peak_threads"]["limit"],
                103578,
            )

    def test_schema_four_and_exact_run_manifest_reference_are_accepted(self):
        document = self.fixture.extract()
        self.assertEqual(document["provenance"]["run_id"], self.fixture.run_id)
        self.assertEqual(
            document["provenance"]["runner_binary_sha256"],
            self.fixture.hashes["runner"],
        )
        self.assertEqual(document["compatibility"]["build_profile"], "release")

    def test_comparison_input_requires_runner_binary_hash(self):
        document = self.fixture.extract()
        document["provenance"]["runner_binary_sha256"] = "not-a-hash"
        with self.assertRaisesRegex(
            PROTOCOL.ProtocolError,
            "provenance.runner_binary_sha256 must be lowercase SHA-256",
        ):
            PROTOCOL.validate_comparison_input(document)

    def test_old_performance_schema_is_rejected(self):
        self.fixture.performance["schema_version"] = 2
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "schema_version 4"):
            self.fixture.extract()

    def test_diagnostic_run_token_must_match_run_manifest(self):
        self.fixture.performance["preparation_diagnostic"]["run_token"] = "other-run"
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "exact formal run"):
            self.fixture.extract()

    def test_diagnostic_prelude_must_end_before_timed_windows(self):
        self.fixture.performance["preparation_diagnostic"][
            "ended_elapsed_micros"
        ] = 100_001
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "overlaps"):
            self.fixture.extract()

    def test_tampered_run_identity_and_manifest_reference_are_rejected(self):
        self.fixture.performance["run_id"] = "2" * 64
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "identity mismatch"):
            self.fixture.extract()
        self.fixture.performance["run_id"] = self.fixture.run_id
        descriptor = self.fixture.write()
        self.fixture.performance["run_manifest_sha256"] = "0" * 64
        (self.fixture.root / "uea1-performance.json").write_text(json.dumps(self.fixture.performance))
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "exact run manifest"):
            PROTOCOL.extract_comparison_input(descriptor)

    def test_dirty_or_tampered_run_manifest_is_rejected(self):
        self.fixture.run_manifest["source_dirty"] = True
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "clean completed"):
            self.fixture.extract()

    def test_formal_native_build_identity_must_equal_source_revision(self):
        self.fixture.run_manifest["native_build_identity"] = "stale-build"
        with self.assertRaisesRegex(
            PROTOCOL.ProtocolError,
            "native_build_identity must equal source_revision",
        ):
            self.fixture.extract()

    def test_run_manifest_schema_and_keys_are_exact(self):
        self.fixture.run_manifest["schema_version"] = 1
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "schema_version 2"):
            self.fixture.extract()
        self.fixture.run_manifest["schema_version"] = 2
        self.fixture.run_manifest["unexpected"] = True
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "keys mismatch"):
            self.fixture.extract()

    def test_formal_runner_identity_is_required(self):
        self.fixture.run_manifest["runner_executable_path"] = "/tmp/novarocks-system-tests"
        with self.assertRaisesRegex(
            PROTOCOL.ProtocolError,
            "checkout-local release runner",
        ):
            self.fixture.extract()
        self.fixture.run_manifest[
            "runner_executable_path"
        ] = "/checkout/target/release/novarocks-system-tests"
        self.fixture.run_manifest["runner_executable_sha256"] = "not-a-hash"
        with self.assertRaisesRegex(
            PROTOCOL.ProtocolError,
            "runner_executable_sha256 must be lowercase SHA-256",
        ):
            self.fixture.extract()

    def test_success_completed_outside_window_is_rejected(self):
        self.fixture.performance["query_samples"][0]["ended_elapsed_micros"] = 200_001
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "outside its window"):
            self.fixture.extract()

    def test_missing_resource_role_is_rejected(self):
        self.fixture.resources.pop()
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "no resource sample for be-2"):
            self.fixture.extract()

    def test_formal_roles_and_preparation_events_are_mandatory(self):
        self.fixture.descriptor["expected"]["roles"] = ["fe", "be-0"]
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, r"exact 1FE\+3BE"):
            self.fixture.extract()
        self.fixture.descriptor["expected"]["roles"] = list(PROTOCOL.FORMAL_ROLES)
        self.fixture.performance["preparation_events_status"] = "unsupported-not-wired"
        self.fixture.performance["preparation_events"] = []
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "available preparation events"):
            self.fixture.extract()

    def test_empty_preparation_event_cannot_satisfy_formal_evidence(self):
        self.fixture.performance["preparation_events"] = [{}]
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "keys mismatch"):
            self.fixture.extract()

    def test_absolute_gates_cannot_supply_manual_values(self):
        self.fixture.descriptor["absolute_gates"]["invented"] = {
            "unit": "items",
            "direction": "at_most",
            "limit": 1,
            "values": [0],
        }
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "exact sampled gate set"):
            self.fixture.extract()

    def test_validator_rejects_incomplete_query_and_resource_metric_sets(self):
        document = self.fixture.extract()
        for metric_group, message in (
            (PROTOCOL.QUERY_RELATIVE_METRICS, "exact query metric set"),
            (PROTOCOL.RESOURCE_RELATIVE_METRICS, "exact resource metric set"),
        ):
            with self.subTest(metric_group=set(metric_group)):
                incomplete = copy.deepcopy(document)
                incomplete["metrics"] = [
                    metric
                    for metric in incomplete["metrics"]
                    if metric["metric"] != next(iter(metric_group))
                ]
                with self.assertRaisesRegex(PROTOCOL.ProtocolError, message):
                    PROTOCOL.validate_comparison_input(incomplete)

    def test_mixed_window_maps_query_and_resource_metrics_to_distinct_cohorts(self):
        scenario = "performance/uea1-mixed"
        self.fixture.run_manifest["scenario"] = scenario
        self.fixture.performance["scenario"] = scenario
        self.fixture.descriptor["expected"]["scenario"] = scenario
        self.fixture.performance["measurement_windows"][0]["workload"] = "mixed"
        self.fixture.performance["measurement_windows"][0]["configured_concurrency"] = 8
        sample = self.fixture.performance["query_samples"][0]
        sample["workload"] = "mixed-foreground"
        sample["configured_concurrency"] = 8
        for phase in ("metadata_observation", "connector_planning_negotiation"):
            event = copy.deepcopy(self.fixture.performance["preparation_events"][0])
            event["phase"] = phase
            event["operation"] = phase
            self.fixture.performance["preparation_events"].append(event)

        document = self.fixture.extract()
        metrics_by_cohort = {}
        for metric in document["metrics"]:
            cohort = (
                metric["cohort"]["workload"],
                metric["cohort"]["configured_concurrency"],
            )
            metrics_by_cohort.setdefault(cohort, set()).add(metric["metric"])
        self.assertEqual(
            metrics_by_cohort[("mixed-foreground", 8)],
            set(PROTOCOL.QUERY_RELATIVE_METRICS),
        )
        self.assertEqual(
            metrics_by_cohort[("mixed", 8)],
            set(PROTOCOL.RESOURCE_RELATIVE_METRICS),
        )

    def test_slow_output_separates_control_foreground_and_resource_cohorts(self):
        scenario = "performance/uea1-slow-output"
        self.fixture.run_manifest["scenario"] = scenario
        self.fixture.performance["scenario"] = scenario
        self.fixture.descriptor["expected"]["scenario"] = scenario
        window = self.fixture.performance["measurement_windows"][0]
        window["workload"] = "slow-output"
        window["configured_concurrency"] = 4
        base = self.fixture.performance["query_samples"][0]
        control = copy.deepcopy(base)
        control["workload"] = "slow-output-control"
        control["configured_concurrency"] = 4
        foreground = copy.deepcopy(base)
        foreground["workload"] = "slow-output-foreground"
        foreground["configured_concurrency"] = 4
        observer = copy.deepcopy(base)
        observer.update(
            {
                "workload": "slow-output-client",
                "configured_concurrency": 4,
                "bytes_read": 4096,
                "outcome": "window-observation",
            }
        )
        self.fixture.performance["query_samples"] = [control, foreground, observer]

        document = self.fixture.extract()
        metrics_by_cohort = {}
        for metric in document["metrics"]:
            cohort = (
                metric["cohort"]["workload"],
                metric["cohort"]["configured_concurrency"],
            )
            metrics_by_cohort.setdefault(cohort, set()).add(metric["metric"])
        self.assertEqual(
            metrics_by_cohort[("slow-output-control", 4)],
            set(PROTOCOL.QUERY_RELATIVE_METRICS),
        )
        self.assertEqual(
            metrics_by_cohort[("slow-output-foreground", 4)],
            set(PROTOCOL.QUERY_RELATIVE_METRICS),
        )
        self.assertEqual(
            metrics_by_cohort[("slow-output", 4)],
            set(PROTOCOL.RESOURCE_RELATIVE_METRICS),
        )
        self.assertNotIn(("slow-output-client", 4), metrics_by_cohort)

        self.fixture.performance["query_samples"] = [control, observer]
        with self.assertRaisesRegex(
            PROTOCOL.ProtocolError,
            "no successful slow-output-foreground query samples",
        ):
            self.fixture.extract()

        client_success = copy.deepcopy(observer)
        client_success["outcome"] = "success"
        self.fixture.performance["query_samples"] = [control, foreground, client_success]
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "diagnostic only"):
            self.fixture.extract()


class StructuredCompareTest(unittest.TestCase):
    def setUp(self):
        fixture = ArtifactFixture()
        self.base = fixture.extract()
        fixture.close()
        self.base_b = copy.deepcopy(self.base)
        self.base_b["provenance"]["run_id"] = "2" * 64
        self.base_b["provenance"]["started_unix_millis"] = 5
        self.base_b["provenance"]["ended_unix_millis"] = 6
        self.candidate_a = copy.deepcopy(self.base)
        self.candidate_a["provenance"]["run_id"] = "3" * 64
        self.candidate_a["provenance"]["source_sha"] = "b" * 40
        self.candidate_a["provenance"]["binary_sha256"] = "b" * 64
        self.candidate_a["provenance"]["started_unix_millis"] = 3
        self.candidate_a["provenance"]["ended_unix_millis"] = 4
        self.candidate_b = copy.deepcopy(self.candidate_a)
        self.candidate_b["provenance"]["run_id"] = "4" * 64
        self.candidate_b["provenance"]["started_unix_millis"] = 7
        self.candidate_b["provenance"]["ended_unix_millis"] = 8

    def test_baseline_aa_source_mismatch_is_rejected(self):
        self.base_b["provenance"]["source_sha"] = "c" * 40
        with self.assertRaisesRegex(COMPARE.ProtocolError, "baseline A/A source_sha mismatch"):
            COMPARE.compare_protocol_inputs(
                self.base, self.candidate_a, self.base_b, self.candidate_b
            )

    def test_candidate_identity_must_differ_from_baseline(self):
        self.candidate_a["provenance"]["source_sha"] = self.base["provenance"]["source_sha"]
        self.candidate_b["provenance"]["source_sha"] = self.base["provenance"]["source_sha"]
        with self.assertRaisesRegex(COMPARE.ProtocolError, "candidate source_sha"):
            COMPARE.compare_protocol_inputs(
                self.base, self.candidate_a, self.base_b, self.candidate_b
            )

    def test_candidate_absolute_gate_failure_fails_comparison(self):
        gate = self.candidate_b["absolute_gates"][0]
        gate["values"] = [gate["limit"] + 1]
        gate["passed"] = False
        result = COMPARE.compare_protocol_inputs(
            self.base, self.candidate_a, self.base_b, self.candidate_b
        )
        self.assertTrue(result["valid"])
        self.assertFalse(result["passed"])

    def test_interleaving_and_candidate_repeat_are_required(self):
        self.base_b["provenance"]["started_unix_millis"] = 2
        with self.assertRaisesRegex(COMPARE.ProtocolError, "ordered B0-A"):
            COMPARE.compare_protocol_inputs(
                self.base, self.candidate_a, self.base_b, self.candidate_b
            )
        self.base_b["provenance"]["started_unix_millis"] = 5
        self.candidate_b["provenance"]["binary_sha256"] = "c" * 64
        with self.assertRaisesRegex(COMPARE.ProtocolError, "candidate A/A binary_sha256"):
            COMPARE.compare_protocol_inputs(
                self.base, self.candidate_a, self.base_b, self.candidate_b
            )

    def test_absolute_report_preserves_all_four_run_positions(self):
        for document, value in (
            (self.base, 1),
            (self.candidate_a, 2),
            (self.base_b, 3),
            (self.candidate_b, 4),
        ):
            document["absolute_gates"][0]["values"] = [value]
            document["absolute_gates"][0]["passed"] = True
        result = COMPARE.compare_protocol_inputs(
            self.base, self.candidate_a, self.base_b, self.candidate_b
        )
        gate = result["absolute_gates"][0]
        self.assertEqual(gate["baseline_a_worst"], 1)
        self.assertEqual(gate["candidate_a_worst"], 2)
        self.assertEqual(gate["baseline_b_worst"], 3)
        self.assertEqual(gate["candidate_b_worst"], 4)


if __name__ == "__main__":
    unittest.main()
