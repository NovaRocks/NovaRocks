import copy
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock


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
            "binary", "runner", "manifest", "fixture", "config", "tool", "lock", "third-party", "tree"
        )}
        self.run_manifest = {
            "schema_version": 5,
            "kind": "performance",
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
            "process_identities": [
                {
                    "role": role,
                    "os_pid": pid,
                    "process_start_token": f"start-{pid}",
                    "application_process_id": None if role == "fe" else f"process-{pid}",
                    "build_identity": self.source,
                    "binary_sha256": self.hashes["binary"],
                    "executable_size_bytes": 1024 + pid,
                    "executable_modified_unix_nanos": 1_000_000 + pid,
                }
                for role, pid in (("fe", 1), ("be-0", 2), ("be-1", 3), ("be-2", 4))
            ],
            "config_sha256": self.hashes["config"],
            "workload_manifest_sha256": self.hashes["manifest"],
            "fixture_sha256": self.hashes["fixture"],
            "tool_tree_sha256": self.hashes["tool"],
            "cargo_lock_sha256": self.hashes["lock"],
            "third_party_build_graph_sha256": self.hashes["third-party"],
            "descriptor_sha256": "",
            "effective_launch_config_sha256": "",
            "effective_launch_config_semantics_sha256": "",
            "fixture_realization_sha256": "",
            "fixture_realization_semantics_sha256": "",
            "raw_artifact_inventory_sha256": "",
            "resources_sha256": "",
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
            "schema_version": 8,
            "run_id": self.run_id,
            "run_manifest_sha256": "",
            "manifest_sha256": self.hashes["manifest"],
            "resources_sha256": "",
            "effective_launch_config_sha256": "",
            "effective_launch_config_semantics_sha256": "",
            "fixture_realization_sha256": "",
            "fixture_realization_semantics_sha256": "",
            "raw_artifact_inventory_sha256": "",
            "scenario": "performance/uea1-short-concurrent",
            "query_samples": [
                {
                    "workload": "short",
                    "window_index": index,
                    "configured_concurrency": 1,
                    "client": 0,
                    "started_elapsed_micros": (110 + index * 200) * 1000,
                    "ended_elapsed_micros": (190 + index * 200) * 1000,
                    "first_row_micros": 100,
                    "total_micros": 200,
                    "rows": 1,
                    "bytes_read": None,
                    "outcome": "success",
                }
                for index in range(15)
            ],
            "measurement_windows": [
                {
                    "workload": "short",
                    "window_index": index,
                    "configured_concurrency": 1,
                    "started_elapsed_micros": (100 + index * 200) * 1000,
                    "ended_elapsed_micros": (200 + index * 200) * 1000,
                    "drain_ended_elapsed_micros": (201 + index * 200) * 1000,
                }
                for index in range(15)
            ],
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
        self.resources = {
            "schema_version": 2,
            "run_id": self.run_id,
            "processes": {
                role: {"pid": pid, "process_start_token": f"start-{pid}"}
                for role, pid in (("fe", 1), ("be-0", 2), ("be-1", 3), ("be-2", 4))
            },
            "samples": [
                {
                    "elapsed_millis": 150 + index * 200,
                    "role": role,
                    "pid": pid,
                    "process_start_token": f"start-{pid}",
                    "rss_bytes": rss,
                    "threads": threads,
                    "unavailable_reason": None,
                }
                for index in range(15)
                for role, pid, rss, threads in (
                    ("fe", 1, 100_000_000, 4),
                    ("be-0", 2, 200_000_000, 5),
                    ("be-1", 3, 210_000_000, 5),
                    ("be-2", 4, 220_000_000, 5),
                )
            ],
        }
        self.descriptor = json.loads((ROOT / "descriptors" / "short.json").read_text())

    def write(self):
        scenario_to_descriptor = {
            "performance/uea1-short-concurrent": "short.json",
            "performance/uea1-mixed": "mixed.json",
            "performance/uea1-slow-output": "slow-output.json",
        }
        canonical = ROOT / "descriptors" / scenario_to_descriptor[
            self.descriptor["expected"]["scenario"]
        ]
        descriptor = self.root / "descriptor.json"
        if self.descriptor == json.loads(canonical.read_text()):
            descriptor.write_bytes(canonical.read_bytes())
        else:
            descriptor.write_text(json.dumps(self.descriptor))
        descriptor_sha256 = hashlib.sha256(descriptor.read_bytes()).hexdigest()
        self.run_manifest["descriptor_sha256"] = descriptor_sha256

        effective = {"schema_version": 1, "semantics": {"profile": "test"}}
        effective_path = self.root / "effective-launch-config.json"
        effective_path.write_text(json.dumps(effective))
        effective_sha256 = hashlib.sha256(effective_path.read_bytes()).hexdigest()
        self.run_manifest["effective_launch_config_sha256"] = effective_sha256
        self.run_manifest["effective_launch_config_semantics_sha256"] = effective_sha256
        self.performance["effective_launch_config_sha256"] = effective_sha256
        self.performance["effective_launch_config_semantics_sha256"] = effective_sha256

        fixture = self._write_fixture_realization()
        fixture_path = self.root / "fixture-realization.json"
        fixture_path.write_text(json.dumps(fixture))
        fixture_sha256 = hashlib.sha256(fixture_path.read_bytes()).hexdigest()
        self.run_manifest["fixture_realization_sha256"] = fixture_sha256
        self.run_manifest["fixture_realization_semantics_sha256"] = fixture["semantics_sha256"]
        self.performance["fixture_realization_sha256"] = fixture_sha256
        self.performance["fixture_realization_semantics_sha256"] = fixture["semantics_sha256"]

        inventory = self._write_raw_artifact_inventory()
        inventory_path = self.root / "raw-artifact-inventory.json"
        inventory_path.write_text(json.dumps(inventory))
        inventory_sha256 = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
        self.run_manifest["raw_artifact_inventory_sha256"] = inventory_sha256
        self.performance["raw_artifact_inventory_sha256"] = inventory_sha256

        resource_path = self.root / "process-resources.json"
        resource_path.write_text(json.dumps(self.resources))
        resource_sha256 = hashlib.sha256(resource_path.read_bytes()).hexdigest()
        self.run_manifest["resources_sha256"] = resource_sha256
        self.performance["resources_sha256"] = resource_sha256
        run_path = self.root / "run-manifest.json"
        run_path.write_text(json.dumps(self.run_manifest))
        self.performance["run_manifest_sha256"] = hashlib.sha256(run_path.read_bytes()).hexdigest()
        performance_path = self.root / "uea1-performance.json"
        performance_path.write_text(json.dumps(self.performance))
        completion = {
            "schema_version": 2,
            "run_id": self.run_id,
            "scenario": self.run_manifest["scenario"],
            "run_manifest_sha256": hashlib.sha256(run_path.read_bytes()).hexdigest(),
            "performance_sha256": hashlib.sha256(performance_path.read_bytes()).hexdigest(),
            "resources_sha256": resource_sha256,
            "descriptor_sha256": descriptor_sha256,
            "effective_launch_config_sha256": effective_sha256,
            "fixture_realization_sha256": fixture_sha256,
            "raw_artifact_inventory_sha256": inventory_sha256,
        }
        (self.root / "run-completion.json").write_text(json.dumps(completion))
        return descriptor

    def _write_raw_artifact_inventory(self):
        scenario = self.run_manifest["scenario"]
        window_count = self.descriptor["expected"]["window_count"]
        artifacts = []
        if scenario == "performance/uea1-mixed":
            aggregate = []
            for index in range(window_count):
                for kind in ("business", "query"):
                    name = f"mixed-{kind}-{index}.json"
                    value = [{"window_index": index, "kind": kind}]
                    (self.root / name).write_text(json.dumps(value))
                    artifacts.append(
                        {
                            "kind": kind,
                            "window_index": index,
                            "path": name,
                            "sha256": hashlib.sha256((self.root / name).read_bytes()).hexdigest(),
                        }
                    )
                    if kind == "business":
                        aggregate.extend(value)
            name = "mixed-business.json"
            (self.root / name).write_text(json.dumps(aggregate))
            artifacts.append(
                {
                    "kind": "business-aggregate",
                    "window_index": None,
                    "path": name,
                    "sha256": hashlib.sha256((self.root / name).read_bytes()).hexdigest(),
                }
            )
        return {
            "schema_version": 1,
            "scenario": scenario,
            "window_count": window_count,
            "artifacts": artifacts,
        }

    def _write_fixture_realization(self):
        scenario = self.run_manifest["scenario"]
        if scenario != "performance/uea1-mixed":
            raw = {"kind": "none"}
            semantic = {"kind": "none"}
            sources = []
        else:
            def window(index):
                table = {
                    "raw": {"qualified_name": f"private.{index}"},
                    "raw_bundle_sha256": sha(f"raw-{index}".encode()),
                    "semantic": {"symbol": f"window/{index}/foreground"},
                }
                return {
                    "schema_version": 1,
                    "window_index": index,
                    "foreground": table,
                    "jobs": [],
                }

            diagnostic = window(5)
            timed = [window(index) for index in range(5)]
            provider = {"schema_version": 1, "images": {"rest": "image-sha"}}
            raw = {
                "kind": "mixed",
                "provider_runtime": provider,
                "diagnostic": diagnostic,
                "timed": timed,
            }
            semantic = {
                "kind": "mixed",
                "provider_runtime": provider,
                "diagnostic": PROTOCOL._project_fixture_window(diagnostic, "test diagnostic"),
                "timed": [
                    PROTOCOL._project_fixture_window(value, f"test timed[{index}]")
                    for index, value in enumerate(timed)
                ],
            }
            sources = []
            for index, identity in enumerate(timed):
                name = f"mixed-fixture-{index}.json"
                path = self.root / name
                path.write_text(json.dumps({"fixture_identity": identity}))
                sources.append({"path": name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        raw_bundle = {"realization": raw, "source_artifacts": sources}
        return {
            "schema_version": 1,
            "scenario": scenario,
            "source_artifacts": sources,
            "raw_bundle_sha256": PROTOCOL._canonical_sha256(raw_bundle),
            "semantics_sha256": PROTOCOL._canonical_sha256(semantic),
            "raw": raw,
            "semantic": semantic,
        }

    def extract(self):
        return PROTOCOL.extract_comparison_input(self.write())

    def close(self):
        self.temporary.cleanup()


class ArtifactProtocolTest(unittest.TestCase):
    def setUp(self):
        self.fixture = ArtifactFixture()

    def tearDown(self):
        self.fixture.close()

    def write_mixed_artifacts(self):
        scenario = "performance/uea1-mixed"
        self.fixture.run_manifest["scenario"] = scenario
        self.fixture.performance["scenario"] = scenario
        self.fixture.descriptor = json.loads((ROOT / "descriptors" / "mixed.json").read_text())
        self.fixture.performance["measurement_windows"] = self.fixture.performance[
            "measurement_windows"
        ][:5]
        self.fixture.performance["query_samples"] = self.fixture.performance["query_samples"][:5]
        for window in self.fixture.performance["measurement_windows"]:
            window["workload"] = "mixed"
        for sample in self.fixture.performance["query_samples"]:
            sample["workload"] = "mixed-foreground"
        for phase in ("metadata_observation", "connector_planning_negotiation"):
            event = copy.deepcopy(self.fixture.performance["preparation_events"][0])
            event["phase"] = phase
            event["operation"] = phase
            self.fixture.performance["preparation_events"].append(event)
        return self.fixture.write()

    def test_raw_artifact_inventory_rejects_missing_artifact(self):
        descriptor = self.write_mixed_artifacts()
        (self.fixture.root / "mixed-query-0.json").unlink()
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "cannot hash"):
            PROTOCOL.extract_comparison_input(descriptor)

    def test_raw_artifact_inventory_rejects_tampered_artifact(self):
        descriptor = self.write_mixed_artifacts()
        (self.fixture.root / "mixed-business-0.json").write_text("[]")
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "missing or has changed"):
            PROTOCOL.extract_comparison_input(descriptor)

    def test_raw_artifact_inventory_rejects_added_artifact(self):
        descriptor = self.write_mixed_artifacts()
        (self.fixture.root / "mixed-query-5.json").write_text("[]")
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "closed inventory"):
            PROTOCOL.extract_comparison_input(descriptor)

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

    def test_formal_extraction_rejects_a_shared_descriptor_with_relaxed_limits(self):
        self.fixture.descriptor["absolute_gates"]["fe_peak_threads"]["limit"] = 10_000_000
        with self.assertRaisesRegex(
            PROTOCOL.ProtocolError, "byte-identical to canonical short.json"
        ):
            self.fixture.extract()

    def test_schema_six_and_exact_run_manifest_reference_are_accepted(self):
        document = self.fixture.extract()
        self.assertEqual(document["provenance"]["run_id"], self.fixture.run_id)
        self.assertEqual(
            document["provenance"]["runner_binary_sha256"],
            self.fixture.hashes["runner"],
        )
        self.assertEqual(
            document["provenance"]["cargo_lock_sha256"],
            self.fixture.hashes["lock"],
        )
        self.assertEqual(
            document["compatibility"]["third_party_build_graph_sha256"],
            self.fixture.hashes["third-party"],
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

        document = self.fixture.extract()
        del document["provenance"]["cargo_lock_sha256"]
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "provenance keys mismatch"):
            PROTOCOL.validate_comparison_input(document)

        document = self.fixture.extract()
        del document["compatibility"]["third_party_build_graph_sha256"]
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "compatibility keys mismatch"):
            PROTOCOL.validate_comparison_input(document)

    def test_raw_artifact_keys_are_exact(self):
        self.fixture.performance["unexpected"] = True
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "performance artifact keys mismatch"):
            self.fixture.extract()

        extra_resource = ArtifactFixture()
        self.addCleanup(extra_resource.close)
        extra_resource.resources["unexpected"] = True
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "resource artifact keys mismatch"):
            extra_resource.extract()

        extra_sample = ArtifactFixture()
        self.addCleanup(extra_sample.close)
        extra_sample.resources["samples"][0]["unexpected"] = True
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, r"resources\[0\] keys mismatch"):
            extra_sample.extract()

    def test_old_performance_schema_is_rejected(self):
        self.fixture.performance["schema_version"] = 2
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "schema_version 8"):
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
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "schema_version 5"):
            self.fixture.extract()
        self.fixture.run_manifest["schema_version"] = 5
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

    def test_sub_millisecond_window_end_keeps_an_in_window_success(self):
        window = self.fixture.performance["measurement_windows"][0]
        window["ended_elapsed_micros"] = 200_999
        window["drain_ended_elapsed_micros"] = 201_000
        self.fixture.performance["query_samples"][0]["ended_elapsed_micros"] = 200_998
        self.fixture.extract()

    def test_missing_resource_role_is_rejected(self):
        self.fixture.resources["samples"].pop()
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "no resource sample for be-2"):
            self.fixture.extract()

    def test_resource_artifact_schema_identity_and_sample_pid_are_exact(self):
        self.fixture.resources = self.fixture.resources["samples"]
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "resource artifact must be an object"):
            self.fixture.extract()

        wrong_run = ArtifactFixture()
        self.addCleanup(wrong_run.close)
        wrong_run.resources["run_id"] = "wrong-run"
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "resource artifact and run manifest"):
            wrong_run.extract()

        wrong_pid = ArtifactFixture()
        self.addCleanup(wrong_pid.close)
        wrong_pid.resources["samples"][0]["pid"] = 999
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "invalid role, pid, process start token"):
            wrong_pid.extract()

    def test_old_resource_schema_is_rejected(self):
        self.fixture.resources["schema_version"] = 1
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "schema_version 2"):
            self.fixture.extract()

    def test_run_manifest_process_identity_set_is_exact_and_consistent(self):
        self.fixture.run_manifest["process_identities"].pop()
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, r"exact 1FE\+3BE"):
            self.fixture.extract()

        extra_process = ArtifactFixture()
        self.addCleanup(extra_process.close)
        extra_process.run_manifest["process_identities"].append(
            {
                "role": "be-3",
                "os_pid": 5,
                "process_start_token": "start-5",
                "application_process_id": "process-5",
                "build_identity": extra_process.source,
                "binary_sha256": extra_process.hashes["binary"],
                "executable_size_bytes": 1029,
                "executable_modified_unix_nanos": 1_000_005,
            }
        )
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, r"exact 1FE\+3BE"):
            extra_process.extract()

        duplicate_pid = ArtifactFixture()
        self.addCleanup(duplicate_pid.close)
        duplicate_pid.run_manifest["process_identities"][1]["os_pid"] = 1
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "duplicate process roles or PIDs"):
            duplicate_pid.extract()

        wrong_build = ArtifactFixture()
        self.addCleanup(wrong_build.close)
        wrong_build.run_manifest["process_identities"][1]["build_identity"] = "wrong"
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "build identity is inconsistent"):
            wrong_build.extract()

        missing_application_identity = ArtifactFixture()
        self.addCleanup(missing_application_identity.close)
        missing_application_identity.run_manifest["process_identities"][1][
            "application_process_id"
        ] = None
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "requires an application"):
            missing_application_identity.extract()

        missing_start_token = ArtifactFixture()
        self.addCleanup(missing_start_token.close)
        missing_start_token.run_manifest["process_identities"][0][
            "process_start_token"
        ] = ""
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "process_start_token"):
            missing_start_token.extract()

        invalid_executable_identity = ArtifactFixture()
        self.addCleanup(invalid_executable_identity.close)
        invalid_executable_identity.run_manifest["process_identities"][0][
            "executable_size_bytes"
        ] = 0
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "executable_size_bytes is invalid"):
            invalid_executable_identity.extract()

    def test_resource_process_map_must_equal_run_manifest(self):
        self.fixture.resources["processes"]["be-2"]["pid"] = 99
        self.fixture.resources["samples"][-1]["pid"] = 99
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "do not match the run manifest"):
            self.fixture.extract()

    def test_pid_reuse_and_sample_start_token_mismatch_are_rejected(self):
        reused_pid = ArtifactFixture()
        self.addCleanup(reused_pid.close)
        reused_pid.resources["processes"]["be-2"][
            "process_start_token"
        ] = "replacement-process"
        for sample in reused_pid.resources["samples"]:
            if sample["role"] == "be-2":
                sample["process_start_token"] = "replacement-process"
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "do not match the run manifest"):
            reused_pid.extract()

        mismatched_sample = ArtifactFixture()
        self.addCleanup(mismatched_sample.close)
        mismatched_sample.resources["samples"][0][
            "process_start_token"
        ] = "replacement-process"
        with self.assertRaisesRegex(
            PROTOCOL.ProtocolError,
            "invalid role, pid, process start token",
        ):
            mismatched_sample.extract()

    def test_resource_hash_is_bound_by_performance_and_run_manifest(self):
        descriptor = self.fixture.write()
        resource_path = self.fixture.root / "process-resources.json"
        resources = json.loads(resource_path.read_text())
        resources["samples"][0]["rss_bytes"] += 1
        resource_path.write_text(json.dumps(resources))
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "exact resource artifact"):
            PROTOCOL.extract_comparison_input(descriptor)

        wrong_performance = ArtifactFixture()
        self.addCleanup(wrong_performance.close)
        descriptor = wrong_performance.write()
        performance_path = wrong_performance.root / "uea1-performance.json"
        performance = json.loads(performance_path.read_text())
        performance["resources_sha256"] = "0" * 64
        performance_path.write_text(json.dumps(performance))
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "exact resource artifact"):
            PROTOCOL.extract_comparison_input(descriptor)

        wrong_manifest = ArtifactFixture()
        self.addCleanup(wrong_manifest.close)
        descriptor = wrong_manifest.write()
        manifest_path = wrong_manifest.root / "run-manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["resources_sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest))
        performance_path = wrong_manifest.root / "uea1-performance.json"
        performance = json.loads(performance_path.read_text())
        performance["run_manifest_sha256"] = hashlib.sha256(
            manifest_path.read_bytes()
        ).hexdigest()
        performance_path.write_text(json.dumps(performance))
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "exact resource artifact"):
            PROTOCOL.extract_comparison_input(descriptor)

    def test_completion_marker_must_exist_and_bind_the_exact_artifact_set(self):
        descriptor = self.fixture.write()
        completion_path = self.fixture.root / "run-completion.json"
        completion_path.unlink()
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "cannot read completion marker"):
            PROTOCOL.extract_comparison_input(descriptor)

        tampered = ArtifactFixture()
        self.addCleanup(tampered.close)
        descriptor = tampered.write()
        completion_path = tampered.root / "run-completion.json"
        completion = json.loads(completion_path.read_text())
        completion["performance_sha256"] = "0" * 64
        completion_path.write_text(json.dumps(completion))
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "exact completed artifact set"):
            PROTOCOL.extract_comparison_input(descriptor)

    def test_effective_config_and_fixture_realization_are_hash_bound(self):
        descriptor = self.fixture.write()
        effective_path = self.fixture.root / "effective-launch-config.json"
        effective_path.write_text(
            json.dumps({"schema_version": 1, "semantics": {"profile": "changed"}})
        )
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "exact config"):
            PROTOCOL.extract_comparison_input(descriptor)

        fixture = ArtifactFixture()
        self.addCleanup(fixture.close)
        descriptor = fixture.write()
        realization_path = fixture.root / "fixture-realization.json"
        realization = json.loads(realization_path.read_text())
        realization["semantic"] = {"kind": "changed"}
        realization_path.write_text(json.dumps(realization))
        with self.assertRaisesRegex(PROTOCOL.ProtocolError, "semantic hash"):
            PROTOCOL.extract_comparison_input(descriptor)

    def test_compare_descriptor_loader_reextracts_raw_artifacts(self):
        descriptor = self.fixture.write()
        derived = self.fixture.root / "comparison-input.json"
        tampered = PROTOCOL.extract_comparison_input(descriptor)
        tampered["metrics"][0]["values"] = [0.0001]
        derived.write_text(json.dumps(tampered))
        loaded = COMPARE.load_descriptor_input(descriptor)
        self.assertNotEqual(loaded["metrics"][0]["values"], [0.0001])

        mixed_descriptor = self.write_mixed_artifacts()
        (self.fixture.root / "mixed-query-0.json").write_text("[]")
        with self.assertRaisesRegex(COMPARE.ProtocolError, "missing or has changed"):
            COMPARE.load_descriptor_input(mixed_descriptor)

    def test_baseline_cli_accepts_descriptors_and_reextracts_artifacts(self):
        baseline_b = ArtifactFixture()
        self.addCleanup(baseline_b.close)
        baseline_b.run_id = "2" * 64
        baseline_b.run_manifest["run_id"] = baseline_b.run_id
        baseline_b.run_manifest["started_unix_millis"] = 3
        baseline_b.run_manifest["ended_unix_millis"] = 4
        baseline_b.performance["run_id"] = baseline_b.run_id
        baseline_b.performance["preparation_diagnostic"]["run_token"] = baseline_b.run_id
        baseline_b.resources["run_id"] = baseline_b.run_id
        descriptor_a = self.fixture.write()
        descriptor_b = baseline_b.write()
        output = io.StringIO()
        arguments = [
            "compare.py",
            "--baseline-noise",
            "--baseline-a",
            str(descriptor_a),
            "--baseline-b",
            str(descriptor_b),
        ]
        with mock.patch.object(sys, "argv", arguments), contextlib.redirect_stdout(output):
            exit_code = COMPARE.main()
        self.assertEqual(exit_code, 0)
        report = json.loads(output.getvalue())
        self.assertEqual(report["runs"]["baseline_a"], self.fixture.run_id)
        self.assertEqual(report["runs"]["baseline_b"], baseline_b.run_id)

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
        self.fixture.descriptor = json.loads((ROOT / "descriptors" / "mixed.json").read_text())
        self.fixture.performance["measurement_windows"] = self.fixture.performance[
            "measurement_windows"
        ][:5]
        self.fixture.performance["query_samples"] = self.fixture.performance["query_samples"][:5]
        for window in self.fixture.performance["measurement_windows"]:
            window["workload"] = "mixed"
            window["configured_concurrency"] = 8
        for sample in self.fixture.performance["query_samples"]:
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
        self.fixture.descriptor = json.loads(
            (ROOT / "descriptors" / "slow-output.json").read_text()
        )
        self.fixture.performance["measurement_windows"] = self.fixture.performance[
            "measurement_windows"
        ][:5]
        controls = []
        foregrounds = []
        observers = []
        for window, base in zip(
            self.fixture.performance["measurement_windows"],
            self.fixture.performance["query_samples"][:5],
        ):
            window["workload"] = "slow-output"
            window["configured_concurrency"] = 4
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
            controls.append(control)
            foregrounds.append(foreground)
            observers.append(observer)
        self.fixture.performance["query_samples"] = controls + foregrounds + observers

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

        self.fixture.performance["query_samples"] = controls + foregrounds[:-1] + observers
        with self.assertRaisesRegex(
            PROTOCOL.ProtocolError,
            "no successful slow-output-foreground query samples",
        ):
            self.fixture.extract()

        client_success = copy.deepcopy(observers[0])
        client_success["outcome"] = "success"
        self.fixture.performance["query_samples"] = controls + foregrounds + [client_success]
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
        gate["values"] = [gate["limit"] + 1] * len(self.candidate_b["windows"])
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
            document["absolute_gates"][0]["values"] = [value] * len(document["windows"])
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
