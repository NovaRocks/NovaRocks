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

"""Cloud machine authority preserves activation identity and original exits."""
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_runtime as runtime
import machine_runtime as machine
from test_machine_runtime import Backend, bom


CLOUD = dict(kind="aliyun-oss", region="cn-test", bucket="fixture-tests",
             role_name="fixture-role", root_prefix="isolated/fixtures/")


class CloudBackend(Backend):
    def ensure_machine(self, record, previous=None):
        machine.validate_cloud_snapshot(record)
        self.events.append("ensure")
        if self.failure == "ensure":
            raise runtime.RuntimeFailure("PortUnavailable", "occupied port")
        self.occupied = self.online = self.catalog_present = True
        self.version = record["version"]
        self.counter += 1
        record["containers"] = {s: f"container-{s}-{self.counter}" for s in record["required_services"]}
        record["network_id"] = "fixed-network-id"

    def validate_minio_volume(self, record):
        raise AssertionError("cloud machine accessed MinIO")

    def rotate_root(self, record, previous):
        raise AssertionError("cloud machine mutated IAM")

    def assert_old_root_revoked(self, previous):
        raise AssertionError("cloud machine assumed role revocation")


class CloudMachineTests(unittest.TestCase):
    def test_native_mc_rejects_cloud_before_journal_or_command_dispatch(self):
        backend = machine.MachineDocker(environment={})
        with mock.patch.object(backend, "command") as command, \
                mock.patch.object(machine.runtime, "atomic_json") as journal:
            with self.assertRaises(runtime.RuntimeFailure) as caught:
                backend.run_mc({"config": {"object_store": CLOUD}}, "mc admin info")
        self.assertEqual("CloudIamUnsupported", caught.exception.code)
        command.assert_not_called()
        journal.assert_not_called()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.backend = CloudBackend()
        self.owner = machine.MachineOwner(self.directory / "state", backend=self.backend,
            sweeper_factory=lambda *_: self.fail("cloud maintenance must not invoke TTL/IAM cleanup"))
        self.inputs = bom()
        self.inputs["derived_images"] = {name: dict(image_id="sha256:" + name,
            definition_sha256="definition-" + name, platform="linux/amd64") for name in
            ("iceberg-rest-oss", "iceberg-spark-oss", "rest-mv-oss")}
        self.inputs["images"] = {}
        self.inputs["platform"] = "linux/amd64"
        self.inputs["_verified_store_root"] = str(self.directory / "store")
        self.inputs["python_environments"] = {"oss-sdk": dict(path="generations/g1/python-environments/oss-sdk",
            python_version="3.14.0", definition_sha256="sdk-definition", packages={"oss2": "2.19.1"}, files={"bin/python": "a" * 64, "lib/oss2/api.py": "b" * 64})}
        self.python = self.directory / "store/generations/g1/python-environments/oss-sdk/bin/python"
        self.python.parent.mkdir(parents=True)
        self.python.symlink_to(sys.executable)

    def definition(self, **changes):
        return self.owner.required_definition(dict(object_store=CLOUD, **changes), self.inputs)

    def start(self):
        return self.owner.activate(self.definition())

    def assert_code(self, code, call):
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            call()
        self.assertEqual(code, caught.exception.code)

    def test_cloud_config_is_canonical_and_never_falls_back(self):
        config = machine.machine_config({"object_store": {**CLOUD, "root_prefix": "isolated/fixtures"}})
        self.assertEqual(CLOUD, config["object_store"])
        self.assertEqual("s3://fixture-tests/isolated/fixtures/benchmarks", config["benchmark_root"])
        self.assertNotIn("minio_volume", config)
        for value in (None, {}, {**CLOUD, "kind": "other"}, {**CLOUD, "token": "forbidden"}):
            self.assert_code("InvalidConfiguration", lambda: machine.machine_config({"object_store": value}))
        self.assert_code("InvalidConfiguration", lambda: machine.machine_config({"object_store": CLOUD, "minio_volume": "old"}))
        self.assertIn("minio_volume", machine.machine_config())

    def test_benchmark_is_inside_cloud_and_disjoint_from_catalog_and_worktrees(self):
        for suffix in ("catalogs", "catalogs/private", "worktrees/private", "machines/private", ""):
            self.assert_code("InvalidConfiguration", lambda: self.definition(
                benchmark_root="s3://fixture-tests/isolated/fixtures/" + suffix))
        self.assert_code("InvalidConfiguration", lambda: self.definition(benchmark_root="s3://other/prefix"))
        self.definition(benchmark_root="s3://fixture-tests/isolated/fixtures/my-benchmarks")

    def test_definition_freezes_only_verified_three_images_and_sdk_semantics(self):
        definition = self.definition()
        self.assertEqual({"rest", "rest-mv", "spark"}, set(definition["images"]))
        self.assertEqual(str(self.python), definition["oss_ops_python"])
        self.assertNotEqual(str(self.python.resolve()), definition["oss_ops_python"])
        changed = copy.deepcopy(self.inputs)
        changed["_verified_store_root"] = str(self.directory / "different-cache")
        changed["python_environments"]["oss-sdk"]["path"] = "generations/elsewhere/sdk"
        other = self.owner.required_definition({"object_store": CLOUD}, changed)
        self.assertEqual(definition["version"], other["version"])
        changed["python_environments"]["oss-sdk"]["definition_sha256"] = "changed-sdk-source"
        self.assertNotEqual(definition["version"], self.owner.required_definition({"object_store": CLOUD}, changed)["version"])

    def relocated_sdk(self):
        inputs = copy.deepcopy(self.inputs)
        inputs["_verified_store_root"] = str(self.directory / "new-store")
        inputs["python_environments"]["oss-sdk"]["path"] = "generations/g2/python-environments/oss-sdk"
        python = self.directory / "new-store/generations/g2/python-environments/oss-sdk/bin/python"
        python.parent.mkdir(parents=True)
        python.symlink_to(sys.executable)
        return inputs, python

    def test_actual_sdk_and_interpreter_file_hashes_change_machine_version(self):
        original = self.start()
        for name in ("bin/python", "lib/oss2/api.py"):
            inputs = copy.deepcopy(self.inputs)
            inputs["python_environments"]["oss-sdk"]["files"][name] = "c" * 64
            inputs["python_environments"]["oss-sdk"]["path"] = "generations/rebuilt/python-environments/oss-sdk"
            definition = self.owner.required_definition({"object_store": CLOUD}, inputs)
            self.assertNotEqual(original["version"], definition["version"])
            self.assert_code("MachineVersionMismatch", lambda: self.owner.check_ready(inputs, {"object_store": CLOUD}))
            self.assert_code("MachineVersionMismatch", lambda: self.owner.activate(definition))
        self.assertEqual(original, self.owner.read_saved_state())

    def test_rebuilt_sdk_cannot_reuse_old_path_even_when_semantic_files_match(self):
        original = self.start()
        inputs, python = self.relocated_sdk()
        definition = self.owner.required_definition({"object_store": CLOUD}, inputs)
        self.assertEqual(original["version"], definition["version"])
        self.assertNotEqual(original["oss_ops_python"], definition["oss_ops_python"])
        before = {path: path.read_bytes() for path in self.owner.root.rglob("*") if path.is_file()}
        events = list(self.backend.events)
        self.assert_code("MachineVersionMismatch", lambda: self.owner.check_ready(inputs, {"object_store": CLOUD}))
        self.assert_code("MachineVersionMismatch", lambda: self.owner.activate(definition))
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            with self.owner.ready_snapshot(definition):
                self.fail("new verification must not authorize the old SDK location")
        self.assertEqual("MachineVersionMismatch", caught.exception.code)
        self.assertEqual(before, {path: path.read_bytes() for path in self.owner.root.rglob("*") if path.is_file()})
        self.assertEqual(events, self.backend.events)
        # An already admitted run still observes its original pinned activation;
        # it is not rebound to the newer mutable BOM or cache location.
        with self.owner.ready_snapshot({"version": original["version"]}, expected_generation=original["generation"]) as pinned:
            self.assertEqual(original["oss_ops_python"], pinned["oss_ops_python"])
            self.assertNotEqual(str(python), pinned["oss_ops_python"])

    def test_explicit_upgrade_binds_new_verified_sdk_without_retargeting_old_run(self):
        original = self.start()
        inputs, python = self.relocated_sdk()
        upgraded = self.owner.activate(self.owner.required_definition({"object_store": CLOUD}, inputs), upgrade=True)
        self.assertEqual(original["version"], upgraded["version"])
        self.assertNotEqual(original["generation"], upgraded["generation"])
        self.assertEqual(str(python), upgraded["oss_ops_python"])
        self.assertEqual(str(self.python), original["oss_ops_python"])
        self.assertEqual(upgraded["generation"], self.owner.check_ready(inputs, {"object_store": CLOUD})["generation"])
        self.assert_code("MachineVersionMismatch", lambda: self.owner.check_ready(self.inputs, {"object_store": CLOUD}))

    def test_saved_maintenance_cannot_switch_to_a_different_verified_sdk_location(self):
        self.backend.failure = "ensure"
        self.assert_code("PortUnavailable", self.start)
        saved = self.owner.read_saved_state()
        inputs, _ = self.relocated_sdk()
        self.backend.failure = None
        self.assert_code("MachineVersionMismatch", lambda: self.owner.activate(
            self.owner.required_definition({"object_store": CLOUD}, inputs)))
        self.assertEqual(saved, self.owner.read_saved_state())

    def test_sdk_without_file_proof_is_not_a_verified_prerequisite(self):
        for files in (None, {}, [], "hash"):
            inputs = copy.deepcopy(self.inputs)
            inputs["python_environments"]["oss-sdk"]["files"] = files
            self.assert_code("FixturePrerequisiteMissing", lambda: self.owner.required_definition({"object_store": CLOUD}, inputs))

    def test_missing_sdk_or_wrapper_is_a_prerequisite_failure(self):
        for group, name in (("derived_images", "iceberg-rest-oss"), ("python_environments", "oss-sdk")):
            inputs = copy.deepcopy(self.inputs)
            del inputs[group][name]
            self.assert_code("FixturePrerequisiteMissing", lambda: self.owner.required_definition({"object_store": CLOUD}, inputs))
        self.assertFalse(self.owner.root.exists())

    def test_record_has_real_storage_owner_and_no_static_credentials_or_minio(self):
        record = self.start()
        self.assertEqual(["rest", "rest-mv", "spark"], record["required_services"])
        self.assertEqual({"rest", "rest-mv", "spark"}, set(record["images"]))
        self.assertNotIn("credentials", record)
        self.assertNotIn("minio_volume", record["config"])
        self.assertEqual(2, len(record["volumes"]))
        self.assertTrue(record["server_warehouse"].startswith("s3://fixture-tests/isolated/fixtures/machines/"))
        self.assertEqual(dict(provider="aliyun-oss", endpoint="https://s3.oss-cn-test-internal.aliyuncs.com",
            region="cn-test", bucket="fixture-tests", root_prefix="isolated/fixtures/"), record["storage_owner"])
        endpoint = machine.endpoints(record)
        self.assertNotIn("minio_endpoint", endpoint)
        self.assertEqual(record["storage_owner"]["endpoint"], endpoint["object_store_endpoint"])
        text = Path(record["compose_env"]).read_text()
        self.assertNotIn("MINIO_", text)
        self.assertNotIn("AWS_ACCESS_KEY_ID", record["template"])
        self.assertIn("CATALOG_CLIENT_CREDENTIALS__PROVIDER_ROLE__NAME", record["template"])
        for name in ("AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION"):
            self.assertIn(name + ": WHEN_REQUIRED", record["template"])
            self.assertNotIn(name, (runtime.SCRIPT_DIR / "templates/machine.yml").read_text())

        self.assertEqual(CLOUD, json.loads(Path(record["object_store_config_file"]).read_text()))
        self.assertEqual(0o600, Path(record["object_store_config_file"]).stat().st_mode & 0o777)

    def test_same_definition_reuses_original_activation_and_check_does_not_write(self):
        original = self.start()
        before = {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in self.owner.root.rglob("*") if path.is_file()}
        self.assertEqual(original, self.owner.activate(self.definition()))
        checked = self.owner.check_ready(self.inputs, {"object_store": CLOUD})
        self.assertEqual(original["generation"], checked["generation"])
        self.assertTrue(checked["observed_healthy"])
        after = {path: (path.stat().st_mtime_ns, path.read_bytes()) for path in self.owner.root.rglob("*") if path.is_file()}
        self.assertEqual(before, after)

    def test_upgrade_stops_original_writers_and_confirms_catalog_absence_without_deleting_oss(self):
        original = self.start()
        upgraded = self.owner.activate(self.definition(ports={"rest": 31002}), upgrade=True)
        self.assertNotEqual(original["generation"], upgraded["generation"])
        self.assertNotEqual(original["server_warehouses"], upgraded["server_warehouses"])
        self.assertLess(self.backend.events.index("stop-writers"), self.backend.events.index("remove-rest-state"))
        self.assertGreaterEqual(self.backend.events.count("verify-catalog-absence"), 2)
        self.assertNotIn("rotate-root", self.backend.events)
        self.assertNotIn("purged", upgraded["steps"])
        self.assertTrue(Path(original["object_store_config_file"]).is_file())

    def test_unhealthy_same_version_recovery_keeps_original_catalog_warehouse(self):
        original = self.start()
        self.backend.online = False
        recovered = self.owner.activate(self.definition())
        self.assertNotEqual(original["generation"], recovered["generation"])
        self.assertEqual(original["server_warehouses"], recovered["server_warehouses"])
        self.assertNotIn("remove-rest-state", self.backend.events)

    def test_unknown_catalog_removal_keeps_maintenance_and_retries_original_intent(self):
        self.start()
        target = self.definition(ports={"rest": 31002})
        self.backend.failure = "remove-rest-state"
        self.assert_code("DockerOperationTimeout", lambda: self.owner.activate(target, upgrade=True))
        interrupted = self.owner.read_saved_state()
        self.assertEqual("maintenance", interrupted["phase"])
        self.assertFalse(interrupted["steps"].get("activation_started", False))
        self.backend.failure = None
        resumed = self.owner.activate(target, upgrade=True)
        self.assertEqual(interrupted["generation"], resumed["generation"])
        self.assertEqual(1, self.backend.events.count("stop-writers"))

    def test_cloud_ttl_maintenance_reports_retention_without_cleanup(self):
        self.start()
        report = self.owner.sweep()
        self.assertEqual("NOT_RUN", report["status"])
        self.assertEqual("cloud-ttl", report["scope"])
        self.assertFalse(report["cleanup_performed"])
        self.assertEqual([], report["errors"])
        self.assertEqual([], report["removed"])
        self.assertEqual("retained-no-writer-exit-proof", report["disposition"])

    def test_mutable_desired_copy_cannot_retarget_original_activation(self):
        record = self.start()
        desired = self.directory / "desired-object-store.json"
        desired.write_text(json.dumps({**CLOUD, "bucket": "another-bucket"}))
        with mock.patch.dict("os.environ", {"NOVA_ENV_OBJECT_STORE_CONFIG": str(desired)}):
            self.assertEqual(record["generation"], self.owner.check_ready(self.inputs, {"object_store": CLOUD})["generation"])
        changed = copy.deepcopy(record)
        changed["object_store_config_file"] = str(desired)
        self.assert_code("RuntimeIdentityMismatch", lambda: machine.validate_cloud_snapshot(changed))
        self.assert_code("RuntimeIdentityMismatch", lambda: self.owner._write_definition({**record,
            "config": {**record["config"], "object_store": {**CLOUD, "bucket": "another-bucket"}}}))
        self.assertEqual(CLOUD, json.loads(Path(record["object_store_config_file"]).read_text()))

    def test_changed_activation_config_or_missing_sdk_is_rejected(self):
        record = self.start()
        path = Path(record["object_store_config_file"])
        path.write_text(json.dumps({**CLOUD, "bucket": "changed"}))
        self.assert_code("RuntimeIdentityMismatch", lambda: self.owner.check_ready(self.inputs, {"object_store": CLOUD}))
        path.write_text(json.dumps(CLOUD))
        self.python.unlink()
        self.assert_code("FixturePrerequisiteMissing", lambda: self.owner.check_ready(self.inputs, {"object_store": CLOUD}))

    def test_cross_provider_upgrade_is_rejected_before_effects(self):
        self.start()
        events = list(self.backend.events)
        local = self.owner.required_definition({}, bom())
        self.assert_code("RuntimeIdentityMismatch", lambda: self.owner.activate(local, upgrade=True))
        self.assertEqual(events, self.backend.events)

    def test_cli_chooses_cloud_verified_consumer(self):
        config_file = self.directory / "machine.json"
        config_file.write_text(json.dumps({"object_store": CLOUD}))
        with mock.patch.object(machine, "MachineOwner", return_value=self.owner), \
                mock.patch.object(machine, "verified_bom", return_value=self.inputs) as verify, \
                mock.patch.object(runtime, "install_signal_handlers"), mock.patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(0, machine.main(["start", "--config", str(config_file), "--store", "/verified-store"]))
        verify.assert_called_once_with(runtime.SCRIPT_DIR.parent.parent, Path("/verified-store"), consumer="iceberg-rest-oss")

    def test_native_activation_starts_only_three_services_and_keeps_authority(self):
        record = self.start()
        backend = object.__new__(machine.MachineDocker)
        backend.ensure_authority_volume = mock.Mock()
        backend.activation_preflight = mock.Mock()
        backend.validate_minio_volume = mock.Mock(side_effect=AssertionError("MinIO accessed"))
        backend.compose = mock.Mock()
        backend.await_health = mock.Mock()
        backend.ensure_machine(record)
        backend.ensure_authority_volume.assert_called_once_with(record)
        backend.activation_preflight.assert_called_once_with(record, None)
        backend.compose.assert_called_once_with(record, ["up", "-d", "--force-recreate", "rest", "rest-mv", "spark"])
        backend.validate_minio_volume.assert_not_called()

    def native_backend_for(self, record, fault=None):
        backend = object.__new__(machine.MachineDocker)
        labels = machine.MachineDocker.labels(record)
        def command(args):
            if args[0] == "ps":
                service = next(value.split("=", 2)[2] for value in args
                    if value.startswith("label=com.docker.compose.service="))
                return record["containers"][service]
            if args[0] == "compose":
                return ""
            self.fail("unexpected native command: " + str(args))
        def inspect(kind, name, **kwargs):
            if kind == "network":
                return dict(Id="foreign-network" if fault == "network" else record["network_id"], Labels=labels)
            if kind == "volume":
                return dict(Labels={**labels, "novarocks.fixture.owner": "foreign"} if fault == "volume" else labels)
            self.assertEqual("container", kind)
            service = next(service for service, identity in record["containers"].items() if identity == name)
            actual_labels = {**labels, "novarocks.fixture.version": record["version"],
                             "novarocks.fixture.activation": record["generation"]}
            if fault == "container-label":
                actual_labels["novarocks.fixture.owner"] = "foreign"
            return dict(Id="foreign-container" if fault == "container-id" else name,
                Image="sha256:foreign" if fault == "container-image" else record["images"][service]["image_id"],
                Config=dict(Labels=actual_labels), HostConfig=dict(PortBindings={port: [{"HostPort": str(host)}]
                    for port, host in record["service_ports"][service].items()}))
        backend.command = mock.Mock(side_effect=command)
        backend.inspect = mock.Mock(side_effect=inspect)
        return backend

    def test_native_writer_stop_can_repair_missing_sdk_without_relaxing_readiness(self):
        record = self.start()
        self.python.unlink()
        self.assert_code("FixturePrerequisiteMissing", lambda: self.owner.check_ready(self.inputs, {"object_store": CLOUD}))
        backend = self.native_backend_for(record)
        self.assert_code("FixturePrerequisiteMissing", lambda: backend.validate_resources(record))
        backend.stop_writers(record)
        stops = [call.args[0] for call in backend.command.call_args_list if call.args[0][0] == "compose"]
        self.assertEqual([["compose", "--env-file", record["compose_env"], "-p", record["project"],
            "-f", record["compose_file"], "stop", "rest", "rest-mv", "spark"]], stops)
        inputs, python = self.relocated_sdk()
        # Exercise the actual native stop component inside the upgrade owner.
        self.backend.stop_writers = backend.stop_writers
        upgraded = self.owner.activate(self.owner.required_definition({"object_store": CLOUD}, inputs), upgrade=True)
        self.assertEqual("ready", upgraded["phase"])
        self.assertTrue(upgraded["steps"]["writers_stopped"])
        self.assertEqual(str(python), upgraded["oss_ops_python"])
        self.assertEqual(upgraded["generation"], self.owner.check_ready(inputs, {"object_store": CLOUD})["generation"])

    def test_native_missing_sdk_stop_still_rejects_foreign_docker_resources(self):
        record = self.start()
        self.python.unlink()
        for fault in ("container-id", "container-label", "container-image", "network", "volume"):
            with self.subTest(fault=fault):
                backend = self.native_backend_for(record, fault)
                self.assert_code("RuntimeIdentityMismatch", lambda: backend.stop_writers(record))
                self.assertFalse(any(call.args[0][0] == "compose" for call in backend.command.call_args_list))

    def test_native_missing_sdk_stop_still_rejects_changed_activation_configuration(self):
        record = self.start()
        self.python.unlink()
        Path(record["object_store_config_file"]).write_text(json.dumps({**CLOUD, "bucket": "foreign-bucket"}))
        backend = self.native_backend_for(record)
        self.assert_code("RuntimeIdentityMismatch", lambda: backend.stop_writers(record))
        backend.command.assert_not_called()

    def test_host_sdk_unknown_exit_remains_maintenance_obligation(self):
        record = self.start()
        backend = object.__new__(machine.MachineDocker)
        journal = self.owner.base / "operations/sdk.json"
        fact = dict(schema=1, kind="host-sdk", phase="cleanup-failed", command_dispatched=True,
            machine=record["id"], control_root=record["control_root"])
        backend.unsettled_operations = mock.Mock(return_value=[(journal, fact)])
        self.assert_code("MachineOperationUnconfirmed", lambda: backend.settle_operations(self.owner.base))
        self.assertEqual("cleanup-failed", fact["phase"])
        fact["command_dispatched"] = False
        self.assert_code("MachineOperationUnconfirmed", lambda: backend.settle_operations(self.owner.base))
        fact["phase"] = "intent"
        backend.settle_operations(self.owner.base)
        self.assertEqual("settled", json.loads(journal.read_text())["phase"])

    def test_host_sdk_operation_uses_original_supervision_adapter(self):
        backend = object.__new__(machine.MachineDocker)
        with mock.patch.object(machine, "validate_cloud_snapshot"), \
                mock.patch("machine_operation.run_owned_host_sdk_operation", return_value='{"ok":true}') as run:
            self.assertEqual('{"ok":true}', backend.run_oss({"generation": "g"}, ["stat", "--uri", "s3://bucket/key"], deadline=17))
        run.assert_called_once_with(backend, {"generation": "g"}, ["stat", "--uri", "s3://bucket/key"], deadline=17)


if __name__ == "__main__":
    unittest.main()
