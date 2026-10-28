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

"""Machine authority tests; Docker effects are observed independently."""
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import socket
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_runtime as runtime
import machine_runtime as machine


def bom():
    return {"schema": 2, "platform": "linux/arm64", "lock_sha256": "locked-inputs", "images": {
        n: {"alias": n, "manifest_digest": "sha256:" + n, "platform": "linux/arm64"}
        for n in ("iceberg-rest",)}, "derived_images": {
        n: {"alias": n + ":current", "image_id": "sha256:" + n,
            "definition_sha256": "definition-" + n, "platform": "linux/arm64"}
        for n in ("minio", "minio-mc", "iceberg-spark", "rest-mv")}}


class Backend:
    def __init__(self, root=None):
        self.events, self.version, self.online = [], None, False
        self.occupied, self.failure, self.counter = False, None, 0
        self.volumes = {}
        self.catalog_present = False

    labels = staticmethod(machine.MachineDocker.labels)
    minio_volume_identity = machine.MachineDocker.minio_volume_identity
    validate_minio_volume = machine.MachineDocker.validate_minio_volume
    ensure_minio_volume = machine.MachineDocker.ensure_minio_volume
    prepare_minio_volume = machine.MachineDocker.prepare_minio_volume
    ensure_authority_volume = machine.MachineDocker.ensure_authority_volume

    def inspect(self, kind, name, **kwargs):
        assert kind == "volume"
        return copy.deepcopy(self.volumes.get(name))

    def command(self, args):
        assert args[:2] == ["volume", "create"]
        labels = dict(value.split("=", 1) for index, value in enumerate(args)
                      if index > 0 and args[index - 1] == "--label")
        self.volumes.setdefault(args[-1], dict(Name=args[-1], CreatedAt="2026-10-21T01:00:00Z",
            Driver="local", Scope="local", Options=None, Labels=labels))
        return args[-1]

    def daemon_id(self):
        return "local-daemon"

    def unsettled_operations(self, base):
        return []

    def settle_operations(self, base):
        pass

    def image_id(self, reference):
        return reference if reference.startswith("sha256:") else "sha256:" + reference

    def healthy(self, record):
        return self.online and self.version == record["version"] and self.failure != "health"

    def validate_absent(self, record):
        if self.occupied:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "fixed project exists")

    def ensure_machine(self, record, previous=None):
        self.validate_minio_volume(record)
        self.events.append("ensure")
        if self.failure == "ensure":
            raise runtime.RuntimeFailure("PortUnavailable", "occupied port")
        self.occupied = self.online = True
        self.catalog_present = True
        self.version = record["version"]
        self.counter += 1
        record["containers"] = {s: f"container-{s}-{self.counter}" for s in record["required_services"]}
        record["network_id"] = "fixed-network-id"

    def stop_writers(self, record):
        self.events.append("stop-writers")

    def rotate_root(self, record, previous):
        self.events.append("rotate-root")
        assert self.online

    def assert_old_root_revoked(self, previous):
        self.events.append("verify-fence")
        if self.failure == "fence":
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old access still works")

    def remove_rest_state(self, record):
        self.events.append("remove-rest-state")
        if self.failure == "remove-rest-state":
            raise runtime.RuntimeFailure("DockerOperationTimeout", "catalog removal outcome is unknown")
        self.catalog_present = False
        return self.rest_state_barrier(record)

    def rest_state_barrier(self, record):
        self.events.append("verify-catalog-absence")
        if self.catalog_present or self.failure == "catalog-absence":
            raise runtime.RuntimeFailure("CatalogRemovalUnconfirmed", "catalog absence is unconfirmed")
        return dict(schema=1, daemon_id=record["daemon_id"], generation=record["generation"],
                    volumes=[machine.PROJECT + "_" + s + "-catalog" for s in ("rest", "rest-mv")],
                    complete=True)

    def validate_resources(self, record):
        self.events.append("validate")

    def connect(self, record, container, alias):
        self.events.append(("connect", container, alias))

    def own_network(self, record):
        return {"Id": "fixed-network-id"}

    def disconnect(self, record, container):
        self.events.append(("disconnect", container))


class DerivedObjectStoreInputTests(unittest.TestCase):
    def test_object_store_uses_exact_derived_ids_and_definition_facts(self):
        inputs = bom()
        inputs["derived_images"]["minio"]["alias"] = "mutable-minio-alias"
        inputs["derived_images"]["minio-mc"]["alias"] = "mutable-mc-alias"
        backend = Backend()
        with mock.patch.object(backend, "image_id", wraps=backend.image_id) as inspect:
            images, facts = machine.input_facts(inputs, backend)
        self.assertEqual(images["minio"]["image_id"], "sha256:minio")
        self.assertEqual(images["mc"]["image_id"], "sha256:minio-mc")
        self.assertEqual(images["mc-init"], images["mc"])
        self.assertEqual(facts["minio"]["receipt"]["definition_sha256"], "definition-minio")
        self.assertEqual(facts["minio-mc"]["receipt"]["definition_sha256"], "definition-minio-mc")
        self.assertIn(mock.call("sha256:minio"), inspect.call_args_list)
        self.assertIn(mock.call("sha256:minio-mc"), inspect.call_args_list)
        self.assertNotIn(mock.call("mutable-minio-alias"), inspect.call_args_list)
        self.assertNotIn(mock.call("mutable-mc-alias"), inspect.call_args_list)

    def test_object_store_requires_derived_definition_and_exact_installed_id(self):
        for name in ("minio", "minio-mc"):
            with self.subTest(name=name, fault="definition"):
                inputs = bom()
                inputs["derived_images"][name].pop("definition_sha256")
                with self.assertRaisesRegex(runtime.RuntimeFailure, "derived definition identity is missing"):
                    machine.input_facts(inputs, Backend())
            with self.subTest(name=name, fault="image"):
                backend = Backend()
                original = backend.image_id
                with mock.patch.object(backend, "image_id", side_effect=lambda reference: "sha256:other" if reference == "sha256:" + name else original(reference)):
                    with self.assertRaisesRegex(runtime.RuntimeFailure, "derived image differs from verified receipt"):
                        machine.input_facts(bom(), backend)


class SupervisionPrerequisiteTests(unittest.TestCase):
    def assert_blocked_before_effects(self, prerequisite):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "not-created"
            with prerequisite, \
                    mock.patch.object(runtime.Docker, "command") as command, \
                    mock.patch.object(machine.subprocess, "Popen") as spawn, \
                    mock.patch.object(runtime, "atomic_json") as journal, \
                    mock.patch("sys.stderr"):
                self.assertEqual(machine.main(["--root", str(state), "status"]), 75)
                with self.assertRaisesRegex(runtime.RuntimeFailure, "FixturePrerequisiteMissing"):
                    machine.MachineDocker()
                command.assert_not_called()
                spawn.assert_not_called()
                journal.assert_not_called()
            self.assertFalse(state.exists())

    def test_old_python_rejects_before_docker_or_journal_creation(self):
        self.assert_blocked_before_effects(mock.patch.object(machine.sys, "version_info", (3, 9, 6)))

    def test_missing_native_wait_capability_rejects_before_docker_or_journal_creation(self):
        for name in ("waitid", "killpg", "WNOWAIT", "WEXITED", "WNOHANG", "P_PID"):
            with self.subTest(capability=name):
                self.assert_blocked_before_effects(mock.patch.object(machine.os, name, None))

    def test_unsupported_platform_or_clock_rejects_before_docker_or_journal_creation(self):
        self.assert_blocked_before_effects(mock.patch.object(machine.sys, "platform", "unsupported"))
        for monotonic, adjustable in ((False, False), (True, True)):
            self.assert_blocked_before_effects(mock.patch.object(machine.time, "get_clock_info",
                return_value=SimpleNamespace(monotonic=monotonic, adjustable=adjustable)))

    def test_missing_native_group_observation_rejects_before_docker_or_journal_creation(self):
        if machine.sys.platform == "darwin":
            import ctypes
            prerequisite = mock.patch.object(ctypes, "CDLL", return_value=SimpleNamespace())
        else:
            prerequisite = mock.patch.object(machine.Path, "is_file", return_value=False)
        self.assert_blocked_before_effects(prerequisite)

    def test_fake_backend_does_not_require_native_supervision(self):
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.object(machine.sys, "version_info", (3, 9, 6)), \
                mock.patch.object(machine.os, "waitid", None):
            backend = Backend()
            owner = machine.MachineOwner(directory, backend=backend, sweeper_factory=Sweeper)
            self.assertEqual(owner.activate(owner.required_definition({}, bom()))["phase"], "ready")


class McTimeoutClassificationTests(unittest.TestCase):
    def invoke(self, changes=None, output="NOVA_SCRATCH_MC_EXIT=0\n", *, absent_stdout=False):
        with tempfile.TemporaryDirectory() as directory:
            backend = machine.MachineDocker()
            backend.maintenance_fd = 42
            record = dict(kind="machine", namespace="owned", control_root=directory,
                version="v", generation="g", network="owned-network",
                compose_file=str(Path(directory) / "compose.yml"),
                credentials={"access_key":"key", "secret_key":"secret"},
                images={"mc":{"image_id":"sha256:mc"}})
            def supervise(journal, request, timeout):
                fact = runtime.read_json(journal)
                fact.update(phase="settled", create_dispatched=True, container_id="exact-owned-id",
                    container_absent=True, group_exit_confirmed=True, group_exit_observed=True,
                    leader_waited=True, command_exit=0, timed_out=False, command_succeeded=True)
                fact.update(changes or {})
                if not absent_stdout:
                    if isinstance(output, bytes):
                        journal.with_suffix(".stdout").write_bytes(output)
                    else:
                        journal.with_suffix(".stdout").write_text(output)
                return fact
            with mock.patch.object(backend, "_supervise", side_effect=supervise):
                return backend.run_mc(record, "private MC script")

    def test_success_returns_original_output_and_native_nonzero_stays_failure(self):
        self.assertEqual(self.invoke(), "NOVA_SCRATCH_MC_EXIT=0\n")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "DockerOperationFailed"):
            self.invoke({"command_succeeded":False, "command_exit":17})

    def test_confirmed_cleaned_timeout_preserves_private_native_output(self):
        output = "private-native-SECRET\nNOVA_SCRATCH_MC_EXIT=17\n"
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            self.invoke({"command_succeeded":False, "timed_out":True}, output)
        self.assertEqual(caught.exception.code, "DockerOperationTimeout")
        self.assertEqual(caught.exception.stdout, output)
        self.assertNotIn("SECRET", str(caught.exception))
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            self.invoke({"command_succeeded":False, "timed_out":True,
                         "create_dispatched":False}, absent_stdout=True)
        self.assertEqual(caught.exception.code, "DockerOperationTimeout")
        self.assertIsNone(caught.exception.stdout)

    def test_missing_started_output_or_unreadable_native_bytes_stays_failure(self):
        changes = {"command_succeeded":False, "timed_out":True, "start_dispatched":True}
        with self.assertRaisesRegex(runtime.RuntimeFailure, "DockerOperationFailed"):
            self.invoke(changes, absent_stdout=True)
        with self.assertRaisesRegex(runtime.RuntimeFailure, "DockerOperationFailed"):
            self.invoke(changes, b"\xff\x80")

    def test_unknown_cleanup_or_observation_or_cancellation_is_never_typed_timeout(self):
        for field, value in (("phase","cleanup-failed"), ("container_absent",False),
                ("container_name","foreign"), ("container_id",None), ("group_exit_confirmed",False),
                ("group_exit_observed",False), ("leader_waited",False), ("interrupted",True),
                ("error","ContainerCleanupFailed"), ("group_observation_error","OSError"),
                ("leader_observation_error","OSError"), ("command_exit",17), ("create_exit",17)):
            with self.subTest(field=field), self.assertRaises(runtime.RuntimeFailure) as caught:
                self.invoke({"command_succeeded":False, "timed_out":True, field:value})
            self.assertEqual(caught.exception.code, "DockerOperationFailed")


class ReadonlyControlTimeoutTests(unittest.TestCase):
    def invoke(self, changes=None, *, args=None, stdout='{"Id":"observed"}', stderr="", undispatched=False,
               missing_output=False):
        with tempfile.TemporaryDirectory() as directory:
            backend = machine.MachineDocker()
            backend.maintenance_fd = 42
            backend.maintenance_base = Path(directory) / "machine-test"
            def supervise(journal, request, timeout):
                fact = runtime.read_json(journal)
                fact.update(phase="settled", command_dispatched=not undispatched, command_exit=124 if undispatched else 0,
                    timed_out=True, interrupted=False, command_succeeded=False)
                if not undispatched:
                    fact.update(cli_pid=12345, group_exit_confirmed=True, group_exit_observed=True, leader_waited=True)
                fact.update(changes or {})
                if not missing_output:
                    output = journal.with_suffix(".stdout")
                    output.write_bytes(stdout if isinstance(stdout, bytes) else stdout.encode())
                    Path(str(output) + ".stderr").write_text(stderr)
                return fact
            with mock.patch.object(backend, "_supervise", side_effect=supervise):
                try:
                    backend.command(args or ["container", "inspect", "owned-id", "--format", "{{json .}}"])
                except runtime.RuntimeFailure as error:
                    return error
            self.fail("deadline command unexpectedly succeeded")

    def test_undispatched_readonly_deadline_and_reaped_readonly_exit_carry_private_evidence(self):
        for kwargs in ({"undispatched":True,"missing_output":True}, {}, {"changes":{"command_exit":-machine.signal.SIGTERM}}):
            with self.subTest(kwargs=kwargs):
                error = self.invoke(**kwargs)
                self.assertEqual(error.code, "DockerOperationTimeout", "ordinary commands retain their failure")
                self.assertIs(error.readonly_deadline_settled, True)

    def test_mutating_command_never_has_readonly_deferral_evidence(self):
        error = self.invoke(args=["container", "rm", "owned-id"])
        self.assertEqual(error.code, "DockerOperationTimeout")
        self.assertIs(error.readonly_deadline_settled, False)

    def test_failed_cleanup_unknown_group_wrong_identity_or_native_failure_is_not_deferrable(self):
        for field,value in (("phase","cleanup-failed"),("mutating",True),("interrupted",True),
                ("group_exit_confirmed",False),("group_exit_observed",False),("leader_waited",False),
                ("cli_pid",None),("control_root","another-root"),("machine","another-machine"),
                ("error","ContainerCleanupFailed"),("cleanup_error","OSError"),
                ("group_observation_error","OSError"),("leader_observation_error","OSError"),
                ("command_exit",17),("command_exit",False),("command_dispatched",None)):
            with self.subTest(field=field,value=value):
                self.assertIs(self.invoke({field:value}).readonly_deadline_settled, False)

    def test_invalid_or_missing_native_output_and_error_stderr_are_not_deferrable(self):
        for kwargs in ({"stdout":"invalid JSON"},{"stdout":"[]"},{"stdout":b"\xff"},
                {"missing_output":True},{"stderr":"Error response from daemon: denied"},
                {"undispatched":True,"stdout":"unexpected dispatched output"},
                {"undispatched":True,"missing_output":True,"changes":{"cli_pid":12345}}):
            with self.subTest(kwargs=kwargs):
                self.assertIs(self.invoke(**kwargs).readonly_deadline_settled, False)


class FailedReadonlyObservationRecoveryTests(unittest.TestCase):
    def fixture(self, directory):
        directory = str(Path(directory).resolve())
        backend = machine.MachineDocker()
        daemon = "test-daemon"
        base = Path(directory) / ("machine-" + runtime.digest(daemon)[:16])
        path = base / "operations" / ("control-" + "a" * 32 + ".json")
        state = dict(schema=machine.PROTOCOL, kind="machine", id=base.name, daemon_id=daemon,
                     control_root=directory, project=machine.PROJECT,
                     required_services=["minio", "mc-init", "rest", "rest-mv", "spark"])
        fact = dict(schema=1, kind="control", phase="cleanup-failed", error="PermissionError",
                    control_root=directory, machine=base.name, mutating=False,
                    command_dispatched=True, cli_pid=12345, group_exit_confirmed=False)
        request = dict(operation="control", deadline_monotonic=1234.5, environment={"PATH": "/usr/bin"},
                       args=["ps", "-a", "--filter", "label=com.docker.compose.project=" + machine.PROJECT,
                             "--filter", "label=com.docker.compose.service=rest-mv", "--format", "{{.ID}}"])
        for name, value in ((base / "state.json", state), (path, fact), (path.with_suffix(".request"), request)):
            runtime.atomic_json(name, value)
        backend.maintenance_fd, backend.maintenance_base = 123, base
        return backend, base, path, fact, request

    def test_explicit_settlement_preserves_failure_and_unknown_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            backend, base, path, fact, request = self.fixture(directory)
            self.assertEqual(backend.unsettled_operations(base), [(path, fact)])
            with mock.patch.object(backend, "command", side_effect=AssertionError("no new Docker command")):
                self.assertTrue(backend.reconcile_failed_readonly_control(path, runtime.read_json(path), base))
            result = runtime.read_json(path)
            self.assertEqual({k: v for k, v in result.items() if k != "observation_disposition"}, fact)
            self.assertNotIn("leader_waited", result)
            self.assertNotIn("group_exit_observed", result)
            self.assertEqual(result["observation_disposition"]["mutation_obligation"], "none")
            self.assertEqual(result["phase"], "cleanup-failed")
            self.assertEqual(backend.unsettled_operations(base), [])
            self.assertEqual(backend.unsettled_operations(base, include_disposed=True), [(path, result)])
            self.assertEqual(runtime.read_json(path.with_suffix(".request")), request)
            status = backend.failed_observation_status(base)
            self.assertEqual(status[0]["error"], "PermissionError")
            self.assertTrue(status[0]["disposition_valid"])

    def test_undisposed_readonly_requires_explicit_owner_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            backend, base, path, fact, _ = self.fixture(directory)
            backend.maintenance_fd = None
            self.assertFalse(backend.reconcile_failed_readonly_control(path, fact, base))
            self.assertEqual(runtime.read_json(path), fact)
            self.assertEqual(len(backend.unsettled_operations(base)), 1)

    def test_mutation_or_forged_request_remains_blocked(self):
        mutations = ("mutating", "exec", "unknown-verb", "mutation-with-inspect-word", "foreign-machine")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                backend, base, path, fact, request = self.fixture(directory)
                if mutation == "mutating": fact["mutating"] = True
                if mutation == "exec": request["args"] = ["exec", "container", "sh"]
                if mutation == "unknown-verb": request["args"] = ["unknown", "inspect", "container"]
                if mutation == "mutation-with-inspect-word": request["args"] = ["run", "inspect", "container"]
                if mutation == "foreign-machine": fact["machine"] = "machine-foreign"
                runtime.atomic_json(path, fact)
                runtime.atomic_json(path.with_suffix(".request"), request)
                self.assertFalse(backend.reconcile_failed_readonly_control(path, fact, base))
                self.assertEqual(runtime.read_json(path), fact)
                self.assertEqual(len(backend.unsettled_operations(base)), 1)

    def test_readonly_verbs_and_different_original_failure_shapes_are_preserved(self):
        cases = (["info", "--format", "{{.ID}}"], ["version"], ["inspect", "container"],
                 ["context", "show"], ["context", "inspect", "local"],
                 ["container", "inspect", "container"], ["image", "inspect", "image"],
                 ["network", "inspect", "network"], ["volume", "inspect", "volume"],
                 ["ps", "--all", "--format", "{{.ID}}"])
        for index, args in enumerate(cases):
            with self.subTest(args=args), tempfile.TemporaryDirectory() as directory:
                backend, base, path, fact, request = self.fixture(directory)
                fact.update(phase="command-unknown", error="ArbitraryOriginalFailure",
                            command_exit=1, leader_waited=True, group_exit_observed=True,
                            group_exit_confirmed=True, command_succeeded=False, timed_out=False)
                if index % 2:
                    fact.pop("error")
                    fact.pop("leader_waited")
                    fact.pop("group_exit_observed")
                request["args"] = args
                runtime.atomic_json(path, fact)
                runtime.atomic_json(path.with_suffix(".request"), request)
                self.assertTrue(backend.readonly_command(args))
                self.assertTrue(backend.reconcile_failed_readonly_control(path, fact, base))
                saved = runtime.read_json(path)
                self.assertEqual({k: v for k, v in saved.items() if k != "observation_disposition"}, fact)
                self.assertEqual(backend.unsettled_operations(base), [])
                self.assertTrue(backend.failed_observation_status(base)[0]["disposition_valid"])
                self.assertEqual(runtime.read_json(path.with_suffix(".request")), request)

    def test_copied_disposition_cannot_hide_changed_request_or_failure(self):
        for change in ("request", "failure"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                backend, base, path, _, request = self.fixture(directory)
                self.assertTrue(backend.reconcile_failed_readonly_control(path, runtime.read_json(path), base))
                result = runtime.read_json(path)
                if change == "request":
                    request["args"] = ["rm", "-f", "container"]
                    runtime.atomic_json(path.with_suffix(".request"), request)
                else:
                    result["cli_pid"] += 1
                    runtime.atomic_json(path, result)
                self.assertEqual(len(backend.unsettled_operations(base)), 1)
                self.assertFalse(backend.failed_observation_status(base)[0]["disposition_valid"])


class ReadonlyObservationOwnerTests(unittest.TestCase):
    def fixture(self, directory):
        backend, base, path, fact, request = FailedReadonlyObservationRecoveryTests().fixture(directory)
        state = runtime.read_json(base / "state.json")
        state.update(phase="ready", credentials={"secret_key": "must-not-escape"})
        runtime.atomic_json(base / "state.json", state)
        owner = machine.MachineOwner(directory, backend=backend, daemon="test-daemon")
        return owner, backend, base, path, fact, request

    def test_exact_owner_resolution_is_idempotent_and_does_not_sweep(self):
        with tempfile.TemporaryDirectory() as directory:
            owner, backend, base, path, fact, request = self.fixture(directory)
            with mock.patch.object(backend, "daemon_id", return_value="test-daemon"), \
                    mock.patch.object(backend, "settle_operations", side_effect=AssertionError("no operation loop")), \
                    mock.patch.object(backend, "command", side_effect=AssertionError("no Docker mutation")):
                first = owner.resolve_readonly_observation(path)
                identity = backend._terminal_material(path)
                second = owner.resolve_readonly_observation(path)
                self.assertEqual(backend._terminal_material(path), identity)
            self.assertEqual(first, second)
            self.assertTrue(first["disposition_valid"])
            self.assertEqual(first["phase"], "cleanup-failed")
            self.assertFalse(first["original_completion"]["group_exit_confirmed"])
            self.assertNotIn("leader_waited", first["original_completion"])
            self.assertNotIn("must-not-escape", json.dumps(first))
            self.assertEqual(runtime.read_json(path.with_suffix(".request")), request)
            self.assertIsNone(getattr(backend, "operation_deadline", None))

    def test_mutation_foreign_path_or_daemon_is_rejected_without_rewrite(self):
        for kind in ("mutation", "foreign-path", "daemon"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                owner, backend, base, path, fact, _ = self.fixture(directory)
                if kind == "mutation":
                    fact["mutating"] = True
                    runtime.atomic_json(path, fact)
                before = path.read_bytes()
                target = path if kind != "foreign-path" else base.parent / "foreign" / path.name
                daemon = "foreign-daemon" if kind == "daemon" else "test-daemon"
                with mock.patch.object(backend, "daemon_id", return_value=daemon):
                    with self.assertRaises(runtime.RuntimeFailure): owner.resolve_readonly_observation(target)
                self.assertEqual(path.read_bytes(), before)

    def test_cli_dispatches_only_exact_journal_and_redacts_output(self):
        resolver = mock.Mock(return_value={"phase": "cleanup-failed", "error": "OriginalFailure",
                                          "credentials": {"secret_key": "must-not-escape"}})
        owner = SimpleNamespace(resolve_readonly_observation=resolver)
        target = Path("/exact/machine/operations/control.json")
        output = io.StringIO()
        with mock.patch.object(machine, "MachineOwner", return_value=owner), \
                mock.patch.object(runtime, "install_signal_handlers"), mock.patch.object(sys, "stdout", output):
            self.assertEqual(machine.main(["resolve-readonly-observation", "--journal", str(target)]), 0)
        resolver.assert_called_once_with(target)
        self.assertNotIn("must-not-escape", output.getvalue())
        self.assertEqual(json.loads(output.getvalue())["error"], "OriginalFailure")

    def test_cli_preserves_owner_refusal_as_error(self):
        owner = SimpleNamespace(resolve_readonly_observation=mock.Mock(side_effect=
            runtime.RuntimeFailure("MachineOperationUnconfirmed", "not a readonly request")))
        error = io.StringIO()
        with mock.patch.object(machine, "MachineOwner", return_value=owner), \
                mock.patch.object(runtime, "install_signal_handlers"), mock.patch.object(sys, "stderr", error):
            self.assertEqual(machine.main(["resolve-readonly-observation", "--journal", "/exact/control.json"]), 1)
        self.assertIn("MachineOperationUnconfirmed", error.getvalue())


class TerminalPortRecoveryTests(unittest.TestCase):
    def fixture(self, directory):
        base = Path(directory) / "machine-test"
        path = base / "operations/control-test.json"
        fact = dict(schema=1, kind="control", phase="command-unknown", mutating=True,
            command_dispatched=True, command_exit=1, timed_out=False, group_exit_observed=True,
            control_root=directory, machine=base.name, maintenance_generation="activation-g", maintenance_version="v")
        state = dict(phase="maintenance", generation="activation-g", version="v")
        runtime.atomic_json(base / "state.json", state)
        runtime.atomic_json(path, fact)
        runtime.atomic_json(path.with_suffix(".request"), {"args": ["compose", "-f", "saved.yml", "up", "-d"]})
        stderr = Path(str(path.with_suffix(".stdout")) + ".stderr")
        stderr.write_text("Error response from daemon: failed to set up container networking: driver failed programming external connectivity on endpoint owned: Bind for 127.0.0.1:28000 failed: port is already allocated\n")
        return machine.MachineDocker(), base, path, fact, state, stderr

    def test_native_terminal_port_failure_allows_same_intent_retry_after_exact_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            backend, base, path, fact, state, _ = self.fixture(directory)
            with mock.patch.object(backend, "activation_preflight") as preflight:
                backend.settle_operations(base)
                preflight.assert_called_once_with(state, None)
            result = runtime.read_json(path)
            self.assertEqual(result["phase"], "settled")
            self.assertFalse(result["command_succeeded"])
            self.assertEqual(result["terminal_failure"], "daemon-port-rejected")
            self.assertEqual(runtime.read_json(base / "state.json")["phase"], "maintenance")

    def test_timeout_transport_missing_group_or_different_intent_stays_unknown(self):
        for mutation in ("timeout", "transport", "group", "intent"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                backend, base, path, fact, state, stderr = self.fixture(directory)
                if mutation == "timeout": fact["timed_out"] = True
                if mutation == "transport": stderr.write_text("Cannot connect to the Docker daemon; port is already allocated\n")
                if mutation == "group": fact["group_exit_observed"] = False
                if mutation == "intent": fact["maintenance_generation"] = "old-activation"
                runtime.atomic_json(path, fact)
                with mock.patch.object(backend, "activation_preflight") as preflight:
                    with self.assertRaisesRegex(runtime.RuntimeFailure, "MachineOperationUnconfirmed"):
                        backend.settle_operations(base)
                    preflight.assert_not_called()
                self.assertEqual(runtime.read_json(path)["phase"], "command-unknown")

    def test_native_rejection_cannot_bypass_foreign_resource_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            backend, base, path, _, _, _ = self.fixture(directory)
            with mock.patch.object(backend, "activation_preflight", side_effect=runtime.RuntimeFailure("RuntimeIdentityMismatch", "foreign service")):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "RuntimeIdentityMismatch"):
                    backend.settle_operations(base)
            self.assertEqual(runtime.read_json(path)["phase"], "command-unknown")


class CredentialFenceTests(unittest.TestCase):
    def probe_output(self, output):
        backend = machine.MachineDocker()
        with mock.patch.object(backend, "run_mc", return_value=output) as run:
            backend.assert_old_root_revoked({"saved": "old private env"})
            script = run.call_args.args[1]
            self.assertNotIn("|| true", script)
            self.assertIn("nr_fence_exit=$?", script)

    def probe(self, value, native_exit=1):
        self.probe_output(json.dumps(value, indent=2) + f"\nNOVA_FIXTURE_FENCE_EXIT={native_exit}\n")

    @staticmethod
    def denial(code="InvalidAccessKeyId"):
        return {"status": "error", "error": {"cause": {"error": {"Code": code}}}}

    @staticmethod
    def summary():
        return {"status": "error", "error": {
            "message": "Unable to stat `prior/warehouse`.", "type": "fatal",
            "cause": {"message": "Object does not exist", "error": {}}}}

    def stream(self, values, native_exit=1):
        return "\n".join(json.dumps(value, indent=2) for value in values) + f"\nNOVA_FIXTURE_FENCE_EXIT={native_exit}\n"

    def test_indented_native_access_rejection_proves_fence(self):
        self.probe({"status": "error", "error": {"message": "stat failed", "cause": {
            "error": {"Code": "InvalidAccessKeyId", "Message": "The access key does not exist"}}}})

    def test_root_success_transport_failure_or_diagnostic_substring_is_not_a_fence(self):
        cases = [
            ({"status": "success"}, 0),
            ({"status": "error", "error": {"cause": {"error": {"Code": "AccessDenied"}}}}, 0),
            ({"status": "error", "error": {"message": "AccessDenied", "cause": {"error": {"Code": "ConnectionRefused"}}}}, 1),
            ({"status": "error", "error": {"message": "SignatureDoesNotMatch"}}, 1),
        ]
        for value, native_exit in cases:
            with self.subTest(value=value, native_exit=native_exit):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "CredentialFenceUnconfirmed"):
                    self.probe(value, native_exit)

    def test_native_stat_denial_stream_and_exact_final_summary_prove_fence(self):
        # Native mc reports the S3 rejection, then stat's ObjectMissing summary.
        self.probe_output(self.stream([self.denial(), self.summary()]))
        self.probe_output(self.stream([self.denial(), self.denial("AccessDenied"), self.summary()]))
        self.probe_output(self.stream([self.denial(), self.denial("InvalidToken")]))

    def test_native_summary_cannot_prove_fence_or_hide_another_failure(self):
        cases = [
            [self.summary()],
            [self.summary(), self.denial()],
            [self.denial(), self.summary(), self.summary()],
            [self.denial(), {"status": "success"}, self.summary()],
            [self.denial(), self.denial("ConnectionRefused"), self.summary()],
            [self.denial(), self.denial("InternalError"), self.summary()],
            [self.denial(), self.denial("NoSuchBucket"), self.summary()],
            [self.denial(), {"status": "error", "error": {"type": "fatal", "message": "AccessDenied"}}],
        ]
        for values in cases:
            with self.subTest(values=values):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "CredentialFenceUnconfirmed"):
                    self.probe_output(self.stream(values))

    def test_summary_requires_exact_target_cause_and_shape(self):
        for mutation in ("target", "cause", "native", "extra", "type"):
            summary = self.summary()
            if mutation == "target": summary["error"]["message"] = "Unable to stat `other/bucket`."
            if mutation == "cause": summary["error"]["cause"]["message"] = "Connection refused"
            if mutation == "native": summary["error"]["cause"]["error"]["Code"] = "InternalError"
            if mutation == "extra": summary["error"]["extra"] = "unknown"
            if mutation == "type": summary["error"]["type"] = "error"
            with self.subTest(mutation=mutation):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "CredentialFenceUnconfirmed"):
                    self.probe_output(self.stream([self.denial(), summary]))

    def test_stream_requires_nonzero_native_footer_and_complete_unambiguous_json(self):
        valid = self.stream([self.denial(), self.summary()])
        body = valid[:valid.index("NOVA_FIXTURE_FENCE_EXIT=")]
        cases = [
            "NOVA_FIXTURE_FENCE_EXIT=1\n",
            body,
            self.stream([self.denial(), self.summary()], 0),
            body + "NOVA_FIXTURE_FENCE_EXIT=invalid\n",
            body + "NOVA_FIXTURE_FENCE_EXIT=256\n",
            valid + "unknown trailing diagnostic\n",
            json.dumps(self.denial()) + '\n{"status":"error"\nNOVA_FIXTURE_FENCE_EXIT=1\n',
            body + 'not JSON\nNOVA_FIXTURE_FENCE_EXIT=1\n',
            '{"status":"success","status":"error","error":{"code":"AccessDenied"}}\nNOVA_FIXTURE_FENCE_EXIT=1\n',
            '{"status":"error","error":{"code":"AccessDenied"},"unknown":NaN}\nNOVA_FIXTURE_FENCE_EXIT=1\n',
        ]
        for output in cases:
            with self.subTest(output=output):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "CredentialFenceUnconfirmed"):
                    self.probe_output(output)

    def test_auth_code_cannot_hide_native_http_server_failure(self):
        for status in (500, 503, "403", True):
            denial = self.denial()
            denial["error"]["cause"]["error"]["StatusCode"] = status
            with self.subTest(status=status):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "CredentialFenceUnconfirmed"):
                    self.probe_output(self.stream([denial, self.summary()]))



class SnapshotDeadlineTests(unittest.TestCase):
    def test_command_expiry_before_dispatch_retains_exact_readonly_fact(self):
        backend = machine.MachineDocker()
        backend.operation_deadline = 99.0
        backend.maintenance_fd = 42
        for args, readonly in ((["info"], True), (["container", "inspect", "owned"], True),
                               (["container", "rm", "owned"], False)):
            with self.subTest(args=args), \
                    mock.patch.object(machine.time, "monotonic", return_value=100.0), \
                    mock.patch.object(runtime.Docker, "command") as command, \
                    mock.patch.object(machine.subprocess, "Popen") as spawn, \
                    mock.patch.object(runtime, "atomic_json") as journal:
                with self.assertRaises(machine.MachineCommandDeadlineExpired) as caught:
                    backend.command(args)
                self.assertEqual(caught.exception.code, "RunBudgetExpired")
                self.assertIs(caught.exception.readonly_not_dispatched, readonly)
                command.assert_not_called()
                spawn.assert_not_called()
                journal.assert_not_called()

    def test_short_lock_deadline_does_not_shorten_validation_or_outer_run_clock(self):
        backend = Backend()
        with tempfile.TemporaryDirectory() as directory:
            owner = machine.MachineOwner(directory, backend=backend, daemon="local-daemon")
            owner._save(dict(schema=machine.PROTOCOL, daemon_id=owner.daemon_id,
                control_root=str(owner.root), phase="ready", version="v", generation="g"))
            now = [100.0]
            backend.operation_deadline = 107.0
            def healthy(state):
                self.assertEqual(backend.operation_deadline, 107.0)
                now[0] = 103.0
                return True
            with mock.patch.object(machine.time, "monotonic", side_effect=lambda: now[0]), \
                    mock.patch.object(backend, "healthy", side_effect=healthy), \
                    mock.patch.object(backend, "validate_minio_volume"):
                with owner.ready_snapshot({"version": "v"}, deadline=190.0, lock_deadline=102.0):
                    self.assertEqual(backend.operation_deadline, 107.0)
                    self.assertGreater(now[0], 102.0)
            self.assertEqual(backend.operation_deadline, 107.0)

    def test_validation_still_expires_at_original_outer_deadline_and_restores_it(self):
        backend = Backend()
        with tempfile.TemporaryDirectory() as directory:
            owner = machine.MachineOwner(directory, backend=backend, daemon="local-daemon")
            owner._save(dict(schema=machine.PROTOCOL, daemon_id=owner.daemon_id,
                control_root=str(owner.root), phase="ready", version="v", generation="g"))
            now = [100.0]
            backend.operation_deadline = 104.0
            def healthy(state):
                self.assertEqual(backend.operation_deadline, 104.0)
                now[0] = 104.0
                return True
            with mock.patch.object(machine.time, "monotonic", side_effect=lambda: now[0]), \
                    mock.patch.object(backend, "healthy", side_effect=healthy), \
                    mock.patch.object(backend, "validate_minio_volume"):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "RunBudgetExpired"):
                    with owner.ready_snapshot({"version": "v"}, deadline=190.0, lock_deadline=102.0):
                        self.fail("expired outer run budget yielded readiness")
            self.assertEqual(backend.operation_deadline, 104.0)

    def test_expired_admitted_clock_precedes_daemon_probe(self):
        backend = Backend()
        with tempfile.TemporaryDirectory() as directory:
            owner = machine.MachineOwner(directory, backend=backend, daemon="local-daemon")
            with mock.patch.object(backend, "daemon_id") as probe:
                with self.assertRaisesRegex(runtime.RuntimeFailure, "RunBudgetExpired"):
                    with owner.ready_snapshot({}, deadline=time.monotonic() - 1):
                        self.fail("expired scope must not be entered")
                probe.assert_not_called()
            self.assertIsNone(backend.operation_deadline)

    def test_slow_health_headers_cannot_renew_original_deadline(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(2)
        stopped = threading.Event()
        def serve():
            try:
                with listener.accept()[0] as connection:
                    connection.recv(4096)
                    for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n":
                        connection.sendall(bytes([byte]))
                        if stopped.wait(0.03):
                            break
            except OSError:
                pass
        worker = threading.Thread(target=serve)
        worker.start()
        backend = machine.MachineDocker()
        backend.operation_deadline = time.monotonic() + 0.2
        start = time.monotonic()
        try:
            self.assertFalse(backend.http_ready(f"http://127.0.0.1:{listener.getsockname()[1]}/v1/config"))
            self.assertLess(time.monotonic() - start, 1.0)
        finally:
            stopped.set()
            listener.close()
            worker.join(timeout=3)
        self.assertFalse(worker.is_alive())

    def test_snapshot_deadline_covers_health_and_body_then_restores_outer_clock(self):
        backend = Backend()
        with tempfile.TemporaryDirectory() as directory:
            owner = machine.MachineOwner(directory, backend=backend, daemon="local-daemon")
            owner._save(dict(schema=machine.PROTOCOL, daemon_id=owner.daemon_id,
                control_root=str(owner.root), phase="ready", version="v", generation="g"))
            outer = time.monotonic() + 20
            inner = time.monotonic() + 5
            backend.operation_deadline = outer
            def healthy(state):
                self.assertEqual(backend.operation_deadline, inner)
                return True
            with mock.patch.object(backend, "healthy", side_effect=healthy), mock.patch.object(backend, "validate_minio_volume"):
                with owner.ready_snapshot({"version": "v"}, deadline=inner):
                    self.assertEqual(backend.operation_deadline, inner)
            self.assertEqual(backend.operation_deadline, outer)
            with mock.patch.object(backend, "healthy", side_effect=RuntimeError("probe failed")), mock.patch.object(backend, "validate_minio_volume"):
                with self.assertRaisesRegex(RuntimeError, "probe failed"):
                    with owner.ready_snapshot({"version": "v"}, deadline=inner):
                        pass
            self.assertEqual(backend.operation_deadline, outer)


class Sweeper:
    def __init__(self, record, backend):
        self.backend = backend
        self.record = record

    def revoke_users(self):
        self.backend.events.append("revoke-users")

    def sweep(self, *, expired_only, deadline):
        assert self.backend.online, "purge requires an online MinIO"
        if not expired_only:
            assert self.record["offline_catalog_barrier"]["complete"]
            assert not self.backend.catalog_present
        self.backend.events.append(("sweep", expired_only))
        return {"complete": self.backend.failure != "purge", "removed": []}


class MachineTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.backend = Backend()
        self.owner = machine.MachineOwner(self.root / "state", backend=self.backend,
                                          sweeper_factory=Sweeper, timeout=0.01)

    def definition(self, **config):
        return self.owner.required_definition(config, bom())

    def start(self):
        return self.owner.activate(self.definition())

    def changed(self):
        return self.definition(ports={"rest": 31002})

    def test_readonly_check_missing_machine_creates_no_state_or_lock(self):
        self.assertFalse(self.owner.root.exists())
        self.assert_code("MachineUnavailable", lambda: self.owner.check_ready(bom()))
        self.assertFalse(self.owner.root.exists())
        self.assertEqual(self.backend.events, [])
        self.assertIsNone(getattr(self.backend, "maintenance_fd", None))

    def test_readonly_check_reuses_exact_readiness_without_rewrites_or_journals(self):
        original = self.start()
        before = {str(path.relative_to(self.owner.root)): (path.stat().st_mtime_ns, path.read_bytes())
                  for path in self.owner.root.rglob("*") if path.is_file()}
        events = list(self.backend.events)
        def healthy(state):
            self.assertIsNone(getattr(self.backend, "maintenance_fd", None))
            self.assertIsNone(getattr(self.backend, "maintenance_base", None))
            return True
        with mock.patch.object(self.backend, "healthy", side_effect=healthy):
            actual = self.owner.check_ready(bom())
        self.assertEqual(actual["generation"], original["generation"])
        self.assertEqual(actual["version"], original["version"])
        self.assertTrue(actual["observed_healthy"])
        after = {str(path.relative_to(self.owner.root)): (path.stat().st_mtime_ns, path.read_bytes())
                 for path in self.owner.root.rglob("*") if path.is_file()}
        self.assertEqual(after, before)
        self.assertEqual(self.backend.events, events)
        changed = bom(); changed["lock_sha256"] = "another-source-lock"
        self.assert_code("MachineVersionMismatch", lambda: self.owner.check_ready(changed))
        self.assert_code("MachineVersionMismatch", lambda: self.owner.check_ready(bom(), {"ports": {"rest": 31002}}))
        self.backend.failure = "health"
        self.assert_code("MachineUnavailable", lambda: self.owner.check_ready(bom()))

    def test_readonly_check_cannot_cross_an_existing_maintenance_lock(self):
        self.start()
        observer = machine.MachineOwner(self.owner.root, backend=copy.deepcopy(self.backend), timeout=0.01)
        with self.owner.maintenance_lock():
            self.assert_code("MachineMaintaining", lambda: observer.check_ready(bom()))

    def test_cli_check_uses_original_bom_and_safe_snapshot(self):
        original = self.start()
        with mock.patch.object(machine, "MachineOwner", return_value=self.owner), \
                mock.patch.object(machine, "verified_bom", return_value=bom()) as verify, \
                mock.patch.object(machine.runtime, "install_signal_handlers"), \
                mock.patch("sys.stdout", new_callable=io.StringIO) as output:
            self.assertEqual(machine.main(["check", "--store", "/original-store"]), 0)
        verify.assert_called_once_with(runtime.SCRIPT_DIR.parent.parent, Path("/original-store"), consumer="iceberg-rest")
        actual = json.loads(output.getvalue())
        self.assertEqual(actual["phase"], "ready")
        self.assertEqual(actual["generation"], original["generation"])
        self.assertNotIn("credentials", actual)

    def test_machine_configuration_rejects_protected_and_rest_domain_overlap_before_effects(self):
        for root in ("s3://warehouse/fixture-machine", "s3://warehouse/fixture-machine/rest/",
                     "s3://warehouse/fixture-machine/rest/data", "s3://warehouse/fixture-machine/rest-mv"):
            with self.subTest(root=root):
                self.assert_code("InvalidConfiguration", lambda: self.definition(benchmark_root=root))
        self.assertEqual(self.backend.events, [])
        self.definition(benchmark_root="s3://warehouse/fixture-machine/rest-other")

    def assert_code(self, code, operation):
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            operation()
        self.assertEqual(caught.exception.code, code)

    def attach(self, definition, generation=None):
        with self.owner.ready_snapshot(definition, expected_generation=generation) as state:
            return state

    def test_repeated_explicit_start_preserves_fixed_resources_and_activation(self):
        first = self.start()
        self.assertEqual(first["phase"], "ready")
        self.assertEqual(first["project"], "nr-fixture-machine")
        self.assertEqual(first["network"], "nr-fixture-machine_iceberg_net")
        self.assertEqual(self.start()["generation"], first["generation"])
        self.assertEqual(self.backend.events, ["ensure"])

    def test_initial_default_volume_with_foreign_owner_is_never_mounted(self):
        name = machine.PROJECT + "_minio-data"
        self.backend.volumes[name] = dict(Name=name, CreatedAt="2026-10-20T00:00:00Z",
            Driver="local", Scope="local", Options=None, Labels={"novarocks.fixture.control-root": "/foreign"})
        before = copy.deepcopy(self.backend.volumes[name])
        self.assert_code("RuntimeIdentityMismatch", self.start)
        self.assertEqual(self.backend.events, [])
        self.assertEqual(self.backend.volumes[name], before)

    def test_volume_facts_are_saved_before_first_writer_and_survive_retry(self):
        def crash(record, previous=None):
            saved = self.owner.read_saved_state()
            self.assertEqual(saved["minio_volume_identity"], record["minio_volume_identity"])
            self.assertTrue(saved["steps"]["activation_started"])
            raise KeyboardInterrupt()
        with mock.patch.object(self.backend, "ensure_machine", side_effect=crash):
            with self.assertRaises(KeyboardInterrupt):
                self.start()
        identity = self.owner.read_saved_state()["minio_volume_identity"]
        final = self.start()
        self.assertEqual(final["minio_volume_identity"], identity)
        self.assertEqual(self.backend.events, ["ensure"])

    def test_default_volume_replacement_missing_or_foreign_fails_before_ready_recover_or_upgrade(self):
        for mutation in ("CreatedAt", "foreign-label", "missing"):
            for operation in ("attach", "start", "recover", "upgrade"):
                with self.subTest(mutation=mutation, operation=operation):
                    self.setUp()
                    state = self.start()
                    name = state["config"]["minio_volume"]
                    if mutation == "missing":
                        del self.backend.volumes[name]
                    elif mutation == "foreign-label":
                        self.backend.volumes[name]["Labels"]["novarocks.fixture.control-root"] = "/foreign"
                    else:
                        self.backend.volumes[name]["CreatedAt"] = "2026-10-21T02:00:00Z"
                    self.backend.events.clear()
                    if operation == "recover": self.backend.online = False
                    call = (lambda: self.attach(self.definition())) if operation == "attach" else (
                        lambda: self.owner.activate(self.changed(), upgrade=True)) if operation == "upgrade" else self.start
                    self.assert_code("RuntimeIdentityMismatch", call)
                    self.assertEqual(self.backend.events, [])
                    self.assertEqual(self.owner.read_saved_state(), state)

    def test_explicit_import_preserves_native_labels_and_rejects_same_name_replacement(self):
        name = "migrated-minio"
        initial = dict(Name=name, CreatedAt="2026-10-19T00:00:00Z", Driver="local", Scope="local",
            Options={"type": "none", "device": "/data/minio", "o": "bind"}, Labels={"original.owner": "legacy"})
        self.backend.volumes[name] = copy.deepcopy(initial)
        definition = self.definition(minio_volume=name)
        state = self.owner.activate(definition)
        self.assertEqual(state["minio_volume_identity"], initial)
        self.assertEqual(self.backend.volumes[name], initial)
        self.backend.online = False
        recovered = self.owner.activate(definition)
        self.assertEqual(recovered["minio_volume_identity"], initial)
        for field, changed in (("CreatedAt", "2026-10-21T00:00:00Z"), ("Driver", "other"),
                ("Scope", "global"), ("Options", {}), ("Labels", {"new.owner": "foreign"})):
            with self.subTest(field=field):
                self.backend.volumes[name] = {**copy.deepcopy(initial), field: changed}
                self.backend.events.clear()
                self.assert_code("RuntimeIdentityMismatch", lambda: self.owner.activate(definition))
                self.assertEqual(self.backend.events, [])
        self.backend.volumes[name] = copy.deepcopy(initial)
        self.assertEqual(self.owner.activate(definition), recovered)

    def test_default_recovery_preserves_frozen_facts(self):
        state = self.start()
        self.backend.online = False
        recovered = self.start()
        self.assertEqual(recovered["minio_volume_identity"], state["minio_volume_identity"])
        self.assertNotEqual(recovered["generation"], state["generation"])

    def test_unstarted_create_crash_can_freeze_volume_but_started_missing_facts_cannot(self):
        record = self.owner._record(self.definition())
        self.owner._save(record)
        self.backend.ensure_authority_volume(record)
        identity = self.backend.ensure_minio_volume(record)
        recovered = self.start()
        self.assertEqual(recovered["minio_volume_identity"], identity)
        recovered["phase"] = "maintenance"
        recovered["steps"]["activation_started"] = True
        recovered.pop("minio_volume_identity")
        self.owner._save(recovered)
        self.backend.events.clear()
        self.assert_code("RuntimeIdentityMismatch", self.start)
        self.assertEqual(self.backend.events, [])

    def test_absent_attach_has_no_service_effect(self):
        self.assert_code("MachineUnavailable", lambda: self.attach(self.definition()))
        self.assertEqual(self.backend.events, [])

    def test_attach_holds_maintenance_lock_until_publication_can_commit(self):
        self.start()
        with self.owner.ready_snapshot(self.definition()):
            other = machine.MachineOwner(self.owner.root, backend=self.backend,
                                         daemon=self.owner.daemon_id, timeout=0.001)
            with mock.patch.object(self.backend, "daemon_id") as daemon, \
                    mock.patch.object(self.backend, "settle_operations") as settle, \
                    mock.patch.object(self.backend, "healthy") as healthy:
                with self.assertRaises(machine.SweepLockTimeout) as caught:
                    other.sweep(deadline=time.monotonic())
            self.assertEqual(caught.exception.code, "MachineMaintaining")
            daemon.assert_not_called()
            settle.assert_not_called()
            healthy.assert_not_called()
        self.assertEqual(self.backend.events, ["ensure"])

    def test_sweep_does_not_reclassify_an_in_lock_timeout_as_unstarted(self):
        self.start()
        error = machine.MaintenanceLockTimeout()
        with mock.patch.object(self.backend, "healthy", side_effect=error):
            with self.assertRaises(machine.MaintenanceLockTimeout) as caught:
                self.owner.sweep(deadline=time.monotonic()+1)
        self.assertIs(caught.exception, error)
        self.assertNotIsInstance(caught.exception, machine.SweepLockTimeout)
        self.assertNotIn(("sweep", True), self.backend.events)

    def test_startup_sweep_pins_version_and_activation_before_cleanup(self):
        state = self.owner.activate(self.definition())
        before = list(self.backend.events)
        with self.assertRaisesRegex(runtime.RuntimeFailure, "MachineVersionMismatch"):
            self.owner.sweep(expected_version="another", expected_generation=state["generation"])
        with self.assertRaisesRegex(runtime.RuntimeFailure, "MachineUnavailable"):
            self.owner.sweep(expected_version=state["version"], expected_generation="old-activation")
        self.assertEqual(self.backend.events, before)
        report = self.owner.sweep(expected_version=state["version"], expected_generation=state["generation"])
        self.assertTrue(report["complete"])
        self.assertIn(("sweep", True), self.backend.events)

    def test_sweep_projects_only_confirmed_readonly_timeout_without_extending_deadline(self):
        state = self.start()
        original = time.monotonic() + 100
        self.backend.operation_deadline = original
        failure = runtime.RuntimeFailure("DockerOperationTimeout", "container")
        failure.readonly_deadline_settled = True
        def timed_out(record):
            self.assertLessEqual(self.backend.operation_deadline, deadline)
            raise failure
        deadline = time.monotonic() + 1
        with mock.patch.object(self.backend, "healthy", side_effect=timed_out):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "ScratchSweepTimeout") as caught:
                self.owner.sweep(deadline=deadline, expected_version=state["version"], expected_generation=state["generation"])
        self.assertIs(caught.exception.__cause__, failure)
        self.assertEqual(self.backend.operation_deadline, original)
        self.assertEqual(self.owner.read_saved_state(), state)
        self.assertNotIn(("sweep", True), self.backend.events)

    def test_sweep_does_not_defer_unconfirmed_timeout_or_other_failure(self):
        self.start()
        for code,evidence in (("DockerOperationTimeout",False),("DockerOperationFailed",True),
                              ("RunBudgetExpired",False),("ContainerCleanupUnconfirmed",False)):
            with self.subTest(code=code):
                failure = runtime.RuntimeFailure(code, "actual failure")
                failure.readonly_deadline_settled = evidence
                with mock.patch.object(self.backend, "healthy", side_effect=failure):
                    self.assert_code(code, lambda:self.owner.sweep(deadline=time.monotonic()+1))

    def test_sweep_defers_readonly_command_not_dispatched_after_lock_window(self):
        state = self.start()
        backend = machine.MachineDocker()
        backend.maintenance_fd = 42
        now = [100.0]
        original_deadline = 190.0
        self.backend.operation_deadline = original_deadline
        def health_after_window(record):
            # The lock succeeded before the window elapsed. The next readonly
            # command must not be mistaken for expiry of the enclosing run.
            now[0] = 105.0
            backend.operation_deadline = self.backend.operation_deadline
            return backend.command(["container", "inspect", "owned"])
        with mock.patch.object(machine.time, "monotonic", side_effect=lambda: now[0]), \
                mock.patch.object(self.backend, "healthy", side_effect=health_after_window), \
                mock.patch.object(machine.subprocess, "Popen") as spawn, \
                mock.patch.object(runtime, "atomic_json") as journal:
            with self.assertRaisesRegex(runtime.RuntimeFailure, "ScratchSweepTimeout") as caught:
                self.owner.sweep(deadline=105.0)
        self.assertIsInstance(caught.exception.__cause__, machine.MachineCommandDeadlineExpired)
        self.assertTrue(caught.exception.__cause__.readonly_not_dispatched)
        self.assertEqual(self.backend.operation_deadline, original_deadline)
        self.assertEqual(self.owner.read_saved_state(), state)
        self.assertNotIn(("sweep", True), self.backend.events)
        spawn.assert_not_called()
        journal.assert_not_called()

    def test_sweep_never_defers_before_dispatch_mutation_recovery_or_unknown(self):
        self.start()
        for condition in ("mutation", "recovery", "unknown"):
            with self.subTest(condition=condition):
                error = machine.MachineCommandDeadlineExpired(readonly=condition != "mutation")
                failing = "settle_operations" if condition == "recovery" else "healthy"
                pending = [("owned-journal", {"phase":"command-unknown"})] if condition == "unknown" else []
                with mock.patch.object(self.backend, failing, side_effect=error), \
                        mock.patch.object(self.backend, "unsettled_operations", return_value=pending):
                    with self.assertRaises(machine.MachineCommandDeadlineExpired) as caught:
                        self.owner.sweep(deadline=time.monotonic()+1)
                self.assertIs(caught.exception, error)

    def test_sweep_daemon_observation_runs_under_the_same_maintenance_lock(self):
        self.start()
        self.owner._daemon = "local-daemon"
        def daemon():
            self.assertIsNotNone(self.backend.maintenance_fd)
            return "local-daemon"
        with mock.patch.object(self.backend, "daemon_id", side_effect=daemon):
            self.assertTrue(self.owner.sweep(deadline=time.monotonic()+1)["complete"])

    def test_sweep_never_defers_operation_recovery_or_an_unsettled_operation(self):
        self.start()
        failure = runtime.RuntimeFailure("DockerOperationTimeout", "readonly recovery probe")
        failure.readonly_deadline_settled = True
        with mock.patch.object(self.backend, "settle_operations", side_effect=failure):
            self.assert_code("DockerOperationTimeout", lambda:self.owner.sweep(deadline=time.monotonic()+1))
        with mock.patch.object(self.backend, "healthy", side_effect=failure), \
                mock.patch.object(self.backend, "unsettled_operations", return_value=[("owned-journal", {"phase":"command-unknown"})]):
            self.assert_code("DockerOperationTimeout", lambda:self.owner.sweep(deadline=time.monotonic()+1))

    def test_wrong_version_neither_attaches_nor_rebuilds_services(self):
        first = self.start()
        before = copy.deepcopy(self.backend.events)
        self.assert_code("MachineVersionMismatch", lambda: self.owner.activate(self.changed()))
        self.assert_code("MachineVersionMismatch", lambda: self.attach(self.changed()))
        self.assertEqual(self.backend.events, before)
        self.assertEqual(self.owner.read_saved_state(), first)

    def test_same_lock_different_definition_or_actual_image_is_a_new_version(self):
        first = self.definition()["version"]
        for field in ("definition_sha256", "image_id"):
            supplied = bom()
            supplied["derived_images"]["rest-mv"][field] = "sha256:different"
            self.assertNotEqual(first, self.owner.required_definition({}, supplied)["version"])
        template = self.root / "model.yml"
        template.write_text(self.owner.template.read_text() + "\n# changed model\n")
        self.owner.template = template
        self.assertNotEqual(first, self.definition()["version"])

    def test_supply_timestamps_and_cache_paths_do_not_change_runtime_identity(self):
        supplied = bom()
        supplied["generated_at"] = "later"
        supplied["derived_images"]["rest-mv"].update(created_at="later", cache_path="/another/cache")
        self.assertEqual(self.definition()["version"], self.owner.required_definition({}, supplied)["version"])

    def test_upgrade_fences_before_online_cleanup_and_preserves_minio_volume(self):
        first = self.start()
        self.backend.events.clear()
        final = self.owner.activate(self.changed(), upgrade=True)
        self.assertEqual(self.backend.events, ["stop-writers", "rotate-root", "verify-fence",
            "remove-rest-state", "verify-catalog-absence", "verify-catalog-absence",
            "revoke-users", ("sweep", False), "ensure"])
        self.assertNotEqual(first["credentials"], final["credentials"])
        self.assertEqual(first["volumes"][0], final["volumes"][0])
        self.assertNotEqual(first["generation"], final["generation"])
        self.assertEqual(final["phase"], "ready")

    def test_partial_cleanup_stays_unpublishable_and_reuses_saved_rotation(self):
        self.start()
        self.backend.failure = "purge"
        self.assert_code("ScratchCleanupIncomplete", lambda: self.owner.activate(self.changed(), upgrade=True))
        failed = self.owner.read_saved_state()
        self.assertEqual(failed["phase"], "maintenance")
        self.assert_code("MachineUnavailable", lambda: self.attach(self.changed()))
        self.backend.failure = None
        final = self.owner.activate(self.changed(), upgrade=True)
        self.assertEqual(final["generation"], failed["generation"])
        self.assertEqual(final["credentials"], failed["credentials"])
        self.assertEqual(self.backend.events.count("rotate-root"), 1)

    def test_unknown_catalog_removal_never_revokes_or_purges_and_is_retried(self):
        self.start()
        self.backend.events.clear()
        self.backend.failure = "remove-rest-state"
        self.assert_code("DockerOperationTimeout", lambda: self.owner.activate(self.changed(), upgrade=True))
        failed = self.owner.read_saved_state()
        self.assertEqual(failed["phase"], "maintenance")
        self.assertFalse(failed["steps"].get("rest_removed", False))
        self.assertNotIn("revoke-users", self.backend.events)
        self.assertNotIn(("sweep", False), self.backend.events)
        self.backend.failure = None
        final = self.owner.activate(self.changed(), upgrade=True)
        self.assertEqual(final["generation"], failed["generation"])
        self.assertEqual(self.backend.events.count("remove-rest-state"), 2)
        self.assertEqual(self.backend.events.count("rotate-root"), 1)

    def test_catalog_absence_must_be_observed_before_persisting_barrier(self):
        self.start()
        self.backend.failure = "catalog-absence"
        self.assert_code("CatalogRemovalUnconfirmed", lambda: self.owner.activate(self.changed(), upgrade=True))
        failed = self.owner.read_saved_state()
        self.assertFalse(failed["steps"].get("rest_removed", False))
        self.assertNotIn("offline_catalog_barrier", failed["fenced_store"])
        self.assertNotIn(("sweep", False), self.backend.events)

    def test_resume_rechecks_native_absence_instead_of_trusting_saved_barrier(self):
        self.start()
        self.backend.failure = "purge"
        self.assert_code("ScratchCleanupIncomplete", lambda: self.owner.activate(self.changed(), upgrade=True))
        failed = self.owner.read_saved_state()
        self.assertTrue(failed["steps"]["rest_removed"])
        self.assertTrue(failed["fenced_store"]["offline_catalog_barrier"]["complete"])
        self.backend.events.clear()
        self.backend.failure = None
        self.backend.catalog_present = True
        self.assert_code("CatalogRemovalUnconfirmed", lambda: self.owner.activate(self.changed(), upgrade=True))
        self.assertEqual(self.backend.events, ["verify-catalog-absence"])
        self.assertEqual(self.owner.read_saved_state()["phase"], "maintenance")

    def test_cancelled_catalog_removal_keeps_files_and_maintenance(self):
        self.start()
        self.backend.events.clear()
        with mock.patch.object(self.backend, "remove_rest_state", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.owner.activate(self.changed(), upgrade=True)
        failed = self.owner.read_saved_state()
        self.assertEqual(failed["phase"], "maintenance")
        self.assertFalse(failed["steps"].get("rest_removed", False))
        self.assertNotIn("revoke-users", self.backend.events)
        self.assertNotIn(("sweep", False), self.backend.events)

    def test_offline_barrier_is_durable_before_constructing_purge_owner(self):
        first = self.start()
        def factory(record, backend):
            saved = self.owner.read_saved_state()
            self.assertTrue(saved["steps"]["rest_removed"])
            self.assertEqual(saved["fenced_store"]["offline_catalog_barrier"], dict(
                schema=1, daemon_id="local-daemon", generation=first["generation"],
                volumes=["nr-fixture-machine_rest-catalog", "nr-fixture-machine_rest-mv-catalog"],
                complete=True))
            return Sweeper(record, backend)
        self.owner.sweeper_factory = factory
        self.owner.activate(self.changed(), upgrade=True)

    def test_upgrade_health_failure_resumes_target_catalogs_without_repurging(self):
        self.start()
        self.backend.failure = "health"
        self.assert_code("ReadinessTimeout", lambda: self.owner.activate(self.changed(), upgrade=True))
        failed = self.owner.read_saved_state()
        self.assertTrue(failed["steps"]["purged"])
        self.assertTrue(failed["steps"]["activation_started"])
        self.assertTrue(self.backend.catalog_present)
        self.backend.events.clear()
        self.backend.failure = None
        final = self.owner.activate(self.changed(), upgrade=True)
        self.assertEqual(final["generation"], failed["generation"])
        self.assertEqual(final["phase"], "ready")
        self.assertEqual(self.backend.events, ["ensure"])

    def test_cancelled_target_activation_resumes_without_deleting_new_catalogs(self):
        self.start()
        original = self.backend.ensure_machine
        def interrupted(record, previous=None):
            original(record, previous)
            raise KeyboardInterrupt
        with mock.patch.object(self.backend, "ensure_machine", side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                self.owner.activate(self.changed(), upgrade=True)
        failed = self.owner.read_saved_state()
        self.assertTrue(failed["steps"]["purged"])
        self.assertTrue(self.backend.catalog_present)
        self.backend.events.clear()
        final = self.owner.activate(self.changed(), upgrade=True)
        self.assertEqual(final["phase"], "ready")
        self.assertEqual(final["generation"], failed["generation"])
        self.assertEqual(self.backend.events, ["ensure"])

    def test_unconfirmed_fence_never_purges_and_retries_without_another_rotation(self):
        self.start()
        self.backend.failure = "fence"
        self.assert_code("CredentialFenceUnconfirmed", lambda: self.owner.activate(self.changed(), upgrade=True))
        self.assertNotIn(("sweep", False), self.backend.events)
        self.backend.failure = None
        self.owner.activate(self.changed(), upgrade=True)
        self.assertEqual(self.backend.events.count("rotate-root"), 1)

    def test_port_and_health_failures_do_not_commit_ready(self):
        for failure, code in (("ensure", "PortUnavailable"), ("health", "ReadinessTimeout")):
            with self.subTest(failure=failure):
                self.backend.failure = failure
                self.assert_code(code, self.start)
                self.assertEqual(self.owner.read_saved_state()["phase"], "maintenance")

    def test_second_control_directory_cannot_create_a_second_machine_stack(self):
        self.start()
        other = machine.MachineOwner(self.root / "other", backend=self.backend)
        self.assert_code("RuntimeIdentityMismatch", lambda: other.activate(other.required_definition({}, bom())))

    def test_daemon_and_activation_identity_must_match(self):
        state = self.start()
        other = machine.MachineOwner(self.owner.root, daemon="other-daemon", backend=self.backend)
        self.assert_code("RuntimeIdentityMismatch", lambda: other.activate(other.required_definition({}, bom())))
        self.assert_code("MachineUnavailable", lambda: self.attach(self.definition(), "stale"))
        self.owner.attach_consumer("exact-container", "hms", state["generation"])
        self.assertEqual(self.backend.events[-1], ("connect", "exact-container", "hms"))

    def test_credentials_remain_private_and_status_is_redacted(self):
        state = self.start()
        public = json.dumps(machine.public_snapshot(state))
        for secret in state["credentials"].values():
            self.assertNotIn(secret, public)
        for path in (self.owner.base / "state.json", Path(state["compose_env"])):
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_committed_state_is_configuration_authority_after_upgrade(self):
        self.start()
        runtime.atomic_json(self.owner.base / "config.json", {"ports": {"rest": 31009}})
        self.assertEqual(self.owner.load_config()["ports"]["rest"], 28002)
        self.owner.activate(self.changed(), upgrade=True)
        self.assertEqual(self.owner.load_config()["ports"]["rest"], 31002)

    def test_volatile_scratch_observations_do_not_change_attach_snapshot(self):
        state = self.start()
        self.assertEqual(self.owner.scratch_observation(state)["status"], "UNKNOWN")
        report = dict(sampled_at_unix_ms=1800000000000, observed_counts={"s3-prefix": 3},
            oldest_created_unix_ms=1799990000000, counts_are_pre_cleanup=True,
            enumeration_complete=False, enumeration_scope=["s3-prefix"], unknown_counts={"user": 1})
        self.owner._save_scratch_observation(state, report)
        self.assertEqual(self.attach(self.definition()), state)
        observed = self.owner.scratch_observation(state)
        self.assertTrue(observed["sample_is_current_activation"])
        self.assertFalse(observed["enumeration_complete"])
        updated = self.owner.activate(self.changed(), upgrade=True)
        self.assertFalse(self.owner.scratch_observation(updated)["sample_is_current_activation"])

    def test_incomplete_input_or_unsafe_lifetime_policy_is_rejected(self):
        supplied = bom()
        del supplied["derived_images"]["rest-mv"]["definition_sha256"]
        self.assert_code("FixturePrerequisiteMissing", lambda: self.owner.required_definition({}, supplied))
        self.assert_code("InvalidConfiguration", lambda: self.definition(policy={"scratch_ttl_seconds": 100}))
        self.assert_code("InvalidConfiguration", lambda: self.definition(ports={"rest": 28000}))
        for timeout in (float("nan"), float("inf"), 0, -1, True):
            self.assert_code("InvalidConfiguration", lambda: machine.MachineOwner(self.root, backend=self.backend, timeout=timeout))
        for root in ("s3://novarocks/a/../b", "s3://novarocks", "s3://novarocks/a?query", "s3://novarocks/a b"):
            self.assert_code("InvalidConfiguration", lambda: self.definition(benchmark_root=root))


class DockerActivationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        fake = Backend()
        self.owner = machine.MachineOwner(Path(self.temporary.name), backend=fake, sweeper_factory=Sweeper)
        self.source = self.owner.activate(self.owner.required_definition({}, bom()))
        self.target = self.owner._record(self.owner.required_definition({"ports": {"rest": 31002}}, bom()))
        self.target["minio_volume_identity"] = copy.deepcopy(self.source["minio_volume_identity"])
        self.infos = {}
        for service in self.source["images"]:
            self.install(self.source, service)
        outer = self
        class Docker(machine.MachineDocker):
            def command(self, args, **kwargs):
                service = next((x.split("=", 2)[-1] for x in args if x.startswith("label=com.docker.compose.service=")), None)
                return outer.infos[service]["Id"] if service in outer.infos else ""
            def inspect(self, kind, identity, **kwargs):
                if kind == "network":
                    return {"Id": outer.source["network_id"], "Labels": self.labels(outer.source)}
                if kind == "volume":
                    return copy.deepcopy(fake.volumes.get(identity))
                return next((info for info in outer.infos.values() if info["Id"] == identity), None)
        self.docker = Docker()

    def install(self, record, service, *, identity=None):
        ports = record["service_ports"].get(service, {})
        self.infos[service] = {"Id": identity or record.get("containers", {}).get(service, "new-" + service),
            "Image": record["images"][service]["image_id"],
            "Config": {"Labels": {**machine.MachineDocker.labels(record),
                "novarocks.fixture.version": record["version"], "novarocks.fixture.activation": record["generation"]}},
            "HostConfig": {"PortBindings": {key: [{"HostPort": str(value)}] for key, value in ports.items()}}}

    def test_existing_minio_volume_with_active_consumer_is_never_adopted(self):
        selected = self.target["config"]["minio_volume"]
        def inspect(kind, identity, **kwargs):
            return {"Labels": self.docker.labels(self.target)} if kind == "volume" and identity == selected else None
        def command(args, **kwargs):
            return "active-original-minio" if "volume=" + selected in args else ""
        with mock.patch.object(self.docker, "inspect", side_effect=inspect), mock.patch.object(self.docker, "command", side_effect=command):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "active consumer"):
                self.docker.validate_absent(self.target)

    def test_daemon_authority_from_other_control_root_prevents_compose_activation(self):
        marker = {"Labels": {**self.docker.labels(self.target), "novarocks.fixture.control-root": "/another-owner"}}
        calls = []
        with mock.patch.object(self.docker, "inspect", return_value=marker), mock.patch.object(self.docker, "command", side_effect=lambda args: calls.append(args)):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "another control directory"):
                self.docker.ensure_machine(self.target)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], ["volume", "create"])

    def test_partial_activation_accepts_exact_source_and_target_mix(self):
        self.install(self.target, "rest")
        self.docker.activation_preflight(self.target, self.source)

    def test_same_image_foreign_activation_is_rejected_before_compose(self):
        self.install(self.target, "rest")
        self.infos["rest"]["Config"]["Labels"]["novarocks.fixture.activation"] = "foreign"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "RuntimeIdentityMismatch"):
            self.docker.activation_preflight(self.target, self.source)

    def test_external_container_replacement_does_not_pass_saved_identity(self):
        self.install(self.source, "rest", identity="unapproved-replacement")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "replaced outside maintenance"):
            self.docker.container(self.source, "rest")

    def test_catalog_barrier_requires_both_exact_containers_and_volumes_absent(self):
        for service in ("rest", "rest-mv"):
            with self.subTest(service=service, kind="container"):
                self.infos = {}
                self.install(self.source, service)
                with self.assertRaisesRegex(runtime.RuntimeFailure, "CatalogRemovalUnconfirmed"):
                    self.docker.rest_state_barrier(self.source)
            with self.subTest(service=service, kind="volume"):
                self.infos = {}
                original = self.docker.inspect
                name = machine.PROJECT + "_" + service + "-catalog"
                def inspect(kind, identity, **kwargs):
                    return {"Name": name} if kind == "volume" and identity == name else original(kind, identity, **kwargs)
                with mock.patch.object(self.docker, "inspect", side_effect=inspect):
                    with self.assertRaisesRegex(runtime.RuntimeFailure, "CatalogRemovalUnconfirmed"):
                        self.docker.rest_state_barrier(self.source)
        self.infos = {}
        barrier = self.docker.rest_state_barrier(self.source)
        self.assertTrue(barrier["complete"])
        self.assertEqual(barrier["generation"], self.source["generation"])

    def test_catalog_removal_rejects_replacement_before_deleting_any_container(self):
        self.install(self.source, "rest-mv", identity="external-replacement")
        with mock.patch.object(self.docker, "command", wraps=self.docker.command) as command:
            with self.assertRaisesRegex(runtime.RuntimeFailure, "replaced outside maintenance"):
                self.docker.remove_rest_state(self.source)
        self.assertFalse(any(call.args[0][0] in {"rm", "compose"} for call in command.call_args_list))

    def test_catalog_removal_targets_verified_ids_and_confirms_native_absence(self):
        identifiers = [self.source["containers"][s] for s in ("rest", "rest-mv")]
        original = self.docker.command
        mutations = []
        def command(args, **kwargs):
            if args[0] == "rm":
                mutations.append(args)
                for service in ("rest", "rest-mv"):
                    self.infos.pop(service, None)
                return ""
            return original(args, **kwargs)
        with mock.patch.object(self.docker, "command", side_effect=command):
            barrier = self.docker.remove_rest_state(self.source)
        self.assertEqual(mutations, [["rm", *identifiers]])
        self.assertTrue(barrier["complete"])


if __name__ == "__main__":
    unittest.main()
