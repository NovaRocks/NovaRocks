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

"""Protocol tests with explicit failure and concurrency barriers, without Docker."""
import copy
import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest

MODULE = Path(__file__).resolve().parents[1] / "fixture_runtime.py"
sys.path.insert(0, str(MODULE.parent))
import fixture_runtime as runtime


def renderer(context, staging):
    for name in runtime.ENTRY_FILES:
        (staging / name).write_text(json.dumps({"ready": context["ready"],
            "endpoints": context["endpoints"], "publication": context["publication_dir"]}))


class Backend:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.calls = []
        self.external = []
        self.fail = None

    def daemon_id(self):
        return "test-daemon"

    def image_id(self, reference):
        return reference if reference.startswith("sha256:") else "sha256:" + reference

    def healthy(self, record):
        return (self.root / record["id"]).exists()

    def ensure(self, record, parent=None):
        self.calls.append(("ensure", record["id"], dict(record["ports"])))
        if self.fail == "ensure":
            raise runtime.RuntimeFailure("PortUnavailable", record["id"])
        (self.root / record["id"]).touch()
        with (self.root / (record["id"] + ".ensure")).open("a") as log:
            log.write("created\n")

    def reconnect(self, parent, catalogs):
        self.calls.append(("reconnect", [item["id"] for item in catalogs]))

    def external_connections(self, record, parent):
        return self.external

    def purge(self, record, prefixes):
        self.calls.append(("purge", record["id"], prefixes))
        if self.fail == "purge":
            raise runtime.RuntimeFailure("DockerOperationFailed", "injected purge failure")

    def stop(self, record):
        self.calls.append(("stop", record["id"]))
        (self.root / record["id"]).unlink(missing_ok=True)

    def container(self, record, service):
        return {"Id": record["id"] + "-" + service}

    def disconnect(self, record, container):
        self.calls.append(("disconnect", record["id"]))
        self.external = []

    def connect(self, record, container, alias):
        self.external = [container]

    def delete_resources(self, record):
        self.calls.append(("delete", record["id"]))
        if self.fail == "delete":
            raise runtime.RuntimeFailure("DockerOperationFailed", "injected uncertain deletion")
        (self.root / record["id"]).unlink(missing_ok=True)

    def untag(self, record):
        self.calls.append(("untag", record["id"]))


def bom(spark="one", rest="rest"):
    return {"lock_sha256": "lock", "images": {name: {"alias": alias} for name, alias in
            (("minio", "minio"), ("minio-mc", "mc"), ("iceberg-rest", rest))},
            "derived_images": {"iceberg-spark": {"image_id": "sha256:" + spark,
              "definition_sha256": "definition-" + spark}}}


CONFIG = {"credentials": {"access_key": "access", "secret_key": "secret"},
          "buckets": ["warehouse", "novarocks"]}


class DockerServiceNetworkTests(unittest.TestCase):
    def fixture(self, service="minio", attached=False, aliases=None):
        record = {"id": "os-test", "kind": "os", "namespace": "test-owner", "key": "test-key",
                  "project": "test-project", "network": "saved-own-network", "volumes": ["saved-volume"],
                  "compose_file": "/saved/compose.yml", "compose_env": "/saved/compose.env",
                  "images": {service: {"image_id": "sha256:service", "tag": "saved-service-tag"}},
                  "service_ports": {service: {"9000/tcp": 38000} if service == "minio" else {}},
                  "ports": {"minio": 38000}, "required_services": [service], "health_urls": ["http://host-proxy"]}
        if service == "minio":
            record["images"]["mc-init"] = {"image_id": "sha256:mc", "tag": "saved-mc-tag"}
            record["required_services"].append("mc-init")
        labels = runtime.Docker.labels(record)
        network = {"Id": "exact-own-network-id", "Labels": dict(labels), "Containers": {}}
        containers = {}
        for name, image in record["images"].items():
            container = {"Id": "exact-" + name, "Image": image["image_id"], "Config": {"Labels": dict(labels)},
                         "HostConfig": {"PortBindings": {port: [{"HostPort": str(host)}]
                                        for port, host in record["service_ports"].get(name, {}).items()}},
                         "State": {"Running": True, "Status": "running"},
                         "NetworkSettings": {"Networks": {}}}
            if name == service:
                # These surviving attachments must never be disconnected during repair.
                container["NetworkSettings"]["Networks"] = {
                    "catalog-a": {"NetworkID": "other-a", "Aliases": ["minio"]},
                    "catalog-b": {"NetworkID": "other-b", "Aliases": ["minio"]}}
            if name != service or attached:
                container["NetworkSettings"]["Networks"][record["network"]] = {
                    "NetworkID": network["Id"], "Aliases": aliases if name == service else [name]}
                network["Containers"][container["Id"]] = {}
            containers[name] = container
        backend = runtime.Docker(timeout=1)
        calls = []
        def inspect(kind, identity, **kwargs):
            if kind == "container":
                return copy.deepcopy(next(value for value in containers.values() if value["Id"] == identity))
            if kind == "network":
                self.assertEqual(identity, record["network"])
                return copy.deepcopy(network)
            if kind == "volume":
                return {"Labels": dict(labels)}
            if kind == "image":
                return {"Id": next(image["image_id"] for image in record["images"].values() if image["tag"] == identity)}
            self.fail("unexpected inspect: " + kind)
        def command(args, **kwargs):
            calls.append(args)
            if args[0] == "ps":
                selected = next(arg.split("=", 1)[1] for arg in args if arg.startswith("label=com.docker.compose.service="))
                name = selected.rsplit("=", 1)[1]
                return containers[name]["Id"]
            if args[0] == "compose":
                self.assertEqual(args[-4:], ["up", "-d", "minio", "mc-init"])
                return ""
            if args[:2] == ["network", "disconnect"]:
                self.assertEqual(args[2], record["network"])
                container = next(value for value in containers.values() if value["Id"] == args[3])
                container["NetworkSettings"]["Networks"].pop(record["network"], None)
                network["Containers"].pop(container["Id"], None)
                return ""
            if args[:2] == ["network", "connect"]:
                self.assertEqual(args[-2], record["network"])
                container = next(value for value in containers.values() if value["Id"] == args[-1])
                container["NetworkSettings"]["Networks"][record["network"]] = {
                    "NetworkID": network["Id"], "Aliases": args[3:-2:2]}
                network["Containers"][container["Id"]] = {}
                if "mc-init" in containers:
                    containers["mc-init"]["State"] = {"Running": False, "Status": "exited", "ExitCode": 0}
                    # Completed initialization does not need a retained live endpoint.
                    containers["mc-init"]["NetworkSettings"]["Networks"] = {}
                    network["Containers"].pop(containers["mc-init"]["Id"], None)
                return ""
            self.fail("unexpected command: " + repr(args))
        backend.inspect = inspect
        backend.command = command
        backend.http_ready = lambda url: True
        return backend, record, containers, network, calls

    def test_failed_start_lost_endpoint_repairs_existing_minio_before_health(self):
        backend, record, containers, network, calls = self.fixture()
        original = copy.deepcopy(containers["minio"])
        ports = copy.deepcopy(record["ports"])
        containers["mc-init"]["State"] = {"Running": False, "Status": "exited", "ExitCode": 0}
        # Even an accepting host HTTP endpoint and successful prior bucket init
        # cannot mask the running service's missing own network.
        self.assertFalse(backend.healthy(record))
        containers["mc-init"]["State"] = {"Running": True, "Status": "running"}
        backend.ensure(record)
        self.assertTrue(backend.healthy(record))
        self.assertEqual(record["ports"], ports)
        self.assertEqual(record["containers"]["minio"], original["Id"])
        for name in ("catalog-a", "catalog-b"):
            self.assertEqual(containers["minio"]["NetworkSettings"]["Networks"][name],
                             original["NetworkSettings"]["Networks"][name])
        changes = [args for args in calls if args[0] == "network"]
        self.assertEqual(changes, [["network", "connect", "--alias", "minio", "--alias", "warehouse.minio",
                                   "--alias", "novarocks.minio", record["network"], original["Id"]]])

    def test_service_aliases_are_repaired_without_recreating_the_container(self):
        backend, record, containers, network, calls = self.fixture(service="spark", attached=True, aliases=["wrong"])
        self.assertFalse(backend.healthy(record))
        backend.repair_service_networks(record)
        self.assertTrue(backend.healthy(record))
        self.assertEqual([args for args in calls if args[0] == "network"], [
            ["network", "disconnect", record["network"], "exact-spark"],
            ["network", "connect", "--alias", "spark", record["network"], "exact-spark"]])

    def test_foreign_network_is_not_repaired(self):
        backend, record, containers, network, calls = self.fixture()
        network["Labels"]["novarocks.fixture.owner"] = "foreign-owner"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "RuntimeIdentityMismatch"):
            backend.repair_service_networks(record)
        self.assertFalse(any(args[0] == "network" for args in calls))


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.backend = Backend(self.root / "backend")
        self.owner = self.make_owner()
        self.entry = self.root / "worktree/runtime/test"

    def make_owner(self, suffix="owner", hook=None):
        return runtime.RuntimeOwner(self.root / suffix, daemon="test-daemon", backend=self.backend,
            renderer=renderer, hook=hook or (lambda *a, **k: None), port_start=38000, port_end=38999)

    def bind(self, images=None, entry=None, name="test"):
        return self.owner.bind(name, entry or self.entry, CONFIG, images or bom())

    def test_prepared_hms_port_is_reserved_before_it_listens(self):
        runtime.atomic_json(self.owner.base / "hms" / "cat-prepared" / "manifest.json",
                            {"ports": {"hms": self.owner.port_start}})
        publication = self.bind()
        ports = [port for record in publication["records"].values()
                 for port in record["ports"].values()]
        self.assertNotIn(self.owner.port_start, ports)

    def assert_code(self, code, callable):
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            callable()
        self.assertEqual(caught.exception.code, code)

    def test_version_keys_share_object_store_and_preserve_data(self):
        first = self.bind()
        second = self.bind(bom("two"))
        self.assertEqual(first["binding"]["object_store"], second["binding"]["object_store"])
        self.assertNotEqual(first["binding"]["catalog"], second["binding"]["catalog"])
        self.assertEqual(len(self.owner.records()), 3)
        self.assertEqual(len(second["data_locations"]), 1)
        self.assertNotEqual(first["published_dir"], second["published_dir"])
        self.assertTrue(Path(first["published_dir"]).is_dir())
        self.assertEqual(second["producer_receipt"]["image_id"], "sha256:two")

    def test_rest_changes_catalog_only_and_config_changes_object_store(self):
        first = self.bind()
        other = self.bind(bom(rest="rest2"))
        self.assertEqual(first["binding"]["object_store"], other["binding"]["object_store"])
        config = copy.deepcopy(CONFIG)
        config["credentials"]["secret_key"] = "changed"
        newest = self.owner.bind("test", self.entry, config, bom())
        self.assertNotEqual(first["binding"]["object_store"], newest["binding"]["object_store"])
        self.assertEqual(len(newest["data_locations"]), 2)

    def test_repeated_bind_never_reallocates_or_recreates_healthy_instances(self):
        first = self.bind()
        calls = len([call for call in self.backend.calls if call[0] == "ensure"])
        second = self.bind()
        self.assertEqual(first["binding"], second["binding"])
        self.assertEqual(calls, len([call for call in self.backend.calls if call[0] == "ensure"]))
        self.assertEqual(first["records"], second["records"])

    def test_starting_retry_retains_ports(self):
        self.backend.fail = "ensure"
        self.assert_code("PortUnavailable", self.bind)
        reserved = self.owner.records()[0]
        self.assertEqual(reserved["state"], "starting")
        self.backend.fail = None
        recovered = self.bind()
        self.assertEqual(recovered["records"]["object_store"]["ports"], reserved["ports"])

    def test_renderer_failure_preserves_complete_old_publication(self):
        first = self.bind()
        def failed(context, staging):
            (staging / "env.sh").write_text("partial")
            raise ValueError("injected")
        self.owner.renderer = failed
        with self.assertRaises(ValueError):
            self.bind(bom("two"))
        self.assertEqual(runtime.current_publication(self.entry)["published_dir"], first["published_dir"])
        self.assertEqual(json.loads((self.entry / "env.sh").read_text())["publication"], first["published_dir"])

    def test_crash_before_and_after_publication_commit(self):
        first = self.bind()
        def failed(name, **facts):
            if name == "publication.before_swap":
                raise ValueError("before commit")
        self.owner.hook = failed
        with self.assertRaises(ValueError):
            self.bind(bom("two"))
        self.assertEqual(runtime.current_publication(self.entry)["binding"], first["binding"])
        def after(name, **facts):
            if name == "publication.after_swap":
                raise ValueError("unknown result")
        self.owner.hook = after
        with self.assertRaises(ValueError):
            self.bind(bom("two"))
        self.assertNotEqual(runtime.current_publication(self.entry)["binding"], first["binding"])
        self.owner.hook = lambda *a, **k: None
        self.bind(bom("two"))

    def test_unbind_preserves_data_and_fixed_lock(self):
        first = self.bind()
        inode = (self.entry / ".owner.lock").stat().st_ino
        unbound = self.owner.unbind("test", self.entry)
        self.assertIsNone(unbound["binding"])
        self.assertEqual(first["data_locations"], unbound["data_locations"])
        self.assertEqual(inode, (self.entry / ".owner.lock").stat().st_ino)
        self.owner.manage(first["binding"]["catalog"], "delete")
        self.assert_code("BindingsPresent", lambda: self.owner.manage(first["binding"]["object_store"], "delete", force=True))
        self.owner.unbind("test", self.entry, purge=True)
        self.owner.manage(first["binding"]["object_store"], "delete")
        self.assertEqual(self.owner.records(), [])

    def test_purge_failure_preserves_publication_and_all_references(self):
        first = self.bind()
        self.backend.fail = "purge"
        self.assert_code("DockerOperationFailed", lambda: self.owner.unbind("test", self.entry, purge=True))
        self.assertEqual(runtime.current_publication(self.entry), first)

    def test_cross_owner_switch_requires_unbind_and_keeps_locations(self):
        first = self.bind()
        other = self.make_owner("other")
        self.assert_code("OwnerMismatch", lambda: other.bind("test", self.entry, CONFIG, bom()))
        self.owner.unbind("test", self.entry)
        second = other.bind("test", self.entry, CONFIG, bom())
        self.assertEqual(len(second["data_locations"]), 2)
        self.assertIn(first["data_locations"][0], second["data_locations"])
        second = other.unbind("test", self.entry, purge=True)
        self.assertEqual(second["data_locations"], [])

    def test_prepare_is_offline_and_does_not_claim_live_health(self):
        first = self.bind()
        self.backend.daemon_id = lambda: self.fail("Docker queried by prepare")
        self.backend.healthy = lambda r: self.fail("health queried by prepare")
        second = self.owner.prepare_entry("test", self.entry, CONFIG)
        self.assertTrue(second["ready"])
        self.assertEqual(first["binding"], second["binding"])
        cat = self.owner.record(first["binding"]["catalog"])
        cat["state"] = "starting"
        self.owner.save_record(cat)
        third = self.owner.prepare_entry("test", self.entry, CONFIG)
        self.assertFalse(third["ready"])
        self.assertEqual(third["data_locations"], first["data_locations"])

    def test_first_prepare_does_not_resolve_daemon(self):
        self.owner._daemon = None
        self.backend.daemon_id = lambda: self.fail("Docker queried")
        prepared = self.owner.prepare_entry("test", self.entry, CONFIG)
        self.assertFalse(prepared["ready"])
        self.assertIsNone(prepared["owner_locator"])

    def test_current_is_only_a_locator_published_inside_owner(self):
        config = {**CONFIG, "update_current": True,
                  "current_link": str((self.entry.parent / "current").resolve())}
        publication = self.owner.prepare_entry("test", self.entry, config)
        current = self.entry.parent / "current"
        self.assertEqual(current.resolve(), self.entry.resolve())
        self.assertEqual(runtime.current_publication(current)["published_dir"], publication["published_dir"])
        isolated = {**CONFIG, "update_current": False, "current_link": str(current)}
        self.owner.prepare_entry("isolated", self.entry.parent / "isolated", isolated)
        self.assertEqual(current.resolve(), self.entry.resolve())

    def test_bad_index_fails_closed(self):
        first = self.bind()
        (self.entry / "published").unlink()
        self.assert_code("RuntimeStateUnreadable", lambda: self.owner.manage(first["binding"]["object_store"], "delete", force=True))

    def test_missing_pointer_never_reinitializes_old_data_references(self):
        first = self.bind()
        (self.entry / "published").unlink()
        self.assert_code("RuntimeStateUnreadable", self.bind)
        self.assert_code("RuntimeStateUnreadable", lambda: self.owner.prepare_entry("test", self.entry, CONFIG))
        self.assertTrue(Path(first["published_dir"]).exists())

    def test_force_delete_refuses_external_consumer_even_before_mark(self):
        first = self.bind()
        cat_id = first["binding"]["catalog"]
        self.owner.consumer(cat_id, "external")
        self.assert_code("ExternalAttachmentsPresent", lambda: self.owner.manage(cat_id, "delete", force=True))
        self.assertEqual(self.owner.record(cat_id)["state"], "ready")
        self.owner.consumer(cat_id, "external", disconnect=True)

    def test_deletion_retry_uses_persisted_token_without_force(self):
        first = self.bind()
        cat_id = first["binding"]["catalog"]
        self.backend.fail = "delete"
        self.assert_code("DockerOperationFailed", lambda: self.owner.manage(cat_id, "delete", force=True))
        deleting = self.owner.record(cat_id)
        self.assertEqual(deleting["state"], "deleting")
        self.assertIsNone(runtime.current_publication(self.entry)["binding"])
        self.assert_code("RuntimeDeleting", self.bind)
        self.backend.fail = None
        seen = []
        self.owner.hook = lambda name, **facts: seen.append(facts) if name == "delete.marked" else None
        self.owner.manage(cat_id, "delete")
        self.assertEqual(seen[0]["deletion_id"], deleting["deletion_id"])
        self.assertIsNone(self.owner.record(cat_id))

    def test_old_delete_cannot_unbind_rebuilt_record(self):
        first = self.bind()
        cat_id = first["binding"]["catalog"]
        def replace(name, **facts):
            if name == "delete.marked":
                record = self.owner.record(cat_id)
                record["state"] = "ready"
                record.pop("deletion_id")
                self.owner.save_record(record)
        self.owner.hook = replace
        self.owner.manage(cat_id, "delete", force=True)
        self.assertEqual(runtime.current_publication(self.entry)["binding"], first["binding"])
        self.assertEqual(self.owner.record(cat_id)["state"], "ready")

    def test_delete_does_not_unbind_worktree_that_switched_catalog(self):
        first = self.bind()
        cat_id = first["binding"]["catalog"]
        def switch(name, **facts):
            if name == "delete.before_worktree_lock":
                self.owner.hook = lambda *a, **k: None
                self.bind(bom("two"))
        self.owner.hook = switch
        self.owner.manage(cat_id, "delete", force=True)
        latest = runtime.current_publication(self.entry)
        self.assertNotEqual(latest["binding"]["catalog"], cat_id)
        self.assertEqual(latest["data_locations"], first["data_locations"])

    def test_force_stop_preserves_record_and_reserved_ports(self):
        first = self.bind()
        os_id = first["binding"]["object_store"]
        self.assert_code("BindingsPresent", lambda: self.owner.manage(os_id, "stop"))
        self.owner.manage(os_id, "stop", force=True)
        recovered = self.bind()
        self.assertEqual(first["records"]["object_store"]["ports"], recovered["records"]["object_store"]["ports"])
        self.assertIn(("reconnect", [first["binding"]["catalog"]]), self.backend.calls)

    def test_object_store_recovery_cannot_skip_reconnect_after_crash(self):
        first = self.bind()
        second = self.bind(bom("two"), self.root / "other-entry", "other")
        os_id = first["binding"]["object_store"]
        self.owner.manage(os_id, "stop", force=True)
        def interrupt(name, **facts):
            if name == "object_store.before_reconnect":
                raise ValueError("injected recovery crash")
        self.owner.hook = interrupt
        with self.assertRaises(ValueError):
            self.bind()
        self.assertEqual(self.owner.record(os_id)["state"], "starting")
        self.owner.hook = lambda *a, **k: None
        self.bind()
        expected = {first["binding"]["catalog"], second["binding"]["catalog"]}
        actual = [set(call[1]) for call in self.backend.calls if call[0] == "reconnect"]
        self.assertEqual(actual[-1], expected)

    def test_docker_environment_discards_inherited_compose_overrides(self):
        env = runtime.controlled_environment({"PATH": "path", "COMPOSE_PROJECT_NAME": "foreign",
            "REST_IMAGE": "foreign", "DOCKER_CONTEXT": "desktop-linux"})
        self.assertEqual(env, {"PATH": "path", "DOCKER_CONTEXT": "desktop-linux"})

    def test_live_container_identity_mismatch_is_not_replaced(self):
        publication = self.bind()
        record = publication["records"]["catalog"]
        backend = runtime.Docker()
        backend.command = lambda *a, **k: "container-id"
        backend.inspect = lambda *a, **k: {"Image": "sha256:foreign", "Config": {"Labels": {}}}
        self.assert_code("RuntimeIdentityMismatch", lambda: backend.ensure(record))
        self.assertEqual(runtime.current_publication(self.entry), publication)

    def test_foreign_volume_or_network_is_rejected_before_compose(self):
        publication = self.bind()
        record = publication["records"]["object_store"]
        backend = runtime.Docker()
        backend.container = lambda *a: None
        backend.inspect = lambda *a, **k: {"Labels": {"foreign": "true"}}
        backend.compose = lambda *a: self.fail("foreign resources touched by Compose")
        self.assert_code("RuntimeIdentityMismatch", lambda: backend.ensure(record))
        self.assert_code("RuntimeIdentityMismatch", lambda: backend.delete_resources(record))

    def fake_docker(self):
        executable = self.root / "bin/docker"
        executable.parent.mkdir()
        pidfile = self.root / "docker-child.pid"
        executable.write_text("#!/bin/sh\n"
            f"printf '%s' \"$$\" > {runtime.shlex.quote(str(pidfile))}\nexec sleep 3600\n")
        executable.chmod(0o700)
        environment = {"PATH": str(executable.parent) + ":" + os.environ["PATH"]}
        return pidfile, environment

    def test_command_timeout_settles_child(self):
        pidfile, environment = self.fake_docker()
        backend = runtime.Docker(timeout=2, environment=environment)
        self.assert_code("DockerOperationTimeout", lambda: backend.command(["compose", "up"]))
        pid = int(pidfile.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_missing_docker_context_is_not_resource_absence(self):
        _, environment = self.fake_docker()
        executable = self.root / "bin/docker"
        executable.write_text("#!/bin/sh\nprintf '%s\\n' 'context foreign not found' >&2\nexit 1\n")
        backend = runtime.Docker(timeout=5, environment=environment)
        self.assert_code("DockerOperationFailed", lambda: backend.command(["network", "inspect", "network"], absent_ok=True))

    def test_sigterm_settles_child_before_releasing_owner_lock(self):
        pidfile, environment = self.fake_docker()
        context = mp.get_context("fork")
        result = context.Queue()
        lock = self.root / "cancel.lock"
        def child():
            runtime.install_signal_handlers()
            try:
                with runtime.file_lock(lock):
                    runtime.Docker(timeout=30, environment=environment).command(["compose", "up"])
            except KeyboardInterrupt:
                result.put("cancelled")
        process = context.Process(target=child)
        process.start()
        deadline = time.monotonic() + 5
        while not pidfile.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        try:
            self.assertTrue(pidfile.exists(), "fake Docker never reached startup barrier")
            pid = int(pidfile.read_text())
            os.kill(process.pid, signal.SIGTERM)
            process.join(5)
            self.assertEqual(process.exitcode, 0)
            self.assertEqual(result.get(timeout=2), "cancelled")
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)
            with lock.open("a+b") as handle:
                runtime.fcntl.flock(handle, runtime.fcntl.LOCK_EX | runtime.fcntl.LOCK_NB)
        finally:
            if process.is_alive():
                process.kill()
                process.join()

    def test_delete_resumes_each_durable_phase(self):
        for phase in ("delete.marked", "delete.after_conditional_unbind", "delete.stopped",
                      "delete.purged", "delete.disconnected", "delete.resources_removed"):
            with self.subTest(phase=phase):
                first = self.bind()
                cat_id = first["binding"]["catalog"]
                def interrupt(name, **facts):
                    if name == phase:
                        raise ValueError("injected crash boundary")
                self.owner.hook = interrupt
                with self.assertRaises(ValueError):
                    self.owner.manage(cat_id, "delete", force=True)
                token = self.owner.record(cat_id)["deletion_id"]
                self.owner.hook = lambda *a, **k: None
                self.owner.manage(cat_id, "delete")
                self.assertIsNone(self.owner.record(cat_id))
                self.assertIsNone(runtime.current_publication(self.entry)["binding"])
                self.assertTrue(token)

    def test_distinct_catalogs_hold_shared_object_store_lock_concurrently(self):
        first = self.bind()
        context = mp.get_context("fork")
        entered = [context.Event(), context.Event()]
        release = context.Event()
        results = context.Queue()
        def bind_child(index):
            def barrier(name, **facts):
                if name == "bind.os_shared_ready":
                    entered[index].set()
                    if not release.wait(10):
                        raise AssertionError("shared lock barrier timed out")
            try:
                result = self.make_owner(hook=barrier).bind("w" + str(index),
                    self.root / ("w" + str(index)), CONFIG, bom("s" + str(index)))
                results.put(result["binding"])
            except Exception as error:
                results.put(str(error))
        children = [context.Process(target=bind_child, args=(index,)) for index in range(2)]
        for child in children:
            child.start()
        try:
            self.assertTrue(entered[0].wait(5))
            self.assertTrue(entered[1].wait(5))
        finally:
            release.set()
            for child in children:
                child.join(10)
                if child.is_alive():
                    child.kill()
                    child.join()
        for child in children:
            self.assertEqual(child.exitcode, 0)
        bindings = [results.get(timeout=2) for _ in children]
        for binding in bindings:
            self.assertIsInstance(binding, dict)
            self.assertEqual(binding["object_store"], first["binding"]["object_store"])
        self.assertNotEqual(bindings[0]["catalog"], bindings[1]["catalog"])

    def test_concurrent_same_key_creates_each_instance_once(self):
        context = mp.get_context("fork")
        reserved, release = context.Event(), context.Event()
        results = context.Queue()
        def child(index):
            def barrier(name, **facts):
                if index == 0 and name == "bind.ports_reserved" and facts["runtime"].startswith("os-"):
                    reserved.set()
                    if not release.wait(10):
                        raise AssertionError("reservation barrier timed out")
            try:
                value = self.make_owner(hook=barrier).bind("w" + str(index),
                    self.root / ("w" + str(index)), CONFIG, bom())
                results.put(value["binding"])
            except Exception as error:
                results.put(str(error))
        children = [context.Process(target=child, args=(index,)) for index in range(2)]
        children[0].start()
        try:
            self.assertTrue(reserved.wait(5))
            children[1].start()
            self.assertEqual(len(self.owner.records()), 1)
            self.assertEqual(self.owner.records()[0]["state"], "starting")
        finally:
            release.set()
            for process in children:
                if process.pid:
                    process.join(10)
                    if process.is_alive():
                        process.kill()
                        process.join()
        bindings = [results.get(timeout=2) for _ in children]
        self.assertEqual(bindings[0], bindings[1])
        self.assertIsInstance(bindings[0], dict)
        for identity in bindings[0].values():
            self.assertEqual((self.backend.root / (identity + ".ensure")).read_text(), "created\n")

    def test_consumer_disconnect_allowed_during_deleting(self):
        first = self.bind()
        cat_id = first["binding"]["catalog"]
        record = self.owner.record(cat_id)
        record.update(state="deleting", deletion_id="intent")
        self.owner.save_record(record)
        self.assert_code("RuntimeDeleting", lambda: self.owner.consumer(cat_id, "consumer"))
        self.owner.consumer(cat_id, "consumer", disconnect=True)

    def test_missing_saved_definition_reconstructed_for_deletion(self):
        first = self.bind()
        cat = first["records"]["catalog"]
        Path(cat["compose_file"]).unlink()
        self.owner.manage(cat["id"], "delete", force=True)
        self.assertIsNone(self.owner.record(cat["id"]))

    def test_crash_after_record_retirement_does_not_poison_same_key_rebuild(self):
        first = self.bind()
        cat_id = first["binding"]["catalog"]
        def interrupt(name, **facts):
            if name == "delete.record_retired":
                raise ValueError("injected retirement crash")
        self.owner.hook = interrupt
        with self.assertRaises(ValueError):
            self.owner.manage(cat_id, "delete", force=True)
        self.assertIsNone(self.owner.record(cat_id))
        self.owner.hook = lambda *a, **k: None
        self.bind(bom("two"), self.root / "other-entry", "other")
        rebuilt = self.bind()
        self.assertEqual(rebuilt["binding"]["catalog"], cat_id)
        self.assertNotEqual(first["records"]["catalog"]["ports"], rebuilt["records"]["catalog"]["ports"])

    def test_two_owner_roots_serialize_on_fixed_worktree_lock(self):
        context = mp.get_context("fork")
        entered, release, attempted, finished = (context.Event() for _ in range(4))
        results = context.Queue()
        def first():
            def barrier(name, **facts):
                if name == "publication.before_swap":
                    entered.set()
                    if not release.wait(10):
                        raise AssertionError("barrier timed out")
            try:
                self.make_owner(hook=barrier).bind("test", self.entry, CONFIG, bom())
                results.put("first-ok")
            except Exception as error:
                results.put(str(error))
        def second():
            with (self.entry / ".owner.lock").open("a+b") as handle:
                try:
                    runtime.fcntl.flock(handle, runtime.fcntl.LOCK_EX | runtime.fcntl.LOCK_NB)
                    results.put("lock-unexpectedly-free")
                except BlockingIOError:
                    results.put("lock-was-held")
            attempted.set()
            try:
                self.make_owner("other").bind("test", self.entry, CONFIG, bom())
                results.put("unexpected-success")
            except runtime.RuntimeFailure as error:
                results.put(error.code)
            finally:
                finished.set()
        one, two = context.Process(target=first), context.Process(target=second)
        one.start()
        try:
            self.assertTrue(entered.wait(5))
            two.start()
            self.assertTrue(attempted.wait(5))
            self.assertEqual(results.get(timeout=2), "lock-was-held")
        finally:
            release.set()
            one.join(10)
            if two.pid:
                two.join(10)
            for process in (one, two):
                if process.pid and process.is_alive():
                    process.kill()
                    process.join()
        self.assertEqual(one.exitcode, 0)
        self.assertEqual(two.exitcode, 0)
        self.assertEqual({results.get(timeout=2), results.get(timeout=2)}, {"first-ok", "OwnerMismatch"})


if __name__ == "__main__":
    unittest.main()
