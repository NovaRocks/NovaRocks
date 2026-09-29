#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
import importlib.util
import re
import tomllib
from pathlib import Path
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("hive_owner", HERE / "runtime_owner.py")
hive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hive)


class Backend:
    def __init__(self):
        self.calls = []
        self.containers = {}
        self.networks = set()
        self.volumes = set()
        self.residual = None
        self.timeout = 90

    def daemon_id(self):
        return "test-daemon"

    def image_id(self, image):
        self.calls.append(("image", image))
        return "sha256:hms"

    def validate_resources(self, record):
        self.calls.append(("validate", record["project"]))

    def container(self, record, service):
        return self.containers.get(record["project"])

    def compose(self, record, args):
        self.calls.append(("compose", record["project"], args))
        if args[0] == "up":
            self.containers[record["project"]] = {"Id": record["project"] + "-exact-id"}
            self.networks.add(record["network"])
            self.volumes.update(record["volumes"])
        if args[0] == "down":
            if self.residual != "container":
                self.containers.pop(record["project"], None)
            if self.residual != "network":
                self.networks.discard(record["network"])
            if ("-v" in args or "--volumes" in args) and self.residual != "volume":
                self.volumes.difference_update(record["volumes"])

    def command(self, args):
        self.calls.append(("command", args))
        if args[:3] != ["ps", "-a", "--filter"]:
            raise AssertionError(args)
        project = args[3].removeprefix("label=com.docker.compose.project=")
        return self.containers.get(project, {}).get("Id", "")

    def inspect(self, kind, identity, *, absent_ok=False):
        self.calls.append(("inspect", kind, identity))
        resources = {"network": self.networks, "volume": self.volumes}
        return {"Name": identity} if identity in resources[kind] else None

    # Exercise the production absence checks against this resource inventory.
    delete_resources = hive.runtime.Docker.delete_resources


class HiveOwnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.backend = Backend()
        self.owner = hive.runtime.RuntimeOwner(self.temp.name, daemon="test-daemon", backend=self.backend, port_start=31000, port_end=31020)
        self.owner.consumer = lambda cat, container, **kw: self.backend.calls.append(("consumer", cat, container, kw))
        self.hive = hive.HiveOwner(self.owner)
        self.hive.wait_ready = lambda record: None

    def manifest(self, cat):
        return {"runtime": {"catalog": {"id": cat, "server_warehouse": f"s3://warehouse/instances/{cat}/server",
                 "network": cat + "_iceberg_net"}},
                "minio": {"endpoint": "http://127.0.0.1:12345", "access_key_id": "key", "secret_access_key": "secret"}}

    def test_generations_have_own_records_ports_volumes_and_exact_attachments(self):
        first = self.hive.up(self.manifest("cat-a"), "local-hms")
        second = self.hive.up(self.manifest("cat-b"), "local-hms")
        self.assertNotEqual(first["project"], second["project"])
        self.assertNotEqual(first["ports"], second["ports"])
        self.assertTrue(31000 <= first["ports"]["hms"] <= 31020)
        self.assertNotEqual(first["volumes"], second["volumes"])
        self.assertNotIn("nr-iceberg-hive", repr(self.backend.calls))
        self.assertIn(("consumer", "cat-a", first["container_id"], {"alias": "hms"}), self.backend.calls)
        self.backend.calls.clear()
        self.hive.down("cat-a", volumes=True)
        operations = self.backend.calls
        disconnect = operations.index(("consumer", "cat-a", first["container_id"], {"disconnect": True}))
        down = operations.index(("compose", first["project"], ["down", "-v"]))
        self.assertLess(disconnect, down)
        self.assertIn(second["project"], self.backend.containers)
        self.assertIsNone(self.hive.read("cat-a"))
        self.assertEqual(self.hive.read("cat-b")["state"], "ready")

    def test_ordinary_down_retains_derby_definition_and_port_reservation(self):
        record = self.hive.up(self.manifest("cat-a"), "local-hms")
        self.owner.port_start = self.owner.port_end = record["ports"]["hms"]
        stopped = self.hive.down("cat-a")
        self.assertEqual(stopped["state"], "stopped")
        self.assertEqual(stopped["ports"], record["ports"])
        self.assertTrue(self.hive.directory("cat-a").is_dir())
        self.assertTrue(set(record["volumes"]) <= self.backend.volumes)
        with self.owner.lock("ports"), self.assertRaisesRegex(hive.runtime.RuntimeFailure, "PortUnavailable"):
            self.owner.allocate(["rest"])
        resumed = self.hive.up(self.manifest("cat-a"), "local-hms")
        self.assertEqual(resumed["ports"], record["ports"])

    def test_destructive_down_retires_definition_and_releases_port_with_stable_lock(self):
        record = self.hive.up(self.manifest("cat-a"), "local-hms")
        self.owner.port_start = self.owner.port_end = record["ports"]["hms"]
        lock = self.owner.base / "locks" / "hms-cat-a.lock"
        inode = lock.stat().st_ino
        self.assertEqual(self.hive.down("cat-a", volumes=True), {"catalog_id": "cat-a", "state": "absent"})
        self.assertNotIn(record["project"], self.backend.containers)
        self.assertNotIn(record["network"], self.backend.networks)
        self.assertFalse(set(record["volumes"]) & self.backend.volumes)
        self.assertFalse(self.hive.directory("cat-a").exists())
        self.assertEqual(lock.stat().st_ino, inode)
        with self.owner.lock("ports"):
            self.assertEqual(self.owner.allocate(["rest"]), {"rest": record["ports"]["hms"]})

    def test_residual_resources_preserve_deleting_record_until_retry_succeeds(self):
        for kind in ("container", "network", "volume"):
            with self.subTest(kind=kind):
                identity = "cat-" + kind
                record = self.hive.up(self.manifest(identity), "local-hms")
                self.owner.port_start = self.owner.port_end = record["ports"]["hms"]
                self.backend.residual = kind
                with self.assertRaises(hive.runtime.RuntimeFailure) as error:
                    self.hive.down(identity, volumes=True)
                self.assertEqual(error.exception.code, "ResourcesStillPresent")
                failed = self.hive.read(identity)
                self.assertEqual(failed["state"], "deleting")
                self.assertEqual(failed["ports"], record["ports"])
                self.assertTrue(Path(failed["compose_file"]).is_file())
                with self.owner.lock("ports"), self.assertRaisesRegex(hive.runtime.RuntimeFailure, "PortUnavailable"):
                    self.owner.allocate(["rest"])
                for prepare in (False, True):
                    with self.assertRaisesRegex(hive.runtime.RuntimeFailure, "RuntimeDeleting"):
                        self.hive.up(self.manifest(identity), "local-hms", prepare_only=prepare)
                self.backend.residual = None
                # The durable deletion intent survives without repeating -v.
                self.assertEqual(self.hive.down(identity)["state"], "absent")
                with self.owner.lock("ports"):
                    self.assertEqual(self.owner.allocate(["rest"])["rest"], record["ports"]["hms"])

    def test_repeated_destructive_down_is_idempotent_without_docker_calls(self):
        self.hive.up(self.manifest("cat-a"), "local-hms")
        self.hive.down("cat-a", volumes=True)
        self.backend.calls.clear()
        self.assertEqual(self.hive.down("cat-a", volumes=True), {"catalog_id": "cat-a", "state": "absent"})
        self.assertEqual(self.backend.calls, [])

    def test_prepare_without_docker_then_retry_keeps_fixed_port(self):
        first = self.hive.up(self.manifest("cat-a"), "local-hms", prepare_only=True)
        self.assertEqual(self.backend.calls, [])
        second = self.hive.up(self.manifest("cat-a"), "local-hms")
        self.assertEqual(first["ports"], second["ports"])
        env = (self.hive.directory("cat-a") / "env.sh").read_text()
        self.assertIn("NOVA_ENV_SHARED_HMS_WAREHOUSE_URI=", env)
        self.assertIn("http://minio:9000", (self.hive.directory("cat-a") / "core-site.xml").read_text())
        self.assertIn("<value>secret</value>", (self.hive.directory("cat-a") / "core-site.xml").read_text())

    def test_native_sql_binds_the_role_local_static_credentials(self):
        import runtime_entry
        record = self.hive.up(self.manifest("cat-a"), "local-hms", prepare_only=True)
        directory = self.hive.directory("cat-a")
        sql = (directory / "ice-hms-catalog.sql").read_text()
        properties = dict(re.findall(r'"([^"\n]+)"\s*=\s*"([^"\n]*)"', sql))
        ports = {"fe_http": 8100, "fe_grpc": 9100, "be_http": 8200, "be_grpc": 9200, "mysql": 9300}
        runtime_entry.render_role_configs(directory, directory, "test-hms", ports,
                                          {"deployment_id": "test-hms", "shared_secret": "test"})
        for purpose, consumer, role in (("metadata", "frontend", "fe"), ("data", "backend", "be")):
            role_config = tomllib.loads((directory / (role + ".toml")).read_text())
            credential = next(item for item in role_config["connector"]["credentials"]
                              if item["purpose"] == "object-store-" + purpose)
            prefix = "credential.object-store-" + purpose + "."
            self.assertEqual(properties[prefix + "consumer-role"], consumer)
            self.assertEqual(properties[prefix + "mode"], "static")
            self.assertEqual(properties[prefix + "name"], credential["name"])
            self.assertEqual(properties[prefix + "generation"], credential["generation"])
        self.assertEqual(properties["iceberg.catalog.warehouse"], record["hms"]["warehouse"])
        self.assertEqual(properties["aws.s3.endpoint"], record["minio"]["endpoint"])
        self.assertNotIn("aws.s3.access_key", properties)
        self.assertNotIn("aws.s3.secret_key", properties)
        self.assertNotIn('= "secret"', sql)
        self.assertNotIn('= "key"', sql)

    def test_repeated_up_keeps_saved_definition_and_bind_mount_inodes(self):
        record = self.hive.up(self.manifest("cat-a"), "local-hms")
        directory = self.hive.directory("cat-a")
        names = ("compose.yml", "compose.env", "core-site.xml", "ice-hms-catalog.sql", "spark-hms-defaults.conf", "env.sh")
        before = {name: ((directory / name).stat().st_ino, (directory / name).read_bytes()) for name in names}
        self.hive.up(self.manifest("cat-a"), "local-hms")
        after = {name: ((directory / name).stat().st_ino, (directory / name).read_bytes()) for name in names}
        self.assertEqual(before, after)
        record["minio"]["secret_access_key"] = "different-secret"
        with self.assertRaisesRegex(hive.runtime.RuntimeFailure, "core-site.xml"):
            self.hive.render(record)
        self.assertEqual(before["core-site.xml"], ((directory / "core-site.xml").stat().st_ino,
                                                 (directory / "core-site.xml").read_bytes()))

    def test_interrupted_connection_replays_same_container_and_record(self):
        def fail(*args, **kwargs):
            raise hive.runtime.RuntimeFailure("RuntimeDeleting", "cat-a")
        self.owner.consumer = fail
        with self.assertRaises(hive.runtime.RuntimeFailure):
            self.hive.up(self.manifest("cat-a"), "local-hms")
        interrupted = self.hive.read("cat-a")
        self.assertEqual(interrupted["state"], "starting")
        self.assertIsNotNone(interrupted["container_id"])
        self.owner.consumer = lambda *args, **kw: None
        recovered = self.hive.up(self.manifest("cat-a"), "local-hms")
        self.assertEqual(recovered["container_id"], interrupted["container_id"])
        self.assertEqual(recovered["ports"], interrupted["ports"])


class ReadinessTests(unittest.TestCase):
    manifest = HiveOwnerTest.manifest

    def setUp(self):
        HiveOwnerTest.setUp(self)
        self.record = self.hive.up(self.manifest("cat-ready"), "local-hms")
        self.clock = 0.0
        self.listener = False
        self.running = True
        self.probes = []
        def inspect(kind, identity, **kwargs):
            self.assertEqual(kind, "container")
            self.assertEqual(identity, self.record["container_id"])
            return {"Id": identity, "State": {"Running": self.running}}
        def command(args):
            self.probes.append((self.clock, args, self.backend.timeout))
            self.assertEqual(args[:4], ["exec", self.record["container_id"], "/bin/bash", "-c"])
            self.assertIn("/dev/tcp/127.0.0.1/9083", args[4])
            if not self.listener:
                raise hive.runtime.RuntimeFailure("DockerOperationFailed", "exec exited with 1")
            return ""
        self.backend.inspect = inspect
        self.backend.command = command
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(hive.time, "monotonic", side_effect=lambda: self.clock).start()
        mock.patch.object(hive.time, "sleep", side_effect=self.advance).start()
        # Simulate an accepting host proxy. It must never be consulted.
        self.host_proxy = mock.patch.object(hive.socket, "create_connection").start()
        self.hive.wait_ready = lambda record: hive.HiveOwner.wait_ready(self.hive, record, timeout=3)

    def advance(self, duration):
        self.clock += duration

    def test_host_proxy_does_not_publish_ready_and_deadline_is_saved(self):
        with self.assertRaisesRegex(hive.runtime.RuntimeFailure, "deadline"):
            self.hive.up(self.manifest("cat-ready"), "local-hms")
        self.assertEqual(self.clock, 3)
        self.assertEqual(len(self.probes), 3)
        self.assertTrue(all(0 < timeout <= 2 for _, _, timeout in self.probes))
        self.assertEqual(self.backend.timeout, 90)
        self.host_proxy.assert_not_called()
        failed = self.hive.read("cat-ready")
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["failure"]["code"], "RuntimeNotReady")

    def test_process_exit_is_immediate_failure_without_listener_probe(self):
        self.running = False
        with self.assertRaisesRegex(hive.runtime.RuntimeFailure, "exited"):
            self.hive.wait_ready(self.record)
        self.assertEqual(self.probes, [])
        self.assertEqual(self.clock, 0)
        self.assertEqual(self.backend.timeout, 90)
        self.host_proxy.assert_not_called()

    def test_actual_container_listener_is_required_and_recovers_failed_record(self):
        with self.assertRaises(hive.runtime.RuntimeFailure):
            self.hive.up(self.manifest("cat-ready"), "local-hms")
        self.assertEqual(self.hive.read("cat-ready")["state"], "failed")
        self.clock = 0
        def become_ready(duration):
            self.advance(duration)
            self.listener = True
        hive.time.sleep.side_effect = become_ready
        self.probes.clear()
        ready = self.hive.up(self.manifest("cat-ready"), "local-hms")
        self.assertEqual(ready["state"], "ready")
        self.assertNotIn("failure", ready)
        self.assertEqual(len(self.probes), 2)
        self.assertEqual(self.clock, 1)
        self.assertEqual(self.backend.timeout, 90)
        self.host_proxy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
