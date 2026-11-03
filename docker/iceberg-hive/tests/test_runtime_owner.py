#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
import importlib.util
import re
import tomllib
from pathlib import Path
import sys
import copy
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("hive_owner", HERE / "runtime_owner.py")
hive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hive)



sys.path.insert(0, str(HERE.parent / 'iceberg-rest/tests'))
from test_machine_runtime import Backend as MachineBackend, Sweeper, bom


class Backend(MachineBackend):
    def __init__(self):
        super().__init__()
        self.calls, self.hms_containers = [], {}
        self.networks = {hive.machine.NETWORK}
        self.residual = None
        self.timeout = 90

    def validate_resources(self, record):
        self.calls.append(('validate', record['project']))

    def container(self, record, service):
        return self.hms_containers.get(record['project'])

    def compose(self, record, args):
        self.calls.append(('compose', record['project'], args))
        if args[0] == 'up':
            self.hms_containers[record['project']] = {'Id': record['project'] + '-exact-id'}
            for name in record['volumes']:
                self.volumes.setdefault(name, {'Name': name})
        elif args[0] == 'down':
            if self.residual != 'container':
                self.hms_containers.pop(record['project'], None)
            if '-v' in args and self.residual != 'volume':
                for name in record['volumes']:
                    self.volumes.pop(name, None)

    def command(self, args):
        self.calls.append(('command', args))
        if args[:2] == ['volume', 'create']:
            return super().command(args)
        if args[:3] != ['ps', '-a', '--filter']:
            raise AssertionError(args)
        project = args[3].removeprefix('label=com.docker.compose.project=')
        return self.hms_containers.get(project, {}).get('Id', '')

    def inspect(self, kind, identity, *, absent_ok=False):
        if kind == 'volume':
            return copy.deepcopy(self.volumes.get(identity))
        return {'Name': identity} if identity in self.networks else None

    delete_resources = hive.runtime.Docker.delete_resources


class HiveOwnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.backend = Backend()
        self.owner = hive.machine.MachineOwner(self.temp.name, backend=self.backend, sweeper_factory=Sweeper)
        self.snapshot = self.owner.activate(self.owner.required_definition({}, bom()))
        self.identity = self.owner.machine_id
        self.hive = hive.HiveOwner(self.owner)
        self.hive.wait_ready = lambda record: None
        self.backend.calls.clear()

    def manifest(self, ignored=None):
        return {'ready': True, 'runtime': {'machine': copy.deepcopy(self.snapshot)},
            'minio': {'endpoint': 'http://127.0.0.1:28000', 'access_key_id': 'key', 'secret_access_key': 'secret'}}

    def test_fixed_port_external_network_and_exact_attachment(self):
        record = self.hive.up(self.manifest(), 'local-hms')
        self.assertEqual(record['ports'], {'hms': 28005})
        self.assertFalse(record['network_owned'])
        self.assertEqual(record['network'], hive.machine.NETWORK)
        self.assertEqual(record['network_id'], self.snapshot['network_id'])
        self.assertIn(('connect', record['container_id'], 'hms'), self.backend.events)
        definition = Path(record['compose_file']).read_text()
        self.assertIn('external: true', definition)

    def test_prepare_without_docker_then_up_uses_recommendation_only_for_creation(self):
        with mock.patch.object(self.owner, 'assert_live_daemon', side_effect=AssertionError('offline Docker call')):
            first = self.hive.up(self.manifest(), prepare_only=True)
        self.assertEqual(self.backend.calls, [])
        prepared = {'derived_images': {'hive-metastore': {'image_id': 'sha256:created-hms'}}}
        with mock.patch.object(hive.machine, 'prepare_inputs', return_value=prepared) as prepare, \
                mock.patch.object(self.backend, 'healthy', side_effect=AssertionError('environment scan')):
            second = self.hive.up(self.manifest())
        self.assertEqual(prepare.call_args.kwargs, {'consumer': 'derived:hive-metastore'})
        self.assertEqual(second['images']['hms']['image_id'], 'sha256:created-hms')
        self.assertEqual(first['ports'], second['ports'])
        self.assertEqual(first['compose_file'], second['compose_file'])
        calls = list(self.backend.calls)
        with mock.patch.object(hive.machine, 'prepare_inputs', side_effect=AssertionError('existing fixture build')), \
                mock.patch.object(self.backend, 'healthy', side_effect=AssertionError('environment scan')):
            self.assertEqual(self.hive.up(self.manifest(), 'different-recommendation'), second)
        self.assertEqual(self.backend.calls, calls)

    def test_changed_machine_activation_needs_explicit_hms_lifecycle(self):
        self.hive.up(self.manifest(), 'local-hms')
        changed = self.manifest(); changed['runtime']['machine']['generation'] = 'other'
        with self.assertRaisesRegex(hive.runtime.RuntimeFailure, 'MachineUnavailable'):
            self.hive.up(changed, 'local-hms', prepare_only=True)

    def test_ordinary_down_keeps_derby_and_shared_network(self):
        record = self.hive.up(self.manifest(), 'local-hms')
        stopped = self.hive.down(self.identity)
        self.assertEqual(stopped['state'], 'stopped')
        self.assertTrue(set(record['volumes']) <= set(self.backend.volumes))
        self.assertIn(hive.machine.NETWORK, self.backend.networks)
        self.assertIn(('disconnect', record['container_id']), self.backend.events)

    def test_destructive_down_preserves_machine_network_and_is_repeatable(self):
        record = self.hive.up(self.manifest(), 'local-hms')
        result = self.hive.down(self.identity, volumes=True)
        self.assertEqual(result['state'], 'absent')
        self.assertFalse(set(record['volumes']) & set(self.backend.volumes))
        self.assertIn(hive.machine.NETWORK, self.backend.networks)
        self.backend.calls.clear()
        self.assertEqual(self.hive.down(self.identity, volumes=True)['state'], 'absent')
        self.assertEqual(self.backend.calls, [])

    def test_residual_owned_resources_keep_deleting_record_and_retry(self):
        for residual in ('container', 'volume'):
            with self.subTest(residual=residual):
                record = self.hive.up(self.manifest(), 'local-hms')
                self.backend.residual = residual
                with self.assertRaisesRegex(hive.runtime.RuntimeFailure, 'ResourcesStillPresent'):
                    self.hive.down(self.identity, volumes=True)
                self.assertEqual(self.hive.read(self.identity)['state'], 'deleting')
                with self.assertRaisesRegex(hive.runtime.RuntimeFailure, 'RuntimeDeleting'):
                    self.hive.up(self.manifest(), 'local-hms', prepare_only=True)
                self.backend.residual = None
                self.assertEqual(self.hive.down(self.identity)['state'], 'absent')
                self.assertIn(hive.machine.NETWORK, self.backend.networks)

    def test_repeated_up_preserves_bind_mount_inodes_and_secret_free_sql(self):
        record = self.hive.up(self.manifest(), 'local-hms')
        directory = self.hive.directory(self.identity)
        names = ('compose.yml', 'compose.env', 'core-site.xml', 'ice-hms-catalog.sql', 'spark-hms-defaults.conf', 'env.sh')
        before = {n: ((directory / n).stat().st_ino, (directory / n).read_bytes()) for n in names}
        self.hive.up(self.manifest(), 'local-hms')
        self.assertEqual(before, {n: ((directory / n).stat().st_ino, (directory / n).read_bytes()) for n in names})
        sql = (directory / 'ice-hms-catalog.sql').read_text()
        self.assertNotIn('= "secret"', sql)
        self.assertNotIn('= "key"', sql)
        self.assertIn('"credential.object-store-metadata.consumer-role" = "frontend"', sql)
        self.assertIn('"credential.object-store-data.consumer-role" = "backend"', sql)
        record['minio']['secret_access_key'] = 'changed'
        with self.assertRaisesRegex(hive.runtime.RuntimeFailure, 'core-site.xml'):
            self.hive.render(record)

    def test_interrupted_attachment_keeps_original_target_for_manual_maintenance(self):
        self.owner.attach_consumer = mock.Mock(side_effect=hive.runtime.RuntimeFailure('MachineUnavailable', 'injected'))
        with self.assertRaises(hive.runtime.RuntimeFailure):
            self.hive.up(self.manifest(), 'local-hms')
        first = self.hive.read(self.identity)
        self.assertEqual(first['state'], 'starting')
        calls = list(self.backend.calls)
        with self.assertRaisesRegex(hive.runtime.RuntimeFailure, 'manual maintenance'):
            self.hive.up(self.manifest(), 'new-recommendation')
        self.assertEqual(self.hive.read(self.identity), first)
        self.assertEqual(self.backend.calls, calls)

class ReadinessTests(unittest.TestCase):
    manifest = HiveOwnerTest.manifest

    def setUp(self):
        HiveOwnerTest.setUp(self)
        self.record = self.hive.prepare(self.manifest(self.identity), "local-hms")
        self.record["container_id"] = self.record["project"] + "-exact-id"
        self.clock = 0.0
        self.listener = False
        self.running = True
        self.probes = []
        original_inspect = self.backend.inspect
        def inspect(kind, identity, **kwargs):
            if kind != "container":
                return original_inspect(kind, identity, **kwargs)
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
            self.hive.up(self.manifest(self.identity), "local-hms")
        self.assertEqual(self.clock, 3)
        self.assertEqual(len(self.probes), 3)
        self.assertTrue(all(0 < timeout <= 2 for _, _, timeout in self.probes))
        self.assertEqual(self.backend.timeout, 90)
        self.host_proxy.assert_not_called()
        failed = self.hive.read(self.identity)
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

    def test_failed_creation_is_retained_until_manual_maintenance(self):
        with self.assertRaises(hive.runtime.RuntimeFailure):
            self.hive.up(self.manifest(self.identity), "local-hms")
        failed = self.hive.read(self.identity)
        self.assertEqual(failed["state"], "failed")
        self.listener = True
        self.probes.clear()
        with self.assertRaisesRegex(hive.runtime.RuntimeFailure, "manual maintenance"):
            self.hive.up(self.manifest(self.identity), "local-hms")
        self.assertEqual(self.hive.read(self.identity), failed)
        self.assertEqual(self.probes, [])
        self.assertEqual(self.backend.timeout, 90)
        self.host_proxy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
