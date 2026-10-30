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

"""Immutable entry, real shell boundaries and unchanged private lifecycle contracts."""
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import fixture_runtime as runtime
import machine_runtime as machine
import entry_publication as publication
import runtime_entry as entry
from test_machine_runtime import Backend, Sweeper, bom


class RendererCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.workspace = self.root / 'workspace'
        self.workspace.mkdir()
        self.entry = self.root / 'runtime' / 'test-worktree'
        self.backend = Backend()
        self.owner = machine.MachineOwner(self.root / 'machine', backend=self.backend, sweeper_factory=Sweeper)
        self.snapshot = self.owner.activate(self.owner.required_definition({}, bom()))
        self.config = entry.request_config(self.workspace, entry.load_settings(HERE / 'shared.env'), HERE / 'shared.env', self.entry, shared=True, update_current=False)
        self.config['fixture_inputs'] = {'bom': str(self.root / 'bom.json'), 'lock_sha256': bom()['lock_sha256'], 'verified': True}

    def bind(self, inputs=None):
        return entry.attach_entry(self.owner, 'test-worktree', self.entry, self.config, inputs or bom())

    def environment(self, path, **extra):
        output = subprocess.check_output(['bash', '-c', 'source "$1"; python3 -c "import os,json; print(json.dumps(dict(os.environ)))"', '_', str(path)], env={**os.environ, **extra}, text=True)
        return json.loads(output)


class RendererTests(RendererCase):
    def test_cloud_request_attaches_to_the_same_default_benchmark_domain(self):
        from test_machine_oss import CLOUD, CloudMachineTests
        fixture = CloudMachineTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        snapshot = fixture.start()
        cloud_file = self.root / "cloud-attach.json"
        cloud_file.write_text(json.dumps(CLOUD))
        settings = entry.load_settings(HERE / "shared.env")
        settings["NOVA_ENV_OBJECT_STORE_CONFIG"] = str(cloud_file)
        config = entry.request_config(self.workspace, settings, HERE / "shared.env", self.entry, shared=True, update_current=False)
        result = entry.attach_entry(fixture.owner, config["env_id"], self.entry, config, fixture.inputs)
        self.assertTrue(result["ready"])
        self.assertEqual(config["benchmark"]["shared_root"].rstrip('/'), snapshot["config"]["benchmark_root"])
        self.assertEqual(fixture.backend.events, ["ensure"])
        self.assertNotIn("minio", json.loads((Path(result["published_dir"]) / "manifest.json").read_text()))

    def test_cloud_render_has_independent_role_sources_and_no_static_material(self):
        cloud = {"kind": "aliyun-oss", "region": "cn-beijing", "bucket": "test-bucket",
                 "role_name": "test-role", "root_prefix": "fixture/"}
        cloud_file = self.root / "cloud.json"
        cloud_file.write_text(json.dumps(cloud))
        settings = entry.load_settings(HERE / "shared.env")
        settings["NOVA_ENV_OBJECT_STORE_CONFIG"] = str(cloud_file)
        config = entry.request_config(self.workspace, settings, HERE / "shared.env", self.entry, shared=True, update_current=False)
        self.assertNotIn("credentials", config)
        self.assertEqual(config["benchmark"]["shared_root"], "s3://test-bucket/fixture/benchmarks/")
        snapshot = copy.deepcopy(self.snapshot)
        snapshot["config"] = {"object_store": cloud, "benchmark_root": "s3://test-bucket/fixture/benchmarks"}
        snapshot.pop("credentials")
        snapshot["server_warehouses"] = {"rest": "s3://test-bucket/fixture/machines/id/gen/rest",
                                         "rest-mv": "s3://test-bucket/fixture/machines/id/gen/rest-mv"}
        snapshot["storage_owner"] = {"provider": "aliyun-oss", "endpoint": "https://s3.oss-cn-beijing-internal.aliyuncs.com",
                                     "region": "cn-beijing", "bucket": "test-bucket", "root_prefix": "fixture/"}
        snapshot["oss_ops_python"] = "/locked-sdk/bin/python"
        snapshot["object_store_config_file"] = str(self.root / "machine-generation/object-store.json")
        snapshot["images"] = {key: value for key, value in snapshot["images"].items() if key in {"rest", "rest-mv", "spark"}}
        endpoints = {"object_store_endpoint": snapshot["storage_owner"]["endpoint"], "rest_uri": "http://127.0.0.1:28002",
            "rest_mv_uri": "http://127.0.0.1:28003", "container_rest_uri": "http://rest:8181",
            "container_rest_mv_uri": "http://rest-mv:8181", "spark_ui": "http://127.0.0.1:28004"}
        output = self.root / "cloud-render"
        with mock.patch.object(machine, "endpoints", return_value=endpoints):
            entry.render_entry({"ready": True, "worktree": config["env_id"], "config": config, "machine": snapshot,
                "publication_dir": str(output), "stable_runtime_dir": str(self.entry)}, output)
        environment = self.environment(output / "env.sh", AWS_S3_ACCESS_KEY_ID="old", AWS_S3_SECRET_ACCESS_KEY="old", MINIO_ROOT_USER="old",
            AWS_REQUEST_CHECKSUM_CALCULATION="WHEN_SUPPORTED", AWS_RESPONSE_CHECKSUM_VALIDATION="WHEN_SUPPORTED")
        for name in ("AWS_S3_ACCESS_KEY_ID", "AWS_S3_SECRET_ACCESS_KEY", "MINIO_ROOT_USER", "NOVA_ENV_MINIO_PORT"):
            self.assertNotIn(name, environment)
        self.assertEqual(environment["NOVA_ENV_OBJECT_STORE_CONFIG"], snapshot["object_store_config_file"])
        for name in ("AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION"):
            self.assertEqual("WHEN_REQUIRED", environment[name])
            self.assertIn(name, entry.MANAGED)
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertNotIn("minio", manifest)
        self.assertEqual(manifest["storage_owner"], snapshot["storage_owner"])
        names = []
        for role in ("fe", "be"):
            source = tomllib.loads((output / (role + ".toml")).read_text())["connector"]["credentials"][0]
            self.assertEqual(source["kind"], "aliyun-ecs-ram-role")
            self.assertEqual(source["scope_prefixes"], ["s3://test-bucket/fixture/"])
            self.assertNotIn("access_key_id", source)
            names.append(source["name"])
        self.assertEqual(names, ["iceberg-oss-metadata", "iceberg-oss-data"])
        values = tomllib.loads((output / "sql-test.toml").read_text())["env"]
        self.assertNotIn("oss_ak", values)
        self.assertEqual(values["oss_path_style"], "false")
        spark = (output / "spark-defaults.conf").read_text()
        self.assertIn("AliyunEcsCredentialsProvider", spark)
        self.assertIn("AliyunEcsV1CredentialsProvider", spark)
        self.assertNotIn("access-key-id", spark)

    def publication_hashes(self, directory):
        return {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                for name in runtime.ENTRY_FILES}

    def test_full_snapshot_without_owner_facade_and_stable_runtime_data(self):
        first = self.bind()
        directory = Path(first['published_dir'])
        manifest = json.loads((directory / 'manifest.json').read_text())
        self.assertEqual(manifest['runtime']['machine']['generation'], self.snapshot['generation'])
        self.assertNotIn('catalog', manifest['runtime'])
        self.assertNotIn('object_store', manifest['runtime'])
        self.assertEqual(manifest['runtime']['producer_receipt'], self.snapshot['producer_receipt'])
        env = self.environment(directory / 'env.sh', REST_IMAGE='foreign',
            AWS_REQUEST_CHECKSUM_CALCULATION='WHEN_REQUIRED', AWS_RESPONSE_CHECKSUM_VALIDATION='WHEN_REQUIRED')
        self.assertNotIn('AWS_REQUEST_CHECKSUM_CALCULATION', env)
        self.assertNotIn('AWS_RESPONSE_CHECKSUM_VALIDATION', env)
        self.assertEqual(env['NOVA_ENV_REST_ENV_FILE'], str(directory / 'env.sh'))
        self.assertEqual(env['NOVAROCKS_STATE_STORE_PATH'], str(self.entry / 'frontend-state.sqlite'))
        self.assertEqual(env['NOVA_FIXTURE_MACHINE_GENERATION'], self.snapshot['generation'])
        self.assertEqual(env['AWS_S3_ACCESS_KEY_ID'], self.snapshot['credentials']['access_key'])
        self.assertEqual(env['NOVAROCKS_PAIMON_DOCKER_NETWORK'], self.snapshot['network'])
        fe = tomllib.loads((directory / 'fe.toml').read_text())
        self.assertEqual(fe['state_store']['path'], str(self.entry / 'frontend-state.sqlite'))
        for name in runtime.ENTRY_FILES:
            self.assertTrue((directory / name).is_file(), name)
            self.assertNotIn('.prepare-', (directory / name).read_text())

    def test_same_configuration_reuses_exact_publication_and_env_bytes(self):
        first = self.bind()
        before = (self.entry / 'env.sh').read_bytes()
        second = self.bind()
        self.assertEqual(first['published_dir'], second['published_dir'])
        self.assertEqual(before, (self.entry / 'env.sh').read_bytes())
        self.assertEqual(self.backend.events, ['ensure'])

    def test_attach_full_validation_can_exceed_short_lock_wait_budget(self):
        now = [100.0]
        def pending(base):
            now[0] += 0.9
            return []
        original_volume = self.backend.validate_minio_volume
        def volume(snapshot):
            now[0] += 0.9
            original_volume(snapshot)
        def healthy(snapshot):
            self.assertEqual(self.backend.operation_deadline, 190.0)
            now[0] += 1.2
            return True
        with mock.patch.object(entry.time, 'monotonic', side_effect=lambda: now[0]), \
                mock.patch.object(self.backend, 'unsettled_operations', side_effect=pending), \
                mock.patch.object(self.backend, 'validate_minio_volume', side_effect=volume), \
                mock.patch.object(self.backend, 'healthy', side_effect=healthy):
            result = self.bind()
        self.assertTrue(result['ready'])
        self.assertGreater(now[0], 102.0)
        self.assertIsNone(self.backend.operation_deadline)
        self.assertTrue(publication.read(self.entry)['ready'])

    def test_attach_machine_lock_wait_remains_two_seconds(self):
        now = [100.0]
        other = machine.MachineOwner(self.owner.root, backend=self.backend)
        def sleep(seconds):
            now[0] += seconds
        with other.maintenance_lock(), \
                mock.patch.object(entry.time, 'monotonic', side_effect=lambda: now[0]), \
                mock.patch.object(machine.time, 'sleep', side_effect=sleep), \
                mock.patch.object(self.backend, 'healthy') as healthy:
            with self.assertRaisesRegex(runtime.RuntimeFailure, 'MachineMaintaining'):
                self.bind()
        self.assertAlmostEqual(now[0], 102.0)
        healthy.assert_not_called()
        self.assertFalse(publication.read(self.entry)['ready'])
        self.assertIsNone(self.backend.operation_deadline)

    def test_attach_validation_expiry_invalidates_entry_without_extending_outer_budget(self):
        for outer, health_end in ((None, 190.0), (104.0, 104.0)):
            with self.subTest(outer=outer):
                now = [100.0]
                self.bind()
                self.assertTrue(publication.read(self.entry)['ready'])
                self.backend.operation_deadline = outer
                def healthy(snapshot):
                    self.assertEqual(self.backend.operation_deadline, 190.0 if outer is None else outer)
                    now[0] = health_end
                    return True
                with mock.patch.object(entry.time, 'monotonic', side_effect=lambda: now[0]), \
                        mock.patch.object(self.backend, 'healthy', side_effect=healthy):
                    with self.assertRaisesRegex(runtime.RuntimeFailure, 'RunBudgetExpired'):
                        self.bind()
                self.assertFalse(publication.read(self.entry)['ready'])
                self.assertEqual(self.backend.operation_deadline, outer)
                self.backend.operation_deadline = None

    def test_server_warehouse_and_storage_owner_are_actual_machine_facts(self):
        first = self.bind()
        directory = Path(first['published_dir'])
        manifest = json.loads((directory / 'manifest.json').read_text())
        env = self.environment(directory / 'env.sh', NOVA_FIXTURE_RUN_CONTEXT='original-budget',
                               NOVA_FIXTURE_RUN_STORAGE='original-storage')
        for service, prefix in (('rest', 'NOVA_ENV_REST_SERVER_WAREHOUSE_URI'),
                                ('rest-mv', 'NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI')):
            self.assertEqual(env[prefix], self.snapshot['server_warehouses'][service] + '/')
        self.assertNotEqual(env['NOVA_ENV_REST_SERVER_WAREHOUSE_URI'], env['NOVAROCKS_ICEBERG_REST_WAREHOUSE'])
        self.assertEqual(manifest['storage_owner'], {'daemon_id': self.snapshot['daemon_id'],
            'minio_volume': self.snapshot['config']['minio_volume']})
        self.assertEqual(manifest['storage_domains']['independent']['worktree'], 's3://novarocks/test-worktree/')
        self.assertEqual(env['NOVA_FIXTURE_RUN_CONTEXT'], 'original-budget')
        self.assertEqual(env['NOVA_FIXTURE_RUN_STORAGE'], 'original-storage')
        self.assertNotIn('original-budget', (directory / 'env.sh').read_text())
        self.assertNotIn('original-storage', (directory / 'manifest.json').read_text())
        self.assertIn('only writers using the run credentials', (directory / 'README.md').read_text())

    def test_sorted_json_roundtrip_reuses_every_publication_byte(self):
        first = self.bind()
        directory = Path(first['published_dir'])
        before = self.publication_hashes(directory)
        # A new invocation reads dictionaries from sorted atomic JSON instead
        # of retaining the first request's in-memory insertion order.
        self.config = json.loads(json.dumps(self.config, sort_keys=True))
        self.owner = machine.MachineOwner(self.root / 'machine', backend=self.backend,
                                          sweeper_factory=Sweeper)
        second = self.bind()
        self.assertEqual(first['published_dir'], second['published_dir'])
        self.assertEqual(before, self.publication_hashes(directory))
        self.assertEqual(self.backend.events, ['ensure'])

    def test_renderer_revision_switch_preserves_old_schema_two_publication(self):
        first = self.bind()
        old_metadata = copy.deepcopy(first)
        old_metadata.pop('published_dir')
        old_metadata.pop('renderer_revision', None)
        old_directory = self.entry / 'publications' / runtime.digest(old_metadata)
        context = {**old_metadata, 'env_id': old_metadata['worktree'],
                   'publication_dir': str(old_directory), 'stable_runtime_dir': str(self.entry)}
        old_directory.mkdir()
        entry.render_entry(context, old_directory)
        runtime.atomic_json(old_directory / 'entry.json', old_metadata)
        old_hashes = self.publication_hashes(old_directory)
        runtime.atomic_json(old_directory / 'files.json', old_hashes)
        (self.entry / 'published').unlink()
        (self.entry / 'published').symlink_to(Path('publications') / old_directory.name)
        self.assertNotIn('renderer_revision', publication.read(self.entry))
        def interrupted(point):
            if point == 'publication.before_swap':
                raise RuntimeError('interrupted renderer revision switch')
        with self.assertRaisesRegex(RuntimeError, 'interrupted renderer revision switch'):
            publication.publish(self.entry, old_metadata, entry.render_entry, hook=interrupted)
        self.assertEqual(publication.read(self.entry)['published_dir'], str(old_directory))
        final = self.bind()
        self.assertEqual(final['renderer_revision'], publication.RENDERER_REVISION)
        self.assertNotEqual(final['published_dir'], str(old_directory))
        self.assertEqual(old_hashes, self.publication_hashes(old_directory))

    def test_new_activation_changes_identity_and_old_snapshot_remains_immutable(self):
        first = self.bind()
        old_bytes = (Path(first['published_dir']) / 'env.sh').read_bytes()
        self.owner.activate(self.owner.required_definition({}, bom()), upgrade=True)
        second = self.bind()
        self.assertNotEqual(first['published_dir'], second['published_dir'])
        self.assertEqual((Path(first['published_dir']) / 'env.sh').read_bytes(), old_bytes)
        self.assertNotEqual(old_bytes, (self.entry / 'env.sh').read_bytes())

    def test_version_failure_does_not_change_services_and_clears_ready_entry(self):
        first = self.bind()
        before = copy.deepcopy(self.backend.events)
        changed = bom(); changed['derived_images']['rest-mv']['definition_sha256'] = 'different'
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'MachineVersionMismatch'):
            self.bind(changed)
        self.assertFalse(publication.read(self.entry)['ready'])
        self.assertEqual(self.backend.events, before)
        self.assertTrue((Path(first['published_dir']) / 'env.sh').is_file())

    def test_identity_observation_failure_disables_discovery_without_mutating_services(self):
        first = self.bind()
        before = copy.deepcopy(self.backend.events)
        with mock.patch.object(self.backend, "healthy", side_effect=runtime.RuntimeFailure("RuntimeIdentityMismatch", "replaced service")):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "RuntimeIdentityMismatch"):
                self.bind()
        self.assertFalse(publication.read(self.entry)["ready"])
        self.assertEqual(self.backend.events, before)
        self.assertTrue((Path(first["published_dir"]) / "env.sh").is_file())

    def test_renderer_and_interrupted_commit_preserve_previous_complete_publication(self):
        first = self.bind()
        invalid = copy.deepcopy(self.config); del invalid['local_ports']['mysql']
        with self.assertRaises(KeyError):
            entry.attach_entry(self.owner, 'test-worktree', self.entry, invalid, bom())
        self.assertEqual(publication.read(self.entry), first)
        changed = copy.deepcopy(first); changed['config']['benchmark']['build_timeout_seconds'] = 120
        for point in ('publication.prepared', 'publication.before_swap'):
            def fail(name):
                if name == point:
                    raise InterruptedError('injected commit interruption')
            with self.assertRaises(InterruptedError):
                publication.publish(self.entry, changed, entry.render_entry, hook=fail)
            self.assertEqual(publication.read(self.entry), first)

    def test_corrupt_publication_fails_closed(self):
        first = self.bind()
        (Path(first['published_dir']) / 'be.toml').write_text('corrupt')
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'IncompletePublication'):
            publication.read(self.entry)
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'IncompletePublication'):
            self.bind()

    def test_legacy_owner_metadata_is_not_read_during_explicit_prepare(self):
        legacy = self.entry / 'publications' / 'legacy'
        legacy.mkdir(parents=True)
        (legacy / 'binding.json').write_text('unreadable retired owner data')
        (legacy / 'env.sh').write_text('export NOVA_ENV_READY=true\nexport AWS_S3_ENDPOINT=stale\n')
        (self.entry / 'published').symlink_to('publications/legacy')
        (self.entry / 'env.sh').symlink_to('published/env.sh')
        config = entry.request_config(self.workspace, entry.load_settings(HERE / 'shared.env'), HERE / 'shared.env', self.entry, shared=True, update_current=False)
        result = entry.prepare_entry('test-worktree', self.entry, config)
        self.assertFalse(result['ready'])
        self.assertNotIn('AWS_S3_ENDPOINT', self.environment(self.entry / 'env.sh'))
        self.assertTrue((legacy / 'binding.json').is_file())

    def test_cli_returns_only_public_identity_and_input_failure_disables_entry(self):
        environment = {**os.environ, 'NOVAROCKS_WORKSPACE_ROOT': str(self.workspace),
            'NOVA_ENV_CONFIG_FILE': str(HERE / 'shared.env'), 'NOVA_ENV_UPDATE_CURRENT': 'true'}
        def verified(config):
            config['fixture_inputs'].update(lock_sha256=bom()['lock_sha256'], verified=True)
            return bom()
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.object(machine, 'MachineOwner', return_value=self.owner), mock.patch.object(entry, 'verify_inputs', side_effect=verified):
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(entry.worktree_main(['up']), 0)
            public = json.loads(output.getvalue())
            self.assertEqual(set(public), {'ready', 'published_dir', 'machine'})
            self.assertNotIn('credentials', public['machine'])
            self.assertNotIn(self.snapshot['credentials']['secret_key'], output.getvalue())
            with mock.patch.object(entry, 'verify_inputs', side_effect=runtime.RuntimeFailure('FixturePrerequisiteMissing', 'absent')), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(entry.worktree_main(['up']), 75)
            actual_entry = Path(public['published_dir']).parent.parent
            self.assertFalse(publication.read(actual_entry)['ready'])

    def test_prepare_is_unavailable_and_offline_even_after_ready(self):
        self.bind()
        with mock.patch.object(machine, 'MachineOwner', side_effect=AssertionError('offline invoked Docker owner')):
            result = entry.prepare_entry('test-worktree', self.entry, self.config)
        self.assertFalse(result['ready'])
        env = self.environment(self.entry / 'env.sh', NOVA_ENV_REST_PORT='stale', NOVA_FIXTURE_MACHINE_GENERATION='stale')
        self.assertEqual(env['NOVA_ENV_READY'], 'false')
        self.assertNotIn('NOVA_ENV_REST_PORT', env)
        self.assertNotIn('NOVA_FIXTURE_MACHINE_GENERATION', env)

    def test_attach_publication_commit_holds_machine_lock(self):
        original = entry.render_entry
        def renderer(context, staging):
            other = machine.MachineOwner(self.owner.root, backend=self.backend, timeout=0.001)
            with self.assertRaisesRegex(runtime.RuntimeFailure, 'MachineMaintaining'):
                with other.maintenance_lock(deadline=entry.time.monotonic()):
                    self.fail('maintenance entered during publication')
            original(context, staging)
        with mock.patch.object(entry, 'render_entry', side_effect=renderer):
            self.bind()


class InheritedAttachBudgetTests(RendererCase):
    wall = 1792650000.0

    def context(self, remaining_work):
        from scratch import ScratchIdentity
        identity = ScratchIdentity.create('s3://novarocks/inherited-owner',
            now_ms=int(self.wall * 1000) - 10000, token='b' * 32)
        grace = self.snapshot['policy']['cleanup_grace_seconds']
        return {'schema': 1, 'identity': identity.as_dict(), 'started_unix_ms': int(self.wall * 1000) - 10000,
                'deadline_unix_ms': int((self.wall + grace + remaining_work) * 1000),
                'cleanup_grace_seconds': grace}

    def invoke(self, raw_context, *, health_seconds=0, expected_deadline=None):
        now = [100.0]
        fresh_owner = machine.MachineOwner(self.owner.root, backend=self.backend)
        environment = {**os.environ, 'NOVAROCKS_WORKSPACE_ROOT': str(self.workspace),
            'NOVA_ENV_CONFIG_FILE': str(HERE / 'shared.env'), 'NOVA_ENV_UPDATE_CURRENT': 'false',
            'NOVA_FIXTURE_RUN_CONTEXT': raw_context}
        def verified(config):
            config['fixture_inputs'].update(lock_sha256=bom()['lock_sha256'], verified=True)
            return bom()
        def healthy(snapshot):
            if expected_deadline is not None:
                self.assertEqual(self.backend.operation_deadline, expected_deadline)
            now[0] += health_seconds
            return True
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.dict(os.environ, environment, clear=True), \
                mock.patch.object(entry.time, 'time', return_value=self.wall), \
                mock.patch.object(entry.time, 'monotonic', side_effect=lambda: now[0]), \
                mock.patch.object(machine, 'MachineOwner', return_value=fresh_owner), \
                mock.patch.object(entry, 'verify_inputs', side_effect=verified), \
                mock.patch.object(self.backend, 'image_id', wraps=self.backend.image_id) as image_id, \
                mock.patch.object(self.backend, 'healthy', side_effect=healthy) as health, \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = entry.worktree_main(['up'])
            self.assertEqual(os.environ['NOVA_FIXTURE_RUN_CONTEXT'], raw_context)
        actual_entry = self.workspace / 'docker/iceberg-rest/runtime' / entry.environment_identity(self.workspace)
        self.assertIsNone(getattr(self.backend, 'operation_deadline', None))
        return code, publication.read(actual_entry), stdout.getvalue(), stderr.getvalue(), health.call_count, image_id.call_count

    def test_real_cli_env_context_bounds_full_validation_without_rewriting_parent(self):
        original = self.context(4)
        raw = json.dumps(original, indent=2)
        code, published, stdout, stderr, health_calls, _ = self.invoke(raw, health_seconds=3, expected_deadline=104.0)
        self.assertEqual(code, 0, stderr)
        self.assertTrue(published['ready'])
        self.assertEqual(health_calls, 1)
        directory = Path(json.loads(stdout)['published_dir'])
        for path in directory.iterdir():
            if path.is_file():
                self.assertNotIn(original['identity']['run_id'], path.read_text(), path.name)
        self.assertNotIn('run_context', published['config'])

    def test_real_cli_rejects_expired_context_before_definition_or_health(self):
        code, published, _, stderr, health_calls, image_calls = self.invoke(json.dumps(self.context(-1)))
        self.assertEqual(code, 1)
        self.assertIn('RunBudgetExpired', stderr)
        self.assertFalse(published['ready'])
        self.assertEqual((health_calls, image_calls), (0, 0))

    def test_real_cli_context_work_expiry_and_owner_cap_do_not_publish_ready(self):
        for remaining, expected, health_seconds in ((4, 104.0, 4), (200, 190.0, 90)):
            with self.subTest(remaining=remaining):
                code, published, _, stderr, health_calls, _ = self.invoke(
                    json.dumps(self.context(remaining)), health_seconds=health_seconds, expected_deadline=expected)
                self.assertEqual(code, 1)
                self.assertIn('RunBudgetExpired', stderr)
                self.assertEqual(health_calls, 1)
                self.assertFalse(published['ready'])

    def test_real_cli_invalid_context_fails_fast_and_null_remains_ordinary_up(self):
        wrong_grace = self.context(4)
        wrong_grace['cleanup_grace_seconds'] = 1
        excessive = self.context(4)
        excessive['deadline_unix_ms'] = excessive['started_unix_ms'] + 86401000
        invalid = ('{', '[]', '{"schema":1}', 'x' * 16385, json.dumps(wrong_grace), json.dumps(excessive))
        for raw in invalid:
            with self.subTest(raw=raw[:30]):
                code, published, _, stderr, health_calls, image_calls = self.invoke(raw)
                self.assertEqual(code, 75)
                self.assertIn('RunBudgetRejected', stderr)
                self.assertFalse(published['ready'])
                self.assertEqual((health_calls, image_calls), (0, 0))
        code, published, _, stderr, health_calls, _ = self.invoke('null', health_seconds=3, expected_deadline=190.0)
        self.assertEqual(code, 0, stderr)
        self.assertTrue(published['ready'])
        self.assertEqual(health_calls, 1)


class SharedEnvironmentTests(RendererCase):
    def test_independent_domains_reject_actual_overlap_and_keep_sibling_boundaries(self):
        domains = entry.storage_domains(self.config, self.snapshot, shared=True)
        self.assertNotEqual(domains['independent']['rest'], domains['independent']['rest-mv'])
        for value in ('s3://warehouse/fixture-machine/rest/',
                      's3://warehouse/fixture-machine/',
                      's3://novarocks/test-worktree/child/'):
            record = copy.deepcopy(self.snapshot)
            record['config']['benchmark_root'] = value
            with self.subTest(value=value), self.assertRaisesRegex(runtime.RuntimeFailure, 'independent storage domains overlap'):
                entry.storage_domains(self.config, record, shared=True)
        record = copy.deepcopy(self.snapshot)
        record['config']['benchmark_root'] = 's3://warehouse/fixture-machine/rest-other/'
        entry.storage_domains(self.config, record, shared=True)

    def test_invalid_warehouse_cannot_publish_ready(self):
        self.bind()
        self.config['warehouses']['catalog'] = 's3://novarocks/shared/benchmarks/catalog'
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'exact worktree storage root'):
            self.bind()
        self.assertFalse(publication.read(self.entry)['ready'])

    def test_private_run_children_are_legal_without_claiming_unknown_roots(self):
        original = 's3://novarocks/test-worktree/scratch/original-run'
        config = copy.deepcopy(self.config)
        config['warehouses'] = {name: original + '/' + name for name in config['warehouses']}
        record = {'external_minio': self.snapshot, 'run_context': {'identity': {'scope_root': original}}}
        domains = entry.storage_domains(config, record, shared=False)
        self.assertEqual(domains['independent']['run_root'], original + '/')
        self.assertEqual(len(domains['owned_children']), 4)
        self.assertNotIn('s3://novarocks/', domains['independent'].values())
        config['warehouses']['rest_client'] = 's3://warehouse/fixture-machine/rest/private'
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'original run storage root'):
            entry.storage_domains(config, record, shared=False)
        self.assertEqual(entry.storage_domains(config, {}, shared=False), {})

    def test_storage_root_rejects_ambiguous_keys_without_normalizing_them(self):
        self.assertEqual(entry.storage_root('s3://bucket/root/'), 's3://bucket/root/')
        self.assertEqual(entry.storage_root('s3://bucket/root'), 's3://bucket/root/')
        for uri in ('s3://bucket/root//', 's3://bucket/root//child', 's3://bucket/root/./child',
                    's3://bucket/root/../child', 's3://bucket/root/%2fchild', 's3://bucket/root?',
                    's3://bucket/root#', 's3://bucket/root?query', 's3a://bucket/root', 's3://bucket/',
                    '\0s3://bucket/root', 's3://bucket/roo*t', 's3://bucket/roo\tt'):
            with self.subTest(uri=uri), self.assertRaises(runtime.RuntimeFailure):
                entry.storage_root(uri)

    def test_offline_request_validates_benchmark_directory_spelling(self):
        settings = entry.load_settings(HERE / 'shared.env')
        config = entry.request_config(self.workspace, settings, HERE / 'shared.env', self.entry,
                                      shared=True, update_current=False)
        self.assertTrue(config['benchmark']['shared_root'].endswith('/'))
        settings['NOVA_ENV_SHARED_BENCHMARK_ROOT'] = 's3://novarocks/shared//benchmarks'
        with self.assertRaisesRegex(runtime.RuntimeFailure, 'ambiguous path component'):
            entry.request_config(self.workspace, settings, HERE / 'shared.env', self.entry,
                                 shared=True, update_current=False)

    def test_two_worktrees_share_machine_and_benchmark_but_not_worktree_prefixes(self):
        first = self.bind()
        other_entry = self.entry.parent / 'other-worktree'
        other_config = copy.deepcopy(self.config)
        other_config.update(env_id='other-worktree', workspace_root=str(self.root / 'other'))
        other_config['warehouses'] = {name: value.replace('test-worktree', 'other-worktree') for name, value in self.config['warehouses'].items()}
        second = entry.attach_entry(self.owner, 'other-worktree', other_entry, other_config, bom())
        a, b = (self.environment(Path(p['published_dir']) / 'env.sh') for p in (first, second))
        self.assertEqual(a['NOVA_FIXTURE_MACHINE_ID'], b['NOVA_FIXTURE_MACHINE_ID'])
        self.assertEqual(a['AWS_S3_ENDPOINT'], b['AWS_S3_ENDPOINT'])
        self.assertEqual(a['NOVA_ENV_SHARED_BENCHMARK_ROOT'], b['NOVA_ENV_SHARED_BENCHMARK_ROOT'])
        self.assertNotEqual(a['CATALOG_WAREHOUSE_URI'], b['CATALOG_WAREHOUSE_URI'])
        self.assertEqual(self.backend.events, ['ensure'])

    def test_declaration_checkouts_share_consumer_entry_lock_and_local_ports(self):
        fakebin = self.root / 'bin'; fakebin.mkdir()
        marker = self.root / 'unexpected-docker'
        docker = fakebin / 'docker'; docker.write_text('#!/bin/sh\ntouch ' + str(marker) + '\nexit 99\n'); docker.chmod(0o755)
        results = []
        for label in ('definition-a', 'definition-b'):
            source = self.root / label / 'docker/iceberg-rest'; source.mkdir(parents=True)
            for name in ('up.sh', 'fixture_runtime.py', 'machine_runtime.py', 'oss_ops.py', 'entry_publication.py', 'runtime_entry.py', 'shared.env'):
                shutil.copy2(HERE / name, source / name)
            environment = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
                'NOVAROCKS_WORKSPACE_ROOT': str(self.workspace), 'NOVA_ENV_CONFIG_FILE': str(source / 'shared.env')}
            process = subprocess.run(['bash', str(source / 'up.sh'), '--prepare-only'], env=environment, capture_output=True, text=True)
            self.assertEqual(process.returncode, 0, process.stderr)
            result = json.loads(process.stdout)
            actual_entry = Path(result['published_dir']).parent.parent
            saved = publication.read(actual_entry)
            results.append((actual_entry, saved['config'], (actual_entry / '.entry.lock').stat().st_ino))
            self.assertFalse((source / 'runtime').exists())
        self.assertEqual(results[0][0], results[1][0])
        self.assertEqual(results[0][1]['local_ports'], results[1][1]['local_ports'])
        self.assertEqual(results[0][1]['native_trust'], results[1][1]['native_trust'])
        self.assertEqual(results[0][2], results[1][2])
        self.assertFalse(marker.exists())

    def test_real_shell_prepare_and_status_are_offline_and_clear_ambient_endpoints(self):
        fakebin = self.root / 'bin'; fakebin.mkdir()
        marker = self.root / 'docker-was-called'
        docker = fakebin / 'docker'; docker.write_text('#!/bin/sh\ntouch ' + str(marker) + '\nexit 99\n'); docker.chmod(0o755)
        environment = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
            'NOVAROCKS_WORKSPACE_ROOT': str(self.workspace), 'NOVA_ENV_CONFIG_FILE': str(HERE / 'shared.env')}
        result = subprocess.run(['bash', str(HERE / 'up.sh'), '--prepare-only'], env=environment, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        env = self.environment(Path(value['published_dir']) / 'env.sh', NOVA_ENV_REST_PORT='9999')
        self.assertNotIn('NOVA_ENV_REST_PORT', env)
        self.assertFalse(value['ready'])
        self.assertNotIn('config', value)  # CLI output does not leak credential material.
        status = subprocess.run(['bash', str(HERE / 'status.sh')], env=environment, capture_output=True, text=True)
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertEqual(json.loads(status.stdout)['published_dir'], value['published_dir'])
        self.assertFalse(marker.exists())


class HelperEntryTests(RendererCase):
    def sweep_python(self, fakebin, log, *, fail=False):
        shim = fakebin / "python3"
        shim.write_text("#!" + sys.executable + "\n" +
            "import json,os,sys\nfrom pathlib import Path\n" +
            "if len(sys.argv)>2 and sys.argv[1:3]==[" + repr(str(HERE / "fixture_run.py")) + ", 'sweep-start']:\n" +
            " with Path(" + repr(str(log)) + ").open('a') as out: out.write(json.dumps({'kind':'sweep','args':sys.argv[2:],'stdin':'','foreign':os.environ.get('REST_IMAGE'),'context':json.loads(os.environ['NOVA_FIXTURE_RUN_CONTEXT'])})+'\\n')\n" +
            " print(json.dumps({'complete':" + repr(not fail) + ",'pending':False,'errors':" +
            repr([{'code':'ScratchCleanupFailed'}] if fail else []) + ",'removed':[]}))\n" +
            " sys.exit(" + ("75" if fail else "0") + ")\n" +
            "os.execv(" + repr(sys.executable) + ", [" + repr(sys.executable) + ", *sys.argv[1:]])\n")
        shim.chmod(0o755)

    def smoke_docker(self, first, mode="success"):
        log = self.root / "smoke-docker.calls"
        fakebin = self.root / "smoke-bin"; fakebin.mkdir(exist_ok=True)
        state = self.root / "smoke-worker-status"
        docker = fakebin / "docker"
        docker.write_text("#!/usr/bin/env python3\n" + "import json,sys\nfrom pathlib import Path\n" +
            "log=Path(" + repr(str(log)) + "); state=Path(" + repr(str(state)) + "); mode=" + repr(mode) + "\n" +
            "args=sys.argv[1:]; data=sys.stdin.buffer.read().decode('utf-8')\n" +
            "with log.open('a') as out: out.write(json.dumps({'args':args,'stdin':data})+'\\n')\n" +
            "if args[-3:]==['ps','-q','spark']: print(" + repr(first["machine"]["containers"]["spark"]) + "); sys.exit(0)\n" +
            "if any('value = json.loads' in a for a in args): print(json.dumps({'returncode':int(state.read_text()),'child_waited':True})); sys.exit(0)\n" +
            "if any('start_new_session=True' in a for a in args):\n" +
            " code=17 if mode=='sql-failure' and any('spark_sql_bin=' in a for a in args) else (23 if mode=='cleanup-failure' and any('shutil.rmtree(folder)' in a for a in args) else 0)\n" +
            " state.write_text(str(code)); sys.exit(code)\n")
        docker.chmod(0o755)
        self.sweep_python(fakebin, log, fail=mode == "sweep-failure")
        return log, {**os.environ, "PATH":str(fakebin)+":"+os.environ["PATH"],
                     "NOVA_ENV_REST_ENV_FILE":str(Path(first["published_dir"])/"env.sh")}

    def test_generated_shared_smoke_uses_native_layout_and_releases_catalog_names_without_purge(self):
        import fixture_run
        first = self.bind()
        publication_dir = Path(first["published_dir"])
        template = (publication_dir/"spark-iceberg-v3-smoke.sql").read_text()
        self.assertTrue(template.startswith("-- novarocks-generated-shared-spark-v3-smoke-v1\n"))
        self.assertNotIn("nr_v3",template)
        contexts=[]
        for _ in range(2):
            with mock.patch.dict(os.environ,{},clear=True): contexts.append(fixture_run.admit(publication_dir/"env.sh")[2].as_dict())
        log,environment = self.smoke_docker(first)
        for context in contexts:
            result = subprocess.run(["bash",str(HERE/"spark-sql.sh")],env={**environment,"NOVA_FIXTURE_RUN_CONTEXT":json.dumps(context)},input="",capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
        calls=[json.loads(line) for line in log.read_text().splitlines()]
        sweeps=[call for call in calls if call.get("kind") == "sweep"]
        self.assertEqual([call["context"] for call in sweeps], contexts)
        self.assertTrue(all(call["args"] == ["sweep-start", "--env-file", str(publication_dir / "env.sh")] for call in sweeps))
        for index, call in enumerate(calls):
            if call.get("kind") == "sweep":
                self.assertLess(index, next(i for i in range(index + 1, len(calls))
                    if any("start_new_session=True" in argument for argument in calls[i]["args"])))
        queries=[call["stdin"] for call in calls if call["stdin"].startswith("-- novarocks-generated-shared-spark-v3-smoke-v1")]
        self.assertEqual(len(queries),2)
        for text,context in zip(queries,contexts):
            namespace=context["identity"]["run_id"]+"_smoke"
            self.assertEqual(len(namespace),63)
            self.assertIn("CREATE NAMESPACE ice_rest."+namespace+";",text)
            self.assertNotIn("LOCATION",text)
            self.assertNotIn("PURGE",text)
            self.assertIn("DROP TABLE IF EXISTS ice_rest."+namespace+".spark_v3_smoke;",text)
            self.assertTrue(text.endswith("DROP NAMESPACE IF EXISTS ice_rest."+namespace+";\n"))
            self.assertIn("(3, 'spark-v3-c', 'alpha', TIMESTAMP '2026-05-07 00:02:00')",text)
            self.assertIn("SELECT * FROM ice_rest."+namespace+".spark_v3_smoke ORDER BY id;",text)
            self.assertNotIn("__NOVA_SMOKE",text)
        self.assertNotIn(contexts[0]["identity"]["run_id"],queries[1])
        self.assertEqual((publication_dir/"spark-iceberg-v3-smoke.sql").read_text(),template)
        for context in contexts: self.assertNotIn(context["identity"]["run_id"],(publication_dir/"env.sh").read_text())

    def test_ordinary_smoke_named_sql_is_copied_without_token_or_namespace_rewrite(self):
        first=self.bind(); log,environment=self.smoke_docker(first)
        source=self.root/"spark-iceberg-v3-smoke.sql"
        text=b"-- novarocks-generated-shared-spark-v3-smoke-v1\r\nSELECT '__NOVA_SMOKE_NAMESPACE__', 'ice_rest.nr_v3';\r\n"
        source.write_bytes(text)
        result=subprocess.run(["bash",str(HERE/"spark-sql.sh"),str(source)],env=environment,input="",capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn(text.decode(),[json.loads(line)["stdin"] for line in log.read_text().splitlines()])

    def test_shared_publication_without_scoped_marker_is_rejected_before_docker(self):
        first=self.bind(); log,environment=self.smoke_docker(first)
        generated=Path(first["published_dir"])/"spark-iceberg-v3-smoke.sql"
        generated.write_text("CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_v3;\n")
        result=subprocess.run(["bash",str(HERE/"spark-sql.sh")],env=environment,input="",capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn("shared generated smoke has no scoped template",result.stderr)
        self.assertFalse(log.exists())

    def test_failed_shared_smoke_retains_diagnostics_and_does_not_claim_remote_cleanup(self):
        first=self.bind(); log,environment=self.smoke_docker(first,"sql-failure")
        result=subprocess.run(["bash",str(HERE/"spark-sql.sh")],env=environment,input="",capture_output=True,text=True)
        self.assertEqual(result.returncode,17,result.stderr)
        self.assertIn("remote_cleanup=NOT_CONFIRMED",result.stderr)
        self.assertIn("diagnostics=/tmp/novarocks-fixture-data-nr_scratch_",result.stderr)
        calls=[json.loads(line) for line in log.read_text().splitlines()]
        self.assertFalse(any("shutil.rmtree(folder)" in arg for call in calls for arg in call["args"]))

    def test_successful_shared_smoke_cannot_hide_local_cleanup_failure(self):
        first=self.bind(); _,environment=self.smoke_docker(first,"cleanup-failure")
        result=subprocess.run(["bash",str(HERE/"spark-sql.sh")],env=environment,input="",capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn("ContainerCleanupFailed",result.stderr)

    def test_startup_sweep_failure_blocks_both_helpers_before_any_writer(self):
        import fixture_run
        first = self.bind()
        publication_dir = Path(first["published_dir"])
        with mock.patch.dict(os.environ, {}, clear=True):
            context = fixture_run.admit(publication_dir / "env.sh")[2].as_dict()
        log, environment = self.smoke_docker(first, "sweep-failure")
        for helper, suffix in (("spark-sql.sh", "sql"), ("spark-shell.sh", "scala")):
            with self.subTest(helper=helper):
                log.unlink(missing_ok=True)
                source = self.root / ("query." + suffix)
                source.write_text("SELECT 1;\n" if suffix == "sql" else "println(1)\n")
                result = subprocess.run(["bash", str(HERE / helper), str(source)],
                    env={**environment, "NOVA_FIXTURE_RUN_CONTEXT":json.dumps(context)},
                    input="", capture_output=True, text=True)
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertIn("ScratchCleanupFailed", result.stderr)
                calls = [json.loads(line) for line in log.read_text().splitlines()]
                self.assertEqual(len(calls), 1, "failed sweep must block Docker and temporary writer setup")
                self.assertEqual(calls[0]["kind"], "sweep")
                self.assertEqual(calls[0]["context"], context)
                self.assertEqual(calls[0]["args"], ["sweep-start", "--env-file", str(publication_dir / "env.sh")])

    def test_helper_uses_exact_published_compose_and_discards_ambient_overrides(self):
        import fixture_run
        first = self.bind()
        log = self.root / 'docker.calls'
        fakebin = self.root / 'bin'; fakebin.mkdir()
        docker = fakebin / 'docker'
        docker.write_text('#!/usr/bin/env python3\nimport json,os,sys\nfrom pathlib import Path\nwith Path(' + repr(str(log)) + ').open("a") as f: f.write(json.dumps({"args":sys.argv[1:],"foreign":os.environ.get("REST_IMAGE")})+"\\n")\n'
            'if sys.argv[-3:]==["ps","-q","spark"]: print(' + repr(first['machine']['containers']['spark']) + ')\n'
            'if "python3" in sys.argv and any("value = json.loads" in value for value in sys.argv):\n'
            ' print(json.dumps({"returncode":0,"child_waited":True}))\n')
        docker.chmod(0o755)
        self.sweep_python(fakebin, log)
        sql = self.root / 'query.sql'; sql.write_text('SELECT 1;\n')
        scala = self.root / 'query.scala'; scala.write_text('println(1)\n')
        environment = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
            'NOVA_ENV_REST_ENV_FILE': str(Path(first['published_dir']) / 'env.sh'), 'REST_IMAGE': 'foreign'}
        with mock.patch.dict(os.environ, {}, clear=True):
            context = fixture_run.admit(Path(first['published_dir']) / 'env.sh')[2].as_dict()
        environment['NOVA_FIXTURE_RUN_CONTEXT'] = json.dumps(context)
        for helper, source in [('spark-sql.sh', sql), ('spark-shell.sh', scala)]:
            result = subprocess.run(['bash', str(HERE / helper), str(source)], env=environment, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in log.read_text().splitlines()]
        self.assertTrue(calls)
        sweeps = [call for call in calls if call.get('kind') == 'sweep']
        self.assertEqual(len(sweeps), 2)
        self.assertTrue(all(call['context'] == context for call in sweeps))
        self.assertTrue(all(call['args'] == ['sweep-start', '--env-file', str(Path(first['published_dir']) / 'env.sh')] for call in sweeps))
        self.assertEqual(calls[0]['kind'], 'sweep')
        for index, call in enumerate(calls):
            if call.get('kind') == 'sweep':
                self.assertLess(index, next(i for i in range(index + 1, len(calls))
                    if any('start_new_session=True' in argument for argument in calls[i]['args'])))
        workers = [call for call in calls if any('start_new_session=True' in argument for argument in call['args'])]
        settlements = [call for call in calls if any('value = json.loads' in argument for argument in call['args'])]
        self.assertTrue(workers)
        self.assertEqual(len(workers), len(settlements))
        self.assertTrue(any('spark-sql' in ' '.join(call['args']) for call in workers))
        self.assertTrue(any('spark-shell' in ' '.join(call['args']) for call in workers))
        lookups = [call for call in calls if call['args'][:1] == ['compose']]
        self.assertEqual(len(lookups), len(workers))
        for call in lookups:
            self.assertIn(first['machine']['compose_file'], call['args'])
            self.assertIn(first['machine']['compose_env'], call['args'])
            self.assertEqual(call['args'][-3:], ['ps', '-q', 'spark'])
        for call in calls:
            if call['args'][:1] == ['exec']:
                self.assertIn(first['machine']['containers']['spark'], call['args'])
            if call.get('kind') == 'sweep':
                self.assertEqual(call['foreign'], 'foreign')
            else:
                self.assertIsNone(call['foreign'])
        entry.release_entry('test-worktree', self.entry)
        environment['NOVA_ENV_REST_ENV_FILE'] = str(self.entry / 'env.sh')
        result = subprocess.run(['bash', str(HERE / 'spark-sql.sh'), str(sql)], env=environment, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('fixture is not ready', result.stderr)


class DownPurgeTests(RendererCase):
    def test_release_keeps_sqlite_and_does_not_touch_shared_machine_or_s3(self):
        first = self.bind()
        lock = self.entry / '.entry.lock'; inode = lock.stat().st_ino
        state = self.entry / 'frontend-state.sqlite'; state.write_text('stable')
        before = copy.deepcopy(self.backend.events)
        result = entry.release_entry('test-worktree', self.entry, purge=True)
        self.assertFalse(result['ready'])
        self.assertEqual(state.read_text(), 'stable')
        self.assertEqual(lock.stat().st_ino, inode)
        self.assertEqual(self.backend.events, before)
        self.assertTrue(result['retained_publications'])
        self.assertTrue((Path(first['published_dir']) / 'env.sh').is_file())


class IsolatedEntryTests(RendererCase):
    def isolated(self, profile='stock'):
        config = copy.deepcopy(self.config)
        config.update(shared_docker=False, update_current=False)
        inputs = bom(); inputs['derived_images']['iceberg-spark']['image_id'] = 'sha256:spark'
        return entry.render_isolated_stack(inputs, 'nr-isolated-rest-test', {'minio': 19101, 'minio_console': 20101, 'rest': 21101, 'rest_mv': 24101, 'spark': 22101, 'control': 23101}, self.entry, config,
            profile=profile, hook_image='publication:exact' if profile != 'stock' else None)

    def test_stock_and_hook_are_initial_models_from_same_templates(self):
        sentinel = self.entry.parent / 'current'; sentinel.parent.mkdir(parents=True)
        sentinel.symlink_to('existing-worktree')
        result = self.isolated()
        entry.render_entry(result['context'], self.entry)
        manifest = json.loads((self.entry / 'manifest.json').read_text())
        self.assertFalse(manifest['shared_docker'])
        private_smoke=(self.entry/'spark-iceberg-v3-smoke.sql').read_text()
        self.assertTrue(private_smoke.startswith('CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_v3;'))
        self.assertNotIn('__NOVA_SMOKE',private_smoke)
        self.assertNotIn('DROP NAMESPACE',private_smoke)
        self.assertEqual(manifest['runtime']['profile'], 'stock')
        self.assertEqual(manifest['compose_file'], str(self.entry / 'compose.yml'))
        self.assertEqual(manifest['runtime']['template_model_sha256'], entry.model_hash(HERE.parent.parent))
        self.assertEqual(manifest['minio']['access_key_id'], self.config['credentials']['access_key'])
        self.assertIsNone(manifest['runtime']['control_uri'])
        model = (self.entry / 'compose.yml').read_text()
        for service in ('minio', 'mc-init', 'mc', 'rest', 'rest-mv', 'spark'):
            self.assertIn('  ' + service + ':\n', model)
        self.assertIn('mc mb --ignore-existing store/warehouse', model)
        self.assertIn('rest-catalog:/tmp', model)
        self.assertIn('rest-mv-catalog:/tmp', model)
        record = result['record']
        self.assertEqual(record['images']['rest-mv']['image_id'], 'sha256:rest-mv')
        self.assertEqual(record['images']['minio']['image_id'], 'sha256:minio')
        self.assertEqual(record['images']['mc']['image_id'], 'sha256:minio-mc')
        self.assertIn("MINIO_IMAGE='sha256:minio'", (self.entry / 'compose.env').read_text())
        self.assertIn("MC_IMAGE='sha256:minio-mc'", (self.entry / 'compose.env').read_text())
        self.assertIn('rest-mv', record['required_services'])
        self.assertIn('nr-isolated-rest-test_rest-mv-catalog', record['volumes'])
        self.assertEqual(record['service_ports']['rest-mv'], {'8181/tcp': 24101})
        self.assertIn('http://127.0.0.1:24101/v1/config', record['health_urls'])
        self.assertEqual(manifest['iceberg_rest_mv'], {'uri': 'http://127.0.0.1:24101',
            'warehouse': 's3://warehouse/test-worktree/rest-mv', 'server_default_warehouse': 's3://warehouse/test-worktree/rest-mv/'})
        self.assertIn("NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI='s3://warehouse/test-worktree/rest-mv'", (self.entry / 'compose.env').read_text())
        self.assertEqual(os.readlink(sentinel), 'existing-worktree')
        hook = self.isolated('publication-hook')
        entry.render_entry(hook['context'], self.entry)
        manifest = json.loads((self.entry / 'manifest.json').read_text())
        self.assertEqual(manifest['runtime']['profile'], 'publication-hook')
        self.assertEqual(manifest['runtime']['control_uri'], 'http://127.0.0.1:23101')
        self.assertIn('8182', (self.entry / 'compose.yml').read_text())
        self.assertIn('TracingFileIO', (self.entry / 'compose.yml').read_text())
        self.assertIn('REST_IMAGE=\'publication:exact\'', (self.entry / 'compose.env').read_text())
        self.assertEqual(hook['record']['images']['rest-mv'], record['images']['rest-mv'])
        self.assertEqual(entry.blocks(entry.sections(model)['services'])['rest-mv'],
                         entry.blocks(entry.sections((self.entry / 'compose.yml').read_text())['services'])['rest-mv'])
        self.assertEqual(hook['record']['service_ports']['rest-mv'], record['service_ports']['rest-mv'])
        self.assertEqual(os.readlink(sentinel), 'existing-worktree')

    def test_prepare_and_teardown_shells_leave_current_untouched_without_docker(self):
        import shutil
        copied = self.root / 'checkout'
        fixture = copied / 'docker/iceberg-rest'
        fixture.mkdir(parents=True)
        for name in ['up.sh', 'down.sh', 'fixture_runtime.py', 'machine_runtime.py', 'oss_ops.py', 'entry_publication.py', 'runtime_entry.py', 'shared.env']:
            shutil.copy2(HERE / name, fixture / name)
        for name in ['templates', 'spark']:
            shutil.copytree(HERE / name, fixture / name)
        workspace = self.root / 'isolated-rest-request'
        workspace.mkdir()
        config = workspace / 'isolated.env'
        project = 'nr-isolated-rest-request'
        config.write_text('NOVA_ENV_SHARED_DOCKER=false\nNOVA_ENV_COMPOSE_PROJECT=' + project + '\nMINIO_ROOT_USER=isolated\nMINIO_ROOT_PASSWORD=isolated-secret\n')
        current = fixture / 'runtime/current'
        current.parent.mkdir(); current.symlink_to('untouched-worktree')
        fakebin = self.root / 'bin'; fakebin.mkdir()
        docker = fakebin / 'docker'; docker.write_text('#!/bin/sh\nexit 99\n'); docker.chmod(0o755)
        env = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
               'NOVAROCKS_WORKSPACE_ROOT': str(workspace), 'NOVA_ENV_CONFIG_FILE': str(config),
               'NOVA_ENV_SHARED_DOCKER': 'false', 'NOVA_ENV_COMPOSE_PROJECT': project,
               'NOVA_ENV_UPDATE_CURRENT': 'false', 'NOVA_ENV_ALLOW_VOLUME_DELETE': 'true',
               'NOVA_ENV_EXPECTED_COMPOSE_PROJECT': project, 'NOVA_ENV_EXPECTED_MINIO_VOLUME': project + '_minio-data'}
        prepared = subprocess.run(['bash', str(fixture / 'up.sh'), '--prepare-only'], env=env, capture_output=True, text=True)
        self.assertEqual(prepared.returncode, 0, prepared.stderr)
        generated = Path(json.loads(prepared.stdout)['published_dir'])
        self.assertEqual(generated.parent, fixture / 'runtime')
        self.assertFalse((workspace / 'docker' / 'iceberg-rest' / 'runtime').exists())
        self.assertEqual(os.readlink(current), 'untouched-worktree')
        env['NOVA_ENV_ID'] = generated.name
        shutil.rmtree(workspace)
        for _ in range(2):
            down = subprocess.run(['bash', str(fixture / 'down.sh'), '--docker', '--purge'], env=env, capture_output=True, text=True)
            self.assertEqual(down.returncode, 0, down.stderr)
        self.assertFalse(generated.exists())
        self.assertEqual(os.readlink(current), 'untouched-worktree')

    def test_hook_requires_explicit_nonzero_control_port(self):
        config = {**self.config, 'shared_docker': False}
        with self.assertRaises(runtime.RuntimeFailure):
            entry.render_isolated_stack(bom(), 'nr-isolated-rest-test', {'control': 0}, self.entry, config, profile='publication-hook', hook_image='exact')

    def test_isolated_teardown_is_exact_and_survives_missing_workspace(self):
        result = self.isolated()
        entry.render_entry(result['context'], self.entry)
        calls = []
        backend = mock.Mock()
        backend.compose.side_effect = lambda record, args: calls.append((record['project'], args))
        with mock.patch.object(runtime, 'Docker', return_value=backend):
            with self.assertRaises(runtime.RuntimeFailure):
                entry.isolated_down(self.entry, 'nr-isolated-rest-test', volumes=True, purge=True)
            self.assertTrue(self.entry.exists())
            with mock.patch.dict(os.environ, {'NOVA_ENV_ALLOW_VOLUME_DELETE': 'true', 'NOVA_ENV_EXPECTED_COMPOSE_PROJECT': 'nr-isolated-rest-test', 'NOVA_ENV_EXPECTED_MINIO_VOLUME': 'nr-isolated-rest-test_minio-data'}):
                self.workspace.rmdir()
                entry.isolated_down(self.entry, 'nr-isolated-rest-test', volumes=True, purge=True)
                entry.isolated_down(self.entry, 'nr-isolated-rest-test', volumes=True, purge=True)
        self.assertEqual(calls, [('nr-isolated-rest-test', ['down', '-v'])])
        self.assertFalse(self.entry.exists())




class ConsumerInputVerificationTests(unittest.TestCase):
    def test_runtime_consumes_exact_verified_snapshot_without_rereading_bom(self):
        snapshot = {'schema': 2, 'platform': 'linux/arm64', 'lock_sha256': 'verified-lock'}
        config = {'repo_root': '/fixture-repository', 'fixture_inputs': {'bom': '/fixture-store/bom.json', 'verified': False}}
        with mock.patch.object(machine, 'verified_bom', return_value=snapshot) as verify, mock.patch.object(runtime, 'read_json', side_effect=AssertionError('reread mutable BOM')):
            actual = entry.verify_inputs(config)
        self.assertIs(actual, snapshot)
        verify.assert_called_once_with(Path('/fixture-repository'), Path('/fixture-store'), consumer='iceberg-rest')
        self.assertEqual(config['fixture_inputs']['consumer'], 'iceberg-rest')
        self.assertEqual(config['fixture_inputs']['lock_sha256'], 'verified-lock')
        self.assertTrue(config['fixture_inputs']['verified'])

    def test_failed_input_verification_never_marks_publication_verified(self):
        config = {'repo_root': '/fixture-repository', 'fixture_inputs': {'bom': '/fixture-store/bom.json', 'verified': False}}
        with mock.patch.object(machine, 'verified_bom', side_effect=runtime.RuntimeFailure('FixturePrerequisiteMissing', 'absent')), self.assertRaises(runtime.RuntimeFailure):
            entry.verify_inputs(config)
        self.assertFalse(config['fixture_inputs']['verified'])


if __name__ == '__main__':
    unittest.main()
