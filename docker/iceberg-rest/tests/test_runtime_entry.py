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

"""Entry formats, shell boundaries and isolated definitions without a Docker daemon."""
import contextlib
import copy
import importlib.util
import io
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))
import fixture_runtime as runtime
import runtime_entry as entry
from test_fixture_runtime import Backend, bom


class RendererCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.workspace = self.root / 'workspace'
        self.workspace.mkdir()
        self.entry = self.root / 'runtime' / 'test-worktree'
        self.backend = Backend(self.root / 'backend')
        base_ensure = self.backend.ensure
        def ensure(record, parent=None):
            base_ensure(record, parent)
            record['containers'] = {name: record['id'] + '-' + name for name in record['required_services']}
        self.backend.ensure = ensure
        self.owner = runtime.RuntimeOwner(self.root / 'owner', daemon='test-daemon', backend=self.backend,
            renderer=entry.render_entry, hook=lambda *a, **k: None, port_start=38000, port_end=38999)
        self.config = entry.request_config(self.workspace, entry.load_settings(HERE / 'shared.env'), HERE / 'shared.env', self.entry, shared=True, update_current=False)
        self.config['fixture_inputs'] = {'bom': str(self.root / 'bom.json'), 'lock_sha256': 'lock', 'verified': True}

    def bind(self, inputs=None):
        return self.owner.bind('test-worktree', self.entry, self.config, inputs or bom())

    def environment(self, path, **extra):
        script = 'source "$1"; python3 -c "import os,json; print(json.dumps(dict(os.environ)))"'
        output = subprocess.check_output(['bash', '-c', script, '_', str(path)], env={**os.environ, **extra}, text=True)
        return json.loads(output)


class RendererTests(RendererCase):
    def test_complete_publication_uses_final_paths_and_stable_database(self):
        first = self.bind()
        final = Path(first['published_dir'])
        self.assertTrue(all((final / name).is_file() for name in runtime.ENTRY_FILES))
        env = self.environment(self.entry / 'env.sh')
        manifest = json.loads((final / 'manifest.json').read_text())
        for key, file in [('NOVAROCKS_FE_CONFIG', 'fe.toml'), ('NOVAROCKS_BE_CONFIG', 'be.toml'), ('NOVAROCKS_SQL_TEST_CONFIG', 'sql-test.toml'), ('NOVAROCKS_SPARK_DEFAULTS', 'spark-defaults.conf')]:
            self.assertEqual(env[key], str(final / file))
        self.assertEqual(env['NOVA_ENV_REST_ENV_FILE'], str(final / 'env.sh'))
        self.assertEqual(env['NOVA_ENV_RUNTIME_DIR'], str(self.entry))
        self.assertEqual(env['NOVAROCKS_STATE_STORE_PATH'], str(self.entry / 'frontend-state.sqlite'))
        fe = tomllib.loads((final / 'fe.toml').read_text())
        be = tomllib.loads((final / 'be.toml').read_text())
        self.assertEqual(fe['state_store']['path'], manifest['novarocks']['state_store_path'])
        self.assertEqual(fe['native_trust'], be['native_trust'])
        self.assertEqual(fe['native_trust']['shared_secret'], '${ENV:NOVAROCKS_NATIVE_SHARED_SECRET}')
        second = self.bind(bom('another'))
        env2 = self.environment(Path(second['published_dir']) / 'env.sh')
        self.assertEqual(env2['NOVAROCKS_STATE_STORE_PATH'], env['NOVAROCKS_STATE_STORE_PATH'])
        self.assertNotEqual(env2['NOVAROCKS_FE_CONFIG'], env['NOVAROCKS_FE_CONFIG'])

    def test_warehouse_credential_and_runner_facts_are_consistent(self):
        self.config['credentials'] = {'access_key': "access'key", 'secret_key': 'secret$special'}
        result = self.bind()
        final = Path(result['published_dir'])
        env = self.environment(final / 'env.sh')
        manifest = json.loads((final / 'manifest.json').read_text())
        runner = tomllib.loads((final / 'sql-test.toml').read_text())
        self.assertEqual(env['AWS_S3_ACCESS_KEY_ID'], "access'key")
        self.assertEqual(env['AWS_S3_SECRET_ACCESS_KEY'], 'secret$special')
        self.assertEqual(env['NOVA_ENV_REST_WAREHOUSE_URI'], manifest['iceberg_rest']['warehouse'])
        self.assertEqual(env['NOVA_ENV_REST_SERVER_WAREHOUSE_URI'], manifest['iceberg_rest']['server_default_warehouse'])
        self.assertNotEqual(env['NOVA_ENV_REST_WAREHOUSE_URI'], env['NOVA_ENV_REST_SERVER_WAREHOUSE_URI'])
        self.assertEqual(runner['env']['fixture_env_file'], env['NOVA_ENV_REST_ENV_FILE'])
        self.assertEqual(runner['env']['iceberg_rest_uri'], manifest['iceberg_rest']['uri'])
        self.assertEqual(runner['env']['oss_endpoint'], manifest['minio']['endpoint'])
        self.assertEqual(runner['env']['oss_sk'], manifest['minio']['secret_access_key'])
        sql = (final / 'ice-rest-catalog.sql').read_text()
        properties = dict(re.findall(r"'([^']+)'\s*=\s*'([^']*)'", sql))
        for purpose, consumer, role in (('metadata', 'frontend', 'fe'), ('data', 'backend', 'be')):
            role_config = tomllib.loads((final / (role + '.toml')).read_text())
            credential = next(item for item in role_config['connector']['credentials']
                              if item['purpose'] == 'object-store-' + purpose)
            prefix = 'credential.object-store-' + purpose + '.'
            self.assertEqual(properties[prefix + 'consumer-role'], consumer)
            self.assertEqual(properties[prefix + 'mode'], 'static')
            self.assertEqual(properties[prefix + 'name'], credential['name'])
            self.assertEqual(properties[prefix + 'generation'], credential['generation'])
            self.assertEqual(credential['access_key_id'], '${ENV:AWS_S3_ACCESS_KEY_ID}')
            self.assertEqual(credential['access_key_secret'], '${ENV:AWS_S3_SECRET_ACCESS_KEY}')
        for retired in ('aws.s3.access_key', 'aws.s3.secret_key'):
            self.assertNotIn(retired, properties)
        self.assertNotIn(env['AWS_S3_ACCESS_KEY_ID'], sql)
        self.assertNotIn(env['AWS_S3_SECRET_ACCESS_KEY'], sql)
        self.assertIn('http://rest:8181', (final / 'spark-defaults.conf').read_text())
        self.assertNotIn('http://rest:8181', (final / 'ice-rest-catalog.sql').read_text())
        with (final / 'sql-test.toml').open('a') as stream:
            stream.write('paimon_catalog_warehouse = "s3://novarocks/paimon"\n')
        self.assertIn('paimon_catalog_warehouse', tomllib.loads((final / 'sql-test.toml').read_text())['env'])
        next_pub = Path(self.bind()['published_dir'])
        self.assertNotIn('paimon_catalog_warehouse', (next_pub / 'sql-test.toml').read_text())

    def test_prepare_is_offline_for_every_record_state(self):
        self.backend.daemon_id = lambda: self.fail('prepare queried Docker')
        prepared = self.owner.prepare_entry('test-worktree', self.entry, self.config)
        self.assertFalse(prepared['ready'])
        self.assertEqual(self.backend.calls, [])
        self.backend.daemon_id = lambda: 'test-daemon'
        first = self.bind()
        self.backend.daemon_id = lambda: self.fail('prepare queried Docker')
        self.backend.healthy = lambda *a: self.fail('prepare checked health')
        for kind in ('object_store', 'catalog'):
            original = first['records'][kind]
            for state in ('starting', 'deleting', 'missing'):
                with self.subTest(kind=kind, state=state):
                    # Restore the complete original publication and records before each observation.
                    for record in first['records'].values():
                        self.owner.save_record(copy.deepcopy(record))
                    self.owner.publish(self.entry, first)
                    path = self.owner.record_path(original['id'])
                    if state == 'missing':
                        path.unlink()
                    else:
                        changed = copy.deepcopy(original); changed['state'] = state
                        self.owner.save_record(changed)
                    result = self.owner.prepare_entry('test-worktree', self.entry, self.config)
                    self.assertFalse(result['ready'])
                    self.assertEqual(result['data_locations'], first['data_locations'])
                    env = self.environment(self.entry / 'env.sh', AWS_S3_ENDPOINT='http://foreign:9000', NOVAROCKS_ICEBERG_REST_URI='http://foreign:8181')
                    self.assertNotIn('AWS_S3_ENDPOINT', env)
                    self.assertNotIn('NOVAROCKS_ICEBERG_REST_URI', env)
        for record in first['records'].values():
            self.owner.save_record(copy.deepcopy(record))
        self.owner.publish(self.entry, first)
        self.assertTrue(self.owner.prepare_entry('test-worktree', self.entry, self.config)['ready'])

    def test_real_renderer_failure_preserves_previous_publication(self):
        first = self.bind()
        invalid = copy.deepcopy(self.config); del invalid['credentials']
        # The renderer requires role-local port values; inject a format failure after ensure.
        invalid = copy.deepcopy(self.config); del invalid['local_ports']['mysql']
        with self.assertRaises(KeyError):
            self.owner.bind('test-worktree', self.entry, invalid, bom())
        self.assertEqual(runtime.current_publication(self.entry), first)
        self.assertEqual(self.environment(self.entry / 'env.sh')['NOVA_ENV_REST_ENV_FILE'], str(Path(first['published_dir']) / 'env.sh'))


class SharedEnvironmentTests(RendererCase):
    def test_two_worktrees_share_instances_and_benchmark_but_not_private_paths(self):
        first = self.bind()
        other_entry = self.root / 'runtime' / 'other-worktree'
        other_config = copy.deepcopy(self.config)
        other_config.update(env_id='other-worktree', workspace_root=str(self.root / 'other'))
        other_config['warehouses'] = {'catalog': 's3://novarocks/other-worktree/catalog', 'test': 's3://novarocks/other-worktree/test', 'rest_client': 's3://warehouse/other-worktree/rest'}
        second = self.owner.bind('other-worktree', other_entry, other_config, bom())
        self.assertEqual(first['binding'], second['binding'])
        self.assertEqual(len([x for x in self.backend.calls if x[0] == 'ensure']), 2)
        a, b = (self.environment(Path(value['published_dir']) / 'env.sh') for value in [first, second])
        self.assertEqual(a['NOVA_ENV_SHARED_BENCHMARK_ROOT'], b['NOVA_ENV_SHARED_BENCHMARK_ROOT'])
        self.assertEqual(a['AWS_S3_ENDPOINT'], b['AWS_S3_ENDPOINT'])
        self.assertNotEqual(a['NOVA_ENV_REST_WAREHOUSE_URI'], b['NOVA_ENV_REST_WAREHOUSE_URI'])

    def test_two_declaration_checkouts_keep_one_worktree_entry_and_lock(self):
        import shutil
        workspace = self.root / 'consumer-worktree'
        workspace.mkdir()
        sources = []
        for generation in ('source-r1', 'source-r2'):
            fixture = self.root / generation / 'docker' / 'iceberg-rest'
            fixture.mkdir(parents=True)
            for name in ('up.sh', 'status.sh', 'fixture_runtime.py', 'runtime_entry.py', 'shared.env'):
                shutil.copy2(HERE / name, fixture / name)
            for name in ('templates', 'spark'):
                shutil.copytree(HERE / name, fixture / name)
            sources.append(fixture)
        fakebin = self.root / 'bin'
        fakebin.mkdir()
        marker = self.root / 'unexpected-docker'
        fake_docker = fakebin / 'docker'
        fake_docker.write_text('#!/bin/sh\ntouch "' + str(marker) + '"\nexit 99\n')
        fake_docker.chmod(0o755)
        environment = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'],
                       'NOVAROCKS_WORKSPACE_ROOT': str(workspace),
                       'NOVA_ENV_SHARED_DOCKER': 'true', 'NOVA_ENV_UPDATE_CURRENT': 'true',
                       'NOVA_FIXTURE_RUNTIME_DIR': str(self.root / 'private-owner')}
        expected = workspace / 'docker' / 'iceberg-rest' / 'runtime' / entry.environment_identity(workspace)
        current = expected.parent / 'current'
        publications = []
        lock_inode = None
        for fixture in sources:
            environment['NOVA_ENV_CONFIG_FILE'] = str(fixture / 'shared.env')
            prepared = subprocess.run(['bash', str(fixture / 'up.sh'), '--prepare-only'],
                                      env=environment, capture_output=True, text=True)
            self.assertEqual(prepared.returncode, 0, prepared.stderr)
            result = json.loads(prepared.stdout)
            publications.append(result)
            self.assertFalse(result['ready'])
            self.assertEqual(result['entry_root'], str(expected))
            self.assertEqual(Path(result['published_dir']).parent, expected / 'publications')
            self.assertEqual(result['config']['repo_root'], str(fixture.parent.parent))
            self.assertEqual(current.resolve(), expected)
            self.assertEqual((current / 'env.sh').resolve(), Path(result['published_dir']) / 'env.sh')
            self.assertFalse((fixture / 'runtime').exists())
            lock = expected / '.owner.lock'
            if lock_inode is None:
                lock_inode = lock.stat().st_ino
            self.assertEqual(lock.stat().st_ino, lock_inode)
        self.assertNotEqual(publications[0]['published_dir'], publications[1]['published_dir'])
        self.assertEqual(publications[0]['config']['local_ports'], publications[1]['config']['local_ports'])
        self.assertEqual(publications[0]['config']['native_trust'], publications[1]['config']['native_trust'])
        # Either declaration checkout discovers the same final publication.
        status = subprocess.run(['bash', str(sources[0] / 'status.sh')],
                                env=environment, capture_output=True, text=True)
        self.assertEqual(status.returncode, 0, status.stderr)
        self.assertEqual(json.loads(status.stdout)['published_dir'], publications[1]['published_dir'])
        self.assertFalse(marker.exists())

    def test_shell_prepare_is_offline_and_ignores_ambient_endpoints(self):
        copied = self.root / 'checkout'
        fixture = copied / 'docker' / 'iceberg-rest'
        fixture.mkdir(parents=True)
        for name in ['up.sh', 'down.sh', 'status.sh', 'fixture_runtime.py', 'runtime_entry.py', 'shared.env']:
            shutil_copy(HERE / name, fixture / name)
        for folder in ('templates', 'spark'):
            import shutil
            shutil.copytree(HERE / folder, fixture / folder)
        fakebin = self.root / 'bin'; fakebin.mkdir()
        fake_docker = fakebin / 'docker'; fake_docker.write_text('#!/bin/sh\necho unexpected-docker >&2\nexit 99\n'); fake_docker.chmod(0o755)
        env = {**os.environ, 'NOVA_ENV_CONFIG_FILE': str(fixture / 'shared.env'), 'PATH': str(fakebin) + ':' + os.environ['PATH'], 'NOVA_ENV_REST_PORT': '8181', 'NOVA_ENV_COMPOSE_PROJECT': 'nr-iceberg-rest', 'NOVA_ENV_REST_WAREHOUSE_URI': 's3://foreign/shared', 'NOVA_FIXTURE_RUNTIME_DIR': str(self.root / 'owner'), 'NOVAROCKS_WORKSPACE_ROOT': str(copied)}
        result = subprocess.run(['bash', str(fixture / 'up.sh'), '--prepare-only'], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertFalse(data['ready'])
        fixed = fixture / 'runtime' / 'current' / 'env.sh'
        loaded = self.environment(fixed)
        self.assertEqual(loaded['NOVA_ENV_READY'], 'false')
        self.assertNotIn('NOVA_ENV_REST_PORT', loaded)
        self.assertNotIn('unverified', Path(data['published_dir']).joinpath('env.sh').read_text())
        rejected = subprocess.run(['bash', str(fixture / 'down.sh'), '--docker'], env=env, capture_output=True, text=True)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn('ExplicitRuntimeManagementRequired', rejected.stderr)
        status = subprocess.run(['bash', str(fixture / 'status.sh')], env=env, capture_output=True, text=True)
        self.assertEqual(json.loads(status.stdout)['published_dir'], data['published_dir'])


def shutil_copy(source, target):
    import shutil
    shutil.copy2(source, target)


class IsolatedEntryTests(RendererCase):
    def isolated(self, profile='stock'):
        config = copy.deepcopy(self.config)
        config.update(shared_docker=False, update_current=False)
        inputs = bom(); inputs['derived_images']['iceberg-spark']['image_id'] = 'sha256:spark'
        return entry.render_isolated_stack(inputs, 'nr-isolated-rest-test', {'minio': 19101, 'minio_console': 20101, 'rest': 21101, 'spark': 22101, 'control': 23101}, self.entry, config,
            profile=profile, hook_image='publication:exact' if profile != 'stock' else None)

    def test_stock_and_hook_are_initial_models_from_same_templates(self):
        sentinel = self.entry.parent / 'current'; sentinel.parent.mkdir(parents=True)
        sentinel.symlink_to('existing-worktree')
        result = self.isolated()
        entry.render_entry(result['context'], self.entry)
        manifest = json.loads((self.entry / 'manifest.json').read_text())
        self.assertFalse(manifest['shared_docker'])
        self.assertEqual(manifest['runtime']['profile'], 'stock')
        self.assertEqual(manifest['compose_file'], str(self.entry / 'compose.yml'))
        self.assertEqual(manifest['runtime']['template_model_sha256'], entry.model_hash(HERE.parent.parent))
        self.assertEqual(manifest['minio']['access_key_id'], self.config['credentials']['access_key'])
        self.assertIsNone(manifest['runtime']['control_uri'])
        model = (self.entry / 'compose.yml').read_text()
        for service in ('minio', 'mc-init', 'mc', 'rest', 'spark'):
            self.assertIn('  ' + service + ':\n', model)
        self.assertIn('mc mb --ignore-existing store/warehouse', model)
        self.assertIn('rest-catalog:/tmp', model)
        self.assertEqual(os.readlink(sentinel), 'existing-worktree')
        hook = self.isolated('publication-hook')
        entry.render_entry(hook['context'], self.entry)
        manifest = json.loads((self.entry / 'manifest.json').read_text())
        self.assertEqual(manifest['runtime']['profile'], 'publication-hook')
        self.assertEqual(manifest['runtime']['control_uri'], 'http://127.0.0.1:23101')
        self.assertIn('8182', (self.entry / 'compose.yml').read_text())
        self.assertIn('TracingFileIO', (self.entry / 'compose.yml').read_text())
        self.assertIn('REST_IMAGE=\'publication:exact\'', (self.entry / 'compose.env').read_text())
        self.assertEqual(os.readlink(sentinel), 'existing-worktree')

    def test_prepare_and_teardown_shells_leave_current_untouched_without_docker(self):
        import shutil
        copied = self.root / 'checkout'
        fixture = copied / 'docker/iceberg-rest'
        fixture.mkdir(parents=True)
        for name in ['up.sh', 'down.sh', 'fixture_runtime.py', 'runtime_entry.py', 'shared.env']:
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


class HelperEntryTests(RendererCase):
    def test_helpers_pin_one_publication_and_discard_compose_overrides(self):
        first = self.bind()
        old = Path(first['published_dir'])
        second = self.bind(bom('other'))
        log = self.root / 'docker.calls'
        fakebin = self.root / 'bin'; fakebin.mkdir()
        docker = fakebin / 'docker'
        docker.write_text('#!/usr/bin/env python3\nimport json,os,sys\nfrom pathlib import Path\np=Path(' + repr(str(log)) + ')\nwith p.open("a") as f: f.write(json.dumps({"args":sys.argv[1:],"foreign":os.environ.get("REST_IMAGE")})+"\\n")\n')
        docker.chmod(0o755)
        sql = self.root / 'query.sql'; sql.write_text('SELECT 1;\n')
        scala = self.root / 'query.scala'; scala.write_text('println(1)\n')
        env = {**os.environ, 'PATH': str(fakebin) + ':' + os.environ['PATH'], 'NOVA_ENV_REST_ENV_FILE': str(old / 'env.sh'), 'REST_IMAGE': 'foreign'}
        for helper, source in [('spark-sql.sh', sql), ('spark-shell.sh', scala)]:
            result = subprocess.run(['bash', str(HERE / helper), str(source)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(x) for x in log.read_text().splitlines()]
        self.assertTrue(calls)
        for call in calls:
            self.assertIn(first['records']['catalog']['compose_file'], call['args'])
            self.assertNotIn(second['records']['catalog']['compose_file'], call['args'])
            self.assertIsNone(call['foreign'])
        # Without an explicit publication, discovery belongs to W rather than
        # the checkout containing the helper script.
        current = self.workspace / 'docker' / 'iceberg-rest' / 'runtime' / 'current'
        current.parent.mkdir(parents=True)
        current.symlink_to(self.entry)
        env.pop('NOVA_ENV_REST_ENV_FILE')
        env['NOVAROCKS_WORKSPACE_ROOT'] = str(self.workspace)
        log.write_text('')
        for helper, source in [('spark-sql.sh', sql), ('spark-shell.sh', scala)]:
            result = subprocess.run(['bash', str(HERE / helper), str(source)], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(x) for x in log.read_text().splitlines()]
        self.assertTrue(calls)
        for call in calls:
            self.assertIn(second['records']['catalog']['compose_file'], call['args'])
        self.owner.unbind('test-worktree', self.entry)
        env['NOVA_ENV_REST_ENV_FILE'] = str(self.entry / 'env.sh')
        result = subprocess.run(['bash', str(HERE / 'spark-sql.sh'), str(sql)], env=env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('fixture is not ready', result.stderr)


class DownPurgeTests(RendererCase):
    def test_unbind_keeps_references_and_purge_is_atomic(self):
        first = self.bind()
        lock_inode = (self.entry / '.owner.lock').stat().st_ino
        state_store = self.entry / 'frontend-state.sqlite'; state_store.write_text('persistent-state')
        unbound = self.owner.unbind('test-worktree', self.entry)
        self.assertEqual(unbound['data_locations'], first['data_locations'])
        self.backend.fail = 'purge'
        with self.assertRaises(runtime.RuntimeFailure):
            self.owner.unbind('test-worktree', self.entry, purge=True)
        self.assertEqual(runtime.current_publication(self.entry), unbound)
        self.backend.fail = None
        cleared = self.owner.unbind('test-worktree', self.entry, purge=True)
        self.assertEqual(cleared['data_locations'], [])
        self.assertEqual((self.entry / '.owner.lock').stat().st_ino, lock_inode)
        self.assertEqual(state_store.read_text(), 'persistent-state')
        self.assertFalse(any(x[0] in ('stop', 'delete') for x in self.backend.calls))
        purges = [x[2] for x in self.backend.calls if x[0] == 'purge']
        self.assertTrue(purges)
        for prefixes in purges:
            self.assertEqual(prefixes, ['s3://warehouse/test-worktree/', 's3://novarocks/test-worktree/'])
            self.assertNotIn('shared/benchmarks', str(prefixes))


class ConsumerInputVerificationTests(unittest.TestCase):
    def test_runtime_requests_closed_iceberg_scope_and_publishes_its_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = Path(temporary)
            (store / 'bom.json').write_text(json.dumps({'lock_sha256': 'current-lock'}))
            config = {'repo_root': '/fixture-repository', 'fixture_inputs': {'bom': str(store / 'bom.json'), 'verified': False}}
            with mock.patch.object(entry.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as run:
                entry.verify_inputs(config)
            command = run.call_args.args[0]
            self.assertEqual(command, [str(HERE.parent / 'fixture-inputs/verify.sh'), '--store', str(store), '--repo-root', '/fixture-repository', '--consumer', 'iceberg-rest'])
            self.assertEqual(config['fixture_inputs']['consumer'], 'iceberg-rest')
            self.assertTrue(config['fixture_inputs']['verified'])
            self.assertEqual(config['fixture_inputs']['lock_sha256'], 'current-lock')

    def test_failed_input_verification_never_marks_publication_verified(self):
        config = {'repo_root': '/fixture-repository', 'fixture_inputs': {'bom': '/fixture-store/bom.json', 'verified': False}}
        with mock.patch.object(entry.subprocess, 'run', return_value=subprocess.CompletedProcess([], 75)), mock.patch.object(entry.runtime, 'read_json') as read, self.assertRaises(runtime.RuntimeFailure):
            entry.verify_inputs(config)
        read.assert_not_called()
        self.assertFalse(config['fixture_inputs']['verified'])


if __name__ == '__main__':
    unittest.main()
