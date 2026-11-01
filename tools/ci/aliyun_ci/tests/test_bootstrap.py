# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Bootstrap capabilities are consumed before runner code can execute.

Tests use recorded OOS shapes and local files; CI maintainers run them whenever
bootstrap, envelope delivery or image assembly changes. No live cloud calls.
"""
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tarfile
import tempfile
import types
import unittest

from aliyun_ci.instance import bootstrap
from aliyun_ci.ports import APIError


class OOS:
    def __init__(self, payload):
        data = json.dumps(payload).encode()
        encoded = base64.b64encode(data).decode()
        self.values = {'g16-test': json.dumps({'schema': 1, 'parts': 2,
                         'sha256': hashlib.sha256(data).hexdigest()}),
                       'g16-test-part-000000': encoded[:15], 'g16-test-part-000001': encoded[15:]}
        self.calls = []

    def call(self, action, **params):
        self.calls.append((action, params))
        if action == 'GetSecretParameter':
            return {'Parameter': {'Name': params['Name'], 'Value': self.values[params['Name']]}}
        if action == 'DeleteSecretParameter':
            del self.values[params['Name']]
            return {'RequestId': 'delete-1'}
        raise AssertionError(action)


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_encrypted_parts_are_verified_then_deleted_by_exact_name(self):
        client = OOS({'jit': 'encoded-secret'})
        self.assertEqual({'jit': 'encoded-secret'}, bootstrap.consume_envelope(client, 'g16-test'))
        self.assertEqual({}, client.values)
        self.assertEqual(['GetSecretParameter'] * 3 + ['DeleteSecretParameter'] * 3,
                         [action for action, _ in client.calls])
        self.assertTrue(all(params['WithDecryption'] is True for action, params in client.calls[:3]))
        self.assertEqual(['g16-test-part-000000', 'g16-test-part-000001', 'g16-test'],
                         [params['Name'] for _, params in client.calls[3:]])

    def test_corrupt_payload_is_not_consumed(self):
        client = OOS({'jit': 'encoded-secret'})
        root = json.loads(client.values['g16-test'])
        root['sha256'] = '0' * 64
        client.values['g16-test'] = json.dumps(root)
        with self.assertRaisesRegex(ValueError, 'SHA-256'):
            bootstrap.consume_envelope(client, 'g16-test')
        self.assertFalse(any(action.startswith('Delete') for action, _ in client.calls))

    def test_initial_not_found_waits_for_control_publication_then_consumes_exact_parts(self):
        class PublishingOOS(OOS):
            def __init__(self):
                super().__init__({'jit': 'encoded-secret'})
                self.pending = True
            def call(self, action, **params):
                if self.pending:
                    self.pending = False
                    self.calls.append((action, params))
                    raise APIError('InvalidSecretParameter.NotFound')
                return super().call(action, **params)
        client = PublishingOOS()
        waits = []
        result = bootstrap.await_envelope(client,
            {'secret_name': 'g16-test', 'deadline': '2026-10-28T01:00:00Z'},
            now=lambda: datetime(2026, 10, 28, tzinfo=timezone.utc), sleep=waits.append)
        self.assertEqual({'jit': 'encoded-secret'}, result)
        self.assertEqual([5], waits)
        self.assertEqual(['GetSecretParameter'] * 4 + ['DeleteSecretParameter'] * 3,
                         [action for action, _ in client.calls])
        self.assertEqual(['g16-test', 'g16-test', 'g16-test-part-000000', 'g16-test-part-000001',
                          'g16-test-part-000000', 'g16-test-part-000001', 'g16-test'],
                         [params['Name'] for _, params in client.calls])
        self.assertEqual({}, client.values)

    def test_publication_wait_stops_at_original_deadline(self):
        current = [datetime(2026, 10, 28, tzinfo=timezone.utc)]
        class Absent:
            def call(self, *args, **kwargs):
                raise APIError('ParameterNotFound')
        def advance(seconds):
            current[0] += timedelta(seconds=seconds)
        with self.assertRaises(TimeoutError):
            bootstrap.await_envelope(Absent(), {'secret_name': 'g16-test', 'deadline': '2026-10-28T00:00:06Z'},
                                      now=lambda: current[0], sleep=advance)
        self.assertEqual(6, current[0].second)

    def test_install_keeps_jit_root_only_and_checks_tools_without_install(self):
        runtime, runner, attempt = (self.root / name for name in ('run', 'runner', 'attempt'))
        calls = []
        def command(argv, **kwargs):
            calls.append(argv)
            return subprocess.CompletedProcess(argv, 0, '2.1\n' if argv[0] == 'runuser' else
                '{"action":"check","rust":{"rustc":"pinned"}}', '')
        def downloader(release, home):
            (home / 'run.sh').write_text('#!/bin/sh\nexit 0\n')
        payload = {'jit': 'encoded-secret', 'runner': {'url': 'https://signed.invalid/runner',
                   'sha256': '0' * 64, 'version': '2.1'}, 'deadline': '2026-10-28T01:00:00Z',
                   'attempt_root': str(attempt), 'upload_urls': {'final': 'https://signed.invalid/final',
                   'interrupted': 'https://signed.invalid/interrupted'},
                   'watcher': {'poll_seconds': 10, 'cancel_budget_seconds': 120}, 'identity': {'run_id': '42'}}
        result = bootstrap.install(payload, uid=os.getuid(), gid=os.getgid(), runtime=runtime,
            runner_home=runner, attempt_root=attempt, tools_source=self.root / 'tools-source',
            run=command, downloader=downloader)
        self.assertTrue(result['installed'])
        self.assertEqual(0o600, stat.S_IMODE((runtime / 'jit.json').stat().st_mode))
        self.assertEqual(0o640, stat.S_IMODE((runtime / 'bootstrap.json').stat().st_mode))
        self.assertNotIn('encoded-secret', (runtime / 'bootstrap.json').read_text())
        self.assertEqual('check', calls[0][1])
        self.assertIn('--tools-only', calls[0])
        self.assertEqual(['systemctl', 'start', '--no-block', 'novarocks-runner.service',
                          'novarocks-spot-watcher.service'], calls[2])
        self.assertEqual(['runuser', '-u', 'runner', '--', str(runner / 'bin/Runner.Listener'),
                          '--version'], calls[1])
        self.assertEqual('2.1', result['runner_version'])
        self.assertEqual('check', json.loads((attempt / 'tools-check.json').read_text())['action'])

    def test_runtime_dependency_failure_or_version_mismatch_never_publishes_jit(self):
        for observed in ('2.0', None):
            with self.subTest(observed=observed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                calls = []
                def command(argv, **kwargs):
                    calls.append(argv)
                    if argv[0] == 'runuser':
                        if observed is None:
                            raise subprocess.CalledProcessError(1, argv, stderr='missing runtime dependency')
                        return subprocess.CompletedProcess(argv, 0, observed + '\n', '')
                    return subprocess.CompletedProcess(argv, 0, '{}', '')
                payload = {'jit': 'encoded-secret', 'runner': {'version': '2.1'},
                           'attempt_root': str(root / 'attempt'), 'deadline': '2026-10-28T01:00:00Z'}
                with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                    bootstrap.install(payload, uid=os.getuid(), gid=os.getgid(),
                        runtime=root / 'runtime', runner_home=root / 'agent', attempt_root=root / 'attempt',
                        tools_source=root / 'tools-source', run=command, downloader=lambda *args: None)
                self.assertFalse((root / 'runtime/jit.json').exists())
                self.assertFalse(any(argv[0] == 'systemctl' for argv in calls))

    def test_launch_destroys_jit_and_drops_privilege_before_exec(self):
        runtime = self.root / 'run'
        runtime.mkdir()
        secret = runtime / 'jit.json'
        secret.write_text(json.dumps('encoded-secret'))
        secret.chmod(0o600)
        calls = []
        account = types.SimpleNamespace(pw_name='runner', pw_uid=1234, pw_gid=1235, pw_dir=str(self.root))
        def execute(program, argv, env):
            self.assertFalse(secret.exists())
            self.assertEqual(['groups', 'gid', 'uid'], calls)
            self.assertEqual('encoded-secret', argv[-1])
            self.assertNotIn('encoded-secret', str(env))
            calls.append('exec')
        previous = Path.cwd()
        try:
            bootstrap.launch_runner(runtime=runtime, runner_home=self.root, account=account,
                drop_groups=lambda *_: calls.append('groups'), drop_gid=lambda *_: calls.append('gid'),
                drop_uid=lambda *_: calls.append('uid'), execute=execute)
        finally:
            os.chdir(previous)
        self.assertEqual(['groups', 'gid', 'uid', 'exec'], calls)

    def test_runner_archive_digest_is_checked_before_extraction(self):
        data = io.BytesIO()
        with tarfile.open(fileobj=data, mode='w:gz') as archive:
            info = tarfile.TarInfo('run.sh')
            info.size = 5
            archive.addfile(info, io.BytesIO(b'hello'))
        content = data.getvalue()
        destination = self.root / 'runner'
        destination.mkdir()
        opener = lambda *args, **kwargs: io.BytesIO(content)
        with self.assertRaisesRegex(ValueError, 'SHA-256'):
            bootstrap.download_runner({'url': 'unused', 'sha256': '0' * 64}, destination, opener=opener)
        self.assertFalse(any(destination.iterdir()))
        bootstrap.download_runner({'url': 'unused', 'sha256': hashlib.sha256(content).hexdigest()}, destination, opener=opener)
        self.assertEqual(b'hello', (destination / 'run.sh').read_bytes())

    def test_explicit_builder_pointer_needs_no_secret_or_cloud_client(self):
        from contextlib import redirect_stdout
        from unittest.mock import patch
        pointer=types.SimpleNamespace(read=lambda path:json.dumps({'role':'builder','deadline':'2026-10-29T00:00:00Z'}))
        with patch.object(bootstrap.os,'geteuid',return_value=0),patch.object(bootstrap,'Metadata',return_value=pointer),patch('aliyun_ci.sdk.RPC',side_effect=AssertionError('builder must not consume worker secrets')),redirect_stdout(io.StringIO()) as output:
            self.assertEqual(0,bootstrap.main([]))
        self.assertEqual({'execution_kind':'builder','installed':False},json.loads(output.getvalue()))
