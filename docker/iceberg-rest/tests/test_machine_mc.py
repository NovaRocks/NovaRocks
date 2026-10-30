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

"""The mc adapter retains caller identity, deadline, generation and failures."""
from contextlib import contextmanager, redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import machine_mc as adapter
from scratch import RunBudget, RunStorageScope, ScratchIdentity


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.state = {"policy": {"scratch_ttl_seconds": 2000, "max_run_seconds": 500,
                      "cleanup_grace_seconds": 30}, "protected_benchmark_roots": [],
                      "config": {"benchmark_root": "s3://novarocks/benchmark", "minio_volume": "exact-minio-volume"},
                      "id": "machine-id", "control_root": "/exact-machine-root", "daemon_id": "daemon-id",
                      "version": "version-id", "generation": "generation-id",
                      "server_warehouses": {"rest": "s3://warehouse/machine/rest", "rest-mv": "s3://warehouse/machine/rest-mv"}}
        self.identity = ScratchIdentity.create("s3://novarocks/test-worktree")
        self.context = RunBudget.admit(self.state, self.identity, seconds=400).as_dict()
        self.storage = RunStorageScope.from_record(self.identity, self.state)
        self.owner = mock.Mock()
        self.owner.read_saved_state.return_value = self.state
        self.calls = []
        @contextmanager
        def snapshot(definition, **kwargs):
            self.calls.append((definition, kwargs))
            yield self.state
        self.owner.ready_snapshot.side_effect = snapshot
        self.owner.backend.run_mc.return_value = 'mc-output\n'
        self.args = ['--root', str(self.root), '--daemon', 'daemon-id',
                     '--expected-version', 'version-id', '--expected-generation', 'generation-id']

    def invoke(self, script='private-secret-payload', extra=(), context=None, storage=None, owner_expected=True):
        out, err = io.StringIO(), io.StringIO()
        environment = {'NOVA_FIXTURE_RUN_CONTEXT': json.dumps(context or self.context)}
        if storage is not None:
            environment['NOVA_FIXTURE_RUN_STORAGE'] = json.dumps(storage)
        with mock.patch.dict(os.environ, environment, clear=True), \
             mock.patch.object(adapter.machine, 'MachineOwner', return_value=self.owner) as constructor, \
             mock.patch.object(adapter.runtime, 'install_signal_handlers'), \
             mock.patch.object(sys, 'stdin', io.StringIO(script)), redirect_stdout(out), redirect_stderr(err):
            status = adapter.main(self.args + list(extra))
        if owner_expected:
            constructor.assert_called_once_with(self.root, daemon='daemon-id')
        else:
            constructor.assert_not_called()
        return status, out.getvalue(), err.getvalue()

    def test_work_uses_original_complete_deadline_and_generation(self):
        before = time.monotonic()
        status, out, err = self.invoke()
        self.assertEqual((status, out, err), (0, 'mc-output\n', ''))
        definition, kwargs = self.calls[0]
        self.assertEqual(definition, {'version': 'version-id'})
        self.assertEqual(kwargs['expected_generation'], 'generation-id')
        self.assertAlmostEqual(kwargs['deadline'] - before, 370, delta=2)
        self.assertEqual(self.owner.backend.run_mc.call_args.args, (self.state, 'private-secret-payload'))
        self.assertEqual(self.owner.backend.run_mc.call_args.kwargs['deadline'], kwargs['deadline'])

    def test_invalid_admission_never_acquires_lock_or_runs_script(self):
        context = dict(self.context, cleanup_grace_seconds=40)
        status, out, err = self.invoke(context=context)
        self.assertEqual(status, 75)
        self.assertEqual(out, '')
        self.assertNotIn('private-secret-payload', err)
        self.owner.ready_snapshot.assert_not_called()
        self.owner.backend.run_mc.assert_not_called()

    def test_admitted_expiry_is_failure_and_cleanup_uses_original_reserve(self):
        now_ms = int(time.time() * 1000)
        context = dict(self.context, started_unix_ms=now_ms-390000, deadline_unix_ms=now_ms+10000)
        status, _, _ = self.invoke(context=context)
        self.assertEqual(status, 1)
        self.owner.ready_snapshot.assert_not_called()
        before = time.monotonic()
        status, _, _ = self.invoke(extra=['--cleanup'], context=context)
        self.assertEqual(status, 0)
        self.assertAlmostEqual(self.calls[0][1]['deadline']-before, 10, delta=2)

    def test_exact_cleanup_uses_registered_pairs_and_settles_only_success(self):
        pairs = [{'user': self.identity.iam_name('init'), 'policy': self.identity.resource_name('init')}]
        request = json.dumps({'identity': self.identity.as_dict(), 'storage': self.storage.as_dict(), 'users': pairs})
        sweeper = mock.Mock()
        sweeper.cleanup_run.return_value = {'complete': True, 'removed': [], 'errors': []}
        with mock.patch.object(adapter, 'ScratchSweeper', return_value=sweeper):
            status, out, err = self.invoke(request, ['--cleanup', '--cleanup-run'])
        self.assertEqual((status, err), (0, ''))
        self.assertTrue(json.loads(out)['complete'])
        self.assertEqual(sweeper.cleanup_run.call_args.args[0].as_dict(), self.storage.as_dict())
        self.assertEqual(sweeper.cleanup_run.call_args.args[1], pairs)
        self.owner.backend.run_mc.assert_not_called()
        sweeper.cleanup_run.return_value = {'complete': False, 'errors': ['denied']}
        with mock.patch.object(adapter, 'ScratchSweeper', return_value=sweeper):
            status, out, err = self.invoke(request, ['--cleanup', '--cleanup-run'])
        self.assertEqual((status, out), (1, ''))
        self.assertIn('ScratchCleanupIncomplete', err)
        self.assertIn('denied', err)
        self.assertNotIn('private-secret-payload', err)

    def test_foreign_cleanup_storage_never_constructs_sweeper_or_runs_script(self):
        for field in ('daemon_id', 'version', 'generation', 'control_root', 'id'):
            with self.subTest(field=field):
                storage = self.storage.as_dict()
                storage['machine'][field] = 'foreign-value'
                request = json.dumps({'identity': self.identity.as_dict(), 'storage': storage, 'users': []})
                with mock.patch.object(adapter, 'ScratchSweeper') as sweeper:
                    status, out, _ = self.invoke(request, ['--cleanup', '--cleanup-run'])
                self.assertNotEqual(status, 0)
                self.assertEqual(out, '')
                sweeper.assert_not_called()
        self.owner.backend.run_mc.assert_not_called()

    def test_missing_cleanup_storage_is_rejected_before_remote_effects(self):
        request = json.dumps({'identity': self.identity.as_dict(), 'users': []})
        with mock.patch.object(adapter, 'ScratchSweeper') as sweeper:
            status, out, _ = self.invoke(request, ['--cleanup', '--cleanup-run'])
        self.assertNotEqual(status, 0)
        self.assertEqual(out, '')
        sweeper.assert_not_called()
        self.owner.backend.run_mc.assert_not_called()

    def test_inherited_storage_is_rechecked_under_locked_machine_snapshot(self):
        storage = self.storage.as_dict()
        storage['storage_owner']['minio_volume'] = 'foreign-volume'
        status, out, _ = self.invoke(storage=storage)
        self.assertNotEqual(status, 0)
        self.assertEqual(out, '')
        self.assertEqual(len(self.calls), 1)
        self.owner.backend.run_mc.assert_not_called()

    def test_exact_iam_ports_keep_original_phase_deadline_and_do_not_call_full_cleanup(self):
        pairs = [{'user': self.identity.iam_name('init'), 'policy': self.identity.resource_name('init')}]
        request = json.dumps({'identity': self.identity.as_dict(), 'storage': self.storage.as_dict(), 'users': pairs})
        for action, phase, method, remaining in (
                ('--check-users-absent', [], 'check_users_absent', 370),
                ('--cleanup-users', ['--cleanup'], 'cleanup_users', 400)):
            with self.subTest(action=action):
                sweeper = mock.Mock()
                operation = getattr(sweeper, method)
                operation.return_value = {'complete': True, 'removed': [], 'errors': []}
                before = time.monotonic()
                with mock.patch.object(adapter, 'ScratchSweeper', return_value=sweeper):
                    status, out, err = self.invoke(request, [*phase, action], storage=self.storage.as_dict())
                self.assertEqual((status, err), (0, ''))
                self.assertTrue(json.loads(out)['complete'])
                self.assertEqual(operation.call_args.args[0].as_dict(), self.storage.as_dict())
                self.assertEqual(operation.call_args.args[1], pairs)
                deadline = operation.call_args.kwargs['deadline']
                self.assertAlmostEqual(deadline - before, remaining, delta=2)
                self.assertEqual(deadline, self.calls[-1][1]['deadline'])
                sweeper.cleanup_run.assert_not_called()
        self.owner.backend.run_mc.assert_not_called()

    def test_exact_iam_ports_reject_wrong_phase_before_snapshot_or_remote_effects(self):
        for extra in (['--cleanup', '--check-users-absent'], ['--cleanup-users']):
            with self.subTest(extra=extra), mock.patch.object(adapter, 'ScratchSweeper') as sweeper:
                status, out, _ = self.invoke('{}', extra, owner_expected=False)
                self.assertNotEqual(status, 0)
                self.assertEqual(out, '')
                sweeper.assert_not_called()
        self.owner.ready_snapshot.assert_not_called()
        self.owner.backend.run_mc.assert_not_called()

    def test_exact_iam_ports_reject_foreign_context_or_storage_before_sweeper(self):
        for action, phase in (('--check-users-absent', []), ('--cleanup-users', ['--cleanup'])):
            for change in ('identity', 'generation', 'missing-storage', 'extra-field'):
                with self.subTest(action=action, change=change):
                    request = {'identity': self.identity.as_dict(), 'storage': self.storage.as_dict(), 'users': []}
                    if change == 'identity':
                        request['identity'] = ScratchIdentity.create('s3://novarocks/peer').as_dict()
                    elif change == 'generation':
                        request['storage']['machine']['generation'] = 'foreign-generation'
                    elif change == 'missing-storage':
                        del request['storage']
                    else:
                        request['script'] = 'unadmitted-shell'
                    with mock.patch.object(adapter, 'ScratchSweeper') as sweeper:
                        status, out, _ = self.invoke(json.dumps(request), [*phase, action])
                    self.assertNotEqual(status, 0)
                    self.assertEqual(out, '')
                    sweeper.assert_not_called()
        self.owner.backend.run_mc.assert_not_called()

    def test_existing_iam_or_cleanup_failure_remains_failure_without_full_cleanup(self):
        request = json.dumps({'identity': self.identity.as_dict(), 'storage': self.storage.as_dict(), 'users': []})
        for action, phase, method, code in (
                ('--check-users-absent', [], 'check_users_absent', 'ScratchIamAbsenceUnconfirmed'),
                ('--cleanup-users', ['--cleanup'], 'cleanup_users', 'ScratchCleanupIncomplete')):
            with self.subTest(action=action):
                sweeper = mock.Mock()
                getattr(sweeper, method).return_value = {'complete': False, 'removed': [], 'errors': [{'code': 'ScratchAccessDenied'}]}
                with mock.patch.object(adapter, 'ScratchSweeper', return_value=sweeper):
                    status, out, err = self.invoke(request, [*phase, action])
                self.assertEqual((status, out), (1, ''))
                self.assertIn(code, err)
                self.assertIn('ScratchAccessDenied', err)
                sweeper.cleanup_run.assert_not_called()
        self.owner.backend.run_mc.assert_not_called()

    def test_expected_activation_failure_is_not_silently_reattached(self):
        self.owner.ready_snapshot.side_effect = adapter.runtime.RuntimeFailure('MachineVersionMismatch', 'activation changed')
        status, out, err = self.invoke()
        self.assertEqual((status, out), (75, ''))
        self.assertIn('MachineVersionMismatch', err)
        self.owner.backend.run_mc.assert_not_called()


if __name__ == '__main__':
    unittest.main()
