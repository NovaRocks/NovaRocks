# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Cloud CLI operations keep the original admission and closed run scope."""
from contextlib import redirect_stdout, redirect_stderr
import io
import json
import os
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from test_cloud_scratch import cloud_record, cloud_identity
import test_machine_mc as minio_tests
import machine_mc as adapter
from scratch import RunBudget, RunStorageScope, ScratchIdentity


class CloudAdapterTests(unittest.TestCase):
    def invoke(self, script, extra=()):
        output, error = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {'NOVA_FIXTURE_RUN_CONTEXT': json.dumps(self.context)}, clear=True), \
             patch.object(adapter.machine, 'MachineOwner', return_value=self.owner), \
             patch.object(adapter.runtime, 'install_signal_handlers'), \
             patch.object(sys, 'stdin', io.StringIO(script)), redirect_stdout(output), redirect_stderr(error):
            status = adapter.main(self.args + list(extra))
        return status, output.getvalue(), error.getvalue()

    def setUp(self):
        minio_tests.AdapterTests.setUp(self)
        self.state = cloud_record()
        self.identity = ScratchIdentity.create("s3://novarocks-ci/dev/worktree", token="e" * 32)
        # Admission uses current wall time; the scope identity is intentionally
        # independent of its run budget's actual start/deadline.
        self.context = RunBudget.admit(self.state, self.identity, seconds=400).as_dict()
        self.storage = RunStorageScope.from_record(self.identity, self.state)
        self.owner.read_saved_state.return_value = self.state
        self.owner.backend.run_oss.return_value = '{"ok":true}\n'
        self.args[self.args.index('--expected-version') + 1] = self.state['version']
        self.args[self.args.index('--expected-generation') + 1] = self.state['generation']

    def request(self, uri=None):
        return json.dumps(dict(storage=self.storage.as_dict(),
            args=['stat', '--uri', uri or self.identity.scope_root + '/data']))

    def test_cloud_work_uses_same_snapshot_and_original_deadline(self):
        status, out, error = self.invoke(self.request(), ['--oss-operation'])
        self.assertEqual((status, out, error), (0, '{"ok":true}\n', ''))
        self.owner.backend.run_mc.assert_not_called()
        arguments = self.owner.backend.run_oss.call_args
        self.assertEqual(arguments.args, (self.state, ['stat', '--uri', self.identity.scope_root + '/data']))
        self.assertEqual(arguments.kwargs['deadline'], self.calls[0][1]['deadline'])

    def test_cloud_work_rejects_sibling_and_cleanup_reserve_before_dispatch(self):
        for uri in [self.identity.scope_root + '-sibling/data', self.state['config']['benchmark_root'] + '/READY']:
            status, out, _ = self.invoke(self.request(uri), ['--oss-operation'])
            self.assertEqual((status, out), (1, ''))
        self.owner.backend.run_oss.assert_not_called()
        self.calls.clear()
        status, out, _ = self.invoke(self.request(), ['--oss-operation', '--cleanup'])
        self.assertEqual((status, out), (75, ''))
        self.assertEqual(self.calls, [])

    def test_cloud_raw_mc_cannot_reach_iam_or_container(self):
        status, out, _ = self.invoke('mc admin user disable store foreign')
        self.assertEqual((status, out), (1, ''))
        self.owner.backend.run_mc.assert_not_called()

    def test_benchmark_dispatch_uses_separate_reviewed_helper_with_original_budget(self):
        helper = Mock(return_value='{"ok":true}\n')
        request = dict(resolved={'contract': 'verified by helper'}, operation='stat',
                       uri=self.state['config']['benchmark_root'] + '/dataset/READY')
        with patch.dict(sys.modules, {'benchmark_oss': SimpleNamespace(run_operation=helper)}):
            status, out, error = self.invoke(json.dumps(request), ['--benchmark-oss'])
        self.assertEqual((status, out, error), (0, '{"ok":true}\n', ''))
        self.assertEqual(helper.call_args.args, (self.state, self.owner.backend, request))
        self.assertEqual(helper.call_args.kwargs['deadline'], self.calls[0][1]['deadline'])
        self.owner.backend.run_mc.assert_not_called()
        self.owner.backend.run_oss.assert_not_called()


if __name__ == '__main__':
    unittest.main()
