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
"""Native host-SDK supervision without network, Docker or real credentials."""
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_runtime as runtime
import machine_operation as operation
import machine_runtime as machine
from test_cloud_scratch import cloud_record

FAKE = r'''import json,os,pathlib,signal,subprocess,sys,time
root=pathlib.Path(__file__).resolve().parent
if sys.argv[1:]==['_writer']:
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
    while True:
        with (root/'writes').open('a') as output: output.write('x')
        time.sleep(0.01)
args=sys.argv[1:]
(root/'argv.json').write_text(json.dumps(args))
(root/'env.json').write_text(json.dumps({key:os.environ.get(key) for key in ['HTTP_PROXY','https_proxy','PYTHONPATH','PYTHONNOUSERSITE','NETRC']}))
mode=(root/'mode').read_text()
if mode=='hold':
    signal.signal(signal.SIGTERM,signal.SIG_IGN)
    subprocess.Popen([sys.executable,__file__,'_writer'])
    (root/'entered').write_text(str(os.getpid()))
    while True: time.sleep(0.01)
print(json.dumps(dict(ok=True,status=200)))
'''


def setup_record(root):
    record = cloud_record()
    record.update(id=root.name, control_root=str(root.parent),
                  compose_file=str(root / 'machine.yml'), oss_ops_python=str(root / 'sdk-python'),
                  object_store_config_file=str(root / 'object-store.json'))
    runtime.atomic_json(Path(record['object_store_config_file']), record['config']['object_store'])
    return record


def owner_process(root):
    record = setup_record(root)
    descriptor = os.open(root / 'maintenance.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        backend = machine.MachineDocker(timeout=20, environment=dict(os.environ))
        backend.maintenance_fd = descriptor
        backend.maintenance_base = root
        return operation.run_owned_host_sdk_operation(backend, record,
            ['put', '--uri', 's3://novarocks-ci/dev/probe', '--input', str(root / 'input')],
            deadline=time.monotonic() + 20)
    finally:
        os.close(descriptor)


class CloudOperationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        fake = self.root / 'sdk-python'
        fake.write_text('#!' + sys.executable + '\n' + FAKE)
        fake.chmod(0o700)
        (self.root / 'mode').write_text('success')
        self.record = setup_record(self.root)
        self.fd = os.open(self.root / 'maintenance.lock', os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, self.fd)
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        self.backend = machine.MachineDocker(timeout=5, environment=dict(os.environ))
        self.backend.maintenance_fd = self.fd
        self.backend.maintenance_base = self.root

    def journal(self):
        paths = list((self.root / 'operations').glob('*.json'))
        self.assertEqual(len(paths), 1)
        return json.loads(paths[0].read_text())

    def test_fixed_argv_and_original_deadline_are_supervised_until_actual_exit(self):
        self.backend.environment.update(HTTP_PROXY='http://unselected-proxy', https_proxy='http://unselected-proxy', PYTHONPATH='/unselected/python')
        output = operation.run_owned_host_sdk_operation(self.backend, self.record,
            ['stat', '--uri', 's3://novarocks-ci/dev/probe'], deadline=time.monotonic() + 2)
        self.assertTrue(json.loads(output)['ok'])
        argv = json.loads((self.root / 'argv.json').read_text())
        self.assertEqual(argv[:3], [str(Path(operation.__file__).with_name('oss_ops.py').resolve()),
                                   '--config', self.record['object_store_config_file']])
        self.assertLessEqual(float(argv[4]), 2)
        self.assertEqual(argv[5:], ['stat', '--uri', 's3://novarocks-ci/dev/probe'])
        environment = json.loads((self.root / 'env.json').read_text())
        self.assertEqual(environment, dict(HTTP_PROXY=None, https_proxy=None, PYTHONPATH=None,
                                         PYTHONNOUSERSITE='1', NETRC=os.devnull))
        fact = self.journal()
        self.assertTrue(all(fact.get(name) is True for name in
                            ['group_exit_confirmed', 'group_exit_observed', 'leader_waited', 'command_succeeded']))
        self.assertEqual(fact['storage_owner'], self.record['storage_owner'])

    def test_closed_grammar_and_scope_reject_before_dispatch(self):
        for args in [['stat', 's3://novarocks-ci/dev/probe'],
                     ['stat', '--uri', 's3://novarocks-ci/outside/probe'],
                     ['stat', '--uri', 's3://novarocks-ci/dev/probe', '--config', '/foreign'],
                     ['admin', '--uri', 's3://novarocks-ci/dev/probe']]:
            with self.subTest(args=args), self.assertRaises(runtime.RuntimeFailure):
                operation.run_owned_host_sdk_operation(self.backend, self.record, args)
        self.assertFalse((self.root / 'argv.json').exists())

    def test_deadline_waits_for_real_descendant_exit(self):
        (self.root / 'mode').write_text('hold')
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            operation.run_owned_host_sdk_operation(self.backend, self.record,
                ['put', '--uri', 's3://novarocks-ci/dev/probe', '--input', str(self.root / 'input')],
                deadline=time.monotonic() + 1.5)
        self.assertEqual(caught.exception.code, 'ObjectStoreOperationTimeout')
        fact = self.journal()
        self.assertTrue(fact['group_exit_confirmed'] and fact['leader_waited'])
        self.assertEqual(fact['remote_outcome'], 'unknown')
        self.assertFalse(operation.process_group_running(fact['cli_pid']))
        size = (self.root / 'writes').stat().st_size
        time.sleep(0.05)
        self.assertEqual((self.root / 'writes').stat().st_size, size)

    def test_owner_loss_retains_original_flock_until_sdk_group_exits(self):
        # The child owns a distinct open-file description; only its original
        # monitor inherits it. Killing the caller closes its liveness writer.
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        (self.root / 'mode').write_text('hold')
        caller = subprocess.Popen([sys.executable, __file__, '--owner', str(self.root)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(lambda: caller.poll() is None and caller.kill())
        until = time.monotonic() + 5
        while not (self.root / 'entered').exists() and time.monotonic() < until:
            time.sleep(0.01)
        self.assertTrue((self.root / 'entered').exists())
        caller.kill()
        caller.wait()
        with self.assertRaises(BlockingIOError):
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while time.monotonic() < until:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(0.02)
        else:
            self.fail('original monitor did not settle its SDK process group')
        fact = self.journal()
        self.assertTrue(fact['interrupted'] and fact['group_exit_confirmed'] and fact['leader_waited'])
        self.assertFalse(fact['command_succeeded'])


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--owner':
        owner_process(Path(sys.argv[2]))
    else:
        unittest.main()
