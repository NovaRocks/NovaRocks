#!/usr/bin/env python3
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
"""Behavioral tests for the runtime model mode and its native CI registration."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / 'tools/ci'))
from verification.declarations import native_stage_mode
from verification import run
from verification.select import select
from verification.model import Scope, UnitKey
from verification.scope import required_scopes


class Models(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='control-runtime-model-mode-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.cargo = self.bin / 'cargo'
        self.cargo.write_text(r'''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
args=sys.argv[1:]
if args==['--version']:
    print('cargo fixture')
    sys.exit(0)
with Path(os.environ['MODEL_TEST_COMMANDS']).open('a') as stream:
    stream.write(json.dumps({'argv':args,'flags':os.environ.get('RUSTFLAGS'),
                             'offline':os.environ.get('CARGO_NET_OFFLINE')})+'\n')
mode=os.environ.get('MODEL_TEST_OUTCOME','pass')
if mode=='fail':
    print('running 1 test\ntest actual_model ... FAILED\ntest result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out;')
    sys.exit(23)
if mode=='incomplete':
    print('running 1 test\ntest actual_model ... ok')
    sys.exit(0)
count=0 if mode=='empty' else 2
print(f'running {count} tests')
for number in range(count): print(f'test model_{number} ... ok')
print(f'test result: ok. {count} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s')
''')
        self.cargo.chmod(0o755)
        self.environment = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                                MODEL_TEST_COMMANDS=str(self.root / 'commands.jsonl'))
        for name in list(self.environment):
            if name.startswith('LOOM_') or name == 'CARGO_ENCODED_RUSTFLAGS':
                self.environment.pop(name)

    def invoke(self, mode='pass', output='evidence', extra=(), environment=None):
        env = dict(self.environment, MODEL_TEST_OUTCOME=mode)
        env.update(environment or {})
        return subprocess.run(['bash', str(REPO / 'tools/ci/control-runtime-model-checks.sh'),
                               '--loom', '--output', str(self.root / output),
                               '--target-dir', str(self.root / 'model-target')] + list(extra),
                              cwd=REPO, env=env, text=True, capture_output=True)

    def evidence(self, output='evidence'):
        return json.loads((self.root / output / 'result.json').read_text())

    def test_nonempty_actual_runs_keep_all_commands_outputs_and_exits(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        evidence = self.evidence()
        self.assertTrue(evidence['complete'])
        self.assertTrue(evidence['sources_stable'])
        targets = ['concurrency_loom', 'continuation_loom', 'exit_custody_loom', 'failure_publication_loom', 'read_resource_loom']
        self.assertEqual([item['target'] for item in evidence['targets']], targets)
        commands = [json.loads(line) for line in (self.root / 'commands.jsonl').read_text().splitlines()]
        self.assertEqual(len(commands), len(targets))
        for target, command in zip(targets, commands):
            with self.subTest(target=target):
                args = command['argv']
                self.assertEqual(args[args.index('--test') + 1], target)
                self.assertIn('--locked', args)
                self.assertIn('--offline', args)
                self.assertEqual(command['offline'], 'true')
                self.assertIn('--cfg loom', command['flags'])
                self.assertEqual(args[args.index('--target-dir') + 1], str((self.root / 'model-target').resolve()))
                directory = self.root / 'evidence' / target
                self.assertEqual(json.loads((directory / 'exit.json').read_text())['code'], 0)
                self.assertTrue((directory / 'command.json').is_file())
                self.assertIn('test result: ok.', (directory / 'stdout.log').read_text())
                self.assertTrue((directory / 'stderr.log').is_file())
        sources = json.loads((self.root / 'evidence/sources-start.json').read_text())
        self.assertIn('novarocks/control-runtime/src/continuation.rs', sources)
        self.assertIn('novarocks/control-runtime/tests/continuation_loom.rs', sources)
        self.assertIn('novarocks/control-runtime-core/src/scope.rs', sources)

    def test_zero_model_and_truncated_run_are_not_passes(self):
        for mode in ('empty', 'incomplete'):
            with self.subTest(mode=mode):
                result = self.invoke(mode, output=mode)
                self.assertEqual(result.returncode, 1, result.stderr)
                evidence = self.evidence(mode)
                self.assertFalse(evidence['complete'])
                self.assertEqual(len(evidence['targets']), 1)
                self.assertEqual(evidence['targets'][0]['exit'], 0, 'keep the real Cargo exit distinct from completeness')

    def test_real_cargo_failure_is_preserved_and_later_targets_are_not_started(self):
        result = self.invoke('fail')
        self.assertEqual(result.returncode, 23, result.stderr)
        evidence = self.evidence()
        self.assertEqual(evidence['exit'], 23)
        self.assertFalse(evidence['complete'])
        self.assertEqual(evidence['targets'][0]['exit'], 23)
        self.assertEqual(len(evidence['targets']), 1)
        self.assertIn('actual_model ... FAILED', (self.root / 'evidence/concurrency_loom/stdout.log').read_text())

    def test_reusing_output_refuses_to_overwrite_original_attempt(self):
        self.assertEqual(self.invoke().returncode, 0)
        previous = (self.root / 'evidence/result.json').read_bytes()
        repeated = self.invoke()
        self.assertNotEqual(repeated.returncode, 0)
        self.assertEqual((self.root / 'evidence/result.json').read_bytes(), previous)
        self.assertEqual(len((self.root / 'commands.jsonl').read_text().splitlines()), 5)

    def test_encoded_flags_and_external_loom_early_stop_cannot_hide_configuration(self):
        result = self.invoke(environment={'CARGO_ENCODED_RUSTFLAGS': '--cfg\x1fnot_loom'})
        self.assertEqual(result.returncode, 2)
        self.assertFalse((self.root / 'commands.jsonl').exists())
        for name in ('LOOM_MAX_PERMUTATIONS', 'LOOM_MAX_DURATION', 'LOOM_CHECKPOINT_FILE'):
            result = self.invoke(output=name, environment={name: '1'})
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertFalse(self.evidence(name)['complete'])
        self.assertFalse((self.root / 'commands.jsonl').exists())

    def test_invalid_mode_runs_no_cargo(self):
        result = subprocess.run(['bash', str(REPO / 'tools/ci/control-runtime-model-checks.sh'), '--miri'],
                                env=self.environment, text=True, capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertFalse((self.root / 'commands.jsonl').exists())

    def test_native_stage_is_heavy_loom_and_describes_the_same_real_wrapper(self):
        stages = json.loads(subprocess.check_output(['bash', str(REPO / 'tools/ci/local-full-ci.sh'), '--list-stages'], cwd=REPO, text=True))
        stage = next(item for item in stages['stages'] if item['name'] == 'control-runtime:loom')
        self.assertEqual(stage['tier'], 'full')
        self.assertEqual(native_stage_mode(stage), 'loom')
        self.assertEqual(native_stage_mode({'name': 'memory-model:loom'}), 'loom')
        self.assertEqual(native_stage_mode({'name': 'memory-model:miri'}), 'miri')
        self.assertEqual(native_stage_mode({'name': 'ordinary'}), 'default')
        spec = run.stage_spec(REPO, {'inputs': str(self.root / 'no-artifacts')}, stage, self.root / 'stage')
        command = spec['native_command']
        self.assertEqual(command[0], 'bash')
        self.assertEqual(Path(command[1]), REPO / 'tools/ci/control-runtime-model-checks.sh')
        self.assertEqual(command[2], '--loom')
        self.assertIn('--output', command)
        self.assertIn('RUSTFLAGS', {item['name'] for item in spec['env_inputs']})
        self.assertFalse((self.root / 'stage').exists(), 'description does not create runtime or compile')
        chosen = select(['novarocks/control-runtime/src/continuation.rs'],
                        {'version': 1, 'workspace_members': [], 'packages': []},
                        {'suites': []}, {'scenarios': []}, stages, {'rules': []}, REPO)
        selected = next(item for item in chosen['stages'] if item['name'] == stage['name'])
        self.assertEqual(selected['tier'], 'full')
        self.assertEqual(selected['mode'], 'loom')
        unit = UnitKey('stage', stage['name'], native_stage_mode(stage))
        scope = Scope(str(unit), (unit,), tier='heavy', selected=True)
        self.assertEqual(required_scopes('admission', [scope]), [])
        self.assertEqual(required_scopes('full', [scope]), [scope])
        self.assertEqual(native_stage_mode({'name': 'arbitrary display name', 'mode': 'loom'}), 'loom')


if __name__ == '__main__':
    unittest.main()
