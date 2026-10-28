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
"""CI owner: advisory discovery preserves native results independently of admission.

Local unittest fixtures project cargo-deny 0.20.2 JSON diagnostics; subprocess
stubs provide independent exit/output oracles without network or shared state.
TemporaryDirectory owns all stub outputs and git databases through actual exit.
The verification composition stage runs these inexpensive checks on CI changes.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[3]
SCRIPT = REPO / 'tools/ci/dependency-advisories.py'
sys.path.insert(0, str(REPO / 'tools/ci'))
from verification import run

# Minimal field projections of the real cargo-deny 0.20.2 diagnostic samples.
VULNERABILITY = {'type': 'diagnostic', 'fields': {
    'severity': 'error', 'code': 'vulnerability',
    'advisory': {'id': 'RUSTSEC-2026-0391', 'package': 'zstd'},
    'graphs': [{'Krate': {'name': 'zstd', 'version': '0.13.3'}}]}}
CLEANUP = {'type': 'diagnostic', 'fields': {
    'severity': 'error', 'code': 'advisory-not-detected',
    'labels': [{'span': 'RUSTSEC-2020-0001'}]}}
ERROR = {'type': 'log', 'fields': {'level': 'ERROR', 'message': 'failed to fetch advisory database'}}


class Advisories(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='dependency-advisories-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.home = self.root / 'cargo-home'
        self.home.mkdir()
        cargo = self.bin / 'cargo'
        cargo.write_text('''#!/bin/sh
if [ "$*" = "deny --version" ]; then echo 'cargo-deny 0.20.2'; exit 0; fi
printf '%s\\n' "$*" >> "$CARGO_MARKER"
cat "$CARGO_STDERR" >&2
printf 'raw stdout\\n'
exit "$CARGO_EXIT"
''')
        cargo.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                        CARGO_HOME=str(self.home), CARGO_MARKER=str(self.root / 'marker'))

    def invoke(self, records=(), code=0, label='run'):
        raw = ''.join(json.dumps(record) + '\n' for record in records).encode()
        stderr = self.root / (label + '.input')
        stderr.write_bytes(raw)
        output = self.root / label
        env = dict(self.env, CARGO_STDERR=str(stderr), CARGO_EXIT=str(code))
        result = subprocess.run(['bash', str(REPO / 'tools/ci/local-full-ci.sh'), '--run-stage',
                                 'Cargo dependency advisories', str(self.root / 'absent'), str(output)],
                                cwd=REPO, env=env, capture_output=True)
        summary_root = output / 'dependency-advisories'
        return result, json.loads((summary_root / 'summary.json').read_text()), summary_root, raw

    def test_stage_registration_and_cargo_chain(self):
        stages = json.loads(subprocess.check_output(['bash', 'tools/ci/local-full-ci.sh', '--list-stages'], cwd=REPO))
        by_name = {stage['name']: stage for stage in stages['stages']}
        self.assertEqual(by_name['Cargo dependency policy'], {'name': 'Cargo dependency policy', 'tier': 'admission', 'domain': ['**']})
        self.assertEqual(by_name['Cargo dependency advisories'], {'name': 'Cargo dependency advisories', 'tier': 'full', 'domain': [
            'Cargo.lock', 'deny.toml', 'tools/ci/dependency-advisories.py', '.github/workflows/dependency-advisories.yml']})
        shell = '''source tools/ci/local-full-ci.sh --source-only
run_fail_fast_stage() { printf '%s\\n' "$1"; }
run_rust_batch_stage() { :; }
run_cargo_gates
'''
        result = subprocess.run(['bash', '-c', shell], cwd=REPO, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Cargo dependency policy', result.stdout.splitlines())
        self.assertNotIn('Cargo dependency advisories', result.stdout.splitlines())

    def test_description_validates_without_creating_outputs(self):
        output = self.root / 'description'
        spec = run.stage_spec(REPO, {'inputs': str(self.root / 'inputs')},
                              {'name': 'Cargo dependency advisories'}, output)
        self.assertEqual(spec['native_command'], ['python3', str(SCRIPT), '--output', str(output / 'dependency-advisories')])
        self.assertEqual(spec['execution_args'][-1], '@output')
        self.assertFalse(output.exists())

    def test_pass(self):
        result, summary, output, _ = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(summary['outcome'], 'pass')
        self.assertEqual((output / 'exit').read_text(), '0\n')
        self.assertEqual((self.root / 'marker').read_text(), 'deny --format json --locked check advisories\n')
        self.assertEqual((output / 'cargo-deny.stdout').read_bytes(), b'raw stdout\n')

    def test_findings_cleanup_and_raw_bytes(self):
        result, summary, output, raw = self.invoke([VULNERABILITY, CLEANUP], 1)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(summary['outcome'], 'findings')
        self.assertEqual(summary['findings'], [{'id': 'RUSTSEC-2026-0391', 'kind': 'vulnerability', 'crate': 'zstd', 'version': '0.13.3'}])
        self.assertEqual(summary['cleanup'], [{'identity': 'RUSTSEC-2020-0001', 'code': 'advisory-not-detected'}])
        self.assertEqual((output / 'cargo-deny.stderr').read_bytes(), raw)

    def test_input_error_and_mixed_diagnostics(self):
        result, summary, _, _ = self.invoke([ERROR], 1)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(summary['outcome'], 'input-error')
        self.assertEqual(summary['findings'], [])
        self.assertEqual(summary['input_errors'], [ERROR['fields']['message']])
        index = {'type': 'diagnostic', 'fields': {'severity': 'warning', 'code': 'index-failure', 'message': 'index unavailable'}}
        _, summary, _, _ = self.invoke([ERROR, index, VULNERABILITY], 3, 'mixed')
        self.assertEqual(summary['outcome'], 'findings')
        self.assertEqual(summary['input_errors'], [ERROR['fields']['message'], 'index unavailable'])

    def test_database_identity_and_ambiguous_or_missing_database(self):
        _, summary, _, _ = self.invoke(label='missing')
        self.assertEqual(summary['advisory_db']['status'], 'unknown')
        db = self.home / 'advisory-dbs/advisory-db-one'
        db.mkdir(parents=True)
        for args in (['init', '-q'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                     'commit', '--allow-empty', '-qm', 'fixture']):
            subprocess.run(['git', '-C', str(db), *args], check=True, capture_output=True)
        (db / '.git/FETCH_HEAD').write_text('fixture fetch')
        _, summary, _, _ = self.invoke(label='one')
        expected = subprocess.check_output(['git', '-C', str(db), 'rev-parse', 'HEAD'], text=True).strip()
        self.assertEqual(summary['advisory_db']['commit'], expected)
        self.assertTrue(summary['advisory_db']['committed_at'])
        self.assertTrue(summary['advisory_db']['fetched_at'])
        (db.parent / 'advisory-db-two').mkdir()
        _, summary, _, _ = self.invoke(label='ambiguous')
        self.assertEqual(summary['advisory_db']['status'], 'unknown')

    def test_nonempty_output_is_not_overwritten(self):
        output = self.root / 'occupied'
        output.mkdir()
        sentinel = output / 'sentinel'
        sentinel.write_bytes(b'original')
        result = subprocess.run([sys.executable, str(SCRIPT), '--output', str(output)], capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(list(output.iterdir()), [sentinel])
        self.assertEqual(sentinel.read_bytes(), b'original')


if __name__ == '__main__':
    unittest.main()
