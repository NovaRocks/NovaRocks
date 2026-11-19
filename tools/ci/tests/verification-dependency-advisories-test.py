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
"""CI owner: the local advisory command preserves native results.

Local unittest fixtures project cargo-deny 0.20.2 JSON diagnostics and the
RustSec advisory TOML header. Subprocess stubs provide independent exit and
output oracles without network or shared state. TemporaryDirectory owns the
stub repository, cargo home, and outputs through actual exit.
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
SPEC = importlib.util.spec_from_file_location('dependency_advisories', SCRIPT)
COMMAND = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(COMMAND)

VULNERABILITY = {'type': 'diagnostic', 'fields': {
    'severity': 'error', 'code': 'vulnerability',
    'advisory': {'id': 'RUSTSEC-2026-0391', 'package': 'zstd'},
    'graphs': [{'Krate': {'name': 'zstd', 'version': '0.13.3'}}]}}
CLEANUP = {'type': 'diagnostic', 'fields': {
    'severity': 'error', 'code': 'advisory-not-detected',
    'labels': [{'span': 'RUSTSEC-2020-0001'}]}}
ERROR = {'type': 'log', 'fields': {'level': 'ERROR', 'message': 'failed to fetch advisory database'}}
UNKNOWN_DB = {'class': 'unknown', 'candidate': None, 'patched': None, 'unaffected': None,
              'reason': 'Expected one advisory database, found 0'}
CHECK = 'deny --format json --locked check advisories'


def openssl_finding(advisory_id='RUSTSEC-2026-0463', version='0.10.75', parents=None):
    return {'type': 'diagnostic', 'fields': {
        'severity': 'error', 'code': 'vulnerability',
        'advisory': {'id': advisory_id, 'package': 'openssl'},
        'graphs': [{'Krate': {'name': 'openssl', 'version': version},
                    'parents': [{'Krate': parent} for parent in (parents or [])]}]}}


class Advisories(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='dependency-advisories-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / 'repo'
        self.repo.mkdir()
        (self.repo / 'Cargo.lock').write_text('# fixture lock\nversion = 3\n')
        (self.repo / 'deny.toml').write_text('[advisories]\n')
        workflow = self.repo / '.github/workflows'
        workflow.mkdir(parents=True)
        (workflow / 'admission.yml').write_text('env:\n  NOVA_CI_CARGO_DENY_VERSION: "0.20.2"\n')
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.home = self.root / 'cargo-home'
        self.home.mkdir()
        cargo = self.bin / 'cargo'
        cargo.write_text('''#!/bin/sh
printf '%s\\n' "$*" >> "$CARGO_MARKER"
case "$*" in
  'deny --version')
    echo "${CARGO_DENY_VERSION:-cargo-deny 0.20.2}"
    exit 0
    ;;
  'update --dry-run')
    if [ -n "$CARGO_UPDATE_FAIL" ]; then echo 'update failed' >&2; exit 1; fi
    if [ -n "$CARGO_UPDATE_STDERR" ]; then cat "$CARGO_UPDATE_STDERR" >&2; fi
    if [ -n "$CARGO_MUTATE_LOCK" ]; then printf 'mutated\\n' >> "$CARGO_MUTATE_LOCK"; fi
    exit 0
    ;;
  'deny --format json --locked check advisories')
    if [ -n "$CARGO_STDERR" ]; then cat "$CARGO_STDERR" >&2; fi
    printf 'raw stdout\\n'
    exit "${CARGO_EXIT:-0}"
    ;;
  *)
    echo "unexpected cargo invocation: $*" >&2
    exit 99
    ;;
esac
''')
        cargo.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                        CARGO_HOME=str(self.home), CARGO_MARKER=str(self.root / 'marker'))

    def invoke(self, records=(), code=0, label='run', updates='', **env):
        raw = ''.join(json.dumps(record) + '\n' for record in records).encode()
        stderr = self.root / (label + '.input')
        stderr.write_bytes(raw)
        updates_path = self.root / (label + '.updates')
        updates_path.write_text(updates)
        output = self.root / label
        merged = dict(self.env, CARGO_STDERR=str(stderr), CARGO_EXIT=str(code),
                      CARGO_UPDATE_STDERR=str(updates_path))
        merged.update(env)
        result = subprocess.run([sys.executable, str(SCRIPT), '--repo', str(self.repo),
                                 '--output', str(output)], env=merged, capture_output=True)
        summary = json.loads((output / 'summary.json').read_text()) if (output / 'summary.json').is_file() else None
        return result, summary, output, raw

    def marker(self):
        path = self.root / 'marker'
        return path.read_text().splitlines() if path.is_file() else []

    def ensure_database(self):
        database = self.home / 'advisory-dbs/advisory-db-one'
        if not (database / '.git').exists():
            database.mkdir(parents=True, exist_ok=True)
            for args in (['init', '-q'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
                         'commit', '--allow-empty', '-qm', 'fixture']):
                subprocess.run(['git', '-C', str(database), *args], check=True, capture_output=True)
        return database

    def write_advisory(self, body, package='openssl', advisory_id='RUSTSEC-2026-0463'):
        database = self.ensure_database()
        directory = database / 'crates' / package
        directory.mkdir(parents=True, exist_ok=True)
        (directory / ('%s.md' % advisory_id)).write_text(
            '```toml\n[advisory]\nid = "%s"\npackage = "%s"\n\n%s\n```\n' % (advisory_id, package, body))
        return database

    def test_pass(self):
        result, summary, output, _ = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(summary['schema'], 2)
        self.assertEqual(summary['outcome'], 'pass')
        self.assertEqual(summary['cargo_deny'], {'declared': '0.20.2', 'actual': 'cargo-deny 0.20.2'})
        self.assertTrue(summary['inputs_unchanged'])
        self.assertEqual(summary['index_refresh']['status'], 'ok')
        self.assertEqual((output / 'exit').read_text(), '0\n')
        self.assertEqual(self.marker(), ['deny --version', 'update --dry-run', CHECK])
        self.assertEqual((output / 'cargo-deny.stdout').read_bytes(), b'raw stdout\n')

    def test_findings_cleanup_and_raw_bytes(self):
        result, summary, output, raw = self.invoke([VULNERABILITY, CLEANUP], 1)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(summary['outcome'], 'findings')
        self.assertEqual(summary['findings'], [{
            'id': 'RUSTSEC-2026-0391', 'kind': 'vulnerability', 'crate': 'zstd', 'version': '0.13.3',
            'parents': [], 'fix': UNKNOWN_DB}])
        self.assertEqual(summary['cleanup'], [{'identity': 'RUSTSEC-2020-0001', 'code': 'advisory-not-detected'}])
        self.assertEqual((output / 'cargo-deny.stderr').read_bytes(), raw)

    def test_input_error_and_mixed_diagnostics(self):
        result, summary, _, _ = self.invoke([ERROR], 1)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(summary['outcome'], 'input-error')
        self.assertEqual(summary['findings'], [])
        self.assertEqual(summary['input_errors'], [ERROR['fields']['message']])
        index = {'type': 'diagnostic', 'fields': {'severity': 'warning', 'code': 'index-failure',
                                                   'message': 'index unavailable'}}
        _, summary, _, _ = self.invoke([ERROR, index, VULNERABILITY], 3, 'mixed')
        self.assertEqual(summary['outcome'], 'findings')
        self.assertEqual(summary['input_errors'], [ERROR['fields']['message'], 'index unavailable'])
        self.assertEqual(summary['findings'][0]['fix'], UNKNOWN_DB)

    def test_database_identity_without_fetch_head_and_ambiguous_database(self):
        _, summary, _, _ = self.invoke(label='missing')
        self.assertIsNone(summary['advisory_db']['commit'])
        self.assertIn('path', summary['advisory_db']['unknown'])
        database = self.ensure_database()
        _, summary, _, _ = self.invoke(label='one')
        expected = subprocess.check_output(['git', '-C', str(database), 'rev-parse', 'HEAD'], text=True).strip()
        self.assertEqual(summary['advisory_db']['commit'], expected)
        self.assertTrue(summary['advisory_db']['committed_at'])
        self.assertIsNone(summary['advisory_db']['fetched_at'])
        self.assertIn('fetched_at', summary['advisory_db']['unknown'])
        self.assertNotIn('commit', summary['advisory_db']['unknown'])
        (database / '.git/FETCH_HEAD').write_text('fixture fetch')
        _, summary, _, _ = self.invoke(label='fetched')
        self.assertEqual(summary['advisory_db']['commit'], expected)
        self.assertTrue(summary['advisory_db']['fetched_at'])
        (database.parent / 'advisory-db-two').mkdir()
        _, summary, _, _ = self.invoke(label='ambiguous')
        self.assertIsNone(summary['advisory_db']['path'])
        self.assertIsNone(summary['advisory_db']['commit'])
        self.assertIn('found 2', summary['advisory_db']['unknown']['path'])

    def test_nonempty_output_is_not_overwritten(self):
        output = self.root / 'occupied'
        output.mkdir()
        sentinel = output / 'sentinel'
        sentinel.write_bytes(b'original')
        result = subprocess.run([sys.executable, str(SCRIPT), '--output', str(output)], capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(list(output.iterdir()), [sentinel])
        self.assertEqual(sentinel.read_bytes(), b'original')

    def test_version_mismatch_or_missing_declaration_skips_check(self):
        result, summary, output, _ = self.invoke(label='mismatch', CARGO_DENY_VERSION='cargo-deny 0.19.0')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(summary['outcome'], 'input-error')
        self.assertIn('cargo install cargo-deny --version 0.20.2 --locked', summary['input_errors'][0])
        self.assertEqual(self.marker(), ['deny --version'])
        self.assertFalse((output / 'cargo-deny.stderr').exists())
        self.assertFalse((output / 'cargo-update.stderr').exists())

        (self.root / 'marker').write_text('')
        admission = self.repo / '.github/workflows/admission.yml'
        admission.unlink()
        result, summary, _, _ = self.invoke(label='missing-declaration')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('missing', summary['input_errors'][0])
        self.assertNotIn('update --dry-run', self.marker())
        self.assertNotIn(CHECK, self.marker())

        (self.root / 'marker').write_text('')
        admission.write_text('NOVA_CI_CARGO_DENY_VERSION: "0.20.2"\nNOVA_CI_CARGO_DENY_VERSION: "0.20.2"\n')
        result, summary, _, _ = self.invoke(label='duplicate-declaration')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn('duplicated', summary['input_errors'][0])
        self.assertNotIn(CHECK, self.marker())

    def test_refresh_failure_skips_check(self):
        result, summary, output, _ = self.invoke(label='refresh-failed', CARGO_UPDATE_FAIL='1')
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertEqual(summary['outcome'], 'input-error')
        self.assertEqual(summary['index_refresh']['status'], 'failed')
        self.assertIn('index refresh failed', summary['input_errors'])
        self.assertEqual(self.marker(), ['deny --version', 'update --dry-run'])
        self.assertTrue((output / 'cargo-update.stderr').is_file())
        self.assertFalse((output / 'cargo-deny.stderr').exists())

    def test_direct_parents_come_from_the_graph(self):
        self.ensure_database()
        record = openssl_finding(parents=[
            {'name': 'novarocks-functions', 'version': '0.1.0'},
            {'name': 'tempfile', 'version': '3.0.0', 'nested': True}])
        record['fields']['graphs'][0]['parents'][1]['parents'] = [
            {'Krate': {'name': 'ignored-grandparent', 'version': '1.0.0'}}]
        _, summary, _, _ = self.invoke([record], 1, label='parents')
        self.assertEqual(summary['findings'][0]['parents'], ['novarocks-functions', 'tempfile'])

    def test_fix_classes(self):
        header = '[versions]\npatched = [">= 0.10.78"]\nunaffected = ["< 0.10.24"]\n'
        self.write_advisory(header)
        record = openssl_finding(parents=[{'name': 'novarocks-functions', 'version': '0.1.0'}])
        _, summary, _, _ = self.invoke([record], 1, 'compatible', '    Updating openssl v0.10.75 -> v0.10.82\n')
        fix = summary['findings'][0]['fix']
        self.assertEqual(fix['class'], 'compatible')
        self.assertEqual(fix['candidate'], '0.10.82')
        self.assertEqual(fix['patched'], ['>= 0.10.78'])

        _, summary, _, _ = self.invoke([record], 1, 'no-candidate', '    Updating unrelated v1.0.0 -> v1.0.1\n')
        self.assertEqual(summary['findings'][0]['fix']['class'], 'major-or-exception')
        self.assertIsNone(summary['findings'][0]['fix']['candidate'])

        _, summary, _, _ = self.invoke([record], 1, 'still-affected', '    Updating openssl v0.10.75 -> v0.10.76\n')
        still = summary['findings'][0]['fix']
        self.assertEqual(still['class'], 'major-or-exception')
        self.assertEqual(still['candidate'], '0.10.76')

        self.write_advisory('[versions]\npatched = [">= *"]\n')
        _, summary, _, _ = self.invoke([record], 1, 'unparsed', '    Updating openssl v0.10.75 -> v0.10.82\n')
        self.assertEqual(summary['findings'][0]['fix']['class'], 'unknown')

        self.write_advisory('[advisory]\nid = "RUSTSEC-2026-0463"\n')
        _, summary, _, _ = self.invoke([record], 1, 'no-ranges', '    Updating openssl v0.10.75 -> v0.10.82\n')
        self.assertEqual(summary['findings'][0]['fix']['class'], 'major-or-exception')

        (self.home / 'advisory-dbs/advisory-db-two').mkdir()
        _, summary, _, _ = self.invoke([record], 1, 'ambiguous-fix', '    Updating openssl v0.10.75 -> v0.10.82\n')
        self.assertEqual(summary['findings'][0]['fix']['class'], 'unknown')

    def test_yanked_with_candidate_is_compatible(self):
        self.ensure_database()
        yanked = {'type': 'diagnostic', 'fields': {
            'severity': 'error', 'code': 'yanked',
            'graphs': [{'Krate': {'name': 'left-pad', 'version': '1.0.0'},
                        'parents': [{'Krate': {'name': 'app', 'version': '0.1.0'}}]}]}}
        _, summary, _, _ = self.invoke([yanked], 1, 'yanked', '    Updating left-pad v1.0.0 -> v1.0.1\n')
        self.assertEqual(summary['findings'][0]['fix']['class'], 'compatible')
        self.assertEqual(summary['findings'][0]['fix']['candidate'], '1.0.1')
        _, summary, _, _ = self.invoke([yanked], 1, 'yanked-none', '')
        self.assertEqual(summary['findings'][0]['fix']['class'], 'major-or-exception')

    def test_changed_lock_is_an_input_error_without_replacing_the_exit_code(self):
        result, summary, _, _ = self.invoke(label='mutated', CARGO_MUTATE_LOCK=str(self.repo / 'Cargo.lock'))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(summary['inputs_unchanged'])
        self.assertEqual(summary['outcome'], 'input-error')
        self.assertIn('Cargo.lock or deny.toml changed during the run', summary['input_errors'])
        result, summary, _, _ = self.invoke([VULNERABILITY], 1, 'mutated-findings',
                                            CARGO_MUTATE_LOCK=str(self.repo / 'Cargo.lock'))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(summary['outcome'], 'findings')
        self.assertFalse(summary['inputs_unchanged'])
        self.assertIn('Cargo.lock or deny.toml changed during the run', summary['input_errors'])


class VersionRequirements(unittest.TestCase):
    def test_requirement_table(self):
        cases = [
            ('1.2.3', '>=1.2.3', True),
            ('1.2.2', '>= 1.2.3', False),
            ('1.2.9', '>=1.2', True),
            ('1.1.9', '>=1.2', False),
            ('1.2.9', '>= 1.2.0, < 1.3.0', True),
            ('1.3.0', '>= 1.2.0, < 1.3.0', False),
            ('1.2.2', '<1.2.3', True),
            ('1.2.3', '<1.2.3', False),
            ('1.1.9', '<1.2', True),
            ('1.2.0', '<1.2', False),
            ('1.2.3', '=1.2.3', True),
            ('1.2.4', '=1.2.3', False),
            ('1.2.9', '=1.2', True),
            ('1.3.0', '=1.2', False),
            ('1.2.3', '~1.2.3', True),
            ('1.2.9', '~1.2.3', True),
            ('1.3.0', '~1.2.3', False),
            ('1.2.0', '~1.2', True),
            ('1.3.0', '~1.2', False),
            ('1.9.0', '~1', True),
            ('2.0.0', '~1', False),
            ('1.2.3', '^1.2.3', True),
            ('1.2.4', '^1.2.3', True),
            ('1.9.0', '^1.2.3', True),
            ('2.0.0', '^1.2.3', False),
            ('1.2.2', '^1.2.3', False),
            ('0.2.3', '^0.2.3', True),
            ('0.2.9', '^0.2.3', True),
            ('0.3.0', '^0.2.3', False),
            ('0.0.3', '^0.0.3', True),
            ('0.0.4', '^0.0.3', False),
            ('1.2.0', '^1.2', True),
            ('1.9.0', '^1.2', True),
            ('1.1.9', '^1.2', False),
            ('0.2.9', '^0.2', True),
            ('0.3.0', '^0.2', False),
            ('1.9.0', '^1', True),
            ('2.0.0', '^1', False),
            ('1.2.4', '1.2.3', True),
            ('1.2.3-alpha.1', '>=1.0.0', None),
            ('1.2.3', '>=1.*', None),
            ('1.2.3', '*', None),
            ('1.2.3', '>=1.2.3-alpha', None),
            ('1.2.3', '>=1.2.3, >=*', None),
        ]
        for version, requirement, expected in cases:
            with self.subTest(version=version, requirement=requirement):
                self.assertIs(COMMAND.requirement_matches(version, requirement), expected)


if __name__ == '__main__':
    unittest.main()
