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
"""Preserve cargo-deny's result and summarize latest advisory discovery."""
# Design: ADR-0200 (docs/adr/ADR-0200-advisory-discovery-is-independent-of-admission.md)
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys

REPO = Path(__file__).resolve().parents[2]
CLEANUP_CODES = {'advisory-not-detected', 'yanked-not-detected'}


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def advisory_database():
    home = Path(os.environ.get('CARGO_HOME') or Path.home() / '.cargo').expanduser()
    candidates = sorted(p for p in (home / 'advisory-dbs').glob('advisory-db-*') if p.is_dir())
    if len(candidates) != 1:
        return {'status': 'unknown', 'reason': f'Expected one advisory database, found {len(candidates)}'}
    db = candidates[0]
    try:
        def git(*args):
            return subprocess.check_output(['git', '-C', str(db), *args], stderr=subprocess.PIPE, text=True).strip()
        commit = git('rev-parse', 'HEAD')
        if not re.fullmatch(r'[0-9a-f]{40}', commit):
            raise ValueError('Advisory database HEAD is not a full commit')
        committed_at = git('show', '-s', '--format=%cI', 'HEAD')
        fetch_head = Path(git('rev-parse', '--git-path', 'FETCH_HEAD'))
        if not fetch_head.is_absolute():
            fetch_head = db / fetch_head
        fetched_at = datetime.fromtimestamp(fetch_head.stat().st_mtime, timezone.utc).isoformat()
        return {'commit': commit, 'committed_at': committed_at, 'fetched_at': fetched_at}
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return {'status': 'unknown', 'reason': str(error)}


def summarize(stderr, code):
    findings, cleanup, input_errors = [], [], []
    has_error = False
    for line in stderr.decode('utf-8', errors='replace').splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue  # Raw output remains available even when it is not JSON.
        fields = record.get('fields', {})
        if str(fields.get('level', '')).upper() == 'ERROR':
            input_errors.append(fields.get('message', line))
        if record.get('type') != 'diagnostic':
            continue
        kind = fields.get('code')
        if kind == 'index-failure':
            input_errors.append(fields.get('message', line))
        if fields.get('severity') != 'error':
            continue
        has_error = True
        if kind in CLEANUP_CODES:
            # cargo-deny labels carry the exact ignored advisory/package identity.
            identity = fields.get('labels', [{}])[0].get('span') if fields.get('labels') else None
            cleanup.append({'identity': identity, 'code': kind})
        else:
            advisory = fields.get('advisory') or {}
            graphs = fields.get('graphs') or [{}]
            crate = graphs[0].get('Krate', {})
            findings.append({'id': advisory.get('id'), 'kind': kind,
                             'crate': crate.get('name') or advisory.get('package'),
                             'version': crate.get('version')})
    outcome = 'pass' if code == 0 else 'findings' if has_error else 'input-error'
    return {'schema': 1, 'outcome': outcome, 'findings': findings,
            'cleanup': cleanup, 'input_errors': input_errors}


def markdown(summary):
    def cell(value):
        return str(value if value is not None else 'unknown').replace('|', '\\|').replace('\n', ' ')
    lines = ['# Cargo dependency advisories', '', f"Outcome: **{summary['outcome']}**",
             f"Started: {summary['started_at']}", f"Tool: {summary['cargo_deny_version']}", '',
             '## Findings', '', '| ID | Kind | Crate | Version |', '|---|---|---|---|']
    lines += ['| ' + ' | '.join(cell(item[key]) for key in ('id', 'kind', 'crate', 'version')) + ' |'
              for item in summary['findings']]
    lines += ['', '## Exceptions to clean up', '']
    lines += [f"- {cell(item['identity'])}: {item['code']}" for item in summary['cleanup']] or ['None.']
    lines += ['', '## Input errors', '']
    lines += ['- ' + cell(item) for item in summary['input_errors']] or ['None.']
    lines += ['', '## Advisory database', '', '```json', json.dumps(summary['advisory_db'], indent=2), '```', '']
    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    output = args.output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('output directory must be empty; previous results are never overwritten')
    started_at = timestamp()
    launch_error = None
    try:
        version = subprocess.run(['cargo', 'deny', '--version'], cwd=REPO, capture_output=True, text=True)
        tool = version.stdout.strip() or version.stderr.strip()
        if version.returncode != 0:
            raise OSError('cargo-deny is unavailable: ' + tool)
        with (output / 'cargo-deny.stdout').open('xb') as stdout, (output / 'cargo-deny.stderr').open('xb') as stderr:
            result = subprocess.run(['cargo', 'deny', '--format', 'json', '--locked', 'check', 'advisories'],
                                    cwd=REPO, stdout=stdout, stderr=stderr)
        code = result.returncode
    except OSError as error:
        code, tool, launch_error = 2, 'unavailable', str(error)
        for name in ('cargo-deny.stdout', 'cargo-deny.stderr'):
            (output / name).touch(exist_ok=True)
    (output / 'exit').write_text(str(code) + '\n')
    summary = summarize((output / 'cargo-deny.stderr').read_bytes(), code)
    if launch_error:
        summary['outcome'] = 'input-error'
        summary['input_errors'].append(launch_error)
    summary.update(started_at=started_at, cargo_deny_version=tool, advisory_db=advisory_database())
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    rendered = markdown(summary)
    (output / 'summary.md').write_text(rendered)
    print(rendered, end='')
    return code


if __name__ == '__main__':
    sys.exit(main())
