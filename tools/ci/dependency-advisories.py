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
"""Local advisory check: refresh the index, then summarize cargo-deny."""
# Design: ADR-0240 (docs/adr/ADR-0240-advisory-discovery-is-a-manual-local-check.md)
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

REPO = Path(__file__).resolve().parents[2]
CLEANUP_CODES = {'advisory-not-detected', 'yanked-not-detected'}
DECLARED_VERSION = re.compile(r'NOVA_CI_CARGO_DENY_VERSION:\s*"([^"]+)"')
UPDATE_LINE = re.compile(r'Updating\s+(\S+)\s+v(\S+)\s+->\s+v(\S+)')
REQUIREMENT_CLAUSE = re.compile(r'^(>=|<=|\^|~|=|>|<)?\s*(\d+(?:\.\d+){0,2})$')
TOML_FENCE = re.compile(r'```toml\s*\n(.*?)```', re.S)
VERSIONS_SECTION = re.compile(r'(?m)^\[versions\]\s*\n(.*?)(?=^\[|\Z)', re.S)
QUOTED = re.compile(r'"(?:\\.|[^"\\])*"')


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def input_fingerprint(repo):
    paths = [repo / 'Cargo.lock', repo / 'deny.toml']
    if any(not path.is_file() for path in paths):
        return None
    return tuple(sha256_file(path) for path in paths)


def declared_deny_version(repo):
    path = repo / '.github/workflows/admission.yml'
    if not path.is_file():
        return None, 'missing'
    found = DECLARED_VERSION.findall(path.read_text(encoding='utf-8', errors='replace'))
    if len(found) != 1:
        return None, 'missing' if not found else 'duplicated'
    return found[0], None


def advisory_database():
    home = Path(os.environ.get('CARGO_HOME') or Path.home() / '.cargo').expanduser()
    candidates = sorted(path for path in (home / 'advisory-dbs').glob('advisory-db-*') if path.is_dir())
    record = {'path': None, 'url': None, 'commit': None, 'committed_at': None,
              'fetched_at': None, 'unknown': {}}
    if len(candidates) != 1:
        record['unknown']['path'] = 'Expected one advisory database, found %d' % len(candidates)
        return record
    database = candidates[0]
    record['path'] = str(database)

    def git(*args):
        return subprocess.check_output(['git', '-C', str(database), *args],
                                       stderr=subprocess.PIPE, text=True).strip()

    try:
        commit = git('rev-parse', 'HEAD')
        if not re.fullmatch(r'[0-9a-f]{40}', commit):
            raise ValueError('Advisory database HEAD is not a full commit')
        record['commit'] = commit
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        record['unknown']['commit'] = str(error)
    try:
        record['committed_at'] = git('show', '-s', '--format=%cI', 'HEAD')
    except (OSError, subprocess.SubprocessError) as error:
        record['unknown']['committed_at'] = str(error)
    try:
        fetch_head = Path(git('rev-parse', '--git-path', 'FETCH_HEAD'))
        if not fetch_head.is_absolute():
            fetch_head = database / fetch_head
        record['fetched_at'] = datetime.fromtimestamp(fetch_head.stat().st_mtime, timezone.utc).isoformat()
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        record['unknown']['fetched_at'] = str(error)
    try:
        record['url'] = git('remote', 'get-url', 'origin')
    except (OSError, subprocess.SubprocessError) as error:
        record['unknown']['url'] = str(error)
    return record


def database_is_unique(database):
    return 'path' not in database['unknown']


def krate_name(node):
    krate = node.get('Krate') if isinstance(node, dict) else None
    if isinstance(krate, dict) and krate.get('name'):
        return krate['name']
    feature = node.get('Feature') if isinstance(node, dict) else None
    if isinstance(feature, dict):
        return feature.get('crate_name')
    return None


def direct_parents(graphs):
    names = []
    for graph in graphs or []:
        if not isinstance(graph, dict):
            continue
        for parent in graph.get('parents') or []:
            name = krate_name(parent)
            if name and name not in names:
                names.append(name)
    return names


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
            labels = fields.get('labels') or [{}]
            identity = labels[0].get('span') if labels else None
            cleanup.append({'identity': identity, 'code': kind})
        else:
            advisory = fields.get('advisory') or {}
            graphs = fields.get('graphs') or [{}]
            first = graphs[0] if graphs else {}
            crate = first.get('Krate', {}) if isinstance(first, dict) else {}
            findings.append({'id': advisory.get('id'), 'kind': kind,
                             'crate': crate.get('name') or advisory.get('package'),
                             'version': crate.get('version'),
                             'parents': direct_parents(graphs)})
    outcome = 'pass' if code == 0 else 'findings' if has_error else 'input-error'
    return {'schema': 2, 'outcome': outcome, 'findings': findings,
            'cleanup': cleanup, 'input_errors': input_errors}


def parse_updates(stderr):
    candidates = {}
    for line in stderr.splitlines():
        match = UPDATE_LINE.search(line)
        if match:
            candidates[(match.group(1), match.group(2))] = match.group(3)
    return candidates


def candidate_records(candidates):
    return [{'name': name, 'from': old, 'to': new}
            for (name, old), new in sorted(candidates.items())]


def parse_candidate(version):
    if version is None or '-' in version or re.search(r'[*xX]', version):
        return None
    base = version.split('+', 1)[0]
    parts = base.split('.')
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        return None
    return tuple(int(part) for part in parts)


def parse_requirement(requirement):
    clauses = []
    for raw in requirement.split(','):
        text = raw.strip()
        if not text or '-' in text or '+' in text or re.search(r'[*xX]', text):
            return None
        match = REQUIREMENT_CLAUSE.match(text)
        if not match:
            return None
        numbers = [int(part) for part in match.group(2).split('.')]
        minor = numbers[1] if len(numbers) > 1 else None
        patch = numbers[2] if len(numbers) > 2 else None
        clauses.append((match.group(1) or '^', numbers[0], minor, patch))
    return clauses or None


def _exact(major, minor, patch, cmaj, cmin, cpatch):
    if cmaj != major:
        return False
    if minor is not None and cmin != minor:
        return False
    if patch is not None and cpatch != patch:
        return False
    return True


def _greater(major, minor, patch, cmaj, cmin, cpatch):
    if cmaj != major:
        return cmaj > major
    if minor is None:
        return False
    if cmin != minor:
        return cmin > minor
    if patch is None or cpatch == patch:
        return False
    return cpatch > patch


def _less(major, minor, patch, cmaj, cmin, cpatch):
    if cmaj != major:
        return cmaj < major
    if minor is None:
        return False
    if cmin != minor:
        return cmin < minor
    if patch is None or cpatch == patch:
        return False
    return cpatch < patch


def _tilde(major, minor, patch, cmaj, cmin, cpatch):
    if cmaj != major:
        return False
    if minor is not None and cmin != minor:
        return False
    if patch is not None and cpatch != patch:
        return cpatch > patch
    return True


def _caret(major, minor, patch, cmaj, cmin, cpatch):
    if cmaj != major:
        return False
    if minor is None:
        return True
    if patch is None:
        return cmin >= minor if major > 0 else cmin == minor
    if major > 0:
        if cmin != minor:
            return cmin > minor
        if cpatch != patch:
            return cpatch > patch
        return True
    if minor > 0:
        if cmin != minor:
            return False
        if cpatch != patch:
            return cpatch > patch
        return True
    return cmin == minor and cpatch == patch


def requirement_matches(version, requirement):
    """Return True, False, or None when the requirement cannot be decided."""
    candidate = parse_candidate(version)
    clauses = parse_requirement(requirement)
    if candidate is None or clauses is None:
        return None
    predicates = {'=': _exact, '>': _greater, '>=': lambda *args: _exact(*args) or _greater(*args),
                  '<': _less, '<=': lambda *args: _exact(*args) or _less(*args),
                  '~': _tilde, '^': _caret}
    for op, major, minor, patch in clauses:
        if not predicates[op](major, minor, patch, *candidate):
            return False
    return True


def matches_any(version, requirements):
    unknown = False
    for requirement in requirements:
        decision = requirement_matches(version, requirement)
        if decision is True:
            return True
        if decision is None:
            unknown = True
    return None if unknown else False


def _array_strings(body, key):
    match = re.search(r'(?m)^%s\s*=\s*\[(.*?)\]' % key, body, re.S)
    if not match:
        return []
    inner = match.group(1)
    values = [value[1:-1] for value in QUOTED.findall(inner)]
    leftover = QUOTED.sub('', inner)
    if re.search(r'\S', re.sub(r'[,\s]', '', leftover)):
        return None
    return values


def read_ranges(database, package, advisory_id):
    if not package or not advisory_id or not database.get('path'):
        return None, 'advisory identity or database path is missing'
    path = Path(database['path']) / 'crates' / package / ('%s.md' % advisory_id)
    if not path.is_file():
        return None, 'advisory file is missing: %s' % path.name
    text = path.read_text(encoding='utf-8', errors='replace')
    fenced = TOML_FENCE.search(text)
    if not fenced:
        return None, 'advisory file has no toml header: %s' % path.name
    section = VERSIONS_SECTION.search(fenced.group(1))
    if not section:
        return {'patched': [], 'unaffected': []}, None
    patched = _array_strings(section.group(1), 'patched')
    unaffected = _array_strings(section.group(1), 'unaffected')
    if patched is None or unaffected is None:
        return None, 'advisory version ranges could not be parsed: %s' % path.name
    return {'patched': patched, 'unaffected': unaffected}, None


def fix_result(kind, candidate, patched, unaffected, reason):
    return {'class': kind, 'candidate': candidate, 'patched': patched,
            'unaffected': unaffected, 'reason': reason}


def classify_fix(finding, candidates, database):
    if not database_is_unique(database):
        return fix_result('unknown', None, None, None, database['unknown']['path'])
    candidate = candidates.get((finding.get('crate'), finding.get('version')))
    if finding.get('kind') == 'yanked':
        if candidate:
            return fix_result('compatible', candidate, None, None,
                              'yanked crate has a compatible update candidate')
        return fix_result('major-or-exception', None, None, None,
                          'yanked crate has no newer compatible version')
    ranges, error = read_ranges(database, finding.get('crate'), finding.get('id'))
    if error:
        return fix_result('unknown', candidate, None, None, error)
    patched = ranges['patched']
    unaffected = ranges['unaffected']
    if candidate is None:
        return fix_result('major-or-exception', None, patched, unaffected,
                          'no newer compatible version in the index refresh')
    decision = matches_any(candidate, patched + unaffected)
    if decision is True:
        return fix_result('compatible', candidate, patched, unaffected,
                          'candidate satisfies a patched or unaffected requirement')
    if decision is None:
        return fix_result('unknown', candidate, patched, unaffected,
                          'candidate could not be compared with the advisory ranges')
    return fix_result('major-or-exception', candidate, patched, unaffected,
                      'candidate does not satisfy patched or unaffected requirements')


def markdown(summary):
    def cell(value):
        return str(value if value is not None else 'unknown').replace('|', '\\|').replace('\n', ' ')

    tool = summary['cargo_deny']
    lines = ['# Dependency advisory check', '', 'Outcome: **%s**' % summary['outcome'],
             'Started: %s' % summary['started_at'],
             'Tool: declared %s, actual %s' % (cell(tool['declared']), cell(tool['actual'])),
             'Inputs unchanged: %s' % summary['inputs_unchanged'], '',
             '## Findings', '',
             '| ID | Kind | Crate | Version | Parents | Fix |',
             '|---|---|---|---|---|---|']
    for item in summary['findings']:
        fix = item.get('fix') or {}
        shown = fix.get('class')
        if fix.get('candidate'):
            shown = '%s %s' % (shown, fix['candidate'])
        lines.append('| ' + ' | '.join(cell(value) for value in (
            item.get('id'), item.get('kind'), item.get('crate'), item.get('version'),
            ', '.join(item.get('parents') or []) or None, shown)) + ' |')
    lines += ['', '## Fix details', '']
    if summary['findings']:
        for item in summary['findings']:
            fix = item.get('fix') or {}
            lines.append('- %s: %s' % (cell(item.get('id')), cell(fix.get('reason'))))
    else:
        lines.append('None.')
    lines += ['', '## Exceptions to clean up', '']
    lines += ['- %s: %s' % (cell(item['identity']), item['code']) for item in summary['cleanup']] or ['None.']
    lines += ['', '## Input errors', '']
    lines += ['- ' + cell(item) for item in summary['input_errors']] or ['None.']
    lines += ['', '## Advisory database', '', '```json',
              json.dumps(summary['advisory_db'], indent=2), '```', '',
              '## Index refresh', '', '```json',
              json.dumps(summary['index_refresh'], indent=2), '```', '']
    return '\n'.join(lines)


def prepare_output(path):
    if path.exists() and any(path.iterdir()):
        print('output directory must be empty; previous results are never overwritten', file=sys.stderr)
        return False
    path.mkdir(parents=True, exist_ok=True)
    return True


def run_to_files(command, cwd, stdout_path, stderr_path):
    with stdout_path.open('xb') as stdout, stderr_path.open('xb') as stderr:
        return subprocess.run(command, cwd=str(cwd), stdout=stdout, stderr=stderr).returncode


def emit(output, code, summary):
    (output / 'exit').write_text('%s\n' % code)
    (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    rendered = markdown(summary)
    (output / 'summary.md').write_text(rendered)
    print(rendered, end='')
    return code


def empty_summary(started_at, declared, actual, database, refresh, inputs_unchanged):
    return {'schema': 2, 'outcome': 'input-error', 'findings': [], 'cleanup': [],
            'input_errors': [], 'started_at': started_at,
            'cargo_deny': {'declared': declared, 'actual': actual},
            'advisory_db': database, 'index_refresh': refresh,
            'inputs_unchanged': inputs_unchanged}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=REPO)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    repo = args.repo.expanduser().resolve()
    output = args.output.expanduser().resolve() if args.output else (
        repo / 'logs' / 'dependency-advisories' /
        datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    if not prepare_output(output):
        return 2

    started_at = timestamp()
    before = input_fingerprint(repo)
    declared, declaration_problem = declared_deny_version(repo)
    actual = None
    try:
        version = subprocess.run(['cargo', 'deny', '--version'], cwd=str(repo),
                                 capture_output=True, text=True)
        actual = version.stdout.strip() or version.stderr.strip() or 'unavailable'
        if version.returncode != 0:
            actual = actual or 'unavailable'
            version_error = 'cargo-deny is unavailable: ' + actual
        else:
            version_error = None
    except OSError as error:
        version_error = 'cargo-deny is unavailable: %s' % error
        actual = 'unavailable'

    database = advisory_database()
    refresh = {'status': 'skipped', 'finished_at': None, 'candidates': []}

    def finish(code, summary, extra_error=None):
        after = input_fingerprint(repo)
        if before is None or after is None:
            summary['inputs_unchanged'] = False
            summary['input_errors'].append('Cargo.lock or deny.toml is missing')
            if summary['outcome'] == 'pass':
                summary['outcome'] = 'input-error'
        else:
            summary['inputs_unchanged'] = before == after
            if before != after:
                summary['input_errors'].append('Cargo.lock or deny.toml changed during the run')
                if summary['outcome'] == 'pass':
                    summary['outcome'] = 'input-error'
        if extra_error and extra_error not in summary['input_errors']:
            summary['input_errors'].append(extra_error)
            if summary['outcome'] == 'pass':
                summary['outcome'] = 'input-error'
        return emit(output, code, summary)

    if declaration_problem or version_error or actual != 'cargo-deny ' + (declared or ''):
        if version_error and not declaration_problem:
            message = version_error
        elif declaration_problem:
            message = ('cargo-deny version declaration is %s in .github/workflows/admission.yml; '
                       'expected exactly one NOVA_CI_CARGO_DENY_VERSION' % declaration_problem)
        else:
            message = ('%s does not match declared %s; install with: '
                       'cargo install cargo-deny --version %s --locked' % (actual, declared, declared))
        summary = empty_summary(started_at, declared, actual, database, refresh, None)
        return finish(2, summary, message)

    if before is None:
        summary = empty_summary(started_at, declared, actual, database, refresh, False)
        return finish(2, summary, 'Cargo.lock or deny.toml is missing')

    refresh_code = run_to_files(['cargo', 'update', '--dry-run'], repo,
                                output / 'cargo-update.stdout', output / 'cargo-update.stderr')
    refresh['finished_at'] = timestamp()
    if refresh_code != 0:
        refresh['status'] = 'failed'
        summary = empty_summary(started_at, declared, actual, database, refresh, None)
        return finish(2, summary, 'index refresh failed')

    candidates = parse_updates((output / 'cargo-update.stderr').read_text(encoding='utf-8', errors='replace'))
    refresh['status'] = 'ok'
    refresh['candidates'] = candidate_records(candidates)
    check_code = run_to_files(
        ['cargo', 'deny', '--format', 'json', '--locked', 'check', 'advisories'], repo,
        output / 'cargo-deny.stdout', output / 'cargo-deny.stderr')
    summary = summarize((output / 'cargo-deny.stderr').read_bytes(), check_code)
    summary.update(started_at=started_at, cargo_deny={'declared': declared, 'actual': actual},
                   advisory_db=database, index_refresh=refresh, inputs_unchanged=None)
    for finding in summary['findings']:
        finding['fix'] = classify_fix(finding, candidates, database)
    return finish(check_code, summary)


if __name__ == '__main__':
    sys.exit(main())
