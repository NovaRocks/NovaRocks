#!/usr/bin/env bash
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

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fixture_dir=$(cd "$script_dir/.." && pwd)
repo_root=$(cd "$fixture_dir/../.." && pwd)
source "$repo_root/tools/ci/lib/build_capabilities.sh"
ci_validate_build_environment
ci_set_default_build_jobs

# Both entrypoints share the original fixture input/run owner. The private
# recursive branch receives its already admitted, immutable input environment.
if [[ "${NOVA_FIXTURE_JAVA_DRIVER_OWNER:-false}" != true ]]; then
    exec python3 "$script_dir/java_rest_inputs.py" legacy "$@"
fi
interop_root="${NR_JAVA_REST_INTEROP_RUN_ROOT:?Java input owner required}"

cd "$repo_root"
# Cargo supplies the actual test executable; preserve build, listing and run
# outputs separately, and reject missing, extra, ignored or zero-case success.
python3 - "$interop_root" <<'PY_RUN'
from pathlib import Path
import datetime, hashlib, json, os, re, subprocess, sys
root = Path(sys.argv[1]).resolve()
expected = set(json.loads(os.environ['NR_JAVA_REST_INTEROP_EXPECTED_TESTS']))
pattern = 'commit::staging::interop_tests::'
def run(name, command):
    (root / f'{name}-command.json').write_text(json.dumps(command, indent=2) + '\n')
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with (root / f'{name}-stdout.log').open('wb') as stdout, (root / f'{name}-stderr.log').open('wb') as stderr:
        result = subprocess.run(command, stdout=stdout, stderr=stderr)
    (root / f'{name}-result.json').write_text(json.dumps({
        'exit_code': result.returncode, 'started': started,
        'ended': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }, indent=2) + '\n')
    if result.returncode:
        print((root / f'{name}-stdout.log').read_text(errors='replace')[-12000:])
        print((root / f'{name}-stderr.log').read_text(errors='replace')[-12000:], file=sys.stderr)
        raise SystemExit(result.returncode)
    return (root / f'{name}-stdout.log').read_text(errors='replace')
facts = {
    'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    'diff': subprocess.check_output(['git', 'diff', 'HEAD'], text=True),
    'cargo': subprocess.check_output(['cargo', '--version'], text=True).strip(),
    'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
    'profile': 'dev', 'target_dir': os.environ['CARGO_TARGET_DIR'],
    'lock_sha256': hashlib.sha256(Path('Cargo.lock').read_bytes()).hexdigest(),
}
(root / 'source-build-facts.json').write_text(json.dumps(facts, indent=2) + '\n')
# Keep the canonical development dependency graph; selection below binds only
# the provider's fourteen ignored tests from the actual Cargo artifact.
output = run('build', [
    'cargo', 'build', '--workspace', '--locked', '--offline', '--profile', 'dev',
    '--target-dir', os.environ['CARGO_TARGET_DIR'], '--tests', '--bins', '--message-format=json',
])
executables = set()
for line in output.splitlines():
    try:
        row = json.loads(line)
    except ValueError:
        continue
    if (row.get('reason') == 'compiler-artifact' and row.get('executable')
            and row.get('target', {}).get('name') == 'novarocks_connector_iceberg'
            and row.get('profile', {}).get('test')):
        executables.add(row['executable'])
if len(executables) != 1:
    raise SystemExit(f'FAIL: expected one Cargo-reported provider test executable, got {len(executables)}')
binary = Path(executables.pop()).resolve(strict=True)
(root / 'test-binary.json').write_text(json.dumps({
    'path': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
}, indent=2) + '\n')
listing = run('list', [str(binary), pattern, '--ignored', '--list'])
listed = {line[:-6] for line in listing.splitlines() if line.endswith(': test')}
if listed != expected:
    raise SystemExit(f'FAIL: Java test inventory differs: missing={sorted(expected-listed)}, extra={sorted(listed-expected)}')
output = run('tests', [str(binary), pattern, '--ignored', '--test-threads=1', '--nocapture'])
passed = set(re.findall(r'^test (\S+) \.\.\. ok$', output, re.MULTILINE))
if passed != expected:
    raise SystemExit(f'FAIL: Java test results differ: missing={sorted(expected-passed)}, extra={sorted(passed-expected)}')
if not re.search(rf'test result: ok\. {len(expected)} passed; 0 failed; 0 ignored;', output):
    raise SystemExit('FAIL: expected all pinned Java tests to pass without skips')
print(f'Java REST interoperability: {len(expected)} passed; 0 failed; 0 ignored')
PY_RUN
printf 'IRU-13a Java REST interoperability artifacts: %s\n' "$interop_root"
