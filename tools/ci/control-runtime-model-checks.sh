#!/usr/bin/env bash
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

# The standard and deterministic drivers share these synchronization models.
# Missing local tools/dependencies fail; this owner never installs them.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: tools/ci/control-runtime-model-checks.sh --loom [--output <new-directory>] [--target-dir <directory>]
Runs all control-runtime Loom targets using their production synchronization code.
Each invocation owns a new evidence directory with exact commands, logs and exits.
Dependencies are locked/offline. No toolchain or component is downloaded.
The default Cargo target is target/control-runtime-loom, separate from normal builds.
EOF
}
mode="${1:-}"
if [[ "$mode" == --help || "$mode" == -h ]]; then usage; exit 0; fi
[[ "$mode" == --loom ]] || { usage >&2; exit 2; }
shift
output=""
target_dir=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output|--target-dir)
      [[ $# -ge 2 && -n "$2" ]] || { usage >&2; exit 2; }
      if [[ "$1" == --output ]]; then output="$2"; else target_dir="$2"; fi
      shift 2 ;;
    *) usage >&2; exit 2 ;;
  esac
done
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"
source "$repo_root/tools/ci/lib/build_capabilities.sh"
ci_set_default_build_jobs
export CARGO_NET_OFFLINE=true RUSTUP_AUTO_INSTALL=0
# Cargo's encoded flag override would otherwise suppress --cfg loom.
if [[ -n "${CARGO_ENCODED_RUSTFLAGS:-}" ]]; then
  echo 'ERROR: CARGO_ENCODED_RUSTFLAGS overrides Loom configuration; clear it before this mode.' >&2
  exit 2
fi
export RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg loom"
python3 - "$repo_root" "$output" "$target_dir" <<'MODELS'
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

repo = Path(sys.argv[1]).resolve()
output = Path(sys.argv[2]).expanduser().resolve() if sys.argv[2] else None
target_dir = Path(sys.argv[3]).expanduser().resolve() if sys.argv[3] else repo / 'target/control-runtime-loom'
if output is None:
    root = Path(os.environ.get('XDG_STATE_HOME') or Path.home() / '.local/state') / 'novarocks/control-runtime-model-checks'
    root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ-')
    output = Path(tempfile.mkdtemp(prefix=stamp, dir=root))
else:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir(mode=0o700, exist_ok=False)
print('Control runtime model evidence: ' + str(output), flush=True)

def save(path, value):
    # The owning attempt is exclusive. Preserve prior outputs on any retry.
    with path.open('x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')

def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def sources():
    patterns = ['Cargo.toml', 'Cargo.lock', 'rust-toolchain*', '.cargo/**/*',
                'novarocks/control-runtime/Cargo.toml', 'novarocks/control-runtime/src/**/*',
                'novarocks/control-runtime/tests/**/*', 'novarocks/control-runtime-core/Cargo.toml',
                'novarocks/control-runtime-core/src/**/*', 'tools/ci/control-runtime-model-checks.sh',
                'tools/ci/lib/build_capabilities.sh', 'tools/ci/lib/build_jobs.py']
    return {str(path.relative_to(repo)): hashlib.sha256(path.read_bytes()).hexdigest()
            for pattern in patterns for path in sorted(repo.glob(pattern)) if path.is_file()}

# An environment checkpoint or permutation/time cap can make Loom return
# successfully after exploring only a prefix. This mode uses the explicit
# bounds in each model, without an external early-stop override.
loom_overrides = sorted(name for name in os.environ if name.startswith('LOOM_'))
if loom_overrides:
    save(output / 'result.json', {'schema': 1, 'complete': False, 'exit': 2,
                                 'error': 'external Loom overrides: ' + ', '.join(loom_overrides)})
    print('ERROR: external Loom overrides are not allowed: ' + ', '.join(loom_overrides), file=sys.stderr)
    sys.exit(2)
cargo = shutil.which('cargo')
if cargo is None:
    save(output / 'result.json', {'schema': 1, 'complete': False, 'error': 'cargo is unavailable', 'exit': 2})
    print('ERROR: cargo is unavailable; no toolchain will be installed.', file=sys.stderr)
    sys.exit(2)
save(output / 'environment.json', {
    'cwd': str(repo), 'cargo': cargo, 'target_dir': str(target_dir),
    'environment': {name: value for name, value in sorted(os.environ.items())
                    if name in {'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_BUILD_JOBS',
                                'CARGO_NET_OFFLINE', 'RUSTUP_TOOLCHAIN', 'RUSTUP_AUTO_INSTALL',
                                'RUSTC', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_BUILD_TARGET'}
                    or name.startswith(('LOOM_', 'CARGO_PROFILE_', 'CARGO_TARGET_'))},
})
toolchain = {}
for name, argv in [('cargo', [cargo, '--version']), ('rustc', ['rustc', '-vV'])]:
    try:
        tool = subprocess.run(argv, cwd=repo, capture_output=True, text=True, check=False)
        toolchain[name] = {'argv': argv, 'exit': tool.returncode, 'stdout': tool.stdout, 'stderr': tool.stderr}
    except OSError as error:
        toolchain[name] = {'argv': argv, 'exit': None, 'error': str(error)}
save(output / 'toolchain.json', toolchain)
if any(value['exit'] != 0 for value in toolchain.values()):
    save(output / 'result.json', {'schema': 1, 'complete': False, 'exit': 2, 'error': 'local toolchain is unavailable'})
    sys.exit(2)
initial = sources()
save(output / 'sources-start.json', initial)
# This target list belongs to the runtime's model boundary. Test counts are
# discovered from each actual native libtest run, never assumed from source.
targets = ('concurrency_loom', 'continuation_loom', 'exit_custody_loom', 'failure_publication_loom', 'read_resource_loom', 'coalescing_input_loom')
results = []
code = 0
for target in targets:
    directory = output / target
    directory.mkdir()
    argv = [cargo, 'test', '-p', 'novarocks-control-runtime', '--test', target,
            '--locked', '--offline', '--target-dir', str(target_dir), '--', '--test-threads=1']
    save(directory / 'command.json', {'argv': argv, 'cwd': str(repo), 'started_at': utc()})
    print('Running bounded Loom target: ' + target, flush=True)
    with (directory / 'stdout.log').open('xb') as stdout, (directory / 'stderr.log').open('xb') as stderr:
        try:
            completed = subprocess.run(argv, cwd=repo, stdout=stdout, stderr=stderr, check=False)
            actual_exit = completed.returncode
        except OSError as error:
            actual_exit = None
            (directory / 'spawn-error.txt').write_text(str(error) + '\n')
    save(directory / 'exit.json', {'code': actual_exit, 'ended_at': utc()})
    for name, sink in [('stdout.log', sys.stdout), ('stderr.log', sys.stderr)]:
        with (directory / name).open() as stream:
            shutil.copyfileobj(stream, sink)
        sink.flush()
    stdout = (directory / 'stdout.log').read_text()
    summaries = re.findall(r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', stdout, re.MULTILINE)
    running = re.findall(r'^running (\d+) tests?$', stdout, re.MULTILINE)
    valid = (actual_exit == 0 and len(summaries) == 1 and len(running) == 1
             and summaries[0][0] == 'ok' and int(summaries[0][1]) > 0
             and int(running[0]) == int(summaries[0][1])
             and all(int(value) == 0 for value in summaries[0][2:]))
    result = {'target': target, 'exit': actual_exit, 'complete': valid,
              'passed': int(summaries[0][1]) if len(summaries) == 1 else None}
    save(directory / 'result.json', result)
    results.append(result)
    if not valid:
        code = actual_exit if actual_exit is not None and actual_exit > 0 else 1
        print('ERROR: Loom target failed or lacked a complete nonempty native run: ' + target, file=sys.stderr)
        break
final = sources()
save(output / 'sources-end.json', final)
stable = initial == final
if not stable:
    code = code or 1
    print('ERROR: control runtime model sources changed during this invocation.', file=sys.stderr)
save(output / 'result.json', {'schema': 1, 'mode': 'loom', 'targets': results, 'expected_targets': list(targets),
                              'sources_stable': stable, 'complete': code == 0 and len(results) == len(targets),
                              'exit': code, 'ended_at': utc()})
sys.exit(code)
MODELS
