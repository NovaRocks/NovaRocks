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

"""Two bounded resource observations; never a cleanup or OOM verdict."""
from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess
import tempfile

from .swap import kernel_snapshot

CGROUP_FILES = ('memory.max', 'memory.high', 'memory.current', 'memory.peak',
                'memory.swap.max', 'memory.swap.current', 'memory.events', 'memory.pressure',
                'cpu.max', 'cpu.stat')


def _read(path, limit=16384):
    try:
        with Path(path).open('rb') as source:
            raw = source.read(limit + 1)
        return {'value': raw[:limit].decode(errors='replace'), 'truncated': len(raw) > limit,
                'error': None}
    except OSError as error:
        return {'value': None, 'error': type(error).__name__, 'errno': error.errno}


def _oom_log():
    # Keep no unfiltered kernel log in the archive, including on timeout.
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        try:
            process = subprocess.Popen(['/usr/bin/dmesg', '--color=never'],
                                       stdout=output, stderr=errors)
        except OSError as error:
            return {'status': 'unknown', 'error': type(error).__name__}
        timed_out = False
        try:
            process.wait(timeout=2.7)
        except subprocess.TimeoutExpired:
            timed_out = True
            try: process.kill()
            except ProcessLookupError: pass
            try: process.wait(timeout=0.2)
            except subprocess.TimeoutExpired: pass
        output.seek(0, 2); length = output.tell()
        output.seek(max(0, length - 262144))
        lines = output.read(262144).decode(errors='replace').splitlines()
        selected = [re.sub(r'https?://\S+', '[url omitted]', line)[:512] for line in lines
                    if re.search(r'out of memory|oom[-_ ]kill|invoked oom|killed process', line, re.I)]
        return {'status': 'observed' if process.returncode == 0 and not timed_out else 'unknown',
                'actual_exit': process.returncode, 'timed_out': timed_out,
                'error': 'dmesg visibility unknown' if process.returncode != 0 or timed_out else None,
                'tail_truncated': length > 262144, 'oom_lines': selected[-64:],
                'matching_lines_truncated': len(selected) > 64, 'absence_proves_no_oom': False}


def _cgroup_read(path, boundary):
    try:
        if not path.resolve().is_relative_to(boundary):
            return {'value': None, 'error': 'cgroup file escapes its filesystem'}
        return _read(path, 4096)
    except OSError as error:
        return {'value': None, 'error': type(error).__name__, 'errno': error.errno}


def snapshot(pid, phase, *, exited, exit_code, start_groups=None,
             proc_root=Path('/proc'), cgroup_root=Path('/sys/fs/cgroup'), oom_log=_oom_log):
    proc_root, cgroup_root = Path(proc_root), Path(cgroup_root)
    result = {'phase': phase, 'pid': pid, 'time': datetime.now(timezone.utc).isoformat(),
              'actual_process_exit_observed': exited, 'actual_exit_code': exit_code,
              'cleanup_complete': None, 'errors': []}
    limits = _read(proc_root / str(pid) / 'limits') if phase != 'end' else {
        'value': None, 'error': 'see original start observation; end PID identity is not revalidated'}
    result['limits_read'] = {key: value for key, value in limits.items() if key != 'value'}
    result['limits'] = {}
    for name in ('Max address space', 'Max data size', 'Max cpu time', 'Max open files'):
        match = next((line[len(name):].split() for line in (limits['value'] or '').splitlines()
                      if line.startswith(name)), None)
        result['limits'][name] = {'soft': match[0], 'hard': match[1], 'units': ' '.join(match[2:])} if match and len(match) >= 2 else None
    membership = _read(proc_root / str(pid) / 'cgroup') if phase != 'end' else {
        'value': None, 'error': 'see original start observation; end PID identity is not revalidated'}
    result['cgroup_membership'] = membership
    groups = []
    if start_groups is not None:
        groups = list(start_groups)
        result['cgroup_source'] = 'original start membership retained for end observation'
    else:
        result['cgroup_source'] = 'original PID current membership'
        for line in (membership['value'] or '').splitlines():
            if line.startswith('0::'):
                groups.append(line[3:])
    result['original_cgroups'] = groups
    result['cgroup_status'] = 'observed' if groups else 'unknown: no original v2 membership available'
    result['cgroups'] = []
    boundary = cgroup_root.resolve()
    for group in groups[:1]:
        try:
            relative = Path(group.lstrip('/'))
            if '..' in relative.parts or len(relative.parts) > 32:
                raise ValueError('invalid or excessive cgroup path')
            current = (boundary / relative).resolve()
            if not current.is_relative_to(boundary):
                raise ValueError('cgroup path escapes its filesystem')
            while True:
                result['cgroups'].append({'path': str(current),
                    'files': {name: _cgroup_read(current / name, boundary) for name in CGROUP_FILES}})
                if current == boundary:
                    break
                current = current.parent
        except (OSError, ValueError) as error:
            result['errors'].append({'source': 'cgroup', 'error': str(error)})
    def kernel_read(path):
        fact = _read(proc_root / Path(path).relative_to('/proc'))
        if fact['error'] is not None or fact.get('truncated'):
            result['errors'].append({'source': path, 'fact': fact})
            raise OSError('kernel observation unavailable')
        return fact['value']
    result['kernel'] = kernel_snapshot(read_text=kernel_read, strict=False)
    vmstat = _read(proc_root / 'vmstat')
    values = dict(line.split() for line in (vmstat['value'] or '').splitlines() if len(line.split()) == 2)
    result['vmstat'] = {'counters': {name: values.get(name) for name in ('oom_kill', 'pswpin', 'pswpout', 'pgmajfault')},
                        'error': vmstat['error'], 'truncated': vmstat.get('truncated', False)}
    result['psi_memory'] = _read(proc_root / 'pressure/memory', 4096)
    result['swappiness'] = _read(proc_root / 'sys/vm/swappiness', 128)
    if phase == 'end':
        result['kernel_oom_log'] = oom_log()
    return result
