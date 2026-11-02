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

"""One worker's explicit disk swap; never imported memory or archive content."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import time

from aliyun_ci.config import LOCAL_SWAP_GIB

SWAP_ROOT = Path('/var/lib/novarocks-ci/private-swap')
SWAPPINESS_PATH = Path('/proc/sys/vm/swappiness')
ROOT_UID = 0
GIB = 1024 ** 3


class SwapSetupError(RuntimeError):
    def __init__(self, fact):
        super().__init__('explicit worker swap setup failed')
        self.fact = fact


def kernel_snapshot(*, read_text=None, strict=True):
    read_text = read_text or (lambda path: Path(path).read_text())
    try:
        swaps = []
        lines = read_text('/proc/swaps').splitlines()
        if not lines or lines[0].split() != ['Filename', 'Type', 'Size', 'Used', 'Priority']:
            raise ValueError('invalid kernel swap table')
        for line in lines[1:]:
            name, kind, size, used, priority = line.split()
            swaps.append({'path': name, 'type': kind, 'size_kib': int(size),
                          'used_kib': int(used), 'priority': int(priority)})
        meminfo = {}
        for line in read_text('/proc/meminfo').splitlines():
            name, value = line.split(':', 1)
            if name in ('MemTotal', 'MemAvailable', 'AnonPages', 'SwapTotal', 'SwapFree'):
                amount, unit = value.split()
                if unit != 'kB':
                    raise ValueError('unexpected kernel memory unit')
                meminfo[name + '_kib'] = int(amount)
        if 'SwapTotal_kib' not in meminfo or 'SwapFree_kib' not in meminfo:
            raise ValueError('kernel swap memory facts are missing')
        return {'swaps': swaps, 'meminfo': meminfo}
    except (OSError, ValueError):
        if strict:
            raise
        return None


def _private_parent(path):
    for parent in (path.parent, *path.parent.parents):
        info = parent.stat()
        if (parent.is_symlink() or not stat.S_ISDIR(info.st_mode)
                or info.st_uid not in (0, ROOT_UID)
                or (info.st_mode & 0o022 and not info.st_mode & stat.S_ISVTX)):
            raise ValueError('swap storage requires root-controlled ordinary parents')


def configure_swap(swap_gib, *, timeout=300, run=subprocess.run,
                   read_text=None, disk_usage=shutil.disk_usage, monotonic=time.monotonic):
    """Allocate one fixed private non-sparse file and verify the kernel accepted it."""
    if type(swap_gib) is not int or swap_gib not in LOCAL_SWAP_GIB:
        raise ValueError('swap_gib must be 0 or 16')
    fact = {'requested_gib': swap_gib, 'enabled': False, 'stage': 'disabled',
            'file': None, 'commands': [], 'kernel_before': None, 'kernel_after': None}
    if swap_gib == 0:
        fact['kernel_before'] = kernel_snapshot(read_text=read_text, strict=False)
        fact['kernel_after'] = fact['kernel_before']
        return fact
    end = monotonic() + timeout
    def command(argv):
        remaining = end - monotonic()
        if remaining <= 0:
            raise TimeoutError('worker swap setup deadline expired')
        record = {'argv': argv, 'exit_code': None}
        fact['commands'].append(record)
        result = run(argv, check=False, capture_output=True, text=True, timeout=remaining)
        record.update(exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        if result.returncode != 0:
            raise RuntimeError('worker swap command failed')
        return result.stdout
    try:
        fact['stage'] = 'validate'
        if sys.platform != 'linux' or os.geteuid() != ROOT_UID:
            raise ValueError('worker swap setup requires Linux root')
        _private_parent(SWAP_ROOT)
        if SWAP_ROOT.exists() or SWAP_ROOT.is_symlink():
            raise ValueError('refusing existing worker swap storage')
        fact['kernel_before'] = kernel_snapshot(read_text=read_text)
        fact['stage'] = 'filesystem'
        mounts = json.loads(command(['/usr/bin/findmnt', '--json', '--target', str(SWAP_ROOT.parent),
                                     '--output', 'FSTYPE,SOURCE,TARGET']))['filesystems']
        if len(mounts) != 1 or mounts[0]['fstype'] not in ('ext4', 'xfs'):
            raise ValueError('worker swap requires an ext4 or xfs disk filesystem')
        fact['filesystem'] = mounts[0]
        size = swap_gib * GIB
        available = disk_usage(SWAP_ROOT.parent).free
        fact.update(file_size_bytes=size, disk_free_bytes_before=available)
        if available < size:
            raise ValueError('insufficient disk space for explicit worker swap')
        SWAP_ROOT.mkdir(mode=0o700)
        os.chmod(SWAP_ROOT, 0o700)
        path = SWAP_ROOT / 'swapfile'
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        fact['file'] = str(path)
        fact['stage'] = 'allocate'
        # Write every byte; truncate and filesystem-dependent preallocation are not sufficient.
        command(['/usr/bin/dd', 'if=/dev/zero', f'of={path}', 'bs=1M',
                 f'count={size // (1024 ** 2)}', 'conv=fsync', 'status=none'])
        info = path.lstat(); directory = SWAP_ROOT.lstat()
        fact['allocated_bytes'] = info.st_blocks * 512
        fact['file_stat'] = {'uid': info.st_uid, 'gid': info.st_gid,
                             'mode': oct(stat.S_IMODE(info.st_mode)), 'size_bytes': info.st_size,
                             'links': info.st_nlink}
        fact['directory_stat'] = {'uid': directory.st_uid,
                                  'mode': oct(stat.S_IMODE(directory.st_mode))}
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != ROOT_UID or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size != size
                or fact['allocated_bytes'] < size or directory.st_uid != ROOT_UID
                or stat.S_IMODE(directory.st_mode) != 0o700):
            raise ValueError('worker swap file allocation or privacy differs from request')
        fact['stage'] = 'format'
        command(['/usr/sbin/mkswap', str(path)])
        fact['stage'] = 'activate'
        fact['enabled'] = None
        command(['/usr/sbin/swapon', str(path)])
        fact['stage'] = 'readback'
        after = kernel_snapshot(read_text=read_text); fact['kernel_after'] = after
        rows = [row for row in after['swaps'] if row['path'] == str(path)]
        fact['enabled'] = bool(rows)
        expected_usable = size - os.sysconf('SC_PAGE_SIZE')
        if (len(rows) != 1 or rows[0]['type'] != 'file'
                or rows[0]['size_kib'] * 1024 != expected_usable
                or after['meminfo']['SwapTotal_kib'] < rows[0]['size_kib']):
            raise ValueError('kernel worker swap readback differs from requested file')
        fact.update(enabled=True, usable_bytes=expected_usable)
        # This disposable worker alone owns this runtime policy; never persist it.
        policy = {'before': None, 'requested': 60, 'after': None}
        fact['swappiness'] = policy
        fact['stage'] = 'swappiness-before'
        policy['before'] = int(SWAPPINESS_PATH.read_text().strip())
        fact['stage'] = 'swappiness-write'
        with SWAPPINESS_PATH.open('w') as output:
            output.write('60\n')
        fact['stage'] = 'swappiness-readback'
        policy['after'] = int(SWAPPINESS_PATH.read_text().strip())
        if policy['after'] != policy['requested']:
            raise ValueError('worker swappiness readback differs from runtime policy')
        fact['stage'] = 'ready'
        return fact
    except Exception as error:
        if fact['stage'] == 'swappiness-write':
            try:
                fact['swappiness']['after'] = int(SWAPPINESS_PATH.read_text().strip())
            except Exception as read_error:
                fact['swappiness']['readback_error'] = type(read_error).__name__
        if fact['stage'] in ('activate', 'readback') and fact['kernel_after'] is None:
            fact['kernel_after'] = kernel_snapshot(read_text=read_text, strict=False)
            if fact['kernel_after'] is not None:
                fact['enabled'] = any(row['path'] == fact['file']
                                      for row in fact['kernel_after']['swaps'])
        fact['error_type'] = type(error).__name__
        fact['reason'] = str(error)
        raise SwapSetupError(fact) from error
