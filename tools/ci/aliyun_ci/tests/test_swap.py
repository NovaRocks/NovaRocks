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

"""Real private-file allocation with injected Linux commands and kernel readback.

These tests never activate host swap. CI maintainers run them for worker resource
changes; actual mkswap/swapon and kernel acceptance remain guest validation.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from aliyun_ci.instance import swap


class SwapTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.private = self.root / 'private-swap'
        self.stack = __import__('contextlib').ExitStack(); self.addCleanup(self.stack.close)
        for target, value in (('SWAP_ROOT', self.private), ('ROOT_UID', os.geteuid()), ('GIB', 65536)):
            self.stack.enter_context(patch.object(swap, target, value))
        self.stack.enter_context(patch.object(swap.sys, 'platform', 'linux'))
        self.active = False; self.calls = []; self.filesystem = 'ext4'; self.sparse = False
        self.command_failure = None; self.command_timeout = None; self.bad_kernel_size = False
        self.policy = self.root / 'vm-swappiness'; self.policy.write_text('0\n')
        self.stack.enter_context(patch.object(swap, 'SWAPPINESS_PATH', self.policy))

    def read_text(self, path):
        size = 16 * swap.GIB - os.sysconf('SC_PAGE_SIZE')
        reported = size - 1024 if self.bad_kernel_size else size
        if path == '/proc/swaps':
            return 'Filename\tType\tSize\tUsed\tPriority\n' + (
                f'{self.private}/swapfile file {reported // 1024} 0 -2\n' if self.active else '')
        if path == '/proc/meminfo':
            amount = size // 1024 if self.active else 0
            return f'MemTotal: 100000 kB\nMemAvailable: 50000 kB\nAnonPages: 40000 kB\nSwapTotal: {amount} kB\nSwapFree: {amount} kB\n'
        raise AssertionError(path)

    def command_runner(self, argv, **options):
        self.calls.append(argv)
        self.assertGreater(options['timeout'], 0)
        name = Path(argv[0]).name
        if self.command_timeout == name:
            raise subprocess.TimeoutExpired(argv, options['timeout'])
        if self.command_failure == name:
            return subprocess.CompletedProcess(argv, 1, '', 'controlled command failure')
        if name == 'findmnt':
            output = json.dumps({'filesystems': [{'fstype': self.filesystem, 'source': '/dev/disk', 'target': '/'}]})
        elif name == 'dd':
            path = Path(next(value[3:] for value in argv if value.startswith('of=')))
            size = int(next(value[6:] for value in argv if value.startswith('count='))) * 1024 ** 2
            with path.open('wb') as target:
                if self.sparse:
                    target.truncate(size)
                else:
                    target.write(b'\0' * size)
                    target.flush(); os.fsync(target.fileno())
            output = ''
        elif name == 'mkswap':
            self.assertEqual(self.private / 'swapfile', Path(argv[1])); output = 'controlled format\n'
        elif name == 'swapon':
            self.assertEqual(self.private / 'swapfile', Path(argv[1])); self.active = True; output = ''
        else:
            raise AssertionError(argv)
        return subprocess.CompletedProcess(argv, 0, output, '')

    def configure(self, size=16, free=2 ** 40):
        return swap.configure_swap(size, run=self.command_runner, read_text=self.read_text,
            disk_usage=lambda _: SimpleNamespace(free=free))

    def test_zero_observes_without_creating_or_activating_swap(self):
        with patch.object(Path, 'read_text', side_effect=AssertionError('zero must not read policy')), \
             patch.object(Path, 'write_text', side_effect=AssertionError('zero must not write policy')):
            fact = self.configure(0)
        self.assertFalse(fact['enabled']); self.assertEqual([], self.calls)
        self.assertFalse(self.private.exists())
        self.assertEqual(0, fact['kernel_after']['meminfo']['SwapTotal_kib'])
        self.assertEqual('0\n', self.policy.read_text()); self.assertNotIn('swappiness', fact)

    def test_private_non_sparse_file_is_formatted_activated_and_verified(self):
        fact = self.configure()
        self.assertTrue(fact['enabled']); self.assertEqual('ready', fact['stage'])
        self.assertEqual(['findmnt', 'dd', 'mkswap', 'swapon'], [Path(c[0]).name for c in self.calls])
        path = self.private / 'swapfile'
        self.assertEqual(16 * swap.GIB, path.stat().st_size)
        self.assertGreaterEqual(path.stat().st_blocks * 512, path.stat().st_size)
        self.assertEqual(0o700, self.private.stat().st_mode & 0o777)
        self.assertEqual(0o600, path.stat().st_mode & 0o777)
        self.assertEqual(os.geteuid(), path.stat().st_uid)
        self.assertEqual(str(path), fact['kernel_after']['swaps'][0]['path'])
        self.assertTrue(all(c['exit_code'] == 0 for c in fact['commands']))
        self.assertEqual({'before': 0, 'requested': 60, 'after': 60}, fact['swappiness'])
        self.assertEqual('60\n', self.policy.read_text())

    def test_policy_write_failure_keeps_actual_active_swap_and_readback(self):
        original = Path.open
        def open_file(path, mode='r', *args, **kwargs):
            if path == self.policy and 'w' in mode:
                raise PermissionError('controlled policy write failure')
            return original(path, mode, *args, **kwargs)
        with patch.object(Path, 'open', open_file), self.assertRaises(swap.SwapSetupError) as raised:
            self.configure()
        fact = raised.exception.fact
        self.assertTrue(fact['enabled']); self.assertTrue(fact['kernel_after']['swaps'])
        self.assertEqual('swappiness-write', fact['stage'])
        self.assertEqual('PermissionError', fact['error_type'])
        self.assertEqual({'before': 0, 'requested': 60, 'after': 0}, fact['swappiness'])

    def test_policy_readback_failure_keeps_unknown_value_and_actual_active_swap(self):
        original = Path.read_text; reads = []
        def read_file(path, *args, **kwargs):
            if path == self.policy:
                reads.append(path)
                if len(reads) > 1:
                    raise PermissionError('controlled policy readback failure')
            return original(path, *args, **kwargs)
        with patch.object(Path, 'read_text', read_file), self.assertRaises(swap.SwapSetupError) as raised:
            self.configure()
        fact = raised.exception.fact
        self.assertTrue(fact['enabled']); self.assertTrue(fact['kernel_after']['swaps'])
        self.assertEqual('swappiness-readback', fact['stage'])
        self.assertEqual('PermissionError', fact['error_type'])
        self.assertEqual({'before': 0, 'requested': 60, 'after': None}, fact['swappiness'])
        self.assertEqual('60\n', self.policy.read_text())

    def test_policy_readback_mismatch_is_not_reported_ready(self):
        original = Path.read_text
        def read_file(path, *args, **kwargs):
            if path == self.policy and original(path) == '60\n':
                path.write_text('0\n')  # Controlled kernel-policy rejection fixture.
            return original(path, *args, **kwargs)
        with patch.object(Path, 'read_text', read_file), self.assertRaises(swap.SwapSetupError) as raised:
            self.configure()
        fact = raised.exception.fact
        self.assertTrue(fact['enabled']); self.assertEqual('swappiness-readback', fact['stage'])
        self.assertEqual(0, fact['swappiness']['after'])
        self.assertIn('readback differs', fact['reason'])

    def test_unknown_directory_or_link_is_never_overwritten(self):
        self.private.mkdir(); marker = self.private / 'existing'; marker.write_text('keep')
        with self.assertRaises(swap.SwapSetupError) as raised: self.configure()
        self.assertIn('existing', raised.exception.fact['reason'])
        self.assertEqual('keep', marker.read_text()); self.assertEqual([], self.calls)
        marker.unlink(); self.private.rmdir()
        target = self.root / 'target'; target.mkdir(); self.private.symlink_to(target)
        with self.assertRaises(swap.SwapSetupError): self.configure()
        self.assertTrue(self.private.is_symlink()); self.assertEqual([], list(target.iterdir()))

    def test_unsupported_filesystem_and_insufficient_space_do_not_allocate(self):
        self.filesystem = 'tmpfs'
        with self.assertRaises(swap.SwapSetupError): self.configure()
        self.assertFalse(self.private.exists())
        self.filesystem = 'ext4'
        with self.assertRaises(swap.SwapSetupError): self.configure(free=16 * swap.GIB - 1)
        self.assertFalse(self.private.exists())
        self.assertTrue(all(Path(c[0]).name == 'findmnt' for c in self.calls))

    def test_sparse_file_is_rejected_before_format_or_activation(self):
        self.sparse = True
        with self.assertRaises(swap.SwapSetupError) as raised: self.configure()
        self.assertEqual('allocate', raised.exception.fact['stage'])
        self.assertEqual(['findmnt', 'dd'], [Path(c[0]).name for c in self.calls])

    def test_activation_failure_retains_original_exit_and_kernel_fact(self):
        self.command_failure = 'swapon'
        with self.assertRaises(swap.SwapSetupError) as raised: self.configure()
        fact = raised.exception.fact
        self.assertEqual('activate', fact['stage']); self.assertFalse(fact['enabled'])
        self.assertEqual(1, fact['commands'][-1]['exit_code'])
        self.assertEqual([], fact['kernel_after']['swaps'])

    def test_command_timeout_retains_stage_and_unknown_command_exit(self):
        self.command_timeout = 'dd'
        with self.assertRaises(swap.SwapSetupError) as raised: self.configure()
        fact = raised.exception.fact
        self.assertEqual('allocate', fact['stage'])
        self.assertEqual('TimeoutExpired', fact['error_type'])
        self.assertIsNone(fact['commands'][-1]['exit_code'])
        self.assertIn('timed out', fact['reason'])

    def test_kernel_size_mismatch_is_failure_even_after_command_success(self):
        self.bad_kernel_size = True
        with self.assertRaises(swap.SwapSetupError) as raised: self.configure()
        self.assertEqual('readback', raised.exception.fact['stage'])
        self.assertTrue(raised.exception.fact['enabled'])
        self.assertTrue(all(c['exit_code'] == 0 for c in raised.exception.fact['commands']))
        self.assertEqual('0\n', self.policy.read_text())
        self.assertNotIn('swappiness', raised.exception.fact)

    def test_unknown_size_fails_before_commands_or_files(self):
        for size in (True, 1, 16.0, -1, '16'):
            with self.subTest(size=size), self.assertRaises(ValueError): self.configure(size)
        self.assertEqual([], self.calls); self.assertFalse(self.private.exists())
