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

"""Bounded local proc/cgroup fixtures; no cloud or host resource mutation."""
import json
from pathlib import Path
import tempfile
import unittest
from aliyun_ci.instance.resource_observation import snapshot
from aliyun_ci.instance import resource_observation


class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.proc = self.root/'proc'; self.cg = self.root/'cgroup'
        (self.proc/'42').mkdir(parents=True); (self.cg/'unit/child').mkdir(parents=True)
        (self.proc/'42/cgroup').write_text('0::/unit/child\n')
        (self.proc/'42/limits').write_text('Max address space         unlimited unlimited bytes\nMax data size             100 200 bytes\nMax cpu time              unlimited unlimited seconds\nMax open files            65536 65536 files\n')
        (self.proc/'meminfo').write_text('MemTotal: 100 kB\nMemAvailable: 50 kB\nSwapTotal: 16 kB\nSwapFree: 8 kB\n')
        (self.proc/'swaps').write_text('Filename Type Size Used Priority\n/swap file 16 8 -2\n')
        (self.proc/'vmstat').write_text('oom_kill 2\npswpin 3\npswpout 4\npgmajfault 5\n')
        for node in (self.cg, self.cg/'unit', self.cg/'unit/child'):
            (node/'memory.max').write_text('max\n'); (node/'memory.events').write_text('oom 2\noom_kill 1\n')
            (node/'cpu.max').write_text('800000 100000\n')
    def observe(self, phase='start', **options):
        return snapshot(42, phase, proc_root=self.proc, cgroup_root=self.cg,
            oom_log=lambda:{'status':'unknown','error':'controlled unsupported permission'},
            exited=phase=='end', exit_code=7 if phase=='end' else None, **options)
    def test_limits_and_original_cgroup_ancestors_are_preserved_after_exit(self):
        first = self.observe()
        self.assertEqual('65536', first['limits']['Max open files']['soft'])
        self.assertEqual(3, len(first['cgroups']))
        self.assertEqual('800000 100000\n',first['cgroups'][0]['files']['cpu.max']['value'])
        self.assertEqual('2', first['vmstat']['counters']['oom_kill'])
        self.assertIsNone(first['cgroups'][0]['files']['memory.high']['value'])
        (self.proc/'42/cgroup').unlink(); (self.proc/'42/limits').unlink()
        end = self.observe('end', start_groups=first['original_cgroups'])
        self.assertEqual(3, len(end['cgroups'])); self.assertIsNone(end['limits']['Max open files'])
        self.assertEqual('unknown', end['kernel_oom_log']['status'])
        self.assertTrue(end['actual_process_exit_observed']); self.assertIsNone(end['cleanup_complete'])
    def test_cgroup_escape_and_symlink_files_are_not_read(self):
        (self.proc/'42/cgroup').write_text('0::/../../outside\n')
        fact=self.observe(); self.assertEqual([],fact['cgroups']); self.assertTrue(fact['errors'])
        (self.proc/'42/cgroup').write_text('0::/unit/child\n')
        secret=self.root/'outside'; secret.write_text('must not read')
        target=self.cg/'unit/child/memory.max'; target.unlink(); target.symlink_to(secret)
        value=self.observe()['cgroups'][0]['files']['memory.max']
        self.assertIsNone(value['value']); self.assertIn('escapes',value['error'])
    def test_unsupported_proc_keeps_all_unknown_instead_of_zero(self):
        result=snapshot(999,'end',exited=False,exit_code=None,proc_root=self.root/'absent',
            cgroup_root=self.cg,oom_log=lambda:{'status':'unknown'})
        self.assertIsNone(result['kernel']); self.assertIsNone(result['vmstat']['counters']['oom_kill'])
        self.assertTrue(result['errors']); self.assertIn('unknown',result['cgroup_status'])

    def test_unknown_end_never_reads_a_reused_pid(self):
        first=self.observe()
        (self.proc/'42/cgroup').write_text('0::/different-process\\n')
        end=snapshot(42,'end',exited=False,exit_code=None,start_groups=first['original_cgroups'],
            proc_root=self.proc,cgroup_root=self.cg,oom_log=lambda:{'status':'unknown'})
        self.assertEqual(['/unit/child'],end['original_cgroups'])
        self.assertIsNone(end['limits']['Max open files'])
        self.assertIsNone(end['cgroup_membership']['value'])

    def test_oom_command_wait_and_kill_grace_are_bounded_and_unknown(self):
        from unittest.mock import Mock, patch
        import subprocess
        child=Mock(); child.returncode=None
        child.wait.side_effect=subprocess.TimeoutExpired('dmesg',2.7)
        with patch.object(resource_observation.subprocess,'Popen',return_value=child):
            fact=resource_observation._oom_log()
        self.assertEqual([2.7,0.2],[call.kwargs['timeout'] for call in child.wait.call_args_list])
        child.kill.assert_called_once()
        self.assertEqual('unknown',fact['status']); self.assertIsNone(fact['actual_exit'])
        self.assertFalse(fact['absence_proves_no_oom'])
