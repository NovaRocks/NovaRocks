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

"""The shell replaces itself with the batch owner, preserving PID and signals.

A tiny Python stand-in records the exact native argv and handles SIGTERM. It
proves the shell boundary only, not product Rust behavior. Every child is reaped.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

SCRIPT = Path(__file__).parents[1] / 'instance/run-rust.sh'


class RunRustTests(unittest.TestCase):
    def test_exec_preserves_pid_exact_native_command_and_signal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = root / 'attempt'
            attempt.mkdir()
            (root / 'tools/ci').mkdir(parents=True)
            (root / 'tools/ci/test-run.py').write_text('''import json,os,pathlib,signal,sys,time
attempt=pathlib.Path(sys.argv[-1]).parent
signal.signal(signal.SIGTERM,lambda *a:(attempt.joinpath('signal').write_text('SIGTERM'),sys.exit(67)))
attempt.joinpath('observed.json').write_text(json.dumps({'pid':os.getpid(),'argv':sys.argv[1:]}))
time.sleep(30)
''')
            child = subprocess.Popen(['bash', str(SCRIPT.resolve()), str(attempt)], cwd=root,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                end = time.monotonic() + 5
                while not (attempt / 'observed.json').exists() and child.poll() is None and time.monotonic() < end:
                    time.sleep(0.01)
                self.assertTrue((attempt / 'observed.json').exists())
                observed = json.loads((attempt / 'observed.json').read_text())
                self.assertEqual(child.pid, int((attempt / 'batch.pid').read_text()))
                self.assertEqual(child.pid, observed['pid'])
                self.assertEqual(['batch', '--output', str(attempt / 'rust')], observed['argv'])
                child.send_signal(signal.SIGTERM)
                stdout, stderr = child.communicate(timeout=5)
                self.assertEqual(67, child.returncode, stderr)
                self.assertEqual('SIGTERM', (attempt / 'signal').read_text())
            finally:
                if child.poll() is None:
                    child.kill()
                child.communicate()

    def test_prior_output_is_rejected_without_starting_another_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            attempt = Path(directory)
            (attempt / 'rust').mkdir()
            result = subprocess.run(['bash', str(SCRIPT.resolve()), str(attempt)],
                                    text=True, capture_output=True)
            self.assertEqual(2, result.returncode)
            self.assertFalse((attempt / 'batch.pid').exists())
