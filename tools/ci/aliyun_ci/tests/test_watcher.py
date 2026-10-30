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

"""A local IMDS/PUT server and a real process prove notice cancellation.

The test owns and reaps its process, closes HTTP servers and joins threads.
CI maintainers run it on watcher, bootstrap and handoff changes.
"""
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import unittest

from aliyun_ci.handoff import verify
from aliyun_ci.instance.watcher import Metadata, cancel_batch, watch


class WatcherTests(unittest.TestCase):
    def test_imds_notice_signals_original_process_waits_and_uploads_original_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready, stopped = root / 'ready', root / 'stopped'
            script = ('import pathlib,signal,time,sys; '
                      'signal.signal(signal.SIGTERM,lambda *a:(pathlib.Path(sys.argv[2]).write_text("SIGTERM"),sys.exit(0))); '
                      'pathlib.Path(sys.argv[1]).write_text("ready"); '
                      'time.sleep(30)')
            child = subprocess.Popen([sys.executable, '-c', script, str(ready), str(stopped)])
            received, requests = [], []
            now = datetime.now(timezone.utc)
            class Handler(BaseHTTPRequestHandler):
                def do_PUT(self):
                    requests.append(('PUT', self.path))
                    self.send_response(200)
                    self.end_headers()
                    if self.path == '/latest/api/token':
                        self.wfile.write(b'local-token')
                    else:
                        received.append(self.rfile.read(int(self.headers['Content-Length'])))
                def do_GET(self):
                    requests.append(('GET', self.path))
                    if self.headers.get('X-aliyun-ecs-metadata-token') != 'local-token':
                        self.send_response(401)
                        self.end_headers()
                        return
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write((now + timedelta(minutes=5)).isoformat().encode())
                def log_message(self, *args):
                    pass
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            server_thread = threading.Thread(target=server.serve_forever)
            server_thread.start()
            reaper = threading.Thread(target=child.wait)
            reaper.start()
            try:
                import time
                end = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < end:
                    time.sleep(0.01)
                self.assertTrue(ready.exists())
                (root / 'batch.pid').write_text(str(child.pid))
                (root / 'native-failure.log').write_text('original failure, never retried')
                base = f'http://127.0.0.1:{server.server_port}'
                config = {'deadline': (now + timedelta(minutes=10)).isoformat(),
                          'attempt_root': str(root), 'upload_urls': {'interrupted': base + '/archive'},
                          'watcher': {'poll_seconds': 1, 'cancel_budget_seconds': 3},
                          'runner_home': str(root / 'absent-runner')}
                result = watch(config, metadata=Metadata(base), uid=os.getuid())
                self.assertTrue(result['cancellation']['signal_sent'])
                self.assertTrue(result['cancellation']['exited'])
                self.assertEqual('SIGTERM', stopped.read_text())
                self.assertEqual(1, len(received))
                fact = verify(received[0])
                self.assertTrue(fact['manifest_ok'], fact)
                self.assertEqual('interrupted', fact['kind'])
                self.assertFalse(fact['complete'])
                self.assertEqual([('PUT', '/latest/api/token'),
                    ('GET', '/latest/meta-data/instance/spot/termination-time'), ('PUT', '/archive')], requests)
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait()
                reaper.join()
                server.shutdown()
                server.server_close()
                server_thread.join()

    def test_no_notice_stops_on_original_deadline(self):
        current = [datetime(2026, 10, 28, tzinfo=timezone.utc)]
        class NoNotice:
            def read(self, _):
                return None
        def advance(seconds):
            current[0] += timedelta(seconds=seconds)
        result = watch({'deadline': '2026-10-28T00:00:03Z', 'watcher': {'poll_seconds': 2}},
                       metadata=NoNotice(), uid=os.getuid(), now=lambda: current[0], sleep=advance)
        self.assertIsNone(result['notice'])
        self.assertEqual(3, current[0].second)

    def test_batch_not_started_is_a_gap_and_system_pids_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            result = cancel_batch(root, 1, uid=os.getuid())
            self.assertIsNone(result['exited'])
            (Path(root) / 'batch.pid').write_text('1')
            with self.assertRaisesRegex(ValueError, 'invalid batch PID'):
                cancel_batch(root, 1, uid=os.getuid())

    def test_local_notice_uses_root_control_pid_not_workload_pid_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);attempt=root/'attempt';attempt.mkdir();control=root/'control';control.mkdir()
            child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])
            reaper=threading.Thread(target=child.wait);reaper.start()
            try:
                (control/'batch.pid').write_text(str(child.pid))
                (attempt/'batch.pid').write_text('1')
                now=datetime.now(timezone.utc)
                class Notice:
                    def read(self,path):return (now+timedelta(minutes=5)).isoformat()
                config={'execution_kind':'local-rust','pid_root':str(control),'attempt_root':str(attempt),
                    'deadline':(now+timedelta(minutes=10)).isoformat(),
                    'watcher':{'poll_seconds':1,'cancel_budget_seconds':3},
                    'upload_urls':{'interrupted':'https://signed.invalid/partial'}}
                result=watch(config,metadata=Notice(),uid=os.getuid(),upload=lambda *a,**kw:{'uploaded':False})
                self.assertEqual(child.pid,result['cancellation']['pid'])
                self.assertTrue(result['cancellation']['exited'])
                self.assertEqual('1',(attempt/'batch.pid').read_text())
            finally:
                if child.poll() is None:child.kill()
                child.wait();reaper.join()

    def test_spot_handoff_can_resume_after_upload_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = root / 'attempt'
            attempt.mkdir()
            now = datetime.now(timezone.utc)
            class Notice:
                def read(self, path):
                    return (now + timedelta(minutes=5)).isoformat()
            config = {'execution_kind': 'local-rust', 'pid_root': str(root / 'control'),
                      'attempt_root': str(attempt), 'deadline': (now + timedelta(minutes=10)).isoformat(),
                      'watcher': {'poll_seconds': 1, 'cancel_budget_seconds': 0},
                      'upload_urls': {'interrupted': 'https://signed.invalid/partial'},
                      'runner_home': str(root / 'absent-runner')}
            calls = []
            def upload(path, diagnostics, **options):
                fact = Path(path) / 'spot-cancellation.json'
                observed = fact.stat()
                self.assertEqual(0o640, observed.st_mode & 0o777)
                self.assertEqual(os.getuid(), observed.st_uid)
                self.assertEqual(attempt.stat().st_gid, observed.st_gid)
                calls.append(json.loads(fact.read_text()))
                if len(calls) == 1:
                    raise ConnectionError('transient upload failure')
                return {'uploaded': True}
            with self.assertRaises(ConnectionError):
                watch(config, metadata=Notice(), uid=os.getuid(), now=lambda: now, upload=upload)
            result = watch(config, metadata=Notice(), uid=os.getuid(), now=lambda: now, upload=upload)
            self.assertTrue(result['handoff']['uploaded'])
            self.assertEqual(2, len(calls))
            self.assertFalse(calls[0]['signal_sent'])
            self.assertIsNone(calls[0]['exited'])
            self.assertEqual([], list(attempt.glob('.spot-cancellation.*')))

    def test_spot_fact_replaces_link_without_overwriting_its_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            attempt = root / 'attempt'
            attempt.mkdir()
            target = root / 'protected-target'
            target.write_text('original target bytes')
            marker = attempt / 'spot-cancellation.json'
            marker.symlink_to(target)
            now = datetime.now(timezone.utc)
            class Notice:
                def read(self, path):
                    return (now + timedelta(minutes=5)).isoformat()
            config = {'execution_kind': 'local-rust', 'pid_root': str(root / 'control'),
                      'attempt_root': str(attempt), 'deadline': (now + timedelta(minutes=10)).isoformat(),
                      'watcher': {'poll_seconds': 1, 'cancel_budget_seconds': 0},
                      'upload_urls': {'interrupted': 'https://signed.invalid/partial'},
                      'runner_home': str(root / 'absent-runner')}
            observed = []
            def upload(path, diagnostics, **options):
                observed.append(json.loads((Path(path) / 'spot-cancellation.json').read_text()))
                return {'uploaded': True}
            result = watch(config, metadata=Notice(), uid=os.getuid(), now=lambda: now, upload=upload)
            self.assertTrue(result['handoff']['uploaded'])
            self.assertEqual('original target bytes', target.read_text())
            self.assertFalse(marker.is_symlink())
            self.assertEqual(0o640, marker.stat().st_mode & 0o777)
            self.assertEqual(os.getuid(), marker.stat().st_uid)
            self.assertEqual(1, len(observed))
            self.assertFalse(observed[0]['signal_sent'])
            self.assertEqual([], list(attempt.glob('.spot-cancellation.*')))
