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

"""Cancel the original batch on a spot notice and upload original partial files."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import pwd
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from aliyun_ci.handoff import pack_upload
from aliyun_ci.plan import parse_time


class Metadata:
    """ECS IMDSv2. Metadata endpoints and clocks are injectable for local tests."""
    def __init__(self, base_url='http://100.100.100.200', *, timeout=5):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout

    def read(self, path):
        request = urllib.request.Request(self.base_url + '/latest/api/token', method='PUT',
            headers={'X-aliyun-ecs-metadata-token-ttl-seconds': '60'})
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            token = response.read().decode()
        request = urllib.request.Request(self.base_url + path,
            headers={'X-aliyun-ecs-metadata-token': token})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read().decode()
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise


def process_uid(pid: int) -> int | None:
    try:
        status = Path(f'/proc/{pid}/status')
        if status.exists():
            line = next(value for value in status.read_text().splitlines() if value.startswith('Uid:'))
            return int(line.split()[1])
        result = subprocess.run(['ps', '-o', 'uid=', '-p', str(pid)], text=True, capture_output=True, check=False)
        return int(result.stdout.strip()) if result.stdout.strip() else None
    except (FileNotFoundError, ProcessLookupError):
        return None


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        status = Path(f'/proc/{pid}/stat')
        if status.exists() and status.read_text().rsplit(')', 1)[1].lstrip().startswith('Z'):
            return False
        return True
    except ProcessLookupError:
        return False


def cancel_batch(attempt_root, budget_seconds, *, uid, monotonic=time.monotonic,
                 sleep=time.sleep) -> dict:
    path = Path(attempt_root) / 'batch.pid'
    if not path.exists():
        return {'pid': None, 'signal_sent': False, 'exited': None, 'reason': 'batch has not started'}
    pid = int(path.read_text().strip())
    if pid <= 1:
        raise ValueError('invalid batch PID')
    owner = process_uid(pid)
    if owner is None:
        return {'pid': pid, 'signal_sent': False, 'exited': True}
    if owner != uid:
        raise ValueError('batch PID does not belong to the runner account')
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return {'pid': pid, 'signal_sent': False, 'exited': True}
    end = monotonic() + budget_seconds
    while alive(pid) and monotonic() < end:
        sleep(min(0.1, max(0, end - monotonic())))
    return {'pid': pid, 'signal_sent': True, 'exited': not alive(pid)}


def watch(config: dict, *, metadata=None, uid=None, now=None,
          monotonic=time.monotonic, sleep=time.sleep, upload=pack_upload) -> dict:
    metadata = metadata or Metadata()
    uid = pwd.getpwnam('runner').pw_uid if uid is None else uid
    now = now or (lambda: datetime.now(timezone.utc))
    deadline = parse_time(config['deadline'])
    while now() < deadline:
        notice = metadata.read('/latest/meta-data/instance/spot/termination-time')
        if notice and notice.strip():
            termination = parse_time(notice.strip())
            available = max(0, (min(termination, deadline) - now()).total_seconds())
            cancel_budget = min(config['watcher']['cancel_budget_seconds'], available)
            pid_root = config['pid_root'] if config.get('execution_kind') == 'local-rust' else config['attempt_root']
            cancellation = cancel_batch(pid_root, cancel_budget, uid=uid,
                                        monotonic=monotonic, sleep=sleep)
            attempt = Path(config['attempt_root'])
            # This reports the observed process fact, never a synthetic Rust exit.
            descriptor, name = tempfile.mkstemp(prefix='.spot-cancellation.', dir=attempt)
            temporary = Path(name)
            try:
                with os.fdopen(descriptor, 'w') as output:
                    os.fchmod(output.fileno(), 0o640)
                    os.fchown(output.fileno(), uid, attempt.stat().st_gid)
                    json.dump(cancellation, output, sort_keys=True)
                    output.write('\n')
                os.replace(temporary, attempt / 'spot-cancellation.json')
            finally:
                temporary.unlink(missing_ok=True)
            diagnostics = {}
            runner_diag = Path(config.get('runner_home', '/var/lib/novarocks-ci/runner/agent')) / '_diag'
            if runner_diag.is_dir():
                diagnostics['runner'] = runner_diag
            bootstrap_log = Path('/var/log/novarocks-ci/bootstrap.log')
            if bootstrap_log.is_file():
                diagnostics['bootstrap.log'] = bootstrap_log
            handoff = upload(attempt, diagnostics, kind='interrupted',
                             url=config['upload_urls']['interrupted'],
                             timeout=max(1, (min(termination, deadline) - now()).total_seconds()))
            return {'notice': notice.strip(), 'cancellation': cancellation, 'handoff': handoff}
        sleep(min(config['watcher']['poll_seconds'], max(0, (deadline - now()).total_seconds())))
    return {'notice': None, 'reason': 'run deadline reached'}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap', type=Path, default=Path('/run/novarocks-ci/bootstrap.json'))
    parser.add_argument('--metadata-url', default='http://100.100.100.200')
    args = parser.parse_args(argv)
    result = watch(json.loads(args.bootstrap.read_text()), metadata=Metadata(args.metadata_url))
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
