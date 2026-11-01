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

"""Consume one encrypted bootstrap envelope, then launch an unprivileged JIT runner.

The image installs this package and tools-source as root-owned files. Worker
code has no writable path to this launcher or the root-only JIT file.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import subprocess
import tarfile
import tempfile
import time
import urllib.request

from aliyun_ci.plan import parse_time
from aliyun_ci.ports import APIError
from aliyun_ci.start import NOT_FOUND
from .watcher import Metadata


RUNTIME = Path('/run/novarocks-ci')
RUNNER_HOME = Path('/var/lib/novarocks-ci/runner/agent')
ATTEMPT = Path('/var/lib/novarocks-ci/attempt')
TOOLS_SOURCE = Path('/opt/novarocks-ci/tools-source')


def consume_envelope(oos, secret_name: str) -> dict:
    root = json.loads(oos.call('GetSecretParameter', Name=secret_name, WithDecryption=True)['Parameter']['Value'])
    if root.get('schema') != 1 or type(root.get('parts')) is not int or root['parts'] < 1:
        raise ValueError('invalid encrypted bootstrap index')
    chunks = [f'{secret_name}-part-{i:06d}' for i in range(root['parts'])]
    encoded = ''.join(oos.call('GetSecretParameter', Name=name, WithDecryption=True)['Parameter']['Value']
                      for name in chunks)
    data = base64.b64decode(encoded, validate=True)
    if hashlib.sha256(data).hexdigest() != root['sha256']:
        raise ValueError('encrypted bootstrap content SHA-256 differs')
    payload = json.loads(data)
    for name in [*chunks, secret_name]:
        try:
            oos.call('DeleteSecretParameter', Name=name)
        except APIError as error:
            if error.code not in NOT_FOUND:
                raise
    return payload


def await_envelope(oos, pointer: dict, *, now=None, sleep=time.sleep):
    now = now or (lambda: datetime.now(timezone.utc))
    deadline = parse_time(pointer['deadline'])
    while now() < deadline:
        try:
            return consume_envelope(oos, pointer['secret_name'])
        except APIError as error:
            if error.code not in NOT_FOUND:
                raise
            # The control job publishes this only after instance readback. The
            # pointer deadline also bounds a control job that never publishes.
            sleep(min(5, max(0, (deadline - now()).total_seconds())))
    raise TimeoutError('bootstrap envelope was not published before the run deadline')


def _write(path: Path, value, *, mode, uid, gid):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, 'w') as output:
        os.fchmod(output.fileno(), mode)
        os.fchown(output.fileno(), uid, gid)
        json.dump(value, output, sort_keys=True)
        output.write('\n')


def download_runner(runner: dict, destination: Path, *, opener=urllib.request.urlopen):
    with tempfile.TemporaryDirectory(prefix='novarocks-runner-download-') as directory:
        downloaded = Path(directory) / 'runner.tar.gz'
        with opener(runner['url'], timeout=60) as source, downloaded.open('xb') as output:
            shutil.copyfileobj(source, output)
        with downloaded.open('rb') as source:
            if hashlib.file_digest(source, 'sha256').hexdigest() != runner['sha256']:
                raise ValueError('runner download SHA-256 differs')
        with tarfile.open(downloaded, 'r:gz') as archive:
            archive.extractall(destination, filter='data')
    if not (destination / 'run.sh').is_file():
        raise ValueError('runner archive contains no run.sh')


def install(payload: dict, *, uid: int, gid: int, runtime=RUNTIME,
            runner_home=RUNNER_HOME, attempt_root=ATTEMPT, tools_source=TOOLS_SOURCE,
            run=subprocess.run, downloader=download_runner):
    """Prepare files once; only trusted image code is run before privilege drop."""
    kind = payload.get('execution_kind', 'github-runner')
    if kind == 'local-rust':
        from .local_rust import install as install_local
        return install_local(payload, uid=uid, gid=gid)
    if kind != 'github-runner':
        raise ValueError('unsupported bootstrap execution kind')
    if not isinstance(payload.get('jit'), str) or not payload['jit']:
        raise ValueError('bootstrap requires an encoded JIT configuration')
    if payload['attempt_root'] != str(attempt_root):
        raise ValueError('bootstrap attempt root differs from the installed worker path')
    parse_time(payload['deadline'])
    runtime, runner_home, attempt_root = Path(runtime), Path(runner_home), Path(attempt_root)
    if runner_home.exists() and any(runner_home.iterdir()):
        raise ValueError('runner home contains prior runtime state')
    runtime.mkdir(mode=0o750, parents=True, exist_ok=True)
    os.chown(runtime, 0 if os.geteuid() == 0 else os.geteuid(), gid)
    os.chmod(runtime, 0o750)
    runner_home.mkdir(mode=0o750, parents=True, exist_ok=True)
    attempt_root.mkdir(mode=0o750, parents=True, exist_ok=False)
    os.chown(attempt_root, uid, gid)
    checked = run([str(tools_source / 'tools/dev/linux-env.sh'), 'check', '--user', 'runner',
                   '--profile', str(tools_source / 'tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json'),
                   '--tools-only'], check=True, capture_output=True, text=True)
    tools_fact = json.loads(checked.stdout)
    _write(attempt_root / 'tools-check.json', tools_fact, mode=0o640, uid=uid, gid=gid)
    downloader(payload['runner'], runner_home)
    for directory, subdirectories, files in os.walk(runner_home, followlinks=False):
        os.chown(directory, uid, gid)
        for name in subdirectories + files:
            os.chown(Path(directory) / name, uid, gid, follow_symlinks=False)
    # The official runtime must load successfully under the final account before
    # any JIT is handed to it. Missing ABI dependencies fail without repair.
    version = run(['runuser', '-u', 'runner', '--', str(runner_home / 'bin/Runner.Listener'),
                   '--version'], check=True, capture_output=True, text=True).stdout.strip()
    if version != payload['runner']['version']:
        raise ValueError('installed Runner version differs from the synchronized release')
    # URLs are the only secrets exposed at rest to the runner account. Keep JIT
    # in a separate root-only file until the privileged launcher consumes it.
    public = {key: payload[key] for key in ('deadline', 'attempt_root', 'upload_urls', 'watcher', 'identity')}
    public['runner_version'] = version
    public['runner_home'] = str(runner_home)
    root_uid = 0 if os.geteuid() == 0 else os.geteuid()
    _write(runtime / 'bootstrap.json', public, mode=0o640, uid=root_uid, gid=gid)
    _write(runtime / 'jit.json', payload['jit'], mode=0o600, uid=root_uid,
           gid=0 if os.geteuid() == 0 else os.getegid())
    run(['systemctl', 'start', '--no-block', 'novarocks-runner.service',
         'novarocks-spot-watcher.service'], check=True)
    return {'installed': True, 'runner_version': version, 'tools_check': tools_fact}


def launch_runner(*, runtime=RUNTIME, runner_home=RUNNER_HOME, account=None,
                  drop_groups=os.initgroups, drop_gid=os.setgid, drop_uid=os.setuid,
                  execute=os.execve):
    """Read and destroy root-only JIT, drop privilege, then execute runner code."""
    account = account or pwd.getpwnam('runner')
    path = Path(runtime) / 'jit.json'
    if path.stat().st_mode & 0o077:
        raise ValueError('JIT file must be root-only')
    jit = json.loads(path.read_text())
    if not isinstance(jit, str) or not jit:
        raise ValueError('invalid encoded JIT configuration')
    path.unlink()
    drop_groups(account.pw_name, account.pw_gid)
    drop_gid(account.pw_gid)
    drop_uid(account.pw_uid)
    os.chdir(runner_home)
    environment = {'HOME': account.pw_dir, 'USER': account.pw_name, 'LOGNAME': account.pw_name,
                   'PATH': f'{account.pw_dir}/.cargo/bin:/usr/local/bin:/usr/bin:/bin',
                   'PYTHONDONTWRITEBYTECODE': '1'}
    script = 'set -e; . "$HOME/.config/novarocks/linux-env.sh"; exec ./run.sh --jitconfig "$1"'
    execute('/bin/bash', ['bash', '-c', script, 'novarocks-runner', jit], environment)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--launch-runner', action='store_true')
    parser.add_argument('--metadata-url', default='http://100.100.100.200')
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        parser.error('bootstrap and JIT launcher require root')
    if args.launch_runner:
        launch_runner()
        return 0
    pointer = json.loads(Metadata(args.metadata_url).read('/latest/user-data'))
    if pointer.get('role') == 'builder':
        print(json.dumps({'execution_kind': 'builder', 'installed': False}, sort_keys=True))
        return 0
    from aliyun_ci.sdk import RPC
    payload = await_envelope(RPC('oos', pointer['region']), pointer)
    account = pwd.getpwnam('runner')
    print(json.dumps(install(payload, uid=account.pw_uid, gid=account.pw_gid), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
