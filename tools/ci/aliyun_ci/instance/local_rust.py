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

"""Run the original local Rust batch with root-owned control and evidence transport.

Only image-owned code runs as root. Source, tools-input data and native batch
execute under the ordinary runner account; URLs stay in the root-only runtime.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pwd
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

from aliyun_ci.handoff import pack_upload
from aliyun_ci.config import LOCAL_CARGO_REGISTRIES
from aliyun_ci.plan import parse_time
from aliyun_ci.source_bundle import import_bundle
from .swap import LOCAL_SWAP_GIB, SwapSetupError, configure_swap
from .resource_observation import snapshot as resource_snapshot

RUNTIME = Path('/run/novarocks-ci')
SOURCE = Path('/var/lib/novarocks-ci/source')
TOOLS_SOURCE = Path('/opt/novarocks-ci/tools-source')
RSPROXY_CARGO_CONFIG = ("# Managed by NovaRocks local Rust worker.\n"
    "[source.crates-io]\nreplace-with = 'rsproxy-sparse'\n"
    "[source.rsproxy-sparse]\nregistry = 'sparse+https://rsproxy.cn/index/'\n")


def _atomic(path, value, *, mode=0o600, uid=None, gid=None):
    # The runner can create names in the attempt directory. Exclusive random
    # creation prevents root from following a preexisting temporary symlink;
    # replace also replaces the destination link itself without following it.
    descriptor, name = tempfile.mkstemp(prefix=f'.{path.name}.', dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, 'w') as output:
            os.fchmod(output.fileno(), mode)
            if uid is not None:
                os.fchown(output.fileno(), uid, gid)
            json.dump(value, output, sort_keys=True)
            output.write('\n')
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _validate(payload):
    if payload.get('execution_kind') != 'local-rust' or payload.get('task') != 'rust-workspace':
        raise ValueError('local worker requires explicit local-rust rust-workspace task')
    parse_time(payload['deadline'])
    if not Path(payload['attempt_root']).is_absolute():
        raise ValueError('local attempt root must be absolute')
    if not isinstance(payload.get('identity'), dict) or not payload['identity'].get('run_id'):
        raise ValueError('local task requires original run identity')
    if not isinstance(payload.get('source'), dict) or not payload['source'].get('url'):
        raise ValueError('local task requires a source manifest and read capability')
    if payload.get('cargo_registry', 'crates-io') not in LOCAL_CARGO_REGISTRIES:
        raise ValueError('unsupported local Cargo registry')
    swap_gib = payload.get('swap_gib', 0)
    if type(swap_gib) is not int or swap_gib not in LOCAL_SWAP_GIB:
        raise ValueError('unsupported local swap_gib')


def configure_cargo_registry(selection, *, account):
    """Select one explicit mirror without changing source identity or unknown config."""
    if selection not in LOCAL_CARGO_REGISTRIES:
        raise ValueError('unsupported local Cargo registry')
    home = Path(account.pw_dir) / '.cargo'
    path = home / 'config.toml'
    fact = {'selection': selection, 'cargo_home': str(home), 'managed': False,
            'config_path': str(path), 'config_sha256': None}
    if selection == 'crates-io':
        return fact
    if home.is_symlink() or (home.exists() and not home.is_dir()):
        raise ValueError('Cargo home must be an ordinary directory')
    legacy = home / 'config'
    if legacy.exists() or legacy.is_symlink():
        raise ValueError('refusing to override existing legacy Cargo configuration')
    expected = RSPROXY_CARGO_CONFIG.encode()
    if path.exists() or path.is_symlink():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != expected:
            raise ValueError('refusing to override unmanaged Cargo configuration')
    else:
        if not home.exists():
            home.mkdir(mode=0o750, parents=True)
            os.chown(home, account.pw_uid, account.pw_gid)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(descriptor, 'wb') as output:
            os.fchmod(output.fileno(), 0o644)
            os.fchown(output.fileno(), account.pw_uid, account.pw_gid)
            output.write(expected)
    actual = path.read_bytes()
    if actual != expected:
        raise ValueError('local Cargo registry configuration readback differs')
    return dict(fact, managed=True, config_sha256=hashlib.sha256(actual).hexdigest(),
                config_text=actual.decode())


def install(payload, *, uid, gid, runtime=RUNTIME, source_root=SOURCE,
            run=subprocess.run, start_services=True):
    """Persist secret control inputs; service performs source/check/run/handoff."""
    _validate(payload)
    runtime, source_root = Path(runtime), Path(source_root)
    runtime.mkdir(mode=0o750, parents=True, exist_ok=True)
    os.chmod(runtime, 0o750)
    attempt = Path(payload['attempt_root'])
    attempt.mkdir(mode=0o750, parents=True, exist_ok=False)
    os.chown(attempt, uid, gid)
    if source_root.exists():
        raise ValueError('local source root contains prior runtime state')
    destination = runtime / 'bootstrap.json'
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w') as output:
        os.fchmod(output.fileno(), 0o600)
        json.dump(dict(payload, pid_root=str(runtime)), output, sort_keys=True)
    if start_services:
        run(['systemctl', 'start', '--no-block', 'novarocks-local-rust.service',
             'novarocks-spot-watcher.service'], check=True)
    return {'installed': True, 'execution_kind': 'local-rust'}


def check_tools(source, *, tools_source=TOOLS_SOURCE, user='runner'):
    """Consume task definitions through the immutable image check implementation."""
    owner = Path(tools_source) / 'tools/dev/linux_env.py'
    spec = importlib.util.spec_from_file_location('novarocks_image_linux_env', owner)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    source = Path(source)
    checker = module.LinuxEnvironment(source,
        module.read_object(source / 'tools/dev/linux-env.inputs.json'),
        module.read_object(source / 'tools/dev/profiles/aliyun-ubuntu26-amd64-tools.json'),
        module.Account.lookup(user), tools_only=True)
    return checker.check()


def prepare_source(payload, source_root, runtime, *, opener=urllib.request.urlopen):
    bundle = Path(runtime) / 'source.bundle'
    with opener(payload['source']['url'], timeout=60) as remote, bundle.open('xb') as output:
        os.fchmod(output.fileno(), 0o600)
        shutil.copyfileobj(remote, output)
    carrier = payload['source']
    if carrier.get('manifest_url'):
        with opener(carrier['manifest_url'], timeout=60) as remote:
            raw = remote.read()
        if hashlib.sha256(raw).hexdigest() != carrier['manifest_sha256']:
            raise ValueError('source manifest SHA-256 differs')
        manifest = json.loads(raw)
        if any(manifest.get(key) != carrier.get(key) for key in ('sha', 'tree', 'bundle_sha256')):
            raise ValueError('source manifest identity differs from the original request')
    else:
        manifest = {key: value for key, value in carrier.items() if key != 'url'}
    result = import_bundle(bundle, manifest, source_root)
    return result


def _chown_tree(root, uid, gid):
    for directory, subdirectories, files in os.walk(root, followlinks=False):
        os.chown(directory, uid, gid, follow_symlinks=False)
        for name in subdirectories + files:
            os.chown(Path(directory) / name, uid, gid, follow_symlinks=False)


def execute_batch(command, *, source_root, attempt_root, runtime, account,
                  environment, deadline=None, cancel_budget_seconds=120,
                  now=None, popen=subprocess.Popen):
    """Observe the actual original child, forwarding SIGTERM without fake exits.

    The command is an internal injection seam for real short-process tests. The
    production caller alone chooses the fixed original Rust batch command.
    """
    now = now or (lambda: datetime.now(timezone.utc))
    attempt, runtime = Path(attempt_root), Path(runtime)
    def cancellation_exists():
        return any((runtime / name).exists() for name in ('local-cancel.json', 'cancellation.json')) or (attempt / 'spot-cancellation.json').exists()
    if cancellation_exists():
        return {'exit_code': None, 'pid': None, 'exited': True,
                'cancel_requested': True, 'reason': 'cancelled before native batch launch'}
    process, requested = None, []
    start_groups, observation_errors, observation_hashes = None, [], {}
    def observe_resources(phase, result):
        nonlocal start_groups
        try:
            fact = resource_snapshot(process.pid, phase, exited=result.get('exited', False),
                exit_code=result.get('exit_code'), start_groups=start_groups if phase == 'end' else None)
            if phase == 'start':
                start_groups = fact.get('original_cgroups', [])
        except Exception as error:
            fact = {'phase': phase, 'pid': process.pid, 'time': now().isoformat(),
                    'actual_process_exit_observed': result.get('exited'),
                    'actual_exit_code': result.get('exit_code'), 'error': type(error).__name__}
        filename = f'resources-{phase}.json'
        try:
            _atomic(runtime / filename, fact)
            observation_hashes[phase] = {'filename': filename,
                'sha256': hashlib.sha256((runtime / filename).read_bytes()).hexdigest()}
        except Exception as error:
            observation_errors.append({'phase': phase, 'copy': 'root', 'error': type(error).__name__})
        try:
            _atomic(attempt / filename, fact, mode=0o640,
                    uid=account.pw_uid, gid=account.pw_gid)
        except Exception as error:
            observation_errors.append({'phase': phase, 'copy': 'attempt', 'error': type(error).__name__})
        result = dict(result, resource_observations=observation_hashes)
        return dict(result, resource_observation_errors=observation_errors) if observation_errors else result
    cancellation_deadline = None
    def stop(signum, frame):
        nonlocal cancellation_deadline
        requested.append(signum)
        cancellation_deadline = time.monotonic() + cancel_budget_seconds
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGTERM)
    previous = {signum: signal.signal(signum, stop) for signum in (signal.SIGTERM, signal.SIGINT)}
    try:
        with (attempt / 'stdout.log').open('xb') as stdout, (attempt / 'stderr.log').open('xb') as stderr:
            for stream in (stdout, stderr):
                os.fchmod(stream.fileno(), 0o640)
                os.fchown(stream.fileno(), account.pw_uid, account.pw_gid)
            options = dict(cwd=source_root, env=environment, stdout=stdout, stderr=stderr,
                           start_new_session=True)
            if os.geteuid() == 0:
                options.update(user=account.pw_uid, group=account.pw_gid, extra_groups=[])
            elif os.geteuid() != account.pw_uid:
                raise ValueError('cannot launch batch as the requested ordinary account')
            process = popen(command, **options)
            for root in (runtime, attempt):
                with (root / 'batch.pid').open('x') as output:
                    os.fchmod(output.fileno(), 0o600 if root == runtime else 0o640)
                    if root == attempt:
                        os.fchown(output.fileno(), account.pw_uid, account.pw_gid)
                    output.write(str(process.pid) + '\n')
            _atomic(runtime / 'batch-identity.json', {'pid': process.pid,
                'uid': account.pw_uid, 'started_at': now().isoformat()})
            observe_resources('start', {})
            while process.poll() is None:
                if cancellation_deadline is None and cancellation_exists():
                    requested.append(signal.SIGTERM)
                    try:
                        process.send_signal(signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    cancellation_deadline = time.monotonic() + cancel_budget_seconds
                if deadline is not None and now() >= deadline and cancellation_deadline is None:
                    requested.append(signal.SIGTERM)
                    process.send_signal(signal.SIGTERM)
                    cancellation_deadline = time.monotonic() + cancel_budget_seconds
                if cancellation_deadline is not None and time.monotonic() >= cancellation_deadline:
                    # Leave raw exit unknown; cloud auto-release owns the final
                    # machine bound. Root never claims native cleanup completed.
                    return observe_resources('end', {'exit_code': None, 'pid': process.pid, 'exited': False,
                            'cancel_requested': True, 'reason': 'native cancellation budget expired'})
                time.sleep(0.1)
            return observe_resources('end', {'exit_code': process.wait(), 'pid': process.pid, 'exited': True,
                    'cancel_requested': bool(requested)})
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def run(payload, *, runtime=RUNTIME, source_root=SOURCE, tools_source=TOOLS_SOURCE,
        account=None, source_preparer=prepare_source, tools_checker=check_tools,
        executor=execute_batch, uploader=pack_upload, now=None):
    _validate(payload)
    now = now or (lambda: datetime.now(timezone.utc))
    account = account or pwd.getpwnam('runner')
    runtime, source_root, attempt = Path(runtime), Path(source_root), Path(payload['attempt_root'])
    status = {'schema': 1, 'identity': payload['identity'], 'state': 'preparing',
              'exit_code': None, 'tools_check_exit': None, 'handoff': None}
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    status['service_rlimit_nofile'] = {'soft': soft, 'hard': hard}
    def publish():
        _atomic(runtime / 'local-status.json', status)
        _atomic(attempt / 'status.json', status, mode=0o640, uid=account.pw_uid, gid=account.pw_gid)
    publish()
    kind = 'final'
    try:
        source = source_preparer(payload, source_root, runtime)
        _atomic(attempt / 'source.json', source, mode=0o640, uid=account.pw_uid, gid=account.pw_gid)
        try:
            tools_fact = tools_checker(source_root, tools_source=tools_source, user=account.pw_name)
            status['tools_check_exit'] = 0
            _atomic(attempt / 'tools-check.json', tools_fact, mode=0o640, uid=account.pw_uid, gid=account.pw_gid)
        except Exception:
            status['tools_check_exit'] = 1
            raise
        finally:
            _atomic(attempt / 'tools-check-result.json', {'exit_code': status['tools_check_exit']}, mode=0o640, uid=account.pw_uid, gid=account.pw_gid)
        registry = configure_cargo_registry(payload.get('cargo_registry', 'crates-io'), account=account)
        status['cargo_registry'] = registry
        _atomic(runtime / 'cargo-registry.json', registry)
        _atomic(attempt / 'cargo-registry.json', registry, mode=0o640, uid=account.pw_uid, gid=account.pw_gid)
        swap_fact = None
        try:
            swap_fact = configure_swap(payload.get('swap_gib', 0), timeout=max(0,
                (parse_time(payload.get('work_deadline', payload['deadline'])) - now()).total_seconds()))
        except SwapSetupError as error:
            swap_fact = error.fact
            raise
        finally:
            if swap_fact is not None:
                status['swap'] = swap_fact
                _atomic(runtime / 'swap.json', swap_fact)
                _atomic(attempt / 'swap.json', swap_fact, mode=0o640, uid=account.pw_uid, gid=account.pw_gid)
        _chown_tree(source_root, account.pw_uid, account.pw_gid)
        status['state'] = 'running'
        publish()
        environment = {'HOME': account.pw_dir, 'USER': account.pw_name,
            'LOGNAME': account.pw_name,
            'PATH': f'{account.pw_dir}/.cargo/bin:/usr/local/bin:/usr/bin:/bin',
            'CARGO_HTTP_MULTIPLEXING': 'false',
            'PYTHONDONTWRITEBYTECODE': '1'}
        if registry['managed']:
            environment['CARGO_HOME'] = registry['cargo_home']
        if payload.get('build_jobs') is not None:
            if type(payload['build_jobs']) is not int or payload['build_jobs'] <= 0:
                raise ValueError('build_jobs must be a positive integer')
            environment['CARGO_BUILD_JOBS'] = str(payload['build_jobs'])
        script = 'set -e; . "$HOME/.config/novarocks/linux-env.sh"; exec python3 tools/ci/test-run.py batch --output "$1"'
        command = ['/bin/bash', '-c', script, 'novarocks-local-rust', str(attempt / 'rust')]
        work_deadline = parse_time(payload.get('work_deadline', payload['deadline']))
        result = executor(command, source_root=source_root, attempt_root=attempt,
            runtime=runtime, account=account, environment=environment,
            deadline=work_deadline, cancel_budget_seconds=payload['watcher']['cancel_budget_seconds'])
        status.update(exit_code=result['exit_code'], batch_pid=result['pid'], batch=result)
        if result['cancel_requested'] or any((runtime / name).exists() for name in
                ('local-cancel.json', 'cancellation.json')) or (attempt / 'spot-cancellation.json').exists():
            kind = 'interrupted'
    except Exception as error:
        # Never serialize exception text which can contain a presigned URL.
        status['platform_error'] = type(error).__name__
    status['state'] = 'archiving'
    publish()
    try:
        remaining = max(1, (parse_time(payload['deadline']) - now()).total_seconds())
        status['handoff'] = uploader(attempt, kind=kind, url=payload['upload_urls'][kind], timeout=remaining)
    except Exception as error:
        status['handoff'] = {'uploaded': None, 'error': type(error).__name__}
    status['state'] = 'complete'
    publish()
    return status


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bootstrap', type=Path, default=RUNTIME / 'bootstrap.json')
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        parser.error('local task supervisor requires root')
    if args.bootstrap.stat().st_mode & 0o077:
        parser.error('local bootstrap must be root-only')
    result = run(json.loads(args.bootstrap.read_text()))
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get('exit_code') == 0 and result['handoff'].get('uploaded') is True else 1


if __name__ == '__main__':
    raise SystemExit(main())
