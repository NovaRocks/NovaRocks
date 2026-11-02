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

"""Local supervisor correctness with real short children and original archives.

The fixtures substitute a short source-owned batch, not process exit or signal
observation. CI maintainers run these when local execution/control changes.
"""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import pwd
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from aliyun_ci import handoff, source_bundle
from aliyun_ci.instance import local_rust


class LocalRustTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.runtime = self.root / 'runtime'
        self.attempt = self.root / 'attempt'
        self.source = self.root / 'source'
        self.account = pwd.getpwuid(os.getuid())
        self.payload = {'execution_kind': 'local-rust', 'task': 'rust-workspace',
            'identity': {'run_id': 'local-test', 'run_attempt': 1},
            'deadline': (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            'attempt_root': str(self.attempt),
            'source': {'url': 'https://signed.invalid/private-capability'},
            'upload_urls': {'final': 'https://signed.invalid/upload',
                            'interrupted': 'https://signed.invalid/partial'},
            'watcher': {'cancel_budget_seconds': 2, 'poll_seconds': 1}}

    def install(self):
        return local_rust.install(self.payload, uid=os.getuid(), gid=os.getgid(),
            runtime=self.runtime, source_root=self.source, start_services=False)

    def test_install_keeps_urls_root_only_and_does_not_start_test(self):
        self.assertTrue(self.install()['installed'])
        self.assertEqual(0o600, (self.runtime / 'bootstrap.json').stat().st_mode & 0o777)
        self.assertFalse((self.attempt / 'batch.pid').exists())
        self.assertEqual(str(self.runtime),json.loads((self.runtime/'bootstrap.json').read_text())['pid_root'])
        self.assertNotIn('pid_root',self.payload)
        self.assertFalse(any(self.attempt.iterdir()))
        with self.assertRaises(FileExistsError):
            self.install()

    def test_nonzero_original_batch_still_archives_logs_and_source_without_capabilities(self):
        self.payload['build_jobs']=2
        self.install()
        self.source.mkdir()
        tools = self.source / 'tools/ci'
        tools.mkdir(parents=True)
        (tools / 'test-run.py').write_text("import json, pathlib, sys\nroot=pathlib.Path(sys.argv[-1]); root.mkdir()\n(root/'execution-index.json').write_text(json.dumps({'native_exit':7}))\nimport os; assert os.environ['CARGO_BUILD_JOBS']=='2'\nassert os.environ['CARGO_HTTP_MULTIPLEXING']=='false'\nprint('actual stdout'); print('actual stderr',file=sys.stderr)\nraise SystemExit(7)\n")
        # The real child sources its ordinary HOME environment, isolated per test.
        home = self.root / 'home'
        (home / '.config/novarocks').mkdir(parents=True)
        (home / '.config/novarocks/linux-env.sh').write_text(f'export PATH={Path(sys.executable).parent}:/usr/bin:/bin\n')
        from types import SimpleNamespace
        account = SimpleNamespace(pw_name=self.account.pw_name, pw_uid=os.getuid(),
            pw_gid=os.getgid(), pw_dir=str(home))
        packed = []
        def upload(attempt, *, kind, url, timeout):
            archive = handoff.pack(attempt, kind=kind, output=self.root / f'{kind}.tar.gz')
            fact = handoff.verify(archive)
            self.assertTrue(fact['manifest_ok'])
            packed.append((archive, kind))
            return {'uploaded': True, 'sha256': fact['archive_sha256']}
        result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
            account=account, source_preparer=lambda *_: {'sha': 'a' * 40, 'tree': 'b' * 40},
            tools_checker=lambda *args, **kwargs: {'action': 'check'}, uploader=upload)
        self.assertEqual(7, result['exit_code'])
        self.assertTrue(result['batch']['exited'])
        self.assertEqual('complete', result['state'])
        self.assertEqual('actual stdout\n', (self.attempt / 'stdout.log').read_text())
        self.assertEqual('actual stderr\n', (self.attempt / 'stderr.log').read_text())
        self.assertEqual({'native_exit': 7}, json.loads((self.attempt / 'rust/execution-index.json').read_text()))
        self.assertEqual('final', packed[0][1])
        import tarfile
        import hashlib
        with tarfile.open(packed[0][0], mode='r:gz') as archive:
            for phase in ('start', 'end'):
                filename=f'resources-{phase}.json'
                archived=archive.extractfile(f'attempt/{filename}').read()
                private=self.runtime/filename
                self.assertEqual(0o600,private.stat().st_mode & 0o777)
                self.assertEqual(private.read_bytes(),archived)
                self.assertEqual(private.read_bytes(),(self.attempt/filename).read_bytes())
                expected=result['batch']['resource_observations'][phase]
                self.assertEqual(filename,expected['filename'])
                self.assertEqual(hashlib.sha256(private.read_bytes()).hexdigest(),expected['sha256'])
                fact=json.loads(archived)
                self.assertEqual(phase, fact['phase'])
                self.assertEqual(result['batch_pid'], fact['pid'])
        end = json.loads((self.attempt / 'resources-end.json').read_text())
        self.assertTrue(end['actual_process_exit_observed'])
        self.assertEqual(7, end['actual_exit_code'])
        self.assertEqual(result, json.loads((self.runtime / 'local-status.json').read_text()))
        self.assertNotIn('private-capability', (self.attempt / 'status.json').read_text())
        self.assertNotIn('signed.invalid', str(result))
        self.assertEqual(0o600, (self.runtime / 'batch.pid').stat().st_mode & 0o777)
        for name in ('status.json','source.json','tools-check.json','tools-check-result.json','stdout.log','stderr.log','batch.pid'):
            self.assertEqual(account.pw_uid,(self.attempt/name).stat().st_uid)
            self.assertEqual(0o640,(self.attempt/name).stat().st_mode & 0o777)

    def test_resource_read_failure_does_not_replace_original_real_exit(self):
        from unittest.mock import patch
        self.runtime.mkdir(); self.attempt.mkdir(); self.source.mkdir()
        with patch.object(local_rust, 'resource_snapshot', side_effect=PermissionError('controlled read failure')):
            result = local_rust.execute_batch([sys.executable, '-c', 'raise SystemExit(7)'],
                source_root=self.source, attempt_root=self.attempt, runtime=self.runtime,
                account=self.account, environment={'PATH': '/usr/bin:/bin'})
        self.assertEqual(7, result['exit_code']); self.assertTrue(result['exited'])
        for phase in ('start', 'end'):
            path = self.attempt / f'resources-{phase}.json'
            self.assertEqual('PermissionError', json.loads(path.read_text())['error'])
            self.assertEqual(0o640, path.stat().st_mode & 0o777)

    def test_rsproxy_config_is_read_by_real_child_and_retained_in_original_archive(self):
        from types import SimpleNamespace
        import hashlib
        self.payload.update(cargo_registry='rsproxy', build_jobs=8)
        self.install()
        self.source.mkdir()
        lock = self.source / 'Cargo.lock'; lock.write_bytes(b'original locked input\n')
        tools = self.source / 'tools/ci'; tools.mkdir(parents=True)
        (tools / 'test-run.py').write_text("""import hashlib,json,os,pathlib,sys,tomllib
root=pathlib.Path(sys.argv[-1]);root.mkdir()
assert os.environ['CARGO_BUILD_JOBS']=='8'
assert os.environ['CARGO_HTTP_MULTIPLEXING']=='false'
config=pathlib.Path(os.environ['CARGO_HOME'])/'config.toml'
raw=config.read_bytes();parsed=tomllib.loads(raw.decode())
assert parsed['source']['crates-io']['replace-with']=='rsproxy-sparse'
assert parsed['source']['rsproxy-sparse']['registry']=='sparse+https://rsproxy.cn/index/'
(root/'observed-cargo-config.json').write_text(json.dumps({'sha256':hashlib.sha256(raw).hexdigest(),'uid':os.getuid()}))
raise SystemExit(7)
""")
        home = self.root / 'home'; (home / '.config/novarocks').mkdir(parents=True)
        (home / '.config/novarocks/linux-env.sh').write_text(
            f'export PATH={Path(sys.executable).parent}:/usr/bin:/bin\nexport CARGO_HOME="$HOME/.cargo"\n')
        account = SimpleNamespace(pw_name=self.account.pw_name, pw_uid=os.getuid(),
            pw_gid=os.getgid(), pw_dir=str(home))
        archives = []
        def upload(attempt, *, kind, url, timeout):
            archive = handoff.pack(attempt, kind=kind, output=self.root / 'original.tar.gz')
            archives.append(archive)
            return {'uploaded': handoff.verify(archive)['manifest_ok']}
        result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
            account=account, source_preparer=lambda *_: {'sha': 'a' * 40},
            tools_checker=lambda *args, **kwargs: {'action': 'check'}, uploader=upload)
        self.assertEqual(7, result['exit_code'])
        self.assertTrue(result['batch']['exited'])
        fact = json.loads((self.attempt / 'cargo-registry.json').read_text())
        observed = json.loads((self.attempt / 'rust/observed-cargo-config.json').read_text())
        self.assertEqual(hashlib.sha256((home / '.cargo/config.toml').read_bytes()).hexdigest(), fact['config_sha256'])
        self.assertEqual(fact['config_sha256'], observed['sha256'])
        self.assertEqual(account.pw_uid, observed['uid'])
        self.assertEqual(b'original locked input\n', lock.read_bytes())
        self.assertEqual(0o640, (self.attempt / 'cargo-registry.json').stat().st_mode & 0o777)
        self.assertEqual(0o600, (self.runtime / 'cargo-registry.json').stat().st_mode & 0o777)
        import tarfile
        with tarfile.open(archives[0], mode='r:gz') as archive:
            self.assertEqual(fact, json.load(archive.extractfile('attempt/cargo-registry.json')))

    def test_registry_configuration_preserves_default_and_rejects_unknown_user_files(self):
        from types import SimpleNamespace
        home = self.root / 'home'; cargo = home / '.cargo'; cargo.mkdir(parents=True)
        account = SimpleNamespace(pw_dir=str(home), pw_uid=os.getuid(), pw_gid=os.getgid())
        path = cargo / 'config.toml'; original = b'[build]\njobs = 3\n'; path.write_bytes(original)
        fact = local_rust.configure_cargo_registry('crates-io', account=account)
        self.assertFalse(fact['managed']); self.assertEqual(original, path.read_bytes())
        with self.assertRaisesRegex(ValueError, 'unmanaged'):
            local_rust.configure_cargo_registry('rsproxy', account=account)
        self.assertEqual(original, path.read_bytes())
        path.unlink()
        target = self.root / 'target'; target.write_bytes(original); path.symlink_to(target)
        with self.assertRaisesRegex(ValueError, 'unmanaged'):
            local_rust.configure_cargo_registry('rsproxy', account=account)
        self.assertTrue(path.is_symlink()); self.assertEqual(original, target.read_bytes())
        path.unlink(); (cargo / 'config').write_bytes(original)
        with self.assertRaisesRegex(ValueError, 'legacy'):
            local_rust.configure_cargo_registry('rsproxy', account=account)
        self.assertFalse(path.exists())

    def test_managed_registry_configuration_can_be_reobserved_without_rewriting(self):
        from types import SimpleNamespace
        home = self.root / 'home'; home.mkdir()
        account = SimpleNamespace(pw_dir=str(home), pw_uid=os.getuid(), pw_gid=os.getgid())
        first = local_rust.configure_cargo_registry('rsproxy', account=account)
        inode = (home / '.cargo/config.toml').stat().st_ino
        second = local_rust.configure_cargo_registry('rsproxy', account=account)
        self.assertEqual(first, second)
        self.assertEqual(inode, (home / '.cargo/config.toml').stat().st_ino)

    def test_unknown_registry_is_refused_before_worker_files_are_installed(self):
        self.payload['cargo_registry'] = 'auto'
        with self.assertRaisesRegex(ValueError, 'unsupported local Cargo registry'):
            self.install()
        self.assertFalse(self.runtime.exists())

    def test_swap_failure_keeps_original_kernel_fact_and_never_launches_native(self):
        from unittest.mock import patch
        from aliyun_ci.instance.swap import SwapSetupError
        self.payload['swap_gib'] = 16; self.install(); self.source.mkdir()
        failed = {'requested_gib': 16, 'enabled': False, 'stage': 'activate',
                  'commands': [{'exit_code': 1}], 'kernel_after': {'swaps': []}}
        def forbidden(*args, **kwargs):
            raise AssertionError('failed swap must not launch or reduce the native workload')
        with patch.object(local_rust, 'configure_swap', side_effect=SwapSetupError(failed)):
            result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
                account=self.account,
                source_preparer=lambda *_: {}, tools_checker=lambda *args, **kwargs: {},
                executor=forbidden, uploader=lambda *args, **kwargs: {'uploaded': True})
        self.assertIsNone(result['exit_code']); self.assertEqual('SwapSetupError', result['platform_error'])
        self.assertEqual(failed, result['swap'])
        self.assertEqual(failed, json.loads((self.attempt / 'swap.json').read_text()))
        self.assertEqual(0o600, (self.runtime / 'swap.json').stat().st_mode & 0o777)

    def test_swap_fact_precedes_original_child_and_survives_archive(self):
        from unittest.mock import patch
        from types import SimpleNamespace
        self.payload.update(swap_gib=16, build_jobs=8); self.install(); self.source.mkdir()
        ready = {'requested_gib': 16, 'enabled': True, 'stage': 'ready',
                 'file': '/var/lib/novarocks-ci/private-swap/swapfile', 'kernel_after': {'swaps': []}}
        packed = []
        def executor(*args, **kwargs):
            self.assertEqual(ready, json.loads((self.attempt / 'swap.json').read_text()))
            self.assertEqual('8', kwargs['environment']['CARGO_BUILD_JOBS'])
            return {'exit_code': 7, 'pid': 99, 'exited': True, 'cancel_requested': False}
        def upload(attempt, **kwargs):
            import tarfile
            path = handoff.pack(attempt, output=self.root / 'result.tar.gz')
            with tarfile.open(path, mode='r:gz') as archive:
                packed.append(json.load(archive.extractfile('attempt/swap.json')))
                self.assertFalse(any('private-swap' in name for name in archive.getnames()))
            return {'uploaded': True}
        account = SimpleNamespace(pw_dir=str(self.root / 'home'), pw_name=self.account.pw_name,
            pw_uid=os.getuid(), pw_gid=os.getgid())
        with patch.object(local_rust, 'configure_swap', return_value=ready) as configure:
            result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
                account=account, source_preparer=lambda *_: {}, tools_checker=lambda *args, **kwargs: {},
                executor=executor, uploader=upload)
        self.assertEqual(16, configure.call_args.args[0]); self.assertEqual(7, result['exit_code'])
        self.assertEqual([ready], packed)

    def test_tools_failure_keeps_unknown_test_exit_and_upload_failure_independent(self):
        self.install()
        def bad_tools(*args, **kwargs):
            raise ValueError('https://signed.invalid/secret')
        def bad_upload(*args, **kwargs):
            raise OSError('https://signed.invalid/other-secret')
        result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
            account=self.account, source_preparer=lambda *_: {'sha': 'a' * 40},
            tools_checker=bad_tools, uploader=bad_upload)
        self.assertEqual(1, result['tools_check_exit'])
        self.assertIsNone(result['exit_code'])
        self.assertIsNone(result['handoff']['uploaded'])
        self.assertEqual('ValueError', result['platform_error'])
        self.assertNotIn('signed.invalid', json.dumps(result))
        self.assertFalse((self.attempt / 'batch.pid').exists())

    def test_sigterm_reaches_original_child_and_real_exit_is_observed(self):
        self.runtime.mkdir()
        self.attempt.mkdir()
        self.source.mkdir()
        child = self.root / 'child.py'
        child.write_text("import pathlib,signal,sys,time\ndef stop(*args):\n pathlib.Path(sys.argv[1]).write_text('received SIGTERM'); raise SystemExit(23)\nsignal.signal(signal.SIGTERM,stop)\npathlib.Path(sys.argv[2]).write_text('ready')\nwhile True: time.sleep(.05)\n")
        supervisor = self.root / 'supervisor.py'
        supervisor.write_text("import json,os,pathlib,pwd,sys\nfrom aliyun_ci.instance.local_rust import execute_batch\nr=pathlib.Path(sys.argv[1])\nv=execute_batch([sys.executable,str(r/'child.py'),str(r/'signal'),str(r/'ready')],source_root=r/'source',attempt_root=r/'attempt',runtime=r/'runtime',account=pwd.getpwuid(os.getuid()),environment=os.environ.copy())\n(r/'result').write_text(json.dumps(v))\n")
        environment = os.environ.copy()
        environment['PYTHONPATH'] = str(Path(__file__).resolve().parents[2])
        process = subprocess.Popen([sys.executable, str(supervisor), str(self.root)], env=environment)
        self.addCleanup(lambda: process.kill() if process.poll() is None else None)
        end = time.monotonic() + 10
        while not (self.root / 'ready').exists() and time.monotonic() < end:
            if process.poll() is not None:
                self.fail(f'supervisor exited early: {process.returncode}')
            time.sleep(.02)
        self.assertTrue((self.root / 'ready').exists())
        original_pid = int((self.runtime / 'batch.pid').read_text())
        process.send_signal(signal.SIGTERM)
        self.assertEqual(0, process.wait(timeout=10))
        fact = json.loads((self.root / 'result').read_text())
        self.assertEqual(23, fact['exit_code'])
        self.assertEqual(original_pid, fact['pid'])
        self.assertTrue(fact['exited'])
        self.assertTrue(fact['cancel_requested'])
        self.assertEqual('received SIGTERM', (self.root / 'signal').read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(original_pid, 0)

    def test_compact_manifest_pointer_verifies_remote_manifest_and_self_contained_bundle(self):
        import hashlib
        import io
        repo = self.root / 'repo'
        repo.mkdir()
        for args in (('init', '-q'), ('config', 'user.name', 'Test'),
                     ('config', 'user.email', 'test@example.invalid')):
            subprocess.run(['git', '-C', str(repo), *args], check=True)
        (repo / 'source.rs').write_text('fn example() {}\n')
        subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(repo), 'commit', '-qm', 'Exact source'], check=True)
        manifest = source_bundle.create(repo, self.root / 'carrier')
        raw = (self.root / 'carrier/source.json').read_bytes()
        bundle = (self.root / 'carrier/source.bundle').read_bytes()
        self.payload['source'] = {key: manifest[key] for key in ('sha', 'tree', 'bundle_sha256')}
        self.payload['source'].update(url='https://signed.invalid/bundle',
            manifest_url='https://signed.invalid/manifest', manifest_sha256=hashlib.sha256(raw).hexdigest())
        self.install()
        values = {'https://signed.invalid/bundle': bundle, 'https://signed.invalid/manifest': raw}
        fact = local_rust.prepare_source(self.payload, self.source, self.runtime,
            opener=lambda url, **kwargs: io.BytesIO(values[url]))
        self.assertEqual(manifest['sha'], fact['sha'])
        self.assertEqual('fn example() {}\n', (self.source / 'source.rs').read_text())
        self.assertNotIn('private', (self.source / '.git/config').read_text())

    def test_native_cancel_budget_expiry_keeps_actual_exit_unknown(self):
        self.runtime.mkdir()
        self.attempt.mkdir()
        self.source.mkdir()
        # A cancellation intent precedes the signal. This child deliberately
        # remains alive: the supervisor may not manufacture a cleanup success.
        ready = self.root / 'ready'
        children = []
        def spawn(*args, **kwargs):
            child = subprocess.Popen(*args, **kwargs)
            children.append(child)
            end = time.monotonic() + 10
            while not ready.exists() and time.monotonic() < end:
                time.sleep(.01)
            self.assertTrue(ready.exists())
            (self.runtime / 'cancellation.json').write_text('{"intent":true}')
            return child
        script = 'import signal,time,pathlib,sys; signal.signal(signal.SIGTERM,signal.SIG_IGN); pathlib.Path(sys.argv[1]).touch(); time.sleep(30)'
        result = local_rust.execute_batch([sys.executable, '-c', script, str(ready)],
            source_root=self.source, attempt_root=self.attempt, runtime=self.runtime,
            account=self.account, environment=os.environ.copy(), cancel_budget_seconds=.1, popen=spawn)
        try:
            self.assertIsNone(result['exit_code'])
            self.assertFalse(result['exited'])
            self.assertTrue(result['cancel_requested'])
            os.kill(result['pid'], 0)
        finally:
            children[0].kill()
            children[0].wait(timeout=5)


    def test_cancellation_before_batch_launch_does_not_start_work(self):
        self.runtime.mkdir()
        self.attempt.mkdir()
        (self.runtime / 'cancellation.json').write_text('{"intent":true}')
        def unexpected_spawn(*args, **kwargs):
            self.fail('preexisting cancellation must prevent native batch launch')
        result = local_rust.execute_batch([], source_root=self.source,
            attempt_root=self.attempt, runtime=self.runtime, account=self.account,
            environment={}, popen=unexpected_spawn)
        self.assertIsNone(result['exit_code'])
        self.assertIsNone(result['pid'])
        self.assertTrue(result['cancel_requested'])
        self.assertFalse((self.runtime / 'batch.pid').exists())


    def test_atomic_status_never_follows_runner_created_symlinks(self):
        self.attempt.mkdir()
        outside = self.root / 'root-owned-target'
        outside.write_text('must remain unchanged')
        target = self.attempt / 'status.json'
        target.symlink_to(outside)
        # This was the predictable root temporary name in the reported bug.
        (self.attempt / 'status.json.new').symlink_to(outside)
        local_rust._atomic(target, {'state': 'complete'}, mode=0o640)
        self.assertEqual('must remain unchanged', outside.read_text())
        self.assertFalse(target.is_symlink())
        self.assertEqual({'state': 'complete'}, json.loads(target.read_text()))
        self.assertEqual(0o640, target.stat().st_mode & 0o777)
        self.assertFalse(list(self.attempt.glob('.status.json.*')))

    def test_upload_timeout_uses_remaining_original_capability_deadline(self):
        self.install()
        clock = datetime(2026, 10, 29, 8, 0, tzinfo=timezone.utc)
        self.payload['deadline'] = (clock + timedelta(seconds=187)).isoformat()
        timeouts = []
        def no_tools(*args, **kwargs):
            raise ValueError('tools unavailable')
        def upload(attempt, *, kind, url, timeout):
            timeouts.append(timeout)
            return {'uploaded': False}
        result = local_rust.run(self.payload, runtime=self.runtime, source_root=self.source,
            account=self.account, source_preparer=lambda *_: {'sha': 'a' * 40},
            tools_checker=no_tools, uploader=upload, now=lambda: clock)
        self.assertEqual([187], timeouts)
        self.assertFalse(result['handoff']['uploaded'])
        self.assertIsNone(result['exit_code'])
