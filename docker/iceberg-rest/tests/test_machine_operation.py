# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Real command-supervisor/CLI processes against a hermetic fake Docker.

Only temporary files represent containers. Nothing invokes the real Docker
binary, socket, network, image store, or daemon.
"""
import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
import fixture_runtime as runtime
import machine_runtime as machine
import machine_operation as operation


FAKE_DOCKER = r'''#!/usr/bin/env python3
import json,os,pathlib,signal,subprocess,sys,time
root=pathlib.Path(__file__).resolve().parent.parent
args=sys.argv[1:]
config=json.loads((root/'fake-config.json').read_text())
state=root/'container.json'
def event(name, **facts):
    value=dict(event=name,pid=os.getpid(),ppid=os.getppid(),args=args,at_monotonic=time.monotonic(),**facts)
    with (root/'events.jsonl').open('a') as out:
        out.write(json.dumps(value)+'\n');out.flush();os.fsync(out.fileno())
def gate(name):
    while not (root/name).exists():time.sleep(0.01)
def save(value):
    path=root/('container-'+str(os.getpid())+'.tmp')
    path.write_text(json.dumps(value));path.replace(state)
if args[0]=='_plugin':
    signal.signal(signal.SIGTERM,signal.SIG_IGN if config.get('plugin_ignore_term') else lambda *_:event('plugin.term'))
    event('plugin.entered',group=os.getpgrp())
    while not (root/'plugin-release').exists():
        with (root/'plugin-writes').open('a') as out:out.write('x')
        time.sleep(0.01)
    (root/'plugin-side-effect').write_text('finished')
    event('plugin.exit')
    raise SystemExit(0)
if config.get('operation')=='control' and args==config.get('control_args',['volume','create','fixture-test-volume']):
    event('control.entered')
    if config.get('hold_control'):gate('control-release')
    if config.get('plugin'):
        plugin=subprocess.Popen([sys.executable,__file__,'_plugin'],close_fds=True)
        event('control.plugin',plugin_pid=plugin.pid)
    code=config.get('control_exit',0)
    if code==0:
        (root/'control-side-effect').write_text('finished')
        print('fake control output',flush=True)
    else:
        print(config.get('control_stderr','phase-specific control failure'),file=sys.stderr,flush=True)
    event('control.exit',code=code)
    raise SystemExit(code)
if args[:2]==['context','show']:
    event('context.show');print('fixture-fake-context');raise SystemExit(0)
if args[:2]==['context','inspect']:
    event('context.inspect');print(json.dumps('unix:///fixture-fake-docker.sock'));raise SystemExit(0)
if args and args[0]=='info':
    event('daemon.info');print('fake-daemon');raise SystemExit(0)
if args[0]=='_late-create':
    event('late.entered')
    gate('late-release')
    save(json.loads((root/'late-container.json').read_text()))
    event('late.created')
    raise SystemExit(0)
if args[0]=='create':
    event('create.entered')
    name=args[args.index('--name')+1]
    labels={}
    for i,a in enumerate(args):
        if a=='--label':
            key,value=args[i+1].split('=',1);labels[key]=value
    image=args[args.index('--entrypoint')+2]
    info=dict(Id='4444444444444444444444444444444444444444444444444444444444444444',Name=name,Image=image,
              Config=dict(Labels=labels),State=dict(Running=False))
    if config.get('foreign'):
        info['Image']='sha256:foreign-image'
    if config.get('hold_create'):gate('create-release')
    if 'create_exit' in config:
        event('create.exit',code=config['create_exit'])
        raise SystemExit(config['create_exit'])
    if config.get('unknown_create'):
        if config.get('late_create'):
            (root/'late-container.json').write_text(json.dumps(info))
            subprocess.Popen([sys.executable,__file__,'_late-create'],
                             stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL,start_new_session=True,close_fds=True)
        print('phase-specific create failure',file=sys.stderr,flush=True)
        event('create.exit',code=1)
        raise SystemExit(1)
    save(info)
    if config.get('hold_created'):
        event('create.saved')
        gate('create-release')
    print(config.get('create_reply',info['Id']),flush=True)
    event('create.exit',code=0)
    raise SystemExit(0)
if args[:2]==['container','inspect']:
    event('inspect')
    if state.exists():
        info=json.loads(state.read_text())
        if args[2] in (info['Id'],info['Name']):
            print(json.dumps(info));raise SystemExit(0)
    print('Error: No such container: '+args[2],file=sys.stderr)
    raise SystemExit(1)
if args[0]=='start':
    info=json.loads(state.read_text())
    assert args==['start','--attach',info['Id']],args
    info['State']['Running']=True;save(info)
    event('start.entered')
    if config.get('hold_start'):
        def terminate(signum,frame):
            event('start.signal',signal=signum)
            gate('start-release')
            code=config.get('start_term_exit',128+signum)
            event('start.exit',code=code)
            raise SystemExit(code)
        signal.signal(signal.SIGTERM,terminate)
        while True:time.sleep(0.01)
    if config.get('binary'):
        sys.stdout.buffer.write(b'\x00\xff\x80PAR1\n');sys.stdout.buffer.flush()
    else:
        print('fake mc output',flush=True)
    code=config.get('start_exit',0)
    event('start.exit',code=code)
    raise SystemExit(code)
if args[:2]==['container','rm']:
    event('rm.entered')
    if config.get('hold_rm'):gate('rm-release')
    if config.get('rm_exit'):
        event('rm.exit',code=config['rm_exit'])
        raise SystemExit(config['rm_exit'])
    info=json.loads(state.read_text())
    assert args==['container','rm','--force',info['Id']],args
    state.unlink();event('rm.exit')
    raise SystemExit(0)
event('unexpected')
raise SystemExit(91)
'''


def backend_for(root):
    return machine.MachineDocker(timeout=8, environment={**os.environ,
        "PATH": str(root / "bin") + os.pathsep + os.environ.get("PATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1"})


def owner_for(root):
    return machine.MachineOwner(root / "state", daemon="fake-daemon",
                                backend=backend_for(root), timeout=8)


def operation_record(root, owner):
    directory = owner.base / "activations" / "generation-1"
    directory.mkdir(parents=True, exist_ok=True)
    return dict(kind="machine", namespace=owner.machine_id, control_root=str(owner.root),
                version="test-version", generation="generation-1", network="fake-machine-network",
                compose_file=str(directory / "compose.yml"),
                credentials=dict(access_key="fixture-test-user", secret_key="fixture-test-secret"),
                images=dict(mc=dict(image_id="sha256:fixture-test-image")))


def run_owner(root):
    owner = owner_for(root)
    record = operation_record(root, owner)
    options = json.loads((root / "fake-config.json").read_text())
    owner.backend.timeout = options.get("timeout", 8)
    if options.get("saved_maintenance"):
        runtime.atomic_json(owner.base / "state.json", {**record, "schema":machine.PROTOCOL,
            "daemon_id":owner.daemon_id, "phase":"maintenance"})
    if options.get("supervisor_start_delay"):
        original = owner.backend._supervise
        def delayed(*args, **kwargs):
            time.sleep(options["supervisor_start_delay"])
            return original(*args, **kwargs)
        owner.backend._supervise = delayed
    result = {}
    try:
        with owner.maintenance_lock():
            (root / "owner-locked").write_text(str(owner.base / "maintenance.lock"))
            if options.get("operation") == "control":
                arguments = options.get("control_args", ["volume", "create", "fixture-test-volume"])
                if options.get("gate_plugin_deadline"):
                    original_popen = machine.subprocess.Popen
                    expected = [sys.executable, str(HERE.parents[1] / "machine_operation.py")]
                    def clocked_monitor(argv, **kwargs):
                        # Redirect only this owner process's exact production
                        # supervisor launch. FD/stdIO/env/session kwargs stay intact;
                        # no liveness writer or extra descriptor crosses exec.
                        if (len(argv) != 9 or argv[:2] != expected or
                                argv[3::2] != ["--control-fd", "--lock-fd", "--timeout"]):
                            raise AssertionError("unexpected production supervisor launch")
                        redirected = [sys.executable, str(HERE), "--fixture-clocked-monitor",
                                      str(root), *argv[2:]]
                        return original_popen(redirected, **kwargs)
                    with patch.object(machine.subprocess, "Popen", side_effect=clocked_monitor):
                        output = owner.backend.command(arguments)
                else:
                    output = owner.backend.command(arguments)
            else:
                if options.get("gate_start_deadline"):
                    original_popen = machine.subprocess.Popen
                    expected = [sys.executable, str(HERE.parents[1] / "machine_operation.py")]
                    def clocked_start_monitor(argv, **kwargs):
                        if (len(argv) != 9 or argv[:2] != expected or
                                argv[3::2] != ["--control-fd", "--lock-fd", "--timeout"]):
                            raise AssertionError("unexpected production supervisor launch")
                        return original_popen([sys.executable, str(HERE), "--fixture-clocked-monitor",
                                               str(root), *argv[2:]], **kwargs)
                    with patch.object(machine.subprocess, "Popen", side_effect=clocked_start_monitor):
                        output = owner.backend.run_mc(record, "echo fixture-command", binary=options.get("binary", False))
                else:
                    output = owner.backend.run_mc(record, "echo fixture-command", binary=options.get("binary", False))
            if isinstance(output, bytes):
                result["stdout_hex"] = output.hex()
            else:
                result["stdout"] = output
        result["exit"] = 0
    except BaseException as error:
        result.update(exit=1, error=getattr(error, "code", type(error).__name__))
    (root / "owner-result.json").write_text(json.dumps(result))
    return result["exit"]


def run_clocked_monitor(root, arguments):
    """Exercise entered-plugin expiry, not Python startup's wall-clock budget."""
    import machine_operation

    options = json.loads((root / "fake-config.json").read_text())
    plugin_mode = (options.get("gate_plugin_deadline") and options.get("plugin")
                   and options.get("plugin_ignore_term") and options.get("operation") == "control")
    start_mode = options.get("gate_start_deadline") and options.get("hold_start") and options.get("operation") != "control"
    if not (plugin_mode or start_mode):
        raise AssertionError("clock helper requires an original entered-plugin or entered-start timeout case")
    gate = "plugin-deadline-release" if plugin_mode else "start-deadline-release"
    journal = Path(arguments[0])
    if not journal.resolve().is_relative_to((root / "state").resolve()):
        raise AssertionError("clock helper journal is outside this test owner")
    request_path = journal.with_suffix(".request")
    original_request = request_path.read_bytes()
    request = json.loads(original_request)
    deadline = request["deadline_monotonic"]
    real_monotonic = time.monotonic
    resumed_at = None

    def controlled_monotonic():
        nonlocal resumed_at
        if resumed_at is None:
            if not (root / gate).exists():
                return deadline - options["timeout"]
            # One existence gate, one real anchor. Never reread a timestamp,
            # extend the original deadline, or freeze the TERM/KILL interval.
            resumed_at = real_monotonic()
            runtime.atomic_json(root / "plugin-clock-expired.json", {
                "original_deadline_monotonic": deadline,
                "real_anchor_monotonic": resumed_at,
                "original_request_sha256": hashlib.sha256(original_request).hexdigest(),
            })
        return deadline + real_monotonic() - resumed_at

    # Replace only this helper process's module-local clock view; all other
    # time owners and OS signals/wait/flock/native group observations are real.
    clock = SimpleNamespace(monotonic=controlled_monotonic, sleep=time.sleep)
    original_argv = sys.argv
    try:
        sys.argv = [str(HERE.parents[1] / "machine_operation.py"), *arguments]
        with patch.object(machine_operation, "time", clock):
            return machine_operation.main()
    finally:
        sys.argv = original_argv
        if request_path.read_bytes() != original_request:
            raise AssertionError("clock helper changed the original persisted request")


class MachineOperationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="novarocks-machine-operation-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "bin").mkdir()
        cli = self.root / "bin" / "docker"
        cli.write_text(FAKE_DOCKER.replace("#!/usr/bin/env python3", "#!" + sys.executable, 1))
        cli.chmod(0o700)
        self.owner_process = None
        self.addCleanup(self.finish)

    def configure(self, **options):
        (self.root / "fake-config.json").write_text(json.dumps(options))

    def start_owner(self, **options):
        self.configure(**options)
        log = (self.root / "owner.log").open("wb")
        try:
            self.owner_process = subprocess.Popen([sys.executable, str(HERE), "--fixture-owner", str(self.root)],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        finally:
            log.close()
        return self.owner_process

    def until(self, predicate, label, timeout=6):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.01)
        self.fail("timed out waiting for " + label + "; events=" + json.dumps(self.events()))

    def events(self):
        path = self.root / "events.jsonl"
        if not path.exists():
            return []
        values = []
        for line in path.read_text().splitlines():
            try:
                values.append(json.loads(line))
            except ValueError:
                pass  # A concurrently appended last line is not yet committed.
        return values

    def event(self, name):
        return next((e for e in self.events() if e["event"] == name), None)

    def journal_path(self):
        paths = list((self.root / "state").glob("*/activations/**/operations/*.json"))
        return paths[0] if len(paths) == 1 else None

    def fact(self):
        path = self.journal_path()
        return json.loads(path.read_text()) if path else {}

    def control_fact(self):
        paths = list((self.root / "state").glob("*/operations/control-*.json"))
        self.assertEqual(len(paths), 1)
        return json.loads(paths[0].read_text())

    def lock_held(self):
        marker = self.root / "owner-locked"
        if not marker.exists():
            return False
        with Path(marker.read_text()).open("a+b") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(lock, fcntl.LOCK_UN)
            return False

    def release(self, name):
        (self.root / name).touch()

    def wait_owner(self, expected):
        self.assertEqual(self.owner_process.wait(timeout=10), expected,
                         (self.root / "owner.log").read_text())
        return json.loads((self.root / "owner-result.json").read_text())

    def finish(self):
        # Every gate belongs to this test's fake CLI; release before reaping.
        # A failed setup must not leave a clock-frozen creating=True monitor
        # retaining its inherited flock after this test's owner is killed.
        for name in ("plugin-deadline-release", "start-deadline-release", "create-release", "start-release", "rm-release",
                     "late-release", "control-release", "plugin-release"):
            self.release(name)
        if self.owner_process is not None and self.owner_process.poll() is None:
            self.owner_process.kill()
            self.owner_process.wait(timeout=5)
        deadline = time.monotonic() + 8
        while self.lock_held() and time.monotonic() < deadline:
            time.sleep(0.01)
        late = self.event("late.entered")
        if late:
            deadline = time.monotonic() + 3
            while not self.event("late.created") and time.monotonic() < deadline:
                time.sleep(0.01)
        evidence = os.environ.get("NOVA_TEST_MONITOR_EVIDENCE")
        if evidence:
            destination = Path(evidence) / self.id().rsplit(".", 1)[-1]
            shutil.copytree(self.root, destination, dirs_exist_ok=True)

    def test_normal_create_start_and_cleanup_use_exact_container_identity(self):
        self.start_owner()
        result = self.wait_owner(0)
        self.assertEqual(result["stdout"], "fake mc output\n")
        events = self.events()
        starts = [e for e in events if e["event"] == "start.entered"]
        removes = [e for e in events if e["event"] == "rm.entered"]
        self.assertEqual(len(starts), 1)
        self.assertEqual(len(removes), 1)
        self.assertEqual(starts[0]["args"], ["start", "--attach", "4444444444444444444444444444444444444444444444444444444444444444"])
        self.assertEqual(removes[0]["args"], ["container", "rm", "--force", "4444444444444444444444444444444444444444444444444444444444444444"])
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertTrue(self.fact()["container_absent"])
        self.assertFalse((self.root / "container.json").exists())
        self.assertFalse(self.lock_held())
        self.assertNotIn("fixture-test-secret", json.dumps(events))
        self.assertEqual(self.journal_path().with_suffix(".env").stat().st_mode & 0o777, 0o600)

    def test_cleaned_start_timeout_with_native_exit_zero_is_typed_timeout(self):
        self.start_owner(hold_start=True, timeout=0.4, start_term_exit=0, gate_start_deadline=True)
        self.until(lambda: self.event("start.entered"), "original START payload")
        self.release("start-deadline-release")
        self.until(lambda: self.event("start.signal"), "original start deadline")
        (self.root / "start-release").touch()
        self.assertEqual(self.wait_owner(1)["error"], "DockerOperationTimeout")
        fact = self.fact()
        self.assertEqual(fact["command_exit"], 0)
        self.assertTrue(fact["timed_out"])
        self.assertEqual(fact["phase"], "settled")
        self.assertTrue(fact["container_absent"])
        self.assertTrue(fact["group_exit_observed"])
        self.assertTrue(fact["leader_waited"])
        self.assertFalse(self.lock_held())

    def test_cleaned_create_timeout_records_top_level_timeout_without_start(self):
        self.start_owner(hold_created=True, timeout=0.4)
        self.assertEqual(self.wait_owner(1)["error"], "DockerOperationTimeout")
        fact = self.fact()
        self.assertTrue(fact["create_timed_out"])
        self.assertTrue(fact["timed_out"])
        self.assertEqual(fact["phase"], "settled")
        self.assertTrue(fact["container_absent"])
        self.assertIsNone(self.event("start.entered"))
        self.assertFalse(self.lock_held())

    def release_create_after_work_deadline(self):
        self.until(lambda: self.event("create.entered"), "original CREATE dispatch")
        request = self.journal_path().with_suffix(".request")
        original = request.read_bytes()
        deadline = json.loads(original)["deadline_monotonic"]
        self.until(lambda: time.monotonic() >= deadline + 0.05, "original work deadline")
        self.assertTrue(self.lock_held())
        self.assertIsNone(self.event("create.exit"), "original CREATE reply must remain in flight")
        self.release("create-release")
        return request, original, deadline

    def test_late_authoritative_create_reply_is_cleaned_without_start_and_stays_timeout(self):
        self.start_owner(hold_create=True, timeout=0.8)
        request, original, deadline = self.release_create_after_work_deadline()
        self.assertEqual(self.wait_owner(1)["error"], "DockerOperationTimeout")
        fact = self.fact()
        self.assertTrue(fact["create_dispatched"])
        self.assertEqual(fact["create_exit"], 0)
        self.assertEqual(fact["create_reply_id"], "4444444444444444444444444444444444444444444444444444444444444444")
        self.assertEqual(fact["container_id"], fact["create_reply_id"])
        self.assertTrue(fact["create_timed_out"])
        self.assertTrue(fact["timed_out"])
        self.assertEqual(fact["create_reply_deadline_monotonic"], deadline + 5)
        self.assertEqual(fact["phase"], "settled")
        self.assertTrue(fact["container_absent"])
        self.assertFalse(fact["command_succeeded"])
        self.assertIsNone(self.event("start.entered"))
        self.assertEqual(self.event("rm.entered")["args"], ["container", "rm", "--force", "4444444444444444444444444444444444444444444444444444444444444444"])
        self.assertGreater(self.event("create.exit")["at_monotonic"], deadline)
        self.assertLess(self.event("create.exit")["at_monotonic"], deadline + 5)
        self.assertEqual(request.read_bytes(), original)
        self.assertFalse(self.lock_held())

    def test_late_create_reply_with_cleanup_failure_remains_unconfirmed(self):
        self.start_owner(hold_create=True, timeout=0.8, rm_exit=17)
        self.release_create_after_work_deadline()
        self.assertEqual(self.wait_owner(1)["error"], "ContainerCleanupUnconfirmed")
        fact = self.fact()
        self.assertEqual(fact["phase"], "cleanup-failed")
        self.assertEqual(fact["create_exit"], 0)
        self.assertTrue(fact["timed_out"])
        self.assertTrue((self.root / "container.json").exists())
        self.assertIsNone(self.event("start.entered"))

    def test_owner_loss_during_create_still_observes_late_reply_and_cleans_exact_id(self):
        self.start_owner(hold_create=True, timeout=0.8)
        self.until(lambda: self.event("create.entered"), "original CREATE dispatch")
        self.owner_process.kill()
        self.assertEqual(self.owner_process.wait(timeout=3), -signal.SIGKILL)
        self.release_create_after_work_deadline()
        self.until(lambda: not self.lock_held(), "late CREATE reply and exact container cleanup")
        fact = self.fact()
        self.assertEqual(fact["phase"], "settled")
        self.assertEqual(fact["create_exit"], 0)
        self.assertTrue(fact["interrupted"])
        self.assertTrue(fact["timed_out"])
        self.assertTrue(fact["container_absent"])
        self.assertFalse(fact["command_succeeded"])
        self.assertIsNone(self.event("start.entered"))

    def test_create_without_reply_by_fixed_settlement_limit_remains_unknown(self):
        self.start_owner(hold_create=True, timeout=0.4)
        self.until(lambda: self.event("create.entered"), "original CREATE dispatch")
        request = json.loads(self.journal_path().with_suffix(".request").read_text())
        self.assertEqual(self.wait_owner(1)["error"], "ContainerCleanupUnconfirmed")
        fact = self.fact()
        self.assertEqual(fact["phase"], "create-unknown")
        self.assertTrue(fact["create_dispatched"])
        self.assertTrue(fact["create_timed_out"])
        self.assertNotIn("container_absent", fact)
        self.assertIsNone(self.event("start.entered"))
        self.assertGreaterEqual(fact["group_exit_observed_monotonic"], request["deadline_monotonic"] + 5)
        self.assertFalse(self.lock_held())

    def test_native_create_exit_124_is_dispatched_unknown_not_predispatch_expiry(self):
        self.start_owner(create_exit=124)
        self.assertEqual(self.wait_owner(1)["error"], "ContainerCleanupUnconfirmed")
        self.assertEqual(self.fact()["phase"], "create-unknown")
        self.assertTrue(self.fact()["create_dispatched"])
        self.assertEqual(self.fact()["create_exit"], 124)
        self.assertFalse(self.fact()["create_timed_out"])
        self.assertNotIn("container_absent", self.fact())

    def test_successful_create_reply_id_cannot_be_replaced_by_same_name_observation(self):
        self.start_owner(create_reply="5" * 64)
        self.assertEqual(self.wait_owner(1)["error"], "ContainerCleanupUnconfirmed")
        fact = self.fact()
        self.assertEqual(fact["phase"], "create-unknown")
        self.assertEqual(fact["create_reply_id"], "5" * 64)
        self.assertNotIn("container_id", fact)
        self.assertNotIn("container_absent", fact)
        self.assertTrue((self.root / "container.json").exists())
        self.assertEqual(self.event("inspect")["args"][2], "5" * 64)
        self.assertIsNone(self.event("start.entered"))
        self.assertIsNone(self.event("rm.entered"))
        owner = owner_for(self.root)
        with owner.maintenance_lock():
            owner.backend.settle_operations(owner.base)
        self.assertTrue((self.root / "container.json").exists())
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertEqual(self.fact()["container_id"], "5" * 64)
        self.assertFalse(self.fact()["command_succeeded"])
        self.assertTrue(all(event["args"][2] == "5" * 64 for event in self.events() if event["event"] == "inspect"))
        self.assertIsNone(self.event("rm.entered"))

    def direct_owner(self, deadline):
        journal = self.root / "direct.json"
        runtime.atomic_json(journal, dict(schema=1, phase="intent", container_name="fixture-direct",
            image_id="sha256:fixture-test-image", labels={}))
        request = dict(environment={}, deadline_monotonic=deadline, network="none",
                       env_file=str(self.root / "unused.env"), script="echo unused")
        return operation.CommandOwner(request, journal, -1, 1)

    def test_create_intent_fsync_crossing_deadline_records_proven_undispatched(self):
        owner = self.direct_owner(101.0)
        now = [100.0]
        original = owner.save
        def save(**facts):
            original(**facts)
            if facts.get("phase") == "creating":
                now[0] = 101.0
        with patch.object(owner, "alive", return_value=True), patch.object(owner, "save", side_effect=save), \
                patch.object(operation.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(operation.subprocess, "Popen") as popen, patch.object(owner, "inspect") as inspect:
            self.assertEqual(owner.run(), 124)
        popen.assert_not_called()
        inspect.assert_not_called()
        self.assertEqual(owner.fact["phase"], "settled")
        self.assertFalse(owner.fact["create_dispatched"])
        self.assertTrue(owner.fact["timed_out"])
        self.assertTrue(owner.fact["container_absent"])
        self.assertFalse(owner.fact["command_succeeded"])

    def test_owner_liveness_check_crossing_deadline_never_dispatches_create(self):
        owner = self.direct_owner(101.0)
        now, checks = [100.0], []
        def alive():
            checks.append(True)
            if len(checks) == 2:
                now[0] = 101.0
            return True
        with patch.object(owner, "alive", side_effect=alive), \
                patch.object(operation.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(operation.subprocess, "Popen") as popen, patch.object(owner, "inspect") as inspect:
            self.assertEqual(owner.run(), 124)
        popen.assert_not_called()
        inspect.assert_not_called()
        self.assertFalse(owner.fact["create_dispatched"])
        self.assertTrue(owner.fact["timed_out"])
        self.assertTrue(owner.fact["container_absent"])

    def test_successful_create_reply_requires_one_full_hex_id_before_inspect(self):
        for reply in (b"", b"4" * 12 + b"\n", b"4" * 64 + b"\n" + b"5" * 64 + b"\n", b"G" * 64 + b"\n"):
            with self.subTest(reply=reply[:16]):
                owner = self.direct_owner(101.0)
                def command(args, output, **kwargs):
                    Path(output).write_bytes(reply)
                    return operation.CommandResult(0, False, True)
                with patch.object(owner, "alive", return_value=True), patch.object(owner, "command", side_effect=command), \
                        patch.object(operation.time, "monotonic", return_value=100.0), \
                        patch.object(owner, "inspect") as inspect:
                    self.assertEqual(owner.run(), 1)
                inspect.assert_not_called()
                self.assertEqual(owner.fact["phase"], "create-unknown")
                self.assertEqual(owner.fact["error"], "DockerCreateReplyInvalid")
                self.assertTrue(owner.fact["create_dispatched"])
                self.assertNotIn("container_absent", owner.fact)

    def test_start_intent_fsync_crossing_deadline_never_dispatches_start(self):
        owner = self.direct_owner(101.0)
        now = [100.0]
        original_save, original_command = owner.save, owner.command
        commands = []
        def save(**facts):
            original_save(**facts)
            if facts.get("phase") == "running":
                now[0] = 101.0
        def command(args, output, **kwargs):
            commands.append(args)
            if args[0] == "create":
                Path(output).write_text("4444444444444444444444444444444444444444444444444444444444444444\n")
                return operation.CommandResult(0, False, True)
            if args[:2] == ["container", "rm"]:
                return operation.CommandResult(0, False, True)
            return original_command(args, output, **kwargs)
        with patch.object(owner, "alive", return_value=True), patch.object(owner, "save", side_effect=save), \
                patch.object(owner, "command", side_effect=command), \
                patch.object(owner, "inspect", side_effect=[dict(Id="4444444444444444444444444444444444444444444444444444444444444444"), None]), \
                patch.object(operation.time, "monotonic", side_effect=lambda: now[0]), \
                patch.object(operation.subprocess, "Popen") as popen:
            self.assertEqual(owner.run(), 1)
        popen.assert_not_called()
        self.assertFalse(owner.fact["start_dispatched"])
        self.assertTrue(owner.fact["timed_out"])
        self.assertTrue(owner.fact["container_absent"])
        self.assertFalse(owner.fact["command_succeeded"])
        self.assertEqual(commands[-1], ["container", "rm", "--force", "4444444444444444444444444444444444444444444444444444444444444444"])

    def test_native_nonzero_start_stays_failure(self):
        self.start_owner(start_exit=17)
        self.assertEqual(self.wait_owner(1)["error"], "DockerOperationFailed")
        self.assertFalse(self.fact()["timed_out"])
        self.assertTrue(self.fact()["container_absent"])

    def test_timeout_with_native_cleanup_failure_stays_unconfirmed(self):
        self.start_owner(hold_start=True, timeout=0.4, rm_exit=17, gate_start_deadline=True)
        self.until(lambda: self.event("start.entered"), "original START payload")
        self.release("start-deadline-release")
        self.until(lambda: self.event("start.signal"), "original start deadline")
        (self.root / "start-release").touch()
        self.assertEqual(self.wait_owner(1)["error"], "ContainerCleanupUnconfirmed")
        fact = self.fact()
        self.assertTrue(fact["timed_out"])
        self.assertEqual(fact["phase"], "cleanup-failed")
        self.assertTrue((self.root / "container.json").exists())

    def test_sigkill_during_create_retains_inherited_flock_through_actual_cleanup(self):
        self.start_owner(hold_create=True, hold_rm=True)
        self.until(lambda: self.event("create.entered"), "create CLI")
        self.owner_process.kill()
        self.assertEqual(self.owner_process.wait(timeout=3), -signal.SIGKILL)
        self.assertTrue(self.lock_held(), "monitor must retain the owner's open flock description")
        self.assertIsNone(self.event("create.exit"))
        self.release("create-release")
        self.until(lambda: self.event("rm.entered"), "exact container cleanup")
        self.assertTrue(self.lock_held(), "flock must cover the live cleanup CLI and container")
        self.assertTrue((self.root / "container.json").exists())
        self.assertIsNone(self.event("start.entered"), "late create must never dispatch start")
        self.release("rm-release")
        self.until(lambda: not self.lock_held(), "monitor's final lock release")
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertTrue(self.fact()["interrupted"])
        self.assertFalse((self.root / "container.json").exists())
        self.assertIsNotNone(self.event("create.exit"))
        self.assertIsNotNone(self.event("rm.exit"))

    def test_sigkill_during_attach_joins_cli_before_removing_container(self):
        self.start_owner(hold_start=True, hold_rm=True)
        self.until(lambda: self.event("start.entered"), "attached Docker CLI")
        self.owner_process.kill()
        self.owner_process.wait(timeout=3)
        self.until(lambda: self.event("start.signal"), "monitor cancellation signal")
        self.assertTrue(self.lock_held())
        self.assertIsNone(self.event("rm.entered"), "container cleanup cannot overtake CLI exit")
        self.release("start-release")
        self.until(lambda: self.event("rm.entered"), "cleanup after CLI wait")
        names = [e["event"] for e in self.events()]
        self.assertLess(names.index("start.exit"), names.index("rm.entered"))
        self.assertTrue(self.lock_held())
        self.release("rm-release")
        self.until(lambda: not self.lock_held(), "settled cleanup")
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertFalse(self.fact()["command_succeeded"])

    def test_unknown_create_plus_observed_absence_never_settles(self):
        self.start_owner(unknown_create=True)
        result = self.wait_owner(1)
        self.assertEqual(result["error"], "ContainerCleanupUnconfirmed")
        self.assertEqual(self.fact()["phase"], "create-unknown")
        self.assertNotIn("container_absent", self.fact())
        owner = owner_for(self.root)
        with owner.maintenance_lock():
            with self.assertRaises(runtime.RuntimeFailure) as caught:
                owner.backend.settle_operations(owner.base)
        self.assertEqual(caught.exception.code, "ContainerCleanupUnconfirmed")
        self.assertEqual(self.fact()["phase"], "create-unknown")
        self.assertIsNone(self.event("start.entered"))
        streams = list(self.journal_path().parent.glob("*.stderr"))
        self.assertTrue(any(b"phase-specific create failure" in p.read_bytes() for p in streams),
                        "inspect/cleanup must not overwrite create stderr")

    def test_binary_mc_output_preserves_non_utf8_bytes(self):
        self.start_owner(binary=True)
        result = self.wait_owner(0)
        self.assertEqual(bytes.fromhex(result["stdout_hex"]), b"\x00\xff\x80PAR1\n")
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertFalse((self.root / "container.json").exists())

    def test_control_sigkill_keeps_flock_until_current_cli_actually_exits(self):
        self.start_owner(operation="control", hold_control=True)
        self.until(lambda: self.event("control.entered"), "machine control CLI")
        self.owner_process.kill()
        self.assertEqual(self.owner_process.wait(timeout=3), -signal.SIGKILL)
        self.assertTrue(self.lock_held())
        self.assertFalse((self.root / "control-side-effect").exists())
        self.assertIsNone(self.event("control.exit"))
        self.release("control-release")
        self.until(lambda: not self.lock_held(), "control CLI exit and inherited flock release")
        self.assertIsNotNone(self.event("control.exit"))
        self.assertTrue((self.root / "control-side-effect").exists())
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "settled")
        self.assertTrue(fact["interrupted"])
        self.assertTrue(fact["command_succeeded"], "the one already-dispatched command completed")
        self.assertEqual(sum(e["event"] == "control.entered" for e in self.events()), 1)

    def test_mutating_control_timeout_remains_unknown_and_blocks_next_activation(self):
        self.start_owner(operation="control", hold_control=True, timeout=0.5)
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertTrue(fact["timed_out"])
        self.assertTrue(fact["mutating"])
        self.assertTrue(fact["command_dispatched"])
        self.assertGreater(fact["cli_pid"], 0)
        # The original deadline includes interpreter startup. A dispatched CLI
        # may exit before its first event; neither outcome authorizes replay.
        entered = sum(e["event"] == "control.entered" for e in self.events())
        self.assertIn(entered, (0, 1))
        self.assertFalse(self.lock_held())
        owner = owner_for(self.root)
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            owner.activate({})
        self.assertEqual(caught.exception.code, "MachineOperationUnconfirmed")
        self.assertEqual(self.control_fact()["phase"], "command-unknown")
        self.assertEqual(sum(e["event"] == "control.entered" for e in self.events()), entered)
        self.assertFalse((self.root / "control-side-effect").exists())

    def test_control_rpc_does_not_receive_mc_create_reply_settlement_window(self):
        self.start_owner(operation="control", hold_control=True, timeout=0.8)
        self.until(lambda: self.event("control.entered"), "original control RPC")
        request = next((self.root / "state").glob("*/operations/control-*.request"))
        deadline = json.loads(request.read_text())["deadline_monotonic"]
        self.wait_owner(1)
        self.assertLess(time.monotonic(), deadline + 2)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertTrue(fact["timed_out"])
        self.assertTrue(fact["command_dispatched"])
        self.assertNotIn("create_reply_deadline_monotonic", fact)
        self.assertFalse((self.root / "control-side-effect").exists())

    def test_control_leader_stays_unreaped_until_plugin_actually_exits(self):
        self.start_owner(operation="control", plugin=True)
        entered = self.until(lambda: self.event("plugin.entered"), "plugin descendant")
        leader = self.until(lambda: self.event("control.exit"), "CLI leader exit")
        self.assertEqual(entered["group"], leader["pid"])
        self.assertTrue(self.lock_held())
        self.assertIsNone(self.owner_process.poll())
        state = subprocess.check_output(["/bin/ps", "-o", "stat=", "-p", str(leader["pid"])], text=True)
        self.assertTrue(state.strip().startswith("Z"), "the original leader must remain unreaped: " + state)
        self.assertFalse((self.root / "plugin-side-effect").exists())
        self.release("plugin-release")
        self.wait_owner(0)
        self.assertTrue((self.root / "plugin-side-effect").exists())
        fact = self.control_fact()
        self.assertTrue(fact["leader_waited"])
        self.assertTrue(fact["group_exit_observed"])
        self.assertGreater(fact["group_exit_observed_monotonic"], 0)
        self.assertFalse(self.lock_held())

    def test_supervisor_start_delay_cannot_renew_original_absolute_deadline(self):
        self.start_owner(operation="control", timeout=0.2, supervisor_start_delay=0.3)
        self.wait_owner(1)
        self.assertIsNone(self.event("control.entered"))
        self.assertFalse((self.root / "control-side-effect").exists())
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "settled")
        self.assertFalse(fact["command_dispatched"])
        self.assertTrue(fact["timed_out"])
        self.assertEqual(fact["command_exit"], 124)

    def test_owner_sigkill_retains_flock_after_cli_leader_exit_until_plugin_exit(self):
        self.start_owner(operation="control", plugin=True)
        self.until(lambda: self.event("plugin.entered"), "plugin descendant")
        self.until(lambda: self.event("control.exit"), "CLI leader exit")
        self.owner_process.kill()
        self.assertEqual(self.owner_process.wait(timeout=3), -signal.SIGKILL)
        self.assertTrue(self.lock_held())
        self.assertFalse((self.root / "plugin-side-effect").exists())
        self.release("plugin-release")
        self.until(lambda: not self.lock_held(), "actual plugin exit and inherited flock release")
        self.assertIsNotNone(self.event("plugin.exit"))
        self.assertTrue((self.root / "plugin-side-effect").exists())
        self.assertTrue(self.control_fact()["group_exit_observed"])
        self.assertTrue(self.control_fact()["leader_waited"])
        self.assertTrue(self.control_fact()["interrupted"])

    def test_timed_out_term_ignoring_plugin_is_killed_before_lock_release(self):
        self.start_owner(operation="control", plugin=True, plugin_ignore_term=True, timeout=0.3,
                         gate_plugin_deadline=True)
        entered = self.until(lambda: self.event("plugin.entered"), "TERM-ignoring plugin")
        leader = self.until(lambda: self.event("control.exit"), "CLI leader exit")
        self.assertEqual(entered["group"], leader["pid"])
        self.until(lambda: subprocess.check_output(
            ["/bin/ps", "-o", "stat=", "-p", str(leader["pid"])], text=True).strip().startswith("Z"),
            "original leader remains unreaped while plugin runs")
        self.until(lambda: (self.root / "plugin-writes").exists()
                   and (self.root / "plugin-writes").stat().st_size > 0, "actual plugin writes")
        self.assertTrue(self.lock_held())
        self.assertIsNone(self.owner_process.poll())
        journals = list((self.root / "state").glob("*/operations/control-*.request"))
        self.assertEqual(len(journals), 1)
        original_request = journals[0].read_bytes()
        # Establish the real TERM-ignoring descendant and exited leader first.
        # The unchanged deadline is then crossed once, followed by real time.
        self.release("plugin-deadline-release")
        self.until(lambda: (self.root / "plugin-clock-expired.json").exists(), "original deadline crossing")
        time.sleep(0.4)
        self.assertTrue(self.lock_held(), "expiry is not proof of group exit")
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertTrue(fact["timed_out"])
        self.assertTrue(fact["group_exit_observed"])
        self.assertTrue(fact["leader_waited"])
        self.assertFalse(fact["interrupted"])
        clock = json.loads((self.root / "plugin-clock-expired.json").read_text())
        self.assertEqual(clock["original_deadline_monotonic"],
                         json.loads(original_request)["deadline_monotonic"])
        self.assertEqual(clock["original_request_sha256"],
                         hashlib.sha256(original_request).hexdigest())
        self.assertEqual(journals[0].read_bytes(), original_request)
        self.assertFalse(self.lock_held())
        size = (self.root / "plugin-writes").stat().st_size
        time.sleep(0.05)
        self.assertEqual((self.root / "plugin-writes").stat().st_size, size)
        observed = subprocess.run(["/bin/ps", "-o", "stat=", "-p", str(entered["pid"])], capture_output=True, text=True)
        self.assertTrue(not observed.stdout.strip() or observed.stdout.strip().startswith("Z"), observed.stdout)
        self.assertFalse((self.root / "plugin-side-effect").exists())

    def test_nonzero_mutating_control_is_not_reinterpreted_as_completed_rpc(self):
        self.start_owner(operation="control", control_exit=17)
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertEqual(fact["command_exit"], 17)
        owner = owner_for(self.root)
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            owner.activate({})
        self.assertEqual(caught.exception.code, "MachineOperationUnconfirmed")

    def test_observed_daemon_port_failure_can_retry_only_same_saved_intent(self):
        self.start_owner(operation="control", control_args=["compose", "up", "-d"], control_exit=1,
            saved_maintenance=True, control_stderr="Error response from daemon: failed to set up container networking: Bind for 127.0.0.1:28000 failed: port is already allocated")
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "command-unknown")
        self.assertTrue(fact["group_exit_observed"])
        self.assertTrue(fact["leader_waited"])
        self.assertIsNotNone(self.event("control.exit"))
        owner = owner_for(self.root)
        checks = []
        with owner.maintenance_lock(), patch.object(owner.backend, "activation_preflight", side_effect=lambda target, source: checks.append(target["generation"])):
            owner.backend.settle_operations(owner.base)
        self.assertEqual(checks, ["generation-1"])
        self.assertEqual(self.control_fact()["phase"], "settled")
        self.assertFalse(self.control_fact()["command_succeeded"])
        self.assertEqual(owner.read_saved_state()["phase"], "maintenance")
        self.assertEqual(sum(event["event"] == "control.entered" for event in self.events()), 1)

    def test_nonzero_readonly_control_settles_failure_without_mutation_barrier(self):
        self.start_owner(operation="control", control_args=["info", "--format", "{{.ID}}"], control_exit=17)
        self.wait_owner(1)
        fact = self.control_fact()
        self.assertEqual(fact["phase"], "settled")
        self.assertFalse(fact["mutating"])
        self.assertFalse(fact["command_succeeded"])
        owner = owner_for(self.root)
        self.assertFalse(owner.backend.unsettled_operations(owner.base))

    def test_delayed_create_after_unknown_outcome_stays_stopped_and_can_be_recovered(self):
        self.start_owner(unknown_create=True, late_create=True)
        self.wait_owner(1)
        self.until(lambda: self.event("late.entered"), "fake daemon late create")
        self.assertEqual(self.fact()["phase"], "create-unknown")
        self.assertFalse((self.root / "container.json").exists())
        self.release("late-release")
        self.until(lambda: self.event("late.created"), "late stopped container")
        self.assertFalse(json.loads((self.root / "container.json").read_text())["State"]["Running"])
        self.assertIsNone(self.event("start.entered"))
        owner = owner_for(self.root)
        with owner.maintenance_lock():
            owner.backend.settle_operations(owner.base)
        self.assertFalse((self.root / "container.json").exists())
        self.assertEqual(self.fact()["phase"], "settled")
        self.assertEqual(self.fact()["container_id"], "4" * 64)
        self.assertIsNone(self.event("start.entered"))
        self.assertEqual(self.event("rm.entered")["args"][-1], "4444444444444444444444444444444444444444444444444444444444444444")

    def test_foreign_container_is_never_started_or_removed_even_during_recovery(self):
        self.start_owner(foreign=True)
        self.wait_owner(1)
        self.assertEqual(self.fact()["phase"], "cleanup-failed")
        self.assertEqual(self.fact()["error"], "RuntimeIdentityMismatch")
        owner = owner_for(self.root)
        with owner.maintenance_lock():
            with self.assertRaises(runtime.RuntimeFailure) as caught:
                owner.backend.settle_operations(owner.base)
        self.assertEqual(caught.exception.code, "RuntimeIdentityMismatch")
        self.assertTrue((self.root / "container.json").exists())
        self.assertIsNone(self.event("start.entered"))
        self.assertIsNone(self.event("rm.entered"))

    def test_recovery_uses_known_id_even_if_owned_container_was_renamed(self):
        self.start_owner(start_exit=17, rm_exit=17)
        self.assertEqual(self.wait_owner(1)["error"], "ContainerCleanupUnconfirmed")
        fact = self.fact()
        self.assertEqual(fact["container_id"], "4" * 64)
        self.assertEqual(fact["create_reply_id"], "4" * 64)
        state = self.root / "container.json"
        info = json.loads(state.read_text())
        info["Name"] = "renamed-owned-container"
        state.write_text(json.dumps(info))
        self.configure()
        before = len(self.events())
        owner = owner_for(self.root)
        with owner.maintenance_lock():
            owner.backend.settle_operations(owner.base)
        self.assertFalse(state.exists())
        self.assertEqual(self.fact()["phase"], "settled")
        later = self.events()[before:]
        self.assertTrue(all(event["args"][2] == "4" * 64 for event in later if event["event"] == "inspect"))
        self.assertTrue(any(event["args"] == ["container", "rm", "--force", "4" * 64]
                            for event in later if event["event"] == "rm.entered"))

    def test_recovery_rejects_conflicting_reply_and_inspected_container_ids(self):
        self.start_owner(start_exit=17, rm_exit=17)
        self.assertEqual(self.wait_owner(1)["error"], "ContainerCleanupUnconfirmed")
        fact = self.fact()
        fact["create_reply_id"] = "5" * 64
        runtime.atomic_json(self.journal_path(), fact)
        self.configure()
        before = len(self.events())
        owner = owner_for(self.root)
        with owner.maintenance_lock(), self.assertRaisesRegex(runtime.RuntimeFailure, "RuntimeIdentityMismatch"):
            owner.backend.settle_operations(owner.base)
        self.assertTrue((self.root / "container.json").exists())
        self.assertFalse(self.events()[before:])

    def test_recovery_after_crash_with_authoritative_reply_and_exact_id_absent_is_known_failure(self):
        self.configure()
        owner = owner_for(self.root)
        record = operation_record(self.root, owner)
        name = machine.PROJECT + "-mc-" + "a" * 32
        journal = Path(record["compose_file"]).parent / "operations" / (name + ".json")
        fact = dict(schema=1, phase="created", container_name=name, generation=record["generation"],
            image_id=record["images"]["mc"]["image_id"], create_dispatched=True, create_exit=0,
            create_reply_id="4" * 64, create_timed_out=True,
            labels={**owner.backend.labels(record), "novarocks.fixture.version": record["version"],
                    "novarocks.fixture.activation": record["generation"]})
        runtime.atomic_json(journal, fact)
        with owner.maintenance_lock():
            owner.backend.settle_operations(owner.base)
        result = json.loads(journal.read_text())
        self.assertEqual(result["phase"], "settled")
        self.assertEqual(result["container_id"], "4" * 64)
        self.assertTrue(result["container_absent"])
        self.assertFalse(result["command_succeeded"])
        self.assertTrue(all(event["args"][2] == "4" * 64 for event in self.events() if event["event"] == "inspect"))
        self.assertIsNone(self.event("start.entered"))
        self.assertIsNone(self.event("rm.entered"))

    def test_monitor_spawn_failure_closes_both_liveness_pipe_descriptors(self):
        self.configure()
        owner = owner_for(self.root)
        record = operation_record(self.root, owner)
        descriptors = []
        real_pipe = os.pipe

        def capture_pipe():
            pair = real_pipe()
            descriptors.extend(pair)
            return pair

        with owner.maintenance_lock():
            with patch.object(machine.os, "pipe", side_effect=capture_pipe), \
                    patch.object(machine.subprocess, "Popen", side_effect=OSError("fake monitor spawn failure")):
                with self.assertRaises(OSError):
                    owner.backend.run_mc(record, "echo fake")
        self.assertEqual(len(descriptors), 2)
        for fd in descriptors:
            try:
                os.fstat(fd)
            except OSError as error:
                self.assertEqual(error.errno, errno.EBADF)
            else:
                os.close(fd)
                self.fail("liveness pipe descriptor leaked after monitor spawn failure")


class MachineOperationDiagnosticTests(unittest.TestCase):
    def test_original_failure_keeps_private_callsite_and_typed_refusal(self):
        import errno

        with tempfile.TemporaryDirectory(prefix="novarocks-operation-diagnostic-") as directory:
            root = Path(directory)
            secret = "request-only-" + root.name
            for command, original_error in (
                ("mc", PermissionError(errno.EACCES, "original output refused",
                                       str(root / "original.stdout"))),
                ("control", runtime.RuntimeFailure("RuntimeIdentityMismatch", "original identity refused")),
            ):
                with self.subTest(command=command):
                    journal = root / (command + ".json")
                    runtime.atomic_json(journal, dict(schema=1, phase="intent",
                        command_succeeded=False, original_fact="retained"))
                    request = dict(environment={"PRIVATE_TEST_INPUT": secret},
                        deadline_monotonic=time.monotonic() + 5, script=secret)
                    if command == "control":
                        request["operation"] = "control"
                    runtime.atomic_json(journal.with_suffix(".request"), request)
                    request_bytes = journal.with_suffix(".request").read_bytes()

                    def failing_native_operation(_owner):
                        raise original_error

                    method = "run_control" if command == "control" else "run"
                    with (root / "lock").open("wb") as lock, \
                            patch.object(sys, "argv", ["machine_operation.py", str(journal),
                                "--control-fd", str(lock.fileno()), "--lock-fd", str(lock.fileno()),
                                "--timeout", "1"]), \
                            patch.object(operation.CommandOwner, method, new=failing_native_operation), \
                            patch.object(operation.subprocess, "Popen") as launch:
                        self.assertEqual(operation.main(), 1)
                    launch.assert_not_called()
                    fact = json.loads(journal.read_text())
                    self.assertEqual(fact["phase"], "cleanup-failed")
                    self.assertEqual(fact["error"], getattr(original_error, "code", type(original_error).__name__))
                    self.assertEqual(fact["original_fact"], "retained")
                    self.assertFalse(fact["command_succeeded"])
                    diagnostic = fact["error_diagnostics"]
                    self.assertEqual(diagnostic["message"], str(original_error))
                    self.assertIn("failing_native_operation", diagnostic["traceback"])
                    self.assertIn("raise original_error", diagnostic["traceback"])
                    self.assertIn(str(original_error), diagnostic["traceback"])
                    self.assertNotIn(secret, journal.read_text())
                    self.assertEqual(journal.with_suffix(".request").read_bytes(), request_bytes)
                    self.assertEqual(journal.stat().st_mode & 0o777, 0o600)
                    if isinstance(original_error, OSError):
                        self.assertEqual(diagnostic["errno"], errno.EACCES)
                        self.assertEqual(diagnostic["filename"], original_error.filename)
                    else:
                        self.assertIsNone(diagnostic["errno"])
                        self.assertIsNone(diagnostic["filename"])


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--fixture-owner":
        raise SystemExit(run_owner(Path(sys.argv[2])))
    if len(sys.argv) == 10 and sys.argv[1] == "--fixture-clocked-monitor":
        raise SystemExit(run_clocked_monitor(Path(sys.argv[2]), sys.argv[3:]))
    unittest.main()
