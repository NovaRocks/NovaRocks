#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Focused executor/control-flow tests; real nextest fixture uses a private target."""
import fcntl
import errno
import importlib.util
import json
import os
from pathlib import Path
import pty
import select
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

REPO = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("test_run", REPO / "tools/ci/test-run.py")
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)


def start_controlling_shell(terminal, case, command):
    # Install the original parent's exit receipt before its one foreground job.
    # Source a private file: a long interactive input line can block on PTY echo.
    script = case.parent / (case.name + ".shell.sh")
    jobs = shlex.quote(str(case / "shell-jobs-before-wait.txt"))
    remaining = shlex.quote(str(case / "shell-jobs-after-wait.txt"))
    receipt = shlex.quote(str(case / "shell-child-wait.tsv"))
    contents = ("umask 077\n"
        "original_child_exit() {\n"
        " trap '' HUP\n"
        " jobs -p > " + jobs + "\n"
        " if [ -s " + jobs + " ]; then kill -HUP %1; kill -CONT %1; fi\n"
        " owner_count=0; owner_status=0\n"
        " while IFS= read -r original_pid; do\n"
        "  owner_count=$((owner_count + 1))\n"
        "  wait \"$original_pid\"\n"
        "  owner_status=$?\n"
        " done < " + jobs + "\n"
        " jobs -p > " + remaining + "\n"
        " if [ ! -s " + remaining + " ]; then owner_empty=true; else owner_empty=false; fi\n"
        " printf '%s\\t%s\\t%s\\t%s\\n' \"$$\" \"$owner_status\" \"$owner_empty\" \"$owner_count\" > " + receipt + "\n"
        "}\n"
        "trap original_child_exit EXIT\n" + command + "\n")
    with os.fdopen(os.open(script, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
        output.write(contents)
    os.write(terminal, ("source " + shlex.quote(str(script)) + "\n").encode())


def exit_controlling_shell(terminal):
    # The installed EXIT trap performs the real parent wait for this same job.
    os.write(terminal, b"fg %1\nexit\n")


def finish_controlling_shell(test, case, trace, shell, terminal, waited, deadline):
    # Close our master first and bound observation by the original five seconds.
    # The Bash EXIT trap owns child wait; this parent owns only shell wait.
    deadline = deadline if deadline is not None else time.monotonic() + 5
    primary_active = sys.exc_info()[0] is not None
    cleanup = dict(shell_pid=shell, shell_reaped=waited, shell_wait_status=None,
                   terminal_closed=terminal < 0, errors=[], owner_wait=None)
    try:
        if terminal >= 0:
            try:
                os.close(terminal)
                cleanup["terminal_closed"] = True
            except OSError as error:
                cleanup["errors"].append("close original PTY master: " + str(error))
            terminal = -1
        while not waited and time.monotonic() < deadline:
            try:
                child, status = os.waitpid(shell, os.WNOHANG)
            except OSError as error:
                cleanup["errors"].append("wait original shell: " + str(error))
                break
            if child:
                waited = True
                cleanup["shell_wait_status"] = status
                break
            time.sleep(min(0.02, max(0, deadline - time.monotonic())))
        cleanup["shell_reaped"] = waited
        receipt = case / "shell-child-wait.tsv"
        if receipt.is_file():
            try:
                fields = receipt.read_text().strip().split("\t")
                if len(fields) != 4 or int(fields[0]) != shell or fields[2] not in ("true", "false"):
                    raise ValueError("original shell child-wait receipt differs")
                count = int(fields[3])
                if count not in (0, 1):
                    raise ValueError("original controlling shell had more than its one job")
                cleanup["owner_wait"] = dict(shell_pid=int(fields[0]), wait_command_status=int(fields[1]),
                                              jobs_empty=fields[2] == "true", waited_job_count=count)
            except (OSError, ValueError) as error:
                cleanup["errors"].append("original child-wait receipt: " + str(error))
        cleanup["owner_exit_confirmed"] = bool(
            waited and cleanup["owner_wait"] and cleanup["owner_wait"]["jobs_empty"])
        if not cleanup["owner_exit_confirmed"]:
            cleanup["errors"].append("original shell child exit remains unconfirmed")
        if not waited:
            cleanup["errors"].append("original shell exit remains unobserved at cleanup deadline")
    finally:
        cleanup["original_root_retained"] = bool(cleanup["errors"])
        if cleanup["original_root_retained"]:
            test.retain_original_root = True
        retain_pty_observation(case, trace, shell, terminal, waited)
        (case / "pty-cleanup.json").write_text(json.dumps(cleanup, indent=2) + "\n")
    # A cleanup refusal must not hide the first assertion. Retain the same root
    # rather than deleting or moving input files still owned by unresolved work.
    if cleanup["errors"] and not primary_active:
        raise AssertionError("original controlling shell cleanup remained unresolved: " + "; ".join(cleanup["errors"]))


def retain_pty_observation(case, trace, shell, terminal, waited):
    case.mkdir(parents=True, exist_ok=True)
    (case / "pty-trace.raw").write_bytes(trace)
    (case / "pty-parent.json").write_text(json.dumps(dict(
        shell_pid=shell, terminal_open=terminal >= 0, shell_reaped=waited),
        indent=2) + "\n")


def controlling_shell_events(test, events):
    # A shared fixture cannot start another event while its original work remains.
    for event in events:
        if test.retain_original_root:
            return
        yield event


class Materials(unittest.TestCase):
    def setUp(self):
        (REPO / "target").mkdir(exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="test-run-tests-", dir=REPO / "target"))
        self.retain_original_root = False

    def tearDown(self):
        if self.retain_original_root:
            (self.root / "retained-original-root.json").write_text(json.dumps(dict(
                original_root=str(self.root), reason="original PTY cleanup remained unresolved"),
                indent=2) + "\n")
        evidence = os.environ.get("NOVA_CI_TEST_EVIDENCE_ROOT")
        if evidence:
            destination = Path(evidence) / self.id()
            destination.mkdir(parents=True, exist_ok=False)
            for directory, children, files in os.walk(self.root):
                children[:] = [name for name in children if name not in ("target", ".git")]
                for name in files:
                    source = Path(directory) / name
                    if source.is_file() and not source.is_symlink():
                        target = destination / source.relative_to(self.root)
                        target.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(source, target)
        if not self.retain_original_root:
            shutil.rmtree(self.root)


    def test_controlling_shell_reaps_its_stopped_owner_after_wrapper_exit(self):
        case = self.root / "stopped-owner-after-wrapper-exit"
        case.mkdir()
        owner = case / "owner.py"
        owner.write_text("import json,os,signal,sys\nfrom pathlib import Path\n"
            "root=Path(sys.argv[1])\n"
            "signals=[getattr(signal,n) for n in ('SIGINT','SIGQUIT','SIGHUP','SIGTERM','SIGTSTP','SIGTTIN','SIGTTOU')]\n"
            "for number in signals: signal.signal(number,signal.SIG_IGN)\n"
            "pid=os.posix_spawn(sys.executable,[sys.executable,'-c','pass'],os.environ,setsigdef=signals,setsigmask=())\n"
            "child,status=os.waitpid(pid,0)\n"
            "(root/'actual-wrapper-exit.json').write_text(json.dumps(dict(pid=pid,wait_status=status)))\n"
            "os.kill(os.getpid(),signal.SIGSTOP)\n"
            "(root/'owner-continued.json').write_text(json.dumps(dict(pid=os.getpid())))\n")
        shell, terminal = pty.fork()
        if shell == 0:
            os.execl("/bin/bash", "bash", "--noprofile", "--norc", "-i")
        trace = bytearray()
        waited = False
        cleanup_deadline = None
        def turn():
            readable, _, _ = select.select([terminal], [], [], 0.02)
            if readable:
                try:
                    trace.extend(os.read(terminal, 65536))
                except OSError:
                    pass
        try:
            command = " ".join(shlex.quote(value) for value in [sys.executable, str(owner), str(case)])
            start_controlling_shell(terminal, case, command)
            receipt = case / "actual-wrapper-exit.json"
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                if receipt.is_file() and b"Stopped" in trace:
                    break
                turn()
            self.assertTrue(receipt.is_file(), trace.decode(errors="replace"))
            self.assertIn(b"Stopped", trace)
            actual = json.loads(receipt.read_text())
            self.assertTrue(os.WIFEXITED(actual["wait_status"]))
            self.assertEqual(os.WEXITSTATUS(actual["wait_status"]), 0)
            self.assertFalse((case / "owner-continued.json").exists())
            exit_controlling_shell(terminal)
            cleanup_deadline = time.monotonic() + 5
            while time.monotonic() < cleanup_deadline:
                child, status = os.waitpid(shell, os.WNOHANG)
                if child:
                    waited = True
                    break
                turn()
            self.assertTrue(waited, "controlling shell did not reap its stopped job: " + trace.decode(errors="replace")[-3000:])
            self.assertTrue(os.WIFEXITED(status))
            self.assertEqual(os.WEXITSTATUS(status), 0)
            self.assertTrue((case / "owner-continued.json").is_file())
        finally:
            finish_controlling_shell(self, case, trace, shell, terminal, waited, cleanup_deadline)

    def test_unresolved_pty_owner_retains_inputs_and_blocks_shared_fixture_suffix(self):
        case = self.root / "unresolved-owner"
        case.mkdir()
        payload = case / "original-input.txt"
        payload.write_bytes(b"original shared fixture input")
        owner = case / "owner.py"
        owner.write_text("import json,os,signal,sys\nfrom pathlib import Path\n"
            "root=Path(sys.argv[1]);release=int(sys.argv[2])\n"
            "for number in (signal.SIGHUP,signal.SIGTERM): signal.signal(number,signal.SIG_IGN)\n"
            "(root/'owner-ready.json').write_text(json.dumps(dict(pid=os.getpid())))\n"
            "assert os.read(release,1)==b'R'\nos.close(release)\n"
            "(root/'owner-exit.json').write_text(json.dumps(dict(pid=os.getpid(),released=True)))\n")
        release_read, release_write = os.pipe()
        os.set_inheritable(release_read, True)
        owner_release_read = release_read
        shell, terminal, waited, status = None, -1, False, None
        original_failure = None
        trace = bytearray()
        # The component owns one eight-second case cut, including post-release
        # observation. An expired cleanup cut below never gets another five seconds.
        case_deadline = time.monotonic() + 8

        def release_and_reap_after_tear_down():
            nonlocal terminal, release_read, release_write, waited, status
            root_retained = self.root.is_dir()
            payload_retained = payload.is_file() and payload.read_bytes() == b"original shared fixture input"
            retained_record = (self.root / "retained-original-root.json").is_file()
            failure_path = case / "pty-cleanup.json"
            failure_unchanged = failure_path.is_file() and failure_path.read_bytes() == original_failure
            errors = []
            # Release belongs to this creator and was held before the original
            # Bash/job existed. No PID-based recovery or new actor is introduced.
            try:
                os.write(release_write, b"R")
            except OSError as error:
                errors.append("release original component owner: " + str(error))
            finally:
                for descriptor in (release_read, release_write, terminal):
                    if descriptor >= 0:
                        try:
                            os.close(descriptor)
                        except OSError as error:
                            errors.append("close original component endpoint: " + str(error))
                release_read, release_write, terminal = -1, -1, -1
            while shell is not None and not waited:
                try:
                    child, status = os.waitpid(shell, os.WNOHANG)
                except OSError as error:
                    errors.append("reap original component shell: " + str(error))
                    break
                if child:
                    waited = True
                    break
                if time.monotonic() >= case_deadline:
                    break
                time.sleep(min(0.02, max(0, case_deadline - time.monotonic())))
            receipt = case / "shell-child-wait.tsv"
            fields = receipt.read_text().strip().split("\t") if receipt.is_file() else []
            owner_exit = case / "owner-exit.json"
            completed = json.loads(owner_exit.read_text()) if owner_exit.is_file() else None
            ready = case / "owner-ready.json"
            original = json.loads(ready.read_text()) if ready.is_file() else None
            owner_reaped = bool(waited and len(fields) == 4 and fields[0] == str(shell)
                                and fields[2] == "true" and fields[3] in ("0", "1")
                                and original and completed == dict(pid=original["pid"], released=True))
            result = dict(shell_pid=shell, shell_reaped=waited, shell_wait_status=status,
                          owner_exit_confirmed=owner_reaped, errors=errors,
                          root_retained_after_tear_down=root_retained,
                          input_retained_after_tear_down=payload_retained,
                          retained_root_record=retained_record,
                          original_cleanup_failure_unchanged=failure_unchanged)
            if case.is_dir():
                (case / "post-release-reap.json").write_text(json.dumps(result, indent=2) + "\n")
                evidence = os.environ.get("NOVA_CI_TEST_EVIDENCE_ROOT")
                if evidence:
                    destination = Path(evidence) / self.id() / "post-release"
                    destination.mkdir(parents=True, exist_ok=False)
                    for name in ("post-release-reap.json", "owner-exit.json", "shell-child-wait.tsv",
                                 "shell-jobs-before-wait.txt", "shell-jobs-after-wait.txt"):
                        source = case / name
                        if source.is_file():
                            shutil.copy2(source, destination / name)
                # The failed cleanup snapshot remains unchanged. Only this
                # creator's later actual reaps permit disposing its original root.
                if owner_reaped and not errors:
                    shutil.rmtree(self.root)
            self.assertTrue(root_retained, result)
            self.assertTrue(payload_retained, result)
            self.assertTrue(retained_record, result)
            self.assertTrue(failure_unchanged, result)
            self.assertTrue(waited, result)
            self.assertTrue(owner_reaped, result)
            self.assertEqual(errors, [], result)

        self.addCleanup(release_and_reap_after_tear_down)
        try:
            shell, terminal = pty.fork()
            if shell == 0:
                os.close(release_write)
                os.execl("/bin/bash", "bash", "--noprofile", "--norc", "-i")
            os.close(release_read)
            release_read = -1
            command = " ".join(shlex.quote(value) for value in [sys.executable, str(owner), str(case), str(owner_release_read)])
            # The inherited descriptor number is captured before the parent's close.
            # Use the actual pipe read descriptor rather than reconstructing an owner.
            start_controlling_shell(terminal, case, command)
            ready = case / "owner-ready.json"
            while not ready.is_file() and time.monotonic() < case_deadline:
                readable, _, _ = select.select([terminal], [], [], 0.02)
                if readable:
                    try:
                        trace.extend(os.read(terminal, 65536))
                    except OSError:
                        pass
            self.assertTrue(ready.is_file(), trace.decode(errors="replace"))
            child, status = os.waitpid(shell, os.WNOHANG)
            waited = bool(child)
            self.assertFalse(waited, "original pipe-held owner unexpectedly exited")
            events = controlling_shell_events(self, ("original", "must-not-start"))
            self.assertEqual(next(events), "original")
            # Model entry after the normal absolute cleanup budget was already spent.
            cleanup_deadline = time.monotonic()
            endpoint, terminal = terminal, -1
            with self.assertRaisesRegex(AssertionError, "original controlling shell cleanup remained unresolved"):
                finish_controlling_shell(self, case, trace, shell, endpoint, waited, cleanup_deadline)
            original_failure = (case / "pty-cleanup.json").read_bytes()
            cleanup = json.loads(original_failure)
            self.assertFalse(cleanup["shell_reaped"])
            self.assertFalse(cleanup["owner_exit_confirmed"])
            self.assertTrue(cleanup["original_root_retained"])
            self.assertTrue(cleanup["errors"])
            self.assertTrue(self.retain_original_root)
            self.assertEqual(list(events), [], "unresolved original work admitted a shared-fixture suffix")

        finally:
            if shell is not None and not waited:
                self.retain_original_root = True

    def context(self, name="run"):
        root = self.root / name
        pin = run.create_context(root)
        self.addCleanup(os.close, pin)
        return root

    def record(self, root, final=True):
        directory = root / "monitor/launch"
        directory.mkdir(parents=True)
        events = [dict(schema=1, event="Pending", label="fixture", payload_os_hex=["2f62696e2f74727565"],
                       native={}, stdout="stdout.log", stderr="stderr.log", deadline_ns=42),
                  dict(schema=1, event="Spawned", pid=123),
                  dict(schema=1, event="Liveness", generation=1, running=False),
                  dict(schema=1, event="ExitObserved", code=0, status=0)]
        if final:
            events.append(dict(schema=1, event="Final", status_raw=0, startup_error=None,
                               owner_lost=False, errors=[], cleanup_complete=True))
        path = directory / "record.jsonl"
        path.write_text("".join(json.dumps(value) + "\n" for value in events))
        return path

    def test_complete_and_failed_test_status_preserve_raw_monitor_facts(self):
        root = self.context()
        path = self.record(root)
        text = path.read_text().replace('"status_raw": 0', '"status_raw": 256')
        path.write_text(text)
        result = run.cleanup(root, time.monotonic() + 1)
        self.assertTrue(result["ok"])
        self.assertEqual(result["records"][0]["final"]["status_raw"], 256)
        self.assertTrue((root / "Closed").is_file())
        self.assertEqual(path.read_text(), text)

    def test_missing_partial_and_cleanup_incomplete_remain_incomplete(self):
        for kind in ("missing", "partial", "incomplete", "no-record"):
            with self.subTest(kind=kind):
                root = self.context(kind)
                path = self.record(root, final=kind != "missing")
                if kind == "partial":
                    path.write_text(path.read_text().rstrip("\n"))
                elif kind == "incomplete":
                    path.write_text(path.read_text().replace('"cleanup_complete": true', '"cleanup_complete": false'))
                elif kind == "no-record":
                    path.unlink()
                result = run.cleanup(root, time.monotonic() + 1)
                self.assertFalse(result["ok"])
                self.assertFalse(result["complete"])

    def test_violations_and_late_registration_do_not_disappear(self):
        root = self.context()
        path = self.record(root)
        path.write_text(path.read_text().replace('"owner_lost": false', '"owner_lost": true'))
        (root / "late-registration-unique").write_text("rejected\n")
        result = run.cleanup(root, time.monotonic() + 1)
        self.assertFalse(result["ok"])
        self.assertEqual(result["late_registrations"], ["late-registration-unique"])
        self.assertIn("payload owner was lost", result["records"][0]["violations"])

    def test_caller_drop_error_survives_eventually_successful_final_without_reading_text(self):
        root = self.context()
        path = self.record(root)
        diagnostic = path.parent / "caller-drop-error.txt"
        diagnostic.write_bytes(b"\xfforiginal caller deadline expired\n" * 10000)
        original_record = path.read_bytes()
        original_diagnostic = diagnostic.read_bytes()
        real_open = os.open
        real_path_open = Path.open
        def regular_open(path, *args, **kwargs):
            self.assertNotEqual(Path(path), diagnostic, "caller diagnostic must not be opened")
            return real_open(path, *args, **kwargs)
        def path_open(path, *args, **kwargs):
            self.assertNotEqual(path, diagnostic, "caller diagnostic content must not be read")
            return real_path_open(path, *args, **kwargs)
        with mock.patch.object(run.os, "open", side_effect=regular_open), \
                mock.patch.object(Path, "open", autospec=True, side_effect=path_open):
            result = run.cleanup(root, time.monotonic() + 1)
        item = result["records"][0]
        self.assertTrue(result["complete"])
        self.assertFalse(result["ok"])
        self.assertTrue(item["final"]["cleanup_complete"])
        self.assertEqual(item["final"]["errors"], [])
        self.assertEqual(item["violations"], ["caller Drop reported cleanup failure"])
        self.assertEqual(item["caller_drop_error"], "monitor/launch/caller-drop-error.txt")
        self.assertEqual(path.read_bytes(), original_record)
        self.assertEqual(diagnostic.read_bytes(), original_diagnostic)

    def test_nonregular_caller_drop_diagnostic_is_an_explicit_error(self):
        for kind in ("directory", "symlink", "fifo"):
            with self.subTest(kind=kind):
                root = self.context(kind)
                path = self.record(root)
                diagnostic = path.parent / "caller-drop-error.txt"
                if kind == "directory":
                    diagnostic.mkdir()
                elif kind == "symlink":
                    diagnostic.symlink_to(path)
                else:
                    os.mkfifo(diagnostic)
                result = run.cleanup(root, time.monotonic() + 1)
                self.assertFalse(result["ok"])
                self.assertFalse(result["complete"])
                self.assertIn("caller-drop diagnostic is not a regular file", result["records"][0]["error"])

    def test_gate_timeout_uses_one_total_budget_and_keeps_open_fact(self):
        root = self.context()
        gate = run.open_file(root / ".gate", os.O_RDWR)
        fcntl.flock(gate, fcntl.LOCK_SH)
        try:
            start = time.monotonic()
            result = run.cleanup(root, start + 0.1)
            self.assertLess(time.monotonic() - start, 1)
            self.assertFalse(result["registration_closed"])
            self.assertFalse((root / "Closed").exists())
            self.assertTrue((root / "closing").exists())
        finally:
            os.close(gate)

    def test_expired_deadline_cannot_accept_an_immediately_available_gate(self):
        root = self.context()
        result = run.cleanup(root, time.monotonic() - 1)
        self.assertFalse(result["ok"])
        self.assertFalse(result["registration_closed"])
        self.assertFalse((root / "Closed").exists())

    def test_normal_domain_exit_keeps_cleanup_deadline_local_to_each_domain(self):
        for first_exit in (0, 1):
            with self.subTest(first_exit=first_exit):
                executor = run.Executor()
                clock = [1000.0]
                with mock.patch.object(run.time, "monotonic", side_effect=lambda: clock[0]):
                    first, first_cleanup, first_deadline = run.execute_domain(
                        executor, self.root / ("first-" + str(first_exit)),
                        [sys.executable, "-c", "raise SystemExit(" + str(first_exit) + ")"],
                        "process", os.environ.copy())
                    # This independent domain starts after the previous cleanup
                    # budget expired. No real signal or control failure occurred.
                    clock[0] = first_deadline + 1
                    second, second_cleanup, second_deadline = run.execute_domain(
                        executor, self.root / ("second-" + str(first_exit)),
                        [sys.executable, "-c", "raise SystemExit(0)"],
                        "process", os.environ.copy())
                self.assertEqual(first["returncode"], first_exit)
                self.assertEqual(first["errors"], [])
                self.assertTrue(first_cleanup["ok"])
                self.assertEqual(first_deadline, 1030.0)
                self.assertEqual(second["returncode"], 0)
                self.assertEqual(second["errors"], [])
                self.assertEqual(second["cancelled"], [])
                self.assertTrue(second_cleanup["ok"])
                self.assertEqual(second_deadline, 1061.0)
                self.assertIsNone(executor.deadline)

    def test_real_cancellation_budget_is_not_renewed_by_exit_or_idle_signal(self):
        executor = run.Executor()
        clock = [1000.0]
        executor.signals.append(signal.SIGTERM)
        with mock.patch.object(run.time, "monotonic", side_effect=lambda: clock[0]):
            result, collected, deadline = run.execute_domain(
                executor, self.root / "real-cancel",
                [sys.executable, "-c", "import time; time.sleep(60)"],
                "process", os.environ.copy())
            self.assertEqual(result["returncode"], -signal.SIGTERM)
            self.assertEqual(result["cancelled"], [signal.SIGTERM])
            self.assertEqual(deadline, 1030.0)
            self.assertTrue(collected["ok"])
            clock[0] = 1015.0
            executor.signals.append(signal.SIGINT)
            executor.observe_idle()
        self.assertEqual(executor.deadline, 1030.0)
        self.assertEqual(executor.cancelled, [signal.SIGTERM, signal.SIGINT])

    def test_cancel_queued_with_actual_child_exit_still_pins_the_original_budget(self):
        for original_deadline in (None, 1010.0):
            with self.subTest(original_deadline=original_deadline):
                executor = run.Executor()
                executor.deadline = original_deadline
                real_waitpid = os.waitpid
                injected = []
                def waitpid(pid, options):
                    child, status = real_waitpid(pid, options)
                    if child and (os.WIFEXITED(status) or os.WIFSIGNALED(status)):
                        self.assertFalse(injected)
                        injected.append(True)
                        # Model a signal arriving in the same turn as actual
                        # exit, before Executor's final queued-signal drain.
                        executor.signals.append(signal.SIGTERM)
                    return child, status
                with mock.patch.object(run.time, "monotonic", return_value=1000.0), \
                        mock.patch.object(run.os, "waitpid", side_effect=waitpid):
                    result, collected, deadline = run.execute_domain(
                        executor, self.root / ("exit-cancel-" + str(original_deadline)),
                        [sys.executable, "-c", "raise SystemExit(0)"],
                        "process", os.environ.copy())
                self.assertEqual(result["returncode"], 0)
                self.assertEqual(result["cancelled"], [signal.SIGTERM])
                self.assertEqual(executor.cancelled, [signal.SIGTERM])
                self.assertTrue(collected["ok"])
                self.assertEqual(deadline, original_deadline or 1030.0)
                self.assertEqual(executor.deadline, original_deadline or 1030.0)

    def test_post_spawn_record_or_display_failure_still_waits_actual_child(self):
        for kind in ("record", "display"):
            with self.subTest(kind=kind):
                executor = run.Executor()
                original = run.write_json
                failed = []
                def write(path, value):
                    if kind == "record" and value.get("started") and not failed:
                        failed.append(True)
                        raise OSError(errno.ENOSPC, "controlled record disk failure")
                    original(path, value)
                display = mock.patch.object(run, "Display", side_effect=OSError(errno.EMFILE, "controlled display init failure")) if kind == "display" else mock.patch.object(run, "Display", wraps=run.Display)
                with mock.patch.object(run, "write_json", side_effect=write), display:
                    result = executor.execute(self.root / kind, [sys.executable, "-c", "import time;time.sleep(60)"], "cargo")
                self.assertTrue(result["errors"])
                self.assertIn("controlled", result["primary_error"]["message"])
                self.assertIsNotNone(result["wait_status"])
                with self.assertRaises(ChildProcessError):
                    os.waitpid(result["pid"], os.WNOHANG)

    def test_closed_inherited_stdio_keeps_posix_spawn_redirections_distinct(self):
        helper = self.root / "closed-stdio.py"
        stage = self.root / "closed-stdio"
        helper.write_text("import importlib.util,os\nfrom pathlib import Path\n"
            "s=importlib.util.spec_from_file_location('run'," + repr(str(REPO / "tools/ci/test-run.py")) + ")\n"
            "m=importlib.util.module_from_spec(s);s.loader.exec_module(m)\n"
            "for fd in (0,1,2): os.close(fd)\n"
            "e=m.Executor()\n"
            "r=e.execute(Path(" + repr(str(stage)) + "),['/bin/sh','-c','read x; printf stdout; printf stderr >&2'],'cargo')\n"
            "assert r['returncode']==0\n")
        result = subprocess.run([sys.executable, str(helper)], capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((stage / "stdout.log").read_bytes(), b"stdout")
        self.assertEqual((stage / "stderr.log").read_bytes(), b"stderr")

    def test_record_lock_timeout_does_not_become_an_empty_success(self):
        root = self.context()
        path = self.record(root)
        fd = run.open_file(path, os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            result = run.cleanup(root, time.monotonic() + 0.1)
            self.assertFalse(result["ok"])
            self.assertEqual(len(result["records"]), 1)
        finally:
            os.close(fd)

    def test_original_streams_are_saved_and_display_is_incremental(self):
        executor = run.Executor()
        stage = self.root / "executor"
        argv = [sys.executable, "-c", "import os,time;os.write(1,b'first\\n');time.sleep(.2);os.write(2,b'last\\n')"]
        with mock.patch.object(run.os, "write", wraps=os.write) as writes:
            result = executor.execute(stage, argv, "cargo")
        self.assertEqual(result["returncode"], 0)
        self.assertEqual((stage / "stdout.log").read_bytes(), b"first\n")
        self.assertEqual((stage / "stderr.log").read_bytes(), b"last\n")
        self.assertTrue(writes.called)
        self.assertGreaterEqual(result["display"]["streams"][0]["queued_bytes"], 6)

    def test_backpressured_or_closed_display_does_not_hold_wrapper_exit(self):
        helper = self.root / "display-helper.py"
        helper.write_text("import importlib.util,sys\nfrom pathlib import Path\n"
            "s=importlib.util.spec_from_file_location('run'," + repr(str(REPO / "tools/ci/test-run.py")) + ")\n"
            "m=importlib.util.module_from_spec(s);s.loader.exec_module(m)\n"
            "e=m.Executor();e.install()\n"
            "try: e.execute(Path(sys.argv[1]),[sys.executable,'-c',\"import os;os.write(1,b'x'*5000000)\"],'cargo')\n"
            "finally: e.restore()\n")
        for closed in (False, True):
            with self.subTest(closed=closed):
                stage = self.root / ("closed" if closed else "blocked")
                child = subprocess.Popen([sys.executable, str(helper), str(stage)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    if closed:
                        child.stdout.close()
                    self.assertEqual(child.wait(timeout=5), 0)
                    result = json.loads((stage / "executor.json").read_text())
                    self.assertEqual((stage / "stdout.log").stat().st_size, 5000000)
                    self.assertEqual(result["returncode"], 0)
                    self.assertFalse(result["display"]["streams"][0]["complete"])
                finally:
                    if child.poll() is None:
                        child.kill()
                    child.wait()
                    if not child.stdout.closed:
                        child.stdout.close()
                    child.stderr.close()

    def test_four_cancellation_signals_reach_only_exact_nextest_child(self):
        for number in run.CANCEL:
            with self.subTest(signal=number):
                stage = self.root / str(number)
                executor = run.Executor()
                executor.install()
                seen = self.root / (str(number) + ".seen")
                program = ("import os,signal,time; from pathlib import Path\n"
                           "def stop(n,f):\n Path(" + repr(str(seen)) + ").write_text(str(n)); raise SystemExit(0)\n"
                           "for n in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP,signal.SIGQUIT): signal.signal(n,stop)\n"
                           "os.write(1,b'ready\\n')\nwhile True: time.sleep(.01)\n")
                def cancel():
                    limit = time.monotonic() + 5
                    while time.monotonic() < limit:
                        output = stage / "stdout.log"
                        if output.exists() and b"ready" in output.read_bytes():
                            os.kill(os.getpid(), number)
                            return
                        time.sleep(0.01)
                sender = threading.Thread(target=cancel)
                sender.start()
                try:
                    result = executor.execute(stage, [sys.executable, "-c", program], "nextest")
                    self.assertEqual(result["cancelled"], [number])
                    self.assertEqual(seen.read_text(), str(int(number)))
                    self.assertEqual(result["returncode"], 0)
                    self.assertIsNotNone(result["wait_status"])
                finally:
                    executor.restore()
                    sender.join(5)

    def test_native_filters_keep_workspace_graph_and_run_only_options(self):
        profile, selection, arguments = run.native_options(["-E", "rdeps(foo)", "--test-threads", "2", "--profile", "default", "--", "--exact", "abc"])
        self.assertEqual(profile, "default")
        self.assertEqual(selection, ["-E", "rdeps(foo)", "--profile", "default", "--", "--exact", "abc"])
        self.assertIn("--test-threads", arguments)
        for option in ("--package", "--target", "--cargo-profile", "--override-version-check", "--no-tests", "--retries"):
            with self.subTest(option=option), self.assertRaises(ValueError):
                run.native_options([option, "other"])

    def identity_inputs(self, root, prepared):
        inputs = root / "inputs"
        for name, value in {
            "cargo-metadata.json": {"workspace_members":["fixture"],"packages":[{"id":"fixture","metadata":{"novarocks":{"test":{"runtime-inputs":[],"env-inputs":[{"name":"NOVAROCKS_BIN","kind":"file-content"}]}}}}]},
            "prepared.json": prepared,
            "test-artifacts.json": {"binaries":{"fixture":{"package-id":"fixture","path":sys.executable}}},
            "selected-tests.json": {"rust-suites":{"fixture":{"testcases":{"case":{"filter-match":{"status":"matches"}}}}}}
        }.items():
            (inputs / name).write_text(json.dumps(value))
        (inputs / "nextest.toml").write_text("[profile.default]\n")

    def identity_batch(self, root, server, during_doctest=None):
        prepared = dict(profile="default", run_argv=["native-nextest"], nextest_version="fixture",
                        junit=str(root / "nextest/default/junit.xml"))
        def fake_execute(executor, stage, argv, adapter, environment=None):
            stage.mkdir(parents=True, exist_ok=True)
            if stage.name == "prepare":
                (root / "inputs").mkdir()
                (root / "inputs/artifacts.json").write_text(json.dumps({
                    "novarocks": dict(path=str(server)),
                    "novarocks-error-manifest": dict(path=sys.executable)}))
                self.identity_inputs(root, prepared)
                (root / "inputs/tool-config.toml").write_text('[store]\ndir = ' + json.dumps(str(root / "store")) + '\n')
            elif stage.name == "rust-run":
                self.assertEqual(environment["NOVAROCKS_BIN"], str(server))
                junit = Path(prepared["junit"])
                junit.parent.mkdir(parents=True)
                junit.write_text("<testsuites/>")
            elif stage.name == "doctest-run":
                self.assertEqual(environment["NOVAROCKS_BIN"], str(server))
                self.assertIn("--doc", argv)
                if during_doctest:
                    during_doctest()
            return dict(started=True, returncode=0, errors=[])
        with mock.patch.object(run.Executor, "execute", fake_execute), mock.patch.object(run, "verify_prepared", return_value=prepared):
            self.assertEqual(run.batch(root, []), 0)
        return {domain: json.loads((root / "identity" / (domain + ".json")).read_text())["scopes"]
                for domain in ("nextest", "doctest")}

    def test_doctest_binary_rewrite_preserves_its_own_identity_confirmation(self):
        server = self.root / "prepared-server"
        server.write_bytes(b"before-doctest")
        original = run.identity_snapshot.digest(server)
        facts = self.identity_batch(self.root / "rewrite-batch", server,
                                    during_doctest=lambda: server.write_bytes(b"after-doctest"))
        self.assertNotEqual(run.identity_snapshot.digest(server), original)
        nextest = facts["nextest"]["fixture::case"]
        self.assertTrue(nextest["inputs_stable"])
        self.assertEqual(nextest["inputs_start"]["env"]["NOVAROCKS_BIN"], original)
        self.assertEqual(nextest["inputs_start"], nextest["inputs_end"])
        doctest = facts["doctest"]["workspace"]
        self.assertTrue(doctest["inputs_stable"])
        self.assertEqual(doctest["inputs_start"], doctest["inputs_end"])
        self.assertNotIn("NOVAROCKS_BIN", doctest["inputs_start"]["env"])

    def test_batch_output_roots_do_not_change_either_execution_domain_identity(self):
        server = self.root / "same-server"
        server.write_bytes(b"same-native-program")
        first = self.identity_batch(self.root / "first-output", server)
        second = self.identity_batch(self.root / "second-output", server)
        for domain in ("nextest", "doctest"):
            with self.subTest(domain=domain):
                self.assertEqual(first[domain], second[domain])
                self.assertTrue(all(scope["inputs_stable"] for scope in first[domain].values()))

    def test_assertion_failure_continues_doctest_and_other_failures_gate(self):
        for exit_code in (0, 100, 101, -signal.SIGTERM):
            with self.subTest(exit_code=exit_code):
                root = self.root / ("batch" + str(exit_code))
                called = []
                prepared = dict(profile="default", run_argv=["native-nextest"], nextest_version="fixture",
                                junit=str(root / "nextest/default/junit.xml"))
                def fake_execute(executor, stage, argv, adapter, environment=None):
                    called.append(stage.name)
                    stage.mkdir(parents=True, exist_ok=True)
                    if stage.name == "prepare":
                        (root / "inputs").mkdir()
                        (root / "inputs/artifacts.json").write_text(json.dumps({name: dict(path=sys.executable) for name in ("novarocks", "novarocks-error-manifest")}))
                        self.identity_inputs(root, prepared)
                    if stage.name == "rust-run":
                        junit = Path(prepared["junit"])
                        junit.parent.mkdir(parents=True)
                        junit.write_text("<testsuites/>")
                    return dict(started=True, returncode=exit_code if stage.name == "rust-run" else 0, errors=[])
                with mock.patch.object(run.Executor, "execute", fake_execute), mock.patch.object(run, "verify_prepared", return_value=prepared):
                    result = run.batch(root, [])
                index = json.loads((root / "execution-index.json").read_text())
                if exit_code in (0, 100):
                    self.assertIn("doctest-run", called)
                    self.assertEqual(index["stages"]["nextest"]["returncode"], exit_code)
                    self.assertEqual(result, 0 if exit_code == 0 else 1)
                else:
                    self.assertNotIn("doctest-run", called)
                    self.assertFalse(index["stages"]["doctest"]["started"])

    def test_final_control_failure_and_late_cancellation_cannot_be_success(self):
        for defect in ("control", "cancel"):
            with self.subTest(defect=defect):
                root = self.root / defect
                prepared = dict(profile="default", run_argv=["native-nextest"], nextest_version="fixture",
                                junit=str(root / "nextest/default/junit.xml"))
                def fake_execute(executor, stage, argv, adapter, environment=None):
                    stage.mkdir(parents=True, exist_ok=True)
                    result = dict(started=True, returncode=0, errors=[])
                    if stage.name == "prepare":
                        (root / "inputs").mkdir()
                        (root / "inputs/artifacts.json").write_text(json.dumps({name: dict(path=sys.executable) for name in ("novarocks", "novarocks-error-manifest")}))
                        self.identity_inputs(root, prepared)
                    elif stage.name == "rust-run":
                        junit = Path(prepared["junit"])
                        junit.parent.mkdir(parents=True)
                        junit.write_text("<testsuites/>")
                    elif stage.name == "final-prepare":
                        if defect == "control":
                            result["errors"].append("controlled final executor failure")
                        else:
                            executor.signals.append(signal.SIGTERM)
                    return result
                with mock.patch.object(run.Executor, "execute", fake_execute), mock.patch.object(run, "verify_prepared", return_value=prepared):
                    code = run.batch(root, [])
                index = json.loads((root / "execution-index.json").read_text())
                self.assertNotEqual(code, 0)
                if defect == "cancel":
                    self.assertEqual(index["cancelled"], [signal.SIGTERM])
                    self.assertEqual(code, 128 + signal.SIGTERM)
                else:
                    self.assertFalse(index["complete"])

    def test_pre_spawn_failure_still_seals_each_independent_domain(self):
        for failed_stage in ("rust-run", "doctest-run"):
            with self.subTest(stage=failed_stage):
                root = self.root / failed_stage
                prepared = dict(profile="default", run_argv=["native-nextest"], nextest_version="fixture",
                                junit=str(root / "nextest/default/junit.xml"))
                def fake_execute(executor, stage, argv, adapter, environment=None):
                    if stage.name == failed_stage:
                        raise OSError(errno.ENOSPC, "controlled pre-spawn file failure")
                    stage.mkdir(parents=True, exist_ok=True)
                    if stage.name == "prepare":
                        (root / "inputs").mkdir()
                        (root / "inputs/artifacts.json").write_text(json.dumps({name: dict(path=sys.executable) for name in ("novarocks", "novarocks-error-manifest")}))
                        self.identity_inputs(root, prepared)
                    elif stage.name == "rust-run":
                        junit = Path(prepared["junit"])
                        junit.parent.mkdir(parents=True)
                        junit.write_text("<testsuites/>")
                    return dict(started=True, returncode=0, errors=[])
                with mock.patch.object(run.Executor, "execute", fake_execute), mock.patch.object(run, "verify_prepared", return_value=prepared):
                    code = run.batch(root, [])
                index = json.loads((root / "execution-index.json").read_text())
                self.assertNotEqual(code, 0)
                self.assertTrue((root / failed_stage / "Closed").is_file())
                summary = json.loads((root / failed_stage / "cleanup-summary.json").read_text())
                self.assertTrue(summary["registration_closed"])
                name = "nextest" if failed_stage == "rust-run" else "doctest"
                self.assertFalse(index["stages"][name]["started"])
                self.assertIn("controlled pre-spawn", index["stages"][name]["primary_error"]["message"])

    def test_hard_kill_doctest_retains_confirmed_nextest_checkpoint(self):
        # The fake executors avoid Cargo and external resources. The batch process
        # is killed in the doctest domain, so its finally block cannot help.
        evidence = Path(os.environ.get("NOVA_CI_TEST_EVIDENCE_ROOT", tempfile.gettempdir()))
        evidence.mkdir(parents=True, exist_ok=True)
        retained = Path(tempfile.mkdtemp(prefix="g15-hard-kill-", dir=evidence)).resolve()
        script = retained / "fake-batch.py"
        script.write_text("""import importlib.util,json,os,sys,time
from pathlib import Path
module_path,root_path=sys.argv[1:]
spec=importlib.util.spec_from_file_location('test_run',module_path)
run=importlib.util.module_from_spec(spec);spec.loader.exec_module(run)
root=Path(root_path)
prepared=dict(schema=1,profile='default',run_argv=['fake-nextest'],nextest_version='fixture',junit=str(root/'nextest/default/junit.xml'))
def execute(executor,stage,argv,adapter,environment=None):
    stage.mkdir(parents=True,exist_ok=True)
    if stage.name=='prepare':
        inputs=root/'inputs';inputs.mkdir()
        values={
          'artifacts.json':{name:{'path':sys.executable} for name in ('novarocks','novarocks-error-manifest')},
          'cargo-metadata.json':{'workspace_members':['p'],'packages':[{'id':'p','metadata':{'novarocks':{'test':{'runtime-inputs':[],'env-inputs':[{'name':'NOVAROCKS_BIN','kind':'file-content'}]}}}}]},
          'prepared.json':prepared,
          'test-artifacts.json':{'schema':1,'binaries':{'p':{'package-id':'p','path':sys.executable}}},
          'selected-tests.json':{'rust-suites':{'p':{'testcases':{'case':{'filter-match':{'status':'matches'}}}}}}}
        (inputs/'nextest.toml').write_text('[profile.default]')
        (inputs/'tool-config.toml').write_text('[store]'+chr(10)+'dir = '+json.dumps(str(root/'nextest'))+chr(10))
        prepared['config_sha256']=run.digest(inputs/'nextest.toml')
        values['test-artifacts.json']['binaries']['p']['sha256']=run.digest(sys.executable)
        for name,value in values.items():(inputs/name).write_text(json.dumps(value))
    elif stage.name=='rust-run':
        junit=Path(prepared['junit']);junit.parent.mkdir(parents=True);junit.write_text('<testsuites><testsuite name="p"><testcase name="case" classname="p" time="0"/></testsuite></testsuites>')
    elif stage.name=='doctest-run':
        (root/'doctest-ready').write_text('ready')
        while True:time.sleep(.05)
    return dict(started=True,returncode=0,errors=[])
run.Executor.execute=execute
run.verify_prepared=lambda _:prepared
sys.exit(run.batch(root,[]))
""")
        attempt = retained / "attempt"
        child = subprocess.Popen([sys.executable, str(script), str(REPO / "tools/ci/test-run.py"), str(attempt)],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True)
        try:
            deadline = time.monotonic() + 15
            while not (attempt / "doctest-ready").exists() and child.poll() is None and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue((attempt / "doctest-ready").is_file(), child.stderr.read() if child.poll() is not None else "doctest did not start")
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)
            index = json.loads((attempt / "execution-index.json").read_text())
            self.assertFalse(index["complete"])
            self.assertNotIn("exit_code", index)
            self.assertEqual(index["stages"]["nextest"]["returncode"],0)
            self.assertTrue(index["materials"]["junit"]["complete"])
            self.assertTrue(index["materials"]["rust_cleanup"]["exists"])
            scopes=json.loads((attempt/"identity/nextest.json").read_text())["scopes"]
            self.assertTrue(scopes["p::case"]["inputs_stable"])
            expected=run.digest(sys.executable)
            self.assertEqual(scopes["p::case"]["inputs_start"]["env"]["NOVAROCKS_BIN"],expected)
            doctest=json.loads((attempt/"identity/doctest.json").read_text())
            self.assertFalse(doctest["scopes"]["workspace"]["inputs_stable"])
            self.assertFalse((attempt/"identity/source-end.json").exists())
            print("hard-kill retained attempt: " + str(attempt))
        finally:
            if child.poll() is None:
                os.killpg(child.pid,signal.SIGKILL)
                child.wait(timeout=5)
            child.stderr.close()

    def test_pre_spawn_primary_and_cleanup_failure_both_survive(self):
        executor = run.Executor()
        with mock.patch.object(executor, "execute", side_effect=OSError(errno.ENOSPC, "primary spawn failure")), \
                mock.patch.object(run, "cleanup", side_effect=OSError(errno.EIO, "independent cleanup failure")):
            result, collected, deadline = run.execute_domain(executor, self.root / "domain", ["/bin/true"], "cargo", os.environ.copy())
        self.assertIn("primary spawn failure", result["primary_error"]["message"])
        self.assertIn("independent cleanup failure", result["cleanup_errors"][0])
        self.assertFalse(collected["ok"])


class NativeNextest(unittest.TestCase):
    setUp = Materials.setUp
    tearDown = Materials.tearDown
    @classmethod
    def setUpClass(cls):
        cls.nextest = os.environ.get("NOVA_CI_NEXTEST_BIN") or shutil.which("cargo-nextest")
        if not cls.nextest:
            raise unittest.SkipTest("native nextest not installed; set NOVA_CI_NEXTEST_BIN for actual metadata/run validation")

    def fixture(self):
        fixture = self.root / "fixture"
        (fixture / "src").mkdir(parents=True)
        (fixture / "tools/ci/lib").mkdir(parents=True)
        for relative in ("test-run.py", "cargo-artifacts.py", "lib/build_capabilities.sh", "lib/build_jobs.py", "lib/identity_snapshot.py"):
            shutil.copy2(REPO / "tools/ci" / relative, fixture / "tools/ci" / relative)
        (fixture / ".config").mkdir()
        configuration = (REPO / ".config/nextest.toml").read_text().split("[test-groups.")[0]
        (fixture / ".config/nextest.toml").write_text(configuration)
        manifest = '[package]\nname="native-fixture"\nversion="0.1.0"\nedition="2024"\n[workspace]\n'
        for name in ("novarocks", "novarocks-sql-test", "novarocks-system-tests", "novarocks-error-manifest"):
            manifest += '\n[[bin]]\nname="' + name + '"\npath="src/main.rs"\n'
        (fixture / "Cargo.toml").write_text(manifest)
        (fixture / "src/main.rs").write_text('fn main() { println!("{}", r#"{"schema_version":1,"actual_cfg":{"debug_assertions":true},"capabilities":[{"owner":"fixture","compiled_capabilities":["test-controls"]}]}"#); }\n')
        (fixture / "src/lib.rs").write_text('''#[test]
fn identity() {
    for name in ["NEXTEST_BINARY_ID", "NEXTEST_TEST_NAME", "NEXTEST_RUN_ID"] {
        assert!(!std::env::var(name).unwrap().is_empty());
    }
}
/// ```
/// assert_eq!(2 + 2, 4);
/// if let Ok(path) = std::env::var("FIXTURE_RELEASE") {
///     std::fs::write(std::env::var("FIXTURE_READY").unwrap(), "ready").unwrap();
///     let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
///     while !std::path::Path::new(&path).exists() {
///         assert!(std::time::Instant::now() < deadline);
///         std::thread::sleep(std::time::Duration::from_millis(10));
///     }
/// }
/// ```
pub struct DocumentationExample;
#[test]
fn assertion() {
    assert!(std::env::var_os("FIXTURE_ASSERT_FAILURE").is_none(), "controlled assertion failure");
}
#[test]
fn linger() {
    if let Ok(path) = std::env::var("FIXTURE_RELEASE") {
        std::fs::write(std::env::var("FIXTURE_READY").unwrap(), "ready").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !std::path::Path::new(&path).exists() {
            assert!(std::time::Instant::now() < deadline, "fixture release deadline expired");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
#[test]
fn leak() {
    if let Ok(path) = std::env::var("FIXTURE_LEAK_SOCKET") {
        let script = "import socket,array,sys; s=socket.socket(socket.AF_UNIX); s.connect(sys.argv[1]); s.sendmsg([b'fd'], [(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[1]))])";
        assert!(std::process::Command::new(std::env::var("FIXTURE_PYTHON").unwrap())
            .args(["-c", script, &path]).status().unwrap().success());
    }
}
''')
        subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=fixture, check=True)
        subprocess.run(["git", "init", "-q"], cwd=fixture, check=True)
        subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-qm", "fixture"], cwd=fixture, check=True)
        (fixture / ".gitignore").write_text("target/\n")
        output = self.root / "prepared"
        with mock.patch.object(run, "REPO", fixture), mock.patch.dict(os.environ, {"NOVA_CI_NEXTEST_BIN": self.nextest}):
            prepared = run.prepare(output, [], self.root / "native-store")
            run.verify_prepared(output)
        return fixture, output, prepared

    def native_argv(self, prepared, name, filterset):
        tool = self.root / (name + ".toml")
        tool.write_text("[store]\ndir = " + json.dumps(str(self.root / (name + "-store"))) + "\n")
        argv = list(prepared["run_argv"])
        argv[argv.index("--tool-config-file") + 1] = "test-fixture:" + str(tool)
        return argv + ["-E", filterset]

    def test_real_metadata_final_binding_and_run_never_calls_cargo(self):
        fixture, output, prepared = self.fixture()
        forbidden = self.root / "forbidden-cargo"
        forbidden.mkdir()
        cargo = forbidden / "cargo"
        cargo.write_text("#!/bin/sh\necho unexpected Cargo invocation >&2\nexit 99\n")
        cargo.chmod(0o755)
        environment = dict(os.environ, PATH=str(forbidden) + os.pathsep + os.environ["PATH"])
        result = subprocess.run(prepared["run_argv"], cwd=fixture, env=environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(Path(prepared["junit"]).is_file())
        self.assertNotIn("unexpected Cargo invocation", result.stderr)
        binaries = json.loads((output / "test-artifacts.json").read_text())["binaries"]
        self.assertTrue(binaries)
        first = Path(next(iter(binaries.values()))["path"])
        with first.open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaises(subprocess.CalledProcessError):
            run.verify_prepared(output)

    def test_real_formal_assertion_failure_preserves_junit_and_runs_doctest(self):
        fixture, output, prepared = self.fixture()
        destination = self.root / "batch"
        environment = dict(os.environ, NOVA_CI_NEXTEST_BIN=self.nextest, FIXTURE_ASSERT_FAILURE="1")
        result = subprocess.run([sys.executable, str(fixture / "tools/ci/test-run.py"), "batch", "--output", str(destination)],
                                cwd=fixture, env=environment, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 1, result.stderr)
        index = json.loads((destination / "execution-index.json").read_text())
        self.assertTrue(index["complete"], index)
        self.assertEqual(index["stages"]["nextest"]["returncode"], 100)
        self.assertEqual(index["stages"]["doctest"]["returncode"], 0)
        self.assertIn("running 1 test", (destination / "doctest-run/stdout.log").read_text())
        self.assertTrue(index["materials"]["junit"]["complete"])
        self.assertTrue((destination / "inputs/artifacts.json").is_file())
        self.assertTrue((destination / "final-prepare/artifacts.json").is_file())
        self.assertTrue((destination / "rust-run/Closed").is_file())
        self.assertTrue((destination / "doctest-run/Closed").is_file())

    def test_pinned_leak_deadline_does_not_renew_for_continuous_output(self):
        fixture, output, prepared = self.fixture()
        address = self.root / "leak.sock"
        # The outer test really owns and waits this broker. The fixture transfers
        # a captured stdout FD; no orphan helper is created to manufacture a leak.
        program = '''import array,os,socket,sys,time
s=socket.socket(socket.AF_UNIX);s.bind(sys.argv[1]);s.listen(1)
c,_=s.accept();data,control,flags,peer=c.recvmsg(16,socket.CMSG_SPACE(4))
fds=array.array('i');fds.frombytes(control[0][2]);fd=fds[0]
while True:
 try: os.write(fd,b'continuous leaked output\\n')
 except BrokenPipeError: time.sleep(.01);continue
 time.sleep(.02)
'''
        broker = subprocess.Popen([sys.executable, "-c", program, str(address)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while not address.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(address.exists())
            environment = dict(os.environ, FIXTURE_PYTHON=sys.executable, FIXTURE_LEAK_SOCKET=str(address))
            started = time.monotonic()
            result = subprocess.run(self.native_argv(prepared, "leak", "test(=leak)"), cwd=fixture,
                                    env=environment, capture_output=True, text=True, timeout=8)
            self.assertEqual(result.returncode, 100, result.stderr)
            self.assertIn("LEAK", result.stderr)
            self.assertLess(time.monotonic() - started, 8)
            self.assertIsNone(broker.poll())
        finally:
            broker.terminate()
            broker.wait(timeout=5)
            broker.stderr.close()

    def test_native_foreground_pty_pause_continue_and_four_cancellations(self):
        fixture, output, prepared = self.fixture()
        helper = self.root / "pty-wrapper.py"
        helper.write_text("import importlib.util,json,os,sys\nfrom pathlib import Path\n"
            "s=importlib.util.spec_from_file_location('run'," + repr(str(REPO / "tools/ci/test-run.py")) + ")\n"
            "m=importlib.util.module_from_spec(s);s.loader.exec_module(m)\n"
            "root=Path(sys.argv[1]);root.mkdir()\n(root/'wrapper.pid').write_text(str(os.getpid()))\n"
            "e=m.Executor();e.install()\n"
            "try:\n r=e.execute(root/'native',json.loads(Path(sys.argv[2]).read_text()),sys.argv[3]);m.write_json(root/'done.json',r)\n"
            "finally: e.restore()\n")
        owner = self.root / "pty-owner.py"
        owner.write_text("import json,os,signal,sys\nfrom pathlib import Path\n"
            "signals=[getattr(signal,n) for n in ('SIGINT','SIGQUIT','SIGHUP','SIGTERM','SIGTSTP','SIGTTIN','SIGTTOU')]\n"
            "for n in signals: signal.signal(n,signal.SIG_IGN)\n"
            "pid=os.posix_spawn(sys.executable,[sys.executable]+sys.argv[1:],os.environ,setsigdef=signals,setsigmask=())\n"
            "while True:\n child,status=os.waitpid(pid,os.WUNTRACED|os.WCONTINUED)\n"
            " if os.WIFSTOPPED(status): os.kill(os.getpid(),signal.SIGSTOP)\n"
            " elif os.WIFEXITED(status) or os.WIFSIGNALED(status):\n"
            "  Path(sys.argv[2],'actual-wrapper-exit.json').write_text(json.dumps(dict(pid=pid,wait_status=status)))\n"
            "  break\n")
        for event in controlling_shell_events(self, (
                "pause", "TTIN", "TTOU", "pause-TERM", "INT", "QUIT", "HUP", "TERM", "close", "doc-pause", "doc-TERM")):
            with self.subTest(event=event):
                case = self.root / event
                argv_file = self.root / (event + ".argv.json")
                adapter = "cargo" if event.startswith("doc-") else "nextest"
                argv = (["cargo", "test", "--manifest-path", str(fixture / "Cargo.toml"), "--doc", "--locked",
                         "--target-dir", str(fixture / "target")] if adapter == "cargo" else self.native_argv(prepared, event, "test(=linger)"))
                argv_file.write_text(json.dumps(argv))
                ready, release = self.root / (event + ".ready"), self.root / (event + ".release")
                shell, terminal = pty.fork()
                if shell == 0:
                    os.environ.update(FIXTURE_READY=str(ready), FIXTURE_RELEASE=str(release))
                    os.execl("/bin/bash", "bash", "--noprofile", "--norc", "-i")
                trace = bytearray()
                def turn():
                    if terminal < 0:
                        time.sleep(0.02)
                        return
                    readable, _, _ = select.select([terminal], [], [], 0.02)
                    if readable:
                        try:
                            trace.extend(os.read(terminal, 65536))
                        except OSError:
                            pass
                def until(predicate, seconds=8):
                    deadline = time.monotonic() + seconds
                    while time.monotonic() < deadline:
                        if predicate():
                            return
                        turn()
                    self.fail("PTY did not reach expected state: " + trace.decode(errors="replace")[-3000:])
                waited = False
                cleanup_deadline = None
                try:
                    command = " ".join(shlex.quote(value) for value in [sys.executable, str(owner), str(helper), str(case), str(argv_file), adapter])
                    start_controlling_shell(terminal, case, command)
                    until(ready.exists)
                    pid = int((case / "wrapper.pid").read_text())
                    if event in ("pause", "TTIN", "TTOU", "pause-TERM", "doc-pause"):
                        if event in ("TTIN", "TTOU"):
                            os.killpg(os.tcgetpgrp(terminal), getattr(signal, "SIG" + event))
                        else:
                            os.write(terminal, b"\x1a")
                        until(lambda: b"Stopped" in trace)
                        state = json.loads((case / "native/executor.json").read_text())
                        self.assertTrue(any(item["event"] == "stopped" for item in state["state_events"]))
                        if event == "pause-TERM":
                            os.killpg(os.getpgid(pid), signal.SIGTERM)
                            time.sleep(0.1)
                            self.assertFalse((case / "done.json").exists(), "stopped Python cannot handle bare TERM")
                            os.killpg(os.getpgid(pid), signal.SIGCONT)
                        else:
                            os.write(terminal, b"fg\n")
                            release.write_text("release")
                    elif event in ("INT", "QUIT"):
                        os.write(terminal, b"\x03" if event == "INT" else b"\x1c")
                    elif event == "close":
                        os.close(terminal)
                        terminal = -1
                    else:
                        # This is a real foreground job in a controlling shell;
                        # HUP/TERM are delivered to that actual foreground group.
                        self.assertEqual(os.tcgetpgrp(terminal), os.getpgid(pid))
                        os.killpg(os.tcgetpgrp(terminal), getattr(signal, "SIG" + event.removeprefix("doc-")))
                    until(lambda: (case / "done.json").is_file())
                    until(lambda: (case / "actual-wrapper-exit.json").is_file())
                    result = json.loads((case / "done.json").read_text())
                    actual_exit = json.loads((case / "actual-wrapper-exit.json").read_text())
                    self.assertEqual(actual_exit["pid"], pid)
                    self.assertTrue(os.WIFEXITED(actual_exit["wait_status"]))
                    if event in ("pause", "TTIN", "TTOU", "doc-pause"):
                        self.assertEqual(result["returncode"], 0)
                        self.assertIn(signal.SIGCONT, result["signals"])
                    elif event == "pause-TERM":
                        self.assertEqual(result["cancelled"], [signal.SIGTERM])
                    elif event == "close":
                        self.assertIn(signal.SIGHUP, result["cancelled"])
                    else:
                        self.assertEqual(result["cancelled"], [getattr(signal, "SIG" + event.removeprefix("doc-"))])
                    if terminal >= 0:
                        exit_controlling_shell(terminal)
                    cleanup_deadline = time.monotonic() + 5
                    while time.monotonic() < cleanup_deadline:
                        child, status = os.waitpid(shell, os.WNOHANG)
                        if child:
                            waited = True
                            break
                        turn()
                    self.assertTrue(waited, "controlling shell did not exit")
                finally:
                    finish_controlling_shell(self, case, trace, shell, terminal, waited, cleanup_deadline)


if __name__ == "__main__":
    unittest.main(verbosity=2)
