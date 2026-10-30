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

"""Own one exact Docker command until its CLI and container actually exit.

The caller passes its maintenance lock descriptor and only a liveness pipe's
read end. Owner loss closes the pipe; the monitor keeps the flock until cleanup.
This is a command supervisor, not a resource discovery/reaper service.
"""
from __future__ import annotations

import argparse
import ctypes
import json
import math
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import struct
import sys
import time
import traceback
import uuid
from typing import NamedTuple

import fixture_runtime as runtime

CLEANUP_TIMEOUT_SECONDS = 5


class CommandResult(NamedTuple):
    code: int
    timed_out: bool
    dispatched: bool


def process_group_running(group):
    """Observe executing members of the original, still-pinned process group."""
    if sys.platform.startswith("linux"):
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                text = (entry / "stat").read_text(errors="replace")
            except FileNotFoundError:
                continue
            try:
                fields = text.rsplit(")", 1)[1].split()
                if int(fields[2]) == group and fields[0] not in {"Z", "X"}:
                    return True
            except (IndexError, ValueError) as error:
                raise runtime.RuntimeFailure("ProcessGroupUnobserved", "invalid native process state") from error
        return False
    if sys.platform != "darwin":
        raise runtime.RuntimeFailure("ProcessGroupUnobserved", "native group observation is unavailable")
    native = ctypes.CDLL(None, use_errno=True)
    list_pids = native.proc_listpids
    list_pids.argtypes = [ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_int]
    list_pids.restype = ctypes.c_int
    pid_info = native.proc_pidinfo
    pid_info.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
    pid_info.restype = ctypes.c_int
    capacity = 64
    while True:
        pids = (ctypes.c_int * capacity)()
        size = ctypes.sizeof(pids)
        # Darwin PROC_PGRP_ONLY=2; the live unreaped leader prevents PGID reuse.
        count = list_pids(2, group, pids, size)
        if count < 0:
            raise OSError(ctypes.get_errno(), "native process group enumeration failed")
        if count == size:
            if capacity >= 65536:
                raise runtime.RuntimeFailure("ProcessGroupUnobserved", "native group enumeration exceeds bound")
            capacity *= 2
            continue
        if count % ctypes.sizeof(ctypes.c_int):
            raise runtime.RuntimeFailure("ProcessGroupUnobserved", "invalid native group enumeration")
        for pid in list(pids)[:count // ctypes.sizeof(ctypes.c_int)]:
            if pid <= 0:
                continue
            # sys/proc_info.h proc_bsdshortinfo has 64 bytes, with pid/ppid/
            # pgid/status as its initial native uint32 fields; SZOMB=5.
            info = ctypes.create_string_buffer(64)
            length = pid_info(pid, 13, 0, info, ctypes.sizeof(info))
            if length != ctypes.sizeof(info):
                error = ctypes.get_errno()
                if error == 3:
                    continue
                raise OSError(error, "native process group member is unobserved")
            _, _, actual_group, status = struct.unpack_from("=IIII", info.raw)
            if actual_group == group and status != 5:
                return True
        return False


class CommandOwner:
    def __init__(self, request, journal, control_fd, timeout):
        self.request, self.journal = request, journal
        self.fact = runtime.read_json(journal)
        self.control_fd, self.owner_lost = control_fd, False
        self.deadline = request.get("deadline_monotonic")
        if type(self.deadline) not in (int, float) or not math.isfinite(self.deadline):
            raise runtime.RuntimeFailure("InvalidConfiguration", "supervisor requires the original absolute deadline")
        self.environment = request["environment"]

    def save(self, **changes):
        self.fact.update(changes)
        runtime.atomic_json(self.journal, self.fact)

    def alive(self):
        if not self.owner_lost and select.select([self.control_fd], [], [], 0)[0]:
            # No heartbeat or process lookup: this pipe has exactly one writer.
            self.owner_lost = os.read(self.control_fd, 1) == b""
        return not self.owner_lost

    def command(self, args, output, *, creating=False, cleanup=False, settle_create=False, host=False):
        if settle_create and (not creating or cleanup or args[0] != "create"
                              or self.request.get("operation") == "control"):
            raise runtime.RuntimeFailure("InvalidConfiguration", "reply settlement is only available to mc create")
        deadline = time.monotonic() + CLEANUP_TIMEOUT_SECONDS if cleanup else self.deadline
        observe_deadline = deadline + CLEANUP_TIMEOUT_SECONDS if settle_create else deadline
        stderr_path = Path(str(output) + ".stderr")
        with Path(output).open("wb") as stdout, stderr_path.open("wb") as stderr:
            os.chmod(output, 0o600)
            os.chmod(stderr_path, 0o600)
            if not cleanup:
                alive = self.alive()
                expired = time.monotonic() >= deadline
                if expired or not alive:
                    return CommandResult(124 if expired else 130, expired, False)
            process = subprocess.Popen(args if host else ["docker", *args], stdout=stdout, stderr=stderr,
                                       env=self.environment, start_new_session=True)
            # waitid(WNOWAIT) observes without reaping the original leader.
            # Its identity pins the PGID through the final group observation.
            self.fact.update(cli_pid=process.pid, group_exit_confirmed=False)
            timed_out, stopping, killed, observation_failed = False, None, False, False
            while True:
                now = time.monotonic()
                owner_alive = self.alive()
                # Late CREATE completion can only settle resources, never
                # turn expired work into success or permit a later START.
                timed_out = timed_out or now >= deadline
                try:
                    observed = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                except OSError as error:
                    # No further signal is safe if the original unreaped
                    # identity itself is unobserved. Keep the flock and retry.
                    observation_failed = timed_out = True
                    self.fact["leader_observation_error"] = type(error).__name__
                    time.sleep(0.01)
                    continue
                try:
                    running = process_group_running(process.pid)
                    if observed is not None and not running:
                        self.fact["group_exit_confirmed"] = True
                        self.fact["group_exit_observed"] = True
                        self.fact["group_exit_observed_monotonic"] = time.monotonic()
                        break
                except (OSError, runtime.RuntimeFailure) as error:
                    # An unavailable observation is never permission to release
                    # this group's flock or to signal a possibly reused PGID.
                    if not observation_failed:
                        self.fact["group_observation_error"] = getattr(error, "code", type(error).__name__)
                        observation_failed = True
                    timed_out = True
                if stopping is None and (now >= observe_deadline or observation_failed
                        or (not owner_alive and not creating and not cleanup)):
                    timed_out = timed_out or now >= deadline
                    stopping = now
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                if stopping is not None and now >= stopping + 2 and not killed:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    killed = True
                # Even after escalation or expiry the supervisor retains the
                # lock and unreaped Child until actual group exit is observed.
                time.sleep(0.01)
            result = process.wait()
            self.fact["leader_waited"] = True
            self.save()
        return CommandResult(result, timed_out, True)

    def inspect(self, identity=None):
        path = self.journal.with_suffix(".inspect")
        target = identity or self.fact.get("container_id") or self.fact["container_name"]
        result = self.command(["container", "inspect", target,
                                "--format", "{{json .}}"], path, cleanup=True)
        if result.code:
            # Missing container and unavailable daemon are not interchangeable.
            stderr = Path(str(path) + ".stderr").read_text(errors="replace").lower()
            if "no such container" in stderr or "no such object" in stderr:
                return None
            raise runtime.RuntimeFailure("DockerOperationFailed", "cannot observe operation container")
        try:
            info = json.loads(path.read_text())
        except ValueError as error:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "invalid operation inspect result") from error
        if ((identity is not None and info.get("Id") != identity)
                or (self.fact.get("container_id") and info.get("Id") != self.fact["container_id"])
                or info.get("Image") != self.fact["image_id"] or any(
            info.get("Config", {}).get("Labels", {}).get(k) != v for k, v in self.fact["labels"].items()
        )):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "operation container ownership changed")
        return info

    def run(self):
        alive = self.alive()
        if not alive or time.monotonic() >= self.deadline:
            self.save(phase="settled", interrupted=not alive, container_absent=True,
                      create_dispatched=False, command_succeeded=False,
                      timed_out=time.monotonic() >= self.deadline)
            return 130 if not alive else 124
        args = ["create", "--pull=never", "--name", self.fact["container_name"],
                "--network", self.request["network"], "--env-file", self.request["env_file"]]
        for key, value in self.fact["labels"].items():
            args += ["--label", f"{key}={value}"]
        args += ["--entrypoint", "/bin/sh", self.fact["image_id"], "-c", self.request["script"]]
        self.save(phase="creating", create_dispatched=True,
                  create_reply_deadline_monotonic=self.deadline + CLEANUP_TIMEOUT_SECONDS)
        result = self.command(args, self.journal.with_suffix(".create"), creating=True, settle_create=True)
        code, timed_out = result.code, result.timed_out
        self.save(create_exit=code, create_timed_out=timed_out, create_dispatched=result.dispatched)
        if not result.dispatched:
            self.save(phase="settled", container_absent=True, command_succeeded=False,
                      timed_out=timed_out, interrupted=self.owner_lost)
            return code
        reply_id = None
        if code == 0:
            try:
                reply = self.journal.with_suffix(".create").read_bytes()
            except OSError:
                reply = b""
            if not re.fullmatch(rb"[0-9a-f]{64}\n?", reply):
                self.save(phase="create-unknown", error="DockerCreateReplyInvalid", interrupted=self.owner_lost)
                return 1
            reply_id = reply.rstrip(b"\n").decode("ascii")
            self.save(create_reply_id=reply_id)
        container = self.inspect(reply_id)
        if container is None:
            # A lost create reply is not proof that a late stopped container
            # cannot appear. Retain uncertainty; no start has been dispatched.
            self.save(phase="create-unknown", error="DockerCreateUnconfirmed", interrupted=self.owner_lost)
            return 1
        identity = container["Id"]
        self.save(phase="created", container_id=identity)
        command_exit = None
        try:
            if code == 0 and self.alive() and time.monotonic() < self.deadline:
                self.save(phase="running", start_dispatched=True)
                started = self.command(["start", "--attach", identity], self.journal.with_suffix(".stdout"))
                command_exit = started.code
                timed_out = timed_out or started.timed_out
                self.save(command_exit=command_exit, timed_out=timed_out, start_dispatched=started.dispatched,
                          interrupted=not self.alive())
            else:
                timed_out = timed_out or time.monotonic() >= self.deadline
                self.save(interrupted=not self.alive(), command_exit=None, timed_out=timed_out)
        finally:
            removed = self.command(["container", "rm", "--force", identity],
                                    self.journal.with_suffix(".cleanup"), cleanup=True)
            if removed.code or self.inspect() is not None:
                self.save(phase="cleanup-failed", error="ContainerCleanupFailed")
                raise runtime.RuntimeFailure("ContainerCleanupFailed", "operation container did not exit")
            self.save(phase="settled", container_absent=True,
                      timed_out=timed_out,
                      command_succeeded=command_exit == 0 and not self.owner_lost and not timed_out)
        return 0 if self.fact.get("command_succeeded") else 1

    def run_host_sdk(self):
        """The same original owner pins one SDK process group through exit."""
        request = self.request
        entry = Path(__file__).with_name("oss_ops.py").resolve()
        python, config = Path(request["sdk_python"]), Path(request["sdk_config"])
        if (not python.is_absolute() or not python.is_file()
                or request.get("sdk_entry") != str(entry)
                or not config.is_absolute() or not config.is_file()):
            raise runtime.RuntimeFailure("InvalidConfiguration", "host SDK inputs are not fixed absolute files")
        from oss_ops import validate_config
        definition = validate_config(runtime.read_json(config))
        if definition != request.get("object_store"):
            raise runtime.RuntimeFailure("InvalidConfiguration", "host SDK definition differs from activation")
        arguments = validate_sdk_arguments(request["args"])
        remaining = self.deadline - time.monotonic()
        argv = [str(python), str(entry), "--config", str(config), "--timeout-seconds", str(max(remaining, 0.001)), *arguments]
        # Publish possible dispatch before Popen. A killed supervisor cannot
        # turn an unknown live SDK group into a never-started operation.
        self.save(phase="running", command_dispatched=True)
        result = self.command(argv, self.journal.with_suffix(".stdout"), host=True)
        interrupted = not self.alive()
        self.save(phase="settled", command_exit=result.code, timed_out=result.timed_out,
                  command_dispatched=result.dispatched, interrupted=interrupted,
                  remote_outcome="unknown" if result.dispatched and (result.timed_out or result.code != 0) and self.fact["mutating"] else "observed",
                  command_succeeded=result.code == 0 and not result.timed_out and not interrupted)
        return 0 if self.fact["command_succeeded"] else 1

    def run_control(self):
        alive = self.alive()
        if not alive or time.monotonic() >= self.deadline:
            expired = time.monotonic() >= self.deadline
            self.save(phase="settled", interrupted=not alive, command_dispatched=False,
                      command_succeeded=False, command_exit=130 if not alive else 124,
                      timed_out=expired)
            return 130 if not alive else 124
        self.save(phase="running", command_dispatched=True)
        # Complete this one control call even after owner loss. No later call
        # is dispatched, and the inherited lock remains held through CLI exit.
        result = self.command(self.request["args"], self.journal.with_suffix(".stdout"), creating=True)
        code, timed_out = result.code, result.timed_out
        uncertain = result.dispatched and (timed_out or code != 0) and self.fact["mutating"]
        self.save(phase="command-unknown" if uncertain else "settled",
                  command_exit=code, timed_out=timed_out, command_dispatched=result.dispatched, interrupted=not self.alive(),
                  command_succeeded=code == 0 and not timed_out)
        return 0 if self.fact["command_succeeded"] else 1


def validate_sdk_arguments(arguments):
    """Closed SDK grammar: callers cannot replace the provider or config."""
    if (not isinstance(arguments, (list, tuple)) or not arguments
            or len(arguments) > 7 or not all(isinstance(value, str) and value and "\x00" not in value for value in arguments)):
        raise runtime.RuntimeFailure("InvalidConfiguration", "invalid host SDK arguments")
    command = arguments[0]
    extra = {"put": "--input", "get": "--output", "abort-multipart": "--upload-id"}
    if command not in {"put", "get", "stat", "list", "delete", "list-multipart", "abort-multipart"}:
        raise runtime.RuntimeFailure("InvalidConfiguration", "unsupported host SDK operation")
    expected = 5 if command in extra else 3
    if len(arguments) != expected or arguments[1] != "--uri" or (command in extra and arguments[3] != extra[command]):
        raise runtime.RuntimeFailure("InvalidConfiguration", "host SDK arguments differ from the closed grammar")
    # Validate before the real provider/metadata endpoint can be reached.
    from scratch import s3_parts
    s3_parts(arguments[2])
    return list(arguments)


def run_owned_host_sdk_operation(backend, record, args, *, deadline=None):
    """Reuse the original machine maintenance flock and command supervisor."""
    from scratch import cloud_storage_owner
    arguments = validate_sdk_arguments(args)
    owner = cloud_storage_owner(record)
    if owner is None or record.get("storage_owner") != owner:
        raise runtime.RuntimeFailure("InvalidConfiguration", "host SDK requires its exact cloud storage owner")
    from oss_ops import OpsError, OssOperations
    # Pure URI validation with a sentinel transport; it creates no SDK/client.
    try:
        with OssOperations(record["config"]["object_store"], bucket=object()) as scope:
            scope._key(arguments[2])
    except OpsError as error:
        raise runtime.RuntimeFailure("InvalidScratchScope", "host SDK target leaves the closed cloud owner") from error
    limits = [time.monotonic() + backend.timeout]
    limits += [value for value in (deadline, getattr(backend, "operation_deadline", None)) if value is not None]
    absolute = min(limits)
    if not math.isfinite(absolute) or absolute <= time.monotonic():
        raise runtime.RuntimeFailure("OperationDeadlineExceeded", "host SDK deadline elapsed before dispatch")
    if getattr(backend, "maintenance_fd", None) is None:
        raise runtime.RuntimeFailure("MaintenanceLockRequired", "host SDK requires the machine maintenance lock")
    python = Path(record["oss_ops_python"])
    config = Path(record["object_store_config_file"])
    activation = Path(record["compose_file"]).parent.resolve()
    if not python.is_absolute() or not config.is_absolute() or config.parent.resolve() != activation:
        raise runtime.RuntimeFailure("InvalidConfiguration", "host SDK inputs differ from the frozen activation")
    base = getattr(backend, "maintenance_base", None)
    if base is None or Path(base).resolve() != (Path(record["control_root"]) / record["id"]).resolve():
        raise runtime.RuntimeFailure("MaintenanceLockRequired", "host SDK requires its original machine maintenance base")
    journal = Path(base) / "operations" / ("oss-" + uuid.uuid4().hex + ".json")
    runtime.atomic_json(journal, dict(schema=1, operation="host-sdk", kind="host-sdk", phase="intent",
        control_root=record["control_root"], machine=record["id"],
        generation=record["generation"], storage_owner=owner,
        mutating=arguments[0] in {"put", "delete", "abort-multipart"}))
    environment = dict(backend.environment)
    for name in ("PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP", "PYTHONUSERBASE",
                 "HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
        environment.pop(name, None)
    environment["PYTHONNOUSERSITE"] = "1"
    environment["NETRC"] = os.devnull
    result = backend._supervise(journal, dict(operation="host-sdk", environment=environment,
        sdk_python=str(python), sdk_entry=str(Path(__file__).with_name("oss_ops.py").resolve()),
        sdk_config=str(config), object_store=record["config"]["object_store"],
        args=arguments, deadline_monotonic=absolute), absolute - time.monotonic())
    output = journal.with_suffix(".stdout")
    if output.exists() and output.stat().st_size > 16 * 1024 * 1024:
        raise runtime.RuntimeFailure("ObjectStoreOperationFailed", "host SDK output exceeds its byte bound")
    text = output.read_text() if output.exists() else ""
    if (not result.get("command_succeeded") or any(result.get(key) is not True for key in
            ("group_exit_confirmed", "group_exit_observed", "leader_waited"))):
        code = "ObjectStoreOperationTimeout" if result.get("timed_out") else "ObjectStoreOperationFailed"
        error = runtime.RuntimeFailure(code, "host SDK operation did not succeed: " + str(journal))
        error.stdout = text
        raise error
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path)
    parser.add_argument("--control-fd", required=True, type=int)
    parser.add_argument("--lock-fd", required=True, type=int)
    parser.add_argument("--timeout", required=True, type=float)
    args = parser.parse_args()
    # Retain the inherited flock until process exit. Child Docker CLIs do not
    # inherit either descriptor; only this supervisor owns the lock after loss.
    os.fstat(args.lock_fd)
    request = runtime.read_json(args.journal.with_suffix(".request"))
    owner = CommandOwner(request, args.journal, args.control_fd, args.timeout)
    try:
        if request.get("operation") == "host-sdk":
            return owner.run_host_sdk()
        return owner.run_control() if request.get("operation") == "control" else owner.run()
    except BaseException as error:
        owner.save(phase="cleanup-failed", error=getattr(error, "code", type(error).__name__),
                   error_diagnostics=dict(
                       errno=error.errno if isinstance(error, OSError) else None,
                       filename=os.fsdecode(error.filename)
                           if isinstance(error, OSError) and error.filename is not None else None,
                       message=str(error), traceback=traceback.format_exc()))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
