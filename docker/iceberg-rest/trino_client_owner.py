#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""TRC-1 client container owner invoked only by trino_run's explicit operations."""
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.request

import fixture_run
import fixture_runtime as runtime
from private_deadline import command_guard


def metric(uri, name, labels=None):
    with urllib.request.urlopen(uri, timeout=2) as response:
        values = json.load(response)
    matches = [entry["value"] for entry in values if entry.get("tags", {}).get("metric") == name and
               all(entry["tags"].get(key) == value for key, value in (labels or {}).items())]
    if len(matches) != 1 or isinstance(matches[0], bool) or int(matches[0]) != matches[0] or matches[0] < 0:
        raise runtime.RuntimeFailure("InvalidMetric", "missing, duplicate or invalid original metric " + name)
    return int(matches[0])


def run(env_file, image, directory, command, *, mounts=(), interrupt_metrics=None):
    """Own one exact child through actual exit; an original guard bounds lost parents."""
    _, _, budget = fixture_run.admit(env_file)
    if budget is None:
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "client requires original shared fixture budget")
    image_info = runtime.Docker(timeout=min(90, budget.remaining_seconds(work=True))).inspect("image", image)
    manifest = "sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534"
    if not image_info or (image_info.get("Id") != manifest and not any(value.endswith("@" + manifest) for value in (image_info.get("RepoDigests") or []))):
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "client image does not expose the frozen Trino483 manifest")
    image = image_info["Id"]
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    cidfile = directory / "container-id"
    intent = {"schema": 1, "image_id": image, "network": "host", "cidfile": str(cidfile), "container_id": None,
              "state": "registered", "run_context": budget.as_dict()}
    runtime.atomic_json(directory / "owner.json", intent)
    guarded = command_guard(command, budget.as_dict())
    argv = ["docker", "run", "--rm", "--pull", "never", "--network", "host", "--user", str(os.getuid()) + ":" + str(os.getgid()), "--cidfile", str(cidfile)]
    for source, target in mounts:
        argv += ["--mount", "type=bind,src=" + str(Path(source).resolve(strict=True)) + ",dst=" + target + ",readonly"]
    argv += ["--entrypoint", guarded[0], image, *guarded[1:]]
    runtime.atomic_json(directory / "command.json", {"argv": argv})
    child = None
    errors = []
    code = None
    backend = runtime.Docker(timeout=min(90, budget.remaining_seconds()))
    backend.operation_deadline = budget.deadline
    try:
        if interrupt_metrics:
            active_before = metric(interrupt_metrics, "novarocks_trino_queries_active")
            cancelled_before = metric(interrupt_metrics, "novarocks_trino_query_settlements_total", {"outcome": "client_cancel"})
        with (directory / "stdout").open("xb") as stdout, (directory / "stderr").open("xb") as stderr:
            child = subprocess.Popen(argv, stdout=stdout, stderr=stderr, env=runtime.controlled_environment(), start_new_session=True)
            while not cidfile.is_file():
                if child.poll() is not None:
                    raise runtime.RuntimeFailure("ContainerCreationUnknown", "Docker exited without returning an exact child identity")
                budget.check()
                time.sleep(0.025)
            identity = cidfile.read_text().strip()
            if not re.fullmatch("[0-9a-f]{64}", identity):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "invalid client container ID")
            intent.update(container_id=identity, state="created")
            runtime.atomic_json(directory / "owner.json", intent)
            if interrupt_metrics:
                while metric(interrupt_metrics, "novarocks_trino_queries_active") <= active_before:
                    if child.poll() is not None:
                        raise runtime.RuntimeFailure("ClientExitedEarly", "official CLI exited before its original statement became active")
                    budget.check()
                    time.sleep(0.025)
                # command wrapper records its own PID before exec; never guess a JVM PID or signal PID1's deadline guard.
                result = fixture_run.communicate(["docker", "exec", identity, "/bin/sh", "-c",
                    'read target </tmp/trc1-cli.pid; case "$target" in ""|*[!0-9]*) exit 1;; esac; kill -INT "$target"; printf "%s\\n" "$target"'],
                    timeout=budget.remaining_seconds(work=True), capture=True, environment=runtime.controlled_environment())
                runtime.atomic_bytes(directory / "sigint.stdout", result.stdout or b"")
                runtime.atomic_bytes(directory / "sigint.stderr", result.stderr or b"")
                runtime.atomic_json(directory / "sigint.json", {"container_id": identity, "signal": "SIGINT", "exit": result.returncode,
                    "original_active_before": active_before, "original_cancelled_before": cancelled_before})
                if result.returncode:
                    raise runtime.RuntimeFailure("SignalDeliveryFailed", "official CLI SIGINT delivery failed")
            code = child.wait(timeout=budget.remaining_seconds(work=True))
            if interrupt_metrics:
                while metric(interrupt_metrics, "novarocks_trino_queries_active") != active_before:
                    budget.check()
                    time.sleep(0.025)
                cancelled = metric(interrupt_metrics, "novarocks_trino_query_settlements_total", {"outcome": "client_cancel"})
                if cancelled != cancelled_before + 1:
                    raise runtime.RuntimeFailure("CancellationNotObserved", "original query owner did not settle exactly one client cancellation")
                runtime.atomic_json(directory / "cluster-cancellation.json", {"active_before": active_before, "active_after": active_before,
                    "cancelled_before": cancelled_before, "cancelled_after": cancelled, "status": "PASS"})
    finally:
        identity = intent.get("container_id")
        if identity is None and cidfile.is_file():
            candidate = cidfile.read_text().strip()
            if re.fullmatch("[0-9a-f]{64}", candidate):
                identity = candidate
                intent["container_id"] = candidate
        if identity:
            try:
                info = backend.inspect("container", identity, absent_ok=True)
                if info:
                    if info.get("Id") != identity:
                        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "client cleanup lost exact container identity")
                    backend.command(["container", "rm", "--force", identity])
                    if backend.inspect("container", identity, absent_ok=True):
                        raise runtime.RuntimeFailure("ResourcesStillPresent", "official client remains after cleanup")
            except BaseException as error:
                errors.append(str(error))
        else:
            errors.append("container creation outcome is unknown")
        if child is not None and child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                child.wait(timeout=min(2, budget.remaining_seconds()))
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait(timeout=budget.remaining_seconds())
        intent.update(state="cleanup_failed" if errors else "released", errors=errors,
                      child_waited=child is not None and child.poll() is not None, native_exit=code)
        runtime.atomic_json(directory / "owner.json", intent)
        if errors:
            raise runtime.RuntimeFailure("ResourcesStillPresent", "; ".join(errors))
    return subprocess.CompletedProcess(argv, code, (directory / "stdout").read_bytes(), (directory / "stderr").read_bytes())


def cli(env_file, image, directory, arguments, metrics):
    arguments = list(arguments)
    mounts = []
    if "--probe-sql-file" in arguments:
        index = arguments.index("--probe-sql-file")
        mounts.append((arguments[index + 1], "/probe/session.sql"))
        del arguments[index:index + 2]
    interrupt = "--probe-interrupt" in arguments
    if interrupt and not metrics:
        raise runtime.RuntimeFailure("InvalidConfiguration", "CLI interrupt requires its original FE metrics endpoint")
    if interrupt:
        arguments.remove("--probe-interrupt")
    command = ["/bin/sh", "-c", 'printf "%s\\n" "$$" >/tmp/trc1-cli.pid; exec /usr/bin/trino "$@"', "trc1-cli", *arguments]
    return run(env_file, image, directory, command, mounts=mounts, interrupt_metrics=metrics if interrupt else None)


def jdbc(env_file, image, directory, source, driver, arguments):
    # Java source-file mode reuses image-local Java/JDBC; it never fetches a driver or compiler.
    command = ["java", "--class-path", driver, "/probe/JdbcProbe.java", *arguments]
    return run(env_file, image, directory, command, mounts=[(source, "/probe/JdbcProbe.java")])
