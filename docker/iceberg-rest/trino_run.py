#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""Bound one existing-image Trino fixture to its original shared run."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import fixture_run
import fixture_runtime as runtime
from private_deadline import command_guard


def image_command(info):
    config = info.get("Config") or {}
    parts = [config.get("Entrypoint") or [], config.get("Cmd") or []]
    if not all(isinstance(value, list) for value in parts):
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "Trino image entrypoint is not an argv array")
    argv = parts[0] + parts[1]
    if not argv or not all(isinstance(value, str) and value and "\0" not in value for value in argv):
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "Trino image has no exact saved entrypoint")
    return argv


def start(env_file, image, name, config_directory, backend=None):
    values, manifest, budget = fixture_run.admit(env_file)
    backend = backend or runtime.Docker(timeout=90)
    if budget:
        backend.timeout = min(backend.timeout, budget.remaining_seconds(work=True))
        backend.operation_deadline = budget.deadline - budget.cleanup_grace_seconds
    info = backend.inspect("image", image)
    if not info or not info.get("Id"):
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "pinned Trino image is absent")
    record = manifest["runtime"]["machine"] if manifest.get("shared_docker") else manifest["runtime"]["catalog"]
    argv = image_command(info)
    if budget:
        budget.check()
        argv = command_guard(argv, budget.as_dict())
        backend.timeout = min(backend.timeout, budget.remaining_seconds(work=True))
        backend.operation_deadline = budget.deadline - budget.cleanup_grace_seconds
    runtime.safe_component(name)
    config_directory = Path(config_directory).resolve(strict=True)
    if budget:
        fixture_run.startup_sweep(env_file, budget=budget)
    with tempfile.TemporaryDirectory(prefix="novarocks-trino-env-") as directory:
        env = Path(directory) / "container.env"
        credentials = {"NOVA_TRINO_REST_WAREHOUSE": values["NOVA_ENV_REST_WAREHOUSE_URI"],
                       "NOVA_TRINO_S3_ACCESS_KEY": values["AWS_S3_ACCESS_KEY_ID"],
                       "NOVA_TRINO_S3_SECRET_KEY": values["AWS_S3_SECRET_ACCESS_KEY"]}
        if any("\n" in value or "\r" in value for value in credentials.values()):
            raise runtime.RuntimeFailure("InvalidConfiguration", "invalid private Trino environment")
        runtime.atomic_bytes(env, "".join(key + "=" + value + "\n" for key,value in credentials.items()).encode())
        identity = backend.command(["run", "--detach", "--rm", "--pull", "never", "--name", name,
            "--label", "org.novarocks.test=trino-fixture", "--network", record["network"],
            "--env-file", str(env), "--mount", "type=bind,src=" + str(config_directory) + ",dst=/etc/trino,readonly",
            "--entrypoint", argv[0], info["Id"], *argv[1:]])
    try:
        actual = backend.inspect("container", identity)
        if not actual or actual.get("Id") != identity or actual.get("Image") != info["Id"]:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "Trino container differs from the captured image")
    except BaseException:
        if budget:
            backend.operation_deadline = budget.deadline
            backend.timeout = max(0.001, budget.remaining_seconds())
        # The returned full ID is our only cleanup target; never resolve a
        # mutable service or a reused container name after start.
        backend.command(["container", "rm", "--force", identity])
        if backend.inspect("container", identity, absent_ok=True):
            raise runtime.RuntimeFailure("ResourcesStillPresent", "failed Trino start did not actually exit")
        raise
    return identity


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("start", "exec", "stop", "cli", "protocol", "jdbc"))
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--image")
    parser.add_argument("--name")
    parser.add_argument("--config-dir", type=Path)
    parser.add_argument("--container")
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--metrics")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--jdbc-jar")
    args, rest = parser.parse_known_args(argv)
    rest = rest[1:] if rest[:1] == ["--"] else rest
    runtime.install_signal_handlers()
    if args.operation in ("cli", "jdbc"):
        import trino_client_owner
        if not args.image or args.evidence_dir is None:
            raise runtime.RuntimeFailure("InvalidConfiguration", "client operation requires exact image and new evidence directory")
        if args.operation == "cli":
            result = trino_client_owner.cli(args.env_file, args.image, args.evidence_dir, rest, args.metrics)
        else:
            if args.source is None or not args.jdbc_jar:
                raise runtime.RuntimeFailure("InvalidConfiguration", "JDBC operation requires image-local driver and saved source")
            result = trino_client_owner.jdbc(args.env_file, args.image, args.evidence_dir, args.source, args.jdbc_jar, rest)
        sys.stdout.buffer.write(result.stdout)
        sys.stderr.buffer.write(result.stderr)
        return result.returncode
    if args.operation == "start":
        print(start(args.env_file, args.image, args.name, args.config_dir))
        return 0
    if args.operation == "stop" and os.environ.get("NOVA_FIXTURE_RUN_CONTEXT") not in (None, "null"):
        os.environ["NOVA_FIXTURE_CLEANUP_MODE"] = "true"
    _, _, budget = fixture_run.admit(args.env_file)
    timeout = budget.remaining_seconds(work=os.environ.get("NOVA_FIXTURE_CLEANUP_MODE") != "true") if budget else 90
    backend = runtime.Docker(timeout=max(0.001, min(90, timeout)))
    if budget:
        backend.operation_deadline = budget.deadline - (0 if os.environ.get("NOVA_FIXTURE_CLEANUP_MODE") == "true" else budget.cleanup_grace_seconds)
    info = backend.inspect("container", args.container, absent_ok=args.operation == "stop")
    if args.operation == "stop":
        if info:
            if info.get("Id") != args.container:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "Trino cleanup requires its exact container ID")
            backend.command(["container", "rm", "--force", args.container])
            if backend.inspect("container", args.container, absent_ok=True):
                raise runtime.RuntimeFailure("ResourcesStillPresent", "Trino fixture did not exit")
        return 0
    if not info or info.get("Id") != args.container:
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "Trino exec requires its exact container ID")
    if budget:
        timeout = budget.remaining_seconds(work=os.environ.get("NOVA_FIXTURE_CLEANUP_MODE") != "true")
    if args.operation == "protocol":
        rest = ["curl", "--silent", "--show-error", "--include", "--header", "Expect:", *rest]
    result = fixture_run.communicate(["docker", "exec", args.container, *rest], timeout=timeout,
                                    environment=runtime.controlled_environment())
    return result.returncode


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except runtime.RuntimeFailure as error:
        print(error, file=sys.stderr)
        raise SystemExit(75 if error.code in {"RunBudgetRejected", "FixturePrerequisiteMissing", "MachineUnavailable"} else 1)
    except subprocess.TimeoutExpired:
        print("RunBudgetExpired: original Trino fixture deadline expired", file=sys.stderr)
        raise SystemExit(124)
