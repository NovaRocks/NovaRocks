#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. The ASF licenses this file under the Apache License, Version 2.0.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Cumulative fixture budgets and an exact container-local process owner."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time
import uuid

import fixture_runtime as runtime
from scratch import RunBudget, RunStorageScope, ScratchIdentity, RUN_STORAGE_ENV


def env_values(path):
    result = {}
    for line in Path(path).read_text().splitlines():
        words = shlex.split(line, comments=True)
        if not words:
            continue
        if words[0] == "unset":
            for key in words[1:]:
                result.pop(key, None)
        else:
            value = line.removeprefix("export ").strip()
            if "=" in value:
                key, text = value.split("=", 1)
                parsed = shlex.split(text, comments=True)
                if len(parsed) == 1:
                    result[key] = parsed[0]
    return result


def cloud_configuration(values, record):
    """Consume the exact non-secret machine definition without provider fallback."""
    config = record.get("config", {})
    if not isinstance(config, dict):
        raise runtime.RuntimeFailure("InvalidScratchScope", "machine storage configuration is invalid")
    if "object_store" not in config:
        owner = record.get("storage_owner", {})
        if values.get("NOVA_ENV_OBJECT_STORE_KIND") == "aliyun-oss" or (
                isinstance(owner, dict) and owner.get("provider") == "aliyun-oss"):
            raise runtime.RuntimeFailure("InvalidScratchScope", "cloud machine storage configuration is missing")
        return None
    from oss_ops import OpsError, validate_config
    try:
        cloud = validate_config(config["object_store"])
    except (OpsError, ValueError, TypeError) as error:
        raise runtime.RuntimeFailure("InvalidScratchScope", "cloud machine storage configuration is invalid") from error
    if cloud != config["object_store"]:
        raise runtime.RuntimeFailure("InvalidScratchScope", "cloud machine storage configuration is not canonical")
    return cloud


def worktree_root(values, record, *, published_env_id):
    cloud = cloud_configuration(values, record)
    if cloud is None:
        return "s3://novarocks/" + values["NOVA_ENV_ID"]
    env_id = values.get("NOVA_ENV_ID")
    if not isinstance(env_id, str):
        raise runtime.RuntimeFailure("InvalidScratchScope", "cloud publication worktree identity is missing")
    env_id = runtime.safe_component(env_id)
    if env_id != published_env_id:
        raise runtime.RuntimeFailure("InvalidScratchScope", "cloud publication worktree identity differs from its manifest")
    return f"s3://{cloud['bucket']}/{cloud['root_prefix']}worktrees/{env_id}"


def admit(env_file, seconds=None, *, inherited=None):
    values = env_values(env_file)
    manifest = json.loads(Path(values.get("NOVA_ENV_MANIFEST", str(Path(env_file).with_name("manifest.json")))).read_text())
    if not manifest.get("ready"):
        raise runtime.RuntimeFailure("MachineUnavailable", "fixture publication is not ready")
    record = manifest.get("runtime", {}).get("machine")
    if not record:
        return values, manifest, None
    inherited = inherited if inherited is not None else os.environ.get("NOVA_FIXTURE_RUN_CONTEXT")
    if isinstance(inherited, str) and len(inherited) > 16384:
        raise runtime.RuntimeFailure("RunBudgetRejected", "inherited context exceeds its input bound")
    inherited = json.loads(inherited) if isinstance(inherited, str) else inherited
    if inherited:
        cloud_configuration(values, record)
        identity = ScratchIdentity.from_dict(inherited["identity"])
    else:
        identity = ScratchIdentity.create(worktree_root(values, record, published_env_id=manifest.get("env_id")))
    budget = RunBudget.admit(record, identity, seconds=int(seconds if seconds is not None else os.environ.get("NOVA_FIXTURE_BUDGET_SECS", "3600")), inherited=inherited, cleanup=os.environ.get("NOVA_FIXTURE_CLEANUP_MODE") == "true")
    scope = RunStorageScope.from_record(identity, record)
    raw_scope = os.environ.get(RUN_STORAGE_ENV)
    if raw_scope not in (None, "null"):
        if len(raw_scope) > 16384:
            raise runtime.RuntimeFailure("InvalidScratchScope", "inherited storage exceeds its input bound")
        RunStorageScope.from_dict(json.loads(raw_scope)).validate_against(identity, record)
    os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(budget.as_dict(), sort_keys=True)
    os.environ[RUN_STORAGE_ENV] = json.dumps(scope.as_dict(), sort_keys=True)
    return values, manifest, budget


def cleanup_run(env_file, *, budget=None):
    """The original owner calls this only after every writer actually exited."""
    from machine_runtime import MachineOwner
    from scratch import ScratchSweeper
    _, manifest, admitted = admit(env_file, inherited=budget.as_dict() if budget else None)
    budget = budget if budget is not None else admitted
    if budget is None or os.environ.get("NOVA_FIXTURE_CLEANUP_MODE") != "true":
        raise runtime.RuntimeFailure("RunBudgetRejected", "cleanup requires the original shared run context")
    record = manifest["runtime"]["machine"]
    owner = MachineOwner(record["control_root"], daemon=record["daemon_id"],
                         timeout=max(0.001, budget.remaining_seconds()))
    with owner.ready_snapshot({"version":record["version"]}, deadline=budget.deadline,
                              expected_generation=record["generation"]) as current:
        scope = RunStorageScope.from_dict(json.loads(os.environ[RUN_STORAGE_ENV])).validate_against(budget.identity, current)
        return ScratchSweeper(current, owner.backend).cleanup_run(scope, [], budget.deadline)


def startup_sweep(env_file, *, budget=None):
    """One explicit writer-start sweep; read-only admission never invokes it."""
    original = budget
    _, manifest, admitted = admit(env_file, inherited=budget.as_dict() if budget is not None else None)
    if admitted is None:
        return dict(complete=True, pending=False, skipped=True, reason="private-publication", errors=[], removed=[])
    budget = original if original is not None else admitted
    budget.check(work=True)
    record = manifest["runtime"]["machine"]
    from machine_runtime import MachineOwner, SweepLockTimeout
    deadline = min(time.monotonic() + 5, budget.deadline - budget.cleanup_grace_seconds)
    owner = MachineOwner(record["control_root"], daemon=record["daemon_id"],
                         timeout=max(0.001, deadline - time.monotonic()))
    try:
        report = owner.sweep(deadline=deadline, expected_version=record["version"],
                             expected_generation=record["generation"])
    except SweepLockTimeout:
        # No GC was dispatched. Ordinary readiness is a separate read-only
        # admission, bounded by its normal timeout and this same run's work end.
        report = dict(complete=False, enumeration_complete=False, observations_unavailable=True,
                      removed=[], errors=[dict(operation="startup-sweep-lock", code="ScratchSweepTimeout")])
        try:
            budget.check(work=True)
            admission = MachineOwner(record["control_root"], daemon=record["daemon_id"])
            admission_deadline = min(time.monotonic() + admission.timeout,
                                     budget.deadline - budget.cleanup_grace_seconds)
            with admission.ready_snapshot({"version": record["version"]},
                                          deadline=admission_deadline,
                                          expected_generation=record["generation"]):
                budget.check(work=True)
            report["admission_complete"] = True
        except runtime.RuntimeFailure as error:
            report["errors"].append(dict(operation="startup-admission", code=error.code))
            error.report = report
            raise
        except OSError as cause:
            error = runtime.RuntimeFailure("ScratchSweepIOFailed", "startup admission I/O failed; its report is retained")
            report["errors"].append(dict(operation="startup-admission", code=error.code))
            error.report = report
            raise error from cause
    except runtime.RuntimeFailure as error:
        report = dict(complete=False, enumeration_complete=False, observations_unavailable=True, removed=[],
                      errors=[dict(operation="startup-sweep", code=error.code)])
        if error.code != "ScratchSweepTimeout":
            error.report = report
            raise
    except OSError as cause:
        error = runtime.RuntimeFailure("ScratchSweepIOFailed", "startup sweep I/O failed; its report is retained")
        error.report = dict(complete=False, enumeration_complete=False, observations_unavailable=True, removed=[],
                            errors=[dict(operation="startup-sweep", code=error.code)])
        raise error from cause
    try:
        budget.check(work=True)
    except runtime.RuntimeFailure as error:
        error.report = report
        raise
    if not isinstance(report, dict) or type(report.get("complete")) is not bool or not isinstance(report.get("errors"), list):
        raise runtime.RuntimeFailure("InvalidScratchSweepReport", "startup sweep returned an invalid report")
    report = dict(report, pending=not report["complete"])
    errors = report["errors"]
    failed = [error for error in errors if not isinstance(error, dict) or error.get("code") != "ScratchSweepTimeout"]
    if failed or (not report["complete"] and not errors) or (report["complete"] and errors):
        code = next((error.get("code") for error in failed if isinstance(error, dict) and isinstance(error.get("code"), str)), "InvalidScratchSweepReport")
        error = runtime.RuntimeFailure(code, "startup sweep failed; its report is retained")
        error.report = report
        raise error
    return report


# This process remains inside Spark when the Docker client is killed. It owns
# the unreaped child and its process group until the original complete deadline.
CONTAINER_OWNER = r'''
import json, os, pathlib, re, shutil, signal, subprocess, sys, time
folder, deadline, cleanup = pathlib.Path(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3])
context = json.loads(sys.argv[4])
complete_end = time.monotonic() + max(0, deadline - time.time())
def publish_result(value):
    pending = folder / "result.pending"
    pending.write_text(json.dumps(value))
    pending.replace(folder / "result.json")
# Only directories carrying our exact run identity, schema and private mode
# are eligible. Arbitrary prefix matches and deadline text are not authority.
for old in list(pathlib.Path("/tmp").glob("novarocks-fixture-data-nr_scratch_*")) + list(pathlib.Path("/tmp").glob("novarocks-fixture-run-nr_scratch_*")):
    match = re.fullmatch(r"novarocks-fixture-(?:data|run)-(nr_scratch_([0-9]{13})_[0-9a-f]{32})-[0-9a-f]{32}", old.name)
    marker = old / ".fixture-context.json"
    if not match or old.is_symlink() or not old.is_dir() or not marker.is_file() or marker.is_symlink():
        continue
    try:
        facts = json.loads(marker.read_text())
        old_context = facts["context"]
        if (facts.get("owner") == "novarocks-fixture-run-v1" and facts.get("schema") == 1
            and old.stat().st_uid == os.getuid() and old.stat().st_mode & 0o777 == 0o700
            and old_context.get("schema") == 1 and old_context["identity"]["run_id"] == match[1]
            and old_context["identity"]["created_unix_ms"] == int(match[2])
            and old_context["identity"]["scope_root"].endswith("/scratch/" + match[1])
            and type(old_context["started_unix_ms"]) is int and type(old_context["deadline_unix_ms"]) is int
            and old_context["started_unix_ms"] >= int(match[2]) - 5000
            and old_context["deadline_unix_ms"] > old_context["started_unix_ms"]
            and old_context["deadline_unix_ms"] / 1000 < time.time()):
            if old.name.startswith("novarocks-fixture-run-"):
                result = json.loads((old / "result.json").read_text())
                if result.get("child_waited") is not True: continue
            else:
                safe = True
                for operation in pathlib.Path("/tmp").glob("novarocks-fixture-run-" + match[1] + "-*"):
                    try:
                        result = json.loads((operation / "result.json").read_text())
                        if result.get("child_waited") is not True: safe = False
                    except (OSError, ValueError):
                        safe = False
                if not safe: continue
            shutil.rmtree(old)
    except (OSError, ValueError, KeyError, TypeError):
        pass
folder.mkdir(mode=0o700, parents=True, exist_ok=False)
(folder / ".fixture-context.json").write_text(json.dumps({"schema":1,"owner":"novarocks-fixture-run-v1","context":context}))
stop = folder / "stop"
if time.monotonic() >= complete_end - cleanup:
    publish_result({"returncode":124,"child_waited":True,"never_started":True})
    sys.exit(124)
try:
    child = subprocess.Popen(sys.argv[5:], start_new_session=True)
except OSError:
    publish_result({"returncode":127,"child_waited":True,"never_started":True})
    sys.exit(127)
cancelled = False
def cancel(number, frame):
    global cancelled
    cancelled = True
signal.signal(signal.SIGTERM, cancel)
signal.signal(signal.SIGINT, cancel)
expired = False
while os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
    if cancelled or stop.exists() or time.monotonic() >= complete_end - cleanup:
        expired = True
        break
    time.sleep(0.025)
if expired:
    try: os.killpg(child.pid, signal.SIGTERM)
    except ProcessLookupError: pass
    term_end = min(complete_end, time.monotonic() + 2)
    while time.monotonic() < term_end:
        if os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None:
            break
        time.sleep(0.025)
# Keep the leader unreaped while inspecting its group, so the PGID cannot
# be reused by another launch during actual descendant settlement.
try: os.killpg(child.pid, signal.SIGKILL)
except ProcessLookupError: pass
cleanup_failed = False
failure_published = False
while True:
    live, observable = [], True
    for item in pathlib.Path("/proc").iterdir():
        if not item.name.isdigit(): continue
        try:
            text = (item / "stat").read_text()
            fields = text[text.rfind(")") + 2:].split()
            if int(fields[2]) == child.pid and fields[0] not in ("Z", "X"):
                live.append(int(item.name))
        except (FileNotFoundError, ProcessLookupError):
            pass
        except PermissionError:
            observable = False
    if time.monotonic() >= complete_end:
        cleanup_failed = True
        if not failure_published:
            publish_result({"returncode":125,"child_waited":False,"cleanup_failed":True,
                            "cleanup_deadline_expired":time.monotonic() >= complete_end})
            failure_published = True
    if observable and not live:
        break
    # Failure at the deadline is evidence, never permission to abandon the
    # unreaped leader or release its PGID pin while descendants still execute.
    try: os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError: pass
    time.sleep(0.025)
child.wait()
code = 125 if cleanup_failed else (124 if expired else child.returncode)
publish_result({"returncode":code,"child_waited":True,"cleanup_failed":cleanup_failed})
sys.exit(code if code >= 0 else 128-code)
'''

CONTAINER_SETTLE = r'''
import json, pathlib, sys, time
folder, deadline = pathlib.Path(sys.argv[1]), float(sys.argv[2])
context = json.loads(sys.argv[3])
complete_end = time.monotonic() + max(0, deadline - time.time())
try:
    folder.mkdir(mode=0o700, parents=True, exist_ok=False)
    (folder / ".fixture-context.json").write_text(json.dumps({"schema":1,"owner":"novarocks-fixture-run-v1","context":context}))
    # A late Docker exec cannot start its owner through this tombstone.
    (folder / "result.json").write_text(json.dumps({"returncode": 125, "child_waited": True, "never_started": True}))
except FileExistsError:
    pass
(folder / "stop").touch()
while time.monotonic() < complete_end:
    if (folder / "result.json").is_file():
        value = json.loads((folder / "result.json").read_text())
        if value.get("cleanup_failed"):
            print(json.dumps(value)); sys.exit(125)
        if value.get("child_waited"):
            print(json.dumps(value)); sys.exit(0)
    time.sleep(0.025)
sys.exit(125)
'''


def communicate(command, *, timeout, data=None, environment=None, capture=False,
                stdout=None, stderr=None, cwd=None):
    if capture and (stdout is not None or stderr is not None):
        raise ValueError("capture and explicit output destinations are mutually exclusive")
    process = subprocess.Popen(command, stdin=subprocess.PIPE if data is not None else None,
        stdout=subprocess.PIPE if capture else stdout, stderr=subprocess.PIPE if capture else stderr,
        env=environment, cwd=cwd, start_new_session=True)
    try:
        output, errors = process.communicate(data, timeout=timeout)
        return subprocess.CompletedProcess(command, process.returncode, output, errors)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
        raise


def compose(env_file, arguments, *, data=None, capture=False):
    values, manifest, budget = admit(env_file)
    command = ["docker", "compose", *arguments]
    if not budget:
        # Private stacks retain their independent lifecycle and do not gain a
        # shared-machine identity or an invented shared scratch namespace.
        return communicate(command, timeout=3600, data=data, capture=capture, environment=runtime.controlled_environment())
    cleanup = os.environ.get("NOVA_FIXTURE_CLEANUP_MODE") == "true"
    budget.check(work=not cleanup)
    if "run" in arguments:
        tail = arguments[arguments.index("run") + 1:]
        if "mc" not in tail or any(option in tail for option in ("-v", "--volume")):
            raise runtime.RuntimeFailure("InvalidConfiguration", "governed compose run requires mc without host mounts")
        entrypoint = tail[tail.index("--entrypoint") + 1] if "--entrypoint" in tail else "/bin/sh"
        script = shlex.join([entrypoint, *tail[tail.index("mc") + 1:]])
        from machine_runtime import MachineOwner
        record = manifest["runtime"]["machine"]
        owner = MachineOwner(record["control_root"], daemon=record["daemon_id"], timeout=max(0.001, budget.remaining_seconds(work=not cleanup)))
        with owner.ready_snapshot({"version": record["version"]},
                                  deadline=budget.deadline - (0 if cleanup else budget.cleanup_grace_seconds),
                                  expected_generation=record["generation"]) as current:
            output = owner.backend.run_mc(current, script,
                deadline=budget.deadline - (0 if cleanup else budget.cleanup_grace_seconds), binary=True)
        if not capture:
            sys.stdout.buffer.write(output)
        return subprocess.CompletedProcess(command, 0, output if capture else None, b"")
    if "cp" in arguments:
        snapshot = manifest["runtime"]["machine"] if manifest.get("shared_docker", "catalog" not in manifest["runtime"]) else manifest["runtime"]["catalog"]
        identity = snapshot["containers"]["spark"]
        operands = []
        for value in arguments[arguments.index("cp") + 1:]:
            operands.append(identity + ":" + value[len("spark:"):] if value.startswith("spark:") else value)
        return communicate(["docker", "cp", *operands], timeout=budget.remaining_seconds(work=not cleanup),
            data=data, capture=capture, environment=runtime.controlled_environment())
    if "exec" not in arguments:
        return communicate(command, timeout=budget.remaining_seconds(work=not cleanup), data=data, capture=capture, environment=runtime.controlled_environment())
    index = arguments.index("exec")
    tail = arguments[index + 1:]
    service = 0
    while service < len(tail) and tail[service].startswith("-"):
        service += 2 if tail[service] in ("-e", "--env", "-u", "--user", "-w", "--workdir") else 1
    if service >= len(tail):
        raise runtime.RuntimeFailure("InvalidConfiguration", "compose exec has no service")
    if tail[service] != "spark":
        raise runtime.RuntimeFailure("InvalidConfiguration", "only Spark supports governed compose exec")
    spark_record = manifest["runtime"]["machine"] if manifest.get("shared_docker", "catalog" not in manifest["runtime"]) else manifest["runtime"]["catalog"]
    expected = spark_record["containers"]["spark"]
    observed = communicate(["docker", "compose", *arguments[:index], "ps", "-q", "spark"],
        timeout=budget.remaining_seconds(work=not cleanup), capture=True, environment=runtime.controlled_environment())
    if observed.returncode or observed.stdout.decode().strip() != expected:
        raise runtime.RuntimeFailure("MachineUnavailable", "Spark differs from the exact publication")
    folder = "/tmp/novarocks-fixture-run-" + budget.identity.run_id + "-" + uuid.uuid4().hex
    prefix = ["docker", "exec", "-i", expected]
    # Bind the actual exec to the immutable container ID: a concurrent machine
    # upgrade must never redirect an old caller into a new credential epoch.
    options = [option for option in tail[:service] if option not in ("-T", "--no-TTY")]
    run_exports = ["-e", "NOVA_FIXTURE_RUN_CONTEXT=" + json.dumps(budget.as_dict(), sort_keys=True),
                   "-e", RUN_STORAGE_ENV + "=" + os.environ[RUN_STORAGE_ENV]]
    wrapped = ["docker", "exec", "-i", *run_exports, *options, expected,
        "python3", "-c", CONTAINER_OWNER, folder, str(budget.deadline_unix_ms / 1000),
        str(0 if cleanup else budget.cleanup_grace_seconds), json.dumps(budget.as_dict(),sort_keys=True), *tail[service + 1:]]
    primary, worker_result = None, None
    try:
        worker_result = communicate(wrapped, timeout=budget.remaining_seconds(work=not cleanup), data=data, capture=capture, environment=runtime.controlled_environment())
        return worker_result
    except BaseException as error:
        primary = error
        raise
    finally:
        settled = communicate([*prefix, "python3", "-c", CONTAINER_SETTLE, folder,
            str(budget.deadline_unix_ms / 1000), json.dumps(budget.as_dict(),sort_keys=True)], timeout=max(0.001, budget.remaining_seconds()), capture=True, environment=runtime.controlled_environment())
        if settled.returncode:
            raise runtime.RuntimeFailure("ContainerCleanupFailed", "Spark run did not confirm actual child exit") from primary
        try:
            fact = json.loads(settled.stdout)
        except (ValueError, TypeError) as error:
            raise runtime.RuntimeFailure("ContainerCleanupFailed", "Spark settlement receipt is invalid") from error
        if (not isinstance(fact, dict) or fact.get("child_waited") is not True
                or type(fact.get("returncode")) is not int or fact.get("cleanup_failed")):
            raise runtime.RuntimeFailure("ContainerCleanupFailed", "Spark settlement did not prove actual exit") from primary
        actual_code = fact["returncode"] if fact["returncode"] >= 0 else 128 - fact["returncode"]
        if worker_result is not None and worker_result.returncode != actual_code:
            raise runtime.RuntimeFailure("ContainerCleanupFailed", "Spark native CLI and actual worker exit disagree")
        if not fact.get("never_started") or worker_result is not None:
            removed = communicate([*prefix, "rm", "-rf", folder], timeout=max(0.001, budget.remaining_seconds()), capture=True, environment=runtime.controlled_environment())
            if removed.returncode:
                raise runtime.RuntimeFailure("ContainerCleanupFailed", "Spark temporary directory cleanup exited " + str(removed.returncode)) from primary



def compose_arguments(values):
    return ["--env-file", values["NOVA_ENV_COMPOSE_ENV"], "-p", values["NOVA_ENV_COMPOSE_PROJECT"],
            "-f", values["NOVA_ENV_COMPOSE_FILE"]]


def create_temp(env_file):
    values, _, budget = admit(env_file)
    suffix = budget.identity.run_id + "-" + uuid.uuid4().hex if budget else uuid.uuid4().hex
    folder = "/tmp/novarocks-fixture-data-" + suffix
    marker = dict(schema=1, owner="novarocks-fixture-run-v1", context=budget.as_dict() if budget else None)
    result = compose(env_file, [*compose_arguments(values), "exec", "-T", "spark", "/bin/sh", "-c",
        'umask 077; mkdir "$1" && cat > "$1/.fixture-context.json"', "fixture-temp", folder],
        data=json.dumps(marker,sort_keys=True).encode(), capture=True)
    if result.returncode:
        raise runtime.RuntimeFailure("ContainerCleanupFailed", "Spark temporary directory setup failed")
    return folder


def cleanup_temp(env_file, folder):
    values, _, budget = admit(env_file)
    prefix = "/tmp/novarocks-fixture-data-" + (budget.identity.run_id + "-" if budget else "")
    suffix = folder.removeprefix(prefix)
    if not folder.startswith(prefix) or len(suffix) != 32 or any(c not in "0123456789abcdef" for c in suffix):
        raise runtime.RuntimeFailure("InvalidScratchScope", "temporary directory differs from the exact original run")
    marker = dict(schema=1, owner="novarocks-fixture-run-v1", context=budget.as_dict() if budget else None)
    code = "\n".join([
        "import json,os,pathlib,shutil,sys",
        "folder=pathlib.Path(sys.argv[1]); expected=json.loads(sys.argv[2])",
        "if not folder.exists() and not folder.is_symlink(): sys.exit(0)",
        "marker=folder/'.fixture-context.json'",
        "assert not folder.is_symlink() and folder.is_dir() and folder.stat().st_uid==os.getuid() and folder.stat().st_mode & 0o777==0o700, 'foreign temporary directory'",
        "assert not marker.is_symlink() and json.loads(marker.read_text())==expected, 'temporary directory marker differs from original run'",
        "shutil.rmtree(folder)"])
    result = compose(env_file, [*compose_arguments(values), "exec", "-T", "spark", "python3", "-c", code, folder,
        json.dumps(marker,sort_keys=True)], capture=True)
    if result.returncode:
        raise runtime.RuntimeFailure("ContainerCleanupFailed", "Spark temporary directory removal exited " + str(result.returncode))


def restore_context(path):
    """Resume the original producer context; never mint a replacement deadline."""
    value = json.loads(Path(path).read_text())
    original = value.get("run_context")
    if original is None:
        return False
    current = os.environ.get("NOVA_FIXTURE_RUN_CONTEXT")
    if current not in (None, "null") and json.loads(current) != original:
        raise runtime.RuntimeFailure("RunBudgetRejected", "producer context differs from its persisted original run")
    storage = value.get("run_storage")
    if storage is not None:
        scope = RunStorageScope.from_dict(storage)
        if scope.identity.as_dict() != original["identity"]:
            raise runtime.RuntimeFailure("InvalidScratchScope", "persisted storage differs from the original context")
        current_storage = os.environ.get(RUN_STORAGE_ENV)
        if current_storage not in (None, "null") and json.loads(current_storage) != storage:
            raise runtime.RuntimeFailure("InvalidScratchScope", "producer storage differs from its persisted original run")
        os.environ[RUN_STORAGE_ENV] = json.dumps(storage, sort_keys=True)
    os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(original,sort_keys=True)
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("context", "storage", "compose", "command", "remaining", "temp-dir", "cleanup-dir", "cleanup-run", "sweep-start"))
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--fixture-budget-secs", type=int)
    parser.add_argument("--path")
    args, rest = parser.parse_known_args(argv)
    rest = rest[1:] if rest[:1] == ["--"] else rest
    runtime.install_signal_handlers()
    if args.operation in {"cleanup-dir", "cleanup-run"} and os.environ.get("NOVA_FIXTURE_RUN_CONTEXT") not in (None, "null"):
        os.environ["NOVA_FIXTURE_CLEANUP_MODE"] = "true"
    values, manifest, budget = admit(args.env_file, args.fixture_budget_secs)
    if budget:
        os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(budget.as_dict(), sort_keys=True)
    if args.operation == "cleanup-run":
        report = cleanup_run(args.env_file, budget=budget)
        print(json.dumps(report, sort_keys=True))
        return 0 if report.get("complete") is True else 1
    if args.operation == "sweep-start":
        try:
            report = startup_sweep(args.env_file, budget=budget)
        except runtime.RuntimeFailure as error:
            if hasattr(error, "report"):
                print(json.dumps(error.report, sort_keys=True))
            raise
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.operation == "temp-dir":
        print(create_temp(args.env_file))
        return 0
    if args.operation == "cleanup-dir":
        cleanup_temp(args.env_file, args.path or "")
        return 0
    if args.operation == "remaining":
        print(max(0.001, budget.remaining_seconds(work=True)) if budget else 3600)
        return 0
    if args.operation == "storage":
        print(os.environ[RUN_STORAGE_ENV] if budget else "null")
        return 0
    if args.operation == "context":
        print(json.dumps(budget.as_dict(), sort_keys=True) if budget else "null")
        return 0
    if args.operation == "compose":
        return compose(args.env_file, rest).returncode
    environment = {**os.environ, **values}
    return communicate(rest, timeout=budget.remaining_seconds(work=True) if budget else 3600,
                       environment=environment).returncode


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except runtime.RuntimeFailure as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(75 if error.code in {"RunBudgetRejected", "FixturePrerequisiteMissing", "MachineUnavailable"} else 1)
    except (ValueError, TypeError, KeyError):
        print("RunBudgetRejected: invalid fixture run input", file=sys.stderr)
        raise SystemExit(75)
    except subprocess.TimeoutExpired:
        print("RunBudgetExpired: original fixture deadline expired", file=sys.stderr)
        raise SystemExit(124)
