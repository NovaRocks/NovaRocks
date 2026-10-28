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

"""One explicitly maintained fixture stack per local Docker daemon."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import subprocess
import sys
import time
from typing import Any
import uuid

import fixture_runtime as runtime

PROTOCOL = 1
PROJECT = "nr-fixture-machine"
NETWORK = PROJECT + "_iceberg_net"
DEFAULT_PORTS = dict(minio=28000, minio_console=28001, rest=28002,
                     rest_mv=28003, spark=28004, hms=28005)
DEFAULT_POLICY = dict(scratch_ttl_seconds=604800, max_run_seconds=86400,
                      cleanup_grace_seconds=300)
BLOCKED = {"FixturePrerequisiteMissing", "MachineUnavailable", "MachineVersionMismatch",
           "MachineMaintaining", "RunBudgetRejected"}


class MaintenanceLockTimeout(runtime.RuntimeFailure):
    """The lock deadline elapsed before this caller acquired the machine lock."""

    def __init__(self):
        super().__init__("MachineMaintaining", "machine maintenance is in progress")


class SweepLockTimeout(runtime.RuntimeFailure):
    """A sweep exhausted its lock wait without dispatching any sweep operation."""

    def __init__(self):
        super().__init__("MachineMaintaining", "bounded sweep could not acquire the machine lock")


class MachineCommandDeadlineExpired(runtime.RuntimeFailure):
    """The command deadline elapsed before a journal or CLI was created."""

    def __init__(self, *, readonly):
        super().__init__("RunBudgetExpired", "original machine operation deadline expired")
        self.readonly_not_dispatched = readonly


def is_cloud(record):
    return record.get("config", {}).get("object_store", {}).get("kind") == "aliyun-oss"


def validate_cloud_snapshot(record, *, require_sdk=True):
    """Validate the saved activation, never a mutable installer desired copy."""
    if not is_cloud(record):
        return
    config = machine_config(record["config"])["object_store"]
    expected = dict(provider="aliyun-oss", endpoint=f"https://s3.oss-{config['region']}-internal.aliyuncs.com",
        region=config["region"], bucket=config["bucket"], root_prefix=config["root_prefix"])
    path = Path(record.get("object_store_config_file", ""))
    if (record.get("storage_owner") != expected or not path.is_absolute()
            or path != Path(record["compose_file"]).parent / "object-store.json"):
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "cloud activation storage identity changed")
    try:
        saved = runtime.read_json(path)
    except (OSError, ValueError) as error:
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "cloud activation configuration is unavailable") from error
    if saved != config:
        raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "cloud activation configuration changed")
    python = Path(record.get("oss_ops_python", ""))
    if require_sdk and (not python.is_absolute() or not python.is_file()):
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "verified OSS SDK interpreter is unavailable")


def validate_verified_sdk_location(record, definition):
    """A fresh verified definition cannot silently reuse a different SDK path."""
    if not is_cloud(record) or not ({"config", "oss_ops_python"} & definition.keys()):
        return
    if not definition.get("oss_ops_python"):
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "verified OSS SDK location is missing")
    if record.get("oss_ops_python") != definition["oss_ops_python"]:
        raise runtime.RuntimeFailure("MachineVersionMismatch", "verified OSS SDK location differs; explicit machine maintenance is required")


def endpoints(snapshot: dict[str, Any]) -> dict[str, str]:
    ports = snapshot["ports"]
    result = {
        "minio_endpoint": f"http://127.0.0.1:{ports['minio']}",
        "minio_console": f"http://127.0.0.1:{ports['minio_console']}",
        "rest_uri": f"http://127.0.0.1:{ports['rest']}",
        "spark_ui": f"http://127.0.0.1:{ports['spark']}",
        "container_minio_endpoint": "http://minio:9000",
        "container_rest_uri": "http://rest:8181",
    }
    if is_cloud(snapshot):
        result.pop("minio_endpoint")
        result.pop("minio_console")
        result.pop("container_minio_endpoint")
        result["object_store_endpoint"] = snapshot["storage_owner"]["endpoint"]
    if "rest-mv" in snapshot["images"]:
        result.update(rest_mv_uri=f"http://127.0.0.1:{ports['rest_mv']}",
                      container_rest_mv_uri="http://rest-mv:8181")
    return result


def machine_config(value: dict[str, Any] | None = None) -> dict[str, Any]:
    value = copy.deepcopy(value or {})
    if not isinstance(value, dict):
        raise runtime.RuntimeFailure("InvalidConfiguration", "machine configuration must be an object")
    unknown = set(value) - {"ports", "policy", "benchmark_root", "minio_volume", "object_store"}
    if unknown:
        raise runtime.RuntimeFailure("InvalidConfiguration", "unknown machine configuration fields")
    cloud = None
    if "object_store" in value:
        try:
            from oss_ops import validate_config
            cloud = validate_config(value["object_store"])
        except Exception as error:
            raise runtime.RuntimeFailure("InvalidConfiguration", "invalid cloud object store configuration") from error
        if "minio_volume" in value:
            raise runtime.RuntimeFailure("InvalidConfiguration", "cloud storage cannot select a MinIO volume")
    ports = {**DEFAULT_PORTS, **value.get("ports", {})}
    policy = {**DEFAULT_POLICY, **value.get("policy", {})}
    if set(ports) != set(DEFAULT_PORTS) or any(type(p) is not int or not 0 < p < 65536 for p in ports.values()) or len(set(ports.values())) != len(ports):
        raise runtime.RuntimeFailure("InvalidConfiguration", "machine ports must be distinct valid integers")
    if set(policy) != set(DEFAULT_POLICY) or any(type(n) is not int or n <= 0 for n in policy.values()):
        raise runtime.RuntimeFailure("InvalidConfiguration", "invalid scratch lifetime policy")
    if policy["scratch_ttl_seconds"] <= 2 * (policy["max_run_seconds"] + policy["cleanup_grace_seconds"]):
        raise runtime.RuntimeFailure("InvalidConfiguration", "scratch TTL must exceed twice the complete run budget and cleanup reserve")
    if policy["max_run_seconds"] <= policy["cleanup_grace_seconds"]:
        raise runtime.RuntimeFailure("InvalidConfiguration", "cleanup reserve exhausts the run budget")
    from scratch import canonical_s3, contains
    try:
        benchmark = canonical_s3(value.get("benchmark_root",
            f"s3://{cloud['bucket']}/{cloud['root_prefix']}benchmarks" if cloud else "s3://novarocks/shared/benchmarks"))
        if "/" not in benchmark[5:]:
            raise runtime.RuntimeFailure("InvalidScratchScope", "benchmark root has no prefix")
    except runtime.RuntimeFailure as error:
        raise runtime.RuntimeFailure("InvalidConfiguration", "benchmark root requires a canonical s3://bucket/prefix") from error
    for warehouse in ("s3://warehouse/fixture-machine/rest", "s3://warehouse/fixture-machine/rest-mv"):
        if contains(benchmark, warehouse) or contains(warehouse, benchmark):
            raise runtime.RuntimeFailure("InvalidConfiguration", "benchmark root overlaps a shared REST warehouse")
    if cloud:
        root = f"s3://{cloud['bucket']}/{cloud['root_prefix']}"
        if not contains(root, benchmark):
            raise runtime.RuntimeFailure("InvalidConfiguration", "benchmark root leaves the configured cloud storage root")
        for reserved in ("catalogs", "worktrees", "machines"):
            protected = root + reserved
            if contains(benchmark, protected) or contains(protected, benchmark):
                raise runtime.RuntimeFailure("InvalidConfiguration", "benchmark root overlaps a catalog or worktree storage domain")
        return dict(ports=ports, policy=policy, benchmark_root=benchmark, object_store=cloud)
    volume = value.get("minio_volume", PROJECT + "_minio-data")
    runtime.safe_component(volume)
    return dict(ports=ports, policy=policy, benchmark_root=benchmark, minio_volume=volume)


def input_facts(bom: dict[str, Any], backend: Any, *, cloud=False) -> tuple[dict, dict]:
    """Consume already verified BOM facts; actual IDs never come from a mutable tag."""
    if not bom.get("lock_sha256"):
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "verified input lock identity is missing")
    images, facts = {}, {"lock_sha256": bom["lock_sha256"], "platform": bom["platform"]}
    sources = (
        ("minio", "derived_images", "minio"), ("mc", "derived_images", "minio-mc"),
        ("rest", "images", "iceberg-rest"),
        ("spark", "derived_images", "iceberg-spark"),
        ("rest-mv", "derived_images", "rest-mv"),
    )
    if cloud:
        sources = (("rest", "derived_images", "iceberg-rest-oss"),
                   ("rest-mv", "derived_images", "rest-mv-oss"),
                   ("spark", "derived_images", "iceberg-spark-oss"))
    for service, group, logical in sources:
        receipt = bom.get(group, {}).get(logical)
        if not isinstance(receipt, dict):
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", logical)
        reference = receipt.get("image_id") if group == "derived_images" else receipt.get("alias")
        if not reference:
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", logical)
        actual = backend.image_id(reference)
        if group == "derived_images" and actual != receipt["image_id"]:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "derived image differs from verified receipt")
        # Times and cache paths identify supply operations, not runtime behavior.
        semantic = {k: receipt[k] for k in (
            "manifest_digest", "platform", "definition_sha256", "image_id",
        ) if k in receipt}
        if group == "derived_images" and not semantic.get("definition_sha256"):
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "derived definition identity is missing")
        facts[logical] = {"image_id": actual, "receipt": semantic}
        images[service] = {"image_id": actual, "tag": actual, "receipt": copy.deepcopy(receipt)}
    if cloud:
        sdk = bom.get("python_environments", {}).get("oss-sdk")
        if not isinstance(sdk, dict) or not sdk.get("definition_sha256") or not sdk.get("python_version") or not sdk.get("packages") or not isinstance(sdk.get("files"), dict) or not sdk["files"]:
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "verified OSS SDK environment is missing")
        facts["oss-sdk"] = {k: copy.deepcopy(sdk[k]) for k in ("definition_sha256", "python_version", "packages")}
        facts["oss-sdk"]["files_sha256"] = runtime.digest(sdk["files"])
    else:
        images["mc-init"] = copy.deepcopy(images["mc"])
    return images, facts


def verified_bom(repo: Path, store: Path | None = None, *, consumer="iceberg-rest") -> dict:
    directory = repo / "docker/fixture-inputs"
    sys.path.insert(0, str(directory))
    try:
        spec = importlib.util.spec_from_file_location("machine_fixture_verify", directory / "verify.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        from fixture_inputs import fixture_store
        selected_store = fixture_store(str(store) if store else None).resolve()
        result = module.verify(selected_store, repo, directory / "lock.json", consumer=consumer)
        result["_verified_store_root"] = str(selected_store)
        return result
    except Exception as error:
        raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "local fixture BOM verification failed") from error
    finally:
        sys.path.remove(str(directory))


class MachineDocker(runtime.Docker):
    """Machine operations preserve the shared network and the selected MinIO volume."""

    def __init__(self, timeout=90, environment=None):
        self.require_native_supervision()
        super().__init__(timeout=timeout, environment=environment)

    @staticmethod
    def require_native_supervision():
        # Python 3.10 made macOS monotonic clocks comparable across processes.
        # The supervisor receives an absolute clock and pins an unreaped leader;
        # silently selecting another interpreter would change that contract.
        if sys.version_info < (3, 10):
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "machine supervision requires Python 3.10 or newer")
        if os.name != "posix" or not (sys.platform == "darwin" or sys.platform.startswith("linux")):
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "machine supervision requires native Linux or macOS process groups")
        missing = [name for name in ("waitid", "killpg") if not callable(getattr(os, name, None))]
        missing += [name for name in ("WNOWAIT", "WEXITED", "WNOHANG", "P_PID")
                    if not isinstance(getattr(os, name, None), int)
                    or isinstance(getattr(os, name, None), bool) or getattr(os, name) <= 0]
        if missing:
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "machine supervision requires os." + ", os.".join(missing))
        clock = time.get_clock_info("monotonic")
        if not clock.monotonic or clock.adjustable:
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "machine supervision requires an unadjustable monotonic clock")
        if sys.platform == "darwin":
            import ctypes
            try:
                native = ctypes.CDLL(None)
                native.proc_listpids
                native.proc_pidinfo
            except (AttributeError, OSError) as error:
                raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "machine supervision requires native process group observation") from error
        elif not Path("/proc/self/stat").is_file():
            raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "machine supervision requires native proc process state")

    @staticmethod
    def labels(record):
        if record.get("kind") != "machine":
            return runtime.Docker.labels(record)
        return {"novarocks.fixture.owner": record["namespace"],
                "novarocks.fixture.key": "machine", "novarocks.fixture.kind": "machine",
                "novarocks.fixture.control-root": record["control_root"]}

    def container(self, record, service):
        info = super().container(record, service)
        if info and record.get("kind") == "machine":
            labels = info.get("Config", {}).get("Labels", {})
            if labels.get("novarocks.fixture.version") != record["version"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine container version mismatch")
            if labels.get("novarocks.fixture.activation") != record["generation"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine container activation mismatch")
            expected = record.get("containers", {}).get(service)
            if expected and info["Id"] != expected:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine container was replaced outside maintenance")
        return info

    def validate_resources(self, record, *, require_sdk=True):
        if record.get("kind") != "machine":
            return super().validate_resources(record)
        if is_cloud(record):
            validate_cloud_snapshot(record, require_sdk=require_sdk)
        else:
            self.validate_minio_volume(record)
        for service in record["images"]:
            self.container(record, service)
        self.own_network(record)
        if record.get("network_id"):
            network = self.own_network(record)
            if not network or network["Id"] != record["network_id"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine network was replaced")
        for name in record["volumes"]:
            volume = self.inspect("volume", name, absent_ok=True)
            if volume and name != record["config"].get("minio_volume") and any(
                volume.get("Labels", {}).get(k) != v for k, v in self.labels(record).items()
            ):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "foreign machine state volume")

    def validate_absent(self, record):
        if self.command(["ps", "-a", "--filter", f"label=com.docker.compose.project={PROJECT}", "-q"]):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "fixed machine project already exists without saved authority")
        if self.inspect("network", NETWORK, absent_ok=True):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "fixed machine network already exists without saved authority")
        if self.inspect("volume", PROJECT + "_authority", absent_ok=True):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine authority exists without saved state")
        if is_cloud(record):
            return
        volume = self.inspect("volume", record["config"]["minio_volume"], absent_ok=True)
        if volume and self.command(["ps", "--filter", "volume=" + record["config"]["minio_volume"], "-q"]):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "selected MinIO volume still has an active consumer")
        if volume and record["config"]["minio_volume"] == PROJECT + "_minio-data" and any(
            (volume.get("Labels") or {}).get(k) != v for k, v in self.labels(record).items()
        ):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "default MinIO volume has foreign ownership")

    def ensure_minio_volume(self, record):
        name = record["config"]["minio_volume"]
        if not self.inspect("volume", name, absent_ok=True):
            if name != PROJECT + "_minio-data":
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "selected existing MinIO volume is missing")
            args = ["volume", "create"]
            for key, value in self.labels(record).items():
                args += ["--label", f"{key}={value}"]
            self.command([*args, name])
        return self.minio_volume_identity(record)

    def minio_volume_identity(self, record):
        """Freeze only native stable facts, never usage or mountpoint observations."""
        name = record["config"]["minio_volume"]
        info = self.inspect("volume", name, absent_ok=True)
        if not info:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "preserved MinIO volume is missing")
        if (info.get("Name") != name or any(not isinstance(info.get(key), str) or not info[key]
                for key in ("CreatedAt", "Driver", "Scope")) or any(
                info.get(key) is not None and (not isinstance(info[key], dict) or any(
                    not isinstance(k, str) or not isinstance(v, str) for k, v in info[key].items()))
                for key in ("Options", "Labels"))):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "MinIO volume native identity is incomplete")
        if name == PROJECT + "_minio-data" and any(
                (info.get("Labels") or {}).get(k) != v for k, v in self.labels(record).items()):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "default MinIO volume has foreign ownership")
        return {key: copy.deepcopy(info.get(key)) for key in
                ("Name", "CreatedAt", "Driver", "Scope", "Options", "Labels")}

    def validate_minio_volume(self, record):
        expected = record.get("minio_volume_identity")
        if not expected or self.minio_volume_identity(record) != expected:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "preserved MinIO volume native identity changed")

    def prepare_minio_volume(self, record):
        # No writer is mounted here. The owner durably saves these facts before
        # any Compose activation or root rotation can mount the selected data.
        self.ensure_authority_volume(record)
        if record.get("minio_volume_identity"):
            self.validate_minio_volume(record)
            return record["minio_volume_identity"]
        if record.get("steps", {}).get("activation_started"):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "started activation has no saved volume identity")
        return self.ensure_minio_volume(record)

    def ensure_authority_volume(self, record):
        # Volume creation is daemon-atomic. A different control directory cannot
        # race a second authority between discovery and Compose activation.
        name = PROJECT + "_authority"
        args = ["volume", "create"]
        for key, value in self.labels(record).items():
            args += ["--label", f"{key}={value}"]
        self.command([*args, name])
        marker = self.inspect("volume", name)
        if any(marker.get("Labels", {}).get(k) != v for k, v in self.labels(record).items()):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "another control directory owns this daemon's machine")

    def ensure_machine(self, record, previous=None):
        if is_cloud(record):
            validate_cloud_snapshot(record)
        self.ensure_authority_volume(record)
        self.activation_preflight(record, previous)
        if not is_cloud(record):
            self.validate_minio_volume(record)
        services = ["rest", "rest-mv", "spark"] if is_cloud(record) else ["minio", "mc-init", "rest", "rest-mv", "spark"]
        self.compose(record, ["up", "-d", "--force-recreate", *services])
        self.await_health(record)

    def activation_preflight(self, record, previous):
        """A saved activation intent authorizes its own partially created targets."""
        if not is_cloud(record):
            self.validate_minio_volume(record)
        for service in record["images"]:
            try:
                self.container(record, service)
            except runtime.RuntimeFailure:
                if not previous:
                    raise
                # A partially completed Compose switch may contain either exact
                # source containers or containers from this exact target intent.
                self.container(previous, service)
        network = self.own_network(record)
        expected = (previous or record).get("network_id")
        if expected and (not network or network["Id"] != expected):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "activation's shared network changed")
        for volume in record["volumes"]:
            info = self.inspect("volume", volume, absent_ok=True)
            if info and volume != record["config"].get("minio_volume") and any(
                info.get("Labels", {}).get(k) != v for k, v in self.labels(record).items()
            ):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "activation has a foreign state volume")

    def await_health(self, record):
        # New activation is the only authorized place to acquire fresh exact IDs.
        record["containers"] = {}
        deadline = time.monotonic() + self.timeout
        while not self.healthy(record):
            if time.monotonic() >= deadline:
                raise runtime.RuntimeFailure("ReadinessTimeout", "machine services did not become healthy")
            time.sleep(0.1)
        record["containers"] = {s: self.container(record, s)["Id"] for s in record["required_services"]}
        record["network_id"] = self.own_network(record)["Id"]

    def stop_writers(self, record):
        if is_cloud(record):
            self.validate_resources(record, require_sdk=False)
        else:
            self.validate_resources(record)
        self.compose(record, ["stop", "rest", "rest-mv", "spark"])

    def rotate_root(self, record, previous):
        # The durable fence intent authorizes replacing exactly this MinIO.
        # Repeating the same rotation must also recover a lost completion reply.
        expected = copy.deepcopy(previous)
        expected.get("containers", {}).pop("minio", None)
        self.validate_resources(expected)
        self.compose(record, ["up", "-d", "--force-recreate", "minio"])
        # Only MinIO was restarted. Other exact identities remain unchanged.
        record["containers"] = {k: v for k, v in previous.get("containers", {}).items() if k != "minio"}
        deadline = time.monotonic() + self.timeout
        while not self.http_ready(endpoints(record)["minio_endpoint"] + "/minio/health/live"):
            if time.monotonic() >= deadline:
                raise runtime.RuntimeFailure("ReadinessTimeout", "maintenance MinIO is unavailable")
            time.sleep(0.1)
        record["containers"]["minio"] = self.container(record, "minio")["Id"]

    def assert_old_root_revoked(self, previous):
        # Read through the saved old env file; never put credentials in argv.
        script = ('export MC_HOST_prior="http://$MINIO_ROOT_USER:$MINIO_ROOT_PASSWORD@minio:9000"\n'
                  'set +e\nmc --json stat prior/warehouse 2>&1\nnr_fence_exit=$?\n'
                  'printf \'\\nNOVA_FIXTURE_FENCE_EXIT=%s\\n\' "$nr_fence_exit"\nexit 0')
        response = self.run_mc(previous, script)
        lines = response.splitlines()
        marker = "NOVA_FIXTURE_FENCE_EXIT="
        if not lines or not lines[-1].startswith(marker) or len(response) > 1024 * 1024:
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root probe omitted its native exit")
        status = lines.pop()[len(marker):]
        if not status.isdecimal() or not 0 < int(status) <= 255:
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root rejection was not observed")
        # mc emits a stream of complete error objects, possibly followed by
        # stat's fixed ObjectMissing summary. Every native error must prove an
        # auth denial; the summary alone never proves a credential fence.
        def unique_object(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError("duplicate JSON member")
                value[key] = item
            return value

        def invalid_constant(value):
            raise ValueError("non-finite JSON value")

        try:
            decoder = json.JSONDecoder(object_pairs_hook=unique_object, parse_constant=invalid_constant)
            remaining = "\n".join(lines).lstrip()
            values = []
            while remaining:
                value, end = decoder.raw_decode(remaining)
                values.append(value)
                remaining = remaining[end:].lstrip()
        except (ValueError, TypeError, RecursionError) as error:
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root probe returned invalid JSON") from error
        summary = {"status": "error", "error": {
            "message": "Unable to stat `prior/warehouse`.", "type": "fatal",
            "cause": {"message": "Object does not exist", "error": {}}}}
        if values and values[-1] == summary:
            values.pop()
        if not values:
            raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root rejection was not observed")
        for value in values:
            detail = value.get("error") if isinstance(value, dict) else None
            cause = detail.get("cause") if isinstance(detail, dict) else None
            native = cause.get("error") if isinstance(cause, dict) else None
            code = native.get("Code") if isinstance(native, dict) else None
            code = code or (detail.get("code") if isinstance(detail, dict) else None)
            http_status = native.get("StatusCode") if isinstance(native, dict) else None
            if not isinstance(value, dict) or value.get("status") != "error" or code not in {
                    "InvalidAccessKeyId", "AccessDenied", "SignatureDoesNotMatch", "InvalidToken"} or (
                    http_status is not None and (type(http_status) is not int or http_status not in {401, 403})):
                raise runtime.RuntimeFailure("CredentialFenceUnconfirmed", "old root rejection was not observed")

    def run_oss(self, record, args, *, deadline=None):
        validate_cloud_snapshot(record)
        from machine_operation import run_owned_host_sdk_operation
        return run_owned_host_sdk_operation(self, record, args, deadline=deadline)

    def run_mc(self, record, script, *, deadline=None, binary=False):
        """Register, execute and actually settle one exact transient container."""
        if is_cloud(record):
            raise runtime.RuntimeFailure("CloudIamUnsupported", "OSS requires explicit SDK operations; MinIO mc is unavailable")
        limits = [time.monotonic() + self.timeout]
        limits += [limit for limit in (deadline, getattr(self, "operation_deadline", None)) if limit is not None]
        deadline = min(limits)
        timeout = deadline - time.monotonic()
        if timeout <= 0:
            raise runtime.RuntimeFailure("OperationDeadlineExceeded", "mc deadline elapsed before dispatch")
        name = PROJECT + "-mc-" + uuid.uuid4().hex
        journal = Path(record["compose_file"]).parent / "operations" / (name + ".json")
        fact = dict(schema=1, container_name=name, phase="intent", generation=record["generation"],
                    image_id=record["images"]["mc"]["image_id"],
                    labels={**self.labels(record), "novarocks.fixture.version": record["version"],
                            "novarocks.fixture.activation": record["generation"]})
        runtime.atomic_json(journal, fact)
        lock_fd = getattr(self, "maintenance_fd", None)
        if lock_fd is None:
            raise runtime.RuntimeFailure("MaintenanceLockRequired", "mc requires the machine maintenance lock")
        env_file = journal.with_suffix(".env")
        credentials = record["credentials"]
        runtime.atomic_bytes(env_file, ("MINIO_ROOT_USER=" + credentials["access_key"] + "\n" +
                                       "MINIO_ROOT_PASSWORD=" + credentials["secret_key"] + "\n").encode())
        result = self._supervise(journal, dict(environment=self.environment, network=record["network"],
            env_file=str(env_file), script=script, deadline_monotonic=deadline), timeout)
        output = journal.with_suffix(".stdout")
        if not result.get("command_succeeded"):
            deadline_exits = {None, 0, 124, -signal.SIGTERM, -signal.SIGKILL,
                              128 + signal.SIGTERM, 128 + signal.SIGKILL}
            clean_exit = (result.get("phase") == "settled" and result.get("container_absent") is True
                and result.get("container_name") == name and not result.get("interrupted")
                and not any(result.get(key) for key in ("error", "leader_observation_error", "group_observation_error"))
                and all(result.get(key) in deadline_exits for key in ("create_exit", "command_exit"))
                and (result.get("create_dispatched") is False or (
                    isinstance(result.get("container_id"), str) and bool(result["container_id"])
                    and all(result.get(key) is True for key in
                            ("group_exit_confirmed", "group_exit_observed", "leader_waited")))))
            if result.get("timed_out") is True and clean_exit:
                error = runtime.RuntimeFailure("DockerOperationTimeout", "mc deadline expired after confirmed cleanup: " + str(journal))
                # Keep any native MC failure available to its protocol owner.
                # Public error text and sweep reports never include this body.
                if not output.exists() and result.get("start_dispatched") is True:
                    raise runtime.RuntimeFailure("DockerOperationFailed", "mc timeout output is missing: " + str(journal))
                try:
                    error.stdout = (output.read_bytes() if binary else output.read_text()) if output.exists() else None
                except (OSError, UnicodeError) as read_error:
                    raise runtime.RuntimeFailure("DockerOperationFailed", "mc timeout output is unreadable: " + str(journal)) from read_error
                raise error
            raise runtime.RuntimeFailure("DockerOperationFailed", "mc command did not finish successfully: " + str(journal))
        return output.read_bytes() if binary else output.read_text()

    def _supervise(self, journal, request, timeout):
        lock_fd = getattr(self, "maintenance_fd", None)
        if lock_fd is None:
            raise runtime.RuntimeFailure("MaintenanceLockRequired", "machine command requires its maintenance lock")
        runtime.atomic_json(journal.with_suffix(".request"), request)
        fact = runtime.read_json(journal)
        read_fd, write_fd = os.pipe()
        monitor = None
        primary = None
        try:
            monitor = subprocess.Popen([
                sys.executable, str(Path(__file__).with_name("machine_operation.py")), str(journal),
                "--control-fd", str(read_fd), "--lock-fd", str(lock_fd), "--timeout", str(timeout),
            ], pass_fds=(read_fd, lock_fd), start_new_session=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=self.environment)
        except BaseException:
            os.close(write_fd)
            fact.update(phase="settled", command_succeeded=False, create_dispatched=False)
            runtime.atomic_json(journal, fact)
            raise
        finally:
            os.close(read_fd)
        try:
            monitor.wait()
        except BaseException as error:
            primary = error
        finally:
            os.close(write_fd)
            # The monitor owns actual Docker exit and cleanup. Releasing our
            # shared flock before its exit would reopen the upgrade race.
            while monitor.poll() is None:
                try:
                    monitor.wait()
                except KeyboardInterrupt:
                    if primary is None:
                        primary = KeyboardInterrupt()
        if primary is not None:
            raise primary
        result = runtime.read_json(journal)
        if result.get("phase") != "settled":
            code = "MachineOperationUnconfirmed" if request.get("operation") in {"control", "host-sdk"} else "ContainerCleanupUnconfirmed"
            raise runtime.RuntimeFailure(code, str(journal))
        return result

    # Design: ADR-0199 (docs/adr/ADR-0199-readonly-observation-and-mutation-settlement.md)
    @staticmethod
    def readonly_command(args):
        # Closed Docker read verbs used by this owner. A word named inspect in
        # an arbitrary command is not evidence that the command is readonly.
        if not isinstance(args, (list, tuple)) or not args or not all(isinstance(a, str) for a in args):
            return False
        return (args[0] in {"ps", "info", "inspect", "version"}
                or tuple(args[:2]) == ("context", "show")
                or (len(args) >= 2 and args[0] in {"container", "image", "network", "volume", "context"}
                    and args[1] == "inspect"))

    def command(self, args, *, absent_ok=False, catalog_removal=None):
        readonly = self.readonly_command(args)
        deadline = min(time.monotonic() + self.timeout,
                       getattr(self, "operation_deadline", None) or float("inf"))
        timeout = deadline - time.monotonic()
        if timeout <= 0:
            raise MachineCommandDeadlineExpired(readonly=readonly)
        if getattr(self, "maintenance_fd", None) is None:
            original_timeout = self.timeout
            try:
                self.timeout = timeout
                return super().command(args, absent_ok=absent_ok)
            finally:
                self.timeout = original_timeout
        base = Path(self.maintenance_base)
        journal = base / "operations" / ("control-" + uuid.uuid4().hex + ".json")
        fact = dict(schema=1, kind="control", phase="intent", mutating=not readonly,
                    control_root=str(base.parent), machine=base.name)
        state_path = base / "state.json"
        if not readonly and state_path.exists():
            state = runtime.read_json(state_path)
            if state.get("phase") == "maintenance":
                fact.update(maintenance_generation=state["generation"], maintenance_version=state["version"])
        if catalog_removal is not None:
            if readonly or not fact.get("maintenance_generation"):
                raise runtime.RuntimeFailure("MaintenanceLockRequired", "catalog removal requires a saved maintenance intent")
            fact["catalog_removal"] = copy.deepcopy(catalog_removal)
        runtime.atomic_json(journal, fact)
        result = self._supervise(journal, dict(operation="control", environment=self.environment,
            args=args, deadline_monotonic=deadline), timeout)
        if result.get("timed_out"):
            error = runtime.RuntimeFailure("DockerOperationTimeout", args[0])
            # Only the sweep owner may project this private completion evidence
            # into a bounded observation timeout. Ordinary commands still fail.
            error.readonly_deadline_settled = self._readonly_deadline_settled(
                args, fact, result, journal)
            raise error
        output = journal.with_suffix(".stdout")
        stderr = Path(str(output) + ".stderr")
        return self.command_result(args, result.get("command_exit", 1), output.read_text() if output.exists() else "",
            stderr.read_text() if stderr.exists() else "", absent_ok=absent_ok)

    @staticmethod
    def _readonly_deadline_settled(args, original, result, journal):
        if (not MachineDocker.readonly_command(args)
                or original["mutating"] is not False or result.get("mutating") is not False
                or type(result.get("schema")) is not int
                or any(result.get(key) != original[key] for key in
                       ("schema", "kind", "control_root", "machine"))
                or result.get("phase") != "settled" or result.get("timed_out") is not True
                or result.get("interrupted") is not False
                or result.get("command_succeeded") is not False
                or any(key in result for key in ("error", "cleanup_error", "cleanup_failed",
                                                "leader_observation_error", "group_observation_error"))):
            return False
        dispatched, code = result.get("command_dispatched"), result.get("command_exit")
        if type(code) is not int:
            return False
        output = journal.with_suffix(".stdout")
        stderr = Path(str(output) + ".stderr")
        try:
            if dispatched is False:
                return (code == 124 and not any(key in result for key in
                        ("cli_pid", "group_exit_confirmed", "group_exit_observed", "leader_waited"))
                        and not any(path.is_symlink() or (path.exists() and path.stat().st_size)
                                    for path in (output, stderr)))
            if (dispatched is not True or type(result.get("cli_pid")) is not int or result["cli_pid"] <= 0
                    or not all(result.get(key) is True for key in
                               ("group_exit_confirmed", "group_exit_observed", "leader_waited"))
                    or code not in {0, 124, -signal.SIGTERM, -signal.SIGKILL,
                                    128 + signal.SIGTERM, 128 + signal.SIGKILL}
                    or not output.is_file() or output.is_symlink()
                    or not stderr.is_file() or stderr.is_symlink() or stderr.stat().st_size):
                return False
            # A killed read can have a truncated body. A native successful read
            # must still satisfy its known output protocol before deferral.
            if code != 0:
                return True
            text = output.read_text().strip()
            if args[-2:] == ["--format", "{{json .}}"]:
                return isinstance(json.loads(text), dict)
            if args[:2] == ["context", "inspect"]:
                value = json.loads(text)
                return isinstance(value, str) and bool(value)
            if args[0] == "ps":
                return all(re.fullmatch(r"[0-9a-f]{12,64}", line) for line in text.splitlines())
            if args[0] == "info" or args[:2] == ["context", "show"]:
                return re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", text) is not None
        except (OSError, UnicodeError, ValueError):
            return False
        return False

    def unsettled_operations(self, base, *, include_disposed=False):
        pending = []
        paths = [*Path(base).glob("activations/**/operations/*.json"), *Path(base).glob("operations/*.json")]
        for path in sorted(paths):
            if path.is_symlink():
                raise runtime.RuntimeFailure("RuntimeStateUnreadable", "operation journal is not a regular owned file")
            fact = runtime.read_json(path)
            if fact.get("schema") != 1 or fact.get("phase") not in {"intent", "creating", "create-unknown", "created", "running", "settled", "cleanup-failed", "command-unknown"}:
                raise runtime.RuntimeFailure("RuntimeStateUnreadable", "invalid operation journal")
            if fact["phase"] != "settled":
                if not include_disposed and self.failed_readonly_disposition(path, fact, base):
                    continue
                pending.append((path, fact))
        return pending

    def _failed_readonly_control_material(self, path, fact, base):
        # This classification proves absence of a daemon mutation obligation,
        # never CLI success, local process exit, or remote writer quiescence.
        base, path = Path(base), Path(path)
        if (path.parent != base / "operations" or not re.fullmatch(r"control-[0-9a-f]{32}\.json", path.name)
                or fact.get("schema") != 1 or fact.get("kind") != "control"
                or fact.get("phase") not in {"cleanup-failed", "command-unknown"}
                or fact.get("control_root") != str(base.parent) or fact.get("machine") != base.name
                or fact.get("mutating") is not False):
            return None
        try:
            paths = (path, path.with_suffix(".request"), base / "state.json")
            material = {name: self._terminal_material(name) for name in paths}
            original, request, state = (json.loads(material[name][0]) for name in paths)
            if original != fact or not isinstance(request, dict) or not isinstance(state, dict):
                return None
            arguments = request.get("args")
            if (request.get("operation") != "control" or not self.readonly_command(arguments)
                    or set(request) != {"operation", "args", "environment", "deadline_monotonic"}
                    or not isinstance(request.get("environment"), dict)
                    or not all(isinstance(k, str) and isinstance(v, str) for k, v in request["environment"].items())
                    or type(request.get("deadline_monotonic")) not in (int, float)
                    or not math.isfinite(request["deadline_monotonic"])
                    or state.get("schema") != PROTOCOL or state.get("kind") != "machine"
                    or state.get("id") != base.name or state.get("control_root") != str(base.parent)
                    or state.get("project") != PROJECT or not isinstance(state.get("daemon_id"), str)
                    or base.name != "machine-" + runtime.digest(state["daemon_id"])[:16]):
                return None
            original = {k: v for k, v in fact.items() if k != "observation_disposition"}
            expected = dict(schema=1, kind="failed-readonly-control",
                            mutation_obligation="none",
                            original_failure_sha256=runtime.digest(original),
                            request_sha256=hashlib.sha256(material[paths[1]][0]).hexdigest(),
                            machine=base.name, daemon_id=state["daemon_id"])
            return expected, material
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def failed_readonly_disposition(self, path, fact, base):
        if "observation_disposition" not in fact:
            return False
        verified = self._failed_readonly_control_material(path, fact, base)
        return verified is not None and fact["observation_disposition"] == verified[0]

    def reconcile_failed_readonly_control(self, path, fact, base):
        # Only an explicitly invoked owner settlement may author this disposition.
        if (getattr(self, "maintenance_fd", None) is None
                or getattr(self, "maintenance_base", None) != Path(base)
                or "observation_disposition" in fact):
            return False
        verified = self._failed_readonly_control_material(path, fact, base)
        if verified is None:
            return False
        disposition, material = verified
        try:
            if any(self._terminal_material(name) != observed for name, observed in material.items()):
                return False
        except (OSError, ValueError):
            return False
        fact = dict(fact, observation_disposition=disposition)
        runtime.atomic_json(path, fact)
        return True

    def observation_status(self, path, fact, base):
        return dict(journal=str(path), phase=fact["phase"], error=fact.get("error"),
                    observation_disposition=fact.get("observation_disposition"),
                    original_completion={k: fact[k] for k in ("command_dispatched", "cli_pid", "command_exit",
                        "timed_out", "command_succeeded", "leader_waited", "group_exit_confirmed",
                        "group_exit_observed") if k in fact},
                    disposition_valid=self.failed_readonly_disposition(path, fact, base))

    def failed_observation_status(self, base):
        return [self.observation_status(path, fact, base)
                for path, fact in self.unsettled_operations(base, include_disposed=True)
                if fact.get("kind") == "control" and fact.get("phase") in {"cleanup-failed", "command-unknown"}]

    def reconcile_terminal_port_failure(self, path, fact, base):
        # A native daemon bind rejection is a terminal RPC result, whereas
        # transport loss or a timeout leaves a dispatched mutation unknown.
        # Reconcile only the same persisted maintenance intent, and recheck its
        # exact source/target resources before permitting that intent to retry.
        if (fact.get("phase") != "command-unknown" or fact.get("command_exit") != 1
                or fact.get("timed_out") is not False or fact.get("group_exit_observed") is not True
                or not fact.get("maintenance_generation")):
            return False
        request = runtime.read_json(path.with_suffix(".request"))
        arguments = request.get("args", [])
        if not arguments or arguments[0] != "compose" or "up" not in arguments:
            return False
        stderr = Path(str(path.with_suffix(".stdout")) + ".stderr")
        if not stderr.is_file() or stderr.stat().st_size > 1024 * 1024:
            return False
        if not re.search(r"(?m)^Error response from daemon: [^\n]*Bind for (?:[0-9.]+|\[[0-9a-fA-F:]+\]):[0-9]+ failed: port is already allocated\r?$", stderr.read_text()):
            return False
        state = runtime.read_json(Path(base) / "state.json")
        if (state.get("phase") != "maintenance"
                or state.get("generation") != fact["maintenance_generation"]
                or state.get("version") != fact.get("maintenance_version")):
            return False
        if state.get("fenced_store") and not state.get("steps", {}).get("root_rotated"):
            target = copy.deepcopy(state["fenced_store"])
            target.get("containers", {}).pop("minio", None)
            self.activation_preflight(target, state.get("previous"))
        else:
            self.activation_preflight(state, state.get("fenced_store") or state.get("previous"))
        fact.update(phase="settled", command_succeeded=False, terminal_failure="daemon-port-rejected",
                    reconciled_same_maintenance_intent=True)
        runtime.atomic_json(path, fact)
        return True

    @staticmethod
    def _terminal_material(path):
        """Read a bounded, owned regular file without following a replacement."""
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            before = os.fstat(descriptor)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                    or before.st_mode & 0o022 or before.st_size > 1024 * 1024):
                raise ValueError("untrusted terminal material")
            content = bytearray()
            while len(content) <= 1024 * 1024:
                block = os.read(descriptor, min(65536, 1024 * 1024 + 1 - len(content)))
                if not block:
                    break
                content.extend(block)
            after, named = os.fstat(descriptor), os.stat(path, follow_symlinks=False)
            identity = lambda value: (value.st_dev, value.st_ino, value.st_size,
                                      value.st_mtime_ns, value.st_ctime_ns)
            if (len(content) != before.st_size or identity(before) != identity(after)
                    or identity(before) != identity(named)):
                raise ValueError("terminal material changed")
            return bytes(content), identity(before)
        finally:
            os.close(descriptor)

    def reconcile_terminal_catalog_refusal(self, path, fact, base):
        # A complete refusal settles this one request as FAIL. It grants no
        # catalog barrier, deletion, replay of an unknown call, or ready state.
        base, path = Path(base), Path(path)
        if (getattr(self, "maintenance_fd", None) is None
                or Path(getattr(self, "maintenance_base", "")) != base
                or base.is_symlink() or (base / "operations").is_symlink()
                or path.parent != base / "operations"
                or type(fact.get("schema")) is not int or fact["schema"] != 1
                or fact.get("kind") != "control" or fact.get("phase") != "command-unknown"
                or fact.get("mutating") is not True
                or type(fact.get("command_exit")) is not int or fact["command_exit"] != 1
                or fact.get("timed_out") is not False or fact.get("interrupted") is not False
                or fact.get("command_succeeded") is not False
                or not all(isinstance(fact.get(key), str) and fact[key] for key in
                           ("maintenance_generation", "maintenance_version"))
                or type(fact.get("cli_pid")) is not int or fact["cli_pid"] <= 0
                or not all(fact.get(key) is True for key in ("command_dispatched", "leader_waited",
                    "group_exit_confirmed", "group_exit_observed"))
                or any(key in fact for key in ("error", "cleanup_error", "cleanup_failed",
                    "leader_observation_error", "group_observation_error"))
                or fact.get("control_root") != str(base.parent) or fact.get("machine") != base.name):
            return False
        files = {"fact": path, "request": path.with_suffix(".request"), "state": base / "state.json",
                 "stdout": path.with_suffix(".stdout"),
                 "stderr": Path(str(path.with_suffix(".stdout")) + ".stderr")}
        try:
            materials = {key: self._terminal_material(value) for key, value in files.items()}
            original = json.loads(materials["fact"][0])
            request, state = (json.loads(materials[key][0]) for key in ("request", "state"))
            purpose = fact.get("catalog_removal", {})
            old = state.get("fenced_store", {})
            steps = state.get("steps", {})
            name = purpose.get("volume", {}).get("Name")
            if (original != fact or request.get("operation") != "control"
                    or request.get("environment") != self.environment
                    or request.get("args") != ["volume", "rm", name]
                    or name not in {PROJECT + "_rest-catalog", PROJECT + "_rest-mv-catalog"}
                    or state.get("phase") != "maintenance"
                    or state.get("generation") != fact.get("maintenance_generation")
                    or state.get("version") != fact.get("maintenance_version")
                    or type(state.get("schema")) is not int or state["schema"] != PROTOCOL
                    or not state.get("previous") or not old
                    or not all(steps.get(key) is True for key in ("writers_stopped", "root_rotated", "root_fenced"))
                    or any(steps.get(key) for key in ("rest_removed", "purged", "activation_started"))
                    or purpose.get("source_generation") != old.get("generation")
                    or purpose.get("source_version") != old.get("version")
                    or old.get("kind") != "machine" or old.get("namespace") != base.name
                    or old.get("control_root") != str(base.parent)
                    or state.get("daemon_id") != old.get("daemon_id")
                    or not old.get("daemon_id") or materials["stdout"][0] != b""):
                return False
            refusal = re.fullmatch(r"Error response from daemon: remove " + re.escape(name)
                + r": volume is in use - \[([0-9a-f]{64}(?:, [0-9a-f]{64})*)\](?:\r?\n)?",
                materials["stderr"][0].decode("utf-8"))
            if refusal is None:
                return False
            # Recheck the exact partially completed source activation. Absence
            # alone never settles the original request, and replacement fails.
            if self.daemon_id() != old["daemon_id"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "catalog refusal belongs to another daemon")
            self.validate_resources(old)
            volume = self.inspect("volume", name, absent_ok=True)
            if not volume or self._catalog_volume_identity(volume) != purpose["volume"]:
                return False
            if any(volume.get("Labels", {}).get(key) != value for key, value in self.labels(old).items()):
                return False
            if any(self._terminal_material(files[key]) != value for key, value in materials.items()):
                return False
        except (OSError, ValueError, UnicodeError, KeyError, TypeError, AttributeError):
            return False
        fact.update(phase="settled", command_succeeded=False,
                    terminal_failure="daemon-catalog-volume-in-use",
                    reconciled_same_maintenance_intent=True,
                    terminal_evidence={key: hashlib.sha256(value[0]).hexdigest()
                                       for key, value in materials.items()})
        runtime.atomic_json(path, fact)
        return True

    @staticmethod
    def _catalog_volume_identity(volume):
        return {key: volume[key] for key in ("Name", "Driver", "Scope", "Options", "Labels", "CreatedAt")}

    def settle_operations(self, base):
        for path, fact in self.unsettled_operations(base):
            if fact.get("kind") == "host-sdk":
                if fact.get("control_root") != str(Path(base).parent) or fact.get("machine") != Path(base).name:
                    raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "host SDK operation belongs to another machine")
                if fact.get("phase") == "intent" and fact.get("command_dispatched") is not True:
                    fact.update(phase="settled", command_succeeded=False, interrupted=True)
                    runtime.atomic_json(path, fact)
                    continue
                raise runtime.RuntimeFailure("MachineOperationUnconfirmed", "host SDK exit remains unconfirmed: " + str(path))
            if fact.get("kind") == "control":
                if fact.get("control_root") != str(Path(base).parent) or fact.get("machine") != Path(base).name:
                    raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "control operation belongs to another machine")
                if not fact.get("command_dispatched"):
                    fact.update(phase="settled", command_succeeded=False, interrupted=True)
                    runtime.atomic_json(path, fact)
                    continue
                if self.reconcile_terminal_port_failure(path, fact, base):
                    continue
                if self.reconcile_terminal_catalog_refusal(path, fact, base):
                    continue
                # No CLI completion fact survives a killed supervisor. A
                # timeout is equally insufficient to authorize a later ready.
                raise runtime.RuntimeFailure("MachineOperationUnconfirmed", "unsettled machine command: " + str(path))
            name, labels = fact.get("container_name", ""), fact.get("labels", {})
            if (not name.startswith(PROJECT + "-mc-") or len(name) != len(PROJECT + "-mc-") + 32
                    or labels.get("novarocks.fixture.control-root") != str(Path(base).parent)
                    or not fact.get("image_id")):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "operation has no exact machine ownership")
            reply_id, known_id = fact.get("create_reply_id"), fact.get("container_id")
            if reply_id is not None and (not isinstance(reply_id, str) or not re.fullmatch(r"[0-9a-f]{64}", reply_id)
                                          or fact.get("create_exit") != 0 or fact.get("create_dispatched") is not True
                                          or (known_id is not None and known_id != reply_id)):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "operation reply identity differs from its journal")
            expected_id = known_id or reply_id
            container = self.inspect("container", expected_id or name, absent_ok=True)
            if container:
                if ((expected_id and container.get("Id") != expected_id)
                        or container.get("Image") != fact["image_id"] or any(
                    container.get("Config", {}).get("Labels", {}).get(k) != v for k, v in labels.items()
                )):
                    raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "orphan operation container differs from its journal")
                identity = container["Id"]
                fact["container_id"] = identity
                runtime.atomic_json(path, fact)
                self.command(["container", "rm", "--force", identity])
                if self.inspect("container", identity, absent_ok=True):
                    raise runtime.RuntimeFailure("ResourcesStillPresent", "orphan operation did not exit")
            elif fact.get("create_dispatched") and not expected_id:
                raise runtime.RuntimeFailure("ContainerCleanupUnconfirmed", "unknown create outcome: " + str(path))
            if expected_id:
                fact["container_id"] = expected_id
            fact.update(phase="settled", interrupted=True, container_absent=True, command_succeeded=False)
            runtime.atomic_json(path, fact)

    def remove_rest_state(self, record):
        # Revalidate the exact saved activation on every retry. Compose service
        # selectors would also select an externally replaced container.
        containers = [info["Id"] for service in ("rest", "rest-mv")
                      if (info := self.container(record, service))]
        if containers:
            # Writers must already have exited; do not force-remove a running
            # process if it was restarted outside this maintenance intent.
            self.command(["rm", *containers])
        for service in ("rest", "rest-mv"):
            name = PROJECT + "_" + service + "-catalog"
            volume = self.inspect("volume", name, absent_ok=True)
            if volume:
                if any(volume.get("Labels", {}).get(k) != v for k, v in self.labels(record).items()):
                    raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "foreign REST state volume")
                self.command(["volume", "rm", name], catalog_removal=dict(
                    source_generation=record["generation"], source_version=record["version"],
                    volume=self._catalog_volume_identity(volume)))
        for service in ("rest", "rest-mv"):
            record.get("containers", {}).pop(service, None)
        return self.rest_state_barrier(record)

    def rest_state_barrier(self, record):
        """Observe exact old catalog absence before granting offline purge."""
        volumes = [PROJECT + "_" + service + "-catalog" for service in ("rest", "rest-mv")]
        for service, name in zip(("rest", "rest-mv"), volumes):
            if self.container(record, service) or self.inspect("volume", name, absent_ok=True):
                raise runtime.RuntimeFailure("CatalogRemovalUnconfirmed", "old REST catalog state is still present")
        return dict(schema=1, daemon_id=record["daemon_id"], generation=record["generation"],
                    volumes=volumes, complete=True)


class MachineOwner:
    def __init__(self, root=None, *, daemon=None, backend=None, template=None,
                 timeout=90, clock=time.time, sweeper_factory=None):
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise runtime.RuntimeFailure("InvalidConfiguration", "machine timeout must be finite and positive")
        state = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state")))
        self.root = Path(root or state / "novarocks/fixture-machine").resolve()
        self.backend = backend if backend is not None else MachineDocker(timeout=timeout)
        self._daemon = daemon
        self.timeout, self.clock = timeout, clock
        self.template = Path(template or runtime.SCRIPT_DIR / "templates/machine.yml")
        self.custom_template = template is not None
        self.sweeper_factory = sweeper_factory

    @property
    def daemon_id(self):
        if self._daemon is None:
            self._daemon = self.backend.daemon_id()
        return runtime.safe_component(self._daemon)

    @property
    def machine_id(self):
        return "machine-" + runtime.digest(self.daemon_id)[:16]

    @property
    def base(self):
        return self.root / self.machine_id

    @property
    def state_dir(self):
        return self.base

    def assert_live_daemon(self):
        if self.backend.daemon_id() != self.daemon_id:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine belongs to another Docker daemon")

    @contextmanager
    def maintenance_lock(self, deadline=None, *, readonly_existing=False):
        deadline = time.monotonic() + self.timeout if deadline is None else deadline
        if not readonly_existing:
            self.base.mkdir(parents=True, exist_ok=True)
            self.base.chmod(0o700)
        try:
            lock = (self.base / "maintenance.lock").open("rb" if readonly_existing else "a+b")
        except OSError as error:
            if readonly_existing:
                raise runtime.RuntimeFailure("MachineUnavailable", "existing machine maintenance lock is unavailable") from error
            raise
        with lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise MaintenanceLockTimeout()
                    time.sleep(min(0.02, max(0, deadline - time.monotonic())))
            previous_fd = getattr(self.backend, "maintenance_fd", None)
            previous_base = getattr(self.backend, "maintenance_base", None)
            if not readonly_existing:
                self.backend.maintenance_fd = lock.fileno()
                self.backend.maintenance_base = self.base
            try:
                yield
            finally:
                if not readonly_existing:
                    self.backend.maintenance_fd = previous_fd
                    self.backend.maintenance_base = previous_base
                fcntl.flock(lock, fcntl.LOCK_UN)

    def read_saved_state(self):
        path = self.base / "state.json"
        if not path.exists():
            return None
        value = runtime.read_json(path)
        if (value.get("schema") != PROTOCOL or value.get("daemon_id") != self.daemon_id
                or value.get("control_root") != str(self.root) or value.get("phase") not in {"maintenance", "ready"}):
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "invalid machine authority state")
        return value

    def resolve_readonly_observation(self, journal):
        """Dispose one failed read's mutation obligation, never its exit proof."""
        deadline = time.monotonic() + self.timeout
        previous = getattr(self.backend, "operation_deadline", None)
        deadline = min(deadline, previous) if previous is not None else deadline
        self.backend.operation_deadline = deadline
        try:
            with self.maintenance_lock(deadline):
                self.assert_live_daemon()
                if self.read_saved_state() is None:
                    raise runtime.RuntimeFailure("MachineUnavailable", "machine authority state is absent")
                journal = Path(journal)
                if not journal.is_absolute() or journal.parent != self.base / "operations":
                    raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "observation belongs to another machine")
                try:
                    fact = json.loads(self.backend._terminal_material(journal)[0])
                    if not isinstance(fact, dict):
                        raise ValueError("invalid control journal")
                except (OSError, ValueError, TypeError) as error:
                    raise runtime.RuntimeFailure("RuntimeStateUnreadable", "invalid observation journal") from error
                if not self.backend.failed_readonly_disposition(journal, fact, self.base):
                    if not self.backend.reconcile_failed_readonly_control(journal, fact, self.base):
                        raise runtime.RuntimeFailure("MachineOperationUnconfirmed", "observation has no readonly disposition")
                    fact = json.loads(self.backend._terminal_material(journal)[0])
                return self.backend.observation_status(journal, fact, self.base)
        finally:
            self.backend.operation_deadline = previous

    def load_config(self):
        state = self.read_saved_state()
        if state:
            return machine_config(state["config"])
        path = self.base / "config.json"
        return machine_config(runtime.read_json(path) if path.exists() else None)

    def required_definition(self, config, verified_bom):
        config = machine_config(config)
        cloud = bool(config.get("object_store"))
        images, facts = input_facts(verified_bom, self.backend, cloud=cloud)
        template = runtime.SCRIPT_DIR / "templates/machine-oss.yml" if cloud and not self.custom_template else self.template
        text = template.read_text()
        version = runtime.digest(dict(protocol=PROTOCOL, inputs=facts,
                                      template_sha256=runtime.digest(text), config=config))
        result = dict(version=version, config=config, images=images, template=text,
                      producer_receipt={"lock_sha256": verified_bom["lock_sha256"],
                                        **copy.deepcopy(verified_bom["derived_images"]["iceberg-spark-oss" if cloud else "iceberg-spark"])})
        if cloud:
            receipt = verified_bom["python_environments"]["oss-sdk"]
            relative = Path(receipt.get("path", ""))
            root = verified_bom.get("_verified_store_root")
            if not root or not Path(root).is_absolute() or relative.is_absolute() or not relative.parts or ".." in relative.parts:
                raise runtime.RuntimeFailure("FixturePrerequisiteMissing", "verified OSS SDK location is missing")
            # Keep the venv symlink name: resolving bin/python would lose its site packages.
            result["oss_ops_python"] = str(Path(root).resolve() / relative / "bin/python")
        return result

    @contextmanager
    def ready_snapshot(self, definition, deadline=None, *, expected_generation=None, lock_deadline=None,
                       readonly_existing=False):
        if lock_deadline is not None and (type(lock_deadline) not in (int, float) or not math.isfinite(lock_deadline)):
            raise runtime.RuntimeFailure("InvalidConfiguration", "machine lock deadline must be finite")
        previous = getattr(self.backend, "operation_deadline", None)
        effective = deadline if previous is None else previous if deadline is None else min(previous, deadline)
        lock_limit = effective
        if lock_deadline is not None:
            lock_limit = lock_deadline if effective is None else min(effective, lock_deadline)
        self.backend.operation_deadline = effective
        def check_deadline():
            if effective is not None and time.monotonic() >= effective:
                raise runtime.RuntimeFailure("RunBudgetExpired", "original fixture operation deadline expired")
        try:
            check_deadline()
            self.assert_live_daemon()
            with self.maintenance_lock(lock_limit, **({"readonly_existing": True} if readonly_existing else {})):
                state = self.read_saved_state()
                if not state or state["phase"] != "ready":
                    raise runtime.RuntimeFailure("MachineUnavailable", "start or finish machine maintenance explicitly")
                if definition["version"] != state["version"]:
                    raise runtime.RuntimeFailure("MachineVersionMismatch", "update the worktree or explicitly upgrade the machine")
                if expected_generation is not None and state["generation"] != expected_generation:
                    raise runtime.RuntimeFailure("MachineUnavailable", "fixture activation changed")
                if self.backend.unsettled_operations(self.base):
                    raise runtime.RuntimeFailure("MachineUnavailable", "finish unsettled machine operations explicitly")
                if is_cloud(state):
                    validate_verified_sdk_location(state, definition)
                    validate_cloud_snapshot(state)
                else:
                    self.backend.validate_minio_volume(state)
                healthy = self.backend.healthy(state)
                check_deadline()
                if not healthy:
                    raise runtime.RuntimeFailure("MachineUnavailable", "machine services are not healthy")
                yield copy.deepcopy(state)
        finally:
            self.backend.operation_deadline = previous

    def check_ready(self, verified_bom, config=None):
        """Observe the original exact readiness contract without publishing state."""
        definition = self.required_definition(self.load_config() if config is None else config, verified_bom)
        with self.ready_snapshot(definition, readonly_existing=True) as state:
            return {**state, "observed_healthy": True}

    def _save(self, record):
        runtime.atomic_json(self.base / "state.json", record)

    def _save_scratch_observation(self, state, report):
        # Volatile observations must not change stable publication identity.
        keys = ("observed_counts", "oldest_created_unix_ms", "sampled_at_unix_ms", "counts_are_pre_cleanup",
                "unknown_counts", "enumeration_complete", "enumeration_scope", "classification_basis")
        if "sampled_at_unix_ms" in report:
            runtime.atomic_json(self.base / "scratch-observation.json", dict(schema=1, machine_id=state["id"],
                version=state["version"], generation=state["generation"], **{k: report[k] for k in keys if k in report}))

    def scratch_observation(self, state):
        path = self.base / "scratch-observation.json"
        if not path.exists():
            return {"status": "UNKNOWN", "reason": "not observed"}
        sample = runtime.read_json(path)
        if sample.get("schema") != 1 or sample.get("machine_id") != state["id"]:
            raise runtime.RuntimeFailure("RuntimeStateUnreadable", "scratch observation identity differs")
        return {"status": "OBSERVED", "sample_is_current_activation": all(sample.get(k) == state[k]
                for k in ("version", "generation")), **sample}

    def _record(self, definition, credentials=None):
        generation = uuid.uuid4().hex
        directory = self.base / "activations" / generation
        if definition["config"].get("object_store"):
            return self._cloud_record(definition, generation, directory)
        credentials = credentials or dict(access_key="nr" + secrets.token_hex(9),
                                           secret_key=secrets.token_urlsafe(32), generation=uuid.uuid4().hex)
        images = copy.deepcopy(definition["images"])
        config = copy.deepcopy(definition["config"])
        record = dict(schema=PROTOCOL, protocol=PROTOCOL, kind="machine", key="machine",
                      id=self.machine_id, daemon_id=self.daemon_id, control_root=str(self.root),
                      namespace=self.machine_id, phase="maintenance", version=definition["version"],
                      generation=generation, created_at=self.clock(), config=config,
                      credentials=credentials, project=PROJECT, network=NETWORK,
                      images=images, ports=config["ports"], policy=config["policy"], containers={},
                      compose_file=str(directory / "compose.yml"), compose_env=str(directory / "compose.env"),
                      template=definition["template"], producer_receipt=definition["producer_receipt"],
                      protected_benchmark_roots=[config["benchmark_root"]],
                      volumes=[config["minio_volume"], PROJECT + "_rest-catalog", PROJECT + "_rest-mv-catalog"],
                      required_services=["minio", "mc-init", "rest", "rest-mv", "spark"],
                      server_warehouse="s3://warehouse/fixture-machine/rest",
                      server_warehouses={"rest": "s3://warehouse/fixture-machine/rest", "rest-mv": "s3://warehouse/fixture-machine/rest-mv"},
                      steps={})
        record["service_ports"] = {"minio": {"9000/tcp": config["ports"]["minio"], "9001/tcp": config["ports"]["minio_console"]},
                                   "rest": {"8181/tcp": config["ports"]["rest"]},
                                   "rest-mv": {"8181/tcp": config["ports"]["rest_mv"]},
                                   "spark": {"4040/tcp": config["ports"]["spark"]}}
        record["health_urls"] = [endpoints(record)["minio_endpoint"] + "/minio/health/live",
                                 endpoints(record)["rest_uri"] + "/v1/config", endpoints(record)["rest_mv_uri"] + "/v1/config"]
        return record

    def _cloud_record(self, definition, generation, directory):
        config = copy.deepcopy(definition["config"])
        cloud = config["object_store"]
        root = f"s3://{cloud['bucket']}/{cloud['root_prefix']}machines/{self.machine_id}/{generation}"
        record = dict(schema=PROTOCOL, protocol=PROTOCOL, kind="machine", key="machine",
            id=self.machine_id, daemon_id=self.daemon_id, control_root=str(self.root), namespace=self.machine_id,
            phase="maintenance", version=definition["version"], generation=generation, created_at=self.clock(),
            config=config, project=PROJECT, network=NETWORK, images=copy.deepcopy(definition["images"]),
            ports=config["ports"], policy=config["policy"], containers={}, steps={},
            compose_file=str(directory / "compose.yml"), compose_env=str(directory / "compose.env"),
            object_store_config_file=str(directory / "object-store.json"), oss_ops_python=definition["oss_ops_python"],
            storage_owner=dict(provider="aliyun-oss", endpoint=f"https://s3.oss-{cloud['region']}-internal.aliyuncs.com",
                region=cloud["region"], bucket=cloud["bucket"], root_prefix=cloud["root_prefix"]),
            template=definition["template"], producer_receipt=definition["producer_receipt"],
            protected_benchmark_roots=[config["benchmark_root"]],
            volumes=[PROJECT + "_rest-catalog", PROJECT + "_rest-mv-catalog"],
            required_services=["rest", "rest-mv", "spark"], server_warehouse=root + "/rest",
            server_warehouses={"rest": root + "/rest", "rest-mv": root + "/rest-mv"})
        record["service_ports"] = {service: {port: config["ports"][name]} for service, port, name in
            (("rest", "8181/tcp", "rest"), ("rest-mv", "8181/tcp", "rest_mv"), ("spark", "4040/tcp", "spark"))}
        record["health_urls"] = [endpoints(record)[name] + "/v1/config" for name in ("rest_uri", "rest_mv_uri")]
        return record

    def _write_definition(self, record):
        if is_cloud(record):
            return self._write_cloud_definition(record)
        credentials = record["credentials"]
        values = dict(NOVA_FIXTURE_OWNER=record["namespace"], NOVA_FIXTURE_CONTROL_ROOT=str(self.root),
                      NOVA_FIXTURE_VERSION=record["version"], NOVA_FIXTURE_PROJECT=PROJECT,
                      NOVA_FIXTURE_GENERATION=record["generation"],
                      MINIO_ROOT_USER=credentials["access_key"], MINIO_ROOT_PASSWORD=credentials["secret_key"],
                      NOVA_FIXTURE_MINIO_VOLUME=record["config"]["minio_volume"])
        values.update({s.upper().replace("-", "_") + "_IMAGE": img["image_id"] for s, img in record["images"].items()})
        values.update({"NOVA_ENV_" + n.upper() + "_PORT": v for n, v in record["ports"].items()})
        values.update(NOVA_ENV_REST_SERVER_WAREHOUSE_URI=record["server_warehouses"]["rest"],
                      NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI=record["server_warehouses"]["rest-mv"])
        text = "".join(f"{key}='{str(v).replace(chr(39), chr(92)+chr(39))}'\n" for key, v in sorted(values.items()))
        for name, data in (("compose_file", record["template"]), ("compose_env", text)):
            path = Path(record[name])
            if path.exists() and path.read_text() != data:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "activation definition changed")
            if not path.exists():
                runtime.atomic_bytes(path, data.encode())

    def _write_cloud_definition(self, record):
        cloud = record["config"]["object_store"]
        values = dict(NOVA_FIXTURE_OWNER=record["namespace"], NOVA_FIXTURE_CONTROL_ROOT=str(self.root),
            NOVA_FIXTURE_VERSION=record["version"], NOVA_FIXTURE_GENERATION=record["generation"],
            NOVA_OSS_ROLE_NAME=cloud["role_name"], NOVA_OSS_REGION=cloud["region"],
            NOVA_OSS_S3_ENDPOINT=record["storage_owner"]["endpoint"],
            NOVA_ENV_REST_SERVER_WAREHOUSE_URI=record["server_warehouses"]["rest"],
            NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI=record["server_warehouses"]["rest-mv"])
        values.update({s.upper().replace("-", "_") + "_IMAGE": img["image_id"] for s, img in record["images"].items()})
        values.update({"NOVA_ENV_" + n.upper() + "_PORT": v for n, v in record["ports"].items() if n in {"rest", "rest_mv", "spark"}})
        text = "".join(f"{key}='{str(value).replace(chr(39), chr(92)+chr(39))}'\n" for key, value in sorted(values.items()))
        config_text = json.dumps(cloud, sort_keys=True, indent=2) + "\n"
        for name, data in (("compose_file", record["template"]), ("compose_env", text), ("object_store_config_file", config_text)):
            path = Path(record[name])
            if path.exists() and path.read_text() != data:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "activation definition changed")
            if not path.exists():
                runtime.atomic_bytes(path, data.encode())

    def _sweeper(self, record):
        if self.sweeper_factory:
            return self.sweeper_factory(record, self.backend)
        from scratch import ScratchSweeper
        return ScratchSweeper(record, self.backend)

    def _activate_cloud(self, definition, *, upgrade=False):
        self.assert_live_daemon()
        with self.maintenance_lock():
            self.backend.settle_operations(self.base)
            old = self.read_saved_state()
            if old and not is_cloud(old):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine storage provider cannot change in place")
            if old and old["phase"] == "ready" and old["version"] == definition["version"] and not upgrade:
                validate_verified_sdk_location(old, definition)
                validate_cloud_snapshot(old)
                if self.backend.healthy(old):
                    return old
                record = self._record(definition)
                record["previous"] = old
                record["recover_only"] = True
                # Recovery reuses the original catalog volume and its exact warehouse.
                record["server_warehouse"] = old["server_warehouse"]
                record["server_warehouses"] = copy.deepcopy(old["server_warehouses"])
            elif old and old["phase"] == "maintenance":
                if old["version"] != definition["version"]:
                    raise runtime.RuntimeFailure("MachineMaintaining", "finish the saved maintenance definition first")
                validate_verified_sdk_location(old, definition)
                record = old
            else:
                if old and not upgrade:
                    raise runtime.RuntimeFailure("MachineVersionMismatch", "explicit upgrade is required")
                record = self._record(definition)
                if old:
                    record["previous"] = old
                else:
                    self.backend.validate_absent(record)
            if old:
                record["protected_benchmark_roots"] = sorted(set(record["protected_benchmark_roots"] + old.get("protected_benchmark_roots", [])))
            self._save(record)
            self._write_definition(record)
            previous = record.get("previous")
            if previous and not record.get("recover_only"):
                steps = record["steps"]
                if not steps.get("writers_stopped"):
                    self.backend.stop_writers(previous)
                    steps["writers_stopped"] = True
                    self._save(record)
                if not steps.get("rest_removed"):
                    record["offline_catalog_barrier"] = self.backend.remove_rest_state(previous)
                    steps["rest_removed"] = True
                    self._save(record)
                # No IAM mutation, unknown-writer TTL cleanup or old-object deletion.
                # A new catalog receives a distinct warehouse; original objects remain.
                if not steps.get("activation_started"):
                    record["offline_catalog_barrier"] = self.backend.rest_state_barrier(previous)
                    self._save(record)
            record["steps"]["activation_started"] = True
            self._save(record)
            self.backend.ensure_machine(record, previous)
            if not self.backend.healthy(record):
                raise runtime.RuntimeFailure("ReadinessTimeout", "activation health was not confirmed")
            record["phase"] = "ready"
            record.pop("previous", None)
            record.pop("recover_only", None)
            self._save(record)
            return record

    def activate(self, definition, *, upgrade=False):
        if definition.get("config", {}).get("object_store"):
            return self._activate_cloud(definition, upgrade=upgrade)
        self.assert_live_daemon()
        with self.maintenance_lock():
            self.backend.settle_operations(self.base)
            old = self.read_saved_state()
            if old and is_cloud(old):
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "machine storage provider cannot change in place")
            if old and old["config"]["minio_volume"] != definition["config"]["minio_volume"]:
                raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "upgrade cannot replace the preserved MinIO data volume")
            if old and (old.get("minio_volume_identity") or old["phase"] == "ready"):
                self.backend.validate_minio_volume(old)
            if old and old["phase"] == "ready" and old["version"] == definition["version"] and not upgrade:
                if self.backend.healthy(old):
                    return old
                record = self._record(definition, credentials=old["credentials"])
                record["previous"] = old
                record["recover_only"] = True
            elif old and old["phase"] == "maintenance":
                if old["version"] != definition["version"]:
                    raise runtime.RuntimeFailure("MachineMaintaining", "finish the saved maintenance definition first")
                record = old
            else:
                if old and not upgrade:
                    raise runtime.RuntimeFailure("MachineVersionMismatch", "explicit upgrade is required")
                record = self._record(definition)
                if old:
                    record["previous"] = old
                else:
                    self.backend.validate_absent(record)
            if old:
                if old.get("minio_volume_identity"):
                    record["minio_volume_identity"] = copy.deepcopy(old["minio_volume_identity"])
                record["protected_benchmark_roots"] = sorted(set(
                    record.get("protected_benchmark_roots", []) + old.get("protected_benchmark_roots", [])
                    + [old["config"]["benchmark_root"], definition["config"]["benchmark_root"]]))
            self._save(record)
            self._write_definition(record)
            record["minio_volume_identity"] = copy.deepcopy(self.backend.prepare_minio_volume(record))
            self._save(record)
            previous = record.get("previous")
            if previous and not record.get("recover_only"):
                steps = record["steps"]
                if not steps.get("writers_stopped"):
                    self.backend.stop_writers(previous)
                    steps["writers_stopped"] = True
                    self._save(record)
                if "fenced_store" not in record:
                    fence = copy.deepcopy(previous)
                    fence["credentials"] = record["credentials"]
                    fence["protected_benchmark_roots"] = record["protected_benchmark_roots"]
                    directory = Path(record["compose_file"]).parent / "fence"
                    fence.update(compose_file=str(directory / "compose.yml"), compose_env=str(directory / "compose.env"))
                    record["fenced_store"] = fence
                    self._save(record)
                fence = record["fenced_store"]
                self._write_definition(fence)
                if not steps.get("root_rotated"):
                    self.backend.rotate_root(fence, previous)
                    steps["root_rotated"] = True
                    self._save(record)
                if not steps.get("root_fenced"):
                    self.backend.assert_old_root_revoked(previous)
                    steps["root_fenced"] = True
                    self._save(record)
                if not steps.get("rest_removed"):
                    barrier = self.backend.remove_rest_state(fence)
                    fence["offline_catalog_barrier"] = barrier
                    steps["rest_removed"] = True
                    self._save(record)
                if not steps.get("purged"):
                    # A durable observation does not replace a fresh native
                    # check before retrying purge. Once purge is committed,
                    # target activation may already have created new catalogs.
                    fence["offline_catalog_barrier"] = self.backend.rest_state_barrier(fence)
                    self._save(record)
                    sweeper = self._sweeper(fence)
                    sweeper.revoke_users()
                    report = sweeper.sweep(expired_only=False, deadline=time.monotonic() + self.timeout)
                    self._save_scratch_observation(fence, report)
                    if not report.get("complete"):
                        raise runtime.RuntimeFailure("ScratchCleanupIncomplete", "machine remains in maintenance")
                    steps["purged"] = True
                    self._save(record)
                previous = fence
            record["steps"]["activation_started"] = True
            self._save(record)
            self.backend.ensure_machine(record, previous)
            if not self.backend.healthy(record):
                raise runtime.RuntimeFailure("ReadinessTimeout", "activation health was not confirmed")
            record["phase"] = "ready"
            record.pop("previous", None)
            record.pop("fenced_store", None)
            record.pop("recover_only", None)
            self._save(record)
            return record

    def sweep(self, *, deadline=None, expected_version=None, expected_generation=None):
        deadline = time.monotonic() + min(5, self.timeout) if deadline is None else deadline
        previous_deadline = getattr(self.backend, "operation_deadline", None)
        deadline = min(deadline, previous_deadline) if previous_deadline is not None else deadline
        self.backend.operation_deadline = deadline
        observation_only = True
        lock_acquired = False
        try:
            with self.maintenance_lock(deadline):
                lock_acquired = True
                self.assert_live_daemon()
                state = self.read_saved_state()
                if not state or state["phase"] != "ready":
                    raise runtime.RuntimeFailure("MachineUnavailable", "machine is not available for sweep")
                if expected_version is not None and state["version"] != expected_version:
                    raise runtime.RuntimeFailure("MachineVersionMismatch", "sweep publication version differs")
                if expected_generation is not None and state["generation"] != expected_generation:
                    raise runtime.RuntimeFailure("MachineUnavailable", "sweep publication activation changed")
                if is_cloud(state):
                    validate_cloud_snapshot(state)
                else:
                    self.backend.validate_minio_volume(state)
                observation_only = False
                self.backend.settle_operations(self.base)
                observation_only = True
                if not self.backend.healthy(state):
                    raise runtime.RuntimeFailure("MachineUnavailable", "machine services are not healthy")
                if is_cloud(state):
                    return {"complete": True, "status": "NOT_RUN", "scope": "cloud-ttl", "cleanup_performed": False,
                            "errors": [], "removed": [],
                            "disposition": "retained-no-writer-exit-proof",
                            "reason": "cloud TTL cleanup requires original writer exit evidence"}
                observation_only = False
                report = self._sweeper(state).sweep(expired_only=True, deadline=deadline)
                self._save_scratch_observation(state, report)
                return report
        except MaintenanceLockTimeout as error:
            if not lock_acquired:
                raise SweepLockTimeout() from error
            raise
        except runtime.RuntimeFailure as error:
            readonly_complete = (
                error.code == "DockerOperationTimeout"
                and getattr(error, "readonly_deadline_settled", False) is True
            ) or (isinstance(error, MachineCommandDeadlineExpired)
                  and error.readonly_not_dispatched is True)
            if (observation_only and readonly_complete
                    and not self.backend.unsettled_operations(self.base)):
                raise runtime.RuntimeFailure("ScratchSweepTimeout",
                    "bounded sweep observation deadline elapsed after confirmed readonly completion") from error
            raise
        finally:
            self.backend.operation_deadline = previous_deadline

    def attach_consumer(self, container, alias, expected_generation):
        self.assert_live_daemon()
        with self.maintenance_lock():
            state = self.read_saved_state()
            if not state or state["phase"] != "ready" or state["generation"] != expected_generation:
                raise runtime.RuntimeFailure("MachineUnavailable", "consumer fixture activation is unavailable")
            self.backend.validate_resources(state)
            self.backend.connect(state, container, alias)

    def detach_consumer(self, container, expected_machine_identity):
        self.assert_live_daemon()
        if expected_machine_identity != self.machine_id:
            raise runtime.RuntimeFailure("RuntimeIdentityMismatch", "consumer belongs to another machine")
        with self.maintenance_lock():
            state = self.read_saved_state()
            if state:
                self.backend.own_network(state)
                self.backend.disconnect(state, container)


def public_snapshot(value):
    """CLI status is safe to retain; publications use private exact snapshots."""
    if isinstance(value, dict):
        return {k: public_snapshot(v) for k, v in value.items()
                if k not in {"credentials", "previous", "fenced_store", "template", "minio_volume_identity"}}
    if isinstance(value, list):
        return [public_snapshot(v) for v in value]
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--timeout", type=float, default=90)
    commands = parser.add_subparsers(dest="operation", required=True)
    for operation in ("start", "upgrade", "check"):
        sub = commands.add_parser(operation)
        sub.add_argument("--config", type=Path)
        sub.add_argument("--store", type=Path)
    commands.add_parser("status")
    commands.add_parser("sweep")
    resolve = commands.add_parser("resolve-readonly-observation")
    resolve.add_argument("--journal", type=Path, required=True)
    args = parser.parse_args(argv)
    runtime.install_signal_handlers()
    try:
        owner = MachineOwner(args.root, timeout=args.timeout)
        if args.operation in {"start", "upgrade", "check"}:
            config = machine_config(runtime.read_json(args.config)) if args.config else owner.load_config()
            bom = verified_bom(runtime.SCRIPT_DIR.parent.parent, args.store,
                consumer="iceberg-rest-oss" if config.get("object_store") else "iceberg-rest")
            if args.operation == "check":
                result = owner.check_ready(bom, config)
            else:
                result = owner.activate(owner.required_definition(config, bom), upgrade=args.operation == "upgrade")
        elif args.operation == "resolve-readonly-observation":
            result = owner.resolve_readonly_observation(args.journal)
        elif args.operation == "sweep":
            result = owner.sweep()
        else:
            result = owner.read_saved_state()
            if result:
                result["observed_healthy"] = owner.backend.healthy(result) if result["phase"] == "ready" else False
                result["scratch_observation"] = owner.scratch_observation(result)
                result["failed_observations"] = owner.backend.failed_observation_status(owner.base)
        print(json.dumps(public_snapshot(result), sort_keys=True))
        return 0
    except runtime.RuntimeFailure as error:
        print(str(error), file=sys.stderr)
        return 75 if error.code in BLOCKED else 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
