#!/usr/bin/env python3
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

"""Run-scoped fixture identities, cumulative budgets and conservative collection.

No liveness registry or worktree lease is involved. Callers own the machine
lock and the actual writer processes; these helpers grant neither authority.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import http.client
import json
import math
from pathlib import Path
import re
import secrets
import shlex
import socket
import time
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import fixture_runtime as runtime

RUN_RE = re.compile(r"nr_scratch_([0-9]{13})_([0-9a-f]{32})\Z")
RESOURCE_RE = re.compile(r"(nr_scratch_[0-9]{13}_[0-9a-f]{32})(?:_([a-z][a-z0-9]{0,11}))?\Z")
IAM_RE = re.compile(r"nr([0-9a-f]{8})([0-9a-f]{10})\Z")
PURPOSE_RE = re.compile(r"[a-z][a-z0-9]{0,11}\Z")
MISSING_CODES = {"NoSuchKey", "NoSuchBucket", "NoSuchUser", "NoSuchPolicy",
                 "XMinioAdminNoSuchUser", "XMinioAdminNoSuchPolicy"}
MAX_REST_BYTES = 8 * 1024 * 1024
MAX_MC_BYTES = 16 * 1024 * 1024
MAX_PREFIX_VISITS = 2048
MAX_PREFIX_DEPTH = 32
MAX_PENDING_PREFIXES = 10000
RUN_STORAGE_ENV = "NOVA_FIXTURE_RUN_STORAGE"


class _DeadlineSocket(socket.socket):
    """SocketIO calls recv_into for every read, including header parsing.

    Reapplying the original deadline prevents continuous slow progress from
    resetting the budget. The connection owner closes this exact socket.
    """
    def __init__(self, original, deadline, monotonic):
        super().__init__(original.family, original.type, original.proto, fileno=original.detach())
        self.deadline, self.monotonic = deadline, monotonic

    def recv_into(self, *args, **kwargs):
        self._remaining_timeout()
        return super().recv_into(*args, **kwargs)

    def sendall(self, *args, **kwargs):
        self._remaining_timeout()
        return super().sendall(*args, **kwargs)

    def _remaining_timeout(self):
        remaining = self.deadline - self.monotonic()
        if remaining <= 0:
            raise TimeoutError("original REST deadline expired")
        self.settimeout(remaining)


class _DeadlineConnection(http.client.HTTPConnection):
    def __init__(self, port, deadline, monotonic):
        self.deadline, self.monotonic = deadline, monotonic
        super().__init__("127.0.0.1", port, timeout=max(0.001, deadline - monotonic()))

    def connect(self):
        super().connect()
        self.sock = _DeadlineSocket(self.sock, self.deadline, self.monotonic)


def s3_parts(uri: str) -> tuple[str, str]:
    if not isinstance(uri, str):
        raise runtime.RuntimeFailure("InvalidScratchScope", "S3 scope must be a string")
    # urlsplit strips leading C0/space and removes TAB/CR/LF anywhere. Reject
    # those original bytes before parsing instead of authorizing another key.
    if any(ord(c) <= 32 for c in uri):
        raise runtime.RuntimeFailure("InvalidScratchScope", "invalid S3 control or space character")
    parsed = urlsplit(uri)
    parts = parsed.path.strip("/").split("/") if parsed.path.strip("/") else []
    if (parsed.scheme != "s3" or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", parsed.netloc)
            or parsed.query or parsed.fragment or any(c in uri for c in ("%", "*", "?", "#")) or "//" in parsed.path
            or any(p in {"", ".", ".."} or any(ord(c) <= 32 for c in p) for p in parts)):
        raise runtime.RuntimeFailure("InvalidScratchScope", "invalid S3 scope")
    return parsed.netloc, "/".join(parts)


def canonical_s3(uri: str) -> str:
    bucket, key = s3_parts(uri)
    return "s3://" + bucket + ("/" + key if key else "")


def contains(root: str, value: str) -> bool:
    root, value = canonical_s3(root), canonical_s3(value)
    return value == root or value.startswith(root + "/")


def resource_run(name: str) -> str | None:
    match = RESOURCE_RE.fullmatch(name) if isinstance(name, str) and len(name) <= 63 else None
    return match[1] if match else None


def namespace_run(name: str) -> str | None:
    """Only a complete, bounded run-plus-purpose name owns a namespace."""
    match = RESOURCE_RE.fullmatch(name) if isinstance(name, str) and len(name) <= 63 else None
    return match[1] if match and match[2] else None


def _policy_shape(document):
    """Compare native IAM set representations without changing their grants.

    MinIO may marshal a singleton set as a string and reorder string sets or
    statements. Sid carries no authority. Every other field remains part of
    the comparison; unknown policy/statement fields are never discarded.
    """
    def strings(value):
        values = [value] if isinstance(value, str) else value
        if not isinstance(values, list) or not values or not all(isinstance(v, str) and v for v in values):
            raise runtime.RuntimeFailure("InvalidScratchScope", "invalid native IAM string set")
        return tuple(sorted(set(values)))

    if (not isinstance(document, dict) or set(document) != {"Version", "Statement"}
            or document["Version"] != "2012-10-17"
            or not isinstance(document["Statement"], list) or not document["Statement"]):
        raise runtime.RuntimeFailure("InvalidScratchScope", "invalid native IAM policy shape")
    statements = []
    for statement in document["Statement"]:
        if (not isinstance(statement, dict) or statement.get("Effect") != "Allow"
                or set(statement) - {"Effect", "Action", "Resource", "Condition", "Sid"}
                or ("Sid" in statement and not isinstance(statement["Sid"], str))):
            raise runtime.RuntimeFailure("InvalidScratchScope", "unsupported native IAM statement")
        condition = statement.get("Condition", {})
        if not isinstance(condition, dict):
            raise runtime.RuntimeFailure("InvalidScratchScope", "invalid native IAM condition")
        predicates = []
        for operator, values in condition.items():
            if not isinstance(operator, str) or not isinstance(values, dict) or not values:
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid native IAM condition predicates")
            if not all(isinstance(key, str) and key for key in values):
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid native IAM condition key")
            predicates.append((operator, tuple(sorted((key, strings(value)) for key, value in values.items()))))
        statements.append(("Allow", strings(statement.get("Action")), strings(statement.get("Resource")),
                           tuple(sorted(predicates))))
    return tuple(sorted(statements))


def created_ms(run_id: str) -> int:
    match = RUN_RE.fullmatch(run_id) if isinstance(run_id, str) else None
    if not match:
        raise runtime.RuntimeFailure("InvalidScratchIdentity", "invalid scratch run identity")
    return int(match[1])


def parse_iam_name(name: str) -> int | None:
    """Return the embedded creation time; this is a reserved scratch grammar."""
    match = IAM_RE.fullmatch(name) if isinstance(name, str) else None
    return int(match[1], 16) * 1000 if match else None


@dataclass(frozen=True)
class ScratchIdentity:
    run_id: str
    scope_root: str

    def __post_init__(self):
        created_ms(self.run_id)
        root = canonical_s3(self.scope_root)
        if not root.endswith("/scratch/" + self.run_id):
            raise runtime.RuntimeFailure("InvalidScratchScope", "scope must end in scratch/run_id")
        object.__setattr__(self, "scope_root", root)

    @classmethod
    def create(cls, worktree_root: str, *, now_ms: int | None = None,
               token: str | None = None) -> ScratchIdentity:
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        if type(now_ms) is not int or not 10**12 <= now_ms < 10**13:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "scratch creation time must have 13 digits")
        token = secrets.token_hex(16) if token is None else token
        run_id = f"nr_scratch_{now_ms:013d}_{token}"
        return cls(run_id, canonical_s3(worktree_root) + "/scratch/" + run_id)

    @classmethod
    def from_dict(cls, value: dict) -> ScratchIdentity:
        if not isinstance(value, dict) or value.get("schema") != 1:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "invalid scratch identity schema")
        result = cls(value.get("run_id"), value.get("scope_root"))
        if value.get("created_unix_ms") != created_ms(result.run_id):
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "scratch timestamp contradicts its name")
        return result

    def resource_name(self, purpose: str | None = None) -> str:
        if purpose is None:
            return self.run_id
        if not PURPOSE_RE.fullmatch(purpose):
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "invalid scratch purpose")
        name = self.run_id + "_" + purpose
        if len(name) > 63:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "scratch resource name exceeds 63 characters")
        return name

    def iam_name(self, purpose: str) -> str:
        if not PURPOSE_RE.fullmatch(purpose):
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "invalid scratch purpose")
        epoch = created_ms(self.run_id) // 1000
        if epoch > 0xffffffff:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "IAM timestamp is out of range")
        suffix = hashlib.sha256((self.run_id + ":" + purpose).encode()).hexdigest()[:10]
        return f"nr{epoch:08x}{suffix}"

    def as_dict(self) -> dict:
        return dict(schema=1, run_id=self.run_id, created_unix_ms=created_ms(self.run_id),
                    scope_root=self.scope_root)


def cloud_storage_owner(record):
    """Derive a closed non-secret owner from the frozen cloud definition."""
    config = record.get("config", {}).get("object_store")
    if config is None:
        return None
    try:
        from oss_ops import OpsError, validate_config
        canonical = validate_config(config)
    except (OpsError, ValueError, TypeError, AttributeError) as error:
        raise runtime.RuntimeFailure("InvalidScratchScope", "invalid cloud storage definition") from error
    if canonical != config:
        raise runtime.RuntimeFailure("InvalidScratchScope", "cloud storage definition is not canonical")
    return dict(provider="aliyun-oss", endpoint="https://s3.oss-" + config["region"] + "-internal.aliyuncs.com",
                region=config["region"], bucket=config["bucket"], root_prefix=config["root_prefix"])


@dataclass(frozen=True)
class RunStorageScope:
    """A fixed storage projection, independently validated against its machine.

    The two warehouse ranges are lexical prefixes of one direct namespace
    component. Their IAM wildcards do not replace namespace grammar checks.
    """
    identity: ScratchIdentity
    machine: dict
    storage_owner: dict
    ranges: dict

    @classmethod
    def from_record(cls, identity, record):
        warehouses = record.get("server_warehouses", {})
        cloud_owner = cloud_storage_owner(record)
        if cloud_owner is not None and record.get("storage_owner") != cloud_owner:
            raise runtime.RuntimeFailure("InvalidScratchScope", "cloud record differs from its closed storage owner")
        value = dict(schema=2 if cloud_owner is not None else 1, identity=identity.as_dict(),
                     machine={k: record.get(k) for k in ("id", "control_root", "daemon_id", "version", "generation")},
                     storage_owner=cloud_owner or dict(daemon_id=record.get("daemon_id"),
                                        minio_volume=record.get("config", {}).get("minio_volume")),
                     ranges=dict(run_root=identity.scope_root + "/", **{
                         name: dict(warehouse=canonical_s3(warehouses.get(service)) + "/",
                                    namespace_prefix=identity.run_id + "_")
                         for name, service in (("rest", "rest"), ("rest_mv", "rest-mv"))}))
        result = cls.from_dict(value)
        protected = record.get("protected_benchmark_roots", []) + [record.get("config", {}).get(
            "benchmark_root", "s3://novarocks/shared/benchmarks")]
        roots = [result.identity.scope_root] + [result.ranges[k]["warehouse"] for k in ("rest", "rest_mv")]
        if any(contains(root, p) or contains(p, root) for root in roots for p in protected):
            raise runtime.RuntimeFailure("InvalidScratchScope", "storage scope overlaps protected data")
        return result

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict) or set(value) != {"schema", "identity", "machine", "storage_owner", "ranges"} or type(value.get("schema")) is not int or value.get("schema") not in (1, 2):
            raise runtime.RuntimeFailure("InvalidScratchScope", "invalid run storage schema")
        if value["schema"] == 2 and (not isinstance(value["identity"], dict)
                or set(value["identity"]) != {"schema", "run_id", "created_unix_ms", "scope_root"}
                or type(value["identity"].get("schema")) is not int):
            raise runtime.RuntimeFailure("InvalidScratchScope", "cloud scratch identity is not closed")
        identity = ScratchIdentity.from_dict(value["identity"])
        machine, owner, ranges = value["machine"], value["storage_owner"], value["ranges"]
        if (not isinstance(machine, dict) or set(machine) != {"id", "control_root", "daemon_id", "version", "generation"}
                or not all(isinstance(v, str) and v for v in machine.values())
                or not isinstance(owner, dict)
                or not isinstance(ranges, dict) or set(ranges) != {"run_root", "rest", "rest_mv"}
                or ranges["run_root"] != identity.scope_root + "/"):
            raise runtime.RuntimeFailure("InvalidScratchScope", "invalid run storage owner or ranges")
        if value["schema"] == 1:
            if (set(owner) != {"daemon_id", "minio_volume"}
                    or not all(isinstance(v, str) and v for v in owner.values())
                    or owner["daemon_id"] != machine["daemon_id"]):
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid MinIO storage owner")
        else:
            if (set(owner) != {"provider", "endpoint", "region", "bucket", "root_prefix"}
                    or owner.get("provider") != "aliyun-oss"
                    or not all(isinstance(v, str) and v for v in owner.values())
                    or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", owner["region"])
                    or owner["endpoint"] != "https://s3.oss-" + owner["region"] + "-internal.aliyuncs.com"
                    or not owner["root_prefix"].endswith("/")):
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid cloud storage owner")
            root = "s3://" + owner["bucket"] + "/" + owner["root_prefix"]
            if canonical_s3(root) + "/" != root:
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid cloud owner root")
        roots = [identity.scope_root]
        for key in ("rest", "rest_mv"):
            entry = ranges[key]
            if (not isinstance(entry, dict) or set(entry) != {"warehouse", "namespace_prefix"}
                    or entry["namespace_prefix"] != identity.run_id + "_"
                    or not isinstance(entry["warehouse"], str)
                    or canonical_s3(entry["warehouse"]) + "/" != entry["warehouse"]):
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid warehouse namespace range")
            roots.append(entry["warehouse"])
        if value["schema"] == 2 and any(not contains(root, candidate) for candidate in roots):
            raise runtime.RuntimeFailure("InvalidScratchScope", "run roots leave the closed cloud owner")
        if any(contains(a, b) or contains(b, a) for i, a in enumerate(roots) for b in roots[i + 1:]):
            raise runtime.RuntimeFailure("InvalidScratchScope", "independent run storage roots overlap")
        # Copy caller-owned maps: subsequent mutations cannot add authority.
        return cls(identity, dict(machine), dict(owner), json.loads(json.dumps(ranges)))

    def as_dict(self):
        return dict(schema=2 if self.storage_owner.get("provider") == "aliyun-oss" else 1,
                    identity=self.identity.as_dict(), machine=dict(self.machine),
                    storage_owner=dict(self.storage_owner), ranges=json.loads(json.dumps(self.ranges)))

    def validate_against(self, identity, record):
        if self.as_dict() != self.from_record(identity, record).as_dict():
            raise runtime.RuntimeFailure("InvalidScratchScope", "inherited storage projection differs from the machine")
        return self

    def owns_namespace(self, name):
        return namespace_run(name) == self.identity.run_id

    def belongs(self, uri):
        # Alias only after validating the entire original storage owner and
        # all roots. An alias never chooses another service or grant.
        if isinstance(uri, str) and uri.startswith("s3a://") and (self.storage_owner.get("provider") == "aliyun-oss"
                or self.storage_owner.get("daemon_id") == self.machine["daemon_id"]):
            uri = "s3://" + uri[6:]
        bucket, key = s3_parts(uri)
        if contains(self.identity.scope_root, uri):
            return True
        parts = key.split("/") if key else []
        for name in ("rest", "rest_mv"):
            wh_bucket, wh_key = s3_parts(self.ranges[name]["warehouse"])
            base = wh_key.split("/") if wh_key else []
            if bucket == wh_bucket and parts[:len(base)] == base and len(parts) > len(base) and self.owns_namespace(parts[len(base)]):
                return True
        return False

    def iam_policy(self):
        if self.storage_owner.get("provider") == "aliyun-oss":
            raise runtime.RuntimeFailure("CloudIamUnsupported", "cloud runs do not create or revoke IAM users")
        prefixes = [self.ranges["run_root"]] + [self.ranges[k]["warehouse"] + self.ranges[k]["namespace_prefix"] for k in ("rest", "rest_mv")]
        by_bucket = {}
        objects = []
        for uri in prefixes:
            bucket, key = s3_parts(uri)
            prefix = key + ("/" if uri.endswith("/") else "")
            by_bucket.setdefault(bucket, []).append(prefix)
            objects.append("arn:aws:s3:::" + bucket + "/" + prefix + "*")
        statements = []
        for bucket, values in sorted(by_bucket.items()):
            statements.append(dict(Effect="Allow", Action=["s3:GetBucketLocation"], Resource=["arn:aws:s3:::" + bucket]))
            statements.append(dict(Effect="Allow", Action=["s3:ListBucket"], Resource=["arn:aws:s3:::" + bucket],
                                   Condition={"StringLike": {"s3:prefix": [p + "*" for p in values]}}))
        statements.append(dict(Effect="Allow", Action=["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject",
                                                       "s3:DeleteObjectVersion", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
                               Resource=objects))
        return dict(Version="2012-10-17", Statement=statements)


@dataclass(frozen=True)
class RunBudget:
    identity: ScratchIdentity
    started_unix_ms: int
    deadline_unix_ms: int
    cleanup_grace_seconds: int
    deadline: float
    monotonic: Any = time.monotonic

    @classmethod
    def admit(cls, record: dict, identity: ScratchIdentity, *, seconds: int = 3600,
              inherited: dict | None = None, wall_clock=time.time,
              monotonic=time.monotonic, cleanup: bool = False) -> RunBudget:
        if type(cleanup) is not bool or (cleanup and inherited is None):
            raise runtime.RuntimeFailure("RunBudgetRejected", "cleanup requires an inherited original budget")
        policy = record.get("policy", record.get("config", {}).get("policy", {}))
        maximum, grace, ttl = (policy.get(k) for k in
                              ("max_run_seconds", "cleanup_grace_seconds", "scratch_ttl_seconds"))
        if (any(type(n) is not int or n <= 0 for n in (maximum, grace, ttl))
                or ttl <= 2 * (maximum + grace) or maximum <= grace
                or (inherited is None and (type(seconds) is not int or seconds <= grace or seconds > maximum))):
            raise runtime.RuntimeFailure("RunBudgetRejected", "invalid or excessive complete fixture budget")
        now = int(wall_clock() * 1000)
        if created_ms(identity.run_id) > now + 5000:
            raise runtime.RuntimeFailure("RunBudgetRejected", "scratch creation time is in the future")
        start, end = now, now
        if inherited is None:
            end += seconds * 1000
        if inherited is not None:
            expected = identity.as_dict()
            if (not isinstance(inherited, dict) or inherited.get("schema") != 1 or inherited.get("identity") != expected
                    or inherited.get("cleanup_grace_seconds") != grace):
                raise runtime.RuntimeFailure("RunBudgetRejected", "inherited run context mismatch")
            start, end = inherited.get("started_unix_ms"), inherited.get("deadline_unix_ms")
            if (type(start) is not int or type(end) is not int or start > now + 5000
                    or end - start <= grace * 1000 or end - start > maximum * 1000):
                raise runtime.RuntimeFailure("RunBudgetRejected", "invalid inherited complete deadline")
        required_remaining = 0 if cleanup else grace * 1000
        if end > created_ms(identity.run_id) + maximum * 1000 + 5000:
            raise runtime.RuntimeFailure("RunBudgetRejected", "deadline exceeds the original identity's run budget")
        protected = record.get("protected_benchmark_roots", []) + [record.get("config", {}).get(
            "benchmark_root", "s3://novarocks/shared/benchmarks")]
        if any(contains(root, identity.scope_root) or contains(identity.scope_root, root) for root in protected):
            raise runtime.RuntimeFailure("RunBudgetRejected", "scratch overlaps a protected benchmark root")
        if end <= now + required_remaining:
            code = "RunBudgetExpired" if inherited is not None else "RunBudgetRejected"
            raise runtime.RuntimeFailure(code, "original fixture deadline has no remaining admitted budget")
        return cls(identity, start, end, grace, monotonic() + (end - now) / 1000, monotonic)

    def remaining_seconds(self, *, work: bool = False) -> float:
        end = self.deadline - (self.cleanup_grace_seconds if work else 0)
        return max(0.0, end - self.monotonic())

    def check(self, *, work: bool = True) -> None:
        if self.remaining_seconds(work=work) <= 0:
            raise runtime.RuntimeFailure("RunBudgetExpired", "original fixture deadline expired")

    def as_dict(self) -> dict:
        return dict(schema=1, identity=self.identity.as_dict(), started_unix_ms=self.started_unix_ms,
                    deadline_unix_ms=self.deadline_unix_ms,
                    cleanup_grace_seconds=self.cleanup_grace_seconds)


class ScratchSweeper:
    def __init__(self, record, backend, *, wall_clock=time.time, monotonic=time.monotonic):
        self.record, self.backend = record, backend
        self.cloud_owner = cloud_storage_owner(record)
        self.wall_clock, self.monotonic = wall_clock, monotonic
        self.blocked_runs = set()
        self.groups = {}
        self.collecting = False
        self.iam_inventory_complete = True
        self.iam_policy_inventory_complete = True
        self.protected = [canonical_s3(p) for p in record.get("protected_benchmark_roots", [])]
        self.protected.append(canonical_s3(record.get("config", {}).get(
            "benchmark_root", "s3://novarocks/shared/benchmarks")))

    def _check(self, deadline):
        if not math.isfinite(deadline) or self.monotonic() >= deadline:
            raise runtime.RuntimeFailure("ScratchSweepTimeout", "scratch sweep deadline expired")

    def _mc(self, args, deadline, *, absent_ok=False):
        if self.cloud_owner is not None:
            raise runtime.RuntimeFailure("CloudIamUnsupported", "cloud storage never executes mc commands")
        self._check(deadline)
        command = " ".join(shlex.quote(str(a)) for a in ["mc", "--json", *args])
        # Preserve mc's native exit in the protocol even though the Docker
        # wrapper itself must finish successfully to return the structured body.
        script = ('export MC_HOST_store="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000"\n'
                  "set +e\n" + command + " 2>&1\nnr_mc_exit=$?\n"
                  "printf '\\nNOVA_SCRATCH_MC_EXIT=%s\\n' \"$nr_mc_exit\"\nexit 0")
        timed_out = False
        try:
            output = self.backend.run_mc(self.record, script, deadline=deadline)
        except runtime.RuntimeFailure as error:
            if error.code == "OperationDeadlineExceeded":
                raise runtime.RuntimeFailure("ScratchSweepTimeout", "scratch sweep deadline expired before dispatch") from error
            if error.code != "DockerOperationTimeout":
                raise
            timed_out = True
            output = getattr(error, "stdout", None)
            if output is None:
                raise runtime.RuntimeFailure("ScratchSweepTimeout", "scratch mc deadline expired after confirmed cleanup") from error
        if len(output.encode()) > MAX_MC_BYTES:
            raise runtime.RuntimeFailure("InvalidMcResponse", "mc response exceeds byte limit")
        lines = output.splitlines()
        has_status = bool(lines and re.fullmatch(r"NOVA_SCRATCH_MC_EXIT=[0-9]+", lines[-1]))
        if not has_status and not timed_out:
            raise runtime.RuntimeFailure("InvalidMcResponse", "missing native mc exit")
        status = int(lines.pop().split("=", 1)[1]) if has_status else None
        records, missing = [], False
        body = "\n".join(lines).strip()
        decoder = json.JSONDecoder()
        while body:
            try:
                value, consumed = decoder.raw_decode(body)
                body = body[consumed:].lstrip()
            except (TypeError, ValueError) as error:
                if timed_out and not has_status:
                    # A deadline may truncate the final JSON object. Complete
                    # native error objects before it still take precedence.
                    break
                raise runtime.RuntimeFailure("InvalidMcResponse", "non-JSON mc response") from error
            if not isinstance(value, dict):
                raise runtime.RuntimeFailure("InvalidMcResponse", "invalid mc JSON shape")
            if value.get("status") == "error":
                detail = value.get("error")
                cause = detail.get("cause") if isinstance(detail, dict) else None
                native = cause.get("error") if isinstance(cause, dict) else None
                code = native.get("Code") if isinstance(native, dict) else None
                code = code or (detail.get("code") if isinstance(detail, dict) else None)
                if absent_ok and code in MISSING_CODES:
                    missing = True
                    continue
                public = "ScratchAccessDenied" if code in {"AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"} else "ScratchMcFailed"
                raise runtime.RuntimeFailure(public, "mc reported an operation failure")
            records.append(value)
        if status and not (absent_ok and missing and not records):
            raise runtime.RuntimeFailure("ScratchMcFailed", f"mc exited with {status}")
        if timed_out:
            raise runtime.RuntimeFailure("ScratchSweepTimeout", "scratch mc deadline expired after confirmed cleanup")
        self._check(deadline)
        return records

    def _expired(self, timestamp, expired_only):
        now = int(self.wall_clock() * 1000)
        if timestamp > now + 5000:
            raise runtime.RuntimeFailure("InvalidScratchIdentity", "scratch timestamp is in the future")
        return not expired_only or timestamp < now - self.record["policy"]["scratch_ttl_seconds"] * 1000

    def _protected(self, uri, *, deletion=False):
        return any(contains(root, uri) or (deletion and contains(uri, root)) for root in self.protected)

    def _error(self, report, operation, target, error):
        # Native diagnostics may contain signed URLs or request credentials.
        # The owner retains those privately; public sweep records use typed codes.
        report["complete"] = False
        report["errors"].append(dict(operation=operation, target=target,
                                     code=getattr(error, "code", type(error).__name__)))

    def _observe(self, report, kind, name, run):
        if "_observed" in report:
            report["_observed"][kind, name] = created_ms(run)

    def _unknown(self, report, kind):
        if "unknown_counts" in report:
            report["unknown_counts"][kind] += 1
            if kind != "rest-namespace":
                report["enumeration_complete"] = False

    def _group(self, run):
        return self.groups.setdefault(run, dict(roots=set(), prefixes=set(), pairs=[], policies=set()))

    def _identity_fact(self, identity):
        group = self._group(identity.run_id)
        group["roots"].add(identity.scope_root)
        if len(group["roots"]) != 1:
            self.blocked_runs.add(identity.run_id)
            raise runtime.RuntimeFailure("InvalidScratchScope", "run has contradictory original storage roots")
        return group

    def _protected_namespace(self, run):
        # Namespace names alone cannot establish their children's locations.
        # A configured protected path containing the run is enough to exclude it.
        return any(run in s3_parts(root)[1].split("/") for root in self.protected)

    def _iam(self, expired_only, deadline, report, *, disable=False):
        # A crash between user-add and policy-attach leaves policyName empty.
        # Read every controllable policy before inferring an orphan's identity;
        # a successfully read subset never proves a unique reverse mapping.
        facts, candidates, failed_policies = {}, {}, set()
        inventory_complete, fact_bytes, names = True, 0, set()
        try:
            items = self._mc(["admin", "policy", "list", "store"], deadline)
        except runtime.RuntimeFailure as error:
            inventory_complete, items = False, []
            if self.collecting:
                self.iam_inventory_complete = False
            if "enumeration_complete" in report:
                report["enumeration_complete"] = False
            self._error(report, "inspect-policies", "store", error)
            if error.code == "ScratchSweepTimeout":
                raise
        for item in items:
            self._check(deadline)
            name = item.get("policy")
            try:
                if not isinstance(name, str) or name in names:
                    raise runtime.RuntimeFailure("InvalidMcResponse", "IAM policy list has an invalid or duplicate name")
                names.add(name)
                if not resource_run(name):
                    if parse_iam_name(name) is not None:
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "compact policy has no full run identity")
                    continue
                identity, purpose = self._policy_identity(name, deadline)
                if identity is None:
                    continue
                self._expired(created_ms(identity.run_id), False)
                fact_bytes += len(name.encode()) + len(identity.scope_root.encode())
                if len(facts) >= MAX_PENDING_PREFIXES or fact_bytes > MAX_MC_BYTES:
                    raise runtime.RuntimeFailure("InvalidMcResponse", "IAM policy inventory exceeds its input bound")
                facts[name] = identity, purpose
                if self.collecting:
                    group = self._identity_fact(identity)
                    group["policies"].add(name)
                candidates.setdefault(identity.iam_name(purpose), set()).add(name)
            except runtime.RuntimeFailure as error:
                inventory_complete = False
                if self.collecting:
                    run = resource_run(name)
                    if run:
                        self.blocked_runs.add(run)
                    else:
                        self.iam_inventory_complete = False
                self._unknown(report, "policy")
                self._error(report, "inspect-policy", name if isinstance(name, str) else "unknown", error)
                if error.code == "ScratchSweepTimeout":
                    raise
                if fact_bytes > MAX_MC_BYTES or len(facts) >= MAX_PENDING_PREFIXES:
                    break
        for item in self._mc(["admin", "user", "list", "store"], deadline):
            self._check(deadline)
            name = item.get("accessKey")
            if not isinstance(name, str):
                raise runtime.RuntimeFailure("InvalidMcResponse", "IAM list has no resource name")
            if parse_iam_name(name) is None:
                continue
            policy, classified = None, False
            possible = candidates.get(name, set())
            try:
                rows = self._mc(["admin", "user", "info", "store", name], deadline, absent_ok=True)
                if not rows:
                    continue
                if len(rows) != 1 or rows[0].get("accessKey") != name:
                    raise runtime.RuntimeFailure("InvalidMcResponse", "user info identity mismatch")
                policy, groups = rows[0].get("policyName"), rows[0].get("memberOf")
                if not isinstance(policy, str) or (groups is not None and not isinstance(groups, list)) or groups:
                    raise runtime.RuntimeFailure("ScratchIamUnmapped", "scratch-shaped user has no exact policy mapping")
                if policy == "":
                    if not inventory_complete or len(possible) != 1:
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "unbound scratch user has no unique complete policy mapping")
                    policy = next(iter(possible))
                    identity, purpose = facts[policy]
                else:
                    if not resource_run(policy):
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "scratch-shaped user has no exact policy mapping")
                    # Explicit native bindings remain usable after a failed
                    # inventory. They do not depend on orphan inference.
                    identity, purpose = facts.get(policy) or self._policy_identity(policy, deadline)
                    if identity is None:
                        raise runtime.RuntimeFailure("ScratchIamUnmapped", "mapped scratch policy is absent")
                if identity.iam_name(purpose) != name:
                    raise runtime.RuntimeFailure("ScratchIamUnmapped", "scratch user differs from policy identity")
                expired = self._expired(created_ms(identity.run_id), expired_only)
                self._observe(report, "user", name, identity.run_id)
                classified = True
                if self.collecting:
                    self._identity_fact(identity)["pairs"].append(dict(user=name, policy=policy))
                    continue
                if not expired:
                    continue
                operation = "disable" if disable else "remove"
                self._mc(["admin", "user", "disable" if disable else "rm", "store", name], deadline, absent_ok=True)
                report["removed"].append(dict(kind="user", name=name, operation=operation))
            except runtime.RuntimeFailure as error:
                if self.collecting:
                    runs = {resource_run(p) for p in possible | ({policy} if isinstance(policy, str) else set())}
                    runs.discard(None)
                    if runs:
                        self.blocked_runs.update(runs)
                    else:
                        self.iam_inventory_complete = False
                if not classified:
                    self._unknown(report, "user")
                # Preserve every validated candidate as well as explicit native
                # bindings. In particular, empty policyName is not a policy ID.
                failed_policies.update(possible)
                if isinstance(policy, str):
                    failed_policies.update(p.strip() for p in policy.split(",") if p.strip())
                self._error(report, "disable" if disable else "remove", name, error)
                if error.code == "ScratchSweepTimeout":
                    raise
        for name, (identity, _) in facts.items():
            self._check(deadline)
            self._observe(report, "policy", name, identity.run_id)
            if self.collecting or disable or not inventory_complete or name in failed_policies:
                continue
            try:
                if self._expired(created_ms(identity.run_id), expired_only):
                    self._mc(["admin", "policy", "rm", "store", name], deadline, absent_ok=True)
                    report["removed"].append(dict(kind="policy", name=name, operation="remove"))
            except runtime.RuntimeFailure as error:
                self._error(report, "remove", name, error)
                if error.code == "ScratchSweepTimeout":
                    raise
        if self.collecting:
            self.iam_policy_inventory_complete = inventory_complete

    def _policy_identity(self, name, deadline):
        match = RESOURCE_RE.fullmatch(name)
        if not match or len(name) > 63 or match[2] not in {"init", "rot"}:
            raise runtime.RuntimeFailure("ScratchIamUnmapped", "policy does not identify an admitted IAM purpose")
        rows = self._mc(["admin", "policy", "info", "store", name], deadline, absent_ok=True)
        if not rows:
            return None, None
        if len(rows) != 1 or rows[0].get("policy") != name:
            raise runtime.RuntimeFailure("InvalidMcResponse", "policy info identity mismatch")
        info = rows[0].get("policyInfo")
        if not isinstance(info, dict) or info.get("PolicyName") != name:
            raise runtime.RuntimeFailure("InvalidMcResponse", "invalid native policyInfo shape")
        document = info.get("Policy")
        if (not isinstance(document, dict) or document.get("Version") != "2012-10-17"
                or set(document) - {"Version", "Statement"}
                or not isinstance(document.get("Statement"), list) or not document["Statement"]):
            raise runtime.RuntimeFailure("InvalidScratchScope", "invalid scoped IAM policy document")
        # New fixed-range policies are recognized from their one original run
        # root, then compared with the complete machine-derived policy. A
        # metadata location can never supply an additional deletion range.
        roots = set()
        for statement in document["Statement"]:
            resources = statement.get("Resource", []) if isinstance(statement, dict) else []
            resources = [resources] if isinstance(resources, str) else resources
            if not isinstance(resources, list):
                continue
            for resource in resources:
                if isinstance(resource, str) and resource.startswith("arn:aws:s3:::") and resource.endswith("/*"):
                    uri = "s3://" + resource[len("arn:aws:s3:::"):-2]
                    if uri.endswith("/scratch/" + match[1]):
                        roots.add(canonical_s3(uri))
        if len(roots) == 1:
            identity = ScratchIdentity(match[1], next(iter(roots)))
            scope = RunStorageScope.from_record(identity, self.record)
            if _policy_shape(document) == _policy_shape(scope.iam_policy()):
                return identity, match[2]
        object_actions = {"s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject",
                          "s3:DeleteObjectVersion", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"}
        root, bucket_statements = None, []
        for statement in document["Statement"]:
            if (not isinstance(statement, dict) or statement.get("Effect") != "Allow"
                    or set(statement) - {"Effect", "Action", "Resource", "Condition", "Sid"}):
                raise runtime.RuntimeFailure("InvalidScratchScope", "unsupported scoped IAM statement")
            actions, resources = statement.get("Action"), statement.get("Resource")
            actions = [actions] if isinstance(actions, str) else actions
            resources = [resources] if isinstance(resources, str) else resources
            if (not isinstance(actions, list) or not actions or not all(isinstance(a, str) for a in actions)
                    or not isinstance(resources, list) or not resources or not all(isinstance(r, str) for r in resources)):
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid scoped IAM actions or resources")
            if set(actions) <= object_actions:
                if statement.get("Condition"):
                    raise runtime.RuntimeFailure("InvalidScratchScope", "unexpected object policy condition")
                for resource in resources:
                    if not resource.startswith("arn:aws:s3:::") or not resource.endswith("/*"):
                        raise runtime.RuntimeFailure("InvalidScratchScope", "object policy must name one exact prefix")
                    candidate = canonical_s3("s3://" + resource[len("arn:aws:s3:::"):-2])
                    if "*" in candidate or "?" in candidate or (root is not None and candidate != root):
                        raise runtime.RuntimeFailure("InvalidScratchScope", "policy grants more than one scratch root")
                    root = candidate
            elif set(actions) <= {"s3:ListBucket", "s3:GetBucketLocation"}:
                bucket_statements.append((set(actions), resources, statement.get("Condition")))
            else:
                raise runtime.RuntimeFailure("InvalidScratchScope", "policy action exceeds the scratch contract")
        if root is None or self._protected(root, deletion=True):
            raise runtime.RuntimeFailure("InvalidScratchScope", "policy root is absent or protected")
        identity = ScratchIdentity(match[1], root)
        bucket, key = s3_parts(root)
        for actions, resources, condition in bucket_statements:
            if set(resources) != {"arn:aws:s3:::" + bucket}:
                raise runtime.RuntimeFailure("InvalidScratchScope", "policy bucket differs from the scratch root")
            if "s3:ListBucket" not in actions:
                if condition:
                    raise runtime.RuntimeFailure("InvalidScratchScope", "unexpected location condition")
                continue
            if not isinstance(condition, dict) or len(condition) != 1 or next(iter(condition)) not in {"StringLike", "StringEquals"}:
                raise runtime.RuntimeFailure("InvalidScratchScope", "bucket listing needs an exact prefix condition")
            predicates = next(iter(condition.values()))
            if not isinstance(predicates, dict) or set(predicates) != {"s3:prefix"}:
                raise runtime.RuntimeFailure("InvalidScratchScope", "invalid bucket prefix condition")
            prefixes = predicates["s3:prefix"]
            prefixes = [prefixes] if isinstance(prefixes, str) else prefixes
            if not isinstance(prefixes, list) or not prefixes or any(not isinstance(p, str) or p not in {key + "/", key + "/*"} for p in prefixes):
                raise runtime.RuntimeFailure("InvalidScratchScope", "bucket listing escapes the scratch root")
        return identity, match[2]

    def revoke_users(self):
        if self.cloud_owner is not None:
            raise runtime.RuntimeFailure("CloudIamUnsupported", "cloud runs do not revoke IAM users")
        deadline = self.monotonic() + getattr(self.backend, "timeout", 90)
        report = dict(complete=True, removed=[], errors=[])
        self._iam(False, deadline, report, disable=True)
        if not report["complete"]:
            raise runtime.RuntimeFailure("ScratchCredentialRevocationFailed", "scratch users were not all disabled")
        return report

    def _catalog_inventory(self, deadline, report):
        inventory = {}
        for service in ("rest", "rest-mv"):
            names = []
            for namespace in self._pages(service, "namespaces", "namespaces", deadline):
                if not isinstance(namespace, list) or not namespace or not all(isinstance(n, str) for n in namespace):
                    raise runtime.RuntimeFailure("InvalidRestResponse", "invalid namespace identity")
                run = namespace_run(namespace[0]) if len(namespace) == 1 else None
                if run is None:
                    self._unknown(report, "rest-namespace")
                    continue
                if namespace[0] in names:
                    raise runtime.RuntimeFailure("InvalidRestResponse", "duplicate namespace identity")
                self._expired(created_ms(run), False)
                if self._protected_namespace(run):
                    continue
                names.append(namespace[0])
                self._group(run)
                self._observe(report, "rest-namespace", (service, namespace[0]), run)
            inventory[service] = names
        return inventory

    def _belongs_group(self, run, location):
        if isinstance(location, str) and location.startswith("s3a://"):
            # The sweeper already runs under the exact machine/MinIO lock and
            # backend binding. Never rewrite the URI used for object I/O.
            if self.cloud_owner is None and (not self.record.get("daemon_id") or not self.record.get("config", {}).get("minio_volume")):
                return False
            location = "s3://" + location[6:]
        bucket, key = s3_parts(location)
        if self._protected(location, deletion=True):
            return False
        if any(contains(root, location) for root in self._group(run)["roots"]):
            return True
        parts = key.split("/") if key else []
        for service in ("rest", "rest-mv"):
            wh_bucket, wh_key = s3_parts(self.record["server_warehouses"][service])
            base = wh_key.split("/") if wh_key else []
            if bucket == wh_bucket and parts[:len(base)] == base and len(parts) > len(base) and namespace_run(parts[len(base)]) == run:
                return True
        return False

    def _catalog_run(self, run, inventory, deadline, report):
        barrier = dict(complete=False, services={s: dict(enumeration_complete=False, entries_absent=False)
                                               for s in ("rest", "rest-mv")})
        report.setdefault("catalog_barrier", {})[run] = barrier
        children, namespaces = [], []
        # Validate every child in both catalogs before the first catalog delete.
        for service, names in inventory.items():
            for name in names:
                if namespace_run(name) != run:
                    continue
                base = "namespaces/" + quote(name, safe="")
                namespaces.append((service, base, name))
                for collection in ("views", "tables"):
                    seen = set()
                    for child in self._pages(service, base + "/" + collection, "identifiers", deadline, absent_ok=True):
                        if (not isinstance(child, dict) or child.get("namespace") != [name]
                                or not isinstance(child.get("name"), str) or not child["name"] or child["name"] in seen):
                            raise runtime.RuntimeFailure("InvalidRestResponse", "invalid child identifier")
                        seen.add(child["name"])
                        path = base + "/" + collection + "/" + quote(child["name"], safe="")
                        metadata = self._rest_request(service, "GET", path, deadline, absent_ok=True)
                        if metadata is None:
                            continue
                        facts = metadata.get("metadata") if isinstance(metadata, dict) else None
                        location = facts.get("location") if isinstance(facts, dict) else None
                        if not isinstance(location, str) or not self._belongs_group(run, location):
                            raise runtime.RuntimeFailure("InvalidScratchScope", "REST child is outside the fixed run storage ranges")
                        children.append((service, path, collection))
            barrier["services"][service]["enumeration_complete"] = True
        for service, path, collection in children:
            self._rest_request(service, "DELETE", path + ("?purgeRequested=false" if collection == "tables" else ""),
                               deadline, absent_ok=True)
            report["removed"].append(dict(kind="rest-child", service=service, path=path))
        for service, base, name in namespaces:
            self._rest_request(service, "DELETE", base, deadline, absent_ok=True)
            report["removed"].append(dict(kind="rest-namespace", service=service, namespace=[name]))
        # A renamed table can still reference another namespace's directory.
        # Only absence of every owned namespace in BOTH catalogs clears files.
        for service in ("rest", "rest-mv"):
            for namespace in self._pages(service, "namespaces", "namespaces", deadline):
                if not isinstance(namespace, list) or not namespace or not all(isinstance(n, str) for n in namespace):
                    raise runtime.RuntimeFailure("InvalidRestResponse", "invalid namespace identity during barrier confirmation")
                if len(namespace) == 1 and namespace_run(namespace[0]) == run:
                    raise runtime.RuntimeFailure("ScratchCatalogNotEmpty", "run catalog entries remain after deletion")
            barrier["services"][service]["entries_absent"] = True
        barrier["complete"] = True

    def _remove_pairs(self, identity, pairs, policies, deadline, report):
        verified = []
        for pair in pairs:
            rows = self._mc(["admin", "user", "info", "store", pair["user"]], deadline, absent_ok=True)
            if rows and (len(rows) != 1 or rows[0].get("accessKey") != pair["user"] or rows[0].get("memberOf")
                         or not isinstance(rows[0].get("policyName", ""), str)
                         or rows[0].get("policyName", "") not in {"", pair["policy"]}):
                raise runtime.RuntimeFailure("ScratchIamUnmapped", "cleanup user has a foreign IAM binding")
            verified.append((pair, bool(rows)))
        for policy in policies:
            found, _ = self._policy_identity(policy, deadline)
            if found is not None and found != identity:
                raise runtime.RuntimeFailure("InvalidScratchScope", "cleanup policy differs from the original run")
        for pair, present in verified:
            if present:
                for operation in ("disable", "rm"):
                    self._mc(["admin", "user", operation, "store", pair["user"]], deadline, absent_ok=True)
                    report["removed"].append(dict(kind="user", name=pair["user"], operation=operation))
        for policy in policies:
            self._mc(["admin", "policy", "rm", "store", policy], deadline, absent_ok=True)
            report["removed"].append(dict(kind="policy", name=policy, operation="remove"))

    def _remove_prefixes(self, group, deadline, report):
        for uri in sorted(group["roots"] | group["prefixes"]):
            if self._protected(uri, deletion=True):
                raise runtime.RuntimeFailure("InvalidScratchScope", "scratch deletion overlaps protected data")
            bucket, key = s3_parts(uri)
            self._mc(["rm", "--recursive", "--force", "store/" + bucket + "/" + key + "/"], deadline, absent_ok=True)
            report["removed"].append(dict(kind="s3-prefix", uri=uri))

    def _warehouse_prefixes(self, deadline, report, *, run=None):
        for service in ("rest", "rest-mv"):
            warehouse = canonical_s3(self.record["server_warehouses"][service])
            if self._protected(warehouse, deletion=True):
                raise runtime.RuntimeFailure("InvalidScratchScope", "warehouse overlaps protected data")
            bucket, key = s3_parts(warehouse)
            for row in self._mc(["ls", "store/" + bucket + ("/" + key if key else "") + "/"], deadline, absent_ok=True):
                component = self._folder_component(row)
                if component is None:
                    continue
                owner = namespace_run(component)
                if owner is not None and (run is None or owner == run):
                    uri = warehouse + "/" + component
                    self._group(owner)["prefixes"].add(uri)
                    self._observe(report, "s3-prefix", uri, owner)

    @staticmethod
    def _folder_component(row):
        child = row.get("key")
        if row.get("type") not in {"folder", "file"} or not isinstance(child, str):
            raise runtime.RuntimeFailure("InvalidMcResponse", "invalid S3 object listing")
        if row["type"] == "file":
            return None
        component = child[:-1] if child.endswith("/") else ""
        if not component or "/" in component or component in {".", ".."}:
            raise runtime.RuntimeFailure("InvalidMcResponse", "unsafe listed S3 prefix")
        # Parse before any path can become traversal or deletion authority.
        s3_parts("s3://validation/" + component)
        return component

    def cleanup_run(self, scope: RunStorageScope, exact_users_and_policies: list, deadline: float) -> dict:
        """The caller confirms actual writer exit and retains its original budget."""
        report = dict(complete=True, removed=[], errors=[], catalog_barrier={})
        try:
            self._check(deadline)
            if not isinstance(scope, RunStorageScope):
                raise runtime.RuntimeFailure("InvalidScratchScope", "cleanup requires a fixed run storage projection")
            scope.validate_against(scope.identity, self.record)
            identity = scope.identity
            if not isinstance(exact_users_and_policies, list):
                raise runtime.RuntimeFailure("ScratchIamUnmapped", "cleanup needs predeclared IAM pairs")
            if self.cloud_owner is not None:
                if exact_users_and_policies:
                    raise runtime.RuntimeFailure("CloudIamUnsupported", "cloud exact cleanup cannot carry IAM pairs")
                self.groups, self.blocked_runs = {}, set()
                self._identity_fact(identity)
                inventory = self._catalog_inventory(deadline, report)
                self._catalog_run(identity.run_id, inventory, deadline, report)
                self._cleanup_cloud_scope(scope, deadline, report)
                return report
            seen = set()
            for pair in exact_users_and_policies:
                if not isinstance(pair, dict) or set(pair) != {"user", "policy"}:
                    raise runtime.RuntimeFailure("ScratchIamUnmapped", "invalid cleanup IAM pair")
                purpose = next((p for p in ("init", "rot") if pair["policy"] == identity.resource_name(p)
                                and pair["user"] == identity.iam_name(p)), None)
                if purpose is None or purpose in seen:
                    raise runtime.RuntimeFailure("ScratchIamUnmapped", "cleanup pair differs from the original run")
                seen.add(purpose)
            self.groups, self.blocked_runs = {}, set()
            group = self._identity_fact(identity)
            inventory = self._catalog_inventory(deadline, report)
            self._catalog_run(identity.run_id, inventory, deadline, report)
            self._remove_pairs(identity, exact_users_and_policies, {p["policy"] for p in exact_users_and_policies}, deadline, report)
            self._warehouse_prefixes(deadline, report, run=identity.run_id)
            self._remove_prefixes(group, deadline, report)
        except runtime.RuntimeFailure as error:
            self._error(report, "cleanup-run", getattr(getattr(scope, "identity", None), "run_id", "invalid-context"), error)
        return report

    def _oss(self, operation, uri, deadline, *extra):
        self._check(deadline)
        output = self.backend.run_oss(self.record, [operation, "--uri", uri, *extra], deadline=deadline)
        if not isinstance(output, str) or len(output.encode()) > MAX_MC_BYTES:
            raise runtime.RuntimeFailure("InvalidObjectStoreResponse", "OSS response exceeds its byte bound")
        try:
            value = json.loads(output)
        except (TypeError, ValueError) as error:
            raise runtime.RuntimeFailure("InvalidObjectStoreResponse", "OSS response is not one JSON object") from error
        if not isinstance(value, dict) or value.get("ok") is not True:
            raise runtime.RuntimeFailure("ScratchObjectStoreFailed", "OSS operation did not succeed")
        self._check(deadline)
        return value

    def _cloud_rows(self, operation, prefix, deadline):
        field = "objects" if operation == "list" else "uploads"
        rows = self._oss(operation, prefix, deadline).get(field)
        if not isinstance(rows, list) or len(rows) > MAX_PENDING_PREFIXES:
            raise runtime.RuntimeFailure("InvalidObjectStoreResponse", "invalid bounded OSS listing")
        for row in rows:
            if (not isinstance(row, dict) or not isinstance(row.get("uri"), str)
                    or not contains(prefix, row["uri"])):
                raise runtime.RuntimeFailure("InvalidScratchScope", "OSS listing leaves its requested prefix")
            if operation == "list-multipart" and (not isinstance(row.get("upload_id"), str) or not row["upload_id"]):
                raise runtime.RuntimeFailure("InvalidObjectStoreResponse", "multipart identity is missing")
        return rows

    def _cleanup_cloud_scope(self, scope, deadline, report):
        barrier = report.get("catalog_barrier", {}).get(scope.identity.run_id, {})
        if barrier.get("complete") is not True:
            raise runtime.RuntimeFailure("ScratchCatalogBarrierRequired", "exact cloud cleanup requires both catalog barriers")
        prefixes = {scope.ranges["run_root"]}
        # A multipart upload may be the only trace of an owned namespace. Both
        # inventories are observations; only the exact original run is removed.
        for service in ("rest", "rest_mv"):
            warehouse = scope.ranges[service]["warehouse"]
            for operation in ("list", "list-multipart"):
                for row in self._cloud_rows(operation, warehouse, deadline):
                    relative = canonical_s3(row["uri"])[len(canonical_s3(warehouse)) + 1:]
                    namespace = relative.split("/", 1)[0]
                    if scope.owns_namespace(namespace):
                        prefixes.add(warehouse + namespace + "/")
        objects, uploads = set(), set()
        for prefix in sorted(prefixes):
            if not scope.belongs(prefix) or self._protected(prefix, deletion=True):
                raise runtime.RuntimeFailure("InvalidScratchScope", "cloud cleanup leaves its exact original run")
            for operation in ("list", "list-multipart"):
                for row in self._cloud_rows(operation, prefix, deadline):
                    if not scope.belongs(row["uri"]) or self._protected(row["uri"], deletion=True):
                        raise runtime.RuntimeFailure("InvalidScratchScope", "cloud object leaves its exact original run")
                    if operation == "list":
                        objects.add(row["uri"])
                    else:
                        uploads.add((row["uri"], row["upload_id"]))
        # All catalog and object inventories are validated before mutating OSS.
        for uri, upload_id in sorted(uploads):
            self._oss("abort-multipart", uri, deadline, "--upload-id", upload_id)
            report["removed"].append(dict(kind="s3-multipart", uri=uri))
        for uri in sorted(objects):
            self._oss("delete", uri, deadline)
            report["removed"].append(dict(kind="s3-object", uri=uri))
        for prefix in sorted(prefixes):
            if self._cloud_rows("list", prefix, deadline) or self._cloud_rows("list-multipart", prefix, deadline):
                raise runtime.RuntimeFailure("ScratchCleanupIncomplete", "owned cloud objects remain after cleanup")

    def _s3(self, expired_only, deadline, report):
        # The optional private cursor is only a continuation of enumeration.
        # It records no holder, lease, authority or liveness fact.
        cursor = (Path(self.record["compose_file"]).parent / "scratch-cursor.json"
                  if self.record.get("compose_file") else None)
        pending = []
        cached_roots = []
        cursor_identity = dict(schema=1, version=self.record.get("version"),
                               generation=self.record.get("generation"), expired_only=expired_only)
        if cursor and cursor.is_symlink():
            raise runtime.RuntimeFailure("RuntimeStateUnreadable", "scratch continuation is not a regular owned file")
        if cursor and cursor.exists():
            saved = runtime.read_json(cursor)
            if all(saved.get(k) == v for k, v in cursor_identity.items()):
                pending = saved.get("pending")
                if (not isinstance(pending, list) or len(pending) > MAX_PENDING_PREFIXES
                        or not all(isinstance(p, str) and canonical_s3(p) == p for p in pending)):
                    raise runtime.RuntimeFailure("RuntimeStateUnreadable", "invalid scratch enumeration continuation")
                if pending:
                    report["enumeration_complete"] = False
                    cached_roots = saved.get("observed_run_roots", [])
                    if not isinstance(cached_roots, list) or len(cached_roots) > MAX_PENDING_PREFIXES:
                        raise runtime.RuntimeFailure("RuntimeStateUnreadable", "invalid scratch root continuation")
                    parents = {}
                    for root in cached_roots:
                        self._check(deadline)
                        bucket, key = s3_parts(root)
                        parent, _, run = key.rpartition("/")
                        ScratchIdentity(run, root)
                        if canonical_s3(root) != root or self._protected(root, deletion=True):
                            raise runtime.RuntimeFailure("RuntimeStateUnreadable", "unsafe continued scratch root")
                        parents.setdefault((bucket, parent), {})[run] = root
                    for (bucket, parent), requested in parents.items():
                        self._check(deadline)
                        # Keep only requested-run presence from each fresh
                        # listing, never the full rows of multiple parents.
                        rows = self._mc(["ls", "store/" + bucket + "/" + parent + "/"], deadline, absent_ok=True)
                        for row in rows:
                            self._check(deadline)
                            run = self._folder_component(row)
                            if run in requested:
                                self._identity_fact(ScratchIdentity(run, requested[run]))
                        del rows
        if not pending:
            for row in self._mc(["ls", "store"], deadline):
                if row.get("type") != "folder" or not isinstance(row.get("key"), str):
                    raise runtime.RuntimeFailure("InvalidMcResponse", "invalid S3 bucket listing")
                bucket, key = s3_parts("s3://" + row["key"].rstrip("/"))
                if key:
                    raise runtime.RuntimeFailure("InvalidMcResponse", "invalid S3 bucket identity")
                pending.append("s3://" + bucket)
        visited = 0
        try:
            while pending:
                self._check(deadline)
                if visited >= MAX_PREFIX_VISITS:
                    raise runtime.RuntimeFailure("ScratchSweepTraversalLimit", "prefix visit budget exhausted")
                uri = pending[-1]
                visited += 1
                bucket, key = s3_parts(uri)
                parts = key.split("/") if key else []
                if self._protected(uri):
                    pending.pop()
                    continue
                if len(parts) > MAX_PREFIX_DEPTH:
                    report["enumeration_complete"] = False
                    pending.pop()
                    self._error(report, "enumerate-s3-prefix", uri, runtime.RuntimeFailure(
                        "ScratchSweepTraversalLimit", "prefix depth exceeded"))
                    continue
                if len(parts) >= 2 and parts[-2] == "scratch" and RUN_RE.fullmatch(parts[-1]):
                    run = parts[-1]
                    if self._protected(uri, deletion=True):
                        pending.pop()
                        continue
                    expired = self._expired(created_ms(run), expired_only)
                    self._observe(report, "s3-prefix", uri, run)
                    if expired:
                        self._identity_fact(ScratchIdentity(run, uri))
                    pending.pop()
                    continue
                if uri in {canonical_s3(v) for v in self.record["server_warehouses"].values()}:
                    pending.pop()
                    continue
                children = []
                target = "store/" + bucket + ("/" + key if key else "") + "/"
                for row in self._mc(["ls", target], deadline, absent_ok=True):
                    self._check(deadline)
                    child = row.get("key")
                    if row.get("type") not in {"folder", "file"} or not isinstance(child, str):
                        raise runtime.RuntimeFailure("InvalidMcResponse", "invalid S3 object listing")
                    if row["type"] == "folder":
                        component = child[:-1] if child.endswith("/") else ""
                        if not component or "/" in component or component in {".", ".."}:
                            raise runtime.RuntimeFailure("InvalidMcResponse", "unsafe listed S3 prefix")
                        child_uri = canonical_s3(uri + "/" + component)
                        if not self._protected(child_uri):
                            children.append(child_uri)
                if len(pending) + len(children) > MAX_PENDING_PREFIXES:
                    raise runtime.RuntimeFailure("ScratchSweepTraversalLimit", "pending prefix budget exhausted")
                pending.pop()
                pending.extend(sorted(set(children), reverse=True))
        finally:
            if cursor:
                roots = sorted({root for group in self.groups.values() for root in group["roots"]}) if pending else []
                runtime.atomic_json(cursor, {**cursor_identity, "pending": pending, "observed_run_roots": roots})

    def _rest_request(self, service, method, path, deadline, *, absent_ok=False):
        self._check(deadline)
        port = self.record["ports"]["rest" if service == "rest" else "rest_mv"]
        connection = _DeadlineConnection(port, deadline, self.monotonic)
        response = None
        try:
            connection.request(method, "/v1/" + path)
            response = connection.getresponse()
            if response.status == 404 and absent_ok:
                return None
            if not 200 <= response.status < 300:
                code = "ScratchAccessDenied" if response.status in (401, 403) else "ScratchRestFailed"
                raise runtime.RuntimeFailure(code, f"REST returned HTTP {response.status}")
            raw = response.read(MAX_REST_BYTES + 1)
            self._check(deadline)
            if len(raw) > MAX_REST_BYTES:
                raise runtime.RuntimeFailure("InvalidRestResponse", "REST listing exceeds byte limit")
            value = json.loads(raw) if raw else {}
            if not isinstance(value, dict):
                raise runtime.RuntimeFailure("InvalidRestResponse", "REST response is not an object")
            return value
        except (TimeoutError, OSError, http.client.HTTPException) as error:
            if self.monotonic() >= deadline:
                raise runtime.RuntimeFailure("ScratchSweepTimeout", "original REST deadline expired") from error
            raise runtime.RuntimeFailure("ScratchRestUnavailable", "REST request did not complete") from error
        except ValueError as error:
            raise runtime.RuntimeFailure("InvalidRestResponse", "REST response is not JSON") from error
        finally:
            if response is not None:
                response.close()
            connection.close()

    def _pages(self, service, path, field, deadline, *, absent_ok=False):
        token, seen = None, set()
        while True:
            query = urlencode({"pageToken": token}) if token else ""
            response = self._rest_request(service, "GET", path + ("?" + query if query else ""), deadline,
                                          absent_ok=absent_ok)
            if response is None:
                return
            if not isinstance(response, dict) or not isinstance(response.get(field), list):
                raise runtime.RuntimeFailure("InvalidRestResponse", "invalid REST list shape")
            yield from response[field]
            token = response.get("next-page-token")
            if token is None:
                return
            if not isinstance(token, str) or not token or token in seen:
                raise runtime.RuntimeFailure("InvalidRestResponse", "invalid REST continuation")
            seen.add(token)

    def _offline_barrier(self):
        expected = dict(schema=1, daemon_id=self.record.get("daemon_id"), generation=self.record.get("generation"),
                        volumes=["nr-fixture-machine_rest-catalog", "nr-fixture-machine_rest-mv-catalog"], complete=True)
        barrier = self.record.get("offline_catalog_barrier")
        if (not expected["daemon_id"] or not expected["generation"] or barrier != expected):
            raise runtime.RuntimeFailure("ScratchCatalogBarrierRequired", "offline purge requires the exact confirmed catalog-state absence barrier")
        return dict(expected)

    def sweep(self, *, expired_only: bool, deadline: float) -> dict:
        if self.cloud_owner is not None:
            raise runtime.RuntimeFailure("CloudSweepUnsupported", "cloud cleanup requires the original run budget and confirmed writer exit")
        kinds = ["s3-prefix", "rest-namespace", "user", "policy"]
        report = dict(complete=True, removed=[], errors=[], catalog_barrier={},
                      observed_counts={kind: 0 for kind in kinds}, oldest_created_unix_ms=None,
                      unknown_counts=dict(user=0, policy=0, **{"rest-namespace": 0}),
                      sampled_at_unix_ms=int(self.wall_clock() * 1000), counts_are_pre_cleanup=True,
                      enumeration_complete=True, enumeration_scope=kinds if expired_only else [k for k in kinds if k != "rest-namespace"],
                      classification_basis={"rest-namespace": "complete run-plus-purpose grammar; nonexpired child metadata not inspected",
                                            "s3-prefix": "independently enumerated exact run root or warehouse direct namespace directory",
                                            "user": "native binding or unique complete policy inventory; exact scoped policy",
                                            "policy": "full run policy name and verified scoped policy"}, _observed={})
        self.groups, self.blocked_runs = {}, set()
        self.iam_inventory_complete = True
        self.iam_policy_inventory_complete = True
        try:
            self._check(deadline)
            if expired_only:
                inventory = self._catalog_inventory(deadline, report)
            else:
                report["offline_catalog_barrier"] = self._offline_barrier()
                inventory = None
            self._warehouse_prefixes(deadline, report)
            self._s3(expired_only, deadline, report)
            self.collecting = True
            try:
                self._iam(expired_only, deadline, report)
            finally:
                self.collecting = False
            for run, group in sorted(self.groups.items()):
                if not self._expired(created_ms(run), expired_only) or self._protected_namespace(run):
                    continue
                if run in self.blocked_runs:
                    continue
                try:
                    if inventory is not None:
                        self._catalog_run(run, inventory, deadline, report)
                    if len(group["roots"]) > 1:
                        raise runtime.RuntimeFailure("InvalidScratchScope", "run has contradictory original roots")
                    if group["policies"] or group["pairs"]:
                        if not group["roots"]:
                            raise runtime.RuntimeFailure("ScratchIamUnmapped", "IAM cleanup has no independent run root")
                        self._remove_pairs(ScratchIdentity(run, next(iter(group["roots"]))), group["pairs"],
                                           group["policies"] if self.iam_policy_inventory_complete else set(), deadline, report)
                    if self.iam_inventory_complete:
                        self._remove_prefixes(group, deadline, report)
                except runtime.RuntimeFailure as error:
                    self.blocked_runs.add(run)
                    self._error(report, "cleanup-discovered-run", run, error)
                    if error.code == "ScratchSweepTimeout":
                        raise
        except runtime.RuntimeFailure as error:
            report["enumeration_complete"] = False
            self._error(report, "enumerate-scratch", "machine", error)
        observed = report.pop("_observed")
        for (kind, _), timestamp in observed.items():
            report["observed_counts"][kind] += 1
            oldest = report["oldest_created_unix_ms"]
            report["oldest_created_unix_ms"] = timestamp if oldest is None else min(oldest, timestamp)
        return report
