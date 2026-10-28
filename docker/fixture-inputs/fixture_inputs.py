#!/usr/bin/env python3
"""Shared integrity primitives for fixture input provision and verification."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import platform as host_platform
import re
import subprocess
from pathlib import Path
from typing import Any, Mapping


class FixtureInputError(RuntimeError):
    """A typed prerequisite/integrity/provision failure safe for CI summaries."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha1_file(path: Path) -> str:
    digest = hashlib.sha1()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureInputError(f"cannot read JSON: {path}") from error
    if not isinstance(value, dict):
        raise FixtureInputError(f"JSON root must be an object: {path}")
    return value


def normalize_platform(value: str) -> str:
    aliases = {"x86_64": "amd64", "aarch64": "arm64", "arm64/v8": "arm64"}
    parts = value.lower().split("/", 1)
    if len(parts) != 2 or parts[0] != "linux":
        raise FixtureInputError(f"unsupported fixture native platform: {value}")
    architecture = aliases.get(parts[1], parts[1])
    if architecture not in ("amd64", "arm64"):
        raise FixtureInputError(f"unsupported fixture native platform: {value}")
    return f"linux/{architecture}"


def native_platform() -> str:
    return normalize_platform(f"linux/{host_platform.machine()}")


def require_native_daemon(target: str) -> None:
    """Observe the actual daemon; emulation is not native fixture readiness."""
    native = native_platform()
    actual = normalize_platform(run(
        ["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"], capture=True
    ).strip())
    if target != native or actual != native:
        raise FixtureInputError(
            f"fixture native platform mismatch: target={target}, host={native}, daemon={actual}"
        )


def read_source_lock(path: Path) -> dict[str, Any]:
    lock = read_json(path)
    if lock.get("schema") != 2:
        raise FixtureInputError(
            f"unsupported fixture input lock schema: {lock.get('schema')!r}; reprovision with schema 2"
        )
    for key in ("images", "artifacts", "derived_images"):
        if not isinstance(lock.get(key), dict):
            raise FixtureInputError(f"fixture input lock has no {key} object")
    for kind in ("images", "derived_images"):
        for name, item in lock[kind].items():
            if not isinstance(item, dict) or not isinstance(item.get("variants"), dict) or not item["variants"]:
                raise FixtureInputError(f"fixture platform variants are missing: {kind}/{name}")
            for target, variant in item["variants"].items():
                if normalize_platform(target) != target or not isinstance(variant, dict):
                    raise FixtureInputError(f"invalid fixture variant: {kind}/{name}/{target}")
                if variant.get("platform", target) != target:
                    raise FixtureInputError(f"fixture variant platform mismatch: {kind}/{name}/{target}")
    for name, declaration in lock["derived_images"].items():
        for variant in declaration["variants"].values():
            item = {**{key: value for key, value in declaration.items() if key != "variants"}, **variant}
            if not isinstance(item, dict):
                raise FixtureInputError(f"fixture derived image must be an object: {name}")
            if "base" in item:
                raise FixtureInputError(f"fixture derived image uses unsupported base field; use bases: {name}")
            bases = item.get("bases")
            if not isinstance(bases, dict) or not bases:
                raise FixtureInputError(f"fixture derived image bases must be a non-empty mapping: {name}")
            for argument, base in bases.items():
                if not isinstance(argument, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", argument):
                    raise FixtureInputError(f"invalid fixture base build argument: {name}/{argument}")
                if not isinstance(base, str) or not isinstance(lock["images"].get(base), dict):
                    raise FixtureInputError(f"fixture derived image base is missing: {name}/{base}")
            if "context" in item:
                context = item["context"]
                if not isinstance(context, str) or not context:
                    raise FixtureInputError(f"fixture build context must be a repository-relative directory: {name}")
                require_relative(context)
    return lock


def resolve_lock(
    source: Mapping[str, Any], platform: str, *, consumer: str = "all"
) -> tuple[dict[str, Any], str]:
    target = normalize_platform(platform)
    # Select variant facts before walking recipe dependencies. Missing variants
    # remain explicit unsupported nodes, never silently omitted from `all`.
    potential: dict[str, Any] = {"schema": 2, "platform": target, "artifacts": source["artifacts"]}
    unavailable: set[tuple[str, str]] = set()
    for kind in ("images", "derived_images"):
        potential[kind] = {}
        for name, declaration in source[kind].items():
            variant = declaration["variants"].get(target)
            if variant is None:
                unavailable.add((kind, name))
            potential[kind][name] = {
                **{key: value for key, value in declaration.items() if key != "variants"},
                **(variant or {}), "platform": target,
            }
    names = required_input_names(potential, consumer)
    effective: dict[str, Any] = {"schema": 2, "platform": target}
    for kind, selected in zip(("images", "artifacts", "derived_images"), names):
        effective[kind] = {}
        for name in sorted(selected):
            if (kind, name) in unavailable:
                raise FixtureInputError(f"unsupported fixture input platform: {kind}/{name} has no {target} variant")
            effective[kind][name] = dict(potential[kind][name])
    identity = sha256_bytes(canonical_bytes({"source_lock": source, "platform": target}))
    return effective, identity


def load_lock(
    path: Path, *, platform: str | None = None, consumer: str = "all"
) -> tuple[dict[str, Any], str]:
    return resolve_lock(read_source_lock(path), platform or native_platform(), consumer=consumer)


# A consumer declares its runtime roots; build dependencies come from the lock.
CONSUMER_INPUT_ROOTS = {
    "object-store": (("derived_images", "minio"), ("derived_images", "minio-mc")),
    "iceberg-rest": (
        ("derived_images", "minio"),
        ("derived_images", "minio-mc"),
        ("images", "iceberg-rest"),
        ("derived_images", "iceberg-spark"),
        ("derived_images", "rest-mv"),
    ),
}


def required_input_names(
    lock: Mapping[str, Any], consumer: str
) -> tuple[set[str], set[str], set[str]]:
    """Resolve a closed consumer's typed input dependency closure."""
    if consumer == "all":
        return set(lock["images"]), set(lock["artifacts"]), set(lock["derived_images"])
    roots = CONSUMER_INPUT_ROOTS.get(consumer)
    for prefix, kind in (("derived:", "derived_images"), ("image:", "images"), ("artifact:", "artifacts")):
        if consumer.startswith(prefix):
            roots = ((kind, consumer.removeprefix(prefix)),)
            break
    if roots is None:
        raise FixtureInputError(f"unknown fixture input consumer: {consumer}")
    selected: dict[str, set[str]] = {
        "images": set(), "artifacts": set(), "derived_images": set()
    }

    def visit(kind: str, name: str) -> None:
        if not isinstance(name, str) or not isinstance(lock[kind].get(name), dict):
            raise FixtureInputError(f"fixture consumer input is missing: {kind}/{name}")
        if name in selected[kind]:
            return
        selected[kind].add(name)
        if kind == "derived_images":
            item = lock[kind][name]
            # Every named base is an image, not another derived image.
            bases = item.get("bases")
            if not isinstance(bases, dict) or not bases:
                raise FixtureInputError(f"fixture consumer base dependencies are missing: {name}")
            for base in bases.values():
                visit("images", base)
            dependencies = item.get("artifacts")
            if not isinstance(dependencies, list):
                raise FixtureInputError(f"fixture consumer artifact dependencies are missing: {name}")
            for artifact in dependencies:
                visit("artifacts", artifact)

    for kind, name in roots:
        visit(kind, name)
    return selected["images"], selected["artifacts"], selected["derived_images"]


def fixture_store(value: str | None) -> Path:
    if value:
        return Path(value).expanduser().resolve()
    cache_root = os.environ.get("XDG_CACHE_HOME")
    if cache_root:
        return (Path(cache_root) / "novarocks" / "fixture-inputs").resolve()
    return (Path.home() / ".cache" / "novarocks" / "fixture-inputs").resolve()


@contextmanager
def fixture_store_lock(
    store: Path, *, exclusive: bool, create: bool = False
) -> Any:
    """Serialize provision publication and local verification for one store.

    Verify intentionally opens an existing lock file read-only: a missing store
    remains a prerequisite result rather than a side effect of verification.
    """
    lock_path = store / ".provision.lock"
    try:
        lock_file = lock_path.open("a+" if create else "r")
    except OSError as error:
        raise FixtureInputError(f"cannot acquire fixture store lock: {lock_path}") from error
    with lock_file:
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        try:
            fcntl.flock(lock_file.fileno(), operation)
        except OSError as error:
            raise FixtureInputError(f"cannot acquire fixture store lock: {lock_path}") from error
        # The protected body's failures belong to its owner, not lock acquisition.
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def require_relative(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise FixtureInputError(f"unsafe relative fixture path: {path}")
    return candidate


def definition_sha256(repo_root: Path, paths: list[str]) -> str:
    digest = hashlib.sha256()
    for name in paths:
        relative = require_relative(name)
        path = repo_root / relative
        if not path.is_file():
            raise FixtureInputError(f"fixture definition input is missing: {relative}")
        digest.update(str(relative).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def run(
    command: list[str], *, capture: bool = False, timeout_seconds: int | None = None
) -> str:
    try:
        result = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise FixtureInputError(
            f"command timed out after {timeout_seconds}s: {' '.join(command)}"
        ) from error
    except OSError as error:
        raise FixtureInputError(f"cannot start fixture command {command[0]}: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip() if capture else ""
        raise FixtureInputError(f"command failed ({result.returncode}): {' '.join(command)} {detail}".strip())
    return result.stdout if capture else ""


def inspect_image(reference: str) -> Mapping[str, Any]:
    output = run(["docker", "image", "inspect", reference, "--format", "{{json .}}"], capture=True)
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise FixtureInputError(f"cannot parse docker image inspect output for {reference}") from error
    if not isinstance(payload, dict):
        raise FixtureInputError(f"unexpected docker image inspect output for {reference}")
    return payload


def image_platform(info: Mapping[str, Any]) -> str:
    result = f"{info.get('Os')}/{info.get('Architecture')}"
    if info.get("Variant"):
        result += f"/{info['Variant']}"
    return result


def verify_image(info: Mapping[str, Any], item: Mapping[str, Any], *, derived: bool = False) -> None:
    expected_platform = item["platform"]
    actual_platform = image_platform(info)
    # Docker may report the default ARM64 variant even when the locked
    # platform and the pull request both spell it as linux/arm64.
    if actual_platform != expected_platform and not (
        expected_platform == "linux/arm64" and actual_platform == "linux/arm64/v8"
    ):
        raise FixtureInputError(
            f"fixture image platform mismatch: expected {expected_platform}, found {actual_platform}"
        )
    if derived:
        return
    expected_digest = item["manifest_digest"]
    repo_digests = info.get("RepoDigests") or []
    if info.get("Id") != expected_digest and not any(
        isinstance(value, str) and value.endswith(f"@{expected_digest}") for value in repo_digests
    ):
        raise FixtureInputError(f"fixture image manifest mismatch: expected {expected_digest}")


def artifact_entry(path: Path) -> dict[str, Any]:
    return {"bytes": path.stat().st_size, "sha1": sha1_file(path), "sha256": sha256_file(path)}


def validate_artifact(path: Path, item: Mapping[str, Any]) -> dict[str, Any]:
    if not path.is_file():
        raise FixtureInputError(f"fixture artifact is missing: {path.name}")
    actual = artifact_entry(path)
    if (actual["bytes"] != item["bytes"] or actual["sha1"] != item["sha1"]
            or ("sha256" in item and actual["sha256"] != item["sha256"])):
        raise FixtureInputError(f"fixture artifact integrity mismatch: {path.name}")
    return actual
