#!/usr/bin/env python3
"""Shared integrity primitives for fixture input provision and verification."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
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


def load_lock(path: Path) -> tuple[dict[str, Any], str]:
    lock = read_json(path)
    if lock.get("schema") != 1:
        raise FixtureInputError(f"unsupported fixture input lock schema: {lock.get('schema')!r}")
    for key in ("images", "artifacts", "derived_images"):
        if not isinstance(lock.get(key), dict):
            raise FixtureInputError(f"fixture input lock has no {key} object")
    return lock, sha256_bytes(canonical_bytes(lock))


# A consumer declares its runtime roots; build dependencies come from the lock.
CONSUMER_INPUT_ROOTS = {
    "iceberg-rest": (
        ("images", "minio"),
        ("images", "minio-mc"),
        ("images", "iceberg-rest"),
        ("derived_images", "iceberg-spark"),
    ),
}


def required_input_names(
    lock: Mapping[str, Any], consumer: str
) -> tuple[set[str], set[str], set[str]]:
    """Resolve a closed consumer's typed input dependency closure."""
    if consumer == "all":
        return set(lock["images"]), set(lock["artifacts"]), set(lock["derived_images"])
    roots = CONSUMER_INPUT_ROOTS.get(consumer)
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
            # The provisioner defines a base as an image, not another derived image.
            visit("images", item.get("base"))
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
        with lock_path.open("a+" if create else "r") as lock_file:
            operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            fcntl.flock(lock_file.fileno(), operation)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    except OSError as error:
        raise FixtureInputError(f"cannot acquire fixture store lock: {lock_path}") from error


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
    if actual["bytes"] != item["bytes"] or actual["sha1"] != item["sha1"]:
        raise FixtureInputError(f"fixture artifact integrity mismatch: {path.name}")
    return actual
