#!/usr/bin/env python3
"""Recommended fixture recipes and operation-local build primitives."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import platform as host_platform
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit


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


def read_recommendations(path: Path) -> dict[str, Any]:
    lock = read_json(path)
    if lock.get("schema") != 1:
        raise FixtureInputError(
            f"unsupported fixture recommendations schema: {lock.get('schema')!r}"
        )
    for key in ("images", "artifacts", "derived_images"):
        if not isinstance(lock.get(key), dict):
            raise FixtureInputError(f"fixture input lock has no {key} object")
    if "python_environments" in lock and not isinstance(lock["python_environments"], dict):
        raise FixtureInputError("fixture python environments must be an object")
    for kind in ("images", "derived_images", "python_environments"):
        for name, item in lock.get(kind, {}).items():
            if not isinstance(item, dict) or not isinstance(item.get("variants"), dict) or not item["variants"]:
                raise FixtureInputError(f"fixture platform variants are missing: {kind}/{name}")
            for target, variant in item["variants"].items():
                if normalize_platform(target) != target or not isinstance(variant, dict):
                    raise FixtureInputError(f"invalid fixture variant: {kind}/{name}/{target}")
                if variant.get("platform", target) != target:
                    raise FixtureInputError(f"fixture variant platform mismatch: {kind}/{name}/{target}")
                if kind == "images":
                    image = {**item, **variant}
                    if ("creation_check" in image
                            and image["creation_check"] != "trino-cli-version"):
                        raise FixtureInputError(f"unknown fixture creation_check: {name}/{target}")
                    if "jdbc_artifact" in image:
                        artifact_name = image["jdbc_artifact"]
                        artifact = lock["artifacts"].get(artifact_name) if isinstance(artifact_name, str) else None
                        if not isinstance(artifact, dict) or not isinstance(artifact.get("url"), str):
                            raise FixtureInputError(f"fixture JDBC artifact is missing: {name}/{target}")
                        parts = urlsplit(artifact["url"]).path.split("/")
                        if len(parts) < 3 or parts[-2] != image.get("version"):
                            raise FixtureInputError(f"fixture JDBC artifact version mismatch: {name}/{target}")
    for name, declaration in lock["derived_images"].items():
        for variant in declaration["variants"].values():
            item = {**{key: value for key, value in declaration.items() if key != "variants"}, **variant}
            if not isinstance(item, dict):
                raise FixtureInputError(f"fixture derived image must be an object: {name}")
            if "base" in item:
                raise FixtureInputError(f"fixture derived image uses unsupported base field; use bases: {name}")
            bases = item.get("bases")
            derived_bases = item.get("derived_bases", {})
            if not isinstance(bases, dict) or not isinstance(derived_bases, dict) or not (bases or derived_bases):
                raise FixtureInputError(f"fixture derived image bases must name image or derived inputs: {name}")
            if set(bases) & set(derived_bases):
                raise FixtureInputError(f"duplicate fixture base build argument: {name}")
            build_args = item.get("build_args", {})
            if (not isinstance(build_args, dict) or set(build_args) & (set(bases) | set(derived_bases))
                    or any(not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key)
                           or not isinstance(value, str) for key, value in build_args.items())):
                raise FixtureInputError(f"invalid fixture scalar build arguments: {name}")
            transport_arg = item.get("artifact_url_prefix_build_arg")
            if "artifact_url_prefix_build_arg" in item and (
                    not isinstance(transport_arg, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]*", transport_arg)
                    or transport_arg in set(bases) | set(derived_bases) | set(build_args)):
                raise FixtureInputError(f"invalid fixture artifact transport build argument: {name}")
            for argument, base in bases.items():
                if not isinstance(argument, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", argument):
                    raise FixtureInputError(f"invalid fixture base build argument: {name}/{argument}")
                if not isinstance(base, str) or not isinstance(lock["images"].get(base), dict):
                    raise FixtureInputError(f"fixture derived image base is missing: {name}/{base}")
            for argument, base in derived_bases.items():
                if not isinstance(argument, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", argument):
                    raise FixtureInputError(f"invalid fixture base build argument: {name}/{argument}")
                if not isinstance(base, str) or not isinstance(lock["derived_images"].get(base), dict):
                    raise FixtureInputError(f"fixture derived base is missing: {name}/{base}")
            if "context" in item:
                context = item["context"]
                if not isinstance(context, str) or not context:
                    raise FixtureInputError(f"fixture build context must be a repository-relative directory: {name}")
                require_relative(context)
    for name, declaration in lock.get("python_environments", {}).items():
        require_relative(name)
        for variant in declaration["variants"].values():
            item = {**{key: value for key, value in declaration.items() if key != "variants"}, **variant}
            if not isinstance(item.get("python_version"), str) or not re.fullmatch(r"[0-9]+\.[0-9]+", item["python_version"]):
                raise FixtureInputError(f"invalid fixture Python version: {name}")
            artifacts = item.get("artifacts")
            bootstrap = item.get("bootstrap_artifacts")
            if (not isinstance(artifacts, list) or not isinstance(bootstrap, list)
                    or not artifacts or not bootstrap or not set(bootstrap).issubset(artifacts)
                    or item.get("pip_artifact") not in bootstrap):
                raise FixtureInputError(f"invalid fixture Python artifact closure: {name}")
            if not isinstance(item.get("packages"), dict) or not item["packages"]:
                raise FixtureInputError(f"invalid fixture Python package versions: {name}")
            if any(not isinstance(package, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]*", package)
                   or not isinstance(version, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.!+_-]*", version)
                   for package, version in item["packages"].items()):
                raise FixtureInputError(f"invalid fixture Python package declaration: {name}")
            package_artifacts = item.get("package_artifacts")
            if (not isinstance(package_artifacts, dict) or set(package_artifacts) != set(item["packages"])
                    or set(package_artifacts.values()) != set(artifacts)):
                raise FixtureInputError(f"fixture Python package artifacts are incomplete: {name}")
            for key in ("wheelhouse",):
                value = item.get(key)
                if not isinstance(value, str) or not value:
                    raise FixtureInputError(f"missing fixture Python {key}: {name}")
                require_relative(value)
            if not isinstance(item.get("definition_files"), list) or not item["definition_files"]:
                raise FixtureInputError(f"fixture Python environment lacks definition custody: {name}")
    return lock


def resolve_inputs(
    source: Mapping[str, Any], platform: str, *, consumer: str = "all"
) -> dict[str, Any]:
    target = normalize_platform(platform)
    # Select variant facts before walking recipe dependencies. Missing variants
    # remain explicit unsupported nodes, never silently omitted from `all`.
    potential: dict[str, Any] = {"schema": 1, "platform": target, "artifacts": source["artifacts"]}
    unavailable: set[tuple[str, str]] = set()
    for kind in ("images", "derived_images", "python_environments"):
        potential[kind] = {}
        for name, declaration in source.get(kind, {}).items():
            variant = declaration["variants"].get(target)
            if variant is None:
                unavailable.add((kind, name))
            potential[kind][name] = {
                **{key: value for key, value in declaration.items() if key != "variants"},
                **(variant or {}), "platform": target,
            }
    names = selected_input_names(potential, consumer)
    effective: dict[str, Any] = {"schema": 1, "platform": target}
    for kind, selected in names.items():
        effective[kind] = {}
        for name in sorted(selected):
            if (kind, name) in unavailable:
                raise FixtureInputError(f"unsupported fixture input platform: {kind}/{name} has no {target} variant")
            effective[kind][name] = dict(potential[kind][name])
    effective["versions"] = dict(source.get("versions", {}))
    return effective


def load_inputs(
    path: Path, *, platform: str | None = None, consumer: str = "all"
) -> dict[str, Any]:
    return resolve_inputs(read_recommendations(path), platform or native_platform(), consumer=consumer)


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
    "iceberg-rest-oss": (
        ("derived_images", "iceberg-rest-oss"),
        ("derived_images", "iceberg-spark-oss"),
        ("derived_images", "rest-mv-oss"),
        ("python_environments", "oss-sdk"),
    ),
}


def required_input_names(
    lock: Mapping[str, Any], consumer: str
) -> tuple[set[str], set[str], set[str]]:
    """Return image/artifact facts from the common typed dependency traversal."""
    selected = selected_input_names(lock, consumer)
    return selected["images"], selected["artifacts"], selected["derived_images"]


def selected_input_names(lock: Mapping[str, Any], consumer: str) -> dict[str, set[str]]:
    """Resolve one closed graph, including host tools and derived image bases."""
    groups = ("images", "artifacts", "derived_images", "python_environments")
    if consumer == "all":
        roots = tuple((kind, name) for kind in groups for name in lock.get(kind, {}))
    else:
        roots = CONSUMER_INPUT_ROOTS.get(consumer)
    for prefix, kind in (("derived:", "derived_images"), ("image:", "images"), ("artifact:", "artifacts"), ("python:", "python_environments")):
        if consumer.startswith(prefix):
            roots = ((kind, consumer.removeprefix(prefix)),)
            break
    if roots is None:
        raise FixtureInputError(f"unknown fixture input consumer: {consumer}")
    selected: dict[str, set[str]] = {kind: set() for kind in groups}
    visiting: set[tuple[str, str]] = set()

    def visit(kind: str, name: str) -> None:
        if not isinstance(name, str) or not isinstance(lock.get(kind, {}).get(name), dict):
            raise FixtureInputError(f"fixture consumer input is missing: {kind}/{name}")
        if (kind, name) in visiting:
            raise FixtureInputError(f"fixture input dependency cycle: {kind}/{name}")
        if name in selected[kind]:
            return
        selected[kind].add(name)
        visiting.add((kind, name))
        if kind == "derived_images":
            item = lock[kind][name]
            bases = item.get("bases")
            derived_bases = item.get("derived_bases", {})
            if not isinstance(bases, dict) or not isinstance(derived_bases, dict) or not (bases or derived_bases):
                raise FixtureInputError(f"fixture consumer base dependencies are missing: {name}")
            for base in bases.values():
                visit("images", base)
            for base in derived_bases.values():
                visit("derived_images", base)
        if kind in {"derived_images", "python_environments"}:
            item = lock[kind][name]
            dependencies = item.get("artifacts")
            if not isinstance(dependencies, list):
                raise FixtureInputError(f"fixture consumer artifact dependencies are missing: {name}")
            for artifact in dependencies:
                visit("artifacts", artifact)
        visiting.remove((kind, name))

    for kind, name in roots:
        visit(kind, name)
    return selected


def derived_build_order(lock: Mapping[str, Any]) -> list[str]:
    """Return parents before children; closure validation rejects cycles."""
    selected_input_names(lock, "all")
    result: list[str] = []
    seen: set[str] = set()

    def visit(name: str) -> None:
        if name in seen:
            return
        for base in lock["derived_images"][name].get("derived_bases", {}).values():
            visit(base)
        seen.add(name)
        result.append(name)

    for name in lock["derived_images"]:
        visit(name)
    return result


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


def confirm_container_absent(cid: str, *, timeout_seconds: int) -> None:
    """Only an explicit Docker absence proves the owned container is removed."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise FixtureInputError(f"container still present after {timeout_seconds}s: {cid}")
        try:
            result = subprocess.run(["docker", "container", "inspect", cid],
                text=True, capture_output=True, check=False, timeout=remaining)
        except (OSError, subprocess.TimeoutExpired) as error:
            raise FixtureInputError(f"cannot confirm container removal: {cid}: {error}") from error
        if result.returncode:
            if result.stderr.strip().lower().startswith((
                    "error response from daemon: no such container:", "error: no such container:")):
                return
            raise FixtureInputError(f"cannot confirm container removal: {cid}: {result.stderr.strip()}")
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))


def run_owned_check(image_id: str, platform: str, *, timeout_seconds: int = 180, work_dir: Path | None = None
                    ) -> subprocess.CompletedProcess[str]:
    # Design: docs/adr/ADR-0231-reference-inputs-declared-once-and-bound-per-run.md
    """Retain exact container custody through client exit and confirmed removal.

    A missing cidfile after dispatch leaves creation unknown. Retain that work
    directory for investigation rather than claiming that client exit removed it.
    """
    work = Path(tempfile.mkdtemp(prefix="fixture-image-check-", dir=work_dir))
    cidfile = work / "container-id"
    command = ["docker", "run", "--rm", "--pull", "never", "--platform", platform,
               "--cidfile", str(cidfile), "--entrypoint", "trino", image_id, "--version"]
    proc = None
    primary = None
    stdout, stderr = "", ""
    previous_term = None
    previous_int = None
    pending_term = None
    handle_term = threading.current_thread() is threading.main_thread()

    def terminated(signum, frame):
        nonlocal pending_term
        pending_term = signum
        if proc is not None:
            raise FixtureInputError(f"command cancelled by {signal.Signals(signum).name}")

    if handle_term:
        previous_term = signal.signal(signal.SIGTERM, terminated)
        previous_int = signal.signal(signal.SIGINT, terminated)
    try:
        try:
            print(f"Checking fixture image creation: {' '.join(command)}", file=sys.stderr, flush=True)
            proc = subprocess.Popen(command, text=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, start_new_session=True)
            if pending_term is not None:
                terminated(pending_term, None)
            stdout, stderr = proc.communicate(timeout=timeout_seconds)
            if proc.returncode:
                primary = f"command failed ({proc.returncode})"
        except (subprocess.TimeoutExpired, KeyboardInterrupt, OSError, FixtureInputError) as error:
            primary = (f"command timed out after {timeout_seconds}s" if isinstance(error, subprocess.TimeoutExpired)
                       else f"command interrupted: {error.__class__.__name__}: {error}")
        finally:
            # Further cancellation must not interrupt custody settlement.
            if handle_term:
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                signal.signal(signal.SIGINT, signal.SIG_IGN)
            if proc is not None and proc.poll() is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                stdout, stderr = proc.communicate()
            try:
                cid = cidfile.read_text().strip() if cidfile.exists() else ""
                if cid and not re.fullmatch(r"[0-9a-f]{64}", cid):
                    raise FixtureInputError(f"invalid owned container identity: {cidfile}")
                if not cid:
                    cleanup = ("not started" if proc is None else
                               f"creation outcome unknown (no container ID); retained {work}")
                    if primary is None:
                        primary = "command returned without an owned container ID"
                else:
                    removal_error = None
                    if primary:
                        try:
                            run(["docker", "rm", "-f", cid], capture=True, timeout_seconds=timeout_seconds)
                        except FixtureInputError as error:
                            removal_error = str(error)
                    try:
                        confirm_container_absent(cid, timeout_seconds=timeout_seconds)
                    except FixtureInputError as error:
                        if primary is not None:
                            raise FixtureInputError(f"{removal_error}; {error}" if removal_error else str(error)) from error
                        primary = f"initial removal confirmation failed: {error}"
                        run(["docker", "rm", "-f", cid], capture=True, timeout_seconds=timeout_seconds)
                        confirm_container_absent(cid, timeout_seconds=timeout_seconds)
                    cleanup = f"confirmed absent: {cid}"
                    if removal_error:
                        cleanup += f"; removal command failed: {removal_error}"
            except (FixtureInputError, OSError) as error:
                cleanup = f"failed: {error}; retained {work}"
                if primary is None:
                    primary = "container removal was not confirmed"
        detail = (f"{' '.join(command)}; stdout={stdout!r}; stderr={stderr!r}; cleanup={cleanup}")
        if primary:
            raise FixtureInputError(f"{primary}: {detail}")
        cidfile.unlink()
        work.rmdir()
        print(f"Fixture image creation check exited: stdout={stdout!r}; stderr={stderr!r}; cleanup={cleanup}",
              file=sys.stderr, flush=True)
        return subprocess.CompletedProcess(command, proc.returncode, stdout, stderr)
    finally:
        if handle_term:
            signal.signal(signal.SIGTERM, previous_term)
            signal.signal(signal.SIGINT, previous_int)


def inspect_image(reference: str) -> Mapping[str, Any] | None:
    """Inspect the build target; only Docker's explicit absence permits creation."""
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", reference, "--format", "{{json .}}"],
            text=True, capture_output=True, check=False,
        )
    except OSError as error:
        raise FixtureInputError(f"cannot inspect fixture image {reference}: {error}") from error
    if result.returncode:
        detail = result.stderr.strip()
        if detail.lower().startswith(("error response from daemon: no such image:", "error: no such image:")):
            return None
        raise FixtureInputError(f"cannot inspect fixture image {reference}: {detail}")
    try:
        value = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise FixtureInputError(f"invalid Docker response for {reference}") from error
    if not isinstance(value, dict) or not value.get("Id"):
        raise FixtureInputError(f"Docker image has no identity: {reference}")
    return value


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
