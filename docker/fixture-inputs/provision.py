#!/usr/bin/env python3
"""Network-enabled fixture input provisioner. Never called by verify CI."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
import urllib.request
from urllib.parse import urlsplit
import uuid
from pathlib import Path
from typing import Any

from fixture_inputs import (
    FixtureInputError,
    derived_build_order,
    definition_sha256,
    fixture_store,
    fixture_store_lock,
    inspect_image,
    load_lock,
    read_source_lock,
    resolve_lock,
    native_platform,
    require_native_daemon,
    read_json,
    require_relative,
    run,
    validate_artifact,
    verify_image,
)


SCRIPT_DIR = Path(__file__).resolve().parent


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def download(url: str, output: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "NovaRocks-fixture-provision/1"})
    try:
        with urllib.request.urlopen(request, timeout=90) as response, output.open("wb") as destination:
            shutil.copyfileobj(response, destination)
    except OSError as error:
        raise FixtureInputError(f"fixture artifact download failed: {url}: {error}") from error


def parse_image_sources(specs: list[str], image_names: set[str]) -> dict[str, str]:
    """Parse explicit transport-only source overrides.

    The lock remains the identity authority: an override can choose where the
    daemon fetches bytes, but never alter a logical item, digest, or platform.
    """
    overrides: dict[str, str] = {}
    for spec in specs:
        name, separator, source = spec.partition("=")
        if not separator or not name or not source:
            raise FixtureInputError(
                "--image-source must use logical-name=transport-repository"
            )
        if name not in image_names:
            raise FixtureInputError(f"unknown fixture image override: {name}")
        if any(character.isspace() for character in source) or "@" in source:
            raise FixtureInputError(
                "fixture image transport repository must not contain whitespace or a digest"
            )
        if name in overrides:
            raise FixtureInputError(f"duplicate fixture image override: {name}")
        overrides[name] = source
    return overrides


def parse_artifact_url_prefixes(specs: list[str] | None) -> dict[str, str]:
    """Choose explicit HTTPS transports without changing locked content identity."""
    overrides: dict[str, str] = {}
    for spec in specs or []:
        if not isinstance(spec, str):
            raise FixtureInputError("--artifact-url-prefix must use ORIGINAL=TRANSPORT")
        original, separator, transport = spec.partition("=")
        if not separator:
            raise FixtureInputError("--artifact-url-prefix must use ORIGINAL=TRANSPORT")
        for prefix in (original, transport):
            try:
                parsed = urlsplit(prefix)
                valid = (prefix.startswith("https://") and prefix.endswith("/")
                         and bool(parsed.hostname) and parsed.port != 0
                         and parsed.username is None and parsed.password is None
                         and not parsed.query and not parsed.fragment
                         and not any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in prefix)
                         and not any(c in prefix for c in "?#\\"))
            except ValueError:
                valid = False
            if not valid:
                raise FixtureInputError("artifact URL prefixes require HTTPS directory URLs without userinfo, query or fragment")
        if any(original.startswith(previous) or previous.startswith(original) for previous in overrides):
            raise FixtureInputError("duplicate or overlapping artifact URL prefixes")
        overrides[original] = transport
    return overrides


def artifact_transport_url(url: str, prefixes: dict[str, str]) -> str:
    # Match exactly once against the locked URL, never against a rewritten URL.
    for original, transport in prefixes.items():
        if url.startswith(original):
            return transport + url[len(original):]
    return url


def image_reference(
    name: str, item: dict[str, Any], overrides: dict[str, str]
) -> tuple[str, str]:
    transport_source = overrides.get(name, item["source"])
    return transport_source, f"{transport_source}@{item['manifest_digest']}"


def prepare_images(
    lock: dict[str, Any], overrides: dict[str, str], pull_timeout_seconds: int
) -> dict[str, dict[str, str]]:
    receipts: dict[str, dict[str, str]] = {}
    for name, item in lock["images"].items():
        transport_source, reference = image_reference(name, item, overrides)
        try:
            info = inspect_image(reference)
            verify_image(info, item)
        except FixtureInputError:
            run(
                ["docker", "pull", "--platform", item["platform"], reference],
                timeout_seconds=pull_timeout_seconds,
            )
            info = inspect_image(reference)
        verify_image(info, item)
        run(["docker", "tag", reference, item["alias"]])
        receipts[name] = {
            "alias": item["alias"],
            "manifest_digest": item["manifest_digest"],
            "platform": item["platform"],
            "transport_source": transport_source,
        }
    return receipts


def prepare_artifacts(lock: dict[str, Any], staging: Path,
                      url_prefixes: dict[str, str] | None = None) -> dict[str, dict[str, Any]]:
    artifacts_dir = staging / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    receipts: dict[str, dict[str, Any]] = {}
    for name, item in lock["artifacts"].items():
        transport_url = artifact_transport_url(item["url"], url_prefixes or {})
        output = artifacts_dir / name
        output.parent.mkdir(parents=True, exist_ok=True)
        # A failed provision keeps its own staging. Reuse a previous transfer
        # only after validating its bytes against this exact selected input.
        # The cache is optional and grants no BOM/READY authority.
        cache = staging.parent / "downloads" if staging.name.startswith(".staging-") else None
        if cache is not None:
            algorithm = "sha256" if "sha256" in item else "sha1"
            key = f"{algorithm}-{item[algorithm]}-{item['bytes']}"
            cached = cache / key
            if cached.is_file():
                try:
                    validate_artifact(cached, item)
                except FixtureInputError:
                    pass
                else:
                    shutil.copy2(cached, output)
                    receipts[name] = validate_artifact(output, item)
                    continue
            # Earlier versions retained failed staging without a download cache.
            # Those files are candidates only, never receipts or trusted names.
            for candidate in sorted(staging.parent.glob(".staging-*/artifacts/" + name)):
                if candidate == output:
                    continue
                try:
                    validate_artifact(candidate, item)
                except FixtureInputError:
                    continue
                shutil.copy2(candidate, output)
                break
            else:
                download(transport_url, output)
        else:
            download(transport_url, output)
        receipts[name] = validate_artifact(output, item)
        if cache is not None:
            cache.mkdir(exist_ok=True)
            algorithm = "sha256" if "sha256" in item else "sha1"
            destination = cache / f"{algorithm}-{item[algorithm]}-{item['bytes']}"
            temporary = cache / (".download-" + uuid.uuid4().hex)
            shutil.copy2(output, temporary)
            validate_artifact(temporary, item)
            temporary.replace(destination)
    return receipts


def build_context(
    lock: dict[str, Any], name: str, repo_root: Path, artifacts_dir: Path, out: Path
) -> Path:
    """Assemble only declared repository files and already verified artifacts."""
    item = lock["derived_images"][name]
    repo_root = repo_root.resolve()
    definitions = {require_relative(path) for path in item["definition_files"]}
    dockerfile = require_relative(item["dockerfile"])
    if dockerfile not in definitions:
        raise FixtureInputError(f"fixture Dockerfile is not a definition input: {dockerfile}")
    sources: list[tuple[Path, Path]] = []
    if "context" in item:
        context = repo_root / require_relative(item["context"])
        if not context.resolve().is_relative_to(repo_root) or not context.is_dir():
            raise FixtureInputError(f"fixture build context is not a repository directory: {name}")
        if out.resolve().is_relative_to(context.resolve()):
            raise FixtureInputError("fixture build output must not be inside its source context")
        for path in sorted(context.rglob("*")):
            if path.is_symlink():
                raise FixtureInputError(f"fixture build context must not contain symlinks: {path.relative_to(repo_root)}")
            if path.is_dir():
                continue
            relative = path.relative_to(repo_root)
            if not path.is_file() or relative not in definitions:
                raise FixtureInputError(f"fixture context file is not a definition input: {relative}")
            destination = path.relative_to(context)
            if destination.parts[0] == "artifacts":
                raise FixtureInputError("fixture build context reserves artifacts/ for locked artifacts")
            if destination == Path("Dockerfile") and relative != dockerfile:
                raise FixtureInputError("fixture build context Dockerfile differs from the declared Dockerfile")
            sources.append((path, destination))
    source_dockerfile = repo_root / dockerfile
    if not source_dockerfile.resolve().is_relative_to(repo_root) or not source_dockerfile.is_file():
        raise FixtureInputError(f"fixture Dockerfile is missing from the repository: {dockerfile}")
    if out.exists() and (not out.is_dir() or any(out.iterdir())):
        raise FixtureInputError(f"fixture build output must be an empty directory: {out}")
    out.mkdir(parents=True, exist_ok=True)
    for source, destination in sources:
        target = out / destination
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    shutil.copy2(source_dockerfile, out / "Dockerfile")
    (out / "artifacts").mkdir()
    for artifact in item["artifacts"]:
        relative = require_relative(artifact)
        target = out / "artifacts" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(artifacts_dir / relative, target)
    return out


def artifact_url_prefix_build_args(item: dict[str, Any], url_prefixes: dict[str, str] | None) -> list[str]:
    """Only an explicitly declared recipe may consume acquisition transports."""
    argument = item.get("artifact_url_prefix_build_arg")
    if argument is None:
        return []
    payload = json.dumps(url_prefixes or {}, sort_keys=True, separators=(",", ":"))
    return ["--build-arg", f"{argument}={payload}"]


def build_derived_images(lock: dict[str, Any], repo_root: Path, staging: Path, lock_sha: str,
                         *, url_prefixes: dict[str, str] | None = None) -> dict[str, dict[str, str]]:
    receipts: dict[str, dict[str, str]] = {}
    available = dict(lock.get("_verified_derived_images", {}))
    names = set(lock.get("_build_names", lock["derived_images"]))
    for name in derived_build_order(lock):
        if name not in names:
            continue
        item = lock["derived_images"][name]
        context = build_context(lock, name, repo_root, staging / "artifacts", staging / "build-contexts" / name)
        definition = definition_sha256(repo_root, item["definition_files"])
        build_args = [argument for key, base in item["bases"].items()
                      for argument in ("--build-arg", f"{key}={lock['images'][base]['alias']}")]
        for key, base in item.get("derived_bases", {}).items():
            if base not in available:
                raise FixtureInputError(f"fixture derived base has no verified image: {name}/{base}")
            image_id = available[base]["image_id"]
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
                raise FixtureInputError(f"fixture derived base has an invalid image ID: {name}/{base}")
            # BuildKit parses a raw sha256:ID as a repository/tag. Give the
            # already verified local image a content-addressed Docker name.
            reference = "novarocks/fixture-build-base:" + image_id.removeprefix("sha256:")
            run(["docker", "tag", image_id, reference])
            info = inspect_image(reference)
            verify_image(info, lock["derived_images"][base], derived=True)
            if info.get("Id") != image_id:
                raise FixtureInputError(f"fixture derived base tag differs from its verified image: {name}/{base}")
            build_args.extend(["--build-arg", f"{key}={reference}"])
        for key, value in item.get("build_args", {}).items():
            build_args.extend(["--build-arg", f"{key}={value}"])
        build_args.extend(artifact_url_prefix_build_args(item, url_prefixes))
        iidfile = context.parent / f"{name}.iid"
        run([
            "docker", "build", "--platform", item["platform"],
            *build_args,
            "--label", f"novarocks.fixture.lock.sha256={lock_sha}",
            "--label", f"novarocks.fixture.definition.sha256={definition}",
            "--iidfile", str(iidfile), str(context),
        ])
        try:
            image_id = iidfile.read_text().strip()
        except OSError as error:
            raise FixtureInputError(f"fixture build did not produce an image ID: {name}") from error
        if not image_id:
            raise FixtureInputError(f"fixture build produced an empty image ID: {name}")
        info = inspect_image(image_id)
        verify_image(info, item, derived=True)
        if info.get("Id") != image_id:
            raise FixtureInputError(f"fixture build image ID mismatch: {name}")
        receipts[name] = {"alias": item["alias"], "platform": item["platform"], "definition_sha256": definition, "image_id": image_id}
        available[name] = receipts[name]
    return receipts


def context_only(
    repo_root: Path, lock_path: Path, name: str, out: Path,
    image_source_specs: list[str], pull_timeout_seconds: int,
    platform: str | None = None, *, artifact_url_prefix_specs: list[str] | None = None,
) -> Path:
    """Prepare one build context without reading or publishing the fixture store."""
    if pull_timeout_seconds < 1:
        raise FixtureInputError("--docker-pull-timeout-seconds must be a positive integer")
    url_prefixes = parse_artifact_url_prefixes(artifact_url_prefix_specs)
    lock, _ = load_lock(lock_path, platform=platform, consumer=f"derived:{name}")
    require_native_daemon(lock["platform"])
    item = lock["derived_images"].get(name)
    if item is None:
        raise FixtureInputError(f"unknown fixture derived image: {name}")
    if item.get("derived_bases"):
        raise FixtureInputError("context-only with derived bases is unsupported; use explicit fixture provision")
    selected = {
        "images": {base: lock["images"][base] for base in item["bases"].values()},
        "artifacts": {artifact: lock["artifacts"][artifact] for artifact in item["artifacts"]},
    }
    image_sources = parse_image_sources(image_source_specs, set(lock["images"]))
    with tempfile.TemporaryDirectory(prefix="novarocks-fixture-context-") as temporary:
        staging = Path(temporary)
        prepare_images(selected, image_sources, pull_timeout_seconds)
        if url_prefixes:
            prepare_artifacts(selected, staging, url_prefixes)
        else:
            prepare_artifacts(selected, staging)
        definition_sha256(repo_root, item["definition_files"])
        build_context(lock, name, repo_root, staging / "artifacts", out)
    print(out)
    return out


def retained_inputs(store: Path, repo_root: Path, source: dict[str, Any],
                    platform: str, lock_sha: str) -> tuple[dict[str, Any], Path | None]:
    """Retain only independently reverified closures from this lock/platform."""
    from verify import verify_contents
    retained: dict[str, Any] = {kind: {} for kind in ("images", "artifacts", "derived_images", "python_environments")}
    try:
        bom = read_json(store / "bom.json")
        if ((store / "READY").read_text().strip() != f"sha256:{lock_sha}"
                or bom.get("schema") != 2 or bom.get("lock_sha256") != lock_sha
                or bom.get("platform") != platform):
            return retained, None
        directory = store / require_relative(str(bom.get("artifact_dir", "")))
    except (FixtureInputError, OSError):
        return retained, None
    for kind, prefix in (("images", "image"), ("artifacts", "artifact"), ("derived_images", "derived"), ("python_environments", "python")):
        entries = bom.get(kind, {})
        if not isinstance(entries, dict):
            continue
        for name in entries:
            if name not in source.get(kind, {}):
                continue
            try:
                closure, _ = resolve_lock(source, platform, consumer=f"{prefix}:{name}")
                verify_contents(store, repo_root, closure, lock_sha, bom)
            except FixtureInputError:
                continue
            for category in retained:
                for dependency in closure[category]:
                    retained[category][dependency] = bom[category][dependency]
    return retained, directory


def provision(
    store: Path,
    repo_root: Path,
    lock_path: Path,
    image_source_specs: list[str],
    pull_timeout_seconds: int,
    *, consumer: str = "all", platform: str | None = None,
    artifact_url_prefix_specs: list[str] | None = None,
) -> Path:
    if pull_timeout_seconds < 1:
        raise FixtureInputError("--docker-pull-timeout-seconds must be a positive integer")
    url_prefixes = parse_artifact_url_prefixes(artifact_url_prefix_specs)
    source = read_source_lock(lock_path)
    lock, lock_sha = resolve_lock(source, platform or native_platform(), consumer=consumer)
    # Missing variants and unsupported consumers fail before touching Docker/store.
    image_sources = parse_image_sources(image_source_specs, set(source["images"]))
    require_native_daemon(lock["platform"])
    store.mkdir(parents=True, exist_ok=True)
    with fixture_store_lock(store, exclusive=True, create=True):
        retained, previous_artifacts = retained_inputs(store, repo_root, source, lock["platform"], lock_sha)
        if all(set(lock.get(kind, {})).issubset(retained[kind]) for kind in retained):
            print(store / "bom.json")
            return store / "bom.json"
        staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=store))
        try:
            artifacts_dir = staging / "artifacts"
            artifacts_dir.mkdir()
            for name in retained["artifacts"]:
                assert previous_artifacts is not None
                target = artifacts_dir / require_relative(name)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(previous_artifacts / require_relative(name), target)
                validate_artifact(artifacts_dir / name, source["artifacts"][name])
            missing = {**lock, **{kind: {name: item for name, item in lock.get(kind, {}).items()
                                              if name not in retained[kind]}
                                 for kind in retained}}
            images = {**retained["images"], **prepare_images(missing, image_sources, pull_timeout_seconds)}
            prepared_artifacts = (prepare_artifacts(missing, staging, url_prefixes) if url_prefixes
                                  else prepare_artifacts(missing, staging))
            artifacts = {**retained["artifacts"], **prepared_artifacts}
            # Recipes still need their complete base declarations; only builders
            # selected below are new, while verified retained bases stay installed.
            build_lock = {**lock, "_build_names": list(missing["derived_images"]),
                          "_verified_derived_images": retained["derived_images"]}
            prepared_derived = (build_derived_images(build_lock, repo_root, staging, lock_sha, url_prefixes=url_prefixes)
                                if url_prefixes else build_derived_images(build_lock, repo_root, staging, lock_sha))
            derived = {**retained["derived_images"], **prepared_derived}
            for name in missing["derived_images"]:
                receipt = derived[name]
                run(["docker", "tag", receipt["image_id"], receipt["alias"]])
            generation = f"generation-{uuid.uuid4().hex}"
            from python_environment import prepare_environments
            python_environments = {**retained["python_environments"],
                                   **prepare_environments(missing, repo_root, staging, generation)}
            generations = store / "generations"
            generations.mkdir(exist_ok=True)
            staging.replace(generations / generation)
            bom = {
                "schema": 2,
                "platform": lock["platform"],
                "lock_sha256": lock_sha,
                "artifact_dir": f"generations/{generation}/artifacts",
                "images": images,
                "artifacts": artifacts,
                "derived_images": derived,
                "python_environments": python_environments,
            }
            atomic_json(store / "bom.json", bom)
            ready = store / f".READY.tmp-{os.getpid()}"
            ready.write_text(f"sha256:{lock_sha}\n")
            ready.replace(store / "READY")
        except Exception:
            # Failed staging/generations are unreachable or fail verification;
            # retain them for diagnosis rather than deleting another attempt.
            raise
    print(store / "bom.json")
    return store / "bom.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store")
    parser.add_argument("--repo-root", default=SCRIPT_DIR.parents[1])
    parser.add_argument("--lock", default=SCRIPT_DIR / "lock.json")
    parser.add_argument("--platform", help="explicit native target, checked against host and Docker daemon")
    parser.add_argument("--consumer", choices=("all", "iceberg-rest", "iceberg-rest-oss", "object-store"), default="all")
    parser.add_argument("--context-only", metavar="NAME", help="prepare one derived image context without publishing a BOM")
    parser.add_argument("--out", type=Path, metavar="DIR", help="empty output directory for --context-only")
    parser.add_argument(
        "--image-source",
        action="append",
        default=[],
        metavar="LOGICAL_NAME=TRANSPORT_REPOSITORY",
        help="fetch one locked image through an explicit mirror without changing its digest",
    )
    parser.add_argument(
        "--artifact-url-prefix", action="append", default=[], metavar="ORIGINAL=TRANSPORT",
        help="rewrite a locked artifact HTTPS URL prefix for transport; repeat for distinct prefixes",
    )
    parser.add_argument(
        "--docker-pull-timeout-seconds",
        type=int,
        default=int(os.environ.get("NOVA_FIXTURE_DOCKER_PULL_TIMEOUT_SECONDS", "180")),
        help="bound one explicit Docker image transfer (default: 180)",
    )
    args = parser.parse_args()
    if bool(args.context_only) != (args.out is not None):
        parser.error("--context-only and --out must be provided together")
    try:
        if args.context_only:
            context_only(
                Path(args.repo_root).resolve(), Path(args.lock).resolve(),
                args.context_only, args.out.resolve(), args.image_source,
                args.docker_pull_timeout_seconds, args.platform,
                artifact_url_prefix_specs=args.artifact_url_prefix,
            )
            return 0
        provision(
            fixture_store(args.store),
            Path(args.repo_root).resolve(),
            Path(args.lock).resolve(),
            args.image_source,
            args.docker_pull_timeout_seconds, consumer=args.consumer, platform=args.platform,
            artifact_url_prefix_specs=args.artifact_url_prefix,
        )
    except FixtureInputError as error:
        print(f"PROVISION FAILED: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
