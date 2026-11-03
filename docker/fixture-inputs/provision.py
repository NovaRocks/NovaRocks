#!/usr/bin/env python3
"""Prepare only missing fixture inputs from the project's recommended recipes."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import urllib.request
from urllib.parse import urlsplit

from fixture_inputs import (
    FixtureInputError, CONSUMER_INPUT_ROOTS, fixture_store, fixture_store_lock,
    inspect_image, load_inputs, read_recommendations, resolve_inputs,
    require_relative, run, validate_artifact,
)

SCRIPT_DIR = Path(__file__).resolve().parent


def download(url: str, output: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "NovaRocks-fixture-provision/1"})
    try:
        with urllib.request.urlopen(request, timeout=90) as source, output.open("xb") as target:
            shutil.copyfileobj(source, target)
    except OSError as error:
        raise FixtureInputError(f"fixture artifact download failed: {url}: {error}") from error


def parse_image_sources(specs: list[str], image_names: set[str]) -> dict[str, str]:
    """Parse explicit transport-only source overrides.

    The override selects the acquisition repository; the declared release and
    requested build platform remain explicit recipe inputs.
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
    for source_name, destination_name in item.get("copy_inputs", {}).items():
        source_relative = require_relative(source_name)
        destination = require_relative(destination_name)
        source = repo_root / source_relative
        if source_relative not in definitions or not source.resolve().is_relative_to(repo_root) or not source.is_file():
            raise FixtureInputError(f"fixture copy source is not declared: {source_name}")
        if destination.parts[0] == "artifacts" or destination == Path("Dockerfile"):
            raise FixtureInputError(f"fixture copy destination conflicts with build inputs: {destination_name}")
        sources.append((source, destination))
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



class InputBuilder:
    """The selected creation owns its dependencies; existing services need no builder."""

    def __init__(self, store, repo, inputs, *, image_sources=None, url_prefixes=None, timeout=180):
        self.store, self.repo, self.inputs = Path(store), Path(repo), inputs
        self.image_sources = image_sources or {}
        self.url_prefixes = url_prefixes or {}
        self.timeout = timeout
        self.outputs = {kind: {} for kind in ("images", "derived_images", "python_environments")}

    def image(self, kind, name):
        if name in self.outputs[kind]:
            return self.outputs[kind][name]
        item = self.inputs[kind][name]
        alias = item["alias"]
        info = inspect_image(alias)
        if info is None:
            if kind == "images":
                source = self.image_sources.get(name, item["source"])
                reference = f"{source}:{item['version']}"
                info = inspect_image(reference)
                if info is None:
                    run(["docker", "pull", "--platform", item["platform"], reference],
                        timeout_seconds=self.timeout)
                    info = inspect_image(reference)
                if info is None:
                    raise FixtureInputError(f"Docker did not retain the pulled image: {reference}")
                run(["docker", "tag", str(info["Id"]), alias])
            else:
                arguments = []
                for argument, base in item["bases"].items():
                    arguments += ["--build-arg", f"{argument}={self.image('images', base)['alias']}"]
                for argument, base in item.get("derived_bases", {}).items():
                    arguments += ["--build-arg", f"{argument}={self.image('derived_images', base)['alias']}"]
                for artifact in item["artifacts"]:
                    self.artifact(artifact)
                work = Path(tempfile.mkdtemp(prefix=f"{name}-", dir=self.store / "builds"))
                context = build_context(self.inputs, name, self.repo, self.store / "artifacts", work / "context")
                arguments += [argument for key, value in item.get("build_args", {}).items()
                              for argument in ("--build-arg", f"{key}={value}")]
                arguments += artifact_url_prefix_build_args(item, self.url_prefixes)
                iid = work / "image.iid"
                run(["docker", "build", "--platform", item["platform"], *arguments,
                     "--iidfile", str(iid), "--tag", alias, str(context)])
                try:
                    image_id = iid.read_text().strip()
                except OSError as error:
                    raise FixtureInputError(f"fixture build did not return its image: {name}") from error
                if not image_id:
                    raise FixtureInputError(f"fixture build returned an empty image: {name}")
                info = {"Id": image_id}
        result = {"alias": alias, "image_id": str(info["Id"]), "platform": item["platform"]}
        self.outputs[kind][name] = result
        return result

    def artifact(self, name):
        item = self.inputs["artifacts"][name]
        path = self.store / "artifacts" / require_relative(name)
        try:
            path.stat()
        except FileNotFoundError:
            path.parent.mkdir(parents=True, exist_ok=True)
            work = Path(tempfile.mkdtemp(prefix="download-", dir=self.store / "builds"))
            temporary = work / "payload"
            download(artifact_transport_url(item["url"], self.url_prefixes), temporary)
            validate_artifact(temporary, item)
            temporary.replace(path)
        except OSError as error:
            raise FixtureInputError(f"cannot read fixture artifact: {path}") from error
        # This is integrity of an actual build input, not environment admission.
        validate_artifact(path, item)
        return path

    def python(self, name):
        if name in self.outputs["python_environments"]:
            return self.outputs["python_environments"][name]
        from python_environment import prepare_environment
        item = self.inputs["python_environments"][name]
        directory = self.store / "python-environments" / require_relative(name)
        try:
            directory.stat()
        except FileNotFoundError:
            for artifact in item["artifacts"]:
                self.artifact(artifact)
            prepare_environment(item, self.inputs, self.store, directory)
        except OSError as error:
            raise FixtureInputError(f"cannot read fixture Python environment: {directory}") from error
        # A partial existing environment is not silently rebuilt.
        try:
            (directory / "bin/python").stat()
        except OSError as error:
            raise FixtureInputError(f"fixture Python interpreter is unavailable: {directory}") from error
        value = {"path": str(directory / "bin/python")}
        self.outputs["python_environments"][name] = value
        return value

    def prepare(self, consumer):
        roots = CONSUMER_INPUT_ROOTS.get(consumer)
        for prefix, kind in (("derived:", "derived_images"), ("image:", "images"),
                             ("artifact:", "artifacts"), ("python:", "python_environments")):
            if consumer.startswith(prefix):
                roots = ((kind, consumer.removeprefix(prefix)),)
                break
        if consumer == "all":
            roots = [(kind, name) for kind in self.outputs for name in self.inputs[kind]]
        if roots is None:
            raise FixtureInputError(f"unknown fixture consumer: {consumer}")
        for kind, name in roots:
            if kind == "python_environments": self.python(name)
            elif kind == "artifacts": self.artifact(name)
            else: self.image(kind, name)
        return {"platform": self.inputs["platform"], "artifact_dir": str(self.store / "artifacts"),
                "versions": self.inputs.get("versions", {}), **self.outputs}


def prepare_inputs(store, repo_root, *, consumer="iceberg-rest", platform=None,
                   versions_path=None, image_source_specs=(), artifact_url_prefix_specs=(),
                   pull_timeout_seconds=180):
    if pull_timeout_seconds < 1:
        raise FixtureInputError("--docker-pull-timeout-seconds must be positive")
    source = read_recommendations(Path(versions_path or SCRIPT_DIR / "versions.json"))
    inputs = resolve_inputs(source, platform or __import__("fixture_inputs").native_platform(), consumer=consumer)
    sources = parse_image_sources(list(image_source_specs), set(source["images"]))
    prefixes = parse_artifact_url_prefixes(list(artifact_url_prefix_specs))
    store = Path(store).resolve()
    store.mkdir(parents=True, exist_ok=True)
    with fixture_store_lock(store, exclusive=True, create=True):
        (store / "builds").mkdir(exist_ok=True)
        return InputBuilder(store, Path(repo_root), inputs, image_sources=sources,
                            url_prefixes=prefixes, timeout=pull_timeout_seconds).prepare(consumer)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store")
    parser.add_argument("--repo-root", type=Path, default=SCRIPT_DIR.parents[1])
    parser.add_argument("--versions", type=Path, default=SCRIPT_DIR / "versions.json")
    parser.add_argument("--platform")
    parser.add_argument("--consumer", default="iceberg-rest")
    parser.add_argument("--image-source", action="append", default=[])
    parser.add_argument("--artifact-url-prefix", action="append", default=[])
    parser.add_argument("--docker-pull-timeout-seconds", type=int,
                        default=int(os.environ.get("NOVA_FIXTURE_DOCKER_PULL_TIMEOUT_SECONDS", "180")))
    parser.add_argument("--context-only")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    try:
        if args.context_only:
            if args.out is None: parser.error("--context-only requires --out")
            consumer = f"derived:{args.context_only}"
            inputs = load_inputs(args.versions, platform=args.platform, consumer=consumer)
            store = fixture_store(args.store)
            store.mkdir(parents=True, exist_ok=True)
            with fixture_store_lock(store, exclusive=True, create=True):
                (store / "builds").mkdir(exist_ok=True)
                builder = InputBuilder(store, args.repo_root, inputs,
                    image_sources=parse_image_sources(args.image_source, set(inputs["images"])),
                    url_prefixes=parse_artifact_url_prefixes(args.artifact_url_prefix),
                    timeout=args.docker_pull_timeout_seconds)
                item = inputs["derived_images"][args.context_only]
                for kind, bases in (("images", item["bases"]), ("derived_images", item.get("derived_bases", {}))):
                    for base in bases.values(): builder.image(kind, base)
                for name in item["artifacts"]: builder.artifact(name)
                build_context(inputs, args.context_only, args.repo_root, store / "artifacts", args.out)
            print(args.out)
        else:
            print(json.dumps(prepare_inputs(fixture_store(args.store), args.repo_root,
                consumer=args.consumer, platform=args.platform, versions_path=args.versions,
                image_source_specs=args.image_source, artifact_url_prefix_specs=args.artifact_url_prefix,
                pull_timeout_seconds=args.docker_pull_timeout_seconds), sort_keys=True))
    except FixtureInputError as error:
        print(f"FIXTURE BUILD FAILED: {error}", file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
