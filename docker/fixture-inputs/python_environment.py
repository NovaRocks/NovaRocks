# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Offline, content-verified Python SDK environments owned by provision."""
from __future__ import annotations

import json
from pathlib import Path
import sys

from fixture_inputs import (
    FixtureInputError, canonical_bytes, definition_sha256, require_relative,
    run, sha256_bytes, sha256_file,
)


def environment_files(directory: Path) -> dict[str, str]:
    """Bytecode caches are accelerators, never installed source authority."""
    return {
        str(path.relative_to(directory)): sha256_file(path)
        for path in sorted(directory.rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    }


def definition(repo: Path, item: dict) -> str:
    return sha256_bytes(canonical_bytes({
        "declaration": item,
        "source": definition_sha256(repo, item["definition_files"]),
    }))


def prepare_environments(lock: dict, repo: Path, staging: Path, generation: str) -> dict:
    receipts = {}
    actual_python = f"{sys.version_info.major}.{sys.version_info.minor}"
    for name, item in lock.get("python_environments", {}).items():
        if actual_python != item["python_version"]:
            raise FixtureInputError(
                f"fixture Python environment requires Python {item['python_version']}; found {actual_python}"
            )
        relative = Path("python-environments") / require_relative(name)
        directory = staging / relative
        run([sys.executable, "-I", "-m", "venv", "--without-pip", str(directory)])
        python = directory / "bin/python"
        artifacts = staging / "artifacts"
        bootstrap = [str(artifacts / require_relative(value)) for value in item["bootstrap_artifacts"]]
        pip_wheel = artifacts / require_relative(item["pip_artifact"])
        # Import the locked wheel explicitly; system/user pip configuration and
        # the base interpreter's ensurepip version do not select our inputs.
        runner = (
            "import sys,runpy; wheel=sys.argv.pop(1); sys.path.insert(0,wheel); "
            "sys.argv[0]='pip'; runpy.run_module('pip',run_name='__main__')"
        )
        run([str(python), "-I", "-c", runner, str(pip_wheel), "--isolated", "install",
             "--no-index", "--no-deps", "--no-compile", *bootstrap])
        requirements = directory.parent / (name + ".requirements.txt")
        requirements.write_text("".join(
            f"{package}=={version} --hash=sha256:{lock['artifacts'][item['package_artifacts'][package]]['sha256']}\n"
            for package, version in sorted(item["packages"].items())
        ))
        run([str(python), "-I", "-m", "pip", "--isolated", "install", "--no-index",
             "--find-links", str(artifacts / require_relative(item["wheelhouse"])),
             "--no-deps", "--no-build-isolation", "--no-compile", "--require-hashes",
             "-r", str(requirements)])
        run([str(python), "-I", "-m", "pip", "--isolated", "check"])
        versions = json.loads(run([
            str(python), "-I", "-c",
            "import importlib.metadata,json,sys; "
            "print(json.dumps({name:importlib.metadata.version(name) for name in sys.argv[1:]}))",
            *sorted(item["packages"]),
        ], capture=True))
        if versions != item["packages"]:
            raise FixtureInputError(f"fixture Python installed package mismatch: {name}")
        receipts[name] = {
            "path": str(Path("generations") / generation / relative),
            "python_version": actual_python,
            "definition_sha256": definition(repo, item),
            "packages": versions,
            "files": environment_files(directory),
        }
    return receipts


def verify_environments(store: Path, repo: Path, lock: dict, bom: dict) -> None:
    for name, item in lock.get("python_environments", {}).items():
        receipt = bom.get("python_environments", {}).get(name)
        if not isinstance(receipt, dict):
            raise FixtureInputError(f"fixture Python environment receipt is missing: {name}")
        if (receipt.get("definition_sha256") != definition(repo, item)
                or receipt.get("python_version") != item["python_version"]
                or receipt.get("packages") != item["packages"]):
            raise FixtureInputError(f"fixture Python environment definition mismatch: {name}")
        directory = store / require_relative(receipt.get("path", ""))
        if not directory.is_dir() or not (directory / "bin/python").is_file():
            raise FixtureInputError(f"fixture Python environment is absent: {name}")
        if not receipt.get("files") or environment_files(directory) != receipt["files"]:
            raise FixtureInputError(f"fixture Python environment content mismatch: {name}")
