#!/usr/bin/env bash
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

set -euo pipefail
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
python3 - "$repo_root" "$@" <<'PYTHON'
import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

repo = Path(sys.argv[1])
parser = argparse.ArgumentParser(description="Regenerate rest-mv dependency checksums without publishing fixture inputs.")
parser.add_argument("--image-source", action="append", default=[], metavar="NAME=REPOSITORY")
parser.add_argument("--artifact-url-prefix", action="append", default=[], metavar="ORIGINAL=TRANSPORT")
parser.add_argument("--docker-pull-timeout-seconds", type=int, default=180)
args = parser.parse_args(sys.argv[2:])
lock_path = repo / "docker/fixture-inputs/lock.json"
sys.path.insert(0, str(repo / "docker/fixture-inputs"))
from fixture_inputs import load_lock
from provision import parse_artifact_url_prefixes, artifact_url_prefix_build_args
prefixes = parse_artifact_url_prefixes(args.artifact_url_prefix)
lock, _ = load_lock(lock_path, consumer="derived:rest-mv")
recipe = lock["derived_images"]["rest-mv"]
with tempfile.TemporaryDirectory(prefix="novarocks-rest-mv-verification-") as temporary:
    work = Path(temporary)
    context = work / "context"
    command = [sys.executable, str(repo / "docker/fixture-inputs/provision.py"),
               "--repo-root", str(repo), "--lock", str(lock_path),
               "--context-only", "rest-mv", "--out", str(context),
               "--docker-pull-timeout-seconds", str(args.docker_pull_timeout_seconds)]
    for source in args.image_source:
        command += ["--image-source", source]
    for prefix in args.artifact_url_prefix:
        command += ["--artifact-url-prefix", prefix]
    subprocess.run(command, check=True)
    output = work / "output"
    command = ["docker", "build", "--platform", recipe["platform"],
               "--target", "verification-metadata-export",
               "--output", f"type=local,dest={output}", "--progress", "plain"]
    for argument, name in recipe["bases"].items():
        command += ["--build-arg", f"{argument}={lock['images'][name]['alias']}"]
    command += artifact_url_prefix_build_args(recipe, prefixes)
    subprocess.run(command + [str(context)], check=True)
    generated = output / "verification-metadata.xml"
    if not generated.is_file() or generated.stat().st_size == 0:
        raise RuntimeError("Build did not export dependency verification metadata")
    destination = repo / recipe["context"] / "verification-metadata.xml"
    shutil.copyfile(generated, destination)
    print(destination)
PYTHON
