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

"""Format first-party workspace packages without following path dependencies."""

import argparse
import json
from pathlib import Path
import subprocess


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Check without writing files")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        cwd=root,
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode:
        return result.returncode
    metadata = json.loads(result.stdout)
    packages = {package["id"]: package for package in metadata["packages"]}
    members = [packages[identity] for identity in metadata["workspace_members"]]
    if not members:
        parser.error("The Cargo workspace has no packages to format")
    if any(Path(package["manifest_path"]).is_relative_to(root / "vendor") for package in members):
        parser.error("Vendored packages must not be workspace formatting targets")
    command = ["cargo", "fmt"]
    for name in sorted(package["name"] for package in members):
        command.extend(["--package", name])
    if args.check:
        command.append("--check")
    return subprocess.run(command, cwd=root, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
