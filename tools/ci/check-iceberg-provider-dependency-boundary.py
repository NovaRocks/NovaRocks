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

"""Verify the stable Cargo ownership boundary for the Iceberg connector.

This check intentionally reasons over Cargo's normal, build, and dev graphs
instead of source-file counts.  The provider may use the neutral filesystem
and SPI contracts, but it must not acquire an application, execution, role, or
other concrete-provider dependency through any dependency kind.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


PROVIDER = "novarocks-connector-iceberg"
SERVER = "novarocks-server"
FRONTEND = "novarocks-frontend"
BACKEND = "novarocks-backend"

FORBIDDEN_PROVIDER_CLOSURE = {
    "novarocks",
    "novarocks-execution",
    "novarocks-frontend",
    "novarocks-backend",
    "novarocks-proto",
    "novarocks-state-store-sqlite",
    "novarocks-connector-starrocks",
}
ALLOWED_PROVIDER_INTERNAL = {
    "novarocks-fs",
    "novarocks-spi",
    "novarocks-types",
}


def fail(message):
    print(f"Iceberg provider dependency boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def cargo_output(manifest_path, *arguments):
    command = ["cargo", *arguments, "--manifest-path", str(manifest_path)]
    try:
        return subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        raise SystemExit(error.returncode) from error


def package(metadata, name):
    matches = [item for item in metadata["packages"] if item["name"] == name]
    if len(matches) != 1:
        fail(f"Cargo metadata must contain exactly one {name} package")
    return matches[0]


def dependency_names(item, kinds):
    return {
        dependency["name"]
        for dependency in item["dependencies"]
        if dependency["kind"] in kinds
    }


def graph_names(manifest_path, package_name):
    output = cargo_output(
        manifest_path,
        "tree",
        "-p",
        package_name,
        "-e",
        "normal,build,dev",
        "--prefix",
        "none",
        "--format",
        "{p}",
    )
    names = set()
    for line in output.splitlines():
        value = line.strip().split(" ", 1)[0]
        if value:
            names.add(value.split(" v", 1)[0])
    return names


def verify_provider(manifest_path, metadata):
    provider = package(metadata, PROVIDER)
    closure = graph_names(manifest_path, PROVIDER)
    forbidden = sorted(closure & FORBIDDEN_PROVIDER_CLOSURE)
    if forbidden:
        fail("provider closure contains forbidden packages: " + ", ".join(forbidden))

    direct_internal = sorted(
        name
        for name in dependency_names(provider, {None, "build", "dev"})
        if name.startswith("novarocks-") and name not in ALLOWED_PROVIDER_INTERNAL
    )
    if direct_internal:
        fail("provider declares forbidden internal dependencies: " + ", ".join(direct_internal))


def verify_roles(metadata):
    provider_direct = {PROVIDER}
    frontend = package(metadata, FRONTEND)
    backend = package(metadata, BACKEND)
    for role_name, role in ((FRONTEND, frontend), (BACKEND, backend)):
        direct = dependency_names(role, {None, "build", "dev"})
        if direct & provider_direct:
            fail(f"{role_name} must not directly depend on {PROVIDER}")

    server = package(metadata, SERVER)
    direct = dependency_names(server, {None})
    missing = sorted({PROVIDER, "novarocks-state-store-sqlite"} - direct)
    if missing:
        fail("server composition root is missing direct dependencies: " + ", ".join(missing))


def main():
    parser = argparse.ArgumentParser(description="Verify the Iceberg provider Cargo dependency boundary.")
    parser.add_argument("--manifest-path", type=Path, required=True)
    arguments = parser.parse_args()
    manifest_path = arguments.manifest_path.resolve()
    metadata = json.loads(
        cargo_output(manifest_path, "metadata", "--format-version", "1", "--no-deps")
    )
    verify_provider(manifest_path, metadata)
    verify_roles(metadata)
    print("Iceberg provider dependency boundary: PASS")


if __name__ == "__main__":
    main()
