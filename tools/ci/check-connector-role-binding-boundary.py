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

"""Verify connector role-binding ownership boundaries after SPI convergence.

Cargo metadata proves the dependency cut.  Small, named source markers prove
the final cutover removed the former FE/BE parallel typed registries; this is
not a general source-style linter.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


RETIRED_BINDING = "novarocks-connector-binding"
SPI = "novarocks-spi"
FRONTEND = "novarocks-frontend"
BACKEND = "novarocks-backend"
LEGACY_PATHS = (
    "novarocks/frontend/src/connector/typed_control_registry.rs",
    "novarocks/backend/src/connector/typed_registry.rs",
)
STARROCKS_PROVIDER_FACTORY = "novarocks/connector/starrocks/src/role_binding.rs"
SERVER_STARROCKS_FACTORY_ADAPTER = "novarocks-server/src/connector_role_binding.rs"
SPI_ROLE_CONTRACTS = (
    "novarocks/spi/src/connector/binding/role.rs",
    "novarocks/spi/src/connector/provider/role.rs",
)
REQUIRED_STARROCKS_FACTORY_MARKERS = (
    "impl ConnectorControlRoleBindingFactory for StarRocksControlRoleBindingFactory",
    "impl ConnectorExecutionRoleBindingFactory for StarRocksExecutionRoleBindingFactory",
)
LEGACY_MARKERS = (
    "InstalledReadControlRegistry",
    "resolve_read_control",
    "InstalledReadExecution",
    "InstalledWriteExecution",
    "CatalogRuntimeMaterializerSet",
    "read_execution_bundle_factories",
    "write_execution_bundle_factories",
)


def fail(message):
    print(f"Connector role binding boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def repo_root():
    return Path(__file__).resolve().parents[2]


def cargo_metadata(manifest_path):
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return json.loads(result.stdout)


def package(metadata, name):
    matches = [entry for entry in metadata["packages"] if entry["name"] == name]
    if len(matches) != 1:
        fail(f"Cargo metadata must contain exactly one {name} package")
    return matches[0]


def normal_direct_dependencies(package_entry):
    return {
        dependency["name"]
        for dependency in package_entry["dependencies"]
        if dependency["kind"] is None and not dependency.get("optional", False)
    }


def normal_closure(metadata, root_name):
    resolve = metadata.get("resolve")
    if resolve is None:
        fail("Cargo metadata must include a resolve graph")
    package_by_id = {entry["id"]: entry["name"] for entry in metadata["packages"]}
    root = package(metadata, root_name)["id"]
    nodes = {entry["id"]: entry for entry in resolve.get("nodes", [])}
    pending, visited = [root], set()
    while pending:
        package_id = pending.pop()
        if package_id in visited:
            continue
        visited.add(package_id)
        node = nodes.get(package_id)
        if node is None:
            fail(f"Cargo metadata resolve graph is missing {package_id}")
        for dependency in node.get("deps", []):
            if any(kind.get("kind") is None for kind in dependency.get("dep_kinds", [])):
                pending.append(dependency["pkg"])
    return {package_by_id[package_id] for package_id in visited}


def verify_metadata(metadata):
    if any(entry["name"] == RETIRED_BINDING for entry in metadata["packages"]):
        fail(f"retired package must be absent: {RETIRED_BINDING}")
    for role in (FRONTEND, BACKEND):
        if SPI not in normal_direct_dependencies(package(metadata, role)):
            fail(f"{role} must directly declare a normal dependency on {SPI}")


def verify_source(source_root):
    retired_manifest = source_root / "novarocks/connector-binding/Cargo.toml"
    if retired_manifest.exists():
        fail("retired connector-binding crate manifest must be removed")
    for relative in SPI_ROLE_CONTRACTS:
        if not (source_root / relative).exists():
            fail(f"SPI role-binding contract is missing: {relative}")
    for relative in LEGACY_PATHS:
        if (source_root / relative).exists():
            fail(f"legacy parallel registry must be removed: {relative}")
    for relative_root in ("novarocks/frontend/src", "novarocks/backend/src"):
        root = source_root / relative_root
        if not root.exists():
            continue
        for source in sorted(root.glob("**/*.rs")):
            text = source.read_text()
            for marker in LEGACY_MARKERS:
                if marker in text:
                    fail(f"{source.relative_to(source_root)} contains legacy marker {marker!r}")

    provider_factory = source_root / STARROCKS_PROVIDER_FACTORY
    if not provider_factory.exists():
        fail(f"provider-owned StarRocks role-binding factory is missing: {STARROCKS_PROVIDER_FACTORY}")
    provider_source = provider_factory.read_text()
    for marker in REQUIRED_STARROCKS_FACTORY_MARKERS:
        if marker not in provider_source:
            fail(
                "provider-owned StarRocks role-binding factory is incomplete: "
                f"{STARROCKS_PROVIDER_FACTORY} lacks {marker!r}"
            )

    server_adapter = source_root / SERVER_STARROCKS_FACTORY_ADAPTER
    if server_adapter.exists():
        fail(
            "Server must not define a parallel StarRocks role-binding factory adapter: "
            f"{SERVER_STARROCKS_FACTORY_ADAPTER}"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--manifest-path", type=Path)
    inputs.add_argument("--metadata-path", type=Path)
    parser.add_argument("--source-root", type=Path, default=repo_root())
    arguments = parser.parse_args()
    if arguments.metadata_path:
        try:
            metadata = json.loads(arguments.metadata_path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            fail(f"cannot read metadata fixture: {error}")
    else:
        metadata = cargo_metadata((arguments.manifest_path or repo_root() / "Cargo.toml").resolve())
    verify_metadata(metadata)
    verify_source(arguments.source_root.resolve())
    print("connector role binding boundary: PASS")


if __name__ == "__main__":
    main()
