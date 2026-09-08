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

"""Verify the Native wire generated/codec Cargo dependency boundary.

This checker deliberately uses Cargo metadata instead of source imports.  The
boundary is about crate ownership and normal dependency reachability, which a
module-path grep cannot prove.  ``--metadata-path`` exists only for the
mutation test: production invocations must obtain the graph from Cargo.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


MODELS = "novarocks-proto-models"
PROTO = "novarocks-proto-codec"
SPI = "novarocks-spi"
TYPES = "novarocks-types"
FRONTEND = "novarocks-frontend"
BACKEND = "novarocks-backend"
SERVER = "novarocks-server"
FAILPOINT = "novarocks-failpoint"
STARROCKS = "novarocks-connector-starrocks"

PROTO_INTERNAL_NORMAL_DEPENDENCIES = {MODELS, SPI, TYPES}
ROLE_DIRECT_REQUIREMENTS = {MODELS, PROTO}
WIRE_PACKAGES = {MODELS, PROTO}

# Models and Proto are codec-layer crates.  They must not reach application,
# provider, state-store, execution, or Tonic ownership through a normal edge.
FORBIDDEN_CODEC_CLOSURE = {
    "tonic",
    "novarocks-backend",
    "novarocks-connector-starrocks",
    "novarocks-execution",
    "novarocks-frontend",
    "novarocks-server",
    "novarocks-sql",
    "novarocks-state-store-foundationdb",
    "novarocks-state-store-mysql",
    "novarocks-state-store-sqlite",
}

# These crates are below the generated/codec wire layer. Their *normal
# transitive closure* cannot acquire wire crates. Server is checked separately
# for direct dependencies because it intentionally composes the FE and BE,
# whose closures contain wire crates. StarRocks is excluded deliberately:
# its provider-owned role-binding factory depends on SPI, but it still must not
# gain application ownership.
LOWER_LAYER_ROOTS = {
    SPI,
    TYPES,
    "novarocks-sql",
    "novarocks-execution",
    "novarocks-state-store-foundationdb",
    "novarocks-state-store-mysql",
    "novarocks-state-store-sqlite",
}

STARROCKS_FORBIDDEN_CLOSURE = {FRONTEND, BACKEND, SERVER}


def fail(message):
    print(f"Native wire dependency boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def cargo_metadata(manifest_path):
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--manifest-path",
        str(manifest_path),
    ]
    try:
        return json.loads(
            subprocess.run(
                command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ).stdout
        )
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        raise SystemExit(error.returncode) from error


def package_by_name(metadata, name):
    matches = [package for package in metadata["packages"] if package["name"] == name]
    if len(matches) != 1:
        fail(f"Cargo metadata must contain exactly one {name} package")
    return matches[0]


def package_name_by_id(metadata):
    return {package["id"]: package["name"] for package in metadata["packages"]}


def normal_dependency_names(package, *, include_optional=True):
    return {
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["kind"] is None
        and (include_optional or not dependency.get("optional", False))
    }


def normal_closure(metadata, root_name):
    """Return package names reachable over resolved normal dependency edges."""

    package = package_by_name(metadata, root_name)
    resolve = metadata.get("resolve")
    if resolve is None:
        fail("Cargo metadata must include resolve nodes; do not pass --no-deps")

    nodes = {node["id"]: node for node in resolve.get("nodes", [])}
    if package["id"] not in nodes:
        fail(f"Cargo metadata resolve graph is missing {root_name}")

    names_by_id = package_name_by_id(metadata)
    visited = set()
    pending = [package["id"]]
    while pending:
        package_id = pending.pop()
        if package_id in visited:
            continue
        visited.add(package_id)
        node = nodes.get(package_id)
        if node is None:
            fail(f"Cargo metadata resolve graph is missing package id {package_id}")
        for dependency in node.get("deps", []):
            if any(kind.get("kind") is None for kind in dependency.get("dep_kinds", [])):
                pending.append(dependency["pkg"])

    missing_names = sorted(package_id for package_id in visited if package_id not in names_by_id)
    if missing_names:
        fail("Cargo metadata packages are missing resolved ids: " + ", ".join(missing_names))
    return {names_by_id[package_id] for package_id in visited}


def verify_models(metadata):
    models = package_by_name(metadata, MODELS)
    internal = sorted(
        name
        for name in normal_dependency_names(models)
        if name.startswith("novarocks-")
    )
    if internal:
        fail(
            f"{MODELS} has forbidden internal normal dependencies: "
            + ", ".join(internal)
        )


def verify_proto(metadata):
    proto = package_by_name(metadata, PROTO)
    actual = {
        name
        for name in normal_dependency_names(proto)
        if name.startswith("novarocks-")
    }
    if actual != PROTO_INTERNAL_NORMAL_DEPENDENCIES:
        missing = sorted(PROTO_INTERNAL_NORMAL_DEPENDENCIES - actual)
        unexpected = sorted(actual - PROTO_INTERNAL_NORMAL_DEPENDENCIES)
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        fail(
            f"{PROTO} internal normal dependencies must be exactly: "
            + ", ".join(sorted(PROTO_INTERNAL_NORMAL_DEPENDENCIES))
            + " ("
            + "; ".join(details)
            + ")"
        )


def verify_role_direct_dependencies(metadata):
    for role in (FRONTEND, BACKEND):
        direct = normal_dependency_names(package_by_name(metadata, role), include_optional=False)
        missing = sorted(ROLE_DIRECT_REQUIREMENTS - direct)
        if missing:
            fail(
                f"{role} must directly declare normal dependencies on: "
                + ", ".join(missing)
            )


def verify_codec_closures(metadata):
    for package_name in (MODELS, PROTO):
        forbidden = sorted(normal_closure(metadata, package_name) & FORBIDDEN_CODEC_CLOSURE)
        if forbidden:
            fail(
                f"{package_name} normal dependency closure contains forbidden packages: "
                + ", ".join(forbidden)
            )


def verify_lower_layer_closures(metadata):
    for package_name in sorted(LOWER_LAYER_ROOTS):
        forbidden = sorted(normal_closure(metadata, package_name) & WIRE_PACKAGES)
        if forbidden:
            fail(
                f"{package_name} normal dependency closure contains forbidden wire packages: "
                + ", ".join(forbidden)
            )


def verify_starrocks_provider_boundary(metadata):
    starrocks = package_by_name(metadata, STARROCKS)
    direct = normal_dependency_names(starrocks, include_optional=False)
    if SPI not in direct:
        fail(f"{STARROCKS} must directly declare normal dependency on {SPI}")

    declared_direct = normal_dependency_names(starrocks, include_optional=True)
    direct_wire = sorted(declared_direct & WIRE_PACKAGES)
    if direct_wire:
        fail(
            f"{STARROCKS} must not directly depend on wire packages: "
            + ", ".join(direct_wire)
        )

    forbidden = sorted(normal_closure(metadata, STARROCKS) & STARROCKS_FORBIDDEN_CLOSURE)
    if forbidden:
        fail(
            f"{STARROCKS} normal dependency closure contains application owners: "
            + ", ".join(forbidden)
        )


def verify_server_direct_dependencies(metadata):
    direct = normal_dependency_names(package_by_name(metadata, SERVER))
    forbidden = sorted(direct & WIRE_PACKAGES)
    if forbidden:
        fail(
            f"{SERVER} must not directly declare normal wire dependencies: "
            + ", ".join(forbidden)
        )


def verify_failpoint_typed_closure(metadata):
    failpoint = package_by_name(metadata, FAILPOINT)
    typed_features = set(failpoint.get("features", {}).get("typed", []))
    if typed_features != {"dep:novarocks-types"}:
        fail(
            f"{FAILPOINT} feature typed must be exactly: dep:novarocks-types"
        )

    forbidden = sorted(normal_closure(metadata, FAILPOINT) & (WIRE_PACKAGES | {SPI}))
    if forbidden:
        fail(
            f"{FAILPOINT} typed normal dependency closure contains forbidden packages: "
            + ", ".join(forbidden)
        )


def default_manifest_path():
    return Path(__file__).resolve().parents[2] / "Cargo.toml"


def main():
    parser = argparse.ArgumentParser(
        description="Verify the Native wire Models/Proto Cargo dependency boundary."
    )
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "--manifest-path",
        type=Path,
        help="workspace Cargo manifest (default: repository root Cargo.toml)",
    )
    input_group.add_argument(
        "--metadata-path",
        type=Path,
        help="Cargo metadata JSON fixture, reserved for mutation tests",
    )
    arguments = parser.parse_args()

    if arguments.metadata_path is not None:
        try:
            metadata = json.loads(arguments.metadata_path.read_text())
        except OSError as error:
            fail(f"cannot read metadata fixture {arguments.metadata_path}: {error}")
        except json.JSONDecodeError as error:
            fail(f"invalid metadata fixture {arguments.metadata_path}: {error}")
    else:
        metadata = cargo_metadata((arguments.manifest_path or default_manifest_path()).resolve())

    verify_models(metadata)
    verify_proto(metadata)
    verify_role_direct_dependencies(metadata)
    verify_codec_closures(metadata)
    verify_lower_layer_closures(metadata)
    verify_starrocks_provider_boundary(metadata)
    verify_server_direct_dependencies(metadata)
    verify_failpoint_typed_closure(metadata)
    print("native wire dependency boundary: PASS")


if __name__ == "__main__":
    main()
