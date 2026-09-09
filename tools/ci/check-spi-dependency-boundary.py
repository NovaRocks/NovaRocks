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

"""Verify the ``novarocks-spi`` Connector contract dependency boundary.

``novarocks-spi`` is the Connector contract and nothing else: the storage
contract now lives in ``novarocks-state-store-api`` and is guarded by
``check-state-store-dependency-boundary.py``.

The guard protects *dependency direction* and *capability ownership*, not an
exact dependency array.  Adding a neutral third-party crate to the Connector
contract is a normal engineering act and must not trip CI.  Acquiring an async
runtime, the storage contract, or an application/execution owner is a boundary
break and must.

Arrow is deliberately *not* forbidden here: Connector contracts are columnar
by design and own the Arrow vocabulary.  That is the difference between this
boundary and the byte-oriented storage boundary.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


PACKAGE_NAME = "novarocks-spi"

# Internal crates the Connector contract closure may legitimately contain.
# This is an allow-list, not a required set.
NEUTRAL_INTERNAL_PACKAGES = frozenset({"novarocks-secret"})


class Capability:
    """A forbidden capability, matched by exact name or name prefix."""

    def __init__(self, label, exact=(), prefixes=()):
        self.label = label
        self.exact = frozenset(exact)
        self.prefixes = tuple(prefixes)

    def matches(self, name):
        return name in self.exact or name.startswith(self.prefixes)

    def hits(self, names):
        return sorted(name for name in names if self.matches(name))


# A contract crate declares traits and types; it must not carry an executor.
# Before the state-store split, Tokio was tolerated behind the removed
# ``state-store-conformance`` feature.  That owner is gone, so the Connector
# contract owns no runtime at all.
ASYNC_RUNTIME = Capability(
    "async runtime",
    exact={"async-std", "smol", "tokio"},
)
# Storage is a separate domain owner after the state-store contract was split
# out of SPI.  The Connector contract must not depend back on it.
STORAGE_CONTRACT = Capability(
    "state-store contract",
    prefixes=("novarocks-state-store-",),
)
# Application and execution owners consume the contract; the contract must
# never consume them.
APPLICATION_OWNER = Capability(
    "application/execution owner",
    exact={
        "novarocks-backend",
        "novarocks-core",
        "novarocks-execution",
        "novarocks-frontend",
        "novarocks-server",
        "novarocks-sql",
    },
    prefixes=("novarocks-connector-",),
)

FORBIDDEN_CAPABILITIES = (ASYNC_RUNTIME, STORAGE_CONTRACT, APPLICATION_OWNER)

NORMAL = None


def fail(message):
    print(f"SPI dependency boundary violation: {message}", file=sys.stderr)
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


def package_from_metadata(metadata):
    matches = [
        package for package in metadata["packages"] if package["name"] == PACKAGE_NAME
    ]
    if len(matches) != 1:
        fail(f"Cargo metadata must contain exactly one {PACKAGE_NAME} package")
    return matches[0]


def declared_normal_dependency_names(package):
    """Declared normal dependencies, optional ones included.

    ``cargo metadata`` resolves default features, so an optional dependency
    behind a non-default feature never reaches the resolve graph.  A declared
    normal edge naming a forbidden capability is a break regardless of which
    feature turns it on.
    """

    return {
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["kind"] is NORMAL
    }


def normal_closure(metadata, package):
    """Package names reachable from ``package`` over resolved normal edges."""

    resolve = metadata.get("resolve")
    if resolve is None:
        fail("Cargo metadata must include resolve nodes; do not pass --no-deps")

    names_by_id = {entry["id"]: entry["name"] for entry in metadata["packages"]}
    nodes = {node["id"]: node for node in resolve.get("nodes", [])}
    if package["id"] not in nodes:
        fail(f"Cargo metadata resolve graph is missing {PACKAGE_NAME}")

    visited = set()
    frontier = [package["id"]]
    while frontier:
        package_id = frontier.pop()
        if package_id in visited:
            continue
        visited.add(package_id)
        node = nodes.get(package_id)
        if node is None:
            fail(f"Cargo metadata resolve graph is missing package id {package_id}")
        for dependency in node.get("deps", []):
            if any(
                entry.get("kind") is NORMAL for entry in dependency.get("dep_kinds", [])
            ):
                frontier.append(dependency["pkg"])
    visited.discard(package["id"])

    missing = sorted(package_id for package_id in visited if package_id not in names_by_id)
    if missing:
        fail("Cargo metadata packages are missing resolved ids: " + ", ".join(missing))
    return {names_by_id[package_id] for package_id in visited}


def capability_hits(names):
    found = []
    for capability in FORBIDDEN_CAPABILITIES:
        hits = capability.hits(names)
        if hits:
            found.append((capability.label, hits))
    return found


def verify_declared_boundary(package):
    declared = declared_normal_dependency_names(package)
    for label, hits in capability_hits(declared):
        fail(
            f"{PACKAGE_NAME} declares a normal dependency on a forbidden "
            f"{label}: " + ", ".join(hits)
        )


def verify_default_dependency_dag(metadata, package):
    closure = normal_closure(metadata, package)
    for label, hits in capability_hits(closure):
        fail(
            f"{PACKAGE_NAME} normal dependency closure contains a forbidden "
            f"{label}: " + ", ".join(hits)
        )

    unexpected_internal = sorted(
        name
        for name in closure
        if name.startswith("novarocks-") and name not in NEUTRAL_INTERNAL_PACKAGES
    )
    if unexpected_internal:
        fail(
            "default normal dependency DAG contains internal crates outside the "
            "neutral allow-list ("
            + ", ".join(sorted(NEUTRAL_INTERNAL_PACKAGES))
            + "): "
            + ", ".join(unexpected_internal)
        )
    return closure


def default_manifest_path():
    return Path(__file__).resolve().parents[2] / "Cargo.toml"


def main():
    parser = argparse.ArgumentParser(
        description="Verify the novarocks-spi production dependency boundary."
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help="workspace Cargo manifest (default: repository root Cargo.toml)",
    )
    arguments = parser.parse_args()
    manifest_path = (arguments.manifest_path or default_manifest_path()).resolve()

    metadata = cargo_metadata(manifest_path)
    package = package_from_metadata(metadata)
    verify_declared_boundary(package)
    closure = verify_default_dependency_dag(metadata, package)

    internal = sorted(name for name in closure if name.startswith("novarocks-"))
    print(
        f"{PACKAGE_NAME} normal dependency closure: {len(closure)} packages; "
        "internal crates: " + (", ".join(internal) if internal else "none")
    )
    print("novarocks-spi dependency boundary: PASS")


if __name__ == "__main__":
    main()
