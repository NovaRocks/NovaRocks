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

"""Verify the state-store API/provider/testkit Cargo dependency boundary.

The guard protects *dependency direction* and *capability ownership*, not an
exact dependency array.  Adding a neutral third-party crate to the storage
contract is a normal engineering act and must not trip CI; acquiring a
columnar runtime, the Connector contract, an application/execution owner, or
the test-only harness is a boundary break and must.

Three semantic assertions are enforced:

1. The neutral storage contract (``novarocks-state-store-api``) and the three
   providers must not reach a forbidden capability through their *resolved
   normal* dependency closure.  Capabilities are matched by exact name and by
   prefix pattern; internal ``novarocks-*`` crates are additionally held to an
   explicit neutral allow-list so an internal crate nobody named cannot slip
   in unnoticed.
2. ``novarocks-state-store-testkit`` never enters a production closure.  No
   package in the workspace may name it on a normal (non-dev, non-build)
   dependency edge, declared or resolved.  It is a dev-dependency only.
3. The API must not depend on the testkit at all -- normal, dev, or build --
   so the contract crate cannot cycle back into its own fake.

Both a declared-table scan and a resolved-graph walk are used on purpose.
``cargo metadata`` resolves workspace members with their *default* features,
so an optional dependency parked behind a non-default feature is invisible in
the resolve graph.  The declared scan sees those edges; the resolved walk sees
transitive reachability.  Neither alone is sufficient.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


API = "novarocks-state-store-api"
TESTKIT = "novarocks-state-store-testkit"
PROVIDERS = (
    "novarocks-state-store-foundationdb",
    "novarocks-state-store-mysql",
    "novarocks-state-store-sqlite",
)
# The neutral contract plus every provider: these are the packages whose
# production closure the storage boundary is about.
PRODUCTION_ROOTS = (API,) + PROVIDERS

# Internal crates a state-store production closure may legitimately contain.
# This is an allow-list, not a required set: a root that depends on neither is
# fine.  Anything else named ``novarocks-*`` is a direction break, whether or
# not a capability rule below happens to name it.
NEUTRAL_INTERNAL_PACKAGES = frozenset({API, "novarocks-secret"})


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


# Columnar execution: storage is byte-oriented and must not acquire an Arrow
# or Parquet runtime.
COLUMNAR_RUNTIME = Capability(
    "columnar runtime",
    exact={"arrow", "parquet"},
    prefixes=("arrow-", "parquet-"),
)
# The Connector contract is a different domain owner after the state-store
# contract was split out of it.  Storage must not depend back on it.
CONNECTOR_CONTRACT = Capability("connector contract", exact={"novarocks-spi"})
# Application and execution owners sit above storage in every topology.
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
# The shared fake/behaviour suite is test scaffolding by construction.
TEST_HARNESS = Capability("test-only harness", exact={TESTKIT})

FORBIDDEN_CAPABILITIES = (
    COLUMNAR_RUNTIME,
    CONNECTOR_CONTRACT,
    APPLICATION_OWNER,
    TEST_HARNESS,
)

NORMAL = None
DEV = "dev"
BUILD = "build"
REPORTED_KINDS = (
    (NORMAL, "normal", "production, enforced"),
    (DEV, "dev", "test-only, reported"),
    (BUILD, "build", "test-only, reported"),
)


def fail(message):
    print(f"state-store dependency boundary violation: {message}", file=sys.stderr)
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


class Graph:
    """Resolved Cargo graph, indexed for closure walks."""

    def __init__(self, metadata):
        resolve = metadata.get("resolve")
        if resolve is None:
            fail("Cargo metadata must include resolve nodes; do not pass --no-deps")
        self.metadata = metadata
        self.names_by_id = {package["id"]: package["name"] for package in metadata["packages"]}
        self.nodes = {node["id"]: node for node in resolve.get("nodes", [])}
        self.packages_by_name = {}
        for package in metadata["packages"]:
            self.packages_by_name.setdefault(package["name"], []).append(package)
        self.workspace_member_ids = set(metadata.get("workspace_members", []))

    def package(self, name):
        matches = self.packages_by_name.get(name, [])
        if len(matches) != 1:
            fail(f"Cargo metadata must contain exactly one {name} package")
        return matches[0]

    def has_package(self, name):
        return len(self.packages_by_name.get(name, [])) == 1

    def workspace_packages(self):
        return [
            package
            for package in self.metadata["packages"]
            if package["id"] in self.workspace_member_ids
        ]

    def node(self, package_id):
        node = self.nodes.get(package_id)
        if node is None:
            fail(f"Cargo metadata resolve graph is missing package id {package_id}")
        return node

    def name_of(self, package_id):
        name = self.names_by_id.get(package_id)
        if name is None:
            fail(f"Cargo metadata packages are missing resolved id {package_id}")
        return name

    def closure(self, root_name, kind):
        """Names reachable from ``root_name`` over one dependency kind.

        The root's own edges are filtered to ``kind``; everything deeper is
        followed over normal edges only, because a dependency's dev- and
        build-dependencies are never compiled into the dependent.
        """

        root = self.package(root_name)
        frontier = [
            dependency["pkg"]
            for dependency in self.node(root["id"]).get("deps", [])
            if any(entry.get("kind") == kind for entry in dependency.get("dep_kinds", []))
        ]
        visited = set()
        while frontier:
            package_id = frontier.pop()
            if package_id in visited:
                continue
            visited.add(package_id)
            for dependency in self.node(package_id).get("deps", []):
                if any(
                    entry.get("kind") is NORMAL
                    for entry in dependency.get("dep_kinds", [])
                ):
                    frontier.append(dependency["pkg"])
        return {self.name_of(package_id) for package_id in visited}


def declared_dependency_names(package, kind):
    return {
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["kind"] == kind
    }


def capability_hits(names):
    """Return ``[(label, [names...])]`` for every capability the set hits."""

    found = []
    for capability in FORBIDDEN_CAPABILITIES:
        hits = capability.hits(names)
        if hits:
            found.append((capability.label, hits))
    return found


def describe_hits(hits):
    if not hits:
        return "none"
    return "; ".join(f"{label} ({', '.join(names)})" for label, names in hits)


def verify_production_closures(graph, violations):
    """Assertion 1: no forbidden capability in a production normal closure."""

    for root in PRODUCTION_ROOTS:
        closure = graph.closure(root, NORMAL)
        for label, hits in capability_hits(closure):
            violations.append(
                f"{root} normal dependency closure contains a forbidden "
                f"{label}: " + ", ".join(hits)
            )
        unexpected_internal = sorted(
            name
            for name in closure
            if name.startswith("novarocks-")
            and name != root
            and name not in NEUTRAL_INTERNAL_PACKAGES
        )
        if unexpected_internal:
            violations.append(
                f"{root} normal dependency closure contains internal crates "
                "outside the neutral allow-list ("
                + ", ".join(sorted(NEUTRAL_INTERNAL_PACKAGES))
                + "): "
                + ", ".join(unexpected_internal)
            )


def verify_declared_capability_edges(graph, violations):
    """Assertion 1 (declared half): catch feature-gated forbidden edges.

    ``cargo metadata`` resolves default features, so an optional dependency
    behind a non-default feature never appears in the resolve graph.  A
    declared normal edge naming a forbidden capability is a break regardless
    of which feature turns it on.
    """

    for root in PRODUCTION_ROOTS:
        declared = declared_dependency_names(graph.package(root), NORMAL)
        for label, hits in capability_hits(declared):
            violations.append(
                f"{root} declares a normal dependency on a forbidden "
                f"{label}: " + ", ".join(hits)
            )


def verify_testkit_is_dev_only(graph, violations):
    """Assertion 2: the testkit never rides a normal dependency edge."""

    if not graph.has_package(TESTKIT):
        violations.append(f"Cargo metadata must contain exactly one {TESTKIT} package")
        return

    for package in graph.workspace_packages():
        if TESTKIT in declared_dependency_names(package, NORMAL):
            violations.append(
                f"{package['name']} declares {TESTKIT} as a normal dependency; "
                "the testkit is dev-dependency-only and must never enter a "
                "production closure"
            )

    testkit_id = graph.package(TESTKIT)["id"]
    resolved_owners = sorted(
        graph.name_of(node_id)
        for node_id, node in graph.nodes.items()
        for dependency in node.get("deps", [])
        if dependency["pkg"] == testkit_id
        and any(entry.get("kind") is NORMAL for entry in dependency.get("dep_kinds", []))
    )
    if resolved_owners:
        violations.append(
            f"resolved normal dependency edges point at {TESTKIT} from: "
            + ", ".join(resolved_owners)
        )


def verify_api_does_not_depend_on_testkit(graph, violations):
    """Assertion 3: the contract crate never reaches its own fake."""

    api = graph.package(API)
    for kind, label, _ in REPORTED_KINDS:
        if TESTKIT in declared_dependency_names(api, kind):
            violations.append(
                f"{API} declares {TESTKIT} as a {label} dependency; the storage "
                "contract must not depend on the testkit in any dependency kind"
            )
    if TESTKIT in graph.closure(API, DEV):
        violations.append(
            f"{API} reaches {TESTKIT} through its dev dependency closure"
        )


def report(graph):
    """Assertion 4: classify every checked package by dependency kind."""

    print("state-store dependency boundary report (resolved from Cargo metadata)")
    for root in PRODUCTION_ROOTS + (TESTKIT,):
        if not graph.has_package(root):
            continue
        print(f"  {root}")
        for kind, label, disposition in REPORTED_KINDS:
            closure = graph.closure(root, kind)
            hits = capability_hits(closure)
            line = (
                f"    {label:<6} ({disposition}): {len(closure)} packages; "
                f"forbidden capabilities: {describe_hits(hits)}"
            )
            if hits and kind is not NORMAL:
                line += " [test-only reach, not a production coupling]"
            print(line)
            if kind is NORMAL:
                internal = sorted(
                    name
                    for name in closure
                    if name.startswith("novarocks-") and name != root
                )
                print(
                    "            internal crates: "
                    + (", ".join(internal) if internal else "none")
                )


def default_manifest_path():
    return Path(__file__).resolve().parents[2] / "Cargo.toml"


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Verify the state-store API/provider/testkit Cargo dependency boundary."
        )
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help="workspace Cargo manifest (default: repository root Cargo.toml)",
    )
    arguments = parser.parse_args()

    manifest_path = (arguments.manifest_path or default_manifest_path()).resolve()
    graph = Graph(cargo_metadata(manifest_path))

    violations = []
    verify_production_closures(graph, violations)
    verify_declared_capability_edges(graph, violations)
    verify_testkit_is_dev_only(graph, violations)
    verify_api_does_not_depend_on_testkit(graph, violations)

    report(graph)

    if violations:
        for violation in violations:
            print(
                f"state-store dependency boundary violation: {violation}",
                file=sys.stderr,
            )
        raise SystemExit(1)

    print("state-store dependency boundary: PASS")


if __name__ == "__main__":
    main()
