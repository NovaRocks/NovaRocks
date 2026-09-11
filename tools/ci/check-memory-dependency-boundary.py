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

"""Verify the memory core / Arrow adapter Cargo dependency boundary.

The memory domain is split in two on purpose:

* ``novarocks-memory`` is the neutral memory core.  It carries *no*
  dependencies at all -- no Arrow, no SQL, no query context, no scheduler, no
  async runtime.  "Zero dependencies" is the contract, not an accident of the
  current code: a capacity or ownership fact that cannot be stated without
  naming a runtime does not belong in the core.
* ``novarocks-memory-arrow`` is the Arrow adapter.  It is the single place
  where a memory fact meets an Arrow buffer, and it is the only reason Arrow
  appears anywhere in the memory domain.

The guard checks the *resolved dependency graph*, never source text.  Per
ADR-0058 a guard protects a real dependency boundary; a scanner that describes
the current file layout goes stale silently when directories move.

``novarocks-memory-arrow`` is treated as **optional**.  The core landed first;
the adapter arrives in a later change on the same branch.  When the package is
absent from ``cargo metadata`` its rules are skipped and the guard still
passes, so the gate is useful from the moment the core exists.

Five assertions are enforced:

1. ``novarocks-memory``'s resolved normal dependency closure is empty, and it
   declares no normal dependency either.
2. ``novarocks-memory``'s dev dependency closure contains no ``novarocks-*``
   package, no ``arrow*`` package, and no ``tokio``.
3. When the adapter exists, no first-party (``novarocks-*``) package other than
   ``novarocks-memory`` appears in its resolved normal closure.
4. Neither memory crate appears in ``novarocks-state-store-api``'s normal
   closure -- that contract is byte-oriented and its own guard forbids Arrow,
   which the adapter would drag in.
5. No first-party package reaches both ``novarocks-memory-arrow`` and the
   retired ``novarocks-connector-starrocks`` through its normal closure.

Both a declared-table scan and a resolved-graph walk are used where they
differ.  ``cargo metadata`` resolves workspace members with their *default*
features, so an optional dependency parked behind a non-default feature is
invisible in the resolve graph.  The declared scan sees those edges; the
resolved walk sees transitive reachability.  Neither alone is sufficient.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


# The neutral memory core. Required: this guard exists because it landed.
MEMORY_CORE = "novarocks-memory"
# The Arrow adapter. Optional: a later change on this branch creates it.
MEMORY_ARROW = "novarocks-memory-arrow"
# The byte-oriented storage contract, which must stay Arrow-free.
STATE_STORE_API = "novarocks-state-store-api"
# The retired reference connector. Optional: it may simply be deleted one day.
RETIRED_CONNECTOR = "novarocks-connector-starrocks"

FIRST_PARTY_PREFIX = "novarocks-"

# The only first-party package the Arrow adapter's production closure may
# contain. This is the whole point of the split: the adapter knows the core and
# Arrow, and nothing else in the workspace.
ADAPTER_ALLOWED_FIRST_PARTY = frozenset({MEMORY_CORE})


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


# Columnar execution. Legal in the adapter by construction; illegal in the
# core, and illegal in the core's dev closure too -- a test that needs an Arrow
# buffer to say something about the core is a test that belongs to the adapter.
COLUMNAR_RUNTIME = Capability(
    "columnar runtime",
    exact={"arrow"},
    prefixes=("arrow-",),
)
# Async runtime. `tokio` by exact name is sufficient: every `tokio-*` helper
# crate carries `tokio` in its own normal closure, so the closure walk catches
# them without this guard enumerating the family.
ASYNC_RUNTIME = Capability("async runtime", exact={"tokio"})
# Any workspace crate at all. The core is a leaf of the first-party graph.
FIRST_PARTY = Capability("first-party crate", prefixes=(FIRST_PARTY_PREFIX,))

# Assertion 2's forbidden set for the core's dev closure.
CORE_DEV_FORBIDDEN_CAPABILITIES = (
    FIRST_PARTY,
    COLUMNAR_RUNTIME,
    ASYNC_RUNTIME,
)

NORMAL = None
DEV = "dev"
BUILD = "build"
REPORTED_KINDS = (
    (NORMAL, "normal", "production, enforced"),
    (DEV, "dev", "test-only, enforced for the core"),
    (BUILD, "build", "test-only, reported"),
)


def fail(message):
    print(f"memory dependency boundary violation: {message}", file=sys.stderr)
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

    def normal_reach(self, root_name):
        """The root plus everything its normal closure reaches.

        Used for co-presence questions ("does anything pull in both X and Y"),
        where the root itself being one of the two packages counts.
        """

        return self.closure(root_name, NORMAL) | {root_name}


def declared_dependency_names(package, kind):
    """Declared dependencies of one kind, optional ones included.

    ``cargo metadata`` resolves default features, so an optional dependency
    behind a non-default feature never appears in the resolve graph.  Declaring
    one is still an edge somebody can turn on.
    """

    return {
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["kind"] == kind
    }


def capability_hits(names, capabilities):
    """Return ``[(label, [names...])]`` for every capability the set hits."""

    found = []
    for capability in capabilities:
        hits = capability.hits(names)
        if hits:
            found.append((capability.label, hits))
    return found


def describe_hits(hits):
    if not hits:
        return "none"
    return "; ".join(f"{label} ({', '.join(names)})" for label, names in hits)


def verify_core_has_no_dependencies(graph, violations):
    """Assertion 1: the neutral memory core is a graph leaf."""

    closure = graph.closure(MEMORY_CORE, NORMAL)
    if closure:
        violations.append(
            f"{MEMORY_CORE} normal dependency closure must be empty; the neutral "
            "memory core carries no dependencies at all, but it now reaches: "
            + ", ".join(sorted(closure))
        )
    declared = declared_dependency_names(graph.package(MEMORY_CORE), NORMAL)
    if declared:
        violations.append(
            f"{MEMORY_CORE} declares normal dependencies ("
            + ", ".join(sorted(declared))
            + "); the neutral memory core declares none, optional ones included"
        )


def verify_core_dev_closure(graph, violations):
    """Assertion 2: the core's tests stay as neutral as the core."""

    closure = graph.closure(MEMORY_CORE, DEV)
    for label, hits in capability_hits(closure, CORE_DEV_FORBIDDEN_CAPABILITIES):
        violations.append(
            f"{MEMORY_CORE} dev dependency closure contains a forbidden "
            f"{label}: " + ", ".join(hits)
        )


def verify_adapter_first_party_closure(graph, violations):
    """Assertion 3: the adapter knows the core, and no other workspace crate."""

    closure = graph.closure(MEMORY_ARROW, NORMAL)
    unexpected = sorted(
        name
        for name in closure
        if name.startswith(FIRST_PARTY_PREFIX)
        and name != MEMORY_ARROW
        and name not in ADAPTER_ALLOWED_FIRST_PARTY
    )
    if unexpected:
        violations.append(
            f"{MEMORY_ARROW} normal dependency closure contains first-party "
            "crates outside the allow-list ("
            + ", ".join(sorted(ADAPTER_ALLOWED_FIRST_PARTY))
            + "): "
            + ", ".join(unexpected)
        )
    declared_first_party = sorted(
        name
        for name in declared_dependency_names(graph.package(MEMORY_ARROW), NORMAL)
        if name.startswith(FIRST_PARTY_PREFIX)
        and name not in ADAPTER_ALLOWED_FIRST_PARTY
    )
    if declared_first_party:
        violations.append(
            f"{MEMORY_ARROW} declares normal dependencies on first-party crates "
            "outside the allow-list ("
            + ", ".join(sorted(ADAPTER_ALLOWED_FIRST_PARTY))
            + "): "
            + ", ".join(declared_first_party)
        )


def verify_state_store_api_is_memory_free(graph, violations):
    """Assertion 4: the storage contract never acquires a memory crate.

    ``novarocks-state-store-api`` is byte-oriented and its own guard forbids
    Arrow.  The memory adapter pulls Arrow in, so neither memory crate may
    appear on the storage contract's production side.
    """

    memory_packages = [
        name for name in (MEMORY_CORE, MEMORY_ARROW) if graph.has_package(name)
    ]
    closure = graph.closure(STATE_STORE_API, NORMAL)
    reached = sorted(name for name in memory_packages if name in closure)
    if reached:
        violations.append(
            f"{STATE_STORE_API} normal dependency closure contains a memory "
            "crate: " + ", ".join(reached)
        )
    declared = declared_dependency_names(graph.package(STATE_STORE_API), NORMAL)
    declared_memory = sorted(name for name in memory_packages if name in declared)
    if declared_memory:
        violations.append(
            f"{STATE_STORE_API} declares a normal dependency on a memory crate: "
            + ", ".join(declared_memory)
        )


def verify_adapter_and_retired_connector_are_disjoint(graph, violations):
    """Assertion 5: no package pairs the adapter with the retired connector.

    StarRocks is a retired reference implementation with no active read
    capability.  Nothing that reaches the new memory path may reach it, so the
    retired code cannot come back through a memory consumer.
    """

    for package in graph.workspace_packages():
        reach = graph.normal_reach(package["name"])
        if MEMORY_ARROW in reach and RETIRED_CONNECTOR in reach:
            violations.append(
                f"{package['name']} reaches both {MEMORY_ARROW} and the retired "
                f"{RETIRED_CONNECTOR} through its normal dependency closure; the "
                "retired connector must stay out of every memory path"
            )


def report(graph, adapter_present, retired_connector_present):
    """Classify each memory crate's closures by dependency kind.

    Each root is reported against the capabilities that are actually forbidden
    for *it*: Arrow is a break in the core and the point of the adapter, so a
    single shared column would misread one of the two.
    """

    print("memory dependency boundary report (resolved from Cargo metadata)")
    roots = [(MEMORY_CORE, CORE_DEV_FORBIDDEN_CAPABILITIES)]
    if adapter_present:
        roots.append((MEMORY_ARROW, (FIRST_PARTY,)))
    for root, capabilities in roots:
        print(f"  {root} (forbidden here: " + ", ".join(c.label for c in capabilities) + ")")
        for kind, label, disposition in REPORTED_KINDS:
            if kind == DEV and root != MEMORY_CORE:
                # Assertion 2 is a core-only rule; say so instead of implying it.
                disposition = "test-only, reported"
            closure = graph.closure(root, kind)
            hits = capability_hits(closure, capabilities)
            if root == MEMORY_ARROW:
                hits = [
                    (label_, [name for name in names if name != MEMORY_CORE])
                    for label_, names in hits
                ]
                hits = [(label_, names) for label_, names in hits if names]
            line = (
                f"    {label:<6} ({disposition}): {len(closure)} packages; "
                f"forbidden capabilities: {describe_hits(hits)}"
            )
            if hits and kind == BUILD:
                line += " [build-only reach, reported not enforced]"
            print(line)
        if root == MEMORY_ARROW:
            declared = sorted(
                declared_dependency_names(graph.package(MEMORY_ARROW), NORMAL)
            )
            print(
                "            declared normal dependencies: "
                + (", ".join(declared) if declared else "none")
            )
    memory_in_storage = sorted(
        name
        for name in (MEMORY_CORE, MEMORY_ARROW)
        if graph.has_package(name)
        and name in graph.closure(STATE_STORE_API, NORMAL)
    )
    print(
        f"  {STATE_STORE_API}: memory crates in normal closure: "
        + (", ".join(memory_in_storage) if memory_in_storage else "none")
    )
    if not adapter_present:
        print(f"  {MEMORY_ARROW}: absent from Cargo metadata (rules skipped)")
    if not retired_connector_present:
        print(f"  {RETIRED_CONNECTOR}: absent from Cargo metadata (rule skipped)")


def default_manifest_path():
    return Path(__file__).resolve().parents[2] / "Cargo.toml"


def main():
    parser = argparse.ArgumentParser(
        description="Verify the memory core / Arrow adapter Cargo dependency boundary."
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help="workspace Cargo manifest (default: repository root Cargo.toml)",
    )
    arguments = parser.parse_args()

    manifest_path = (arguments.manifest_path or default_manifest_path()).resolve()
    graph = Graph(cargo_metadata(manifest_path))

    if not graph.has_package(MEMORY_CORE):
        fail(f"Cargo metadata must contain exactly one {MEMORY_CORE} package")
    if not graph.has_package(STATE_STORE_API):
        fail(f"Cargo metadata must contain exactly one {STATE_STORE_API} package")

    # The adapter is created by a later change; the retired connector may be
    # deleted by one. Either absence skips its rules instead of failing.
    adapter_present = graph.has_package(MEMORY_ARROW)
    retired_connector_present = graph.has_package(RETIRED_CONNECTOR)

    violations = []
    verified = []

    verify_core_has_no_dependencies(graph, violations)
    verified.append(f"{MEMORY_CORE} normal closure and declared table are empty")
    verify_core_dev_closure(graph, violations)
    verified.append(
        f"{MEMORY_CORE} dev closure is free of novarocks-*, arrow*, and tokio"
    )

    skipped = []
    if adapter_present:
        verify_adapter_first_party_closure(graph, violations)
        verified.append(
            f"{MEMORY_ARROW} normal closure names no first-party crate but "
            f"{MEMORY_CORE}"
        )
    else:
        skipped.append(
            f"{MEMORY_ARROW} is absent from Cargo metadata; its first-party "
            "closure rule was not evaluated"
        )

    verify_state_store_api_is_memory_free(graph, violations)
    verified.append(f"{STATE_STORE_API} normal closure contains no memory crate")

    if adapter_present and retired_connector_present:
        verify_adapter_and_retired_connector_are_disjoint(graph, violations)
        verified.append(
            f"no workspace package reaches both {MEMORY_ARROW} and "
            f"{RETIRED_CONNECTOR}"
        )
    else:
        missing = MEMORY_ARROW if not adapter_present else RETIRED_CONNECTOR
        skipped.append(
            f"{missing} is absent from Cargo metadata; the "
            f"{MEMORY_ARROW}/{RETIRED_CONNECTOR} disjointness rule was not "
            "evaluated"
        )

    report(graph, adapter_present, retired_connector_present)

    if violations:
        for violation in violations:
            print(
                f"memory dependency boundary violation: {violation}",
                file=sys.stderr,
            )
        raise SystemExit(1)

    print("memory dependency boundary: verified")
    for line in verified:
        print(f"  - {line}")
    for line in skipped:
        print(f"  - skipped: {line}")
    print("memory dependency boundary: PASS")


if __name__ == "__main__":
    main()
