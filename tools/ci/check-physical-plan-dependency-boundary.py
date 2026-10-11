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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify the pure final physical-plan Cargo dependency boundary.

The final physical plan is a carrier-neutral semantic contract.  It may reuse
the neutral type, Connector and immutable constant vocabularies, but no
application, execution kernel, provider implementation, wire codec, RPC stack,
or task runtime.

The checker deliberately combines Cargo metadata with Cargo's package-selected
dependency tree:

* every declared dependency kind includes optional dependencies that default
  feature resolution does not activate, and preserves a dependency's canonical
  package name even when the crate is renamed;
* `cargo tree -p --target all` computes the resolved normal/build closure for every
  target in the physical-plan package's own feature context, so inactive target
  edges remain visible while features enabled only by unrelated workspace
  members cannot create false dependencies;
* every normal/build package is admitted by exact Cargo identity. Audited
  Arrow build scripts and proc macros have separate exact authority lists;
  repository-owned contracts may execute neither, except the type contract
  build receipt that rejects unsupported compiler/allocation source profiles;
* the repository-owned neutral contracts expose no Cargo feature or target
  variation, so optional and target-specific edges cannot hide an unaudited
  closure behind a different build configuration.

Neither source is sufficient on its own. Dependency versions belong to the
root manifest and Cargo.lock; this checker owns package authority and the
closed dependency surface, not a second version policy.
Design: ADR-0168 (docs/adr/ADR-0168-ci-dependency-guards-never-restate-versions.md)
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


PACKAGE_NAME = "novarocks-physical-plan"
TYPE_CONTRACT = "novarocks-type-contract"
CONNECTOR_CONTRACT = "novarocks-connector-contract"
CONSTANT_CONTRACT = "novarocks-constant-contract"
RESULT_CONTRACT = "novarocks-result-contract"
FUNCTION_CONTRACT = "novarocks-function-contract"

# These are allowed direct internal dependencies, not required dependencies.
# Removing one as the contract gets smaller remains legal.
DIRECT_INTERNAL_ALLOW_LIST = frozenset({TYPE_CONTRACT, CONNECTOR_CONTRACT, CONSTANT_CONTRACT, RESULT_CONTRACT, FUNCTION_CONTRACT})
DIRECT_PACKAGE_ALLOW_LIST = frozenset(
    {"arrow-schema", TYPE_CONTRACT, CONNECTOR_CONTRACT, CONSTANT_CONTRACT, RESULT_CONTRACT, FUNCTION_CONTRACT}
)

# Dependency direction is part of the architecture. The type contract is the
# lower-level vocabulary; the Connector contract may consume it, but neither
# contract may acquire physical-plan or application authority.
INTERNAL_CONTRACT_NORMAL_ALLOW_LISTS = {
    TYPE_CONTRACT: frozenset({"arrow-schema", RESULT_CONTRACT}),
    RESULT_CONTRACT: frozenset(),
    FUNCTION_CONTRACT: frozenset({TYPE_CONTRACT, CONSTANT_CONTRACT}),
    CONSTANT_CONTRACT: frozenset({"arrow-array", "arrow-buffer", "arrow-data",
                                  "arrow-schema", "arrow-cast", TYPE_CONTRACT}),
    # Complete public read/write recipes freeze exact Arrow schemas, while
    # arrays, decoding, storage and executable capabilities stay outside.
    CONNECTOR_CONTRACT: frozenset({"arrow-schema", "bytes", TYPE_CONTRACT}),
}

# This vocabulary is used only for declared-edge diagnostics. Resolved closure
# admission below uses exact Cargo package identities and must never fall back
# to these names.
CRATES_IO_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
# Audited immutable Arrow backing closure, including all target and build edges.
# Membership is permission, not a requirement that every package be selected.
EXTERNAL_PACKAGE_NAMES = frozenset(['ahash', 'android_system_properties', 'arrow-array', 'arrow-buffer', 'arrow-cast', 'arrow-data', 'arrow-ord', 'arrow-schema', 'arrow-select', 'atoi', 'base64', 'lexical-core', 'lexical-parse-float', 'lexical-parse-integer', 'lexical-util', 'lexical-write-float', 'lexical-write-integer', 'ryu', 'autocfg', 'bumpalo', 'bytes', 'cc', 'cfg-if', 'chrono', 'const-random', 'const-random-macro', 'core-foundation-sys', 'crunchy', 'find-msvc-tools', 'getrandom', 'half', 'hashbrown', 'iana-time-zone', 'iana-time-zone-haiku', 'js-sys', 'libc', 'libm', 'log', 'num-bigint', 'num-complex', 'num-integer', 'num-traits', 'once_cell', 'proc-macro2', 'quote', 'r-efi', 'rustversion', 'shlex', 'syn', 'tiny-keccak', 'unicode-ident', 'version_check', 'wasi', 'wasip2', 'wasm-bindgen', 'wasm-bindgen-macro', 'wasm-bindgen-macro-support', 'wasm-bindgen-shared', 'windows-core', 'windows-implement', 'windows-interface', 'windows-link', 'windows-result', 'windows-strings', 'wit-bindgen', 'zerocopy', 'zerocopy-derive'])
EXTERNAL_PACKAGE_SOURCES = {name: CRATES_IO_SOURCE for name in EXTERNAL_PACKAGE_NAMES}
EXTERNAL_IDENTITY_UNIQUE_NAMES = frozenset({"arrow-array", "arrow-buffer", "arrow-cast", "arrow-data", "arrow-ord", "arrow-schema", "arrow-select", "bytes"})
EXTERNAL_BUILD_TARGETS = frozenset(['ahash', 'crunchy', 'getrandom', 'iana-time-zone-haiku', 'libc', 'libm', 'num-traits', 'proc-macro2', 'quote', 'rustversion', 'tiny-keccak', 'wasm-bindgen', 'wasm-bindgen-shared', 'wit-bindgen', 'zerocopy'])
EXTERNAL_PROC_MACROS = frozenset(['const-random-macro', 'rustversion', 'wasm-bindgen-macro', 'windows-implement', 'windows-interface', 'zerocopy-derive'])
EXTERNAL_BUILD_EDGES = {
    "ahash": frozenset({"version_check"}),
    "iana-time-zone-haiku": frozenset({"cc"}),
    "num-traits": frozenset({"autocfg"}),
    "wasm-bindgen": frozenset({"rustversion"}),
}
RESOLVED_PACKAGE_ALLOW_LIST = EXTERNAL_PACKAGE_NAMES | DIRECT_INTERNAL_ALLOW_LIST

NORMAL = None


class Capability:
    """A forbidden capability, matched by exact package name or prefix."""

    def __init__(self, label, exact=(), prefixes=(), excluded=()):
        self.label = label
        self.exact = frozenset(exact)
        self.prefixes = tuple(prefixes)
        self.excluded = frozenset(excluded)

    def hits(self, names):
        return sorted(
            name
            for name in names
            if name not in self.excluded
            and (name in self.exact or name.startswith(self.prefixes))
        )


WIRE_AND_RPC = Capability(
    "wire/RPC capability",
    exact={"prost", "tonic"},
    prefixes=("prost-", "tonic-"),
)

TASK_RUNTIME = Capability(
    "task runtime capability",
    exact={"async-std", "rayon", "smol", "tokio", "mio", "async-executor",
           "hyper", "h2", "reqwest", "object_store", "opendal", "sqlx",
           "rusqlite", "rocksdb", "redb"},
    prefixes=("tokio-", "hyper-", "async-std-", "sqlx-"),
)

APPLICATION_OWNER = Capability(
    "application/execution owner",
    exact={
        "novarocks-backend",
        "novarocks-execution",
        "novarocks-frontend",
        "novarocks-server",
        "novarocks-sql",
    },
)

WIRE_OWNER = Capability(
    "Native wire owner",
    exact={
        "novarocks-plan-codec",
        "novarocks-proto-codec",
        "novarocks-proto-models",
        "novarocks-task-codec",
    },
)

PROVIDER_OR_STORAGE_OWNER = Capability(
    "provider/storage owner",
    prefixes=("novarocks-connector-", "novarocks-state-store-"),
    excluded={CONNECTOR_CONTRACT},
)

FORBIDDEN_CAPABILITIES = (
    WIRE_AND_RPC,
    TASK_RUNTIME,
    APPLICATION_OWNER,
    WIRE_OWNER,
    PROVIDER_OR_STORAGE_OWNER,
)


def fail(messages):
    for message in messages:
        print(f"physical-plan dependency boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def cargo_metadata(manifest_path):
    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--locked",
        "--offline",
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


def resolved_normal_packages(manifest_path, graph):
    """Return package-selected normal/build closure with exact Cargo identities."""

    command = [
        "cargo",
        "tree",
        "--package",
        PACKAGE_NAME,
        "--edges",
        "normal,build",
        "--target",
        "all",
        "--no-dedupe",
        "--locked",
        "--offline",
        "--prefix",
        "depth",
        "--format",
        "|{p}",
        "--manifest-path",
        str(manifest_path),
    ]
    try:
        output = subprocess.run(
            command,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        raise SystemExit(error.returncode) from error

    root = graph.workspace_package(PACKAGE_NAME)
    packages = {}
    parents = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        match = re.fullmatch(r"(\d+)\|(.*)", line.strip())
        if match is None:
            fail([f"cannot parse Cargo tree depth and package identity: {line}"])
        depth = int(match.group(1))
        label = match.group(2)
        if depth == 0:
            package = graph.package_from_tree_label(label, {root["id"]})
        else:
            parent = parents.get(depth - 1)
            if parent is None:
                fail([f"Cargo tree has no parent for depth {depth}: {line}"])
            package = graph.package_from_tree_label(
                label, {edge["pkg"] for edge in graph.resolve_nodes[parent["id"]]["deps"]
                        if any(kind["kind"] in (NORMAL, "build")
                               for kind in edge["dep_kinds"])}
            )
        parents[depth] = package
        parents = {
            candidate_depth: candidate
            for candidate_depth, candidate in parents.items()
            if candidate_depth <= depth
        }
        if depth != 0:
            packages[package["id"]] = package
    return packages


class Graph:
    def __init__(self, metadata):
        self.packages_by_id = {
            package["id"]: package for package in metadata["packages"]
        }
        self.packages_by_name = {}
        for package in metadata["packages"]:
            self.packages_by_name.setdefault(package["name"], []).append(package)
        self.workspace_members = frozenset(metadata["workspace_members"])
        self.resolve_nodes = {
            node["id"]: node for node in metadata.get("resolve", {}).get("nodes", [])
        }

    def workspace_package(self, name):
        matches = [
            package
            for package in self.packages_by_name.get(name, [])
            if package["id"] in self.workspace_members
        ]
        if len(matches) != 1:
            fail([f"Cargo workspace must contain exactly one {name} package"])
        return matches[0]

    def normal_dependency_ids(self, package_id):
        node = self.resolve_nodes.get(package_id)
        if node is None:
            fail([f"Cargo metadata resolve graph omits package: {package_id}"])
        return {
            dependency["pkg"]
            for dependency in node["deps"]
            if any(kind["kind"] is NORMAL for kind in dependency["dep_kinds"])
        }

    def package_from_tree_label(self, label, candidate_ids=None):
        match = re.fullmatch(r"(\S+) v(\S+?)(?: \((.+)\))?", label)
        if match is None:
            fail([f"cannot parse Cargo tree package identity: {label}"])
        name, version, location = match.groups()
        matches = [
            package
            for package in self.packages_by_name.get(name, [])
            if package["version"] == version
            and (candidate_ids is None or package["id"] in candidate_ids)
        ]
        if location is not None:
            location_path = Path(location)
            path_matches = [
                package
                for package in matches
                if package["source"] is None
                and Path(package["manifest_path"]).parent.resolve()
                == location_path.resolve()
            ]
            if path_matches:
                matches = path_matches
            else:
                source_matches = [
                    package
                    for package in matches
                    if package["source"] is not None
                    and location in package["source"]
                ]
                if source_matches:
                    matches = source_matches
        if len(matches) != 1:
            fail([f"Cargo tree package identity is ambiguous: {label}"])
        return matches[0]

    def external_packages(self, name, source):
        """Resolve registry authorities using each package's own version."""

        matches = []
        for package in self.packages_by_name.get(name, []):
            manifest = Path(package["manifest_path"]).resolve()
            version = package["version"]
            if (
                package["id"] == f"{source}#{name}@{version}"
                and package["source"] == source
                and manifest.name == "Cargo.toml"
                and manifest.parent.name == f"{name}-{version}"
                and len(manifest.parents) >= 4
                and manifest.parents[2].name == "src"
                and manifest.parents[3].name == "registry"
                and manifest.is_file()
            ):
                matches.append(package)
        return matches


def package_identity(package):
    """Return every Cargo field that distinguishes one package authority."""

    return (
        package["id"],
        package["source"],
        package["version"],
        str(Path(package["manifest_path"]).resolve()),
    )


def describe_package_identity(package):
    source = package["source"] if package["source"] is not None else "local"
    return (
        f"{package['name']} v{package['version']} "
        f"(id={package['id']}, source={source}, manifest={package['manifest_path']})"
    )


def resolved_package_allow_list(graph):
    """Resolve the audited authorities from Cargo's actual package identities."""
    packages = [graph.workspace_package(name) for name in DIRECT_INTERNAL_ALLOW_LIST]
    packages.extend(
        package
        for name, source in sorted(EXTERNAL_PACKAGE_SOURCES.items())
        for package in graph.external_packages(name, source)
    )
    return frozenset(package_identity(package) for package in packages)


def verify_external_identity_uniqueness(closure):
    """Allow removal, but never multiple authorities for one external name."""

    violations = []
    for name in sorted(EXTERNAL_IDENTITY_UNIQUE_NAMES):
        identities = {
            package_identity(package): package
            for package in closure.values()
            if package["name"] == name
        }
        if len(identities) > 1:
            violations.append(
                "resolved normal dependency closure contains more than one "
                f"identity for {name}: "
                + "; ".join(
                    describe_package_identity(package)
                    for _, package in sorted(identities.items(), key=lambda item: str(item[0]))
                )
            )
    return violations


def declared_dependencies_by_kind(package):
    """Return canonical package names, including optional and renamed edges."""

    dependencies = {NORMAL: set(), "dev": set(), "build": set()}
    unknown_kinds = set()
    for dependency in package["dependencies"]:
        kind = dependency["kind"]
        if kind in dependencies:
            dependencies[kind].add(dependency["name"])
        else:
            unknown_kinds.add(str(kind))
    return dependencies, unknown_kinds


def capability_violations(names, location):
    violations = []
    for capability in FORBIDDEN_CAPABILITIES:
        hits = capability.hits(names)
        if hits:
            violations.append(
                f"{location} contains forbidden {capability.label}: " + ", ".join(hits)
            )
    return violations


def verify_dependency_feature_policy(package, owner):
    violations = []
    configured_features = sorted(
        f"{dependency['name']}=[{','.join(dependency['features'])}]"
        for dependency in package["dependencies"]
        if dependency["kind"] is NORMAL and dependency["features"]
    )
    if configured_features:
        violations.append(
            f"{owner} enables dependency features, but its dependency semantics "
            "must be invariant: " + ", ".join(configured_features)
        )
    disabled_defaults = sorted(
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["kind"] is NORMAL and not dependency["uses_default_features"]
    )
    if disabled_defaults:
        violations.append(
            f"{owner} disables dependency default features, but the audited surface "
            "uses each dependency's default feature policy: "
            + ", ".join(disabled_defaults)
        )
    return violations


def verify_declared_dependencies(package):
    dependencies, unknown_kinds = declared_dependencies_by_kind(package)
    normal = dependencies[NORMAL]
    dev = dependencies["dev"]
    build = dependencies["build"]
    violations = capability_violations(normal, "declared normal dependencies")
    unexpected = sorted(normal - DIRECT_PACKAGE_ALLOW_LIST)
    if unexpected:
        violations.append(
            "declares packages outside the exact direct allow-list "
            f"({', '.join(sorted(DIRECT_PACKAGE_ALLOW_LIST))}): "
            + ", ".join(unexpected)
        )
    unexpected_internal = sorted(
        name
        for name in normal
        if name.startswith("novarocks-") and name not in DIRECT_INTERNAL_ALLOW_LIST
    )
    if unexpected_internal:
        violations.append(
            "declares internal normal dependencies outside the direct allow-list "
            f"({', '.join(sorted(DIRECT_INTERNAL_ALLOW_LIST))}): "
            + ", ".join(unexpected_internal)
        )
    if build:
        violations.extend(
            capability_violations(build, "declared build dependencies")
        )
        violations.append(
            "declares build dependencies, but the physical-plan contract permits none: "
            + ", ".join(sorted(build))
        )
    if dev - {"arrow-array"}:
        violations.extend(capability_violations(dev, "declared dev dependencies"))
        violations.append(
            "declares dev dependencies, but the physical-plan contract permits none: "
            + ", ".join(sorted(dev))
        )
    if unknown_kinds:
        violations.append(
            "Cargo metadata contains unknown dependency kinds: "
            + ", ".join(sorted(unknown_kinds))
        )
    optional = sorted(
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["optional"]
    )
    if optional:
        violations.append(
            "declares optional dependencies, but the physical-plan contract "
            "requires one closed dependency surface: " + ", ".join(optional)
        )
    targeted = sorted(
        dependency["name"]
        for dependency in package["dependencies"]
        if dependency["target"] is not None
    )
    if targeted:
        violations.append(
            "declares target-specific dependencies, but the physical-plan contract "
            "must be target invariant: " + ", ".join(targeted)
        )
    if package.get("features"):
        violations.append(
            "declares Cargo features, but the physical-plan contract requires one "
            "closed dependency surface: " + ", ".join(sorted(package["features"]))
        )
    violations.extend(verify_dependency_feature_policy(package, PACKAGE_NAME))
    return dependencies, violations


def verify_package_targets(package, owner=PACKAGE_NAME):
    violations = []
    for kind, label in (("custom-build", "custom build target"),
                        ("proc-macro", "proc-macro target")):
        targets = sorted(target["name"] for target in package.get("targets", [])
                         if kind in target.get("kind", []))
        if kind == "custom-build" and owner in {TYPE_CONTRACT, "novarocks-functions"}:
            # Source-model audits may inspect the compiler and locked sources;
            # all dependency authority remains subject to the checks below.
            audited = Path(package["manifest_path"]).parent / "build.rs"
            targets = [target["name"] for target in package.get("targets", [])
                       if kind in target.get("kind", [])
                       and Path(target["src_path"]).resolve() != audited.resolve()]
        if targets:
            violations.append(
                f"{owner} declares a {label}, but repository-owned pure contracts "
                "permit no build.rs or proc-macro authority: " + ", ".join(targets))
    return violations


def verify_external_authority(package, names=EXTERNAL_PACKAGE_NAMES,
                              build_targets=EXTERNAL_BUILD_TARGETS,
                              proc_macros=EXTERNAL_PROC_MACROS,
                              build_edges=EXTERNAL_BUILD_EDGES):
    """Admit exact registry authorities, never a same-name source replacement."""
    name, version = package["name"], package["version"]
    manifest = Path(package["manifest_path"])
    identity_ok = (name in names
                   and package["source"] == CRATES_IO_SOURCE
                   and package["id"] == f"{CRATES_IO_SOURCE}#{name}@{version}"
                   and manifest.name == "Cargo.toml"
                   and manifest.parent.name == f"{name}-{version}")
    violations = []
    if not identity_ok:
        violations.append("unaudited external package identity: "
                          + describe_package_identity(package))
    dependencies, unknown = declared_dependencies_by_kind(package)
    if unknown:
        violations.append(f"{name} has unknown dependency kinds: {sorted(unknown)}")
    unexpected_build = dependencies["build"] - build_edges.get(name, frozenset())
    if unexpected_build:
        violations.append(f"{name} declares unaudited build dependencies: "
                          + ", ".join(sorted(unexpected_build)))
    for target in package.get("targets", []):
        for kind, allow in (("custom-build", build_targets), ("proc-macro", proc_macros)):
            if kind in target.get("kind", []):
                if not identity_ok or name not in allow:
                    violations.append(f"{name} declares unaudited {kind} authority: "
                                      + target["name"])
                source = target.get("src_path")
                if source is None or not Path(source).resolve().is_relative_to(manifest.parent.resolve()):
                    violations.append(f"{name} {kind} source escapes its audited package")
    return violations


def verify_internal_contract_surface(graph):
    """Keep repository-owned neutral contracts invariant across configurations."""

    violations = []
    for name in sorted(DIRECT_INTERNAL_ALLOW_LIST):
        package = graph.workspace_package(name)
        dependencies, unknown_kinds = declared_dependencies_by_kind(package)
        normal_dependencies = dependencies[NORMAL]
        owner_allow_list = INTERNAL_CONTRACT_NORMAL_ALLOW_LISTS[name]
        unexpected = sorted(normal_dependencies - owner_allow_list)
        if unexpected:
            violations.append(
                f"{name} declares normal dependencies outside its exact owner "
                f"allow-list ({', '.join(sorted(owner_allow_list))}): "
                + ", ".join(unexpected)
            )
        optional = sorted(
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["kind"] is NORMAL and dependency["optional"]
        )
        if optional:
            violations.append(
                f"{name} declares optional normal dependencies, but neutral contract "
                "features must not alter the physical-plan closure: "
                + ", ".join(optional)
            )
        targeted = sorted(
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["kind"] is NORMAL and dependency["target"] is not None
        )
        if targeted:
            violations.append(
                f"{name} declares target-specific normal dependencies, but the "
                "physical-plan closure must be target invariant: "
                + ", ".join(targeted)
            )
        if package.get("features"):
            violations.append(
                f"{name} declares Cargo features, but the physical-plan contract "
                "requires one closed dependency surface: "
                + ", ".join(sorted(package["features"]))
            )
        violations.extend(verify_dependency_feature_policy(package, name))
        build_dependencies = dependencies["build"]
        if build_dependencies:
            violations.append(
                f"{name} declares build dependencies, but the physical-plan closure "
                "permits none: " + ", ".join(sorted(build_dependencies))
            )
        dev_dependencies = dependencies["dev"]
        dev_allow = {"arrow-array", "arrow-schema"} if name == FUNCTION_CONTRACT else set()
        if dev_dependencies - dev_allow:
            violations.extend(
                capability_violations(
                    dev_dependencies, f"{name} declared dev dependencies"
                )
            )
            violations.append(
                f"{name} declares dev dependencies, but the physical-plan closure "
                "permits none: " + ", ".join(sorted(dev_dependencies))
            )
        if unknown_kinds:
            violations.append(
                f"{name} contains unknown dependency kinds: "
                + ", ".join(sorted(unknown_kinds))
            )
        violations.extend(verify_package_targets(package, name))
    return violations


def verify_closure_declared_boundary(closure):
    """Audit build authority and target variants for every resolved package."""

    violations = []
    for package in sorted(closure.values(), key=lambda item: item["id"]):
        name = package["name"]
        dependencies, unknown_kinds = declared_dependencies_by_kind(package)
        if package["source"] is None:
            build_dependencies = dependencies["build"]
            if build_dependencies:
                violations.append(f"resolved normal dependency {name} declares build dependencies: "
                                  + ", ".join(sorted(build_dependencies)))
            violations.extend(verify_package_targets(package, name))
        else:
            violations.extend(verify_external_authority(package))
        if unknown_kinds:
            violations.append(
                f"resolved normal dependency {name} contains unknown dependency kinds: "
                + ", ".join(sorted(unknown_kinds))
            )
        targeted_normal = {
            dependency["name"]
            for dependency in package["dependencies"]
            if dependency["kind"] is NORMAL and dependency["target"] is not None
            and not dependency["optional"]
        }
        violations.extend(
            capability_violations(
                targeted_normal,
                f"resolved normal dependency {name} target-specific dependencies",
            )
        )
        unexpected_targeted = sorted(
            targeted_normal - RESOLVED_PACKAGE_ALLOW_LIST
        )
        if unexpected_targeted:
            violations.append(
                f"resolved normal dependency {name} declares target-specific normal "
                "dependencies outside the exact audited closure allow-list: "
                + ", ".join(unexpected_targeted)
            )
    return violations


def verify_resolved_closure(manifest_path, graph):
    closure = resolved_normal_packages(manifest_path, graph)
    names = {package["name"] for package in closure.values()}
    violations = capability_violations(names, "resolved normal/build dependency closure")
    allowed_identities = resolved_package_allow_list(graph)
    unexpected = sorted(
        (
            package
            for package in closure.values()
            if package_identity(package) not in allowed_identities
        ),
        key=lambda package: package["id"],
    )
    if unexpected:
        violations.append(
            "resolved normal/build dependency closure contains package identities outside "
            "the exact audited allow-list: "
            + "; ".join(describe_package_identity(package) for package in unexpected)
        )
    violations.extend(verify_external_identity_uniqueness(closure))
    return closure, violations


def default_manifest_path():
    return Path(__file__).resolve().parents[2] / "Cargo.toml"


def main():
    parser = argparse.ArgumentParser(
        description="Verify the final physical-plan Cargo dependency boundary."
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help="workspace Cargo manifest (default: repository root Cargo.toml)",
    )
    arguments = parser.parse_args()
    manifest_path = (arguments.manifest_path or default_manifest_path()).resolve()

    graph = Graph(cargo_metadata(manifest_path))
    package = graph.workspace_package(PACKAGE_NAME)
    declared, declared_violations = verify_declared_dependencies(package)
    target_violations = verify_package_targets(package)
    contract_surface_violations = verify_internal_contract_surface(graph)
    closure, closure_violations = verify_resolved_closure(manifest_path, graph)
    closure_declared_violations = verify_closure_declared_boundary(closure)
    violations = (
        declared_violations
        + target_violations
        + contract_surface_violations
        + closure_violations
        + closure_declared_violations
    )
    if violations:
        fail(violations)

    closure_names = {package["name"] for package in closure.values()}
    internal = sorted(name for name in closure_names if name.startswith("novarocks-"))
    print(
        "novarocks-physical-plan declared normal dependencies: "
        + (", ".join(sorted(declared[NORMAL])) if declared[NORMAL] else "none")
    )
    print(
        f"novarocks-physical-plan resolved normal/build dependency closure: "
        f"{len(closure)} package identities; internal crates: "
        + (", ".join(internal) if internal else "none")
    )
    print("physical-plan dependency boundary: PASS")


if __name__ == "__main__":
    main()
