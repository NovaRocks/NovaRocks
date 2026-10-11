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

"""Protect LocalProgram's pure data dependency closure (UEA-5D).

Inspect declared edges on repository-owned pure packages, including optional,
renamed and target-specific edges. Inspect the package-selected normal/build
closure (including this package's test support) on all targets: workspace-wide
feature unification must not give this
contract runtime authority. Cargo metadata identifies packages; Cargo tree
selects features. No source layout or retired symbol is part of this guard.

Arrow backing, arithmetic, schema and immutable function signature support are
allowed. This guard proves package ownership boundaries, not source purity;
closed static fields and independent runtime instances still require review
and behavioral tests. Exact audited registry build helpers used by Arrow are allowed, while
runtime/wire/storage capabilities in build dependencies are still rejected.
"""

import argparse
import importlib.util
import re
import subprocess
import sys
from pathlib import Path


PACKAGE_NAME = "novarocks-local-program"
PURE_OWNERS = frozenset({
    PACKAGE_NAME,
    "novarocks-connector-contract",
    "novarocks-constant-contract",
    "novarocks-execution-contract",
    "novarocks-functions",
    "novarocks-function-contract",
    "novarocks-result-contract",
    "novarocks-type-contract",
    "novarocks-types",
})
REGISTRY_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
# Additional immutable Arrow arithmetic/signature dependencies of LocalProgram.
# Constant backing and its exact build authorities are shared with the physical guard.
EXTRA_EXTERNAL_NAMES = frozenset(['aho-corasick', 'allocator-api2', 'arrow-arith', 'arrow-cast', 'arrow-ord', 'arrow-select', 'atoi', 'base64', 'bitflags', 'block-buffer', 'bytemuck', 'byteorder', 'chrono-tz', 'cpufeatures', 'crypto-common', 'datasketches', 'digest', 'displaydoc', 'foreign-types', 'foreign-types-shared', 'form_urlencoded', 'generic-array', 'getrandom', 'hex', 'icu_collections', 'icu_locale_core', 'icu_normalizer', 'icu_normalizer_data', 'icu_properties', 'icu_properties_data', 'icu_provider', 'idna', 'idna_adapter', 'itoa', 'lexical-core', 'lexical-parse-float', 'lexical-parse-integer', 'lexical-util', 'lexical-write-float', 'lexical-write-integer', 'litemap', 'md-5', 'memchr', 'openssl', 'openssl-macros', 'openssl-sys', 'percent-encoding', 'phf', 'phf_shared', 'pkg-config', 'potential_utf', 'ppv-lite86', 'r-efi', 'rand', 'rand_chacha', 'rand_core', 'regex', 'regex-automata', 'regex-syntax', 'roaring', 'ryu', 'serde', 'serde_core', 'serde_derive', 'serde_json', 'sha2', 'siphasher', 'sm3', 'smallvec', 'stable_deref_trait', 'synstructure', 'tinystr', 'twox-hash', 'typenum', 'url', 'utf8_iter', 'uuid', 'vcpkg', 'writeable', 'yoke', 'yoke-derive', 'zerofrom', 'zerofrom-derive', 'zerotrie', 'zerovec', 'zerovec-derive', 'zmij'])
EXTRA_BUILD_TARGETS = frozenset(['chrono-tz', 'generic-array', 'getrandom', 'icu_normalizer_data', 'icu_properties_data', 'openssl', 'openssl-sys', 'serde', 'serde_core', 'serde_json', 'zmij'])
EXTRA_PROC_MACROS = frozenset(['displaydoc', 'openssl-macros', 'serde_derive', 'yoke-derive', 'zerofrom-derive', 'zerovec-derive'])


# These categories name capabilities, not every package currently in the tree.
FORBIDDEN_EXACT = frozenset({
    "async-std", "smol", "tokio", "rayon", "mio", "async-executor",
    "prost", "tonic", "hyper", "h2", "reqwest", "object_store",
    "opendal", "sqlx", "rusqlite", "rocksdb", "redb",
})
FORBIDDEN_PREFIXES = (
    "tokio-", "tonic-", "prost-", "hyper-", "async-std-", "sqlx-",
)


def capability_violations(names, owner):
    hits = sorted(name for name in names if
                  (name.startswith("novarocks-") and name not in PURE_OWNERS)
                  or name in FORBIDDEN_EXACT
                  or name.startswith(FORBIDDEN_PREFIXES))
    return [f"{owner} acquires runtime/wire/provider/storage capability: "
            + ", ".join(hits)] if hits else []


def verify_package(package, workspace_ids):
    """Check authority and hidden declared edges without enforcing file counts."""
    name = package["name"]
    violations = capability_violations({name}, "selected dependency closure")
    internal = name in PURE_OWNERS
    if internal:
        if package["id"] not in workspace_ids or package["source"] is not None:
            violations.append(f"{name} is not the workspace-owned pure package")
        if name == "novarocks-result-contract" and package["dependencies"]:
            violations.append(f"{name} must remain dependency-free")
        # A feature/target variant of a repository-owned contract requires a new
        # audit. Optional dependencies cannot hide outside the selected tree.
        allowed_features = {"test-support"} if name == "novarocks-functions" else set()
        if set(package.get("features", {})) - allowed_features:
            violations.append(f"{name} exposes unaudited Cargo feature variants")
        for dependency in package["dependencies"]:
            violations.extend(capability_violations(
                {dependency["name"]}, f"{name} declared {dependency['kind'] or 'normal'} edge"))
            if dependency["kind"] == "build":
                violations.append(f"{name} declares a build dependency")
            if dependency["kind"] is None and (
                    dependency["optional"] or dependency["target"] is not None):
                violations.append(f"{name} hides a normal edge behind a feature/target: "
                                  + dependency["name"])
        if name in {"novarocks-type-contract", "novarocks-functions"}:
            # These pure owners audit their exact allocation-source/compiler
            # prerequisites in one dependency-free package-local build host.
            violations.extend(metadata_support().verify_package_targets(package, name))
        elif any("custom-build" in target["kind"] for target in package["targets"]):
            violations.append(f"{name} executes a custom build script")
        if any("proc-macro" in target["kind"] for target in package["targets"]):
            violations.append(f"{name} executes a proc-macro target")
        support = metadata_support()
        if name in support.INTERNAL_CONTRACT_NORMAL_ALLOW_LISTS:
            allowed = support.INTERNAL_CONTRACT_NORMAL_ALLOW_LISTS[name]
            unexpected = {d["name"] for d in package["dependencies"] if d["kind"] is None} - allowed
            if unexpected:
                violations.append(f"{name} declares unaudited normal edges: {sorted(unexpected)}")
            violations.extend(support.verify_dependency_feature_policy(package, name))
    else:
        support = metadata_support()
        build_edges = dict(support.EXTERNAL_BUILD_EDGES)
        build_edges["generic-array"] = frozenset({"version_check"})
        build_edges["openssl"] = frozenset({"cc"})
        build_edges["openssl-sys"] = frozenset({"cc", "pkg-config", "vcpkg", "bindgen", "openssl-src"})
        build_edges["chrono-tz"] = frozenset({"chrono-tz-build"})
        violations.extend(support.verify_external_authority(
            package, support.EXTERNAL_PACKAGE_NAMES | EXTRA_EXTERNAL_NAMES,
            support.EXTERNAL_BUILD_TARGETS | EXTRA_BUILD_TARGETS,
            support.EXTERNAL_PROC_MACROS | EXTRA_PROC_MACROS, build_edges))
        if package["source"] != REGISTRY_SOURCE:
            violations.append(f"{name} has unaudited dependency source: {package['source']}")
    return violations


def selected_packages(manifest_path, graph):
    command = ["cargo", "tree", "--package", PACKAGE_NAME, "--edges", "normal,build,dev",
               "--target", "all", "--no-dedupe", "--locked", "--offline",
               "--prefix", "depth", "--format", "|{p}",
               "--manifest-path", str(manifest_path)]
    output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    root = graph.workspace_package(PACKAGE_NAME)
    packages, parents = {}, {}
    for line in output.splitlines():
        match = re.fullmatch(r"(\d+)\|(.*)", line.strip())
        if not match:
            raise ValueError(f"cannot parse Cargo tree identity: {line}")
        depth, label = int(match[1]), match[2]
        if depth == 0:
            candidates = {root["id"]}
        else:
            parent = parents.get(depth - 1)
            if parent is None:
                raise ValueError(f"Cargo tree omits parent: {line}")
            node = graph.resolve_nodes[parent["id"]]
            candidates = {edge["pkg"] for edge in node["deps"]
                          if any(kind["kind"] in (None, "build", "dev")
                                 for kind in edge["dep_kinds"])}
        package = graph.package_from_tree_label(label, candidates)
        packages[package["id"]] = package
        parents = {key: value for key, value in parents.items() if key < depth}
        parents[depth] = package
    if root["id"] not in packages:
        raise ValueError("Cargo tree omitted LocalProgram root")
    return packages.values()


_METADATA_SUPPORT = None

def metadata_support():
    global _METADATA_SUPPORT
    if _METADATA_SUPPORT is not None:
        return _METADATA_SUPPORT
    # Reuse the existing guard's exact Cargo identity parser, not its policy.
    path = Path(__file__).with_name("check-physical-plan-dependency-boundary.py")
    spec = importlib.util.spec_from_file_location("physical_plan_metadata", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _METADATA_SUPPORT = module
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-path", type=Path, default=Path("Cargo.toml"))
    args = parser.parse_args()
    support = metadata_support()
    graph = support.Graph(support.cargo_metadata(args.manifest_path))
    try:
        packages = list(selected_packages(args.manifest_path, graph))
    except subprocess.CalledProcessError as error:
        sys.stderr.write(error.stderr)
        return error.returncode
    except (ValueError, KeyError) as error:
        print(f"local-program dependency boundary violation: {error}", file=sys.stderr)
        return 1
    violations = [message for package in packages
                  for message in verify_package(package, graph.workspace_members)]
    if violations:
        for violation in violations:
            print(f"local-program dependency boundary violation: {violation}", file=sys.stderr)
        return 1
    print(f"LocalProgram pure dependency boundary PASS ({len(packages)} selected packages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
