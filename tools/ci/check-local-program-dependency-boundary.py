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
and behavioral tests. External build helpers used by Arrow are allowed, while
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
    "novarocks-execution-contract",
    "novarocks-functions",
    "novarocks-type-contract",
    "novarocks-types",
})
REGISTRY_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"

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
        # A feature/target variant of a repository-owned contract requires a new
        # audit. Optional dependencies cannot hide outside the selected tree.
        if package.get("features"):
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
        if any("custom-build" in target["kind"] for target in package["targets"]):
            violations.append(f"{name} executes a custom build script")
    elif package["source"] != REGISTRY_SOURCE:
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


def metadata_support():
    # Reuse the existing guard's exact Cargo identity parser, not its policy.
    path = Path(__file__).with_name("check-physical-plan-dependency-boundary.py")
    spec = importlib.util.spec_from_file_location("physical_plan_metadata", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
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
