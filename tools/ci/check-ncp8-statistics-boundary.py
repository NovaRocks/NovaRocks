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

"""Verify statistics wire reservations and sketch dependency boundaries.

Cargo's resolved graph defines the permitted dependency kinds and features.
The source check protects reservations on the current DataSink carrier;
descriptor and unknown-field behavior are exercised by the protocol tests.
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


DATASKETCHES = "datasketches"
EXECUTION = "novarocks-execution"
FUNCTIONS = "novarocks-functions"
ICEBERG_FUNCTIONS = "novarocks-connector-iceberg-functions"
ICEBERG_PROVIDER = "novarocks-connector-iceberg"
FRONTEND = "novarocks-frontend"
BACKEND = "novarocks-backend"
SERVER = "novarocks-server"

ROLE_PACKAGES = {FRONTEND, BACKEND, SERVER}
ALLOWED_PRODUCTION_DATASKETCHES_OWNERS = {EXECUTION, ICEBERG_FUNCTIONS}
FORBIDDEN_ICEBERG_FUNCTIONS_CLOSURE = {
    EXECUTION,
    FRONTEND,
    BACKEND,
    SERVER,
    ICEBERG_PROVIDER,
}


def fail(message):
    print(f"NCP-8 statistics boundary violation: {message}", file=sys.stderr)
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


def direct_dependencies(package, dependency_name):
    return [
        dependency
        for dependency in package["dependencies"]
        if dependency["name"] == dependency_name
    ]


def require_exact_dependency(
    package, dependency_name, *, kind, features, disable_default_features=True
):
    dependencies = direct_dependencies(package, dependency_name)
    if len(dependencies) != 1:
        fail(
            f"{package['name']} must declare exactly one direct {dependency_name} dependency"
        )
    dependency = dependencies[0]
    actual_kind = dependency["kind"]
    if actual_kind != kind:
        expected = "normal" if kind is None else kind
        actual = "normal" if actual_kind is None else actual_kind
        fail(
            f"{package['name']} {dependency_name} dependency kind must be {expected}, got {actual}"
        )
    actual_features = set(dependency.get("features", []))
    if actual_features != features:
        fail(
            f"{package['name']} {dependency_name} features must be exactly "
            f"{sorted(features)}, got {sorted(actual_features)}"
        )
    if disable_default_features and dependency.get("uses_default_features", True):
        fail(f"{package['name']} {dependency_name} must disable default features")
    if dependency.get("optional", False):
        fail(f"{package['name']} {dependency_name} must not be optional")
    if dependency.get("rename") is not None:
        fail(f"{package['name']} must not rename {dependency_name}")


def normal_closure(metadata, root_name):
    package = package_by_name(metadata, root_name)
    resolve = metadata.get("resolve")
    if resolve is None:
        fail("Cargo metadata must include a resolved dependency graph")
    nodes = {node["id"]: node for node in resolve.get("nodes", [])}
    names_by_id = {package["id"]: package["name"] for package in metadata["packages"]}
    if package["id"] not in nodes:
        fail(f"Cargo resolve graph is missing {root_name}")

    visited = set()
    pending = [package["id"]]
    while pending:
        package_id = pending.pop()
        if package_id in visited:
            continue
        visited.add(package_id)
        node = nodes.get(package_id)
        if node is None:
            fail(f"Cargo resolve graph is missing package id {package_id}")
        for dependency in node.get("deps", []):
            if any(kind.get("kind") is None for kind in dependency.get("dep_kinds", [])):
                pending.append(dependency["pkg"])
    try:
        return {names_by_id[package_id] for package_id in visited}
    except KeyError as error:
        fail(f"Cargo metadata packages are missing resolved id {error.args[0]}")


def package_test_closure(metadata, root_name):
    """Resolve root normal/build/dev edges, then dependency normal/build edges."""

    package = package_by_name(metadata, root_name)
    resolve = metadata.get("resolve")
    if resolve is None:
        fail("Cargo metadata must include a resolved dependency graph")
    nodes = {node["id"]: node for node in resolve.get("nodes", [])}
    names_by_id = {package["id"]: package["name"] for package in metadata["packages"]}
    visited = set()
    pending = [(package["id"], True)]
    while pending:
        package_id, is_root = pending.pop()
        if package_id in visited:
            continue
        visited.add(package_id)
        node = nodes.get(package_id)
        if node is None:
            fail(f"Cargo resolve graph is missing package id {package_id}")
        allowed_kinds = {None, "build", "dev"} if is_root else {None, "build"}
        for dependency in node.get("deps", []):
            if any(
                kind.get("kind") in allowed_kinds
                for kind in dependency.get("dep_kinds", [])
            ):
                pending.append((dependency["pkg"], False))
    try:
        return {names_by_id[package_id] for package_id in visited}
    except KeyError as error:
        fail(f"Cargo metadata packages are missing resolved id {error.args[0]}")


def is_production_package(package, repo_root):
    manifest = Path(package["manifest_path"]).resolve()
    try:
        relative = manifest.relative_to(repo_root)
    except ValueError:
        return False
    if not relative.parts:
        return False
    return relative.parts[0] in {"novarocks", "novarocks-server"}


def verify_dependency_boundary(metadata, repo_root):
    execution = package_by_name(metadata, EXECUTION)
    iceberg_functions = package_by_name(metadata, ICEBERG_FUNCTIONS)
    functions = package_by_name(metadata, FUNCTIONS)

    require_exact_dependency(execution, DATASKETCHES, kind=None, features={"hll"})
    require_exact_dependency(
        iceberg_functions, DATASKETCHES, kind=None, features={"theta"}
    )
    require_exact_dependency(
        execution,
        ICEBERG_FUNCTIONS,
        kind="dev",
        features=set(),
        disable_default_features=False,
    )

    for role_name in sorted(ROLE_PACKAGES):
        role = package_by_name(metadata, role_name)
        if direct_dependencies(role, DATASKETCHES):
            fail(f"{role_name} must not directly depend on {DATASKETCHES}")

    for package in metadata["packages"]:
        if not is_production_package(package, repo_root):
            continue
        if package["name"] in ALLOWED_PRODUCTION_DATASKETCHES_OWNERS:
            continue
        if direct_dependencies(package, DATASKETCHES):
            fail(
                f"production package {package['name']} must not directly depend on {DATASKETCHES}"
            )

    iceberg_internal = {
        name
        for name in normal_closure(metadata, ICEBERG_FUNCTIONS)
        if name.startswith("novarocks-")
    }
    expected_internal = {ICEBERG_FUNCTIONS, FUNCTIONS}
    if iceberg_internal != expected_internal:
        fail(
            f"{ICEBERG_FUNCTIONS} internal normal closure must be exactly "
            f"{sorted(expected_internal)}, got {sorted(iceberg_internal)}"
        )
    forbidden = sorted(
        normal_closure(metadata, ICEBERG_FUNCTIONS)
        & FORBIDDEN_ICEBERG_FUNCTIONS_CLOSURE
    )
    if forbidden:
        fail(
            f"{ICEBERG_FUNCTIONS} normal closure contains forbidden packages: "
            + ", ".join(forbidden)
        )

    functions_internal = {
        name
        for name in normal_closure(metadata, FUNCTIONS)
        if name.startswith("novarocks-")
    }
    if functions_internal != {FUNCTIONS}:
        fail(
            f"{FUNCTIONS} internal normal closure must contain only itself, got "
            + ", ".join(sorted(functions_internal))
        )
    if DATASKETCHES in normal_closure(metadata, FUNCTIONS):
        fail(f"{FUNCTIONS} normal closure must not contain {DATASKETCHES}")

    if direct_dependencies(execution, ICEBERG_PROVIDER):
        fail(f"{EXECUTION} must not directly depend on {ICEBERG_PROVIDER}")
    execution_forbidden = normal_closure(metadata, EXECUTION) & {
        ICEBERG_FUNCTIONS,
        ICEBERG_PROVIDER,
    }
    if execution_forbidden:
        fail(
            f"{EXECUTION} normal closure contains provider packages: "
            + ", ".join(sorted(execution_forbidden))
        )
    if ICEBERG_PROVIDER in package_test_closure(metadata, EXECUTION):
        fail(
            f"{EXECUTION} normal/build/dev closure must not contain {ICEBERG_PROVIDER}"
        )
    execution_closure = normal_closure(metadata, EXECUTION)
    execution_sketch_owners = sorted(
        package["name"]
        for package in metadata["packages"]
        if package["name"] in execution_closure
        and direct_dependencies(package, DATASKETCHES)
        and package["name"] != EXECUTION
    )
    if execution_sketch_owners:
        fail(
            f"{EXECUTION} normal closure has indirect {DATASKETCHES} owners: "
            + ", ".join(execution_sketch_owners)
        )


def extract_proto_message(source, message_name):
    code = re.sub(r"//[^\n]*|/\*.*?\*/", "", source, flags=re.DOTALL)
    match = re.search(rf"\bmessage\s+{re.escape(message_name)}\s*\{{", code)
    if match is None:
        fail(f"proto message {message_name} is missing")
    depth = 1
    index = match.end()
    while index < len(code) and depth:
        if code[index] == "{":
            depth += 1
        elif code[index] == "}":
            depth -= 1
        index += 1
    if depth:
        fail(f"proto message {message_name} is unterminated")
    return code[match.end() : index - 1]


def require_reserved(message_body, number, name, context):
    if re.search(rf"\breserved\s+{number}\s*;", message_body) is None:
        fail(f"{context} must reserve field number {number}")
    if re.search(rf'\breserved\s+"{re.escape(name)}"\s*;', message_body) is None:
        fail(f"{context} must reserve field name {name}")


def verify_source_boundary(repo_root):
    plan = (repo_root / "idl/novarocks/plan.proto").read_text(encoding="utf-8")
    sink = extract_proto_message(plan, "DataSink")
    require_reserved(sink, 8, "statistics", "DataSink")


def main():
    default_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Verify statistics wire reservations and sketch dependency boundaries."
    )
    parser.add_argument("--repo-root", type=Path, default=default_root)
    parser.add_argument("--manifest-path", type=Path)
    arguments = parser.parse_args()
    repo_root = arguments.repo_root.resolve()
    manifest_path = (arguments.manifest_path or repo_root / "Cargo.toml").resolve()

    verify_dependency_boundary(cargo_metadata(manifest_path), repo_root)
    verify_source_boundary(repo_root)
    print("NCP-8 statistics boundary: PASS")


if __name__ == "__main__":
    main()
