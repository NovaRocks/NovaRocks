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

"""Verify the retired NCP-8 path and sketch dependency boundaries.

The Cargo checks use resolved dependency metadata rather than source imports.
The source check tokenizes Rust/proto code after removing comments, so string-
addressed revivals are rejected while historical documentation and the one
generated compatibility test remain explicit witnesses.
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

RETIRED_IDENTIFIERS = {
    "StatisticsSink",
    "StatisticsSinkProgram",
    "StatisticsSinkFactory",
    "StatisticsSinkHandle",
    "StatisticsBatchCollector",
    "StatisticsFragmentPartial",
    "QUERY_TERMINAL_STATISTICS_PAYLOAD_MAX_BYTES",
    "statistics_payload",
}
EXECUTION_PROVIDER_IDENTIFIERS = {
    "Puffin",
    "StatisticAggregationsDescriptor",
    "StatisticsAggregationDescriptor",
    "StatisticsDescriptor",
    "StatisticsMetricDescriptor",
}
RETIRED_EXECUTION_PATHS = {
    "novarocks/execution/src/exec/statistics.rs",
    "novarocks/execution/src/exec/operators/statistics_sink.rs",
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


def strip_comments(source):
    """Remove Rust/proto comments while preserving code and string contents."""

    output = []
    index = 0
    block_depth = 0
    length = len(source)
    while index < length:
        if block_depth:
            if source.startswith("/*", index):
                block_depth += 1
                index += 2
            elif source.startswith("*/", index):
                block_depth -= 1
                index += 2
            else:
                index += 1
            continue
        if source.startswith("//", index):
            newline = source.find("\n", index + 2)
            index = length if newline < 0 else newline
            continue
        if source.startswith("/*", index):
            block_depth = 1
            index += 2
            continue

        raw = re.match(r"(?:b|c)?r(#+)?\"", source[index:])
        if raw:
            hashes = raw.group(1) or ""
            terminator = '"' + hashes
            end = source.find(terminator, index + raw.end())
            next_index = length if end < 0 else end + len(terminator)
            output.append(source[index:next_index])
            index = next_index
            continue

        prefix_length = 0
        if source.startswith(('b"', 'c"'), index):
            prefix_length = 1
        if source[index + prefix_length : index + prefix_length + 1] == '"':
            start = index
            index += prefix_length + 1
            while index < length:
                if source[index] == "\\":
                    index += 2
                elif source[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            output.append(source[start:index])
            continue

        output.append(source[index])
        index += 1
    return "".join(output)


def identifiers(source):
    return set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", strip_comments(source)))


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


def production_source_files(repo_root):
    roots = [repo_root / "novarocks", repo_root / "novarocks-server", repo_root / "idl"]
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in {".rs", ".proto"}:
                continue
            relative = path.relative_to(repo_root)
            if "benches" in relative.parts:
                continue
            if relative == Path("novarocks/proto-models/tests/generated_contract.rs"):
                continue
            yield relative, path


def verify_source_boundary(repo_root):
    for retired in sorted(RETIRED_EXECUTION_PATHS):
        if (repo_root / retired).exists():
            fail(f"retired execution source path exists: {retired}")

    violations = []
    execution_prefix = Path("novarocks/execution/src")
    for relative, path in production_source_files(repo_root):
        forbidden = set(RETIRED_IDENTIFIERS)
        if relative.is_relative_to(execution_prefix):
            forbidden |= EXECUTION_PROVIDER_IDENTIFIERS
        source = path.read_text(encoding="utf-8")
        if relative == Path("idl/novarocks/service.proto"):
            # The old name must survive only as this exact reservation.  Any
            # other code or string occurrence remains visible to the scan.
            source = re.sub(
                r'\breserved\s+"statistics_payload"\s*;', "", source, count=1
            )
        # Most files contain none of the exact retired identifiers.  Avoid
        # running the Rust comment/literal scanner over the entire tree when a
        # cheap raw token prefilter proves there can be no violation.
        candidates = {
            identifier
            for identifier in forbidden
            if re.search(rf"\b{re.escape(identifier)}\b", source)
        }
        found = identifiers(source) & candidates if candidates else set()
        if found:
            violations.append(f"{relative}: {', '.join(sorted(found))}")
    if violations:
        fail("retired/provider-specific production identifiers found: " + "; ".join(violations))

    service = (repo_root / "idl/novarocks/service.proto").read_text(encoding="utf-8")
    terminal = extract_proto_message(service, "QueryTerminalFragmentSnapshot")
    require_reserved(terminal, 12, "statistics_payload", "QueryTerminalFragmentSnapshot")

    plan = (repo_root / "idl/novarocks/plan.proto").read_text(encoding="utf-8")
    sink = extract_proto_message(plan, "DataSink")
    require_reserved(sink, 8, "statistics", "DataSink")
    if re.search(r"\bmessage\s+StatisticsSink\b", plan):
        fail("plan.proto must not define the retired StatisticsSink message")

    witness_path = repo_root / "novarocks/proto-models/tests/generated_contract.rs"
    witness = strip_comments(witness_path.read_text(encoding="utf-8"))
    for required in (
        '"novarocks.plan.StatisticsSink"',
        '"statistics_payload"',
    ):
        if required not in witness:
            fail(f"generated compatibility witness is missing {required}")
    for test_name in (
        "retired_write_operation_aggregate_fields_remain_reserved",
        "retired_write_operation_aggregate_wire_fields_fail_closed",
    ):
        if re.search(rf"#\s*\[\s*test\s*\]\s*fn\s+{test_name}\b", witness) is None:
            fail(f"generated compatibility witness is missing active test {test_name}")


def main():
    default_root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(
        description="Verify NCP-8 retired statistics and dependency boundaries."
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
