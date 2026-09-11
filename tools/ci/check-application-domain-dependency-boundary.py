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

"""Verify the resolved normal dependency direction between application domains."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


QUERY = "novarocks-query-application"
WORKER = "novarocks-worker"
EXECUTION = "novarocks-execution"
WORKLOAD = "novarocks-workload-control"
CATALOG = "novarocks-catalog-application"

PRODUCTS = {
    "novarocks-mv-application",
    "novarocks-statistics-application",
    "novarocks-table-maintenance",
}
ROLE_IMPLEMENTATIONS = {"novarocks-frontend", "novarocks-backend"}
ADAPTERS = {"novarocks-native-adapter", "novarocks-mysql-adapter"}
WIRE = {
    "novarocks-proto-models",
    "novarocks-proto-codec",
    "novarocks-task-codec",
}

QUERY_FORBIDDEN = PRODUCTS | ROLE_IMPLEMENTATIONS | ADAPTERS | WIRE | {
    WORKER,
    "novarocks-server",
}
WORKER_FORBIDDEN = PRODUCTS | ROLE_IMPLEMENTATIONS | ADAPTERS | WIRE | {
    QUERY,
    "novarocks-server",
}
EXECUTION_FORBIDDEN = PRODUCTS | ROLE_IMPLEMENTATIONS | ADAPTERS | {
    QUERY,
    WORKER,
    "novarocks-server",
}
WORKLOAD_FORBIDDEN = PRODUCTS | ROLE_IMPLEMENTATIONS | ADAPTERS | WIRE | {
    QUERY,
    WORKER,
    EXECUTION,
    "novarocks-server",
}
CATALOG_FORBIDDEN = PRODUCTS | ROLE_IMPLEMENTATIONS | ADAPTERS | WIRE | {
    QUERY,
    WORKER,
    "novarocks-server",
}


def fail(message):
    print(f"Application domain dependency boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def package_by_name(metadata, name):
    matches = [package for package in metadata["packages"] if package["name"] == name]
    if len(matches) != 1:
        fail(f"Cargo metadata must contain exactly one {name} package")
    return matches[0]


def package_names(metadata):
    return {package["name"] for package in metadata["packages"]}


def normal_closure(metadata, root_name):
    root = package_by_name(metadata, root_name)
    resolve = metadata.get("resolve")
    if resolve is None:
        fail("Cargo metadata must include resolve nodes")
    nodes = {node["id"]: node for node in resolve.get("nodes", [])}
    names = {package["id"]: package["name"] for package in metadata["packages"]}
    visited = set()
    pending = [root["id"]]
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
    return {names[package_id] for package_id in visited}


def verify_forbidden_closure(metadata, root, forbidden):
    found = sorted(normal_closure(metadata, root) & forbidden)
    if found:
        fail(f"{root} normal dependency closure contains forbidden domains: " + ", ".join(found))


def verify_products(metadata):
    available = package_names(metadata)
    for product in sorted(PRODUCTS & available):
        closure = normal_closure(metadata, product)
        if QUERY not in closure:
            fail(f"{product} normal dependency closure must contain {QUERY}")
        forbidden = sorted(
            closure
            & (ROLE_IMPLEMENTATIONS | ADAPTERS | WIRE | {WORKER, "novarocks-server"})
        )
        if forbidden:
            fail(f"{product} normal dependency closure contains forbidden owners: " + ", ".join(forbidden))


def load_metadata(arguments):
    if arguments.metadata_path is not None:
        return json.loads(arguments.metadata_path.read_text())
    return json.loads(
        subprocess.run(
            [
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--manifest-path",
                str(arguments.manifest_path),
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ).stdout
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "Cargo.toml",
    )
    parser.add_argument("--metadata-path", type=Path)
    arguments = parser.parse_args()
    metadata = load_metadata(arguments)

    verify_forbidden_closure(metadata, WORKER, WORKER_FORBIDDEN)
    verify_forbidden_closure(metadata, EXECUTION, EXECUTION_FORBIDDEN)
    verify_forbidden_closure(metadata, WORKLOAD, WORKLOAD_FORBIDDEN)
    if CATALOG in package_names(metadata):
        verify_forbidden_closure(metadata, CATALOG, CATALOG_FORBIDDEN)
    verify_products(metadata)
    verify_forbidden_closure(metadata, QUERY, QUERY_FORBIDDEN)
    if WORKLOAD not in normal_closure(metadata, QUERY):
        fail(f"{QUERY} normal dependency closure must contain {WORKLOAD}")
    print("application domain dependency boundary: PASS")


if __name__ == "__main__":
    main()
