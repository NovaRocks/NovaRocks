#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership and limitations under the License.

"""Verify the narrow Native trust ownership and dependency boundary."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


TRUST = "novarocks-native-trust"
ALLOWED_INTERNAL = {"novarocks-secret", "novarocks-types"}
FORBIDDEN_INTERNAL = {
    "novarocks-native-adapter",
    "novarocks-connector-iceberg",
    "novarocks-connector-starrocks",
    "novarocks-execution",
    "novarocks-failpoint",
    "novarocks-frontend",
    "novarocks-fs",
    "novarocks-parser",
    "novarocks-proto-codec",
    "novarocks-proto-models",
    "novarocks-server",
    "novarocks-spi",
    "novarocks-sql",
    "novarocks-state-store-foundationdb",
    "novarocks-state-store-mysql",
    "novarocks-state-store-sqlite",
}
FORBIDDEN_SOURCE_MARKERS = (
    "std::env",
    "std::fs",
    "tokio::fs",
    "NovaRocksGrpcClient",
    "Endpoint::connect(",
)


def fail(message):
    print(f"Native trust dependency boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def repo_root():
    return Path(__file__).resolve().parents[2]


def read_metadata(arguments):
    if arguments.metadata_path:
        try:
            return json.loads(arguments.metadata_path.read_text())
        except (OSError, json.JSONDecodeError) as error:
            fail(f"cannot read metadata fixture: {error}")
    command = [
        "cargo",
        "metadata",
        "--manifest-path",
        str((arguments.manifest_path or repo_root() / "Cargo.toml").resolve()),
        "--format-version",
        "1",
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return json.loads(result.stdout)


def package(metadata):
    packages = [entry for entry in metadata["packages"] if entry["name"] == TRUST]
    if len(packages) != 1:
        fail(f"Cargo metadata must contain exactly one {TRUST} package")
    return packages[0]


def verify_metadata(metadata):
    trust = package(metadata)
    internal = {
        dependency["name"]
        for dependency in trust["dependencies"]
        if dependency["kind"] is None and dependency["name"].startswith("novarocks-")
    }
    if internal != ALLOWED_INTERNAL:
        missing = sorted(ALLOWED_INTERNAL - internal)
        unexpected = sorted(internal - ALLOWED_INTERNAL)
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        fail(f"{TRUST} internal normal dependencies must be exactly "
             f"{', '.join(sorted(ALLOWED_INTERNAL))} ({'; '.join(details)})")
    prohibited = sorted(internal & FORBIDDEN_INTERNAL)
    if prohibited:
        fail(f"{TRUST} has forbidden internal normal dependencies: {', '.join(prohibited)}")


def verify_source():
    source_root = repo_root() / "novarocks" / "native-trust" / "src"
    for source in sorted(source_root.glob("**/*.rs")):
        text = source.read_text()
        for marker in FORBIDDEN_SOURCE_MARKERS:
            if marker in text:
                fail(f"{source.relative_to(repo_root())} contains forbidden source marker {marker!r}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--manifest-path", type=Path)
    inputs.add_argument("--metadata-path", type=Path)
    arguments = parser.parse_args()
    verify_metadata(read_metadata(arguments))
    if arguments.metadata_path is None:
        verify_source()
    print("native trust dependency boundary: PASS")


if __name__ == "__main__":
    main()
