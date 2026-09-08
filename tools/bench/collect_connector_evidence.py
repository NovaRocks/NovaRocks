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

"""Collect connector counters from saved NovaRocks runtime profiles."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any


COUNTERS = {
    "connector_file_bytes_read": ("bytes", ("ConnectorFileBytesRead",)),
    "connector_file_read_requests": ("count", ("ConnectorFileReadRequests",)),
    "connector_batches": ("count", ("ConnectorBatches",)),
    "connector_io_time": ("time", ("ConnectorIoTime", "ConnectorIOTime")),
    "connector_decode_time": ("time", ("ConnectorDecodeTime",)),
    "driver_io_task_wait_time": ("time", ("IOTaskWaitTime",)),
    "source_wait_time": ("time", ("SourceWaitTime", "source_wait")),
    "scan_io_time": ("time", ("ScanIoTime", "scan_io")),
    "footer_cache_hits": ("count", ("FooterCacheHits",)),
    "footer_cache_loads": ("count", ("FooterCacheLoads",)),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--patch", type=Path)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def flatten(value: Any, path: str = "") -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            rows.extend(flatten(child, f"{path}.{key}" if path else str(key)))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            rows.extend(flatten(child, f"{path}[{index}]"))
    else:
        rows.append((path, value))
    return rows


def source_files(roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        resolved = root.resolve()
        if resolved.is_file():
            files.append(resolved)
        elif resolved.is_dir():
            files.extend(path for path in resolved.rglob("*") if path.is_file())
        else:
            raise SystemExit(f"input does not exist: {resolved}")
    return sorted(set(files))


def occurrences(path: Path) -> list[tuple[str, Any, str]]:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return []
    rows: list[tuple[str, Any, str]] = []
    if path.suffix == ".json":
        try:
            for field, value in flatten(json.loads(text)):
                rows.append((field, value, "json"))
        except json.JSONDecodeError:
            pass
    for number, line in enumerate(text.splitlines(), 1):
        rows.append((f"line:{number}", line, "text"))
    return rows


def main() -> int:
    args = parse_args()
    files = source_files(args.input)
    extracted: dict[str, Any] = {}
    for canonical, (unit, aliases) in COUNTERS.items():
        matches = []
        alias_pattern = re.compile("|".join(re.escape(alias) for alias in aliases), re.IGNORECASE)
        for path in files:
            for field, value, encoding in occurrences(path):
                candidate = f"{field} {value}"
                if alias_pattern.search(candidate):
                    matches.append(
                        {
                            "source": str(path),
                            "field": field,
                            "value": value,
                            "encoding": encoding,
                        }
                    )
        extracted[canonical] = {
            "unit": unit,
            "aliases": list(aliases),
            "status": "observed" if matches else "missing",
            "observations": matches,
        }

    patch = args.patch.resolve() if args.patch is not None else None
    if patch is not None and not patch.is_file():
        raise SystemExit(f"patch does not exist: {patch}")
    report = {
        "schema_version": 1,
        "revision": args.revision,
        "inputs": [str(path) for path in files],
        "patch": str(patch) if patch is not None else None,
        "patch_sha256": sha256(patch) if patch is not None else None,
        "counters": extracted,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
