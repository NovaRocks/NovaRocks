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
"""Bind executable paths from Cargo, and reject changed or missing programs."""
import hashlib
import json
import os
import shutil
from pathlib import Path
import sys

REQUIRED = ("novarocks", "novarocks-sql-test", "novarocks-system-tests", "novarocks-error-manifest")
OPTIONAL = ("novarocks-trino-query",)

def digest(path):
    with open(path, "rb") as stream:
        value = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
        return value.hexdigest()

def record(path):
    path = Path(path).resolve(strict=True)
    if not os.access(path, os.X_OK):
        raise ValueError(f"artifact is not executable: {path}")
    return dict(path=str(path), sha256=digest(path))

def bind_tests(binaries_path, cargo_path, output, host):
    """Validate native nextest facts against the last joint Cargo build."""
    metadata = json.loads(Path(binaries_path).read_text())
    build = metadata["rust-build-meta"]
    if build["platforms"]["host"]["platform"]["triple"] != host:
        raise ValueError("nextest host differs from the actual native Rust host")
    for target in build["platforms"].get("targets", []):
        if target["platform"]["triple"] != host:
            raise ValueError("cross-target nextest metadata is unsupported")
    for target in build.get("target-platforms", []):
        if target["triple"] != host:
            raise ValueError("nextest target is not the actual native Rust host")
    artifacts = []
    for line in Path(cargo_path).read_text().splitlines():
        item = json.loads(line)
        if item.get("reason") == "compiler-artifact" and item.get("executable"):
            artifacts.append(item)
    bound = {}
    for identity, binary in metadata["rust-binaries"].items():
        if identity != binary["binary-id"]:
            raise ValueError("inconsistent native binary identity")
        matches = [item for item in artifacts if
                   item["package_id"] == binary["package-id"] and
                   item["target"]["name"] == binary["binary-name"] and
                   binary["kind"] in item["target"]["kind"] and
                   item["profile"]["test"] is True and
                   Path(item["executable"]).resolve() == Path(binary["binary-path"]).resolve()]
        if not matches:
            raise ValueError("last joint Cargo build did not report native test binary: " + identity)
        bound[identity] = dict(binary, **record(binary["binary-path"]))
    if not bound:
        raise ValueError("native binaries metadata is empty")
    Path(output).write_text(json.dumps(dict(schema=1, host=host, binaries=bound), indent=2) + "\n")


try:
    if sys.argv[1] == "bind-tests":
        bind_tests(*sys.argv[2:])
        sys.exit(0)
    if sys.argv[1] == "verify-tests":
        expected = json.loads(Path(sys.argv[2]).read_text())
        if expected["schema"] != 1:
            raise ValueError("unsupported test binding schema")
        for identity, binary in expected["binaries"].items():
            actual = record(binary["path"])
            if any(actual[key] != binary[key] for key in actual):
                raise ValueError("prepared test executable changed: " + identity)
        sys.exit(0)
    action, source, arg = sys.argv[1:]
    if action == "copy-server":
        found = []
        for line in Path(source).read_text().splitlines():
            v = json.loads(line)
            if v.get("reason") == "compiler-artifact" and v.get("executable") and v["target"]["name"] == "novarocks" and "bin" in v["target"]["kind"] and not v["profile"]["test"]:
                found.append(v["executable"])
        if len(set(found)) != 1:
            raise ValueError("Cargo did not report one actual Server executable")
        shutil.copy2(found[0], arg)
        Path(arg + ".binding.json").write_text(json.dumps(record(arg), indent=2) + "\n")
    elif action == "collect":
        found = {}
        for line in Path(source).read_text().splitlines():
            v = json.loads(line)
            if v.get("reason") != "compiler-artifact" or not v.get("executable"):
                continue
            target = v["target"]
            if "bin" in target["kind"] and not v["profile"]["test"] and target["name"] in REQUIRED + OPTIONAL:
                found[target["name"]] = record(v["executable"])
        missing = set(REQUIRED) - found.keys()
        if missing:
            raise ValueError("Cargo did not report required binaries: " + ", ".join(sorted(missing)))
        Path(arg).write_text(json.dumps(found, indent=2) + "\n")
    elif action == "verify-copy":
        expected = json.loads(Path(source).read_text())
        if record(arg) != expected:
            raise ValueError("selected variant content changed")
    elif action == "verify":
        expected = json.loads(Path(source).read_text())[arg]
        actual = record(expected["path"])
        if actual != expected:
            raise ValueError("prepared artifact changed; collect paths, build information and hashes again")
        print(actual["path"])
    else:
        raise ValueError("unknown artifact action")
except (OSError, ValueError, KeyError) as error:
    sys.exit("error: " + str(error))
