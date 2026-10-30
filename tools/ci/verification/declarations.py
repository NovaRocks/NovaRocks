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
"""Validate package declarations against native nextest facts, without I/O.

Native nextest evaluates every filterset. This module neither parses Rust source
nor reproduces nextest's filter language or Cargo's dependency graph. The caller
retains each requested native command, its output and its actual exit.
"""
from collections import defaultdict
from pathlib import PurePosixPath
import re

from .model import UnitKey


def _packages(metadata):
    if metadata.get("version") != 1:
        raise ValueError("expected cargo metadata format version 1")
    members = set(metadata["workspace_members"])
    return sorted((package for package in metadata["packages"] if package["id"] in members),
                  key=lambda package: package["name"])


def _declaration(package):
    return (package.get("metadata") or {}).get("novarocks", {}).get("test", {})


def native_selector_requests(metadata):
    """Return requests to evaluate with native ``nextest list``.

    ``key`` identifies this diagnostic request only, never a verification unit.
    The request must be evaluated with the same prepared Cargo/test bindings as
    the complete native list, and with ``--run-ignored only``.
    """
    result = []
    for package in _packages(metadata):
        declaration = _declaration(package)
        if not isinstance(declaration, dict) or not isinstance(declaration.get("ignored", []), list):
            continue
        for index, entry in enumerate(declaration.get("ignored", [])):
            if not isinstance(entry, dict) or not isinstance(entry.get("filter"), str) or not entry["filter"]:
                continue
            result.append({"key": package["name"] + "/" + str(index),
                           "package": package["name"], "package_id": package["id"],
                           "index": index, "filter": entry["filter"],
                           "filterset": "package(={}) & ({})".format(package["name"], entry["filter"])})
    return result


def _native_cases(native, bindings, matched_only=False):
    if bindings.get("schema") != 1:
        raise ValueError("unsupported native test-bindings schema")
    result = {}
    for binary_id, suite in native["rust-suites"].items():
        package = bindings["binaries"][binary_id]["package-id"]
        for name, case in suite["testcases"].items():
            if type(case.get("ignored")) is not bool:
                raise ValueError("native testcase lacks explicit ignored flag")
            if matched_only and case.get("filter-match", {}).get("status") != "matches":
                continue
            result[str(UnitKey("rust-test", binary_id + "::" + name))] = {
                "package_id": package, "ignored": case["ignored"]}
    return result


def validate_declarations(metadata, native_list, test_bindings, selector_results=None):
    """Check declarations and ignored coverage using native enumeration alone.

    ``selector_results`` maps request keys to complete native list documents or
    ``{"error": <native diagnostic>}``. Missing results remain pending; callers
    cannot treat syntactically unexamined filter strings as validated. Packages
    with no input declaration stay visible as ineligible for historical reuse.
    """
    packages = _packages(metadata)
    selector_results = selector_results or {}
    errors, undeclared, requests = [], [], native_selector_requests(metadata)
    package_by_id = {package["id"]: package for package in packages}
    entries = {}
    for package in packages:
        name, declaration = package["name"], _declaration(package)
        if not isinstance(declaration, dict):
            errors.append(name + ": test declaration must be an object")
            continue
        file_declared, env_declared = "runtime-inputs" in declaration, "env-inputs" in declaration
        if not file_declared and not env_declared:
            undeclared.append(name)
        elif not file_declared or not env_declared:
            errors.append(name + ": runtime-inputs and env-inputs must be declared together")
        paths = declaration.get("runtime-inputs", [])
        if (not isinstance(paths, list) or any(not isinstance(path, str) or not path
                or PurePosixPath(path).is_absolute() or ".." in PurePosixPath(path).parts for path in paths)):
            errors.append(name + ": runtime inputs must be repository-relative globs")
        environment = declaration.get("env-inputs", [])
        if not isinstance(environment, list):
            errors.append(name + ": env-inputs must be an array")
        else:
            seen = set()
            for item in environment:
                if (not isinstance(item, dict) or item.get("kind") not in {"value", "file-content"}
                        or not isinstance(item.get("name"), str)
                        or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", item["name"])):
                    errors.append(name + ": invalid environment input")
                elif item["name"] in seen:
                    errors.append(name + ": duplicate environment input " + item["name"])
                else:
                    seen.add(item["name"])
        ignored = declaration.get("ignored", [])
        if not isinstance(ignored, list):
            errors.append(name + ": ignored must be an array")
            continue
        for index, entry in enumerate(ignored):
            key = name + "/" + str(index)
            if (not isinstance(entry, dict) or not isinstance(entry.get("filter"), str) or not entry["filter"]
                    or entry.get("class") not in {"heavy", "helper", "manual"}
                    or not isinstance(entry.get("reason"), str) or not entry["reason"].strip()):
                errors.append(key + ": ignored declaration needs filter, class and reason")
                continue
            if 'input-owner' in entry and (entry['input-owner'] != 'iceberg-java-rest-interop'
                    or name != 'novarocks-connector-iceberg' or entry['class'] != 'heavy'):
                errors.append(key + ': unsupported native input owner')
            if entry["class"] == "heavy" and (not isinstance(entry.get("requires"), list)
                    or not entry["requires"] or any(not isinstance(value, str) or not value for value in entry["requires"])):
                errors.append(key + ": heavy ignored test needs external requirements")
            entries[key] = entry
    try:
        native_cases = _native_cases(native_list, test_bindings)
    except (ValueError, TypeError, KeyError) as error:
        return {"schema": 1, "ok": False, "errors": errors + ["invalid native enumeration: " + str(error)],
                "native_requests": requests, "pending_native_selectors": [], "no_reuse": undeclared,
                "coverage": [], "inactive": [], "native_ignored": 0}
    ignored_cases = {unit: facts for unit, facts in native_cases.items() if facts["ignored"]}
    covered, inactive, pending = defaultdict(list), [], []
    for request in requests:
        key = request["key"]
        if key not in entries:
            continue
        if key not in selector_results:
            pending.append(key)
            continue
        selected = selector_results[key]
        if not isinstance(selected, dict) or "error" in selected:
            errors.append(key + ": native selector failed: " + str(selected))
            continue
        try:
            matched = _native_cases(selected, test_bindings, matched_only=True)
        except (ValueError, TypeError, KeyError) as error:
            errors.append(key + ": invalid native selector output: " + str(error))
            continue
        if entries[key]["class"] == "heavy" and len(matched) != 1:
            errors.append(key + ": heavy selector must identify exactly one native ignored test")
        if not matched:
            inactive.append(key)
        for unit, facts in matched.items():
            if unit not in ignored_cases or facts != ignored_cases[unit]:
                errors.append(key + ": selector does not identify a retained native ignored test: " + unit)
            elif facts["package_id"] != request["package_id"]:
                errors.append(key + ": selector escaped its package: " + unit)
            else:
                covered[unit].append(key)
    coverage = []
    for unit, facts in sorted(ignored_cases.items()):
        owners = covered[unit]
        package = package_by_id.get(facts["package_id"])
        if package is None:
            errors.append("native ignored test belongs to a non-workspace package: " + unit)
        if not owners:
            # Pending native commands may still establish coverage. Keep the
            # gap explicit rather than claiming those declarations are valid.
            if not pending:
                errors.append("native ignored test has no declaration: " + unit)
        elif len(owners) != 1:
            errors.append("native ignored test has ambiguous declarations: " + unit)
        coverage.append({"unit": unit, "package": package["name"] if package else None,
                         "declarations": owners,
                         "class": entries[owners[0]]["class"] if len(owners) == 1 else None})
    return {"schema": 1, "ok": not errors and not pending, "errors": errors,
            "native_requests": requests, "pending_native_selectors": pending,
            "no_reuse": undeclared, "native_ignored": len(ignored_cases),
            "coverage": coverage, "inactive": inactive}
