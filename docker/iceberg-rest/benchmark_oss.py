#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Admit benchmark object operations without granting scratch deletion rights."""
from __future__ import annotations

import importlib.util
from pathlib import Path

import fixture_runtime as runtime
from scratch import contains


def resolve_current(snapshot, resolved):
    repo = Path(__file__).resolve().parents[2]
    path = repo / "tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py"
    spec = importlib.util.spec_from_file_location("benchmark_oss_contract", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    key = resolved["dataset_key"]
    return module.resolve_fixture(
        module.load_model(path.with_name("benchmark_tools.toml")), repo,
        key["suite"], key["scale"], snapshot["config"]["benchmark_root"], "aliyun-oss")


def run_operation(snapshot, backend, request, *, deadline):
    if not snapshot["config"].get("object_store") or not isinstance(request, dict):
        raise runtime.RuntimeFailure("RunBudgetRejected", "benchmark OSS operation requires its cloud machine")
    operation = request.get("operation")
    extra = {"input"} if operation == "put" else {"output"} if operation == "get" else set()
    if operation not in {"get", "stat", "list", "put"} or set(request) != {"resolved", "operation", "uri"} | extra:
        raise runtime.RuntimeFailure("RunBudgetRejected", "unsupported benchmark object operation")
    try:
        resolved = resolve_current(snapshot, request["resolved"])
        if resolved != request["resolved"] or not contains(resolved["dataset_root"], request["uri"]):
            raise ValueError("benchmark contract or target differs")
    except (KeyError, TypeError, ValueError) as error:
        raise runtime.RuntimeFailure("RunBudgetRejected", "benchmark operation differs from its current resolved contract") from error
    args = [operation, "--uri", request["uri"]]
    for field in extra:
        value = request[field]
        if not isinstance(value, str) or not Path(value).is_absolute():
            raise runtime.RuntimeFailure("RunBudgetRejected", "benchmark object I/O requires an absolute local path")
        args += ["--" + field, value]
    return backend.run_oss(snapshot, args, deadline=deadline)
