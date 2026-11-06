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

"""Run one SQL through the receipt-bound shared Trino query tool."""
from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import sys


def bound_tool(receipt: str | Path | None) -> str:
    receipt = receipt or os.environ.get("NOVA_CI_ARTIFACT_RECEIPT")
    if not receipt:
        raise ValueError("--artifact-receipt or NOVA_CI_ARTIFACT_RECEIPT is required for NovaRocks")
    verifier = Path(__file__).resolve().parents[1] / "ci" / "cargo-artifacts.py"
    result = subprocess.run(
        [sys.executable, str(verifier), "verify", str(receipt), "novarocks-trino-query"],
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise RuntimeError("Trino tool artifact binding failed: " + result.stderr.strip())
    return result.stdout.strip()


def run_text_column(endpoint, sql: str, timeout: float, database: str | None,
                    *, receipt: str | Path | None = None, catalog: str | None = None) -> str:
    if timeout <= 0:
        raise ValueError("query timeout must be positive")
    command = [bound_tool(receipt), "--host", endpoint.host, "--port", str(endpoint.port),
               "--user", endpoint.user, "--time-zone", "UTC", "--timeout-secs", str(timeout),
               "--mode", "text-column"]
    if catalog is not None:
        command += ["--catalog", catalog]
    if database is not None:
        command += ["--schema", database]
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        output, errors = process.communicate(sql, timeout=timeout)
    except (subprocess.TimeoutExpired, KeyboardInterrupt) as original:
        # The tool owns DELETE and its original statement worker Join. SIGTERM
        # requests that cleanup; an immediate kill would strand the query.
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        output, errors = process.communicate()
        raise RuntimeError(f"Trino tool interrupted ({type(original).__name__}): {errors.strip()}") from original
    if process.returncode:
        raise RuntimeError(f"Trino tool failed ({process.returncode}): {errors.strip()}")
    return output
