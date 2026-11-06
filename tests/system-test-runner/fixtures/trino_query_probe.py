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

"""Fixed finite native smoke for the receipt-bound repository Trino tools."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

REPOSITORY = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPOSITORY / "tools" / "plan-quality"))
import agg_plan_diff as agg
import rf_plan_diff as rf
import trino_query

AGG_SQL = "SELECT SUM(generate_series) AS total FROM generate_series(1, 100)"
RF_SQL = ("SELECT COUNT(*) FROM TABLE(generate_series(1, 100)) a "
          "JOIN TABLE(generate_series(1, 100)) b ON a.generate_series = b.generate_series")
TEXT_SQL = "SELECT REPEAT('q', 65536) AS payload FROM generate_series(1, 64)"


class Probe:
    def __init__(self, args):
        self.args = args
        self.root = args.output
        self.root.mkdir()
        # One inherited whole-scene deadline, including process startup. Each
        # operation gets only what remains, never a new complete duration.
        self.deadline = time.monotonic() + (args.deadline_unix_ms / 1000 - time.time())
        self.results = {"status": "FAIL", "external_starrocks_comparison": "NOT_RUN"}

    def remaining(self):
        value = self.deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("original whole-scene query tool deadline expired")
        return value

    def write(self, name, value):
        (self.root / name).write_text(json.dumps(value, indent=2) + "\n")

    def explain(self, module, endpoint, sql, name):
        timeout = self.remaining()
        self.write(name + ".call.json", {"module": module.__name__, "function": "run_novarocks",
                   "endpoint": endpoint._asdict(), "sql": sql, "receipt": str(self.args.receipt),
                   "timeout_seconds": timeout, "deadline_unix_ms": self.args.deadline_unix_ms})
        try:
            text = module.run_novarocks(endpoint, sql, timeout, artifact_receipt=str(self.args.receipt))
        except Exception as error:
            self.write(name + ".exit.json", {"success": False, "error": str(error)})
            raise
        (self.root / (name + ".stdout")).write_text(text)
        self.write(name + ".exit.json", {"success": True, "exit": 0})
        if not text.strip():
            raise ValueError(name + " returned empty EXPLAIN text")
        return text

    def query(self, name, sql, mode):
        directory = self.root / name
        directory.mkdir()
        observation = directory / "observation.json"
        command = [trino_query.bound_tool(self.args.receipt),
                   "--host", self.args.host, "--port", str(self.args.port),
                   "--user", self.args.user, "--time-zone", "UTC",
                   "--timeout-secs", str(self.remaining()), "--mode", mode,
                   "--observation-output", str(observation)]
        self.write(name + "/command.json", {"arguments": command, "stdin_sql": sql,
                   "deadline_unix_ms": self.args.deadline_unix_ms})
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        interrupted = None
        try:
            output, errors = process.communicate(sql.encode(), timeout=self.remaining())
        except (subprocess.TimeoutExpired, TimeoutError, KeyboardInterrupt) as error:
            interrupted = str(error)
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            # The original scene's ManagedCommand remains the authoritative
            # outer cap. Let the tool request DELETE and actually join.
            output, errors = process.communicate()
        (directory / "stdout.log").write_bytes(output)
        (directory / "stderr.log").write_bytes(errors)
        self.write(name + "/exit.json", {"exit": process.returncode, "interrupted": interrupted})
        if interrupted is not None or process.returncode or errors:
            raise RuntimeError(name + " tool failed; retained original stderr and exit")
        observed = json.loads(observation.read_text())
        if not isinstance(observed.get("query_id"), str) or not observed["query_id"]:
            raise ValueError(name + " omitted original query identity")
        if type(observed.get("pages")) is not int or observed["pages"] <= 1:
            raise ValueError(name + " did not consume continuation responses")
        if observed.get("update_type") is not None or observed.get("update_count") is not None:
            raise ValueError(name + " unexpectedly became a command completion")
        return output, observed

    def run(self):
        agg_text = self.explain(agg, agg.Endpoint("nr", self.args.host, str(self.args.port), self.args.user, "nr"), AGG_SQL, "aggregate-explain")
        self.results["aggregate_markers"] = agg.agg_counts(agg_text, "nr")
        rf_text = self.explain(rf, rf.Endpoint("nr", self.args.host, str(self.args.port), self.args.user), RF_SQL, "runtime-filter-explain")
        self.results["runtime_filter_lines"] = rf.rf_lines(rf_text)
        integer, integer_observation = self.query("scalar-integer", AGG_SQL, "scalar-integer")
        if integer != b"5050\n":
            raise ValueError("scalar result differs from the independent sum1..100 =5050")
        text, text_observation = self.query("cross-page-text", TEXT_SQL, "text-column")
        expected = (b"q" * 65536 + b"\n") * 64
        if text != expected:
            raise ValueError("text differs from64 independent rows of65536 q bytes")
        self.results.update(status="PASS", scalar_expected=5050,
                            scalar_observation=integer_observation,
                            text_observation=text_observation,
                            text_rows=64, text_bytes_per_row=65536,
                            text_payload_bytes=4194304, stdout_bytes=len(text),
                            page_observation="original shared StatementResult.pages, not data-page count")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--user", required=True)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--deadline-unix-ms", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    probe = Probe(parser.parse_args())
    try:
        probe.run()
    except Exception as error:
        probe.results["error"] = str(error)
        print(str(error), file=sys.stderr)
        return 1
    finally:
        probe.write("result.json", probe.results)
    print("PASS: actual plan helpers, integer5050 and64 complete text rows; external comparison NOT_RUN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
