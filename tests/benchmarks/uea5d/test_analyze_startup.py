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

import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from analyze_startup import EXPECTED_FIXTURES, STEM, analyze, read_run


class StartupAnalysisTests(unittest.TestCase):
    def write_run(self, root: Path, run_id: str, multiplier: int = 1) -> None:
        root.mkdir()
        fixture_spec = [
            [
                fixture,
                f"SELECT '{fixture}'",
                int(fixture.rsplit("-", 1)[1]) if fixture.startswith("union-width-") else None,
            ]
            for fixture in sorted(EXPECTED_FIXTURES)
        ]
        fixture_bytes = json.dumps(fixture_spec).encode()
        (root / "fixture-spec.json").write_bytes(fixture_bytes)
        manifest = {
            "schema_version": 6,
            "kind": "startup-baseline",
            "status": "completed",
            "exit_code": 0,
            "formal": True,
            "source_dirty": False,
            "scenario": "task-execution/uea5d-startup-baseline",
            "run_id": run_id,
            "source_revision": "revision",
            "source_tree_sha256": "source",
            "native_build_identity": "revision",
            "config_sha256": "config",
            "fixture_sha256": hashlib.sha256(fixture_bytes).hexdigest(),
            "tool_tree_sha256": "tools",
            "cargo_lock_sha256": "lock",
            "third_party_build_graph_sha256": "graph",
            "effective_launch_config_semantics_sha256": "launch",
            "build_profile": "release",
            "platform": {"architecture": "arm64", "cpu_model": "test", "power_mode": "ac"},
            "process_identities": [
                {"role": role, "os_pid": index + 1, "build_identity": "revision"}
                for index, role in enumerate(("fe", "be-0", "be-1", "be-2"))
            ],
        }
        manifest_bytes = json.dumps(manifest).encode()
        (root / "run-manifest.json").write_bytes(manifest_bytes)
        identity = {
            "schema_version": 1,
            "run_id": run_id,
            "run_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "measured_binary_sha256": "a" * 64,
            "runner_binary_sha256": "b" * 64,
            "fixtures": [
                {
                    "fixture_id": fixture,
                    "query_sha256": hashlib.sha256(sql.encode()).hexdigest(),
                    "plan_sha256": "plan",
                    "plan_fragments": 2,
                    "exchange_levels": 1,
                }
                for fixture, sql, _ in fixture_spec
            ],
        }
        (root / f"{STEM}-fixtures.json").write_text(json.dumps(identity))
        with (root / f"{STEM}.csv").open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                ["fixture", "phase", "run", "first_row_micros", "total_micros"]
            )
            for fixture in sorted(EXPECTED_FIXTURES):
                for phase, count in (("warmup", 5), ("measured", 200)):
                    for index in range(count):
                        writer.writerow(
                            [fixture, phase, index, (index + 1) * multiplier, (index + 2) * multiplier]
                        )

    def test_aa_requires_complete_independent_runs_and_reports_p99(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_run(root / "a", "first")
            self.write_run(root / "b", "second", multiplier=2)
            first, second = read_run(root / "a"), read_run(root / "b")
            report = analyze(first, second)
            self.assertEqual(report["fixtures"]["shallow"]["first_row"]["runs"][0]["p99_us"], 198)
            self.assertGreater(report["fixtures"]["shallow"]["first_row"]["p99_spread_ratio"], 0)
            with self.assertRaisesRegex(ValueError, "independent"):
                analyze(first, first)

    def test_missing_sample_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_run(root / "a", "first")
            csv_path = root / "a" / f"{STEM}.csv"
            lines = csv_path.read_text().splitlines()
            csv_path.write_text("\n".join(lines[:-1]) + "\n")
            with self.assertRaisesRegex(ValueError, "incomplete"):
                read_run(root / "a")

    def test_frozen_sql_and_environment_mismatch_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.write_run(root / "a", "first")
            self.write_run(root / "b", "second")
            spec_path = root / "a" / "fixture-spec.json"
            spec_path.write_text(spec_path.read_text().replace("SELECT", "SELECT DISTINCT"))
            with self.assertRaisesRegex(ValueError, "frozen fixture bytes"):
                read_run(root / "a")

            identity_path = root / "b" / f"{STEM}-fixtures.json"
            identity = json.loads(identity_path.read_text())
            identity["fixtures"][0]["query_sha256"] = "0" * 64
            identity_path.write_text(json.dumps(identity))
            with self.assertRaisesRegex(ValueError, "fixture SQL differs"):
                read_run(root / "b")

            self.write_run(root / "c", "third")
            self.write_run(root / "d", "fourth")
            second = read_run(root / "c")
            third = read_run(root / "d")
            third["manifest"]["platform"]["power_mode"] = "battery"
            with self.assertRaisesRegex(ValueError, "configuration differs"):
                analyze(second, third)


if __name__ == "__main__":
    unittest.main()
