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

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent
COMBINER = ROOT / "combine_ac32.py"


def observed(value: int) -> dict[str, object]:
    return {"value": value, "source": "test.owner.metric", "observed": True}


def execution_records() -> list[dict[str, object]]:
    names = [
        "throughput",
        "cpu",
        "peak_memory",
        "writer_queue_blocked_time",
        "final_aggregate_blocked_time",
        "exchange_encoded_payload_bytes",
        "exchange_sent_payload_bytes",
        "provider_calls_on_driver_thread",
    ]
    records: list[dict[str, object]] = [
        {
            "record": "config",
            "schema_version": 3,
            "warmup_rounds": 1,
            "measurement_rounds": 2,
            "measurement_scope": "execution_writer_exchange_decode_finish",
            "publication_io_observer": {"observed": False},
        }
    ]
    for case in ("empty_auxiliary", "theta_auxiliary"):
        for iteration in range(3):
            values = {name: observed(1) for name in names}
            values["provider_calls_on_driver_thread"] = observed(0)
            values["exchange_encoded_payload_bytes"] = observed(32)
            values["exchange_sent_payload_bytes"] = observed(32)
            records.append(
                {
                    "record": "sample",
                    "schema_version": 3,
                    "case": case,
                    "phase": "warmup" if iteration == 0 else "measurement",
                    "observations": values,
                }
            )
        records.append({"record": "summary", "case": case, "measurement_rounds": 2})
    return records


def iceberg_report() -> dict[str, object]:
    observations = {
        name: observed(0)
        for name in (
            "data_input_opens",
            "data_exists_calls",
            "data_metadata_calls",
            "data_read_calls",
            "data_reader_calls",
            "data_read_bytes",
        )
    }
    for name in (
        "puffin_write_calls",
        "puffin_write_bytes",
        "puffin_read_calls",
        "puffin_read_bytes",
        "puffin_input_opens",
        "puffin_output_opens",
        "puffin_metadata_calls",
    ):
        observations[name] = observed(1)
    return {
        "record": "iceberg_publication_io",
        "schema_version": 1,
        "scope": "one_eager_collect_on_write_attempt",
        "observations": observations,
    }


class CombineAc32Test(unittest.TestCase):
    def run_combiner(
        self,
        execution: list[dict[str, object]],
        iceberg: dict[str, object] | None,
        source_clean: bool = True,
    ) -> tuple[subprocess.CompletedProcess[str], dict[str, object]]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            execution_path = root / "execution.jsonl"
            execution_path.write_text(
                "\n".join(json.dumps(record) for record in execution) + "\n"
            )
            iceberg_path = root / "iceberg.log"
            if iceberg is not None:
                iceberg_path.write_text(
                    "NCP8_AC32_ICEBERG_IO=" + json.dumps(iceberg) + "\n"
                )
            else:
                iceberg_path.write_text("test completed without owner report\n")
            output = root / "report.json"
            result = subprocess.run(
                [
                    sys.executable,
                    str(COMBINER),
                    "--execution-jsonl",
                    str(execution_path),
                    "--iceberg-test-log",
                    str(iceberg_path),
                    "--output",
                    str(output),
                    "--revision",
                    "test-revision",
                    "--execution-exit",
                    "0",
                    "--iceberg-exit",
                    "0",
                    "--source-clean",
                    "true" if source_clean else "false",
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            return result, json.loads(output.read_text())

    def test_complete_report_requires_both_owner_observers(self) -> None:
        result, report = self.run_combiner(execution_records(), iceberg_report())

        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["issues"], [])

    def test_missing_iceberg_owner_report_is_incomplete(self) -> None:
        result, report = self.run_combiner(execution_records(), None)

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(report["status"], "incomplete")
        self.assertTrue(any("exactly one Iceberg" in issue for issue in report["issues"]))

    def test_unobserved_metric_is_incomplete(self) -> None:
        report = iceberg_report()
        report["observations"]["puffin_read_bytes"]["observed"] = False

        result, combined = self.run_combiner(execution_records(), report)

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(combined["status"], "incomplete")
        self.assertTrue(any("puffin_read_bytes" in issue for issue in combined["issues"]))

    def test_dirty_source_identity_is_incomplete(self) -> None:
        result, report = self.run_combiner(
            execution_records(), iceberg_report(), source_clean=False
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(report["status"], "incomplete")
        self.assertFalse(report["source_clean"])
        self.assertTrue(any("worktree is dirty" in issue for issue in report["issues"]))

    def test_invalid_execution_measurement_is_incomplete(self) -> None:
        execution = execution_records()
        execution[1]["observations"]["throughput"]["value"] = 0

        result, report = self.run_combiner(execution, iceberg_report())

        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(any("throughput must be" in issue for issue in report["issues"]))

    def test_data_reread_is_incomplete(self) -> None:
        iceberg = iceberg_report()
        iceberg["observations"]["data_read_bytes"]["value"] = 8

        result, report = self.run_combiner(execution_records(), iceberg)

        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(any("proves a data reread" in issue for issue in report["issues"]))


if __name__ == "__main__":
    unittest.main()
