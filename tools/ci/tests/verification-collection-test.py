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
"""Collection may continue after a real failure, never after missing safety facts."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.collection import check

# Reuse native producer-format fixtures without collecting the owner's tests.
spec = importlib.util.spec_from_file_location("materials_fixture_owner", Path(__file__).with_name("verification-materials-test.py"))
owner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner)


class Collection(unittest.TestCase):
    def fixture(self):
        value = owner.Materials()
        value.setUp()
        self.addCleanup(value.doCleanups)
        return value

    def rust(self, fail=False):
        value = self.fixture()
        value.rust(doctest=True)
        index = json.loads((value.root / "execution-index.json").read_text())
        index.update(complete=True, cancelled=[], exit_code=int(fail))
        for name in ("prepare", "error_manifest", "nextest", "doctest", "final_prepare"):
            index["stages"].setdefault(name, {"returncode": 0, "errors": []})
            index["stages"][name]["started"] = True
        if fail:
            index["stages"]["nextest"]["returncode"] = 100
            path = value.root / "nextest/default/junit.xml"
            path.write_text('<testsuites><testsuite name="pkg::integration"><testcase classname="pkg::integration" name="test_a"><failure message="original failure"/></testcase></testsuite></testsuites>')
        value.put("execution-index.json", index)
        return value

    def test_complete_pass_and_assertion_failure_continue_without_rewriting(self):
        for failed in (False, True):
            value = self.rust(failed)
            before = {str(path): path.read_bytes() for path in value.root.rglob("*") if path.is_file()}
            result = check("rust", value.root, int(failed))
            self.assertTrue(result["safe_to_continue"], result)
            self.assertEqual(result["observed_results"], {"pass": 1, "fail": 1} if failed else {"pass": 2})
            self.assertEqual(before, {str(path): path.read_bytes() for path in value.root.rglob("*") if path.is_file()})

    def test_complete_doctest_failure_remains_a_failure(self):
        value = self.rust()
        index = json.loads((value.root / "execution-index.json").read_text())
        index["exit_code"] = 1
        index["stages"]["doctest"]["returncode"] = 101
        value.put("execution-index.json", index)
        result = check("rust", value.root, 1)
        self.assertTrue(result["safe_to_continue"], result)
        self.assertEqual(result["observed_results"], {"pass": 1, "fail": 1})

    def test_rust_missing_cancelled_or_unsafe_facts_stop_even_when_passed(self):
        for fault in ("index", "cancel", "stage", "cleanup", "identity", "report", "selection", "bindings"):
            with self.subTest(fault=fault):
                value = self.rust()
                index = json.loads((value.root / "execution-index.json").read_text())
                if fault == "index": index["complete"] = False
                if fault == "cancel": index["cancelled"] = [15]
                if fault == "stage": index["stages"]["final_prepare"]["returncode"] = 1
                if fault == "report": (value.root / "nextest/default/junit.xml").unlink()
                if fault == "selection": value.put("inputs/selected-tests.json", {"rust-suites": {}})
                if fault == "bindings": value.put("inputs/test-artifacts.json", {"schema": 1, "binaries": {}})
                if fault == "cleanup": value.put("doctest-run/cleanup-summary.json", {"schema": 1, "complete": True, "ok": False, "errors": ["cleanup failed"]})
                if fault == "identity": value.put("identity/nextest.json", {"schema": 1, "scopes": {}})
                value.put("execution-index.json", index)
                self.assertFalse(check("rust", value.root, 0)["safe_to_continue"])

    def test_system_native_fail_and_pass_both_need_complete_cleanup(self):
        for failed in (False, True):
            value = self.fixture()
            path = value.system(code=int(failed), outcome="failed" if failed else "passed")
            self.assertTrue(check("invocation", path, int(failed))["safe_to_continue"])
            value.put("scenario-teardown.json", {"schema": 1, "fixture": {"teardown": {"status": "failed"}}, "cluster": {"shutdown": "ok"}})
            self.assertFalse(check("invocation", path, int(failed))["safe_to_continue"])

    def test_system_unknown_cancelled_and_contradictory_results_stop(self):
        for code, outcome in ((0, "Passed"), (1, "passed"), (0, "failed"), (137, "failed"), (23, "failed")):
            value = self.fixture()
            path = value.system(code=code, outcome=outcome)
            self.assertFalse(check("invocation", path, code)["safe_to_continue"])
        value = self.fixture()
        path = value.system()
        (value.root / "scenario-teardown.json").unlink()
        self.assertFalse(check("invocation", path, 0)["safe_to_continue"])

    def test_sql_native_failure_continues_without_a_disposition(self):
        value = self.fixture()
        records = value.sql()
        records[2].update(status="fail", failure={"class": "error_code", "error_code": "unregistered"},
                          oracle={"required": [{"step": 1, "kind": "result_set"}], "executed": []})
        records[-1]["exit_code"] = 1
        path = value.sql_file(records)
        before = path.read_bytes()
        self.assertTrue(check("sql", path, 1)["safe_to_continue"])
        self.assertEqual(path.read_bytes(), before)
        # An independently failed negative oracle remains a failure for a later
        # layer's consumer; it does not prevent safe independent collection.
        records[2]["oracle"]["executed"] = [{"step": 1, "kind": "expected_error", "outcome": "fail"}]
        self.assertTrue(check("sql", value.sql_file(records), 1)["safe_to_continue"])

    def test_sql_iceberg_view_oracles_allow_continuation_after_another_case_fails(self):
        value = self.fixture()
        records = value.sql()
        required = [{"step": 1, "kind": "iceberg_view_fixture"},
                    {"step": 2, "kind": "iceberg_view_fixture_unchanged"}]
        oracle = {"required": required,
                  "executed": [dict(assertion, outcome="pass", policy={}) for assertion in required]}
        records[2]["oracle"] = oracle
        records[3].update(status="fail", failure={"class": "error_code", "error_code": "unregistered"})
        records[-1]["exit_code"] = 1
        result = check("sql", value.sql_file(records), 1)
        self.assertTrue(result["safe_to_continue"], result)

        oracle["executed"].pop()
        result = check("sql", value.sql_file(records), 1)
        self.assertFalse(result["safe_to_continue"], result)

    def test_sql_parent_invocation_identity_and_exact_material_are_required(self):
        value = self.fixture()
        sql = value.sql_file(value.sql())
        identity = {"platform": {"system": "Linux", "machine": "x86_64"}}
        invocation = {"schema": 1, "kind": "sql-suite", "units": [], "ended_at": "now", "exit": {"code": 0},
                      "inputs_start": identity, "inputs_end": identity, "inputs_stable": True,
                      "materials": {"sql_results": str(sql)}}
        path = value.put("invocations/0001.json", invocation)
        self.assertTrue(check("sql", sql, 0, path)["safe_to_continue"])
        invocation["inputs_end"] = {"changed": "binary"}
        value.put("invocations/0001.json", invocation)
        self.assertFalse(check("sql", sql, 0, path)["safe_to_continue"])
        invocation["inputs_end"] = identity
        invocation["materials"]["sql_results"] = "other.jsonl"
        value.put("invocations/0001.json", invocation)
        self.assertFalse(check("sql", sql, 0, path)["safe_to_continue"])

    def test_sql_missing_or_diagnostic_pass_is_not_collection_complete(self):
        for fault in ("end", "cleanup", "unknown", "identity", "diagnostic", "exit"):
            value = self.fixture()
            records = value.sql()
            if fault == "end": records.pop()
            if fault == "cleanup": records[4]["cleanup"]["status"] = "failed"
            if fault == "unknown": records[3]["status"] = "not_run"
            if fault == "identity": records[4]["inputs_stable"] = False
            if fault == "diagnostic": records[0]["verification"]["verify_enabled"] = False
            if fault == "exit": records[-1]["exit_code"] = 1
            path = value.sql_file(records)
            self.assertFalse(check("sql", path, 1 if fault == "exit" else 0)["safe_to_continue"], fault)


if __name__ == "__main__":
    unittest.main()
