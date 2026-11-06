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

"""Bound NovaRocks tool calls leave external FE mysql commands untouched."""
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0,str(Path(__file__).resolve().parent))
import agg_plan_diff as agg
import rf_plan_diff as rf
import trino_query


class ToolTests(unittest.TestCase):
    def test_bound_tool_preserves_text_input_and_explicit_parameters(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            tool = root / "query-tool"
            capture = root / "captured.json"
            tool.write_text("#!/usr/bin/env python3\nimport json,sys\nfrom pathlib import Path\n"
                            + "Path("+repr(str(capture))+").write_text(json.dumps({'args':sys.argv[1:],'sql':sys.stdin.read()}))\n"
                            + "sys.stdout.write('PLAN FRAGMENT 0\\n  HASH AGGREGATE (LOCAL)\\n')\n")
            tool.chmod(0o755)
            receipt = root / "artifacts.json"
            receipt.write_text(json.dumps({"novarocks-trino-query":{"path":str(tool),"sha256":hashlib.sha256(tool.read_bytes()).hexdigest()}}))
            endpoint = agg.Endpoint("nr","127.0.0.1","8088","root","nr")
            sql = "SELECT 'a;b'"
            output = agg.run_novarocks(endpoint,sql,5,"tpch",artifact_receipt=str(receipt),nr_catalog="ice")
            self.assertEqual(output,"PLAN FRAGMENT 0\n  HASH AGGREGATE (LOCAL)\n")
            captured = json.loads(capture.read_text())
            self.assertEqual(captured["sql"],"EXPLAIN VERBOSE SELECT 'a;b';")
            self.assertEqual(captured["args"], ["--host","127.0.0.1","--port","8088","--user","root","--time-zone","UTC","--timeout-secs","5","--mode","text-column","--catalog","ice","--schema","tpch"])
            rf_output = rf.run_novarocks(rf.Endpoint("nr","127.0.0.1","8088","root"),
                                         "-- input comment\nSELECT 'a;b';",5,
                                         artifact_receipt=str(receipt))
            self.assertEqual(rf_output,output)
            captured = json.loads(capture.read_text())
            self.assertEqual(captured["sql"],"EXPLAIN VERBOSE SELECT 'a;b';")
            tool.write_text("changed")
            with self.assertRaisesRegex(RuntimeError,"binding failed"):
                trino_query.run_text_column(endpoint,sql,5,"tpch",receipt=receipt)

    def test_missing_optional_record_has_no_path_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory)/"artifacts.json"; receipt.write_text("{}")
            with self.assertRaisesRegex(RuntimeError,"binding failed"):
                trino_query.bound_tool(receipt)

    def test_correctness_sql_paths_and_novarocks_dispatch(self):
        self.assertTrue(agg.case_to_sql_path("tpc-h/q1").is_file())
        self.assertTrue(rf.case_to_sql_path("ssb/q1.1").is_file())
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(agg,"run_text_column",return_value="HASH AGGREGATE (LOCAL)\n") as typed, mock.patch.object(agg,"run_mysql",return_value="AGGREGATE (merge finalize)\n") as mysql:
            agg.collect_case(agg.Endpoint("nr","n","8","u","nr"),"tpc-h/q1",Path(directory),10,artifact_receipt="receipt")
            typed.assert_called_once(); mysql.assert_not_called()
            agg.collect_case(agg.Endpoint("fe","f","9","u","fe"),"tpc-h/q1",Path(directory),10)
            mysql.assert_called_once()
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(rf,"run_text_column",return_value="runtime filter\n") as typed, mock.patch.object(rf,"run_mysql",return_value="runtime filter\n") as mysql:
            rf.collect_case("tpc-h/q1",[rf.Endpoint("fe","f","9","u"),rf.Endpoint("nr","n","8","u")],Path(directory),10,artifact_receipt="receipt")
            typed.assert_called_once(); mysql.assert_called_once()

    def test_external_fe_mysql_arguments_are_unchanged(self):
        completed = mock.Mock(returncode=0,stdout="original\n")
        with mock.patch.object(agg.subprocess,"run",return_value=completed) as run:
            agg.run_mysql(agg.Endpoint("fe","host","9030","root","fe"),"EXPLAIN SELECT 1",9,"db")
            self.assertEqual(run.call_args.args[0],["mysql","-h","host","-P","9030","-u","root","--batch","--raw","-D","db","-e","EXPLAIN SELECT 1"])


if __name__ == "__main__":
    unittest.main()
