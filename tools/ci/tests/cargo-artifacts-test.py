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
"""Actual Cargo artifact receipt collection and optional tool verification."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "cargo-artifacts.py"
REQUIRED = ("novarocks", "novarocks-sql-test", "novarocks-system-tests", "novarocks-error-manifest")


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cargo = self.root / "cargo.jsonl"
        self.receipt = self.root / "artifacts.json"

    def artifact(self, name, *, test=False):
        path = self.root / (name + ("-test" if test else ""))
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o755)
        return {"reason":"compiler-artifact", "executable":str(path),
                "target":{"name":name,"kind":["bin"]},"profile":{"test":test}}

    def invoke(self, *args):
        return subprocess.run([sys.executable, str(SCRIPT), *map(str,args)], capture_output=True, text=True)

    def collect(self, extras=()):
        artifacts = [self.artifact(name) for name in REQUIRED] + list(extras)
        self.cargo.write_text("".join(json.dumps(item)+"\n" for item in artifacts))
        result = self.invoke("collect", self.cargo, self.receipt)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(self.receipt.read_text())

    def test_only_four_main_binaries_remain_required(self):
        records = self.collect()
        self.assertEqual(set(records), set(REQUIRED))
        result = self.invoke("verify", self.receipt, "novarocks-trino-query")
        self.assertNotEqual(result.returncode, 0)

    def test_optional_non_test_tool_is_bound_and_changes_are_refused(self):
        tool = self.artifact("novarocks-trino-query")
        records = self.collect([tool])
        result = self.invoke("verify", self.receipt, "novarocks-trino-query")
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(result.stdout.strip(),records["novarocks-trino-query"]["path"])
        Path(tool["executable"]).write_text("#!/bin/sh\nexit 1\n")
        self.assertNotEqual(self.invoke("verify",self.receipt,"novarocks-trino-query").returncode,0)

    def test_test_harness_executable_cannot_masquerade_as_tool(self):
        records = self.collect([self.artifact("novarocks-trino-query",test=True)])
        self.assertNotIn("novarocks-trino-query",records)

    def test_missing_required_binary_still_fails(self):
        self.cargo.write_text(json.dumps(self.artifact("novarocks-trino-query"))+"\n")
        self.assertNotEqual(self.invoke("collect",self.cargo,self.receipt).returncode,0)


if __name__ == "__main__":
    unittest.main()
