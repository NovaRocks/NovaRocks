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

"""Benchmark admission keeps its protected domain separate from scratch cleanup."""
import copy
from pathlib import Path
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import benchmark_oss
import fixture_runtime as runtime


class BenchmarkOssTests(unittest.TestCase):
    def setUp(self):
        self.snapshot = {"config": {"object_store": {"kind": "aliyun-oss"},
            "benchmark_root": "s3://test-bucket/fixture/benchmarks"}}
        self.resolved = benchmark_oss.resolve_current(self.snapshot, {"dataset_key": {"suite": "ssb", "scale": "0.01"}})
        self.backend = mock.Mock()
        self.backend.run_oss.return_value = '{"status":200}\n'

    def request(self, **changes):
        return {"operation": "stat", "resolved": copy.deepcopy(self.resolved), "uri": self.resolved["ready_uri"], **changes}

    def test_exact_current_contract_uses_original_supervised_backend_and_deadline(self):
        result = benchmark_oss.run_operation(self.snapshot, self.backend, self.request(), deadline=123.0)
        self.assertEqual(result, '{"status":200}\n')
        self.backend.run_oss.assert_called_once_with(self.snapshot, ["stat", "--uri", self.resolved["ready_uri"]], deadline=123.0)

    def test_sibling_or_spoofed_contract_never_dispatches(self):
        changed = copy.deepcopy(self.resolved)
        changed["contract"]["schema_version"] = "spoof"
        for request in [self.request(uri=self.resolved["dataset_root"] + "-sibling/READY.json"),
                        self.request(resolved=changed), self.request(uri="s3://test-bucket/fixture/worktrees/run/file")]:
            with self.subTest(request=request), self.assertRaises(runtime.RuntimeFailure):
                benchmark_oss.run_operation(self.snapshot, self.backend, request, deadline=123.0)
        self.backend.run_oss.assert_not_called()

    def test_no_deletion_or_additional_fields_are_authorized(self):
        for request in [self.request(operation="delete"), self.request(extra="ignored"),
                        self.request(operation="put", input="relative")]:
            with self.subTest(request=request), self.assertRaises(runtime.RuntimeFailure):
                benchmark_oss.run_operation(self.snapshot, self.backend, request, deadline=123.0)
        self.backend.run_oss.assert_not_called()
