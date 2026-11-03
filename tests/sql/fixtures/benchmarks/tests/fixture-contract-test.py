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

"""Focused regression checks for the shared benchmark fixture contract."""

from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / "docker/fixture-inputs"))
RESOLVER_PATH = ROOT / "tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py"
SPEC = importlib.util.spec_from_file_location("fixture_resolver", RESOLVER_PATH)
RESOLVER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(RESOLVER)


class FixtureContractTest(unittest.TestCase):
    def setUp(self):
        self.model = RESOLVER.load_model(
            ROOT / "tests/sql/fixtures/benchmarks/benchmark_tools.toml"
        )

    def resolve(self, model=None, suite="ssb", scale="1"):
        return RESOLVER.resolve_fixture(model or self.model, ROOT, suite, scale)

    def test_deterministic_key_has_no_writer_or_warehouse(self):
        first = self.resolve()
        second = self.resolve()
        self.assertEqual(first, second)
        self.assertEqual(first["dataset_key"]["scale"], "1")
        self.assertNotIn("warehouse", first)
        self.assertNotIn("staging_identity", first)
        self.assertTrue(first["ready_uri"].endswith("/READY.json"))
        self.assertTrue(first["staging_parent"].endswith("/staging"))

    def test_object_store_provider_and_source_recipe_remain_dataset_inputs(self):
        baseline = self.resolve()
        cloud = RESOLVER.resolve_fixture(self.model, ROOT, "ssb", "1", object_store_kind="aliyun-oss")
        self.assertNotEqual(baseline["dataset_key"], cloud["dataset_key"])
        self.assertEqual(cloud["producer_fingerprint"]["object_store_kind"], "aliyun-oss")
        self.assertEqual(baseline["producer_fingerprint"]["source_inputs"], cloud["producer_fingerprint"]["source_inputs"])

    def test_scale_normalization(self):
        self.assertEqual(self.resolve(scale="1.0")["dataset_key"], self.resolve(scale="1")["dataset_key"])
        self.assertEqual(
            self.resolve(suite="tpc-ds", scale="1gb")["dataset_key"]["scale"], "1GB"
        )

    def test_contract_inputs_change_the_key(self):
        baseline = self.resolve()["fixture_contract_id"]
        mutations = [
            ("ssb", "version", "different-generator"),
            ("fixture", "contract_schema_version", 99),
            ("fixture.loader", "schema_version", "next-schema"),
            ("fixture.loader", "statistics_contract", "other-statistics"),
            ("fixture.spark_runtime", "spark_version", "9.9.9"),
        ]
        for section, field, replacement in mutations:
            model = deepcopy(self.model)
            target = model
            for part in section.split("."):
                target = target[part]
            target[field] = replacement
            self.assertNotEqual(baseline, self.resolve(model)["fixture_contract_id"], section)

    def test_standard_layout_is_pinned_to_tested_p4_candidate(self):
        layouts = {
            (layout["suite"], layout["table"]): {
                "range_partitions": layout["range_partitions"],
                "target_file_size_bytes": layout["target_file_size_bytes"],
            }
            for layout in self.model["fixture"]["table_layouts"]
        }
        self.assertEqual(
            layouts,
            {
                ("ssb", "lineorder"): {
                    "range_partitions": 4,
                    "target_file_size_bytes": 64 * 1024 * 1024,
                },
                ("tpc-h", "lineitem"): {
                    "range_partitions": 4,
                    "target_file_size_bytes": 256 * 1024 * 1024,
                },
                ("tpc-h", "orders"): {
                    "range_partitions": 1,
                    "target_file_size_bytes": 256 * 1024 * 1024,
                },
                ("tpc-ds", "store_sales"): {
                    "range_partitions": 4,
                    "target_file_size_bytes": 256 * 1024 * 1024,
                },
                ("tpc-ds", "catalog_sales"): {
                    "range_partitions": 4,
                    "target_file_size_bytes": 256 * 1024 * 1024,
                },
                ("tpc-ds", "web_sales"): {
                    "range_partitions": 4,
                    "target_file_size_bytes": 256 * 1024 * 1024,
                },
                ("tpc-ds", "inventory"): {
                    "range_partitions": 1,
                    "target_file_size_bytes": 256 * 1024 * 1024,
                },
            },
        )

    def test_producer_file_bytes_change_the_key(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            for relative in self.model["fixture"]["producer_inputs"].values():
                source = ROOT / relative
                destination = workspace / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
            baseline = RESOLVER.resolve_fixture(self.model, workspace, "ssb", "1")
            loader = workspace / self.model["fixture"]["producer_inputs"]["loader"]
            loader.write_bytes(loader.read_bytes() + b"\n# contract-test mutation\n")
            changed = RESOLVER.resolve_fixture(self.model, workspace, "ssb", "1")
            self.assertNotEqual(baseline["fixture_contract_id"], changed["fixture_contract_id"])

    def test_recommendation_updates_do_not_change_existing_dataset_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Path(temporary)
            for relative in self.model["fixture"]["producer_inputs"].values():
                destination = workspace / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, destination)
            baseline = RESOLVER.resolve_fixture(self.model, workspace, "ssb", "1")
            recommended = workspace / "docker/fixture-inputs/versions.json"
            recommended.parent.mkdir(parents=True, exist_ok=True)
            recommended.write_text('{"versions":{"spark":"different"}}')
            changed = RESOLVER.resolve_fixture(self.model, workspace, "ssb", "1")
            self.assertEqual(baseline["dataset_key"], changed["dataset_key"])
            self.assertEqual(baseline["producer_fingerprint"], changed["producer_fingerprint"])



    def test_ready_and_error_schemas_fail_closed(self):
        resolved = self.resolve()
        result = {
            "schema_version": 1,
            "dataset_key": resolved["dataset_key"],
            "state": "ReadyValid",
            "reused": True,
            "built": False,
            "exact_warehouse": "s3://novarocks/shared/benchmarks/staging/w1/warehouse",
            "manifest_uri": "s3://novarocks/shared/benchmarks/staging/w1/manifest.json",
            "publication": {"ready_uri": resolved["ready_uri"], "etag": "etag", "identity": "pub-1"},
        }
        RESOLVER.validate_ensure_result(result, resolved["dataset_key"])
        for field in ("exact_warehouse", "manifest_uri"):
            malformed = deepcopy(result)
            malformed.pop(field)
            with self.assertRaises(ValueError):
                RESOLVER.validate_ensure_result(malformed, resolved["dataset_key"])
        error = {
            "schema_version": 1,
            "error": "ready_invalid",
            "dataset_key": resolved["dataset_key"],
            "message": "broken object",
        }
        RESOLVER.validate_error(error, resolved["dataset_key"])
        error["error"] = "unknown"
        with self.assertRaises(ValueError):
            RESOLVER.validate_error(error, resolved["dataset_key"])




if __name__ == "__main__":
    unittest.main()
