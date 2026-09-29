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
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[5]
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
            lock = workspace / "docker/fixture-inputs/lock.json"
            lock.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / "docker/fixture-inputs/lock.json", lock)
            baseline = RESOLVER.resolve_fixture(self.model, workspace, "ssb", "1")
            loader = workspace / self.model["fixture"]["producer_inputs"]["loader"]
            loader.write_bytes(loader.read_bytes() + b"\n# contract-test mutation\n")
            changed = RESOLVER.resolve_fixture(self.model, workspace, "ssb", "1")
            self.assertNotEqual(baseline["fixture_contract_id"], changed["fixture_contract_id"])

    def test_lock_projection_only_tracks_spark_semantics(self):
        lock = json.loads((ROOT / "docker/fixture-inputs/lock.json").read_text())
        declarations = self.model["fixture"]["producer_lock_projections"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "docker/fixture-inputs/lock.json"
            path.parent.mkdir(parents=True)
            def projection(value):
                path.write_text(json.dumps(value))
                return RESOLVER.producer_lock_projections(root, declarations)
            baseline = projection(lock)
            positives = [
                ("images", "iceberg-spark-base", "manifest_digest", "sha256:new"),
                ("images", "iceberg-spark-base", "source", "different/spark"),
                ("images", "iceberg-spark-base", "platform", "linux/other"),
                ("derived_images", "iceberg-spark", "platform", "linux/other"),
                ("derived_images", "iceberg-spark", "dockerfile", "other/Dockerfile"),
                ("artifacts", "hadoop-aws-3.3.4.jar", "sha1", "different"),
                ("artifacts", "hadoop-aws-3.3.4.jar", "bytes", 42),
                ("artifacts", "hadoop-aws-3.3.4.jar", "url", "https://other/jar"),
            ]
            for section, name, field, value in positives:
                changed = deepcopy(lock)
                changed[section][name][field] = value
                self.assertNotEqual(baseline, projection(changed), (section, field))
            changed = deepcopy(lock)
            changed["images"]["iceberg-rest"]["manifest_digest"] = "other"
            changed["images"]["paimon-spark-base"]["source"] = "other"
            changed["artifacts"]["paimon-s3.jar"]["sha1"] = "other"
            changed["images"]["iceberg-spark-base"]["alias"] = "transport-only"
            changed["derived_images"]["iceberg-spark"]["alias"] = "transport-only"
            changed["derived_images"]["iceberg-spark"]["build_args"] = {"RENAMED": "unused"}
            changed["unrelated_ports"] = [1001, 2002]
            self.assertEqual(baseline, projection(changed))
            self.assertNotIn("docker/iceberg-rest/shared.env", self.model["fixture"]["producer_inputs"].values())

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
