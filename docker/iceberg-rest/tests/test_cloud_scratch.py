# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Closed cloud ownership and catalog-first cleanup with independent oracles."""
import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_runtime as runtime
import scratch
from test_scratch import Clock, NOW, POLICY, record as minio_record


def cloud_record():
    config = dict(kind="aliyun-oss", region="cn-beijing", bucket="novarocks-ci",
                  role_name="ci-role", root_prefix="dev/")
    value = minio_record()
    value["config"] = dict(object_store=config, benchmark_root="s3://novarocks-ci/dev/benchmark")
    value["storage_owner"] = scratch.cloud_storage_owner(value)
    value["server_warehouses"] = {"rest": "s3://novarocks-ci/dev/catalog/rest",
                                  "rest-mv": "s3://novarocks-ci/dev/catalog/rest-mv"}
    value["protected_benchmark_roots"] = [value["config"]["benchmark_root"]]
    value.pop("credentials")
    return value


def cloud_identity():
    return scratch.ScratchIdentity.create("s3://novarocks-ci/dev/worktree", now_ms=NOW, token="c" * 32)


class Store:
    timeout = 10

    def __init__(self):
        self.objects, self.uploads, self.calls = set(), set(), []

    def run_mc(self, *_args, **_kwargs):
        raise AssertionError("cloud cleanup cannot invoke mc or IAM")

    def run_oss(self, _record, args, *, deadline):
        self.calls.append((list(args), deadline))
        operation, _, uri = args[:3]
        if operation == "list":
            return json.dumps(dict(ok=True, objects=[dict(uri=value) for value in sorted(self.objects)
                                                     if value.startswith(uri)]))
        if operation == "list-multipart":
            return json.dumps(dict(ok=True, uploads=[dict(uri=value, upload_id=identity)
                             for value, identity in sorted(self.uploads) if value.startswith(uri)]))
        if operation == "delete":
            self.objects.discard(uri)
        elif operation == "abort-multipart":
            self.uploads.discard((uri, args[4]))
        else:
            raise AssertionError(operation)
        return json.dumps(dict(ok=True, status=204))


class CloudScopeTests(unittest.TestCase):
    def test_closed_schema_matches_all_machine_roots_and_aliases(self):
        record, identity = cloud_record(), cloud_identity()
        scope = scratch.RunStorageScope.from_record(identity, record)
        value = scope.as_dict()
        self.assertEqual(value["schema"], 2)
        self.assertEqual(value["storage_owner"], record["storage_owner"])
        self.assertTrue(scope.belongs(identity.scope_root.replace("s3://", "s3a://") + "/object"))
        self.assertTrue(scope.belongs(record["server_warehouses"]["rest"] + "/" + identity.resource_name("sql") + "/x"))
        self.assertFalse(scope.belongs(record["config"]["benchmark_root"] + "/dataset"))
        with self.assertRaises(runtime.RuntimeFailure):
            scope.iam_policy()
        for changes in [dict(endpoint="https://other"), dict(bucket="another-bucket"),
                        dict(root_prefix="another/"), dict(region="cn-shanghai"), dict(extra="unknown")]:
            bad = copy.deepcopy(value)
            bad["storage_owner"].update(changes)
            with self.subTest(changes=changes), self.assertRaises(runtime.RuntimeFailure):
                scratch.RunStorageScope.from_dict(bad).validate_against(identity, record)

    def test_every_root_is_inside_owner_and_protected_roots_remain_disjoint(self):
        record, identity = cloud_record(), cloud_identity()
        for field in ["rest", "rest-mv"]:
            bad = copy.deepcopy(record)
            bad["server_warehouses"][field] = "s3://novarocks-ci/outside/catalog"
            with self.subTest(field=field), self.assertRaises(runtime.RuntimeFailure):
                scratch.RunStorageScope.from_record(identity, bad)
        with self.assertRaises(runtime.RuntimeFailure):
            scratch.RunStorageScope.from_record(scratch.ScratchIdentity.create(
                "s3://novarocks-ci/outside", now_ms=NOW, token="d" * 32), record)
        record["protected_benchmark_roots"].append(identity.scope_root + "/protected")
        with self.assertRaises(runtime.RuntimeFailure):
            scratch.RunStorageScope.from_record(identity, record)


class CloudCleanupTests(unittest.TestCase):
    def setUp(self):
        self.clock, self.store = Clock(), Store()
        self.record, self.identity = cloud_record(), cloud_identity()
        self.scope = scratch.RunStorageScope.from_record(self.identity, self.record)
        self.sweeper = scratch.ScratchSweeper(self.record, self.store,
            wall_clock=self.clock.wall, monotonic=self.clock.mono)

    def barrier(self, run, _inventory, _deadline, report):
        self.assertFalse(any(args[0] in {"delete", "abort-multipart"} for args, _ in self.store.calls))
        report["catalog_barrier"][run] = dict(complete=True)

    def test_exact_cleanup_retains_other_runs_and_protected_data(self):
        own = self.record["server_warehouses"]["rest"] + "/" + self.identity.resource_name("sql") + "/table/metadata"
        other = self.record["server_warehouses"]["rest"] + "/foreign/metadata"
        protected = self.record["config"]["benchmark_root"] + "/READY"
        self.store.objects.update([self.identity.scope_root + "/data", own, other, protected])
        multipart = self.record["server_warehouses"]["rest-mv"] + "/" + self.identity.resource_name("mv") + "/part"
        self.store.uploads.add((multipart, "upload-1"))
        with patch.object(self.sweeper, "_catalog_inventory", return_value={}), \
             patch.object(self.sweeper, "_catalog_run", side_effect=self.barrier):
            result = self.sweeper.cleanup_run(self.scope, [], deadline=120)
        self.assertTrue(result["complete"], result)
        self.assertEqual(self.store.objects, {other, protected})
        self.assertEqual(self.store.uploads, set())
        self.assertTrue(all(deadline == 120 for _, deadline in self.store.calls))

    def test_missing_catalog_barrier_and_iam_pairs_cannot_reach_object_deletion(self):
        for users in [[], [{"user": "foreign", "policy": "foreign"}]]:
            with patch.object(self.sweeper, "_catalog_inventory", return_value={}), \
                 patch.object(self.sweeper, "_catalog_run", return_value=None):
                result = self.sweeper.cleanup_run(self.scope, users, deadline=120)
            self.assertFalse(result["complete"])
        self.assertEqual(self.store.calls, [])

    def test_cloud_never_uses_ttl_or_revocation_as_writer_exit(self):
        with self.assertRaises(runtime.RuntimeFailure):
            self.sweeper.sweep(expired_only=True, deadline=120)
        with self.assertRaises(runtime.RuntimeFailure):
            self.sweeper.revoke_users()
        self.assertEqual(self.store.calls, [])


if __name__ == "__main__":
    unittest.main()
