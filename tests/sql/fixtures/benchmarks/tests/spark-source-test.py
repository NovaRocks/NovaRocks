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

"""Pure Spark source selection and supervised OSS publication regressions."""
import argparse
from contextlib import redirect_stderr
import io
import ast
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[5]
LOADER = ROOT / "tests/sql/fixtures/benchmarks/spark/write_standard_benchmark.py"
namespace = {"argparse": argparse, "os": os, "re": re}
source = ast.parse(LOADER.read_text())
functions = [node for node in source.body if isinstance(node, ast.FunctionDef) and node.name in {"parse_args", "s3_settings"}]
exec(compile(ast.Module(body=functions, type_ignores=[]), str(LOADER), "exec"), namespace)


class SparkSourceTest(unittest.TestCase):
    def arguments(self, *extra):
        return ["--suite", "ssb", "--scale", "1", "--raw-base-uri", "s3://bucket/root/raw", "--catalog", "fixture",
                "--database", "ssb", "--warehouse", "s3://bucket/root/warehouse", "--manifest-output", "s3://bucket/root/manifest",
                "--generator", "ssb-dbgen", "--generator-version", "version", *extra]

    def test_oss_has_exact_renewable_providers_without_ambient_material(self):
        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "ambient-canary", "AWS_SECRET_ACCESS_KEY": "ambient-secret"}, clear=True):
            args = namespace["parse_args"](self.arguments("--object-store-kind", "aliyun-oss", "--s3-region", "cn-beijing", "--s3-role-name", "fixture-role", "--s3-endpoint", "https://s3.oss-cn-beijing-internal.aliyuncs.com"))
        self.assertIsNone(args.s3_access_key)
        self.assertIsNone(args.s3_secret_key)
        catalog, hadoop = namespace["s3_settings"](args)
        self.assertEqual(catalog["client.credentials-provider"], "org.novarocks.fixture.auth.AliyunEcsCredentialsProvider")
        self.assertEqual(catalog["client.credentials-provider.role-name"], "fixture-role")
        self.assertEqual(catalog["client.region"], "cn-beijing")
        self.assertEqual(catalog["s3.path-style-access"], "false")
        self.assertEqual(hadoop["fs.s3a.aws.credentials.provider"], "org.novarocks.fixture.auth.AliyunEcsV1CredentialsProvider")
        self.assertEqual(hadoop["fs.s3a.path.style.access"], "false")
        self.assertEqual(hadoop["fs.s3a.connection.ssl.enabled"], "true")
        self.assertFalse(any("access-key" in key or "secret" in key or "token" in key for key in catalog))
        self.assertFalse(any(key.endswith(("access.key", "secret.key", "session.token")) for key in hadoop))

    def test_static_source_keeps_the_original_explicit_material_semantics(self):
        args = namespace["parse_args"](self.arguments("--s3-endpoint", "http://minio:9000", "--s3-access-key", "static-key", "--s3-secret-key", "static-secret"))
        catalog, hadoop = namespace["s3_settings"](args)
        self.assertEqual(catalog["s3.access-key-id"], "static-key")
        self.assertEqual(catalog["s3.path-style-access"], "true")
        self.assertEqual(hadoop["fs.s3a.aws.credentials.provider"], "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    def test_oss_refuses_missing_role_wrong_route_and_static_snapshot(self):
        base = self.arguments("--object-store-kind", "aliyun-oss", "--s3-region", "cn-beijing", "--s3-endpoint", "https://s3.oss-cn-beijing-internal.aliyuncs.com")
        for extra in [[], ["--s3-role-name", "bad/role"], ["--s3-role-name", "fixture-role", "--s3-access-key", "snapshot"]]:
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit): namespace["parse_args"]([*base, *extra])
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit): namespace["parse_args"](self.arguments("--object-store-kind", "aliyun-oss", "--s3-role-name", "fixture-role", "--s3-region", "cn-beijing", "--s3-endpoint", "http://minio:9000"))

    def test_publication_uses_original_machine_entrypoint_for_each_plain_operation(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            gateway = directory / "docker/iceberg-rest/machine_mc.py"
            gateway.parent.mkdir(parents=True)
            log = directory / "calls.jsonl"
            gateway.write_text("import json,sys\nfrom pathlib import Path\nrequest=json.load(sys.stdin)\nwith open(" + repr(str(log)) + ", 'a') as out: out.write(json.dumps({'args':sys.argv[1:], 'request':request})+'\\n')\noperation=request['operation']\nvalue={'ok':True}\nif operation=='get':\n Path(request['output']).write_text('object-body')\n value.update(etag='object-etag')\nelif operation=='stat': value.update(exists=True)\nelif operation=='list': value.update(objects=[{'uri':request['uri']+'/child'}])\nelif operation=='put': value.update(status=200)\nelse: raise SystemExit(9)\nprint(json.dumps(value))\n")
            resolved = directory / "resolved.json"
            contract = {"dataset_key": {"fixture_contract_id": "exact-key"}}
            resolved.write_text(json.dumps(contract))
            body = directory / "body"
            body.write_text("data")
            helper = ROOT / "tests/sql/fixtures/benchmarks/benchmark_fixture_publication.sh"
            script = f"""set -euo pipefail
source {shlex.quote(str(helper))}
fixture_publication_get s3://bucket/root/READY.json
[[ "$FIXTURE_PUBLICATION_BODY" == object-body && "$FIXTURE_PUBLICATION_ETAG" == object-etag ]]
fixture_publication_head s3://bucket/root/file
fixture_publication_list_prefix s3://bucket/root/manifest
fixture_publication_put s3://bucket/root/file {shlex.quote(str(body))}
"""
            environment = {**os.environ, "SOURCE_ROOT": str(directory), "resolved_dataset_file": str(resolved), "NOVA_ENV_OBJECT_STORE_KIND": "aliyun-oss", "NOVA_FIXTURE_MACHINE_ROOT": "/original-machine", "NOVA_FIXTURE_MACHINE_DAEMON": "original-daemon", "NOVA_FIXTURE_MACHINE_VERSION": "original-version", "NOVA_FIXTURE_MACHINE_GENERATION": "original-generation"}
            environment.pop("BENCHMARK_FIXTURE_STORAGE_DIR", None)
            result = subprocess.run(["bash", "-c", script], env=environment, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            requests = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual([item["request"]["operation"] for item in requests], ["get", "stat", "list", "put"])
            for item in requests:
                self.assertEqual(item["request"]["resolved"], contract)
                self.assertEqual(item["args"], ["--root", "/original-machine", "--daemon", "original-daemon", "--expected-version", "original-version", "--expected-generation", "original-generation", "--benchmark-oss"])


if __name__ == "__main__": unittest.main()
