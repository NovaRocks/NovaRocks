# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Scoped-operation contracts; fake transport owns no external resources."""

import importlib.util
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location("oss_ops", Path(__file__).parents[2] / "oss_ops.py")
OPS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(OPS)
CONFIG = dict(kind="aliyun-oss", region="cn-beijing", bucket="fixture-bucket",
              role_name="fixture-role", root_prefix="test-root")
URI = "s3://fixture-bucket/test-root/run/object"


class ObjectResult(io.BytesIO):
    status = 200
    etag = "content-etag"


class MissingObject(Exception):
    code = "NoSuchKey"


class FakeBucket:
    def __init__(self):
        self.objects = {}
        self.calls = []
        self.pages = []
        self.upload_pages = []
        self.results = []

    def put_object(self, key, stream):
        self.calls.append(("put", key))
        self.objects[key] = stream.read()
        return SimpleNamespace(status=200, etag="content-etag")

    def get_object(self, key):
        self.calls.append(("get", key))
        result = ObjectResult(self.objects[key])
        self.results.append(result)
        return result

    def head_object(self, key):
        self.calls.append(("head", key))
        if key not in self.objects:
            raise MissingObject()
        return SimpleNamespace(status=200, etag="content-etag", content_length=len(self.objects[key]))

    def list_objects_v2(self, **kwargs):
        self.calls.append(("list", kwargs))
        return self.pages.pop(0)

    def delete_object(self, key):
        self.calls.append(("delete", key))
        self.objects.pop(key, None)
        return SimpleNamespace(status=204)

    def list_multipart_uploads(self, **kwargs):
        self.calls.append(("list-multipart", kwargs))
        return self.upload_pages.pop(0)

    def abort_multipart_upload(self, key, upload_id):
        self.calls.append(("abort", key, upload_id))
        return SimpleNamespace(status=204)


class AtomicCredentials:
    def __init__(self, generation):
        self.generation = generation

    def get_access_key_id(self):
        return "id-" + str(self.generation)

    def get_access_key_secret(self):
        return "secret-" + str(self.generation)

    def get_security_token(self):
        return "token-" + str(self.generation)


class RotatingSource:
    def __init__(self):
        self.calls = 0

    def get_credentials(self):
        self.calls += 1
        return AtomicCredentials(self.calls)


class OssOperationsTest(unittest.TestCase):
    def setUp(self):
        self.bucket = FakeBucket()
        self.close_calls = []
        self.ops = OPS.OssOperations(CONFIG, bucket=self.bucket,
                                     close=lambda: self.close_calls.append("closed"))

    def tearDown(self):
        self.ops.close()

    def test_one_atomic_value_per_signature_and_rotation(self):
        source = RotatingSource()
        adapter = OPS.EcsCredentials("fixture-role", source=source,
                                     convert=lambda *parts: tuple(parts))
        self.assertEqual(adapter.get_credentials(), ("id-1", "secret-1", "token-1"))
        self.assertEqual(adapter.get_credentials(), ("id-2", "secret-2", "token-2"))
        self.assertEqual(source.calls, 2)
        adapter.close()
        with self.assertRaises(OPS.OpsError):
            adapter.get_credentials()
        self.assertEqual(source.calls, 2)

    def test_put_get_delete_exact_object_and_preserve_sibling(self):
        sibling = "test-root/other/object"
        self.bucket.objects[sibling] = b"keep"
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "data"
            source.write_bytes(bytes(range(256)))
            self.assertEqual(self.ops.put(URI, source)["status"], 200)
        self.assertEqual(self.ops.get(URI), bytes(range(256)))
        self.assertTrue(self.bucket.results[-1].closed)
        self.assertEqual(self.ops.stat(URI)["size"], 256)
        self.assertEqual(self.ops.delete(URI)["status"], 204)
        self.assertFalse(self.ops.stat(URI)["exists"])
        self.assertEqual(self.bucket.objects, {sibling: b"keep"})

    def test_every_operation_rejects_other_bucket_or_sibling_prefix_before_io(self):
        for uri in ["s3://other-bucket/test-root/run/object",
                    "s3://fixture-bucket/test-root-other/run/object"]:
            for operation in [lambda: self.ops.put(uri, "unused"),
                              lambda: self.ops.get(uri), lambda: self.ops.stat(uri),
                              lambda: self.ops.list(uri), lambda: self.ops.delete(uri),
                              lambda: self.ops.list_multipart(uri),
                              lambda: self.ops.abort_multipart(uri, "upload")]:
                with self.assertRaises(OPS.OpsError):
                    operation()
        self.assertEqual(self.bucket.calls, [])

    def test_directory_listing_is_component_scoped_and_covers_all_pages(self):
        def row(key):
            return SimpleNamespace(key=key, size=4, etag="etag", last_modified=1)
        self.bucket.pages = [
            SimpleNamespace(object_list=[row("test-root/run/a")], is_truncated=True,
                            next_continuation_token="next"),
            SimpleNamespace(object_list=[row("test-root/run/b")], is_truncated=False)]
        result = self.ops.list("s3://fixture-bucket/test-root/run")
        self.assertEqual([r["uri"] for r in result["objects"]],
                         ["s3://fixture-bucket/test-root/run/a", "s3://fixture-bucket/test-root/run/b"])
        self.assertEqual(self.bucket.calls[0][1]["prefix"], "test-root/run/")
        self.assertEqual(self.bucket.calls[1][1]["continuation_token"], "next")

    def test_raw_controls_cannot_be_stripped_into_an_authorized_uri(self):
        for uri in ["s3://fixture-bucket/test-root/ru\nn/object",
                    "\ts3://fixture-bucket/test-root/run/object",
                    "s3://fixture-bucket/test-root/run/object\r"]:
            with self.assertRaises(OPS.OpsError):
                self.ops.delete(uri)
        self.assertEqual(self.bucket.calls, [])

    def test_multipart_listing_and_abort_use_exact_upload_identity(self):
        upload = SimpleNamespace(key="test-root/run/part", upload_id="original-upload", initiation_date=10)
        self.bucket.upload_pages = [SimpleNamespace(upload_list=[upload], is_truncated=False)]
        result = self.ops.list_multipart("s3://fixture-bucket/test-root/run/")
        self.assertEqual(result["uploads"][0]["upload_id"], "original-upload")
        self.ops.abort_multipart(result["uploads"][0]["uri"], "original-upload")
        self.assertEqual(self.bucket.calls[-1], ("abort", "test-root/run/part", "original-upload"))

    def test_download_replaces_target_after_complete_read_and_closes_response(self):
        self.bucket.objects["test-root/run/object"] = b"new-complete-content"
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "download"
            target.write_bytes(b"old")
            self.ops.download(URI, target)
            self.assertEqual(target.read_bytes(), b"new-complete-content")
            self.assertEqual(list(Path(directory).iterdir()), [target])
        self.assertTrue(self.bucket.results[-1].closed)

    def test_failed_download_retains_original_target_and_cleans_partial(self):
        class FailedRead(ObjectResult):
            def read(self, size=-1):
                raise OSError("transport failed")
        result = FailedRead(b"partial")
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(self.bucket, "get_object", return_value=result):
            target = Path(directory) / "download"
            target.write_bytes(b"old")
            with self.assertRaises(OSError):
                self.ops.download(URI, target)
            self.assertEqual(target.read_bytes(), b"old")
            self.assertEqual(list(Path(directory).iterdir()), [target])
        self.assertTrue(result.closed)

    def test_close_is_observed_once_and_prevents_late_operations(self):
        self.ops.close()
        self.ops.close()
        self.assertEqual(self.close_calls, ["closed"])
        with self.assertRaises(OPS.OpsError):
            self.ops.get(URI)
        self.assertEqual(self.bucket.calls, [])

    def test_error_result_does_not_render_exception_body_or_secrets(self):
        error = RuntimeError("access-key secret token response-body")
        result = OPS.error_result(error)
        self.assertEqual(result, {"ok": False, "error": "RuntimeError", "status": None})
        self.assertNotIn("secret", json.dumps(result))


if __name__ == "__main__":
    unittest.main()
