#!/usr/bin/env python3
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

"""Explicit, scoped OSS operations using the provisioned official SDK.

The caller owns run admission, deadlines, writer exit and deletion authority.
This module enforces its configured bucket/prefix and owns no IAM lifecycle.
"""

import argparse
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile
from urllib.parse import urlsplit


class OpsError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _component_path(value):
    if (not isinstance(value, str) or not value or any(c in value for c in "%*?#")
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise OpsError("InvalidStorageScope")
    result = value.removesuffix("/")
    if any(part in ("", ".", "..") for part in result.split("/")):
        raise OpsError("InvalidStorageScope")
    return result


def validate_config(config):
    if (not isinstance(config, dict)
            or set(config) != {"kind", "region", "bucket", "role_name", "root_prefix"}
            or config.get("kind") != "aliyun-oss"):
        raise OpsError("InvalidObjectStoreConfiguration")
    for name in ("region", "bucket", "role_name"):
        if not isinstance(config[name], str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*", config[name]):
            raise OpsError("InvalidObjectStoreConfiguration")
    result = dict(config)
    result["root_prefix"] = _component_path(config["root_prefix"]) + "/"
    return result


class EcsCredentials:
    """Adapt a fresh atomic SDK value for each actual OSS signing request."""

    def __init__(self, role_name, *, source=None, convert=None):
        if source is None:
            from alibabacloud_credentials.provider import EcsRamRoleCredentialsProvider
            source = EcsRamRoleCredentialsProvider(
                role_name=role_name, disable_imds_v1=True, async_update_enabled=False)
        if convert is None:
            from oss2.credentials import Credentials
            convert = Credentials
        self.source = source
        self.convert = convert
        self.closed = False

    def get_credentials(self):
        if self.closed:
            raise OpsError("CredentialProviderClosed")
        value = self.source.get_credentials()
        return self.convert(value.get_access_key_id(), value.get_access_key_secret(),
                            value.get_security_token())

    def close(self):
        self.closed = True


class OssOperations:
    def __init__(self, config, *, timeout=None, bucket=None, close=None):
        self.config = validate_config(config)
        self._close = close or (lambda: None)
        self._credentials = None
        self.closed = False
        if bucket is None:
            import oss2
            self._credentials = EcsCredentials(self.config["role_name"])
            session = oss2.Session()
            self._close = session.session.close
            try:
                bucket = oss2.Bucket(
                    oss2.ProviderAuthV4(self._credentials),
                    "https://oss-" + self.config["region"] + "-internal.aliyuncs.com",
                    self.config["bucket"], region=self.config["region"],
                    session=session, connect_timeout=timeout)
            except Exception:
                session.session.close()
                self._credentials.close()
                raise
        self.bucket = bucket

    def _key(self, uri):
        if self.closed:
            raise OpsError("ObjectStoreClosed")
        if not isinstance(uri, str):
            raise OpsError("InvalidObjectUri")
        if any(ord(char) < 32 or ord(char) == 127 for char in uri):
            raise OpsError("InvalidObjectUri")
        parsed = urlsplit(uri)
        if (parsed.scheme != "s3" or parsed.netloc != self.config["bucket"]
                or parsed.query or parsed.fragment or not parsed.path.startswith("/")):
            raise OpsError("ObjectOutsideStorageScope")
        key = parsed.path[1:]
        _component_path(key)
        if not key.startswith(self.config["root_prefix"]):
            raise OpsError("ObjectOutsideStorageScope")
        return key

    def _directory(self, uri):
        return self._key(uri.removesuffix("/") + "/")

    def _uri(self, key):
        uri = "s3://" + self.config["bucket"] + "/" + key
        self._key(uri)
        return uri

    def put(self, uri, source):
        key = self._key(uri)
        with Path(source).open("rb") as stream:
            result = self.bucket.put_object(key, stream)
        return {"status": result.status, "etag": result.etag}

    def get(self, uri):
        result = self.bucket.get_object(self._key(uri))
        try:
            return result.read()
        finally:
            result.close()

    def download(self, uri, output):
        key = self._key(uri)
        destination = Path(output)
        result = self.bucket.get_object(key)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
                temporary = Path(stream.name)
                while True:
                    chunk = result.read(1024 * 1024)
                    if not chunk:
                        break
                    stream.write(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
            temporary = None
            return {"status": result.status, "etag": result.etag, "output": str(destination)}
        finally:
            result.close()
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def stat(self, uri):
        try:
            result = self.bucket.head_object(self._key(uri))
            return {"exists": True, "status": result.status,
                    "etag": result.etag, "size": result.content_length}
        except Exception as error:
            if getattr(error, "code", None) != "NoSuchKey":
                raise
            return {"exists": False, "status": 404}

    def list(self, uri):
        prefix = self._directory(uri)
        token = ""
        objects = []
        while True:
            result = self.bucket.list_objects_v2(prefix=prefix, continuation_token=token)
            for value in result.object_list:
                if not value.key.startswith(prefix):
                    raise OpsError("UnexpectedObjectListing")
                objects.append({"uri": self._uri(value.key), "size": value.size,
                                "etag": value.etag, "last_modified": value.last_modified})
            if not result.is_truncated:
                return {"objects": objects}
            if not result.next_continuation_token or token == result.next_continuation_token:
                raise OpsError("InvalidObjectContinuation")
            token = result.next_continuation_token

    def delete(self, uri):
        result = self.bucket.delete_object(self._key(uri))
        return {"status": result.status}

    def list_multipart(self, uri):
        prefix = self._directory(uri)
        key_marker = upload_marker = ""
        uploads = []
        while True:
            result = self.bucket.list_multipart_uploads(
                prefix=prefix, key_marker=key_marker, upload_id_marker=upload_marker)
            for value in result.upload_list:
                if not value.key.startswith(prefix):
                    raise OpsError("UnexpectedMultipartListing")
                uploads.append({"uri": self._uri(value.key), "upload_id": value.upload_id,
                                "initiated": value.initiation_date})
            if not result.is_truncated:
                return {"uploads": uploads}
            next_marker = (result.next_key_marker, result.next_upload_id_marker)
            if not next_marker[0] or next_marker == (key_marker, upload_marker):
                raise OpsError("InvalidMultipartContinuation")
            key_marker, upload_marker = next_marker

    def abort_multipart(self, uri, upload_id):
        if not isinstance(upload_id, str) or not upload_id:
            raise OpsError("InvalidUploadIdentity")
        result = self.bucket.abort_multipart_upload(self._key(uri), upload_id)
        return {"status": result.status}

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self._close()
            finally:
                if self._credentials is not None:
                    self._credentials.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def error_result(error):
    code = getattr(error, "code", None)
    if not isinstance(code, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]+", code):
        code = type(error).__name__
    status = getattr(error, "status", None)
    return {"ok": False, "error": code,
            "status": status if isinstance(status, int) else None}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--timeout-seconds", type=float)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ["put", "get", "stat", "list", "delete", "list-multipart", "abort-multipart"]:
        command = commands.add_parser(name)
        command.add_argument("--uri", required=True)
        if name == "put":
            command.add_argument("--input", required=True)
        elif name == "get":
            command.add_argument("--output", required=True)
        elif name == "abort-multipart":
            command.add_argument("--upload-id", required=True)
    args = parser.parse_args(argv)
    try:
        if args.timeout_seconds is not None and (not math.isfinite(args.timeout_seconds)
                                                 or args.timeout_seconds <= 0):
            raise OpsError("InvalidTimeout")
        config = json.loads(Path(args.config).read_text())
        with OssOperations(config, timeout=args.timeout_seconds) as ops:
            methods = {"put": lambda: ops.put(args.uri, args.input),
                       "get": lambda: ops.download(args.uri, args.output),
                       "stat": lambda: ops.stat(args.uri), "list": lambda: ops.list(args.uri),
                       "delete": lambda: ops.delete(args.uri),
                       "list-multipart": lambda: ops.list_multipart(args.uri),
                       "abort-multipart": lambda: ops.abort_multipart(args.uri, args.upload_id)}
            result = methods[args.command]()
        print(json.dumps({"ok": True, **result}, sort_keys=True))
        return 0
    except Exception as error:
        print(json.dumps(error_result(error), sort_keys=True))
        return 1


if __name__ == "__main__":
    sys.exit(main())
