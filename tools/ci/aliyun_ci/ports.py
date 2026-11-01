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

"""Carrier ports; implementations preserve actual API failures and responses."""
from typing import Protocol

MANIFEST_SCHEMA = 1
SUMMARY_SCHEMA = 1


class CloudAPI(Protocol):
    def call(self, action: str, **parameters) -> dict: ...


class ECSPort(CloudAPI, Protocol): pass
class OOSPort(CloudAPI, Protocol): pass
class ROSPort(CloudAPI, Protocol): pass


class OSSPort(Protocol):
    def put(self, key: str, data: bytes, *, write_once: bool = False) -> None: ...
    def get(self, key: str) -> bytes: ...
    def list(self, prefix: str) -> list[str]: ...
    def presign(self, method: str, key: str, expires_at: str) -> str: ...


class GitHubPort(Protocol):
    def call(self, method: str, path: str, body: dict | None = None): ...


class APIError(RuntimeError):
    def __init__(self, code: str, message: str = ''):
        self.code = code
        super().__init__(f'{code}: {message}')
