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

"""Stage the source-pinned nextest archive for a disposable image builder.

This transports an already declared tool through OSS when the builder cannot
reach the public release. The native Linux environment still owns its cache,
archive verification and executable installation; staging never selects a
version or replaces that owner.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import tarfile
import urllib.request
from urllib.parse import urlsplit

from .ports import APIError


_INPUT = 'tools/dev/linux-env.inputs.json'
_ABSENT = {'NoSuchKey', 'NotFound', '404'}


def _nextest_input(source_archive):
    with tarfile.open(fileobj=io.BytesIO(source_archive), mode='r:*') as archive:
        members = [member for member in archive.getmembers() if member.name == _INPUT]
        if len(members) != 1 or not members[0].isfile():
            raise ValueError('image source archive requires one regular Linux environment input')
        with archive.extractfile(members[0]) as source:
            value = json.load(source)
    if value.get('schema') != 1:
        raise ValueError('unsupported Linux environment input schema')
    item = value.get('nextest')
    if not isinstance(item, dict):
        raise ValueError('image source has no pinned nextest input')
    version, digest, url = (item.get(key) for key in ('version', 'sha256', 'url'))
    if not isinstance(version, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', version):
        raise ValueError('nextest version is not a valid cache component')
    if not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest):
        raise ValueError('nextest archive SHA-256 is required')
    if not isinstance(url, str) or urlsplit(url).scheme != 'https' or not urlsplit(url).netloc:
        raise ValueError('pinned nextest archive URL must use HTTPS')
    return version, digest, url


def stage_nextest(config, oss, source_archive: bytes, deadline, fetch=urllib.request.urlopen):
    """Verify exact pinned bytes before immutable PUT and return one read capability.

    Missing objects may be obtained from the source-declared public URL. Unknown
    OSS outcomes and unreadable objects retain their actual failure; neither
    authorizes a replacement fetch or an overwrite. Call before ECS allocation
    so a temporary credential with insufficient validity cannot strand a node.
    """
    version, digest, source_url = _nextest_input(source_archive)
    key = f"{config['oss']['prefix']}/tools/nextest/{version}/{digest}.tar.gz"
    try:
        data = oss.get(key)
    except APIError as error:
        if error.code not in _ABSENT:
            raise
        data = None
    if data is None:
        with fetch(source_url, timeout=60) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError('public nextest archive SHA-256 differs from source input')
        # Validate the credential lifetime before any persistent staging write.
        url = oss.presign('GET', key, deadline)
        oss.put(key, data, write_once=True)
    else:
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError('existing staged nextest archive SHA-256 differs from source input')
        url = oss.presign('GET', key, deadline)
    return {'url': url, 'sha256': digest,
            'cache_name': f'nextest-{version}-{digest}', 'key': key}
