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

"""Copy the official Runner release into immutable versioned OSS objects."""
from __future__ import annotations
import hashlib
import json
import re
import urllib.request


def download(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as response:
        return response.read()


def sync_runner(config, github, oss, *, fetch=download) -> dict:
    release = github.call('GET', '/repos/actions/runner/releases/latest')
    tag = release['tag_name']
    if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', tag):
        raise ValueError('Runner release has no supported version')
    version = tag[1:]
    name = f'actions-runner-linux-x64-{version}.tar.gz'
    assets = [item for item in release['assets'] if item['name'] == name]
    if len(assets) != 1:
        raise ValueError('Runner release must contain one linux-x64 archive')
    checksum = re.search(re.escape(name) + r'\s+<!-- BEGIN SHA linux-x64 -->([0-9a-f]{64})<!-- END SHA linux-x64 -->', release['body'])
    if checksum is None:
        raise ValueError('Runner release is missing the published linux-x64 checksum')
    expected = checksum.group(1)
    if assets[0].get('digest') not in (None, f'sha256:{expected}'):
        raise ValueError('Runner asset and release checksums disagree')
    url = assets[0]['browser_download_url']
    if not url.startswith(f'https://github.com/actions/runner/releases/download/{tag}/'):
        raise ValueError('Runner download must come from its official release')
    content = fetch(url)
    if hashlib.sha256(content).hexdigest() != expected:
        raise ValueError('Runner archive checksum mismatch')
    prefix = f"{config['oss']['prefix']}/runners/{version}"
    record = {'schema': 1, 'version': version, 'sha256': expected,
              'key': f'{prefix}/{name}', 'source_url': url}
    # Version objects are immutable; a failed copy never updates latest.
    oss.put(record['key'], content, write_once=True)
    oss.put(f'{prefix}/release.json', json.dumps(record, sort_keys=True).encode(), write_once=True)
    oss.put(f"{config['oss']['prefix']}/runners/latest.json", json.dumps(record, sort_keys=True).encode())
    return record


def current_runner(config, oss) -> dict:
    record = json.loads(oss.get(f"{config['oss']['prefix']}/runners/latest.json"))
    if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', record.get('version', '')):
        raise ValueError('invalid synchronized Runner version')
    expected_key = f"{config['oss']['prefix']}/runners/{record['version']}/actions-runner-linux-x64-{record['version']}.tar.gz"
    if record.get('key') != expected_key or not re.fullmatch(r'[0-9a-f]{64}', record.get('sha256', '')):
        raise ValueError('invalid synchronized Runner record')
    immutable = json.loads(oss.get(f"{config['oss']['prefix']}/runners/{record['version']}/release.json"))
    if immutable != record:
        raise ValueError('Runner latest pointer differs from immutable release')
    return record
