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

"""Official release checksum is the independent oracle; no live downloads."""
import hashlib
import json
import unittest
from aliyun_ci.runner_sync import sync_runner, current_runner
from aliyun_ci.config import validate_config
from .test_config import example


class MemoryObjects:
    def __init__(self): self.objects, self.writes = {}, []
    def put(self, key, data, *, write_once=False):
        if write_once and key in self.objects and self.objects[key] != data:
            raise ValueError('immutable object differs')
        self.objects[key] = data
        self.writes.append((key, write_once))
    def get(self, key): return self.objects[key]


class Release:
    def __init__(self, content):
        checksum = hashlib.sha256(content).hexdigest()
        self.record = {'tag_name': 'v2.300.1', 'body': 'actions-runner-linux-x64-2.300.1.tar.gz '
                       f'<!-- BEGIN SHA linux-x64 -->{checksum}<!-- END SHA linux-x64 -->',
                       'assets': [{'name': 'actions-runner-linux-x64-2.300.1.tar.gz',
                       'browser_download_url': 'https://github.com/actions/runner/releases/download/v2.300.1/actions-runner-linux-x64-2.300.1.tar.gz',
                       'digest': f'sha256:{checksum}'}]}
        self.calls = []
    def call(self, *args):
        self.calls.append(args)
        return self.record


class RunnerSyncTests(unittest.TestCase):
    def test_verified_version_is_immutable_and_latest_is_published_last(self):
        content = b'official runner bytes'
        github, oss, config = Release(content), MemoryObjects(), validate_config(example())
        record = sync_runner(config, github, oss, fetch=lambda url: content)
        self.assertEqual(content, oss.get(record['key']))
        self.assertTrue(oss.writes[0][1])
        self.assertTrue(oss.writes[1][1])
        self.assertEqual(('novarocks-ci/runners/latest.json', False), oss.writes[-1])
        self.assertEqual(record, current_runner(config, oss))
        sync_runner(config, github, oss, fetch=lambda url: content)

    def test_mismatch_never_uploads_or_moves_latest(self):
        github, oss = Release(b'official'), MemoryObjects()
        with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
            sync_runner(validate_config(example()), github, oss, fetch=lambda url: b'bad')
        self.assertEqual([], oss.writes)

    def test_asset_disagreement_and_unpublished_checksum_are_refused(self):
        for case in ('digest', 'body'):
            github = Release(b'official')
            if case == 'digest': github.record['assets'][0]['digest'] = 'sha256:' + '0' * 64
            else: github.record['body'] = ''
            with self.subTest(case=case), self.assertRaises(ValueError):
                sync_runner(validate_config(example()), github, MemoryObjects(), fetch=lambda url: b'official')
