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

"""Pinned nextest staging uses real HTTP bytes and immutable object semantics.

CI maintainers own these low-cost transport tests. The source manifest and
independent SHA-256 oracle determine the one permitted archive; native G19
installation remains outside this owner and is not simulated as successful.
"""
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import tarfile
import threading
import unittest
import urllib.request

from aliyun_ci.image_tools import stage_nextest
from aliyun_ci.ports import APIError


def archive_member(name, data):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w') as archive:
        member = tarfile.TarInfo(name)
        member.size = len(data)
        archive.addfile(member, io.BytesIO(data))
    return raw.getvalue()


class Objects:
    def __init__(self):
        self.values, self.writes, self.capabilities = {}, [], []
        self.failure = None
        self.expiry_failure = False

    def get(self, key):
        if self.failure:
            raise APIError(self.failure)
        if key not in self.values:
            raise APIError('NoSuchKey')
        return self.values[key]

    def put(self, key, data, *, write_once=False):
        self.writes.append((key, data, write_once))
        if write_once and key in self.values and self.values[key] != data:
            raise APIError('ImmutableObjectConflict')
        self.values[key] = data

    def presign(self, method, key, deadline):
        if self.expiry_failure:
            raise ValueError('object capability outlives the known STS credential expiry')
        self.capabilities.append((method, key, deadline))
        return 'https://object.invalid/read?signature=limited-capability'


class ImageToolsTests(unittest.TestCase):
    def setUp(self):
        self.data = archive_member('cargo-nextest', b'pinned executable fixture')
        self.digest = hashlib.sha256(self.data).hexdigest()
        self.item = {'version': '0.9.148', 'sha256': self.digest,
                     'url': 'https://public.invalid/nextest.tar.gz'}
        self.config = {'oss': {'prefix': 'novarocks-ci'}}
        self.key = f'novarocks-ci/tools/nextest/0.9.148/{self.digest}.tar.gz'
        self.deadline = '2026-10-29T12:00:00Z'
        self.objects = Objects()

    def source(self):
        return archive_member('tools/dev/linux-env.inputs.json',
            json.dumps({'schema': 1, 'nextest': self.item}).encode())

    def http_fetch(self, content):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(inner):
                inner.send_response(200)
                inner.send_header('Content-Length', str(len(content)))
                inner.end_headers()
                inner.wfile.write(content)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def cleanup():
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        self.addCleanup(cleanup)
        calls = []
        def fetch(url, *, timeout):
            calls.append((url, timeout))
            # Production still receives the exact HTTPS input. Only this
            # injected opener routes its known public fixture to local HTTP.
            return urllib.request.urlopen(f'http://127.0.0.1:{server.server_port}/nextest', timeout=timeout)
        return fetch, calls

    def test_public_archive_is_hash_verified_then_written_immutably(self):
        fetch, calls = self.http_fetch(self.data)
        result = stage_nextest(self.config, self.objects, self.source(), self.deadline, fetch=fetch)
        self.assertEqual([(self.item['url'], 60)], calls)
        self.assertEqual([(self.key, self.data, True)], self.objects.writes)
        self.assertEqual([('GET', self.key, self.deadline)], self.objects.capabilities)
        self.assertEqual(self.key, result['key'])
        self.assertEqual(self.digest, result['sha256'])
        self.assertEqual(f'nextest-0.9.148-{self.digest}', result['cache_name'])
        self.assertEqual('https://object.invalid/read?signature=limited-capability', result['url'])

    def test_wrong_public_digest_never_writes_an_object(self):
        fetch, calls = self.http_fetch(b'different bytes')
        with self.assertRaisesRegex(ValueError, 'public nextest archive SHA-256'):
            stage_nextest(self.config, self.objects, self.source(), self.deadline, fetch=fetch)
        self.assertEqual(1, len(calls))
        self.assertEqual([], self.objects.writes)
        self.assertEqual([], self.objects.capabilities)

    def test_verified_existing_object_skips_public_fetch(self):
        self.objects.values[self.key] = self.data
        def no_fetch(*args, **kwargs):
            self.fail('verified existing object must not be fetched again')
        result = stage_nextest(self.config, self.objects, self.source(), self.deadline, fetch=no_fetch)
        self.assertEqual(self.digest, result['sha256'])
        self.assertEqual([], self.objects.writes)
        self.assertEqual([('GET', self.key, self.deadline)], self.objects.capabilities)

    def test_bad_existing_object_is_not_replaced_or_refetched(self):
        self.objects.values[self.key] = b'incorrect existing bytes'
        def no_fetch(*args, **kwargs):
            self.fail('existing object hash mismatch must not trigger public fetch')
        with self.assertRaisesRegex(ValueError, 'existing staged nextest archive SHA-256'):
            stage_nextest(self.config, self.objects, self.source(), self.deadline, fetch=no_fetch)
        self.assertEqual([], self.objects.writes)
        self.assertEqual([], self.objects.capabilities)

    def test_unknown_or_denied_object_read_does_not_become_absent(self):
        for failure in ('AccessDenied', 'TransportUnknown', 'RequestTimeout'):
            with self.subTest(failure=failure):
                self.objects.failure = failure
                def no_fetch(*args, **kwargs):
                    self.fail('unknown or denied object must not be treated as absent')
                with self.assertRaises(APIError) as error:
                    stage_nextest(self.config, self.objects, self.source(), self.deadline, fetch=no_fetch)
                self.assertEqual(failure, error.exception.code)
                self.assertEqual([], self.objects.writes)

    def test_explicit_404_is_absent_and_uses_exact_public_input(self):
        self.objects.failure = '404'
        fetch, calls = self.http_fetch(self.data)
        result = stage_nextest(self.config, self.objects, self.source(), self.deadline, fetch=fetch)
        self.assertEqual(self.key, result['key'])
        self.assertEqual([(self.item['url'], 60)], calls)
        self.assertTrue(self.objects.writes[0][2])

    def test_plain_http_input_is_rejected_before_object_side_effects(self):
        self.item['url'] = 'http://public.invalid/nextest.tar.gz'
        with self.assertRaisesRegex(ValueError, 'HTTPS'):
            stage_nextest(self.config, self.objects, self.source(), self.deadline)
        self.assertEqual([], self.objects.writes)
        self.assertEqual([], self.objects.capabilities)

    def test_credential_expiration_refuses_staging_write(self):
        self.objects.expiry_failure = True
        fetch, _ = self.http_fetch(self.data)
        with self.assertRaisesRegex(ValueError, 'credential expiry'):
            stage_nextest(self.config, self.objects, self.source(), self.deadline, fetch=fetch)
        self.assertEqual([], self.objects.writes)
