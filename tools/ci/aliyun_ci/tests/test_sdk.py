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

"""CI owner tests for exact temporary signing capability lifetimes."""
from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from aliyun_ci.sdk import signing_credential_expiration, ObjectStore


class SigningExpiryTests(unittest.TestCase):
    def credential(self, **changes):
        data = dict(access_key_id='test-id', access_key_secret='test-secret',
                    security_token='test-token', provider_name='default/cli_profile/oauth')
        data.update(changes)
        return SimpleNamespace(**data)

    def cache(self, root, **changes):
        profile = dict(name='local', mode='OAuth', access_key_id='test-id',
                       access_key_secret='test-secret', sts_token='test-token', sts_expiration=1893456000)
        profile.update(changes)
        path = Path(root) / 'profile.json'
        path.write_text(json.dumps(dict(current='local', profiles=[profile])))
        return path

    def test_oauth_expiry_is_actual_cache_fact_for_same_identity(self):
        with TemporaryDirectory() as root:
            expiry = signing_credential_expiration(self.credential(), profile_file=self.cache(root), environ={})
            self.assertEqual(datetime(2030, 1, 1, tzinfo=timezone.utc), expiry)

    def test_cache_refresh_does_not_authorize_another_signing_identity(self):
        with TemporaryDirectory() as root:
            for changes in ({'access_key_id': 'new-id'}, {'sts_token': 'new-token'},
                            {'access_key_secret': 'new-secret'}):
                with self.subTest(changes=tuple(changes)), self.assertRaisesRegex(ValueError, 'cache changed'):
                    signing_credential_expiration(self.credential(), profile_file=self.cache(root, **changes), environ={})

    def test_unknown_expiry_and_provider_refuse_without_explicit_fact(self):
        with TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError, 'unknown'):
                signing_credential_expiration(self.credential(), profile_file=self.cache(root, sts_expiration=None), environ={})
        with self.assertRaisesRegex(ValueError, 'known STS expiration'):
            signing_credential_expiration(self.credential(provider_name='environment'), environ={})
        expiry = signing_credential_expiration(self.credential(provider_name='environment'),
            environ={'ALIYUN_CI_STS_NOT_AFTER': '2029-01-01T00:00:00Z'})
        self.assertEqual(2029, expiry.year)

    def test_explicit_bound_cannot_extend_actual_oauth_expiry(self):
        with TemporaryDirectory() as root:
            expiry = signing_credential_expiration(self.credential(), profile_file=self.cache(root),
                environ={'ALIYUN_CI_STS_NOT_AFTER': '2031-01-01T00:00:00Z'})
            self.assertEqual(2030, expiry.year)

    def test_capability_outliving_credentials_is_rejected_before_sdk_signing(self):
        store = ObjectStore.__new__(ObjectStore)
        store.credential = SimpleNamespace(get_credential=self.credential)
        with patch('aliyun_ci.sdk.signing_credential_expiration',
                   return_value=datetime(2030, 1, 1, tzinfo=timezone.utc)):
            with self.assertRaisesRegex(ValueError, 'outlives'):
                store.presign('GET', 'key', '2030-01-02T00:00:00Z')

    def test_signature_uses_checked_snapshot_even_if_provider_refreshes(self):
        original = self.credential(provider_name='environment')
        newer = self.credential(access_key_id='new-id', security_token='new-token')
        readings = iter([original, newer])
        store = ObjectStore.__new__(ObjectStore)
        store.credential = SimpleNamespace(get_credential=lambda: next(readings))
        store.region, store.bucket = 'cn-beijing', 'bucket'
        observed = []
        class Signer:
            def __init__(self, settings): self.settings = settings
            def presign(self, request, expiration):
                observed.append(self.settings.credentials_provider())
                return SimpleNamespace(signed_headers={}, url='https://example.invalid/fixed')
        store.sdk = SimpleNamespace(credentials=SimpleNamespace(
            Credentials=lambda *fields: fields, CredentialsProviderFunc=lambda fn: fn),
            config=SimpleNamespace(load_default=SimpleNamespace), Client=Signer,
            GetObjectRequest=lambda **kw: kw, PutObjectRequest=lambda **kw: kw)
        with patch.dict('os.environ', {'ALIYUN_CI_STS_NOT_AFTER': '2030-01-01T00:00:00Z'}, clear=True):
            self.assertEqual('https://example.invalid/fixed', store.presign('GET', 'key', '2029-01-01T00:00:00Z'))
        self.assertEqual([('test-id', 'test-secret', 'test-token')], observed)
        self.assertIs(newer, next(readings))


class ObjectFailureTests(unittest.TestCase):
    def test_official_operation_wrapper_preserves_no_such_key_and_denied(self):
        from aliyun_ci.ports import APIError
        class OperationError(Exception):
            def __init__(self,error):self.error=error
            def unwrap(self):return self.error
        class ServiceError(Exception):
            def __init__(self,code):self.code=code
        for code in ('NoSuchKey','AccessDenied'):
            def get_object(*args):raise OperationError(ServiceError(code))
            store=ObjectStore.__new__(ObjectStore);store.bucket='bucket'
            store.sdk=SimpleNamespace(exceptions=SimpleNamespace(OperationError=OperationError),GetObjectRequest=lambda **kw:kw)
            store.client=SimpleNamespace(get_object=get_object)
            with self.subTest(code=code),self.assertRaises(APIError) as raised:
                store.get('object')
            self.assertEqual(code,raised.exception.code)
