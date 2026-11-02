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

"""Lazy official SDK adapters. All callers use the same standard credential chain."""
from __future__ import annotations
from importlib import import_module
from datetime import datetime, timezone
import hmac
from pathlib import Path
import json
import os
from .ports import APIError
from .plan import parse_time


def rpc_parameters(parameters):
    """Alibaba RPC repeated structures use the documented one-based dotted keys."""
    result = {}
    def visit(key, value):
        if isinstance(value, dict):
            for child, item in value.items():
                visit(f'{key}.{child}', item)
        elif isinstance(value, list):
            for index, item in enumerate(value, 1):
                visit(f'{key}.{index}', item)
        elif value is not None:
            result[key] = str(value).lower() if isinstance(value, bool) else str(value)
    for key, value in parameters.items():
        visit(key, value)
    return result


class RPC:
    SERVICES = {
        'ecs': ('alibabacloud_ecs20140526', '2014-05-26', 'ecs'),
        'oos': ('alibabacloud_oos20190601', '2019-06-01', 'oos'),
        'ros': ('alibabacloud_ros20190910', '2019-09-10', 'ros'),
        'sts': ('alibabacloud_sts20150401', '2015-04-01', 'sts'),
    }

    def __init__(self, service, region, *, credential=None):
        package, self.version, _ = self.SERVICES[service]
        self.service, self.region = service, region
        self.models = import_module('alibabacloud_tea_openapi.models')
        self.runtime_models = import_module('alibabacloud_tea_util.models')
        credential = credential or import_module('alibabacloud_credentials.client').Client()
        # Endpoint mappings belong to the pinned official service client.
        config = self.models.Config(credential=credential, region_id=region)
        self.client = import_module(package + '.client').Client(config)

    def call(self, action, **parameters):
        # SDK transport retry is disabled: mutations recover with their original token.
        try:
            request = self.models.OpenApiRequest(query=rpc_parameters(parameters))
            params = self.models.Params(action=action, version=self.version, protocol='HTTPS',
                pathname='/', method='POST', auth_type='AK', style='RPC', req_body_type='formData', body_type='json')
            response = self.client.call_api(params, request, self.runtime_models.RuntimeOptions(autoretry=False))
            return response['body']
        except Exception as error:
            code = getattr(error, 'code', None)
            if code:
                raise APIError(code, f'{self.service}.{action} failed') from None
            if isinstance(error, (TimeoutError, ConnectionError)):
                raise
            raise APIError('TransportUnknown', type(error).__name__) from None


def check_account(config, sts):
    identity = sts.call('GetCallerIdentity')
    if str(identity.get('AccountId')) != str(config['account_id']):
        raise ValueError('credential account does not match deployment account_id')
    return identity


def signing_credential_expiration(current, *, profile_file=None, environ=None):
    """Read expiry facts for the exact signing identity, never infer a duration.

    The pinned Credentials Client and CLI provider discard the provider's public
    expiration. OAuth persists its actual exchanged STS expiry in the official
    CLI cache. Consume that fact only when all cached credential fields match
    the fetched signing snapshot; another process may have refreshed the cache.
    """
    environ = os.environ if environ is None else environ
    bounds = []
    explicit = environ.get('ALIYUN_CI_STS_NOT_AFTER')
    if explicit:
        bounds.append(parse_time(explicit))
    provider = getattr(current, 'provider_name', '') or ''
    if provider.split('/')[-2:] == ['cli_profile', 'oauth']:
        path = Path(profile_file) if profile_file is not None else Path.home() / '.aliyun/config.json'
        try:
            cache = json.loads(path.read_text())
        except (OSError, ValueError):
            raise ValueError('cannot read the OAuth STS expiration fact') from None
        selected = environ.get('ALIBABA_CLOUD_PROFILE') or cache.get('current')
        rows = [row for row in cache.get('profiles', []) if row.get('name') == selected]
        if len(rows) != 1 or rows[0].get('mode') != 'OAuth':
            raise ValueError('OAuth signing profile does not match the active CLI profile')
        profile = rows[0]
        pairs = (('access_key_id', 'access_key_id'), ('access_key_secret', 'access_key_secret'),
                 ('sts_token', 'security_token'))
        if any(not isinstance(profile.get(key), str)
               or not isinstance(getattr(current, field, None), str)
               or not hmac.compare_digest(profile[key], getattr(current, field))
               for key, field in pairs):
            raise ValueError('OAuth STS cache changed; signing identity must be resolved again')
        expiration = profile.get('sts_expiration')
        if type(expiration) is not int or expiration <= 0:
            raise ValueError('OAuth STS expiration is unknown')
        bounds.append(datetime.fromtimestamp(expiration, timezone.utc))
    if not bounds:
        raise ValueError('temporary signing credentials require a known STS expiration')
    return min(bounds)


def oss_error_code(error, sdk):
    """Read the service verdict through the official operation wrapper."""
    exceptions = getattr(sdk, 'exceptions', None)
    if exceptions is not None and isinstance(error, exceptions.OperationError):
        error = error.unwrap()
    return getattr(error, 'code', None) or type(error).__name__


class ObjectStore:
    def __init__(self, config, *, credential=None):
        self.sdk = import_module('alibabacloud_oss_v2')
        credential = credential or import_module('alibabacloud_credentials.client').Client()
        self.credential = credential
        def get_credentials():
            value = credential.get_credential()
            return self.sdk.credentials.Credentials(value.access_key_id, value.access_key_secret,
                                                    value.security_token)
        settings = self.sdk.config.load_default()
        self.region = settings.region = config['region']
        settings.credentials_provider = self.sdk.credentials.CredentialsProviderFunc(get_credentials)
        self.client, self.bucket = self.sdk.Client(settings), config['oss']['bucket']

    def put(self, key, data, *, write_once=False):
        try:
            self.client.put_object(self.sdk.PutObjectRequest(bucket=self.bucket, key=key, body=data,
                                                            forbid_overwrite=write_once))
        except Exception as error:
            code = oss_error_code(error, self.sdk)
            if write_once and code in ('FileAlreadyExists', 'ObjectAlreadyExists'):
                if self.get(key) == data:
                    return
                raise APIError('ImmutableObjectConflict', f'object already contains different bytes: {key}') from error
            raise APIError(code, 'OSS PUT failed') from error

    def get(self, key):
        try:
            result = self.client.get_object(self.sdk.GetObjectRequest(bucket=self.bucket, key=key))
            try:
                return result.body.read()
            finally:
                result.body.close()
        except Exception as error:
            raise APIError(oss_error_code(error, self.sdk), 'OSS GET failed') from error

    def list(self, prefix):
        keys, token = [], None
        while True:
            value = self.client.list_objects_v2(self.sdk.ListObjectsV2Request(
                bucket=self.bucket, prefix=prefix, continuation_token=token))
            keys.extend(item.key for item in (value.contents or []))
            if not value.is_truncated:
                return keys
            token = value.next_continuation_token
            if not token:
                raise ValueError('truncated OSS list has no continuation token')

    def presign(self, method, key, expires_at):
        expiration = parse_time(expires_at)
        current = self.credential.get_credential()
        if current.security_token:
            if expiration > signing_credential_expiration(current):
                raise ValueError('object capability outlives the known STS credential expiry')
        request_type = {'PUT': self.sdk.PutObjectRequest, 'GET': self.sdk.GetObjectRequest}.get(method)
        if request_type is None:
            raise ValueError('only GET and PUT object capabilities are supported')
        # Freeze the checked identity for this signature. A refreshing provider
        # could otherwise replace it between expiry validation and signing.
        snapshot = self.sdk.credentials.Credentials(current.access_key_id, current.access_key_secret,
                                                     current.security_token)
        settings = self.sdk.config.load_default()
        settings.region = self.region
        settings.credentials_provider = self.sdk.credentials.CredentialsProviderFunc(lambda: snapshot)
        result = self.sdk.Client(settings).presign(request_type(bucket=self.bucket, key=key), expiration=expiration)
        if result.signed_headers:
            raise ValueError('bootstrap upload capability unexpectedly requires signed headers')
        return result.url


def adapters(config):
    credential = import_module('alibabacloud_credentials.client').Client()
    sts = RPC('sts', config['region'], credential=credential)
    check_account(config, sts)
    return {name: RPC(name, config['region'], credential=credential) for name in ('ecs', 'oos', 'ros')} | {
        'oss': ObjectStore(config, credential=credential)}
