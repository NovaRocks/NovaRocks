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

"""Image API request replay protects actual cleanup and explicit promotion facts."""
import base64
from datetime import datetime, timezone
import json
import unittest
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from aliyun_ci.image import build_image, promote_image, image_key
from aliyun_ci.ports import APIError
from .test_config import example
from .test_runner_sync import MemoryObjects


class BuilderAPI:
    def __init__(self, plan, fail=None):
        self.plan, self.fail, self.calls = plan, fail, []
        self.exists, self.status, self.commands = False, 'Running', 0
    def call(self, action, **params):
        self.calls.append((action, params))
        if action == self.fail: raise APIError('FixtureFailure', action)
        if action == 'DescribeInstances':
            rows = [{'InstanceId': 'i-builder', 'RegionId': 'cn-beijing', 'ImageId': 'replace-with-ubuntu26-image-id',
                     'Status': self.status, 'AutoReleaseTime': self.plan.deadline,
                     'Tags': {'Tag': [{'TagKey': k, 'TagValue': v} for k, v in self.plan.tags.items()]}}] if self.exists else []
            return {'TotalCount': len(rows), 'Instances': {'Instance': rows}}
        if action == 'RunInstances':
            self.exists = True
            return {'InstanceIdSets': {'InstanceIdSet': ['i-builder']}}
        if action == 'RunCommand':
            self.commands += 1
            return {'InvokeId': str(self.commands)}
        if action == 'DescribeInvocationResults':
            output = json.dumps({'preparation_sha': 'a' * 40, 'os': 'ubuntu-26.04', 'arch': 'amd64',
                                 'tools_check': {'rust': 'observed'}}) if params['InvokeId'] == '2' else ('{"remaining_capabilities":0}' if params['InvokeId'] == '3' else '')
            return {'Invocation': {'InvocationResults': {'InvocationResult': [{'InstanceId': 'i-builder',
                'InvokeId': params['InvokeId'], 'InvocationStatus': 'Success', 'ExitCode': 0, 'Output': base64.b64encode(output.encode()).decode()}]}}}
        if action == 'StopInstance': self.status = 'Stopped'; return {}
        if action == 'CreateImage': return {'ImageId': 'm-candidate'}
        if action == 'DescribeImages': return {'Images': {'Image': [{'ImageId': 'm-candidate', 'Status': 'Available'}]}}
        if action == 'DeleteInstance': self.exists = False; return {}
        raise AssertionError((action, params))


class BuilderObjects(MemoryObjects):
    def get(self, key):
        if key.endswith('/build.log'): return b'original builder output\n'
        return super().get(key)
    def presign(self, *args): return 'https://public-source.invalid/archive.tar'


class ImageTests(unittest.TestCase):
    def setUp(self):
        self.config = validate_config(example())
        self.now = lambda: datetime(2026, 10, 28, tzinfo=timezone.utc)
        self.plan = derive(self.config, '42', 1, self.now(), role='builder')

    def test_builder_stops_captures_records_then_releases(self):
        api, objects = BuilderAPI(self.plan), BuilderObjects()
        result = build_image(self.config, self.plan, api, objects, preparation_sha='a'*40,
                             source_archive=b'public source', now=self.now, sleep=lambda _: None)
        self.assertEqual('m-candidate', result['image_id'])
        self.assertEqual('i-builder', result['builder_run']['instance_id'])
        self.assertTrue(result['build_log']['key'].endswith('/build.log'))
        names = [call[0] for call in api.calls]
        self.assertLess(names.index('StopInstance'), names.index('CreateImage'))
        self.assertLess(names.index('CreateImage'), names.index('DeleteInstance'))
        self.assertFalse(api.exists)
        self.assertNotIn(image_key(self.config, 'm-candidate', 'ready.json'), objects.objects)
        self.assertIn(image_key(self.config, 'm-candidate', 'release.json'), objects.objects)
        self.assertNotIn('local_rust_cargo_registries', result)
        self.assertNotIn('local_rust_swap_gib', result)

    def test_release_keeps_guest_capabilities_instead_of_inventing_operator_support(self):
        class GuestCratesOnly(BuilderAPI):
            def call(self, action, **parameters):
                value = super().call(action, **parameters)
                if action == 'DescribeInvocationResults' and parameters['InvokeId'] == '2':
                    row = value['Invocation']['InvocationResults']['InvocationResult'][0]
                    facts = json.loads(base64.b64decode(row['Output']))
                    facts['local_rust_cargo_registries'] = ['crates-io']
                    facts['local_rust_swap_gib'] = [0]
                    row['Output'] = base64.b64encode(json.dumps(facts).encode()).decode()
                return value
        result = build_image(self.config, self.plan, GuestCratesOnly(self.plan), BuilderObjects(),
            preparation_sha='a' * 40, source_archive=b'source', now=self.now, sleep=lambda _: None)
        self.assertEqual(['crates-io'], result['local_rust_cargo_registries'])
        self.assertEqual([0], result['local_rust_swap_gib'])

    def test_builder_is_deleted_on_prepare_stop_or_capture_failure(self):
        for failure in ('RunCommand', 'StopInstance', 'CreateImage'):
            api = BuilderAPI(self.plan, failure)
            with self.subTest(failure=failure), self.assertRaises(APIError):
                build_image(self.config, self.plan, api, BuilderObjects(), preparation_sha='a'*40,
                            source_archive=b'source', now=self.now, sleep=lambda _: None)
            self.assertFalse(api.exists)
            self.assertEqual(['i-builder'], [p['InstanceId'] for a,p in api.calls if a == 'DeleteInstance'])

    def test_promotion_requires_exact_image_and_observed_tools_pass(self):
        objects = MemoryObjects()
        objects.put(image_key(self.config, 'm-1', 'release.json'), b'{"image_id":"m-1"}')
        summary = {'image_id': 'm-1', 'tools_check': False, 'run_id': '42', 'run_attempt': 1}
        objects.put('summary.json', json.dumps(summary).encode())
        with self.assertRaisesRegex(ValueError, 'not passed'):
            promote_image(self.config, objects, 'm-1', 'summary.json')
        summary['tools_check'] = True
        objects.put('summary.json', json.dumps(summary).encode())
        result = promote_image(self.config, objects, 'm-1', 'summary.json')
        self.assertEqual('42', result['validation_run']['run_id'])
        self.assertTrue(objects.writes[-1][1])

    def test_image_scrub_removes_only_capability_scripts(self):
        from pathlib import Path
        import tempfile
        from aliyun_ci.image import scrub_command_history
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'source.sh').write_bytes(b'curl https://example.invalid?x-oss-signature=actual-signed-value')
            (root/'observe.sh').write_text('echo status')
            result=scrub_command_history(root)
            self.assertEqual(['source.sh'],result['removed_scripts'])
            self.assertEqual(0,result['remaining_capabilities'])
            self.assertFalse((root/'source.sh').exists())
            self.assertEqual('echo status',(root/'observe.sh').read_text())

    def test_pending_image_is_observed_with_all_statuses_without_recreation(self):
        expected=self
        class PendingImage(BuilderAPI):
            states=[None,'Creating','Available']
            def call(self,action,**params):
                if action=='DescribeImages':
                    self.calls.append((action,params));expected.assertEqual('ALL',params['Status'])
                    state=self.states.pop(0)
                    return {'Images':{'Image':[] if state is None else [{'ImageId':'m-candidate','Status':state}]}}
                return super().call(action,**params)
        api=PendingImage(self.plan);result=build_image(self.config,self.plan,api,BuilderObjects(),preparation_sha='a'*40,source_archive=b'source',now=self.now,sleep=lambda _:None)
        self.assertEqual('m-candidate',result['image_id'])
        self.assertEqual(1,sum(action=='CreateImage' for action,params in api.calls))
        self.assertFalse(api.exists)
