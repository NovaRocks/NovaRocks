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

"""Replay lifecycle requests against independently scripted service facts; no cloud cost."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import json
import unittest
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from aliyun_ci.ports import APIError
from aliyun_ci.start import create_once, start_run, publish_bootstrap
from .test_config import example

SHAPES = json.loads((Path(__file__).parent/'fixtures/api/response-shapes.json').read_text())
EMPTY = SHAPES['empty_instances']
CREATED = SHAPES['created']
NOW = datetime(2026,10,28, tzinfo=timezone.utc)


class ScriptAPI:
    def __init__(self, script):
        self.script, self.calls = list(script), []
    def call(self, action, **parameters):
        self.calls.append((action, parameters))
        if not self.script:
            raise AssertionError(f'unexpected API call: {action}')
        expected, response = self.script.pop(0)
        if action != expected:
            raise AssertionError(f'expected {expected}, got {action}')
        if isinstance(response, BaseException):
            raise response
        return response


def instance(config, plan, **updates):
    result = {'InstanceId':'i-exact','ImageId':'m-exact','RegionId':config['region'],
              'Status':'Running','AutoReleaseTime':plan.auto_release,
              'Tags':{'Tag':[{'TagKey':k,'TagValue':v} for k,v in plan.tags.items()]}}
    return dict(result, **updates)


def listed(value):
    return {'TotalCount':1,'Instances':{'Instance':[value]}}


class StartTests(unittest.TestCase):
    def setUp(self):
        data = example()
        data['candidates'] *= 2
        self.config = validate_config(data)
        self.plan = derive(self.config, '42', 1, NOW, nonce='a'*12)
    def test_no_stock_advances_candidate_and_creates_spot_once(self):
        api = ScriptAPI([('DescribeInstances',EMPTY),('RunInstances',APIError('OperationDenied.NoStock')),('RunInstances',CREATED)])
        self.assertEqual('i-exact', create_once(self.config,self.plan,api,'m-exact',now=lambda:NOW))
        requests = [request for action,request in api.calls if action=='RunInstances']
        self.assertEqual([self.plan.client_token+'-c0', self.plan.client_token+'-c1'],[r['ClientToken'] for r in requests])
        self.assertEqual('PostPaid',requests[0]['InstanceChargeType'])
        self.assertEqual('SpotAsPriceGo',requests[0]['SpotStrategy'])
        self.assertEqual('Terminate',requests[0]['SpotInterruptionBehavior'])
        self.assertEqual(1,requests[0]['Amount'])
        self.assertEqual(self.plan.auto_release,requests[0]['AutoReleaseTime'])
        self.assertEqual(self.plan.tags,{r['Key']:r['Value'] for r in requests[0]['Tag']})
    def test_unknown_replays_exact_token_then_finds_by_tags(self):
        api = ScriptAPI([('DescribeInstances',EMPTY),('RunInstances',TimeoutError()),('RunInstances',TimeoutError()),('DescribeInstances',listed(instance(self.config,self.plan)))])
        self.assertEqual('i-exact',create_once(self.config,self.plan,api,'m-exact',now=lambda:NOW))
        self.assertEqual(api.calls[1],api.calls[2])
        self.assertEqual(4,len(api.calls))
    def test_existing_instance_never_creates_again(self):
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan)))])
        self.assertEqual('i-exact',create_once(self.config,self.plan,api,'m-exact',now=lambda:NOW))
        self.assertEqual(1,len(api.calls))
    def test_creation_auto_release_rejection_uses_modify(self):
        api=ScriptAPI([('DescribeInstances',EMPTY),('RunInstances',APIError('InvalidAutoReleaseTime.Malformed')),('RunInstances',CREATED),('ModifyInstanceAutoReleaseTime',{})])
        create_once(self.config,self.plan,api,'m-exact',now=lambda:NOW)
        self.assertEqual(api.calls[1][1]['ClientToken'],api.calls[2][1]['ClientToken'])
        self.assertNotIn('AutoReleaseTime',api.calls[2][1])
        self.assertEqual('i-exact',api.calls[3][1]['InstanceId'])
    def test_permission_error_does_not_try_another_candidate(self):
        api=ScriptAPI([('DescribeInstances',EMPTY),('RunInstances',APIError('Forbidden'))])
        with self.assertRaises(APIError):create_once(self.config,self.plan,api,'m-exact',now=lambda:NOW)
        self.assertEqual(2,len(api.calls))
    def test_bad_readback_is_deleted_before_any_jit_or_secret(self):
        bad=instance(self.config,self.plan,ImageId='m-wrong')
        api=ScriptAPI([('DescribeInstances',EMPTY),('RunInstances',CREATED),('DescribeInstances',listed(bad)),('DescribeInstances',listed(bad)),('DeleteInstance',{}),('DescribeInstances',EMPTY)])
        class GH:
            def __init__(self):self.calls=[]
            def call(self,method,path,body=None):
                self.calls.append((method,path,body))
                return {'type':'file'} if '/contents/' in path else {'sha':'a'*40}
        class OSS:
            def get(self,key):return json.dumps({'image_id':'m-exact','region':'cn-beijing'}).encode()
            def presign(self,method,key,expires_at):return 'https://example.test/upload'
        gh=GH();oos=ScriptAPI([])
        with self.assertRaisesRegex(ValueError,'image or auto release'):
            start_run(self.config,self.plan,api,oos,OSS(),gh,sha='a'*40,image_id='m-exact',runner={},now=lambda:NOW,sleep=lambda _:None)
        self.assertFalse(any(method=='POST' for method,_,_ in gh.calls))
        self.assertEqual([],oos.calls)
        self.assertEqual('i-exact',api.calls[-2][1]['InstanceId'])
    def test_secret_chunks_are_bounded_and_root_is_published_last(self):
        api=ScriptAPI([('CreateSecretParameter',{})]*6)
        payload={'jit':'x'*10000}
        publish_bootstrap(self.config,self.plan,api,payload)
        root=json.loads(api.calls[-1][1]['Value'])
        encoded=''.join(call[1]['Value'] for call in api.calls[:-1])
        self.assertEqual(payload,json.loads(base64.b64decode(encoded)))
        self.assertEqual(len(api.calls)-1,root['parts'])
        self.assertTrue(all(len(p['Value'])<=4096 for _,p in api.calls))
        self.assertEqual(self.plan.secret_name,api.calls[-1][1]['Name'])

    def test_local_premerge_boot_does_not_require_unpublished_profile_on_github(self):
        plan = derive(self.config, 'local-20261028T000000Z-abcd', 1, NOW)
        bad = instance(self.config, plan, ImageId='m-wrong')
        api = ScriptAPI([('DescribeInstances', EMPTY), ('RunInstances', CREATED),
            ('DescribeInstances', listed(bad)), ('DescribeInstances', listed(bad)),
            ('DeleteInstance', {}), ('DescribeInstances', EMPTY)])
        class NoRemoteSource:
            def call(self, *args):
                raise AssertionError('local pre-merge boot has no remote workload admission')
        class OSS:
            def get(self, key): return json.dumps({'image_id':'m-exact','region':'cn-beijing'}).encode()
            def presign(self, *args): return 'https://example.test/upload'
        with self.assertRaisesRegex(ValueError, 'image or auto release'):
            start_run(self.config, plan, api, ScriptAPI([]), OSS(), NoRemoteSource(),
                sha='a'*40, image_id='m-exact', runner={}, now=lambda:NOW, sleep=lambda _:None)
        self.assertEqual('i-exact', api.calls[-2][1]['InstanceId'])
