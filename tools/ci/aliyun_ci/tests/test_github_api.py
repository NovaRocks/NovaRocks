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

"""GitHub API request projection and trusted workflow restrictions."""
import unittest
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from aliyun_ci.github_api import generate_jit, runner_tags, validate_target
from .test_config import example
from .test_start import NOW


class GH:
    def __init__(self,config,restricted=True):self.config=config;self.calls=[];self.restricted=restricted
    def call(self,method,path,body=None):
        self.calls.append((method,path,body))
        if path.endswith('/generate-jitconfig'):return {'encoded_jit_config':'encoded-secret'}
        if '/runner-groups/' in path and '/repositories?' not in path:
            return {'restricted_to_workflows':self.restricted,'selected_workflows':['NovaRocks/NovaRocks/.github/workflows/aliyun-ci.yml@refs/heads/main'], 'allows_public_repositories':True,'visibility':'selected'}
        if '/repositories?' in path:return {'repositories':[{'full_name':self.config['repository']}]}
        if '/actions/runners?' in path:return {'runners':[]}
        if '/contents/' in path:return {'type':'file'}
        if '/commits/' in path:return {'sha':'a'*40}
        raise AssertionError(path)


class GitHubTests(unittest.TestCase):
    def test_jit_requires_trusted_workflow_and_carries_resource_labels(self):
        config=validate_config(example());plan=derive(config,'42',1,NOW)
        api=GH(config)
        self.assertEqual('encoded-secret',generate_jit(config,plan,api,instance_id='i-exact'))
        body=api.calls[-1][2]
        self.assertEqual(plan.label,body['name'])
        tags=runner_tags({'labels':[{'name':name} for name in body['labels']]})
        self.assertEqual(plan.deadline,tags['deadline'])
        self.assertEqual('i-exact',tags['instance_id'])
        self.assertEqual(config['deployment_id'],tags['deployment'])
        bad=GH(config,False)
        with self.assertRaises(ValueError):generate_jit(config,plan,bad,instance_id='i-exact')
        self.assertFalse(any(method=='POST' for method,_,_ in bad.calls))
    def test_extra_workflow_is_not_granted_runner_access(self):
        config=validate_config(example());plan=derive(config,'42',1,NOW)
        class ExtraWorkflow(GH):
            def call(self,method,path,body=None):
                value=super().call(method,path,body)
                if isinstance(value,dict) and 'selected_workflows' in value:
                    value['selected_workflows'].append('Other/Repo/.github/workflows/pr.yml@main')
                return value
        api=ExtraWorkflow(config)
        with self.assertRaisesRegex(ValueError,'trusted main workflow'):
            generate_jit(config,plan,api,instance_id='i-exact')
        self.assertFalse(any(method=='POST' for method,_,_ in api.calls))

    def test_target_sha_is_exact_and_profile_must_exist(self):
        config=validate_config(example());api=GH(config)
        validate_target(config,api,'a'*40)
        self.assertIn('?ref='+'a'*40,api.calls[-1][1])
        with self.assertRaises(ValueError):validate_target(config,api,'main')


class SDKProjectionTests(unittest.TestCase):
    def test_rpc_dotted_parameters_and_account_fact(self):
        from aliyun_ci.sdk import rpc_parameters, check_account
        from .test_start import ScriptAPI
        self.assertEqual({'Tag.1.Key':'a','Tag.1.Value':'b','SystemDisk.Size':'200','Force':'true'},
                         rpc_parameters({'Tag':[{'Key':'a','Value':'b'}],'SystemDisk':{'Size':200},'Force':True}))
        config=validate_config(example())
        good=ScriptAPI([('GetCallerIdentity',{'AccountId':str(config['account_id'])})])
        check_account(config,good)
        bad=ScriptAPI([('GetCallerIdentity',{'AccountId':'another-account'})])
        with self.assertRaisesRegex(ValueError,'account'):check_account(config,bad)

    def test_official_client_call_shape_without_importing_sdk(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from aliyun_ci.sdk import RPC
        calls=[]
        class Value:
            def __init__(self,**kwargs):self.__dict__.update(kwargs)
        class Client:
            def __init__(self,config):self.config=config
            def call_api(self,params,request,runtime):
                calls.append((params,request,runtime))
                return {'body':{'RequestId':'observed-request'}}
        def module(name):
            if name=='alibabacloud_tea_openapi.models':return SimpleNamespace(Config=Value,OpenApiRequest=Value,Params=Value)
            if name=='alibabacloud_tea_util.models':return SimpleNamespace(RuntimeOptions=Value)
            if name.endswith('.client'):return SimpleNamespace(Client=Client)
            raise AssertionError(name)
        with patch('aliyun_ci.sdk.import_module',side_effect=module):
            api=RPC('ecs','cn-beijing',credential=object())
            response=api.call('DeleteInstance',InstanceId='i-exact',Force=True)
        self.assertEqual('observed-request',response['RequestId'])
        self.assertEqual('DeleteInstance',calls[0][0].action)
        self.assertEqual('false',str(calls[0][2].autoretry).lower())
        self.assertEqual({'InstanceId':'i-exact','Force':'true'},calls[0][1].query)

    def test_write_once_upload_is_idempotent_only_for_identical_bytes(self):
        from types import SimpleNamespace
        from aliyun_ci.sdk import ObjectStore
        from aliyun_ci.ports import APIError
        class AlreadyExists(Exception):code='FileAlreadyExists'
        class Request:
            def __init__(self,**kwargs):self.__dict__.update(kwargs)
        class Client:
            def __init__(self):self.calls=[]
            def put_object(self,request):self.calls.append(request);raise AlreadyExists()
        store=ObjectStore.__new__(ObjectStore)
        store.sdk=SimpleNamespace(PutObjectRequest=Request)
        store.client=Client();store.bucket='example-bucket';store.get=lambda key:b'original'
        store.put('immutable-key',b'original',write_once=True)
        with self.assertRaisesRegex(APIError,'different bytes'):store.put('immutable-key',b'changed',write_once=True)
        self.assertTrue(all(request.forbid_overwrite for request in store.client.calls))

    def test_presign_temporary_credentials_must_cover_the_object_deadline(self):
        import os
        from types import SimpleNamespace
        from unittest.mock import patch
        from aliyun_ci.sdk import ObjectStore
        class Request:
            def __init__(self,**kwargs):self.__dict__.update(kwargs)
        for temporary in (True,False):
            with self.subTest(temporary=temporary):
                store=ObjectStore.__new__(ObjectStore)
                store.sdk=SimpleNamespace(PutObjectRequest=Request,GetObjectRequest=Request,
                    credentials=SimpleNamespace(Credentials=lambda *args:args,CredentialsProviderFunc=lambda fn:fn),
                    config=SimpleNamespace(load_default=SimpleNamespace),
                    Client=lambda settings:SimpleNamespace(presign=lambda request,**kwargs:called.append(kwargs) or SimpleNamespace(signed_headers=None,url='https://example.test/signed')))
                store.credential=SimpleNamespace(get_credential=lambda:SimpleNamespace(access_key_id='test-id',access_key_secret='test-secret',security_token='temporary-token' if temporary else None))
                called=[]
                store.client=SimpleNamespace(presign=lambda request,**kwargs:called.append(kwargs) or SimpleNamespace(signed_headers=None,url='https://example.test/signed'))
                store.bucket='example-bucket'
                store.region='cn-beijing'
                with patch.dict(os.environ,{},clear=True):
                    if temporary:
                        with self.assertRaisesRegex(ValueError,'known STS expiration'):
                            store.presign('PUT','run/final','2026-10-28T03:00:00Z')
                        self.assertEqual([],called)
                        os.environ['ALIYUN_CI_STS_NOT_AFTER']='2026-10-28T01:00:00Z'
                        with self.assertRaisesRegex(ValueError,'outlives'):
                            store.presign('PUT','run/final','2026-10-28T03:00:00Z')
                        self.assertEqual([],called)
                        os.environ['ALIYUN_CI_STS_NOT_AFTER']='2026-10-28T04:00:00Z'
                    self.assertEqual('https://example.test/signed',store.presign('PUT','run/final','2026-10-28T03:00:00Z'))
                    self.assertEqual(1,len(called))
