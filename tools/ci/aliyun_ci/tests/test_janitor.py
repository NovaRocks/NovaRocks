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

"""Janitor ownership and original expiry facts survive cloud-side automatic release."""
from datetime import timedelta
import unittest
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from aliyun_ci.janitor import sweep
from .test_config import example
from .test_start import ScriptAPI,EMPTY,NOW,instance
from .test_teardown import Store, EmptyGitHub


class JanitorTests(unittest.TestCase):
    def test_only_overdue_deployment_resources_are_deleted(self):
        config=validate_config(example());plan=derive(config,'42',1,NOW)
        foreign=instance(config,plan)
        foreign['Tags']['Tag']=[{'TagKey':k,'TagValue':('Other/Repository' if k=='repository' else v)} for k,v in plan.tags.items()]
        current=NOW+timedelta(hours=5)
        api=ScriptAPI([('DescribeInstances',{'TotalCount':2,'Instances':{'Instance':[instance(config,plan),foreign]}}),
                       ('DescribeInstances',{'TotalCount':1,'Instances':{'Instance':[instance(config,plan)]}}),('DeleteInstance',{}),('DescribeInstances',EMPTY)])
        oos=ScriptAPI([('ListSecretParameters',{'Parameters':[]}),('ListSecretParameters',{'Parameters':[]})])
        result=sweep(config,api,oos,Store(),EmptyGitHub(),now=lambda:current,sleep=lambda _:None)
        self.assertEqual(['i-exact'],[params['InstanceId'] for action,params in api.calls if action=='DeleteInstance'])
        self.assertTrue(result['anomalies'][-1]['missing_teardown_summary'])
    def test_local_runner_original_labels_allow_cleanup_after_auto_release(self):
        config=validate_config(example());plan=derive(config,'local-20261028T000000Z-abcd',1,NOW,nonce='a'*12)
        tags=dict(plan.tags,account_id=str(config['account_id']),region=config['region'],instance_id='i-exact')
        runner={'id':5,'name':plan.label,'status':'offline','labels':[{'name':f'g16-{key}:{value}'} for key,value in tags.items()]}
        class GH:
            def __init__(self):self.deleted=[];self.alive=True
            def call(self,method,path,body=None):
                if method=='DELETE':self.deleted.append(path);self.alive=False;return None
                return {'runners':[runner] if self.alive else []}
        gh=GH();api=ScriptAPI([('DescribeInstances',EMPTY),('DescribeInstances',EMPTY)])
        oos=ScriptAPI([('ListSecretParameters',{'Parameters':[]}),('ListSecretParameters',{'Parameters':[]})])
        result=sweep(config,api,oos,Store(),gh,now=lambda:NOW+timedelta(hours=5))
        self.assertEqual(['/repos/NovaRocks/NovaRocks/actions/runners/5'],gh.deleted)
        self.assertTrue(result['anomalies'][-1]['missing_teardown_summary'])

    def test_local_sweep_skips_other_execution_kinds_without_github(self):
        data=example();data.pop('github');data['execution_kind']='local-rust'
        config=validate_config(data);old=derive(validate_config(example()),'42',1,NOW)
        class DeniedGitHub:
            def call(self,*args,**kwargs):raise AssertionError('local sweep cannot call GitHub')
        api=ScriptAPI([('DescribeInstances',{'TotalCount':1,'Instances':{'Instance':[instance(config,old)]}})])
        result=sweep(config,api,ScriptAPI([('ListSecretParameters',{'Parameters':[]})]),Store(),DeniedGitHub(),now=lambda:NOW+timedelta(hours=5))
        self.assertEqual([],result['anomalies'])
        self.assertEqual(['DescribeInstances'],[action for action,params in api.calls])
