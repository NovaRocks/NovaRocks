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

"""Offline template and exact reviewed cloud change-set tests."""
import hashlib
import json
import unittest
from aliyun_ci.config import validate_config
from aliyun_ci.deploy import render, plan_deployment, apply_deployment, reviewed_change_digest
from .test_config import example
from .test_start import ScriptAPI


class DeployTests(unittest.TestCase):
    def test_render_is_deterministic_and_scopes_oidc_and_secret_access(self):
        config=validate_config(example());first=render(config)
        self.assertEqual(first,render(config))
        template=first['template']
        trust=template['Resources']['ControlRole']['Properties']['AssumeRolePolicyDocument']['Statement'][0]
        self.assertEqual('sts.aliyuncs.com',trust['Condition']['StringEquals']['oidc:aud'])
        self.assertEqual({'Fn::Sub':'repo:${Repository}:environment:aliyun-ci'},trust['Condition']['StringEquals']['oidc:sub'])
        self.assertEqual([],template['Resources']['OutboundSecurityGroup']['Properties']['SecurityGroupIngress'])
        self.assertEqual('sts:AssumeRole',template['Resources']['InstanceRole']['Properties']['AssumeRolePolicyDocument']['Statement'][0]['Action'])
        worker=template['Resources']['InstanceRole']['Properties']['Policies'][0]['PolicyDocument']
        self.assertIn('kms:DeleteSecret',worker['Statement'][1]['Action'])
        self.assertTrue(first['parameters']['SecretPrefix'].startswith('g16-'))
        statements=template['Resources']['ControlRole']['Properties']['Policies'][0]['PolicyDocument']['Statement']
        image_permissions=[row for row in statements if 'ecs:CreateImage' in row['Action']]
        self.assertTrue(any('image/*' in json.dumps(row['Resource']) for row in image_permissions))
        self.assertTrue(any('instance/*' in json.dumps(row['Resource']) and 'Condition' in row for row in image_permissions))
    def test_apply_binds_exact_reviewed_change_set(self):
        config=validate_config(example());change={'ChangeSetId':'cs-1','Status':'CREATE_COMPLETE','Changes':[{'Action':'Add'}],'RequestId':'inspection-request'}
        digest=reviewed_change_digest(change)
        observed=dict(change,RequestId='apply-request')
        api=ScriptAPI([('GetChangeSet',observed),('ExecuteChangeSet',{})])
        apply_deployment(config,api,change_set_id='cs-1',reviewed_digest=digest)
        self.assertEqual('cs-1',api.calls[-1][1]['ChangeSetId'])
        bad=ScriptAPI([('GetChangeSet',dict(change,Changes=[{'Action':'Remove'}]))])
        with self.assertRaisesRegex(ValueError,'reviewed'):apply_deployment(config,bad,change_set_id='cs-1',reviewed_digest=digest)
        self.assertEqual(1,len(bad.calls))
    def test_plan_requires_explicit_network_and_certificate_inputs(self):
        config=validate_config(example());api=ScriptAPI([])
        with self.assertRaisesRegex(ValueError,'parameters'):plan_deployment(config,api,stack_name='ci',parameters={})
        self.assertEqual([],api.calls)

    def test_local_foundation_requires_only_network_and_no_github(self):
        data=example();data.pop('github');data['execution_kind']='local-rust'
        config=validate_config(data);value=render(config,{'VpcId':'vpc-existing'})
        self.assertEqual({'OutboundSecurityGroup','OptionalVSwitch','InstanceRole'},set(value['template']['Resources']))
        self.assertNotIn('GitHubOidcFingerprint',value['template']['Parameters'])
        from aliyun_ci.deploy import check_deployment
        outputs={key: config[source] for key,source in [('SecurityGroupId','security_group_id'),('InstanceRoleName','instance_ram_role')]}
        outputs.update(BucketName=config['oss']['bucket'],ObjectPrefix=config['oss']['prefix'])
        api=ScriptAPI([('GetStack',{'Status':'CREATE_COMPLETE','Outputs':[{'OutputKey':k,'OutputValue':v} for k,v in outputs.items()]})])
        self.assertTrue(check_deployment(config,api,stack_id='stack')['matched'])
