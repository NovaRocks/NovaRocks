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

"""Exact invocation replay tests; independent scripted API facts, no cloud cost."""
import base64
from datetime import timedelta
import json
import unittest
from aliyun_ci import cloud_command
from aliyun_ci.ports import APIError
from aliyun_ci.plan import timestamp
from .test_start import ScriptAPI, NOW
from .test_config import example
from aliyun_ci.config import validate_config


def row(*, instance='i-exact', invocation='invoke-original', status='Success', code=0, dropped=0, output='{}'):
    return {'Invocation': {'InvocationResults': {'InvocationResult': [
        {'InstanceId':instance,'InvokeId':invocation,'InvocationStatus':status,'ExitCode':code,
         'Dropped':dropped,'Output':base64.b64encode(output.encode()).decode()}]}}}


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.config=validate_config(example())
        self.command={'instance_id':'i-exact','invoke_id':'invoke-original','dispatch_state':'accepted',
                      'deadline':timestamp(NOW+timedelta(hours=1))}

    def test_dispatch_persists_before_effect_and_after_accepted_identity(self):
        saved=[]
        class API:
            def call(inner, action, **parameters):
                self.assertEqual('pending',saved[-1]['dispatch_state'])
                self.assertEqual('RunCommand',action)
                self.assertEqual(['i-exact'],parameters['InstanceId'])
                self.assertEqual('ProcessTree',parameters['TerminationMode'])
                return {'InvokeId':'invoke-original'}
        command=cloud_command.dispatch(self.config,API(),'i-exact','echo public',self.command['deadline'],
            persist=lambda v:saved.append(dict(v)),now=lambda:NOW)
        self.assertEqual(['pending','accepted'],[v['dispatch_state'] for v in saved])
        self.assertEqual('invoke-original',command['invoke_id'])

    def test_unknown_dispatch_never_replays_and_can_be_observed_without_api(self):
        saved=[];api=ScriptAPI([('RunCommand',TimeoutError())])
        with self.assertRaises(TimeoutError):
            cloud_command.dispatch(self.config,api,'i-exact','echo public',self.command['deadline'],
                persist=lambda v:saved.append(dict(v)),now=lambda:NOW)
        self.assertEqual('unknown',saved[-1]['dispatch_state'])
        result=cloud_command.observe(self.config,ScriptAPI([]),saved[-1])
        self.assertEqual('DispatchUnknown',result['status'])
        self.assertEqual(1,len(api.calls))

    def test_terminal_failure_and_dropped_output_are_original_facts(self):
        api=ScriptAPI([('DescribeInvocationResults',row(status='Failed',code=9,dropped=41,output='partial'))])
        result=cloud_command.observe(self.config,api,self.command)
        self.assertTrue(result['terminal']);self.assertEqual(9,result['exit_code'])
        self.assertEqual(41,result['dropped']);self.assertEqual('partial',result['output'])

    def test_identity_mismatch_refused_and_observation_error_does_not_dispatch(self):
        api=ScriptAPI([('DescribeInvocationResults',row(invocation='another'))])
        with self.assertRaisesRegex(ValueError,'identity mismatch'):
            cloud_command.observe(self.config,api,self.command)
        api=ScriptAPI([('DescribeInvocationResults',APIError('TransportUnknown'))])
        with self.assertRaises(APIError):cloud_command.observe(self.config,api,self.command)
        self.assertEqual(['DescribeInvocationResults'],[a for a,_ in api.calls])

    def test_wait_only_observes_original_invocation(self):
        api=ScriptAPI([('DescribeInvocationResults',row(status='Running',code=None)),
                       ('DescribeInvocationResults',row(output='{"finished":true}'))])
        result=cloud_command.wait(self.config,api,self.command,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('Success',result['status'])
        self.assertEqual(['invoke-original']*2,[p['InvokeId'] for _,p in api.calls])
