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

"""Local control correctness using exact API facts and real hashed archives.

CI maintainers run this inexpensive suite when local control changes. The
independent native JSON/log bytes are never manufactured into a product pass.
"""
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from aliyun_ci.config import validate_config
from aliyun_ci.handoff import pack
from aliyun_ci.local_run import (start_local,status_local,wait_local,cancel_local,collect_local,
    finish_local,result_exit_code,_save,_cancel_script)
from aliyun_ci.plan import derive
from aliyun_ci.ports import APIError
from .test_start import ScriptAPI,NOW,EMPTY,CREATED,instance,listed
from .test_cloud_command import row
from .test_config import example


class Store:
    def __init__(self):self.objects={};self.calls=[]
    def get(self,key):
        self.calls.append(('get',key))
        if key not in self.objects:raise APIError('NoSuchKey')
        return self.objects[key]
    def put(self,key,data,**kwargs):self.calls.append(('put',key));self.objects[key]=data
    def presign(self,method,key,expires_at):return 'https://example.invalid/capability-secret'


class LocalTests(unittest.TestCase):
    def setUp(self):
        data=example();data.pop('github');data['execution_kind']='local-rust'
        self.config=validate_config(data)
        self.plan=derive(self.config,'local-20261029T000000Z-abcd',1,NOW)
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.record=self.root/'run.json'
        self.store=Store()
        self.store.objects[f"{self.config['oss']['prefix']}/images/m-exact/release.json"]=json.dumps(
            {'image_id':'m-exact','region':self.config['region']}).encode()
        self.source=self.root/'source';self.source.mkdir();(self.source/'source.bundle').write_bytes(b'exact-source')
        self.manifest={'sha':'a'*40,'tree':'b'*40,'bundle_sha256':hashlib.sha256(b'exact-source').hexdigest()}
        (self.source/'source.json').write_text(json.dumps(self.manifest))

    def prepare_record(self,**updates):
        value={'schema':1,'config':self.config.data,'plan':self.plan.to_dict(),
            'image_id':'m-exact','instance_id':'i-exact','source':self.manifest,'phase':'observing',
            'command':{'instance_id':'i-exact','invoke_id':'invoke-original','dispatch_state':'accepted',
                       'deadline':self.plan.deadline}}
        value.update(updates);_save(self.record,value,exclusive=True);return value

    def archive(self,kind='final'):
        attempt=self.root/'attempt';(attempt/'rust').mkdir(parents=True)
        raw_index={'actual_native_exit':7,'unchanged':['nextest','doctest']}
        (attempt/'rust'/'execution-index.json').write_text(json.dumps(raw_index))
        (attempt/'stdout.log').write_bytes(b'original failure\n')
        archive=pack(attempt,kind=kind)
        self.addCleanup(lambda: __import__('shutil').rmtree(archive.parent))
        self.store.objects[self.plan.oss_keys[kind]]=archive.read_bytes()
        return raw_index

    def test_start_persists_source_identity_and_never_writes_capabilities_to_record(self):
        api=ScriptAPI([('DescribeInstances',EMPTY),('RunInstances',CREATED),
            ('DescribeInstances',listed(instance(self.config,self.plan))),('RunCommand',{'InvokeId':'invoke-original'})])
        class OOS:
            def call(inner,action,**parameters):
                self.assertTrue(self.record.exists());return {}
        value=start_local(self.config,self.plan,api,OOS(),self.store,image_id='m-exact',
            source=self.source,record_path=self.record,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('invoke-original',value['command']['invoke_id'])
        self.assertEqual(self.manifest,value['source'])
        self.assertNotIn('capability-secret',self.record.read_text())
        self.assertEqual(1,sum(a=='RunInstances' for a,_ in api.calls))
        with self.assertRaises(FileExistsError):
            start_local(self.config,self.plan,ScriptAPI([]),OOS(),self.store,image_id='m-exact',
                source=self.source,record_path=self.record,now=lambda:NOW)

    def test_status_reconnection_only_observes_same_nonzero_batch(self):
        self.prepare_record()
        task={'identity':{'run_id':self.plan.run_id,'run_attempt':1},'state':'complete','exit_code':7}
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('DescribeInvocationResults',row(output=json.dumps(task)))])
        value=status_local(self.config,api,self.record)
        self.assertEqual(7,value['task']['exit_code'])
        self.assertEqual(['DescribeInstances','DescribeInvocationResults'],[a for a,_ in api.calls])
        self.assertEqual('invoke-original',api.calls[-1][1]['InvokeId'])

    def test_dropped_output_cannot_supply_batch_result(self):
        self.prepare_record()
        api=ScriptAPI([('DescribeInstances',EMPTY),('DescribeInvocationResults',row(dropped=3))])
        self.assertIsNone(status_local(self.config,api,self.record)['task'])

    def test_collect_preserves_native_result_and_log_bytes_with_member_verification(self):
        self.prepare_record();native=self.archive()
        value=collect_local(self.config,self.store,self.record,self.root/'results')
        self.assertTrue(value['archives']['final']['manifest_ok'])
        self.assertEqual(native,value['native']['final']['execution_index']['value'])
        self.assertEqual(b'original failure\n',(self.root/'results/final/attempt/stdout.log').read_bytes())
        self.assertFalse(value['archives']['interrupted']['present'])

    def test_unknown_archive_read_is_not_absent(self):
        self.prepare_record()
        class Denied:
            def get(inner,key):raise APIError('AccessDenied')
        value=collect_local(self.config,Denied(),self.record,self.root/'results')
        self.assertIsNone(value['archives']['final']['present'])

    def test_collect_failure_still_releases_exact_instance_without_github(self):
        self.prepare_record()
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('DescribeInstances',listed(instance(self.config,self.plan))),('DeleteInstance',{}),('DescribeInstances',EMPTY)])
        oos=ScriptAPI([('ListSecretParameters',{'Parameters':[]})])
        with patch('aliyun_ci.local_run.collect_local',side_effect=OSError('disk full')):
            result=finish_local(self.config,api,oos,self.store,self.record,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('released',result['compute_terminal'])
        self.assertEqual('disk full',result['local_collection_error'])
        self.assertEqual(1,result_exit_code(result))

    def test_nonzero_native_result_does_not_erase_successful_handoff_or_release(self):
        self.prepare_record(observation={'task':{'exit_code':7}});native=self.archive()
        api=ScriptAPI([('DescribeInstances',EMPTY),('DescribeInstances',EMPTY)])
        result=finish_local(self.config,api,ScriptAPI([('ListSecretParameters',{'Parameters':[]})]),
            self.store,self.record,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('released',result['compute_terminal'])
        self.assertEqual(native,result['rust_result']['value'])
        self.assertEqual(7,result['local_task']['exit_code'])
        self.assertEqual(1,result_exit_code(result))

    def test_cancel_never_uses_stop_invocation_or_repeats_unknown_command(self):
        self.prepare_record()
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),('RunCommand',TimeoutError())])
        with self.assertRaises(TimeoutError):cancel_local(self.config,api,self.record,now=lambda:NOW)
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan)))])
        result=cancel_local(self.config,api,self.record,now=lambda:NOW)
        self.assertEqual('DispatchUnknown',result['status'])
        self.assertEqual(['DescribeInstances'],[a for a,_ in api.calls])

    def test_failed_release_is_independent_from_valid_materials(self):
        self.prepare_record(observation={'task':{'exit_code':0}});self.archive()
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('DescribeInstances',listed(instance(self.config,self.plan))),('DeleteInstance',APIError('Forbidden'))])
        result=finish_local(self.config,api,ScriptAPI([('ListSecretParameters',{'Parameters':[]})]),
            self.store,self.record,now=lambda:NOW,sleep=lambda _:None)
        self.assertEqual('unknown',result['compute_terminal'])
        self.assertTrue(result['local_collection']['archives']['final']['manifest_ok'])
        self.assertEqual('compute',result['leftovers'][0]['kind'])
        self.assertEqual(1,result_exit_code(result))

    def test_keyboard_interrupt_dispatch_preserves_unknown_original_identity(self):
        self.prepare_record()
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),('RunCommand',KeyboardInterrupt())])
        with self.assertRaises(KeyboardInterrupt):
            cancel_local(self.config,api,self.record,now=lambda:NOW)
        value=json.loads(self.record.read_text())
        self.assertEqual('unknown',value['cancellation']['dispatch_state'])
        self.assertEqual('invoke-original',value['command']['invoke_id'])

    def test_observation_merge_retains_concurrent_cancellation_identity(self):
        self.prepare_record()
        class ConcurrentAPI:
            def call(inner,action,**parameters):
                if action == 'DescribeInstances':return EMPTY
                value=json.loads(self.record.read_text())
                value['cancellation']={'invoke_id':'cancel-original','dispatch_state':'accepted'}
                _save(self.record,value)
                return row(status='Running')
        status_local(self.config,ConcurrentAPI(),self.record)
        self.assertEqual('cancel-original',json.loads(self.record.read_text())['cancellation']['invoke_id'])

    def test_credential_expiry_refusal_precedes_allocation(self):
        class Refused(Store):
            def presign(inner,*args):raise ValueError('known credential expiry is too short')
        store=Refused();store.objects.update(self.store.objects)
        with self.assertRaisesRegex(ValueError,'expiry'):
            start_local(self.config,self.plan,ScriptAPI([]),ScriptAPI([]),store,image_id='m-exact',
                source=self.source,record_path=self.record,now=lambda:NOW)
        self.assertFalse(self.record.exists())

    def test_rsproxy_requires_guest_reported_image_capability_before_any_allocation(self):
        self.config = validate_config(dict(self.config.data, cargo_registry='rsproxy'))
        key = f"{self.config['oss']['prefix']}/images/m-exact/release.json"
        for capabilities in (None, ['crates-io']):
            release = {'image_id': 'm-exact', 'region': self.config['region']}
            if capabilities is not None:
                release['local_rust_cargo_registries'] = capabilities
            self.store.objects[key] = json.dumps(release).encode()
            api = ScriptAPI([])
            with self.subTest(capabilities=capabilities), self.assertRaisesRegex(ValueError, 'does not support'):
                start_local(self.config, self.plan, api, ScriptAPI([]), self.store, image_id='m-exact',
                    source=self.source, record_path=self.record, now=lambda: NOW)
            self.assertEqual([], api.calls)
            self.assertFalse(any(action == 'put' for action, *_ in self.store.calls))
            self.assertFalse(self.record.exists())

    def test_rsproxy_choice_reaches_original_bootstrap_payload_on_supported_image(self):
        self.config = validate_config(dict(self.config.data, cargo_registry='rsproxy'))
        self.store.objects[f"{self.config['oss']['prefix']}/images/m-exact/release.json"] = json.dumps(
            {'image_id': 'm-exact', 'region': self.config['region'],
             'local_rust_cargo_registries': ['crates-io', 'rsproxy']}).encode()
        api = ScriptAPI([('DescribeInstances', EMPTY), ('RunInstances', CREATED),
            ('DescribeInstances', listed(instance(self.config, self.plan))),
            ('RunCommand', {'InvokeId': 'invoke-original'})])
        payloads = []
        with patch('aliyun_ci.local_run.publish_bootstrap', side_effect=lambda *args: payloads.append(args[-1])):
            value = start_local(self.config, self.plan, api, ScriptAPI([]), self.store, image_id='m-exact',
                source=self.source, record_path=self.record, now=lambda: NOW, sleep=lambda _: None)
        self.assertEqual('rsproxy', payloads[0]['cargo_registry'])
        self.assertEqual('rsproxy', value['config']['cargo_registry'])
        self.assertEqual(1, sum(action == 'RunInstances' for action, _ in api.calls))

    def test_swap_requires_guest_capability_and_reaches_supported_worker_payload(self):
        self.config = validate_config(dict(self.config.data, swap_gib=16, build_jobs=8))
        key = f"{self.config['oss']['prefix']}/images/m-exact/release.json"
        for supported in (None, [0]):
            release = {'image_id': 'm-exact', 'region': self.config['region']}
            if supported is not None:
                release['local_rust_swap_gib'] = supported
            self.store.objects[key] = json.dumps(release).encode()
            api = ScriptAPI([])
            with self.subTest(supported=supported), self.assertRaisesRegex(ValueError, 'does not support local swap'):
                start_local(self.config, self.plan, api, ScriptAPI([]), self.store, image_id='m-exact',
                    source=self.source, record_path=self.record, now=lambda: NOW)
            self.assertEqual([], api.calls); self.assertFalse(self.record.exists())
            self.assertFalse(any(action == 'put' for action, *_ in self.store.calls))
        release['local_rust_swap_gib'] = [0, 16]; self.store.objects[key] = json.dumps(release).encode()
        api = ScriptAPI([('DescribeInstances', EMPTY), ('RunInstances', CREATED),
            ('DescribeInstances', listed(instance(self.config, self.plan))),
            ('RunCommand', {'InvokeId': 'invoke-original'})])
        payloads = []
        with patch('aliyun_ci.local_run.publish_bootstrap', side_effect=lambda *args: payloads.append(args[-1])):
            start_local(self.config, self.plan, api, ScriptAPI([]), self.store, image_id='m-exact',
                source=self.source, record_path=self.record, now=lambda: NOW, sleep=lambda _: None)
        self.assertEqual(16, payloads[0]['swap_gib']); self.assertEqual(8, payloads[0]['build_jobs'])

    def test_cancel_without_recorded_instance_has_no_cloud_effect(self):
        self.prepare_record(instance_id=None)
        value=cancel_local(self.config,ScriptAPI([]),self.record,now=lambda:NOW)
        self.assertFalse(value['signal_sent']);self.assertIsNone(value['instance_id'])

    def test_cancel_recovers_complete_original_task_when_observer_dispatch_is_unknown(self):
        self.prepare_record(command={'instance_id':'i-exact','invoke_id':None,
            'dispatch_state':'unknown','deadline':self.plan.deadline})
        task={'identity':{'run_id':self.plan.run_id,'run_attempt':1},'state':'complete',
              'exit_code':143,'batch':{'exited':True},'handoff':{'uploaded':True}}
        result={'cancellation':{'signal_sent':True,'exited':True},'task':task,
                'task_completion_observed':True}
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('RunCommand',{'InvokeId':'cancel-original'}),
            ('DescribeInvocationResults',row(invocation='cancel-original',output=json.dumps(result)))])
        value=cancel_local(self.config,api,self.record,now=lambda:NOW)
        self.assertTrue(value['task_completion_observed'])
        self.assertEqual(143,value['task']['exit_code'])
        self.assertEqual(1,sum(a=='RunCommand' for a,_ in api.calls))
        record=json.loads(self.record.read_text())
        self.assertEqual('unknown',record['command']['dispatch_state'])
        self.assertEqual(task,record['observation']['task'])
        # Re-observing an unknown original invocation cannot erase the terminal
        # root status recovered through the one cancellation command.
        value=status_local(self.config,ScriptAPI([('DescribeInstances',EMPTY)]),self.record)
        self.assertEqual(task,value['task'])
        self.assertEqual('DispatchUnknown',value['cloud_command']['status'])

    def test_cloud_cancellation_does_not_prove_native_exit(self):
        self.prepare_record(cancellation={'instance_id':'i-exact','invoke_id':'cancel-original',
            'dispatch_state':'accepted','deadline':self.plan.deadline})
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('DescribeInvocationResults',row(invocation='cancel-original',status='Cancelled',code=137))])
        value=cancel_local(self.config,api,self.record,now=lambda:NOW)
        self.assertEqual('Cancelled',value['status'])
        self.assertNotIn('task_completion_observed',value)
        self.assertNotIn('task',json.loads(self.record.read_text()).get('observation',{}))

    def test_cancel_budget_expiry_keeps_native_exit_unknown(self):
        self.prepare_record(cancellation={'instance_id':'i-exact','invoke_id':'cancel-original',
            'dispatch_state':'accepted','deadline':self.plan.deadline})
        task={'identity':{'run_id':self.plan.run_id,'run_attempt':1},'state':'complete',
              'exit_code':None,'batch':{'exited':False},'handoff':{'uploaded':True}}
        output={'cancellation':{'signal_sent':True,'exited':False},'task':task}
        api=ScriptAPI([('DescribeInstances',listed(instance(self.config,self.plan))),
            ('DescribeInvocationResults',row(invocation='cancel-original',output=json.dumps(output)))])
        value=cancel_local(self.config,api,self.record,now=lambda:NOW)
        self.assertFalse(value['native_cancellation']['exited'])
        self.assertIsNone(value['task']['exit_code'])
        self.assertFalse(value['task']['batch']['exited'])

    def test_root_cancel_waits_for_original_supervisor_upload_and_completion(self):
        import os,subprocess,sys,threading,time
        from datetime import datetime,timedelta,timezone
        runtime=self.root/'runtime';runtime.mkdir()
        identity={'run_id':self.plan.run_id,'run_attempt':1}
        (runtime/'bootstrap.json').write_text(json.dumps({'execution_kind':'local-rust',
            'identity':identity,'deadline':(datetime.now(timezone.utc)+timedelta(seconds=10)).isoformat(),
            'watcher':{'cancel_budget_seconds':2}}))
        (runtime/'local-status.json').write_text(json.dumps({'identity':identity,'state':'running'}))
        worker=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])
        self.addCleanup(lambda: worker.kill() if worker.poll() is None else None)
        (runtime/'batch.pid').write_text(str(worker.pid))
        complete={'identity':identity,'state':'complete','exit_code':-15,
                  'batch':{'exited':True},'handoff':{'uploaded':True}}
        def archive_after_exit():
            worker.wait(timeout=5)
            time.sleep(0.25)
            temporary=runtime/'status.new';temporary.write_text(json.dumps(complete))
            temporary.replace(runtime/'local-status.json')
        producer=threading.Thread(target=archive_after_exit);producer.start()
        try:
            script=_cancel_script(self.plan,2)
            body=script.split("<<'PY'\n",1)[1].rsplit('\nPY',1)[0]
            body=body.replace("Path('/run/novarocks-ci')",repr(runtime).replace('PosixPath','Path'))
            body=body.replace("pwd.getpwnam('runner').pw_uid",str(os.getuid()))
            env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[2]))
            started=time.monotonic()
            result=subprocess.run([sys.executable,'-c',body],env=env,text=True,capture_output=True,timeout=6,check=True)
            value=json.loads(result.stdout)
            self.assertGreaterEqual(time.monotonic()-started,0.25)
            self.assertTrue(value['task_completion_observed'])
            self.assertTrue(value['task']['handoff']['uploaded'])
            self.assertTrue(value['cancellation']['signal_sent'])
            self.assertTrue(value['cancellation']['exited'])
        finally:
            producer.join(timeout=6)
            if worker.poll() is None:worker.kill()
            worker.wait()
