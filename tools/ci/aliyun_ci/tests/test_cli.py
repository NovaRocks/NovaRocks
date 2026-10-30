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

"""Offline commands stay usable without SDKs or credentials; identities round-trip."""
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from aliyun_ci import cli
from aliyun_ci.config import validate_config
from aliyun_ci.plan import derive
from .test_config import example


class CliTests(unittest.TestCase):
    def test_offline_commands_need_no_cloud_import_or_credentials(self):
        path = Path(__file__).parents[1] / 'config.example.json'
        for command in (['validate-config'], ['plan-run', '--run-id', '42', '--now', '2026-10-28T00:00:00Z', '--nonce', 'fixed'], ['deploy', 'render']):
            with self.subTest(command=command), redirect_stdout(io.StringIO()) as output:
                self.assertEqual(0, cli.main(['--config', str(path), *command]))
                result = json.loads(output.getvalue())
            self.assertIsInstance(result, dict)

    def test_saved_start_plan_keeps_original_absolute_deadline(self):
        config = validate_config(example())
        plan = derive(config, '42', 2, datetime(2026, 10, 28, tzinfo=timezone.utc))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'plan.json'
            path.write_text(json.dumps({'plan': plan.to_dict()}))
            args = cli.parser().parse_args(['--config', 'unused', 'teardown', '--plan', str(path)])
            self.assertEqual(plan, cli.load_plan(config, args))
            data = plan.to_dict(); data['tags']['deployment'] = 'another'
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, 'deployment'):
                cli.load_plan(config, args)

    def test_local_prepare_is_offline_and_dirty_rejected_before_sdk(self):
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);repo=root/'repo';repo.mkdir()
            def git(*args):return subprocess.run(['git','-C',str(repo),*args],check=True,capture_output=True)
            git('init');git('config','user.email','ci@example.invalid');git('config','user.name','CI')
            (repo/'file').write_text('original');git('add','file');git('commit','-m','Initial')
            data=example();data.pop('github');data['execution_kind']='local-rust'
            config=root/'config.json';config.write_text(json.dumps(data))
            command=['--config',str(config),'local-run','--source',str(repo),'--image-id','m-candidate','--record',str(root/'attempt/run.json'),'--prepare-only']
            with redirect_stdout(io.StringIO()) as output:
                self.assertEqual(0,cli.main(command))
            self.assertEqual('local-rust',json.loads(output.getvalue())['plan']['execution_kind'])
            (repo/'file').write_text('dirty')
            with self.assertRaisesRegex(SystemExit,'2'):
                cli.main(command)

    def test_observation_failure_cancels_before_exact_reclamation(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            record=Path(directory)/'run.json'
            args=cli.parser().parse_args(['--config','unused','local-run','--image-id','m-one','--record',str(record)])
            data=example();data.pop('github');data['execution_kind']='local-rust';config=validate_config(data)
            order=[]
            def start(*args,**kwargs):
                order.append('start');record.write_text('{}')
            def wait(*args,**kwargs):
                order.append('observe');raise ConnectionError('observation disconnected')
            def cancel(*args,**kwargs):order.append('cancel original batch')
            def finish(*args,**kwargs):
                order.append('collect and reclaim');return {'compute_terminal':'released','leftovers':[]}
            with patch('aliyun_ci.source_bundle.create',return_value={}),patch('aliyun_ci.sdk.adapters',return_value={'ecs':object(),'oos':object(),'oss':object()}),patch('aliyun_ci.local_run.start_local',side_effect=start),patch('aliyun_ci.local_run.wait_local',side_effect=wait),patch('aliyun_ci.local_run.cancel_local',side_effect=cancel),patch('aliyun_ci.local_run.finish_local',side_effect=finish):
                result,code=cli.execute(config,args)
            self.assertEqual(['start','observe','cancel original batch','collect and reclaim'],order)
            self.assertEqual(1,code)
            self.assertEqual('released',result['compute_terminal'])
            self.assertEqual('ConnectionError',json.loads(record.read_text())['summary']['control_error']['error'])
