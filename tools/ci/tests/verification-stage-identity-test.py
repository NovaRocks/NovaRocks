#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Native stage descriptions share the real command boundary with execution."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

REPO=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(REPO/'tools/ci'))
from verification import run
import invocation_record as record


class Stages(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(prefix='stage-identity-')
        self.addCleanup(self.temporary.cleanup)
        self.root=Path(self.temporary.name)
        self.native={'inputs':str(self.root/'inputs')}

    def test_real_describe_boundary_does_not_execute_or_create_runtime(self):
        marker=self.root/'executed';bin_dir=self.root/'bin';bin_dir.mkdir()
        cargo=bin_dir/'cargo';cargo.write_text('#!/bin/sh\necho executed >"$STAGE_EXECUTION_MARKER"\nexit 91\n');cargo.chmod(0o755)
        environment=dict(os.environ,PATH=str(bin_dir)+os.pathsep+os.environ['PATH'],STAGE_EXECUTION_MARKER=str(marker))
        for name in ('CI build inputs','locked Cargo metadata','legacy branding','workspace nextest and doctests','memory-model:loom'):
            output=self.root/('runtime-'+name.replace('/','-'))
            result=subprocess.run(['bash',str(REPO/'tools/ci/local-full-ci.sh'),'--describe-stage',name,str(self.root/'absent-receipt'),str(output)],cwd=REPO,env=environment,text=True,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
            value=json.loads(result.stdout)
            self.assertEqual(value['name'],name)
            self.assertEqual(value['argv'],value['spec']['native_command'])
            self.assertFalse(output.exists())
        self.assertFalse(marker.exists())

    def test_real_default_stage_and_target_identity_match_without_rewriting_argv(self):
        output=self.root/'native';output.mkdir()
        shell="""source tools/ci/local-full-ci.sh --source-only
CI_RUN_DIR="$1"
ci_record_stage() { :; }
ci_render_summary() { :; }
run_fail_fast_stage 'CI build inputs' stage.log ci_validate_build_inputs dev
"""
        result=subprocess.run(['bash','-c',shell,'fixture',str(output)],cwd=REPO,text=True,capture_output=True)
        self.assertEqual(result.returncode,0,result.stderr)
        path=next(p for p in (output/'invocations').glob('*.json') if not p.name.endswith('.started.json'))
        actual=json.loads(path.read_text())
        item={'name':'CI build inputs'}
        spec=run.stage_spec(REPO,self.native,item,self.root/'wrapper-output')
        expected=record.snapshot(REPO,record.execution_spec('stage',run.stage_argv(REPO,self.native,item,self.root/'wrapper-output'),spec),os.environ)
        self.assertEqual(actual['inputs_start'],expected)
        self.assertEqual(actual['argv'],['ci_validate_build_inputs','dev'])
        self.assertIn('source',expected)
        self.assertTrue(actual['inputs_stable'])

    def test_selected_middle_stage_preserves_native_failure_and_runs_no_other_commands(self):
        marker=self.root/'cargo-args';bin_dir=self.root/'bin';bin_dir.mkdir()
        cargo=bin_dir/'cargo'
        cargo.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >>"$STAGE_EXECUTION_MARKER"\nexit 23\n')
        cargo.chmod(0o755)
        env=dict(os.environ,PATH=str(bin_dir)+os.pathsep+os.environ['PATH'],STAGE_EXECUTION_MARKER=str(marker))
        result=subprocess.run(['bash',str(REPO/'tools/ci/local-full-ci.sh'),'--run-stage','Cargo dependency policy',
            str(self.root/'unused-receipt'),str(self.root/'output')],cwd=REPO,env=env,text=True,capture_output=True)
        self.assertEqual(marker.read_text().splitlines(),['deny --locked check bans licenses sources'])
        self.assertEqual(result.returncode,23,result.stderr)
        self.assertEqual(result.stderr,'')

    def test_native_script_and_config_facts_survive_wrapper_and_output_changes(self):
        script=self.root/'native';script.write_text('#!/bin/sh\nexit 0\n');script.chmod(0o755)
        config=self.root/'config';config.write_text('config-one')
        argv=[str(script),'--config',str(config)]
        spec=record.stage_descriptor('fixture',argv)['spec']
        wrapper=['bash',str(REPO/'tools/ci/local-full-ci.sh'),'--run-stage','fixture','receipt','output-one']
        with mock.patch.object(record,'source_snapshot',return_value={'source':{'worktree_tree':'same'}}),mock.patch.object(record,'tools',return_value={'fixed':'tools'}):
            actual=record.start(self.root,self.root,'stage','fixture',['stage:fixture'],argv)
            wrapped=record.start(self.root,self.root,'stage','fixture',['stage:fixture'],wrapper,spec)
            left=json.loads(actual.read_text());right=json.loads(wrapped.read_text())
            self.assertEqual(left['inputs_start'],right['inputs_start'])
            self.assertEqual(left['argv'],argv);self.assertEqual(right['argv'],wrapper)
            wrapper[-1]='output-two'
            self.assertEqual(left['inputs_start'],record.snapshot(self.root,record.execution_spec('stage',wrapper,spec),os.environ))
            script.write_text('#!/bin/sh\nexit 1\n')
            self.assertNotEqual(left['inputs_start'],record.snapshot(self.root,spec,os.environ))
            script.write_text('#!/bin/sh\nexit 0\n');config.write_text('config-two')
            self.assertNotEqual(left['inputs_start'],record.snapshot(self.root,spec,os.environ))
            self.assertIn('executor',right['inputs_start']['files_explicit'])
            self.assertIn('--config',right['inputs_start']['files_explicit'])

    def test_effective_stage_values_and_only_declared_outputs_affect_identity(self):
        def capture(argv,outputs=()):
            spec=record.stage_descriptor('same-unit',argv,list(outputs))['spec']
            return record.snapshot(self.root,spec,{})
        with mock.patch.object(record,'source_snapshot',return_value={'source':{'worktree_tree':'same'}}),mock.patch.object(record,'tools',return_value={'fixed':'tools'}):
            self.assertNotEqual(capture(['ci_validate_build_inputs','dev']),capture(['ci_validate_build_inputs','release']))
            self.assertNotEqual(capture(['cargo','build','--profile','dev']),capture(['cargo','build','--profile','release']))
            self.assertEqual(capture(['ci_prepare_main','/output/one'],[1]),capture(['ci_prepare_main','/output/two'],[1]))
            self.assertEqual(capture(['python3','owner.py','--output','one']),capture(['python3','owner.py','--output','two']))
            self.assertNotEqual(capture(['owner','input-one']),capture(['owner','input-two']))
            one=self.root/'receipt-one';one.write_text('same content')
            two=self.root/'receipt-two';two.write_text('same content')
            first=record.stage_descriptor('same-unit',['ci_capture_main_builds',str(one),'out-one'],[2],[1])['spec']
            second=record.stage_descriptor('same-unit',['ci_capture_main_builds',str(two),'out-two'],[2],[1])['spec']
            self.assertEqual(record.snapshot(self.root,first,{}),record.snapshot(self.root,second,{}))
            two.write_text('changed content')
            self.assertNotEqual(record.snapshot(self.root,first,{}),record.snapshot(self.root,second,{}))

    def test_unknown_stage_description_fails_and_cannot_bootstrap(self):
        output=self.root/'runtime'
        value=subprocess.run(['bash',str(REPO/'tools/ci/local-full-ci.sh'),'--describe-stage','unknown-stage',str(self.root/'absent-receipt'),str(output)],cwd=REPO,text=True,capture_output=True)
        self.assertNotEqual(value.returncode,0)
        self.assertFalse(output.exists())


if __name__=='__main__':unittest.main()
