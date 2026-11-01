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
"""Boundaries of development orchestration; native outputs remain the oracle."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from verification import run
from verification.model import Attempt, Observation, Scope, UnitKey


def put(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2)+'\n')
    return path


def rust_batch(root,binary='p',name='regular',identity=None):
    identity=identity or {'executable_sha256':'binary','nextest_profile':'default','nextest_config_sha256':'config'}
    put(root/'inputs/prepared.json',{'schema':1,'profile':'default','config_sha256':identity['nextest_config_sha256']})
    put(root/'inputs/test-artifacts.json',{'schema':1,'binaries':{binary:{'sha256':identity['executable_sha256']}}})
    put(root/'inputs/selected-tests.json',{'rust-suites':{binary:{'testcases':{name:{'filter-match':{'status':'matches'}}}}}})
    (root/'inputs/tool-config.toml').write_text('[store]\ndir = "/private/materials"\n')
    key=binary+'::'+name
    put(root/'identity/source-start.json',{'schema':1,'source':{'commit':'a'*40,'dirty':False,'worktree_tree':'tree'}})
    put(root/'identity/nextest.json',{'schema':1,'domain':'nextest','scopes':{key:{'declared':True,'inputs_start':identity,'inputs_end':identity,'inputs_stable':True}}})
    put(root/'rust-run/cleanup-summary.json',{'schema':1,'registration_closed':True,'complete':True,'ok':True,'records':[],'errors':[]})
    junit=root/'nextest/default/junit.xml';junit.parent.mkdir(parents=True)
    junit.write_text('<testsuites><testsuite name="'+binary+'"><testcase name="'+name+'" classname="'+binary+'"/></testsuite></testsuites>')
    put(root/'execution-index.json',{'schema':1,'complete':False,'stages':{'nextest':{'started':True,'returncode':0,'errors':[]}},
        'materials':{'inputs':'inputs','selection':'inputs/selected-tests.json','test_bindings':'inputs/test-artifacts.json',
        'tool_config':'inputs/tool-config.toml','junit':{'path':'nextest/default/junit.xml','complete':True},'rust_cleanup':'rust-run/cleanup-summary.json'}})
    return identity


class Run(unittest.TestCase):
    def test_ecs_oss_requires_an_explicit_absolute_config_file_without_probing_cloud(self):
        config=self.base/'oss.json'
        config.write_text('not parsed by the availability observer')
        probe=mock.Mock(side_effect=AssertionError('availability must not probe cloud services'))
        for path in (None,'oss.json',str(self.base/'missing.json'),str(self.base)):
            with self.subTest(path=path):
                environment={} if path is None else {'NOVA_OSS_TEST_CONFIG':path}
                result=run.requirement_checks(['fixture:aliyun-ecs-oss'],environment,self.native,probe)
                self.assertFalse(result[0]['ok'])
                self.assertEqual(result[0]['missing'],['NOVA_OSS_TEST_CONFIG'])
        result=run.requirement_checks(['fixture:aliyun-ecs-oss'],
            {'NOVA_OSS_TEST_CONFIG':str(config)},self.native,probe)
        self.assertTrue(result[0]['ok'])
        self.assertEqual(result[0]['missing'],[])
        self.assertIn('native owner validates runtime facts',result[0]['basis'])
        probe.assert_not_called()

    def test_cli_direct_module_and_shell_entrypoints_start_from_another_directory(self):
        ci=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix='verification-cli-') as directory:
            commands=[([sys.executable,str(ci/'verification/cli.py'),'--help'],directory),
                      ([sys.executable,'-m','verification.cli','--help'],str(ci)),
                      (['bash',str(ci/'dev-verify.sh'),'--help'],directory)]
            for argv,cwd in commands:
                with self.subTest(argv=argv):
                    result=subprocess.run(argv,cwd=cwd,text=True,capture_output=True,timeout=15)
                    self.assertEqual(result.returncode,0,result.stderr)
                    self.assertIn('check-declarations',result.stdout)
                    self.assertEqual(result.stderr,'')

    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(prefix='verification-run-')
        self.addCleanup(self.temporary.cleanup)
        self.base=Path(self.temporary.name)
        self.repo=self.base/'repo';self.repo.mkdir()
        for argv in (['git','init','-q'],['git','-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--allow-empty','-qm','fixture']):
            subprocess.run(argv,cwd=self.repo,check=True)
        patch=mock.patch.object(run.identity_snapshot,'tools',return_value={'fixture':'toolchain'})
        patch.start();self.addCleanup(patch.stop)
        self.inputs=self.base/'prepared/inputs';self.inputs.mkdir(parents=True)
        self.metadata={'version':1,'workspace_members':['p'], 'packages':[{
            'id':'p','name':'pkg','manifest_path':str(self.repo/'Cargo.toml'),
            'metadata':{'novarocks':{'test':{'runtime-inputs':[],'env-inputs':[],'ignored':[]}}}}]}
        self.native={'repo':str(self.repo),'metadata':self.metadata,'sql':{'schema':1,'suites':[]},'system':{'schema':1,'scenarios':[]},
            'stages':{'schema':1,'stages':[]},'inputs':str(self.inputs),'receipt':{'novarocks':{'path':sys.executable,'sha256':run.identity_snapshot.digest(sys.executable)}}}
        put(self.inputs/'test-artifacts.json',{'schema':1,'binaries':{'p':{'path':sys.executable,'sha256':run.identity_snapshot.digest(sys.executable),'package-id':'p','binary-name':'pkg'}}})
        put(self.inputs/'cargo-metadata.json',self.metadata)
        put(self.inputs/'prepared.json',{'schema':1,'profile':'default','nextest_version':'fixture'})
        (self.inputs/'nextest.toml').write_text('[profile.default]\n')
        self.chosen={'rust_filterset':None,'doctest':False,'sql_suites':[],'system_scenarios':[],'stages':[]}

    def scope(self,kind,native,tier='regular',selected=True):
        unit=UnitKey(kind,native)
        return Scope(str(unit),(unit,),{'build':'fixture'},'isolated',True,selected,tier)

    def java_owner_group(self):
        module = run.java_rest_owner(Path(run.__file__).resolve().parents[3])
        names = sorted(module.EXPECTED)
        listing = {'rust-suites':{'p':{'testcases':{
            name:{'ignored':True,'filter-match':{'status':'matches'}} for name in names}}}}
        scopes = [self.scope('rust-test','p::'+name,'heavy',selected=index==0)
                  for index,name in enumerate(names)]
        ignored = {str(scope.units[0]):{'class':'heavy','input-owner':module.OWNER,
                   'requires':['fixture:iceberg-rest','fixture:minio','toolchain:java']} for scope in scopes}
        bindings = json.loads((self.inputs/'test-artifacts.json').read_text())['binaries']
        with mock.patch.object(run,'java_rest_owner',return_value=module):
            grouped = run.java_rest_group(self.repo,scopes,ignored,bindings,listing)
        return module, scopes, grouped[0], ignored, bindings, listing

    def test_java_input_owner_is_complete_group_even_for_partial_selection_or_resume(self):
        module, original, group, ignored, bindings, listing = self.java_owner_group()
        self.assertEqual(group.key,module.SCOPE)
        self.assertEqual(set(group.units),{scope.units[0] for scope in original})
        self.assertEqual(len(group.units),14)
        self.assertTrue(group.selected)
        expression = run._rust_expression(group,bindings,{'p':{'name':'pkg'}},listing)
        for name in module.EXPECTED:
            self.assertIn('test(='+name+')',expression)
        with mock.patch.object(run,'java_rest_owner',return_value=module):
            with self.assertRaisesRegex(ValueError,'complete original fourteen'):
                run.java_rest_group(self.repo,original[:-1],ignored,bindings,listing)

    def test_java_target_is_written_before_whole_native_batch_and_not_learned_from_results(self):
        module, original, group, ignored, bindings, listing = self.java_owner_group()
        attempt=self.base/'attempt';attempt.mkdir()
        self.native.update(ignored_units=ignored,regular_inventory=listing,identity_inputs=str(self.inputs))
        caller_declarations=[{'name':name,'kind':'value'}
                             for name in ('AWS_SESSION_TOKEN','AWS_REGION')]
        caller_values={'AWS_SESSION_TOKEN':'fixture-token','AWS_REGION':'fixture-region'}
        caller_inputs=run.identity_snapshot.env_inputs(caller_declarations,caller_values)
        payload={'executable_sha256':'actual-binary','env':dict(caller_inputs,
                 PATH='same-java',NOVA_FIXTURE_RUN_CONTEXT='actual-run')}
        root=attempt/'java-rest-inputs';root.mkdir()
        bound_file=self.base/'pinned-bom.json';bound_file.write_text('original BOM')
        owner=mock.Mock(root=root,environment={'actual':'owner'},
                        input_files={str(bound_file):run.identity_snapshot.digest(bound_file)})
        owned=mock.MagicMock();owned.__enter__.return_value=owner
        snapshots={unit.id:{'inputs':payload} for unit in group.units}
        def batch(repo, current, output, expression, environment, **kwargs):
            saved=json.loads((attempt/'java-rest-target.json').read_text())
            self.assertEqual(saved['phase'],'before-execution')
            self.assertEqual(saved['units'],[str(unit) for unit in group.units])
            self.assertEqual(saved['identity'],payload)
            self.assertTrue((self.base/'java-rest-target.json').is_file())
            self.assertEqual(environment,owner.environment)
            self.assertIs(kwargs['input_owner'],owner)
            self.assertTrue(kwargs['ignored'])
            return 0
        with mock.patch.object(run,'java_rest_owner',return_value=module), \
                mock.patch.object(module,'own_inputs',return_value=owned), \
                mock.patch.object(run,'native_list',return_value=listing), \
                mock.patch.object(run,'requirement_checks',return_value=[{'ok':True}]), \
                mock.patch.object(run.identity_snapshot,'rust_snapshot',return_value=snapshots), \
                mock.patch.object(run,'read_rust',return_value=([
                    Observation(unit,group.key,attempt.name,'nextest',payload,'pass',True,True,True,'ok')
                    for unit in group.units],[])), \
                mock.patch.object(run,'_rust_batch',side_effect=batch):
            self.assertEqual(run._java_rest_batch(self.repo,self.base,attempt,self.native,group,
                             bindings,{'p':{'name':'pkg'}},{}),0)
        # A later read uses the pre-execution declaration, not JUnit or a receipt.
        target={'identities':{group.key:None}}
        group.identity=dict(payload,env=dict(caller_inputs,PATH='same-java'))
        with mock.patch.object(run,'java_rest_owner',return_value=module):
            rebound=run._java_rest_target(self.base,[group],target)
            self.assertEqual(rebound['identities'][group.key],payload)
            bound_file.write_text('changed BOM')
            self.assertEqual(run._java_rest_target(self.base,[group],target),target)
            bound_file.write_text('original BOM')
            group.identity=dict(payload,env=dict(caller_inputs,PATH='different-java'))
            self.assertEqual(run._java_rest_target(self.base,[group],target),target)
            for name in ('AWS_SESSION_TOKEN','AWS_REGION'):
                for absent in (False,True):
                    with self.subTest(caller_input=name,absent=absent):
                        changed=dict(caller_values)
                        if absent:
                            changed.pop(name)
                        else:
                            changed[name]='changed-fixture-value'
                        current=run.identity_snapshot.env_inputs(caller_declarations,changed)
                        group.identity=dict(payload,env=dict(current,PATH='same-java'))
                        self.assertEqual(run._java_rest_target(self.base,[group],target),target)

    def test_java_native_membership_mismatch_refuses_before_run_admission(self):
        module, original, group, ignored, bindings, listing = self.java_owner_group()
        partial=copy.deepcopy(listing)
        partial['rust-suites']['p']['testcases'].pop(next(iter(module.EXPECTED)))
        with mock.patch.object(run,'java_rest_owner',return_value=module), \
                mock.patch.object(module,'own_inputs') as owner, \
                mock.patch.object(run,'native_list',return_value=partial):
            with self.assertRaisesRegex(ValueError,'complete owner group'):
                run._java_rest_batch(self.repo,self.base,self.base,self.native,group,
                                     bindings,{'p':{'name':'pkg'}},{})
            owner.assert_not_called()

    def test_exclusive_attempts_and_native_preparation_has_no_positional_filter(self):
        self.assertNotEqual(run.exclusive(self.base),run.exclusive(self.base))
        with mock.patch.object(run.subprocess,'run',return_value=subprocess.CompletedProcess([],0)) as command:
            run.prepare(self.repo,self.base)
        argv=command.call_args[0][0]
        self.assertNotIn('list',argv)

    def test_native_enumeration_removes_inherited_names_partitions_and_filters(self):
        put(self.inputs/'selected-tests.command.json',{'cwd':str(self.repo),'argv':['native-nextest','nextest','list','--cargo-metadata','meta','--message-format','json','narrow-name','--partition','hash:1/3','-E','old']})
        with mock.patch.object(run.subprocess,'check_output',return_value='{"rust-suites":{}}') as command:
            run.native_list(self.inputs,'rdeps(=pkg)','all')
        argv=command.call_args[0][0]
        self.assertNotIn('narrow-name',argv);self.assertNotIn('--partition',argv);self.assertNotIn('old',argv)
        self.assertEqual(argv[-4:],['-E','rdeps(=pkg)','--run-ignored','all'])
        self.assertEqual(command.call_args[1]['cwd'],str(self.repo))

    def test_native_ignored_rules_add_heavy_and_remove_helpers(self):
        declaration=self.metadata['packages'][0]['metadata']['novarocks']['test']
        declaration['ignored']=[{'filter':'test(=heavy)','class':'heavy','requires':['binary:novarocks'],'reason':'server'},
                                {'filter':'test(=helper)','class':'helper','reason':'nested helper'},
                                {'filter':'test(=manual)','class':'manual','reason':'recorder'}]
        put(self.inputs/'cargo-metadata.json',self.metadata)
        listing={'rust-suites':{'p':{'testcases':{name:{'ignored':name!='regular','filter-match':{'status':'matches'}} for name in ('regular','heavy','helper','manual')}}}}
        def native(inputs,expression=None,ignored=None):
            value=copy.deepcopy(listing)
            if expression and expression!='rdeps(=pkg)':
                name=next(name for name in ('heavy','helper','manual') if 'test(='+name+')' in expression)
                for key,case in value['rust-suites']['p']['testcases'].items():case['filter-match']['status']='matches' if key==name else 'mismatch'
            return value
        chosen=dict(self.chosen,rust_filterset='rdeps(=pkg)',doctest=True)
        with mock.patch.object(run,'native_list',side_effect=native):scopes=run.scopes(self.repo,self.native,chosen)
        by_key={scope.key:scope for scope in scopes}
        self.assertIn('rust-test:p::regular',by_key);self.assertIn('rust-test:p::heavy',by_key)
        self.assertNotIn('rust-test:p::helper',by_key);self.assertNotIn('rust-test:p::manual',by_key)
        self.assertEqual(by_key['rust-test:p::heavy'].tier,'heavy')
        self.assertTrue(by_key['rust-test:p::heavy'].selected)
        self.assertEqual(chosen['resolved_heavy_ignored'][0]['unit'],'rust-test:p::heavy')

    def test_resume_only_executes_unmet_regular_scopes_and_retains_inventory(self):
        regular=self.scope('rust-test','p::regular');satisfied=self.scope('rust-test','p::done')
        unselected=self.scope('stage','unaffected',selected=False)
        with mock.patch.object(run,'_rust_batch',return_value=0) as batch:
            value,path,attempt=run.execute(self.repo,self.base/'verification',self.native,self.chosen,[regular,satisfied,unselected],{regular.key})
        expression=batch.call_args[0][3]
        self.assertIn('test(=regular)',expression);self.assertNotIn('test(=done)',expression)
        selection=json.loads((attempt/'selection.json').read_text())
        self.assertEqual(len(selection['scopes']),3);self.assertEqual(selection['executed_scope_keys'],[regular.key])
        self.assertEqual(selection['unselected_obligations'][0]['backstop'],'admission verification')
        self.assertTrue((attempt/'identity/source-start.json').is_file());self.assertTrue((attempt/'identity/source-end.json').is_file())
        self.assertFalse(value['layer_ok'],'a mocked command exit without native evidence is never positive coverage')

    def test_heavy_uses_ignored_only_and_missing_prerequisite_remains_not_run(self):
        heavy=self.scope('rust-test','p::heavy',tier='heavy')
        self.native['ignored_units']={heavy.key:{'class':'heavy','requires':['binary:novarocks']}}
        with mock.patch.object(run,'_rust_batch',return_value=0) as batch:
            value,_,attempt=run.execute(self.repo,self.base/'available',self.native,self.chosen,[heavy])
        self.assertTrue(batch.call_args[1]['ignored']);self.assertIn('test(=heavy)',batch.call_args[0][3])
        self.assertTrue(list(attempt.glob('prerequisites/*.json')));self.assertFalse(value['layer_ok'])
        self.native['ignored_units'][heavy.key]['requires']=['unsupported:external']
        with mock.patch.object(run,'_rust_batch',return_value=0) as batch:
            value,_,attempt=run.execute(self.repo,self.base/'missing',self.native,self.chosen,[heavy])
        batch.assert_not_called();self.assertFalse(value['layer_ok'])
        self.assertTrue(json.loads((attempt/'execution.json').read_text())['errors'])

    def test_java_owner_cleanup_retains_original_native_material_failures(self):
        owner_ok={'command_returned':True,'cleanup':{'summary':'ok'}}
        cases=[
            ('missing-owner','violation',None,'violation'),
            ('unfinished-owner','violation',{'command_returned':False,'cleanup':{'summary':'unknown'}},'violation'),
            ('native-unknown','unknown',owner_ok,'unknown'),
            ('native-violation','violation',owner_ok,'violation'),
            ('owner-violation','ok',{'command_returned':True,'cleanup':{'summary':'violation'}},'violation'),
            ('both-ok','ok',owner_ok,'ok'),
        ]
        for label,native_cleanup,owner,expected in cases:
            with self.subTest(case=label):
                root=self.base/label
                batch=root/'rust-heavy-java-rest'
                identity=rust_batch(batch)
                put(batch/'identity/doctest.json',{'schema':1,'domain':'doctest','scopes':{}})
                scope=Scope('rust-test:p::regular',(UnitKey('rust-test','p::regular'),),identity,'isolated')
                if native_cleanup == 'violation':
                    put(batch/'rust-run/cleanup-summary.json',
                        {'schema':1,'complete':True,'ok':False,'records':[],
                         'errors':['original native cleanup failure']})
                elif native_cleanup == 'unknown':
                    put(batch/'rust-run/cleanup-summary.json',
                        {'schema':1,'complete':False,'ok':False,'records':[],'errors':[]})
                native,errors=run.read_rust(batch,root.name,[scope])
                self.assertEqual(errors,[])
                native=[item for item in native if item.unit == scope.units[0]]
                self.assertEqual(len(native),1)
                self.assertEqual(native[0].cleanup,native_cleanup)
                if owner is not None:
                    put(root/'java-rest-inputs/owner-result.json',owner)
                observed=[item for item in run.owned_attempt(root,[scope]).observations
                          if item.unit == scope.units[0]]
                self.assertEqual(len(observed),1)
                self.assertEqual(observed[0].cleanup,expected)
                self.assertEqual(observed[0].detail['native_cleanup'],native_cleanup)
                owner_status=observed[0].detail['java_rest_owner_cleanup']
                self.assertEqual(owner_status['summary'],owner['cleanup']['summary'] if owner else 'unknown')
                self.assertEqual(owner_status['command_returned'],owner['command_returned'] if owner else None)
                self.assertEqual(Path(observed[0].detail['java_rest_owner_result']),
                                 (root/'java-rest-inputs/owner-result.json').resolve())

    def test_workspace_stage_runs_full_native_command_and_imports_its_batch(self):
        stage=self.scope('stage','workspace nextest and doctests')
        regular=self.scope('rust-test','p::regular')
        item={'name':'workspace nextest and doctests','tier':'admission'}
        chosen=dict(self.chosen,stages=[item])
        def invoked(root,repo,kind,name,units,argv,spec,materials,environment):
            self.assertIn('--run-stage',argv)
            self.assertEqual(argv[argv.index('--run-stage')+1],item['name'])
            self.assertIn('execution_index',materials)
            rust_batch(Path(materials['execution_index']).parent)
            put(Path(root)/'invocations/0001-stage.json',{'schema':1,'kind':'stage','name':name,'units':[],'materials':materials,
                'inputs_start':{},'inputs_end':{},'inputs_stable':True,'exit':{'code':0},'ended_at':'now','cleanup':{'source':'none','summary':'ok','refs':[]}})
            return 0
        with mock.patch.object(run,'invoke',side_effect=invoked),mock.patch.object(run,'stage_spec',return_value={'env_inputs':[]}),mock.patch.object(run,'_rust_batch') as narrowed:
            _,_,attempt=run.execute(self.repo,self.base/'workspace',self.native,chosen,[stage,regular])
        narrowed.assert_not_called()
        native=run.owned_attempt(attempt,[regular])
        self.assertTrue(any(obs.unit==regular.units[0] and obs.result=='pass' for obs in native.observations))

    def test_sql_dataset_launch_uses_native_prepared_fact_without_changing_regular_or_scope(self):
        self.native['receipt']['novarocks-sql-test']={'path':sys.executable}
        prepared={'dataset':'ssb','scale':'1'}
        for label,fact,heavy in [('regular',None,None),('cost-heavy',None,{'reason':'cost'}),
                                 ('dataset',prepared,{'reason':'dataset'})]:
            declaration={'name':'fixture-suite','prepared_dataset':fact,'heavy':heavy,
                         'reuse':{'scope':'suite','env_inputs':[]}}
            selected=[self.scope('sql-case','fixture-suite/first'),
                      self.scope('sql-case','fixture-suite/second')]
            with self.subTest(label=label),mock.patch.dict(os.environ,{'NOVAROCKS_SQL_TEST_CONFIG':'fixture.toml'}),mock.patch.object(run,'invoke',return_value=0) as invoked:
                value,_,_=run.execute(self.repo,self.base/label,self.native,
                    dict(self.chosen,sql_suites=[declaration]),selected)
            invoked.assert_called_once()
            argv=invoked.call_args[0][5]
            if fact is not None:
                self.assertEqual(argv[-2:],['--cluster-launch-profile','performance'])
                self.assertEqual(argv.count('--cluster-launch-profile'),1)
            else:
                self.assertNotIn('--cluster-launch-profile',argv)
            self.assertEqual(argv[argv.index('--suite')+1],'fixture-suite')
            self.assertEqual(argv[argv.index('--cluster-mode')+1],'cross-process')
            self.assertEqual(argv[argv.index('--cluster-size')+1],'3')
            self.assertNotIn('--only',argv)
            self.assertNotIn('--query-timeout',argv)
            self.assertEqual(invoked.call_args[0][4],[unit for scope in selected for unit in scope.units])
            self.assertFalse(value['layer_ok'],'mocked command exit is not native positive evidence')

    def test_doctest_only_gap_does_not_secretly_rerun_satisfied_tests(self):
        doc=self.scope('rust-doctest','workspace')
        with mock.patch.object(run,'_rust_batch') as batch:
            result,_,attempt=run.execute(self.repo,self.base/'docs',self.native,self.chosen,[doc])
        batch.assert_not_called();self.assertFalse(result['layer_ok'])
        gap=json.loads((attempt/'doctest-only-gap.json').read_text())
        self.assertEqual(gap['status'],'not_run')
        self.assertIsNone(json.loads((attempt/'selection.json').read_text())['resume_exception'])

    def test_regular_filter_compresses_full_native_binary_and_checks_membership(self):
        listing={'rust-suites':{'p':{'testcases':{name:{'ignored':False,'filter-match':{'status':'matches'}}
                    for name in ('regular','done')}}}}
        listing['rust-suites']['p']['testcases']['helper']={'ignored':True,'filter-match':{'status':'mismatch'}}
        bindings=json.loads((self.inputs/'test-artifacts.json').read_text())['binaries']
        metadata={'p':self.metadata['packages'][0]}
        all_scopes=[self.scope('rust-test','p::'+name) for name in ('regular','done')]
        with mock.patch.object(run,'native_list',return_value=listing):
            expression,actual=run._regular_expression(all_scopes,bindings,metadata,self.inputs,listing)
        self.assertEqual(expression,'(package(=pkg) & binary(=pkg))')
        self.assertEqual(set(actual),{scope.key for scope in all_scopes})
        narrowed=json.loads(json.dumps(listing))
        narrowed['rust-suites']['p']['testcases']['done']['filter-match']['status']='mismatch'
        with mock.patch.object(run,'native_list',return_value=narrowed):
            expression,_=run._regular_expression(all_scopes[:1],bindings,metadata,self.inputs,listing)
        self.assertIn('test(=regular)',expression);self.assertNotIn('test(=done)',expression)
        with mock.patch.object(run,'native_list',return_value=listing):
            with self.assertRaisesRegex(ValueError,'changes requested'):
                run._regular_expression(all_scopes[:1],bindings,metadata,self.inputs,listing)

    def test_native_lib_module_prefix_is_not_guessed_as_an_integration_binary(self):
        bindings={'p':{'package-id':'p','binary-name':'lib'},'p::exec':{'package-id':'p','binary-name':'exec'}}
        listing={'rust-suites':{'p':{'testcases':{'exec::module::check':{'ignored':False}}},
                               'p::exec':{'testcases':{'different_check':{'ignored':False}}}}}
        value=self.scope('rust-test','p::exec::module::check')
        expression=run._rust_expression(value,bindings,{'p':self.metadata['packages'][0]},listing)
        self.assertIn('binary(=lib)',expression)
        self.assertIn('test(=exec::module::check)',expression)
        with self.assertRaisesRegex(ValueError,'no unique'):
            run._rust_expression(value,bindings,{'p':self.metadata['packages'][0]})
        listing['rust-suites']['p::exec']['testcases']['module::check']={'ignored':False}
        with self.assertRaisesRegex(ValueError,'no unique'):
            run._rust_expression(value,bindings,{'p':self.metadata['packages'][0]},listing)

    def test_repeated_or_heavy_unit_requires_unique_native_selector(self):
        bindings={key:{'package-id':'p','binary-name':'same'} for key in ('p::one','p::two')}
        listing={'rust-suites':{key:{'testcases':{'check':{'ignored':False}}} for key in bindings}}
        value=self.scope('rust-test','p::one::check')
        self.assertEqual(run._rust_binding(value,bindings,listing),('p::one','check'))
        with self.assertRaisesRegex(ValueError,'selector is not unique'):
            run._rust_expression(value,bindings,{'p':self.metadata['packages'][0]},listing)

    def test_large_complete_regular_binary_filter_has_bounded_argv(self):
        names=['parser::session::case_%05d'%n for n in range(13228)]
        listing={'rust-suites':{'p':{'testcases':{name:{'ignored':False,'filter-match':{'status':'matches'}} for name in names}}}}
        values=[self.scope('rust-test','p::'+name) for name in names]
        bindings=json.loads((self.inputs/'test-artifacts.json').read_text())['binaries']
        with mock.patch.object(run,'native_list',return_value=listing):
            expression,actual=run._regular_expression(values,bindings,{'p':self.metadata['packages'][0]},self.inputs,listing)
        self.assertLess(len(expression.encode()),128)
        self.assertEqual(len(actual),13228)

    def test_large_partial_regular_binaries_use_bounded_exact_complements(self):
        bindings = {}
        metadata = {}
        listing = {'rust-suites': {}}
        values = []
        wanted = set()
        clauses = []
        for binary, size, omitted in (('p0', 2277, 1), ('p1', 1874, 1),
                                      ('p2', 2718, 2), ('p3', 3293, 6)):
            names = ['parser::session::case_%05d' % n for n in range(size)]
            excluded = set(names[:omitted])
            bindings[binary] = {'package-id': binary, 'binary-name': binary}
            metadata[binary] = {'name': binary}
            listing['rust-suites'][binary] = {'testcases': {
                name: {'ignored': False, 'filter-match': {'status': 'matches'}} for name in names}}
            listing['rust-suites'][binary]['testcases']['helper'] = {
                'ignored': True, 'filter-match': {'status': 'mismatch'}}
            for name in names[omitted:]:
                value = self.scope('rust-test', binary+'::'+name)
                values.append(value)
                wanted.add(value.key)
            clauses.append('(package(={0}) & binary(={0}) & not ({1}))'.format(
                binary, ' | '.join('test(={})'.format(name) for name in sorted(excluded))))
        # Model the native response from the fixed omitted fixtures, independently
        # of the expression under test. The author must submit these exact clauses.
        selected = copy.deepcopy(listing)
        for binary, suite in selected['rust-suites'].items():
            for name, case in suite['testcases'].items():
                case['filter-match']['status'] = (
                    'matches' if 'rust-test:'+binary+'::'+name in wanted else 'mismatch')
        with mock.patch.object(run, 'native_list', return_value=selected) as native:
            expression, actual = run._regular_expression(values, bindings, metadata, self.inputs, listing)
        self.assertEqual(expression, ' | '.join(clauses))
        self.assertEqual(native.call_args.args, (self.inputs, expression))
        self.assertLess(len(expression.encode()), 4096)
        self.assertEqual(set(actual), wanted)
        self.assertEqual(len(actual), 10152)
        # A native result which also selects even one omitted test must still fail.
        selected['rust-suites']['p0']['testcases']['parser::session::case_00000']['filter-match']['status'] = 'matches'
        with mock.patch.object(run, 'native_list', return_value=selected):
            with self.assertRaisesRegex(ValueError, 'changes requested'):
                run._regular_expression(values, bindings, metadata, self.inputs, listing)

    def test_regular_binary_compression_keeps_prefix_ambiguity_guard(self):
        bindings = {'p': {'package-id': 'p', 'binary-name': 'lib'},
                    'p::exec': {'package-id': 'p', 'binary-name': 'exec'}}
        listing = {'rust-suites': {
            'p': {'testcases': {'exec::module::check': {'ignored': False}}},
            'p::exec': {'testcases': {'different_check': {'ignored': False}}}}}
        value = self.scope('rust-test', 'p::exec::module::check')
        selected = copy.deepcopy(listing)
        selected['rust-suites']['p']['testcases']['exec::module::check']['filter-match'] = {'status': 'matches'}
        selected['rust-suites']['p::exec']['testcases']['different_check']['filter-match'] = {'status': 'mismatch'}
        with mock.patch.object(run, 'native_list', return_value=selected):
            expression, actual = run._regular_expression([value], bindings,
                {'p': self.metadata['packages'][0]}, self.inputs, listing)
        self.assertEqual(expression, '(package(=pkg) & binary(=lib))')
        self.assertEqual(actual, [value.key])
        listing['rust-suites']['p::exec']['testcases']['module::check'] = {'ignored': False}
        with mock.patch.object(run, 'native_list') as native:
            with self.assertRaisesRegex(ValueError, 'no unique'):
                run._regular_expression([value], bindings,
                    {'p': self.metadata['packages'][0]}, self.inputs, listing)
        native.assert_not_called()

    def test_native_inventory_keeps_deferred_aggregate_scope_before_results(self):
        item={'name':'workspace nextest and doctests','tier':'admission','domain':['**']}
        self.native['stages']['stages']=[item]
        chosen=dict(self.chosen,stages=[item])
        listing={'rust-suites':{'p':{'testcases':{'regular':{'ignored':False,'filter-match':{'status':'matches'}}}}}}
        with mock.patch.object(run,'native_list',return_value=listing),mock.patch.object(run,'stage_spec',return_value={}),mock.patch.object(run,'stage_argv',return_value=['native-stage']),mock.patch.object(run.invocation_record,'snapshot',return_value={'stage':'identity'}):
            values=run.scopes(self.repo,self.native,chosen)
        stage=next(scope for scope in values if scope.key=='stage:'+item['name'])
        self.assertFalse(stage.selected)
        self.assertEqual(stage.deferred['backstop'],'admission')
        with mock.patch.object(run,'invoke') as invoked,mock.patch.object(run,'_rust_batch') as batch:
            run.execute(self.repo,self.base/'deferred',self.native,chosen,values)
        invoked.assert_not_called()
        batch.assert_not_called()

    def test_aggregate_projection_keeps_admission_full_and_negative_observations(self):
        from verification.scope import required_scopes
        unit=UnitKey('stage','workspace nextest and doctests')
        deferred=Scope(str(unit),(unit,),{'build':'fixture'},'immutable',True,False,
                       deferred=dict(run.RUST_AGGREGATE_DEFERRED))
        self.assertEqual(required_scopes('dev',[deferred]),[])
        self.assertEqual(required_scopes('admission',[deferred]),[deferred])
        self.assertEqual(required_scopes('full',[deferred]),[deferred])
        for result,cleanup in [('fail','ok'),('unknown','unknown')]:
            with self.subTest(result=result,cleanup=cleanup):
                obs=Observation(unit,deferred.key,'old','stage',deferred.identity,result,
                                True,True,True,cleanup,ref='original-stage-record')
                value,_=run.compose_root(self.base/('audit-'+result),[deferred],
                    {'identities':{deferred.key:deferred.identity}},extra_attempts=[Attempt('old',[obs])])
                self.assertFalse(value['layer_ok'])
                self.assertEqual(value['scopes'],[])
                self.assertEqual(value['deferred_scopes'][0]['observations'][0]['ref'],'original-stage-record')
                self.assertEqual(value['unselected_obligations'][0]['backstop'],'admission')
                if cleanup=='unknown':self.assertTrue(value['deferred_scopes'][0]['cleanup_obligations'])
                else:self.assertEqual(value['deferred_scopes'][0]['verdict'],'failed')

    def doc_resume_fixture(self, label):
        root=self.base/label
        old=run.exclusive(root)
        put(old/'selection.json',{'schema':1})
        identity=rust_batch(old/'rust')
        regular=self.scope('rust-test','p::regular');regular.identity=identity
        doc=self.scope('rust-doctest','workspace');doc.identity={'domain':'workspace-docs'}
        return root,old,regular,doc

    def test_doctest_only_exception_is_recorded_before_one_native_batch_and_uses_reader(self):
        root,old,regular,doc=self.doc_resume_fixture('doc-exception')
        before={str(p):p.read_bytes() for p in old.rglob('*') if p.is_file()}
        def batch(repo,attempt,output,expression,environment):
            saved=json.loads((attempt/'selection.json').read_text())
            exception=saved['resume_exception']
            self.assertEqual(exception['repeated_satisfied_scopes'],[regular.key])
            self.assertEqual(exception['pending_scopes'],[doc.key])
            self.assertEqual(exception['actual_filterset'],expression)
            self.assertEqual(exception['batch_output'],str(output))
            self.assertTrue(Path(exception['prior_composition']).is_file())
            self.assertIn('test(=regular)',expression)
            rust_batch(output,identity=regular.identity)
            index=json.loads((output/'execution-index.json').read_text())
            index['stages']['doctest']={'started':True,'returncode':0,'errors':[]}
            index['materials']['doctest_cleanup']='doctest-run/cleanup-summary.json'
            put(output/'execution-index.json',index)
            put(output/'doctest-run/cleanup-summary.json',{'schema':1,'registration_closed':True,'complete':True,'ok':True,'records':[],'errors':[]})
            put(output/'identity/doctest.json',{'schema':1,'domain':'doctest','scopes':{'workspace':{
                'declared':True,'inputs_start':doc.identity,'inputs_end':doc.identity,'inputs_stable':True}}})
            return 0
        with mock.patch.object(run,'_rust_batch',side_effect=batch) as executor:
            value,_,attempt=run.execute(self.repo,root,self.native,self.chosen,[regular,doc],{doc.key})
        self.assertEqual(executor.call_count,1)
        doc_result=next(item for item in value['scopes'] if item['key']==doc.key)
        self.assertEqual(doc_result['verdict'],'satisfied')
        self.assertTrue(doc_result['cleanup_obligations'],'an old unknown cleanup is not discharged by new PASS')
        self.assertFalse(value['layer_ok'])
        self.assertTrue(value['resume_exceptions'])
        for path,raw in before.items():self.assertEqual(Path(path).read_bytes(),raw)

    def test_doctest_exception_new_failure_is_inconsistent_and_not_retried(self):
        root,old,regular,doc=self.doc_resume_fixture('doc-failure')
        def batch(repo,attempt,output,expression,environment):
            rust_batch(output,identity=regular.identity)
            junit=output/'nextest/default/junit.xml'
            junit.write_text('<testsuites><testsuite name="p"><testcase name="regular" classname="p"><failure message="new failure"/></testcase></testsuite></testsuites>')
            index=json.loads((output/'execution-index.json').read_text());index['stages']['nextest']['returncode']=100
            put(output/'execution-index.json',index)
            return 100
        with mock.patch.object(run,'_rust_batch',side_effect=batch) as executor:
            value,_,_=run.execute(self.repo,root,self.native,self.chosen,[regular,doc],{doc.key})
        self.assertEqual(executor.call_count,1)
        row=next(item for item in value['scopes'] if item['key']==regular.key)
        self.assertEqual(row['verdict'],'inconsistent')
        self.assertEqual({obs['result'] for obs in row['observations']},{'pass','fail'})
        self.assertFalse(value['layer_ok'])

    def test_doc_exception_refuses_narrow_pending_when_other_target_is_unmet(self):
        root,old,regular,doc=self.doc_resume_fixture('doc-other')
        other=self.scope('sql-case','suite/missing')
        with mock.patch.object(run,'_rust_batch') as executor:
            value,_,attempt=run.execute(self.repo,root,self.native,self.chosen,[regular,doc,other],{doc.key})
        executor.assert_not_called()
        self.assertIsNone(json.loads((attempt/'selection.json').read_text())['resume_exception'])
        self.assertFalse(value['layer_ok'])

    def test_doc_exception_requires_matching_satisfied_regular_scope(self):
        for mutation in ('identity','heavy','unselected','cleanup','counterexample','binding'):
            root,old,regular,doc=self.doc_resume_fixture('refuse-'+mutation)
            if mutation=='identity':regular.identity=dict(regular.identity,executable_sha256='changed')
            elif mutation=='heavy':regular.tier='heavy'
            elif mutation=='unselected':regular.selected=False
            elif mutation=='cleanup':
                (old/'rust/rust-run/cleanup-summary.json').unlink()
            elif mutation=='binding':
                self.native['regular_inventory']={'rust-suites':{}}
            else:
                junit=old/'rust/nextest/default/junit.xml'
                junit.write_text('<testsuites><testsuite name="p"><testcase name="regular" classname="p"><failure/></testcase></testsuite></testsuites>')
            with self.subTest(mutation=mutation),mock.patch.object(run,'_rust_batch') as executor:
                _,_,attempt=run.execute(self.repo,root,self.native,self.chosen,[regular,doc],{doc.key})
                executor.assert_not_called()
                self.assertIsNone(json.loads((attempt/'selection.json').read_text())['resume_exception'])

    def test_heavy_native_binding_refusal_is_recorded_and_composed(self):
        heavy=self.scope('rust-test','missing::heavy',tier='heavy')
        self.native['ignored_units']={heavy.key:{'class':'heavy','requires':['binary:novarocks']}}
        with mock.patch.object(run,'_rust_batch') as batch:
            value,_,attempt=run.execute(self.repo,self.base/'heavy-refusal',self.native,self.chosen,[heavy])
        batch.assert_not_called()
        self.assertFalse(value['layer_ok'])
        errors=json.loads((attempt/'execution.json').read_text())['errors']
        self.assertEqual(errors[0]['kind'],'rust-heavy')
        self.assertIn('no unique binary/test binding',errors[0]['error'])
        self.assertTrue((attempt/'result.json').is_file())

    def test_spawn_failure_has_no_fabricated_native_exit(self):
        attempt=run.exclusive(self.base/'spawn');put(attempt/'selection.json',{'schema':1})
        code=run.invoke(attempt,self.repo,'stage','missing',[UnitKey('stage','missing')],['/missing/native-executor'],{'inputs':[]},{},{})
        self.assertEqual(code,127)
        final=next(path for path in (attempt/'invocations').glob('*.json') if not path.name.endswith('.started.json'))
        value=json.loads(final.read_text());self.assertIsNone(value['exit']['code']);self.assertFalse(value['execution']['started'])
        observation=run.owned_attempt(attempt).observations[0]
        self.assertEqual(observation.result,'not_run');self.assertFalse(observation.terminated)

    def system_attempt(self,extra_missing=False):
        attempt=run.exclusive(self.base/'cleanup');put(attempt/'selection.json',{'schema':1})
        registration=put(attempt/'resource.json',{'schema':1,'owner':'isolated-iceberg-rest','id':'unique','label':'compose-fixture','state':'registered'})
        refs=[str(registration)]
        if extra_missing:refs.append(str(attempt/'missing.json'))
        resources=[{'owner':'isolated-iceberg-rest','id':'unique','label':'compose-fixture','registration':ref,'state':'unsettled'} for ref in refs]
        teardown=put(attempt/'scenario-teardown.json',{'schema':1,'fixture':{'launch_config':'failed','teardown':{'status':'ok'},'resources':resources},
            'cluster':{'launch':'not_attempted','shutdown':'not_applicable','partial_cleanup':None}})
        evidence=put(attempt/'scenario-evidence.json',{'schema_version':5,'scenario':'scene','outcome':'failed'})
        put(attempt/'invocations/0001-scene.json',{'schema':1,'kind':'system-scenario','units':['system-scenario:scene'],'seq':1,
            'inputs_start':{},'inputs_end':{},'inputs_stable':True,'exit':{'code':1},'ended_at':'now',
            'materials':{'evidence':str(evidence),'teardown':str(teardown)},'cleanup':{'source':'producer','summary':'unknown','refs':[str(teardown)]+refs}})
        return attempt

    def sql_cleanup_attempt(self,failure=None,suite_cleanup='ok',unrelated=False,replay=False):
        attempt=run.exclusive(self.base/'sql-cleanup');put(attempt/'selection.json',{'schema':1})
        records=[
            {'kind':'run','schema':1,'mode':'verify','verification':{'verify_enabled':True},
             'platform':{'system':'fixture','machine':'fixture','release':'fixture'}},
            {'kind':'selection','suites':[{'suite':'s','inputs_sha256':'inputs','verify_enabled':True,
             'declaration':{'scope':'suite','initial_state':'isolated','env_inputs':[]},'cases':[{'case':'a'}]}]},
            {'kind':'case','suite':'s','case':'a','lifecycle_id':'actual','status':'fail','failure':failure,
             'oracle':{'required':[{'step':1,'kind':'result_set'}],'executed':[]}},
            {'kind':'suite','suite':'s','lifecycle_id':'actual','inputs_end_sha256':'inputs',
             'inputs_stable':True,'executed_cases':['a'],'cleanup':{'status':suite_cleanup}},
            {'kind':'server','cleanup':{'status':'ok'}},
            {'kind':'fixture','shutdown':{'status':'ok'},'resources':[]},
            {'kind':'end','completed':True,'exit_code':1}]
        if unrelated:
            records[4:4]=[
                {'kind':'case','suite':'other','case':'a','lifecycle_id':'actual','failure':{'cleanup':True}},
                {'kind':'case','suite':'s','case':'b','lifecycle_id':'actual','failure':{'cleanup':True}},
                {'kind':'suite','suite':'s','lifecycle_id':'other','cleanup':{'status':'failed'}}]
        if replay:
            records[4:4]=[
                {'kind':'case','suite':'s','case':'a','lifecycle_id':'replay','status':'pass',
                 'oracle':{'required':[{'step':1,'kind':'result_set'}],
                           'executed':[{'step':1,'kind':'result_set','outcome':'pass'}]}},
                {'kind':'suite','suite':'s','lifecycle_id':'replay','inputs_end_sha256':'inputs',
                 'inputs_stable':True,'executed_cases':['a'],'cleanup':{'status':'ok'}}]
        path=attempt/'sql/results.jsonl';path.parent.mkdir()
        path.write_text(''.join(json.dumps(row)+'\n' for row in records))
        put(attempt/'invocations/0001-s.json',{'schema':1,'kind':'sql-suite','seq':1,
            'units':['sql-case:s/a'],'materials':{'sql_results':str(path)},
            'inputs_start':{},'inputs_end':{},'inputs_stable':True,'ended_at':'now','exit':{'code':1}})
        return attempt,path

    def test_sql_cleanup_confirmation_cannot_clear_case_or_suite_failure_with_other_owners_ok(self):
        primary={'class':'error_code','error_code':'NR-KNOWN','statement_index':1}
        for failure,closure,domain in (
            ({'class':'error','cleanup':True},'ok','case'),
            ({'class':'error','primary_failure':dict(primary,cleanup=True)},'ok','case'),
            ({'class':'error','primary_failure':primary,
              'additional_failures':[{'class':'error','cleanup':True}]},'ok','case'),
            (primary,'failed','suite')):
            with self.subTest(failure=failure,closure=closure):
                attempt,path=self.sql_cleanup_attempt(failure,closure,unrelated=True,replay=True)
                before=path.read_bytes();calls=[]
                def absent(argv,**kwargs):
                    calls.append(argv);return subprocess.CompletedProcess(argv,0,'','')
                value,_=run.cleanup_confirmation(attempt,run_command=absent)
                self.assertEqual(len(value['confirmations']),1)
                confirmation=value['confirmations'][0]
                self.assertFalse(confirmation['complete'])
                self.assertEqual(confirmation['summary'],'unknown')
                self.assertTrue(any(c.get('domain')==domain and not c['ok']
                                    and 'independent' in c.get('reason','') for c in confirmation['checks']))
                self.assertEqual(path.read_bytes(),before)
                self.assertEqual(calls,[])

    def test_sql_generic_error_has_no_invented_cleanup_or_unrelated_lifecycle_failure(self):
        failure={'class':'error','primary_failure':{'class':'error_code','error_code':'NR-KNOWN'},
                 'additional_failures':[{'class':'error','error':'cleanup in diagnostic text only'}]}
        attempt,path=self.sql_cleanup_attempt(failure,unrelated=True)
        before=path.read_bytes();observations=run.owned_attempt(attempt).observations
        self.assertEqual(len(observations),1)
        self.assertEqual(observations[0].cleanup,'ok')
        value,_=run.cleanup_confirmation(attempt)
        self.assertEqual(value['confirmations'],[])
        checks=run._producer_cleanup_checks(observations[0],attempt,
              lambda argv,**kwargs:subprocess.CompletedProcess(argv,0,'',''),{})
        self.assertTrue(checks)
        self.assertTrue(all(c['ok'] for c in checks))
        self.assertEqual(path.read_bytes(),before)

    def test_cleanup_confirmation_queries_only_exact_resources_and_loads_repeated_attempts(self):
        attempt=self.system_attempt()
        calls=[]
        def present(argv,**kwargs):calls.append(argv);return subprocess.CompletedProcess(argv,0,'still-present\n','')
        first,_=run.cleanup_confirmation(attempt,run_command=present)
        self.assertFalse(first['confirmations'][0]['complete'])
        def absent(argv,**kwargs):calls.append(argv);return subprocess.CompletedProcess(argv,0,'','')
        second,path=run.cleanup_confirmation(attempt,run_command=absent)
        self.assertTrue(second['confirmations'][0]['complete']);self.assertNotEqual(path.name,'cleanup-confirmation.json')
        self.assertEqual(len(run.confirmations(attempt)),2)
        self.assertEqual(len(calls),6)
        self.assertTrue(all('label=com.docker.compose.project=compose-fixture' in argv for argv in calls))
        self.assertFalse(any(any(word in argv for word in ('rm','down','stop','kill')) for argv in calls))

    def test_missing_required_cleanup_ref_cannot_be_hidden_by_other_successes(self):
        attempt=self.system_attempt(extra_missing=True)
        value,_=run.cleanup_confirmation(attempt,run_command=lambda argv,**kwargs:subprocess.CompletedProcess(argv,0,'',''))
        self.assertFalse(value['confirmations'][0]['complete'])
        self.assertTrue(any(not check['ok'] for check in value['confirmations'][0]['checks']))
        with self.assertRaises(ValueError):run.cleanup_confirmation(self.base/'does-not-exist')

    def test_direct_native_root_and_admission_rehearsal_keep_local_authority(self):
        root=self.base/'native-root';identity=rust_batch(root)
        scope=Scope('rust-test:p::regular',(UnitKey('rust-test','p::regular'),),identity,'isolated')
        value,_=run.compose_root(root,[scope],{'identities':{scope.key:identity}},current=root.name)
        self.assertTrue(value['layer_ok']);self.assertEqual(len(value['attempts']),1)
        admission,_=run.compose_root(root,[scope],{'identities':{scope.key:identity}},layer='admission',current=root.name)
        self.assertFalse(admission['layer_ok']);self.assertFalse(admission['attempts'][0]['formal'])
        self.assertFalse(admission['local_rehearsal']['qualifies_for_admission'])
        self.assertTrue(admission['local_rehearsal']['scopes'][0]['observations'])

    def test_local_rehearsal_labels_quarantine_separately_from_raw_membership(self):
        root=self.base/'quarantined-root';identity=rust_batch(root)
        unit=UnitKey('rust-test','p::regular')
        scope=Scope('rust-test:p::regular',(unit,),identity,'isolated')
        disposition=run.Dispositions({'schema':1,'known_failures':[], 'quarantine':[{
            'unit':str(unit),'reason':'unstable','owner':'tests','exit_condition':'fix',
            'review_by':'2099-01-01'}]})
        with mock.patch.object(run.Dispositions,'read',return_value=disposition):
            value,_=run.compose_root(root,[scope],{'identities':{scope.key:identity}},
                                     layer='admission',current=root.name)
        rehearsal=value['local_rehearsal']
        self.assertIn('before admission quarantine exclusions',rehearsal['membership'])
        self.assertIn(str(unit),rehearsal['admission_membership_exclusions'])
        self.assertEqual(rehearsal['scopes'][0]['gated_units'],[str(unit)])
        self.assertFalse(rehearsal['qualifies_for_admission'])

    def post_compat_main_owner_fixture(self, program, exit_code=0):
        library=self.repo/'tools/ci/lib';library.mkdir(parents=True,exist_ok=True)
        (library/'build_capabilities.sh').write_text("""ci_prepare_main() {
  printf '%s\\n' "$1" >"$REFRESH_CALLS"
  [ "$REFRESH_OWNER_EXIT" -eq 0 ] || return "$REFRESH_OWNER_EXIT"
  "$REFRESH_PYTHON" - "$1.cargo.jsonl" "$REFRESH_PROGRAM" <<'CARGO'
import json,sys
names=('novarocks','novarocks-sql-test','novarocks-system-tests','novarocks-error-manifest')
with open(sys.argv[1],'w') as stream:
    for name in names:
        stream.write(json.dumps({'reason':'compiler-artifact','executable':sys.argv[2],
            'target':{'name':name,'kind':['bin']},'profile':{'test':False}})+'\\n')
CARGO
  "$REFRESH_PYTHON" "$REFRESH_ARTIFACT_SCRIPT" collect "$1.cargo.jsonl" "$1"
}
ci_capture_main_builds() { printf '%s\\n' "$1" "$2" >"$REFRESH_BUILD_INFO_CALLS"; }
""")
        return {'REFRESH_PROGRAM':str(program),'REFRESH_OWNER_EXIT':str(exit_code),
                'REFRESH_PYTHON':sys.executable,
                'REFRESH_ARTIFACT_SCRIPT':str(Path(run.__file__).resolve().parents[1]/'cargo-artifacts.py'),
                'REFRESH_CALLS':str(self.base/'refresh-owner-calls'),
                'REFRESH_BUILD_INFO_CALLS':str(self.base/'refresh-build-info-calls')}

    def test_compatibility_stage_renews_own_receipt_before_next_descriptor_without_retargeting(self):
        for compatibility_exit in (0,1):
            with self.subTest(compatibility_exit=compatibility_exit):
                program=(self.base/'main-program').resolve();program.write_text('initial canonical main');program.chmod(0o755)
                old_receipt={name:{'path':str(program),'sha256':run.identity_snapshot.digest(program)}
                             for name in ('novarocks','novarocks-sql-test','novarocks-system-tests','novarocks-error-manifest')}
                self.native['receipt']=copy.deepcopy(old_receipt)
                put(self.inputs/'artifacts.json',old_receipt)
                put(self.inputs/'selected-tests.json',{'original_selection':'unchanged'})
                put(self.inputs/'selected-tests.command.json',{'argv':['original-nextest','list']})
                (self.inputs/'artifacts.json.cargo.jsonl').write_text('initial original Cargo stream')
                previous={p.name:p.read_bytes() for p in self.inputs.iterdir() if p.is_file()}
                first={'name':'native-compatibility','tier':'full'}
                later={'name':'selected system program preparation','tier':'admission'}
                chosen=dict(self.chosen,stages=[first,later])
                scopes=[self.scope('stage',first['name']),self.scope('stage',later['name'])]
                identities=copy.deepcopy([scope.identity for scope in scopes])
                calls=[]
                real_run=subprocess.run
                guard=Path(run.__file__).resolve().parents[1]/'cargo-artifacts.py'
                def descriptor(repo,native,item,output):
                    receipt=json.loads((Path(native['inputs'])/'artifacts.json').read_text())
                    self.assertEqual(native['receipt'],receipt)
                    actual=real_run([sys.executable,str(guard),'verify',str(Path(native['inputs'])/'artifacts.json'),'novarocks'],capture_output=True,text=True)
                    if actual.returncode:
                        raise subprocess.CalledProcessError(actual.returncode,actual.args,actual.stdout,actual.stderr)
                    self.assertEqual(actual.stdout.strip(),str(program))
                    return {'env_inputs':[]}
                def invoked(root,repo,kind,name,units,argv,spec,materials,environment):
                    calls.append(name)
                    if name==first['name']:
                        program.write_text('actual restored main after native variants')
                        return compatibility_exit
                    self.assertEqual(environment['NOVAROCKS_BIN'],str(program))
                    return 0
                owner_env=self.post_compat_main_owner_fixture(program)
                with mock.patch.object(run,'stage_spec',side_effect=descriptor),mock.patch.object(run,'invoke',side_effect=invoked),mock.patch.dict(os.environ,owner_env):
                    _,_,attempt=run.execute(self.repo,self.base/('post-compat-'+str(compatibility_exit)),self.native,chosen,scopes)
                self.assertEqual(calls,[first['name'],later['name']])
                self.assertEqual([scope.identity for scope in scopes],identities,'program refresh must not rewrite target scopes')
                refresh=next((attempt/'stage-runtime').glob('*/development-program-refresh'))
                for name,data in previous.items():self.assertEqual((refresh/'before'/name).read_bytes(),data)
                for name,data in previous.items():
                    if name not in ('artifacts.json','artifacts.json.cargo.jsonl'):self.assertEqual((self.inputs/name).read_bytes(),data)
                self.assertEqual(json.loads((refresh/'after/artifacts.json').read_text()),self.native['receipt'])
                self.assertEqual(Path(owner_env['REFRESH_CALLS']).read_text().strip(),str(self.inputs/'artifacts.json'))
                self.assertTrue(Path(owner_env['REFRESH_BUILD_INFO_CALLS']).is_file())
                self.assertEqual(self.native['inputs'],str(self.inputs),'canonical seven input paths remain owned in place')
                errors=json.loads((attempt/'execution.json').read_text())['errors']
                self.assertEqual([error.get('exit') for error in errors],[1] if compatibility_exit else [])
                self.assertEqual(json.loads((attempt/'selection.json').read_text())['executed_scope_keys'],sorted(scope.key for scope in scopes))

    def test_failed_post_compat_refresh_preserves_failure_guard_and_independent_static(self):
        program=(self.base/'failed-main').resolve();program.write_text('old main');program.chmod(0o755)
        receipt={'novarocks':{'path':str(program),'sha256':run.identity_snapshot.digest(program)}}
        self.native['receipt']=copy.deepcopy(receipt);put(self.inputs/'artifacts.json',receipt)
        before=(self.inputs/'artifacts.json').read_bytes()
        first={'name':'native-compatibility','tier':'full'}
        guarded={'name':'selected system program preparation','tier':'admission'}
        independent={'name':'CI build inputs','tier':'admission'}
        calls=[]
        real_run=subprocess.run
        guard=Path(run.__file__).resolve().parents[1]/'cargo-artifacts.py'
        def descriptor(repo,native,item,output):
            if item['name']==guarded['name']:
                actual=real_run([sys.executable,str(guard),'verify',str(Path(native['inputs'])/'artifacts.json'),'novarocks'],capture_output=True,text=True)
                if actual.returncode:
                    raise subprocess.CalledProcessError(actual.returncode,actual.args,actual.stdout,actual.stderr)
            return {'env_inputs':[]}
        def invoked(root,repo,kind,name,*args):
            calls.append(name)
            if name==first['name']:
                program.write_text('changed by compatibility')
                return 1
            return 0
        stages=[first,guarded,independent]
        with mock.patch.object(run,'stage_spec',side_effect=descriptor),mock.patch.object(run,'invoke',side_effect=invoked),mock.patch.dict(os.environ,self.post_compat_main_owner_fixture(program,17)):
            _,_,attempt=run.execute(self.repo,self.base/'failed-main-refresh',self.native,dict(self.chosen,stages=stages),[self.scope('stage',item['name']) for item in stages])
        self.assertEqual((self.inputs/'artifacts.json').read_bytes(),before)
        self.assertEqual(self.native['receipt'],receipt)
        self.assertEqual(calls,[first['name'],independent['name']],'original guard rejects stale input; independent static continues')
        errors=json.loads((attempt/'execution.json').read_text())['errors']
        self.assertEqual(errors[0]['exit'],1)
        self.assertEqual(errors[1]['kind'],'artifact-preparation')
        self.assertIn('development main-program refresh failed',errors[1]['error'])
        self.assertEqual(errors[2]['name'],guarded['name'])
        refresh=next((attempt/'stage-runtime').glob('*/development-program-refresh'))
        self.assertEqual(json.loads((refresh/'exit.json').read_text())['code'],17)
        self.assertEqual((refresh/'before/artifacts.json').read_bytes(),before)
        self.assertEqual((refresh/'after/artifacts.json').read_bytes(),before)

    def test_unrelated_stage_does_not_refresh_development_programs(self):
        item={'name':'CI build inputs','tier':'admission'}
        with mock.patch.object(run,'stage_spec',return_value={'env_inputs':[]}),mock.patch.object(run,'invoke',return_value=0),mock.patch.object(run,'refresh_development_programs') as refresh:
            run.execute(self.repo,self.base/'unrelated-no-refresh',self.native,dict(self.chosen,stages=[item]),[self.scope('stage',item['name'])])
        refresh.assert_not_called()

    def test_variant_preparation_executes_existing_owner_helper(self):
        library=self.repo/'tools/ci/lib';library.mkdir(parents=True)
        actual=Path(run.__file__).resolve().parents[1]/'lib/identity_snapshot.py'
        (library/'identity_snapshot.py').symlink_to(actual)
        variant=self.base/'compatible-program';variant.write_text('variant')
        calls=self.base/'owner-calls'
        (library/'system_scenarios.sh').write_text('ci_prepare_system_scenario_binaries() {\n  printf \'%s\\n\' "$@" >"$OWNER_CALLS"\n  SYSTEM_SCENARIO_PRIMARY_BINARY="$1"\n  SYSTEM_SCENARIO_COMPATIBLE_BINARY="$COMPATIBLE_FIXTURE_BINARY"\n  SYSTEM_SCENARIO_OTHER_ISLAND_BINARY=""\n}\n')
        put(self.inputs/'artifacts.json',self.native['receipt'])
        self.native['receipt']['novarocks-system-tests']={'path':sys.executable}
        with mock.patch.object(run.subprocess,'check_output',return_value='primary\ncompatible\n'),mock.patch.dict(os.environ,{'OWNER_CALLS':str(calls),'COMPATIBLE_FIXTURE_BINARY':str(variant)}):
            run.prepare_system_variants(self.repo,self.native,dict(self.chosen,system_scenarios=[{'name':'mixed'}]),self.base/'variants')
        self.assertTrue(calls.is_file());self.assertEqual(calls.read_text().splitlines()[0],sys.executable)
        self.assertEqual(self.native['system_variants']['compatible']['path'],str(variant))
        self.assertTrue(self.native['system_variants_prepared'])

    def test_system_variant_arguments_bind_prepared_programs(self):
        variant=self.base/'compatible';variant.write_text('compiled-variant')
        self.native['receipt']['novarocks-system-tests']={'path':sys.executable}
        self.native['system_variants']={'compatible':{'path':str(variant),'sha256':run.identity_snapshot.digest(variant)}}
        argv=run.system_argv(self.native,{'name':'native-compatibility/mixed'},'/materials')
        self.assertEqual(argv[argv.index('--compatible-binary')+1],str(variant))
        variant.write_text('changed')
        with self.assertRaises(ValueError):run.system_argv(self.native,{'name':'native-compatibility/mixed'},'/materials')


if __name__=='__main__':unittest.main()
