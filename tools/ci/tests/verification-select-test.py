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
"""Impact-selection expectations use manual native declarations and a tiny Git repo."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from verification.select import (changed_paths, domain_intersects, ignored_scope_declarations,
                                 select, sql_case_arguments)


class Selection(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='verification-select-')
        self.addCleanup(self.temporary.cleanup)
        self.repo = Path(self.temporary.name)
        self.metadata = {'version':1, 'workspace_members':['sql-id','ice-id','server-id'], 'packages':[
            self.package('sql-id','sql-owner','novarocks/sql'),
            self.package('ice-id','ice-owner','novarocks/connector/iceberg'),
            self.package('server-id','server','novarocks-server')]}
        self.sql = {'schema':1, 'suites':[
            {'name':'optimizer', 'impact':{'paths':['novarocks/sql/src/optimizer/**']}, 'explicit_only':False},
            {'name':'sql-heavy', 'impact':{'paths':['novarocks/sql/src/planner/**']}, 'explicit_only':True},
            {'name':'ice-heavy', 'impact':{'paths':['novarocks/connector/iceberg/**']}, 'explicit_only':True}]}
        self.system = {'schema':1, 'scenarios':[
            {'name':'sql-system-heavy','default':False,'declaration':{'impact':{'paths':['novarocks/sql/src/planner/**']}}},
            {'name':'ice-system-heavy','default':False,'declaration':{'impact':{'paths':['novarocks/connector/iceberg/**']}}}]}
        self.stages = {'schema':1,'stages':[
            {'name':'generic','tier':'admission','domain':['**']},
            {'name':'sql-stage','tier':'admission','domain':['novarocks/sql/src/optimizer/**']},
            {'name':'ice-stage','tier':'full','domain':['novarocks/connector/iceberg/**']}]}
        self.rules = {'schema':1,'rules':[
            {'paths':['Cargo.toml','Cargo.lock','.cargo/**'],'action':'broad','reason':'global build input'},
            {'paths':['docs/**'],'action':'documentation','reason':'document review'},
            {'paths':['tools/**'],'action':'declared','reason':'delegate to native declarations'}]}

    def package(self, identity, name, directory, ignored=None, inputs=None):
        return {'id':identity,'name':name,'manifest_path':str(self.repo/directory/'Cargo.toml'),
                'metadata':{'novarocks':{'test':{'runtime-inputs':inputs or [],'env-inputs':[],
                    'ignored':ignored or [{'filter':'test(=heavy_test)','class':'heavy','reason':'external fixture',
                                          'requires':['fixture:external']}]}}}}

    def choose(self, paths):
        return select(paths,self.metadata,self.sql,self.system,self.stages,self.rules,self.repo)

    def names(self, chosen, kind):
        return {value['name'] for value in chosen[kind]}

    def test_direct_mapping_and_native_rdeps_filter(self):
        chosen=self.choose(['novarocks/sql/src/optimizer/rule.rs'])
        self.assertEqual(chosen['packages'],['sql-owner'])
        self.assertEqual(chosen['rust_filterset'],'rdeps(=sql-owner)')
        self.assertTrue(chosen['doctest'])
        self.assertEqual(self.names(chosen,'sql_suites'),{'optimizer'})
        self.assertEqual(self.names(chosen,'system_scenarios'),set())
        self.assertEqual(chosen['gaps'],[])
        self.assertFalse(chosen['broad'])

    def test_workspace_aggregate_is_deferred_before_observations_even_for_broad_selection(self):
        item={'name':'workspace nextest and doctests','tier':'admission','domain':['**']}
        self.stages['stages'].append(item)
        for paths in (['novarocks/sql/src/optimizer/rule.rs'],['Cargo.toml']):
            with self.subTest(paths=paths):
                chosen=self.choose(paths)
                self.assertNotIn(item['name'],self.names(chosen,'stages'))
                deferred=chosen['deferred_stages']
                self.assertEqual(len(deferred),1)
                self.assertEqual(deferred[0]['deferred']['backstop'],'admission')
                self.assertTrue(deferred[0]['deferred']['reason'])
                self.assertTrue(chosen['doctest'])
                self.assertTrue(chosen['rust_filterset'])
                self.assertEqual(self.stages['stages'][-1],item)

    def test_native_suite_input_selects_full_suite(self):
        chosen=self.choose(['tests/sql/correctness/optimizer/sql/new_case.sql'])
        self.assertEqual(self.names(chosen,'sql_suites'),{'optimizer'})
        self.assertIsNone(chosen['rust_filterset'])
        self.assertEqual(chosen['gaps'],[])

    def test_shared_suite_cannot_be_reduced_to_cases(self):
        with self.assertRaises(ValueError):
            sql_case_arguments({'reuse':{'scope':'suite','initial_state':'isolated'}},['a'])
        with self.assertRaises(ValueError):
            sql_case_arguments({'reuse':{'scope':'case'}},['a'])
        self.assertEqual(sql_case_arguments({'reuse':{'scope':'case','initial_state':'reset'}},['a','b']),['--only','a,b'])
        self.assertEqual(sql_case_arguments({},None),[])

    def test_unknown_owned_path_widens_only_known_package_domain(self):
        chosen=self.choose(['novarocks/sql/src/new_unmapped/file.rs'])
        self.assertFalse(chosen['broad'])
        self.assertEqual(chosen['packages'],['sql-owner'])
        self.assertEqual(self.names(chosen,'sql_suites'),{'optimizer','sql-heavy'})
        self.assertEqual(self.names(chosen,'system_scenarios'),{'sql-system-heavy'})
        self.assertEqual(self.names(chosen,'stages'),{'generic','sql-stage'})
        self.assertEqual(chosen['gaps'][0]['owner'],'sql-owner')
        self.assertEqual(chosen['gaps'][0]['expansion'],'package-domain')
        self.assertEqual({entry['package'] for entry in chosen['heavy_ignored']},{'sql-owner'})
        self.assertTrue(any(reason['reason']=='unmapped path widens to the Cargo package domain' for reason in chosen['reasons']))

    def test_global_build_configuration_expands_broadly(self):
        chosen=self.choose(['Cargo.toml'])
        self.assertTrue(chosen['broad'])
        self.assertEqual(chosen['packages'],['ice-owner','server','sql-owner'])
        self.assertEqual(self.names(chosen,'sql_suites'),{'optimizer','sql-heavy','ice-heavy'})
        self.assertEqual(len(chosen['heavy_ignored']),3)
        self.assertEqual(chosen['gaps'],[])

    def test_unowned_path_reports_gap_before_broad_expansion(self):
        chosen=self.choose(['unmapped/root-input.data'])
        self.assertTrue(chosen['broad'])
        self.assertEqual(chosen['gaps'][0]['expansion'],'broad')
        self.assertIsNone(chosen['gaps'][0]['owner'])

    def test_declared_rule_without_actual_mapping_cannot_hide_gap(self):
        chosen=self.choose(['tools/unmapped-tool.py'])
        self.assertTrue(chosen['broad'])
        self.assertEqual(chosen['gaps'][0]['path'],'tools/unmapped-tool.py')

    def test_document_review_rule_does_not_select_unrelated_heavy(self):
        chosen=self.choose(['docs/new-note.md'])
        self.assertFalse(chosen['broad'])
        self.assertEqual(chosen['packages'],[])
        self.assertEqual(chosen['heavy_ignored'],[])
        self.assertEqual(chosen['gaps'],[])

    def test_deployable_fe_example_is_exact_document_review_without_masking_other_inputs(self):
        repository = Path(__file__).resolve().parents[3]
        rules = json.loads((repository / 'tools/ci/verification/impact-rules.json').read_text())
        self.rules = rules
        chosen = self.choose(['novarocks-fe.toml.example'])
        self.assertFalse(chosen['broad'])
        self.assertEqual(chosen['gaps'], [])
        self.assertEqual(chosen['packages'], [])
        self.assertEqual(self.names(chosen, 'sql_suites'), set())
        unknown = self.choose(['unregistered-deployment.toml.example'])
        self.assertTrue(unknown['broad'])
        self.assertEqual(unknown['gaps'][0]['path'], 'unregistered-deployment.toml.example')
        global_build = self.choose(['Cargo.toml'])
        self.assertTrue(global_build['broad'])
        self.assertEqual(global_build['gaps'], [])

    def test_declared_runtime_input_selects_its_rust_consumer(self):
        self.metadata['packages'][0]['metadata']['novarocks']['test']['runtime-inputs']=['tests/sql/correctness/**/*.sql']
        chosen=self.choose(['tests/sql/correctness/optimizer/sql/new_case.sql'])
        self.assertEqual(chosen['packages'],['sql-owner'])
        self.assertEqual(chosen['rust_filterset'],'rdeps(=sql-owner)')
        self.assertTrue(any(reason['reason'].startswith('declared runtime input') for reason in chosen['reasons']))

    def test_root_agent_guides_use_document_review_without_masking_unknown_inputs(self):
        repository = Path(__file__).resolve().parents[3]
        self.rules = json.loads((repository / 'tools/ci/verification/impact-rules.json').read_text())
        for path in ('AGENTS.md', 'CLAUDE.md'):
            with self.subTest(path=path):
                chosen = self.choose([path])
                self.assertFalse(chosen['broad'])
                self.assertEqual(chosen['gaps'], [])
                self.assertEqual(chosen['packages'], [])
                self.assertEqual(self.names(chosen, 'sql_suites'), set())
                self.assertEqual(self.names(chosen, 'system_scenarios'), set())
        unknown = self.choose(['unregistered-root-guidance.md'])
        self.assertTrue(unknown['broad'])
        self.assertEqual(unknown['gaps'][0]['path'], 'unregistered-root-guidance.md')

    def test_documented_agent_guide_still_selects_a_declared_executable_consumer(self):
        repository = Path(__file__).resolve().parents[3]
        self.rules = json.loads((repository / 'tools/ci/verification/impact-rules.json').read_text())
        self.metadata['packages'][0]['metadata']['novarocks']['test']['runtime-inputs'] = ['AGENTS.md']
        self.stages['stages'].append({'name':'guide-consumer','tier':'admission','domain':['AGENTS.md']})
        chosen = self.choose(['AGENTS.md'])
        self.assertFalse(chosen['broad'])
        self.assertEqual(chosen['gaps'], [])
        self.assertEqual(chosen['packages'], ['sql-owner'])
        self.assertEqual(chosen['rust_filterset'], 'rdeps(=sql-owner)')
        self.assertIn('guide-consumer', self.names(chosen, 'stages'))

    def test_agent_navigation_does_not_narrow_shared_wire_contract_selection(self):
        repository = Path(__file__).resolve().parents[3]
        self.rules = json.loads((repository / 'tools/ci/verification/impact-rules.json').read_text())
        for path in ('novarocks/spi/src/connector/write_stack/relation.rs',
                     'novarocks/plan-codec/src/physical_summary_v2.rs'):
            with self.subTest(path=path):
                wire = self.choose([path])
                combined = self.choose(['AGENTS.md', 'CLAUDE.md', path])
                self.assertTrue(combined['broad'])
                self.assertEqual(combined['gaps'], [])
                for key in ('packages', 'rust_filterset', 'sql_suites', 'system_scenarios', 'stages'):
                    self.assertEqual(combined[key], wire[key])

    def test_heavy_scope_declarations_preserve_native_identity_authority(self):
        self.metadata['packages'][0]['metadata']['novarocks']['test']['ignored'] += [
            {'filter':'test(=child_helper)','class':'helper','reason':'owned by outer test'},
            {'filter':'test(=recorder)','class':'manual','reason':'manual recorder'}]
        records=ignored_scope_declarations(self.metadata)
        heavy=[record for record in records if record['validation_unit']]
        self.assertEqual(len(heavy),3)
        self.assertTrue(all(record['kind']=='rust-test' and record['tier']=='heavy' and record['mode']=='default' for record in heavy))
        self.assertTrue(all('package(=' in record['filterset'] for record in records))
        self.assertTrue(all('unit' not in record for record in records))
        chosen=self.choose(['novarocks/sql/src/optimizer/rule.rs'])
        self.assertEqual(len(chosen['heavy_ignored']),1)
        self.assertEqual(chosen['heavy_ignored_summary_basis'],'direct-owner')
        self.assertTrue(chosen['heavy_ignored_requires_native_resolution'])

    def test_nested_cargo_owner_is_most_specific(self):
        self.metadata['workspace_members'].append('connector-parent-id')
        self.metadata['packages'].append(self.package('connector-parent-id','connector-parent','novarocks/connector'))
        chosen=self.choose(['novarocks/connector/iceberg/src/new.rs'])
        self.assertEqual(chosen['packages'],['ice-owner'])
        self.assertFalse(chosen['broad'])

    def test_domain_intersection_understands_globbed_package_names(self):
        self.assertTrue(domain_intersects('novarocks/sql',['novarocks/sql/src/optimizer/**']))
        self.assertTrue(domain_intersects('novarocks/query-application',['novarocks/*-application/src/**']))
        self.assertFalse(domain_intersects('novarocks/execution',['novarocks/*-application/src/**']))
        self.assertFalse(domain_intersects('novarocks/sql',['novarocks/connector/iceberg/**']))

    def test_input_order_and_duplicates_do_not_change_selection(self):
        a='novarocks/sql/src/optimizer/rule.rs'
        b='novarocks/connector/iceberg/src/reader.rs'
        self.assertEqual(self.choose([a,b]),self.choose([b,a,a]))

    def test_changed_paths_keeps_both_rename_owners_and_untracked_content(self):
        def git(*args):
            return subprocess.check_output(['git']+list(args),cwd=self.repo,text=True).strip()
        git('init','-q')
        git('config','user.name','Selection Fixture')
        git('config','user.email','selection@example.invalid')
        source=self.repo/'old-package/file.rs';source.parent.mkdir();source.write_text('unchanged content\n')
        git('add','-A');git('commit','-qm','Add input fixture')
        base=git('rev-parse','HEAD')
        destination=self.repo/'new-package/file.rs';destination.parent.mkdir();source.rename(destination)
        git('add','-A')
        (self.repo/'new-untracked.data').write_text('input\n')
        paths=changed_paths(self.repo,base)
        self.assertEqual(paths,['new-package/file.rs','new-untracked.data','old-package/file.rs'])


if __name__=='__main__':
    unittest.main()
