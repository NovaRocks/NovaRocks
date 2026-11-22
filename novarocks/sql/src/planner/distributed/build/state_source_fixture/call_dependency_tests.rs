// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::compiler::{
    AggregateRuntimeDemand, SqlCallDependencyProvenance, SqlCallDependencySite, SqlCompileControl,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};

#[test]
fn sql_call_dependency_loan_original_aggregate_all_phases_keep_source_and_state_channels() {
    let control = SqlCompileControl::unbounded();
    let owner = super::ordinary_union_source(Some((7, 11, 13)), true, &control).unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut updates = 0;
    let mut merges = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let novarocks_physical_plan::NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, source) in calls.iter().enumerate() {
                let site = novarocks_physical_plan::PhysicalCallSite::Aggregate {
                    node: node.id,
                    call: ordinal as u32,
                };
                let loan = owner
                    .borrow_call_dependency_observed(
                        SqlCallDependencySite::Aggregate {
                            fragment,
                            node,
                            site,
                            source,
                        },
                        &mut work,
                    )
                    .unwrap();
                assert_eq!(loan.original_logical_argument_count(), 1);
                assert!(loan.original_binding().result_constraint().is_none());
                match loan.provenance() {
                    SqlCallDependencyProvenance::Aggregate {
                        source: actual,
                        phase,
                        runtime,
                        ..
                    } => {
                        assert!(std::ptr::eq(actual, source));
                        assert_eq!(phase, source.binding.phase);
                        match runtime {
                            AggregateRuntimeDemand::Update
                            | AggregateRuntimeDemand::FilteredUpdate { .. } => updates += 1,
                            AggregateRuntimeDemand::ExpressionState(id) => {
                                let [root] = source.arguments.as_ref() else {
                                    panic!("the merge consumes exactly its original state");
                                };
                                let unit = node.expressions.as_deref().unwrap();
                                assert_eq!(unit.root(*root).unwrap().definition, id);
                                merges += 1;
                            }
                            _ => panic!("aggregate is not writer"),
                        }
                    }
                    _ => panic!("original aggregate source must remain aggregate"),
                }
            }
        }
    }
    assert_eq!(updates, 2);
    assert!(merges >= 2);
    work.finish().unwrap();
}
