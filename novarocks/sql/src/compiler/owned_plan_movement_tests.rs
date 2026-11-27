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

//! Owned driver for the final physical-plan completion protocol.
//!
//! Every pending state in this module contains SQL values only. Catalog and
//! provider capabilities remain with the application that answers a typed
//! need batch. The optimizer is run only after the corresponding immutable
//! catalog, MV and statistics facts have been installed.

use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use novarocks_functions::FunctionArgument;
use novarocks_physical_plan::{
    AggregateCall, AggregatePhase, Fragment, NodeKind, PhysicalCallSite, PhysicalNode,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
};

use super::SqlCompileIntent;
use super::tests::{complete_with_exact_statistics, frozen_table_statistics};
use crate::compiler::SqlAuthoredPhysicalPlan;
use crate::planner::distributed::build::{
    AggregateRuntimeDemand, CheckedAggregateLogicalSourceEntry, SqlSourceJournalError,
};

fn completed_min() -> crate::compiler::SqlCompletedPlan {
    complete_with_exact_statistics(
        "SELECT MIN(order_key) AS lo FROM orders",
        SqlCompileIntent::Query,
        13,
    )
}

fn first_aggregate(
    owner: &SqlAuthoredPhysicalPlan,
) -> (&Fragment, &PhysicalNode, PhysicalCallSite, &AggregateCall) {
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Aggregate { calls, .. } = &node.kind
                && let Some(call) = calls.first()
            {
                return (
                    fragment,
                    node,
                    PhysicalCallSite::Aggregate {
                        node: node.id,
                        call: 0,
                    },
                    call,
                );
            }
        }
    }
    panic!("the actual SQL aggregate must survive physical lowering")
}

fn loan<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    call: &'a AggregateCall,
    control: &dyn PureCompileControl,
) -> Result<CheckedAggregateLogicalSourceEntry<'a>, SqlSourceJournalError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = owner.checked_aggregate_source_observed(fragment, node, site, call, &mut work);
    match result {
        Err(SqlSourceJournalError::Control(cause)) => Err(SqlSourceJournalError::Control(cause)),
        result => {
            work.finish()?;
            result
        }
    }
}

#[test]
fn completed_query_parts_move_original_plan_and_logical_journal_together() {
    let completed = completed_min();
    let original_address = completed.plan() as *const _;
    let original_annotations = frozen_table_statistics(completed.plan())
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert!(!original_annotations.is_empty());
    let (owner, intent, annotations) = completed.into_parts();
    assert!(matches!(intent, super::SqlDisplayIntent::Execute));
    assert!(annotations.is_empty());
    assert_eq!(owner.plan() as *const _, original_address);
    assert_eq!(frozen_table_statistics(owner.plan()), original_annotations);

    let cloned = owner.clone();
    assert!(Arc::ptr_eq(owner.plan_arc(), cloned.plan_arc()));
    let control = crate::compiler::SqlCompileControl::unbounded();
    let mut count = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                let site = PhysicalCallSite::Aggregate {
                    node: node.id,
                    call: u32::try_from(ordinal).unwrap(),
                };
                let entry = loan(&owner, fragment, node, site, call, &control).unwrap();
                let same = loan(&cloned, fragment, node, site, call, &control).unwrap();
                assert!(std::ptr::eq(entry.captured(), same.captured()));
                assert!(std::ptr::eq(entry.fragment(), fragment));
                assert!(std::ptr::eq(entry.node(), node));
                assert!(std::ptr::eq(entry.source(), call));
                assert_eq!(entry.site(), site);
                assert_eq!(entry.phase(), call.binding.phase);
                let request = entry.captured().request();
                assert_eq!(request.logical_argument_count, 1);
                assert_eq!(request.arguments.len(), 1);
                let FunctionArgument::Value {
                    value_type,
                    constant,
                } = &request.arguments[0]
                else {
                    panic!("MIN logical source is its original scan value")
                };
                assert_eq!(*value_type, FunctionValueType::new(DataType::Int64, false));
                assert!(constant.is_none());
                assert_eq!(
                    request.expected_result_type,
                    Some(&call.binding.function.result_type)
                );
                assert_eq!(
                    entry.captured().constant_policy(),
                    crate::constant::test_constant_policy()
                );
                match call.binding.phase {
                    AggregatePhase::Single | AggregatePhase::Partial { .. } => {
                        assert_eq!(entry.runtime(), AggregateRuntimeDemand::Update);
                    }
                    AggregatePhase::Final { .. } | AggregatePhase::Intermediate { .. } => {
                        assert_eq!(call.arguments.len(), 1);
                        assert_eq!(
                            entry.runtime(),
                            AggregateRuntimeDemand::ExpressionState(
                                fragment
                                    .root_expression(node.id, call.arguments[0])
                                    .unwrap()
                                    .id
                            )
                        );
                        assert_eq!(
                            fragment
                                .root_expression(node.id, call.arguments[0])
                                .unwrap()
                                .ty,
                            call.binding.merge_state.as_ref().unwrap().intermediate_type
                        );
                    }
                }
                count += 1;
            }
        }
    }
    assert!(count > 0);
}

#[test]
fn journal_rejects_equal_foreign_fragment_and_wrong_actual_array_ordinal() {
    let owner = completed_min().into_plan();
    let (fragment, node, site, call) = first_aggregate(&owner);
    let foreign_plan = owner.plan().clone();
    let foreign_fragment = foreign_plan.fragments().get(&fragment.id()).unwrap();
    let foreign_node = foreign_fragment.nodes().get(&node.id).unwrap();
    let NodeKind::Aggregate { calls, .. } = &foreign_node.kind else {
        unreachable!()
    };
    let control = crate::compiler::SqlCompileControl::unbounded();
    assert!(matches!(
        loan(
            &owner,
            foreign_fragment,
            foreign_node,
            site,
            &calls[0],
            &control
        ),
        Err(SqlSourceJournalError::InvalidSource(
            "aggregate journal loans a foreign plan or node"
        ))
    ));
    assert!(matches!(
        loan(
            &owner,
            fragment,
            node,
            PhysicalCallSite::Aggregate {
                node: node.id,
                call: u32::MAX
            },
            call,
            &control
        ),
        Err(SqlSourceJournalError::InvalidSource(
            "aggregate journal call differs from its original site"
        ))
    ));
}

struct TraceControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for TraceControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        if let Some((index, _)) = self.refusal {
            assert!(
                trace.len() <= index,
                "the original refusal cannot be followed by another callback"
            );
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((index, cause)) if trace.len() == index + 1 => Err(cause),
            _ => Ok(()),
        }
    }
}

#[test]
fn journal_moved_owner_preserves_each_original_control_prefix_and_ordinary_tail() {
    let owner = completed_min().into_plan();
    let (fragment, node, site, call) = first_aggregate(&owner);
    for invalid in [false, true] {
        let attempted = if invalid {
            PhysicalCallSite::Aggregate {
                node: node.id,
                call: u32::MAX,
            }
        } else {
            site
        };
        let control = TraceControl {
            trace: Mutex::new(Vec::new()),
            refusal: None,
        };
        let result = loan(&owner, fragment, node, attempted, call, &control);
        assert_eq!(result.is_ok(), !invalid);
        if invalid {
            assert!(matches!(
                result,
                Err(SqlSourceJournalError::InvalidSource(
                    "aggregate journal call differs from its original site"
                ))
            ));
        }
        let baseline = control.trace.into_inner().unwrap();
        assert_eq!(baseline[0], (CompilePhase::Validate, 0));
        assert!(baseline.iter().any(|(_, units)| *units > 0));
        if invalid {
            assert!(
                baseline.last().unwrap().1 > 0,
                "the completed ordinary comparison reaches its caller footer"
            );
        }
        for index in 0..baseline.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = TraceControl {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((index, cause.clone())),
                };
                let result = loan(&owner, fragment, node, attempted, call, &control);
                assert!(
                    matches!(result, Err(SqlSourceJournalError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace.into_inner().unwrap(), baseline[..=index]);
            }
        }
    }
}
