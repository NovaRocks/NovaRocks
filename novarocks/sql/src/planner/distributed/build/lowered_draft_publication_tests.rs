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

//! Actual lowering publication, constant reuse and original control tails.

use super::*;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        if let Some((stop, _)) = self.refusal {
            assert!(trace.len() <= stop, "callback after first refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if trace.len() == stop + 1 => Err(cause),
            _ => Ok(()),
        }
    }
}
fn values_draft() -> LoweredSqlPhysicalDraft {
    use crate::analysis::{ExprKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode, PhysicalPlanStats};
    let ty =
        novarocks_type_contract::FunctionValueType::new(arrow::datatypes::DataType::Int64, false);
    let node = PhysicalPlanNode {
        kind: PhysicalPlanKind::Values(crate::planner::payload::PlanValuesNode {
            rows: vec![vec![TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(7)),
                value_type: ty.clone(),
            }]],
            columns: vec![OutputColumn {
                column_id: ColumnId(1),
                name: "k".into(),
                value_type: ty.clone(),
                is_internal: false,
            }],
        }),
        children: Vec::new(),
        output_columns: vec![OutputColumn {
            column_id: ColumnId(1),
            name: "k".into(),
            value_type: ty,
            is_internal: false,
        }],
        stats: PhysicalPlanStats {
            output_row_count: 1.0,
            row_count_confidence: crate::planner::physical::PlannerConfidence::Exact,
            column_statistics: Default::default(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        probe_runtime_filters: Vec::new(),
    };
    super::super::contract_lowering::lower_final_physical_plan(
        &node,
        novarocks_physical_plan::PlanVersionId::try_new([17; 16]).unwrap(),
        novarocks_physical_plan::PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap()
}
#[test]
fn actual_source_owner_publication_observes_arc_completion_at_every_control_prefix() {
    let control = Control::default();
    let owner = values_draft().finish_observed(&control).unwrap();
    assert_eq!(owner.plan().fragments().len(), 1);
    assert!(
        owner.call_sources.entries.is_empty(),
        "only the actual visitor may produce a lawful empty journal"
    );
    let baseline = control.trace.into_inner().unwrap();
    assert_eq!(baseline[0], (CompilePhase::Validate, 0));
    assert_eq!(
        &baseline[baseline.len() - 2..],
        &[(CompilePhase::Validate, 1), (CompilePhase::Validate, 1)],
        "each new Arc owner has its own completed publication observation"
    );
    for index in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((index, cause.clone())),
            };
            let result = values_draft().finish_observed(&control);
            assert!(
                matches!(result,Err(PlanConstructionError::Constants(novarocks_physical_plan::ConstantReferenceError::Control(actual))) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), baseline[..=index]);
        }
    }
}
#[test]
fn actual_lowering_reuses_each_captured_literal_and_preserves_distinct_sites() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT COUNT(7), COUNT(9) FROM orders",
    );
    let control = crate::compiler::SqlCompileControl::unbounded();
    let mut constants = Vec::new();
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                if !call.binding.phase.consumes_logical_arguments() {
                    continue;
                }
                let site = PhysicalCallSite::Aggregate {
                    node: node.id,
                    call: ordinal.try_into().unwrap(),
                };
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
                let loan = owner
                    .checked_aggregate_source_observed(fragment, node, site, call, &mut work)
                    .unwrap();
                let novarocks_functions::FunctionArgument::Value {
                    constant: Some(captured),
                    ..
                } = &loan.captured().request().arguments[0]
                else {
                    panic!("original top-level literal must remain a checked constant")
                };
                let expression = fragment
                    .root_expression(node.id, call.arguments[0])
                    .unwrap();
                let novarocks_expression_contract::ExprKind::Constant(reference) = expression.kind
                else {
                    panic!("lowering must reuse the original literal constant")
                };
                let actual = owner
                    .plan()
                    .constants()
                    .resolve_observed(reference, &expression.ty, &mut work)
                    .unwrap();
                assert_eq!(
                    actual.pool().backing_identity(),
                    captured.pool().backing_identity()
                );
                assert_eq!(actual.ordinal(), captured.ordinal());
                assert!(Arc::ptr_eq(
                    actual.pool().field_ref(),
                    captured.pool().field_ref()
                ));
                constants.push(actual);
                work.finish().unwrap();
            }
        }
    }
    assert_eq!(
        constants.len(),
        2,
        "both actual array ordinals must retain their independent requests"
    );
    assert_ne!(
        constants[0].pool().backing_identity(),
        constants[1].pool().backing_identity()
    );
}
