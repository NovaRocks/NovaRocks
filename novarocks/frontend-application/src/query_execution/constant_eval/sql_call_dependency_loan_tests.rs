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

//! Genuine completed SQL source loans; no owner preparation or source reconstruction.
use super::*;
use arrow::datatypes::DataType;
use novarocks_sql::binding::SqlResultConstraintOrigin;
use novarocks_sql::compiler::{
    AggregateRuntimeDemand, SqlCallDependencyProvenance, SqlCallDependencySite,
    SqlExpressionCallKind, SqlSourceJournalError,
};
use novarocks_type_contract::{CompileCheckpoints, ValueLogicalType};

fn exact(sql: &str) -> SqlAuthoredPhysicalPlan {
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([84; 16]).unwrap(),
        SqlStatementInput::sql(sql),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "fixture".into(),
            optimizer_settings: SessionOptimizerSettings {
                enable_materialized_view_rewrite: Some(false),
                enable_common_subexpr_reuse: Some(false),
                ..SessionOptimizerSettings::default()
            },
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        &DECLINE,
        policy(),
        control.clone(),
        PipelineDopDomain {
            min: 1,
            max: 8,
            requires_power_of_two: true,
        },
        ScanReadBudget {
            max_batch_rows: 64,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    match SqlCompiler::start(request.try_into_completion().unwrap(), &control).unwrap() {
        SqlCompileProgress::Complete(completed) => completed.into_plan(),
        SqlCompileProgress::Incomplete(_) => panic!("table-free actual source must complete"),
    }
}
fn function_site(
    owner: &SqlAuthoredPhysicalPlan,
) -> (
    &novarocks_physical_plan::Fragment,
    &novarocks_physical_plan::ExprNode,
) {
    owner
        .plan()
        .fragments()
        .values()
        .find_map(|fragment| {
            fragment.expressions().iter().find_map(|(_, source)| {
                matches!(&source.kind, ExprKind::FunctionCall { .. }).then_some((fragment, source))
            })
        })
        .expect("actual SQL fixture retains a call")
}
#[test]
fn sql_call_dependency_loan_real_upper_keeps_original_unconstrained_and_constants() {
    let owner = exact("SELECT upper('kept') AS source_call");
    let (fragment, expression) = function_site(&owner);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let loan = owner
        .borrow_call_dependency_observed(
            SqlCallDependencySite::Expression {
                fragment,
                source: expression,
            },
            &mut work,
        )
        .unwrap();
    assert!(loan.original_binding().result_constraint().is_none());
    assert_eq!(
        loan.original_binding().result_constraint_origin(),
        &SqlResultConstraintOrigin::Unconstrained
    );
    assert_eq!(loan.original_logical_argument_count(), 1);
    assert_eq!(loan.original_constant_policy(), policy());
    let [
        novarocks_functions::FunctionArgument::Value {
            constant: Some(value),
            value_type,
        },
    ] = loan.original_arguments()
    else {
        panic!("original literal must remain an admitted CV")
    };
    assert_eq!(text(value), "kept");
    assert_eq!(value.value_type(), value_type);
    let canonical = loan
        .canonical()
        .expect("exact same-emission call has canonical metadata");
    assert!(canonical.request().expected_result_type.is_none());
    assert_eq!(
        canonical.selected().result_type,
        FunctionResultType::Scalar(expression.ty.clone())
    );
    assert_eq!(
        canonical.binding().resolved().function_id,
        loan.original_binding().resolved().function_id
    );
    match loan.provenance() {
        SqlCallDependencyProvenance::Expression {
            source,
            kind,
            channels,
            ..
        } => {
            assert!(std::ptr::eq(source, expression));
            assert_eq!(kind, SqlExpressionCallKind::Scalar);
            let ExprKind::FunctionCall { args, .. } = &expression.kind else {
                unreachable!()
            };
            assert_eq!(channels, args.as_ref());
        }
        _ => panic!("expression provenance must not become relation provenance"),
    }
    work.finish().unwrap();
}
#[test]
fn sql_call_dependency_loan_real_typed_empty_array_has_original_constraint() {
    let owner = exact("SELECT ARRAY<JSON>[] AS source_array");
    let (fragment, expression) = function_site(&owner);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let loan = owner
        .borrow_call_dependency_observed(
            SqlCallDependencySite::Expression {
                fragment,
                source: expression,
            },
            &mut work,
        )
        .unwrap();
    assert_eq!(
        loan.original_binding().result_constraint_origin(),
        &SqlResultConstraintOrigin::EmptyArrayLiteral
    );
    let original = loan
        .original_binding()
        .result_constraint()
        .expect("actual syntax authored result target");
    assert_eq!(original, &expression.ty);
    assert!(!original.nullable());
    assert_eq!(original.logical_type(), ValueLogicalType::Physical);
    let DataType::List(item) = &original.data_type() else {
        panic!("original typed array target")
    };
    assert_eq!(item.name(), "item");
    assert_eq!(item.data_type(), &DataType::Utf8);
    let original_child = original
        .child_at(0)
        .expect("complete syntax-authored list child");
    assert_eq!(original_child.logical_type(), ValueLogicalType::Json);
    assert!(matches!(
        original_child.value_type().kind(),
        novarocks_type_contract::ValueKind::Nominal(ValueLogicalType::Json)
    ));
    assert_eq!(original_child.data_type(), item.data_type());
    assert_eq!(original_child.nullable(), item.is_nullable());
    assert!(item.metadata().as_arc().is_none());
    assert!(loan.original_arguments().is_empty());
    assert_eq!(loan.original_logical_argument_count(), 0);
    let canonical = loan.canonical().unwrap();
    assert_eq!(canonical.request().expected_result_type, Some(original));
    assert_eq!(
        canonical.selected().result_type,
        FunctionResultType::Scalar(original.clone())
    );
    work.finish().unwrap();
}
#[test]
fn sql_call_dependency_loan_original_foreign_and_missing_source_refusals_stay_typed() {
    let owner = compile("SELECT upper('kept')", &DECLINE, None).unwrap();
    let foreign = compile("SELECT upper('kept')", &DECLINE, None).unwrap();
    let (fragment, source) = function_site(&foreign);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(matches!(
        owner.borrow_call_dependency_observed(
            SqlCallDependencySite::Expression { fragment, source },
            &mut work
        ),
        Err(SqlSourceJournalError::InvalidSource(
            "expression journal loans a foreign plan or expression"
        ))
    ));
    let (fragment, _) = function_site(&owner);
    let literal = fragment
        .expressions()
        .iter()
        .find_map(|(_, expression)| {
            matches!(
                expression.kind,
                ExprKind::Literal(_) | ExprKind::Constant(_)
            )
            .then_some(expression)
        })
        .expect("original argument expression");
    assert!(matches!(
        owner.borrow_call_dependency_observed(
            SqlCallDependencySite::Expression {
                fragment,
                source: literal
            },
            &mut work
        ),
        Err(SqlSourceJournalError::MissingEntry)
    ));
    work.finish().unwrap();
}
#[derive(Default)]
struct JournalMeter {
    events: Mutex<Vec<u32>>,
    fail: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for JournalMeter {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let ordinal = events.len();
        events.push(units);
        if let Some((at, cause)) = self.fail.clone()
            && ordinal == at
        {
            return Err(cause);
        }
        Ok(())
    }
}
#[test]
fn sql_call_dependency_loan_does_not_add_scope_step_or_success_footer_and_latches_control() {
    let owner = compile("SELECT upper('kept')", &DECLINE, None).unwrap();
    let (fragment, source) = function_site(&owner);
    let success = JournalMeter::default();
    let mut work = CompileCheckpoints::try_new(&success, CompilePhase::Validate).unwrap();
    let _loan = owner
        .borrow_call_dependency_observed(
            SqlCallDependencySite::Expression { fragment, source },
            &mut work,
        )
        .unwrap();
    // Original checked_expression_source: entry flush, four owner/kind steps,
    // one captured-origin step, scope/count two steps, one channel step, final flush.
    assert_eq!(&*success.events.lock().unwrap(), &[0, 0, 8]);
    work.finish().unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [1, 2] {
            let meter = JournalMeter {
                fail: Some((at, cause.clone())),
                ..JournalMeter::default()
            };
            let mut work = CompileCheckpoints::try_new(&meter, CompilePhase::Validate).unwrap();
            assert!(
                matches!(owner.borrow_call_dependency_observed(SqlCallDependencySite::Expression { fragment, source }, &mut work), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            let events = meter.events.lock().unwrap().clone();
            assert!(
                matches!(owner.borrow_call_dependency_observed(SqlCallDependencySite::Expression { fragment, source }, &mut work), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            assert_eq!(*meter.events.lock().unwrap(), events);
        }
    }
}
#[test]
fn sql_call_dependency_loan_real_table_unnest_retains_relation_and_channels() {
    let owner =
        exact("SELECT u.* FROM (SELECT [1, 2] AS a) q CROSS JOIN LATERAL UNNEST(q.a) AS u(value)");
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut count = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let novarocks_physical_plan::NodeKind::TableFunction { arguments, .. } = &node.kind
            else {
                continue;
            };
            let loan = owner
                .borrow_call_dependency_observed(
                    SqlCallDependencySite::Table {
                        fragment,
                        source: node,
                    },
                    &mut work,
                )
                .unwrap();
            assert_eq!(loan.original_logical_argument_count(), arguments.len());
            let canonical = loan.canonical().unwrap();
            assert!(matches!(
                canonical.selected().result_type,
                FunctionResultType::Relation(_)
            ));
            assert!(canonical.request().expected_result_type.is_none());
            match loan.provenance() {
                SqlCallDependencyProvenance::Table {
                    source, channels, ..
                } => {
                    assert!(std::ptr::eq(source, node));
                    assert_eq!(channels, arguments.as_ref());
                }
                _ => panic!("table loan must keep original relation"),
            }
            count += 1;
        }
    }
    assert!(count > 0);
    work.finish().unwrap();
}
#[test]
fn sql_call_dependency_loan_original_aggregate_all_phases_keep_source_and_state_channels() {
    let control = SqlCompileControl::unbounded();
    let owner =
        novarocks_sql::compiler::ordinary_union_source_for_test(Some((7, 11, 13)), true, &control)
            .unwrap();
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
                            AggregateRuntimeDemand::Update => updates += 1,
                            AggregateRuntimeDemand::ExpressionState(id) => {
                                assert_eq!(source.arguments.as_ref(), &[id]);
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

#[path = "sql_dependency_request_host_abort_tests.rs"]
mod sql_dependency_request_host_abort_tests;

#[path = "sql_dependency_original_binding_record_tests.rs"]
mod sql_dependency_original_binding_record_tests;
