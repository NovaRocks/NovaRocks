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

//! Original call dependency loans from public SQL completion.

use crate::fixtures::{compile_table_free, expression_sources, session, text};
use arrow::datatypes::DataType;
use novarocks_expression_contract::{ExprKind, ExprNode};
use novarocks_functions::FunctionResultType;
use novarocks_physical_plan::{Fragment, PhysicalNode};
use novarocks_sql::binding::SqlResultConstraintOrigin;
use novarocks_sql::compiler::{
    SqlAuthoredPhysicalPlan, SqlCallDependencyProvenance, SqlCallDependencySite, SqlCompileControl,
    SqlExpressionCallKind, SqlSourceJournalError,
};
use novarocks_sql_testkit::sql_fixture_constant_policy;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::sync::{Arc, Mutex};

fn exact(sql: &str) -> SqlAuthoredPhysicalPlan {
    compile_table_free(
        sql,
        session(Default::default(), false),
        &SqlCompileControl::unbounded(),
    )
    .unwrap()
}

fn function_site(owner: &SqlAuthoredPhysicalPlan) -> (&Fragment, &PhysicalNode, &ExprNode) {
    expression_sources(owner)
        .find(|(_, _, source)| matches!(&source.kind, ExprKind::FunctionCall { .. }))
        .expect("actual SQL fixture retains a call")
}
#[test]
fn sql_call_dependency_loan_real_upper_keeps_original_unconstrained_and_constants() {
    let owner = exact("SELECT upper('kept') AS source_call");
    let (fragment, node, expression) = function_site(&owner);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let loan = owner
        .borrow_call_dependency_observed(
            SqlCallDependencySite::Expression {
                fragment,
                node,
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
    assert_eq!(
        loan.original_constant_policy(),
        sql_fixture_constant_policy()
    );
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
            fragment: actual_fragment,
            node: actual_node,
            source,
            kind,
            channels,
        } => {
            assert!(std::ptr::eq(actual_fragment, fragment));
            assert!(std::ptr::eq(actual_node, node));
            assert!(std::ptr::eq(source, expression));
            assert_eq!(kind, SqlExpressionCallKind::Scalar);
            let ExprKind::FunctionCall { args, .. } = &expression.kind else {
                unreachable!()
            };
            assert_eq!(channels, args.as_ref());
        }
        _ => panic!("expression provenance must not become relation provenance"),
    }
    let unit = node.expressions.as_deref().unwrap();
    let request = &unit.requests()[&expression.id];
    assert_eq!(request.constant_policy, loan.original_constant_policy());
    assert_eq!(
        request.logical_argument_count,
        loan.original_logical_argument_count()
    );
    assert!(request.expected_result_type.is_none());
    let [
        novarocks_physical_plan::StaticFunctionArgument::Value {
            constant: Some(reference),
            value_type: retained_type,
        },
    ] = request.arguments.as_ref()
    else {
        panic!("the published unit must keep the original constant request");
    };
    let retained = owner
        .plan()
        .constants()
        .resolve_source_observed(*reference, &mut work)
        .unwrap();
    assert_eq!(retained_type, value_type);
    assert_eq!(retained.value_type(), value.value_type());
    assert_eq!(retained.ordinal(), value.ordinal());
    assert!(Arc::ptr_eq(retained.pool().array(), value.pool().array()));
    work.finish().unwrap();
}
#[test]
fn sql_call_dependency_loan_empty_array_keeps_unknown_source_and_outer_json_target() {
    for sql in [
        "SELECT CAST(ARRAY[] AS ARRAY(JSON)) AS source_array",
        "SELECT CAST([] AS ARRAY<JSON>) AS source_array",
    ] {
        let owner = exact(sql);
        let (fragment, node, expression) = function_site(&owner);
        let control = SqlCompileControl::unbounded();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let loan = owner
            .borrow_call_dependency_observed(
                SqlCallDependencySite::Expression {
                    fragment,
                    node,
                    source: expression,
                },
                &mut work,
            )
            .unwrap();
        assert_eq!(
            loan.original_binding().result_constraint_origin(),
            &SqlResultConstraintOrigin::Unconstrained
        );
        assert!(loan.original_binding().result_constraint().is_none());
        assert!(!expression.ty.nullable());
        let DataType::List(source_item) = expression.ty.data_type() else {
            panic!("original independent ARRAY source")
        };
        assert_eq!(source_item.data_type(), &DataType::Null);
        assert!(source_item.is_nullable());
        assert_eq!(
            expression.ty.child_at(0).unwrap().logical_type(),
            ValueLogicalType::Physical
        );
        assert!(loan.original_arguments().is_empty());
        assert_eq!(loan.original_logical_argument_count(), 0);
        let canonical = loan.canonical().unwrap();
        assert!(canonical.request().expected_result_type.is_none());
        assert_eq!(
            canonical.selected().result_type,
            FunctionResultType::Scalar(expression.ty.clone())
        );
        let unit = node
            .expressions
            .iter()
            .find(|unit| {
                unit.definition(expression.id)
                    .is_some_and(|source| std::ptr::eq(source, expression))
            })
            .expect("the original ARRAY definition belongs to this node's expression unit");
        let casts = unit
            .definitions()
            .iter()
            .filter(|definition| {
                matches!(&definition.kind, ExprKind::Cast { expr, .. } if *expr == expression.id)
            })
            .collect::<Vec<_>>();
        let [cast] = casts.as_slice() else {
            panic!("one actual outer CAST of the original constructor")
        };
        let ExprKind::Cast {
            target,
            array_failure_mode,
            ..
        } = &cast.kind
        else {
            unreachable!()
        };
        assert_eq!(
            *array_failure_mode,
            Some(novarocks_type_contract::ArrayCastFailureMode::ReportError)
        );
        assert_eq!(target, cast.ty.data_type());
        let DataType::List(item) = cast.ty.data_type() else {
            panic!("outer complete JSON target")
        };
        assert_eq!(item.name(), "item");
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert!(item.is_nullable());
        let child = cast.ty.child_at(0).unwrap();
        assert_eq!(child.logical_type(), ValueLogicalType::Json);
        assert!(matches!(
            child.value_type().kind(),
            novarocks_type_contract::ValueKind::Nominal(ValueLogicalType::Json)
        ));
        assert_eq!(child.data_type(), item.data_type());
        assert_eq!(child.nullable(), item.is_nullable());
        assert!(item.metadata().as_arc().is_none());
        assert_eq!(owner.plan().result_port().unwrap().fields[0].ty, cast.ty);
        work.finish().unwrap();
    }
}

#[test]
fn sql_call_dependency_loan_original_foreign_and_missing_source_refusals_stay_typed() {
    let owner = exact("SELECT upper('kept')");
    let foreign = exact("SELECT upper('kept')");
    let (foreign_fragment, foreign_node, foreign_source) = function_site(&foreign);
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(matches!(
        owner.borrow_call_dependency_observed(
            SqlCallDependencySite::Expression {
                fragment: foreign_fragment,
                node: foreign_node,
                source: foreign_source,
            },
            &mut work
        ),
        Err(SqlSourceJournalError::InvalidSource(
            "expression journal loans a foreign plan or node"
        ))
    ));
    let (fragment, node, source) = function_site(&owner);
    assert_eq!(source.id, foreign_source.id);
    assert!(matches!(
        owner.borrow_call_dependency_observed(
            SqlCallDependencySite::Expression {
                fragment,
                node,
                source: foreign_source
            },
            &mut work
        ),
        Err(SqlSourceJournalError::InvalidSource(
            "expression journal loans a foreign unit definition"
        ))
    ));
    let literal = node
        .expressions
        .as_deref()
        .unwrap()
        .definitions()
        .iter()
        .find(|expression| matches!(expression.kind, ExprKind::Constant(_)))
        .expect("original argument expression");
    assert!(matches!(
        owner.borrow_call_dependency_observed(
            SqlCallDependencySite::Expression {
                fragment,
                node,
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
    let owner = exact("SELECT upper('kept')");
    let (fragment, node, source) = function_site(&owner);
    let success = JournalMeter::default();
    let mut work = CompileCheckpoints::try_new(&success, CompilePhase::Validate).unwrap();
    let _loan = owner
        .borrow_call_dependency_observed(
            SqlCallDependencySite::Expression {
                fragment,
                node,
                source,
            },
            &mut work,
        )
        .unwrap();
    // Entry and loan flushes, then two owner, three unit/entry/kind, three
    // origin/scope/count, and one original channel check. The caller owns finish.
    assert_eq!(&*success.events.lock().unwrap(), &[0, 0, 9]);
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
                matches!(owner.borrow_call_dependency_observed(SqlCallDependencySite::Expression { fragment, node, source }, &mut work), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            let events = meter.events.lock().unwrap().clone();
            assert!(
                matches!(owner.borrow_call_dependency_observed(SqlCallDependencySite::Expression { fragment, node, source }, &mut work), Err(SqlSourceJournalError::Control(actual)) if actual == cause)
            );
            assert_eq!(*meter.events.lock().unwrap(), events);
        }
    }
}
#[test]
fn sql_call_dependency_loan_real_table_unnest_retains_relation_and_channels() {
    let owner =
        exact("SELECT u.* FROM (SELECT ARRAY[1, 2] AS a) q CROSS JOIN UNNEST(q.a) AS u(value)");
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
                    let unit = node.expressions.as_deref().unwrap();
                    assert_eq!(channels.len(), arguments.len());
                    for (&channel, &root) in channels.iter().zip(arguments.iter()) {
                        assert_eq!(channel, unit.root(root).unwrap().definition);
                    }
                }
                _ => panic!("table loan must keep original relation"),
            }
            count += 1;
        }
    }
    assert!(count > 0);
    work.finish().unwrap();
}
