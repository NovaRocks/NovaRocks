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
use crate::fixtures::{compile_table_free, expression_sources, session};
use arrow::datatypes::{DataType, Field};
use novarocks_expression_contract::{ExprKind, ExprNode, UnaryOperator};
use novarocks_sql::{
    analyze_error::AnalyzeErrorKind,
    compiler::{
        SqlAuthoredPhysicalPlan, SqlCompileControl, SqlCompileError, SqlCompileProgressError,
    },
};
use novarocks_sql_testkit::{sql_source_with_single_field, try_sql_source_with_declared_fields};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, OperatorFunction};

fn boolean_constant(plan: &SqlAuthoredPhysicalPlan, node: &ExprNode) -> Option<bool> {
    let ExprKind::Constant(reference) = node.kind else {
        panic!("expected a Boolean constant: {node:?}");
    };
    let control = SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let value = plan
        .plan()
        .constants()
        .resolve_observed(reference, &node.ty, &mut work)
        .unwrap();
    work.finish().unwrap();
    value.try_boolean().unwrap()
}

#[test]
fn truth_tests_expand_to_existing_nonnullable_boolean_primitives() {
    for (test, truth, negated) in [
        ("IS TRUE", Some(true), false),
        ("IS NOT TRUE", Some(true), true),
        ("IS FALSE", Some(false), false),
        ("IS NOT FALSE", Some(false), true),
        ("IS UNKNOWN", None, false),
        ("IS NOT UNKNOWN", None, true),
    ] {
        for input in ["k", "NULL"] {
            let sql = format!("SELECT {input} {test} AS tested FROM fixture");
            let plan = sql_source_with_single_field(&sql, Field::new("k", DataType::Boolean, true));
            let [result] = plan.plan().result_port().unwrap().fields.as_ref() else {
                panic!("expected one result: {sql}");
            };
            assert_eq!(result.ty.data_type(), &DataType::Boolean, "{sql}");
            assert!(!result.ty.nullable(), "{sql}");
            assert!(
                !expression_sources(&plan)
                    .any(|(_, _, node)| matches!(node.kind, ExprKind::IsTruthValue { .. })),
                "{sql}"
            );

            let (unit, root) = plan
                .plan()
                .fragments()
                .values()
                .flat_map(|fragment| fragment.nodes().values())
                .filter_map(|node| node.expressions.as_deref())
                .flat_map(|unit| unit.roots().iter().map(move |root| (unit, root)))
                .map(|(unit, root)| (unit, unit.definition(root.definition).unwrap()))
                .find(|(_, node)| {
                    matches!(
                        &node.kind,
                        ExprKind::FunctionCall { function, .. }
                            if function.function_id == OperatorFunction::Identical.function_id()
                    ) || matches!(
                        node.kind,
                        ExprKind::Unary {
                            op: UnaryOperator::Not,
                            ..
                        } | ExprKind::IsNull { .. }
                    )
                })
                .unwrap_or_else(|| panic!("missing expanded predicate: {sql}"));
            assert!(!root.ty.nullable(), "{sql}");

            // The expanded predicate has exactly one edge to the operand.
            let operand = if let Some(expected) = truth {
                let comparison = if negated {
                    let ExprKind::Unary {
                        op: UnaryOperator::Not,
                        expr,
                    } = root.kind
                    else {
                        panic!("expected NOT: {sql}");
                    };
                    unit.definition(expr).unwrap()
                } else {
                    root
                };
                let ExprKind::FunctionCall { function, args } = &comparison.kind else {
                    panic!("expected bound null-safe equality: {sql}");
                };
                assert_eq!(
                    function.function_id,
                    OperatorFunction::Identical.function_id(),
                    "{sql}"
                );
                let &[left, right] = args.as_ref() else {
                    panic!("expected both original operands: {sql}");
                };
                assert!(!comparison.ty.nullable(), "{sql}");
                assert_ne!(left, right, "{sql}");
                assert_eq!(
                    boolean_constant(&plan, unit.definition(right).unwrap()),
                    Some(expected)
                );
                left
            } else {
                let ExprKind::IsNull {
                    expr,
                    negated: actual,
                } = root.kind
                else {
                    panic!("expected a NULL test: {sql}");
                };
                assert_eq!(actual, negated, "{sql}");
                expr
            };

            let operand = unit.definition(operand).unwrap();
            assert_eq!(operand.ty.data_type(), &DataType::Boolean, "{sql}");
            assert!(operand.ty.nullable(), "{sql}");
            if input == "NULL" {
                assert_eq!(boolean_constant(&plan, operand), None, "{sql}");
            } else {
                assert!(matches!(operand.kind, ExprKind::Value(_)), "{sql}");
            }
        }
    }
}

#[test]
fn truth_tests_reject_non_boolean_operands_during_analysis() {
    for (test, data_type) in [
        ("IS TRUE", DataType::Int32),
        ("IS NOT UNKNOWN", DataType::Utf8),
    ] {
        let failure = try_sql_source_with_declared_fields(
            &format!("SELECT k {test} AS tested FROM fixture"),
            &[Field::new("k", data_type, false)],
        )
        .unwrap_err();
        let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = failure else {
            panic!("expected analysis type error: {failure:?}");
        };
        assert_eq!(error.kind(), AnalyzeErrorKind::TypeMismatch);
        assert_eq!(
            error.code(),
            AnalyzeErrorKind::TypeMismatch.descriptor().code
        );
        assert!(error.span().is_some());
    }
}

#[test]
fn composite_in_short_circuit_freezes_a_searched_case_and_nullable_mark() {
    let sql = "SELECT guard OR (ARRAY[CAST(v AS INTEGER)] IN (SELECT ARRAY[1])) AS mark \
        FROM (VALUES (TRUE, 'bad integer'), (FALSE, '1')) AS t(guard, v)";
    let control = SqlCompileControl::unbounded();
    let compiled = compile_table_free(sql, session(Default::default(), true), &control).unwrap();
    assert!(compiled.plan().fragments().values().any(|fragment| {
        fragment.nodes().values().any(|node| {
            matches!(
                node.kind,
                novarocks_physical_plan::NodeKind::NestLoopJoin {
                    kind: novarocks_physical_plan::JoinKind::LeftMark,
                    predicate: Some(_),
                    ..
                }
            )
        })
    }));
    assert!(expression_sources(&compiled).any(|(_, _, node)| {
        matches!(
            node.kind,
            ExprKind::Case {
                operand: None,
                ieee_float_equality: false,
                ..
            }
        )
    }));
}
