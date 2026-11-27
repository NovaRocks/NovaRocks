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

use arrow::datatypes::{DataType, Field};
use novarocks_expression_contract::ExprKind;
use novarocks_physical_plan::{HashJoinCondition, NodeKind};
use novarocks_sql::analyze_error::AnalyzeErrorKind;
use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError};
use novarocks_sql_testkit::{sql_source, try_sql_source_with_declared_fields};
use novarocks_type_contract::{JoinKeyMatch, OperatorFunction, TrinoCastMode};

use super::fixtures::expression_sources;

#[test]
fn numeric_operators_preserve_explicit_selected_width_and_expression_identity() {
    let source = sql_source(
        "SELECT k + DECIMAL '1.25' AS sum, k IS DISTINCT FROM DECIMAL '0.00' AS different FROM fixture",
        DataType::Decimal128(7, 3),
    );
    let result = source.plan().result_port().unwrap();
    assert_eq!(result.fields[0].ty.data_type(), &DataType::Decimal128(8, 3));
    assert_eq!(result.fields[1].ty.data_type(), &DataType::Boolean);
    assert!(!result.fields[1].ty.nullable());
    let ops: Vec<_> = expression_sources(&source)
        .filter_map(|(_, _, node)| match &node.kind {
            ExprKind::FunctionCall { function, args } if args.len() == 2 => {
                OperatorFunction::from_function_id(&function.function_id)
            }
            _ => None,
        })
        .collect();
    assert!(ops.contains(&OperatorFunction::Add));
    assert!(ops.contains(&OperatorFunction::Distinct));
}

#[test]
fn numeric_comparison_requires_an_explicit_text_to_integer_cast() {
    let rejected = try_sql_source_with_declared_fields(
        "SELECT k > 5 FROM fixture",
        &[Field::new("k", DataType::Utf8, true)],
    );
    assert!(matches!(
        rejected,
        Err(SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)))
            if error.kind() == AnalyzeErrorKind::TypeMismatch
    ));

    let source = sql_source("SELECT CAST(k AS INTEGER) > 5 FROM fixture", DataType::Utf8);
    assert!(expression_sources(&source).any(|(_, node, expression)| {
        let ExprKind::FunctionCall { function, args } = &expression.kind else {
            return false;
        };
        if function.function_id != OperatorFunction::LessThan.function_id() {
            return false;
        }
        let converted = node
            .expressions
            .as_ref()
            .unwrap()
            .definition(args[1])
            .unwrap();
        matches!(
            &converted.kind,
            ExprKind::Cast {
                target: DataType::Int32,
                cast_mode: TrinoCastMode::Explicit,
                ..
            }
        )
    }));
}

#[test]
fn numeric_operators_freeze_ordinary_and_null_safe_double_join_relations() {
    for (relation, null_safe, predicate) in [
        (JoinKeyMatch::IeeeFloat, false, "l.k = r.k"),
        (JoinKeyMatch::Grouping, true, "l.k IS NOT DISTINCT FROM r.k"),
    ] {
        let source = sql_source(
            &format!("SELECT l.k, r.k FROM fixture l FULL JOIN fixture r ON {predicate}"),
            DataType::Float64,
        );
        let installed = source
            .plan()
            .fragments()
            .values()
            .flat_map(|fragment| {
                fragment
                    .nodes()
                    .values()
                    .filter_map(|node| match &node.kind {
                        NodeKind::HashJoin {
                            condition: HashJoinCondition::Ordinary { keys, .. },
                            ..
                        } => Some(keys.iter()),
                        _ => None,
                    })
            })
            .flatten()
            .any(|key| key.null_safe == null_safe && key.match_relation == relation);
        assert!(
            installed,
            "expected original frozen match relationship for {predicate}"
        );
    }
}

#[test]
fn numeric_operators_are_valid_distinct_aggregate_arguments() {
    let source = sql_source(
        "SELECT COUNT(DISTINCT k % 3), SUM(DISTINCT k + 1), AVG(DISTINCT k * 2) FROM fixture",
        DataType::Int32,
    );
    let plan = source.plan();
    let aggregates: Vec<_> = plan
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment
                .nodes()
                .values()
                .filter_map(|node| match &node.kind {
                    NodeKind::Aggregate { calls, .. } => Some(calls.iter()),
                    _ => None,
                })
                .flatten()
        })
        .collect();
    for (function, arithmetic) in [
        (
            "builtin.aggregate/multi_distinct_count/v1",
            OperatorFunction::Modulo,
        ),
        (
            "builtin.aggregate/multi_distinct_sum/v1",
            OperatorFunction::Add,
        ),
        (
            "builtin.aggregate/multi_distinct_avg/v1",
            OperatorFunction::Multiply,
        ),
    ] {
        assert!(
            aggregates
                .iter()
                .any(|call| call.binding.function.function_id.as_str() == function),
            "actual DISTINCT aggregate identity {function}"
        );
        assert!(expression_sources(&source).any(|(_, _, node)| matches!(
            &node.kind, ExprKind::FunctionCall { function, .. } if function.function_id == arithmetic.function_id()
        )));
    }
}
