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

//! Actual retained statistics SQL source and permanent package compilation expectations.
use super::filter_conjunction_actual_sql_compiler_tests::compiler_results;
use super::ndv_filter_actual_sql_source_tests::sql_source_with_columns;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use arrow::datatypes::DataType;
use novarocks_expression_contract::ExprKind;
use novarocks_sql::compiler::SqlAuthoredPhysicalPlan;
use novarocks_type_contract::{ValueLogicalType, arithmetic_result_value_type_with_op};
fn statement(text: &str, query: u32) -> String {
    let marker = format!("-- query {query}\n");
    let start = text.find(&marker).unwrap() + marker.len();
    let rest = &text[start..];
    let end = rest.find("-- query ").unwrap_or(rest.len());
    let slice = &rest[..end];
    slice
        .split(';')
        .map(|part| {
            part.lines()
                .filter(|line| !line.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_owned()
        })
        .find(|part| part.to_ascii_lowercase().starts_with("select"))
        .unwrap()
        .replace("${case_db}", "fixture")
}
fn sources() -> Vec<SqlAuthoredPhysicalPlan> {
    let statistic = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/sql/correctness/aggregate/sql/agg_test_statistic.sql"
    ));
    let nullable = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/sql/correctness/aggregate/sql/agg_statistic_null_window_semantics.sql"
    ));
    let fields = [
        ("no", DataType::Int32),
        ("k", DataType::Decimal128(10, 2)),
        ("v", DataType::Decimal128(10, 2)),
    ];
    [statement(statistic, 8), statement(nullable, 2)]
        .into_iter()
        .map(|sql| sql_source_with_columns(&sql, &fields))
        .collect()
}
fn authored() {
    for (case, source) in sources().into_iter().enumerate() {
        let mut float = 0;
        for fragment in source.plan().fragments().values() {
            for (_source_node, unit, id, expr) in super::sql_test_source::unit_definitions(fragment)
            {
                let ExprKind::Binary {
                    op,
                    left,
                    right,
                    decimal_overflow_policy,
                    allow_throw_exception,
                } = &expr.kind
                else {
                    continue;
                };
                let Some(operator) = op.arithmetic_operator() else {
                    continue;
                };
                let left = &unit.definition(*left).unwrap().ty;
                let right = &unit.definition(*right).unwrap().ty;
                eprintln!(
                    "statistics original arithmetic case={case} mode=compiled fragment={:?} definition={id:?} operator={operator:?} left={left:?} right={right:?} result={:?} policy={decimal_overflow_policy:?} allow_source={allow_throw_exception:?}",
                    fragment.id(),
                    expr.ty
                );
                let mut expected =
                    arithmetic_result_value_type_with_op(left, right, operator).unwrap();
                expected = expected
                    .with_nullable(true)
                    .expect("valid test root nullability");
                assert_eq!(expr.ty, expected);
                assert!(allow_throw_exception.is_some());
                if matches!(left.data_type(), DataType::Float32 | DataType::Float64)
                    || matches!(right.data_type(), DataType::Float32 | DataType::Float64)
                {
                    assert_eq!(expr.ty.data_type().clone(), DataType::Float64);
                    assert_eq!(expr.ty.logical_type(), ValueLogicalType::Physical);
                    float += 1;
                }
            }
        }
        assert!(
            float > 0,
            "actual retained SQL must expose its Float arithmetic dependencies"
        );
    }
}

#[test]
fn float_arithmetic_statistics_actual_candidate_sql_source() {
    authored()
}
fn completed() {
    let functions = installed_builtin_owner_catalogue();
    for source in sources() {
        for (id, result) in compiler_results(&source, &functions) {
            result.unwrap_or_else(|error| {
                panic!("statistics complete package fragment {id:?}: {error}")
            });
        }
    }
}

#[test]
fn float_arithmetic_statistics_actual_candidate_sql_complete_compilation() {
    completed()
}
