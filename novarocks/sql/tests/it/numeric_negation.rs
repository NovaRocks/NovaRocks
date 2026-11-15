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
use super::fixtures::assert_original_same_owner;
use arrow::datatypes::DataType;
use novarocks_functions::{KernelEvaluationControl, KernelFailure};
use novarocks_physical_plan::{ExprKind, UnaryOperator};
use novarocks_sql::compiler::author_fragment_package_semantics;
use novarocks_sql_testkit::{sql_fixture_constant_policy as policy, sql_source};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::time::Duration;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("numeric transaction never waits")
    }
}
#[test]
fn numeric_unary_original_nonnull_sql_baseline_three_narrow_widths_remain_nonnullable() {
    for (ty, min, dtype) in [
        ("TINYINT", "-128", DataType::Int8),
        ("SMALLINT", "-32768", DataType::Int16),
        ("INT", "-2147483648", DataType::Int32),
    ] {
        let _ = (ty, min); // Values are runtime inputs, never used to manufacture plan types.
        let source = sql_source("SELECT -k AS original_negated FROM fixture", dtype.clone());
        let port = source.plan().result_port().unwrap();
        assert_original_same_owner(port, &source);
        assert_eq!(port.fields.len(), 1);
        assert_eq!(port.fields[0].alias.as_deref(), Some("original_negated"));
        assert_eq!(port.fields[0].ty.data_type().clone(), dtype);
        // A required signed input retains its declaration: MIN raises a
        // required row error rather than a successful own NULL.
        assert!(!port.fields[0].ty.nullable());
        let mut actual_minus = 0;
        for fragment in source.plan().fragments().values() {
            for (_, expr) in fragment.expressions().iter() {
                if matches!(
                    expr.kind,
                    ExprKind::Unary {
                        op: UnaryOperator::Minus,
                        ..
                    }
                ) {
                    actual_minus += 1;
                    assert_eq!(expr.ty.data_type().clone(), dtype);
                    assert!(!expr.ty.nullable());
                    let ExprKind::Unary { expr: input, .. } = &expr.kind else {
                        unreachable!();
                    };
                    let input = fragment.expressions().get(*input).unwrap();
                    assert_eq!(input.ty.data_type(), &dtype);
                    assert!(!input.ty.nullable());
                }
            }
        }
        assert!(
            actual_minus > 0,
            "fixture must retain the actual original Minus definition"
        );
    }
}
#[test]
fn numeric_unary_original_nonnull_sql_baseline_ordered_labels_and_values_are_actual_root() {
    let source = sql_source(
        "SELECT -k AS first_alias, k AS original_source, -k AS repeated_alias FROM fixture",
        DataType::Int8,
    );
    let port = source.plan().result_port().unwrap();
    assert_original_same_owner(port, &source);
    assert_eq!(port.fields.len(), 3);
    for (field, alias) in
        port.fields
            .iter()
            .zip(["first_alias", "original_source", "repeated_alias"])
    {
        assert_eq!(field.alias.as_deref(), Some(alias));
        assert_eq!(field.ty.data_type().clone(), DataType::Int8);
        assert!(!field.ty.nullable());
    }
    // Whether optimization reused an expression/ValueId is the actual plan's
    // choice. Each ordered output occurrence above is checked individually.
}

#[test]
fn numeric_unary_owned_transaction_actual_aggregate_window_and_table_sources_publish_new_canonical_requests()
 {
    // These are actual SQL-source and full source-journal/effect checks; their
    // separate operator/native execution acceptance is deliberately not claimed.
    for sql in [
        "SELECT COUNT(-k) AS observed FROM fixture",
        "SELECT COUNT(-k) OVER () AS observed FROM fixture",
        "SELECT SUM(-k) AS observed FROM fixture",
        "SELECT x FROM fixture, UNNEST([-k]) AS u(x)",
    ] {
        eprintln!("actual canonical state SQL: {sql}");
        let source = sql_source(sql, DataType::Int8);
        assert!(
            source
                .checked_original_result_declaration_observed(&Control)
                .unwrap()
                .is_some()
        );
        let semantics = author_fragment_package_semantics(&source, policy(), &Control).unwrap();
        assert_eq!(semantics.len(), source.plan().fragments().len());
        for fragment in source.plan().fragments().values() {
            for (_, expr) in fragment.expressions().iter() {
                if let ExprKind::Unary {
                    op: UnaryOperator::Minus,
                    ..
                } = &expr.kind
                {
                    assert!(!expr.ty.nullable());
                }
            }
        }
    }
}
