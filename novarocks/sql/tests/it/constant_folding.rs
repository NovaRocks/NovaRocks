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

//! Optional folding through ordinary SQL completion.

use crate::fixtures::{compile_table_free, expression_sources, session, text};
use crate::fold_observation::FoldProbe;
use arrow::array::Decimal128Array;
use arrow::datatypes::DataType;
use novarocks_expression_contract::{BinaryOperator, ExprKind, UnaryOperator};
use novarocks_functions::{ConstantValue, FunctionResultType};
use novarocks_sql::binding::SqlResultConstraintOrigin;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlCallDependencySite, SqlCompileControl};
use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings};
use novarocks_sql_testkit::sql_fixture_constant_policy;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, DecimalOverflowPolicy, RegexpCountPatternSource,
    ValueLogicalType,
};
use std::sync::Arc;

fn compile(sql: &str, probe: &Arc<FoldProbe>) -> SqlAuthoredPhysicalPlan {
    compile_table_free(sql, session(Default::default(), true), &probe.control()).unwrap()
}

fn is_null(value: &ConstantValue) -> bool {
    value
        .is_null_observed(CompilePhase::Validate, &SqlCompileControl::unbounded())
        .unwrap()
}

#[test]
fn folds_int32_addition_without_an_integer_carrier_roundtrip() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile("SELECT CAST(1 AS INT) + CAST(1 AS INT)", &probe);
    let receipts = probe.receipts.lock().unwrap();
    let (receipt, left, right) = receipts
        .iter()
        .find_map(|receipt| match receipt.root().kind {
            ExprKind::Binary {
                left,
                op: BinaryOperator::Add,
                right,
                ..
            } => Some((receipt, left, right)),
            _ => None,
        })
        .unwrap();
    for input in [left, right] {
        let value = receipt.constant(input);
        assert_eq!(value.value_type().data_type(), &DataType::Int32);
        assert_eq!(value.try_i64().unwrap(), Some(1));
    }
    assert_eq!(receipt.root().ty.data_type(), &DataType::Int32);
    let result = receipt.result.as_ref().unwrap();
    assert_eq!(result.value_type().data_type(), &DataType::Int32);
    assert_eq!(result.try_i64().unwrap(), Some(2));
    assert!(!expression_sources(&owner).any(|(_, _, node)| matches!(
        node.kind,
        ExprKind::Binary {
            op: BinaryOperator::Add,
            ..
        }
    )));
}

#[test]
fn folds_date_format_and_utf8_to_date32_through_real_kernels() {
    let probe = Arc::new(FoldProbe::default());
    compile(
        "SELECT date_format(CAST('2020-01-01' AS DATE), '%Y-%m-%d')",
        &probe,
    );
    let receipts = probe.receipts.lock().unwrap();
    let cast = receipts
        .iter()
        .find(|receipt| receipt.root().ty.data_type() == &DataType::Date32)
        .unwrap();
    assert_eq!(
        cast.result.as_ref().unwrap().try_date32().unwrap(),
        Some(18262)
    );
    let formatted = receipts
        .iter()
        .find(|receipt| {
            matches!(&receipt.root().kind, ExprKind::FunctionCall { function, .. }
            if function.function_id.as_str().starts_with("builtin.scalar/date_format/"))
        })
        .unwrap();
    assert_eq!(text(formatted.result.as_ref().unwrap()), "2020-01-01");
}

fn decimal_coefficient(value: &ConstantValue) -> i128 {
    value
        .pool()
        .array()
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(value.ordinal() as usize)
}

#[test]
fn folds_decimal_multiplication_and_checked_rounding_cast_with_exact_coefficients() {
    let probe = Arc::new(FoldProbe::default());
    compile(
        "SELECT CAST(1.25 AS DECIMAL(10,2)) * CAST(4.00 AS DECIMAL(10,2)),
        CAST(CAST(99999.999 AS DECIMAL(8,3)) AS DECIMAL(8,2)),
        CAST(CAST(99999.999 AS DECIMAL(8,3)) AS DECIMAL(7,2))",
        &probe,
    );
    let receipts = probe.receipts.lock().unwrap();
    let multiplication = receipts
        .iter()
        .find(|receipt| {
            matches!(
                receipt.root().kind,
                ExprKind::Binary {
                    op: BinaryOperator::Multiply,
                    ..
                }
            )
        })
        .unwrap();
    assert_eq!(
        decimal_coefficient(multiplication.result.as_ref().unwrap()),
        50_000
    );
    let selected =
        |target: DataType| {
            receipts.iter().find(|receipt| matches!(
            &receipt.root().kind, ExprKind::Cast { target: actual, .. } if actual == &target
        )).unwrap().result.as_ref().unwrap()
        };
    assert_eq!(
        decimal_coefficient(selected(DataType::Decimal128(8, 2))),
        10_000_000
    );
    assert!(is_null(selected(DataType::Decimal128(7, 2))));
}

#[test]
fn declines_unary_minus_without_a_fake_kernel() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile("SELECT -CAST(7 AS INT), NOT true", &probe);
    let receipts = probe.receipts.lock().unwrap();
    let is_minus = |kind: &ExprKind| {
        matches!(
            kind,
            ExprKind::Unary {
                op: UnaryOperator::Minus,
                ..
            }
        )
    };
    assert!(expression_sources(&owner).any(|(_, _, node)| is_minus(&node.kind)));
    assert!(
        receipts
            .iter()
            .all(|receipt| !is_minus(&receipt.root().kind))
    );
    let negation = receipts
        .iter()
        .find(|receipt| {
            matches!(
                receipt.root().kind,
                ExprKind::Unary {
                    op: UnaryOperator::Not,
                    ..
                }
            )
        })
        .unwrap();
    assert_eq!(
        negation.result.as_ref().unwrap().try_boolean().unwrap(),
        Some(false)
    );
    let Err(novarocks_sql::compiler::SqlCompileProgressError::Compile(
        novarocks_sql::compiler::SqlCompileError::Analyze(error),
    )) = compile_table_free(
        "SELECT no_such_novarocks_function(1)",
        session(Default::default(), true),
        &SqlCompileControl::unbounded(),
    )
    else {
        panic!("an unbound function must fail in actual SQL analysis");
    };
    assert!(error.message().contains("no_such_novarocks_function"));
}

#[test]
fn checked_decimal_error_remains_ordinary_evaluation_while_policy_null_is_success() {
    let sql = "SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) + 1";
    for (mode, policy) in [
        ("", DecimalOverflowPolicy::OutputNull),
        ("ERROR_IF_OVERFLOW", DecimalOverflowPolicy::ReportError),
    ] {
        let probe = Arc::new(FoldProbe::default());
        let semantics =
            SqlSemanticSettings::default().with_sql_mode(SqlMode::from_assignment(mode));
        let owner = compile_table_free(sql, session(semantics, true), &probe.control()).unwrap();
        let receipts = probe.receipts.lock().unwrap();
        let addition = receipts
            .iter()
            .find(|receipt| {
                matches!(
                    receipt.root().kind,
                    ExprKind::Binary {
                        op: BinaryOperator::Add,
                        ..
                    }
                )
            })
            .unwrap();
        let ExprKind::Binary {
            decimal_overflow_policy,
            ..
        } = addition.root().kind
        else {
            unreachable!()
        };
        assert_eq!(decimal_overflow_policy, policy);
        let retains_addition = expression_sources(&owner).any(|(_, _, node)| {
            matches!(
                node.kind,
                ExprKind::Binary {
                    op: BinaryOperator::Add,
                    ..
                }
            )
        });
        if policy == DecimalOverflowPolicy::OutputNull {
            assert!(is_null(addition.result.as_ref().unwrap()));
            assert!(!retains_addition);
        } else {
            assert!(addition.result.is_none());
            assert!(retains_addition);
        }
    }
}

#[test]
fn regexp_count_real_sql_fold_author_attaches_existing_emitted_source_before_error_decline() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile("SELECT regexp_count('test string','[0-9')", &probe);
    let receipts = probe.receipts.lock().unwrap();
    let calls = receipts
        .iter()
        .filter(|receipt| receipt.is_call("builtin.scalar/regexp_count/v1"))
        .collect::<Vec<_>>();
    assert_eq!(calls.len(), 1);
    let receipt = calls[0];
    assert_eq!(
        receipt.unit.claims()[&receipt.root_use()].regexp_count_pattern_source,
        Some(RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent),
    );
    assert!(receipt.result.is_none());
    assert!(
        expression_sources(&owner).any(|(_, _, expression)| matches!(
            &expression.kind, ExprKind::FunctionCall { function, .. }
                if function.function_id.as_str() == "builtin.scalar/regexp_count/v1"
        ))
    );
}

#[test]
fn sql_fold_dependency_ds_scalar_actual_literal_three_arity_exact_source() {
    const FUNCTION: &str = "builtin.scalar/ds_hll_count_distinct_state/v1";
    for (count, sql) in [
        (1, "SELECT ds_hll_count_distinct_state(7) AS folded"),
        (2, "SELECT ds_hll_count_distinct_state(7,10) AS folded"),
        (
            3,
            "SELECT ds_hll_count_distinct_state(7,10,'HLL_8') AS folded",
        ),
    ] {
        let probe = Arc::new(FoldProbe::default());
        let control = probe.control();
        let owner = compile_table_free(sql, session(Default::default(), true), &control).unwrap();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let retained = expression_sources(&owner)
            .filter(|(_, _, expression)| {
                matches!(
                    &expression.kind, ExprKind::FunctionCall { function, args }
                        if function.function_id.as_str() == FUNCTION && args.len() == count
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            retained.len(),
            1,
            "opaque state must keep the executable call"
        );
        let (fragment, node, source) = retained[0];
        let loan = owner
            .borrow_call_dependency_observed(
                SqlCallDependencySite::Expression {
                    fragment,
                    node,
                    source,
                },
                &mut work,
            )
            .unwrap();
        assert_eq!(loan.original_logical_argument_count(), count);
        assert_eq!(loan.original_arguments().len(), count);
        assert!(loan.original_binding().result_constraint().is_none());
        assert_eq!(
            loan.original_binding().result_constraint_origin(),
            &SqlResultConstraintOrigin::Unconstrained
        );
        let canonical = loan.canonical().unwrap();
        assert_eq!(source.ty.data_type(), &DataType::Binary);
        assert_eq!(
            canonical.selected().result_type,
            FunctionResultType::Scalar(source.ty.clone())
        );
        for arg in loan.original_arguments() {
            let novarocks_functions::FunctionArgument::Value {
                value_type,
                constant: Some(value),
            } = arg
            else {
                panic!("the exact source keeps the constant input");
            };
            assert_eq!(value.value_type(), value_type);
        }
        work.finish().unwrap();
        assert!(
            probe
                .receipts
                .lock()
                .unwrap()
                .iter()
                .all(|receipt| { !receipt.is_call(FUNCTION) || receipt.result.is_none() })
        );
    }
}

#[test]
fn public_sql_map_float_probe_keeps_nullable_cast_before_optional_folding() {
    for (sql, null_index) in [
        ("SELECT map_agg(CAST(1 AS INT),'one')[1e0]", false),
        ("SELECT map_agg(CAST(1 AS INT),'one')[1e100]", true),
    ] {
        let probe = Arc::new(FoldProbe::default());
        let owner = compile(sql, &probe);
        let receipts = probe.receipts.lock().unwrap();
        let (receipt, argument) = receipts
            .iter()
            .find_map(|receipt| match &receipt.root().kind {
                ExprKind::Cast {
                    expr,
                    target: DataType::Int32,
                    ..
                } if receipt.unit.definition(*expr).unwrap().ty.data_type()
                    == &DataType::Float64 =>
                {
                    Some((receipt, *expr))
                }
                _ => None,
            })
            .expect("the original float CAST reaches optional folding");
        assert_eq!(receipt.root().ty.logical_type(), ValueLogicalType::Physical);
        assert!(receipt.root().ty.nullable());
        assert!(!receipt.constant(argument).value_type().nullable());
        let value = receipt.result.as_ref().unwrap();
        assert_eq!(is_null(value), null_index);
        let result = owner.plan().result_port().unwrap();
        assert_eq!(result.fields.len(), 1);
        assert_eq!(result.fields[0].ty.data_type(), &DataType::Utf8);
        assert_eq!(
            result.fields[0].ty.logical_type(),
            ValueLogicalType::Physical
        );
        assert!(result.fields[0].ty.nullable());
        owner
            .require_statement_constant_policy(sql_fixture_constant_policy())
            .unwrap();
        for fragment in owner.plan().fragments().values() {
            fragment
                .calls()
                .validate_fragment(fragment, &SqlCompileControl::unbounded())
                .unwrap();
        }
        novarocks_physical_plan::validate_plan(owner.plan()).unwrap();
    }
}

#[test]
fn e08s1_table_fold_original_source_keeps_real_child_evaluation() {
    let probe = Arc::new(FoldProbe::default());
    let catalog =
        crate::fixtures::table_parent::source(crate::fixtures::table_parent::Outcome::Allow);
    crate::fixtures::table_parent::optimize(&catalog, &probe).unwrap();
    let receipts = probe.receipts.lock().unwrap();
    assert!(
        receipts.iter().any(|receipt| {
            receipt.is_call("builtin.scalar/reverse/v1")
                && receipt
                    .result
                    .as_ref()
                    .is_some_and(|result| text(result) == "cba")
        }),
        "the real argument folds after Table parent admission"
    );
}

#[test]
fn e08s1_fold_environment_exact_original_concat_raw_limits_keep_child_fold() {
    for raw in [-1, 4096] {
        let semantics = SqlSemanticSettings::default().with_group_concat_max_len(raw);
        let probe = Arc::new(FoldProbe::default());
        crate::fixtures::optimize_table_free(
            "SELECT GROUP_CONCAT(REVERSE('abc'))",
            session(semantics, true),
            novarocks_sql::compiler::builtin_sql_function_catalog(),
            &probe.control(),
        )
        .unwrap();
        let receipts = probe.receipts.lock().unwrap();
        assert!(
            receipts.iter().any(|receipt| {
                receipt.is_call("builtin.scalar/reverse/v1")
                    && receipt
                        .result
                        .as_ref()
                        .is_some_and(|result| text(result) == "cba")
            }),
            "the original raw limit has a genuine author"
        );
    }
}
