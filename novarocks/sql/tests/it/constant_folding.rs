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
use novarocks_expression_contract::{ExprId, ExprKind, UnaryOperator};
use novarocks_functions::ConstantValue;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlCompileControl};
use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings};
use novarocks_sql_testkit::sql_fixture_constant_policy;
use novarocks_type_contract::{
    CompilePhase, OperatorFunction, RegexpCountPatternSource, ValueLogicalType,
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

fn binary_operands(kind: &ExprKind, operator: OperatorFunction) -> Option<[ExprId; 2]> {
    let ExprKind::FunctionCall { function, args } = kind else {
        return None;
    };
    let &[left, right] = args.as_ref() else {
        return None;
    };
    (OperatorFunction::from_function_id(&function.function_id) == Some(operator))
        .then_some([left, right])
}

#[test]
fn folds_int32_addition_without_an_integer_carrier_roundtrip() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile("SELECT CAST(1 AS INT) + CAST(1 AS INT)", &probe);
    let receipts = probe.receipts.lock().unwrap();
    let (receipt, [left, right]) = receipts
        .iter()
        .find_map(|receipt| {
            binary_operands(&receipt.root().kind, OperatorFunction::Add).map(|args| (receipt, args))
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
    assert!(
        !expression_sources(&owner).any(|(_, _, node)| binary_operands(
            &node.kind,
            OperatorFunction::Add
        )
        .is_some())
    );
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
        TRY_CAST(CAST(99999.999 AS DECIMAL(8,3)) AS DECIMAL(7,2))",
        &probe,
    );
    let receipts = probe.receipts.lock().unwrap();
    let multiplication = receipts
        .iter()
        .find(|receipt| binary_operands(&receipt.root().kind, OperatorFunction::Multiply).is_some())
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
fn folds_successful_negation_and_keeps_the_overflowing_call() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile("SELECT -(7), -(-9223372036854775807 - 1), NOT true", &probe);
    let receipts = probe.receipts.lock().unwrap();
    let operator = novarocks_type_contract::OperatorFunction::Negation;
    let is_minus = |kind: &ExprKind| {
        matches!(kind, ExprKind::FunctionCall { function, .. }
            if novarocks_type_contract::OperatorFunction::from_function_id(&function.function_id) == Some(operator))
    };
    let success = receipts
        .iter()
        .find(|receipt| {
            is_minus(&receipt.root().kind) && receipt.root().ty.data_type() == &DataType::Int32
        })
        .expect("production fold candidate for INTEGER negation");
    let folded = success.result.as_ref().expect("accepted negative constant");
    assert_eq!(folded.value_type().data_type(), &DataType::Int32);
    assert_eq!(folded.try_i64().unwrap(), Some(-7));
    let rejected = receipts
        .iter()
        .find(|receipt| {
            is_minus(&receipt.root().kind) && receipt.root().ty.data_type() == &DataType::Int64
        })
        .expect("production fold attempted negation of BIGINT MIN");
    assert!(rejected.result.is_none());
    let final_negations = expression_sources(&owner)
        .filter(|(_, _, node)| is_minus(&node.kind))
        .map(|(_, _, node)| node.ty.data_type().clone())
        .collect::<Vec<_>>();
    assert_eq!(final_negations, [DataType::Int64]);
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
fn standard_decimal_overflow_remains_an_unfolded_call_in_both_sql_modes() {
    let sql = "SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) + 1";
    for mode in ["", "ERROR_IF_OVERFLOW"] {
        let probe = Arc::new(FoldProbe::default());
        let semantics =
            SqlSemanticSettings::default().with_sql_mode(SqlMode::from_assignment(mode));
        let owner = compile_table_free(sql, session(semantics, true), &probe.control()).unwrap();
        let receipts = probe.receipts.lock().unwrap();
        let addition = receipts
            .iter()
            .find(|receipt| binary_operands(&receipt.root().kind, OperatorFunction::Add).is_some())
            .unwrap();
        assert!(addition.result.is_none());
        assert!(
            expression_sources(&owner).any(|(_, _, node)| binary_operands(
                &node.kind,
                OperatorFunction::Add
            )
            .is_some())
        );
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
fn public_sql_map_key_transition_reports_uninstalled_conversion_during_analysis() {
    use novarocks_sql::analyze_error::AnalyzeErrorKind;
    use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError};

    let probe = Arc::new(FoldProbe::default());
    let error = compile_table_free(
        "SELECT map_agg(CAST(1 AS INTEGER),'one')[1e0]",
        session(Default::default(), true),
        &probe.control(),
    )
    .unwrap_err();
    let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = error else {
        panic!("expected a typed analysis error: {error}");
    };
    assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
    assert!(error.message().contains("Cannot cast Map"));
    assert!(error.span().is_some());
}

#[test]
fn public_sql_map_float_probe_keeps_nullable_try_cast_before_optional_folding() {
    for (sql, null_index) in [
        (
            "SELECT map_agg(CAST(1 AS INT),'one')[TRY_CAST(1e0 AS INTEGER)]",
            false,
        ),
        (
            "SELECT map_agg(CAST(1 AS INT),'one')[TRY_CAST(1e100 AS INTEGER)]",
            true,
        ),
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
                    cast_mode: novarocks_type_contract::TrinoCastMode::Try,
                } if receipt.unit.definition(*expr).unwrap().ty.data_type()
                    == &DataType::Float64 =>
                {
                    Some((receipt, *expr))
                }
                _ => None,
            })
            .expect("the original float TRY_CAST reaches optional folding");
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
