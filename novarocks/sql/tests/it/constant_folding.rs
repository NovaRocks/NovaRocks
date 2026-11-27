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

use crate::fixtures::{
    compile_table_free, expression_sources, session, table_free_request_with_constant_policy, text,
};
use crate::fold_observation::FoldProbe;
use arrow::array::Decimal128Array;
use arrow::datatypes::DataType;
use novarocks_expression_contract::{ExprId, ExprKind};
use novarocks_functions::ConstantValue;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlCompileControl};
use novarocks_sql::sql_mode::{SqlMode, SqlSemanticSettings};
use novarocks_type_contract::{ArgumentControl, CompilePhase, OperatorFunction, ValueLogicalType};
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

#[test]
fn date_format_folds_representable_output_with_input_derived_validation() {
    const FORMAT: &str = "builtin.scalar/date_format/v1";
    let sql = format!(
        "SELECT date_format(TIMESTAMP '2024-09-01 12:00:00.000', '{}')",
        "%M".repeat(96)
    );
    let expected = "September".repeat(96);
    let fold = Arc::new(FoldProbe::default());
    let control = fold.control();
    let owner = table_free_request_with_constant_policy(
        &sql,
        session(Default::default(), true),
        &control,
        novarocks_functions::ConstantValidation::INPUT_DERIVED,
    )
    .try_into_completion()
    .unwrap();
    let owner = novarocks_sql::compiler::SqlCompiler::start(owner, &control)
        .unwrap()
        .into_complete()
        .unwrap()
        .into_plan();
    let receipts = fold.receipts.lock().unwrap();
    let attempted = receipts
        .iter()
        .find(|receipt| receipt.is_call(FORMAT))
        .unwrap();
    assert_eq!(text(attempted.result.as_ref().unwrap()), expected);
    assert!(!expression_sources(&owner).any(|(_, _, node)| {
        matches!(&node.kind, ExprKind::FunctionCall { function, .. }
            if function.function_id.as_str() == FORMAT)
    }));
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
                &receipt.root().kind,
                ExprKind::FunctionCall { function, .. }
                    if function.function_id.as_str()
                        == novarocks_type_contract::OperatorFunction::BooleanNot.function_id_text()
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
fn regexp_count_fold_preserves_the_original_ordered_call_for_hosted_evaluation() {
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
        receipt.unit.claims()[&receipt.root_use()]
            .effects
            .argument_control,
        ArgumentControl::OrderedRegexp,
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
fn ordered_regexp_folds_known_null_even_with_an_invalid_pattern_or_array_result() {
    let probe = Arc::new(FoldProbe::default());
    let owner = compile(
        "SELECT regexp_replace('a', '[', CAST(NULL AS VARCHAR)), regexp_extract_all(CAST(NULL AS VARCHAR), '[')",
        &probe,
    );
    assert!(!expression_sources(&owner).any(|(_, _, node)| {
        matches!(&node.kind, ExprKind::FunctionCall { function, .. }
            if function.function_id.as_str().starts_with("builtin.scalar/regexp_"))
    }));
    assert!(expression_sources(&owner).any(|(_, _, node)| {
        matches!(node.ty.data_type(), DataType::List(_))
            && matches!(node.kind, ExprKind::Constant(_))
    }));
}

#[test]
fn public_sql_map_lookup_rejects_a_probe_that_cannot_coerce_to_the_declared_key() {
    use novarocks_sql::analyze_error::AnalyzeErrorKind;
    use novarocks_sql::compiler::{SqlCompileError, SqlCompileProgressError};

    let probe = Arc::new(FoldProbe::default());
    let expression = "map_agg(CAST(1 AS INTEGER),'one')[1e0]";
    let sql = format!("SELECT {expression}");
    let error =
        compile_table_free(&sql, session(Default::default(), true), &probe.control()).unwrap_err();
    let SqlCompileProgressError::Compile(SqlCompileError::Analyze(error)) = error else {
        panic!("expected a typed analysis error: {error}");
    };
    assert_eq!(error.kind(), AnalyzeErrorKind::UnknownFunction);
    assert_eq!(error.code().as_str(), "sql.analyze.unknown_function");
    assert_eq!(error.message(), "no matching declared overload");
    let span = error.span().expect("the original MAP subscript has a span");
    assert_eq!(&sql[span.start()..span.end()], expression);
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
fn listagg_original_arguments_keep_child_fold() {
    {
        let semantics = SqlSemanticSettings::default();
        let probe = Arc::new(FoldProbe::default());
        crate::fixtures::optimize_table_free(
            "SELECT LISTAGG(REVERSE('abc')) WITHIN GROUP (ORDER BY 1)",
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

#[test]
fn temporal_fold_reads_the_selected_statement_time_facts() {
    use novarocks_type_contract::{
        StatementStartUtc, StatementTimeEnvironment, TemporalMicros, TemporalValue, TimeZoneKey,
    };
    let probe = Arc::new(FoldProbe::default());
    let zone = TimeZoneKey::parse("America/New_York").unwrap();
    let start = StatementStartUtc::try_new(1_704_067_200, 123_456_789).unwrap();
    let semantics = SqlSemanticSettings::default()
        .with_statement_time(StatementTimeEnvironment { start, zone });
    let physical = compile_table_free(
        "SELECT CURRENT_TIMESTAMP(12)",
        session(semantics, true),
        &probe.control(),
    )
    .unwrap();
    let receipts = probe.receipts.lock().unwrap();
    let receipt = receipts.iter().find(|receipt| {
        matches!(&receipt.root().kind, ExprKind::FunctionCall { function, .. }
            if function.overload.as_str() == "builtin.scalar/temporal_statement_syntax/current_timestamp/v1")
    }).expect("the original SQL fold observes the selected clock overload");
    assert_eq!(
        receipt.parameters.entries().values().collect::<Vec<_>>(),
        [
            &novarocks_type_contract::SemanticParameterValue::StatementStartUtc(start),
            &novarocks_type_contract::SemanticParameterValue::TimeZone(zone),
        ],
    );
    assert_eq!(
        receipt
            .result
            .as_ref()
            .unwrap()
            .temporal_observed(CompilePhase::Validate, &SqlCompileControl::unbounded(),)
            .unwrap(),
        Some(TemporalValue::TimestampWithTimeZone {
            instant: TemporalMicros::new(1_704_067_200_123_456, 789_000).unwrap(),
            zone,
        }),
    );
    assert!(!expression_sources(&physical).any(|(_, _, node)| {
        matches!(&node.kind, ExprKind::FunctionCall { function, .. }
            if function.overload.as_str() == "builtin.scalar/temporal_statement_syntax/current_timestamp/v1")
    }));
}

#[test]
fn temporal_at_local_fold_keeps_the_original_zone_during_source_admission() {
    use novarocks_type_contract::{
        SemanticParameterValue, StatementStartUtc, StatementTimeEnvironment, TemporalMicros,
        TemporalValue, TimeZoneKey,
    };
    let probe = Arc::new(FoldProbe::default());
    let zone = TimeZoneKey::parse("America/New_York").unwrap();
    let start = StatementStartUtc::try_new(1_704_067_200, 123_456_789).unwrap();
    let semantics = SqlSemanticSettings::default()
        .with_statement_time(StatementTimeEnvironment { start, zone });
    compile_table_free(
        "SELECT TIMESTAMP '2024-01-01 00:00:00.000000000001' AT LOCAL",
        session(semantics, true),
        &probe.control(),
    )
    .unwrap();
    let receipts = probe.receipts.lock().unwrap();
    let receipt = receipts.iter().find(|receipt| {
        matches!(&receipt.root().kind, ExprKind::FunctionCall { function, .. }
            if function.overload.as_str() == "builtin.scalar/value_domain_conversion/temporal_zone/v1")
    }).expect("the original SQL fold observes the selected local timestamp conversion");
    assert_eq!(
        receipt.parameters.entries().values().collect::<Vec<_>>(),
        [&SemanticParameterValue::TimeZone(zone)]
    );
    assert_eq!(
        receipt
            .result
            .as_ref()
            .unwrap()
            .temporal_observed(CompilePhase::Validate, &SqlCompileControl::unbounded())
            .unwrap(),
        Some(TemporalValue::TimestampWithTimeZone {
            instant: TemporalMicros::new(1_704_085_200_000_000, 1).unwrap(),
            zone,
        })
    );
}
