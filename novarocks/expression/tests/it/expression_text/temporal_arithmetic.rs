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

use super::*;
use arrow::array::FixedSizeBinaryArray;
use novarocks_expression::typing::temporal_function_value_type;
use novarocks_function_contract::kernel_failure::{
    INVALID_FUNCTION_ARGUMENT, NUMERIC_OUT_OF_RANGE,
};
use novarocks_type_contract::{
    StatementStartUtc, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue, TimeZoneKey,
};

#[test]
fn temporal_arithmetic_keeps_exact_calendar_and_fractional_operators() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    for (sql, day) in [
        ("DATE '2024-01-31' + INTERVAL '1' MONTH", 19782),
        ("INTERVAL '-1' MONTH + DATE '2024-03-31'", 19782),
        ("DATE '1970-01-03' - INTERVAL '2' DAY", 0),
    ] {
        let result = constant(sql, &context);
        let days = result
            .batch
            .values
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(days.value(0), day, "{sql}");
    }

    let result = constant("INTERVAL '1' MINUTE + INTERVAL '2.003' SECOND", &context);
    let ty = result.batch.result_type.value_type().temporal().unwrap();
    let TemporalType::Interval(q) = ty else {
        panic!("exact interval result")
    };
    assert_eq!(q.start(), novarocks_type_contract::IntervalField::Minute);
    assert_eq!(q.end(), novarocks_type_contract::IntervalField::Second);
    assert_eq!(q.leading_precision(), 12);
    assert_eq!(q.fractional_precision(), TemporalPrecision::DEFAULT);
    let micros = result
        .batch
        .values
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(micros.value(0), 62_003_000);

    let result = constant(
        "TIMESTAMP '1969-12-31 23:59:59.9999' - TIMESTAMP '1970-01-01 00:00:00.0002'",
        &context,
    );
    let ty = result.batch.result_type.value_type().temporal().unwrap();
    let TemporalType::Interval(q) = ty else {
        panic!("datetime difference must be an interval")
    };
    assert_eq!(q.leading_precision(), 9);
    assert_eq!(q.fractional_precision(), TemporalPrecision::MICROSECONDS);
    let micros = result
        .batch
        .values
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(micros.value(0), -300);

    let result = constant(
        "TIMESTAMP '2024-01-01 23:59:59.999999999999' + INTERVAL '0.001' SECOND",
        &context,
    );
    let ty = TemporalType::Timestamp(TemporalPrecision::PICOSECONDS);
    assert_eq!(result.batch.result_type.value_type().temporal(), Some(ty));
    let values = result
        .batch
        .values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        TemporalValue::from_fixed_carrier(ty, values.value(0)).unwrap(),
        TemporalValue::Timestamp(TemporalMicros::new(1_704_153_600_000_999, 999_999).unwrap()),
    );

    let result = constant("(DATE '2024-03-01' - DATE '2024-02-28') DAY", &context);
    let micros = result
        .batch
        .values
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(micros.value(0), 172_800_000_000);
}

#[test]
fn interval_assignment_casts_trim_trailing_fields_after_full_value_truncation() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    for (sql, expected) in [
        ("CAST(INTERVAL '25' HOUR AS INTERVAL DAY)", 86_400_000_000),
        (
            "CAST(INTERVAL '-86400.000000000001' SECOND(13, 12) AS INTERVAL DAY)",
            -86_400_000_000,
        ),
        (
            "CAST(INTERVAL '-86399.999999999999' SECOND(13, 12) AS INTERVAL DAY)",
            0,
        ),
        ("CAST(INTERVAL '90' SECOND AS INTERVAL MINUTE)", 60_000_000),
        (
            "CAST(INTERVAL '-0.500000000000' SECOND(2, 12) AS INTERVAL SECOND(2, 0))",
            0,
        ),
        (
            "(TIMESTAMP '2024-01-02 01:00:00' - TIMESTAMP '2024-01-01 00:00:00') DAY",
            86_400_000_000,
        ),
    ] {
        let result = constant(sql, &context);
        let micros = result
            .batch
            .values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(micros.value(0), expected, "{sql}");
    }

    let result = constant("CAST(INTERVAL '-23' MONTH AS INTERVAL YEAR)", &context);
    let months = result
        .batch
        .values
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(months.value(0), -12);

    let sql = "CAST(INTERVAL '999.5' SECOND AS INTERVAL SECOND(3, 0))";
    let unit = compile(sql, &[], &context);
    let checked = evaluate(&unit, &batch(&[], vec![]));
    assert_eq!(checked.batch.errors.len(), 1);
    assert_eq!(
        checked.batch.errors[0].reason(),
        DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE)
    );
}

#[test]
fn single_field_interval_numeric_casts_use_the_declared_unit_and_preserve_errors() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    for (sql, expected) in [
        (
            "CAST(INTERVAL '-0.999999999999' SECOND(4, 12) AS BIGINT)",
            0,
        ),
        ("CAST(INTERVAL '-11' HOUR AS BIGINT)", -11),
        ("CAST(-(INTERVAL '2' HOUR) AS BIGINT)", -2),
        ("CAST(CAST(12 AS INTERVAL SECOND(4, 12)) AS BIGINT)", 12),
        ("CAST(CAST(-2 AS INTERVAL YEAR) AS BIGINT)", -2),
        (
            "CAST(CAST(2147483648 AS INTERVAL SECOND(13, 6)) AS BIGINT)",
            2_147_483_648,
        ),
    ] {
        let result = constant(sql, &context);
        let values = result
            .batch
            .values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), expected, "{sql}");
    }
    for sql in [
        "CAST(2 AS INTERVAL DAY TO SECOND)",
        "CAST(INTERVAL '1' DAY AS INTEGER)",
        "CAST(INTERVAL '2 00:00:00' DAY TO SECOND AS BIGINT)",
    ] {
        assert!(
            matches!(
                type_expression_text(sql, &[], &context, &HarnessControl),
                Err(TextError::Typing {
                    cause: TypingError::TypeMismatch(_),
                    ..
                })
            ),
            "{sql}"
        );
    }
    assert!(matches!(
        type_expression_text(
            "CAST(x AS INTERVAL MINUTE)",
            &[column("x", 5, DataType::Int16, false)],
            &context,
            &HarnessControl,
        ),
        Err(TextError::Typing {
            cause: TypingError::TypeMismatch(_),
            ..
        })
    ));
    let result = constant("TRY_CAST(100 AS INTERVAL SECOND(2, 0))", &context);
    assert!(result.batch.values.is_null(0));
    let unit = compile("CAST(100 AS INTERVAL SECOND(2, 0))", &[], &context);
    let checked = evaluate(&unit, &batch(&[], vec![]));
    assert_eq!(checked.batch.errors.len(), 1);
    assert_eq!(
        checked.batch.errors[0].reason(),
        DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE)
    );
}

#[test]
fn datetime_subtraction_selects_exact_common_operands_using_the_frozen_zone() {
    let functions = fixture();
    let parameters = SemanticParameters::try_new([
        (
            ALLOW_THROW.id,
            SemanticParameterValue::AllowThrowException(true),
        ),
        (
            SemanticParameterId::new(11),
            SemanticParameterValue::StatementStartUtc(
                StatementStartUtc::try_new(1_704_067_200, 0).unwrap(),
            ),
        ),
        (
            SemanticParameterId::new(21),
            SemanticParameterValue::TimeZone(TimeZoneKey::parse("Asia/Shanghai").unwrap()),
        ),
    ])
    .unwrap();
    let context = context(&functions, &parameters);
    for sql in [
        "TIMESTAMP '2024-01-01 08:00:00.000000000001' - TIMESTAMP '2024-01-01 00:00:00 UTC'",
        "TIME '08:00:00.000000000001' - TIME '00:00:00 +00:00'",
    ] {
        let result = constant(sql, &context);
        let ty = result.batch.result_type.value_type().temporal().unwrap();
        assert_eq!(ty.precision(), TemporalPrecision::PICOSECONDS);
        let durations = result
            .batch
            .values
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            TemporalValue::from_fixed_carrier(ty, durations.value(0)).unwrap(),
            TemporalValue::IntervalDayTime(TemporalMicros::new(0, 1).unwrap()),
            "{sql}"
        );
    }
    for (sql, microseconds) in [
        ("DATE '2024-01-02' - DATE '2024-01-01'", 86_400_000_000),
        (
            "DATE '2024-01-02' - TIMESTAMP '2024-01-01 12:00:00'",
            43_200_000_000,
        ),
        (
            "DATE '2024-01-02' - TIMESTAMP '2024-01-01 12:00:00 UTC'",
            14_400_000_000,
        ),
    ] {
        let result = constant(sql, &context);
        let values = result
            .batch
            .values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), microseconds, "{sql}");
    }
}

#[test]
fn temporal_arithmetic_live_time_rows_and_typed_date_errors_follow_the_same_operator() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let columns = [TextColumn {
        name: "t".into(),
        value: ValueId::new(5),
        value_type: temporal_function_value_type(
            TemporalType::Time(TemporalPrecision::PICOSECONDS),
            true,
        )
        .unwrap(),
    }];
    let unit = compile("t + INTERVAL '0.001' SECOND", &columns, &context);
    let rows = Int64Array::from(vec![Some(novarocks_type_contract::PICOS_PER_DAY - 1), None]);
    let checked = evaluate(&unit, &batch(&columns, vec![Arc::new(rows)]));
    assert_compared(&checked, 1, 2);
    let output = checked
        .batch
        .values
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(output.value(0), 999_999_999);
    assert!(output.is_null(1));

    for sql in [
        "DATE '2024-01-31' + 1",
        "DATE '2024-01-31' - 1",
        "TIME '12:34:56' % 2",
        "INTERVAL '2' DAY % 2",
        "+(TIME '12:34:56')",
        "+(INTERVAL '2' DAY)",
    ] {
        assert!(matches!(
            type_expression_text(sql, &[], &context, &HarnessControl),
            Err(TextError::Typing {
                cause: TypingError::TypeMismatch(_),
                ..
            })
        ));
    }
    let unit = compile("DATE '2024-01-31' + INTERVAL '1' HOUR", &[], &context);
    let checked = evaluate(&unit, &batch(&[], vec![]));
    assert_eq!(checked.batch.errors.len(), 1);
    assert_eq!(
        checked.batch.errors[0].reason(),
        DataFailureReason::Domain(INVALID_FUNCTION_ARGUMENT),
    );
}

#[test]
fn temporal_date_add_keeps_month_end_and_actual_row_errors() {
    use novarocks_function_contract::kernel_failure::DATE_RESULT_OUT_OF_RANGE;

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    let result = constant("date_add('MoNtH', 1, DATE '2024-01-31')", &context);
    let days = result
        .batch
        .values
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(days.value(0), 19782);

    let rejected = compile(
        "TRY(date_add('day', 1, DATE '+5881580-07-11'))",
        &[],
        &context,
    );
    let rows = evaluate(&rejected, &batch(&[], vec![]));
    assert_eq!(rows.batch.errors.len(), 1);
    assert_eq!(
        rows.batch.errors[0].reason(),
        DataFailureReason::Domain(DATE_RESULT_OUT_OF_RANGE)
    );

    let caught = constant("TRY(date_add('hour', 1, DATE '2024-01-31'))", &context);
    assert!(caught.batch.values.is_null(0));
    let unselected = constant(
        "CASE WHEN false THEN date_add('day', 1, DATE '+5881580-07-11') ELSE DATE '1970-01-01' END",
        &context,
    );
    let days = unselected
        .batch
        .values
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap();
    assert_eq!(days.value(0), 0);
}

#[test]
fn calendar_difference_and_truncation_use_trino_units_and_precision() {
    use novarocks_function_contract::kernel_failure::DATE_RESULT_OUT_OF_RANGE;

    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    for (sql, expected) in [
        (
            "date_diff('month', DATE '2024-01-31', DATE '2024-02-29')",
            1,
        ),
        ("date_diff('day', DATE '2024-03-01', DATE '2024-02-28')", -2),
        (
            "date_diff('millisecond', TIMESTAMP '1969-12-31 23:59:59.9999', TIMESTAMP '1970-01-01 00:00:00.0001')",
            1,
        ),
        (
            "date_diff('hour', TIME '00:00:00+02:00', TIME '01:00:00+00:00')",
            -21,
        ),
        (
            "date_diff('day', DATE '2024-03-09', TIMESTAMP '2024-03-10 01:00:00.1234')",
            1,
        ),
    ] {
        let result = constant(sql, &context);
        let values = result
            .batch
            .values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), expected, "{sql}");
    }

    let rejected = constant(
        "TRY(date_trunc('microsecond', TIMESTAMP '1970-01-01 00:00:00.000000000001'))",
        &context,
    );
    assert!(rejected.batch.values.is_null(0));

    for sql in [
        "TRY(date_trunc('week', DATE '-5877641-06-23'))",
        "TRY(last_day_of_month(DATE '+5881580-07-11'))",
    ] {
        let unit = compile(sql, &[], &context);
        let checked = evaluate(&unit, &batch(&[], vec![]));
        assert_eq!(checked.batch.errors.len(), 1, "{sql}");
        assert_eq!(
            checked.batch.errors[0].reason(),
            DataFailureReason::Domain(DATE_RESULT_OUT_OF_RANGE),
            "{sql}"
        );
    }
}

#[test]
fn extract_preserves_field_domain_binding_and_interval_capability() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    for (sql, expected) in [
        ("EXTRACT(YOW FROM DATE '2021-01-01')", 2020),
        ("EXTRACT(WEEK FROM DATE '2021-01-01')", 53),
        ("EXTRACT(DOW FROM DATE '2021-01-03')", 7),
        ("day_of_year(DATE '2024-03-01')", 61),
        ("hour(TIME '13:54:11')", 13),
        ("EXTRACT(TIMEZONE_MINUTE FROM TIME '02:00:00-03:45')", -45),
        ("year(INTERVAL '25' MONTH)", 2),
        ("hour(INTERVAL '36' HOUR)", 36),
        ("EXTRACT(HOUR FROM INTERVAL '36' HOUR)", 36),
        (
            "EXTRACT(SECOND FROM INTERVAL '-0 23:59:59.999999999999' DAY TO SECOND(12))",
            -59,
        ),
        (
            "EXTRACT(MONTH FROM TIMESTAMP '2024-03-01 00:15:00+13:00')",
            3,
        ),
    ] {
        let result = constant(sql, &context);
        let values = result
            .batch
            .values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), expected, "{sql}");
    }
    for sql in [
        "EXTRACT(HOUR FROM DATE '2024-01-01')",
        "EXTRACT(HOUR FROM INTERVAL '1500' MINUTE)",
        "EXTRACT(YEAR FROM INTERVAL '25' MONTH)",
        "EXTRACT(DAY_OF_MONTH FROM INTERVAL '1' DAY)",
    ] {
        assert!(
            matches!(
                type_expression_text(sql, &[], &context, &HarnessControl),
                Err(TextError::Typing {
                    cause: TypingError::TypeMismatch(_),
                    ..
                })
            ),
            "{sql}"
        );
    }
    for sql in [
        "hour(INTERVAL '36' HOUR)",
        "EXTRACT(HOUR FROM INTERVAL '36' HOUR)",
    ] {
        let typed = compile(sql, &[], &context);
        assert!(
            typed.unit().definitions().iter().any(|definition| matches!(
                &definition.kind,
                ExprKind::FunctionCall { function, .. }
                    if function.overload.as_str() == "builtin.scalar/hour/interval_day_time/v1"
            )),
            "{sql}"
        );
    }
}

#[test]
fn date_projection_reuses_the_exact_date_conversion() {
    let functions = fixture();
    let parameters = parameters();
    let context = context(&functions, &parameters);
    for sql in [
        "date(' 2024-2-29 ')",
        "date(DATE '2024-02-29')",
        "date(TIMESTAMP '2024-02-29 00:15:00+13:00')",
        "date(TIMESTAMP '2024-02-29 23:30:00.9999')",
    ] {
        let result = constant(sql, &context);
        let values = result
            .batch
            .values
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(values.value(0), 19782, "{sql}");
    }
    assert!(
        constant("TRY(date('2024-02-30'))", &context)
            .batch
            .values
            .is_null(0)
    );
}
