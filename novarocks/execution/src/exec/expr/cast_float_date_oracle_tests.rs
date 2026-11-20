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

//! Full float-to-DATE profiles with fixed IEEE/calendar corpus expectations.
use arrow::array::{Array, ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use novarocks_functions::{
    CastOperation, CastRowResult, EvaluatedArgument, KernelEvaluationControl, KernelFailure,
    PreparedCastRecipe, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType, PureCompileControl,
};
use std::{sync::Arc, time::Duration};
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
        panic!("float DATE does not wait")
    }
}
// Pure source/corpus helpers remain available to retained sibling tests.
use chrono::{Datelike, NaiveDate};
pub(super) fn days(year: i32, month: u32, day: u32) -> i32 {
    NaiveDate::from_ymd_opt(year, month, day)
        .unwrap()
        .num_days_from_ce()
        - 719163
}
pub(super) fn input(wide: bool, values: Vec<Option<f64>>) -> ArrayRef {
    if wide {
        Arc::new(Float64Array::from(values))
    } else {
        Arc::new(Float32Array::from(
            values
                .into_iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        ))
    }
}
pub(super) fn cases(wide: bool) -> Vec<(f64, &'static str)> {
    let mut cases = vec![
        (0.0, "0"),
        (-0.0, "0"),
        (f64::from_bits(1), "0"),
        (-f64::from_bits(1), "0"),
        (100.999, "100"),
        (-101.9, "-101"),
        (691232.0, "691232"),
        (700100.0, "700100"),
        (20240230.0, "20240230"),
        (20241301.0, "20241300"),
        (f64::NAN, "NaN"),
        (f64::INFINITY, "inf"),
        (f64::NEG_INFINITY, "-inf"),
        (f64::MAX, "9223372036854775807"),
        (-f64::MAX, "-9223372036854775808"),
        (9223372036854775808.0, "9223372036854775807"),
        (-9223372036854775808.0, "-9223372036854775808"),
    ];
    if wide {
        // F64-to-i64 truncation does not validate the fractional part before the original grammar.
        cases.retain(|(value, _)| *value != 20241301.0);
        cases.extend([
            (20241301.0, "20241301"),
            (20240229240000.0, "20240229240000"),
            (20240229126000.0, "20240229126000"),
            (20240229123460.0, "20240229123460"),
            (99999999999999.0, "99999999999999"),
            (100000000000000.0, "100000000000000"),
        ]);
    } else {
        // Use actual finite f32 extrema; f64::MAX would already be an infinite f32 source.
        cases.retain(|(value, _)| value.abs() != f64::MAX);
        cases.extend([
            (f32::MAX as f64, "9223372036854775807"),
            (-(f32::MAX as f64), "-9223372036854775808"),
            (20240101.0, "20240100"),
            (99991231.0, "99991232"),
        ]);
    }
    cases
}
#[derive(Clone, Copy)]
enum Expected {
    Day(i32),
    Invalid(&'static str),
}
// Fixed source IEEE bits -> explicit Gregorian date or rejected literal.
// No runtime numeric-date parser, old evaluator or product author is an oracle.
fn expected(a: &ArrayRef, row: usize) -> Option<Expected> {
    if a.is_null(row) {
        return None;
    }
    let (wide, bits) = match a.data_type() {
        DataType::Float32 => (
            false,
            u64::from(
                a.as_any()
                    .downcast_ref::<arrow::array::Float32Array>()
                    .unwrap()
                    .value(row)
                    .to_bits(),
            ),
        ),
        DataType::Float64 => (
            true,
            a.as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .value(row)
                .to_bits(),
        ),
        _ => panic!("fixed float-date carrier"),
    };
    Some(match (wide, bits) {
        (false, 0x0) => Expected::Invalid("0"),
        (false, 0x3e800000) => Expected::Invalid("0"),
        (false, 0x3f800000) => Expected::Invalid("1"),
        (false, 0x3fe00000) => Expected::Invalid("1"),
        (false, 0x42c68000) => Expected::Invalid("99"),
        (false, 0x42c80000) => Expected::Invalid("100"),
        (false, 0x42c88000) => Expected::Invalid("100"),
        (false, 0x42c98000) => Expected::Invalid("100"),
        (false, 0x42c9ff7d) => Expected::Invalid("100"),
        (false, 0x42ca0000) => Expected::Day(10957),
        (false, 0x42cb8000) => Expected::Day(10957),
        (false, 0x42cbcccd) => Expected::Day(10957),
        (false, 0x4499c800) => Expected::Day(11321),
        (false, 0x4499e000) => Expected::Day(11322),
        (false, 0x4499f800) => Expected::Day(11322),
        (false, 0x49287b50) => Expected::Day(36160),
        (false, 0x4928c1e4) => Expected::Day(36523),
        (false, 0x4928c1f0) => Expected::Day(36524),
        (false, 0x4928c1f4) => Expected::Day(36524),
        (false, 0x4928c1fc) => Expected::Day(36524),
        (false, 0x4928c200) => Expected::Invalid("691232"),
        (false, 0x4928c20c) => Expected::Invalid("691232"),
        (false, 0x492aec34) => Expected::Invalid("700099"),
        (false, 0x492aec40) => Expected::Invalid("700100"),
        (false, 0x492aec44) => Expected::Invalid("700100"),
        (false, 0x492aec4c) => Expected::Invalid("700100"),
        (false, 0x492aec50) => Expected::Day(0),
        (false, 0x492aec5c) => Expected::Day(0),
        (false, 0x4971ffe4) => Expected::Day(10955),
        (false, 0x4971fff0) => Expected::Day(10956),
        (false, 0x4971fff4) => Expected::Day(10956),
        (false, 0x4971fffc) => Expected::Day(10956),
        (false, 0x49720000) => Expected::Invalid("991232"),
        (false, 0x4972000c) => Expected::Invalid("991232"),
        (false, 0x4b1896e3) => Expected::Invalid("10000099"),
        (false, 0x4b1896e4) => Expected::Invalid("10000100"),
        (false, 0x4b1896e5) => Expected::Day(-354285),
        (false, 0x4b1896e6) => Expected::Day(-354284),
        (false, 0x4b9a6b72) => Expected::Invalid("20240100"),
        (false, 0x4b9a6bb2) => Expected::Day(19781),
        (false, 0x4b9a6bb3) => Expected::Invalid("20240230"),
        (false, 0x4b9a6dca) => Expected::Invalid("20241300"),
        (false, 0x4cbeb7d8) => Expected::Invalid("99991232"),
        (false, 0x4cc0a468) => Expected::Day(10957),
        (false, 0x4fcdac40) => Expected::Invalid("6901235712"),
        (false, 0x5320ad3b) => Expected::Day(36160),
        (false, 0x53230147) => Expected::Invalid("700101099520"),
        (false, 0x5366c9fc) => Expected::Invalid("991231213568"),
        (false, 0x55118547) => Expected::Invalid("10000100491264"),
        (false, 0x5593445d) => Expected::Invalid("20240228417536"),
        (false, 0x56b5e20c) => Expected::Invalid("99991234281472"),
        (false, 0x56b5e621) => Expected::Invalid("100000000376832"),
        (false, 0x5f000000) => Expected::Invalid("9223372036854775807"),
        (false, 0x7f7fffff) => Expected::Invalid("9223372036854775807"),
        (false, 0x7f800000) => Expected::Invalid("inf"),
        (false, 0x7fc00000) => Expected::Invalid("NaN"),
        (false, 0x80000000) => Expected::Invalid("0"),
        (false, 0xbf800000) => Expected::Invalid("-1"),
        (false, 0xc2c80000) => Expected::Invalid("-100"),
        (false, 0xc2ca0000) => Expected::Invalid("-101"),
        (false, 0xc2cbcccd) => Expected::Invalid("-101"),
        (false, 0xc499e000) => Expected::Invalid("-1231"),
        (false, 0xc928c1f0) => Expected::Invalid("-691231"),
        (false, 0xc928c200) => Expected::Invalid("-691232"),
        (false, 0xc92aec40) => Expected::Invalid("-700100"),
        (false, 0xc92aec50) => Expected::Invalid("-700101"),
        (false, 0xc971fff0) => Expected::Invalid("-991231"),
        (false, 0xc9720000) => Expected::Invalid("-991232"),
        (false, 0xcb1896e4) => Expected::Invalid("-10000100"),
        (false, 0xcb1896e5) => Expected::Invalid("-10000101"),
        (false, 0xccbeb7d8) => Expected::Invalid("-99991232"),
        (false, 0xccc0a468) => Expected::Invalid("-101000000"),
        (false, 0xcfcdac40) => Expected::Invalid("-6901235712"),
        (false, 0xd366c9fc) => Expected::Invalid("-991231213568"),
        (false, 0xd5118547) => Expected::Invalid("-10000100491264"),
        (false, 0xd6b5e621) => Expected::Invalid("-100000000376832"),
        (false, 0xdf000000) => Expected::Invalid("-9223372036854775808"),
        (false, 0xff7fffff) => Expected::Invalid("-9223372036854775808"),
        (false, 0xff800000) => Expected::Invalid("-inf"),
        (true, 0x0) => Expected::Invalid("0"),
        (true, 0x1) => Expected::Invalid("0"),
        (true, 0x3fd0000000000000) => Expected::Invalid("0"),
        (true, 0x3ff0000000000000) => Expected::Invalid("1"),
        (true, 0x3ffc000000000000) => Expected::Invalid("1"),
        (true, 0x4058d00000000000) => Expected::Invalid("99"),
        (true, 0x4059000000000000) => Expected::Invalid("100"),
        (true, 0x4059100000000000) => Expected::Invalid("100"),
        (true, 0x4059300000000000) => Expected::Invalid("100"),
        (true, 0x40593fef9db22d0e) => Expected::Invalid("100"),
        (true, 0x4059400000000000) => Expected::Day(10957),
        (true, 0x4059700000000000) => Expected::Day(10957),
        (true, 0x405979999999999a) => Expected::Day(10957),
        (true, 0x4093390000000000) => Expected::Day(11321),
        (true, 0x40933c0000000000) => Expected::Day(11322),
        (true, 0x40933f0000000000) => Expected::Day(11322),
        (true, 0x41250f6a00000000) => Expected::Day(36160),
        (true, 0x4125183c80000000) => Expected::Day(36523),
        (true, 0x4125183e00000000) => Expected::Day(36524),
        (true, 0x4125183e80000000) => Expected::Day(36524),
        (true, 0x4125183f80000000) => Expected::Day(36524),
        (true, 0x4125184000000000) => Expected::Invalid("691232"),
        (true, 0x4125184180000000) => Expected::Invalid("691232"),
        (true, 0x41255d8680000000) => Expected::Invalid("700099"),
        (true, 0x41255d8800000000) => Expected::Invalid("700100"),
        (true, 0x41255d8880000000) => Expected::Invalid("700100"),
        (true, 0x41255d8980000000) => Expected::Invalid("700100"),
        (true, 0x41255d8a00000000) => Expected::Day(0),
        (true, 0x41255d8b80000000) => Expected::Day(0),
        (true, 0x412e3ffc80000000) => Expected::Day(10955),
        (true, 0x412e3ffe00000000) => Expected::Day(10956),
        (true, 0x412e3ffe80000000) => Expected::Day(10956),
        (true, 0x412e3fff80000000) => Expected::Day(10956),
        (true, 0x412e400000000000) => Expected::Invalid("991232"),
        (true, 0x412e400180000000) => Expected::Invalid("991232"),
        (true, 0x416312dc68000000) => Expected::Invalid("10000099"),
        (true, 0x416312dc80000000) => Expected::Invalid("10000100"),
        (true, 0x416312dc88000000) => Expected::Invalid("10000100"),
        (true, 0x416312dc98000000) => Expected::Invalid("10000100"),
        (true, 0x416312dca0000000) => Expected::Day(-354285),
        (true, 0x416312dcb8000000) => Expected::Day(-354285),
        (true, 0x41734d6e50000000) => Expected::Day(19723),
        (true, 0x41734d7640000000) => Expected::Day(19781),
        (true, 0x41734d7650000000) => Expected::Day(19782),
        (true, 0x41734d7660000000) => Expected::Invalid("20240230"),
        (true, 0x41734db950000000) => Expected::Invalid("20241301"),
        (true, 0x4197d6faf9000000) => Expected::Day(2932895),
        (true, 0x4197d6fafc000000) => Expected::Day(2932896),
        (true, 0x4197d6fafd000000) => Expected::Day(2932896),
        (true, 0x4197d6faff000000) => Expected::Day(2932896),
        (true, 0x4197d6fb00000000) => Expected::Invalid("99991232"),
        (true, 0x4197d6fb03000000) => Expected::Invalid("99991232"),
        (true, 0x4198148cfd000000) => Expected::Invalid("100999999"),
        (true, 0x4198148d00000000) => Expected::Day(10957),
        (true, 0x4198148d03000000) => Expected::Day(10957),
        (true, 0x41f9b5880f640000) => Expected::Invalid("6901235958"),
        (true, 0x41f9b5880f700000) => Expected::Invalid("6901235959"),
        (true, 0x41f9b5880f7c0000) => Expected::Invalid("6901235959"),
        (true, 0x426415a76f300000) => Expected::Day(36160),
        (true, 0x42646028ebb00000) => Expected::Day(0),
        (true, 0x426cd93f8aeec800) => Expected::Day(10956),
        (true, 0x426cd93f8aeee000) => Expected::Day(10956),
        (true, 0x426cd93f8aeef800) => Expected::Day(10956),
        (true, 0x42a230a8ef867e80) => Expected::Invalid("10000100999999"),
        (true, 0x42a230a8ef868000) => Expected::Day(-354285),
        (true, 0x42a230a8ef868180) => Expected::Day(-354285),
        (true, 0x42b2688baac58000) => Expected::Day(19782),
        (true, 0x42b2688baac58400) => Expected::Invalid("20240229123460"),
        (true, 0x42b2688baacf7000) => Expected::Invalid("20240229126000"),
        (true, 0x42b2688bac8cc000) => Expected::Invalid("20240229240000"),
        (true, 0x42d6bc417461ddc0) => Expected::Day(2932896),
        (true, 0x42d6bcc41e8fff90) => Expected::Invalid("99999999999998"),
        (true, 0x42d6bcc41e8fffc0) => Expected::Invalid("99999999999999"),
        (true, 0x42d6bcc41e8fffd0) => Expected::Invalid("99999999999999"),
        (true, 0x42d6bcc41e8ffff0) => Expected::Invalid("99999999999999"),
        (true, 0x42d6bcc41e900000) => Expected::Invalid("100000000000000"),
        (true, 0x42d6bcc41e900030) => Expected::Invalid("100000000000000"),
        (true, 0x43e0000000000000) => Expected::Invalid("9223372036854775807"),
        (true, 0x7fefffffffffffff) => Expected::Invalid("9223372036854775807"),
        (true, 0x7ff0000000000000) => Expected::Invalid("inf"),
        (true, 0x7ff8000000000000) => Expected::Invalid("NaN"),
        (true, 0x8000000000000000) => Expected::Invalid("0"),
        (true, 0x8000000000000001) => Expected::Invalid("0"),
        (true, 0xbff0000000000000) => Expected::Invalid("-1"),
        (true, 0xc059000000000000) => Expected::Invalid("-100"),
        (true, 0xc059400000000000) => Expected::Invalid("-101"),
        (true, 0xc05979999999999a) => Expected::Invalid("-101"),
        (true, 0xc0933c0000000000) => Expected::Invalid("-1231"),
        (true, 0xc125183e00000000) => Expected::Invalid("-691231"),
        (true, 0xc125184000000000) => Expected::Invalid("-691232"),
        (true, 0xc1255d8800000000) => Expected::Invalid("-700100"),
        (true, 0xc1255d8a00000000) => Expected::Invalid("-700101"),
        (true, 0xc12e3ffe00000000) => Expected::Invalid("-991231"),
        (true, 0xc12e400000000000) => Expected::Invalid("-991232"),
        (true, 0xc16312dc80000000) => Expected::Invalid("-10000100"),
        (true, 0xc16312dca0000000) => Expected::Invalid("-10000101"),
        (true, 0xc197d6fafc000000) => Expected::Invalid("-99991231"),
        (true, 0xc197d6fb00000000) => Expected::Invalid("-99991232"),
        (true, 0xc198148d00000000) => Expected::Invalid("-101000000"),
        (true, 0xc1f9b5880f700000) => Expected::Invalid("-6901235959"),
        (true, 0xc26cd93f8aeee000) => Expected::Invalid("-991231235959"),
        (true, 0xc2a230a8ef868000) => Expected::Invalid("-10000101000000"),
        (true, 0xc2d6bcc41e8fffc0) => Expected::Invalid("-99999999999999"),
        (true, 0xc2d6bcc41e900000) => Expected::Invalid("-100000000000000"),
        (true, 0xc3e0000000000000) => Expected::Invalid("-9223372036854775808"),
        (true, 0xffefffffffffffff) => Expected::Invalid("-9223372036854775808"),
        (true, 0xfff0000000000000) => Expected::Invalid("-inf"),
        _ => panic!("float-date corpus needs a fixed independent expected row"),
    })
}
fn recipe(
    wide: bool,
    nullable: bool,
    result_nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(
            if wide {
                DataType::Float64
            } else {
                DataType::Float32
            },
            nullable,
        ),
        &FunctionValueType::new(DataType::Date32, result_nullable),
        policy,
        allow,
        None,
        &Control,
    )
    .unwrap()
}
fn compare_value(
    r: &PreparedCastRecipe,
    array: &ArrayRef,
    ordinal: usize,
    row: usize,
    policy: DecimalOverflowPolicy,
    allow: bool,
) {
    let _ = (policy, allow); // Invalid dates remain strict under every frozen mode.
    let value = r
        .evaluate_row(EvaluatedArgument::Column(array), ordinal, row, &Control)
        .unwrap();
    match expected(array, row) {
        None => assert_eq!(value, CastRowResult::Null),
        Some(Expected::Day(days)) => assert_eq!(value, CastRowResult::Signed(i64::from(days))),
        Some(Expected::Invalid(literal)) => {
            let CastRowResult::RowError(error) = value else {
                panic!("strict invalid date requires row error");
            };
            assert_eq!(error.selected_ordinal(), ordinal);
            assert_eq!(
                error.message(),
                format!(
                    "CAST failed: from {:?} to Date32: invalid date literal {literal}",
                    array.data_type()
                )
            );
            use novarocks_failure_contract::FailureProjection;
            assert_eq!(
                error.failure_fact(),
                novarocks_functions::DataFailureReason::InvalidValue.failure_fact()
            );
        }
    }
}

fn complete(wide: bool) {
    let mut values = vec![
        Some(101.9),
        Some(690101.0),
        Some(700101.0),
        Some(991231.0),
        Some(10000101.0),
        Some(20240229.0),
        Some(20240101.0),
        Some(99991231.0),
        Some(101000000.0),
        Some(690101123456.0),
        Some(700101123456.0),
        Some(20240229123456.0),
        Some(99991231235959.0),
    ];
    values.extend(cases(wide).into_iter().map(|(value, _)| Some(value)));
    // Deliberately include every grammar region and precision boundary; no source shape is excluded.
    for value in [
        1.0,
        100.0,
        101.0,
        1231.0,
        691231.0,
        691232.0,
        700100.0,
        700101.0,
        991231.0,
        991232.0,
        10000100.0,
        10000101.0,
        99991231.0,
        99991232.0,
        101000000.0,
        6901235959.0,
        991231235959.0,
        10000101000000.0,
        99999999999999.0,
        100000000000000.0,
    ] {
        values.extend([
            Some(value),
            Some(value + 0.75),
            Some(value - 0.75),
            Some(-value),
        ]);
    }
    for source_nullable in [false, true] {
        let mut rows = values.clone();
        if source_nullable {
            rows.push(None);
        }
        let array = input(wide, rows);
        for result_nullable in [true, false] {
            if source_nullable && !result_nullable {
                continue;
            }
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let r = recipe(wide, source_nullable, result_nullable, policy, allow);
                    for row in 0..array.len() {
                        compare_value(&r, &array, row, row, policy, allow);
                    }
                    let slice = array.slice(1, array.len() - 1);
                    for row in 0..slice.len() {
                        compare_value(&r, &slice, row, row, policy, allow);
                    }
                    let empty = array.slice(0, 0);
                    assert_eq!(empty.len(), 0);
                    for row in Selection::all(0).iter() {
                        compare_value(&r, &empty, row, row, policy, allow);
                    }
                    let indices = [0, 2, array.len() - 1];
                    let selection = Selection::try_sparse(array.len(), &indices).unwrap();
                    for (ordinal, row) in selection.iter().enumerate() {
                        compare_value(&r, &array, ordinal, row, policy, allow);
                    }
                    // Nonzero Scalar source addresses are not logical-row lookups.
                    let scalar = array.slice(2, 1);
                    assert_eq!(days(1970, 1, 1), 0);
                    assert_eq!(
                        r.evaluate_row(EvaluatedArgument::Scalar(&scalar), 33, 999, &Control)
                            .unwrap(),
                        CastRowResult::Signed(i64::from(days(1970, 1, 1)))
                    );
                }
            }
        }
    }
    let hidden: ArrayRef = if wide {
        Arc::new(arrow::array::Float64Array::new(
            vec![f64::NAN, 20240228.0].into(),
            Some(arrow_buffer::NullBuffer::from(vec![false, true])),
        ))
    } else {
        Arc::new(arrow::array::Float32Array::new(
            vec![f32::NAN, 20240228.0].into(),
            Some(arrow_buffer::NullBuffer::from(vec![false, true])),
        ))
    };
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let r = recipe(wide, true, true, policy, allow);
            compare_value(&r, &hidden, 0, 0, policy, allow);
            compare_value(&r, &hidden, 1, 1, policy, allow);
        }
    }
}
#[test]
fn float_date_oracle_complete_float32_date32_profile() {
    complete(false);
}
#[test]
fn float_date_oracle_complete_float64_date32_profile() {
    complete(true);
}
