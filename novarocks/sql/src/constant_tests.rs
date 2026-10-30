// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{common::LiteralValue, constant::admit_syntax_constant};
use arrow::{
    array::{Array, Date64Array, Float32Array},
    datatypes::{DataType, i256},
};
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantValue};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use std::sync::Mutex;

fn policy() -> ConstantPolicy {
    // Explicit finite fixture admission, not a production policy or MEM grant.
    ConstantPolicy {
        max_rows: 4,
        max_array_nodes: 16,
        max_logical_elements: 64,
        max_retained_buffer_bytes: 8 << 20,
        max_type_depth: 64,
        max_type_nodes: 64,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    }
}
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn new(stop: Option<(usize, CompileControlError)>) -> Self {
        Self {
            trace: Default::default(),
            stop,
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.stop {
            assert!(
                trace.len() < at,
                "original refusal must not be followed by another callback"
            );
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((at, error)) if trace.len() == at => Err(error),
            _ => Ok(()),
        }
    }
}
fn admit(value: LiteralValue, ty: &FunctionValueType) -> ConstantValue {
    let value = admit_syntax_constant(&value, ty, policy(), &Control::new(None)).unwrap();
    assert_eq!(value.ordinal(), 0);
    assert_eq!(value.pool().array().len(), 1);
    assert_eq!(value.value_type(), ty);
    assert_eq!(value.pool().field_ref().name(), "literal");
    assert_eq!(value.pool().field_ref().data_type(), ty.data_type());
    assert_eq!(value.pool().field_ref().is_nullable(), ty.nullable());
    assert!(value.pool().field_ref().metadata().as_arc().is_none());
    value
}
fn refused(value: LiteralValue, ty: &FunctionValueType) {
    assert!(admit_syntax_constant(&value, ty, policy(), &Control::new(None)).is_err());
}
fn ty(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(carrier, nullable)
}

#[test]
fn syntax_signed_and_date_carriers_keep_exact_values_and_reject_out_of_range() {
    for (carrier, min, max) in [
        (DataType::Int8, i64::from(i8::MIN), i64::from(i8::MAX)),
        (DataType::Int16, i64::from(i16::MIN), i64::from(i16::MAX)),
        (DataType::Int32, i64::from(i32::MIN), i64::from(i32::MAX)),
        (DataType::Int64, i64::MIN, i64::MAX),
        (DataType::Date32, i64::from(i32::MIN), i64::from(i32::MAX)),
        (DataType::Date64, i64::MIN, i64::MAX),
    ] {
        let declared = ty(carrier.clone(), false);
        for input in [min, -1, 0, max] {
            let value = admit(LiteralValue::Int(input), &declared);
            let actual = match carrier {
                DataType::Date32 => i64::from(value.try_date32().unwrap().unwrap()),
                DataType::Date64 => value
                    .pool()
                    .array()
                    .as_any()
                    .downcast_ref::<Date64Array>()
                    .unwrap()
                    .value(0),
                _ => value.try_i64().unwrap().unwrap(),
            };
            assert_eq!(actual, input);
        }
        for over in [min.checked_sub(1), max.checked_add(1)]
            .into_iter()
            .flatten()
        {
            refused(LiteralValue::Int(over), &declared);
        }
    }
    refused(LiteralValue::Int(42), &ty(DataType::Float64, false));
}

#[test]
fn syntax_unsigned_carriers_preserve_representable_syntax_without_signed_casts() {
    for (carrier, max) in [
        (DataType::UInt8, i64::from(u8::MAX)),
        (DataType::UInt16, i64::from(u16::MAX)),
        (DataType::UInt32, i64::from(u32::MAX)),
        // SQL syntax Int is i64; this does not claim UInt64::MAX spelling here.
        (DataType::UInt64, i64::MAX),
    ] {
        let declared = ty(carrier, false);
        for input in [0, 1, max] {
            assert_eq!(
                admit(LiteralValue::Int(input), &declared)
                    .try_u64()
                    .unwrap(),
                Some(input as u64)
            );
        }
        refused(LiteralValue::Int(-1), &declared);
        if max != i64::MAX {
            refused(LiteralValue::Int(max + 1), &declared);
        }
    }
}

#[test]
fn syntax_floating_sources_keep_bits_and_refuse_rounded_float32() {
    let double = ty(DataType::Float64, false);
    for bits in [
        0,
        1 << 63,
        1,
        0x7ff0_0000_0000_0000,
        0xfff0_0000_0000_0000,
        0x7ff8_0000_0000_0042,
        0xfff8_0000_0000_0081,
    ] {
        assert_eq!(
            admit(LiteralValue::Float(f64::from_bits(bits)), &double)
                .try_f64_bits()
                .unwrap(),
            Some(bits)
        );
    }
    let single = ty(DataType::Float32, true);
    for bits in [0u32, 1 << 31, 1, 0x3fc0_0000, 0x7f80_0000, 0xff80_0000] {
        let input = f64::from(f32::from_bits(bits));
        let value = admit(LiteralValue::Float(input), &single);
        assert_eq!(
            value
                .pool()
                .array()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            bits
        );
    }
    for input in [
        0.1,
        f64::MAX,
        f64::from_bits(1),
        f64::from_bits(0x7ff8_0000_0000_0042),
    ] {
        refused(LiteralValue::Float(input), &single);
    }
}

#[test]
fn syntax_text_binary_nominal_and_typed_null_have_one_complete_source_type() {
    for carrier in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for nullable in [false, true] {
            let declared = ty(carrier.clone(), nullable);
            for input in ["", "µ\0雪🦀"] {
                assert_eq!(
                    admit(LiteralValue::String(input.into()), &declared)
                        .try_utf8()
                        .unwrap(),
                    Some(input)
                );
            }
            if nullable {
                assert_eq!(
                    admit(LiteralValue::Null, &declared).try_utf8().unwrap(),
                    None
                );
            } else {
                refused(LiteralValue::Null, &declared);
            }
        }
    }
    for carrier in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        let declared = ty(carrier, true);
        assert_eq!(
            admit(LiteralValue::Binary(vec![0, 255, 128]), &declared)
                .try_binary()
                .unwrap(),
            Some(&[0, 255, 128][..])
        );
        assert_eq!(
            admit(LiteralValue::Null, &declared).try_binary().unwrap(),
            None
        );
    }
    for (carrier, logical, syntax) in [
        (
            DataType::Utf8,
            ValueLogicalType::Json,
            LiteralValue::String("{\"x\":1}".into()),
        ),
        (
            DataType::LargeBinary,
            ValueLogicalType::Variant,
            LiteralValue::Binary(vec![0, 42, 255]),
        ),
        (
            DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
            LiteralValue::LargeInt(i128::MIN),
        ),
    ] {
        let declared = FunctionValueType::try_with_logical_type(carrier, true, logical).unwrap();
        admit(syntax, &declared);
        let null = admit(LiteralValue::Null, &declared);
        assert!(null.pool().array().is_null(0));
    }
    refused(
        LiteralValue::String("x".into()),
        &ty(DataType::Binary, false),
    );
    refused(
        LiteralValue::LargeInt(1),
        &ty(DataType::FixedSizeBinary(16), false),
    );
}

#[test]
fn syntax_decimals_preserve_exact_coefficients_precision_and_negative_scale() {
    for (carrier, spelling, coefficient) in [
        (DataType::Decimal128(3, 2), "1.55", 155i128),
        (DataType::Decimal128(3, 2), "-1.5500", -155),
        (DataType::Decimal128(3, 2), "+0.01000", 1),
        (DataType::Decimal128(3, 0), "999.000", 999),
        (
            DataType::Decimal128(38, 0),
            "99999999999999999999999999999999999999",
            10i128.pow(38) - 1,
        ),
        (DataType::Decimal128(38, -2), "1200", 12),
        (DataType::Decimal128(38, -2), "-1200.00", -12),
        (DataType::Decimal128(38, -2), "0", 0),
        (DataType::Decimal128(38, i8::MIN), "0", 0),
    ] {
        assert_eq!(
            admit(LiteralValue::Decimal(spelling.into()), &ty(carrier, false))
                .try_decimal128()
                .unwrap(),
            Some(coefficient)
        );
    }
    let big = "123456789012345678901234567890123456789012345678901234567890";
    let expected = big.parse::<i256>().unwrap();
    for (carrier, spelling, coefficient) in [
        (DataType::Decimal256(76, 0), big.to_owned(), expected),
        (
            DataType::Decimal256(76, 2),
            format!("-{big}.00"),
            -expected * i256::from_i128(100),
        ),
        (DataType::Decimal256(76, -2), format!("{big}00"), expected),
        (
            DataType::Decimal256(76, i8::MIN),
            "0".to_owned(),
            i256::ZERO,
        ),
    ] {
        assert_eq!(
            admit(LiteralValue::Decimal(spelling), &ty(carrier, true))
                .try_decimal256()
                .unwrap(),
            Some(coefficient)
        );
    }
}

#[test]
fn syntax_decimal_admission_never_truncates_nonzero_digits_or_accepts_scientific_notation() {
    for (carrier, spelling) in [
        (DataType::Decimal128(3, 2), "1.551"),
        (DataType::Decimal128(3, 0), "1.1"),
        (DataType::Decimal128(3, 0), "1000"),
        (DataType::Decimal128(38, -2), "1201"),
        (DataType::Decimal128(38, -2), "12"),
        (DataType::Decimal128(38, -2), "1200.01"),
        (DataType::Decimal256(76, 0), "1e2"),
        (DataType::Decimal256(76, 2), "1E-2"),
        (DataType::Decimal256(76, 0), "."),
        (DataType::Decimal256(76, 0), "--1"),
        (DataType::Decimal256(76, 0), "1.2.3"),
    ] {
        refused(LiteralValue::Decimal(spelling.into()), &ty(carrier, false));
    }
    refused(
        LiteralValue::Decimal("1".into()),
        &ty(DataType::Int64, false),
    );
    // The borrowed spelling is finite and observed before a parser can wrap a digit counter.
    refused(
        LiteralValue::Decimal("9".repeat(320 * 1024)),
        &ty(DataType::Decimal256(76, 0), false),
    );
}

fn trace_case(
    value: &LiteralValue,
    declared: &FunctionValueType,
    caps: ConstantPolicy,
    success: bool,
    wide: bool,
) {
    let baseline = Control::new(None);
    let result = admit_syntax_constant(value, declared, caps, &baseline);
    assert_eq!(result.is_ok(), success);
    assert!(!matches!(result, Err(ConstantError::Control(_))));
    let trace = baseline.trace.into_inner().unwrap();
    assert!(trace.len() >= 2);
    assert!(
        trace
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
    );
    if wide {
        assert!(trace.iter().any(|(_, units)| *units == 256));
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let positions: Vec<_> = (1..=trace.len()).filter(|at| !wide || *at == 1 || *at == trace.len() || trace[*at-1].1 == 256 &&
            // Exercise both the first and last real quantum without quadratic replay of the long spelling.
            (!trace[..at-1].iter().any(|(_,units)| *units == 256) || !trace[*at..].iter().any(|(_,units)| *units == 256))).collect();
        for at in positions {
            let control = Control::new(Some((at, cause.clone())));
            assert!(
                matches!(admit_syntax_constant(value,declared,caps,&control),Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

#[test]
fn syntax_constant_original_controls_cover_success_ordinary_tail_and_real_quantum() {
    trace_case(
        &LiteralValue::Int(42),
        &ty(DataType::Int8, false),
        policy(),
        true,
        false,
    );
    trace_case(
        &LiteralValue::Int(128),
        &ty(DataType::Int8, false),
        policy(),
        false,
        false,
    );
    trace_case(
        &LiteralValue::Float(0.1),
        &ty(DataType::Float32, false),
        policy(),
        false,
        false,
    );
    trace_case(
        &LiteralValue::Decimal("1.001".into()),
        &ty(DataType::Decimal128(3, 2), false),
        policy(),
        false,
        false,
    );
    trace_case(
        &LiteralValue::String("µ雪\0".repeat(64 * 1024)),
        &ty(DataType::Utf8View, true),
        policy(),
        true,
        true,
    );
    trace_case(
        &LiteralValue::Decimal(format!("{}42", "0".repeat(320 * 1024))),
        &ty(DataType::Decimal256(76, 0), false),
        policy(),
        true,
        true,
    );
}

#[test]
fn syntax_constructor_policy_limits_are_exact_at_boundary_and_refuse_one_below() {
    let spelling = LiteralValue::String("retained payload".into());
    let declared = ty(DataType::Utf8, false);
    for field in 0..10 {
        let set = |caps: &mut ConstantPolicy, n: u64| match field {
            0 => caps.max_rows = n,
            1 => caps.max_array_nodes = n,
            2 => caps.max_logical_elements = n,
            3 => caps.max_retained_buffer_bytes = n,
            4 => caps.max_type_depth = u32::try_from(n).unwrap(),
            5 => caps.max_type_nodes = n,
            6 => caps.max_metadata_bytes = n,
            7 => caps.max_library_validation_work = n,
            8 => caps.max_library_validation_bytes = n,
            _ => caps.max_dictionary_depth = u32::try_from(n).unwrap(),
        };
        let upper = match field {
            0 => 4,
            1 => 16,
            2 => 64,
            4 | 9 => 64,
            5 => 64,
            6 => 65536,
            7 | 8 => 64 << 20,
            _ => 8 << 20,
        };
        let mut lo = 0;
        let mut hi = upper;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mut caps = policy();
            set(&mut caps, mid);
            if admit_syntax_constant(&spelling, &declared, caps, &Control::new(None)).is_ok() {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        let mut exact = policy();
        set(&mut exact, lo);
        trace_case(&spelling, &declared, exact, true, false);
        // A non-dictionary has a genuine zero dictionary-depth boundary.
        if lo != 0 {
            let mut below = policy();
            set(&mut below, lo - 1);
            assert!(matches!(
                admit_syntax_constant(&spelling, &declared, below, &Control::new(None)),
                Err(ConstantError::Limit(_))
            ));
            trace_case(&spelling, &declared, below, false, false);
        }
    }
}

#[test]
fn syntax_temporal_integer_routes_keep_actual_units_timezone_and_signed_counts() {
    use arrow::array::{
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::TimeUnit;
    for carrier in [
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
    ] {
        for input in [i64::MIN, -1, 0, i64::MAX] {
            let value = admit(LiteralValue::Int(input), &ty(carrier.clone(), false));
            let array = value.pool().array();
            let actual = match carrier {
                DataType::Time64(TimeUnit::Microsecond) => array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap()
                    .value(0),
                DataType::Time64(TimeUnit::Nanosecond) => array
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap()
                    .value(0),
                _ => unreachable!(),
            };
            assert_eq!(actual, input);
        }
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [
            None,
            Some(std::sync::Arc::<str>::from("")),
            Some(std::sync::Arc::<str>::from("UTC")),
            Some(std::sync::Arc::<str>::from("Asia/Shanghai")),
        ] {
            let declared = ty(DataType::Timestamp(unit, zone), false);
            for input in [i64::MIN, -1, 0, i64::MAX] {
                let value = admit(LiteralValue::Int(input), &declared);
                let array = value.pool().array();
                let actual = match unit {
                    TimeUnit::Second => array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(0),
                    TimeUnit::Millisecond => array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(0),
                    TimeUnit::Microsecond => array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(0),
                    TimeUnit::Nanosecond => array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .value(0),
                };
                assert_eq!(actual, input);
                assert_eq!(array.data_type().clone(), declared.data_type().clone());
            }
        }
    }
}

#[test]
fn syntax_decimal_accepts_explicit_sign_and_fraction_without_integer_digits() {
    let small = admit(
        LiteralValue::Decimal("+.50".into()),
        &ty(DataType::Decimal128(4, 2), false),
    );
    assert_eq!(small.try_decimal128().unwrap(), Some(50));
    let wide = admit(
        LiteralValue::Decimal("+.50".into()),
        &ty(DataType::Decimal256(40, 2), false),
    );
    assert_eq!(
        wide.try_decimal256().unwrap(),
        Some(arrow::datatypes::i256::from_i128(50))
    );
}
#[test]
fn pure_nominal_constant_author_keeps_semantics_out_of_field_metadata() {
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    let control = Control::new(None);
    for (literal, carrier, domain) in [
        (
            crate::common::LiteralValue::String("{}".into()),
            arrow::datatypes::DataType::Utf8,
            ValueLogicalType::Json,
        ),
        (
            crate::common::LiteralValue::LargeInt(7),
            arrow::datatypes::DataType::FixedSizeBinary(16),
            ValueLogicalType::LargeInt,
        ),
    ] {
        let declared = FunctionValueType::try_with_logical_type(carrier, false, domain).unwrap();
        let value = super::admit_syntax_constant(
            &literal,
            &declared,
            super::test_constant_policy(),
            &control,
        )
        .unwrap();
        assert_eq!(value.value_type(), &declared);
        assert!(value.pool().field_ref().metadata().as_arc().is_none());
        assert!(value.pool().source_metadata_namespace().is_none());
    }
}

#[test]
fn first_syntax_runs_preserve_scalar_rows_types_nulls_and_original_field() {
    use crate::constant::admit_syntax_constant_run;
    let large = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let cases = vec![
        (
            ty(DataType::Int8, true),
            vec![
                LiteralValue::Int(-128),
                LiteralValue::Null,
                LiteralValue::Int(127),
            ],
        ),
        (
            ty(DataType::UInt64, true),
            vec![
                LiteralValue::Int(0),
                LiteralValue::Null,
                LiteralValue::Int(i64::MAX),
            ],
        ),
        (
            ty(DataType::Date64, true),
            vec![
                LiteralValue::Int(-1),
                LiteralValue::Null,
                LiteralValue::Int(9),
            ],
        ),
        (
            ty(
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            vec![
                LiteralValue::Int(-1),
                LiteralValue::Null,
                LiteralValue::Int(9),
            ],
        ),
        (
            ty(DataType::Boolean, true),
            vec![
                LiteralValue::Bool(true),
                LiteralValue::Null,
                LiteralValue::Bool(false),
            ],
        ),
        (
            ty(DataType::Float32, true),
            vec![
                LiteralValue::Float(-0.0),
                LiteralValue::Null,
                LiteralValue::Float(1.5),
            ],
        ),
        (
            ty(DataType::Float64, true),
            vec![
                LiteralValue::Float(-0.0),
                LiteralValue::Null,
                LiteralValue::Float(0.1),
            ],
        ),
        (
            ty(DataType::Decimal32(8, 2), true),
            vec![
                LiteralValue::Decimal("-1.23".into()),
                LiteralValue::Null,
                LiteralValue::Decimal("45.60".into()),
            ],
        ),
        (
            ty(DataType::Decimal64(16, -2), true),
            vec![
                LiteralValue::Decimal("-1200".into()),
                LiteralValue::Null,
                LiteralValue::Decimal("4500".into()),
            ],
        ),
        (
            ty(DataType::Decimal128(30, 3), true),
            vec![
                LiteralValue::Decimal("-1.234".into()),
                LiteralValue::Null,
                LiteralValue::Decimal("45.600".into()),
            ],
        ),
        (
            ty(DataType::Decimal256(76, 0), true),
            vec![
                LiteralValue::Decimal("-1234567890123456789012345678901234567890".into()),
                LiteralValue::Null,
                LiteralValue::Decimal("45".into()),
            ],
        ),
        (
            large,
            vec![
                LiteralValue::LargeInt(i128::MIN),
                LiteralValue::Null,
                LiteralValue::LargeInt(i128::MAX),
            ],
        ),
        (
            ty(DataType::Utf8, true),
            vec![
                LiteralValue::String("é".into()),
                LiteralValue::Null,
                LiteralValue::String("".into()),
            ],
        ),
        (
            ty(DataType::LargeUtf8, true),
            vec![
                LiteralValue::String("é".into()),
                LiteralValue::Null,
                LiteralValue::String("".into()),
            ],
        ),
        (
            ty(DataType::Binary, true),
            vec![
                LiteralValue::Binary(vec![0, 255]),
                LiteralValue::Null,
                LiteralValue::Binary(vec![]),
            ],
        ),
        (
            ty(DataType::LargeBinary, true),
            vec![
                LiteralValue::Binary(vec![0, 255]),
                LiteralValue::Null,
                LiteralValue::Binary(vec![]),
            ],
        ),
        (
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
            vec![
                LiteralValue::String("{\"x\":1}".into()),
                LiteralValue::Null,
                LiteralValue::String("null".into()),
            ],
        ),
        (
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                true,
                ValueLogicalType::Variant,
            )
            .unwrap(),
            vec![
                LiteralValue::Binary(vec![0, 42, 255]),
                LiteralValue::Null,
                LiteralValue::Binary(vec![]),
            ],
        ),
        (
            ty(DataType::Null, true),
            vec![LiteralValue::Null, LiteralValue::Null, LiteralValue::Null],
        ),
    ];
    for (declared, values) in cases {
        let refs = values.iter().collect::<Vec<_>>();
        let (pool, count) =
            admit_syntax_constant_run(&refs, &declared, policy(), &Control::new(None)).unwrap();
        assert_eq!(count, values.len());
        assert_eq!(pool.array().len(), values.len());
        assert_eq!(pool.value_type(), &declared);
        assert!(
            pool.source_owner().is_none(),
            "actual SQL creation has no borrowed-source replacement"
        );
        for (ordinal, value) in values.iter().enumerate() {
            let scalar =
                admit_syntax_constant(value, &declared, policy(), &Control::new(None)).unwrap();
            assert_eq!(pool.field_ref(), scalar.pool().field_ref());
            assert_eq!(
                pool.value(ordinal as u32).unwrap().ordinal(),
                ordinal as u32
            );
            assert_eq!(
                pool.array().slice(ordinal, 1).to_data(),
                scalar.pool().array().to_data(),
                "typed syntax cell {ordinal} for {declared:?}"
            );
        }
    }
}

#[test]
fn syntax_runs_chunk_only_new_pools_under_original_policy() {
    use crate::constant::admit_syntax_constant_run;
    let values = (0..9).map(LiteralValue::Int).collect::<Vec<_>>();
    let refs = values.iter().collect::<Vec<_>>();
    let declared = ty(DataType::Int64, false);
    let (pool, count) =
        admit_syntax_constant_run(&refs, &declared, policy(), &Control::new(None)).unwrap();
    assert_eq!(count, 4);
    assert_eq!(pool.array().len(), 4);
    for row in 0..4 {
        assert_eq!(
            pool.value(row).unwrap().try_i64().unwrap(),
            Some(i64::from(row))
        );
    }
    let text = (0..4)
        .map(|_| LiteralValue::String("x".repeat(100)))
        .collect::<Vec<_>>();
    let refs = text.iter().collect::<Vec<_>>();
    let tight = ConstantPolicy {
        max_retained_buffer_bytes: 256,
        ..policy()
    };
    let (pool, count) =
        admit_syntax_constant_run(&refs, &ty(DataType::Utf8, true), tight, &Control::new(None))
            .unwrap();
    assert_eq!(count, 2);
    assert_eq!(
        pool.value(0).unwrap().try_utf8().unwrap(),
        Some("x".repeat(100).as_str())
    );
    let too_large = LiteralValue::String("x".repeat(1000));
    assert!(matches!(
        admit_syntax_constant_run(
            &[&too_large],
            &ty(DataType::Utf8, true),
            tight,
            &Control::new(None)
        ),
        Err(ConstantError::Limit(_))
    ));
    for (value, declared) in [
        (LiteralValue::Int(128), ty(DataType::Int8, false)),
        (LiteralValue::Float(0.1), ty(DataType::Float32, false)),
        (
            LiteralValue::Decimal("1.001".into()),
            ty(DataType::Decimal128(3, 2), false),
        ),
        (
            LiteralValue::LargeInt(1),
            ty(DataType::FixedSizeBinary(16), false),
        ),
        (LiteralValue::Null, ty(DataType::Utf8, false)),
    ] {
        assert!(
            admit_syntax_constant_run(&[&value], &declared, policy(), &Control::new(None)).is_err()
        );
    }
}

#[test]
fn first_literal_run_keeps_original_control_and_source_at_every_selected_prefix() {
    use crate::constant::admit_syntax_constant_run;
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection, FailureReason, SharedFailureCause, domain,
    };
    #[derive(Debug)]
    struct Source;
    impl std::fmt::Display for Source {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("original literal source")
        }
    }
    impl std::error::Error for Source {}
    impl FailureProjection for Source {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::CONSTANT, std::num::NonZeroU16::new(2).unwrap()),
            )
        }
    }
    let values = (0..1024)
        .map(|n| LiteralValue::String(format!("{n}:{}", "x".repeat(40))))
        .collect::<Vec<_>>();
    let refs = values.iter().collect::<Vec<_>>();
    let declared = ty(DataType::Utf8, true);
    let caps = ConstantPolicy {
        max_rows: 1024,
        max_logical_elements: 4096,
        ..policy()
    };
    let baseline = Control::new(None);
    assert_eq!(
        admit_syntax_constant_run(&refs, &declared, caps, &baseline)
            .unwrap()
            .1,
        1024
    );
    let trace = baseline.trace.into_inner().unwrap();
    let first_quantum = trace.iter().position(|(_, n)| *n == 256).unwrap() + 1;
    let last_quantum = trace.iter().rposition(|(_, n)| *n == 256).unwrap() + 1;
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::Source(SharedFailureCause::new(Source)),
    ] {
        for at in [1, first_quantum, last_quantum, trace.len()] {
            let control = Control::new(Some((at, cause.clone())));
            assert!(
                matches!(admit_syntax_constant_run(&refs,&declared,caps,&control),
                Err(ConstantError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}
