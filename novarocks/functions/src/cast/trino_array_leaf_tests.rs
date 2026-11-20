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

#[test]
fn numeric_profile_is_exact_and_reports_only_actual_data_failure_capability() {
    for (source, target, leaf, may_fail) in [
        (DataType::Int32, DataType::Int64, Leaf::Int32ToInt64, false),
        (
            DataType::Float64,
            DataType::Decimal128(20, 0),
            Leaf::DoubleToDecimal20,
            true,
        ),
        (
            DataType::Decimal128(20, 1),
            DataType::Float64,
            Leaf::DecimalToFloat {
                precision: 20,
                scale: 1,
                width: FloatWidth::F64,
            },
            false,
        ),
    ] {
        assert_eq!(profile(&source, &target), Some(leaf));
        assert_eq!(leaf.may_fail(), may_fail);
    }
    for (source, target) in [
        (DataType::Utf8, DataType::Int32),
        (DataType::Int64, DataType::Int32),
        (DataType::Float64, DataType::Decimal128(19, 0)),
        (DataType::Float64, DataType::Decimal128(20, 1)),
        (DataType::Decimal128(0, 0), DataType::Float64),
        (DataType::Decimal128(39, 1), DataType::Float64),
        (DataType::Decimal128(20, -1), DataType::Float64),
        (DataType::Decimal128(20, 21), DataType::Float32),
        (DataType::Decimal256(20, 1), DataType::Float64),
    ] {
        assert_eq!(profile(&source, &target), None);
    }
}

#[test]
fn integer_widening_keeps_complete_signed_input_range() {
    for input in [i32::MIN, -1, 0, 1, i32::MAX] {
        assert_eq!(
            Leaf::Int32ToInt64.convert(Input::Int32(input)),
            Ok(Value::Int64(i64::from(input)))
        );
    }
}

#[test]
fn double_decimal20_keeps_half_up_and_shortest_decimal_integer_counterexamples() {
    for (input, coefficient) in [
        (0.0, 0),
        (-0.0, 0),
        (f64::from_bits(1), 0),
        (f64::from_bits(0x3fdf_ffff_ffff_ffff), 0),
        (0.5, 1),
        (1.0, 1),
        (1.5, 2),
        (2.5, 3),
        (f64::from_bits(0x3fe0_0000_0000_0001), 1),
        (4_503_599_627_370_495.5, 4_503_599_627_370_496),
        (4_503_599_627_370_497.0, 4_503_599_627_370_497),
        (9_007_199_254_740_992.0, 9_007_199_254_740_992),
        // Java selects the closest shortest decimal, not the binary64
        // integer payload, and not a Rust-formatted-string assumption.
        (
            f64::from_bits(0x43e0_0000_0000_0000),
            9_223_372_036_854_776_000,
        ),
        (
            f64::from_bits(0x43e5_6a95_319d_63e1),
            12_345_678_901_234_567_000,
        ),
        (1e19, 10_000_000_000_000_000_000),
        (
            f64::from_bits(0x4415_af1d_78b5_8c3f),
            99_999_999_999_999_980_000,
        ),
    ] {
        for (value, expected) in [(input, coefficient), (-input, -coefficient)] {
            assert_eq!(
                Leaf::DoubleToDecimal20.convert(Input::Float64(value)),
                Ok(Value::Decimal128(expected)),
                "input bits {:016x}",
                value.to_bits()
            );
        }
    }
}

#[test]
fn double_decimal20_distinguishes_nan_invalid_cast_from_infinity_and_range() {
    for input in [f64::NAN, f64::from_bits(0xfff8_0000_0000_0001)] {
        assert_eq!(
            Leaf::DoubleToDecimal20.convert(Input::Float64(input)),
            Err(LeafError::InvalidCastArgument)
        );
    }
    for input in [
        f64::INFINITY,
        f64::NEG_INFINITY,
        1e20,
        -1e20,
        f64::MAX,
        f64::MIN,
    ] {
        assert_eq!(
            Leaf::DoubleToDecimal20.convert(Input::Float64(input)),
            Err(LeafError::NumericOutOfRange)
        );
    }
}

fn double_bits(coefficient: i128) -> u64 {
    let Value::Float64(value) = Leaf::DecimalToFloat {
        precision: 20,
        scale: 1,
        width: FloatWidth::F64,
    }
    .convert(Input::Decimal128(coefficient))
    .unwrap() else {
        panic!("exact floating-point result");
    };
    value.to_bits()
}

#[test]
fn decimal20_double_keeps_correct_rounding_and_independent_exact_bits() {
    assert_eq!(double_bits(0), 0);
    for (coefficient, expected) in [
        (1, 0x3fb9_9999_9999_999a),
        (10, 0x3ff0_0000_0000_0000),
        (35, 0x400c_0000_0000_0000),
        (45_035_996_273_704_965, 0x4330_0000_0000_0000),
        (90_071_992_547_409_930, 0x4340_0000_0000_0000),
        (90_071_992_547_409_931, 0x4340_0000_0000_0001),
        (99_999_999_999_999_999_999, 0x43e1_58e4_6091_3d00),
    ] {
        assert_eq!(double_bits(coefficient), expected);
        assert_eq!(double_bits(-coefficient), expected | (1_u64 << 63));
    }
}

#[test]
fn decimal20_double_matches_independent_decimal_text_parser_at_magnitude_boundaries() {
    // An independently implemented standard-library decimal parser supplies
    // the oracle; production uses integer quotient/remainder and emits bits.
    let compare = |coefficient: i128| {
        if coefficient.unsigned_abs() >= DECIMAL20_LIMIT {
            return;
        }
        let magnitude = coefficient.unsigned_abs();
        let decimal = format!(
            "{}{}.{}",
            if coefficient < 0 { "-" } else { "" },
            magnitude / 10,
            magnitude % 10
        );
        let expected: f64 = decimal.parse().unwrap();
        assert_eq!(
            double_bits(coefficient),
            expected.to_bits(),
            "exact decimal {decimal}"
        );
    };
    for exponent in 0..=66 {
        for delta in -3..=3 {
            let coefficient = (1_i128 << exponent) + delta;
            compare(coefficient);
            compare(-coefficient);
        }
    }
    for exponent in 0..=19 {
        for delta in -3..=3 {
            let coefficient = 10_i128.pow(exponent) + delta;
            compare(coefficient);
            compare(-coefficient);
        }
    }
}

#[test]
fn malformed_primitive_or_decimal_input_is_an_outer_contract_failure() {
    for (leaf, input) in [
        (Leaf::Int32ToInt64, Input::Float64(1.0)),
        (Leaf::Int32ToInt64, Input::Decimal128(1)),
        (Leaf::DoubleToDecimal20, Input::Int32(1)),
        (Leaf::DoubleToDecimal20, Input::Decimal128(1)),
        (
            Leaf::DecimalToFloat {
                precision: 20,
                scale: 1,
                width: FloatWidth::F64,
            },
            Input::Int32(1),
        ),
        (
            Leaf::DecimalToFloat {
                precision: 20,
                scale: 1,
                width: FloatWidth::F64,
            },
            Input::Float64(1.0),
        ),
    ] {
        assert_eq!(leaf.convert(input), Err(LeafError::TypeMismatch));
    }
    for coefficient in [
        DECIMAL20_LIMIT as i128,
        -(DECIMAL20_LIMIT as i128),
        i128::MIN,
        i128::MAX,
    ] {
        assert_eq!(
            Leaf::DecimalToFloat {
                precision: 20,
                scale: 1,
                width: FloatWidth::F64
            }
            .convert(Input::Decimal128(coefficient)),
            Err(LeafError::TypeMismatch)
        );
    }
}

fn decimal_float_bits(precision: u8, scale: i8, width: FloatWidth, coefficient: i128) -> u64 {
    let target = match width {
        FloatWidth::F32 => DataType::Float32,
        FloatWidth::F64 => DataType::Float64,
    };
    let leaf =
        profile(&DataType::Decimal128(precision, scale), &target).expect("legal decimal profile");
    assert!(!leaf.may_fail());
    match (width, leaf.convert(Input::Decimal128(coefficient)).unwrap()) {
        (FloatWidth::F32, Value::Float32(value)) => u64::from(value.to_bits()),
        (FloatWidth::F64, Value::Float64(value)) => value.to_bits(),
        _ => panic!("exact target width"),
    }
}

#[test]
fn decimal_float32_rounds_the_rational_directly_instead_of_narrowing_a_double() {
    for (precision, scale, coefficient, expected) in [
        // The exact midpoint above 1.0 ties to the lower, even float.
        (25, 24, 1000000059604644775390625_i128, 0x3f80_0000_u64),
        (
            38,
            37,
            10_000_000_596_046_447_753_906_250_000_000_000_001,
            0x3f80_0001,
        ),
        (
            38,
            37,
            10_000_000_596_046_447_753_906_249_999_999_999_999,
            0x3f80_0000,
        ),
        // This midpoint ties to the upper, even float. A decimal just below
        // it must choose the odd lower float, despite rounding to the midpoint
        // if converted through binary64 first.
        (25, 24, 1_000_000_178_813_934_326_171_875, 0x3f80_0002),
        (
            38,
            37,
            10_000_001_788_139_343_261_718_749_999_999_999_999,
            0x3f80_0001,
        ),
        (
            38,
            37,
            10_000_001_788_139_343_261_718_750_000_000_000_001,
            0x3f80_0002,
        ),
        (8, 0, 16_777_217, 0x4b80_0000),
        (8, 0, 16_777_219, 0x4b80_0002),
    ] {
        assert_eq!(
            decimal_float_bits(precision, scale, FloatWidth::F32, coefficient),
            expected
        );
        assert_eq!(
            decimal_float_bits(precision, scale, FloatWidth::F32, -coefficient),
            expected | (1_u64 << 31)
        );
    }
    // Explicitly retain a counterexample to double rounding as a test oracle.
    let midpoint = f64::from_bits(0x3ff0_0000_1000_0000);
    assert_eq!((midpoint as f32).to_bits(), 0x3f80_0000);
    assert_eq!(
        decimal_float_bits(
            38,
            37,
            FloatWidth::F32,
            10_000_000_596_046_447_753_906_250_000_000_000_001
        ),
        0x3f80_0001
    );
}

#[test]
fn decimal_float_general_limits_zero_subnormal_and_signed_ties_have_fixed_bits() {
    for (precision, scale, coefficient, f32_bits, f64_bits) in [
        (1, 1, 1_i128, 0x3dcc_cccd_u64, 0x3fb9_9999_9999_999a_u64),
        (38, 38, 1, 0x006c_e3ee, 0x380b_38fb_9daa_78e4),
        (
            38,
            0,
            99_999_999_999_999_999_999_999_999_999_999_999_999,
            0x7e96_7699,
            0x47d2_ced3_2a16_a1b1,
        ),
        (
            38,
            38,
            99_999_999_999_999_999_999_999_999_999_999_999_999,
            0x3f80_0000,
            0x3ff0_0000_0000_0000,
        ),
    ] {
        for (width, bits, sign) in [
            (FloatWidth::F32, f32_bits, 1_u64 << 31),
            (FloatWidth::F64, f64_bits, 1_u64 << 63),
        ] {
            assert_eq!(
                decimal_float_bits(precision, scale, width, coefficient),
                bits
            );
            assert_eq!(
                decimal_float_bits(precision, scale, width, -coefficient),
                bits | sign
            );
            assert_eq!(decimal_float_bits(precision, scale, width, 0), 0);
        }
    }
    assert_eq!(
        decimal_float_bits(16, 0, FloatWidth::F64, 9_007_199_254_740_993),
        0x4340_0000_0000_0000
    );
    assert_eq!(
        decimal_float_bits(16, 0, FloatWidth::F64, 9_007_199_254_740_995),
        0x4340_0000_0000_0002
    );
}

#[test]
fn every_legal_decimal_profile_matches_independent_direct_width_decimal_parsers() {
    for precision in 1_u8..=38 {
        let maximum = 10_i128.pow(u32::from(precision)) - 1;
        for scale in 0_u8..=precision {
            for coefficient in [0, 1, -1, maximum, -maximum, maximum / 2, -(maximum / 2)] {
                let magnitude = coefficient.unsigned_abs();
                let factor = 10_u128.pow(u32::from(scale));
                let decimal = if scale == 0 {
                    coefficient.to_string()
                } else {
                    format!(
                        "{}{}.{:0width$}",
                        if coefficient < 0 { "-" } else { "" },
                        magnitude / factor,
                        magnitude % factor,
                        width = usize::from(scale)
                    )
                };
                // Each parser targets its final width directly. The Float32
                // oracle never goes through a parsed Float64 intermediate.
                let expected32: f32 = decimal.parse().unwrap();
                let expected64: f64 = decimal.parse().unwrap();
                assert!(expected32.is_finite() && expected64.is_finite());
                assert_eq!(
                    decimal_float_bits(precision, scale as i8, FloatWidth::F32, coefficient),
                    u64::from(expected32.to_bits()),
                    "{decimal}"
                );
                assert_eq!(
                    decimal_float_bits(precision, scale as i8, FloatWidth::F64, coefficient),
                    expected64.to_bits(),
                    "{decimal}"
                );
                if coefficient != 0 {
                    assert!(expected32 != 0.0 && expected64 != 0.0);
                }
            }
        }
    }
}

#[test]
fn general_decimal_source_precision_and_private_metadata_remain_exact_contracts() {
    for precision in 1_u8..=38 {
        let invalid = 10_i128.pow(u32::from(precision));
        for width in [FloatWidth::F32, FloatWidth::F64] {
            let leaf = Leaf::DecimalToFloat {
                precision,
                scale: precision,
                width,
            };
            for coefficient in [invalid, -invalid, i128::MIN, i128::MAX] {
                assert_eq!(
                    leaf.convert(Input::Decimal128(coefficient)),
                    Err(LeafError::TypeMismatch)
                );
            }
        }
    }
    for (precision, scale) in [(0, 0), (39, 0), (20, 21), (255, 255)] {
        assert_eq!(
            Leaf::DecimalToFloat {
                precision,
                scale,
                width: FloatWidth::F32
            }
            .convert(Input::Decimal128(0)),
            Err(LeafError::TypeMismatch)
        );
    }
}

fn decimal_result(
    source: DataType,
    target_precision: u8,
    target_scale: i8,
    input: Input,
) -> Result<i128, LeafError> {
    let leaf = profile(
        &source,
        &DataType::Decimal128(target_precision, target_scale),
    )
    .expect("admitted decimal conversion");
    match leaf.convert(input)? {
        Value::Decimal128(value) => Ok(value),
        _ => panic!("exact decimal result"),
    }
}
fn decimal_scale(
    source_precision: u8,
    source_scale: i8,
    target_precision: u8,
    target_scale: i8,
    value: i128,
) -> Result<i128, LeafError> {
    decimal_result(
        DataType::Decimal128(source_precision, source_scale),
        target_precision,
        target_scale,
        Input::Decimal128(value),
    )
}

#[test]
fn signed_decimal_authors_preserve_all_four_source_widths_and_exact_failures() {
    for (source, width, min, max) in [
        (
            DataType::Int8,
            SignedWidth::I8,
            i64::from(i8::MIN),
            i64::from(i8::MAX),
        ),
        (
            DataType::Int16,
            SignedWidth::I16,
            i64::from(i16::MIN),
            i64::from(i16::MAX),
        ),
        (
            DataType::Int32,
            SignedWidth::I32,
            i64::from(i32::MIN),
            i64::from(i32::MAX),
        ),
        (DataType::Int64, SignedWidth::I64, i64::MIN, i64::MAX),
    ] {
        for precision in 1_u8..=38 {
            let limit = 10_u128.pow(u32::from(precision));
            for scale in 0_u8..=precision {
                for value in [min, -1, 0, 1, max] {
                    // Independent checked primitive multiplication supplies
                    // the exact coefficient oracle. Any u128 overflow also
                    // exceeds every admitted Decimal128 target precision.
                    let magnitude =
                        u128::from(value.unsigned_abs()).checked_mul(10_u128.pow(u32::from(scale)));
                    let expected = magnitude
                        .filter(|n| *n < limit)
                        .map(|n| if value < 0 { -(n as i128) } else { n as i128 })
                        .ok_or(LeafError::NumericOutOfRange);
                    assert_eq!(
                        decimal_result(
                            source.clone(),
                            precision,
                            scale as i8,
                            Input::Signed { width, value }
                        ),
                        expected
                    );
                }
            }
        }
    }
    assert_eq!(
        decimal_result(
            DataType::Int64,
            19,
            0,
            Input::Signed {
                width: SignedWidth::I64,
                value: i64::MIN
            }
        ),
        Ok(-9_223_372_036_854_775_808)
    );
    assert_eq!(
        decimal_result(
            DataType::Int64,
            18,
            0,
            Input::Signed {
                width: SignedWidth::I64,
                value: i64::MIN
            }
        ),
        Err(LeafError::NumericOutOfRange)
    );
}

#[test]
fn decimal_rescale_keeps_half_up_both_signs_carry_and_required_nested_cast() {
    for (source_precision, source_scale, target_precision, target_scale, input, expected) in [
        (4, 0, 3, 0, 1_i128, 1_i128),
        (4, 1, 3, 0, 14, 1),
        (4, 1, 3, 0, 15, 2),
        (4, 1, 3, 0, 25, 3),
        (6, 3, 5, 2, 12344, 1234),
        (6, 3, 5, 2, 12345, 1235),
        (4, 1, 3, 0, 9994, 999),
        (3, 0, 6, 3, 999, 999000),
        (
            38,
            38,
            1,
            0,
            99_999_999_999_999_999_999_999_999_999_999_999_999,
            1,
        ),
    ] {
        if target_scale > source_scale {
            assert_eq!(
                profile(
                    &DataType::Decimal128(source_precision, source_scale),
                    &DataType::Decimal128(target_precision, target_scale)
                ),
                None
            );
            continue;
        }
        assert_eq!(
            decimal_scale(
                source_precision,
                source_scale,
                target_precision,
                target_scale,
                input
            ),
            Ok(expected)
        );
        assert_eq!(
            decimal_scale(
                source_precision,
                source_scale,
                target_precision,
                target_scale,
                -input
            ),
            Ok(-expected)
        );
    }
    for value in [9995, -9995] {
        assert_eq!(
            decimal_scale(4, 1, 3, 0, value),
            Err(LeafError::NumericOutOfRange)
        );
    }
    for source_precision in [1, 38] {
        assert_eq!(
            profile(
                &DataType::Decimal128(source_precision, 0),
                &DataType::Decimal128(38, 38)
            ),
            None
        );
    }
    let first = decimal_result(
        DataType::Int32,
        4,
        0,
        Input::Signed {
            width: SignedWidth::I32,
            value: 1,
        },
    )
    .unwrap();
    assert_eq!(decimal_scale(4, 0, 3, 0, first), Ok(1));
}

fn decimal_text_rescale_oracle(
    value: i128,
    source_scale: u8,
    target_precision: u8,
    target_scale: u8,
) -> Result<i128, LeafError> {
    let mut digits = value.unsigned_abs().to_string();
    let magnitude = if target_scale >= source_scale {
        digits.extend(std::iter::repeat_n(
            '0',
            usize::from(target_scale - source_scale),
        ));
        digits.parse::<u128>().ok()
    } else {
        let discarded = usize::from(source_scale - target_scale);
        digits = format!("{:0>width$}", digits, width = discarded + 1);
        let split = digits.len() - discarded;
        let retained = digits[..split].parse::<u128>().unwrap();
        Some(retained + u128::from(digits.as_bytes()[split] >= b'5'))
    };
    let magnitude = magnitude
        .filter(|n| *n < 10_u128.pow(u32::from(target_precision)))
        .ok_or(LeafError::NumericOutOfRange)?;
    Ok(if value < 0 {
        -(magnitude as i128)
    } else {
        magnitude as i128
    })
}

#[test]
fn decimal_rescale_boundary_profiles_match_independent_decimal_digit_oracle() {
    for (source_precision, source_scale) in [
        (1, 0),
        (4, 0),
        (4, 1),
        (6, 3),
        (20, 0),
        (20, 1),
        (38, 0),
        (38, 38),
    ] {
        let max = 10_i128.pow(u32::from(source_precision)) - 1;
        for target_precision in 1_u8..=38 {
            for target_scale in 0_u8..=target_precision {
                if target_scale as i8 > source_scale {
                    assert_eq!(
                        profile(
                            &DataType::Decimal128(source_precision, source_scale),
                            &DataType::Decimal128(target_precision, target_scale as i8)
                        ),
                        None
                    );
                    continue;
                }
                for coefficient in [0, 1, -1, max, -max, max / 2, -(max / 2)] {
                    assert_eq!(
                        decimal_scale(
                            source_precision,
                            source_scale,
                            target_precision,
                            target_scale as i8,
                            coefficient
                        ),
                        decimal_text_rescale_oracle(
                            coefficient,
                            source_scale as u8,
                            target_precision,
                            target_scale
                        )
                    );
                }
            }
        }
    }
}

#[test]
fn decimal_effects_use_full_source_endpoints_including_rounding_carry() {
    for (source, target, may_fail) in [
        (DataType::Int64, DataType::Decimal128(19, 0), false),
        (DataType::Int64, DataType::Decimal128(18, 0), true),
        (DataType::Int8, DataType::Decimal128(3, 0), false),
        (DataType::Int8, DataType::Decimal128(2, 0), true),
        (DataType::Decimal128(4, 0), DataType::Decimal128(3, 0), true),
        (
            DataType::Decimal128(4, 1),
            DataType::Decimal128(4, 0),
            false,
        ),
        (DataType::Decimal128(4, 1), DataType::Decimal128(3, 0), true),
        (
            DataType::Decimal128(3, 0),
            DataType::Decimal128(6, 3),
            false,
        ),
        (
            DataType::Decimal128(38, 38),
            DataType::Decimal128(1, 0),
            false,
        ),
    ] {
        if matches!((&source,&target),(DataType::Decimal128(_,ss),DataType::Decimal128(_,ts)) if ts>ss)
        {
            assert_eq!(profile(&source, &target), None);
            continue;
        }
        assert_eq!(profile(&source, &target).unwrap().may_fail(), may_fail);
    }
}

#[test]
fn signed_tags_decimal_source_and_target_metadata_fail_on_outer_contract() {
    let leaf = profile(&DataType::Int8, &DataType::Decimal128(38, 0)).unwrap();
    for input in [
        Input::Signed {
            width: SignedWidth::I64,
            value: 1,
        },
        Input::Signed {
            width: SignedWidth::I8,
            value: 128,
        },
        Input::Signed {
            width: SignedWidth::I8,
            value: -129,
        },
        Input::Int32(1),
        Input::Decimal128(1),
    ] {
        assert_eq!(leaf.convert(input), Err(LeafError::TypeMismatch));
    }
    assert_eq!(
        decimal_scale(4, 0, 38, 0, 10000),
        Err(LeafError::TypeMismatch)
    );
    assert_eq!(
        decimal_scale(4, 0, 38, 0, -10000),
        Err(LeafError::TypeMismatch)
    );
    for source in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Decimal128(20, 1),
    ] {
        for target in [
            DataType::Decimal128(0, 0),
            DataType::Decimal128(39, 0),
            DataType::Decimal128(20, -1),
            DataType::Decimal128(20, 21),
        ] {
            assert_eq!(profile(&source, &target), None);
        }
    }
    for source in [
        DataType::Decimal128(0, 0),
        DataType::Decimal128(39, 0),
        DataType::Decimal128(20, -1),
        DataType::Decimal128(20, 21),
    ] {
        assert_eq!(profile(&source, &DataType::Decimal128(20, 1)), None);
    }
}

#[test]
fn decimal_upscale_admission_is_rejected_from_types_before_any_value() {
    for (source_precision, source_scale, target_precision, target_scale) in [
        (3, 0, 6, 3),
        (14, 0, 18, 18),
        (20, 0, 38, 18),
        (38, 37, 38, 38),
    ] {
        assert_eq!(
            profile(
                &DataType::Decimal128(source_precision, source_scale),
                &DataType::Decimal128(target_precision, target_scale)
            ),
            None
        );
        // Even a forged private recipe with a zero payload cannot bypass the
        // unsupported profile or manufacture a successful special case.
        assert_eq!(
            Leaf::DecimalRescale {
                source_precision,
                source_scale: source_scale as u8,
                target_precision,
                target_scale: target_scale as u8
            }
            .convert(Input::Decimal128(0)),
            Err(LeafError::TypeMismatch)
        );
    }
}

#[test]
fn signed_widening_admits_only_total_original_width_conversions() {
    let widths = [
        (DataType::Int8, SignedWidth::I8, -128, 127),
        (DataType::Int16, SignedWidth::I16, -32_768, 32_767),
        (
            DataType::Int32,
            SignedWidth::I32,
            -2_147_483_648,
            2_147_483_647,
        ),
        (DataType::Int64, SignedWidth::I64, i64::MIN, i64::MAX),
    ];
    for (source_index, (source, source_width, minimum, maximum)) in widths.iter().enumerate() {
        for (target_index, (target, _, _, _)) in widths.iter().enumerate() {
            if source_index >= target_index {
                assert_eq!(profile(source, target), None);
                continue;
            }
            let leaf = profile(source, target).expect("total widening");
            assert!(!leaf.may_fail());
            assert_eq!(common_profile(source, target), Some(leaf));
            for value in [*minimum, -1, 0, 1, *maximum] {
                let input = if leaf == Leaf::Int32ToInt64 {
                    Input::Int32(i32::try_from(value).unwrap())
                } else {
                    Input::Signed {
                        width: *source_width,
                        value,
                    }
                };
                assert_eq!(leaf.convert(input), Ok(Value::Int64(value)));
            }
        }
    }
    let leaf = profile(&DataType::Int8, &DataType::Int16).unwrap();
    for input in [
        Input::Signed {
            width: SignedWidth::I8,
            value: 128,
        },
        Input::Signed {
            width: SignedWidth::I16,
            value: 127,
        },
        Input::Int32(127),
    ] {
        assert_eq!(leaf.convert(input), Err(LeafError::TypeMismatch));
    }
    assert_eq!(
        Leaf::SignedWiden {
            source: SignedWidth::I64,
            target: SignedWidth::I32
        }
        .convert(Input::Signed {
            width: SignedWidth::I64,
            value: 0
        }),
        Err(LeafError::TypeMismatch)
    );
}

#[test]
fn signed_to_ieee_uses_one_target_width_rounding_and_exact_source_bounds() {
    let real = profile(&DataType::Int64, &DataType::Float32).unwrap();
    let double = profile(&DataType::Int64, &DataType::Float64).unwrap();
    assert!(!real.may_fail());
    assert!(!double.may_fail());
    // The integer lies one unit above/below a REAL midpoint. Rounding it to
    // DOUBLE first loses that unit and reverses the final ties-even decision.
    for (value, expected, via_double) in [
        (
            4_611_686_293_305_294_849_i64,
            0x5e80_0001_u32,
            0x5e80_0000_u32,
        ),
        (
            4_611_686_843_061_108_735_i64,
            0x5e80_0001_u32,
            0x5e80_0002_u32,
        ),
        (i64::MAX, 0x5f00_0000_u32, 0x5f00_0000_u32),
    ] {
        for (value, sign) in [(value, 0), (-value, 0x8000_0000)] {
            let Value::Float32(result) = real
                .convert(Input::Signed {
                    width: SignedWidth::I64,
                    value,
                })
                .unwrap()
            else {
                panic!("REAL result");
            };
            assert_eq!(result.to_bits(), expected | sign);
            assert_eq!(((value as f64) as f32).to_bits(), via_double | sign);
        }
    }
    for (value, expected) in [
        (9_007_199_254_740_993, 0x4340_0000_0000_0000),
        (9_007_199_254_740_995, 0x4340_0000_0000_0002),
        (i64::MIN, 0xc3e0_0000_0000_0000),
        (i64::MAX, 0x43e0_0000_0000_0000),
    ] {
        let Value::Float64(result) = double
            .convert(Input::Signed {
                width: SignedWidth::I64,
                value,
            })
            .unwrap()
        else {
            panic!("DOUBLE result");
        };
        assert_eq!(result.to_bits(), expected);
    }
    for (source, width, minimum, maximum) in [
        (DataType::Int8, SignedWidth::I8, -128, 127),
        (DataType::Int16, SignedWidth::I16, -32_768, 32_767),
        (
            DataType::Int32,
            SignedWidth::I32,
            -2_147_483_648,
            2_147_483_647,
        ),
        (DataType::Int64, SignedWidth::I64, i64::MIN, i64::MAX),
    ] {
        for target in [DataType::Float32, DataType::Float64] {
            let leaf = profile(&source, &target).unwrap();
            assert!(!leaf.may_fail());
            assert_eq!(common_profile(&source, &target), Some(leaf));
            assert!(
                leaf.convert(Input::Signed {
                    width,
                    value: minimum
                })
                .is_ok()
            );
            assert!(
                leaf.convert(Input::Signed {
                    width,
                    value: maximum
                })
                .is_ok()
            );
            assert_eq!(
                leaf.convert(Input::Float64(0.0)),
                Err(LeafError::TypeMismatch)
            );
            if width != SignedWidth::I64 {
                assert_eq!(
                    leaf.convert(Input::Signed {
                        width,
                        value: maximum + 1
                    }),
                    Err(LeafError::TypeMismatch)
                );
            }
        }
    }
}

#[test]
fn real_to_double_retains_actual_real_payload_and_refuses_wrong_input_width() {
    let leaf = profile(&DataType::Float32, &DataType::Float64).unwrap();
    assert_eq!(leaf, Leaf::Float32ToFloat64);
    assert_eq!(
        common_profile(&DataType::Float32, &DataType::Float64),
        Some(leaf)
    );
    assert!(!leaf.may_fail());
    for (input, expected) in [
        (0x0000_0000, 0x0000_0000_0000_0000),
        (0x8000_0000, 0x8000_0000_0000_0000),
        (0x0000_0001, 0x36a0_0000_0000_0000),
        (0x3dcc_cccd, 0x3fb9_9999_a000_0000),
        (0x7f7f_ffff, 0x47ef_ffff_e000_0000),
        (0x7f80_0000, 0x7ff0_0000_0000_0000),
        (0xff80_0000, 0xfff0_0000_0000_0000),
    ] {
        let Value::Float64(result) = leaf.convert(Input::Float32(f32::from_bits(input))).unwrap()
        else {
            panic!("DOUBLE result");
        };
        assert_eq!(result.to_bits(), expected);
    }
    let Value::Float64(nan) = leaf
        .convert(Input::Float32(f32::from_bits(0x7fc0_0123)))
        .unwrap()
    else {
        panic!("DOUBLE result");
    };
    assert!(nan.is_nan());
    assert_eq!(
        leaf.convert(Input::Float64(0.1)),
        Err(LeafError::TypeMismatch)
    );
    assert_eq!(profile(&DataType::Float64, &DataType::Float32), None);
    assert_eq!(common_profile(&DataType::Float64, &DataType::Float32), None);
}

#[test]
fn common_decimal_rescale_obeys_neutral_relation_and_keeps_explicit_upscale_closed() {
    for (source, target) in [
        (DataType::Decimal128(3, 0), DataType::Decimal128(6, 3)),
        (DataType::Decimal128(18, 0), DataType::Decimal128(38, 20)),
        (DataType::Decimal128(38, 0), DataType::Decimal128(38, 1)),
    ] {
        assert_eq!(profile(&source, &target), None);
        assert!(matches!(
            common_profile(&source, &target),
            Some(Leaf::CommonDecimalRescale { .. })
        ));
    }
    for (source, target) in [
        (DataType::Decimal128(14, 0), DataType::Decimal128(18, 18)),
        (DataType::Decimal128(6, 3), DataType::Decimal128(3, 0)),
        (DataType::Decimal128(4, 0), DataType::Decimal128(3, 0)),
        (DataType::Float64, DataType::Decimal128(20, 0)),
        (DataType::Int64, DataType::Int32),
        (DataType::Utf8, DataType::Int32),
        (DataType::Decimal128(0, 0), DataType::Decimal128(38, 0)),
        (DataType::Decimal128(20, -1), DataType::Decimal128(38, 1)),
    ] {
        assert_eq!(common_profile(&source, &target), None);
    }
    let short = common_profile(&DataType::Decimal128(3, 0), &DataType::Decimal128(6, 3)).unwrap();
    assert!(!short.may_fail());
    for (input, expected) in [(999, 999_000), (-999, -999_000), (0, 0)] {
        assert_eq!(
            short.convert(Input::Decimal128(input)),
            Ok(Value::Decimal128(expected))
        );
    }
    assert_eq!(
        short.convert(Input::Decimal128(1000)),
        Err(LeafError::TypeMismatch)
    );
    let capped =
        common_profile(&DataType::Decimal128(38, 0), &DataType::Decimal128(38, 1)).unwrap();
    assert!(capped.may_fail());
    let first_overflow = 10_i128.pow(37);
    for sign in [-1, 1] {
        assert_eq!(
            capped.convert(Input::Decimal128(sign * (first_overflow - 1))),
            Ok(Value::Decimal128(sign * (10_i128.pow(38) - 10)))
        );
        assert_eq!(
            capped.convert(Input::Decimal128(sign * first_overflow)),
            Err(LeafError::NumericOutOfRange)
        );
        assert_eq!(
            capped.convert(Input::Decimal128(sign * 10_i128.pow(38))),
            Err(LeafError::TypeMismatch)
        );
    }
    for leaf in [
        Leaf::CommonDecimalRescale {
            source_precision: 14,
            source_scale: 0,
            target_precision: 18,
            target_scale: 18,
        },
        Leaf::CommonDecimalRescale {
            source_precision: 6,
            source_scale: 3,
            target_precision: 3,
            target_scale: 0,
        },
    ] {
        assert_eq!(
            leaf.convert(Input::Decimal128(0)),
            Err(LeafError::TypeMismatch)
        );
    }
}
