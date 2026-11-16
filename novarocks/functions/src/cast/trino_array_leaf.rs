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

//! Pure non-NULL value authors for the admitted Trino ARRAY leaf profiles.
//! These functions neither select rows nor assemble Arrow values or journals.
//! Legacy scalar CAST keeps its original conversion and NULL policies.

use arrow_buffer::i256;
use arrow_schema::DataType;

const DECIMAL20_LIMIT: u128 = 100_000_000_000_000_000_000;
const FIRST_FRACTIONLESS_DOUBLE: f64 = 4_503_599_627_370_496.0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Leaf {
    Int32ToInt64,
    DoubleToDecimal20,
    SignedToDecimal {
        source: SignedWidth,
        target_precision: u8,
        target_scale: u8,
    },
    DecimalRescale {
        source_precision: u8,
        source_scale: u8,
        target_precision: u8,
        target_scale: u8,
    },
    DecimalToFloat {
        precision: u8,
        scale: u8,
        width: FloatWidth,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FloatWidth {
    F32,
    F64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SignedWidth {
    I8,
    I16,
    I32,
    I64,
}

impl SignedWidth {
    fn from_type(source: &DataType) -> Option<Self> {
        match source {
            DataType::Int8 => Some(Self::I8),
            DataType::Int16 => Some(Self::I16),
            DataType::Int32 => Some(Self::I32),
            DataType::Int64 => Some(Self::I64),
            _ => None,
        }
    }
    fn accepts(self, value: i64) -> bool {
        match self {
            Self::I8 => i8::try_from(value).is_ok(),
            Self::I16 => i16::try_from(value).is_ok(),
            Self::I32 => i32::try_from(value).is_ok(),
            Self::I64 => true,
        }
    }
    fn minimum(self) -> i64 {
        match self {
            Self::I8 => i64::from(i8::MIN),
            Self::I16 => i64::from(i16::MIN),
            Self::I32 => i64::from(i32::MIN),
            Self::I64 => i64::MIN,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Input {
    Int32(i32),
    Signed { width: SignedWidth, value: i64 },
    Float64(f64),
    Decimal128(i128),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Value {
    Int64(i64),
    Decimal128(i128),
    Float64(f64),
    Float32(f32),
}

/// Data errors retain their actual conversion reason. TypeMismatch is an
/// invocation/source contract violation and must never enter a Data journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LeafError {
    InvalidCastArgument,
    NumericOutOfRange,
    TypeMismatch,
}

/// Admission is deliberately exact. An unimplemented profile must not fall
/// back to a legacy scalar conversion with different success/NULL semantics.
pub(super) fn profile(source: &DataType, target: &DataType) -> Option<Leaf> {
    match (source, target) {
        (DataType::Int32, DataType::Int64) => Some(Leaf::Int32ToInt64),
        (DataType::Float64, DataType::Decimal128(20, 0)) => Some(Leaf::DoubleToDecimal20),
        (DataType::Decimal128(precision, scale), DataType::Float32 | DataType::Float64)
            if (1..=38).contains(precision) && *scale >= 0 && (*scale as u8) <= *precision =>
        {
            Some(Leaf::DecimalToFloat {
                precision: *precision,
                scale: *scale as u8,
                width: if target == &DataType::Float32 {
                    FloatWidth::F32
                } else {
                    FloatWidth::F64
                },
            })
        }
        (
            DataType::Decimal128(source_precision, source_scale),
            DataType::Decimal128(target_precision, target_scale),
        ) if valid_decimal_arrow(*source_precision, *source_scale)
            && valid_decimal_arrow(*target_precision, *target_scale)
            && target_scale <= source_scale =>
        {
            Some(Leaf::DecimalRescale {
                source_precision: *source_precision,
                source_scale: *source_scale as u8,
                target_precision: *target_precision,
                target_scale: *target_scale as u8,
            })
        }
        (source, DataType::Decimal128(target_precision, target_scale))
            if valid_decimal_arrow(*target_precision, *target_scale) =>
        {
            SignedWidth::from_type(source).map(|source| Leaf::SignedToDecimal {
                source,
                target_precision: *target_precision,
                target_scale: *target_scale as u8,
            })
        }
        _ => None,
    }
}

impl Leaf {
    pub(super) fn may_fail(self) -> bool {
        match self {
            Self::DoubleToDecimal20 => true,
            Self::SignedToDecimal {
                source,
                target_precision,
                target_scale,
            } => rescale_coefficient(
                i128::from(source.minimum()),
                0,
                target_precision,
                target_scale,
            )
            .is_err(),
            Self::DecimalRescale {
                source_precision,
                source_scale,
                target_precision,
                target_scale,
            } => {
                if !valid_decimal(source_precision, source_scale) {
                    return true;
                }
                let maximum = (10_u128.pow(u32::from(source_precision)) - 1) as i128;
                rescale_coefficient(maximum, source_scale, target_precision, target_scale).is_err()
            }
            _ => false,
        }
    }

    /// The caller retains the original NULL, ordinal, control and error owner.
    /// All admitted work is bounded by the fixed primitive representations.
    pub(super) fn convert(self, input: Input) -> Result<Value, LeafError> {
        match (self, input) {
            (Self::Int32ToInt64, Input::Int32(value)) => Ok(Value::Int64(i64::from(value))),
            (Self::DoubleToDecimal20, Input::Float64(value)) => {
                decimal20_from_double(value).map(Value::Decimal128)
            }
            (
                Self::DecimalToFloat {
                    precision,
                    scale,
                    width,
                },
                Input::Decimal128(value),
            ) => {
                if !(1..=38).contains(&precision)
                    || scale > precision
                    || value.unsigned_abs() >= 10_u128.pow(u32::from(precision))
                {
                    return Err(LeafError::TypeMismatch);
                }
                let bits = decimal_to_float_bits(value, scale, width);
                Ok(match width {
                    FloatWidth::F32 => Value::Float32(f32::from_bits(bits as u32)),
                    FloatWidth::F64 => Value::Float64(f64::from_bits(bits)),
                })
            }
            (
                Self::SignedToDecimal {
                    source,
                    target_precision,
                    target_scale,
                },
                Input::Signed { width, value },
            ) => {
                if width != source || !source.accepts(value) {
                    return Err(LeafError::TypeMismatch);
                }
                rescale_coefficient(i128::from(value), 0, target_precision, target_scale)
                    .map(Value::Decimal128)
            }
            (
                Self::DecimalRescale {
                    source_precision,
                    source_scale,
                    target_precision,
                    target_scale,
                },
                Input::Decimal128(value),
            ) => {
                if !valid_decimal(source_precision, source_scale)
                    || target_scale > source_scale
                    || value.unsigned_abs() >= 10_u128.pow(u32::from(source_precision))
                {
                    return Err(LeafError::TypeMismatch);
                }
                rescale_coefficient(value, source_scale, target_precision, target_scale)
                    .map(Value::Decimal128)
            }
            _ => Err(LeafError::TypeMismatch),
        }
    }
}

fn valid_decimal(precision: u8, scale: u8) -> bool {
    (1..=38).contains(&precision) && scale <= precision
}
fn valid_decimal_arrow(precision: u8, scale: i8) -> bool {
    scale >= 0 && valid_decimal(precision, scale as u8)
}

/// Exact coefficient conversion shared by signed-to-decimal and rescale leaves.
/// The original scalar array/NULL/policy authors are intentionally not invoked.
fn rescale_coefficient(
    value: i128,
    source_scale: u8,
    target_precision: u8,
    target_scale: u8,
) -> Result<i128, LeafError> {
    if source_scale > 38 || !valid_decimal(target_precision, target_scale) {
        return Err(LeafError::TypeMismatch);
    }
    let mut magnitude = i256::from_parts(value.unsigned_abs(), 0);
    let delta = i16::from(target_scale) - i16::from(source_scale);
    if delta > 0 {
        let factor = i256::from_parts(10_u128.pow(delta as u32), 0);
        // Valid Decimal128 coefficients and a scale delta at most 38 produce
        // less than 10^76, which fits the positive signed i256 range.
        magnitude = magnitude
            .checked_mul(factor)
            .expect("bounded Decimal128 scale-up fits i256");
    } else if delta < 0 {
        let factor = i256::from_parts(10_u128.pow((-delta) as u32), 0);
        let (quotient, remainder) = magnitude.wrapping_div_rem(factor);
        magnitude = quotient;
        // Trino rescale uses HALF_UP, including negative half ties. The sign
        // is restored only after rounding the exact unsigned magnitude.
        if remainder << 1_u8 >= factor {
            magnitude = magnitude + i256::ONE;
        }
    }
    if magnitude >= i256::from_parts(10_u128.pow(u32::from(target_precision)), 0) {
        return Err(LeafError::NumericOutOfRange);
    }
    let coefficient = magnitude
        .to_i128()
        .expect("declared Decimal128 coefficient fits i128");
    Ok(if value < 0 { -coefficient } else { coefficient })
}

fn decimal20_from_double(value: f64) -> Result<i128, LeafError> {
    if value.is_nan() {
        return Err(LeafError::InvalidCastArgument);
    }
    let magnitude = value.abs();
    // Pinned Trino distinguishes NaN from infinity and precision overflow.
    // 10^20 is exactly representable in binary64.
    if !value.is_finite() || magnitude >= DECIMAL20_LIMIT as f64 {
        return Err(LeafError::NumericOutOfRange);
    }
    let coefficient = if magnitude < FIRST_FRACTIONLESS_DOUBLE {
        // Every relevant integer midpoint is exactly representable here. A
        // shortest round-trip decimal cannot cross one of those midpoints.
        // Subtracting the integral part is exact; adding 0.5 is not equivalent.
        let integral = magnitude.trunc();
        integral as u128 + u128::from(magnitude - integral >= 0.5)
    } else {
        shortest_integer_decimal(magnitude)
    };
    if coefficient >= DECIMAL20_LIMIT {
        return Err(LeafError::NumericOutOfRange);
    }
    let coefficient = coefficient as i128;
    Ok(if value.is_sign_negative() {
        -coefficient
    } else {
        coefficient
    })
}

/// In [2^52, 10^20), the binary64 input is an exact integer. Every shortest
/// decimal is also integral: an exact integral representation is already
/// available with no fractional digits. Search the coarsest decimal unit that
/// round-trips, then choose the nearest admissible decimal, ties to an even
/// significand. This is the numerical value chosen by Java Double.toString;
/// no Rust/Java text-formatting equivalence is assumed.
fn shortest_integer_decimal(value: f64) -> u128 {
    let integer = value as u128;
    let original_bits = value.to_bits();
    let mut result = integer;
    let mut unit = 10_u128;
    while unit <= DECIMAL20_LIMIT {
        let quotient = integer / unit;
        let remainder = integer % unit;
        let lower = quotient * unit;
        let upper = lower + unit;
        // At powers of two, the two rounding margins are asymmetric. Check
        // both adjacent decimal candidates, not only the nearest candidate.
        let lower_matches = (lower as f64).to_bits() == original_bits;
        let upper_matches = (upper as f64).to_bits() == original_bits;
        result = match (lower_matches, upper_matches) {
            (false, false) => break,
            (true, false) => lower,
            (false, true) => upper,
            (true, true) => {
                let upper_distance = unit - remainder;
                if remainder < upper_distance || (remainder == upper_distance && quotient % 2 == 0)
                {
                    lower
                } else {
                    upper
                }
            }
        };
        unit *= 10;
    }
    result
}

/// Correctly round coefficient/10^scale directly to the target IEEE width.
/// A valid decimal(p,s), 1<=p<=38 and 0<=s<=p, is finite in both target widths.
/// Integer normalization retains all remainder bits, including Float32's
/// subnormal boundary; narrowing an intermediate double would lose them.
fn decimal_to_float_bits(value: i128, scale: u8, width: FloatWidth) -> u64 {
    let magnitude = value.unsigned_abs();
    if magnitude == 0 {
        return 0;
    }
    let denominator_magnitude = 10_u128.pow(u32::from(scale));
    let numerator = i256::from_parts(magnitude, 0);
    let denominator = i256::from_parts(denominator_magnitude, 0);
    let mut exponent =
        denominator_magnitude.leading_zeros() as i32 - magnitude.leading_zeros() as i32;
    let below_power = if exponent >= 0 {
        numerator < (denominator << exponent as u8)
    } else {
        (numerator << (-exponent) as u8) < denominator
    };
    if below_power {
        exponent -= 1;
    }
    let (fraction_bits, exponent_bits, bias, minimum_normal) = match width {
        FloatWidth::F32 => (23_u32, 8_u32, 127_i32, -126_i32),
        FloatWidth::F64 => (52, 11, 1023, -1022),
    };
    // Normal values use 24/53 significant bits. Subnormal values use the
    // minimum normal exponent with a leading zero instead of a hidden one.
    // All intermediate magnitudes are below 2^181 for the admitted domain.
    let shift = fraction_bits as i32 - exponent.max(minimum_normal);
    let (scaled, divisor) = if shift >= 0 {
        (numerator << shift as u8, denominator)
    } else {
        (numerator, denominator << (-shift) as u8)
    };
    let (quotient, remainder) = scaled.wrapping_div_rem(divisor);
    let mut significand = quotient
        .to_i128()
        .expect("decimal float quotient fits 53 bits") as u64;
    let doubled_remainder = remainder << 1_u8;
    if doubled_remainder > divisor || (doubled_remainder == divisor && significand % 2 != 0) {
        significand += 1;
    }
    let sign = u64::from(value < 0) << (fraction_bits + exponent_bits);
    if exponent < minimum_normal {
        // A carry to the hidden bit is exactly the smallest normal encoding.
        return sign | significand;
    }
    if significand == 1_u64 << (fraction_bits + 1) {
        significand >>= 1;
        exponent += 1;
    }
    let fraction = significand - (1_u64 << fraction_bits);
    sign | (((exponent + bias) as u64) << fraction_bits) | fraction
}

#[cfg(test)]
#[path = "trino_array_leaf_tests.rs"]
mod tests;
