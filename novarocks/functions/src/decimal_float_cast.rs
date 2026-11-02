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

//! Original Decimal128 Arrow and Decimal256 Types floating-point value authors.
//! These are distinct original formulas. No overflow policy or context is read.
use arrow_array::{
    Decimal128Array, Float32Array, Float64Array,
    types::{Float32Type, Float64Type},
};
use arrow_buffer::i256;

/// Original Arrow 58.2.0 cast_decimal_to_float Float64 closure. Division at
/// negative scales is intentional; replacing it with multiplication changes bits.
pub fn decimal128_to_f64(value: i128, scale: i8) -> f64 {
    value as f64 / 10_f64.powi(scale as i32)
}

/// Original Arrow primitive unary also transforms hidden NULL backing payloads.
/// The selected owner consumes only demanded non-NULL values using the same author.
pub fn decimal128_array_to_f64(array: &Decimal128Array, scale: i8) -> Float64Array {
    array.unary::<_, Float64Type>(|value| decimal128_to_f64(value, scale))
}

/// The original Arrow integer-to-float author, retained independently of later
/// SDK rounding changes. The low 192 bits are deliberately discarded before
/// binary64 rounding. This does not allocate and does not read decimal metadata.
pub fn decimal256_unscaled_to_f64(value: i256) -> f64 {
    match value {
        i256::MIN => -2_f64.powi(255),
        i256::ZERO => 0.0,
        i256::ONE => 1.0,
        value => {
            let mask = value >> 255;
            let redundant_sign_bits = (value ^ mask).leading_zeros() - 1;
            let shifted = value << redundant_sign_bits;
            let (_, high) = shifted.to_parts();
            let retained = (high >> 64) as i64;
            (retained as f64) * 2_f64.powi(192 - redundant_sign_bits as i32)
        }
    }
}

pub fn decimal256_to_f64(value: i256, scale: i8) -> f64 {
    // Convert i256 to f64 using the same arithmetic approach as StarRocks BE:
    // (double)unscaled / (double)scale_factor.
    // This matches StarRocks's to_float() implementation in decimalv3.h which does:
    //   *to_value = static_cast<To>(static_cast<double>(value) / static_cast<double>(scale_factor));
    let unscaled_f64 = decimal256_unscaled_to_f64(value);
    if scale <= 0 {
        // Widen before negating: every signed Arrow scale, including -128, is legal.
        let factor = 10f64.powi(-i32::from(scale));
        unscaled_f64 * factor
    } else {
        let factor = 10f64.powi(scale as i32);
        unscaled_f64 / factor
    }
}

/// Original Arrow Decimal128 Float32 closure, sharing its original f64 division.
pub fn decimal128_to_f32(value: i128, scale: i8) -> f32 {
    decimal128_to_f64(value, scale) as f32
}

/// Original Arrow primitive unary computes even hidden NULL backing values.
pub fn decimal128_array_to_f32(array: &Decimal128Array, scale: i8) -> Float32Array {
    array.unary::<_, Float32Type>(|value| decimal128_to_f32(value, scale))
}

/// Original Types and Execution Decimal256 narrowing preserves infinities.
pub fn decimal256_to_f32(value: i256, scale: i8) -> f32 {
    decimal256_to_f64(value, scale) as f32
}

/// Original Execution Float32 sanitizer's selected non-NULL value projection.
/// Types/Project Decimal casters deliberately do not consume this projection.
pub fn finite_f32_value(value: f32) -> Option<f32> {
    if !value.is_finite() {
        None
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn decimal256_unscaled_keeps_original_truncated_midpoint_bits() {
        for (value, expected) in [
            (9_223_372_036_854_776_832_i128, 0x43e0_0000_0000_0000),
            (9_223_372_036_854_776_833, 0x43e0_0000_0000_0000),
            (9_223_372_036_854_776_834, 0x43e0_0000_0000_0001),
            (-9_223_372_036_854_776_832, 0xc3e0_0000_0000_0000),
            (-9_223_372_036_854_776_833, 0xc3e0_0000_0000_0001),
        ] {
            let value = i256::from_i128(value);
            assert_eq!(decimal256_unscaled_to_f64(value).to_bits(), expected);
            assert_eq!(decimal256_to_f64(value, 0).to_bits(), expected);
        }
        let midpoint = (i256::ONE << 200) + (i256::ONE << 147) + i256::ONE;
        assert_eq!(
            decimal256_unscaled_to_f64(midpoint).to_bits(),
            0x4c70_0000_0000_0000
        );
        assert_eq!(
            decimal256_unscaled_to_f64(i256::MIN).to_bits(),
            0xcfe0_0000_0000_0000
        );
        assert_eq!(decimal256_unscaled_to_f64(i256::ZERO).to_bits(), 0);
    }

    #[test]
    fn decimal128_complete_child_cast_keeps_hidden_null_payload_and_old_bits() {
        let values = Decimal128Array::new(
            vec![12_345_678_901_234_567_890_i128, 0].into(),
            Some(arrow_buffer::NullBuffer::from(vec![false, true])),
        )
        .with_precision_and_scale(38, 2)
        .unwrap();
        let converted = decimal128_array_to_f64(&values, 2);
        assert!(converted.is_null(0));
        assert_eq!(converted.value(0).to_bits(), 0x437b_69b4_ba63_0f34);
        assert_eq!(converted.value(1).to_bits(), 0);
    }
}
