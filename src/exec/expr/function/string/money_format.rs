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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

pub fn eval_money_format(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let num_arr = arena.eval(args[0], chunk)?;
    let len = num_arr.len();
    let mut out = Vec::with_capacity(len);

    match num_arr.data_type() {
        DataType::Int8 => {
            let arr = num_arr
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "money_format expects int8".to_string())?;
            for row in 0..len {
                if arr.is_null(row) {
                    out.push(None);
                    continue;
                }
                let cents = (arr.value(row) as i128)
                    .checked_mul(100)
                    .ok_or_else(|| "money_format overflow".to_string())?;
                out.push(Some(format_currency(cents, false, false)));
            }
        }
        DataType::Int16 => {
            let arr = num_arr
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "money_format expects int16".to_string())?;
            for row in 0..len {
                if arr.is_null(row) {
                    out.push(None);
                    continue;
                }
                let cents = (arr.value(row) as i128)
                    .checked_mul(100)
                    .ok_or_else(|| "money_format overflow".to_string())?;
                out.push(Some(format_currency(cents, false, false)));
            }
        }
        DataType::Int32 => {
            let arr = num_arr
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "money_format expects int32".to_string())?;
            for row in 0..len {
                if arr.is_null(row) {
                    out.push(None);
                    continue;
                }
                let cents = (arr.value(row) as i128)
                    .checked_mul(100)
                    .ok_or_else(|| "money_format overflow".to_string())?;
                out.push(Some(format_currency(cents, false, false)));
            }
        }
        DataType::Int64 => {
            let arr = num_arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "money_format expects int64".to_string())?;
            for row in 0..len {
                if arr.is_null(row) {
                    out.push(None);
                    continue;
                }
                let cents = (arr.value(row) as i128)
                    .checked_mul(100)
                    .ok_or_else(|| "money_format overflow".to_string())?;
                out.push(Some(format_currency(cents, false, false)));
            }
        }
        DataType::Float32 => {
            let arr = num_arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "money_format expects float".to_string())?;
            for row in 0..len {
                if arr.is_null(row) {
                    out.push(None);
                    continue;
                }
                let value = arr.value(row) as f64;
                let cents = round_half_away_from_zero(value * 100.0)?;
                let preserve_negative_zero = value.is_sign_negative() && cents == 0;
                out.push(Some(format_currency(cents, true, preserve_negative_zero)));
            }
        }
        DataType::Float64 => {
            let arr = num_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "money_format expects double".to_string())?;
            for row in 0..len {
                if arr.is_null(row) {
                    out.push(None);
                    continue;
                }
                let value = arr.value(row);
                let cents = round_half_away_from_zero(value * 100.0)?;
                let preserve_negative_zero = value.is_sign_negative() && cents == 0;
                out.push(Some(format_currency(cents, true, preserve_negative_zero)));
            }
        }
        DataType::Decimal128(_, scale) => {
            let arr = num_arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "money_format expects decimal".to_string())?;
            for row in 0..len {
                if arr.is_null(row) {
                    out.push(None);
                    continue;
                }
                let cents = decimal_to_cents(arr.value(row), *scale as i32)?;
                out.push(Some(format_currency(cents, false, false)));
            }
        }
        other => {
            return Err(format!("money_format expects numeric, got {:?}", other));
        }
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

fn round_half_away_from_zero(value: f64) -> Result<i128, String> {
    if !value.is_finite() {
        return Err("money_format input must be finite".to_string());
    }
    let rounded = if value >= 0.0 {
        (value + 0.5).floor()
    } else {
        (value - 0.5).ceil()
    };
    if rounded < i128::MIN as f64 || rounded > i128::MAX as f64 {
        return Err("money_format overflow".to_string());
    }
    Ok(rounded as i128)
}

fn pow10_i128(exp: u32) -> Result<i128, String> {
    10_i128
        .checked_pow(exp)
        .ok_or_else(|| "money_format overflow".to_string())
}

fn decimal_to_cents(value: i128, scale: i32) -> Result<i128, String> {
    if scale <= 2 {
        let factor = pow10_i128((2 - scale) as u32)?;
        return value
            .checked_mul(factor)
            .ok_or_else(|| "money_format overflow".to_string());
    }

    let divisor = pow10_i128((scale - 2) as u32)?;
    let mut cents = value / divisor;
    let remainder = value.unsigned_abs() % (divisor as u128);
    if remainder.saturating_mul(2) >= divisor as u128 {
        cents = cents
            .checked_add(if value >= 0 { 1 } else { -1 })
            .ok_or_else(|| "money_format overflow".to_string())?;
    }
    Ok(cents)
}

fn format_currency(cents: i128, omit_leading_zero: bool, preserve_negative_zero: bool) -> String {
    let sign = if cents < 0 || (preserve_negative_zero && cents == 0) {
        "-"
    } else {
        ""
    };
    let abs_cents = cents.unsigned_abs();
    let integer_part = abs_cents / 100;
    let fractional_part = abs_cents % 100;
    let integer = if omit_leading_zero && integer_part == 0 {
        String::new()
    } else {
        format_grouped_integer(integer_part)
    };
    if integer.is_empty() {
        format!("{sign}.{:02}", fractional_part)
    } else {
        format!("{sign}{integer}.{:02}", fractional_part)
    }
}

fn format_grouped_integer(v: u128) -> String {
    let digits = v.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().enumerate() {
        if idx > 0 && (digits.len() - idx) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_money_format_logic() {
        assert_string_function_logic("money_format");
    }

    #[test]
    fn test_money_format_float_style() {
        assert_eq!(super::format_currency(2, true, false), ".02");
        assert_eq!(super::format_currency(0, true, true), "-.00");
        assert_eq!(super::format_currency(50, true, false), ".50");
    }

    #[test]
    fn test_money_format_decimal_rounding() {
        assert_eq!(
            super::format_currency(
                super::decimal_to_cents(12_345_678, 3).unwrap(),
                false,
                false
            ),
            "12,345.68"
        );
        assert_eq!(
            super::format_currency(super::decimal_to_cents(5, 3).unwrap(), false, false),
            "0.01"
        );
    }
}
