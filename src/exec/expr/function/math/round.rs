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
use crate::exec::expr::decimal::{div_round_i128, pow10_i128};
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

/// Round a double value to the nearest integer.
/// Returns BIGINT (Int64).
/// Implementation matches StarRocks: round(x) = static_cast<int64_t>(x + ((x < 0) ? -0.5 : 0.5))
fn round_double_to_int(value: f64) -> i64 {
    (value + if value < 0.0 { -0.5 } else { 0.5 }) as i64
}

/// Round a double value to d decimal places.
/// Returns DOUBLE.
/// Implementation matches StarRocks double_round function.
fn round_double_to_decimals(value: f64, decimals: i64) -> f64 {
    // Handle negative decimals (round to tens, hundreds, etc.)
    let dec_negative = decimals < 0;
    let abs_dec = if dec_negative { -decimals } else { decimals } as u64;

    // Pre-compute 10^abs_dec
    let tmp = if abs_dec < 10 {
        // Fast path for small exponents
        let mut result = 1.0;
        for _ in 0..abs_dec {
            result *= 10.0;
        }
        result
    } else {
        10.0_f64.powi(abs_dec as i32)
    };

    // Pre-compute these to avoid optimization issues
    let value_div_tmp = value / tmp;
    let value_mul_tmp = value * tmp;

    // Handle infinity cases
    if dec_negative && tmp.is_infinite() {
        return 0.0;
    }
    if !dec_negative && value_mul_tmp.is_infinite() {
        return value;
    }

    // Round using f64::round
    if dec_negative {
        value_div_tmp.round() * tmp
    } else {
        value_mul_tmp.round() / tmp
    }
}

/// Round a decimal value to d decimal places.
/// Returns Decimal128 with the target scale.
fn round_decimal(value: i128, original_scale: i8, target_scale: i8) -> Result<i128, String> {
    let scale_diff = target_scale as i32 - original_scale as i32;

    if scale_diff == 0 {
        // No rounding needed
        return Ok(value);
    } else if scale_diff > 0 {
        // Scale up: multiply by 10^scale_diff
        let factor = pow10_i128(scale_diff as usize)?;
        value
            .checked_mul(factor)
            .ok_or_else(|| "decimal overflow in round".to_string())
    } else {
        // Scale down: divide by 10^(-scale_diff) with rounding
        let factor = pow10_i128((-scale_diff) as usize)?;
        Ok(div_round_i128(value, factor))
    }
}

/// Evaluate round function.
/// Supports:
/// - round(x): round double to nearest integer (returns BIGINT)
/// - round(x, d): round double to d decimal places (returns DOUBLE)
/// - round(decimal, d): round decimal to d decimal places (returns Decimal128)
pub fn eval_round(
    arena: &ExprArena,
    expr: ExprId,
    value_expr: ExprId,
    decimals_expr: Option<ExprId>,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let value_arr = arena.eval(value_expr, chunk)?;
    let output_type = arena
        .data_type(expr)
        .ok_or_else(|| "round: missing output type".to_string())?;

    // Handle single-argument round(x) - round to nearest integer
    if decimals_expr.is_none() {
        // Single argument: round to integer
        match value_arr.data_type() {
            DataType::Float64 | DataType::Float32 => {
                let f64_arr = if matches!(value_arr.data_type(), DataType::Float64) {
                    value_arr
                } else {
                    cast(&value_arr, &DataType::Float64)
                        .map_err(|e| format!("round: failed to cast to Float64: {}", e))?
                };
                let f64_arr = f64_arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "round: failed to downcast to Float64Array".to_string())?;

                let len = f64_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if f64_arr.is_null(i) {
                        values.push(None);
                    } else {
                        let v = f64_arr.value(i);
                        values.push(Some(round_double_to_int(v)));
                    }
                }
                Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
            }
            DataType::Decimal128(_, _) => {
                // For decimal, round to 0 decimal places (nearest integer)
                let dec_arr = value_arr
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "round: failed to downcast to Decimal128Array".to_string())?;
                let (_, original_scale) = match value_arr.data_type() {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => unreachable!(),
                };
                let (out_precision, out_scale) = match output_type {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err(format!(
                            "round: expected Decimal128 output type, got {:?}",
                            output_type
                        ));
                    }
                };

                let len = dec_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if dec_arr.is_null(i) {
                        values.push(None);
                    } else {
                        let v = dec_arr.value(i);
                        let rounded = round_decimal(v, original_scale, out_scale)?;
                        values.push(Some(rounded));
                    }
                }
                let array = Decimal128Array::from(values)
                    .with_precision_and_scale(out_precision, out_scale)
                    .map_err(|e| format!("round: failed to create Decimal128Array: {}", e))?;
                Ok(Arc::new(array) as ArrayRef)
            }
            _ => {
                // Cast to Float64 and round
                let f64_arr = cast(&value_arr, &DataType::Float64)
                    .map_err(|e| format!("round: failed to cast to Float64: {}", e))?;
                let f64_arr = f64_arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "round: failed to downcast to Float64Array".to_string())?;

                let len = f64_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if f64_arr.is_null(i) {
                        values.push(None);
                    } else {
                        let v = f64_arr.value(i);
                        values.push(Some(round_double_to_int(v)));
                    }
                }
                Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
            }
        }
    } else {
        // Two arguments: round(x, d)
        let decimals_arr = arena.eval(decimals_expr.unwrap(), chunk)?;

        // Get length from value_arr
        let len = value_arr.len();

        // Check if decimals is constant (length 1) or per-row (length matches value)
        let decimals_is_constant = decimals_arr.len() == 1;
        if !decimals_is_constant && decimals_arr.len() != len {
            return Err(format!(
                "round: decimals array length {} does not match value array length {}",
                decimals_arr.len(),
                len
            ));
        }

        // Cast decimals to Int64 for processing
        let decimals_i64_arr = if matches!(decimals_arr.data_type(), DataType::Int64) {
            decimals_arr
        } else {
            cast(&decimals_arr, &DataType::Int64)
                .map_err(|e| format!("round: failed to cast decimals to Int64: {}", e))?
        };
        let decimals_i64 = decimals_i64_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "round: failed to downcast decimals to Int64Array".to_string())?;

        match value_arr.data_type() {
            DataType::Decimal128(_, _) => {
                // Decimal round
                let dec_arr = value_arr
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "round: failed to downcast to Decimal128Array".to_string())?;
                let (_, original_scale) = match value_arr.data_type() {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => unreachable!(),
                };
                let (out_precision, out_scale) = match output_type {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err(format!(
                            "round: expected Decimal128 output type, got {:?}",
                            output_type
                        ));
                    }
                };

                // Clamp target_scale to valid range
                let max_precision = 38i8;
                let len = dec_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if dec_arr.is_null(i)
                        || decimals_i64.is_null(if decimals_is_constant { 0 } else { i })
                    {
                        values.push(None);
                        continue;
                    }
                    let decimals_value =
                        decimals_i64.value(if decimals_is_constant { 0 } else { i });
                    let target_scale = if decimals_value > max_precision as i64 {
                        max_precision
                    } else if decimals_value < -(max_precision as i64) {
                        -max_precision
                    } else {
                        decimals_value as i8
                    };
                    let v = dec_arr.value(i);
                    let rounded = round_decimal(v, original_scale, target_scale)?;
                    values.push(Some(rounded));
                }
                let array = Decimal128Array::from(values)
                    .with_precision_and_scale(out_precision, out_scale)
                    .map_err(|e| format!("round: failed to create Decimal128Array: {}", e))?;
                Ok(Arc::new(array) as ArrayRef)
            }
            _ => {
                // Double round
                let f64_arr = if matches!(value_arr.data_type(), DataType::Float64) {
                    value_arr
                } else {
                    cast(&value_arr, &DataType::Float64)
                        .map_err(|e| format!("round: failed to cast to Float64: {}", e))?
                };
                let f64_arr = f64_arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "round: failed to downcast to Float64Array".to_string())?;

                let len = f64_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if f64_arr.is_null(i)
                        || decimals_i64.is_null(if decimals_is_constant { 0 } else { i })
                    {
                        values.push(None);
                        continue;
                    }
                    let v = f64_arr.value(i);
                    let decimals_value =
                        decimals_i64.value(if decimals_is_constant { 0 } else { i });
                    values.push(Some(round_double_to_decimals(v, decimals_value)));
                }
                Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
            }
        }
    }
}
