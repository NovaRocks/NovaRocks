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
use crate::exec::expr::decimal::{div_round_i256, pow10_i128, pow10_i256};
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array};
use arrow::compute::kernels::numeric::{add, div, mul, rem, sub};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_types::largeint;
use std::sync::Arc;

// Helper to cast array to Int64Array for arithmetic
fn cast_to_i64(arr: &ArrayRef) -> Result<&Int64Array, String> {
    arr.as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("expected Int64Array, got {:?}", arr.data_type()))
}

// Helper to cast array to Float64Array for arithmetic
fn cast_to_f64(arr: &ArrayRef) -> Result<&Float64Array, String> {
    arr.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| format!("expected Float64Array, got {:?}", arr.data_type()))
}

fn to_largeint_values(arr: &ArrayRef, context: &str) -> Result<Vec<Option<i128>>, String> {
    match arr.data_type() {
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let fixed = largeint::as_fixed_size_binary_array(arr, context)?;
            let mut values = Vec::with_capacity(fixed.len());
            for row in 0..fixed.len() {
                if fixed.is_null(row) {
                    values.push(None);
                } else {
                    values.push(Some(largeint::value_at(fixed, row)?));
                }
            }
            Ok(values)
        }
        DataType::Int64 => {
            let int_arr = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
            let mut values = Vec::with_capacity(int_arr.len());
            for row in 0..int_arr.len() {
                if int_arr.is_null(row) {
                    values.push(None);
                } else {
                    values.push(Some(int_arr.value(row) as i128));
                }
            }
            Ok(values)
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 => {
            let casted = arrow::compute::cast(arr, &DataType::Int64)
                .map_err(|e| format!("{context}: failed to cast operand to Int64: {e}"))?;
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
            let mut values = Vec::with_capacity(int_arr.len());
            for row in 0..int_arr.len() {
                if int_arr.is_null(row) {
                    values.push(None);
                } else {
                    values.push(Some(int_arr.value(row) as i128));
                }
            }
            Ok(values)
        }
        DataType::Null => Ok(vec![None; arr.len()]),
        other => Err(format!(
            "{context}: unsupported LARGEINT operand type: {:?}",
            other
        )),
    }
}

enum LargeIntOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

fn eval_largeint_binop(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    output_type: &DataType,
    op: LargeIntOp,
) -> Result<Option<ArrayRef>, String> {
    if !largeint::is_largeint_data_type(output_type) {
        return Ok(None);
    }
    let context = match op {
        LargeIntOp::Add => "add",
        LargeIntOp::Sub => "sub",
        LargeIntOp::Mul => "mul",
        LargeIntOp::Div => "div",
        LargeIntOp::Mod => "mod",
    };
    let lhs_values = to_largeint_values(lhs, context)?;
    let rhs_values = to_largeint_values(rhs, context)?;
    if lhs_values.len() != rhs_values.len() {
        return Err(format!("largeint {context} length mismatch"));
    }

    let mut values = Vec::with_capacity(lhs_values.len());
    for row in 0..lhs_values.len() {
        let out = match (lhs_values[row], rhs_values[row]) {
            (Some(l), Some(r)) => match op {
                LargeIntOp::Add => Some(l.wrapping_add(r)),
                LargeIntOp::Sub => Some(l.wrapping_sub(r)),
                LargeIntOp::Mul => Some(l.wrapping_mul(r)),
                LargeIntOp::Div => {
                    if r == 0 {
                        None
                    } else if l == i128::MIN && r == -1 {
                        Some(i128::MIN)
                    } else {
                        Some(l / r)
                    }
                }
                LargeIntOp::Mod => {
                    if r == 0 {
                        None
                    } else if l == i128::MIN && r == -1 {
                        Some(0)
                    } else {
                        Some(l % r)
                    }
                }
            },
            _ => None,
        };
        values.push(out);
    }
    largeint::array_from_i128(&values).map(Some)
}

#[derive(Clone, Copy)]
enum DecimalOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

fn to_decimal128_values(arr: &ArrayRef, context: &str) -> Result<(Vec<Option<i128>>, i32), String> {
    match arr.data_type() {
        DataType::Decimal128(_, scale) => {
            let typed = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Decimal128Array"))?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(typed.value(row)));
                }
            }
            Ok((out, *scale as i32))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let casted = if matches!(arr.data_type(), DataType::Int64) {
                arr.clone()
            } else {
                arrow::compute::cast(arr, &DataType::Int64).map_err(|e| {
                    format!("{context}: failed to cast integer operand to Int64: {e}")
                })?
            };
            let typed = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(typed.value(row) as i128));
                }
            }
            Ok((out, 0))
        }
        DataType::Null => Ok((vec![None; arr.len()], 0)),
        other => Err(format!(
            "{context}: unsupported Decimal128 operand type: {:?}",
            other
        )),
    }
}

fn to_decimal256_values(arr: &ArrayRef, context: &str) -> Result<(Vec<Option<i256>>, i32), String> {
    match arr.data_type() {
        DataType::Decimal256(_, scale) => {
            let typed = arr
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Decimal256Array"))?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(typed.value(row)));
                }
            }
            Ok((out, *scale as i32))
        }
        DataType::Decimal128(_, scale) => {
            let typed = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Decimal128Array"))?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(i256::from_i128(typed.value(row))));
                }
            }
            Ok((out, *scale as i32))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let casted = if matches!(arr.data_type(), DataType::Int64) {
                arr.clone()
            } else {
                arrow::compute::cast(arr, &DataType::Int64).map_err(|e| {
                    format!("{context}: failed to cast integer operand to Int64: {e}")
                })?
            };
            let typed = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
            let mut out = Vec::with_capacity(typed.len());
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    out.push(None);
                } else {
                    out.push(Some(i256::from_i128(typed.value(row) as i128)));
                }
            }
            Ok((out, 0))
        }
        ty if largeint::is_largeint_data_type(ty) => {
            let typed = largeint::as_fixed_size_binary_array(arr, context)?;
            let values = (0..typed.len())
                .map(|row| {
                    if typed.is_null(row) {
                        Ok(None)
                    } else {
                        largeint::value_at(typed, row).map(|value| Some(i256::from_i128(value)))
                    }
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok((values, 0))
        }
        DataType::Null => Ok((vec![None; arr.len()], 0)),
        other => Err(format!(
            "{context}: unsupported Decimal256 operand type: {:?}",
            other
        )),
    }
}

fn eval_decimal256_div_value(
    lhs_val: i256,
    rhs_val: i256,
    lhs_scale_i32: i32,
    rhs_scale_i32: i32,
    out_scale_i32: i32,
) -> Result<Option<i256>, String> {
    if rhs_val == i256::ZERO {
        return Ok(None);
    }
    let exponent = out_scale_i32 + rhs_scale_i32 - lhs_scale_i32;
    let numerator = if exponent >= 0 {
        let factor = match pow10_i256(exponent as usize) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        match lhs_val.checked_mul(factor) {
            Some(v) => v,
            None => return Ok(None),
        }
    } else {
        let factor = match pow10_i256((-exponent) as usize) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        lhs_val
            .checked_div(factor)
            .ok_or_else(|| "decimal overflow".to_string())?
    };
    Ok(Some(div_round_i256(numerator, rhs_val)?))
}

fn checked_decimal_divide_half_up(numerator: i128, denominator: i128) -> Option<i128> {
    let quotient = numerator.checked_div(denominator)?;
    let remainder = numerator.checked_rem(denominator)?;
    let magnitude = denominator.unsigned_abs();
    let threshold = (magnitude >> 1) + (magnitude & 1);
    if remainder.unsigned_abs() >= threshold {
        quotient.checked_add(if (numerator < 0) ^ (denominator < 0) {
            -1
        } else {
            1
        })
    } else {
        Some(quotient)
    }
}

fn decimal_overflow_error(op: DecimalOp) -> String {
    let name = match op {
        DecimalOp::Add => "add",
        DecimalOp::Sub => "sub",
        DecimalOp::Mul => "mul",
        DecimalOp::Div => "div",
        DecimalOp::Mod => "mod",
    };
    format!("Expr evaluate meet error: The '{name}' operation involving decimal values overflows")
}

fn eval_decimal_binop(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    output_type: &DataType,
    op: DecimalOp,
    strict_overflow: bool,
    decimal_overflow_policy: DecimalOverflowPolicy,
) -> Result<Option<ArrayRef>, String> {
    let is_decimal =
        |ty: &DataType| matches!(ty, DataType::Decimal128(_, _) | DataType::Decimal256(_, _));
    if (largeint::is_largeint_data_type(lhs.data_type()) && is_decimal(rhs.data_type()))
        || (largeint::is_largeint_data_type(rhs.data_type()) && is_decimal(lhs.data_type()))
    {
        let operation = match &op {
            DecimalOp::Add => novarocks_type_contract::ArithmeticOperator::Add,
            DecimalOp::Sub => novarocks_type_contract::ArithmeticOperator::Subtract,
            DecimalOp::Mul => novarocks_type_contract::ArithmeticOperator::Multiply,
            DecimalOp::Div => novarocks_type_contract::ArithmeticOperator::Divide,
            DecimalOp::Mod => novarocks_type_contract::ArithmeticOperator::Modulo,
        };
        if novarocks_type_contract::arithmetic_result_type_with_op(
            lhs.data_type(),
            rhs.data_type(),
            operation,
        )
        .as_ref()
            != Some(output_type)
        {
            return Err(
                "Decimal/LARGEINT arithmetic differs from its frozen add/subtract rule".to_string(),
            );
        }
    }
    match output_type {
        DataType::Decimal128(out_precision, out_scale) => {
            if !matches!(
                lhs.data_type(),
                DataType::Decimal128(_, _)
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Null
            ) || !matches!(
                rhs.data_type(),
                DataType::Decimal128(_, _)
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Null
            ) {
                return Ok(None);
            }
            let (lhs_values, lhs_scale_i32) = to_decimal128_values(lhs, "decimal arithmetic lhs")?;
            let (rhs_values, rhs_scale_i32) = to_decimal128_values(rhs, "decimal arithmetic rhs")?;
            if lhs_values.len() != rhs_values.len() {
                return Err("decimal arithmetic length mismatch".to_string());
            }
            let mut values = Vec::with_capacity(lhs_values.len());
            let ls = lhs_scale_i32;
            let rs = rhs_scale_i32;
            let os = i32::from(*out_scale);
            if matches!(op, DecimalOp::Add | DecimalOp::Sub | DecimalOp::Mod)
                && (os < ls || os < rs)
            {
                return Err("frozen decimal add/sub/mod scale mismatch".to_string());
            }
            let precision_limit = 10_u128
                .checked_pow(u32::from(*out_precision))
                .filter(|_| (1..=38).contains(out_precision))
                .ok_or_else(|| "invalid frozen Decimal128 precision".to_string())?;
            // All metadata-derived factors are computed once per batch.
            let factor = |exponent: i32| pow10_i128(exponent.unsigned_abs() as usize).ok();
            let left_factor = factor(os - ls);
            let right_factor = factor(os - rs);
            let product_diff = os - ls - rs;
            let product_factor = factor(product_diff);
            let division_diff = os + rs - ls;
            let division_factor = factor(division_diff);
            for row in 0..lhs_values.len() {
                let (Some(left), Some(right)) = (lhs_values[row], rhs_values[row]) else {
                    values.push(None);
                    continue;
                };
                if matches!(op, DecimalOp::Div | DecimalOp::Mod) && right == 0 {
                    values.push(None);
                    continue;
                }
                let checked = match op {
                    DecimalOp::Add | DecimalOp::Sub | DecimalOp::Mod => left_factor
                        .and_then(|factor| left.checked_mul(factor))
                        .zip(right_factor.and_then(|factor| right.checked_mul(factor)))
                        .and_then(|(left, right)| match op {
                            DecimalOp::Add => left.checked_add(right),
                            DecimalOp::Sub => left.checked_sub(right),
                            DecimalOp::Mod => left.checked_rem(right),
                            _ => unreachable!(),
                        }),
                    DecimalOp::Mul => left.checked_mul(right).and_then(|product| {
                        product_factor.and_then(|factor| {
                            if product_diff >= 0 {
                                product.checked_mul(factor)
                            } else {
                                product.checked_div(factor)
                            }
                        })
                    }),
                    DecimalOp::Div => division_factor
                        .and_then(|factor| {
                            if division_diff >= 0 {
                                left.checked_mul(factor)
                            } else {
                                left.checked_div(factor)
                            }
                        })
                        .and_then(|numerator| checked_decimal_divide_half_up(numerator, right)),
                }
                .filter(|value| value.unsigned_abs() < precision_limit);
                match checked {
                    Some(value) => values.push(Some(value)),
                    None if decimal_overflow_policy == DecimalOverflowPolicy::ReportError
                        || (strict_overflow && matches!(op, DecimalOp::Mul)) =>
                    {
                        return Err(decimal_overflow_error(op));
                    }
                    None => values.push(None),
                }
            }
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| e.to_string())?;
            Ok(Some(Arc::new(array)))
        }
        DataType::Decimal256(out_precision, out_scale) => {
            let (lhs_values, lhs_scale_i32) = to_decimal256_values(lhs, "decimal arithmetic lhs")?;
            let (rhs_values, rhs_scale_i32) = to_decimal256_values(rhs, "decimal arithmetic rhs")?;
            if lhs_values.len() != rhs_values.len() {
                return Err("decimal arithmetic length mismatch".to_string());
            }
            let out_scale_i32 = *out_scale as i32;
            let mut values: Vec<Option<i256>> = Vec::with_capacity(lhs_values.len());
            let mut had_mul_overflow = false;
            let mut had_numeric_overflow = false;
            let precision_limit = pow10_i256(*out_precision as usize)?;
            for row in 0..lhs_values.len() {
                let (Some(lhs_val), Some(rhs_val)) = (lhs_values[row], rhs_values[row]) else {
                    values.push(None);
                    continue;
                };
                let out_val = match op {
                    DecimalOp::Add | DecimalOp::Sub => {
                        if out_scale_i32 < lhs_scale_i32 || out_scale_i32 < rhs_scale_i32 {
                            return Err("decimal add/sub scale mismatch".to_string());
                        }
                        let lhs_factor = match pow10_i256((out_scale_i32 - lhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        };
                        let rhs_factor = match pow10_i256((out_scale_i32 - rhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        };
                        let Some(lhs_scaled) = lhs_val.checked_mul(lhs_factor) else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        let Some(rhs_scaled) = rhs_val.checked_mul(rhs_factor) else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        let out = if matches!(op, DecimalOp::Add) {
                            lhs_scaled.checked_add(rhs_scaled)
                        } else {
                            lhs_scaled.checked_sub(rhs_scaled)
                        };
                        let Some(out) = out else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        out
                    }
                    DecimalOp::Mul => {
                        let scale_in = lhs_scale_i32 + rhs_scale_i32;
                        let diff = out_scale_i32 - scale_in;
                        let Some(product) = lhs_val.checked_mul(rhs_val) else {
                            had_mul_overflow = true;
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        if diff >= 0 {
                            let factor = match pow10_i256(diff as usize) {
                                Ok(v) => v,
                                Err(_) => {
                                    had_mul_overflow = true;
                                    had_numeric_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            };
                            let Some(out) = product.checked_mul(factor) else {
                                had_mul_overflow = true;
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            };
                            out
                        } else {
                            let factor = match pow10_i256((-diff) as usize) {
                                Ok(v) => v,
                                Err(_) => {
                                    had_mul_overflow = true;
                                    had_numeric_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            };
                            match product.checked_div(factor) {
                                Some(v) => v,
                                None => {
                                    had_mul_overflow = true;
                                    had_numeric_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            }
                        }
                    }
                    DecimalOp::Div => {
                        let Some(divided) = eval_decimal256_div_value(
                            lhs_val,
                            rhs_val,
                            lhs_scale_i32,
                            rhs_scale_i32,
                            out_scale_i32,
                        )?
                        else {
                            had_numeric_overflow |= rhs_val != i256::ZERO;
                            values.push(None);
                            continue;
                        };
                        divided
                    }
                    DecimalOp::Mod => {
                        if rhs_val == i256::ZERO {
                            values.push(None);
                            continue;
                        }
                        if out_scale_i32 < lhs_scale_i32 || out_scale_i32 < rhs_scale_i32 {
                            return Err("decimal mod scale mismatch".to_string());
                        }
                        let lhs_factor = match pow10_i256((out_scale_i32 - lhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        };
                        let rhs_factor = match pow10_i256((out_scale_i32 - rhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        };
                        let Some(lhs_scaled) = lhs_val.checked_mul(lhs_factor) else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        let Some(rhs_scaled) = rhs_val.checked_mul(rhs_factor) else {
                            had_numeric_overflow = true;
                            values.push(None);
                            continue;
                        };
                        match lhs_scaled.checked_rem(rhs_scaled) {
                            Some(v) => v,
                            None => {
                                had_numeric_overflow = true;
                                values.push(None);
                                continue;
                            }
                        }
                    }
                };
                // Declared precision and carrier capacity both bound the result;
                // the frozen policy determines how numeric overflow is returned.
                if out_val <= -precision_limit || out_val >= precision_limit {
                    had_numeric_overflow = true;
                    had_mul_overflow |= matches!(op, DecimalOp::Mul);
                    values.push(None);
                    continue;
                }
                values.push(Some(out_val));
            }
            if (decimal_overflow_policy == DecimalOverflowPolicy::ReportError
                && had_numeric_overflow)
                || (strict_overflow && matches!(op, DecimalOp::Mul) && had_mul_overflow)
            {
                return Err(decimal_overflow_error(op));
            }
            let array = Decimal256Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| e.to_string())?;
            Ok(Some(Arc::new(array)))
        }
        _ => Ok(None),
    }
}

// Generic Arrow arithmetic operation with type coercion
fn eval_numeric_binop_arrays<F1, F2>(
    lhs: ArrayRef,
    rhs: ArrayRef,
    int_op: F1,
    float_op: F2,
) -> Result<ArrayRef, String>
where
    F1: FnOnce(
        &Int64Array,
        &Int64Array,
    ) -> Result<Arc<dyn arrow::array::Array>, arrow::error::ArrowError>,
    F2: FnOnce(
        &Float64Array,
        &Float64Array,
    ) -> Result<Arc<dyn arrow::array::Array>, arrow::error::ArrowError>,
{
    use arrow::compute::cast;

    let is_float = |dt: &DataType| matches!(dt, DataType::Float32 | DataType::Float64);
    let is_lhs_float = is_float(lhs.data_type());
    let is_rhs_float = is_float(rhs.data_type());

    if is_lhs_float || is_rhs_float {
        let lhs_f64_arr = if matches!(lhs.data_type(), DataType::Float64) {
            lhs
        } else {
            cast(&lhs, &DataType::Float64).map_err(|e| e.to_string())?
        };
        let rhs_f64_arr = if matches!(rhs.data_type(), DataType::Float64) {
            rhs
        } else {
            cast(&rhs, &DataType::Float64).map_err(|e| e.to_string())?
        };
        let lhs_f64 = cast_to_f64(&lhs_f64_arr)?;
        let rhs_f64 = cast_to_f64(&rhs_f64_arr)?;
        float_op(lhs_f64, rhs_f64)
            .map_err(|e| e.to_string())
            .map(|arc| arc as ArrayRef)
    } else {
        let lhs_i64_arr = if matches!(lhs.data_type(), DataType::Int64) {
            lhs
        } else {
            cast(&lhs, &DataType::Int64).map_err(|e| e.to_string())?
        };
        let rhs_i64_arr = if matches!(rhs.data_type(), DataType::Int64) {
            rhs
        } else {
            cast(&rhs, &DataType::Int64).map_err(|e| e.to_string())?
        };
        let lhs_i64 = cast_to_i64(&lhs_i64_arr)?;
        let rhs_i64 = cast_to_i64(&rhs_i64_arr)?;
        int_op(lhs_i64, rhs_i64)
            .map_err(|e| e.to_string())
            .map(|arc| arc as ArrayRef)
    }
}

fn cast_numeric_output(result: ArrayRef, output_type: &DataType) -> Result<ArrayRef, String> {
    use arrow::compute::cast;
    if matches!(output_type, DataType::Null) || result.data_type() == output_type {
        return Ok(result);
    }
    match output_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64 => cast(&result, output_type).map_err(|e| e.to_string()),
        other => Err(format!("arithmetic output type mismatch: {:?}", other)),
    }
}

pub fn eval_add(
    arena: &ExprArena,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Add)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Add,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = add(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = add(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_sub(
    arena: &ExprArena,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Sub)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Sub,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = sub(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = sub(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_mul(
    arena: &ExprArena,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Mul)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Mul,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = mul(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = mul(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}

pub fn eval_div(
    arena: &ExprArena,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    // Replace zeros in the divisor with NULLs so that division by zero
    // returns NULL instead of an error (matches StarRocks behavior).
    let rhs = nullify_zeros(&rhs);
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Div)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Div,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    // StarRocks: integer / integer → DOUBLE. Cast integer inputs to Float64
    // BEFORE dividing so that the result preserves fractional parts.
    let both_integral = is_integer_type(lhs.data_type()) && is_integer_type(rhs.data_type());
    if both_integral && matches!(output_type, DataType::Float64) {
        let lhs_f = arrow::compute::cast(&lhs, &DataType::Float64)
            .map_err(|e| format!("div cast lhs: {e}"))?;
        let rhs_f = arrow::compute::cast(&rhs, &DataType::Float64)
            .map_err(|e| format!("div cast rhs: {e}"))?;
        let result = eval_numeric_binop_arrays(
            lhs_f,
            rhs_f,
            |x, y| {
                let result = div(x, y)?;
                Ok(Arc::new(result))
            },
            |x, y| {
                let result = div(x, y)?;
                Ok(Arc::new(result))
            },
        )?;
        return Ok(result);
    }
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = div(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = div(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}

fn is_integer_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// Replace zero values in a numeric array with NULLs for safe division.
fn nullify_zeros(arr: &ArrayRef) -> ArrayRef {
    use arrow::array::BooleanArray;
    let len = arr.len();
    let mut is_zero_buf = vec![false; len];
    match arr.data_type() {
        DataType::Int8 => {
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int8Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0 {
                        *is_zero = true;
                    }
                }
            }
        }
        DataType::Int16 => {
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int16Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0 {
                        *is_zero = true;
                    }
                }
            }
        }
        DataType::Int32 => {
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0 {
                        *is_zero = true;
                    }
                }
            }
        }
        DataType::Int64 => {
            if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0 {
                        *is_zero = true;
                    }
                }
            }
        }
        DataType::Float64 => {
            if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                for (i, is_zero) in is_zero_buf.iter_mut().enumerate().take(len) {
                    if !a.is_null(i) && a.value(i) == 0.0 {
                        *is_zero = true;
                    }
                }
            }
        }
        _ => return arr.clone(),
    }
    let mask = BooleanArray::from(is_zero_buf);
    arrow::compute::nullif(arr, &mask).unwrap_or_else(|_| arr.clone())
}

pub fn eval_mod(
    arena: &ExprArena,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
    decimal_overflow_policy: DecimalOverflowPolicy,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
    let output_type = arena.data_type(expr).cloned().unwrap_or(DataType::Null);
    if let Some(arr) = eval_largeint_binop(&lhs, &rhs, &output_type, LargeIntOp::Mod)? {
        return Ok(arr);
    }
    if let Some(arr) = eval_decimal_binop(
        &lhs,
        &rhs,
        &output_type,
        DecimalOp::Mod,
        arena.allow_throw_exception(),
        decimal_overflow_policy,
    )? {
        return Ok(arr);
    }
    let result = eval_numeric_binop_arrays(
        lhs,
        rhs,
        |x, y| {
            let result = rem(x, y)?;
            Ok(Arc::new(result))
        },
        |x, y| {
            let result = rem(x, y)?;
            Ok(Arc::new(result))
        },
    )?;
    cast_numeric_output(result, &output_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Decimal128Array, FixedSizeBinaryArray, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    use novarocks_types::largeint;

    fn create_test_chunk_int(values: Vec<i64>) -> Chunk {
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col0",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_chunk_float(values: Vec<f64>) -> Chunk {
        let array = Arc::new(Float64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col0",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn create_test_chunk_two_decimals(
        left: Vec<Option<i128>>,
        right: Vec<Option<i128>>,
        precision: u8,
        scale: i8,
    ) -> Chunk {
        let left_arr = Arc::new(
            Decimal128Array::from(left)
                .with_precision_and_scale(precision, scale)
                .expect("left decimal array"),
        ) as ArrayRef;
        let right_arr = Arc::new(
            Decimal128Array::from(right)
                .with_precision_and_scale(precision, scale)
                .expect("right decimal array"),
        ) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", DataType::Decimal128(precision, scale), true),
            Field::new("right", DataType::Decimal128(precision, scale), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left_arr, right_arr]).expect("batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1), SlotId::new(2)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    #[test]
    fn mixed_decimal_largeint_prepared_columns_preserve_extrema_and_nulls() {
        let decimal = Arc::new(
            Decimal128Array::from(vec![
                Some(1250000000000000_i128),
                Some(-1250000000000000),
                None,
                Some(1),
            ])
            .with_precision_and_scale(38, 15)
            .unwrap(),
        ) as ArrayRef;
        let integers =
            largeint::array_from_i128(&[Some(i128::MAX), Some(i128::MIN), Some(1), None]).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("decimal", decimal.data_type().clone(), true),
                Field::new("integer", integers.data_type().clone(), true),
            ])),
            vec![decimal, integers],
        )
        .unwrap();
        let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let decimal = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal128(38, 15),
        );
        let integer = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::FixedSizeBinary(16),
        );
        let out = DataType::Decimal256(55, 15);
        let add = arena.push_typed(
            ExprNode::Add(decimal, integer, DecimalOverflowPolicy::OutputNull),
            out.clone(),
        );
        let add_reverse = arena.push_typed(
            ExprNode::Add(integer, decimal, DecimalOverflowPolicy::OutputNull),
            out.clone(),
        );
        let sub = arena.push_typed(
            ExprNode::Sub(decimal, integer, DecimalOverflowPolicy::OutputNull),
            out.clone(),
        );
        let reverse = arena.push_typed(
            ExprNode::Sub(integer, decimal, DecimalOverflowPolicy::OutputNull),
            out.clone(),
        );
        let unsupported = arena.push_typed(
            ExprNode::Mul(decimal, integer, DecimalOverflowPolicy::OutputNull),
            out,
        );
        let frozen = arena.into_immutable().unwrap();
        let prepared = ExprArena::from_immutable(&frozen);
        let factor = pow10_i256(15).unwrap();
        let scaled = [
            i256::from_i128(i128::MAX).checked_mul(factor).unwrap(),
            i256::from_i128(i128::MIN).checked_mul(factor).unwrap(),
        ];
        let coefficient = [
            i256::from_i128(1250000000000000),
            i256::from_i128(-1250000000000000),
        ];
        for (expr, expected) in [
            (
                add,
                vec![
                    Some(scaled[0].checked_add(coefficient[0]).unwrap()),
                    Some(scaled[1].checked_add(coefficient[1]).unwrap()),
                    None,
                    None,
                ],
            ),
            (
                add_reverse,
                vec![
                    Some(scaled[0].checked_add(coefficient[0]).unwrap()),
                    Some(scaled[1].checked_add(coefficient[1]).unwrap()),
                    None,
                    None,
                ],
            ),
            (
                sub,
                vec![
                    Some(coefficient[0].checked_sub(scaled[0]).unwrap()),
                    Some(coefficient[1].checked_sub(scaled[1]).unwrap()),
                    None,
                    None,
                ],
            ),
            (
                reverse,
                vec![
                    Some(scaled[0].checked_sub(coefficient[0]).unwrap()),
                    Some(scaled[1].checked_sub(coefficient[1]).unwrap()),
                    None,
                    None,
                ],
            ),
        ] {
            let output = prepared.eval(expr, &chunk).unwrap();
            assert_eq!(output.data_type(), &DataType::Decimal256(55, 15));
            let output = output.as_any().downcast_ref::<Decimal256Array>().unwrap();
            assert_eq!(output.iter().collect::<Vec<_>>(), expected);
        }
        assert!(
            prepared
                .eval(unsupported, &chunk)
                .unwrap_err()
                .contains("frozen add/subtract rule")
        );
    }

    #[test]
    fn mixed_decimal_largeint_scale36_fits_precision76() {
        let decimal = Arc::new(
            Decimal128Array::from(vec![Some(1_i128)])
                .with_precision_and_scale(38, 36)
                .unwrap(),
        ) as ArrayRef;
        let integer = largeint::array_from_i128(&[Some(i128::MAX)]).unwrap();
        let expected = i256::from_i128(i128::MAX)
            .checked_mul(pow10_i256(36).unwrap())
            .unwrap()
            .checked_add(i256::ONE)
            .unwrap();
        let output = eval_decimal_binop(
            &decimal,
            &integer,
            &DataType::Decimal256(76, 36),
            DecimalOp::Add,
            false,
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap()
        .unwrap();
        let output = output.as_any().downcast_ref::<Decimal256Array>().unwrap();
        assert_eq!(output.value(0), expected);
        assert!(
            eval_decimal_binop(
                &decimal,
                &integer,
                &DataType::Decimal128(38, 36),
                DecimalOp::Add,
                false,
                DecimalOverflowPolicy::OutputNull,
            )
            .unwrap_err()
            .contains("frozen add/subtract rule")
        );
    }

    #[test]
    fn test_add_integers() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let add = arena.push_typed(
            ExprNode::Add(lit5, lit3, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1, 2, 3]);

        let result = arena.eval(add, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.len(), 3);
        assert_eq!(result_arr.value(0), 8);
    }

    #[test]
    fn test_sub_integers() {
        let mut arena = ExprArena::default();
        let lit10 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let sub = arena.push_typed(
            ExprNode::Sub(lit10, lit3, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(sub, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 7);
    }

    #[test]
    fn test_mul_integers() {
        let mut arena = ExprArena::default();
        let lit6 = arena.push(ExprNode::Literal(LiteralValue::Int64(6)));
        let lit7 = arena.push(ExprNode::Literal(LiteralValue::Int64(7)));
        let mul = arena.push_typed(
            ExprNode::Mul(lit6, lit7, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(mul, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 42);
    }

    #[test]
    fn test_div_integers() {
        let mut arena = ExprArena::default();
        let lit20 = arena.push(ExprNode::Literal(LiteralValue::Int64(20)));
        let lit4 = arena.push(ExprNode::Literal(LiteralValue::Int64(4)));
        let div = arena.push_typed(
            ExprNode::Div(lit20, lit4, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(div, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 5);
    }

    #[test]
    fn test_mod_integers() {
        let mut arena = ExprArena::default();
        let lit10 = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let rem = arena.push_typed(
            ExprNode::Mod(lit10, lit3, DecimalOverflowPolicy::OutputNull),
            DataType::Int64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(rem, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 1);
    }

    #[test]
    fn test_add_floats() {
        let mut arena = ExprArena::default();
        let lit1 = arena.push(ExprNode::Literal(LiteralValue::Float64(1.5)));
        let lit2 = arena.push(ExprNode::Literal(LiteralValue::Float64(2.3)));
        let add = arena.push_typed(
            ExprNode::Add(lit1, lit2, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );

        let chunk = create_test_chunk_float(vec![0.0]);

        let result = arena.eval(add, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert!((result_arr.value(0) - 3.8).abs() < 0.0001);
    }

    #[test]
    fn test_mixed_int_float() {
        let mut arena = ExprArena::default();
        let lit_int = arena.push(ExprNode::Literal(LiteralValue::Int64(10)));
        let lit_float = arena.push(ExprNode::Literal(LiteralValue::Float64(2.5)));
        let mul = arena.push_typed(
            ExprNode::Mul(lit_int, lit_float, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );

        let chunk = create_test_chunk_int(vec![1]);

        let result = arena.eval(mul, &chunk).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert!((result_arr.value(0) - 25.0).abs() < 0.0001);
    }

    #[test]
    fn test_add_largeint_and_bigint_returns_largeint() {
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(
            ExprNode::Literal(LiteralValue::LargeInt(9_223_372_036_854_775_808_i128)),
            DataType::FixedSizeBinary(16),
        );
        let rhs = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let add = arena.push_typed(
            ExprNode::Add(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::FixedSizeBinary(16),
        );

        let chunk = create_test_chunk_int(vec![1]);
        let result = arena.eval(add, &chunk).unwrap();
        let result_arr = result
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let parsed = largeint::i128_from_be_bytes(result_arr.value(0)).unwrap();
        assert_eq!(parsed, 9_223_372_036_854_775_809_i128);
    }

    #[test]
    fn test_mul_largeint_and_bigint_returns_largeint() {
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(
            ExprNode::Literal(LiteralValue::LargeInt(
                170141183460469231731687303715884105727_i128,
            )),
            DataType::FixedSizeBinary(16),
        );
        let rhs = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(0)), DataType::Int64);
        let mul = arena.push_typed(
            ExprNode::Mul(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::FixedSizeBinary(16),
        );

        let chunk = create_test_chunk_int(vec![1]);
        let result = arena.eval(mul, &chunk).unwrap();
        let result_arr = result
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let parsed = largeint::i128_from_be_bytes(result_arr.value(0)).unwrap();
        assert_eq!(parsed, 0);
    }

    #[test]
    fn frozen_float64_decimal_product_casts_operands_before_multiplication() {
        let lhs_type = DataType::Decimal128(30, 10);
        let rhs_type = DataType::Decimal128(18, 9);
        let lhs_coefficient = 123456789012345678901234567890_i128;
        let rhs_coefficient = 123456789123456789_i128;
        let lhs_array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(lhs_coefficient),
                Some(-lhs_coefficient),
                None,
                Some(0),
            ])
            .with_precision_and_scale(30, 10)
            .unwrap(),
        );
        let rhs_array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![
                Some(rhs_coefficient),
                Some(-rhs_coefficient),
                Some(rhs_coefficient),
                Some(rhs_coefficient),
            ])
            .with_precision_and_scale(18, 9)
            .unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", lhs_type.clone(), true),
            Field::new("right", rhs_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![lhs_array, rhs_array]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), lhs_type);
        let rhs = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), rhs_type);
        let lhs_f64 = arena.push_typed(
            ExprNode::Cast(lhs, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );
        let rhs_f64 = arena.push_typed(
            ExprNode::Cast(rhs, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );
        let promoted = arena.push_typed(
            ExprNode::Mul(lhs_f64, rhs_f64, DecimalOverflowPolicy::OutputNull),
            DataType::Float64,
        );
        let checked = arena.push_typed(
            ExprNode::Mul(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(38, 19),
        );
        let output = arena.eval(promoted, &chunk).unwrap();
        assert_eq!(output.data_type(), &DataType::Float64);
        let output = output.as_any().downcast_ref::<Float64Array>().unwrap();
        // Independent original-SQL coefficient -> binary64 cast -> binary64 product oracle.
        assert_eq!(output.value(0).to_bits(), 0x4593_b303_f039_904e);
        assert_eq!(output.value(1).to_bits(), 0x4593_b303_f039_904e);
        assert!(output.is_null(2));
        assert!(!output.is_null(3));
        assert_eq!(output.value(3), 0.0);
        let checked = arena.eval(checked, &chunk).unwrap();
        let checked = checked.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(checked.is_null(0));
        assert!(checked.is_null(1));
        assert!(checked.is_null(2));
        assert_eq!(checked.value(3), 0);
    }

    #[test]
    fn test_decimal_div_precision_overflow_returns_null() {
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal128(38, 18),
        );
        let rhs = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::Decimal128(38, 18),
        );
        let div_expr = arena.push_typed(
            ExprNode::Div(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(38, 38),
        );

        let chunk = create_test_chunk_two_decimals(
            vec![Some(-2_516_460_439_000_000_000_000_i128)],
            vec![Some(1_673_370_000_000_000_000_000_i128)],
            38,
            18,
        );
        let result = arena.eval(div_expr, &chunk).expect("decimal div");
        let result_arr = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(result_arr.is_null(0));
    }

    #[test]
    fn test_decimal_div_non_overflow_keeps_value() {
        let mut arena = ExprArena::default();
        let lhs = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal128(38, 18),
        );
        let rhs = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::Decimal128(38, 18),
        );
        let div_expr = arena.push_typed(
            ExprNode::Div(lhs, rhs, DecimalOverflowPolicy::OutputNull),
            DataType::Decimal128(38, 18),
        );

        let chunk = create_test_chunk_two_decimals(
            vec![Some(1_200_000_000_000_000_000_i128)],
            vec![Some(2_000_000_000_000_000_000_i128)],
            38,
            18,
        );
        let result = arena.eval(div_expr, &chunk).expect("decimal div");
        let result_arr = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert!(!result_arr.is_null(0));
    }
}

#[cfg(test)]
mod overflow_policy_prepared_tests {
    use super::*;
    use crate::exec::expr::ExprNode;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn chunk(columns: Vec<ArrayRef>) -> Chunk {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(i, array)| Field::new(format!("c{i}"), array.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let ids = (1..=batch.num_columns())
            .map(|i| SlotId::new(i as u32))
            .collect::<Vec<_>>();
        let schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &ids,
        )
        .unwrap();
        Chunk::new_with_chunk_schema(batch, schema)
    }

    #[test]
    fn prepared_decimal256_policy_uses_wide_coefficients_and_precision76_boundary() {
        use DecimalOverflowPolicy::{OutputNull, ReportError};
        // Independent decimal coefficients: wide exceeds i128, and max is the
        // largest lawful DECIMAL(76,0) coefficient. No kernel pow10 is an oracle.
        let wide = i256::from_string("200000000000000000000000000000000000000").unwrap();
        let max = i256::from_string(
            "9999999999999999999999999999999999999999999999999999999999999999999999999999",
        )
        .unwrap();
        assert!(wide.to_i128().is_none());
        for (operation, right_value, right_scale, output_scale, expected) in [
            (
                DecimalOp::Add,
                1,
                0,
                0,
                "200000000000000000000000000000000000001",
            ),
            (
                DecimalOp::Sub,
                1,
                0,
                0,
                "199999999999999999999999999999999999999",
            ),
            (
                DecimalOp::Mul,
                2,
                0,
                0,
                "400000000000000000000000000000000000000",
            ),
            (
                DecimalOp::Div,
                1,
                1,
                6,
                "2000000000000000000000000000000000000000000000",
            ),
            (DecimalOp::Mod, 1, 1, 1, "0"),
        ] {
            let batch = chunk(vec![
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(wide),
                        None,
                        Some(if matches!(operation, DecimalOp::Sub) {
                            -max
                        } else {
                            max
                        }),
                    ])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    Decimal256Array::from(vec![Some(i256::from_i128(right_value)); 3])
                        .with_precision_and_scale(2, right_scale)
                        .unwrap(),
                ) as ArrayRef,
            ]);
            let mut arena = ExprArena::default();
            let left = arena.push_typed(
                ExprNode::SlotId(SlotId::new(1)),
                DataType::Decimal256(76, 0),
            );
            let right = arena.push_typed(
                ExprNode::SlotId(SlotId::new(2)),
                DataType::Decimal256(2, right_scale),
            );
            let node = |policy| match operation {
                DecimalOp::Add => ExprNode::Add(left, right, policy),
                DecimalOp::Sub => ExprNode::Sub(left, right, policy),
                DecimalOp::Mul => ExprNode::Mul(left, right, policy),
                DecimalOp::Div => ExprNode::Div(left, right, policy),
                DecimalOp::Mod => ExprNode::Mod(left, right, policy),
            };
            let output = DataType::Decimal256(76, output_scale);
            let nullable = arena.push_typed(node(OutputNull), output.clone());
            let throwing = arena.push_typed(node(ReportError), output.clone());
            let result = arena.eval(nullable, &batch).unwrap();
            assert_eq!(result.data_type(), &output);
            let result = result.as_any().downcast_ref::<Decimal256Array>().unwrap();
            assert_eq!(result.value(0).to_string(), expected);
            assert!(result.is_null(1), "input NULL remains NULL");
            assert!(
                result.is_null(2),
                "numeric overflow is NULL under OutputNull"
            );
            assert_eq!(
                arena.eval(throwing, &batch).unwrap_err(),
                decimal_overflow_error(operation)
            );
            let again = arena.eval(nullable, &batch).unwrap();
            let again = again.as_any().downcast_ref::<Decimal256Array>().unwrap();
            assert_eq!(again.value(0).to_string(), expected);
            assert_eq!(again.null_count(), 2);
        }

        // max * 2 is representable by i256 but exceeds declared precision;
        // max * 6 exceeds i256 itself. Both are checked numeric overflow.
        let batch = chunk(vec![
            Arc::new(
                Decimal256Array::from(vec![Some(max), None])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal256Array::from(vec![Some(i256::from_i128(6)); 2])
                    .with_precision_and_scale(2, 0)
                    .unwrap(),
            ) as ArrayRef,
        ]);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal256(76, 0),
        );
        let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Decimal256(2, 0));
        let nullable = arena.push_typed(
            ExprNode::Mul(left, right, OutputNull),
            DataType::Decimal256(76, 0),
        );
        let throwing = arena.push_typed(
            ExprNode::Mul(left, right, ReportError),
            DataType::Decimal256(76, 0),
        );
        assert_eq!(arena.eval(nullable, &batch).unwrap().null_count(), 2);
        assert_eq!(
            arena.eval(throwing, &batch).unwrap_err(),
            decimal_overflow_error(DecimalOp::Mul)
        );

        // Maximum finite input divided/modulo zero is NULL, never overflow;
        // either input's NULL also remains NULL under the reporting policy.
        let batch = chunk(vec![
            Arc::new(
                Decimal256Array::from(vec![None, Some(max), Some(i256::ZERO), Some(max)])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
            ) as ArrayRef,
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::ZERO),
                    Some(i256::ZERO),
                    Some(i256::ZERO),
                    None,
                ])
                .with_precision_and_scale(2, 0)
                .unwrap(),
            ) as ArrayRef,
        ]);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal256(76, 0),
        );
        let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Decimal256(2, 0));
        for policy in [OutputNull, ReportError] {
            for (node, scale) in [
                (ExprNode::Div(left, right, policy), 6),
                (ExprNode::Mod(left, right, policy), 0),
            ] {
                let result = arena.push_typed(node, DataType::Decimal256(76, scale));
                assert_eq!(arena.eval(result, &batch).unwrap().null_count(), 4);
            }
        }
    }

    #[test]
    fn prepared_arithmetic_nodes_keep_independent_policies_for_every_decimal_operation() {
        use DecimalOverflowPolicy::{OutputNull, ReportError};
        let max = 10_i128.pow(38) - 1;
        for (operation, overflow_left, right_value, right_scale, output_scale, first) in [
            (DecimalOp::Add, max, 1, 0, 0, 2),
            (DecimalOp::Sub, -max, 1, 0, 0, 0),
            (DecimalOp::Mul, max, 2, 0, 0, 2),
            (DecimalOp::Div, max, 1, 1, 6, 10_000_000),
            (DecimalOp::Mod, max, 1, 1, 1, 0),
        ] {
            let left = Arc::new(
                Decimal128Array::from(vec![Some(1), None, Some(overflow_left)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ) as ArrayRef;
            let right = Arc::new(
                Decimal128Array::from(vec![Some(right_value); 3])
                    .with_precision_and_scale(2, right_scale)
                    .unwrap(),
            ) as ArrayRef;
            let batch = chunk(vec![left, right]);
            let mut arena = ExprArena::default();
            let left = arena.push_typed(
                ExprNode::SlotId(SlotId::new(1)),
                DataType::Decimal128(38, 0),
            );
            let right = arena.push_typed(
                ExprNode::SlotId(SlotId::new(2)),
                DataType::Decimal128(2, right_scale),
            );
            let node = |policy| match operation {
                DecimalOp::Add => ExprNode::Add(left, right, policy),
                DecimalOp::Sub => ExprNode::Sub(left, right, policy),
                DecimalOp::Mul => ExprNode::Mul(left, right, policy),
                DecimalOp::Div => ExprNode::Div(left, right, policy),
                DecimalOp::Mod => ExprNode::Mod(left, right, policy),
            };
            let output = DataType::Decimal128(38, output_scale);
            let nullable = arena.push_typed(node(OutputNull), output.clone());
            let throwing = arena.push_typed(node(ReportError), output);
            let result = arena.eval(nullable, &batch).unwrap();
            let result = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(result.value(0), first);
            assert!(result.is_null(1));
            assert!(result.is_null(2));
            assert_eq!(
                arena.eval(throwing, &batch).unwrap_err(),
                decimal_overflow_error(operation)
            );
            // A failed sibling publishes no partial result and cannot mutate the NULL policy.
            assert_eq!(arena.eval(nullable, &batch).unwrap().null_count(), 2);
        }
    }

    #[test]
    fn prepared_null_and_zero_are_not_decimal_overflow_and_allow_throw_is_independent() {
        use DecimalOverflowPolicy::{OutputNull, ReportError};
        let batch = chunk(vec![
            Arc::new(
                Decimal128Array::from(vec![Some(1), None])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(0), Some(1)])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ]);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(
            ExprNode::SlotId(SlotId::new(1)),
            DataType::Decimal128(38, 0),
        );
        let right = arena.push_typed(
            ExprNode::SlotId(SlotId::new(2)),
            DataType::Decimal128(38, 0),
        );
        for node in [
            ExprNode::Div(left, right, ReportError),
            ExprNode::Mod(left, right, ReportError),
        ] {
            let id = arena.push_typed(node, DataType::Decimal128(38, 6));
            assert_eq!(arena.eval(id, &batch).unwrap().null_count(), 2);
        }
        let overflow = chunk(vec![
            Arc::new(
                Decimal128Array::from(vec![10_i128.pow(38) - 1])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![2])
                    .with_precision_and_scale(38, 0)
                    .unwrap(),
            ),
        ]);
        arena.set_allow_throw_exception(true);
        let add = arena.push_typed(
            ExprNode::Add(left, right, OutputNull),
            DataType::Decimal128(38, 0),
        );
        let multiply = arena.push_typed(
            ExprNode::Mul(left, right, OutputNull),
            DataType::Decimal128(38, 0),
        );
        assert_eq!(arena.eval(add, &overflow).unwrap().null_count(), 1);
        assert!(
            arena
                .eval(multiply, &overflow)
                .unwrap_err()
                .contains("'mul'")
        );
    }
}
