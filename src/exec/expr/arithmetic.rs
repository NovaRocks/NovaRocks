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
use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::decimal::{div_round_i128, div_round_i256, pow10_i128, pow10_i256};
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int64Array};
use arrow::compute::kernels::numeric::{add, div, mul, rem, sub};
use arrow::datatypes::DataType;
use arrow_buffer::i256;
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

enum DecimalOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

fn decimal_value_fits_precision(unscaled: i128, precision: u8) -> bool {
    unscaled.unsigned_abs().to_string().len() <= precision as usize
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
        DataType::Null => Ok((vec![None; arr.len()], 0)),
        other => Err(format!(
            "{context}: unsupported Decimal256 operand type: {:?}",
            other
        )),
    }
}

fn eval_decimal_div_value(
    lhs_val: i128,
    rhs_val: i128,
    lhs_scale_i32: i32,
    rhs_scale_i32: i32,
    out_scale_i32: i32,
) -> Result<Option<i128>, String> {
    if rhs_val == 0 {
        return Ok(None);
    }
    let exponent = out_scale_i32 + rhs_scale_i32 - lhs_scale_i32;
    let numerator = if exponent >= 0 {
        let factor = match pow10_i128(exponent as usize) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        match lhs_val.checked_mul(factor) {
            Some(v) => v,
            None => return Ok(None),
        }
    } else {
        let factor = match pow10_i128((-exponent) as usize) {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        lhs_val / factor
    };
    Ok(Some(div_round_i128(numerator, rhs_val)))
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

fn eval_decimal_binop(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    output_type: &DataType,
    op: DecimalOp,
    strict_overflow: bool,
) -> Result<Option<ArrayRef>, String> {
    match output_type {
        DataType::Decimal128(out_precision, out_scale) => {
            let (_lhs_precision, lhs_scale) = match lhs.data_type() {
                DataType::Decimal128(p, s) => (*p, *s),
                _ => return Ok(None),
            };
            let (_rhs_precision, rhs_scale) = match rhs.data_type() {
                DataType::Decimal128(p, s) => (*p, *s),
                _ => return Ok(None),
            };
            let lhs_arr = lhs
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            let rhs_arr = rhs
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string())?;
            if lhs_arr.len() != rhs_arr.len() {
                return Err("decimal arithmetic length mismatch".to_string());
            }
            let len = lhs_arr.len();
            let mut values: Vec<Option<i128>> = Vec::with_capacity(len);
            let mut had_mul_overflow = false;
            let lhs_scale_i32 = lhs_scale as i32;
            let rhs_scale_i32 = rhs_scale as i32;
            let out_scale_i32 = *out_scale as i32;
            for row in 0..len {
                if lhs_arr.is_null(row) || rhs_arr.is_null(row) {
                    values.push(None);
                    continue;
                }
                let lhs_val = lhs_arr.value(row);
                let rhs_val = rhs_arr.value(row);
                let out_val = match op {
                    DecimalOp::Add | DecimalOp::Sub => {
                        if out_scale_i32 < lhs_scale_i32 || out_scale_i32 < rhs_scale_i32 {
                            return Err("decimal add/sub scale mismatch".to_string());
                        }
                        let lhs_factor = match pow10_i128((out_scale_i32 - lhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                values.push(None);
                                continue;
                            }
                        };
                        let rhs_factor = match pow10_i128((out_scale_i32 - rhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                values.push(None);
                                continue;
                            }
                        };
                        let Some(lhs_scaled) = lhs_val.checked_mul(lhs_factor) else {
                            values.push(None);
                            continue;
                        };
                        let Some(rhs_scaled) = rhs_val.checked_mul(rhs_factor) else {
                            values.push(None);
                            continue;
                        };
                        let out = if matches!(op, DecimalOp::Add) {
                            lhs_scaled.checked_add(rhs_scaled)
                        } else {
                            lhs_scaled.checked_sub(rhs_scaled)
                        };
                        let Some(out) = out else {
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
                            values.push(None);
                            continue;
                        };
                        if diff >= 0 {
                            let factor = match pow10_i128(diff as usize) {
                                Ok(v) => v,
                                Err(_) => {
                                    had_mul_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            };
                            let Some(out) = product.checked_mul(factor) else {
                                had_mul_overflow = true;
                                values.push(None);
                                continue;
                            };
                            out
                        } else {
                            let factor = match pow10_i128((-diff) as usize) {
                                Ok(v) => v,
                                Err(_) => {
                                    values.push(None);
                                    continue;
                                }
                            };
                            product / factor
                        }
                    }
                    DecimalOp::Div => {
                        let Some(divided) = eval_decimal_div_value(
                            lhs_val,
                            rhs_val,
                            lhs_scale_i32,
                            rhs_scale_i32,
                            out_scale_i32,
                        )?
                        else {
                            values.push(None);
                            continue;
                        };
                        divided
                    }
                    DecimalOp::Mod => {
                        if rhs_val == 0 {
                            values.push(None);
                            continue;
                        }
                        if out_scale_i32 < lhs_scale_i32 || out_scale_i32 < rhs_scale_i32 {
                            return Err("decimal mod scale mismatch".to_string());
                        }
                        let lhs_factor = match pow10_i128((out_scale_i32 - lhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                values.push(None);
                                continue;
                            }
                        };
                        let rhs_factor = match pow10_i128((out_scale_i32 - rhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                values.push(None);
                                continue;
                            }
                        };
                        let Some(lhs_scaled) = lhs_val.checked_mul(lhs_factor) else {
                            values.push(None);
                            continue;
                        };
                        let Some(rhs_scaled) = rhs_val.checked_mul(rhs_factor) else {
                            values.push(None);
                            continue;
                        };
                        lhs_scaled % rhs_scaled
                    }
                };
                if !decimal_value_fits_precision(out_val, *out_precision) {
                    if matches!(op, DecimalOp::Mul) {
                        had_mul_overflow = true;
                    }
                    values.push(None);
                    continue;
                }
                values.push(Some(out_val));
            }
            if strict_overflow && matches!(op, DecimalOp::Mul) && had_mul_overflow {
                return Err(
                    "Expr evaluate meet error: The 'mul' operation involving decimal values overflows"
                        .to_string(),
                );
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
                                values.push(None);
                                continue;
                            }
                        };
                        let rhs_factor = match pow10_i256((out_scale_i32 - rhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                values.push(None);
                                continue;
                            }
                        };
                        let Some(lhs_scaled) = lhs_val.checked_mul(lhs_factor) else {
                            values.push(None);
                            continue;
                        };
                        let Some(rhs_scaled) = rhs_val.checked_mul(rhs_factor) else {
                            values.push(None);
                            continue;
                        };
                        let out = if matches!(op, DecimalOp::Add) {
                            lhs_scaled.checked_add(rhs_scaled)
                        } else {
                            lhs_scaled.checked_sub(rhs_scaled)
                        };
                        let Some(out) = out else {
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
                            values.push(None);
                            continue;
                        };
                        if diff >= 0 {
                            let factor = match pow10_i256(diff as usize) {
                                Ok(v) => v,
                                Err(_) => {
                                    had_mul_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            };
                            let Some(out) = product.checked_mul(factor) else {
                                had_mul_overflow = true;
                                values.push(None);
                                continue;
                            };
                            out
                        } else {
                            let factor = match pow10_i256((-diff) as usize) {
                                Ok(v) => v,
                                Err(_) => {
                                    had_mul_overflow = true;
                                    values.push(None);
                                    continue;
                                }
                            };
                            match product.checked_div(factor) {
                                Some(v) => v,
                                None => {
                                    had_mul_overflow = true;
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
                                values.push(None);
                                continue;
                            }
                        };
                        let rhs_factor = match pow10_i256((out_scale_i32 - rhs_scale_i32) as usize)
                        {
                            Ok(v) => v,
                            Err(_) => {
                                values.push(None);
                                continue;
                            }
                        };
                        let Some(lhs_scaled) = lhs_val.checked_mul(lhs_factor) else {
                            values.push(None);
                            continue;
                        };
                        let Some(rhs_scaled) = rhs_val.checked_mul(rhs_factor) else {
                            values.push(None);
                            continue;
                        };
                        match lhs_scaled.checked_rem(rhs_scaled) {
                            Some(v) => v,
                            None => {
                                values.push(None);
                                continue;
                            }
                        }
                    }
                };
                // StarRocks decimal256 expression evaluation is bounded by int256 range rather
                // than declared precision, so we do not nullify by precision here.
                values.push(Some(out_val));
            }
            if strict_overflow && matches!(op, DecimalOp::Mul) && had_mul_overflow {
                return Err(
                    "Expr evaluate meet error: The 'mul' operation involving decimal values overflows"
                        .to_string(),
                );
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
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let lhs = arena.eval(a, chunk)?;
    let rhs = arena.eval(b, chunk)?;
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
    )? {
        return Ok(arr);
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

pub fn eval_mod(
    arena: &ExprArena,
    expr: ExprId,
    a: ExprId,
    b: ExprId,
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
    use crate::common::ids::SlotId;
    use crate::common::largeint;
    use crate::exec::chunk::field_with_slot_id;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::{Decimal128Array, FixedSizeBinaryArray, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn create_test_chunk_int(values: Vec<i64>) -> Chunk {
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("col0", DataType::Int64, false),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
    }

    fn create_test_chunk_float(values: Vec<f64>) -> Chunk {
        let array = Arc::new(Float64Array::from(values)) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![field_with_slot_id(
            Field::new("col0", DataType::Float64, false),
            SlotId::new(1),
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Chunk::new(batch)
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
            field_with_slot_id(
                Field::new("left", DataType::Decimal128(precision, scale), true),
                SlotId::new(1),
            ),
            field_with_slot_id(
                Field::new("right", DataType::Decimal128(precision, scale), true),
                SlotId::new(2),
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left_arr, right_arr]).expect("batch");
        Chunk::new(batch)
    }

    #[test]
    fn test_add_integers() {
        let mut arena = ExprArena::default();
        let lit5 = arena.push(ExprNode::Literal(LiteralValue::Int64(5)));
        let lit3 = arena.push(ExprNode::Literal(LiteralValue::Int64(3)));
        let add = arena.push_typed(ExprNode::Add(lit5, lit3), DataType::Int64);

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
        let sub = arena.push_typed(ExprNode::Sub(lit10, lit3), DataType::Int64);

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
        let mul = arena.push_typed(ExprNode::Mul(lit6, lit7), DataType::Int64);

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
        let div = arena.push_typed(ExprNode::Div(lit20, lit4), DataType::Int64);

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
        let rem = arena.push_typed(ExprNode::Mod(lit10, lit3), DataType::Int64);

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
        let add = arena.push_typed(ExprNode::Add(lit1, lit2), DataType::Float64);

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
        let mul = arena.push_typed(ExprNode::Mul(lit_int, lit_float), DataType::Float64);

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
        let add = arena.push_typed(ExprNode::Add(lhs, rhs), DataType::FixedSizeBinary(16));

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
        let mul = arena.push_typed(ExprNode::Mul(lhs, rhs), DataType::FixedSizeBinary(16));

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
        let div_expr = arena.push_typed(ExprNode::Div(lhs, rhs), DataType::Decimal128(38, 38));

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
        let div_expr = arena.push_typed(ExprNode::Div(lhs, rhs), DataType::Decimal128(38, 18));

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
