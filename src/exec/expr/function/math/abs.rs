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
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::i256;
use std::sync::Arc;

fn eval_abs_largeint_output(value_arr: ArrayRef) -> Result<ArrayRef, String> {
    match value_arr.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let casted = if value_arr.data_type() == &DataType::Int64 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int64)
                    .map_err(|e| format!("abs: failed to cast input to Int64: {}", e))?
            };
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "abs: failed to downcast to Int64Array".to_string())?;

            let mut values = Vec::with_capacity(int_arr.len());
            for i in 0..int_arr.len() {
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    values.push(Some((int_arr.value(i) as i128).wrapping_abs()));
                }
            }
            largeint::array_from_i128(&values)
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let largeint_arr = largeint::as_fixed_size_binary_array(&value_arr, "abs")?;
            let mut values = Vec::with_capacity(largeint_arr.len());
            for i in 0..largeint_arr.len() {
                if largeint_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = largeint::value_at(largeint_arr, i)?;
                    // Keep two's-complement overflow behavior aligned with StarRocks `abs_largeint`.
                    values.push(Some(v.wrapping_abs()));
                }
            }
            largeint::array_from_i128(&values)
        }
        DataType::Null => {
            let values = vec![None; value_arr.len()];
            largeint::array_from_i128(&values)
        }
        other => Err(format!(
            "abs: unsupported input type {:?} for LARGEINT output",
            other
        )),
    }
}

/// Evaluate abs function.
/// Supports:
/// - abs(int): returns int (absolute value with planned output type)
/// - abs(float): returns float (absolute value with planned output type)
/// - abs(decimal): returns decimal (absolute value)
///
/// Implementation aligns with StarRocks BE:
/// - Execute according to FE-declared output type.
/// - Surface overflow explicitly when planned output type cannot represent ABS result.
pub fn eval_abs(
    arena: &ExprArena,
    expr: ExprId,
    value_expr: ExprId,
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let value_arr = arena.eval(value_expr, chunk)?;
    let output_type = arena
        .data_type(expr)
        .ok_or_else(|| "abs: missing output type".to_string())?;

    // Execute ABS according to FE-declared output type. This avoids type guessing and keeps
    // overflow behavior aligned with planned return type (for example BIGINT -> LARGEINT).
    match output_type {
        t if largeint::is_largeint_data_type(t) => eval_abs_largeint_output(value_arr),
        DataType::Int8 => {
            let casted = if value_arr.data_type() == &DataType::Int8 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int8)
                    .map_err(|e| format!("abs: failed to cast input to Int8: {}", e))?
            };
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "abs: failed to downcast to Int8Array".to_string())?;
            let mut values = Vec::with_capacity(int_arr.len());
            for i in 0..int_arr.len() {
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        "abs overflow on Int8 minimum; FE should promote result type".to_string()
                    })?;
                    values.push(Some(v_abs));
                }
            }
            Ok(Arc::new(Int8Array::from(values)) as ArrayRef)
        }
        DataType::Int16 => {
            let casted = if value_arr.data_type() == &DataType::Int16 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int16)
                    .map_err(|e| format!("abs: failed to cast input to Int16: {}", e))?
            };
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "abs: failed to downcast to Int16Array".to_string())?;
            let mut values = Vec::with_capacity(int_arr.len());
            for i in 0..int_arr.len() {
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        "abs overflow on Int16 minimum; FE should promote result type".to_string()
                    })?;
                    values.push(Some(v_abs));
                }
            }
            Ok(Arc::new(Int16Array::from(values)) as ArrayRef)
        }
        DataType::Int32 => {
            let casted = if value_arr.data_type() == &DataType::Int32 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int32)
                    .map_err(|e| format!("abs: failed to cast input to Int32: {}", e))?
            };
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "abs: failed to downcast to Int32Array".to_string())?;
            let mut values = Vec::with_capacity(int_arr.len());
            for i in 0..int_arr.len() {
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v_abs = int_arr.value(i).checked_abs().ok_or_else(|| {
                        "abs overflow on Int32 minimum; FE should promote result type".to_string()
                    })?;
                    values.push(Some(v_abs));
                }
            }
            Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
        }
        DataType::Int64 => {
            let casted = if value_arr.data_type() == &DataType::Int64 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Int64)
                    .map_err(|e| format!("abs: failed to cast input to Int64: {}", e))?
            };
            let int_arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "abs: failed to downcast to Int64Array".to_string())?;

            let len = int_arr.len();
            let mut values = Vec::with_capacity(len);
            for i in 0..len {
                if int_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = int_arr.value(i);
                    let v_abs = v.checked_abs().ok_or_else(|| {
                        "abs overflow on Int64 minimum; FE should promote result type".to_string()
                    })?;
                    values.push(Some(v_abs));
                }
            }
            Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
        }
        DataType::Float32 => {
            let casted = if value_arr.data_type() == &DataType::Float32 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Float32)
                    .map_err(|e| format!("abs: failed to cast input to Float32: {}", e))?
            };
            let float_arr = casted
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "abs: failed to downcast to Float32Array".to_string())?;
            let mut values = Vec::with_capacity(float_arr.len());
            for i in 0..float_arr.len() {
                if float_arr.is_null(i) {
                    values.push(None);
                } else {
                    values.push(Some(float_arr.value(i).abs()));
                }
            }
            Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
        }
        DataType::Float64 => {
            let casted = if value_arr.data_type() == &DataType::Float64 {
                value_arr
            } else {
                cast(&value_arr, &DataType::Float64)
                    .map_err(|e| format!("abs: failed to cast input to Float64: {}", e))?
            };
            let float_arr = casted
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "abs: failed to downcast to Float64Array".to_string())?;

            let len = float_arr.len();
            let mut values = Vec::with_capacity(len);
            for i in 0..len {
                if float_arr.is_null(i) {
                    values.push(None);
                } else {
                    values.push(Some(float_arr.value(i).abs()));
                }
            }
            Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
        }
        DataType::Decimal128(out_precision, out_scale) => {
            let casted = if value_arr.data_type() == output_type {
                value_arr
            } else {
                cast(&value_arr, output_type).map_err(|e| {
                    format!(
                        "abs: failed to cast input from {:?} to {:?}: {}",
                        value_arr.data_type(),
                        output_type,
                        e
                    )
                })?
            };
            let dec_arr = casted
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "abs: failed to downcast to Decimal128Array".to_string())?;

            let len = dec_arr.len();
            let mut values = Vec::with_capacity(len);
            for i in 0..len {
                if dec_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = dec_arr.value(i);
                    let v_abs = v
                        .checked_abs()
                        .ok_or_else(|| "abs overflow on Decimal128 minimum".to_string())?;
                    values.push(Some(v_abs));
                }
            }
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| format!("abs: failed to create Decimal128Array: {}", e))?;
            Ok(Arc::new(array) as ArrayRef)
        }
        DataType::Decimal256(out_precision, out_scale) => {
            let casted = if value_arr.data_type() == output_type {
                value_arr
            } else {
                cast(&value_arr, output_type).map_err(|e| {
                    format!(
                        "abs: failed to cast input from {:?} to {:?}: {}",
                        value_arr.data_type(),
                        output_type,
                        e
                    )
                })?
            };
            let dec_arr = casted
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| "abs: failed to downcast to Decimal256Array".to_string())?;

            let len = dec_arr.len();
            let mut values: Vec<Option<i256>> = Vec::with_capacity(len);
            for i in 0..len {
                if dec_arr.is_null(i) {
                    values.push(None);
                } else {
                    let v = dec_arr.value(i);
                    let v_abs = if v.is_negative() {
                        v.checked_neg()
                            .ok_or_else(|| "abs overflow on Decimal256 minimum".to_string())?
                    } else {
                        v
                    };
                    values.push(Some(v_abs));
                }
            }
            let array = Decimal256Array::from(values)
                .with_precision_and_scale(*out_precision, *out_scale)
                .map_err(|e| format!("abs: failed to create Decimal256Array: {}", e))?;
            Ok(Arc::new(array) as ArrayRef)
        }
        other => Err(format!(
            "abs: unsupported output type from FE plan: {:?}",
            other
        )),
    }
}

