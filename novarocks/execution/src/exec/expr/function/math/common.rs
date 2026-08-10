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
use arrow::array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array,
};
use arrow::datatypes::DataType;

#[derive(Clone, Debug)]
pub enum NumericArrayView<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Int8(&'a Int8Array),
    Float64(&'a Float64Array),
    Float32(&'a Float32Array),
    Decimal128(&'a Decimal128Array, i8),
    Null(usize),
}

impl<'a> NumericArrayView<'a> {
    pub fn new(array: &'a ArrayRef) -> Result<Self, String> {
        match array.data_type() {
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or_else(|| "failed to downcast to Int64Array".to_string()),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or_else(|| "failed to downcast to Int32Array".to_string()),
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or_else(|| "failed to downcast to Int16Array".to_string()),
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or_else(|| "failed to downcast to Int8Array".to_string()),
            DataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(Self::Float64)
                .ok_or_else(|| "failed to downcast to Float64Array".to_string()),
            DataType::Float32 => array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(Self::Float32)
                .ok_or_else(|| "failed to downcast to Float32Array".to_string()),
            DataType::Decimal128(_, scale) => array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .map(|arr| Self::Decimal128(arr, *scale))
                .ok_or_else(|| "failed to downcast to Decimal128Array".to_string()),
            DataType::Null => Ok(Self::Null(array.len())),
            other => Err(format!("unsupported numeric type: {:?}", other)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            NumericArrayView::Int64(arr) => arr.len(),
            NumericArrayView::Int32(arr) => arr.len(),
            NumericArrayView::Int16(arr) => arr.len(),
            NumericArrayView::Int8(arr) => arr.len(),
            NumericArrayView::Float64(arr) => arr.len(),
            NumericArrayView::Float32(arr) => arr.len(),
            NumericArrayView::Decimal128(arr, _) => arr.len(),
            NumericArrayView::Null(len) => *len,
        }
    }

    pub fn value_f64(&self, row: usize) -> Option<f64> {
        match self {
            NumericArrayView::Int64(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Int32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Int16(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Int8(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Float64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            NumericArrayView::Float32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
            NumericArrayView::Decimal128(arr, scale) => (!arr.is_null(row)).then(|| {
                let v = arr.value(row) as f64;
                let divisor = 10_f64.powi(*scale as i32);
                v / divisor
            }),
            NumericArrayView::Null(_) => None,
        }
    }

    pub fn value_i64(&self, row: usize) -> Option<i64> {
        match self {
            NumericArrayView::Int64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            NumericArrayView::Int32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            NumericArrayView::Int16(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            NumericArrayView::Int8(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            NumericArrayView::Float64(arr) => (!arr.is_null(row))
                .then(|| arr.value(row))
                .filter(|v| v.is_finite())
                .map(|v| v as i64),
            NumericArrayView::Float32(arr) => (!arr.is_null(row))
                .then(|| arr.value(row) as f64)
                .filter(|v| v.is_finite())
                .map(|v| v as i64),
            NumericArrayView::Decimal128(arr, scale) => (!arr.is_null(row)).then(|| {
                let v = arr.value(row) as f64;
                let divisor = 10_f64.powi(*scale as i32);
                (v / divisor) as i64
            }),
            NumericArrayView::Null(_) => None,
        }
    }
}

pub fn value_at_f64(view: &NumericArrayView<'_>, row: usize, len: usize) -> Option<f64> {
    if view.len() == 1 && len > 1 {
        view.value_f64(0)
    } else {
        view.value_f64(row)
    }
}

pub fn value_at_i64(view: &NumericArrayView<'_>, row: usize, len: usize) -> Option<i64> {
    if view.len() == 1 && len > 1 {
        view.value_i64(0)
    } else {
        view.value_i64(row)
    }
}

use arrow::array::{Float32Builder, Float64Builder};
use arrow::compute::cast;
use std::sync::Arc;

fn sanitize_non_finite_float_output(out: ArrayRef) -> ArrayRef {
    match out.data_type() {
        DataType::Float64 => {
            let Some(arr) = out.as_any().downcast_ref::<Float64Array>() else {
                return out;
            };
            let mut builder = Float64Builder::new();
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = arr.value(row);
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        DataType::Float32 => {
            let Some(arr) = out.as_any().downcast_ref::<Float32Array>() else {
                return out;
            };
            let mut builder = Float32Builder::new();
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let value = arr.value(row);
                if value.is_finite() {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        _ => out,
    }
}

pub(super) fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
) -> Result<ArrayRef, String> {
    let out = sanitize_non_finite_float_output(out);
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    let casted = cast(&out, target).map_err(|e| format!("math: failed to cast output: {}", e))?;
    Ok(sanitize_non_finite_float_output(casted))
}
