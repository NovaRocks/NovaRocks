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
use arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, ListArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use std::sync::Arc;

pub fn eval_array_difference(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let list = arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        format!(
            "array_difference expects ListArray, got {:?}",
            arr.data_type()
        )
    })?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_difference output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();

    let mut values = list.values().clone();
    if values.data_type() != &target_item_type {
        values = cast(&values, &target_item_type).map_err(|e| {
            format!(
                "array_difference failed to cast element type {:?} -> {:?}: {}",
                values.data_type(),
                target_item_type,
                e
            )
        })?;
    }
    let offsets = list.value_offsets();

    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    let out_values: ArrayRef = match target_item_type {
        DataType::Int64 => {
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    "array_difference failed to downcast values to Int64Array".to_string()
                })?;
            let mut out = Vec::<Option<i64>>::new();
            for row in 0..chunk.len() {
                let row_idx = super::common::row_index(row, list.len());
                if list.is_null(row_idx) {
                    out_offsets.push(current as i32);
                    null_builder.append_null();
                    continue;
                }
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                for idx in start..end {
                    if idx == start {
                        if values.is_null(idx) {
                            out.push(None);
                        } else {
                            out.push(Some(0));
                        }
                    } else if values.is_null(idx) || values.is_null(idx - 1) {
                        out.push(None);
                    } else {
                        out.push(Some(values.value(idx) - values.value(idx - 1)));
                    }
                    current += 1;
                }
                if current > i32::MAX as i64 {
                    return Err("array_difference offset overflow".to_string());
                }
                out_offsets.push(current as i32);
                null_builder.append_non_null();
            }
            Arc::new(Int64Array::from(out))
        }
        DataType::Float64 => {
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    "array_difference failed to downcast values to Float64Array".to_string()
                })?;
            let mut out = Vec::<Option<f64>>::new();
            for row in 0..chunk.len() {
                let row_idx = super::common::row_index(row, list.len());
                if list.is_null(row_idx) {
                    out_offsets.push(current as i32);
                    null_builder.append_null();
                    continue;
                }
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                for idx in start..end {
                    if idx == start {
                        if values.is_null(idx) {
                            out.push(None);
                        } else {
                            out.push(Some(0.0));
                        }
                    } else if values.is_null(idx) || values.is_null(idx - 1) {
                        out.push(None);
                    } else {
                        out.push(Some(values.value(idx) - values.value(idx - 1)));
                    }
                    current += 1;
                }
                if current > i32::MAX as i64 {
                    return Err("array_difference offset overflow".to_string());
                }
                out_offsets.push(current as i32);
                null_builder.append_non_null();
            }
            Arc::new(Float64Array::from(out))
        }
        DataType::Decimal128(precision, scale) => {
            let values = values
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    "array_difference failed to downcast values to Decimal128Array".to_string()
                })?;
            let mut out = Vec::<Option<i128>>::new();
            for row in 0..chunk.len() {
                let row_idx = super::common::row_index(row, list.len());
                if list.is_null(row_idx) {
                    out_offsets.push(current as i32);
                    null_builder.append_null();
                    continue;
                }
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                for idx in start..end {
                    if idx == start {
                        if values.is_null(idx) {
                            out.push(None);
                        } else {
                            out.push(Some(0));
                        }
                    } else if values.is_null(idx) || values.is_null(idx - 1) {
                        out.push(None);
                    } else {
                        out.push(Some(values.value(idx) - values.value(idx - 1)));
                    }
                    current += 1;
                }
                if current > i32::MAX as i64 {
                    return Err("array_difference offset overflow".to_string());
                }
                out_offsets.push(current as i32);
                null_builder.append_non_null();
            }
            let out = Decimal128Array::from(out)
                .with_precision_and_scale(precision, scale)
                .map_err(|e| format!("array_difference failed to build decimal output: {}", e))?;
            Arc::new(out)
        }
        other => {
            return Err(format!(
                "array_difference unsupported output element type: {:?}",
                other
            ));
        }
    };

    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(out_offsets.into()),
        out_values,
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}
