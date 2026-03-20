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
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, ListArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::OffsetBuffer;
use std::sync::Arc;

pub fn eval_array_cum_sum(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_cum_sum expects ListArray, got {:?}", arr.data_type()))?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_cum_sum output type must be List, got {:?}",
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
                "array_cum_sum failed to cast element type {:?} -> {:?}: {}",
                values.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let offsets = list.value_offsets();
    let out_values: ArrayRef = match target_item_type {
        DataType::Int64 => {
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    "array_cum_sum failed to downcast values to Int64Array".to_string()
                })?;
            let mut out = Vec::with_capacity(values.len());
            for row in 0..chunk.len() {
                let row_idx = super::common::row_index(row, list.len());
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                let mut sum = 0_i64;
                for idx in start..end {
                    if values.is_null(idx) {
                        out.push(None);
                    } else {
                        sum += values.value(idx);
                        out.push(Some(sum));
                    }
                }
            }
            Arc::new(Int64Array::from(out))
        }
        DataType::Float64 => {
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    "array_cum_sum failed to downcast values to Float64Array".to_string()
                })?;
            let mut out = Vec::with_capacity(values.len());
            for row in 0..chunk.len() {
                let row_idx = super::common::row_index(row, list.len());
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                let mut sum = 0.0_f64;
                for idx in start..end {
                    if values.is_null(idx) {
                        out.push(None);
                    } else {
                        sum += values.value(idx);
                        out.push(Some(sum));
                    }
                }
            }
            Arc::new(Float64Array::from(out))
        }
        other => {
            return Err(format!(
                "array_cum_sum unsupported output element type: {:?}",
                other
            ));
        }
    };

    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.to_vec().into()),
        out_values,
        list.nulls().cloned(),
    );
    Ok(Arc::new(out) as ArrayRef)
}
