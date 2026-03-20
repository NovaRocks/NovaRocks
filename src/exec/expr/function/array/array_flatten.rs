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
use arrow::array::{Array, ArrayRef, ListArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_array_flatten(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let outer = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_flatten expects ListArray, got {:?}", arr.data_type()))?;

    let inner = outer
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "array_flatten expects ARRAY<ARRAY<...>>, got {:?}",
                outer.data_type()
            )
        })?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match inner.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_flatten output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();

    let mut inner_values = inner.values().clone();
    if inner_values.data_type() != &target_item_type {
        inner_values = cast(&inner_values, &target_item_type).map_err(|e| {
            format!(
                "array_flatten failed to cast inner element {:?} -> {:?}: {}",
                inner_values.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let inner_values_data = inner_values.to_data();
    let mut mutable = MutableArrayData::new(vec![&inner_values_data], false, 0);

    let outer_offsets = outer.value_offsets();
    let inner_offsets = inner.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let outer_row = super::common::row_index(row, outer.len());
        if outer.is_null(outer_row) {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let outer_start = outer_offsets[outer_row] as usize;
        let outer_end = outer_offsets[outer_row + 1] as usize;
        for inner_row in outer_start..outer_end {
            if inner.is_null(inner_row) {
                continue;
            }
            let start = inner_offsets[inner_row] as usize;
            let end = inner_offsets[inner_row + 1] as usize;
            if end > start {
                mutable.extend(0, start, end);
                current += (end - start) as i64;
            }
        }

        if current > i32::MAX as i64 {
            return Err("array_flatten offset overflow".to_string());
        }
        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let out_values = make_array(mutable.freeze());
    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(out_offsets.into()),
        out_values,
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}
