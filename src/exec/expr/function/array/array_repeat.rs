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
use arrow::array::{Array, ArrayRef, Int64Array, ListArray, make_array, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

fn cast_repeat_count(array: ArrayRef) -> Result<ArrayRef, String> {
    if array.data_type() == &DataType::Int64 {
        return Ok(array);
    }
    cast(&array, &DataType::Int64).map_err(|e| {
        format!(
            "array_repeat failed to cast repeat count {:?} -> BIGINT: {}",
            array.data_type(),
            e
        )
    })
}

pub fn eval_array_repeat(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let source = arena.eval(args[0], chunk)?;
    let count_array = cast_repeat_count(arena.eval(args[1], chunk)?)?;
    let counts = count_array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "array_repeat internal count downcast failure".to_string())?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => Arc::new(Field::new("item", source.data_type().clone(), true)),
    };
    let item_type = output_field.data_type().clone();
    let mut values = if source.data_type() == &DataType::Null && item_type != DataType::Null {
        new_null_array(&item_type, source.len())
    } else if source.data_type() == &item_type {
        source
    } else {
        cast(&source, &item_type).map_err(|e| {
            format!(
                "array_repeat failed to cast source type {:?} -> {:?}: {}",
                source.data_type(),
                item_type,
                e
            )
        })?
    };
    if values.is_empty() && chunk.len() > 0 {
        values = new_null_array(&item_type, chunk.len());
    }

    let values_data = values.to_data();
    let mut mutable = MutableArrayData::new(vec![&values_data], false, 0);
    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut null_builder = NullBufferBuilder::new(chunk.len());
    let mut current: i64 = 0;

    for row in 0..chunk.len() {
        let source_row = super::common::row_index(row, values.len());
        let count_row = super::common::row_index(row, counts.len());
        if counts.is_null(count_row) {
            null_builder.append_null();
            offsets.push(current as i32);
            continue;
        }

        let repeat_count = counts.value(count_row);
        if repeat_count > 0 {
            for _ in 0..repeat_count {
                mutable.extend(0, source_row, source_row + 1);
            }
            current = current
                .checked_add(repeat_count)
                .ok_or_else(|| "array_repeat offset overflow".to_string())?;
            if current > i32::MAX as i64 {
                return Err("array_repeat offset overflow".to_string());
            }
        }
        null_builder.append_non_null();
        offsets.push(current as i32);
    }

    let out = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        make_array(mutable.freeze()),
        null_builder.finish(),
    );
    Ok(Arc::new(out) as ArrayRef)
}
