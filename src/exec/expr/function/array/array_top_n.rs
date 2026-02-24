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
    Array, ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, make_array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::cmp::Ordering;
use std::sync::Arc;

fn insertion_sort_indices_desc(values: &ArrayRef, indices: &mut [usize]) -> Result<(), String> {
    for i in 1..indices.len() {
        let mut j = i;
        while j > 0 {
            let prev = indices[j - 1];
            let curr = indices[j];
            let ord = super::common::compare_values_ordered(values, prev, curr)?;
            if ord == Ordering::Less {
                indices.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

fn top_n_value_at(array: &ArrayRef, row: usize) -> Result<Option<i64>, String> {
    let idx = super::common::row_index(row, array.len());
    match array.data_type() {
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "array_top_n failed to downcast n to Int8Array".to_string())?;
            Ok((!arr.is_null(idx)).then_some(i64::from(arr.value(idx))))
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "array_top_n failed to downcast n to Int16Array".to_string())?;
            Ok((!arr.is_null(idx)).then_some(i64::from(arr.value(idx))))
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "array_top_n failed to downcast n to Int32Array".to_string())?;
            Ok((!arr.is_null(idx)).then_some(i64::from(arr.value(idx))))
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "array_top_n failed to downcast n to Int64Array".to_string())?;
            Ok((!arr.is_null(idx)).then_some(arr.value(idx)))
        }
        other => Err(format!("array_top_n expects BIGINT n, got {:?}", other)),
    }
}

pub fn eval_array_top_n(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let n_arr = arena.eval(args[1], chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_top_n expects ListArray, got {:?}", arr.data_type()))?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_top_n output type must be List, got {:?}",
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
                "array_top_n failed to cast element type {:?} -> {:?}: {}",
                values.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let values_data = values.to_data();
    let mut mutable = MutableArrayData::new(vec![&values_data], false, 0);
    let offsets = list.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let Some(n) = top_n_value_at(&n_arr, row)? else {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        };
        if n <= 0 {
            out_offsets.push(current as i32);
            null_builder.append_non_null();
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut null_indices = Vec::<usize>::new();
        let mut non_null_indices = Vec::<usize>::new();
        for idx in start..end {
            if values.is_null(idx) {
                null_indices.push(idx);
            } else {
                non_null_indices.push(idx);
            }
        }
        insertion_sort_indices_desc(&values, &mut non_null_indices)?;

        let mut picked = 0_i64;
        for idx in non_null_indices.into_iter().chain(null_indices.into_iter()) {
            if picked >= n {
                break;
            }
            mutable.extend(0, idx, idx + 1);
            current += 1;
            picked += 1;
        }

        if current > i32::MAX as i64 {
            return Err("array_top_n offset overflow".to_string());
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
