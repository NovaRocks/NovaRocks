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
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_array_remove(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let target_arr = arena.eval(args[1], chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_remove expects ListArray, got {:?}", arr.data_type()))?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_remove output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = super::common::adjust_legacy_decimalv2_target_type(
        list.values().data_type(),
        output_field.data_type(),
    );
    let output_field = if output_field.data_type() == &target_item_type {
        output_field
    } else {
        Arc::new(arrow::datatypes::Field::new(
            output_field.name(),
            target_item_type.clone(),
            output_field.is_nullable(),
        ))
    };

    let mut values = list.values().clone();
    if values.data_type() != &target_item_type {
        values =
            super::common::cast_with_special_rules(&values, &target_item_type, "array_remove")?;
    }

    let mut targets = target_arr.clone();
    if targets.data_type() != &target_item_type {
        targets =
            super::common::cast_with_special_rules(&targets, &target_item_type, "array_remove")?;
    }

    let values_data = values.to_data();
    let mut mutable = MutableArrayData::new(vec![&values_data], false, 0);

    let offsets = list.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let list_row = super::common::row_index(row, list.len());
        if list.is_null(list_row) {
            null_builder.append_null();
            out_offsets.push(current as i32);
            continue;
        }

        let target_row = super::common::row_index(row, targets.len());
        let target_is_null = targets.is_null(target_row);
        let start = offsets[list_row] as usize;
        let end = offsets[list_row + 1] as usize;

        for idx in start..end {
            let should_remove = if target_is_null {
                values.is_null(idx)
            } else {
                super::common::compare_values_with_null(&values, idx, &targets, target_row, false)?
            };
            if !should_remove {
                mutable.extend(0, idx, idx + 1);
                current += 1;
            }
        }
        if current > i32::MAX as i64 {
            return Err("array_remove offset overflow".to_string());
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
