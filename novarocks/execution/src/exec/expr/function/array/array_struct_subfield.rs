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
use arrow::array::{Array, ArrayRef, ListArray, StringArray, StructArray, UInt32Array};
use arrow::compute::{cast, take};
use std::sync::Arc;

pub fn eval_array_struct_subfield(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let input = arena.eval(args[0], chunk)?;
    let field_name_arr = arena.eval(args[1], chunk)?;
    let list = input.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        format!(
            "__array_struct_subfield expects ListArray, got {:?}",
            input.data_type()
        )
    })?;
    let struct_values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            format!(
                "__array_struct_subfield expects list values to be StructArray, got {:?}",
                list.values().data_type()
            )
        })?;
    let field_name = parse_constant_field_name(field_name_arr.as_ref())?;
    let field_idx = struct_values
        .fields()
        .iter()
        .position(|field| field.name() == field_name.as_str())
        .ok_or_else(|| {
            format!(
                "__array_struct_subfield field '{}' does not exist",
                field_name
            )
        })?;
    let field_col = struct_values.column(field_idx);

    let mut indices = Vec::with_capacity(struct_values.len());
    for row in 0..struct_values.len() {
        if struct_values.is_null(row) {
            indices.push(None);
        } else {
            let idx = u32::try_from(row)
                .map_err(|_| "__array_struct_subfield index exceeds UInt32 range".to_string())?;
            indices.push(Some(idx));
        }
    }

    let indices = UInt32Array::from(indices);
    let mut out_values = take(field_col.as_ref(), &indices, None).map_err(|e| e.to_string())?;
    let output_field = match arena.data_type(expr) {
        Some(arrow::datatypes::DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            arrow::datatypes::DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "__array_struct_subfield output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();
    if out_values.data_type() != &target_item_type {
        out_values = cast(&out_values, &target_item_type).map_err(|e| {
            format!(
                "__array_struct_subfield: failed to cast output {:?} -> {:?}: {}",
                out_values.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let out = ListArray::new(
        output_field,
        list.offsets().clone(),
        out_values,
        list.nulls().cloned(),
    );
    Ok(Arc::new(out) as ArrayRef)
}

fn parse_constant_field_name(field_name_arr: &dyn Array) -> Result<String, String> {
    let arr = field_name_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "__array_struct_subfield field-name argument must be VARCHAR".to_string())?;
    if arr.is_empty() {
        return Err("__array_struct_subfield field-name argument is empty".to_string());
    }
    let first = if arr.is_null(0) {
        return Err("__array_struct_subfield field-name argument must be non-null".to_string());
    } else {
        arr.value(0)
    };
    for i in 1..arr.len() {
        if arr.is_null(i) || arr.value(i) != first {
            return Err("__array_struct_subfield field-name argument must be constant".to_string());
        }
    }
    Ok(first.to_string())
}
