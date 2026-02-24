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
    Array, ArrayRef, Float32Array, Float64Array, ListArray, StringBuilder, StructArray, make_array,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

fn format_float_for_varchar(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    format!("{value}")
}

fn cast_float_to_utf8(value_arr: &ArrayRef) -> Result<ArrayRef, String> {
    let mut out = StringBuilder::new();
    match value_arr.data_type() {
        DataType::Float64 => {
            let arr = value_arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "arrays_zip failed to downcast Float64Array".to_string())?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    out.append_null();
                } else {
                    out.append_value(format_float_for_varchar(arr.value(idx)));
                }
            }
        }
        DataType::Float32 => {
            let arr = value_arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "arrays_zip failed to downcast Float32Array".to_string())?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    out.append_null();
                } else {
                    out.append_value(format_float_for_varchar(arr.value(idx) as f64));
                }
            }
        }
        _ => {
            return Err(format!(
                "arrays_zip float->utf8 cast got non-float input: {:?}",
                value_arr.data_type()
            ));
        }
    }
    Ok(Arc::new(out.finish()) as ArrayRef)
}

pub fn eval_arrays_zip(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("arrays_zip expects at least one argument".to_string());
    }

    let mut list_arrays: Vec<ArrayRef> = Vec::with_capacity(args.len());
    let mut value_arrays: Vec<ArrayRef> = Vec::with_capacity(args.len());
    for arg in args {
        let arr = arena.eval(*arg, chunk)?;
        let list_values = arr
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| format!("arrays_zip expects ListArray, got {:?}", arr.data_type()))?
            .values()
            .clone();
        list_arrays.push(arr);
        value_arrays.push(list_values);
    }

    let (list_field, struct_fields) = resolve_output_type(arena.data_type(expr), &value_arrays)?;
    if struct_fields.len() != value_arrays.len() {
        return Err(format!(
            "arrays_zip output struct field count mismatch: expected {}, got {}",
            value_arrays.len(),
            struct_fields.len()
        ));
    }

    for (idx, value_arr) in value_arrays.iter_mut().enumerate() {
        let expected = struct_fields[idx].data_type();
        if value_arr.data_type() != expected {
            *value_arr = if matches!(expected, DataType::Utf8)
                && matches!(value_arr.data_type(), DataType::Float32 | DataType::Float64)
            {
                cast_float_to_utf8(value_arr)?
            } else {
                cast(value_arr, expected).map_err(|e| {
                    format!(
                        "arrays_zip failed to cast field {} to {:?}: {}",
                        idx, expected, e
                    )
                })?
            };
        }
    }

    let value_data: Vec<arrow_data::ArrayData> = value_arrays.iter().map(|a| a.to_data()).collect();
    let mut mutables: Vec<MutableArrayData> = value_data
        .iter()
        .map(|data| MutableArrayData::new(vec![data], true, 0))
        .collect();

    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let mut row_indices = Vec::with_capacity(list_arrays.len());
        let mut row_is_null = false;
        let mut max_len = 0usize;

        for list_arr in &list_arrays {
            let list = list_arr
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "arrays_zip internal list downcast failed".to_string())?;
            let row_idx = super::common::row_index(row, list.len());
            row_indices.push(row_idx);
            if list.is_null(row_idx) {
                row_is_null = true;
                break;
            }
            let offsets = list.value_offsets();
            let len = (offsets[row_idx + 1] - offsets[row_idx]) as usize;
            if len > max_len {
                max_len = len;
            }
        }

        if row_is_null {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        for element_idx in 0..max_len {
            for (array_idx, list_arr) in list_arrays.iter().enumerate() {
                let list = list_arr
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| "arrays_zip internal list downcast failed".to_string())?;
                let row_idx = row_indices[array_idx];
                let offsets = list.value_offsets();
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                let index = start + element_idx;
                if index < end {
                    mutables[array_idx].extend(0, index, index + 1);
                } else {
                    mutables[array_idx].extend_nulls(1);
                }
            }
            current += 1;
            if current > i32::MAX as i64 {
                return Err("arrays_zip offset overflow".to_string());
            }
        }

        out_offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let struct_columns: Vec<ArrayRef> = mutables
        .into_iter()
        .map(|m| make_array(m.freeze()))
        .collect();
    let out_struct = StructArray::new(struct_fields, struct_columns, None);
    let out = ListArray::new(
        list_field,
        OffsetBuffer::new(out_offsets.into()),
        Arc::new(out_struct),
        null_builder.finish(),
    );
    Ok(Arc::new(out))
}

fn resolve_output_type(
    output_type: Option<&DataType>,
    values: &[ArrayRef],
) -> Result<(Arc<Field>, Fields), String> {
    match output_type {
        Some(DataType::List(item_field)) => {
            let DataType::Struct(fields) = item_field.data_type() else {
                return Err(format!(
                    "arrays_zip output element type must be Struct, got {:?}",
                    item_field.data_type()
                ));
            };
            Ok((item_field.clone(), fields.clone()))
        }
        Some(other) => Err(format!(
            "arrays_zip output type must be List, got {:?}",
            other
        )),
        None => {
            let fields: Fields = values
                .iter()
                .enumerate()
                .map(|(idx, value)| {
                    Arc::new(Field::new(
                        format!("col{}", idx + 1),
                        value.data_type().clone(),
                        true,
                    ))
                })
                .collect();
            let struct_type = DataType::Struct(fields.clone());
            Ok((Arc::new(Field::new("item", struct_type, true)), fields))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::map::eval_map_function;
    use crate::exec::expr::function::map::test_utils::{slot_id_expr, typed_null};
    use arrow::array::{
        ArrayRef, Int64Array, Int64Builder, ListBuilder, StringArray, StringBuilder,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn chunk_with_two_lists() -> (Chunk, DataType, DataType) {
        let mut a = ListBuilder::new(Int64Builder::new());
        a.values().append_value(1);
        a.values().append_value(2);
        a.append(true);
        a.values().append_value(3);
        a.append(true);
        a.append(false);
        let a = Arc::new(a.finish()) as ArrayRef;
        let a_type = a.data_type().clone();

        let mut b = ListBuilder::new(StringBuilder::new());
        b.values().append_value("a");
        b.append(true);
        b.values().append_value("b");
        b.values().append_value("c");
        b.append(true);
        b.values().append_value("z");
        b.append(true);
        let b = Arc::new(b.finish()) as ArrayRef;
        let b_type = b.data_type().clone();

        let fields = vec![
            field_with_slot_id(Field::new("a", a_type.clone(), true), SlotId::new(1)),
            field_with_slot_id(Field::new("b", b_type.clone(), true), SlotId::new(2)),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![a, b]).unwrap();
        (Chunk::new(batch), a_type, b_type)
    }

    #[test]
    fn test_arrays_zip_basic_and_padding() {
        let (chunk, a_type, b_type) = chunk_with_two_lists();
        let mut arena = ExprArena::default();
        let a = slot_id_expr(&mut arena, 1, a_type);
        let b = slot_id_expr(&mut arena, 2, b_type);
        let out_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("col1", DataType::Int64, true)),
                Arc::new(Field::new("col2", DataType::Utf8, true)),
            ])),
            true,
        )));
        let expr = typed_null(&mut arena, out_type);

        let out = eval_map_function("arrays_zip", &arena, expr, &[a, b], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.value_length(0), 2);
        assert_eq!(list.value_length(1), 2);
        assert!(list.is_null(2));

        let values = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let col1 = values
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let col2 = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(col1.value(0), 1);
        assert_eq!(col2.value(0), "a");
        assert_eq!(col1.value(2), 3);
        assert_eq!(col2.value(2), "b");
        assert!(col1.is_null(3));
        assert_eq!(col2.value(3), "c");
    }
}
