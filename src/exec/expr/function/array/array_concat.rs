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
use arrow::datatypes::{DataType, Field, Fields};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

fn relax_map_entry_nullability(target_type: &DataType, source_type: &DataType) -> DataType {
    let (DataType::Map(target_map_field, ordered), DataType::Map(source_map_field, _)) =
        (target_type, source_type)
    else {
        return target_type.clone();
    };
    let DataType::Struct(target_entries) = target_map_field.data_type() else {
        return target_type.clone();
    };
    let DataType::Struct(source_entries) = source_map_field.data_type() else {
        return target_type.clone();
    };
    if target_entries.len() != source_entries.len() {
        return target_type.clone();
    }

    let mut adjusted_entries = target_entries.iter().cloned().collect::<Vec<_>>();
    let mut changed = false;
    for idx in 0..adjusted_entries.len() {
        if !target_entries[idx].is_nullable() && source_entries[idx].is_nullable() {
            adjusted_entries[idx] = Arc::new(Field::new(
                target_entries[idx].name(),
                target_entries[idx].data_type().clone(),
                true,
            ));
            changed = true;
        }
    }
    if !changed {
        return target_type.clone();
    }

    DataType::Map(
        Arc::new(Field::new(
            target_map_field.name(),
            DataType::Struct(Fields::from(adjusted_entries)),
            target_map_field.is_nullable(),
        )),
        *ordered,
    )
}

pub fn eval_array_concat(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("array_concat expects at least one argument".to_string());
    }

    let mut list_arrays = Vec::with_capacity(args.len());
    for arg in args {
        let arr = arena.eval(*arg, chunk)?;
        arr.as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| format!("array_concat expects ListArray, got {:?}", arr.data_type()))?;
        list_arrays.push(arr);
    }

    let mut output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list_arrays[0].data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_concat output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let mut target_item_type = output_field.data_type().clone();
    for arr in &list_arrays {
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        target_item_type =
            relax_map_entry_nullability(&target_item_type, list.values().data_type());
    }
    if output_field.data_type() != &target_item_type {
        output_field = Arc::new(Field::new(
            output_field.name(),
            target_item_type.clone(),
            output_field.is_nullable(),
        ));
    }

    let mut value_arrays: Vec<ArrayRef> = Vec::with_capacity(list_arrays.len());
    for arr in &list_arrays {
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        let mut values = list.values().clone();
        if values.data_type() != &target_item_type {
            values =
                super::common::cast_with_special_rules(&values, &target_item_type, "array_concat")?;
        }
        value_arrays.push(values);
    }
    let values_data: Vec<arrow_data::ArrayData> =
        value_arrays.iter().map(|v| v.to_data()).collect();
    let data_refs: Vec<&arrow_data::ArrayData> = values_data.iter().collect();
    let mut mutable = MutableArrayData::new(data_refs, false, 0);

    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let mut row_null = false;
        let mut row_len: i64 = 0;

        for arr in &list_arrays {
            let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let row_idx = super::common::row_index(row, list.len());
            if list.is_null(row_idx) {
                row_null = true;
                break;
            }
            let offs = list.value_offsets();
            row_len += (offs[row_idx + 1] - offs[row_idx]) as i64;
        }

        if row_null {
            null_builder.append_null();
            offsets.push(current as i32);
            continue;
        }

        for (src_idx, arr) in list_arrays.iter().enumerate() {
            let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let row_idx = super::common::row_index(row, list.len());
            let offs = list.value_offsets();
            let start = offs[row_idx] as usize;
            let end = offs[row_idx + 1] as usize;
            if end > start {
                mutable.extend(src_idx, start, end);
            }
        }

        current += row_len;
        if current > i32::MAX as i64 {
            return Err("array_concat offset overflow".to_string());
        }
        offsets.push(current as i32);
        null_builder.append_non_null();
    }

    let out_values = make_array(mutable.freeze());
    let list = ListArray::new(
        output_field,
        OffsetBuffer::new(offsets.into()),
        out_values,
        null_builder.finish(),
    );
    Ok(Arc::new(list) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::Int64Array;
    use arrow::datatypes::Field;

    #[test]
    fn test_array_concat_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let b1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![a1, a2],
            },
            list_type.clone(),
        );
        let arr2 = arena.push_typed(ExprNode::ArrayExpr { elements: vec![b1] }, list_type);

        let out = eval_array_function("array_concat", &arena, expr, &[arr1, arr2], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert_eq!(values.value(2), 3);
    }
}
