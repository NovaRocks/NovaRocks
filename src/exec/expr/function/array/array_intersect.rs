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
use std::cmp::Ordering;
use std::sync::Arc;

fn insertion_sort_indices(values: &ArrayRef, indices: &mut [usize]) -> Result<(), String> {
    for i in 1..indices.len() {
        let mut j = i;
        while j > 0 {
            let prev = indices[j - 1];
            let curr = indices[j];
            let ord = super::common::compare_values_ordered(values, prev, curr)?;
            if ord == Ordering::Greater {
                indices.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
}

pub fn eval_array_intersect(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() < 2 {
        return Err("array_intersect expects at least two arguments".to_string());
    }

    let mut list_arrays = Vec::with_capacity(args.len());
    for arg in args {
        let arr = arena.eval(*arg, chunk)?;
        arr.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            format!(
                "array_intersect expects ListArray, got {:?}",
                arr.data_type()
            )
        })?;
        list_arrays.push(arr);
    }

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list_arrays[0].data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_intersect output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();

    let mut value_arrays: Vec<ArrayRef> = Vec::with_capacity(list_arrays.len());
    for arr in &list_arrays {
        let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
        let mut values = list.values().clone();
        if values.data_type() != &target_item_type {
            values = super::common::cast_with_special_rules(
                &values,
                &target_item_type,
                "array_intersect",
            )?;
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
        let mut row_indices = Vec::<usize>::new();
        let mut row_null_index: Option<usize> = None;

        for arr in &list_arrays {
            let list = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let idx = super::common::row_index(row, list.len());
            if list.is_null(idx) {
                row_null = true;
                break;
            }
        }
        if row_null {
            null_builder.append_null();
            offsets.push(current as i32);
            continue;
        }

        let first = list_arrays[0].as_any().downcast_ref::<ListArray>().unwrap();
        let first_row = super::common::row_index(row, first.len());
        let first_offsets = first.value_offsets();
        let first_start = first_offsets[first_row] as usize;
        let first_end = first_offsets[first_row + 1] as usize;

        for i in first_start..first_end {
            if value_arrays[0].is_null(i) {
                if row_null_index.is_some() {
                    continue;
                }
                let mut null_in_all = true;
                for idx in 1..list_arrays.len() {
                    let list = list_arrays[idx]
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap();
                    let list_row = super::common::row_index(row, list.len());
                    let offs = list.value_offsets();
                    let s = offs[list_row] as usize;
                    let e = offs[list_row + 1] as usize;
                    let mut found_null = false;
                    for j in s..e {
                        if value_arrays[idx].is_null(j) {
                            found_null = true;
                            break;
                        }
                    }
                    if !found_null {
                        null_in_all = false;
                        break;
                    }
                }
                if null_in_all {
                    row_null_index = Some(i);
                }
                continue;
            }

            let mut duplicated = false;
            for &picked in &row_indices {
                if super::common::compare_values_at(&value_arrays[0], i, &value_arrays[0], picked)?
                {
                    duplicated = true;
                    break;
                }
            }
            if duplicated {
                continue;
            }

            let mut in_all = true;
            for idx in 1..list_arrays.len() {
                let list = list_arrays[idx]
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                let list_row = super::common::row_index(row, list.len());
                let offs = list.value_offsets();
                let s = offs[list_row] as usize;
                let e = offs[list_row + 1] as usize;

                let mut found = false;
                for j in s..e {
                    if super::common::compare_values_at(&value_arrays[0], i, &value_arrays[idx], j)?
                    {
                        found = true;
                        break;
                    }
                }
                if !found {
                    in_all = false;
                    break;
                }
            }
            if in_all {
                row_indices.push(i);
            }
        }
        insertion_sort_indices(&value_arrays[0], &mut row_indices)?;

        if let Some(idx) = row_null_index {
            mutable.extend(0, idx, idx + 1);
            current += 1;
        }
        for idx in row_indices {
            mutable.extend(0, idx, idx + 1);
            current += 1;
        }
        if current > i32::MAX as i64 {
            return Err("array_intersect offset overflow".to_string());
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
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_intersect_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let a3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let b1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let b2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![a1, a2, a3],
            },
            list_type.clone(),
        );
        let arr2 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![b1, b2],
            },
            list_type,
        );

        let out =
            eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values.value(0), 2);
    }

    #[test]
    fn test_array_intersect_keeps_shared_null_once() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let a1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let an = typed_null(&mut arena, DataType::Int64);
        let a2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let bn = typed_null(&mut arena, DataType::Int64);
        let b2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let arr1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![a1, an, a2],
            },
            list_type.clone(),
        );
        let arr2 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![bn, b2],
            },
            list_type,
        );

        let out =
            eval_array_function("array_intersect", &arena, expr, &[arr1, arr2], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.is_null(0));
        assert_eq!(values.value(1), 2);
    }
}
