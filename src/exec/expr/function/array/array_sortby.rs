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
use std::cmp::Ordering;
use std::sync::Arc;

fn compare_key_positions(
    key_values: &[ArrayRef],
    key_starts: &[Option<usize>],
    left_pos: usize,
    right_pos: usize,
) -> Result<Ordering, String> {
    for (idx, key_values) in key_values.iter().enumerate() {
        let Some(key_start) = key_starts[idx] else {
            continue;
        };
        let left_idx = key_start + left_pos;
        let right_idx = key_start + right_pos;

        let ord = if key_values.is_null(left_idx) || key_values.is_null(right_idx) {
            if key_values.is_null(left_idx) && key_values.is_null(right_idx) {
                Ordering::Equal
            } else if key_values.is_null(left_idx) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        } else {
            super::common::compare_values_ordered(key_values, left_idx, right_idx)?
        };
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

fn insertion_sort_by_keys(
    key_values: &[ArrayRef],
    key_starts: &[Option<usize>],
    indices: &mut [usize],
) -> Result<(), String> {
    for i in 1..indices.len() {
        let mut j = i;
        while j > 0 {
            let left = indices[j - 1];
            let right = indices[j];
            let ord = compare_key_positions(key_values, key_starts, left, right)?;

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

pub fn eval_array_sortby(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let src_arr = arena.eval(args[0], chunk)?;
    let src = src_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "array_sortby expects ListArray src, got {:?}",
                src_arr.data_type()
            )
        })?;
    let mut key_arrays = Vec::with_capacity(args.len().saturating_sub(1));
    for key_expr in args.iter().skip(1) {
        key_arrays.push(arena.eval(*key_expr, chunk)?);
    }
    let mut key_lists = Vec::with_capacity(key_arrays.len());
    for key_arr in &key_arrays {
        let key = key_arr
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                format!(
                    "array_sortby expects ListArray key, got {:?}",
                    key_arr.data_type()
                )
            })?;
        key_lists.push(key);
    }

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match src.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_sortby output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();

    let mut src_values = src.values().clone();
    if src_values.data_type() != &target_item_type {
        src_values = cast(&src_values, &target_item_type).map_err(|e| {
            format!(
                "array_sortby failed to cast src element {:?} -> {:?}: {}",
                src_values.data_type(),
                target_item_type,
                e
            )
        })?;
    }
    let key_values: Vec<ArrayRef> = key_lists.iter().map(|key| key.values().clone()).collect();

    let src_values_data = src_values.to_data();
    let mut mutable = MutableArrayData::new(vec![&src_values_data], false, 0);

    let src_offsets = src.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let src_row = super::common::row_index(row, src.len());
        if src.is_null(src_row) {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let src_start = src_offsets[src_row] as usize;
        let src_end = src_offsets[src_row + 1] as usize;
        let src_len = src_end.saturating_sub(src_start);

        let mut key_starts = Vec::with_capacity(key_lists.len());
        for key in &key_lists {
            let key_row = super::common::row_index(row, key.len());
            if key.is_null(key_row) {
                key_starts.push(None);
                continue;
            }
            let key_offsets = key.value_offsets();
            let key_start = key_offsets[key_row] as usize;
            let key_end = key_offsets[key_row + 1] as usize;
            let key_len = key_end.saturating_sub(key_start);
            if src_len != key_len {
                return Err("Input arrays' size are not equal in array_sortby.".to_string());
            }
            key_starts.push(Some(key_start));
        }

        let mut positions: Vec<usize> = (0..src_len).collect();
        insertion_sort_by_keys(&key_values, &key_starts, &mut positions)?;
        for pos in positions {
            let src_idx = src_start + pos;
            mutable.extend(0, src_idx, src_idx + 1);
            current += 1;
        }

        if current > i32::MAX as i64 {
            return Err("array_sortby offset overflow".to_string());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_sortby_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let src_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let key_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, src_type.clone());

        let a = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("b".to_string())),
            DataType::Utf8,
        );
        let b = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("a".to_string())),
            DataType::Utf8,
        );
        let c = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("c".to_string())),
            DataType::Utf8,
        );
        let src = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![a, b, c],
            },
            src_type,
        );

        let k2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let k1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let k3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let key = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![k2, k1, k3],
            },
            key_type,
        );

        let out = eval_array_function("array_sortby", &arena, expr, &[src, key], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "a");
        assert_eq!(values.value(1), "b");
        assert_eq!(values.value(2), "c");
    }

    #[test]
    fn test_array_sortby_key_null_keeps_source() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let src_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let key_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, src_type.clone());

        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let src = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v2, v1],
            },
            src_type,
        );
        let key_null = typed_null(&mut arena, key_type);

        let out =
            eval_array_function("array_sortby", &arena, expr, &[src, key_null], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[2, 1]);
    }
}
