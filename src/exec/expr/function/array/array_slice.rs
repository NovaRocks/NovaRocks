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
use arrow::array::{Array, ArrayRef, Int64Array, ListArray, make_array};
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_array_slice(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let offset_arr = arena.eval(args[1], chunk)?;
    let length_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };

    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_slice expects ListArray, got {:?}", arr.data_type()))?;
    let offset_arr = offset_arr
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "array_slice expects BIGINT offset".to_string())?;
    let length_arr = length_arr
        .as_ref()
        .map(|a| {
            a.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "array_slice expects BIGINT length".to_string())
        })
        .transpose()?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match list.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_slice output type must be List, got {:?}",
                    other
                ));
            }
        },
    };

    let values = list.values();
    let values_data = values.to_data();
    let data_refs = vec![&values_data];
    let mut mutable = MutableArrayData::new(data_refs, false, values.len());

    let mut offsets = Vec::with_capacity(chunk.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    let list_offsets = list.value_offsets();
    for row in 0..chunk.len() {
        let off_idx = super::common::row_index(row, offset_arr.len());
        let len_idx = length_arr
            .as_ref()
            .map(|a| super::common::row_index(row, a.len()));

        if list.is_null(row)
            || offset_arr.is_null(off_idx)
            || len_idx
                .and_then(|i| length_arr.as_ref().map(|a| a.is_null(i)))
                .unwrap_or(false)
        {
            null_builder.append_null();
            offsets.push(current as i32);
            continue;
        }

        let row_start = list_offsets[row] as i64;
        let row_end = list_offsets[row + 1] as i64;
        let row_len = row_end - row_start;

        let offset = offset_arr.value(off_idx);
        // StarRocks array_slice is 1-based for positive offsets, supports negative offsets from
        // the end, and treats offset=0 as an empty slice.
        let mut start = if offset > 0 {
            offset - 1
        } else if offset < 0 {
            row_len + offset
        } else {
            row_len
        };
        if start < 0 {
            start = 0;
        }
        if start > row_len {
            start = row_len;
        }

        let take_len = match (length_arr, len_idx) {
            (Some(arr), Some(i)) => arr.value(i).max(0),
            _ => row_len - start,
        };

        let mut end = start + take_len;
        if end > row_len {
            end = row_len;
        }

        let abs_start = (row_start + start) as usize;
        let abs_end = (row_start + end) as usize;
        if abs_end > abs_start {
            mutable.extend(0, abs_start, abs_end);
        }

        current += end - start;
        if current > i32::MAX as i64 {
            return Err("array_slice offset overflow".to_string());
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
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, literal_i64, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::Int64Array;
    use arrow::datatypes::Field;

    #[test]
    fn test_array_slice_two_args() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let e3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![e1, e2, e3],
            },
            list_type.clone(),
        );
        let offset = literal_i64(&mut arena, 2);

        let out = eval_array_function("array_slice", &arena, expr, &[arr, offset], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), 2);
        assert_eq!(values.value(1), 3);
    }

    #[test]
    fn test_array_slice_three_args() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let e3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![e1, e2, e3],
            },
            list_type.clone(),
        );
        let offset = literal_i64(&mut arena, 1);
        let len = literal_i64(&mut arena, 2);

        let out =
            eval_array_function("array_slice", &arena, expr, &[arr, offset, len], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
    }

    #[test]
    fn test_array_slice_zero_offset_returns_empty() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());

        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let e3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![e1, e2, e3],
            },
            list_type.clone(),
        );
        let offset = literal_i64(&mut arena, 0);
        let len = literal_i64(&mut arena, 2);

        let out =
            eval_array_function("array_slice", &arena, expr, &[arr, offset, len], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.len(), 0);
    }

    #[test]
    fn test_array_slice_null_input() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, list_type.clone());
        let arr = typed_null(&mut arena, list_type);
        let offset = literal_i64(&mut arena, 1);
        let out = eval_array_slice(&arena, expr, &[arr, offset], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(out.is_null(0));
    }
}
