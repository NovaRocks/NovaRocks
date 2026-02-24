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
use arrow::array::{Array, ArrayRef, BooleanArray, ListArray, make_array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_data::transform::MutableArrayData;
use std::sync::Arc;

pub fn eval_array_filter(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let src_arr = arena.eval(args[0], chunk)?;
    let filter_arr = arena.eval(args[1], chunk)?;
    let src = src_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "array_filter expects ListArray src, got {:?}",
                src_arr.data_type()
            )
        })?;
    let filter = filter_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "array_filter expects ListArray filter, got {:?}",
                filter_arr.data_type()
            )
        })?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match src.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_filter output type must be List, got {:?}",
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
                "array_filter failed to cast src element {:?} -> {:?}: {}",
                src_values.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let mut filter_values = filter.values().clone();
    if filter_values.data_type() != &DataType::Boolean {
        filter_values = cast(&filter_values, &DataType::Boolean).map_err(|e| {
            format!(
                "array_filter failed to cast filter element to BOOLEAN: {}",
                e
            )
        })?;
    }
    let filter_values = filter_values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            "array_filter failed to downcast filter values to BooleanArray".to_string()
        })?;

    let src_values_data = src_values.to_data();
    let mut mutable = MutableArrayData::new(vec![&src_values_data], false, 0);

    let src_offsets = src.value_offsets();
    let filter_offsets = filter.value_offsets();
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

        let filter_row = super::common::row_index(row, filter.len());
        if !filter.is_null(filter_row) {
            let filter_start = filter_offsets[filter_row] as usize;
            let filter_end = filter_offsets[filter_row + 1] as usize;
            let filter_len = filter_end.saturating_sub(filter_start);

            for pos in 0..src_len {
                if pos >= filter_len {
                    break;
                }
                let f_idx = filter_start + pos;
                if !filter_values.is_null(f_idx) && filter_values.value(f_idx) {
                    let src_idx = src_start + pos;
                    mutable.extend(0, src_idx, src_idx + 1);
                    current += 1;
                }
            }
        }

        if current > i32::MAX as i64 {
            return Err("array_filter offset overflow".to_string());
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
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_filter_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let src_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let filter_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));
        let expr = typed_null(&mut arena, src_type.clone());

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let src = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, v2, v3],
            },
            src_type,
        );
        let t = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            DataType::Boolean,
        );
        let f = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(false)),
            DataType::Boolean,
        );
        let filter = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![t, f, t],
            },
            filter_type,
        );

        let out =
            eval_array_function("array_filter", &arena, expr, &[src, filter], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[1, 3]);
    }

    #[test]
    fn test_array_filter_filter_null_row_returns_empty() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let src_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let filter_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));
        let expr = typed_null(&mut arena, src_type.clone());

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let src = arena.push_typed(ExprNode::ArrayExpr { elements: vec![v1] }, src_type);
        let filter = typed_null(&mut arena, filter_type);

        let out =
            eval_array_function("array_filter", &arena, expr, &[src, filter], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.value_length(0), 0);
        assert!(!out.is_null(0));
    }
}
