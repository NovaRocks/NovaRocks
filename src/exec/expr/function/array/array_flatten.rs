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
use std::sync::Arc;

pub fn eval_array_flatten(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let outer = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_flatten expects ListArray, got {:?}", arr.data_type()))?;

    let inner = outer
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "array_flatten expects ARRAY<ARRAY<...>>, got {:?}",
                outer.data_type()
            )
        })?;

    let output_field = match arena.data_type(expr) {
        Some(DataType::List(field)) => field.clone(),
        _ => match inner.data_type() {
            DataType::List(field) => field.clone(),
            other => {
                return Err(format!(
                    "array_flatten output type must be List, got {:?}",
                    other
                ));
            }
        },
    };
    let target_item_type = output_field.data_type().clone();

    let mut inner_values = inner.values().clone();
    if inner_values.data_type() != &target_item_type {
        inner_values = cast(&inner_values, &target_item_type).map_err(|e| {
            format!(
                "array_flatten failed to cast inner element {:?} -> {:?}: {}",
                inner_values.data_type(),
                target_item_type,
                e
            )
        })?;
    }

    let inner_values_data = inner_values.to_data();
    let mut mutable = MutableArrayData::new(vec![&inner_values_data], false, 0);

    let outer_offsets = outer.value_offsets();
    let inner_offsets = inner.value_offsets();
    let mut out_offsets = Vec::with_capacity(chunk.len() + 1);
    out_offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut null_builder = NullBufferBuilder::new(chunk.len());

    for row in 0..chunk.len() {
        let outer_row = super::common::row_index(row, outer.len());
        if outer.is_null(outer_row) {
            out_offsets.push(current as i32);
            null_builder.append_null();
            continue;
        }

        let outer_start = outer_offsets[outer_row] as usize;
        let outer_end = outer_offsets[outer_row + 1] as usize;
        for inner_row in outer_start..outer_end {
            if inner.is_null(inner_row) {
                continue;
            }
            let start = inner_offsets[inner_row] as usize;
            let end = inner_offsets[inner_row + 1] as usize;
            if end > start {
                mutable.extend(0, start, end);
                current += (end - start) as i64;
            }
        }

        if current > i32::MAX as i64 {
            return Err("array_flatten offset overflow".to_string());
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
    fn test_array_flatten_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let inner_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let outer_type = DataType::List(Arc::new(Field::new("item", inner_type.clone(), true)));
        let expr = typed_null(&mut arena, inner_type.clone());

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let inner1 = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, v2],
            },
            inner_type.clone(),
        );
        let inner2 = arena.push_typed(
            ExprNode::ArrayExpr { elements: vec![v3] },
            inner_type.clone(),
        );
        let outer = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![inner1, inner2],
            },
            outer_type,
        );

        let out = eval_array_function("array_flatten", &arena, expr, &[outer], &chunk).unwrap();
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_array_flatten_skip_null_inner_and_keep_null_outer() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let inner_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let outer_type = DataType::List(Arc::new(Field::new("item", inner_type.clone(), true)));
        let expr = typed_null(&mut arena, inner_type.clone());

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let inner1 = arena.push_typed(
            ExprNode::ArrayExpr { elements: vec![v1] },
            inner_type.clone(),
        );
        let null_inner = typed_null(&mut arena, inner_type.clone());
        let outer = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![inner1, null_inner],
            },
            outer_type.clone(),
        );
        let out = eval_array_function("array_flatten", &arena, expr, &[outer], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        let values = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[1]);

        let expr2 = typed_null(&mut arena, inner_type);
        let outer_null = typed_null(&mut arena, outer_type);
        let out_null =
            eval_array_function("array_flatten", &arena, expr2, &[outer_null], &chunk).unwrap();
        let out_null = out_null.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(out_null.is_null(0));
    }
}
