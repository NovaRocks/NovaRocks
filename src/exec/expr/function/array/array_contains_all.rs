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
use arrow::array::{Array, ArrayRef, BooleanArray, ListArray};
use std::sync::Arc;

pub fn eval_array_contains_all(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let left_arr = arena.eval(args[0], chunk)?;
    let right_arr = arena.eval(args[1], chunk)?;
    let left = left_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "array_contains_all expects ListArray, got {:?}",
                left_arr.data_type()
            )
        })?;
    let right = right_arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            format!(
                "array_contains_all expects ListArray, got {:?}",
                right_arr.data_type()
            )
        })?;

    let mut left_values = left.values().clone();
    let mut right_values = right.values().clone();
    if left_values.data_type() != right_values.data_type() {
        let left_type = left_values.data_type().clone();
        let right_type = right_values.data_type().clone();
        if let Ok(casted) =
            super::common::cast_with_special_rules(&right_values, &left_type, "array_contains_all")
        {
            right_values = casted;
        } else if let Ok(casted) =
            super::common::cast_with_special_rules(&left_values, &right_type, "array_contains_all")
        {
            left_values = casted;
        } else {
            return Err(format!(
                "array_contains_all type mismatch after coercion attempts: {:?} vs {:?}",
                left_type, right_type
            ));
        }
    }
    let left_offsets = left.value_offsets();
    let right_offsets = right.value_offsets();

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let left_row = super::common::row_index(row, left.len());
        let right_row = super::common::row_index(row, right.len());
        if left.is_null(left_row) || right.is_null(right_row) {
            out.push(None);
            continue;
        }

        let left_start = left_offsets[left_row] as usize;
        let left_end = left_offsets[left_row + 1] as usize;
        let right_start = right_offsets[right_row] as usize;
        let right_end = right_offsets[right_row + 1] as usize;

        let mut contains_all = true;
        for ridx in right_start..right_end {
            let mut found = false;
            for lidx in left_start..left_end {
                if super::common::compare_values_with_null(
                    &left_values,
                    lidx,
                    &right_values,
                    ridx,
                    true,
                )? {
                    found = true;
                    break;
                }
            }
            if !found {
                contains_all = false;
                break;
            }
        }
        out.push(Some(contains_all));
    }

    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::BooleanArray;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_contains_all_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, DataType::Boolean);

        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let v2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let left = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, v2, v3],
            },
            list_type.clone(),
        );
        let right = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v2, v1],
            },
            list_type,
        );

        let out = eval_array_function("array_contains_all", &arena, expr, &[left, right], &chunk)
            .unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
    }

    #[test]
    fn test_array_contains_all_with_null_element() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, DataType::Boolean);

        let null_elem = typed_null(&mut arena, DataType::Int64);
        let v1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let left = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v1, null_elem],
            },
            list_type.clone(),
        );
        let right = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![null_elem],
            },
            list_type,
        );

        let out = eval_array_function("array_contains_all", &arena, expr, &[left, right], &chunk)
            .unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
    }
}
