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
use arrow::array::{Array, ArrayRef, ListArray, UInt32Array};
use arrow::compute::{cast, take};
use std::cmp::Ordering;

pub fn eval_array_max(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("array_max expects ListArray, got {:?}", arr.data_type()))?;

    let target_type = match arena.data_type(expr) {
        Some(t) => t.clone(),
        None => list.values().data_type().clone(),
    };
    let mut values = list.values().clone();
    if values.data_type() != &target_type {
        values = cast(&values, &target_type).map_err(|e| {
            format!(
                "array_max failed to cast element type {:?} -> {:?}: {}",
                values.data_type(),
                target_type,
                e
            )
        })?;
    }

    let offsets = list.value_offsets();
    let mut picked = Vec::<Option<u32>>::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let row_idx = super::common::row_index(row, list.len());
        if list.is_null(row_idx) {
            picked.push(None);
            continue;
        }

        let start = offsets[row_idx] as usize;
        let end = offsets[row_idx + 1] as usize;
        let mut best = None::<usize>;
        for idx in start..end {
            if values.is_null(idx) {
                continue;
            }
            best = match best {
                None => Some(idx),
                Some(prev) => match super::common::compare_values_ordered(&values, prev, idx)? {
                    Ordering::Less => Some(idx),
                    _ => Some(prev),
                },
            };
        }
        picked.push(best.map(|idx| idx as u32));
    }

    if crate::common::largeint::is_largeint_data_type(values.data_type()) {
        let typed = crate::common::largeint::as_fixed_size_binary_array(&values, "array_max")?;
        let mut out_values = Vec::with_capacity(picked.len());
        for picked_idx in &picked {
            let Some(idx) = picked_idx else {
                out_values.push(None);
                continue;
            };
            let idx = *idx as usize;
            if typed.is_null(idx) {
                out_values.push(None);
            } else {
                out_values.push(Some(crate::common::largeint::value_at(typed, idx)?));
            }
        }
        let out = crate::common::largeint::array_from_i128(&out_values)?;
        return super::common::cast_output(out, arena.data_type(expr), "array_max");
    }

    let indices = UInt32Array::from(picked);
    let out = take(values.as_ref(), &indices, None).map_err(|e| e.to_string())?;
    super::common::cast_output(out, arena.data_type(expr), "array_max")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_array_max_basic() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, DataType::Int64);
        let v5 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(5)), DataType::Int64);
        let v3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let v7 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(7)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![v5, v3, v7],
            },
            list_type,
        );

        let out = eval_array_function("array_max", &arena, expr, &[arr], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 7);
    }

    #[test]
    fn test_array_max_empty_or_all_null() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let expr = typed_null(&mut arena, DataType::Int64);

        let empty = arena.push_typed(ExprNode::ArrayExpr { elements: vec![] }, list_type.clone());
        let out_empty = eval_array_function("array_max", &arena, expr, &[empty], &chunk).unwrap();
        let out_empty = out_empty.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(out_empty.is_null(0));

        let null_elem = typed_null(&mut arena, DataType::Int64);
        let arr_all_null = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![null_elem, null_elem],
            },
            list_type,
        );
        let expr2 = typed_null(&mut arena, DataType::Int64);
        let out_null =
            eval_array_function("array_max", &arena, expr2, &[arr_all_null], &chunk).unwrap();
        let out_null = out_null.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(out_null.is_null(0));
    }
}
