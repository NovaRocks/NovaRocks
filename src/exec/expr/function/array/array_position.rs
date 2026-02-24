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
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{Array, ArrayRef, Int64Array};
use std::sync::Arc;

fn is_row_constant_expr(arena: &ExprArena, expr: ExprId) -> bool {
    match arena.node(expr) {
        Some(ExprNode::Literal(_)) => true,
        Some(ExprNode::SlotId(_)) => false,
        Some(ExprNode::ArrayExpr { elements }) => elements
            .iter()
            .all(|child| is_row_constant_expr(arena, *child)),
        Some(ExprNode::StructExpr { fields }) => fields
            .iter()
            .all(|child| is_row_constant_expr(arena, *child)),
        Some(ExprNode::LambdaFunction { .. }) => false,
        Some(ExprNode::DictDecode { child, .. }) => is_row_constant_expr(arena, *child),
        Some(ExprNode::Cast(child))
        | Some(ExprNode::CastTime(child))
        | Some(ExprNode::CastTimeFromDatetime(child))
        | Some(ExprNode::Not(child))
        | Some(ExprNode::IsNull(child))
        | Some(ExprNode::IsNotNull(child))
        | Some(ExprNode::Clone(child)) => is_row_constant_expr(arena, *child),
        Some(ExprNode::Add(a, b))
        | Some(ExprNode::Sub(a, b))
        | Some(ExprNode::Mul(a, b))
        | Some(ExprNode::Div(a, b))
        | Some(ExprNode::Mod(a, b))
        | Some(ExprNode::Eq(a, b))
        | Some(ExprNode::EqForNull(a, b))
        | Some(ExprNode::Ne(a, b))
        | Some(ExprNode::Lt(a, b))
        | Some(ExprNode::Le(a, b))
        | Some(ExprNode::Gt(a, b))
        | Some(ExprNode::Ge(a, b))
        | Some(ExprNode::And(a, b))
        | Some(ExprNode::Or(a, b)) => {
            is_row_constant_expr(arena, *a) && is_row_constant_expr(arena, *b)
        }
        Some(ExprNode::In { child, values, .. }) => {
            is_row_constant_expr(arena, *child)
                && values
                    .iter()
                    .all(|value| is_row_constant_expr(arena, *value))
        }
        Some(ExprNode::Case { children, .. }) => children
            .iter()
            .all(|child| is_row_constant_expr(arena, *child)),
        Some(ExprNode::FunctionCall { args, .. }) => {
            args.iter().all(|child| is_row_constant_expr(arena, *child))
        }
        _ => false,
    }
}

pub fn eval_array_position(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if chunk.is_empty() {
        let out = Arc::new(Int64Array::from(Vec::<Option<i64>>::new())) as ArrayRef;
        return super::common::cast_output(out, arena.data_type(expr), "array_position");
    }
    let arr_is_const = is_row_constant_expr(arena, args[0]);
    let target_is_const = is_row_constant_expr(arena, args[1]);
    let const_chunk = chunk.slice(0, 1);
    let arr_eval_chunk = if arr_is_const { &const_chunk } else { chunk };
    let target_eval_chunk = if target_is_const { &const_chunk } else { chunk };
    let arr = arena.eval(args[0], arr_eval_chunk)?;
    let mut target_arr = arena.eval(args[1], target_eval_chunk)?;
    let list = arr
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .ok_or_else(|| {
            format!(
                "array_position expects ListArray, got {:?}",
                arr.data_type()
            )
        })?;
    let values = list.values();
    if target_arr.data_type() != values.data_type() {
        target_arr = super::common::cast_with_special_rules(
            &target_arr,
            values.data_type(),
            "array_position",
        )?;
    }
    let offsets = list.value_offsets();

    if arr_is_const {
        if list.is_null(0) {
            let out = Arc::new(Int64Array::from(vec![None; chunk.len()])) as ArrayRef;
            return super::common::cast_output(out, arena.data_type(expr), "array_position");
        }

        let start = offsets[0] as usize;
        let end = offsets[1] as usize;
        if target_is_const {
            let pos = if target_arr.is_null(0) {
                let mut p = 0_i64;
                for (idx, i) in (start..end).enumerate() {
                    if values.is_null(i) {
                        p = idx as i64 + 1;
                        break;
                    }
                }
                p
            } else {
                let mut p = 0_i64;
                for (idx, i) in (start..end).enumerate() {
                    if super::common::compare_values_with_null(&values, i, &target_arr, 0, false)? {
                        p = idx as i64 + 1;
                        break;
                    }
                }
                p
            };
            let out = Arc::new(Int64Array::from(vec![Some(pos); chunk.len()])) as ArrayRef;
            return super::common::cast_output(out, arena.data_type(expr), "array_position");
        }

        let mut out = Vec::with_capacity(chunk.len());
        for row in 0..chunk.len() {
            let target_idx = super::common::row_index(row, target_arr.len());
            if target_arr.is_null(target_idx) {
                let mut pos = 0_i64;
                for (idx, i) in (start..end).enumerate() {
                    if values.is_null(i) {
                        pos = idx as i64 + 1;
                        break;
                    }
                }
                out.push(Some(pos));
                continue;
            }

            let mut pos = 0_i64;
            for (idx, i) in (start..end).enumerate() {
                if super::common::compare_values_with_null(
                    &values,
                    i,
                    &target_arr,
                    target_idx,
                    false,
                )? {
                    pos = idx as i64 + 1;
                    break;
                }
            }
            out.push(Some(pos));
        }
        let out = Arc::new(Int64Array::from(out)) as ArrayRef;
        return super::common::cast_output(out, arena.data_type(expr), "array_position");
    }

    let mut out = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        let target_idx = super::common::row_index(row, target_arr.len());
        if list.is_null(row) {
            out.push(None);
            continue;
        }

        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;

        if target_arr.is_null(target_idx) {
            let mut pos = 0_i64;
            for (idx, i) in (start..end).enumerate() {
                if values.is_null(i) {
                    pos = idx as i64 + 1;
                    break;
                }
            }
            out.push(Some(pos));
            continue;
        }

        let mut pos = 0_i64;
        for (idx, i) in (start..end).enumerate() {
            if super::common::compare_value_to_target(&values, i, &target_arr, row)? {
                pos = idx as i64 + 1;
                break;
            }
        }
        out.push(Some(pos));
    }

    let out = Arc::new(Int64Array::from(out)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "array_position")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, literal_i64, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_position_found() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Int32);

        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let e3 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(3)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![e1, e2, e3],
            },
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        );
        let target = literal_i64(&mut arena, 2);

        let out =
            eval_array_function("array_position", &arena, expr, &[arr, target], &chunk).unwrap();
        let out = out
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(out.value(0), 2);
    }

    #[test]
    fn test_array_position_not_found() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Int64);

        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr { elements: vec![e1] },
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        );
        let target = literal_i64(&mut arena, 9);

        let out = eval_array_position(&arena, expr, &[arr, target], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 0);
    }
}
