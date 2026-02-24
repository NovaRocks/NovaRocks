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
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
};
use std::collections::HashSet;
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

pub fn eval_array_contains(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    if chunk.is_empty() {
        return Ok(Arc::new(BooleanArray::from(Vec::<Option<bool>>::new())) as ArrayRef);
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
                "array_contains expects ListArray, got {:?}",
                arr.data_type()
            )
        })?;
    let values = list.values();
    if target_arr.data_type() != values.data_type() {
        target_arr = super::common::cast_with_special_rules(
            &target_arr,
            values.data_type(),
            "array_contains",
        )?;
    }
    let offsets = list.value_offsets();

    if arr_is_const {
        if list.is_null(0) {
            return Ok(Arc::new(BooleanArray::from(vec![None; chunk.len()])) as ArrayRef);
        }
        let start = offsets[0] as usize;
        let end = offsets[1] as usize;

        macro_rules! const_numeric_fast_path {
            ($typed_arr:ty) => {
                if let (Some(typed_values), Some(typed_targets)) = (
                    values.as_any().downcast_ref::<$typed_arr>(),
                    target_arr.as_any().downcast_ref::<$typed_arr>(),
                ) {
                    let mut seen = HashSet::with_capacity(end.saturating_sub(start));
                    let mut has_null = false;
                    for i in start..end {
                        if typed_values.is_null(i) {
                            has_null = true;
                        } else {
                            seen.insert(typed_values.value(i));
                        }
                    }

                    let mut out = Vec::with_capacity(chunk.len());
                    for row in 0..chunk.len() {
                        let target_idx = if target_is_const {
                            0
                        } else {
                            super::common::row_index(row, typed_targets.len())
                        };
                        let value = if typed_targets.is_null(target_idx) {
                            has_null
                        } else {
                            seen.contains(&typed_targets.value(target_idx))
                        };
                        out.push(Some(value));
                    }
                    return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
                }
            };
        }

        const_numeric_fast_path!(Int8Array);
        const_numeric_fast_path!(Int16Array);
        const_numeric_fast_path!(Int32Array);
        if let (Some(string_values), Some(string_targets)) = (
            values.as_any().downcast_ref::<StringArray>(),
            target_arr.as_any().downcast_ref::<StringArray>(),
        ) {
            let mut seen: HashSet<&str> = HashSet::with_capacity(end.saturating_sub(start));
            let mut has_null = false;
            for i in start..end {
                if string_values.is_null(i) {
                    has_null = true;
                } else {
                    seen.insert(string_values.value(i));
                }
            }

            let mut out = Vec::with_capacity(chunk.len());
            for row in 0..chunk.len() {
                let target_idx = if target_is_const {
                    0
                } else {
                    super::common::row_index(row, string_targets.len())
                };
                let value = if string_targets.is_null(target_idx) {
                    has_null
                } else {
                    seen.contains(string_targets.value(target_idx))
                };
                out.push(Some(value));
            }
            return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
        }
        if let (Some(int_values), Some(int_targets)) = (
            values.as_any().downcast_ref::<Int64Array>(),
            target_arr.as_any().downcast_ref::<Int64Array>(),
        ) {
            let mut seen = HashSet::with_capacity(end.saturating_sub(start));
            let mut has_null = false;
            for i in start..end {
                if int_values.is_null(i) {
                    has_null = true;
                } else {
                    seen.insert(int_values.value(i));
                }
            }

            let mut out = Vec::with_capacity(chunk.len());
            for row in 0..chunk.len() {
                let target_idx = if target_is_const {
                    0
                } else {
                    super::common::row_index(row, int_targets.len())
                };
                let value = if int_targets.is_null(target_idx) {
                    has_null
                } else {
                    seen.contains(&int_targets.value(target_idx))
                };
                out.push(Some(value));
            }
            return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
        }

        if target_is_const {
            let target_idx = 0usize;
            let result = if target_arr.is_null(target_idx) {
                let mut found_null = false;
                for i in start..end {
                    if values.is_null(i) {
                        found_null = true;
                        break;
                    }
                }
                Some(found_null)
            } else {
                let mut found = false;
                for i in start..end {
                    if super::common::compare_values_with_null(
                        &values,
                        i,
                        &target_arr,
                        target_idx,
                        false,
                    )? {
                        found = true;
                        break;
                    }
                }
                Some(found)
            };
            return Ok(Arc::new(BooleanArray::from(vec![result; chunk.len()])) as ArrayRef);
        }

        let mut out = Vec::with_capacity(chunk.len());
        for row in 0..chunk.len() {
            let target_idx = super::common::row_index(row, target_arr.len());
            if target_arr.is_null(target_idx) {
                let mut found_null = false;
                for i in start..end {
                    if values.is_null(i) {
                        found_null = true;
                        break;
                    }
                }
                out.push(Some(found_null));
                continue;
            }

            let mut found = false;
            for i in start..end {
                if super::common::compare_values_with_null(
                    &values,
                    i,
                    &target_arr,
                    target_idx,
                    false,
                )? {
                    found = true;
                    break;
                }
            }
            out.push(Some(found));
        }
        return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
    }

    if list.len() == 1 {
        if list.is_null(0) {
            return Ok(Arc::new(BooleanArray::from(vec![None; chunk.len()])) as ArrayRef);
        }
        let start = offsets[0] as usize;
        let end = offsets[1] as usize;

        if let (Some(int_values), Some(int_targets)) = (
            values.as_any().downcast_ref::<Int64Array>(),
            target_arr.as_any().downcast_ref::<Int64Array>(),
        ) {
            let mut seen = HashSet::with_capacity(end.saturating_sub(start));
            let mut has_null = false;
            for i in start..end {
                if int_values.is_null(i) {
                    has_null = true;
                } else {
                    seen.insert(int_values.value(i));
                }
            }

            let mut out = Vec::with_capacity(chunk.len());
            for row in 0..chunk.len() {
                let target_idx = super::common::row_index(row, int_targets.len());
                let value = if int_targets.is_null(target_idx) {
                    has_null
                } else {
                    seen.contains(&int_targets.value(target_idx))
                };
                out.push(Some(value));
            }
            return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
        }

        if target_arr.len() != 1 {
            let mut out = Vec::with_capacity(chunk.len());
            for row in 0..chunk.len() {
                let target_idx = super::common::row_index(row, target_arr.len());
                if target_arr.is_null(target_idx) {
                    let mut found_null = false;
                    for i in start..end {
                        if values.is_null(i) {
                            found_null = true;
                            break;
                        }
                    }
                    out.push(Some(found_null));
                    continue;
                }
                let mut found = false;
                for i in start..end {
                    if super::common::compare_values_with_null(
                        &values,
                        i,
                        &target_arr,
                        target_idx,
                        false,
                    )? {
                        found = true;
                        break;
                    }
                }
                out.push(Some(found));
            }
            return Ok(Arc::new(BooleanArray::from(out)) as ArrayRef);
        }

        let result = if target_arr.is_null(0) {
            let mut found_null = false;
            for i in start..end {
                if values.is_null(i) {
                    found_null = true;
                    break;
                }
            }
            Some(found_null)
        } else {
            let mut found = false;
            for i in start..end {
                if super::common::compare_values_with_null(&values, i, &target_arr, 0, false)? {
                    found = true;
                    break;
                }
            }
            Some(found)
        };
        return Ok(Arc::new(BooleanArray::from(vec![result; chunk.len()])) as ArrayRef);
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
            let mut found_null = false;
            for i in start..end {
                if values.is_null(i) {
                    found_null = true;
                    break;
                }
            }
            out.push(Some(found_null));
            continue;
        }

        let mut found = false;
        for i in start..end {
            if super::common::compare_value_to_target(&values, i, &target_arr, row)? {
                found = true;
                break;
            }
        }
        out.push(Some(found));
    }

    Ok(Arc::new(BooleanArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::array::eval_array_function;
    use crate::exec::expr::function::array::test_utils::{chunk_len_1, literal_i64, typed_null};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_array_contains_true_false() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Boolean);

        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![e1, e2],
            },
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        );
        let target = literal_i64(&mut arena, 2);

        let out =
            eval_array_function("array_contains", &arena, expr, &[arr, target], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
    }

    #[test]
    fn test_array_contains_null_target() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Boolean);
        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr { elements: vec![e1] },
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        );
        let target = typed_null(&mut arena, DataType::Int64);

        let out = eval_array_contains(&arena, expr, &[arr, target], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!out.is_null(0));
        assert!(!out.value(0));
    }
}
