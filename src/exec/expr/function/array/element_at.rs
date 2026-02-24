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
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, ListArray, StringArray, StringBuilder, UInt32Array,
};
use arrow::compute::take;

pub fn eval_element_at(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let arr = arena.eval(args[0], chunk)?;
    let subscript_arr = arena.eval(args[1], chunk)?;
    let check_arr = if args.len() == 3 {
        Some(arena.eval(args[2], chunk)?)
    } else {
        None
    };

    let list = arr
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| format!("element_at expects ListArray, got {:?}", arr.data_type()))?;
    let subscript_arr = subscript_arr
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| "element_at expects INT subscript".to_string())?;
    let check_arr = check_arr
        .as_ref()
        .map(|a| {
            a.as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "element_at check flag must be BOOLEAN".to_string())
        })
        .transpose()?;

    if subscript_arr.len() != 1 && subscript_arr.len() != list.len() {
        return Err(format!(
            "element_at subscript length mismatch: list rows={}, subscript rows={}",
            list.len(),
            subscript_arr.len()
        ));
    }
    if let Some(flags) = check_arr {
        if flags.len() != 1 && flags.len() != list.len() {
            return Err(format!(
                "element_at check flag length mismatch: list rows={}, check rows={}",
                list.len(),
                flags.len()
            ));
        }
    }

    let offsets = list.value_offsets();
    let values = list.values();
    let mut indices = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        let subscript_idx = super::common::row_index(row, subscript_arr.len());
        let check_idx = check_arr.map(|flags| super::common::row_index(row, flags.len()));
        let check_out_of_bounds = check_idx
            .map(|idx| {
                let flags = check_arr.expect("check_arr exists when check_idx exists");
                !flags.is_null(idx) && flags.value(idx)
            })
            .unwrap_or(false);

        if list.is_null(row) || subscript_arr.is_null(subscript_idx) {
            indices.push(None);
            continue;
        }

        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let row_len = end - start;
        let subscript = subscript_arr.value(subscript_idx);

        if subscript <= 0 {
            if check_out_of_bounds {
                return Err("Array subscript start at 1".to_string());
            }
            indices.push(None);
            continue;
        }

        let subscript = usize::try_from(subscript)
            .map_err(|_| "element_at subscript conversion failed".to_string())?;
        if subscript > row_len {
            if check_out_of_bounds {
                return Err(format!(
                    "Array subscript must be less than or equal to array length: {} > {}",
                    subscript, row_len
                ));
            }
            indices.push(None);
            continue;
        }

        let target = start + subscript - 1;
        let target = u32::try_from(target)
            .map_err(|_| "element_at index exceeds UInt32 range".to_string())?;
        indices.push(Some(target));
    }

    let indices = UInt32Array::from(indices);
    let out = take(values.as_ref(), &indices, None).map_err(|e| e.to_string())?;
    let out = maybe_unquote_wrapped_utf8(out)?;
    super::common::cast_output(out, arena.data_type(expr), "element_at")
}

fn maybe_unquote_wrapped_utf8(array: ArrayRef) -> Result<ArrayRef, String> {
    if array.data_type() != &arrow::datatypes::DataType::Utf8 {
        return Ok(array);
    }
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "failed to downcast to StringArray".to_string())?;
    let mut builder = StringBuilder::new();
    for row in 0..arr.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let value = arr.value(row);
        if value.len() >= 2 && value.starts_with('\"') && value.ends_with('\"') {
            let inner = &value[1..value.len() - 1];
            if !inner.contains('\"') && !inner.contains('\\') {
                builder.append_value(inner);
                continue;
            }
        }
        builder.append_value(value);
    }
    Ok(std::sync::Arc::new(builder.finish()) as ArrayRef)
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

    fn build_input(
        arena: &mut ExprArena,
    ) -> (
        ExprId,
        ExprId,
        ExprId,
        ExprId,
        ExprId,
        DataType,
        crate::exec::chunk::Chunk,
    ) {
        let chunk = chunk_len_1();
        let item_type = DataType::Int64;
        let list_type = DataType::List(Arc::new(Field::new("item", item_type.clone(), true)));
        let expr = typed_null(arena, item_type.clone());

        let e1 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(10)), DataType::Int64);
        let e2 = arena.push_typed(ExprNode::Literal(LiteralValue::Int64(20)), DataType::Int64);
        let arr = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![e1, e2],
            },
            list_type,
        );
        let idx_ok = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(2)), DataType::Int32);
        let idx_oob = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(3)), DataType::Int32);
        let check_true = arena.push_typed(
            ExprNode::Literal(LiteralValue::Bool(true)),
            DataType::Boolean,
        );
        (expr, arr, idx_ok, idx_oob, check_true, item_type, chunk)
    }

    #[test]
    fn test_element_at_returns_value() {
        let mut arena = ExprArena::default();
        let (expr, arr, idx_ok, _, _, _, chunk) = build_input(&mut arena);
        let out = eval_array_function("element_at", &arena, expr, &[arr, idx_ok], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 20);
    }

    #[test]
    fn test_element_at_out_of_bounds_returns_null_without_check() {
        let mut arena = ExprArena::default();
        let (expr, arr, _, idx_oob, _, _, chunk) = build_input(&mut arena);
        let out = eval_array_function("element_at", &arena, expr, &[arr, idx_oob], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(out.is_null(0));
    }

    #[test]
    fn test_element_at_out_of_bounds_errors_with_check() {
        let mut arena = ExprArena::default();
        let (expr, arr, _, idx_oob, check_true, _, chunk) = build_input(&mut arena);
        let err = eval_element_at(&arena, expr, &[arr, idx_oob, check_true], &chunk).unwrap_err();
        assert!(err.contains("Array subscript must be less than or equal to array length"));
    }
}
