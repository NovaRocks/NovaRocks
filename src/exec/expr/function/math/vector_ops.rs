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
use super::common::cast_output;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

fn row_index(row: usize, len: usize, out_len: usize) -> usize {
    if len == 1 && out_len > 1 { 0 } else { row }
}

fn ensure_row_count(
    len: usize,
    out_len: usize,
    fn_name: &str,
    arg_name: &str,
) -> Result<(), String> {
    if out_len == 0 || len == out_len || len == 1 {
        return Ok(());
    }
    Err(format!(
        "{} requires array arguments with row count 1 or {}, but {} array size is {}",
        fn_name, out_len, arg_name, len
    ))
}

fn require_list_arg<'a>(arg: &'a ArrayRef, fn_name: &str) -> Result<&'a ListArray, String> {
    arg.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        format!(
            "{} expects ListArray arguments, got {:?}",
            fn_name,
            arg.data_type()
        )
    })
}

fn cast_list_values_to_f64(
    list: &ListArray,
    fn_name: &str,
    arg_name: &str,
) -> Result<Arc<Float64Array>, String> {
    let values = list.values();
    let casted = cast(&values, &DataType::Float64).map_err(|e| {
        format!(
            "{} expects numeric array elements for {} argument: {}",
            fn_name, arg_name, e
        )
    })?;
    let out = casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| format!("{} failed to cast {} values to float64", fn_name, arg_name))?;
    if out.null_count() > 0 {
        return Err(format!(
            "{} does not support null values. {} array has null value.",
            fn_name, arg_name
        ));
    }
    Ok(Arc::new(out.clone()))
}

fn eval_cosine_impl(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    normalized: bool,
    fn_name: &'static str,
) -> Result<ArrayRef, String> {
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_list = require_list_arg(&left, fn_name)?;
    let right_list = require_list_arg(&right, fn_name)?;

    if left_list.null_count() > 0 {
        return Err(format!(
            "{} does not support null values. base array has null value.",
            fn_name
        ));
    }
    if right_list.null_count() > 0 {
        return Err(format!(
            "{} does not support null values. target array has null value.",
            fn_name
        ));
    }

    let out_len = chunk.len();
    ensure_row_count(left_list.len(), out_len, fn_name, "base")?;
    ensure_row_count(right_list.len(), out_len, fn_name, "target")?;

    let left_values = cast_list_values_to_f64(left_list, fn_name, "base")?;
    let right_values = cast_list_values_to_f64(right_list, fn_name, "target")?;
    let left_offsets = left_list.value_offsets();
    let right_offsets = right_list.value_offsets();

    let mut out = Vec::with_capacity(out_len);
    for row in 0..out_len {
        let left_row = row_index(row, left_list.len(), out_len);
        let right_row = row_index(row, right_list.len(), out_len);

        let left_start = left_offsets[left_row] as usize;
        let left_end = left_offsets[left_row + 1] as usize;
        let right_start = right_offsets[right_row] as usize;
        let right_end = right_offsets[right_row + 1] as usize;

        let left_dim = left_end - left_start;
        let right_dim = right_end - right_start;
        if left_dim != right_dim {
            return Err(format!(
                "{} requires equal length arrays in each row. base array dimension size is {}, target array dimension size is {}.",
                fn_name, left_dim, right_dim
            ));
        }
        if left_dim == 0 {
            return Err(format!("{} requires non-empty arrays in each row", fn_name));
        }

        let mut dot = 0.0_f64;
        let mut left_sq = 0.0_f64;
        let mut right_sq = 0.0_f64;
        for idx in 0..left_dim {
            let lv = left_values.value(left_start + idx);
            let rv = right_values.value(right_start + idx);
            dot += lv * rv;
            if !normalized {
                left_sq += lv * lv;
                right_sq += rv * rv;
            }
        }

        let value = if normalized {
            dot
        } else if left_sq == 0.0 || right_sq == 0.0 {
            0.0
        } else {
            dot / (left_sq.sqrt() * right_sq.sqrt())
        };
        out.push(Some(value));
    }

    cast_output(
        Arc::new(Float64Array::from(out)) as ArrayRef,
        arena.data_type(expr),
    )
}

pub fn eval_cosine_similarity(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_cosine_impl(arena, expr, args, chunk, false, "cosine_similarity")
}

pub fn eval_cosine_similarity_norm(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_cosine_impl(arena, expr, args, chunk, true, "cosine_similarity_norm")
}

pub fn eval_l2_distance(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_list = require_list_arg(&left, "l2_distance")?;
    let right_list = require_list_arg(&right, "l2_distance")?;

    if left_list.null_count() > 0 {
        return Err(
            "l2_distance does not support null values. base array has null value.".to_string(),
        );
    }
    if right_list.null_count() > 0 {
        return Err(
            "l2_distance does not support null values. target array has null value.".to_string(),
        );
    }

    let out_len = chunk.len();
    ensure_row_count(left_list.len(), out_len, "l2_distance", "base")?;
    ensure_row_count(right_list.len(), out_len, "l2_distance", "target")?;

    let left_values = cast_list_values_to_f64(left_list, "l2_distance", "base")?;
    let right_values = cast_list_values_to_f64(right_list, "l2_distance", "target")?;
    let left_offsets = left_list.value_offsets();
    let right_offsets = right_list.value_offsets();

    let mut out = Vec::with_capacity(out_len);
    for row in 0..out_len {
        let left_row = row_index(row, left_list.len(), out_len);
        let right_row = row_index(row, right_list.len(), out_len);

        let left_start = left_offsets[left_row] as usize;
        let left_end = left_offsets[left_row + 1] as usize;
        let right_start = right_offsets[right_row] as usize;
        let right_end = right_offsets[right_row + 1] as usize;

        let left_dim = left_end - left_start;
        let right_dim = right_end - right_start;
        if left_dim != right_dim {
            return Err(format!(
                "l2_distance requires equal length arrays in each row. base array dimension size is {}, target array dimension size is {}.",
                left_dim, right_dim
            ));
        }
        if left_dim == 0 {
            return Err("l2_distance requires non-empty arrays in each row".to_string());
        }

        let mut distance = 0.0_f64;
        for idx in 0..left_dim {
            let delta = left_values.value(left_start + idx) - right_values.value(right_start + idx);
            distance += delta * delta;
        }
        out.push(Some(distance));
    }

    cast_output(
        Arc::new(Float64Array::from(out)) as ArrayRef,
        arena.data_type(expr),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::function::math::dispatch::eval_math_function;
    use crate::exec::expr::function::math::test_utils::{chunk_len_1, typed_null};
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn float_array_literal(arena: &mut ExprArena, values: &[f32]) -> ExprId {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Float32, true)));
        let elements = values
            .iter()
            .map(|v| {
                arena.push_typed(
                    ExprNode::Literal(LiteralValue::Float32(*v)),
                    DataType::Float32,
                )
            })
            .collect::<Vec<_>>();
        arena.push_typed(ExprNode::ArrayExpr { elements }, list_type)
    }

    fn eval_single(
        name: &str,
        arena: &ExprArena,
        expr: ExprId,
        args: &[ExprId],
        chunk: &Chunk,
    ) -> f64 {
        let out = eval_math_function(name, arena, expr, args, chunk).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        out.value(0)
    }

    #[test]
    fn test_vector_math_values() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Float64);

        let a = float_array_literal(&mut arena, &[0.1, 0.2, 0.3]);
        let b = float_array_literal(&mut arena, &[0.2, 0.1, 0.3]);

        let cosine = eval_single("cosine_similarity", &arena, expr, &[a, b], &chunk);
        assert!((cosine - 0.92857142857).abs() < 1e-8);

        let cosine_norm = eval_single("cosine_similarity_norm", &arena, expr, &[a, a], &chunk);
        assert!((cosine_norm - 0.14).abs() < 1e-7);

        let l2 = eval_single("l2_distance", &arena, expr, &[a, b], &chunk);
        assert!((l2 - 0.02).abs() < 1e-8);
    }

    #[test]
    fn test_vector_math_empty_array_rejected() {
        let mut arena = ExprArena::default();
        let chunk = chunk_len_1();
        let expr = typed_null(&mut arena, DataType::Float64);
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Float32, true)));
        let empty = arena.push_typed(ExprNode::ArrayExpr { elements: vec![] }, list_type.clone());

        let err =
            eval_math_function("l2_distance", &arena, expr, &[empty, empty], &chunk).unwrap_err();
        assert!(err.contains("requires non-empty arrays"));
    }
}
