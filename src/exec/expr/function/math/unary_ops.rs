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
use super::common::{NumericArrayView, cast_output, value_at_f64};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Float64Array};
use std::sync::Arc;

fn finite_or_null(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

pub(super) fn eval_unary_f64<F>(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, String>
where
    F: Fn(f64) -> f64,
{
    let array = arena.eval(args[0], chunk)?;
    let view = NumericArrayView::new(&array)?;
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    for row in 0..len {
        let v = value_at_f64(&view, row, len);
        values.push(v.and_then(|x| finite_or_null(func(x))));
    }
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    cast_output(out, arena.data_type(expr))
}

pub fn eval_acos(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.acos())
}

pub fn eval_asin(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.asin())
}

pub fn eval_atan(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.atan())
}

pub fn eval_ceil(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.ceil())
}

pub fn eval_cos(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.cos())
}

pub fn eval_cbrt(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.cbrt())
}

pub fn eval_cot(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| 1.0 / v.tan())
}

pub fn eval_degress(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.to_degrees())
}

pub fn eval_dlog1(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| (1.0 + v).ln())
}

pub fn eval_exp(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.exp())
}

pub fn eval_floor(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.floor())
}

pub fn eval_ln(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.ln())
}

pub fn eval_log10(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.log10())
}

pub fn eval_log2(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.log2())
}

pub fn eval_radians(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.to_radians())
}

pub fn eval_positive(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let out = arena.eval(args[0], chunk)?;
    cast_output(out, arena.data_type(expr))
}

pub fn eval_sin(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.sin())
}

pub fn eval_sqrt(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.sqrt())
}

pub fn eval_square(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v * v)
}

pub fn eval_tan(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_unary_f64(arena, expr, args, chunk, |v| v.tan())
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::math::test_utils::assert_math_function_logic;

    #[test]
    fn test_unary_math_logic() {
        for name in [
            "acos", "asin", "atan", "ceil", "ceiling", "dceil", "cos", "cot", "degress", "dexp",
            "dlog1", "dlog10", "exp", "floor", "dfloor", "ln", "log10", "log2", "radians", "sin",
            "sqrt", "dsqrt", "square", "tan", "cbrt", "positive",
        ] {
            assert_math_function_logic(name);
        }
    }
}
