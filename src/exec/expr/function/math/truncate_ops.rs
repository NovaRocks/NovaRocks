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
use super::common::{NumericArrayView, cast_output, value_at_f64, value_at_i64};
use super::unary_ops::eval_unary_f64;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Float64Array};
use std::sync::Arc;

fn finite_or_null(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

fn eval_truncate_impl(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() == 1 {
        return eval_unary_f64(arena, expr, args, chunk, |v| v.trunc());
    }
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_view = NumericArrayView::new(&left)?;
    let right_view = NumericArrayView::new(&right)?;
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    for row in 0..len {
        let v = value_at_f64(&left_view, row, len);
        let d = value_at_i64(&right_view, row, len);
        let out = match (v, d) {
            (Some(x), Some(dec)) => {
                if dec >= 0 {
                    let factor = 10_f64.powi(dec as i32);
                    finite_or_null((x * factor).trunc() / factor)
                } else {
                    let factor = 10_f64.powi((-dec) as i32);
                    finite_or_null((x / factor).trunc() * factor)
                }
            }
            _ => None,
        };
        values.push(out);
    }
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    cast_output(out, arena.data_type(expr))
}

pub fn eval_truncate(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_truncate_impl(arena, expr, args, chunk)
}

pub fn eval_dround(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() == 1 {
        return eval_unary_f64(arena, expr, args, chunk, |v| v.round());
    }
    eval_truncate_impl(arena, expr, args, chunk)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::math::test_utils::assert_math_function_logic;

    #[test]
    fn test_truncate_logic() {
        assert_math_function_logic("truncate");
    }

    #[test]
    fn test_dround_logic() {
        assert_math_function_logic("dround");
    }
}
