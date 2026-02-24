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
use super::common::{NumericArrayView, cast_output, value_at_i64};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Float64Array};
use rand::prelude::*;
use std::sync::Arc;

fn eval_rand_impl(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    if args.is_empty() {
        let mut rng = rand::thread_rng();
        for _ in 0..len {
            values.push(Some(rng.r#gen::<f64>()));
        }
    } else {
        let seed_arr = arena.eval(args[0], chunk)?;
        let seed_view = NumericArrayView::new(&seed_arr)?;
        let seed = value_at_i64(&seed_view, 0, len).unwrap_or(0) as u64;
        let mut rng = StdRng::seed_from_u64(seed);
        for _ in 0..len {
            values.push(Some(rng.r#gen::<f64>()));
        }
    }
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    cast_output(out, arena.data_type(expr))
}

pub fn eval_rand(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_rand_impl(arena, expr, args, chunk)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::math::test_utils::assert_math_function_logic;

    #[test]
    fn test_rand_logic() {
        assert_math_function_logic("rand");
    }

    #[test]
    fn test_random_logic() {
        assert_math_function_logic("random");
    }
}
