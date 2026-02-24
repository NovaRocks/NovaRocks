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
use arrow::array::{ArrayRef, Float64Array};
use std::sync::Arc;

fn finite_or_null(value: f64) -> Option<f64> {
    value.is_finite().then_some(value)
}

fn eval_const_f64(
    len: usize,
    value: f64,
    output_type: Option<&arrow::datatypes::DataType>,
) -> Result<ArrayRef, String> {
    let values = vec![finite_or_null(value); len];
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    cast_output(out, output_type)
}

pub fn eval_e(
    arena: &ExprArena,
    expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_const_f64(chunk.len(), std::f64::consts::E, arena.data_type(expr))
}

pub fn eval_pi(
    arena: &ExprArena,
    expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_const_f64(chunk.len(), std::f64::consts::PI, arena.data_type(expr))
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::math::test_utils::assert_math_function_logic;

    #[test]
    fn test_const_logic() {
        assert_math_function_logic("e");
        assert_math_function_logic("pi");
    }
}
