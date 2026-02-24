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
use super::common::{NumericArrayView, value_at_f64};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array};
use std::sync::Arc;

pub fn eval_sign(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let array = arena.eval(args[0], chunk)?;
    let view = NumericArrayView::new(&array)?;
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    for row in 0..len {
        let v = value_at_f64(&view, row, len);
        let out = v.map(|x| {
            if x > 0.0 {
                1
            } else if x < 0.0 {
                -1
            } else {
                0
            }
        });
        values.push(out);
    }
    let out = Arc::new(Int64Array::from(values)) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr))
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::math::test_utils::assert_math_function_logic;

    #[test]
    fn test_sign_logic() {
        assert_math_function_logic("sign");
    }
}
