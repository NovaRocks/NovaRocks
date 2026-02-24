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
use arrow::array::ArrayRef;
use arrow::array::StringArray;
use std::sync::Arc;

use super::common::{IntArgArray, downcast_int_arg_array};

const BAR_CHAR: &str = "\u{2593}";

fn const_i64_arg(arr: IntArgArray<'_>, name: &str) -> Result<i64, String> {
    if arr.len() == 0 {
        return Err(format!("bar: {name} must be constant"));
    }
    if arr.is_null(0) {
        return Err(format!("bar: {name} must be constant"));
    }
    let first = arr.value(0);
    for row in 1..arr.len() {
        if arr.is_null(row) || arr.value(row) != first {
            return Err(format!("bar: {name} must be constant"));
        }
    }
    Ok(first)
}

pub fn eval_bar(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let size_eval = arena.eval(args[0], chunk)?;
    let min_eval = arena.eval(args[1], chunk)?;
    let max_eval = arena.eval(args[2], chunk)?;
    let width_eval = arena.eval(args[3], chunk)?;

    let size_arr = downcast_int_arg_array(&size_eval, "bar")?;
    let min_arr = downcast_int_arg_array(&min_eval, "bar")?;
    let max_arr = downcast_int_arg_array(&max_eval, "bar")?;
    let width_arr = downcast_int_arg_array(&width_eval, "bar")?;

    let min = const_i64_arg(min_arr, "argument[min]")?;
    let max = const_i64_arg(max_arr, "argument[max]")?;
    let width = const_i64_arg(width_arr, "argument[width]")?;

    if min >= max {
        return Err("bar requirement: min < max".to_string());
    }
    if width <= 0 {
        return Err("bar requirement: width > 0".to_string());
    }

    let mut out = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if size_arr.is_null(row) {
            out.push(None);
            continue;
        }

        let size = size_arr.value(row);
        if size < min {
            return Err("bar requirement: size >= min".to_string());
        }
        if size > max {
            return Err("bar requirement: size <= max".to_string());
        }

        let ratio = ((size - min) as f64 / (max - min) as f64).min(1.0);
        let bar_width = (ratio * width as f64) as usize;
        out.push(Some(BAR_CHAR.repeat(bar_width)));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_bar_logic() {
        assert_string_function_logic("bar");
    }
}
