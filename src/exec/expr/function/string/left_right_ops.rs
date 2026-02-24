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
use arrow::array::{Array, ArrayRef, StringArray};
use std::sync::Arc;

fn eval_left_right_impl(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    left: bool,
) -> Result<ArrayRef, String> {
    let str_arr = arena.eval(args[0], chunk)?;
    let len_arr = arena.eval(args[1], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "left expects string".to_string())?;
    let len_arr = super::common::downcast_int_arg_array(&len_arr, "left")?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || len_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let n = len_arr.value(i);
        if n <= 0 {
            out.push(Some(String::new()));
            continue;
        }
        let n = n as usize;
        let result = if left {
            s.chars().take(n).collect::<String>()
        } else {
            s.chars()
                .rev()
                .take(n)
                .collect::<String>()
                .chars()
                .rev()
                .collect()
        };
        out.push(Some(result));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_left(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_left_right_impl(arena, args, chunk, true)
}

pub fn eval_right(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_left_right_impl(arena, args, chunk, false)
}

pub fn eval_strleft(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_left_right_impl(arena, args, chunk, true)
}

pub fn eval_strright(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_left_right_impl(arena, args, chunk, false)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_left_logic() {
        assert_string_function_logic("left");
    }

    #[test]
    fn test_right_logic() {
        assert_string_function_logic("right");
    }

    #[test]
    fn test_strleft_logic() {
        assert_string_function_logic("strleft");
    }

    #[test]
    fn test_strright_logic() {
        assert_string_function_logic("strright");
    }
}
