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

enum TrimMode {
    Left,
    Right,
    Both,
}

fn eval_trim_impl(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    mode: TrimMode,
) -> Result<ArrayRef, String> {
    let str_arr = arena.eval(args[0], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "trim expects string".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let v = match mode {
            TrimMode::Left => s.trim_start().to_string(),
            TrimMode::Right => s.trim_end().to_string(),
            TrimMode::Both => s.trim().to_string(),
        };
        out.push(Some(v));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_ltrim(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_trim_impl(arena, args, chunk, TrimMode::Left)
}

pub fn eval_rtrim(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_trim_impl(arena, args, chunk, TrimMode::Right)
}

pub fn eval_trim(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_trim_impl(arena, args, chunk, TrimMode::Both)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_ltrim_logic() {
        assert_string_function_logic("ltrim");
    }

    #[test]
    fn test_rtrim_logic() {
        assert_string_function_logic("rtrim");
    }

    #[test]
    fn test_trim_logic() {
        assert_string_function_logic("trim");
    }
}
