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

use super::common::downcast_int_arg_array;

pub fn eval_substring_index(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let delim_arr = arena.eval(args[1], chunk)?;
    let count_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "substring_index expects string".to_string())?;
    let d_arr = delim_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "substring_index expects string".to_string())?;
    let c_arr = downcast_int_arg_array(&count_arr, "substring_index")?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || d_arr.is_null(i) || c_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let delim = d_arr.value(i);
        let count = c_arr.value(i);
        if delim.is_empty() || count == 0 {
            out.push(None);
            continue;
        }
        out.push(Some(substring_index_impl(s, delim, count)));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

fn substring_index_impl(s: &str, delim: &str, count: i64) -> String {
    let positions: Vec<usize> = s.match_indices(delim).map(|(idx, _)| idx).collect();
    if positions.is_empty() {
        return s.to_string();
    }

    if count > 0 {
        let n = count as usize;
        if n > positions.len() {
            return s.to_string();
        }
        let cut = positions[n - 1];
        s[..cut].to_string()
    } else {
        let n = (-count) as usize;
        if n > positions.len() {
            return s.to_string();
        }
        let pos = positions[positions.len() - n];
        s[pos + delim.len()..].to_string()
    }
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_substring_index_logic() {
        assert_string_function_logic("substring_index");
    }
}
