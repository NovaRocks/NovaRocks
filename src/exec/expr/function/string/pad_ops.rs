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

fn eval_pad_impl(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    left: bool,
) -> Result<ArrayRef, String> {
    let str_arr = arena.eval(args[0], chunk)?;
    let len_arr = arena.eval(args[1], chunk)?;
    let pad_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "pad expects string".to_string())?;
    let len_arr = super::common::downcast_int_arg_array(&len_arr, "pad")?;
    let p_arr = pad_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "pad expects string".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || len_arr.is_null(i) || p_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let target_len = len_arr.value(i);
        if target_len < 0 {
            out.push(None);
            continue;
        }
        let target_len = target_len as usize;
        if target_len > super::common::OLAP_STRING_MAX_LENGTH {
            out.push(None);
            continue;
        }
        let pad = p_arr.value(i);
        let source_chars: Vec<char> = s.chars().collect();
        let source_len = source_chars.len();

        let result = if target_len <= source_len || pad.is_empty() {
            source_chars.iter().take(target_len).collect::<String>()
        } else {
            let pad_chars: Vec<char> = pad.chars().collect();
            let needed = target_len - source_len;

            // Build exactly the required number of fill characters in one pass.
            let mut fill = String::new();
            for idx in 0..needed {
                fill.push(pad_chars[idx % pad_chars.len()]);
            }

            if left {
                let mut composed = String::with_capacity(fill.len() + s.len());
                composed.push_str(&fill);
                composed.push_str(s);
                composed
            } else {
                let mut composed = String::with_capacity(s.len() + fill.len());
                composed.push_str(s);
                composed.push_str(&fill);
                composed
            }
        };

        if result.len() > super::common::OLAP_STRING_MAX_LENGTH {
            out.push(None);
        } else {
            out.push(Some(result));
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}

pub fn eval_lpad(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_pad_impl(arena, args, chunk, true)
}

pub fn eval_rpad(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    eval_pad_impl(arena, args, chunk, false)
}

#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_lpad_logic() {
        assert_string_function_logic("lpad");
    }

    #[test]
    fn test_rpad_logic() {
        assert_string_function_logic("rpad");
    }
}
