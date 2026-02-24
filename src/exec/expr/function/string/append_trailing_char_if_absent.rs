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

pub fn eval_append_trailing_char_if_absent(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let ch_arr = arena.eval(args[1], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "append_trailing_char_if_absent expects string".to_string())?;
    let c_arr = ch_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "append_trailing_char_if_absent expects string".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || c_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = s_arr.value(i);
        let ch = c_arr.value(i);
        if ch.len() != 1 {
            out.push(None);
            continue;
        }
        let trailing = ch.as_bytes()[0];
        let s_bytes = s.as_bytes();
        if s_bytes.is_empty() || s_bytes[s_bytes.len() - 1] == trailing {
            out.push(Some(s.to_string()));
        } else {
            let mut v = String::with_capacity(s.len() + 1);
            v.push_str(s);
            v.push_str(ch);
            out.push(Some(v));
        }
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_append_trailing_char_if_absent_logic() {
        assert_string_function_logic("append_trailing_char_if_absent");
    }
}
