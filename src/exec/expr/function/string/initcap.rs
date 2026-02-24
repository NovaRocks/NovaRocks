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

pub fn eval_initcap(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let input = arena.eval(args[0], chunk)?;
    let arr = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "initcap expects string".to_string())?;
    let len = arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if arr.is_null(i) {
            out.push(None);
            continue;
        }
        let s = arr.value(i);
        let mut result = String::new();
        let mut new_word = true;
        for ch in s.chars() {
            if ch.is_whitespace() {
                new_word = true;
                result.push(ch);
            } else if new_word {
                for up in ch.to_uppercase() {
                    result.push(up);
                }
                new_word = false;
            } else {
                for lo in ch.to_lowercase() {
                    result.push(lo);
                }
            }
        }
        out.push(Some(result));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_initcap_logic() {
        assert_string_function_logic("initcap");
    }
}
