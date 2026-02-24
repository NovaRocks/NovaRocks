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
use regex::Regex;
use std::sync::Arc;

pub fn eval_regexp_replace(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    let rep_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_replace expects string".to_string())?;
    let p_arr = pat_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_replace expects string".to_string())?;
    let r_arr = rep_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_replace expects string".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || p_arr.is_null(i) || r_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let re = Regex::new(p_arr.value(i)).map_err(|e| e.to_string())?;
        let val = re.replace_all(s_arr.value(i), r_arr.value(i)).to_string();
        out.push(Some(val));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_regexp_replace_logic() {
        assert_string_function_logic("regexp_replace");
    }
}
