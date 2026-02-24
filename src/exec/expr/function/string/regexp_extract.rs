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

pub fn eval_regexp_extract(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    let idx_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_extract expects string".to_string())?;
    let p_arr = pat_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_extract expects string".to_string())?;
    let idx_arr = idx_arr
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .ok_or_else(|| "regexp_extract expects int".to_string())?;
    let len = s_arr.len();
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if s_arr.is_null(i) || p_arr.is_null(i) || idx_arr.is_null(i) {
            out.push(None);
            continue;
        }
        let re = Regex::new(p_arr.value(i)).map_err(|e| e.to_string())?;
        let idx = idx_arr.value(i) as usize;
        let caps = re.captures(s_arr.value(i));
        let val = caps
            .and_then(|c| c.get(idx))
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        out.push(Some(val));
    }
    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use super::eval_regexp_extract;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;
    use crate::exec::expr::function::string::test_utils::{
        chunk_len_1, literal_i64, literal_string, typed_null,
    };
    use arrow::array::{Array, StringArray};
    use arrow::datatypes::DataType;

    #[test]
    fn test_regexp_extract_logic() {
        assert_string_function_logic("regexp_extract");
    }

    #[test]
    fn test_regexp_extract_no_match_returns_empty_string() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Utf8);
        let input = literal_string(&mut arena, "foo=123");
        let pat = literal_string(&mut arena, "bar=([0-9]+)");
        let idx = literal_i64(&mut arena, 1);

        let out = eval_regexp_extract(&arena, expr, &[input, pat, idx], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!out.is_null(0));
        assert_eq!(out.value(0), "");
    }
}
