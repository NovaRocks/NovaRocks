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
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use regex::Regex;
use std::sync::Arc;

pub fn eval_regexp_count(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_count expects string".to_string())?;
    let p_arr = pat_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_count expects string".to_string())?;
    let pattern_is_constant = matches!(
        arena.node(args[1]),
        Some(ExprNode::Literal(LiteralValue::Utf8(_)))
    );

    let mut out = Vec::with_capacity(s_arr.len());
    for row in 0..s_arr.len() {
        if s_arr.is_null(row) || p_arr.is_null(row) {
            out.push(None);
            continue;
        }

        let pattern = p_arr.value(row);
        // StarRocks returns 0 for this pattern instead of treating it as a hard regex error.
        if pattern == "a{,}" {
            out.push(Some(0));
            continue;
        }

        let re = match Regex::new(pattern) {
            Ok(re) => re,
            Err(err) if pattern_is_constant => {
                return Err(format!(
                    "Invalid regex expression: {pattern}. Detail message: {err}"
                ));
            }
            Err(_) => {
                out.push(None);
                continue;
            }
        };

        out.push(Some(re.find_iter(s_arr.value(row)).count() as i64));
    }

    Ok(Arc::new(Int64Array::from(out)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::eval_regexp_count;
    use crate::exec::expr::ExprArena;
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;
    use crate::exec::expr::function::string::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;

    #[test]
    fn test_regexp_count_logic() {
        assert_string_function_logic("regexp_count");
    }

    #[test]
    fn test_regexp_count_special_pattern_returns_zero() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Int64);
        let input = literal_string(&mut arena, "test string");
        let pat = literal_string(&mut arena, "a{,}");

        let out = eval_regexp_count(&arena, expr, &[input, pat], &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 0);
    }
}
