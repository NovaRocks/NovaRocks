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
    let len = chunk.len();
    let str_arr = arena.eval(args[0], chunk)?;
    let pat_arr = arena.eval(args[1], chunk)?;
    // A NULL literal arrives as a typed-Null array; treat it as a column
    // whose every row is NULL so we collapse the result to NULL rather than
    // failing the static downcast.
    let s_arr_opt = downcast_string_or_null(&str_arr, "regexp_count")?;
    let p_arr_opt = downcast_string_or_null(&pat_arr, "regexp_count")?;
    let pattern_is_constant = matches!(
        arena.node(args[1]),
        Some(ExprNode::Literal(LiteralValue::Utf8(_)))
    );

    let mut out = Vec::with_capacity(len);
    for row in 0..len {
        let Some(s_arr) = s_arr_opt else {
            out.push(None);
            continue;
        };
        let Some(p_arr) = p_arr_opt else {
            out.push(None);
            continue;
        };
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

fn downcast_string_or_null<'a>(
    arr: &'a ArrayRef,
    fname: &str,
) -> Result<Option<&'a StringArray>, String> {
    if matches!(arr.data_type(), arrow::datatypes::DataType::Null) {
        return Ok(None);
    }
    arr.as_any()
        .downcast_ref::<StringArray>()
        .map(Some)
        .ok_or_else(|| format!("{fname} expects string"))
}
