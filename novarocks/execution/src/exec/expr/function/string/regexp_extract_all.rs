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

pub fn eval_regexp_extract_all(
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
        .ok_or_else(|| "regexp_extract_all expects string".to_string())?;
    let p_arr = pat_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "regexp_extract_all expects string".to_string())?;
    let idx_arr = super::common::downcast_int_arg_array(&idx_arr, "regexp_extract_all")?;

    let mut out = Vec::with_capacity(s_arr.len());
    for row in 0..s_arr.len() {
        if s_arr.is_null(row) || p_arr.is_null(row) || idx_arr.is_null(row) {
            out.push(None);
            continue;
        }

        let group_idx = idx_arr.value(row);
        if group_idx < 0 {
            out.push(Some("[]".to_string()));
            continue;
        }
        let group_idx = group_idx as usize;

        let re = Regex::new(p_arr.value(row)).map_err(|e| e.to_string())?;
        let mut matches = Vec::new();
        for caps in re.captures_iter(s_arr.value(row)) {
            if let Some(matched) = caps.get(group_idx) {
                matches.push(matched.as_str().to_string());
            }
        }

        let json = serde_json::to_string(&matches).map_err(|e| e.to_string())?;
        out.push(Some(json));
    }

    Ok(Arc::new(StringArray::from(out)) as ArrayRef)
}
