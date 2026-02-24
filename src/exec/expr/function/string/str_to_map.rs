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
use arrow::array::{Array, ArrayRef, MapBuilder, StringArray, StringBuilder};
use std::sync::Arc;

pub fn eval_str_to_map(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let str_arr = arena.eval(args[0], chunk)?;
    let entry_delim_arr = arena.eval(args[1], chunk)?;
    let kv_delim_arr = arena.eval(args[2], chunk)?;
    let s_arr = str_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "str_to_map expects string".to_string())?;
    let e_arr = entry_delim_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "str_to_map expects string".to_string())?;
    let kv_arr = kv_delim_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "str_to_map expects string".to_string())?;

    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    let len = s_arr.len();
    for i in 0..len {
        if s_arr.is_null(i) || e_arr.is_null(i) || kv_arr.is_null(i) {
            builder.append(false).map_err(|e| e.to_string())?;
            continue;
        }
        let s = s_arr.value(i);
        let entry_delim = e_arr.value(i);
        let kv_delim = kv_arr.value(i);
        let entries: Vec<&str> = s.split(entry_delim).filter(|e| !e.is_empty()).collect();
        if entries.is_empty() {
            builder.append(true).map_err(|e| e.to_string())?;
            continue;
        }
        for entry in entries {
            let mut parts = entry.splitn(2, kv_delim);
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).map_err(|e| e.to_string())?;
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
#[cfg(test)]
mod tests {
    use crate::exec::expr::function::string::test_utils::assert_string_function_logic;

    #[test]
    fn test_str_to_map_logic() {
        assert_string_function_logic("str_to_map");
    }
}
