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
use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use serde_json::Value as JsonValue;
use std::sync::Arc;

fn format_json_value(value: &JsonValue, out: &mut String) -> Result<(), String> {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonValue::Number(v) => out.push_str(&v.to_string()),
        JsonValue::String(v) => {
            let escaped = serde_json::to_string(v)
                .map_err(|e| format!("parse_json stringify failed: {e}"))?;
            out.push_str(&escaped);
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                format_json_value(item, out)?;
            }
            out.push(']');
        }
        JsonValue::Object(map) => {
            out.push('{');
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (idx, key) in keys.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                let escaped_key = serde_json::to_string(key)
                    .map_err(|e| format!("parse_json stringify key failed: {e}"))?;
                out.push_str(&escaped_key);
                out.push_str(": ");
                let child = map
                    .get(*key)
                    .ok_or_else(|| "parse_json missing object key".to_string())?;
                format_json_value(child, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn normalize_json_text(raw: &str) -> Result<String, String> {
    let value: JsonValue =
        serde_json::from_str(raw).map_err(|e| format!("parse_json invalid input: {e}"))?;
    let mut out = String::new();
    format_json_value(&value, &mut out)?;
    Ok(out)
}

pub fn eval_parse_json(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!("parse_json expects 1 argument, got {}", args.len()));
    }
    let input = arena.eval(args[0], chunk)?;
    let arr = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "parse_json expects VARCHAR input".to_string())?;

    let mut builder = StringBuilder::new();
    for row in 0..chunk.len() {
        if arr.is_null(row) {
            builder.append_null();
            continue;
        }
        match normalize_json_text(arr.value(row)) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}
