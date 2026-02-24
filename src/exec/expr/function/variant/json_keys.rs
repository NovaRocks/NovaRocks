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
use arrow::array::{Array, ArrayRef, ListBuilder, StringArray, StringBuilder};
use serde_json::Value as JsonValue;
use std::sync::Arc;

pub fn eval_json_keys(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.len() != 1 {
        return Err(format!("json_keys expects 1 argument, got {}", args.len()));
    }

    let input = arena.eval(args[0], chunk)?;
    let arr = input
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "json_keys expects VARCHAR input".to_string())?;

    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in 0..chunk.len() {
        if arr.is_null(row) {
            builder.append(false);
            continue;
        }
        let parsed: JsonValue = match serde_json::from_str(arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append(false);
                continue;
            }
        };
        let JsonValue::Object(obj) = parsed else {
            builder.append(false);
            continue;
        };
        for key in obj.keys() {
            builder.values().append_value(key);
        }
        builder.append(true);
    }

    let mut out = Arc::new(builder.finish()) as ArrayRef;
    if let Some(target_type) = arena.data_type(expr) {
        if out.data_type() != target_type {
            out = crate::exec::expr::cast::cast_with_special_rules(&out, target_type).map_err(
                |e| {
                    format!(
                        "json_keys failed to cast output {:?} -> {:?}: {}",
                        out.data_type(),
                        target_type,
                        e
                    )
                },
            )?;
        }
    }
    Ok(out)
}
