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

use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder};
use chrono::NaiveDate;
use serde_json::Value as JsonValue;

use crate::exec::chunk::Chunk;
use crate::exec::expr::agg::{AggScalarValue, agg_scalar_from_array};
use crate::exec::expr::{ExprArena, ExprId};

const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;

fn format_date32(days: i32) -> Result<String, String> {
    let naive = NaiveDate::from_num_days_from_ce_opt(UNIX_EPOCH_DAY_OFFSET + days)
        .ok_or_else(|| format!("json_object invalid date32 day value: {}", days))?;
    Ok(naive.format("%Y-%m-%d").to_string())
}

fn json_key_from_scalar(value: &AggScalarValue) -> Result<String, String> {
    match value {
        AggScalarValue::Bool(v) => Ok(if *v { "true" } else { "false" }.to_string()),
        AggScalarValue::Int64(v) => Ok(v.to_string()),
        AggScalarValue::Float64(v) => Ok(v.to_string()),
        AggScalarValue::Utf8(v) => Ok(v.clone()),
        AggScalarValue::Date32(v) => format_date32(*v),
        AggScalarValue::Timestamp(v) => Ok(v.to_string()),
        AggScalarValue::Decimal128(v) => Ok(v.to_string()),
        AggScalarValue::Decimal256(v) => Ok(v.to_string()),
        AggScalarValue::Struct(_) | AggScalarValue::Map(_) | AggScalarValue::List(_) => {
            Err("json_object key does not support complex type".to_string())
        }
    }
}

fn format_json_value(value: &JsonValue, out: &mut String) -> Result<(), String> {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonValue::Number(v) => out.push_str(&v.to_string()),
        JsonValue::String(v) => {
            let escaped = serde_json::to_string(v)
                .map_err(|e| format!("json_object stringify failed: {e}"))?;
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
                    .map_err(|e| format!("json_object stringify key failed: {e}"))?;
                out.push_str(&escaped_key);
                out.push_str(": ");
                let child = map
                    .get(*key)
                    .ok_or_else(|| "json_object missing object key".to_string())?;
                format_json_value(child, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn normalize_json_value_text(raw: &str) -> Option<String> {
    let parsed = serde_json::from_str::<JsonValue>(raw).ok()?;
    let mut out = String::new();
    format_json_value(&parsed, &mut out).ok()?;
    Some(out)
}

fn json_value_from_scalar(value: &AggScalarValue) -> Result<String, String> {
    match value {
        AggScalarValue::Bool(v) => Ok(if *v { "true" } else { "false" }.to_string()),
        AggScalarValue::Int64(v) => Ok(v.to_string()),
        AggScalarValue::Float64(v) => Ok(if v.is_finite() {
            v.to_string()
        } else {
            "null".to_string()
        }),
        AggScalarValue::Utf8(v) => {
            if let Some(normalized) = normalize_json_value_text(v) {
                Ok(normalized)
            } else {
                serde_json::to_string(v).map_err(|e| e.to_string())
            }
        }
        AggScalarValue::Date32(v) => {
            let date = format_date32(*v)?;
            serde_json::to_string(&date).map_err(|e| e.to_string())
        }
        AggScalarValue::Timestamp(v) => {
            serde_json::to_string(&v.to_string()).map_err(|e| e.to_string())
        }
        AggScalarValue::Decimal128(v) => Ok(v.to_string()),
        AggScalarValue::Decimal256(v) => Ok(v.to_string()),
        AggScalarValue::Struct(v) => {
            serde_json::to_string(&format!("{:?}", v)).map_err(|e| e.to_string())
        }
        AggScalarValue::Map(v) => {
            serde_json::to_string(&format!("{:?}", v)).map_err(|e| e.to_string())
        }
        AggScalarValue::List(v) => {
            serde_json::to_string(&format!("{:?}", v)).map_err(|e| e.to_string())
        }
    }
}

pub fn eval_json_object(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("json_object expects at least 1 argument".to_string());
    }

    let mut arg_arrays: Vec<ArrayRef> = Vec::with_capacity(args.len());
    for arg in args {
        arg_arrays.push(arena.eval(*arg, chunk)?);
    }

    let mut builder = StringBuilder::new();
    for row in 0..chunk.len() {
        let mut object = String::from("{");
        let mut first_pair = true;
        let mut key_idx = 0usize;
        let mut row_is_null = false;

        while key_idx < arg_arrays.len() {
            let key_value = agg_scalar_from_array(&arg_arrays[key_idx], row)?;
            let Some(key_scalar) = key_value.as_ref() else {
                row_is_null = true;
                break;
            };
            let key = json_key_from_scalar(key_scalar)?;
            if !first_pair {
                object.push(',');
            }
            first_pair = false;
            object.push_str(&serde_json::to_string(&key).map_err(|e| e.to_string())?);
            object.push_str(": ");

            let value_idx = key_idx + 1;
            if value_idx >= arg_arrays.len() {
                object.push_str("null");
            } else {
                let value = agg_scalar_from_array(&arg_arrays[value_idx], row)?;
                if let Some(value_scalar) = value.as_ref() {
                    object.push_str(&json_value_from_scalar(value_scalar)?);
                } else {
                    object.push_str("null");
                }
            }

            key_idx += 2;
        }

        if row_is_null {
            builder.append_null();
            continue;
        }
        object.push('}');
        builder.append_value(object);
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::{AggScalarValue, json_value_from_scalar};

    #[test]
    fn test_json_value_from_utf8_json_number() {
        let out = json_value_from_scalar(&AggScalarValue::Utf8("23".to_string())).unwrap();
        assert_eq!(out, "23");
    }

    #[test]
    fn test_json_value_from_utf8_plain_string() {
        let out = json_value_from_scalar(&AggScalarValue::Utf8("abc".to_string())).unwrap();
        assert_eq!(out, "\"abc\"");
    }
}
