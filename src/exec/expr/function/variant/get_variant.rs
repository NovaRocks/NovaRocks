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

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, LargeBinaryArray,
    LargeBinaryBuilder, StringArray, StringBuilder, TimestampMicrosecondBuilder,
};
use chrono::{Local, Offset};
use serde_json::Value as JsonValue;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::variant::{
    VariantValue, is_variant_null, parse_variant_path, variant_query, variant_to_bool,
    variant_to_date_days, variant_to_datetime_micros, variant_to_f64, variant_to_i64,
    variant_to_string, variant_to_time_micros,
};

fn eval_variant_path_args(
    arena: &ExprArena,
    args: &[ExprId],
    chunk: &Chunk,
    fn_name: &str,
) -> Result<(ArrayRef, ArrayRef), String> {
    if args.len() != 2 {
        return Err(format!(
            "{} expects 2 arguments, got {}",
            fn_name,
            args.len()
        ));
    }
    let variant_array = arena.eval(args[0], chunk)?;
    let path_array = arena.eval(args[1], chunk)?;
    Ok((variant_array, path_array))
}

fn downcast_variant_path_args<'a>(
    variant_array: &'a ArrayRef,
    path_array: &'a ArrayRef,
    fn_name: &str,
) -> Result<(&'a LargeBinaryArray, &'a StringArray), String> {
    let variant_arr = variant_array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| format!("{} expects LargeBinary for variant argument", fn_name))?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| format!("{} expects Utf8 for path argument", fn_name))?;
    Ok((variant_arr, path_arr))
}

fn resolve_variant_value(
    variant_arr: &LargeBinaryArray,
    path_arr: &StringArray,
    row: usize,
) -> Option<VariantValue> {
    if variant_arr.is_null(row) || path_arr.is_null(row) {
        return None;
    }
    let path = parse_variant_path(path_arr.value(row)).ok()?;
    VariantValue::from_serialized(variant_arr.value(row))
        .and_then(|v| variant_query(&v, &path))
        .ok()
}

fn query_json_by_path<'a>(
    json: &'a JsonValue,
    path: &[crate::exec::variant::VariantPathSegment],
) -> Option<&'a JsonValue> {
    let mut current = json;
    for segment in path {
        current = match segment {
            crate::exec::variant::VariantPathSegment::ObjectKey(key) => current.get(key)?,
            crate::exec::variant::VariantPathSegment::ArrayIndex(idx) => {
                current.get(*idx as usize)?
            }
        };
    }
    Some(current)
}

fn json_value_to_output_string(value: &JsonValue) -> Option<String> {
    if value.is_null() {
        None
    } else if let Some(s) = value.as_str() {
        Some(s.to_string())
    } else {
        Some(value.to_string())
    }
}

fn json_value_to_i64(value: &JsonValue) -> Option<i64> {
    if value.is_null() {
        return None;
    }
    if let Some(v) = value.as_i64() {
        return Some(v);
    }
    if let Some(v) = value.as_u64() {
        return i64::try_from(v).ok();
    }
    if let Some(v) = value.as_f64() {
        if v.is_finite() && v.fract() == 0.0 {
            let as_i128 = v as i128;
            if as_i128 >= i64::MIN as i128 && as_i128 <= i64::MAX as i128 {
                return Some(as_i128 as i64);
            }
        }
        return None;
    }
    if let Some(v) = value.as_str() {
        return v.trim().parse::<i64>().ok();
    }
    None
}

fn format_json_query_value(value: &JsonValue, out: &mut String) -> Result<(), String> {
    match value {
        JsonValue::Null => out.push_str("null"),
        JsonValue::Bool(v) => out.push_str(if *v { "true" } else { "false" }),
        JsonValue::Number(v) => out.push_str(&v.to_string()),
        JsonValue::String(v) => {
            let escaped = serde_json::to_string(v).map_err(|e| e.to_string())?;
            out.push_str(&escaped);
        }
        JsonValue::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push_str(", ");
                }
                format_json_query_value(item, out)?;
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
                let escaped_key = serde_json::to_string(key).map_err(|e| e.to_string())?;
                out.push_str(&escaped_key);
                out.push_str(": ");
                let child = map
                    .get(*key)
                    .ok_or_else(|| "json_query missing object key".to_string())?;
                format_json_query_value(child, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

fn json_value_to_query_string(value: &JsonValue) -> Option<String> {
    if value.is_null() {
        return None;
    }
    let mut out = String::new();
    format_json_query_value(value, &mut out).ok()?;
    Some(out)
}

fn variant_value_to_query_string(value: &VariantValue) -> Option<String> {
    if is_variant_null(value).unwrap_or(true) {
        return None;
    }
    let text = value.to_json_local().ok()?;
    let json = serde_json::from_str::<JsonValue>(&text).ok()?;
    json_value_to_query_string(&json)
}

pub fn eval_json_query(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) = eval_variant_path_args(arena, args, chunk, "json_query")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "json_query expects Utf8 for path argument".to_string())?;

    let mut builder = StringBuilder::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            let value = match resolve_variant_value(variant_arr, path_arr, row) {
                Some(v) => v,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            match variant_value_to_query_string(&value) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            "json_query expects VARIANT or JSON/VARCHAR as first argument".to_string()
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match parse_variant_path(path_arr.value(row)) {
            Ok(path) => path,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let output = query_json_by_path(&json, &path.segments).and_then(json_value_to_query_string);
        match output {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_variant_query(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) = eval_variant_path_args(arena, args, chunk, "variant_query")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "variant_query")?;

    let mut builder = LargeBinaryBuilder::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row) {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        let serialized = value.serialize();
        builder.append_value(serialized.as_slice());
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_bool(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_bool")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "get_variant_bool")?;

    let mut builder = BooleanBuilder::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row) {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        if is_variant_null(&value).unwrap_or(true) {
            builder.append_null();
            continue;
        }
        match variant_to_bool(&value) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_int(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_int")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "get_variant_int expects Utf8 for path argument".to_string())?;

    let mut builder = Int64Builder::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            let value = match resolve_variant_value(variant_arr, path_arr, row) {
                Some(v) => v,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            if is_variant_null(&value).unwrap_or(true) {
                builder.append_null();
                continue;
            }
            match variant_to_i64(&value) {
                Ok(v) => builder.append_value(v),
                Err(_) => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            "get_variant_int expects VARIANT or JSON/VARCHAR as first argument".to_string()
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match parse_variant_path(path_arr.value(row)) {
            Ok(path) => path,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        match query_json_by_path(&json, &path.segments).and_then(json_value_to_i64) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_double(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_double")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "get_variant_double")?;

    let mut builder = Float64Builder::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row) {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        if is_variant_null(&value).unwrap_or(true) {
            builder.append_null();
            continue;
        }
        match variant_to_f64(&value) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_string(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_string")?;
    let path_arr = path_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "get_variant_string expects Utf8 for path argument".to_string())?;

    let mut builder = StringBuilder::new();
    if let Some(variant_arr) = variant_array.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..chunk.len() {
            let value = match resolve_variant_value(variant_arr, path_arr, row) {
                Some(v) => v,
                None => {
                    builder.append_null();
                    continue;
                }
            };
            if is_variant_null(&value).unwrap_or(true) {
                builder.append_null();
                continue;
            }
            match variant_to_string(&value) {
                Ok(v) => builder.append_value(v),
                Err(_) => builder.append_null(),
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    let json_arr = variant_array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            "get_variant_string expects VARIANT or JSON/VARCHAR as first argument".to_string()
        })?;
    for row in 0..chunk.len() {
        if json_arr.is_null(row) || path_arr.is_null(row) {
            builder.append_null();
            continue;
        }
        let path = match parse_variant_path(path_arr.value(row)) {
            Ok(path) => path,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let json = match serde_json::from_str::<JsonValue>(json_arr.value(row)) {
            Ok(v) => v,
            Err(_) => {
                builder.append_null();
                continue;
            }
        };
        let output =
            query_json_by_path(&json, &path.segments).and_then(json_value_to_output_string);
        match output {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_date(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_date")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "get_variant_date")?;

    let tz = Local::now().offset().fix();
    let mut builder = Date32Builder::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row) {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        if is_variant_null(&value).unwrap_or(true) {
            builder.append_null();
            continue;
        }
        match variant_to_date_days(&value, tz) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_datetime(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_datetime")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "get_variant_datetime")?;

    let tz = Local::now().offset().fix();
    let mut builder = TimestampMicrosecondBuilder::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row) {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        if is_variant_null(&value).unwrap_or(true) {
            builder.append_null();
            continue;
        }
        match variant_to_datetime_micros(&value, tz) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

pub fn eval_get_variant_time(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let (variant_array, path_array) =
        eval_variant_path_args(arena, args, chunk, "get_variant_time")?;
    let (variant_arr, path_arr) =
        downcast_variant_path_args(&variant_array, &path_array, "get_variant_time")?;

    let tz = Local::now().offset().fix();
    let mut builder = TimestampMicrosecondBuilder::new();
    for row in 0..chunk.len() {
        let value = match resolve_variant_value(variant_arr, path_arr, row) {
            Some(v) => v,
            None => {
                builder.append_null();
                continue;
            }
        };
        if is_variant_null(&value).unwrap_or(true) {
            builder.append_null();
            continue;
        }
        match variant_to_time_micros(&value, tz) {
            Ok(v) => builder.append_value(v),
            Err(_) => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::function::variant::test_utils::{slot_id_expr, typed_null};
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, LargeBinaryArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn literal_string(arena: &mut ExprArena, v: &str) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Utf8(v.to_string())))
    }

    #[test]
    fn test_variant_query_root_path() {
        let variant = super::super::common::variant_primitive_serialized(6, &123_i64.to_le_bytes());
        let variant_arr =
            Arc::new(LargeBinaryArray::from(vec![Some(variant.as_slice())])) as ArrayRef;
        let variant_type = DataType::LargeBinary;
        let field = field_with_slot_id(Field::new("v", variant_type.clone(), true), SlotId::new(1));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![variant_arr]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg0 = slot_id_expr(&mut arena, 1, variant_type);
        let arg1 = literal_string(&mut arena, "$");
        let expr = typed_null(&mut arena, DataType::LargeBinary);
        let out = eval_variant_query(&arena, expr, &[arg0, arg1], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert!(!out.is_null(0));
    }

    #[test]
    fn test_get_variant_bool() {
        let variant = super::super::common::variant_primitive_serialized(1, &[]);
        let variant_arr =
            Arc::new(LargeBinaryArray::from(vec![Some(variant.as_slice())])) as ArrayRef;
        let variant_type = DataType::LargeBinary;
        let field = field_with_slot_id(Field::new("v", variant_type.clone(), true), SlotId::new(1));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![variant_arr]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg0 = slot_id_expr(&mut arena, 1, variant_type);
        let arg1 = literal_string(&mut arena, "$");
        let expr = typed_null(&mut arena, DataType::Boolean);
        let out = eval_get_variant_bool(&arena, expr, &[arg0, arg1], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
    }

    #[test]
    fn test_get_variant_int() {
        let variant = super::super::common::variant_primitive_serialized(6, &123_i64.to_le_bytes());
        let variant_arr =
            Arc::new(LargeBinaryArray::from(vec![Some(variant.as_slice())])) as ArrayRef;
        let variant_type = DataType::LargeBinary;
        let field = field_with_slot_id(Field::new("v", variant_type.clone(), true), SlotId::new(1));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![variant_arr]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg0 = slot_id_expr(&mut arena, 1, variant_type);
        let arg1 = literal_string(&mut arena, "$");
        let expr = typed_null(&mut arena, DataType::Int64);
        let out = eval_get_variant_int(&arena, expr, &[arg0, arg1], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out.value(0), 123);
    }

    #[test]
    fn test_get_variant_double() {
        let payload = 3.5_f64.to_le_bytes();
        let variant = super::super::common::variant_primitive_serialized(7, &payload);
        let variant_arr =
            Arc::new(LargeBinaryArray::from(vec![Some(variant.as_slice())])) as ArrayRef;
        let variant_type = DataType::LargeBinary;
        let field = field_with_slot_id(Field::new("v", variant_type.clone(), true), SlotId::new(1));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![variant_arr]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg0 = slot_id_expr(&mut arena, 1, variant_type);
        let arg1 = literal_string(&mut arena, "$");
        let expr = typed_null(&mut arena, DataType::Float64);
        let out = eval_get_variant_double(&arena, expr, &[arg0, arg1], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((out.value(0) - 3.5).abs() < 1e-12);
    }

    #[test]
    fn test_get_variant_string() {
        let variant = super::super::common::variant_primitive_serialized(6, &123_i64.to_le_bytes());
        let variant_arr =
            Arc::new(LargeBinaryArray::from(vec![Some(variant.as_slice())])) as ArrayRef;
        let variant_type = DataType::LargeBinary;
        let field = field_with_slot_id(Field::new("v", variant_type.clone(), true), SlotId::new(1));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![variant_arr]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg0 = slot_id_expr(&mut arena, 1, variant_type);
        let arg1 = literal_string(&mut arena, "$");
        let expr = typed_null(&mut arena, DataType::Utf8);
        let out = eval_get_variant_string(&arena, expr, &[arg0, arg1], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "123");
    }

    #[test]
    fn test_json_query_quotes_string_and_null_for_missing() {
        let json_arr = Arc::new(StringArray::from(vec![
            Some("{\"name\":\"abc\",\"age\":23}"),
            Some("{\"age\":23}"),
        ])) as ArrayRef;
        let field = field_with_slot_id(Field::new("j", DataType::Utf8, true), SlotId::new(1));
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![json_arr]).unwrap();
        let chunk = Chunk::new(batch);

        let mut arena = ExprArena::default();
        let arg0 = slot_id_expr(&mut arena, 1, DataType::Utf8);
        let arg1 = literal_string(&mut arena, "$.name");
        let expr = typed_null(&mut arena, DataType::Utf8);
        let out = eval_json_query(&arena, expr, &[arg0, arg1], &chunk).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "\"abc\"");
        assert!(out.is_null(1));
    }
}
