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
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use novarocks_types::value::bitmap::encode_bitmap_single;
use std::sync::Arc;

pub fn eval_to_bitmap(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let input = arena.eval(args[0], chunk)?;
    let mut builder = BinaryBuilder::new();

    macro_rules! encode_signed_array {
        ($arr:expr) => {{
            for row in 0..$arr.len() {
                if $arr.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let raw = i128::from($arr.value(row));
                if raw < 0 {
                    builder.append_null();
                    continue;
                }
                builder.append_value(encode_bitmap_single(raw as u64));
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }};
    }

    macro_rules! encode_unsigned_array {
        ($arr:expr) => {{
            for row in 0..$arr.len() {
                if $arr.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let raw = u64::from($arr.value(row));
                builder.append_value(encode_bitmap_single(raw));
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }};
    }

    if let Some(arr) = input.as_any().downcast_ref::<BooleanArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
                continue;
            }
            let value = if arr.value(row) { 1 } else { 0 };
            builder.append_value(encode_bitmap_single(value));
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<Int8Array>() {
        encode_signed_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int16Array>() {
        encode_signed_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int32Array>() {
        encode_signed_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int64Array>() {
        encode_signed_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<UInt8Array>() {
        encode_unsigned_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<UInt16Array>() {
        encode_unsigned_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<UInt32Array>() {
        encode_unsigned_array!(arr);
    }
    if let Some(arr) = input.as_any().downcast_ref::<UInt64Array>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
                continue;
            }
            builder.append_value(encode_bitmap_single(arr.value(row)));
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<StringArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
                continue;
            }
            if let Some(parsed) = parse_unsigned_decimal(arr.value(row)) {
                builder.append_value(encode_bitmap_single(parsed));
            } else {
                builder.append_null();
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<LargeStringArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
                continue;
            }
            if let Some(parsed) = parse_unsigned_decimal(arr.value(row)) {
                builder.append_value(encode_bitmap_single(parsed));
            } else {
                builder.append_null();
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<BinaryArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
                continue;
            }
            let Ok(text) = std::str::from_utf8(arr.value(row)) else {
                builder.append_null();
                continue;
            };
            if let Some(parsed) = parse_unsigned_decimal(text) {
                builder.append_value(encode_bitmap_single(parsed));
            } else {
                builder.append_null();
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_null();
                continue;
            }
            let Ok(text) = std::str::from_utf8(arr.value(row)) else {
                builder.append_null();
                continue;
            };
            if let Some(parsed) = parse_unsigned_decimal(text) {
                builder.append_value(encode_bitmap_single(parsed));
            } else {
                builder.append_null();
            }
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    Err(format!(
        "to_bitmap expects BOOLEAN/INTEGER/VARCHAR/BINARY input, got {:?}",
        input.data_type()
    ))
}

fn parse_unsigned_decimal(text: &str) -> Option<u64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<u64>().ok()
}
