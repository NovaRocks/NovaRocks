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
use std::sync::Arc;

const BITMAP_TYPE_SINGLE32: u8 = 1;
const BITMAP_TYPE_SINGLE64: u8 = 3;

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

pub(crate) fn encode_bitmap_single(value: u64) -> Vec<u8> {
    if u32::try_from(value).is_ok() {
        let mut out = Vec::with_capacity(1 + std::mem::size_of::<u32>());
        out.push(BITMAP_TYPE_SINGLE32);
        out.extend_from_slice(&(value as u32).to_le_bytes());
        out
    } else {
        let mut out = Vec::with_capacity(1 + std::mem::size_of::<u64>());
        out.push(BITMAP_TYPE_SINGLE64);
        out.extend_from_slice(&value.to_le_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::eval_to_bitmap;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::{ExprArena, ExprNode};
    use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn one_col_chunk(data_type: DataType, array: ArrayRef) -> Chunk {
        let field = field_with_slot_id(Field::new("c1", data_type, true), SlotId(1));
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        Chunk::new(batch)
    }

    #[test]
    fn to_bitmap_encodes_single_values() {
        let mut arena = ExprArena::default();
        let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Int64);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Object("to_bitmap"),
                args: vec![arg],
            },
            DataType::Binary,
        );

        let input = Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as ArrayRef;
        let chunk = one_col_chunk(DataType::Int64, input);
        let out = arena.eval(expr, &chunk).expect("eval");
        let out = out.as_any().downcast_ref::<BinaryArray>().expect("binary");

        assert_eq!(out.value(0), &[1, 1, 0, 0, 0]);
        assert_eq!(out.value(1), &[1, 2, 0, 0, 0]);
        assert!(out.is_null(2));
    }

    #[test]
    fn to_bitmap_ignores_negative_values() {
        let mut arena = ExprArena::default();
        let arg = arena.push_typed(ExprNode::SlotId(SlotId(1)), DataType::Int64);
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind: crate::exec::expr::function::FunctionKind::Object("to_bitmap"),
                args: vec![arg],
            },
            DataType::Binary,
        );

        let input = Arc::new(Int64Array::from(vec![Some(-1)])) as ArrayRef;
        let chunk = one_col_chunk(DataType::Int64, input);
        let out = arena.eval(expr, &chunk).expect("eval");
        let out = out.as_any().downcast_ref::<BinaryArray>().expect("binary");

        assert!(out.is_null(0));
    }
}
