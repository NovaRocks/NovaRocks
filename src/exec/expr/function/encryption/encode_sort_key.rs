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
use crate::common::largeint;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use std::sync::Arc;

const NULL_MARKER: u8 = 0x00;
const NOT_NULL_MARKER: u8 = 0x01;
const COLUMN_SEPARATOR: u8 = 0x00;
const SLICE_ESCAPE_BYTE: u8 = 0x00;
const SLICE_ESCAPE_SUFFIX: u8 = 0x01;

#[inline]
fn check_arg_len(arg_len: usize, rows: usize, arg_idx: usize) -> Result<(), String> {
    if arg_len == rows || arg_len == 1 {
        return Ok(());
    }
    Err(format!(
        "encode_sort_key: arg{} length mismatch: expected {} or 1, got {}",
        arg_idx, rows, arg_len
    ))
}

#[inline]
fn arg_row_index(arg_len: usize, row: usize) -> usize {
    if arg_len == 1 { 0 } else { row }
}

#[inline]
fn encode_i8(value: i8, out: &mut Vec<u8>) {
    out.push((value as u8) ^ 0x80);
}

#[inline]
fn encode_i16(value: i16, out: &mut Vec<u8>) {
    let encoded = (value as u16) ^ 0x8000;
    out.extend_from_slice(&encoded.to_be_bytes());
}

#[inline]
fn encode_i32(value: i32, out: &mut Vec<u8>) {
    let encoded = (value as u32) ^ 0x8000_0000;
    out.extend_from_slice(&encoded.to_be_bytes());
}

#[inline]
fn encode_i64(value: i64, out: &mut Vec<u8>) {
    let encoded = (value as u64) ^ 0x8000_0000_0000_0000;
    out.extend_from_slice(&encoded.to_be_bytes());
}

#[inline]
fn encode_i128(value: i128, out: &mut Vec<u8>) {
    let encoded = (value as u128) ^ 0x8000_0000_0000_0000_0000_0000_0000_0000;
    out.extend_from_slice(&encoded.to_be_bytes());
}

#[inline]
fn encode_u16(value: u16, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_be_bytes());
}

#[inline]
fn encode_u32(value: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_be_bytes());
}

#[inline]
fn encode_u64(value: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_be_bytes());
}

#[inline]
fn encode_float32(value: f32, out: &mut Vec<u8>) {
    let mut bits = value.to_bits();
    bits ^= if (bits & 0x8000_0000) != 0 {
        0xFFFF_FFFF
    } else {
        0x8000_0000
    };
    out.extend_from_slice(&bits.to_be_bytes());
}

#[inline]
fn encode_float64(value: f64, out: &mut Vec<u8>) {
    let mut bits = value.to_bits();
    bits ^= if (bits & 0x8000_0000_0000_0000) != 0 {
        0xFFFF_FFFF_FFFF_FFFF
    } else {
        0x8000_0000_0000_0000
    };
    out.extend_from_slice(&bits.to_be_bytes());
}

fn encode_slice(value: &[u8], is_last_field: bool, out: &mut Vec<u8>) {
    if is_last_field {
        out.extend_from_slice(value);
        return;
    }
    for byte in value {
        if *byte == SLICE_ESCAPE_BYTE {
            out.push(SLICE_ESCAPE_BYTE);
            out.push(SLICE_ESCAPE_SUFFIX);
        } else {
            out.push(*byte);
        }
    }
    out.push(SLICE_ESCAPE_BYTE);
    out.push(SLICE_ESCAPE_BYTE);
}

fn encode_column(
    arg: &ArrayRef,
    arg_idx: usize,
    is_last_field: bool,
    rows: usize,
    out: &mut [Vec<u8>],
) -> Result<(), String> {
    check_arg_len(arg.len(), rows, arg_idx)?;

    match arg.data_type() {
        DataType::Null => {
            for buffer in out.iter_mut().take(rows) {
                buffer.push(NULL_MARKER);
            }
        }
        DataType::Boolean => {
            let arr = arg
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "encode_sort_key: failed to downcast BooleanArray".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    buffer.push(u8::from(arr.value(idx)));
                }
            }
        }
        DataType::Int8 => {
            let arr = arg
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast Int8Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_i8(arr.value(idx), buffer);
                }
            }
        }
        DataType::UInt8 => {
            let arr = arg
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast UInt8Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    buffer.push(arr.value(idx));
                }
            }
        }
        DataType::Int16 => {
            let arr = arg
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast Int16Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_i16(arr.value(idx), buffer);
                }
            }
        }
        DataType::UInt16 => {
            let arr = arg
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast UInt16Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_u16(arr.value(idx), buffer);
                }
            }
        }
        DataType::Int32 => {
            let arr = arg
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast Int32Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_i32(arr.value(idx), buffer);
                }
            }
        }
        DataType::UInt32 => {
            let arr = arg
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast UInt32Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_u32(arr.value(idx), buffer);
                }
            }
        }
        DataType::Date32 => {
            let arr = arg
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast Date32Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_i32(arr.value(idx), buffer);
                }
            }
        }
        DataType::Int64 => {
            let arr = arg
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast Int64Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_i64(arr.value(idx), buffer);
                }
            }
        }
        DataType::UInt64 => {
            let arr = arg
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast UInt64Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_u64(arr.value(idx), buffer);
                }
            }
        }
        DataType::Float32 => {
            let arr = arg
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast Float32Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_float32(arr.value(idx), buffer);
                }
            }
        }
        DataType::Float64 => {
            let arr = arg
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast Float64Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_float64(arr.value(idx), buffer);
                }
            }
        }
        DataType::Timestamp(_, _) => {
            let casted = cast(arg, &DataType::Int64)
                .map_err(|e| format!("encode_sort_key: failed to cast TIMESTAMP to BIGINT: {e}"))?;
            let arr = casted
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    "encode_sort_key: failed to downcast timestamp cast result".to_string()
                })?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_i64(arr.value(idx), buffer);
                }
            }
        }
        DataType::Utf8 => {
            let arr = arg
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "encode_sort_key: failed to downcast StringArray".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_slice(arr.value(idx).as_bytes(), is_last_field, buffer);
                }
            }
        }
        DataType::Binary => {
            let arr = arg
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "encode_sort_key: failed to downcast BinaryArray".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_slice(arr.value(idx), is_last_field, buffer);
                }
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = arg
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| "encode_sort_key: failed to downcast Decimal128Array".to_string())?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    encode_i128(arr.value(idx), buffer);
                }
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = arg
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    "encode_sort_key: failed to downcast FixedSizeBinaryArray".to_string()
                })?;
            for (row, buffer) in out.iter_mut().enumerate().take(rows) {
                let idx = arg_row_index(arr.len(), row);
                if arr.is_null(idx) {
                    buffer.push(NULL_MARKER);
                } else {
                    buffer.push(NOT_NULL_MARKER);
                    let value = largeint::value_at(arr, idx)?;
                    encode_i128(value, buffer);
                }
            }
        }
        other => {
            return Err(format!(
                "encode_sort_key: unsupported argument type: {:?}",
                other
            ));
        }
    }

    if !is_last_field {
        for buffer in out.iter_mut().take(rows) {
            buffer.push(COLUMN_SEPARATOR);
        }
    }
    Ok(())
}

pub fn eval_encode_sort_key(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    if args.is_empty() {
        return Err("encode_sort_key requires at least 1 argument".to_string());
    }

    let rows = chunk.len();
    let evaluated_args: Vec<ArrayRef> = args
        .iter()
        .map(|arg| arena.eval(*arg, chunk))
        .collect::<Result<Vec<_>, _>>()?;

    let mut encoded = vec![Vec::<u8>::new(); rows];
    for (idx, arg) in evaluated_args.iter().enumerate() {
        encode_column(
            arg,
            idx,
            idx + 1 == evaluated_args.len(),
            rows,
            &mut encoded,
        )?;
    }

    let mut builder = BinaryBuilder::new();
    for value in encoded {
        builder.append_value(value);
    }
    let out = Arc::new(builder.finish()) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "encode_sort_key")
}

#[cfg(test)]
mod tests {
    use super::eval_encode_sort_key;
    use crate::exec::expr::function::encryption::test_utils::{
        chunk_len_1, literal_string, typed_null,
    };
    use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
    use arrow::array::BinaryArray;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn literal_i32(arena: &mut ExprArena, v: i32) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Int32(v)))
    }

    fn literal_i64(arena: &mut ExprArena, v: i64) -> ExprId {
        arena.push(ExprNode::Literal(LiteralValue::Int64(v)))
    }

    fn eval_hex(arena: &ExprArena, expr: ExprId, args: &[ExprId]) -> String {
        let out = eval_encode_sort_key(arena, expr, args, &chunk_len_1()).unwrap();
        let out = out.as_any().downcast_ref::<BinaryArray>().unwrap();
        hex::encode_upper(out.value(0))
    }

    #[test]
    fn test_encode_sort_key_known_vectors() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Binary);
        let null = typed_null(&mut arena, DataType::Null);
        let empty = literal_string(&mut arena, "");
        let max_i64 = literal_i64(&mut arena, i64::MAX);
        let neg = literal_i64(&mut arena, -9_223_372_036_854_775_807);
        let v32 = literal_i32(&mut arena, 465_254_298);

        assert_eq!(eval_hex(&arena, expr, &[null]), "00");
        assert_eq!(eval_hex(&arena, expr, &[null, null]), "000000");
        assert_eq!(
            eval_hex(&arena, expr, &[empty, max_i64]),
            "0100000001FFFFFFFFFFFFFFFF"
        );
        assert_eq!(
            eval_hex(&arena, expr, &[empty, neg, v32]),
            "0100000001000000000000000100019BBB379A"
        );
    }

    #[test]
    fn test_encode_sort_key_unsupported_complex_type() {
        let mut arena = ExprArena::default();
        let expr = typed_null(&mut arena, DataType::Binary);
        let list_null = typed_null(
            &mut arena,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );
        let err = eval_encode_sort_key(&arena, expr, &[list_null], &chunk_len_1()).unwrap_err();
        assert!(err.contains("unsupported argument type"));
    }
}
