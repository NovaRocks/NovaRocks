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
    Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, Int16Array, Int32Array, Int32Builder,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

const MURMUR3_32_SEED: u32 = 104_729;

pub fn eval_murmur_hash3_32(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let mut inputs = Vec::with_capacity(args.len());
    for arg in args {
        inputs.push(arena.eval(*arg, chunk)?);
    }

    let mut builder = Int32Builder::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        let mut seed = MURMUR3_32_SEED;
        let mut has_null = false;
        for input in &inputs {
            match input.data_type() {
                DataType::Utf8 => {
                    let arr = input
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| "downcast StringArray failed".to_string())?;
                    if arr.is_null(row) {
                        has_null = true;
                        break;
                    }
                    seed = murmur_hash3_32(arr.value(row).as_bytes(), seed);
                }
                DataType::LargeUtf8 => {
                    let arr = input
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .ok_or_else(|| "downcast LargeStringArray failed".to_string())?;
                    if arr.is_null(row) {
                        has_null = true;
                        break;
                    }
                    seed = murmur_hash3_32(arr.value(row).as_bytes(), seed);
                }
                DataType::Binary => {
                    let arr = input
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| "downcast BinaryArray failed".to_string())?;
                    if arr.is_null(row) {
                        has_null = true;
                        break;
                    }
                    seed = murmur_hash3_32(arr.value(row), seed);
                }
                DataType::LargeBinary => {
                    let arr = input
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()
                        .ok_or_else(|| "downcast LargeBinaryArray failed".to_string())?;
                    if arr.is_null(row) {
                        has_null = true;
                        break;
                    }
                    seed = murmur_hash3_32(arr.value(row), seed);
                }
                // StarRocks coerces non-VARCHAR inputs to their textual form
                // via `ColumnViewer<TYPE_VARCHAR>`; mirror that so callers like
                // `murmur_hash3_32(ifnull(int_col, 0))` hash the value's decimal
                // representation rather than failing.
                _ => {
                    if let Some(s) = try_stringify_scalar(input, row)? {
                        seed = murmur_hash3_32(s.as_bytes(), seed);
                    } else {
                        has_null = true;
                        break;
                    }
                }
            }
        }
        if has_null {
            builder.append_null();
        } else {
            builder.append_value(seed as i32);
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Best-effort StarRocks-compatible `ColumnViewer<TYPE_VARCHAR>` on an arbitrary
/// scalar input array. Returns `None` when the row is NULL. Returns an error
/// for aggregate/nested types that StarRocks itself doesn't hash directly.
fn try_stringify_scalar(input: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    if input.is_null(row) {
        return Ok(None);
    }
    macro_rules! cast {
        ($t:ty) => {{
            let arr = input
                .as_any()
                .downcast_ref::<$t>()
                .ok_or_else(|| format!("downcast {} failed", stringify!($t)))?;
            Ok(Some(arr.value(row).to_string()))
        }};
    }
    match input.data_type() {
        DataType::Int8 => cast!(Int8Array),
        DataType::Int16 => cast!(Int16Array),
        DataType::Int32 => cast!(Int32Array),
        DataType::Int64 => cast!(Int64Array),
        DataType::UInt8 => cast!(UInt8Array),
        DataType::UInt16 => cast!(UInt16Array),
        DataType::UInt32 => cast!(UInt32Array),
        DataType::UInt64 => cast!(UInt64Array),
        DataType::Boolean => {
            let arr = input
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "downcast BooleanArray failed".to_string())?;
            // StarRocks casts BOOLEAN → VARCHAR as "1"/"0".
            Ok(Some(if arr.value(row) { "1" } else { "0" }.to_string()))
        }
        _ => {
            // Fall back to arrow's cast-to-string formatter for everything else
            // (Decimal, Float, Date, Timestamp, ...). StarRocks hashes these via
            // the VARCHAR viewer, which produces the same lexical form.
            use arrow::compute::kernels::cast::{CastOptions, cast_with_options};
            use arrow::util::display::FormatOptions;
            let opts = CastOptions {
                safe: false,
                format_options: FormatOptions::default(),
            };
            let casted = cast_with_options(input.as_ref(), &DataType::Utf8, &opts)
                .map_err(|e| format!("cast to Utf8 failed for murmur_hash3_32: {e}"))?;
            let arr = casted
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "cast result is not StringArray".to_string())?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(arr.value(row).to_string()))
            }
        }
    }
}

fn murmur_hash3_32(data: &[u8], seed: u32) -> u32 {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut hash = seed;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let rem = chunks.remainder();
    let mut k1 = 0u32;
    match rem.len() {
        3 => {
            k1 ^= (rem[2] as u32) << 16;
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        2 => {
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        1 => {
            k1 ^= rem[0] as u32;
        }
        _ => {}
    }
    if k1 != 0 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        hash ^= k1;
    }

    hash ^= data.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;
    hash
}
