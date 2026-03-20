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
    Array, ArrayRef, BinaryArray, Int32Builder, LargeBinaryArray, LargeStringArray, StringArray,
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
                other => {
                    return Err(format!(
                        "murmur_hash3_32 expects VARCHAR/VARBINARY input, got {:?}",
                        other
                    ));
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
