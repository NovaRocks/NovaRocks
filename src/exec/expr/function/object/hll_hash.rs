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
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use std::sync::Arc;

const HLL_DATA_EMPTY: u8 = 0;
const HLL_DATA_EXPLICIT: u8 = 1;

const MURMUR_PRIME: u64 = 0xc6a4_a793_5bd1_e995;
const MURMUR_SEED: u32 = 0xadc8_3b19;

pub fn eval_hll_hash(
    arena: &ExprArena,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let _ = expr;
    let input = arena.eval(args[0], chunk)?;
    let mut builder = BinaryBuilder::new();

    macro_rules! hash_from_bytes {
        ($arr:expr, $row:ident => $bytes:expr) => {{
            for $row in 0..$arr.len() {
                if $arr.is_null($row) {
                    builder.append_value(encode_hll_empty());
                    continue;
                }
                let hash = murmur_hash64a($bytes.as_ref(), MURMUR_SEED);
                builder.append_value(encode_hll_single(hash));
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }};
    }

    if let Some(arr) = input.as_any().downcast_ref::<BooleanArray>() {
        for row in 0..arr.len() {
            if arr.is_null(row) {
                builder.append_value(encode_hll_empty());
                continue;
            }
            let hash = murmur_hash64a(&[if arr.value(row) { 1 } else { 0 }], MURMUR_SEED);
            builder.append_value(encode_hll_single(hash));
        }
        return Ok(Arc::new(builder.finish()) as ArrayRef);
    }

    if let Some(arr) = input.as_any().downcast_ref::<Int8Array>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int16Array>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int32Array>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<Int64Array>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<Float32Array>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<Float64Array>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<Date32Array>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<TimestampSecondArray>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<TimestampMillisecondArray>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<TimestampNanosecondArray>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<Decimal128Array>() {
        hash_from_bytes!(arr, row => arr.value(row).to_le_bytes());
    }
    if let Some(arr) = input.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        hash_from_bytes!(arr, row => arr.value(row));
    }

    if let Some(arr) = input.as_any().downcast_ref::<StringArray>() {
        hash_from_bytes!(arr, row => arr.value(row).as_bytes());
    }

    if let Some(arr) = input.as_any().downcast_ref::<LargeStringArray>() {
        hash_from_bytes!(arr, row => arr.value(row).as_bytes());
    }

    if let Some(arr) = input.as_any().downcast_ref::<BinaryArray>() {
        hash_from_bytes!(arr, row => arr.value(row));
    }

    if let Some(arr) = input.as_any().downcast_ref::<LargeBinaryArray>() {
        hash_from_bytes!(arr, row => arr.value(row));
    }

    Err(format!(
        "hll_hash expects scalar input, got {:?}",
        input.data_type()
    ))
}

fn encode_hll_empty() -> Vec<u8> {
    vec![HLL_DATA_EMPTY]
}

fn encode_hll_single(hash: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + std::mem::size_of::<u64>());
    out.push(HLL_DATA_EXPLICIT);
    out.push(1);
    out.extend_from_slice(&hash.to_le_bytes());
    out
}

fn murmur_hash64a(data: &[u8], seed: u32) -> u64 {
    let r: u32 = 47;
    let mut h = (seed as u64) ^ (data.len() as u64).wrapping_mul(MURMUR_PRIME);

    let mut offset = 0usize;
    while offset + 8 <= data.len() {
        let mut block = [0u8; 8];
        block.copy_from_slice(&data[offset..offset + 8]);
        let mut k = u64::from_le_bytes(block);
        k = k.wrapping_mul(MURMUR_PRIME);
        k ^= k >> r;
        k = k.wrapping_mul(MURMUR_PRIME);
        h ^= k;
        h = h.wrapping_mul(MURMUR_PRIME);
        offset += 8;
    }

    let tail = &data[offset..];
    if !tail.is_empty() {
        for (idx, byte) in tail.iter().enumerate() {
            h ^= (*byte as u64) << (idx * 8);
        }
        h = h.wrapping_mul(MURMUR_PRIME);
    }

    h ^= h >> r;
    h = h.wrapping_mul(MURMUR_PRIME);
    h ^= h >> r;
    h
}
