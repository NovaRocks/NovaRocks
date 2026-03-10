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

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

pub const MURMUR_SEED: u32 = 0xadc8_3b19;
const MURMUR_PRIME: u64 = 0xc6a4_a793_5bd1_e995;

pub fn murmur_hash64a(data: &[u8], seed: u32) -> Result<u64, String> {
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
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::{MURMUR_SEED, murmur_hash64a};

    #[test]
    fn murmur_hash64a_matches_known_value() {
        let hash = murmur_hash64a(b"novarocks", MURMUR_SEED).expect("hash");
        assert_eq!(hash, 7_139_930_336_803_328_733);
    }
}

pub fn prehash_array_value(
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<Option<u64>, String> {
    macro_rules! hash_from_bytes {
        ($arr:expr, $value:expr) => {{
            if $arr.is_null(row) {
                return Ok(None);
            }
            return Ok(Some(murmur_hash64a($value.as_ref(), MURMUR_SEED)?));
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| format!("{context}: failed to downcast BooleanArray"))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(murmur_hash64a(
                    &[if arr.value(row) { 1 } else { 0 }],
                    MURMUR_SEED,
                )?))
            }
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int8Array"))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int16Array"))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int32Array"))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Int64Array"))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Float32Array"))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Float64Array"))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Date32Array"))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| format!("{context}: failed to downcast TimestampSecondArray"))?;
                hash_from_bytes!(arr, arr.value(row).to_le_bytes())
            }
            TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        format!("{context}: failed to downcast TimestampMillisecondArray")
                    })?;
                hash_from_bytes!(arr, arr.value(row).to_le_bytes())
            }
            TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        format!("{context}: failed to downcast TimestampMicrosecondArray")
                    })?;
                hash_from_bytes!(arr, arr.value(row).to_le_bytes())
            }
            TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        format!("{context}: failed to downcast TimestampNanosecondArray")
                    })?;
                hash_from_bytes!(arr, arr.value(row).to_le_bytes())
            }
        },
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| format!("{context}: failed to downcast Decimal128Array"))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("{context}: failed to downcast StringArray"))?;
            hash_from_bytes!(arr, arr.value(row).as_bytes())
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| format!("{context}: failed to downcast LargeStringArray"))?;
            hash_from_bytes!(arr, arr.value(row).as_bytes())
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| format!("{context}: failed to downcast BinaryArray"))?;
            hash_from_bytes!(arr, arr.value(row))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| format!("{context}: failed to downcast LargeBinaryArray"))?;
            hash_from_bytes!(arr, arr.value(row))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| format!("{context}: failed to downcast FixedSizeBinaryArray"))?;
            hash_from_bytes!(arr, arr.value(row))
        }
        other => Err(format!(
            "{context}: unsupported sketch hash input type {:?}",
            other
        )),
    }
}
