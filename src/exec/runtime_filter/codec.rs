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
//! StarRocks runtime-filter wire codec.
//!
//! Responsibilities:
//! - Encodes and decodes IN/membership/bloom runtime filters for RPC exchange compatibility.
//! - Implements binary parsing helpers with strict bounds and type validation.
//!
//! Key exported interfaces:
//! - Types: `StarrocksRuntimeFilterType`.
//! - Functions: `peek_starrocks_filter_type`, `decode_starrocks_in_filter`, `encode_starrocks_in_filter`, `decode_starrocks_membership_filter`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use hashbrown::HashSet;

use crate::common::ids::SlotId;

use super::{
    RuntimeBitsetFilter, RuntimeBloomFilter, RuntimeEmptyFilter, RuntimeInFilter,
    RuntimeInFilterValues, RuntimeMembershipFilter, RuntimeMinMaxFilter, SimdBlockFilter,
};

const RF_VERSION_V2: u8 = 0x3;
const RF_VERSION_V3: u8 = 0x4;
const RF_TYPE_EMPTY_FILTER: u8 = 1;
const RF_TYPE_BLOOM_FILTER: u8 = 2;
const RF_TYPE_BITSET_FILTER: u8 = 3;
const RF_TYPE_IN_FILTER: u8 = 4;

type RuntimeInFilterWriter<'a> = Box<dyn FnMut(&mut Vec<u8>) + 'a>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Wire-level runtime filter type tags used by StarRocks compatibility codec.
pub(crate) enum StarrocksRuntimeFilterType {
    Empty,
    Bloom,
    Bitset,
    In,
}

/// Read runtime filter type tag from a StarRocks wire payload without full decoding.
pub(crate) fn peek_starrocks_filter_type(
    data: &[u8],
) -> Result<StarrocksRuntimeFilterType, String> {
    if data.is_empty() {
        return Err("runtime filter data is empty".to_string());
    }
    let version = data[0];
    if version < RF_VERSION_V2 {
        return Err(format!("unsupported runtime filter version: {version}"));
    }
    let rf_type = if version >= RF_VERSION_V3 {
        if data.len() < 2 {
            return Err("runtime filter data truncated (missing type)".to_string());
        }
        data[1]
    } else {
        RF_TYPE_BLOOM_FILTER
    };
    match rf_type {
        RF_TYPE_EMPTY_FILTER => Ok(StarrocksRuntimeFilterType::Empty),
        RF_TYPE_BLOOM_FILTER => Ok(StarrocksRuntimeFilterType::Bloom),
        RF_TYPE_BITSET_FILTER => Ok(StarrocksRuntimeFilterType::Bitset),
        RF_TYPE_IN_FILTER => Ok(StarrocksRuntimeFilterType::In),
        _ => Err(format!("unsupported runtime filter type: {rf_type}")),
    }
}

/// Decode a StarRocks IN filter payload into runtime in-filter representation.
pub(crate) fn decode_starrocks_in_filter(
    filter_id: i32,
    slot_id: SlotId,
    data_type: Option<&arrow::datatypes::DataType>,
    data: &[u8],
) -> Result<RuntimeInFilter, String> {
    if data.is_empty() {
        return Err("runtime filter data is empty".to_string());
    }

    let mut offset = 0usize;
    let version = data[0];
    offset += 1;
    if version < RF_VERSION_V2 {
        return Err(format!("unsupported runtime filter version: {version}"));
    }

    if version >= RF_VERSION_V3 {
        if data.len() < offset + 1 {
            return Err("runtime filter data truncated (missing type)".to_string());
        }
        let filter_type = data[offset];
        offset += 1;
        if filter_type != RF_TYPE_IN_FILTER {
            return Err(format!("unsupported runtime filter type: {filter_type}"));
        }
    } else {
        return Err("runtime filter type missing (version v2)".to_string());
    }

    let ltype = read_i32_le(data, &mut offset)?;
    let element_count = read_u32_le(data, &mut offset)? as usize;

    use crate::thrift::types;
    let t = types::TPrimitiveType(ltype);
    let values = if t == types::TPrimitiveType::BOOLEAN {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let v = read_u8(data, &mut offset)?;
            set.insert(v != 0);
        }
        RuntimeInFilterValues::Bool(set)
    } else if t == types::TPrimitiveType::TINYINT {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let v = read_i8(data, &mut offset)?;
            set.insert(v);
        }
        RuntimeInFilterValues::Int8(set)
    } else if t == types::TPrimitiveType::SMALLINT {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let v = read_i16_le(data, &mut offset)?;
            set.insert(v);
        }
        RuntimeInFilterValues::Int16(set)
    } else if t == types::TPrimitiveType::INT {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let v = read_i32_le(data, &mut offset)?;
            set.insert(v);
        }
        RuntimeInFilterValues::Int32(set)
    } else if t == types::TPrimitiveType::BIGINT {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let v = read_i64_le(data, &mut offset)?;
            set.insert(v);
        }
        RuntimeInFilterValues::Int64(set)
    } else if t == types::TPrimitiveType::LARGEINT {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let v = read_i128_le(data, &mut offset)?;
            set.insert(v);
        }
        RuntimeInFilterValues::LargeInt(set)
    } else if t == types::TPrimitiveType::FLOAT {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let bits = read_u32_le(data, &mut offset)?;
            set.insert(bits);
        }
        RuntimeInFilterValues::Float32(set)
    } else if t == types::TPrimitiveType::DOUBLE {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let bits = read_u64_le(data, &mut offset)?;
            set.insert(bits);
        }
        RuntimeInFilterValues::Float64(set)
    } else if t == types::TPrimitiveType::DATE {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let v = read_i32_le(data, &mut offset)?;
            set.insert(v);
        }
        RuntimeInFilterValues::Date32(set)
    } else if t == types::TPrimitiveType::DATETIME || t == types::TPrimitiveType::TIME {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            let v = read_i64_le(data, &mut offset)?;
            set.insert(v);
        }
        RuntimeInFilterValues::TimestampMicrosecond(set)
    } else if t == types::TPrimitiveType::VARCHAR || t == types::TPrimitiveType::CHAR {
        let mut set = HashSet::new();
        for _ in 0..element_count {
            set.insert(read_varchar(data, &mut offset)?);
        }
        RuntimeInFilterValues::Utf8(set)
    } else if t == types::TPrimitiveType::DECIMAL128 {
        let (precision, scale) = match data_type {
            Some(arrow::datatypes::DataType::Decimal128(p, s)) => (*p, *s),
            _ => {
                return Err(
                    "decimal128 in-filter decode requires probe Decimal128 column type".to_string(),
                );
            }
        };
        let mut set = HashSet::new();
        for _ in 0..element_count {
            set.insert(read_i128_le(data, &mut offset)?);
        }
        RuntimeInFilterValues::Decimal128 {
            values: set,
            precision,
            scale,
        }
    } else {
        return Err(format!(
            "unsupported runtime filter primitive type: {:?}",
            t
        ));
    };

    Ok(RuntimeInFilter::new(filter_id, slot_id, values))
}

/// Encode a runtime IN filter into StarRocks-compatible wire payload.
pub(crate) fn encode_starrocks_in_filter(filter: &RuntimeInFilter) -> Result<Vec<u8>, String> {
    use crate::thrift::types;
    let (t, count, mut write_values): (types::TPrimitiveType, usize, RuntimeInFilterWriter<'_>) =
        match filter.values() {
            RuntimeInFilterValues::Bool(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::BOOLEAN,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.push(if v { 1 } else { 0 });
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Int8(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::TINYINT,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.push(v as u8);
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Int16(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::SMALLINT,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Int32(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::INT,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Int64(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::BIGINT,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::LargeInt(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::LARGEINT,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Float32(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::FLOAT,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Float64(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::DOUBLE,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Date32(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::DATE,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::TimestampSecond(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::DATETIME,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            let micros = v.saturating_mul(1_000_000);
                            buf.extend_from_slice(&micros.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::TimestampMillisecond(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::DATETIME,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            let micros = v.saturating_mul(1_000);
                            buf.extend_from_slice(&micros.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::TimestampMicrosecond(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::DATETIME,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::TimestampNanosecond(values) => {
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::DATETIME,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            let micros = v / 1_000;
                            buf.extend_from_slice(&micros.to_le_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Utf8(values) => {
                // SR VARCHAR in-filter: each value = int32 len (LE) + raw bytes (runtime_in_filter.h:224-231)
                let mut iter = values.iter().cloned();
                (
                    types::TPrimitiveType::VARCHAR,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&(v.len() as i32).to_le_bytes());
                            buf.extend_from_slice(v.as_bytes());
                        }
                    }),
                )
            }
            RuntimeInFilterValues::Decimal128 { values, .. } => {
                // SR DECIMAL128: each value = 16B int128 LE; ltype tag carries no precision/scale (runtime_in_filter.h:233-236)
                let mut iter = values.iter().copied();
                (
                    types::TPrimitiveType::DECIMAL128,
                    values.len(),
                    Box::new(move |buf| {
                        for v in iter.by_ref() {
                            buf.extend_from_slice(&v.to_le_bytes());
                        }
                    }),
                )
            }
        };

    let mut buf = Vec::with_capacity(1 + 1 + 4 + 4 + count.saturating_mul(16));
    buf.push(RF_VERSION_V3);
    buf.push(RF_TYPE_IN_FILTER);
    buf.extend_from_slice(&t.0.to_le_bytes());
    buf.extend_from_slice(&(count as u32).to_le_bytes());
    write_values(&mut buf);
    Ok(buf)
}

/// Decode a StarRocks membership filter payload into runtime filter representation.
pub(crate) fn decode_starrocks_membership_filter(
    filter_id: i32,
    slot_id: SlotId,
    data: &[u8],
) -> Result<RuntimeMembershipFilter, String> {
    if data.is_empty() {
        return Err("runtime filter data is empty".to_string());
    }
    let mut offset = 0usize;
    let version = read_u8(data, &mut offset)?;
    if version < RF_VERSION_V2 {
        return Err(format!("unsupported runtime filter version: {version}"));
    }
    let rf_type = if version >= RF_VERSION_V3 {
        read_u8(data, &mut offset)?
    } else {
        RF_TYPE_BLOOM_FILTER
    };
    if rf_type == RF_TYPE_IN_FILTER {
        return Err("runtime filter type is IN_FILTER".to_string());
    }
    let ltype = read_i32_le(data, &mut offset)?;
    let ltype = crate::thrift::types::TPrimitiveType(ltype);

    match rf_type {
        RF_TYPE_BLOOM_FILTER => {
            let has_null = read_bool(data, &mut offset)?;
            let size = read_u64_le(data, &mut offset)?;
            let partitions = read_u64_le(data, &mut offset)? as usize;
            let join_mode = read_i8(data, &mut offset)?;

            let mut merged: Option<SimdBlockFilter> = None;
            if partitions == 0 {
                let bf = SimdBlockFilter::deserialize(data, &mut offset)?;
                if bf.can_use() {
                    merged = Some(bf);
                }
            } else {
                for _ in 0..partitions {
                    let bf = SimdBlockFilter::deserialize(data, &mut offset)?;
                    if bf.can_use() {
                        if let Some(dst) = merged.as_mut() {
                            dst.merge_from(&bf)?;
                        } else {
                            merged = Some(bf);
                        }
                    }
                }
            }
            let min_max = RuntimeMinMaxFilter::decode(ltype, data, &mut offset)?;

            Ok(RuntimeMembershipFilter::Bloom(RuntimeBloomFilter::new(
                filter_id, slot_id, ltype, has_null, join_mode, size, merged, min_max,
            )))
        }
        RF_TYPE_BITSET_FILTER => {
            let has_null = read_bool(data, &mut offset)?;
            let size = read_u64_le(data, &mut offset)?;
            let join_mode = read_i8(data, &mut offset)?;
            let (min_value, max_value) = read_min_max_i64(&ltype, data, &mut offset)?;
            let bitset_size = read_u64_le(data, &mut offset)? as usize;
            if data.len() < offset + bitset_size {
                return Err("runtime bitset filter data truncated".to_string());
            }
            let bitset = data[offset..offset + bitset_size].to_vec();
            offset += bitset_size;
            let min_max = RuntimeMinMaxFilter::decode(ltype, data, &mut offset)?;
            Ok(RuntimeMembershipFilter::Bitset(RuntimeBitsetFilter::new(
                filter_id, slot_id, ltype, has_null, join_mode, size, min_value, max_value, bitset,
                min_max,
            )))
        }
        RF_TYPE_EMPTY_FILTER => {
            let has_null = read_bool(data, &mut offset)?;
            let size = read_u64_le(data, &mut offset)?;
            let join_mode = read_i8(data, &mut offset)?;
            let min_max = RuntimeMinMaxFilter::decode(ltype, data, &mut offset)?;
            Ok(RuntimeMembershipFilter::Empty(RuntimeEmptyFilter::new(
                filter_id, slot_id, ltype, has_null, join_mode, size, min_max,
            )))
        }
        _ => Err(format!("unsupported runtime filter type: {rf_type}")),
    }
}

/// Encode a runtime bloom filter into StarRocks-compatible wire payload.
pub(crate) fn encode_starrocks_bloom_filter(
    filter: &RuntimeBloomFilter,
) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    buf.push(RF_VERSION_V3);
    buf.push(RF_TYPE_BLOOM_FILTER);
    buf.extend_from_slice(&filter.ltype().0.to_le_bytes());
    buf.push(if filter.has_null() { 1 } else { 0 });
    buf.extend_from_slice(&filter.size().to_le_bytes());
    buf.extend_from_slice(&(0u64).to_le_bytes()); // num_partitions
    buf.push(filter.join_mode() as u8);
    if let Some(bf) = filter.bf() {
        bf.serialize(&mut buf);
    } else {
        let empty = SimdBlockFilter::empty();
        empty.serialize(&mut buf);
    }
    filter.min_max().encode_into(&mut buf)?;
    Ok(buf)
}

/// Encode a runtime bitset filter into StarRocks-compatible wire payload.
pub(crate) fn encode_starrocks_bitset_filter(
    filter: &RuntimeBitsetFilter,
) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    buf.push(RF_VERSION_V3);
    buf.push(RF_TYPE_BITSET_FILTER);
    buf.extend_from_slice(&filter.ltype().0.to_le_bytes());
    buf.push(if filter.has_null() { 1 } else { 0 });
    buf.extend_from_slice(&filter.size().to_le_bytes());
    buf.push(filter.join_mode() as u8);
    write_min_max_i64(
        &filter.ltype(),
        filter.min_value(),
        filter.max_value(),
        &mut buf,
    )?;
    let bitset = filter.bitset();
    buf.extend_from_slice(&(bitset.len() as u64).to_le_bytes());
    buf.extend_from_slice(bitset);
    filter.min_max().encode_into(&mut buf)?;
    Ok(buf)
}

/// Encode an always-empty runtime filter payload in StarRocks wire format.
pub(crate) fn encode_starrocks_empty_filter(
    filter: &RuntimeEmptyFilter,
) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    buf.push(RF_VERSION_V3);
    buf.push(RF_TYPE_EMPTY_FILTER);
    buf.extend_from_slice(&filter.ltype().0.to_le_bytes());
    buf.push(if filter.has_null() { 1 } else { 0 });
    buf.extend_from_slice(&filter.size().to_le_bytes());
    buf.push(filter.join_mode() as u8);
    filter.min_max().encode_into(&mut buf)?;
    Ok(buf)
}

/// Read one u8 value from codec payload and advance the decode offset.
pub(super) fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, String> {
    if data.len() < *offset + 1 {
        return Err("runtime filter data truncated".to_string());
    }
    let v = data[*offset];
    *offset += 1;
    Ok(v)
}

fn read_bool(data: &[u8], offset: &mut usize) -> Result<bool, String> {
    Ok(read_u8(data, offset)? != 0)
}

/// Read one i8 value from codec payload and advance the decode offset.
pub(super) fn read_i8(data: &[u8], offset: &mut usize) -> Result<i8, String> {
    let v = read_u8(data, offset)?;
    Ok(v as i8)
}

/// Read one little-endian i16 value from codec payload and advance the decode offset.
pub(super) fn read_i16_le(data: &[u8], offset: &mut usize) -> Result<i16, String> {
    if data.len() < *offset + 2 {
        return Err("runtime filter data truncated".to_string());
    }
    let v = i16::from_le_bytes([data[*offset], data[*offset + 1]]);
    *offset += 2;
    Ok(v)
}

pub(in crate::exec::runtime_filter) fn read_i32_le(
    data: &[u8],
    offset: &mut usize,
) -> Result<i32, String> {
    if data.len() < *offset + 4 {
        return Err("runtime filter data truncated".to_string());
    }
    let v = i32::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
    ]);
    *offset += 4;
    Ok(v)
}

pub(in crate::exec::runtime_filter) fn read_u32_le(
    data: &[u8],
    offset: &mut usize,
) -> Result<u32, String> {
    Ok(read_i32_le(data, offset)? as u32)
}

/// Read one little-endian i64 value from codec payload and advance the decode offset.
pub(super) fn read_i64_le(data: &[u8], offset: &mut usize) -> Result<i64, String> {
    if data.len() < *offset + 8 {
        return Err("runtime filter data truncated".to_string());
    }
    let v = i64::from_le_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[*offset + 3],
        data[*offset + 4],
        data[*offset + 5],
        data[*offset + 6],
        data[*offset + 7],
    ]);
    *offset += 8;
    Ok(v)
}

fn read_i128_le(data: &[u8], offset: &mut usize) -> Result<i128, String> {
    if data.len() < *offset + 16 {
        return Err("runtime filter data truncated".to_string());
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&data[*offset..*offset + 16]);
    *offset += 16;
    Ok(i128::from_le_bytes(buf))
}

/// Read one little-endian u64 value from codec payload and advance the decode offset.
pub(super) fn read_u64_le(data: &[u8], offset: &mut usize) -> Result<u64, String> {
    Ok(read_i64_le(data, offset)? as u64)
}

fn read_varchar(data: &[u8], offset: &mut usize) -> Result<String, String> {
    let len = read_i32_le(data, offset)?;
    if len < 0 {
        return Err("runtime filter varchar length negative".to_string());
    }
    let len = len as usize;
    if data.len() < *offset + len {
        return Err("runtime filter data truncated".to_string());
    }
    let bytes = data[*offset..*offset + len].to_vec();
    *offset += len;
    String::from_utf8(bytes).map_err(|e| format!("runtime filter varchar not utf8: {e}"))
}

fn write_min_max_i64(
    ltype: &crate::thrift::types::TPrimitiveType,
    min_value: i64,
    max_value: i64,
    buf: &mut Vec<u8>,
) -> Result<(), String> {
    use crate::thrift::types;
    if *ltype == types::TPrimitiveType::BOOLEAN {
        if !(0..=1).contains(&min_value) || !(0..=1).contains(&max_value) {
            return Err("runtime bitset filter boolean min/max out of range".to_string());
        }
        buf.push(min_value as u8);
        buf.push(max_value as u8);
        return Ok(());
    }
    if *ltype == types::TPrimitiveType::TINYINT {
        let min = i8::try_from(min_value)
            .map_err(|_| "runtime bitset filter tinyint min out of range".to_string())?;
        let max = i8::try_from(max_value)
            .map_err(|_| "runtime bitset filter tinyint max out of range".to_string())?;
        buf.push(min as u8);
        buf.push(max as u8);
        return Ok(());
    }
    if *ltype == types::TPrimitiveType::SMALLINT {
        let min = i16::try_from(min_value)
            .map_err(|_| "runtime bitset filter smallint min out of range".to_string())?;
        let max = i16::try_from(max_value)
            .map_err(|_| "runtime bitset filter smallint max out of range".to_string())?;
        buf.extend_from_slice(&min.to_le_bytes());
        buf.extend_from_slice(&max.to_le_bytes());
        return Ok(());
    }
    if *ltype == types::TPrimitiveType::INT {
        let min = i32::try_from(min_value)
            .map_err(|_| "runtime bitset filter int min out of range".to_string())?;
        let max = i32::try_from(max_value)
            .map_err(|_| "runtime bitset filter int max out of range".to_string())?;
        buf.extend_from_slice(&min.to_le_bytes());
        buf.extend_from_slice(&max.to_le_bytes());
        return Ok(());
    }
    if *ltype == types::TPrimitiveType::DECIMAL32 {
        let min = i32::try_from(min_value)
            .map_err(|_| "runtime bitset filter decimal32 min out of range".to_string())?;
        let max = i32::try_from(max_value)
            .map_err(|_| "runtime bitset filter decimal32 max out of range".to_string())?;
        buf.extend_from_slice(&min.to_le_bytes());
        buf.extend_from_slice(&max.to_le_bytes());
        return Ok(());
    }
    if *ltype == types::TPrimitiveType::BIGINT {
        buf.extend_from_slice(&min_value.to_le_bytes());
        buf.extend_from_slice(&max_value.to_le_bytes());
        return Ok(());
    }
    if *ltype == types::TPrimitiveType::DECIMAL64 {
        buf.extend_from_slice(&min_value.to_le_bytes());
        buf.extend_from_slice(&max_value.to_le_bytes());
        return Ok(());
    }
    if *ltype == types::TPrimitiveType::LARGEINT {
        return Err(unsupported_runtime_bitset_filter_type(ltype));
    }
    if *ltype == types::TPrimitiveType::DECIMAL128
        || *ltype == types::TPrimitiveType::DECIMAL
        || *ltype == types::TPrimitiveType::DECIMALV2
    {
        return Err(unsupported_runtime_bitset_filter_type(ltype));
    }
    if *ltype == types::TPrimitiveType::DATE {
        let min = i32::try_from(min_value)
            .map_err(|_| "runtime bitset filter date min out of range".to_string())?;
        let max = i32::try_from(max_value)
            .map_err(|_| "runtime bitset filter date max out of range".to_string())?;
        buf.extend_from_slice(&min.to_le_bytes());
        buf.extend_from_slice(&max.to_le_bytes());
        return Ok(());
    }
    if *ltype == types::TPrimitiveType::DATETIME || *ltype == types::TPrimitiveType::TIME {
        buf.extend_from_slice(&min_value.to_le_bytes());
        buf.extend_from_slice(&max_value.to_le_bytes());
        return Ok(());
    }
    Err(format!(
        "unsupported runtime bitset filter primitive type: {:?}",
        ltype
    ))
}

fn read_min_max_i64(
    ltype: &crate::thrift::types::TPrimitiveType,
    data: &[u8],
    offset: &mut usize,
) -> Result<(i64, i64), String> {
    use crate::thrift::types;
    if *ltype == types::TPrimitiveType::BOOLEAN {
        let min = read_u8(data, offset)? as i64;
        let max = read_u8(data, offset)? as i64;
        return Ok((min, max));
    }
    if *ltype == types::TPrimitiveType::TINYINT {
        let min = read_i8(data, offset)? as i64;
        let max = read_i8(data, offset)? as i64;
        return Ok((min, max));
    }
    if *ltype == types::TPrimitiveType::SMALLINT {
        let min = read_i16_le(data, offset)? as i64;
        let max = read_i16_le(data, offset)? as i64;
        return Ok((min, max));
    }
    if *ltype == types::TPrimitiveType::INT {
        let min = read_i32_le(data, offset)? as i64;
        let max = read_i32_le(data, offset)? as i64;
        return Ok((min, max));
    }
    if *ltype == types::TPrimitiveType::DECIMAL32 {
        let min = read_i32_le(data, offset)? as i64;
        let max = read_i32_le(data, offset)? as i64;
        return Ok((min, max));
    }
    if *ltype == types::TPrimitiveType::BIGINT {
        let min = read_i64_le(data, offset)?;
        let max = read_i64_le(data, offset)?;
        return Ok((min, max));
    }
    if *ltype == types::TPrimitiveType::DECIMAL64 {
        let min = read_i64_le(data, offset)?;
        let max = read_i64_le(data, offset)?;
        return Ok((min, max));
    }
    if *ltype == types::TPrimitiveType::LARGEINT {
        return Err(unsupported_runtime_bitset_filter_type(ltype));
    }
    if *ltype == types::TPrimitiveType::DECIMAL128
        || *ltype == types::TPrimitiveType::DECIMAL
        || *ltype == types::TPrimitiveType::DECIMALV2
    {
        return Err(unsupported_runtime_bitset_filter_type(ltype));
    }
    if *ltype == types::TPrimitiveType::DATE {
        let min = read_i32_le(data, offset)? as i64;
        let max = read_i32_le(data, offset)? as i64;
        return Ok((min, max));
    }
    if *ltype == types::TPrimitiveType::DATETIME || *ltype == types::TPrimitiveType::TIME {
        let min = read_i64_le(data, offset)?;
        let max = read_i64_le(data, offset)?;
        return Ok((min, max));
    }
    Err(format!(
        "unsupported runtime bitset filter primitive type: {:?}",
        ltype
    ))
}

fn unsupported_runtime_bitset_filter_type(ltype: &crate::thrift::types::TPrimitiveType) -> String {
    use crate::thrift::types;
    let name = if *ltype == types::TPrimitiveType::LARGEINT {
        "LARGEINT"
    } else if *ltype == types::TPrimitiveType::DECIMAL128 {
        "DECIMAL128"
    } else if *ltype == types::TPrimitiveType::DECIMAL {
        "DECIMAL"
    } else if *ltype == types::TPrimitiveType::DECIMALV2 {
        "DECIMALV2"
    } else {
        return format!(
            "unsupported runtime bitset filter primitive type: {:?}",
            ltype
        );
    };
    format!("runtime bitset filter does not support {name}")
}

#[cfg(test)]
mod tests {
    use hashbrown::HashSet;

    use super::{
        RF_TYPE_BITSET_FILTER, RF_TYPE_IN_FILTER, RF_VERSION_V3, decode_starrocks_in_filter,
        decode_starrocks_membership_filter, encode_starrocks_bitset_filter,
        encode_starrocks_in_filter,
    };
    use crate::common::ids::SlotId;
    use crate::exec::runtime_filter::min_max::MinMaxValue;
    use crate::exec::runtime_filter::{
        RuntimeBitsetFilter, RuntimeInFilter, RuntimeInFilterValues, RuntimeMembershipFilter,
        RuntimeMinMaxFilter,
    };

    #[test]
    fn test_largeint_in_filter_roundtrip() {
        let mut values = HashSet::new();
        values.insert(i128::MIN + 7);
        values.insert(-1);
        values.insert(0);
        values.insert(i128::MAX - 9);

        let filter = RuntimeInFilter::new(
            1001,
            SlotId::new(11),
            RuntimeInFilterValues::LargeInt(values.clone()),
        );
        let encoded = encode_starrocks_in_filter(&filter).unwrap();
        let decoded = decode_starrocks_in_filter(1001, SlotId::new(11), None, &encoded).unwrap();
        match decoded.values() {
            RuntimeInFilterValues::LargeInt(decoded_values) => {
                assert_eq!(&values, decoded_values);
            }
            other => panic!("expected LargeInt runtime in-filter, got {:?}", other),
        }
    }

    #[test]
    fn varchar_in_filter_round_trips() {
        let mut set = HashSet::new();
        set.insert(String::new()); // empty string
        set.insert("a".to_string());
        set.insert("中文".to_string()); // multibyte
        let filter =
            RuntimeInFilter::new(7, SlotId::new(3), RuntimeInFilterValues::Utf8(set.clone()));
        let encoded = encode_starrocks_in_filter(&filter).expect("encode");
        assert_eq!(encoded[0], RF_VERSION_V3);
        assert_eq!(encoded[1], RF_TYPE_IN_FILTER);
        let decoded =
            decode_starrocks_in_filter(7, SlotId::new(3), None, &encoded).expect("decode");
        match decoded.values() {
            RuntimeInFilterValues::Utf8(got) => assert_eq!(got, &set),
            other => panic!("expected Utf8, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_in_filter_round_trips() {
        use crate::exec::runtime_filter::{RuntimeInFilter, RuntimeInFilterValues};
        let mut set = HashSet::new();
        set.insert(-1i128);
        set.insert(0i128);
        set.insert(i128::MAX);
        set.insert(i128::MIN);
        let filter = RuntimeInFilter::new(
            9,
            SlotId::new(4),
            RuntimeInFilterValues::Decimal128 {
                values: set.clone(),
                precision: 38,
                scale: 6,
            },
        );
        let encoded = encode_starrocks_in_filter(&filter).expect("encode");
        let dt = arrow::datatypes::DataType::Decimal128(38, 6);
        let decoded =
            decode_starrocks_in_filter(9, SlotId::new(4), Some(&dt), &encoded).expect("decode");
        match decoded.values() {
            RuntimeInFilterValues::Decimal128 {
                values,
                precision,
                scale,
            } => {
                assert_eq!(values, &set);
                assert_eq!(*precision, 38);
                assert_eq!(*scale, 6);
            }
            other => panic!("expected Decimal128, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_decode_without_type_errors() {
        use crate::exec::runtime_filter::{RuntimeInFilter, RuntimeInFilterValues};
        let mut set = HashSet::new();
        set.insert(5i128);
        let filter = RuntimeInFilter::new(
            9,
            SlotId::new(4),
            RuntimeInFilterValues::Decimal128 {
                values: set,
                precision: 18,
                scale: 2,
            },
        );
        let encoded = encode_starrocks_in_filter(&filter).expect("encode");
        // No probe column type => decimal decode must fail-fast (not fabricate p/s).
        assert!(decode_starrocks_in_filter(9, SlotId::new(4), None, &encoded).is_err());
    }

    #[test]
    fn varchar_in_filter_wire_layout() {
        let mut set = HashSet::new();
        set.insert("ab".to_string());
        let filter = RuntimeInFilter::new(1, SlotId::new(1), RuntimeInFilterValues::Utf8(set));
        let buf = encode_starrocks_in_filter(&filter).expect("encode");
        // header: [0x4][type=4][ltype i32 LE = VARCHAR(15)][count u32 LE = 1]
        assert_eq!(buf[0], RF_VERSION_V3);
        assert_eq!(buf[1], RF_TYPE_IN_FILTER);
        assert_eq!(
            i32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]),
            crate::thrift::types::TPrimitiveType::VARCHAR.0
        );
        assert_eq!(u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]), 1);
        // value: [int32 len=2 LE]['a','b']
        assert_eq!(i32::from_le_bytes([buf[10], buf[11], buf[12], buf[13]]), 2);
        assert_eq!(&buf[14..16], b"ab");
    }

    #[test]
    fn decimal128_in_filter_wire_layout() {
        let mut set = HashSet::new();
        set.insert(7i128);
        let filter = RuntimeInFilter::new(
            1,
            SlotId::new(1),
            RuntimeInFilterValues::Decimal128 {
                values: set,
                precision: 10,
                scale: 2,
            },
        );
        let buf = encode_starrocks_in_filter(&filter).expect("encode");
        assert_eq!(buf[0], RF_VERSION_V3);
        assert_eq!(buf[1], RF_TYPE_IN_FILTER);
        assert_eq!(
            i32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]),
            crate::thrift::types::TPrimitiveType::DECIMAL128.0
        );
        assert_eq!(u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]), 1);
        // value: 16B i128 LE = 7
        let mut v = [0u8; 16];
        v.copy_from_slice(&buf[10..26]);
        assert_eq!(i128::from_le_bytes(v), 7);
    }

    #[test]
    fn decimal128_decode_with_wrong_type_errors() {
        let mut set = HashSet::new();
        set.insert(5i128);
        let filter = RuntimeInFilter::new(
            9,
            SlotId::new(4),
            RuntimeInFilterValues::Decimal128 {
                values: set,
                precision: 18,
                scale: 2,
            },
        );
        let encoded = encode_starrocks_in_filter(&filter).expect("encode");
        let wrong = arrow::datatypes::DataType::Int64;
        assert!(decode_starrocks_in_filter(9, SlotId::new(4), Some(&wrong), &encoded).is_err());
    }

    #[test]
    fn decimal64_bitset_filter_round_trips() {
        let ltype = crate::thrift::types::TPrimitiveType::DECIMAL64;
        let min_value = -123_456_789i64;
        let max_value = 987_654_321i64;
        let bitset = vec![0b1010_0101, 0b0101_1010, 0xff];
        let min_max = RuntimeMinMaxFilter::new(
            ltype,
            true,
            MinMaxValue::Decimal128(min_value as i128),
            MinMaxValue::Decimal128(max_value as i128),
        );
        let filter = RuntimeBitsetFilter::new(
            11,
            SlotId::new(5),
            ltype,
            true,
            7,
            123,
            min_value,
            max_value,
            bitset.clone(),
            min_max,
        );

        let encoded = encode_starrocks_bitset_filter(&filter).expect("encode");
        let decoded =
            decode_starrocks_membership_filter(11, SlotId::new(5), &encoded).expect("decode");

        match decoded {
            RuntimeMembershipFilter::Bitset(decoded) => {
                assert_eq!(decoded.ltype(), ltype);
                assert!(decoded.has_null());
                assert_eq!(decoded.size(), 123);
                assert_eq!(decoded.join_mode(), 7);
                assert_eq!(decoded.min_value(), min_value);
                assert_eq!(decoded.max_value(), max_value);
                assert_eq!(decoded.bitset(), bitset.as_slice());
                assert!(decoded.min_max().has_min_max());
                match decoded.min_max().min() {
                    MinMaxValue::Decimal128(min) => assert_eq!(*min, min_value as i128),
                    other => panic!("expected Decimal128 min, got {other:?}"),
                }
                match decoded.min_max().max() {
                    MinMaxValue::Decimal128(max) => assert_eq!(*max, max_value as i128),
                    other => panic!("expected Decimal128 max, got {other:?}"),
                }
            }
            other => panic!("expected Bitset runtime filter, got {other:?}"),
        }
    }

    #[test]
    fn decimal128_bitset_payload_is_rejected() {
        let mut payload = Vec::new();
        payload.push(RF_VERSION_V3);
        payload.push(RF_TYPE_BITSET_FILTER);
        payload.extend_from_slice(
            &crate::thrift::types::TPrimitiveType::DECIMAL128
                .0
                .to_le_bytes(),
        );
        payload.push(0); // has_null
        payload.extend_from_slice(&0u64.to_le_bytes()); // size
        payload.push(0); // join_mode
        payload.extend_from_slice(&(-1i128).to_le_bytes()); // min_value
        payload.extend_from_slice(&(1i128).to_le_bytes()); // max_value
        payload.extend_from_slice(&0u64.to_le_bytes()); // bitset_size

        let err = decode_starrocks_membership_filter(12, SlotId::new(6), &payload).unwrap_err();
        assert!(
            err.contains("does not support DECIMAL128")
                || err.contains("unsupported runtime bitset filter decimal type")
        );
    }

    #[test]
    fn test_reject_bitset_largeint_payload() {
        let mut payload = Vec::new();
        payload.push(RF_VERSION_V3);
        payload.push(RF_TYPE_BITSET_FILTER);
        payload.extend_from_slice(
            &crate::thrift::types::TPrimitiveType::LARGEINT
                .0
                .to_le_bytes(),
        );
        payload.push(0); // has_null
        payload.extend_from_slice(&0u64.to_le_bytes()); // size
        payload.push(0); // join_mode

        let err = decode_starrocks_membership_filter(7, SlotId::new(3), &payload).unwrap_err();
        assert!(err.contains("does not support LARGEINT"));
    }
}
