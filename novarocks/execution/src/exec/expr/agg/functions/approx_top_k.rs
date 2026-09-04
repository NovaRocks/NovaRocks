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
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, ListArray, StructArray, new_null_array};
use arrow::datatypes::DataType;
use arrow_buffer::OffsetBuffer;

use crate::exec::expr::agg::{
    AggregateAllocator, AggregateHashMap, AggregateVec, RetainedMemoryPolicy, aggregate_hash_map,
};
use crate::exec::node::aggregate::AggFunction;
use crate::runtime::mem_tracker::MemTracker;

use super::super::*;
use super::AggregateFunction;
use super::common::{
    AggScalarValue, TrackedAggScalarValue, aggregate_vec_with_capacity, build_scalar_array,
    scalar_from_array, tracked_optional_key_fingerprint, tracked_scalar_from_array,
    tracked_scalar_to_output,
};

pub(super) struct ApproxTopKAgg;

const DEFAULT_K: usize = 5;
const MAX_COUNTER_NUM: usize = 100_000;

struct TopKEntry {
    value: Option<TrackedAggScalarValue>,
    count: i64,
}

#[cfg(test)]
struct DecodedTopKEntry {
    value: Option<AggScalarValue>,
    count: i64,
}

struct TrackedDecodedTopK {
    k: usize,
    counter_num: usize,
    entries: AggregateVec<TopKEntry>,
}

struct ApproxTopKState {
    allocator: AggregateAllocator,
    initialized: bool,
    k: usize,
    counter_num: usize,
    counts: AggregateHashMap<AggregateVec<u8>, TopKEntry>,
}

impl ApproxTopKState {
    fn new(tracker: Arc<MemTracker>) -> Self {
        let allocator = AggregateAllocator::new(tracker);
        Self {
            counts: aggregate_hash_map(allocator.clone()),
            allocator,
            initialized: false,
            k: DEFAULT_K,
            counter_num: default_counter_num(DEFAULT_K),
        }
    }
}

fn default_counter_num(k: usize) -> usize {
    (2 * k).clamp(100, MAX_COUNTER_NUM)
}

fn clamp_k(v: i64) -> Option<usize> {
    if v <= 0 {
        return None;
    }
    let v = usize::try_from(v).ok()?;
    if v == 0 || v > MAX_COUNTER_NUM {
        return None;
    }
    Some(v)
}

fn clamp_counter_num(v: i64, k: usize) -> Option<usize> {
    if v <= 0 {
        return None;
    }
    let v = usize::try_from(v).ok()?;
    if v == 0 || v > MAX_COUNTER_NUM {
        return None;
    }
    Some(v.max(k))
}

fn scalar_to_i64(value: &Option<AggScalarValue>) -> Option<i64> {
    match value {
        Some(AggScalarValue::Int64(v)) => Some(*v),
        Some(AggScalarValue::Float64(v)) => Some(*v as i64),
        Some(AggScalarValue::Decimal128(v)) => i64::try_from(*v).ok(),
        Some(AggScalarValue::Decimal256(v)) => v.to_string().parse::<i64>().ok(),
        _ => None,
    }
}

fn encode_optional_scalar(value: &Option<AggScalarValue>) -> Vec<u8> {
    fn encode_into(buf: &mut Vec<u8>, value: &AggScalarValue) {
        match value {
            AggScalarValue::Bool(v) => {
                buf.push(1);
                buf.push(*v as u8);
            }
            AggScalarValue::Int64(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggScalarValue::Float64(v) => {
                buf.push(3);
                buf.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            AggScalarValue::Utf8(v) => {
                buf.push(4);
                let len = u32::try_from(v.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            AggScalarValue::Date32(v) => {
                buf.push(5);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggScalarValue::Timestamp(v) => {
                buf.push(6);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggScalarValue::Decimal128(v) => {
                buf.push(7);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            AggScalarValue::Decimal256(v) => {
                buf.push(11);
                let text = v.to_string();
                let len = u32::try_from(text.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(text.as_bytes());
            }
            AggScalarValue::Binary(v) => {
                buf.push(12);
                let len = u32::try_from(v.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(v);
            }
            AggScalarValue::Struct(items) => {
                buf.push(8);
                let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v);
                        }
                        None => buf.push(0),
                    }
                }
            }
            AggScalarValue::Map(items) => {
                buf.push(9);
                let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                for (k, v) in items {
                    match k {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v);
                        }
                        None => buf.push(0),
                    }
                    match v {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v);
                        }
                        None => buf.push(0),
                    }
                }
            }
            AggScalarValue::List(items) => {
                buf.push(10);
                let len = u32::try_from(items.len()).unwrap_or(u32::MAX);
                buf.extend_from_slice(&len.to_le_bytes());
                for item in items {
                    match item {
                        Some(v) => {
                            buf.push(1);
                            encode_into(buf, v);
                        }
                        None => buf.push(0),
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    match value {
        Some(v) => {
            out.push(1);
            encode_into(&mut out, v);
        }
        None => out.push(0),
    }
    out
}

#[cfg(test)]
fn decode_optional_scalar(bytes: &[u8]) -> Result<Option<AggScalarValue>, String> {
    fn need_len(bytes: &[u8], pos: usize, need: usize, label: &str) -> Result<(), String> {
        if pos + need > bytes.len() {
            return Err(format!("approx_top_k decode {}: buffer too short", label));
        }
        Ok(())
    }

    fn read_u32(bytes: &[u8], pos: &mut usize, label: &str) -> Result<u32, String> {
        need_len(bytes, *pos, 4, label)?;
        let v = u32::from_le_bytes(
            bytes[*pos..*pos + 4]
                .try_into()
                .map_err(|_| format!("approx_top_k decode {}: invalid u32", label))?,
        );
        *pos += 4;
        Ok(v)
    }

    fn decode_value(bytes: &[u8], pos: &mut usize) -> Result<AggScalarValue, String> {
        need_len(bytes, *pos, 1, "tag")?;
        let tag = bytes[*pos];
        *pos += 1;
        match tag {
            1 => {
                need_len(bytes, *pos, 1, "bool")?;
                let v = bytes[*pos] != 0;
                *pos += 1;
                Ok(AggScalarValue::Bool(v))
            }
            2 => {
                need_len(bytes, *pos, 8, "int64")?;
                let v = i64::from_le_bytes(
                    bytes[*pos..*pos + 8]
                        .try_into()
                        .map_err(|_| "approx_top_k decode int64: invalid bytes".to_string())?,
                );
                *pos += 8;
                Ok(AggScalarValue::Int64(v))
            }
            3 => {
                need_len(bytes, *pos, 8, "float64")?;
                let bits = u64::from_le_bytes(
                    bytes[*pos..*pos + 8]
                        .try_into()
                        .map_err(|_| "approx_top_k decode float64: invalid bytes".to_string())?,
                );
                *pos += 8;
                Ok(AggScalarValue::Float64(f64::from_bits(bits)))
            }
            4 => {
                let len = read_u32(bytes, pos, "utf8_len")? as usize;
                need_len(bytes, *pos, len, "utf8")?;
                let v = std::str::from_utf8(&bytes[*pos..*pos + len])
                    .map_err(|e| format!("approx_top_k decode utf8: {}", e))?
                    .to_string();
                *pos += len;
                Ok(AggScalarValue::Utf8(v))
            }
            5 => {
                need_len(bytes, *pos, 4, "date32")?;
                let v = i32::from_le_bytes(
                    bytes[*pos..*pos + 4]
                        .try_into()
                        .map_err(|_| "approx_top_k decode date32: invalid bytes".to_string())?,
                );
                *pos += 4;
                Ok(AggScalarValue::Date32(v))
            }
            6 => {
                need_len(bytes, *pos, 8, "timestamp")?;
                let v = i64::from_le_bytes(
                    bytes[*pos..*pos + 8]
                        .try_into()
                        .map_err(|_| "approx_top_k decode timestamp: invalid bytes".to_string())?,
                );
                *pos += 8;
                Ok(AggScalarValue::Timestamp(v))
            }
            7 => {
                need_len(bytes, *pos, 16, "decimal128")?;
                let v =
                    i128::from_le_bytes(bytes[*pos..*pos + 16].try_into().map_err(|_| {
                        "approx_top_k decode decimal128: invalid bytes".to_string()
                    })?);
                *pos += 16;
                Ok(AggScalarValue::Decimal128(v))
            }
            8 => {
                let len = read_u32(bytes, pos, "struct_len")? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    need_len(bytes, *pos, 1, "struct_item_flag")?;
                    let present = bytes[*pos];
                    *pos += 1;
                    if present == 0 {
                        out.push(None);
                    } else {
                        out.push(Some(decode_value(bytes, pos)?));
                    }
                }
                Ok(AggScalarValue::Struct(out))
            }
            9 => {
                let len = read_u32(bytes, pos, "map_len")? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    need_len(bytes, *pos, 1, "map_key_flag")?;
                    let key_present = bytes[*pos];
                    *pos += 1;
                    let key = if key_present == 0 {
                        None
                    } else {
                        Some(decode_value(bytes, pos)?)
                    };
                    need_len(bytes, *pos, 1, "map_value_flag")?;
                    let value_present = bytes[*pos];
                    *pos += 1;
                    let value = if value_present == 0 {
                        None
                    } else {
                        Some(decode_value(bytes, pos)?)
                    };
                    out.push((key, value));
                }
                Ok(AggScalarValue::Map(out))
            }
            10 => {
                let len = read_u32(bytes, pos, "list_len")? as usize;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    need_len(bytes, *pos, 1, "list_item_flag")?;
                    let present = bytes[*pos];
                    *pos += 1;
                    if present == 0 {
                        out.push(None);
                    } else {
                        out.push(Some(decode_value(bytes, pos)?));
                    }
                }
                Ok(AggScalarValue::List(out))
            }
            other => Err(format!("approx_top_k decode: unsupported tag {}", other)),
        }
    }

    if bytes.is_empty() {
        return Err("approx_top_k decode: empty payload".to_string());
    }
    let mut pos = 0usize;
    let present = bytes[pos];
    pos += 1;
    if present == 0 {
        if pos != bytes.len() {
            return Err("approx_top_k decode: trailing bytes on null value".to_string());
        }
        return Ok(None);
    }
    let value = decode_value(bytes, &mut pos)?;
    if pos != bytes.len() {
        return Err("approx_top_k decode: trailing bytes".to_string());
    }
    Ok(Some(value))
}

fn decode_optional_scalar_tracked(
    bytes: &[u8],
    allocator: &AggregateAllocator,
) -> Result<Option<TrackedAggScalarValue>, String> {
    fn need(bytes: &[u8], pos: usize, len: usize, label: &str) -> Result<(), String> {
        if pos.checked_add(len).is_none_or(|end| end > bytes.len()) {
            Err(format!("approx_top_k decode {label}: buffer too short"))
        } else {
            Ok(())
        }
    }
    fn read_u32(bytes: &[u8], pos: &mut usize, label: &str) -> Result<usize, String> {
        need(bytes, *pos, 4, label)?;
        let value = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().unwrap()) as usize;
        *pos += 4;
        Ok(value)
    }
    fn decode_optional(
        bytes: &[u8],
        pos: &mut usize,
        allocator: &AggregateAllocator,
    ) -> Result<Option<TrackedAggScalarValue>, String> {
        need(bytes, *pos, 1, "optional marker")?;
        let present = bytes[*pos];
        *pos += 1;
        match present {
            0 => Ok(None),
            1 => decode_value(bytes, pos, allocator).map(Some),
            _ => Err("approx_top_k decode: invalid optional marker".to_string()),
        }
    }
    fn decode_values(
        bytes: &[u8],
        pos: &mut usize,
        allocator: &AggregateAllocator,
        label: &str,
    ) -> Result<AggregateVec<Option<TrackedAggScalarValue>>, String> {
        let len = read_u32(bytes, pos, label)?;
        let mut values =
            aggregate_vec_with_capacity(allocator, len, "reserve approx_top_k nested scalar")?;
        for _ in 0..len {
            values.push(decode_optional(bytes, pos, allocator)?);
        }
        Ok(values)
    }
    fn decode_value(
        bytes: &[u8],
        pos: &mut usize,
        allocator: &AggregateAllocator,
    ) -> Result<TrackedAggScalarValue, String> {
        need(bytes, *pos, 1, "tag")?;
        let tag = bytes[*pos];
        *pos += 1;
        macro_rules! fixed {
            ($len:expr, $label:literal, $ctor:expr) => {{
                need(bytes, *pos, $len, $label)?;
                let raw: [u8; $len] = bytes[*pos..*pos + $len].try_into().unwrap();
                *pos += $len;
                $ctor(raw)
            }};
        }
        Ok(match tag {
            1 => {
                need(bytes, *pos, 1, "bool")?;
                let value = bytes[*pos] != 0;
                *pos += 1;
                TrackedAggScalarValue::Bool(value)
            }
            2 => fixed!(8, "int64", |raw| TrackedAggScalarValue::Int64(
                i64::from_le_bytes(raw)
            )),
            3 => fixed!(8, "float64", |raw| TrackedAggScalarValue::Float64(
                f64::from_bits(u64::from_le_bytes(raw))
            )),
            4 | 12 => {
                let len = read_u32(bytes, pos, "byte length")?;
                need(bytes, *pos, len, "byte payload")?;
                let value = crate::exec::expr::agg::aggregate_bytes(
                    allocator.clone(),
                    &bytes[*pos..*pos + len],
                )?;
                *pos += len;
                if tag == 4 {
                    std::str::from_utf8(&value)
                        .map_err(|error| format!("approx_top_k decode utf8: {error}"))?;
                    TrackedAggScalarValue::Utf8(value)
                } else {
                    TrackedAggScalarValue::Binary(value)
                }
            }
            5 => fixed!(4, "date32", |raw| TrackedAggScalarValue::Date32(
                i32::from_le_bytes(raw)
            )),
            6 => fixed!(8, "timestamp", |raw| TrackedAggScalarValue::Timestamp(
                i64::from_le_bytes(raw)
            )),
            7 => fixed!(16, "decimal128", |raw| {
                TrackedAggScalarValue::Decimal128(i128::from_le_bytes(raw))
            }),
            8 => TrackedAggScalarValue::Struct(decode_values(
                bytes,
                pos,
                allocator,
                "struct length",
            )?),
            9 => {
                let len = read_u32(bytes, pos, "map length")?;
                let mut entries =
                    aggregate_vec_with_capacity(allocator, len, "reserve approx_top_k map scalar")?;
                for _ in 0..len {
                    entries.push((
                        decode_optional(bytes, pos, allocator)?,
                        decode_optional(bytes, pos, allocator)?,
                    ));
                }
                TrackedAggScalarValue::Map(entries)
            }
            10 => TrackedAggScalarValue::List(decode_values(bytes, pos, allocator, "list length")?),
            11 => {
                let len = read_u32(bytes, pos, "decimal256 length")?;
                need(bytes, *pos, len, "decimal256 payload")?;
                let text = std::str::from_utf8(&bytes[*pos..*pos + len])
                    .map_err(|error| format!("approx_top_k decode decimal256: {error}"))?;
                *pos += len;
                TrackedAggScalarValue::Decimal256(
                    text.parse()
                        .map_err(|error| format!("approx_top_k decode decimal256: {error}"))?,
                )
            }
            other => return Err(format!("approx_top_k decode: unknown tag {other}")),
        })
    }

    let mut pos = 0;
    let value = decode_optional(bytes, &mut pos, allocator)?;
    if pos != bytes.len() {
        return Err("approx_top_k decode: trailing bytes".to_string());
    }
    Ok(value)
}

fn ensure_initialized(state: &mut ApproxTopKState) {
    if !state.initialized {
        state.initialized = true;
        state.k = DEFAULT_K;
        state.counter_num = default_counter_num(DEFAULT_K);
    }
}

fn evict_one_counter(state: &mut ApproxTopKState) -> i64 {
    let victim = state
        .counts
        .iter()
        .min_by(|(left_key, left), (right_key, right)| {
            left.count
                .cmp(&right.count)
                .then_with(|| left_key.cmp(right_key))
        })
        .map(|(key, entry)| (key as *const AggregateVec<u8> as usize, entry.count));
    let Some((key_address, count)) = victim else {
        return 0;
    };
    state
        .counts
        .retain(|key, _| key as *const AggregateVec<u8> as usize != key_address);
    count
}

fn enforce_counter_limit(state: &mut ApproxTopKState) {
    while state.counts.len() > state.counter_num {
        evict_one_counter(state);
    }
}

fn update_one(
    state: &mut ApproxTopKState,
    value: Option<TrackedAggScalarValue>,
    count: i64,
) -> Result<(), String> {
    if count <= 0 {
        return Ok(());
    }
    ensure_initialized(state);
    let key = tracked_optional_key_fingerprint(&value, &state.allocator)?;
    if let Some(entry) = state.counts.get_mut(&key) {
        entry.count = entry.count.saturating_add(count);
        return Ok(());
    }
    let inherited = if state.counts.len() >= state.counter_num {
        evict_one_counter(state)
    } else {
        0
    };
    state.counts.try_reserve(1).map_err(|_| {
        state
            .allocator
            .allocation_error("reserve approx_top_k counter")
    })?;
    state.counts.insert(
        key,
        TopKEntry {
            value,
            count: inherited.saturating_add(count),
        },
    );
    Ok(())
}

fn serialize_state(state: &ApproxTopKState) -> Vec<u8> {
    let mut out = Vec::new();
    let k = u32::try_from(state.k).unwrap_or(DEFAULT_K as u32);
    let counter_num = u32::try_from(state.counter_num).unwrap_or(MAX_COUNTER_NUM as u32);
    out.extend_from_slice(&k.to_le_bytes());
    out.extend_from_slice(&counter_num.to_le_bytes());
    let count = u32::try_from(state.counts.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&count.to_le_bytes());
    for entry in state.counts.values() {
        let value = entry
            .value
            .as_ref()
            .map(tracked_scalar_to_output)
            .transpose()
            .expect("tracked approx_top_k value must materialize");
        let value_bytes = encode_optional_scalar(&value);
        let len = u32::try_from(value_bytes.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&value_bytes);
        out.extend_from_slice(&entry.count.to_le_bytes());
    }
    out
}

#[cfg(test)]
fn deserialize_state(bytes: &[u8]) -> Result<(usize, usize, Vec<DecodedTopKEntry>), String> {
    if bytes.len() < 12 {
        return Err("approx_top_k merge payload too short".to_string());
    }
    let mut pos = 0usize;
    let k = u32::from_le_bytes(
        bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| "approx_top_k decode k failed".to_string())?,
    ) as usize;
    pos += 4;
    let counter_num = u32::from_le_bytes(
        bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| "approx_top_k decode counter_num failed".to_string())?,
    ) as usize;
    pos += 4;
    let entry_num = u32::from_le_bytes(
        bytes[pos..pos + 4]
            .try_into()
            .map_err(|_| "approx_top_k decode entry_num failed".to_string())?,
    ) as usize;
    pos += 4;

    let mut entries = Vec::with_capacity(entry_num);
    for _ in 0..entry_num {
        if pos + 4 > bytes.len() {
            return Err("approx_top_k decode entry len failed".to_string());
        }
        let value_len = u32::from_le_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| "approx_top_k decode value_len failed".to_string())?,
        ) as usize;
        pos += 4;
        if pos + value_len + 8 > bytes.len() {
            return Err("approx_top_k decode value/count out of bounds".to_string());
        }
        let value = decode_optional_scalar(&bytes[pos..pos + value_len])?;
        pos += value_len;
        let count = i64::from_le_bytes(
            bytes[pos..pos + 8]
                .try_into()
                .map_err(|_| "approx_top_k decode count failed".to_string())?,
        );
        pos += 8;
        entries.push(DecodedTopKEntry { value, count });
    }

    if pos != bytes.len() {
        return Err("approx_top_k decode trailing bytes".to_string());
    }
    Ok((k, counter_num, entries))
}

fn deserialize_state_tracked(
    bytes: &[u8],
    allocator: &AggregateAllocator,
) -> Result<TrackedDecodedTopK, String> {
    if bytes.len() < 12 {
        return Err("approx_top_k merge payload too short".to_string());
    }
    let k = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let counter_num = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let entry_num = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if entry_num > MAX_COUNTER_NUM {
        return Err(format!(
            "approx_top_k merge entry count {entry_num} exceeds {MAX_COUNTER_NUM}"
        ));
    }
    let mut entries =
        aggregate_vec_with_capacity(allocator, entry_num, "reserve approx_top_k decoded entries")?;
    let mut pos = 12usize;
    for _ in 0..entry_num {
        let value_len_end = pos
            .checked_add(4)
            .ok_or_else(|| "approx_top_k decode entry length overflow".to_string())?;
        let value_len_bytes = bytes
            .get(pos..value_len_end)
            .ok_or_else(|| "approx_top_k decode entry len failed".to_string())?;
        let value_len = u32::from_le_bytes(value_len_bytes.try_into().unwrap()) as usize;
        pos = value_len_end;
        let value_end = pos
            .checked_add(value_len)
            .ok_or_else(|| "approx_top_k decode value length overflow".to_string())?;
        let count_end = value_end
            .checked_add(8)
            .ok_or_else(|| "approx_top_k decode count offset overflow".to_string())?;
        let value_bytes = bytes
            .get(pos..value_end)
            .ok_or_else(|| "approx_top_k decode value out of bounds".to_string())?;
        let count_bytes = bytes
            .get(value_end..count_end)
            .ok_or_else(|| "approx_top_k decode count out of bounds".to_string())?;
        entries.push(TopKEntry {
            value: decode_optional_scalar_tracked(value_bytes, allocator)?,
            count: i64::from_le_bytes(count_bytes.try_into().unwrap()),
        });
        pos = count_end;
    }
    if pos != bytes.len() {
        return Err("approx_top_k decode trailing bytes".to_string());
    }
    Ok(TrackedDecodedTopK {
        k,
        counter_num,
        entries,
    })
}

fn sorted_top_entries(state: &ApproxTopKState) -> Vec<(&AggregateVec<u8>, &TopKEntry)> {
    let mut entries = state.counts.iter().collect::<Vec<_>>();
    entries.sort_by(|(lk, lv), (rk, rv)| match rv.count.cmp(&lv.count) {
        Ordering::Equal => lk.cmp(rk),
        other => other,
    });
    if entries.len() > state.k {
        entries.truncate(state.k);
    }
    entries
}

fn output_topk_array(
    output_type: &DataType,
    offset: usize,
    group_states: &[AggStatePtr],
) -> Result<ArrayRef, String> {
    let DataType::List(list_field) = output_type else {
        return Err(format!(
            "approx_top_k output type must be LIST<STRUCT>, got {:?}",
            output_type
        ));
    };
    let DataType::Struct(struct_fields) = list_field.data_type() else {
        return Err(format!(
            "approx_top_k list element type must be STRUCT, got {:?}",
            list_field.data_type()
        ));
    };
    if struct_fields.len() < 2 {
        return Err("approx_top_k output struct must have at least 2 fields".to_string());
    }

    let mut offsets = Vec::with_capacity(group_states.len() + 1);
    offsets.push(0_i32);
    let mut current: i64 = 0;
    let mut items = Vec::new();
    let mut counts = Vec::new();
    let mut extras: Vec<Vec<Option<AggScalarValue>>> =
        (2..struct_fields.len()).map(|_| Vec::new()).collect();

    for &base in group_states {
        let state = unsafe { &*((base as *mut u8).add(offset) as *const ApproxTopKState) };
        let top_entries = sorted_top_entries(state);
        current += top_entries.len() as i64;
        if current > i32::MAX as i64 {
            return Err("approx_top_k output offset overflow".to_string());
        }
        offsets.push(current as i32);
        for (_key, entry) in top_entries {
            items.push(
                entry
                    .value
                    .as_ref()
                    .map(tracked_scalar_to_output)
                    .transpose()?,
            );
            counts.push(Some(AggScalarValue::Int64(entry.count)));
            for extra in &mut extras {
                extra.push(None);
            }
        }
    }

    let item_array = build_scalar_array(struct_fields[0].data_type(), items)?;
    let count_array = build_scalar_array(struct_fields[1].data_type(), counts)?;
    let mut struct_columns = vec![item_array, count_array];
    for (idx, field) in struct_fields.iter().enumerate().skip(2) {
        let values = std::mem::take(&mut extras[idx - 2]);
        if values.is_empty() {
            struct_columns.push(new_null_array(field.data_type(), 0));
        } else {
            struct_columns.push(build_scalar_array(field.data_type(), values)?);
        }
    }
    let struct_array = StructArray::new(struct_fields.clone(), struct_columns, None);
    let list_array = ListArray::new(
        list_field.clone(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        None,
    );
    Ok(Arc::new(list_array))
}

impl AggregateFunction for ApproxTopKAgg {
    fn build_spec_from_type(
        &self,
        func: &AggFunction,
        input_type: Option<&DataType>,
        input_is_intermediate: bool,
    ) -> Result<AggSpec, String> {
        let input_type = input_type.ok_or_else(|| "approx_top_k input type missing".to_string())?;
        if input_is_intermediate {
            let output_type = func
                .types
                .as_ref()
                .and_then(|t| t.output_type.clone())
                .ok_or_else(|| "approx_top_k output type missing".to_string())?;
            let intermediate_type = func
                .types
                .as_ref()
                .and_then(|t| t.intermediate_type.clone())
                .unwrap_or(DataType::Binary);
            return Ok(AggSpec {
                kind: AggKind::ApproxTopK,
                output_type,
                intermediate_type,
                input_arg_type: None,
                count_all: false,
            });
        }

        let output_type = func
            .types
            .as_ref()
            .and_then(|t| t.output_type.clone())
            .ok_or_else(|| "approx_top_k output type missing".to_string())?;
        let intermediate_type = func
            .types
            .as_ref()
            .and_then(|t| t.intermediate_type.clone())
            .unwrap_or(DataType::Binary);
        let input_arg_type = match input_type {
            DataType::Struct(fields) => fields.first().map(|f| f.data_type().clone()),
            other => Some(other.clone()),
        };
        Ok(AggSpec {
            kind: AggKind::ApproxTopK,
            output_type,
            intermediate_type,
            input_arg_type,
            count_all: false,
        })
    }

    fn state_layout_for(&self, kind: &AggKind) -> (usize, usize) {
        match kind {
            AggKind::ApproxTopK => (
                std::mem::size_of::<ApproxTopKState>(),
                std::mem::align_of::<ApproxTopKState>(),
            ),
            other => unreachable!("unexpected kind for approx_top_k: {:?}", other),
        }
    }

    fn build_input_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "approx_top_k input missing".to_string())?;
        Ok(AggInputView::Any(arr))
    }

    fn build_merge_view<'a>(
        &self,
        _spec: &AggSpec,
        array: &'a Option<ArrayRef>,
    ) -> Result<AggInputView<'a>, String> {
        let arr = array
            .as_ref()
            .ok_or_else(|| "approx_top_k merge input missing".to_string())?;
        let binary = arr
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "approx_top_k merge input must be BinaryArray".to_string())?;
        Ok(AggInputView::Binary(binary))
    }

    fn init_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        let _ = ptr;
        panic!("allocation-tracked approx_top_k requires tracker-aware initialization");
    }

    fn init_state_with_tracker(
        &self,
        _spec: &AggSpec,
        ptr: *mut u8,
        tracker: Option<Arc<MemTracker>>,
    ) -> Result<(), String> {
        let tracker = tracker.ok_or_else(|| {
            "allocation-tracked approx_top_k requires an aggregate memory tracker".to_string()
        })?;
        unsafe {
            std::ptr::write(ptr as *mut ApproxTopKState, ApproxTopKState::new(tracker));
        }
        Ok(())
    }

    fn drop_state(&self, _spec: &AggSpec, ptr: *mut u8) {
        unsafe {
            std::ptr::drop_in_place(ptr as *mut ApproxTopKState);
        }
    }

    fn retained_bytes(&self, _spec: &AggSpec, ptr: *const u8) -> usize {
        let _ = ptr;
        0
    }

    fn retained_memory_policy(&self, _spec: &AggSpec) -> RetainedMemoryPolicy {
        RetainedMemoryPolicy::AllocationTracked
    }

    fn update_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Any(array) = input else {
            return Err("approx_top_k input view mismatch".to_string());
        };

        if let Some(struct_arr) = array.as_any().downcast_ref::<StructArray>() {
            let cols = struct_arr.columns();
            if cols.is_empty() {
                return Err("approx_top_k struct input must have at least 1 field".to_string());
            }
            for (row, &base) in state_ptrs.iter().enumerate() {
                let state =
                    unsafe { &mut *((base as *mut u8).add(offset) as *mut ApproxTopKState) };
                ensure_initialized(state);
                if cols.len() >= 2 {
                    let k = scalar_to_i64(&scalar_from_array(&cols[1], row)?).and_then(clamp_k);
                    if let Some(k) = k {
                        state.k = k;
                        state.counter_num = state.counter_num.max(k);
                    }
                }
                if cols.len() >= 3 {
                    let counter = scalar_to_i64(&scalar_from_array(&cols[2], row)?)
                        .and_then(|v| clamp_counter_num(v, state.k));
                    if let Some(counter) = counter {
                        state.counter_num = counter;
                        enforce_counter_limit(state);
                    }
                } else {
                    state.counter_num = state.counter_num.max(default_counter_num(state.k));
                }
                let value = if struct_arr.is_null(row) {
                    None
                } else {
                    tracked_scalar_from_array(&cols[0], row, &state.allocator)?
                };
                update_one(state, value, 1)?;
            }
            return Ok(());
        }

        for (row, &base) in state_ptrs.iter().enumerate() {
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ApproxTopKState) };
            ensure_initialized(state);
            state.counter_num = state.counter_num.max(default_counter_num(state.k));
            let value = tracked_scalar_from_array(array, row, &state.allocator)?;
            update_one(state, value, 1)?;
        }
        Ok(())
    }

    fn merge_batch(
        &self,
        _spec: &AggSpec,
        offset: usize,
        state_ptrs: &[AggStatePtr],
        input: &AggInputView,
    ) -> Result<(), String> {
        let AggInputView::Binary(array) = input else {
            return Err("approx_top_k merge input view mismatch".to_string());
        };
        for (row, &base) in state_ptrs.iter().enumerate() {
            if array.is_null(row) {
                continue;
            }
            let payload = array.value(row);
            let state = unsafe { &mut *((base as *mut u8).add(offset) as *mut ApproxTopKState) };
            let decoded = deserialize_state_tracked(payload, &state.allocator)?;
            ensure_initialized(state);
            let has_entries = !decoded.entries.is_empty();
            if has_entries {
                if decoded.k > 0 {
                    state.k = decoded.k.min(MAX_COUNTER_NUM);
                }
                if decoded.counter_num > 0 {
                    state.counter_num = decoded.counter_num.max(state.k).min(MAX_COUNTER_NUM);
                    enforce_counter_limit(state);
                }
            }
            for entry in decoded.entries {
                update_one(state, entry.value, entry.count)?;
            }
            enforce_counter_limit(state);
        }
        Ok(())
    }

    fn build_array(
        &self,
        spec: &AggSpec,
        offset: usize,
        group_states: &[AggStatePtr],
        output_intermediate: bool,
    ) -> Result<ArrayRef, String> {
        if output_intermediate {
            let mut builder = BinaryBuilder::new();
            for &base in group_states {
                let state = unsafe { &*((base as *mut u8).add(offset) as *const ApproxTopKState) };
                builder.append_value(serialize_state(state));
            }
            return Ok(Arc::new(builder.finish()));
        }
        output_topk_array(&spec.output_type, offset, group_states)
    }
}

#[cfg(test)]
mod retained_bytes_tests {
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;

    fn tracked_utf8(state: &ApproxTopKState, value: &str) -> Option<TrackedAggScalarValue> {
        Some(
            super::super::common::tracked_scalar_from_value(
                AggScalarValue::Utf8(value.to_string()),
                &state.allocator,
            )
            .unwrap(),
        )
    }

    #[test]
    fn counter_limit_bounds_state_and_keeps_cached_payload_exact() {
        let tracker = MemTracker::new_root("approx-top-k-test");
        let mut state = ApproxTopKState::new(tracker.clone());
        state.initialized = true;
        state.k = 2;
        state.counter_num = 2;

        let value = tracked_utf8(&state, "aa");
        update_one(&mut state, value, 1).unwrap();
        let value = tracked_utf8(&state, "bb");
        update_one(&mut state, value, 1).unwrap();
        assert_eq!(state.counts.len(), 2);
        let before_evict = tracker.current();

        let value = tracked_utf8(&state, "cc");
        update_one(&mut state, value, 1).unwrap();
        assert_eq!(state.counts.len(), 2);
        assert!(state.counts.values().any(|entry| entry.count == 2));

        state.counter_num = 1;
        enforce_counter_limit(&mut state);
        assert_eq!(state.counts.len(), 1);
        assert!(tracker.current() < before_evict);
        drop(state);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn weighted_summary_merge_remains_bounded() {
        let tracker = MemTracker::new_root("approx-top-k-merge-test");
        let mut source = ApproxTopKState::new(tracker.clone());
        source.initialized = true;
        source.k = 2;
        source.counter_num = 2;
        for value in ["a", "b", "c", "a", "a"] {
            let value = tracked_utf8(&source, value);
            update_one(&mut source, value, 1).unwrap();
        }
        let (_, counter_num, entries) =
            deserialize_state(&serialize_state(&source)).expect("decode summary");
        let mut merged = ApproxTopKState::new(tracker.clone());
        merged.initialized = true;
        merged.k = 2;
        merged.counter_num = counter_num;
        for entry in entries {
            let value = entry
                .value
                .map(|value| {
                    super::super::common::tracked_scalar_from_value(value, &merged.allocator)
                })
                .transpose()
                .unwrap();
            update_one(&mut merged, value, entry.count).unwrap();
        }

        assert!(merged.counts.len() <= merged.counter_num);
        drop(source);
        drop(merged);
        assert_eq!(tracker.current(), 0);
    }
}
