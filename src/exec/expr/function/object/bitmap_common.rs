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

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;

use roaring::RoaringBitmap;

pub(crate) const BITMAP_TYPE_EMPTY: u8 = 0;
pub(crate) const BITMAP_TYPE_SINGLE32: u8 = 1;
pub(crate) const BITMAP_TYPE_BITMAP32: u8 = 2;
pub(crate) const BITMAP_TYPE_SINGLE64: u8 = 3;
pub(crate) const BITMAP_TYPE_BITMAP64: u8 = 4;
pub(crate) const BITMAP_TYPE_SET: u8 = 10;
pub(crate) const BITMAP_TYPE_BITMAP32_SERIV2: u8 = 12;
pub(crate) const BITMAP_TYPE_BITMAP64_SERIV2: u8 = 13;

const ROARING_COOKIE_NO_RUNCONTAINER: u32 = 12_346; // 0x303A
const ROARING_COOKIE_RUNCONTAINER: u16 = 12_347; // 0x303B

pub(crate) fn encode_varint_u64(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn decode_varint_u64(bytes: &[u8]) -> Result<(u64, usize), String> {
    let mut out = 0u64;
    let mut shift = 0u32;
    for (idx, byte) in bytes.iter().enumerate() {
        out |= u64::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Ok((out, idx + 1));
        }
        shift += 7;
        if shift > 63 {
            return Err("bitmap decode varint overflow".to_string());
        }
    }
    Err("bitmap decode varint reached end of payload".to_string())
}

pub(crate) fn parse_bitmap_string(text: &str) -> Result<BTreeSet<u64>, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut out = BTreeSet::new();
    for part in trimmed.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        let value = token
            .parse::<u64>()
            .map_err(|_| format!("bitmap string contains invalid value: {}", token))?;
        out.insert(value);
    }
    Ok(out)
}

fn parse_bitmap_text(bytes: &[u8]) -> Result<BTreeSet<u64>, String> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| "bitmap payload is not utf8 text".to_string())?;
    parse_bitmap_string(text)
}

pub(crate) fn decode_internal_bitmap(bytes: &[u8]) -> Result<BTreeSet<u64>, String> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    match bytes[0] {
        BITMAP_TYPE_EMPTY => {
            if bytes.len() != 1 {
                return Err(format!(
                    "bitmap internal EMPTY payload length mismatch: expected=1 actual={}",
                    bytes.len()
                ));
            }
            Ok(BTreeSet::new())
        }
        BITMAP_TYPE_SINGLE32 => {
            if bytes.len() != 5 {
                return Err(format!(
                    "bitmap internal SINGLE32 payload length mismatch: expected=5 actual={}",
                    bytes.len()
                ));
            }
            let value = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "bitmap internal decode SINGLE32 failed".to_string())?,
            );
            Ok(BTreeSet::from([u64::from(value)]))
        }
        BITMAP_TYPE_SINGLE64 => {
            if bytes.len() != 9 {
                return Err(format!(
                    "bitmap internal SINGLE64 payload length mismatch: expected=9 actual={}",
                    bytes.len()
                ));
            }
            let value = u64::from_le_bytes(
                bytes[1..9]
                    .try_into()
                    .map_err(|_| "bitmap internal decode SINGLE64 failed".to_string())?,
            );
            Ok(BTreeSet::from([value]))
        }
        BITMAP_TYPE_SET => {
            if bytes.len() < 5 {
                return Err(format!(
                    "bitmap internal SET payload too short: actual={}",
                    bytes.len()
                ));
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "bitmap internal decode SET count failed".to_string())?,
            ) as usize;
            let mut offset = 5usize;
            let mut values = BTreeSet::new();
            for idx in 0..count {
                let (value, consumed) = decode_varint_u64(&bytes[offset..]).map_err(|e| {
                    format!(
                        "bitmap internal decode SET value failed at entry {}: {}",
                        idx, e
                    )
                })?;
                if consumed == 0 {
                    return Err(format!(
                        "bitmap internal decode SET consumed zero bytes at entry {}",
                        idx
                    ));
                }
                offset = offset.saturating_add(consumed);
                if offset > bytes.len() {
                    return Err(format!(
                        "bitmap internal SET payload overflow: offset={} len={}",
                        offset,
                        bytes.len()
                    ));
                }
                values.insert(value);
            }
            if offset != bytes.len() {
                return Err(format!(
                    "bitmap internal SET payload has trailing bytes: offset={} len={}",
                    offset,
                    bytes.len()
                ));
            }
            Ok(values)
        }
        _ => Err(format!(
            "bitmap internal unsupported payload type code: {}",
            bytes[0]
        )),
    }
}

fn decode_roaring32_payload(bytes: &[u8]) -> Result<(Vec<u32>, usize), String> {
    if bytes.is_empty() {
        return Err("bitmap roaring32 payload is empty".to_string());
    }
    let mut cursor = Cursor::new(bytes);
    let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
        .map_err(|e| format!("bitmap decode roaring32 payload failed: {}", e))?;
    let consumed = usize::try_from(cursor.position())
        .map_err(|_| "bitmap decode roaring32 payload length overflow".to_string())?;
    if consumed == 0 || consumed > bytes.len() {
        return Err(format!(
            "bitmap decode roaring32 payload consumed invalid size: consumed={} len={}",
            consumed,
            bytes.len()
        ));
    }
    Ok((bitmap.iter().collect(), consumed))
}

fn decode_external_bitmap32(bytes: &[u8]) -> Result<BTreeSet<u64>, String> {
    if bytes.len() <= 1 {
        return Err("bitmap external BITMAP32 payload is empty".to_string());
    }
    let (values, _) = decode_roaring32_payload(&bytes[1..])?;
    Ok(values.into_iter().map(u64::from).collect())
}

fn decode_external_bitmap64(bytes: &[u8]) -> Result<BTreeSet<u64>, String> {
    if bytes.len() <= 1 {
        return Err("bitmap external BITMAP64 payload is empty".to_string());
    }
    let (map_size, consumed) = decode_varint_u64(&bytes[1..])?;
    let mut offset = 1usize.saturating_add(consumed);
    let mut out = BTreeSet::new();
    for idx in 0..map_size {
        if offset.saturating_add(4) > bytes.len() {
            return Err(format!(
                "bitmap external BITMAP64 map key overflow at entry {}: offset={} len={}",
                idx,
                offset,
                bytes.len()
            ));
        }
        let high = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| "bitmap external BITMAP64 decode high bits failed".to_string())?,
        );
        offset += 4;
        let (values, used) = decode_roaring32_payload(&bytes[offset..]).map_err(|e| {
            format!(
                "bitmap external BITMAP64 decode roaring payload failed at entry {}: {}",
                idx, e
            )
        })?;
        offset = offset.saturating_add(used);
        if offset > bytes.len() {
            return Err(format!(
                "bitmap external BITMAP64 payload overflow at entry {}: offset={} len={}",
                idx,
                offset,
                bytes.len()
            ));
        }
        for low in values {
            out.insert((u64::from(high) << 32) | u64::from(low));
        }
    }
    Ok(out)
}

pub(crate) fn decode_external_bitmap(bytes: &[u8]) -> Result<BTreeSet<u64>, String> {
    if bytes.is_empty() {
        return Err("bitmap external payload is empty".to_string());
    }
    match bytes[0] {
        BITMAP_TYPE_EMPTY => Ok(BTreeSet::new()),
        BITMAP_TYPE_SINGLE32 => {
            if bytes.len() < 5 {
                return Err(format!(
                    "bitmap external SINGLE32 payload too short: actual={}",
                    bytes.len()
                ));
            }
            let value = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "bitmap external decode SINGLE32 failed".to_string())?,
            );
            Ok(BTreeSet::from([u64::from(value)]))
        }
        BITMAP_TYPE_SINGLE64 => {
            if bytes.len() < 9 {
                return Err(format!(
                    "bitmap external SINGLE64 payload too short: actual={}",
                    bytes.len()
                ));
            }
            let value = u64::from_le_bytes(
                bytes[1..9]
                    .try_into()
                    .map_err(|_| "bitmap external decode SINGLE64 failed".to_string())?,
            );
            Ok(BTreeSet::from([value]))
        }
        BITMAP_TYPE_SET => {
            if bytes.len() < 5 {
                return Err(format!(
                    "bitmap external SET payload too short: actual={}",
                    bytes.len()
                ));
            }
            let count = u32::from_le_bytes(
                bytes[1..5]
                    .try_into()
                    .map_err(|_| "bitmap external decode SET count failed".to_string())?,
            ) as usize;
            let required = 5usize.saturating_add(count.saturating_mul(8));
            if bytes.len() < required {
                return Err(format!(
                    "bitmap external SET payload too short: required={} actual={}",
                    required,
                    bytes.len()
                ));
            }
            let mut out = BTreeSet::new();
            let mut offset = 5usize;
            for _ in 0..count {
                let value = u64::from_le_bytes(
                    bytes[offset..offset + 8]
                        .try_into()
                        .map_err(|_| "bitmap external decode SET value failed".to_string())?,
                );
                offset += 8;
                out.insert(value);
            }
            Ok(out)
        }
        BITMAP_TYPE_BITMAP32 | BITMAP_TYPE_BITMAP32_SERIV2 => decode_external_bitmap32(bytes),
        BITMAP_TYPE_BITMAP64 | BITMAP_TYPE_BITMAP64_SERIV2 => decode_external_bitmap64(bytes),
        _ => Err(format!(
            "bitmap external unsupported payload type code: {}",
            bytes[0]
        )),
    }
}

pub(crate) fn decode_bitmap(bytes: &[u8]) -> Result<BTreeSet<u64>, String> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    if let Ok(values) = decode_internal_bitmap(bytes) {
        return Ok(values);
    }
    if let Ok(values) = decode_external_bitmap(bytes) {
        return Ok(values);
    }
    parse_bitmap_text(bytes)
}

pub(crate) fn encode_internal_bitmap(values: &BTreeSet<u64>) -> Result<Vec<u8>, String> {
    if values.is_empty() {
        return Ok(vec![BITMAP_TYPE_EMPTY]);
    }
    if values.len() == 1 {
        let value = values
            .first()
            .copied()
            .ok_or_else(|| "bitmap internal encode missing singleton value".to_string())?;
        if let Ok(v32) = u32::try_from(value) {
            let mut out = Vec::with_capacity(5);
            out.push(BITMAP_TYPE_SINGLE32);
            out.extend_from_slice(&v32.to_le_bytes());
            return Ok(out);
        }
        let mut out = Vec::with_capacity(9);
        out.push(BITMAP_TYPE_SINGLE64);
        out.extend_from_slice(&value.to_le_bytes());
        return Ok(out);
    }

    let count = u32::try_from(values.len())
        .map_err(|_| format!("bitmap internal value count overflow: {}", values.len()))?;
    let mut out = Vec::new();
    out.push(BITMAP_TYPE_SET);
    out.extend_from_slice(&count.to_le_bytes());
    for value in values {
        encode_varint_u64(*value, &mut out);
    }
    Ok(out)
}

fn collect_runs_u16(values: &[u32]) -> Option<(u16, Vec<(u16, u16)>)> {
    let first = *values.first()?;
    let key = (first >> 16) as u16;
    let mut runs = Vec::new();
    let mut start = (first & 0xffff) as u16;
    let mut prev = start;

    for &value in values.iter().skip(1) {
        if ((value >> 16) as u16) != key {
            return None;
        }
        let low = (value & 0xffff) as u16;
        let is_next = prev != u16::MAX && low == prev + 1;
        if is_next {
            prev = low;
            continue;
        }
        runs.push((start, prev.wrapping_sub(start)));
        start = low;
        prev = low;
    }
    runs.push((start, prev.wrapping_sub(start)));
    Some((key, runs))
}

fn should_encode_run_container(values: &[u32]) -> bool {
    if values.len() <= 32 {
        return false;
    }
    let Some((_, runs)) = collect_runs_u16(values) else {
        return false;
    };
    runs.len() <= values.len() / 2
}

fn encode_roaring32_no_run(values: &[u32]) -> Result<Vec<u8>, String> {
    let mut bitmap = RoaringBitmap::new();
    for &value in values {
        bitmap.insert(value);
    }
    let mut out = Vec::new();
    bitmap
        .serialize_into(&mut out)
        .map_err(|e| format!("bitmap encode roaring32 payload failed: {}", e))?;
    Ok(out)
}

fn encode_roaring32_run_single_container(values: &[u32]) -> Option<Vec<u8>> {
    let (key, runs) = collect_runs_u16(values)?;
    if runs.len() > u16::MAX as usize {
        return None;
    }
    let cardinality_minus_one = u16::try_from(values.len().checked_sub(1)?).ok()?;
    let runs_count = u16::try_from(runs.len()).ok()?;

    let mut out = Vec::new();
    let cookie = u32::from(ROARING_COOKIE_RUNCONTAINER);
    out.extend_from_slice(&cookie.to_le_bytes());
    out.push(0x01); // run-container bitmap for one container
    out.extend_from_slice(&key.to_le_bytes());
    out.extend_from_slice(&cardinality_minus_one.to_le_bytes());
    out.extend_from_slice(&runs_count.to_le_bytes());
    for (start, len_minus_one) in runs {
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&len_minus_one.to_le_bytes());
    }
    Some(out)
}

fn encode_roaring32_payload(values: &[u32]) -> Result<Vec<u8>, String> {
    if should_encode_run_container(values) {
        if let Some(out) = encode_roaring32_run_single_container(values) {
            return Ok(out);
        }
    }
    encode_roaring32_no_run(values)
}

pub(crate) fn encode_external_bitmap(values: &BTreeSet<u64>) -> Result<Vec<u8>, String> {
    if values.is_empty() {
        return Ok(vec![BITMAP_TYPE_EMPTY]);
    }
    if values.len() == 1 {
        let value = values
            .first()
            .copied()
            .ok_or_else(|| "bitmap external encode missing singleton value".to_string())?;
        if let Ok(v32) = u32::try_from(value) {
            let mut out = Vec::with_capacity(5);
            out.push(BITMAP_TYPE_SINGLE32);
            out.extend_from_slice(&v32.to_le_bytes());
            return Ok(out);
        }
        let mut out = Vec::with_capacity(9);
        out.push(BITMAP_TYPE_SINGLE64);
        out.extend_from_slice(&value.to_le_bytes());
        return Ok(out);
    }
    if values.len() <= 32 {
        let count = u32::try_from(values.len())
            .map_err(|_| format!("bitmap external value count overflow: {}", values.len()))?;
        let mut out = Vec::with_capacity(1 + 4 + values.len() * 8);
        out.push(BITMAP_TYPE_SET);
        out.extend_from_slice(&count.to_le_bytes());
        for &value in values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        return Ok(out);
    }

    let mut buckets: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for &value in values {
        let high = (value >> 32) as u32;
        let low = value as u32;
        buckets.entry(high).or_default().push(low);
    }
    if buckets.len() == 1 && buckets.contains_key(&0) {
        let payload = encode_roaring32_payload(
            buckets
                .get(&0)
                .ok_or_else(|| "bitmap external encode missing 32-bit bucket".to_string())?,
        )?;
        let mut out = Vec::with_capacity(1 + payload.len());
        out.push(BITMAP_TYPE_BITMAP32);
        out.extend_from_slice(&payload);
        return Ok(out);
    }

    let mut out = Vec::new();
    out.push(BITMAP_TYPE_BITMAP64);
    encode_varint_u64(
        u64::try_from(buckets.len())
            .map_err(|_| "bitmap external map size overflow".to_string())?,
        &mut out,
    );
    for (high, lows) in buckets {
        out.extend_from_slice(&high.to_le_bytes());
        let payload = encode_roaring32_payload(&lows)?;
        out.extend_from_slice(&payload);
    }
    Ok(out)
}

pub(crate) fn roaring_cookie_no_run() -> u32 {
    ROARING_COOKIE_NO_RUNCONTAINER
}
