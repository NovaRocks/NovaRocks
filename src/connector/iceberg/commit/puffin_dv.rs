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

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use anyhow::{Context, Result, anyhow, ensure};
use roaring::RoaringBitmap;

const MAGIC: [u8; 4] = [0xD1, 0xD3, 0x39, 0x64];
const MAX_POSITIVE_I64_POSITION: u64 = 1u64 << 63;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeletionVector {
    bitmaps: BTreeMap<u32, RoaringBitmap>,
}

impl DeletionVector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, position: u64) -> Result<()> {
        ensure_positive_64_bit_position(position)?;
        let key = high_key(position);
        let value = low_value(position);
        self.bitmaps.entry(key).or_default().insert(value);
        Ok(())
    }

    pub fn merge(&mut self, other: &DeletionVector) {
        for (key, bitmap) in &other.bitmaps {
            let target = self.bitmaps.entry(*key).or_default();
            *target |= bitmap.clone();
        }
    }

    pub fn contains(&self, position: u64) -> bool {
        if position >= MAX_POSITIVE_I64_POSITION {
            return false;
        }
        self.bitmaps
            .get(&high_key(position))
            .is_some_and(|bitmap| bitmap.contains(low_value(position)))
    }

    pub fn cardinality(&self) -> u64 {
        self.bitmaps.values().map(RoaringBitmap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.bitmaps.values().all(RoaringBitmap::is_empty)
    }

    pub fn to_iceberg_payload(&self) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.extend_from_slice(&(self.bitmaps.len() as u64).to_le_bytes());
        for (key, bitmap) in &self.bitmaps {
            body.extend_from_slice(&key.to_le_bytes());
            bitmap
                .serialize_into(&mut body)
                .context("failed to serialize deletion vector bitmap")?;
        }

        let body_len = u32::try_from(body.len()).context("deletion vector payload too large")?;
        let crc = crc32fast::hash(&body);

        let mut payload = Vec::with_capacity(4 + body.len() + 4);
        payload.extend_from_slice(&body_len.to_be_bytes());
        payload.extend_from_slice(&body);
        payload.extend_from_slice(&crc.to_be_bytes());
        Ok(payload)
    }

    pub fn from_iceberg_payload(payload: &[u8]) -> Result<Self> {
        ensure!(
            payload.len() >= 4 + MAGIC.len() + 8 + 4,
            "deletion vector payload is too short"
        );

        let declared_len = read_be_u32(&payload[..4])? as usize;
        ensure!(
            payload.len() == 4 + declared_len + 4,
            "deletion vector payload length mismatch: declared {}, actual {}",
            declared_len,
            payload.len().saturating_sub(8)
        );

        let body_start = 4;
        let body_end = body_start + declared_len;
        let body = &payload[body_start..body_end];
        ensure!(
            body.starts_with(&MAGIC),
            "invalid deletion vector magic bytes"
        );

        let expected_crc = read_be_u32(&payload[body_end..body_end + 4])?;
        let actual_crc = crc32fast::hash(body);
        ensure!(
            expected_crc == actual_crc,
            "deletion vector CRC mismatch: expected {expected_crc:#010x}, actual {actual_crc:#010x}"
        );

        let mut cursor = Cursor::new(&body[MAGIC.len()..]);
        let bitmap_count = read_le_u64_from(&mut cursor)?;
        let mut bitmaps = BTreeMap::new();
        for _ in 0..bitmap_count {
            let key = read_le_u32_from(&mut cursor)?;
            ensure!(
                !bitmaps.contains_key(&key),
                "deletion vector payload contains duplicate key {key}"
            );
            let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
                .context("failed to deserialize deletion vector bitmap")?;
            bitmaps.insert(key, bitmap);
        }

        ensure!(
            cursor.position() as usize == body.len() - MAGIC.len(),
            "deletion vector payload contains trailing bytes"
        );

        Ok(Self { bitmaps })
    }
}

fn ensure_positive_64_bit_position(position: u64) -> Result<()> {
    ensure!(
        position < MAX_POSITIVE_I64_POSITION,
        "deletion vector position must be a non-negative 63-bit value"
    );
    Ok(())
}

fn high_key(position: u64) -> u32 {
    (position >> 32) as u32
}

fn low_value(position: u64) -> u32 {
    position as u32
}

fn read_be_u32(bytes: &[u8]) -> Result<u32> {
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| anyhow!("expected 4 bytes for big-endian u32"))?;
    Ok(u32::from_be_bytes(array))
}

fn read_le_u32_from(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0u8; 4];
    cursor
        .read_exact(&mut bytes)
        .context("failed to read little-endian u32")?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_le_u64_from(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0u8; 8];
    cursor
        .read_exact(&mut bytes)
        .context("failed to read little-endian u64")?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bitmap_with(values: &[u32]) -> RoaringBitmap {
        let mut bitmap = RoaringBitmap::new();
        for value in values {
            bitmap.insert(*value);
        }
        bitmap
    }

    fn payload_from_body(body: &[u8]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        payload.extend_from_slice(body);
        payload.extend_from_slice(&crc32fast::hash(body).to_be_bytes());
        payload
    }

    fn payload_from_entries(entries: &[(u32, RoaringBitmap)]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (key, bitmap) in entries {
            body.extend_from_slice(&key.to_le_bytes());
            bitmap.serialize_into(&mut body).unwrap();
        }
        payload_from_body(&body)
    }

    fn assert_payload_error_contains(payload: &[u8], expected: &str) {
        let err = DeletionVector::from_iceberg_payload(payload)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(expected),
            "expected error containing {expected:?}, got {err:?}"
        );
    }

    #[test]
    fn deletion_vector_round_trips_32_and_64_bit_positions() {
        let mut dv = DeletionVector::new();
        dv.insert(0).unwrap();
        dv.insert(7).unwrap();
        dv.insert(u32::MAX as u64 + 3).unwrap();

        let payload = dv.to_iceberg_payload().unwrap();
        let decoded = DeletionVector::from_iceberg_payload(&payload).unwrap();

        assert_eq!(decoded.cardinality(), 3);
        assert!(decoded.contains(0));
        assert!(decoded.contains(7));
        assert!(decoded.contains(u32::MAX as u64 + 3));
        assert!(!decoded.contains(6));
        assert!(!decoded.is_empty());
        assert_eq!(decoded, dv);
    }

    #[test]
    fn deletion_vector_rejects_high_bit_positions() {
        let mut dv = DeletionVector::new();
        let err = dv.insert(1u64 << 63).unwrap_err().to_string();
        assert!(err.contains("non-negative 63-bit"));
    }

    #[test]
    fn deletion_vector_rejects_duplicate_keys() {
        let payload = payload_from_entries(&[(7, bitmap_with(&[1, 2])), (7, bitmap_with(&[3, 4]))]);

        assert_payload_error_contains(&payload, "duplicate key");
    }

    #[test]
    fn deletion_vector_rejects_bad_length() {
        let mut dv = DeletionVector::new();
        dv.insert(7).unwrap();
        let mut payload = dv.to_iceberg_payload().unwrap();
        payload[..4].copy_from_slice(&1u32.to_be_bytes());

        assert_payload_error_contains(&payload, "length mismatch");
    }

    #[test]
    fn deletion_vector_rejects_bad_magic() {
        let mut dv = DeletionVector::new();
        dv.insert(7).unwrap();
        let mut payload = dv.to_iceberg_payload().unwrap();
        payload[4] ^= 0xff;

        assert_payload_error_contains(&payload, "magic");
    }

    #[test]
    fn deletion_vector_rejects_crc_mismatch() {
        let mut dv = DeletionVector::new();
        dv.insert(7).unwrap();
        let mut payload = dv.to_iceberg_payload().unwrap();
        let last = payload.len() - 1;
        payload[last] ^= 0xff;

        assert_payload_error_contains(&payload, "CRC mismatch");
    }

    #[test]
    fn deletion_vector_rejects_trailing_bytes() {
        let bitmap = bitmap_with(&[1, 2, 3]);
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC);
        body.extend_from_slice(&1u64.to_le_bytes());
        body.extend_from_slice(&9u32.to_le_bytes());
        bitmap.serialize_into(&mut body).unwrap();
        body.push(0);
        let payload = payload_from_body(&body);

        assert_payload_error_contains(&payload, "trailing bytes");
    }
}
