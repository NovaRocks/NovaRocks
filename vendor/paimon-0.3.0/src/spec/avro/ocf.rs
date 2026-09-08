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

use super::cursor::AvroCursor;
use super::decode::neg_count_to_usize;
use crate::io::{ReadControl, ReadReservation};
use crate::Error;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

const AVRO_MAGIC: &[u8; 4] = b"Obj\x01";
const SYNC_MARKER_LEN: usize = 16;
const ZSTD_DECODE_CHUNK_BYTES: usize = 64 * 1024;
const MAX_AVRO_COMPRESSED_BLOCK_BYTES: usize = 64 * 1024 * 1024;
const MAX_AVRO_DECOMPRESSED_BLOCK_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_AVRO_BLOCK_OBJECTS: usize = 1024 * 1024;

/// A decoded Avro OCF header.
pub struct OcfHeader {
    pub schema_json: String,
    pub codec: OcfCodec,
    pub sync_marker: [u8; SYNC_MARKER_LEN],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OcfCodec {
    Null,
    Snappy,
    Zstandard,
}

/// A single data block from the OCF.
#[derive(Debug)]
pub struct OcfBlock<'a> {
    pub object_count: usize,
    pub data: Cow<'a, [u8]>,
    reservations: Vec<Box<dyn ReadReservation>>,
}

impl<'a> OcfBlock<'a> {
    pub(crate) fn into_parts(self) -> (usize, Cow<'a, [u8]>, Vec<Box<dyn ReadReservation>>) {
        (self.object_count, self.data, self.reservations)
    }
}

/// Streaming iterator over OCF blocks with lazy decompression and reusable decoder state.
pub struct OcfBlockIter<'a> {
    cursor: AvroCursor<'a>,
    codec: OcfCodec,
    sync_marker: [u8; SYNC_MARKER_LEN],
    snappy_decoder: snap::raw::Decoder,
    control: Option<Arc<dyn ReadControl>>,
}

impl<'a> OcfBlockIter<'a> {
    fn new(
        cursor: AvroCursor<'a>,
        codec: OcfCodec,
        sync_marker: [u8; SYNC_MARKER_LEN],
        control: Option<Arc<dyn ReadControl>>,
    ) -> Self {
        Self {
            cursor,
            codec,
            sync_marker,
            snappy_decoder: snap::raw::Decoder::new(),
            control,
        }
    }

    pub fn next_block(&mut self) -> crate::Result<Option<OcfBlock<'a>>> {
        if self.cursor.remaining() == 0 {
            return Ok(None);
        }

        let raw_object_count = self.cursor.read_long()?;
        if raw_object_count < 0 {
            return Err(Error::UnexpectedError {
                message: format!("avro ocf: negative object count: {raw_object_count}"),
                source: None,
            });
        }
        let object_count = raw_object_count as usize;
        if object_count > MAX_AVRO_BLOCK_OBJECTS {
            return Err(Error::DataInvalid {
                message: format!(
                    "avro ocf: block object count {object_count} exceeds limit {MAX_AVRO_BLOCK_OBJECTS}"
                ),
                source: None,
            });
        }
        let raw_compressed_size = self.cursor.read_long()?;
        if raw_compressed_size < 0 {
            return Err(Error::UnexpectedError {
                message: format!("avro ocf: negative compressed size: {raw_compressed_size}"),
                source: None,
            });
        }
        let compressed_size = raw_compressed_size as usize;
        if compressed_size > MAX_AVRO_COMPRESSED_BLOCK_BYTES {
            return Err(Error::DataInvalid {
                message: format!(
                    "avro ocf: compressed block size {compressed_size} exceeds limit {MAX_AVRO_COMPRESSED_BLOCK_BYTES}"
                ),
                source: None,
            });
        }
        let compressed_data = self.cursor.read_fixed(compressed_size)?;

        self.checkpoint()?;
        let (data, reservations) = self.decompress(compressed_data)?;

        let block_sync = self.cursor.read_fixed(SYNC_MARKER_LEN)?;
        if block_sync != self.sync_marker {
            return Err(Error::UnexpectedError {
                message: "avro ocf: sync marker mismatch".into(),
                source: None,
            });
        }

        self.checkpoint()?;
        Ok(Some(OcfBlock {
            object_count,
            data,
            reservations,
        }))
    }

    fn checkpoint(&self) -> crate::Result<()> {
        if let Some(control) = &self.control {
            control.checkpoint()?;
        }
        Ok(())
    }

    fn reserve(&self, bytes: usize) -> crate::Result<Option<Box<dyn ReadReservation>>> {
        self.control
            .as_ref()
            .map(|control| control.try_reserve(u64::try_from(bytes).unwrap_or(u64::MAX).max(1)))
            .transpose()
    }

    fn decompress(
        &mut self,
        data: &'a [u8],
    ) -> crate::Result<(Cow<'a, [u8]>, Vec<Box<dyn ReadReservation>>)> {
        match self.codec {
            OcfCodec::Null => {
                let reservation = self.reserve(data.len())?;
                self.checkpoint()?;
                Ok((Cow::Borrowed(data), reservation.into_iter().collect()))
            }
            OcfCodec::Snappy => {
                if data.len() < 4 {
                    return Err(Error::UnexpectedError {
                        message: "avro ocf: snappy block too short for CRC".into(),
                        source: None,
                    });
                }
                let compressed = &data[..data.len() - 4];
                let expected_crc = u32::from_be_bytes(data[data.len() - 4..].try_into().unwrap());
                let decompressed_len =
                    snap::raw::decompress_len(compressed).map_err(|e| Error::UnexpectedError {
                        message: format!("avro ocf: invalid snappy decompressed size: {e}"),
                        source: None,
                    })?;
                if decompressed_len > MAX_AVRO_DECOMPRESSED_BLOCK_BYTES {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "avro ocf: snappy decoded size {decompressed_len} exceeds limit {MAX_AVRO_DECOMPRESSED_BLOCK_BYTES}"
                        ),
                        source: None,
                    });
                }
                let reservation = self.reserve(decompressed_len)?;
                self.checkpoint()?;
                let mut decompressed = Vec::new();
                decompressed
                    .try_reserve_exact(decompressed_len)
                    .map_err(|error| Error::DataInvalid {
                        message: format!(
                            "avro ocf: cannot allocate {decompressed_len} bytes for snappy block"
                        ),
                        source: Some(Box::new(error)),
                    })?;
                decompressed.resize(decompressed_len, 0);
                let actual_len = self
                    .snappy_decoder
                    .decompress(compressed, &mut decompressed)
                    .map_err(|e| Error::UnexpectedError {
                        message: format!("avro ocf: snappy decompression failed: {e}"),
                        source: None,
                    })?;
                if actual_len != decompressed_len {
                    return Err(Error::DataInvalid {
                        message: format!(
                            "avro ocf: snappy size mismatch: declared {decompressed_len}, decoded {actual_len}"
                        ),
                        source: None,
                    });
                }
                let actual_crc = crc32fast::hash(&decompressed);
                if actual_crc != expected_crc {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "avro ocf: snappy CRC32C mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
                        ),
                        source: None,
                    });
                }
                self.checkpoint()?;
                Ok((Cow::Owned(decompressed), reservation.into_iter().collect()))
            }
            OcfCodec::Zstandard => {
                let mut decoder =
                    zstd::stream::read::Decoder::new(data).map_err(|e| Error::UnexpectedError {
                        message: format!("avro ocf: zstd decompression failed: {e}"),
                        source: None,
                    })?;
                let mut decompressed = Vec::new();
                let mut reservations = Vec::new();
                let mut chunk = [0u8; ZSTD_DECODE_CHUNK_BYTES];
                loop {
                    self.checkpoint()?;
                    let read = decoder
                        .read(&mut chunk)
                        .map_err(|e| Error::UnexpectedError {
                            message: format!("avro ocf: zstd decompression failed: {e}"),
                            source: None,
                        })?;
                    if read == 0 {
                        break;
                    }
                    let decoded_len =
                        decompressed
                            .len()
                            .checked_add(read)
                            .ok_or_else(|| Error::DataInvalid {
                                message: "avro ocf: zstd decoded size overflow".to_string(),
                                source: None,
                            })?;
                    if decoded_len > MAX_AVRO_DECOMPRESSED_BLOCK_BYTES {
                        return Err(Error::DataInvalid {
                            message: format!(
                                "avro ocf: zstd decoded size {decoded_len} exceeds limit {MAX_AVRO_DECOMPRESSED_BLOCK_BYTES}"
                            ),
                            source: None,
                        });
                    }
                    if let Some(reservation) = self.reserve(read)? {
                        reservations.push(reservation);
                    }
                    decompressed
                        .try_reserve_exact(read)
                        .map_err(|error| Error::DataInvalid {
                            message: format!(
                                "avro ocf: cannot extend zstd block by {read} decoded bytes"
                            ),
                            source: Some(Box::new(error)),
                        })?;
                    decompressed.extend_from_slice(&chunk[..read]);
                }
                self.checkpoint()?;
                Ok((Cow::Owned(decompressed), reservations))
            }
        }
    }
}

/// Parse an Avro OCF header and return a streaming block iterator.
#[allow(dead_code)]
pub fn parse_ocf_streaming(bytes: &[u8]) -> crate::Result<(OcfHeader, OcfBlockIter<'_>)> {
    parse_ocf_streaming_with_control(bytes, None)
}

/// Parse an Avro OCF header while applying one host-owned control to block
/// decompression and decode checkpoints.
pub(crate) fn parse_ocf_streaming_with_control(
    bytes: &[u8],
    control: Option<Arc<dyn ReadControl>>,
) -> crate::Result<(OcfHeader, OcfBlockIter<'_>)> {
    let mut cursor = AvroCursor::new(bytes);

    let magic = cursor.read_fixed(4)?;
    if magic != AVRO_MAGIC {
        return Err(Error::UnexpectedError {
            message: "avro ocf: invalid magic bytes".into(),
            source: None,
        });
    }

    let meta = read_avro_map(&mut cursor)?;

    let schema_json = meta
        .get("avro.schema")
        .ok_or_else(|| Error::UnexpectedError {
            message: "avro ocf: missing avro.schema in header".into(),
            source: None,
        })?
        .clone();

    let codec = match meta.get("avro.codec").map(|s| s.as_str()) {
        None | Some("null") => OcfCodec::Null,
        Some("snappy") => OcfCodec::Snappy,
        Some("zstandard") => OcfCodec::Zstandard,
        Some(other) => {
            return Err(Error::UnexpectedError {
                message: format!("avro ocf: unsupported codec: {other}"),
                source: None,
            });
        }
    };

    let sync_marker: [u8; SYNC_MARKER_LEN] =
        cursor.read_fixed(SYNC_MARKER_LEN)?.try_into().unwrap();

    let header = OcfHeader {
        schema_json,
        codec,
        sync_marker,
    };

    let iter = OcfBlockIter::new(cursor, header.codec, header.sync_marker, control);
    Ok((header, iter))
}

/// Parse an Avro OCF file into header + blocks (eagerly decompresses all blocks).
#[cfg(test)]
pub fn parse_ocf(bytes: &[u8]) -> crate::Result<(OcfHeader, Vec<OcfBlock<'_>>)> {
    let (header, mut iter) = parse_ocf_streaming(bytes)?;
    let mut blocks = Vec::new();
    while let Some(block) = iter.next_block()? {
        blocks.push(block);
    }
    Ok((header, blocks))
}

/// Read an Avro-encoded map (used for OCF file metadata).
/// Map encoding: series of blocks, each block: count(long), entries...; terminated by 0-count.
fn read_avro_map(cursor: &mut AvroCursor) -> crate::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    loop {
        let count = cursor.read_long()?;
        if count == 0 {
            break;
        }
        let count = if count < 0 {
            // Negative count means the block size in bytes follows (we skip it).
            cursor.skip_long()?;
            neg_count_to_usize(count)?
        } else {
            count as usize
        };
        for _ in 0..count {
            let key = cursor.read_string()?.to_string();
            let value_bytes = cursor.read_bytes()?;
            let value = String::from_utf8_lossy(value_bytes).into_owned();
            map.insert(key, value);
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct TestReservation {
        bytes: u64,
        retained: Arc<AtomicU64>,
    }

    impl Drop for TestReservation {
        fn drop(&mut self) {
            self.retained.fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }

    impl ReadReservation for TestReservation {
        fn bytes(&self) -> u64 {
            self.bytes
        }
    }

    #[derive(Debug)]
    struct TestControl {
        limit: u64,
        retained: Arc<AtomicU64>,
        peak: AtomicU64,
        checkpoints: AtomicUsize,
    }

    impl TestControl {
        fn new(limit: u64) -> Self {
            Self {
                limit,
                retained: Arc::new(AtomicU64::new(0)),
                peak: AtomicU64::new(0),
                checkpoints: AtomicUsize::new(0),
            }
        }
    }

    impl ReadControl for TestControl {
        fn check_active(&self) -> crate::Result<()> {
            Ok(())
        }

        fn checkpoint(&self) -> crate::Result<()> {
            self.checkpoints.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn try_reserve(&self, bytes: u64) -> crate::Result<Box<dyn ReadReservation>> {
            let current = self.retained.load(Ordering::SeqCst);
            let next = current
                .checked_add(bytes)
                .ok_or_else(|| Error::DataInvalid {
                    message: "test reservation overflow".to_string(),
                    source: None,
                })?;
            if next > self.limit {
                return Err(Error::DataInvalid {
                    message: format!("test reservation limit exceeded: {next} > {}", self.limit),
                    source: None,
                });
            }
            self.retained.store(next, Ordering::SeqCst);
            self.peak.fetch_max(next, Ordering::SeqCst);
            Ok(Box::new(TestReservation {
                bytes,
                retained: self.retained.clone(),
            }))
        }
    }

    fn compressed_ocf(codec: apache_avro::Codec, payload_len: usize) -> Vec<u8> {
        use apache_avro::{Schema, Writer};

        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "test", "fields": [{"name": "payload", "type": "string"}]}"#,
        )
        .unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), codec);
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("payload", "x".repeat(payload_len));
        writer.append(record).unwrap();
        writer.into_inner().unwrap()
    }

    fn encode_long(value: i64) -> Vec<u8> {
        let mut value = ((value << 1) ^ (value >> 63)) as u64;
        let mut encoded = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            encoded.push(byte);
            if value == 0 {
                return encoded;
            }
        }
    }

    fn forge_first_object_count(bytes: &[u8], object_count: i64) -> Vec<u8> {
        let mut cursor = AvroCursor::new(bytes);
        cursor.read_fixed(AVRO_MAGIC.len()).unwrap();
        read_avro_map(&mut cursor).unwrap();
        cursor.read_fixed(SYNC_MARKER_LEN).unwrap();
        let count_start = cursor.position();
        cursor.read_long().unwrap();
        let count_end = cursor.position();

        let mut forged = Vec::with_capacity(bytes.len() + 10);
        forged.extend_from_slice(&bytes[..count_start]);
        forged.extend_from_slice(&encode_long(object_count));
        forged.extend_from_slice(&bytes[count_end..]);
        forged
    }

    #[test]
    fn test_parse_ocf_roundtrip() {
        // Write a simple OCF with apache-avro, then parse with our reader
        use apache_avro::{Codec, Schema, Writer};

        let schema = Schema::parse_str(r#"{"type": "record", "name": "test", "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]}"#).unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("a", 42i32);
        record.put("b", "hello");
        writer.append(record).unwrap();
        let mut record2 = apache_avro::types::Record::new(&schema).unwrap();
        record2.put("a", 99i32);
        record2.put("b", "world");
        writer.append(record2).unwrap();
        let bytes = writer.into_inner().unwrap();

        let (header, blocks) = parse_ocf(&bytes).unwrap();
        assert_eq!(header.codec, OcfCodec::Null);
        assert!(header.schema_json.contains("test"));

        let total_objects: usize = blocks.iter().map(|b| b.object_count).sum();
        assert_eq!(total_objects, 2);
    }

    #[test]
    fn test_parse_ocf_zstd() {
        use apache_avro::{Codec, Schema, Writer};

        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "test", "fields": [{"name": "x", "type": "long"}]}"#,
        )
        .unwrap();
        let mut writer = Writer::with_codec(
            &schema,
            Vec::new(),
            Codec::Zstandard(apache_avro::ZstandardSettings::default()),
        );
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("x", 67890i64);
        writer.append(record).unwrap();
        let bytes = writer.into_inner().unwrap();

        let (header, blocks) = parse_ocf(&bytes).unwrap();
        assert_eq!(header.codec, OcfCodec::Zstandard);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].object_count, 1);
    }

    #[test]
    fn test_parse_ocf_snappy() {
        use apache_avro::{Codec, Schema, Writer};

        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "test", "fields": [{"name": "x", "type": "long"}]}"#,
        )
        .unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Snappy);
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("x", 12345i64);
        writer.append(record).unwrap();
        let bytes = writer.into_inner().unwrap();

        let (header, blocks) = parse_ocf(&bytes).unwrap();
        assert_eq!(header.codec, OcfCodec::Snappy);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].object_count, 1);
    }

    #[test]
    fn controlled_snappy_decompression_reserves_and_checkpoints() {
        let bytes = compressed_ocf(apache_avro::Codec::Snappy, 256 * 1024);
        let control = Arc::new(TestControl::new(1024 * 1024));
        let (_, mut blocks) =
            parse_ocf_streaming_with_control(&bytes, Some(control.clone())).unwrap();
        let block = blocks.next_block().unwrap().unwrap();
        assert!(block.data.len() >= 256 * 1024);
        assert!(control.peak.load(Ordering::SeqCst) >= 256 * 1024);
        assert!(control.checkpoints.load(Ordering::SeqCst) >= 2);
        assert!(control.retained.load(Ordering::SeqCst) >= 256 * 1024);
        drop(block);
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn controlled_zstd_decompression_is_bounded_by_host_reservation() {
        let bytes = compressed_ocf(
            apache_avro::Codec::Zstandard(apache_avro::ZstandardSettings::default()),
            256 * 1024,
        );
        let control = Arc::new(TestControl::new(96 * 1024));
        let (_, mut blocks) =
            parse_ocf_streaming_with_control(&bytes, Some(control.clone())).unwrap();
        let error = blocks.next_block().unwrap_err();
        assert!(error.to_string().contains("reservation limit exceeded"));
        assert!(control.checkpoints.load(Ordering::SeqCst) >= 2);
        assert_eq!(control.retained.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn rejects_forged_huge_object_count_before_block_allocation() {
        let bytes = compressed_ocf(apache_avro::Codec::Null, 1);
        let forged = forge_first_object_count(&bytes, MAX_AVRO_BLOCK_OBJECTS as i64 + 1);
        let (_, mut blocks) = parse_ocf_streaming(&forged).unwrap();
        let error = blocks.next_block().unwrap_err();
        assert!(error.to_string().contains("block object count"));
        assert!(error.to_string().contains("exceeds limit"));
    }
}
