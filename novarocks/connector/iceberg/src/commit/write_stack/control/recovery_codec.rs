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

//! One bounded, lossless private encoding for write-session recovery facts.

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self, Read, Write};

const MAGIC: &[u8; 8] = b"NIWREC03";
const CODEC: u8 = 1;
const HEADER_BYTES: usize = 13;
const MAX_DECODED_BYTES: usize = 8 * 1024 * 1024;
const WINDOW_LOG: u32 = 20;
const LEVEL: i32 = 3;

struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl LimitedWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "Recovery codec byte capacity exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn capacity(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}
fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
fn encode_error(error: io::Error) -> ConnectorError {
    if error.kind() == io::ErrorKind::WriteZero {
        capacity("Iceberg recovery evidence exceeds 64 KiB")
    } else {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("Encode Iceberg recovery frame: {error}"),
        )
    }
}

pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, ConnectorError> {
    let mut raw = LimitedWriter::new(MAX_DECODED_BYTES);
    serde_json::to_writer(&mut raw, value).map_err(|error| {
        if error.io_error_kind() == Some(io::ErrorKind::WriteZero) {
            capacity("Iceberg recovery decoded facts exceed 8 MiB")
        } else {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("Encode complete Iceberg recovery facts: {error}"),
            )
        }
    })?;
    let decoded_len = u32::try_from(raw.bytes.len())
        .map_err(|_| capacity("Iceberg recovery decoded length overflow"))?;
    let output = LimitedWriter::new(MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES - HEADER_BYTES);
    let mut encoder = zstd::stream::Encoder::new(output, LEVEL).map_err(encode_error)?;
    encoder.window_log(WINDOW_LOG).map_err(encode_error)?;
    encoder.include_checksum(true).map_err(encode_error)?;
    encoder.include_contentsize(true).map_err(encode_error)?;
    encoder
        .set_pledged_src_size(Some(u64::from(decoded_len)))
        .map_err(encode_error)?;
    encoder.write_all(&raw.bytes).map_err(encode_error)?;
    let compressed = encoder.finish().map_err(encode_error)?;
    let mut output = Vec::with_capacity(HEADER_BYTES + compressed.bytes.len());
    output.extend_from_slice(MAGIC);
    output.push(CODEC);
    output.extend_from_slice(&decoded_len.to_le_bytes());
    output.extend_from_slice(&compressed.bytes);
    Ok(output)
}

pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ConnectorError> {
    if bytes.len() > MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES || bytes.len() <= HEADER_BYTES {
        return Err(corrupt(
            "Iceberg recovery encoded length is outside its bounded frame",
        ));
    }
    if &bytes[..8] != MAGIC || bytes[8] != CODEC {
        return Err(corrupt(
            "Iceberg recovery magic or private codec version differs",
        ));
    }
    let declared =
        u32::from_le_bytes(bytes[9..13].try_into().expect("header length checked")) as usize;
    if declared > MAX_DECODED_BYTES {
        return Err(corrupt(
            "Iceberg recovery declared decoded length exceeds 8 MiB",
        ));
    }
    let mut decoder = zstd::stream::Decoder::with_buffer(&bytes[HEADER_BYTES..])
        .map_err(|error| corrupt(format!("Decode Iceberg recovery frame: {error}")))?
        .single_frame();
    decoder
        .window_log_max(WINDOW_LOG)
        .map_err(|error| corrupt(format!("Bound Iceberg recovery decoder window: {error}")))?;
    // Do not trust either the header or the frame's claimed content size.
    let mut raw = Vec::new();
    (&mut decoder)
        .take((declared as u64) + 1)
        .read_to_end(&mut raw)
        .map_err(|error| corrupt(format!("Read bounded Iceberg recovery frame: {error}")))?;
    if raw.len() != declared {
        return Err(corrupt(
            "Iceberg recovery actual decoded length differs from its header",
        ));
    }
    if !decoder.finish().is_empty() {
        return Err(corrupt(
            "Iceberg recovery contains a concatenated frame or trailing bytes",
        ));
    }
    serde_json::from_slice(&raw)
        .map_err(|error| corrupt(format!("Decode complete Iceberg recovery facts: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Facts {
        marker: String,
    }
    fn frame(raw: &[u8], declared: usize) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.push(CODEC);
        bytes.extend_from_slice(&(declared as u32).to_le_bytes());
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), LEVEL).unwrap();
        encoder.window_log(WINDOW_LOG).unwrap();
        encoder
            .set_pledged_src_size(Some(raw.len() as u64))
            .unwrap();
        encoder.write_all(raw).unwrap();
        bytes.extend_from_slice(&encoder.finish().unwrap());
        bytes
    }
    fn assert_corrupt(bytes: &[u8]) {
        assert_eq!(
            decode::<Facts>(bytes).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }
    #[test]
    fn codec_rejects_malformed_headers_lengths_frames_and_unknown_fields() {
        let facts = Facts {
            marker: "original operation".into(),
        };
        let bytes = encode(&facts).unwrap();
        assert_eq!(decode::<Facts>(&bytes).unwrap(), facts);
        for length in 0..bytes.len() {
            assert_corrupt(&bytes[..length]);
        }
        let mut wrong = bytes.clone();
        wrong[0] ^= 1;
        assert_corrupt(&wrong);
        let mut wrong = bytes.clone();
        wrong[8] += 1;
        assert_corrupt(&wrong);
        let mut wrong = bytes.clone();
        wrong[9..13].copy_from_slice(&0u32.to_le_bytes());
        assert_corrupt(&wrong);
        let mut wrong = bytes.clone();
        wrong[9..13].copy_from_slice(&((MAX_DECODED_BYTES + 1) as u32).to_le_bytes());
        assert_corrupt(&wrong);
        let mut wrong = bytes.clone();
        wrong.extend_from_slice(b"trailing");
        assert_corrupt(&wrong);
        let mut wrong = bytes.clone();
        wrong.extend_from_slice(&bytes[HEADER_BYTES..]);
        assert_corrupt(&wrong);
        let unknown = br#"{"marker":"original operation","unknown":true}"#;
        assert_corrupt(&frame(unknown, unknown.len()));
        assert_corrupt(&vec![0; MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES + 1]);
    }
    #[test]
    fn codec_bounds_bomb_output_even_when_declared_length_is_small() {
        let raw = vec![b'x'; MAX_DECODED_BYTES + 1];
        let bytes = frame(&raw, 1);
        assert!(bytes.len() < MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES);
        let error = decode::<Facts>(&bytes).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.message().contains("actual decoded length"));
        let facts = Facts {
            marker: "x".repeat(MAX_DECODED_BYTES),
        };
        assert_eq!(
            encode(&facts).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }
    #[test]
    fn codec_rejects_large_window_before_decoding_a_small_fact() {
        let raw = br#"{"marker":"bounded"}"#;
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), LEVEL).unwrap();
        encoder.window_log(WINDOW_LOG + 1).unwrap();
        // No pledged source size: frame advertises the selected streaming window.
        encoder.write_all(raw).unwrap();
        let mut bytes = MAGIC.to_vec();
        bytes.push(CODEC);
        bytes.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        let mut compressed = encoder.finish().unwrap();
        // Advertise an otherwise valid larger window without changing content.
        assert_eq!(compressed[4] & 0x20, 0);
        compressed[5] = ((WINDOW_LOG + 1 - 10) << 3) as u8;
        assert_eq!(
            zstd::stream::decode_all(compressed.as_slice()).unwrap(),
            raw
        );
        bytes.extend_from_slice(&compressed);
        assert_corrupt(&bytes);
    }
    #[test]
    fn codec_keeps_the_entropy_capacity_refusal_and_encoded_upper_bound() {
        let random: String = (0..6000)
            .map(|_| uuid::Uuid::new_v4().simple().to_string())
            .collect();
        let facts = Facts { marker: random };
        assert_eq!(
            encode(&facts).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        let lower = Facts {
            marker: (0..1221)
                .map(|_| uuid::Uuid::new_v4().simple().to_string())
                .collect(),
        };
        let bytes = encode(&lower).unwrap();
        assert!(bytes.len() <= MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES);
        assert_eq!(decode::<Facts>(&bytes).unwrap(), lower);
    }
}
