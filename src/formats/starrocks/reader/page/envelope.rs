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
//! Page envelope parser and decompression helpers.
//!
//! This module validates page checksum/footer trailers and materializes
//! uncompressed page bodies for higher-level value decoders.
//!
//! Current limitations:
//! - Page compression supports only `LZ4_FRAME`.
//! - Only footer fields needed by scalar native reads are parsed.

use std::io::{Cursor, Read};

use crc32c::crc32c;
use prost::Message;

use crate::formats::starrocks::segment::StarRocksPagePointer;

use super::super::constants::*;
use super::footer::PageFooterPbLite;

/// Page body after checksum/footer validation and optional decompression.
pub(crate) struct DecodedPageEnvelope {
    pub(crate) footer: PageFooterPbLite,
    pub(crate) body: Vec<u8>,
}

pub(crate) fn decode_page_envelope(
    segment_path: &str,
    page_bytes: &[u8],
    column_compression: i32,
) -> Result<DecodedPageEnvelope, String> {
    if page_bytes.len() < 8 {
        return Err(format!(
            "invalid page bytes (too small): segment={}, page_size={}",
            segment_path,
            page_bytes.len()
        ));
    }

    let expected_checksum = u32::from_le_bytes(
        page_bytes[page_bytes.len() - 4..]
            .try_into()
            .map_err(|_| "decode page checksum failed".to_string())?,
    );
    let actual_checksum = crc32c(&page_bytes[..page_bytes.len() - 4]);
    if actual_checksum != expected_checksum {
        return Err(format!(
            "page checksum mismatch: segment={}, actual={}, expected={}",
            segment_path, actual_checksum, expected_checksum
        ));
    }

    let footer_size = u32::from_le_bytes(
        page_bytes[page_bytes.len() - 8..page_bytes.len() - 4]
            .try_into()
            .map_err(|_| "decode page footer size failed".to_string())?,
    ) as usize;
    let footer_start = page_bytes
        .len()
        .checked_sub(8 + footer_size)
        .ok_or_else(|| {
            format!(
                "invalid page footer size: segment={}, page_size={}, footer_size={}",
                segment_path,
                page_bytes.len(),
                footer_size
            )
        })?;
    let footer = PageFooterPbLite::decode(&page_bytes[footer_start..page_bytes.len() - 8])
        .map_err(|e| {
            format!(
                "decode PageFooterPB failed: segment={}, footer_size={}, error={}",
                segment_path, footer_size, e
            )
        })?;

    let mut body = page_bytes[..footer_start].to_vec();
    let uncompressed_size =
        usize::try_from(footer.uncompressed_size.unwrap_or(0)).map_err(|_| {
            format!(
                "invalid uncompressed_size in page footer: segment={}, uncompressed_size={:?}",
                segment_path, footer.uncompressed_size
            )
        })?;
    if uncompressed_size == 0 {
        return Err(format!(
            "missing or zero uncompressed_size in page footer: segment={}",
            segment_path
        ));
    }
    if body.len() != uncompressed_size {
        body = decompress_page_body(segment_path, &body, column_compression, uncompressed_size)?;
    }

    Ok(DecodedPageEnvelope { footer, body })
}

pub(crate) fn slice_page_bytes<'a>(
    segment_path: &str,
    segment_bytes: &'a [u8],
    page_ptr: &StarRocksPagePointer,
    page_label: &str,
    unique_id: Option<u32>,
) -> Result<&'a [u8], String> {
    let page_offset = usize::try_from(page_ptr.offset).map_err(|_| {
        format!(
            "invalid {} offset in segment column meta: segment={}, unique_id={:?}, offset={}",
            page_label, segment_path, unique_id, page_ptr.offset
        )
    })?;
    let page_size = usize::try_from(page_ptr.size).map_err(|_| {
        format!(
            "invalid {} size in segment column meta: segment={}, unique_id={:?}, size={}",
            page_label, segment_path, unique_id, page_ptr.size
        )
    })?;
    let page_end = page_offset.checked_add(page_size).ok_or_else(|| {
        format!(
            "{} range overflow in segment column meta: segment={}, unique_id={:?}, offset={}, size={}",
            page_label, segment_path, unique_id, page_offset, page_size
        )
    })?;
    if page_end > segment_bytes.len() {
        return Err(format!(
            "{} out of segment bounds: segment={}, unique_id={:?}, offset={}, size={}, segment_size={}",
            page_label,
            segment_path,
            unique_id,
            page_offset,
            page_size,
            segment_bytes.len()
        ));
    }
    Ok(&segment_bytes[page_offset..page_end])
}

fn decompress_page_body(
    segment_path: &str,
    compressed_body: &[u8],
    compression: i32,
    expected_uncompressed_size: usize,
) -> Result<Vec<u8>, String> {
    match compression {
        COMPRESSION_NO_COMPRESSION => {
            if compressed_body.len() != expected_uncompressed_size {
                return Err(format!(
                    "NO_COMPRESSION page body size mismatch: segment={}, expected_uncompressed_size={}, actual_size={}",
                    segment_path,
                    expected_uncompressed_size,
                    compressed_body.len()
                ));
            }
            Ok(compressed_body.to_vec())
        }
        COMPRESSION_LZ4_FRAME => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(Cursor::new(compressed_body));
            let mut out = Vec::with_capacity(expected_uncompressed_size);
            decoder.read_to_end(&mut out).map_err(|e| {
                format!(
                    "decompress LZ4_FRAME page body failed: segment={}, compressed_size={}, error={}",
                    segment_path,
                    compressed_body.len(),
                    e
                )
            })?;
            if out.len() != expected_uncompressed_size {
                return Err(format!(
                    "LZ4_FRAME page body size mismatch: segment={}, expected_uncompressed_size={}, actual_uncompressed_size={}",
                    segment_path,
                    expected_uncompressed_size,
                    out.len()
                ));
            }
            Ok(out)
        }
        other => Err(format!(
            "unsupported page compression for rust native reader: segment={}, compression={}, supported=[{},{}]",
            segment_path, other, COMPRESSION_NO_COMPRESSION, COMPRESSION_LZ4_FRAME
        )),
    }
}
