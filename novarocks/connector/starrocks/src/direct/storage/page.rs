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

//! Provider-private StarRocks data-page framing and scalar PLAIN decoding.
//!
//! Page bytes are persisted storage data, never connector carrier payloads.
//! The decoder therefore validates the checksum, protobuf wire shape and enum
//! values before exposing a body to the Arrow read kernel.  It intentionally
//! does not accept unknown fields: a newer storage format must be negotiated
//! explicitly instead of being silently interpreted as this V1 snapshot.

use std::io::{Cursor, Read};

use crc32c::crc32c;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use prost::Message;

use super::segment::{StarRocksCompression, StarRocksPagePointer};

const PAGE_TRAILER_SIZE: usize = 8;
const PAGE_TYPE_DATA: i32 = 1;
const PAGE_TYPE_DICTIONARY: i32 = 2;
const DATA_PAGE_FORMAT_V2: u32 = 2;

/// A validated data page body.  This is crate-private so neither the raw page
/// body nor storage protobuf DTOs become a connector public contract.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct StarRocksDecodedDataPage {
    pub(crate) body: Vec<u8>,
    pub(crate) num_values: usize,
    pub(crate) nullmap_size: usize,
}

/// Decode one exact page range, validate its checksum/footer, and materialize
/// the uncompressed body.  Only data pages are accepted by this entry point.
pub(crate) fn decode_data_page(
    segment_path: &str,
    page_bytes: &[u8],
    compression: StarRocksCompression,
) -> Result<StarRocksDecodedDataPage, ConnectorError> {
    let decoded = decode_page(segment_path, page_bytes, compression)?;
    if decoded.footer.r#type != Some(PAGE_TYPE_DATA) {
        return Err(unsupported("StarRocks page is not a data page"));
    }
    let data = decoded
        .footer
        .data_page_footer
        .ok_or_else(|| corrupt("StarRocks data page is missing its data footer"))?;
    let num_values = usize::try_from(
        data.num_values
            .ok_or_else(|| corrupt("StarRocks data page is missing num_values"))?,
    )
    .map_err(|_| corrupt("StarRocks data page num_values is out of range"))?;
    if num_values == 0 {
        return Err(corrupt("StarRocks data page has zero values"));
    }
    let nullmap_size = usize::try_from(data.nullmap_size.unwrap_or(0))
        .map_err(|_| corrupt("StarRocks data page nullmap_size is out of range"))?;
    if nullmap_size > decoded.body.len() {
        return Err(corrupt("StarRocks data page nullmap exceeds page body"));
    }
    if nullmap_size > 0 && data.format_version.unwrap_or(1) != DATA_PAGE_FORMAT_V2 {
        return Err(unsupported(
            "StarRocks nullable data page format is not supported",
        ));
    }
    Ok(StarRocksDecodedDataPage {
        body: decoded.body,
        num_values,
        nullmap_size,
    })
}

/// Slice an exact page range from a segment object.  The direct reader never
/// probes adjacent offsets or later tablet versions.
pub(crate) fn page_slice<'a>(
    segment_path: &str,
    segment: &'a [u8],
    pointer: &StarRocksPagePointer,
) -> Result<&'a [u8], ConnectorError> {
    let offset = usize::try_from(pointer.offset)
        .map_err(|_| corrupt("StarRocks page offset is out of range"))?;
    let size = usize::try_from(pointer.size)
        .map_err(|_| corrupt("StarRocks page size is out of range"))?;
    let end = offset
        .checked_add(size)
        .ok_or_else(|| corrupt("StarRocks page range overflows"))?;
    if end > segment.len() {
        let _ = segment_path;
        return Err(corrupt("StarRocks page range exceeds segment object"));
    }
    Ok(&segment[offset..end])
}

/// Decode fixed-width PLAIN values.  Values remain little-endian storage
/// bytes; typed Arrow builders own the final type conversion.
pub(crate) fn decode_fixed_plain_values(
    body: &[u8],
    expected_values: usize,
    value_size: usize,
) -> Result<Vec<u8>, ConnectorError> {
    if body.len() < 4 || value_size == 0 {
        return Err(corrupt("invalid StarRocks fixed PLAIN page body"));
    }
    let values = u32::from_le_bytes(
        body[..4]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks fixed PLAIN value count"))?,
    ) as usize;
    if values != expected_values {
        return Err(corrupt(
            "StarRocks fixed PLAIN value count differs from page footer",
        ));
    }
    let value_bytes = values
        .checked_mul(value_size)
        .ok_or_else(|| corrupt("StarRocks fixed PLAIN value size overflows"))?;
    if body.len() != 4 + value_bytes {
        return Err(corrupt("StarRocks fixed PLAIN body length is invalid"));
    }
    Ok(body[4..].to_vec())
}

/// Decode variable-width PLAIN values using StarRocks' offsets trailer.
pub(crate) fn decode_binary_plain_values(
    body: &[u8],
    expected_values: usize,
) -> Result<Vec<Vec<u8>>, ConnectorError> {
    if body.len() < 4 {
        return Err(corrupt("invalid StarRocks binary PLAIN page body"));
    }
    let values = u32::from_le_bytes(
        body[body.len() - 4..]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks binary PLAIN value count"))?,
    ) as usize;
    if values != expected_values {
        return Err(corrupt(
            "StarRocks binary PLAIN value count differs from page footer",
        ));
    }
    let offsets_size = values
        .checked_mul(4)
        .and_then(|size| size.checked_add(4))
        .ok_or_else(|| corrupt("StarRocks binary PLAIN offsets overflow"))?;
    if offsets_size > body.len() {
        return Err(corrupt("StarRocks binary PLAIN offsets exceed page body"));
    }
    let offsets_begin = body.len() - offsets_size;
    let mut output = Vec::with_capacity(values);
    let mut previous = 0usize;
    for index in 0..values {
        let start = u32::from_le_bytes(
            body[offsets_begin + index * 4..offsets_begin + (index + 1) * 4]
                .try_into()
                .map_err(|_| corrupt("invalid StarRocks binary PLAIN offset"))?,
        ) as usize;
        let end = if index + 1 == values {
            offsets_begin
        } else {
            u32::from_le_bytes(
                body[offsets_begin + (index + 1) * 4..offsets_begin + (index + 2) * 4]
                    .try_into()
                    .map_err(|_| corrupt("invalid StarRocks binary PLAIN offset"))?,
            ) as usize
        };
        if start < previous || end < start || end > offsets_begin {
            return Err(corrupt("StarRocks binary PLAIN offsets are invalid"));
        }
        previous = end;
        output.push(body[start..end].to_vec());
    }
    Ok(output)
}

struct DecodedPage {
    footer: PageFooterPb,
    body: Vec<u8>,
}

fn decode_page(
    segment_path: &str,
    page_bytes: &[u8],
    compression: StarRocksCompression,
) -> Result<DecodedPage, ConnectorError> {
    if page_bytes.len() < PAGE_TRAILER_SIZE {
        return Err(corrupt("StarRocks page is smaller than its trailer"));
    }
    let trailer = page_bytes.len() - PAGE_TRAILER_SIZE;
    let footer_size = u32::from_le_bytes(
        page_bytes[trailer..trailer + 4]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks page footer size"))?,
    ) as usize;
    let expected_checksum = u32::from_le_bytes(
        page_bytes[trailer + 4..]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks page checksum"))?,
    );
    if crc32c(&page_bytes[..trailer + 4]) != expected_checksum {
        return Err(corrupt("StarRocks page checksum mismatch"));
    }
    let footer_begin = trailer
        .checked_sub(footer_size)
        .ok_or_else(|| corrupt("StarRocks page footer range is invalid"))?;
    let footer_bytes = &page_bytes[footer_begin..trailer];
    validate_footer_wire(footer_bytes)?;
    let footer = PageFooterPb::decode(footer_bytes)
        .map_err(|_| corrupt("invalid StarRocks page footer protobuf"))?;
    validate_page_footer(&footer)?;
    let expected_size = usize::try_from(
        footer
            .uncompressed_size
            .ok_or_else(|| corrupt("StarRocks page footer is missing uncompressed_size"))?,
    )
    .map_err(|_| corrupt("StarRocks page uncompressed_size is out of range"))?;
    if expected_size == 0 {
        return Err(corrupt("StarRocks page uncompressed_size is zero"));
    }
    let compressed = &page_bytes[..footer_begin];
    let body = if compressed.len() == expected_size {
        compressed.to_vec()
    } else {
        match compression {
            StarRocksCompression::None => {
                return Err(corrupt(
                    "uncompressed StarRocks page has an invalid body size",
                ));
            }
            StarRocksCompression::Lz4Frame => {
                let mut decoder = lz4_flex::frame::FrameDecoder::new(Cursor::new(compressed));
                let mut out = Vec::with_capacity(expected_size);
                decoder
                    .read_to_end(&mut out)
                    .map_err(|_| corrupt("cannot decompress StarRocks LZ4 page body"))?;
                if out.len() != expected_size {
                    return Err(corrupt("decompressed StarRocks page has an invalid size"));
                }
                out
            }
        }
    };
    let _ = segment_path;
    Ok(DecodedPage { footer, body })
}

fn validate_page_footer(footer: &PageFooterPb) -> Result<(), ConnectorError> {
    match footer.r#type {
        Some(PAGE_TYPE_DATA) | Some(PAGE_TYPE_DICTIONARY) => Ok(()),
        Some(_) => Err(unsupported("unsupported StarRocks page type")),
        None => Err(corrupt("StarRocks page footer is missing page type")),
    }
}

/// Validate the subset protobuf wire shape before Prost decodes it.  Prost
/// purposefully preserves protobuf forward compatibility by ignoring unknown
/// fields; storage snapshot readers cannot do that safely.
fn validate_footer_wire(input: &[u8]) -> Result<(), ConnectorError> {
    validate_message(input, &[1, 2, 7, 8], |field, nested| match field {
        7 => validate_message(nested, &[1, 2, 3, 20, 21], |_, _| Ok(())),
        8 => validate_message(nested, &[1, 2], |_, _| Ok(())),
        _ => Ok(()),
    })
}

fn validate_message(
    mut input: &[u8],
    allowed: &[u32],
    mut nested: impl FnMut(u32, &[u8]) -> Result<(), ConnectorError>,
) -> Result<(), ConnectorError> {
    while !input.is_empty() {
        let key = read_varint(&mut input)?;
        let field = (key >> 3) as u32;
        let wire = (key & 7) as u8;
        if field == 0 || !allowed.contains(&field) {
            return Err(unsupported("unknown field in StarRocks page footer"));
        }
        match wire {
            0 => {
                let _ = read_varint(&mut input)?;
            }
            1 => {
                let _ = take_bytes(&mut input, 8)?;
            }
            2 => {
                let len = usize::try_from(read_varint(&mut input)?)
                    .map_err(|_| corrupt("StarRocks protobuf length is out of range"))?;
                let value = take_bytes(&mut input, len)?;
                nested(field, value)?;
            }
            5 => {
                let _ = take_bytes(&mut input, 4)?;
            }
            _ => return Err(corrupt("invalid wire type in StarRocks page footer")),
        }
    }
    Ok(())
}

fn read_varint(input: &mut &[u8]) -> Result<u64, ConnectorError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *input
            .first()
            .ok_or_else(|| corrupt("truncated StarRocks protobuf varint"))?;
        *input = &input[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(corrupt("overflowing StarRocks protobuf varint"))
}

fn take_bytes<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], ConnectorError> {
    if len > input.len() {
        return Err(corrupt("truncated StarRocks protobuf field"));
    }
    let (value, rest) = input.split_at(len);
    *input = rest;
    Ok(value)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[derive(Clone, PartialEq, Message)]
struct DataPageFooterPb {
    #[prost(uint64, optional, tag = "1")]
    first_ordinal: Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    num_values: Option<u64>,
    #[prost(uint32, optional, tag = "3")]
    nullmap_size: Option<u32>,
    #[prost(uint32, optional, tag = "20")]
    format_version: Option<u32>,
    #[prost(int32, optional, tag = "21")]
    null_encoding: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexPageFooterPb {
    #[prost(uint32, optional, tag = "1")]
    num_entries: Option<u32>,
    #[prost(int32, optional, tag = "2")]
    r#type: Option<i32>,
}

#[derive(Clone, PartialEq, Message)]
struct PageFooterPb {
    #[prost(int32, optional, tag = "1")]
    r#type: Option<i32>,
    #[prost(uint32, optional, tag = "2")]
    uncompressed_size: Option<u32>,
    #[prost(message, optional, tag = "7")]
    data_page_footer: Option<DataPageFooterPb>,
    #[prost(message, optional, tag = "8")]
    index_page_footer: Option<IndexPageFooterPb>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_page(body: &[u8], values: u64) -> Vec<u8> {
        let footer = PageFooterPb {
            r#type: Some(PAGE_TYPE_DATA),
            uncompressed_size: Some(body.len() as u32),
            data_page_footer: Some(DataPageFooterPb {
                first_ordinal: Some(0),
                num_values: Some(values),
                nullmap_size: Some(0),
                format_version: None,
                null_encoding: None,
            }),
            index_page_footer: None,
        }
        .encode_to_vec();
        let mut page = body.to_vec();
        page.extend_from_slice(&footer);
        page.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        page.extend_from_slice(&crc32c(&page).to_le_bytes());
        page
    }

    #[test]
    fn decodes_checked_data_page_and_plain_values() {
        let mut body = 2u32.to_le_bytes().to_vec();
        body.extend_from_slice(&10i64.to_le_bytes());
        body.extend_from_slice(&20i64.to_le_bytes());
        let decoded = decode_data_page(
            "segment.dat",
            &data_page(&body, 2),
            StarRocksCompression::None,
        )
        .unwrap();
        assert_eq!(decoded.num_values, 2);
        assert_eq!(
            decode_fixed_plain_values(&decoded.body, 2, 8)
                .unwrap()
                .len(),
            16
        );

        let binary = [b'a', b'b', b'c', 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0];
        assert_eq!(
            decode_binary_plain_values(&binary, 2).unwrap(),
            vec![b"a".to_vec(), b"bc".to_vec()]
        );
    }

    #[test]
    fn rejects_bad_checksum_unknown_field_and_enum() {
        let mut body = 1u32.to_le_bytes().to_vec();
        body.extend_from_slice(&1i64.to_le_bytes());
        let mut checksum = data_page(&body, 1);
        checksum[0] ^= 1;
        assert_eq!(
            decode_data_page("segment.dat", &checksum, StarRocksCompression::None)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let mut footer = PageFooterPb {
            r#type: Some(PAGE_TYPE_DATA),
            uncompressed_size: Some(body.len() as u32),
            data_page_footer: Some(DataPageFooterPb {
                first_ordinal: Some(0),
                num_values: Some(1),
                nullmap_size: None,
                format_version: None,
                null_encoding: None,
            }),
            index_page_footer: None,
        }
        .encode_to_vec();
        footer.extend_from_slice(&[0x18, 0x01]);
        let mut unknown = body.clone();
        unknown.extend_from_slice(&footer);
        unknown.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        unknown.extend_from_slice(&crc32c(&unknown).to_le_bytes());
        assert_eq!(
            decode_data_page("segment.dat", &unknown, StarRocksCompression::None)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );

        let unsupported_footer = PageFooterPb {
            r#type: Some(999),
            uncompressed_size: Some(body.len() as u32),
            data_page_footer: None,
            index_page_footer: None,
        }
        .encode_to_vec();
        let mut unsupported = body;
        unsupported.extend_from_slice(&unsupported_footer);
        unsupported.extend_from_slice(&(unsupported_footer.len() as u32).to_le_bytes());
        unsupported.extend_from_slice(&crc32c(&unsupported).to_le_bytes());
        assert_eq!(
            decode_data_page("segment.dat", &unsupported, StarRocksCompression::None)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }
}
