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
const PAGE_TYPE_INDEX: i32 = 2;
const PAGE_TYPE_DICTIONARY: i32 = 3;
const DATA_PAGE_FORMAT_V2: u32 = 2;
const BITSHUFFLE_HEADER_SIZE: usize = 16;
const BITSHUFFLE_TARGET_BLOCK_BYTES: usize = 8 * 1024;
const BITSHUFFLE_MIN_BLOCK_VALUES: usize = 8;

/// A validated data page body.  This is crate-private so neither the raw page
/// body nor storage protobuf DTOs become a connector public contract.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct StarRocksDecodedDataPage {
    pub(crate) body: Vec<u8>,
    pub(crate) num_values: usize,
    pub(crate) nullmap_size: usize,
    pub(crate) null_flags: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksIndexPageNodeType {
    Leaf,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksIndexPageEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) pointer: StarRocksPagePointer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksDecodedIndexPage {
    pub(crate) node_type: StarRocksIndexPageNodeType,
    pub(crate) entries: Vec<StarRocksIndexPageEntry>,
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
    let null_flags = if nullmap_size == 0 {
        None
    } else {
        let nullmap = &decoded.body[decoded.body.len() - nullmap_size..];
        match data.null_encoding.unwrap_or(0) {
            1 => {
                let mut flags = vec![0_u8; num_values];
                let decoded_size = lz4_flex::block::decompress_into(nullmap, &mut flags)
                    .map_err(|_| corrupt("cannot decompress StarRocks nullable page bitmap"))?;
                if decoded_size != num_values || flags.iter().any(|flag| *flag > 1) {
                    return Err(corrupt("StarRocks nullable page bitmap is invalid"));
                }
                Some(flags)
            }
            0 => Some(decode_bitshuffle_null_flags(nullmap, num_values)?),
            _ => {
                return Err(unsupported(
                    "unknown StarRocks nullable page bitmap encoding",
                ));
            }
        }
    };
    Ok(StarRocksDecodedDataPage {
        body: decoded.body,
        num_values,
        nullmap_size,
        null_flags,
    })
}

/// Decode an ordinal-index page. Index pages are always uncompressed in the
/// storage V1 snapshot and contain `key_len + key + page_pointer` entries.
pub(crate) fn decode_index_page(
    segment_path: &str,
    page_bytes: &[u8],
) -> Result<StarRocksDecodedIndexPage, ConnectorError> {
    let decoded = decode_page(segment_path, page_bytes, StarRocksCompression::None)?;
    if decoded.footer.r#type != Some(PAGE_TYPE_INDEX) {
        return Err(unsupported("StarRocks page is not an ordinal index page"));
    }
    let footer = decoded
        .footer
        .index_page_footer
        .ok_or_else(|| corrupt("StarRocks index page is missing its index footer"))?;
    let node_type = match footer.r#type {
        Some(1) => StarRocksIndexPageNodeType::Leaf,
        Some(2) => StarRocksIndexPageNodeType::Internal,
        _ => return Err(unsupported("unsupported StarRocks ordinal index node type")),
    };
    let count = usize::try_from(
        footer
            .num_entries
            .ok_or_else(|| corrupt("StarRocks index page is missing entry count"))?,
    )
    .map_err(|_| corrupt("StarRocks ordinal index entry count is out of range"))?;
    if count == 0 {
        return Err(corrupt("StarRocks ordinal index has no entries"));
    }
    let mut input = decoded.body.as_slice();
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let key_size = usize::try_from(read_varint(&mut input)?)
            .map_err(|_| corrupt("StarRocks ordinal index key size is out of range"))?;
        let key = take_bytes(&mut input, key_size)?.to_vec();
        let offset = read_varint(&mut input)?;
        let size = u32::try_from(read_varint(&mut input)?)
            .map_err(|_| corrupt("StarRocks ordinal index page size is out of range"))?;
        if size == 0 {
            return Err(corrupt("StarRocks ordinal index has a zero-sized page"));
        }
        entries.push(StarRocksIndexPageEntry {
            key,
            pointer: StarRocksPagePointer { offset, size },
        });
    }
    if !input.is_empty() {
        return Err(corrupt("StarRocks ordinal index page has trailing bytes"));
    }
    Ok(StarRocksDecodedIndexPage { node_type, entries })
}

/// Decode StarRocks' bitshuffle+LZ4 nullable bitmap. The bitmap is a
/// byte-per-row vector, padded to eight rows by the storage encoder.
fn decode_bitshuffle_null_flags(payload: &[u8], values: usize) -> Result<Vec<u8>, ConnectorError> {
    let padded = (values + 7) & !7;
    let mut output = vec![0_u8; padded];
    let mut input_offset = 0usize;
    let mut output_offset = 0usize;
    const BLOCK: usize = 8192;
    while output_offset < padded {
        let elements = (padded - output_offset).min(BLOCK);
        let header_end = input_offset
            .checked_add(4)
            .ok_or_else(|| corrupt("StarRocks bitshuffle bitmap header overflows"))?;
        if header_end > payload.len() {
            return Err(corrupt("truncated StarRocks bitshuffle bitmap header"));
        }
        let compressed_len = u32::from_be_bytes(
            payload[input_offset..header_end]
                .try_into()
                .map_err(|_| corrupt("invalid StarRocks bitshuffle bitmap header"))?,
        ) as usize;
        input_offset = header_end;
        let end = input_offset
            .checked_add(compressed_len)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| corrupt("StarRocks bitshuffle bitmap range is invalid"))?;
        let mut shuffled = vec![0_u8; elements];
        let decoded = lz4_flex::block::decompress_into(&payload[input_offset..end], &mut shuffled)
            .map_err(|_| corrupt("cannot decompress StarRocks bitshuffle nullable bitmap"))?;
        if decoded != elements {
            return Err(corrupt(
                "StarRocks bitshuffle nullable bitmap size is invalid",
            ));
        }
        bitunshuffle_one_byte(
            &shuffled,
            &mut output[output_offset..output_offset + elements],
        )?;
        input_offset = end;
        output_offset += elements;
    }
    if input_offset != payload.len() || output[..values].iter().any(|flag| *flag > 1) {
        return Err(corrupt("StarRocks bitshuffle nullable bitmap is invalid"));
    }
    Ok(output[..values].to_vec())
}

fn bitunshuffle_one_byte(input: &[u8], output: &mut [u8]) -> Result<(), ConnectorError> {
    if input.len() != output.len() || !input.len().is_multiple_of(8) {
        return Err(corrupt(
            "StarRocks bitshuffle bitmap dimensions are invalid",
        ));
    }
    for (input_block, output_block) in input.chunks_exact(8).zip(output.chunks_exact_mut(8)) {
        let mut matrix = u64::from_le_bytes(
            input_block
                .try_into()
                .map_err(|_| corrupt("invalid StarRocks bitshuffle bitmap matrix"))?,
        );
        matrix = transpose_bit_matrix(matrix);
        for (index, output) in output_block.iter_mut().enumerate() {
            *output = ((matrix >> (index * 8)) & 0xff) as u8;
        }
    }
    Ok(())
}

fn transpose_bit_matrix(mut value: u64) -> u64 {
    let mut temporary = (value ^ (value >> 7)) & 0x00AA00AA00AA00AA_u64;
    value ^= temporary ^ (temporary << 7);
    temporary = (value ^ (value >> 14)) & 0x0000CCCC0000CCCC_u64;
    value ^= temporary ^ (temporary << 14);
    temporary = (value ^ (value >> 28)) & 0x00000000F0F0F0F0_u64;
    value ^= temporary ^ (temporary << 28);
    value
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
    decode_binary_plain_values_inner(body, Some(expected_values))
}

fn decode_binary_plain_values_inner(
    body: &[u8],
    expected_values: Option<usize>,
) -> Result<Vec<Vec<u8>>, ConnectorError> {
    if body.len() < 4 {
        return Err(corrupt("invalid StarRocks binary PLAIN page body"));
    }
    let values = u32::from_le_bytes(
        body[body.len() - 4..]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks binary PLAIN value count"))?,
    ) as usize;
    if expected_values.is_some_and(|expected| values != expected) {
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

/// Decode a dictionary page, whose body itself uses binary PLAIN layout.
pub(crate) fn decode_binary_dictionary_page(
    segment_path: &str,
    page_bytes: &[u8],
    compression: StarRocksCompression,
) -> Result<Vec<Vec<u8>>, ConnectorError> {
    let decoded = decode_page(segment_path, page_bytes, compression)?;
    if decoded.footer.r#type != Some(PAGE_TYPE_DICTIONARY) {
        return Err(corrupt(
            "StarRocks dictionary page has an invalid page type",
        ));
    }
    decode_binary_plain_values_inner(&decoded.body, None)
}

/// Decode a variable-width DICT data page that stores its values as a PLAIN
/// binary payload. Bitshuffle dictionary codes remain a distinct format and
/// fail explicitly until the data-page bitshuffle decoder is installed.
pub(crate) fn decode_binary_dictionary_values(
    body: &[u8],
    expected_values: usize,
    dictionary: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>, ConnectorError> {
    if body.len() < 4 {
        return Err(corrupt("invalid StarRocks dictionary data page body"));
    }
    let mode = i32::from_le_bytes(
        body[..4]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks dictionary data page mode"))?,
    );
    match mode {
        2 => decode_binary_plain_values(&body[4..], expected_values),
        5 => {
            let _ = dictionary;
            Err(unsupported(
                "StarRocks bitshuffle dictionary codes are not implemented",
            ))
        }
        _ => Err(unsupported("unknown StarRocks dictionary data page mode")),
    }
}

/// Decode the hybrid RLE/bit-packed fixed-width page body used by StarRocks.
/// The output is normalized to little-endian fixed-width storage values.
pub(crate) fn decode_fixed_rle_values(
    body: &[u8],
    expected_values: usize,
    value_size: usize,
    bit_width: usize,
) -> Result<Vec<u8>, ConnectorError> {
    if body.len() < 4 || value_size == 0 || bit_width == 0 || bit_width > 128 {
        return Err(corrupt("invalid StarRocks fixed RLE page body"));
    }
    let values = u32::from_le_bytes(
        body[..4]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks RLE value count"))?,
    ) as usize;
    if values != expected_values {
        return Err(corrupt(
            "StarRocks RLE value count differs from page footer",
        ));
    }
    let mut reader = RleBitReader::new(&body[4..]);
    let mut remaining = values;
    let mut output = Vec::with_capacity(
        values
            .checked_mul(value_size)
            .ok_or_else(|| corrupt("StarRocks RLE output size overflows"))?,
    );
    while remaining > 0 {
        let indicator = reader
            .read_varint()
            .ok_or_else(|| corrupt("truncated StarRocks RLE run header"))?;
        if indicator == 0 {
            return Err(corrupt("StarRocks RLE run has zero length"));
        }
        if indicator & 1 == 0 {
            let count = (indicator >> 1) as usize;
            if count == 0 || count > remaining {
                return Err(corrupt("StarRocks RLE repeated run length is invalid"));
            }
            let value = reader
                .read_aligned_bits(bit_width)
                .ok_or_else(|| corrupt("truncated StarRocks RLE repeated value"))?;
            append_rle_value(&mut output, value, value_size, bit_width)?;
            let value = output[output.len() - value_size..].to_vec();
            for _ in 1..count {
                output.extend_from_slice(&value);
            }
            remaining -= count;
            continue;
        }
        let count = ((indicator >> 1) as usize)
            .checked_mul(8)
            .ok_or_else(|| corrupt("StarRocks RLE literal run length overflows"))?;
        if count == 0 {
            return Err(corrupt("StarRocks RLE literal run has zero length"));
        }
        let emit = count.min(remaining);
        for _ in 0..emit {
            let value = reader
                .read_bits(bit_width)
                .ok_or_else(|| corrupt("truncated StarRocks RLE literal value"))?;
            append_rle_value(&mut output, value, value_size, bit_width)?;
        }
        let skip = count - emit;
        if skip > 0
            && !reader.skip_bits(
                skip.checked_mul(bit_width)
                    .ok_or_else(|| corrupt("StarRocks RLE literal skip overflows"))?,
            )
        {
            return Err(corrupt("truncated StarRocks RLE literal padding"));
        }
        remaining -= emit;
    }
    Ok(output)
}

/// Decode StarRocks' fixed-width bitshuffle-plus-LZ4 data payload. The page
/// header records both logical and padded value counts; decoding rejects a
/// mismatch rather than treating trailing padding as user rows.
pub(crate) fn decode_bitshuffle_fixed_values(
    body: &[u8],
    expected_values: usize,
    expected_element_size: usize,
) -> Result<Vec<u8>, ConnectorError> {
    if body.len() < BITSHUFFLE_HEADER_SIZE || expected_element_size == 0 {
        return Err(corrupt("invalid StarRocks bitshuffle page body"));
    }
    let values = u32::from_le_bytes(
        body[..4]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks bitshuffle value count"))?,
    ) as usize;
    let encoded_size = u32::from_le_bytes(
        body[4..8]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks bitshuffle encoded size"))?,
    ) as usize;
    let padded_values = u32::from_le_bytes(
        body[8..12]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks bitshuffle padded count"))?,
    ) as usize;
    let element_size = u32::from_le_bytes(
        body[12..16]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks bitshuffle element size"))?,
    ) as usize;
    if values != expected_values
        || element_size != expected_element_size
        || padded_values != align_up_8(values)
        || encoded_size < BITSHUFFLE_HEADER_SIZE
        || encoded_size != body.len()
    {
        return Err(corrupt(
            "StarRocks bitshuffle page header conflicts with frozen values",
        ));
    }
    let payload = &body[BITSHUFFLE_HEADER_SIZE..];
    let output_size = padded_values
        .checked_mul(element_size)
        .ok_or_else(|| corrupt("StarRocks bitshuffle output size overflows"))?;
    let mut output = vec![0_u8; output_size];
    let block_values = bitshuffle_block_values(element_size);
    let mut payload_offset = 0usize;
    let mut output_offset = 0usize;
    while output_offset < output.len() {
        let values = ((output.len() - output_offset) / element_size).min(block_values);
        let values = values - values % 8;
        if values == 0 {
            return Err(corrupt(
                "StarRocks bitshuffle block is not eight-value aligned",
            ));
        }
        let compressed_end = payload_offset
            .checked_add(4)
            .ok_or_else(|| corrupt("StarRocks bitshuffle block header overflows"))?;
        if compressed_end > payload.len() {
            return Err(corrupt("truncated StarRocks bitshuffle block header"));
        }
        let compressed_size = u32::from_be_bytes(
            payload[payload_offset..compressed_end]
                .try_into()
                .map_err(|_| corrupt("invalid StarRocks bitshuffle block header"))?,
        ) as usize;
        payload_offset = compressed_end;
        let end = payload_offset
            .checked_add(compressed_size)
            .filter(|end| *end <= payload.len())
            .ok_or_else(|| corrupt("StarRocks bitshuffle block range is invalid"))?;
        let bytes = values
            .checked_mul(element_size)
            .ok_or_else(|| corrupt("StarRocks bitshuffle block size overflows"))?;
        let mut shuffled = vec![0_u8; bytes];
        let decoded =
            lz4_flex::block::decompress_into(&payload[payload_offset..end], &mut shuffled)
                .map_err(|_| corrupt("cannot decompress StarRocks bitshuffle data block"))?;
        if decoded != bytes {
            return Err(corrupt("StarRocks bitshuffle data block size is invalid"));
        }
        bitunshuffle(
            &shuffled,
            &mut output[output_offset..output_offset + bytes],
            values,
            element_size,
        )?;
        payload_offset = end;
        output_offset += bytes;
    }
    if payload_offset != payload.len() {
        return Err(corrupt("StarRocks bitshuffle payload has trailing bytes"));
    }
    output.truncate(
        values
            .checked_mul(element_size)
            .ok_or_else(|| corrupt("StarRocks bitshuffle value size overflows"))?,
    );
    Ok(output)
}

fn bitshuffle_block_values(element_size: usize) -> usize {
    let values = (BITSHUFFLE_TARGET_BLOCK_BYTES / element_size) / 8 * 8;
    values.max(BITSHUFFLE_MIN_BLOCK_VALUES)
}

fn align_up_8(value: usize) -> usize {
    (value + 7) & !7
}

fn bitunshuffle(
    input: &[u8],
    output: &mut [u8],
    values: usize,
    element_size: usize,
) -> Result<(), ConnectorError> {
    if !values.is_multiple_of(8)
        || input.len() != values * element_size
        || output.len() != input.len()
    {
        return Err(corrupt("StarRocks bitshuffle dimensions are invalid"));
    }
    let mut transposed = vec![0_u8; input.len()];
    let rows = values / 8;
    for byte in 0..element_size {
        for row in 0..rows {
            for bit in 0..8 {
                transposed[row * 8 * element_size + byte * 8 + bit] =
                    input[(byte * 8 + bit) * rows + row];
            }
        }
    }
    for byte_bit in (0..8 * element_size).step_by(8) {
        for group in 0..rows {
            let start = group * 8 * element_size + byte_bit;
            let matrix = u64::from_le_bytes(
                transposed[start..start + 8]
                    .try_into()
                    .map_err(|_| corrupt("invalid StarRocks bitshuffle bit matrix"))?,
            );
            let matrix = transpose_bit_matrix(matrix);
            for index in 0..8 {
                output[group * 8 * element_size + byte_bit / 8 + index * element_size] =
                    ((matrix >> (index * 8)) & 0xff) as u8;
            }
        }
    }
    Ok(())
}

fn append_rle_value(
    output: &mut Vec<u8>,
    value: u128,
    value_size: usize,
    bit_width: usize,
) -> Result<(), ConnectorError> {
    if bit_width == 1 && value > 1 {
        return Err(corrupt("invalid StarRocks BOOLEAN RLE value"));
    }
    for index in 0..value_size {
        output.push(((value >> (index * 8)) & 0xff) as u8);
    }
    Ok(())
}

struct RleBitReader<'a> {
    bytes: &'a [u8],
    bit_offset: usize,
}

impl<'a> RleBitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            bit_offset: 0,
        }
    }

    fn read_varint(&mut self) -> Option<u32> {
        let mut value = 0u32;
        for shift in (0..35).step_by(7) {
            let byte = self.read_aligned_byte()?;
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }

    fn read_aligned_bits(&mut self, width: usize) -> Option<u128> {
        self.align();
        self.read_bits(width)
    }

    fn read_bits(&mut self, width: usize) -> Option<u128> {
        let end = self.bit_offset.checked_add(width)?;
        if end > self.bytes.len().checked_mul(8)? {
            return None;
        }
        let mut value = 0u128;
        for bit in 0..width {
            let absolute = self.bit_offset + bit;
            value |= u128::from((self.bytes[absolute / 8] >> (absolute % 8)) & 1) << bit;
        }
        self.bit_offset = end;
        Some(value)
    }

    fn skip_bits(&mut self, width: usize) -> bool {
        let Some(end) = self.bit_offset.checked_add(width) else {
            return false;
        };
        if end > self.bytes.len().saturating_mul(8) {
            return false;
        }
        self.bit_offset = end;
        true
    }

    fn read_aligned_byte(&mut self) -> Option<u8> {
        self.align();
        let index = self.bit_offset / 8;
        let byte = *self.bytes.get(index)?;
        self.bit_offset = self.bit_offset.checked_add(8)?;
        Some(byte)
    }

    fn align(&mut self) {
        self.bit_offset = (self.bit_offset + 7) & !7;
    }
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
        Some(PAGE_TYPE_DATA) | Some(PAGE_TYPE_INDEX) | Some(PAGE_TYPE_DICTIONARY) => Ok(()),
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
        if shift == 63 && byte > 1 {
            return Err(corrupt("overflowing StarRocks protobuf varint"));
        }
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
    fn decodes_lz4_nullable_data_page_flags() {
        let mut body = 2u32.to_le_bytes().to_vec();
        body.extend_from_slice(&10i64.to_le_bytes());
        body.extend_from_slice(&99i64.to_le_bytes());
        let nullmap = lz4_flex::block::compress(&[0, 1]);
        body.extend_from_slice(&nullmap);
        let footer = PageFooterPb {
            r#type: Some(PAGE_TYPE_DATA),
            uncompressed_size: Some(body.len() as u32),
            data_page_footer: Some(DataPageFooterPb {
                first_ordinal: Some(0),
                num_values: Some(2),
                nullmap_size: Some(nullmap.len() as u32),
                format_version: Some(DATA_PAGE_FORMAT_V2),
                null_encoding: Some(1),
            }),
            index_page_footer: None,
        }
        .encode_to_vec();
        body.extend_from_slice(&footer);
        body.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        body.extend_from_slice(&crc32c(&body).to_le_bytes());
        let decoded = decode_data_page("segment.dat", &body, StarRocksCompression::None).unwrap();
        assert_eq!(decoded.null_flags, Some(vec![0, 1]));
        assert_eq!(decoded.nullmap_size, nullmap.len());
    }

    #[test]
    fn decodes_bitshuffle_nullable_bitmap() {
        let compressed = lz4_flex::block::compress(&[0_u8; 8]);
        let mut payload = (compressed.len() as u32).to_be_bytes().to_vec();
        payload.extend_from_slice(&compressed);
        assert_eq!(
            decode_bitshuffle_null_flags(&payload, 3).unwrap(),
            vec![0, 0, 0]
        );
        assert_eq!(
            decode_bitshuffle_null_flags(&payload[..3], 3)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn decodes_fixed_width_bitshuffle_data_values() {
        let values = [1_u8, 2, 3, 4, 5, 6, 7, 8];
        let shuffled = transpose_bit_matrix(u64::from_le_bytes(values)).to_le_bytes();
        let compressed = lz4_flex::block::compress(&shuffled);
        let mut body = 8u32.to_le_bytes().to_vec();
        body.extend_from_slice(
            &(BITSHUFFLE_HEADER_SIZE as u32 + 4 + compressed.len() as u32).to_le_bytes(),
        );
        body.extend_from_slice(&8u32.to_le_bytes());
        body.extend_from_slice(&1u32.to_le_bytes());
        body.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        body.extend_from_slice(&compressed);
        assert_eq!(decode_bitshuffle_fixed_values(&body, 8, 1).unwrap(), values);
    }

    #[test]
    fn decodes_fixed_rle_repeated_and_literal_runs() {
        let mut repeated = 3u32.to_le_bytes().to_vec();
        repeated.push(6); // repeated run of three values
        repeated.extend_from_slice(&7i64.to_le_bytes());
        assert_eq!(
            decode_fixed_rle_values(&repeated, 3, 8, 64).unwrap(),
            [7i64.to_le_bytes(), 7i64.to_le_bytes(), 7i64.to_le_bytes()].concat()
        );

        let mut literal = 3u32.to_le_bytes().to_vec();
        literal.push(3); // one eight-value literal group, of which only three are emitted
        literal.extend_from_slice(&[0b0000_0101]);
        assert_eq!(
            decode_fixed_rle_values(&literal, 3, 1, 1).unwrap(),
            vec![1, 0, 1]
        );
    }

    #[test]
    fn decodes_binary_dictionary_page_and_plain_mode_data() {
        let dictionary_body = vec![b'a', b'b', b'c', 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0];
        let footer = PageFooterPb {
            r#type: Some(PAGE_TYPE_DICTIONARY),
            uncompressed_size: Some(dictionary_body.len() as u32),
            data_page_footer: None,
            index_page_footer: None,
        }
        .encode_to_vec();
        let mut page = dictionary_body.clone();
        page.extend_from_slice(&footer);
        page.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        page.extend_from_slice(&crc32c(&page).to_le_bytes());
        let dictionary =
            decode_binary_dictionary_page("segment.dat", &page, StarRocksCompression::None)
                .unwrap();
        assert_eq!(dictionary, vec![b"a".to_vec(), b"bc".to_vec()]);

        let mut data = 2i32.to_le_bytes().to_vec();
        data.extend_from_slice(&dictionary_body);
        assert_eq!(
            decode_binary_dictionary_values(&data, 2, &dictionary).unwrap(),
            dictionary
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
