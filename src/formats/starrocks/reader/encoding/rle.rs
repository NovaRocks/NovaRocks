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
//! RLE page decoding helpers.
//!
//! This module decodes StarRocks RLE page payloads into little-endian value
//! bytes for fixed-width scalar columns.
//!
//! Current limitations:
//! - Supports scalar fixed-width output kinds only.
//! - BOOLEAN values are validated as `{0,1}` only.

use super::super::column_state::OutputColumnKind;
use super::super::constants::*;

pub(crate) fn decode_rle_page_body(
    segment_path: &str,
    body: &[u8],
    output_kind: OutputColumnKind,
    elem_size: usize,
) -> Result<(usize, usize, Vec<u8>), String> {
    if body.len() < RLE_PAGE_HEADER_SIZE {
        return Err(format!(
            "invalid RLE page body (too small): segment={}, body_size={}",
            segment_path,
            body.len()
        ));
    }
    let num_values = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| "decode RLE page num_values failed".to_string())?,
    ) as usize;
    if num_values == 0 {
        return Err(format!(
            "invalid RLE page header num_values=0: segment={}",
            segment_path
        ));
    }

    let bit_width = output_kind.rle_bit_width(elem_size);
    let output_bytes = num_values
        .checked_mul(elem_size)
        .ok_or_else(|| "RLE page output size overflow".to_string())?;
    let mut values = Vec::with_capacity(output_bytes);
    let mut remaining = num_values;
    let mut reader = RleBitReader::new(&body[RLE_PAGE_HEADER_SIZE..]);

    while remaining > 0 {
        let indicator = reader.read_vlq_u32().ok_or_else(|| {
            format!(
                "decode RLE run header failed: segment={}, remaining_values={}",
                segment_path, remaining
            )
        })?;
        if indicator == 0 {
            return Err(format!(
                "invalid RLE run header with zero indicator: segment={}",
                segment_path
            ));
        }

        if indicator & 1 == 0 {
            let run_len = (indicator >> 1) as usize;
            if run_len == 0 {
                return Err(format!(
                    "invalid RLE repeated run length=0: segment={}",
                    segment_path
                ));
            }
            if run_len > remaining {
                return Err(format!(
                    "RLE repeated run exceeds page value count: segment={}, run_len={}, remaining_values={}",
                    segment_path, run_len, remaining
                ));
            }
            let repeated_value = reader.read_aligned_bits(bit_width).ok_or_else(|| {
                format!(
                    "decode RLE repeated value failed: segment={}, bit_width={}",
                    segment_path, bit_width
                )
            })?;
            for _ in 0..run_len {
                append_rle_decoded_value(
                    &mut values,
                    output_kind,
                    elem_size,
                    repeated_value,
                    segment_path,
                )?;
            }
            remaining -= run_len;
            continue;
        }

        let run_len = ((indicator >> 1) as usize)
            .checked_mul(8)
            .ok_or_else(|| "RLE literal run length overflow".to_string())?;
        if run_len == 0 {
            return Err(format!(
                "invalid RLE literal run length=0: segment={}",
                segment_path
            ));
        }
        let emit = run_len.min(remaining);
        for _ in 0..emit {
            let value = reader.read_bits(bit_width).ok_or_else(|| {
                format!(
                    "decode RLE literal value failed: segment={}, bit_width={}",
                    segment_path, bit_width
                )
            })?;
            append_rle_decoded_value(&mut values, output_kind, elem_size, value, segment_path)?;
        }
        remaining -= emit;
        let to_skip = run_len - emit;
        if to_skip > 0 {
            let skip_bits = to_skip
                .checked_mul(bit_width)
                .ok_or_else(|| "RLE literal skip size overflow".to_string())?;
            if !reader.skip_bits(skip_bits) {
                return Err(format!(
                    "skip trailing padded RLE literal values failed: segment={}, skip_bits={}",
                    segment_path, skip_bits
                ));
            }
        }
    }

    Ok((num_values, elem_size, values))
}

fn append_rle_decoded_value(
    values: &mut Vec<u8>,
    output_kind: OutputColumnKind,
    elem_size: usize,
    value: u128,
    segment_path: &str,
) -> Result<(), String> {
    match output_kind {
        OutputColumnKind::Boolean => {
            if value > 1 {
                return Err(format!(
                    "invalid BOOLEAN value decoded from RLE page: segment={}, value={}",
                    segment_path, value
                ));
            }
            values.push(value as u8);
        }
        _ => {
            for i in 0..elem_size {
                values.push(((value >> (i * 8)) & 0xFF) as u8);
            }
        }
    }
    Ok(())
}

/// Bit-level reader used by StarRocks RLE page decoder.
struct RleBitReader<'a> {
    data: &'a [u8],
    bit_pos: usize,
}

impl<'a> RleBitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, bit_pos: 0 }
    }

    fn read_vlq_u32(&mut self) -> Option<u32> {
        let mut out = 0_u32;
        for i in 0..MAX_VLQ_U32_BYTES {
            let b = self.read_aligned_u8()?;
            out |= ((b & 0x7F) as u32) << (7 * i);
            if b & 0x80 == 0 {
                return Some(out);
            }
        }
        None
    }

    fn read_aligned_bits(&mut self, bit_width: usize) -> Option<u128> {
        self.align_to_byte();
        self.read_bits(bit_width)
    }

    fn read_bits(&mut self, bit_width: usize) -> Option<u128> {
        if bit_width > 128 {
            return None;
        }
        let end_bit = self.bit_pos.checked_add(bit_width)?;
        if end_bit > self.data.len().checked_mul(8)? {
            return None;
        }

        let mut out = 0_u128;
        for i in 0..bit_width {
            let absolute_bit = self.bit_pos + i;
            let byte = *self.data.get(absolute_bit / 8)?;
            let bit = (byte >> (absolute_bit % 8)) & 1;
            out |= (bit as u128) << i;
        }
        self.bit_pos = end_bit;
        Some(out)
    }

    fn skip_bits(&mut self, bit_count: usize) -> bool {
        let Some(end_bit) = self.bit_pos.checked_add(bit_count) else {
            return false;
        };
        let Some(total_bits) = self.data.len().checked_mul(8) else {
            return false;
        };
        if end_bit > total_bits {
            return false;
        }
        self.bit_pos = end_bit;
        true
    }

    fn read_aligned_u8(&mut self) -> Option<u8> {
        self.align_to_byte();
        let byte_index = self.bit_pos / 8;
        let byte = *self.data.get(byte_index)?;
        self.bit_pos = self.bit_pos.checked_add(8)?;
        Some(byte)
    }

    fn align_to_byte(&mut self) {
        let rem = self.bit_pos % 8;
        if rem != 0 {
            self.bit_pos += 8 - rem;
        }
    }
}
