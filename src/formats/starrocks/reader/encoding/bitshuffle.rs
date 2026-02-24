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
//! Bitshuffle + LZ4 payload decoders.
//!
//! This module mirrors StarRocks bitshuffle decoding for data pages and nullmaps.
//!
//! Current limitations:
//! - Block sizes must be multiples of 8 elements.
//! - Decoder supports the bitshuffle+LZ4 layout used by native scalar pages.

use super::super::constants::*;

pub(crate) fn decode_bitshuffle_page_body(
    segment_path: &str,
    body: &[u8],
) -> Result<(usize, usize, Vec<u8>), String> {
    if body.len() < BITSHUFFLE_PAGE_HEADER_SIZE {
        return Err(format!(
            "invalid bitshuffle page body (too small): segment={}, body_size={}",
            segment_path,
            body.len()
        ));
    }

    let num_values = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| "decode bitshuffle num_values failed".to_string())?,
    ) as usize;
    let compressed_size = u32::from_le_bytes(
        body[4..8]
            .try_into()
            .map_err(|_| "decode bitshuffle compressed_size failed".to_string())?,
    ) as usize;
    let padded_num_values = u32::from_le_bytes(
        body[8..12]
            .try_into()
            .map_err(|_| "decode bitshuffle padded_num_values failed".to_string())?,
    ) as usize;
    let elem_size = u32::from_le_bytes(
        body[12..16]
            .try_into()
            .map_err(|_| "decode bitshuffle elem_size failed".to_string())?,
    ) as usize;

    if num_values == 0 {
        return Err(format!(
            "invalid bitshuffle page header num_values=0: segment={}",
            segment_path
        ));
    }
    if elem_size == 0 {
        return Err(format!(
            "invalid bitshuffle page header elem_size=0: segment={}",
            segment_path
        ));
    }
    if compressed_size < BITSHUFFLE_PAGE_HEADER_SIZE || compressed_size > body.len() {
        return Err(format!(
            "invalid bitshuffle compressed_size: segment={}, compressed_size={}, body_size={}",
            segment_path,
            compressed_size,
            body.len()
        ));
    }
    if padded_num_values != align_up_8(num_values) {
        return Err(format!(
            "invalid bitshuffle padded_num_values: segment={}, num_values={}, padded_num_values={}",
            segment_path, num_values, padded_num_values
        ));
    }

    let compressed_payload = &body[BITSHUFFLE_PAGE_HEADER_SIZE..compressed_size];
    let decoded_payload = bitshuffle_lz4_decompress(
        compressed_payload,
        padded_num_values,
        elem_size,
        segment_path,
    )?;
    let value_bytes = num_values
        .checked_mul(elem_size)
        .ok_or_else(|| "bitshuffle value byte size overflow".to_string())?;
    if decoded_payload.len() < value_bytes {
        return Err(format!(
            "decoded bitshuffle payload too small: segment={}, required_bytes={}, actual_bytes={}",
            segment_path,
            value_bytes,
            decoded_payload.len()
        ));
    }
    Ok((
        num_values,
        elem_size,
        decoded_payload[..value_bytes].to_vec(),
    ))
}

pub(crate) fn bitshuffle_lz4_decompress(
    payload: &[u8],
    num_elements: usize,
    elem_size: usize,
    segment_path: &str,
) -> Result<Vec<u8>, String> {
    if num_elements % 8 != 0 {
        return Err(format!(
            "bitshuffle num_elements must be multiple of 8: segment={}, num_elements={}",
            segment_path, num_elements
        ));
    }
    let output_bytes = num_elements
        .checked_mul(elem_size)
        .ok_or_else(|| "bitshuffle output size overflow".to_string())?;
    let mut output = vec![0_u8; output_bytes];
    let block_size = bitshuffle_default_block_size(elem_size);

    let mut in_offset = 0_usize;
    let mut out_offset = 0_usize;

    let full_blocks = num_elements / block_size;
    for _ in 0..full_blocks {
        decode_bitshuffle_lz4_block(
            payload,
            &mut in_offset,
            &mut output[out_offset..out_offset + block_size * elem_size],
            block_size,
            elem_size,
            segment_path,
        )?;
        out_offset += block_size * elem_size;
    }

    let mut last_block_size = num_elements % block_size;
    last_block_size -= last_block_size % 8;
    if last_block_size > 0 {
        decode_bitshuffle_lz4_block(
            payload,
            &mut in_offset,
            &mut output[out_offset..out_offset + last_block_size * elem_size],
            last_block_size,
            elem_size,
            segment_path,
        )?;
        out_offset += last_block_size * elem_size;
    }

    let leftover_bytes = (num_elements % 8) * elem_size;
    if leftover_bytes > 0 {
        let end = in_offset
            .checked_add(leftover_bytes)
            .ok_or_else(|| "bitshuffle leftover range overflow".to_string())?;
        if end > payload.len() {
            return Err(format!(
                "bitshuffle leftover bytes out of range: segment={}, in_offset={}, leftover_bytes={}, payload_len={}",
                segment_path,
                in_offset,
                leftover_bytes,
                payload.len()
            ));
        }
        output[out_offset..out_offset + leftover_bytes].copy_from_slice(&payload[in_offset..end]);
        in_offset = end;
        out_offset += leftover_bytes;
    }

    if in_offset != payload.len() {
        return Err(format!(
            "bitshuffle payload not fully consumed: segment={}, consumed={}, payload_len={}",
            segment_path,
            in_offset,
            payload.len()
        ));
    }
    if out_offset != output.len() {
        return Err(format!(
            "bitshuffle output size mismatch: segment={}, produced={}, expected={}",
            segment_path,
            out_offset,
            output.len()
        ));
    }
    Ok(output)
}

fn decode_bitshuffle_lz4_block(
    payload: &[u8],
    in_offset: &mut usize,
    output_block: &mut [u8],
    block_elements: usize,
    elem_size: usize,
    segment_path: &str,
) -> Result<(), String> {
    let header_end = in_offset
        .checked_add(4)
        .ok_or_else(|| "bitshuffle block header range overflow".to_string())?;
    if header_end > payload.len() {
        return Err(format!(
            "bitshuffle block header out of range: segment={}, in_offset={}, payload_len={}",
            segment_path,
            *in_offset,
            payload.len()
        ));
    }
    let compressed_len = u32::from_be_bytes(
        payload[*in_offset..header_end]
            .try_into()
            .map_err(|_| "decode bitshuffle block compressed length failed".to_string())?,
    ) as usize;
    *in_offset = header_end;

    let block_end = in_offset
        .checked_add(compressed_len)
        .ok_or_else(|| "bitshuffle block range overflow".to_string())?;
    if block_end > payload.len() {
        return Err(format!(
            "bitshuffle block out of range: segment={}, in_offset={}, compressed_len={}, payload_len={}",
            segment_path,
            *in_offset,
            compressed_len,
            payload.len()
        ));
    }
    let compressed_block = &payload[*in_offset..block_end];
    *in_offset = block_end;

    let decoded_len = block_elements
        .checked_mul(elem_size)
        .ok_or_else(|| "bitshuffle decoded block size overflow".to_string())?;
    if output_block.len() != decoded_len {
        return Err(format!(
            "bitshuffle output block size mismatch: segment={}, output_block_len={}, expected_len={}",
            segment_path,
            output_block.len(),
            decoded_len
        ));
    }

    let mut bitshuffled_block = vec![0_u8; decoded_len];
    let decompressed = lz4_flex::block::decompress_into(compressed_block, &mut bitshuffled_block)
        .map_err(|e| {
            format!(
                "decompress bitshuffle LZ4 block failed: segment={}, compressed_len={}, expected_len={}, error={}",
                segment_path, compressed_len, decoded_len, e
            )
        })?;
    if decompressed != decoded_len {
        return Err(format!(
            "bitshuffle LZ4 block size mismatch: segment={}, expected_len={}, actual_len={}",
            segment_path, decoded_len, decompressed
        ));
    }

    bitunshuffle_scal(&bitshuffled_block, output_block, block_elements, elem_size)
}

fn bitunshuffle_scal(
    input: &[u8],
    output: &mut [u8],
    size: usize,
    elem_size: usize,
) -> Result<(), String> {
    if size % 8 != 0 {
        return Err(format!(
            "bitunshuffle size must be multiple of 8: size={}",
            size
        ));
    }
    let total_bytes = size
        .checked_mul(elem_size)
        .ok_or_else(|| "bitunshuffle total bytes overflow".to_string())?;
    if input.len() != total_bytes || output.len() != total_bytes {
        return Err(format!(
            "bitunshuffle buffer size mismatch: input_len={}, output_len={}, expected_len={}",
            input.len(),
            output.len(),
            total_bytes
        ));
    }

    let mut tmp = vec![0_u8; total_bytes];
    trans_byte_bitrow_scal(input, &mut tmp, size, elem_size)?;
    shuffle_bit_eightelem_scal(&tmp, output, size, elem_size)?;
    Ok(())
}

fn trans_byte_bitrow_scal(
    input: &[u8],
    output: &mut [u8],
    size: usize,
    elem_size: usize,
) -> Result<(), String> {
    if size % 8 != 0 {
        return Err(format!(
            "trans_byte_bitrow size must be multiple of 8: size={}",
            size
        ));
    }
    let nbyte_row = size / 8;
    for jj in 0..elem_size {
        for ii in 0..nbyte_row {
            for kk in 0..8 {
                let out_index = ii * 8 * elem_size + jj * 8 + kk;
                let in_index = (jj * 8 + kk) * nbyte_row + ii;
                output[out_index] = input[in_index];
            }
        }
    }
    Ok(())
}

fn shuffle_bit_eightelem_scal(
    input: &[u8],
    output: &mut [u8],
    size: usize,
    elem_size: usize,
) -> Result<(), String> {
    if size % 8 != 0 {
        return Err(format!(
            "shuffle_bit_eightelem size must be multiple of 8: size={}",
            size
        ));
    }
    let nbyte = size
        .checked_mul(elem_size)
        .ok_or_else(|| "shuffle_bit_eightelem total bytes overflow".to_string())?;
    let mut jj = 0_usize;
    while jj < 8 * elem_size {
        let mut ii = 0_usize;
        while ii + (8 * elem_size - 1) < nbyte {
            let start = ii + jj;
            let end = start + 8;
            let mut x = u64::from_le_bytes(
                input[start..end]
                    .try_into()
                    .map_err(|_| "decode 8x8 bit matrix failed".to_string())?,
            );
            x = transpose_bit_8x8(x);
            for kk in 0..8 {
                let out_index = ii + jj / 8 + kk * elem_size;
                output[out_index] = (x & 0xFF) as u8;
                x >>= 8;
            }
            ii += 8 * elem_size;
        }
        jj += 8;
    }
    Ok(())
}

fn transpose_bit_8x8(mut x: u64) -> u64 {
    let mut t = (x ^ (x >> 7)) & 0x00AA00AA00AA00AA_u64;
    x ^= t ^ (t << 7);
    t = (x ^ (x >> 14)) & 0x0000CCCC0000CCCC_u64;
    x ^= t ^ (t << 14);
    t = (x ^ (x >> 28)) & 0x00000000F0F0F0F0_u64;
    x ^= t ^ (t << 28);
    x
}

fn bitshuffle_default_block_size(elem_size: usize) -> usize {
    let block_size = BITSHUFFLE_TARGET_BLOCK_SIZE_BYTES / elem_size;
    let block_size = (block_size / 8) * 8;
    block_size.max(BITSHUFFLE_MIN_BLOCK_SIZE)
}

pub(crate) fn align_up_8(v: usize) -> usize {
    (v + 7) & !7
}
