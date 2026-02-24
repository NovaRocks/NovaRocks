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
//! Variable-length PLAIN payload decoder.
//!
//! StarRocks binary plain page layout:
//! - value bytes region
//! - offsets trailer (`num_values` little-endian u32 offsets)
//! - terminal `num_values` little-endian u32
//!
//! Current limitations:
//! - Offsets must be monotonic and inside value region.
//! - The decoder materializes owned byte vectors for each value.

/// Decode variable-length plain values as a vector of owned byte slices.
pub(crate) fn decode_binary_plain_values(
    segment_path: &str,
    body: &[u8],
    expected_num_values: Option<usize>,
) -> Result<Vec<Vec<u8>>, String> {
    if body.len() < 4 {
        return Err(format!(
            "invalid binary plain page body (too small): segment={}, body_size={}",
            segment_path,
            body.len()
        ));
    }
    let num_values = u32::from_le_bytes(
        body[body.len() - 4..]
            .try_into()
            .map_err(|_| "decode binary plain num_values failed".to_string())?,
    ) as usize;
    if let Some(expected) = expected_num_values {
        if num_values != expected {
            return Err(format!(
                "binary plain page num_values mismatch: segment={}, expected_num_values={}, actual_num_values={}",
                segment_path, expected, num_values
            ));
        }
    }
    let offsets_bytes = num_values
        .checked_mul(4)
        .ok_or_else(|| "binary plain offsets bytes overflow".to_string())?;
    let trailer_bytes = offsets_bytes
        .checked_add(4)
        .ok_or_else(|| "binary plain trailer bytes overflow".to_string())?;
    if trailer_bytes > body.len() {
        return Err(format!(
            "invalid binary plain trailer range: segment={}, body_size={}, trailer_bytes={}",
            segment_path,
            body.len(),
            trailer_bytes
        ));
    }
    let offsets_pos = body.len() - trailer_bytes;
    let mut values = Vec::with_capacity(num_values);
    let mut prev_offset = 0_usize;
    for idx in 0..num_values {
        let off_begin = offsets_pos + idx * 4;
        let off = u32::from_le_bytes(
            body[off_begin..off_begin + 4]
                .try_into()
                .map_err(|_| "decode binary plain offset failed".to_string())?,
        ) as usize;
        if off > offsets_pos {
            return Err(format!(
                "binary plain offset out of range: segment={}, index={}, offset={}, max_offset={}",
                segment_path, idx, off, offsets_pos
            ));
        }
        if off < prev_offset {
            return Err(format!(
                "binary plain offsets are not sorted: segment={}, index={}, offset={}, previous_offset={}",
                segment_path, idx, off, prev_offset
            ));
        }
        prev_offset = off;
    }
    for idx in 0..num_values {
        let start_off = u32::from_le_bytes(
            body[offsets_pos + idx * 4..offsets_pos + (idx + 1) * 4]
                .try_into()
                .map_err(|_| "decode binary plain start offset failed".to_string())?,
        ) as usize;
        let end_off = if idx + 1 < num_values {
            u32::from_le_bytes(
                body[offsets_pos + (idx + 1) * 4..offsets_pos + (idx + 2) * 4]
                    .try_into()
                    .map_err(|_| "decode binary plain end offset failed".to_string())?,
            ) as usize
        } else {
            offsets_pos
        };
        if end_off < start_off || end_off > offsets_pos {
            return Err(format!(
                "invalid binary plain slice range: segment={}, index={}, start_offset={}, end_offset={}, max_offset={}",
                segment_path, idx, start_off, end_off, offsets_pos
            ));
        }
        values.push(body[start_off..end_off].to_vec());
    }
    Ok(values)
}
