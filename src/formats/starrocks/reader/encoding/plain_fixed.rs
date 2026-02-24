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
//! Fixed-width PLAIN payload decoder.
//!
//! StarRocks fixed-width plain page layout:
//! - first 4 bytes: `num_values` (u32 little-endian)
//! - remaining bytes: contiguous fixed-size values
//!
//! Current limitations:
//! - The decoder expects exact body size match and rejects trailing bytes.

/// Decode fixed-width plain values and return `(num_values, value_bytes)`.
pub(crate) fn decode_fixed_plain_values(
    segment_path: &str,
    body: &[u8],
    elem_size: usize,
) -> Result<(usize, Vec<u8>), String> {
    if body.len() < 4 {
        return Err(format!(
            "invalid fixed plain page body (too small): segment={}, body_size={}",
            segment_path,
            body.len()
        ));
    }
    let num_values = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| "decode fixed plain num_values failed".to_string())?,
    ) as usize;
    if num_values == 0 {
        return Err(format!(
            "invalid fixed plain page num_values=0: segment={}",
            segment_path
        ));
    }
    let value_bytes = num_values
        .checked_mul(elem_size)
        .ok_or_else(|| "fixed plain page value byte size overflow".to_string())?;
    let expected_body_size = 4_usize
        .checked_add(value_bytes)
        .ok_or_else(|| "fixed plain page body size overflow".to_string())?;
    if body.len() != expected_body_size {
        return Err(format!(
            "fixed plain page body size mismatch: segment={}, expected_body_size={}, actual_body_size={}, num_values={}, elem_size={}",
            segment_path,
            expected_body_size,
            body.len(),
            num_values,
            elem_size
        ));
    }
    Ok((num_values, body[4..].to_vec()))
}
