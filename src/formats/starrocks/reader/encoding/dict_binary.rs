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
//! Variable-length DICT payload decoder.
//!
//! Dict page data area starts with a mode field:
//! - `PLAIN`: payload falls back to binary plain layout.
//! - `DICT`: payload stores dictionary codes in bitshuffle layout.
//!
//! Current limitations:
//! - Dictionary codes are expected to be signed 32-bit integers.
//! - Only variable-length dictionary values are supported.

use super::super::constants::{ENCODING_DICT, ENCODING_PLAIN};
use super::{decode_binary_plain_values, decode_bitshuffle_page_body};

pub(crate) fn decode_binary_dict_values(
    segment_path: &str,
    value_body: &[u8],
    num_values: usize,
    dict_values: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>, String> {
    if value_body.len() < 4 {
        return Err(format!(
            "invalid binary dict page body (too small): segment={}, body_size={}",
            segment_path,
            value_body.len()
        ));
    }
    let mode = i32::from_le_bytes(
        value_body[0..4]
            .try_into()
            .map_err(|_| "decode binary dict page mode failed".to_string())?,
    );
    let payload = &value_body[4..];
    match mode {
        ENCODING_PLAIN => decode_binary_plain_values(segment_path, payload, Some(num_values)),
        ENCODING_DICT => {
            let (header_num_values, elem_size, code_bytes) =
                decode_bitshuffle_page_body(segment_path, payload)?;
            if header_num_values != num_values {
                return Err(format!(
                    "binary dict page num_values mismatch: segment={}, expected_num_values={}, actual_num_values={}",
                    segment_path, num_values, header_num_values
                ));
            }
            if elem_size != 4 {
                return Err(format!(
                    "unsupported binary dict code elem_size: segment={}, elem_size={}, expected=4",
                    segment_path, elem_size
                ));
            }
            if code_bytes.len() != num_values * 4 {
                return Err(format!(
                    "binary dict code bytes size mismatch: segment={}, expected_bytes={}, actual_bytes={}",
                    segment_path,
                    num_values * 4,
                    code_bytes.len()
                ));
            }
            let mut out = Vec::with_capacity(num_values);
            for (idx, chunk) in code_bytes.chunks_exact(4).enumerate() {
                let code = i32::from_le_bytes(
                    chunk
                        .try_into()
                        .map_err(|_| "decode binary dict code failed".to_string())?,
                );
                if code < 0 || (code as usize) >= dict_values.len() {
                    return Err(format!(
                        "binary dict code out of dictionary range: segment={}, row_index={}, code={}, dict_size={}",
                        segment_path,
                        idx,
                        code,
                        dict_values.len()
                    ));
                }
                out.push(dict_values[code as usize].clone());
            }
            Ok(out)
        }
        other => Err(format!(
            "unsupported binary dict data page mode for rust native reader: segment={}, mode={}, supported=[{},{}]",
            segment_path, other, ENCODING_PLAIN, ENCODING_DICT
        )),
    }
}
