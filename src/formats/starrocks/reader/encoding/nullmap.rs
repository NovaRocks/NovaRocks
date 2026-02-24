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
//! Nullable data-page nullmap decoders (format v2).
//!
//! Current limitations:
//! - Supports only v2 nullmap encodings (`BITSHUFFLE_NULL`, `LZ4_NULL`).
//! - The output is a byte-per-row flag vector where `0=not-null, 1=null`.

use super::super::constants::{NULL_ENCODING_BITSHUFFLE, NULL_ENCODING_LZ4};
use super::{align_up_8, bitshuffle_lz4_decompress};

pub(crate) fn decode_v2_null_flags(
    segment_path: &str,
    nullmap_body: &[u8],
    num_values: usize,
    null_encoding: i32,
) -> Result<Vec<u8>, String> {
    match null_encoding {
        NULL_ENCODING_BITSHUFFLE => {
            let padded_num_values = align_up_8(num_values);
            let decoded =
                bitshuffle_lz4_decompress(nullmap_body, padded_num_values, 1, segment_path)?;
            if decoded.len() < num_values {
                return Err(format!(
                    "decoded BITSHUFFLE_NULL payload too small: segment={}, required_bytes={}, actual_bytes={}",
                    segment_path,
                    num_values,
                    decoded.len()
                ));
            }
            Ok(decoded[..num_values].to_vec())
        }
        NULL_ENCODING_LZ4 => {
            let mut decoded = vec![0_u8; num_values];
            let decompressed = lz4_flex::block::decompress_into(nullmap_body, &mut decoded)
                .map_err(|e| {
                    format!(
                        "decompress LZ4_NULL payload failed: segment={}, nullmap_size={}, expected_bytes={}, error={}",
                        segment_path,
                        nullmap_body.len(),
                        num_values,
                        e
                    )
                })?;
            if decompressed != num_values {
                return Err(format!(
                    "LZ4_NULL payload size mismatch: segment={}, expected_bytes={}, actual_bytes={}",
                    segment_path, num_values, decompressed
                ));
            }
            Ok(decoded)
        }
        other => Err(format!(
            "unsupported null encoding for rust native reader: segment={}, null_encoding={}, supported=[{},{}]",
            segment_path, other, NULL_ENCODING_BITSHUFFLE, NULL_ENCODING_LZ4
        )),
    }
}
