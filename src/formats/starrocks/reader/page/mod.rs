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
//! Native StarRocks page decoding pipeline.
//!
//! Module split:
//! - `footer`: protobuf-lite page footer structs.
//! - `envelope`: checksum/footer validation and decompression.
//! - `data_page`: page-level decode control flow.
//! - `index_page`: index-page entry decode for ordinal/page indexes.
//!
//! Current limitations:
//! - Nullable page decoding supports format-v2 nullmaps only.
//! - Page decompression supports `NO_COMPRESSION` and `LZ4_FRAME` only.

mod data_page;
mod envelope;
mod footer;
mod index_page;

pub(super) use data_page::{
    DecodedDataPageValues, DecodedPageValuePayload, decode_data_page_values, fixed_value_size_bytes,
};
pub(super) use envelope::{decode_page_envelope, slice_page_bytes};
pub(super) use index_page::{DecodedIndexPageEntry, IndexPageNodeType, decode_index_page};

#[cfg(test)]
mod tests {
    use super::super::column_state::OutputColumnKind;
    use super::super::constants::NULL_ENCODING_LZ4;
    use super::super::encoding::{
        decode_fixed_plain_values, decode_rle_page_body, decode_v2_null_flags,
    };

    #[test]
    fn decode_lz4_null_flags_v2() {
        let null_flags = vec![0_u8, 1, 0, 1, 1, 0, 0, 1];
        let compressed = lz4_flex::block::compress(&null_flags);
        let decoded = decode_v2_null_flags(
            "segment.dat",
            &compressed,
            null_flags.len(),
            NULL_ENCODING_LZ4,
        )
        .expect("decode lz4 null flags");
        assert_eq!(decoded, null_flags);
    }

    #[test]
    fn decode_rle_page_body_boolean_repeated_run() {
        // num_values=5, repeated run length=5, repeated value=true
        let body = vec![5, 0, 0, 0, 0x0A, 0x01];
        let (num_values, elem_size, decoded) =
            decode_rle_page_body("segment.dat", &body, OutputColumnKind::Boolean, 1)
                .expect("decode boolean rle page");
        assert_eq!(num_values, 5);
        assert_eq!(elem_size, 1);
        assert_eq!(decoded, vec![1, 1, 1, 1, 1]);
    }

    #[test]
    fn decode_rle_page_body_int32_repeated_run() {
        // num_values=3, repeated run length=3, repeated value=42 (little-endian int32)
        let body = vec![3, 0, 0, 0, 0x06, 42, 0, 0, 0];
        let (num_values, elem_size, decoded) =
            decode_rle_page_body("segment.dat", &body, OutputColumnKind::Int32, 4)
                .expect("decode int32 rle page");
        assert_eq!(num_values, 3);
        assert_eq!(elem_size, 4);
        assert_eq!(decoded, vec![42, 0, 0, 0, 42, 0, 0, 0, 42, 0, 0, 0]);
    }

    #[test]
    fn decode_fixed_plain_values_parses_header_and_payload() {
        let mut body = Vec::new();
        body.extend_from_slice(&(2_u32).to_le_bytes());
        body.extend_from_slice(&10_i32.to_le_bytes());
        body.extend_from_slice(&(-20_i32).to_le_bytes());
        let (num_values, payload) =
            decode_fixed_plain_values("segment.dat", &body, 4).expect("decode fixed plain values");
        assert_eq!(num_values, 2);
        assert_eq!(payload, vec![10, 0, 0, 0, 236, 255, 255, 255]);
    }
}
