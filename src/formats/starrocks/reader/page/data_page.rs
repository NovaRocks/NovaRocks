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
//! Data-page decode orchestration.
//!
//! This module decodes one StarRocks data page into either:
//! - fixed-width little-endian value bytes, or
//! - variable-length value vectors.
//!
//! Current limitations:
//! - Nullable pages require data page format version `2`.
//! - Nullmap decoding supports `BITSHUFFLE_NULL` and `LZ4_NULL` only.
//! - Fixed-width DICT encoding is not supported.

use super::super::column_state::OutputColumnKind;
use super::super::constants::{
    DATA_PAGE_FORMAT_V2, ENCODING_BIT_SHUFFLE, ENCODING_DICT, ENCODING_PLAIN, ENCODING_RLE,
    NULL_ENCODING_BITSHUFFLE, PAGE_TYPE_DATA,
};
use super::super::encoding::{
    decode_binary_dict_values, decode_binary_plain_values, decode_bitshuffle_page_body,
    decode_fixed_plain_values, decode_rle_page_body, decode_v2_null_flags,
};
use super::super::types::decimal::decimal_elem_size_from_logical_type;
use super::envelope::{DecodedPageEnvelope, decode_page_envelope};

/// Decoded value payload extracted from a single data page.
pub(crate) enum DecodedPageValuePayload {
    Fixed {
        value_bytes: Vec<u8>,
        elem_size: usize,
    },
    Variable {
        values: Vec<Vec<u8>>,
    },
}

/// Fully decoded data page result for one projected column.
pub(crate) struct DecodedDataPageValues {
    pub(crate) payload: DecodedPageValuePayload,
    pub(crate) num_values: usize,
    pub(crate) null_flags: Option<Vec<u8>>,
}

pub(crate) fn fixed_value_size_bytes(
    output_kind: OutputColumnKind,
    logical_type: i32,
) -> Result<usize, String> {
    match output_kind {
        OutputColumnKind::Decimal128 | OutputColumnKind::Decimal256 => {
            decimal_elem_size_from_logical_type(logical_type)
        }
        _ => Ok(output_kind.value_size_bytes()),
    }
}

pub(crate) fn decode_data_page_values(
    segment_path: &str,
    page_bytes: &[u8],
    column_compression: i32,
    column_encoding: i32,
    output_kind: OutputColumnKind,
    fixed_elem_size: Option<usize>,
    column_is_nullable: Option<bool>,
    dict_values: Option<&[Vec<u8>]>,
) -> Result<DecodedDataPageValues, String> {
    // Parse and validate page checksum/footer, then materialize page body.
    let DecodedPageEnvelope { footer, body } =
        decode_page_envelope(segment_path, page_bytes, column_compression)?;
    if footer.r#type != Some(PAGE_TYPE_DATA) {
        return Err(format!(
            "unsupported non-data page type for rust native reader: segment={}, page_type={:?}",
            segment_path, footer.r#type
        ));
    }
    let data_footer = footer.data_page_footer.as_ref().ok_or_else(|| {
        format!(
            "missing data_page_footer in page footer: segment={}",
            segment_path
        )
    })?;
    let num_values = usize::try_from(data_footer.num_values.unwrap_or(0)).map_err(|_| {
        format!(
            "invalid num_values in data page footer: segment={}, num_values={:?}",
            segment_path, data_footer.num_values
        )
    })?;
    if num_values == 0 {
        return Err(format!(
            "invalid data page num_values for rust native reader: segment={}, num_values=0",
            segment_path
        ));
    }
    let null_size = usize::try_from(data_footer.nullmap_size.unwrap_or(0)).map_err(|_| {
        format!(
            "invalid nullmap_size in data page footer: segment={}, nullmap_size={:?}",
            segment_path, data_footer.nullmap_size
        )
    })?;
    if null_size > body.len() {
        return Err(format!(
            "invalid data page nullmap_size out of range: segment={}, nullmap_size={}, body_size={}",
            segment_path,
            null_size,
            body.len()
        ));
    }
    if null_size > 0 && column_is_nullable == Some(false) {
        return Err(format!(
            "unexpected nullmap in non-nullable column page: segment={}, nullmap_size={}",
            segment_path, null_size
        ));
    }

    let value_body_end = body
        .len()
        .checked_sub(null_size)
        .ok_or_else(|| "data page body split overflow".to_string())?;
    let value_body = &body[..value_body_end];
    let nullmap_body = &body[value_body_end..];

    // Decode page value area according to column encoding.
    let payload = match column_encoding {
        ENCODING_BIT_SHUFFLE => {
            let (header_num_values, elem_size, payload) =
                decode_bitshuffle_page_body(segment_path, value_body)?;
            if let Some(expected_elem_size) = fixed_elem_size {
                if elem_size != expected_elem_size {
                    return Err(format!(
                        "data page elem_size mismatch in bitshuffle payload: segment={}, expected_elem_size={}, actual_elem_size={}",
                        segment_path, expected_elem_size, elem_size
                    ));
                }
            }
            if header_num_values != num_values {
                return Err(format!(
                    "data page num_values mismatch between footer and bitshuffle header: segment={}, footer_num_values={}, header_num_values={}",
                    segment_path, num_values, header_num_values
                ));
            }
            DecodedPageValuePayload::Fixed {
                value_bytes: payload,
                elem_size,
            }
        }
        ENCODING_RLE => {
            let elem_size = fixed_elem_size.ok_or_else(|| {
                format!(
                    "missing fixed elem_size for RLE decoding in rust native reader: segment={}",
                    segment_path
                )
            })?;
            let (header_num_values, elem_size, payload) =
                decode_rle_page_body(segment_path, value_body, output_kind, elem_size)?;
            if header_num_values != num_values {
                return Err(format!(
                    "data page num_values mismatch between footer and rle header: segment={}, footer_num_values={}, header_num_values={}",
                    segment_path, num_values, header_num_values
                ));
            }
            DecodedPageValuePayload::Fixed {
                value_bytes: payload,
                elem_size,
            }
        }
        ENCODING_PLAIN => {
            if output_kind.is_variable_len() {
                DecodedPageValuePayload::Variable {
                    values: decode_binary_plain_values(segment_path, value_body, Some(num_values))?,
                }
            } else {
                let elem_size = fixed_elem_size.ok_or_else(|| {
                    format!(
                        "missing fixed elem_size for PLAIN decoding in rust native reader: segment={}",
                        segment_path
                    )
                })?;
                let (header_num_values, payload) =
                    decode_fixed_plain_values(segment_path, value_body, elem_size)?;
                if header_num_values != num_values {
                    return Err(format!(
                        "data page num_values mismatch between footer and plain header: segment={}, footer_num_values={}, header_num_values={}",
                        segment_path, num_values, header_num_values
                    ));
                }
                DecodedPageValuePayload::Fixed {
                    value_bytes: payload,
                    elem_size,
                }
            }
        }
        ENCODING_DICT => {
            if !output_kind.is_variable_len() {
                return Err(format!(
                    "unsupported DICT encoding for fixed-width output column: segment={}, output_kind={}",
                    segment_path,
                    output_kind.arrow_type_name()
                ));
            }
            let dict_values = dict_values.ok_or_else(|| {
                format!(
                    "missing dictionary values for DICT-encoded variable column data page: segment={}",
                    segment_path
                )
            })?;
            DecodedPageValuePayload::Variable {
                values: decode_binary_dict_values(
                    segment_path,
                    value_body,
                    num_values,
                    dict_values,
                )?,
            }
        }
        other => {
            return Err(format!(
                "unsupported column encoding in data page decoder: segment={}, encoding={}",
                segment_path, other
            ));
        }
    };

    // Decode nullable bitmap section when present.
    let null_flags = if null_size > 0 {
        let format_version = data_footer.format_version.unwrap_or(1);
        if format_version != DATA_PAGE_FORMAT_V2 {
            return Err(format!(
                "unsupported nullable data page format_version for rust native reader: segment={}, format_version={}, supported=[{}]",
                segment_path, format_version, DATA_PAGE_FORMAT_V2
            ));
        }
        let null_encoding = data_footer
            .null_encoding
            .unwrap_or(NULL_ENCODING_BITSHUFFLE);
        Some(decode_v2_null_flags(
            segment_path,
            nullmap_body,
            num_values,
            null_encoding,
        )?)
    } else {
        None
    };

    Ok(DecodedDataPageValues {
        payload,
        num_values,
        null_flags,
    })
}
