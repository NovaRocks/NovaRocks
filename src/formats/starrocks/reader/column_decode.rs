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
//! Column-level decode entrypoints and metadata validation.
//!
//! This module bridges segment footer metadata and page-level decoders:
//! - Validates logical type, encoding, compression, and ordinal-index layout.
//! - Loads dictionary pages for DICT-encoded variable-length columns.
//! - Delegates to page decoder and returns typed page payloads.
//!
//! Current limitations:
//! - Column compression must be `LZ4_FRAME`.
//! - Fixed-width columns support encodings: `BIT_SHUFFLE`, `RLE`, `PLAIN`.
//! - Variable-length columns support encodings: `PLAIN`, `DICT`.
//! - `decode_column_values_bytes` only supports `root_is_data_page == true`
//!   (multi-page decode should use `decode_all_data_page_refs` + `decode_one_data_page`).
//! - Dictionary pages are only used for variable-length columns.

use crate::formats::starrocks::segment::{StarRocksPagePointer, StarRocksSegmentColumnMeta};

use super::column_state::OutputColumnKind;
use super::constants::{
    COMPRESSION_LZ4_FRAME, ENCODING_BIT_SHUFFLE, ENCODING_DICT, ENCODING_PLAIN, ENCODING_RLE,
    LOGICAL_TYPE_CHAR, LOGICAL_TYPE_JSON, LOGICAL_TYPE_OBJECT, LOGICAL_TYPE_VARCHAR,
    PAGE_TYPE_DICTIONARY,
};
use super::encoding::decode_binary_plain_values;
use super::indexed_column::{DataPageRef, decode_data_page_refs_from_ordinal_index};
use super::page::{
    DecodedDataPageValues, decode_data_page_values, decode_page_envelope, fixed_value_size_bytes,
    slice_page_bytes,
};

/// Validated decode spec reused across multiple page decodes of one column.
pub(super) struct ColumnDecodeSpec {
    pub(super) encoding: i32,
    pub(super) compression: i32,
    pub(super) output_kind: OutputColumnKind,
    pub(super) fixed_elem_size: Option<usize>,
    pub(super) is_nullable: Option<bool>,
    pub(super) dict_values: Option<Vec<Vec<u8>>>,
}

#[inline]
fn is_logical_type_compatible(actual: i32, expected: i32, output_kind: OutputColumnKind) -> bool {
    if actual == expected {
        return true;
    }
    if output_kind.is_variable_len()
        && ((actual == LOGICAL_TYPE_JSON && expected == LOGICAL_TYPE_OBJECT)
            || (actual == LOGICAL_TYPE_OBJECT && expected == LOGICAL_TYPE_JSON))
    {
        return true;
    }
    // Some StarRocks writer paths persist CHAR columns as VARCHAR on disk.
    output_kind.is_variable_len()
        && ((actual == LOGICAL_TYPE_VARCHAR && expected == LOGICAL_TYPE_CHAR)
            || (actual == LOGICAL_TYPE_CHAR && expected == LOGICAL_TYPE_VARCHAR))
}

/// Decode projected column values from one segment.
///
/// The returned payload contains one decoded data page for the projected column.
/// The caller is responsible for appending payload values to cross-segment buffers.
pub(super) fn decode_column_values_bytes(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    expected_logical_type: i32,
    expected_arrow_type: &str,
    output_kind: OutputColumnKind,
    output_name: &str,
) -> Result<DecodedDataPageValues, String> {
    let spec = build_column_decode_spec(
        segment_path,
        segment_bytes,
        column_meta,
        expected_logical_type,
        expected_arrow_type,
        output_kind,
        output_name,
    )?;
    if column_meta.ordinal_index_root_is_data_page != Some(true) {
        return Err(format!(
            "unsupported non-data-page ordinal root for decode_column_values_bytes: segment={}, unique_id={:?}, is_root_data_page={:?}",
            segment_path, column_meta.unique_id, column_meta.ordinal_index_root_is_data_page
        ));
    }
    let page_ptr = column_meta.ordinal_index_root_page.as_ref().ok_or_else(|| {
        format!(
            "missing ordinal index root page pointer in segment column meta: segment={}, unique_id={:?}",
            segment_path, column_meta.unique_id
        )
    })?;
    decode_one_data_page(
        segment_path,
        segment_bytes,
        page_ptr,
        column_meta.unique_id,
        &spec,
    )
}

/// Build a reusable decode spec for one column.
pub(super) fn build_column_decode_spec(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    expected_logical_type: i32,
    expected_arrow_type: &str,
    output_kind: OutputColumnKind,
    output_name: &str,
) -> Result<ColumnDecodeSpec, String> {
    let logical_type = column_meta.logical_type.ok_or_else(|| {
        format!(
            "missing logical_type in segment column meta: segment={}, unique_id={:?}",
            segment_path, column_meta.unique_id
        )
    })?;
    if !is_logical_type_compatible(logical_type, expected_logical_type, output_kind) {
        return Err(format!(
            "segment logical_type mismatch for rust native reader: segment={}, unique_id={:?}, output_column={}, logical_type={}, expected_logical_type={}, expected_arrow_type={}",
            segment_path,
            column_meta.unique_id,
            output_name,
            logical_type,
            expected_logical_type,
            expected_arrow_type
        ));
    }

    let encoding = column_meta.encoding.ok_or_else(|| {
        format!(
            "missing encoding in segment column meta: segment={}, unique_id={:?}",
            segment_path, column_meta.unique_id
        )
    })?;
    let is_supported_encoding = if output_kind.is_variable_len() {
        encoding == ENCODING_PLAIN || encoding == ENCODING_DICT
    } else {
        encoding == ENCODING_BIT_SHUFFLE || encoding == ENCODING_RLE || encoding == ENCODING_PLAIN
    };
    if !is_supported_encoding {
        let supported = if output_kind.is_variable_len() {
            format!("[{},{}]", ENCODING_PLAIN, ENCODING_DICT)
        } else {
            format!(
                "[{},{},{}]",
                ENCODING_BIT_SHUFFLE, ENCODING_RLE, ENCODING_PLAIN
            )
        };
        return Err(format!(
            "unsupported segment column encoding for rust native reader: segment={}, unique_id={:?}, output_column={}, encoding={}, supported={}",
            segment_path, column_meta.unique_id, output_name, encoding, supported
        ));
    }

    let compression = column_meta.compression.ok_or_else(|| {
        format!(
            "missing compression in segment column meta: segment={}, unique_id={:?}",
            segment_path, column_meta.unique_id
        )
    })?;
    if compression != COMPRESSION_LZ4_FRAME {
        return Err(format!(
            "unsupported segment column compression for rust native reader: segment={}, unique_id={:?}, compression={}, expected={}",
            segment_path, column_meta.unique_id, compression, COMPRESSION_LZ4_FRAME
        ));
    }

    let dict_values = if output_kind.is_variable_len() && encoding == ENCODING_DICT {
        let dict_page_ptr = column_meta.dict_page.as_ref().ok_or_else(|| {
            format!(
                "missing dict_page in segment column meta for DICT-encoded variable column: segment={}, unique_id={:?}, output_column={}",
                segment_path, column_meta.unique_id, output_name
            )
        })?;
        let dict_page_bytes = slice_page_bytes(
            segment_path,
            segment_bytes,
            dict_page_ptr,
            "dict page",
            column_meta.unique_id,
        )?;
        Some(
            decode_dictionary_page_values(segment_path, dict_page_bytes, compression, output_name)
                .map_err(|e| {
                    format!(
                        "decode dictionary page failed: segment={}, output_column={}, unique_id={:?}, offset={}, size={}, error={}",
                        segment_path,
                        output_name,
                        column_meta.unique_id,
                        dict_page_ptr.offset,
                        dict_page_ptr.size,
                        e
                    )
                })?,
        )
    } else {
        None
    };
    let fixed_elem_size = if output_kind.is_variable_len() {
        None
    } else {
        Some(fixed_value_size_bytes(output_kind, logical_type)?)
    };
    Ok(ColumnDecodeSpec {
        encoding,
        compression,
        output_kind,
        fixed_elem_size,
        is_nullable: column_meta.is_nullable,
        dict_values,
    })
}

/// Decode one concrete data page pointed by `page_ptr`.
pub(super) fn decode_one_data_page(
    segment_path: &str,
    segment_bytes: &[u8],
    page_ptr: &StarRocksPagePointer,
    column_unique_id: Option<u32>,
    spec: &ColumnDecodeSpec,
) -> Result<DecodedDataPageValues, String> {
    let page_bytes = slice_page_bytes(
        segment_path,
        segment_bytes,
        page_ptr,
        "data page",
        column_unique_id,
    )?;
    decode_data_page_values(
        segment_path,
        page_bytes,
        spec.compression,
        spec.encoding,
        spec.output_kind,
        spec.fixed_elem_size,
        spec.is_nullable,
        spec.dict_values.as_deref(),
    )
    .map_err(|e| {
        format!(
            "decode data page failed: segment={}, unique_id={:?}, page_offset={}, page_size={}, error={}",
            segment_path, column_unique_id, page_ptr.offset, page_ptr.size, e
        )
    })
}

/// Decode and merge all data pages for one column using ordinal index metadata.
///
/// Unlike `decode_column_values_bytes`, this helper supports both:
/// - `root_is_data_page == true` (single page), and
/// - `root_is_data_page == false` (index tree with multiple data pages).
pub(super) fn decode_column_values_by_total_rows(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    expected_logical_type: i32,
    expected_arrow_type: &str,
    output_kind: OutputColumnKind,
    output_name: &str,
    total_num_values: usize,
) -> Result<DecodedDataPageValues, String> {
    if total_num_values == 0 {
        let payload = if output_kind.is_variable_len() {
            super::page::DecodedPageValuePayload::Variable { values: Vec::new() }
        } else {
            let elem_size =
                fixed_value_size_bytes(output_kind, expected_logical_type).map_err(|e| {
                    format!(
                        "resolve fixed elem_size failed for empty decode: segment={}, output_column={}, error={}",
                        segment_path, output_name, e
                    )
                })?;
            super::page::DecodedPageValuePayload::Fixed {
                value_bytes: Vec::new(),
                elem_size,
            }
        };
        return Ok(DecodedDataPageValues {
            payload,
            num_values: 0,
            null_flags: None,
        });
    }

    if column_meta.ordinal_index_root_is_data_page == Some(true) {
        let decoded = decode_column_values_bytes(
            segment_path,
            segment_bytes,
            column_meta,
            expected_logical_type,
            expected_arrow_type,
            output_kind,
            output_name,
        )?;
        if decoded.num_values != total_num_values {
            return Err(format!(
                "decoded row count mismatch for single-page column: segment={}, output_column={}, expected_rows={}, actual_rows={}",
                segment_path, output_name, total_num_values, decoded.num_values
            ));
        }
        if let Some(flags) = decoded.null_flags.as_ref() {
            if flags.len() != total_num_values {
                return Err(format!(
                    "decoded null flag count mismatch for single-page column: segment={}, output_column={}, expected_rows={}, actual_null_flags={}",
                    segment_path,
                    output_name,
                    total_num_values,
                    flags.len()
                ));
            }
        }
        return Ok(decoded);
    }

    let spec = build_column_decode_spec(
        segment_path,
        segment_bytes,
        column_meta,
        expected_logical_type,
        expected_arrow_type,
        output_kind,
        output_name,
    )?;
    let page_refs =
        decode_all_data_page_refs(segment_path, segment_bytes, column_meta, total_num_values)?;
    if page_refs.is_empty() {
        return Err(format!(
            "ordinal index resolved zero data pages for non-empty column: segment={}, unique_id={:?}, output_column={}, total_num_values={}",
            segment_path, column_meta.unique_id, output_name, total_num_values
        ));
    }

    enum PayloadAcc {
        Fixed {
            elem_size: usize,
            value_bytes: Vec<u8>,
        },
        Variable {
            values: Vec<Vec<u8>>,
        },
    }

    let mut payload_acc: Option<PayloadAcc> = None;
    let mut decoded_rows = 0usize;
    let mut null_flags_acc: Vec<u8> = Vec::new();
    let mut has_null_flags = false;

    for page_ref in &page_refs {
        let expected_page_rows = usize::try_from(page_ref.num_values).map_err(|_| {
            format!(
                "page num_values overflow while decoding full column: segment={}, output_column={}, num_values={}",
                segment_path, output_name, page_ref.num_values
            )
        })?;
        let decoded = decode_one_data_page(
            segment_path,
            segment_bytes,
            &page_ref.page_pointer,
            column_meta.unique_id,
            &spec,
        )?;
        if decoded.num_values != expected_page_rows {
            return Err(format!(
                "decoded data page row count mismatch while merging full column: segment={}, output_column={}, expected_rows={}, actual_rows={}",
                segment_path, output_name, expected_page_rows, decoded.num_values
            ));
        }
        decoded_rows = decoded_rows.checked_add(decoded.num_values).ok_or_else(|| {
            format!(
                "decoded row count overflow while merging full column: segment={}, output_column={}, decoded_rows={}, page_rows={}",
                segment_path, output_name, decoded_rows, decoded.num_values
            )
        })?;

        match decoded.payload {
            super::page::DecodedPageValuePayload::Fixed {
                value_bytes,
                elem_size,
            } => match payload_acc.as_mut() {
                None => {
                    payload_acc = Some(PayloadAcc::Fixed {
                        elem_size,
                        value_bytes,
                    });
                }
                Some(PayloadAcc::Fixed {
                    elem_size: acc_elem_size,
                    value_bytes: acc_values,
                }) => {
                    if *acc_elem_size != elem_size {
                        return Err(format!(
                            "inconsistent elem_size while merging fixed pages: segment={}, output_column={}, acc_elem_size={}, page_elem_size={}",
                            segment_path, output_name, acc_elem_size, elem_size
                        ));
                    }
                    acc_values.extend(value_bytes);
                }
                Some(PayloadAcc::Variable { .. }) => {
                    return Err(format!(
                        "payload kind mismatch while merging column pages: segment={}, output_column={}, expected=Variable, actual=Fixed",
                        segment_path, output_name
                    ));
                }
            },
            super::page::DecodedPageValuePayload::Variable { values } => match payload_acc.as_mut()
            {
                None => {
                    payload_acc = Some(PayloadAcc::Variable { values });
                }
                Some(PayloadAcc::Variable { values: acc_values }) => {
                    acc_values.extend(values);
                }
                Some(PayloadAcc::Fixed { .. }) => {
                    return Err(format!(
                        "payload kind mismatch while merging column pages: segment={}, output_column={}, expected=Fixed, actual=Variable",
                        segment_path, output_name
                    ));
                }
            },
        }

        if !has_null_flags && decoded.null_flags.is_some() {
            has_null_flags = true;
            null_flags_acc.resize(decoded_rows - decoded.num_values, 0_u8);
        }
        if has_null_flags {
            match decoded.null_flags {
                Some(page_flags) => {
                    if page_flags.len() != decoded.num_values {
                        return Err(format!(
                            "decoded null flag length mismatch while merging full column: segment={}, output_column={}, page_rows={}, page_null_flags={}",
                            segment_path,
                            output_name,
                            decoded.num_values,
                            page_flags.len()
                        ));
                    }
                    null_flags_acc.extend(page_flags);
                }
                None => {
                    null_flags_acc.resize(decoded_rows, 0_u8);
                }
            }
        }
    }

    if decoded_rows != total_num_values {
        return Err(format!(
            "decoded row count mismatch after merging full column pages: segment={}, output_column={}, expected_rows={}, actual_rows={}",
            segment_path, output_name, total_num_values, decoded_rows
        ));
    }
    if has_null_flags && null_flags_acc.len() != total_num_values {
        return Err(format!(
            "decoded null flag count mismatch after merging full column pages: segment={}, output_column={}, expected_rows={}, actual_null_flags={}",
            segment_path,
            output_name,
            total_num_values,
            null_flags_acc.len()
        ));
    }

    let payload = match payload_acc {
        Some(PayloadAcc::Fixed {
            elem_size,
            value_bytes,
        }) => {
            let expected_bytes = total_num_values.checked_mul(elem_size).ok_or_else(|| {
                format!(
                    "fixed payload size overflow while merging full column pages: segment={}, output_column={}, rows={}, elem_size={}",
                    segment_path, output_name, total_num_values, elem_size
                )
            })?;
            if value_bytes.len() != expected_bytes {
                return Err(format!(
                    "fixed payload size mismatch after merging full column pages: segment={}, output_column={}, expected_bytes={}, actual_bytes={}",
                    segment_path,
                    output_name,
                    expected_bytes,
                    value_bytes.len()
                ));
            }
            super::page::DecodedPageValuePayload::Fixed {
                value_bytes,
                elem_size,
            }
        }
        Some(PayloadAcc::Variable { values }) => {
            if values.len() != total_num_values {
                return Err(format!(
                    "variable payload size mismatch after merging full column pages: segment={}, output_column={}, expected_rows={}, actual_values={}",
                    segment_path,
                    output_name,
                    total_num_values,
                    values.len()
                ));
            }
            super::page::DecodedPageValuePayload::Variable { values }
        }
        None => {
            return Err(format!(
                "missing decoded payload while merging full column pages: segment={}, output_column={}",
                segment_path, output_name
            ));
        }
    };

    Ok(DecodedDataPageValues {
        payload,
        num_values: total_num_values,
        null_flags: if has_null_flags {
            Some(null_flags_acc)
        } else {
            None
        },
    })
}

/// Decode all data page references from column ordinal index.
pub(super) fn decode_all_data_page_refs(
    segment_path: &str,
    segment_bytes: &[u8],
    column_meta: &StarRocksSegmentColumnMeta,
    total_num_values: usize,
) -> Result<Vec<DataPageRef>, String> {
    let root_page = column_meta.ordinal_index_root_page.as_ref().ok_or_else(|| {
        format!(
            "missing ordinal index root page pointer in segment column meta: segment={}, unique_id={:?}",
            segment_path, column_meta.unique_id
        )
    })?;
    decode_data_page_refs_from_ordinal_index(
        segment_path,
        segment_bytes,
        root_page,
        column_meta.ordinal_index_root_is_data_page,
        total_num_values,
    )
}

/// Decode variable-length dictionary payload from the segment dict page.
///
/// StarRocks stores dictionary values as a binary PLAIN page.
fn decode_dictionary_page_values(
    segment_path: &str,
    page_bytes: &[u8],
    column_compression: i32,
    output_name: &str,
) -> Result<Vec<Vec<u8>>, String> {
    let envelope = decode_page_envelope(segment_path, page_bytes, column_compression)?;
    if envelope.footer.r#type != Some(PAGE_TYPE_DICTIONARY) {
        return Err(format!(
            "invalid dict page type for rust native reader: segment={}, output_column={}, page_type={:?}, expected={}",
            segment_path, output_name, envelope.footer.r#type, PAGE_TYPE_DICTIONARY
        ));
    }
    decode_binary_plain_values(segment_path, &envelope.body, None)
}

/// Recursively locate column metadata by StarRocks unique column id.
///
/// Segment footer columns can be nested for complex schemas; this helper keeps
/// lookup behavior explicit even though current native reader supports scalars only.
pub(super) fn find_column_meta_by_unique_id(
    columns: &[StarRocksSegmentColumnMeta],
    unique_id: u32,
) -> Option<&StarRocksSegmentColumnMeta> {
    fn walk(
        nodes: &[StarRocksSegmentColumnMeta],
        unique_id: u32,
    ) -> Option<&StarRocksSegmentColumnMeta> {
        for node in nodes {
            if node.unique_id == Some(unique_id) {
                return Some(node);
            }
            if let Some(found) = walk(&node.children, unique_id) {
                return Some(found);
            }
        }
        None
    }
    walk(columns, unique_id)
}
