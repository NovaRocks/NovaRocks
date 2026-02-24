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
//! Indexed-column helpers used by page-level pruning.
//!
//! This module decodes the small IndexedColumn structures used by StarRocks
//! zone-map and bloom-filter indexes.
//!
//! Current limitations:
//! - Assumes ordinal index keys for page references are `TYPE_UNSIGNED_BIGINT`
//!   full keys (8-byte big-endian order-preserving encoding).
//! - Indexed-column data-page payload is expected to be binary values.

use crate::formats::starrocks::segment::{StarRocksIndexedColumnMeta, StarRocksPagePointer};

use super::column_state::OutputColumnKind;
use super::constants::COMPRESSION_NO_COMPRESSION;
use super::page::{
    DecodedPageValuePayload, IndexPageNodeType, decode_data_page_values, decode_index_page,
    slice_page_bytes,
};

const MAX_INDEX_TREE_DEPTH: usize = 16;

/// One data page reference resolved from ordinal index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct DataPageRef {
    pub(super) first_ordinal: u64,
    pub(super) num_values: u64,
    pub(super) page_pointer: StarRocksPagePointer,
}

/// Decode per-page references from one ordinal index root.
pub(super) fn decode_data_page_refs_from_ordinal_index(
    segment_path: &str,
    segment_bytes: &[u8],
    root_page: &StarRocksPagePointer,
    root_is_data_page: Option<bool>,
    total_num_values: usize,
) -> Result<Vec<DataPageRef>, String> {
    let total_num_values = u64::try_from(total_num_values).map_err(|_| {
        format!(
            "invalid total_num_values for ordinal index decode: segment={}, total_num_values={}",
            segment_path, total_num_values
        )
    })?;

    match root_is_data_page {
        Some(true) => Ok(vec![DataPageRef {
            first_ordinal: 0,
            num_values: total_num_values,
            page_pointer: root_page.clone(),
        }]),
        Some(false) => {
            let entries = decode_leaf_entries_from_index_tree(
                segment_path,
                segment_bytes,
                root_page,
                "ordinal index root page",
                0,
            )?;
            if entries.is_empty() {
                return Err(format!(
                    "ordinal index root page has zero entries: segment={}",
                    segment_path
                ));
            }

            let mut refs = Vec::with_capacity(entries.len());
            for (idx, entry) in entries.iter().enumerate() {
                let first_ordinal =
                    decode_unsigned_bigint_ordinal_key(segment_path, idx, &entry.key)?;
                if first_ordinal > total_num_values {
                    return Err(format!(
                        "ordinal index key exceeds total rows: segment={}, entry_index={}, first_ordinal={}, total_num_values={}",
                        segment_path, idx, first_ordinal, total_num_values
                    ));
                }
                let next_first = if idx + 1 < entries.len() {
                    decode_unsigned_bigint_ordinal_key(
                        segment_path,
                        idx + 1,
                        &entries[idx + 1].key,
                    )?
                } else {
                    total_num_values
                };
                if next_first > total_num_values {
                    return Err(format!(
                        "ordinal index next key exceeds total rows: segment={}, entry_index={}, next_first_ordinal={}, total_num_values={}",
                        segment_path, idx, next_first, total_num_values
                    ));
                }
                if next_first < first_ordinal {
                    return Err(format!(
                        "ordinal index key is not sorted: segment={}, entry_index={}, first_ordinal={}, next_first_ordinal={}",
                        segment_path, idx, first_ordinal, next_first
                    ));
                }
                let num_values = next_first.saturating_sub(first_ordinal);
                if num_values == 0 {
                    continue;
                }
                refs.push(DataPageRef {
                    first_ordinal,
                    num_values,
                    page_pointer: entry.page_pointer.clone(),
                });
            }
            Ok(refs)
        }
        None => Err(format!(
            "missing ordinal_index_root_is_data_page in segment column meta: segment={}",
            segment_path
        )),
    }
}

fn decode_leaf_entries_from_index_tree(
    segment_path: &str,
    segment_bytes: &[u8],
    page_ptr: &StarRocksPagePointer,
    page_label: &str,
    depth: usize,
) -> Result<Vec<super::page::DecodedIndexPageEntry>, String> {
    if depth > MAX_INDEX_TREE_DEPTH {
        return Err(format!(
            "index page tree depth overflow: segment={}, page_label={}, depth={}, max_depth={}",
            segment_path, page_label, depth, MAX_INDEX_TREE_DEPTH
        ));
    }
    let page_bytes = slice_page_bytes(segment_path, segment_bytes, page_ptr, page_label, None)?;
    let decoded = decode_index_page(segment_path, page_bytes).map_err(|e| {
        format!(
            "decode index page failed: segment={}, page_label={}, page_offset={}, page_size={}, depth={}, error={}",
            segment_path, page_label, page_ptr.offset, page_ptr.size, depth, e
        )
    })?;
    match decoded.node_type {
        IndexPageNodeType::Leaf => Ok(decoded.entries),
        IndexPageNodeType::Internal => {
            let mut out = Vec::new();
            for (idx, entry) in decoded.entries.iter().enumerate() {
                let mut child = decode_leaf_entries_from_index_tree(
                    segment_path,
                    segment_bytes,
                    &entry.page_pointer,
                    "index page child",
                    depth + 1,
                )
                .map_err(|e| {
                    format!(
                        "decode index page child failed: segment={}, page_label={}, child_index={}, error={}",
                        segment_path, page_label, idx, e
                    )
                })?;
                out.append(&mut child);
            }
            Ok(out)
        }
    }
}

/// Decode all binary values from one indexed column.
pub(super) fn decode_indexed_binary_values(
    segment_path: &str,
    segment_bytes: &[u8],
    meta: &StarRocksIndexedColumnMeta,
) -> Result<Vec<Vec<u8>>, String> {
    let encoding = meta.encoding.ok_or_else(|| {
        format!(
            "missing encoding in indexed column meta: segment={}",
            segment_path
        )
    })?;
    let compression = meta.compression.unwrap_or(COMPRESSION_NO_COMPRESSION);
    let num_values_i64 = meta.num_values.ok_or_else(|| {
        format!(
            "missing num_values in indexed column meta: segment={}",
            segment_path
        )
    })?;
    let num_values = usize::try_from(num_values_i64).map_err(|_| {
        format!(
            "invalid indexed column num_values: segment={}, num_values={}",
            segment_path, num_values_i64
        )
    })?;
    let root_page = meta.ordinal_index_root_page.as_ref().ok_or_else(|| {
        format!(
            "missing ordinal index root page pointer in indexed column meta: segment={}",
            segment_path
        )
    })?;

    let page_refs = decode_data_page_refs_from_ordinal_index(
        segment_path,
        segment_bytes,
        root_page,
        meta.ordinal_index_root_is_data_page,
        num_values,
    )?;

    let mut out = Vec::with_capacity(num_values);
    for page_ref in &page_refs {
        let page_bytes = slice_page_bytes(
            segment_path,
            segment_bytes,
            &page_ref.page_pointer,
            "indexed column data page",
            None,
        )?;
        let decoded = decode_data_page_values(
            segment_path,
            page_bytes,
            compression,
            encoding,
            OutputColumnKind::Binary,
            None,
            Some(false),
            None,
        )
        .map_err(|e| {
            format!(
                "decode indexed column data page failed: segment={}, page_first_ordinal={}, page_offset={}, page_size={}, error={}",
                segment_path,
                page_ref.first_ordinal,
                page_ref.page_pointer.offset,
                page_ref.page_pointer.size,
                e
            )
        })?;
        if decoded.num_values as u64 != page_ref.num_values {
            return Err(format!(
                "indexed column page num_values mismatch: segment={}, page_first_ordinal={}, expected_num_values={}, actual_num_values={}",
                segment_path, page_ref.first_ordinal, page_ref.num_values, decoded.num_values
            ));
        }
        match decoded.payload {
            DecodedPageValuePayload::Variable { values } => {
                out.extend(values);
            }
            DecodedPageValuePayload::Fixed { .. } => {
                return Err(format!(
                    "indexed column payload must be variable-length values: segment={}",
                    segment_path
                ));
            }
        }
    }

    if out.len() != num_values {
        return Err(format!(
            "indexed column value count mismatch after decode: segment={}, expected_num_values={}, actual_num_values={}",
            segment_path,
            num_values,
            out.len()
        ));
    }
    Ok(out)
}

fn decode_unsigned_bigint_ordinal_key(
    segment_path: &str,
    entry_index: usize,
    key: &[u8],
) -> Result<u64, String> {
    if key.len() != 8 {
        return Err(format!(
            "invalid ordinal index key size: segment={}, entry_index={}, key_size={}, expected=8",
            segment_path,
            entry_index,
            key.len()
        ));
    }
    let raw: [u8; 8] = key
        .try_into()
        .map_err(|_| "convert ordinal key bytes failed".to_string())?;
    Ok(u64::from_be_bytes(raw))
}
