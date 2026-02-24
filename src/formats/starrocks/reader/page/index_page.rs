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
//! Index-page decoder used by ordinal/page-index readers.
//!
//! StarRocks index page layout follows `IndexEntry := key_len(varint32) + key + page_pointer`.
//! `page_pointer` itself is `offset(varint64) + size(varint32)`.

use crate::formats::starrocks::segment::StarRocksPagePointer;

use super::super::constants::{COMPRESSION_NO_COMPRESSION, PAGE_TYPE_INDEX};
use super::decode_page_envelope;

/// Node type encoded by `IndexPageFooterPB.type`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexPageNodeType {
    Leaf,
    Internal,
}

/// One decoded index entry from an index page body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodedIndexPageEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) page_pointer: StarRocksPagePointer,
}

/// Decoded index page payload including node type and entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodedIndexPage {
    pub(crate) node_type: IndexPageNodeType,
    pub(crate) entries: Vec<DecodedIndexPageEntry>,
}

/// Decode all entries from one StarRocks index page.
pub(crate) fn decode_index_page(
    segment_path: &str,
    page_bytes: &[u8],
) -> Result<DecodedIndexPage, String> {
    let envelope = decode_page_envelope(segment_path, page_bytes, COMPRESSION_NO_COMPRESSION)?;
    if envelope.footer.r#type != Some(PAGE_TYPE_INDEX) {
        return Err(format!(
            "invalid page type for index page decoder: segment={}, page_type={:?}, expected={}",
            segment_path, envelope.footer.r#type, PAGE_TYPE_INDEX
        ));
    }
    let index_footer = envelope.footer.index_page_footer.as_ref().ok_or_else(|| {
        format!(
            "missing index_page_footer in index page footer: segment={}",
            segment_path
        )
    })?;
    let node_type = match index_footer.r#type.unwrap_or(0) {
        1 => IndexPageNodeType::Leaf,
        2 => IndexPageNodeType::Internal,
        other => {
            return Err(format!(
                "unsupported index page node type: segment={}, node_type={}, supported=[1(LEAF),2(INTERNAL)]",
                segment_path, other
            ));
        }
    };
    let num_entries = usize::try_from(index_footer.num_entries.unwrap_or(0)).map_err(|_| {
        format!(
            "invalid index page num_entries: segment={}, num_entries={:?}",
            segment_path, index_footer.num_entries
        )
    })?;

    let mut entries = Vec::with_capacity(num_entries);
    let mut offset = 0usize;
    for idx in 0..num_entries {
        let key_len = decode_varint32(&envelope.body, &mut offset).ok_or_else(|| {
            format!(
                "decode index entry key_len failed: segment={}, entry_index={}",
                segment_path, idx
            )
        })? as usize;
        let key_end = offset.checked_add(key_len).ok_or_else(|| {
            format!(
                "index entry key range overflow: segment={}, entry_index={}, offset={}, key_len={}",
                segment_path, idx, offset, key_len
            )
        })?;
        if key_end > envelope.body.len() {
            return Err(format!(
                "index entry key out of page body range: segment={}, entry_index={}, offset={}, key_len={}, body_size={}",
                segment_path,
                idx,
                offset,
                key_len,
                envelope.body.len()
            ));
        }
        let key = envelope.body[offset..key_end].to_vec();
        offset = key_end;

        let page_offset = decode_varint64(&envelope.body, &mut offset).ok_or_else(|| {
            format!(
                "decode index entry page offset failed: segment={}, entry_index={}",
                segment_path, idx
            )
        })?;
        let page_size = decode_varint32(&envelope.body, &mut offset).ok_or_else(|| {
            format!(
                "decode index entry page size failed: segment={}, entry_index={}",
                segment_path, idx
            )
        })?;

        entries.push(DecodedIndexPageEntry {
            key,
            page_pointer: StarRocksPagePointer {
                offset: page_offset,
                size: page_size,
            },
        });
    }

    Ok(DecodedIndexPage { node_type, entries })
}

fn decode_varint32(buf: &[u8], offset: &mut usize) -> Option<u32> {
    decode_varint64(buf, offset).and_then(|v| u32::try_from(v).ok())
}

fn decode_varint64(buf: &[u8], offset: &mut usize) -> Option<u64> {
    let mut shift = 0u32;
    let mut out = 0u64;
    while *offset < buf.len() && shift < 64 {
        let byte = buf[*offset];
        *offset += 1;
        out |= u64::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Some(out);
        }
        shift += 7;
    }
    None
}
