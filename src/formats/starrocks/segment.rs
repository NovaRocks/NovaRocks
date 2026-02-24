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
//! Segment footer decoder for StarRocks native segment files.
//!
//! This module validates trailer magic/checksum and decodes a lightweight
//! protobuf view of column metadata used by the reader.
//!
//! Current limitations:
//! - Only footer trailer with magic `D0R1` is accepted.
//! - Segment footer schema is decoded as a partial protobuf view focused on
//!   fields required by native scan and pruning.

use crc32c::crc32c;
use prost::Message;

const SEGMENT_FOOTER_TRAILER_SIZE: usize = 12;
const SEGMENT_MAGIC: &[u8; 4] = b"D0R1";

#[derive(Clone, Debug, PartialEq, Eq)]
/// Decoded segment footer plus flattened column metadata tree.
pub struct StarRocksSegmentFooter {
    pub footer_size: u32,
    pub footer_checksum: u32,
    pub version: u32,
    pub num_rows: Option<u32>,
    pub columns: Vec<StarRocksSegmentColumnMeta>,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
/// Decoded column metadata required by native page reader.
pub struct StarRocksSegmentColumnMeta {
    pub column_id: Option<u32>,
    pub unique_id: Option<u32>,
    pub logical_type: Option<i32>,
    pub encoding: Option<i32>,
    pub compression: Option<i32>,
    pub is_nullable: Option<bool>,
    pub dict_page: Option<StarRocksPagePointer>,
    pub num_rows: Option<u64>,
    pub ordinal_index_root_page: Option<StarRocksPagePointer>,
    pub ordinal_index_root_is_data_page: Option<bool>,
    pub segment_zone_map: Option<StarRocksZoneMapMeta>,
    pub page_zone_map_index: Option<StarRocksIndexedColumnMeta>,
    pub bloom_filter_index: Option<StarRocksBloomFilterIndexMeta>,
    pub children: Vec<StarRocksSegmentColumnMeta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Byte range pointer inside one segment file.
pub struct StarRocksPagePointer {
    pub offset: u64,
    pub size: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Decoded zone map metadata from segment footer or page-zone-map index payload.
pub struct StarRocksZoneMapMeta {
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
    pub has_null: Option<bool>,
    pub has_not_null: Option<bool>,
}

impl StarRocksZoneMapMeta {
    /// Decode one serialized `ZoneMapPB` payload.
    pub fn decode_from_bytes(value: &[u8]) -> Result<Self, String> {
        let pb = ZoneMapPbLite::decode(value)
            .map_err(|e| format!("decode ZoneMapPB from page-zone-map payload failed: {e}"))?;
        Ok(Self::from_pb(&pb))
    }

    fn from_pb(pb: &ZoneMapPbLite) -> Self {
        Self {
            min: pb.min.clone(),
            max: pb.max.clone(),
            has_null: pb.has_null,
            has_not_null: pb.has_not_null,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Decoded indexed-column metadata used by zone map / bloom page indexes.
pub struct StarRocksIndexedColumnMeta {
    pub data_type: Option<i32>,
    pub encoding: Option<i32>,
    pub num_values: Option<i64>,
    pub ordinal_index_root_page: Option<StarRocksPagePointer>,
    pub ordinal_index_root_is_data_page: Option<bool>,
    pub compression: Option<i32>,
    pub size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Decoded bloom-filter index metadata.
pub struct StarRocksBloomFilterIndexMeta {
    pub hash_strategy: Option<i32>,
    pub algorithm: Option<i32>,
    pub bloom_filter: Option<StarRocksIndexedColumnMeta>,
}

/// Decode and validate segment footer from a complete segment byte slice.
pub fn decode_segment_footer(
    segment_path: &str,
    segment_bytes: &[u8],
) -> Result<StarRocksSegmentFooter, String> {
    if segment_bytes.len() < SEGMENT_FOOTER_TRAILER_SIZE {
        return Err(format!(
            "invalid segment file: {} (file too small, size={})",
            segment_path,
            segment_bytes.len()
        ));
    }

    let trailer_offset = segment_bytes.len() - SEGMENT_FOOTER_TRAILER_SIZE;
    let footer_size = u32::from_le_bytes(
        segment_bytes[trailer_offset..trailer_offset + 4]
            .try_into()
            .map_err(|_| "decode segment footer size failed".to_string())?,
    ) as usize;
    let footer_checksum = u32::from_le_bytes(
        segment_bytes[trailer_offset + 4..trailer_offset + 8]
            .try_into()
            .map_err(|_| "decode segment footer checksum failed".to_string())?,
    );
    let magic = &segment_bytes[trailer_offset + 8..trailer_offset + SEGMENT_FOOTER_TRAILER_SIZE];
    if magic != SEGMENT_MAGIC {
        return Err(format!(
            "invalid segment magic number: path={}, actual={:?}, expected={:?}",
            segment_path, magic, SEGMENT_MAGIC
        ));
    }
    if footer_size == 0 || footer_size > trailer_offset {
        return Err(format!(
            "invalid segment footer size: path={}, file_size={}, footer_size={}",
            segment_path,
            segment_bytes.len(),
            footer_size
        ));
    }

    let footer_offset = trailer_offset - footer_size;
    let footer_bytes = &segment_bytes[footer_offset..trailer_offset];
    let actual_checksum = crc32c(footer_bytes);
    if actual_checksum != footer_checksum {
        return Err(format!(
            "segment footer checksum mismatch: path={}, actual={}, expected={}",
            segment_path, actual_checksum, footer_checksum
        ));
    }

    let footer = SegmentFooterPbLite::decode(footer_bytes).map_err(|e| {
        format!(
            "decode SegmentFooterPB failed: path={}, footer_size={}, error={}",
            segment_path, footer_size, e
        )
    })?;
    Ok(StarRocksSegmentFooter {
        footer_size: u32::try_from(footer_size).map_err(|_| {
            format!(
                "segment footer size overflow: path={}, size={}",
                segment_path, footer_size
            )
        })?,
        footer_checksum,
        version: footer.version.unwrap_or(1),
        num_rows: footer.num_rows,
        columns: footer.columns.iter().map(to_column_meta).collect(),
    })
}

fn to_column_meta(pb: &ColumnMetaPbLite) -> StarRocksSegmentColumnMeta {
    let ordinal_index = pb
        .indexes
        .iter()
        .find(|idx| idx.r#type == Some(COLUMN_INDEX_TYPE_ORDINAL_INDEX))
        .and_then(|idx| idx.ordinal_index.as_ref())
        .and_then(|idx| idx.root_page.as_ref());
    let zone_map_index = pb
        .indexes
        .iter()
        .find(|idx| idx.r#type == Some(COLUMN_INDEX_TYPE_ZONE_MAP_INDEX))
        .and_then(|idx| idx.zone_map_index.as_ref());
    let bloom_filter_index = pb
        .indexes
        .iter()
        .find(|idx| idx.r#type == Some(COLUMN_INDEX_TYPE_BLOOM_FILTER_INDEX))
        .and_then(|idx| idx.bloom_filter_index.as_ref());
    let ordinal_index_root_page =
        ordinal_index
            .and_then(|root| root.root_page.as_ref())
            .map(|page| StarRocksPagePointer {
                offset: page.offset.unwrap_or(0),
                size: page.size.unwrap_or(0),
            });
    let ordinal_index_root_is_data_page = ordinal_index.and_then(|root| root.is_root_data_page);

    StarRocksSegmentColumnMeta {
        column_id: pb.column_id,
        unique_id: pb.unique_id,
        logical_type: pb.r#type,
        encoding: pb.encoding,
        compression: pb.compression,
        is_nullable: pb.is_nullable,
        dict_page: pb.dict_page.as_ref().map(|page| StarRocksPagePointer {
            offset: page.offset.unwrap_or(0),
            size: page.size.unwrap_or(0),
        }),
        num_rows: pb.num_rows,
        ordinal_index_root_page,
        ordinal_index_root_is_data_page,
        segment_zone_map: zone_map_index
            .and_then(|z| z.segment_zone_map.as_ref())
            .map(StarRocksZoneMapMeta::from_pb),
        page_zone_map_index: zone_map_index
            .and_then(|z| z.page_zone_maps.as_ref())
            .map(to_indexed_column_meta),
        bloom_filter_index: bloom_filter_index.map(|meta| StarRocksBloomFilterIndexMeta {
            hash_strategy: meta.hash_strategy,
            algorithm: meta.algorithm,
            bloom_filter: meta.bloom_filter.as_ref().map(to_indexed_column_meta),
        }),
        children: pb.children_columns.iter().map(to_column_meta).collect(),
    }
}

fn to_indexed_column_meta(pb: &IndexedColumnMetaPbLite) -> StarRocksIndexedColumnMeta {
    let ordinal_index = pb.ordinal_index_meta.as_ref();
    StarRocksIndexedColumnMeta {
        data_type: pb.data_type,
        encoding: pb.encoding,
        num_values: pb.num_values,
        ordinal_index_root_page: ordinal_index.and_then(|meta| meta.root_page.as_ref()).map(
            |page| StarRocksPagePointer {
                offset: page.offset.unwrap_or(0),
                size: page.size.unwrap_or(0),
            },
        ),
        ordinal_index_root_is_data_page: ordinal_index.and_then(|meta| meta.is_root_data_page),
        compression: pb.compression,
        size: pb.size,
    }
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `SegmentFooterPB` fields used by this crate.
struct SegmentFooterPbLite {
    #[prost(uint32, optional, tag = "1")]
    version: Option<u32>,
    #[prost(message, repeated, tag = "2")]
    columns: Vec<ColumnMetaPbLite>,
    #[prost(uint32, optional, tag = "3")]
    num_rows: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `ColumnMetaPB` fields used by this crate.
struct ColumnMetaPbLite {
    #[prost(uint32, optional, tag = "1")]
    column_id: Option<u32>,
    #[prost(uint32, optional, tag = "2")]
    unique_id: Option<u32>,
    #[prost(int32, optional, tag = "3")]
    r#type: Option<i32>,
    #[prost(int32, optional, tag = "5")]
    encoding: Option<i32>,
    #[prost(int32, optional, tag = "6")]
    compression: Option<i32>,
    #[prost(bool, optional, tag = "7")]
    is_nullable: Option<bool>,
    #[prost(message, repeated, tag = "8")]
    indexes: Vec<ColumnIndexMetaPbLite>,
    #[prost(message, optional, tag = "9")]
    dict_page: Option<PagePointerPbLite>,
    #[prost(message, repeated, tag = "10")]
    children_columns: Vec<ColumnMetaPbLite>,
    #[prost(uint64, optional, tag = "11")]
    num_rows: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `ColumnIndexMetaPB` fields used by this crate.
struct ColumnIndexMetaPbLite {
    #[prost(int32, optional, tag = "1")]
    r#type: Option<i32>,
    #[prost(message, optional, tag = "7")]
    ordinal_index: Option<OrdinalIndexPbLite>,
    #[prost(message, optional, tag = "8")]
    zone_map_index: Option<ZoneMapIndexPbLite>,
    #[prost(message, optional, tag = "10")]
    bloom_filter_index: Option<BloomFilterIndexPbLite>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `OrdinalIndexPB` fields used by this crate.
struct OrdinalIndexPbLite {
    #[prost(message, optional, tag = "1")]
    root_page: Option<BTreeMetaPbLite>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `BTreeMetaPB` fields used by this crate.
struct BTreeMetaPbLite {
    #[prost(message, optional, tag = "1")]
    root_page: Option<PagePointerPbLite>,
    #[prost(bool, optional, tag = "2")]
    is_root_data_page: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `PagePointerPB` fields used by this crate.
struct PagePointerPbLite {
    #[prost(uint64, optional, tag = "1")]
    offset: Option<u64>,
    #[prost(uint32, optional, tag = "2")]
    size: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `ZoneMapPB` fields used by pruning.
struct ZoneMapPbLite {
    #[prost(bytes, optional, tag = "1")]
    min: Option<Vec<u8>>,
    #[prost(bytes, optional, tag = "2")]
    max: Option<Vec<u8>>,
    #[prost(bool, optional, tag = "3")]
    has_null: Option<bool>,
    #[prost(bool, optional, tag = "4")]
    has_not_null: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `ZoneMapIndexPB` fields used by pruning.
struct ZoneMapIndexPbLite {
    #[prost(message, optional, tag = "1")]
    segment_zone_map: Option<ZoneMapPbLite>,
    #[prost(message, optional, tag = "2")]
    page_zone_maps: Option<IndexedColumnMetaPbLite>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `BloomFilterIndexPB` fields used by pruning.
struct BloomFilterIndexPbLite {
    #[prost(int32, optional, tag = "1")]
    hash_strategy: Option<i32>,
    #[prost(int32, optional, tag = "2")]
    algorithm: Option<i32>,
    #[prost(message, optional, tag = "3")]
    bloom_filter: Option<IndexedColumnMetaPbLite>,
}

#[derive(Clone, PartialEq, Message)]
/// Minimal `IndexedColumnMetaPB` fields used by pruning.
struct IndexedColumnMetaPbLite {
    #[prost(int32, optional, tag = "1")]
    data_type: Option<i32>,
    #[prost(int32, optional, tag = "2")]
    encoding: Option<i32>,
    #[prost(int64, optional, tag = "3")]
    num_values: Option<i64>,
    #[prost(message, optional, tag = "4")]
    ordinal_index_meta: Option<BTreeMetaPbLite>,
    #[prost(int32, optional, tag = "6")]
    compression: Option<i32>,
    #[prost(uint64, optional, tag = "7")]
    size: Option<u64>,
}

const COLUMN_INDEX_TYPE_ORDINAL_INDEX: i32 = 1;
const COLUMN_INDEX_TYPE_ZONE_MAP_INDEX: i32 = 2;
const COLUMN_INDEX_TYPE_BLOOM_FILTER_INDEX: i32 = 4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_valid_segment_footer() {
        let bytes = build_segment_bytes(
            SegmentFooterPbLite {
                version: Some(2),
                columns: Vec::new(),
                num_rows: Some(123),
            },
            16,
        );
        let footer = decode_segment_footer("s1.dat", &bytes).expect("decode footer");
        assert_eq!(footer.version, 2);
        assert_eq!(footer.num_rows, Some(123));
        assert!(footer.columns.is_empty());
    }

    #[test]
    fn reject_bad_segment_magic() {
        let mut bytes = build_segment_bytes(
            SegmentFooterPbLite {
                version: Some(1),
                columns: Vec::new(),
                num_rows: Some(1),
            },
            8,
        );
        let len = bytes.len();
        bytes[len - 1] = b'X';
        let err = decode_segment_footer("s2.dat", &bytes).expect_err("magic should mismatch");
        assert!(err.contains("invalid segment magic number"), "err={}", err);
    }

    #[test]
    fn reject_bad_segment_footer_checksum() {
        let mut bytes = build_segment_bytes(
            SegmentFooterPbLite {
                version: Some(1),
                columns: Vec::new(),
                num_rows: Some(9),
            },
            8,
        );
        let trailer_offset = bytes.len() - SEGMENT_FOOTER_TRAILER_SIZE;
        let checksum = u32::from_le_bytes(
            bytes[trailer_offset + 4..trailer_offset + 8]
                .try_into()
                .expect("checksum"),
        );
        bytes[trailer_offset + 4..trailer_offset + 8]
            .copy_from_slice(&checksum.wrapping_add(1).to_le_bytes());
        let err = decode_segment_footer("s3.dat", &bytes).expect_err("checksum should mismatch");
        assert!(err.contains("checksum mismatch"), "err={}", err);
    }

    #[test]
    fn decode_segment_footer_columns() {
        let bytes = build_segment_bytes(
            SegmentFooterPbLite {
                version: Some(1),
                columns: vec![ColumnMetaPbLite {
                    column_id: Some(1),
                    unique_id: Some(101),
                    r#type: Some(3),
                    encoding: Some(2),
                    compression: Some(7),
                    is_nullable: Some(false),
                    indexes: vec![ColumnIndexMetaPbLite {
                        r#type: Some(COLUMN_INDEX_TYPE_ORDINAL_INDEX),
                        ordinal_index: Some(OrdinalIndexPbLite {
                            root_page: Some(BTreeMetaPbLite {
                                root_page: Some(PagePointerPbLite {
                                    offset: Some(11),
                                    size: Some(22),
                                }),
                                is_root_data_page: Some(true),
                            }),
                        }),
                        zone_map_index: None,
                        bloom_filter_index: None,
                    }],
                    dict_page: None,
                    num_rows: Some(5),
                    children_columns: vec![ColumnMetaPbLite {
                        column_id: Some(2),
                        unique_id: Some(102),
                        r#type: Some(4),
                        encoding: None,
                        compression: None,
                        is_nullable: Some(true),
                        indexes: Vec::new(),
                        dict_page: None,
                        num_rows: None,
                        children_columns: Vec::new(),
                    }],
                }],
                num_rows: Some(5),
            },
            4,
        );
        let footer = decode_segment_footer("s4.dat", &bytes).expect("decode footer");
        assert_eq!(footer.columns.len(), 1);
        assert_eq!(footer.columns[0].column_id, Some(1));
        assert_eq!(footer.columns[0].unique_id, Some(101));
        assert_eq!(footer.columns[0].logical_type, Some(3));
        assert_eq!(footer.columns[0].encoding, Some(2));
        assert_eq!(footer.columns[0].compression, Some(7));
        assert_eq!(footer.columns[0].is_nullable, Some(false));
        assert_eq!(
            footer.columns[0]
                .ordinal_index_root_page
                .as_ref()
                .expect("ordinal root page")
                .offset,
            11
        );
        assert_eq!(
            footer.columns[0].ordinal_index_root_is_data_page,
            Some(true)
        );
        assert_eq!(footer.columns[0].children.len(), 1);
        assert_eq!(footer.columns[0].children[0].column_id, Some(2));
    }

    fn build_segment_bytes(footer: SegmentFooterPbLite, payload_len: usize) -> Vec<u8> {
        let footer_bytes = footer.encode_to_vec();
        let footer_len_u32 = u32::try_from(footer_bytes.len()).expect("footer bytes fit u32");
        let checksum = crc32c(&footer_bytes);

        let mut bytes = vec![0_u8; payload_len];
        bytes.extend_from_slice(&footer_bytes);
        bytes.extend_from_slice(&footer_len_u32.to_le_bytes());
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes.extend_from_slice(SEGMENT_MAGIC);
        bytes
    }
}
