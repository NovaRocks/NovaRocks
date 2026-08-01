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

//! Minimal, provider-private StarRocks segment footer decoder.
//!
//! Segment footer protobuf messages are persisted data rather than a public
//! connector contract.  The private DTOs below intentionally cover only the
//! facts the direct reader needs to locate and decode physical column pages.
//! They do not expose generated protobuf values outside the provider.

use crc32c::crc32c;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use prost::Message;

const FOOTER_TRAILER_SIZE: usize = 12;
const SEGMENT_MAGIC: &[u8; 4] = b"D0R1";
const SUPPORTED_FOOTER_VERSION: u32 = 1;

const COLUMN_INDEX_TYPE_ORDINAL_INDEX: i32 = 1;

/// A decoded segment footer for the provider-private direct reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksSegmentFooter {
    pub(crate) version: u32,
    pub(crate) num_rows: u32,
    pub(crate) columns: Vec<StarRocksSegmentColumnMeta>,
}

/// Physical column metadata needed to find data, dictionary, and ordinal pages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksSegmentColumnMeta {
    pub(crate) column_id: Option<u32>,
    pub(crate) unique_id: Option<u32>,
    pub(crate) logical_type: StarRocksLogicalType,
    pub(crate) encoding: Option<StarRocksPageEncoding>,
    pub(crate) compression: Option<StarRocksCompression>,
    pub(crate) nullable: bool,
    pub(crate) dictionary_page: Option<StarRocksPagePointer>,
    pub(crate) ordinal_index_page: Option<StarRocksPagePointer>,
    pub(crate) ordinal_index_is_data_page: bool,
    pub(crate) num_rows: Option<u64>,
    pub(crate) children: Vec<StarRocksSegmentColumnMeta>,
}

/// An absolute page position within its segment object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StarRocksPagePointer {
    pub(crate) offset: u64,
    pub(crate) size: u32,
}

/// Logical types consumed by the initial direct-read kernel closure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksLogicalType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Float,
    Double,
    Char,
    Varchar,
    Hll,
    Object,
    Decimal256,
    Boolean,
    Binary,
    VarBinary,
    Decimal32,
    Decimal64,
    Decimal128,
    Date,
    DateTime,
    Percentile,
    Json,
}

impl StarRocksLogicalType {
    fn decode(value: i32) -> Result<Self, ConnectorError> {
        match value {
            1 => Ok(Self::TinyInt),
            3 => Ok(Self::SmallInt),
            5 => Ok(Self::Int),
            7 => Ok(Self::BigInt),
            9 => Ok(Self::LargeInt),
            10 => Ok(Self::Float),
            11 => Ok(Self::Double),
            13 => Ok(Self::Char),
            17 => Ok(Self::Varchar),
            23 => Ok(Self::Hll),
            25 => Ok(Self::Object),
            26 => Ok(Self::Decimal256),
            24 => Ok(Self::Boolean),
            45 => Ok(Self::Binary),
            46 => Ok(Self::VarBinary),
            47 => Ok(Self::Decimal32),
            48 => Ok(Self::Decimal64),
            49 => Ok(Self::Decimal128),
            50 => Ok(Self::Date),
            51 => Ok(Self::DateTime),
            53 => Ok(Self::Percentile),
            54 => Ok(Self::Json),
            _ => Err(unsupported("unsupported StarRocks segment logical type")),
        }
    }
}

/// Page encodings that the direct reader may decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksPageEncoding {
    Plain,
    Rle,
    Dictionary,
    BitShuffle,
}

impl StarRocksPageEncoding {
    fn decode(value: i32) -> Result<Self, ConnectorError> {
        match value {
            2 => Ok(Self::Plain),
            4 => Ok(Self::Rle),
            5 => Ok(Self::Dictionary),
            6 => Ok(Self::BitShuffle),
            _ => Err(unsupported("unsupported StarRocks segment page encoding")),
        }
    }
}

/// Page compression algorithms supported by the direct-read closure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksCompression {
    None,
    Lz4Frame,
}

impl StarRocksCompression {
    fn decode(value: i32) -> Result<Self, ConnectorError> {
        match value {
            0 => Ok(Self::None),
            5 => Ok(Self::Lz4Frame),
            _ => Err(unsupported("unsupported StarRocks segment compression")),
        }
    }
}

/// Decode a complete StarRocks segment object footer after validating its
/// `D0R1` trailer, declared size, and CRC32C checksum.
pub(crate) fn decode_segment_footer(
    segment_path: &str,
    segment_bytes: &[u8],
) -> Result<StarRocksSegmentFooter, ConnectorError> {
    if segment_bytes.len() < FOOTER_TRAILER_SIZE {
        return Err(corrupt(
            "StarRocks segment is smaller than its footer trailer",
        ));
    }

    let trailer_offset = segment_bytes.len() - FOOTER_TRAILER_SIZE;
    let footer_size = u32::from_le_bytes(
        segment_bytes[trailer_offset..trailer_offset + 4]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks segment footer size"))?,
    ) as usize;
    let expected_checksum = u32::from_le_bytes(
        segment_bytes[trailer_offset + 4..trailer_offset + 8]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks segment footer checksum"))?,
    );
    if &segment_bytes[trailer_offset + 8..] != SEGMENT_MAGIC {
        return Err(corrupt("invalid StarRocks segment footer magic"));
    }
    if footer_size == 0 || footer_size > trailer_offset {
        return Err(corrupt("invalid StarRocks segment footer size"));
    }

    let footer_offset = trailer_offset - footer_size;
    let footer_bytes = &segment_bytes[footer_offset..trailer_offset];
    if crc32c(footer_bytes) != expected_checksum {
        return Err(corrupt("StarRocks segment footer checksum mismatch"));
    }
    validate_footer_wire(footer_bytes)?;
    let footer = SegmentFooterPbLite::decode(footer_bytes)
        .map_err(|_| corrupt("invalid StarRocks segment footer protobuf"))?;
    let version = footer.version.unwrap_or(SUPPORTED_FOOTER_VERSION);
    if version != SUPPORTED_FOOTER_VERSION {
        return Err(unsupported("unsupported StarRocks segment footer version"));
    }
    let num_rows = footer
        .num_rows
        .ok_or_else(|| corrupt("StarRocks segment footer is missing num_rows"))?;
    if footer.columns.is_empty() {
        return Err(corrupt("StarRocks segment footer has no columns"));
    }
    let columns = footer
        .columns
        .iter()
        .map(decode_column)
        .collect::<Result<Vec<_>, _>>()?;

    let _ = segment_path;
    Ok(StarRocksSegmentFooter {
        version,
        num_rows,
        columns,
    })
}

/// Prost intentionally retains unknown protobuf fields for forward
/// compatibility. Persisted direct-read metadata cannot use that behavior:
/// this V1 storage snapshot must fail closed when a read-reachable footer
/// changes, rather than silently interpreting a newer segment as an older one.
fn validate_footer_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    validate_message(bytes, |field, wire, value| match field {
        1 | 3 => require_wire(wire, 0),
        2 => validate_column_wire(require_message(wire, value)?),
        _ => Err(unsupported("unsupported StarRocks segment footer field")),
    })
}

fn validate_column_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    validate_message(bytes, |field, wire, value| match field {
        1 | 2 | 3 | 5 | 6 | 7 | 11 => require_wire(wire, 0),
        8 => validate_column_index_wire(require_message(wire, value)?),
        9 => validate_page_pointer_wire(require_message(wire, value)?),
        10 => validate_column_wire(require_message(wire, value)?),
        _ => Err(unsupported("unsupported StarRocks segment column field")),
    })
}

fn validate_column_index_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    validate_message(bytes, |field, wire, value| match field {
        1 => require_wire(wire, 0),
        7 => validate_ordinal_index_wire(require_message(wire, value)?),
        _ => Err(unsupported("unsupported StarRocks segment index field")),
    })
}

fn validate_ordinal_index_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    validate_message(bytes, |field, wire, value| match field {
        1 => validate_btree_meta_wire(require_message(wire, value)?),
        _ => Err(unsupported("unsupported StarRocks ordinal index field")),
    })
}

fn validate_btree_meta_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    validate_message(bytes, |field, wire, value| match field {
        1 => validate_page_pointer_wire(require_message(wire, value)?),
        2 => require_wire(wire, 0),
        _ => Err(unsupported("unsupported StarRocks B-tree metadata field")),
    })
}

fn validate_page_pointer_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    validate_message(bytes, |field, wire, _| match field {
        1 | 2 => require_wire(wire, 0),
        _ => Err(unsupported("unsupported StarRocks page pointer field")),
    })
}

fn validate_message(
    mut bytes: &[u8],
    mut field: impl FnMut(u32, u8, &[u8]) -> Result<(), ConnectorError>,
) -> Result<(), ConnectorError> {
    while !bytes.is_empty() {
        let key = read_varint(&mut bytes)?;
        let number = u32::try_from(key >> 3)
            .map_err(|_| corrupt("StarRocks protobuf field number is out of range"))?;
        let wire = (key & 0x7) as u8;
        if number == 0 {
            return Err(corrupt("StarRocks protobuf field number is zero"));
        }
        let value = match wire {
            0 => {
                let start = bytes;
                let _ = read_varint(&mut bytes)?;
                &start[..start.len() - bytes.len()]
            }
            2 => {
                let length = usize::try_from(read_varint(&mut bytes)?)
                    .map_err(|_| corrupt("StarRocks protobuf length is out of range"))?;
                if length > bytes.len() {
                    return Err(corrupt(
                        "truncated StarRocks protobuf length-delimited field",
                    ));
                }
                let (value, remaining) = bytes.split_at(length);
                bytes = remaining;
                value
            }
            _ => return Err(corrupt("unsupported StarRocks protobuf wire type")),
        };
        field(number, wire, value)?;
    }
    Ok(())
}

fn require_wire(actual: u8, expected: u8) -> Result<(), ConnectorError> {
    if actual == expected {
        Ok(())
    } else {
        Err(corrupt("StarRocks protobuf field has an invalid wire type"))
    }
}

fn require_message(wire: u8, value: &[u8]) -> Result<&[u8], ConnectorError> {
    require_wire(wire, 2)?;
    Ok(value)
}

fn read_varint(bytes: &mut &[u8]) -> Result<u64, ConnectorError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let Some((&byte, rest)) = bytes.split_first() else {
            return Err(corrupt("truncated StarRocks protobuf varint"));
        };
        *bytes = rest;
        if shift == 63 && byte > 1 {
            return Err(corrupt("StarRocks protobuf varint overflows"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(corrupt("StarRocks protobuf varint overflows"))
}

fn decode_column(value: &ColumnMetaPbLite) -> Result<StarRocksSegmentColumnMeta, ConnectorError> {
    let logical_type = value
        .r#type
        .ok_or_else(|| corrupt("StarRocks segment column is missing logical type"))
        .and_then(StarRocksLogicalType::decode)?;
    let encoding = value
        .encoding
        .map(StarRocksPageEncoding::decode)
        .transpose()?;
    let compression = value
        .compression
        .map(StarRocksCompression::decode)
        .transpose()?;
    let ordinal = value
        .indexes
        .iter()
        .filter(|index| index.r#type == Some(COLUMN_INDEX_TYPE_ORDINAL_INDEX))
        .map(|index| {
            index
                .ordinal_index
                .as_ref()
                .and_then(|ordinal| ordinal.root_page.as_ref())
                .ok_or_else(|| corrupt("StarRocks ordinal index is missing its root page"))
        })
        .next()
        .transpose()?;
    Ok(StarRocksSegmentColumnMeta {
        column_id: value.column_id,
        unique_id: value.unique_id,
        logical_type,
        encoding,
        compression,
        nullable: value.is_nullable.unwrap_or(false),
        dictionary_page: value
            .dict_page
            .as_ref()
            .map(decode_page_pointer)
            .transpose()?,
        ordinal_index_page: ordinal
            .and_then(|tree| tree.root_page.as_ref())
            .map(decode_page_pointer)
            .transpose()?,
        ordinal_index_is_data_page: ordinal
            .and_then(|tree| tree.is_root_data_page)
            .unwrap_or(false),
        num_rows: value.num_rows,
        children: value
            .children_columns
            .iter()
            .map(decode_column)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn decode_page_pointer(value: &PagePointerPbLite) -> Result<StarRocksPagePointer, ConnectorError> {
    let size = value
        .size
        .ok_or_else(|| corrupt("StarRocks segment page pointer is missing size"))?;
    if size == 0 {
        return Err(corrupt("StarRocks segment page pointer has zero size"));
    }
    Ok(StarRocksPagePointer {
        offset: value.offset.unwrap_or(0),
        size,
    })
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[derive(Clone, PartialEq, Message)]
struct SegmentFooterPbLite {
    #[prost(uint32, optional, tag = "1")]
    version: Option<u32>,
    #[prost(message, repeated, tag = "2")]
    columns: Vec<ColumnMetaPbLite>,
    #[prost(uint32, optional, tag = "3")]
    num_rows: Option<u32>,
}

#[derive(Clone, PartialEq, Message)]
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
struct ColumnIndexMetaPbLite {
    #[prost(int32, optional, tag = "1")]
    r#type: Option<i32>,
    #[prost(message, optional, tag = "7")]
    ordinal_index: Option<OrdinalIndexPbLite>,
}

#[derive(Clone, PartialEq, Message)]
struct OrdinalIndexPbLite {
    #[prost(message, optional, tag = "1")]
    root_page: Option<BTreeMetaPbLite>,
}

#[derive(Clone, PartialEq, Message)]
struct BTreeMetaPbLite {
    #[prost(message, optional, tag = "1")]
    root_page: Option<PagePointerPbLite>,
    #[prost(bool, optional, tag = "2")]
    is_root_data_page: Option<bool>,
}

#[derive(Clone, PartialEq, Message)]
struct PagePointerPbLite {
    #[prost(uint64, optional, tag = "1")]
    offset: Option<u64>,
    #[prost(uint32, optional, tag = "2")]
    size: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_footer_bytes() -> Vec<u8> {
        let footer = SegmentFooterPbLite {
            version: Some(SUPPORTED_FOOTER_VERSION),
            columns: vec![ColumnMetaPbLite {
                column_id: Some(0),
                unique_id: Some(1),
                r#type: Some(7),
                encoding: Some(2),
                compression: Some(0),
                is_nullable: Some(false),
                indexes: vec![],
                dict_page: None,
                children_columns: vec![],
                num_rows: Some(3),
            }],
            num_rows: Some(3),
        };
        let footer = footer.encode_to_vec();
        let mut segment = b"data".to_vec();
        segment.extend_from_slice(&footer);
        segment.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        segment.extend_from_slice(&crc32c(&footer).to_le_bytes());
        segment.extend_from_slice(SEGMENT_MAGIC);
        segment
    }

    #[test]
    fn decodes_valid_segment_footer() {
        let decoded = decode_segment_footer("segment.dat", &valid_footer_bytes()).unwrap();
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.num_rows, 3);
        assert_eq!(decoded.columns.len(), 1);
        assert_eq!(
            decoded.columns[0].logical_type,
            StarRocksLogicalType::BigInt
        );
        assert_eq!(
            decoded.columns[0].encoding,
            Some(StarRocksPageEncoding::Plain)
        );
    }

    #[test]
    fn rejects_invalid_magic_size_and_checksum() {
        let mut bad_magic = valid_footer_bytes();
        let len = bad_magic.len();
        bad_magic[len - 1] = b'X';
        assert_eq!(
            decode_segment_footer("segment.dat", &bad_magic)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let mut bad_size = valid_footer_bytes();
        let trailer = bad_size.len() - FOOTER_TRAILER_SIZE;
        bad_size[trailer..trailer + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            decode_segment_footer("segment.dat", &bad_size)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );

        let mut bad_checksum = valid_footer_bytes();
        let trailer = bad_checksum.len() - FOOTER_TRAILER_SIZE;
        bad_checksum[trailer + 4] ^= 1;
        assert_eq!(
            decode_segment_footer("segment.dat", &bad_checksum)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn rejects_unknown_mandatory_logical_type() {
        let footer = SegmentFooterPbLite {
            version: Some(SUPPORTED_FOOTER_VERSION),
            columns: vec![ColumnMetaPbLite {
                column_id: Some(0),
                unique_id: Some(1),
                r#type: Some(9_999),
                encoding: Some(2),
                compression: Some(0),
                is_nullable: Some(false),
                indexes: vec![],
                dict_page: None,
                children_columns: vec![],
                num_rows: Some(1),
            }],
            num_rows: Some(1),
        };
        let footer = footer.encode_to_vec();
        let mut segment = Vec::new();
        segment.extend_from_slice(&footer);
        segment.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        segment.extend_from_slice(&crc32c(&footer).to_le_bytes());
        segment.extend_from_slice(SEGMENT_MAGIC);

        assert_eq!(
            decode_segment_footer("segment.dat", &segment)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }

    #[test]
    fn rejects_unknown_footer_fields_before_prost_decode() {
        let mut segment = valid_footer_bytes();
        let trailer = segment.len() - FOOTER_TRAILER_SIZE;
        let footer_start = trailer
            - u32::from_le_bytes(segment[trailer..trailer + 4].try_into().unwrap()) as usize;
        let mut footer = segment[footer_start..trailer].to_vec();
        footer.extend_from_slice(&[0x20, 0x01]); // Unknown footer field 4, varint 1.
        segment.truncate(footer_start);
        segment.extend_from_slice(&footer);
        segment.extend_from_slice(&(footer.len() as u32).to_le_bytes());
        segment.extend_from_slice(&crc32c(&footer).to_le_bytes());
        segment.extend_from_slice(SEGMENT_MAGIC);

        assert_eq!(
            decode_segment_footer("segment.dat", &segment)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }
}
