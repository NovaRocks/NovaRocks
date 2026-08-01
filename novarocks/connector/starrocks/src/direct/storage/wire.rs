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

//! Provider-private read-only view of StarRocks lake metadata protobufs.
//!
//! This deliberately contains no transaction, compaction, write, or placement
//! records. The tags match StarRocks' persisted `lake_types.proto` and
//! `tablet_schema.proto`; the DTOs stay private to this connector.

use std::collections::BTreeMap;

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use prost::Message;

const BUNDLE_FOOTER_BYTES: usize = 8;

#[allow(clippy::enum_variant_names)]
mod generated {
    tonic::include_proto!("starrocks.storage");
}

use generated::{
    BundleTabletMetadataPb, ColumnPb, DeletePredicatePb, DelvecPagePb, FileMetaPb,
    RowsetMetadataPb, TabletMetadataPb, TabletSchemaPb,
};
#[cfg(test)]
use generated::{DelvecMetadataPb, PagePointerPb};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageModel {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageColumn {
    pub(crate) unique_id: i32,
    pub(crate) name: String,
    pub(crate) physical_type: String,
    pub(crate) is_key: bool,
    pub(crate) aggregation: Option<String>,
    pub(crate) nullable: bool,
    pub(crate) default_value: Option<Vec<u8>>,
    pub(crate) precision: Option<i32>,
    pub(crate) scale: Option<i32>,
    pub(crate) length: Option<i32>,
    pub(crate) children: Vec<StorageColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageSchema {
    pub(crate) id: Option<i64>,
    pub(crate) model: StorageModel,
    pub(crate) columns: Vec<StorageColumn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageDeletePredicate {
    pub(crate) sub_predicates: Vec<String>,
    pub(crate) in_predicates: Vec<StorageInPredicate>,
    pub(crate) binary_predicates: Vec<StorageBinaryPredicate>,
    pub(crate) is_null_predicates: Vec<StorageIsNullPredicate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageInPredicate {
    pub(crate) column_name: String,
    pub(crate) is_not_in: bool,
    pub(crate) values: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageBinaryPredicate {
    pub(crate) column_name: String,
    pub(crate) op: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageIsNullPredicate {
    pub(crate) column_name: String,
    pub(crate) is_not_null: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageDelvecPage {
    pub(crate) version: i64,
    pub(crate) offset: u64,
    pub(crate) size: u64,
    pub(crate) crc32c: Option<u32>,
    pub(crate) crc32c_gen_version: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageDelvecFile {
    pub(crate) name: String,
    pub(crate) size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageRowset {
    pub(crate) id: u32,
    pub(crate) segments: Vec<String>,
    pub(crate) segment_sizes: Vec<u64>,
    pub(crate) bundle_offsets: Vec<i64>,
    pub(crate) num_rows: i64,
    pub(crate) delete_predicate: Option<StorageDeletePredicate>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StorageTabletMetadata {
    pub(crate) id: i64,
    pub(crate) version: i64,
    pub(crate) schema: StorageSchema,
    pub(crate) rowsets: Vec<StorageRowset>,
    pub(crate) historical_schemas: BTreeMap<i64, StorageSchema>,
    pub(crate) rowset_to_schema: BTreeMap<u32, i64>,
    pub(crate) delvecs: BTreeMap<u32, StorageDelvecPage>,
    pub(crate) delvec_files: BTreeMap<i64, StorageDelvecFile>,
}

pub(crate) fn decode_standalone_metadata(
    bytes: &[u8],
    tablet_id: i64,
    version: i64,
) -> Result<StorageTabletMetadata, ConnectorError> {
    validate_tablet_metadata_wire(bytes)?;
    let raw = TabletMetadataPb::decode(bytes)
        .map_err(|_| corrupt("invalid StarRocks tablet metadata protobuf"))?;
    decode_tablet_metadata(raw, tablet_id, version)
}

pub(crate) fn decode_bundle_metadata(
    bytes: &[u8],
    tablet_id: i64,
    version: i64,
) -> Result<StorageTabletMetadata, ConnectorError> {
    if bytes.len() < BUNDLE_FOOTER_BYTES {
        return Err(corrupt(
            "StarRocks bundle metadata is smaller than its footer",
        ));
    }
    let footer = bytes.len() - BUNDLE_FOOTER_BYTES;
    let metadata_size = u64::from_le_bytes(
        bytes[footer..]
            .try_into()
            .map_err(|_| corrupt("invalid StarRocks bundle footer"))?,
    );
    let metadata_size = usize::try_from(metadata_size)
        .map_err(|_| corrupt("StarRocks bundle metadata size overflows"))?;
    if metadata_size == 0 || metadata_size > footer {
        return Err(corrupt("invalid StarRocks bundle metadata size"));
    }
    let bundle_start = footer - metadata_size;
    validate_bundle_metadata_wire(&bytes[bundle_start..footer])?;
    let bundle = BundleTabletMetadataPb::decode(&bytes[bundle_start..footer])
        .map_err(|_| corrupt("invalid StarRocks bundle metadata protobuf"))?;
    let page = bundle
        .tablet_meta_pages
        .get(&tablet_id)
        .ok_or_else(|| corrupt("StarRocks bundle omits the requested tablet page"))?;
    let offset = usize::try_from(page.offset.unwrap_or(0))
        .map_err(|_| corrupt("StarRocks tablet metadata page offset overflows"))?;
    let size = usize::try_from(page.size.unwrap_or(0))
        .map_err(|_| corrupt("StarRocks tablet metadata page size overflows"))?;
    let end = offset
        .checked_add(size)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| corrupt("StarRocks tablet metadata page is out of range"))?;
    let mut metadata = decode_standalone_metadata(&bytes[offset..end], tablet_id, version)?;
    let schema_id = *bundle
        .tablet_to_schema
        .get(&tablet_id)
        .ok_or_else(|| corrupt("StarRocks bundle omits the requested tablet schema ID"))?;
    let schema = bundle
        .schemas
        .get(&schema_id)
        .ok_or_else(|| corrupt("StarRocks bundle omits the requested tablet schema"))?;
    metadata.schema = decode_schema(schema.clone())?;
    metadata
        .historical_schemas
        .insert(schema_id, metadata.schema.clone());
    for schema_id in metadata.rowset_to_schema.values() {
        let schema = bundle
            .schemas
            .get(schema_id)
            .ok_or_else(|| corrupt("StarRocks bundle omits a rowset historical schema"))?;
        metadata
            .historical_schemas
            .insert(*schema_id, decode_schema(schema.clone())?);
    }
    Ok(metadata)
}

fn decode_tablet_metadata(
    value: TabletMetadataPb,
    tablet_id: i64,
    version: i64,
) -> Result<StorageTabletMetadata, ConnectorError> {
    if value.id != Some(tablet_id) || value.version != Some(version) {
        return Err(corrupt(
            "StarRocks tablet metadata identity does not match the frozen split",
        ));
    }
    let schema = value
        .schema
        .ok_or_else(|| corrupt("StarRocks tablet metadata has no schema"))
        .and_then(decode_schema)?;
    let historical_schemas = value
        .historical_schemas
        .into_iter()
        .map(|(id, schema)| decode_schema(schema).map(|schema| (id, schema)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let rowsets = value
        .rowsets
        .into_iter()
        .map(decode_rowset)
        .collect::<Result<Vec<_>, _>>()?;
    let (delvec_files, delvecs) = value
        .delvec_meta
        .map(|meta| {
            let files = meta
                .version_to_file
                .into_iter()
                .map(|(version, file)| {
                    decode_delvec_file(version, file).map(|file| (version, file))
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let pages = meta
                .delvecs
                .into_iter()
                .map(|(segment, page)| decode_delvec_page(page).map(|page| (segment, page)))
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            Ok::<_, ConnectorError>((files, pages))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(StorageTabletMetadata {
        id: tablet_id,
        version,
        schema,
        rowsets,
        historical_schemas,
        rowset_to_schema: value.rowset_to_schema.into_iter().collect(),
        delvecs,
        delvec_files,
    })
}

fn decode_schema(value: TabletSchemaPb) -> Result<StorageSchema, ConnectorError> {
    let model = match value.keys_type.unwrap_or(0) {
        0 => StorageModel::Duplicate,
        1 => StorageModel::Unique,
        2 => StorageModel::Aggregate,
        10 => StorageModel::Primary,
        _ => return Err(unsupported("unsupported StarRocks tablet key model")),
    };
    let columns = value
        .column
        .into_iter()
        .map(decode_column)
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    if columns.is_empty() {
        return Err(corrupt("StarRocks tablet schema has no columns"));
    }
    Ok(StorageSchema {
        id: value.id,
        model,
        columns,
    })
}

fn decode_column(column: ColumnPb) -> Result<StorageColumn, ConnectorError> {
    let unique_id = column
        .unique_id
        .ok_or_else(|| corrupt("StarRocks column is missing unique ID"))?;
    let name = column
        .name
        .ok_or_else(|| corrupt("StarRocks column is missing name"))?;
    let physical_type = column
        .r#type
        .ok_or_else(|| corrupt("StarRocks column is missing type"))?;
    if unique_id <= 0 || name.trim().is_empty() || physical_type.trim().is_empty() {
        return Err(corrupt("StarRocks column has invalid immutable facts"));
    }
    Ok(StorageColumn {
        unique_id,
        name,
        physical_type,
        is_key: column.is_key.unwrap_or(false),
        aggregation: column.aggregation,
        nullable: column.is_nullable.unwrap_or(false),
        default_value: column.default_value,
        precision: column.precision,
        scale: column.frac,
        length: column.length,
        children: column
            .children_columns
            .into_iter()
            .map(decode_column)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn decode_rowset(value: RowsetMetadataPb) -> Result<StorageRowset, ConnectorError> {
    let id = value
        .id
        .ok_or_else(|| corrupt("StarRocks rowset is missing ID"))?;
    let num_rows = value
        .num_rows
        .ok_or_else(|| corrupt("StarRocks rowset is missing row count"))?;
    if num_rows < 0 || value.segments.iter().any(|name| name.trim().is_empty()) {
        return Err(corrupt("StarRocks rowset has invalid immutable facts"));
    }
    if !value.segment_size.is_empty() && value.segment_size.len() != value.segments.len() {
        return Err(corrupt(
            "StarRocks rowset segment sizes do not match segments",
        ));
    }
    if !value.bundle_file_offsets.is_empty()
        && value.bundle_file_offsets.len() != value.segments.len()
    {
        return Err(corrupt(
            "StarRocks rowset bundle offsets do not match segments",
        ));
    }
    Ok(StorageRowset {
        id,
        segments: value.segments,
        segment_sizes: value.segment_size,
        bundle_offsets: value.bundle_file_offsets,
        num_rows,
        delete_predicate: value.delete_predicate.map(decode_delete_predicate),
    })
}

fn decode_delete_predicate(value: DeletePredicatePb) -> StorageDeletePredicate {
    StorageDeletePredicate {
        sub_predicates: value.sub_predicates,
        in_predicates: value
            .in_predicates
            .into_iter()
            .map(|value| StorageInPredicate {
                column_name: value.column_name.unwrap_or_default(),
                is_not_in: value.is_not_in.unwrap_or(false),
                values: value.values,
            })
            .collect(),
        binary_predicates: value
            .binary_predicates
            .into_iter()
            .map(|value| StorageBinaryPredicate {
                column_name: value.column_name.unwrap_or_default(),
                op: value.op.unwrap_or_default(),
                value: value.value.unwrap_or_default(),
            })
            .collect(),
        is_null_predicates: value
            .is_null_predicates
            .into_iter()
            .map(|value| StorageIsNullPredicate {
                column_name: value.column_name.unwrap_or_default(),
                is_not_null: value.is_not_null.unwrap_or(false),
            })
            .collect(),
    }
}

fn decode_delvec_page(value: DelvecPagePb) -> Result<StorageDelvecPage, ConnectorError> {
    let version = value
        .version
        .ok_or_else(|| corrupt("StarRocks delvec page is missing version"))?;
    let size = value
        .size
        .ok_or_else(|| corrupt("StarRocks delvec page is missing size"))?;
    if version <= 0 || size == 0 {
        return Err(corrupt("StarRocks delvec page has invalid facts"));
    }
    Ok(StorageDelvecPage {
        version,
        offset: value.offset.unwrap_or(0),
        size,
        crc32c: value.crc32c,
        crc32c_gen_version: value.crc32c_gen_version,
    })
}

fn decode_delvec_file(
    version: i64,
    value: FileMetaPb,
) -> Result<StorageDelvecFile, ConnectorError> {
    let name = value
        .name
        .filter(|name| !name.trim().is_empty())
        .ok_or_else(|| corrupt("StarRocks delete-vector file is missing its name"))?;
    if version <= 0 || value.size.is_some_and(|size| size < 0) {
        return Err(corrupt(
            "StarRocks delete-vector file has invalid frozen facts",
        ));
    }
    Ok(StorageDelvecFile {
        name,
        size: value.size.map(|size| size as u64),
    })
}

// Generated Prost types intentionally ignore fields they do not know.  That
// is correct for an application protocol but unsafe for a persisted-storage
// snapshot reader: an unrecognised field or enum can change read semantics.
// Validate every read-reachable message before decoding it into the generated
// private DTOs.  The validator never includes encoded values in diagnostics.
fn validate_tablet_metadata_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    validate_message(bytes, MessageKind::TabletMetadata)
}

fn validate_bundle_metadata_wire(bytes: &[u8]) -> Result<(), ConnectorError> {
    validate_message(bytes, MessageKind::BundleTabletMetadata)
}

#[derive(Clone, Copy)]
enum MessageKind {
    TabletMetadata,
    TabletSchema,
    Column,
    Rowset,
    DeletePredicate,
    InPredicate,
    BinaryPredicate,
    IsNullPredicate,
    DelvecMetadata,
    DelvecPage,
    DelvecFile,
    BundleTabletMetadata,
    PagePointer,
    MapInt64Schema,
    MapU32Int64,
    MapU32Delvec,
    MapInt64DelvecFile,
    MapInt64Int64,
    MapInt64PagePointer,
}

fn validate_message(mut input: &[u8], kind: MessageKind) -> Result<(), ConnectorError> {
    while !input.is_empty() {
        let key = read_varint(&mut input)?;
        let field = u32::try_from(key >> 3)
            .map_err(|_| corrupt("StarRocks storage protobuf field number overflows"))?;
        let wire = (key & 7) as u8;
        if field == 0 {
            return Err(corrupt("StarRocks storage protobuf uses field number zero"));
        }
        let nested = message_field(kind, field, wire)?;
        let payload = consume_field(&mut input, wire)?;
        if let Some(nested) = nested {
            if wire != 2 {
                return Err(corrupt(
                    "StarRocks storage nested protobuf has invalid wire type",
                ));
            }
            validate_message(payload, nested)?;
        }
        if matches!(kind, MessageKind::TabletSchema) && field == 1 {
            let value = decode_scalar_varint(payload, wire)?;
            if !matches!(value, 0 | 1 | 2 | 10) {
                return Err(unsupported("unknown StarRocks tablet key model enum"));
            }
        }
    }
    Ok(())
}

#[allow(clippy::let_and_return)]
fn message_field(
    kind: MessageKind,
    field: u32,
    wire: u8,
) -> Result<Option<MessageKind>, ConnectorError> {
    use MessageKind::*;
    let expected = match kind {
        TabletMetadata => match field {
            1 | 2 => scalar(0, wire),
            3 => nested(2, wire, TabletSchema),
            4 => nested(2, wire, Rowset),
            7 => nested(2, wire, DelvecMetadata),
            17 => nested(2, wire, MapInt64Schema),
            18 => nested(2, wire, MapU32Int64),
            _ => return Err(unsupported("unknown field in StarRocks tablet metadata")),
        },
        TabletSchema => match field {
            1 => scalar(0, wire),
            2 => nested(2, wire, Column),
            50 => scalar(0, wire),
            _ => return Err(unsupported("unknown field in StarRocks tablet schema")),
        },
        Column => match field {
            1 | 4 | 6 | 8 | 9 | 10 => scalar(0, wire),
            2 | 3 | 5 | 7 => scalar(2, wire),
            17 => nested(2, wire, Column),
            _ => return Err(unsupported("unknown field in StarRocks tablet column")),
        },
        Rowset => match field {
            1 | 4 => scalar(0, wire),
            3 | 8 => scalar(2, wire),
            14 => scalar(0, wire),
            6 => nested(2, wire, DeletePredicate),
            _ => return Err(unsupported("unknown field in StarRocks rowset metadata")),
        },
        DeletePredicate => match field {
            2 => scalar(2, wire),
            3 => nested(2, wire, InPredicate),
            4 => nested(2, wire, BinaryPredicate),
            5 => nested(2, wire, IsNullPredicate),
            _ => return Err(unsupported("unknown field in StarRocks delete predicate")),
        },
        InPredicate => match field {
            1 | 3 => scalar(2, wire),
            2 => scalar(0, wire),
            _ => {
                return Err(unsupported(
                    "unknown field in StarRocks IN delete predicate",
                ));
            }
        },
        BinaryPredicate => match field {
            1..=3 => scalar(2, wire),
            _ => {
                return Err(unsupported(
                    "unknown field in StarRocks binary delete predicate",
                ));
            }
        },
        IsNullPredicate => match field {
            1 => scalar(2, wire),
            2 => scalar(0, wire),
            _ => {
                return Err(unsupported(
                    "unknown field in StarRocks null delete predicate",
                ));
            }
        },
        DelvecMetadata => match field {
            1 => nested(2, wire, MapInt64DelvecFile),
            2 => nested(2, wire, MapU32Delvec),
            _ => {
                return Err(unsupported(
                    "unknown field in StarRocks delete-vector metadata",
                ));
            }
        },
        DelvecPage => match field {
            1..=5 => scalar(0, wire),
            _ => return Err(unsupported("unknown field in StarRocks delete-vector page")),
        },
        DelvecFile => match field {
            1 => scalar(2, wire),
            2 => scalar(0, wire),
            _ => return Err(unsupported("unknown field in StarRocks delete-vector file")),
        },
        BundleTabletMetadata => match field {
            1 => nested(2, wire, MapInt64Int64),
            2 => nested(2, wire, MapInt64Schema),
            3 => nested(2, wire, MapInt64PagePointer),
            _ => return Err(unsupported("unknown field in StarRocks bundle metadata")),
        },
        PagePointer => match field {
            1 | 2 => scalar(0, wire),
            _ => return Err(unsupported("unknown field in StarRocks page pointer")),
        },
        MapInt64Schema => match field {
            1 => scalar(0, wire),
            2 => nested(2, wire, TabletSchema),
            _ => return Err(unsupported("unknown field in StarRocks metadata map")),
        },
        MapU32Int64 | MapInt64Int64 => match field {
            1 | 2 => scalar(0, wire),
            _ => return Err(unsupported("unknown field in StarRocks metadata map")),
        },
        MapU32Delvec => match field {
            1 => scalar(0, wire),
            2 => nested(2, wire, DelvecPage),
            _ => return Err(unsupported("unknown field in StarRocks metadata map")),
        },
        MapInt64DelvecFile => match field {
            1 => scalar(0, wire),
            2 => nested(2, wire, DelvecFile),
            _ => return Err(unsupported("unknown field in StarRocks metadata map")),
        },
        MapInt64PagePointer => match field {
            1 => scalar(0, wire),
            2 => nested(2, wire, PagePointer),
            _ => return Err(unsupported("unknown field in StarRocks metadata map")),
        },
    };
    expected
}

fn scalar(expected_wire: u8, actual_wire: u8) -> Result<Option<MessageKind>, ConnectorError> {
    if expected_wire != actual_wire {
        return Err(corrupt(
            "StarRocks storage protobuf field has invalid wire type",
        ));
    }
    Ok(None)
}

fn nested(
    expected_wire: u8,
    actual_wire: u8,
    kind: MessageKind,
) -> Result<Option<MessageKind>, ConnectorError> {
    if expected_wire != actual_wire {
        return Err(corrupt(
            "StarRocks storage protobuf nested field has invalid wire type",
        ));
    }
    Ok(Some(kind))
}

fn consume_field<'a>(input: &mut &'a [u8], wire: u8) -> Result<&'a [u8], ConnectorError> {
    match wire {
        0 => {
            let begin = *input;
            let _ = read_varint(input)?;
            Ok(&begin[..begin.len() - input.len()])
        }
        1 => take(input, 8),
        2 => {
            let size = usize::try_from(read_varint(input)?)
                .map_err(|_| corrupt("StarRocks storage protobuf length overflows"))?;
            take(input, size)
        }
        5 => take(input, 4),
        _ => Err(corrupt("StarRocks storage protobuf has invalid wire type")),
    }
}

fn decode_scalar_varint(payload: &[u8], wire: u8) -> Result<u64, ConnectorError> {
    if wire != 0 {
        return Err(corrupt("StarRocks storage enum has invalid wire type"));
    }
    let mut payload = payload;
    let value = read_varint(&mut payload)?;
    if !payload.is_empty() {
        return Err(corrupt("StarRocks storage enum payload is malformed"));
    }
    Ok(value)
}

fn read_varint(input: &mut &[u8]) -> Result<u64, ConnectorError> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *input
            .first()
            .ok_or_else(|| corrupt("truncated StarRocks storage protobuf varint"))?;
        *input = &input[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(corrupt("overflowing StarRocks storage protobuf varint"))
}

fn take<'a>(input: &mut &'a [u8], size: usize) -> Result<&'a [u8], ConnectorError> {
    if size > input.len() {
        return Err(corrupt("truncated StarRocks storage protobuf field"));
    }
    let (value, remaining) = input.split_at(size);
    *input = remaining;
    Ok(value)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn schema() -> TabletSchemaPb {
        TabletSchemaPb {
            keys_type: Some(0),
            column: vec![ColumnPb {
                unique_id: Some(1),
                name: Some("id".into()),
                r#type: Some("BIGINT".into()),
                is_key: Some(true),
                aggregation: Some("SUM".into()),
                is_nullable: Some(false),
                default_value: None,
                precision: None,
                frac: None,
                length: None,
                children_columns: vec![],
            }],
            id: Some(7),
        }
    }
    fn tablet() -> TabletMetadataPb {
        TabletMetadataPb {
            id: Some(9),
            version: Some(3),
            schema: Some(schema()),
            rowsets: vec![RowsetMetadataPb {
                id: Some(4),
                segments: vec!["rs_0.dat".into()],
                num_rows: Some(2),
                delete_predicate: None,
                segment_size: vec![],
                bundle_file_offsets: vec![],
            }],
            delvec_meta: None,
            historical_schemas: Default::default(),
            rowset_to_schema: Default::default(),
        }
    }
    #[test]
    fn standalone_metadata_decodes_frozen_read_facts() {
        let bytes = tablet().encode_to_vec();
        let decoded = decode_standalone_metadata(&bytes, 9, 3).unwrap();
        assert_eq!(decoded.schema.columns[0].name, "id");
        assert!(decoded.schema.columns[0].is_key);
        assert_eq!(
            decoded.schema.columns[0].aggregation.as_deref(),
            Some("SUM")
        );
        assert_eq!(decoded.rowsets[0].segments, ["rs_0.dat"]);
    }

    #[test]
    fn standalone_metadata_decodes_primary_delvec_file_mapping() {
        let mut value = tablet();
        let mut delvec = DelvecMetadataPb {
            version_to_file: Default::default(),
            delvecs: Default::default(),
        };
        delvec.version_to_file.insert(
            7,
            FileMetaPb {
                name: Some("0000000000000007.delvec".into()),
                size: Some(32),
            },
        );
        delvec.delvecs.insert(
            4,
            DelvecPagePb {
                version: Some(7),
                offset: Some(4),
                size: Some(12),
                crc32c: Some(9),
                crc32c_gen_version: Some(7),
            },
        );
        value.delvec_meta = Some(delvec);

        let metadata = decode_standalone_metadata(&value.encode_to_vec(), 9, 3).unwrap();
        assert_eq!(
            metadata.delvec_files.get(&7).unwrap().name,
            "0000000000000007.delvec"
        );
        assert_eq!(metadata.delvec_files.get(&7).unwrap().size, Some(32));
        assert_eq!(
            metadata.delvecs.get(&4).unwrap().crc32c_gen_version,
            Some(7)
        );
    }
    #[test]
    fn standalone_metadata_rejects_frozen_identity_mismatch() {
        assert_eq!(
            decode_standalone_metadata(&tablet().encode_to_vec(), 9, 4)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn metadata_rejects_unknown_wire_field_and_key_model_enum() {
        let mut unknown_field = tablet().encode_to_vec();
        unknown_field.extend_from_slice(&[0x28, 1]); // field 5, varint
        assert_eq!(
            decode_standalone_metadata(&unknown_field, 9, 3)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );

        let mut unknown_enum = tablet();
        unknown_enum.schema.as_mut().unwrap().keys_type = Some(99);
        assert_eq!(
            decode_standalone_metadata(&unknown_enum.encode_to_vec(), 9, 3)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::Unsupported
        );
    }
    #[test]
    fn bundle_metadata_decodes_exact_tablet_page() {
        let page = tablet().encode_to_vec();
        let bundle = BundleTabletMetadataPb {
            tablet_to_schema: [(9, 7)].into_iter().collect(),
            schemas: [(7, schema())].into_iter().collect(),
            tablet_meta_pages: [(
                9,
                PagePointerPb {
                    offset: Some(0),
                    size: Some(page.len() as u32),
                },
            )]
            .into_iter()
            .collect(),
        }
        .encode_to_vec();
        let mut bytes = page;
        bytes.extend_from_slice(&bundle);
        bytes.extend_from_slice(&(bundle.len() as u64).to_le_bytes());
        assert_eq!(
            decode_bundle_metadata(&bytes, 9, 3).unwrap().schema.id,
            Some(7)
        );
    }
}
