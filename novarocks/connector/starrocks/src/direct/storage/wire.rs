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
//! This deliberately contains no transaction, compaction, write, or replica
//! records. The tags match StarRocks' persisted `lake_types.proto` and
//! `tablet_schema.proto`; the DTOs stay private to this connector.

use std::collections::BTreeMap;

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use prost::Message;

const BUNDLE_FOOTER_BYTES: usize = 8;

mod generated {
    tonic::include_proto!("starrocks.storage");
}

use generated::{
    BundleTabletMetadataPb, ColumnPb, DeletePredicatePb, DelvecPagePb, PagePointerPb,
    RowsetMetadataPb, TabletMetadataPb, TabletSchemaPb,
};

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
    pub(crate) nullable: bool,
    pub(crate) default_value: Option<Vec<u8>>,
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
}

pub(crate) fn decode_standalone_metadata(
    bytes: &[u8],
    tablet_id: i64,
    version: i64,
) -> Result<StorageTabletMetadata, ConnectorError> {
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
    Ok(StorageTabletMetadata {
        id: tablet_id,
        version,
        schema,
        rowsets,
        historical_schemas,
        rowset_to_schema: value.rowset_to_schema.into_iter().collect(),
        delvecs: value
            .delvec_meta
            .map(|meta| {
                meta.delvecs
                    .into_iter()
                    .map(|(segment, page)| decode_delvec_page(page).map(|page| (segment, page)))
                    .collect()
            })
            .transpose()?
            .unwrap_or_default(),
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
        .map(|column| {
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
                nullable: column.is_nullable.unwrap_or(false),
                default_value: column.default_value,
            })
        })
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
    })
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
                is_nullable: Some(false),
                default_value: None,
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
        assert_eq!(decoded.rowsets[0].segments, ["rs_0.dat"]);
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
