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

use std::collections::{BTreeMap, HashSet};

use super::super::error::NativeFragmentLeafDecodeError;
use novarocks::connector::starrocks::STARROCKS_WIRE_INTERNAL_CATALOG_NAME;
use novarocks::connector::starrocks::fe_v2_meta::{LakeScanTabletRef, LakeTableIdentity};
use novarocks::connector::starrocks::scan::{
    DeferredLakeScanResolution, LakeScanSchemaMeta, StarRocksScanRange, StarRocksSchemaColumnHint,
};
use novarocks::connector::starrocks::schema::{
    StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
};
use novarocks::protocol::ProtocolErrorKind;
use novarocks::runtime::query_context::QueryId;
use novarocks::runtime::scan_range::{ScanRange, ScanRangeParams};
use novarocks_protocol::plan;

fn invalid<T>(detail: String) -> Result<T, NativeFragmentLeafDecodeError> {
    Err(NativeFragmentLeafDecodeError::at_field(
        ProtocolErrorKind::InvalidValue,
        "source",
        detail,
    ))
}
pub(super) struct DecodedStarRocksScanPreparation {
    pub(crate) properties: BTreeMap<String, String>,
    pub(crate) ranges: Vec<StarRocksScanRange>,
    pub(crate) lake_schema_meta: LakeScanSchemaMeta,
    pub(crate) deferred_lake_resolution: DeferredLakeScanResolution,
}

pub(super) fn decode_starrocks_scan_preparation(
    node_id: i32,
    scan: &plan::ScanNode,
    source: &plan::StarRocksTableSource,
    query_id: Option<QueryId>,
    range_params: &[ScanRangeParams],
) -> Result<DecodedStarRocksScanPreparation, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<DecodedStarRocksScanPreparation, NativeFragmentLeafDecodeError> {
        let native_tablet_schema = validate_source(node_id, source)?;
        let ranges = decode_ranges(node_id, range_params).map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "ranges",
                error,
            )
        })?;
        let tablet_refs = ranges
            .iter()
            .map(|range| LakeScanTabletRef {
                tablet_id: range.tablet_id,
                partition_id: range.partition_id.expect("validated partition_id"),
                version: range.version.expect("validated version"),
            })
            .collect::<Vec<_>>();
        let deferred_lake_resolution = DeferredLakeScanResolution::new(
            query_id,
            LakeTableIdentity {
                catalog: source.catalog_name.clone(),
                db_name: scan.database.clone(),
                table_name: scan
                    .table
                    .as_ref()
                    .map(|table| table.name.clone())
                    .unwrap_or_default(),
                db_id: source.db_id,
                table_id: source.table_id,
                schema_id: source.schema_id,
            },
            tablet_refs,
            None,
        );
        Ok(DecodedStarRocksScanPreparation {
            properties: BTreeMap::new(),
            ranges,
            lake_schema_meta: LakeScanSchemaMeta::with_embedded_schema(
                source.db_id,
                source.table_id,
                source.schema_id,
                query_id,
                native_tablet_schema,
                source
                    .storage_columns
                    .iter()
                    .map(|column| {
                        StarRocksSchemaColumnHint::new(
                            column.name.clone(),
                            column.unique_id,
                            column.default_value.clone(),
                        )
                    })
                    .collect(),
            ),
            deferred_lake_resolution,
        })
    })();
    decoded
}

fn validate_source(
    node_id: i32,
    source: &plan::StarRocksTableSource,
) -> Result<StarRocksTabletSchema, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<StarRocksTabletSchema, NativeFragmentLeafDecodeError> {
        if source.catalog_name.trim().is_empty() {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} catalog_name must not be empty"
            ));
        }
        if source.catalog_name != STARROCKS_WIRE_INTERNAL_CATALOG_NAME {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} catalog_name must be {STARROCKS_WIRE_INTERNAL_CATALOG_NAME}, got {}",
                source.catalog_name
            ));
        }
        for (field, value) in [
            ("db_id", source.db_id),
            ("table_id", source.table_id),
            ("schema_id", source.schema_id),
        ] {
            if value <= 0 {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} {field} must be positive, got {value}"
                ));
            }
        }
        if source.storage_columns.is_empty() {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} storage_columns must not be empty"
            ));
        }
        let mut names = HashSet::new();
        let mut unique_ids = HashSet::new();
        for column in &source.storage_columns {
            let name = column.name.trim();
            if name.is_empty() {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} storage column name must not be empty"
                ));
            }
            if column.unique_id < 0 {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} storage column {name} unique_id must be non-negative, got {}",
                    column.unique_id
                ));
            }
            if !names.insert(name.to_ascii_lowercase()) {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} storage columns contain duplicate name {name}"
                ));
            }
            if !unique_ids.insert(column.unique_id) {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} storage columns contain duplicate unique_id {}",
                    column.unique_id
                ));
            }
        }
        Ok(decode_native_tablet_schema(node_id, source)?)
    })();
    decoded
}

fn decode_native_tablet_schema(
    node_id: i32,
    source: &plan::StarRocksTableSource,
) -> Result<StarRocksTabletSchema, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<StarRocksTabletSchema, NativeFragmentLeafDecodeError> {
        let schema = source.current_schema.as_ref().ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "current_schema",
                format!("StarRocks ScanNode node_id={node_id} current_schema must be present"),
            )
        })?;
        if schema.schema_id != source.schema_id {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema id mismatch: source_schema_id={} current_schema_id={}",
                source.schema_id, schema.schema_id
            ));
        }
        let keys_type = match plan::StarRocksKeysType::try_from(schema.keys_type).ok() {
            Some(plan::StarRocksKeysType::StarrocksKeysTypeDuplicate) => {
                StarRocksKeysType::Duplicate
            }
            Some(plan::StarRocksKeysType::StarrocksKeysTypeUnique) => StarRocksKeysType::Unique,
            Some(plan::StarRocksKeysType::StarrocksKeysTypeAggregate) => {
                StarRocksKeysType::Aggregate
            }
            Some(plan::StarRocksKeysType::StarrocksKeysTypePrimary) => StarRocksKeysType::Primary,
            _ => {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} current schema keys_type is missing or unknown: {}",
                    schema.keys_type
                ));
            }
        };
        if schema.columns.is_empty() {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema columns must not be empty"
            ));
        }
        let columns = schema
            .columns
            .iter()
            .map(|column| decode_native_column_schema(node_id, column, true))
            .collect::<Result<Vec<_>, _>>()?;
        let mut names = HashSet::new();
        let mut unique_ids = HashSet::new();
        for column in &columns {
            let name = column.name.as_deref().expect("top-level name validated");
            if !names.insert(name.to_ascii_lowercase()) {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} current schema contains duplicate column name {name}"
                ));
            }
            if !unique_ids.insert(column.unique_id) {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} current schema contains duplicate unique_id {}",
                    column.unique_id
                ));
            }
        }
        if let Some(count) = schema.num_short_key_columns
            && (count < 0 || count as usize > columns.len())
        {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema num_short_key_columns out of range: {count}"
            ));
        }
        if schema
            .sort_key_idxes
            .iter()
            .any(|index| *index as usize >= columns.len())
        {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema sort_key_idxes contains out-of-range index"
            ));
        }
        for unique_id in &schema.sort_key_unique_ids {
            if !unique_ids.contains(&(*unique_id as i32)) {
                return invalid(format!(
                    "StarRocks ScanNode node_id={node_id} current schema sort_key_unique_ids references unknown unique_id {unique_id}"
                ));
            }
        }
        if !schema.sort_key_idxes.is_empty()
            && !schema.sort_key_unique_ids.is_empty()
            && (schema.sort_key_idxes.len() != schema.sort_key_unique_ids.len()
                || schema
                    .sort_key_idxes
                    .iter()
                    .zip(&schema.sort_key_unique_ids)
                    .any(|(index, unique_id)| {
                        columns[*index as usize].unique_id != *unique_id as i32
                    }))
        {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema sort key indexes and unique ids are inconsistent"
            ));
        }
        let visible_columns = columns
            .iter()
            .filter(|column| column.visible.unwrap_or(true))
            .map(|column| {
                (
                    column
                        .name
                        .as_deref()
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    column.unique_id,
                    column
                        .default_value
                        .as_deref()
                        .and_then(|value| std::str::from_utf8(value).ok()),
                )
            })
            .collect::<Vec<_>>();
        let storage_columns = source
            .storage_columns
            .iter()
            .map(|column| {
                (
                    column.name.to_ascii_lowercase(),
                    column.unique_id,
                    column.default_value.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        if visible_columns != storage_columns {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} storage_columns do not match current schema visible columns"
            ));
        }
        Ok(StarRocksTabletSchema {
            id: Some(schema.schema_id),
            keys_type: Some(keys_type),
            column: columns,
            num_short_key_columns: schema.num_short_key_columns,
            sort_key_idxes: schema.sort_key_idxes.clone(),
            sort_key_unique_ids: schema.sort_key_unique_ids.clone(),
            ..Default::default()
        })
    })();
    decoded
}

fn decode_native_column_schema(
    node_id: i32,
    column: &plan::StarRocksColumnSchema,
    top_level: bool,
) -> Result<StarRocksColumnSchema, NativeFragmentLeafDecodeError> {
    let decoded = (|| -> Result<StarRocksColumnSchema, NativeFragmentLeafDecodeError> {
        let name = column
            .name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_string);
        if top_level && name.is_none() {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema top-level column name must not be empty"
            ));
        }
        if top_level && column.unique_id < 0 {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema column {} unique_id must be non-negative, got {}",
                name.as_deref().unwrap_or("<unnamed>"),
                column.unique_id
            ));
        }
        let physical_type = column.physical_type.trim().to_ascii_uppercase();
        if physical_type.is_empty() {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema column {} physical_type must not be empty",
                name.as_deref().unwrap_or("<unnamed>")
            ));
        }
        let is_key = column.is_key.ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "is_key",
                format!(
                    "StarRocks ScanNode node_id={node_id} current schema column {} missing is_key",
                    name.as_deref().unwrap_or("<unnamed>")
                ),
            )
        })?;
        let nullable = column.nullable.ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "nullable",
                format!(
                    "StarRocks ScanNode node_id={node_id} current schema column {} missing nullable",
                    name.as_deref().unwrap_or("<unnamed>")
                ),
            )
        })?;
        let visible = column.visible.ok_or_else(|| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "visible",
                format!(
                    "StarRocks ScanNode node_id={node_id} current schema column {} missing visible",
                    name.as_deref().unwrap_or("<unnamed>")
                ),
            )
        })?;
        let aggregation = column
            .aggregation
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_ascii_uppercase);
        if column.aggregation.is_some() && aggregation.is_none() {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema column {} aggregation must not be empty when present",
                name.as_deref().unwrap_or("<unnamed>")
            ));
        }
        let children = column
            .children
            .iter()
            .map(|child| decode_native_column_schema(node_id, child, false))
            .collect::<Result<Vec<_>, _>>()?;
        let expected_children = match physical_type.as_str() {
            "ARRAY" => Some(1),
            "MAP" => Some(2),
            "STRUCT" => None,
            _ => Some(0),
        };
        if let Some(expected) = expected_children
            && children.len() != expected
        {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema column {} type {physical_type} requires {expected} children, got {}",
                name.as_deref().unwrap_or("<unnamed>"),
                children.len()
            ));
        }
        if physical_type == "STRUCT" && children.is_empty() {
            return invalid(format!(
                "StarRocks ScanNode node_id={node_id} current schema column {} STRUCT requires at least one child",
                name.as_deref().unwrap_or("<unnamed>")
            ));
        }
        if physical_type == "STRUCT" {
            let mut child_names = HashSet::new();
            let mut positive_child_ids = HashSet::new();
            for child in &children {
                let child_name = child
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::MissingField,
                        "name",
                        format!(
                            "StarRocks ScanNode node_id={node_id} STRUCT column {} child name must not be empty",
                            name.as_deref().unwrap_or("<unnamed>")
                        ),
                    )
                })?;
                if !child_names.insert(child_name.to_ascii_lowercase()) {
                    return invalid(format!(
                        "StarRocks ScanNode node_id={node_id} STRUCT column {} contains duplicate child name {child_name}",
                        name.as_deref().unwrap_or("<unnamed>")
                    ));
                }
                if child.unique_id >= 0 && !positive_child_ids.insert(child.unique_id) {
                    return invalid(format!(
                        "StarRocks ScanNode node_id={node_id} STRUCT column {} contains duplicate positive child unique_id {}",
                        name.as_deref().unwrap_or("<unnamed>"),
                        child.unique_id
                    ));
                }
            }
        }
        Ok(StarRocksColumnSchema {
            unique_id: column.unique_id,
            name,
            r#type: physical_type,
            is_key: Some(is_key),
            aggregation,
            is_nullable: Some(nullable),
            default_value: column
                .default_value
                .as_ref()
                .map(|value| value.as_bytes().to_vec()),
            precision: column.precision,
            frac: column.scale,
            visible: Some(visible),
            children_columns: children,
            ..Default::default()
        })
    })();
    decoded
}

fn decode_ranges(
    node_id: i32,
    ranges: &[ScanRangeParams],
) -> Result<Vec<StarRocksScanRange>, String> {
    let mut tablets = HashSet::new();
    let mut out = Vec::with_capacity(ranges.len());
    for (index, params) in ranges.iter().enumerate() {
        if params.has_more == Some(true) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} range index={index} does not support has_more=true"
            ));
        }
        if params.empty == Some(true) {
            continue;
        }
        let ScanRange::StarRocksTablet(range) = &params.range else {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} range index={index} expected StarRocks tablet range"
            ));
        };
        if !tablets.insert(range.tablet_id) {
            return Err(format!(
                "StarRocks ScanNode node_id={node_id} has duplicate tablet_id={}",
                range.tablet_id
            ));
        }
        out.push(StarRocksScanRange::new(
            range.tablet_id,
            range.partition_id,
            range.version,
        ));
    }
    Ok(out)
}
