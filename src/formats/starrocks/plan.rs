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
//! Native read plan builder for StarRocks segment scans.
//!
//! This module validates FE schema/output-schema compatibility and emits a
//! deterministic scan plan consumed by the native reader.
//!
//! Current limitations:
//! - Supports DECIMALV3 only (DECIMAL32/64/128).
//! - Does not support DECIMALV2.
//! - Does not support VARIANT.

use std::collections::{BTreeSet, HashMap};

use arrow::datatypes::{DataType, Field, Fields, SchemaRef, TimeUnit};

use crate::common::largeint;
use crate::formats::starrocks::metadata::{
    StarRocksDeletePredicateRaw, StarRocksDelvecMetaRaw, StarRocksSegmentFile,
    StarRocksTabletSnapshot,
};
use crate::formats::starrocks::segment::{StarRocksSegmentColumnMeta, StarRocksSegmentFooter};
use crate::service::grpc_client::proto::starrocks::{ColumnPb, KeysType, TabletSchemaPb};

const STARROCKS_TYPE_TINYINT: &str = "TINYINT";
const STARROCKS_TYPE_SMALLINT: &str = "SMALLINT";
const STARROCKS_TYPE_INT: &str = "INT";
const STARROCKS_TYPE_BIGINT: &str = "BIGINT";
const STARROCKS_TYPE_LARGEINT: &str = "LARGEINT";
const STARROCKS_TYPE_FLOAT: &str = "FLOAT";
const STARROCKS_TYPE_DOUBLE: &str = "DOUBLE";
const STARROCKS_TYPE_BOOLEAN: &str = "BOOLEAN";
const STARROCKS_TYPE_DATE: &str = "DATE";
const STARROCKS_TYPE_DATE_V2: &str = "DATE_V2";
const STARROCKS_TYPE_DATETIME: &str = "DATETIME";
const STARROCKS_TYPE_DATETIME_V2: &str = "DATETIME_V2";
const STARROCKS_TYPE_TIMESTAMP: &str = "TIMESTAMP";
const STARROCKS_TYPE_CHAR: &str = "CHAR";
const STARROCKS_TYPE_VARCHAR: &str = "VARCHAR";
const STARROCKS_TYPE_STRING: &str = "STRING";
const STARROCKS_TYPE_HLL: &str = "HLL";
const STARROCKS_TYPE_OBJECT: &str = "OBJECT";
const STARROCKS_TYPE_BITMAP: &str = "BITMAP";
const STARROCKS_TYPE_JSON: &str = "JSON";
const STARROCKS_TYPE_PERCENTILE: &str = "PERCENTILE";
const STARROCKS_TYPE_BINARY: &str = "BINARY";
const STARROCKS_TYPE_VARBINARY: &str = "VARBINARY";
const STARROCKS_TYPE_DECIMAL32: &str = "DECIMAL32";
const STARROCKS_TYPE_DECIMAL64: &str = "DECIMAL64";
const STARROCKS_TYPE_DECIMAL128: &str = "DECIMAL128";
const STARROCKS_TYPE_DECIMAL256: &str = "DECIMAL256";
const STARROCKS_TYPE_ARRAY: &str = "ARRAY";
const STARROCKS_TYPE_MAP: &str = "MAP";
const STARROCKS_TYPE_STRUCT: &str = "STRUCT";
const SUPPORTED_SCHEMA_TYPES: [&str; 30] = [
    STARROCKS_TYPE_TINYINT,
    STARROCKS_TYPE_SMALLINT,
    STARROCKS_TYPE_INT,
    STARROCKS_TYPE_BIGINT,
    STARROCKS_TYPE_LARGEINT,
    STARROCKS_TYPE_FLOAT,
    STARROCKS_TYPE_DOUBLE,
    STARROCKS_TYPE_BOOLEAN,
    STARROCKS_TYPE_DATE,
    STARROCKS_TYPE_DATE_V2,
    STARROCKS_TYPE_DATETIME,
    STARROCKS_TYPE_DATETIME_V2,
    STARROCKS_TYPE_TIMESTAMP,
    STARROCKS_TYPE_CHAR,
    STARROCKS_TYPE_VARCHAR,
    STARROCKS_TYPE_STRING,
    STARROCKS_TYPE_HLL,
    STARROCKS_TYPE_OBJECT,
    STARROCKS_TYPE_BITMAP,
    STARROCKS_TYPE_JSON,
    STARROCKS_TYPE_PERCENTILE,
    STARROCKS_TYPE_BINARY,
    STARROCKS_TYPE_VARBINARY,
    STARROCKS_TYPE_DECIMAL32,
    STARROCKS_TYPE_DECIMAL64,
    STARROCKS_TYPE_DECIMAL128,
    STARROCKS_TYPE_DECIMAL256,
    STARROCKS_TYPE_ARRAY,
    STARROCKS_TYPE_MAP,
    STARROCKS_TYPE_STRUCT,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Subset of StarRocks schema types currently accepted by native reader.
enum SupportedSchemaType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Float,
    Double,
    Boolean,
    Date,
    DateTime,
    Char,
    Varchar,
    Hll,
    Object,
    Percentile,
    Binary,
    VarBinary,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Array,
    Map,
    Struct,
}

impl SupportedSchemaType {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_uppercase().as_str() {
            STARROCKS_TYPE_TINYINT => Some(Self::TinyInt),
            STARROCKS_TYPE_SMALLINT => Some(Self::SmallInt),
            STARROCKS_TYPE_INT => Some(Self::Int),
            STARROCKS_TYPE_BIGINT => Some(Self::BigInt),
            STARROCKS_TYPE_LARGEINT => Some(Self::LargeInt),
            STARROCKS_TYPE_FLOAT => Some(Self::Float),
            STARROCKS_TYPE_DOUBLE => Some(Self::Double),
            STARROCKS_TYPE_BOOLEAN => Some(Self::Boolean),
            STARROCKS_TYPE_DATE | STARROCKS_TYPE_DATE_V2 => Some(Self::Date),
            STARROCKS_TYPE_DATETIME | STARROCKS_TYPE_DATETIME_V2 | STARROCKS_TYPE_TIMESTAMP => {
                Some(Self::DateTime)
            }
            STARROCKS_TYPE_CHAR => Some(Self::Char),
            STARROCKS_TYPE_VARCHAR | STARROCKS_TYPE_STRING => Some(Self::Varchar),
            STARROCKS_TYPE_HLL => Some(Self::Hll),
            STARROCKS_TYPE_OBJECT | STARROCKS_TYPE_BITMAP | STARROCKS_TYPE_JSON => {
                Some(Self::Object)
            }
            STARROCKS_TYPE_PERCENTILE => Some(Self::Percentile),
            STARROCKS_TYPE_BINARY => Some(Self::Binary),
            STARROCKS_TYPE_VARBINARY => Some(Self::VarBinary),
            STARROCKS_TYPE_DECIMAL32 => Some(Self::Decimal32),
            STARROCKS_TYPE_DECIMAL64 => Some(Self::Decimal64),
            STARROCKS_TYPE_DECIMAL128 => Some(Self::Decimal128),
            STARROCKS_TYPE_DECIMAL256 => Some(Self::Decimal256),
            STARROCKS_TYPE_ARRAY => Some(Self::Array),
            STARROCKS_TYPE_MAP => Some(Self::Map),
            STARROCKS_TYPE_STRUCT => Some(Self::Struct),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::TinyInt => STARROCKS_TYPE_TINYINT,
            Self::SmallInt => STARROCKS_TYPE_SMALLINT,
            Self::Int => STARROCKS_TYPE_INT,
            Self::BigInt => STARROCKS_TYPE_BIGINT,
            Self::LargeInt => STARROCKS_TYPE_LARGEINT,
            Self::Float => STARROCKS_TYPE_FLOAT,
            Self::Double => STARROCKS_TYPE_DOUBLE,
            Self::Boolean => STARROCKS_TYPE_BOOLEAN,
            Self::Date => STARROCKS_TYPE_DATE,
            Self::DateTime => STARROCKS_TYPE_DATETIME,
            Self::Char => STARROCKS_TYPE_CHAR,
            Self::Varchar => STARROCKS_TYPE_VARCHAR,
            Self::Hll => STARROCKS_TYPE_HLL,
            Self::Object => STARROCKS_TYPE_OBJECT,
            Self::Percentile => STARROCKS_TYPE_PERCENTILE,
            Self::Binary => STARROCKS_TYPE_BINARY,
            Self::VarBinary => STARROCKS_TYPE_VARBINARY,
            Self::Decimal32 => STARROCKS_TYPE_DECIMAL32,
            Self::Decimal64 => STARROCKS_TYPE_DECIMAL64,
            Self::Decimal128 => STARROCKS_TYPE_DECIMAL128,
            Self::Decimal256 => STARROCKS_TYPE_DECIMAL256,
            Self::Array => STARROCKS_TYPE_ARRAY,
            Self::Map => STARROCKS_TYPE_MAP,
            Self::Struct => STARROCKS_TYPE_STRUCT,
        }
    }

    fn expected_arrow_type(self) -> &'static str {
        match self {
            Self::TinyInt => "Int8",
            Self::SmallInt => "Int16",
            Self::Int => "Int32",
            Self::BigInt => "Int64",
            Self::LargeInt => "FixedSizeBinary(16)",
            Self::Float => "Float32",
            Self::Double => "Float64",
            Self::Boolean => "Boolean",
            Self::Date => "Date32",
            Self::DateTime => "Timestamp(Microsecond,None)",
            Self::Char => "Utf8",
            Self::Varchar => "Utf8",
            Self::Hll => "Binary",
            Self::Object => "Binary",
            Self::Percentile => "Binary",
            Self::Binary => "Binary",
            Self::VarBinary => "Binary",
            Self::Decimal32 => "Decimal128(precision<=9,scale)",
            Self::Decimal64 => "Decimal128(precision<=18,scale)",
            Self::Decimal128 => "Decimal128(precision<=38,scale)",
            Self::Decimal256 => "Decimal256(precision<=76,scale)",
            Self::Array => "List",
            Self::Map => "Map",
            Self::Struct => "Struct",
        }
    }

    fn matches_arrow_type(self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (Self::TinyInt, DataType::Int8)
            | (Self::SmallInt, DataType::Int16)
            | (Self::Int, DataType::Int32)
            | (Self::BigInt, DataType::Int64)
            | (Self::Float, DataType::Float32)
            | (Self::Double, DataType::Float64)
            | (Self::Boolean, DataType::Boolean)
            | (Self::Date, DataType::Date32)
            | (Self::DateTime, DataType::Timestamp(TimeUnit::Microsecond, None))
            | (Self::Char, DataType::Utf8)
            | (Self::Varchar, DataType::Utf8)
            | (Self::Hll, DataType::Binary)
            | (Self::Hll, DataType::Utf8)
            | (Self::Object, DataType::Binary)
            | (Self::Object, DataType::Utf8)
            | (Self::Percentile, DataType::Binary)
            | (Self::Percentile, DataType::Utf8)
            | (Self::Binary, DataType::Binary)
            | (Self::VarBinary, DataType::Binary) => true,
            (Self::LargeInt, DataType::FixedSizeBinary(width))
                if *width == largeint::LARGEINT_BYTE_WIDTH =>
            {
                true
            }
            (Self::Decimal32, DataType::Decimal128(precision, _))
            | (Self::Decimal64, DataType::Decimal128(precision, _))
            | (Self::Decimal128, DataType::Decimal128(precision, _)) => {
                *precision > 0 && *precision <= self.decimal_max_precision()
            }
            (Self::Decimal256, DataType::Decimal256(precision, _)) => {
                *precision > 0 && *precision <= self.decimal_max_precision()
            }
            (Self::Array, DataType::List(_))
            | (Self::Map, DataType::Map(_, _))
            | (Self::Struct, DataType::Struct(_)) => true,
            _ => false,
        }
    }

    fn is_decimal_v3(self) -> bool {
        matches!(
            self,
            Self::Decimal32 | Self::Decimal64 | Self::Decimal128 | Self::Decimal256
        )
    }

    fn decimal_max_precision(self) -> u8 {
        match self {
            Self::Decimal32 => 9,
            Self::Decimal64 => 18,
            Self::Decimal128 => 38,
            Self::Decimal256 => 76,
            _ => 0,
        }
    }
}

#[derive(Clone, Debug)]
/// Recursive FE schema column plan used by native page readers.
pub struct StarRocksNativeSchemaColumnPlan {
    pub unique_id: Option<u32>,
    pub source_index: Option<usize>,
    pub source_lookup_attempted: bool,
    pub schema_type: String,
    pub is_nullable: bool,
    pub is_key: bool,
    pub aggregation: Option<String>,
    pub precision: Option<u8>,
    pub scale: Option<i8>,
    pub children: Vec<StarRocksNativeSchemaColumnPlan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Flat-JSON projection metadata for rewritten outputs like `json_col.key`.
pub struct StarRocksFlatJsonProjectionPlan {
    pub base_column_name: String,
    pub path: Vec<String>,
}

#[derive(Clone, Debug)]
/// Projection mapping from FE schema column to output schema slot.
pub struct StarRocksNativeColumnPlan {
    pub output_index: usize,
    pub output_name: String,
    pub schema_unique_id: u32,
    pub schema_type: String,
    pub schema: StarRocksNativeSchemaColumnPlan,
    pub flat_json_projection: Option<StarRocksFlatJsonProjectionPlan>,
    pub source_column_missing: bool,
    pub fallback_default_literal: Option<String>,
    pub fallback_is_nullable: bool,
}

#[derive(Clone, Debug)]
/// Grouping key columns used by AGG_KEYS / UNIQUE_KEYS model readers.
pub struct StarRocksNativeGroupKeyColumnPlan {
    pub output_name: String,
    pub schema_unique_id: u32,
    pub schema_type: String,
    pub schema: StarRocksNativeSchemaColumnPlan,
}

#[derive(Clone, Debug)]
/// One segment read unit in native scan order.
pub struct StarRocksNativeSegmentPlan {
    pub index: usize,
    pub path: String,
    pub relative_path: String,
    pub rowset_version: i64,
    pub segment_id: Option<u32>,
    pub bundle_file_offset: i64,
    pub segment_size: u64,
    pub footer_version: u32,
    pub footer_num_rows: u32,
    pub projected_schemas: Vec<StarRocksNativeSchemaColumnPlan>,
    pub source_column_missing_by_output: Vec<bool>,
}

#[derive(Clone, Debug)]
/// Primary-key delete-vector page pointer used by native reader.
pub struct StarRocksDelvecPagePlan {
    pub version: i64,
    pub offset: u64,
    pub size: u64,
    pub crc32c: Option<u32>,
    pub crc32c_gen_version: Option<i64>,
}

#[derive(Clone, Debug, Default)]
/// Primary-key delete-vector metadata needed by native reader.
pub struct StarRocksPrimaryDelvecPlan {
    pub version_to_file_rel_path: HashMap<i64, String>,
    pub segment_delvec_pages: HashMap<u32, StarRocksDelvecPagePlan>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Table model parsed from StarRocks tablet schema keys type.
pub enum StarRocksTableModelPlan {
    DupKeys,
    AggKeys,
    UniqueKeys,
    PrimaryKeys,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// One supported delete predicate operator in StarRocks metadata.
pub enum StarRocksDeletePredicateOpPlan {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotIn,
    IsNull,
    IsNotNull,
}

#[derive(Clone, Debug)]
/// One conjunctive delete predicate term resolved to schema unique id.
pub struct StarRocksDeletePredicateTermPlan {
    pub column_name: String,
    pub schema_unique_id: u32,
    pub schema_type: String,
    pub precision: Option<u8>,
    pub scale: Option<i8>,
    pub op: StarRocksDeletePredicateOpPlan,
    pub values: Vec<String>,
}

#[derive(Clone, Debug)]
/// One delete predicate group; terms in one group are conjunctive (AND).
pub struct StarRocksDeletePredicatePlan {
    pub version: i64,
    pub terms: Vec<StarRocksDeletePredicateTermPlan>,
}

#[derive(Clone, Debug)]
/// Full native read plan passed into data page reader.
pub struct StarRocksNativeReadPlan {
    pub tablet_id: i64,
    pub version: i64,
    pub table_model: StarRocksTableModelPlan,
    pub projected_columns: Vec<StarRocksNativeColumnPlan>,
    pub group_key_columns: Vec<StarRocksNativeGroupKeyColumnPlan>,
    pub segments: Vec<StarRocksNativeSegmentPlan>,
    pub delete_predicates: Vec<StarRocksDeletePredicatePlan>,
    pub primary_delvec: Option<StarRocksPrimaryDelvecPlan>,
    pub estimated_rows: u64,
}

struct SchemaColumnLookup<'a> {
    by_name: HashMap<String, &'a ColumnPb>,
    by_unique_id: HashMap<u32, &'a ColumnPb>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StarRocksOutputColumnHint {
    pub schema_unique_id: Option<u32>,
    pub fallback_default_literal: Option<String>,
}

pub fn build_native_read_plan(
    snapshot: &StarRocksTabletSnapshot,
    segment_footers: &[StarRocksSegmentFooter],
    output_schema: &SchemaRef,
    source_tablet_schema: Option<&TabletSchemaPb>,
) -> Result<StarRocksNativeReadPlan, String> {
    let output_column_hints = vec![
        StarRocksOutputColumnHint {
            schema_unique_id: None,
            fallback_default_literal: None,
        };
        output_schema.fields().len()
    ];
    build_native_read_plan_with_output_hints(
        snapshot,
        segment_footers,
        output_schema,
        &output_column_hints,
        source_tablet_schema,
    )
}

pub fn build_native_read_plan_with_output_hints(
    snapshot: &StarRocksTabletSnapshot,
    segment_footers: &[StarRocksSegmentFooter],
    output_schema: &SchemaRef,
    output_column_hints: &[StarRocksOutputColumnHint],
    source_tablet_schema: Option<&TabletSchemaPb>,
) -> Result<StarRocksNativeReadPlan, String> {
    if segment_footers.len() != snapshot.segment_files.len() {
        return Err(format!(
            "segment footer count mismatch: snapshot_segments={}, segment_footers={}",
            snapshot.segment_files.len(),
            segment_footers.len()
        ));
    }
    if output_column_hints.len() != output_schema.fields().len() {
        return Err(format!(
            "output column hint count mismatch: schema_fields={} hints={}",
            output_schema.fields().len(),
            output_column_hints.len()
        ));
    }

    let schema_columns = &snapshot.tablet_schema.column;
    if schema_columns.is_empty() {
        return Err(format!(
            "tablet schema has no columns in snapshot: tablet_id={}, version={}",
            snapshot.tablet_id, snapshot.version
        ));
    }
    let table_model = parse_table_model(
        snapshot.tablet_schema.keys_type,
        snapshot.tablet_id,
        snapshot.version,
    )?;
    let current_lookup =
        build_schema_column_lookup(schema_columns, snapshot.tablet_id, snapshot.version)?;
    let projected_columns = build_projected_columns(
        snapshot,
        output_schema,
        output_column_hints,
        &current_lookup,
        source_tablet_schema,
    )?;
    let group_key_columns = build_group_key_columns_plan(
        snapshot.tablet_id,
        snapshot.version,
        schema_columns,
        table_model,
    )?;
    let delete_predicates = build_delete_predicates_plan(
        snapshot.tablet_id,
        snapshot.version,
        &snapshot.delete_predicates,
        &current_lookup.by_name,
    )?;
    let primary_delvec = build_primary_delvec_plan(
        table_model,
        snapshot.tablet_id,
        snapshot.version,
        &snapshot.delvec_meta,
    )?;
    let mut segments = Vec::with_capacity(snapshot.segment_files.len());
    let mut estimated_rows = 0_u64;
    for (idx, (segment, footer)) in snapshot
        .segment_files
        .iter()
        .zip(segment_footers.iter())
        .enumerate()
    {
        let bundle_file_offset = segment.bundle_file_offset.ok_or_else(|| {
            format!(
                "segment bundle_file_offset missing in snapshot: index={}, path={}",
                idx, segment.path
            )
        })?;
        let segment_size = segment.segment_size.ok_or_else(|| {
            format!(
                "segment size missing in snapshot: index={}, path={}",
                idx, segment.path
            )
        })?;
        let footer_num_rows = footer.num_rows.ok_or_else(|| {
            format!(
                "segment footer num_rows is missing: index={}, path={}",
                idx, segment.path
            )
        })?;
        if table_model == StarRocksTableModelPlan::PrimaryKeys && segment.segment_id.is_none() {
            return Err(format!(
                "missing segment_id in primary key native read plan segment: tablet_id={}, version={}, segment_index={}, path={}",
                snapshot.tablet_id, snapshot.version, idx, segment.path
            ));
        }
        let segment_source_schema =
            resolve_segment_source_schema(snapshot, segment, source_tablet_schema)?;
        let segment_projected_columns = build_projected_columns(
            snapshot,
            output_schema,
            output_column_hints,
            &current_lookup,
            segment_source_schema,
        )?;
        if segment_projected_columns.len() != projected_columns.len() {
            let global_columns = projected_columns
                .iter()
                .map(|col| {
                    format!(
                        "{}#{}:{}",
                        col.output_index, col.output_name, col.schema_unique_id
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let segment_columns = segment_projected_columns
                .iter()
                .map(|col| {
                    format!(
                        "{}#{}:{}",
                        col.output_index, col.output_name, col.schema_unique_id
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let segment_schema_columns = segment_source_schema
                .map(|schema| {
                    schema
                        .column
                        .iter()
                        .map(|col| {
                            format!(
                                "{}:{}",
                                col.name.as_deref().unwrap_or("<unnamed>"),
                                col.unique_id
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "<none>".to_string());
            return Err(format!(
                "segment projected column count drifted from global plan: tablet_id={}, version={}, segment_index={}, path={}, global_count={}, segment_count={}, global_columns=[{}], segment_columns=[{}], segment_schema_columns=[{}]",
                snapshot.tablet_id,
                snapshot.version,
                idx,
                segment.path,
                projected_columns.len(),
                segment_projected_columns.len(),
                global_columns,
                segment_columns,
                segment_schema_columns
            ));
        }
        let footer_unique_ids = collect_unique_ids(&footer.columns)?;
        let mut projected_schemas = Vec::with_capacity(projected_columns.len());
        let mut source_column_missing_by_output = Vec::with_capacity(projected_columns.len());
        for (projected, segment_projected) in projected_columns
            .iter()
            .zip(segment_projected_columns.iter())
        {
            if projected.output_index != segment_projected.output_index
                || projected.schema_unique_id != segment_projected.schema_unique_id
            {
                return Err(format!(
                    "segment projected column plan drifted from global plan: tablet_id={}, version={}, segment_index={}, output_column={}, global_output_index={}, segment_output_index={}, global_unique_id={}, segment_unique_id={}",
                    snapshot.tablet_id,
                    snapshot.version,
                    idx,
                    projected.output_name,
                    projected.output_index,
                    segment_projected.output_index,
                    projected.schema_unique_id,
                    segment_projected.schema_unique_id
                ));
            }
            if !footer_unique_ids.contains(&projected.schema_unique_id)
                && !projected_can_fill_missing_values(
                    projected,
                    segment_projected.source_column_missing,
                )
            {
                return Err(format!(
                    "projected column unique_id is missing in segment footer and cannot be backfilled: tablet_id={}, version={}, segment_index={}, unique_id={}, output_column={}, path={}",
                    snapshot.tablet_id,
                    snapshot.version,
                    idx,
                    projected.schema_unique_id,
                    projected.output_name,
                    segment.path
                ));
            }
            projected_schemas.push(segment_projected.schema.clone());
            source_column_missing_by_output.push(segment_projected.source_column_missing);
        }
        estimated_rows = estimated_rows.saturating_add(u64::from(footer_num_rows));
        segments.push(StarRocksNativeSegmentPlan {
            index: idx,
            path: segment.path.clone(),
            relative_path: segment.relative_path.clone(),
            rowset_version: segment.rowset_version,
            segment_id: segment.segment_id,
            bundle_file_offset,
            segment_size,
            footer_version: footer.version,
            footer_num_rows,
            projected_schemas,
            source_column_missing_by_output,
        });
    }

    Ok(StarRocksNativeReadPlan {
        tablet_id: snapshot.tablet_id,
        version: snapshot.version,
        table_model,
        projected_columns,
        group_key_columns,
        segments,
        delete_predicates,
        primary_delvec,
        estimated_rows,
    })
}

fn build_schema_column_lookup<'a>(
    schema_columns: &'a [ColumnPb],
    tablet_id: i64,
    version: i64,
) -> Result<SchemaColumnLookup<'a>, String> {
    let mut by_name = HashMap::<String, &'a ColumnPb>::new();
    let mut by_unique_id = HashMap::<u32, &'a ColumnPb>::new();
    for col in schema_columns {
        let name = col
            .name
            .as_deref()
            .ok_or_else(|| {
                format!(
                    "tablet schema column name is missing: tablet_id={}, version={}, unique_id={}",
                    tablet_id, version, col.unique_id
                )
            })?
            .trim();
        if name.is_empty() {
            return Err(format!(
                "tablet schema column name is empty: tablet_id={}, version={}, unique_id={}",
                tablet_id, version, col.unique_id
            ));
        }
        let key = normalize_column_name(name);
        if by_name.insert(key, col).is_some() {
            return Err(format!(
                "duplicated column name in tablet schema: tablet_id={}, version={}, column_name={}",
                tablet_id, version, name
            ));
        }
        let unique_id = u32::try_from(col.unique_id).map_err(|_| {
            format!(
                "invalid column unique_id in tablet schema: tablet_id={}, version={}, unique_id={}",
                tablet_id, version, col.unique_id
            )
        })?;
        if by_unique_id.insert(unique_id, col).is_some() {
            return Err(format!(
                "duplicated column unique_id in tablet schema: tablet_id={}, version={}, unique_id={}",
                tablet_id, version, unique_id
            ));
        }
    }
    Ok(SchemaColumnLookup {
        by_name,
        by_unique_id,
    })
}

fn build_source_schema_lookup<'a>(
    source_schema: Option<&'a TabletSchemaPb>,
) -> SchemaColumnLookup<'a> {
    let mut by_name = HashMap::<String, &'a ColumnPb>::new();
    let mut by_unique_id = HashMap::<u32, &'a ColumnPb>::new();
    if let Some(source_schema) = source_schema {
        for col in &source_schema.column {
            let Some(name) = col.name.as_deref().map(str::trim) else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            by_name.insert(normalize_column_name(name), col);
            if let Ok(unique_id) = u32::try_from(col.unique_id) {
                by_unique_id.insert(unique_id, col);
            }
        }
    }
    SchemaColumnLookup {
        by_name,
        by_unique_id,
    }
}

fn build_projected_columns(
    snapshot: &StarRocksTabletSnapshot,
    output_schema: &SchemaRef,
    output_column_hints: &[StarRocksOutputColumnHint],
    current_lookup: &SchemaColumnLookup<'_>,
    source_tablet_schema: Option<&TabletSchemaPb>,
) -> Result<Vec<StarRocksNativeColumnPlan>, String> {
    let source_lookup = build_source_schema_lookup(source_tablet_schema);
    let mut projected_columns = Vec::with_capacity(output_schema.fields().len());
    for (idx, field) in output_schema.fields().iter().enumerate() {
        let output_name = field.name().trim();
        let normalized_name = normalize_column_name(output_name);
        let output_hint = output_column_hints.get(idx).ok_or_else(|| {
            format!(
                "missing output column hint for projected column: tablet_id={}, version={}, output_field={}, output_index={}",
                snapshot.tablet_id, snapshot.version, output_name, idx
            )
        })?;
        let output_field_unique_id = output_hint.schema_unique_id;
        let schema_col_from_unique_id = output_field_unique_id
            .and_then(|unique_id| current_lookup.by_unique_id.get(&unique_id).copied());
        let schema_col_from_name = current_lookup.by_name.get(&normalized_name).copied();
        let schema_col = if let Some(schema_col) = schema_col_from_unique_id {
            Some(schema_col)
        } else if let Some(schema_col) = schema_col_from_name {
            if let Some(expected_unique_id) = output_field_unique_id {
                let name_col_unique_id = u32::try_from(schema_col.unique_id).map_err(|_| {
                    format!(
                        "invalid column unique_id in tablet schema while matching output column hint: tablet_id={}, version={}, output_field={}, unique_id={}",
                        snapshot.tablet_id, snapshot.version, output_name, schema_col.unique_id
                    )
                })?;
                (name_col_unique_id == expected_unique_id).then_some(schema_col)
            } else {
                Some(schema_col)
            }
        } else {
            None
        };
        let allow_flat_json_fallback = output_field_unique_id.is_none();
        let source_schema_col = if source_tablet_schema.is_some() {
            if let Some(unique_id) = output_field_unique_id {
                source_lookup
                    .by_unique_id
                    .get(&unique_id)
                    .copied()
                    .or_else(|| source_lookup.by_name.get(&normalized_name).copied())
            } else {
                source_lookup.by_name.get(&normalized_name).copied()
            }
        } else {
            None
        };
        let (
            schema,
            schema_unique_id,
            flat_json_projection,
            source_column_missing,
            fallback_default_literal,
            fallback_is_nullable,
        ) = if let Some(schema_col) = schema_col {
            let schema = build_schema_column_plan(
                snapshot.tablet_id,
                snapshot.version,
                output_name,
                schema_col,
                source_schema_col,
                None,
                source_schema_col.is_some(),
                field.data_type(),
            )?;
            let schema_unique_id = schema.unique_id.ok_or_else(|| {
                format!(
                    "invalid schema column unique_id for output field: tablet_id={}, version={}, output_field={}, unique_id={}",
                    snapshot.tablet_id, snapshot.version, output_name, schema_col.unique_id
                )
            })?;
            (
                schema,
                schema_unique_id,
                None,
                false,
                schema_col
                    .default_value
                    .as_ref()
                    .map(|raw| String::from_utf8_lossy(raw).to_string()),
                schema_col.is_nullable.unwrap_or(field.is_nullable()),
            )
        } else if allow_flat_json_fallback {
            if let Some((schema_col, projection)) =
                try_build_flat_json_projection(output_name, &current_lookup.by_name)
            {
                let schema = build_schema_column_plan(
                    snapshot.tablet_id,
                    snapshot.version,
                    output_name,
                    schema_col,
                    None,
                    None,
                    false,
                    &DataType::Binary,
                )?;
                let schema_unique_id = schema.unique_id.ok_or_else(|| {
                    format!(
                        "invalid schema column unique_id for output field: tablet_id={}, version={}, output_field={}, unique_id={}",
                        snapshot.tablet_id, snapshot.version, output_name, schema_col.unique_id
                    )
                })?;
                (
                    schema,
                    schema_unique_id,
                    Some(projection),
                    false,
                    None,
                    true,
                )
            } else if let Some(projection) = parse_flat_json_projection(output_name) {
                let normalized_base_name = normalize_column_name(&projection.base_column_name);
                if let Some(schema_col) = current_lookup.by_name.get(&normalized_base_name).copied()
                {
                    return Err(format!(
                        "flat json projection base column is not JSON: tablet_id={}, version={}, output_field={}, base_column={}, base_schema_type={}",
                        snapshot.tablet_id,
                        snapshot.version,
                        output_name,
                        projection.base_column_name,
                        schema_col.r#type
                    ));
                }
                let schema_type =
                    infer_missing_source_schema_type(field.data_type()).ok_or_else(|| {
                        format!(
                            "unsupported output field type for missing flat json source column: tablet_id={}, version={}, output_field={}, output_type={:?}, supported=[Boolean,Int8,Int16,Int32,Int64,Float32,Float64,Utf8,Binary]",
                            snapshot.tablet_id,
                            snapshot.version,
                            output_name,
                            field.data_type()
                        )
                    })?;
                let output_index_u32 = u32::try_from(idx).map_err(|_| {
                    format!(
                        "output index overflow for missing flat json source column: tablet_id={}, version={}, output_field={}, output_index={}",
                        snapshot.tablet_id, snapshot.version, output_name, idx
                    )
                })?;
                let schema_unique_id = u32::MAX.checked_sub(output_index_u32).ok_or_else(|| {
                    format!(
                        "failed to assign synthetic unique id for missing flat json source column: tablet_id={}, version={}, output_field={}, output_index={}",
                        snapshot.tablet_id, snapshot.version, output_name, idx
                    )
                })?;
                let schema = StarRocksNativeSchemaColumnPlan {
                    unique_id: None,
                    source_index: None,
                    source_lookup_attempted: false,
                    schema_type: schema_type.to_string(),
                    is_nullable: true,
                    is_key: false,
                    aggregation: None,
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                };
                (schema, schema_unique_id, Some(projection), true, None, true)
            } else if !output_name.contains('.')
                && matches!(field.data_type(), DataType::Binary | DataType::Utf8)
            {
                let schema_type =
                    infer_missing_source_schema_type(field.data_type()).ok_or_else(|| {
                        format!(
                            "unsupported output field type for missing source column: tablet_id={}, version={}, output_field={}, output_type={:?}",
                            snapshot.tablet_id,
                            snapshot.version,
                            output_name,
                            field.data_type()
                        )
                    })?;
                let output_index_u32 = u32::try_from(idx).map_err(|_| {
                    format!(
                        "output index overflow for missing source column: tablet_id={}, version={}, output_field={}, output_index={}",
                        snapshot.tablet_id, snapshot.version, output_name, idx
                    )
                })?;
                let schema_unique_id = u32::MAX.checked_sub(output_index_u32).ok_or_else(|| {
                    format!(
                        "failed to assign synthetic unique id for missing source column: tablet_id={}, version={}, output_field={}, output_index={}",
                        snapshot.tablet_id, snapshot.version, output_name, idx
                    )
                })?;
                let schema = StarRocksNativeSchemaColumnPlan {
                    unique_id: None,
                    source_index: None,
                    source_lookup_attempted: false,
                    schema_type: schema_type.to_string(),
                    is_nullable: true,
                    is_key: false,
                    aggregation: None,
                    precision: None,
                    scale: None,
                    children: Vec::new(),
                };
                (schema, schema_unique_id, None, true, None, true)
            } else {
                let (schema, schema_unique_id, fallback_default_literal, fallback_is_nullable) =
                    build_missing_output_schema_column_plan(snapshot, field.as_ref(), output_hint)?;
                (
                    schema,
                    schema_unique_id,
                    None,
                    false,
                    fallback_default_literal,
                    fallback_is_nullable,
                )
            }
        } else {
            let (schema, schema_unique_id, fallback_default_literal, fallback_is_nullable) =
                build_missing_output_schema_column_plan(snapshot, field.as_ref(), output_hint)?;
            (
                schema,
                schema_unique_id,
                None,
                false,
                fallback_default_literal,
                fallback_is_nullable,
            )
        };
        projected_columns.push(StarRocksNativeColumnPlan {
            output_index: idx,
            output_name: output_name.to_string(),
            schema_unique_id,
            schema_type: schema.schema_type.clone(),
            schema,
            flat_json_projection,
            source_column_missing,
            fallback_default_literal,
            fallback_is_nullable,
        });
    }
    Ok(projected_columns)
}

fn resolve_segment_source_schema<'a>(
    snapshot: &'a StarRocksTabletSnapshot,
    segment: &StarRocksSegmentFile,
    fallback_source_schema: Option<&'a TabletSchemaPb>,
) -> Result<Option<&'a TabletSchemaPb>, String> {
    let Some(schema_id) = segment.schema_id.filter(|id| *id > 0) else {
        return Ok(fallback_source_schema);
    };
    if let Some(schema) = snapshot.historical_schemas.get(&schema_id) {
        return Ok(Some(schema));
    }
    if fallback_source_schema.is_some_and(|schema| schema.id == Some(schema_id)) {
        return Ok(fallback_source_schema);
    }
    Err(format!(
        "segment rowset schema id is missing from snapshot historical schemas: tablet_id={}, version={}, segment_path={}, rowset_version={}, schema_id={}",
        snapshot.tablet_id, snapshot.version, segment.path, segment.rowset_version, schema_id
    ))
}

fn projected_can_fill_missing_values(
    projected: &StarRocksNativeColumnPlan,
    source_column_missing: bool,
) -> bool {
    source_column_missing
        || projected.fallback_default_literal.is_some()
        || projected.fallback_is_nullable
}

fn build_missing_output_schema_column_plan(
    snapshot: &StarRocksTabletSnapshot,
    output_field: &Field,
    output_hint: &StarRocksOutputColumnHint,
) -> Result<(StarRocksNativeSchemaColumnPlan, u32, Option<String>, bool), String> {
    let output_name = output_field.name().trim();
    let schema_unique_id = output_hint.schema_unique_id.ok_or_else(|| {
        format!(
            "output column not found in tablet schema and missing unique_id hint: tablet_id={}, version={}, output_field={}",
            snapshot.tablet_id, snapshot.version, output_name
        )
    })?;
    if schema_unique_id == 0 {
        return Err(format!(
            "invalid output column unique_id hint (zero): tablet_id={}, version={}, output_field={}",
            snapshot.tablet_id, snapshot.version, output_name
        ));
    }

    let fallback_default_literal = output_hint.fallback_default_literal.clone();
    let fallback_is_nullable = output_field.is_nullable();
    if fallback_default_literal.is_none() && !fallback_is_nullable {
        return Err(format!(
            "output column not found in tablet schema and cannot be backfilled (non-nullable without default): tablet_id={}, version={}, output_field={}",
            snapshot.tablet_id, snapshot.version, output_name
        ));
    }

    let (schema_type, precision, scale) =
        synthetic_schema_type_from_output_arrow_type(output_field.data_type()).ok_or_else(|| {
            format!(
                "unsupported output field type for missing tablet schema column: tablet_id={}, version={}, output_field={}, output_type={:?}",
                snapshot.tablet_id,
                snapshot.version,
                output_name,
                output_field.data_type()
            )
        })?;
    let schema = StarRocksNativeSchemaColumnPlan {
        unique_id: Some(schema_unique_id),
        source_index: None,
        source_lookup_attempted: false,
        schema_type,
        is_nullable: fallback_is_nullable,
        is_key: false,
        aggregation: None,
        precision,
        scale,
        children: Vec::new(),
    };
    Ok((
        schema,
        schema_unique_id,
        fallback_default_literal,
        fallback_is_nullable,
    ))
}

fn synthetic_schema_type_from_output_arrow_type(
    data_type: &DataType,
) -> Option<(String, Option<u8>, Option<i8>)> {
    match data_type {
        DataType::Int8 => Some((STARROCKS_TYPE_TINYINT.to_string(), None, None)),
        DataType::Int16 => Some((STARROCKS_TYPE_SMALLINT.to_string(), None, None)),
        DataType::Int32 => Some((STARROCKS_TYPE_INT.to_string(), None, None)),
        DataType::Int64 => Some((STARROCKS_TYPE_BIGINT.to_string(), None, None)),
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            Some((STARROCKS_TYPE_LARGEINT.to_string(), None, None))
        }
        DataType::Float32 => Some((STARROCKS_TYPE_FLOAT.to_string(), None, None)),
        DataType::Float64 => Some((STARROCKS_TYPE_DOUBLE.to_string(), None, None)),
        DataType::Boolean => Some((STARROCKS_TYPE_BOOLEAN.to_string(), None, None)),
        DataType::Date32 => Some((STARROCKS_TYPE_DATE.to_string(), None, None)),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Some((STARROCKS_TYPE_DATETIME.to_string(), None, None))
        }
        DataType::Utf8 => Some((STARROCKS_TYPE_VARCHAR.to_string(), None, None)),
        DataType::Binary => Some((STARROCKS_TYPE_VARBINARY.to_string(), None, None)),
        DataType::Decimal128(precision, scale) => {
            let p = u8::try_from(*precision).ok().filter(|v| *v > 0)?;
            let schema_type = if p <= 9 {
                STARROCKS_TYPE_DECIMAL32
            } else if p <= 18 {
                STARROCKS_TYPE_DECIMAL64
            } else {
                STARROCKS_TYPE_DECIMAL128
            };
            Some((schema_type.to_string(), Some(p), Some(*scale)))
        }
        DataType::Decimal256(precision, scale) => {
            let p = u8::try_from(*precision).ok().filter(|v| *v > 0)?;
            Some((STARROCKS_TYPE_DECIMAL256.to_string(), Some(p), Some(*scale)))
        }
        _ => None,
    }
}

fn parse_table_model(
    keys_type: Option<i32>,
    tablet_id: i64,
    version: i64,
) -> Result<StarRocksTableModelPlan, String> {
    let raw_keys_type = keys_type.ok_or_else(|| {
        format!(
            "missing keys_type in tablet schema for native read plan: tablet_id={}, version={}",
            tablet_id, version
        )
    })?;
    let keys_type = KeysType::try_from(raw_keys_type).map_err(|_| {
        format!(
            "unknown keys_type in tablet schema for native read plan: tablet_id={}, version={}, keys_type={}",
            tablet_id, version, raw_keys_type
        )
    })?;
    let model = match keys_type {
        KeysType::DupKeys => StarRocksTableModelPlan::DupKeys,
        KeysType::AggKeys => StarRocksTableModelPlan::AggKeys,
        KeysType::UniqueKeys => StarRocksTableModelPlan::UniqueKeys,
        KeysType::PrimaryKeys => StarRocksTableModelPlan::PrimaryKeys,
    };
    Ok(model)
}

fn build_group_key_columns_plan(
    tablet_id: i64,
    version: i64,
    schema_columns: &[ColumnPb],
    table_model: StarRocksTableModelPlan,
) -> Result<Vec<StarRocksNativeGroupKeyColumnPlan>, String> {
    if !matches!(
        table_model,
        StarRocksTableModelPlan::AggKeys | StarRocksTableModelPlan::UniqueKeys
    ) {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for schema_col in schema_columns {
        if !schema_col.is_key.unwrap_or(false) {
            continue;
        }

        let output_name = schema_col
            .name
            .as_deref()
            .ok_or_else(|| {
                format!(
                    "group key column name is missing in tablet schema: tablet_id={}, version={}, unique_id={}",
                    tablet_id, version, schema_col.unique_id
                )
            })?
            .trim();
        if output_name.is_empty() {
            return Err(format!(
                "group key column name is empty in tablet schema: tablet_id={}, version={}, unique_id={}",
                tablet_id, version, schema_col.unique_id
            ));
        }
        let output_arrow_type =
            infer_group_key_arrow_type(tablet_id, version, output_name, schema_col)?;
        let schema = build_schema_column_plan(
            tablet_id,
            version,
            output_name,
            schema_col,
            None,
            None,
            false,
            &output_arrow_type,
        )?;
        let schema_unique_id = schema.unique_id.ok_or_else(|| {
            format!(
                "invalid group key unique_id in tablet schema: tablet_id={}, version={}, output_field={}, unique_id={}",
                tablet_id, version, output_name, schema_col.unique_id
            )
        })?;
        out.push(StarRocksNativeGroupKeyColumnPlan {
            output_name: output_name.to_string(),
            schema_unique_id,
            schema_type: schema.schema_type.clone(),
            schema,
        });
    }
    Ok(out)
}

fn infer_group_key_arrow_type(
    tablet_id: i64,
    version: i64,
    output_name: &str,
    schema_col: &ColumnPb,
) -> Result<DataType, String> {
    let schema_type = SupportedSchemaType::parse(&schema_col.r#type).ok_or_else(|| {
        format!(
            "unsupported schema type for rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, supported=[{}]",
            tablet_id,
            version,
            output_name,
            schema_col.r#type,
            SUPPORTED_SCHEMA_TYPES.join(",")
        )
    })?;

    match schema_type {
        SupportedSchemaType::TinyInt => Ok(DataType::Int8),
        SupportedSchemaType::SmallInt => Ok(DataType::Int16),
        SupportedSchemaType::Int => Ok(DataType::Int32),
        SupportedSchemaType::BigInt => Ok(DataType::Int64),
        SupportedSchemaType::LargeInt => {
            Ok(DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH))
        }
        SupportedSchemaType::Float => Ok(DataType::Float32),
        SupportedSchemaType::Double => Ok(DataType::Float64),
        SupportedSchemaType::Boolean => Ok(DataType::Boolean),
        SupportedSchemaType::Date => Ok(DataType::Date32),
        SupportedSchemaType::DateTime => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        SupportedSchemaType::Char | SupportedSchemaType::Varchar => Ok(DataType::Utf8),
        SupportedSchemaType::Binary | SupportedSchemaType::VarBinary => Ok(DataType::Binary),
        SupportedSchemaType::Decimal32
        | SupportedSchemaType::Decimal64
        | SupportedSchemaType::Decimal128
        | SupportedSchemaType::Decimal256 => {
            let (precision, scale) = parse_decimal_v3_schema_metadata(
                tablet_id,
                version,
                output_name,
                schema_col,
                schema_type,
            )?;
            if schema_type == SupportedSchemaType::Decimal256 {
                Ok(DataType::Decimal256(precision, scale))
            } else {
                Ok(DataType::Decimal128(precision, scale))
            }
        }
        SupportedSchemaType::Hll
        | SupportedSchemaType::Object
        | SupportedSchemaType::Percentile
        | SupportedSchemaType::Array
        | SupportedSchemaType::Map
        | SupportedSchemaType::Struct => Err(format!(
            "unsupported non-scalar group key type in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}",
            tablet_id,
            version,
            output_name,
            schema_type.as_str()
        )),
    }
}

fn build_primary_delvec_plan(
    table_model: StarRocksTableModelPlan,
    tablet_id: i64,
    version: i64,
    raw: &StarRocksDelvecMetaRaw,
) -> Result<Option<StarRocksPrimaryDelvecPlan>, String> {
    if table_model != StarRocksTableModelPlan::PrimaryKeys {
        return Ok(None);
    }

    let mut plan = StarRocksPrimaryDelvecPlan::default();
    for (v, rel_path) in &raw.version_to_file_rel_path {
        if *v < 0 {
            return Err(format!(
                "invalid primary delvec file version in read plan: tablet_id={}, version={}, delvec_version={}",
                tablet_id, version, v
            ));
        }
        if rel_path.trim().is_empty() {
            return Err(format!(
                "empty primary delvec file path in read plan: tablet_id={}, version={}, delvec_version={}",
                tablet_id, version, v
            ));
        }
        plan.version_to_file_rel_path.insert(*v, rel_path.clone());
    }

    for (segment_id, page) in &raw.segment_delvec_pages {
        if page.version < 0 {
            return Err(format!(
                "invalid primary delvec page version in read plan: tablet_id={}, version={}, segment_id={}, delvec_version={}",
                tablet_id, version, segment_id, page.version
            ));
        }
        if page.size > 0 && !plan.version_to_file_rel_path.contains_key(&page.version) {
            return Err(format!(
                "missing primary delvec file mapping in read plan: tablet_id={}, version={}, segment_id={}, delvec_version={}",
                tablet_id, version, segment_id, page.version
            ));
        }
        plan.segment_delvec_pages.insert(
            *segment_id,
            StarRocksDelvecPagePlan {
                version: page.version,
                offset: page.offset,
                size: page.size,
                crc32c: page.crc32c,
                crc32c_gen_version: page.crc32c_gen_version,
            },
        );
    }

    Ok(Some(plan))
}

fn normalize_column_name(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn positive_schema_column_unique_id(column: &ColumnPb) -> Option<u32> {
    u32::try_from(column.unique_id)
        .ok()
        .filter(|unique_id| *unique_id > 0)
}

fn source_schema_column_matches_output_arrow_type(
    source_col: &ColumnPb,
    output_arrow_type: &DataType,
) -> bool {
    let Some(source_type) = SupportedSchemaType::parse(&source_col.r#type) else {
        return false;
    };
    if !source_type.matches_arrow_type(output_arrow_type) {
        return false;
    }
    if !source_type.is_decimal_v3() {
        return true;
    }
    match (source_type, output_arrow_type) {
        (SupportedSchemaType::Decimal256, DataType::Decimal256(precision, scale)) => {
            source_col.precision == Some((*precision).into())
                && source_col.frac == Some((*scale).into())
        }
        (
            SupportedSchemaType::Decimal32
            | SupportedSchemaType::Decimal64
            | SupportedSchemaType::Decimal128,
            DataType::Decimal128(precision, scale),
        ) => {
            source_col.precision == Some((*precision).into())
                && source_col.frac == Some((*scale).into())
        }
        _ => false,
    }
}

fn align_struct_source_children<'a>(
    source_schema_col: Option<&'a ColumnPb>,
    current_children: &[ColumnPb],
    output_fields: &Fields,
) -> Vec<(Option<usize>, Option<&'a ColumnPb>, bool)> {
    let lookup_attempted = source_schema_col.is_some();
    let Some(source_col) = source_schema_col else {
        return vec![(None, None, false); current_children.len()];
    };

    let mut source_children_by_unique_id = HashMap::new();
    for (idx, child_col) in source_col.children_columns.iter().enumerate() {
        if let Some(unique_id) = positive_schema_column_unique_id(child_col) {
            source_children_by_unique_id.insert(unique_id, (idx, child_col));
        }
    }

    let mut matched_source_indexes = vec![false; source_col.children_columns.len()];
    let mut next_source_name_idx = 0usize;
    let mut aligned = Vec::with_capacity(current_children.len());

    for (field, child_schema_col) in output_fields.iter().zip(current_children.iter()) {
        let matched = if let Some(unique_id) = positive_schema_column_unique_id(child_schema_col) {
            source_children_by_unique_id
                .get(&unique_id)
                .copied()
                .filter(|(_, source_child)| {
                    source_schema_column_matches_output_arrow_type(source_child, field.data_type())
                })
        } else {
            let current_name = child_schema_col
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| field.name());
            let normalized_current_name = normalize_column_name(current_name);
            let mut found = None;
            for source_idx in next_source_name_idx..source_col.children_columns.len() {
                if matched_source_indexes[source_idx] {
                    continue;
                }
                let source_child = &source_col.children_columns[source_idx];
                let Some(source_name) = source_child
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|name| !name.is_empty())
                else {
                    continue;
                };
                if normalize_column_name(source_name) != normalized_current_name {
                    continue;
                }
                if !source_schema_column_matches_output_arrow_type(source_child, field.data_type())
                {
                    continue;
                }
                found = Some((source_idx, source_child));
                break;
            }
            found
        };

        if let Some((source_idx, source_child)) = matched {
            matched_source_indexes[source_idx] = true;
            next_source_name_idx = next_source_name_idx.max(source_idx.saturating_add(1));
            aligned.push((Some(source_idx), Some(source_child), lookup_attempted));
        } else {
            aligned.push((None, None, lookup_attempted));
        }
    }

    aligned
}

fn infer_missing_source_schema_type(output_arrow_type: &DataType) -> Option<&'static str> {
    match output_arrow_type {
        DataType::Boolean => Some("BOOLEAN"),
        DataType::Int8 => Some("TINYINT"),
        DataType::Int16 => Some("SMALLINT"),
        DataType::Int32 => Some("INT"),
        DataType::Int64 => Some("BIGINT"),
        DataType::Float32 => Some("FLOAT"),
        DataType::Float64 => Some("DOUBLE"),
        DataType::Utf8 => Some("VARCHAR"),
        DataType::Binary => Some("VARBINARY"),
        _ => None,
    }
}

fn parse_flat_json_projection(output_name: &str) -> Option<StarRocksFlatJsonProjectionPlan> {
    let output_name = output_name.trim();
    let first_dot = output_name.find('.')?;
    if first_dot == 0 || first_dot + 1 >= output_name.len() {
        return None;
    }
    let base_name = output_name[..first_dot].trim();
    if base_name.is_empty() {
        return None;
    }
    let path = output_name[first_dot + 1..]
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if path.is_empty() {
        return None;
    }
    Some(StarRocksFlatJsonProjectionPlan {
        base_column_name: base_name.to_string(),
        path,
    })
}

fn try_build_flat_json_projection<'a>(
    output_name: &str,
    by_name: &'a HashMap<String, &'a ColumnPb>,
) -> Option<(&'a ColumnPb, StarRocksFlatJsonProjectionPlan)> {
    let projection = parse_flat_json_projection(output_name)?;
    let schema_col = by_name
        .get(&normalize_column_name(&projection.base_column_name))
        .copied()?;
    if !schema_col
        .r#type
        .trim()
        .eq_ignore_ascii_case(STARROCKS_TYPE_JSON)
    {
        return None;
    }
    Some((schema_col, projection))
}

fn build_delete_predicates_plan(
    tablet_id: i64,
    version: i64,
    raw_predicates: &[StarRocksDeletePredicateRaw],
    by_name: &HashMap<String, &ColumnPb>,
) -> Result<Vec<StarRocksDeletePredicatePlan>, String> {
    let mut plans = Vec::with_capacity(raw_predicates.len());
    for raw in raw_predicates {
        if raw.version < 0 {
            return Err(format!(
                "invalid delete predicate version in tablet metadata: tablet_id={}, version={}, delete_version={}",
                tablet_id, version, raw.version
            ));
        }

        let mut terms = Vec::new();
        for sub in &raw.sub_predicates {
            let (column_name, op, values) = parse_delete_sub_predicate(sub).map_err(|e| {
                format!(
                    "parse delete sub predicate failed: tablet_id={}, version={}, delete_version={}, predicate={}, error={}",
                    tablet_id, version, raw.version, sub, e
                )
            })?;
            terms.push(build_delete_predicate_term_plan(
                tablet_id,
                version,
                raw.version,
                by_name,
                &column_name,
                op,
                values,
            )?);
        }
        for in_pred in &raw.in_predicates {
            if in_pred.values.is_empty() {
                return Err(format!(
                    "delete IN predicate has empty values: tablet_id={}, version={}, delete_version={}, column_name={}",
                    tablet_id, version, raw.version, in_pred.column_name
                ));
            }
            terms.push(build_delete_predicate_term_plan(
                tablet_id,
                version,
                raw.version,
                by_name,
                &in_pred.column_name,
                if in_pred.is_not_in {
                    StarRocksDeletePredicateOpPlan::NotIn
                } else {
                    StarRocksDeletePredicateOpPlan::In
                },
                in_pred.values.clone(),
            )?);
        }
        for binary_pred in &raw.binary_predicates {
            let op = parse_delete_binary_op(&binary_pred.op).ok_or_else(|| {
                format!(
                    "unsupported delete binary predicate op: tablet_id={}, version={}, delete_version={}, column_name={}, op={}",
                    tablet_id, version, raw.version, binary_pred.column_name, binary_pred.op
                )
            })?;
            terms.push(build_delete_predicate_term_plan(
                tablet_id,
                version,
                raw.version,
                by_name,
                &binary_pred.column_name,
                op,
                vec![binary_pred.value.clone()],
            )?);
        }
        for is_null_pred in &raw.is_null_predicates {
            terms.push(build_delete_predicate_term_plan(
                tablet_id,
                version,
                raw.version,
                by_name,
                &is_null_pred.column_name,
                if is_null_pred.is_not_null {
                    StarRocksDeletePredicateOpPlan::IsNotNull
                } else {
                    StarRocksDeletePredicateOpPlan::IsNull
                },
                Vec::new(),
            )?);
        }

        if terms.is_empty() {
            continue;
        }
        plans.push(StarRocksDeletePredicatePlan {
            version: raw.version,
            terms,
        });
    }

    plans.sort_by_key(|v| v.version);
    Ok(plans)
}

fn build_delete_predicate_term_plan(
    tablet_id: i64,
    version: i64,
    delete_version: i64,
    by_name: &HashMap<String, &ColumnPb>,
    column_name: &str,
    op: StarRocksDeletePredicateOpPlan,
    values: Vec<String>,
) -> Result<StarRocksDeletePredicateTermPlan, String> {
    let normalized_name = normalize_column_name(column_name);
    if normalized_name.is_empty() {
        return Err(format!(
            "delete predicate column name is empty: tablet_id={}, version={}, delete_version={}",
            tablet_id, version, delete_version
        ));
    }

    let column = by_name.get(&normalized_name).copied().ok_or_else(|| {
        format!(
            "delete predicate column not found in tablet schema: tablet_id={}, version={}, delete_version={}, column_name={}",
            tablet_id, version, delete_version, column_name
        )
    })?;
    let supported_type = SupportedSchemaType::parse(&column.r#type).ok_or_else(|| {
        format!(
            "unsupported delete predicate schema type in tablet schema: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}, supported=[{}]",
            tablet_id,
            version,
            delete_version,
            column_name,
            column.r#type,
            SUPPORTED_SCHEMA_TYPES.join(",")
        )
    })?;
    if matches!(
        supported_type,
        SupportedSchemaType::Array | SupportedSchemaType::Map | SupportedSchemaType::Struct
    ) {
        return Err(format!(
            "delete predicate does not support complex schema type: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}",
            tablet_id,
            version,
            delete_version,
            column_name,
            supported_type.as_str()
        ));
    }
    if !column.children_columns.is_empty() {
        return Err(format!(
            "delete predicate scalar schema column should not have children: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}, children={}",
            tablet_id,
            version,
            delete_version,
            column_name,
            supported_type.as_str(),
            column.children_columns.len()
        ));
    }

    match op {
        StarRocksDeletePredicateOpPlan::Eq
        | StarRocksDeletePredicateOpPlan::Ne
        | StarRocksDeletePredicateOpPlan::Lt
        | StarRocksDeletePredicateOpPlan::Le
        | StarRocksDeletePredicateOpPlan::Gt
        | StarRocksDeletePredicateOpPlan::Ge => {
            if values.len() != 1 {
                return Err(format!(
                    "delete predicate expects single value: tablet_id={}, version={}, delete_version={}, column_name={}, op={:?}, values={}",
                    tablet_id,
                    version,
                    delete_version,
                    column_name,
                    op,
                    values.len()
                ));
            }
        }
        StarRocksDeletePredicateOpPlan::In | StarRocksDeletePredicateOpPlan::NotIn => {
            if values.is_empty() {
                return Err(format!(
                    "delete IN predicate has empty values: tablet_id={}, version={}, delete_version={}, column_name={}, op={:?}",
                    tablet_id, version, delete_version, column_name, op
                ));
            }
        }
        StarRocksDeletePredicateOpPlan::IsNull | StarRocksDeletePredicateOpPlan::IsNotNull => {
            if !values.is_empty() {
                return Err(format!(
                    "delete is-null predicate should not have values: tablet_id={}, version={}, delete_version={}, column_name={}, op={:?}, values={}",
                    tablet_id,
                    version,
                    delete_version,
                    column_name,
                    op,
                    values.len()
                ));
            }
        }
    }

    let (precision, scale) = if supported_type.is_decimal_v3() {
        let precision = column.precision.ok_or_else(|| {
            format!(
                "missing decimal precision in delete predicate column: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}",
                tablet_id,
                version,
                delete_version,
                column_name,
                supported_type.as_str()
            )
        })?;
        let scale = column.frac.ok_or_else(|| {
            format!(
                "missing decimal scale(frac) in delete predicate column: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}",
                tablet_id,
                version,
                delete_version,
                column_name,
                supported_type.as_str()
            )
        })?;
        let precision_u8 = u8::try_from(precision).map_err(|_| {
            format!(
                "invalid decimal precision in delete predicate column: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}, precision={}",
                tablet_id,
                version,
                delete_version,
                column_name,
                supported_type.as_str(),
                precision
            )
        })?;
        let scale_i8 = i8::try_from(scale).map_err(|_| {
            format!(
                "invalid decimal scale(frac) in delete predicate column: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}, scale={}",
                tablet_id,
                version,
                delete_version,
                column_name,
                supported_type.as_str(),
                scale
            )
        })?;
        if precision_u8 == 0 || precision_u8 > supported_type.decimal_max_precision() {
            return Err(format!(
                "decimal precision exceeds schema type range in delete predicate column: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}, precision={}, max_precision={}",
                tablet_id,
                version,
                delete_version,
                column_name,
                supported_type.as_str(),
                precision_u8,
                supported_type.decimal_max_precision()
            ));
        }
        if scale_i8 < 0 || scale_i8 > precision_u8 as i8 {
            return Err(format!(
                "invalid decimal precision/scale in delete predicate column: tablet_id={}, version={}, delete_version={}, column_name={}, schema_type={}, precision={}, scale={}",
                tablet_id,
                version,
                delete_version,
                column_name,
                supported_type.as_str(),
                precision_u8,
                scale_i8
            ));
        }
        (Some(precision_u8), Some(scale_i8))
    } else {
        (None, None)
    };

    let schema_unique_id = u32::try_from(column.unique_id).map_err(|_| {
        format!(
            "invalid delete predicate column unique_id: tablet_id={}, version={}, delete_version={}, column_name={}, unique_id={}",
            tablet_id, version, delete_version, column_name, column.unique_id
        )
    })?;

    Ok(StarRocksDeletePredicateTermPlan {
        column_name: column_name.trim().to_string(),
        schema_unique_id,
        schema_type: supported_type.as_str().to_string(),
        precision,
        scale,
        op,
        values,
    })
}

fn parse_delete_sub_predicate(
    raw_predicate: &str,
) -> Result<(String, StarRocksDeletePredicateOpPlan, Vec<String>), String> {
    let text = raw_predicate.trim();
    if text.is_empty() {
        return Err("predicate is empty".to_string());
    }

    let upper = text.to_ascii_uppercase();
    if let Some(pos) = upper.find(" IS ") {
        let column = text[..pos].trim();
        let value = text[pos + 4..].trim();
        if column.is_empty() {
            return Err("column name is empty".to_string());
        }
        if value.eq_ignore_ascii_case("NULL") {
            return Ok((
                column.to_string(),
                StarRocksDeletePredicateOpPlan::IsNull,
                Vec::new(),
            ));
        }
        if value.eq_ignore_ascii_case("NOT NULL") {
            return Ok((
                column.to_string(),
                StarRocksDeletePredicateOpPlan::IsNotNull,
                Vec::new(),
            ));
        }
        return Err(format!("invalid IS predicate value: {}", value));
    }

    for (token, op) in [
        ("!=", StarRocksDeletePredicateOpPlan::Ne),
        (">=", StarRocksDeletePredicateOpPlan::Ge),
        ("<=", StarRocksDeletePredicateOpPlan::Le),
        ("<<", StarRocksDeletePredicateOpPlan::Lt),
        (">>", StarRocksDeletePredicateOpPlan::Gt),
        ("=", StarRocksDeletePredicateOpPlan::Eq),
    ] {
        if let Some(pos) = text.find(token) {
            let column = text[..pos].trim();
            let value = text[pos + token.len()..].trim();
            if column.is_empty() {
                return Err(format!("column name is empty around operator '{}'", token));
            }
            if value.is_empty() {
                return Err(format!(
                    "predicate value is empty: column={}, operator={}",
                    column, token
                ));
            }
            return Ok((column.to_string(), op, vec![value.to_string()]));
        }
    }

    Err(format!("unsupported predicate syntax: {}", text))
}

fn parse_delete_binary_op(op: &str) -> Option<StarRocksDeletePredicateOpPlan> {
    match op.trim() {
        "=" => Some(StarRocksDeletePredicateOpPlan::Eq),
        "!=" => Some(StarRocksDeletePredicateOpPlan::Ne),
        "<" | "<<" => Some(StarRocksDeletePredicateOpPlan::Lt),
        "<=" => Some(StarRocksDeletePredicateOpPlan::Le),
        ">" | ">>" => Some(StarRocksDeletePredicateOpPlan::Gt),
        ">=" => Some(StarRocksDeletePredicateOpPlan::Ge),
        _ => None,
    }
}

fn build_schema_column_plan(
    tablet_id: i64,
    version: i64,
    output_path: &str,
    schema_col: &ColumnPb,
    source_schema_col: Option<&ColumnPb>,
    source_index: Option<usize>,
    source_lookup_attempted: bool,
    output_arrow_type: &DataType,
) -> Result<StarRocksNativeSchemaColumnPlan, String> {
    let schema_type = SupportedSchemaType::parse(&schema_col.r#type).ok_or_else(|| {
        format!(
            "unsupported schema type for rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, supported=[{}]",
            tablet_id,
            version,
            output_path,
            schema_col.r#type,
            SUPPORTED_SCHEMA_TYPES.join(",")
        )
    })?;
    if !schema_type.matches_arrow_type(output_arrow_type) {
        return Err(format!(
            "output field type mismatch with tablet schema type in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, expected_arrow_type={}, actual_arrow_type={:?}",
            tablet_id,
            version,
            output_path,
            schema_type.as_str(),
            schema_type.expected_arrow_type(),
            output_arrow_type
        ));
    }

    let (precision, scale) = if schema_type.is_decimal_v3() {
        let (precision, scale) = validate_decimal_v3_schema_column(
            tablet_id,
            version,
            output_path,
            schema_col,
            schema_type,
            output_arrow_type,
        )?;
        (Some(precision), Some(scale))
    } else {
        (None, None)
    };

    let mut children = Vec::new();
    match schema_type {
        SupportedSchemaType::Array => {
            let DataType::List(item_field) = output_arrow_type else {
                return Err(format!(
                    "ARRAY output type mismatch with tablet schema type in rust native starrocks reader: tablet_id={}, version={}, output_field={}, actual_arrow_type={:?}",
                    tablet_id, version, output_path, output_arrow_type
                ));
            };
            if schema_col.children_columns.len() != 1 {
                return Err(format!(
                    "ARRAY schema child count mismatch in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_children={}, expected=1",
                    tablet_id,
                    version,
                    output_path,
                    schema_col.children_columns.len()
                ));
            }
            let child = build_schema_column_plan(
                tablet_id,
                version,
                &format!("{output_path}.item"),
                &schema_col.children_columns[0],
                source_schema_col.and_then(|col| col.children_columns.first()),
                source_schema_col.map(|_| 0),
                source_schema_col.is_some(),
                item_field.data_type(),
            )?;
            children.push(child);
        }
        SupportedSchemaType::Map => {
            let DataType::Map(entries_field, _) = output_arrow_type else {
                return Err(format!(
                    "MAP output type mismatch with tablet schema type in rust native starrocks reader: tablet_id={}, version={}, output_field={}, actual_arrow_type={:?}",
                    tablet_id, version, output_path, output_arrow_type
                ));
            };
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(format!(
                    "MAP entries type mismatch in rust native starrocks reader: tablet_id={}, version={}, output_field={}, entries_type={:?}, expected=Struct(key,value)",
                    tablet_id,
                    version,
                    output_path,
                    entries_field.data_type()
                ));
            };
            if entry_fields.len() != 2 {
                return Err(format!(
                    "MAP entries field count mismatch in rust native starrocks reader: tablet_id={}, version={}, output_field={}, entries_fields={}, expected=2",
                    tablet_id,
                    version,
                    output_path,
                    entry_fields.len()
                ));
            }
            if schema_col.children_columns.len() != 2 {
                return Err(format!(
                    "MAP schema child count mismatch in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_children={}, expected=2",
                    tablet_id,
                    version,
                    output_path,
                    schema_col.children_columns.len()
                ));
            }
            let key_child = build_schema_column_plan(
                tablet_id,
                version,
                &format!("{output_path}.key"),
                &schema_col.children_columns[0],
                source_schema_col.and_then(|col| col.children_columns.first()),
                source_schema_col.map(|_| 0),
                source_schema_col.is_some(),
                entry_fields[0].data_type(),
            )?;
            let value_child = build_schema_column_plan(
                tablet_id,
                version,
                &format!("{output_path}.value"),
                &schema_col.children_columns[1],
                source_schema_col.and_then(|col| col.children_columns.get(1)),
                source_schema_col.map(|_| 1),
                source_schema_col.is_some(),
                entry_fields[1].data_type(),
            )?;
            children.push(key_child);
            children.push(value_child);
        }
        SupportedSchemaType::Struct => {
            let DataType::Struct(struct_fields) = output_arrow_type else {
                return Err(format!(
                    "STRUCT output type mismatch with tablet schema type in rust native starrocks reader: tablet_id={}, version={}, output_field={}, actual_arrow_type={:?}",
                    tablet_id, version, output_path, output_arrow_type
                ));
            };
            if schema_col.children_columns.len() != struct_fields.len() {
                return Err(format!(
                    "STRUCT schema child count mismatch in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_children={}, output_fields={}",
                    tablet_id,
                    version,
                    output_path,
                    schema_col.children_columns.len(),
                    struct_fields.len()
                ));
            }
            let source_children = align_struct_source_children(
                source_schema_col,
                &schema_col.children_columns,
                struct_fields,
            );
            for (idx, (field, child_schema_col)) in struct_fields
                .iter()
                .zip(schema_col.children_columns.iter())
                .enumerate()
            {
                if let Some(schema_child_name) = child_schema_col.name.as_deref() {
                    if normalize_column_name(schema_child_name)
                        != normalize_column_name(field.name())
                    {
                        return Err(format!(
                            "STRUCT field name mismatch in rust native starrocks reader: tablet_id={}, version={}, output_field={}, field_index={}, schema_field_name={}, output_field_name={}",
                            tablet_id,
                            version,
                            output_path,
                            idx,
                            schema_child_name,
                            field.name()
                        ));
                    }
                }
                let (child_source_index, child_source_schema_col, child_source_lookup_attempted) =
                    source_children[idx];
                let child = build_schema_column_plan(
                    tablet_id,
                    version,
                    &format!("{output_path}.{}", field.name()),
                    child_schema_col,
                    child_source_schema_col,
                    child_source_index,
                    child_source_lookup_attempted,
                    field.data_type(),
                )?;
                children.push(child);
            }
        }
        _ => {
            if !schema_col.children_columns.is_empty() {
                return Err(format!(
                    "scalar schema column should not have children in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, schema_children={}",
                    tablet_id,
                    version,
                    output_path,
                    schema_type.as_str(),
                    schema_col.children_columns.len()
                ));
            }
        }
    }

    let unique_id = u32::try_from(schema_col.unique_id).ok();

    Ok(StarRocksNativeSchemaColumnPlan {
        unique_id,
        source_index,
        source_lookup_attempted,
        schema_type: schema_type.as_str().to_string(),
        is_nullable: schema_col.is_nullable.unwrap_or(true),
        is_key: schema_col.is_key.unwrap_or(false),
        aggregation: normalize_aggregation(schema_col.aggregation.as_deref()),
        precision,
        scale,
        children,
    })
}

fn normalize_aggregation(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_ascii_uppercase())
}

fn validate_decimal_v3_schema_column(
    tablet_id: i64,
    version: i64,
    output_name: &str,
    schema_col: &ColumnPb,
    schema_type: SupportedSchemaType,
    output_arrow_type: &DataType,
) -> Result<(u8, i8), String> {
    let (precision, scale) =
        parse_decimal_v3_schema_metadata(tablet_id, version, output_name, schema_col, schema_type)?;

    match (schema_type, output_arrow_type) {
        (SupportedSchemaType::Decimal256, DataType::Decimal256(output_precision, output_scale)) => {
            if *output_precision != precision || *output_scale != scale {
                return Err(format!(
                    "decimal output field type mismatch with tablet schema decimal metadata in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, schema_decimal=Decimal256({}, {}), output_arrow_type=Decimal256({}, {})",
                    tablet_id,
                    version,
                    output_name,
                    schema_type.as_str(),
                    precision,
                    scale,
                    output_precision,
                    output_scale
                ));
            }
        }
        (
            SupportedSchemaType::Decimal32
            | SupportedSchemaType::Decimal64
            | SupportedSchemaType::Decimal128,
            DataType::Decimal128(output_precision, output_scale),
        ) => {
            if *output_precision != precision || *output_scale != scale {
                return Err(format!(
                    "decimal output field type mismatch with tablet schema decimal metadata in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, schema_decimal=Decimal128({}, {}), output_arrow_type=Decimal128({}, {})",
                    tablet_id,
                    version,
                    output_name,
                    schema_type.as_str(),
                    precision,
                    scale,
                    output_precision,
                    output_scale
                ));
            }
        }
        (_, other) => {
            return Err(format!(
                "decimal output field type mismatch with tablet schema type in rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, expected_arrow_type={}, actual_arrow_type={:?}",
                tablet_id,
                version,
                output_name,
                schema_type.as_str(),
                schema_type.expected_arrow_type(),
                other
            ));
        }
    }
    Ok((precision, scale))
}

fn parse_decimal_v3_schema_metadata(
    tablet_id: i64,
    version: i64,
    output_name: &str,
    schema_col: &ColumnPb,
    schema_type: SupportedSchemaType,
) -> Result<(u8, i8), String> {
    let precision = schema_col.precision.ok_or_else(|| {
        format!(
            "missing decimal precision in tablet schema for rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}",
            tablet_id,
            version,
            output_name,
            schema_type.as_str()
        )
    })?;
    let scale = schema_col.frac.ok_or_else(|| {
        format!(
            "missing decimal scale(frac) in tablet schema for rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}",
            tablet_id,
            version,
            output_name,
            schema_type.as_str()
        )
    })?;

    let precision = u8::try_from(precision).map_err(|_| {
        format!(
            "invalid decimal precision in tablet schema for rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, precision={}",
            tablet_id,
            version,
            output_name,
            schema_type.as_str(),
            precision
        )
    })?;
    let scale = i8::try_from(scale).map_err(|_| {
        format!(
            "invalid decimal scale(frac) in tablet schema for rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, scale={}",
            tablet_id,
            version,
            output_name,
            schema_type.as_str(),
            scale
        )
    })?;
    if precision == 0 || precision > schema_type.decimal_max_precision() {
        return Err(format!(
            "decimal precision exceeds schema type range for rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, precision={}, max_precision={}",
            tablet_id,
            version,
            output_name,
            schema_type.as_str(),
            precision,
            schema_type.decimal_max_precision()
        ));
    }
    if scale < 0 || scale > precision as i8 {
        return Err(format!(
            "invalid decimal precision/scale in tablet schema for rust native starrocks reader: tablet_id={}, version={}, output_field={}, schema_type={}, precision={}, scale={}",
            tablet_id,
            version,
            output_name,
            schema_type.as_str(),
            precision,
            scale
        ));
    }
    Ok((precision, scale))
}

fn collect_unique_ids(columns: &[StarRocksSegmentColumnMeta]) -> Result<BTreeSet<u32>, String> {
    fn walk(node: &StarRocksSegmentColumnMeta, out: &mut BTreeSet<u32>) -> Result<(), String> {
        if let Some(unique_id) = node.unique_id {
            out.insert(unique_id);
        } else {
            return Err("segment footer column unique_id is missing".to_string());
        }
        for child in &node.children {
            walk(child, out)?;
        }
        Ok(())
    }

    let mut out = BTreeSet::new();
    for column in columns {
        walk(column, &mut out)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::starrocks::metadata::{
        StarRocksBinaryPredicateRaw, StarRocksDeletePredicateRaw, StarRocksInPredicateRaw,
        StarRocksIsNullPredicateRaw, StarRocksSegmentFile, StarRocksTabletSnapshot,
    };
    use crate::formats::starrocks::segment::{StarRocksSegmentColumnMeta, StarRocksSegmentFooter};
    use crate::service::grpc_client::proto::starrocks::{ColumnPb, KeysType, TabletSchemaPb};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn build_read_plan_success() {
        let snapshot = build_snapshot();
        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, true),
        ]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 2);
        assert_eq!(plan.segments.len(), 2);
        assert_eq!(plan.estimated_rows, 30);
    }

    #[test]
    fn parse_delete_sub_predicate_supports_binary_ops() {
        let (column, op, values) =
            parse_delete_sub_predicate("c1<<42").expect("parse lt delete predicate");
        assert_eq!(column, "c1");
        assert_eq!(op, StarRocksDeletePredicateOpPlan::Lt);
        assert_eq!(values, vec!["42".to_string()]);

        let (column, op, values) =
            parse_delete_sub_predicate("c2>=100").expect("parse ge delete predicate");
        assert_eq!(column, "c2");
        assert_eq!(op, StarRocksDeletePredicateOpPlan::Ge);
        assert_eq!(values, vec!["100".to_string()]);
    }

    #[test]
    fn parse_delete_sub_predicate_supports_is_null_ops() {
        let (column, op, values) =
            parse_delete_sub_predicate(" c3 IS NULL ").expect("parse is null delete predicate");
        assert_eq!(column, "c3");
        assert_eq!(op, StarRocksDeletePredicateOpPlan::IsNull);
        assert!(values.is_empty());

        let (column, op, values) = parse_delete_sub_predicate("c4 IS NOT NULL")
            .expect("parse is not null delete predicate");
        assert_eq!(column, "c4");
        assert_eq!(op, StarRocksDeletePredicateOpPlan::IsNotNull);
        assert!(values.is_empty());
    }

    #[test]
    fn build_read_plan_binds_delete_predicates_to_schema_unique_id() {
        let mut snapshot = build_snapshot_with_columns(vec![
            build_column(1, "c1", "BIGINT"),
            build_column(2, "c2", "VARCHAR"),
        ]);
        snapshot.delete_predicates = vec![StarRocksDeletePredicateRaw {
            version: 3,
            sub_predicates: Vec::new(),
            in_predicates: vec![StarRocksInPredicateRaw {
                column_name: "c1".to_string(),
                is_not_in: false,
                values: vec!["1".to_string(), "2".to_string()],
            }],
            binary_predicates: vec![StarRocksBinaryPredicateRaw {
                column_name: "c2".to_string(),
                op: "=".to_string(),
                value: "abc".to_string(),
            }],
            is_null_predicates: vec![StarRocksIsNullPredicateRaw {
                column_name: "c2".to_string(),
                is_not_null: true,
            }],
        }];

        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Utf8, true),
        ]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");

        assert_eq!(plan.delete_predicates.len(), 1);
        assert_eq!(plan.delete_predicates[0].version, 3);
        assert_eq!(plan.delete_predicates[0].terms.len(), 3);
        assert_eq!(plan.delete_predicates[0].terms[0].schema_unique_id, 1);
        assert_eq!(
            plan.delete_predicates[0].terms[0].op,
            StarRocksDeletePredicateOpPlan::In
        );
        assert_eq!(plan.delete_predicates[0].terms[1].schema_unique_id, 2);
        assert_eq!(
            plan.delete_predicates[0].terms[1].op,
            StarRocksDeletePredicateOpPlan::Eq
        );
        assert_eq!(
            plan.delete_predicates[0].terms[2].op,
            StarRocksDeletePredicateOpPlan::IsNotNull
        );
    }

    #[test]
    fn reject_delete_predicate_when_column_not_found_in_schema() {
        let mut snapshot = build_snapshot();
        snapshot.delete_predicates = vec![StarRocksDeletePredicateRaw {
            version: 1,
            sub_predicates: Vec::new(),
            in_predicates: vec![StarRocksInPredicateRaw {
                column_name: "missing_col".to_string(),
                is_not_in: false,
                values: vec!["1".to_string()],
            }],
            binary_predicates: Vec::new(),
            is_null_predicates: Vec::new(),
        }];
        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, true),
        ]));

        let err = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect_err("missing delete predicate column should fail");
        assert!(
            err.contains("delete predicate column not found in tablet schema"),
            "err={err}"
        );
    }

    #[test]
    fn reject_missing_output_column_in_schema() {
        let snapshot = build_snapshot();
        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "missing_col",
            DataType::Int64,
            true,
        )]));
        let err = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect_err("should reject unknown output column");
        assert!(err.contains("output column not found"), "err={}", err);
    }

    #[test]
    fn build_read_plan_supports_flat_json_rewritten_output_column() {
        let snapshot = build_snapshot_with_columns(vec![
            build_column(1, "id", "BIGINT"),
            build_column(2, "j", "JSON"),
        ]);
        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("j.a", DataType::Int64, true),
        ]));

        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 2);
        assert!(plan.projected_columns[0].flat_json_projection.is_none());
        let flat = plan.projected_columns[1]
            .flat_json_projection
            .as_ref()
            .expect("flat json projection should be present");
        assert_eq!(flat.base_column_name, "j");
        assert_eq!(flat.path, vec!["a".to_string()]);
        assert_eq!(plan.projected_columns[1].schema_unique_id, 2);
        assert!(!plan.projected_columns[1].source_column_missing);
    }

    #[test]
    fn build_read_plan_supports_missing_flat_json_base_column() {
        let snapshot = build_snapshot_with_columns(vec![
            build_column(1, "id", "BIGINT"),
            build_column(2, "j", "JSON"),
        ]);
        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "j3.key3",
            DataType::Float64,
            true,
        )]));

        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 1);
        let projected = &plan.projected_columns[0];
        assert!(projected.source_column_missing);
        assert_eq!(projected.schema_type, "DOUBLE");
        let flat = projected
            .flat_json_projection
            .as_ref()
            .expect("flat json projection should be present");
        assert_eq!(flat.base_column_name, "j3");
        assert_eq!(flat.path, vec!["key3".to_string()]);
    }

    #[test]
    fn build_read_plan_supports_missing_json_output_column() {
        let snapshot = build_snapshot_with_columns(vec![
            build_column(1, "id", "BIGINT"),
            build_column(2, "j", "JSON"),
        ]);
        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![Field::new("j3", DataType::Binary, true)]));

        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 1);
        let projected = &plan.projected_columns[0];
        assert!(projected.source_column_missing);
        assert!(projected.flat_json_projection.is_none());
        assert_eq!(projected.schema_type, "VARBINARY");
    }

    #[test]
    fn reject_segment_footer_missing_projected_unique_id() {
        let snapshot = build_snapshot();
        let footers = vec![build_footer(10, &[1]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, false),
        ]));
        let err = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect_err("should reject footer unique id mismatch");
        assert!(err.contains("cannot be backfilled"), "err={}", err);
    }

    #[test]
    fn allow_segment_footer_missing_projected_unique_id_when_nullable() {
        let snapshot = build_snapshot();
        let footers = vec![build_footer(10, &[1]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, true),
        ]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("nullable column should allow footer fallback");
        assert_eq!(plan.projected_columns.len(), 2);
        assert!(plan.projected_columns[1].fallback_is_nullable);
    }

    #[test]
    fn reject_unsupported_projected_column_type() {
        let mut snapshot = build_snapshot();
        snapshot.tablet_schema.column[1].r#type = "VARIANT".to_string();
        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, true),
        ]));
        let err = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect_err("unsupported schema type should be rejected");
        assert!(err.contains("unsupported schema type"), "err={}", err);
    }

    #[test]
    fn reject_output_field_arrow_type_mismatch() {
        let snapshot = build_snapshot_with_columns(vec![
            build_column(1, "c1", "BIGINT"),
            build_column(2, "c2", "INT"),
        ]);
        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, true),
        ]));
        let err = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect_err("schema/output type mismatch should be rejected");
        assert!(
            err.contains("output field type mismatch with tablet schema type"),
            "err={}",
            err
        );
    }

    #[test]
    fn build_read_plan_supports_basic_and_temporal_scalar_types() {
        let snapshot = build_snapshot_with_columns(vec![
            build_column(1, "c_tiny", "TINYINT"),
            build_column(2, "c_small", "SMALLINT"),
            build_column(3, "c_int", "INT"),
            build_column(4, "c_big", "BIGINT"),
            build_column(5, "c_float", "FLOAT"),
            build_column(6, "c_double", "DOUBLE"),
            build_column(7, "c_bool", "BOOLEAN"),
            build_column(8, "c_date", "DATE_V2"),
            build_column(9, "c_datetime", "DATETIME_V2"),
        ]);
        let footers = vec![
            build_footer(10, &[1, 2, 3, 4, 5, 6, 7, 8, 9]),
            build_footer(20, &[1, 2, 3, 4, 5, 6, 7, 8, 9]),
        ];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c_tiny", DataType::Int8, true),
            Field::new("c_small", DataType::Int16, true),
            Field::new("c_int", DataType::Int32, true),
            Field::new("c_big", DataType::Int64, true),
            Field::new("c_float", DataType::Float32, true),
            Field::new("c_double", DataType::Float64, true),
            Field::new("c_bool", DataType::Boolean, true),
            Field::new("c_date", DataType::Date32, true),
            Field::new(
                "c_datetime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 9);
        assert_eq!(plan.segments.len(), 2);
        assert_eq!(plan.estimated_rows, 30);
    }

    #[test]
    fn build_read_plan_supports_timestamp_schema_type() {
        let snapshot = build_snapshot_with_columns(vec![build_column(1, "c_ts", "TIMESTAMP")]);
        let footers = vec![build_footer(10, &[1]), build_footer(20, &[1])];
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "c_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 1);
        assert_eq!(plan.segments.len(), 2);
        assert_eq!(plan.estimated_rows, 30);
    }

    #[test]
    fn build_read_plan_supports_unique_keys_model() {
        let mut snapshot = build_snapshot_with_columns(vec![
            build_column(1, "c1", "BIGINT"),
            build_column(2, "c2", "BIGINT"),
        ]);
        snapshot.tablet_schema.keys_type = Some(KeysType::UniqueKeys as i32);
        snapshot.tablet_schema.column[0].is_key = Some(true);
        snapshot.tablet_schema.column[1].is_key = Some(false);

        let footers = vec![build_footer(10, &[1, 2]), build_footer(20, &[1, 2])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int64, true),
            Field::new("c2", DataType::Int64, true),
        ]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.table_model, StarRocksTableModelPlan::UniqueKeys);
        assert_eq!(plan.group_key_columns.len(), 1);
        assert_eq!(plan.group_key_columns[0].output_name, "c1");
        assert_eq!(plan.group_key_columns[0].schema_unique_id, 1);
    }

    #[test]
    fn build_read_plan_supports_text_and_binary_schema_types() {
        let snapshot = build_snapshot_with_columns(vec![
            build_column(1, "c_char", "CHAR"),
            build_column(2, "c_varchar", "VARCHAR"),
            build_column(3, "c_string", "STRING"),
            build_column(4, "c_binary", "BINARY"),
            build_column(5, "c_varbinary", "VARBINARY"),
        ]);
        let footers = vec![
            build_footer(10, &[1, 2, 3, 4, 5]),
            build_footer(20, &[1, 2, 3, 4, 5]),
        ];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c_char", DataType::Utf8, true),
            Field::new("c_varchar", DataType::Utf8, true),
            Field::new("c_string", DataType::Utf8, true),
            Field::new("c_binary", DataType::Binary, true),
            Field::new("c_varbinary", DataType::Binary, true),
        ]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 5);
        assert_eq!(plan.segments.len(), 2);
        assert_eq!(plan.estimated_rows, 30);
    }

    #[test]
    fn build_read_plan_supports_decimal_v3_schema_types() {
        let snapshot = build_snapshot_with_columns(vec![
            build_decimal_column(1, "c_d32", "DECIMAL32", 9, 2),
            build_decimal_column(2, "c_d64", "DECIMAL64", 18, 4),
            build_decimal_column(3, "c_d128", "DECIMAL128", 38, 10),
        ]);
        let footers = vec![build_footer(10, &[1, 2, 3]), build_footer(20, &[1, 2, 3])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c_d32", DataType::Decimal128(9, 2), true),
            Field::new("c_d64", DataType::Decimal128(18, 4), true),
            Field::new("c_d128", DataType::Decimal128(38, 10), true),
        ]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 3);
        assert_eq!(plan.segments.len(), 2);
        assert_eq!(plan.estimated_rows, 30);
    }

    #[test]
    fn reject_decimal_v2_schema_type() {
        let snapshot =
            build_snapshot_with_columns(vec![build_decimal_column(1, "c_dec", "DECIMALV2", 27, 9)]);
        let footers = vec![build_footer(10, &[1]), build_footer(20, &[1])];
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "c_dec",
            DataType::Decimal128(27, 9),
            true,
        )]));
        let err = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect_err("DECIMALV2 should be rejected");
        assert!(err.contains("unsupported schema type"), "err={}", err);
    }

    #[test]
    fn reject_decimal_precision_scale_mismatch_with_schema_metadata() {
        let snapshot =
            build_snapshot_with_columns(vec![build_decimal_column(1, "c_dec", "DECIMAL64", 18, 6)]);
        let footers = vec![build_footer(10, &[1]), build_footer(20, &[1])];
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "c_dec",
            DataType::Decimal128(18, 4),
            true,
        )]));
        let err = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect_err("decimal precision/scale mismatch should be rejected");
        assert!(
            err.contains("decimal output field type mismatch with tablet schema decimal metadata"),
            "err={}",
            err
        );
    }

    #[test]
    fn build_read_plan_supports_array_map_struct_schema_types() {
        let snapshot = build_snapshot_with_columns(vec![
            build_array_column(1, "c_arr", build_column(11, "item", "BIGINT")),
            build_map_column(
                2,
                "c_map",
                build_column(21, "key", "INT"),
                build_column(22, "value", "VARCHAR"),
            ),
            build_struct_column(
                3,
                "c_struct",
                vec![
                    build_column(31, "f1", "DATE"),
                    build_decimal_column(32, "f2", "DECIMAL64", 18, 2),
                ],
            ),
        ]);
        let footers = vec![build_footer(10, &[1, 2, 3]), build_footer(20, &[1, 2, 3])];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new(
                "c_arr",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new(
                "c_map",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Int32, false),
                                Field::new("value", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new(
                "c_struct",
                DataType::Struct(
                    vec![
                        Field::new("f1", DataType::Date32, true),
                        Field::new("f2", DataType::Decimal128(18, 2), true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));
        let plan = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 3);
        assert_eq!(plan.segments.len(), 2);
        assert_eq!(plan.estimated_rows, 30);
    }

    #[test]
    fn reject_struct_schema_child_count_mismatch() {
        let snapshot = build_snapshot_with_columns(vec![build_struct_column(
            1,
            "c_struct",
            vec![build_column(11, "f1", "INT"), build_column(12, "f2", "INT")],
        )]);
        let footers = vec![build_footer(10, &[1]), build_footer(20, &[1])];
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "c_struct",
            DataType::Struct(vec![Field::new("f1", DataType::Int32, true)].into()),
            true,
        )]));
        let err = build_native_read_plan(&snapshot, &footers, &output_schema, None)
            .expect_err("struct child mismatch should fail");
        assert!(
            err.contains("STRUCT schema child count mismatch"),
            "err={}",
            err
        );
    }

    #[test]
    fn allow_missing_output_column_with_hint_default() {
        let snapshot = build_snapshot_with_columns(vec![build_column(1, "c1", "BIGINT")]);
        let footers = vec![build_footer(10, &[1]), build_footer(20, &[1])];
        let output_schema = Arc::new(Schema::new(vec![Field::new("c2", DataType::Int64, false)]));
        let output_hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(2),
            fallback_default_literal: Some("7".to_string()),
        }];
        let plan = build_native_read_plan_with_output_hints(
            &snapshot,
            &footers,
            &output_schema,
            &output_hints,
            None,
        )
        .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 1);
        assert_eq!(plan.projected_columns[0].schema_unique_id, 2);
        assert_eq!(
            plan.projected_columns[0]
                .fallback_default_literal
                .as_deref(),
            Some("7")
        );
    }

    #[test]
    fn prefer_output_field_unique_id_hint_over_name_match() {
        let snapshot = build_snapshot_with_columns(vec![
            build_column(1, "k1", "BIGINT"),
            build_column(2, "v0", "INT"),
        ]);
        let footers = vec![build_footer(10, &[1]), build_footer(20, &[1])];
        let output_schema = Arc::new(Schema::new(vec![Field::new("v0", DataType::Utf8, true)]));
        let output_hints = vec![StarRocksOutputColumnHint {
            schema_unique_id: Some(4),
            fallback_default_literal: None,
        }];
        let plan = build_native_read_plan_with_output_hints(
            &snapshot,
            &footers,
            &output_schema,
            &output_hints,
            None,
        )
        .expect("build read plan");
        assert_eq!(plan.projected_columns.len(), 1);
        assert_eq!(plan.projected_columns[0].schema_unique_id, 4);
        assert_eq!(plan.projected_columns[0].schema_type, "VARCHAR");
    }

    #[test]
    fn align_array_struct_children_monotonically_when_nested_unique_ids_are_missing() {
        let snapshot = build_snapshot_with_columns(vec![build_array_column(
            1,
            "c1",
            build_struct_column(
                -1,
                "element",
                vec![
                    build_column(-1, "v2", "INT"),
                    build_column(-1, "val1", "INT"),
                ],
            ),
        )]);
        let source_schema = TabletSchemaPb {
            column: vec![build_array_column(
                1,
                "c1",
                build_struct_column(
                    -1,
                    "element",
                    vec![
                        build_column(-1, "v1", "INT"),
                        build_column(-1, "v2", "INT"),
                        build_column(-1, "val1", "INT"),
                    ],
                ),
            )],
            ..Default::default()
        };
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "c1",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Field::new("v2", DataType::Int32, true),
                        Field::new("val1", DataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        )]));

        let plan = build_native_read_plan(
            &snapshot,
            &[build_footer(10, &[1]), build_footer(20, &[1])],
            &output_schema,
            Some(&source_schema),
        )
        .expect("build read plan");
        let element_schema = &plan.projected_columns[0].schema.children[0];
        assert_eq!(element_schema.children[0].source_index, Some(1));
        assert_eq!(element_schema.children[1].source_index, Some(2));
        assert!(element_schema.children[1].source_lookup_attempted);
    }

    #[test]
    fn do_not_reuse_same_name_struct_field_after_drop_and_readd() {
        let snapshot = build_snapshot_with_columns(vec![build_struct_column(
            1,
            "c1",
            vec![
                build_column(-1, "v2", "INT"),
                build_column(-1, "v1", "INT"),
                build_column(-1, "val1", "INT"),
            ],
        )]);
        let source_schema = TabletSchemaPb {
            column: vec![build_struct_column(
                1,
                "c1",
                vec![
                    build_column(-1, "v1", "INT"),
                    build_column(-1, "v2", "INT"),
                    build_column(-1, "val1", "INT"),
                ],
            )],
            ..Default::default()
        };
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "c1",
            DataType::Struct(
                vec![
                    Field::new("v2", DataType::Int32, true),
                    Field::new("v1", DataType::Int32, true),
                    Field::new("val1", DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        )]));

        let plan = build_native_read_plan(
            &snapshot,
            &[build_footer(10, &[1]), build_footer(20, &[1])],
            &output_schema,
            Some(&source_schema),
        )
        .expect("build read plan");
        let struct_schema = &plan.projected_columns[0].schema;
        assert_eq!(struct_schema.children[0].source_index, Some(1));
        assert_eq!(struct_schema.children[1].source_index, None);
        assert!(struct_schema.children[1].source_lookup_attempted);
        assert_eq!(struct_schema.children[2].source_index, Some(2));
    }

    #[test]
    fn do_not_name_fallback_across_nested_type_change() {
        let snapshot = build_snapshot_with_columns(vec![build_struct_column(
            1,
            "c1",
            vec![
                build_column(-1, "v2_1", "INT"),
                build_column(-1, "v2_2", "DATE"),
            ],
        )]);
        let source_schema = TabletSchemaPb {
            column: vec![build_struct_column(
                1,
                "c1",
                vec![
                    build_column(-1, "v2_1", "INT"),
                    build_column(-1, "v2_2", "VARCHAR"),
                ],
            )],
            ..Default::default()
        };
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "c1",
            DataType::Struct(
                vec![
                    Field::new("v2_1", DataType::Int32, true),
                    Field::new("v2_2", DataType::Date32, true),
                ]
                .into(),
            ),
            true,
        )]));

        let plan = build_native_read_plan(
            &snapshot,
            &[build_footer(10, &[1]), build_footer(20, &[1])],
            &output_schema,
            Some(&source_schema),
        )
        .expect("build read plan");
        let struct_schema = &plan.projected_columns[0].schema;
        assert_eq!(struct_schema.children[0].source_index, Some(0));
        assert_eq!(struct_schema.children[1].source_index, None);
        assert!(struct_schema.children[1].source_lookup_attempted);
    }

    #[test]
    fn segment_projected_schema_uses_rowset_historical_schema() {
        let current_struct = build_struct_column(
            1,
            "c1",
            vec![
                build_column(-1, "v2", "INT"),
                build_column(-1, "val1", "INT"),
            ],
        );
        let old_struct = build_struct_column(
            1,
            "c1",
            vec![
                build_column(-1, "v1", "INT"),
                build_column(-1, "v2", "INT"),
                build_column(-1, "val1", "INT"),
            ],
        );
        let mut snapshot = build_snapshot_with_columns(vec![current_struct]);
        snapshot.historical_schemas.insert(
            900,
            TabletSchemaPb {
                id: Some(900),
                column: vec![old_struct],
                ..Default::default()
            },
        );
        snapshot.segment_files[0].schema_id = Some(900);
        snapshot.segment_files[1].schema_id = snapshot.tablet_schema.id;
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "c1",
            DataType::Struct(
                vec![
                    Field::new("v2", DataType::Int32, true),
                    Field::new("val1", DataType::Int32, true),
                ]
                .into(),
            ),
            true,
        )]));

        let plan = build_native_read_plan(
            &snapshot,
            &[build_footer(10, &[1]), build_footer(20, &[1])],
            &output_schema,
            None,
        )
        .expect("build read plan");
        let old_segment_schema = &plan.segments[0].projected_schemas[0];
        assert_eq!(old_segment_schema.children[0].source_index, Some(1));
        assert_eq!(old_segment_schema.children[1].source_index, Some(2));
        let current_segment_schema = &plan.segments[1].projected_schemas[0];
        assert_eq!(current_segment_schema.children[0].source_index, Some(0));
        assert_eq!(current_segment_schema.children[1].source_index, Some(1));
    }

    fn build_snapshot() -> StarRocksTabletSnapshot {
        build_snapshot_with_columns(vec![
            build_column(1, "c1", "BIGINT"),
            build_column(2, "c2", "BIGINT"),
        ])
    }

    fn build_snapshot_with_columns(columns: Vec<ColumnPb>) -> StarRocksTabletSnapshot {
        let tablet_schema = TabletSchemaPb {
            id: Some(1000),
            keys_type: Some(KeysType::DupKeys as i32),
            column: columns,
            ..Default::default()
        };
        StarRocksTabletSnapshot {
            tablet_id: 10,
            version: 20,
            metadata_path: "meta/path".to_string(),
            tablet_schema: tablet_schema.clone(),
            historical_schemas: std::collections::BTreeMap::from([(1000, tablet_schema)]),
            total_num_rows: 30,
            rowset_count: 1,
            segment_files: vec![
                StarRocksSegmentFile {
                    name: "s1.dat".to_string(),
                    relative_path: "data/s1.dat".to_string(),
                    path: "/tmp/data/s1.dat".to_string(),
                    rowset_version: 10,
                    schema_id: None,
                    segment_id: Some(1),
                    bundle_file_offset: Some(0),
                    segment_size: Some(100),
                },
                StarRocksSegmentFile {
                    name: "s2.dat".to_string(),
                    relative_path: "data/s2.dat".to_string(),
                    path: "/tmp/data/s2.dat".to_string(),
                    rowset_version: 20,
                    schema_id: None,
                    segment_id: Some(2),
                    bundle_file_offset: Some(100),
                    segment_size: Some(200),
                },
            ],
            delete_predicates: Vec::new(),
            delvec_meta: Default::default(),
        }
    }

    fn build_column(unique_id: i32, name: &str, schema_type: &str) -> ColumnPb {
        ColumnPb {
            unique_id,
            name: Some(name.to_string()),
            r#type: schema_type.to_string(),
            ..Default::default()
        }
    }

    fn build_decimal_column(
        unique_id: i32,
        name: &str,
        schema_type: &str,
        precision: i32,
        scale: i32,
    ) -> ColumnPb {
        ColumnPb {
            unique_id,
            name: Some(name.to_string()),
            r#type: schema_type.to_string(),
            precision: Some(precision),
            frac: Some(scale),
            ..Default::default()
        }
    }

    fn build_array_column(unique_id: i32, name: &str, item: ColumnPb) -> ColumnPb {
        ColumnPb {
            unique_id,
            name: Some(name.to_string()),
            r#type: STARROCKS_TYPE_ARRAY.to_string(),
            children_columns: vec![item],
            ..Default::default()
        }
    }

    fn build_map_column(unique_id: i32, name: &str, key: ColumnPb, value: ColumnPb) -> ColumnPb {
        ColumnPb {
            unique_id,
            name: Some(name.to_string()),
            r#type: STARROCKS_TYPE_MAP.to_string(),
            children_columns: vec![key, value],
            ..Default::default()
        }
    }

    fn build_struct_column(unique_id: i32, name: &str, fields: Vec<ColumnPb>) -> ColumnPb {
        ColumnPb {
            unique_id,
            name: Some(name.to_string()),
            r#type: STARROCKS_TYPE_STRUCT.to_string(),
            children_columns: fields,
            ..Default::default()
        }
    }

    fn build_footer(num_rows: u32, unique_ids: &[u32]) -> StarRocksSegmentFooter {
        StarRocksSegmentFooter {
            footer_size: 64,
            footer_checksum: 100,
            version: 1,
            num_rows: Some(num_rows),
            columns: unique_ids
                .iter()
                .map(|unique_id| StarRocksSegmentColumnMeta {
                    column_id: Some(*unique_id),
                    unique_id: Some(*unique_id),
                    logical_type: Some(3),
                    encoding: Some(2),
                    compression: Some(7),
                    is_nullable: Some(true),
                    ..Default::default()
                })
                .collect(),
        }
    }
}
