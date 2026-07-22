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

use std::collections::{HashMap, HashSet};

use crate::connector::starrocks::table::catalog::StarRocksTableRuntime;
use crate::connector::starrocks::table::ddl::{
    aggregation_string_to_column_aggregation, parse_keys_type, parse_starrocks_logical_type,
    to_keys_type,
};
use crate::sql::parser::ast::{ColumnAggregation, TableColumnDef, TableKeyDesc, TableKeyKind};
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::schema::SqlType;

pub(crate) fn request_schema_from_runtime(
    runtime: &StarRocksTableRuntime,
) -> Result<crate::thrift::agent_service::TTabletSchema, String> {
    // Build a name -> aggregation-string lookup from the tablet schema PB so
    // we can restore the ColumnAggregation modifier (BITMAP_UNION, HLL_UNION,
    // SUM, ...) that StoredStarRocksColumn does not carry.
    let pb_agg_by_name: HashMap<String, Option<String>> = runtime
        .tablet_schema
        .column
        .iter()
        .filter_map(|col| {
            let name = col.name.as_deref()?;
            let key = normalize_identifier(name).ok()?;
            Some((key, col.aggregation.clone()))
        })
        .collect();

    let columns = runtime
        .columns
        .iter()
        .map(|column| {
            let normalized = normalize_identifier(&column.column_name)?;
            let aggregation = match pb_agg_by_name.get(&normalized) {
                Some(agg_opt) => {
                    aggregation_string_to_column_aggregation(agg_opt.as_deref().unwrap_or("NONE"))?
                }
                None => None,
            };
            Ok(TableColumnDef {
                name: column.column_name.clone(),
                data_type: parse_starrocks_logical_type(&column.logical_type)?,
                nullable: column.nullable,
                aggregation,
                default: None,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let key_columns = runtime
        .columns
        .iter()
        .filter(|column| column.is_key)
        .map(|column| column.column_name.clone())
        .collect::<Vec<_>>();
    build_tablet_schema(
        &columns,
        &TableKeyDesc {
            kind: parse_keys_type(&runtime.table.keys_type)?,
            columns: key_columns,
        },
        runtime.table.current_schema_id,
    )
}

pub(crate) fn build_create_tablet_request(
    tablet_id: i64,
    table_id: i64,
    partition_id: i64,
    tablet_schema: crate::thrift::agent_service::TTabletSchema,
) -> crate::thrift::agent_service::TCreateTabletReq {
    crate::thrift::agent_service::TCreateTabletReq {
        tablet_id,
        tablet_schema,
        version: None,
        version_hash: None,
        storage_medium: None,
        in_restore_mode: None,
        base_tablet_id: None,
        base_schema_hash: None,
        table_id: Some(table_id),
        partition_id: Some(partition_id),
        allocation_term: None,
        is_eco_mode: None,
        storage_format: None,
        tablet_type: None,
        enable_persistent_index: Some(false),
        compression_type: Some(crate::thrift::types::TCompressionType::LZ4_FRAME),
        binlog_config: None,
        persistent_index_type: None,
        primary_index_cache_expire_sec: None,
        create_schema_file: Some(false),
        compression_level: None,
        enable_tablet_creation_optimization: Some(false),
        timeout_ms: None,
        gtid: Some(0),
        flat_json_config: None,
        compaction_strategy: None,
    }
}

pub(crate) fn build_tablet_schema(
    columns: &[TableColumnDef],
    key_desc: &TableKeyDesc,
    schema_id: i64,
) -> Result<crate::thrift::agent_service::TTabletSchema, String> {
    let key_columns = key_desc
        .columns
        .iter()
        .map(|column| normalize_identifier(column))
        .collect::<Result<Vec<_>, _>>()?;
    let mut key_column_set = HashSet::with_capacity(key_columns.len());
    for key_column in &key_columns {
        if !key_column_set.insert(key_column.clone()) {
            return Err(format!(
                "duplicate key column `{key_column}` in StarRocks standalone CREATE TABLE"
            ));
        }
    }

    let mut key_indices = Vec::with_capacity(key_columns.len());
    let mut thrift_columns = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let normalized = normalize_identifier(&column.name)?;
        let is_key = key_column_set.contains(&normalized);
        if is_key {
            key_indices.push(idx as i32);
        }
        let complex = is_complex_type(&column.data_type);
        if complex && is_key {
            return Err(format!(
                "StarRocks standalone CREATE TABLE key column `{normalized}` cannot be a complex type ({:?})",
                column.data_type
            ));
        }
        let (column_type, type_desc) = if complex {
            (None, Some(sql_type_to_ttype_desc(&column.data_type)?))
        } else {
            (Some(sql_type_to_tcolumn_type(&column.data_type)?), None)
        };
        let aggregation_type = if is_key {
            if column.aggregation.is_some() {
                return Err(format!(
                    "StarRocks standalone CREATE TABLE key column `{normalized}` cannot have aggregation"
                ));
            }
            None
        } else {
            match key_desc.kind {
                TableKeyKind::Duplicate => None,
                TableKeyKind::Unique | TableKeyKind::Primary => {
                    Some(crate::thrift::types::TAggregationType::REPLACE)
                }
                TableKeyKind::Aggregate => {
                    let aggregation = column.aggregation.ok_or_else(|| {
                        format!(
                            "StarRocks standalone CREATE TABLE aggregate value column `{normalized}` requires aggregation"
                        )
                    })?;
                    Some(column_aggregation_to_thrift(aggregation))
                }
            }
        };
        thrift_columns.push(crate::thrift::descriptors::TColumn {
            column_name: normalized,
            column_type,
            aggregation_type,
            is_key: Some(is_key),
            is_allow_null: Some(column.nullable),
            default_value: None,
            default_expr: None,
            is_bloom_filter_column: Some(false),
            define_expr: None,
            is_auto_increment: Some(false),
            col_unique_id: Some(idx as i32),
            has_bitmap_index: Some(false),
            agg_state_desc: None,
            index_len: index_length_for_sql_type(&column.data_type),
            type_desc,
        });
    }
    if key_columns.is_empty() {
        return Err(
            "StarRocks standalone CREATE TABLE requires at least one key column".to_string(),
        );
    }
    if key_indices.len() != key_columns.len() {
        let missing = key_columns
            .into_iter()
            .filter(|key| {
                !thrift_columns
                    .iter()
                    .any(|column| column.column_name == *key)
            })
            .collect::<Vec<_>>();
        return Err(format!(
            "StarRocks standalone CREATE TABLE key columns are missing from table schema: {}",
            missing.join(", ")
        ));
    }
    if key_indices.is_empty() {
        return Err(
            "StarRocks standalone CREATE TABLE requires at least one key column".to_string(),
        );
    }
    let expected_prefix = (0..key_indices.len())
        .map(|idx| idx as i32)
        .collect::<Vec<_>>();
    if key_indices != expected_prefix {
        return Err(
            "StarRocks standalone CREATE TABLE requires key columns to be a leading column prefix"
                .to_string(),
        );
    }
    let key_count = key_indices.len();
    Ok(crate::thrift::agent_service::TTabletSchema {
        short_key_column_count: i16::try_from(key_count)
            .map_err(|_| "too many key columns for tablet schema".to_string())?,
        schema_hash: 1,
        keys_type: to_keys_type(key_desc.kind),
        storage_type: crate::thrift::types::TStorageType::COLUMN,
        columns: thrift_columns,
        bloom_filter_fpp: None,
        indexes: None,
        is_in_memory: Some(false),
        id: Some(schema_id),
        sort_key_idxes: Some(key_indices.clone()),
        sort_key_unique_ids: Some(key_indices),
        schema_version: Some(0),
        compression_type: Some(crate::thrift::types::TCompressionType::LZ4_FRAME),
        compression_level: None,
    })
}

fn column_aggregation_to_thrift(
    aggregation: ColumnAggregation,
) -> crate::thrift::types::TAggregationType {
    match aggregation {
        ColumnAggregation::Sum => crate::thrift::types::TAggregationType::SUM,
        ColumnAggregation::Min => crate::thrift::types::TAggregationType::MIN,
        ColumnAggregation::Max => crate::thrift::types::TAggregationType::MAX,
        ColumnAggregation::Replace => crate::thrift::types::TAggregationType::REPLACE,
        ColumnAggregation::ReplaceIfNotNull => {
            crate::thrift::types::TAggregationType::REPLACE_IF_NOT_NULL
        }
        ColumnAggregation::BitmapUnion => crate::thrift::types::TAggregationType::BITMAP_UNION,
        ColumnAggregation::HllUnion => crate::thrift::types::TAggregationType::HLL_UNION,
    }
}

fn is_complex_type(data_type: &SqlType) -> bool {
    matches!(
        data_type,
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_)
    )
}

fn sql_type_to_tcolumn_type(
    data_type: &SqlType,
) -> Result<crate::thrift::types::TColumnType, String> {
    let (primitive, len, precision, scale) = match data_type {
        SqlType::TinyInt => (
            crate::thrift::types::TPrimitiveType::TINYINT,
            Some(1),
            None,
            None,
        ),
        SqlType::SmallInt => (
            crate::thrift::types::TPrimitiveType::SMALLINT,
            Some(2),
            None,
            None,
        ),
        SqlType::Int => (
            crate::thrift::types::TPrimitiveType::INT,
            Some(4),
            None,
            None,
        ),
        SqlType::BigInt => (
            crate::thrift::types::TPrimitiveType::BIGINT,
            Some(8),
            None,
            None,
        ),
        SqlType::LargeInt => (
            crate::thrift::types::TPrimitiveType::LARGEINT,
            Some(16),
            None,
            None,
        ),
        SqlType::Float => (
            crate::thrift::types::TPrimitiveType::FLOAT,
            Some(4),
            None,
            None,
        ),
        SqlType::Double => (
            crate::thrift::types::TPrimitiveType::DOUBLE,
            Some(8),
            None,
            None,
        ),
        SqlType::String => (
            crate::thrift::types::TPrimitiveType::VARCHAR,
            Some(65_533),
            None,
            None,
        ),
        SqlType::Json => (
            crate::thrift::types::TPrimitiveType::JSON,
            Some(16),
            None,
            None,
        ),
        SqlType::Bitmap => (
            crate::thrift::types::TPrimitiveType::OBJECT,
            None,
            None,
            None,
        ),
        SqlType::Hll => (crate::thrift::types::TPrimitiveType::HLL, None, None, None),
        SqlType::Boolean => (
            crate::thrift::types::TPrimitiveType::BOOLEAN,
            Some(1),
            None,
            None,
        ),
        SqlType::Date => (
            crate::thrift::types::TPrimitiveType::DATE,
            Some(4),
            None,
            None,
        ),
        SqlType::DateTime => (
            crate::thrift::types::TPrimitiveType::DATETIME,
            Some(8),
            None,
            None,
        ),
        SqlType::DateTimeNs => (
            crate::thrift::types::TPrimitiveType::DATETIME,
            Some(8),
            None,
            None,
        ),
        SqlType::Time => (
            crate::thrift::types::TPrimitiveType::TIME,
            Some(8),
            None,
            None,
        ),
        SqlType::Decimal { precision, scale } => (
            crate::thrift::types::TPrimitiveType::DECIMAL128,
            None,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
        ),
        SqlType::Binary => (
            crate::thrift::types::TPrimitiveType::VARBINARY,
            Some(65_533),
            None,
            None,
        ),
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_) => {
            return Err(format!(
                "sql_type_to_tcolumn_type called on complex type {data_type:?}; callers must use sql_type_to_ttype_desc instead"
            ));
        }
        SqlType::Variant => {
            return Err(
                "VARIANT columns are only supported on iceberg tables; StarRocks table CREATE TABLE rejects VARIANT".to_string(),
            );
        }
    };
    Ok(crate::thrift::types::TColumnType {
        type_: primitive,
        len,
        index_len: len,
        precision,
        scale,
    })
}

/// Build a flat DFS list of `TTypeNode` that describes `data_type`.
/// Handles nested ARRAY/MAP/STRUCT so they round-trip through the
/// `create_tablet` protobuf path (`build_create_tablet_column_pb_from_type_desc`).
fn sql_type_to_ttype_desc(data_type: &SqlType) -> Result<crate::thrift::types::TTypeDesc, String> {
    let mut nodes = Vec::new();
    append_sql_type_nodes(data_type, &mut nodes)?;
    Ok(crate::thrift::types::TTypeDesc { types: Some(nodes) })
}

fn append_sql_type_nodes(
    data_type: &SqlType,
    nodes: &mut Vec<crate::thrift::types::TTypeNode>,
) -> Result<(), String> {
    match data_type {
        SqlType::Array(element) => {
            nodes.push(crate::thrift::types::TTypeNode {
                type_: crate::thrift::types::TTypeNodeType::ARRAY,
                scalar_type: None,
                is_named: None,
                struct_fields: None,
            });
            append_sql_type_nodes(element, nodes)
        }
        SqlType::Map(key, value) => {
            nodes.push(crate::thrift::types::TTypeNode {
                type_: crate::thrift::types::TTypeNodeType::MAP,
                scalar_type: None,
                is_named: None,
                struct_fields: None,
            });
            append_sql_type_nodes(key, nodes)?;
            append_sql_type_nodes(value, nodes)
        }
        SqlType::Struct(fields) => {
            let struct_fields = fields
                .iter()
                .map(|(name, _)| {
                    crate::thrift::types::TStructField::new(
                        Some(name.clone()),
                        None::<String>,
                        None::<i32>,
                        None::<String>,
                    )
                })
                .collect();
            nodes.push(crate::thrift::types::TTypeNode {
                type_: crate::thrift::types::TTypeNodeType::STRUCT,
                scalar_type: None,
                is_named: None,
                struct_fields: Some(struct_fields),
            });
            for (_, field_type) in fields {
                append_sql_type_nodes(field_type, nodes)?;
            }
            Ok(())
        }
        _ => {
            let scalar = sql_type_to_tcolumn_type(data_type)?;
            nodes.push(crate::thrift::types::TTypeNode {
                type_: crate::thrift::types::TTypeNodeType::SCALAR,
                scalar_type: Some(crate::thrift::types::TScalarType {
                    type_: scalar.type_,
                    len: scalar.len,
                    precision: scalar.precision,
                    scale: scalar.scale,
                    time_unit: None,
                }),
                is_named: None,
                struct_fields: None,
            });
            Ok(())
        }
    }
}

fn index_length_for_sql_type(data_type: &SqlType) -> Option<i32> {
    match data_type {
        SqlType::String => Some(10),
        SqlType::Json => None,
        SqlType::TinyInt => Some(1),
        SqlType::SmallInt => Some(2),
        SqlType::Int => Some(4),
        SqlType::BigInt | SqlType::DateTime | SqlType::DateTimeNs | SqlType::Time => Some(8),
        SqlType::LargeInt => Some(16),
        SqlType::Float => Some(4),
        SqlType::Double => Some(8),
        SqlType::Boolean => Some(1),
        SqlType::Date => Some(4),
        SqlType::Decimal { .. }
        | SqlType::Array(_)
        | SqlType::Binary
        | SqlType::Bitmap
        | SqlType::Hll
        | SqlType::Map(_, _)
        | SqlType::Struct(_)
        | SqlType::Variant => None,
    }
}
