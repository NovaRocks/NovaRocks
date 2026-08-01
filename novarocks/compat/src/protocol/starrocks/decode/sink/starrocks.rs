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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::protocol::starrocks::compat::sink::select_partition_boundary_key;
use crate::protocol::starrocks::decode::expr::lower_t_expr_at;
use crate::protocol::starrocks::decode::layout::Layout;
use crate::protocol::starrocks::decode::{
    FragmentExprArenaOwner, StarRocksExternalDependencyDraft, StarRocksFragmentDecodeError,
};
use crate::protocol::starrocks::type_mapping::thrift_desc_to_arrow_type;
use crate::schema_wire::build_sink_tablet_schema;
use crate::thrift::{data_sinks, descriptors, exprs, types};
use novarocks::common::ids::SlotId;
use novarocks::connector::starrocks::lake::context::PartialUpdateWriteMode;
use novarocks::connector::starrocks::schema::StarRocksKeysType;
use novarocks::connector::starrocks::sink::partition_key::{PartitionExprPlan, PartitionKeyValue};
use novarocks::connector::starrocks::sink::plan::{
    SinkIndexDescriptor, SinkLocationDescriptor, SinkNodeInfo, SinkNodesDescriptor,
    SinkOutputProjectionPlan, SinkPartitionDescriptor, SinkPartitionEntry, SinkPartitionIndex,
    SinkPredicatePlan, SinkSchemaDescriptor, SinkSlotDescriptor, SinkTabletLocation,
    StarRocksTableSinkDescriptor, StarRocksTableSinkProgram,
};
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::{ExecNodeKind, ExecPlan};
use novarocks::protocol::FieldPath;
use novarocks::runtime::fragment::instance::StarRocksTableSinkAssignment;
use novarocks_types::UniqueId;

const LOAD_OP_COLUMN: &str = "__op";
const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;

#[derive(Debug)]
pub(crate) enum StarRocksTableSinkDecodeError {
    Sink(String),
    Protocol(StarRocksFragmentDecodeError),
}

impl StarRocksTableSinkDecodeError {
    pub(crate) fn into_fragment(self, sink_path: FieldPath) -> StarRocksFragmentDecodeError {
        match self {
            Self::Sink(detail) => StarRocksFragmentDecodeError::invalid_value(sink_path, detail),
            Self::Protocol(error) => error,
        }
    }
}

impl From<String> for StarRocksTableSinkDecodeError {
    fn from(detail: String) -> Self {
        Self::Sink(detail)
    }
}

impl From<StarRocksFragmentDecodeError> for StarRocksTableSinkDecodeError {
    fn from(error: StarRocksFragmentDecodeError) -> Self {
        Self::Protocol(error)
    }
}

pub(crate) fn lower_starrocks_table_sink(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: Option<&[exprs::TExpr]>,
    exec_plan: Option<&ExecPlan>,
    layout: Option<&Layout>,
    last_query_id: Option<&str>,
    session_time_zone: Option<&str>,
    external_dependencies: Option<&StarRocksExternalDependencyDraft>,
    sink_path: FieldPath,
    output_exprs_path: FieldPath,
) -> Result<(StarRocksTableSinkProgram, StarRocksTableSinkAssignment), StarRocksTableSinkDecodeError>
{
    let keys_type = lower_keys_type(sink.keys_type)?;
    let write_indexes = resolve_sink_write_index_selections(sink)?;
    let primary_schema_id = write_indexes
        .first()
        .map(|index| index.schema_id)
        .ok_or_else(|| {
            "OLAP_TABLE_SINK cannot resolve any write index from sink schema/partition metadata"
                .to_string()
        })?;
    let output_expr_slot_name_map =
        lower_output_expr_slot_name_map(&sink.schema, primary_schema_id, output_exprs)?;
    let output_expr_slot_ids = resolve_output_expr_slot_ids_for_write(output_exprs)?;
    let lower_output = || {
        lower_output_projection(
            sink,
            output_exprs,
            layout,
            session_time_zone,
            last_query_id,
            external_dependencies,
            output_exprs_path,
        )
    };
    let output_projection = if let Some(dependencies) = external_dependencies {
        dependencies.with_expr_arena_owner(
            FragmentExprArenaOwner::StarRocksOutputProjection,
            lower_output,
        )?
    } else {
        lower_output()?
    };
    let slot_name_overrides = if output_expr_slot_name_map.is_empty() {
        None
    } else {
        Some(&output_expr_slot_name_map)
    };
    let output_expr_slot_id_overrides = if output_projection.is_none() {
        build_output_expr_slot_id_overrides(&sink.schema.slot_descs, slot_name_overrides)?
    } else {
        HashMap::new()
    };
    let slot_id_overrides = if output_expr_slot_id_overrides.is_empty() {
        None
    } else {
        Some(&output_expr_slot_id_overrides)
    };

    let frontend = external_dependencies
        .and_then(StarRocksExternalDependencyDraft::frontend_endpoint)
        .cloned();
    let schema = lower_sink_schema(
        &sink.schema,
        keys_type,
        session_time_zone,
        external_dependencies,
        sink_path.clone().field("schema"),
    )?;
    let partition = lower_sink_partition(
        &sink.partition,
        session_time_zone,
        slot_id_overrides,
        external_dependencies,
        sink_path.field("partition"),
    )?;
    let location = lower_sink_location(&sink.location);
    let nodes = lower_sink_nodes(&sink.nodes_info)?;
    let literal_partition_values =
        lower_literal_partition_values(sink, primary_schema_id, output_exprs, exec_plan)?;

    let program = StarRocksTableSinkProgram {
        name: "OLAP_TABLE_SINK".to_string(),
        descriptor: StarRocksTableSinkDescriptor {
            db_id: sink.db_id,
            table_id: sink.table_id,
            db_name: sink.db_name.clone(),
            table_name: sink.table_name.clone(),
            keys_type,
            is_lake_table: sink.is_lake_table.unwrap_or(false),
            dynamic_overwrite: sink.dynamic_overwrite.unwrap_or(false),
            partial_update_mode: decode_partial_update_mode(sink.partial_update_mode),
            merge_condition: sink
                .merge_condition
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(ToString::to_string),
            null_expr_in_auto_increment: sink.null_expr_in_auto_increment.unwrap_or(false),
            miss_auto_increment_column: sink.miss_auto_increment_column.unwrap_or(false),
            schema,
            partition,
            location,
            nodes,
            frontend_provider: external_dependencies
                .and_then(StarRocksExternalDependencyDraft::sink_frontend_provider),
            starlet_metadata_provider: external_dependencies
                .and_then(StarRocksExternalDependencyDraft::starlet_metadata_provider),
            storage_metadata_provider: external_dependencies
                .and_then(StarRocksExternalDependencyDraft::storage_metadata_provider),
        },
        output_projection,
        output_expr_slot_name_map,
        output_expr_slot_ids,
        literal_partition_values,
    };
    program.validate()?;
    let assignment = StarRocksTableSinkAssignment::new(
        sink.txn_id,
        UniqueId::new(sink.load_id.hi, sink.load_id.lo),
        frontend,
    );
    Ok((program, assignment))
}

fn lower_keys_type(keys_type: Option<types::TKeysType>) -> Result<StarRocksKeysType, String> {
    match keys_type.unwrap_or(types::TKeysType::DUP_KEYS) {
        t if t == types::TKeysType::DUP_KEYS => Ok(StarRocksKeysType::Duplicate),
        t if t == types::TKeysType::AGG_KEYS => Ok(StarRocksKeysType::Aggregate),
        t if t == types::TKeysType::PRIMARY_KEYS => Ok(StarRocksKeysType::Primary),
        t if t == types::TKeysType::UNIQUE_KEYS => Ok(StarRocksKeysType::Unique),
        other => Err(format!(
            "OLAP_TABLE_SINK does not support keys_type={other:?}"
        )),
    }
}

fn decode_partial_update_mode(mode: Option<types::TPartialUpdateMode>) -> PartialUpdateWriteMode {
    match mode {
        Some(types::TPartialUpdateMode::ROW_MODE) => PartialUpdateWriteMode::Row,
        Some(types::TPartialUpdateMode::COLUMN_UPSERT_MODE) => PartialUpdateWriteMode::ColumnUpsert,
        Some(types::TPartialUpdateMode::AUTO_MODE) => PartialUpdateWriteMode::Auto,
        Some(types::TPartialUpdateMode::COLUMN_UPDATE_MODE) => PartialUpdateWriteMode::ColumnUpdate,
        _ => PartialUpdateWriteMode::Unknown,
    }
}

fn lower_sink_schema(
    schema: &descriptors::TOlapTableSchemaParam,
    keys_type: StarRocksKeysType,
    session_time_zone: Option<&str>,
    external_dependencies: Option<&StarRocksExternalDependencyDraft>,
    schema_path: FieldPath,
) -> Result<SinkSchemaDescriptor, StarRocksTableSinkDecodeError> {
    let slot_descs = schema
        .slot_descs
        .iter()
        .map(|slot| {
            let id = match slot.id {
                Some(id) if id >= 0 => SlotId::try_from(id).ok(),
                _ => None,
            };
            SinkSlotDescriptor {
                id,
                col_name: slot.col_name.clone(),
                col_physical_name: slot.col_physical_name.clone(),
            }
        })
        .collect();

    let indexes = schema
        .indexes
        .iter()
        .enumerate()
        .map(|(index_idx, index)| {
            let schema_id = index.schema_id.filter(|v| *v > 0).unwrap_or(index.id);
            let tablet_schema = build_sink_tablet_schema(schema, schema_id, keys_type)?;
            Ok(SinkIndexDescriptor {
                index_id: index.id,
                schema_id,
                column_names: lower_index_column_names(index),
                tablet_schema,
                column_to_expr_value: lower_column_to_expr_value(index),
                is_shadow: index.is_shadow.unwrap_or(false),
                where_clause: {
                    let lower = || {
                        lower_optional_predicate(
                            index.where_clause.as_ref(),
                            session_time_zone,
                            external_dependencies,
                            schema_path
                                .clone()
                                .field("indexes")
                                .index(index_idx)
                                .field("where_clause"),
                        )
                    };
                    if let Some(dependencies) = external_dependencies {
                        dependencies.with_expr_arena_owner(
                            FragmentExprArenaOwner::StarRocksIndexPredicate { index: index_idx },
                            lower,
                        )?
                    } else {
                        lower()?
                    }
                },
            })
        })
        .collect::<Result<Vec<_>, StarRocksTableSinkDecodeError>>()?;

    Ok(SinkSchemaDescriptor {
        slot_descs,
        indexes,
    })
}

fn lower_optional_predicate(
    expr: Option<&exprs::TExpr>,
    session_time_zone: Option<&str>,
    external_dependencies: Option<&StarRocksExternalDependencyDraft>,
    expr_path: FieldPath,
) -> Result<Option<SinkPredicatePlan>, StarRocksTableSinkDecodeError> {
    let Some(expr) = expr.filter(|expr| !expr.nodes.is_empty()) else {
        return Ok(None);
    };
    let mut arena = ExprArena::default();
    arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
    let empty_layout = Layout {
        order: Vec::new(),
        index: HashMap::new(),
    };
    let expr_id = lower_t_expr_at(
        expr,
        &mut arena,
        &empty_layout,
        None,
        external_dependencies,
        expr_path,
    )?;
    Ok(Some(SinkPredicatePlan {
        arena: Arc::new(arena),
        expr_id,
    }))
}

fn lower_index_column_names(index: &descriptors::TOlapTableIndexSchema) -> Vec<String> {
    let mut column_names = if let Some(param) = index.column_param.as_ref() {
        param
            .columns
            .iter()
            .map(|c| c.column_name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| name.to_ascii_lowercase())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if column_names.is_empty() {
        column_names = index
            .columns
            .iter()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| name.to_ascii_lowercase())
            .collect::<Vec<_>>();
    }
    column_names
}

fn lower_column_to_expr_value(
    index: &descriptors::TOlapTableIndexSchema,
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(expr_map) = index.column_to_expr_value.as_ref() {
        for (key, value) in expr_map {
            let normalized_key = key.trim().to_ascii_lowercase();
            if normalized_key.is_empty() {
                continue;
            }
            out.insert(normalized_key, value.clone());
        }
    }
    out
}

fn lower_sink_partition(
    partition: &descriptors::TOlapTablePartitionParam,
    session_time_zone: Option<&str>,
    slot_id_overrides: Option<&HashMap<SlotId, SlotId>>,
    external_dependencies: Option<&StarRocksExternalDependencyDraft>,
    partition_path: FieldPath,
) -> Result<SinkPartitionDescriptor, StarRocksTableSinkDecodeError> {
    let partition_exprs = if let Some(exprs) = partition.partition_exprs.as_ref()
        && !exprs.is_empty()
    {
        let lower = || {
            lower_partition_expr_plan(
                exprs,
                session_time_zone,
                slot_id_overrides,
                external_dependencies,
                partition_path.clone().field("partition_exprs"),
            )
        };
        let plan = if let Some(dependencies) = external_dependencies {
            dependencies.with_expr_arena_owner(FragmentExprArenaOwner::StarRocksPartition, lower)?
        } else {
            lower()?
        };
        Some(Arc::new(plan))
    } else {
        None
    };

    let partitions = partition
        .partitions
        .iter()
        .map(|part| {
            Ok(SinkPartitionEntry {
                partition_id: part.id,
                is_shadow: part.is_shadow_partition.unwrap_or(false),
                indexes: part
                    .indexes
                    .iter()
                    .map(|index| SinkPartitionIndex {
                        index_id: index.index_id,
                        tablet_ids: index.tablet_ids.clone(),
                    })
                    .collect(),
                start_key: lower_partition_boundary_key(
                    part.start_keys.as_deref(),
                    part.start_key.as_ref(),
                )?,
                end_key: lower_partition_boundary_key(
                    part.end_keys.as_deref(),
                    part.end_key.as_ref(),
                )?,
                in_keys: lower_partition_in_keys(part.in_keys.as_deref())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(SinkPartitionDescriptor {
        enable_automatic_partition: partition.enable_automatic_partition.unwrap_or(false),
        partition_columns: lower_partition_columns(partition),
        distributed_columns: lower_string_list(partition.distributed_columns.as_deref()),
        partition_exprs,
        partitions,
    })
}

fn lower_partition_columns(partition: &descriptors::TOlapTablePartitionParam) -> Vec<String> {
    let mut cols = partition
        .partition_columns
        .as_ref()
        .map(|values| lower_string_list(Some(values)))
        .unwrap_or_default();
    if cols.is_empty()
        && let Some(col) = partition
            .partition_column
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    {
        cols.push(col.to_ascii_lowercase());
    }
    cols
}

fn lower_string_list(values: Option<&[String]>) -> Vec<String> {
    values
        .map(|values| {
            values
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn lower_partition_expr_plan(
    exprs: &[exprs::TExpr],
    session_time_zone: Option<&str>,
    slot_id_overrides: Option<&HashMap<SlotId, SlotId>>,
    external_dependencies: Option<&StarRocksExternalDependencyDraft>,
    exprs_path: FieldPath,
) -> Result<PartitionExprPlan, StarRocksTableSinkDecodeError> {
    let mut arena = ExprArena::default();
    arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
    let mut expr_ids = Vec::with_capacity(exprs.len());
    let empty_layout = Layout {
        order: Vec::new(),
        index: HashMap::new(),
    };
    for (expr_idx, expr) in exprs.iter().enumerate() {
        let mut rewritten_expr = expr.clone();
        if let Some(overrides) = slot_id_overrides {
            remap_partition_expr_slot_ids(&mut rewritten_expr, overrides)?;
        }
        let expr_id = lower_t_expr_at(
            &rewritten_expr,
            &mut arena,
            &empty_layout,
            None,
            external_dependencies,
            exprs_path.clone().index(expr_idx),
        )?;
        expr_ids.push(expr_id);
    }
    Ok(PartitionExprPlan { arena, expr_ids })
}

fn remap_partition_expr_slot_ids(
    expr: &mut exprs::TExpr,
    slot_id_overrides: &HashMap<SlotId, SlotId>,
) -> Result<(), String> {
    if slot_id_overrides.is_empty() {
        return Ok(());
    }
    for (idx, node) in expr.nodes.iter_mut().enumerate() {
        if node.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = node.slot_ref.as_mut() else {
            continue;
        };
        let source_slot_id = SlotId::try_from(slot_ref.slot_id).map_err(|e| {
            format!(
                "invalid partition expr slot id {} at node {}: {}",
                slot_ref.slot_id, idx, e
            )
        })?;
        let Some(target_slot_id) = slot_id_overrides.get(&source_slot_id).copied() else {
            continue;
        };
        let target_slot_i32 = i32::try_from(target_slot_id.as_u32()).map_err(|_| {
            format!(
                "partition expr remapped slot id {} exceeds i32 range",
                target_slot_id
            )
        })?;
        slot_ref.slot_id = target_slot_i32;
    }
    Ok(())
}

fn lower_sink_location(location: &descriptors::TOlapTableLocationParam) -> SinkLocationDescriptor {
    SinkLocationDescriptor {
        tablets: location
            .tablets
            .iter()
            .map(|tablet| SinkTabletLocation {
                tablet_id: tablet.tablet_id,
                node_ids: tablet.node_ids.clone(),
            })
            .collect(),
    }
}

fn lower_sink_nodes(nodes_info: &descriptors::TNodesInfo) -> Result<SinkNodesDescriptor, String> {
    let nodes = nodes_info
        .nodes
        .iter()
        .map(|node| {
            let option = i32::try_from(node.option).map_err(|_| {
                format!("OLAP_TABLE_SINK node option out of range: {}", node.option)
            })?;
            Ok(SinkNodeInfo {
                id: node.id,
                option,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(SinkNodesDescriptor { nodes })
}

pub(crate) fn lower_partition_boundary_key(
    key_nodes: Option<&[exprs::TExprNode]>,
    legacy_key_node: Option<&exprs::TExprNode>,
) -> Result<Option<Vec<PartitionKeyValue>>, String> {
    let Some(nodes) = select_partition_boundary_key(key_nodes, legacy_key_node) else {
        return Ok(None);
    };
    if nodes.is_empty() {
        return Ok(None);
    }
    parse_partition_key_nodes(nodes).map(Some)
}

pub(crate) fn lower_partition_in_keys(
    in_keys: Option<&[Vec<exprs::TExprNode>]>,
) -> Result<Vec<Vec<PartitionKeyValue>>, String> {
    let Some(in_keys) = in_keys else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(in_keys.len());
    for key in in_keys {
        out.push(parse_partition_key_nodes(key)?);
    }
    Ok(out)
}

fn parse_partition_key_nodes(nodes: &[exprs::TExprNode]) -> Result<Vec<PartitionKeyValue>, String> {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        out.push(parse_partition_key_node(node)?);
    }
    Ok(out)
}

fn parse_partition_key_node(node: &exprs::TExprNode) -> Result<PartitionKeyValue, String> {
    match node.node_type {
        t if t == exprs::TExprNodeType::NULL_LITERAL => Ok(PartitionKeyValue::Null),
        t if t == exprs::TExprNodeType::BOOL_LITERAL => {
            let value = node
                .bool_literal
                .as_ref()
                .ok_or_else(|| "BOOL_LITERAL missing bool_literal payload".to_string())?
                .value;
            Ok(PartitionKeyValue::Bool(value))
        }
        t if t == exprs::TExprNodeType::INT_LITERAL => {
            let value = node
                .int_literal
                .as_ref()
                .ok_or_else(|| "INT_LITERAL missing int_literal payload".to_string())?
                .value as i128;
            Ok(PartitionKeyValue::Int(value))
        }
        t if t == exprs::TExprNodeType::LARGE_INT_LITERAL => {
            let value = node
                .large_int_literal
                .as_ref()
                .ok_or_else(|| "LARGE_INT_LITERAL missing payload".to_string())?
                .value
                .trim()
                .parse::<i128>()
                .map_err(|_| "LARGE_INT_LITERAL parse failed".to_string())?;
            Ok(PartitionKeyValue::Int(value))
        }
        t if t == exprs::TExprNodeType::DECIMAL_LITERAL => {
            let text = node
                .decimal_literal
                .as_ref()
                .ok_or_else(|| "DECIMAL_LITERAL missing decimal_literal payload".to_string())?
                .value
                .clone();
            let DataType::Decimal128(precision, scale) = thrift_desc_to_arrow_type(&node.type_)
                .ok_or_else(|| {
                    "DECIMAL_LITERAL missing or unsupported type descriptor".to_string()
                })?
            else {
                return Err("DECIMAL_LITERAL type descriptor is not decimal".to_string());
            };
            let value = parse_decimal_literal_value(&text, precision, scale)?;
            Ok(PartitionKeyValue::Decimal { value, scale })
        }
        t if t == exprs::TExprNodeType::STRING_LITERAL
            || t == exprs::TExprNodeType::DATE_LITERAL =>
        {
            let value = if t == exprs::TExprNodeType::STRING_LITERAL {
                node.string_literal
                    .as_ref()
                    .ok_or_else(|| "STRING_LITERAL missing string_literal payload".to_string())?
                    .value
                    .clone()
            } else {
                node.date_literal
                    .as_ref()
                    .ok_or_else(|| "DATE_LITERAL missing date_literal payload".to_string())?
                    .value
                    .clone()
            };
            match thrift_desc_to_arrow_type(&node.type_) {
                Some(DataType::Date32) => {
                    Ok(PartitionKeyValue::Date32(parse_date_literal_days(&value)?))
                }
                Some(DataType::Timestamp(_, _)) | Some(DataType::Time64(_)) => Ok(
                    PartitionKeyValue::TimestampMicros(parse_datetime_literal_micros(&value)?),
                ),
                Some(DataType::Binary) => Ok(PartitionKeyValue::Binary(value.into_bytes())),
                _ => Ok(PartitionKeyValue::Utf8(value)),
            }
        }
        t if t == exprs::TExprNodeType::BINARY_LITERAL => {
            let value = node
                .binary_literal
                .as_ref()
                .ok_or_else(|| "BINARY_LITERAL missing payload".to_string())?
                .value
                .clone();
            Ok(PartitionKeyValue::Binary(value))
        }
        t if t == exprs::TExprNodeType::FLOAT_LITERAL => {
            let _ = node
                .float_literal
                .as_ref()
                .ok_or_else(|| "FLOAT_LITERAL missing float_literal payload".to_string())?;
            Err("unsupported partition key literal node type: FLOAT_LITERAL".to_string())
        }
        other => Err(format!(
            "unsupported partition key literal node type: {:?}",
            other
        )),
    }
}

fn parse_date_literal_days(value: &str) -> Result<i32, String> {
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("invalid DATE literal '{value}'"))
}

fn parse_datetime_literal_micros(value: &str) -> Result<i64, String> {
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid DATETIME literal '{value}'"))?;
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid DATETIME literal '{value}'"))
}

fn parse_decimal_literal_value(value: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {scale}"));
    }
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty DECIMAL literal".to_string());
    }

    let mut sign: i128 = 1;
    if let Some(rest) = s.strip_prefix('-') {
        sign = -1;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty DECIMAL literal".to_string());
    }

    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }

    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }

    let scale_usize = scale as usize;
    if frac_part.len() > scale_usize {
        return Err(format!(
            "DECIMAL literal '{}' exceeds scale {}",
            value, scale_usize
        ));
    }

    let mut digits = String::with_capacity(int_part.len() + scale_usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(scale_usize - frac_part.len()) {
        digits.push('0');
    }

    let digits_trim = digits.trim_start_matches('0');
    let digits_final = if digits_trim.is_empty() {
        "0"
    } else {
        digits_trim
    };
    if digits_final.len() > precision as usize {
        return Err(format!(
            "DECIMAL literal '{}' exceeds precision {}",
            value, precision
        ));
    }

    let unsigned = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL literal '{value}'"))?;
    Ok(unsigned.saturating_mul(sign))
}

fn lower_output_projection(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: Option<&[exprs::TExpr]>,
    layout: Option<&Layout>,
    session_time_zone: Option<&str>,
    last_query_id: Option<&str>,
    external_dependencies: Option<&StarRocksExternalDependencyDraft>,
    output_exprs_path: FieldPath,
) -> Result<Option<SinkOutputProjectionPlan>, StarRocksTableSinkDecodeError> {
    let Some(output_exprs) = output_exprs.filter(|exprs| !exprs.is_empty()) else {
        return Ok(None);
    };
    let has_index_where_clause = sink.schema.indexes.iter().any(|index| {
        index
            .where_clause
            .as_ref()
            .is_some_and(|expr| !expr.nodes.is_empty())
    });
    if output_exprs_are_plain_slot_refs(output_exprs) && !has_index_where_clause {
        return Ok(None);
    }
    let layout = layout.ok_or_else(|| {
        "OLAP_TABLE_SINK requires layout for output expression projection".to_string()
    })?;
    let output_slots = resolve_output_projection_slots(sink, output_exprs)?;
    if output_slots.len() != output_exprs.len() {
        return Err(format!(
            "OLAP_TABLE_SINK output projection slot count mismatch: slots={} output_exprs={}",
            output_slots.len(),
            output_exprs.len()
        )
        .into());
    }

    let mut arena = ExprArena::default();
    arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
    let mut expr_ids = Vec::with_capacity(output_exprs.len());
    for (expr_idx, expr) in output_exprs.iter().enumerate() {
        let expr_id = lower_t_expr_at(
            expr,
            &mut arena,
            layout,
            last_query_id,
            external_dependencies,
            output_exprs_path.clone().index(expr_idx),
        )?;
        expr_ids.push(expr_id);
    }
    let (output_slot_ids, output_field_names): (Vec<_>, Vec<_>) = output_slots.into_iter().unzip();
    Ok(Some(SinkOutputProjectionPlan {
        arena: Arc::new(arena),
        expr_ids,
        output_slot_ids,
        output_field_names,
    }))
}

fn output_exprs_are_plain_slot_refs(output_exprs: &[exprs::TExpr]) -> bool {
    output_exprs.iter().all(|expr| {
        expr.nodes.len() == 1
            && expr.nodes.first().is_some_and(|node| {
                node.node_type == exprs::TExprNodeType::SLOT_REF && node.slot_ref.is_some()
            })
    })
}

fn resolve_output_projection_slots(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: &[exprs::TExpr],
) -> Result<Vec<(SlotId, String)>, String> {
    if let Some(mapped) = resolve_slots_from_expr_output_column(sink, output_exprs)? {
        return Ok(mapped);
    }

    let collect_named_slots = |skip_load_op: bool| -> Result<Vec<(SlotId, String)>, String> {
        let mut out = Vec::new();
        for (idx, slot_desc) in sink.schema.slot_descs.iter().enumerate() {
            let Some(raw_name) = slot_desc
                .col_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            if skip_load_op && raw_name.eq_ignore_ascii_case(LOAD_OP_COLUMN) {
                continue;
            }
            let slot_id = slot_desc.id.ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK schema.slot_descs[{}] missing id while resolving output projection slots",
                    idx
                )
            })?;
            if slot_id < 0 {
                return Err(format!(
                    "OLAP_TABLE_SINK schema.slot_descs[{}] has invalid id {} while resolving output projection slots",
                    idx, slot_id
                ));
            }
            let slot_id = SlotId::try_from(slot_id)?;
            let name = if raw_name.eq_ignore_ascii_case(LOAD_OP_COLUMN) {
                LOAD_OP_COLUMN.to_string()
            } else {
                raw_name.to_string()
            };
            out.push((slot_id, name));
        }
        Ok(out)
    };

    let named_slot_count = sink
        .schema
        .slot_descs
        .iter()
        .filter(|slot| {
            slot.col_name
                .as_deref()
                .map(str::trim)
                .is_some_and(|name| !name.is_empty())
        })
        .count();
    let skip_load_op = output_exprs.len() < named_slot_count;
    let named_slots = collect_named_slots(skip_load_op)?;
    if named_slots.len() == output_exprs.len() {
        return Ok(named_slots);
    }

    let mut ordinal_slots = Vec::new();
    for (idx, slot_desc) in sink.schema.slot_descs.iter().enumerate() {
        let Some(id) = slot_desc.id.filter(|id| *id >= 0) else {
            continue;
        };
        let slot_id = SlotId::try_from(id)?;
        let name = slot_desc
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|name| {
                if name.eq_ignore_ascii_case(LOAD_OP_COLUMN) {
                    LOAD_OP_COLUMN.to_string()
                } else {
                    name.to_string()
                }
            })
            .unwrap_or_else(|| format!("col_{idx}"));
        ordinal_slots.push((slot_id, name));
    }
    if ordinal_slots.len() == output_exprs.len() {
        return Ok(ordinal_slots);
    }

    let mut slot_name_by_id = HashMap::new();
    for (slot_id, name) in &ordinal_slots {
        slot_name_by_id
            .entry(*slot_id)
            .or_insert_with(|| name.clone());
    }
    let mut expr_slots = Vec::with_capacity(output_exprs.len());
    for (idx, expr) in output_exprs.iter().enumerate() {
        let root = expr.nodes.first().ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK output_exprs[{}] is empty while resolving output projection slots",
                idx
            )
        })?;
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            return Err(format!(
                "OLAP_TABLE_SINK cannot resolve output projection slot for output_exprs[{}] node_type={:?}",
                idx, root.node_type
            ));
        }
        let slot_ref = root.slot_ref.as_ref().ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK output_exprs[{}] SLOT_REF missing slot_ref payload",
                idx
            )
        })?;
        let slot_id = SlotId::try_from(slot_ref.slot_id)?;
        let name = slot_name_by_id
            .get(&slot_id)
            .cloned()
            .unwrap_or_else(|| format!("col_{idx}"));
        expr_slots.push((slot_id, name));
    }
    Ok(expr_slots)
}

fn resolve_slots_from_expr_output_column(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: &[exprs::TExpr],
) -> Result<Option<Vec<(SlotId, String)>>, String> {
    if output_exprs.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut out = Vec::with_capacity(output_exprs.len());
    for (expr_idx, expr) in output_exprs.iter().enumerate() {
        let Some(root) = expr.nodes.first() else {
            return Ok(None);
        };
        let Some(output_column) = root.output_column else {
            return Ok(None);
        };
        if output_column < 0 {
            return Ok(None);
        }
        let output_idx = usize::try_from(output_column).map_err(|_| {
            format!(
                "OLAP_TABLE_SINK output_exprs[{}] has invalid output_column={}",
                expr_idx, output_column
            )
        })?;
        let Some(slot_desc) = sink.schema.slot_descs.get(output_idx) else {
            return Ok(None);
        };
        let Some(slot_id_i32) = slot_desc.id else {
            return Ok(None);
        };
        if slot_id_i32 < 0 {
            return Ok(None);
        }
        let slot_id = SlotId::try_from(slot_id_i32)?;
        let name = slot_desc
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|name| {
                if name.eq_ignore_ascii_case(LOAD_OP_COLUMN) {
                    LOAD_OP_COLUMN.to_string()
                } else {
                    name.to_string()
                }
            })
            .unwrap_or_else(|| format!("col_{output_idx}"));
        out.push((slot_id, name));
    }

    Ok(Some(out))
}

fn lower_output_expr_slot_name_map(
    schema: &descriptors::TOlapTableSchemaParam,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<HashMap<String, SlotId>, String> {
    build_output_expr_slot_name_map_for_write(schema, schema_id, output_exprs)
}

fn build_output_expr_slot_name_map_for_write(
    schema: &descriptors::TOlapTableSchemaParam,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<HashMap<String, SlotId>, String> {
    let Some(output_exprs) = output_exprs else {
        return Ok(HashMap::new());
    };
    if output_exprs.is_empty() {
        return Ok(HashMap::new());
    }
    let mut slot_map = HashMap::new();
    let named_slot_count = schema
        .slot_descs
        .iter()
        .filter(|slot| {
            slot.col_name
                .as_deref()
                .map(str::trim)
                .is_some_and(|name| !name.is_empty())
        })
        .count();
    let skip_load_op = output_exprs.len() < named_slot_count;
    let mut expr_iter = output_exprs.iter();
    for slot_desc in &schema.slot_descs {
        let Some(column_name) = slot_desc
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_ascii_lowercase)
        else {
            continue;
        };
        if skip_load_op && column_name == LOAD_OP_COLUMN {
            continue;
        }
        let Some(expr) = expr_iter.next() else {
            break;
        };
        let Some(root) = expr.nodes.first() else {
            continue;
        };
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = root.slot_ref.as_ref() else {
            continue;
        };
        let Ok(slot_id) = SlotId::try_from(slot_ref.slot_id) else {
            continue;
        };
        slot_map.insert(column_name, slot_id);
    }
    if !slot_map.is_empty() {
        return Ok(slot_map);
    }

    let column_names = resolve_index_column_names_for_write(schema, schema_id)?;
    if column_names.is_empty() {
        return Ok(HashMap::new());
    }

    for (column_name, expr) in column_names.iter().zip(output_exprs.iter()) {
        let Some(root) = expr.nodes.first() else {
            continue;
        };
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = root.slot_ref.as_ref() else {
            continue;
        };
        let Ok(slot_id) = SlotId::try_from(slot_ref.slot_id) else {
            continue;
        };
        slot_map.insert(column_name.clone(), slot_id);
    }
    Ok(slot_map)
}

fn build_output_expr_slot_id_overrides(
    slot_descs: &[descriptors::TSlotDescriptor],
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
) -> Result<HashMap<SlotId, SlotId>, String> {
    let Some(slot_name_overrides) = slot_name_overrides else {
        return Ok(HashMap::new());
    };
    if slot_name_overrides.is_empty() {
        return Ok(HashMap::new());
    }

    let schema_slot_by_name = build_slot_name_map(slot_descs)?;
    let mut slot_id_overrides = HashMap::new();
    for (column_name, schema_slot_id) in schema_slot_by_name {
        let Some(output_slot_id) = slot_name_overrides.get(&column_name).copied() else {
            continue;
        };
        if output_slot_id != schema_slot_id {
            slot_id_overrides.insert(schema_slot_id, output_slot_id);
        }
    }
    Ok(slot_id_overrides)
}

fn resolve_output_expr_slot_ids_for_write(
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<Vec<Option<SlotId>>, String> {
    let Some(output_exprs) = output_exprs else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(output_exprs.len());
    for expr in output_exprs {
        let Some(root) = expr.nodes.first() else {
            out.push(None);
            continue;
        };
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            out.push(None);
            continue;
        }
        let Some(slot_ref) = root.slot_ref.as_ref() else {
            out.push(None);
            continue;
        };
        out.push(SlotId::try_from(slot_ref.slot_id).ok());
    }
    Ok(out)
}

fn build_slot_name_map(
    slot_descs: &[descriptors::TSlotDescriptor],
) -> Result<HashMap<String, SlotId>, String> {
    let mut slot_by_name = HashMap::new();
    for slot in slot_descs {
        let Some(id) = slot.id.filter(|id| *id >= 0) else {
            continue;
        };
        let slot_id = SlotId::try_from(id)?;
        if let Some(name) = slot
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            slot_by_name.insert(name.to_ascii_lowercase(), slot_id);
        }
        if let Some(physical_name) = slot
            .col_physical_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            slot_by_name.insert(physical_name.to_ascii_lowercase(), slot_id);
        }
    }
    Ok(slot_by_name)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SinkWriteIndexSelection {
    index_id: i64,
    schema_id: i64,
}

fn resolve_sink_write_index_selections(
    sink: &data_sinks::TOlapTableSink,
) -> Result<Vec<SinkWriteIndexSelection>, String> {
    let mut schema_id_by_index_id = HashMap::<i64, i64>::new();
    let mut index_by_id = HashMap::<i64, &descriptors::TOlapTableIndexSchema>::new();
    for index in &sink.schema.indexes {
        if index.id <= 0 {
            continue;
        }
        let schema_id = index.schema_id.filter(|v| *v > 0).unwrap_or(index.id);
        if schema_id <= 0 {
            return Err(format!(
                "OLAP_TABLE_SINK schema.indexes contains non-positive schema_id/index_id: index_id={} schema_id={}",
                index.id, schema_id
            ));
        }
        schema_id_by_index_id.insert(index.id, schema_id);
        index_by_id.insert(index.id, index);
    }
    if schema_id_by_index_id.is_empty() {
        return Err("OLAP_TABLE_SINK schema.indexes has no valid index_id/schema_id".to_string());
    }

    let mut candidate_index_ids = BTreeSet::<i64>::new();
    for partition in sink
        .partition
        .partitions
        .iter()
        .filter(|part| !part.is_shadow_partition.unwrap_or(false))
    {
        for index in &partition.indexes {
            if index.index_id > 0 {
                candidate_index_ids.insert(index.index_id);
            }
        }
    }
    if candidate_index_ids.is_empty() {
        let fallback_schema_id = resolve_schema_id(&sink.schema)?;
        for index in &sink.schema.indexes {
            let schema_id = index.schema_id.filter(|v| *v > 0).unwrap_or(index.id);
            if schema_id == fallback_schema_id && index.id > 0 {
                candidate_index_ids.insert(index.id);
            }
        }
    }
    if candidate_index_ids.is_empty() {
        return Err(
            "OLAP_TABLE_SINK cannot resolve candidate write index ids from partition/schema metadata"
                .to_string(),
        );
    }

    let slot_names = sink
        .schema
        .slot_descs
        .iter()
        .filter_map(|slot| {
            slot.col_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(|name| name.to_ascii_lowercase())
        })
        .filter(|name| name != LOAD_OP_COLUMN)
        .collect::<HashSet<_>>();

    let mut scored = Vec::<(i64, i64, bool, usize, usize)>::new();
    for index_id in candidate_index_ids {
        let schema_id = schema_id_by_index_id
            .get(&index_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK partition index_id={} is absent in schema.indexes",
                    index_id
                )
            })?;
        let index = index_by_id.get(&index_id).copied().ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK cannot resolve schema index for index_id={}",
                index_id
            )
        })?;
        let index_columns = lower_index_column_names(index);
        let overlap = if slot_names.is_empty() {
            0
        } else {
            index_columns
                .iter()
                .filter(|name| slot_names.contains(*name))
                .count()
        };
        scored.push((
            index_id,
            schema_id,
            index.is_shadow.unwrap_or(false),
            overlap,
            index_columns.len(),
        ));
    }
    if scored.is_empty() {
        return Err("OLAP_TABLE_SINK candidate write indexes are empty".to_string());
    }

    scored.sort_by(|left, right| {
        let left_shadow = if left.2 { 1 } else { 0 };
        let right_shadow = if right.2 { 1 } else { 0 };
        left_shadow
            .cmp(&right_shadow)
            .then(right.3.cmp(&left.3))
            .then(right.4.cmp(&left.4))
            .then(left.0.cmp(&right.0))
    });
    let primary_index_id = scored
        .first()
        .map(|item| item.0)
        .ok_or_else(|| "OLAP_TABLE_SINK cannot select primary write index".to_string())?;

    let mut out = Vec::with_capacity(scored.len());
    out.push(SinkWriteIndexSelection {
        index_id: primary_index_id,
        schema_id: schema_id_by_index_id
            .get(&primary_index_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK missing schema_id for primary write index_id={}",
                    primary_index_id
                )
            })?,
    });

    let mut rest = scored
        .into_iter()
        .filter(|item| item.0 != primary_index_id)
        .map(|item| SinkWriteIndexSelection {
            index_id: item.0,
            schema_id: item.1,
        })
        .collect::<Vec<_>>();
    rest.sort_by_key(|item| item.index_id);
    out.extend(rest);
    Ok(out)
}

fn resolve_schema_id(schema: &descriptors::TOlapTableSchemaParam) -> Result<i64, String> {
    for index in &schema.indexes {
        if let Some(schema_id) = index.schema_id.filter(|v| *v > 0) {
            return Ok(schema_id);
        }
        if index.id > 0 {
            return Ok(index.id);
        }
    }
    Err("OLAP_TABLE_SINK schema.indexes has no valid schema_id".to_string())
}

fn resolve_index_column_names_for_write(
    schema: &descriptors::TOlapTableSchemaParam,
    schema_id: i64,
) -> Result<Vec<String>, String> {
    let index = schema
        .indexes
        .iter()
        .find(|idx| idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id) == schema_id)
        .ok_or_else(|| {
            format!("OLAP_TABLE_SINK cannot resolve schema index for schema_id={schema_id}")
        })?;
    Ok(lower_index_column_names(index))
}

fn lower_literal_partition_values(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
    exec_plan: Option<&ExecPlan>,
) -> Result<Option<Vec<String>>, String> {
    if !sink.partition.enable_automatic_partition.unwrap_or(false) {
        return Ok(None);
    }
    if sink
        .partition
        .partition_exprs
        .as_ref()
        .is_some_and(|exprs| !exprs.is_empty())
    {
        return Ok(None);
    }

    let partition_columns = lower_partition_columns(&sink.partition);
    if partition_columns.is_empty() {
        return Ok(None);
    }

    let Some(output_exprs) = output_exprs else {
        return Ok(None);
    };
    let index_columns = resolve_index_column_names_for_write(&sink.schema, schema_id)?;
    if index_columns.is_empty() {
        return Ok(None);
    }

    let mut partition_values = Vec::with_capacity(partition_columns.len());
    for partition_col in &partition_columns {
        let Some(column_idx) = index_columns.iter().position(|name| name == partition_col) else {
            return Ok(None);
        };
        let Some(expr) = output_exprs.get(column_idx) else {
            return Ok(None);
        };
        let Some(value) = extract_partition_literal_value(expr)
            .or_else(|| extract_partition_value_from_exec_plan(expr, exec_plan))
        else {
            return Ok(None);
        };
        partition_values.push(value);
    }

    Ok(Some(partition_values))
}

fn extract_partition_literal_value(expr: &exprs::TExpr) -> Option<String> {
    for node in &expr.nodes {
        let ty = node.node_type;
        if ty == exprs::TExprNodeType::STRING_LITERAL {
            return node.string_literal.as_ref().map(|v| v.value.clone());
        }
        if ty == exprs::TExprNodeType::DATE_LITERAL {
            return node.date_literal.as_ref().map(|v| v.value.clone());
        }
        if ty == exprs::TExprNodeType::INT_LITERAL {
            return node.int_literal.as_ref().map(|v| v.value.to_string());
        }
        if ty == exprs::TExprNodeType::LARGE_INT_LITERAL {
            return node.large_int_literal.as_ref().map(|v| v.value.clone());
        }
        if ty == exprs::TExprNodeType::BOOL_LITERAL {
            return node.bool_literal.as_ref().map(|v| {
                if v.value {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            });
        }
    }
    None
}

fn extract_partition_value_from_exec_plan(
    expr: &exprs::TExpr,
    exec_plan: Option<&ExecPlan>,
) -> Option<String> {
    let exec_plan = exec_plan?;
    let root = expr.nodes.first()?;
    if root.node_type != exprs::TExprNodeType::SLOT_REF {
        return None;
    }
    let slot_ref = root.slot_ref.as_ref()?;
    let output_slot_id = SlotId::try_from(slot_ref.slot_id).ok()?;

    let project = match &exec_plan.root.kind {
        ExecNodeKind::Project(project) => project,
        _ => return None,
    };
    let values = match &project.input.kind {
        ExecNodeKind::Values(values) => values,
        _ => return None,
    };
    if values.chunk.is_empty() {
        return None;
    }

    let output_pos = project
        .output_chunk_schema
        .slot_ids()
        .iter()
        .position(|slot| *slot == output_slot_id)?;
    let expr_idx = project
        .output_indices
        .as_ref()
        .and_then(|indices| indices.get(output_pos).copied())
        .unwrap_or(output_pos);
    let expr_id = *project.exprs.get(expr_idx)?;
    let array = exec_plan.arena.eval(expr_id, &values.chunk).ok()?;
    if array.is_null(0) {
        return None;
    }
    scalar_partition_value_to_string(array.as_ref(), 0)
}

fn scalar_partition_value_to_string(array: &dyn Array, row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|v| v.value(row).to_string()),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|v| v.value(row).to_string()),
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|v| v.value(row).to_string()),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|v| v.value(row).to_string()),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|v| v.value(row).to_string()),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|v| v.value(row).to_string()),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|v| v.value(row).to_string()),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|v| v.value(row).to_string()),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|v| v.value(row).to_string()),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|v| v.value(row).to_string()),
        DataType::Boolean => array.as_any().downcast_ref::<BooleanArray>().map(|v| {
            if v.value(row) {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }),
        DataType::Date32 => {
            let days_since_epoch = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|v| v.value(row))?;
            let days_from_ce = UNIX_EPOCH_DAY_OFFSET.checked_add(days_since_epoch)?;
            let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce)?;
            Some(date.format("%Y-%m-%d").to_string())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft;
    use crate::thrift::{data_sinks, descriptors, types};
    use novarocks::connector::starrocks::schema::StarRocksKeysType;
    use novarocks::connector::starrocks::sink::plan::{
        StarRocksTableSinkDescriptor, StarRocksTableSinkProgram,
    };
    use novarocks::protocol::FieldPath;
    use novarocks::runtime::endpoint::RuntimeEndpoint;

    use super::lower_starrocks_table_sink;

    fn minimal_olap_sink() -> data_sinks::TOlapTableSink {
        let bigint = crate::protocol::starrocks::type_mapping::thrift_type_desc_from_primitive(
            types::TPrimitiveType::BIGINT,
        );
        let slot = descriptors::TSlotDescriptor::new(
            1,
            1,
            bigint.clone(),
            0,
            None,
            None,
            None,
            "k".to_string(),
            0,
            true,
            true,
            false,
            1,
            None,
            false,
        );
        let column = descriptors::TColumn::new(
            "k".to_string(),
            None,
            None,
            true,
            false,
            None,
            None,
            None,
            false,
            1,
            false,
            None,
            None,
            bigint,
            None,
        );
        let index = descriptors::TOlapTableIndexSchema::new(
            10,
            vec!["k".to_string()],
            0,
            descriptors::TOlapTableColumnParam::new(vec![column], vec![1], 1),
            None,
            10,
            None,
            false,
        );
        let schema = descriptors::TOlapTableSchemaParam::new(
            1,
            2,
            0,
            vec![slot],
            descriptors::TTupleDescriptor::new(1, None, None, 2, None),
            vec![index],
        );
        let partition = descriptors::TOlapTablePartition::new(
            20,
            None,
            None,
            None,
            vec![descriptors::TOlapTableIndexTablets::new(10, vec![30], None)],
            None,
            None,
            None,
            false,
        );

        data_sinks::TOlapTableSink::new(
            types::TUniqueId { hi: 101, lo: 103 },
            97,
            1,
            2,
            1,
            1,
            false,
            "db".to_string(),
            "tbl".to_string(),
            schema,
            descriptors::TOlapTablePartitionParam::new(
                1,
                2,
                0,
                None,
                Vec::<String>::new(),
                vec![partition],
                Vec::<String>::new(),
                Vec::<crate::thrift::exprs::TExpr>::new(),
                false,
                None,
            ),
            descriptors::TOlapTableLocationParam::new(
                1,
                2,
                0,
                vec![descriptors::TTabletLocation::new(30, vec![40])],
            ),
            descriptors::TNodesInfo::new(
                0,
                vec![descriptors::TNodeInfo::new(
                    40,
                    0,
                    "backend".to_string(),
                    8060,
                )],
            ),
            None,
            true,
            None,
            types::TKeysType::PRIMARY_KEYS,
            None,
            None,
            None,
            false,
            false,
            None,
            None,
            types::TPartialUpdateMode::ROW_MODE,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            None,
        )
    }

    #[test]
    fn olap_decode_separates_immutable_program_from_instance_assignment() {
        let frontend = RuntimeEndpoint::new("frontend", 9020).expect("frontend endpoint");
        let dependencies =
            StarRocksExternalDependencyDraft::new(Some(frontend.clone()), BTreeMap::new());
        let (program, assignment) = lower_starrocks_table_sink(
            &minimal_olap_sink(),
            None,
            None,
            None,
            None,
            None,
            Some(&dependencies),
            FieldPath::root("olap_table_sink"),
            FieldPath::root("output_exprs"),
        )
        .expect("OLAP sink decode");

        // Keep this exhaustive: adding transaction, load, or frontend identity to
        // either immutable type must break this compile-time contract test.
        let StarRocksTableSinkProgram {
            name,
            descriptor,
            output_projection,
            output_expr_slot_name_map,
            output_expr_slot_ids,
            literal_partition_values,
        } = program;
        let StarRocksTableSinkDescriptor {
            db_id,
            table_id,
            db_name,
            table_name,
            keys_type,
            is_lake_table,
            dynamic_overwrite,
            partial_update_mode,
            merge_condition,
            null_expr_in_auto_increment,
            miss_auto_increment_column,
            schema,
            partition,
            location,
            nodes,
            ..
        } = descriptor;

        assert_eq!(name, "OLAP_TABLE_SINK");
        assert_eq!((db_id, table_id), (1, 2));
        assert_eq!(db_name.as_deref(), Some("db"));
        assert_eq!(table_name.as_deref(), Some("tbl"));
        assert_eq!(keys_type, StarRocksKeysType::Primary);
        assert!(is_lake_table);
        assert!(!dynamic_overwrite);
        assert!(matches!(
            partial_update_mode,
            novarocks::connector::starrocks::lake::context::PartialUpdateWriteMode::Row
        ));
        assert!(merge_condition.is_none());
        assert!(!null_expr_in_auto_increment);
        assert!(!miss_auto_increment_column);
        assert_eq!(schema.indexes.len(), 1);
        assert_eq!(partition.partitions.len(), 1);
        assert_eq!(location.tablets.len(), 1);
        assert_eq!(nodes.nodes.len(), 1);
        assert!(output_projection.is_none());
        assert!(output_expr_slot_name_map.is_empty());
        assert!(output_expr_slot_ids.is_empty());
        assert!(literal_partition_values.is_none());

        assert_eq!(assignment.txn_id(), 97);
        assert_eq!(
            (assignment.load_id().high(), assignment.load_id().low()),
            (101, 103)
        );
        assert_eq!(assignment.frontend(), Some(&frontend));
    }
}
