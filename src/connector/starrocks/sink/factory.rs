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
//! Simplified OLAP table sink for internal shared-data write paths.
//!
//! Responsibilities:
//! - Build write target contexts from FE sink metadata.
//! - Resolve row routing plan and tablet commit infos.
//! - Construct sink operator instances.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, BooleanArray, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use chrono::NaiveDate;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{
    ReadHalf as TReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel,
    TTcpChannel, WriteHalf as TWriteHalf,
};

use crate::common::ids::SlotId;
use crate::connector::starrocks::fe_v2_meta::{
    LakeTableIdentity, resolve_tablet_paths_for_olap_sink,
};
use crate::connector::starrocks::lake::context::{
    AutoIncrementWritePolicy, PartialUpdateWriteMode, PartialUpdateWritePolicy, get_tablet_runtime,
};
use crate::connector::starrocks::lake::{TabletWriteContext, build_sink_tablet_schema};
use crate::connector::starrocks::sink::operator::{
    OlapSinkFinalizeSharedState, OlapTableSinkOperator,
};
use crate::connector::starrocks::sink::partition_key::{
    PartitionKeySource, build_partition_key_source, build_slot_name_map, resolve_slot_ids_by_names,
};
use crate::connector::starrocks::sink::routing::{RowRoutingPlan, build_sink_routing};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::node::{ExecNodeKind, ExecPlan};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::formats::starrocks::writer::StarRocksWriteFormat;
use crate::frontend_service::{self, FrontendServiceSyncClient, TFrontendServiceSyncClient};
use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::Layout;
use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_logging::info;
use crate::runtime::starlet_shard_registry;
use crate::service::disk_report;
use crate::service::grpc_client::proto::starrocks::{KeysType, PUniqueId, TabletSchemaPb};
use crate::status_code;
use crate::{data_sinks, descriptors, exprs, types};

const FE_AUTOMATIC_PARTITION_TIMEOUT_SECS: u64 = 60;
const LOAD_OP_COLUMN: &str = "__op";
pub(crate) const STARROCKS_DEFAULT_PARTITION_VALUE: &str = "__STARROCKS_DEFAULT_PARTITION__";

#[derive(Clone)]
pub struct OlapTableSinkFactory {
    name: String,
    plan: Arc<OlapTableSinkPlan>,
    finalize_shared: Arc<OlapSinkFinalizeSharedState>,
}

#[derive(Clone)]
pub(crate) struct OlapTableSinkPlan {
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) table_identity: LakeTableIdentity,
    pub(crate) db_name: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) txn_id: i64,
    pub(crate) load_id: PUniqueId,
    pub(crate) write_format: StarRocksWriteFormat,
    pub(crate) tablet_commit_infos: Vec<types::TTabletCommitInfo>,
    pub(crate) write_targets: HashMap<i64, TabletWriteTarget>,
    pub(crate) row_routing: RowRoutingPlan,
    pub(crate) schema_slot_bindings: Vec<Option<SlotId>>,
    pub(crate) op_slot_id: Option<SlotId>,
    pub(crate) output_projection: Option<SinkOutputProjectionPlan>,
    pub(crate) auto_partition: Option<AutomaticPartitionPlan>,
}

#[derive(Clone)]
pub(crate) struct SinkOutputProjectionPlan {
    pub(crate) arena: Arc<ExprArena>,
    pub(crate) expr_ids: Vec<ExprId>,
    pub(crate) output_slot_ids: Vec<SlotId>,
    pub(crate) output_field_names: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct TabletWriteTarget {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) context: TabletWriteContext,
}

#[derive(Clone)]
pub(crate) struct AutomaticPartitionPlan {
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) txn_id: i64,
    pub(crate) fe_addr: types::TNetworkAddress,
    pub(crate) partition_key_source: PartitionKeySource,
    pub(crate) partition_column_names: Vec<String>,
    pub(crate) partition_slot_ids: Vec<SlotId>,
    pub(crate) candidate_index_ids: HashSet<i64>,
}

impl OlapTableSinkFactory {
    pub(crate) fn try_new(
        sink: data_sinks::TOlapTableSink,
        output_exprs: Option<&[exprs::TExpr]>,
        exec_plan: Option<&ExecPlan>,
        layout: Option<&Layout>,
        last_query_id: Option<&str>,
        fe_addr: Option<&types::TNetworkAddress>,
    ) -> Result<Self, String> {
        let mut sink = sink;
        if sink.is_lake_table != Some(true) {
            return Err(
                "OLAP_TABLE_SINK only supports shared-data lake table in novarocks phase-1 write path"
                    .to_string(),
            );
        }
        let keys_type = map_sink_keys_type(sink.keys_type)?;

        let schema_id = resolve_schema_id(&sink.schema)?;
        maybe_create_automatic_partitions_for_literal_insert(
            &mut sink,
            schema_id,
            output_exprs,
            exec_plan,
            fe_addr,
        )?;
        let output_projection =
            build_output_projection_plan(&sink, output_exprs, layout, last_query_id, fe_addr)?;
        // StarRocks routes and writes against the post-output_expr tuple slots.
        // Once we materialize output_exprs into sink schema slot ids, downstream slot resolution
        // must also use sink schema ids instead of original child slot refs.
        let routing_exprs = if output_projection.is_some() {
            None
        } else {
            output_exprs
        };
        let routing = build_sink_routing(&sink, schema_id, routing_exprs)?;
        if routing.commit_infos.is_empty() {
            return Err("OLAP_TABLE_SINK resolved empty tablet commit infos".to_string());
        }
        if sink.keys_type == Some(types::TKeysType::PRIMARY_KEYS) {
            info!(
                target: "novarocks::sink",
                table_id = sink.table_id,
                schema_id,
                distributed_slot_ids = ?routing.row_routing.distributed_slot_ids,
                partition_key_len = routing.row_routing.partition_key_len,
                tablet_count = routing.row_routing.tablet_ids.len(),
                "OLAP_TABLE_SINK built row routing for primary key table"
            );
        }
        let table_identity = build_lake_table_identity(&sink)?;
        let auto_partition = build_auto_partition_plan(&sink, schema_id, routing_exprs, fe_addr)?;
        let write_format = load_lake_data_write_format()?;
        let path_map =
            resolve_tablet_paths_for_olap_sink(None, fe_addr, &table_identity, &routing.refs)?;
        let shard_infos = starlet_shard_registry::select_infos(&routing.row_routing.tablet_ids);
        let tablet_schema = build_sink_tablet_schema(&sink.schema, schema_id, keys_type)?;
        let (schema_slot_bindings, op_slot_id) =
            resolve_write_slot_bindings(&sink, schema_id, routing_exprs, &tablet_schema)?;
        let partial_mode = PartialUpdateWriteMode::from_thrift(sink.partial_update_mode);
        let merge_condition = sink
            .merge_condition
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(ToString::to_string);
        let column_to_expr_value = resolve_column_to_expr_value_for_write(&sink, schema_id)?;
        let auto_increment = resolve_auto_increment_write_policy(&sink, &tablet_schema, fe_addr)?;

        let mut write_targets = HashMap::with_capacity(routing.row_routing.tablet_ids.len());
        for tablet_id in &routing.row_routing.tablet_ids {
            let tablet_root_path = path_map.get(tablet_id).ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK missing resolved storage path for tablet {}",
                    tablet_id
                )
            })?;
            let target = TabletWriteTarget {
                tablet_id: *tablet_id,
                partition_id: *routing.tablet_to_partition.get(tablet_id).ok_or_else(|| {
                    format!(
                        "OLAP_TABLE_SINK missing partition mapping for tablet {}",
                        tablet_id
                    )
                })?,
                context: {
                    let scheme = classify_scan_paths([tablet_root_path.as_str()])?;
                    let s3_config = match scheme {
                        ScanPathScheme::Local => None,
                        ScanPathScheme::Oss => {
                            let from_shard =
                                shard_infos.get(tablet_id).and_then(|info| info.s3.clone());
                            let from_runtime = if from_shard.is_none() {
                                get_tablet_runtime(*tablet_id)
                                    .ok()
                                    .and_then(|entry| entry.s3_config.clone())
                            } else {
                                None
                            };
                            let inferred = if from_shard.is_none() && from_runtime.is_none() {
                                starlet_shard_registry::infer_s3_config_for_path(tablet_root_path)
                            } else {
                                None
                            };
                            Some(from_shard.or(from_runtime).or(inferred).ok_or_else(|| {
                                format!(
                                    "OLAP_TABLE_SINK missing S3 config for object-store tablet {} (path={})",
                                    tablet_id, tablet_root_path
                                )
                            })?)
                        }
                        ScanPathScheme::Hdfs => {
                            return Err(format!(
                                "OLAP_TABLE_SINK does not support hdfs tablet path yet: tablet_id={} path={}",
                                tablet_id, tablet_root_path
                            ));
                        }
                    };
                    TabletWriteContext {
                        db_id: sink.db_id,
                        table_id: sink.table_id,
                        tablet_id: *tablet_id,
                        tablet_root_path: tablet_root_path.clone(),
                        tablet_schema: tablet_schema.clone(),
                        s3_config,
                        partial_update: PartialUpdateWritePolicy {
                            mode: partial_mode.clone(),
                            merge_condition: merge_condition.clone(),
                            column_to_expr_value: column_to_expr_value.clone(),
                            schema_slot_bindings: schema_slot_bindings.clone(),
                            auto_increment: auto_increment.clone(),
                        },
                    }
                },
            };
            if write_targets.insert(*tablet_id, target).is_some() {
                return Err(format!(
                    "duplicate write target resolved for tablet {}",
                    tablet_id
                ));
            }
        }

        let plan = OlapTableSinkPlan {
            db_id: sink.db_id,
            table_id: sink.table_id,
            table_identity,
            db_name: sink.db_name.clone(),
            table_name: sink.table_name.clone(),
            txn_id: sink.txn_id,
            load_id: PUniqueId {
                hi: sink.load_id.hi,
                lo: sink.load_id.lo,
            },
            write_format,
            tablet_commit_infos: routing.commit_infos,
            write_targets,
            row_routing: routing.row_routing,
            schema_slot_bindings,
            op_slot_id,
            output_projection,
            auto_partition,
        };

        Ok(Self {
            name: "OLAP_TABLE_SINK".to_string(),
            plan: Arc::new(plan),
            finalize_shared: Arc::new(OlapSinkFinalizeSharedState::default()),
        })
    }
}

fn maybe_create_automatic_partitions_for_literal_insert(
    sink: &mut data_sinks::TOlapTableSink,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
    exec_plan: Option<&ExecPlan>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<(), String> {
    if !sink.partition.enable_automatic_partition.unwrap_or(false) {
        return Ok(());
    }
    if sink
        .partition
        .partition_exprs
        .as_ref()
        .is_some_and(|exprs| !exprs.is_empty())
    {
        // For expression partitions, runtime automatic partition discovery should use
        // evaluated partition expr outputs from incoming chunks.
        return Ok(());
    }

    let partition_columns = resolve_partition_columns_for_write(&sink.partition);
    if partition_columns.is_empty() {
        return Ok(());
    }

    let Some(output_exprs) = output_exprs else {
        return Ok(());
    };
    let index_columns = resolve_index_column_names_for_write(sink, schema_id)?;
    if index_columns.is_empty() {
        return Ok(());
    }

    let mut partition_values = Vec::with_capacity(partition_columns.len());
    for partition_col in &partition_columns {
        let Some(column_idx) = index_columns.iter().position(|name| name == partition_col) else {
            return Ok(());
        };
        let Some(expr) = output_exprs.get(column_idx) else {
            return Ok(());
        };
        let Some(value) = extract_partition_literal_value(expr)
            .or_else(|| extract_partition_value_from_exec_plan(expr, exec_plan))
        else {
            info!(
                target: "novarocks::starrocks::sink",
                table_id = sink.table_id,
                txn_id = sink.txn_id,
                partition_column = partition_col,
                column_idx,
                "OLAP_TABLE_SINK skip createPartition: cannot resolve partition value"
            );
            return Ok(());
        };
        partition_values.push(value);
    }

    let fe_addr = resolve_frontend_addr(fe_addr).ok_or_else(|| {
        "OLAP_TABLE_SINK automatic partition cannot resolve FE address".to_string()
    })?;
    info!(
        target: "novarocks::starrocks::sink",
        table_id = sink.table_id,
        txn_id = sink.txn_id,
        partition_values = ?partition_values,
        "OLAP_TABLE_SINK attempting FE createPartition for automatic partition"
    );
    let response = create_automatic_partitions(
        &fe_addr,
        sink.db_id,
        sink.table_id,
        sink.txn_id,
        vec![partition_values],
    )
    .map_err(|e| format!("OLAP_TABLE_SINK precreate automatic partition failed: {e}"))?;
    info!(
        target: "novarocks::starrocks::sink",
        table_id = sink.table_id,
        txn_id = sink.txn_id,
        partitions = response.partitions.as_ref().map(|v| v.len()).unwrap_or(0),
        tablets = response.tablets.as_ref().map(|v| v.len()).unwrap_or(0),
        nodes = response.nodes.as_ref().map(|v| v.len()).unwrap_or(0),
        "OLAP_TABLE_SINK FE createPartition succeeded"
    );

    let mut partition_ids = sink
        .partition
        .partitions
        .iter()
        .map(|part| part.id)
        .collect::<HashSet<_>>();
    for part in response.partitions.unwrap_or_default() {
        if partition_ids.insert(part.id) {
            sink.partition.partitions.push(part);
        }
    }

    let mut tablet_ids = sink
        .location
        .tablets
        .iter()
        .map(|tablet| tablet.tablet_id)
        .collect::<HashSet<_>>();
    for tablet in response.tablets.unwrap_or_default() {
        if tablet_ids.insert(tablet.tablet_id) {
            sink.location.tablets.push(tablet);
        }
    }

    let mut node_ids = sink
        .nodes_info
        .nodes
        .iter()
        .map(|node| node.id)
        .collect::<HashSet<_>>();
    for node in response.nodes.unwrap_or_default() {
        if node_ids.insert(node.id) {
            sink.nodes_info.nodes.push(node);
        }
    }

    Ok(())
}

fn build_auto_partition_plan(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Option<AutomaticPartitionPlan>, String> {
    if !sink.partition.enable_automatic_partition.unwrap_or(false) {
        return Ok(None);
    }
    let has_shadow_partition = sink
        .partition
        .partitions
        .iter()
        .any(|part| part.is_shadow_partition.unwrap_or(false));
    if !has_shadow_partition {
        return Ok(None);
    }
    // Automatic partition tables may carry both visible and shadow partitions.
    // Keep runtime createPartition enabled as long as FE marks this table with
    // shadow partition metadata.

    let partition_column_names = resolve_partition_columns_for_write(&sink.partition);
    if partition_column_names.is_empty() {
        return Ok(None);
    }
    let output_expr_slot_map =
        build_output_expr_slot_name_map_for_write(sink, schema_id, output_exprs)?;
    let slot_name_overrides = if output_expr_slot_map.is_empty() {
        None
    } else {
        Some(&output_expr_slot_map)
    };
    let output_expr_slot_id_overrides = build_output_expr_slot_id_overrides_for_write(
        &sink.schema.slot_descs,
        slot_name_overrides,
    )?;
    let slot_id_overrides = if output_expr_slot_id_overrides.is_empty() {
        None
    } else {
        Some(&output_expr_slot_id_overrides)
    };
    let partition_key_source =
        build_partition_key_source(sink, slot_name_overrides, slot_id_overrides)?;
    let partition_slot_ids = resolve_slot_ids_by_names(
        &sink.schema.slot_descs,
        &partition_column_names,
        "automatic partition column",
        slot_name_overrides,
    )?;
    if partition_slot_ids.is_empty() {
        return Ok(None);
    }
    let candidate_index_ids = sink
        .schema
        .indexes
        .iter()
        .filter(|idx| idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id) == schema_id)
        .map(|idx| idx.id)
        .collect::<HashSet<_>>();
    if candidate_index_ids.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK automatic partition cannot resolve routing index for schema_id={schema_id}"
        ));
    }
    let fe_addr = resolve_frontend_addr(fe_addr).ok_or_else(|| {
        format!(
            "OLAP_TABLE_SINK automatic partition cannot resolve FE address: table_id={} txn_id={}",
            sink.table_id, sink.txn_id
        )
    })?;

    Ok(Some(AutomaticPartitionPlan {
        db_id: sink.db_id,
        table_id: sink.table_id,
        schema_id,
        txn_id: sink.txn_id,
        fe_addr,
        partition_key_source,
        partition_column_names,
        partition_slot_ids,
        candidate_index_ids,
    }))
}

fn resolve_partition_columns_for_write(
    partition: &descriptors::TOlapTablePartitionParam,
) -> Vec<String> {
    let mut cols = partition
        .partition_columns
        .as_ref()
        .map(|values| {
            values
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
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
            return node
                .bool_literal
                .as_ref()
                .map(|v| if v.value { "1" } else { "0" }.to_string());
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
        .output_slots
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
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|v| if v.value(row) { "1" } else { "0" }.to_string()),
        DataType::Date32 => {
            let days_since_epoch = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|v| v.value(row))?;
            let days_from_ce = 719_163_i32.checked_add(days_since_epoch)?;
            let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce)?;
            Some(date.format("%Y-%m-%d").to_string())
        }
        _ => None,
    }
}

fn resolve_frontend_addr(
    fe_addr: Option<&types::TNetworkAddress>,
) -> Option<types::TNetworkAddress> {
    fe_addr.cloned().or_else(disk_report::latest_fe_addr)
}

fn with_frontend_client<T>(
    fe_addr: &types::TNetworkAddress,
    f: impl FnOnce(&mut dyn TFrontendServiceSyncClient) -> Result<T, String>,
) -> Result<T, String> {
    let addr: SocketAddr = format!("{}:{}", fe_addr.hostname, fe_addr.port)
        .parse()
        .map_err(|e| format!("invalid FE address: {e}"))?;
    let stream = TcpStream::connect_timeout(
        &addr,
        Duration::from_secs(FE_AUTOMATIC_PARTITION_TIMEOUT_SECS),
    )
    .map_err(|e| format!("connect FE failed: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_secs(
        FE_AUTOMATIC_PARTITION_TIMEOUT_SECS,
    )));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(
        FE_AUTOMATIC_PARTITION_TIMEOUT_SECS,
    )));
    let _ = stream.set_nodelay(true);

    let channel = TTcpChannel::with_stream(stream);
    let (i_chan, o_chan): (TReadHalf<TTcpChannel>, TWriteHalf<TTcpChannel>) = channel
        .split()
        .map_err(|e| format!("split FE thrift channel failed: {e}"))?;
    let i_trans = TBufferedReadTransport::new(i_chan);
    let o_trans = TBufferedWriteTransport::new(o_chan);
    let i_prot = TBinaryInputProtocol::new(i_trans, true);
    let o_prot = TBinaryOutputProtocol::new(o_trans, true);
    let mut client = FrontendServiceSyncClient::new(i_prot, o_prot);
    f(&mut client)
}

pub(crate) fn create_automatic_partitions(
    fe_addr: &types::TNetworkAddress,
    db_id: i64,
    table_id: i64,
    txn_id: i64,
    partition_values: Vec<Vec<String>>,
) -> Result<frontend_service::TCreatePartitionResult, String> {
    if db_id <= 0 {
        return Err(format!(
            "invalid db_id for automatic partition create: {db_id}"
        ));
    }
    if table_id <= 0 {
        return Err(format!(
            "invalid table_id for automatic partition create: {table_id}"
        ));
    }
    if txn_id <= 0 {
        return Err(format!(
            "invalid txn_id for automatic partition create: {txn_id}"
        ));
    }
    if partition_values.is_empty() {
        return Err("automatic partition values cannot be empty".to_string());
    }

    let request = frontend_service::TCreatePartitionRequest::new(
        Some(txn_id),
        Some(db_id),
        Some(table_id),
        Some(partition_values),
        Some(false),
    );
    let response = with_frontend_client(fe_addr, |client| {
        client
            .create_partition(request)
            .map_err(|e| format!("createPartition RPC failed: {e}"))
    })?;
    let status = response
        .status
        .as_ref()
        .ok_or_else(|| "createPartition response missing status".to_string())?;
    if status.status_code != status_code::TStatusCode::OK {
        let detail = status
            .error_msgs
            .as_ref()
            .map(|v| v.join("; "))
            .unwrap_or_default();
        return Err(format!(
            "createPartition failed: status={:?}, error={detail}",
            status.status_code
        ));
    }
    Ok(response)
}

fn resolve_write_slot_bindings(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
    tablet_schema: &TabletSchemaPb,
) -> Result<(Vec<Option<SlotId>>, Option<SlotId>), String> {
    let slot_by_name = build_slot_name_map(&sink.schema.slot_descs)?;
    let output_expr_slot_map =
        build_output_expr_slot_name_map_for_write(sink, schema_id, output_exprs)?;
    let output_expr_slot_by_ordinal = resolve_output_expr_slot_ids_for_write(output_exprs)?;
    let schema_slot_by_ordinal = resolve_schema_slot_ids_by_ordinal(&sink.schema.slot_descs)?;
    let index_column_names = resolve_index_column_names_for_write(sink, schema_id)?;
    let allow_output_ordinal_fallback = output_expr_slot_map.is_empty()
        && output_expr_slot_by_ordinal.len() == tablet_schema.column.len();
    let allow_schema_ordinal_fallback =
        slot_by_name.is_empty() && schema_slot_by_ordinal.len() == tablet_schema.column.len();
    let mut out = Vec::with_capacity(tablet_schema.column.len());
    for (idx, col) in tablet_schema.column.iter().enumerate() {
        let name = col
            .name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK tablet schema column missing name: schema_index={}",
                    idx
                )
            })?
            .to_ascii_lowercase();
        let slot_id = output_expr_slot_map
            .get(&name)
            .copied()
            .or_else(|| slot_by_name.get(&name).copied())
            .or_else(|| {
                index_column_names
                    .get(idx)
                    .and_then(|column_name| output_expr_slot_map.get(column_name).copied())
            })
            .or_else(|| {
                index_column_names
                    .get(idx)
                    .and_then(|column_name| slot_by_name.get(column_name).copied())
            })
            .or_else(|| {
                allow_output_ordinal_fallback
                    .then(|| output_expr_slot_by_ordinal.get(idx).and_then(|v| *v))
                    .flatten()
            })
            .or_else(|| {
                allow_schema_ordinal_fallback
                    .then(|| schema_slot_by_ordinal.get(idx).and_then(|v| *v))
                    .flatten()
            });
        out.push(slot_id);
    }
    // Prefer resolved output expression slot for __op, and fallback to slot descriptors.
    // In DELETE plans, slot descriptor ids can be tuple-local while output expr slot ids
    // are from upstream tuple, so output_expr mapping is the reliable source.
    let op_slot_id = output_expr_slot_map
        .get(LOAD_OP_COLUMN)
        .copied()
        .or_else(|| slot_by_name.get(LOAD_OP_COLUMN).copied());
    if op_slot_id.is_none() {
        let slot_desc_summary = sink
            .schema
            .slot_descs
            .iter()
            .map(|slot| {
                format!(
                    "id={:?},col_name={:?},col_physical_name={:?},col_unique_id={:?}",
                    slot.id, slot.col_name, slot.col_physical_name, slot.col_unique_id
                )
            })
            .collect::<Vec<_>>();
        let output_expr_summary = output_exprs
            .unwrap_or(&[])
            .iter()
            .map(|expr| {
                let Some(root) = expr.nodes.first() else {
                    return "empty".to_string();
                };
                let slot_id = root.slot_ref.as_ref().map(|slot| slot.slot_id);
                format!("node_type={:?},slot_id={:?}", root.node_type, slot_id)
            })
            .collect::<Vec<_>>();
        info!(
            target: "novarocks::sink",
            table_id = sink.table_id,
            schema_id,
            index_column_names = ?index_column_names,
            slot_descs = ?slot_desc_summary,
            output_exprs = ?output_expr_summary,
            "OLAP_TABLE_SINK cannot resolve __op slot from sink schema.slot_descs"
        );
    }
    let has_missing_key_binding =
        tablet_schema.column.iter().enumerate().any(|(idx, col)| {
            col.is_key.unwrap_or(false) && out.get(idx).and_then(|v| *v).is_none()
        });
    if sink.keys_type == Some(types::TKeysType::PRIMARY_KEYS) {
        let slot_desc_summary = sink
            .schema
            .slot_descs
            .iter()
            .map(|slot| {
                format!(
                    "id={:?},col_name={:?},col_physical_name={:?},col_unique_id={:?}",
                    slot.id, slot.col_name, slot.col_physical_name, slot.col_unique_id
                )
            })
            .collect::<Vec<_>>();
        let output_expr_summary = output_exprs
            .unwrap_or(&[])
            .iter()
            .map(|expr| {
                let Some(root) = expr.nodes.first() else {
                    return "empty".to_string();
                };
                let slot_id = root.slot_ref.as_ref().map(|slot| slot.slot_id);
                format!("node_type={:?},slot_id={:?}", root.node_type, slot_id)
            })
            .collect::<Vec<_>>();
        info!(
            target: "novarocks::sink",
            table_id = sink.table_id,
            schema_id,
            index_column_names = ?index_column_names,
            slot_descs = ?slot_desc_summary,
            output_exprs = ?output_expr_summary,
            schema_slot_bindings = ?out,
            op_slot_id = ?op_slot_id,
            "OLAP_TABLE_SINK resolved write slot bindings for primary key table"
        );
    }
    if has_missing_key_binding {
        let slot_desc_summary = sink
            .schema
            .slot_descs
            .iter()
            .map(|slot| {
                format!(
                    "id={:?},col_name={:?},col_physical_name={:?},col_unique_id={:?}",
                    slot.id, slot.col_name, slot.col_physical_name, slot.col_unique_id
                )
            })
            .collect::<Vec<_>>();
        let output_expr_summary = output_exprs
            .unwrap_or(&[])
            .iter()
            .map(|expr| {
                let Some(root) = expr.nodes.first() else {
                    return "empty".to_string();
                };
                let slot_id = root.slot_ref.as_ref().map(|slot| slot.slot_id);
                format!("node_type={:?},slot_id={:?}", root.node_type, slot_id)
            })
            .collect::<Vec<_>>();
        info!(
            target: "novarocks::sink",
            table_id = sink.table_id,
            schema_id,
            index_column_names = ?index_column_names,
            slot_descs = ?slot_desc_summary,
            output_exprs = ?output_expr_summary,
            schema_slot_bindings = ?out,
            op_slot_id = ?op_slot_id,
            "OLAP_TABLE_SINK resolved write slot bindings contain missing key columns"
        );
    }
    Ok((out, op_slot_id))
}

fn resolve_column_to_expr_value_for_write(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
) -> Result<HashMap<String, String>, String> {
    let index = sink
        .schema
        .indexes
        .iter()
        .find(|idx| idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id) == schema_id)
        .ok_or_else(|| {
            format!("OLAP_TABLE_SINK cannot resolve schema index for schema_id={schema_id}")
        })?;
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
    Ok(out)
}

fn resolve_auto_increment_write_policy(
    sink: &data_sinks::TOlapTableSink,
    tablet_schema: &TabletSchemaPb,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<AutoIncrementWritePolicy, String> {
    let null_expr_in_auto_increment = sink.null_expr_in_auto_increment.unwrap_or(false);
    let miss_auto_increment_column = sink.miss_auto_increment_column.unwrap_or(false);

    let mut auto_increment_column_idx = None;
    for (idx, column) in tablet_schema.column.iter().enumerate() {
        if !column.is_auto_increment.unwrap_or(false) {
            continue;
        }
        if auto_increment_column_idx.is_some() {
            return Err(format!(
                "OLAP_TABLE_SINK found multiple auto_increment columns in tablet schema: schema_id={:?}",
                tablet_schema.id
            ));
        }
        auto_increment_column_idx = Some(idx);
    }
    let auto_increment_column_name = auto_increment_column_idx.and_then(|idx| {
        tablet_schema
            .column
            .get(idx)
            .and_then(|column| column.name.as_ref())
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
    });
    let auto_increment_in_sort_key = auto_increment_column_idx.is_some_and(|idx| {
        tablet_schema
            .sort_key_idxes
            .iter()
            .filter_map(|v| usize::try_from(*v).ok())
            .any(|sort_idx| sort_idx == idx)
    });

    Ok(AutoIncrementWritePolicy {
        null_expr_in_auto_increment,
        miss_auto_increment_column,
        auto_increment_column_idx,
        auto_increment_column_name,
        auto_increment_in_sort_key,
        fe_addr: resolve_frontend_addr(fe_addr),
    })
}

fn build_output_expr_slot_name_map_for_write(
    sink: &data_sinks::TOlapTableSink,
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
    // Prefer slot descriptor order because it matches sink output tuple in DELETE/partial plans.
    // Only consume one output expr after we find a valid column slot, so hidden/empty slots
    // cannot shift all subsequent bindings.
    let mut expr_iter = output_exprs.iter();
    for slot_desc in &sink.schema.slot_descs {
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

    let column_names = resolve_index_column_names_for_write(sink, schema_id)?;
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

fn build_output_expr_slot_id_overrides_for_write(
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

fn build_output_projection_plan(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: Option<&[exprs::TExpr]>,
    layout: Option<&Layout>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Option<SinkOutputProjectionPlan>, String> {
    let Some(output_exprs) = output_exprs.filter(|exprs| !exprs.is_empty()) else {
        return Ok(None);
    };
    // Slot-ref-only output exprs do not need an extra projection stage.
    // Keeping the original upstream slot ids avoids accidental remapping when
    // FE expr order does not match sink slot descriptor order.
    if output_exprs_are_plain_slot_refs(output_exprs) {
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
        ));
    }

    let mut arena = ExprArena::default();
    let mut expr_ids = Vec::with_capacity(output_exprs.len());
    for expr in output_exprs {
        let expr_id = lower_t_expr(expr, &mut arena, layout, last_query_id, fe_addr)?;
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

fn resolve_schema_slot_ids_by_ordinal(
    slot_descs: &[descriptors::TSlotDescriptor],
) -> Result<Vec<Option<SlotId>>, String> {
    let mut out = Vec::with_capacity(slot_descs.len());
    for slot in slot_descs {
        let slot_id = match slot.id {
            Some(id) if id >= 0 => SlotId::try_from(id).ok(),
            None => None,
            _ => None,
        };
        out.push(slot_id);
    }
    Ok(out)
}

fn resolve_index_column_names_for_write(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
) -> Result<Vec<String>, String> {
    let index = sink
        .schema
        .indexes
        .iter()
        .find(|idx| idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id) == schema_id)
        .ok_or_else(|| {
            format!("OLAP_TABLE_SINK cannot resolve schema index for schema_id={schema_id}")
        })?;

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
    Ok(column_names)
}

impl OperatorFactory for OlapTableSinkFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        self.finalize_shared.register_driver();
        Box::new(OlapTableSinkOperator::new_with_shared(
            self.name.clone(),
            Arc::clone(&self.plan),
            driver_id,
            Arc::clone(&self.finalize_shared),
        ))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

fn build_lake_table_identity(
    sink: &data_sinks::TOlapTableSink,
) -> Result<LakeTableIdentity, String> {
    let db_name = sink
        .db_name
        .as_ref()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "OLAP_TABLE_SINK missing db_name".to_string())?
        .to_string();
    let table_name = sink
        .table_name
        .as_ref()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "OLAP_TABLE_SINK missing table_name".to_string())?
        .to_string();
    let schema_id = resolve_schema_id(&sink.schema)?;
    Ok(LakeTableIdentity {
        catalog: load_lake_catalog()?,
        db_name,
        table_name,
        db_id: sink.db_id,
        table_id: sink.table_id,
        schema_id,
    })
}

fn load_lake_catalog() -> Result<String, String> {
    let cfg = novarocks_app_config().map_err(|e| e.to_string())?;
    let catalog = cfg.starrocks.fe_catalog.trim();
    if catalog.is_empty() {
        return Err("starrocks.fe_catalog cannot be empty".to_string());
    }
    Ok(catalog.to_string())
}

fn load_lake_data_write_format() -> Result<StarRocksWriteFormat, String> {
    let cfg = novarocks_app_config().map_err(|e| e.to_string())?;
    StarRocksWriteFormat::parse(&cfg.starrocks.lake_data_write_format)
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

fn map_sink_keys_type(keys_type: Option<types::TKeysType>) -> Result<KeysType, String> {
    match keys_type.unwrap_or(types::TKeysType::DUP_KEYS) {
        types::TKeysType::DUP_KEYS => Ok(KeysType::DupKeys),
        types::TKeysType::AGG_KEYS => Ok(KeysType::AggKeys),
        types::TKeysType::PRIMARY_KEYS => Ok(KeysType::PrimaryKeys),
        types::TKeysType::UNIQUE_KEYS => Ok(KeysType::UniqueKeys),
        other => Err(format!(
            "OLAP_TABLE_SINK does not support keys_type={:?} in simplified path",
            other
        )),
    }
}

#[allow(dead_code)]
fn collect_required_tablets(partition: &descriptors::TOlapTablePartitionParam) -> Vec<i64> {
    let mut tablets = BTreeSet::new();
    for part in &partition.partitions {
        for index in &part.indexes {
            for tablet_id in &index.tablet_ids {
                tablets.insert(*tablet_id);
            }
        }
    }
    tablets.into_iter().collect()
}
