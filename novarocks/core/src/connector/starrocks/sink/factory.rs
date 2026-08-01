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
use std::sync::Arc;

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::connector::starrocks::fe_v2_meta::{
    LakeTableIdentity, LakeTabletPartitionRef, resolve_tablet_paths_for_olap_sink,
};
use crate::connector::starrocks::fs_access::resolve_tablet_root;
use crate::connector::starrocks::lake::TabletWriteContext;
use crate::connector::starrocks::lake::context::{
    AutoIncrementWritePolicy, PartialUpdateWritePolicy, get_tablet_runtime,
};
use crate::connector::starrocks::ports::{
    AutomaticPartitionKey, AutomaticPartitionRequest, AutomaticPartitionResult, SinkFrontendAddress,
};
use crate::connector::starrocks::schema::{StarRocksKeysType, StarRocksTabletSchema};
use crate::connector::starrocks::sink::operator::{
    OlapSinkFinalizeSharedState, OlapTableSinkOperator,
};
use crate::connector::starrocks::sink::partition_key::{
    PartitionKeySource, build_partition_key_source, build_slot_name_map, resolve_slot_ids_by_names,
};
use crate::connector::starrocks::sink::plan::{
    CreatePartitionResult, FrontendAddress, SinkIndexDescriptor, SinkNodeInfo,
    SinkOutputProjectionPlan, SinkPartitionEntry, SinkPartitionIndex, SinkPredicatePlan,
    SinkSchemaDescriptor, SinkSlotDescriptor, SinkTabletLocation, StarRocksSinkDescriptor,
    StarRocksSinkFactoryInput,
};
use crate::connector::starrocks::sink::routing::{RowRoutingPlan, build_sink_routing_for_index_id};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::formats::starrocks::writer::StarRocksWriteFormat;
use crate::novarocks_config::config as novarocks_app_config;
use crate::novarocks_logging::info;
use crate::runtime::sink_commit::TabletCommitInfo;
use crate::runtime::starlet_shard_registry::{self, S3StoreConfig};

const LOAD_OP_COLUMN: &str = "__op";
pub(crate) const STARROCKS_DEFAULT_PARTITION_VALUE: &str = "__STARROCKS_DEFAULT_PARTITION__";

#[derive(Clone)]
pub(crate) struct OlapTableSinkFactory {
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
    pub(crate) load_id: UniqueId,
    pub(crate) write_format: StarRocksWriteFormat,
    pub(crate) tablet_commit_infos: Vec<TabletCommitInfo>,
    pub(crate) write_targets: HashMap<i64, TabletWriteTarget>,
    pub(crate) row_routing: RowRoutingPlan,
    pub(crate) schema_slot_bindings: Vec<Option<SlotId>>,
    pub(crate) op_slot_id: Option<SlotId>,
    pub(crate) index_write_plans: Vec<SinkIndexWritePlan>,
    pub(crate) output_projection: Option<SinkOutputProjectionPlan>,
    pub(crate) auto_partition: Option<AutomaticPartitionPlan>,
    /// Slot ID of the auto-increment column **in the sink chunk** (output projection space).
    /// Resolved from schema_slot_bindings using auto_increment_column_idx.
    /// Used to fill auto-increment NULLs before hash distribution, matching StarRocks
    /// C++ BE behavior where IDs are assigned in original INSERT row order.
    pub(crate) auto_increment_output_slot_id: Option<SlotId>,
    pub(crate) null_expr_in_auto_increment: bool,
    pub(crate) miss_auto_increment_column: bool,
    pub(crate) starlet_metadata_provider:
        Option<Arc<dyn crate::connector::starrocks::ports::StarletMetadataProvider>>,
}

#[derive(Clone)]
pub(crate) struct SinkIndexWritePlan {
    pub(crate) index_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) row_routing: RowRoutingPlan,
    pub(crate) write_targets: HashMap<i64, TabletWriteTarget>,
    pub(crate) schema_slot_bindings: Vec<Option<SlotId>>,
    pub(crate) op_slot_id: Option<SlotId>,
    pub(crate) where_clause: Option<SinkPredicatePlan>,
}

#[derive(Clone)]
struct SinkWriteIndexSelection {
    index_id: i64,
    schema_id: i64,
    where_clause: Option<SinkPredicatePlan>,
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
    pub(crate) txn_id: i64,
    pub(crate) dynamic_overwrite: bool,
    pub(crate) fe_addr: FrontendAddress,
    pub(crate) frontend_provider: Arc<dyn crate::connector::starrocks::ports::SinkFrontendProvider>,
    pub(crate) partition_key_source: PartitionKeySource,
    pub(crate) partition_column_names: Vec<String>,
    pub(crate) partition_slot_ids: Vec<SlotId>,
}

impl OlapTableSinkFactory {
    pub(crate) fn try_new(input: StarRocksSinkFactoryInput) -> Result<Self, String> {
        let StarRocksSinkFactoryInput {
            name,
            mut descriptor,
            output_projection,
            output_expr_slot_name_map,
            output_expr_slot_ids,
            literal_partition_values,
        } = input;
        if !descriptor.is_lake_table {
            return Err(
                "OLAP_TABLE_SINK only supports shared-data lake table in novarocks phase-1 write path"
                    .to_string(),
            );
        }
        let write_indexes = resolve_sink_write_index_selections(&descriptor)?;
        let primary_write_index = write_indexes.first().ok_or_else(|| {
            "OLAP_TABLE_SINK cannot resolve any write index from sink schema/partition metadata"
                .to_string()
        })?;
        let schema_id = primary_write_index.schema_id;
        maybe_create_automatic_partitions_for_literal_insert(
            &mut descriptor,
            schema_id,
            literal_partition_values,
        )?;
        let routing_output_expr_slot_name_map = if output_projection.is_some() {
            HashMap::new()
        } else {
            output_expr_slot_name_map.clone()
        };
        let table_identity = build_lake_table_identity_with_schema_id(&descriptor, schema_id)?;
        let auto_partition = build_auto_partition_plan(
            &descriptor,
            &write_indexes,
            schema_id,
            &routing_output_expr_slot_name_map,
        )?;
        let write_format = load_lake_data_write_format()?;

        let mut index_routings = Vec::with_capacity(write_indexes.len());
        let mut all_refs = Vec::new();
        let mut all_tablets = BTreeSet::new();
        let mut tablet_commit_infos = Vec::new();
        let mut commit_info_keys = HashSet::<(i64, i64)>::new();
        for index in &write_indexes {
            let routing = build_sink_routing_for_index_id(
                &descriptor,
                index.index_id,
                index.schema_id,
                &routing_output_expr_slot_name_map,
            )?;
            if routing.commit_infos.is_empty() {
                return Err(format!(
                    "OLAP_TABLE_SINK resolved empty tablet commit infos for index_id={} schema_id={}",
                    index.index_id, index.schema_id
                ));
            }
            if descriptor.keys_type == StarRocksKeysType::Primary {
                info!(
                    target: "novarocks::sink",
                    table_id = descriptor.table_id,
                    schema_id = index.schema_id,
                    index_id = index.index_id,
                    distributed_slot_ids = ?routing.row_routing.distributed_slot_ids,
                    partition_key_len = routing.row_routing.partition_key_len,
                    tablet_count = routing.row_routing.tablet_ids.len(),
                    "OLAP_TABLE_SINK built row routing for primary key table"
                );
            }
            for tablet_id in &routing.row_routing.tablet_ids {
                all_tablets.insert(*tablet_id);
                all_refs.push(LakeTabletPartitionRef {
                    tablet_id: *tablet_id,
                });
            }
            for commit in &routing.commit_infos {
                if commit_info_keys.insert((commit.tablet_id, commit.backend_id)) {
                    tablet_commit_infos.push(commit.clone());
                }
            }
            index_routings.push((index.clone(), routing));
        }
        if all_refs.is_empty() {
            return Err("OLAP_TABLE_SINK resolved empty tablet refs for write targets".to_string());
        }
        let path_map = resolve_tablet_paths_for_olap_sink(
            None,
            &table_identity,
            &all_refs,
            descriptor.starlet_metadata_provider.as_deref(),
        )?;
        let shard_infos =
            starlet_shard_registry::select_infos(&all_tablets.into_iter().collect::<Vec<_>>());

        let partial_mode = descriptor.partial_update_mode.clone();
        let merge_condition = descriptor.merge_condition.clone();
        let mut index_write_plans = Vec::with_capacity(index_routings.len());
        for (index, routing) in index_routings {
            let index_descriptor = resolve_index_descriptor(&descriptor.schema, index.schema_id)?;
            let sink_tablet_schema = index_descriptor.tablet_schema.clone();
            let tablet_schema = resolve_effective_tablet_schema_for_index(
                descriptor.table_id,
                index.index_id,
                index.schema_id,
                &routing.row_routing.tablet_ids,
                &sink_tablet_schema,
            );
            let projected_output_slot_ids = output_projection
                .as_ref()
                .map(|plan| plan.output_slot_ids.as_slice());
            let (schema_slot_bindings, op_slot_id) = resolve_write_slot_bindings(
                &descriptor,
                index.schema_id,
                &output_expr_slot_name_map,
                &output_expr_slot_ids,
                projected_output_slot_ids,
                &tablet_schema,
            )?;
            let column_to_expr_value = index_descriptor.column_to_expr_value.clone();
            let auto_increment = resolve_auto_increment_write_policy(&descriptor, &tablet_schema)?;

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
                        let from_shard =
                            shard_infos.get(tablet_id).and_then(|info| info.s3.clone());
                        let from_runtime = if from_shard.is_none() {
                            get_tablet_runtime(*tablet_id)
                                .ok()
                                .and_then(|entry| entry.s3_config.clone())
                        } else {
                            None
                        };
                        let s3_config = resolve_s3_for_sink_tablet(
                            *tablet_id,
                            tablet_root_path,
                            from_shard,
                            from_runtime,
                        )?;
                        TabletWriteContext {
                            db_id: descriptor.db_id,
                            table_id: descriptor.table_id,
                            tablet_id: *tablet_id,
                            tablet_root_path: tablet_root_path.clone(),
                            tablet_schema: tablet_schema.clone(),
                            s3_config,
                            storage_metadata_provider: descriptor.storage_metadata_provider.clone(),
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
                        "duplicate write target resolved for tablet {} in index_id={}",
                        tablet_id, index.index_id
                    ));
                }
            }
            index_write_plans.push(SinkIndexWritePlan {
                index_id: index.index_id,
                schema_id: index.schema_id,
                row_routing: routing.row_routing,
                write_targets,
                schema_slot_bindings,
                op_slot_id,
                where_clause: index.where_clause.clone(),
            });
        }
        let primary_plan = index_write_plans.first().cloned().ok_or_else(|| {
            "OLAP_TABLE_SINK resolved empty index write plans after routing".to_string()
        })?;

        let plan = OlapTableSinkPlan {
            db_id: descriptor.db_id,
            table_id: descriptor.table_id,
            table_identity,
            db_name: descriptor.db_name.clone(),
            table_name: descriptor.table_name.clone(),
            txn_id: descriptor.txn_id,
            load_id: descriptor.load_id,
            write_format,
            tablet_commit_infos,
            write_targets: primary_plan.write_targets.clone(),
            row_routing: primary_plan.row_routing.clone(),
            schema_slot_bindings: primary_plan.schema_slot_bindings.clone(),
            op_slot_id: primary_plan.op_slot_id,
            index_write_plans,
            output_projection,
            auto_partition,
            auto_increment_output_slot_id: {
                // Find the output-projection slot ID for the auto-increment column
                // by looking up the schema_slot_bindings at the auto_increment_column_idx.
                let auto_idx = primary_plan.write_targets.values().next().and_then(|t| {
                    t.context
                        .partial_update
                        .auto_increment
                        .auto_increment_column_idx
                });
                auto_idx.and_then(|idx| primary_plan.schema_slot_bindings.get(idx).and_then(|s| *s))
            },
            null_expr_in_auto_increment: descriptor.null_expr_in_auto_increment,
            miss_auto_increment_column: descriptor.miss_auto_increment_column,
            starlet_metadata_provider: descriptor.starlet_metadata_provider.clone(),
        };

        Ok(Self {
            name,
            plan: Arc::new(plan),
            finalize_shared: Arc::new(OlapSinkFinalizeSharedState::default()),
        })
    }
}

fn maybe_create_automatic_partitions_for_literal_insert(
    descriptor: &mut StarRocksSinkDescriptor,
    schema_id: i64,
    literal_partition_values: Option<Vec<String>>,
) -> Result<(), String> {
    if !descriptor.partition.enable_automatic_partition {
        return Ok(());
    }
    if descriptor.partition.partition_exprs.is_some() {
        // For expression partitions, runtime automatic partition discovery should use
        // evaluated partition expr outputs from incoming chunks.
        return Ok(());
    }

    if descriptor.partition.partition_columns.is_empty() {
        return Ok(());
    }

    let Some(partition_values) = literal_partition_values else {
        return Ok(());
    };
    let fe_addr = descriptor.frontend.as_ref().ok_or_else(|| {
        "OLAP_TABLE_SINK automatic partition cannot resolve FE address".to_string()
    })?;
    info!(
        target: "novarocks::starrocks::sink",
        table_id = descriptor.table_id,
        txn_id = descriptor.txn_id,
        schema_id,
        partition_values = ?partition_values,
        "OLAP_TABLE_SINK attempting FE createPartition for automatic partition"
    );
    let provider = descriptor.frontend_provider.as_ref().ok_or_else(|| {
        "OLAP_TABLE_SINK automatic partition requires StarRocks FE capability".to_string()
    })?;
    let response = provider
        .create_automatic_partitions(&AutomaticPartitionRequest {
            frontend: SinkFrontendAddress {
                host: fe_addr.hostname.clone(),
                port: fe_addr.port,
            },
            db_id: descriptor.db_id,
            table_id: descriptor.table_id,
            txn_id: descriptor.txn_id,
            is_temp: descriptor.dynamic_overwrite,
            partition_values: vec![partition_values],
        })
        .map(automatic_partition_result_from_port)
        .map_err(|e| format!("OLAP_TABLE_SINK precreate automatic partition failed: {e}"))?;
    info!(
        target: "novarocks::starrocks::sink",
        table_id = descriptor.table_id,
        txn_id = descriptor.txn_id,
        partitions = response.partitions.len(),
        tablets = response.tablets.len(),
        nodes = response.nodes.len(),
        "OLAP_TABLE_SINK FE createPartition succeeded"
    );

    let mut partition_ids = descriptor
        .partition
        .partitions
        .iter()
        .map(|part| part.partition_id)
        .collect::<HashSet<_>>();
    for part in response.partitions {
        if partition_ids.insert(part.partition_id) {
            descriptor.partition.partitions.push(part);
        }
    }

    let mut tablet_ids = descriptor
        .location
        .tablets
        .iter()
        .map(|tablet| tablet.tablet_id)
        .collect::<HashSet<_>>();
    for tablet in response.tablets {
        if tablet_ids.insert(tablet.tablet_id) {
            descriptor.location.tablets.push(tablet);
        }
    }

    let mut node_ids = descriptor
        .nodes
        .nodes
        .iter()
        .map(|node| node.id)
        .collect::<HashSet<_>>();
    for node in response.nodes {
        if node_ids.insert(node.id) {
            descriptor.nodes.nodes.push(node);
        }
    }

    Ok(())
}

pub(crate) fn automatic_partition_result_from_port(
    result: AutomaticPartitionResult,
) -> CreatePartitionResult {
    CreatePartitionResult {
        partitions: result
            .partitions
            .into_iter()
            .map(|partition| SinkPartitionEntry {
                partition_id: partition.partition_id,
                is_shadow: partition.is_shadow,
                indexes: partition
                    .indexes
                    .into_iter()
                    .map(|index| SinkPartitionIndex {
                        index_id: index.index_id,
                        tablet_ids: index.tablet_ids,
                    })
                    .collect(),
                start_key: partition
                    .start_key
                    .map(automatic_partition_key_list_from_port),
                end_key: partition
                    .end_key
                    .map(automatic_partition_key_list_from_port),
                in_keys: partition
                    .in_keys
                    .into_iter()
                    .map(automatic_partition_key_list_from_port)
                    .collect(),
            })
            .collect(),
        tablets: result
            .tablets
            .into_iter()
            .map(|tablet| SinkTabletLocation {
                tablet_id: tablet.tablet_id,
                node_ids: tablet.node_ids,
            })
            .collect(),
        nodes: result
            .nodes
            .into_iter()
            .map(|node| SinkNodeInfo {
                id: node.id,
                option: node.option,
            })
            .collect(),
    }
}

fn automatic_partition_key_list_from_port(
    keys: Vec<AutomaticPartitionKey>,
) -> Vec<crate::connector::starrocks::sink::partition_key::PartitionKeyValue> {
    keys
        .into_iter()
        .map(|key| match key {
            AutomaticPartitionKey::Null => {
                crate::connector::starrocks::sink::partition_key::PartitionKeyValue::Null
            }
            AutomaticPartitionKey::Bool(value) => {
                crate::connector::starrocks::sink::partition_key::PartitionKeyValue::Bool(value)
            }
            AutomaticPartitionKey::Int(value) => {
                crate::connector::starrocks::sink::partition_key::PartitionKeyValue::Int(value)
            }
            AutomaticPartitionKey::Date32(value) => {
                crate::connector::starrocks::sink::partition_key::PartitionKeyValue::Date32(value)
            }
            AutomaticPartitionKey::TimestampMicros(value) => {
                crate::connector::starrocks::sink::partition_key::PartitionKeyValue::TimestampMicros(value)
            }
            AutomaticPartitionKey::Decimal { value, scale } => {
                crate::connector::starrocks::sink::partition_key::PartitionKeyValue::Decimal {
                    value,
                    scale,
                }
            }
            AutomaticPartitionKey::Utf8(value) => {
                crate::connector::starrocks::sink::partition_key::PartitionKeyValue::Utf8(value)
            }
            AutomaticPartitionKey::Binary(value) => {
                crate::connector::starrocks::sink::partition_key::PartitionKeyValue::Binary(value)
            }
        })
        .collect()
}

fn build_auto_partition_plan(
    descriptor: &StarRocksSinkDescriptor,
    write_indexes: &[SinkWriteIndexSelection],
    schema_id: i64,
    output_expr_slot_name_map: &HashMap<String, SlotId>,
) -> Result<Option<AutomaticPartitionPlan>, String> {
    if !descriptor.partition.enable_automatic_partition {
        return Ok(None);
    }
    // StarRocks BE keeps automatic partition routing enabled for the whole load,
    // even when FE only opens a subset of partitions in the initial sink metadata.
    // Existing partitions may therefore be missing from initial sink partition metadata
    // and need to be recovered through createPartition reuse semantics at runtime.
    // Gating this path on shadow partitions breaks insert-select/reuse flows once
    // FE stops sending shadow metadata after the first load.

    let partition_column_names = descriptor.partition.partition_columns.clone();
    if partition_column_names.is_empty() {
        return Ok(None);
    }
    let slot_name_overrides = if output_expr_slot_name_map.is_empty() {
        None
    } else {
        Some(output_expr_slot_name_map)
    };
    let partition_key_source = build_partition_key_source(
        &descriptor.partition,
        &descriptor.schema,
        slot_name_overrides,
    )?;
    let partition_slot_ids = resolve_slot_ids_by_names(
        &descriptor.schema.slot_descs,
        &partition_column_names,
        "automatic partition column",
        slot_name_overrides,
    )?;
    if partition_slot_ids.is_empty() {
        return Ok(None);
    }
    let candidate_index_ids = write_indexes
        .iter()
        .map(|index| index.index_id)
        .collect::<HashSet<_>>();
    if candidate_index_ids.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK automatic partition cannot resolve routing index for schema_id={schema_id}"
        ));
    }
    let fe_addr = descriptor.frontend.clone().ok_or_else(|| {
        format!(
            "OLAP_TABLE_SINK automatic partition cannot resolve FE address: table_id={} txn_id={}",
            descriptor.table_id, descriptor.txn_id
        )
    })?;
    let frontend_provider = descriptor.frontend_provider.clone().ok_or_else(|| {
        format!(
            "OLAP_TABLE_SINK automatic partition requires StarRocks FE capability: table_id={} txn_id={}",
            descriptor.table_id, descriptor.txn_id
        )
    })?;

    Ok(Some(AutomaticPartitionPlan {
        db_id: descriptor.db_id,
        table_id: descriptor.table_id,
        txn_id: descriptor.txn_id,
        dynamic_overwrite: descriptor.dynamic_overwrite,
        fe_addr,
        frontend_provider,
        partition_key_source,
        partition_column_names,
        partition_slot_ids,
    }))
}

fn resolve_write_slot_bindings(
    descriptor: &StarRocksSinkDescriptor,
    schema_id: i64,
    output_expr_slot_map: &HashMap<String, SlotId>,
    output_expr_slot_ids: &[Option<SlotId>],
    projected_output_slot_ids: Option<&[SlotId]>,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<(Vec<Option<SlotId>>, Option<SlotId>), String> {
    let has_hidden_op_slot = descriptor.keys_type == StarRocksKeysType::Primary
        && descriptor.schema.slot_descs.iter().any(|slot| {
            slot.col_name
                .as_deref()
                .map(str::trim)
                .is_some_and(|name| name.eq_ignore_ascii_case(LOAD_OP_COLUMN))
        });
    let output_expr_slot_by_ordinal = filter_hidden_op_slot_ids_by_ordinal(
        &descriptor.schema.slot_descs,
        output_expr_slot_ids.to_vec(),
        has_hidden_op_slot,
    );
    let slot_by_name = build_slot_name_map(&descriptor.schema.slot_descs)?;
    let projected_output_slot_by_ordinal = filter_hidden_op_slot_ids_by_ordinal(
        &descriptor.schema.slot_descs,
        projected_output_slot_ids
            .map(|slot_ids| slot_ids.iter().copied().map(Some).collect::<Vec<_>>())
            .unwrap_or_default(),
        has_hidden_op_slot,
    );
    let schema_slot_by_ordinal = filter_hidden_op_slot_ids_by_ordinal(
        &descriptor.schema.slot_descs,
        resolve_schema_slot_ids_by_ordinal(&descriptor.schema.slot_descs),
        has_hidden_op_slot,
    );
    let index_column_names = resolve_index_column_names_for_write(&descriptor.schema, schema_id)?;
    let allow_output_ordinal_fallback =
        output_expr_slot_by_ordinal.len() == tablet_schema.column.len();
    let allow_projected_ordinal_fallback =
        projected_output_slot_by_ordinal.len() == tablet_schema.column.len();
    let allow_schema_ordinal_fallback = schema_slot_by_ordinal.len() == tablet_schema.column.len();
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
                allow_projected_ordinal_fallback
                    .then(|| projected_output_slot_by_ordinal.get(idx).and_then(|v| *v))
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
        let slot_desc_summary = descriptor
            .schema
            .slot_descs
            .iter()
            .map(|slot| {
                format!(
                    "id={:?},col_name={:?},col_physical_name={:?}",
                    slot.id, slot.col_name, slot.col_physical_name
                )
            })
            .collect::<Vec<_>>();
        info!(
            target: "novarocks::sink",
            table_id = descriptor.table_id,
            schema_id,
            index_column_names = ?index_column_names,
            slot_descs = ?slot_desc_summary,
            output_expr_slot_ids = ?output_expr_slot_ids,
            "OLAP_TABLE_SINK cannot resolve __op slot from sink schema.slot_descs"
        );
    }
    let has_missing_key_binding =
        tablet_schema.column.iter().enumerate().any(|(idx, col)| {
            col.is_key.unwrap_or(false) && out.get(idx).and_then(|v| *v).is_none()
        });
    if descriptor.keys_type == StarRocksKeysType::Primary {
        let slot_desc_summary = descriptor
            .schema
            .slot_descs
            .iter()
            .map(|slot| {
                format!(
                    "id={:?},col_name={:?},col_physical_name={:?}",
                    slot.id, slot.col_name, slot.col_physical_name
                )
            })
            .collect::<Vec<_>>();
        info!(
            target: "novarocks::sink",
            table_id = descriptor.table_id,
            schema_id,
            index_column_names = ?index_column_names,
            slot_descs = ?slot_desc_summary,
            output_expr_slot_ids = ?output_expr_slot_ids,
            schema_slot_bindings = ?out,
            op_slot_id = ?op_slot_id,
            "OLAP_TABLE_SINK resolved write slot bindings for primary key table"
        );
    }
    if has_missing_key_binding {
        let slot_desc_summary = descriptor
            .schema
            .slot_descs
            .iter()
            .map(|slot| {
                format!(
                    "id={:?},col_name={:?},col_physical_name={:?}",
                    slot.id, slot.col_name, slot.col_physical_name
                )
            })
            .collect::<Vec<_>>();
        info!(
            target: "novarocks::sink",
            table_id = descriptor.table_id,
            schema_id,
            index_column_names = ?index_column_names,
            slot_descs = ?slot_desc_summary,
            output_expr_slot_ids = ?output_expr_slot_ids,
            schema_slot_bindings = ?out,
            op_slot_id = ?op_slot_id,
            "OLAP_TABLE_SINK resolved write slot bindings contain missing key columns"
        );
    }
    Ok((out, op_slot_id))
}

fn resolve_effective_tablet_schema_for_index(
    table_id: i64,
    index_id: i64,
    schema_id: i64,
    tablet_ids: &[i64],
    fallback_schema: &StarRocksTabletSchema,
) -> StarRocksTabletSchema {
    for tablet_id in tablet_ids {
        let Ok(runtime) = get_tablet_runtime(*tablet_id) else {
            continue;
        };
        if runtime.schema.id != Some(schema_id) {
            continue;
        }
        if runtime.schema != *fallback_schema {
            info!(
                target: "novarocks::sink",
                table_id,
                index_id,
                schema_id,
                tablet_id,
                fallback_keys_type = ?fallback_schema.keys_type,
                runtime_keys_type = ?runtime.schema.keys_type,
                "OLAP_TABLE_SINK prefer registered tablet runtime schema for write index"
            );
        }
        return runtime.schema;
    }
    fallback_schema.clone()
}

fn resolve_auto_increment_write_policy(
    descriptor: &StarRocksSinkDescriptor,
    tablet_schema: &StarRocksTabletSchema,
) -> Result<AutoIncrementWritePolicy, String> {
    let null_expr_in_auto_increment = descriptor.null_expr_in_auto_increment;
    let miss_auto_increment_column = descriptor.miss_auto_increment_column;

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
        fe_addr: descriptor.frontend.clone(),
        frontend_provider: descriptor.frontend_provider.clone(),
    })
}

fn filter_hidden_op_slot_ids_by_ordinal(
    slot_descs: &[SinkSlotDescriptor],
    slot_ids: Vec<Option<SlotId>>,
    filter_load_op: bool,
) -> Vec<Option<SlotId>> {
    if !filter_load_op || slot_descs.len() != slot_ids.len() {
        return slot_ids;
    }
    slot_descs
        .iter()
        .zip(slot_ids)
        .filter_map(|(slot_desc, slot_id)| {
            let is_load_op = slot_desc
                .col_name
                .as_deref()
                .map(str::trim)
                .is_some_and(|name| name.eq_ignore_ascii_case(LOAD_OP_COLUMN));
            (!is_load_op).then_some(slot_id)
        })
        .collect()
}

fn resolve_schema_slot_ids_by_ordinal(slot_descs: &[SinkSlotDescriptor]) -> Vec<Option<SlotId>> {
    slot_descs.iter().map(|slot| slot.id).collect()
}

fn resolve_index_descriptor(
    schema: &SinkSchemaDescriptor,
    schema_id: i64,
) -> Result<&SinkIndexDescriptor, String> {
    schema
        .indexes
        .iter()
        .find(|idx| idx.schema_id == schema_id)
        .ok_or_else(|| {
            format!("OLAP_TABLE_SINK cannot resolve schema index for schema_id={schema_id}")
        })
}

fn resolve_index_column_names_for_write(
    schema: &SinkSchemaDescriptor,
    schema_id: i64,
) -> Result<Vec<String>, String> {
    Ok(resolve_index_descriptor(schema, schema_id)?
        .column_names
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .map(|name| name.to_ascii_lowercase())
        .collect())
}

fn resolve_sink_write_index_selections(
    descriptor: &StarRocksSinkDescriptor,
) -> Result<Vec<SinkWriteIndexSelection>, String> {
    let mut schema_id_by_index_id = HashMap::<i64, i64>::new();
    let mut index_by_id = HashMap::<i64, &SinkIndexDescriptor>::new();
    for index in &descriptor.schema.indexes {
        if index.index_id <= 0 {
            continue;
        }
        let schema_id = index.schema_id;
        if schema_id <= 0 {
            return Err(format!(
                "OLAP_TABLE_SINK schema.indexes contains non-positive schema_id/index_id: index_id={} schema_id={}",
                index.index_id, schema_id
            ));
        }
        schema_id_by_index_id.insert(index.index_id, schema_id);
        index_by_id.insert(index.index_id, index);
    }
    if schema_id_by_index_id.is_empty() {
        return Err("OLAP_TABLE_SINK schema.indexes has no valid index_id/schema_id".to_string());
    }

    let mut candidate_index_ids = BTreeSet::<i64>::new();
    for partition in descriptor
        .partition
        .partitions
        .iter()
        .filter(|part| !part.is_shadow)
    {
        for index in &partition.indexes {
            if index.index_id > 0 {
                candidate_index_ids.insert(index.index_id);
            }
        }
    }
    if candidate_index_ids.is_empty() {
        let fallback_schema_id = descriptor
            .schema
            .indexes
            .iter()
            .find_map(|index| (index.schema_id > 0).then_some(index.schema_id))
            .ok_or_else(|| "OLAP_TABLE_SINK schema.indexes has no valid schema_id".to_string())?;
        for index in &descriptor.schema.indexes {
            if index.schema_id == fallback_schema_id && index.index_id > 0 {
                candidate_index_ids.insert(index.index_id);
            }
        }
    }
    if candidate_index_ids.is_empty() {
        return Err(
            "OLAP_TABLE_SINK cannot resolve candidate write index ids from partition/schema metadata"
                .to_string(),
        );
    }

    let slot_names = descriptor
        .schema
        .slot_descs
        .iter()
        .flat_map(|slot| {
            [slot.col_name.as_deref(), slot.col_physical_name.as_deref()]
                .into_iter()
                .flatten()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(|name| name.to_ascii_lowercase())
                .collect::<Vec<_>>()
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
        let index_columns = index.column_names.clone();
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
            index.is_shadow,
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
        where_clause: index_by_id
            .get(&primary_index_id)
            .and_then(|index| index.where_clause.clone()),
    });

    let mut rest = scored
        .into_iter()
        .filter(|item| item.0 != primary_index_id)
        .map(|item| SinkWriteIndexSelection {
            index_id: item.0,
            schema_id: item.1,
            where_clause: index_by_id
                .get(&item.0)
                .and_then(|index| index.where_clause.clone()),
        })
        .collect::<Vec<_>>();
    rest.sort_by_key(|item| item.index_id);
    out.extend(rest);
    Ok(out)
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

fn build_lake_table_identity_with_schema_id(
    descriptor: &StarRocksSinkDescriptor,
    schema_id: i64,
) -> Result<LakeTableIdentity, String> {
    let db_name = descriptor
        .db_name
        .as_ref()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "OLAP_TABLE_SINK missing db_name".to_string())?
        .to_string();
    let table_name = descriptor
        .table_name
        .as_ref()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "OLAP_TABLE_SINK missing table_name".to_string())?
        .to_string();
    if schema_id <= 0 {
        return Err(format!(
            "OLAP_TABLE_SINK has non-positive schema_id for lake table identity: {}",
            schema_id
        ));
    }
    Ok(LakeTableIdentity {
        catalog: load_lake_catalog()?,
        db_name,
        table_name,
        db_id: descriptor.db_id,
        table_id: descriptor.table_id,
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

pub(crate) fn resolve_s3_for_sink_tablet(
    tablet_id: i64,
    tablet_root_path: &str,
    from_shard: Option<S3StoreConfig>,
    from_runtime: Option<S3StoreConfig>,
) -> Result<Option<S3StoreConfig>, String> {
    let selected = from_shard
        .or(from_runtime)
        .or_else(|| starlet_shard_registry::infer_s3_config_for_path(tablet_root_path));
    resolve_tablet_root(tablet_root_path, selected.as_ref()).map_err(|err| {
        if selected.is_none() {
            format!(
                "OLAP_TABLE_SINK missing S3 config or invalid tablet root for tablet {} (path={}): {}",
                tablet_id, tablet_root_path, err
            )
        } else {
            format!(
                "OLAP_TABLE_SINK invalid tablet root for tablet {} (path={}): {}",
                tablet_id, tablet_root_path, err
            )
        }
    })?;
    Ok(selected)
}
