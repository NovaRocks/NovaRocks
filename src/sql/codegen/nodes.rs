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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::common::types::UniqueId;
#[cfg(feature = "compat")]
use crate::connector::scan_planning::ConnectorScanPlanner;
use crate::lower::compat::expr::parse_min_max_conjuncts_with_column_resolver;
use crate::runtime::fragment_exec_params::FragmentExecParams;
#[cfg(feature = "compat")]
use crate::runtime::fragment_exec_params::compat_exec_params_from_parts;
#[cfg(feature = "compat")]
use crate::sql::codegen::connector_scan_wire::to_thrift_scan;
use crate::sql::codegen::connector_scan_wire::{ThriftScanContext, to_native_file_scan};
use crate::thrift::exprs;
#[cfg(feature = "compat")]
use crate::thrift::internal_service;
use crate::thrift::partitions;
use crate::thrift::plan_nodes;
use crate::thrift::types;

use super::resolve::ResolvedTable;

use crate::sql::catalog::ScanSource;

// ---------------------------------------------------------------------------
// Scan node
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct PlannedScanTable {
    pub(crate) scan_node_id: i32,
    pub(crate) scan_tuple_id: types::TTupleId,
    pub(crate) resolved: ResolvedTable,
    pub(crate) min_max_conjuncts: Vec<exprs::TExpr>,
    pub(crate) slot_to_column: HashMap<types::TSlotId, String>,
    pub(crate) iceberg_metadata_pseudo_column_slots: BTreeSet<types::TSlotId>,
}

pub(crate) fn build_scan_node(
    connectors: &crate::connector::ConnectorRegistry,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
    min_max_predicates: Vec<crate::common::min_max_predicate::MinMaxPredicate>,
    change_op_slot: Option<types::TSlotId>,
) -> Result<plan_nodes::TPlanNode, String> {
    match &resolved.table.source {
        ScanSource::StarRocks { .. } => {
            #[cfg(feature = "compat")]
            {
                let planned = resolved.planned_scan.as_ref().ok_or_else(|| {
                    format!(
                        "StarRocks scan {}.{} reached build_scan_node without planned connector scan",
                        resolved.database, resolved.table.name
                    )
                })?;
                let planner = connectors.scan_planner("starrocks")?;
                let plan = to_thrift_scan(
                    planner.name(),
                    &planned.scan,
                    &planned.splits,
                    ThriftScanContext {
                        database: resolved.database.clone(),
                        table: resolved.table.name.clone(),
                        node_id,
                        scan_tuple_id,
                        conjuncts,
                        ..ThriftScanContext::default()
                    },
                )?;
                plan.node.ok_or_else(|| {
                    format!(
                        "StarRocks to_thrift_scan returned no node for {}.{}",
                        resolved.database, resolved.table.name
                    )
                })
            }
            #[cfg(not(feature = "compat"))]
            {
                Err("StarRocks scan nodes require feature compat".to_string())
            }
        }
        ScanSource::IcebergDataFiles {
            cloud_properties, ..
        } => {
            let planned = resolved.planned_scan.as_ref().ok_or_else(|| {
                format!(
                    "Iceberg scan {}.{} reached build_scan_node without planned connector scan",
                    resolved.database, resolved.table.name
                )
            })?;
            let planner = connectors.scan_planner("iceberg")?;
            let plan = to_native_file_scan(
                planner.name(),
                &planned.scan,
                &planned.splits,
                ThriftScanContext {
                    database: resolved.database.clone(),
                    table: resolved.table.name.clone(),
                    node_id,
                    scan_tuple_id,
                    conjuncts,
                    min_max_predicates,
                    change_op_slot,
                    cloud_properties: cloud_properties.clone(),
                    columns: resolved.table.columns.clone(),
                },
            )?;
            plan.node.ok_or_else(|| {
                format!(
                    "Iceberg to_native_file_scan returned no node for {}.{}",
                    resolved.database, resolved.table.name
                )
            })
        }
        ScanSource::IcebergDeltaTable { .. } => Ok(build_iceberg_delta_scan_node(
            node_id,
            scan_tuple_id,
            resolved,
            conjuncts,
            mv_refresh_ctx,
        )?),
        _ => Ok(build_hdfs_scan_node(
            node_id,
            scan_tuple_id,
            resolved,
            conjuncts,
        )),
    }
}

/// Emit `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE` for an IVM-A1 delta scan.
/// The Thrift payload carries identity, snapshot range, and an explicit JSON
/// payload. Change-file enumeration and equality-delete target planning happen
/// here at refresh/codegen time; lower_plan only consumes the typed payload.
///
/// `conjuncts` is the predicate-pushdown output for this scan. We forward
/// them on `node.conjuncts` so the shared `LowerNode::evaluate_conjuncts`
/// path applies them post-scan, just like `HDFS_SCAN_NODE`. Without this,
/// `WHERE` clauses on an `__nr_ivm_delta(...)` table reference are silently
/// dropped because there is no Filter node above the scan after pushdown.
fn build_iceberg_delta_scan_node(
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<plan_nodes::TPlanNode, String> {
    let (table_info, from_snapshot_id, to_snapshot_id) = match &resolved.table.source {
        ScanSource::IcebergDeltaTable {
            table,
            from_snapshot_id,
            to_snapshot_id,
        } => (table, *from_snapshot_id, *to_snapshot_id),
        _ => unreachable!("build_iceberg_delta_scan_node called on non-IcebergDeltaTable"),
    };
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::ICEBERG_DELTA_SCAN_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = vec![scan_tuple_id];
    node.nullable_tuples = vec![];
    node.conjuncts = if conjuncts.is_empty() {
        None
    } else {
        Some(conjuncts)
    };
    node.compact_data = true;
    let delta_plan = {
        let runtime_plan =
            crate::sql::codegen::iceberg_delta_scan_wire::build_iceberg_delta_scan_runtime_plan(
                table_info,
                from_snapshot_id,
                to_snapshot_id,
                mv_refresh_ctx,
            )?;
        crate::sql::codegen::iceberg_delta_scan_wire::encode_iceberg_delta_scan_plan_thrift(
            &runtime_plan,
        )?
    };
    node.iceberg_delta_scan_node = Some(plan_nodes::TIcebergDeltaScanNode {
        catalog: table_info.catalog.clone(),
        iceberg_namespace: table_info.namespace.clone(),
        table: table_info.table.clone(),
        from_snapshot_id,
        to_snapshot_id,
        delta_plan,
    });
    Ok(node)
}

fn build_hdfs_scan_node(
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::HDFS_SCAN_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = vec![scan_tuple_id];
    node.nullable_tuples = vec![];
    let min_max_conjuncts = if conjuncts.is_empty() {
        None
    } else {
        Some(conjuncts.clone())
    };
    let min_max_tuple_id = min_max_conjuncts.as_ref().map(|_| scan_tuple_id);
    node.conjuncts = if conjuncts.is_empty() {
        None
    } else {
        Some(conjuncts)
    };
    node.compact_data = true;

    let cloud_config = match &resolved.table.source {
        ScanSource::IcebergMetadataTable {
            cloud_properties, ..
        } => Some(
            crate::thrift::cloud_configuration::TCloudConfiguration::new(
                None::<crate::thrift::cloud_configuration::TCloudType>,
                None::<Vec<crate::thrift::cloud_configuration::TCloudProperty>>,
                Some(cloud_properties.clone()),
                None::<bool>,
            ),
        ),
        _ => None,
    };

    let (serialized_table, metadata_table_type, serialized_predicate) = match &resolved.table.source
    {
        ScanSource::IcebergMetadataTable {
            metadata_table_type,
            serialized_table,
            metadata_payload,
            ..
        } => (
            Some(serialized_table.clone()),
            Some(iceberg_metadata_table_type_thrift_str(metadata_table_type).to_string()),
            metadata_payload.clone(),
        ),
        _ => (None, None, None),
    };

    node.hdfs_scan_node = Some(plan_nodes::THdfsScanNode::new(
        Some(scan_tuple_id),
        None::<BTreeMap<types::TTupleId, Vec<exprs::TExpr>>>,
        min_max_conjuncts,
        min_max_tuple_id,
        None::<BTreeMap<types::TSlotId, Vec<i32>>>,
        None::<Vec<exprs::TExpr>>,
        Some(
            resolved
                .table
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        ),
        Some(resolved.table.name.clone()),
        None::<String>,
        None::<String>,
        None::<String>,
        Some(true), // case_sensitive
        cloud_config,
        None::<bool>,
        None::<bool>,
        None::<bool>,
        None::<types::TTupleId>,
        serialized_table,
        serialized_predicate,
        None::<bool>,
        metadata_table_type,
        None::<crate::thrift::data_cache::TDataCacheOptions>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<Vec<partitions::TBucketProperty>>,
        None::<bool>,
        None::<i64>,
        None::<Vec<plan_nodes::TColumnAccessPath>>,
        None::<Vec<plan_nodes::TVariantPathColumn>>,
    ));

    node
}

/// Map an `IcebergMetadataTableType` to the uppercase thrift string the
/// downstream `IcebergMetadataTableType::parse` expects.
fn iceberg_metadata_table_type_thrift_str(
    ty: &crate::connector::iceberg::IcebergMetadataTableType,
) -> &'static str {
    use crate::connector::iceberg::IcebergMetadataTableType as T;
    match ty {
        T::Files => "FILES",
        T::Manifests => "MANIFESTS",
        T::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
        T::Snapshots => "SNAPSHOTS",
        T::History => "HISTORY",
        T::Refs => "REFS",
        T::Partitions => "PARTITIONS",
    }
}

pub(crate) fn append_hdfs_scan_min_max_conjuncts(
    node: &mut plan_nodes::TPlanNode,
    conjuncts: &[exprs::TExpr],
) {
    if conjuncts.is_empty() {
        return;
    }
    let Some(hdfs) = node.hdfs_scan_node.as_mut() else {
        return;
    };
    hdfs.min_max_conjuncts
        .get_or_insert_with(Vec::new)
        .extend(conjuncts.iter().cloned());
    if hdfs.min_max_tuple_id.is_none() {
        hdfs.min_max_tuple_id = hdfs.tuple_id;
    }
}

// ---------------------------------------------------------------------------
// Project node
// ---------------------------------------------------------------------------

pub(crate) fn build_project_node(
    node_id: i32,
    tuple_id: i32,
    slot_map: BTreeMap<types::TSlotId, exprs::TExpr>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::PROJECT_NODE;
    node.num_children = 1;
    node.limit = -1;
    node.row_tuples = vec![tuple_id];
    node.nullable_tuples = vec![];
    node.compact_data = true;

    node.project_node = Some(plan_nodes::TProjectNode {
        slot_map: Some(slot_map),
        common_slot_map: None,
    });

    node
}

// ---------------------------------------------------------------------------
// Hash join node
// ---------------------------------------------------------------------------

pub(crate) fn build_hash_join_node(
    node_id: i32,
    left_tuple_ids: &[i32],
    right_tuple_ids: &[i32],
    join_op: plan_nodes::TJoinOp,
    distribution_mode: plan_nodes::TJoinDistributionMode,
    eq_join_conjuncts: Vec<plan_nodes::TEqJoinCondition>,
    other_join_conjuncts: Vec<exprs::TExpr>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::HASH_JOIN_NODE;
    node.num_children = 2;
    node.limit = -1;
    // row_tuples must include ALL tuples from both sides so the lowering
    // validation can verify that the output-side tuples are present (required
    // for SEMI/ANTI joins where the left or right side may have multiple
    // tuples from nested cross-joins).
    let mut row_tuples = Vec::with_capacity(left_tuple_ids.len() + right_tuple_ids.len());
    row_tuples.extend_from_slice(left_tuple_ids);
    row_tuples.extend_from_slice(right_tuple_ids);
    // Build nullable_tuples: left side tuples are not nullable for left joins,
    // right side tuples are nullable, etc.
    let mut nullable_tuples = Vec::with_capacity(row_tuples.len());
    let (left_nullable, right_nullable) = match join_op {
        plan_nodes::TJoinOp::LEFT_OUTER_JOIN
        | plan_nodes::TJoinOp::LEFT_ANTI_JOIN
        | plan_nodes::TJoinOp::LEFT_SEMI_JOIN
        | plan_nodes::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN => (false, true),
        plan_nodes::TJoinOp::RIGHT_OUTER_JOIN
        | plan_nodes::TJoinOp::RIGHT_ANTI_JOIN
        | plan_nodes::TJoinOp::RIGHT_SEMI_JOIN => (true, false),
        plan_nodes::TJoinOp::FULL_OUTER_JOIN => (true, true),
        _ => (false, false),
    };
    for _ in left_tuple_ids {
        nullable_tuples.push(left_nullable);
    }
    for _ in right_tuple_ids {
        nullable_tuples.push(right_nullable);
    }
    node.row_tuples = row_tuples;
    node.nullable_tuples = nullable_tuples;
    node.compact_data = true;

    node.hash_join_node = Some(plan_nodes::THashJoinNode {
        join_op,
        eq_join_conjuncts,
        other_join_conjuncts: if other_join_conjuncts.is_empty() {
            None
        } else {
            Some(other_join_conjuncts)
        },
        is_push_down: None,
        add_probe_filters: None,
        is_rewritten_from_not_in: None,
        sql_join_predicates: None,
        sql_predicates: None,
        build_runtime_filters: None,
        build_runtime_filters_from_planner: None,
        distribution_mode: Some(distribution_mode),
        partition_exprs: None,
        output_columns: None,
        interpolate_passthrough: None,
        late_materialization: None,
        enable_partition_hash_join: None,
        is_skew_join: None,
        common_slot_map: None,
        asof_join_condition: None,
    });

    node
}

// ---------------------------------------------------------------------------
// Nested loop join node (for CROSS JOIN and non-equi joins)
// ---------------------------------------------------------------------------

pub(crate) fn build_nestloop_join_node(
    node_id: i32,
    left_tuple_ids: &[i32],
    right_tuple_ids: &[i32],
    join_op: plan_nodes::TJoinOp,
    join_conjuncts: Vec<exprs::TExpr>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::NESTLOOP_JOIN_NODE;
    node.num_children = 2;
    node.limit = -1;
    let mut row_tuples = Vec::with_capacity(left_tuple_ids.len() + right_tuple_ids.len());
    row_tuples.extend_from_slice(left_tuple_ids);
    row_tuples.extend_from_slice(right_tuple_ids);
    let mut nullable_tuples = Vec::with_capacity(row_tuples.len());
    let (left_nullable, right_nullable) = match join_op {
        plan_nodes::TJoinOp::LEFT_OUTER_JOIN
        | plan_nodes::TJoinOp::LEFT_ANTI_JOIN
        | plan_nodes::TJoinOp::LEFT_SEMI_JOIN
        | plan_nodes::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN => (false, true),
        plan_nodes::TJoinOp::RIGHT_OUTER_JOIN
        | plan_nodes::TJoinOp::RIGHT_ANTI_JOIN
        | plan_nodes::TJoinOp::RIGHT_SEMI_JOIN => (true, false),
        plan_nodes::TJoinOp::FULL_OUTER_JOIN => (true, true),
        _ => (false, false),
    };
    for _ in left_tuple_ids {
        nullable_tuples.push(left_nullable);
    }
    for _ in right_tuple_ids {
        nullable_tuples.push(right_nullable);
    }
    node.row_tuples = row_tuples;
    node.nullable_tuples = nullable_tuples;
    node.compact_data = true;

    node.nestloop_join_node = Some(plan_nodes::TNestLoopJoinNode::new(
        Some(join_op),
        None::<Vec<crate::thrift::runtime_filter::TRuntimeFilterDescription>>,
        if join_conjuncts.is_empty() {
            None
        } else {
            Some(join_conjuncts)
        },
        None::<String>,
        None::<bool>,
        None::<BTreeMap<types::TSlotId, exprs::TExpr>>,
    ));

    node
}

// ---------------------------------------------------------------------------
// Aggregation node
// ---------------------------------------------------------------------------

pub(crate) fn build_aggregation_node(
    node_id: i32,
    output_tuple_id: i32,
    intermediate_tuple_id: i32,
    grouping_exprs: Vec<exprs::TExpr>,
    aggregate_functions: Vec<exprs::TExpr>,
    need_finalize: bool,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::AGGREGATION_NODE;
    node.num_children = 1;
    node.limit = -1;
    node.row_tuples = vec![output_tuple_id];
    node.nullable_tuples = vec![];
    node.compact_data = true;

    node.agg_node = Some(plan_nodes::TAggregationNode {
        grouping_exprs: if grouping_exprs.is_empty() {
            None
        } else {
            Some(grouping_exprs)
        },
        aggregate_functions,
        intermediate_tuple_id,
        output_tuple_id,
        need_finalize,
        use_streaming_preaggregation: None,
        has_outer_join_child: None,
        streaming_preaggregation_mode: None,
        sql_grouping_keys: None,
        sql_aggregate_functions: None,
        agg_func_set_version: None,
        intermediate_aggr_exprs: None,
        interpolate_passthrough: None,
        use_sort_agg: None,
        use_per_bucket_optimize: None,
        enable_pipeline_share_limit: None,
        build_runtime_filters: None,
        group_by_min_max: None,
    });

    node
}

// ---------------------------------------------------------------------------
// Sort node
// ---------------------------------------------------------------------------

/// Build a sort node from pre-compiled expressions (for use in window
/// function multi-group emission).
pub(crate) fn build_sort_node_raw(
    node_id: i32,
    row_tuples: Vec<i32>,
    ordering_exprs: Vec<exprs::TExpr>,
    is_asc: Vec<bool>,
    nulls_first_list: Vec<bool>,
    limit: i64,
    offset: Option<i64>,
) -> plan_nodes::TPlanNode {
    let use_top_n = limit > 0 && !ordering_exprs.is_empty();
    let sort_info = plan_nodes::TSortInfo::new(
        ordering_exprs,
        is_asc,
        nulls_first_list,
        None::<Vec<exprs::TExpr>>,
    );
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
    node.num_children = 1;
    node.limit = limit;
    node.row_tuples = row_tuples;
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.sort_node = Some(plan_nodes::TSortNode {
        sort_info,
        use_top_n,
        offset,
        ordering_exprs: None,
        is_asc_order: None,
        is_default_limit: None,
        nulls_first: None,
        sort_tuple_slot_exprs: None,
        has_outer_join_child: None,
        sql_sort_keys: None,
        analytic_partition_exprs: None,
        partition_exprs: None,
        partition_limit: None,
        topn_type: None,
        build_runtime_filters: None,
        max_buffered_rows: None,
        max_buffered_bytes: None,
        late_materialization: None,
        enable_parallel_merge: None,
        analytic_partition_skewed: None,
        pre_agg_exprs: None,
        pre_agg_output_slot_id: None,
        pre_agg_insert_local_shuffle: None,
        parallel_merge_late_materialize_mode: None,
        per_pipeline: None,
    });
    node
}

// ---------------------------------------------------------------------------
// Exec params (scan ranges)
// ---------------------------------------------------------------------------

/// Build exec params for multiple scan nodes (used in JOIN queries).
#[cfg(feature = "compat")]
pub(crate) fn build_exec_params_multi(
    connectors: &crate::connector::ConnectorRegistry,
    scan_tables: &[PlannedScanTable],
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    build_scan_ranges_multi_with_refresh_context(connectors, scan_tables, None)?
        .to_compat_exec_params()
}

#[cfg(feature = "compat")]
pub(crate) fn build_exec_params_multi_with_refresh_context(
    connectors: &crate::connector::ConnectorRegistry,
    scan_tables: &[PlannedScanTable],
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    build_scan_ranges_multi_with_refresh_context(connectors, scan_tables, mv_refresh_ctx)?
        .to_compat_exec_params()
}

#[derive(Clone, Debug)]
pub(crate) struct ScanRangeBuildResult {
    pub(crate) fragment_exec_params: FragmentExecParams,
    #[cfg(feature = "compat")]
    compat_scan_ranges: BTreeMap<i32, Vec<internal_service::TScanRangeParams>>,
}

impl ScanRangeBuildResult {
    pub(crate) fn native_scan_ranges(
        &self,
    ) -> &BTreeMap<i32, Vec<crate::runtime::scan_range::ScanRangeParams>> {
        self.fragment_exec_params.per_node_scan_ranges()
    }

    #[cfg(feature = "compat")]
    pub(crate) fn to_compat_exec_params(
        &self,
    ) -> Result<internal_service::TPlanFragmentExecParams, String> {
        compat_exec_params_from_parts(
            self.fragment_exec_params.query_id(),
            self.fragment_exec_params.fragment_instance_id(),
            self.compat_scan_ranges.clone(),
            self.fragment_exec_params.per_exch_num_senders().clone(),
            None,
        )
    }
}

pub(crate) fn build_scan_ranges_multi_with_refresh_context(
    connectors: &crate::connector::ConnectorRegistry,
    scan_tables: &[PlannedScanTable],
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<ScanRangeBuildResult, String> {
    #[cfg(feature = "compat")]
    let mut per_node_scan_ranges = BTreeMap::new();
    let mut native_scan_ranges = BTreeMap::new();

    for planned in scan_tables {
        let scan_node_id = planned.scan_node_id;
        let resolved = &planned.resolved;
        if matches!(
            resolved.table.source,
            crate::sql::catalog::ScanSource::StarRocks { .. }
        ) {
            #[cfg(feature = "compat")]
            {
                let planner = connectors.scan_planner("starrocks")?;
                let ranges =
                    build_starrocks_scan_ranges_from_planned_scan(planner.as_ref(), planned)?;
                if ranges.is_empty() {
                    return Err(format!(
                        "StarRocks table {}.{} has no selected tablet splits",
                        resolved.database, resolved.table.name
                    ));
                }
                per_node_scan_ranges.insert(scan_node_id, ranges);
                continue;
            }
            #[cfg(not(feature = "compat"))]
            {
                return Err("StarRocks scan ranges require feature compat".to_string());
            }
        } else {
            let native = match &resolved.table.source {
                ScanSource::IcebergDataFiles {
                    cloud_properties, ..
                } => {
                    let planned_scan = resolved.planned_scan.as_ref().ok_or_else(|| {
                        format!(
                            "Iceberg scan {}.{} reached scan-range builder without planned connector scan",
                            resolved.database, resolved.table.name
                        )
                    })?;
                    let planner = connectors.scan_planner("iceberg")?;
                    let plan = to_native_file_scan(
                        planner.name(),
                        &planned_scan.scan,
                        &planned_scan.splits,
                        ThriftScanContext {
                            database: resolved.database.clone(),
                            table: resolved.table.name.clone(),
                            node_id: planned.scan_node_id,
                            scan_tuple_id: planned.scan_tuple_id,
                            min_max_predicates: scan_file_min_max_predicates(planned),
                            change_op_slot: planned_change_op_slot(planned),
                            cloud_properties: cloud_properties.clone(),
                            columns: resolved.table.columns.clone(),
                            ..ThriftScanContext::default()
                        },
                    )?;
                    plan.scan_ranges
                }
                ScanSource::IcebergMetadataTable { .. } => {
                    // The native iceberg-rust metadata scan operator
                    // produces all rows in a single call keyed off
                    // `serialized_table`. We still need at least one
                    // scan range so the runtime allocates a morsel and
                    // dispatches to `IcebergMetadataScanOp`.
                    let native = vec![build_iceberg_metadata_scan_range_params()];
                    native
                }
                ScanSource::IcebergDeltaTable { .. } => {
                    // IVM delta-scan is a single-instance operator: the
                    // change-file enumeration happens inside lower_plan
                    // from `plan_changes`, so we emit one placeholder
                    // morsel for the runtime to dispatch on.
                    let native = vec![build_iceberg_metadata_scan_range_params()];
                    native
                }
                ScanSource::IcebergVersionTable { table, snapshot_id } => {
                    let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                        "Iceberg version scan requires MV refresh context".to_string()
                    })?;
                    let source = refresh_ctx.version_scan_source(table, *snapshot_id)?;
                    let native =
                        build_iceberg_scan_ranges_from_source(connectors, planned, &source, None)?;
                    native
                }
                ScanSource::IcebergMvTargetState(scan) => {
                    let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                        "Iceberg target-state scan requires MV refresh context".to_string()
                    })?;
                    let source = refresh_ctx.target_state_scan_source(scan)?;
                    reject_target_state_equality_deletes(&source)?;
                    let native = build_iceberg_scan_ranges_from_source(
                        connectors,
                        planned,
                        &source,
                        Some(projected_target_state_column_names(scan)),
                    )?;
                    native
                }
                ScanSource::IcebergMvTargetLocator(scan) => {
                    let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                        "Iceberg target-locator scan requires MV refresh context".to_string()
                    })?;
                    let source = refresh_ctx.target_locator_scan_source(scan)?;
                    reject_target_state_equality_deletes(&source)?;
                    let native = build_iceberg_scan_ranges_from_source(
                        connectors,
                        planned,
                        &source,
                        Some(projected_target_locator_column_names(scan)),
                    )?;
                    native
                }
                ScanSource::StarRocks { .. } => unreachable!(
                    "StarRocks scan source is handled by the planned-connector branch above"
                ),
            };
            #[cfg(feature = "compat")]
            {
                let ranges = native
                    .iter()
                    .map(crate::runtime::scan_range::thrift_scan_range_params_from_native)
                    .collect::<Result<Vec<_>, _>>()?;
                per_node_scan_ranges.insert(scan_node_id, ranges);
            }
            native_scan_ranges.insert(scan_node_id, native);
        }
    }

    let fragment_exec_params = FragmentExecParams::new(
        UniqueId { hi: 1, lo: 1 },
        UniqueId { hi: 2, lo: 2 },
        native_scan_ranges.clone(),
        BTreeMap::new(),
        Vec::new(),
    )?;

    Ok(ScanRangeBuildResult {
        fragment_exec_params,
        #[cfg(feature = "compat")]
        compat_scan_ranges: per_node_scan_ranges,
    })
}

fn build_iceberg_scan_ranges_from_source(
    connectors: &crate::connector::ConnectorRegistry,
    planned: &PlannedScanTable,
    source: &ScanSource,
    column_names: Option<Vec<String>>,
) -> Result<Vec<crate::runtime::scan_range::ScanRangeParams>, String> {
    let ScanSource::IcebergDataFiles {
        table,
        files,
        cloud_properties,
        ..
    } = source
    else {
        return Err("refresh-only scan source did not resolve to Iceberg data files".to_string());
    };
    let planner = connectors.scan_planner("iceberg")?;
    let column_names = column_names.unwrap_or_else(|| {
        planned
            .resolved
            .table
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect()
    });
    let table_handle =
        crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
            &table.catalog,
            &table.namespace,
            &table.table,
            table.current_snapshot_id,
            table.clone(),
            files.clone(),
            column_names,
        );
    let scan = planner.begin_scan(
        table_handle,
        crate::connector::scan_planning::BeginScanContext::default(),
    )?;
    let splits = planner.plan_splits(
        &scan,
        crate::connector::scan_planning::SplitPlanningContext::default(),
    )?;
    let plan = to_native_file_scan(
        planner.name(),
        &scan,
        &splits,
        ThriftScanContext {
            database: planned.resolved.database.clone(),
            table: planned.resolved.table.name.clone(),
            node_id: planned.scan_node_id,
            scan_tuple_id: planned.scan_tuple_id,
            min_max_predicates: scan_file_min_max_predicates(planned),
            change_op_slot: planned_change_op_slot(planned),
            cloud_properties: cloud_properties.clone(),
            columns: planned.resolved.table.columns.clone(),
            ..ThriftScanContext::default()
        },
    )?;
    Ok(plan.scan_ranges)
}

pub(crate) fn projected_target_state_column_names(
    scan: &crate::sql::catalog::IcebergMvTargetStateScan,
) -> Vec<String> {
    let mut names = Vec::new();
    push_unique_projected_name(&mut names, &scan.row_id_column_name);
    for name in scan
        .group_key_names
        .iter()
        .chain(scan.aggregate_state_names.iter())
    {
        push_unique_projected_name(&mut names, name);
    }
    if let crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
        branch_scope: Some(scope),
        ..
    } = &scan.row_filter
    {
        push_unique_projected_name(&mut names, &scope.branch_id_column_name);
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        push_unique_projected_name(&mut names, name);
    }
    names
}

fn push_unique_projected_name(names: &mut Vec<String>, name: &str) {
    if !names
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(name))
    {
        names.push(name.to_string());
    }
}

pub(crate) fn projected_target_locator_column_names(
    scan: &crate::sql::catalog::IcebergMvTargetLocatorScan,
) -> Vec<String> {
    let mut names = vec![scan.apply_key_column.clone()];
    if let Some(branch_id_column) = &scan.branch_id_column
        && !names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(branch_id_column))
    {
        names.push(branch_id_column.clone());
    }
    for name in [
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
    ] {
        if !names
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(name))
        {
            names.push(name.to_string());
        }
    }
    names
}

pub(crate) fn reject_target_state_equality_deletes(source: &ScanSource) -> Result<(), String> {
    let ScanSource::IcebergDataFiles { files, .. } = source else {
        return Ok(());
    };
    let has_equality_delete = files.iter().any(|file| {
        file.delete_files.iter().any(|delete_file| {
            delete_file.file_content == crate::sql::catalog::IcebergDeleteFileContent::Equality
        })
    });
    if has_equality_delete {
        return Err("Iceberg target-state scan does not support equality deletes yet".to_string());
    }
    Ok(())
}

fn scan_file_min_max_predicates(planned: &PlannedScanTable) -> Vec<MinMaxPredicate> {
    scan_file_min_max_predicates_from_state(&planned.min_max_conjuncts, &planned.slot_to_column)
}

pub(crate) fn scan_file_min_max_predicates_from_state(
    min_max_conjuncts: &[exprs::TExpr],
    slot_to_column: &HashMap<types::TSlotId, String>,
) -> Vec<MinMaxPredicate> {
    let mut predicates = Vec::new();
    for conjunct in min_max_conjuncts {
        let parsed = parse_min_max_conjuncts_with_column_resolver(conjunct, |slot_ref| {
            slot_to_column
                .get(&slot_ref.slot_id)
                .cloned()
                .ok_or_else(|| format!("slot_id {} has no scan column", slot_ref.slot_id))
        });
        if let Ok(parsed) = parsed {
            predicates.extend(parsed);
        }
    }
    predicates
}

fn planned_change_op_slot(planned: &PlannedScanTable) -> Option<types::TSlotId> {
    planned_change_op_slot_from_state(
        &planned.iceberg_metadata_pseudo_column_slots,
        &planned.slot_to_column,
    )
}

pub(crate) fn planned_change_op_slot_from_state(
    iceberg_metadata_pseudo_column_slots: &BTreeSet<types::TSlotId>,
    slot_to_column: &HashMap<types::TSlotId, String>,
) -> Option<types::TSlotId> {
    iceberg_metadata_pseudo_column_slots
        .iter()
        .copied()
        .find(|slot_id| {
            slot_to_column.get(slot_id).is_some_and(|column| {
                column.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
            })
        })
}

#[cfg(feature = "compat")]
pub(crate) fn build_starrocks_scan_ranges_from_planned_scan(
    planner: &dyn ConnectorScanPlanner,
    planned_table: &PlannedScanTable,
) -> Result<Vec<internal_service::TScanRangeParams>, String> {
    let resolved = &planned_table.resolved;
    let planned_scan = resolved.planned_scan.as_ref().ok_or_else(|| {
        format!(
            "StarRocks table {}.{} reached scan-range builder without planned connector scan",
            resolved.database, resolved.table.name
        )
    })?;
    let thrift = to_thrift_scan(
        planner.name(),
        &planned_scan.scan,
        &planned_scan.splits,
        ThriftScanContext {
            database: resolved.database.clone(),
            table: resolved.table.name.clone(),
            node_id: planned_table.scan_node_id,
            scan_tuple_id: planned_table.scan_tuple_id,
            ..ThriftScanContext::default()
        },
    )?;
    Ok(thrift.scan_ranges)
}

// ---------------------------------------------------------------------------
// Metadata scan range helper
// ---------------------------------------------------------------------------

/// Build a single placeholder scan range that drives the native
/// iceberg-rust metadata scan operator. The operator keys off
/// `serialized_table` on the `THdfsScanNode`, so the per-range payload
/// only needs to satisfy HDFS scan-range lowering invariants: a
/// non-empty path. (The earlier embedded-JVM bridge keyed the same
/// way; that path has been replaced by `IcebergMetadataScanOp` —
/// see `src/connector/iceberg/metadata.rs`.)
fn build_iceberg_metadata_scan_range_params() -> crate::runtime::scan_range::ScanRangeParams {
    crate::runtime::scan_range::ScanRangeParams::file(crate::runtime::scan_range::FileScanRange {
        file_format: crate::runtime::scan_range::FileFormat::Parquet,
        full_path: Some("iceberg-metadata".to_string()),
        relative_path: None,
        table_id: None,
        offset: 0,
        length: 0,
        file_length: 0,
        delete_files: Vec::new(),
        deletion_vector_descriptor: None,
        first_row_id: None,
        data_sequence_number: None,
        modification_time: None,
        datacache_options: None,
        included_positions: Vec::new(),
        serialized_split: Some(String::new()),
        use_iceberg_jni_metadata_reader: true,
        ivm_change_op: None,
        file_pruning_min_max_values: None,
        compat_change_op_slot_id: None,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::{BTreeSet, HashMap};
    use std::rc::Rc;

    use arrow::datatypes::DataType;

    use crate::common::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::codegen::expr_compiler::ExprCompiler;
    use crate::sql::codegen::resolve::{ColumnBinding, ExprScope, ResolvedTable};
    use crate::sql::column_id::ColumnId;

    use super::PlannedScanTable;

    #[test]
    fn pushed_scan_conjuncts_still_feed_native_min_max_predicates() {
        let column_id = ColumnId::new_for_test(1);
        let mut scope = ExprScope::new();
        scope.add_column_with_id(
            column_id,
            None,
            "k".to_string(),
            ColumnBinding {
                tuple_id: 1,
                slot_id: 7,
                data_type: DataType::Int64,
                type_desc: None,
                nullable: false,
            },
        );
        let predicate = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id,
                        qualifier: None,
                        column: "k".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                }),
                op: BinOp::Gt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(10)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let mut compiler = ExprCompiler::new(Rc::new(RefCell::new(1000)), &scope);
        let conjunct = compiler
            .compile_typed(&predicate)
            .expect("compile predicate");

        let slot_to_column = HashMap::from([(7, "k".to_string())]);
        let min_max_predicates =
            super::scan_file_min_max_predicates_from_state(&[conjunct.clone()], &slot_to_column);
        assert_eq!(
            min_max_predicates,
            vec![MinMaxPredicate::Gt {
                column: "k".to_string(),
                value: MinMaxPredicateValue::Int64(10),
            }]
        );

        let planned = PlannedScanTable {
            scan_node_id: 3,
            scan_tuple_id: 1,
            resolved: ResolvedTable {
                database: "db".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::StarRocks {
                        db_id: 1,
                        table_id: 2,
                    },
                },
                planned_scan: None,
                alias: None,
            },
            min_max_conjuncts: vec![conjunct],
            slot_to_column,
            iceberg_metadata_pseudo_column_slots: BTreeSet::new(),
        };

        assert_eq!(
            super::scan_file_min_max_predicates(&planned),
            vec![MinMaxPredicate::Gt {
                column: "k".to_string(),
                value: MinMaxPredicateValue::Int64(10),
            }]
        );
    }

    #[test]
    fn late_filter_appended_scan_conjuncts_feed_native_min_max_predicates() {
        let column_id = ColumnId::new_for_test(1);
        let mut scope = ExprScope::new();
        scope.add_column_with_id(
            column_id,
            None,
            "k".to_string(),
            ColumnBinding {
                tuple_id: 1,
                slot_id: 7,
                data_type: DataType::Int64,
                type_desc: None,
                nullable: false,
            },
        );
        let predicate = TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id,
                        qualifier: None,
                        column: "k".to_string(),
                    },
                    data_type: DataType::Int64,
                    nullable: false,
                }),
                op: BinOp::Gt,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(10)),
                    data_type: DataType::Int64,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        };
        let mut compiler = ExprCompiler::new(Rc::new(RefCell::new(1000)), &scope);
        let late_conjunct = compiler
            .compile_typed(&predicate)
            .expect("compile predicate");
        let mut planned = PlannedScanTable {
            scan_node_id: 3,
            scan_tuple_id: 1,
            resolved: ResolvedTable {
                database: "db".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: "k".to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::StarRocks {
                        db_id: 1,
                        table_id: 2,
                    },
                },
                planned_scan: None,
                alias: None,
            },
            min_max_conjuncts: Vec::new(),
            slot_to_column: HashMap::from([(7, "k".to_string())]),
            iceberg_metadata_pseudo_column_slots: BTreeSet::new(),
        };

        planned.min_max_conjuncts.push(late_conjunct);

        assert_eq!(
            super::scan_file_min_max_predicates(&planned),
            vec![MinMaxPredicate::Gt {
                column: "k".to_string(),
                value: MinMaxPredicateValue::Int64(10),
            }]
        );
    }

    #[test]
    fn native_change_op_tag_records_plain_slot_metadata_without_thrift_expr() {
        let native = crate::sql::codegen::connector_scan_wire::build_native_file_scan_range_params(
            "s3://bucket/path/file.parquet",
            1024,
            0,
            1024,
            None,
            None,
            Some(crate::exec::change_op::CHANGE_OP_DELETE),
            None,
            Some(9),
            &[],
            None,
        )
        .expect("native scan range");
        let crate::runtime::scan_range::ScanRange::File(file) = native.range;
        assert_eq!(
            file.ivm_change_op,
            Some(crate::exec::change_op::CHANGE_OP_DELETE)
        );
        assert_eq!(file.compat_change_op_slot_id, Some(9));
    }

    #[test]
    fn projected_target_state_uses_contract_row_id_column_name() {
        let scan = test_target_state_scan();

        assert_eq!(
            super::projected_target_state_column_names(&scan),
            vec![
                "__row_id__".to_string(),
                "k".to_string(),
                "sum_v".to_string(),
                "_file".to_string(),
                "_pos".to_string(),
                "_row_id".to_string(),
                "_last_updated_sequence_number".to_string(),
            ]
        );
    }

    #[test]
    fn projected_target_state_columns_include_branch_scope_column() {
        let mut scan = test_target_state_scan();
        scan.row_filter = crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: "__row_id__".to_string(),
            branch_scope: Some(crate::sql::catalog::BranchScope {
                branch_id_column_name: "__branch_id__".to_string(),
                branch_id: 1,
            }),
        };
        let projected = super::projected_target_state_column_names(&scan);
        assert!(projected.iter().any(|name| name == "__branch_id__"));
    }

    fn test_target_state_scan() -> crate::sql::catalog::IcebergMvTargetStateScan {
        crate::sql::catalog::IcebergMvTargetStateScan {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            table: "mv_b".to_string(),
            target_table_uuid: "target-uuid".to_string(),
            target_snapshot_id: Some(123),
            aggregate_state_layout_version: 1,
            columns: vec![
                crate::sql::catalog::ColumnDef {
                    name: "__row_id__".to_string(),
                    data_type: arrow::datatypes::DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "visible_sum".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
                crate::sql::catalog::ColumnDef {
                    name: "sum_v".to_string(),
                    data_type: arrow::datatypes::DataType::Binary,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
            group_key_names: vec!["k".to_string()],
            aggregate_state_names: vec!["sum_v".to_string()],
            physical_column_names: vec![
                "__row_id__".to_string(),
                "k".to_string(),
                "visible_sum".to_string(),
                "sum_v".to_string(),
            ],
            row_id_column_name: "__row_id__".to_string(),
            row_filter: crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            partition_constraint:
                crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        }
    }
}

#[cfg(all(test, feature = "compat"))]
mod compat_tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::{
        PlannedScanTable, build_exec_params_multi, build_exec_params_multi_with_refresh_context,
    };
    use crate::connector::scan_planning::ConnectorScanPlanner;
    use crate::sql::catalog::{
        ColumnDef, IcebergDataFileInfo, IcebergMvTargetStateScan, IcebergSchemaDef,
        IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::codegen::connector_scan_wire::build_hdfs_scan_range_params;
    use crate::sql::codegen::resolve::ResolvedTable;
    use crate::thrift::internal_service;

    fn test_iceberg_table_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn hdfs_range(
        params: &crate::thrift::internal_service::TScanRangeParams,
    ) -> &crate::thrift::plan_nodes::THdfsScanRange {
        params
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range")
    }

    #[derive(Debug)]
    struct TestStarRocksScanPlanner;

    impl crate::connector::scan_planning::ConnectorScanPlanner for TestStarRocksScanPlanner {
        fn name(&self) -> &'static str {
            "starrocks"
        }

        fn begin_scan(
            &self,
            _table: crate::connector::scan_planning::TableHandle,
            _ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            Err("test planner should use pre-planned StarRocks scans".to_string())
        }

        fn plan_splits(
            &self,
            _scan: &crate::connector::scan_planning::ScanHandle,
            _ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            Err("test planner should use pre-planned StarRocks splits".to_string())
        }
    }

    fn test_connector_registry() -> crate::connector::ConnectorRegistry {
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(Arc::new(TestStarRocksScanPlanner));
        registry.register_scan_planner(Arc::new(
            crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
        ));
        registry
    }

    #[test]
    fn change_op_tag_without_projected_slot_does_not_emit_extended_columns() {
        let params = build_hdfs_scan_range_params(
            "s3://bucket/path/file.parquet",
            1024,
            0,
            1024,
            None,
            None,
            Some(crate::exec::change_op::CHANGE_OP_INSERT),
            None,
            None,
            &[],
            None,
        )
        .expect("tagged file without __change_op projection should scan ordinary columns");

        assert!(hdfs_range(&params).extended_columns.is_none());
    }

    #[test]
    fn change_op_tag_with_projected_slot_emits_extended_columns() {
        let params = build_hdfs_scan_range_params(
            "s3://bucket/path/file.parquet",
            1024,
            0,
            1024,
            None,
            None,
            Some(crate::exec::change_op::CHANGE_OP_DELETE),
            None,
            Some(9),
            &[],
            None,
        )
        .expect("tagged file with __change_op projection should emit metadata");

        let extended_columns = hdfs_range(&params)
            .extended_columns
            .as_ref()
            .expect("extended_columns");
        assert_eq!(extended_columns.len(), 1);
        assert!(extended_columns.contains_key(&9));
    }

    #[test]
    fn physical_change_op_column_does_not_emit_extended_columns() {
        let iceberg_files = vec![IcebergDataFileInfo {
            path: "s3://bucket/path/file.parquet".to_string(),
            size: 1024,
            row_count: Some(1),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: Some(crate::exec::change_op::CHANGE_OP_INSERT),
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }];
        let iceberg_table_info = test_iceberg_table_info();
        let planner = crate::connector::iceberg::IcebergConnectorScanPlanner::new();
        let table_handle =
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
                &iceberg_table_info.catalog,
                &iceberg_table_info.namespace,
                &iceberg_table_info.table,
                iceberg_table_info.current_snapshot_id,
                iceberg_table_info.clone(),
                iceberg_files.clone(),
                vec![crate::exec::change_op::CHANGE_OP_COLUMN.to_string()],
            );
        let scan = planner
            .begin_scan(
                table_handle,
                crate::connector::scan_planning::BeginScanContext::default(),
            )
            .expect("begin_scan");
        let splits = planner
            .plan_splits(
                &scan,
                crate::connector::scan_planning::SplitPlanningContext::default(),
            )
            .expect("plan_splits");
        let planned = PlannedScanTable {
            scan_node_id: 3,
            scan_tuple_id: 4,
            resolved: ResolvedTable {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
                        data_type: DataType::Int8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: iceberg_table_info,
                        files: iceberg_files,
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
                    },
                },
                alias: None,
                planned_scan: Some(crate::sql::codegen::resolve::PlannedConnectorScan {
                    scan,
                    splits,
                }),
            },
            min_max_conjuncts: vec![],
            slot_to_column: HashMap::from([(
                9,
                crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
            )]),
            iceberg_metadata_pseudo_column_slots: Default::default(),
        };

        let registry = test_connector_registry();
        let params = build_exec_params_multi(&registry, &[planned]).expect("build scan ranges");
        let ranges = params
            .per_node_scan_ranges
            .get(&3)
            .expect("scan node ranges");

        assert_eq!(ranges.len(), 1);
        assert!(hdfs_range(&ranges[0]).extended_columns.is_none());
    }

    #[test]
    #[cfg(feature = "compat")]
    fn starrocks_scan_ranges_use_planned_connector_scan_without_physical_layout() {
        use crate::connector::scan_planning::{ScanHandle, Split};
        use crate::connector::starrocks::table::scan_planner::{
            StarRocksScanHandle, StarRocksSplit, StarRocksTableHandle,
        };
        use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
        use crate::sql::codegen::resolve::{PlannedConnectorScan, ResolvedTable};
        use arrow::datatypes::DataType;

        let table = TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 10,
                table_id: 20,
            },
        };
        let planned_scan = PlannedConnectorScan {
            scan: ScanHandle::new(
                "starrocks",
                StarRocksScanHandle {
                    table: StarRocksTableHandle {
                        database: "default".to_string(),
                        table: "orders".to_string(),
                        db_id: 10,
                        table_id: 20,
                    },
                    schema_id: 30,
                },
            ),
            splits: vec![Split::new(
                "starrocks",
                StarRocksSplit {
                    tablet_id: 300,
                    partition_id: 100,
                    version: 7,
                },
            )],
        };
        let planned = PlannedScanTable {
            scan_node_id: 3,
            scan_tuple_id: 4,
            resolved: ResolvedTable {
                database: "default".to_string(),
                table,
                planned_scan: Some(planned_scan),
                alias: None,
            },
            min_max_conjuncts: vec![],
            slot_to_column: HashMap::new(),
            iceberg_metadata_pseudo_column_slots: Default::default(),
        };
        let registry = test_connector_registry();
        let planner = registry
            .scan_planner("starrocks")
            .expect("starrocks scan planner");
        let node = super::build_scan_node(
            &registry,
            None,
            planned.scan_node_id,
            planned.scan_tuple_id,
            &planned.resolved,
            Vec::new(),
            Vec::new(),
            None,
        )
        .expect("build StarRocks scan node from planned connector scan");
        assert_eq!(
            node.node_type,
            crate::thrift::plan_nodes::TPlanNodeType::LAKE_SCAN_NODE
        );

        let ranges =
            super::build_starrocks_scan_ranges_from_planned_scan(planner.as_ref(), &planned)
                .expect("planned scan ranges");

        assert_eq!(ranges.len(), 1);
        let internal = ranges[0]
            .scan_range
            .internal_scan_range
            .as_ref()
            .expect("internal scan range");
        assert_eq!(internal.tablet_id, 300);
        assert_eq!(internal.partition_id, Some(100));
        assert_eq!(internal.version, "7");
        assert_eq!(internal.schema_hash, "30");
    }

    #[test]
    #[cfg(feature = "compat")]
    fn starrocks_scan_ranges_include_catalog_identity() {
        use crate::connector::scan_planning::{ScanHandle, Split};
        use crate::connector::starrocks::table::scan_planner::{
            StarRocksScanHandle, StarRocksSplit, StarRocksTableHandle,
        };
        use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
        use crate::sql::codegen::resolve::{PlannedConnectorScan, ResolvedTable};
        use arrow::datatypes::DataType;

        let table = TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 10,
                table_id: 20,
            },
        };
        let planned_scan = PlannedConnectorScan {
            scan: ScanHandle::new(
                "starrocks",
                StarRocksScanHandle {
                    table: StarRocksTableHandle {
                        database: "analytics".to_string(),
                        table: "orders".to_string(),
                        db_id: 10,
                        table_id: 20,
                    },
                    schema_id: 30,
                },
            ),
            splits: vec![Split::new(
                "starrocks",
                StarRocksSplit {
                    tablet_id: 300,
                    partition_id: 100,
                    version: 7,
                },
            )],
        };
        let planned = PlannedScanTable {
            scan_node_id: 3,
            scan_tuple_id: 4,
            resolved: ResolvedTable {
                database: "analytics".to_string(),
                table,
                planned_scan: Some(planned_scan),
                alias: None,
            },
            min_max_conjuncts: vec![],
            slot_to_column: HashMap::new(),
            iceberg_metadata_pseudo_column_slots: Default::default(),
        };
        let registry = test_connector_registry();
        let planner = registry
            .scan_planner("starrocks")
            .expect("starrocks scan planner");

        let ranges =
            super::build_starrocks_scan_ranges_from_planned_scan(planner.as_ref(), &planned)
                .expect("planned scan ranges");
        let internal = ranges[0]
            .scan_range
            .internal_scan_range
            .as_ref()
            .expect("internal scan range");

        assert_eq!(internal.catalog_name.as_deref(), Some("default_catalog"));
        assert_eq!(internal.db_name, "analytics");
        assert_eq!(internal.table_name.as_deref(), Some("orders"));
    }

    #[test]
    fn metadata_change_op_column_emits_extended_columns() {
        let iceberg_files = vec![IcebergDataFileInfo {
            path: "s3://bucket/path/file.parquet".to_string(),
            size: 1024,
            row_count: Some(1),
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: None,
            ivm_change_op: Some(crate::exec::change_op::CHANGE_OP_INSERT),
            included_positions: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }];
        let iceberg_table_info = test_iceberg_table_info();
        let planner = crate::connector::iceberg::IcebergConnectorScanPlanner::new();
        let table_handle =
            crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
                &iceberg_table_info.catalog,
                &iceberg_table_info.namespace,
                &iceberg_table_info.table,
                iceberg_table_info.current_snapshot_id,
                iceberg_table_info.clone(),
                iceberg_files.clone(),
                vec![crate::exec::change_op::CHANGE_OP_COLUMN.to_string()],
            );
        let scan = planner
            .begin_scan(
                table_handle,
                crate::connector::scan_planning::BeginScanContext::default(),
            )
            .expect("begin_scan");
        let splits = planner
            .plan_splits(
                &scan,
                crate::connector::scan_planning::SplitPlanningContext::default(),
            )
            .expect("plan_splits");
        let planned = PlannedScanTable {
            scan_node_id: 3,
            scan_tuple_id: 4,
            resolved: ResolvedTable {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![ColumnDef {
                        name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
                        data_type: DataType::Int8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    source: ScanSource::IcebergDataFiles {
                        table: iceberg_table_info,
                        files: iceberg_files,
                        cloud_properties: BTreeMap::new(),
                        binding: crate::sql::catalog::IcebergDataFileBinding::ExplicitFiles,
                    },
                },
                alias: None,
                planned_scan: Some(crate::sql::codegen::resolve::PlannedConnectorScan {
                    scan,
                    splits,
                }),
            },
            min_max_conjuncts: vec![],
            slot_to_column: HashMap::from([(
                9,
                crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
            )]),
            iceberg_metadata_pseudo_column_slots: [9].into(),
        };

        let registry = test_connector_registry();
        let params = build_exec_params_multi(&registry, &[planned]).expect("build scan ranges");
        let ranges = params
            .per_node_scan_ranges
            .get(&3)
            .expect("scan node ranges");
        let extended_columns = hdfs_range(&ranges[0])
            .extended_columns
            .as_ref()
            .expect("extended columns");

        assert_eq!(extended_columns.len(), 1);
        assert!(extended_columns.contains_key(&9));
    }

    fn build_iceberg_version_scan_node_for_test(
        mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    ) -> Result<internal_service::TPlanFragmentExecParams, String> {
        let resolved = ResolvedTable {
            database: "db".to_string(),
            table: TableDef {
                name: "b".to_string(),
                columns: vec![ColumnDef {
                    name: "k".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergVersionTable {
                    table: IcebergTableInfo {
                        catalog: "ice".to_string(),
                        namespace: "db".to_string(),
                        table: "b".to_string(),
                        table_uuid: Some("uuid-b".to_string()),
                        current_snapshot_id: Some(22),
                        schema_id: 7,
                        location: "file:///tmp/ice/db/b".to_string(),
                        schema: IcebergSchemaDef { fields: Vec::new() },
                        serialized_metadata: None,
                        serialized_metadata_rows: None,
                    },
                    snapshot_id: 11,
                },
            },
            planned_scan: None,
            alias: None,
        };
        let planned = PlannedScanTable {
            scan_node_id: 9,
            scan_tuple_id: 4,
            resolved,
            min_max_conjuncts: Vec::new(),
            slot_to_column: std::collections::HashMap::new(),
            iceberg_metadata_pseudo_column_slots: std::collections::BTreeSet::new(),
        };

        let registry = test_connector_registry();
        build_exec_params_multi_with_refresh_context(&registry, &[planned], mv_refresh_ctx)
    }

    fn build_iceberg_target_state_scan_node_for_test(
        mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    ) -> Result<internal_service::TPlanFragmentExecParams, String> {
        let resolved = ResolvedTable {
            database: "db".to_string(),
            table: TableDef {
                name: "mv_b".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "k".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                    ColumnDef {
                        name: "sum_v".to_string(),
                        data_type: arrow::datatypes::DataType::Int64,
                        nullable: true,
                        write_default: None,
                        logical_type: None,
                    },
                ],
                iceberg_row_lineage_metadata_columns: vec![ColumnDef {
                    name: "_row_id".to_string(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                source: ScanSource::IcebergMvTargetState(IcebergMvTargetStateScan {
                    catalog: "ice".to_string(),
                    database: "db".to_string(),
                    table: "mv_b".to_string(),
                    target_table_uuid: "target-uuid".to_string(),
                    target_snapshot_id: Some(123),
                    aggregate_state_layout_version: 1,
                    columns: vec![
                        ColumnDef {
                            name: "k".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "sum_v".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    group_key_names: vec!["k".to_string()],
                    aggregate_state_names: vec!["sum_v".to_string()],
                    physical_column_names: vec!["k".to_string(), "sum_v".to_string()],
                    row_id_column_name: "_row_id".to_string(),
                    row_filter:
                        crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                            row_id_column_name: "_row_id".to_string(),
                            branch_scope: None,
                        },
                    partition_constraint:
                        crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
                }),
            },
            planned_scan: None,
            alias: None,
        };
        let planned = PlannedScanTable {
            scan_node_id: 10,
            scan_tuple_id: 5,
            resolved,
            min_max_conjuncts: Vec::new(),
            slot_to_column: std::collections::HashMap::new(),
            iceberg_metadata_pseudo_column_slots: std::collections::BTreeSet::new(),
        };

        let registry = test_connector_registry();
        build_exec_params_multi_with_refresh_context(&registry, &[planned], mv_refresh_ctx)
    }

    #[test]
    fn iceberg_version_scan_without_refresh_context_fails_fast() {
        let err = build_iceberg_version_scan_node_for_test(None)
            .expect_err("version scan outside MV refresh must fail");
        assert!(
            err.to_string()
                .contains("Iceberg version scan requires MV refresh context"),
            "{err}"
        );
    }

    #[test]
    fn iceberg_target_state_scan_without_refresh_context_fails_fast() {
        let err = build_iceberg_target_state_scan_node_for_test(None)
            .expect_err("target-state scan outside MV refresh must fail");
        assert!(
            err.to_string()
                .contains("Iceberg target-state scan requires MV refresh context"),
            "{err}"
        );
    }

    #[test]
    fn projected_target_state_uses_contract_row_id_column_name() {
        let scan = test_target_state_scan();

        assert_eq!(
            super::projected_target_state_column_names(&scan),
            vec![
                "__row_id__".to_string(),
                "k".to_string(),
                "sum_v".to_string(),
                "_file".to_string(),
                "_pos".to_string(),
                "_row_id".to_string(),
                "_last_updated_sequence_number".to_string(),
            ]
        );
    }

    #[test]
    fn projected_target_state_columns_include_branch_scope_column() {
        let mut scan = test_target_state_scan();
        scan.row_filter = crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: "__row_id__".to_string(),
            branch_scope: Some(crate::sql::catalog::BranchScope {
                branch_id_column_name: "__branch_id__".to_string(),
                branch_id: 1,
            }),
        };
        let projected = super::projected_target_state_column_names(&scan);
        assert!(projected.iter().any(|name| name == "__branch_id__"));
    }

    fn test_target_state_scan() -> IcebergMvTargetStateScan {
        IcebergMvTargetStateScan {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            table: "mv_b".to_string(),
            target_table_uuid: "target-uuid".to_string(),
            target_snapshot_id: Some(123),
            aggregate_state_layout_version: 1,
            columns: vec![
                ColumnDef {
                    name: "__row_id__".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "visible_sum".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "sum_v".to_string(),
                    data_type: DataType::Binary,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
            ],
            group_key_names: vec!["k".to_string()],
            aggregate_state_names: vec!["sum_v".to_string()],
            physical_column_names: vec![
                "__row_id__".to_string(),
                "k".to_string(),
                "visible_sum".to_string(),
                "sum_v".to_string(),
            ],
            row_id_column_name: "__row_id__".to_string(),
            row_filter: crate::sql::catalog::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            partition_constraint:
                crate::sql::catalog::IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        }
    }
}

// ---------------------------------------------------------------------------
// Exchange node (used for CTE consume)
// ---------------------------------------------------------------------------

pub(crate) fn build_exchange_node(
    node_id: i32,
    input_row_tuples: Vec<i32>,
    partition_type: partitions::TPartitionType,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::EXCHANGE_NODE;
    node.num_children = 0;
    node.limit = -1;
    node.row_tuples = input_row_tuples.clone();
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.exchange_node = Some(plan_nodes::TExchangeNode::new(
        input_row_tuples,
        None::<plan_nodes::TSortInfo>,
        None::<i64>,
        Some(partition_type),
        None::<bool>,
        None::<plan_nodes::TLateMaterializeMode>,
    ));
    node
}

/// Build a non-ordering EXCHANGE_NODE whose receive side applies LIMIT/OFFSET.
pub(crate) fn build_limit_exchange_node(
    node_id: i32,
    input_row_tuples: Vec<i32>,
    partition_type: partitions::TPartitionType,
    limit: Option<i64>,
    offset: Option<i64>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::EXCHANGE_NODE;
    node.num_children = 0;
    node.limit = limit.unwrap_or(-1);
    node.row_tuples = input_row_tuples.clone();
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.exchange_node = Some(plan_nodes::TExchangeNode::new(
        input_row_tuples,
        None::<plan_nodes::TSortInfo>,
        offset,
        Some(partition_type),
        None::<bool>,
        None::<plan_nodes::TLateMaterializeMode>,
    ));
    node
}

/// Build a merging EXCHANGE_NODE. The receive side performs k-way merge
/// over sorted input streams using `sort_info`, then applies offset/limit.
/// Used for distributed TopN FINAL(split) and global ORDER BY.
pub(crate) fn build_merging_exchange_node(
    node_id: i32,
    input_row_tuples: Vec<i32>,
    partition_type: partitions::TPartitionType,
    sort_info: plan_nodes::TSortInfo,
    limit: Option<i64>,
    offset: Option<i64>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::EXCHANGE_NODE;
    node.num_children = 0;
    node.limit = limit.unwrap_or(-1);
    node.row_tuples = input_row_tuples.clone();
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.exchange_node = Some(plan_nodes::TExchangeNode::new(
        input_row_tuples,
        Some(sort_info),
        offset,
        Some(partition_type),
        None::<bool>,
        None::<plan_nodes::TLateMaterializeMode>,
    ));
    node
}

// ---------------------------------------------------------------------------
// Default plan node
// ---------------------------------------------------------------------------

pub(crate) fn default_plan_node() -> plan_nodes::TPlanNode {
    plan_nodes::TPlanNode {
        node_id: 0,
        node_type: plan_nodes::TPlanNodeType::HDFS_SCAN_NODE,
        num_children: 0,
        limit: -1,
        row_tuples: vec![],
        nullable_tuples: vec![],
        conjuncts: None,
        compact_data: true,
        common: None,
        hash_join_node: None,
        agg_node: None,
        sort_node: None,
        merge_node: None,
        exchange_node: None,
        mysql_scan_node: None,
        olap_scan_node: None,
        file_scan_node: None,
        schema_scan_node: None,
        meta_scan_node: None,
        analytic_node: None,
        union_node: None,
        resource_profile: None,
        es_scan_node: None,
        repeat_node: None,
        assert_num_rows_node: None,
        intersect_node: None,
        except_node: None,
        merge_join_node: None,
        raw_values_node: None,
        use_vectorized: None,
        hdfs_scan_node: None,
        project_node: None,
        table_function_node: None,
        probe_runtime_filters: None,
        decode_node: None,
        local_rf_waiting_set: None,
        filter_null_value_columns: None,
        need_create_tuple_columns: None,
        jdbc_scan_node: None,
        connector_scan_node: None,
        cross_join_node: None,
        lake_scan_node: None,
        nestloop_join_node: None,
        stream_scan_node: None,
        stream_join_node: None,
        stream_agg_node: None,
        select_node: None,
        fetch_node: None,
        look_up_node: None,
        benchmark_scan_node: None,
        cache_stats_scan_node: None,
        iceberg_delta_scan_node: None,
        change_event_expand_node: None,
    }
}
