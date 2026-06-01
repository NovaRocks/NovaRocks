use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::connector::scan_planning::{ConnectorScanPlanner, ThriftScanContext};
use crate::descriptors;
use crate::exprs;
use crate::internal_service;
use crate::lower::expr::parse_min_max_conjuncts_with_column_resolver;
use crate::partitions;
use crate::plan_nodes;
use crate::types;

use super::resolve::ResolvedTable;

use crate::sql::catalog::{
    IcebergColumnStats, IcebergDataFileInfo, IcebergPartitionValue, ScanSource,
};

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
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
    min_max_predicates: Vec<crate::common::min_max_predicate::MinMaxPredicate>,
    change_op_slot: Option<types::TSlotId>,
) -> Result<plan_nodes::TPlanNode, String> {
    match &resolved.table.source {
        ScanSource::StarRocks { .. } => {
            let planned = resolved.planned_scan.as_ref().ok_or_else(|| {
                format!(
                    "StarRocks scan {}.{} reached build_scan_node without planned connector scan",
                    resolved.database, resolved.table.name
                )
            })?;
            let planner = connectors.scan_planner("starrocks")?;
            let plan = planner.to_thrift_scan(
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
            let plan = planner.to_thrift_scan(
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
                },
            )?;
            plan.node.ok_or_else(|| {
                format!(
                    "Iceberg to_thrift_scan returned no node for {}.{}",
                    resolved.database, resolved.table.name
                )
            })
        }
        ScanSource::IcebergDeltaTable { .. } => Ok(build_iceberg_delta_scan_node(
            node_id,
            scan_tuple_id,
            resolved,
            conjuncts,
        )),
        _ => Ok(build_hdfs_scan_node(
            node_id,
            scan_tuple_id,
            resolved,
            conjuncts,
        )),
    }
}

/// Emit `TPlanNodeType::ICEBERG_DELTA_SCAN_NODE` for an IVM-A1 delta scan.
/// Only the lightweight identity + snapshot range is carried in the Thrift
/// payload; the actual change-file enumeration happens at `lower_plan`
/// time via `connector::iceberg::changes::plan_changes`.
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
) -> plan_nodes::TPlanNode {
    let (catalog, namespace, table, from_snapshot_id, to_snapshot_id) = match &resolved.table.source
    {
        ScanSource::IcebergDeltaTable {
            table,
            from_snapshot_id,
            to_snapshot_id,
        } => (
            table.catalog.clone(),
            table.namespace.clone(),
            table.table.clone(),
            *from_snapshot_id,
            *to_snapshot_id,
        ),
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
    node.iceberg_delta_scan_node = Some(plan_nodes::TIcebergDeltaScanNode {
        catalog,
        iceberg_namespace: namespace,
        table,
        from_snapshot_id,
        to_snapshot_id,
    });
    node
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
        } => Some(crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties.clone()),
            None::<bool>,
        )),
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
        None::<crate::data_cache::TDataCacheOptions>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<Vec<partitions::TBucketProperty>>,
        None::<bool>,
        None::<i64>,
        None::<Vec<plan_nodes::TColumnAccessPath>>,
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
        | plan_nodes::TJoinOp::LEFT_SEMI_JOIN => (false, true),
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
        distribution_mode: Some(plan_nodes::TJoinDistributionMode::BROADCAST),
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
        | plan_nodes::TJoinOp::LEFT_SEMI_JOIN => (false, true),
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
        None::<Vec<crate::runtime_filter::TRuntimeFilterDescription>>,
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

pub(crate) fn build_aggregate_state_merge_exec_node(
    old_input: crate::exec::node::ExecNode,
    delta_input: crate::exec::node::ExecNode,
    layout: crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
) -> crate::exec::node::ExecNode {
    crate::exec::node::ExecNode {
        kind: crate::exec::node::ExecNodeKind::AggregateStateMerge(
            crate::exec::operators::aggregate_state_merge::AggregateStateMergePlan {
                old_input: Box::new(old_input),
                delta_input: Box::new(delta_input),
                layout,
            },
        ),
    }
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
pub(crate) fn build_exec_params_multi(
    connectors: &crate::connector::ConnectorRegistry,
    scan_tables: &[PlannedScanTable],
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    build_exec_params_multi_with_refresh_context(connectors, scan_tables, None)
}

pub(crate) fn build_exec_params_multi_with_refresh_context(
    connectors: &crate::connector::ConnectorRegistry,
    scan_tables: &[PlannedScanTable],
    mv_refresh_ctx: Option<&crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    let mut per_node_scan_ranges = BTreeMap::new();

    for planned in scan_tables {
        let scan_node_id = planned.scan_node_id;
        let resolved = &planned.resolved;
        let ranges = if matches!(
            resolved.table.source,
            crate::sql::catalog::ScanSource::StarRocks { .. }
        ) {
            let planner = connectors.scan_planner("starrocks")?;
            let ranges = build_starrocks_scan_ranges_from_planned_scan(planner.as_ref(), planned)?;
            if ranges.is_empty() {
                return Err(format!(
                    "StarRocks table {}.{} has no selected tablet splits",
                    resolved.database, resolved.table.name
                ));
            }
            ranges
        } else {
            match &resolved.table.source {
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
                    let plan = planner.to_thrift_scan(
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
                    vec![build_iceberg_metadata_scan_range_params()]
                }
                ScanSource::IcebergDeltaTable { .. } => {
                    // IVM delta-scan is a single-instance operator: the
                    // change-file enumeration happens inside lower_plan
                    // from `plan_changes`, so we emit one placeholder
                    // morsel for the runtime to dispatch on.
                    vec![build_iceberg_metadata_scan_range_params()]
                }
                ScanSource::IcebergVersionTable { table, snapshot_id } => {
                    let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                        "Iceberg version scan requires MV refresh context".to_string()
                    })?;
                    let source = refresh_ctx.version_scan_source(table, *snapshot_id)?;
                    build_iceberg_scan_ranges_from_source(connectors, planned, &source, None)?
                }
                ScanSource::IcebergMvTargetState(scan) => {
                    let refresh_ctx = mv_refresh_ctx.ok_or_else(|| {
                        "Iceberg target-state scan requires MV refresh context".to_string()
                    })?;
                    let source = refresh_ctx.target_state_scan_source(scan)?;
                    reject_target_state_equality_deletes(&source)?;
                    build_iceberg_scan_ranges_from_source(
                        connectors,
                        planned,
                        &source,
                        Some(projected_target_state_column_names(scan)),
                    )?
                }
                ScanSource::StarRocks { .. } => unreachable!(
                    "StarRocks scan source is handled by the planned-connector branch above"
                ),
            }
        };
        per_node_scan_ranges.insert(scan_node_id, ranges);
    }

    Ok(internal_service::TPlanFragmentExecParams::new(
        types::TUniqueId::new(1, 1),
        types::TUniqueId::new(2, 2),
        per_node_scan_ranges,
        BTreeMap::new(),
        None::<Vec<crate::data_sinks::TPlanFragmentDestination>>,
        None::<i32>,
        None::<i32>,
        None::<bool>,
        None::<bool>,
        None::<crate::runtime_filter::TRuntimeFilterParams>,
        None::<i32>,
        None::<bool>,
        None::<BTreeMap<types::TPlanNodeId, BTreeMap<i32, Vec<internal_service::TScanRangeParams>>>>,
        None::<bool>,
        None::<i32>,
        None::<bool>,
        None::<Vec<internal_service::TExecDebugOption>>,
    ))
}

fn build_iceberg_scan_ranges_from_source(
    connectors: &crate::connector::ConnectorRegistry,
    planned: &PlannedScanTable,
    source: &ScanSource,
    column_names: Option<Vec<String>>,
) -> Result<Vec<internal_service::TScanRangeParams>, String> {
    let ScanSource::IcebergDataFiles {
        table,
        files,
        cloud_properties,
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
    let plan = planner.to_thrift_scan(
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
            ..ThriftScanContext::default()
        },
    )?;
    Ok(plan.scan_ranges)
}

pub(crate) fn projected_target_state_column_names(
    scan: &crate::sql::catalog::IcebergMvTargetStateScan,
) -> Vec<String> {
    let mut names = Vec::new();
    for name in scan
        .group_key_names
        .iter()
        .chain(scan.aggregate_state_names.iter())
    {
        if !names
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(name))
        {
            names.push(name.clone());
        }
    }
    if !names
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&scan.row_id_column_name))
    {
        names.push(scan.row_id_column_name.clone());
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

pub(crate) fn file_may_satisfy_min_max(
    file: &IcebergDataFileInfo,
    predicates: &[MinMaxPredicate],
) -> bool {
    if predicates.is_empty() {
        return true;
    }
    let column_stats = file.column_stats.as_ref();
    predicates.iter().all(|predicate| {
        if let Some(may_satisfy) = partition_may_satisfy_predicate(file, predicate) {
            return may_satisfy;
        }
        let Some(column_stats) = column_stats else {
            return true;
        };
        let Some(stats) = find_column_stats(column_stats, predicate.column()) else {
            return true;
        };
        stats_may_satisfy_predicate(stats, predicate)
    })
}

fn partition_may_satisfy_predicate(
    file: &IcebergDataFileInfo,
    predicate: &MinMaxPredicate,
) -> Option<bool> {
    let partition = file.partition_values.iter().find(|value| {
        value.transform.eq_ignore_ascii_case("identity")
            && value.source_column.eq_ignore_ascii_case(predicate.column())
    })?;
    let Some(value) = partition.value.as_ref() else {
        return Some(false);
    };
    partition_value_may_satisfy_predicate(value, predicate)
}

fn partition_value_may_satisfy_predicate(
    partition_value: &IcebergPartitionValue,
    predicate: &MinMaxPredicate,
) -> Option<bool> {
    let value = predicate.value();
    match partition_value {
        IcebergPartitionValue::Boolean(v) => {
            let value = value.as_bool()?;
            let left = i64::from(*v);
            let right = i64::from(value);
            Some(point_may_satisfy_i64(left, predicate, right))
        }
        IcebergPartitionValue::Int32(v) => {
            let value = value.as_i64()?;
            Some(point_may_satisfy_i64(i64::from(*v), predicate, value))
        }
        IcebergPartitionValue::Int64(v) => {
            let value = value.as_i64()?;
            Some(point_may_satisfy_i64(*v, predicate, value))
        }
        IcebergPartitionValue::Float(v) => {
            let value = value.as_f64()?;
            Some(point_may_satisfy_f64(f64::from(*v), predicate, value))
        }
        IcebergPartitionValue::Double(v) => {
            let value = value.as_f64()?;
            Some(point_may_satisfy_f64(*v, predicate, value))
        }
        IcebergPartitionValue::String(v) => {
            let value = value.as_bytes()?;
            Some(point_may_satisfy_bytes(v.as_bytes(), predicate, value))
        }
        IcebergPartitionValue::Binary(v) => {
            let value = value.as_bytes()?;
            Some(point_may_satisfy_bytes(v.as_slice(), predicate, value))
        }
    }
}

fn find_column_stats<'a>(
    column_stats: &'a HashMap<String, IcebergColumnStats>,
    column: &str,
) -> Option<&'a IcebergColumnStats> {
    column_stats.get(column).or_else(|| {
        column_stats
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))
            .map(|(_, stats)| stats)
    })
}

fn stats_may_satisfy_predicate(stats: &IcebergColumnStats, predicate: &MinMaxPredicate) -> bool {
    let value = predicate.value();
    if let Some(value) = value.as_bool() {
        return stats_may_satisfy_bool(stats, predicate, value);
    }
    if let Some(value) = value.as_i64() {
        return stats_may_satisfy_i64(stats, predicate, value);
    }
    if let Some(value) = value.as_f64() {
        return stats_may_satisfy_f64(stats, predicate, value);
    }
    if let Some(value) = value.as_bytes() {
        return stats_may_satisfy_bytes(stats, predicate, value);
    }
    true
}

fn stats_may_satisfy_bool(
    stats: &IcebergColumnStats,
    predicate: &MinMaxPredicate,
    value: bool,
) -> bool {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_bool_bound) else {
        return true;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_bool_bound) else {
        return true;
    };
    let value = i64::from(value);
    range_may_satisfy_i64(i64::from(lower), i64::from(upper), predicate, value)
}

fn stats_may_satisfy_i64(
    stats: &IcebergColumnStats,
    predicate: &MinMaxPredicate,
    value: i64,
) -> bool {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_i64_bound) else {
        return true;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_i64_bound) else {
        return true;
    };
    range_may_satisfy_i64(lower, upper, predicate, value)
}

fn stats_may_satisfy_f64(
    stats: &IcebergColumnStats,
    predicate: &MinMaxPredicate,
    value: f64,
) -> bool {
    let Some(lower) = stats.lower_bound.as_deref().and_then(decode_f64_bound) else {
        return true;
    };
    let Some(upper) = stats.upper_bound.as_deref().and_then(decode_f64_bound) else {
        return true;
    };
    range_may_satisfy_f64(lower, upper, predicate, value)
}

fn stats_may_satisfy_bytes(
    stats: &IcebergColumnStats,
    predicate: &MinMaxPredicate,
    value: &[u8],
) -> bool {
    let Some(lower) = stats.lower_bound.as_deref() else {
        return true;
    };
    let Some(upper) = stats.upper_bound.as_deref() else {
        return true;
    };
    range_may_satisfy_bytes(lower, upper, predicate, value)
}

fn point_may_satisfy_i64(point: i64, predicate: &MinMaxPredicate, value: i64) -> bool {
    range_may_satisfy_i64(point, point, predicate, value)
}

fn point_may_satisfy_f64(point: f64, predicate: &MinMaxPredicate, value: f64) -> bool {
    range_may_satisfy_f64(point, point, predicate, value)
}

fn point_may_satisfy_bytes(point: &[u8], predicate: &MinMaxPredicate, value: &[u8]) -> bool {
    range_may_satisfy_bytes(point, point, predicate, value)
}

fn range_may_satisfy_i64(lower: i64, upper: i64, predicate: &MinMaxPredicate, value: i64) -> bool {
    match predicate {
        MinMaxPredicate::Le { .. } => lower <= value,
        MinMaxPredicate::Ge { .. } => upper >= value,
        MinMaxPredicate::Lt { .. } => lower < value,
        MinMaxPredicate::Gt { .. } => upper > value,
        MinMaxPredicate::Eq { .. } => lower <= value && value <= upper,
    }
}

fn range_may_satisfy_f64(lower: f64, upper: f64, predicate: &MinMaxPredicate, value: f64) -> bool {
    if lower.is_nan() || upper.is_nan() || value.is_nan() {
        return true;
    }
    match predicate {
        MinMaxPredicate::Le { .. } => lower <= value,
        MinMaxPredicate::Ge { .. } => upper >= value,
        MinMaxPredicate::Lt { .. } => lower < value,
        MinMaxPredicate::Gt { .. } => upper > value,
        MinMaxPredicate::Eq { .. } => lower <= value && value <= upper,
    }
}

fn range_may_satisfy_bytes(
    lower: &[u8],
    upper: &[u8],
    predicate: &MinMaxPredicate,
    value: &[u8],
) -> bool {
    match predicate {
        MinMaxPredicate::Le { .. } => lower <= value,
        MinMaxPredicate::Ge { .. } => upper >= value,
        MinMaxPredicate::Lt { .. } => lower < value,
        MinMaxPredicate::Gt { .. } => upper > value,
        MinMaxPredicate::Eq { .. } => lower <= value && value <= upper,
    }
}

fn decode_bool_bound(bytes: &[u8]) -> Option<bool> {
    match bytes {
        [0] => Some(false),
        [1] => Some(true),
        _ => None,
    }
}

fn decode_i64_bound(bytes: &[u8]) -> Option<i64> {
    match bytes.len() {
        1 => bytes.first().copied().map(i64::from),
        4 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(i64::from(i32::from_le_bytes(arr)))
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(i64::from_le_bytes(arr))
        }
        _ => None,
    }
}

fn decode_f64_bound(bytes: &[u8]) -> Option<f64> {
    match bytes.len() {
        4 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(f64::from(f32::from_le_bytes(arr)))
        }
        8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(f64::from_le_bytes(arr))
        }
        _ => None,
    }
}

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
    let thrift = planner.to_thrift_scan(
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
/// only needs to satisfy `lower::node::hdfs_scan` invariants: a
/// non-empty path. (The earlier embedded-JVM bridge keyed the same
/// way; that path has been replaced by `IcebergMetadataScanOp` —
/// see `src/connector/iceberg/metadata.rs`.)
fn build_iceberg_metadata_scan_range_params() -> internal_service::TScanRangeParams {
    let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
        None::<String>,
        Some(0),
        Some(0),
        None::<i64>,
        Some(0),
        Some(descriptors::THdfsFileFormat::PARQUET),
        None::<descriptors::TTextFileDesc>,
        Some("iceberg-metadata".to_string()),
        None::<Vec<String>>,
        None::<bool>,
        None::<Vec<plan_nodes::TIcebergDeleteFile>>,
        None::<i64>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<i64>,
        None::<crate::data_cache::TDataCacheOptions>,
        None::<Vec<types::TSlotId>>,
        None::<bool>,
        None::<BTreeMap<String, String>>,
        None::<Vec<types::TSlotId>>,
        Some(true),
        Some(String::new()),
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<plan_nodes::TPaimonDeletionFile>,
        None::<BTreeMap<types::TSlotId, exprs::TExpr>>,
        None::<descriptors::THdfsPartition>,
        None::<types::TTableId>,
        None::<plan_nodes::TDeletionVectorDescriptor>,
        None::<String>,
        None::<i64>,
        None::<bool>,
        None::<BTreeMap<i32, exprs::TExprMinMaxValue>>,
        None::<i32>,
        None::<i64>,
        None::<i64>,
    );
    internal_service::TScanRangeParams::new(
        plan_nodes::TScanRange::new(
            None::<plan_nodes::TInternalScanRange>,
            None::<Vec<u8>>,
            None::<plan_nodes::TBrokerScanRange>,
            None::<plan_nodes::TEsScanRange>,
            Some(hdfs_scan_range),
            None::<plan_nodes::TBinlogScanRange>,
            None::<plan_nodes::TBenchmarkScanRange>,
        ),
        None::<i32>,
        Some(false),
        Some(false),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::{
        PlannedScanTable, build_exec_params_multi, build_exec_params_multi_with_refresh_context,
    };
    use crate::connector::iceberg::scan_planner::build_hdfs_scan_range_params;
    use crate::connector::scan_planning::ConnectorScanPlanner;
    use crate::internal_service;
    use crate::sql::catalog::{
        ColumnDef, IcebergDataFileInfo, IcebergMvTargetStateScan, IcebergSchemaDef,
        IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::codegen::resolve::ResolvedTable;

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
        }
    }

    fn hdfs_range(
        params: &crate::internal_service::TScanRangeParams,
    ) -> &crate::plan_nodes::THdfsScanRange {
        params
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range")
    }

    fn test_connector_registry() -> crate::connector::ConnectorRegistry {
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(Arc::new(
            crate::connector::starrocks::table::StarRocksTableScanPlanner::stateless_for_codegen(),
        ));
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
            &[],
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
            Some(9),
            &[],
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
    fn starrocks_scan_ranges_use_planned_connector_scan_without_physical_layout() {
        use crate::connector::scan_planning::{ScanHandle, Split};
        use crate::connector::starrocks::table::{
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
                    row_id_column_name: "_row_id".to_string(),
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
        let scan = IcebergMvTargetStateScan {
            catalog: "ice".to_string(),
            database: "db".to_string(),
            table: "mv_b".to_string(),
            columns: Vec::new(),
            group_key_names: vec!["k".to_string()],
            aggregate_state_names: vec!["sum_v".to_string()],
            row_id_column_name: "__row_id__".to_string(),
        };

        assert_eq!(
            super::projected_target_state_column_names(&scan),
            vec![
                "k".to_string(),
                "sum_v".to_string(),
                "__row_id__".to_string()
            ]
        );
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
// Decode node
// ---------------------------------------------------------------------------

/// Build a `TDecodeNode` that maps dictionary-encoded INT slot ids back to
/// their string slot ids. The decode node is inserted above an upstream
/// subtree that exposes dict-encoded slots (typically a StarRocks scan); the
/// BE-side lowering in `src/lower/node/decode.rs` consumes the resulting
/// `TPlanNode`.
pub(crate) fn build_decode_node(
    node_id: i32,
    row_tuples: Vec<i32>,
    dict_id_to_string_ids: BTreeMap<i32, i32>,
) -> plan_nodes::TPlanNode {
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::DECODE_NODE;
    node.num_children = 1;
    node.limit = -1;
    node.row_tuples = row_tuples;
    node.nullable_tuples = vec![];
    node.compact_data = true;
    node.decode_node = Some(plan_nodes::TDecodeNode {
        dict_id_to_string_ids: Some(dict_id_to_string_ids),
        string_functions: None,
    });
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
    }
}
