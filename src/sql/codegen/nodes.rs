use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::common::min_max_predicate::MinMaxPredicate;
use crate::descriptors;
use crate::exprs;
use crate::internal_service;
use crate::lower::expr::parse_min_max_conjunct_with_column_resolver;
use crate::partitions;
use crate::plan_nodes;
use crate::types;

use super::resolve::ResolvedTable;

use crate::sql::catalog::{
    IcebergColumnStats, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
    IcebergPartitionValue, S3FileInfo, TableStorage,
};

// ---------------------------------------------------------------------------
// Scan node
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct PlannedScanTable {
    pub(crate) scan_node_id: i32,
    pub(crate) resolved: ResolvedTable,
    pub(crate) min_max_conjuncts: Vec<exprs::TExpr>,
    pub(crate) slot_to_column: HashMap<types::TSlotId, String>,
    pub(crate) iceberg_metadata_pseudo_column_slots: BTreeSet<types::TSlotId>,
}

const ICEBERG_SCAN_SPLIT_TARGET_BYTES: i64 = 128 * 1024 * 1024;
const ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE: usize = 1024;
const ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE: i64 = 512 * 1024 * 1024;

pub(crate) fn build_scan_node(
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
) -> plan_nodes::TPlanNode {
    if resolved.physical_layout.is_some() {
        return build_lake_scan_node(node_id, scan_tuple_id, resolved, conjuncts);
    }
    build_hdfs_scan_node(node_id, scan_tuple_id, resolved, conjuncts)
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

    let cloud_config = match &resolved.table.storage {
        TableStorage::S3ParquetFiles {
            cloud_properties, ..
        }
        | TableStorage::IcebergMetadataTable {
            cloud_properties, ..
        } => Some(crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties.clone()),
            None::<bool>,
        )),
        _ => None,
    };

    let (serialized_table, metadata_table_type) = match &resolved.table.storage {
        TableStorage::IcebergMetadataTable {
            metadata_table_type,
            serialized_table,
            ..
        } => (
            Some(serialized_table.clone()),
            Some(iceberg_metadata_table_type_thrift_str(metadata_table_type).to_string()),
        ),
        _ => (None, None),
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
        None::<String>,
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

fn build_lake_scan_node(
    node_id: i32,
    scan_tuple_id: i32,
    resolved: &ResolvedTable,
    conjuncts: Vec<exprs::TExpr>,
) -> plan_nodes::TPlanNode {
    let layout = resolved
        .physical_layout
        .as_ref()
        .expect("managed scan requires physical layout");
    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::LAKE_SCAN_NODE;
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
    node.lake_scan_node = Some(plan_nodes::TLakeScanNode {
        tuple_id: scan_tuple_id,
        key_column_name: vec![],
        key_column_type: vec![],
        is_preaggregation: false,
        sort_column: None,
        rollup_name: None,
        sql_predicates: None,
        enable_column_expr_predicate: None,
        dict_string_id_to_int_ids: None,
        unused_output_column_name: None,
        sort_key_column_names: None,
        bucket_exprs: None,
        column_access_paths: None,
        sorted_by_keys_per_tablet: None,
        output_chunk_by_bucket: None,
        output_asc_hint: None,
        partition_order_hint: None,
        enable_topn_filter_back_pressure: None,
        back_pressure_max_rounds: None,
        back_pressure_throttle_time: None,
        back_pressure_throttle_time_upper_bound: None,
        back_pressure_num_rows: None,
        schema_key: Some(descriptors::TTableSchemaKey::new(
            Some(layout.db_id),
            Some(layout.table_id),
            Some(layout.schema_id),
        )),
        enable_prune_column_after_index_filter: None,
        enable_gin_filter: None,
        next_uniq_id: None,
        enable_global_late_materialization: None,
    });

    node
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
    scan_tables: &[PlannedScanTable],
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    let mut per_node_scan_ranges = BTreeMap::new();

    for planned in scan_tables {
        let scan_node_id = planned.scan_node_id;
        let resolved = &planned.resolved;
        let ranges = if let Some(layout) = resolved.physical_layout.as_ref() {
            if layout.tablets.is_empty() {
                return Err(format!(
                    "managed table {}.{} has no active tablets",
                    resolved.database, resolved.table.name
                ));
            }
            layout
                .tablets
                .iter()
                .map(|tablet| build_internal_scan_range_params(resolved, layout, tablet))
                .collect()
        } else {
            match &resolved.table.storage {
                TableStorage::LocalParquetFile { path } => {
                    let metadata = std::fs::metadata(path)
                        .map_err(|e| format!("stat parquet file failed: {e}"))?;
                    let file_len = i64::try_from(metadata.len())
                        .map_err(|_| "parquet file is too large".to_string())?;
                    vec![build_hdfs_scan_range_params(
                        &path.display().to_string(),
                        file_len,
                        0,
                        file_len,
                        None,
                        None,
                        None,
                        None,
                        &[],
                    )?]
                }
                TableStorage::S3ParquetFiles { files, .. } => {
                    let file_predicates = scan_file_min_max_predicates(planned);
                    let change_op_slot = planned_change_op_slot(planned);
                    let mut ranges = Vec::new();
                    for file in files
                        .iter()
                        .filter(|f| file_may_satisfy_min_max(f, &file_predicates))
                    {
                        ranges.extend(build_hdfs_scan_range_params_for_file(file, change_op_slot)?);
                    }
                    ranges
                }
                TableStorage::IcebergMetadataTable { .. } => {
                    // The JVM metadata bridge produces all rows in a single
                    // call keyed off `serialized_table`. We still need at
                    // least one scan range so the runtime allocates a morsel
                    // and dispatches to `IcebergMetadataScanOp`.
                    vec![build_iceberg_metadata_scan_range_params()]
                }
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

fn scan_file_min_max_predicates(planned: &PlannedScanTable) -> Vec<MinMaxPredicate> {
    let mut predicates = Vec::new();
    for conjunct in &planned.min_max_conjuncts {
        let parsed = parse_min_max_conjunct_with_column_resolver(conjunct, |slot_ref| {
            planned
                .slot_to_column
                .get(&slot_ref.slot_id)
                .cloned()
                .ok_or_else(|| format!("slot_id {} has no scan column", slot_ref.slot_id))
        });
        if let Ok(Some(predicate)) = parsed {
            predicates.push(predicate);
        }
    }
    predicates
}

fn planned_change_op_slot(planned: &PlannedScanTable) -> Option<types::TSlotId> {
    planned
        .iceberg_metadata_pseudo_column_slots
        .iter()
        .copied()
        .find(|slot_id| {
            planned.slot_to_column.get(slot_id).is_some_and(|column| {
                column.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
            })
        })
}

fn int_literal_expr(value: i64) -> exprs::TExpr {
    exprs::TExpr::new(vec![super::expr_compiler::int_literal_node(value)])
}

fn file_may_satisfy_min_max(file: &S3FileInfo, predicates: &[MinMaxPredicate]) -> bool {
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

fn partition_may_satisfy_predicate(file: &S3FileInfo, predicate: &MinMaxPredicate) -> Option<bool> {
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

fn build_hdfs_scan_range_params_for_file(
    file: &S3FileInfo,
    change_op_slot: Option<types::TSlotId>,
) -> Result<Vec<internal_service::TScanRangeParams>, String> {
    validate_iceberg_delete_apply_cost(&file.path, &file.delete_files)?;
    let splits = plan_hdfs_file_splits(file);
    splits
        .into_iter()
        .map(|(offset, length)| {
            build_hdfs_scan_range_params(
                &file.path,
                file.size,
                offset,
                length,
                file.first_row_id,
                file.data_sequence_number,
                file.ivm_change_op,
                change_op_slot,
                &file.delete_files,
            )
        })
        .collect()
}

fn plan_hdfs_file_splits(file: &S3FileInfo) -> Vec<(i64, i64)> {
    let file_len = file.size.max(0);
    if file_len <= ICEBERG_SCAN_SPLIT_TARGET_BYTES
        || file.first_row_id.is_some()
        || !file.delete_files.is_empty()
    {
        return vec![(0, file_len)];
    }

    let mut out = Vec::new();
    let mut offset = 0_i64;
    while offset < file_len {
        let remaining = file_len - offset;
        let length = remaining.min(ICEBERG_SCAN_SPLIT_TARGET_BYTES);
        out.push((offset, length));
        offset += length;
    }
    if out.is_empty() {
        out.push((0, 0));
    }
    out
}

fn validate_iceberg_delete_apply_cost(
    data_path: &str,
    delete_files: &[IcebergDeleteFileInfo],
) -> Result<(), String> {
    if delete_files.len() > ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE {
        return Err(format!(
            "too many Iceberg delete files attached to data file {data_path}: count={} max={}",
            delete_files.len(),
            ICEBERG_DELETE_APPLY_MAX_FILES_PER_DATA_FILE
        ));
    }
    let total_bytes = delete_files.iter().try_fold(0_i64, |acc, delete_file| {
        let Some(length) = delete_file.length else {
            return Ok(acc);
        };
        acc.checked_add(length.max(0))
            .ok_or_else(|| format!("Iceberg delete file length overflow for data file {data_path}"))
    })?;
    if total_bytes > ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE {
        return Err(format!(
            "Iceberg delete files attached to data file {data_path} are too large: bytes={total_bytes} max={ICEBERG_DELETE_APPLY_MAX_BYTES_PER_DATA_FILE}"
        ));
    }
    Ok(())
}

fn build_internal_scan_range_params(
    resolved: &ResolvedTable,
    layout: &crate::sql::catalog::PhysicalTableLayout,
    tablet: &crate::sql::catalog::ManagedTabletRef,
) -> internal_service::TScanRangeParams {
    let internal_scan_range = plan_nodes::TInternalScanRange::new(
        vec![],
        layout.schema_id.to_string(),
        tablet.version.to_string(),
        tablet.version.to_string(),
        tablet.tablet_id,
        resolved.database.clone(),
        None::<Vec<plan_nodes::TKeyRange>>,
        None::<String>,
        Some(resolved.table.name.clone()),
        Some(tablet.partition_id),
        None::<i64>,
        Some(true),
        None::<i32>,
        Some(false),
        Some(false),
        None::<i64>,
    );

    internal_service::TScanRangeParams::new(
        plan_nodes::TScanRange::new(
            Some(internal_scan_range),
            None::<Vec<u8>>,
            None::<plan_nodes::TBrokerScanRange>,
            None::<plan_nodes::TEsScanRange>,
            None::<plan_nodes::THdfsScanRange>,
            None::<plan_nodes::TBinlogScanRange>,
            None::<plan_nodes::TBenchmarkScanRange>,
        ),
        None::<i32>,
        Some(false),
        Some(false),
    )
}

// ---------------------------------------------------------------------------
// Scan range helper
// ---------------------------------------------------------------------------

fn build_hdfs_scan_range_params(
    full_path: &str,
    file_len: i64,
    offset: i64,
    length: i64,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    ivm_change_op: Option<i8>,
    change_op_slot: Option<types::TSlotId>,
    delete_files: &[IcebergDeleteFileInfo],
) -> Result<internal_service::TScanRangeParams, String> {
    let mut parquet_delete_files = Vec::new();
    let mut deletion_vector_descriptor = None;
    for delete_file in delete_files {
        match delete_file.file_format {
            IcebergDeleteFileFormat::Parquet => {
                let file_content = match delete_file.file_content {
                    IcebergDeleteFileContent::Position => {
                        types::TIcebergFileContent::POSITION_DELETES
                    }
                    IcebergDeleteFileContent::Equality => {
                        // Equality field IDs are read from the equality-delete Parquet schema by
                        // the Rust scan runner. The Thrift scan range only needs to identify the
                        // delete file as an equality-delete file.
                        types::TIcebergFileContent::EQUALITY_DELETES
                    }
                };
                parquet_delete_files.push(plan_nodes::TIcebergDeleteFile::new(
                    Some(delete_file.path.clone()),
                    Some(descriptors::THdfsFileFormat::PARQUET),
                    Some(file_content),
                    delete_file.length,
                ));
            }
            IcebergDeleteFileFormat::Puffin => {
                if deletion_vector_descriptor.is_some() {
                    return Err(format!(
                        "multiple Puffin deletion vectors are attached to data file {}",
                        full_path
                    ));
                }
                let offset = delete_file.content_offset.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_offset",
                        delete_file.path, full_path
                    )
                })?;
                let size = delete_file.content_size_in_bytes.ok_or_else(|| {
                    format!(
                        "Puffin deletion vector {} for data file {} is missing content_size_in_bytes",
                        delete_file.path, full_path
                    )
                })?;
                deletion_vector_descriptor = Some(plan_nodes::TDeletionVectorDescriptor::new(
                    Some("PUFFIN".to_string()),
                    Some(delete_file.path.clone()),
                    Some(offset),
                    Some(size),
                    None::<i64>,
                ));
            }
        }
    }
    let parquet_delete_files = if parquet_delete_files.is_empty() {
        None
    } else {
        Some(parquet_delete_files)
    };
    let extended_columns = match (ivm_change_op, change_op_slot) {
        (Some(op), Some(slot_id)) => {
            crate::exec::change_op::validate_change_op_value(op)?;
            Some(BTreeMap::from([(slot_id, int_literal_expr(op as i64))]))
        }
        _ => None,
    };
    let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
        None::<String>,
        Some(offset),
        Some(length),
        None::<i64>,
        Some(file_len),
        Some(descriptors::THdfsFileFormat::PARQUET),
        None::<descriptors::TTextFileDesc>,
        Some(full_path.to_string()),
        None::<Vec<String>>,
        None::<bool>,
        parquet_delete_files,
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
        None::<bool>,
        None::<String>,
        None::<bool>,
        None::<String>,
        None::<String>,
        None::<plan_nodes::TPaimonDeletionFile>,
        extended_columns,
        None::<descriptors::THdfsPartition>,
        None::<types::TTableId>,
        deletion_vector_descriptor,
        None::<String>,
        None::<i64>,
        None::<bool>,
        None::<BTreeMap<i32, exprs::TExprMinMaxValue>>,
        None::<i32>,
        first_row_id,
        data_sequence_number,
    );

    Ok(internal_service::TScanRangeParams::new(
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
    ))
}

/// Build a single placeholder scan range that drives the iceberg metadata
/// JVM bridge. The bridge keys off `serialized_table` on the
/// `THdfsScanNode`, so the per-range payload only needs to satisfy
/// `lower::node::hdfs_scan` invariants: a non-empty path and the
/// `use_iceberg_jni_metadata_reader` flag set.
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

    use arrow::datatypes::DataType;

    use super::{PlannedScanTable, build_exec_params_multi, build_hdfs_scan_range_params};
    use crate::sql::catalog::{ColumnDef, S3FileInfo, TableDef, TableStorage};
    use crate::sql::codegen::resolve::ResolvedTable;

    fn hdfs_range(
        params: &crate::internal_service::TScanRangeParams,
    ) -> &crate::plan_nodes::THdfsScanRange {
        params
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .expect("hdfs scan range")
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
        let planned = PlannedScanTable {
            scan_node_id: 3,
            resolved: ResolvedTable {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
                        data_type: DataType::Int8,
                        nullable: false,
                        write_default: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: TableStorage::S3ParquetFiles {
                        files: vec![S3FileInfo {
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
                        }],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                physical_layout: None,
                alias: None,
            },
            min_max_conjuncts: vec![],
            slot_to_column: HashMap::from([(
                9,
                crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
            )]),
            iceberg_metadata_pseudo_column_slots: Default::default(),
        };

        let params = build_exec_params_multi(&[planned]).expect("build scan ranges");
        let ranges = params
            .per_node_scan_ranges
            .get(&3)
            .expect("scan node ranges");

        assert_eq!(ranges.len(), 1);
        assert!(hdfs_range(&ranges[0]).extended_columns.is_none());
    }

    #[test]
    fn metadata_change_op_column_emits_extended_columns() {
        let planned = PlannedScanTable {
            scan_node_id: 3,
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
                    }],
                    iceberg_table: None,
                    storage: TableStorage::S3ParquetFiles {
                        files: vec![S3FileInfo {
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
                        }],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                physical_layout: None,
                alias: None,
            },
            min_max_conjuncts: vec![],
            slot_to_column: HashMap::from([(
                9,
                crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
            )]),
            iceberg_metadata_pseudo_column_slots: [9].into(),
        };

        let params = build_exec_params_multi(&[planned]).expect("build scan ranges");
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
    }
}
