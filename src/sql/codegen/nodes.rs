use std::collections::BTreeMap;

use crate::descriptors;
use crate::exprs;
use crate::internal_service;
use crate::partitions;
use crate::plan_nodes;
use crate::types;

use super::resolve::ResolvedTable;

use crate::sql::catalog::{IcebergDeleteFileFormat, IcebergDeleteFileInfo, TableStorage};

// ---------------------------------------------------------------------------
// Scan node
// ---------------------------------------------------------------------------

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
    node.conjuncts = if conjuncts.is_empty() {
        None
    } else {
        Some(conjuncts)
    };
    node.compact_data = true;

    let cloud_config = match &resolved.table.storage {
        TableStorage::S3ParquetFiles {
            cloud_properties, ..
        } => Some(crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(cloud_properties.clone()),
            None::<bool>,
        )),
        _ => None,
    };

    node.hdfs_scan_node = Some(plan_nodes::THdfsScanNode::new(
        Some(scan_tuple_id),
        None::<BTreeMap<types::TTupleId, Vec<exprs::TExpr>>>,
        None::<Vec<exprs::TExpr>>,
        None::<types::TTupleId>,
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
        Some(true), // can_use_any_column
        cloud_config,
        None::<bool>,
        None::<bool>,
        None::<bool>,
        None::<types::TTupleId>,
        None::<String>,
        None::<String>,
        None::<bool>,
        None::<String>,
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
    scan_tables: &[(i32, ResolvedTable)],
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    let mut per_node_scan_ranges = BTreeMap::new();

    for (scan_node_id, resolved) in scan_tables {
        let scan_node_id = *scan_node_id;
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
                        None,
                        None,
                        &[],
                    )?]
                }
                TableStorage::S3ParquetFiles { files, .. } => files
                    .iter()
                    .map(|f| {
                        build_hdfs_scan_range_params(
                            &f.path,
                            f.size,
                            f.first_row_id,
                            f.data_sequence_number,
                            &f.delete_files,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
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
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    delete_files: &[IcebergDeleteFileInfo],
) -> Result<internal_service::TScanRangeParams, String> {
    let mut parquet_delete_files = Vec::new();
    let mut deletion_vector_descriptor = None;
    for delete_file in delete_files {
        match delete_file.file_format {
            IcebergDeleteFileFormat::Parquet => {
                parquet_delete_files.push(plan_nodes::TIcebergDeleteFile::new(
                    Some(delete_file.path.clone()),
                    Some(descriptors::THdfsFileFormat::PARQUET),
                    Some(types::TIcebergFileContent::POSITION_DELETES),
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
    let hdfs_scan_range = plan_nodes::THdfsScanRange::new(
        None::<String>,
        Some(0_i64),
        Some(file_len),
        None::<i64>,
        None::<i64>, // file_length: let scan connector determine actual size
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
        None::<BTreeMap<types::TSlotId, exprs::TExpr>>,
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
