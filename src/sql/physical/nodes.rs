use std::collections::BTreeMap;

use sqlparser::ast as sqlast;

use crate::descriptors;
use crate::exprs;
use crate::internal_service;
use crate::partitions;
use crate::plan_nodes;
use crate::types;

use super::expr_compiler::ExprCompiler;
use super::resolve::{ExprScope, ResolvedTable};

use crate::sql::catalog::{TableDef, TableStorage};

// ---------------------------------------------------------------------------
// Scan node
// ---------------------------------------------------------------------------

pub(super) fn build_scan_node(
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
    ));

    node
}

// ---------------------------------------------------------------------------
// Project node
// ---------------------------------------------------------------------------

pub(super) fn build_project_node(
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

pub(super) fn build_hash_join_node(
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
        plan_nodes::TJoinOp::LEFT_OUTER_JOIN | plan_nodes::TJoinOp::LEFT_ANTI_JOIN => (false, true),
        plan_nodes::TJoinOp::RIGHT_OUTER_JOIN | plan_nodes::TJoinOp::RIGHT_ANTI_JOIN => {
            (true, false)
        }
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

pub(super) fn build_nestloop_join_node(
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
        plan_nodes::TJoinOp::LEFT_OUTER_JOIN | plan_nodes::TJoinOp::LEFT_ANTI_JOIN => (false, true),
        plan_nodes::TJoinOp::RIGHT_OUTER_JOIN | plan_nodes::TJoinOp::RIGHT_ANTI_JOIN => {
            (true, false)
        }
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

pub(super) fn build_aggregation_node(
    node_id: i32,
    output_tuple_id: i32,
    intermediate_tuple_id: i32,
    grouping_exprs: Vec<exprs::TExpr>,
    aggregate_functions: Vec<exprs::TExpr>,
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
        need_finalize: true,
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

pub(super) fn build_sort_node(
    node_id: i32,
    tuple_id: i32,
    order_by: &[sqlast::OrderByExpr],
    scope: &ExprScope,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<plan_nodes::TPlanNode, String> {
    let mut ordering_exprs = Vec::new();
    let mut is_asc = Vec::new();
    let mut nulls_first = Vec::new();

    for item in order_by {
        let mut compiler = ExprCompiler::new(scope);
        let texpr = compiler.compile(&item.expr)?;
        ordering_exprs.push(texpr);
        let asc = item.options.asc.unwrap_or(true);
        is_asc.push(asc);
        nulls_first.push(item.options.nulls_first.unwrap_or(!asc));
    }

    let use_top_n = limit.is_some() && !ordering_exprs.is_empty();
    let node_limit = limit.unwrap_or(-1);

    let sort_info = plan_nodes::TSortInfo::new(
        ordering_exprs,
        is_asc,
        nulls_first,
        None::<Vec<exprs::TExpr>>,
    );

    let mut node = default_plan_node();
    node.node_id = node_id;
    node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
    node.num_children = 1;
    node.limit = node_limit;
    node.row_tuples = vec![tuple_id];
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

    Ok(node)
}

// ---------------------------------------------------------------------------
// Exec params (scan ranges)
// ---------------------------------------------------------------------------

pub(super) fn build_exec_params(
    table: &TableDef,
    scan_node_id: i32,
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    let scan_ranges = match &table.storage {
        TableStorage::LocalParquetFile { path } => {
            let metadata =
                std::fs::metadata(path).map_err(|e| format!("stat parquet file failed: {e}"))?;
            let file_len = i64::try_from(metadata.len())
                .map_err(|_| "parquet file is too large".to_string())?;
            vec![build_hdfs_scan_range_params(
                &path.display().to_string(),
                file_len,
            )]
        }
        TableStorage::S3ParquetFiles { files, .. } => files
            .iter()
            .map(|f| build_hdfs_scan_range_params(&f.path, f.size))
            .collect(),
    };

    Ok(internal_service::TPlanFragmentExecParams::new(
        types::TUniqueId::new(1, 1),
        types::TUniqueId::new(2, 2),
        BTreeMap::from([(scan_node_id, scan_ranges)]),
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

/// Build exec params for multiple scan nodes (used in JOIN queries).
pub(super) fn build_exec_params_multi(
    scan_tables: &[(i32, ResolvedTable)],
) -> Result<internal_service::TPlanFragmentExecParams, String> {
    let mut per_node_scan_ranges = BTreeMap::new();

    for (scan_node_id, resolved) in scan_tables {
        let scan_node_id = *scan_node_id;
        let ranges = match &resolved.table.storage {
            TableStorage::LocalParquetFile { path } => {
                let metadata = std::fs::metadata(path)
                    .map_err(|e| format!("stat parquet file failed: {e}"))?;
                let file_len = i64::try_from(metadata.len())
                    .map_err(|_| "parquet file is too large".to_string())?;
                vec![build_hdfs_scan_range_params(
                    &path.display().to_string(),
                    file_len,
                )]
            }
            TableStorage::S3ParquetFiles { files, .. } => files
                .iter()
                .map(|f| build_hdfs_scan_range_params(&f.path, f.size))
                .collect(),
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

// ---------------------------------------------------------------------------
// Scan range helper
// ---------------------------------------------------------------------------

fn build_hdfs_scan_range_params(
    full_path: &str,
    file_len: i64,
) -> internal_service::TScanRangeParams {
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
        None::<bool>,
        None::<String>,
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

// ---------------------------------------------------------------------------
// Default plan node
// ---------------------------------------------------------------------------

pub(super) fn default_plan_node() -> plan_nodes::TPlanNode {
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
        starrocks_scan_node: None,
        stream_scan_node: None,
        stream_join_node: None,
        stream_agg_node: None,
        select_node: None,
        fetch_node: None,
        look_up_node: None,
        benchmark_scan_node: None,
    }
}
