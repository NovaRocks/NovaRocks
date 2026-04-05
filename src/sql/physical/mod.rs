//! Physical plan layer — converts [`LogicalPlan`] into Thrift execution plans.
//!
//! This layer allocates physical resources (tuple_id, slot_id, node_id),
//! compiles `TypedExpr` into Thrift `TExpr`, and assembles the Thrift
//! plan structures expected by the pipeline executor.

pub(crate) mod descriptors;
pub(crate) mod emitter;
pub(crate) mod expr_compiler;
pub(crate) mod nodes;
pub(crate) mod resolve;
pub(crate) mod type_infer;

use arrow::datatypes::DataType;

use crate::data_sinks;
use crate::descriptors as thrift_descriptors;
use crate::internal_service;
use crate::partitions;
use crate::plan_nodes;

use super::catalog::CatalogProvider;
use super::cte::CteId;
use super::fragment::{FragmentId, FragmentPlan};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

pub(crate) struct PlanBuildResult {
    pub plan: plan_nodes::TPlan,
    pub desc_tbl: thrift_descriptors::TDescriptorTable,
    pub exec_params: internal_service::TPlanFragmentExecParams,
    pub output_columns: Vec<OutputColumn>,
}

#[derive(Clone)]
pub(crate) struct OutputColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Result of emitting a multi-fragment plan.
pub(crate) enum FragmentEdgeKind {
    Stream,
    CteMulticast { cte_id: CteId },
}

pub(crate) struct FragmentEdge {
    pub source_fragment_id: FragmentId,
    pub target_fragment_id: FragmentId,
    pub target_exchange_node_id: i32,
    pub output_partition: partitions::TDataPartition,
    pub edge_kind: FragmentEdgeKind,
}

pub(crate) struct MultiFragmentBuildResult {
    /// Per-fragment build results.
    pub fragment_results: Vec<FragmentBuildResult>,
    /// Which fragment is the root (result sink).
    pub root_fragment_id: FragmentId,
    /// Fragment-to-fragment data edges.
    pub edges: Vec<FragmentEdge>,
    /// Runtime filter planning result (populated for standalone mode).
    pub rf_plan: Option<crate::sql::cascades::runtime_filter_planner::RuntimeFilterPlanResult>,
}

/// Physical emission result for a single fragment.
pub(crate) struct FragmentBuildResult {
    pub fragment_id: FragmentId,
    pub plan: plan_nodes::TPlan,
    pub desc_tbl: thrift_descriptors::TDescriptorTable,
    pub exec_params: internal_service::TPlanFragmentExecParams,
    pub output_sink: data_sinks::TDataSink,
    pub output_columns: Vec<OutputColumn>,
    /// CTE ID if this is a multicast fragment.
    pub cte_id: Option<CteId>,
    /// Exchange node IDs in this fragment that consume from CTE fragments:
    /// `(cte_id, exchange_node_id)`.
    pub cte_exchange_nodes: Vec<(CteId, i32)>,
}

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

/// Emit a physical Thrift plan from an optimized logical plan.
pub(crate) fn emit(
    plan: super::plan::LogicalPlan,
    output_columns: &[super::ir::OutputColumn],
    catalog: &dyn CatalogProvider,
    current_database: &str,
) -> Result<PlanBuildResult, String> {
    emitter::emit(plan, output_columns, catalog, current_database)
}

/// Emit a multi-fragment physical plan from a FragmentPlan.
pub(crate) fn emit_multi_fragment(
    fragment_plan: FragmentPlan,
    catalog: &dyn CatalogProvider,
    current_database: &str,
) -> Result<MultiFragmentBuildResult, String> {
    emitter::emit_multi_fragment(fragment_plan, catalog, current_database)
}

// ---------------------------------------------------------------------------
// Parquet write utility (used by generate_series emission)
// ---------------------------------------------------------------------------

pub(crate) fn write_parquet_to_path(
    path: &std::path::Path,
    batch: &arrow::record_batch::RecordBatch,
) -> Result<(), String> {
    use parquet::arrow::ArrowWriter;

    let file = std::fs::File::create(path)
        .map_err(|e| format!("create local parquet file failed: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)
        .map_err(|e| format!("create local parquet writer failed: {e}"))?;
    writer
        .write(batch)
        .map_err(|e| format!("write local parquet batch failed: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close local parquet writer failed: {e}"))?;
    Ok(())
}
