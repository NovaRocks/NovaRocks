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

use crate::descriptors as thrift_descriptors;
use crate::internal_service;
use crate::plan_nodes;

use super::catalog::CatalogProvider;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

pub(crate) struct PlanBuildResult {
    pub plan: plan_nodes::TPlan,
    pub desc_tbl: thrift_descriptors::TDescriptorTable,
    pub exec_params: internal_service::TPlanFragmentExecParams,
    pub output_columns: Vec<OutputColumn>,
}

pub(crate) struct OutputColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
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
