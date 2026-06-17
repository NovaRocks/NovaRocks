//! Transparent MV query rewrite (single-table SPJG + aggregate rollup).
//!
//! Design spec: docs/superpowers/specs/2026-06-10-mv-query-rewrite-design.md
//! StarRocks counterparts: MaterializedViewRewriter / AggregatedMaterializedViewRewriter.

pub(crate) mod aggregate_rollup;
pub(crate) mod column_mapping;
pub(crate) mod descriptor;
pub(crate) mod predicate_split;
pub(crate) mod rule;

use crate::sql::catalog::TableDef;
use descriptor::SpjgDescriptor;

pub(crate) const RULE_NAME: &str = "MvRewrite";

/// One usable MV candidate, fully prepared by the engine layer
/// (`src/engine/mv_rewrite_prep.rs`). Everything the optimizer rule needs;
/// no engine/catalog handles cross this boundary.
#[derive(Clone, Debug)]
pub(crate) struct MvRewriteCandidate {
    /// MV name, for logging and the EXPLAIN annotation.
    pub mv_name: String,
    /// SPJG decomposition of the MV defining query, expressed over the
    /// base table's ColumnIds (allocated in the shared ColumnRefFactory).
    pub mv: SpjgDescriptor,
    /// Database (namespace) of the MV target table, for ScanOp.
    pub target_database: String,
    /// Executable TableDef of the MV target table
    /// (ScanSource::IcebergDataFiles, binding = CurrentSnapshot).
    pub target_table: TableDef,
}
