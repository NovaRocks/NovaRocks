//! Physical plan layer — converts [`LogicalPlan`] into Thrift execution plans.
//!
//! This layer allocates physical resources (tuple_id, slot_id, node_id),
//! compiles `TypedExpr` into Thrift `TExpr`, and assembles the Thrift
//! plan structures expected by the pipeline executor.

pub(crate) mod boundary_schema;
pub(crate) mod descriptors;
pub(crate) mod expr_compiler;
pub(crate) mod fallback_audit;
pub(crate) mod fragment_builder;
pub(crate) mod helpers;
pub(crate) mod iceberg_write_sink;
pub(crate) mod id_binding_verifier;
pub(crate) mod nodes;
pub(crate) mod resolve;
pub(crate) mod type_infer;

use arrow::datatypes::DataType;

use crate::data_sinks;
use crate::descriptors as thrift_descriptors;
use crate::internal_service;
use crate::partitions;
use crate::plan_nodes;

use super::analysis::cte::CteId;

pub(crate) type FragmentId = u32;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) enum DirectExecPlan {
    AggregateStateMerge {
        old_input: Box<PlanBuildResult>,
        delta_input: Box<PlanBuildResult>,
        layout: crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
        branch_id: Option<i32>,
        pruning_limits: crate::engine::mv::refresh_context::MvRefreshPruningLimits,
        target_position_locator: Option<AggregateStateTargetPositionLocator>,
    },
    AggregateStatePhysicalize {
        input: Box<PlanBuildResult>,
        layout: crate::connector::starrocks::table::mv_agg_state::AggregateMvLayout,
    },
    UnionAll {
        inputs: Vec<PlanBuildResult>,
    },
}

#[derive(Clone)]
pub(crate) struct AggregateStateTargetPositionLocator {
    pub(crate) target_entry:
        std::sync::Arc<crate::connector::iceberg::catalog::registry::IcebergCatalogEntry>,
    pub(crate) target_table: iceberg::table::Table,
    pub(crate) partition_filter: crate::engine::mv::partition::TargetPartitionFilter,
    pub(crate) apply_key_column: String,
}

#[derive(Clone)]
pub(crate) struct PlanBuildResult {
    pub plan: plan_nodes::TPlan,
    pub desc_tbl: thrift_descriptors::TDescriptorTable,
    pub exec_params: internal_service::TPlanFragmentExecParams,
    pub output_columns: Vec<OutputColumn>,
    pub direct_exec: Option<Box<DirectExecPlan>>,
    #[allow(dead_code)]
    // Carried by single-fragment conversions so EXPLAIN/codegen consumers do
    // not lose boundary schema reports when a multi-fragment build collapses.
    pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
    /// Per-fragment dictionary payload required by `lower_decode_node` and
    /// `lower_lake_scan_node` to build their `query_global_dict_map`. When
    /// `LowCardinalityDictionaryRewrite` has fired, this carries one
    /// `TGlobalDict` per encoded slot id surfaced by this fragment;
    /// otherwise `None`. The execution path (`execute_plan`) must thread
    /// these into `lower_plan` — without them, every Decode in the plan
    /// fails with `missing query global dict for encoded slot_id=<N>`.
    pub query_global_dicts: Option<Vec<crate::data::TGlobalDict>>,
    /// Derived dictionary expressions keyed by target dict slot id. Mirrors
    /// `FragmentBuildResult.query_global_dict_exprs`. Empty today (Task 8
    /// derived-expr support is deferred); kept here so the execution path's
    /// `lower_plan` call signature stays consistent across single- and
    /// multi-fragment plans.
    pub query_global_dict_exprs: Option<std::collections::BTreeMap<i32, crate::exprs::TExpr>>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FragmentStreamKind {
    Gather,
    Broadcast,
    Partitioned,
    Other,
}

pub(crate) struct FragmentEdge {
    pub source_fragment_id: FragmentId,
    pub target_fragment_id: FragmentId,
    pub target_exchange_node_id: i32,
    #[allow(dead_code)]
    // populated by fragment builder, will be read when partition-aware exchange is enabled
    pub output_partition: partitions::TDataPartition,
    pub stream_kind: FragmentStreamKind,
    pub edge_kind: FragmentEdgeKind,
}

pub(crate) struct MultiFragmentBuildResult {
    /// Per-fragment build results.
    pub fragment_results: Vec<FragmentBuildResult>,
    /// Which fragment is the root (result sink).
    pub root_fragment_id: FragmentId,
    /// Fragment-to-fragment data edges.
    pub edges: Vec<FragmentEdge>,
    pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
    /// Runtime filter planning result (populated for standalone mode).
    pub rf_plan: Option<RuntimeFilterPlanResult>,
}

/// Result of lowering runtime-filter annotations to thrift.
///
/// Assembled by [`fragment_builder::PlanFragmentBuilder`] directly from the
/// `RuntimeFilterDesc` / `RuntimeFilterProbe` annotations attached to the
/// physical plan by `runtime_filter_pass`. Consumed by the execution
/// coordinator (`setup_runtime_filter_params`).
pub(crate) struct RuntimeFilterPlanResult {
    /// filter_id -> RF description.
    pub all_filters:
        std::collections::HashMap<i32, crate::runtime_filter::TRuntimeFilterDescription>,
    /// fragment_id -> build-side filter IDs in that fragment.
    pub build_side_filters: std::collections::HashMap<FragmentId, Vec<i32>>,
    /// fragment_id -> (filter_id, probe_target_node_id) for probe-side targets.
    pub probe_side_filters: std::collections::HashMap<FragmentId, Vec<(i32, i32)>>,
}

/// Physical emission result for a single fragment.
pub(crate) struct FragmentBuildResult {
    pub fragment_id: FragmentId,
    pub plan: plan_nodes::TPlan,
    pub desc_tbl: thrift_descriptors::TDescriptorTable,
    pub exec_params: internal_service::TPlanFragmentExecParams,
    #[allow(dead_code)]
    // populated by fragment builder, will be read when standalone multi-fragment execution is wired
    pub output_sink: data_sinks::TDataSink,
    pub output_exprs: Option<Vec<crate::exprs::TExpr>>,
    pub output_columns: Vec<OutputColumn>,
    pub direct_exec: Option<Box<DirectExecPlan>>,
    pub boundary_schemas: Vec<boundary_schema::BoundarySchemaReport>,
    /// CTE ID if this is a multicast fragment.
    pub cte_id: Option<CteId>,
    /// Exchange node IDs in this fragment that consume from CTE fragments:
    /// `(cte_id, exchange_node_id)`.
    pub cte_exchange_nodes: Vec<(CteId, i32)>,
    /// Per-fragment global dictionaries emitted to `TPlanFragment.query_global_dicts`.
    /// Populated by the fragment builder when a scan exposes a dict-encoded slot.
    /// `None` when this fragment has no dictionary-encoded slots.
    pub query_global_dicts: Option<Vec<crate::data::TGlobalDict>>,
    /// Per-fragment dictionary expressions emitted to
    /// `TPlanFragment.query_global_dict_exprs`. Wired through for Task 7+;
    /// today this stays `None` because no codegen path populates it.
    pub query_global_dict_exprs: Option<std::collections::BTreeMap<i32, crate::exprs::TExpr>>,
}
