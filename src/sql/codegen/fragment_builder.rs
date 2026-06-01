//! PlanFragmentBuilder — converts a PhysicalPlanNode tree into Thrift TPlan
//! per fragment.
//!
//! Fragment boundaries are created at `PhysicalDistribution` nodes.
//! `PhysicalCTEProduce` / `PhysicalCTEConsume` create multicast fragments
//! whose sinks are wired by the `ExecutionCoordinator` after building.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::rc::Rc;

use arrow::datatypes::DataType;

use crate::data_sinks;
use crate::exprs;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::partitions;
use crate::plan_nodes;
use crate::types;

use crate::sql::analysis::cte::CteId;
use crate::sql::catalog::CatalogProvider;
use crate::sql::codegen::FragmentId;
use crate::sql::codegen::descriptors::DescriptorTableBuilder;
use crate::sql::codegen::expr_compiler::{self, ExprCompiler};
use crate::sql::codegen::helpers::{
    agg_call_display_name, join_kind_to_op, split_and_conjuncts_typed, typed_expr_display_name,
};
use crate::sql::codegen::nodes;
use crate::sql::codegen::resolve::{ColumnBinding, ExprScope, ResolvedTable};
use crate::sql::codegen::type_infer;
use crate::sql::codegen::{
    FragmentBuildResult, FragmentEdge, FragmentEdgeKind, MultiFragmentBuildResult, OutputColumn,
};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::operator::{
    AggMode, PhysicalCTEAnchorOp, PhysicalCTEConsumeOp, PhysicalCTEProduceOp, PhysicalDecodeOp,
    PhysicalDistributionOp, PhysicalExceptOp, PhysicalFilterOp, PhysicalGenerateSeriesOp,
    PhysicalHashAggregateOp, PhysicalHashJoinOp, PhysicalIntersectOp, PhysicalLimitOp,
    PhysicalNestLoopJoinOp, PhysicalProjectOp, PhysicalRepeatOp, PhysicalScanOp, PhysicalSortOp,
    PhysicalSubqueryAliasOp, PhysicalTableFunctionOp, PhysicalTopNOp, PhysicalUnionOp,
    PhysicalValuesOp, PhysicalWindowOp, ScanDictionaryColumn,
};
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use crate::sql::optimizer::property::{OrderingSpec, window_ordering_spec};

use crate::sql::analysis::{ExprKind, JoinKind, LiteralValue, TypedExpr};
use crate::sql::planner::plan::AggregateCall;

// ---------------------------------------------------------------------------
// Internal visitor result
// ---------------------------------------------------------------------------

struct VisitResult {
    /// Plan nodes in pre-order (top-down) traversal order.
    plan_nodes: Vec<plan_nodes::TPlanNode>,
    /// Scope describing the output columns with their physical bindings.
    scope: ExprScope,
    /// Tuple IDs in this subtree's output.
    tuple_ids: Vec<i32>,
    /// Exchange nodes in this fragment that consume from CTE fragments:
    /// `(cte_id, exchange_node_id)`.
    cte_exchange_nodes: Vec<(CteId, i32)>,
    /// Physical ordering currently provided by this subtree, if it can be
    /// represented by column ids.
    ordering: OrderingSpec,
}

fn limit_child_can_apply_offset_locally(child: &PhysicalPlanNode) -> bool {
    matches!(
        &child.op,
        Operator::PhysicalSort(_) | Operator::PhysicalTopN(_)
    )
}


/// Probe-side target recorded while visiting a node that carries a
/// `RuntimeFilterProbe` annotation. The build-side hash join (visited AFTER
/// its probe descendants) looks this up by `filter_id` to wire the RF's
/// `plan_node_id_to_target_expr` and the probe-side prober params.
#[derive(Clone, Debug)]
struct RfProbeTarget {
    /// Thrift node id of the node that consumes the probe (scan or the
    /// root thrift node of an intermediate operator's subtree).
    thrift_node_id: i32,
    /// Probe key expression compiled against the target node's output scope.
    probe_texpr: exprs::TExpr,
    /// Fragment that owns the probe target node.
    fragment_id: FragmentId,
}

/// Standalone-mode pipeline DOP used for the RF layout's
/// `num_drivers_per_instance`. Mirrors the historical post-pass computation.
fn rf_pipeline_dop() -> i32 {
    std::thread::available_parallelism()
        .map(|p| p.get().min(4))
        .unwrap_or(4) as i32
}

/// Remap a runtime filter's `expr_order` from the join's PRE-demote
/// `op.eq_conditions` index space to the POST-demote `eq_join_conjuncts`
/// index space that BE lowering indexes (`src/lower/node/hash_join.rs`).
///
/// `surviving_eq_origin[j]` is the original `op.eq_conditions` index of the
/// `j`-th surviving (non-demoted) `eq_join_conjuncts` entry — built in
/// `visit_hash_join` as eq conditions are compiled and kept. Demoted
/// conditions never get an entry, so the vec is the post-demote conjunct list
/// keyed by its source index.
///
/// Returns:
/// - `Some(j)` when the RF's original conjunct survived demotion, where `j`
///   is its post-demote index.
/// - `None` when the RF's conjunct was demoted to `other_join_conjuncts` (it
///   is no longer an equi-join key at execution) — the caller MUST drop the RF.
fn remap_rf_expr_order(
    surviving_eq_origin: &[usize],
    pre_demote_expr_order: usize,
) -> Option<usize> {
    surviving_eq_origin
        .iter()
        .position(|&origin| origin == pre_demote_expr_order)
}

/// Map a join `JoinDistribution` to the thrift RF
/// `(build_join_mode, local_layout, global_layout)` triple. Copied verbatim
/// from the v1 post-pass so the wire encoding is identical.
fn rf_layout_for_distribution(
    distribution: &crate::sql::optimizer::operator::JoinDistribution,
) -> (
    crate::runtime_filter::TRuntimeFilterBuildJoinMode,
    crate::runtime_filter::TRuntimeFilterLayoutMode,
    crate::runtime_filter::TRuntimeFilterLayoutMode,
) {
    use crate::runtime_filter::{TRuntimeFilterBuildJoinMode, TRuntimeFilterLayoutMode};
    use crate::sql::optimizer::operator::JoinDistribution;
    match distribution {
        JoinDistribution::Broadcast => (
            TRuntimeFilterBuildJoinMode::BORADCAST,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::SINGLETON,
        ),
        JoinDistribution::Shuffle => (
            TRuntimeFilterBuildJoinMode::PARTITIONED,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L,
        ),
        JoinDistribution::Colocate => (
            TRuntimeFilterBuildJoinMode::COLOCATE,
            TRuntimeFilterLayoutMode::SINGLETON,
            TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L,
        ),
    }
}

fn iceberg_table_info(
    source: &crate::sql::catalog::ScanSource,
) -> Option<&crate::sql::catalog::IcebergTableInfo> {
    match source {
        crate::sql::catalog::ScanSource::IcebergDataFiles { table, .. }
        | crate::sql::catalog::ScanSource::IcebergMetadataTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergDeltaTable { table, .. }
        | crate::sql::catalog::ScanSource::IcebergVersionTable { table, .. } => Some(table),
        crate::sql::catalog::ScanSource::StarRocks { .. } => None,
    }
}

fn add_iceberg_equality_delete_required_columns(
    required: &mut std::collections::HashSet<String>,
    table: &crate::sql::catalog::TableDef,
) -> Result<(), String> {
    let crate::sql::catalog::ScanSource::IcebergDataFiles {
        table: iceberg,
        files,
        ..
    } = &table.source
    else {
        return Ok(());
    };
    let field_id_to_name: HashMap<i32, String> = iceberg
        .schema
        .fields
        .iter()
        .map(|field| (field.field_id, field.name.clone()))
        .collect();
    for file in files {
        for delete_file in &file.delete_files {
            if delete_file.file_content != crate::sql::catalog::IcebergDeleteFileContent::Equality {
                continue;
            }
            if delete_file.equality_field_ids.is_empty() {
                for column in &delete_file.equality_column_names {
                    required.insert(column.to_lowercase());
                }
                continue;
            }
            for field_id in &delete_file.equality_field_ids {
                let column = field_id_to_name.get(field_id).ok_or_else(|| {
                    format!(
                        "iceberg equality-delete file {} references unknown field id {} in table {}",
                        delete_file.path, field_id, table.name
                    )
                })?;
                required.insert(column.to_lowercase());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// PlanFragmentBuilder
// ---------------------------------------------------------------------------

pub(crate) struct PlanFragmentBuilder<'a> {
    // Retained for Stage 5 cleanup that also drops `CatalogProvider::get_physical_layout`.
    // Codegen no longer reads `self.catalog`; the field stays so the builder can keep
    // its existing constructor shape during the transition.
    #[allow(dead_code)]
    catalog: &'a dyn CatalogProvider,
    connectors: &'a crate::connector::ConnectorRegistry,
    desc_builder: DescriptorTableBuilder,
    scan_tables: Vec<nodes::PlannedScanTable>,
    next_node_id: i32,
    /// Slot ids are shared with `ExprCompiler` so that lambda-parameter slot
    /// ids stay unique across the entire query. Wrapped in `Rc<RefCell<_>>`
    /// to allow handing out an allocator handle without borrowing the whole
    /// builder.
    next_slot_id: Rc<RefCell<i32>>,
    next_tuple_id: i32,
    next_fragment_id: FragmentId,
    /// Fragment ids for current visit context. Top is active fragment id.
    fragment_stack: Vec<FragmentId>,
    /// Fragments finalized during visitation (child fragments from distribution
    /// boundaries and CTE produce fragments).
    completed_fragments: Vec<FragmentBuildResult>,
    /// Fragment-to-fragment stream/multicast edges.
    completed_edges: Vec<FragmentEdge>,
    /// CTE ID -> index in `completed_fragments`.
    cte_fragments: HashMap<CteId, usize>,
    /// Per-fragment accumulator of `TGlobalDict` entries emitted by scans
    /// with non-empty `dict_columns`. Drained into
    /// `FragmentBuildResult.query_global_dicts` when each fragment is
    /// finalized. Empty in all production paths until Task 7+.
    query_global_dicts_per_fragment: HashMap<FragmentId, Vec<crate::data::TGlobalDict>>,
    /// Maps each slot id that currently carries dict-encoded data to the
    /// (strings, ids, version) of the source dictionary. When a new
    /// operator allocates a slot whose value flows from a tracked slot
    /// (Aggregate's group-by passthrough, Project's column-ref item,
    /// Decode's passthrough, etc.), the operator re-registers the same
    /// dict against the new slot id so the downstream consumer (the
    /// `Decode` node above an exchange, in the parent fragment) finds
    /// its `dict_id_to_string_ids` key in the fragment's
    /// `query_global_dicts`. Without this map, the dict registration
    /// stays pinned to the scan's slot id and never reaches the slot id
    /// the parent fragment's Decode actually receives.
    slot_to_global_dict: HashMap<i32, crate::data::TGlobalDict>,
    /// OQ-5 runtime-filter lowering: probe targets recorded as nodes carrying
    /// `probe_runtime_filters` are visited. Keyed by `filter_id`; consumed by
    /// `visit_hash_join` to wire each build descriptor to its probe node.
    /// Probe descendants are always visited before the owning join (children
    /// first), so the lookup is populated by the time the join needs it.
    rf_probe_targets: HashMap<i32, RfProbeTarget>,
    /// Accumulated `filter_id -> TRuntimeFilterDescription` across all joins.
    rf_all_filters: HashMap<i32, crate::runtime_filter::TRuntimeFilterDescription>,
    /// Accumulated build-side filter ids per fragment (the join's fragment).
    rf_build_side_filters: HashMap<FragmentId, Vec<i32>>,
    /// Accumulated probe-side `(filter_id, probe_target_node_id)` per fragment.
    rf_probe_side_filters: HashMap<FragmentId, Vec<(i32, i32)>>,
}

impl<'a> PlanFragmentBuilder<'a> {
    // -------------------------------------------------------------------
    // Public entry
    // -------------------------------------------------------------------

    pub(crate) fn build(
        plan: &PhysicalPlanNode,
        catalog: &'a dyn CatalogProvider,
        connectors: &'a crate::connector::ConnectorRegistry,
        _current_database: &str,
    ) -> Result<MultiFragmentBuildResult, String> {
        let mut builder = PlanFragmentBuilder {
            catalog,
            connectors,
            desc_builder: DescriptorTableBuilder::new(),
            scan_tables: Vec::new(),
            next_node_id: 1,
            next_slot_id: Rc::new(RefCell::new(1)),
            next_tuple_id: 1,
            next_fragment_id: 0,
            fragment_stack: Vec::new(),
            completed_fragments: Vec::new(),
            completed_edges: Vec::new(),
            cte_fragments: HashMap::new(),
            query_global_dicts_per_fragment: HashMap::new(),
            slot_to_global_dict: HashMap::new(),
            rf_probe_targets: HashMap::new(),
            rf_all_filters: HashMap::new(),
            rf_build_side_filters: HashMap::new(),
            rf_probe_side_filters: HashMap::new(),
        };

        // Elide a root-level Gather: on a single node the top-level gather
        // adds an unnecessary fragment boundary.
        let plan = match &plan.op {
            Operator::PhysicalDistribution(op)
                if matches!(
                    op.spec,
                    crate::sql::optimizer::property::DistributionSpec::Gather
                ) =>
            {
                plan.children
                    .first()
                    .ok_or_else(|| "root PhysicalDistribution(Gather) missing child".to_string())?
            }
            _ => plan,
        };

        let root_fragment_id = builder.alloc_fragment_id();
        builder.fragment_stack.push(root_fragment_id);
        let result = builder.visit(plan)?;

        // Build the shared descriptor table and exec params.  All fragments
        // share the same descriptor table and scan ranges since the
        // coordinator rewires instance IDs and sinks after the fact.
        let desc_tbl =
            std::mem::replace(&mut builder.desc_builder, DescriptorTableBuilder::new()).build();

        let exec_params = nodes::build_exec_params_multi(builder.connectors, &builder.scan_tables)?;

        let output_columns = plan
            .output_columns
            .iter()
            .map(|c| OutputColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            })
            .collect();

        // Build the root fragment with a result sink. Drain the per-fragment
        // dictionary accumulator into `query_global_dicts` — for Task 6 this
        // accumulator is always empty unless a test populates `dict_columns`.
        let root_dicts = builder
            .query_global_dicts_per_fragment
            .remove(&root_fragment_id)
            .filter(|v| !v.is_empty());
        let root_fragment = FragmentBuildResult {
            fragment_id: root_fragment_id,
            plan: plan_nodes::TPlan::new(result.plan_nodes),
            desc_tbl: desc_tbl.clone(),
            exec_params: exec_params.clone(),
            output_sink: build_result_sink(),
            output_columns,
            cte_id: None,
            cte_exchange_nodes: result.cte_exchange_nodes,
            // Dictionary plumbing: populated when Task 7+ inserts dict slots.
            query_global_dicts: root_dicts,
            query_global_dict_exprs: None,
        };

        // Patch all completed (child) fragments with the shared descriptor
        // table and exec params.
        for frag in &mut builder.completed_fragments {
            frag.desc_tbl = desc_tbl.clone();
            frag.exec_params = exec_params.clone();
        }

        // Assemble all fragments: completed child fragments first, then root.
        let mut fragment_results = builder.completed_fragments;
        fragment_results.push(root_fragment);

        // OQ-5: the runtime-filter descriptors were lowered during
        // `visit_hash_join` (and already patched onto the join thrift nodes).
        // Assemble the coordinator-facing result from the builder's
        // accumulators. `None` when no join produced a filter.
        let rf_plan = if builder.rf_all_filters.is_empty() {
            None
        } else {
            Some(crate::sql::codegen::RuntimeFilterPlanResult {
                all_filters: builder.rf_all_filters,
                build_side_filters: builder.rf_build_side_filters,
                probe_side_filters: builder.rf_probe_side_filters,
            })
        };

        Ok(MultiFragmentBuildResult {
            fragment_results,
            root_fragment_id,
            edges: builder.completed_edges,
            rf_plan,
        })
    }

    // -------------------------------------------------------------------
    // ID allocators
    // -------------------------------------------------------------------

    fn alloc_node(&mut self) -> i32 {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    fn alloc_slot(&mut self) -> i32 {
        let mut next = self.next_slot_id.borrow_mut();
        let id = *next;
        *next += 1;
        id
    }

    /// Return a shared handle to the slot id allocator. Hand this to
    /// `ExprCompiler::new_with_slot_alloc` so lambda parameter slots are
    /// allocated from the same monotonic counter as physical tuple slots.
    fn slot_allocator(&self) -> expr_compiler::SlotAllocator {
        Rc::clone(&self.next_slot_id)
    }

    fn alloc_tuple(&mut self) -> i32 {
        let id = self.next_tuple_id;
        self.next_tuple_id += 1;
        id
    }

    fn alloc_fragment_id(&mut self) -> FragmentId {
        let id = self.next_fragment_id;
        self.next_fragment_id += 1;
        id
    }

    fn current_fragment_id(&self) -> Result<FragmentId, String> {
        self.fragment_stack
            .last()
            .copied()
            .ok_or_else(|| "no active fragment id in builder".to_string())
    }

    /// Re-register the source slot's TGlobalDict on a NEW slot id. Used
    /// when an operator allocates a fresh slot that holds the same dict-
    /// encoded values as the source (Aggregate group-by passthrough,
    /// Project column-ref passthrough, Decode passthrough, etc.). The
    /// dict is also pushed onto every fragment on the current stack so a
    /// parent-fragment Decode finds it in its own `query_global_dicts`.
    /// No-op when the source slot has no dict registered.
    fn propagate_dict_to_slot(&mut self, source_slot_id: i32, new_slot_id: i32) {
        if source_slot_id == new_slot_id {
            return;
        }
        let Some(source_dict) = self.slot_to_global_dict.get(&source_slot_id).cloned() else {
            return;
        };
        // Build a new TGlobalDict carrying the new slot's id so the BE's
        // `query_global_dict_map` is keyed correctly.
        let new_dict = crate::data::TGlobalDict::new(
            Some(new_slot_id),
            source_dict.strings.clone(),
            source_dict.ids.clone(),
            source_dict.version,
        );
        let fragments: Vec<FragmentId> = if self.fragment_stack.is_empty() {
            self.current_fragment_id()
                .ok()
                .map(|f| vec![f])
                .unwrap_or_default()
        } else {
            self.fragment_stack.clone()
        };
        for fragment_id in fragments {
            self.query_global_dicts_per_fragment
                .entry(fragment_id)
                .or_default()
                .push(new_dict.clone());
        }
        self.slot_to_global_dict.insert(new_slot_id, new_dict);
    }

    // -------------------------------------------------------------------
    // Dispatcher
    // -------------------------------------------------------------------

    fn visit(&mut self, node: &PhysicalPlanNode) -> Result<VisitResult, String> {
        let result = match &node.op {
            Operator::PhysicalScan(op) => self.visit_scan(op, node),
            Operator::PhysicalFilter(op) => self.visit_filter(op, node),
            Operator::PhysicalProject(op) => self.visit_project(op, node),
            Operator::PhysicalHashJoin(op) => self.visit_hash_join(op, node),
            Operator::PhysicalNestLoopJoin(op) => self.visit_nest_loop_join(op, node),
            Operator::PhysicalHashAggregate(op) => self.visit_hash_aggregate(op, node),
            Operator::PhysicalSort(op) => self.visit_sort(op, node),
            Operator::PhysicalTopN(op) => self.visit_physical_top_n(op, node),
            Operator::PhysicalLimit(op) => self.visit_limit(op, node),
            Operator::PhysicalWindow(op) => self.visit_window(op, node),
            Operator::PhysicalValues(op) => self.visit_values(op, node),
            Operator::PhysicalGenerateSeries(op) => self.visit_generate_series(op, node),
            Operator::PhysicalTableFunction(op) => self.visit_table_function(op, node),
            Operator::PhysicalSubqueryAlias(op) => self.visit_subquery_alias(op, node),
            Operator::PhysicalRepeat(op) => self.visit_repeat(op, node),
            Operator::PhysicalDistribution(op) => self.visit_distribution(op, node),
            Operator::PhysicalCTEAnchor(op) => self.visit_cte_anchor(op, node),
            Operator::PhysicalCTEProduce(op) => self.visit_cte_produce(op, node),
            Operator::PhysicalCTEConsume(op) => self.visit_cte_consume(op),
            Operator::PhysicalUnion(op) => self.visit_union(op, node),
            Operator::PhysicalIntersect(op) => self.visit_intersect(op, node),
            Operator::PhysicalExcept(op) => self.visit_except(op, node),
            Operator::PhysicalDecode(op) => self.visit_decode(op, node),
            // Logical operators should never appear in an extracted physical plan
            other if other.is_logical() => {
                return Err(format!(
                    "unexpected logical operator in physical plan: {:?}",
                    other
                ));
            }
            other => {
                return Err(format!(
                    "unhandled operator in fragment builder: {:?}",
                    other
                ));
            }
        }?;

        // OQ-5: record any probe-side runtime filters that the
        // physical-tree pass attached to THIS node. The probe target is the
        // thrift node at the root of this subtree (the node's own thrift node;
        // for pass-through visitors such as filter this is the underlying
        // scan node). Probe targets are recorded before the owning hash join
        // is visited because children are visited first.
        self.record_probe_targets(node, &result);

        Ok(result)
    }

    /// Compile each `probe_runtime_filters` entry on `node` against the
    /// subtree's output scope and stash the resulting probe target so the
    /// owning hash join can wire it. No-op when the node carries no probe
    /// filters or produced no thrift node.
    ///
    /// A runtime filter is an optimization: if the probe expression cannot be
    /// compiled against this node's scope (it should always succeed for a
    /// probe the physical pass placed here by ColumnId, but we stay defensive),
    /// we simply skip recording the target. The owning join then emits a
    /// build-only descriptor rather than failing the whole query.
    fn record_probe_targets(&mut self, node: &PhysicalPlanNode, result: &VisitResult) {
        if node.probe_runtime_filters.is_empty() {
            return;
        }
        let Some(target_node) = result.plan_nodes.first() else {
            return;
        };
        let thrift_node_id = target_node.node_id;
        let Ok(fragment_id) = self.current_fragment_id() else {
            return;
        };
        for probe in &node.probe_runtime_filters {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &result.scope);
            let Ok(probe_texpr) = compiler.compile_typed(&probe.probe_expr) else {
                continue;
            };
            self.rf_probe_targets.insert(
                probe.filter_id,
                RfProbeTarget {
                    thrift_node_id,
                    probe_texpr,
                    fragment_id,
                },
            );
        }
    }

    // -------------------------------------------------------------------
    // Conjunct splitting helper
    // -------------------------------------------------------------------

    fn split_and_compile_conjuncts(
        &self,
        predicate: &TypedExpr,
        scope: &ExprScope,
    ) -> Result<Vec<exprs::TExpr>, String> {
        let conjuncts = split_and_conjuncts_typed(predicate);
        let mut results = Vec::new();
        for conj in conjuncts {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), scope);
            results.push(compiler.compile_typed(conj)?);
        }
        Ok(results)
    }

    // -------------------------------------------------------------------
    // visit_scan
    // -------------------------------------------------------------------

    fn visit_scan(
        &mut self,
        op: &PhysicalScanOp,
        _node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let scan_tuple_id = self.alloc_tuple();
        let scan_node_id = self.alloc_node();

        let mut scope = ExprScope::new();
        let qualifier = op.alias.as_deref().or(Some(&op.table.name));
        let mut slot_to_column = HashMap::new();
        let mut iceberg_metadata_pseudo_column_slots = BTreeSet::new();

        // Determine which columns to emit
        let mut required: Option<std::collections::HashSet<String>> = op
            .required_columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.to_lowercase()).collect());
        if let Some(required) = required.as_mut() {
            add_iceberg_equality_delete_required_columns(required, &op.table)?;
        }

        let planned_scan = match &op.table.source {
            crate::sql::catalog::ScanSource::StarRocks { db_id, table_id } => {
                let planner = self.connectors.scan_planner("starrocks")?;
                let table_handle =
                    crate::connector::starrocks::table::StarRocksTableScanPlanner::table_handle_from_source(
                        &op.database,
                        &op.table.name,
                        *db_id,
                        *table_id,
                    );
                let scan = planner.begin_scan(
                    table_handle,
                    crate::connector::scan_planning::BeginScanContext::default(),
                )?;
                let splits = planner.plan_splits(
                    &scan,
                    crate::connector::scan_planning::SplitPlanningContext::default(),
                )?;
                Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
            }
            crate::sql::catalog::ScanSource::IcebergDataFiles {
                table: iceberg_table,
                files,
                ..
            } => {
                let planner = self.connectors.scan_planner("iceberg")?;
                let column_names = op
                    .table
                    .columns
                    .iter()
                    .map(|c| c.name.clone())
                    .collect::<Vec<_>>();
                let table_handle =
                    crate::connector::iceberg::IcebergConnectorScanPlanner::table_handle_from_source(
                        &iceberg_table.catalog,
                        &iceberg_table.namespace,
                        &iceberg_table.table,
                        iceberg_table.current_snapshot_id,
                        iceberg_table.clone(),
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
                Some(crate::sql::codegen::resolve::PlannedConnectorScan { scan, splits })
            }
            _ => None,
        };
        let scan_table_id = match &op.table.source {
            crate::sql::catalog::ScanSource::StarRocks { table_id, .. } => Some(*table_id),
            _ => iceberg_table_info(&op.table.source)
                .is_some()
                .then_some(synthetic_iceberg_table_id(scan_node_id)),
        };
        if let Some(table_id) = scan_table_id {
            self.desc_builder
                .add_table_for_scan(table_id, &op.database, &op.table);
        }

        // Build a quick lookup so the column registration loop below can
        // recognise base-table columns that the dict rewriter retargeted
        // to a hidden `__nr_dict_<t>_<c>` Int32 slot. For those columns
        // we allocate ONE slot (at the source column's storage position)
        // named after the dict column and typed Int32 — keeping the
        // single-slot-per-column contract the StarRocks lake scan
        // expects (see `src/lower/node/lake_scan.rs`'s
        // `dict_int_to_string` self-map handling).
        let dict_source_to_target: std::collections::HashMap<String, &ScanDictionaryColumn> = op
            .dict_columns
            .iter()
            .map(|dc| (dc.source_column.to_ascii_lowercase(), dc))
            .collect();
        // Track dict slot ids by source column so the second loop over
        // `op.dict_columns` doesn't re-allocate a slot for the same
        // column. Also accumulates the `(dict_slot_id, dict_col)` pairs
        // that feed the TGlobalDict / dict_string_id_to_int_ids payload
        // construction further down.
        let mut dict_slot_for_source: std::collections::HashMap<String, i32> =
            std::collections::HashMap::new();
        let mut dict_slot_to_dict: Vec<(i32, &ScanDictionaryColumn)> = Vec::new();
        for (idx, col) in op.table.columns.iter().enumerate() {
            // The dict rewriter renames the source string column to the
            // dict column name in `op.columns` / `op.required_columns`,
            // so check membership using BOTH names when a dict mapping
            // exists for this base column.
            let dict_target = dict_source_to_target.get(&col.name.to_lowercase());
            if let Some(ref req) = required {
                let keep = req.contains(&col.name.to_lowercase())
                    || dict_target
                        .map(|dc| req.contains(&dc.dict_column.to_lowercase()))
                        .unwrap_or(false);
                if !keep {
                    continue;
                }
            }
            let slot_id = self.alloc_slot();
            // Bug B contract: slot keeps the SOURCE column's storage
            // name (so the lake scan finds the column by name in the
            // tablet schema) and Int32 type (the BE reads it as Utf8 via
            // `build_scan_schema_for_global_dict_encoding` when a
            // TGlobalDict is registered for the slot, then encodes
            // string -> dict id). The dict_column NAME is exposed only
            // in the FE codegen scope below, NOT in the slot descriptor
            // — the BE never sees `__nr_dict_t_s` as a column name.
            let slot_type = match dict_target {
                Some(_) => DataType::Int32,
                None => col.data_type.clone(),
            };
            let nullable = col.nullable;
            self.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &slot_type,
                nullable,
                idx as i32,
            );
            slot_to_column.insert(slot_id, col.name.clone());
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: slot_type.clone(),
                type_desc: None,
                nullable,
            };
            // G1: pick up the per-column ColumnId from `op.columns` so the
            // scope's by-id index is populated for base-table reads. This is
            // what lets the optimizer's `DistributionSpec::HashPartitioned`
            // (which is now a `Vec<ColumnId>`) resolve directly against the
            // scan's child scope without having to round-trip through the
            // display name. The dict-renamed `OutputColumn` carries the
            // source column's id so the lookup still hits.
            let col_id = op
                .columns
                .iter()
                .find(|oc| {
                    let lc = oc.name.to_ascii_lowercase();
                    lc == col.name.to_ascii_lowercase()
                        || dict_target
                            .map(|dc| lc == dc.dict_column.to_ascii_lowercase())
                            .unwrap_or(false)
                })
                .map(|oc| oc.column_id)
                .unwrap_or(crate::sql::column_id::ColumnId::UNSET);
            scope.add_column_with_id(
                col_id,
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding.clone(),
            );
            // Also register the dict column name in the scope so the
            // post-rewrite `ColumnRef("__nr_dict_t_s")` resolves to this
            // same slot. The scan tuple holds a SINGLE slot for this
            // column; both names refer to it.
            if let Some(dict_col) = dict_target {
                scope.add_column(
                    qualifier.map(|s| s.to_string()),
                    dict_col.dict_column.clone(),
                    binding.clone(),
                );
            }
            // When alias differs from table name, also register with original table name
            if op
                .alias
                .as_deref()
                .is_some_and(|a| !a.eq_ignore_ascii_case(&op.table.name))
            {
                scope.add_column(
                    Some(op.table.name.clone()),
                    col.name.clone(),
                    binding.clone(),
                );
                if let Some(dict_col) = dict_target {
                    scope.add_column(
                        Some(op.table.name.clone()),
                        dict_col.dict_column.clone(),
                        binding.clone(),
                    );
                }
            }
            if let Some(dict_col) = dict_target {
                dict_slot_for_source.insert(col.name.to_ascii_lowercase(), slot_id);
                dict_slot_to_dict.push((slot_id, *dict_col));
            }
        }

        // Iceberg V3 row-lineage pseudo-columns (_row_id,
        // _last_updated_sequence_number): register in ExprScope and emit as
        // output slots so that SELECT _row_id references resolve in codegen
        // and the slot flows through to the HDFS_SCAN_NODE tuple descriptor.
        // Lowering picks up the slot by name via `is_iceberg_row_id` /
        // `is_iceberg_last_updated_sequence_number` to populate
        // IcebergVirtualSpec.
        //
        // Note: these pseudo-columns are NOT in `scan.columns`, so the column
        // pruning rule never adds them to `required_columns`. Always register
        // them regardless of `required`; the lowering layer only synthesises
        // the values for slots that are actually in the tuple descriptor.
        let meta_col_offset = op.table.columns.len();
        for (meta_idx, col) in op
            .table
            .iceberg_row_lineage_metadata_columns
            .iter()
            .enumerate()
        {
            let col_pos = (meta_col_offset + meta_idx) as i32;
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                col_pos,
            );
            slot_to_column.insert(slot_id, col.name.clone());
            iceberg_metadata_pseudo_column_slots.insert(slot_id);
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: None,
                nullable: col.nullable,
            };
            scope.add_column(
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding.clone(),
            );
            if op
                .alias
                .as_deref()
                .is_some_and(|a| !a.eq_ignore_ascii_case(&op.table.name))
            {
                scope.add_column(Some(op.table.name.clone()), col.name.clone(), binding);
            }
        }

        // Compile predicates pushed down by the optimizer
        let pushed_conjuncts = if op.predicates.is_empty() {
            vec![]
        } else {
            let mut conjuncts = Vec::new();
            for pred in &op.predicates {
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &scope);
                conjuncts.push(compiler.compile_typed(pred)?);
            }
            conjuncts
        };

        // Dict-encoded scan columns (Task 5/7/8 plan hints). The slot for
        // each dict column was already allocated in the table-column loop
        // above (where its storage `col_pos` is recorded). Here we just
        // build the BE-facing payload: a self-map `dict_slot → dict_slot`
        // for `TLakeScanNode.dict_string_id_to_int_ids` (the BE replaces
        // the dict slot in the scan layout with this id, so the storage
        // reader keeps the same slot) and a TGlobalDict for each. The
        // dict_columns hint is still consulted later to detect a planning
        // bug on non-StarRocks scans. `dict_columns` is empty in all
        // production paths today.
        let mut string_to_dict_slot: std::collections::BTreeMap<i32, i32> =
            std::collections::BTreeMap::new();
        for dict_col in &op.dict_columns {
            let dict_slot_id = dict_slot_for_source
                .get(&dict_col.source_column.to_ascii_lowercase())
                .copied()
                .ok_or_else(|| {
                    format!(
                        "scan `{}.{}` dict_columns references unknown source column `{}`",
                        op.database, op.table.name, dict_col.source_column
                    )
                })?;
            // Self-map: the BE's `lake_scan.rs` rewrites every dict int
            // slot in the layout to its mapped string slot before issuing
            // the storage read. With the FE-only fix the FE no longer
            // emits a separate string slot — the dict slot itself is the
            // storage slot, declared Int32 in desc_tbl but read as Utf8
            // via the query global dict path (see
            // `build_scan_schema_for_global_dict_encoding`). A self-map
            // keeps the BE's layout swap a no-op while preserving the
            // "dict-encoded" semantics on the FE/wire contract.
            string_to_dict_slot.insert(dict_slot_id, dict_slot_id);
        }

        let resolved = ResolvedTable {
            database: op.database.clone(),
            table: op.table.clone(),
            planned_scan,
            alias: op.alias.clone(),
        };
        self.desc_builder.add_tuple(scan_tuple_id, scan_table_id);

        let min_max_predicates =
            nodes::scan_file_min_max_predicates_from_state(&pushed_conjuncts, &slot_to_column);
        let change_op_slot = nodes::planned_change_op_slot_from_state(
            &iceberg_metadata_pseudo_column_slots,
            &slot_to_column,
        );
        let mut scan_plan_node = nodes::build_scan_node(
            self.connectors,
            scan_node_id,
            scan_tuple_id,
            &resolved,
            pushed_conjuncts.clone(),
            min_max_predicates,
            change_op_slot,
        )?;

        // StarRocks lake scans carry the dict slot self-map on the wire via
        // `TLakeScanNode.dict_string_id_to_int_ids`. Iceberg/HDFS scans have no
        // such thrift field and don't need one: the dict slot is already an
        // Int32 storage slot, and the per-fragment `TGlobalDict` payloads
        // emitted below feed `lower_hdfs_scan_node`'s encode map directly
        // (the parquet reader reads Utf8 and encodes to dict ids). So for an
        // iceberg `hdfs_scan_node` we leave the thrift node untouched. Any
        // other scan kind receiving dict_columns is a planning bug.
        if !string_to_dict_slot.is_empty() {
            if let Some(lake) = scan_plan_node.lake_scan_node.as_mut() {
                lake.dict_string_id_to_int_ids = Some(string_to_dict_slot);
            } else if scan_plan_node.hdfs_scan_node.is_some() {
                // iceberg/HDFS: dicts flow via query_global_dicts in lowering.
            } else {
                return Err(format!(
                    "scan `{}.{}` has dict_columns but is neither a StarRocks lake scan nor an iceberg/HDFS scan",
                    op.database, op.table.name,
                ));
            }
        }

        // Emit per-dict-column TGlobalDict payloads onto EVERY fragment in
        // the current stack (the leaf scan's fragment plus every parent
        // fragment that consumes its output through an exchange). The
        // dict slot id is consistent across fragments, so a Decode
        // operator inserted above the exchange — which lives in a parent
        // fragment — must also receive the TGlobalDict via its own
        // fragment's `query_global_dicts`. Without this, the BE's
        // `lower_decode_node` fails with `missing query global dict for
        // encoded slot_id=<N>` (each fragment builds its own
        // QueryGlobalDictMap from its own TGlobalDict list).
        let current_frag = self.current_fragment_id()?;
        let dict_fragments: Vec<FragmentId> = if self.fragment_stack.is_empty() {
            vec![current_frag]
        } else {
            self.fragment_stack.clone()
        };
        for (dict_slot_id, dict_col) in &dict_slot_to_dict {
            let snapshot = dict_col.dictionary.as_ref();
            let mut ids = Vec::with_capacity(snapshot.values.len());
            let mut strings = Vec::with_capacity(snapshot.values.len());
            for value in &snapshot.values {
                ids.push(value.id);
                strings.push(value.bytes.clone());
            }
            let global_dict = crate::data::TGlobalDict::new(
                Some(*dict_slot_id),
                Some(strings),
                Some(ids),
                Some(snapshot.version),
            );
            for fragment_id in &dict_fragments {
                self.query_global_dicts_per_fragment
                    .entry(*fragment_id)
                    .or_default()
                    .push(global_dict.clone());
            }
            // Track the slot -> dict association so any operator that
            // allocates a new slot inheriting this slot's values
            // (Aggregate group-by, Project column ref, etc.) can
            // re-register the dict on the new slot id. The downstream
            // `Decode` resolves by NAME against the new slot, then the
            // BE's `lower_decode_node` needs a TGlobalDict keyed by that
            // new slot id — registered via `propagate_dict_to_slot`.
            self.slot_to_global_dict.insert(*dict_slot_id, global_dict);
        }

        self.scan_tables.push(nodes::PlannedScanTable {
            scan_node_id,
            scan_tuple_id,
            resolved,
            min_max_conjuncts: pushed_conjuncts,
            slot_to_column,
            iceberg_metadata_pseudo_column_slots,
        });

        Ok(VisitResult {
            plan_nodes: vec![scan_plan_node],
            scope,
            tuple_ids: vec![scan_tuple_id],
            cte_exchange_nodes: Vec::new(),
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_filter
    // -------------------------------------------------------------------

    fn visit_filter(
        &mut self,
        op: &PhysicalFilterOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let mut child = self.visit(&node.children[0])?;

        let conjuncts = self.split_and_compile_conjuncts(&op.predicate, &child.scope)?;

        if !conjuncts.is_empty() {
            // Push conjuncts onto the first (scan) node if it has none yet
            if let Some(scan) = child.plan_nodes.first_mut() {
                let scan_node_id = scan.node_id;
                let extra_conjuncts = conjuncts.clone();
                if scan.conjuncts.is_none() {
                    scan.conjuncts = Some(conjuncts);
                } else {
                    scan.conjuncts.as_mut().unwrap().extend(conjuncts);
                }
                nodes::append_hdfs_scan_min_max_conjuncts(scan, &extra_conjuncts);
                if let Some(planned) = self
                    .scan_tables
                    .iter_mut()
                    .find(|planned| planned.scan_node_id == scan_node_id)
                {
                    planned.min_max_conjuncts.extend(extra_conjuncts);
                }
            }
        }

        Ok(child)
    }

    // -------------------------------------------------------------------
    // visit_decode
    // -------------------------------------------------------------------

    /// Emit a `TDecodeNode` for a `PhysicalDecodeOp`.
    ///
    /// The BE-side decode (see `src/lower/node/decode.rs`) treats the decode
    /// as a Project that allocates a new output tuple. For each `(dict_id,
    /// string_id)` pair in `dict_id_to_string_ids`:
    /// * `dict_id` is an EXISTING child slot (the encoded Int32 dict slot).
    /// * `string_id` is a NEW slot in the decode's own tuple — the slot
    ///   that materializes the decoded string value.
    /// All other child slots pass through the decode unchanged: the new
    /// tuple "borrows" their slot ids verbatim (matches the StarRocks
    /// `DecodeNode` codegen, where a new `TupleDescriptor` reuses the
    /// child's slot ids for passthrough columns and adds a fresh slot
    /// id per decoded column).
    ///
    /// The string slot id is freshly allocated here — it is NOT expected
    /// to be in the child scope, because the rewriter inserts the Decode
    /// above operators (e.g. `Aggregate(group_by = dict)`) that strip
    /// the source string slot from their output.
    fn visit_decode(
        &mut self,
        op: &PhysicalDecodeOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "PhysicalDecode expected exactly 1 child, got {}",
                node.children.len()
            ));
        }
        let child = self.visit(&node.children[0])?;

        // Build `dict_slot_id -> (string_column_name, data_type, nullable)`
        // for every declared mapping. The dict column name MUST be in the
        // child scope — the rewriter is the only producer of decode
        // mappings, and it only creates one when its input subtree
        // publishes the dict column. The `op.output_columns` declared
        // name / type is consulted only as a hint for the post-decode
        // string column's data type; we default to Utf8 when the
        // rewriter handed us no declared output column.
        let dict_target_meta: BTreeMap<i32, (String, DataType, bool)> = op
            .mappings
            .iter()
            .map(|item| {
                let dict_binding = child
                    .scope
                    .resolve_column(None, &item.dict_column)
                    .map_err(|_| {
                        format!(
                            "decode dict column `{}` is not in child scope",
                            item.dict_column
                        )
                    })?;
                let declared = op
                    .output_columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&item.string_column));
                let data_type = declared
                    .map(|c| c.data_type.clone())
                    .unwrap_or(DataType::Utf8);
                let nullable = declared.map(|c| c.nullable).unwrap_or(true);
                Ok::<_, String>((
                    dict_binding.slot_id,
                    (item.string_column.clone(), data_type, nullable),
                ))
            })
            .collect::<Result<_, _>>()?;

        // Allocate the decode's new output tuple. New string slots live
        // here; passthrough slots are re-registered here under the same
        // slot id they used in the child tuple.
        let decode_tuple_id = self.alloc_tuple();
        let mut decode_scope = ExprScope::new();
        let mut mapping: BTreeMap<i32, i32> = BTreeMap::new();
        let mut consumed_dict_slots: std::collections::BTreeSet<i32> = Default::default();

        // Iterate the child's ordered columns. For each child column whose
        // slot id matches a declared dict source slot (`mappings.dict_column`),
        // allocate a fresh string slot in the decode tuple and surface it
        // under the mapping's `string_column` name. All other child columns
        // pass through verbatim: same slot id, same name.
        //
        // Why iterate child scope rather than `op.output_columns`: the
        // optimizer's `output_columns` carries analyzer-level names (often
        // SELECT aliases like `sum(c3) AS sc3`), but the aggregate codegen
        // registers slots under unaliased display names (`sum(c3)`). The
        // outer Project compiles `AggregateCall` references against the
        // display name, so Decode must republish that display name verbatim
        // for resolution to succeed.

        let child_columns: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();

        let mut col_pos: i32 = 0;
        for (child_name, child_binding) in &child_columns {
            if let Some((string_name, data_type, nullable)) =
                dict_target_meta.get(&child_binding.slot_id)
            {
                // Decoded string output: allocate a NEW slot in the
                // decode's tuple. The mapping pairs the child's dict
                // slot with this new string slot.
                let string_slot_id = self.alloc_slot();
                self.desc_builder.add_slot(
                    string_slot_id,
                    decode_tuple_id,
                    string_name,
                    data_type,
                    *nullable,
                    col_pos,
                );
                if consumed_dict_slots.insert(child_binding.slot_id) {
                    mapping.insert(child_binding.slot_id, string_slot_id);
                }
                decode_scope.add_column(
                    None,
                    string_name.clone(),
                    ColumnBinding {
                        tuple_id: decode_tuple_id,
                        slot_id: string_slot_id,
                        data_type: data_type.clone(),
                        type_desc: None,
                        nullable: *nullable,
                    },
                );
            } else {
                // Passthrough: re-register the child's slot id under
                // the decode's new tuple, matching the StarRocks BE
                // contract (passthrough slots keep their slot id and
                // gain a new tuple parent).
                self.desc_builder.add_slot(
                    child_binding.slot_id,
                    decode_tuple_id,
                    child_name,
                    &child_binding.data_type,
                    child_binding.nullable,
                    col_pos,
                );
                decode_scope.add_column(
                    None,
                    child_name.clone(),
                    ColumnBinding {
                        tuple_id: decode_tuple_id,
                        slot_id: child_binding.slot_id,
                        data_type: child_binding.data_type.clone(),
                        type_desc: child_binding.type_desc.clone(),
                        nullable: child_binding.nullable,
                    },
                );
            }
            col_pos += 1;
        }

        if mapping.len() != op.mappings.len() {
            return Err(format!(
                "decode mappings unresolved: declared {} entries, materialized {}",
                op.mappings.len(),
                mapping.len()
            ));
        }

        self.desc_builder.add_tuple(decode_tuple_id, None);
        let decode_node =
            nodes::build_decode_node(self.alloc_node(), vec![decode_tuple_id], mapping);

        // Pre-order: decode first, then child nodes.
        let mut plan_nodes = vec![decode_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: decode_scope,
            tuple_ids: vec![decode_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_project
    // -------------------------------------------------------------------

    fn visit_project(
        &mut self,
        op: &PhysicalProjectOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let project_tuple_id = self.alloc_tuple();
        let project_node_id = self.alloc_node();

        let mut output_columns = Vec::new();
        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            let data_type = item.expr.data_type.clone();
            let nullable = item.expr.nullable;
            let name = item.output_name.clone();
            let slot_id = self.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("project expr `{name}` compiled to empty TExpr"))?;
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                project_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                output_columns.len() as i32,
            );
            slot_map.insert(slot_id, texpr);
            output_columns.push(OutputColumn {
                name: name.clone(),
                data_type: data_type.clone(),
                nullable,
            });

            project_scope.add_column_with_id(
                item.output_column_id,
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: project_tuple_id,
                    slot_id,
                    data_type: data_type.clone(),
                    type_desc: Some(slot_type_desc.clone()),
                    nullable,
                },
            );

            // Also register with qualifier if the expression is a column ref.
            // Use add_qualified_alias to avoid pushing a duplicate entry into
            // the ordered list (which would inflate iter_columns and break
            // UNION output slot counts).
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
                ..
            } = item.expr.kind
            {
                project_scope.add_qualified_alias(
                    q.clone(),
                    column.clone(),
                    ColumnBinding {
                        tuple_id: project_tuple_id,
                        slot_id,
                        data_type,
                        type_desc: Some(slot_type_desc),
                        nullable,
                    },
                );
            }

            // Propagate the dict registration on a ColumnRef passthrough:
            // the new slot inherits the source slot's dict, so a parent
            // fragment's Decode (post-exchange) finds the matching dict
            // in its own `query_global_dicts`.
            if let ExprKind::ColumnRef { ref column, .. } = item.expr.kind
                && let Ok(child_binding) = child.scope.resolve_column(None, column)
            {
                let source_slot_id = child_binding.slot_id;
                self.propagate_dict_to_slot(source_slot_id, slot_id);
            }
        }

        self.desc_builder.add_tuple(project_tuple_id, None);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        // Pre-order: project first, then child nodes
        let mut plan_nodes = vec![project_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: project_scope,
            tuple_ids: vec![project_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_hash_join
    // -------------------------------------------------------------------

    fn visit_hash_join(
        &mut self,
        op: &PhysicalHashJoinOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let left = self.visit(&node.children[0])?;
        let right = self.visit(&node.children[1])?;

        let join_op = join_kind_to_op(op.join_type);
        let join_node_id = self.alloc_node();

        // Compile eq conditions.  Pairs are pre-oriented by JoinToHashJoin so
        // that pair.0 references the left child and pair.1 references the right
        // in the common case.  However, orientation can fail when the same
        // column name appears in both children (e.g. self-join on a CTE) or
        // when logical_props is missing for a child group.  We therefore try
        // the natural order first, then the swapped order as a fallback, and
        // demote only when neither compiles successfully.
        let mut eq_join_conjuncts = Vec::new();
        let mut demoted_eq_exprs: Vec<crate::sql::analysis::TypedExpr> = Vec::new();
        // Parallel to `eq_join_conjuncts`: `surviving_eq_origin[j]` is the
        // original `op.eq_conditions` index of the j-th surviving conjunct.
        // Demoted conditions get no entry, so this lets runtime-filter lowering
        // remap the physical pass's pre-demote `expr_order` onto the post-demote
        // conjunct index that BE lowering uses. See `remap_rf_expr_order`.
        let mut surviving_eq_origin: Vec<usize> = Vec::new();
        for (eq_index, eq) in op.eq_conditions.iter().enumerate() {
            let expr_a = &eq.left;
            let expr_b = &eq.right;
            // Try natural order: expr_a on left, expr_b on right.
            let natural = ExprCompiler::new(self.slot_allocator(), &left.scope)
                .compile_typed(expr_a)
                .ok()
                .and_then(|lt| {
                    ExprCompiler::new(self.slot_allocator(), &right.scope)
                        .compile_typed(expr_b)
                        .ok()
                        .map(|rt| (lt, rt))
                });
            // Try swapped order: expr_b on left, expr_a on right.
            // Needed when JoinCommutativity swapped children but the
            // eq_condition columns still reference the original order.
            let result = natural.or_else(|| {
                ExprCompiler::new(self.slot_allocator(), &left.scope)
                    .compile_typed(expr_b)
                    .ok()
                    .and_then(|lt| {
                        ExprCompiler::new(self.slot_allocator(), &right.scope)
                            .compile_typed(expr_a)
                            .ok()
                            .map(|rt| (lt, rt))
                    })
            });
            if let Some((lt, rt)) = result {
                eq_join_conjuncts.push(plan_nodes::TEqJoinCondition {
                    left: lt,
                    right: rt,
                    opcode: Some(if eq.null_safe {
                        crate::opcodes::TExprOpcode::EQ_FOR_NULL
                    } else {
                        crate::opcodes::TExprOpcode::EQ
                    }),
                });
                surviving_eq_origin.push(eq_index);
            } else {
                // Both sides belong to the same child — demote to other_condition
                // compiled with a merged scope.
                demoted_eq_exprs.push(crate::sql::analysis::TypedExpr {
                    kind: crate::sql::analysis::ExprKind::BinaryOp {
                        left: Box::new(expr_a.clone()),
                        op: if eq.null_safe {
                            crate::sql::analysis::BinOp::EqForNull
                        } else {
                            crate::sql::analysis::BinOp::Eq
                        },
                        right: Box::new(expr_b.clone()),
                    },
                    data_type: arrow::datatypes::DataType::Boolean,
                    nullable: false,
                });
            }
        }

        // Compile other conditions (including any eq pairs demoted above).
        let mut other_join_conjuncts = Vec::new();
        {
            let mut merged = ExprScope::new();
            merged.merge(&left.scope);
            merged.merge(&right.scope);
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &merged);
            for demoted in &demoted_eq_exprs {
                other_join_conjuncts.push(compiler.compile_typed(demoted)?);
            }
            if let Some(ref cond) = op.other_condition {
                other_join_conjuncts.push(compiler.compile_typed(cond)?);
            }
        }

        let mut join_plan_node = nodes::build_hash_join_node(
            join_node_id,
            &left.tuple_ids,
            &right.tuple_ids,
            join_op,
            eq_join_conjuncts,
            other_join_conjuncts,
        );

        // OQ-5: lower the runtime-filter annotations the physical-tree pass
        // attached to this join into thrift `TRuntimeFilterDescription`s and
        // patch them onto the join node. Compiled here while `right.scope`
        // (the build side) is still available — it is moved into the merged
        // output scope further below. `surviving_eq_origin` remaps each RF's
        // pre-demote `expr_order` onto the post-demote `eq_join_conjuncts`
        // index that BE lowering indexes.
        let rf_descs =
            self.build_rf_descriptors(node, join_node_id, &right.scope, &surviving_eq_origin)?;
        if !rf_descs.is_empty()
            && let Some(hj) = join_plan_node.hash_join_node.as_mut()
        {
            hj.build_runtime_filters = Some(rf_descs);
        }

        // Widen nullable flags on the join's null-producing side(s). Note: this
        // is the tuple-level widening needed by the descriptor table and the
        // runtime's null-padding for SEMI/ANTI pruned columns. The authoritative
        // source of column-level nullability is `node.output_columns`, populated
        // by stats::derive_output_columns via widen_for_join_kind. This match
        // intentionally mirrors that widening at the tuple level — a per-slot
        // nullability mechanism would let us drive both from output_columns,
        // but is out of scope here.
        match op.join_type {
            JoinKind::LeftOuter | JoinKind::LeftAnti | JoinKind::LeftSemi => {
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::RightOuter | JoinKind::RightAnti | JoinKind::RightSemi => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::FullOuter => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            _ => {}
        }

        // tuple_ids always includes both sides — the join node's row_tuples
        // must reference all probe and build tuples.
        let mut merged_tuple_ids = left.tuple_ids.clone();
        merged_tuple_ids.extend(&right.tuple_ids);

        // Output scope: SEMI/ANTI joins only expose the surviving side's
        // columns to downstream operators (preventing stale column
        // references when multiple SEMI joins are chained).
        let merged_scope = match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti => left.scope,
            JoinKind::RightSemi | JoinKind::RightAnti => right.scope,
            _ => {
                let mut scope = left.scope;
                scope.merge(&right.scope);
                scope
            }
        };

        // Pre-order: join node, then left subtree, then right subtree
        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left.plan_nodes);
        plan_nodes.extend(right.plan_nodes);
        let mut cte_exchange_nodes = left.cte_exchange_nodes;
        cte_exchange_nodes.extend(right.cte_exchange_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: merged_scope,
            tuple_ids: merged_tuple_ids,
            cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // Runtime filter lowering (OQ-5)
    // -------------------------------------------------------------------

    /// Lower the build-side runtime-filter annotations on `node` into thrift
    /// `TRuntimeFilterDescription`s and accumulate the coordinator-facing RF
    /// maps. Each build key is compiled fresh against `build_scope` (the join's
    /// right child scope) to avoid index drift with the eq-conjunct demote
    /// dance. The probe target (node id + compiled probe expr) is looked up
    /// from `rf_probe_targets`, populated while visiting the probe descendants
    /// before this join. A descriptor with no recorded probe target is a
    /// build-only RF (empty `plan_node_id_to_target_expr`).
    ///
    /// `surviving_eq_origin` is the parallel vec from `visit_hash_join` that
    /// maps each surviving `eq_join_conjuncts` entry back to its source
    /// `op.eq_conditions` index. The physical pass records `rf.expr_order` in
    /// the PRE-demote `op.eq_conditions` space, but BE lowering indexes the
    /// POST-demote `eq_join_conjuncts` (and the build/probe key + null-safe
    /// vectors derived from it). We therefore remap every descriptor's
    /// `expr_order` through `surviving_eq_origin` and DROP any RF whose source
    /// conjunct was demoted to `other_join_conjuncts` (no longer an equi-key).
    fn build_rf_descriptors(
        &mut self,
        node: &PhysicalPlanNode,
        join_node_id: i32,
        build_scope: &ExprScope,
        surviving_eq_origin: &[usize],
    ) -> Result<Vec<crate::runtime_filter::TRuntimeFilterDescription>, String> {
        use crate::runtime_filter;

        if node.build_runtime_filters.is_empty() {
            return Ok(Vec::new());
        }

        let pipeline_dop = rf_pipeline_dop();
        let join_fragment = self.current_fragment_id()?;
        let mut descs: Vec<runtime_filter::TRuntimeFilterDescription> =
            Vec::with_capacity(node.build_runtime_filters.len());

        for rf in &node.build_runtime_filters {
            let filter_id = rf.filter_id;

            // Remap the physical pass's pre-demote `expr_order` onto the
            // post-demote `eq_join_conjuncts` index that BE lowering indexes.
            // If the source conjunct was demoted to `other_join_conjuncts`,
            // it is no longer an equi-join key at execution — drop the RF
            // entirely rather than emit a descriptor BE cannot align.
            let Some(post_demote_expr_order) =
                remap_rf_expr_order(surviving_eq_origin, rf.expr_order)
            else {
                continue;
            };
            // Defensive: never emit a descriptor whose `expr_order` is out of
            // range for the join's `eq_join_conjuncts` (BE would Err on it).
            // `remap_rf_expr_order` returns a position within
            // `surviving_eq_origin`, whose length equals `eq_join_conjuncts`,
            // so this can only trip on a future invariant break.
            if post_demote_expr_order >= surviving_eq_origin.len() {
                continue;
            }

            // The build key MUST be the equi-join side that binds the build
            // (right) child's scope. The physical pass labels build/probe by
            // `eq.right`/`eq.left`, but join commutativity can swap children
            // relative to that labeling (the same try-natural-then-swap
            // ambiguity `visit_hash_join` resolves for `eq_join_conjuncts`).
            // We therefore compile `build_expr` against the build scope and,
            // on failure, fall back to `probe_expr` (the swapped orientation).
            // Resolution is ColumnId-aware (see `ExprCompiler::compile_typed`),
            // so this matches the same physical column the join key uses.
            let build_texpr = match ExprCompiler::new(self.slot_allocator(), build_scope)
                .compile_typed(&rf.build_expr)
            {
                Ok(t) => t,
                Err(_) => ExprCompiler::new(self.slot_allocator(), build_scope)
                    .compile_typed(&rf.probe_expr)
                    .map_err(|e| {
                        format!(
                            "runtime filter {filter_id}: neither build nor probe key \
                             binds the build child scope: {e}"
                        )
                    })?,
            };

            // Probe target recorded while visiting the probe descendants. When
            // children were swapped relative to the eq labeling, the annotation
            // pass pushed the probe expr down the wrong child (it could not bind
            // by ColumnId), so no target was recorded — the descriptor is then
            // build-only with an empty target map (matches StarRocks "no probe
            // target").
            let probe_target = self.rf_probe_targets.get(&filter_id).cloned();
            let has_remote_targets = probe_target
                .as_ref()
                .map(|t| t.fragment_id != join_fragment)
                .unwrap_or(false);

            let (build_join_mode, local_layout, global_layout) =
                rf_layout_for_distribution(&rf.distribution);

            let layout = runtime_filter::TRuntimeFilterLayout::new(
                filter_id,
                local_layout,
                global_layout,
                false,            // pipeline_level_multi_partitioned
                1_i32,            // num_instances
                pipeline_dop,     // num_drivers_per_instance
                None::<Vec<i32>>, // bucketseq_to_instance
                None::<Vec<i32>>, // bucketseq_to_driverseq
                None::<Vec<i32>>, // bucketseq_to_partition
                None::<Vec<crate::partitions::TBucketProperty>>, // bucket_properties
            );

            // Probe targets: one entry per placed probe; empty for build-only.
            let mut target_map = BTreeMap::new();
            if let Some(target) = &probe_target {
                target_map.insert(target.thrift_node_id, target.probe_texpr.clone());
            }

            let desc = runtime_filter::TRuntimeFilterDescription::new(
                filter_id,                                              // filter_id
                build_texpr,                                            // build_expr
                post_demote_expr_order as i32,                          // expr_order
                target_map,                                 // plan_node_id_to_target_expr
                has_remote_targets,                         // has_remote_targets
                None::<i64>,                                // bloom_filter_size
                None::<Vec<crate::types::TNetworkAddress>>, // runtime_filter_merge_nodes
                build_join_mode,                            // build_join_mode
                None::<crate::types::TUniqueId>,            // sender_finst_id
                join_node_id,                               // build_plan_node_id
                None::<Vec<crate::types::TUniqueId>>,       // broadcast_grf_senders
                None::<Vec<runtime_filter::TRuntimeFilterDestination>>, // broadcast_grf_destinations
                None::<Vec<i32>>,                                       // bucketseq_to_instance
                None::<BTreeMap<i32, Vec<exprs::TExpr>>>, // plan_node_id_to_partition_by_exprs
                runtime_filter::TRuntimeFilterBuildType::JOIN_FILTER, // filter_type
                layout,                                   // layout
                None::<bool>,                             // build_from_group_execution
                None::<bool>,                             // is_broad_cast_join_in_skew
                None::<i32>,                              // skew_shuffle_filter_id
                None::<bool>,                             // is_asc
                None::<bool>,                             // is_nulls_first
                None::<i64>,                              // limit
            );

            descs.push(desc.clone());

            // Accumulate coordinator-facing RF maps.
            self.rf_all_filters.insert(filter_id, desc);
            self.rf_build_side_filters
                .entry(join_fragment)
                .or_default()
                .push(filter_id);
            if let Some(target) = &probe_target {
                self.rf_probe_side_filters
                    .entry(target.fragment_id)
                    .or_default()
                    .push((filter_id, target.thrift_node_id));
            }
        }

        Ok(descs)
    }

    // -------------------------------------------------------------------
    // visit_nest_loop_join
    // -------------------------------------------------------------------

    fn visit_nest_loop_join(
        &mut self,
        op: &PhysicalNestLoopJoinOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let left = self.visit(&node.children[0])?;
        let right = self.visit(&node.children[1])?;

        let join_op = join_kind_to_op(op.join_type);
        let join_node_id = self.alloc_node();

        let join_conjuncts = if let Some(ref cond) = op.condition {
            let mut merged = ExprScope::new();
            merged.merge(&left.scope);
            merged.merge(&right.scope);
            let conjuncts = split_and_conjuncts_typed(cond);
            let mut results = Vec::new();
            for conj in conjuncts {
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &merged);
                results.push(compiler.compile_typed(conj)?);
            }
            results
        } else {
            vec![]
        };

        let join_plan_node = nodes::build_nestloop_join_node(
            join_node_id,
            &left.tuple_ids,
            &right.tuple_ids,
            join_op,
            join_conjuncts,
        );

        // Widen nullable for outer/anti join nullable side tuples.
        match op.join_type {
            JoinKind::LeftOuter | JoinKind::LeftAnti => {
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::RightOuter | JoinKind::RightAnti => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            JoinKind::FullOuter => {
                for &tid in &left.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
                for &tid in &right.tuple_ids {
                    self.desc_builder.widen_tuple_nullable(tid);
                }
            }
            _ => {}
        }

        // tuple_ids always includes both sides for the join node.
        let mut merged_tuple_ids = left.tuple_ids.clone();
        merged_tuple_ids.extend(&right.tuple_ids);

        // Output scope: SEMI/ANTI only expose surviving side.
        let merged_scope = match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti => left.scope,
            JoinKind::RightSemi | JoinKind::RightAnti => right.scope,
            _ => {
                let mut scope = left.scope;
                scope.merge(&right.scope);
                scope
            }
        };

        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left.plan_nodes);
        plan_nodes.extend(right.plan_nodes);
        let mut cte_exchange_nodes = left.cte_exchange_nodes;
        cte_exchange_nodes.extend(right.cte_exchange_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: merged_scope,
            tuple_ids: merged_tuple_ids,
            cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_hash_aggregate
    // -------------------------------------------------------------------

    fn visit_hash_aggregate(
        &mut self,
        op: &PhysicalHashAggregateOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;
        let need_finalize = matches!(op.mode, AggMode::Single | AggMode::Global);

        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        // Compile GROUP BY expressions (same for all modes — the child scope
        // has the correct columns for both scan-level and Local-output contexts).
        for (idx, gb_expr) in op.group_by.iter().enumerate() {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
            let texpr = compiler.compile_typed(gb_expr)?;
            let data_type = gb_expr.data_type.clone();
            let nullable = gb_expr.nullable;
            let name = typed_expr_display_name(gb_expr);
            let slot_id = self.alloc_slot();
            let slot_type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("group by expr `{name}` compiled to empty TExpr"))?;
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                agg_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: agg_tuple_id,
                slot_id,
                data_type: data_type.clone(),
                type_desc: Some(slot_type_desc),
                nullable,
            };
            // G1: when the group-by expression is itself a ColumnRef with a
            // real ColumnId, register the agg's output slot under that id so
            // upstream operators (the SELECT projection on top of a GROUPING
            // SETS / CUBE Aggregate, the Global merge above a Local agg,
            // etc.) can resolve the column by id regardless of the slot's
            // display name. Non-ColumnRef group-by exprs (e.g. `a + b`)
            // remain name-indexed only.
            let gb_column_id = match &gb_expr.kind {
                ExprKind::ColumnRef { column_id, .. } => *column_id,
                _ => crate::sql::column_id::ColumnId::UNSET,
            };
            agg_scope.add_column_with_id(gb_column_id, None, name, binding.clone());
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
                ..
            } = gb_expr.kind
            {
                agg_scope.add_qualified_alias(q.clone(), column.clone(), binding);
            }
            // Propagate dict registration through the aggregate's group-
            // by output: when the group-by is a passthrough ColumnRef of
            // a dict-encoded source slot, the new agg output slot also
            // carries dict ids. Re-register the TGlobalDict on the new
            // slot so a downstream Decode (in this or a parent fragment
            // post-exchange) resolves its `dict_id_to_string_ids` key.
            if let ExprKind::ColumnRef { ref column, .. } = gb_expr.kind
                && let Ok(child_binding) = child.scope.resolve_column(None, column)
            {
                let source_slot_id = child_binding.slot_id;
                self.propagate_dict_to_slot(source_slot_id, slot_id);
            }
            grouping_exprs.push(texpr);
        }

        // Compile aggregate function expressions — mode-dependent.
        let agg_start_col = op.group_by.len();
        let mut aggregate_functions = Vec::new();

        debug_assert_eq!(
            op.is_merge.len(),
            op.aggregates.len(),
            "PhysicalHashAggregate (node_id={}): is_merge.len() = {}, aggregates.len() = {}",
            agg_node_id,
            op.is_merge.len(),
            op.aggregates.len(),
        );

        for (idx, agg_call) in op.aggregates.iter().enumerate() {
            let texpr = if op.is_merge[idx] {
                // Global (merge) phase: the child scope contains the Local's
                // output.  Each intermediate aggregate column sits at position
                // group_by.len() + idx in the child scope's ordered columns.
                let child_columns: Vec<_> = child.scope.iter_columns().collect();
                let child_col_idx = agg_start_col + idx;
                let (_, binding) = child_columns.get(child_col_idx).ok_or_else(|| {
                    format!(
                        "Global agg: child scope missing intermediate column at index {}",
                        child_col_idx
                    )
                })?;
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
                compiler.compile_merge_aggregate_call(
                    agg_call,
                    binding.slot_id,
                    binding.tuple_id,
                    &binding.data_type,
                )?
            } else {
                // Single or Local: compile against child scope normally.
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
                compiler.compile_aggregate_call_typed(agg_call).map_err(|err| {
                    let available = child
                        .scope
                        .iter_columns()
                        .map(|(name, _)| name.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "failed to compile aggregate `{}` in {:?} mode against child scope [{}]: {}",
                        agg_call_display_name(agg_call),
                        op.mode,
                        available,
                        err
                    )
                })?
            };

            let data_type = if need_finalize {
                agg_call.result_type.clone()
            } else {
                texpr
                    .nodes
                    .first()
                    .and_then(|root| root.fn_.as_ref())
                    .and_then(|func| func.aggregate_fn.as_ref())
                    .and_then(|agg_fn| arrow_type_from_desc(&agg_fn.intermediate_type))
                    .unwrap_or_else(|| agg_call.result_type.clone())
            };
            let nullable = true;
            let name = agg_call_display_name(agg_call);
            let slot_id = self.alloc_slot();
            let col_pos = (agg_start_col + idx) as i32;
            let slot_type_desc = if need_finalize {
                texpr
                    .nodes
                    .first()
                    .map(|root| root.type_.clone())
                    .ok_or_else(|| format!("aggregate `{name}` compiled to empty TExpr"))?
            } else {
                texpr
                    .nodes
                    .first()
                    .and_then(|root| root.fn_.as_ref())
                    .and_then(|func| func.aggregate_fn.as_ref())
                    .map(|agg_fn| agg_fn.intermediate_type.clone())
                    .unwrap_or_else(|| {
                        texpr
                            .nodes
                            .first()
                            .map(|root| root.type_.clone())
                            .unwrap_or_else(|| {
                                crate::lower::thrift::type_lowering::scalar_type_desc(
                                    crate::types::TPrimitiveType::NULL_TYPE,
                                )
                            })
                    })
            };
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                agg_tuple_id,
                &name,
                slot_type_desc.clone(),
                nullable,
                col_pos,
            );
            agg_scope.add_column(
                None,
                name,
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type,
                    type_desc: Some(slot_type_desc),
                    nullable,
                },
            );
            aggregate_functions.push(texpr);
        }

        self.desc_builder.add_tuple(agg_tuple_id, None);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
            need_finalize,
        );

        // Pre-order: agg first, then child nodes
        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_sort
    // -------------------------------------------------------------------

    fn visit_sort(
        &mut self,
        op: &PhysicalSortOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let sort_node_id = self.alloc_node();
        // Pass all of child's tuples through Sort. Previously the optimizer
        // forced a GATHER EXCHANGE before Sort whenever the Sort wasn't
        // top-level, which collapsed multi-tuple JOIN output into a single
        // exchange tuple — so taking `.last()` happened to work. With
        // analytic-partition Sorts running directly above multi-tuple JOIN
        // output (no Gather), we must pass every tuple through, otherwise
        // the lowering layout-match check at `lower/node/sort.rs:306` fails
        // with `output column count mismatch`.

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        // Compile analytic-partition exprs (set when this Sort precedes a
        // Window). Emitting them as TSortNode.analytic_partition_exprs tells
        // the pipeline engine to run sort locally per partition instead of
        // doing a global merge — matching StarRocks's parallel analytic
        // sort behaviour. Empty for plain ORDER BY.
        let analytic_partition_exprs = if op.analytic_partition_exprs.is_empty() {
            None
        } else {
            let mut out = Vec::with_capacity(op.analytic_partition_exprs.len());
            for expr in &op.analytic_partition_exprs {
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
                out.push(compiler.compile_typed(expr)?);
            }
            Some(out)
        };

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            None::<Vec<exprs::TExpr>>,
        );

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = -1;
        sort_plan_node.row_tuples = child.tuple_ids.clone();
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: false,
            offset: None,
            ordering_exprs: None,
            is_asc_order: None,
            is_default_limit: None,
            nulls_first: None,
            sort_tuple_slot_exprs: None,
            has_outer_join_child: None,
            sql_sort_keys: None,
            analytic_partition_exprs,
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

        // Pre-order: sort first, then child
        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: OrderingSpec::from_sort_items(&op.items),
        })
    }

    // -------------------------------------------------------------------
    // visit_physical_top_n — Sort + Limit as a single operator
    // -------------------------------------------------------------------

    fn visit_physical_top_n(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        use crate::sql::optimizer::operator::TopNPhase;
        match (op.phase, op.is_split) {
            // Single-stage (today's behavior) and PARTIAL both emit a single
            // SORT_NODE and return. PARTIAL's output is consumed by the
            // FINAL+split visitor without a fragment boundary.
            (TopNPhase::Final, false) | (TopNPhase::Partial, _) => {
                self.visit_physical_top_n_single_or_partial(op, node)
            }
            // FINAL+split: adds a fragment boundary + merging EXCHANGE_NODE.
            (TopNPhase::Final, true) => self.visit_physical_top_n_final_split(op, node),
        }
    }

    fn visit_physical_top_n_single_or_partial(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let sort_node_id = self.alloc_node();
        let sort_tuple_id = *child.tuple_ids.last().unwrap();

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            ordering_exprs.push(texpr);
            is_asc.push(item.asc);
            nulls_first_list.push(item.nulls_first);
        }

        let sort_info = plan_nodes::TSortInfo::new(
            ordering_exprs,
            is_asc,
            nulls_first_list,
            None::<Vec<exprs::TExpr>>,
        );

        let mut sort_plan_node = nodes::default_plan_node();
        sort_plan_node.node_id = sort_node_id;
        sort_plan_node.node_type = plan_nodes::TPlanNodeType::SORT_NODE;
        sort_plan_node.num_children = 1;
        sort_plan_node.limit = op.limit.unwrap_or(-1);
        sort_plan_node.row_tuples = vec![sort_tuple_id];
        sort_plan_node.nullable_tuples = vec![];
        sort_plan_node.compact_data = true;
        sort_plan_node.sort_node = Some(plan_nodes::TSortNode {
            sort_info,
            use_top_n: true,
            offset: op.offset,
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

        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: OrderingSpec::from_sort_items(&op.items),
        })
    }

    /// FINAL+split TopN: close the partial fragment (ending in a SORT_NODE) and
    /// start a coordinator fragment whose root is a merging EXCHANGE_NODE. The
    /// receive side does the k-way merge and applies offset/limit — no final
    /// SORT_NODE is needed because the pre-sorted input streams already give
    /// the merged output its order.
    fn visit_physical_top_n_final_split(
        &mut self,
        op: &PhysicalTopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let parent_fragment_id = self.current_fragment_id()?;
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(&node.children[0]);
        self.fragment_stack.pop();
        let child = child_result?;
        let VisitResult {
            plan_nodes: child_plan_nodes,
            scope: child_scope,
            tuple_ids: child_tuple_ids,
            cte_exchange_nodes,
            ordering: _,
        } = child;

        // PARTIAL should have emitted a SORT_NODE at the head.
        let partial_sort_info = child_plan_nodes
            .first()
            .and_then(|n| n.sort_node.as_ref())
            .map(|s| s.sort_info.clone())
            .ok_or_else(|| {
                let got = child_plan_nodes
                    .first()
                    .map(|n| format!("{:?}", n.node_type))
                    .unwrap_or_else(|| "<empty>".to_string());
                format!(
                    "FINAL+split TopN (node_id={}): expected PARTIAL child's root to be SORT_NODE, got {}",
                    child_plan_nodes
                        .first()
                        .map(|n| n.node_id)
                        .unwrap_or(-1),
                    got
                )
            })?;

        // Close the partial fragment with Unpartitioned/Gather sender into the merging exchange.
        let gather_spec = crate::sql::optimizer::property::DistributionSpec::Gather;
        let output_partition = self.build_output_partition(
            &gather_spec,
            &child_scope,
            &node.children[0].output_columns,
        )?;
        let exchange_partition_type = output_partition.type_;

        let child_dicts = self
            .query_global_dicts_per_fragment
            .remove(&child_fragment_id)
            .filter(|v| !v.is_empty());
        self.completed_fragments.push(FragmentBuildResult {
            fragment_id: child_fragment_id,
            plan: plan_nodes::TPlan::new(child_plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(self.connectors, &[])?,
            output_sink: build_noop_sink(),
            output_columns: node.children[0]
                .output_columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id: None,
            cte_exchange_nodes,
            query_global_dicts: child_dicts,
            query_global_dict_exprs: None,
        });

        let exchange_node_id = self.alloc_node();
        let exchange_node = nodes::build_merging_exchange_node(
            exchange_node_id,
            child_tuple_ids.clone(),
            exchange_partition_type,
            partial_sort_info,
            op.limit,
            op.offset,
        );

        self.completed_edges.push(FragmentEdge {
            source_fragment_id: child_fragment_id,
            target_fragment_id: parent_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition,
            edge_kind: FragmentEdgeKind::Stream,
        });

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope: child_scope,
            tuple_ids: child_tuple_ids,
            cte_exchange_nodes: Vec::new(),
            ordering: OrderingSpec::from_sort_items(&op.items),
        })
    }

    // -------------------------------------------------------------------
    // visit_limit
    // -------------------------------------------------------------------

    fn visit_limit(
        &mut self,
        op: &PhysicalLimitOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "PhysicalLimit expected exactly 1 child, got {}",
                node.children.len()
            ));
        }

        if op.offset.unwrap_or(0) > 0 && !limit_child_can_apply_offset_locally(&node.children[0]) {
            return self.visit_limit_offset_exchange(op, node);
        }

        let mut child = self.visit(&node.children[0])?;

        if let Some(top) = child.plan_nodes.first_mut() {
            if top.node_type == plan_nodes::TPlanNodeType::SORT_NODE {
                top.limit = op.limit.unwrap_or(-1);
                let sort_node = top
                    .sort_node
                    .as_mut()
                    .ok_or_else(|| "SORT_NODE missing sort payload".to_string())?;
                sort_node.offset = op.offset;
            } else {
                if let Some(limit) = op.limit {
                    top.limit = limit;
                }
                if op.offset.unwrap_or(0) > 0 {
                    return Err("LIMIT/OFFSET without a SORT child is not supported".to_string());
                }
            }
        }

        Ok(child)
    }

    fn visit_limit_offset_exchange(
        &mut self,
        op: &PhysicalLimitOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let parent_fragment_id = self.current_fragment_id()?;
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(&node.children[0]);
        self.fragment_stack.pop();
        let child = child_result?;
        let VisitResult {
            plan_nodes: child_plan_nodes,
            scope: child_scope,
            tuple_ids: child_tuple_ids,
            cte_exchange_nodes,
            ordering: _,
        } = child;

        let gather_spec = crate::sql::optimizer::property::DistributionSpec::Gather;
        let output_partition = self.build_output_partition(
            &gather_spec,
            &child_scope,
            &node.children[0].output_columns,
        )?;
        let exchange_partition_type = output_partition.type_;

        let child_dicts = self
            .query_global_dicts_per_fragment
            .remove(&child_fragment_id)
            .filter(|v| !v.is_empty());
        self.completed_fragments.push(FragmentBuildResult {
            fragment_id: child_fragment_id,
            plan: plan_nodes::TPlan::new(child_plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(self.connectors, &[])?,
            output_sink: build_noop_sink(),
            output_columns: node.children[0]
                .output_columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id: None,
            cte_exchange_nodes,
            query_global_dicts: child_dicts,
            query_global_dict_exprs: None,
        });

        let exchange_node_id = self.alloc_node();
        let exchange_node = nodes::build_limit_exchange_node(
            exchange_node_id,
            child_tuple_ids.clone(),
            exchange_partition_type,
            op.limit,
            op.offset,
        );

        self.completed_edges.push(FragmentEdge {
            source_fragment_id: child_fragment_id,
            target_fragment_id: parent_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition,
            edge_kind: FragmentEdgeKind::Stream,
        });

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope: child_scope,
            tuple_ids: child_tuple_ids,
            cte_exchange_nodes: Vec::new(),
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_window
    // -------------------------------------------------------------------

    fn visit_window(
        &mut self,
        op: &PhysicalWindowOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        use crate::sql::analysis::{WindowBound, WindowFrameType};

        // Group window expressions by (partition_by, order_by) signature.
        // Different signatures need separate Sort + Analytic nodes.
        let groups = crate::sql::codegen::helpers::group_win_exprs_by_sig(&op.window_exprs);
        if groups.len() > 1 {
            return self.visit_window_multi_group(op, node, &groups);
        }

        let child = self.visit(&node.children[0])?;
        let analytic_node_id = self.alloc_node();

        let intermediate_tuple_id = self.alloc_tuple();
        let output_tuple_id = self.alloc_tuple();

        // Compile partition_by and order_by from the first window expr
        let first_win = op.window_exprs.first().ok_or("empty window_exprs")?;

        let mut partition_exprs = Vec::new();
        for expr in &first_win.partition_by {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
            partition_exprs.push(compiler.compile_typed(expr)?);
        }

        let mut order_by_exprs = Vec::new();
        for item in &first_win.order_by {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            order_by_exprs.push(texpr);
        }

        // Compile analytic functions
        let mut analytic_functions = Vec::new();
        for win_expr in &op.window_exprs {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
            let agg_call = AggregateCall {
                name: win_expr.name.clone(),
                args: win_expr.args.clone(),
                distinct: win_expr.distinct,
                result_type: win_expr.result_type.clone(),
                order_by: vec![],
            };
            let mut texpr = compiler.compile_aggregate_call_typed(&agg_call)?;
            apply_ignore_nulls_to_root_fn(&mut texpr, win_expr.ignore_nulls);
            analytic_functions.push(texpr);
        }

        // Register intermediate slots
        for (idx, win_expr) in op.window_exprs.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                intermediate_tuple_id,
                &format!("__win_intermediate_{idx}"),
                &win_expr.result_type,
                true,
                idx as i32,
            );
        }
        self.desc_builder.add_tuple(intermediate_tuple_id, None);

        // Register output slots. Forward the child's full scope (both
        // qualified and unqualified) so post-window references like
        // `<table>.col` (produced by `SELECT *` expansion under FROM <table>)
        // still resolve.
        let mut output_scope = ExprScope::new();
        output_scope.merge(&child.scope);
        for (idx, win_expr) in op.window_exprs.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                &win_expr.output_name,
                &win_expr.result_type,
                true,
                idx as i32,
            );
            output_scope.add_column(
                None,
                win_expr.output_name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: win_expr.result_type.clone(),
                    type_desc: None,
                    nullable: true,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id, None);

        // Window frame
        let window = first_win.window_frame.as_ref().map(|frame| {
            let window_type = match frame.frame_type {
                WindowFrameType::Rows => plan_nodes::TAnalyticWindowType::ROWS,
                WindowFrameType::Range => plan_nodes::TAnalyticWindowType::RANGE,
            };
            let window_start = match &frame.start {
                WindowBound::UnboundedPreceding => None,
                WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                    range_offset_predicate: None,
                    rows_offset_value: None,
                }),
                WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::UnboundedFollowing => None,
            };
            let window_end = match &frame.end {
                WindowBound::UnboundedFollowing => None,
                WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                    range_offset_predicate: None,
                    rows_offset_value: None,
                }),
                WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                    type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                    range_offset_predicate: None,
                    rows_offset_value: Some(*n),
                }),
                WindowBound::UnboundedPreceding => None,
            };
            plan_nodes::TAnalyticWindow {
                type_: window_type,
                window_start,
                window_end,
            }
        });

        // Build TAnalyticNode
        let analytic_tnode = plan_nodes::TAnalyticNode {
            partition_exprs,
            order_by_exprs,
            analytic_functions,
            window,
            intermediate_tuple_id,
            output_tuple_id,
            buffered_tuple_id: None,
            partition_by_eq: None,
            order_by_eq: None,
            sql_partition_keys: None,
            sql_aggregate_functions: None,
            has_outer_join_child: None,
            use_hash_based_partition: None,
            is_skewed: None,
        };

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = analytic_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        let mut row_tuples = child.tuple_ids.clone();
        row_tuples.push(output_tuple_id);
        plan_node.row_tuples = row_tuples;
        plan_node.nullable_tuples = vec![];
        plan_node.analytic_node = Some(analytic_tnode);

        // Pre-order: analytic node first, then child
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: child.ordering,
        })
    }

    // -------------------------------------------------------------------
    // visit_window_multi_group
    // -------------------------------------------------------------------

    /// Handle window functions with multiple different partition/order signatures.
    /// Each group gets its own Sort + Analytic node, chained sequentially within
    /// the same fragment (no cross-group exchanges).
    fn visit_window_multi_group(
        &mut self,
        op: &PhysicalWindowOp,
        node: &PhysicalPlanNode,
        groups: &[Vec<usize>],
    ) -> Result<VisitResult, String> {
        use crate::sql::analysis::{WindowBound, WindowFrameType};

        let mut current = self.visit(&node.children[0])?;

        for group_indices in groups {
            let group_exprs: Vec<_> = group_indices
                .iter()
                .map(|&i| op.window_exprs[i].clone())
                .collect();
            let first_win = &group_exprs[0];
            let required_ordering =
                window_ordering_spec(&first_win.partition_by, &first_win.order_by);
            let has_sort_keys =
                !first_win.partition_by.is_empty() || !first_win.order_by.is_empty();
            let ordering_is_representable = !matches!(required_ordering, OrderingSpec::Any);
            let needs_sort = has_sort_keys
                && (!ordering_is_representable || !current.ordering.satisfies(&required_ordering));

            // Build Sort node for this group's partition+order
            let mut sort_ordering = Vec::new();
            let mut sort_is_asc = Vec::new();
            let mut sort_nulls_first_list = Vec::new();
            if needs_sort {
                for expr in &first_win.partition_by {
                    let mut compiler = ExprCompiler::new(self.slot_allocator(), &current.scope);
                    sort_ordering.push(compiler.compile_typed(expr)?);
                    sort_is_asc.push(true);
                    sort_nulls_first_list.push(true);
                }
                for item in &first_win.order_by {
                    let mut compiler = ExprCompiler::new(self.slot_allocator(), &current.scope);
                    sort_ordering.push(compiler.compile_typed(&item.expr)?);
                    sort_is_asc.push(item.asc);
                    sort_nulls_first_list.push(item.nulls_first);
                }
                let sort_node_id = self.alloc_node();
                let sort_plan = nodes::build_sort_node_raw(
                    sort_node_id,
                    current.tuple_ids.clone(),
                    sort_ordering,
                    sort_is_asc,
                    sort_nulls_first_list,
                    -1,
                    None,
                );
                let mut pnodes = vec![sort_plan];
                pnodes.extend(current.plan_nodes);
                current.plan_nodes = pnodes;
                current.ordering = required_ordering.clone();
            }

            // Build Analytic node for this group
            let analytic_node_id = self.alloc_node();
            let intermediate_tuple_id = self.alloc_tuple();
            let output_tuple_id = self.alloc_tuple();

            let mut partition_exprs = Vec::new();
            for expr in &first_win.partition_by {
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &current.scope);
                partition_exprs.push(compiler.compile_typed(expr)?);
            }
            let mut order_by_exprs = Vec::new();
            for item in &first_win.order_by {
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &current.scope);
                order_by_exprs.push(compiler.compile_typed(&item.expr)?);
            }

            let mut analytic_functions = Vec::new();
            for win_expr in &group_exprs {
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &current.scope);
                let agg_call = AggregateCall {
                    name: win_expr.name.clone(),
                    args: win_expr.args.clone(),
                    distinct: win_expr.distinct,
                    result_type: win_expr.result_type.clone(),
                    order_by: vec![],
                };
                let mut texpr = compiler.compile_aggregate_call_typed(&agg_call)?;
                apply_ignore_nulls_to_root_fn(&mut texpr, win_expr.ignore_nulls);
                analytic_functions.push(texpr);
            }

            for (idx, win_expr) in group_exprs.iter().enumerate() {
                let slot_id = self.alloc_slot();
                self.desc_builder.add_slot(
                    slot_id,
                    intermediate_tuple_id,
                    &format!("__win_intermediate_{idx}"),
                    &win_expr.result_type,
                    true,
                    idx as i32,
                );
            }
            self.desc_builder.add_tuple(intermediate_tuple_id, None);

            let mut output_scope = ExprScope::new();
            output_scope.merge(&current.scope);
            for (idx, win_expr) in group_exprs.iter().enumerate() {
                let slot_id = self.alloc_slot();
                self.desc_builder.add_slot(
                    slot_id,
                    output_tuple_id,
                    &win_expr.output_name,
                    &win_expr.result_type,
                    true,
                    idx as i32,
                );
                output_scope.add_column(
                    None,
                    win_expr.output_name.clone(),
                    ColumnBinding {
                        tuple_id: output_tuple_id,
                        slot_id,
                        data_type: win_expr.result_type.clone(),
                        type_desc: None,
                        nullable: true,
                    },
                );
            }
            self.desc_builder.add_tuple(output_tuple_id, None);

            let window = first_win.window_frame.as_ref().map(|frame| {
                let window_type = match frame.frame_type {
                    WindowFrameType::Rows => plan_nodes::TAnalyticWindowType::ROWS,
                    WindowFrameType::Range => plan_nodes::TAnalyticWindowType::RANGE,
                };
                let window_start = match &frame.start {
                    WindowBound::UnboundedPreceding => None,
                    WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                        range_offset_predicate: None,
                        rows_offset_value: None,
                    }),
                    WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                        range_offset_predicate: None,
                        rows_offset_value: Some(*n),
                    }),
                    WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                        range_offset_predicate: None,
                        rows_offset_value: Some(*n),
                    }),
                    WindowBound::UnboundedFollowing => None,
                };
                let window_end = match &frame.end {
                    WindowBound::UnboundedFollowing => None,
                    WindowBound::CurrentRow => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::CURRENT_ROW,
                        range_offset_predicate: None,
                        rows_offset_value: None,
                    }),
                    WindowBound::Following(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::FOLLOWING,
                        range_offset_predicate: None,
                        rows_offset_value: Some(*n),
                    }),
                    WindowBound::Preceding(n) => Some(plan_nodes::TAnalyticWindowBoundary {
                        type_: plan_nodes::TAnalyticWindowBoundaryType::PRECEDING,
                        range_offset_predicate: None,
                        rows_offset_value: Some(*n),
                    }),
                    WindowBound::UnboundedPreceding => None,
                };
                plan_nodes::TAnalyticWindow {
                    type_: window_type,
                    window_start,
                    window_end,
                }
            });

            let analytic_tnode = plan_nodes::TAnalyticNode {
                partition_exprs,
                order_by_exprs,
                analytic_functions,
                window,
                intermediate_tuple_id,
                output_tuple_id,
                buffered_tuple_id: None,
                partition_by_eq: None,
                order_by_eq: None,
                sql_partition_keys: None,
                sql_aggregate_functions: None,
                has_outer_join_child: None,
                use_hash_based_partition: None,
                is_skewed: None,
            };

            let mut plan_node = nodes::default_plan_node();
            plan_node.node_id = analytic_node_id;
            plan_node.node_type = plan_nodes::TPlanNodeType::ANALYTIC_EVAL_NODE;
            plan_node.num_children = 1;
            plan_node.limit = -1;
            let mut new_tuple_ids = current.tuple_ids.clone();
            new_tuple_ids.push(output_tuple_id);
            plan_node.row_tuples = new_tuple_ids.clone();
            plan_node.nullable_tuples = vec![];
            plan_node.analytic_node = Some(analytic_tnode);

            let mut pnodes = vec![plan_node];
            pnodes.extend(current.plan_nodes);
            let cte_exchange_nodes = current.cte_exchange_nodes.clone();
            let ordering = current.ordering.clone();
            current = VisitResult {
                plan_nodes: pnodes,
                scope: output_scope,
                tuple_ids: new_tuple_ids,
                cte_exchange_nodes,
                ordering,
            };
        }

        Ok(current)
    }

    // -------------------------------------------------------------------
    // visit_values
    // -------------------------------------------------------------------

    fn visit_values(
        &mut self,
        op: &PhysicalValuesOp,
        _node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let output_tuple_id = self.alloc_tuple();
        let values_node_id = self.alloc_node();

        let mut scope = ExprScope::new();
        for (idx, col) in op.columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            scope.add_column(
                None,
                col.name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: col.data_type.clone(),
                    type_desc: None,
                    nullable: col.nullable,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id, None);

        let empty_scope = ExprScope::new();
        let mut const_expr_lists = Vec::with_capacity(op.rows.len());
        for row in &op.rows {
            if row.len() != op.columns.len() {
                return Err(format!(
                    "VALUES row column count mismatch: expected {}, got {}",
                    op.columns.len(),
                    row.len()
                ));
            }
            let mut exprs = Vec::with_capacity(row.len());
            for expr in row {
                let mut compiler = ExprCompiler::new(self.slot_allocator(), &empty_scope);
                exprs.push(compiler.compile_typed(expr)?);
            }
            const_expr_lists.push(exprs);
        }

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = values_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::UNION_NODE;
        plan_node.num_children = 0;
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];
        plan_node.union_node = Some(plan_nodes::TUnionNode {
            tuple_id: output_tuple_id,
            result_expr_lists: vec![],
            const_expr_lists,
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        });

        Ok(VisitResult {
            plan_nodes: vec![plan_node],
            scope,
            tuple_ids: vec![output_tuple_id],
            cte_exchange_nodes: Vec::new(),
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_generate_series
    // -------------------------------------------------------------------

    fn visit_generate_series(
        &mut self,
        op: &PhysicalGenerateSeriesOp,
        _node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        if op.step == 0 {
            return Err("generate_series step size cannot equal zero".to_string());
        }

        let param_tuple_id = self.alloc_tuple();
        let param_values_node_id = self.alloc_node();
        let int64_type_desc = type_infer::arrow_type_to_type_desc(&DataType::Int64)?;

        let empty_scope = ExprScope::new();
        let mut param_slots = Vec::with_capacity(3);
        let mut param_exprs = Vec::with_capacity(3);
        for (idx, (name, value)) in [
            ("__gs_start", op.start),
            ("__gs_end", op.end),
            ("__gs_step", op.step),
        ]
        .into_iter()
        .enumerate()
        {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                param_tuple_id,
                name,
                int64_type_desc.clone(),
                false,
                idx as i32,
            );
            param_slots.push(slot_id);
            param_exprs.push(self.compile_int64_literal(value, &empty_scope)?);
        }
        self.desc_builder.add_tuple(param_tuple_id, None);

        let mut param_values_node = nodes::default_plan_node();
        param_values_node.node_id = param_values_node_id;
        param_values_node.node_type = plan_nodes::TPlanNodeType::UNION_NODE;
        param_values_node.num_children = 0;
        param_values_node.row_tuples = vec![param_tuple_id];
        param_values_node.nullable_tuples = vec![];
        param_values_node.union_node = Some(plan_nodes::TUnionNode {
            tuple_id: param_tuple_id,
            result_expr_lists: vec![],
            const_expr_lists: vec![param_exprs],
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        });

        let output_tuple_id = self.alloc_tuple();
        let table_fn_node_id = self.alloc_node();
        let col_name = &op.column_name;

        let slot_id = self.alloc_slot();
        self.desc_builder.add_slot_with_type_desc(
            slot_id,
            output_tuple_id,
            col_name,
            int64_type_desc.clone(),
            false,
            0,
        );
        self.desc_builder.add_tuple(output_tuple_id, None);

        let mut scope = ExprScope::new();
        let qualifier = op
            .alias
            .clone()
            .unwrap_or_else(|| "generate_series".to_string());
        scope.add_column(
            Some(qualifier),
            col_name.clone(),
            ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id,
                data_type: DataType::Int64,
                type_desc: Some(int64_type_desc.clone()),
                nullable: false,
            },
        );

        let table_function_expr = exprs::TExpr::new(vec![exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: int64_type_desc.clone(),
            num_children: 0,
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: "generate_series".to_string(),
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: vec![
                    int64_type_desc.clone(),
                    int64_type_desc.clone(),
                    int64_type_desc.clone(),
                ],
                ret_type: int64_type_desc.clone(),
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: None,
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: Some(types::TTableFunction::new(
                    vec![int64_type_desc.clone()],
                    None::<String>,
                    Some(false),
                )),
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..expr_compiler::default_expr_node()
        }]);

        let mut table_fn_plan_node = nodes::default_plan_node();
        table_fn_plan_node.node_id = table_fn_node_id;
        table_fn_plan_node.node_type = plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE;
        table_fn_plan_node.num_children = 1;
        table_fn_plan_node.limit = -1;
        table_fn_plan_node.row_tuples = vec![output_tuple_id];
        table_fn_plan_node.nullable_tuples = vec![];
        table_fn_plan_node.compact_data = true;
        table_fn_plan_node.table_function_node = Some(plan_nodes::TTableFunctionNode::new(
            Some(table_function_expr),
            Some(param_slots),
            Some(Vec::new()),
            Some(vec![slot_id]),
            Some(true),
        ));

        Ok(VisitResult {
            plan_nodes: vec![table_fn_plan_node, param_values_node],
            scope,
            tuple_ids: vec![output_tuple_id],
            cte_exchange_nodes: Vec::new(),
            ordering: OrderingSpec::Any,
        })
    }

    fn compile_int64_literal(
        &self,
        value: i64,
        empty_scope: &ExprScope,
    ) -> Result<exprs::TExpr, String> {
        let typed = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        };
        let mut compiler = ExprCompiler::new(self.slot_allocator(), empty_scope);
        compiler.compile_typed(&typed)
    }

    // -------------------------------------------------------------------
    // visit_table_function
    // -------------------------------------------------------------------

    fn visit_table_function(
        &mut self,
        op: &PhysicalTableFunctionOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        if !op.function_name.eq_ignore_ascii_case("unnest") {
            return Err(format!(
                "unsupported standalone table function: {}",
                op.function_name
            ));
        }
        if node.children.len() != 1 {
            return Err(format!(
                "table function expects one child, got {}",
                node.children.len()
            ));
        }
        if op.args.len() != op.output_columns.len() {
            return Err(format!(
                "table function output column count mismatch: args={} outputs={}",
                op.args.len(),
                op.output_columns.len()
            ));
        }

        let child = self.visit(&node.children[0])?;

        let project_tuple_id = self.alloc_tuple();
        let project_node_id = self.alloc_node();
        let mut slot_map = BTreeMap::new();
        let mut project_scope = ExprScope::new();
        let mut remapped_child_bindings = HashMap::new();
        let mut outer_columns = Vec::new();
        let mut outer_slots = Vec::new();

        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();
        for (idx, (name, binding)) in child_cols.iter().enumerate() {
            let slot_id = self.alloc_slot();
            let type_desc = expr_compiler::binding_type_desc(binding)?;
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                project_tuple_id,
                name,
                type_desc.clone(),
                binding.nullable,
                idx as i32,
            );
            slot_map.insert(
                slot_id,
                expr_compiler::build_slot_ref_texpr(
                    binding.slot_id,
                    binding.tuple_id,
                    type_desc.clone(),
                ),
            );
            let new_binding = ColumnBinding {
                tuple_id: project_tuple_id,
                slot_id,
                data_type: binding.data_type.clone(),
                type_desc: Some(type_desc),
                nullable: binding.nullable,
            };
            project_scope.add_column(None, name.clone(), new_binding.clone());
            remapped_child_bindings
                .insert((binding.tuple_id, binding.slot_id), new_binding.clone());
            outer_slots.push(slot_id);
            outer_columns.push((name.clone(), new_binding));
        }
        for (qualifier, name, binding) in child.scope.iter_qualified() {
            if let Some(new_binding) =
                remapped_child_bindings.get(&(binding.tuple_id, binding.slot_id))
            {
                project_scope.add_qualified_alias(
                    qualifier.clone(),
                    name.clone(),
                    new_binding.clone(),
                );
            }
        }

        let mut param_slots = Vec::with_capacity(op.args.len());
        let mut param_type_descs = Vec::with_capacity(op.args.len());
        for (idx, arg) in op.args.iter().enumerate() {
            let mut compiler = ExprCompiler::new(self.slot_allocator(), &child.scope);
            let texpr = compiler.compile_typed(arg)?;
            let type_desc = texpr
                .nodes
                .first()
                .map(|root| root.type_.clone())
                .ok_or_else(|| format!("table function arg {idx} compiled to empty TExpr"))?;
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                project_tuple_id,
                &format!("__tf_arg_{idx}"),
                type_desc.clone(),
                arg.nullable,
                (child_cols.len() + idx) as i32,
            );
            slot_map.insert(slot_id, texpr);
            param_slots.push(slot_id);
            param_type_descs.push(type_desc);
        }
        self.desc_builder.add_tuple(project_tuple_id, None);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        let output_tuple_id = self.alloc_tuple();
        let table_fn_node_id = self.alloc_node();
        let mut output_scope = ExprScope::new();
        let mut output_outer_by_project_slot = HashMap::new();

        for (idx, (name, binding)) in outer_columns.iter().enumerate() {
            let type_desc = expr_compiler::binding_type_desc(binding)?;
            self.desc_builder.add_slot_with_type_desc(
                binding.slot_id,
                output_tuple_id,
                name,
                type_desc.clone(),
                binding.nullable,
                idx as i32,
            );
            let output_binding = ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id: binding.slot_id,
                data_type: binding.data_type.clone(),
                type_desc: Some(type_desc),
                nullable: binding.nullable,
            };
            output_scope.add_column(None, name.clone(), output_binding.clone());
            output_outer_by_project_slot.insert(binding.slot_id, output_binding);
        }
        for (qualifier, name, binding) in project_scope.iter_qualified() {
            if let Some(output_binding) = output_outer_by_project_slot.get(&binding.slot_id) {
                output_scope.add_qualified_alias(
                    qualifier.clone(),
                    name.clone(),
                    output_binding.clone(),
                );
            }
        }

        let mut fn_result_slots = Vec::with_capacity(op.output_columns.len());
        let mut ret_type_descs = Vec::with_capacity(op.output_columns.len());
        let result_qualifier = op.alias.clone().or_else(|| Some(op.function_name.clone()));
        for (idx, col) in op.output_columns.iter().enumerate() {
            let DataType::List(item_field) = &op.args[idx].data_type else {
                return Err(format!(
                    "UNNEST argument {} must be ARRAY, got {:?}",
                    idx + 1,
                    op.args[idx].data_type
                ));
            };
            if item_field.data_type() != &col.data_type {
                return Err(format!(
                    "UNNEST result type mismatch for column {}: arg item={:?} output={:?}",
                    col.name,
                    item_field.data_type(),
                    col.data_type
                ));
            }
            let slot_id = self.alloc_slot();
            let type_desc = type_infer::arrow_type_to_type_desc(&col.data_type)?;
            self.desc_builder.add_slot_with_type_desc(
                slot_id,
                output_tuple_id,
                &col.name,
                type_desc.clone(),
                true,
                (outer_columns.len() + idx) as i32,
            );
            let binding = ColumnBinding {
                tuple_id: output_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: Some(type_desc.clone()),
                nullable: true,
            };
            output_scope.add_column(result_qualifier.clone(), col.name.clone(), binding);
            fn_result_slots.push(slot_id);
            ret_type_descs.push(type_desc);
        }
        self.desc_builder.add_tuple(output_tuple_id, None);

        let ret_type = ret_type_descs
            .first()
            .cloned()
            .ok_or_else(|| "table function requires at least one return type".to_string())?;
        let table_function_expr = exprs::TExpr::new(vec![exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: ret_type.clone(),
            num_children: 0,
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: op.function_name.clone(),
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: param_type_descs,
                ret_type,
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: None,
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: Some(types::TTableFunction::new(
                    ret_type_descs,
                    None::<String>,
                    Some(op.is_left_join),
                )),
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..expr_compiler::default_expr_node()
        }]);

        let mut table_fn_plan_node = nodes::default_plan_node();
        table_fn_plan_node.node_id = table_fn_node_id;
        table_fn_plan_node.node_type = plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE;
        table_fn_plan_node.num_children = 1;
        table_fn_plan_node.limit = -1;
        table_fn_plan_node.row_tuples = vec![output_tuple_id];
        table_fn_plan_node.nullable_tuples = vec![];
        table_fn_plan_node.compact_data = true;
        table_fn_plan_node.table_function_node = Some(plan_nodes::TTableFunctionNode::new(
            Some(table_function_expr),
            Some(param_slots),
            Some(outer_slots),
            Some(fn_result_slots),
            Some(true),
        ));

        let mut plan_nodes = vec![table_fn_plan_node, project_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: vec![output_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_subquery_alias
    // -------------------------------------------------------------------

    fn visit_subquery_alias(
        &mut self,
        op: &PhysicalSubqueryAliasOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let mut child = self.visit(&node.children[0])?;
        let child_output_bindings: Vec<_> = child
            .scope
            .iter_columns()
            .map(|(_, binding)| binding.clone())
            .collect();

        // Register all output columns with the alias as qualifier. Per the
        // G1 invariant ("SubqueryAlias does not create new ids, only changes
        // the display name") we also re-index each binding under the alias's
        // ColumnId so the by-id lookup follows the column through the alias
        // boundary.
        for (idx, col) in op.output_columns.iter().enumerate() {
            let col_name_lower = col.name.to_lowercase();
            let binding = child
                .scope
                .resolve_by_id(col.column_id)
                .cloned()
                .or_else(|| child.scope.resolve_column(None, &col_name_lower).cloned().ok())
                .or_else(|| child_output_bindings.get(idx).cloned())
                .ok_or_else(|| {
                    format!(
                        "subquery alias '{}' exposes column '{}' at position {} but child has only {} columns",
                        op.alias,
                        col.name,
                        idx,
                        child_output_bindings.len()
                    )
                })?;
            child.scope.add_column_with_id(
                col.column_id,
                Some(op.alias.clone()),
                col.name.clone(),
                binding,
            );
        }

        Ok(child)
    }

    // -------------------------------------------------------------------
    // visit_repeat
    // -------------------------------------------------------------------

    fn visit_repeat(
        &mut self,
        op: &PhysicalRepeatOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let child = self.visit(&node.children[0])?;

        let repeat_node_id = self.alloc_node();

        let has_grouping_fns = !op.grouping_fn_args.is_empty();
        let virtual_tuple_id = self.alloc_tuple();

        // Collect child columns for rollup slot mapping
        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();

        // Start with the child's full scope
        let mut output_scope = child.scope;

        for (original_name, alias_name) in &op.grouping_key_aliases {
            if let Ok(binding) = output_scope.resolve_column(None, alias_name) {
                output_scope.add_qualified_alias(
                    "__repeat_group".to_string(),
                    original_name.clone(),
                    binding.clone(),
                );
            }
        }

        // Add virtual slots
        let num_virtual = 1 + op.grouping_fn_args.len();
        let mut virtual_slot_ids = Vec::with_capacity(num_virtual);

        let grouping_id_slot = self.alloc_slot();
        self.desc_builder.add_slot(
            grouping_id_slot,
            virtual_tuple_id,
            "__grouping_id",
            &DataType::Int64,
            false,
            0,
        );
        if !op.grouping_fn_args.is_empty() {
            output_scope.add_column(
                None,
                "__grouping_id".to_string(),
                ColumnBinding {
                    tuple_id: virtual_tuple_id,
                    slot_id: grouping_id_slot,
                    data_type: DataType::Int64,
                    type_desc: None,
                    nullable: false,
                },
            );
        }
        virtual_slot_ids.push(grouping_id_slot);

        for (fn_idx, (fn_name, _)) in op.grouping_fn_args.iter().enumerate() {
            let slot = self.alloc_slot();
            self.desc_builder.add_slot(
                slot,
                virtual_tuple_id,
                fn_name,
                &DataType::Int64,
                false,
                1 + fn_idx as i32,
            );
            output_scope.add_column(
                None,
                fn_name.clone(),
                ColumnBinding {
                    tuple_id: virtual_tuple_id,
                    slot_id: slot,
                    data_type: DataType::Int64,
                    type_desc: None,
                    nullable: false,
                },
            );
            virtual_slot_ids.push(slot);
        }

        self.desc_builder.add_tuple(virtual_tuple_id, None);

        // Build slot_id_set_list and all_rollup_slot_ids
        let all_rollup_slot_ids: BTreeSet<i32> = op
            .all_rollup_columns
            .iter()
            .filter_map(|col| {
                child_cols.iter().find_map(|(name, binding)| {
                    if name.to_lowercase() == col.to_lowercase() {
                        Some(binding.slot_id)
                    } else {
                        None
                    }
                })
            })
            .collect();

        let slot_id_set_list: Vec<BTreeSet<i32>> = op
            .repeat_column_ref_list
            .iter()
            .map(|non_null_cols| {
                non_null_cols
                    .iter()
                    .filter_map(|col| {
                        child_cols.iter().find_map(|(name, binding)| {
                            if name.to_lowercase() == col.to_lowercase() {
                                Some(binding.slot_id)
                            } else {
                                None
                            }
                        })
                    })
                    .collect()
            })
            .collect();

        // Build grouping_list
        let repeat_times = op.grouping_ids.len();
        let mut grouping_list: Vec<Vec<i64>> = Vec::with_capacity(num_virtual);

        grouping_list.push(op.grouping_ids.iter().map(|g| *g as i64).collect());

        for (_fn_name, fn_args) in &op.grouping_fn_args {
            let mut values = Vec::with_capacity(repeat_times);
            for non_null_cols in &op.repeat_column_ref_list {
                let mut bits: u64 = 0;
                for (bit_pos, arg_col) in fn_args.iter().enumerate() {
                    let is_null = !non_null_cols
                        .iter()
                        .any(|c| c.to_lowercase() == arg_col.to_lowercase());
                    if is_null {
                        let reverse_bit_pos = fn_args.len() - 1 - bit_pos;
                        bits |= 1 << reverse_bit_pos;
                    }
                }
                values.push(bits as i64);
            }
            grouping_list.push(values);
        }

        let repeat_id_list: Vec<i64> = op.grouping_ids.iter().map(|g| *g as i64).collect();

        // Build TPlanNode
        let mut row_tuples = child.tuple_ids.clone();
        if has_grouping_fns {
            row_tuples.push(virtual_tuple_id);
        }

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = repeat_node_id;
        plan_node.node_type = plan_nodes::TPlanNodeType::REPEAT_NODE;
        plan_node.num_children = 1;
        plan_node.limit = -1;
        plan_node.row_tuples = row_tuples;
        plan_node.nullable_tuples = vec![];
        plan_node.compact_data = true;
        plan_node.repeat_node = Some(plan_nodes::TRepeatNode {
            output_tuple_id: virtual_tuple_id,
            slot_id_set_list,
            repeat_id_list,
            grouping_list,
            all_slot_ids: all_rollup_slot_ids,
        });

        // Pre-order: repeat node first, then child nodes
        let mut plan_nodes = vec![plan_node];
        plan_nodes.extend(child.plan_nodes);

        // Output tuple_ids
        let mut output_tuple_ids = child.tuple_ids;
        if has_grouping_fns {
            output_tuple_ids.push(virtual_tuple_id);
        }

        Ok(VisitResult {
            plan_nodes,
            scope: output_scope,
            tuple_ids: output_tuple_ids,
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_distribution
    // -------------------------------------------------------------------

    fn build_output_partition(
        &self,
        spec: &crate::sql::optimizer::property::DistributionSpec,
        child_scope: &ExprScope,
        output_columns: &[crate::sql::analysis::OutputColumn],
    ) -> Result<partitions::TDataPartition, String> {
        match spec {
            crate::sql::optimizer::property::DistributionSpec::Gather => {
                Ok(unpartitioned_stream_partition())
            }
            crate::sql::optimizer::property::DistributionSpec::HashPartitioned { cols, .. } => {
                // For shuffle joins, cols contains ALL eq key columns from both
                // sides. Pick the ones that resolve in this child's scope.
                //
                // G1: prefer ColumnId-based lookup against the child scope's
                // id index. Fall back to the legacy path that resolves a
                // ColumnId → display name (via output_columns) → name lookup
                // for scopes / call sites that have not yet been migrated to
                // register ColumnIds.
                let mut partition_exprs = Vec::new();
                let mut used_ids = std::collections::HashSet::new();
                let mut used_names = std::collections::HashSet::new();
                for col_id in cols.iter() {
                    if used_ids.contains(col_id) {
                        continue; // skip duplicate column ids
                    }
                    if let Some(binding) = child_scope.resolve_by_id(*col_id) {
                        let binding = binding.clone();
                        let type_desc = expr_compiler::binding_type_desc(&binding)?;
                        partition_exprs.push(expr_compiler::build_slot_ref_texpr(
                            binding.slot_id,
                            binding.tuple_id,
                            type_desc,
                        ));
                        used_ids.insert(*col_id);
                        continue;
                    }
                    // Fallback: ColumnId → name (via output_columns) → name lookup.
                    let col_meta = output_columns.iter().find(|oc| oc.column_id == *col_id);
                    let col_name = match col_meta {
                        Some(oc) => oc.name.clone(),
                        None => continue, // column not in this child's output
                    };
                    if used_names.contains(&col_name.to_lowercase()) {
                        continue; // skip duplicate column names
                    }
                    if let Ok(binding) = child_scope.resolve_column(None, &col_name) {
                        let binding = binding.clone();
                        let type_desc = expr_compiler::binding_type_desc(&binding)?;
                        partition_exprs.push(expr_compiler::build_slot_ref_texpr(
                            binding.slot_id,
                            binding.tuple_id,
                            type_desc,
                        ));
                        used_names.insert(col_name.to_lowercase());
                    }
                }
                if partition_exprs.is_empty() {
                    // A `HashPartitioned` requirement whose cols are all
                    // invisible to the immediate child indicates the
                    // optimizer asked one side of a join to be partitioned
                    // by the OTHER side's key — most commonly with a
                    // chained `FULL OUTER JOIN … USING(k)` whose final
                    // INNER-join key is `coalesce(coalesce(…), tN.id)`
                    // (last term resolved against the build side). The
                    // selected physical plan is a BROADCAST join, so the
                    // hash partitioning was never going to be used at
                    // runtime; emitting it as an error blocks an otherwise
                    // valid plan. Treat it as the no-op UNPARTITIONED
                    // distribution: the child fragment streams as one
                    // partition, and the broadcast exchange downstream
                    // still produces correct output.
                    return Ok(unpartitioned_stream_partition());
                }
                Ok(partitions::TDataPartition::new(
                    partitions::TPartitionType::HASH_PARTITIONED,
                    Some(partition_exprs),
                    None::<Vec<partitions::TRangePartition>>,
                    None::<Vec<partitions::TBucketProperty>>,
                ))
            }
            crate::sql::optimizer::property::DistributionSpec::Any => {
                Err("PhysicalDistribution(Any) is not supported in fragment builder".to_string())
            }
        }
    }

    fn visit_distribution(
        &mut self,
        op: &PhysicalDistributionOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "PhysicalDistribution expected exactly 1 child, got {}",
                node.children.len()
            ));
        }

        let parent_fragment_id = self.current_fragment_id()?;
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(&node.children[0]);
        self.fragment_stack.pop();
        let child = child_result?;
        let VisitResult {
            plan_nodes,
            scope,
            tuple_ids,
            cte_exchange_nodes,
            ordering: _,
        } = child;

        let output_partition =
            self.build_output_partition(&op.spec, &scope, &node.children[0].output_columns)?;
        let exchange_partition_type = output_partition.type_;

        let child_dicts = self
            .query_global_dicts_per_fragment
            .remove(&child_fragment_id)
            .filter(|v| !v.is_empty());
        self.completed_fragments.push(FragmentBuildResult {
            fragment_id: child_fragment_id,
            plan: plan_nodes::TPlan::new(plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(self.connectors, &[])?,
            output_sink: build_noop_sink(),
            output_columns: node.children[0]
                .output_columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id: None,
            cte_exchange_nodes,
            query_global_dicts: child_dicts,
            query_global_dict_exprs: None,
        });

        let exchange_node_id = self.alloc_node();
        let exchange_node = nodes::build_exchange_node(
            exchange_node_id,
            tuple_ids.clone(),
            exchange_partition_type,
        );

        self.completed_edges.push(FragmentEdge {
            source_fragment_id: child_fragment_id,
            target_fragment_id: parent_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition,
            edge_kind: FragmentEdgeKind::Stream,
        });

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope,
            tuple_ids,
            cte_exchange_nodes: Vec::new(),
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_union / visit_intersect / visit_except
    // -------------------------------------------------------------------

    fn visit_union(
        &mut self,
        op: &PhysicalUnionOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        let result = self.visit_set_op_common(
            node,
            plan_nodes::TPlanNodeType::UNION_NODE,
            |plan_node, tnode| {
                plan_node.union_node = Some(tnode);
            },
        )?;
        if op.all {
            Ok(result)
        } else {
            self.emit_distinct_on_top(result)
        }
    }

    fn visit_intersect(
        &mut self,
        _op: &PhysicalIntersectOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        self.visit_set_op_common(
            node,
            plan_nodes::TPlanNodeType::INTERSECT_NODE,
            |plan_node, tnode| {
                plan_node.intersect_node = Some(plan_nodes::TIntersectNode {
                    tuple_id: tnode.tuple_id,
                    result_expr_lists: tnode.result_expr_lists,
                    const_expr_lists: tnode.const_expr_lists,
                    first_materialized_child_idx: tnode.first_materialized_child_idx,
                    has_outer_join_child: None,
                    local_partition_by_exprs: None,
                });
            },
        )
    }

    fn visit_except(
        &mut self,
        _op: &PhysicalExceptOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        self.visit_set_op_common(
            node,
            plan_nodes::TPlanNodeType::EXCEPT_NODE,
            |plan_node, tnode| {
                plan_node.except_node = Some(plan_nodes::TExceptNode {
                    tuple_id: tnode.tuple_id,
                    result_expr_lists: tnode.result_expr_lists,
                    const_expr_lists: tnode.const_expr_lists,
                    first_materialized_child_idx: tnode.first_materialized_child_idx,
                    local_partition_by_exprs: None,
                });
            },
        )
    }

    fn visit_set_op_common(
        &mut self,
        node: &PhysicalPlanNode,
        node_type: plan_nodes::TPlanNodeType,
        apply_payload: impl FnOnce(&mut plan_nodes::TPlanNode, plan_nodes::TUnionNode),
    ) -> Result<VisitResult, String> {
        if node.children.is_empty() {
            return Err("set operation node has no inputs".into());
        }

        let mut child_results = Vec::with_capacity(node.children.len());
        for child in &node.children {
            child_results.push(self.visit(child)?);
        }

        let output_tuple_id = self.alloc_tuple();
        let set_op_node_id = self.alloc_node();

        let output_columns: Vec<crate::sql::analysis::OutputColumn> =
            if node.output_columns.is_empty() {
                child_results[0]
                    .scope
                    .iter_columns()
                    .map(|(name, binding)| crate::sql::analysis::OutputColumn {
                        column_id: ColumnId::UNSET,
                        name: name.clone(),
                        data_type: binding.data_type.clone(),
                        nullable: binding.nullable,
                        is_internal: false,
                    })
                    .collect::<Vec<_>>()
            } else {
                node.output_columns.clone()
            };

        let mut output_scope = ExprScope::new();
        let first_child_cols: Vec<(String, ColumnBinding)> = child_results[0]
            .scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();

        if first_child_cols.len() != output_columns.len() {
            return Err(format!(
                "set operation column count mismatch during codegen: child has {}, output has {}",
                first_child_cols.len(),
                output_columns.len()
            ));
        }

        for (idx, output_col) in output_columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                &output_col.name,
                &output_col.data_type,
                output_col.nullable,
                idx as i32,
            );
            output_scope.add_column(
                None,
                output_col.name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: output_col.data_type.clone(),
                    type_desc: None,
                    nullable: output_col.nullable,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id, None);

        let mut result_expr_lists = Vec::with_capacity(child_results.len());
        for child_result in &child_results {
            let mut expr_list = Vec::new();
            for (col_idx, (_, child_binding)) in child_result.scope.iter_columns().enumerate() {
                let output_col = output_columns.get(col_idx).ok_or_else(|| {
                    format!("missing output column {} for set operation", col_idx)
                })?;
                let needs_cast = child_binding.data_type != output_col.data_type;
                if needs_cast {
                    let target_desc = type_infer::arrow_type_to_type_desc(&output_col.data_type)?;
                    let child_desc = expr_compiler::binding_type_desc(child_binding)?;
                    let slot_ref = expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        child_desc,
                    );
                    expr_list.push(expr_compiler::build_cast_texpr(slot_ref, target_desc));
                } else {
                    let type_desc = expr_compiler::binding_type_desc(child_binding)?;
                    expr_list.push(expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        type_desc,
                    ));
                }
            }
            result_expr_lists.push(expr_list);
        }

        let tnode = plan_nodes::TUnionNode {
            tuple_id: output_tuple_id,
            result_expr_lists,
            const_expr_lists: vec![],
            first_materialized_child_idx: 0,
            pass_through_slot_maps: None,
            local_exchanger_type: None,
            local_partition_by_exprs: None,
        };

        let mut plan_node = nodes::default_plan_node();
        plan_node.node_id = set_op_node_id;
        plan_node.node_type = node_type;
        plan_node.row_tuples = vec![output_tuple_id];
        plan_node.nullable_tuples = vec![];

        apply_payload(&mut plan_node, tnode);

        plan_node.num_children = child_results.len() as i32;
        let mut plan_nodes_out = vec![plan_node];
        let mut cte_exchange_nodes = Vec::new();
        for child_result in child_results {
            plan_nodes_out.extend(child_result.plan_nodes);
            cte_exchange_nodes.extend(child_result.cte_exchange_nodes);
        }

        Ok(VisitResult {
            plan_nodes: plan_nodes_out,
            scope: output_scope,
            tuple_ids: vec![output_tuple_id],
            cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    fn emit_distinct_on_top(&mut self, child: VisitResult) -> Result<VisitResult, String> {
        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        let child_cols: Vec<(String, ColumnBinding)> = child
            .scope
            .iter_columns()
            .map(|(n, b)| (n.clone(), b.clone()))
            .collect();

        for (idx, (name, binding)) in child_cols.iter().enumerate() {
            let type_desc = expr_compiler::binding_type_desc(binding)?;
            let texpr =
                expr_compiler::build_slot_ref_texpr(binding.slot_id, binding.tuple_id, type_desc);
            grouping_exprs.push(texpr);

            let slot_id = self.alloc_slot();
            if let Some(slot_type_desc) = binding.type_desc.clone() {
                self.desc_builder.add_slot_with_type_desc(
                    slot_id,
                    agg_tuple_id,
                    name,
                    slot_type_desc,
                    binding.nullable,
                    idx as i32,
                );
            } else {
                self.desc_builder.add_slot(
                    slot_id,
                    agg_tuple_id,
                    name,
                    &binding.data_type,
                    binding.nullable,
                    idx as i32,
                );
            }
            agg_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type: binding.data_type.clone(),
                    type_desc: binding.type_desc.clone(),
                    nullable: binding.nullable,
                },
            );
        }

        self.desc_builder.add_tuple(agg_tuple_id, None);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            vec![],
            true,
        );

        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_cte_anchor
    // -------------------------------------------------------------------

    fn visit_cte_anchor(
        &mut self,
        _op: &PhysicalCTEAnchorOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // Visit the produce subtree first — this creates a completed CTE
        // fragment (stored in self.completed_fragments / self.cte_fragments)
        // as a side effect. The returned VisitResult is intentionally discarded
        // because the anchor's output comes entirely from the consumer subtree.
        let _ = self.visit(&node.children[0])?;
        self.visit(&node.children[1])
    }

    // -------------------------------------------------------------------
    // visit_cte_produce
    // -------------------------------------------------------------------

    fn visit_cte_produce(
        &mut self,
        op: &PhysicalCTEProduceOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // Allocate the CTE fragment ID before visiting the child so that
        // any Distribution nodes inside the child correctly target this
        // CTE fragment as their parent in the fragment_stack.
        let cte_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(cte_fragment_id);
        let child_result = self.visit(&node.children[0]);
        self.fragment_stack.pop();
        let child = child_result?;
        let cte_dicts = self
            .query_global_dicts_per_fragment
            .remove(&cte_fragment_id)
            .filter(|v| !v.is_empty());
        let cte_fragment = FragmentBuildResult {
            fragment_id: cte_fragment_id,
            plan: plan_nodes::TPlan::new(child.plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(self.connectors, &[])?,
            output_sink: build_noop_sink(),
            output_columns: op
                .output_columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id: Some(op.cte_id),
            cte_exchange_nodes: child.cte_exchange_nodes,
            query_global_dicts: cte_dicts,
            query_global_dict_exprs: None,
        };
        let idx = self.completed_fragments.len();
        self.completed_fragments.push(cte_fragment);
        self.cte_fragments.insert(op.cte_id, idx);

        Ok(VisitResult {
            plan_nodes: Vec::new(),
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: Vec::new(),
            ordering: OrderingSpec::Any,
        })
    }

    // -------------------------------------------------------------------
    // visit_cte_consume
    // -------------------------------------------------------------------

    fn visit_cte_consume(&mut self, op: &PhysicalCTEConsumeOp) -> Result<VisitResult, String> {
        // Verify the CTE produce fragment was already visited.
        let cte_frag_idx = self
            .cte_fragments
            .get(&op.cte_id)
            .copied()
            .ok_or_else(|| format!("CTE consume references unknown cte_id={}", op.cte_id))?;
        let cte_fragment_id = self.completed_fragments[cte_frag_idx].fragment_id;

        // Allocate an exchange node that will receive data from the CTE
        // produce fragment's multicast sink.
        let exchange_node_id = self.alloc_node();

        // Build the scope from the CTE consume's declared output columns
        // so that parent operators can resolve column references.
        let exchange_tuple_id = self.alloc_tuple();
        let mut scope = ExprScope::new();

        for (idx, col) in op.output_columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                exchange_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: exchange_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                type_desc: None,
                nullable: col.nullable,
            };
            // Register the CTE-consumed column under its ColumnId so that
            // ID-based lookups (e.g. distribution column resolution in
            // `build_output_partition`) succeed when an outer operator needs
            // to find the hash partition columns produced by the CTE.
            scope.add_column_with_id(col.column_id, None, col.name.clone(), binding.clone());
            // Also register the column under the CTE alias as qualifier so
            // that `alias.col` references in the consuming query resolve.
            scope.add_qualified_alias(op.alias.clone(), col.name.clone(), binding);
        }
        self.desc_builder.add_tuple(exchange_tuple_id, None);

        let exchange_node = nodes::build_exchange_node(
            exchange_node_id,
            vec![exchange_tuple_id],
            partitions::TPartitionType::UNPARTITIONED,
        );

        // Record the CTE multicast edge so the coordinator can wire sinks.
        let target_fragment_id = self.current_fragment_id()?;
        self.completed_edges.push(FragmentEdge {
            source_fragment_id: cte_fragment_id,
            target_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: unpartitioned_stream_partition(),
            edge_kind: FragmentEdgeKind::CteMulticast { cte_id: op.cte_id },
        });

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope,
            tuple_ids: vec![exchange_tuple_id],
            cte_exchange_nodes: vec![(op.cte_id, exchange_node_id)],
            ordering: OrderingSpec::Any,
        })
    }
}

fn synthetic_iceberg_table_id(scan_node_id: i32) -> i64 {
    -(scan_node_id as i64)
}

/// Set `TFunction.ignore_nulls` on the root function node of an analytic-call
/// `TExpr` so the BE-side `lower_window_function` picks up the modifier when
/// constructing `WindowFunctionKind::FirstValue/LastValue/Lead/Lag`.
fn apply_ignore_nulls_to_root_fn(texpr: &mut exprs::TExpr, ignore_nulls: bool) {
    if !ignore_nulls {
        return;
    }
    if let Some(root) = texpr.nodes.first_mut()
        && let Some(fn_) = root.fn_.as_mut()
    {
        fn_.ignore_nulls = Some(true);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unpartitioned_stream_partition() -> partitions::TDataPartition {
    partitions::TDataPartition::new(
        partitions::TPartitionType::UNPARTITIONED,
        None::<Vec<crate::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    )
}

fn build_result_sink() -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::RESULT_SINK,
        None::<data_sinks::TDataStreamSink>,
        Some(data_sinks::TResultSink::default()),
        None::<data_sinks::TMysqlTableSink>,
        None::<data_sinks::TExportSink>,
        None::<data_sinks::TOlapTableSink>,
        None::<data_sinks::TMemoryScratchSink>,
        None::<data_sinks::TMultiCastDataStreamSink>,
        None::<data_sinks::TSchemaTableSink>,
        None::<data_sinks::TIcebergTableSink>,
        None::<data_sinks::THiveTableSink>,
        None::<data_sinks::TTableFunctionTableSink>,
        None::<data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<data_sinks::TDataSink>>>,
        None::<i64>,
        None::<data_sinks::TSplitDataStreamSink>,
    )
}

/// Placeholder sink for child / CTE fragments.  The coordinator replaces
/// this with the real DataStreamSink or MultiCastDataStreamSink after
/// fragment instance IDs are assigned.
fn build_noop_sink() -> data_sinks::TDataSink {
    data_sinks::TDataSink::new(
        data_sinks::TDataSinkType::NOOP_SINK,
        None::<data_sinks::TDataStreamSink>,
        None::<data_sinks::TResultSink>,
        None::<data_sinks::TMysqlTableSink>,
        None::<data_sinks::TExportSink>,
        None::<data_sinks::TOlapTableSink>,
        None::<data_sinks::TMemoryScratchSink>,
        None::<data_sinks::TMultiCastDataStreamSink>,
        None::<data_sinks::TSchemaTableSink>,
        None::<data_sinks::TIcebergTableSink>,
        None::<data_sinks::THiveTableSink>,
        None::<data_sinks::TTableFunctionTableSink>,
        None::<data_sinks::TDictionaryCacheSink>,
        None::<Vec<Box<data_sinks::TDataSink>>>,
        None::<i64>,
        None::<data_sinks::TSplitDataStreamSink>,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;

    use arrow::datatypes::DataType;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::plan_nodes;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, SortItem, TypedExpr, WindowBound,
        WindowFrame, WindowFrameType,
    };
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergColumnStats, IcebergDataFileInfo,
        IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
        IcebergPartitionFieldValue, IcebergPartitionValue, IcebergSchemaDef, IcebergSchemaFieldDef,
        IcebergTableInfo, PhysicalTableLayout, ScanSource, StarRocksTabletRef, TableDef,
    };
    use crate::sql::optimizer::operator::{
        JoinDistribution, Operator, PhysicalDistributionOp, PhysicalGenerateSeriesOp,
        PhysicalHashJoinEqCondition, PhysicalHashJoinOp, PhysicalScanOp, PhysicalSortOp,
        PhysicalWindowOp, ScanDictionaryColumn,
    };
    use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::statistics::Statistics;
    use crate::sql::planner::plan::WindowExpr;

    /// OQ-5 B1: `remap_rf_expr_order` must translate a runtime filter's
    /// pre-demote `op.eq_conditions` index into the post-demote
    /// `eq_join_conjuncts` index that BE lowering uses, and drop (return
    /// `None`) any RF whose source conjunct was demoted to
    /// `other_join_conjuncts`.
    #[test]
    fn rf_expr_order_remap_handles_demote() {
        // No demotion: surviving conjuncts cover every source index in order,
        // so each pre-demote index maps to itself.
        let identity = [0usize, 1, 2];
        assert_eq!(remap_rf_expr_order(&identity, 0), Some(0));
        assert_eq!(remap_rf_expr_order(&identity, 1), Some(1));
        assert_eq!(remap_rf_expr_order(&identity, 2), Some(2));

        // Earlier conjunct (source index 0) demoted: surviving conjuncts are
        // source indices [1, 2]. An RF on the demoted index 0 must be dropped;
        // indices 1 and 2 shift down to post-demote positions 0 and 1.
        let first_demoted = [1usize, 2];
        assert_eq!(
            remap_rf_expr_order(&first_demoted, 0),
            None,
            "RF on a demoted conjunct must be dropped"
        );
        assert_eq!(remap_rf_expr_order(&first_demoted, 1), Some(0));
        assert_eq!(remap_rf_expr_order(&first_demoted, 2), Some(1));

        // Middle conjunct (source index 1) demoted: surviving = [0, 2]. The
        // surviving RF on source index 2 lands at post-demote position 1 —
        // exactly the index BE uses into build_keys/probe_keys/eq_null_safe.
        let middle_demoted = [0usize, 2];
        assert_eq!(remap_rf_expr_order(&middle_demoted, 0), Some(0));
        assert_eq!(remap_rf_expr_order(&middle_demoted, 1), None);
        assert_eq!(remap_rf_expr_order(&middle_demoted, 2), Some(1));

        // Every remapped index is in range for the post-demote conjunct list
        // (whose length equals `surviving_eq_origin.len()`), which is the
        // invariant the defensive guard in `build_rf_descriptors` relies on.
        for origin in [&identity[..], &first_demoted[..], &middle_demoted[..]] {
            for src in 0..3usize {
                if let Some(j) = remap_rf_expr_order(origin, src) {
                    assert!(
                        j < origin.len(),
                        "post-demote index {j} out of range for {origin:?}"
                    );
                }
            }
        }

        // An out-of-range source index (no matching surviving conjunct) is
        // dropped rather than mis-mapped.
        assert_eq!(remap_rf_expr_order(&identity, 7), None);
        assert_eq!(remap_rf_expr_order(&[], 0), None);
    }

    fn test_iceberg_table_info_with_schema(fields: Vec<IcebergSchemaFieldDef>) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/test_table".to_string(),
            schema: IcebergSchemaDef { fields },
            serialized_metadata: None,
        }
    }

    fn test_iceberg_table_info() -> IcebergTableInfo {
        test_iceberg_table_info_with_schema(vec![])
    }

    fn test_iceberg_table_info_with_id_schema() -> IcebergTableInfo {
        test_iceberg_table_info_with_schema(vec![IcebergSchemaFieldDef {
            field_id: 1,
            name: "id".to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            children: vec![],
        }])
    }

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in scan-only builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            Ok(None)
        }
    }

    struct StarRocksCatalog {
        layout: PhysicalTableLayout,
    }

    impl CatalogProvider for StarRocksCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in StarRocks scan builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            _table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            Ok(Some(self.layout.clone()))
        }
    }

    struct MixedCatalog {
        starrocks_layout: PhysicalTableLayout,
    }

    impl CatalogProvider for MixedCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in mixed scan builder tests".to_string())
        }

        fn get_physical_layout(
            &self,
            _database: &str,
            table: &str,
        ) -> Result<Option<PhysicalTableLayout>, String> {
            if table == "starrocks_t" {
                Ok(Some(self.starrocks_layout.clone()))
            } else {
                Ok(None)
            }
        }
    }

    #[derive(Debug)]
    struct MockScanPlanner {
        schema_id: i64,
        splits: Vec<crate::connector::starrocks::table::StarRocksSplit>,
    }

    impl crate::connector::scan_planning::ConnectorScanPlanner for MockScanPlanner {
        fn name(&self) -> &'static str {
            "starrocks"
        }

        fn begin_scan(
            &self,
            table: crate::connector::scan_planning::TableHandle,
            _ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            let inner = table
                .downcast_ref::<crate::connector::starrocks::table::StarRocksTableHandle>()
                .ok_or_else(|| "MockScanPlanner expected StarRocksTableHandle".to_string())?
                .clone();
            Ok(crate::connector::scan_planning::ScanHandle::new(
                "starrocks",
                crate::connector::starrocks::table::StarRocksScanHandle {
                    table: inner,
                    schema_id: self.schema_id,
                },
            ))
        }

        fn plan_splits(
            &self,
            _scan: &crate::connector::scan_planning::ScanHandle,
            _ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            Ok(self
                .splits
                .iter()
                .map(|split| {
                    crate::connector::scan_planning::Split::new("starrocks", split.clone())
                })
                .collect())
        }

        fn to_thrift_scan(
            &self,
            scan: &crate::connector::scan_planning::ScanHandle,
            splits: &[crate::connector::scan_planning::Split],
            ctx: crate::connector::scan_planning::ThriftScanContext,
        ) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
            let planner =
                crate::connector::starrocks::table::StarRocksTableScanPlanner::stateless_for_codegen();
            <crate::connector::starrocks::table::StarRocksTableScanPlanner as crate::connector::scan_planning::ConnectorScanPlanner>::to_thrift_scan(
                &planner, scan, splits, ctx,
            )
        }
    }

    #[derive(Debug, Default)]
    struct ScanPlannerCallCounts {
        begin_scan: std::sync::atomic::AtomicUsize,
        plan_splits: std::sync::atomic::AtomicUsize,
        to_thrift_scan: std::sync::atomic::AtomicUsize,
        thrift_contexts: std::sync::Mutex<Vec<crate::connector::scan_planning::ThriftScanContext>>,
    }

    #[derive(Debug)]
    struct CountingScanPlanner {
        inner: MockScanPlanner,
        counts: std::sync::Arc<ScanPlannerCallCounts>,
    }

    impl crate::connector::scan_planning::ConnectorScanPlanner for CountingScanPlanner {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn begin_scan(
            &self,
            table: crate::connector::scan_planning::TableHandle,
            ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            self.counts
                .begin_scan
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.begin_scan(table, ctx)
        }

        fn plan_splits(
            &self,
            scan: &crate::connector::scan_planning::ScanHandle,
            ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            self.counts
                .plan_splits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.plan_splits(scan, ctx)
        }

        fn to_thrift_scan(
            &self,
            scan: &crate::connector::scan_planning::ScanHandle,
            splits: &[crate::connector::scan_planning::Split],
            ctx: crate::connector::scan_planning::ThriftScanContext,
        ) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
            self.counts
                .to_thrift_scan
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.counts
                .thrift_contexts
                .lock()
                .expect("thrift contexts")
                .push(ctx.clone());
            self.inner.to_thrift_scan(scan, splits, ctx)
        }
    }

    #[derive(Debug)]
    struct CountingIcebergScanPlanner {
        inner: crate::connector::iceberg::IcebergConnectorScanPlanner,
        counts: std::sync::Arc<ScanPlannerCallCounts>,
    }

    impl crate::connector::scan_planning::ConnectorScanPlanner for CountingIcebergScanPlanner {
        fn name(&self) -> &'static str {
            crate::connector::scan_planning::ConnectorScanPlanner::name(&self.inner)
        }

        fn begin_scan(
            &self,
            table: crate::connector::scan_planning::TableHandle,
            ctx: crate::connector::scan_planning::BeginScanContext,
        ) -> Result<crate::connector::scan_planning::ScanHandle, String> {
            self.counts
                .begin_scan
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.begin_scan(table, ctx)
        }

        fn plan_splits(
            &self,
            scan: &crate::connector::scan_planning::ScanHandle,
            ctx: crate::connector::scan_planning::SplitPlanningContext,
        ) -> Result<Vec<crate::connector::scan_planning::Split>, String> {
            self.counts
                .plan_splits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.plan_splits(scan, ctx)
        }

        fn to_thrift_scan(
            &self,
            scan: &crate::connector::scan_planning::ScanHandle,
            splits: &[crate::connector::scan_planning::Split],
            ctx: crate::connector::scan_planning::ThriftScanContext,
        ) -> Result<crate::connector::scan_planning::ThriftScanPlan, String> {
            self.counts
                .to_thrift_scan
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.counts
                .thrift_contexts
                .lock()
                .expect("thrift contexts")
                .push(ctx.clone());
            self.inner.to_thrift_scan(scan, splits, ctx)
        }
    }

    fn mock_starrocks_registry(
        layout: &crate::sql::catalog::PhysicalTableLayout,
    ) -> crate::connector::ConnectorRegistry {
        use crate::connector::starrocks::table::StarRocksSplit;
        let splits = layout
            .tablets
            .iter()
            .map(|tablet| StarRocksSplit {
                tablet_id: tablet.tablet_id,
                partition_id: tablet.partition_id,
                version: tablet.version,
            })
            .collect();
        let planner = std::sync::Arc::new(MockScanPlanner {
            schema_id: layout.schema_id,
            splits,
        });
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(planner);
        registry
    }

    fn mock_iceberg_registry() -> crate::connector::ConnectorRegistry {
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(std::sync::Arc::new(
            crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
        ));
        registry
    }

    fn mock_starrocks_and_iceberg_registry(
        layout: &crate::sql::catalog::PhysicalTableLayout,
    ) -> crate::connector::ConnectorRegistry {
        let mut registry = mock_starrocks_registry(layout);
        registry.register_scan_planner(std::sync::Arc::new(
            crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
        ));
        registry
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            column_id: crate::sql::column_id::ColumnId::UNSET,
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }]
    }

    fn id_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: crate::sql::column_id::ColumnId::UNSET,
                qualifier: None,
                column: "id".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn id_expr_with_column_id(column_id: crate::sql::column_id::ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: "id".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn id_eq_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(id_expr()),
                op: BinOp::Eq,
                right: Box::new(TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(value)),
                    data_type: DataType::Int32,
                    nullable: false,
                }),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn with_id_predicate(mut plan: PhysicalPlanNode, value: i64) -> PhysicalPlanNode {
        let Operator::PhysicalScan(scan) = &mut plan.op else {
            panic!("expected scan plan");
        };
        scan.predicates = vec![id_eq_literal(value)];
        plan
    }

    fn iceberg_i32_file(path: &str, min: i32, max: i32) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: Some(HashMap::from([(
                "id".to_string(),
                IcebergColumnStats {
                    null_count: Some(0),
                    value_count: None,
                    column_size: None,
                    lower_bound: Some(min.to_le_bytes().to_vec()),
                    upper_bound: Some(max.to_le_bytes().to_vec()),
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            delete_files: vec![],
            manifest_path: None,
            partition_values: vec![],
        }
    }

    fn iceberg_i32_partition_file(path: &str, id: i32) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: Some(format!("Struct([{id}])")),
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            delete_files: vec![],
            manifest_path: Some(format!("manifest-{id}.avro")),
            partition_values: vec![IcebergPartitionFieldValue {
                source_column: "id".to_string(),
                field_name: "id".to_string(),
                transform: "identity".to_string(),
                value: Some(IcebergPartitionValue::Int32(id)),
            }],
        }
    }

    fn iceberg_delete_file(path: &str, length: i64) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: path.to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Position,
            length: Some(length),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: None,
            equality_column_names: vec![],
            equality_field_ids: vec![],
        }
    }

    fn stats() -> Statistics {
        Statistics {
            output_row_count: 3.0,
            column_statistics: HashMap::new(),
        }
    }

    fn table_with_delete_files(
        delete_files: Vec<IcebergDeleteFileInfo>,
        iceberg_schema_fields: Vec<IcebergSchemaFieldDef>,
    ) -> TableDef {
        TableDef {
            name: "ice_t".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "category".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: test_iceberg_table_info_with_schema(iceberg_schema_fields),
                files: vec![crate::sql::catalog::IcebergDataFileInfo {
                    path: "s3://bucket/data.parquet".to_string(),
                    size: 1,
                    row_count: Some(1),
                    column_stats: None,
                    partition_spec_id: Some(0),
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: Some(1),
                    ivm_change_op: None,
                    delete_files,
                    manifest_path: None,
                    partition_values: vec![],
                }],
                cloud_properties: BTreeMap::new(),
            },
        }
    }

    fn equality_delete_file(
        equality_column_names: Vec<String>,
        equality_field_ids: Vec<i32>,
    ) -> IcebergDeleteFileInfo {
        IcebergDeleteFileInfo {
            path: "s3://bucket/eq-delete.parquet".to_string(),
            file_format: IcebergDeleteFileFormat::Parquet,
            file_content: IcebergDeleteFileContent::Equality,
            length: Some(1),
            content_offset: None,
            content_size_in_bytes: None,
            sequence_number: Some(2),
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            equality_column_names,
            equality_field_ids,
        }
    }

    #[test]
    fn equality_delete_field_ids_are_resolved_to_required_scan_columns() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(Vec::new(), vec![3])],
            vec![
                IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    children: vec![],
                },
                IcebergSchemaFieldDef {
                    field_id: 3,
                    name: "category".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    children: vec![],
                },
            ],
        );

        add_iceberg_equality_delete_required_columns(&mut required, &table).expect("resolve ids");

        assert!(required.contains("id"));
        assert!(required.contains("category"));
    }

    #[test]
    fn equality_delete_column_names_are_legacy_fallback_for_required_scan_columns() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(
                vec!["category".to_string()],
                Vec::new(),
            )],
            vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                children: vec![],
            }],
        );

        add_iceberg_equality_delete_required_columns(&mut required, &table).expect("legacy names");

        assert!(required.contains("id"));
        assert!(required.contains("category"));
    }

    #[test]
    fn equality_delete_unknown_field_id_is_planning_error() {
        let mut required = std::collections::HashSet::from(["id".to_string()]);
        let table = table_with_delete_files(
            vec![equality_delete_file(Vec::new(), vec![99])],
            vec![IcebergSchemaFieldDef {
                field_id: 1,
                name: "id".to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                children: vec![],
            }],
        );

        let err = add_iceberg_equality_delete_required_columns(&mut required, &table)
            .expect_err("unknown field id");

        assert!(err.contains("unknown field id 99"), "{err}");
    }

    fn scan_plan(path: PathBuf) -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info(),
                        files: vec![crate::sql::catalog::IcebergDataFileInfo {
                            path: path.display().to_string(),
                            size: 0,
                            row_count: None,
                            column_stats: None,
                            partition_spec_id: None,
                            partition_key: None,
                            first_row_id: None,
                            data_sequence_number: None,
                            ivm_change_op: None,
                            delete_files: Vec::new(),
                            manifest_path: None,
                            partition_values: Vec::new(),
                        }],
                        cloud_properties: Default::default(),
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn starrocks_scan_plan() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "starrocks_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 11,
                        table_id: 22,
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn iceberg_scan_plan() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn iceberg_scan_plan_with_file_stats() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![
                            iceberg_i32_file("s3://bucket/file-1-5.parquet", 1, 5),
                            iceberg_i32_file("s3://bucket/file-10-20.parquet", 10, 20),
                        ],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![id_eq_literal(12)],
                required_columns: None,
                dict_columns: vec![],
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn iceberg_scan_plan_with_partition_values() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![
                            iceberg_i32_partition_file("s3://bucket/id-1.parquet", 1),
                            iceberg_i32_partition_file("s3://bucket/id-12.parquet", 12),
                        ],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![id_eq_literal(12)],
                required_columns: None,
                dict_columns: vec![],
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn iceberg_scan_plan_with_large_file(size: i64) -> PhysicalPlanNode {
        let mut file = iceberg_i32_file("s3://bucket/large.parquet", 1, 100);
        file.size = size;
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![file],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    fn iceberg_scan_plan_with_many_delete_files(delete_count: usize) -> PhysicalPlanNode {
        let mut file = iceberg_i32_file("s3://bucket/delete-heavy.parquet", 1, 100);
        file.delete_files = (0..delete_count)
            .map(|idx| iceberg_delete_file(&format!("s3://bucket/delete-{idx}.parquet"), 1))
            .collect();
        PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int32,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: test_iceberg_table_info_with_id_schema(),
                        files: vec![file],
                        cloud_properties: BTreeMap::new(),
                    },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    #[test]
    fn iceberg_scan_predicates_feed_min_max_and_file_stats_pruning() {
        let plan = iceberg_scan_plan_with_file_stats();

        let build =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default")
                .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let hdfs = scan.hdfs_scan_node.as_ref().expect("hdfs scan payload");

        assert_eq!(
            hdfs.min_max_conjuncts.as_ref().map(Vec::len),
            Some(1),
            "standalone scan predicates should be available to HDFS min/max pruning"
        );
        assert_eq!(hdfs.min_max_tuple_id, hdfs.tuple_id);

        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");
        assert_eq!(
            ranges.len(),
            1,
            "file-level Iceberg stats should prune the file whose id range cannot contain 12"
        );
        let kept_path = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .and_then(|range| range.full_path.as_deref());
        assert_eq!(kept_path, Some("s3://bucket/file-10-20.parquet"));
    }

    #[test]
    fn iceberg_identity_partition_values_prune_scan_ranges() {
        let plan = iceberg_scan_plan_with_partition_values();

        let build =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default")
                .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");

        assert_eq!(
            ranges.len(),
            1,
            "identity partition values should prune files before scan range planning"
        );
        let kept_path = ranges[0]
            .scan_range
            .hdfs_scan_range
            .as_ref()
            .and_then(|range| range.full_path.as_deref());
        assert_eq!(kept_path, Some("s3://bucket/id-12.parquet"));
    }

    #[test]
    fn iceberg_large_plain_files_are_split_into_parallel_scan_ranges() {
        let plan = iceberg_scan_plan_with_large_file(300 * 1024 * 1024);

        let build =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default")
                .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&scan.node_id)
            .expect("scan ranges");

        assert_eq!(ranges.len(), 3);
        let first = ranges[0].scan_range.hdfs_scan_range.as_ref().unwrap();
        let second = ranges[1].scan_range.hdfs_scan_range.as_ref().unwrap();
        let third = ranges[2].scan_range.hdfs_scan_range.as_ref().unwrap();
        assert_eq!(first.offset, Some(0));
        assert_eq!(first.length, Some(128 * 1024 * 1024));
        assert_eq!(first.file_length, Some(300 * 1024 * 1024));
        assert_eq!(second.offset, Some(128 * 1024 * 1024));
        assert_eq!(second.length, Some(128 * 1024 * 1024));
        assert_eq!(third.offset, Some(256 * 1024 * 1024));
        assert_eq!(third.length, Some(44 * 1024 * 1024));
    }

    #[test]
    fn iceberg_delete_apply_cost_rejects_too_many_delete_files() {
        let plan = iceberg_scan_plan_with_many_delete_files(1025);

        let err = match PlanFragmentBuilder::build(
            &plan,
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        ) {
            Ok(_) => panic!("delete-heavy scan should fail fast"),
            Err(err) => err,
        };

        assert!(
            err.contains("too many Iceberg delete files"),
            "unexpected error: {err}"
        );
    }

    fn mixed_starrocks_iceberg_join_plan() -> PhysicalPlanNode {
        PhysicalPlanNode {
            op: Operator::PhysicalHashJoin(PhysicalHashJoinOp {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: crate::sql::column_id::ColumnId::UNSET,
                            qualifier: Some("ice_t".to_string()),
                            column: "id".to_string(),
                        },
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    right: TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: crate::sql::column_id::ColumnId::UNSET,
                            qualifier: Some("starrocks_t".to_string()),
                            column: "id".to_string(),
                        },
                        data_type: DataType::Int32,
                        nullable: false,
                    },
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Colocate,
            }),
            children: vec![iceberg_scan_plan(), starrocks_scan_plan()],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        }
    }

    #[test]
    fn build_splits_gather_distribution_into_stream_edge() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalSort(PhysicalSortOp {
                items: vec![SortItem {
                    expr: id_expr(),
                    asc: true,
                    nulls_first: false,
                }],
                analytic_partition_exprs: Vec::new(),
            }),
            children: vec![PhysicalPlanNode {
                op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                    spec: DistributionSpec::Gather,
                }),
                children: vec![scan_plan(file.path().to_path_buf())],
                stats: stats(),
                output_columns: output_columns(),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let build =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default")
                .expect("build");

        assert_eq!(build.fragment_results.len(), 2);
        assert_eq!(build.edges.len(), 1);
        assert!(matches!(
            build.edges[0].edge_kind,
            crate::sql::codegen::FragmentEdgeKind::Stream
        ));

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        assert!(
            root.plan
                .nodes
                .iter()
                .any(|node| { node.node_type == plan_nodes::TPlanNodeType::EXCHANGE_NODE })
        );
    }

    #[test]
    fn multi_group_window_reuses_child_ordering_without_redundant_sorts() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let id = id_expr_with_column_id(crate::sql::column_id::ColumnId(1));
        let order_by = vec![SortItem {
            expr: id.clone(),
            asc: true,
            nulls_first: true,
        }];
        let win_rows = WindowExpr {
            name: "sum".to_string(),
            args: vec![id.clone()],
            distinct: false,
            partition_by: vec![],
            order_by: order_by.clone(),
            window_frame: Some(WindowFrame {
                frame_type: WindowFrameType::Rows,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
            }),
            result_type: DataType::Int64,
            output_name: "sum_rows".to_string(),
            ignore_nulls: false,
        };
        let win_range = WindowExpr {
            window_frame: Some(WindowFrame {
                frame_type: WindowFrameType::Range,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
            }),
            output_name: "sum_range".to_string(),
            ..win_rows.clone()
        };
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalWindow(PhysicalWindowOp {
                window_exprs: vec![win_rows, win_range],
                output_columns: vec![],
            }),
            children: vec![PhysicalPlanNode {
                op: Operator::PhysicalSort(PhysicalSortOp {
                    items: order_by,
                    analytic_partition_exprs: Vec::new(),
                }),
                children: vec![scan_plan(file.path().to_path_buf())],
                stats: stats(),
                output_columns: output_columns(),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let build =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default")
                .expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let sort_count = root
            .plan
            .nodes
            .iter()
            .filter(|node| node.node_type == plan_nodes::TPlanNodeType::SORT_NODE)
            .count();

        assert_eq!(
            sort_count, 1,
            "child ordering already satisfies both window groups"
        );
    }

    #[test]
    fn build_nested_gather_distribution_targets_immediate_parent_fragment() {
        // Wrap the nested gathers inside a Sort so the root is NOT a Gather
        // (root-level Gather is elided).
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalSort(PhysicalSortOp {
                items: vec![SortItem {
                    expr: id_expr(),
                    asc: true,
                    nulls_first: false,
                }],
                analytic_partition_exprs: Vec::new(),
            }),
            children: vec![PhysicalPlanNode {
                op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                    spec: DistributionSpec::Gather,
                }),
                children: vec![PhysicalPlanNode {
                    op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                        spec: DistributionSpec::Gather,
                    }),
                    children: vec![scan_plan(file.path().to_path_buf())],
                    stats: stats(),
                    output_columns: output_columns(),
                    build_runtime_filters: Vec::new(),
                    probe_runtime_filters: Vec::new(),
                }],
                stats: stats(),
                output_columns: output_columns(),
                build_runtime_filters: Vec::new(),
                probe_runtime_filters: Vec::new(),
            }],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let build =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default")
                .expect("build");
        assert_eq!(build.fragment_results.len(), 3);
        assert_eq!(build.edges.len(), 2);

        // The inner gather targets its immediate parent (the outer gather fragment),
        // not the root fragment directly.
        let outer_gather_frag_id = build
            .edges
            .iter()
            .find(|e| e.target_fragment_id == build.root_fragment_id)
            .expect("edge to root")
            .source_fragment_id;
        assert!(build.edges.iter().any(|e| {
            e.target_fragment_id == outer_gather_frag_id
                && e.source_fragment_id != outer_gather_frag_id
                && matches!(e.edge_kind, crate::sql::codegen::FragmentEdgeKind::Stream)
        }));
    }

    #[test]
    fn build_maps_hash_distribution_to_hash_partitioned_edge() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let hash_col = crate::sql::column_id::ColumnId(1);
        let mut scan = scan_plan(file.path().to_path_buf());
        scan.output_columns[0].column_id = hash_col;
        let Operator::PhysicalScan(scan_op) = &mut scan.op else {
            panic!("expected scan child");
        };
        scan_op.columns[0].column_id = hash_col;
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::shuffle_agg([hash_col]),
            }),
            children: vec![scan],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let build =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default")
                .expect("build");
        let edge = build.edges.first().expect("stream edge");
        assert_eq!(
            edge.output_partition.type_,
            crate::partitions::TPartitionType::HASH_PARTITIONED
        );
        assert_eq!(
            edge.output_partition
                .partition_exprs
                .as_ref()
                .map(|v| v.len()),
            Some(1)
        );
    }

    #[test]
    fn build_rejects_any_distribution_in_fragment_builder() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Any,
            }),
            children: vec![scan_plan(file.path().to_path_buf())],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let result =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default");
        let err = result.err().expect("distribution any must fail");
        assert!(err.contains("PhysicalDistribution(Any)"));
    }

    #[test]
    fn build_elides_root_gather_distribution() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            }),
            children: vec![scan_plan(file.path().to_path_buf())],
            stats: stats(),
            output_columns: output_columns(),
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let build =
            PlanFragmentBuilder::build(&plan, &DummyCatalog, &mock_iceberg_registry(), "default")
                .expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        assert!(build.edges.is_empty());
    }

    #[test]
    fn build_generate_series_emits_table_function_without_scan_source() {
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalGenerateSeries(PhysicalGenerateSeriesOp {
                start: 1,
                end: 3_000_000,
                step: 1,
                column_name: "generate_series".to_string(),
                alias: Some("gs".to_string()),
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 3_000_000.0,
                column_statistics: HashMap::new(),
            },
            output_columns: vec![OutputColumn {
                column_id: crate::sql::column_id::ColumnId::UNSET,
                name: "generate_series".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                is_internal: false,
            }],
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let build = PlanFragmentBuilder::build(
            &plan,
            &DummyCatalog,
            &crate::connector::ConnectorRegistry::new(),
            "default",
        )
        .expect("build");
        let root = build.fragment_results.first().expect("root fragment");
        assert!(root.exec_params.per_node_scan_ranges.is_empty());
        assert!(
            root.plan.nodes.iter().all(|node| {
                !matches!(
                    node.node_type,
                    plan_nodes::TPlanNodeType::HDFS_SCAN_NODE
                        | plan_nodes::TPlanNodeType::LAKE_SCAN_NODE
                )
            }),
            "generate_series must not be emitted as a scan: {:?}",
            root.plan.nodes
        );
        let table_function = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::TABLE_FUNCTION_NODE)
            .and_then(|node| node.table_function_node.as_ref())
            .expect("table function node");
        assert_eq!(
            table_function.param_columns.as_ref().expect("params").len(),
            3
        );
        assert!(
            table_function
                .outer_columns
                .as_ref()
                .expect("outer columns")
                .is_empty()
        );
        assert_eq!(
            table_function
                .fn_result_columns
                .as_ref()
                .expect("result columns")
                .len(),
            1
        );
        let union = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::UNION_NODE)
            .and_then(|node| node.union_node.as_ref())
            .expect("parameter values node");
        assert_eq!(union.const_expr_lists.len(), 1);
        assert_eq!(union.const_expr_lists[0].len(), 3);
    }

    #[test]
    fn build_starrocks_scan_emits_lake_scan_with_internal_ranges() {
        let layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let plan = starrocks_scan_plan();
        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };

        let build =
            PlanFragmentBuilder::build(&plan, &catalog, &registry, "default").expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        let root = build.fragment_results.first().expect("root fragment");
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        let lake = scan_node
            .lake_scan_node
            .as_ref()
            .expect("lake scan payload");
        let schema_key = lake.schema_key.as_ref().expect("schema_key");
        assert_eq!(schema_key.db_id, Some(11));
        assert_eq!(schema_key.table_id, Some(22));
        assert_eq!(schema_key.schema_id, Some(33));

        let tuple_desc = root
            .desc_tbl
            .tuple_descriptors
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .expect("StarRocks scan tuple descriptor");
        assert_eq!(tuple_desc.table_id, Some(22));

        let table_descs = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors");
        let table_desc = table_descs
            .iter()
            .find(|table| table.id == 22)
            .expect("StarRocks table descriptor");
        assert_eq!(table_desc.db_name, "default");
        assert_eq!(table_desc.table_name, "starrocks_t");

        let ranges = root
            .exec_params
            .per_node_scan_ranges
            .get(&1)
            .expect("scan ranges");
        assert_eq!(ranges.len(), 1);
        let internal = ranges[0]
            .scan_range
            .internal_scan_range
            .as_ref()
            .expect("internal scan range");
        assert_eq!(internal.tablet_id, 101);
        assert_eq!(internal.partition_id, Some(201));
        assert_eq!(internal.version, "7");
        assert_eq!(internal.db_name, "default");
        assert_eq!(internal.table_name.as_deref(), Some("starrocks_t"));
    }

    #[test]
    fn iceberg_scan_without_starrocks_layout_uses_synthetic_descriptor_table_id() {
        let build = PlanFragmentBuilder::build(
            &iceberg_scan_plan(),
            &DummyCatalog,
            &mock_iceberg_registry(),
            "default",
        )
        .expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        let root = build.fragment_results.first().expect("root fragment");
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let synthetic_table_id = synthetic_iceberg_table_id(scan_node.node_id);
        let tuple_desc = root
            .desc_tbl
            .tuple_descriptors
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .expect("scan tuple descriptor");
        assert_eq!(tuple_desc.table_id, Some(synthetic_table_id));

        let table_desc = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors")
            .iter()
            .find(|table| table.id == synthetic_table_id)
            .expect("synthetic iceberg table descriptor");
        assert_eq!(
            table_desc.table_type,
            crate::types::TTableType::ICEBERG_TABLE
        );
        assert_eq!(
            table_desc
                .iceberg_table
                .as_ref()
                .and_then(|table| table.iceberg_schema.as_ref())
                .and_then(|schema| schema.fields.as_ref())
                .and_then(|fields| fields.first())
                .and_then(|field| field.field_id),
            Some(1)
        );
    }

    #[test]
    fn mixed_starrocks_and_iceberg_scan_table_ids_do_not_collide() {
        let starrocks_layout = PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        };
        let registry = mock_starrocks_and_iceberg_registry(&starrocks_layout);
        let catalog = MixedCatalog { starrocks_layout };

        let build = PlanFragmentBuilder::build(
            &mixed_starrocks_iceberg_join_plan(),
            &catalog,
            &registry,
            "default",
        )
        .expect("build");
        let root = build.fragment_results.first().expect("root fragment");
        let tuple_descs = &root.desc_tbl.tuple_descriptors;
        let iceberg_table_id = tuple_descs
            .iter()
            .find(|tuple| tuple.id == Some(1))
            .and_then(|tuple| tuple.table_id)
            .expect("iceberg tuple table id");
        let starrocks_table_id = tuple_descs
            .iter()
            .find(|tuple| tuple.id == Some(2))
            .and_then(|tuple| tuple.table_id)
            .expect("StarRocks tuple table id");
        assert_ne!(iceberg_table_id, starrocks_table_id);
        assert_eq!(starrocks_table_id, 22);

        let table_descs = root
            .desc_tbl
            .table_descriptors
            .as_ref()
            .expect("table descriptors");
        let iceberg_desc = table_descs
            .iter()
            .find(|table| table.id == iceberg_table_id)
            .expect("iceberg table descriptor");
        assert_eq!(
            iceberg_desc.table_type,
            crate::types::TTableType::ICEBERG_TABLE
        );
        let starrocks_desc = table_descs
            .iter()
            .find(|table| table.id == starrocks_table_id)
            .expect("StarRocks table descriptor");
        assert_eq!(
            starrocks_desc.table_type,
            crate::types::TTableType::OLAP_TABLE
        );
    }

    // -------------------------------------------------------------------
    // Task 6: codegen dictionary plan interface
    // -------------------------------------------------------------------

    fn dict_snapshot_a_b() -> std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot> {
        use crate::engine::dictionary::model::{
            DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue,
            DictionaryWatermark,
        };
        std::sync::Arc::new(DictionarySnapshot {
            dictionary_id: 1,
            owner: DictionaryOwner::StarRocksTable {
                database: "default".to_string(),
                table: "starrocks_t".to_string(),
                db_id: 11,
                table_id: 22,
            },
            column_id: None,
            column_name: "id".to_string(),
            data_type: DataType::Int32,
            version: 1,
            watermark: DictionaryWatermark::StarRocks {
                schema_id: 33,
                tablets: vec![],
            },
            values: vec![
                DictionaryValue {
                    id: 1,
                    bytes: b"a".to_vec(),
                },
                DictionaryValue {
                    id: 2,
                    bytes: b"b".to_vec(),
                },
            ],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        })
    }

    fn dict_snapshot_x_y_z() -> std::sync::Arc<crate::engine::dictionary::model::DictionarySnapshot>
    {
        use crate::engine::dictionary::model::{
            DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue,
            DictionaryWatermark,
        };
        std::sync::Arc::new(DictionarySnapshot {
            dictionary_id: 2,
            owner: DictionaryOwner::StarRocksTable {
                database: "default".to_string(),
                table: "starrocks_t".to_string(),
                db_id: 11,
                table_id: 22,
            },
            column_id: None,
            column_name: "name".to_string(),
            data_type: DataType::Int32,
            version: 3,
            watermark: DictionaryWatermark::StarRocks {
                schema_id: 33,
                tablets: vec![],
            },
            values: vec![
                DictionaryValue {
                    id: 10,
                    bytes: b"x".to_vec(),
                },
                DictionaryValue {
                    id: 11,
                    bytes: b"y".to_vec(),
                },
                DictionaryValue {
                    id: 12,
                    bytes: b"z".to_vec(),
                },
            ],
            null_id: 0,
            state: DictionaryState::Active,
            order_preserving: true,
        })
    }

    /// Look up the slot id of a slot by its column name in `desc_tbl`.
    /// Panics if no such slot exists — the caller is asserting that the
    /// builder produced a slot with the expected name.
    fn slot_id_by_name(desc_tbl: &crate::descriptors::TDescriptorTable, column_name: &str) -> i32 {
        slot_id_by_name_opt(desc_tbl, column_name)
            .unwrap_or_else(|| panic!("no slot named `{}` in desc_tbl", column_name))
    }

    /// Optional variant for tests that need to assert ABSENCE of a slot
    /// (e.g. Bug B regression: a dict-rewritten scan must NOT emit a
    /// separate source-string slot alongside its dict slot).
    fn slot_id_by_name_opt(
        desc_tbl: &crate::descriptors::TDescriptorTable,
        column_name: &str,
    ) -> Option<i32> {
        let slots = desc_tbl.slot_descriptors.as_ref()?;
        for slot in slots {
            if slot.col_name.as_deref() == Some(column_name) {
                return slot.id;
            }
        }
        None
    }

    fn starrocks_layout() -> PhysicalTableLayout {
        PhysicalTableLayout {
            db_id: 11,
            table_id: 22,
            schema_id: 33,
            tablets: vec![StarRocksTabletRef {
                tablet_id: 101,
                partition_id: 201,
                version: 7,
            }],
        }
    }

    #[test]
    fn physical_decode_emits_decode_node() {
        use crate::sql::optimizer::operator::PhysicalDecodeOp;
        use crate::sql::planner::plan::DecodeMapping;

        // Build a StarRocks scan that exposes one dict column ("id" string
        // column gets a sibling "id_dict" INT slot via dict_columns).
        let layout = starrocks_layout();
        let scan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "starrocks_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 11,
                        table_id: 22,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: crate::sql::column_id::ColumnId::UNSET,
                    name: "id".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![ScanDictionaryColumn {
                    source_column: "id".to_string(),
                    dict_column: "id_dict".to_string(),
                    dictionary: dict_snapshot_a_b(),
                }],
            }),
            children: vec![],
            stats: stats(),
            output_columns: vec![OutputColumn {
                column_id: crate::sql::column_id::ColumnId::UNSET,
                name: "id".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let decode_plan = PhysicalPlanNode {
            op: Operator::PhysicalDecode(PhysicalDecodeOp {
                mappings: vec![DecodeMapping {
                    dict_column: "id_dict".to_string(),
                    string_column: "id".to_string(),
                }],
                output_columns: vec![OutputColumn {
                    column_id: crate::sql::column_id::ColumnId::UNSET,
                    name: "id".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            children: vec![scan],
            stats: stats(),
            output_columns: vec![OutputColumn {
                column_id: crate::sql::column_id::ColumnId::UNSET,
                name: "id".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };
        let build = PlanFragmentBuilder::build(&decode_plan, &catalog, &registry, "default")
            .expect("build");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");

        // The decode allocates a NEW tuple with a NEW Utf8 slot named
        // `id` (the decoded output). Under the Bug B fix the scan tuple
        // also holds a slot named `id` (single-slot-per-column contract)
        // but typed Int32 — the dict-encoded payload. The
        // `dict_id_to_string_ids` mapping pairs the scan's `id` slot id
        // with the decode tuple's new string slot id.
        let desc_tbl = &root.desc_tbl;
        let slots = desc_tbl
            .slot_descriptors
            .as_ref()
            .expect("slot_descriptors");
        let tuples = &desc_tbl.tuple_descriptors;
        assert_eq!(tuples.len(), 2, "expected scan tuple + decode tuple");
        let scan_tuple_id = tuples[0].id.expect("scan tuple id");
        let decode_tuple_id = tuples[1].id.expect("decode tuple id");
        let scan_dict_slot = slots
            .iter()
            .find(|s| s.parent == Some(scan_tuple_id) && s.col_name.as_deref() == Some("id"))
            .and_then(|s| s.id)
            .expect("scan dict slot named after source `id`");
        let decode_string_slot = slots
            .iter()
            .find(|s| s.parent == Some(decode_tuple_id) && s.col_name.as_deref() == Some("id"))
            .and_then(|s| s.id)
            .expect("decode id slot");
        assert_ne!(
            scan_dict_slot, decode_string_slot,
            "scan dict slot (Int32) and decode string slot (Utf8) must be distinct"
        );

        // First plan node is the decode node (pre-order).
        let first = root.plan.nodes.first().expect("decode plan node");
        assert_eq!(first.node_type, plan_nodes::TPlanNodeType::DECODE_NODE);
        assert_eq!(
            first.row_tuples,
            vec![decode_tuple_id],
            "decode row_tuples must reference the new decode tuple"
        );
        let decode = first.decode_node.as_ref().expect("decode payload");
        let mapping = decode
            .dict_id_to_string_ids
            .as_ref()
            .expect("dict_id_to_string_ids");
        assert_eq!(mapping.len(), 1);
        let (dict_slot, string_slot) = mapping.iter().next().expect("one entry");
        assert_eq!(
            *dict_slot, scan_dict_slot,
            "decode mapping key must be the scan's dict slot id"
        );
        assert_eq!(
            *string_slot, decode_string_slot,
            "decode mapping value must be the decode tuple's new string slot id"
        );
    }

    #[test]
    fn scan_dict_column_emits_query_global_dict() {
        let layout = starrocks_layout();
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "starrocks_t".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: DataType::Utf8,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "name".to_string(),
                            data_type: DataType::Utf8,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 11,
                        table_id: 22,
                    },
                },
                alias: None,
                columns: vec![
                    OutputColumn {
                        column_id: crate::sql::column_id::ColumnId::UNSET,
                        name: "id".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: crate::sql::column_id::ColumnId::UNSET,
                        name: "name".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        is_internal: false,
                    },
                ],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![
                    ScanDictionaryColumn {
                        source_column: "id".to_string(),
                        dict_column: "id_dict".to_string(),
                        dictionary: dict_snapshot_a_b(),
                    },
                    ScanDictionaryColumn {
                        source_column: "name".to_string(),
                        dict_column: "name_dict".to_string(),
                        dictionary: dict_snapshot_x_y_z(),
                    },
                ],
            }),
            children: vec![],
            stats: stats(),
            output_columns: vec![
                OutputColumn {
                    column_id: crate::sql::column_id::ColumnId::UNSET,
                    name: "id".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                },
                OutputColumn {
                    column_id: crate::sql::column_id::ColumnId::UNSET,
                    name: "name".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                },
            ],
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };
        let build =
            PlanFragmentBuilder::build(&plan, &catalog, &registry, "default").expect("build");

        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");

        // Bug B regression: the scan must emit exactly ONE slot per dict
        // column. The slot keeps the SOURCE column's name (so the lake
        // scan finds the storage column by name) but its declared type
        // is Int32 (the BE encodes string -> dict id at read time using
        // the per-slot TGlobalDict). Emitting BOTH a source string slot
        // AND a separate dict int slot would let the BE's
        // `lake_scan.rs::dict_int_to_string` swap collapse them onto the
        // same storage slot id, producing `duplicate slot id <N> in
        // chunk schema contract` at runtime.
        let id_slot = slot_id_by_name(&root.desc_tbl, "id");
        let name_slot = slot_id_by_name(&root.desc_tbl, "name");
        assert_ne!(id_slot, name_slot, "scan slots must be distinct");
        // The dict_column NAMES (`id_dict`, `name_dict`) must NOT appear
        // as slot descriptor `col_name`s. The dict_column lives only in
        // the FE codegen scope as an alias for the source slot — the BE
        // never sees a column named `id_dict` in the tablet schema.
        assert!(
            slot_id_by_name_opt(&root.desc_tbl, "id_dict").is_none(),
            "dict_column name must not surface as a slot descriptor col_name"
        );
        assert!(
            slot_id_by_name_opt(&root.desc_tbl, "name_dict").is_none(),
            "dict_column name must not surface as a slot descriptor col_name"
        );
        // The dict slot type is Int32 (so the BE knows to encode).
        let id_slot_desc = root
            .desc_tbl
            .slot_descriptors
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .find(|s| s.id == Some(id_slot))
            .expect("id slot desc");
        assert_eq!(
            id_slot_desc
                .slot_type
                .as_ref()
                .and_then(|t| t.types.as_ref())
                .and_then(|tys| tys.first())
                .and_then(|tn| tn.scalar_type.as_ref())
                .map(|st| st.type_),
            Some(crate::types::TPrimitiveType::INT),
            "dict slot type must be INT (Int32) — see build_scan_schema_for_global_dict_encoding"
        );
        // The tuple itself should contain exactly the two dict slots.
        let scan_tuple_id = root
            .desc_tbl
            .tuple_descriptors
            .first()
            .and_then(|t| t.id)
            .expect("scan tuple id");
        let scan_slots: Vec<i32> = root
            .desc_tbl
            .slot_descriptors
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|s| s.parent == Some(scan_tuple_id))
            .filter_map(|s| s.id)
            .collect();
        assert_eq!(
            scan_slots.len(),
            2,
            "scan tuple must contain exactly two slots (one per dict column), got {scan_slots:?}"
        );

        // The fragment should carry two TGlobalDicts, one per source column.
        let dicts = root
            .query_global_dicts
            .as_ref()
            .expect("query_global_dicts populated");
        assert!(
            dicts.len() >= 2,
            "at least one TGlobalDict per source column; got {}",
            dicts.len()
        );

        // Match each TGlobalDict back to its slot id and check payload.
        let id_dict = dicts
            .iter()
            .find(|d| d.column_id == Some(id_slot))
            .expect("TGlobalDict for id slot");
        assert_eq!(id_dict.ids.as_deref(), Some(&[1, 2][..]));
        assert_eq!(
            id_dict.strings.as_deref(),
            Some(&[b"a".to_vec(), b"b".to_vec()][..])
        );
        let name_dict = dicts
            .iter()
            .find(|d| d.column_id == Some(name_slot))
            .expect("TGlobalDict for name slot");
        assert_eq!(name_dict.ids.as_deref(), Some(&[10, 11, 12][..]));
        assert_eq!(
            name_dict.strings.as_deref(),
            Some(&[b"x".to_vec(), b"y".to_vec(), b"z".to_vec()][..])
        );
        // Distinct column_ids on the two TGlobalDicts.
        assert_ne!(id_dict.column_id, name_dict.column_id);

        // StarRocks scan's TLakeScanNode carries the
        // `dict_string_id_to_int_ids` map. Under the Bug B fix this is a
        // SELF-map (slot -> same slot): the BE's layout rewrite at
        // `src/lower/node/lake_scan.rs` replaces every dict int slot
        // with its mapped string slot, and with the dict slot now
        // occupying the storage column's tuple position the self-map
        // keeps the layout swap a no-op — which is exactly what avoids
        // the duplicate-slot-id error.
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        let lake = scan_node
            .lake_scan_node
            .as_ref()
            .expect("lake scan payload");
        let mapping = lake
            .dict_string_id_to_int_ids
            .as_ref()
            .expect("dict_string_id_to_int_ids populated");
        assert_eq!(mapping.len(), 2);
        assert_eq!(
            mapping.get(&id_slot).copied(),
            Some(id_slot),
            "id slot must self-map"
        );
        assert_eq!(
            mapping.get(&name_slot).copied(),
            Some(name_slot),
            "name slot must self-map"
        );
    }

    #[test]
    fn scan_emits_single_slot_per_dict_column() {
        // Direct Bug B regression: build a single-column StarRocks scan
        // where the rewriter has produced a `ScanDictionaryColumn` for
        // `s` and renamed the OutputColumn to `__nr_dict_t_s` (Int32).
        // Mirrors the post-rewriter shape that the FE actually emits
        // after `rewrite_scan`. The scan tuple must contain exactly one
        // slot: a single Int32 slot named after the SOURCE column `s`
        // (so the BE lake scan finds the storage column by name) with
        // the dict_column name (`__nr_dict_t_s`) registered as a scope
        // alias for the same slot. The LakeScanNode's
        // `dict_string_id_to_int_ids` must self-map that slot id, so
        // the BE layout swap is a no-op rather than collapsing two
        // distinct slots onto one storage slot id.
        let layout = starrocks_layout();
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "t".to_string(),
                    columns: vec![ColumnDef {
                        name: "s".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks {
                        db_id: 11,
                        table_id: 22,
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: crate::sql::column_id::ColumnId::UNSET,
                    name: "__nr_dict_t_s".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: Some(vec!["__nr_dict_t_s".to_string()]),
                dict_columns: vec![ScanDictionaryColumn {
                    source_column: "s".to_string(),
                    dict_column: "__nr_dict_t_s".to_string(),
                    dictionary: dict_snapshot_a_b(),
                }],
            }),
            children: vec![],
            stats: stats(),
            output_columns: vec![OutputColumn {
                column_id: crate::sql::column_id::ColumnId::UNSET,
                name: "__nr_dict_t_s".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            }],
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };
        let build =
            PlanFragmentBuilder::build(&plan, &catalog, &registry, "default").expect("build");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");

        // The scan tuple must contain exactly one slot: the dict slot.
        let scan_tuple_id = root
            .desc_tbl
            .tuple_descriptors
            .first()
            .and_then(|t| t.id)
            .expect("scan tuple id");
        let scan_slots: Vec<&crate::descriptors::TSlotDescriptor> = root
            .desc_tbl
            .slot_descriptors
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter(|s| s.parent == Some(scan_tuple_id))
            .collect();
        assert_eq!(
            scan_slots.len(),
            1,
            "scan tuple must contain exactly the dict slot, got {} slots",
            scan_slots.len()
        );
        let dict_slot = &scan_slots[0];
        // The slot keeps the SOURCE column's name `s` so the BE finds
        // the storage column in the tablet schema by name. The dict
        // column name lives only in the FE codegen scope.
        assert_eq!(dict_slot.col_name.as_deref(), Some("s"));
        assert_eq!(
            dict_slot
                .slot_type
                .as_ref()
                .and_then(|t| t.types.as_ref())
                .and_then(|tys| tys.first())
                .and_then(|tn| tn.scalar_type.as_ref())
                .map(|st| st.type_),
            Some(crate::types::TPrimitiveType::INT),
            "dict slot type must be INT (Int32)"
        );
        let dict_slot_id = dict_slot.id.expect("dict slot id");

        // LakeScanNode's dict_string_id_to_int_ids must self-map
        // (dict_slot -> dict_slot). The BE swaps each int slot with its
        // mapped string slot in the layout; with the dict slot at the
        // source column's storage position, the self-map keeps the
        // layout one slot wide, avoiding the duplicate-slot-id error.
        let scan_node = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        let lake = scan_node
            .lake_scan_node
            .as_ref()
            .expect("lake scan payload");
        let mapping = lake
            .dict_string_id_to_int_ids
            .as_ref()
            .expect("dict_string_id_to_int_ids populated");
        assert_eq!(mapping.len(), 1, "exactly one dict slot mapping");
        assert_eq!(
            mapping.get(&dict_slot_id).copied(),
            Some(dict_slot_id),
            "dict slot must self-map (FE emits single slot per dict column)"
        );
    }

    #[test]
    fn scan_dict_column_on_iceberg_scan_is_supported() {
        use crate::sql::catalog::{IcebergSchemaDef, IcebergTableInfo};

        // Build an Iceberg scan (non-StarRocks ScanSource) carrying a
        // dict_columns entry. With Option A landed, iceberg/HDFS scans now
        // support dict_columns: the dicts flow via query_global_dicts in
        // lowering rather than via TLakeScanNode.dict_string_id_to_int_ids.
        // visit_scan must succeed (the thrift node is left untouched).
        let iceberg_table_info = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 0,
            location: "s3://b/t".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
        };
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalScan(PhysicalScanOp {
                database: "default".to_string(),
                table: TableDef {
                    name: "ice_t".to_string(),
                    columns: vec![ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    }],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::IcebergDataFiles {
                        table: iceberg_table_info,
                        files: vec![],
                        cloud_properties: std::collections::BTreeMap::new(),
                    },
                },
                alias: None,
                columns: vec![OutputColumn {
                    column_id: crate::sql::column_id::ColumnId::UNSET,
                    name: "id".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    is_internal: false,
                }],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![ScanDictionaryColumn {
                    source_column: "id".to_string(),
                    dict_column: "id_dict".to_string(),
                    dictionary: dict_snapshot_a_b(),
                }],
            }),
            children: vec![],
            stats: stats(),
            output_columns: vec![OutputColumn {
                column_id: crate::sql::column_id::ColumnId::UNSET,
                name: "id".to_string(),
                data_type: DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
        };

        // Use an iceberg-only catalog (returns None for physical_layout) so
        // codegen routes the scan through the HDFS-style scan node instead of
        // the StarRocks lake scan path. visit_scan must now succeed: the HDFS
        // node is left untouched and the dict flows via query_global_dicts.
        struct IcebergCatalog;
        impl CatalogProvider for IcebergCatalog {
            fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
                Err("not used".to_string())
            }
            fn get_physical_layout(
                &self,
                _database: &str,
                _table: &str,
            ) -> Result<Option<PhysicalTableLayout>, String> {
                Ok(None)
            }
        }
        let catalog = IcebergCatalog;
        PlanFragmentBuilder::build(&plan, &catalog, &mock_iceberg_registry(), "default")
            .expect("iceberg scan with dict_columns must now succeed (Option A)");
    }

    #[test]
    fn starrocks_fragment_exec_params_are_generated_from_planned_connector_scan() {
        let layout = starrocks_layout();
        let plan = starrocks_scan_plan();
        let registry = mock_starrocks_registry(&layout);
        let catalog = StarRocksCatalog { layout };

        let build = PlanFragmentBuilder::build(&plan, &catalog, &registry, "default")
            .expect("build StarRocks fragment");
        let root = build
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == build.root_fragment_id)
            .expect("root fragment");
        let exec_params = &root.exec_params;
        let per_node = &exec_params.per_node_scan_ranges;
        let ranges = per_node
            .values()
            .next()
            .expect("one scan node should have ranges");

        assert_eq!(ranges.len(), 1);
        let tablet_ids = ranges
            .iter()
            .map(|range| {
                range
                    .scan_range
                    .internal_scan_range
                    .as_ref()
                    .map(|internal| internal.tablet_id)
                    .expect("internal scan range")
            })
            .collect::<Vec<_>>();
        assert_eq!(tablet_ids, vec![101]);
    }

    #[test]
    fn visit_scan_calls_connector_begin_scan_and_plan_splits_for_starrocks() {
        use crate::connector::starrocks::table::StarRocksSplit;
        let layout = starrocks_layout();
        let plan = with_id_predicate(starrocks_scan_plan(), 7);
        let catalog = StarRocksCatalog {
            layout: layout.clone(),
        };

        let splits: Vec<StarRocksSplit> = layout
            .tablets
            .iter()
            .map(|tablet| StarRocksSplit {
                tablet_id: tablet.tablet_id,
                partition_id: tablet.partition_id,
                version: tablet.version,
            })
            .collect();
        let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
        let planner = std::sync::Arc::new(CountingScanPlanner {
            inner: MockScanPlanner {
                schema_id: layout.schema_id,
                splits,
            },
            counts: counts.clone(),
        });
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(planner);

        let built = PlanFragmentBuilder::build(&plan, &catalog, &registry, "default")
            .expect("build StarRocks fragment");

        assert_eq!(
            counts.begin_scan.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "begin_scan must be invoked exactly once for the StarRocks scan"
        );
        assert_eq!(
            counts.plan_splits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "plan_splits must be invoked exactly once for the StarRocks scan"
        );
        assert_eq!(
            counts
                .to_thrift_scan
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "to_thrift_scan must be invoked for both scan node and scan ranges"
        );
        let root = built
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == built.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::LAKE_SCAN_NODE)
            .expect("lake scan node");
        let contexts = counts.thrift_contexts.lock().expect("thrift contexts");
        assert_eq!(
            contexts
                .iter()
                .map(|ctx| (ctx.node_id, ctx.scan_tuple_id))
                .collect::<Vec<_>>(),
            vec![(scan.node_id, scan.row_tuples[0]); 2],
            "both to_thrift_scan calls must carry the real scan node and tuple ids"
        );
        let contexts_with_conjuncts = contexts
            .iter()
            .filter(|ctx| !ctx.conjuncts.is_empty())
            .count();
        assert_eq!(
            contexts_with_conjuncts, 1,
            "exactly one to_thrift_scan call should carry node conjuncts; the range-only call should not"
        );
    }

    #[test]
    fn visit_scan_calls_connector_begin_scan_and_plan_splits_for_iceberg() {
        let plan = with_id_predicate(iceberg_scan_plan(), 7);
        let catalog = DummyCatalog;

        let counts = std::sync::Arc::new(ScanPlannerCallCounts::default());
        let planner = std::sync::Arc::new(CountingIcebergScanPlanner {
            inner: crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
            counts: counts.clone(),
        });
        let mut registry = crate::connector::ConnectorRegistry::new();
        registry.register_scan_planner(planner);

        let built = PlanFragmentBuilder::build(&plan, &catalog, &registry, "default")
            .expect("build Iceberg fragment");

        assert_eq!(
            counts.begin_scan.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "begin_scan must be invoked exactly once for the Iceberg scan"
        );
        assert_eq!(
            counts.plan_splits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "plan_splits must be invoked exactly once for the Iceberg scan"
        );
        assert_eq!(
            counts
                .to_thrift_scan
                .load(std::sync::atomic::Ordering::SeqCst),
            2,
            "to_thrift_scan must be invoked for both scan node and scan ranges"
        );
        let root = built
            .fragment_results
            .iter()
            .find(|fragment| fragment.fragment_id == built.root_fragment_id)
            .expect("root fragment");
        let scan = root
            .plan
            .nodes
            .iter()
            .find(|node| node.node_type == plan_nodes::TPlanNodeType::HDFS_SCAN_NODE)
            .expect("hdfs scan node");
        let contexts = counts.thrift_contexts.lock().expect("thrift contexts");
        assert_eq!(
            contexts
                .iter()
                .map(|ctx| (ctx.node_id, ctx.scan_tuple_id))
                .collect::<Vec<_>>(),
            vec![(scan.node_id, scan.row_tuples[0]); 2],
            "both to_thrift_scan calls must carry the real scan node and tuple ids"
        );
        let contexts_with_conjuncts = contexts
            .iter()
            .filter(|ctx| !ctx.conjuncts.is_empty())
            .count();
        assert_eq!(
            contexts_with_conjuncts, 1,
            "exactly one to_thrift_scan call should carry node conjuncts; the range-only call should not"
        );
    }
}
