//! Thrift Emitter — converts [`LogicalPlan`] into [`PlanBuildResult`].
//!
//! Walks the logical plan tree, allocates physical resources (tuple_id,
//! slot_id, node_id), compiles TypedExpr into Thrift TExpr, and produces
//! the final Thrift plan structures expected by the pipeline executor.

mod emit_aggregate;
mod emit_join;
mod emit_project;
mod emit_repeat;
mod emit_scan;
mod emit_set_op;
mod emit_sort;
mod emit_values;
mod emit_window;
pub(super) mod helpers;

use crate::exprs;
use crate::internal_service;
use crate::plan_nodes;

use crate::sql::catalog::CatalogProvider;

use super::descriptors::DescriptorTableBuilder as DescBuilder;
use super::expr_compiler::ExprCompiler;
use super::nodes;
use super::resolve::{ExprScope, ResolvedTable};
use crate::sql::ir::{self as query_ir, TypedExpr};
use crate::sql::plan::*;

use crate::sql::cte::CteId;

use super::{OutputColumn, PlanBuildResult};

// Re-export for expr_compiler.rs which references super::emitter::agg_call_display_name_from_parts
pub(crate) use helpers::agg_call_display_name_from_parts;

// ---------------------------------------------------------------------------
// Public entry
// ---------------------------------------------------------------------------

pub(crate) fn emit(
    plan: LogicalPlan,
    output_columns: &[query_ir::OutputColumn],
    catalog: &dyn CatalogProvider,
    current_database: &str,
) -> Result<PlanBuildResult, String> {
    let mut emitter = ThriftEmitter::new(catalog, current_database);
    emitter.emit_plan(plan, output_columns)
}

pub(crate) fn emit_multi_fragment(
    fragment_plan: crate::sql::fragment::FragmentPlan,
    catalog: &dyn CatalogProvider,
    current_database: &str,
) -> Result<super::MultiFragmentBuildResult, String> {
    let mut emitter = ThriftEmitter::new(catalog, current_database);
    let mut fragment_results = Vec::new();

    for fragment in &fragment_plan.fragments {
        // Save and reset per-fragment mutable state
        let prev_scan_tables = std::mem::take(&mut emitter.scan_tables);
        let prev_cte_exchange_nodes = std::mem::take(&mut emitter.cte_exchange_nodes);

        // Emit this fragment's plan tree
        let result = emitter.emit_node(fragment.plan.clone())?;

        // Extract per-fragment state
        let scan_tables = std::mem::replace(&mut emitter.scan_tables, prev_scan_tables);
        let cte_exchange_nodes =
            std::mem::replace(&mut emitter.cte_exchange_nodes, prev_cte_exchange_nodes);

        // Build per-fragment exec_params
        let exec_params = nodes::build_exec_params_multi(
            &scan_tables
                .iter()
                .map(|(id, rt)| (*id, rt.clone()))
                .collect::<Vec<_>>(),
        )?;

        // Build output sink
        let output_sink = match &fragment.sink {
            crate::sql::fragment::FragmentSink::Result => {
                // Build a RESULT_SINK
                crate::data_sinks::TDataSink::new(
                    crate::data_sinks::TDataSinkType::RESULT_SINK,
                    None::<crate::data_sinks::TDataStreamSink>,
                    Some(crate::data_sinks::TResultSink::default()),
                    None::<crate::data_sinks::TMysqlTableSink>,
                    None::<crate::data_sinks::TExportSink>,
                    None::<crate::data_sinks::TOlapTableSink>,
                    None::<crate::data_sinks::TMemoryScratchSink>,
                    None::<crate::data_sinks::TMultiCastDataStreamSink>,
                    None::<crate::data_sinks::TSchemaTableSink>,
                    None::<crate::data_sinks::TIcebergTableSink>,
                    None::<crate::data_sinks::THiveTableSink>,
                    None::<crate::data_sinks::TTableFunctionTableSink>,
                    None::<crate::data_sinks::TDictionaryCacheSink>,
                    None::<Vec<Box<crate::data_sinks::TDataSink>>>,
                    None::<i64>,
                    None::<crate::data_sinks::TSplitDataStreamSink>,
                )
            }
            crate::sql::fragment::FragmentSink::MultiCast { .. } => {
                // Placeholder — actual multicast sink built by coordinator
                // after all fragments emitted
                crate::data_sinks::TDataSink::new(
                    crate::data_sinks::TDataSinkType::NOOP_SINK,
                    None::<crate::data_sinks::TDataStreamSink>,
                    None::<crate::data_sinks::TResultSink>,
                    None::<crate::data_sinks::TMysqlTableSink>,
                    None::<crate::data_sinks::TExportSink>,
                    None::<crate::data_sinks::TOlapTableSink>,
                    None::<crate::data_sinks::TMemoryScratchSink>,
                    None::<crate::data_sinks::TMultiCastDataStreamSink>,
                    None::<crate::data_sinks::TSchemaTableSink>,
                    None::<crate::data_sinks::TIcebergTableSink>,
                    None::<crate::data_sinks::THiveTableSink>,
                    None::<crate::data_sinks::TTableFunctionTableSink>,
                    None::<crate::data_sinks::TDictionaryCacheSink>,
                    None::<Vec<Box<crate::data_sinks::TDataSink>>>,
                    None::<i64>,
                    None::<crate::data_sinks::TSplitDataStreamSink>,
                )
            }
        };

        let cte_id = match &fragment.sink {
            crate::sql::fragment::FragmentSink::MultiCast { cte_id, .. } => Some(*cte_id),
            _ => None,
        };

        fragment_results.push(super::FragmentBuildResult {
            fragment_id: fragment.id,
            plan: crate::plan_nodes::TPlan::new(result.plan_nodes),
            desc_tbl: DescBuilder::new().build(), // placeholder, replaced below
            exec_params,
            output_sink,
            output_columns: fragment
                .output_columns
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                })
                .collect(),
            cte_id,
            cte_exchange_nodes,
        });
    }

    // Build shared descriptor table and assign to all fragments
    let desc_tbl =
        std::mem::replace(&mut emitter.desc_builder, super::descriptors::DescriptorTableBuilder::new()).build();
    for fr in &mut fragment_results {
        fr.desc_tbl = desc_tbl.clone();
    }

    Ok(super::MultiFragmentBuildResult {
        fragment_results,
        root_fragment_id: fragment_plan.root_fragment_id,
    })
}

// ---------------------------------------------------------------------------
// Emitter state
// ---------------------------------------------------------------------------

pub(super) struct ThriftEmitter<'a> {
    next_slot_id: i32,
    next_tuple_id: i32,
    next_node_id: i32,
    desc_builder: DescBuilder,
    /// (node_id, ResolvedTable) for each scan node, used to build exec_params.
    scan_tables: Vec<(i32, ResolvedTable)>,
    /// (cte_id, exchange_node_id) for each CTE consume node emitted in the current fragment.
    cte_exchange_nodes: Vec<(CteId, i32)>,
    catalog: &'a dyn CatalogProvider,
    current_database: &'a str,
}

/// Result of emitting a subtree: the generated plan nodes and the output scope.
pub(super) struct EmitResult {
    /// Plan nodes in pre-order (top-down) traversal order.
    pub plan_nodes: Vec<plan_nodes::TPlanNode>,
    /// Scope describing the output columns with their physical bindings.
    pub scope: ExprScope,
    /// Tuple IDs in this subtree's output.
    pub tuple_ids: Vec<i32>,
}

impl<'a> ThriftEmitter<'a> {
    fn new(catalog: &'a dyn CatalogProvider, current_database: &'a str) -> Self {
        Self {
            next_slot_id: 1,
            next_tuple_id: 1,
            next_node_id: 1,
            desc_builder: DescBuilder::new(),
            scan_tables: Vec::new(),
            cte_exchange_nodes: Vec::new(),
            catalog,
            current_database,
        }
    }

    fn alloc_slot(&mut self) -> i32 {
        let id = self.next_slot_id;
        self.next_slot_id += 1;
        id
    }

    fn alloc_tuple(&mut self) -> i32 {
        let id = self.next_tuple_id;
        self.next_tuple_id += 1;
        id
    }

    fn alloc_node(&mut self) -> i32 {
        let id = self.next_node_id;
        self.next_node_id += 1;
        id
    }

    // -----------------------------------------------------------------------
    // Top-level plan emission
    // -----------------------------------------------------------------------

    fn emit_plan(
        &mut self,
        plan: LogicalPlan,
        output_columns: &[query_ir::OutputColumn],
    ) -> Result<PlanBuildResult, String> {
        let result = self.emit_node(plan)?;

        let desc_tbl = std::mem::replace(&mut self.desc_builder, DescBuilder::new()).build();
        let exec_params = self.build_exec_params()?;

        let out_cols = output_columns
            .iter()
            .map(|c| OutputColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            })
            .collect();

        Ok(PlanBuildResult {
            plan: plan_nodes::TPlan::new(result.plan_nodes),
            desc_tbl,
            exec_params,
            output_columns: out_cols,
        })
    }

    // -----------------------------------------------------------------------
    // Recursive plan node emission
    // -----------------------------------------------------------------------

    fn emit_node(&mut self, plan: LogicalPlan) -> Result<EmitResult, String> {
        match plan {
            LogicalPlan::Scan(node) => self.emit_scan(node),
            LogicalPlan::Filter(node) => self.emit_filter(node),
            LogicalPlan::Project(node) => self.emit_project(node),
            LogicalPlan::Aggregate(node) => self.emit_aggregate(node),
            LogicalPlan::Join(node) => self.emit_join(node),
            LogicalPlan::Sort(node) => self.emit_sort(node),
            LogicalPlan::Limit(node) => self.emit_limit(node),
            LogicalPlan::Union(node) => self.emit_union(node),
            LogicalPlan::Values(node) => self.emit_values(node),
            LogicalPlan::Intersect(node) => self.emit_intersect(node),
            LogicalPlan::Except(node) => self.emit_except(node),
            LogicalPlan::GenerateSeries(node) => self.emit_generate_series(node),
            LogicalPlan::Window(node) => self.emit_window(node),
            LogicalPlan::SubqueryAlias(node) => self.emit_subquery_alias(node),
            LogicalPlan::Repeat(node) => self.emit_repeat(node),
            LogicalPlan::CTEConsume(node) => self.emit_cte_consume(node),
        }
    }

    fn emit_subquery_alias(
        &mut self,
        node: crate::sql::plan::SubqueryAliasNode,
    ) -> Result<EmitResult, String> {
        let mut child = self.emit_node(*node.input)?;

        // Register all output columns with the alias as qualifier, so that
        // downstream references like `ctr1.ctr_customer_sk` can resolve.
        // The child plan's scope already has unqualified entries; we add
        // qualified entries on top.
        for col in &node.output_columns {
            let col_name_lower = col.name.to_lowercase();
            // Find the binding for this column in the child scope (unqualified)
            if let Ok(binding) = child.scope.resolve_column(None, &col_name_lower) {
                let binding = binding.clone();
                child
                    .scope
                    .add_column(Some(node.alias.clone()), col.name.clone(), binding);
            }
        }

        Ok(child)
    }

    // -----------------------------------------------------------------------
    // CTE consume emission (exchange node for multi-fragment plans)
    // -----------------------------------------------------------------------

    fn emit_cte_consume(
        &mut self,
        node: crate::sql::plan::CTEConsumeNode,
    ) -> Result<EmitResult, String> {
        use super::resolve::ColumnBinding;

        let tuple_id = self.alloc_tuple();
        let node_id = self.alloc_node();

        let mut scope = ExprScope::new();

        for (idx, col) in node.output_columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                nullable: col.nullable,
            };
            scope.add_column(Some(node.alias.clone()), col.name.clone(), binding.clone());
            // Also register unqualified
            scope.add_column(None, col.name.clone(), binding);
        }
        self.desc_builder.add_tuple(tuple_id);

        let exchange_plan_node = nodes::build_exchange_node(node_id, vec![tuple_id]);

        // Record this CTE consume for the coordinator to wire up multicast destinations
        self.cte_exchange_nodes.push((node.cte_id, node_id));

        Ok(EmitResult {
            plan_nodes: vec![exchange_plan_node],
            scope,
            tuple_ids: vec![tuple_id],
        })
    }

    // -----------------------------------------------------------------------
    // Conjunct splitting and compilation for filters
    // -----------------------------------------------------------------------

    fn split_and_compile_conjuncts(
        &self,
        predicate: &TypedExpr,
        scope: &ExprScope,
    ) -> Result<Vec<exprs::TExpr>, String> {
        let conjuncts = helpers::split_and_conjuncts_typed(predicate);
        let mut results = Vec::new();
        for conj in conjuncts {
            let mut compiler = ExprCompiler::new(scope);
            results.push(compiler.compile_typed(conj)?);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Exec params
    // -----------------------------------------------------------------------

    fn build_exec_params(&self) -> Result<internal_service::TPlanFragmentExecParams, String> {
        nodes::build_exec_params_multi(
            &self
                .scan_tables
                .iter()
                .map(|(id, rt)| (*id, rt.clone()))
                .collect::<Vec<_>>(),
        )
    }
}
