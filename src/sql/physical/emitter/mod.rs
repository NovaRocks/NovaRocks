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
            LogicalPlan::CTEConsume(_node) => {
                Err("CTEConsume should be handled by multi-fragment emission".to_string())
            }
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
