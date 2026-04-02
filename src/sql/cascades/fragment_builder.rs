//! PlanFragmentBuilder — converts a PhysicalPlanNode tree into Thrift TPlan
//! per fragment.
//!
//! Fragment boundaries are created at `PhysicalDistribution` nodes.
//! `PhysicalCTEProduce` / `PhysicalCTEConsume` create multicast fragments
//! whose sinks are wired by the `ExecutionCoordinator` after building.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::data_sinks;
use crate::exprs;
use crate::plan_nodes;

use crate::sql::catalog::CatalogProvider;
use crate::sql::cascades::operator::Operator;
use crate::sql::cascades::operator::{
    PhysicalCTEConsumeOp, PhysicalCTEProduceOp, PhysicalDistributionOp, PhysicalFilterOp,
    PhysicalGenerateSeriesOp, PhysicalHashAggregateOp, PhysicalHashJoinOp, PhysicalLimitOp,
    PhysicalNestLoopJoinOp, PhysicalProjectOp, PhysicalRepeatOp, PhysicalScanOp, PhysicalSortOp,
    PhysicalSubqueryAliasOp, PhysicalValuesOp, PhysicalWindowOp,
};
use crate::sql::cascades::physical_plan::PhysicalPlanNode;
use crate::sql::cte::CteId;
use crate::sql::fragment::FragmentId;
use crate::sql::physical::descriptors::DescriptorTableBuilder;
use crate::sql::physical::emitter::helpers::{
    agg_call_display_name, join_kind_to_op, split_and_conjuncts_typed, typed_expr_display_name,
};
use crate::sql::physical::expr_compiler::ExprCompiler;
use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ColumnBinding, ExprScope, ResolvedTable};
use crate::sql::physical::{FragmentBuildResult, OutputColumn};

use crate::sql::ir::{ExprKind, TypedExpr};
use crate::sql::plan::AggregateCall;

// ---------------------------------------------------------------------------
// Public result type
// ---------------------------------------------------------------------------

pub(crate) struct BuildResult {
    pub fragments: Vec<FragmentBuildResult>,
    pub root_fragment_id: FragmentId,
}

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
}

// ---------------------------------------------------------------------------
// PlanFragmentBuilder
// ---------------------------------------------------------------------------

pub(crate) struct PlanFragmentBuilder<'a> {
    catalog: &'a dyn CatalogProvider,
    current_database: &'a str,
    desc_builder: DescriptorTableBuilder,
    scan_tables: Vec<(i32, ResolvedTable)>,
    next_node_id: i32,
    next_slot_id: i32,
    next_tuple_id: i32,
    next_fragment_id: FragmentId,
    /// Fragments finalized during visitation (child fragments from distribution
    /// boundaries and CTE produce fragments).
    completed_fragments: Vec<FragmentBuildResult>,
    /// CTE ID -> index in `completed_fragments`.
    cte_fragments: HashMap<CteId, usize>,
    /// Exchange node IDs that consume from CTE fragments, recorded for the root
    /// fragment: `(cte_id, exchange_node_id)`.
    cte_exchange_nodes: Vec<(CteId, i32)>,
}

impl<'a> PlanFragmentBuilder<'a> {
    // -------------------------------------------------------------------
    // Public entry
    // -------------------------------------------------------------------

    pub(crate) fn build(
        plan: &PhysicalPlanNode,
        catalog: &'a dyn CatalogProvider,
        current_database: &str,
    ) -> Result<BuildResult, String> {
        let mut builder = PlanFragmentBuilder {
            catalog,
            current_database,
            desc_builder: DescriptorTableBuilder::new(),
            scan_tables: Vec::new(),
            next_node_id: 1,
            next_slot_id: 1,
            next_tuple_id: 1,
            next_fragment_id: 0,
            completed_fragments: Vec::new(),
            cte_fragments: HashMap::new(),
            cte_exchange_nodes: Vec::new(),
        };

        let result = builder.visit(plan)?;

        // Build the shared descriptor table and exec params.  All fragments
        // share the same descriptor table and scan ranges since the
        // coordinator rewires instance IDs and sinks after the fact.
        let desc_tbl = std::mem::replace(&mut builder.desc_builder, DescriptorTableBuilder::new())
            .build();

        let exec_params = nodes::build_exec_params_multi(
            &builder
                .scan_tables
                .iter()
                .map(|(id, rt)| (*id, rt.clone()))
                .collect::<Vec<_>>(),
        )?;

        let output_columns = plan
            .output_columns
            .iter()
            .map(|c| OutputColumn {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
            })
            .collect();

        // Build the root fragment with a result sink.
        let root_fragment_id = builder.alloc_fragment_id();
        let root_fragment = FragmentBuildResult {
            fragment_id: root_fragment_id,
            plan: plan_nodes::TPlan::new(result.plan_nodes),
            desc_tbl: desc_tbl.clone(),
            exec_params: exec_params.clone(),
            output_sink: build_result_sink(),
            output_columns,
            cte_id: None,
            cte_exchange_nodes: builder.cte_exchange_nodes,
        };

        // Patch all completed (child) fragments with the shared descriptor
        // table and exec params.
        for frag in &mut builder.completed_fragments {
            frag.desc_tbl = desc_tbl.clone();
            frag.exec_params = exec_params.clone();
        }

        // Assemble all fragments: completed child fragments first, then root.
        let mut fragments = builder.completed_fragments;
        fragments.push(root_fragment);

        Ok(BuildResult {
            fragments,
            root_fragment_id,
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
        let id = self.next_slot_id;
        self.next_slot_id += 1;
        id
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

    // -------------------------------------------------------------------
    // Dispatcher
    // -------------------------------------------------------------------

    fn visit(&mut self, node: &PhysicalPlanNode) -> Result<VisitResult, String> {
        match &node.op {
            Operator::PhysicalScan(op) => self.visit_scan(op, node),
            Operator::PhysicalFilter(op) => self.visit_filter(op, node),
            Operator::PhysicalProject(op) => self.visit_project(op, node),
            Operator::PhysicalHashJoin(op) => self.visit_hash_join(op, node),
            Operator::PhysicalNestLoopJoin(op) => self.visit_nest_loop_join(op, node),
            Operator::PhysicalHashAggregate(op) => self.visit_hash_aggregate(op, node),
            Operator::PhysicalSort(op) => self.visit_sort(op, node),
            Operator::PhysicalLimit(op) => self.visit_limit(op, node),
            Operator::PhysicalWindow(op) => self.visit_window(op, node),
            Operator::PhysicalValues(op) => self.visit_values(op, node),
            Operator::PhysicalGenerateSeries(op) => self.visit_generate_series(op, node),
            Operator::PhysicalSubqueryAlias(op) => self.visit_subquery_alias(op, node),
            Operator::PhysicalRepeat(op) => self.visit_repeat(op, node),
            Operator::PhysicalDistribution(op) => self.visit_distribution(op, node),
            Operator::PhysicalCTEProduce(op) => self.visit_cte_produce(op, node),
            Operator::PhysicalCTEConsume(op) => self.visit_cte_consume(op),
            // Set operations: union, intersect, except — not yet wired in cascades
            Operator::PhysicalUnion(_)
            | Operator::PhysicalIntersect(_)
            | Operator::PhysicalExcept(_) => {
                Err(format!("set operation {:?} not yet supported in cascades fragment builder", node.op))
            }
            // Logical operators should never appear in an extracted physical plan
            other if other.is_logical() => {
                Err(format!("unexpected logical operator in physical plan: {:?}", other))
            }
            other => Err(format!("unhandled operator in fragment builder: {:?}", other)),
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
            let mut compiler = ExprCompiler::new(scope);
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

        // Determine which columns to emit
        let required: Option<std::collections::HashSet<String>> = op
            .required_columns
            .as_ref()
            .map(|cols| cols.iter().map(|c| c.to_lowercase()).collect());

        for (idx, col) in op.table.columns.iter().enumerate() {
            if let Some(ref req) = required {
                if !req.contains(&col.name.to_lowercase()) {
                    continue;
                }
            }
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: scan_tuple_id,
                slot_id,
                data_type: col.data_type.clone(),
                nullable: col.nullable,
            };
            scope.add_column(
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                binding.clone(),
            );
            // When alias differs from table name, also register with original table name
            if op
                .alias
                .as_deref()
                .is_some_and(|a| !a.eq_ignore_ascii_case(&op.table.name))
            {
                scope.add_column(Some(op.table.name.clone()), col.name.clone(), binding);
            }
        }
        self.desc_builder.add_tuple(scan_tuple_id);

        // Compile predicates pushed down by the optimizer
        let pushed_conjuncts = if op.predicates.is_empty() {
            vec![]
        } else {
            let mut conjuncts = Vec::new();
            for pred in &op.predicates {
                let mut compiler = ExprCompiler::new(&scope);
                conjuncts.push(compiler.compile_typed(pred)?);
            }
            conjuncts
        };

        let resolved = ResolvedTable {
            database: op.database.clone(),
            table: op.table.clone(),
            alias: op.alias.clone(),
        };

        let scan_plan_node =
            nodes::build_scan_node(scan_node_id, scan_tuple_id, &resolved, pushed_conjuncts);
        self.scan_tables.push((scan_node_id, resolved));

        Ok(VisitResult {
            plan_nodes: vec![scan_plan_node],
            scope,
            tuple_ids: vec![scan_tuple_id],
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
                if scan.conjuncts.is_none() {
                    scan.conjuncts = Some(conjuncts);
                } else {
                    scan.conjuncts.as_mut().unwrap().extend(conjuncts);
                }
            }
        }

        Ok(child)
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
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            let data_type = item.expr.data_type.clone();
            let nullable = item.expr.nullable;
            let name = item.output_name.clone();
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                project_tuple_id,
                &name,
                &data_type,
                nullable,
                output_columns.len() as i32,
            );
            slot_map.insert(slot_id, texpr);
            output_columns.push(OutputColumn {
                name: name.clone(),
                data_type: data_type.clone(),
                nullable,
            });

            project_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: project_tuple_id,
                    slot_id,
                    data_type: data_type.clone(),
                    nullable,
                },
            );

            // Also register with qualifier if the expression is a column ref
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
            } = item.expr.kind
            {
                project_scope.add_column(
                    Some(q.clone()),
                    column.clone(),
                    ColumnBinding {
                        tuple_id: project_tuple_id,
                        slot_id,
                        data_type,
                        nullable,
                    },
                );
            }
        }

        self.desc_builder.add_tuple(project_tuple_id);
        let project_plan_node =
            nodes::build_project_node(project_node_id, project_tuple_id, slot_map);

        // Pre-order: project first, then child nodes
        let mut plan_nodes = vec![project_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: project_scope,
            tuple_ids: vec![project_tuple_id],
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

        // Compile eq conditions.  The eq_conditions pairs come from the SQL
        // text order (e.g. `l_orderkey = o_orderkey`), which may not match the
        // join's left/right child assignment.  Try the natural order first; if
        // the left expr fails against the left scope, swap sides.
        let mut eq_join_conjuncts = Vec::new();
        for (expr_a, expr_b) in &op.eq_conditions {
            let (left_texpr, right_texpr) = {
                let try_left = ExprCompiler::new(&left.scope).compile_typed(expr_a);
                if let Ok(lt) = try_left {
                    let rt = ExprCompiler::new(&right.scope).compile_typed(expr_b)?;
                    (lt, rt)
                } else {
                    // Swap: expr_a is from the right child, expr_b from the left.
                    let lt = ExprCompiler::new(&left.scope).compile_typed(expr_b)?;
                    let rt = ExprCompiler::new(&right.scope).compile_typed(expr_a)?;
                    (lt, rt)
                }
            };
            eq_join_conjuncts.push(plan_nodes::TEqJoinCondition {
                left: left_texpr,
                right: right_texpr,
                opcode: Some(crate::opcodes::TExprOpcode::EQ),
            });
        }

        // Compile other conditions
        let other_join_conjuncts = if let Some(ref cond) = op.other_condition {
            let mut merged = ExprScope::new();
            merged.merge(&left.scope);
            merged.merge(&right.scope);
            let mut compiler = ExprCompiler::new(&merged);
            vec![compiler.compile_typed(cond)?]
        } else {
            vec![]
        };

        let join_plan_node = nodes::build_hash_join_node(
            join_node_id,
            &left.tuple_ids,
            &right.tuple_ids,
            join_op,
            eq_join_conjuncts,
            other_join_conjuncts,
        );

        let mut merged_scope = left.scope;
        merged_scope.merge(&right.scope);

        let mut merged_tuple_ids = left.tuple_ids;
        merged_tuple_ids.extend(right.tuple_ids);

        // Pre-order: join node, then left subtree, then right subtree
        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left.plan_nodes);
        plan_nodes.extend(right.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: merged_scope,
            tuple_ids: merged_tuple_ids,
        })
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
                let mut compiler = ExprCompiler::new(&merged);
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

        let mut merged_scope = left.scope;
        merged_scope.merge(&right.scope);

        let mut merged_tuple_ids = left.tuple_ids;
        merged_tuple_ids.extend(right.tuple_ids);

        let mut plan_nodes = vec![join_plan_node];
        plan_nodes.extend(left.plan_nodes);
        plan_nodes.extend(right.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: merged_scope,
            tuple_ids: merged_tuple_ids,
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

        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();

        let mut agg_scope = ExprScope::new();
        let mut grouping_exprs = Vec::new();

        // Compile GROUP BY expressions
        for (idx, gb_expr) in op.group_by.iter().enumerate() {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(gb_expr)?;
            let data_type = gb_expr.data_type.clone();
            let nullable = gb_expr.nullable;
            let name = typed_expr_display_name(gb_expr);
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                agg_tuple_id,
                &name,
                &data_type,
                nullable,
                idx as i32,
            );
            let binding = ColumnBinding {
                tuple_id: agg_tuple_id,
                slot_id,
                data_type: data_type.clone(),
                nullable,
            };
            agg_scope.add_column(None, name, binding.clone());
            // Also register with qualifier for post-aggregate qualified refs
            if let ExprKind::ColumnRef {
                qualifier: Some(ref q),
                ref column,
            } = gb_expr.kind
            {
                agg_scope.add_column(Some(q.clone()), column.clone(), binding);
            }
            grouping_exprs.push(texpr);
        }

        // Compile aggregate function expressions
        let agg_start_col = op.group_by.len();
        let mut aggregate_functions = Vec::new();

        for (idx, agg_call) in op.aggregates.iter().enumerate() {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_aggregate_call_typed(agg_call)?;
            let data_type = agg_call.result_type.clone();
            let nullable = true;
            let name = agg_call_display_name(agg_call);
            let slot_id = self.alloc_slot();
            let col_pos = (agg_start_col + idx) as i32;
            self.desc_builder
                .add_slot(slot_id, agg_tuple_id, &name, &data_type, nullable, col_pos);
            agg_scope.add_column(
                None,
                name,
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type,
                    nullable,
                },
            );
            aggregate_functions.push(texpr);
        }

        self.desc_builder.add_tuple(agg_tuple_id);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            aggregate_functions,
        );

        // Pre-order: agg first, then child nodes
        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
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
        let sort_tuple_id = *child.tuple_ids.last().unwrap();

        let mut ordering_exprs = Vec::new();
        let mut is_asc = Vec::new();
        let mut nulls_first_list = Vec::new();

        for item in &op.items {
            let mut compiler = ExprCompiler::new(&child.scope);
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
        sort_plan_node.limit = -1;
        sort_plan_node.row_tuples = vec![sort_tuple_id];
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

        // Pre-order: sort first, then child
        let mut plan_nodes = vec![sort_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: child.scope,
            tuple_ids: child.tuple_ids,
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
        let mut child = self.visit(&node.children[0])?;

        // Apply limit/offset to the top-most sort node if present,
        // or to the top-most node otherwise.
        if let Some(top) = child.plan_nodes.first_mut() {
            if top.node_type == plan_nodes::TPlanNodeType::SORT_NODE {
                if let Some(limit) = op.limit {
                    top.limit = limit;
                    if let Some(ref mut sn) = top.sort_node {
                        sn.use_top_n = true;
                        sn.offset = op.offset;
                    }
                }
            } else if let Some(limit) = op.limit {
                top.limit = limit;
            }
        }

        Ok(child)
    }

    // -------------------------------------------------------------------
    // visit_window
    // -------------------------------------------------------------------

    fn visit_window(
        &mut self,
        op: &PhysicalWindowOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        use crate::sql::ir::{WindowBound, WindowFrameType};

        let child = self.visit(&node.children[0])?;
        let analytic_node_id = self.alloc_node();

        let intermediate_tuple_id = self.alloc_tuple();
        let output_tuple_id = self.alloc_tuple();

        // Compile partition_by and order_by from the first window expr
        let first_win = op.window_exprs.first().ok_or("empty window_exprs")?;

        let mut partition_exprs = Vec::new();
        for expr in &first_win.partition_by {
            let mut compiler = ExprCompiler::new(&child.scope);
            partition_exprs.push(compiler.compile_typed(expr)?);
        }

        let mut order_by_exprs = Vec::new();
        for item in &first_win.order_by {
            let mut compiler = ExprCompiler::new(&child.scope);
            let texpr = compiler.compile_typed(&item.expr)?;
            order_by_exprs.push(texpr);
        }

        // Compile analytic functions
        let mut analytic_functions = Vec::new();
        for win_expr in &op.window_exprs {
            let mut compiler = ExprCompiler::new(&child.scope);
            let agg_call = AggregateCall {
                name: win_expr.name.clone(),
                args: win_expr.args.clone(),
                distinct: win_expr.distinct,
                result_type: win_expr.result_type.clone(),
                order_by: vec![],
            };
            let texpr = compiler.compile_aggregate_call_typed(&agg_call)?;
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
        self.desc_builder.add_tuple(intermediate_tuple_id);

        // Register output slots
        let mut output_scope = ExprScope::new();
        for (name, binding) in child.scope.iter_columns() {
            output_scope.add_column(None, name.clone(), binding.clone());
        }
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
                    nullable: true,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id);

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
        })
    }

    // -------------------------------------------------------------------
    // visit_values
    // -------------------------------------------------------------------

    fn visit_values(
        &mut self,
        _op: &PhysicalValuesOp,
        _node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // For values node (SELECT without FROM), produce a scan on __dual__ table.
        let scan_tuple_id = self.alloc_tuple();
        let scan_node_id = self.alloc_node();

        let dual_table = self
            .catalog
            .get_table(self.current_database, "__dual__")
            .or_else(|_| self.catalog.get_table("default", "__dual__"))
            .map_err(|_| "internal error: __dual__ table not found in catalog")?;

        for (idx, col) in dual_table.columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
        }
        self.desc_builder.add_tuple(scan_tuple_id);

        let resolved = ResolvedTable {
            database: self.current_database.to_string(),
            table: dual_table,
            alias: None,
        };

        let scan_plan_node = nodes::build_scan_node(scan_node_id, scan_tuple_id, &resolved, vec![]);
        self.scan_tables.push((scan_node_id, resolved));

        let scope = ExprScope::new();

        Ok(VisitResult {
            plan_nodes: vec![scan_plan_node],
            scope,
            tuple_ids: vec![scan_tuple_id],
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
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Generate the series values
        let mut values = Vec::new();
        let mut v = op.start;
        if op.step > 0 {
            while v <= op.end {
                values.push(v);
                v += op.step;
            }
        } else {
            while v >= op.end {
                values.push(v);
                v += op.step;
            }
        }

        // Build a temporary parquet file
        let col_name = &op.column_name;
        let schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            ArrowDataType::Int64,
            false,
        )]));
        let col_array = Arc::new(Int64Array::from(values));
        let batch = RecordBatch::try_new(schema, vec![col_array])
            .map_err(|e| format!("build generate_series batch failed: {e}"))?;

        let dir = std::env::temp_dir().join("novarocks_generate_series");
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("create generate_series dir failed: {e}"))?;
        let path = dir.join(format!(
            "gs_{}_{}_{}_{}.parquet",
            op.start, op.end, op.step, self.next_node_id
        ));
        crate::sql::physical::write_parquet_to_path(&path, &batch)?;

        // Build a TableDef and emit as a scan
        let table_def = crate::sql::catalog::TableDef {
            name: op
                .alias
                .as_deref()
                .unwrap_or("generate_series")
                .to_string(),
            columns: vec![crate::sql::catalog::ColumnDef {
                name: col_name.clone(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            }],
            storage: crate::sql::catalog::TableStorage::LocalParquetFile { path },
        };

        // Create a PhysicalScanOp and delegate to visit_scan
        let scan_op = PhysicalScanOp {
            database: self.current_database.to_string(),
            table: table_def,
            alias: op.alias.clone(),
            columns: vec![crate::sql::ir::OutputColumn {
                name: col_name.clone(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        };

        // Use a dummy node (visit_scan only reads the op, not the children)
        self.visit_scan(&scan_op, _node)
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

        // Register all output columns with the alias as qualifier
        for col in &op.output_columns {
            let col_name_lower = col.name.to_lowercase();
            if let Ok(binding) = child.scope.resolve_column(None, &col_name_lower) {
                let binding = binding.clone();
                child
                    .scope
                    .add_column(Some(op.alias.clone()), col.name.clone(), binding);
            }
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
                    nullable: false,
                },
            );
            virtual_slot_ids.push(slot);
        }

        self.desc_builder.add_tuple(virtual_tuple_id);

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
                        bits |= 1 << bit_pos;
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
        })
    }

    // -------------------------------------------------------------------
    // visit_distribution
    // -------------------------------------------------------------------

    fn visit_distribution(
        &mut self,
        _op: &PhysicalDistributionOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // In standalone mode (single node), distribution enforcers are logical
        // markers for cost model purposes.  Actual execution doesn't need
        // separate fragments — all data is local.  Pass through to child.
        self.visit(&node.children[0])
    }

    // -------------------------------------------------------------------
    // visit_cte_produce
    // -------------------------------------------------------------------

    fn visit_cte_produce(
        &mut self,
        op: &PhysicalCTEProduceOp,
        node: &PhysicalPlanNode,
    ) -> Result<VisitResult, String> {
        // Visit the child subtree that produces the CTE data.
        let child = self.visit(&node.children[0])?;

        // Finalize the child as a fragment marked with the CTE ID.
        // The coordinator will replace the NOOP_SINK with a multicast sink
        // after seeing all consumers.
        let cte_fragment_id = self.alloc_fragment_id();
        let cte_fragment = FragmentBuildResult {
            fragment_id: cte_fragment_id,
            plan: plan_nodes::TPlan::new(child.plan_nodes),
            // Placeholder; build() patches these.
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(&[])?,
            output_sink: build_noop_sink(),
            output_columns: Vec::new(),
            cte_id: Some(op.cte_id),
            cte_exchange_nodes: Vec::new(),
        };
        let idx = self.completed_fragments.len();
        self.completed_fragments.push(cte_fragment);
        self.cte_fragments.insert(op.cte_id, idx);

        // CTE produce is transparent — return the child's scope and tuple IDs
        // so that the parent can continue building on top of the CTE output.
        // The plan_nodes are empty because they have been moved into the
        // completed fragment; the parent will reference them via exchange.
        // However, the *logical* output shape (scope + tuple_ids) propagates
        // upward so that a subsequent visit_cte_consume can create a matching
        // exchange node.

        // Build an exchange node for the implicit consume that follows in the
        // plan tree (the parent of CTE produce is typically the root fragment
        // or another CTE consumer).
        let exchange_node_id = self.alloc_node();
        let exchange_node =
            nodes::build_exchange_node(exchange_node_id, child.tuple_ids.clone());
        self.cte_exchange_nodes
            .push((op.cte_id, exchange_node_id));

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope: child.scope,
            tuple_ids: child.tuple_ids,
        })
    }

    // -------------------------------------------------------------------
    // visit_cte_consume
    // -------------------------------------------------------------------

    fn visit_cte_consume(&mut self, op: &PhysicalCTEConsumeOp) -> Result<VisitResult, String> {
        // Verify the CTE produce fragment was already visited.
        if !self.cte_fragments.contains_key(&op.cte_id) {
            return Err(format!(
                "CTE consume references unknown cte_id={}",
                op.cte_id
            ));
        }

        // Allocate an exchange node that will receive data from the CTE
        // produce fragment's multicast sink.
        let exchange_node_id = self.alloc_node();

        // Record this consumer so the root fragment carries the metadata
        // needed by the coordinator to wire multicast destinations.
        self.cte_exchange_nodes
            .push((op.cte_id, exchange_node_id));

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
                nullable: col.nullable,
            };
            scope.add_column(None, col.name.clone(), binding.clone());
            // Also register with the CTE alias as qualifier
            scope.add_column(Some(op.alias.clone()), col.name.clone(), binding);
        }
        self.desc_builder.add_tuple(exchange_tuple_id);

        let exchange_node =
            nodes::build_exchange_node(exchange_node_id, vec![exchange_tuple_id]);

        Ok(VisitResult {
            plan_nodes: vec![exchange_node],
            scope,
            tuple_ids: vec![exchange_tuple_id],
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
