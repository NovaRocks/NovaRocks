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
use crate::partitions;
use crate::plan_nodes;

use crate::sql::cascades::operator::Operator;
use crate::sql::cascades::operator::{
    PhysicalCTEAnchorOp, PhysicalCTEConsumeOp, PhysicalCTEProduceOp, PhysicalDistributionOp,
    PhysicalExceptOp, PhysicalFilterOp, PhysicalGenerateSeriesOp, PhysicalHashAggregateOp,
    PhysicalHashJoinOp, PhysicalIntersectOp, PhysicalLimitOp, PhysicalNestLoopJoinOp,
    PhysicalProjectOp, PhysicalRepeatOp, PhysicalScanOp, PhysicalSortOp, PhysicalSubqueryAliasOp,
    PhysicalUnionOp, PhysicalValuesOp, PhysicalWindowOp,
};
use crate::sql::cascades::physical_plan::PhysicalPlanNode;
use crate::sql::catalog::CatalogProvider;
use crate::sql::cte::CteId;
use crate::sql::fragment::FragmentId;
use crate::sql::physical::descriptors::DescriptorTableBuilder;
use crate::sql::physical::emitter::helpers::{
    agg_call_display_name, join_kind_to_op, split_and_conjuncts_typed, typed_expr_display_name,
};
use crate::sql::physical::expr_compiler::{self, ExprCompiler};
use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ColumnBinding, ExprScope, ResolvedTable};
use crate::sql::physical::type_infer;
use crate::sql::physical::{
    FragmentBuildResult, FragmentEdge, FragmentEdgeKind, MultiFragmentBuildResult, OutputColumn,
};

use crate::sql::ir::{ExprKind, JoinKind, TypedExpr};
use crate::sql::plan::AggregateCall;

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
}

// ---------------------------------------------------------------------------
// Scan/join ownership metadata (used by RF planning)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ScanTupleOwner {
    pub scan_node_id: i32,
    pub fragment_id: FragmentId,
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
    /// Fragment ids for current visit context. Top is active fragment id.
    fragment_stack: Vec<FragmentId>,
    /// Fragments finalized during visitation (child fragments from distribution
    /// boundaries and CTE produce fragments).
    completed_fragments: Vec<FragmentBuildResult>,
    /// Fragment-to-fragment stream/multicast edges.
    completed_edges: Vec<FragmentEdge>,
    /// CTE ID -> index in `completed_fragments`.
    cte_fragments: HashMap<CteId, usize>,
    /// tuple_id -> owning scan node and fragment (for RF target identification).
    pub(crate) scan_tuple_owners: HashMap<i32, ScanTupleOwner>,
    /// hash join node_id -> fragment_id for RF eligibility.
    pub(crate) join_fragment_map: HashMap<i32, FragmentId>,
    /// hash join node_id -> JoinDistribution for RF join mode mapping.
    pub(crate) join_distributions: HashMap<i32, crate::sql::cascades::operator::JoinDistribution>,
}

impl<'a> PlanFragmentBuilder<'a> {
    // -------------------------------------------------------------------
    // Public entry
    // -------------------------------------------------------------------

    pub(crate) fn build(
        plan: &PhysicalPlanNode,
        catalog: &'a dyn CatalogProvider,
        current_database: &str,
    ) -> Result<MultiFragmentBuildResult, String> {
        let mut builder = PlanFragmentBuilder {
            catalog,
            current_database,
            desc_builder: DescriptorTableBuilder::new(),
            scan_tables: Vec::new(),
            next_node_id: 1,
            next_slot_id: 1,
            next_tuple_id: 1,
            next_fragment_id: 0,
            fragment_stack: Vec::new(),
            completed_fragments: Vec::new(),
            completed_edges: Vec::new(),
            cte_fragments: HashMap::new(),
            scan_tuple_owners: HashMap::new(),
            join_fragment_map: HashMap::new(),
            join_distributions: HashMap::new(),
        };

        // Elide a root-level Gather: on a single node the top-level gather
        // adds an unnecessary fragment boundary.
        let plan = match &plan.op {
            Operator::PhysicalDistribution(op)
                if matches!(
                    op.spec,
                    crate::sql::cascades::property::DistributionSpec::Gather
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
        let root_fragment = FragmentBuildResult {
            fragment_id: root_fragment_id,
            plan: plan_nodes::TPlan::new(result.plan_nodes),
            desc_tbl: desc_tbl.clone(),
            exec_params: exec_params.clone(),
            output_sink: build_result_sink(),
            output_columns,
            cte_id: None,
            cte_exchange_nodes: result.cte_exchange_nodes,
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

        // Runtime filter planning pass: identify RF opportunities and patch
        // join nodes with TRuntimeFilterDescription.
        let pipeline_dop = std::thread::available_parallelism()
            .map(|p| p.get().min(4))
            .unwrap_or(4) as i32;
        let rf_plan = super::runtime_filter_planner::plan_runtime_filters(
            &mut fragment_results,
            &builder.scan_tuple_owners,
            &builder.join_fragment_map,
            &builder.join_distributions,
            pipeline_dop,
        );
        let rf_plan = if rf_plan.all_filters.is_empty() {
            None
        } else {
            Some(rf_plan)
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

    fn current_fragment_id(&self) -> Result<FragmentId, String> {
        self.fragment_stack
            .last()
            .copied()
            .ok_or_else(|| "no active fragment id in builder".to_string())
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
            Operator::PhysicalCTEAnchor(op) => self.visit_cte_anchor(op, node),
            Operator::PhysicalCTEProduce(op) => self.visit_cte_produce(op, node),
            Operator::PhysicalCTEConsume(op) => self.visit_cte_consume(op),
            Operator::PhysicalUnion(op) => self.visit_union(op, node),
            Operator::PhysicalIntersect(op) => self.visit_intersect(op, node),
            Operator::PhysicalExcept(op) => self.visit_except(op, node),
            // Logical operators should never appear in an extracted physical plan
            other if other.is_logical() => Err(format!(
                "unexpected logical operator in physical plan: {:?}",
                other
            )),
            other => Err(format!(
                "unhandled operator in fragment builder: {:?}",
                other
            )),
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

        // Track tuple -> scan node ownership for runtime filter planning.
        let current_frag = self.current_fragment_id()?;
        self.scan_tuple_owners.insert(
            scan_tuple_id,
            ScanTupleOwner {
                scan_node_id,
                fragment_id: current_frag,
            },
        );

        Ok(VisitResult {
            plan_nodes: vec![scan_plan_node],
            scope,
            tuple_ids: vec![scan_tuple_id],
            cte_exchange_nodes: Vec::new(),
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
            cte_exchange_nodes: child.cte_exchange_nodes,
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

        // Track join node -> fragment for runtime filter planning.
        if let Ok(frag_id) = self.current_fragment_id() {
            self.join_fragment_map.insert(join_node_id, frag_id);
        }
        self.join_distributions
            .insert(join_node_id, op.distribution.clone());

        // Compile eq conditions.  The eq_conditions pairs come from the SQL
        // text order (e.g. `l_orderkey = o_orderkey`), which may not match the
        // join's left/right child assignment.  Try the natural order first; if
        // the left expr fails against the left scope, swap sides.
        // If BOTH sides fail against the left scope, the pair belongs to a
        // single side (e.g. inner predicates in a SEMI JOIN condition) and is
        // demoted to other_join_conjuncts.
        let mut eq_join_conjuncts = Vec::new();
        let mut demoted_eq_exprs: Vec<TypedExpr> = Vec::new();
        for (expr_a, expr_b) in &op.eq_conditions {
            let result = {
                let try_left_a = ExprCompiler::new(&left.scope).compile_typed(expr_a);
                if let Ok(lt) = try_left_a {
                    ExprCompiler::new(&right.scope)
                        .compile_typed(expr_b)
                        .map(|rt| (lt, rt))
                        .ok()
                } else {
                    // Swap: expr_a might be from the right child, expr_b from the left.
                    let try_left_b = ExprCompiler::new(&left.scope).compile_typed(expr_b);
                    if let Ok(lt) = try_left_b {
                        ExprCompiler::new(&right.scope)
                            .compile_typed(expr_a)
                            .map(|rt| (lt, rt))
                            .ok()
                    } else {
                        None
                    }
                }
            };
            if let Some((lt, rt)) = result {
                eq_join_conjuncts.push(plan_nodes::TEqJoinCondition {
                    left: lt,
                    right: rt,
                    opcode: Some(crate::opcodes::TExprOpcode::EQ),
                });
            } else {
                // Both sides from the same input — demote to other_condition.
                demoted_eq_exprs.push(TypedExpr {
                    kind: ExprKind::BinaryOp {
                        left: Box::new(expr_a.clone()),
                        op: crate::sql::ir::BinOp::Eq,
                        right: Box::new(expr_b.clone()),
                    },
                    data_type: DataType::Boolean,
                    nullable: false,
                });
            }
        }

        // Compile other conditions (including demoted eq pairs).
        let mut other_join_conjuncts = Vec::new();
        {
            let mut merged = ExprScope::new();
            merged.merge(&left.scope);
            merged.merge(&right.scope);
            let mut compiler = ExprCompiler::new(&merged);
            if let Some(ref cond) = op.other_condition {
                other_join_conjuncts.push(compiler.compile_typed(cond)?);
            }
            for demoted in &demoted_eq_exprs {
                other_join_conjuncts.push(compiler.compile_typed(demoted)?);
            }
        }

        let join_plan_node = nodes::build_hash_join_node(
            join_node_id,
            &left.tuple_ids,
            &right.tuple_ids,
            join_op,
            eq_join_conjuncts,
            other_join_conjuncts,
        );

        // Widen nullable for join nullable side tuples so that the Arrow
        // schema allows NULL values from unmatched rows.  SEMI joins also
        // need widening: the runtime emits null-padded columns for the
        // pruned side (see `extend_with_null_build_columns` /
        // `extend_with_null_probe_columns`), and downstream operators
        // reference those slots.
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

        // SEMI/ANTI joins only output columns from the surviving side.
        let (merged_scope, merged_tuple_ids) = match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti => (left.scope, left.tuple_ids),
            JoinKind::RightSemi | JoinKind::RightAnti => (right.scope, right.tuple_ids),
            _ => {
                let mut scope = left.scope;
                scope.merge(&right.scope);
                let mut tids = left.tuple_ids;
                tids.extend(right.tuple_ids);
                (scope, tids)
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

        // SEMI/ANTI joins only output columns from the surviving side.
        let (merged_scope, merged_tuple_ids) = match op.join_type {
            JoinKind::LeftSemi | JoinKind::LeftAnti => (left.scope, left.tuple_ids),
            JoinKind::RightSemi | JoinKind::RightAnti => (right.scope, right.tuple_ids),
            _ => {
                let mut scope = left.scope;
                scope.merge(&right.scope);
                let mut tids = left.tuple_ids;
                tids.extend(right.tuple_ids);
                (scope, tids)
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
            cte_exchange_nodes: child.cte_exchange_nodes,
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
            cte_exchange_nodes: child.cte_exchange_nodes,
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
            cte_exchange_nodes: child.cte_exchange_nodes,
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
            cte_exchange_nodes: Vec::new(),
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
            name: op.alias.as_deref().unwrap_or("generate_series").to_string(),
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
            cte_exchange_nodes: child.cte_exchange_nodes,
        })
    }

    // -------------------------------------------------------------------
    // visit_distribution
    // -------------------------------------------------------------------

    fn build_output_partition(
        &self,
        spec: &crate::sql::cascades::property::DistributionSpec,
        child_scope: &ExprScope,
    ) -> Result<partitions::TDataPartition, String> {
        match spec {
            crate::sql::cascades::property::DistributionSpec::Gather => {
                Ok(unpartitioned_stream_partition())
            }
            crate::sql::cascades::property::DistributionSpec::HashPartitioned(cols) => {
                let mut partition_exprs = Vec::with_capacity(cols.len());
                for col in cols {
                    let binding = child_scope
                        .resolve_column(col.qualifier.as_deref(), &col.column)?
                        .clone();
                    let type_desc = type_infer::arrow_type_to_type_desc(&binding.data_type)?;
                    partition_exprs.push(expr_compiler::build_slot_ref_texpr(
                        binding.slot_id,
                        binding.tuple_id,
                        type_desc,
                    ));
                }
                Ok(partitions::TDataPartition::new(
                    partitions::TPartitionType::HASH_PARTITIONED,
                    Some(partition_exprs),
                    None::<Vec<partitions::TRangePartition>>,
                    None::<Vec<partitions::TBucketProperty>>,
                ))
            }
            crate::sql::cascades::property::DistributionSpec::Any => {
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
        } = child;

        let output_partition = self.build_output_partition(&op.spec, &scope)?;
        let exchange_partition_type = output_partition.type_.clone();

        self.completed_fragments.push(FragmentBuildResult {
            fragment_id: child_fragment_id,
            plan: plan_nodes::TPlan::new(plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(&[])?,
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

        let mut output_scope = ExprScope::new();
        let first_child_cols: Vec<(String, ColumnBinding)> = child_results[0]
            .scope
            .iter_columns()
            .map(|(name, binding)| (name.clone(), binding.clone()))
            .collect();

        for (idx, (name, child_binding)) in first_child_cols.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                output_tuple_id,
                name,
                &child_binding.data_type,
                child_binding.nullable,
                idx as i32,
            );
            output_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: output_tuple_id,
                    slot_id,
                    data_type: child_binding.data_type.clone(),
                    nullable: child_binding.nullable,
                },
            );
        }
        self.desc_builder.add_tuple(output_tuple_id);

        let mut result_expr_lists = Vec::with_capacity(child_results.len());
        for child_result in &child_results {
            let mut expr_list = Vec::new();
            for (col_idx, (_, child_binding)) in child_result.scope.iter_columns().enumerate() {
                let output_type = first_child_cols.get(col_idx).map(|(_, b)| &b.data_type);
                let needs_cast = matches!(child_binding.data_type, DataType::Null)
                    && output_type.is_some_and(|t| !matches!(t, DataType::Null));
                if needs_cast {
                    let target_type = output_type.unwrap();
                    let target_desc = type_infer::arrow_type_to_type_desc(target_type)?;
                    let child_desc = type_infer::arrow_type_to_type_desc(&child_binding.data_type)?;
                    let slot_ref = expr_compiler::build_slot_ref_texpr(
                        child_binding.slot_id,
                        child_binding.tuple_id,
                        child_desc,
                    );
                    expr_list.push(expr_compiler::build_cast_texpr(slot_ref, target_desc));
                } else {
                    let type_desc = type_infer::arrow_type_to_type_desc(&child_binding.data_type)?;
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
            let type_desc = type_infer::arrow_type_to_type_desc(&binding.data_type)?;
            let texpr =
                expr_compiler::build_slot_ref_texpr(binding.slot_id, binding.tuple_id, type_desc);
            grouping_exprs.push(texpr);

            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                agg_tuple_id,
                name,
                &binding.data_type,
                binding.nullable,
                idx as i32,
            );
            agg_scope.add_column(
                None,
                name.clone(),
                ColumnBinding {
                    tuple_id: agg_tuple_id,
                    slot_id,
                    data_type: binding.data_type.clone(),
                    nullable: binding.nullable,
                },
            );
        }

        self.desc_builder.add_tuple(agg_tuple_id);
        let agg_plan_node = nodes::build_aggregation_node(
            agg_node_id,
            agg_tuple_id,
            agg_tuple_id,
            grouping_exprs,
            vec![],
        );

        let mut plan_nodes = vec![agg_plan_node];
        plan_nodes.extend(child.plan_nodes);

        Ok(VisitResult {
            plan_nodes,
            scope: agg_scope,
            tuple_ids: vec![agg_tuple_id],
            cte_exchange_nodes: child.cte_exchange_nodes,
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
        let cte_fragment = FragmentBuildResult {
            fragment_id: cte_fragment_id,
            plan: plan_nodes::TPlan::new(child.plan_nodes),
            desc_tbl: DescriptorTableBuilder::new().build(),
            exec_params: nodes::build_exec_params_multi(&[])?,
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
        };
        let idx = self.completed_fragments.len();
        self.completed_fragments.push(cte_fragment);
        self.cte_fragments.insert(op.cte_id, idx);

        Ok(VisitResult {
            plan_nodes: Vec::new(),
            scope: child.scope,
            tuple_ids: child.tuple_ids,
            cte_exchange_nodes: Vec::new(),
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
                nullable: col.nullable,
            };
            scope.add_column(None, col.name.clone(), binding.clone());
            // Also register with the CTE alias as qualifier
            scope.add_column(Some(op.alias.clone()), col.name.clone(), binding);
        }
        self.desc_builder.add_tuple(exchange_tuple_id);

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
        })
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
    use std::collections::HashMap;
    use std::path::PathBuf;

    use arrow::datatypes::DataType;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::plan_nodes;
    use crate::sql::cascades::operator::{
        Operator, PhysicalDistributionOp, PhysicalScanOp, PhysicalSortOp,
    };
    use crate::sql::cascades::physical_plan::PhysicalPlanNode;
    use crate::sql::cascades::property::DistributionSpec;
    use crate::sql::catalog::{CatalogProvider, ColumnDef, TableDef, TableStorage};
    use crate::sql::ir::{ExprKind, OutputColumn, SortItem, TypedExpr};
    use crate::sql::statistics::Statistics;

    struct DummyCatalog;

    impl CatalogProvider for DummyCatalog {
        fn get_table(&self, _database: &str, _table: &str) -> Result<TableDef, String> {
            Err("not used in scan-only builder tests".to_string())
        }
    }

    fn output_columns() -> Vec<OutputColumn> {
        vec![OutputColumn {
            name: "id".to_string(),
            data_type: DataType::Int32,
            nullable: false,
        }]
    }

    fn id_expr() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: "id".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn stats() -> Statistics {
        Statistics {
            output_row_count: 3.0,
            column_statistics: HashMap::new(),
        }
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
                    }],
                    storage: TableStorage::LocalParquetFile { path },
                },
                alias: None,
                columns: output_columns(),
                predicates: vec![],
                required_columns: None,
            }),
            children: vec![],
            stats: stats(),
            output_columns: output_columns(),
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
            }),
            children: vec![PhysicalPlanNode {
                op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                    spec: DistributionSpec::Gather,
                }),
                children: vec![scan_plan(file.path().to_path_buf())],
                stats: stats(),
                output_columns: output_columns(),
            }],
            stats: stats(),
            output_columns: output_columns(),
        };

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");

        assert_eq!(build.fragment_results.len(), 2);
        assert_eq!(build.edges.len(), 1);
        assert!(matches!(
            build.edges[0].edge_kind,
            crate::sql::physical::FragmentEdgeKind::Stream
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
                }],
                stats: stats(),
                output_columns: output_columns(),
            }],
            stats: stats(),
            output_columns: output_columns(),
        };

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
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
                && matches!(e.edge_kind, crate::sql::physical::FragmentEdgeKind::Stream)
        }));
    }

    #[test]
    fn build_maps_hash_distribution_to_hash_partitioned_edge() {
        let file = NamedTempFile::new().expect("temp parquet path");
        let plan = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::HashPartitioned(vec![
                    crate::sql::cascades::property::ColumnRef {
                        qualifier: None,
                        column: "id".to_string(),
                    },
                ]),
            }),
            children: vec![scan_plan(file.path().to_path_buf())],
            stats: stats(),
            output_columns: output_columns(),
        };

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
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
        };

        let result = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default");
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
        };

        let build = PlanFragmentBuilder::build(&plan, &DummyCatalog, "default").expect("build");
        assert_eq!(build.fragment_results.len(), 1);
        assert!(build.edges.is_empty());
    }
}
