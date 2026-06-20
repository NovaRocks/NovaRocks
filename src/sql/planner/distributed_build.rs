//! Bridge 2: PhysicalPlanNode/PhysicalOperator to DistributedPlanNode.
//!
//! This bridge materializes optimizer scalars into typed plan nodes and splits
//! the tree into distributed fragments.

use std::collections::HashMap;

use crate::partitions;
use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{ExprKind, TypedExpr};
use crate::sql::codegen::helpers::{group_win_exprs_by_sig, split_and_conjuncts_typed};
use crate::sql::codegen::{FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind};
use crate::sql::optimizer::cost::{CostInput, CostOptions, compute_cost_estimate};
use crate::sql::optimizer::derive::PropertyAlternativeKind;
use crate::sql::optimizer::operator::{
    CTEAnchorOp, CTEConsumeOp, CTEProduceOp, LimitOp, Operator, PhysicalDistributionOp, TopNOp,
    TopNPhase, UnionOp,
};
use crate::sql::optimizer::physical_plan::PhysicalPlanNode;
use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::optimizer_bridge::property::{
    ordering_spec_from_sort_items, window_ordering_spec,
};
use crate::sql::planner::optimizer_bridge::scalar::{
    materialize, materialize_aggregate_calls, materialize_exprs, materialize_project_items,
    materialize_sort_keys, materialize_window_exprs,
};
use crate::sql::optimizer::statistics::Statistics;
use crate::sql::planner::plan::{
    DistributedExchangeNode, DistributedHashAggregateNode, DistributedHashJoinEqCondition,
    DistributedHashJoinNode, DistributedNestLoopJoinNode, DistributedSetOpNode,
    DistributedTopNNode, ExchangeFlavor, PlanAssertOneRowNode as DistributedAssertOneRowNode,
    PlanDecodeNode as DistributedDecodeNode, PlanFilterNode as DistributedFilterNode,
    PlanGenerateSeriesNode as DistributedGenerateSeriesNode,
    PlanProjectNode as DistributedProjectNode, PlanRepeatNode as DistributedRepeatNode,
    PlanScanNode as DistributedScanNode, PlanSetOpKind as SetOpKind,
    PlanSortNode as DistributedSortNode, PlanTableFunctionNode as DistributedTableFunctionNode,
    PlanValuesNode as DistributedValuesNode, PlanWindowNode as DistributedWindowNode,
};

use super::distributed_fragment::{
    DataPartition, DataSink, DistributedPlan, PartitionKind, PlanFragment,
};
use super::distributed_node::{DistributedPlanNode, PlanNodeKind, PlanNodeStats};

struct DistributedPlanBuilder<'a> {
    scalars: &'a ScalarArena,
    next_node_id: i32,
    next_tuple_id: i32,
    next_fragment_id: FragmentId,
    fragment_stack: Vec<FragmentId>,
    completed_fragments: Vec<PlanFragment>,
    edges: Vec<FragmentEdge>,
    #[allow(dead_code)]
    cte_fragments: HashMap<CteId, usize>,
}

fn stats_for_physical_node(node: &PhysicalPlanNode) -> PlanNodeStats {
    let child_stats: Vec<&Statistics> = node.children.iter().map(|child| &child.stats).collect();
    let child_outputs: Vec<&PhysicalPropertySet> = node
        .execution_props
        .child_output_properties
        .iter()
        .collect();
    let options = CostOptions::default();
    let alt_kind = match node.execution_props.join_distribution {
        Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Broadcast) => {
            PropertyAlternativeKind::BroadcastJoin
        }
        Some(crate::sql::optimizer::physical_plan::JoinExecutionDistribution::Partitioned) => {
            PropertyAlternativeKind::ShuffleJoin
        }
        _ => PropertyAlternativeKind::Default,
    };
    let input = CostInput {
        op: &node.op,
        own_stats: &node.stats,
        child_stats: &child_stats,
        child_outputs: &child_outputs,
        required_output: &node.execution_props.output_property,
        alt_kind: &alt_kind,
        scalars: node.execution_props.scalar_arena.as_deref(),
        options: &options,
    };
    PlanNodeStats::from_statistics_with_cost(&node.stats, Some(compute_cost_estimate(&input)))
}

impl<'a> DistributedPlanBuilder<'a> {
    fn alloc_node(&mut self) -> i32 {
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        node_id
    }

    fn alloc_tuple(&mut self) -> i32 {
        let tuple_id = self.next_tuple_id;
        self.next_tuple_id += 1;
        tuple_id
    }

    fn alloc_fragment_id(&mut self) -> FragmentId {
        let fragment_id = self.next_fragment_id;
        self.next_fragment_id += 1;
        fragment_id
    }

    fn current_fragment_id(&self) -> Result<FragmentId, String> {
        self.fragment_stack
            .last()
            .copied()
            .ok_or_else(|| "build_distributed_plan has no active fragment id".to_string())
    }

    fn visit(&mut self, node: &PhysicalPlanNode) -> Result<DistributedPlanNode, String> {
        let fragment_id = self.current_fragment_id()?;
        match &node.op {
            Operator::PhysicalScan(op) => {
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::Scan(DistributedScanNode {
                        database: op.database.clone(),
                        table: op.table.clone(),
                        alias: op.alias.clone(),
                        columns: op.columns.clone(),
                        predicates: materialize_exprs(self.scalars, &op.predicates),
                        required_columns: op.required_columns.clone(),
                        dict_columns: op.dict_columns.clone(),
                        variant_columns: op.variant_columns.clone(),
                        mv_rewritten_from: op.mv_rewritten_from.clone(),
                    }),
                })
            }
            Operator::PhysicalFilter(op) => {
                let child_plan = expect_single_child(node, "PhysicalFilter")?;
                let mut child = self.visit(child_plan)?;
                let predicate = materialize(self.scalars, op.predicate);
                if fold_filter_into_scan(&mut child, &predicate) {
                    child.stats = PlanNodeStats::from_statistics_with_cost(
                        &node.stats,
                        child.stats.cost_estimate.clone(),
                    );
                    child
                        .probe_runtime_filters
                        .extend(node.probe_runtime_filters.clone());
                    Ok(child)
                } else {
                    let node_id = self.alloc_node();
                    Ok(DistributedPlanNode {
                        node_id,
                        fragment_id,
                        tuple_ids: child.tuple_ids.clone(),
                        nullable_tuple_ids: vec![],
                        limit: -1,
                        execution_join_distribution: node.execution_props.join_distribution,
                        build_runtime_filters: node.build_runtime_filters.clone(),
                        probe_runtime_filters: node.probe_runtime_filters.clone(),
                        children: vec![child],
                        stats: stats_for_physical_node(node),
                        kind: PlanNodeKind::Filter(DistributedFilterNode { predicate }),
                    })
                }
            }
            Operator::PhysicalProject(op) => {
                let child_plan = expect_single_child(node, "PhysicalProject")?;
                let child = self.visit(child_plan)?;
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::Project(DistributedProjectNode {
                        items: materialize_project_items(self.scalars, &op.items),
                        output_qualifier: op.output_qualifier.clone(),
                    }),
                })
            }
            Operator::PhysicalSort(op) => {
                let child_plan = expect_single_child(node, "PhysicalSort")?;
                let child = self.visit(child_plan)?;
                let node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: child.tuple_ids.clone(),
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::Sort(DistributedSortNode {
                        items: materialize_sort_keys(self.scalars, &op.items),
                        analytic_partition_by: materialize_exprs(
                            self.scalars,
                            &op.analytic_partition_exprs,
                        ),
                        output_columns: node.output_columns.clone(),
                        offset: None,
                        partition_limit: op.partition_limit,
                        topn_type: op.topn_type,
                    }),
                })
            }
            Operator::PhysicalLimit(op) => {
                let child_plan = expect_single_child(node, "PhysicalLimit")?;
                let offset = op.offset.unwrap_or(0);
                if offset > 0 && !limit_child_can_apply_offset_locally(child_plan) {
                    return self.visit_limit_offset_exchange(op, node);
                }

                let mut child = self.visit(child_plan)?;
                child.limit = op.limit.unwrap_or(-1);
                child.stats = PlanNodeStats::from_statistics_with_cost(
                    &node.stats,
                    child.stats.cost_estimate.clone(),
                );
                match &mut child.kind {
                    PlanNodeKind::Sort(sort) => {
                        sort.offset = op.offset;
                    }
                    PlanNodeKind::TopN(topn) => {
                        topn.limit = op.limit;
                        topn.offset = op.offset;
                    }
                    _ if offset > 0 => {
                        return Err(
                            "LIMIT/OFFSET without a local SORT/TOPN child is not supported"
                                .to_string(),
                        );
                    }
                    _ => {}
                }
                Ok(child)
            }
            Operator::PhysicalTopN(op) => {
                let child_plan = expect_single_child(node, "PhysicalTopN")?;
                match (op.phase, op.is_split) {
                    (TopNPhase::Final, true) => self.visit_physical_top_n_final_split(op, node),
                    (TopNPhase::Final, false) | (TopNPhase::Partial, _) => {
                        let child = self.visit(child_plan)?;
                        let node_id = self.alloc_node();
                        Ok(DistributedPlanNode {
                            node_id,
                            fragment_id,
                            tuple_ids: child.tuple_ids.clone(),
                            nullable_tuple_ids: vec![],
                            limit: op.limit.unwrap_or(-1),
                            execution_join_distribution: node.execution_props.join_distribution,
                            build_runtime_filters: node.build_runtime_filters.clone(),
                            probe_runtime_filters: node.probe_runtime_filters.clone(),
                            children: vec![child],
                            stats: stats_for_physical_node(node),
                            kind: PlanNodeKind::TopN(DistributedTopNNode {
                                items: materialize_sort_keys(self.scalars, &op.items),
                                limit: op.limit,
                                offset: op.offset,
                                phase: op.phase,
                                is_split: op.is_split,
                            }),
                        })
                    }
                }
            }
            Operator::PhysicalHashAggregate(op) => {
                let child_plan = expect_single_child(node, "PhysicalHashAggregate")?;
                let child = self.visit(child_plan)?;
                let agg_tuple_id = self.alloc_tuple();
                let agg_node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id: agg_node_id,
                    fragment_id,
                    tuple_ids: vec![agg_tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::HashAggregate(Box::new(DistributedHashAggregateNode {
                        mode: op.mode,
                        group_by: materialize_exprs(self.scalars, &op.group_by),
                        aggregates: materialize_aggregate_calls(
                            self.scalars,
                            &op.aggregates,
                            op.group_by.len(),
                            &op.output_columns,
                        ),
                        is_merge: op.is_merge.clone(),
                        output_columns: op.output_columns.clone(),
                    })),
                })
            }
            Operator::PhysicalCTEAnchor(op) => self.visit_cte_anchor(op, node),
            Operator::PhysicalCTEProduce(op) => {
                self.visit_cte_produce(op, node)?;
                Err(
                    "PhysicalCTEProduce emits no DistributedPlan node outside CTEAnchor"
                        .to_string(),
                )
            }
            Operator::PhysicalCTEConsume(op) => self.visit_cte_consume(op, node),
            Operator::PhysicalDistribution(op) => self.visit_distribution(op, node),
            Operator::PhysicalHashJoin(op) => {
                let (left_plan, right_plan) = expect_binary_children(node, "PhysicalHashJoin")?;
                let left = self.visit(left_plan)?;
                let right = self.visit(right_plan)?;
                let node_id = self.alloc_node();
                let mut tuple_ids = left.tuple_ids.clone();
                tuple_ids.extend(right.tuple_ids.iter().copied());
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids,
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![left, right],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::HashJoin(Box::new(DistributedHashJoinNode {
                        join_type: op.join_type,
                        eq_conditions: op
                            .eq_conditions
                            .iter()
                            .map(|eq| DistributedHashJoinEqCondition {
                                left: materialize(self.scalars, eq.left),
                                right: materialize(self.scalars, eq.right),
                                null_safe: eq.null_safe,
                            })
                            .collect(),
                        other_condition: op
                            .other_condition
                            .map(|condition| materialize(self.scalars, condition)),
                        distribution: op.distribution.clone(),
                    })),
                })
            }
            Operator::PhysicalNestLoopJoin(op) => {
                let (left_plan, right_plan) = expect_binary_children(node, "PhysicalNestLoopJoin")?;
                let left = self.visit(left_plan)?;
                let right = self.visit(right_plan)?;
                let node_id = self.alloc_node();
                let mut tuple_ids = left.tuple_ids.clone();
                tuple_ids.extend(right.tuple_ids.iter().copied());
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids,
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![left, right],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::NestLoopJoin(DistributedNestLoopJoinNode {
                        join_type: op.join_type,
                        condition: op
                            .condition
                            .map(|condition| materialize(self.scalars, condition)),
                    }),
                })
            }
            Operator::PhysicalValues(op) => {
                if !node.children.is_empty() {
                    return Err(format!(
                        "build_distributed_plan M0: PhysicalValues expected 0 children, got {}",
                        node.children.len()
                    ));
                }
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::Values(DistributedValuesNode {
                        rows: op
                            .rows
                            .iter()
                            .map(|row| materialize_exprs(self.scalars, row))
                            .collect(),
                        columns: op.columns.clone(),
                    }),
                })
            }
            Operator::PhysicalAssertOneRow(op) => {
                let child_plan = expect_single_child(node, "PhysicalAssertOneRow")?;
                let child = self.visit(child_plan)?;
                let node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: child.tuple_ids.clone(),
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::AssertOneRow(DistributedAssertOneRowNode {
                        subquery_text: op.subquery_text.clone(),
                    }),
                })
            }
            Operator::PhysicalDecode(op) => {
                let child_plan = expect_single_child(node, "PhysicalDecode")?;
                let child = self.visit(child_plan)?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids: vec![tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::Decode(DistributedDecodeNode {
                        mappings: op.mappings.clone(),
                        output_columns: op.output_columns.clone(),
                    }),
                })
            }
            Operator::PhysicalRepeat(op) => {
                let child_plan = expect_single_child(node, "PhysicalRepeat")?;
                let child = self.visit(child_plan)?;
                let node_id = self.alloc_node();
                let virtual_tuple_id = self.alloc_tuple();
                let mut tuple_ids = child.tuple_ids.clone();
                if !op.grouping_fn_args.is_empty() {
                    tuple_ids.push(virtual_tuple_id);
                }
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids,
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::Repeat(DistributedRepeatNode {
                        virtual_tuple_id: Some(virtual_tuple_id),
                        repeat_column_ref_list: op.repeat_column_ref_list.clone(),
                        repeat_column_ref_ids: op.repeat_column_ref_ids.clone(),
                        grouping_ids: op.grouping_ids.clone(),
                        all_rollup_columns: op.all_rollup_columns.clone(),
                        all_rollup_column_ids: op.all_rollup_column_ids.clone(),
                        grouping_key_aliases: op.grouping_key_aliases.clone(),
                        grouping_fn_args: op.grouping_fn_args.clone(),
                        grouping_fn_arg_ids: op.grouping_fn_arg_ids.clone(),
                        grouping_fn_ids: op.grouping_fn_ids.clone(),
                    }),
                })
            }
            Operator::PhysicalWindow(op) => {
                let child_plan = expect_single_child(node, "PhysicalWindow")?;
                let child = self.visit(child_plan)?;
                let window_exprs =
                    materialize_window_exprs(self.scalars, &op.window_exprs, &op.output_columns);
                let groups = group_win_exprs_by_sig(&window_exprs);
                if groups.is_empty() {
                    return Err(
                        "build_distributed_plan M0: PhysicalWindow has no window expressions"
                            .to_string(),
                    );
                }

                let mut first_node_id = None;
                let mut tuple_ids = child.tuple_ids.clone();
                let mut current_ordering = distributed_node_ordering(&child);
                for group_indices in &groups {
                    let Some(first_idx) = group_indices.first().copied() else {
                        continue;
                    };
                    let first_win = &window_exprs[first_idx];
                    if groups.len() > 1 {
                        let required_ordering =
                            window_ordering_spec(&first_win.partition_by, &first_win.order_by);
                        let has_sort_keys =
                            !first_win.partition_by.is_empty() || !first_win.order_by.is_empty();
                        let ordering_is_representable =
                            !matches!(required_ordering, OrderingSpec::Any);
                        let needs_sort = has_sort_keys
                            && (!ordering_is_representable
                                || !current_ordering.satisfies(&required_ordering));
                        if needs_sort {
                            let sort_node_id = self.alloc_node();
                            first_node_id.get_or_insert(sort_node_id);
                            current_ordering = required_ordering;
                        }
                    }
                    let analytic_node_id = self.alloc_node();
                    first_node_id.get_or_insert(analytic_node_id);
                    let _ = self.alloc_tuple();
                    let output_tuple_id = self.alloc_tuple();
                    tuple_ids.push(output_tuple_id);
                }

                let node_id = first_node_id.ok_or_else(|| {
                    "build_distributed_plan M0: PhysicalWindow produced no thrift node".to_string()
                })?;
                Ok(DistributedPlanNode {
                    node_id,
                    fragment_id,
                    tuple_ids,
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::Window(DistributedWindowNode {
                        window_exprs,
                        output_columns: op.output_columns.clone(),
                    }),
                })
            }
            Operator::PhysicalUnion(op) => {
                if op.all {
                    return self.visit_set_op(
                        node,
                        SetOpKind::UnionAll,
                        &op.output_columns,
                        &op.child_output_columns,
                    );
                }
                self.visit_union_distinct(op, node)
            }
            Operator::PhysicalIntersect(op) => self.visit_set_op(
                node,
                SetOpKind::Intersect,
                &op.output_columns,
                &op.child_output_columns,
            ),
            Operator::PhysicalExcept(op) => self.visit_set_op(
                node,
                SetOpKind::Except,
                &op.output_columns,
                &op.child_output_columns,
            ),
            Operator::PhysicalGenerateSeries(op) => {
                if !node.children.is_empty() {
                    return Err(format!(
                        "build_distributed_plan M0: PhysicalGenerateSeries expected 0 children, got {}",
                        node.children.len()
                    ));
                }
                if op.step == 0 {
                    return Err("generate_series step size cannot equal zero".to_string());
                }
                let _ = self.alloc_tuple();
                let _ = self.alloc_node();
                let output_tuple_id = self.alloc_tuple();
                let table_fn_node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id: table_fn_node_id,
                    fragment_id,
                    tuple_ids: vec![output_tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::GenerateSeries(DistributedGenerateSeriesNode {
                        start: op.start,
                        end: op.end,
                        step: op.step,
                        column_name: op.column_name.clone(),
                        alias: op.alias.clone(),
                        output_column_id: op.output_column_id,
                    }),
                })
            }
            Operator::PhysicalTableFunction(op) => {
                let child_plan = expect_single_child(node, "PhysicalTableFunction")?;
                let child = self.visit(child_plan)?;
                let _ = self.alloc_tuple();
                let _ = self.alloc_node();
                let output_tuple_id = self.alloc_tuple();
                let table_fn_node_id = self.alloc_node();
                Ok(DistributedPlanNode {
                    node_id: table_fn_node_id,
                    fragment_id,
                    tuple_ids: vec![output_tuple_id],
                    nullable_tuple_ids: vec![],
                    limit: -1,
                    execution_join_distribution: node.execution_props.join_distribution,
                    build_runtime_filters: node.build_runtime_filters.clone(),
                    probe_runtime_filters: node.probe_runtime_filters.clone(),
                    children: vec![child],
                    stats: stats_for_physical_node(node),
                    kind: PlanNodeKind::TableFunction(DistributedTableFunctionNode {
                        function_name: op.function_name.clone(),
                        args: materialize_exprs(self.scalars, &op.args),
                        output_columns: op.output_columns.clone(),
                        alias: op.alias.clone(),
                        is_left_join: op.is_left_join,
                    }),
                })
            }
            other => Err(format!(
                "build_distributed_plan slice 1 does not handle operator {other:?}"
            )),
        }
    }

    fn visit_distribution(
        &mut self,
        op: &PhysicalDistributionOp,
        node: &PhysicalPlanNode,
    ) -> Result<DistributedPlanNode, String> {
        if node.children.len() != 1 {
            return Err(format!(
                "PhysicalDistribution expected exactly 1 child, got {}",
                node.children.len()
            ));
        }
        if matches!(op.spec, DistributionSpec::Gather)
            && let Operator::PhysicalLimit(limit_op) = &node.children[0].op
        {
            return self.visit_gather_distribution_over_limit(limit_op, &node.children[0], node);
        }
        let child_plan = &node.children[0];
        let output_partition =
            data_partition_for_distribution_spec(&op.spec, &child_plan.output_columns)?;
        let partition_type = partition_type_for_data_partition(&output_partition);
        let partition_exprs = output_partition.exprs.clone();
        self.emit_fragment_exchange(
            node,
            child_plan,
            output_partition,
            stream_kind_for_distribution(&op.spec),
            ExchangeFlavor::Distribution,
            -1,
            Vec::new(),
            stats_for_physical_node(node),
            |source_fragment_id| DistributedExchangeNode {
                partition_type,
                partition_exprs,
                source_fragment_id,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: ExchangeFlavor::Distribution,
            },
        )
    }

    fn visit_gather_distribution_over_limit(
        &mut self,
        op: &LimitOp,
        limit_node: &PhysicalPlanNode,
        distribution_node: &PhysicalPlanNode,
    ) -> Result<DistributedPlanNode, String> {
        let child_plan = expect_single_child(limit_node, "PhysicalLimit below Gather")?;
        let output_partition = data_partition_for_distribution_spec(
            &DistributionSpec::Gather,
            &limit_node.output_columns,
        )?;
        let partition_type = partition_type_for_data_partition(&output_partition);
        self.emit_fragment_exchange(
            distribution_node,
            child_plan,
            output_partition,
            FragmentStreamKind::Gather,
            ExchangeFlavor::LimitOffset {
                limit: op.limit,
                offset: op.offset,
            },
            op.limit.unwrap_or(-1),
            Vec::new(),
            stats_for_physical_node(distribution_node),
            |source_fragment_id| DistributedExchangeNode {
                partition_type,
                partition_exprs: Vec::new(),
                source_fragment_id,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: ExchangeFlavor::LimitOffset {
                    limit: op.limit,
                    offset: op.offset,
                },
            },
        )
    }

    fn visit_limit_offset_exchange(
        &mut self,
        op: &LimitOp,
        node: &PhysicalPlanNode,
    ) -> Result<DistributedPlanNode, String> {
        let child_plan = expect_single_child(node, "PhysicalLimit")?;
        let output_partition = data_partition_for_distribution_spec(
            &DistributionSpec::Gather,
            &child_plan.output_columns,
        )?;
        let partition_type = partition_type_for_data_partition(&output_partition);
        self.emit_fragment_exchange(
            node,
            child_plan,
            output_partition,
            FragmentStreamKind::Gather,
            ExchangeFlavor::LimitOffset {
                limit: op.limit,
                offset: op.offset,
            },
            op.limit.unwrap_or(-1),
            Vec::new(),
            PlanNodeStats::from_statistics(&node.stats),
            |source_fragment_id| DistributedExchangeNode {
                partition_type,
                partition_exprs: Vec::new(),
                source_fragment_id,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: ExchangeFlavor::LimitOffset {
                    limit: op.limit,
                    offset: op.offset,
                },
            },
        )
    }

    fn visit_physical_top_n_final_split(
        &mut self,
        op: &TopNOp,
        node: &PhysicalPlanNode,
    ) -> Result<DistributedPlanNode, String> {
        let child_plan = expect_single_child(node, "PhysicalTopN")?;
        let output_partition = data_partition_for_distribution_spec(
            &DistributionSpec::Gather,
            &child_plan.output_columns,
        )?;
        let partition_type = partition_type_for_data_partition(&output_partition);
        let topn_items = materialize_sort_keys(self.scalars, &op.items);
        self.emit_fragment_exchange(
            node,
            child_plan,
            output_partition,
            FragmentStreamKind::Gather,
            ExchangeFlavor::TopNSplit {
                items: topn_items.clone(),
                limit: op.limit,
                offset: op.offset,
            },
            op.limit.unwrap_or(-1),
            Vec::new(),
            PlanNodeStats::from_statistics(&node.stats),
            |source_fragment_id| DistributedExchangeNode {
                partition_type,
                partition_exprs: Vec::new(),
                source_fragment_id,
                output_columns: Vec::new(),
                output_qualifier: None,
                flavor: ExchangeFlavor::TopNSplit {
                    items: topn_items,
                    limit: op.limit,
                    offset: op.offset,
                },
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_fragment_exchange(
        &mut self,
        node: &PhysicalPlanNode,
        child_plan: &PhysicalPlanNode,
        output_partition: DataPartition,
        stream_kind: FragmentStreamKind,
        flavor_for_validation: ExchangeFlavor,
        limit: i64,
        exchange_output_columns: Vec<crate::sql::analysis::OutputColumn>,
        exchange_stats: PlanNodeStats,
        build_exchange: impl FnOnce(FragmentId) -> DistributedExchangeNode,
    ) -> Result<DistributedPlanNode, String> {
        let parent_fragment_id = self.current_fragment_id()?;
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(child_plan);
        self.fragment_stack.pop();
        let child = child_result?;

        let partition_type = partition_type_for_data_partition(&output_partition);
        let exchange_node_id = self.alloc_node();
        let edge_output_columns = child_plan.output_columns.clone();

        self.completed_fragments.push(PlanFragment {
            fragment_id: child_fragment_id,
            root: child.clone(),
            data_partition: DataPartition::unpartitioned(),
            output_partition: output_partition.clone(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: edge_output_columns,
            cte_id: None,
            cte_exchange_nodes: collect_cte_exchange_nodes(&child),
        });

        self.edges.push(FragmentEdge {
            source_fragment_id: child_fragment_id,
            target_fragment_id: parent_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: tdata_partition_placeholder(partition_type),
            stream_kind,
            edge_kind: FragmentEdgeKind::Stream,
        });

        let mut exchange = build_exchange(child_fragment_id);
        exchange.output_columns = exchange_output_columns;
        exchange.flavor = flavor_for_validation;
        Ok(DistributedPlanNode {
            node_id: exchange_node_id,
            fragment_id: parent_fragment_id,
            tuple_ids: child.tuple_ids.clone(),
            nullable_tuple_ids: vec![],
            limit,
            execution_join_distribution: node.execution_props.join_distribution,
            build_runtime_filters: node.build_runtime_filters.clone(),
            probe_runtime_filters: node.probe_runtime_filters.clone(),
            children: vec![],
            stats: exchange_stats,
            kind: PlanNodeKind::Exchange(exchange),
        })
    }

    fn visit_cte_anchor(
        &mut self,
        _op: &CTEAnchorOp,
        node: &PhysicalPlanNode,
    ) -> Result<DistributedPlanNode, String> {
        let (produce, consume) = expect_binary_children(node, "PhysicalCTEAnchor")?;
        let Operator::PhysicalCTEProduce(produce_op) = &produce.op else {
            return Err("PhysicalCTEAnchor first child must be PhysicalCTEProduce".to_string());
        };
        self.visit_cte_produce(produce_op, produce)?;
        self.visit(consume)
    }

    fn visit_cte_produce(
        &mut self,
        op: &CTEProduceOp,
        node: &PhysicalPlanNode,
    ) -> Result<(), String> {
        let child_plan = expect_single_child(node, "PhysicalCTEProduce")?;
        let cte_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(cte_fragment_id);
        let child_result = self.visit(child_plan);
        self.fragment_stack.pop();
        let child = child_result?;

        let idx = self.completed_fragments.len();
        self.completed_fragments.push(PlanFragment {
            fragment_id: cte_fragment_id,
            root: child.clone(),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: op.output_columns.clone(),
            cte_id: Some(op.cte_id),
            cte_exchange_nodes: collect_cte_exchange_nodes(&child),
        });
        self.cte_fragments.insert(op.cte_id, idx);
        Ok(())
    }

    fn visit_cte_consume(
        &mut self,
        op: &CTEConsumeOp,
        node: &PhysicalPlanNode,
    ) -> Result<DistributedPlanNode, String> {
        if !node.children.is_empty() {
            return Err(format!(
                "build_distributed_plan M0: PhysicalCTEConsume expected 0 children, got {}",
                node.children.len()
            ));
        }
        let cte_frag_idx = self
            .cte_fragments
            .get(&op.cte_id)
            .copied()
            .ok_or_else(|| format!("CTE consume references unknown cte_id={}", op.cte_id))?;
        let cte_fragment_id = self.completed_fragments[cte_frag_idx].fragment_id;

        let exchange_node_id = self.alloc_node();
        let exchange_tuple_id = self.alloc_tuple();
        let target_fragment_id = self.current_fragment_id()?;

        self.edges.push(FragmentEdge {
            source_fragment_id: cte_fragment_id,
            target_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: tdata_partition_placeholder(
                partitions::TPartitionType::UNPARTITIONED,
            ),
            stream_kind: FragmentStreamKind::Broadcast,
            edge_kind: FragmentEdgeKind::CteMulticast { cte_id: op.cte_id },
        });

        Ok(DistributedPlanNode {
            node_id: exchange_node_id,
            fragment_id: target_fragment_id,
            tuple_ids: vec![exchange_tuple_id],
            nullable_tuple_ids: vec![],
            limit: -1,
            execution_join_distribution: node.execution_props.join_distribution,
            build_runtime_filters: node.build_runtime_filters.clone(),
            probe_runtime_filters: node.probe_runtime_filters.clone(),
            children: vec![],
            stats: PlanNodeStats::from_statistics(&node.stats),
            kind: PlanNodeKind::Exchange(DistributedExchangeNode {
                partition_type: partitions::TPartitionType::UNPARTITIONED,
                partition_exprs: Vec::new(),
                source_fragment_id: cte_fragment_id,
                output_columns: op.output_columns.clone(),
                output_qualifier: Some(op.alias.clone()),
                flavor: ExchangeFlavor::CteMulticast { cte_id: op.cte_id },
            }),
        })
    }

    fn visit_set_op(
        &mut self,
        node: &PhysicalPlanNode,
        kind: SetOpKind,
        explicit_output_columns: &[crate::sql::analysis::OutputColumn],
        child_output_columns: &[Vec<crate::sql::analysis::OutputColumn>],
    ) -> Result<DistributedPlanNode, String> {
        if node.children.is_empty() {
            return Err("set operation node has no inputs".to_string());
        }
        let fragment_id = self.current_fragment_id()?;

        let mut children = Vec::with_capacity(node.children.len());
        for child in &node.children {
            children.push(self.visit(child)?);
        }

        let output_columns = if !explicit_output_columns.is_empty() {
            explicit_output_columns.to_vec()
        } else if !node.output_columns.is_empty() {
            node.output_columns.clone()
        } else {
            node.children[0].output_columns.clone()
        };
        let tuple_id = self.alloc_tuple();
        let node_id = self.alloc_node();

        Ok(DistributedPlanNode {
            node_id,
            fragment_id,
            tuple_ids: vec![tuple_id],
            nullable_tuple_ids: vec![],
            limit: -1,
            execution_join_distribution: node.execution_props.join_distribution,
            build_runtime_filters: node.build_runtime_filters.clone(),
            probe_runtime_filters: node.probe_runtime_filters.clone(),
            children,
            stats: stats_for_physical_node(node),
            kind: PlanNodeKind::SetOp(DistributedSetOpNode {
                kind,
                output_columns,
                child_output_columns: child_output_columns.to_vec(),
            }),
        })
    }

    fn visit_union_distinct(
        &mut self,
        op: &UnionOp,
        node: &PhysicalPlanNode,
    ) -> Result<DistributedPlanNode, String> {
        let output_columns = if node.output_columns.is_empty() {
            op.output_columns.clone()
        } else {
            node.output_columns.clone()
        };
        let union_all_node = PhysicalPlanNode {
            op: Operator::PhysicalUnion(UnionOp {
                all: true,
                output_columns: op.output_columns.clone(),
                child_output_columns: op.child_output_columns.clone(),
            }),
            children: node.children.clone(),
            stats: node.stats.clone(),
            output_columns: output_columns.clone(),
            execution_props: node.execution_props.clone(),
            build_runtime_filters: node.build_runtime_filters.clone(),
            probe_runtime_filters: node.probe_runtime_filters.clone(),
        };
        let gathered_union = PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            }),
            children: vec![union_all_node],
            stats: node.stats.clone(),
            output_columns: output_columns.clone(),
            execution_props: node.execution_props.clone(),
            build_runtime_filters: node.build_runtime_filters.clone(),
            probe_runtime_filters: node.probe_runtime_filters.clone(),
        };
        let gathered = self.visit_distribution(
            &PhysicalDistributionOp {
                spec: DistributionSpec::Gather,
            },
            &gathered_union,
        )?;
        self.emit_distinct_on_top(
            gathered,
            if op.output_columns.is_empty() {
                &output_columns
            } else {
                &op.output_columns
            },
            node,
        )
    }

    fn emit_distinct_on_top(
        &mut self,
        child: DistributedPlanNode,
        output_columns: &[crate::sql::analysis::OutputColumn],
        node: &PhysicalPlanNode,
    ) -> Result<DistributedPlanNode, String> {
        if output_columns.is_empty() {
            return Err("UNION DISTINCT requires at least one output column".to_string());
        }

        let fragment_id = self.current_fragment_id()?;
        let agg_tuple_id = self.alloc_tuple();
        let agg_node_id = self.alloc_node();
        let group_by = output_columns
            .iter()
            .map(|column| TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: column.column_id,
                    qualifier: None,
                    column: column.name.clone(),
                },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
            .collect();

        Ok(DistributedPlanNode {
            node_id: agg_node_id,
            fragment_id,
            tuple_ids: vec![agg_tuple_id],
            nullable_tuple_ids: vec![],
            limit: -1,
            execution_join_distribution: node.execution_props.join_distribution,
            build_runtime_filters: node.build_runtime_filters.clone(),
            probe_runtime_filters: node.probe_runtime_filters.clone(),
            children: vec![child],
            stats: PlanNodeStats::from_statistics(&node.stats),
            kind: PlanNodeKind::HashAggregate(Box::new(DistributedHashAggregateNode {
                mode: crate::sql::optimizer::operator::AggMode::Single,
                group_by,
                aggregates: Vec::new(),
                is_merge: Vec::new(),
                output_columns: output_columns.to_vec(),
            })),
        })
    }
}

fn distributed_node_ordering(node: &DistributedPlanNode) -> OrderingSpec {
    match &node.kind {
        PlanNodeKind::Sort(sort) => ordering_spec_from_sort_items(&sort.items),
        PlanNodeKind::TopN(topn) => ordering_spec_from_sort_items(&topn.items),
        PlanNodeKind::Exchange(exchange) => match &exchange.flavor {
            ExchangeFlavor::TopNSplit { items, .. } => ordering_spec_from_sort_items(items),
            _ => OrderingSpec::Any,
        },
        PlanNodeKind::AssertOneRow(_) => node
            .children
            .first()
            .map(distributed_node_ordering)
            .unwrap_or(OrderingSpec::Any),
        PlanNodeKind::Window(window) => {
            let mut current_ordering = node
                .children
                .first()
                .map(distributed_node_ordering)
                .unwrap_or(OrderingSpec::Any);
            let groups = group_win_exprs_by_sig(&window.window_exprs);
            for group_indices in &groups {
                let Some(first_idx) = group_indices.first().copied() else {
                    continue;
                };
                let first_win = &window.window_exprs[first_idx];
                if groups.len() > 1 {
                    let required_ordering =
                        window_ordering_spec(&first_win.partition_by, &first_win.order_by);
                    let has_sort_keys =
                        !first_win.partition_by.is_empty() || !first_win.order_by.is_empty();
                    let ordering_is_representable = !matches!(required_ordering, OrderingSpec::Any);
                    let needs_sort = has_sort_keys
                        && (!ordering_is_representable
                            || !current_ordering.satisfies(&required_ordering));
                    if needs_sort {
                        current_ordering = required_ordering;
                    }
                }
            }
            current_ordering
        }
        _ => OrderingSpec::Any,
    }
}

fn expect_binary_children<'a>(
    node: &'a PhysicalPlanNode,
    operator_name: &str,
) -> Result<(&'a PhysicalPlanNode, &'a PhysicalPlanNode), String> {
    if node.children.len() != 2 {
        return Err(format!(
            "build_distributed_plan M0: {operator_name} expected 2 children, got {}",
            node.children.len()
        ));
    }
    Ok((&node.children[0], &node.children[1]))
}

fn expect_single_child<'a>(
    node: &'a PhysicalPlanNode,
    operator_name: &str,
) -> Result<&'a PhysicalPlanNode, String> {
    if node.children.len() != 1 {
        return Err(format!(
            "build_distributed_plan slice 1: {operator_name} expected 1 child, got {}",
            node.children.len()
        ));
    }
    Ok(&node.children[0])
}

fn limit_child_can_apply_offset_locally(child: &PhysicalPlanNode) -> bool {
    matches!(
        &child.op,
        Operator::PhysicalSort(_) | Operator::PhysicalTopN(_)
    )
}

fn fold_filter_into_scan(node: &mut DistributedPlanNode, predicate: &TypedExpr) -> bool {
    if let PlanNodeKind::Scan(scan) = &mut node.kind {
        scan.predicates
            .extend(split_and_conjuncts_typed(predicate).into_iter().cloned());
        true
    } else {
        false
    }
}

fn data_partition_for_distribution_spec(
    spec: &DistributionSpec,
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> Result<DataPartition, String> {
    match spec {
        DistributionSpec::Gather | DistributionSpec::Broadcast => {
            Ok(DataPartition::unpartitioned())
        }
        DistributionSpec::HashPartitioned { cols, .. } => {
            let exprs = partition_exprs_for_columns(cols, output_columns);
            if exprs.is_empty() {
                Ok(DataPartition::unpartitioned())
            } else {
                Ok(DataPartition {
                    kind: PartitionKind::Hash,
                    exprs,
                })
            }
        }
        DistributionSpec::Any => {
            Err("PhysicalDistribution(Any) is not supported in DistributedPlan builder".to_string())
        }
    }
}

fn partition_exprs_for_columns(
    cols: &[crate::sql::column_id::ColumnId],
    output_columns: &[crate::sql::analysis::OutputColumn],
) -> Vec<TypedExpr> {
    cols.iter()
        .filter_map(|col_id| {
            output_columns
                .iter()
                .find(|column| column.column_id == *col_id)
                .map(|column| TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: column.column_id,
                        qualifier: None,
                        column: column.name.clone(),
                    },
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
        })
        .collect()
}

fn partition_type_for_data_partition(partition: &DataPartition) -> partitions::TPartitionType {
    match partition.kind {
        PartitionKind::Unpartitioned => partitions::TPartitionType::UNPARTITIONED,
        PartitionKind::Random => partitions::TPartitionType::RANDOM,
        PartitionKind::Hash => partitions::TPartitionType::HASH_PARTITIONED,
    }
}

fn tdata_partition_placeholder(
    partition_type: partitions::TPartitionType,
) -> partitions::TDataPartition {
    partitions::TDataPartition::new(
        partition_type,
        None::<Vec<crate::exprs::TExpr>>,
        None::<Vec<partitions::TRangePartition>>,
        None::<Vec<partitions::TBucketProperty>>,
    )
}

fn stream_kind_for_distribution(spec: &DistributionSpec) -> FragmentStreamKind {
    match spec {
        DistributionSpec::Gather => FragmentStreamKind::Gather,
        DistributionSpec::Broadcast => FragmentStreamKind::Broadcast,
        DistributionSpec::HashPartitioned { .. } => FragmentStreamKind::Partitioned,
        DistributionSpec::Any => FragmentStreamKind::Other,
    }
}

pub(crate) fn build_distributed_plan(plan: &PhysicalPlanNode) -> Result<DistributedPlan, String> {
    let plan = match &plan.op {
        Operator::PhysicalDistribution(op) if matches!(op.spec, DistributionSpec::Gather) => plan
            .children
            .first()
            .ok_or_else(|| "root PhysicalDistribution(Gather) missing child".to_string())?,
        _ => plan,
    };

    let scalar_arena = plan
        .execution_props
        .scalar_arena
        .as_ref()
        .cloned()
        .ok_or_else(|| {
            "PhysicalPlanNode missing scalar arena for distributed plan build".to_string()
        })?;

    let mut builder = DistributedPlanBuilder {
        scalars: scalar_arena.as_ref(),
        next_node_id: 1,
        next_tuple_id: 1,
        next_fragment_id: 0,
        fragment_stack: Vec::new(),
        completed_fragments: Vec::new(),
        edges: Vec::new(),
        cte_fragments: HashMap::new(),
    };

    let root_fragment_id = builder.alloc_fragment_id();
    builder.fragment_stack.push(root_fragment_id);
    let root = builder.visit(plan)?;
    builder.fragment_stack.pop();
    let root_cte_exchange_nodes = collect_cte_exchange_nodes(&root);

    let mut fragments = builder.completed_fragments;
    fragments.push(PlanFragment {
        fragment_id: root_fragment_id,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: plan.output_columns.clone(),
        cte_id: None,
        cte_exchange_nodes: root_cte_exchange_nodes,
    });

    Ok(DistributedPlan {
        fragments,
        root_fragment_id,
        edges: builder.edges,
        scalar_arena,
    })
}

fn collect_cte_exchange_nodes(node: &DistributedPlanNode) -> Vec<(CteId, i32)> {
    let mut nodes = Vec::new();
    collect_cte_exchange_nodes_inner(node, &mut nodes);
    nodes
}

fn collect_cte_exchange_nodes_inner(node: &DistributedPlanNode, nodes: &mut Vec<(CteId, i32)>) {
    if let PlanNodeKind::Exchange(exchange) = &node.kind
        && let ExchangeFlavor::CteMulticast { cte_id } = exchange.flavor
    {
        nodes.push((cte_id, node.node_id));
    }
    for child in &node.children {
        collect_cte_exchange_nodes_inner(child, nodes);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::build_distributed_plan;
    use crate::sql::analysis::cte::CteId;
    use crate::sql::analysis::{
        BinOp, ExprKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::{
        AssertOneRowOp, CTEAnchorOp, CTEConsumeOp, CTEProduceOp, FilterOp, LimitOp, Operator,
        PhysicalDistributionOp, ProjectOp, ScanOp, SortOp, TopNOp, TopNPhase, UnionOp, WindowOp,
    };
    use crate::sql::optimizer::physical_plan::{
        PhysicalPlanNode, PlanExecutionProps, attach_scalar_arena,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::{ColumnStatistic, Statistics};
    use crate::sql::planner::optimizer_bridge::scalar::{
        intern_project_items, intern_sort_items, intern_typed, intern_window_exprs,
    };
    use crate::sql::planner::plan::{ExchangeFlavor, PlanNodeKind, WindowExpr};

    #[test]
    fn build_distributed_plan_scan_project_shapes_one_fragment() {
        let physical = scan_then_project_plan();
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        assert_eq!(dp.fragments.len(), 1);
        assert_eq!(dp.root_fragment_id, 0);
        let root = &dp.fragments[0].root;
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert!(matches!(root.kind, PlanNodeKind::Project(_)));
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].node_id, 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
        assert!(matches!(root.children[0].kind, PlanNodeKind::Scan(_)));
    }

    #[test]
    fn build_distributed_plan_folds_filter_predicate_into_scan() {
        let physical = filter_then_project_plan();
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = &dp.fragments[0].root;
        assert!(matches!(root.kind, PlanNodeKind::Project(_)));
        let PlanNodeKind::Scan(scan) = &root.children[0].kind else {
            panic!("project child should be scan");
        };
        assert_eq!(scan.predicates.len(), 3);
        assert_binary_predicate(&scan.predicates[0], ColumnId::new_for_test(1), BinOp::Eq, 7);
        assert_binary_predicate(
            &scan.predicates[1],
            ColumnId::new_for_test(1),
            BinOp::Gt,
            10,
        );
        assert_binary_predicate(
            &scan.predicates[2],
            ColumnId::new_for_test(2),
            BinOp::Lt,
            20,
        );
    }

    #[test]
    fn build_distributed_plan_folded_filter_uses_filter_stats() {
        let physical = project_plan(filter_plan_with_row_count(
            scan_plan_with_row_count(100.0),
            5.0,
        ));
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let folded_scan = &dp.fragments[0].root.children[0];

        assert!(matches!(folded_scan.kind, PlanNodeKind::Scan(_)));
        assert_eq!(folded_scan.stats.output_row_count, 5.0);
    }

    #[test]
    fn build_distributed_plan_folded_filter_preserves_scan_cost() {
        let scan = scan_plan_with_row_count(100.0);
        let expected_scan_cost = super::stats_for_physical_node(&scan)
            .cost_estimate
            .expect("scan cost");
        let expected_filter_cost =
            super::stats_for_physical_node(&filter_plan_with_row_count(scan.clone(), 5.0))
                .cost_estimate
                .expect("filter cost");
        let physical = filter_plan_with_row_count(scan, 5.0);
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let folded_scan = &dp.fragments[0].root;
        let preserved_cost = folded_scan
            .stats
            .cost_estimate
            .as_ref()
            .expect("folded scan cost");

        assert!(matches!(folded_scan.kind, PlanNodeKind::Scan(_)));
        assert_eq!(folded_scan.stats.output_row_count, 5.0);
        assert_eq!(preserved_cost.cpu_cost, expected_scan_cost.cpu_cost);
        assert_ne!(preserved_cost.cpu_cost, expected_filter_cost.cpu_cost);
    }

    #[test]
    fn build_distributed_plan_fused_limit_preserves_child_cost() {
        let scan = scan_plan_with_row_count(100.0);
        let expected_scan_cost = super::stats_for_physical_node(&scan)
            .cost_estimate
            .expect("scan cost");
        let expected_limit_cost =
            super::stats_for_physical_node(&limit_plan(scan.clone(), Some(7), None, 7.0))
                .cost_estimate
                .expect("limit cost");
        let physical = limit_plan(scan, Some(7), None, 7.0);
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = &dp.fragments[0].root;
        let preserved_cost = root
            .stats
            .cost_estimate
            .as_ref()
            .expect("fused limit child cost");

        assert!(matches!(root.kind, PlanNodeKind::Scan(_)));
        assert_eq!(root.limit, 7);
        assert_eq!(root.stats.output_row_count, 7.0);
        assert_eq!(preserved_cost.cpu_cost, expected_scan_cost.cpu_cost);
        assert_ne!(preserved_cost.cpu_cost, expected_limit_cost.cpu_cost);
    }

    #[test]
    fn build_distributed_plan_limit_offset_exchange_has_no_limit_cost() {
        let physical = limit_plan(scan_plan_with_row_count(100.0), Some(7), Some(2), 7.0);
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = root_plan_node(&dp);

        let PlanNodeKind::Exchange(exchange) = &root.kind else {
            panic!("root should be exchange");
        };
        assert!(matches!(
            exchange.flavor,
            ExchangeFlavor::LimitOffset { .. }
        ));
        assert!(
            root.stats.cost_estimate.is_none(),
            "synthetic limit-offset exchange should not display PhysicalLimit cost"
        );
    }

    #[test]
    fn build_distributed_plan_topn_split_exchange_has_no_topn_cost() {
        let physical = topn_split_plan(scan_plan_with_row_count(100.0));
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = root_plan_node(&dp);

        let PlanNodeKind::Exchange(exchange) = &root.kind else {
            panic!("root should be exchange");
        };
        assert!(matches!(exchange.flavor, ExchangeFlavor::TopNSplit { .. }));
        assert!(
            root.stats.cost_estimate.is_none(),
            "synthetic top-n split exchange should not display PhysicalTopN cost"
        );
    }

    #[test]
    fn build_distributed_plan_real_distribution_exchange_keeps_distribution_cost() {
        let physical = project_plan(distribution_plan(
            scan_plan_with_row_count(100.0),
            DistributionSpec::Gather,
        ));
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let exchange =
            find_plan_exchange_node(&dp, |flavor| matches!(flavor, ExchangeFlavor::Distribution));
        assert!(
            exchange.stats.cost_estimate.is_some(),
            "real distribution exchange should display PhysicalDistribution cost"
        );
    }

    #[test]
    fn build_distributed_plan_union_distinct_synthetic_aggregate_has_no_union_cost() {
        let physical = union_distinct_plan(vec![
            scan_plan_with_row_count(10.0),
            scan_plan_with_row_count(20.0),
        ]);
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = root_plan_node(&dp);

        assert!(matches!(root.kind, PlanNodeKind::HashAggregate(_)));
        assert!(
            root.stats.cost_estimate.is_none(),
            "synthetic distinct aggregate should not display PhysicalUnion cost"
        );
    }

    #[test]
    fn build_distributed_plan_cte_multicast_exchange_has_no_consume_cost() {
        let physical = cte_anchor_plan(scan_plan_with_row_count(10.0), cte_consume_plan(1));
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let exchange = find_plan_exchange_node(&dp, |flavor| {
            matches!(flavor, ExchangeFlavor::CteMulticast { .. })
        });

        assert!(
            exchange.stats.cost_estimate.is_none(),
            "synthetic CTE multicast exchange should not display PhysicalCTEConsume cost"
        );
    }

    #[test]
    fn build_distributed_plan_copies_scan_column_statistics() {
        let column_id = ColumnId::new_for_test(1);
        let mut physical = scan_plan();
        physical.stats.column_statistics.insert(
            column_id,
            ColumnStatistic {
                min_value: 1.0,
                max_value: 9.0,
                distinct_values_count: 4.0,
                ..Default::default()
            },
        );

        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = &dp.fragments[0].root;

        assert!(matches!(root.kind, PlanNodeKind::Scan(_)));
        let stat = root
            .stats
            .column_statistics
            .get(&column_id)
            .expect("column statistics for k");
        assert_eq!(stat.min_value, 1.0);
        assert_eq!(stat.max_value, 9.0);
        assert_eq!(stat.distinct_values_count, 4.0);
    }

    #[test]
    fn build_distributed_plan_preserves_filter_over_project() {
        let physical = filter_over_project_plan();
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = &dp.fragments[0].root;

        assert!(matches!(root.kind, PlanNodeKind::Filter(_)));
        assert_eq!(root.children.len(), 1);
        assert!(matches!(root.children[0].kind, PlanNodeKind::Project(_)));
    }

    #[test]
    fn window_pass_one_preserves_child_ordering_through_assert_for_parent_ids() {
        let physical = project_plan(multi_group_window_plan(assert_one_row_plan(sort_plan(
            scan_plan(),
        ))));
        let dp = build_distributed_plan(&physical).expect("build_distributed_plan");
        let root = &dp.fragments[0].root;

        assert_eq!(
            root.node_id, 7,
            "Project above the window must not be shifted by a phantom pre-window Sort"
        );
        assert!(matches!(root.kind, PlanNodeKind::Project(_)));
        assert_eq!(root.children[0].node_id, 4);
        assert!(matches!(root.children[0].kind, PlanNodeKind::Window(_)));
    }

    fn scan_then_project_plan() -> PhysicalPlanNode {
        project_plan(scan_plan())
    }

    fn root_plan_node(dp: &super::DistributedPlan) -> &super::DistributedPlanNode {
        &dp.fragments
            .iter()
            .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
            .expect("root fragment")
            .root
    }

    fn find_plan_exchange_node(
        dp: &super::DistributedPlan,
        matches_flavor: fn(&ExchangeFlavor) -> bool,
    ) -> &super::DistributedPlanNode {
        dp.fragments
            .iter()
            .find_map(|fragment| find_exchange_node(&fragment.root, matches_flavor))
            .expect("exchange node")
    }

    fn find_exchange_node(
        node: &super::DistributedPlanNode,
        matches_flavor: fn(&ExchangeFlavor) -> bool,
    ) -> Option<&super::DistributedPlanNode> {
        if let PlanNodeKind::Exchange(exchange) = &node.kind
            && matches_flavor(&exchange.flavor)
        {
            return Some(node);
        }
        node.children
            .iter()
            .find_map(|child| find_exchange_node(child, matches_flavor))
    }

    fn filter_then_project_plan() -> PhysicalPlanNode {
        project_plan(filter_plan(scan_plan()))
    }

    fn filter_over_project_plan() -> PhysicalPlanNode {
        filter_plan(project_plan(scan_plan()))
    }

    fn sort_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = child.output_columns.clone();
        let mut plan = physical_node(
            Operator::PhysicalSort(SortOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: column_ref_expr(1, "k", DataType::Int64, false),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                analytic_partition_exprs: vec![],
                partition_limit: None,
                topn_type: None,
            }),
            vec![child],
            output_columns,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn limit_plan(
        child: PhysicalPlanNode,
        limit: Option<i64>,
        offset: Option<i64>,
        row_count: f64,
    ) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node_with_row_count(
            Operator::PhysicalLimit(LimitOp { limit, offset }),
            vec![child],
            output_columns,
            row_count,
        )
    }

    fn topn_split_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = child.output_columns.clone();
        let mut plan = physical_node(
            Operator::PhysicalTopN(TopNOp {
                items: intern_sort_items(
                    &mut scalars,
                    &[SortItem {
                        expr: column_ref_expr(1, "k", DataType::Int64, false),
                        asc: true,
                        nulls_first: false,
                    }],
                ),
                limit: Some(3),
                offset: None,
                phase: TopNPhase::Final,
                is_split: true,
            }),
            vec![child],
            output_columns,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn distribution_plan(child: PhysicalPlanNode, spec: DistributionSpec) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalDistribution(PhysicalDistributionOp { spec }),
            vec![child],
            output_columns,
        )
    }

    fn union_distinct_plan(children: Vec<PhysicalPlanNode>) -> PhysicalPlanNode {
        let output_columns = children[0].output_columns.clone();
        let child_output_columns = children
            .iter()
            .map(|child| child.output_columns.clone())
            .collect();
        physical_node(
            Operator::PhysicalUnion(UnionOp {
                all: false,
                output_columns: output_columns.clone(),
                child_output_columns,
            }),
            children,
            output_columns,
        )
    }

    fn cte_anchor_plan(
        produce_child: PhysicalPlanNode,
        consume: PhysicalPlanNode,
    ) -> PhysicalPlanNode {
        let output_columns = consume.output_columns.clone();
        physical_node(
            Operator::PhysicalCTEAnchor(CTEAnchorOp { cte_id: 1 }),
            vec![cte_produce_plan(1, produce_child), consume],
            output_columns,
        )
    }

    fn cte_produce_plan(cte_id: CteId, child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalCTEProduce(CTEProduceOp {
                cte_id,
                output_columns: output_columns.clone(),
            }),
            vec![child],
            output_columns,
        )
    }

    fn cte_consume_plan(cte_id: CteId) -> PhysicalPlanNode {
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        let output_columns = vec![k, v];
        physical_node(
            Operator::PhysicalCTEConsume(CTEConsumeOp {
                cte_id,
                alias: "cte_t".to_string(),
                output_columns: output_columns.clone(),
            }),
            vec![],
            output_columns,
        )
    }

    fn assert_one_row_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let output_columns = child.output_columns.clone();
        physical_node(
            Operator::PhysicalAssertOneRow(AssertOneRowOp {
                subquery_text: "select k from t".to_string(),
            }),
            vec![child],
            output_columns,
        )
    }

    fn multi_group_window_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        let rn_by_k = output_col(3, "rn_by_k", DataType::Int64, false);
        let rn_by_v = output_col(4, "rn_by_v", DataType::Int64, false);
        let window_exprs = vec![
            WindowExpr {
                name: "row_number".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![],
                order_by: vec![SortItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    asc: true,
                    nulls_first: false,
                }],
                window_frame: None,
                result_type: DataType::Int64,
                output_name: rn_by_k.name.clone(),
                output_column_id: rn_by_k.column_id,
                ignore_nulls: false,
            },
            WindowExpr {
                name: "row_number".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![],
                order_by: vec![SortItem {
                    expr: column_ref_expr(2, "v", DataType::Int64, true),
                    asc: true,
                    nulls_first: false,
                }],
                window_frame: None,
                result_type: DataType::Int64,
                output_name: rn_by_v.name.clone(),
                output_column_id: rn_by_v.column_id,
                ignore_nulls: false,
            },
        ];
        let mut plan = physical_node(
            Operator::PhysicalWindow(WindowOp {
                window_exprs: intern_window_exprs(&mut scalars, &window_exprs),
                output_columns: vec![k.clone(), v.clone(), rn_by_k.clone(), rn_by_v.clone()],
            }),
            vec![child],
            vec![k, v, rn_by_k, rn_by_v],
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn scan_plan() -> PhysicalPlanNode {
        scan_plan_with_row_count(3.0)
    }

    fn scan_plan_with_row_count(row_count: f64) -> PhysicalPlanNode {
        let mut scalars = ScalarArena::new();
        let k = output_col(1, "k", DataType::Int64, false);
        let v = output_col(2, "v", DataType::Int64, true);
        let mut plan = physical_node_with_row_count(
            Operator::PhysicalScan(ScanOp {
                database: "test_db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: vec![k.clone(), v.clone()],
                predicates: vec![intern_typed(
                    &mut scalars,
                    &cmp_expr(
                        column_ref_expr(1, "k", DataType::Int64, false),
                        BinOp::Eq,
                        int_lit(7),
                    ),
                )],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            vec![k, v],
            row_count,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn filter_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        filter_plan_with_row_count(child, 3.0)
    }

    fn filter_plan_with_row_count(child: PhysicalPlanNode, row_count: f64) -> PhysicalPlanNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = child.output_columns.clone();
        let mut plan = physical_node_with_row_count(
            Operator::PhysicalFilter(FilterOp {
                predicate: intern_typed(
                    &mut scalars,
                    &and_expr(
                        cmp_expr(
                            column_ref_expr(1, "k", DataType::Int64, false),
                            BinOp::Gt,
                            int_lit(10),
                        ),
                        cmp_expr(
                            column_ref_expr(2, "v", DataType::Int64, true),
                            BinOp::Lt,
                            int_lit(20),
                        ),
                    ),
                ),
            }),
            vec![child],
            output_columns,
            row_count,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn project_plan(child: PhysicalPlanNode) -> PhysicalPlanNode {
        let mut scalars = scalars_from_children(std::slice::from_ref(&child));
        let output_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let items = vec![ProjectItem {
            expr: column_ref_expr(1, "k", DataType::Int64, false),
            output_name: "k".to_string(),
            output_column_id: ColumnId::new_for_test(1),
        }];
        let mut plan = physical_node(
            Operator::PhysicalProject(ProjectOp {
                items: intern_project_items(&mut scalars, &items),
                output_qualifier: None,
            }),
            vec![child],
            output_columns,
        );
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn physical_node(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
    ) -> PhysicalPlanNode {
        physical_node_with_row_count(op, children, output_columns, 3.0)
    }

    fn physical_node_with_row_count(
        op: Operator,
        children: Vec<PhysicalPlanNode>,
        output_columns: Vec<OutputColumn>,
        row_count: f64,
    ) -> PhysicalPlanNode {
        let scalars = scalars_from_children(&children);
        let mut plan = PhysicalPlanNode {
            op,
            children,
            stats: stats(row_count),
            output_columns,
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        attach_scalar_arena(&mut plan, Arc::new(scalars));
        plan
    }

    fn scalars_from_children(children: &[PhysicalPlanNode]) -> ScalarArena {
        children
            .iter()
            .find_map(|child| child.execution_props.scalar_arena.as_deref().cloned())
            .unwrap_or_else(ScalarArena::new)
    }

    fn stats(row_count: f64) -> Statistics {
        Statistics {
            output_row_count: row_count,
            ..Default::default()
        }
    }

    fn table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![
                column_def("k", DataType::Int64, false),
                column_def("v", DataType::Int64, true),
            ],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 1,
                table_id: 2,
            },
        }
    }

    fn column_def(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn output_col(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId::new_for_test(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn column_ref_expr(id: u32, column: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: column.to_string(),
            },
            data_type,
            nullable,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn and_expr(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn assert_binary_predicate(expr: &TypedExpr, column_id: ColumnId, op: BinOp, value: i64) {
        let ExprKind::BinaryOp {
            left,
            op: actual_op,
            right,
        } = &expr.kind
        else {
            panic!("expected binary predicate, got {expr:?}");
        };
        assert_eq!(*actual_op, op);
        let ExprKind::ColumnRef {
            column_id: actual_column_id,
            ..
        } = &left.kind
        else {
            panic!("expected column ref left predicate, got {left:?}");
        };
        assert_eq!(*actual_column_id, column_id);
        let ExprKind::Literal(LiteralValue::Int(actual_value)) = &right.kind else {
            panic!("expected int literal right predicate, got {right:?}");
        };
        assert_eq!(*actual_value, value);
    }
}
