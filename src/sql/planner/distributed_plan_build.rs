#![allow(dead_code)]
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};

use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{ExprKind, JoinKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::codegen::helpers::{group_win_exprs_by_sig, split_and_conjuncts_typed};
use crate::sql::codegen::{FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed_fragment::{
    DataPartition, DataSink, DistributedPlan, PlanFragment,
};
use crate::sql::planner::distributed_node::{
    DistributedNode, DistributedPayload, ExchangeReceiver,
};
use crate::sql::planner::optimizer_bridge::property::{
    ordering_spec_from_sort_items, window_ordering_spec,
};
use crate::sql::planner::plan::{
    ExchangeFlavor, PhysicalPlanKind, PhysicalPlanNode, PhysicalSetOpNode, PlanProjectNode,
    PlanScanNode, PlanSetOpKind, RedistributeMode, RedistributeNode,
};
use crate::sql::planner::{
    OrderingSpec, RuntimeFilterBuildIntent, RuntimeFilterProbeIntent, TopNPhase,
    WiredRuntimeFilterBuild, WiredRuntimeFilterProbe,
};

pub(crate) fn build_distributed_plan(plan: &PhysicalPlanNode) -> Result<DistributedPlan, String> {
    let mut builder = DistributedPlanBuilder {
        next_node_id: 1,
        next_tuple_id: 1,
        next_fragment_id: 0,
        fragment_stack: Vec::new(),
        completed_fragments: Vec::new(),
        edges: Vec::new(),
        cte_fragments: HashMap::new(),
        rf_build_intents: Vec::new(),
        rf_probe_intents: Vec::new(),
    };
    let root_fragment_id = builder.alloc_fragment_id();
    let root_plan = if let PhysicalPlanKind::Redistribute(redistribute) = &plan.kind {
        if matches!(redistribute.mode, RedistributeMode::Gather) {
            expect_child_count(plan, 1)?;
            &plan.children[0]
        } else {
            plan
        }
    } else {
        plan
    };

    builder.fragment_stack.push(root_fragment_id);
    let root_result = builder.visit(root_plan);
    let popped_fragment_id = builder.fragment_stack.pop();
    debug_assert_eq!(popped_fragment_id, Some(root_fragment_id));
    let root = root_result?;
    let root_cte_exchange_nodes = collect_cte_exchange_nodes(&root);

    let mut fragments = builder.completed_fragments;
    let rf_build_intents = builder.rf_build_intents;
    let rf_probe_intents = builder.rf_probe_intents;
    fragments.push(PlanFragment {
        fragment_id: root_fragment_id,
        root,
        data_partition: DataPartition::unpartitioned(),
        output_partition: DataPartition::unpartitioned(),
        sink: DataSink::Result,
        output_exprs: None,
        output_columns: root_plan.output_columns.clone(),
        cte_id: None,
        cte_exchange_nodes: root_cte_exchange_nodes,
    });
    wire_runtime_filters(&mut fragments, &rf_build_intents, &rf_probe_intents);

    Ok(DistributedPlan {
        fragments,
        root_fragment_id,
        edges: builder.edges,
    })
}

struct DistributedPlanBuilder {
    next_node_id: i32,
    next_tuple_id: i32,
    next_fragment_id: FragmentId,
    fragment_stack: Vec<FragmentId>,
    completed_fragments: Vec<PlanFragment>,
    edges: Vec<FragmentEdge>,
    cte_fragments: HashMap<CteId, usize>,
    rf_build_intents: Vec<RuntimeFilterBuildBinding>,
    rf_probe_intents: Vec<RuntimeFilterProbeBinding>,
}

struct RuntimeFilterBuildBinding {
    node_id: i32,
    fragment_id: FragmentId,
    intent: RuntimeFilterBuildIntent,
}

struct RuntimeFilterProbeBinding {
    node_id: i32,
    fragment_id: FragmentId,
    intent: RuntimeFilterProbeIntent,
}

impl DistributedPlanBuilder {
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
            .ok_or_else(|| "build_distributed_plan internal error: no current fragment".to_string())
    }

    fn visit(&mut self, node: &PhysicalPlanNode) -> Result<DistributedNode, String> {
        let fragment_id = self.current_fragment_id()?;
        match &node.kind {
            PhysicalPlanKind::Values(_) => {
                expect_child_count(node, 0)?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], Vec::new()))
            }
            PhysicalPlanKind::Scan(_) => {
                expect_child_count(node, 0)?;
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], Vec::new()))
            }
            PhysicalPlanKind::Project(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                let node_id = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], vec![child]))
            }
            PhysicalPlanKind::Filter(filter) => {
                expect_child_count(node, 1)?;
                let mut child = self.visit(&node.children[0])?;
                if let DistributedPayload::Physical(PhysicalPlanKind::Scan(scan)) =
                    &mut child.payload
                {
                    let folded_predicates = split_and_conjuncts_typed(&filter.predicate)
                        .into_iter()
                        .cloned()
                        .collect::<Vec<_>>();
                    merge_scan_required_columns_for_predicates(scan, &folded_predicates)?;
                    scan.predicates.extend(folded_predicates);
                    child.stats = node.stats.clone();
                    self.record_probe_runtime_filter_intents(
                        child.node_id,
                        child.fragment_id,
                        &node.probe_runtime_filters,
                    );
                    Ok(child)
                } else {
                    let node_id = self.alloc_node();
                    let tuple_ids = child.tuple_ids.clone();
                    Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![child]))
                }
            }
            PhysicalPlanKind::Sort(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                let node_id = self.alloc_node();
                let tuple_ids = child.tuple_ids.clone();
                Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![child]))
            }
            PhysicalPlanKind::HashAggregate(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], vec![child]))
            }
            PhysicalPlanKind::HashJoin(_) => {
                expect_child_count(node, 2)?;
                let left = self.visit(&node.children[0])?;
                let right = self.visit(&node.children[1])?;
                let node_id = self.alloc_node();
                let mut tuple_ids = left.tuple_ids.clone();
                tuple_ids.extend(right.tuple_ids.iter().copied());
                Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![left, right]))
            }
            PhysicalPlanKind::NestLoopJoin(_) => {
                expect_child_count(node, 2)?;
                let left = self.visit(&node.children[0])?;
                let right = self.visit(&node.children[1])?;
                let node_id = self.alloc_node();
                let mut tuple_ids = left.tuple_ids.clone();
                tuple_ids.extend(right.tuple_ids.iter().copied());
                Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![left, right]))
            }
            PhysicalPlanKind::AssertOneRow(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                let node_id = self.alloc_node();
                let tuple_ids = child.tuple_ids.clone();
                Ok(self.make_node(node, fragment_id, node_id, tuple_ids, vec![child]))
            }
            PhysicalPlanKind::Repeat(repeat) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                let node_id = self.alloc_node();
                let virtual_tuple_id = self.alloc_tuple();
                let mut tuple_ids = child.tuple_ids.clone();
                if !repeat.grouping_fn_args.is_empty() {
                    tuple_ids.push(virtual_tuple_id);
                }
                let mut payload = repeat.clone();
                payload.virtual_tuple_id = Some(virtual_tuple_id);
                Ok(self.make_node_with_payload(
                    node,
                    fragment_id,
                    node_id,
                    tuple_ids,
                    vec![child],
                    PhysicalPlanKind::Repeat(payload),
                ))
            }
            PhysicalPlanKind::Window(window) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                let groups = group_win_exprs_by_sig(&window.window_exprs);
                if groups.is_empty() {
                    return Err(
                        "build_distributed_plan: PhysicalWindow has no window expressions"
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
                    let first_win = &window.window_exprs[first_idx];
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
                    "build_distributed_plan: PhysicalWindow produced no thrift node".to_string()
                })?;
                Ok(self.make_node_with_payload(
                    node,
                    fragment_id,
                    node_id,
                    tuple_ids,
                    vec![child],
                    PhysicalPlanKind::Window(window.clone()),
                ))
            }
            PhysicalPlanKind::ChangeEventExpand(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], vec![child]))
            }
            PhysicalPlanKind::GenerateSeries(_) => {
                expect_child_count(node, 0)?;
                let _ = self.alloc_tuple();
                let _ = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], Vec::new()))
            }
            PhysicalPlanKind::TableFunction(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                let _ = self.alloc_tuple();
                let _ = self.alloc_node();
                let tuple_id = self.alloc_tuple();
                let node_id = self.alloc_node();
                Ok(self.make_node(node, fragment_id, node_id, vec![tuple_id], vec![child]))
            }
            PhysicalPlanKind::Redistribute(redistribute) => {
                self.visit_redistribute(node, redistribute)
            }
            PhysicalPlanKind::SetOp(set_op) => match set_op.kind {
                PlanSetOpKind::UnionDistinct => {
                    Err(union_distinct_must_be_rewritten_error().to_string())
                }
                PlanSetOpKind::UnionAll | PlanSetOpKind::Intersect | PlanSetOpKind::Except => {
                    self.visit_set_op(node, set_op)
                }
            },
            PhysicalPlanKind::Limit(limit) => self.visit_limit(node, limit),
            PhysicalPlanKind::TopN(topn) => self.visit_topn(node, topn),
            PhysicalPlanKind::CTEAnchor(anchor) => self.visit_cte_anchor(node, anchor),
            PhysicalPlanKind::CTEProduce(_) => Err(
                "PhysicalCTEProduce emits no DistributedPlan node outside CTEAnchor".to_string(),
            ),
            PhysicalPlanKind::CTEConsume(consume) => self.visit_cte_consume(node, consume),
        }
    }

    fn make_node(
        &mut self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
        node_id: i32,
        tuple_ids: Vec<i32>,
        children: Vec<DistributedNode>,
    ) -> DistributedNode {
        self.make_node_with_payload(
            node,
            fragment_id,
            node_id,
            tuple_ids,
            children,
            node.kind.clone(),
        )
    }

    fn make_node_with_payload(
        &mut self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
        node_id: i32,
        tuple_ids: Vec<i32>,
        children: Vec<DistributedNode>,
        payload: PhysicalPlanKind,
    ) -> DistributedNode {
        self.record_runtime_filter_intents(node, node_id, fragment_id, &payload);
        DistributedNode {
            node_id,
            fragment_id,
            tuple_ids,
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children,
            stats: node.stats.clone(),
            payload: DistributedPayload::Physical(payload),
        }
    }

    fn record_runtime_filter_intents(
        &mut self,
        node: &PhysicalPlanNode,
        node_id: i32,
        fragment_id: FragmentId,
        payload: &PhysicalPlanKind,
    ) {
        self.record_probe_runtime_filter_intents(node_id, fragment_id, &node.probe_runtime_filters);
        if let PhysicalPlanKind::HashJoin(join) = payload {
            for intent in &join.build_runtime_filters {
                self.rf_build_intents.push(RuntimeFilterBuildBinding {
                    node_id,
                    fragment_id,
                    intent: intent.clone(),
                });
            }
        }
    }

    fn record_probe_runtime_filter_intents(
        &mut self,
        node_id: i32,
        fragment_id: FragmentId,
        intents: &[RuntimeFilterProbeIntent],
    ) {
        for intent in intents {
            self.rf_probe_intents.push(RuntimeFilterProbeBinding {
                node_id,
                fragment_id,
                intent: intent.clone(),
            });
        }
    }

    fn visit_set_op(
        &mut self,
        node: &PhysicalPlanNode,
        set_op: &PhysicalSetOpNode,
    ) -> Result<DistributedNode, String> {
        if node.children.is_empty() {
            return Err("set operation node has no inputs".to_string());
        }
        let fragment_id = self.current_fragment_id()?;

        let mut children = Vec::with_capacity(node.children.len());
        for child in &node.children {
            children.push(self.visit(child)?);
        }

        let output_columns = set_op_payload_output_columns(node, set_op);
        let tuple_id = self.alloc_tuple();
        let node_id = self.alloc_node();
        Ok(self.make_node_with_payload(
            node,
            fragment_id,
            node_id,
            vec![tuple_id],
            children,
            PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: set_op.kind,
                output_columns,
                child_output_columns: set_op.child_output_columns.clone(),
            }),
        ))
    }

    fn visit_redistribute(
        &mut self,
        node: &PhysicalPlanNode,
        redistribute: &RedistributeNode,
    ) -> Result<DistributedNode, String> {
        expect_child_count(node, 1)?;
        let child_plan = &node.children[0];
        let output_partition = data_partition_for_redistribute_node(redistribute)?;
        let stream_kind = stream_kind_for_redistribute_mode(&redistribute.mode);

        self.emit_stream_exchange(
            child_plan,
            output_partition,
            stream_kind,
            ExchangeFlavor::Distribution,
            -1,
            stream_exchange_output_columns(child_plan, &redistribute.output_columns),
            None,
            node.stats.clone(),
        )
    }

    fn visit_limit(
        &mut self,
        node: &PhysicalPlanNode,
        limit: &crate::sql::planner::plan::PlanLimitNode,
    ) -> Result<DistributedNode, String> {
        expect_child_count(node, 1)?;
        let child_plan = &node.children[0];
        let offset = limit.offset.unwrap_or(0);
        if offset > 0 && !limit_child_can_apply_offset_locally(child_plan) {
            return self.emit_stream_exchange(
                child_plan,
                DataPartition::unpartitioned(),
                FragmentStreamKind::Gather,
                ExchangeFlavor::LimitOffset {
                    limit: limit.limit,
                    offset: limit.offset,
                },
                limit.limit.unwrap_or(-1),
                Vec::new(),
                None,
                synthetic_exchange_stats(&node.stats),
            );
        }

        let mut child = self.visit(child_plan)?;
        child.limit = limit.limit.unwrap_or(-1);
        child.stats = limit_stats_with_child_cost(&node.stats, &child.stats);
        match &mut child.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Sort(sort)) => {
                sort.offset = limit.offset;
            }
            DistributedPayload::Physical(PhysicalPlanKind::TopN(topn)) => {
                topn.limit = limit.limit;
                topn.offset = limit.offset;
            }
            _ if offset > 0 => {
                return Err(
                    "LIMIT/OFFSET without a local SORT/TOPN child is not supported".to_string(),
                );
            }
            _ => {}
        }
        Ok(child)
    }

    fn visit_topn(
        &mut self,
        node: &PhysicalPlanNode,
        topn: &crate::sql::planner::plan::PhysicalTopNNode,
    ) -> Result<DistributedNode, String> {
        expect_child_count(node, 1)?;
        let child_plan = &node.children[0];
        match (topn.phase, topn.is_split) {
            (TopNPhase::Final, true) => self.emit_stream_exchange(
                child_plan,
                DataPartition::unpartitioned(),
                FragmentStreamKind::Gather,
                ExchangeFlavor::TopNSplit {
                    items: topn.items.clone(),
                    limit: topn.limit,
                    offset: topn.offset,
                },
                topn.limit.unwrap_or(-1),
                Vec::new(),
                None,
                synthetic_exchange_stats(&node.stats),
            ),
            (TopNPhase::Final, false) | (TopNPhase::Partial, _) => {
                let child = self.visit(child_plan)?;
                let node_id = self.alloc_node();
                let tuple_ids = child.tuple_ids.clone();
                let fragment_id = self.current_fragment_id()?;
                let mut topn_node =
                    self.make_node(node, fragment_id, node_id, tuple_ids, vec![child]);
                topn_node.limit = topn.limit.unwrap_or(-1);
                Ok(topn_node)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_stream_exchange(
        &mut self,
        child_plan: &PhysicalPlanNode,
        output_partition: DataPartition,
        stream_kind: FragmentStreamKind,
        flavor: ExchangeFlavor,
        limit: i64,
        exchange_output_columns: Vec<OutputColumn>,
        output_qualifier: Option<String>,
        exchange_stats: crate::sql::planner::PhysicalPlanStats,
    ) -> Result<DistributedNode, String> {
        let parent_fragment_id = self.current_fragment_id()?;
        let source_output_columns = stream_exchange_source_output_columns(child_plan);
        let exchange_output_columns = if exchange_output_columns.is_empty()
            && matches!(&flavor, ExchangeFlavor::Distribution)
        {
            source_output_columns.clone()
        } else {
            normalize_stream_exchange_output_columns(
                exchange_output_columns,
                &source_output_columns,
            )
        };
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(child_plan);
        let popped_fragment_id = self.fragment_stack.pop();
        debug_assert_eq!(popped_fragment_id, Some(child_fragment_id));
        let child = child_result?;

        let exchange_node_id = self.alloc_node();
        self.completed_fragments.push(PlanFragment {
            fragment_id: child_fragment_id,
            root: child.clone(),
            data_partition: DataPartition::unpartitioned(),
            output_partition: output_partition.clone(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: source_output_columns,
            cte_id: None,
            cte_exchange_nodes: collect_cte_exchange_nodes(&child),
        });
        self.edges.push(FragmentEdge {
            source_fragment_id: child_fragment_id,
            target_fragment_id: parent_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: output_partition.clone(),
            stream_kind,
            edge_kind: FragmentEdgeKind::Stream,
            output_slot_ids: output_slot_ids_for_exchange(&exchange_output_columns)?,
        });

        let exchange_tuple_ids = if exchange_output_columns.is_empty() {
            child.tuple_ids.clone()
        } else {
            vec![self.alloc_tuple()]
        };

        Ok(DistributedNode {
            node_id: exchange_node_id,
            fragment_id: parent_fragment_id,
            tuple_ids: exchange_tuple_ids,
            nullable_tuple_ids: Vec::new(),
            limit,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: exchange_stats,
            payload: DistributedPayload::Exchange(ExchangeReceiver {
                partition: output_partition,
                source_fragment_id: child_fragment_id,
                output_columns: exchange_output_columns,
                output_qualifier,
                flavor,
            }),
        })
    }

    fn visit_cte_anchor(
        &mut self,
        node: &PhysicalPlanNode,
        _anchor: &crate::sql::planner::plan::LogicalCTEAnchorNode,
    ) -> Result<DistributedNode, String> {
        expect_child_count(node, 2)?;
        let produce = &node.children[0];
        let consume = &node.children[1];
        let PhysicalPlanKind::CTEProduce(produce_payload) = &produce.kind else {
            return Err("PhysicalCTEAnchor first child must be PhysicalCTEProduce".to_string());
        };

        self.visit_cte_produce(produce, produce_payload)?;
        self.visit(consume)
    }

    fn visit_cte_produce(
        &mut self,
        node: &PhysicalPlanNode,
        produce: &crate::sql::planner::plan::LogicalCTEProduceNode,
    ) -> Result<(), String> {
        expect_child_count(node, 1)?;
        let child_plan = &node.children[0];
        let cte_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(cte_fragment_id);
        let child_result = self.visit(child_plan);
        let popped_fragment_id = self.fragment_stack.pop();
        debug_assert_eq!(popped_fragment_id, Some(cte_fragment_id));
        let child = child_result?;

        let idx = self.completed_fragments.len();
        self.completed_fragments.push(PlanFragment {
            fragment_id: cte_fragment_id,
            root: child.clone(),
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Noop,
            output_exprs: None,
            output_columns: produce.output_columns.clone(),
            cte_id: Some(produce.cte_id),
            cte_exchange_nodes: collect_cte_exchange_nodes(&child),
        });
        self.cte_fragments.insert(produce.cte_id, idx);
        Ok(())
    }

    fn visit_cte_consume(
        &mut self,
        node: &PhysicalPlanNode,
        consume: &crate::sql::planner::plan::LogicalCTEConsumeNode,
    ) -> Result<DistributedNode, String> {
        expect_child_count(node, 0)?;
        let cte_frag_idx = self
            .cte_fragments
            .get(&consume.cte_id)
            .copied()
            .ok_or_else(|| format!("CTE consume references unknown cte_id={}", consume.cte_id))?;
        let cte_fragment = &self.completed_fragments[cte_frag_idx];
        let cte_fragment_id = cte_fragment.fragment_id;
        let producer_output_columns = cte_fragment.output_columns.clone();
        validate_cte_consume_mapping(consume)?;
        let receive_producer_column_ids = consume.producer_column_ids.clone();
        let exchange_output_columns =
            cte_consume_exchange_output_columns(consume, &producer_output_columns)?;
        let project_items = cte_consume_remap_project_items(consume, &exchange_output_columns)?;

        let exchange_node_id = self.alloc_node();
        let exchange_tuple_id = self.alloc_tuple();
        let project_node_id = self.alloc_node();
        let project_tuple_id = self.alloc_tuple();
        let target_fragment_id = self.current_fragment_id()?;

        self.edges.push(FragmentEdge {
            source_fragment_id: cte_fragment_id,
            target_fragment_id,
            target_exchange_node_id: exchange_node_id,
            output_partition: DataPartition::unpartitioned(),
            stream_kind: FragmentStreamKind::Broadcast,
            edge_kind: FragmentEdgeKind::CteMulticast {
                cte_id: consume.cte_id,
                receive_producer_column_ids: receive_producer_column_ids.clone(),
            },
            output_slot_ids: Vec::new(),
        });

        let exchange = DistributedNode {
            node_id: exchange_node_id,
            fragment_id: target_fragment_id,
            tuple_ids: vec![exchange_tuple_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: Vec::new(),
            stats: synthetic_exchange_stats(&node.stats),
            payload: DistributedPayload::Exchange(ExchangeReceiver {
                partition: DataPartition::unpartitioned(),
                source_fragment_id: cte_fragment_id,
                output_columns: exchange_output_columns,
                output_qualifier: Some(consume.alias.clone()),
                flavor: ExchangeFlavor::CteMulticast {
                    cte_id: consume.cte_id,
                    receive_producer_column_ids,
                },
            }),
        };

        Ok(DistributedNode {
            node_id: project_node_id,
            fragment_id: target_fragment_id,
            tuple_ids: vec![project_tuple_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![exchange],
            stats: node.stats.clone(),
            payload: DistributedPayload::Physical(PhysicalPlanKind::Project(PlanProjectNode {
                items: project_items,
                output_qualifier: Some(consume.alias.clone()),
            })),
        })
    }
}

fn cte_consume_exchange_output_columns(
    consume: &crate::sql::planner::plan::LogicalCTEConsumeNode,
    producer_output_columns: &[OutputColumn],
) -> Result<Vec<OutputColumn>, String> {
    let columns_by_id = producer_output_columns
        .iter()
        .cloned()
        .map(|column| (column.column_id, column))
        .collect::<HashMap<_, _>>();
    consume
        .producer_column_ids
        .iter()
        .map(|producer_id| {
            columns_by_id.get(producer_id).cloned().ok_or_else(|| {
                format!(
                    "CTEConsume producer column {} not found in CTE fragment output for cte_id={}",
                    producer_id.0, consume.cte_id
                )
            })
        })
        .collect()
}

fn cte_consume_remap_project_items(
    consume: &crate::sql::planner::plan::LogicalCTEConsumeNode,
    exchange_output_columns: &[OutputColumn],
) -> Result<Vec<ProjectItem>, String> {
    if consume.output_columns.len() != exchange_output_columns.len() {
        return Err(format!(
            "CTEConsume output/exchange arity mismatch for cte_id={}",
            consume.cte_id
        ));
    }

    Ok(consume
        .output_columns
        .iter()
        .zip(exchange_output_columns.iter())
        .map(|(consumer, producer)| ProjectItem {
            expr: TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: producer.column_id,
                    qualifier: Some(consume.alias.clone()),
                    column: producer.name.clone(),
                },
                data_type: producer.data_type.clone(),
                nullable: producer.nullable,
            },
            output_name: consumer.name.clone(),
            output_column_id: consumer.column_id,
        })
        .collect())
}

fn distributed_node_ordering(node: &DistributedNode) -> OrderingSpec {
    match &node.payload {
        DistributedPayload::Physical(PhysicalPlanKind::Sort(sort)) => {
            ordering_spec_from_sort_items(&sort.items)
        }
        DistributedPayload::Physical(PhysicalPlanKind::TopN(topn)) => {
            ordering_spec_from_sort_items(&topn.items)
        }
        DistributedPayload::Exchange(exchange) => match &exchange.flavor {
            ExchangeFlavor::TopNSplit { items, .. } => ordering_spec_from_sort_items(items),
            _ => OrderingSpec::Any,
        },
        DistributedPayload::Physical(PhysicalPlanKind::AssertOneRow(_)) => node
            .children
            .first()
            .map(distributed_node_ordering)
            .unwrap_or(OrderingSpec::Any),
        DistributedPayload::Physical(PhysicalPlanKind::Window(window)) => {
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

fn set_op_payload_output_columns(
    node: &PhysicalPlanNode,
    set_op: &PhysicalSetOpNode,
) -> Vec<OutputColumn> {
    if !set_op.output_columns.is_empty() {
        set_op.output_columns.clone()
    } else if !node.output_columns.is_empty() {
        node.output_columns.clone()
    } else {
        node.children
            .first()
            .map(|child| child.output_columns.clone())
            .unwrap_or_default()
    }
}

fn stream_exchange_output_columns(
    child_plan: &PhysicalPlanNode,
    requested_output_columns: &[OutputColumn],
) -> Vec<OutputColumn> {
    if requested_output_columns.is_empty() {
        return Vec::new();
    }

    let actual_output_columns = stream_exchange_source_output_columns(child_plan);
    if actual_output_columns.is_empty() {
        return requested_output_columns.to_vec();
    }

    let requested_projected =
        produced_exchange_output_columns(requested_output_columns, &actual_output_columns);
    if requested_projected.is_empty() {
        dedup_output_columns(actual_output_columns)
    } else {
        requested_projected
    }
}

fn dedup_output_columns(columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    let mut seen = HashSet::new();
    columns
        .into_iter()
        .filter(|column| seen.insert(column.column_id))
        .collect()
}

fn stream_exchange_source_output_columns(plan: &PhysicalPlanNode) -> Vec<OutputColumn> {
    match &plan.kind {
        PhysicalPlanKind::Scan(scan) => scan_materialized_output_columns(scan, plan),
        PhysicalPlanKind::Values(values) => values.columns.clone(),
        PhysicalPlanKind::Project(project) => project_items_output_columns(&project.items),
        PhysicalPlanKind::Sort(sort) => {
            if sort.output_columns.is_empty() {
                stream_exchange_source_output_columns(&plan.children[0])
            } else {
                let child_output_columns = plan
                    .children
                    .first()
                    .map(stream_exchange_source_output_columns)
                    .unwrap_or_default();
                projected_source_output_columns(&sort.output_columns, &child_output_columns)
            }
        }
        PhysicalPlanKind::Filter(_) => plan
            .children
            .first()
            .map(stream_exchange_source_output_columns)
            .unwrap_or_else(|| plan.output_columns.clone()),
        PhysicalPlanKind::Limit(_)
        | PhysicalPlanKind::TopN(_)
        | PhysicalPlanKind::AssertOneRow(_)
        | PhysicalPlanKind::Redistribute(_) => {
            let child_output_columns = plan
                .children
                .first()
                .map(stream_exchange_source_output_columns)
                .unwrap_or_default();
            projected_source_output_columns(&plan.output_columns, &child_output_columns)
        }
        PhysicalPlanKind::Repeat(_) => plan.output_columns.clone(),
        PhysicalPlanKind::Window(window) => window.output_columns.clone(),
        PhysicalPlanKind::GenerateSeries(generate_series) => vec![OutputColumn {
            column_id: generate_series.output_column_id,
            name: generate_series.column_name.clone(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            is_internal: false,
        }],
        PhysicalPlanKind::TableFunction(table_function) => table_function.output_columns.clone(),
        PhysicalPlanKind::HashAggregate(aggregate) => aggregate.output_layout.full_output_columns(),
        PhysicalPlanKind::HashJoin(join) => {
            if join.output_columns.is_empty() {
                join_source_output_columns(join.join_type, &plan.children)
            } else {
                join_requested_source_output_columns(
                    join.join_type,
                    &plan.children,
                    &join.output_columns,
                )
            }
        }
        PhysicalPlanKind::NestLoopJoin(join) => {
            if join.output_columns.is_empty() {
                join_source_output_columns(join.join_type, &plan.children)
            } else {
                join_requested_source_output_columns(
                    join.join_type,
                    &plan.children,
                    &join.output_columns,
                )
            }
        }
        PhysicalPlanKind::SetOp(set_op) => set_op_payload_output_columns(plan, set_op),
        PhysicalPlanKind::ChangeEventExpand(expand) => expand.output_columns.clone(),
        PhysicalPlanKind::CTEAnchor(_) => plan.output_columns.clone(),
        PhysicalPlanKind::CTEProduce(produce) => produce.output_columns.clone(),
        PhysicalPlanKind::CTEConsume(consume) => consume.output_columns.clone(),
    }
}

fn projected_source_output_columns(
    requested: &[OutputColumn],
    source_output_columns: &[OutputColumn],
) -> Vec<OutputColumn> {
    if requested.is_empty() {
        source_output_columns.to_vec()
    } else {
        produced_exchange_output_columns(requested, source_output_columns)
    }
}

fn project_items_output_columns(items: &[ProjectItem]) -> Vec<OutputColumn> {
    items
        .iter()
        .map(|item| OutputColumn {
            column_id: item.output_column_id,
            name: item.output_name.clone(),
            data_type: item.expr.data_type.clone(),
            nullable: item.expr.nullable,
            is_internal: false,
        })
        .collect()
}

fn join_source_output_columns(
    join_type: JoinKind,
    children: &[PhysicalPlanNode],
) -> Vec<OutputColumn> {
    match join_type {
        JoinKind::LeftSemi | JoinKind::LeftAnti | JoinKind::NullAwareLeftAnti => children
            .first()
            .map(stream_exchange_source_output_columns)
            .unwrap_or_default(),
        JoinKind::RightSemi | JoinKind::RightAnti => children
            .get(1)
            .map(stream_exchange_source_output_columns)
            .unwrap_or_default(),
        JoinKind::Inner | JoinKind::Cross => {
            let mut columns = children
                .first()
                .map(stream_exchange_source_output_columns)
                .unwrap_or_default();
            if let Some(right) = children.get(1) {
                columns.extend(stream_exchange_source_output_columns(right));
            }
            columns
        }
        JoinKind::LeftOuter => {
            let mut columns = children
                .first()
                .map(stream_exchange_source_output_columns)
                .unwrap_or_default();
            if let Some(right) = children.get(1) {
                columns.extend(nullable_output_columns(
                    stream_exchange_source_output_columns(right),
                ));
            }
            columns
        }
        JoinKind::RightOuter => {
            let mut columns = children
                .first()
                .map(stream_exchange_source_output_columns)
                .map(nullable_output_columns)
                .unwrap_or_default();
            if let Some(right) = children.get(1) {
                columns.extend(stream_exchange_source_output_columns(right));
            }
            columns
        }
        JoinKind::FullOuter => {
            let mut columns = children
                .first()
                .map(stream_exchange_source_output_columns)
                .map(nullable_output_columns)
                .unwrap_or_default();
            if let Some(right) = children.get(1) {
                columns.extend(nullable_output_columns(
                    stream_exchange_source_output_columns(right),
                ));
            }
            columns
        }
    }
}

fn join_requested_source_output_columns(
    join_type: JoinKind,
    children: &[PhysicalPlanNode],
    requested: &[OutputColumn],
) -> Vec<OutputColumn> {
    let produced = join_source_output_columns(join_type, children);
    projected_source_output_columns(requested, &produced)
}

fn nullable_output_columns(mut columns: Vec<OutputColumn>) -> Vec<OutputColumn> {
    for column in &mut columns {
        column.nullable = true;
    }
    columns
}

fn normalize_stream_exchange_output_columns(
    requested: Vec<OutputColumn>,
    source_output_columns: &[OutputColumn],
) -> Vec<OutputColumn> {
    if requested.is_empty() {
        return requested;
    }

    let source_ids = source_output_columns
        .iter()
        .map(|column| column.column_id)
        .collect::<HashSet<_>>();
    if requested
        .iter()
        .all(|column| source_ids.contains(&column.column_id))
    {
        requested
    } else {
        source_output_columns.to_vec()
    }
}

fn produced_exchange_output_columns(
    requested: &[OutputColumn],
    child_output_columns: &[OutputColumn],
) -> Vec<OutputColumn> {
    let produced = child_output_columns
        .iter()
        .map(|column| (column.column_id, column))
        .collect::<HashMap<_, _>>();
    let mut seen = HashSet::new();
    requested
        .iter()
        .filter_map(|column| {
            if seen.insert(column.column_id) {
                produced.get(&column.column_id).copied()
            } else {
                None
            }
        })
        .cloned()
        .collect()
}

fn scan_materialized_output_columns(
    scan: &PlanScanNode,
    plan: &PhysicalPlanNode,
) -> Vec<OutputColumn> {
    let Some(required_columns) = scan.required_columns.as_ref() else {
        return scan.columns.clone();
    };
    let required: HashSet<String> = required_columns
        .iter()
        .map(|column| column.to_lowercase())
        .collect();
    let variant_ids: HashSet<ColumnId> = scan
        .variant_columns
        .iter()
        .map(|column| column.synthetic_column_id)
        .collect();

    let projected: Vec<OutputColumn> = scan
        .columns
        .iter()
        .filter(|column| {
            required.contains(&column.name.to_lowercase())
                || variant_ids.contains(&column.column_id)
                || !scan
                    .table
                    .columns
                    .iter()
                    .any(|table_column| table_column.name.eq_ignore_ascii_case(&column.name))
        })
        .cloned()
        .collect();
    if projected.is_empty() {
        plan.output_columns.clone()
    } else {
        projected
    }
}

pub(crate) fn union_distinct_must_be_rewritten_error() -> &'static str {
    "UNION DISTINCT must be rewritten by UnionDistinctToAggregate before distributed build"
}

fn data_partition_for_redistribute_mode(
    mode: &RedistributeMode,
    output_columns: &[OutputColumn],
) -> Result<DataPartition, String> {
    match mode {
        RedistributeMode::Gather | RedistributeMode::Broadcast => {
            Ok(DataPartition::unpartitioned())
        }
        RedistributeMode::Hash { cols, .. } => {
            let exprs = partition_exprs_for_columns(cols, output_columns)?;
            if exprs.is_empty() {
                Ok(DataPartition::unpartitioned())
            } else {
                Ok(DataPartition::hash(exprs))
            }
        }
    }
}

fn output_slot_ids_for_exchange(output_columns: &[OutputColumn]) -> Result<Vec<i32>, String> {
    output_columns
        .iter()
        .map(|column| {
            i32::try_from(column.column_id.0).map_err(|_| {
                format!(
                    "build_distributed_plan: output column id {} cannot be encoded as stream output slot id",
                    column.column_id
                )
            })
        })
        .collect()
}

fn merge_scan_required_columns_for_predicates(
    scan: &mut PlanScanNode,
    predicates: &[TypedExpr],
) -> Result<(), String> {
    let Some(required_columns) = scan.required_columns.as_mut() else {
        return Ok(());
    };
    let mut existing = required_columns
        .iter()
        .map(|name| name.to_lowercase())
        .collect::<HashSet<_>>();
    let columns_by_id = scan
        .columns
        .iter()
        .map(|column| (column.column_id, column.name.clone()))
        .collect::<HashMap<_, _>>();

    let mut predicate_column_ids = Vec::new();
    for predicate in predicates {
        collect_typed_expr_column_ids(predicate, &mut predicate_column_ids);
    }
    for column_id in predicate_column_ids {
        let Some(name) = columns_by_id.get(&column_id) else {
            return Err(format!(
                "build_distributed_plan: folded Scan predicate references unknown column id {}",
                column_id.0
            ));
        };
        if existing.insert(name.to_lowercase()) {
            required_columns.push(name.clone());
        }
    }
    Ok(())
}

fn collect_typed_expr_column_ids(expr: &TypedExpr, out: &mut Vec<ColumnId>) {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => {
            if *column_id != ColumnId::UNSET && !out.contains(column_id) {
                out.push(*column_id);
            }
        }
        ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => {}
        ExprKind::BinaryOp { left, right, .. } => {
            collect_typed_expr_column_ids(left, out);
            collect_typed_expr_column_ids(right, out);
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => collect_typed_expr_column_ids(expr, out),
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            for arg in args {
                collect_typed_expr_column_ids(arg, out);
            }
            if let ExprKind::AggregateCall { order_by, .. } = &expr.kind {
                for item in order_by {
                    collect_typed_expr_column_ids(&item.expr, out);
                }
            }
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            collect_typed_expr_column_ids(body, out);
        }
        ExprKind::InList { expr, list, .. } => {
            collect_typed_expr_column_ids(expr, out);
            for item in list {
                collect_typed_expr_column_ids(item, out);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            collect_typed_expr_column_ids(expr, out);
            collect_typed_expr_column_ids(low, out);
            collect_typed_expr_column_ids(high, out);
        }
        ExprKind::Like { expr, pattern, .. } => {
            collect_typed_expr_column_ids(expr, out);
            collect_typed_expr_column_ids(pattern, out);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_typed_expr_column_ids(operand, out);
            }
            for (when, then) in when_then {
                collect_typed_expr_column_ids(when, out);
                collect_typed_expr_column_ids(then, out);
            }
            if let Some(else_expr) = else_expr {
                collect_typed_expr_column_ids(else_expr, out);
            }
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                collect_typed_expr_column_ids(arg, out);
            }
            for item in partition_by {
                collect_typed_expr_column_ids(item, out);
            }
            for item in order_by {
                collect_typed_expr_column_ids(&item.expr, out);
            }
        }
    }
}

fn data_partition_for_redistribute_node(
    redistribute: &RedistributeNode,
) -> Result<DataPartition, String> {
    if let RedistributeMode::Hash { cols, .. } = &redistribute.mode
        && !redistribute.partition_exprs.is_empty()
    {
        validate_partition_exprs(cols, &redistribute.partition_exprs)?;
        return Ok(DataPartition::hash(redistribute.partition_exprs.clone()));
    }
    data_partition_for_redistribute_mode(&redistribute.mode, &redistribute.output_columns)
}

fn validate_partition_exprs(cols: &[ColumnId], exprs: &[TypedExpr]) -> Result<(), String> {
    if cols.len() != exprs.len() {
        return Err(format!(
            "build_distributed_plan: hash partition expression arity {} does not match hash column arity {}",
            exprs.len(),
            cols.len()
        ));
    }
    for (idx, (expected, expr)) in cols.iter().zip(exprs.iter()).enumerate() {
        let ExprKind::ColumnRef { column_id, .. } = &expr.kind else {
            return Err(format!(
                "build_distributed_plan: hash partition expression {idx} must be a ColumnRef, got {expr:?}"
            ));
        };
        if column_id != expected {
            return Err(format!(
                "build_distributed_plan: hash partition expression {idx} references {}, expected {}",
                column_id, expected
            ));
        }
    }
    Ok(())
}

fn partition_exprs_for_columns(
    cols: &[ColumnId],
    output_columns: &[OutputColumn],
) -> Result<Vec<TypedExpr>, String> {
    let mut exprs = Vec::with_capacity(cols.len());
    let mut missing = Vec::new();
    for col_id in cols {
        match output_columns
            .iter()
            .find(|column| column.column_id == *col_id)
        {
            Some(column) => exprs.push(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: column.column_id,
                    qualifier: None,
                    column: column.name.clone(),
                },
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            }),
            None => missing.push(*col_id),
        }
    }
    if missing.is_empty() {
        return Ok(exprs);
    }

    let missing = missing
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let available = output_columns
        .iter()
        .map(|column| format!("{}({})", column.column_id, column.name))
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "build_distributed_plan: missing hash partition columns [{missing}]; available output columns [{available}]"
    ))
}

fn stream_kind_for_redistribute_mode(mode: &RedistributeMode) -> FragmentStreamKind {
    match mode {
        RedistributeMode::Gather => FragmentStreamKind::Gather,
        RedistributeMode::Broadcast => FragmentStreamKind::Broadcast,
        RedistributeMode::Hash { .. } => FragmentStreamKind::Partitioned,
    }
}

fn validate_cte_consume_mapping(
    consume: &crate::sql::planner::plan::LogicalCTEConsumeNode,
) -> Result<(), String> {
    if consume.output_columns.len() != consume.producer_column_ids.len() {
        return Err(format!(
            "CTEConsume output/producers arity mismatch for cte_id={}",
            consume.cte_id
        ));
    }
    let mut seen = HashSet::new();
    for column in &consume.output_columns {
        if !seen.insert(column.column_id) {
            return Err(format!(
                "CTEConsume duplicate output column {} for cte_id={}",
                column.column_id.0, consume.cte_id
            ));
        }
    }
    Ok(())
}

fn synthetic_exchange_stats(
    stats: &crate::sql::planner::PhysicalPlanStats,
) -> crate::sql::planner::PhysicalPlanStats {
    crate::sql::planner::PhysicalPlanStats {
        output_row_count: stats.output_row_count,
        row_count_confidence: stats.row_count_confidence,
        column_statistics: stats.column_statistics.clone(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn limit_stats_with_child_cost(
    limit_stats: &crate::sql::planner::PhysicalPlanStats,
    child_stats: &crate::sql::planner::PhysicalPlanStats,
) -> crate::sql::planner::PhysicalPlanStats {
    crate::sql::planner::PhysicalPlanStats {
        output_row_count: limit_stats.output_row_count,
        row_count_confidence: limit_stats.row_count_confidence,
        column_statistics: limit_stats.column_statistics.clone(),
        cost_estimate: child_stats.cost_estimate.clone(),
        broadcast_decision: child_stats.broadcast_decision.clone(),
    }
}

fn limit_child_can_apply_offset_locally(child: &PhysicalPlanNode) -> bool {
    matches!(
        child.kind,
        PhysicalPlanKind::Sort(_) | PhysicalPlanKind::TopN(_)
    )
}

fn collect_cte_exchange_nodes(node: &DistributedNode) -> Vec<(CteId, i32, Vec<ColumnId>)> {
    let mut nodes = Vec::new();
    collect_cte_exchange_nodes_inner(node, &mut nodes);
    nodes
}

fn collect_cte_exchange_nodes_inner(
    node: &DistributedNode,
    nodes: &mut Vec<(CteId, i32, Vec<ColumnId>)>,
) {
    if let DistributedPayload::Exchange(exchange) = &node.payload
        && let ExchangeFlavor::CteMulticast {
            cte_id,
            receive_producer_column_ids,
        } = &exchange.flavor
    {
        nodes.push((*cte_id, node.node_id, receive_producer_column_ids.clone()));
    }
    for child in &node.children {
        collect_cte_exchange_nodes_inner(child, nodes);
    }
}

fn wire_runtime_filters(
    fragments: &mut [PlanFragment],
    build_bindings: &[RuntimeFilterBuildBinding],
    probe_bindings: &[RuntimeFilterProbeBinding],
) {
    let mut source_fragment_by_filter = HashMap::new();
    for build in build_bindings {
        source_fragment_by_filter
            .entry(build.intent.filter_id)
            .or_insert(build.fragment_id);
    }

    let mut target_fragments_by_filter: HashMap<i32, Vec<FragmentId>> = HashMap::new();
    for probe in probe_bindings {
        if !source_fragment_by_filter.contains_key(&probe.intent.filter_id) {
            continue;
        }
        let targets = target_fragments_by_filter
            .entry(probe.intent.filter_id)
            .or_default();
        if !targets.contains(&probe.fragment_id) {
            targets.push(probe.fragment_id);
        }
    }

    let mut builds_by_node: HashMap<i32, Vec<WiredRuntimeFilterBuild>> = HashMap::new();
    for build in build_bindings {
        let target_fragment_ids = target_fragments_by_filter
            .get(&build.intent.filter_id)
            .cloned()
            .unwrap_or_default();
        builds_by_node
            .entry(build.node_id)
            .or_default()
            .push(WiredRuntimeFilterBuild {
                filter_id: build.intent.filter_id,
                build_expr: build.intent.build_expr.clone(),
                probe_expr: build.intent.probe_expr.clone(),
                expr_order: build.intent.expr_order,
                execution_mode: build.intent.execution_mode,
                source_fragment_id: build.fragment_id,
                target_fragment_ids,
            });
    }

    let mut probes_by_node: HashMap<i32, Vec<WiredRuntimeFilterProbe>> = HashMap::new();
    for probe in probe_bindings {
        let Some(&source_fragment_id) = source_fragment_by_filter.get(&probe.intent.filter_id)
        else {
            continue;
        };
        let probes = probes_by_node.entry(probe.node_id).or_default();
        if probes
            .iter()
            .any(|wired| wired.filter_id == probe.intent.filter_id)
        {
            continue;
        }
        probes.push(WiredRuntimeFilterProbe {
            filter_id: probe.intent.filter_id,
            probe_expr: probe.intent.probe_expr.clone(),
            source_fragment_id,
        });
    }

    for fragment in fragments {
        wire_runtime_filters_in_node(&mut fragment.root, &mut builds_by_node, &mut probes_by_node);
    }
}

fn wire_runtime_filters_in_node(
    node: &mut DistributedNode,
    builds_by_node: &mut HashMap<i32, Vec<WiredRuntimeFilterBuild>>,
    probes_by_node: &mut HashMap<i32, Vec<WiredRuntimeFilterProbe>>,
) {
    if let Some(mut builds) = builds_by_node.remove(&node.node_id) {
        node.build_runtime_filters.append(&mut builds);
    }
    if let Some(mut probes) = probes_by_node.remove(&node.node_id) {
        node.probe_runtime_filters.append(&mut probes);
    }
    for child in &mut node.children {
        wire_runtime_filters_in_node(child, builds_by_node, probes_by_node);
    }
}

fn expect_child_count(node: &PhysicalPlanNode, expected: usize) -> Result<(), String> {
    if node.children.len() == expected {
        return Ok(());
    }

    Err(format!(
        "build_distributed_plan: PhysicalPlanKind::{} expected {} children, got {}",
        physical_kind_name(&node.kind),
        expected,
        node.children.len()
    ))
}

fn physical_kind_name(kind: &PhysicalPlanKind) -> &'static str {
    match kind {
        PhysicalPlanKind::Scan(_) => "Scan",
        PhysicalPlanKind::Filter(_) => "Filter",
        PhysicalPlanKind::Project(_) => "Project",
        PhysicalPlanKind::Sort(_) => "Sort",
        PhysicalPlanKind::Limit(_) => "Limit",
        PhysicalPlanKind::Values(_) => "Values",
        PhysicalPlanKind::Repeat(_) => "Repeat",
        PhysicalPlanKind::Window(_) => "Window",
        PhysicalPlanKind::GenerateSeries(_) => "GenerateSeries",
        PhysicalPlanKind::TableFunction(_) => "TableFunction",
        PhysicalPlanKind::AssertOneRow(_) => "AssertOneRow",
        PhysicalPlanKind::TopN(_) => "TopN",
        PhysicalPlanKind::HashAggregate(_) => "HashAggregate",
        PhysicalPlanKind::HashJoin(_) => "HashJoin",
        PhysicalPlanKind::NestLoopJoin(_) => "NestLoopJoin",
        PhysicalPlanKind::SetOp(_) => "SetOp",
        PhysicalPlanKind::ChangeEventExpand(_) => "ChangeEventExpand",
        PhysicalPlanKind::CTEAnchor(_) => "CTEAnchor",
        PhysicalPlanKind::CTEProduce(_) => "CTEProduce",
        PhysicalPlanKind::CTEConsume(_) => "CTEConsume",
        PhysicalPlanKind::Redistribute(_) => "Redistribute",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;

    use super::{
        build_distributed_plan, join_source_output_columns, union_distinct_must_be_rewritten_error,
    };
    use crate::sql::analysis::cte::CteId;
    use crate::sql::analysis::{
        BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, ProjectItem, SortItem, TypedExpr,
    };
    use crate::sql::catalog::{ColumnDef, ScanSource, TableDef};
    use crate::sql::codegen::{FragmentEdgeKind, FragmentStreamKind};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::distributed_fragment::{DataSink, PartitionKind};
    use crate::sql::planner::distributed_node::DistributedPayload;
    use crate::sql::planner::plan::{
        AggregateCall, DistributedChangeEventExpandNode, ExchangeFlavor, LogicalCTEAnchorNode,
        LogicalCTEConsumeNode, LogicalCTEProduceNode, PhysicalHashAggregateNode,
        PhysicalHashJoinEqCondition, PhysicalHashJoinNode, PhysicalNestLoopJoinNode,
        PhysicalPlanKind, PhysicalPlanNode, PhysicalSetOpNode, PhysicalTopNNode,
        PlanAssertOneRowNode, PlanFilterNode, PlanGenerateSeriesNode, PlanLimitNode,
        PlanProjectNode, PlanRepeatNode, PlanScanNode, PlanSetOpKind, PlanSortNode,
        PlanTableFunctionNode, PlanValuesNode, PlanWindowNode, RedistributeMode, RedistributeNode,
        WindowExpr,
    };
    use crate::sql::planner::{
        AggMode, AggregateOutputLayout, HashSource, JoinDistribution, JoinExecutionMode,
        PhysicalPlanStats, PlannerConfidence, PlannerCostEstimate, RuntimeFilterBuildIntent,
        RuntimeFilterProbeIntent, TopNPhase,
    };

    #[test]
    fn build_distributed_plan_values_shapes_root_fragment() {
        let output_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let plan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: output_columns.clone(),
            }),
            children: vec![],
            output_columns: output_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&plan).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        assert_eq!(dp.root_fragment_id, 0);
        assert!(dp.edges.is_empty());

        let fragment = &dp.fragments[0];
        assert_eq!(fragment.fragment_id, 0);
        assert!(matches!(fragment.sink, DataSink::Result));
        assert!(matches!(
            fragment.data_partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(matches!(
            fragment.output_partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(fragment.output_exprs.is_none());
        assert_eq!(fragment.output_columns.len(), output_columns.len());
        assert_eq!(
            fragment.output_columns[0].column_id,
            output_columns[0].column_id
        );
        assert_eq!(fragment.output_columns[0].name, output_columns[0].name);
        assert!(fragment.cte_id.is_none());
        assert!(fragment.cte_exchange_nodes.is_empty());

        assert!(matches!(
            &fragment.root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Values(_))
        ));
        assert_eq!(fragment.root.node_id, 1);
        assert_eq!(fragment.root.tuple_ids, vec![1]);
        assert_eq!(fragment.root.fragment_id, 0);
        assert!(fragment.root.children.is_empty());
    }

    #[test]
    fn build_distributed_plan_scan_project_shapes_one_fragment() {
        let scan_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let project_columns = vec![output_col(2, "k_alias", DataType::Int64, false)];
        let scan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns.clone(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            output_columns: scan_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    output_name: "k_alias".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                }],
                output_qualifier: None,
            }),
            children: vec![scan],
            output_columns: project_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&project).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children.len(), 1);

        let child = &root.children[0];
        assert!(matches!(
            &child.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
        assert_eq!(child.node_id, 1);
        assert_eq!(child.tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_folds_filter_predicate_into_scan() {
        let scan_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let project_columns = vec![output_col(2, "k_alias", DataType::Int64, false)];
        let scan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns.clone(),
                predicates: vec![bool_lit(true)],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            output_columns: scan_columns.clone(),
            stats: stats_with_row_count(100.0),
            probe_runtime_filters: vec![],
        };
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: and_expr(
                    cmp_expr(1, "k", BinOp::Gt, 10),
                    cmp_expr(1, "k", BinOp::Lt, 20),
                ),
            }),
            children: vec![scan],
            output_columns: scan_columns,
            stats: stats_with_row_count(5.0),
            probe_runtime_filters: vec![],
        };
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    output_name: "k_alias".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                }],
                output_qualifier: None,
            }),
            children: vec![filter],
            output_columns: project_columns,
            stats: stats_with_row_count(5.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&project).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children.len(), 1);
        let child = &root.children[0];
        let scan = match &child.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Scan(scan)) => scan,
            other => panic!("expected folded Scan child, got {other:?}"),
        };
        assert_eq!(child.node_id, 1);
        assert_eq!(child.tuple_ids, vec![1]);
        assert_eq!(scan.predicates.len(), 3);
        assert_bool_lit(&scan.predicates[0], true);
        assert_cmp_expr(&scan.predicates[1], 1, "k", BinOp::Gt, 10);
        assert_cmp_expr(&scan.predicates[2], 1, "k", BinOp::Lt, 20);
        assert_eq!(child.stats.output_row_count, 5.0);
    }

    #[test]
    fn build_distributed_plan_folded_filter_extends_scan_required_columns() {
        let scan_columns = vec![
            output_col(1, "k", DataType::Int64, false),
            output_col(2, "predicate_only", DataType::Int64, false),
        ];
        let scan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns.clone(),
                predicates: vec![],
                required_columns: Some(vec!["k".to_string()]),
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            output_columns: scan_columns.clone(),
            stats: stats_with_row_count(100.0),
            probe_runtime_filters: vec![],
        };
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: cmp_expr(2, "predicate_only", BinOp::Gt, 10),
            }),
            children: vec![scan],
            output_columns: scan_columns,
            stats: stats_with_row_count(5.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&filter).expect("build_distributed_plan");

        let scan = match &dp.fragments[0].root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Scan(scan)) => scan,
            other => panic!("expected folded Scan root, got {other:?}"),
        };
        assert_eq!(
            scan.required_columns.as_ref(),
            Some(&vec!["k".to_string(), "predicate_only".to_string()])
        );
    }

    #[test]
    fn build_distributed_plan_preserves_filter_over_project() {
        let scan_columns = vec![output_col(1, "k", DataType::Int64, false)];
        let project_columns = vec![output_col(2, "k_alias", DataType::Int64, false)];
        let scan = PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: table_def(),
                alias: Some("t".to_string()),
                columns: scan_columns.clone(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            output_columns: scan_columns.clone(),
            stats: stats_with_row_count(100.0),
            probe_runtime_filters: vec![],
        };
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    output_name: "k_alias".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                }],
                output_qualifier: None,
            }),
            children: vec![scan],
            output_columns: project_columns.clone(),
            stats: stats_with_row_count(10.0),
            probe_runtime_filters: vec![],
        };
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: cmp_expr(2, "k_alias", BinOp::Gt, 10),
            }),
            children: vec![project],
            output_columns: project_columns,
            stats: stats_with_row_count(5.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&filter).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        let root_filter = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Filter(filter)) => filter,
            other => panic!("expected Filter root, got {other:?}"),
        };
        assert_cmp_expr(&root_filter.predicate, 2, "k_alias", BinOp::Gt, 10);
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.stats.output_row_count, 5.0);
        assert_eq!(root.children.len(), 1);

        let child = &root.children[0];
        assert!(matches!(
            &child.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(child.node_id, 2);
        assert_eq!(child.tuple_ids, vec![2]);
    }

    #[test]
    fn build_distributed_plan_sort_reuses_child_tuple() {
        let scan = scan_node(1, "k");
        let sort = PhysicalPlanNode {
            kind: PhysicalPlanKind::Sort(PlanSortNode {
                items: vec![],
                analytic_partition_by: vec![],
                output_columns: scan.output_columns.clone(),
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&sort).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Sort(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1]);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].node_id, 1);
        assert!(matches!(
            &root.children[0].payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
    }

    #[test]
    fn build_distributed_plan_hash_aggregate_allocates_new_tuple() {
        let scan = scan_node(1, "k");
        let aggregate = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
                mode: AggMode::Single,
                group_by: vec![],
                aggregates: vec![],
                is_merge: vec![],
                output_layout: AggregateOutputLayout::new(vec![], vec![]),
                output_columns: vec![],
            })),
            children: vec![scan],
            output_columns: vec![],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&aggregate).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::HashAggregate(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_hash_join_combines_child_tuples() {
        let left = scan_node(1, "l_k");
        let right = scan_node(2, "r_k");
        let output_columns = vec![
            output_col(1, "l_k", DataType::Int64, false),
            output_col(2, "r_k", DataType::Int64, false),
        ];
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Unknown,
                execution_mode: None,
                build_runtime_filters: vec![],
                output_columns: output_columns.clone(),
            })),
            children: vec![left, right],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&join).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::HashJoin(_))
        ));
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![1, 2]);
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].node_id, 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
        assert_eq!(root.children[1].node_id, 2);
        assert_eq!(root.children[1].tuple_ids, vec![2]);
    }

    #[test]
    fn join_source_output_columns_widens_outer_nullable_side() {
        let left_col = output_col(1, "l_k", DataType::Int64, false);
        let right_col = output_col(2, "r_k", DataType::Int64, false);
        let children = vec![
            values_node(vec![left_col.clone()]),
            values_node(vec![right_col.clone()]),
        ];

        let left_outer = join_source_output_columns(JoinKind::LeftOuter, &children);
        assert_eq!(left_outer.len(), 2);
        assert!(!left_outer[0].nullable);
        assert!(left_outer[1].nullable);

        let right_outer = join_source_output_columns(JoinKind::RightOuter, &children);
        assert_eq!(right_outer.len(), 2);
        assert!(right_outer[0].nullable);
        assert!(!right_outer[1].nullable);

        let full_outer = join_source_output_columns(JoinKind::FullOuter, &children);
        assert_eq!(full_outer.len(), 2);
        assert!(full_outer[0].nullable);
        assert!(full_outer[1].nullable);
    }

    #[test]
    fn build_distributed_plan_wires_runtime_filters_across_redistribute_fragment() {
        let filter_id = 77;
        let probe_expr = column_ref_expr(1, "l_k", DataType::Int64, false);
        let build_expr = column_ref_expr(2, "r_k", DataType::Int64, false);
        let duplicate_probe_intent = RuntimeFilterProbeIntent {
            filter_id,
            probe_expr: probe_expr.clone(),
        };
        let mut left_scan = scan_node(1, "l_k");
        left_scan.probe_runtime_filters = vec![duplicate_probe_intent.clone()];
        let left_project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: probe_expr.clone(),
                    output_name: "l_k".to_string(),
                    output_column_id: ColumnId::new_for_test(1),
                }],
                output_qualifier: None,
            }),
            children: vec![left_scan],
            output_columns: vec![output_col(1, "l_k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![duplicate_probe_intent],
        };
        let left_redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Hash {
                    cols: vec![ColumnId::new_for_test(1)],
                    source: HashSource::ShuffleJoin,
                },
                partition_exprs: vec![],
                output_columns: left_project.output_columns.clone(),
            }),
            children: vec![left_project],
            output_columns: vec![output_col(1, "l_k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let right = scan_node(2, "r_k");
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: probe_expr.clone(),
                    right: build_expr.clone(),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
                execution_mode: Some(JoinExecutionMode::Partitioned),
                build_runtime_filters: vec![RuntimeFilterBuildIntent {
                    filter_id,
                    build_expr: build_expr.clone(),
                    probe_expr: probe_expr.clone(),
                    expr_order: 3,
                    execution_mode: JoinExecutionMode::Partitioned,
                }],
                output_columns: vec![
                    output_col(1, "l_k", DataType::Int64, false),
                    output_col(2, "r_k", DataType::Int64, false),
                ],
            })),
            children: vec![left_redistribute, right],
            output_columns: vec![
                output_col(1, "l_k", DataType::Int64, false),
                output_col(2, "r_k", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&join).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 2);
        let root_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
            .expect("root fragment");
        let probe_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id != dp.root_fragment_id)
            .expect("probe fragment");
        let join_node = &root_fragment.root;
        assert_eq!(join_node.fragment_id, dp.root_fragment_id);
        assert!(matches!(
            &join_node.payload,
            DistributedPayload::Physical(PhysicalPlanKind::HashJoin(_))
        ));
        assert_eq!(join_node.build_runtime_filters.len(), 1);
        let build = &join_node.build_runtime_filters[0];
        assert_eq!(build.filter_id, filter_id);
        assert_column_ref(&build.build_expr, 2, "r_k");
        assert_column_ref(&build.probe_expr, 1, "l_k");
        assert_eq!(build.expr_order, 3);
        assert_eq!(build.execution_mode, JoinExecutionMode::Partitioned);
        assert_eq!(build.source_fragment_id, join_node.fragment_id);
        assert_eq!(build.target_fragment_ids, vec![probe_fragment.fragment_id]);

        let probe_project = &probe_fragment.root;
        assert_eq!(probe_project.fragment_id, probe_fragment.fragment_id);
        assert_eq!(probe_project.probe_runtime_filters.len(), 1);
        assert_eq!(
            probe_project.probe_runtime_filters[0].source_fragment_id,
            join_node.fragment_id
        );
        let probe_scan = &probe_project.children[0];
        assert!(matches!(
            &probe_scan.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
        assert_eq!(probe_scan.probe_runtime_filters.len(), 1);
        let probe = &probe_scan.probe_runtime_filters[0];
        assert_eq!(probe.filter_id, filter_id);
        assert_column_ref(&probe.probe_expr, 1, "l_k");
        assert_eq!(probe.source_fragment_id, join_node.fragment_id);
    }

    #[test]
    fn build_distributed_plan_preserves_folded_filter_runtime_filter_probe() {
        let filter_id = 78;
        let probe_expr = column_ref_expr(1, "l_k", DataType::Int64, false);
        let build_expr = column_ref_expr(2, "r_k", DataType::Int64, false);
        let left_scan = scan_node(1, "l_k");
        let left_filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: cmp_expr(1, "l_k", BinOp::Gt, 10),
            }),
            children: vec![left_scan],
            output_columns: vec![output_col(1, "l_k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![RuntimeFilterProbeIntent {
                filter_id,
                probe_expr: probe_expr.clone(),
            }],
        };
        let right = scan_node(2, "r_k");
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![PhysicalHashJoinEqCondition {
                    left: probe_expr.clone(),
                    right: build_expr.clone(),
                    null_safe: false,
                }],
                other_condition: None,
                distribution: JoinDistribution::Shuffle,
                execution_mode: Some(JoinExecutionMode::Partitioned),
                build_runtime_filters: vec![RuntimeFilterBuildIntent {
                    filter_id,
                    build_expr: build_expr.clone(),
                    probe_expr: probe_expr.clone(),
                    expr_order: 0,
                    execution_mode: JoinExecutionMode::Partitioned,
                }],
                output_columns: vec![
                    output_col(1, "l_k", DataType::Int64, false),
                    output_col(2, "r_k", DataType::Int64, false),
                ],
            })),
            children: vec![left_filter, right],
            output_columns: vec![
                output_col(1, "l_k", DataType::Int64, false),
                output_col(2, "r_k", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&join).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        let join_node = &dp.fragments[0].root;
        assert_eq!(join_node.build_runtime_filters.len(), 1);
        assert_eq!(
            join_node.build_runtime_filters[0].target_fragment_ids,
            vec![join_node.fragment_id]
        );
        let scan = &join_node.children[0];
        assert!(matches!(
            &scan.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
        assert_eq!(scan.probe_runtime_filters.len(), 1);
        assert_eq!(scan.probe_runtime_filters[0].filter_id, filter_id);
        assert_eq!(
            scan.probe_runtime_filters[0].source_fragment_id,
            join_node.fragment_id
        );
    }

    #[test]
    fn build_distributed_plan_nest_loop_join_combines_child_tuples() {
        let left = scan_node(1, "l_k");
        let right = scan_node(2, "r_k");
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::NestLoopJoin(PhysicalNestLoopJoinNode {
                join_type: JoinKind::Inner,
                condition: None,
                output_columns: vec![
                    output_col(1, "l_k", DataType::Int64, false),
                    output_col(2, "r_k", DataType::Int64, false),
                ],
            }),
            children: vec![left, right],
            output_columns: vec![
                output_col(1, "l_k", DataType::Int64, false),
                output_col(2, "r_k", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&join).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::NestLoopJoin(_))
        ));
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![1, 2]);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
        assert_eq!(root.children[1].tuple_ids, vec![2]);
    }

    #[test]
    fn build_distributed_plan_assert_one_row_reuses_child_tuple() {
        let scan = scan_node(1, "k");
        let assert_one_row = PhysicalPlanNode {
            kind: PhysicalPlanKind::AssertOneRow(PlanAssertOneRowNode::global_at_most_one(
                "select k from t",
            )),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&assert_one_row).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::AssertOneRow(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1]);
        assert_eq!(root.children[0].node_id, 1);
    }

    #[test]
    fn build_distributed_plan_change_event_expand_allocates_new_tuple() {
        let scan = scan_node(1, "k");
        let expand = PhysicalPlanNode {
            kind: PhysicalPlanKind::ChangeEventExpand(DistributedChangeEventExpandNode {
                events: vec![],
                output_columns: vec![
                    output_col(2, "payload", DataType::Int64, false),
                    output_col(3, "change_op", DataType::Int64, false),
                ],
                change_op_column_id: ColumnId::new_for_test(3),
                data_route_column_id: None,
            }),
            children: vec![scan],
            output_columns: vec![
                output_col(2, "payload", DataType::Int64, false),
                output_col(3, "change_op", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&expand).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::ChangeEventExpand(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_repeat_appends_virtual_tuple_only_when_grouping_fn_args_present() {
        let scan = scan_node(1, "k");
        let repeat = PhysicalPlanNode {
            kind: PhysicalPlanKind::Repeat(repeat_node(false)),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&repeat).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        let repeat = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Repeat(repeat)) => repeat,
            other => panic!("expected Repeat root, got {other:?}"),
        };
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1]);
        assert_eq!(repeat.virtual_tuple_id, Some(2));

        let scan = scan_node(1, "k");
        let repeat = PhysicalPlanNode {
            kind: PhysicalPlanKind::Repeat(repeat_node(true)),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&repeat).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        let repeat = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Repeat(repeat)) => repeat,
            other => panic!("expected Repeat root, got {other:?}"),
        };
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1, 2]);
        assert_eq!(repeat.virtual_tuple_id, Some(2));
    }

    #[test]
    fn build_distributed_plan_generate_series_replicates_dummy_allocations() {
        let output_columns = vec![output_col(1, "x", DataType::Int64, false)];
        let generate_series = PhysicalPlanNode {
            kind: PhysicalPlanKind::GenerateSeries(PlanGenerateSeriesNode {
                start: 1,
                end: 3,
                step: 1,
                column_name: "x".to_string(),
                alias: None,
                output_column_id: ColumnId::new_for_test(1),
            }),
            children: vec![],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&generate_series).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::GenerateSeries(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![2]);
        assert!(root.children.is_empty());
    }

    #[test]
    fn build_distributed_plan_table_function_replicates_dummy_allocations() {
        let scan = scan_node(1, "k");
        let output_columns = vec![output_col(2, "item", DataType::Int64, false)];
        let table_function = PhysicalPlanNode {
            kind: PhysicalPlanKind::TableFunction(PlanTableFunctionNode {
                function_name: "unnest".to_string(),
                args: vec![],
                output_columns: output_columns.clone(),
                alias: None,
                is_left_join: false,
            }),
            children: vec![scan],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&table_function).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::TableFunction(_))
        ));
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![3]);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].node_id, 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_window_single_group_allocates_analytic_ids() {
        let scan = scan_node(1, "k");
        let rn = output_col(2, "rn", DataType::Int64, false);
        let output_columns = vec![output_col(1, "k", DataType::Int64, false), rn.clone()];
        let window = PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![window_expr(rn, vec![], vec![])],
                output_columns: output_columns.clone(),
            }),
            children: vec![scan],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&window).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Window(_))
        ));
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1, 3]);
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].node_id, 1);
        assert_eq!(root.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_window_multi_group_allocates_sort_when_ordering_changes() {
        let scan = scan_node_with_columns(vec![
            output_col(1, "k", DataType::Int64, false),
            output_col(2, "v", DataType::Int64, true),
        ]);
        let rn_by_k = output_col(3, "rn_by_k", DataType::Int64, false);
        let rn_by_v = output_col(4, "rn_by_v", DataType::Int64, false);
        let output_columns = vec![
            output_col(1, "k", DataType::Int64, false),
            output_col(2, "v", DataType::Int64, true),
            rn_by_k.clone(),
            rn_by_v.clone(),
        ];
        let window = PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![
                    window_expr(
                        rn_by_k,
                        vec![],
                        vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
                    ),
                    window_expr(
                        rn_by_v,
                        vec![],
                        vec![sort_item(column_ref_expr(2, "v", DataType::Int64, true))],
                    ),
                ],
                output_columns: output_columns.clone(),
            }),
            children: vec![scan],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let project_columns = vec![output_col(5, "rn_alias", DataType::Int64, false)];
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref_expr(4, "rn_by_v", DataType::Int64, false),
                    output_name: "rn_alias".to_string(),
                    output_column_id: ColumnId::new_for_test(5),
                }],
                output_qualifier: None,
            }),
            children: vec![window],
            output_columns: project_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&project).expect("build_distributed_plan");

        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(root.node_id, 6);
        assert_eq!(root.tuple_ids, vec![6]);
        assert_eq!(root.children.len(), 1);
        let window = &root.children[0];
        assert!(matches!(
            &window.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Window(_))
        ));
        assert_eq!(window.node_id, 2);
        assert_eq!(window.tuple_ids, vec![1, 3, 5]);
        assert_eq!(window.children.len(), 1);
        assert_eq!(window.children[0].node_id, 1);
        assert_eq!(window.children[0].tuple_ids, vec![1]);
    }

    #[test]
    fn build_distributed_plan_window_rejects_empty_window_exprs() {
        let scan = scan_node(1, "k");
        let window = PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![],
                output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err =
            build_distributed_plan(&window).expect_err("empty Window expressions are invalid");

        assert!(
            err.contains("PhysicalWindow has no window expressions"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_distributed_plan_hash_redistribute_creates_exchange_edge() {
        let scan = scan_node(1, "k");
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Hash {
                    cols: vec![ColumnId::new_for_test(1)],
                    source: HashSource::ShuffleJoin,
                },
                partition_exprs: vec![column_ref_expr(1, "qualified_k", DataType::Int64, false)],
                output_columns: scan.output_columns.clone(),
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    output_name: "k_alias".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                }],
                output_qualifier: None,
            }),
            children: vec![redistribute],
            output_columns: vec![output_col(2, "k_alias", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&project).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 2);
        assert_eq!(dp.root_fragment_id, 0);
        assert_eq!(dp.fragments[0].fragment_id, 1);
        assert_eq!(dp.fragments[1].fragment_id, 0);
        assert_eq!(dp.edges.len(), 1);

        let root = &dp.fragments[1].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Project(_))
        ));
        assert_eq!(root.fragment_id, 0);
        assert_eq!(root.children.len(), 1);

        let exchange = &root.children[0];
        let exchange_receiver = match &exchange.payload {
            DistributedPayload::Exchange(exchange_receiver) => exchange_receiver,
            other => panic!("expected Exchange child, got {other:?}"),
        };
        assert_eq!(exchange.fragment_id, 0);
        assert_eq!(exchange_receiver.source_fragment_id, 1);
        assert!(matches!(
            exchange_receiver.partition.kind,
            PartitionKind::Hash
        ));
        assert_eq!(exchange_receiver.partition.exprs.len(), 1);
        assert_column_ref(&exchange_receiver.partition.exprs[0], 1, "qualified_k");
        assert!(matches!(
            exchange_receiver.flavor,
            crate::sql::planner::plan::ExchangeFlavor::Distribution
        ));
        assert_eq!(exchange_receiver.output_columns.len(), 1);
        assert_eq!(
            exchange_receiver.output_columns[0].column_id,
            ColumnId::new_for_test(1)
        );
        assert_eq!(exchange_receiver.output_columns[0].name, "k");

        let edge = &dp.edges[0];
        assert_eq!(edge.source_fragment_id, 1);
        assert_eq!(edge.target_fragment_id, 0);
        assert_eq!(edge.target_exchange_node_id, exchange.node_id);
        assert_eq!(edge.stream_kind, FragmentStreamKind::Partitioned);
        assert!(matches!(edge.edge_kind, FragmentEdgeKind::Stream));
        assert_eq!(edge.output_slot_ids, vec![1]);

        let child_fragment = &dp.fragments[0];
        assert_ne!(exchange.tuple_ids, child_fragment.root.tuple_ids);
        assert_eq!(exchange.tuple_ids.len(), 1);
        assert!(matches!(child_fragment.sink, DataSink::Noop));
        assert!(matches!(
            child_fragment.output_partition.kind,
            PartitionKind::Hash
        ));
        assert_eq!(
            child_fragment.output_partition.explain_label(),
            "HASH_PARTITIONED (t.qualified_k)"
        );
        assert_eq!(
            child_fragment.output_columns[0].column_id,
            ColumnId::new_for_test(1)
        );
        assert!(matches!(edge.output_partition.kind, PartitionKind::Hash));
        assert_eq!(edge.output_partition.exprs.len(), 1);
        assert!(matches!(
            &child_fragment.root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
        assert_no_physical_redistribute(&dp.fragments[1].root);
        assert_no_physical_redistribute(&child_fragment.root);
    }

    #[test]
    fn build_distributed_plan_stream_edge_carries_exchange_output_slot_order() {
        let source_columns = vec![
            output_col(1, "old", DataType::Int64, false),
            output_col(2, "delta", DataType::Int64, false),
        ];
        let exchange_columns = vec![source_columns[1].clone(), source_columns[0].clone()];
        let scan = scan_node_with_columns(source_columns.clone());
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Hash {
                    cols: vec![ColumnId::new_for_test(2)],
                    source: HashSource::ShuffleJoin,
                },
                partition_exprs: vec![],
                output_columns: exchange_columns.clone(),
            }),
            children: vec![scan],
            output_columns: exchange_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

        let edge = &dp.edges[0];
        assert_eq!(edge.output_slot_ids, vec![2, 1]);

        let root = &dp.fragments[1].root;
        let receiver = match &root.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected Exchange root, got {other:?}"),
        };
        assert_eq!(
            receiver
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(2), ColumnId::new_for_test(1)]
        );

        let child_fragment = &dp.fragments[0];
        assert_eq!(
            child_fragment
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(2)]
        );
    }

    #[test]
    fn build_distributed_plan_redistribute_drops_non_child_output_columns() {
        let source_columns = vec![
            output_col(1, "old", DataType::Int64, false),
            output_col(2, "delta", DataType::Int64, false),
        ];
        let predicate_only = output_col(3, "predicate_only", DataType::Int64, false);
        let exchange_columns = vec![
            source_columns[1].clone(),
            predicate_only,
            source_columns[0].clone(),
        ];
        let mut scan = scan_node_with_columns(source_columns.clone());
        scan.output_columns = exchange_columns.clone();
        let filter = PhysicalPlanNode {
            kind: PhysicalPlanKind::Filter(PlanFilterNode {
                predicate: column_ref_expr(3, "predicate_only", DataType::Int64, false),
            }),
            children: vec![scan],
            output_columns: exchange_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: vec![],
                output_columns: exchange_columns.clone(),
            }),
            children: vec![filter],
            output_columns: exchange_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

        let edge = &dp.edges[0];
        assert_eq!(edge.output_slot_ids, vec![2, 1]);

        let root = &dp.fragments[1].root;
        let receiver = match &root.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected Exchange root, got {other:?}"),
        };
        assert_eq!(
            receiver
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(2), ColumnId::new_for_test(1)]
        );

        let child_fragment = &dp.fragments[0];
        assert_eq!(
            child_fragment
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(2)]
        );
    }

    #[test]
    fn build_distributed_plan_stream_edge_drops_scan_columns_pruned_from_required() {
        let source_columns = vec![
            output_col(1, "c0", DataType::Int64, false),
            output_col(2, "c1", DataType::Utf8, true),
            output_col(3, "c2", DataType::Utf8, true),
            output_col(4, "c3", DataType::Int64, true),
        ];
        let mut scan = scan_node_with_columns(source_columns.clone());
        if let PhysicalPlanKind::Scan(scan) = &mut scan.kind {
            scan.required_columns =
                Some(vec!["c0".to_string(), "c1".to_string(), "c3".to_string()]);
        }
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: vec![],
                output_columns: source_columns.clone(),
            }),
            children: vec![scan],
            output_columns: source_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

        let edge = &dp.edges[0];
        assert_eq!(edge.output_slot_ids, vec![1, 2, 4]);

        let child_fragment = &dp.fragments[0];
        assert_eq!(
            child_fragment
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![
                ColumnId::new_for_test(1),
                ColumnId::new_for_test(2),
                ColumnId::new_for_test(4)
            ]
        );
    }

    #[test]
    fn build_distributed_plan_stream_edge_drops_join_columns_pruned_from_child_source() {
        let left_columns = vec![
            output_col(1, "l_c0", DataType::Int64, false),
            output_col(2, "l_c1", DataType::Utf8, true),
            output_col(3, "l_c2", DataType::Utf8, true),
        ];
        let right_columns = vec![output_col(4, "r_c0", DataType::Int64, false)];
        let mut left = scan_node_with_columns(left_columns.clone());
        if let PhysicalPlanKind::Scan(scan) = &mut left.kind {
            scan.required_columns = Some(vec!["l_c0".to_string(), "l_c1".to_string()]);
        }
        let right = scan_node_with_columns(right_columns.clone());
        let join_output_columns = vec![
            left_columns[0].clone(),
            left_columns[2].clone(),
            right_columns[0].clone(),
        ];
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
                execution_mode: None,
                build_runtime_filters: vec![],
                output_columns: join_output_columns.clone(),
            })),
            children: vec![left, right],
            output_columns: join_output_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: vec![],
                output_columns: join_output_columns,
            }),
            children: vec![join],
            output_columns: vec![
                output_col(1, "l_c0", DataType::Int64, false),
                output_col(3, "l_c2", DataType::Utf8, true),
                output_col(4, "r_c0", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

        assert_eq!(dp.edges[0].output_slot_ids, vec![1, 4]);
        let receiver = match &dp.fragments[1].root.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected Exchange root, got {other:?}"),
        };
        assert_eq!(
            receiver
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(4)]
        );
        assert_eq!(
            dp.fragments[0]
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(4)]
        );
    }

    #[test]
    fn build_distributed_plan_stream_edge_drops_redistribute_child_columns_pruned_from_source() {
        let left_columns = vec![
            output_col(1, "l_c0", DataType::Int64, false),
            output_col(2, "l_c1", DataType::Utf8, true),
        ];
        let right_columns = vec![
            output_col(9, "r_c0", DataType::Int64, false),
            output_col(10, "r_c1", DataType::Utf8, true),
            output_col(11, "r_c2", DataType::Utf8, true),
            output_col(12, "r_c3", DataType::Int64, true),
        ];
        let left = scan_node_with_columns(left_columns.clone());
        let mut right = scan_node_with_columns(right_columns.clone());
        if let PhysicalPlanKind::Scan(scan) = &mut right.kind {
            scan.required_columns = Some(vec![
                "r_c0".to_string(),
                "r_c1".to_string(),
                "r_c3".to_string(),
            ]);
        }
        let right_redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Gather,
                partition_exprs: vec![],
                output_columns: right_columns.clone(),
            }),
            children: vec![right],
            output_columns: right_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let join_output_columns = vec![
            left_columns[0].clone(),
            right_columns[1].clone(),
            right_columns[2].clone(),
        ];
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
                execution_mode: None,
                build_runtime_filters: vec![],
                output_columns: join_output_columns.clone(),
            })),
            children: vec![left, right_redistribute],
            output_columns: join_output_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: vec![],
                output_columns: join_output_columns,
            }),
            children: vec![join],
            output_columns: vec![
                left_columns[0].clone(),
                right_columns[1].clone(),
                right_columns[2].clone(),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

        assert_eq!(dp.edges.len(), 2);
        assert_eq!(dp.edges[0].output_slot_ids, vec![9, 10, 12]);
        assert_eq!(dp.edges[1].output_slot_ids, vec![1, 10]);
        let receiver = match &dp.fragments[2].root.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected Exchange root, got {other:?}"),
        };
        assert_eq!(
            receiver
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(10)]
        );
    }

    #[test]
    fn build_distributed_plan_stream_edge_uses_project_item_outputs() {
        let scan_columns = vec![
            output_col(1, "c0", DataType::Int64, false),
            output_col(2, "c1", DataType::Utf8, true),
            output_col(3, "c2", DataType::Utf8, true),
        ];
        let scan = scan_node_with_columns(scan_columns.clone());
        let project_output_columns = vec![scan_columns[0].clone(), scan_columns[2].clone()];
        let stale_node_output_columns = vec![
            scan_columns[0].clone(),
            scan_columns[1].clone(),
            scan_columns[2].clone(),
        ];
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![
                    ProjectItem {
                        expr: column_ref_expr(1, "c0", DataType::Int64, false),
                        output_name: "c0".to_string(),
                        output_column_id: ColumnId::new_for_test(1),
                    },
                    ProjectItem {
                        expr: column_ref_expr(3, "c2", DataType::Utf8, true),
                        output_name: "c2".to_string(),
                        output_column_id: ColumnId::new_for_test(3),
                    },
                ],
                output_qualifier: None,
            }),
            children: vec![scan],
            output_columns: stale_node_output_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: vec![],
                output_columns: stale_node_output_columns,
            }),
            children: vec![project],
            output_columns: vec![
                scan_columns[0].clone(),
                scan_columns[1].clone(),
                scan_columns[2].clone(),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

        assert_eq!(dp.edges[0].output_slot_ids, vec![1, 3]);
        let receiver = match &dp.fragments[1].root.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected Exchange root, got {other:?}"),
        };
        assert_eq!(
            receiver
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![ColumnId::new_for_test(1), ColumnId::new_for_test(3)]
        );
        assert_eq!(
            dp.fragments[0]
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            project_output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn build_distributed_plan_local_aggregate_stream_uses_layout_output_types() {
        let input_columns = vec![output_col(1, "v", DataType::Int64, true)];
        let scan = scan_node_with_columns(input_columns);
        let final_avg = output_col(20, "avg(v)", DataType::Float64, true);
        let local_avg_state = output_col(20, "avg(v)", DataType::Utf8, true);
        let aggregate = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
                mode: AggMode::Local,
                group_by: vec![],
                aggregates: vec![AggregateCall {
                    name: "avg".to_string(),
                    args: vec![column_ref_expr(1, "v", DataType::Int64, true)],
                    distinct: false,
                    result_type: DataType::Float64,
                    order_by: vec![],
                    output_column_id: ColumnId::new_for_test(20),
                }],
                is_merge: vec![false],
                output_layout: AggregateOutputLayout::new(vec![], vec![local_avg_state.clone()]),
                output_columns: vec![final_avg.clone()],
            })),
            children: vec![scan],
            output_columns: vec![final_avg.clone()],
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Hash {
                    cols: vec![ColumnId::new_for_test(20)],
                    source: HashSource::ShuffleAgg,
                },
                partition_exprs: vec![column_ref_expr(20, "avg(v)", DataType::Utf8, true)],
                output_columns: vec![final_avg],
            }),
            children: vec![aggregate],
            output_columns: vec![output_col(20, "avg(v)", DataType::Float64, true)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

        assert_eq!(dp.edges[0].output_slot_ids, vec![20]);
        let receiver = match &dp.fragments[1].root.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected Exchange root, got {other:?}"),
        };
        assert_eq!(receiver.output_columns.len(), 1);
        assert_eq!(
            receiver.output_columns[0].column_id,
            local_avg_state.column_id
        );
        assert_eq!(receiver.output_columns[0].data_type, DataType::Utf8);
        assert_eq!(dp.fragments[0].output_columns.len(), 1);
        assert_eq!(
            dp.fragments[0].output_columns[0].column_id,
            local_avg_state.column_id
        );
        assert_eq!(dp.fragments[0].output_columns[0].data_type, DataType::Utf8);
    }

    #[test]
    fn build_distributed_plan_hash_redistribute_rejects_missing_partition_column() {
        let scan = scan_node(1, "k");
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Hash {
                    cols: vec![ColumnId::new_for_test(1), ColumnId::new_for_test(99)],
                    source: HashSource::ShuffleJoin,
                },
                partition_exprs: vec![],
                output_columns: scan.output_columns.clone(),
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err = build_distributed_plan(&redistribute)
            .expect_err("missing hash column should be rejected");

        assert!(
            err.contains("missing hash partition columns"),
            "unexpected error: {err}"
        );
        assert!(err.contains("c99"), "unexpected error: {err}");
        assert!(err.contains("available"), "unexpected error: {err}");
    }

    #[test]
    fn build_distributed_plan_broadcast_redistribute_creates_broadcast_edge() {
        let scan = scan_node(1, "k");
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Broadcast,
                partition_exprs: vec![],
                output_columns: scan.output_columns.clone(),
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![ProjectItem {
                    expr: column_ref_expr(1, "k", DataType::Int64, false),
                    output_name: "k_alias".to_string(),
                    output_column_id: ColumnId::new_for_test(2),
                }],
                output_qualifier: None,
            }),
            children: vec![redistribute],
            output_columns: vec![output_col(2, "k_alias", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&project).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 2);
        assert_eq!(dp.edges.len(), 1);
        let root = &dp.fragments[1].root;
        let exchange = &root.children[0];
        let exchange_receiver = match &exchange.payload {
            DistributedPayload::Exchange(exchange_receiver) => exchange_receiver,
            other => panic!("expected Exchange child, got {other:?}"),
        };
        assert!(matches!(
            exchange_receiver.partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(exchange_receiver.partition.exprs.is_empty());
        assert_eq!(dp.edges[0].stream_kind, FragmentStreamKind::Broadcast);
        assert!(matches!(dp.edges[0].edge_kind, FragmentEdgeKind::Stream));
        assert!(matches!(
            dp.edges[0].output_partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert_no_physical_redistribute(root);
        assert_no_physical_redistribute(&dp.fragments[0].root);
    }

    #[test]
    fn build_distributed_plan_root_gather_is_skipped() {
        let scan = scan_node(1, "k");
        let redistribute = PhysicalPlanNode {
            kind: PhysicalPlanKind::Redistribute(RedistributeNode {
                mode: RedistributeMode::Gather,
                partition_exprs: vec![],
                output_columns: scan.output_columns.clone(),
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&redistribute).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        assert!(dp.edges.is_empty());
        let root = &dp.fragments[0].root;
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
        assert_no_physical_redistribute(root);
    }

    #[test]
    fn build_distributed_plan_cte_anchor_splits_produce_fragment_and_consume_exchange() {
        let cte_id: CteId = 7;
        let producer_columns = vec![output_col(1, "p_k", DataType::Int64, false)];
        let consumer_columns = vec![output_col(2, "c_k", DataType::Int64, false)];
        let scan = scan_node_with_columns(producer_columns.clone());
        let produce = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEProduce(LogicalCTEProduceNode {
                cte_id,
                output_columns: producer_columns.clone(),
            }),
            children: vec![scan],
            output_columns: producer_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let consume = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id,
                alias: "cte_alias".to_string(),
                output_columns: consumer_columns.clone(),
                producer_column_ids: vec![producer_columns[0].column_id],
            }),
            children: vec![],
            output_columns: consumer_columns.clone(),
            stats: stats_with_cost(),
            probe_runtime_filters: vec![],
        };
        let anchor = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEAnchor(LogicalCTEAnchorNode { cte_id }),
            children: vec![produce, consume],
            output_columns: consumer_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&anchor).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 2);
        assert_eq!(dp.root_fragment_id, 0);
        assert_eq!(dp.edges.len(), 1);

        let produce_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.cte_id == Some(cte_id))
            .expect("produce fragment");
        assert_eq!(produce_fragment.fragment_id, 1);
        assert!(matches!(produce_fragment.sink, DataSink::Noop));
        assert_eq!(
            produce_fragment.output_columns.len(),
            producer_columns.len()
        );
        assert_eq!(
            produce_fragment.output_columns[0].column_id,
            producer_columns[0].column_id
        );
        assert!(produce_fragment.cte_exchange_nodes.is_empty());
        assert!(matches!(
            &produce_fragment.root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));

        let root_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
            .expect("root fragment");
        assert!(matches!(root_fragment.sink, DataSink::Result));
        assert_eq!(root_fragment.cte_id, None);

        let project = &root_fragment.root;
        let project_payload = match &project.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Project(project)) => project,
            other => panic!("expected CTE consume remap Project root, got {other:?}"),
        };
        assert_eq!(
            project_payload.output_qualifier.as_deref(),
            Some("cte_alias")
        );
        assert_eq!(project_payload.items.len(), consumer_columns.len());
        assert_eq!(
            project_payload.items[0].output_column_id,
            consumer_columns[0].column_id
        );
        match &project_payload.items[0].expr.kind {
            ExprKind::ColumnRef { column_id, .. } => {
                assert_eq!(*column_id, producer_columns[0].column_id);
            }
            other => panic!("expected producer ColumnRef, got {other:?}"),
        }

        let exchange = project.children.first().expect("project exchange child");
        let receiver = match &exchange.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected CTE consume Exchange child, got {other:?}"),
        };
        assert_eq!(exchange.fragment_id, dp.root_fragment_id);
        assert_eq!(exchange.tuple_ids.len(), 1);
        assert!(
            exchange.stats.cost_estimate.is_none(),
            "synthetic CTE Exchange must not inherit CTEConsume cost"
        );
        assert_eq!(receiver.source_fragment_id, produce_fragment.fragment_id);
        assert!(matches!(
            receiver.partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(receiver.partition.exprs.is_empty());
        assert_eq!(receiver.output_columns.len(), producer_columns.len());
        assert_eq!(
            receiver.output_columns[0].column_id,
            producer_columns[0].column_id
        );
        assert_eq!(receiver.output_qualifier.as_deref(), Some("cte_alias"));
        let receive_producer_column_ids = match &receiver.flavor {
            ExchangeFlavor::CteMulticast {
                cte_id: flavor_cte_id,
                receive_producer_column_ids,
            } => {
                assert_eq!(*flavor_cte_id, cte_id);
                receive_producer_column_ids
            }
            other => panic!("expected CteMulticast exchange flavor, got {other:?}"),
        };
        assert_eq!(
            receive_producer_column_ids,
            &vec![producer_columns[0].column_id]
        );

        let edge = &dp.edges[0];
        assert_eq!(edge.source_fragment_id, produce_fragment.fragment_id);
        assert_eq!(edge.target_fragment_id, dp.root_fragment_id);
        assert_eq!(edge.target_exchange_node_id, exchange.node_id);
        assert_eq!(edge.stream_kind, FragmentStreamKind::Broadcast);
        assert!(matches!(
            edge.output_partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(edge.output_slot_ids.is_empty());
        match &edge.edge_kind {
            FragmentEdgeKind::CteMulticast {
                cte_id: edge_cte_id,
                receive_producer_column_ids,
            } => {
                assert_eq!(*edge_cte_id, cte_id);
                assert_eq!(
                    receive_producer_column_ids,
                    &vec![producer_columns[0].column_id]
                );
            }
            other => panic!("expected CteMulticast edge, got {other:?}"),
        }
        assert_eq!(
            root_fragment.cte_exchange_nodes,
            vec![(
                cte_id,
                exchange.node_id,
                vec![producer_columns[0].column_id]
            )]
        );
    }

    #[test]
    fn build_distributed_plan_cte_consume_remaps_pruned_producer_columns_with_project() {
        let cte_id: CteId = 8;
        let producer_columns = vec![
            output_col(1, "k", DataType::Int64, false),
            output_col(2, "v", DataType::Int64, false),
            output_col(3, "payload", DataType::Int64, false),
        ];
        let consumer_columns = vec![
            output_col(11, "k", DataType::Int64, false),
            output_col(13, "payload", DataType::Int64, false),
        ];
        let scan = scan_node_with_columns(producer_columns.clone());
        let produce = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEProduce(LogicalCTEProduceNode {
                cte_id,
                output_columns: producer_columns.clone(),
            }),
            children: vec![scan],
            output_columns: producer_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let consume = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id,
                alias: "cte_alias".to_string(),
                output_columns: consumer_columns.clone(),
                producer_column_ids: vec![
                    producer_columns[0].column_id,
                    producer_columns[2].column_id,
                ],
            }),
            children: vec![],
            output_columns: consumer_columns.clone(),
            stats: stats_with_cost(),
            probe_runtime_filters: vec![],
        };
        let anchor = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEAnchor(LogicalCTEAnchorNode { cte_id }),
            children: vec![produce, consume],
            output_columns: consumer_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&anchor).expect("build_distributed_plan");
        let produce_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.cte_id == Some(cte_id))
            .expect("produce fragment");
        let root_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
            .expect("root fragment");

        let project = &root_fragment.root;
        let DistributedPayload::Physical(PhysicalPlanKind::Project(project_payload)) =
            &project.payload
        else {
            panic!("expected CTE consume remap Project root");
        };
        assert_eq!(
            project_payload.output_qualifier.as_deref(),
            Some("cte_alias")
        );
        assert_eq!(
            project_payload
                .items
                .iter()
                .map(|item| item.output_column_id)
                .collect::<Vec<_>>(),
            consumer_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            project_payload
                .items
                .iter()
                .map(|item| match &item.expr.kind {
                    ExprKind::ColumnRef { column_id, .. } => *column_id,
                    other => panic!("expected producer ColumnRef, got {other:?}"),
                })
                .collect::<Vec<_>>(),
            vec![producer_columns[0].column_id, producer_columns[2].column_id]
        );

        let exchange = project.children.first().expect("project exchange child");
        let receiver = match &exchange.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected CTE consume Exchange child, got {other:?}"),
        };
        assert_eq!(receiver.source_fragment_id, produce_fragment.fragment_id);
        assert_eq!(receiver.output_qualifier.as_deref(), Some("cte_alias"));
        assert_eq!(
            receiver
                .output_columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            vec![producer_columns[0].column_id, producer_columns[2].column_id]
        );

        let edge = &dp.edges[0];
        assert_eq!(edge.target_exchange_node_id, exchange.node_id);
        match &edge.edge_kind {
            FragmentEdgeKind::CteMulticast {
                cte_id: edge_cte_id,
                receive_producer_column_ids,
            } => {
                assert_eq!(*edge_cte_id, cte_id);
                assert_eq!(
                    receive_producer_column_ids,
                    &vec![producer_columns[0].column_id, producer_columns[2].column_id]
                );
            }
            other => panic!("expected CteMulticast edge, got {other:?}"),
        }
        assert_eq!(
            root_fragment.cte_exchange_nodes,
            vec![(
                cte_id,
                exchange.node_id,
                vec![producer_columns[0].column_id, producer_columns[2].column_id]
            )]
        );
    }

    #[test]
    fn build_distributed_plan_cte_produce_root_fails_without_visiting_child() {
        let cte_id: CteId = 7;
        let scan = scan_node(1, "k");
        let limit = PhysicalPlanNode {
            kind: PhysicalPlanKind::Limit(PlanLimitNode {
                limit: Some(1),
                offset: None,
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let produce = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEProduce(LogicalCTEProduceNode {
                cte_id,
                output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            }),
            children: vec![limit],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err = build_distributed_plan(&produce)
            .expect_err("direct CTEProduce must fail before visiting child");

        assert!(
            err.contains("PhysicalCTEProduce emits no DistributedPlan node outside CTEAnchor"),
            "unexpected error: {err}"
        );
        assert!(
            !err.contains("PhysicalPlanKind::Limit"),
            "direct CTEProduce should fail before visiting unsupported child: {err}"
        );
    }

    #[test]
    fn build_distributed_plan_cte_anchor_rejects_non_produce_first_child() {
        let cte_id: CteId = 7;
        let scan = scan_node(1, "k");
        let consume = cte_consume_node(cte_id, 2, vec![ColumnId::new_for_test(1)]);
        let anchor = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEAnchor(LogicalCTEAnchorNode { cte_id }),
            children: vec![scan, consume],
            output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err =
            build_distributed_plan(&anchor).expect_err("CTEAnchor first child must be CTEProduce");

        assert!(
            err.contains("PhysicalCTEAnchor first child must be PhysicalCTEProduce"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_distributed_plan_cte_consume_rejects_unknown_cte_id() {
        let consume = cte_consume_node(7, 2, vec![ColumnId::new_for_test(1)]);

        let err = build_distributed_plan(&consume).expect_err("unknown CTE id should be rejected");

        assert!(
            err.contains("CTE consume references unknown cte_id=7"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_distributed_plan_cte_consume_rejects_bad_mapping() {
        let cte_id: CteId = 7;
        let producer_columns = vec![output_col(1, "p_k", DataType::Int64, false)];
        let produce = cte_produce_node(cte_id, producer_columns.clone(), scan_node(1, "p_k"));
        let bad_arity_consume = cte_consume_node(cte_id, 2, vec![]);
        let anchor = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEAnchor(LogicalCTEAnchorNode { cte_id }),
            children: vec![produce.clone(), bad_arity_consume],
            output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err = build_distributed_plan(&anchor).expect_err("bad CTE mapping should be rejected");

        assert!(
            err.contains("CTEConsume output/producers arity mismatch for cte_id=7"),
            "unexpected error: {err}"
        );

        let duplicate_output_consume = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id,
                alias: "cte_alias".to_string(),
                output_columns: vec![
                    output_col(2, "c_k", DataType::Int64, false),
                    output_col(2, "c_k_dup", DataType::Int64, false),
                ],
                producer_column_ids: vec![
                    producer_columns[0].column_id,
                    producer_columns[0].column_id,
                ],
            }),
            children: vec![],
            output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let anchor = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEAnchor(LogicalCTEAnchorNode { cte_id }),
            children: vec![produce, duplicate_output_consume],
            output_columns: vec![output_col(2, "c_k", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err = build_distributed_plan(&anchor)
            .expect_err("duplicate CTE consume output should be rejected");

        assert!(
            err.contains("CTEConsume duplicate output column 2 for cte_id=7"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn build_distributed_plan_collects_multiple_cte_exchange_nodes_in_root_tree() {
        let cte_id: CteId = 7;
        let producer_columns = vec![output_col(1, "p_k", DataType::Int64, false)];
        let produce = cte_produce_node(cte_id, producer_columns.clone(), scan_node(1, "p_k"));
        let left_consume = cte_consume_node(cte_id, 2, vec![producer_columns[0].column_id]);
        let right_consume = cte_consume_node(cte_id, 3, vec![producer_columns[0].column_id]);
        let join = PhysicalPlanNode {
            kind: PhysicalPlanKind::HashJoin(Box::new(PhysicalHashJoinNode {
                join_type: JoinKind::Inner,
                eq_conditions: vec![],
                other_condition: None,
                distribution: JoinDistribution::Broadcast,
                execution_mode: None,
                build_runtime_filters: vec![],
                output_columns: vec![
                    output_col(2, "c_k", DataType::Int64, false),
                    output_col(3, "c_k", DataType::Int64, false),
                ],
            })),
            children: vec![left_consume, right_consume],
            output_columns: vec![
                output_col(2, "c_k", DataType::Int64, false),
                output_col(3, "c_k", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };
        let anchor = PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEAnchor(LogicalCTEAnchorNode { cte_id }),
            children: vec![produce, join],
            output_columns: vec![
                output_col(2, "c_k", DataType::Int64, false),
                output_col(3, "c_k", DataType::Int64, false),
            ],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&anchor).expect("build_distributed_plan");
        let root_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
            .expect("root fragment");

        assert_eq!(root_fragment.cte_exchange_nodes.len(), 2);
        assert_eq!(dp.edges.len(), 2);
        assert!(root_fragment.cte_exchange_nodes.iter().all(
            |(exchange_cte_id, _, producer_ids)| {
                *exchange_cte_id == cte_id && producer_ids == &vec![producer_columns[0].column_id]
            }
        ));
    }

    #[test]
    fn build_distributed_plan_limit_offset_over_scan_creates_gather_exchange() {
        let scan = scan_node(1, "k");
        let limit = PhysicalPlanNode {
            kind: PhysicalPlanKind::Limit(PlanLimitNode {
                limit: Some(5),
                offset: Some(2),
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats_with_row_count_and_cost(5.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&limit).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 2);
        assert_eq!(dp.edges.len(), 1);

        let root_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
            .expect("root fragment");
        let exchange = &root_fragment.root;
        let receiver = match &exchange.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected LimitOffset Exchange root, got {other:?}"),
        };
        assert_eq!(exchange.limit, 5);
        assert_eq!(exchange.stats.output_row_count, 5.0);
        assert!(
            exchange.stats.cost_estimate.is_none(),
            "synthetic LimitOffset Exchange must not inherit Limit cost"
        );
        assert!(
            exchange.stats.broadcast_decision.is_none(),
            "synthetic LimitOffset Exchange must not inherit Limit broadcast decision"
        );
        assert!(matches!(
            receiver.partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(receiver.partition.exprs.is_empty());
        assert!(receiver.output_columns.is_empty());
        assert_eq!(receiver.output_qualifier, None);
        match &receiver.flavor {
            ExchangeFlavor::LimitOffset { limit, offset } => {
                assert_eq!(*limit, Some(5));
                assert_eq!(*offset, Some(2));
            }
            other => panic!("expected LimitOffset exchange flavor, got {other:?}"),
        }

        let edge = &dp.edges[0];
        assert_eq!(edge.source_fragment_id, receiver.source_fragment_id);
        assert_eq!(edge.target_fragment_id, dp.root_fragment_id);
        assert_eq!(edge.target_exchange_node_id, exchange.node_id);
        assert_eq!(edge.stream_kind, FragmentStreamKind::Gather);
        assert!(matches!(edge.edge_kind, FragmentEdgeKind::Stream));
        assert!(matches!(
            edge.output_partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(edge.output_slot_ids.is_empty());

        let child_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == receiver.source_fragment_id)
            .expect("child fragment");
        assert!(matches!(child_fragment.sink, DataSink::Noop));
        assert!(matches!(
            child_fragment.output_partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(matches!(
            &child_fragment.root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::Scan(_))
        ));
        assert_eq!(exchange.tuple_ids, child_fragment.root.tuple_ids);
    }

    #[test]
    fn build_distributed_plan_topn_final_split_creates_topn_exchange() {
        let scan = scan_node(1, "k");
        let sort_key = sort_item(column_ref_expr(1, "k", DataType::Int64, false));
        let topn = PhysicalPlanNode {
            kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: vec![sort_key.clone()],
                limit: Some(10),
                offset: Some(3),
                phase: TopNPhase::Final,
                is_split: true,
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats_with_row_count_and_cost(10.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&topn).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 2);
        assert_eq!(dp.edges.len(), 1);

        let root_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == dp.root_fragment_id)
            .expect("root fragment");
        let exchange = &root_fragment.root;
        let receiver = match &exchange.payload {
            DistributedPayload::Exchange(receiver) => receiver,
            other => panic!("expected TopNSplit Exchange root, got {other:?}"),
        };
        assert_eq!(exchange.limit, 10);
        assert_eq!(exchange.stats.output_row_count, 10.0);
        assert!(
            exchange.stats.cost_estimate.is_none(),
            "synthetic TopNSplit Exchange must not inherit TopN cost"
        );
        assert!(
            exchange.stats.broadcast_decision.is_none(),
            "synthetic TopNSplit Exchange must not inherit TopN broadcast decision"
        );
        assert!(matches!(
            receiver.partition.kind,
            PartitionKind::Unpartitioned
        ));
        assert!(receiver.partition.exprs.is_empty());
        assert!(receiver.output_columns.is_empty());
        assert_eq!(receiver.output_qualifier, None);
        match &receiver.flavor {
            ExchangeFlavor::TopNSplit {
                items,
                limit,
                offset,
            } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].asc, sort_key.asc);
                assert_eq!(items[0].nulls_first, sort_key.nulls_first);
                assert_column_ref(&items[0].expr, 1, "k");
                assert_eq!(*limit, Some(10));
                assert_eq!(*offset, Some(3));
            }
            other => panic!("expected TopNSplit exchange flavor, got {other:?}"),
        }

        let edge = &dp.edges[0];
        assert_eq!(edge.source_fragment_id, receiver.source_fragment_id);
        assert_eq!(edge.target_fragment_id, dp.root_fragment_id);
        assert_eq!(edge.target_exchange_node_id, exchange.node_id);
        assert_eq!(edge.stream_kind, FragmentStreamKind::Gather);
        assert!(matches!(edge.edge_kind, FragmentEdgeKind::Stream));
        assert!(matches!(
            edge.output_partition.kind,
            PartitionKind::Unpartitioned
        ));
        let child_fragment = dp
            .fragments
            .iter()
            .find(|fragment| fragment.fragment_id == receiver.source_fragment_id)
            .expect("child fragment");
        assert!(matches!(child_fragment.sink, DataSink::Noop));
        assert_eq!(
            child_fragment.output_columns[0].column_id,
            ColumnId::new_for_test(1)
        );
    }

    #[test]
    fn build_distributed_plan_limit_over_sort_collapses_into_local_sort() {
        let scan = scan_node(1, "k");
        let sort_stats = stats_with_cost();
        let sort = PhysicalPlanNode {
            kind: PhysicalPlanKind::Sort(PlanSortNode {
                items: vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
                analytic_partition_by: vec![],
                output_columns: scan.output_columns.clone(),
                offset: None,
                partition_limit: None,
                topn_type: None,
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: sort_stats.clone(),
            probe_runtime_filters: vec![],
        };
        let limit = PhysicalPlanNode {
            kind: PhysicalPlanKind::Limit(PlanLimitNode {
                limit: Some(7),
                offset: Some(4),
            }),
            children: vec![sort],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats_with_row_count(7.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&limit).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        assert!(dp.edges.is_empty());
        let root = &dp.fragments[0].root;
        let sort = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::Sort(sort)) => sort,
            other => panic!("expected Sort root, got {other:?}"),
        };
        assert_eq!(root.limit, 7);
        assert_eq!(sort.offset, Some(4));
        assert_eq!(root.stats.output_row_count, 7.0);
        assert_eq!(root.stats.cost_estimate, sort_stats.cost_estimate);
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1]);
        assert_eq!(root.children.len(), 1);
    }

    #[test]
    fn build_distributed_plan_limit_over_topn_collapses_into_local_topn() {
        let scan = scan_node(1, "k");
        let topn_stats = stats_with_cost();
        let topn = PhysicalPlanNode {
            kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
                limit: Some(100),
                offset: None,
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: topn_stats.clone(),
            probe_runtime_filters: vec![],
        };
        let limit = PhysicalPlanNode {
            kind: PhysicalPlanKind::Limit(PlanLimitNode {
                limit: Some(7),
                offset: Some(4),
            }),
            children: vec![topn],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats_with_row_count(7.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&limit).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        assert!(dp.edges.is_empty());
        let root = &dp.fragments[0].root;
        let topn = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::TopN(topn)) => topn,
            other => panic!("expected TopN root, got {other:?}"),
        };
        assert_eq!(root.limit, 7);
        assert_eq!(topn.limit, Some(7));
        assert_eq!(topn.offset, Some(4));
        assert_eq!(root.stats.output_row_count, 7.0);
        assert_eq!(root.stats.cost_estimate, topn_stats.cost_estimate);
    }

    #[test]
    fn build_distributed_plan_topn_non_split_stays_in_fragment() {
        let scan = scan_node(1, "k");
        let topn = PhysicalPlanNode {
            kind: PhysicalPlanKind::TopN(PhysicalTopNNode {
                items: vec![sort_item(column_ref_expr(1, "k", DataType::Int64, false))],
                limit: Some(3),
                offset: Some(1),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![scan],
            output_columns: vec![output_col(1, "k", DataType::Int64, false)],
            stats: stats_with_row_count(3.0),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&topn).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        assert!(dp.edges.is_empty());
        let root = &dp.fragments[0].root;
        assert_eq!(root.limit, 3);
        assert_eq!(root.node_id, 2);
        assert_eq!(root.tuple_ids, vec![1]);
        assert_eq!(root.stats.output_row_count, 3.0);
        assert!(matches!(
            &root.payload,
            DistributedPayload::Physical(PhysicalPlanKind::TopN(_))
        ));
    }

    #[test]
    fn build_distributed_plan_union_distinct_rejects_residual_distinct() {
        let output_columns = vec![output_col(1, "u_k", DataType::Int64, false)];
        let set_op = PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::UnionDistinct,
                output_columns: output_columns.clone(),
                child_output_columns: vec![output_columns.clone(), output_columns.clone()],
            }),
            children: vec![
                values_node(output_columns.clone()),
                values_node(output_columns.clone()),
            ],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err = build_distributed_plan(&set_op)
            .expect_err("residual UnionDistinct must fail before distributed build");

        assert_eq!(err, union_distinct_must_be_rewritten_error());
    }

    #[test]
    fn build_distributed_plan_set_op_rejects_empty_inputs() {
        let output_columns = vec![output_col(1, "u_k", DataType::Int64, false)];
        let set_op = PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::UnionAll,
                output_columns: output_columns.clone(),
                child_output_columns: vec![],
            }),
            children: vec![],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err =
            build_distributed_plan(&set_op).expect_err("SetOp without children must be rejected");

        assert_eq!(err, "set operation node has no inputs");
    }

    #[test]
    fn build_distributed_plan_union_all_passes_through_same_fragment() {
        let output_columns = vec![output_col(1, "u_k", DataType::Int64, false)];
        let left_columns = vec![output_col(11, "l_k", DataType::Int64, false)];
        let right_columns = vec![output_col(21, "r_k", DataType::Int64, false)];
        let set_op = PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::UnionAll,
                output_columns: output_columns.clone(),
                child_output_columns: vec![left_columns.clone(), right_columns.clone()],
            }),
            children: vec![values_node(left_columns), values_node(right_columns)],
            output_columns: output_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&set_op).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        assert!(dp.edges.is_empty());
        let root = &dp.fragments[0].root;
        let union_all = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::SetOp(set_op)) => set_op,
            other => panic!("expected SetOp root, got {other:?}"),
        };
        assert_eq!(union_all.kind, PlanSetOpKind::UnionAll);
        assert_eq!(union_all.output_columns.len(), output_columns.len());
        assert_eq!(
            union_all.output_columns[0].column_id,
            output_columns[0].column_id
        );
        assert_eq!(root.node_id, 3);
        assert_eq!(root.tuple_ids, vec![3]);
        assert_eq!(root.fragment_id, dp.root_fragment_id);
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].fragment_id, dp.root_fragment_id);
        assert_eq!(root.children[1].fragment_id, dp.root_fragment_id);
        assert!(matches!(
            &root.children[0].payload,
            DistributedPayload::Physical(PhysicalPlanKind::Values(_))
        ));
        assert!(matches!(
            &root.children[1].payload,
            DistributedPayload::Physical(PhysicalPlanKind::Values(_))
        ));
    }

    #[test]
    fn build_distributed_plan_intersect_passes_through_same_fragment() {
        let output_columns = vec![output_col(1, "u_k", DataType::Int64, false)];
        let left_columns = vec![output_col(11, "l_k", DataType::Int64, false)];
        let right_columns = vec![output_col(21, "r_k", DataType::Int64, false)];
        let set_op = PhysicalPlanNode {
            kind: PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: PlanSetOpKind::Intersect,
                output_columns: output_columns.clone(),
                child_output_columns: vec![left_columns.clone(), right_columns.clone()],
            }),
            children: vec![values_node(left_columns), values_node(right_columns)],
            output_columns: output_columns.clone(),
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let dp = build_distributed_plan(&set_op).expect("build_distributed_plan");

        assert_eq!(dp.fragments.len(), 1);
        assert!(dp.edges.is_empty());
        let root = &dp.fragments[0].root;
        let intersect = match &root.payload {
            DistributedPayload::Physical(PhysicalPlanKind::SetOp(set_op)) => set_op,
            other => panic!("expected SetOp root, got {other:?}"),
        };
        assert_eq!(intersect.kind, PlanSetOpKind::Intersect);
        assert_eq!(intersect.output_columns.len(), output_columns.len());
        assert_eq!(
            intersect.output_columns[0].column_id,
            output_columns[0].column_id
        );
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].fragment_id, dp.root_fragment_id);
        assert_eq!(root.children[1].fragment_id, dp.root_fragment_id);
    }

    #[test]
    fn build_distributed_plan_rejects_project_without_child() {
        let project = PhysicalPlanNode {
            kind: PhysicalPlanKind::Project(PlanProjectNode {
                items: vec![],
                output_qualifier: None,
            }),
            children: vec![],
            output_columns: vec![output_col(2, "k_alias", DataType::Int64, false)],
            stats: stats(),
            probe_runtime_filters: vec![],
        };

        let err =
            build_distributed_plan(&project).expect_err("Project with 0 children is malformed");

        assert!(err.contains("Project"), "unexpected error: {err}");
        assert!(
            err.contains("expected 1 children"),
            "unexpected error: {err}"
        );
        assert!(err.contains("got 0"), "unexpected error: {err}");
    }

    fn stats() -> PhysicalPlanStats {
        stats_with_row_count(0.0)
    }

    fn stats_with_row_count(output_row_count: f64) -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        }
    }

    fn stats_with_cost() -> PhysicalPlanStats {
        stats_with_row_count_and_cost(0.0)
    }

    fn stats_with_row_count_and_cost(output_row_count: f64) -> PhysicalPlanStats {
        PhysicalPlanStats {
            output_row_count,
            cost_estimate: Some(PlannerCostEstimate {
                cpu_cost: 1.0,
                memory_cost: 2.0,
                network_cost: 3.0,
            }),
            broadcast_decision: Some(crate::sql::planner::PlannerBroadcastDecision {
                feasible: true,
                forced: false,
                build_bytes: 10.0,
                hash_table_bytes: 20.0,
                effective_backend_count: 3.0,
                risk_adj_fanout_bytes: 30.0,
                per_node_budget_bytes: 40.0,
                cluster_network_budget_bytes: 50.0,
                risk_multiplier: 1.0,
                reject_reason: None,
            }),
            ..stats()
        }
    }

    fn table_def() -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: vec![column_def("k", DataType::Int64, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 1,
                table_id: 2,
            },
        }
    }

    fn table_def_with_columns(columns: &[OutputColumn]) -> TableDef {
        TableDef {
            name: "t".to_string(),
            columns: columns
                .iter()
                .map(|column| column_def(&column.name, column.data_type.clone(), column.nullable))
                .collect(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::StarRocks {
                db_id: 1,
                table_id: 2,
            },
        }
    }

    fn scan_node(column_id: u32, column_name: &str) -> PhysicalPlanNode {
        let scan_columns = vec![output_col(column_id, column_name, DataType::Int64, false)];
        scan_node_with_columns(scan_columns)
    }

    fn scan_node_with_columns(scan_columns: Vec<OutputColumn>) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Scan(PlanScanNode {
                database: "db".to_string(),
                table: table_def_with_columns(&scan_columns),
                alias: Some("t".to_string()),
                columns: scan_columns.clone(),
                predicates: vec![],
                required_columns: None,
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            children: vec![],
            output_columns: scan_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        }
    }

    fn values_node(columns: Vec<OutputColumn>) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Values(PlanValuesNode {
                rows: vec![],
                columns: columns.clone(),
            }),
            children: vec![],
            output_columns: columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        }
    }

    fn cte_produce_node(
        cte_id: CteId,
        output_columns: Vec<OutputColumn>,
        child: PhysicalPlanNode,
    ) -> PhysicalPlanNode {
        PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEProduce(LogicalCTEProduceNode {
                cte_id,
                output_columns: output_columns.clone(),
            }),
            children: vec![child],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        }
    }

    fn cte_consume_node(
        cte_id: CteId,
        output_column_id: u32,
        producer_column_ids: Vec<ColumnId>,
    ) -> PhysicalPlanNode {
        let output_columns = vec![output_col(output_column_id, "c_k", DataType::Int64, false)];
        PhysicalPlanNode {
            kind: PhysicalPlanKind::CTEConsume(LogicalCTEConsumeNode {
                cte_id,
                alias: "cte_alias".to_string(),
                output_columns: output_columns.clone(),
                producer_column_ids,
            }),
            children: vec![],
            output_columns,
            stats: stats(),
            probe_runtime_filters: vec![],
        }
    }

    fn window_expr(
        output_column: OutputColumn,
        partition_by: Vec<TypedExpr>,
        order_by: Vec<SortItem>,
    ) -> WindowExpr {
        WindowExpr {
            name: "row_number".to_string(),
            args: vec![],
            distinct: false,
            partition_by,
            order_by,
            window_frame: None,
            result_type: output_column.data_type,
            output_name: output_column.name,
            output_column_id: output_column.column_id,
            ignore_nulls: false,
        }
    }

    fn sort_item(expr: TypedExpr) -> SortItem {
        SortItem {
            expr,
            asc: true,
            nulls_first: false,
        }
    }

    fn repeat_node(with_grouping_fn_arg: bool) -> PlanRepeatNode {
        let grouping_fn_args = if with_grouping_fn_arg {
            vec![("grouping_k".to_string(), vec!["k".to_string()])]
        } else {
            vec![]
        };
        let grouping_fn_arg_ids = if with_grouping_fn_arg {
            vec![vec![ColumnId::new_for_test(1)]]
        } else {
            vec![]
        };
        let grouping_fn_ids = if with_grouping_fn_arg {
            vec![("grouping_k".to_string(), ColumnId::new_for_test(2))]
        } else {
            vec![]
        };

        PlanRepeatNode {
            repeat_column_ref_list: vec![],
            repeat_column_ref_ids: vec![],
            grouping_ids: vec![],
            all_rollup_columns: vec![],
            all_rollup_column_ids: vec![],
            grouping_key_aliases: vec![],
            grouping_fn_args,
            grouping_fn_arg_ids,
            grouping_fn_ids,
            virtual_tuple_id: None,
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

    fn bool_lit(value: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(value)),
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn int_lit(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn cmp_expr(column_id: u32, column: &str, op: BinOp, value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(column_ref_expr(column_id, column, DataType::Int64, false)),
                op,
                right: Box::new(int_lit(value)),
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

    fn assert_bool_lit(expr: &TypedExpr, expected: bool) {
        match &expr.kind {
            ExprKind::Literal(LiteralValue::Bool(value)) => assert_eq!(*value, expected),
            other => panic!("expected Bool literal, got {other:?}"),
        }
    }

    fn assert_cmp_expr(
        expr: &TypedExpr,
        expected_column_id: u32,
        expected_column: &str,
        expected_op: BinOp,
        expected_value: i64,
    ) {
        let (left, op, right) = match &expr.kind {
            ExprKind::BinaryOp { left, op, right } => (left, op, right),
            other => panic!("expected comparison expression, got {other:?}"),
        };
        assert_eq!(*op, expected_op);
        match &left.kind {
            ExprKind::ColumnRef {
                column_id, column, ..
            } => {
                assert_eq!(*column_id, ColumnId::new_for_test(expected_column_id));
                assert_eq!(column, expected_column);
            }
            other => panic!("expected column ref, got {other:?}"),
        }
        match &right.kind {
            ExprKind::Literal(LiteralValue::Int(value)) => assert_eq!(*value, expected_value),
            other => panic!("expected Int literal, got {other:?}"),
        }
    }

    fn assert_column_ref(expr: &TypedExpr, expected_column_id: u32, expected_column: &str) {
        match &expr.kind {
            ExprKind::ColumnRef {
                column_id, column, ..
            } => {
                assert_eq!(*column_id, ColumnId::new_for_test(expected_column_id));
                assert_eq!(column, expected_column);
            }
            other => panic!("expected ColumnRef, got {other:?}"),
        }
    }

    fn assert_no_physical_redistribute(
        node: &crate::sql::planner::distributed_node::DistributedNode,
    ) {
        assert!(
            !matches!(
                node.payload,
                DistributedPayload::Physical(PhysicalPlanKind::Redistribute(_))
            ),
            "DistributedPayload::Physical(Redistribute) must not be emitted"
        );
        for child in &node.children {
            assert_no_physical_redistribute(child);
        }
    }
}
