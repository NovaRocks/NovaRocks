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
use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::activation_decision::DraftRuntimeFilterGraph;
use crate::sql::planner::distributed::fragment::DistributedPlanDraft;
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, ExchangeFlavor,
    ExchangeReceiver, FragmentEdge, FragmentEdgeKind, FragmentId, FragmentStreamKind, PlanFragment,
};
use crate::sql::planner::payload::PlanProjectNode;
use crate::sql::planner::physical::TopNPhase;
use crate::sql::planner::physical::{
    PhysicalPlanKind, PhysicalPlanNode, PhysicalSetOpNode, PlanSetOpKind, RedistributeMode,
    RedistributeNode,
};

use super::lowering::{
    NodeIdAllocator, lower_fragment_local_node, lower_fragment_local_node_with_payload,
};
use super::runtime_filter_binding::RuntimeFilterBindings;
use super::union_distinct_must_be_rewritten_error;

pub(super) struct FragmentCutResult {
    pub(super) plan: DistributedPlanDraft,
    pub(super) bindings: RuntimeFilterBindings,
}

pub(super) fn cut(plan: &PhysicalPlanNode) -> Result<FragmentCutResult, String> {
    let mut builder = FragmentCutBuilder {
        ids: NodeIdAllocator::new(1, 1),
        next_fragment_id: 0,
        fragment_stack: Vec::new(),
        completed_fragments: Vec::new(),
        edges: Vec::new(),
        cte_fragments: HashMap::new(),
        bindings: RuntimeFilterBindings::new(),
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
    let bindings = builder.bindings;
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
    Ok(FragmentCutResult {
        plan: DistributedPlanDraft {
            fragments,
            root_fragment_id: Some(root_fragment_id),
            edges: builder.edges,
            runtime_filter_graph: DraftRuntimeFilterGraph::default(),
        },
        bindings,
    })
}

pub(super) struct FragmentCutBuilder {
    ids: NodeIdAllocator,
    next_fragment_id: FragmentId,
    fragment_stack: Vec<FragmentId>,
    completed_fragments: Vec<PlanFragment>,
    edges: Vec<FragmentEdge>,
    cte_fragments: HashMap<CteId, usize>,
    bindings: RuntimeFilterBindings,
}

impl FragmentCutBuilder {
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
                self.lower_local(node, fragment_id, Vec::new())
            }
            PhysicalPlanKind::Scan(_) => {
                expect_child_count(node, 0)?;
                self.lower_local(node, fragment_id, Vec::new())
            }
            PhysicalPlanKind::Project(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
            }
            PhysicalPlanKind::Filter(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
            }
            PhysicalPlanKind::Sort(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
            }
            PhysicalPlanKind::HashAggregate(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
            }
            PhysicalPlanKind::HashJoin(_) => {
                expect_child_count(node, 2)?;
                let left = self.visit(&node.children[0])?;
                let right = self.visit(&node.children[1])?;
                self.lower_local(node, fragment_id, vec![left, right])
            }
            PhysicalPlanKind::NestLoopJoin(_) => {
                expect_child_count(node, 2)?;
                let left = self.visit(&node.children[0])?;
                let right = self.visit(&node.children[1])?;
                self.lower_local(node, fragment_id, vec![left, right])
            }
            PhysicalPlanKind::AssertOneRow(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
            }
            PhysicalPlanKind::Repeat(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
            }
            PhysicalPlanKind::Window(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
            }
            PhysicalPlanKind::ChangeEventExpand(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
            }
            PhysicalPlanKind::GenerateSeries(_) => {
                expect_child_count(node, 0)?;
                self.lower_local(node, fragment_id, Vec::new())
            }
            PhysicalPlanKind::TableFunction(_) => {
                expect_child_count(node, 1)?;
                let child = self.visit(&node.children[0])?;
                self.lower_local(node, fragment_id, vec![child])
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

    fn lower_local(
        &mut self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
        children: Vec<DistributedNode>,
    ) -> Result<DistributedNode, String> {
        let lowered = lower_fragment_local_node(node, fragment_id, children, &mut self.ids)?;
        self.bindings
            .record(lowered.node_id, fragment_id, node, &lowered.payload)?;
        Ok(lowered)
    }

    fn lower_local_with_payload(
        &mut self,
        node: &PhysicalPlanNode,
        fragment_id: FragmentId,
        children: Vec<DistributedNode>,
        payload: PhysicalPlanKind,
    ) -> Result<DistributedNode, String> {
        let lowered = lower_fragment_local_node_with_payload(
            node,
            fragment_id,
            children,
            payload,
            &mut self.ids,
        )?;
        self.bindings
            .record(lowered.node_id, fragment_id, node, &lowered.payload)?;
        Ok(lowered)
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
        self.lower_local_with_payload(
            node,
            fragment_id,
            children,
            PhysicalPlanKind::SetOp(PhysicalSetOpNode {
                kind: set_op.kind,
                output_columns,
                child_output_columns: set_op.child_output_columns.clone(),
            }),
        )
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
            node.output_columns.clone(),
            output_partition,
            stream_kind,
            ExchangeFlavor::Distribution,
            -1,
            redistribute.output_columns.clone(),
            None,
            node.stats.clone(),
        )
    }

    fn visit_limit(
        &mut self,
        node: &PhysicalPlanNode,
        limit: &crate::sql::planner::payload::PlanLimitNode,
    ) -> Result<DistributedNode, String> {
        expect_child_count(node, 1)?;
        let child_plan = &node.children[0];
        let offset = limit.offset.unwrap_or(0);
        if offset > 0 && !limit_child_can_apply_offset_locally(child_plan) {
            return self.emit_stream_exchange(
                child_plan,
                node.output_columns.clone(),
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
            DistributedNodeKind::Sort(sort) => {
                sort.offset = limit.offset;
            }
            DistributedNodeKind::TopN(topn) => {
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
        topn: &crate::sql::planner::physical::PhysicalTopNNode,
    ) -> Result<DistributedNode, String> {
        expect_child_count(node, 1)?;
        let child_plan = &node.children[0];
        match (topn.phase, topn.is_split) {
            (TopNPhase::Final, true) => self.emit_stream_exchange(
                child_plan,
                node.output_columns.clone(),
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
                let fragment_id = self.current_fragment_id()?;
                self.lower_local(node, fragment_id, vec![child])
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_stream_exchange(
        &mut self,
        child_plan: &PhysicalPlanNode,
        source_output_columns: Vec<OutputColumn>,
        output_partition: DataPartition,
        stream_kind: FragmentStreamKind,
        flavor: ExchangeFlavor,
        limit: i64,
        requested_output_columns: Vec<OutputColumn>,
        output_qualifier: Option<String>,
        exchange_stats: crate::sql::planner::physical::PhysicalPlanStats,
    ) -> Result<DistributedNode, String> {
        let parent_fragment_id = self.current_fragment_id()?;
        let exchange_output_columns = if requested_output_columns.is_empty()
            && matches!(&flavor, ExchangeFlavor::Distribution)
        {
            source_output_columns.clone()
        } else {
            stream_exchange_output_columns(&source_output_columns, &requested_output_columns)
        };
        let child_fragment_id = self.alloc_fragment_id();
        self.fragment_stack.push(child_fragment_id);
        let child_result = self.visit(child_plan);
        let popped_fragment_id = self.fragment_stack.pop();
        debug_assert_eq!(popped_fragment_id, Some(child_fragment_id));
        let child = child_result?;

        let exchange_node_id = self.ids.alloc_node();
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
            vec![self.ids.alloc_tuple()]
        };

        Ok(DistributedNode {
            node_id: exchange_node_id,
            fragment_id: parent_fragment_id,
            tuple_ids: exchange_tuple_ids,
            nullable_tuple_ids: Vec::new(),
            limit,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: exchange_stats,
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
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
        _anchor: &crate::sql::planner::payload::PlanCTEAnchorNode,
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
        produce: &crate::sql::planner::payload::PlanCTEProduceNode,
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
        consume: &crate::sql::planner::payload::PlanCTEConsumeNode,
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
        let output_slot_ids = output_slot_ids_for_exchange(&exchange_output_columns)?;
        let project_items = cte_consume_remap_project_items(consume, &exchange_output_columns)?;

        let exchange_node_id = self.ids.alloc_node();
        let exchange_tuple_id = self.ids.alloc_tuple();
        let project_node_id = self.ids.alloc_node();
        let project_tuple_id = self.ids.alloc_tuple();
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
            output_slot_ids,
        });

        let exchange = DistributedNode {
            node_id: exchange_node_id,
            fragment_id: target_fragment_id,
            tuple_ids: vec![exchange_tuple_id],
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            stats: synthetic_exchange_stats(&node.stats),
            payload: DistributedNodeKind::Exchange(ExchangeReceiver {
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
            runtime_filter_binding_ids: Vec::new(),
            children: vec![exchange],
            stats: node.stats.clone(),
            payload: DistributedNodeKind::Project(PlanProjectNode {
                items: project_items,
                output_qualifier: Some(consume.alias.clone()),
            }),
        })
    }
}

fn cte_consume_exchange_output_columns(
    consume: &crate::sql::planner::payload::PlanCTEConsumeNode,
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
    consume: &crate::sql::planner::payload::PlanCTEConsumeNode,
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

pub(super) fn stream_exchange_output_columns(
    source_output_columns: &[OutputColumn],
    requested_output_columns: &[OutputColumn],
) -> Vec<OutputColumn> {
    if requested_output_columns.is_empty() {
        return Vec::new();
    }
    if source_output_columns.is_empty() {
        return requested_output_columns.to_vec();
    }

    let requested_projected =
        produced_exchange_output_columns(requested_output_columns, source_output_columns);
    if requested_projected.is_empty() {
        dedup_output_columns(source_output_columns.to_vec())
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
    consume: &crate::sql::planner::payload::PlanCTEConsumeNode,
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
    stats: &crate::sql::planner::physical::PhysicalPlanStats,
) -> crate::sql::planner::physical::PhysicalPlanStats {
    crate::sql::planner::physical::PhysicalPlanStats {
        output_row_count: stats.output_row_count,
        row_count_confidence: stats.row_count_confidence,
        column_statistics: stats.column_statistics.clone(),
        cost_estimate: None,
        broadcast_decision: None,
    }
}

fn limit_stats_with_child_cost(
    limit_stats: &crate::sql::planner::physical::PhysicalPlanStats,
    child_stats: &crate::sql::planner::physical::PhysicalPlanStats,
) -> crate::sql::planner::physical::PhysicalPlanStats {
    crate::sql::planner::physical::PhysicalPlanStats {
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
    if let DistributedNodeKind::Exchange(exchange) = &node.payload
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
