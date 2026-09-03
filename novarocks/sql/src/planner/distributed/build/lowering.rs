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

use crate::planner::distributed::{
    DistributedNode, DistributedNodeKind, ExchangeFlavor, FragmentId,
    distributed_kind_from_physical,
};
use crate::planner::optimizer_bridge::property::{
    ordering_spec_from_sort_items, window_ordering_spec,
};
use crate::planner::ordering::OrderingSpec;
use crate::planner::payload::WindowExpr;
use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};

pub(super) struct NodeIdAllocator {
    next_node_id: i32,
    next_tuple_id: i32,
}

impl NodeIdAllocator {
    pub(super) fn new(next_node_id: i32, next_tuple_id: i32) -> Self {
        Self {
            next_node_id,
            next_tuple_id,
        }
    }

    pub(super) fn alloc_node(&mut self) -> i32 {
        let node_id = self.next_node_id;
        self.next_node_id += 1;
        node_id
    }

    pub(super) fn alloc_tuple(&mut self) -> i32 {
        let tuple_id = self.next_tuple_id;
        self.next_tuple_id += 1;
        tuple_id
    }
}

pub(super) fn lower_fragment_local_node(
    physical: &PhysicalPlanNode,
    fragment_id: FragmentId,
    children: Vec<DistributedNode>,
    ids: &mut NodeIdAllocator,
) -> Result<DistributedNode, String> {
    lower_fragment_local_node_with_payload(
        physical,
        fragment_id,
        children,
        physical.kind.clone(),
        ids,
    )
}

pub(super) fn lower_fragment_local_node_with_payload(
    physical: &PhysicalPlanNode,
    fragment_id: FragmentId,
    children: Vec<DistributedNode>,
    mut payload: PhysicalPlanKind,
    ids: &mut NodeIdAllocator,
) -> Result<DistributedNode, String> {
    let (node_id, tuple_ids) = match &mut payload {
        PhysicalPlanKind::Values(_) => {
            let tuple_id = ids.alloc_tuple();
            let node_id = ids.alloc_node();
            (node_id, vec![tuple_id])
        }
        PhysicalPlanKind::Scan(_) => {
            let node_id = ids.alloc_node();
            let tuple_id = ids.alloc_tuple();
            (node_id, vec![tuple_id])
        }
        PhysicalPlanKind::Project(_) | PhysicalPlanKind::Unpivot(_) => {
            let node_id = ids.alloc_node();
            let tuple_id = ids.alloc_tuple();
            (node_id, vec![tuple_id])
        }
        PhysicalPlanKind::Filter(_)
        | PhysicalPlanKind::Sort(_)
        | PhysicalPlanKind::AssertOneRow(_) => {
            let node_id = ids.alloc_node();
            let tuple_ids = children[0].tuple_ids.clone();
            (node_id, tuple_ids)
        }
        PhysicalPlanKind::HashAggregate(_) | PhysicalPlanKind::ChangeEventExpand(_) => {
            let tuple_id = ids.alloc_tuple();
            let node_id = ids.alloc_node();
            (node_id, vec![tuple_id])
        }
        PhysicalPlanKind::HashJoin(_) | PhysicalPlanKind::NestLoopJoin(_) => {
            let node_id = ids.alloc_node();
            let mut tuple_ids = children[0].tuple_ids.clone();
            tuple_ids.extend(children[1].tuple_ids.iter().copied());
            (node_id, tuple_ids)
        }
        PhysicalPlanKind::Repeat(repeat) => {
            let node_id = ids.alloc_node();
            let virtual_tuple_id = ids.alloc_tuple();
            let mut tuple_ids = children[0].tuple_ids.clone();
            if !repeat.grouping_fn_args.is_empty() {
                tuple_ids.push(virtual_tuple_id);
            }
            repeat.virtual_tuple_id = Some(virtual_tuple_id);
            (node_id, tuple_ids)
        }
        PhysicalPlanKind::Window(window) => {
            let groups = group_win_exprs_by_sig(&window.window_exprs);
            if groups.is_empty() {
                return Err(
                    "build_distributed_plan: PhysicalWindow has no window expressions".to_string(),
                );
            }

            let child = &children[0];
            let mut first_node_id = None;
            let mut tuple_ids = child.tuple_ids.clone();
            let mut current_ordering = distributed_node_ordering(child);
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
                        let sort_node_id = ids.alloc_node();
                        first_node_id.get_or_insert(sort_node_id);
                        current_ordering = required_ordering;
                    }
                }
                let analytic_node_id = ids.alloc_node();
                first_node_id.get_or_insert(analytic_node_id);
                let _ = ids.alloc_tuple();
                let output_tuple_id = ids.alloc_tuple();
                tuple_ids.push(output_tuple_id);
            }

            let node_id = first_node_id.ok_or_else(|| {
                "build_distributed_plan: PhysicalWindow produced no distributed node".to_string()
            })?;
            (node_id, tuple_ids)
        }
        PhysicalPlanKind::GenerateSeries(_) => {
            let _ = ids.alloc_tuple();
            let _ = ids.alloc_node();
            let tuple_id = ids.alloc_tuple();
            let node_id = ids.alloc_node();
            (node_id, vec![tuple_id])
        }
        PhysicalPlanKind::TableFunction(_) => {
            let _ = ids.alloc_tuple();
            let _ = ids.alloc_node();
            let tuple_id = ids.alloc_tuple();
            let node_id = ids.alloc_node();
            (node_id, vec![tuple_id])
        }
        PhysicalPlanKind::SetOp(_) => {
            let tuple_id = ids.alloc_tuple();
            let node_id = ids.alloc_node();
            (node_id, vec![tuple_id])
        }
        PhysicalPlanKind::TopN(topn) => {
            let node_id = ids.alloc_node();
            let tuple_ids = children[0].tuple_ids.clone();
            let limit = topn.limit.unwrap_or(-1);
            let mut node = make_node(physical, fragment_id, node_id, tuple_ids, children, payload)?;
            node.limit = limit;
            return Ok(node);
        }
        _ => {
            return Err(
                "build_distributed_plan internal error: control node reached lowering".to_string(),
            );
        }
    };

    make_node(physical, fragment_id, node_id, tuple_ids, children, payload)
}

fn make_node(
    physical: &PhysicalPlanNode,
    fragment_id: FragmentId,
    node_id: i32,
    tuple_ids: Vec<i32>,
    children: Vec<DistributedNode>,
    payload: PhysicalPlanKind,
) -> Result<DistributedNode, String> {
    Ok(DistributedNode {
        node_id,
        fragment_id,
        tuple_ids,
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children,
        stats: physical.stats.clone(),
        payload: distributed_kind_from_physical(payload)?,
    })
}

pub(super) fn distributed_node_ordering(node: &DistributedNode) -> OrderingSpec {
    match &node.payload {
        DistributedNodeKind::Sort(sort) => ordering_spec_from_sort_items(&sort.items),
        DistributedNodeKind::TopN(topn) => ordering_spec_from_sort_items(&topn.items),
        DistributedNodeKind::Exchange(exchange) => match &exchange.flavor {
            ExchangeFlavor::TopNSplit { items, .. } => ordering_spec_from_sort_items(items),
            _ => OrderingSpec::Any,
        },
        DistributedNodeKind::AssertOneRow(_) => node
            .children
            .first()
            .map(distributed_node_ordering)
            .unwrap_or(OrderingSpec::Any),
        DistributedNodeKind::Window(window) => {
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

fn group_win_exprs_by_sig(exprs: &[WindowExpr]) -> Vec<Vec<usize>> {
    let sig = |expr: &WindowExpr| -> String {
        format!(
            "{:?}|{:?}|{:?}",
            expr.partition_by
                .iter()
                .map(|partition| format!("{:?}", partition.kind))
                .collect::<Vec<_>>(),
            expr.order_by
                .iter()
                .map(|ordering| format!("{:?}:{}", ordering.expr.kind, ordering.asc))
                .collect::<Vec<_>>(),
            expr.window_frame,
        )
    };
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (index, expr) in exprs.iter().enumerate() {
        let signature = sig(expr);
        if let Some(group) = groups
            .iter_mut()
            .find(|(group_signature, _)| *group_signature == signature)
        {
            group.1.push(index);
        } else {
            groups.push((signature, vec![index]));
        }
    }
    groups.into_iter().map(|(_, indices)| indices).collect()
}
