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
pub mod aggregate;
pub mod analytic;
pub mod assert;
pub mod exchange_source;
pub mod fetch;
pub mod filter;
pub mod join;
pub mod limit;
pub mod lookup;
pub mod nljoin;
pub mod project;
pub mod repeat;
pub mod scan;
pub mod set_op;
pub mod sort;
pub mod table_function;
pub mod union_all;
pub mod values;

use crate::common::ids::SlotId;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::exec::node::aggregate::AggregateNode;
use crate::exec::node::analytic::AnalyticNode;
use crate::exec::node::assert::AssertNumRowsNode;
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::node::fetch::FetchNode;
use crate::exec::node::filter::FilterNode;
use crate::exec::node::join::{JoinNode, JoinType};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::lookup::LookUpNode;
use crate::exec::node::nljoin::NestedLoopJoinNode;
use crate::exec::node::project::ProjectNode;
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::scan::ScanNode;
use crate::exec::node::set_op::SetOpNode;
use crate::exec::node::sort::SortNode;
use crate::exec::node::table_function::TableFunctionNode;
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::values::ValuesNode;
use std::collections::HashSet;

pub type ExecResult = Result<Chunk, String>;
pub type BoxedExecIter = Box<dyn Iterator<Item = ExecResult> + Send>;

#[derive(Clone, Debug)]
pub struct RuntimeFilterProbeSpec {
    pub filter_id: i32,
    pub expr_id: ExprId,
    pub slot_id: SlotId,
}

#[derive(Clone, Debug)]
pub enum ExecNodeKind {
    AssertNumRows(AssertNumRowsNode),
    Values(ValuesNode),
    Project(ProjectNode),
    Filter(FilterNode),
    Repeat(RepeatNode),
    UnionAll(UnionAllNode),
    Limit(LimitNode),
    ExchangeSource(ExchangeSourceNode),
    Scan(ScanNode),
    Fetch(FetchNode),
    LookUp(LookUpNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    NestedLoopJoin(NestedLoopJoinNode),
    Sort(SortNode),
    TableFunction(TableFunctionNode),
    Analytic(AnalyticNode),
    SetOp(SetOpNode),
}

#[derive(Clone, Debug)]
pub struct ExecNode {
    pub kind: ExecNodeKind,
}

#[derive(Clone, Debug)]
pub struct ExecPlan {
    pub arena: ExprArena,
    pub root: ExecNode,
}

#[derive(Clone)]
struct RuntimeFilterPushSpec {
    filter_id: i32,
    expr_id: ExprId,
    slot_id: SlotId,
}

fn expr_slot_ref(arena: &ExprArena, expr_id: ExprId) -> Option<SlotId> {
    match arena.node(expr_id) {
        Some(ExprNode::SlotId(slot_id)) => Some(*slot_id),
        _ => None,
    }
}

fn collect_expr_slots(arena: &ExprArena, expr_id: ExprId, out: &mut HashSet<SlotId>) {
    let mut stack = vec![expr_id];
    while let Some(id) = stack.pop() {
        let Some(node) = arena.node(id) else { continue };
        match node {
            ExprNode::Literal(_) => {}
            ExprNode::SlotId(slot_id) => {
                out.insert(*slot_id);
            }
            ExprNode::ArrayExpr { elements } => {
                for child in elements {
                    stack.push(*child);
                }
            }
            ExprNode::StructExpr { fields } => {
                for child in fields {
                    stack.push(*child);
                }
            }
            ExprNode::LambdaFunction { .. } => {
                // Do not descend into nested lambdas when collecting slots.
            }
            ExprNode::DictDecode { child, .. } => {
                stack.push(*child);
            }
            ExprNode::Cast(child)
            | ExprNode::CastTime(child)
            | ExprNode::CastTimeFromDatetime(child)
            | ExprNode::Not(child)
            | ExprNode::IsNull(child)
            | ExprNode::IsNotNull(child)
            | ExprNode::Clone(child) => {
                stack.push(*child);
            }
            ExprNode::Add(a, b)
            | ExprNode::Sub(a, b)
            | ExprNode::Mul(a, b)
            | ExprNode::Div(a, b)
            | ExprNode::Mod(a, b)
            | ExprNode::Eq(a, b)
            | ExprNode::EqForNull(a, b)
            | ExprNode::Ne(a, b)
            | ExprNode::Lt(a, b)
            | ExprNode::Le(a, b)
            | ExprNode::Gt(a, b)
            | ExprNode::Ge(a, b)
            | ExprNode::And(a, b)
            | ExprNode::Or(a, b) => {
                stack.push(*a);
                stack.push(*b);
            }
            ExprNode::In { child, values, .. } => {
                stack.push(*child);
                for value in values {
                    stack.push(*value);
                }
            }
            ExprNode::Case { children, .. } => {
                for child in children {
                    stack.push(*child);
                }
            }
            ExprNode::FunctionCall { args, .. } => {
                for arg in args {
                    stack.push(*arg);
                }
            }
        }
    }
}

fn expr_slot_ids(arena: &ExprArena, expr_id: ExprId) -> HashSet<SlotId> {
    let mut out = HashSet::new();
    collect_expr_slots(arena, expr_id, &mut out);
    out
}

fn output_slots_for_node(node: &ExecNode) -> Option<HashSet<SlotId>> {
    match &node.kind {
        ExecNodeKind::Project(ProjectNode { output_slots, .. }) => {
            Some(output_slots.iter().copied().collect())
        }
        ExecNodeKind::Aggregate(AggregateNode { output_slots, .. }) => {
            Some(output_slots.iter().copied().collect())
        }
        ExecNodeKind::Analytic(AnalyticNode { output_slots, .. }) => {
            Some(output_slots.iter().copied().collect())
        }
        ExecNodeKind::SetOp(SetOpNode { output_slots, .. }) => {
            Some(output_slots.iter().copied().collect())
        }
        ExecNodeKind::ExchangeSource(exchange) => {
            Some(exchange.output_slots().iter().copied().collect())
        }
        ExecNodeKind::Scan(scan) => Some(scan.output_slots().iter().copied().collect()),
        ExecNodeKind::Fetch(fetch) => Some(fetch.output_slots().iter().copied().collect()),
        ExecNodeKind::LookUp(lookup) => Some(lookup.output_slots().iter().copied().collect()),
        ExecNodeKind::AssertNumRows(AssertNumRowsNode { input, .. }) => {
            output_slots_for_node(input)
        }
        ExecNodeKind::Filter(FilterNode { input, .. }) => output_slots_for_node(input),
        ExecNodeKind::Repeat(RepeatNode { input, .. }) => output_slots_for_node(input),
        ExecNodeKind::Limit(LimitNode { input, .. }) => output_slots_for_node(input),
        ExecNodeKind::Sort(SortNode { input, .. }) => output_slots_for_node(input),
        ExecNodeKind::TableFunction(TableFunctionNode { output_slots, .. }) => {
            Some(output_slots.iter().copied().collect())
        }
        ExecNodeKind::UnionAll(UnionAllNode { inputs, .. }) => {
            inputs.first().and_then(output_slots_for_node)
        }
        ExecNodeKind::Values(_) => None,
        ExecNodeKind::Join(_) | ExecNodeKind::NestedLoopJoin(_) => None,
    }
}

fn filter_specs_by_output_slots(
    arena: &ExprArena,
    specs: &[RuntimeFilterPushSpec],
    output_slots: &HashSet<SlotId>,
) -> Vec<RuntimeFilterPushSpec> {
    specs
        .iter()
        .filter(|spec| {
            let slots = expr_slot_ids(arena, spec.expr_id);
            slots.is_empty() || slots.iter().all(|slot| output_slots.contains(slot))
        })
        .cloned()
        .collect()
}

fn filter_specs_for_child(
    arena: &ExprArena,
    specs: &[RuntimeFilterPushSpec],
    child: &ExecNode,
) -> Vec<RuntimeFilterPushSpec> {
    let Some(output_slots) = output_slots_for_node(child) else {
        return specs.to_vec();
    };
    filter_specs_by_output_slots(arena, specs, &output_slots)
}

pub(crate) fn push_down_local_runtime_filters(root: &mut ExecNode, arena: &ExprArena) {
    let inherited = Vec::new();
    push_down_local_runtime_filters_inner(root, arena, &inherited);
}

fn push_down_local_runtime_filters_inner(
    node: &mut ExecNode,
    arena: &ExprArena,
    inherited: &[RuntimeFilterPushSpec],
) {
    match &mut node.kind {
        ExecNodeKind::AssertNumRows(AssertNumRowsNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Values(_) => {}
        ExecNodeKind::Project(ProjectNode {
            input,
            exprs,
            output_indices,
            output_slots,
            ..
        }) => {
            if inherited.is_empty() {
                return;
            }

            let mut rewritten = Vec::new();
            for spec in inherited {
                let Some(slot_id) = expr_slot_ref(arena, spec.expr_id) else {
                    continue;
                };
                let Some(pos) = output_slots.iter().position(|s| *s == slot_id) else {
                    continue;
                };
                let expr_idx = output_indices
                    .as_ref()
                    .and_then(|indices| indices.get(pos).copied())
                    .unwrap_or(pos);
                let Some(&new_expr_id) = exprs.get(expr_idx) else {
                    continue;
                };
                let mut next = spec.clone();
                next.expr_id = new_expr_id;
                let expr_slots = expr_slot_ids(arena, new_expr_id);
                if expr_slots.len() == 1 {
                    if let Some(slot) = expr_slots.iter().next() {
                        next.slot_id = *slot;
                    }
                }
                rewritten.push(next);
            }
            let filtered = filter_specs_for_child(arena, &rewritten, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Filter(FilterNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Repeat(RepeatNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::UnionAll(UnionAllNode { inputs, .. }) => {
            for input in inputs {
                let filtered = filter_specs_for_child(arena, inherited, input);
                push_down_local_runtime_filters_inner(input, arena, &filtered);
            }
        }
        ExecNodeKind::Limit(LimitNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::TableFunction(TableFunctionNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::ExchangeSource(exchange) => {
            if inherited.is_empty() {
                return;
            }
            let output_slots: HashSet<SlotId> = exchange.output_slots().iter().copied().collect();
            let filtered = filter_specs_by_output_slots(arena, inherited, &output_slots);
            if filtered.is_empty() {
                return;
            }
            let specs: Vec<RuntimeFilterProbeSpec> = filtered
                .iter()
                .map(|spec| RuntimeFilterProbeSpec {
                    filter_id: spec.filter_id,
                    expr_id: spec.expr_id,
                    slot_id: spec.slot_id,
                })
                .collect();
            exchange.add_runtime_filter_specs(&specs);
        }
        ExecNodeKind::Scan(scan) => {
            if inherited.is_empty() {
                return;
            }
            let output_slots: HashSet<SlotId> = scan.output_slots().iter().copied().collect();
            let filtered = filter_specs_by_output_slots(arena, inherited, &output_slots);
            if filtered.is_empty() {
                return;
            }
            let specs: Vec<RuntimeFilterProbeSpec> = filtered
                .iter()
                .map(|spec| RuntimeFilterProbeSpec {
                    filter_id: spec.filter_id,
                    expr_id: spec.expr_id,
                    slot_id: spec.slot_id,
                })
                .collect();
            scan.add_runtime_filter_specs(&specs);
        }
        ExecNodeKind::Fetch(FetchNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::LookUp(_) => {}
        ExecNodeKind::Aggregate(AggregateNode {
            input, group_by, ..
        }) => {
            if inherited.is_empty() {
                return;
            }
            let mut group_by_slots = HashSet::new();
            for expr_id in group_by {
                if let Some(slot_id) = expr_slot_ref(arena, *expr_id) {
                    group_by_slots.insert(slot_id);
                }
            }
            let mut pushable = Vec::new();
            for spec in inherited {
                let Some(slot_id) = expr_slot_ref(arena, spec.expr_id) else {
                    continue;
                };
                if group_by_slots.contains(&slot_id) {
                    pushable.push(spec.clone());
                }
            }
            let filtered = filter_specs_for_child(arena, &pushable, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Join(JoinNode {
            left,
            right,
            node_id: _node_id,
            join_type,
            probe_keys,
            build_keys: _build_keys,
            runtime_filters,
            ..
        }) => {
            // StarRocks plans define the probe side as the left child and the build side
            // as the right child. Do not swap based on join type.
            let probe_child = left.as_mut();
            let build_child = right.as_mut();
            let probe_exprs = probe_keys;

            let mut probe_filters = inherited.to_vec();
            if matches!(
                *join_type,
                JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
            ) {
                if !runtime_filters.is_empty() {
                    for rf in runtime_filters {
                        if let Some(expr_id) = probe_exprs.get(rf.expr_order) {
                            probe_filters.push(RuntimeFilterPushSpec {
                                filter_id: rf.filter_id,
                                expr_id: *expr_id,
                                slot_id: rf.probe_slot_id,
                            });
                        }
                    }
                }
            }

            let probe_filters = filter_specs_for_child(arena, &probe_filters, probe_child);
            let build_filters = filter_specs_for_child(arena, inherited, build_child);

            push_down_local_runtime_filters_inner(probe_child, arena, &probe_filters);
            push_down_local_runtime_filters_inner(build_child, arena, &build_filters);
        }
        ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode { left, right, .. }) => {
            let left_filters = filter_specs_for_child(arena, inherited, left);
            let right_filters = filter_specs_for_child(arena, inherited, right);
            push_down_local_runtime_filters_inner(left, arena, &left_filters);
            push_down_local_runtime_filters_inner(right, arena, &right_filters);
        }
        ExecNodeKind::Sort(SortNode { input, .. }) => {
            let filtered = filter_specs_for_child(arena, inherited, input);
            push_down_local_runtime_filters_inner(input, arena, &filtered);
        }
        ExecNodeKind::Analytic(AnalyticNode { .. }) => {}
        ExecNodeKind::SetOp(SetOpNode { inputs, .. }) => {
            for input in inputs {
                let filtered = filter_specs_for_child(arena, inherited, input);
                push_down_local_runtime_filters_inner(input, arena, &filtered);
            }
        }
    }
}
