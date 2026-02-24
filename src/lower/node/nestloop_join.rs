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
use arrow::datatypes::DataType;

use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::lower::expr::lower_t_expr;
use crate::lower::layout::{Layout, schema_for_layout};
use crate::lower::node::Lowered;

use crate::{descriptors, plan_nodes, types};

/// Lower a NESTLOOP_JOIN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_nestloop_join_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    if children.len() != 2 {
        return Err(format!(
            "NESTLOOP_JOIN_NODE expected 2 children, got {}",
            children.len()
        ));
    }

    let mut it = children.into_iter();
    let left = it.next().expect("left");
    let right = it.next().expect("right");
    let Some(nl) = node.nestloop_join_node.as_ref() else {
        return Err("NESTLOOP_JOIN_NODE missing nestloop_join_node payload".to_string());
    };

    let op = nl
        .join_op
        .ok_or_else(|| "NESTLOOP_JOIN_NODE missing join_op".to_string())?;
    let join_type = match op {
        plan_nodes::TJoinOp::INNER_JOIN => NestedLoopJoinType::Inner,
        plan_nodes::TJoinOp::CROSS_JOIN => NestedLoopJoinType::Cross,
        plan_nodes::TJoinOp::LEFT_OUTER_JOIN => NestedLoopJoinType::LeftOuter,
        plan_nodes::TJoinOp::RIGHT_OUTER_JOIN => NestedLoopJoinType::RightOuter,
        plan_nodes::TJoinOp::FULL_OUTER_JOIN => NestedLoopJoinType::FullOuter,
        plan_nodes::TJoinOp::LEFT_SEMI_JOIN => NestedLoopJoinType::LeftSemi,
        plan_nodes::TJoinOp::LEFT_ANTI_JOIN => NestedLoopJoinType::LeftAnti,
        plan_nodes::TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN => NestedLoopJoinType::NullAwareLeftAnti,
        other => {
            return Err(format!(
                "unsupported NESTLOOP_JOIN_NODE join_op={other:?} (supported: INNER/CROSS/LEFT_OUTER/RIGHT_OUTER/FULL_OUTER/LEFT_SEMI/LEFT_ANTI/NULL_AWARE_LEFT_ANTI)"
            ));
        }
    };

    let mut order = Vec::with_capacity(left.layout.order.len() + right.layout.order.len());
    order.extend_from_slice(&left.layout.order);
    order.extend_from_slice(&right.layout.order);
    let index = order.iter().enumerate().map(|(i, key)| (*key, i)).collect();
    let layout = Layout { order, index };

    let mut join_conjunct: Option<crate::exec::expr::ExprId> = None;
    if let Some(exprs) = nl.join_conjuncts.as_ref().filter(|v| !v.is_empty()) {
        let mut lowered = Vec::with_capacity(exprs.len());
        for e in exprs {
            lowered.push(lower_t_expr(e, arena, &layout, last_query_id, fe_addr)?);
        }
        let mut it = lowered.into_iter();
        let Some(first) = it.next() else {
            return Err("NESTLOOP_JOIN_NODE join_conjuncts is empty".to_string());
        };
        let mut acc = first;
        for next in it {
            acc = arena.push_typed(ExprNode::And(acc, next), DataType::Boolean);
        }
        join_conjunct = Some(acc);
    }

    let output_layout = match join_type {
        NestedLoopJoinType::LeftSemi
        | NestedLoopJoinType::LeftAnti
        | NestedLoopJoinType::NullAwareLeftAnti => left.layout.clone(),
        NestedLoopJoinType::Inner
        | NestedLoopJoinType::Cross
        | NestedLoopJoinType::LeftOuter
        | NestedLoopJoinType::RightOuter
        | NestedLoopJoinType::FullOuter => layout.clone(),
    };

    let Some(desc_tbl) = desc_tbl else {
        return Err("NESTLOOP_JOIN_NODE requires desc_tbl for schema".to_string());
    };
    let left_schema = schema_for_layout(desc_tbl, &left.layout)?;
    let right_schema = schema_for_layout(desc_tbl, &right.layout)?;
    let join_scope_schema = schema_for_layout(desc_tbl, &layout)?;

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                join_conjunct,
                left_schema,
                right_schema,
                join_scope_schema,
            }),
        },
        layout: output_layout,
    })
}
