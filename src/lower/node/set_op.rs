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
use crate::common::ids::SlotId;
use crate::exec::expr::ExprArena;
use crate::exec::node::project::ProjectNode;
use crate::exec::node::set_op::{SetOpKind, SetOpNode};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::exprs;
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::{Layout, layout_from_slot_ids};
use crate::lower::node::Lowered;
use crate::{plan_nodes, types};

pub(crate) fn lower_intersect_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    arena: &mut ExprArena,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let Some(intersect) = node.intersect_node.as_ref() else {
        return Err("INTERSECT_NODE missing intersect_node payload".to_string());
    };
    lower_distinct_set_node(
        children,
        node,
        out_layout,
        arena,
        last_query_id,
        fe_addr,
        intersect.tuple_id,
        &intersect.result_expr_lists,
        "INTERSECT_NODE",
        SetOpKind::Intersect,
    )
}

pub(crate) fn lower_except_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    arena: &mut ExprArena,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let Some(except) = node.except_node.as_ref() else {
        return Err("EXCEPT_NODE missing except_node payload".to_string());
    };
    lower_distinct_set_node(
        children,
        node,
        out_layout,
        arena,
        last_query_id,
        fe_addr,
        except.tuple_id,
        &except.result_expr_lists,
        "EXCEPT_NODE",
        SetOpKind::Except,
    )
}

fn lower_distinct_set_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    mut out_layout: Layout,
    arena: &mut ExprArena,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
    tuple_id: types::TTupleId,
    result_expr_lists: &[Vec<exprs::TExpr>],
    op_name: &'static str,
    set_op_kind: SetOpKind,
) -> Result<Lowered, String> {
    if children.len() < 2 {
        return Err(format!(
            "{op_name} expected >=2 children, got {}",
            children.len()
        ));
    }
    if out_layout.order.is_empty() {
        let col_count = result_expr_lists.first().map(|r| r.len()).unwrap_or(0);
        if col_count == 0 {
            return Err(format!("{op_name} cannot infer output columns"));
        }
        out_layout = layout_from_slot_ids(tuple_id, (0..col_count).map(|i| i as types::TSlotId));
    }

    if result_expr_lists.len() != children.len() {
        return Err(format!(
            "{op_name} result_expr_lists size mismatch: expr_lists={} children={}",
            result_expr_lists.len(),
            children.len()
        ));
    }

    let output_slots = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<Vec<_>, _>>()?;

    let mut inputs = Vec::with_capacity(children.len());
    for (child, expr_list) in children.into_iter().zip(result_expr_lists.iter()) {
        let mut exprs = Vec::with_capacity(expr_list.len());
        for e in expr_list {
            exprs.push(lower_t_expr(
                e,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
            )?);
        }
        inputs.push(ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids: output_slots.clone(),
                output_indices: None,
                output_slots: output_slots.clone(),
            }),
        });
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::SetOp(SetOpNode {
                kind: set_op_kind,
                inputs,
                node_id: node.node_id,
                output_slots,
            }),
        },
        layout: out_layout,
    })
}
