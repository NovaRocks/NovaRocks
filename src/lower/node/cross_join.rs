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
use crate::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
use crate::exec::node::{ExecNode, ExecNodeKind};

use crate::lower::layout::{Layout, schema_for_layout};
use crate::lower::node::Lowered;

use crate::{descriptors, plan_nodes};

/// Lower a CROSS_JOIN_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_cross_join_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
) -> Result<Lowered, String> {
    if children.len() != 2 {
        return Err(format!(
            "CROSS_JOIN_NODE expected 2 children, got {}",
            children.len()
        ));
    }

    let mut it = children.into_iter();
    let left = it.next().expect("left");
    let right = it.next().expect("right");

    let mut order = Vec::with_capacity(left.layout.order.len() + right.layout.order.len());
    order.extend_from_slice(&left.layout.order);
    order.extend_from_slice(&right.layout.order);
    let index = order.iter().enumerate().map(|(i, key)| (*key, i)).collect();
    let layout = Layout { order, index };

    let Some(desc_tbl) = desc_tbl else {
        return Err("CROSS_JOIN_NODE requires desc_tbl for schema".to_string());
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
                join_type: NestedLoopJoinType::Cross,
                join_conjunct: None,
                left_schema,
                right_schema,
                join_scope_schema,
            }),
        },
        layout,
    })
}
