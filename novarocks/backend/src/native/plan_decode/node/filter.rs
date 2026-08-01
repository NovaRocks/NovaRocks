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

use super::{DecodedNode, NativePlanDecodeContext};
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::filter::FilterNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::plan;
use novarocks::protocol::common::error::FieldPath;

pub(super) fn lower_filter_node(
    node: &plan::DistributedNode,
    filter: &plan::FilterNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, crate::native::plan_decode::error::NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let predicate = filter.predicate.as_ref().ok_or_else(|| {
        crate::native::plan_decode::error::NativeFragmentDecodeError::missing(
            path.clone().field("predicate"),
            "native FilterNode requires predicate",
        )
    })?;
    let predicate =
        ctx.decode_expression(predicate, path.field("predicate"), arena, &child.layout)?;
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Filter(FilterNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                predicate,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}
