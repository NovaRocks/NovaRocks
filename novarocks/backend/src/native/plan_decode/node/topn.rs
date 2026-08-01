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

use super::common::{merge_limits, parse_distributed_limit, parse_optional_nonnegative_i64};
use super::sort::{lower_sort_items_with_context, parse_sort_topn_type};
use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::sort::SortNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::plan;
use novarocks::protocol::common::error::FieldPath;

pub(super) fn lower_topn_node(
    node: &plan::DistributedNode,
    topn: &plan::TopNNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let payload_limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        parse_optional_nonnegative_i64(topn.limit, "TopNNode.limit"),
    )?;
    let outer_limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        parse_distributed_limit(node.limit, "TopNNode DistributedNode.limit"),
    )?;
    let limit = NativeFragmentDecodeError::map_invalid(
        path.clone().field("limit"),
        merge_limits("TopNNode", payload_limit, outer_limit),
    )?;
    if limit.is_none() {
        return Err(NativeFragmentDecodeError::missing(
            path.clone().field("limit"),
            "TopNNode requires a non-negative limit",
        ));
    }
    let offset = NativeFragmentDecodeError::map_invalid(
        path.clone().field("offset"),
        parse_optional_nonnegative_i64(topn.offset, "TopNNode.offset"),
    )?
    .unwrap_or(0);
    let phase = plan::TopNPhase::try_from(topn.phase).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("phase"),
            format!("TopNNode unknown phase {}", topn.phase),
        )
    })?;
    if phase == plan::TopNPhase::TopnPhaseUnspecified {
        return Err(NativeFragmentDecodeError::invalid_enum(
            path.clone().field("phase"),
            "TopNNode phase is unspecified",
        ));
    }
    if topn.is_split && phase == plan::TopNPhase::TopnPhaseFinal {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("is_split"),
            "TopNNode final split must be represented as ExchangeReceiver TopNSplit",
        ));
    }
    let order_by = lower_sort_items_with_context(
        "TopNNode",
        &topn.items,
        path.clone().field("items"),
        arena,
        &child.layout,
        ctx,
    )?;
    let topn_type = NativeFragmentDecodeError::map_invalid(
        path.clone().field("topn_type"),
        parse_sort_topn_type(None),
    )?;
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Sort(SortNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                use_top_n: true,
                order_by,
                limit,
                offset,
                topn_type,
                max_buffered_rows: None,
                max_buffered_bytes: None,
                partition_exprs: Vec::new(),
                partition_limit: None,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}
