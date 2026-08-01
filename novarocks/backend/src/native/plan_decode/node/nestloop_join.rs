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

use std::sync::Arc;

use super::common::concat_layouts;
use super::hash_join;
use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::exec::chunk::ChunkSchema;
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::nljoin::{NestedLoopJoinNode, NestedLoopJoinType};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::plan;
use novarocks::protocol::common::error::FieldPath;

pub(super) fn lower_nest_loop_join_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    join: &plan::NestLoopJoinNode,
    path: FieldPath,
    node_path: FieldPath,
    physical_output_path: FieldPath,
    children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let mut it = children.into_iter();
    let mut left = it.next().expect("left");
    let mut right = it.next().expect("right");
    let join_kind = plan::JoinKind::try_from(join.join_type).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("join_type"),
            format!("NestLoopJoinNode unknown join_type {}", join.join_type),
        )
    })?;
    let join_type = match join_kind {
        plan::JoinKind::RightSemi => {
            std::mem::swap(&mut left, &mut right);
            NestedLoopJoinType::LeftSemi
        }
        plan::JoinKind::RightAnti => {
            std::mem::swap(&mut left, &mut right);
            NestedLoopJoinType::LeftAnti
        }
        _ => NativeFragmentDecodeError::map_invalid(
            path.clone().field("join_type"),
            proto_nested_loop_join_type(join.join_type, "NestLoopJoinNode"),
        )?,
    };
    let join_layout = NativeFragmentDecodeError::map_invalid(
        node_path.clone().field("children"),
        concat_layouts(&left.layout, &right.layout),
    )?;
    let join_scope_chunk_schema = Arc::new(NativeFragmentDecodeError::map_invalid(
        node_path.field("children"),
        ChunkSchema::concat(&[left.output_schema.clone(), right.output_schema.clone()]),
    )?);
    let is_semi_anti = matches!(
        join_type,
        NestedLoopJoinType::LeftSemi
            | NestedLoopJoinType::LeftAnti
            | NestedLoopJoinType::NullAwareLeftAnti
    );
    let output_schema = if is_semi_anti && !physical.output_columns.is_empty() {
        ctx.decode_output_layout(&physical.output_columns, physical_output_path.clone())?
            .chunk_schema()
    } else {
        hash_join::join_output_chunk_schema(
            physical,
            join_scope_chunk_schema.clone(),
            "NestLoopJoinNode",
            physical_output_path,
            ctx,
        )?
    };
    let join_conjunct = join
        .condition
        .as_ref()
        .map(|expr| {
            ctx.decode_expression(expr, path.clone().field("condition"), arena, &join_layout)
        })
        .transpose()?;
    let output_layout = if is_semi_anti {
        Layout::for_slots(output_schema.slot_ids().iter().copied())
    } else {
        join_layout.clone()
    };
    let execution_scope_chunk_schema = if is_semi_anti {
        join_scope_chunk_schema
    } else {
        output_schema.clone()
    };

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::NestedLoopJoin(NestedLoopJoinNode {
                left: Box::new(left.node),
                right: Box::new(right.node),
                node_id: node.node_id,
                join_type,
                join_conjunct,
                left_chunk_schema: left.output_schema,
                right_chunk_schema: right.output_schema,
                join_scope_chunk_schema: execution_scope_chunk_schema,
            }),
        },
        layout: output_layout,
        output_schema,
    })
}

fn proto_nested_loop_join_type(value: i32, node_kind: &str) -> Result<NestedLoopJoinType, String> {
    match plan::JoinKind::try_from(value)
        .map_err(|_| format!("{node_kind} unknown join_type {value}"))?
    {
        plan::JoinKind::Inner => Ok(NestedLoopJoinType::Inner),
        plan::JoinKind::Cross => Ok(NestedLoopJoinType::Cross),
        plan::JoinKind::LeftOuter => Ok(NestedLoopJoinType::LeftOuter),
        plan::JoinKind::RightOuter => Ok(NestedLoopJoinType::RightOuter),
        plan::JoinKind::FullOuter => Ok(NestedLoopJoinType::FullOuter),
        plan::JoinKind::LeftSemi => Ok(NestedLoopJoinType::LeftSemi),
        plan::JoinKind::LeftAnti => Ok(NestedLoopJoinType::LeftAnti),
        plan::JoinKind::NullAwareLeftAnti => Ok(NestedLoopJoinType::NullAwareLeftAnti),
        plan::JoinKind::RightSemi | plan::JoinKind::RightAnti => Err(format!(
            "{node_kind} right semi/anti must be rewritten before nested-loop join type lowering"
        )),
        plan::JoinKind::Unspecified => Err(format!("{node_kind} join_type is unspecified")),
    }
}
