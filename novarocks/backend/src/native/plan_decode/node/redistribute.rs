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
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::exec::expr::ExprArena;
use novarocks::proto::plan;
use novarocks::protocol::common::error::FieldPath;

pub(super) fn lower_redistribute_node(
    physical: &plan::PlanNode,
    redistribute: &plan::RedistributeNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let mode = redistribute
        .mode
        .as_ref()
        .and_then(|mode| mode.mode.as_ref())
        .ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                path.clone().field("mode").field("mode"),
                "RedistributeNode mode missing",
            )
        })?;
    match mode {
        plan::redistribute_mode::Mode::Gather(true)
        | plan::redistribute_mode::Mode::Broadcast(true) => {}
        plan::redistribute_mode::Mode::Hash(hash) => {
            if hash.cols.is_empty() {
                return Err(NativeFragmentDecodeError::missing(
                    path.clone().field("mode").field("hash").field("cols"),
                    "RedistributeNode hash mode requires cols",
                ));
            }
            for col in &hash.cols {
                NativeFragmentDecodeError::map_invalid(
                    path.clone().field("mode").field("hash").field("cols"),
                    child.layout.resolve_column_id(*col),
                )?;
            }
        }
        plan::redistribute_mode::Mode::Gather(false)
        | plan::redistribute_mode::Mode::Broadcast(false) => {
            return Err(NativeFragmentDecodeError::invalid_value(
                path.clone().field("mode"),
                "RedistributeNode boolean mode must be true",
            ));
        }
    }
    for (idx, expr) in redistribute.partition_exprs.iter().enumerate() {
        ctx.decode_expression(
            expr,
            path.clone().field("partition_exprs").index(idx),
            arena,
            &child.layout,
        )?;
    }
    let (output_columns, output_path) = if redistribute.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (
            &redistribute.output_columns,
            path.clone().field("output_columns"),
        )
    };
    if output_columns.is_empty() {
        return Ok(child);
    }
    let output_layout = ctx.decode_output_layout(output_columns, output_path.clone())?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    if layout.order() != child.layout.order() {
        return Err(NativeFragmentDecodeError::inconsistent(
            output_path.clone(),
            format!(
                "RedistributeNode output columns must preserve child order: child={:?} output={:?}",
                child.layout.order(),
                layout.order()
            ),
        ));
    }
    let output_schema = output_layout.chunk_schema();
    Ok(DecodedNode {
        node: child.node,
        layout,
        output_schema,
    })
}
