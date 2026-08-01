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

use std::collections::HashSet;

use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::error::NativeFragmentLeafDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::exec::expr::{ExprArena, ExprNode};
use novarocks::exec::node::join::JoinType;
use novarocks::exec::node::project::ProjectNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::{common as proto_common, plan};
use novarocks::protocol::common::error::{FieldPath, ProtocolErrorKind};

pub(crate) fn unsupported<T>(kind: &str) -> Result<T, String> {
    Err(format!(
        "{kind} native proto node lowering is not implemented"
    ))
}

pub(crate) fn exec_node_kind_label(kind: &ExecNodeKind) -> &'static str {
    match kind {
        ExecNodeKind::Scan(_) => "Scan",
        ExecNodeKind::Project(_) => "Project",
        ExecNodeKind::Filter(_) => "Filter",
        ExecNodeKind::Aggregate(_) => "Aggregate",
        ExecNodeKind::Join(_) => "Join",
        ExecNodeKind::NestedLoopJoin(_) => "NestedLoopJoin",
        ExecNodeKind::Sort(_) => "Sort",
        ExecNodeKind::Limit(_) => "Limit",
        ExecNodeKind::ExchangeSource(_) => "ExchangeSource",
        ExecNodeKind::UnionAll(_) => "UnionAll",
        ExecNodeKind::SetOp(_) => "SetOp",
        ExecNodeKind::Values(_) => "Values",
        ExecNodeKind::RuntimeFilterConsumer(_) => "RuntimeFilterConsumer",
        ExecNodeKind::TableFunction(_) => "TableFunction",
        ExecNodeKind::Repeat(_) => "Repeat",
        ExecNodeKind::ChangeEventExpand(_) => "ChangeEventExpand",
        ExecNodeKind::AssertNumRows(_) => "AssertNumRows",
        ExecNodeKind::Analytic(_) => "Analytic",
        ExecNodeKind::Fetch(_) => "Fetch",
        ExecNodeKind::LookUp(_) => "LookUp",
    }
}

fn check_children_arity(
    node_path: FieldPath,
    kind: &str,
    expected: &str,
    actual: usize,
    ok: bool,
) -> Result<(), NativeFragmentDecodeError> {
    if ok {
        Ok(())
    } else {
        Err(NativeFragmentDecodeError::inconsistent(
            node_path.field("children"),
            format!("{kind} expected {expected} children, got {actual}"),
        ))
    }
}

pub(crate) fn require_exact_children(
    node_path: FieldPath,
    kind: &str,
    expected: usize,
    actual: usize,
) -> Result<(), NativeFragmentDecodeError> {
    check_children_arity(
        node_path,
        kind,
        &expected.to_string(),
        actual,
        actual == expected,
    )
}

pub(crate) fn require_min_children(
    node_path: FieldPath,
    kind: &str,
    min: usize,
    actual: usize,
) -> Result<(), NativeFragmentDecodeError> {
    check_children_arity(node_path, kind, &format!(">={min}"), actual, actual >= min)
}

pub(crate) fn concat_layouts(left: &Layout, right: &Layout) -> Result<Layout, String> {
    let mut slots = Vec::with_capacity(left.order().len() + right.order().len());
    let mut seen = HashSet::with_capacity(left.order().len() + right.order().len());
    for slot in left.order().iter().chain(right.order().iter()).copied() {
        if !seen.insert(slot) {
            return Err(format!("duplicate slot id {} in joined layout", slot));
        }
        slots.push(slot);
    }
    Ok(Layout::for_slots(slots))
}

pub(crate) fn proto_join_type(
    value: i32,
    node_kind: &str,
) -> Result<JoinType, NativeFragmentLeafDecodeError> {
    match plan::JoinKind::try_from(value).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "join_type",
            format!("{node_kind} unknown join_type {value}"),
        )
    })? {
        plan::JoinKind::Inner => Ok(JoinType::Inner),
        plan::JoinKind::LeftOuter => Ok(JoinType::LeftOuter),
        plan::JoinKind::RightOuter => Ok(JoinType::RightOuter),
        plan::JoinKind::FullOuter => Ok(JoinType::FullOuter),
        plan::JoinKind::LeftSemi => Ok(JoinType::LeftSemi),
        plan::JoinKind::RightSemi => Ok(JoinType::RightSemi),
        plan::JoinKind::LeftAnti => Ok(JoinType::LeftAnti),
        plan::JoinKind::RightAnti => Ok(JoinType::RightAnti),
        plan::JoinKind::NullAwareLeftAnti => Ok(JoinType::NullAwareLeftAnti),
        plan::JoinKind::Cross => Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "join_type",
            format!("{node_kind} CROSS join requires NestLoopJoinNode"),
        )),
        plan::JoinKind::Unspecified => Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidEnum,
            "join_type",
            format!("{node_kind} join_type is unspecified"),
        )),
    }
}

pub(crate) fn parse_optional_nonnegative_i64(
    value: Option<i64>,
    label: &str,
) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            if value < 0 {
                Err(format!("{label} must be >= 0, got {value}"))
            } else {
                Ok(value as usize)
            }
        })
        .transpose()
}

pub(crate) fn parse_distributed_limit(value: i64, label: &str) -> Result<Option<usize>, String> {
    if value == -1 {
        Ok(None)
    } else if value < 0 {
        Err(format!("{label} must be -1 or >= 0, got {value}"))
    } else {
        Ok(Some(value as usize))
    }
}

pub(crate) fn merge_limits(
    node_kind: &str,
    payload_limit: Option<usize>,
    outer_limit: Option<usize>,
) -> Result<Option<usize>, String> {
    match (payload_limit, outer_limit) {
        (Some(left), Some(right)) if left != right => Err(format!(
            "{node_kind} payload limit {left} conflicts with DistributedNode.limit {right}"
        )),
        (Some(value), _) | (_, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

pub(super) fn build_slot_projection(
    label: &str,
    input: DecodedNode,
    output_columns: &[proto_common::OutputColumn],
    path: FieldPath,
    node_id: i32,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let output_layout = ctx.decode_output_layout(output_columns, path.clone())?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let expr_slot_schemas = output_layout.slot_schemas().to_vec();
    let mut exprs = Vec::with_capacity(layout.order().len());
    for slot in layout.order().iter().copied() {
        if !input.layout.contains_slot(slot) {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone(),
                format!(
                    "{label} output column id {} has no input slot",
                    slot.as_u32()
                ),
            ));
        }
        exprs.push(arena.push(ExprNode::SlotId(slot)));
    }

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(input.node),
                node_id,
                is_subordinate: true,
                exprs,
                expr_slot_ids: layout.order().to_vec(),
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: None,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}
