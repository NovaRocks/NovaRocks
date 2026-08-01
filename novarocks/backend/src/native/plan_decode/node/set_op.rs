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
use crate::native::type_decode::decode_type;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::ChunkSchemaRef;
use novarocks::exec::expr::{ExprArena, ExprNode};
use novarocks::exec::node::project::ProjectNode;
use novarocks::exec::node::set_op::{SetOpKind, SetOpNode};
use novarocks::exec::node::union_all::UnionAllNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::{common as proto_common, plan};
use novarocks::protocol::common::error::FieldPath;

pub(super) fn lower_set_op_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    set_op: &plan::SetOpNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let kind = plan::PlanSetOpKind::try_from(set_op.kind).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone().field("kind"),
            format!("SetOpNode unknown kind {}", set_op.kind),
        )
    })?;
    let (output_columns, output_columns_path) = if set_op.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (&set_op.output_columns, path.clone().field("output_columns"))
    };
    let output_layout = ctx.decode_output_layout(output_columns, output_columns_path.clone())?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let inputs = normalize_set_op_inputs(
        node.node_id,
        children,
        &set_op.child_output_columns,
        output_columns,
        output_columns_path,
        output_schema.clone(),
        path.clone(),
        arena,
        ctx,
    )?;
    match kind {
        plan::PlanSetOpKind::UnionAll => Ok(DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::UnionAll(UnionAllNode {
                    inputs,
                    node_id: node.node_id,
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Intersect => Ok(DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::SetOp(SetOpNode {
                    kind: SetOpKind::Intersect,
                    inputs,
                    node_id: node.node_id,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::Except => Ok(DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::SetOp(SetOpNode {
                    kind: SetOpKind::Except,
                    inputs,
                    node_id: node.node_id,
                    output_chunk_schema: output_schema.clone(),
                }),
            },
            layout,
            output_schema,
        }),
        plan::PlanSetOpKind::UnionDistinct => Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("kind"),
            "UnionDistinct native proto node lowering is not implemented",
        )),
        plan::PlanSetOpKind::Unspecified => Err(NativeFragmentDecodeError::invalid_enum(
            path.field("kind"),
            "SetOpNode kind is unspecified",
        )),
    }
}

fn normalize_set_op_inputs(
    node_id: i32,
    children: Vec<DecodedNode>,
    child_output_columns: &[plan::OutputColumnList],
    output_columns: &[proto_common::OutputColumn],
    output_columns_path: FieldPath,
    output_schema: ChunkSchemaRef,
    path: FieldPath,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<Vec<ExecNode>, NativeFragmentDecodeError> {
    if child_output_columns.is_empty() {
        return normalize_set_op_inputs_by_position(
            node_id,
            children,
            output_columns,
            output_columns_path,
            output_schema,
            path,
            arena,
            ctx,
        );
    }
    if child_output_columns.len() != children.len() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("child_output_columns"),
            format!(
                "SetOpNode child_output_columns size mismatch: expected {}, got {}",
                children.len(),
                child_output_columns.len()
            ),
        ));
    }
    let output_layout = ctx.decode_output_layout(output_columns, output_columns_path.clone())?;
    let output_slots = output_layout.slot_ids().to_vec();
    let output_slot_schemas = output_layout.slot_schemas().to_vec();
    children
        .into_iter()
        .zip(child_output_columns.iter())
        .enumerate()
        .map(|(idx, (child, child_columns))| {
            let child_path = path.clone().field("child_output_columns").index(idx).field("columns");
            if child_columns.columns.len() != output_columns.len() {
                return Err(NativeFragmentDecodeError::inconsistent(child_path.clone(), format!("SetOpNode child {idx} output width mismatch: expected {}, got {}", output_columns.len(), child_columns.columns.len())));
            }
            let expected_child_layout = Layout::for_slots(
                ctx.decode_output_layout(&child_columns.columns, child_path.clone())?
                    .slot_ids()
                    .iter()
                    .copied(),
            );
            if expected_child_layout.order() != child.layout.order() {
                return Err(NativeFragmentDecodeError::inconsistent(child_path.clone(), format!("SetOpNode child {idx} output columns do not match child layout: columns={:?} layout={:?}", expected_child_layout.order(), child.layout.order())));
            }
            let exprs = child_columns
                .columns
                .iter()
                .enumerate()
                .map(|(col_idx, col)| {
                    let slot = SlotId::new(col.column_id);
                    let data_type = col
                        .r#type
                        .as_ref()
                        .ok_or_else(|| NativeFragmentDecodeError::missing(child_path.clone().index(col_idx).field("type"), format!("SetOpNode child {idx} column {} type missing", col.column_id)))?;
                    let data_type = NativeFragmentDecodeError::map_invalid(child_path.clone().index(col_idx).field("type"), decode_type(data_type))?;
                    Ok(arena.push_typed(ExprNode::SlotId(slot), data_type))
                })
                .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
            Ok(ExecNode {
                kind: ExecNodeKind::Project(ProjectNode {
                    input: Box::new(child.node),
                    node_id,
                    is_subordinate: true,
                    exprs,
                    expr_slot_ids: output_slots.clone(),
                    expr_slot_schemas: Some(output_slot_schemas.clone()),
                    output_indices: None,
                    output_chunk_schema: output_schema.clone(),
                }),
            })
        })
        .collect()
}

fn normalize_set_op_inputs_by_position(
    node_id: i32,
    children: Vec<DecodedNode>,
    output_columns: &[proto_common::OutputColumn],
    output_columns_path: FieldPath,
    output_schema: ChunkSchemaRef,
    path: FieldPath,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<Vec<ExecNode>, NativeFragmentDecodeError> {
    let output_layout = ctx.decode_output_layout(output_columns, output_columns_path.clone())?;
    let output_slots = output_layout.slot_ids().to_vec();
    let output_slot_schemas = output_layout.slot_schemas().to_vec();
    children
        .into_iter()
        .enumerate()
        .map(|(idx, child)| {
            if child.layout.order().len() != output_slots.len() {
                return Err(NativeFragmentDecodeError::inconsistent(path.clone().field("child_output_columns").index(idx), format!("SetOpNode child {idx} width mismatch without child_output_columns: expected {}, got {}", output_slots.len(), child.layout.order().len())));
            }
            if child.layout.order() == output_slots.as_slice() {
                return Ok(child.node);
            }
            let exprs = child
                .layout
                .order()
                .iter()
                .copied()
                .map(|slot| {
                    let data_type = child
                        .output_schema
                        .slot(slot)
                        .ok_or_else(|| NativeFragmentDecodeError::inconsistent(path.clone().field("child_output_columns").index(idx), format!("SetOpNode child {idx} slot {} missing from child output schema", slot)))?
                        .data_type()
                        .clone();
                    Ok(arena.push_typed(ExprNode::SlotId(slot), data_type))
                })
                .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
            Ok(ExecNode {
                kind: ExecNodeKind::Project(ProjectNode {
                    input: Box::new(child.node),
                    node_id,
                    is_subordinate: true,
                    exprs,
                    expr_slot_ids: output_slots.clone(),
                    expr_slot_schemas: Some(output_slot_schemas.clone()),
                    output_indices: None,
                    output_chunk_schema: output_schema.clone(),
                }),
            })
        })
        .collect()
}
