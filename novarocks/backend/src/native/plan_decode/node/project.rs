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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentLeafDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkFieldSchema, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::project::ProjectNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::{expr, plan};
use novarocks::protocol::common::error::{FieldPath, ProtocolErrorKind};

pub(super) fn lower_project_node(
    node: &plan::DistributedNode,
    project: &plan::ProjectNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, crate::native::plan_decode::error::NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let project_outputs = project_output_plan(project, &child.layout, path.clone())?;
    let layout = project_outputs.layout.clone();
    let output_schema = Arc::clone(&project_outputs.output_schema);
    let expr_slot_schemas = project_outputs.computed_slot_schemas.clone();

    let exprs = project_outputs
        .computed_item_indices
        .iter()
        .map(|idx| {
            let item = project.items.get(*idx).ok_or_else(|| {
                crate::native::plan_decode::error::NativeFragmentDecodeError::missing(
                    path.clone().field("items").index(*idx),
                    "native ProjectNode item is missing",
                )
            })?;
            let expr = item.expr.as_ref().ok_or_else(|| {
                crate::native::plan_decode::error::NativeFragmentDecodeError::missing(
                    path.clone().field("items").index(*idx).field("expr"),
                    "native ProjectNode item requires expr",
                )
            })?;
            ctx.decode_expression(
                expr,
                path.clone().field("items").index(*idx).field("expr"),
                arena,
                &child.layout,
            )
        })
        .collect::<Result<Vec<_>, crate::native::plan_decode::error::NativeFragmentDecodeError>>(
        )?;
    let expr_slot_ids = project_outputs.computed_slot_ids;

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: false,
                exprs,
                expr_slot_ids,
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: project_outputs.output_indices,
                output_chunk_schema: output_schema.clone(),
            }),
        },
        layout,
        output_schema,
    })
}

struct ProjectOutputPlan {
    computed_item_indices: Vec<usize>,
    computed_slot_ids: Vec<SlotId>,
    computed_slot_schemas: Vec<ChunkSlotSchema>,
    layout: Layout,
    output_schema: ChunkSchemaRef,
    output_indices: Option<Vec<usize>>,
}

fn project_output_plan(
    project: &plan::ProjectNode,
    input_layout: &Layout,
    path: FieldPath,
) -> Result<ProjectOutputPlan, crate::native::plan_decode::error::NativeFragmentDecodeError> {
    let decoded = (|| -> Result<ProjectOutputPlan, NativeFragmentLeafDecodeError> {
        let item_outputs = project
            .items
            .iter()
            .enumerate()
            .map(project_item_output)
            .collect::<Result<Vec<_>, _>>()?;
        let input_column_ids = input_layout
            .order()
            .iter()
            .map(|slot| slot.as_u32())
            .collect::<HashSet<_>>();
        let output_column_id_candidates = item_outputs
            .iter()
            .map(|item| item.output_column_id)
            .collect::<HashSet<_>>();
        let mut used_output_column_ids = HashSet::new();
        let mut used_compute_column_ids = input_column_ids.clone();
        let mut next_synthetic_column_id = output_column_id_candidates
            .iter()
            .chain(used_compute_column_ids.iter())
            .copied()
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        let mut first_expr_index_by_column_id = HashMap::new();
        let mut computed_item_indices = Vec::new();
        let mut computed_slot_ids = Vec::new();
        let mut computed_slot_schemas = Vec::new();
        let mut output_slot_schemas = Vec::with_capacity(project.items.len());
        let mut output_indices = Vec::with_capacity(project.items.len());
        let mut needs_output_indices = false;

        for item in item_outputs {
            let preferred_compute_column_id = item.preferred_compute_column_id;
            let mut compute_column_id = if item.can_reuse_input_slot
                || !input_column_ids.contains(&preferred_compute_column_id)
            {
                preferred_compute_column_id
            } else {
                allocate_project_synthetic_column_id(
                    &mut next_synthetic_column_id,
                    &mut used_output_column_ids,
                    &mut used_compute_column_ids,
                )
                .map_err(project_synthetic_id_error)?
            };
            if !item.can_reuse_input_slot && used_compute_column_ids.contains(&compute_column_id) {
                compute_column_id = allocate_project_synthetic_column_id(
                    &mut next_synthetic_column_id,
                    &mut used_output_column_ids,
                    &mut used_compute_column_ids,
                )
                .map_err(project_synthetic_id_error)?;
            }

            let (computed_idx, is_duplicate_compute) = if item.can_reuse_input_slot
                && let Some(computed_idx) = first_expr_index_by_column_id.get(&compute_column_id)
            {
                (*computed_idx, true)
            } else {
                let computed_idx = computed_slot_ids.len();
                first_expr_index_by_column_id.insert(compute_column_id, computed_idx);
                used_compute_column_ids.insert(compute_column_id);
                computed_item_indices.push(item.item_index);
                let compute_slot_id = SlotId::new(compute_column_id);
                computed_slot_ids.push(compute_slot_id);
                computed_slot_schemas.push(ChunkSlotSchema::new_with_field(
                    compute_slot_id,
                    item.field.clone(),
                    Some(item.field_schema.clone()),
                    None,
                ));
                (computed_idx, false)
            };

            let output_column_id = if used_output_column_ids.insert(item.output_column_id) {
                item.output_column_id
            } else {
                allocate_project_synthetic_column_id(
                    &mut next_synthetic_column_id,
                    &mut used_output_column_ids,
                    &mut used_compute_column_ids,
                )
                .map_err(project_synthetic_id_error)?
            };
            output_slot_schemas.push(ChunkSlotSchema::new_with_field(
                SlotId::new(output_column_id),
                item.field,
                Some(item.field_schema),
                None,
            ));
            if is_duplicate_compute
                || computed_idx != output_indices.len()
                || compute_column_id != output_column_id
            {
                needs_output_indices = true;
            }
            output_indices.push(computed_idx);
        }

        let layout = Layout::for_slots(output_slot_schemas.iter().map(ChunkSlotSchema::slot_id));
        let output_schema = ChunkSchema::try_new(output_slot_schemas)
            .map(Arc::new)
            .map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "items",
                    error,
                )
            })?;
        Ok(ProjectOutputPlan {
            computed_item_indices,
            computed_slot_ids,
            computed_slot_schemas,
            layout,
            output_schema,
            output_indices: needs_output_indices.then_some(output_indices),
        })
    })();
    decoded.map_err(|error| error.into_native(path))
}

fn project_synthetic_id_error(error: String) -> NativeFragmentLeafDecodeError {
    NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::OutOfRange, "items", error)
}

fn allocate_project_synthetic_column_id(
    next_synthetic_column_id: &mut u32,
    used_output_column_ids: &mut HashSet<u32>,
    used_compute_column_ids: &mut HashSet<u32>,
) -> Result<u32, String> {
    while used_output_column_ids.contains(next_synthetic_column_id)
        || used_compute_column_ids.contains(next_synthetic_column_id)
    {
        *next_synthetic_column_id = next_synthetic_column_id
            .checked_add(1)
            .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    }
    let synthetic = *next_synthetic_column_id;
    used_output_column_ids.insert(synthetic);
    used_compute_column_ids.insert(synthetic);
    *next_synthetic_column_id = next_synthetic_column_id
        .checked_add(1)
        .ok_or_else(|| "ProjectNode cannot allocate synthetic output column id".to_string())?;
    Ok(synthetic)
}

struct ProjectItemOutput {
    item_index: usize,
    preferred_compute_column_id: u32,
    output_column_id: u32,
    can_reuse_input_slot: bool,
    field: arrow::datatypes::Field,
    field_schema: ChunkFieldSchema,
}

fn project_item_output(
    (idx, item): (usize, &plan::ProjectItem),
) -> Result<ProjectItemOutput, NativeFragmentLeafDecodeError> {
    let expr = item.expr.as_ref().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "items",
            format!("ProjectNode item {idx} expr missing"),
        )
        .append_index(idx)
        .append_field("expr")
    })?;
    let r#type = expr.r#type.clone().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "items",
            format!("ProjectNode item {idx} expr type missing"),
        )
        .append_index(idx)
        .append_field("expr")
        .append_field("type")
    })?;
    let type_error = |error| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "items", error)
            .append_index(idx)
            .append_field("expr")
            .append_field("type")
    };
    let field =
        crate::native::type_decode::decode_field_type(&item.output_name, expr.nullable, &r#type)
            .map_err(type_error)?;
    let field_schema = ChunkFieldSchema::from_field(&field).map_err(type_error)?;
    let (preferred_compute_column_id, can_reuse_input_slot) = match expr.kind.as_ref() {
        Some(expr::expr::Kind::ColumnRef(column)) => (column.column_id, true),
        _ => (item.output_column_id, false),
    };
    Ok(ProjectItemOutput {
        item_index: idx,
        preferred_compute_column_id,
        output_column_id: item.output_column_id,
        can_reuse_input_slot,
        field,
        field_schema,
    })
}
