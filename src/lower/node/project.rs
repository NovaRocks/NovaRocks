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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::ExprArena;
use crate::exec::node::project::ProjectNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::novarocks_logging::debug;

use crate::descriptors;
use crate::exprs;
use crate::lower::expr::{lower_t_expr, lower_t_expr_with_common_slot_map};
use crate::lower::layout::{Layout, layout_from_slot_ids, slot_display_name_from_desc};
use crate::lower::node::Lowered;
use crate::{plan_nodes, types};

fn project_slot_schema_from_desc(
    desc_tbl: &descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
    slot_id: types::TSlotId,
) -> Result<Option<ChunkSlotSchema>, String> {
    let Some(slot_descs) = desc_tbl.slot_descriptors.as_ref() else {
        return Ok(None);
    };
    let Some(desc) = slot_descs
        .iter()
        .find(|desc| desc.parent == Some(tuple_id) && desc.id == Some(slot_id))
    else {
        return Ok(None);
    };
    let slot_id = SlotId::try_from(slot_id)?;
    let name = slot_display_name_from_desc(desc);
    let nullable = desc.is_nullable.unwrap_or(true);
    let unique_id = desc.col_unique_id.filter(|v| *v > 0);
    Ok(Some(ChunkSlotSchema::new(
        slot_id,
        name,
        nullable,
        desc.slot_type.clone(),
        unique_id,
    )))
}

fn project_slot_schema_from_expr(
    desc_tbl: &descriptors::TDescriptorTable,
    tuple_id: types::TTupleId,
    slot_id: types::TSlotId,
    expr: &exprs::TExpr,
) -> Result<ChunkSlotSchema, String> {
    if let Some(schema) = project_slot_schema_from_desc(desc_tbl, tuple_id, slot_id)? {
        return Ok(schema);
    }
    let root = expr
        .nodes
        .first()
        .ok_or_else(|| format!("project expr for slot_id={} has no root node", slot_id))?;
    Ok(ChunkSlotSchema::new(
        SlotId::try_from(slot_id)?,
        format!("_expr_{}", slot_id),
        root.is_nullable.unwrap_or(true),
        Some(root.type_.clone()),
        None,
    ))
}

fn project_output_chunk_schema(
    expr_slot_schemas: &[ChunkSlotSchema],
    output_indices: &[usize],
) -> Result<ChunkSchemaRef, String> {
    let mut output_schemas = Vec::with_capacity(output_indices.len());
    for &idx in output_indices {
        let slot_schema = expr_slot_schemas.get(idx).cloned().ok_or_else(|| {
            format!(
                "project output index {} out of range for expr_slot_schemas={}",
                idx,
                expr_slot_schemas.len()
            )
        })?;
        output_schemas.push(slot_schema);
    }
    Ok(Arc::new(ChunkSchema::try_new(output_schemas)?))
}

/// Lower a PROJECT_NODE plan node to a `Lowered` ExecNode.
///
/// This helper encapsulates all PROJECT_NODE specific logic, including:
/// - resolving combined slot maps (common + output)
/// - determining output layout
/// - handling CSE (common slot expressions)
/// - lowering expressions with tuple remapping.
pub(crate) fn lower_project_node(
    child: Lowered,
    node: &plan_nodes::TPlanNode,
    mut out_layout: Layout,
    arena: &mut ExprArena,
    desc_tbl: &descriptors::TDescriptorTable,
    global_common_slot_map: &BTreeMap<types::TSlotId, exprs::TExpr>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Lowered, String> {
    let Some(project) = node.project_node.as_ref() else {
        return Ok(child);
    };

    let project_common_map = project.common_slot_map.as_ref();
    let heavy_common_map = node
        .common
        .as_ref()
        .and_then(|common| common.heavy_exprs.as_ref());
    let local_common_map = {
        let mut merged = BTreeMap::new();
        if let Some(heavy_map) = heavy_common_map {
            for (&slot_id, expr) in heavy_map {
                merged.insert(slot_id, expr.clone());
            }
        }
        if let Some(project_map) = project_common_map {
            for (&slot_id, expr) in project_map {
                merged.insert(slot_id, expr.clone());
            }
        }
        if merged.is_empty() {
            None
        } else {
            Some(merged)
        }
    };
    let common_map = local_common_map.as_ref();
    let slot_map = project.slot_map.as_ref();
    let has_common = common_map.is_some_and(|m| !m.is_empty());
    let has_outputs = slot_map.is_some_and(|m| !m.is_empty());
    if !has_common && !has_outputs {
        return Ok(child);
    }

    let output_slot_ids: HashSet<types::TSlotId> = slot_map
        .as_ref()
        .map(|map| map.keys().copied().collect())
        .unwrap_or_default();

    debug!(
        "PROJECT_NODE node_id={} row_tuples={:?} common_slot_map.keys={:?} heavy_exprs.keys={:?} slot_map.keys={:?} output_slot_ids={:?} child.layout.keys={:?}",
        node.node_id,
        node.row_tuples,
        project_common_map.map(|m| m.keys().collect::<Vec<_>>()),
        heavy_common_map.map(|m| m.keys().collect::<Vec<_>>()),
        slot_map.map(|m| m.keys().collect::<Vec<_>>()),
        &output_slot_ids,
        child.layout.index.keys().collect::<Vec<_>>()
    );

    let output_tuple_id = node.row_tuples.get(0).copied().unwrap_or(0);
    if !out_layout.order.is_empty() && !output_slot_ids.is_empty() {
        let filtered: Vec<(types::TTupleId, types::TSlotId)> = out_layout
            .order
            .into_iter()
            .filter(|(_, slot_id)| output_slot_ids.contains(slot_id))
            .collect();
        if filtered.len() == output_slot_ids.len() {
            let index = filtered
                .iter()
                .enumerate()
                .map(|(i, key)| (*key, i))
                .collect();
            out_layout = Layout {
                order: filtered,
                index,
            };
        } else {
            out_layout = Layout {
                order: Vec::new(),
                index: HashMap::new(),
            };
        }
    }
    if out_layout.order.is_empty() {
        if !output_slot_ids.is_empty() {
            let Some(slot_map) = slot_map else {
                return Err("project node has output_slot_ids but missing slot_map".to_string());
            };
            out_layout = layout_from_slot_ids(output_tuple_id, slot_map.keys().copied());
        } else {
            let Some(common_map) = common_map else {
                return Err(
                    "project node has empty slot_map and missing common_slot_map".to_string(),
                );
            };
            out_layout = layout_from_slot_ids(output_tuple_id, common_map.keys().copied());
        }
    }

    let mut child_slot_ids = std::collections::HashSet::<types::TSlotId>::new();
    for (_tuple_id, slot_id) in &child.layout.order {
        child_slot_ids.insert(*slot_id);
    }
    let mut resolution_common_map = BTreeMap::<types::TSlotId, exprs::TExpr>::new();
    for (&slot_id, expr) in global_common_slot_map {
        if !child_slot_ids.contains(&slot_id) {
            resolution_common_map.insert(slot_id, expr.clone());
        }
    }
    if let Some(local) = common_map {
        for (&slot_id, expr) in local {
            resolution_common_map.insert(slot_id, expr.clone());
        }
    }
    let resolution_common_map = if resolution_common_map.is_empty() {
        None
    } else {
        Some(resolution_common_map)
    };

    // Sequential materialization following StarRocks BE approach:
    // 1. Materialize common expressions first (they extend the input).
    // 2. Then evaluate output expressions (they may reference common expression slots).
    //
    // IMPORTANT: Do not merge `common_slot_map` and `slot_map` by overwriting keys.
    // In StarRocks plans, a slot id can appear in both maps (e.g. slot 35), where:
    // - common_slot_map defines how to compute the slot
    // - slot_map may simply reference that slot (identity) or clone it
    // Overwriting would lose the definition and break dependency ordering.

    let mut exprs = Vec::new();
    let mut expr_slot_ids: Vec<SlotId> = Vec::new();
    let mut expr_slot_schemas = Vec::new();

    // Step 1: Lower common expressions and add to exprs.
    // These are evaluated first so outputs can reference their slots.
    if let Some(common_map) = common_map {
        for (&slot_id, texpr) in common_map.iter() {
            let expr_id = lower_t_expr_with_common_slot_map(
                texpr,
                arena,
                &out_layout,
                last_query_id,
                fe_addr,
                Some(common_map),
            )?;
            exprs.push(expr_id);
            expr_slot_ids.push(SlotId::try_from(slot_id)?);
            expr_slot_schemas.push(project_slot_schema_from_expr(
                desc_tbl,
                output_tuple_id,
                slot_id,
                texpr,
            )?);
        }
    }
    let num_common = exprs.len();

    // Step 2: Lower output expressions in output layout order.
    for &(_out_tuple_id, slot_id) in &out_layout.order {
        let texpr = slot_map
            .and_then(|m| m.get(&slot_id))
            .or_else(|| common_map.and_then(|m| m.get(&slot_id)))
            .ok_or_else(|| {
                format!(
                    "slot_id {} in output layout not found in slot_map/common_slot_map",
                    slot_id
                )
            })?;

        let expr_id = if let Some(map) = resolution_common_map.as_ref() {
            lower_t_expr_with_common_slot_map(
                texpr,
                arena,
                &out_layout,
                last_query_id,
                fe_addr,
                Some(map),
            )?
        } else {
            lower_t_expr(texpr, arena, &out_layout, last_query_id, fe_addr)?
        };
        exprs.push(expr_id);
        expr_slot_ids.push(SlotId::try_from(slot_id)?);
        expr_slot_schemas.push(project_slot_schema_from_expr(
            desc_tbl,
            output_tuple_id,
            slot_id,
            texpr,
        )?);
    }

    let output_slots = out_layout
        .order
        .iter()
        .map(|(_, slot_id)| SlotId::try_from(*slot_id))
        .collect::<Result<Vec<_>, _>>()?;
    // output_indices = [num_common, num_common+1, ..., exprs.len()-1]
    let output_indices: Vec<usize> = (num_common..exprs.len()).collect();
    let output_chunk_schema = project_output_chunk_schema(&expr_slot_schemas, &output_indices)?;

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: false,
                exprs,
                expr_slot_ids,
                expr_slot_schemas: Some(expr_slot_schemas),
                output_indices: Some(output_indices),
                output_slots: output_slots.clone(),
                output_chunk_schema: Some(output_chunk_schema),
            }),
        },
        layout: out_layout,
    })
}

#[cfg(test)]
mod tests {
    use super::project_output_chunk_schema;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::ChunkSlotSchema;
    use crate::types::{TPrimitiveType, TScalarType, TTypeDesc, TTypeNode, TTypeNodeType};

    fn scalar_type_desc(ty: TPrimitiveType) -> TTypeDesc {
        TTypeDesc::new(vec![TTypeNode {
            type_: TTypeNodeType::SCALAR,
            scalar_type: Some(TScalarType::new(ty, None, None, None)),
            struct_fields: None,
            is_named: None,
        }])
    }

    #[test]
    fn project_output_chunk_schema_keeps_common_expr_slots_without_descriptors() {
        let expr_slot_schemas = vec![
            ChunkSlotSchema::new(
                SlotId::new(128),
                "_expr_128",
                true,
                Some(scalar_type_desc(TPrimitiveType::BOOLEAN)),
                None,
            ),
            ChunkSlotSchema::new(
                SlotId::new(129),
                "_expr_129",
                true,
                Some(scalar_type_desc(TPrimitiveType::BOOLEAN)),
                None,
            ),
            ChunkSlotSchema::new(
                SlotId::new(5),
                "ss_customer_sk",
                true,
                Some(scalar_type_desc(TPrimitiveType::INT)),
                None,
            ),
            ChunkSlotSchema::new(
                SlotId::new(61),
                "cs_bill_customer_sk",
                true,
                Some(scalar_type_desc(TPrimitiveType::INT)),
                None,
            ),
            ChunkSlotSchema::new(
                SlotId::new(128),
                "_expr_128",
                true,
                Some(scalar_type_desc(TPrimitiveType::BOOLEAN)),
                None,
            ),
        ];

        let output_chunk_schema =
            project_output_chunk_schema(&expr_slot_schemas, &[2, 3, 4]).expect("output schema");

        let slots = output_chunk_schema.slots();
        assert_eq!(slots.len(), 3);
        assert_eq!(slots[0].slot_id().as_u32(), 5);
        assert_eq!(slots[1].slot_id().as_u32(), 61);
        assert_eq!(slots[2].slot_id().as_u32(), 128);
        assert_eq!(slots[2].name(), "_expr_128");
        assert_eq!(
            slots[2]
                .type_desc()
                .and_then(crate::lower::type_lowering::primitive_type_from_desc),
            Some(TPrimitiveType::BOOLEAN)
        );
    }
}
