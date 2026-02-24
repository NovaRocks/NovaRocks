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

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::ids::SlotId;
use crate::exec::chunk::{Chunk, field_with_slot_id};
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::project::ProjectNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::expr::lower_t_expr;
use crate::lower::layout::Layout;
use crate::lower::node::Lowered;
use crate::lower::type_lowering::arrow_type_from_desc;
use crate::{data, descriptors, exprs, plan_nodes, types};

pub(crate) type QueryGlobalDictMap = HashMap<types::TSlotId, Arc<HashMap<i32, Vec<u8>>>>;

pub(crate) fn build_query_global_dict_map(
    query_global_dicts: Option<&[data::TGlobalDict]>,
    query_global_dict_exprs: Option<&BTreeMap<i32, exprs::TExpr>>,
) -> Result<QueryGlobalDictMap, String> {
    let mut out = QueryGlobalDictMap::new();
    let Some(dicts) = query_global_dicts else {
        return Ok(out);
    };
    for dict in dicts {
        let column_id = dict
            .column_id
            .ok_or_else(|| "query_global_dict missing column_id".to_string())?;
        let strings = dict
            .strings
            .as_ref()
            .ok_or_else(|| format!("query_global_dict column_id={} missing strings", column_id))?;
        let ids = dict
            .ids
            .as_ref()
            .ok_or_else(|| format!("query_global_dict column_id={} missing ids", column_id))?;
        if strings.len() != ids.len() {
            return Err(format!(
                "query_global_dict column_id={} strings/ids length mismatch: {} vs {}",
                column_id,
                strings.len(),
                ids.len()
            ));
        }
        let mut dict_values = HashMap::with_capacity(ids.len());
        for (id, value) in ids.iter().zip(strings.iter()) {
            dict_values.insert(*id, value.clone());
        }
        out.insert(column_id, Arc::new(dict_values));
    }

    // FE may send derived dictionary expressions in `query_global_dict_exprs`, where target
    // dict slots are defined from source dict slots (for example `upper(<placeholder>)`).
    // We must derive target slot dictionary values from expression semantics instead of
    // reusing source dictionaries directly.
    if let Some(dict_exprs) = query_global_dict_exprs {
        for (target_slot, expr) in dict_exprs {
            if out.contains_key(target_slot) {
                continue;
            }
            if let Some(derived_dict) = derive_query_global_dict_expr(expr, &out)? {
                out.insert(*target_slot, Arc::new(derived_dict));
                continue;
            }

            // Fallback for non-DICT_EXPR/unsupported derived exprs:
            // reuse source dictionary only when there is a single referenced slot.
            let mut referenced_slots = HashSet::new();
            for node in &expr.nodes {
                if node.node_type == exprs::TExprNodeType::SLOT_REF {
                    if let Some(slot_ref) = node.slot_ref.as_ref() {
                        referenced_slots.insert(slot_ref.slot_id);
                    }
                }
                if node.node_type == exprs::TExprNodeType::PLACEHOLDER_EXPR {
                    if let Some(slot_id) = node.vslot_ref.as_ref().and_then(|v| v.slot_id) {
                        referenced_slots.insert(slot_id);
                    }
                }
            }
            if referenced_slots.len() != 1 {
                continue;
            }
            let source_slot = *referenced_slots.iter().next().expect("single slot");
            if let Some(source_dict) = out.get(&source_slot).cloned() {
                out.insert(*target_slot, source_dict);
            }
        }
    }

    Ok(out)
}

fn walk_subtree_end(nodes: &[exprs::TExprNode], idx: &mut usize) -> Result<(), String> {
    let node = nodes
        .get(*idx)
        .ok_or_else(|| format!("invalid expr node index {}", *idx))?;
    *idx += 1;
    for _ in 0..node.num_children {
        walk_subtree_end(nodes, idx)?;
    }
    Ok(())
}

fn subtree_end(nodes: &[exprs::TExprNode], start: usize) -> Result<usize, String> {
    let mut idx = start;
    walk_subtree_end(nodes, &mut idx)?;
    Ok(idx)
}

fn extract_dict_expr_source_and_mapped(
    expr: &exprs::TExpr,
) -> Result<Option<(types::TSlotId, exprs::TExpr)>, String> {
    let Some(root) = expr.nodes.first() else {
        return Ok(None);
    };
    if root.node_type != exprs::TExprNodeType::DICT_EXPR || root.num_children < 2 {
        return Ok(None);
    }

    let nodes = &expr.nodes;
    let first_child_start = 1usize;
    let first_child_end = subtree_end(nodes, first_child_start)?;
    let source_slot = nodes
        .get(first_child_start)
        .and_then(|n| n.slot_ref.as_ref())
        .map(|s| s.slot_id)
        .ok_or_else(|| "DICT_EXPR source child is not SLOT_REF".to_string())?;

    let mapped_start = first_child_end;
    let mapped_end = subtree_end(nodes, mapped_start)?;
    let mapped_nodes = nodes[mapped_start..mapped_end].to_vec();

    Ok(Some((
        source_slot,
        exprs::TExpr {
            nodes: mapped_nodes,
        },
    )))
}

fn collect_expr_slot_inputs(
    expr: &exprs::TExpr,
) -> Result<Vec<(types::TSlotId, DataType)>, String> {
    let mut slot_inputs: BTreeMap<types::TSlotId, DataType> = BTreeMap::new();
    for node in &expr.nodes {
        if node.node_type == exprs::TExprNodeType::PLACEHOLDER_EXPR {
            let slot_id = node
                .vslot_ref
                .as_ref()
                .and_then(|v| v.slot_id)
                .ok_or_else(|| "PLACEHOLDER_EXPR missing vslot_ref.slot_id".to_string())?;
            let data_type = arrow_type_from_desc(&node.type_).unwrap_or(DataType::Utf8);
            match slot_inputs.get(&slot_id) {
                Some(existing) if existing != &data_type => {
                    return Err(format!(
                        "dict expr slot {} has conflicting types {:?} vs {:?}",
                        slot_id, existing, data_type
                    ));
                }
                None => {
                    slot_inputs.insert(slot_id, data_type);
                }
                _ => {}
            }
            continue;
        }
        if node.node_type == exprs::TExprNodeType::SLOT_REF {
            if let Some(slot_ref) = node.slot_ref.as_ref() {
                let slot_id = slot_ref.slot_id;
                let data_type = arrow_type_from_desc(&node.type_).unwrap_or(DataType::Int32);
                match slot_inputs.get(&slot_id) {
                    Some(existing) if existing != &data_type => {
                        return Err(format!(
                            "dict expr slot {} has conflicting types {:?} vs {:?}",
                            slot_id, existing, data_type
                        ));
                    }
                    None => {
                        slot_inputs.insert(slot_id, data_type);
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(slot_inputs.into_iter().collect())
}

fn build_slot_array_for_dict_entries(
    data_type: &DataType,
    dict_entries: &[(i32, Option<Vec<u8>>)],
) -> Result<ArrayRef, String> {
    match data_type {
        DataType::Utf8 => {
            let mut values = Vec::with_capacity(dict_entries.len());
            for (_code, bytes) in dict_entries {
                let value = bytes
                    .as_ref()
                    .map(|b| {
                        std::str::from_utf8(b)
                            .map(|s| s.to_string())
                            .map_err(|e| format!("dict entry is not valid utf8: {}", e))
                    })
                    .transpose()?;
                values.push(value);
            }
            Ok(Arc::new(StringArray::from(values)))
        }
        DataType::LargeUtf8 => {
            let mut values = Vec::with_capacity(dict_entries.len());
            for (_code, bytes) in dict_entries {
                let value = bytes
                    .as_ref()
                    .map(|b| {
                        std::str::from_utf8(b)
                            .map(|s| s.to_string())
                            .map_err(|e| format!("dict entry is not valid utf8: {}", e))
                    })
                    .transpose()?;
                values.push(value);
            }
            Ok(Arc::new(LargeStringArray::from(values)))
        }
        DataType::Binary => {
            let values = dict_entries
                .iter()
                .map(|(_, bytes)| bytes.as_ref().map(|b| b.as_slice()))
                .collect::<Vec<_>>();
            Ok(Arc::new(BinaryArray::from(values)))
        }
        DataType::LargeBinary => {
            let values = dict_entries
                .iter()
                .map(|(_, bytes)| bytes.as_ref().map(|b| b.as_slice()))
                .collect::<Vec<_>>();
            Ok(Arc::new(LargeBinaryArray::from(values)))
        }
        DataType::Int8 => {
            let mut values = Vec::with_capacity(dict_entries.len());
            for (code, bytes) in dict_entries {
                if bytes.is_none() {
                    values.push(None);
                    continue;
                }
                values.push(Some(
                    i8::try_from(*code).map_err(|_| format!("dict code {} overflows i8", code))?,
                ));
            }
            Ok(Arc::new(Int8Array::from(values)))
        }
        DataType::Int16 => {
            let mut values = Vec::with_capacity(dict_entries.len());
            for (code, bytes) in dict_entries {
                if bytes.is_none() {
                    values.push(None);
                    continue;
                }
                values.push(Some(
                    i16::try_from(*code)
                        .map_err(|_| format!("dict code {} overflows i16", code))?,
                ));
            }
            Ok(Arc::new(Int16Array::from(values)))
        }
        DataType::Int32 => {
            let values = dict_entries
                .iter()
                .map(|(code, bytes)| bytes.as_ref().map(|_| *code))
                .collect::<Vec<_>>();
            Ok(Arc::new(Int32Array::from(values)))
        }
        DataType::Int64 => {
            let values = dict_entries
                .iter()
                .map(|(code, bytes)| bytes.as_ref().map(|_| i64::from(*code)))
                .collect::<Vec<_>>();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::UInt8 => {
            let mut values = Vec::with_capacity(dict_entries.len());
            for (code, bytes) in dict_entries {
                if bytes.is_none() {
                    values.push(None);
                    continue;
                }
                values.push(Some(
                    u8::try_from(*code).map_err(|_| format!("dict code {} overflows u8", code))?,
                ));
            }
            Ok(Arc::new(UInt8Array::from(values)))
        }
        DataType::UInt16 => {
            let mut values = Vec::with_capacity(dict_entries.len());
            for (code, bytes) in dict_entries {
                if bytes.is_none() {
                    values.push(None);
                    continue;
                }
                values.push(Some(
                    u16::try_from(*code)
                        .map_err(|_| format!("dict code {} overflows u16", code))?,
                ));
            }
            Ok(Arc::new(UInt16Array::from(values)))
        }
        DataType::UInt32 => {
            let mut values = Vec::with_capacity(dict_entries.len());
            for (code, bytes) in dict_entries {
                if bytes.is_none() {
                    values.push(None);
                    continue;
                }
                values.push(Some(
                    u32::try_from(*code)
                        .map_err(|_| format!("dict code {} overflows u32", code))?,
                ));
            }
            Ok(Arc::new(UInt32Array::from(values)))
        }
        DataType::UInt64 => {
            let mut values = Vec::with_capacity(dict_entries.len());
            for (code, bytes) in dict_entries {
                if bytes.is_none() {
                    values.push(None);
                    continue;
                }
                values.push(Some(
                    u64::try_from(*code)
                        .map_err(|_| format!("dict code {} overflows u64", code))?,
                ));
            }
            Ok(Arc::new(UInt64Array::from(values)))
        }
        other => Err(format!(
            "unsupported dict expr slot input type for derivation: {:?}",
            other
        )),
    }
}

fn supports_dict_expr_derivation_slot_input_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn array_value_to_bytes(array: &ArrayRef, row: usize) -> Result<Option<Vec<u8>>, String> {
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast Utf8 result array".to_string())?;
            Ok(Some(arr.value(row).as_bytes().to_vec()))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "failed to downcast LargeUtf8 result array".to_string())?;
            Ok(Some(arr.value(row).as_bytes().to_vec()))
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| "failed to downcast Binary result array".to_string())?;
            Ok(Some(arr.value(row).to_vec()))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| "failed to downcast LargeBinary result array".to_string())?;
            Ok(Some(arr.value(row).to_vec()))
        }
        other => Err(format!(
            "derived dict expression result type must be string/binary, got {:?}",
            other
        )),
    }
}

fn derive_query_global_dict_expr(
    dict_expr: &exprs::TExpr,
    dict_map: &QueryGlobalDictMap,
) -> Result<Option<HashMap<i32, Vec<u8>>>, String> {
    let Some((source_slot, mapped_expr)) = extract_dict_expr_source_and_mapped(dict_expr)? else {
        return Ok(None);
    };
    let Some(source_dict) = dict_map.get(&source_slot) else {
        return Ok(None);
    };

    let mut dict_entries = source_dict
        .iter()
        .map(|(code, value)| (*code, Some(value.clone())))
        .collect::<Vec<_>>();
    // Dictionary id 0 is the null sentinel for encoded string slots. Add a synthetic null row so
    // derived expressions (for example `if(<placeholder> IS NOT NULL, 'A', 'B')`) can decide
    // whether null input maps to null or to a concrete output value.
    if !dict_entries.iter().any(|(code, _)| *code == 0) {
        dict_entries.push((0, None));
    }
    dict_entries.sort_by_key(|(code, _)| *code);
    if dict_entries.is_empty() {
        return Ok(Some(HashMap::new()));
    }

    let slot_inputs = collect_expr_slot_inputs(&mapped_expr)?;
    if slot_inputs.is_empty() {
        return Err("derived dict expression has no slot inputs".to_string());
    }
    if slot_inputs
        .iter()
        .any(|(_slot_id, data_type)| !supports_dict_expr_derivation_slot_input_type(data_type))
    {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(slot_inputs.len());
    let mut columns = Vec::with_capacity(slot_inputs.len());
    for (slot_id, data_type) in &slot_inputs {
        let slot_id_u32 =
            u32::try_from(*slot_id).map_err(|_| format!("slot id {} overflows u32", slot_id))?;
        let slot = SlotId::new(slot_id_u32);
        let field = field_with_slot_id(
            Field::new(format!("slot_{}", slot_id), data_type.clone(), true),
            slot,
        );
        fields.push(field);
        columns.push(build_slot_array_for_dict_entries(data_type, &dict_entries)?);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("failed to build dict expr input batch: {}", e))?;
    let chunk = Chunk::new(batch);

    let mut arena = ExprArena::default();
    let mapped_expr_id = lower_t_expr(
        &mapped_expr,
        &mut arena,
        &Layout {
            order: Vec::new(),
            index: HashMap::new(),
        },
        None,
        None,
    )?;
    let mapped_array = arena.eval(mapped_expr_id, &chunk)?;

    let mut derived = HashMap::with_capacity(dict_entries.len());
    for (row, (code, original)) in dict_entries.iter().enumerate() {
        let mapped_value = array_value_to_bytes(&mapped_array, row)?;
        if let Some(value) = mapped_value {
            derived.insert(*code, value);
            continue;
        }
        if let Some(original) = original {
            // Keep previous fallback behavior for non-null source dictionary rows.
            derived.insert(*code, original.clone());
        }
    }
    Ok(Some(derived))
}

fn slot_type_map(
    desc_tbl: Option<&descriptors::TDescriptorTable>,
) -> HashMap<types::TSlotId, DataType> {
    let mut out = HashMap::new();
    let Some(desc_tbl) = desc_tbl else {
        return out;
    };
    let Some(slot_descs) = desc_tbl.slot_descriptors.as_ref() else {
        return out;
    };
    for slot in slot_descs {
        let (Some(slot_id), Some(slot_type)) = (slot.id, slot.slot_type.as_ref()) else {
            continue;
        };
        if let Some(data_type) = arrow_type_from_desc(slot_type) {
            out.insert(slot_id, data_type);
        }
    }
    out
}

fn supports_dict_decode_input_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => true,
        DataType::List(field) | DataType::LargeList(field) => {
            supports_dict_decode_input_type(field.data_type())
        }
        _ => false,
    }
}

pub(crate) fn lower_decode_node(
    child: Lowered,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    arena: &mut ExprArena,
    desc_tbl: Option<&descriptors::TDescriptorTable>,
    query_global_dict_map: &QueryGlobalDictMap,
) -> Result<Lowered, String> {
    let decode = node
        .decode_node
        .as_ref()
        .ok_or_else(|| "DECODE_NODE missing decode_node payload".to_string())?;
    let Some(dict_id_to_string_ids) = decode.dict_id_to_string_ids.as_ref() else {
        // No remapping means decode is a no-op for novarocks.
        return Ok(child);
    };
    if dict_id_to_string_ids.is_empty() {
        return Ok(child);
    }

    let slot_types = slot_type_map(desc_tbl);
    // Output slot id -> encoded dict slot id.
    let mut decode_input_slots = HashMap::with_capacity(dict_id_to_string_ids.len());
    for (dict_slot_id, string_slot_id) in dict_id_to_string_ids {
        decode_input_slots.insert(*string_slot_id, *dict_slot_id);
    }

    let mut exprs = Vec::with_capacity(out_layout.order.len());
    let mut expr_slot_ids = Vec::with_capacity(out_layout.order.len());
    for (_tuple_id, output_slot_id) in &out_layout.order {
        let output_slot = SlotId::try_from(*output_slot_id)?;
        let output_type = slot_types
            .get(output_slot_id)
            .cloned()
            .unwrap_or(DataType::Utf8);
        let expr_id = if let Some(encoded_slot_id) = decode_input_slots.get(output_slot_id) {
            let dict_values = query_global_dict_map.get(encoded_slot_id).ok_or_else(|| {
                format!(
                    "missing query global dict for encoded slot_id={}",
                    encoded_slot_id
                )
            })?;
            let encoded_slot = SlotId::try_from(*encoded_slot_id)?;
            let encoded_type = slot_types
                .get(encoded_slot_id)
                .cloned()
                .unwrap_or(DataType::Int32);
            let encoded_expr = arena.push_typed(ExprNode::SlotId(encoded_slot), encoded_type);
            if arena
                .data_type(encoded_expr)
                .is_some_and(supports_dict_decode_input_type)
            {
                arena.push_typed(
                    ExprNode::DictDecode {
                        child: encoded_expr,
                        dict: Arc::clone(dict_values),
                    },
                    output_type,
                )
            } else {
                // If upstream already materializes string values for the encoded slot,
                // keep it as-is and just remap the slot id.
                encoded_expr
            }
        } else {
            // Keep passthrough slots unchanged.
            arena.push_typed(ExprNode::SlotId(output_slot), output_type)
        };
        exprs.push(expr_id);
        expr_slot_ids.push(output_slot);
    }

    let output_indices: Vec<usize> = (0..exprs.len()).collect();
    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Project(ProjectNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                is_subordinate: false,
                exprs,
                expr_slot_ids,
                output_indices: Some(output_indices),
                output_slots: out_layout
                    .order
                    .iter()
                    .map(|(_, slot_id)| SlotId::try_from(*slot_id))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
        },
        layout: out_layout,
    })
}
