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

use crate::common::ids::SlotId;
use crate::descriptors;
use crate::exec::node::lookup::LookUpNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::exec::row_position::RowPositionDescriptor;
use crate::lower::layout::Layout;
use crate::lower::node::Lowered;
use crate::plan_nodes;
use crate::types;

fn lower_row_pos_descs(
    descs: &BTreeMap<i32, descriptors::TRowPositionDescriptor>,
) -> Result<HashMap<i32, RowPositionDescriptor>, String> {
    let mut out = HashMap::new();
    for (tuple_id, desc) in descs {
        let row_position_type = desc
            .row_position_type
            .ok_or_else(|| "missing row_position_type".to_string())?;
        if row_position_type != descriptors::TRowPositionType::ICEBERG_V3_ROW_POSITION {
            return Err(format!(
                "unsupported row position type: {:?}",
                row_position_type
            ));
        }
        let row_source_slot = desc
            .row_source_slot
            .ok_or_else(|| "missing row_source_slot".to_string())?;
        let row_source_slot = SlotId::try_from(row_source_slot)?;
        let fetch_ref_slots = desc
            .fetch_ref_slots
            .as_ref()
            .ok_or_else(|| "missing fetch_ref_slots".to_string())?
            .iter()
            .map(|v| SlotId::try_from(*v))
            .collect::<Result<Vec<_>, _>>()?;
        let lookup_ref_slots = desc
            .lookup_ref_slots
            .as_ref()
            .ok_or_else(|| "missing lookup_ref_slots".to_string())?
            .iter()
            .map(|v| SlotId::try_from(*v))
            .collect::<Result<Vec<_>, _>>()?;
        out.insert(
            *tuple_id,
            RowPositionDescriptor {
                row_position_type,
                row_source_slot,
                fetch_ref_slots,
                lookup_ref_slots,
            },
        );
    }
    Ok(out)
}

pub(crate) fn lower_lookup_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
) -> Result<Lowered, String> {
    if !children.is_empty() {
        return Err(format!(
            "LOOKUP_NODE expected 0 children, got {}",
            children.len()
        ));
    }
    let look_up = node
        .look_up_node
        .as_ref()
        .ok_or_else(|| "LOOKUP_NODE missing look_up_node payload".to_string())?;
    let descs = look_up
        .row_pos_descs
        .as_ref()
        .ok_or_else(|| "LOOKUP_NODE missing row_pos_descs".to_string())?;
    let row_pos_descs = lower_row_pos_descs(descs)?;
    if row_pos_descs.is_empty() {
        return Err("LOOKUP_NODE row_pos_descs is empty".to_string());
    }

    let mut row_pos_slots: HashSet<types::TSlotId> = HashSet::new();
    for desc in row_pos_descs.values() {
        row_pos_slots.insert(desc.row_source_slot.as_u32() as i32);
        for slot in &desc.lookup_ref_slots {
            row_pos_slots.insert(slot.as_u32() as i32);
        }
    }

    let mut output_slots = Vec::new();
    let mut output_slots_by_tuple: HashMap<i32, Vec<SlotId>> = HashMap::new();
    let mut order = Vec::new();
    for (tuple_id, slot_id) in &out_layout.order {
        if row_pos_slots.contains(slot_id) {
            continue;
        }
        order.push((*tuple_id, *slot_id));
        let slot = SlotId::try_from(*slot_id)?;
        output_slots.push(slot);
        output_slots_by_tuple
            .entry(*tuple_id)
            .or_default()
            .push(slot);
    }

    let index = order
        .iter()
        .enumerate()
        .map(|(idx, key)| (*key, idx))
        .collect();
    let lookup_layout = Layout { order, index };

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::LookUp(LookUpNode {
                node_id: node.node_id,
                row_pos_descs,
                output_slots,
                output_slots_by_tuple,
            }),
        },
        layout: lookup_layout,
    })
}
