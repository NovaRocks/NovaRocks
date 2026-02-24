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

use crate::common::ids::SlotId;
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::lower::layout::Layout;
use crate::lower::node::Lowered;
use crate::{plan_nodes, types};

pub(crate) fn lower_repeat_node(
    child: Lowered,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
) -> Result<Lowered, String> {
    let Some(repeat) = node.repeat_node.as_ref() else {
        return Err("REPEAT_NODE missing repeat_node payload".to_string());
    };
    let repeat_times = repeat.repeat_id_list.len();
    if repeat_times == 0 {
        return Err("REPEAT_NODE repeat_id_list is empty".to_string());
    }
    if repeat.slot_id_set_list.len() != repeat_times {
        return Err(format!(
            "REPEAT_NODE slot_id_set_list size mismatch: repeat_id_list={} slot_id_set_list={}",
            repeat_times,
            repeat.slot_id_set_list.len()
        ));
    }
    if repeat.grouping_list.is_empty() {
        return Err("REPEAT_NODE grouping_list is empty".to_string());
    }
    for (idx, row) in repeat.grouping_list.iter().enumerate() {
        if row.len() != repeat_times {
            return Err(format!(
                "REPEAT_NODE grouping_list[{}] size mismatch: expected={} actual={}",
                idx,
                repeat_times,
                row.len()
            ));
        }
    }

    let out_tuple_id = repeat.output_tuple_id;
    let virtual_slots = tuple_slots.get(&out_tuple_id).ok_or_else(|| {
        format!("REPEAT_NODE missing tuple_slots for output_tuple_id={out_tuple_id}")
    })?;
    let virtual_slot_ids: Vec<SlotId> = virtual_slots
        .iter()
        .map(|sid| SlotId::try_from(*sid))
        .collect::<Result<Vec<_>, _>>()?;
    if virtual_slot_ids.len() != repeat.grouping_list.len() {
        return Err(format!(
            "REPEAT_NODE grouping_list row count mismatch: expected={} actual={}",
            virtual_slot_ids.len(),
            repeat.grouping_list.len()
        ));
    }

    // FE encodes `slot_id_set_list` as the slot ids to keep for each repeat.
    // Repeat runtime needs the complement set (slots to be set NULL).
    let all_slot_ids: Vec<SlotId> = repeat
        .all_slot_ids
        .iter()
        .map(|sid| SlotId::try_from(*sid))
        .collect::<Result<Vec<_>, _>>()?;
    let mut null_slot_ids: Vec<Vec<SlotId>> = Vec::with_capacity(repeat_times);
    for keep_set in &repeat.slot_id_set_list {
        let keep_slots: HashSet<SlotId> = keep_set
            .iter()
            .map(|sid| {
                if !repeat.all_slot_ids.contains(sid) {
                    return Err(format!(
                        "REPEAT_NODE slot_id_set_list contains unknown slot id {}",
                        sid
                    ));
                }
                SlotId::try_from(*sid).map_err(Into::into)
            })
            .collect::<Result<HashSet<_>, String>>()?;

        let mut null_slots: Vec<SlotId> = all_slot_ids
            .iter()
            .copied()
            .filter(|sid| !keep_slots.contains(sid))
            .collect();
        null_slots.sort_by_key(|sid| sid.as_u32());
        null_slot_ids.push(null_slots);
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::Repeat(RepeatNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                null_slot_ids,
                grouping_slot_ids: virtual_slot_ids,
                grouping_list: repeat.grouping_list.clone(),
                repeat_times,
            }),
        },
        layout: out_layout,
    })
}
