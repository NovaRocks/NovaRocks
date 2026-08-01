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
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use super::DecodedNode;
use crate::native::plan_decode::error::NativeFragmentLeafDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks::exec::node::repeat::RepeatNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::plan;
use novarocks::protocol::common::error::{FieldPath, ProtocolErrorKind};

pub(super) fn lower_repeat_node(
    node: &plan::DistributedNode,
    repeat: &plan::RepeatNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
) -> Result<DecodedNode, crate::native::plan_decode::error::NativeFragmentDecodeError> {
    let decoded = (|| -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
        let child = children.pop().expect("child");
        let repeat_times = repeat.grouping_ids.len();
        if repeat_times == 0 {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::MissingField,
                "grouping_ids",
                "RepeatNode grouping_ids is empty",
            ));
        }
        if repeat.repeat_column_ref_ids.len() != repeat_times {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InconsistentFields,
                "repeat_column_ref_ids",
                format!(
                    "RepeatNode repeat_column_ref_ids size mismatch: expected {}, got {}",
                    repeat_times,
                    repeat.repeat_column_ref_ids.len()
                ),
            ));
        }
        let all_slot_ids = repeat
            .all_rollup_column_ids
            .iter()
            .copied()
            .map(SlotId::new)
            .collect::<Vec<_>>();
        let all_slot_set = all_slot_ids.iter().copied().collect::<HashSet<_>>();
        let null_slot_ids = repeat
            .repeat_column_ref_ids
            .iter()
            .enumerate()
            .map(|(idx, keep_ids)| {
                let keep = keep_ids
                    .values
                    .iter()
                    .copied()
                    .map(SlotId::new)
                    .collect::<HashSet<_>>();
                for (value_index, slot) in
                    keep_ids.values.iter().copied().map(SlotId::new).enumerate()
                {
                    if !all_slot_set.contains(&slot) {
                        return Err(NativeFragmentLeafDecodeError::at_field(
                            ProtocolErrorKind::InvalidValue,
                            "repeat_column_ref_ids",
                            format!(
                                "RepeatNode keep set {idx} contains unknown rollup slot {slot}"
                            ),
                        )
                        .append_index(idx)
                        .append_field("values")
                        .append_index(value_index));
                    }
                }
                let mut nulls = all_slot_ids
                    .iter()
                    .copied()
                    .filter(|slot| !keep.contains(slot))
                    .collect::<Vec<_>>();
                nulls.sort_by_key(|slot| slot.as_u32());
                Ok(nulls)
            })
            .collect::<Result<Vec<_>, NativeFragmentLeafDecodeError>>()?;
        let grouping_slot_ids = repeat
            .grouping_fn_ids
            .iter()
            .map(|entry| SlotId::new(entry.value))
            .collect::<Vec<_>>();
        let grouping_list = repeat_grouping_values(repeat)?;
        let (layout, output_schema) =
            repeat_output_layout_and_schema(&child, &repeat.grouping_fn_ids, &grouping_slot_ids)?;

        Ok(DecodedNode {
            node: ExecNode {
                kind: ExecNodeKind::Repeat(RepeatNode {
                    input: Box::new(child.node),
                    node_id: node.node_id,
                    null_slot_ids,
                    grouping_slot_ids,
                    grouping_list,
                    repeat_times,
                }),
            },
            layout,
            output_schema,
        })
    })();
    decoded.map_err(|error| error.into_native(path))
}

fn repeat_output_layout_and_schema(
    child: &DecodedNode,
    grouping_fn_ids: &[plan::NamedUInt32],
    grouping_slot_ids: &[SlotId],
) -> Result<(Layout, ChunkSchemaRef), NativeFragmentLeafDecodeError> {
    let mut slots = child.output_schema.slots().to_vec();
    let mut output_slot_ids = child.layout.order().to_vec();
    for (idx, slot_id) in grouping_slot_ids.iter().copied().enumerate() {
        if child.layout.contains_slot(slot_id) || output_slot_ids.contains(&slot_id) {
            return Err(NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::DuplicateField,
                "grouping_fn_ids",
                format!("RepeatNode grouping slot {slot_id} duplicates input slot"),
            )
            .append_index(idx)
            .append_field("value"));
        }
        let name = grouping_fn_ids
            .get(idx)
            .map(|entry| entry.name.as_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("__grouping_fn");
        let field = Field::new(name, DataType::Int64, true);
        slots.push(ChunkSlotSchema::new_with_field(slot_id, field, None, None));
        output_slot_ids.push(slot_id);
    }
    let layout = Layout::for_slots(output_slot_ids);
    let output_schema = Arc::new(ChunkSchema::try_new(slots).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "grouping_fn_ids",
            error,
        )
    })?);
    Ok((layout, output_schema))
}

fn repeat_grouping_values(
    repeat: &plan::RepeatNode,
) -> Result<Vec<Vec<i64>>, NativeFragmentLeafDecodeError> {
    if repeat.grouping_fn_ids.len() != repeat.grouping_fn_arg_ids.len() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "grouping_fn_arg_ids",
            format!(
                "RepeatNode grouping fn length mismatch: ids={} arg_ids={}",
                repeat.grouping_fn_ids.len(),
                repeat.grouping_fn_arg_ids.len()
            ),
        ));
    }
    let repeat_times = repeat.grouping_ids.len();
    let keep_sets = repeat
        .repeat_column_ref_ids
        .iter()
        .map(|ids| ids.values.iter().copied().collect::<HashSet<_>>())
        .collect::<Vec<_>>();
    repeat
        .grouping_fn_arg_ids
        .iter()
        .enumerate()
        .map(|(idx, args)| {
            if args.values.len() > 63 {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::OutOfRange,
                    "grouping_fn_arg_ids",
                    format!(
                        "RepeatNode grouping_fn_arg_ids[{idx}] has too many arguments: {}",
                        args.values.len()
                    ),
                )
                .append_index(idx)
                .append_field("values"));
            }
            let mut values = Vec::with_capacity(repeat_times);
            for keep in &keep_sets {
                let mut value = 0i64;
                for (arg_idx, column_id) in args.values.iter().enumerate() {
                    if !keep.contains(column_id) {
                        let reverse_bit_pos = args.values.len() - 1 - arg_idx;
                        value |= 1i64 << reverse_bit_pos;
                    }
                }
                values.push(value);
            }
            Ok(values)
        })
        .collect()
}
