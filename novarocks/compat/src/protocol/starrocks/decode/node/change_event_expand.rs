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

use arrow::datatypes::DataType;

use crate::protocol::starrocks::decode::error::StarRocksFragmentDecodeError;
use crate::protocol::starrocks::decode::expr::lower_t_expr_at;
use crate::protocol::starrocks::decode::layout::{Layout, chunk_schema_for_layout};
use crate::protocol::starrocks::decode::node::Lowered;
use crate::thrift::{descriptors, plan_nodes, types};
use novarocks::common::ids::SlotId;
use novarocks::exec::change_op::ChangeStreamBranchKind;
use novarocks::exec::expr::{ExprArena, ExprId};
use novarocks::exec::node::change_event_expand::{
    ChangeEventExpandNode, ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::protocol::FieldPath;

pub(crate) fn lower_change_event_expand_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    out_layout: Layout,
    arena: &mut ExprArena,
    desc_tbl: &descriptors::TDescriptorTable,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    node_path: FieldPath,
) -> Result<Lowered, StarRocksFragmentDecodeError> {
    let payload_path = node_path.clone().field("change_event_expand_node");
    if children.len() != 1 {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            node_path,
            format!(
                "CHANGE_EVENT_EXPAND_NODE expected 1 child, got {}",
                children.len()
            ),
        ));
    }
    let child = children.into_iter().next().expect("child");
    let payload = node.change_event_expand_node.as_ref().ok_or_else(|| {
        StarRocksFragmentDecodeError::missing(
            payload_path.clone(),
            format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} missing change_event_expand_node payload",
                node.node_id
            ),
        )
    })?;
    if payload.output_slot_ids.is_empty() {
        return Err(StarRocksFragmentDecodeError::missing(
            payload_path.clone().field("output_slot_ids"),
            format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} output_slot_ids is empty",
                node.node_id
            ),
        ));
    }

    let output_set: HashSet<types::TSlotId> = payload.output_slot_ids.iter().copied().collect();
    if output_set.len() != payload.output_slot_ids.len() {
        return Err(StarRocksFragmentDecodeError::inconsistent(
            payload_path.clone().field("output_slot_ids"),
            format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} output_slot_ids contains duplicates",
                node.node_id
            ),
        ));
    }
    require_route_slot_in_outputs(
        "change_op_slot_id",
        payload.change_op_slot_id,
        &output_set,
        node.node_id,
    )
    .map_err(|error| {
        StarRocksFragmentDecodeError::invalid_value(
            payload_path.clone().field("change_op_slot_id"),
            error,
        )
    })?;
    if let Some(data_route_slot_id) = payload.data_route_slot_id {
        if data_route_slot_id == payload.change_op_slot_id {
            return Err(StarRocksFragmentDecodeError::inconsistent(
                payload_path.clone().field("data_route_slot_id"),
                format!(
                    "CHANGE_EVENT_EXPAND_NODE node_id={} change_op_slot_id {} and data_route_slot_id {} must be distinct",
                    node.node_id, payload.change_op_slot_id, data_route_slot_id
                ),
            ));
        }
        require_route_slot_in_outputs(
            "data_route_slot_id",
            data_route_slot_id,
            &output_set,
            node.node_id,
        )
        .map_err(|error| {
            StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("data_route_slot_id"),
                error,
            )
        })?;
    }

    let mut events = Vec::with_capacity(payload.events.len());
    for (event_idx, event) in payload.events.iter().enumerate() {
        let event_path = payload_path.clone().field("events").index(event_idx);
        let branch_kind =
            change_event_branch_kind_from_thrift(event.branch_kind).map_err(|error| {
                StarRocksFragmentDecodeError::invalid_enum(
                    event_path.clone().field("branch_kind"),
                    error,
                )
            })?;
        if matches!(
            branch_kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        ) && payload.data_route_slot_id.is_none()
        {
            return Err(StarRocksFragmentDecodeError::missing(
                payload_path.clone().field("data_route_slot_id"),
                format!(
                    "CHANGE_EVENT_EXPAND_NODE node_id={} data branch {:?} requires data_route_slot_id",
                    node.node_id, branch_kind
                ),
            ));
        }
        let predicate = event
            .predicate
            .as_ref()
            .map(|expr| {
                lower_t_expr_at(
                    expr,
                    arena,
                    &child.layout,
                    last_query_id,
                    fe_addr,
                    event_path.clone().field("predicate"),
                )
            })
            .transpose()?;
        let mut assignments = Vec::with_capacity(event.assignments.len());
        for (assignment_index, assignment) in event.assignments.iter().enumerate() {
            let assignment_path = event_path
                .clone()
                .field("assignments")
                .index(assignment_index);
            if !output_set.contains(&assignment.output_slot_id) {
                return Err(StarRocksFragmentDecodeError::invalid_value(
                    assignment_path.clone().field("output_slot_id"),
                    format!(
                        "CHANGE_EVENT_EXPAND_NODE node_id={} assignment output slot {} is not in output_slot_ids",
                        node.node_id, assignment.output_slot_id
                    ),
                ));
            }
            let expr = event_assignment_expr(
                assignment,
                arena,
                &child.layout,
                last_query_id,
                fe_addr,
                assignment_path.clone(),
            )?;
            assignments.push(ChangeEventRuntimeOutputExpr {
                output_slot_id: SlotId::try_from(assignment.output_slot_id).map_err(|error| {
                    StarRocksFragmentDecodeError::invalid_value(
                        assignment_path.field("output_slot_id"),
                        error,
                    )
                })?,
                expr,
            });
        }
        events.push(ChangeEventRuntimeSpec {
            predicate,
            branch_kind,
            assignments,
        });
    }

    let output_slot_ids = payload
        .output_slot_ids
        .iter()
        .copied()
        .map(SlotId::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("output_slot_ids"),
                error,
            )
        })?;
    let layout =
        output_layout_for_slots(&out_layout, &payload.output_slot_ids).map_err(|error| {
            StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("output_slot_ids"),
                error,
            )
        })?;
    let output_chunk_schema = chunk_schema_for_layout(desc_tbl, &layout).map_err(|error| {
        StarRocksFragmentDecodeError::invalid_value(payload_path.clone(), error)
    })?;
    let change_op_slot_id = SlotId::try_from(payload.change_op_slot_id).map_err(|error| {
        StarRocksFragmentDecodeError::invalid_value(
            payload_path.clone().field("change_op_slot_id"),
            error,
        )
    })?;
    let change_op_slot = output_chunk_schema.slot(change_op_slot_id).ok_or_else(|| {
        StarRocksFragmentDecodeError::invalid_value(
            payload_path.clone().field("change_op_slot_id"),
            format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} change_op_slot_id {} is missing from output schema",
                node.node_id, payload.change_op_slot_id
            ),
        )
    })?;
    if change_op_slot.data_type() != &DataType::Int8 {
        return Err(StarRocksFragmentDecodeError::invalid_value(
            payload_path.clone().field("change_op_slot_id"),
            format!(
                "CHANGE_EVENT_EXPAND_NODE node_id={} change_op_slot_id {} must be TINYINT/Int8, got {:?}",
                node.node_id,
                payload.change_op_slot_id,
                change_op_slot.data_type()
            ),
        ));
    }
    let data_route_slot_id = payload
        .data_route_slot_id
        .map(SlotId::try_from)
        .transpose()
        .map_err(|error| {
            StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("data_route_slot_id"),
                error,
            )
        })?;
    if let Some(data_route_slot_id) = data_route_slot_id {
        let data_route_slot = output_chunk_schema.slot(data_route_slot_id).ok_or_else(|| {
            StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("data_route_slot_id"),
                format!(
                    "CHANGE_EVENT_EXPAND_NODE node_id={} data_route_slot_id {} is missing from output schema",
                    node.node_id, data_route_slot_id
                ),
            )
        })?;
        if !is_signed_integer_route_type(data_route_slot.data_type()) {
            return Err(StarRocksFragmentDecodeError::invalid_value(
                payload_path.clone().field("data_route_slot_id"),
                format!(
                    "CHANGE_EVENT_EXPAND_NODE node_id={} data_route_slot_id {} must be a signed integer route type, got {:?}",
                    node.node_id,
                    data_route_slot_id,
                    data_route_slot.data_type()
                ),
            ));
        }
    }

    Ok(Lowered {
        node: ExecNode {
            kind: ExecNodeKind::ChangeEventExpand(ChangeEventExpandNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                events,
                output_slot_ids,
                output_chunk_schema,
                change_op_slot_id,
                data_route_slot_id,
            }),
        },
        layout,
    })
}

fn is_signed_integer_route_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn require_route_slot_in_outputs(
    name: &str,
    slot_id: types::TSlotId,
    output_set: &HashSet<types::TSlotId>,
    node_id: i32,
) -> Result<(), String> {
    if !output_set.contains(&slot_id) {
        return Err(format!(
            "CHANGE_EVENT_EXPAND_NODE node_id={} {} {} is not in output_slot_ids",
            node_id, name, slot_id
        ));
    }
    Ok(())
}

fn event_assignment_expr(
    assignment: &plan_nodes::TChangeEventOutputExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
    assignment_path: FieldPath,
) -> Result<Option<ExprId>, StarRocksFragmentDecodeError> {
    assignment
        .expr
        .as_ref()
        .map(|expr| {
            lower_t_expr_at(
                expr,
                arena,
                input_layout,
                last_query_id,
                fe_addr,
                assignment_path.field("expr"),
            )
        })
        .transpose()
}

fn change_event_branch_kind_from_thrift(
    kind: plan_nodes::TChangeEventBranchKind,
) -> Result<ChangeStreamBranchKind, String> {
    match kind {
        plan_nodes::TChangeEventBranchKind::DELETE_DV => Ok(ChangeStreamBranchKind::DeleteDv),
        plan_nodes::TChangeEventBranchKind::REUSE_DATA => Ok(ChangeStreamBranchKind::ReuseData),
        plan_nodes::TChangeEventBranchKind::FRESH_DATA => Ok(ChangeStreamBranchKind::FreshData),
        other => Err(format!("unknown change event branch kind: {other:?}")),
    }
}

fn output_layout_for_slots(
    out_layout: &Layout,
    output_slot_ids: &[types::TSlotId],
) -> Result<Layout, String> {
    let requested: HashSet<types::TSlotId> = output_slot_ids.iter().copied().collect();
    let mut tuple_by_slot = HashMap::with_capacity(output_slot_ids.len());
    for (tuple_id, slot_id) in &out_layout.order {
        if !requested.contains(slot_id) {
            continue;
        }
        if let Some(previous_tuple_id) = tuple_by_slot.insert(*slot_id, *tuple_id)
            && previous_tuple_id != *tuple_id
        {
            return Err(format!(
                "CHANGE_EVENT_EXPAND_NODE output slot {} appears in multiple output layout tuples: {} and {}",
                slot_id, previous_tuple_id, tuple_id
            ));
        }
    }

    let mut order = Vec::with_capacity(output_slot_ids.len());
    for slot_id in output_slot_ids {
        let tuple_id = tuple_by_slot.get(slot_id).copied().ok_or_else(|| {
            format!(
                "CHANGE_EVENT_EXPAND_NODE output slot {} is missing from output layout",
                slot_id
            )
        })?;
        order.push((tuple_id, *slot_id));
    }
    let index = order
        .iter()
        .enumerate()
        .map(|(idx, key)| (*key, idx))
        .collect();
    Ok(Layout { order, index })
}
