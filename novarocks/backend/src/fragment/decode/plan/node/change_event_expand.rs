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

//! Fragment change-event-expand decoding.

use std::collections::HashSet;

use arrow::datatypes::DataType;

use super::{DecodedNode, NativePlanDecodeContext};
use crate::fragment::decode::plan::error::NativeFragmentDecodeError;
use crate::fragment::decode::plan::layout::Layout;
use novarocks_execution::exec::expr::ExprArena;
use novarocks_execution::exec::node::change_event_expand::{
    ChangeEventExpandNode, ChangeEventRuntimeOutputExpr, ChangeEventRuntimeSpec,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto::FieldPath;
use novarocks_proto::plan;
use novarocks_spi::connector::ConnectorRowMutationEffect;
use novarocks_types::SlotId;

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen native boundary keeps independently validated inputs explicit."
)]
pub(super) fn lower_change_event_expand_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    expand: &plan::ChangeEventExpandNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    let (output_columns, output_columns_path) = if expand.output_columns.is_empty() {
        (&physical.output_columns, physical_output_path)
    } else {
        (&expand.output_columns, path.clone().field("output_columns"))
    };
    let output_layout = ctx.decode_output_layout(output_columns, output_columns_path)?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let output_slot_ids = layout.order().to_vec();
    let output_set = output_slot_ids.iter().copied().collect::<HashSet<_>>();
    let effect_slot_id = SlotId::new(expand.effect_column_id);
    if !output_set.contains(&effect_slot_id) {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("effect_column_id"),
            format!(
                "ChangeEventExpandNode effect_column_id {} is not in outputs",
                expand.effect_column_id
            ),
        ));
    }
    let effect_field = output_schema.slot(effect_slot_id).ok_or_else(|| {
        NativeFragmentDecodeError::inconsistent(
            path.clone().field("effect_column_id"),
            format!(
                "ChangeEventExpandNode effect_column_id {} missing from output schema",
                expand.effect_column_id
            ),
        )
    })?;
    if effect_field.data_type() != &DataType::Int8 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("effect_column_id"),
            format!(
                "ChangeEventExpandNode effect_column_id {} must be Int8, got {:?}",
                expand.effect_column_id,
                effect_field.data_type()
            ),
        ));
    }

    let mut events = Vec::with_capacity(expand.events.len());
    for (event_idx, event) in expand.events.iter().enumerate() {
        let event_path = path.clone().field("events").index(event_idx);
        let effect = change_event_effect(event.effect, event_path.clone().field("effect"))?;
        let predicate = event
            .predicate
            .as_ref()
            .map(|expr| {
                ctx.decode_expression(
                    expr,
                    event_path.clone().field("predicate"),
                    arena,
                    &child.layout,
                )
            })
            .transpose()?;
        let assignments = event
            .assignments
            .iter()
            .enumerate()
            .map(|(assign_idx, assignment)| {
                let slot_id = SlotId::new(assignment.output_column_id);
                if !output_set.contains(&slot_id) {
                    return Err(NativeFragmentDecodeError::inconsistent(event_path.clone().field("assignments").index(assign_idx).field("output_column_id"), format!(
                        "ChangeEventExpandNode event {event_idx} assignment {assign_idx} output column {} is not in outputs",
                        assignment.output_column_id
                    )));
                }
                let expr = assignment
                    .expr
                    .as_ref()
                    .map(|expr| ctx.decode_expression(expr, event_path.clone().field("assignments").index(assign_idx).field("expr"), arena, &child.layout))
                    .transpose()?;
                Ok(ChangeEventRuntimeOutputExpr {
                    output_slot_id: slot_id,
                    expr,
                })
            })
            .collect::<Result<Vec<_>, NativeFragmentDecodeError>>()?;
        events.push(ChangeEventRuntimeSpec {
            predicate,
            effect,
            assignments,
        });
    }

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::ChangeEventExpand(ChangeEventExpandNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                events,
                output_slot_ids,
                output_chunk_schema: output_schema.clone(),
                effect_slot_id,
            }),
        },
        layout,
        output_schema,
    })
}

fn change_event_effect(
    value: i32,
    path: FieldPath,
) -> Result<ConnectorRowMutationEffect, NativeFragmentDecodeError> {
    match plan::RowMutationEffect::try_from(value).map_err(|_| {
        NativeFragmentDecodeError::invalid_enum(
            path.clone(),
            format!("unknown row mutation effect {value}"),
        )
    })? {
        plan::RowMutationEffect::Delete => Ok(ConnectorRowMutationEffect::Delete),
        plan::RowMutationEffect::Replace => Ok(ConnectorRowMutationEffect::Replace),
        plan::RowMutationEffect::Insert => Ok(ConnectorRowMutationEffect::Insert),
        plan::RowMutationEffect::Unspecified => Err(NativeFragmentDecodeError::invalid_enum(
            path,
            "row mutation effect is unspecified",
        )),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::tests::{one_col_values_node, output_column, physical_node};
    use super::super::{NativePlanDecodeContext, decode_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_proto::plan;

    #[test]
    fn change_event_rejects_invalid_effect_slot() {
        let missing_slot = physical_node(
            30,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    effect: plan::RowMutationEffect::Replace as i32,
                    assignments: Vec::new(),
                }],
                output_columns: vec![output_column(2, "effect", DataType::Int8)],
                effect_column_id: 3,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let mut arena = ExprArena::default();
        let err = decode_node(
            &missing_slot,
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap_err();
        assert!(err.contains("is not in outputs"));

        let non_integer = physical_node(
            31,
            plan::plan_node::Kind::ChangeEventExpand(plan::ChangeEventExpandNode {
                events: vec![plan::DistributedChangeEventSpec {
                    predicate: None,
                    effect: plan::RowMutationEffect::Replace as i32,
                    assignments: Vec::new(),
                }],
                output_columns: vec![output_column(2, "effect", DataType::Utf8)],
                effect_column_id: 2,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let mut arena = ExprArena::default();
        let err = decode_node(
            &non_integer,
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap_err();
        assert!(err.contains("must be Int8"));
    }
}
