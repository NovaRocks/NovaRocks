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
use crate::fragment::decode::plan::error::NativeFragmentDecodeError;
use crate::fragment::decode::plan::layout::Layout;
use novarocks_execution::exec::expr::{ExprArena, ExprNode};
use novarocks_execution::exec::node::unpivot::{
    UnpivotNode, UnpivotPassthroughColumn, UnpivotValueMapping,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::plan;
use novarocks_types::SlotId;

#[expect(
    clippy::too_many_arguments,
    reason = "The frozen native boundary keeps independently validated inputs explicit."
)]
pub(super) fn lower_unpivot_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    unpivot: &plan::UnpivotNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    if unpivot.max_output_rows == 0 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("max_output_rows"),
            "UnpivotNode max_output_rows must be greater than zero",
        ));
    }
    if unpivot.max_output_bytes == 0 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("max_output_bytes"),
            "UnpivotNode max_output_bytes must be greater than zero",
        ));
    }
    let max_output_rows = usize::try_from(unpivot.max_output_rows).map_err(|_| {
        NativeFragmentDecodeError::out_of_range(
            path.clone().field("max_output_rows"),
            format!(
                "UnpivotNode max_output_rows {} exceeds platform usize",
                unpivot.max_output_rows
            ),
        )
    })?;
    let max_output_bytes = usize::try_from(unpivot.max_output_bytes).map_err(|_| {
        NativeFragmentDecodeError::out_of_range(
            path.clone().field("max_output_bytes"),
            format!(
                "UnpivotNode max_output_bytes {} exceeds platform usize",
                unpivot.max_output_bytes
            ),
        )
    })?;
    if unpivot.value_mappings.is_empty() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("value_mappings"),
            "UnpivotNode requires at least one value mapping",
        ));
    }

    let output_layout =
        ctx.decode_output_layout(&physical.output_columns, physical_output_path.clone())?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let output_slots = output_schema
        .slot_ids()
        .iter()
        .copied()
        .collect::<HashSet<_>>();

    let mut output_roles = HashSet::new();
    let mut passthrough_columns = Vec::with_capacity(unpivot.passthrough_columns.len());
    for (index, mapping) in unpivot.passthrough_columns.iter().enumerate() {
        let mapping_path = path.clone().field("passthrough_columns").index(index);
        let input_slot_id = child
            .layout
            .resolve_column_id(mapping.input_column_id)
            .map_err(|error| error.into_native(mapping_path.clone()))?;
        let output_slot_id = SlotId::new(mapping.output_column_id);
        require_output_role(
            output_slot_id,
            &output_slots,
            &mut output_roles,
            mapping_path.clone().field("output_column_id"),
        )?;
        let input_slot = child.output_schema.slot(input_slot_id).ok_or_else(|| {
            NativeFragmentDecodeError::inconsistent(
                mapping_path.clone().field("input_column_id"),
                format!("UnpivotNode input slot {input_slot_id} is missing from child schema"),
            )
        })?;
        let output_slot = output_schema
            .slot(output_slot_id)
            .expect("validated output slot");
        require_exact_slot_shape(input_slot, output_slot, mapping_path.clone())?;
        passthrough_columns.push(UnpivotPassthroughColumn {
            input_slot_id,
            output_slot_id,
        });
    }

    let value_output_slot_id = SlotId::new(unpivot.value_output_column_id);
    require_output_role(
        value_output_slot_id,
        &output_slots,
        &mut output_roles,
        path.clone().field("value_output_column_id"),
    )?;
    let value_output = output_schema
        .slot(value_output_slot_id)
        .expect("validated value output slot");

    let mut literal_output_slot_ids = Vec::with_capacity(unpivot.literal_output_column_ids.len());
    for (index, column_id) in unpivot.literal_output_column_ids.iter().enumerate() {
        let slot_id = SlotId::new(*column_id);
        require_output_role(
            slot_id,
            &output_slots,
            &mut output_roles,
            path.clone().field("literal_output_column_ids").index(index),
        )?;
        literal_output_slot_ids.push(slot_id);
    }
    if output_roles != output_slots {
        let extras = output_slots
            .difference(&output_roles)
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(NativeFragmentDecodeError::inconsistent(
            physical_output_path,
            format!("UnpivotNode output schema contains unassigned slots [{extras}]"),
        ));
    }

    let mut value_nullable = false;
    let mut literal_nullable = vec![false; literal_output_slot_ids.len()];
    let mut value_mappings = Vec::with_capacity(unpivot.value_mappings.len());
    for (mapping_index, mapping) in unpivot.value_mappings.iter().enumerate() {
        let mapping_path = path.clone().field("value_mappings").index(mapping_index);
        let input_value_slot_id = child
            .layout
            .resolve_column_id(mapping.input_value_column_id)
            .map_err(|error| error.into_native(mapping_path.clone()))?;
        let input_value = child
            .output_schema
            .slot(input_value_slot_id)
            .ok_or_else(|| {
                NativeFragmentDecodeError::inconsistent(
                    mapping_path.clone().field("input_value_column_id"),
                    format!(
                        "UnpivotNode input value slot {input_value_slot_id} is missing from child schema"
                    ),
                )
            })?;
        if input_value.data_type() != value_output.data_type()
            || input_value.field_schema() != value_output.field_schema()
        {
            return Err(NativeFragmentDecodeError::inconsistent(
                mapping_path.clone().field("input_value_column_id"),
                format!(
                    "UnpivotNode value type mismatch: input {:?}, output {:?}",
                    input_value.data_type(),
                    value_output.data_type()
                ),
            ));
        }
        value_nullable |= input_value.nullable();
        if mapping.literals.len() != literal_output_slot_ids.len() {
            return Err(NativeFragmentDecodeError::inconsistent(
                mapping_path.clone().field("literals"),
                format!(
                    "UnpivotNode mapping {mapping_index} literal count mismatch: expected {}, got {}",
                    literal_output_slot_ids.len(),
                    mapping.literals.len()
                ),
            ));
        }
        let mut literal_exprs = Vec::with_capacity(mapping.literals.len());
        for (literal_index, literal) in mapping.literals.iter().enumerate() {
            let literal_path = mapping_path.clone().field("literals").index(literal_index);
            let expr_id =
                ctx.decode_expression(literal, literal_path.clone(), arena, &Layout::default())?;
            if !matches!(arena.node(expr_id), Some(ExprNode::Literal(_))) {
                return Err(NativeFragmentDecodeError::inconsistent(
                    literal_path,
                    format!(
                        "UnpivotNode mapping {mapping_index} literal {literal_index} is not a literal expression"
                    ),
                ));
            }
            let literal_output = output_schema
                .slot(literal_output_slot_ids[literal_index])
                .expect("validated literal output slot");
            let expr_type = arena.data_type(expr_id).expect("decoded expression type");
            if expr_type != literal_output.data_type() {
                return Err(NativeFragmentDecodeError::inconsistent(
                    literal_path,
                    format!(
                        "UnpivotNode mapping {mapping_index} literal {literal_index} type mismatch: expression {expr_type:?}, output {:?}",
                        literal_output.data_type()
                    ),
                ));
            }
            literal_nullable[literal_index] |= literal.nullable;
            literal_exprs.push(expr_id);
        }
        value_mappings.push(UnpivotValueMapping {
            input_value_slot_id,
            literal_exprs,
        });
    }
    if value_nullable != value_output.nullable() {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("value_output_column_id"),
            format!(
                "UnpivotNode value output nullability mismatch: expected {value_nullable}, got {}",
                value_output.nullable()
            ),
        ));
    }
    for (index, nullable) in literal_nullable.into_iter().enumerate() {
        let output = output_schema
            .slot(literal_output_slot_ids[index])
            .expect("validated literal output slot");
        if nullable != output.nullable() {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("literal_output_column_ids").index(index),
                format!(
                    "UnpivotNode literal output nullability mismatch: expected {nullable}, got {}",
                    output.nullable()
                ),
            ));
        }
    }

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Unpivot(UnpivotNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                passthrough_columns,
                value_output_slot_id,
                literal_output_slot_ids,
                value_mappings,
                output_chunk_schema: output_schema.clone(),
                max_output_rows,
                max_output_bytes,
            }),
        },
        layout,
        output_schema,
    })
}

fn require_output_role(
    slot_id: SlotId,
    output_slots: &HashSet<SlotId>,
    assigned: &mut HashSet<SlotId>,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    if !output_slots.contains(&slot_id) {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            format!("UnpivotNode output slot {slot_id} is not in PlanNode.output_columns"),
        ));
    }
    if !assigned.insert(slot_id) {
        return Err(NativeFragmentDecodeError::inconsistent(
            path,
            format!("UnpivotNode output slot {slot_id} has multiple producers"),
        ));
    }
    Ok(())
}

fn require_exact_slot_shape(
    input: &novarocks_execution::exec::chunk::ChunkSlotSchema,
    output: &novarocks_execution::exec::chunk::ChunkSlotSchema,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    if input.data_type() != output.data_type()
        || input.nullable() != output.nullable()
        || input.field_schema() != output.field_schema()
    {
        Err(NativeFragmentDecodeError::inconsistent(
            path,
            format!(
                "UnpivotNode passthrough shape mismatch: input {:?} nullable={}, output {:?} nullable={}",
                input.data_type(),
                input.nullable(),
                output.data_type(),
                output.nullable()
            ),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::super::tests::{
        column_ref, one_col_values_node, output_column_with_nullable, physical_node, string_literal,
    };
    use super::super::{NativePlanDecodeContext, decode_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto_models::plan;

    fn valid_unpivot(literal: novarocks_proto_models::expr::Expr) -> plan::DistributedNode {
        physical_node(
            20,
            plan::plan_node::Kind::Unpivot(plan::UnpivotNode {
                passthrough_columns: Vec::new(),
                value_output_column_id: 11,
                literal_output_column_ids: vec![10],
                value_mappings: vec![plan::UnpivotValueMapping {
                    input_value_column_id: 1,
                    literals: vec![literal],
                }],
                max_output_rows: 1024,
                max_output_bytes: 1024 * 1024,
            }),
            vec![
                output_column_with_nullable(10, "label", DataType::Utf8, false),
                output_column_with_nullable(11, "value", DataType::Int64, true),
            ],
            vec![one_col_values_node(10)],
        )
    }

    #[test]
    fn lowers_typed_unpivot_contract() {
        let mut arena = ExprArena::default();
        let decoded = decode_node(
            &valid_unpivot(string_literal("first")),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap();
        let ExecNodeKind::Unpivot(unpivot) = decoded.node.kind else {
            panic!("expected Unpivot");
        };
        assert_eq!(unpivot.value_mappings.len(), 1);
        assert_eq!(unpivot.literal_output_slot_ids.len(), 1);
        assert_eq!(unpivot.max_output_rows, 1024);
    }

    #[test]
    fn rejects_non_literal_mapping_tag() {
        let mut arena = ExprArena::default();
        let error = decode_node(
            &valid_unpivot(column_ref(1, DataType::Utf8)),
            &mut arena,
            &NativePlanDecodeContext::default(),
        )
        .unwrap_err();
        assert!(error.contains("not found in input layout") || error.contains("not a literal"));
    }

    #[test]
    fn rejects_value_nullability_drift() {
        let mut node = valid_unpivot(string_literal("first"));
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
        else {
            panic!("physical node");
        };
        physical.output_columns[1].nullable = false;
        let mut arena = ExprArena::default();
        let error =
            decode_node(&node, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(
            error.contains("value output nullability mismatch"),
            "{error}"
        );
    }
}
