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
use novarocks_execution::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use novarocks_execution::exec::expr::{ExprArena, ExprNode};
use novarocks_execution::exec::node::unpivot::{
    UnpivotConstant, UnpivotNode, UnpivotPassthroughColumn, UnpivotValueMapping,
};
use novarocks_execution::exec::node::{ExecNode, ExecNodeKind};
use novarocks_proto_codec::{FieldPath, arrow_physical};
use novarocks_proto_models::plan;
use novarocks_types::SlotId;
use prost::Message;

const MAX_UNPIVOT_CONSTANT_BYTES: usize = 16 * 1024 * 1024;
const MAX_UNPIVOT_CONSTANT_ELEMENTS: usize = 4_096;
const MAX_UNPIVOT_CONSTANTS: usize = 16_384;
const MAX_UNPIVOT_MAPPINGS: usize = 4_096;

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
    if unpivot.value_mappings.len() > MAX_UNPIVOT_MAPPINGS {
        return Err(NativeFragmentDecodeError::out_of_range(
            path.clone().field("value_mappings"),
            "UnpivotNode exceeds the value mapping limit",
        ));
    }

    if !physical.output_columns.is_empty() {
        return Err(NativeFragmentDecodeError::inconsistent(
            physical_output_path.clone(),
            "UnpivotNode PlanNode.output_columns must be empty; output_schema is the only physical schema authority",
        ));
    }
    let output_schema = decode_unpivot_output_schema(
        unpivot.output_schema.as_ref(),
        path.clone().field("output_schema"),
    )?;
    let layout = Layout::for_slots(output_schema.slot_ids().iter().copied());
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
            path.clone().field("output_schema").field("columns"),
            format!("UnpivotNode output schema contains unassigned slots [{extras}]"),
        ));
    }

    let mut value_nullable = false;
    let mut literal_nullable = vec![false; literal_output_slot_ids.len()];
    let mut value_mappings = Vec::with_capacity(unpivot.value_mappings.len());
    let mut decoded_constant_count = 0usize;
    let mut decoded_nested_elements = 0usize;
    let mut decoded_constant_bytes = 0usize;
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
        if mapping.constants.len() != literal_output_slot_ids.len() {
            return Err(NativeFragmentDecodeError::inconsistent(
                mapping_path.clone().field("constants"),
                format!(
                    "UnpivotNode mapping {mapping_index} literal count mismatch: expected {}, got {}",
                    literal_output_slot_ids.len(),
                    mapping.constants.len()
                ),
            ));
        }
        decoded_constant_count = decoded_constant_count
            .checked_add(mapping.constants.len())
            .ok_or_else(|| {
                NativeFragmentDecodeError::out_of_range(
                    mapping_path.clone().field("constants"),
                    "Unpivot constant count overflowed",
                )
            })?;
        if decoded_constant_count > MAX_UNPIVOT_CONSTANTS {
            return Err(NativeFragmentDecodeError::out_of_range(
                mapping_path.clone().field("constants"),
                "UnpivotNode exceeds the constant count limit",
            ));
        }
        let mut constants = Vec::with_capacity(mapping.constants.len());
        for (literal_index, wire_constant) in mapping.constants.iter().enumerate() {
            let literal_path = mapping_path.clone().field("constants").index(literal_index);
            let (constant, constant_type, nullable) = decode_unpivot_constant(
                wire_constant,
                literal_path.clone(),
                arena,
                ctx,
                &mut decoded_nested_elements,
                &mut decoded_constant_bytes,
            )?;
            let literal_output = output_schema
                .slot(literal_output_slot_ids[literal_index])
                .expect("validated literal output slot");
            if &constant_type != literal_output.data_type() {
                return Err(NativeFragmentDecodeError::inconsistent(
                    literal_path,
                    format!(
                        "UnpivotNode mapping {mapping_index} constant {literal_index} type mismatch: constant {:?}, output {:?}",
                        constant_type,
                        literal_output.data_type()
                    ),
                ));
            }
            literal_nullable[literal_index] |= nullable;
            constants.push(constant);
        }
        value_mappings.push(UnpivotValueMapping {
            input_value_slot_id,
            constants,
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

fn decode_unpivot_output_schema(
    wire: Option<&plan::ArrowPhysicalSchema>,
    path: FieldPath,
) -> Result<ChunkSchemaRef, NativeFragmentDecodeError> {
    let wire = wire.ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone(), "UnpivotNode output_schema is required")
    })?;
    let decoded = arrow_physical::decode_schema(&wire.columns, &wire.schema_metadata, path.clone())
        .map_err(NativeFragmentDecodeError::from)?;
    let slot_ids = decoded
        .slot_ids()
        .iter()
        .copied()
        .map(SlotId::new)
        .collect::<Vec<_>>();
    ChunkSchema::try_ref_from_schema_and_slot_ids(decoded.schema().as_ref(), &slot_ids).map_err(
        |error| {
            NativeFragmentDecodeError::invalid_value(
                path.clone().field("columns"),
                format!("UnpivotNode output schema: {error}"),
            )
        },
    )
}

pub(super) fn decode_unpivot_constant(
    wire: &plan::UnpivotConstant,
    path: FieldPath,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
    decoded_nested_elements: &mut usize,
    decoded_constant_bytes: &mut usize,
) -> Result<(UnpivotConstant, arrow::datatypes::DataType, bool), NativeFragmentDecodeError> {
    use plan::unpivot_constant::Value;
    let value = wire.value.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("value"),
            "Unpivot constant value is required",
        )
    })?;
    match value {
        Value::ScalarLiteral(expression) => {
            charge_constant_bytes(
                decoded_constant_bytes,
                expression.encoded_len(),
                path.clone(),
            )?;
            let expr_id = ctx.decode_expression(
                expression,
                path.clone().field("scalar_literal"),
                arena,
                &Layout::default(),
            )?;
            if !matches!(arena.node(expr_id), Some(ExprNode::Literal(_))) {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone().field("scalar_literal"),
                    "Unpivot scalar constant is not a literal expression",
                ));
            }
            let data_type = arena
                .data_type(expr_id)
                .expect("decoded expression type")
                .clone();
            Ok((
                UnpivotConstant::Scalar {
                    expr_id,
                    nullable: expression.nullable,
                },
                data_type,
                expression.nullable,
            ))
        }
        Value::Int32List(values) => {
            if values.values.len() > MAX_UNPIVOT_CONSTANT_ELEMENTS {
                return Err(NativeFragmentDecodeError::out_of_range(
                    path.clone().field("int32_list"),
                    "Unpivot constant list exceeds the element limit",
                ));
            }
            charge_nested_elements(decoded_nested_elements, values.values.len(), path.clone())?;
            charge_constant_bytes(
                decoded_constant_bytes,
                values.values.len().saturating_mul(size_of::<i32>()),
                path.clone(),
            )?;
            Ok((
                UnpivotConstant::Int32List(values.values.clone()),
                arrow::datatypes::DataType::List(std::sync::Arc::new(
                    arrow::datatypes::Field::new("item", arrow::datatypes::DataType::Int32, false),
                )),
                false,
            ))
        }
        Value::Utf8Map(map) => {
            if map.entries.len() > MAX_UNPIVOT_CONSTANT_ELEMENTS {
                return Err(NativeFragmentDecodeError::out_of_range(
                    path.clone().field("utf8_map"),
                    "Unpivot constant map exceeds the entry limit",
                ));
            }
            charge_nested_elements(decoded_nested_elements, map.entries.len(), path.clone())?;
            let mut previous = None;
            let mut retained_bytes = 0usize;
            for (index, entry) in map.entries.iter().enumerate() {
                if entry.key.is_empty() {
                    return Err(NativeFragmentDecodeError::invalid_value(
                        path.clone().field("utf8_map").field("entries").index(index),
                        "Unpivot map constant key is empty",
                    ));
                }
                if previous.is_some_and(|key: &str| key >= entry.key.as_str()) {
                    return Err(NativeFragmentDecodeError::invalid_value(
                        path.clone().field("utf8_map").field("entries").index(index),
                        "Unpivot map constant keys must be strictly increasing",
                    ));
                }
                retained_bytes = retained_bytes
                    .checked_add(entry.key.len())
                    .and_then(|total| total.checked_add(entry.value.len()))
                    .ok_or_else(|| {
                        NativeFragmentDecodeError::out_of_range(
                            path.clone().field("utf8_map"),
                            "Unpivot map constant byte charge overflowed",
                        )
                    })?;
                previous = Some(entry.key.as_str());
            }
            charge_constant_bytes(decoded_constant_bytes, retained_bytes, path.clone())?;
            Ok((
                UnpivotConstant::Utf8Map(
                    map.entries
                        .iter()
                        .map(|entry| (entry.key.clone(), entry.value.clone()))
                        .collect(),
                ),
                arrow::datatypes::DataType::Map(
                    std::sync::Arc::new(arrow::datatypes::Field::new(
                        "entries",
                        arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::from(vec![
                            arrow::datatypes::Field::new(
                                "key",
                                arrow::datatypes::DataType::Utf8,
                                false,
                            ),
                            arrow::datatypes::Field::new(
                                "value",
                                arrow::datatypes::DataType::Utf8,
                                false,
                            ),
                        ])),
                        false,
                    )),
                    false,
                ),
                false,
            ))
        }
    }
}

fn charge_constant_bytes(
    total: &mut usize,
    amount: usize,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    *total = total.checked_add(amount).ok_or_else(|| {
        NativeFragmentDecodeError::out_of_range(
            path.clone(),
            "Unpivot decoded constant byte charge overflowed",
        )
    })?;
    if *total > MAX_UNPIVOT_CONSTANT_BYTES {
        return Err(NativeFragmentDecodeError::out_of_range(
            path,
            "UnpivotNode exceeds the decoded constant byte limit",
        ));
    }
    Ok(())
}

fn charge_nested_elements(
    total: &mut usize,
    amount: usize,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    *total = total.checked_add(amount).ok_or_else(|| {
        NativeFragmentDecodeError::out_of_range(
            path.clone(),
            "Unpivot nested element count overflowed",
        )
    })?;
    if *total > MAX_UNPIVOT_CONSTANT_ELEMENTS {
        return Err(NativeFragmentDecodeError::out_of_range(
            path,
            "UnpivotNode exceeds the nested element limit",
        ));
    }
    Ok(())
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
            format!("UnpivotNode output slot {slot_id} is not in output_schema"),
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
    use arrow::datatypes::{DataType, Field, Schema};

    use super::super::tests::{
        column_ref, one_col_values_node, output_column_with_nullable, physical_node, string_literal,
    };
    use super::super::{NativePlanDecodeContext, decode_node};
    use novarocks_execution::exec::expr::ExprArena;
    use novarocks_execution::exec::node::ExecNodeKind;
    use novarocks_proto_codec::{FieldPath, arrow_physical};
    use novarocks_proto_models::plan;

    fn valid_unpivot(literal: novarocks_proto_models::expr::Expr) -> plan::DistributedNode {
        let mut node = physical_node(
            20,
            plan::plan_node::Kind::Unpivot(plan::UnpivotNode {
                passthrough_columns: Vec::new(),
                value_output_column_id: 11,
                literal_output_column_ids: vec![10],
                value_mappings: vec![plan::UnpivotValueMapping {
                    input_value_column_id: 1,
                    constants: vec![plan::UnpivotConstant {
                        value: Some(plan::unpivot_constant::Value::ScalarLiteral(literal)),
                    }],
                }],
                max_output_rows: 1024,
                max_output_bytes: 1024 * 1024,
                output_schema: None,
            }),
            vec![
                output_column_with_nullable(10, "label", DataType::Utf8, false),
                output_column_with_nullable(11, "value", DataType::Int64, true),
            ],
            vec![one_col_values_node(10)],
        );
        install_exact_output_schema(
            &mut node,
            vec![
                Field::new("label", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ],
            &[10, 11],
        );
        node
    }

    fn install_exact_output_schema(
        node: &mut plan::DistributedNode,
        fields: Vec<Field>,
        slot_ids: &[u32],
    ) {
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
        else {
            panic!("physical node");
        };
        physical.output_columns.clear();
        let Some(plan::plan_node::Kind::Unpivot(unpivot)) = physical.kind.as_mut() else {
            panic!("Unpivot node");
        };
        let schema = Schema::new(fields);
        let (columns, schema_metadata) = arrow_physical::encode_schema(
            &schema,
            slot_ids,
            false,
            FieldPath::root("unpivot.output_schema"),
        )
        .expect("exact output schema");
        unpivot.output_schema = Some(plan::ArrowPhysicalSchema {
            columns,
            schema_metadata,
        });
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
        let Some(plan::plan_node::Kind::Unpivot(unpivot)) = physical.kind.as_mut() else {
            panic!("Unpivot node");
        };
        unpivot
            .output_schema
            .as_mut()
            .expect("output schema")
            .columns[1]
            .field
            .as_mut()
            .expect("field")
            .nullable = false;
        let mut arena = ExprArena::default();
        let error =
            decode_node(&node, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(
            error.contains("value output nullability mismatch"),
            "{error}"
        );
    }

    #[test]
    fn preserves_non_nullable_list_element_shape() {
        let mut node = physical_node(
            20,
            plan::plan_node::Kind::Unpivot(plan::UnpivotNode {
                passthrough_columns: Vec::new(),
                value_output_column_id: 11,
                literal_output_column_ids: vec![10],
                value_mappings: vec![plan::UnpivotValueMapping {
                    input_value_column_id: 1,
                    constants: vec![plan::UnpivotConstant {
                        value: Some(plan::unpivot_constant::Value::Int32List(plan::Int32List {
                            values: vec![1, 2],
                        })),
                    }],
                }],
                max_output_rows: 1024,
                max_output_bytes: 1024 * 1024,
                output_schema: None,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let list_type = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Int32,
            false,
        )));
        install_exact_output_schema(
            &mut node,
            vec![
                Field::new("field_ids", list_type.clone(), false),
                Field::new("value", DataType::Int64, true),
            ],
            &[10, 11],
        );

        let mut arena = ExprArena::default();
        let decoded = decode_node(&node, &mut arena, &NativePlanDecodeContext::default()).unwrap();
        let ExecNodeKind::Unpivot(unpivot) = decoded.node.kind else {
            panic!("expected Unpivot");
        };
        assert_eq!(
            unpivot
                .output_chunk_schema
                .slot(novarocks_types::SlotId::new(10))
                .expect("list output")
                .data_type(),
            &list_type
        );
    }
}
