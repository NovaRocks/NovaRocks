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

use arrow::datatypes::DataType;

use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use novarocks::exec::expr::{ExprArena, ExprNode};
use novarocks::exec::node::project::ProjectNode;
use novarocks::exec::node::table_function::{TableFunctionNode, TableFunctionOutputSlot};
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::{common as proto_common, expr, plan};
use novarocks::protocol::common::error::FieldPath;

pub(super) fn lower_table_function_node(
    node: &plan::DistributedNode,
    table_function: &plan::TableFunctionNode,
    path: FieldPath,
    mut children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let child = children.pop().expect("child");
    validate_table_function_signature(table_function, path.clone())?;

    let param_slots = table_function_param_slots(
        &child.layout,
        &table_function.output_columns,
        &table_function.args,
        path.clone(),
    )?;
    let (param_types, param_slot_schemas) =
        table_function_param_schemas(table_function, &param_slots, path.clone())?;
    let result_slot_schemas = ctx
        .decode_output_layout(
            &table_function.output_columns,
            path.clone().field("output_columns"),
        )?
        .slot_schemas()
        .to_vec();
    let ret_types = table_function_result_types(table_function, path.clone())?;

    let mut project_exprs = Vec::with_capacity(child.layout.order().len() + param_slots.len());
    let mut project_slot_ids = Vec::with_capacity(project_exprs.capacity());
    let mut project_slot_schemas =
        Vec::with_capacity(child.output_schema.slots().len() + param_slot_schemas.len());
    for slot_schema in child.output_schema.slots() {
        let slot_id = slot_schema.slot_id();
        project_exprs
            .push(arena.push_typed(ExprNode::SlotId(slot_id), slot_schema.data_type().clone()));
        project_slot_ids.push(slot_id);
        project_slot_schemas.push(slot_schema.clone());
    }
    for ((idx, arg), slot_schema) in table_function
        .args
        .iter()
        .enumerate()
        .zip(param_slot_schemas.iter())
    {
        let expr = ctx.decode_expression(
            arg,
            path.clone().field("args").index(idx),
            arena,
            &child.layout,
        )?;
        project_exprs.push(expr);
        project_slot_ids.push(slot_schema.slot_id());
        project_slot_schemas.push(slot_schema.clone());
    }
    let project_output_schema = Arc::new(NativeFragmentDecodeError::map_invalid(
        path.clone().field("args"),
        ChunkSchema::try_new(project_slot_schemas),
    )?);

    let mut output_slot_schemas =
        Vec::with_capacity(child.output_schema.slots().len() + result_slot_schemas.len());
    let mut output_slot_sources =
        Vec::with_capacity(child.output_schema.slots().len() + result_slot_schemas.len());
    let mut outer_slots = Vec::with_capacity(child.output_schema.slots().len());
    for slot_schema in child.output_schema.slots() {
        let slot_id = slot_schema.slot_id();
        outer_slots.push(slot_id);
        output_slot_schemas.push(slot_schema.clone());
        output_slot_sources.push(TableFunctionOutputSlot::Outer { slot: slot_id });
    }
    let mut fn_result_slots = Vec::with_capacity(result_slot_schemas.len());
    for (idx, slot_schema) in result_slot_schemas.iter().enumerate() {
        let slot_id = slot_schema.slot_id();
        fn_result_slots.push(slot_id);
        output_slot_schemas.push(slot_schema.clone());
        output_slot_sources.push(TableFunctionOutputSlot::Result { index: idx });
    }
    let output_schema = Arc::new(NativeFragmentDecodeError::map_invalid(
        path.clone().field("output_columns"),
        ChunkSchema::try_new(output_slot_schemas),
    )?);
    let layout = Layout::for_slots(output_schema.slot_ids().iter().copied());

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFunction(TableFunctionNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Project(ProjectNode {
                        input: Box::new(child.node),
                        node_id: node.node_id,
                        is_subordinate: true,
                        exprs: project_exprs,
                        expr_slot_ids: project_slot_ids,
                        expr_slot_schemas: Some(project_output_schema.slots().to_vec()),
                        output_indices: None,
                        output_chunk_schema: project_output_schema,
                    }),
                }),
                node_id: node.node_id,
                function_name: table_function.function_name.clone(),
                param_slots,
                outer_slots,
                fn_result_slots,
                fn_result_required: true,
                is_left_join: table_function.is_left_join,
                param_types,
                ret_types,
                output_chunk_schema: output_schema.clone(),
                output_slot_sources,
            }),
        },
        layout,
        output_schema,
    })
}

fn validate_table_function_signature(
    table_function: &plan::TableFunctionNode,
    path: FieldPath,
) -> Result<(), NativeFragmentDecodeError> {
    let function_name = table_function.function_name.to_ascii_lowercase();
    let param_types = table_function_arg_types(table_function, path.clone())?;
    let ret_types = table_function_result_types(table_function, path.clone())?;
    match function_name.as_str() {
        "unnest" => NativeFragmentDecodeError::map_invalid(
            path,
            validate_unnest_table_function(&param_types, &ret_types),
        ),
        "unnest_bitmap" => {
            NativeFragmentDecodeError::map_invalid(
                path.clone(),
                validate_table_function_arity("unnest_bitmap", &param_types, &ret_types, 1, 1),
            )?;
            if !matches!(param_types.first(), Some(DataType::Binary)) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("args").index(0).field("type"),
                    format!(
                        "table function unnest_bitmap param 0 expects Binary, got {:?}",
                        param_types.first()
                    ),
                ));
            }
            if !matches!(ret_types.first(), Some(DataType::Int64)) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("output_columns").index(0).field("type"),
                    format!(
                        "table function unnest_bitmap return type expects Int64, got {:?}",
                        ret_types.first()
                    ),
                ));
            }
            Ok(())
        }
        "subdivide_bitmap" => {
            NativeFragmentDecodeError::map_invalid(
                path.clone(),
                validate_table_function_arity("subdivide_bitmap", &param_types, &ret_types, 2, 1),
            )?;
            if !matches!(param_types.first(), Some(DataType::Binary)) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("args").index(0).field("type"),
                    format!(
                        "table function subdivide_bitmap param 0 expects Binary, got {:?}",
                        param_types.first()
                    ),
                ));
            }
            if !matches!(ret_types.first(), Some(DataType::Binary)) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("output_columns").index(0).field("type"),
                    format!(
                        "table function subdivide_bitmap return type expects Binary, got {:?}",
                        ret_types.first()
                    ),
                ));
            }
            Ok(())
        }
        "generate_series" => {
            if !(param_types.len() == 2 || param_types.len() == 3) || ret_types.len() != 1 {
                return Err(NativeFragmentDecodeError::inconsistent(
                    path.clone(),
                    format!(
                        "table function generate_series expects 2 or 3 args and 1 output, got args={} outputs={}",
                        param_types.len(),
                        ret_types.len()
                    ),
                ));
            }
            if !ret_types.iter().all(is_table_function_integer_type) {
                return Err(NativeFragmentDecodeError::invalid_value(
                    path.clone().field("output_columns").index(0).field("type"),
                    format!(
                        "table function generate_series return type expects integer, got {:?}",
                        ret_types.first()
                    ),
                ));
            }
            for (idx, param_type) in param_types.iter().enumerate() {
                if !is_table_function_integer_type(param_type) {
                    return Err(NativeFragmentDecodeError::invalid_value(
                        path.clone().field("args").index(idx).field("type"),
                        format!(
                            "table function generate_series param {idx} expects integer, got {param_type:?}"
                        ),
                    ));
                }
            }
            Ok(())
        }
        _ => Err(NativeFragmentDecodeError::unsupported(
            path.field("function_name"),
            format!(
                "unsupported native table function: {}",
                table_function.function_name
            ),
        )),
    }
}

fn validate_unnest_table_function(
    param_types: &[DataType],
    ret_types: &[DataType],
) -> Result<(), String> {
    if param_types.is_empty() {
        return Err("table function unnest requires at least one argument".to_string());
    }
    if param_types.len() != ret_types.len() {
        return Err(format!(
            "table function unnest output column count mismatch: args={} outputs={}",
            param_types.len(),
            ret_types.len()
        ));
    }
    for (idx, (param_type, ret_type)) in param_types.iter().zip(ret_types.iter()).enumerate() {
        let DataType::List(item_field) = param_type else {
            return Err(format!(
                "table function unnest param {idx} expects List, got {param_type:?}"
            ));
        };
        if item_field.data_type() != ret_type {
            return Err(format!(
                "table function unnest result type mismatch for param {idx}: item={:?} output={:?}",
                item_field.data_type(),
                ret_type
            ));
        }
    }
    Ok(())
}

fn validate_table_function_arity(
    name: &str,
    param_types: &[DataType],
    ret_types: &[DataType],
    expected_params: usize,
    expected_results: usize,
) -> Result<(), String> {
    if param_types.len() != expected_params || ret_types.len() != expected_results {
        return Err(format!(
            "table function {name} expects {expected_params} args and {expected_results} outputs, got args={} outputs={}",
            param_types.len(),
            ret_types.len()
        ));
    }
    Ok(())
}

fn is_table_function_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn table_function_arg_types(
    table_function: &plan::TableFunctionNode,
    path: FieldPath,
) -> Result<Vec<DataType>, NativeFragmentDecodeError> {
    table_function
        .args
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            let type_desc = arg.r#type.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    path.clone().field("args").index(idx).field("type"),
                    format!("TableFunctionNode arg {idx} type missing"),
                )
            })?;
            NativeFragmentDecodeError::map_invalid(
                path.clone().field("args").index(idx).field("type"),
                crate::native::type_decode::decode_type(type_desc),
            )
        })
        .collect()
}

fn table_function_result_types(
    table_function: &plan::TableFunctionNode,
    path: FieldPath,
) -> Result<Vec<DataType>, NativeFragmentDecodeError> {
    table_function
        .output_columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            let column_path = path.clone().field("output_columns").index(idx);
            let type_desc = column.r#type.as_ref().ok_or_else(|| {
                NativeFragmentDecodeError::missing(
                    column_path.clone().field("type"),
                    format!(
                        "TableFunctionNode output column {} '{}' type missing",
                        idx, column.name
                    ),
                )
            })?;
            NativeFragmentDecodeError::map_invalid(
                column_path.field("type"),
                crate::native::type_decode::decode_type(type_desc),
            )
        })
        .collect()
}

fn table_function_param_schemas(
    table_function: &plan::TableFunctionNode,
    param_slots: &[SlotId],
    path: FieldPath,
) -> Result<(Vec<DataType>, Vec<ChunkSlotSchema>), NativeFragmentDecodeError> {
    let mut param_types = Vec::with_capacity(table_function.args.len());
    let mut slot_schemas = Vec::with_capacity(table_function.args.len());
    for (idx, (arg, slot_id)) in table_function
        .args
        .iter()
        .zip(param_slots.iter())
        .enumerate()
    {
        let arg_path = path.clone().field("args").index(idx);
        let type_desc = arg.r#type.as_ref().ok_or_else(|| {
            NativeFragmentDecodeError::missing(
                arg_path.clone().field("type"),
                format!("TableFunctionNode arg {idx} type missing"),
            )
        })?;
        let data_type = NativeFragmentDecodeError::map_invalid(
            arg_path.clone().field("type"),
            crate::native::type_decode::decode_type(type_desc),
        )?;
        let field = crate::native::type_decode::decode_field_type(
            &format!("__tf_arg_{idx}"),
            arg.nullable,
            type_desc,
        )
        .map_err(|err| {
            NativeFragmentDecodeError::invalid_value(arg_path.clone().field("type"), err)
        })?;
        slot_schemas.push(NativeFragmentDecodeError::map_invalid(
            arg_path.field("type"),
            ChunkSchema::slot_schema_from_arrow_field(*slot_id, &field),
        )?);
        param_types.push(data_type);
    }
    Ok((param_types, slot_schemas))
}

fn table_function_param_slots(
    input_layout: &Layout,
    output_columns: &[proto_common::OutputColumn],
    args: &[expr::Expr],
    path: FieldPath,
) -> Result<Vec<SlotId>, NativeFragmentDecodeError> {
    let mut used = input_layout
        .order()
        .iter()
        .map(|slot| slot.as_u32())
        .collect::<HashSet<_>>();
    used.extend(output_columns.iter().map(|column| column.column_id));
    let mut slot = u32::MAX;
    let mut slots = Vec::with_capacity(args.len());
    while slots.len() < args.len() {
        if used.insert(slot) {
            slots.push(SlotId::new(slot));
        }
        slot = slot.checked_sub(1).ok_or_else(|| {
            NativeFragmentDecodeError::out_of_range(
                path.clone().field("args"),
                "TableFunctionNode could not allocate internal slots",
            )
        })?;
    }
    Ok(slots)
}
