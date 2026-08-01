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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

use super::values::materialize_values_chunk_with_context;
use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::common::ids::SlotId;
use novarocks::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use novarocks::exec::expr::ExprArena;
use novarocks::exec::node::table_function::{TableFunctionNode, TableFunctionOutputSlot};
use novarocks::exec::node::values::ValuesNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::{common as proto_common, expr, plan};
use novarocks::protocol::common::error::FieldPath;

pub(super) fn lower_generate_series_node(
    node: &plan::DistributedNode,
    generate_series: &plan::GenerateSeriesNode,
    path: FieldPath,
    _children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    if generate_series.step == 0 {
        return Err(NativeFragmentDecodeError::invalid_value(
            path.clone().field("step"),
            "GenerateSeriesNode step must not be zero",
        ));
    }

    let param_slots = NativeFragmentDecodeError::map_invalid(
        path.clone().field("output_column_id"),
        generate_series_param_slots(generate_series.output_column_id),
    )?;
    let param_columns = vec![
        bigint_output_column(param_slots[0].as_u32(), "generate_series_start", false),
        bigint_output_column(param_slots[1].as_u32(), "generate_series_end", false),
        bigint_output_column(param_slots[2].as_u32(), "generate_series_step", false),
    ];
    let input_schema = int64_chunk_schema(
        &[
            (param_slots[0], "generate_series_start"),
            (param_slots[1], "generate_series_end"),
            (param_slots[2], "generate_series_step"),
        ],
        path.clone().field("output_column_id"),
    )?;
    let rows = vec![plan::ExprList {
        values: vec![
            int64_literal_expr(generate_series.start),
            int64_literal_expr(generate_series.end),
            int64_literal_expr(generate_series.step),
        ],
    }];
    let input_chunk = materialize_values_chunk_with_context(
        &rows,
        &param_columns,
        input_schema,
        arena,
        path.clone(),
        Some(ctx),
    )?;

    let output_columns = vec![bigint_output_column(
        generate_series.output_column_id,
        if generate_series.column_name.is_empty() {
            "generate_series"
        } else {
            &generate_series.column_name
        },
        false,
    )];
    let output_slot = SlotId::new(generate_series.output_column_id);
    let layout = Layout::for_slots([output_slot]);
    let output_schema = int64_chunk_schema(
        &[(output_slot, output_columns[0].name.as_str())],
        path.clone().field("output_column_id"),
    )?;

    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::TableFunction(TableFunctionNode {
                input: Box::new(ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: input_chunk,
                        node_id: node.node_id,
                    }),
                }),
                node_id: node.node_id,
                function_name: "generate_series".to_string(),
                param_slots: param_slots.to_vec(),
                outer_slots: Vec::new(),
                fn_result_slots: vec![SlotId::new(generate_series.output_column_id)],
                fn_result_required: true,
                is_left_join: false,
                param_types: vec![DataType::Int64, DataType::Int64, DataType::Int64],
                ret_types: vec![DataType::Int64],
                output_chunk_schema: output_schema.clone(),
                output_slot_sources: vec![TableFunctionOutputSlot::Result { index: 0 }],
            }),
        },
        layout,
        output_schema,
    })
}

fn int64_chunk_schema(
    slots: &[(SlotId, &str)],
    source_path: FieldPath,
) -> Result<ChunkSchemaRef, NativeFragmentDecodeError> {
    let slots = slots
        .iter()
        .map(|(slot_id, name)| {
            ChunkSlotSchema::try_new_with_field(
                *slot_id,
                Field::new(*name, DataType::Int64, false),
                None,
                None,
            )
        })
        .collect::<Result<Vec<_>, _>>();
    let slots = NativeFragmentDecodeError::map_invalid(source_path.clone(), slots)?;
    NativeFragmentDecodeError::map_invalid(source_path, ChunkSchema::try_new(slots)).map(Arc::new)
}

fn generate_series_param_slots(output_column_id: u32) -> Result<[SlotId; 3], String> {
    let mut slot = u32::MAX;
    let mut slots = Vec::with_capacity(3);
    while slots.len() < 3 {
        if slot != output_column_id {
            slots.push(SlotId::new(slot));
        }
        slot = slot
            .checked_sub(1)
            .ok_or_else(|| "GenerateSeriesNode could not allocate internal slots".to_string())?;
    }
    Ok([slots[0], slots[1], slots[2]])
}

fn bigint_output_column(column_id: u32, name: &str, nullable: bool) -> proto_common::OutputColumn {
    proto_common::OutputColumn {
        column_id,
        name: name.to_string(),
        r#type: Some(bigint_type_desc()),
        nullable,
        is_internal: false,
    }
}

fn bigint_type_desc() -> proto_common::TypeDesc {
    proto_common::TypeDesc {
        kind: Some(proto_common::type_desc::Kind::Scalar(
            proto_common::ScalarType {
                r#type: proto_common::PrimitiveType::Bigint as i32,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            },
        )),
    }
}

fn int64_literal_expr(value: i64) -> expr::Expr {
    expr::Expr {
        r#type: Some(bigint_type_desc()),
        nullable: false,
        kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
            value: Some(proto_common::LiteralValue {
                value: Some(proto_common::literal_value::Value::IntValue(value)),
            }),
        })),
    }
}
