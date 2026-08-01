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

use super::super::NativeFragmentDecodeError;
use super::super::layout::Layout;
use super::values::materialize_values_chunk_with_context;
use super::{DecodedNode, NativePlanDecodeContext};
use crate::common::ids::SlotId;
use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
use crate::exec::expr::ExprArena;
use crate::exec::node::table_function::{TableFunctionNode, TableFunctionOutputSlot};
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::{common as proto_common, expr, plan};
use crate::protocol::common::error::FieldPath;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;

    use super::super::{NativePlanDecodeContext, decode_node};
    use super::*;
    use crate::exec::expr::ExprArena;
    use crate::protocol::native::test_assembly::{
        NativeExpressionDecoder, NativeExpressionInputLayout,
    };
    use crate::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};

    struct RejectingBackendExpressionDecoder;

    impl NativeExpressionDecoder for RejectingBackendExpressionDecoder {
        fn decode_expression(
            &self,
            _expression: &expr::Expr,
            path: FieldPath,
            _arena: &mut ExprArena,
            _input: &NativeExpressionInputLayout,
        ) -> Result<crate::exec::expr::ExprId, ProtocolError> {
            Err(ProtocolError::new(
                ProtocolFamily::Native,
                path,
                ProtocolErrorKind::InvalidValue,
                "backend expression decoder invoked",
            ))
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<proto_common::OutputColumn>,
        children: Vec<plan::DistributedNode>,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id,
            fragment_id: 1,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(kind),
            })),
        }
    }

    fn one_col_values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![bigint_output_column(1, "id", true)];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList {
                    values: vec![int64_literal_expr(10)],
                }],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn lower(node: &plan::DistributedNode) -> DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_generate_series_to_table_function_exec_node() {
        let node = physical_node(
            20,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 5,
                step: 2,
                column_name: "x".to_string(),
                alias: Some("gs".to_string()),
                output_column_id: 9,
            }),
            Vec::new(),
            Vec::new(),
        );

        let lowered = lower(&node);
        let ExecNodeKind::TableFunction(table_function) = lowered.node.kind else {
            panic!("expected TableFunction");
        };
        assert_eq!(table_function.node_id, 20);
        assert_eq!(table_function.function_name, "generate_series");
        assert_eq!(table_function.param_slots.len(), 3);
        assert!(table_function.outer_slots.is_empty());
        assert_eq!(table_function.fn_result_slots, vec![SlotId::new(9)]);
        assert!(table_function.fn_result_required);
        assert!(!table_function.is_left_join);
        assert_eq!(
            table_function.param_types,
            vec![DataType::Int64, DataType::Int64, DataType::Int64]
        );
        assert_eq!(table_function.ret_types, vec![DataType::Int64]);
        assert_eq!(
            table_function.output_chunk_schema.slot_ids(),
            &[SlotId::new(9)]
        );
        assert_eq!(
            table_function.output_chunk_schema.field(0).unwrap().name(),
            "x"
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(9)]);
        assert_eq!(lowered.output_schema.slot_ids(), &[SlotId::new(9)]);
        assert!(matches!(
            table_function.output_slot_sources.as_slice(),
            [TableFunctionOutputSlot::Result { index: 0 }]
        ));

        let ExecNodeKind::Values(input) = table_function.input.kind else {
            panic!("expected synthetic Values input");
        };
        assert_eq!(input.chunk.len(), 1);
        assert_eq!(input.chunk.chunk_schema().slot_ids().len(), 3);
        for (slot, expected) in table_function.param_slots.iter().zip([1, 5, 2]) {
            let column = input.chunk.column_by_slot_id(*slot).expect("param column");
            let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.value(0), expected);
        }
    }

    #[test]
    fn generate_series_uses_backend_expression_decoder_for_synthetic_values() {
        let node = physical_node(
            20,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 5,
                step: 2,
                column_name: "x".to_string(),
                alias: None,
                output_column_id: 9,
            }),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let context = NativePlanDecodeContext::default()
            .with_expression_decoder(Arc::new(RejectingBackendExpressionDecoder));

        let err = decode_node(&node, &mut arena, &context).expect_err("decoder must be invoked");
        assert!(err.contains("backend expression decoder invoked"), "{err}");
    }

    #[test]
    fn generate_series_rejects_zero_step_and_children() {
        let zero_step = physical_node(
            20,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 5,
                step: 0,
                column_name: "x".to_string(),
                alias: None,
                output_column_id: 9,
            }),
            Vec::new(),
            Vec::new(),
        );
        let mut arena = ExprArena::default();
        let err =
            decode_node(&zero_step, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(err.contains("step must not be zero"), "{err}");

        let with_child = physical_node(
            21,
            plan::plan_node::Kind::GenerateSeries(plan::GenerateSeriesNode {
                start: 1,
                end: 5,
                step: 1,
                column_name: "x".to_string(),
                alias: None,
                output_column_id: 9,
            }),
            Vec::new(),
            vec![one_col_values_node(10)],
        );
        let err =
            decode_node(&with_child, &mut arena, &NativePlanDecodeContext::default()).unwrap_err();
        assert!(
            err.contains("GenerateSeriesNode expected 0 children"),
            "{err}"
        );
    }
}
