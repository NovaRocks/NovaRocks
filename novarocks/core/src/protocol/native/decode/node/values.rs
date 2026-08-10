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

use arrow::array::{Array, ArrayRef};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use super::super::NativeFragmentDecodeError;
#[cfg(any(test, feature = "query-execution-contract-test-support"))]
use super::super::expr::decode_expr_at;
use super::super::layout::Layout;
use super::{DecodedNode, NativePlanDecodeContext};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::{ExprArena, cast_array_to_target};
use crate::exec::node::values::ValuesNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::protocol::common::error::FieldPath;
use novarocks_protocol::{common as proto_common, plan};

pub(super) fn lower_values_node(
    node: &plan::DistributedNode,
    physical: &plan::PlanNode,
    values: &plan::ValuesNode,
    path: FieldPath,
    physical_output_path: FieldPath,
    _children: Vec<DecodedNode>,
    arena: &mut ExprArena,
    ctx: &NativePlanDecodeContext,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    let columns = if values.columns.is_empty() {
        &physical.output_columns
    } else {
        &values.columns
    };
    let columns_path = if values.columns.is_empty() {
        physical_output_path
    } else {
        path.clone().field("columns")
    };
    let output_layout = ctx.decode_output_layout(columns, columns_path)?;
    let layout = Layout::for_slots(output_layout.slot_ids().iter().copied());
    let output_schema = output_layout.chunk_schema();
    let chunk = materialize_values_chunk_with_context(
        &values.rows,
        columns,
        output_schema.clone(),
        arena,
        path.clone(),
        Some(ctx),
    )?;
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Values(ValuesNode {
                chunk,
                node_id: node.node_id,
            }),
        },
        layout,
        output_schema,
    })
}

pub(super) fn materialize_values_chunk(
    rows: &[plan::ExprList],
    columns: &[proto_common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
    path: FieldPath,
) -> Result<Chunk, NativeFragmentDecodeError> {
    materialize_values_chunk_with_context(rows, columns, output_schema, arena, path, None)
}

pub(super) fn materialize_values_chunk_with_context(
    rows: &[plan::ExprList],
    columns: &[proto_common::OutputColumn],
    output_schema: ChunkSchemaRef,
    arena: &mut ExprArena,
    path: FieldPath,
    ctx: Option<&NativePlanDecodeContext>,
) -> Result<Chunk, NativeFragmentDecodeError> {
    if columns.is_empty() {
        return NativeFragmentDecodeError::map_invalid(
            path.field("rows"),
            empty_chunk_with_row_count(rows.len().max(1)),
        );
    }
    if rows.is_empty() {
        let batch = RecordBatch::new_empty(output_schema.arrow_schema_ref());
        return NativeFragmentDecodeError::map_invalid(
            path.field("rows"),
            Chunk::try_new_with_chunk_schema(batch, output_schema),
        );
    }
    let column_count = columns.len();
    if output_schema.slots().len() != column_count {
        return Err(NativeFragmentDecodeError::inconsistent(
            path.clone().field("columns"),
            format!(
                "ValuesNode output schema width mismatch: columns={}, schema_slots={}",
                column_count,
                output_schema.slots().len()
            ),
        ));
    }
    let target_types = output_schema
        .slots()
        .iter()
        .map(|slot| slot.data_type().clone())
        .collect::<Vec<_>>();
    let mut arrays_by_column = vec![Vec::<ArrayRef>::with_capacity(rows.len()); column_count];
    let input_layout = Layout::default();
    let one_row = NativeFragmentDecodeError::map_invalid(
        path.clone().field("rows"),
        empty_chunk_with_row_count(1),
    )?;

    for (row_idx, row) in rows.iter().enumerate() {
        if row.values.len() != column_count {
            return Err(NativeFragmentDecodeError::inconsistent(
                path.clone().field("rows").index(row_idx).field("values"),
                format!(
                    "ValuesNode row {row_idx} width mismatch: expected {column_count}, got {}",
                    row.values.len()
                ),
            ));
        }
        for (col_idx, expr) in row.values.iter().enumerate() {
            let expr_path = path
                .clone()
                .field("rows")
                .index(row_idx)
                .field("values")
                .index(col_idx);
            let expr_id = match ctx {
                Some(ctx) => ctx.decode_expression(expr, expr_path.clone(), arena, &input_layout),
                None => {
                    #[cfg(any(test, feature = "query-execution-contract-test-support"))]
                    {
                        decode_expr_at(expr, expr_path.clone(), arena, &input_layout)
                    }
                    #[cfg(not(any(test, feature = "query-execution-contract-test-support")))]
                    {
                        Err(NativeFragmentDecodeError::unsupported(
                            expr_path.clone(),
                            "native expression decoder must be supplied by the backend runtime",
                        ))
                    }
                }
            }?;
            let array = arena
                .eval(expr_id, &one_row)
                .map_err(|err| NativeFragmentDecodeError::invalid_value(expr_path.clone(), err))?;
            if array.len() != 1 {
                return Err(NativeFragmentDecodeError::inconsistent(
                    expr_path.clone(),
                    format!(
                        "ValuesNode row {row_idx} column {col_idx} evaluated to {} rows, expected 1",
                        array.len()
                    ),
                ));
            }
            let array = NativeFragmentDecodeError::map_invalid(
                expr_path,
                normalize_values_array(row_idx, col_idx, array, &target_types[col_idx]),
            )?;
            arrays_by_column[col_idx].push(array);
        }
    }

    let columns = arrays_by_column
        .into_iter()
        .enumerate()
        .map(|(col_idx, parts)| {
            let refs = parts
                .iter()
                .map(|part| part.as_ref() as &dyn Array)
                .collect::<Vec<_>>();
            concat(&refs).map_err(|err| format!("ValuesNode column {col_idx} concat failed: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| NativeFragmentDecodeError::invalid_value(path.clone().field("rows"), err))?;
    NativeFragmentDecodeError::map_invalid(
        path.field("rows"),
        Chunk::try_new_with_columns(output_schema, columns),
    )
}

fn normalize_values_array(
    row_idx: usize,
    col_idx: usize,
    array: ArrayRef,
    target_type: &DataType,
) -> Result<ArrayRef, String> {
    if array.data_type() == target_type || matches!(target_type, DataType::Null) {
        return Ok(array);
    }
    cast_array_to_target(&array, target_type).map_err(|err| {
        format!(
            "ValuesNode row {row_idx} column {col_idx} cast from {:?} to {:?} failed: {err}",
            array.data_type(),
            target_type
        )
    })
}

fn empty_chunk_with_row_count(row_count: usize) -> Result<Chunk, String> {
    let schema = Arc::new(Schema::empty());
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    let batch = RecordBatch::try_new_with_options(schema, Vec::new(), &options)
        .map_err(|err| format!("build empty values input chunk failed: {err}"))?;
    Chunk::try_new_with_chunk_schema(batch, Arc::new(ChunkSchema::empty()))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::DataType;

    use super::super::{NativePlanDecodeContext, decode_node};
    use super::*;
    use crate::exec::expr::ExprArena;
    use crate::protocol::native::type_mapping::encode_type;
    use novarocks_protocol::{common, expr, plan};
    use novarocks_types::SlotId;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn string_literal(value: &str) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Utf8)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            })),
        }
    }

    fn null_literal(data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            })),
        }
    }

    fn physical_node(
        node_id: i32,
        kind: plan::plan_node::Kind,
        output_columns: Vec<common::OutputColumn>,
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

    fn values_node(node_id: i32) -> plan::DistributedNode {
        let columns = vec![
            output_column(1, "id", DataType::Int64),
            output_column(2, "name", DataType::Utf8),
        ];
        physical_node(
            node_id,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![
                    plan::ExprList {
                        values: vec![int_literal(10), string_literal("alice")],
                    },
                    plan::ExprList {
                        values: vec![int_literal(20), string_literal("bob")],
                    },
                ],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        )
    }

    fn lower(node: &plan::DistributedNode) -> super::super::DecodedNode {
        let mut arena = ExprArena::default();
        decode_node(node, &mut arena, &NativePlanDecodeContext::default()).expect("lower node")
    }

    #[test]
    fn lowers_values_rows_into_chunk_schema() {
        let lowered = lower(&values_node(10));
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.node_id, 10);
        assert_eq!(values.chunk.len(), 2);
        assert_eq!(
            values.chunk.chunk_schema().slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(lowered.layout.order(), &[SlotId::new(1), SlotId::new(2)]);
        assert_eq!(
            lowered.output_schema.slot_ids(),
            &[SlotId::new(1), SlotId::new(2)]
        );

        let id_column = values
            .chunk
            .column_by_slot_id(SlotId::new(1))
            .expect("id column");
        let id = id_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 id");
        assert_eq!(id.value(0), 10);
        assert_eq!(id.value(1), 20);

        let name_column = values
            .chunk
            .column_by_slot_id(SlotId::new(2))
            .expect("name column");
        let name = name_column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 name");
        assert_eq!(name.value(0), "alice");
        assert_eq!(name.value(1), "bob");
    }

    #[test]
    fn values_casts_null_rows_to_declared_column_type_before_concat() {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        let node = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![
                    plan::ExprList {
                        values: vec![int_literal(10)],
                    },
                    plan::ExprList {
                        values: vec![null_literal(DataType::Null)],
                    },
                ],
                columns: columns.clone(),
            }),
            columns,
            Vec::new(),
        );

        let lowered = lower(&node);
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        let id_column = values
            .chunk
            .column_by_slot_id(SlotId::new(1))
            .expect("id column");
        assert_eq!(id_column.data_type(), &DataType::Int64);
        let id = id_column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 id");
        assert_eq!(id.value(0), 10);
        assert!(id.is_null(1));
    }

    #[test]
    fn lowers_zero_column_values_rows_as_seed_rows() {
        let node = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![plan::ExprList { values: vec![] }],
                columns: vec![],
            }),
            vec![],
            Vec::new(),
        );

        let lowered = lower(&node);
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.chunk.len(), 1);
        assert!(values.chunk.chunk_schema().slot_ids().is_empty());
        assert!(lowered.layout.order().is_empty());
        assert!(lowered.output_schema.slot_ids().is_empty());
    }

    #[test]
    fn lowers_empty_zero_column_values_as_single_seed_row() {
        let node = physical_node(
            10,
            plan::plan_node::Kind::Values(plan::ValuesNode {
                rows: vec![],
                columns: vec![],
            }),
            vec![],
            Vec::new(),
        );

        let lowered = lower(&node);
        let ExecNodeKind::Values(values) = lowered.node.kind else {
            panic!("expected Values");
        };
        assert_eq!(values.chunk.len(), 1);
        assert!(values.chunk.chunk_schema().slot_ids().is_empty());
        assert!(lowered.layout.order().is_empty());
        assert!(lowered.output_schema.slot_ids().is_empty());
    }
}
