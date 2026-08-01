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

use super::{DecodedNode, NativePlanDecodeContext};
use crate::native::plan_decode::error::NativeFragmentDecodeError;
use crate::native::plan_decode::layout::Layout;
use novarocks::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use novarocks::exec::expr::{ExprArena, cast_array_to_target};
use novarocks::exec::node::values::ValuesNode;
use novarocks::exec::node::{ExecNode, ExecNodeKind};
use novarocks::proto::{common as proto_common, plan};
use novarocks::protocol::common::error::FieldPath;

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
                None => Err(NativeFragmentDecodeError::unsupported(
                    expr_path.clone(),
                    "native expression decoder must be supplied by the backend runtime",
                )),
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
