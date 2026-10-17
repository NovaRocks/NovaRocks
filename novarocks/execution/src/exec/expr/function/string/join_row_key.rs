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

use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, StringArray};
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::mv::stable_join_row_key;

enum BinaryInput<'a> {
    Binary(&'a BinaryArray),
    LargeBinary(&'a LargeBinaryArray),
}

impl BinaryInput<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Binary(array) => array.is_null(row),
            Self::LargeBinary(array) => array.is_null(row),
        }
    }

    fn value(&self, row: usize) -> &[u8] {
        match self {
            Self::Binary(array) => array.value(row),
            Self::LargeBinary(array) => array.value(row),
        }
    }
}

pub fn eval_join_row_key<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let left_uuid_array = arena.eval(args[0], chunk)?;
    let left_row_id_array = arena.eval(args[1], chunk)?;
    let right_uuid_array = arena.eval(args[2], chunk)?;
    let right_row_id_array = arena.eval(args[3], chunk)?;

    let left_object_id = binary_input(&left_uuid_array, "left_object_id")?;
    let left_row_id = int64_input(&left_row_id_array, "left_row_id")?;
    let right_object_id = binary_input(&right_uuid_array, "right_object_id")?;
    let right_row_id = int64_input(&right_row_id_array, "right_row_id")?;

    let mut values = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left_object_id.is_null(row)
            || left_row_id.is_null(row)
            || right_object_id.is_null(row)
            || right_row_id.is_null(row)
        {
            values.push(None);
            continue;
        }
        values.push(Some(stable_join_row_key(
            left_object_id.value(row),
            left_row_id.value(row),
            right_object_id.value(row),
            right_row_id.value(row),
        )));
    }

    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

fn binary_input<'a>(array: &'a ArrayRef, arg_name: &str) -> Result<BinaryInput<'a>, String> {
    match array.data_type() {
        DataType::Binary => Ok(BinaryInput::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| format!("join_row_key: failed to downcast {arg_name} to Binary"))?,
        )),
        DataType::LargeBinary => Ok(BinaryInput::LargeBinary(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    format!("join_row_key: failed to downcast {arg_name} to LargeBinary")
                })?,
        )),
        other => Err(format!(
            "join_row_key expects {arg_name} to be BINARY or LARGE_BINARY, got {other:?}"
        )),
    }
}

fn int64_input<'a>(array: &'a ArrayRef, arg_name: &str) -> Result<&'a Int64Array, String> {
    array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        format!(
            "join_row_key expects {arg_name} to be BIGINT, got {:?}",
            array.data_type()
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BinaryArray, LargeBinaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::function::lookup_function;
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::mv::stable_join_row_key;
    use novarocks_types::SlotId;

    fn chunk_with_object_ids() -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new("left_object_id", DataType::Binary, true),
            Field::new("left_row_id", DataType::Int64, true),
            Field::new("right_object_id", DataType::LargeBinary, true),
            Field::new("right_row_id", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from(vec![Some(&b"\xff\0left"[..]), None])),
                Arc::new(arrow::array::Int64Array::from(vec![Some(1), Some(1)])),
                Arc::new(LargeBinaryArray::from(vec![
                    Some(&b"right\0\x80"[..]),
                    Some(&b"right\0\x80"[..]),
                ])),
                Arc::new(arrow::array::Int64Array::from(vec![Some(2), Some(2)])),
            ],
        )
        .unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[
                SlotId::new(1),
                SlotId::new(2),
                SlotId::new(3),
                SlotId::new(4),
            ],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn join_row_key_scalar_accepts_binary_and_large_binary_and_propagates_nulls() {
        let mut arena = ExprArena::default();
        let left_object_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Binary);
        let left_row_id = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Int64);
        let right_object_id =
            arena.push_typed(ExprNode::SlotId(SlotId::new(3)), DataType::LargeBinary);
        let right_row_id = arena.push_typed(ExprNode::SlotId(SlotId::new(4)), DataType::Int64);
        let kind = lookup_function("join_row_key").expect("join_row_key must be registered");
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind,
                args: vec![left_object_id, left_row_id, right_object_id, right_row_id],
            },
            DataType::Utf8,
        );

        let out = arena
            .eval(expr, &chunk_with_object_ids())
            .expect("join_row_key eval");
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(
            out.value(0),
            stable_join_row_key(b"\xff\0left", 1, b"right\0\x80", 2),
            "join_row_key scalar must preserve binary object identity"
        );
        assert!(out.is_null(1), "join_row_key must propagate null inputs");
    }
}
