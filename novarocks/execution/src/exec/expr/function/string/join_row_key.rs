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

use arrow::array::{Array, ArrayRef, Int64Array, LargeStringArray, StringArray};
use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::mv::stable_join_row_key;

enum StringInput<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
}

impl StringInput<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Utf8(array) => array.is_null(row),
            Self::LargeUtf8(array) => array.is_null(row),
        }
    }

    fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(array) => array.value(row),
            Self::LargeUtf8(array) => array.value(row),
        }
    }
}

pub fn eval_join_row_key(
    arena: &ExprArena,
    _expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let left_uuid_array = arena.eval(args[0], chunk)?;
    let left_row_id_array = arena.eval(args[1], chunk)?;
    let right_uuid_array = arena.eval(args[2], chunk)?;
    let right_row_id_array = arena.eval(args[3], chunk)?;

    let left_uuid = string_input(&left_uuid_array, "left_uuid")?;
    let left_row_id = int64_input(&left_row_id_array, "left_row_id")?;
    let right_uuid = string_input(&right_uuid_array, "right_uuid")?;
    let right_row_id = int64_input(&right_row_id_array, "right_row_id")?;

    let mut values = Vec::with_capacity(chunk.len());
    for row in 0..chunk.len() {
        if left_uuid.is_null(row)
            || left_row_id.is_null(row)
            || right_uuid.is_null(row)
            || right_row_id.is_null(row)
        {
            values.push(None);
            continue;
        }
        values.push(Some(stable_join_row_key(
            left_uuid.value(row),
            left_row_id.value(row),
            right_uuid.value(row),
            right_row_id.value(row),
        )));
    }

    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

fn string_input<'a>(array: &'a ArrayRef, arg_name: &str) -> Result<StringInput<'a>, String> {
    match array.data_type() {
        DataType::Utf8 => Ok(StringInput::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| format!("join_row_key: failed to downcast {arg_name} to Utf8"))?,
        )),
        DataType::LargeUtf8 => Ok(StringInput::LargeUtf8(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    format!("join_row_key: failed to downcast {arg_name} to LargeUtf8")
                })?,
        )),
        other => Err(format!(
            "join_row_key expects {arg_name} to be VARCHAR, got {other:?}"
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

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::function::lookup_function;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue};
    use crate::exec::mv::stable_join_row_key;
    use novarocks_types::SlotId;

    fn chunk_len_1() -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dummy",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn join_row_key_scalar_matches_stable_join_row_key() {
        let mut arena = ExprArena::default();
        let left_uuid = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("lu".to_string())),
            DataType::Utf8,
        );
        let left_row_id =
            arena.push_typed(ExprNode::Literal(LiteralValue::Int64(1)), DataType::Int64);
        let right_uuid = arena.push_typed(
            ExprNode::Literal(LiteralValue::Utf8("ru".to_string())),
            DataType::Utf8,
        );
        let right_row_id =
            arena.push_typed(ExprNode::Literal(LiteralValue::Int64(2)), DataType::Int64);
        let kind = lookup_function("join_row_key").expect("join_row_key must be registered");
        let expr = arena.push_typed(
            ExprNode::FunctionCall {
                kind,
                args: vec![left_uuid, left_row_id, right_uuid, right_row_id],
            },
            DataType::Utf8,
        );

        let out = arena.eval(expr, &chunk_len_1()).expect("join_row_key eval");
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(
            out.value(0),
            stable_join_row_key("lu", 1, "ru", 2),
            "join_row_key SQL scalar must preserve stable join row identity"
        );
    }
}
