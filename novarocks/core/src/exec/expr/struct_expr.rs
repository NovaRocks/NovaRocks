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
use crate::exec::chunk::Chunk;
use crate::exec::chunk::type_compatibility::{check_exact, nested_path_label, retag_column};
use crate::exec::expr::{ExprArena, ExprId, cast_with_special_rules};
use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

fn assert_struct_child_type(
    context: &str,
    idx: usize,
    expected: &DataType,
    actual: &DataType,
) -> Result<(), String> {
    if expected == actual {
        return Ok(());
    }
    let path = match check_exact(expected, actual) {
        Ok(()) => format!("field[{idx}]"),
        Err(mismatch) => nested_path_label(&format!("field[{idx}]"), &mismatch.nested_path),
    };
    Err(format!(
        "{context} field type mismatch at {path}: expected {:?}, got {:?}",
        expected, actual
    ))
}

pub fn eval_struct_expr(
    arena: &ExprArena,
    id: ExprId,
    fields: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, String> {
    let output_type = arena
        .data_type(id)
        .cloned()
        .ok_or_else(|| "struct_expr missing output type".to_string())?;
    let struct_fields = match output_type {
        DataType::Struct(fields) => fields,
        other => {
            return Err(format!(
                "struct_expr output type must be Struct, got {:?}",
                other
            ));
        }
    };
    if struct_fields.len() != fields.len() {
        return Err(format!(
            "struct_expr field count mismatch: expected {}, got {}",
            struct_fields.len(),
            fields.len()
        ));
    }

    let num_rows = chunk.len();
    let mut arrays = Vec::with_capacity(fields.len());
    for (idx, expr_id) in fields.iter().enumerate() {
        let mut array = arena.eval(*expr_id, chunk)?;
        if array.len() != num_rows {
            return Err(format!(
                "struct_expr field length mismatch at {}: expected {}, got {}",
                idx,
                num_rows,
                array.len()
            ));
        }
        let expected_type = struct_fields
            .get(idx)
            .map(|f| f.data_type())
            .ok_or_else(|| "struct_expr field missing".to_string())?;
        if array.data_type() != expected_type {
            array = match retag_column(&array, expected_type) {
                Ok(retagged) => retagged,
                Err(_) => cast_with_special_rules(&array, expected_type).map_err(|e| {
                    format!(
                        "struct_expr field cast failed at field[{idx}] from {:?} to {:?}: {}",
                        array.data_type(),
                        expected_type,
                        e
                    )
                })?,
            };
        }
        assert_struct_child_type("struct_expr", idx, expected_type, array.data_type())?;
        arrays.push(array);
    }

    let array = StructArray::new(struct_fields, arrays, None);
    Ok(Arc::new(array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::expr::ExprNode;
    use arrow::array::{Array, Int32Array, Int64Array, ListArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    #[test]
    fn struct_expr_casts_field_to_declared_type() {
        let input_type = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let output_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let input = Arc::new(ListArray::new(
            match &input_type {
                DataType::List(field) => field.clone(),
                _ => unreachable!(),
            },
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            None::<NullBuffer>,
        )) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            input_type.clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let slot = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), input_type);
        let expr = arena.push_typed(
            ExprNode::StructExpr { fields: vec![slot] },
            DataType::Struct(Fields::from(vec![Field::new("col1", output_type, true)])),
        );

        let out = eval_struct_expr(&arena, expr, &[slot], &chunk).expect("struct expr");
        let out = out.as_any().downcast_ref::<StructArray>().unwrap();
        let field = out.column(0).as_any().downcast_ref::<ListArray>().unwrap();

        assert_eq!(field.values().data_type(), &DataType::Int32);
        let values = field
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
    }
}
