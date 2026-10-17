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
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::{ListArray, new_null_array};
#[cfg(test)]
use arrow::datatypes::{DataType, Field};
use novarocks_functions::builtin::array_literal_core::{ArrayLiteralInputs, ConstructionFailure};
use std::convert::Infallible;
#[cfg(test)]
use std::sync::Arc;
fn legacy_error(failure: ConstructionFailure<Infallible>) -> String {
    match failure {
        ConstructionFailure::Data(error) => error.legacy_message(),
        ConstructionFailure::Control(never) => match never {},
    }
}
pub fn eval_array_expr<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    id: ExprId,
    elements: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let mut observe = |_| Ok::<(), Infallible>(());
    let mut inputs = ArrayLiteralInputs::from_output(
        arena.data_type(id),
        chunk.len(),
        elements.len(),
        &mut observe,
    )
    .map_err(legacy_error)?;
    for expr_id in elements {
        let array = arena.eval(*expr_id, chunk)?;
        inputs
            .push_evaluated(array, &mut observe)
            .map_err(legacy_error)?;
    }
    Ok((inputs
        .finish(
            crate::exec::expr::cast::cast_with_special_rules,
            &mut observe,
        )
        .map_err(legacy_error))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn one_row_chunk() -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).expect("batch");
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn array_expr_preserves_null_elements() {
        let mut arena = ExprArena::default();
        let value = arena.push_typed(ExprNode::Literal(LiteralValue::Int32(11)), DataType::Int32);
        let null = arena.push_typed(ExprNode::Literal(LiteralValue::Null), DataType::Int32);
        let array = arena.push_typed(
            ExprNode::ArrayExpr {
                elements: vec![value, null],
            },
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        let result = arena.eval(array, &one_row_chunk()).expect("array expr");
        let list = result
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list array");
        let values = list
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int values");

        assert_eq!(list.len(), 1);
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), 11);
        assert!(values.is_null(1));
    }
}
