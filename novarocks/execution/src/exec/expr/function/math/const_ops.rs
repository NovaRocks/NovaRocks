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

use super::common::cast_output;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::Float64Array;
use novarocks_functions::builtin::numeric_elementary::{
    NumericElementaryOp, evaluate_legacy_numeric_elementary,
};
#[cfg(test)]
use std::sync::Arc;

pub fn eval_e<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let out = evaluate_legacy_numeric_elementary(NumericElementaryOp::E, &[], chunk.len())
        .map_err(|error| error.to_string())?;
    Ok((cast_output(out, arena.data_type(expr)))?)
}

pub fn eval_pi<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    _args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let out = evaluate_legacy_numeric_elementary(NumericElementaryOp::Pi, &[], chunk.len())
        .map_err(|error| error.to_string())?;
    Ok((cast_output(out, arena.data_type(expr)))?)
}

#[cfg(test)]
mod legacy_elementary_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(name: &'static str, rows: usize) -> ArrayRef {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unused",
                DataType::Int32,
                false,
            )])),
            vec![Arc::new(Int32Array::from(vec![0; rows])) as ArrayRef],
        )
        .unwrap();
        let schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math(name),
                args: vec![],
            },
            DataType::Float64,
        );
        let frozen = arena.into_immutable().unwrap();
        let result = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)
            .unwrap();
        assert_eq!(result.data_type(), &DataType::Float64);
        result
    }

    #[test]
    fn legacy_elementary_e_pi_have_exact_double_bits_for_every_batch_row() {
        for (name, bits) in [("e", 0x4005bf0a8b145769u64), ("pi", 0x400921fb54442d18u64)] {
            let result = evaluate(name, 3);
            let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
            assert_eq!(result.len(), 3);
            assert_eq!(
                result
                    .iter()
                    .map(|v| v.unwrap().to_bits())
                    .collect::<Vec<_>>(),
                vec![bits; 3]
            );
        }
    }

    #[test]
    fn legacy_elementary_e_pi_empty_batch_remains_empty_double() {
        for name in ["e", "pi"] {
            let result = evaluate(name, 0);
            assert_eq!(result.len(), 0);
            assert_eq!(result.data_type(), &DataType::Float64);
        }
    }
}
