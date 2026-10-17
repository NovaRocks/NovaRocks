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
use novarocks_functions::builtin::numeric_elementary::{
    NumericElementaryOp, evaluate_legacy_numeric_elementary,
};
#[cfg(test)]
use std::sync::Arc;

pub fn eval_sign<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let arguments = [arena.eval(args[0], chunk)?];
    let out =
        evaluate_legacy_numeric_elementary(NumericElementaryOp::Sign, &arguments, chunk.len())
            .map_err(|error| error.to_string())?;
    // The shared core returns the original Int64 SIGN carrier before this
    // legacy cast; the selected owner separately projects the same value to f64.
    Ok((super::common::cast_output(out, arena.data_type(expr)))?)
}

#[cfg(test)]
mod legacy_elementary_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(name: &'static str, inputs: Vec<ArrayRef>) -> Vec<Option<f64>> {
        let fields = inputs
            .iter()
            .enumerate()
            .map(|(i, array)| Field::new(format!("v{i}"), array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let slots = (1..=inputs.len())
            .map(|i| SlotId::new(i as u32))
            .collect::<Vec<_>>();
        let types = inputs
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs).unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        let args = slots
            .into_iter()
            .zip(types)
            .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
            .collect::<Vec<_>>();
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math(name),
                args,
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
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }
    fn floats(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }

    #[test]
    fn legacy_elementary_sign_nan_infinities_and_signed_zero_are_successful_values() {
        let result = evaluate(
            "sign",
            vec![floats(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(0.0),
                Some(-0.0),
                Some(f64::MIN_POSITIVE),
                Some(-f64::MIN_POSITIVE),
                None,
            ])],
        );
        assert_eq!(
            result,
            vec![
                Some(0.0),
                Some(1.0),
                Some(-1.0),
                Some(0.0),
                Some(0.0),
                Some(1.0),
                Some(-1.0),
                None
            ]
        );
        assert_eq!(result[4].unwrap().to_bits(), 0);
    }

    #[test]
    fn legacy_elementary_sign_decimal_scale_preserves_sign_and_null() {
        use arrow::array::Decimal128Array;
        for scale in [-2, 0, 38] {
            let input: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(-1), Some(0), Some(1), None])
                    .with_precision_and_scale(38, scale)
                    .unwrap(),
            );
            assert_eq!(
                evaluate("sign", vec![input]),
                vec![Some(-1.0), Some(0.0), Some(1.0), None]
            );
        }
    }
}
