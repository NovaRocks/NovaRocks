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
use super::common::{NumericArrayView, cast_output};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{ArrayRef, Int64Array};
use novarocks_functions::builtin::numeric_mod_core::{NumericModOp, evaluate_mod_rows};
use std::sync::Arc;

fn eval_mod_impl<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    positive: bool,
) -> Result<ArrayRef, __Eval::Error> {
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_view = NumericArrayView::new(&left)?;
    let right_view = NumericArrayView::new(&right)?;
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    let operation = if positive {
        NumericModOp::Pmod
    } else {
        NumericModOp::Mod
    };
    evaluate_mod_rows(
        operation,
        &left_view,
        &right_view,
        0..len,
        |_, row| {
            Ok((
                if left_view.len() == 1 && len > 1 {
                    0
                } else {
                    row
                },
                if right_view.len() == 1 && len > 1 {
                    0
                } else {
                    row
                },
            ))
        },
        |out| {
            values.push(out);
            Ok(())
        },
        || Ok(()),
    )
    .map_err(|error| error.to_string())?;
    let out = Arc::new(Int64Array::from(values)) as ArrayRef;
    Ok((cast_output(out, arena.data_type(expr)))?)
}

pub fn eval_mod<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_mod_impl(arena, expr, args, chunk, false))?)
}

pub fn eval_pmod<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_mod_impl(arena, expr, args, chunk, true))?)
}

#[cfg(test)]
mod legacy_mod_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{Float32Array, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(name: &'static str, left: ArrayRef, right: ArrayRef) -> Vec<Option<f64>> {
        let left_type = left.data_type().clone();
        let right_type = right.data_type().clone();
        let schema = Arc::new(Schema::new(vec![
            Field::new("left", left_type.clone(), true),
            Field::new("right", right_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![left, right]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1), SlotId::new(2)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let left = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), left_type);
        let right = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), right_type);
        // The actual registered scalar signatures return DOUBLE; do not
        // substitute an artificial BIGINT result projection for this oracle.
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math(name),
                args: vec![left, right],
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

    fn signed(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }
    fn floating(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }

    #[test]
    fn legacy_mod_full_signed_boundaries_preserve_double_result_rounding() {
        let left = signed(vec![
            Some(i64::MIN),
            Some(i64::MIN),
            Some(i64::MIN),
            Some(i64::MIN),
            Some(-1),
            Some(-2),
            Some(i64::MIN + 1),
            Some(i64::MAX),
            Some(1),
            Some(0),
        ]);
        let right = signed(vec![
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
            Some(3),
            Some(i64::MIN),
            Some(i64::MIN),
            Some(i64::MIN),
            Some(i64::MIN),
            Some(i64::MIN),
            Some(i64::MIN),
        ]);
        let mod_result = evaluate("mod", left.clone(), right.clone());
        let pmod_result = evaluate("pmod", left, right);
        // Independent signed-remainder arithmetic followed by the public
        // DOUBLE projection: both MAX and MAX-1 round to exactly 2^63.
        let two63 = f64::from_bits(0x43e0000000000000);
        assert_eq!(
            mod_result,
            vec![
                Some(0.0),
                Some(0.0),
                Some(-1.0),
                Some(-2.0),
                Some(-1.0),
                Some(-2.0),
                Some(-two63),
                Some(two63),
                Some(1.0),
                Some(0.0)
            ]
        );
        assert_eq!(
            pmod_result,
            vec![
                Some(0.0),
                Some(0.0),
                Some(two63),
                Some(1.0),
                Some(two63),
                Some(two63),
                Some(1.0),
                Some(two63),
                Some(1.0),
                Some(0.0)
            ]
        );
    }

    #[test]
    fn legacy_mod_dividend_sign_and_pmod_absolute_divisor_are_preserved() {
        let left = signed(vec![
            Some(-7),
            Some(-7),
            Some(7),
            Some(7),
            Some(-6),
            Some(-6),
        ]);
        let right = signed(vec![
            Some(3),
            Some(-3),
            Some(3),
            Some(-3),
            Some(3),
            Some(-3),
        ]);
        assert_eq!(
            evaluate("mod", left.clone(), right.clone()),
            vec![
                Some(-1.0),
                Some(-1.0),
                Some(1.0),
                Some(1.0),
                Some(0.0),
                Some(0.0)
            ]
        );
        assert_eq!(
            evaluate("pmod", left, right),
            vec![
                Some(2.0),
                Some(2.0),
                Some(1.0),
                Some(1.0),
                Some(0.0),
                Some(0.0)
            ]
        );
    }

    #[test]
    fn legacy_mod_zero_divisors_and_either_null_remain_successful_null() {
        let left = signed(vec![Some(i64::MIN), Some(3), None, Some(3), None]);
        let right = signed(vec![Some(0), Some(0), Some(-1), None, None]);
        for name in ["mod", "pmod"] {
            assert_eq!(evaluate(name, left.clone(), right.clone()), vec![None; 5]);
        }
    }

    #[test]
    fn legacy_mod_float_inputs_truncate_and_saturate_before_remainder() {
        let left = floating(vec![
            Some(-7.9),
            Some(7.9),
            Some(f64::MAX),
            Some(-f64::MAX),
            Some(f64::MAX),
            Some(-f64::MAX),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]);
        let right = floating(vec![
            Some(-3.8),
            Some(3.8),
            Some(2.0),
            Some(3.0),
            Some(-f64::MAX),
            Some(-1.0),
            Some(3.0),
            Some(3.0),
            Some(3.0),
        ]);
        let two63 = f64::from_bits(0x43e0000000000000);
        assert_eq!(
            evaluate("mod", left.clone(), right.clone()),
            vec![
                Some(-1.0),
                Some(1.0),
                Some(1.0),
                Some(-2.0),
                Some(two63),
                Some(0.0),
                None,
                None,
                None
            ]
        );
        assert_eq!(
            evaluate("pmod", left, right),
            vec![
                Some(2.0),
                Some(1.0),
                Some(1.0),
                Some(1.0),
                Some(two63),
                Some(0.0),
                None,
                None,
                None
            ]
        );
    }

    #[test]
    fn legacy_mod_float32_fractional_zero_and_nonfinite_divisors_are_not_fmod() {
        let left: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(-7.9f32),
            Some(7.9),
            Some(1.0),
            Some(1.0),
            Some(1.0),
            Some(1.0),
            Some(1.0),
        ]));
        let right: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(3.8f32),
            Some(-3.8),
            Some(0.9),
            Some(-0.9),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::NAN),
        ]));
        assert_eq!(
            evaluate("mod", left.clone(), right.clone()),
            vec![Some(-1.0), Some(1.0), None, None, None, None, None]
        );
        assert_eq!(
            evaluate("pmod", left, right),
            vec![Some(2.0), Some(1.0), None, None, None, None, None]
        );
    }
}
