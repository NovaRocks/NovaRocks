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
use arrow::array::{ArrayRef, Float64Array};
use novarocks_functions::builtin::numeric_binary_core::{NumericBinaryOp, evaluate_binary_rows};
use std::sync::Arc;

fn eval_binary_f64<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    operation: NumericBinaryOp,
) -> Result<ArrayRef, __Eval::Error> {
    let left = arena.eval(args[0], chunk)?;
    let right = arena.eval(args[1], chunk)?;
    let left_view = NumericArrayView::new(&left)?;
    let right_view = NumericArrayView::new(&right)?;
    let len = chunk.len();
    let mut values = Vec::with_capacity(len);
    evaluate_binary_rows(
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
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    Ok((cast_output(out, arena.data_type(expr)))?)
}

pub fn eval_atan2<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_binary_f64(arena, expr, args, chunk, NumericBinaryOp::Atan2))?)
}

pub fn eval_fmod<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_binary_f64(arena, expr, args, chunk, NumericBinaryOp::Fmod))?)
}

pub fn eval_pow<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_binary_f64(arena, expr, args, chunk, NumericBinaryOp::Pow))?)
}

#[cfg(test)]
mod legacy_binary_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{Decimal128Array, Float32Array, Int64Array};
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
        // These are canonical names present in the legacy runtime metadata.
        // The pure owner's actual binding tests cover aliases independently.
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math(name),
                args: vec![left, right],
            },
            DataType::Float64,
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)
            .unwrap();
        assert_eq!(output.data_type(), &DataType::Float64);
        output
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn doubles(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }

    #[test]
    fn legacy_binary_atan2_preserves_argument_order_and_quadrants() {
        let actual = evaluate(
            "atan2",
            doubles(vec![Some(1.0), Some(0.0), Some(1.0), Some(-1.0), None]),
            doubles(vec![
                Some(0.0),
                Some(1.0),
                Some(-1.0),
                Some(-1.0),
                Some(1.0),
            ]),
        );
        let expected = [
            Some(std::f64::consts::FRAC_PI_2),
            Some(0.0),
            Some(3.0 * std::f64::consts::FRAC_PI_4),
            Some(-3.0 * std::f64::consts::FRAC_PI_4),
            None,
        ];
        for (actual, expected) in actual.into_iter().zip(expected) {
            match (actual, expected) {
                (Some(actual), Some(expected)) => assert!((actual - expected).abs() < 1e-15),
                (None, None) => {}
                pair => panic!("unexpected quadrant result: {pair:?}"),
            }
        }
    }

    #[test]
    fn legacy_binary_atan2_computes_infinities_and_preserves_signed_zero() {
        let actual = evaluate(
            "atan2",
            doubles(vec![
                Some(0.0),
                Some(-0.0),
                Some(0.0),
                Some(-0.0),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(1.0),
                Some(f64::NAN),
            ]),
            doubles(vec![
                Some(1.0),
                Some(1.0),
                Some(-1.0),
                Some(-1.0),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(1.0),
            ]),
        );
        assert_eq!(actual[0].unwrap().to_bits(), 0);
        assert_eq!(actual[1].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(actual[2], Some(std::f64::consts::PI));
        assert_eq!(actual[3], Some(-std::f64::consts::PI));
        assert_eq!(actual[4], Some(std::f64::consts::FRAC_PI_4));
        assert_eq!(actual[5], Some(-std::f64::consts::FRAC_PI_4));
        assert_eq!(actual[6].unwrap().to_bits(), 0);
        assert_eq!(actual[7], None);
    }

    #[test]
    fn legacy_binary_fmod_uses_dividend_sign_and_strict_nulls() {
        let actual = evaluate(
            "fmod",
            doubles(vec![
                Some(5.5),
                Some(-5.5),
                Some(5.5),
                Some(-5.5),
                Some(-0.0),
                None,
                Some(1.0),
            ]),
            doubles(vec![
                Some(2.0),
                Some(2.0),
                Some(-2.0),
                Some(-2.0),
                Some(2.0),
                Some(2.0),
                None,
            ]),
        );
        assert_eq!(
            &actual[..4],
            &[Some(1.5), Some(-1.5), Some(1.5), Some(-1.5)]
        );
        assert_eq!(actual[4].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(&actual[5..], &[None, None]);
    }

    #[test]
    fn legacy_binary_fmod_filters_results_after_infinite_operand_evaluation() {
        assert_eq!(
            evaluate(
                "fmod",
                doubles(vec![
                    Some(5.5),
                    Some(-5.5),
                    Some(f64::INFINITY),
                    Some(1.0),
                    Some(1.0),
                    Some(f64::NAN)
                ]),
                doubles(vec![
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(2.0),
                    Some(0.0),
                    Some(-0.0),
                    Some(2.0)
                ])
            ),
            vec![Some(5.5), Some(-5.5), None, None, None, None],
        );
    }

    #[test]
    fn legacy_binary_pow_computes_nan_identity_domain_and_overflow_before_nulling() {
        assert_eq!(
            evaluate(
                "pow",
                doubles(vec![
                    Some(f64::NAN),
                    None,
                    Some(-4.0),
                    Some(-2.0),
                    Some(-2.0),
                    Some(2.0),
                    Some(1.0),
                    Some(f64::INFINITY)
                ]),
                doubles(vec![
                    Some(0.0),
                    Some(0.0),
                    Some(0.5),
                    Some(3.0),
                    Some(2.0),
                    Some(1024.0),
                    Some(f64::NAN),
                    Some(-1.0)
                ])
            ),
            vec![
                Some(1.0),
                None,
                None,
                Some(-8.0),
                Some(4.0),
                None,
                Some(1.0),
                Some(0.0)
            ],
        );
    }

    #[test]
    fn legacy_binary_pow_keeps_signed_zero_and_nulls_infinite_reciprocals() {
        let actual = evaluate(
            "pow",
            doubles(vec![
                Some(-0.0),
                Some(-0.0),
                Some(-0.0),
                Some(-0.0),
                Some(0.0),
                Some(-0.0),
            ]),
            doubles(vec![
                Some(3.0),
                Some(2.0),
                Some(0.5),
                Some(-3.0),
                Some(-1.0),
                Some(0.0),
            ]),
        );
        assert_eq!(actual[0].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(actual[1].unwrap().to_bits(), 0);
        assert_eq!(actual[2].unwrap().to_bits(), 0);
        assert_eq!(&actual[3..], &[None, None, Some(1.0)]);
    }

    #[test]
    fn legacy_binary_numeric_readers_widen_float32_and_decode_each_decimal_scale() {
        let float32: ArrayRef = Arc::new(Float32Array::from(vec![Some(f32::MAX), None]));
        let exponent: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), Some(2)]));
        let actual = evaluate("pow", float32, exponent);
        // A Float32 computation overflows; the actual reader widens before pow.
        assert_eq!(actual[0].unwrap().to_bits(), 0x4fefffffc0000020);
        assert_eq!(actual[1], None);
        let left: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(123), Some(-123), None])
                .with_precision_and_scale(5, -2)
                .unwrap(),
        );
        let right: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(2_000_000), Some(2_000_000), Some(2_000_000)])
                .with_precision_and_scale(7, 3)
                .unwrap(),
        );
        assert_eq!(
            evaluate("fmod", left, right),
            vec![Some(300.0), Some(-300.0), None]
        );
    }
}
