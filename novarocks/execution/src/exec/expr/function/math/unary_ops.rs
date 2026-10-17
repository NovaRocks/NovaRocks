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
use arrow::array::{ArrayRef, Float64Array};
use novarocks_functions::builtin::numeric_unary::{self, NumericUnaryOp};
use std::sync::Arc;
pub(super) fn eval_unary_f64<
    F: Fn(f64) -> f64,
    __Eval: crate::exec::expr::ExprEvaluation + ?Sized,
>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    func: F,
) -> Result<ArrayRef, __Eval::Error> {
    let array = arena.eval(args[0], chunk)?;
    Ok((numeric_unary::evaluate_legacy_unary_f64(
        &array,
        chunk.len(),
        arena.data_type(expr),
        func,
    )
    .map_err(|error| error.to_string()))?)
}
fn eval_unary<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    op: NumericUnaryOp,
) -> Result<ArrayRef, __Eval::Error> {
    let array = arena.eval(args[0], chunk)?;
    Ok((numeric_unary::evaluate_legacy_numeric_unary(
        op,
        &array,
        chunk.len(),
        arena.data_type(expr),
    )
    .map_err(|error| error.to_string()))?)
}
pub fn eval_acos<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Acos))?)
}
pub fn eval_asin<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Asin))?)
}
pub fn eval_atan<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Atan))?)
}
pub fn eval_ceil<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Ceil))?)
}
pub fn eval_cos<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Cos))?)
}
pub fn eval_cbrt<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Cbrt))?)
}
pub fn eval_cot<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Cot))?)
}
pub fn eval_degress<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Degrees))?)
}
pub fn eval_dlog1<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Dlog1))?)
}
pub fn eval_exp<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Exp))?)
}
pub fn eval_floor<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Floor))?)
}
pub fn eval_ln<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Ln))?)
}
pub fn eval_log10<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Log10))?)
}
pub fn eval_log2<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Log2))?)
}
pub fn eval_radians<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Radians))?)
}
pub fn eval_positive<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Positive))?)
}
pub fn eval_sin<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Sin))?)
}
pub fn eval_sqrt<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Sqrt))?)
}
pub fn eval_square<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Square))?)
}
pub fn eval_tan<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_unary(arena, expr, args, chunk, NumericUnaryOp::Tan))?)
}
#[cfg(test)]
mod legacy_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{Array, Decimal128Array, Float32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(name: &'static str, input: ArrayRef, output_type: DataType) -> ArrayRef {
        let input_type = input.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new("v", input_type.clone(), true)]));
        let batch = RecordBatch::try_new(schema, vec![input]).unwrap();
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(1)],
        )
        .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), input_type);
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math(name),
                args: vec![source],
            },
            output_type.clone(),
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)
            .unwrap();
        assert_eq!(output.data_type(), &output_type);
        output
    }

    fn doubles(array: &ArrayRef) -> Vec<Option<f64>> {
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }
    fn integers(array: &ArrayRef) -> Vec<Option<i64>> {
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }
    fn decimal(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .unwrap(),
        )
    }

    #[test]
    fn legacy_positive_sanitizes_nonfinite_sources_before_named_float_cast() {
        let float64: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(1.5),
            None,
        ]));
        let float32: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(-0.0),
            Some(1.5),
            None,
        ]));
        for source in [float64, float32] {
            let result = doubles(&evaluate("positive", source, DataType::Float64));
            assert_eq!(&result[..3], &[None, None, None]);
            assert_eq!(result[3].unwrap().to_bits(), (-0.0f64).to_bits());
            assert_eq!(result[4], Some(1.5));
            assert_eq!(result[5], None);
        }
    }

    #[test]
    fn legacy_unary_computes_infinite_inputs_before_sanitizing_results() {
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::NEG_INFINITY),
            Some(f64::INFINITY),
            Some(f64::NAN),
            None,
        ]));
        let atan = doubles(&evaluate("atan", input.clone(), DataType::Float64));
        assert_eq!(atan[0], Some(-std::f64::consts::FRAC_PI_2));
        assert_eq!(atan[1], Some(std::f64::consts::FRAC_PI_2));
        assert_eq!(&atan[2..], &[None, None]);
        assert_eq!(
            doubles(&evaluate("exp", input, DataType::Float64)),
            vec![Some(0.0), None, None, None],
        );
    }

    #[test]
    fn legacy_ceil_floor_use_checked_arrow_int64_boundaries() {
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::from_bits(0xc3e0000000000000)),
            Some(f64::from_bits(0xc3e0000000000001)),
            Some(f64::from_bits(0x43dfffffffffffff)),
            Some(f64::from_bits(0x43e0000000000000)),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(-0.0),
            None,
        ]));
        // The legacy frozen runtime registry accepts the canonical operation
        // names. Alias binding is exercised by the pure owner contract tests.
        for name in ["ceil", "floor"] {
            assert_eq!(
                integers(&evaluate(name, input.clone(), DataType::Int64)),
                vec![
                    Some(i64::MIN),
                    None,
                    Some(9_223_372_036_854_774_784),
                    None,
                    None,
                    None,
                    Some(0),
                    None
                ],
                "{name}",
            );
        }
        // The historical reader first rounds signed BIGINT to f64, even for
        // integer inputs. Do not replace that step with exact integer identity.
        let signed: ArrayRef =
            Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, i64::MAX - 1023]));
        for name in ["ceil", "floor"] {
            assert_eq!(
                integers(&evaluate(name, signed.clone(), DataType::Int64)),
                vec![Some(i64::MIN), None, Some(9_223_372_036_854_774_784)],
            );
        }
    }

    #[test]
    fn legacy_decimal_reader_keeps_binary_rounding_and_negative_scale() {
        let large = decimal(
            vec![
                Some(9_007_199_254_740_993),
                Some(-9_007_199_254_740_993),
                None,
            ],
            38,
            0,
        );
        for name in ["ceil", "floor"] {
            assert_eq!(
                integers(&evaluate(name, large.clone(), DataType::Int64)),
                vec![
                    Some(9_007_199_254_740_992),
                    Some(-9_007_199_254_740_992),
                    None
                ],
            );
        }
        let scaled = decimal(vec![Some(123), Some(-123), None], 5, -2);
        assert_eq!(
            doubles(&evaluate("positive", scaled.clone(), DataType::Float64)),
            vec![Some(12_300.0), Some(-12_300.0), None],
        );
        assert_eq!(
            integers(&evaluate("ceil", scaled, DataType::Int64)),
            vec![Some(12_300), Some(-12_300), None],
        );
    }

    #[test]
    fn legacy_large_decimal_scale_and_dlog1_round_before_the_operation() {
        let tiny = decimal(vec![Some(1), Some(-1), None], 38, 38);
        assert_eq!(
            doubles(&evaluate("dlog1", tiny.clone(), DataType::Float64)),
            vec![Some(0.0), Some(0.0), None],
        );
        assert_eq!(
            integers(&evaluate("ceil", tiny.clone(), DataType::Int64)),
            vec![Some(1), Some(0), None],
        );
        assert_eq!(
            integers(&evaluate("floor", tiny, DataType::Int64)),
            vec![Some(0), Some(-1), None],
        );
        let endpoint: ArrayRef = Arc::new(Float64Array::from(vec![
            -1.0,
            f64::from_bits(0xbfefffffffffffff),
            -2.0,
            2.0f64.powi(-54),
        ]));
        let result = doubles(&evaluate("dlog1", endpoint, DataType::Float64));
        assert_eq!(result[0], None);
        assert!((result[1].unwrap() - (-36.736_800_569_677_1)).abs() < 1e-14);
        assert_eq!(result[2], None);
        assert_eq!(result[3], Some(0.0));
    }

    #[test]
    fn legacy_unary_signed_zero_is_preserved_only_by_the_actual_operation() {
        let negative_zero: ArrayRef = Arc::new(Float64Array::from(vec![-0.0]));
        for name in [
            "positive", "sqrt", "sin", "atan", "asin", "tan", "cbrt", "radians", "degress",
        ] {
            let result = doubles(&evaluate(name, negative_zero.clone(), DataType::Float64));
            assert_eq!(result[0].unwrap().to_bits(), 0x8000000000000000, "{name}");
        }
        for name in ["square", "dlog1"] {
            let result = doubles(&evaluate(name, negative_zero.clone(), DataType::Float64));
            assert_eq!(result[0].unwrap().to_bits(), 0, "{name}");
        }
        assert_eq!(
            doubles(&evaluate("cot", negative_zero, DataType::Float64)),
            vec![None]
        );
    }

    #[test]
    fn legacy_float32_is_widened_before_square_and_domain_nulling() {
        let input: ArrayRef = Arc::new(Float32Array::from(vec![Some(f32::MAX), None]));
        let output = doubles(&evaluate("square", input, DataType::Float64));
        assert_eq!(output[0].unwrap().to_bits(), 0x4fefffffc0000020);
        assert_eq!(output[1], None);
        let domains: ArrayRef = Arc::new(Float64Array::from(vec![-1.0, 0.0, 1.0, 2.0]));
        let acos = doubles(&evaluate("acos", domains.clone(), DataType::Float64));
        assert_eq!(acos[0], Some(std::f64::consts::PI));
        assert_eq!(acos[2], Some(0.0));
        assert_eq!(acos[3], None);
        assert_eq!(
            doubles(&evaluate("sqrt", domains, DataType::Float64)),
            vec![None, Some(0.0), Some(1.0), Some(std::f64::consts::SQRT_2)],
        );
    }
}
