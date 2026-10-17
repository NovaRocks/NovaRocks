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
use novarocks_functions::builtin::dround_core::{DroundComputation, evaluate_legacy_dround};
use std::sync::Arc;
fn evaluate<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    unary: DroundComputation,
) -> Result<ArrayRef, __Eval::Error> {
    let left = arena.eval(args[0], chunk)?;
    let (op, right) = if args.len() == 1 {
        (unary, None)
    } else {
        (
            DroundComputation::TruncateDigits,
            Some(arena.eval(args[1], chunk)?),
        )
    };
    Ok((evaluate_legacy_dround(
        op,
        &left,
        right.as_ref(),
        chunk.len(),
        arena.data_type(expr),
    )
    .map_err(|error| error.to_string()))?)
}
pub fn eval_truncate<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((evaluate(arena, expr, args, chunk, DroundComputation::Truncate))?)
}
pub fn eval_dround<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((evaluate(arena, expr, args, chunk, DroundComputation::Round))?)
}

#[cfg(test)]
mod legacy_dround_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::ExprNode;
    use crate::exec::expr::function::FunctionKind;
    use arrow::array::{
        Decimal128Array, Float32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(inputs: Vec<ArrayRef>) -> Vec<Option<f64>> {
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
                kind: FunctionKind::Math("dround"),
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
    fn digits(values: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(values))
    }

    #[test]
    fn legacy_dround_six_fixed_unary_profiles_keep_double_and_strict_null() {
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Int16Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Int32Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Int64Array::from(vec![Some(2), Some(-2), None])),
            Arc::new(Float32Array::from(vec![Some(2.0), Some(-2.0), None])),
            floats(vec![Some(2.0), Some(-2.0), None]),
        ];
        for input in inputs {
            assert_eq!(evaluate(vec![input]), vec![Some(2.0), Some(-2.0), None]);
        }
    }

    #[test]
    fn legacy_dround_unary_rounds_ties_but_binary_zero_digits_truncates() {
        let input = floats(vec![Some(2.5), Some(-2.5), Some(3.5), Some(-3.5), None]);
        assert_eq!(
            evaluate(vec![input.clone()]),
            vec![Some(3.0), Some(-3.0), Some(4.0), Some(-4.0), None]
        );
        assert_eq!(
            evaluate(vec![input, digits(vec![Some(0); 5])]),
            vec![Some(2.0), Some(-2.0), Some(3.0), Some(-3.0), None]
        );
    }

    #[test]
    fn legacy_dround_signed_zero_and_nonfinite_results_are_preserved() {
        let input = floats(vec![
            Some(-0.0),
            Some(0.0),
            Some(-0.25),
            Some(0.25),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ]);
        for inputs in [vec![input.clone()], vec![input, digits(vec![Some(0); 7])]] {
            let result = evaluate(inputs);
            assert_eq!(result[0].unwrap().to_bits(), 0x8000000000000000);
            assert_eq!(result[1].unwrap().to_bits(), 0);
            assert_eq!(result[2].unwrap().to_bits(), 0x8000000000000000);
            assert_eq!(result[3].unwrap().to_bits(), 0);
            assert_eq!(&result[4..], &[None; 3]);
        }
    }

    #[test]
    fn legacy_dround_int32_extreme_digits_preserve_original_powi_formula() {
        let result = evaluate(vec![
            floats(vec![
                Some(1.0),
                Some(-1.0),
                Some(2.0),
                Some(1.0),
                Some(-1.0),
                Some(0.0),
                Some(-0.0),
                Some(1.0),
                Some(1.0),
                Some(1.0),
            ]),
            digits(vec![
                Some(308),
                Some(308),
                Some(308),
                Some(-308),
                Some(-308),
                Some(308),
                Some(-308),
                Some(i32::MAX),
                Some(i32::MIN),
                Some(-i32::MAX),
            ]),
        ]);
        assert_eq!(&result[..3], &[Some(1.0), Some(-1.0), None]);
        assert_eq!(result[3].unwrap().to_bits(), 0);
        assert_eq!(result[4].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(result[5].unwrap().to_bits(), 0);
        assert_eq!(result[6].unwrap().to_bits(), 0x8000000000000000);
        // For MIN digits, the original i64 negation then i32 projection
        // feeds MIN to powi. Do not introduce a wider exponent protocol.
        assert_eq!(&result[7..], &[None; 3]);
    }

    #[test]
    fn legacy_dround_decimal_unary_decodes_negative_and_large_scales_before_rounding() {
        let negative_scale: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(123), Some(-123), None])
                .with_precision_and_scale(38, -2)
                .unwrap(),
        );
        assert_eq!(
            evaluate(vec![negative_scale]),
            vec![Some(12_300.0), Some(-12_300.0), None]
        );
        let large_scale: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(-1), None])
                .with_precision_and_scale(38, 38)
                .unwrap(),
        );
        let result = evaluate(vec![large_scale]);
        assert_eq!(result[0].unwrap().to_bits(), 0);
        assert_eq!(result[1].unwrap().to_bits(), 0x8000000000000000);
        assert_eq!(result[2], None);
    }

    #[test]
    fn legacy_dround_binary_null_value_or_digits_stays_successful_null() {
        assert_eq!(
            evaluate(vec![
                floats(vec![None, Some(2.5), None]),
                digits(vec![Some(0), None, None])
            ]),
            vec![None; 3]
        );
    }
}

#[cfg(test)]
mod legacy_rounding_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::types::Int8Type;
    use arrow::array::{
        Decimal128Array, DictionaryArray, Int8Array, Int64Array, StringArray, UInt8Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    enum Digits {
        Column(ArrayRef),
        Literal(LiteralValue),
    }

    fn evaluate(
        value: ArrayRef,
        digits: Option<Digits>,
        output_type: DataType,
    ) -> Result<ArrayRef, String> {
        let mut inputs = vec![value];
        let literal = match digits {
            Some(Digits::Column(array)) => {
                inputs.push(array);
                None
            }
            Some(Digits::Literal(value)) => Some(value),
            None => None,
        };
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
        let mut args = slots
            .into_iter()
            .zip(types)
            .map(|(slot, ty)| arena.push_typed(ExprNode::SlotId(slot), ty))
            .collect::<Vec<_>>();
        if let Some(value) = literal {
            args.push(arena.push_typed(ExprNode::Literal(value), DataType::Int64));
        }
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math("truncate"),
                args,
            },
            output_type.clone(),
        );
        let frozen = arena.into_immutable().unwrap();
        let result = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)?;
        assert_eq!(result.data_type(), &output_type);
        Ok(result)
    }

    fn floats(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }

    fn doubles(output: &ArrayRef) -> Vec<Option<f64>> {
        output
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn decimals(output: &ArrayRef) -> Vec<Option<i128>> {
        output
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn unary_truncate_sanitizes_nonfinite_and_uses_checked_arrow_integer_cast() {
        let output = evaluate(
            floats(vec![
                Some(2.9),
                Some(-2.9),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(9_223_372_036_854_775_808.0),
                Some(-9_223_372_036_854_775_808.0),
                Some(-0.0),
                None,
            ]),
            None,
            DataType::Int64,
        )
        .unwrap();
        let values = output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                Some(2),
                Some(-2),
                None,
                None,
                None,
                None,
                Some(i64::MIN),
                Some(0),
                None
            ]
        );
    }

    #[test]
    fn direct_decimal_truncate_keeps_lossy_double_then_arrow_output_cast() {
        let input = Arc::new(
            Decimal128Array::from(vec![
                Some(9_007_199_254_740_993),
                Some(-9_007_199_254_740_993),
                None,
            ])
            .with_precision_and_scale(18, 0)
            .unwrap(),
        ) as ArrayRef;
        for digits in [None, Some(Digits::Literal(LiteralValue::Int64(0)))] {
            let output = evaluate(input.clone(), digits, DataType::Decimal128(38, 0)).unwrap();
            assert_eq!(
                decimals(&output),
                vec![
                    Some(9_007_199_254_740_992),
                    Some(-9_007_199_254_740_992),
                    None
                ]
            );
        }
        let input = Arc::new(
            Decimal128Array::from(vec![129, -129])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        );
        assert_eq!(
            decimals(&evaluate(input, None, DataType::Decimal128(38, 2)).unwrap()),
            vec![Some(100), Some(-100)]
        );
    }

    #[test]
    fn truncate_literal_column_digits_and_float_to_i64_saturation_keep_strict_null() {
        let input = floats(vec![Some(1.25), Some(-1.25), None]);
        for digits in [
            Digits::Literal(LiteralValue::Int64(1)),
            Digits::Column(Arc::new(Int64Array::from(vec![1, 1, 1]))),
        ] {
            assert_eq!(
                doubles(&evaluate(input.clone(), Some(digits), DataType::Float64).unwrap()),
                vec![Some(1.2), Some(-1.2), None]
            );
        }
        assert_eq!(
            doubles(
                &evaluate(
                    input.clone(),
                    Some(Digits::Column(Arc::new(Int64Array::from(vec![
                        Some(1),
                        None,
                        Some(1)
                    ])))),
                    DataType::Float64
                )
                .unwrap()
            ),
            vec![Some(1.2), None, None]
        );
        assert_eq!(
            doubles(
                &evaluate(
                    input,
                    Some(Digits::Literal(LiteralValue::Null)),
                    DataType::Float64
                )
                .unwrap()
            ),
            vec![None; 3]
        );
        // Finite float digits saturate as i64 before the original i32 exponent
        // conversion. ROUND instead uses Arrow's checked digits cast.
        assert_eq!(
            doubles(
                &evaluate(
                    floats(vec![Some(3.75)]),
                    Some(Digits::Column(floats(vec![Some(1e100)]))),
                    DataType::Float64
                )
                .unwrap()
            ),
            vec![Some(0.0)]
        );
    }

    #[test]
    fn truncate_rejects_unsigned_text_and_dictionary_instead_of_using_round_casts() {
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(UInt8Array::from(vec![Some(2), None])),
            Arc::new(StringArray::from(vec![Some("2.5"), None])),
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![Some(0), None]),
                    floats(vec![Some(2.5)]),
                )
                .unwrap(),
            ),
        ];
        for input in inputs {
            for binary in [false, true] {
                let digits = binary.then_some(Digits::Literal(LiteralValue::Int64(0)));
                let output_type = if binary {
                    DataType::Float64
                } else {
                    DataType::Int64
                };
                let error = evaluate(input.clone(), digits, output_type).unwrap_err();
                assert!(error.contains("unsupported numeric type"), "{error}");
            }
        }
    }
}
