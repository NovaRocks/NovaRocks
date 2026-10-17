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
use crate::exec::expr::decimal::{div_round_i128, pow10_i128};
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::{Array, ArrayRef, Decimal128Array, Float64Array, Int64Array};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use novarocks_functions::builtin::round_core::round_float_to_integer as round_double_to_int;
use std::sync::Arc;

/// Round a double value to d decimal places.
/// Returns DOUBLE.
/// Implementation matches StarRocks double_round function.
fn round_double_to_decimals(value: f64, decimals: i64) -> f64 {
    // Handle negative decimals (round to tens, hundreds, etc.)
    let dec_negative = decimals < 0;
    let abs_dec = if dec_negative { -decimals } else { decimals } as u64;

    // Pre-compute 10^abs_dec
    let tmp = if abs_dec < 10 {
        // Fast path for small exponents
        let mut result = 1.0;
        for _ in 0..abs_dec {
            result *= 10.0;
        }
        result
    } else {
        10.0_f64.powi(abs_dec as i32)
    };

    // Pre-compute these to avoid optimization issues
    let value_div_tmp = value / tmp;
    let value_mul_tmp = value * tmp;

    // Handle infinity cases
    if dec_negative && tmp.is_infinite() {
        return 0.0;
    }
    if !dec_negative && value_mul_tmp.is_infinite() {
        return value;
    }

    // Round using f64::round
    if dec_negative {
        value_div_tmp.round() * tmp
    } else {
        value_mul_tmp.round() / tmp
    }
}

/// Round a decimal value to d decimal places.
/// Returns Decimal128 with the target scale.
fn round_decimal(value: i128, original_scale: i8, target_scale: i8) -> Result<i128, String> {
    let scale_diff = target_scale as i32 - original_scale as i32;

    if scale_diff == 0 {
        // No rounding needed
        Ok(value)
    } else if scale_diff > 0 {
        // Scale up: multiply by 10^scale_diff
        let factor = pow10_i128(scale_diff as usize)?;
        value
            .checked_mul(factor)
            .ok_or_else(|| "decimal overflow in round".to_string())
    } else {
        // Scale down: divide by 10^(-scale_diff) with rounding
        let factor = pow10_i128((-scale_diff) as usize)?;
        Ok(div_round_i128(value, factor))
    }
}

/// Evaluate round function.
/// Supports:
/// - round(x): round double to nearest integer (returns BIGINT)
/// - round(x, d): round double to d decimal places (returns DOUBLE)
/// - round(decimal, d): round decimal to d decimal places (returns Decimal128)
pub fn eval_round<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    value_expr: ExprId,
    decimals_expr: Option<ExprId>,
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    let value_arr = arena.eval(value_expr, chunk)?;
    let output_type = arena
        .data_type(expr)
        .ok_or_else(|| "round: missing output type".to_string())?;

    // Handle single-argument round(x) - round to nearest integer
    if decimals_expr.is_none() {
        // Single argument: round to integer
        match value_arr.data_type() {
            DataType::Float64 | DataType::Float32 => {
                let f64_arr = if matches!(value_arr.data_type(), DataType::Float64) {
                    value_arr
                } else {
                    cast(&value_arr, &DataType::Float64)
                        .map_err(|e| format!("round: failed to cast to Float64: {}", e))?
                };
                let f64_arr = f64_arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "round: failed to downcast to Float64Array".to_string())?;

                let len = f64_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if f64_arr.is_null(i) {
                        values.push(None);
                    } else {
                        let v = f64_arr.value(i);
                        values.push(Some(round_double_to_int(v)));
                    }
                }
                Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
            }
            DataType::Decimal128(_, _) => {
                // For decimal, round to 0 decimal places (nearest integer)
                let dec_arr = value_arr
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "round: failed to downcast to Decimal128Array".to_string())?;
                let (_, original_scale) = match value_arr.data_type() {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => unreachable!(),
                };
                let (out_precision, out_scale) = match output_type {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err((format!(
                            "round: expected Decimal128 output type, got {:?}",
                            output_type
                        ))
                        .into());
                    }
                };

                let len = dec_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if dec_arr.is_null(i) {
                        values.push(None);
                    } else {
                        let v = dec_arr.value(i);
                        let rounded = round_decimal(v, original_scale, out_scale)?;
                        values.push(Some(rounded));
                    }
                }
                let array = Decimal128Array::from(values)
                    .with_precision_and_scale(out_precision, out_scale)
                    .map_err(|e| format!("round: failed to create Decimal128Array: {}", e))?;
                Ok(Arc::new(array) as ArrayRef)
            }
            _ => {
                // Cast to Float64 and round
                let f64_arr = cast(&value_arr, &DataType::Float64)
                    .map_err(|e| format!("round: failed to cast to Float64: {}", e))?;
                let f64_arr = f64_arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "round: failed to downcast to Float64Array".to_string())?;

                let len = f64_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if f64_arr.is_null(i) {
                        values.push(None);
                    } else {
                        let v = f64_arr.value(i);
                        values.push(Some(round_double_to_int(v)));
                    }
                }
                Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
            }
        }
    } else {
        // Two arguments: round(x, d)
        let Some(decimals_expr) = decimals_expr else {
            return Err(("round: missing decimal argument".to_string()).into());
        };
        let decimals_arr = arena.eval(decimals_expr, chunk)?;

        // Get length from value_arr
        let len = value_arr.len();

        // Check if decimals is constant (length 1) or per-row (length matches value)
        let decimals_is_constant = decimals_arr.len() == 1;
        if !decimals_is_constant && decimals_arr.len() != len {
            return Err((format!(
                "round: decimals array length {} does not match value array length {}",
                decimals_arr.len(),
                len
            ))
            .into());
        }

        // Cast decimals to Int64 for processing
        let decimals_i64_arr = if matches!(decimals_arr.data_type(), DataType::Int64) {
            decimals_arr
        } else {
            cast(&decimals_arr, &DataType::Int64)
                .map_err(|e| format!("round: failed to cast decimals to Int64: {}", e))?
        };
        let decimals_i64 = decimals_i64_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "round: failed to downcast decimals to Int64Array".to_string())?;

        match value_arr.data_type() {
            DataType::Decimal128(_, _) => {
                // Decimal round
                let dec_arr = value_arr
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| "round: failed to downcast to Decimal128Array".to_string())?;
                let (_, original_scale) = match value_arr.data_type() {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => unreachable!(),
                };
                let (out_precision, out_scale) = match output_type {
                    DataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err((format!(
                            "round: expected Decimal128 output type, got {:?}",
                            output_type
                        ))
                        .into());
                    }
                };

                // Clamp target_scale to valid range
                let max_precision = 38i8;
                let len = dec_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if dec_arr.is_null(i)
                        || decimals_i64.is_null(if decimals_is_constant { 0 } else { i })
                    {
                        values.push(None);
                        continue;
                    }
                    let decimals_value =
                        decimals_i64.value(if decimals_is_constant { 0 } else { i });
                    let target_scale = if decimals_value > max_precision as i64 {
                        max_precision
                    } else if decimals_value < -(max_precision as i64) {
                        -max_precision
                    } else {
                        decimals_value as i8
                    };
                    let v = dec_arr.value(i);
                    let rounded = round_decimal(v, original_scale, target_scale)?;
                    // Adjust to output scale: round_decimal returns value
                    // in target_scale, but output array uses out_scale.
                    let adjusted = if target_scale < out_scale {
                        let factor = pow10_i128((out_scale - target_scale) as usize)?;
                        rounded
                            .checked_mul(factor)
                            .ok_or("decimal overflow in round adjust")?
                    } else if target_scale > out_scale {
                        let factor = pow10_i128((target_scale - out_scale) as usize)?;
                        div_round_i128(rounded, factor)
                    } else {
                        rounded
                    };
                    values.push(Some(adjusted));
                }
                let array = Decimal128Array::from(values)
                    .with_precision_and_scale(out_precision, out_scale)
                    .map_err(|e| format!("round: failed to create Decimal128Array: {}", e))?;
                Ok(Arc::new(array) as ArrayRef)
            }
            _ => {
                // Double round
                let f64_arr = if matches!(value_arr.data_type(), DataType::Float64) {
                    value_arr
                } else {
                    cast(&value_arr, &DataType::Float64)
                        .map_err(|e| format!("round: failed to cast to Float64: {}", e))?
                };
                let f64_arr = f64_arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| "round: failed to downcast to Float64Array".to_string())?;

                let len = f64_arr.len();
                let mut values = Vec::with_capacity(len);
                for i in 0..len {
                    if f64_arr.is_null(i)
                        || decimals_i64.is_null(if decimals_is_constant { 0 } else { i })
                    {
                        values.push(None);
                        continue;
                    }
                    let v = f64_arr.value(i);
                    let decimals_value =
                        decimals_i64.value(if decimals_is_constant { 0 } else { i });
                    values.push(Some(round_double_to_decimals(v, decimals_value)));
                }
                Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
            }
        }
    }
}

#[cfg(test)]
mod legacy_rounding_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{ExprNode, LiteralValue};
    use arrow::array::types::{Int8Type, Int16Type};
    use arrow::array::{
        BooleanArray, DictionaryArray, FixedSizeListArray, Int8Array, Int16Array, RunArray,
        StringArray, UInt64Array,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    enum Digits {
        Column(ArrayRef),
        Literal(LiteralValue),
    }

    fn evaluate(value: ArrayRef, digits: Option<Digits>, output_type: DataType) -> ArrayRef {
        evaluate_result(value, digits, output_type).unwrap()
    }

    fn evaluate_result(
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
                kind: FunctionKind::Round,
                args,
            },
            output_type.clone(),
        );
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)?;
        assert_eq!(output.data_type(), &output_type);
        Ok(output)
    }

    fn floats(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values))
    }

    fn integers(output: &ArrayRef) -> Vec<Option<i64>> {
        output
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
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
    fn unary_round_uses_rust_integer_saturation_and_nan_zero() {
        let output = evaluate(
            floats(vec![
                Some(2.5),
                Some(-2.5),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(9_223_372_036_854_775_808.0),
                Some(-0.0),
                None,
            ]),
            None,
            DataType::Int64,
        );
        assert_eq!(
            integers(&output),
            vec![
                Some(3),
                Some(-3),
                Some(0),
                Some(i64::MAX),
                Some(i64::MIN),
                Some(i64::MAX),
                Some(0),
                None
            ]
        );
    }

    #[test]
    fn binary_round_retains_nonfinite_and_huge_factor_branches() {
        let output = evaluate(
            floats(vec![
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(f64::NAN),
                Some(2.0),
                Some(-2.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
            ]),
            Some(Digits::Column(Arc::new(Int64Array::from(vec![
                0, 0, 0, 400, -400, -400, -400,
            ])))),
            DataType::Float64,
        );
        let values = doubles(&output);
        assert_eq!(values[0], Some(f64::INFINITY));
        assert_eq!(values[1], Some(f64::NEG_INFINITY));
        assert!(values[2].unwrap().is_nan());
        assert_eq!(values[3], Some(2.0));
        for value in &values[4..] {
            assert_eq!(value.unwrap().to_bits(), 0);
        }
    }

    #[test]
    fn direct_decimal_round_preserves_unary_scale_and_rescales_binary_raw_values() {
        let input = Arc::new(
            Decimal128Array::from(vec![Some(125), Some(-125), Some(124), Some(-124), None])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        ) as ArrayRef;
        assert_eq!(
            decimals(&evaluate(input.clone(), None, DataType::Decimal128(38, 2))),
            vec![Some(125), Some(-125), Some(124), Some(-124), None]
        );
        assert_eq!(
            decimals(&evaluate(
                input.clone(),
                Some(Digits::Literal(LiteralValue::Int64(1))),
                DataType::Decimal128(38, 1)
            )),
            vec![Some(13), Some(-13), Some(12), Some(-12), None]
        );
        assert_eq!(
            decimals(&evaluate(
                input,
                Some(Digits::Column(Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(0),
                    Some(-1),
                    None,
                    Some(1)
                ])))),
                DataType::Decimal128(38, 1)
            )),
            vec![Some(13), Some(-10), Some(0), None, None]
        );
    }

    #[test]
    fn round_literal_and_column_digits_keep_strict_null_and_equal_values() {
        let input = floats(vec![Some(1.25), Some(-1.25), None]);
        let expected = vec![Some(1.3), Some(-1.3), None];
        for digits in [
            Digits::Literal(LiteralValue::Int64(1)),
            Digits::Column(Arc::new(Int64Array::from(vec![1, 1, 1]))),
        ] {
            assert_eq!(
                doubles(&evaluate(input.clone(), Some(digits), DataType::Float64)),
                expected
            );
        }
        assert_eq!(
            doubles(&evaluate(
                input.clone(),
                Some(Digits::Column(Arc::new(Int64Array::from(vec![
                    Some(1),
                    None,
                    Some(1)
                ])))),
                DataType::Float64
            )),
            vec![Some(1.3), None, None]
        );
        assert_eq!(
            doubles(&evaluate(
                input,
                Some(Digits::Literal(LiteralValue::Null)),
                DataType::Float64
            )),
            vec![None; 3]
        );
        assert_eq!(
            doubles(&evaluate(
                floats(vec![Some(3.75)]),
                Some(Digits::Column(floats(vec![Some(1e100)]))),
                DataType::Float64,
            )),
            vec![None]
        );
    }

    #[test]
    fn round_uses_real_unsigned_text_boolean_and_encoded_arrow_casts() {
        let cases: Vec<(ArrayRef, Vec<Option<i64>>)> = vec![
            (
                Arc::new(UInt64Array::from(vec![Some(2), Some(u64::MAX), None])),
                vec![Some(2), Some(i64::MAX), None],
            ),
            (
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                vec![Some(1), Some(0), None],
            ),
            (
                Arc::new(StringArray::from(vec![
                    Some("2.5"),
                    Some("bad"),
                    Some("-2.5"),
                ])),
                vec![Some(3), None, Some(-3)],
            ),
            (
                Arc::new(
                    DictionaryArray::<Int8Type>::try_new(
                        Int8Array::from(vec![Some(0), Some(1), None]),
                        Arc::new(StringArray::from(vec!["2.5", "-2.5"])),
                    )
                    .unwrap(),
                ),
                vec![Some(3), Some(-3), None],
            ),
            (
                Arc::new(
                    RunArray::<Int16Type>::try_new(
                        &Int16Array::from(vec![2, 4]),
                        &Float64Array::from(vec![2.5, -2.5]),
                    )
                    .unwrap(),
                ),
                vec![Some(3), Some(3), Some(-3), Some(-3)],
            ),
            (
                Arc::new(
                    FixedSizeListArray::try_new(
                        Arc::new(Field::new("item", DataType::Float64, true)),
                        1,
                        floats(vec![Some(2.5), Some(-2.5), None]),
                        None,
                    )
                    .unwrap(),
                ),
                vec![Some(3), Some(-3), None],
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(integers(&evaluate(input, None, DataType::Int64)), expected);
        }
    }

    #[test]
    fn legacy_decimal_round_can_publish_a_raw_value_above_declared_precision() {
        // This is the existing precision hole, not an expected checked overflow
        // policy for the new owner. No I64::MIN digits panic is used as an oracle.
        let bound = 10_i128.pow(38);
        let input = Arc::new(
            Decimal128Array::from(vec![bound - 1])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        let output = evaluate(
            input,
            Some(Digits::Literal(LiteralValue::Int64(-38))),
            DataType::Decimal128(38, 0),
        );
        assert_eq!(decimals(&output), vec![Some(bound)]);
        assert!(decimals(&output)[0].unwrap() > bound - 1);
    }

    #[test]
    fn round_interval_digits_report_real_outer_cast_failure_despite_arrow_capability() {
        use arrow::array::types::IntervalDayTimeType;
        use arrow::array::{IntervalDayTimeArray, IntervalYearMonthArray};
        let intervals: Vec<ArrayRef> = vec![
            Arc::new(IntervalYearMonthArray::from(vec![Some(1), None])),
            Arc::new(IntervalDayTimeArray::from(vec![
                Some(IntervalDayTimeType::make_value(1, 0)),
                None,
            ])),
        ];
        for digits in intervals {
            assert!(arrow::compute::can_cast_types(
                digits.data_type(),
                &DataType::Int64
            ));
            let error = evaluate_result(
                floats(vec![Some(1.25), None]),
                Some(Digits::Column(digits)),
                DataType::Float64,
            )
            .unwrap_err();
            assert!(
                error.contains("round: failed to cast decimals to Int64:"),
                "{error}"
            );
            assert!(error.contains("Interval"), "{error}");
        }
    }

    fn overflowing_negative_scale_digits(all_null: bool) -> Vec<(ArrayRef, i8)> {
        use arrow::array::{Decimal32Array, Decimal64Array, Decimal256Array};
        use arrow::datatypes::i256;
        let i32_value = if all_null { None } else { Some(0_i32) };
        let i64_value = if all_null { None } else { Some(0_i64) };
        let i128_value = if all_null { None } else { Some(0_i128) };
        let i256_value = if all_null {
            None
        } else {
            Some(i256::from_i128(0))
        };
        vec![
            (
                Arc::new(
                    Decimal32Array::from(vec![i32_value, None])
                        .with_precision_and_scale(9, -10)
                        .unwrap(),
                ),
                -10,
            ),
            (
                Arc::new(
                    Decimal64Array::from(vec![i64_value, None])
                        .with_precision_and_scale(18, -19)
                        .unwrap(),
                ),
                -19,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![i128_value, None])
                        .with_precision_and_scale(38, -39)
                        .unwrap(),
                ),
                -39,
            ),
            (
                Arc::new(
                    Decimal256Array::from(vec![i256_value, None])
                        .with_precision_and_scale(76, -77)
                        .unwrap(),
                ),
                -77,
            ),
        ]
    }

    #[test]
    fn round_all_null_decimal_digits_still_fail_static_native_factor_construction() {
        for (digits, scale) in overflowing_negative_scale_digits(true) {
            assert!(arrow::compute::can_cast_types(
                digits.data_type(),
                &DataType::Int64
            ));
            let error = evaluate_result(
                floats(vec![Some(1.25), Some(2.5)]),
                Some(Digits::Column(digits)),
                DataType::Float64,
            )
            .unwrap_err();
            assert!(
                error.contains("round: failed to cast decimals to Int64:"),
                "{error}"
            );
            assert!(
                error.contains(&format!("The scale {scale} causes overflow.")),
                "{error}"
            );
        }
    }

    #[test]
    fn round_null_source_does_not_hide_decimal_digits_static_factor_failure() {
        for all_null in [false, true] {
            for (digits, scale) in overflowing_negative_scale_digits(all_null) {
                let error = evaluate_result(
                    floats(vec![None, None]),
                    Some(Digits::Column(digits)),
                    DataType::Float64,
                )
                .unwrap_err();
                assert!(
                    error.contains("round: failed to cast decimals to Int64:"),
                    "{error}"
                );
                assert!(
                    error.contains(&format!("The scale {scale} causes overflow.")),
                    "{error}"
                );
            }
        }
    }

    #[test]
    fn round_decimal32_digits_native_multiplication_overflow_is_safe_null_even_when_i64_fits() {
        use arrow::array::Decimal32Array;
        let digits: ArrayRef = Arc::new(
            Decimal32Array::from(vec![Some(300_000_000_i32), Some(1), None])
                .with_precision_and_scale(9, -1)
                .unwrap(),
        );
        // The mathematical integer 3,000,000,000 fits BIGINT; Arrow first
        // multiplies in the source i32 carrier, whose safe cast produces NULL.
        let output = evaluate(
            floats(vec![Some(1.25), Some(1.25), Some(1.25)]),
            Some(Digits::Column(digits.clone())),
            DataType::Float64,
        );
        assert_eq!(doubles(&output), vec![None, Some(1.25), None]);
        // The same legal decimal used as the value follows the Float64 path.
        assert_eq!(
            integers(&evaluate(digits, None, DataType::Int64)),
            vec![Some(3_000_000_000), Some(10), None]
        );
    }
}
