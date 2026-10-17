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
#[cfg(test)]
use super::common::{NumericArrayView, value_at_f64};
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::{Array, Float64Array, StringArray, TimestampMicrosecondArray};
#[cfg(test)]
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::builtin::scalar_extrema::{self, ExtremaOperation};
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
fn datetime_value_at(
    values: &[Option<chrono::NaiveDateTime>],
    row: usize,
    len: usize,
) -> Option<chrono::NaiveDateTime> {
    scalar_extrema::datetime_value_at(values, row, len)
}
fn eval_greatest_least<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
    greatest: bool,
) -> Result<ArrayRef, __Eval::Error> {
    // Legacy owns eager child evaluation and its original output carrier lookup.
    let mut arrays = Vec::with_capacity(args.len());
    for arg in args {
        arrays.push(arena.eval(*arg, chunk)?);
    }
    Ok((scalar_extrema::evaluate_legacy(
        if greatest {
            ExtremaOperation::Greatest
        } else {
            ExtremaOperation::Least
        },
        &arrays,
        chunk.len(),
        arena.data_type(expr),
    ))?)
}
pub fn eval_greatest<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_greatest_least(arena, expr, args, chunk, true))?)
}
pub fn eval_least<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, __Eval::Error> {
    Ok((eval_greatest_least(arena, expr, args, chunk, false))?)
}
#[cfg(test)]
mod legacy_extrema_contract_tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::expr::function::FunctionKind;
    use crate::exec::expr::{DecimalOverflowPolicy, ExprNode};
    use arrow::array::{
        Date32Array, Decimal128Array, Int64Array, TimestampNanosecondArray, UInt8Array,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;

    fn evaluate(
        name: &'static str,
        inputs: Vec<ArrayRef>,
        result_type: DataType,
        child_policy: Option<DecimalOverflowPolicy>,
    ) -> Result<ArrayRef, String> {
        let fields = inputs
            .iter()
            .enumerate()
            .map(|(i, array)| Field::new(format!("v{i}"), array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let types = inputs
            .iter()
            .map(|array| array.data_type().clone())
            .collect::<Vec<_>>();
        let slots = (1..=inputs.len())
            .map(|i| SlotId::new(i as u32))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), inputs).unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        let mut arena = ExprArena::default();
        arena.set_allow_throw_exception(child_policy == Some(DecimalOverflowPolicy::ReportError));
        arena.set_session_time_zone(Some("America/New_York".into()));
        let args = slots
            .into_iter()
            .zip(types)
            .map(|(slot, ty)| {
                let source = arena.push_typed(ExprNode::SlotId(slot), ty.clone());
                match child_policy {
                    Some(policy) => {
                        let cast = arena.push_typed(ExprNode::Cast(source, policy), ty);
                        assert_eq!(arena.decimal_overflow_policy(cast), Some(policy));
                        cast
                    }
                    None => source,
                }
            })
            .collect();
        // The legacy function ABI has canonical Math kind and an exact output
        // carrier, but no new dynamic binding receipt or call-policy slot.
        let call = arena.push_typed(
            ExprNode::FunctionCall {
                kind: FunctionKind::Math(name),
                args,
            },
            result_type.clone(),
        );
        assert_eq!(arena.decimal_overflow_policy(call), None);
        let frozen = arena.into_immutable().unwrap();
        let output = ExprArena::from_immutable(&frozen)
            .expect("legacy frozen expression fixture")
            .eval(call, &chunk)?;
        assert_eq!(output.data_type(), &result_type);
        Ok(output)
    }

    fn integers(output: &ArrayRef) -> Vec<Option<i64>> {
        output
            .as_any()
            .downcast_ref::<Int64Array>()
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
    fn text(output: &ArrayRef) -> Vec<Option<&str>> {
        output
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect()
    }

    fn resolve_extrema_binding(
        name: &str,
        sources: &[novarocks_functions::FunctionValueType],
    ) -> novarocks_functions::ResolvedFunctionBinding {
        use novarocks_functions::{
            FunctionArgument, FunctionBindingRequest, FunctionKind as BindingKind,
        };
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct TestControl;
        impl PureCompileControl for TestControl {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                assert!(units <= 256);
                Ok(())
            }
        }
        let catalog =
            novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
                .unwrap();
        let arguments = sources
            .iter()
            .map(|value_type| FunctionArgument::Value {
                value_type: value_type.clone(),
                constant: None,
            })
            .collect::<Vec<_>>();
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: arguments.len(),
        };
        let bound = catalog
            .resolve_bound_user(name, BindingKind::Scalar, request, &TestControl)
            .unwrap();
        catalog
            .validate_bound(&bound, request, &TestControl)
            .unwrap();
        assert_eq!(
            bound.selected.overload.as_str(),
            format!("builtin.scalar/{name}/dynamic-v1")
        );
        assert_eq!(
            bound.selected.argument_types.as_ref(),
            arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect::<Vec<_>>()
                .as_slice()
        );
        bound
    }

    #[test]
    fn legacy_extrema_json_binding_retains_domain_while_old_utf8_entry_formats_dates() {
        use novarocks_functions::{FunctionResultType, FunctionValueType};
        use novarocks_type_contract::ValueLogicalType;
        let source =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        // These numeric strings and "null" are valid JSON source values.
        let left = Arc::new(StringArray::from(vec![
            Some("20260114"),
            Some("null"),
            None,
        ])) as ArrayRef;
        let right = Arc::new(StringArray::from(vec![
            Some("20260111"),
            Some("20260115"),
            Some("20260115"),
        ])) as ArrayRef;
        for (name, first) in [
            ("greatest", "2026-01-14 00:00:00"),
            ("least", "2026-01-11 00:00:00"),
        ] {
            let bound = resolve_extrema_binding(name, &[source.clone(), source.clone()]);
            let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
                panic!("scalar result required");
            };
            assert_eq!(result.logical_type(), ValueLogicalType::Json);
            assert_eq!(result.data_type().clone(), DataType::Utf8);
            assert!(result.nullable());
            // The legacy Arena only carries Utf8. This records its non-JSON
            // formatted output, without inventing a nominal Arena receipt.
            let output = evaluate(
                name,
                vec![left.clone(), right.clone()],
                result.data_type().clone(),
                None,
            )
            .unwrap();
            assert_eq!(text(&output), vec![Some(first), None, None]);
        }
    }

    #[test]
    fn legacy_extrema_two_decimal128_sources_produce_actual_bound_decimal256_result() {
        use arrow::array::Decimal256Array;
        use arrow::datatypes::i256;
        use novarocks_functions::{FunctionResultType, FunctionValueType};
        let sources = [
            FunctionValueType::new(DataType::Decimal128(38, 0), true),
            FunctionValueType::new(DataType::Decimal128(38, 1), true),
        ];
        let left = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(-1), None])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ) as ArrayRef;
        let right = Arc::new(
            Decimal128Array::from(vec![Some(20), Some(-20), Some(20)])
                .with_precision_and_scale(38, 1)
                .unwrap(),
        ) as ArrayRef;
        for (name, expected) in [
            ("greatest", vec![Some(20), Some(-10), None]),
            ("least", vec![Some(10), Some(-20), None]),
        ] {
            let bound = resolve_extrema_binding(name, &sources);
            let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
                panic!("scalar result required");
            };
            assert_eq!(result.data_type().clone(), DataType::Decimal256(39, 1));
            assert!(result.nullable());
            let output = evaluate(
                name,
                vec![left.clone(), right.clone()],
                result.data_type().clone(),
                None,
            )
            .unwrap();
            let actual = output
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            assert_eq!(
                actual,
                expected
                    .into_iter()
                    .map(|value| value.map(i256::from_i128))
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn legacy_extrema_i64_keeps_double_precision_loss_and_safe_overflow_null() {
        for name in ["greatest", "least"] {
            let input = Arc::new(Int64Array::from(vec![
                Some(9_007_199_254_740_993),
                Some(-9_007_199_254_740_993),
                Some(i64::MAX),
                Some(i64::MIN),
                None,
            ]));
            assert_eq!(
                integers(&evaluate(name, vec![input], DataType::Int64, None).unwrap()),
                vec![
                    Some(9_007_199_254_740_992),
                    Some(-9_007_199_254_740_992),
                    None,
                    Some(i64::MIN),
                    None
                ]
            );
        }
    }

    #[test]
    fn legacy_extrema_decimal_p38_safe_null_is_independent_of_child_overflow_policy() {
        let maximum = 10_i128.pow(38) - 1;
        for name in ["greatest", "least"] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let input = Arc::new(
                    // Binary64 decode/re-encode at this legal negative scale
                    // exceeds 10^38; scale zero instead remains below 10^38.
                    Decimal128Array::from(vec![Some(maximum), Some(-maximum), Some(0), None])
                        .with_precision_and_scale(38, -46)
                        .unwrap(),
                );
                assert_eq!(
                    decimals(
                        &evaluate(
                            name,
                            vec![input],
                            DataType::Decimal128(38, -46),
                            Some(policy)
                        )
                        .unwrap()
                    ),
                    vec![None, None, Some(0), None]
                );
            }
        }
    }

    #[test]
    fn legacy_extrema_decimal_arguments_decode_their_own_positive_and_negative_scales() {
        let left = Arc::new(
            Decimal128Array::from(vec![Some(123), Some(-123), None])
                .with_precision_and_scale(5, 2)
                .unwrap(),
        ) as ArrayRef;
        let right = Arc::new(
            Decimal128Array::from(vec![Some(1), Some(-1), Some(1)])
                .with_precision_and_scale(3, -2)
                .unwrap(),
        ) as ArrayRef;
        for (name, expected) in [
            ("greatest", vec![Some(10_000), Some(-123), None]),
            ("least", vec![Some(123), Some(-10_000), None]),
        ] {
            assert_eq!(
                decimals(
                    &evaluate(
                        name,
                        vec![left.clone(), right.clone()],
                        DataType::Decimal128(38, 2),
                        None
                    )
                    .unwrap()
                ),
                expected
            );
        }
    }

    #[test]
    fn legacy_extrema_utf8_parses_dates_and_reformats_instead_of_comparing_spelling() {
        let left = Arc::new(StringArray::from(vec![
            Some("20260114"),
            Some("2026-01-13 20:01:02.999"),
            Some("z"),
        ])) as ArrayRef;
        let right = Arc::new(StringArray::from(vec![
            Some("2026-01-11"),
            Some("2026-01-14 00:00:00"),
            Some("a"),
        ])) as ArrayRef;
        for (name, expected) in [
            (
                "greatest",
                vec![
                    Some("2026-01-14 00:00:00"),
                    Some("2026-01-14 00:00:00"),
                    None,
                ],
            ),
            (
                "least",
                vec![
                    Some("2026-01-11 00:00:00"),
                    Some("2026-01-13 20:01:02"),
                    None,
                ],
            ),
        ] {
            let output = evaluate(
                name,
                vec![left.clone(), right.clone()],
                DataType::Utf8,
                None,
            )
            .unwrap();
            assert_eq!(text(&output), expected);
        }
    }

    #[test]
    fn legacy_extrema_date32_returns_datetime_microseconds_with_strict_null() {
        let left = Arc::new(Date32Array::from(vec![Some(-1), Some(0), Some(1), None])) as ArrayRef;
        let right =
            Arc::new(Date32Array::from(vec![Some(0), Some(-1), Some(2), Some(1)])) as ArrayRef;
        for (name, expected) in [
            (
                "greatest",
                vec![Some(0), Some(0), Some(172_800_000_000), None],
            ),
            (
                "least",
                vec![
                    Some(-86_400_000_000),
                    Some(-86_400_000_000),
                    Some(86_400_000_000),
                    None,
                ],
            ),
        ] {
            let output = evaluate(
                name,
                vec![left.clone(), right.clone()],
                DataType::Timestamp(TimeUnit::Microsecond, None),
                None,
            )
            .unwrap();
            let actual = output
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn legacy_extrema_timestamp_read_uses_utc_then_output_cast_adjusts_zone_or_fails() {
        let left = Arc::new(
            TimestampNanosecondArray::from(vec![Some(-1), Some(1001), Some(1_234_567_899), None])
                .with_timezone("Europe/Berlin"),
        ) as ArrayRef;
        let right = Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(-1001),
                Some(999),
                Some(1_234_567_891),
                Some(0),
            ])
            .with_timezone("Europe/Berlin"),
        ) as ArrayRef;
        for (name, expected) in [
            (
                "greatest",
                vec![Some(-1000), Some(1000), Some(1_234_567_000), None],
            ),
            (
                "least",
                vec![Some(-2000), Some(0), Some(1_234_567_000), None],
            ),
        ] {
            for timezone in [None, Some("+02:00")] {
                let output = evaluate(
                    name,
                    vec![left.clone(), right.clone()],
                    DataType::Timestamp(TimeUnit::Nanosecond, timezone.map(Into::into)),
                    None,
                )
                .unwrap();
                let actual = output
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                let adjustment = if timezone.is_some() {
                    7_200_000_000_000
                } else {
                    0
                };
                assert_eq!(
                    actual,
                    expected
                        .iter()
                        .map(|value| value.map(|value| value - adjustment))
                        .collect::<Vec<_>>()
                );
            }
            let named_zone = evaluate(
                name,
                vec![left.clone(), right.clone()],
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Europe/Berlin".into())),
                None,
            );
            // The Server dependency closure enables Arrow's chrono-tz
            // feature; Execution alone currently supports fixed offsets.
            if "Europe/Berlin"
                .parse::<arrow::array::timezone::Tz>()
                .is_ok()
            {
                let output = named_zone.unwrap();
                let actual = output
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual,
                    expected
                        .iter()
                        .map(|value| value.map(|value| value - 3_600_000_000_000))
                        .collect::<Vec<_>>()
                );
            } else {
                let error = named_zone.unwrap_err();
                assert!(error.starts_with("math: failed to cast output:"));
                assert!(error.contains("only offset based timezones supported without chrono-tz"));
            }
            let error = evaluate(
                name,
                vec![left.clone(), right.clone()],
                DataType::Timestamp(TimeUnit::Nanosecond, Some("Invalid/Zone".into())),
                None,
            )
            .unwrap_err();
            assert!(error.starts_with("math: failed to cast output:"));
        }
    }

    #[test]
    fn legacy_extrema_any_null_or_nonfinite_numeric_argument_nulls_the_row() {
        let left = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
            Some(1.0),
        ])) as ArrayRef;
        let right = Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(2.0),
            Some(2.0),
            Some(2.0),
            Some(2.0),
            None,
        ])) as ArrayRef;
        for (name, first) in [("greatest", 2.0), ("least", 1.0)] {
            let output = evaluate(
                name,
                vec![left.clone(), right.clone()],
                DataType::Float64,
                None,
            )
            .unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(first), None, None, None, None, None]
            );
        }
    }

    #[test]
    fn legacy_extrema_mixed_datetime_numeric_and_unsigned_domains_remain_outer_failures() {
        for name in ["greatest", "least"] {
            let numeric = Arc::new(Int64Array::from(vec![2])) as ArrayRef;
            for other in [
                Arc::new(Date32Array::from(vec![0])) as ArrayRef,
                Arc::new(StringArray::from(vec!["2026-01-01"])) as ArrayRef,
                Arc::new(UInt8Array::from(vec![1])) as ArrayRef,
            ] {
                let error = evaluate(name, vec![numeric.clone(), other], DataType::Float64, None)
                    .unwrap_err();
                assert!(error.contains("unsupported numeric type"), "{error}");
            }
            let output = evaluate(
                name,
                vec![numeric, Arc::new(Float64Array::from(vec![1.5]))],
                DataType::Float64,
                None,
            )
            .unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0),
                if name == "greatest" { 2.0 } else { 1.5 }
            );
        }
    }

    #[test]
    fn legacy_extrema_zero_argument_dispatch_rejects_even_though_raw_helper_returns_null() {
        let slot = SlotId::new(1);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "unused",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &[slot])
                .unwrap();
        let chunk = Chunk::new_with_chunk_schema(batch, schema);
        for name in ["greatest", "least"] {
            let mut arena = ExprArena::default();
            let call = arena.push_typed(
                ExprNode::FunctionCall {
                    kind: FunctionKind::Math(name),
                    args: vec![],
                },
                DataType::Float64,
            );
            let frozen = arena.into_immutable().unwrap();
            let arena =
                ExprArena::from_immutable(&frozen).expect("legacy frozen expression fixture");
            let error = arena.eval(call, &chunk).unwrap_err();
            assert!(
                error.contains("expects 1 to") && error.contains("got 0"),
                "{error}"
            );
            // This direct legacy helper intentionally bypasses the real
            // ExprArena arity gate; it is not evidence of admitted zero arity.
            let output = if name == "greatest" {
                eval_greatest(&arena, call, &[], &chunk)
            } else {
                eval_least(&arena, call, &[], &chunk)
            }
            .unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, None]
            );
        }
    }
}

#[cfg(test)]
#[path = "legacy_extrema_baseline_tests.rs"]
mod legacy_extrema_baseline_tests;
