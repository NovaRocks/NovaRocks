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

//! Real LocalCompiler Date32 float profiles, native literal projection and host first-failure evidence.
use super::*;
use arrow::array::{Date32Array, Float32Array, Float64Array};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
// Proleptic Gregorian day offsets from 1970-01-01: 2024-02-29 is
// 19_723 + 31 + 28 = 19_782; astronomical -0001-12-31 is -719_529.
// The date -> float contract encodes year*10000 + month*100 + day, so the
// latter date has coefficient -10_000 + 1_200 + 31 = -8_769. Day 71 is
// 1970-03-13 (31 January days + 28 February days + 12).
// Expected IEEE bits are independently rounded from those integer
// coefficients. At these positive Float32 magnitudes the ULP is 2: ties
// round to even, giving 19700100, 20240228, 19691232 and 19700312.
// -8769 is exact in both formats; all coefficients are exact in Float64.
// No runtime conversion, Arrow cast, or retired evaluator generates them.
fn expected_date_floats(target: &DataType, coefficients: &[Option<i32>]) -> ArrayRef {
    match target {
        DataType::Float32 => Arc::new(Float32Array::from(
            coefficients
                .iter()
                .map(|value| {
                    value.map(|value| {
                        f32::from_bits(match value {
                            19_700_101 => 0x4b96_4cc2,
                            20_240_229 => 0x4b9a_6bb2,
                            19_691_231 => 0x4b96_3b70,
                            -8_769 => 0xc609_0400,
                            19_700_313 => 0x4b96_4d2c,
                            _ => panic!("coefficient absent from independent IEEE table"),
                        })
                    })
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            coefficients
                .iter()
                .map(|value| {
                    value.map(|value| {
                        f64::from_bits(match value {
                            19_700_101 => 0x4172_c998_5000_0000,
                            20_240_229 => 0x4173_4d76_5000_0000,
                            19_691_231 => 0x4172_c76d_f000_0000,
                            -8_769 => 0xc0c1_2080_0000_0000,
                            19_700_313 => 0x4172_c9a5_9000_0000,
                            _ => panic!("coefficient absent from independent IEEE table"),
                        })
                    })
                })
                .collect::<Vec<_>>(),
        )),
        _ => panic!("exact date-to-float profile"),
    }
}
fn date_batch(program: &CompiledFixture, dates: ArrayRef) -> RecordBatch {
    let count = dates.len();
    let schema = program.input_schema().clone();
    RecordBatch::try_new(
        schema,
        vec![
            dates,
            Arc::new(Int64Array::from(vec![42; count])),
            Arc::new(BooleanArray::from(vec![true; count])),
        ],
    )
    .unwrap()
}

#[test]
fn date_float_actual_compiler_selected_error_origin_null_placeholder_and_unselected_panic() {
    for target in [DataType::Float32, DataType::Float64] {
        let program = compiled(
            FunctionValueType::new(DataType::Date32, true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let source: ArrayRef = Arc::new(Date32Array::new(
            vec![i32::MAX, 0, i32::MIN, i32::MAX, -1].into(),
            Some(arrow::buffer::NullBuffer::from(vec![
                true, true, true, false, true,
            ])),
        ));
        let batch = date_batch(&program, source);
        let rows = [1, 2, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let result = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &batch,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(result.errors().len(), 1);
        assert_eq!(result.errors()[0].selected_ordinal(), 1);
        assert_eq!(
            result.errors()[0].message(),
            format!("CAST failed: from Date32 to {target:?}: invalid Date32 value -2147483648")
        );
        let expected =
            expected_date_floats(&target, &[Some(19_700_101), None, None, Some(19_691_231)]);
        assert_eq!(result.values().to_data(), expected.to_data());
    }
}
#[test]
fn date_float_actual_compiler_every_runtime_cause_keeps_first_failure_without_replay() {
    for target in [DataType::Float32, DataType::Float64] {
        let program = compiled(
            FunctionValueType::new(DataType::Date32, true),
            FunctionValueType::new(target, true),
            Source::Column,
            Wrap::Bare,
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let source: ArrayRef = Arc::new(Date32Array::from(vec![Some(0), Some(i32::MIN), None]));
        let batch = date_batch(&program, source);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        assert_eq!(
            instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                    ScalarInvocationActivation::Activated,
                    &recorder,
                    &mut PassThroughCallHost
                )
                .unwrap()
                .errors()
                .len(),
            1
        );
        let trace = recorder.trace();
        assert!(!trace.is_empty());
        assert!(trace.iter().all(|n| *n <= 256));
        for at in 1..=trace.len() {
            for cause in causes() {
                let control = CallbackControl::new(cause.clone(), at);
                let mut evaluator = instance(&program);
                assert!(
                    matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(error)) if error==cause)
                );
                assert_eq!(control.trace(), trace[..at]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                        ScalarInvocationActivation::Activated,
                        &after,
                        &mut PassThroughCallHost
                    ),
                    Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
                ));
                assert!(after.trace().is_empty());
            }
        }
    }
}
