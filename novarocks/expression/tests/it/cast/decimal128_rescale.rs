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

//! Actual LocalCompiler/Frame coverage with independent Decimal128 expectations.
//! Each corpus below states its raw output integers and overflow flags by hand.
//! No runtime reference calculator or old evaluator supplies expected values.
use super::*;
use arrow::array::Decimal128Array;
use novarocks_expression::testkit::{PassThroughCallHost, numeric_fixtures::compiled_cast};
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_failure_contract::kernel_reason::NUMERIC_OUT_OF_RANGE;
use novarocks_functions::{DataFailureReason, ScalarInvocationActivation, ScalarInvocationScope};
use novarocks_type_contract::TrinoCastMode;

const OVERFLOW: DataFailureReason = DataFailureReason::Domain(NUMERIC_OUT_OF_RANGE);

// Pure data constructors and inspectors are local to this retained test. They
// do not invoke a CAST or calculate a decimal result.
fn input(p: u8, s: i8, raw: Vec<Option<i128>>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(raw)
            .with_precision_and_scale(p, s)
            .unwrap(),
    )
}
fn values(array: &ArrayRef) -> Vec<Option<i128>> {
    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    (0..array.len())
        .map(|r| (!array.is_null(r)).then(|| array.value(r)))
        .collect()
}
#[derive(Clone, Copy, Debug)]
enum Expected {
    Value(i128),
    SourceNull,
    // An admitted value overflows the target precision.
    Overflow,
}
impl Expected {
    fn outcome(self, mode: TrinoCastMode) -> (Option<i128>, Option<DataFailureReason>) {
        match self {
            Self::Value(value) => (Some(value), None),
            Self::SourceNull => (None, None),
            Self::Overflow if mode == TrinoCastMode::Try => (None, None),
            Self::Overflow => (None, Some(OVERFLOW)),
        }
    }
}
use Expected::{Overflow as O, SourceNull as N, Value as V};

fn batch(program: &CompiledFixture, source: ArrayRef) -> RecordBatch {
    let n = source.len();
    RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            source,
            Arc::new(Int64Array::from(vec![42; n])),
            Arc::new(BooleanArray::from(vec![true; n])),
        ],
    )
    .unwrap()
}
fn compare(
    p: u8,
    s: i8,
    target: DataType,
    raw: Vec<Option<i128>>,
    expected_full: &[Expected],
    expected_constant: Expected,
) {
    assert_eq!(raw.len(), expected_full.len());
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        let full = input(p, s, raw.clone());
        let program = compiled_cast(
            FunctionValueType::new(full.data_type().clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        // Each variant carries the corresponding part of the same handwritten
        // result table, so row addresses and compact ordinals remain distinct.
        for (source, expected_source) in [
            (full.clone(), expected_full),
            (full.slice(1, full.len() - 1), &expected_full[1..]),
            (full.slice(0, 0), &expected_full[..0]),
        ] {
            let data = batch(&program, source.clone());
            for rows in [
                (0..source.len()).collect::<Vec<_>>(),
                (0..source.len()).filter(|r| r % 2 == 0).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(source.len(), &rows).unwrap();
                let out = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &data,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(out.selection(), selection);
                assert_eq!(out.values().data_type(), &target);
                let mut expected = Vec::new();
                let mut errors = Vec::new();
                for (ordinal, row) in rows.iter().copied().enumerate() {
                    let (value, error) = expected_source[row].outcome(mode);
                    expected.push(value);
                    if let Some(reason) = error {
                        errors.push((ordinal, reason));
                    }
                }
                assert_eq!(
                    values(out.values()),
                    expected,
                    "source Decimal128({p}, {s}), target {target:?}, mode {mode:?}, rows {rows:?}"
                );
                assert_eq!(
                    out.errors()
                        .iter()
                        .map(|e| (e.selected_ordinal(), e.reason()))
                        .collect::<Vec<_>>(),
                    errors,
                    "source Decimal128({p}, {s}), target {target:?}, mode {mode:?}, rows {rows:?}",
                );
            }
        }
        let constant = compiled_cast(
            FunctionValueType::new(full.data_type().clone(), false),
            FunctionValueType::new(target.clone(), true),
            Source::Constant,
            Wrap::Bare,
            mode,
            false,
        );
        let cb = batch(&constant, input(p, s, vec![Some(0); 5]));
        let selected = Selection::try_sparse(5, &[1, 4]).unwrap();
        let out = instance(&constant)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &cb,
                    source: None,
                },
                ScalarInvocationScope::new(selected, selected),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(out.selection(), selected);
        assert_eq!(out.values().data_type(), &target);
        // The shared compiled fixture's Source::Constant authors raw Decimal128
        // integer 1. Its profile-specific result is explicitly listed below.
        let (expected, error) = expected_constant.outcome(mode);
        assert_eq!(values(out.values()), vec![expected, expected]);
        let errors = match error {
            Some(reason) => vec![(0, reason), (1, reason)],
            None => vec![],
        };
        assert_eq!(
            out.errors()
                .iter()
                .map(|e| (e.selected_ordinal(), e.reason()))
                .collect::<Vec<_>>(),
            errors
        );
    }
}
#[test]
fn decimal128_rescale_actual_compiler_observed_7_2_to_9_3_both_modes_and_selection() {
    // A stored integer r at scale s means r * 10^-s. Raising scale
    // from 2 to 3 multiplies r by 10; all four outputs fit precision 9.
    compare(
        7,
        2,
        DataType::Decimal128(9, 3),
        vec![Some(12000), Some(9000), None, Some(-1)],
        &[V(120000), V(90000), N, V(-10)],
        V(10),
    );
}
#[test]
fn decimal128_rescale_actual_compiler_observed_14_13_to_38_13_both_modes_and_selection() {
    // Equal scales preserve raw integers; each fits the wider precision.
    compare(
        14,
        13,
        DataType::Decimal128(38, 13),
        vec![Some(1), Some(2), None, Some(-2)],
        &[V(1), V(2), N, V(-2)],
        V(1),
    );
}
#[test]
fn decimal128_rescale_actual_compiler_identity_preserves_valid_boundary_coefficients() {
    compare(
        1,
        0,
        DataType::Decimal128(1, 0),
        vec![Some(-9), Some(9), None, Some(0), Some(-1)],
        &[V(-9), V(9), N, V(0), V(-1)],
        V(1),
    );
}

#[test]
fn decimal128_rescale_actual_compiler_rejects_malformed_source_in_both_modes() {
    let source = input(
        1,
        0,
        vec![Some(i128::MIN), Some(i128::MAX), Some(-10), Some(10)],
    );
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        let program = compiled_cast(
            FunctionValueType::new(source.data_type().clone(), true),
            FunctionValueType::new(DataType::Decimal128(2, 1), true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        let data = batch(&program, source.clone());
        assert!(matches!(
            instance(&program).evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            ),
            Err(ExprEvaluationFailure::Kernel(
                KernelFailure::InvalidProgram(_)
            ))
        ));
    }
}

#[test]
fn decimal128_rescale_actual_compiler_full_rescale_rounding_overflow_scale_and_null_profiles() {
    const MAX: i128 = 10_i128.pow(38) - 1;
    // Scaling to one decimal place overflows on the two largest declared
    // integers. The remaining signed coefficients scale exactly.
    compare(
        38,
        0,
        DataType::Decimal128(38, 1),
        vec![
            Some(-MAX),
            Some(MAX),
            None,
            Some(3185),
            Some(-3185),
            Some(1),
        ],
        &[O, O, N, V(31850), V(-31850), V(10)],
        V(10),
    );
    // Half ties are away from zero. Larger valid source values already exceed
    // the target integer width before rounding.
    compare(
        10,
        4,
        DataType::Decimal128(6, 2),
        vec![
            Some(-9999999999),
            Some(9999999999),
            None,
            Some(3150),
            Some(-3150),
            Some(1),
        ],
        &[O, O, N, V(32), V(-32), V(0)],
        V(0),
    );
    // 99.995 is within the target integer width before rounding but carries
    // into the next digit. Its neighbor still fits.
    compare(
        9,
        3,
        DataType::Decimal128(4, 2),
        vec![
            Some(-99995),
            Some(99995),
            None,
            Some(99994),
            Some(-99994),
            Some(1),
        ],
        &[O, O, N, V(9999), V(-9999), V(0)],
        V(0),
    );
    compare(
        38,
        38,
        DataType::Decimal128(38, 0),
        vec![Some(-MAX), Some(MAX), None, Some(3185), Some(-1)],
        &[V(-1), V(1), N, V(0), V(0)],
        V(0),
    );
    // The scale-38 target has no integer digits. Zero fits and the original
    // SQL NULL remains distinct from an error or a TRY_CAST-created NULL.
    compare(
        38,
        0,
        DataType::Decimal128(38, 38),
        vec![Some(0), None, Some(1), Some(-1)],
        &[V(0), N, O, O],
        O,
    );
}

#[test]
fn decimal128_rescale_actual_compiler_every_seven_callback_causes_stop_and_latch_without_replay() {
    for (p, s, tp, ts, mode) in [
        (7, 2, 9, 3, TrinoCastMode::Explicit),
        (2, 0, 1, 0, TrinoCastMode::Explicit),
        (2, 0, 1, 0, TrinoCastMode::Try),
        (38, 0, 38, 38, TrinoCastMode::Try),
    ] {
        let source = input(p, s, vec![Some(1), Some(99), None, Some(-1)]);
        let program = compiled_cast(
            FunctionValueType::new(source.data_type().clone(), true),
            FunctionValueType::new(DataType::Decimal128(tp, ts), true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        let data = batch(&program, source);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &data,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                ScalarInvocationActivation::Activated,
                &recorder,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let trace = recorder.trace();
        assert!(trace.iter().all(|n| *n <= 256));
        for stop in 1..=trace.len() {
            for cause in causes() {
                let mut frame = instance(&program);
                let control = CallbackControl::new(cause.clone(), stop);
                assert!(
                    matches!(frame.evaluate(EvaluationInput::Batch { batch: &data, source: None }, ScalarInvocationScope::new(Selection::all(4), Selection::all(4)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..stop]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    frame.evaluate(
                        EvaluationInput::Batch {
                            batch: &data,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
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
