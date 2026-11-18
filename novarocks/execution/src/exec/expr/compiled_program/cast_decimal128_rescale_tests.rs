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
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

const OVERFLOW: &str =
    "Expr evaluate meet error: The numeric type cast involving decimal overflows";

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
fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}

#[derive(Clone, Copy, Debug)]
enum Expected {
    Value(i128),
    SourceNull,
    // Finite conversion overflow: NULL only in OutputNull/allow=false.
    Overflow,
}
impl Expected {
    // Materialize only the frozen policy response to a handwritten flag.
    // This function contains no precision, rounding or scale calculation.
    fn outcome(
        self,
        policy: DecimalOverflowPolicy,
        allow: bool,
    ) -> (Option<i128>, Option<&'static str>) {
        match self {
            Self::Value(value) => (Some(value), None),
            Self::SourceNull => (None, None),
            Self::Overflow if policy == DecimalOverflowPolicy::OutputNull && !allow => (None, None),
            Self::Overflow => (None, Some(OVERFLOW)),
        }
    }
}
use Expected::{Overflow as O, SourceNull as N, Value as V};

fn batch(program: &LocalProgram, source: ArrayRef) -> RecordBatch {
    let n = source.len();
    RecordBatch::try_new(
        program.graph().nodes()[1].output_layout().schema().clone(),
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
    for (policy, allow) in modes() {
        let full = input(p, s, raw.clone());
        let program = compiled(
            FunctionValueType::new(full.data_type().clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
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
                    let (value, error) = expected_source[row].outcome(policy, allow);
                    expected.push(value);
                    if let Some(message) = error {
                        errors.push((ordinal, message.to_owned()));
                    }
                }
                assert_eq!(
                    values(out.values()),
                    expected,
                    "source Decimal128({p}, {s}), target {target:?}, policy {policy:?}, allow {allow}, rows {rows:?}"
                );
                assert_eq!(
                    out.errors()
                        .iter()
                        .map(|e| (e.selected_ordinal(), e.message().to_owned()))
                        .collect::<Vec<_>>(),
                    errors,
                    "source Decimal128({p}, {s}), target {target:?}, policy {policy:?}, allow {allow}, rows {rows:?}",
                );
            }
        }
        let constant = compiled(
            FunctionValueType::new(full.data_type().clone(), false),
            FunctionValueType::new(target.clone(), true),
            Source::Constant,
            Wrap::Bare,
            policy,
            allow,
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
        let (expected, error) = expected_constant.outcome(policy, allow);
        assert_eq!(values(out.values()), vec![expected, expected]);
        let errors = match error {
            Some(message) => vec![(0, message.to_owned()), (1, message.to_owned())],
            None => vec![],
        };
        assert_eq!(
            out.errors()
                .iter()
                .map(|e| (e.selected_ordinal(), e.message().to_owned()))
                .collect::<Vec<_>>(),
            errors
        );
    }
}
#[test]
fn decimal128_rescale_actual_compiler_observed_7_2_to_9_3_full_policy_and_selection() {
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
fn decimal128_rescale_actual_compiler_observed_14_13_to_38_13_full_policy_and_selection() {
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
fn decimal128_rescale_actual_compiler_identical_metadata_full_carrier_precision_is_not_identity() {
    // Precision 1 admits only -9..=9 even when the source metadata matches.
    // The full physical carrier values remain data; a precision claim cannot
    // hide MIN/MAX or +/-10 from target precision enforcement.
    compare(
        1,
        0,
        DataType::Decimal128(1, 0),
        vec![
            Some(i128::MIN),
            Some(i128::MAX),
            None,
            Some(9),
            Some(10),
            Some(-10),
        ],
        &[O, O, N, V(9), O, O],
        V(1),
    );
}

fn full_carrier_corpus() -> Vec<Option<i128>> {
    vec![
        Some(i128::MIN),
        Some(i128::MAX),
        None,
        Some(3185),
        Some(-3185),
        Some(1),
    ]
}
#[test]
fn decimal128_rescale_actual_compiler_full_rescale_rounding_overflow_scale_and_null_profiles() {
    // These are fixed tables, not a second converter. Precision p accepts
    // absolute raw magnitude < 10^p; downscale uses nearest with half ties
    // away from zero. All large-carrier outcomes are justified per row below.
    // A scale difference does not require materializing a power of ten.
    // Every i128 magnitude is below 10^39 / 2, so division by 10^76
    // rounds to zero. Multiplication by 10^76 overflows for any nonzero
    // i128; zero remains exactly zero. These are mathematical value proofs.
    for (p, s, tp, ts, expected, constant) in [
        // x10: MIN/MAX cannot fit the output carrier/precision. The three
        // small raw values become 31850, -31850 and 10.
        (38, 0, 38, 1, [O, O, N, V(31850), V(-31850), V(10)], V(10)),
        // /100: rounded MIN/MAX still have 37 digits, exceeding precision10.
        // 31.85 -> 32; -31.85 -> -32; 0.01 -> 0.
        (10, 4, 10, 2, [O, O, N, V(32), V(-32), V(0)], V(0)),
        // x100: MIN/MAX exceed precision20; 3185 -> 318500.
        (
            18,
            -2,
            20,
            0,
            [O, O, N, V(318500), V(-318500), V(100)],
            V(100),
        ),
        // /10000: rounded MIN/MAX still have 35 digits, exceeding precision20.
        // Each small corpus magnitude is below one half, hence rounds to 0.
        (18, 2, 20, -2, [O, O, N, V(0), V(0), V(0)], V(0)),
        // /10^38: MIN/MAX have magnitude 1.7014118346..., hence -2/+2.
        // All three small values round to 0 and fit precision38.
        (38, 0, 38, -38, [V(-2), V(2), N, V(0), V(0), V(0)], V(0)),
        // /10^76 rounds every physical carrier value to zero.
        (38, 38, 38, -38, [V(0), V(0), N, V(0), V(0), V(0)], V(0)),
        // x10^76 overflows for every nonzero corpus value, with the
        // original numeric-overflow policy rather than a factor error.
        (1, -38, 38, 38, [O, O, N, O, O, O], O),
    ] {
        compare(
            p,
            s,
            DataType::Decimal128(tp, ts),
            full_carrier_corpus(),
            &expected,
            constant,
        );
    }
    // Zero is not an overflow when scaling up; source NULL stays separate.
    // Keep this boundary alongside the nonzero full-carrier table above.
    compare(
        1,
        -38,
        DataType::Decimal128(38, 38),
        vec![Some(0), None, Some(1)],
        &[V(0), N, O],
        O,
    );
}

#[test]
fn decimal128_rescale_actual_compiler_every_seven_callback_causes_stop_and_latch_without_replay() {
    for (p, s, tp, ts, policy, allow) in [
        (7, 2, 9, 3, DecimalOverflowPolicy::OutputNull, false),
        (1, 0, 1, 0, DecimalOverflowPolicy::ReportError, true),
        (38, 38, 38, -38, DecimalOverflowPolicy::OutputNull, false),
    ] {
        let source = input(p, s, vec![Some(1), Some(i128::MAX), None, Some(-1)]);
        let program = compiled(
            FunctionValueType::new(source.data_type().clone(), true),
            FunctionValueType::new(DataType::Decimal128(tp, ts), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
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
