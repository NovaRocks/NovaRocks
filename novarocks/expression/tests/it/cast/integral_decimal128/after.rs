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

//! Full admitted successful-value shape matrix and real Frame fault/latch probes.
use super::*;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
fn compare_profile(dtype: DataType, p: u8, s: i8, expected_full: [Expected; 5]) {
    let (min, max) = bound(&dtype);
    let target = DataType::Decimal128(p, s);
    for (policy, allow) in modes() {
        let full = integral_input(&dtype, vec![Some(min), None, Some(max), Some(1), Some(-1)]);
        let program = compiled(
            FunctionValueType::new(dtype.clone(), true),
            FunctionValueType::new(target.clone(), true),
            Source::Column,
            Wrap::Bare,
            policy,
            allow,
        );
        for (source, fixed) in [
            (full.clone(), expected_full.as_slice()),
            (full.slice(1, 4), &expected_full[1..]),
            (full.slice(0, 0), &expected_full[..0]),
        ] {
            let n = source.len();
            let b = RecordBatch::try_new(
                program.input_schema().clone(),
                vec![
                    source.clone(),
                    Arc::new(Int64Array::from(vec![42; n])),
                    Arc::new(BooleanArray::from(vec![true; n])),
                ],
            )
            .unwrap();
            for rows in [
                (0..n).collect::<Vec<_>>(),
                (0..n).filter(|r| r % 2 == 0).collect(),
                // Overflowing inactive extrema cannot leak into this invocation.
                (0..n).filter(|r| r % 2 == 1).collect(),
                Vec::new(),
            ] {
                let selection = Selection::try_sparse(n, &rows).unwrap();
                let output = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &b,
                            source: None,
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(output.selection(), selection);
                assert_eq!(output.values().data_type(), &target);
                let mut expected = Vec::new();
                let mut errors = Vec::new();
                for (ordinal, row) in rows.iter().copied().enumerate() {
                    let (value, error) = fixed[row].result(policy);
                    expected.push(value);
                    if let Some(message) = error {
                        errors.push((ordinal, message.to_owned()));
                    }
                }
                assert_eq!(
                    values(output.values()),
                    expected,
                    "{dtype:?} -> Decimal128({p}, {s}), rows {rows:?}, {policy:?}, allow {allow}"
                );
                assert_eq!(
                    output
                        .errors()
                        .iter()
                        .map(|e| (e.selected_ordinal(), e.message().to_owned()))
                        .collect::<Vec<_>>(),
                    errors
                );
            }
        }
    }
}
#[test]
fn integral_decimal128_actual_after_full_width_precision_scale_policy_sparse_and_empty() {
    // Fixed raw-output tables for [source MIN, source NULL, source MAX, +1,-1].
    // Decimal coefficient = integer*10^scale; negative scales truncate toward
    // zero. Precision p requires coefficient magnitude < 10^p. O denotes a
    // finite overflow, independently of source NULL, and is never computed by
    // a reference converter. These tables include full physical source extrema.
    for (dtype, p, s, expected) in [
        (DataType::Int8, 3, 0, [V(-128), N, V(127), V(1), V(-1)]),
        (DataType::Int8, 4, 0, [V(-128), N, V(127), V(1), V(-1)]),
        (
            DataType::Int8,
            7,
            2,
            [V(-12800), N, V(12700), V(100), V(-100)],
        ),
        (DataType::Int8, 7, -2, [V(-1), N, V(1), V(0), V(0)]),
        (DataType::Int8, 18, 1, [V(-1280), N, V(1270), V(10), V(-10)]),
        (DataType::Int8, 19, 1, [V(-1280), N, V(1270), V(10), V(-10)]),
        (DataType::Int8, 38, 1, [V(-1280), N, V(1270), V(10), V(-10)]),
        (DataType::Int8, 38, 38, [O, N, O, O, O]),
        (DataType::Int8, 38, -38, [V(0), N, V(0), V(0), V(0)]),
        (DataType::Int16, 3, 0, [O, N, O, V(1), V(-1)]),
        (DataType::Int16, 4, 0, [O, N, O, V(1), V(-1)]),
        (
            DataType::Int16,
            7,
            2,
            [V(-3276800), N, V(3276700), V(100), V(-100)],
        ),
        (DataType::Int16, 7, -2, [V(-327), N, V(327), V(0), V(0)]),
        (
            DataType::Int16,
            18,
            1,
            [V(-327680), N, V(327670), V(10), V(-10)],
        ),
        (
            DataType::Int16,
            19,
            1,
            [V(-327680), N, V(327670), V(10), V(-10)],
        ),
        (
            DataType::Int16,
            38,
            1,
            [V(-327680), N, V(327670), V(10), V(-10)],
        ),
        (DataType::Int16, 38, 38, [O, N, O, O, O]),
        (DataType::Int16, 38, -38, [V(0), N, V(0), V(0), V(0)]),
        (DataType::Int32, 3, 0, [O, N, O, V(1), V(-1)]),
        (DataType::Int32, 4, 0, [O, N, O, V(1), V(-1)]),
        (DataType::Int32, 7, 2, [O, N, O, V(100), V(-100)]),
        (DataType::Int32, 7, -2, [O, N, O, V(0), V(0)]),
        (
            DataType::Int32,
            18,
            1,
            [V(-21474836480), N, V(21474836470), V(10), V(-10)],
        ),
        (
            DataType::Int32,
            19,
            1,
            [V(-21474836480), N, V(21474836470), V(10), V(-10)],
        ),
        (
            DataType::Int32,
            38,
            1,
            [V(-21474836480), N, V(21474836470), V(10), V(-10)],
        ),
        (DataType::Int32, 38, 38, [O, N, O, O, O]),
        (DataType::Int32, 38, -38, [V(0), N, V(0), V(0), V(0)]),
        (DataType::Int64, 3, 0, [O, N, O, V(1), V(-1)]),
        (DataType::Int64, 4, 0, [O, N, O, V(1), V(-1)]),
        (DataType::Int64, 7, 2, [O, N, O, V(100), V(-100)]),
        (DataType::Int64, 7, -2, [O, N, O, V(0), V(0)]),
        (DataType::Int64, 18, 1, [O, N, O, V(10), V(-10)]),
        (DataType::Int64, 19, 1, [O, N, O, V(10), V(-10)]),
        (
            DataType::Int64,
            38,
            1,
            [
                V(-92233720368547758080),
                N,
                V(92233720368547758070),
                V(10),
                V(-10),
            ],
        ),
        (DataType::Int64, 38, 38, [O, N, O, O, O]),
        (DataType::Int64, 38, -38, [V(0), N, V(0), V(0), V(0)]),
    ] {
        compare_profile(dtype, p, s, expected);
    }
}

#[test]
fn integral_decimal128_actual_after_original_i64_constant_broadcast_exact_source() {
    for (policy, allow) in modes() {
        let target = DataType::Decimal128(4, 0);
        let program = compiled(
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(target.clone(), true),
            Source::Constant,
            Wrap::Bare,
            policy,
            allow,
        );
        let b = RecordBatch::try_new(
            program.input_schema().clone(),
            vec![
                integral_input(&DataType::Int64, vec![Some(9999); 5]),
                Arc::new(Int64Array::from(vec![42; 5])),
                Arc::new(BooleanArray::from(vec![true; 5])),
            ],
        )
        .unwrap();
        let output = instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &b,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(5, &[1, 4]).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(values(output.values()), vec![Some(71), Some(71)]);
    }
}
struct Callback {
    trace: Mutex<Vec<u32>>,
    stop: usize,
    cause: KernelFailure,
}
impl Callback {
    fn new(stop: usize, cause: KernelFailure) -> Self {
        Self {
            trace: Mutex::new(vec![]),
            stop,
            cause,
        }
    }
}
impl KernelEvaluationControl for Callback {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut t = self.trace.lock().unwrap();
        assert!(t.len() < self.stop, "no callback after first refusal");
        t.push(n);
        if t.len() == self.stop {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("integral Decimal CAST never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("integral-frame-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("integral-frame-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("integral-frame-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn integral_decimal128_actual_after_every_frame_callback_keeps_seven_causes_and_no_replay() {
    let program = compiled(
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Decimal128(3, 0), true),
        Source::Column,
        Wrap::Bare,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let b = RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            integral_input(&DataType::Int64, vec![Some(1000), None, Some(99)]),
            Arc::new(Int64Array::from(vec![42; 3])),
            Arc::new(BooleanArray::from(vec![true; 3])),
        ],
    )
    .unwrap();
    for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let record = Callback::new(usize::MAX, KernelFailure::Cancelled);
        instance(&program)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &b,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &record,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let trace = record.trace.lock().unwrap().clone();
        for stop in 1..=trace.len() {
            for cause in causes() {
                let c = Callback::new(stop, cause.clone());
                let mut frame = instance(&program);
                assert!(
                    matches!(frame.evaluate(EvaluationInput::Batch { batch: &b, source: None }, ScalarInvocationScope::new(selection, selection), ScalarInvocationActivation::Activated, &c, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..stop]);
                let after = Callback::new(usize::MAX, KernelFailure::Cancelled);
                assert!(matches!(
                    frame.evaluate(
                        EvaluationInput::Batch {
                            batch: &b,
                            source: None
                        },
                        ScalarInvocationScope::new(selection, selection),
                        ScalarInvocationActivation::Activated,
                        &after,
                        &mut PassThroughCallHost
                    ),
                    Err(ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed))
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
