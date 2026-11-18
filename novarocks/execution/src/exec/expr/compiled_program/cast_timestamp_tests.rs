// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use arrow::array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::TimeUnit;
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::SelectedValues;
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};

const UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

fn timestamp_type(unit: TimeUnit, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Timestamp(unit, None), nullable)
}
pub(super) fn timestamp_array(unit: TimeUnit, values: Vec<Option<i64>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)),
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values)),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values)),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
    }
}
fn timestamp_values(output: &SelectedValues<'_>) -> Vec<Option<i64>> {
    macro_rules! values {
        ($array:ty) => {
            output
                .values()
                .as_any()
                .downcast_ref::<$array>()
                .unwrap()
                .iter()
                .collect()
        };
    }
    match output.values().data_type() {
        DataType::Timestamp(TimeUnit::Second, None) => values!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, None) => values!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, None) => values!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => values!(TimestampNanosecondArray),
        other => panic!("timestamp result lost its exact carrier: {other:?}"),
    }
}
fn timestamp_batch(
    program: &LocalProgram,
    values: &[Option<i64>],
    flags: &[Option<bool>],
) -> RecordBatch {
    let schema = program.graph().nodes()[1].output_layout().schema().clone();
    let DataType::Timestamp(unit, None) = schema.field(0).data_type() else {
        panic!("actual frozen timestamp source")
    };
    let mut padded = vec![Some(99)];
    padded.extend_from_slice(values);
    padded.push(Some(-99));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            timestamp_array(*unit, padded).slice(1, values.len()),
            Arc::new(Int64Array::from(vec![Some(42); values.len()])),
            Arc::new(BooleanArray::from(flags.to_vec())),
        ],
    )
    .unwrap()
}

// Independent coefficient tables for [-1001,-1,0,1,1001,MIN,MAX,NULL].
// Unit ratios are decimal powers; narrowing truncates toward zero. The table
// explicitly records range overflow as a successful NULL except for the
// separately frozen microsecond-to-nanosecond row-error contract.
fn expected_unit_rows(source: TimeUnit, target: TimeUnit) -> [Option<i64>; 8] {
    use TimeUnit::*;
    match (source, target) {
        (Second, Second)
        | (Millisecond, Millisecond)
        | (Microsecond, Microsecond)
        | (Nanosecond, Nanosecond) => [
            Some(-1001),
            Some(-1),
            Some(0),
            Some(1),
            Some(1001),
            Some(i64::MIN),
            Some(i64::MAX),
            None,
        ],
        (Second, Millisecond) | (Millisecond, Microsecond) | (Microsecond, Nanosecond) => [
            Some(-1_001_000),
            Some(-1_000),
            Some(0),
            Some(1_000),
            Some(1_001_000),
            None,
            None,
            None,
        ],
        (Second, Microsecond) | (Millisecond, Nanosecond) => [
            Some(-1_001_000_000),
            Some(-1_000_000),
            Some(0),
            Some(1_000_000),
            Some(1_001_000_000),
            None,
            None,
            None,
        ],
        (Second, Nanosecond) => [
            Some(-1_001_000_000_000),
            Some(-1_000_000_000),
            Some(0),
            Some(1_000_000_000),
            Some(1_001_000_000_000),
            None,
            None,
            None,
        ],
        (Millisecond, Second) | (Microsecond, Millisecond) | (Nanosecond, Microsecond) => [
            Some(-1),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(-9_223_372_036_854_775),
            Some(9_223_372_036_854_775),
            None,
        ],
        (Microsecond, Second) | (Nanosecond, Millisecond) => [
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(-9_223_372_036_854),
            Some(9_223_372_036_854),
            None,
        ],
        (Nanosecond, Second) => [
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(-9_223_372_036),
            Some(9_223_372_036),
            None,
        ],
    }
}
fn expected_constant_unit(source: TimeUnit, target: TimeUnit) -> i64 {
    use TimeUnit::*;
    match (source, target) {
        (Second, Second)
        | (Millisecond, Millisecond)
        | (Microsecond, Microsecond)
        | (Nanosecond, Nanosecond) => 71,
        (Second, Millisecond) | (Millisecond, Microsecond) | (Microsecond, Nanosecond) => 71_000,
        (Second, Microsecond) | (Millisecond, Nanosecond) => 71_000_000,
        (Second, Nanosecond) => 71_000_000_000,
        (Millisecond, Second)
        | (Microsecond, Millisecond)
        | (Nanosecond, Microsecond)
        | (Microsecond, Second)
        | (Nanosecond, Millisecond)
        | (Nanosecond, Second) => 0,
    }
}

#[test]
fn timestamp_cast_compiler_controller_all_sixteen_pairs_keep_independent_coefficient_tables() {
    let values = [
        Some(8),
        Some(-1001),
        Some(-1),
        Some(0),
        Some(1),
        Some(1001),
        Some(i64::MIN),
        Some(i64::MAX),
        None,
        Some(9),
    ];
    let rows = [1, 2, 3, 4, 5, 6, 7, 8];
    let selection = Selection::try_sparse(values.len(), &rows).unwrap();
    for source in UNITS {
        for target in UNITS {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let program = compiled(
                        timestamp_type(source, true),
                        timestamp_type(target, true),
                        Source::Column,
                        Wrap::Bare,
                        policy,
                        allow,
                    );
                    let batch = timestamp_batch(&program, &values, &[Some(true); 10]);
                    let mut evaluator = instance(&program);
                    let output = evaluator
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
                    assert_eq!(output.selection(), selection);
                    assert_eq!(
                        output.values().data_type(),
                        &DataType::Timestamp(target, None)
                    );
                    assert_eq!(
                        timestamp_values(&output),
                        expected_unit_rows(source, target)
                    );
                    let expected_errors =
                        if (source, target) == (TimeUnit::Microsecond, TimeUnit::Nanosecond) {
                            vec![5, 6]
                        } else {
                            Vec::new()
                        };
                    assert_eq!(errors(&output), expected_errors);
                    for error in output.errors() {
                        let raw = values[rows[error.selected_ordinal()]].unwrap();
                        assert_eq!(
                            error.message(),
                            format!(
                                "CAST failed: from Timestamp(Microsecond, None) to Timestamp(Nanosecond, None): CAST timestamp microsecond->nanosecond overflow: value {raw} cannot be represented as nanoseconds in i64"
                            )
                        );
                    }
                    let empty = evaluator
                        .evaluate(
                            EvaluationInput::Batch {
                                batch: &batch,
                                source: None,
                            },
                            {
                                let selection = Selection::try_sparse(values.len(), &[]).unwrap();
                                ScalarInvocationScope::new(selection, selection)
                            },
                            ScalarInvocationActivation::Activated,
                            &Control,
                            &mut PassThroughCallHost,
                        )
                        .unwrap();
                    assert!(empty.values().is_empty());
                    assert!(empty.errors().is_empty());
                    assert!(evaluator.instances.is_empty());
                }
            }
        }
    }
}

#[test]
fn timestamp_cast_nonnullable_contract_requires_only_five_successful_null_promises() {
    for source in UNITS {
        for target in UNITS {
            let safe_widen = matches!(
                (source, target),
                (
                    TimeUnit::Second,
                    TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond
                ) | (
                    TimeUnit::Millisecond,
                    TimeUnit::Microsecond | TimeUnit::Nanosecond
                )
            );
            let (functions, package) = fixture(
                timestamp_type(source, false),
                timestamp_type(target, false),
                Source::Column,
                Wrap::Bare,
                DecimalOverflowPolicy::ReportError,
                false,
            );
            let result = compile(&functions, package, &Control);
            if safe_widen {
                assert!(result.unwrap_err().to_string().contains("successful-NULL"));
            } else {
                let program = result.unwrap();
                let batch =
                    timestamp_batch(&program, &[Some(i64::MAX), Some(-1)], &[Some(true); 2]);
                let output = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(
                    errors(&output),
                    if (source, target) == (TimeUnit::Microsecond, TimeUnit::Nanosecond) {
                        vec![0]
                    } else {
                        vec![]
                    }
                );
            }
        }
    }
}

#[test]
fn timestamp_cast_literal_broadcast_and_batch_splits_keep_original_coefficients() {
    for source in UNITS {
        for target in UNITS {
            let program = compiled(
                timestamp_type(source, false),
                timestamp_type(target, true),
                Source::Constant,
                Wrap::Bare,
                DecimalOverflowPolicy::OutputNull,
                false,
            );
            let batch = timestamp_batch(&program, &[Some(i64::MAX); 5], &[Some(true); 5]);
            let coefficient = Some(expected_constant_unit(source, target));
            let rows = [0, 2, 4];
            let mut evaluator = instance(&program);
            for _ in 0..2 {
                let output = evaluator
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        {
                            let selection = Selection::try_sparse(5, &rows).unwrap();
                            ScalarInvocationScope::new(selection, selection)
                        },
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(timestamp_values(&output), vec![coefficient; 3]);
                assert!(output.errors().is_empty());
            }
        }
    }
}

#[test]
fn timestamp_cast_guards_distinguish_successful_nulls_from_required_overflow_errors() {
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for wrap in [Wrap::Bare, Wrap::IsNull, Wrap::If, Wrap::Coalesce] {
                let program = compiled(
                    timestamp_type(TimeUnit::Microsecond, true),
                    timestamp_type(TimeUnit::Nanosecond, true),
                    Source::Column,
                    wrap,
                    policy,
                    allow,
                );
                let batch = timestamp_batch(
                    &program,
                    &[Some(i64::MAX), None, Some(1), Some(i64::MAX)],
                    &[Some(true), Some(true), Some(true), Some(false)],
                );
                let output = instance(&program)
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &batch,
                            source: None,
                        },
                        ScalarInvocationScope::new(Selection::all(4), Selection::all(4)),
                        ScalarInvocationActivation::Activated,
                        &Control,
                        &mut PassThroughCallHost,
                    )
                    .unwrap();
                assert_eq!(
                    errors(&output),
                    if matches!(wrap, Wrap::If) {
                        vec![0]
                    } else {
                        vec![0, 3]
                    }
                );
                if matches!(wrap, Wrap::IsNull) {
                    let values = output
                        .values()
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap();
                    assert_eq!(
                        values.iter().collect::<Vec<_>>(),
                        vec![None, Some(true), Some(false), None]
                    );
                } else {
                    assert_eq!(
                        timestamp_values(&output),
                        match wrap {
                            Wrap::If => vec![None, None, Some(1000), Some(71)],
                            Wrap::Coalesce => vec![None, Some(71), Some(1000), None],
                            _ => vec![None, None, Some(1000), None],
                        }
                    );
                }
                for error in output.errors() {
                    assert!(error.message().contains("microsecond->nanosecond overflow"));
                }
            }
            let program = compiled(
                timestamp_type(TimeUnit::Second, true),
                timestamp_type(TimeUnit::Nanosecond, true),
                Source::Column,
                Wrap::Coalesce,
                policy,
                allow,
            );
            let batch =
                timestamp_batch(&program, &[Some(i64::MAX), None, Some(1)], &[Some(true); 3]);
            let output = instance(&program)
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &batch,
                        source: None,
                    },
                    ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                    ScalarInvocationActivation::Activated,
                    &Control,
                    &mut PassThroughCallHost,
                )
                .unwrap();
            assert_eq!(
                timestamp_values(&output),
                vec![Some(71), Some(71), Some(1000000000)]
            );
            assert!(output.errors().is_empty());
        }
    }
}

#[test]
fn timestamp_cast_actual_runtime_callbacks_preserve_all_seven_causes_and_failed_latch() {
    let program = compiled(
        timestamp_type(TimeUnit::Microsecond, true),
        timestamp_type(TimeUnit::Nanosecond, true),
        Source::Column,
        Wrap::Coalesce,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let batch = timestamp_batch(
        &program,
        &[Some(i64::MAX), None, Some(-1)],
        &[Some(true); 3],
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(errors(&output), vec![0]);
    let trace = recorder.trace();
    for stop_at in 1..=trace.len() {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), stop_at);
            let mut evaluator = instance(&program);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(3), Selection::all(3)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..stop_at]);
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
    let values = vec![Some(1); 320];
    let batch = timestamp_batch(&program, &values, &[Some(true); 320]);
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = instance(&program)
        .evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(timestamp_values(&output), vec![Some(1000); 320]);
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for stop_at in (1..=trace.len()).filter(|&n| n == 1 || n == trace.len() || trace[n - 1] == 256)
    {
        for cause in causes() {
            let control = CallbackControl::new(cause.clone(), stop_at);
            assert!(
                matches!(instance(&program).evaluate(EvaluationInput::Batch { batch: &batch, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..stop_at]);
        }
    }
}
