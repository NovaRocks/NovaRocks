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

use arrow::{
    array::{Array, ArrayRef, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray},
    datatypes::DataType,
};
use novarocks_expression::{
    CompiledExpressionInstance, EvaluationInput, ExprEvaluationFailure,
    testkit::{
        CallbackControl, PassThroughCallHost, callback_causes,
        guarded_fixtures::{
            GUARDED_CONTROL as Control,
            temporal::{Shape as Fixture, batch, program},
        },
    },
};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};
use novarocks_type_contract::TemporalSourceShape as Source;
use std::sync::Arc;

fn causes() -> [KernelFailure; 7] {
    callback_causes([
        "guarded-origin-invalid",
        "guarded-origin-internal",
        "guarded-origin-operational",
    ])
}

#[test]
fn actual_each_registered_profile_uses_explicit_source_receipt_and_original_math() {
    let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
    for (dtype, values, expected) in [
        (
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some("12:34:56"), None, Some("bad")])) as ArrayRef,
            vec![Some(45296), None, None],
        ),
        (
            DataType::Date32,
            Arc::new(Date32Array::from(vec![Some(1), None, Some(-1)])) as ArrayRef,
            vec![Some(0), None, Some(0)],
        ),
        (
            timestamp,
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(45296000000),
                None,
                Some(1000000),
            ])) as ArrayRef,
            vec![Some(45296), None, Some(1)],
        ),
    ] {
        let seconds = program(Fixture::PlainSec, dtype.clone());
        let input = batch(
            &seconds,
            values.clone(),
            vec![Some("%f"); 3],
            vec![Some(true); 3],
        );
        let result = seconds
            .instance(&Control)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        let format = program(Fixture::PlainFormat, dtype);
        let input = batch(
            &format,
            values,
            vec![Some("%H:%i:%s/%f"); 3],
            vec![Some(true); 3],
        );
        let result = format
            .instance(&Control)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let expected = expected
            .iter()
            .map(|value| value.map(|v| format!("00:00:00/{v:06}")))
            .collect::<Vec<_>>();
        assert_eq!(
            result
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.map(str::to_owned))
                .collect::<Vec<_>>(),
            expected
        );
    }
}
#[test]
fn utf8_override_preserves_raw_clock_result_and_two_distinct_source_occurrences() {
    let program = program(Fixture::FormatOverride, DataType::Utf8);
    let calls = &program.unit;
    let source = calls
        .calls()
        .values()
        .find_map(|call| call.call_contract().temporal_source())
        .unwrap();
    assert_eq!(source.facts.shape(), Source::FormatUtf8Override);
    assert_ne!(
        source.sources[0].context.use_id,
        source.sources[1].context.use_id
    );
    let input = batch(
        &program,
        Arc::new(StringArray::from(vec![Some("12:34:56"), Some("bad"), None])),
        vec![Some("%f"); 3],
        vec![Some(true); 3],
    );
    let result = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("045296"), None, None]
    );
}
#[test]
fn sec_roundtrip_bypasses_unprepared_transformed_child_and_keeps_saturation() {
    let program = program(Fixture::Roundtrip, DataType::Int64);
    let input = batch(
        &program,
        Arc::new(Int64Array::from(vec![Some(-12), Some(4000000), None])),
        vec![None; 3],
        vec![None; 3],
    );
    let result = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(-12), Some(3023999), None]
    );
}
#[test]
fn sparse_if_invocation_ignores_inactive_original_rows_and_empty_never_demands_sources() {
    let program = program(Fixture::SparseIf, DataType::Utf8);
    let input = batch(
        &program,
        Arc::new(StringArray::from(vec![Some("bad"), Some("12:34:56"), None])),
        vec![None; 3],
        vec![Some(false), Some(true), Some(false)],
    );
    let result = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![None, Some(45296), None]
    );
    let empty = [];
    let result = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            {
                let selection = Selection::try_sparse(3, &empty).unwrap();
                ScalarInvocationScope::new(selection, selection)
            },
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert!(result.values().is_empty());
}
#[test]
fn every_actual_temporal_constructor_and_evaluation_callback_preserves_seven_causes_and_failed_latch()
 {
    for fixture in [
        Fixture::PlainSec,
        Fixture::PlainFormat,
        Fixture::FormatOverride,
        Fixture::Roundtrip,
        Fixture::SparseIf,
        Fixture::CastOther,
    ] {
        let dtype = match fixture {
            Fixture::Roundtrip => DataType::Int64,
            Fixture::CastOther => DataType::Date32,
            _ => DataType::Utf8,
        };
        let program = program(fixture, dtype);
        let rows = if matches!(fixture, Fixture::Roundtrip | Fixture::CastOther) {
            320
        } else {
            8
        };
        let long_text = "12:34:56".repeat(40);
        let values: ArrayRef = if matches!(fixture, Fixture::Roundtrip) {
            Arc::new(Int64Array::from(vec![Some(45296); rows]))
        } else if matches!(fixture, Fixture::CastOther) {
            Arc::new(Date32Array::from(vec![Some(i32::MAX); rows]))
        } else {
            Arc::new(StringArray::from(vec![Some(long_text.as_str()); rows]))
        };
        let input = batch(
            &program,
            values,
            vec![Some("%H:%i:%s/%f"); rows],
            vec![Some(true); rows],
        );
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        CompiledExpressionInstance::try_new(program.unit.clone(), program.root, &recorder).unwrap();
        let constructor = recorder.trace();
        for index in 1..=constructor.len() {
            for cause in causes() {
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(CompiledExpressionInstance::try_new(program.unit.clone(),program.root,&control),Err(actual) if actual==cause)
                );
                assert_eq!(control.trace(), constructor[..index]);
            }
        }
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        program
            .instance(&Control)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(rows), Selection::all(rows)),
                ScalarInvocationActivation::Activated,
                &recorder,
                &mut PassThroughCallHost,
            )
            .unwrap();
        let trace = recorder.trace();
        assert!(trace.contains(&256));
        assert!(trace.iter().all(|units| *units <= 256));
        for index in 1..=trace.len() {
            for cause in causes() {
                let mut evaluator = program.instance(&Control);
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(rows), Selection::all(rows)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..index]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None
                        },
                        ScalarInvocationScope::new(Selection::all(rows), Selection::all(rows)),
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

#[test]
fn deepest_data_error_projects_across_successful_current_invocation_and_sparse_guard() {
    for (dtype, array, expected) in [
        (
            DataType::Date32,
            Arc::new(Date32Array::from(vec![0, i32::MAX])) as ArrayRef,
            "Cast error: Failed to convert 2147483647 to temporal for Date32",
        ),
        (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![0, i64::MAX])) as ArrayRef,
            "Cast error: Failed to convert 9223372036854775807 to datetime for Timestamp(µs)",
        ),
    ] {
        let ordinary = program(Fixture::CastOther, dtype.clone());
        let input = batch(&ordinary, array.clone(), vec![None; 2], vec![None; 2]);
        let mut evaluator = ordinary.instance(&Control);
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            output
                .errors()
                .iter()
                .map(|e| (e.selected_ordinal(), e.message()))
                .collect::<Vec<_>>(),
            vec![(0, expected), (1, expected)]
        );
        assert!(output.values().is_null(0));
        assert!(output.values().is_null(1));
        // SQL data errors do not latch the runtime-control failure path.
        let sparse = [0];
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                {
                    let selection = Selection::try_sparse(2, &sparse).unwrap();
                    ScalarInvocationScope::new(selection, selection)
                },
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
        let guarded = program(Fixture::SparseCastOther, dtype);
        let input = batch(
            &guarded,
            array,
            vec![None; 2],
            vec![Some(true), Some(false)],
        );
        let output = guarded
            .instance(&Control)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(2), Selection::all(2)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0), None]
        );
    }
}
