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

//! Compiled invocation-order probes with independent values and diagnostics.
//! SEP-1 checks both epoch-offset addition and the calendar literal width.
//! Extreme carriers produce row errors under every build profile, never panics.
//! The legacy arena's first-error String return was a historical shell contract,
//! not the compiled row-journal contract; no old evaluator runs in these tests.
//! Mount as a child of compiled_program/cast_tests.rs. Each case owns a fresh
//! actual compiler instance; a caught panic is never followed by reuse.
use super::*;
use arrow::array::{Date32Array, Float32Array, Float64Array};
use novarocks_expression::testkit::PassThroughCallHost;
use novarocks_expression::{EvaluationInput, ExprEvaluationFailure};
use novarocks_functions::{ScalarInvocationActivation, ScalarInvocationScope};
use std::panic::{AssertUnwindSafe, catch_unwind};

#[derive(Debug, Clone, PartialEq, Eq)]
struct FloatSnapshot {
    dtype: DataType,
    bits: Vec<Option<u64>>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum Observation {
    Rows(FloatSnapshot, Vec<(usize, String)>),
    Kernel(String),
    Panic(String),
}
fn snapshot(values: &ArrayRef) -> FloatSnapshot {
    let bits = match values.data_type() {
        DataType::Float32 => values
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(|v| u64::from(v.to_bits())))
            .collect(),
        DataType::Float64 => values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(f64::to_bits))
            .collect(),
        other => panic!("probe expected float output, got {other:?}"),
    };
    FloatSnapshot {
        dtype: values.data_type().clone(),
        bits,
    }
}
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_owned()
    }
}
fn compiled_observation(
    program: &CompiledFixture,
    source: &ArrayRef,
    selected_rows: &[usize],
) -> Observation {
    let batch = RecordBatch::try_new(
        program.input_schema().clone(),
        vec![
            source.clone(),
            Arc::new(Int64Array::from(vec![42; source.len()])),
            Arc::new(BooleanArray::from(vec![true; source.len()])),
        ],
    )
    .unwrap();
    let mut evaluator = instance(program);
    let selection = Selection::try_sparse(source.len(), selected_rows).unwrap();
    let caught = catch_unwind(AssertUnwindSafe(|| {
        match evaluator.evaluate(
            EvaluationInput::Batch {
                batch: &batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            &Control,
            &mut PassThroughCallHost,
        ) {
            Ok(values) => Observation::Rows(
                snapshot(values.values()),
                values
                    .errors()
                    .iter()
                    .map(|error| (error.selected_ordinal(), error.message().to_owned()))
                    .collect(),
            ),
            Err(ExprEvaluationFailure::Kernel(error)) => Observation::Kernel(format!("{error:?}")),
            Err(error) => panic!("date Cast returned a non-kernel failure: {error:?}"),
        }
    }));
    // Drop the instance after either a normal return or unwind. This probe does
    // not add a panic-to-KernelFailure adapter or assert direct panic latching.
    drop(evaluator);
    match caught {
        Ok(observed) => observed,
        Err(payload) => Observation::Panic(panic_message(payload)),
    }
}
struct Case {
    label: &'static str,
    carriers: Vec<i32>,
    valid: Vec<bool>,
    selected: Vec<usize>,
}
fn cases() -> Vec<Case> {
    vec![
        Case {
            label: "MIN_MAX_all",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, true],
            selected: vec![0, 1],
        },
        Case {
            label: "MAX_MIN_all",
            carriers: vec![i32::MAX, i32::MIN],
            valid: vec![true, true],
            selected: vec![0, 1],
        },
        Case {
            label: "MIN_isolated",
            carriers: vec![i32::MIN],
            valid: vec![true],
            selected: vec![0],
        },
        Case {
            label: "MAX_isolated",
            carriers: vec![i32::MAX],
            valid: vec![true],
            selected: vec![0],
        },
        Case {
            label: "MIN_MAX_only_MIN",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, true],
            selected: vec![0],
        },
        Case {
            label: "MIN_MAX_only_MAX",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, true],
            selected: vec![1],
        },
        Case {
            label: "MAX_MIN_only_MIN",
            carriers: vec![i32::MAX, i32::MIN],
            valid: vec![true, true],
            selected: vec![1],
        },
        Case {
            label: "empty_demand",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, true],
            selected: vec![],
        },
        Case {
            label: "MIN_hidden_MAX_NULL",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![true, false],
            selected: vec![0, 1],
        },
        Case {
            label: "hidden_MIN_NULL_MAX",
            carriers: vec![i32::MIN, i32::MAX],
            valid: vec![false, true],
            selected: vec![0, 1],
        },
        Case {
            label: "MAX_hidden_MIN_NULL",
            carriers: vec![i32::MAX, i32::MIN],
            valid: vec![true, false],
            selected: vec![0, 1],
        },
        Case {
            label: "hidden_MAX_NULL_MIN",
            carriers: vec![i32::MAX, i32::MIN],
            valid: vec![false, true],
            selected: vec![0, 1],
        },
        Case {
            label: "safe_hidden_MAX_NULL",
            carriers: vec![0, i32::MAX],
            valid: vec![true, false],
            selected: vec![0, 1],
        },
        Case {
            label: "safe_inactive_MAX",
            carriers: vec![0, i32::MAX],
            valid: vec![true, true],
            selected: vec![0],
        },
    ]
}
fn expected_compiled(case: &Case, target: &DataType) -> Observation {
    let min = format!("CAST failed: from Date32 to {target:?}: invalid Date32 value -2147483648");
    let max = format!("CAST failed: from Date32 to {target:?}: invalid Date32 value 2147483647");
    // Epoch day zero encodes decimal date 19700101. IEEE nearest-even gives
    // f32 19700100 (ULP 2) and exact f64 19700101. These fixed bits do not
    // invoke a cast implementation or derive an expectation from its output.
    let epoch = match target {
        DataType::Float32 => 0x4b96_4cc2,
        DataType::Float64 => 0x4172_c998_5000_0000,
        _ => panic!("probe expected float target"),
    };
    let rows = |bits, errors| {
        Observation::Rows(
            FloatSnapshot {
                dtype: target.clone(),
                bits,
            },
            errors,
        )
    };
    // Epoch-offset and calendar bounds are checked by the production owner.
    // Both extremes are row errors; inactive and NULL carriers are untouched.
    match case.label {
        "MIN_MAX_all" => rows(vec![None, None], vec![(0, min), (1, max)]),
        "MAX_MIN_all" => rows(vec![None, None], vec![(0, max), (1, min)]),
        "MIN_isolated" | "MIN_MAX_only_MIN" | "MAX_MIN_only_MIN" => {
            rows(vec![None], vec![(0, min)])
        }
        "MAX_isolated" | "MIN_MAX_only_MAX" => rows(vec![None], vec![(0, max)]),
        "empty_demand" => rows(vec![], vec![]),
        "MIN_hidden_MAX_NULL" => rows(vec![None, None], vec![(0, min)]),
        "hidden_MIN_NULL_MAX" => rows(vec![None, None], vec![(1, max)]),
        "MAX_hidden_MIN_NULL" => rows(vec![None, None], vec![(0, max)]),
        "hidden_MAX_NULL_MIN" => rows(vec![None, None], vec![(1, min)]),
        "safe_hidden_MAX_NULL" => rows(vec![Some(epoch), None], vec![]),
        "safe_inactive_MAX" => rows(vec![Some(epoch)], vec![]),
        _ => panic!(
            "unmapped independently authored probe shape: {}",
            case.label
        ),
    }
}
fn run_probe(target: DataType) {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let program = compiled(
                FunctionValueType::new(DataType::Date32, true),
                FunctionValueType::new(target.clone(), true),
                Source::Column,
                Wrap::Bare,
                policy,
                allow,
            );
            for case in cases() {
                let expected = expected_compiled(&case, &target);
                let source: ArrayRef = Arc::new(Date32Array::new(
                    case.carriers.into(),
                    Some(arrow::buffer::NullBuffer::from(case.valid)),
                ));
                let observed = compiled_observation(&program, &source, &case.selected);
                eprintln!(
                    "DATE_FLOAT_INVOCATION_PROBE target={target:?} policy={policy:?} allow={allow} case={} selected={:?} expected={expected:?} compiled={observed:?}",
                    case.label, case.selected,
                );
                // Exact values, NULL placeholders, error order/selected ordinal,
                // full diagnostics are pinned. Any caught panic fails this
                // expected Rows result; every observation owns a fresh instance.
                assert_eq!(observed, expected, "{}", case.label);
                if matches!(case.label, "MIN_MAX_only_MIN" | "MAX_MIN_only_MIN") {
                    let Observation::Rows(_, errors) = &observed else {
                        panic!("MIN must be a recoverable selected-row error")
                    };
                    assert_eq!(errors.len(), 1);
                    assert_eq!(errors[0].0, 0);
                    assert_eq!(
                        errors[0].1,
                        format!(
                            "CAST failed: from Date32 to {target:?}: invalid Date32 value -2147483648"
                        )
                    );
                }
                if matches!(
                    case.label,
                    "empty_demand" | "safe_hidden_MAX_NULL" | "safe_inactive_MAX"
                ) {
                    let Observation::Rows(values, errors) = &observed else {
                        panic!("inactive or NULL extreme cannot panic")
                    };
                    assert_eq!(values.dtype, target);
                    assert_eq!(values.bits.len(), case.selected.len());
                    assert!(errors.is_empty());
                }
            }
        }
    }
}
#[test]
fn date_float_review_probe_checked_compiler_f32_invocation_order() {
    run_probe(DataType::Float32);
}
#[test]
fn date_float_review_probe_checked_compiler_f64_invocation_order() {
    run_probe(DataType::Float64);
}
