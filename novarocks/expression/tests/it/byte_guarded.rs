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

use arrow::{array::Array, datatypes::DataType};
use novarocks_expression::{
    EvaluationInput, ExprEvaluationFailure,
    testkit::{
        CallbackControl, PassThroughCallHost, callback_causes,
        guarded_fixtures::{
            GUARDED_CONTROL as Control,
            byte::{byte_batch, byte_program, bytes_of, carriers},
        },
    },
};
use novarocks_functions::{
    KernelFailure, ScalarInvocationActivation, ScalarInvocationScope, Selection,
};

fn causes() -> [KernelFailure; 7] {
    callback_causes([
        "guarded-origin-invalid",
        "guarded-origin-internal",
        "guarded-origin-operational",
    ])
}

#[test]
fn four_byte_carriers_nested_if_case_coalesce_keep_sparse_sliced_values_and_success_nulls() {
    for ty in carriers() {
        let program = byte_program(ty.clone(), false);
        let mut flags = vec![Some(false); 8];
        flags[1] = Some(true);
        flags[3] = None;
        flags[5] = Some(true);
        let mut left: Vec<Option<&[u8]>> = vec![Some(b"unselected"); 8];
        left[1] = Some(b"");
        left[3] = None;
        left[5] = None;
        left[7] = Some(if matches!(ty, DataType::Binary | DataType::LargeBinary) {
            b"\0\xff\x80"
        } else {
            "中\0e\u{301}😀".as_bytes()
        });
        let right: Vec<Option<&[u8]>> = vec![Some(b"fallback"); 8];
        let input = byte_batch(&program, flags, &left, &right, vec![None; 8]);
        let rows = [1, 3, 5, 7];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let mut evaluator = program.instance(&Control);
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(output.values().data_type(), &ty);
        assert_eq!(output.selection(), selection);
        assert_eq!(
            bytes_of(&output),
            vec![
                Some(vec![]),
                Some(b"fallback".to_vec()),
                None,
                left[7].map(<[u8]>::to_vec)
            ]
        );
        assert!(output.errors().is_empty());
        let next = byte_batch(
            &program,
            vec![Some(false), None, Some(true)],
            &[None, None, Some(b"next")],
            &[None, Some(b"last"), Some(b"unused")],
            vec![None; 3],
        );
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &next,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(3), Selection::all(3)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(
            bytes_of(&output),
            vec![None, Some(b"last".to_vec()), Some(b"next".to_vec())]
        );
        assert!(output.errors().is_empty());
    }
}

#[test]
fn four_byte_carriers_required_round_when_error_is_null_placeholder_without_branch_fallback() {
    for ty in carriers() {
        let program = byte_program(ty.clone(), true);
        let mut decimals = vec![Some(25); 8];
        decimals[1] = None;
        decimals[5] = Some(10_i128.pow(38) - 1);
        let input = byte_batch(
            &program,
            vec![None; 8],
            &[Some(b"then".as_slice()); 8],
            &[Some(b"else".as_slice()); 8],
            decimals,
        );
        let rows = [1, 3, 5, 7];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let mut evaluator = program.instance(&Control);
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None,
                },
                ScalarInvocationScope::new(selection, selection),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(output.values().data_type(), &ty);
        assert_eq!(
            bytes_of(&output),
            vec![
                Some(b"then".to_vec()),
                Some(b"else".to_vec()),
                None,
                Some(b"else".to_vec())
            ]
        );
        assert_eq!(output.errors().len(), 1);
        assert_eq!(output.errors()[0].selected_ordinal(), 2);
        assert_eq!(output.selection().row(2), Some(5));
        assert!(output.errors()[0].message().contains("overflow"));
        let next = byte_batch(
            &program,
            vec![None],
            &[Some(b"clean")],
            &[Some(b"right")],
            vec![None],
        );
        let output = evaluator
            .evaluate(
                EvaluationInput::Batch {
                    batch: &next,
                    source: None,
                },
                ScalarInvocationScope::new(Selection::all(1), Selection::all(1)),
                ScalarInvocationActivation::Activated,
                &Control,
                &mut PassThroughCallHost,
            )
            .unwrap();
        assert_eq!(bytes_of(&output), vec![Some(b"clean".to_vec())]);
        assert!(output.errors().is_empty());
    }
}

#[test]
fn every_actual_byte_guarded_callback_preserves_seven_causes_and_refuses_replay() {
    let program = byte_program(DataType::Utf8, true);
    let input = byte_batch(
        &program,
        vec![None; 320],
        &[Some("中".as_bytes()); 320],
        &[Some(b"fallback".as_slice()); 320],
        (0..320)
            .map(|row| match row % 3 {
                0 => None,
                1 => Some(25),
                _ => Some(10_i128.pow(38) - 1),
            })
            .collect(),
    );
    let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    let output = program
        .instance(&Control)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
            ScalarInvocationActivation::Activated,
            &recorder,
            &mut PassThroughCallHost,
        )
        .unwrap();
    assert_eq!(output.errors().len(), 106);
    assert_eq!(
        bytes_of(&output),
        (0..320)
            .map(|row| match row % 3 {
                0 => Some("中".as_bytes().to_vec()),
                1 => Some(b"fallback".to_vec()),
                _ => None,
            })
            .collect::<Vec<_>>()
    );
    let trace = recorder.trace();
    assert!(trace.contains(&256));
    assert!(trace.iter().all(|units| *units <= 256));
    for index in 1..=trace.len() {
        for cause in causes() {
            let mut evaluator = program.instance(&Control);
            let control = CallbackControl::new(cause.clone(), index);
            assert!(
                matches!(evaluator.evaluate(EvaluationInput::Batch { batch: &input, source: None }, ScalarInvocationScope::new(Selection::all(320), Selection::all(320)), ScalarInvocationActivation::Activated, &control, &mut PassThroughCallHost), Err(ExprEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..index]);
            let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
            assert!(matches!(
                evaluator.evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None
                    },
                    ScalarInvocationScope::new(Selection::all(320), Selection::all(320)),
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
