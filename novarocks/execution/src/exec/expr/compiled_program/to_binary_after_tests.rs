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
use super::*;
#[path = "to_binary_fixture.rs"]
mod fixture;
#[test]
fn to_binary_corrected_called_on_null_eager_both_overloads_compile_and_evaluate() {
    use novarocks_type_contract::FunctionNullBehavior;
    for count in [1, 2] {
        let program = fixture::program(count);
        let batch = fixture::source_batch(&program, count);
        for call in root_unit(&program).calls().values() {
            assert_eq!(
                call.call_contract().effects().null_behavior,
                FunctionNullBehavior::CalledOnNull
            );
            assert_eq!(
                call.call_contract().effects().argument_control,
                novarocks_type_contract::ArgumentControl::Eager
            );
        }
        let all_expected = if count == 1 {
            vec![
                Some(vec![255]),
                Some(vec![]),
                None,
                None,
                Some(vec![0]),
                None,
            ]
        } else {
            vec![
                Some(vec![255]),
                Some(vec![]),
                Some(b"abc".to_vec()),
                None,
                Some(vec![0]),
                None,
            ]
        };
        for selection in [
            Selection::all(6),
            Selection::try_sparse(6, &[0, 2, 3, 4]).unwrap(),
            Selection::try_sparse(6, &[]).unwrap(),
        ] {
            let expected: Vec<_> = selection
                .iter()
                .map(|row| all_expected[row].clone())
                .collect();
            let mut instance = CompiledExpressionInstance::try_new(
                root_unit(&program).clone(),
                root(&program).root,
                &Control,
            )
            .unwrap();
            let out = instance.evaluate(&batch, selection, &Control).unwrap();
            assert_eq!(fixture::binary_values(out.values()), expected);
            assert!(out.errors().is_empty());
        }
    }
}

#[test]
fn to_binary_both_overloads_constructor_preserves_every_callback_cause() {
    for count in [1, 2] {
        let program = fixture::program(count);
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        let _ = CompiledExpressionInstance::try_new(
            root_unit(&program).clone(),
            root(&program).root,
            &recorder,
        )
        .unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        assert!(trace.iter().all(|n| *n <= 256));
        for index in 1..=trace.len() {
            for cause in causes() {
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(CompiledExpressionInstance::try_new(root_unit(&program).clone(),root(&program).root,&control),Err(actual) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
            }
        }
    }
}
#[test]
fn to_binary_both_overloads_evaluation_preserves_every_callback_cause_and_latch() {
    use arrow::array::{ArrayRef, StringArray};
    for count in [1, 2] {
        let program = fixture::program(count);
        let long = "00ff".repeat(160);
        let input: ArrayRef = Arc::new(StringArray::from(
            (0..321)
                .map(|i| {
                    if i == 2 {
                        Some(long.as_str())
                    } else if i % 4 == 0 {
                        None
                    } else if i % 4 == 1 {
                        Some("")
                    } else {
                        Some("00")
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let mut arrays = vec![input];
        if count == 2 {
            arrays.push(Arc::new(StringArray::from(
                (0..321)
                    .map(|i| if i % 2 == 0 { None } else { Some("hex") })
                    .collect::<Vec<_>>(),
            )));
        }
        let batch = RecordBatch::try_new(
            program.graph().nodes()[1].output_layout().schema().clone(),
            arrays,
        )
        .unwrap();
        let recorder = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
        let mut evaluator = CompiledExpressionInstance::try_new(
            root_unit(&program).clone(),
            root(&program).root,
            &Control,
        )
        .unwrap();
        let _ = evaluator
            .evaluate(&batch, Selection::all(321), &recorder)
            .unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        assert!(trace.iter().all(|n| *n <= 256));
        for index in 1..=trace.len() {
            for cause in causes() {
                let mut evaluator = CompiledExpressionInstance::try_new(
                    root_unit(&program).clone(),
                    root(&program).root,
                    &Control,
                )
                .unwrap();
                let control = CallbackControl::new(cause.clone(), index);
                assert!(
                    matches!(evaluator.evaluate(&batch,Selection::all(321),&control),Err(actual) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..index]);
                let after = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
                assert!(matches!(
                    evaluator.evaluate(&batch, Selection::all(321), &after),
                    Err(KernelFailure::InstanceFailed)
                ));
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
