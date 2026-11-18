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
use arrow::record_batch::RecordBatch;
use novarocks_expression::{
    EvaluationInput, ExprEvaluationFailure,
    testkit::{
        CallbackControl, PassThroughCallHost, callback_causes,
        filter_conjunction_fixture::{Control, FilterShape, batch, fixture, group},
    },
};
use novarocks_functions::{KernelFailure, Selection};
use std::sync::Arc;

#[test]
fn filter_conjunction_actual_every_runtime_callback_seven_causes_prefix_no_tail_latch() {
    let f = fixture(FilterShape::TwoCounts);
    let input = batch(&f, &[0, 1, 0], &[]);
    let success = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    group(&f)
        .evaluate(
            EvaluationInput::Batch {
                batch: &input,
                source: None,
            },
            Selection::all(3),
            &success,
            &mut PassThroughCallHost,
        )
        .unwrap();
    let trace = success.trace();
    assert!(!trace.is_empty());
    for index in 0..trace.len() {
        for cause in callback_causes(["source invalid", "source internal", "source operational"]) {
            let stop = CallbackControl::new(cause.clone(), index + 1);
            let mut instance = group(&f);
            let error = instance
                .evaluate(
                    EvaluationInput::Batch {
                        batch: &input,
                        source: None,
                    },
                    Selection::all(3),
                    &stop,
                    &mut PassThroughCallHost,
                )
                .unwrap_err();
            assert!(matches!(error, ExprEvaluationFailure::Kernel(actual) if actual == cause));
            let mut attempted = stop.trace();
            // Shared recording includes the rejecting callback. Compare the
            // accepted prefix and its exact rejection separately.
            assert_eq!(attempted.pop(), Some(trace[index]));
            assert_eq!(attempted, trace[..index]);
            let len = stop.trace().len();
            assert!(matches!(
                instance
                    .evaluate(
                        EvaluationInput::Batch {
                            batch: &input,
                            source: None
                        },
                        Selection::all(3),
                        &stop,
                        &mut PassThroughCallHost,
                    )
                    .unwrap_err(),
                ExprEvaluationFailure::Kernel(KernelFailure::InstanceFailed)
            ));
            assert_eq!(stop.trace().len(), len);
        }
    }
}

#[test]
fn filter_conjunction_actual_row_quantum_and_empty_schema_validation() {
    let f = fixture(FilterShape::TwoCounts);
    let input = batch(&f, &vec![0; 321], &[]);
    let control = CallbackControl::new(KernelFailure::Cancelled, usize::MAX);
    assert_eq!(
        group(&f)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &input,
                    source: None
                },
                Selection::all(321),
                &control,
                &mut PassThroughCallHost,
            )
            .unwrap()
            .len(),
        321
    );
    assert!(control.trace().contains(&256));
    let mut fields = input
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields[0] = fields[0].clone().with_name("not_the_original_field");
    let wrong = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        input.columns().to_vec(),
    )
    .unwrap();
    assert!(
        group(&f)
            .evaluate(
                EvaluationInput::Batch {
                    batch: &wrong,
                    source: None
                },
                Selection::try_sparse(321, &[]).unwrap(),
                &Control,
                &mut PassThroughCallHost,
            )
            .is_err()
    );
}
