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

//! Private evaluator completion preserves the originating failure.
use super::super::*;
use novarocks_failure_contract::FailureProjection;
use novarocks_type_contract::{CompileControlError, owned_resources::layout::LayoutResourceError};
use std::sync::atomic::AtomicUsize;
struct Footer(AtomicUsize);
impl KernelEvaluationControl for Footer {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(KernelFailure::Cancelled)
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("a synchronous footer cannot wait")
    }
}
#[test]
fn every_originating_failure_skips_fallible_tail_but_success_observes_it() {
    let raw = LayoutResourceError::SourceModel;
    let expected = raw.failure_fact();
    let cause = CompileControlError::from_domain(raw);
    let footer = Footer(AtomicUsize::new(0));
    let control = ObservedControl {
        original: &footer,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work: Work<'_, '_> = Work {
        control: &control,
        pending: 1,
        scalar_host: None,
    };
    let result: Result<(), _> = work.finish(Err(KernelFailure::CompileControl(cause.clone())));
    let KernelFailure::CompileControl(actual) = result.unwrap_err() else {
        panic!("original interruption")
    };
    assert_eq!(actual, cause);
    assert_eq!(actual.failure_fact(), expected);
    assert_eq!(footer.0.load(Ordering::SeqCst), 0);
    let expected = invalid("ordinary validation");
    let result: Result<(), _> = work.finish(Err(expected.clone()));
    assert_eq!(result.unwrap_err(), expected);
    assert_eq!(footer.0.load(Ordering::SeqCst), 0);
    let result = work.finish(Ok(()));
    assert_eq!(result.unwrap_err(), KernelFailure::Cancelled);
    assert_eq!(footer.0.load(Ordering::SeqCst), 1);
}

#[test]
fn original_occurrence_survives_late_cancel_after_real_charged_resource_settlement() {
    let expected = KernelFailure::from_domain(LayoutResourceError::SourceModel);
    let footer = Footer(AtomicUsize::new(0));
    let observed = ObservedControl {
        original: &footer,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work: Work<'_, '_> = Work {
        control: &observed,
        pending: 1,
        scalar_host: None,
    };
    let result: Result<(), _> = Err(ExprEvaluationFailure::Kernel(expected.clone()));
    let actual = work.finish_evaluation(result).unwrap_err();
    assert_eq!(actual, ExprEvaluationFailure::Kernel(expected));
    assert_eq!(footer.0.load(Ordering::SeqCst), 0);
    assert!(!observed.refused.load(Ordering::Relaxed));
    assert_eq!(
        KernelEvaluationControl::checkpoint(&footer, 0),
        Err(KernelFailure::Cancelled)
    );
    assert_eq!(footer.0.load(Ordering::SeqCst), 1);
}
