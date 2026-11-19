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

//! The fragment boundary forwards original unit diagnostics without another callback.
use super::*;
use novarocks_expression::ExprCompileError;
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::{
    ConstantError, FunctionBindingError, FunctionSpecializationFailure, KernelFailure,
};
use novarocks_type_contract::CompilePhase;
use std::sync::atomic::{AtomicUsize, Ordering};
struct FooterControl(AtomicUsize);
impl PureCompileControl for FooterControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(CompileControlError::Cancelled)
        }
    }
}
fn refuses() -> Vec<ExprCompileError> {
    let mut cases = vec![
        ExprCompileError::Construction(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ),
        ExprCompileError::Reference(novarocks_constant_contract::ConstantTableError::Constant(
            ConstantError::Limit("original constant extent"),
        )),
        ExprCompileError::Arithmetic(novarocks_functions::ArithmeticPrepareError::Kernel(
            KernelFailure::ResourceExhausted,
        )),
        ExprCompileError::Cast(novarocks_functions::CastPrepareError::Kernel(
            KernelFailure::DeadlineExceeded,
        )),
        ExprCompileError::Comparison(novarocks_functions::ComparisonPrepareError::Kernel(
            KernelFailure::Cancelled,
        )),
        ExprCompileError::Specialization(FunctionSpecializationFailure::Kernel(
            KernelFailure::ResourceExhausted,
        )),
    ];
    use novarocks_type_contract::owned_resources::{
        arrow_metadata::hashmap_conversion_requests, btree::BTreeResourceError,
    };
    cases.extend([
        ExprCompileError::BTree(hashmap_conversion_requests(usize::MAX).unwrap_err()),
        ExprCompileError::BTree(BTreeResourceError::SourceModel(
            "original metadata source model",
        )),
    ]);
    cases
}
#[test]
fn pure_and_created_expression_refusals_keep_source_before_later_footer_control() {
    // The SDK owns its inner completion. Both fragment carriers must preserve
    // that completed owner's original refusal at the enclosing completion.
    for created in [false, true] {
        for source in refuses() {
            let expected = source.failure_fact();
            let original = ExpressionLoweringError::from(source);
            assert_eq!(original.failure_fact(), expected);
            assert!(std::error::Error::source(&original).is_some());
            let control = FooterControl(AtomicUsize::new(0));
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            work.step().unwrap();
            let error: crate::FragmentCreationError<std::convert::Infallible> = if created {
                ExpressionCreationFailure::Owner(original).into()
            } else {
                original.into()
            };
            let result: Result<(), _> = crate::creation::finish_attempt(Err(error), work);
            assert_eq!(result.unwrap_err().failure_fact(), expected);
            assert_eq!(
                control.0.load(Ordering::SeqCst),
                1,
                "no footer callback after the unit author's refusal"
            );
        }
    }
}
#[test]
fn invariant_compile_interruption_survives_each_function_and_both_footer_paths() {
    let raw = novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel;
    let expected = raw.failure_fact();
    let cause = CompileControlError::from_domain(raw);
    let converted: Vec<ExprCompileError> = vec![
        cause.clone().into(),
        novarocks_functions::ArithmeticPrepareError::Kernel(KernelFailure::CompileControl(
            cause.clone(),
        ))
        .into(),
        novarocks_functions::CastPrepareError::Kernel(KernelFailure::CompileControl(cause.clone()))
            .into(),
        novarocks_functions::ComparisonPrepareError::Kernel(KernelFailure::CompileControl(
            cause.clone(),
        ))
        .into(),
        FunctionSpecializationFailure::Kernel(KernelFailure::CompileControl(cause.clone())).into(),
        FunctionSpecializationFailure::Binding(FunctionBindingError::Control(cause.clone())).into(),
    ];
    for source in converted {
        assert_eq!(source.failure_fact(), expected);
        let original = ExpressionLoweringError::from(source);
        let ExpressionLoweringError::Control(actual) = &original else {
            panic!("interruption role")
        };
        assert_eq!(actual, &cause);
        let control = FooterControl(AtomicUsize::new(0));
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        work.step().unwrap();
        let error: crate::FragmentCreationError<std::convert::Infallible> = original.into();
        let result: Result<(), _> = crate::creation::finish_attempt(Err(error), work);
        let crate::FragmentCreationError::Control(actual) = result.unwrap_err() else {
            panic!("interruption role")
        };
        assert_eq!(actual, cause);
        assert_eq!(actual.failure_fact(), expected);
        assert_eq!(control.0.load(Ordering::SeqCst), 1);
    }
}
#[test]
fn ordinary_expression_validation_still_observes_pending_footer() {
    for created in [false, true] {
        let control = FooterControl(AtomicUsize::new(0));
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        work.step().unwrap();
        let original =
            ExpressionLoweringError::from(ExprCompileError::Invalid("ordinary validation"));
        let error: crate::FragmentCreationError<std::convert::Infallible> = if created {
            ExpressionCreationFailure::Owner(original).into()
        } else {
            original.into()
        };
        let result: Result<(), _> = crate::creation::finish_attempt(Err(error), work);
        assert!(matches!(
            result,
            Err(crate::FragmentCreationError::Control(
                CompileControlError::Cancelled
            ))
        ));
        assert_eq!(control.0.load(Ordering::SeqCst), 2);
    }
}
