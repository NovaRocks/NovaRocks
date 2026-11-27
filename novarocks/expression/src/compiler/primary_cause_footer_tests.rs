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
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::KernelFailure;
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
fn refuses() -> Vec<(ExprCompileError, novarocks_failure_contract::FailureFact)> {
    fn original<E: FailureProjection + Into<ExprCompileError>>(
        source: E,
    ) -> (ExprCompileError, novarocks_failure_contract::FailureFact) {
        let expected = source.failure_fact();
        (source.into(), expected)
    }
    let allocation = Vec::<u64>::new().try_reserve_exact(usize::MAX).unwrap_err();
    let publication = crate::ProgramCreationFailure::<
        std::convert::Infallible,
        std::convert::Infallible,
    >::Capacity(allocation);
    let publication_fact = publication.failure_fact();
    let geometry =
        novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic;
    let mut cases = vec![
        (ExprCompileError::Publication(publication), publication_fact),
        (
            ExprCompileError::Construction(geometry),
            geometry.failure_fact(),
        ),
        original(ConstantReferenceError::Constant(ConstantError::Limit(
            "original constant extent",
        ))),
        original(novarocks_functions::ArithmeticPrepareError::Kernel(
            KernelFailure::ResourceExhausted,
        )),
        original(novarocks_functions::CastPrepareError::Kernel(
            KernelFailure::DeadlineExceeded,
        )),
        original(novarocks_functions::ComparisonPrepareError::Kernel(
            KernelFailure::Cancelled,
        )),
        original(FunctionSpecializationFailure::Kernel(
            KernelFailure::ResourceExhausted,
        )),
    ];
    use novarocks_type_contract::owned_resources::{
        arrow_metadata::hashmap_conversion_requests, btree::BTreeResourceError,
    };
    for source in [
        hashmap_conversion_requests(usize::MAX).unwrap_err(),
        BTreeResourceError::SourceModel("original metadata source model"),
    ] {
        let expected = source.failure_fact();
        cases.push(original(source));
        let binding = novarocks_functions::FunctionBindingCreationFailure::<
                std::convert::Infallible,
            >::Resource(
                novarocks_functions::FunctionBindingConstructionResource::BTree(source)
            );
        let binding: ExprCompileFailure<std::convert::Infallible> = binding.into();
        cases.push((binding.into_pure(), expected));
    }
    cases
}
#[test]
fn pure_and_created_expression_refusals_keep_source_before_later_footer_control() {
    for (original, expected) in refuses() {
        assert_eq!(original.failure_fact(), expected);
        assert!(
            !matches!(original, ExprCompileError::Control(_)),
            "the original domain source must survive conversion"
        );
        assert!(std::error::Error::source(&original).is_some());
        let control = FooterControl(AtomicUsize::new(0));
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        work.step().unwrap();
        let result: Result<(), _> = finish(Err(original), &mut work);
        assert_eq!(result.unwrap_err().failure_fact(), expected);
        assert_eq!(
            control.0.load(Ordering::SeqCst),
            1,
            "no footer callback after an owner's refusal"
        );
    }
}
#[test]
fn invariant_compile_interruption_survives_each_function_and_both_footer_paths() {
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    let raw = LayoutResourceError::SourceModel;
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
    for original in converted {
        assert_eq!(original.failure_fact(), expected);
        let ExprCompileError::Control(actual) = &original else {
            panic!("interruption role")
        };
        assert_eq!(actual, &cause);
        let control = FooterControl(AtomicUsize::new(0));
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        work.step().unwrap();
        let result: Result<(), _> = finish(Err(original), &mut work);
        let original = result.unwrap_err();
        assert_eq!(original.failure_fact(), expected);
        assert_eq!(control.0.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn ordinary_expression_validation_still_observes_pending_footer() {
    let control = FooterControl(AtomicUsize::new(0));
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    work.step().unwrap();
    let result: Result<(), _> = finish(
        Err(ExprCompileError::Invalid("ordinary validation")),
        &mut work,
    );
    assert!(matches!(
        result,
        Err(ExprCompileError::Control(CompileControlError::Cancelled))
    ));
    assert_eq!(control.0.load(Ordering::SeqCst), 2);
}
