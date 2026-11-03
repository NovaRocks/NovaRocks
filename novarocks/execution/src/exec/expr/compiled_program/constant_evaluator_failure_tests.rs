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
use arrow::array::Int64Array;
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::KernelDiagnostic;
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use std::sync::atomic::{AtomicBool, AtomicUsize};

// Each source is authored once, then cloned through the actual failure path.
// SharedFailureCause equality preserves the originating Arc, not just text.
pub(super) fn original_kernel_causes() -> Vec<KernelFailure> {
    let reason = LayoutResourceError::Arithmetic.failure_fact().reason();
    vec![
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
        KernelFailure::Capacity(novarocks_functions::KernelCapacityRefusal::new(
            reason, 16, 8, 16,
        )),
        KernelFailure::from_domain(LayoutResourceError::SourceModel),
        KernelFailure::CompileControl(novarocks_type_contract::CompileControlError::Cancelled),
        KernelFailure::CompileControl(
            novarocks_type_contract::CompileControlError::DeadlineExceeded,
        ),
        KernelFailure::CompileControl(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        ),
        KernelFailure::CompileControl(novarocks_type_contract::CompileControlError::from_domain(
            LayoutResourceError::Arithmetic,
        )),
        KernelFailure::CompileControl(novarocks_type_contract::CompileControlError::from_domain(
            LayoutResourceError::SourceModel,
        )),
    ]
}
pub(super) fn assert_original_entry_failure(actual: ConstantNodeFailure, expected: &KernelFailure) {
    match expected {
        // A caller's explicit CompileControl wrapper is the provenance. Bare
        // kernel cancellation/resource variants remain original Kernel errors.
        KernelFailure::CompileControl(expected) => {
            let ConstantNodeFailure::Control(actual) = actual else {
                panic!("compile wrapper was lost: {actual:?}")
            };
            assert_eq!(&actual, expected);
            assert_eq!(actual.failure_fact(), expected.failure_fact());
            if let (
                novarocks_type_contract::CompileControlError::Source(actual),
                novarocks_type_contract::CompileControlError::Source(expected),
            ) = (&actual, expected)
            {
                assert!(std::ptr::eq(
                    actual.downcast_ref::<LayoutResourceError>().unwrap(),
                    expected.downcast_ref::<LayoutResourceError>().unwrap()
                ));
            }
        }
        expected => {
            let ConstantNodeFailure::Kernel(actual) = actual else {
                panic!("original kernel error was reclassified: {actual:?}")
            };
            assert_eq!(&actual, expected);
            assert_eq!(actual.failure_fact(), expected.failure_fact());
            if matches!(expected, KernelFailure::Capacity(_)) {
                let facts = actual.failure_fact();
                let resource = facts.resource().unwrap();
                assert_eq!(
                    (resource.requested, resource.limit, resource.committed),
                    (Some(16), Some(8), Some(16))
                );
            }
        }
    }
}

#[test]
fn constant_node_every_kernel_checkpoint_keeps_all_seven_original_causes() {
    struct Stop {
        calls: AtomicUsize,
        stop: usize,
        cause: KernelFailure,
    }
    impl KernelEvaluationControl for Stop {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            let current = self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                current <= self.stop,
                "entry observed control after the original refusal"
            );
            if current == self.stop {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("arithmetic cannot wait")
        }
    }
    let compile = crate::exec::expr::test_support::HarnessControl;
    let children: Vec<_> = [20, 22]
        .into_iter()
        .map(|n| {
            let ty = FunctionValueType::new(DataType::Int64, false);
            let array = Int64Array::from(vec![n]);
            ConstantValue::from_scalar_array(
                Arc::new(ty.try_to_field("source").unwrap()),
                ty,
                array.to_data(),
                crate::exec::expr::test_support::constant_policy(),
                CompilePhase::FunctionSpecialization,
                &compile,
            )
            .unwrap()
        })
        .collect();
    let result = FunctionValueType::new(DataType::Int64, true);
    let node = || ConstantNode::Arithmetic {
        operator: novarocks_type_contract::ArithmeticOperator::Add,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::ReportError,
        allow_throw_exception: true,
    };
    let baseline = Stop {
        calls: AtomicUsize::new(0),
        stop: usize::MAX,
        cause: KernelFailure::Cancelled,
    };
    assert!(matches!(
        evaluate_with_kernel_control(node(), &children, &result, &compile, &baseline).unwrap(),
        ConstantNodeOutcome::Value(_)
    ));
    for stop in 0..baseline.calls.load(Ordering::SeqCst) {
        for cause in original_kernel_causes() {
            let control = Stop {
                calls: AtomicUsize::new(0),
                stop,
                cause: cause.clone(),
            };
            let actual =
                evaluate_with_kernel_control(node(), &children, &result, &compile, &control)
                    .unwrap_err();
            assert_original_entry_failure(actual, &cause);
            assert_eq!(control.calls.load(Ordering::SeqCst), stop + 1);
        }
    }
}

#[test]
fn constant_node_original_primitive_refusal_does_not_start_a_kernel_footer() {
    struct PrepareControl {
        entered: AtomicBool,
    }
    impl PureCompileControl for PrepareControl {
        fn checkpoint(
            &self,
            _: CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            self.entered.store(true, Ordering::SeqCst);
            Ok(())
        }
    }
    struct NoFooter<'a> {
        prepare: &'a PrepareControl,
        calls: AtomicUsize,
    }
    impl KernelEvaluationControl for NoFooter<'_> {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            assert!(
                !self.prepare.entered.load(Ordering::SeqCst),
                "outer primitive error must not start a fallible kernel footer"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("constant arithmetic cannot wait")
        }
    }
    let ordinary = crate::exec::expr::test_support::HarnessControl;
    let ty = FunctionValueType::new(DataType::Int64, false);
    let children: Vec<_> = [20, 22]
        .into_iter()
        .map(|n| {
            ConstantValue::from_scalar_array(
                Arc::new(ty.try_to_field("original source").unwrap()),
                ty.clone(),
                Int64Array::from(vec![n]).to_data(),
                crate::exec::expr::test_support::constant_policy(),
                CompilePhase::FunctionSpecialization,
                &ordinary,
            )
            .unwrap()
        })
        .collect();
    let prepare = PrepareControl {
        entered: AtomicBool::new(false),
    };
    let kernel = NoFooter {
        prepare: &prepare,
        calls: AtomicUsize::new(0),
    };
    let error = evaluate_with_kernel_control(
        ConstantNode::Arithmetic {
            operator: novarocks_type_contract::ArithmeticOperator::Add,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::ReportError,
            allow_throw_exception: true,
        },
        &children,
        &ty,
        &prepare,
        &kernel,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ConstantNodeFailure::Primitive(ProgramPrimitiveError::Arithmetic(
            novarocks_functions::ArithmeticPrepareError::TypeMismatch
        ))
    ));
    assert!(prepare.entered.load(Ordering::SeqCst));
    assert!(kernel.calls.load(Ordering::SeqCst) > 0);
}
