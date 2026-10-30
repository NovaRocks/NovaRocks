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

//! Function-domain projections. Codes describe function facts, never SQL or
//! recovery decisions. Control observation is not accepted client cancellation.

use crate::{KernelFailure, ScalarResourceError};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};
use std::num::NonZeroU16;

fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(
            domain::KERNEL,
            NonZeroU16::new(code).expect("nonzero reason"),
        ),
    )
}
impl FailureProjection for KernelFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Source(source) => source.failure_fact(),
            Self::CompileControl(source) => source.failure_fact(),
            Self::Capacity(source) => source.failure_fact(),
            Self::Cancelled => {
                FailureFact::stop_observation(fact(FailureClass::Control, 1).reason())
            }
            Self::DeadlineExceeded => {
                FailureFact::stop_observation(fact(FailureClass::Control, 2).reason())
            }
            Self::ResourceExhausted => fact(FailureClass::Resource, 3),
            Self::InvalidProgram(_) => fact(FailureClass::Invariant, 4),
            Self::Internal(_) => fact(FailureClass::Invariant, 5),
            Self::Operational(_) => fact(FailureClass::Unexpected, 6),
            Self::InstanceFailed => fact(FailureClass::Invariant, 7),
        }
    }
}
impl FailureProjection for ScalarResourceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Arithmetic => fact(FailureClass::Resource, 20),
            Self::SourceModel(_) => fact(FailureClass::Invariant, 21),
            Self::Invariant(_) => fact(FailureClass::Invariant, 22),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_arithmetic_is_resource_not_numeric_data_overflow() {
        let projected = ScalarResourceError::Arithmetic.failure_fact();
        assert_eq!(projected.class(), FailureClass::Resource);
        assert_eq!(projected.reason().code(), 20);
        assert_eq!(projected.resource(), None);
    }
    #[test]
    fn observed_stop_and_operational_diagnostic_do_not_invent_origin() {
        assert_eq!(KernelFailure::Cancelled.failure_fact().reason().code(), 1);
        let error =
            KernelFailure::Operational(crate::KernelDiagnostic::new("S3 403 numeric overflow"));
        assert_eq!(error.failure_fact().class(), FailureClass::Unexpected);
        assert_eq!(error.failure_fact().reason().code(), 6);
    }
}

impl FailureProjection for crate::DataFailureReason {
    fn failure_fact(&self) -> FailureFact {
        let code = match self {
            Self::InvalidValue => 40,
            Self::NumericOutOfRange => {
                return FailureFact::new(
                    FailureClass::Data,
                    novarocks_function_contract::kernel_failure::NUMERIC_OUT_OF_RANGE,
                );
            }
            Self::InvalidEncoding => 42,
            Self::Domain(reason) => return FailureFact::new(FailureClass::Data, *reason),
        };
        fact(FailureClass::Data, code)
    }
}
impl FailureProjection for crate::RowDataError {
    fn failure_fact(&self) -> FailureFact {
        self.projected_fact()
    }
}

impl FailureProjection for crate::InvocationData {
    fn failure_fact(&self) -> FailureFact {
        self.projected_fact()
    }
}
impl FailureProjection for crate::ScalarInvocationData {
    fn failure_fact(&self) -> FailureFact {
        self.reason().failure_fact()
    }
}
impl FailureProjection for crate::WindowInvocationData {
    fn failure_fact(&self) -> FailureFact {
        self.projected_fact()
    }
}

#[cfg(test)]
mod row_reason_tests {
    use super::*;
    #[test]
    fn row_reason_survives_remapping_and_does_not_follow_diagnostic_text() {
        let numeric = crate::RowDataError::with_reason(
            2,
            crate::DataFailureReason::NumericOutOfRange,
            "invalid encoding",
        );
        let encoding = crate::RowDataError::with_reason(
            2,
            crate::DataFailureReason::InvalidEncoding,
            "numeric overflow",
        );
        assert_eq!(
            numeric
                .with_selected_ordinal(19)
                .failure_fact()
                .reason()
                .code(),
            41
        );
        assert_eq!(numeric.with_selected_ordinal(19).selected_ordinal(), 19);
        assert_eq!(encoding.failure_fact().reason().code(), 42);
        assert_ne!(numeric.failure_fact(), encoding.failure_fact());
    }
}

impl FailureProjection for crate::EvaluationContractError {
    fn failure_fact(&self) -> FailureFact {
        let code = match self {
            Self::InvalidSelection => 60,
            Self::ArgumentLength => 61,
            Self::ArgumentType => 62,
            Self::ArgumentSelection => 63,
            Self::ArgumentRowErrors => 64,
            Self::ResultLength => 65,
            Self::ResultType => 66,
            Self::InvalidRowErrors => 67,
        };
        fact(FailureClass::Invariant, code)
    }
}
#[cfg(test)]
mod evaluation_contract_tests {
    use super::*;
    #[test]
    fn invalid_selection_contract_remains_distinct_from_numeric_row_data() {
        let failure = crate::Selection::try_sparse(3, &[2, 1]).unwrap_err();
        assert_eq!(failure, crate::EvaluationContractError::InvalidSelection);
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        let data = crate::RowDataError::with_reason(
            0,
            crate::DataFailureReason::NumericOutOfRange,
            "invalid selection",
        );
        assert_ne!(failure.failure_fact(), data.failure_fact());
    }
}

impl novarocks_failure_contract::OuterFailureProjection for crate::EvaluationContractError {}
impl novarocks_failure_contract::OuterFailureProjection for crate::KernelFailure {}

#[cfg(test)]
mod kernel_source_tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn outer_source_keeps_original_type_fact_and_identity_without_entering_row_data() {
        let original = novarocks_type_contract::ValueTypeError::TooDeep;
        let expected = original.failure_fact();
        let failure = KernelFailure::from(original);
        assert_eq!(failure.failure_fact(), expected);
        assert_eq!(failure.failure_fact().class(), FailureClass::Resource);
        let copy = failure.clone();
        let a = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<novarocks_type_contract::ValueTypeError>()
            .unwrap();
        let b = copy
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<novarocks_type_contract::ValueTypeError>()
            .unwrap();
        assert!(std::ptr::eq(a, b));
        assert!(matches!(
            a,
            novarocks_type_contract::ValueTypeError::TooDeep
        ));
        assert_eq!(failure, copy);
    }
}

#[cfg(test)]
mod source_lifecycle_tests {
    use super::*;
    #[test]
    fn each_evaluation_reason_keeps_its_exact_source_in_the_outer_channel() {
        use crate::EvaluationContractError::*;
        for original in [
            InvalidSelection,
            ArgumentLength,
            ArgumentType,
            ArgumentSelection,
            ArgumentRowErrors,
            ResultLength,
            ResultType,
            InvalidRowErrors,
        ] {
            let failure = KernelFailure::from(original);
            crate::kernel_control::test_contract::exact(&failure, original);
        }
    }
    #[test]
    fn typed_resource_refusal_survives_a_later_lifecycle_invariant() {
        let failure = KernelFailure::from(novarocks_type_contract::ValueTypeError::TooDeep);
        let result = crate::evaluation_failure::finish_evaluation_lifecycle::<()>(
            Err(failure.clone().into()),
            || panic!("no optional validation after the original resource refusal"),
        );
        assert_eq!(result, Err(crate::EvaluationFailure::Kernel(failure)));
    }
}

#[cfg(test)]
mod capacity_refusal_tests {
    use super::*;
    #[test]
    fn compact_refusal_preserves_the_producer_reason_and_only_known_measurements() {
        let reason = FailureReason::new(domain::MEMORY, NonZeroU16::new(8).unwrap());
        let error = KernelFailure::Capacity(crate::KernelCapacityRefusal::new(reason, 16, 8, 16));
        let fact = error.failure_fact();
        assert_eq!(fact.reason(), reason);
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!(
            (resource.requested, resource.limit, resource.committed),
            (Some(16), Some(8), Some(16))
        );
        assert_eq!(
            (
                resource.request_scope,
                resource.constraint_scope,
                resource.live
            ),
            (None, None, None)
        );
        let result = crate::evaluation_failure::finish_evaluation_lifecycle::<()>(
            Err(error.clone().into()),
            || panic!("no optional validation after the original resource refusal"),
        );
        assert_eq!(result, Err(crate::EvaluationFailure::Kernel(error)));
    }
}

impl FailureProjection for crate::arrow_ipc_construction::IpcGeometryError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Overflow => fact(FailureClass::Resource, 148),
            Self::SourceModel => fact(FailureClass::Invariant, 149),
        }
    }
}

impl FailureProjection for crate::AggregateInputBatchError {
    fn failure_fact(&self) -> FailureFact {
        fact(FailureClass::Invariant, 150)
    }
}
impl FailureProjection for crate::AggregateBindOptionsError {
    fn failure_fact(&self) -> FailureFact {
        fact(
            FailureClass::Invariant,
            match self {
                Self::OrderKeyLengthMismatch { .. } => 151,
                Self::InvalidMaxOutputBytes(_) => 152,
            },
        )
    }
}
impl FailureProjection for crate::FunctionCatalogError {
    fn failure_fact(&self) -> FailureFact {
        fact(FailureClass::Invariant, 153)
    }
}

impl FailureProjection for crate::FunctionSpecializationFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Binding(cause) => cause.failure_fact(),
            Self::Effects(cause) => cause.failure_fact(),
            Self::Control(cause) => cause.failure_fact(),
            Self::Kernel(cause) => cause.failure_fact(),
            Self::MissingPureImplementation(_) => FailureFact::new(
                FailureClass::Unsupported,
                novarocks_function_contract::kernel_failure::MISSING_PURE_IMPLEMENTATION,
            ),
            Self::InvalidInput(_) => fact(FailureClass::Invariant, 601),
        }
    }
}
#[cfg(test)]
mod specialization_failure_tests {
    use super::*;
    use std::error::Error;
    #[test]
    fn missing_pure_and_nested_binding_keep_their_own_capability_facts_and_sources() {
        let overload = crate::FunctionOverloadId::try_new("fixture/missing/pure-v1").unwrap();
        let missing =
            crate::FunctionSpecializationFailure::MissingPureImplementation(overload.clone());
        assert_eq!(missing.failure_fact(), fact(FailureClass::Unsupported, 600));
        assert!(!missing.is_interruption());
        assert!(missing.source().is_none());
        assert!(
            matches!(missing, crate::FunctionSpecializationFailure::MissingPureImplementation(ref actual) if actual == &overload)
        );
        let original = crate::FunctionBindingError::UnavailableImplementation(overload);
        let nested = crate::FunctionSpecializationFailure::from(original.clone());
        assert_eq!(nested.failure_fact(), original.failure_fact());
        assert_eq!(nested.failure_fact().class(), FailureClass::Unsupported);
        assert_eq!(
            nested.failure_fact().reason().domain(),
            domain::FUNCTION_BINDING.get()
        );
        assert_eq!(nested.failure_fact().reason().code(), 2);
        assert_eq!(
            nested
                .source()
                .unwrap()
                .downcast_ref::<crate::FunctionBindingError>(),
            Some(&original),
        );
        assert!(!nested.is_interruption());
        for text in ["unsupported", "missing pure implementation", "cancelled"] {
            let invalid = crate::FunctionSpecializationFailure::InvalidInput(text);
            assert_eq!(invalid.failure_fact(), fact(FailureClass::Invariant, 601));
        }
        let ordinary = crate::FunctionSpecializationFailure::Binding(
            crate::FunctionBindingError::NoMatchingOverload,
        );
        assert_eq!(ordinary.failure_fact().class(), FailureClass::Data);
        for control in [
            novarocks_type_contract::CompileControlError::Cancelled,
            novarocks_type_contract::CompileControlError::DeadlineExceeded,
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        ] {
            let wrapped = crate::FunctionSpecializationFailure::from(
                crate::FunctionBindingError::Control(control.clone()),
            );
            assert_eq!(wrapped.failure_fact(), control.failure_fact());
            assert!(wrapped.is_interruption());
        }
    }

    #[test]
    fn specialization_retains_value_type_refusal_after_binding_conversion() {
        for original in [
            novarocks_type_contract::ValueTypeError::TooDeep,
            novarocks_type_contract::ValueTypeError::TooManyNodes,
            novarocks_type_contract::ValueTypeError::UnknownLogicalMetadata,
        ] {
            let error = crate::FunctionSpecializationFailure::from(
                crate::FunctionBindingError::from(original),
            );
            assert_eq!(error.failure_fact(), original.failure_fact());
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .downcast_ref::<novarocks_type_contract::ValueTypeError>(),
                Some(&original)
            );
        }
    }
    #[test]
    fn specialization_keeps_original_binding_control_and_effect_sources() {
        let sources = [
            crate::FunctionSpecializationFailure::Binding(
                crate::FunctionBindingError::NoMatchingOverload,
            ),
            crate::FunctionSpecializationFailure::Control(
                novarocks_type_contract::CompileControlError::DeadlineExceeded,
            ),
            crate::FunctionSpecializationFailure::Effects(
                novarocks_type_contract::EffectContractError::ProofScopeMismatch,
            ),
        ];
        for error in sources {
            let expected = match &error {
                crate::FunctionSpecializationFailure::Binding(cause) => cause.failure_fact(),
                crate::FunctionSpecializationFailure::Control(cause) => cause.failure_fact(),
                crate::FunctionSpecializationFailure::Effects(cause) => cause.failure_fact(),
                _ => unreachable!(),
            };
            assert_eq!(error.failure_fact(), expected);
            assert!(error.source().is_some());
        }
    }
}

impl FailureProjection for crate::ComparisonPrepareError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Kernel(source) => source.failure_fact(),
            Self::Unsupported => FailureFact::new(
                FailureClass::Unsupported,
                novarocks_function_contract::kernel_failure::UNSUPPORTED_COMPARISON,
            ),
            Self::TypeMismatch => fact(FailureClass::Invariant, 603),
        }
    }
}

impl FailureProjection for crate::ArithmeticPrepareError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Kernel(source) => source.failure_fact(),
            Self::Unsupported => FailureFact::new(
                FailureClass::Unsupported,
                novarocks_function_contract::kernel_failure::UNSUPPORTED_ARITHMETIC,
            ),
            Self::TypeMismatch => fact(FailureClass::Invariant, 605),
        }
    }
}

impl FailureProjection for crate::CastPrepareError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Kernel(source) => source.failure_fact(),
            Self::Unsupported => FailureFact::new(
                FailureClass::Unsupported,
                novarocks_function_contract::kernel_failure::UNSUPPORTED_CAST,
            ),
            Self::TypeMismatch => fact(FailureClass::Invariant, 607),
        }
    }
}

#[cfg(test)]
mod comparison_failure_tests {
    use super::*;
    use crate::{ComparisonPrepareError, PreparedComparisonRecipe};
    use arrow_schema::{DataType, Field};
    use novarocks_type_contract::{
        ComparisonOperator, CompileControlError, CompilePhase, FunctionValueType,
        PureCompileControl,
    };
    use std::{error::Error, sync::Arc};

    struct CompileControl(Option<CompileControlError>);
    impl PureCompileControl for CompileControl {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            self.0.clone().map_or(Ok(()), Err)
        }
    }

    fn value(data_type: DataType) -> FunctionValueType {
        FunctionValueType::from_legacy_declared_projection(data_type, true)
    }

    #[test]
    fn actual_comparison_recipe_authors_capability_and_mismatch_as_distinct_facts() {
        let nested = value(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))));
        let int32 = value(DataType::Int32);
        let int64 = value(DataType::Int64);
        let control = CompileControl(None);
        let supported =
            PreparedComparisonRecipe::try_new(ComparisonOperator::Eq, &int32, &int32, &control)
                .unwrap();
        assert_eq!(supported.left_type(), &int32);
        assert_eq!(supported.right_type(), &int32);
        for (left, right, expected, class, reason) in [
            (
                &nested,
                &nested,
                ComparisonPrepareError::Unsupported,
                FailureClass::Unsupported,
                602,
            ),
            (
                &int32,
                &int64,
                ComparisonPrepareError::TypeMismatch,
                FailureClass::Invariant,
                603,
            ),
        ] {
            let error =
                PreparedComparisonRecipe::try_new(ComparisonOperator::Eq, left, right, &control)
                    .unwrap_err();
            assert_eq!(error, expected);
            assert_eq!(error.failure_fact(), fact(class, reason));
            assert_eq!(error.failure_fact().reason().domain(), domain::KERNEL.get());
            assert!(error.source().is_none());
            let source = novarocks_failure_contract::SharedFailureCause::new(error.clone());
            assert_eq!(source.failure_fact(), error.failure_fact());
            assert_eq!(source.detail(), error.to_string());
            assert_eq!(
                source.downcast_ref::<ComparisonPrepareError>(),
                Some(&error)
            );
        }
    }

    #[test]
    fn comparison_control_and_kernel_keep_their_original_authors() {
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        let int32 = value(DataType::Int32);
        let source =
            novarocks_failure_contract::SharedFailureCause::new(LayoutResourceError::SourceModel);
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::Source(source),
        ] {
            let error = PreparedComparisonRecipe::try_new(
                ComparisonOperator::Eq,
                &int32,
                &int32,
                &CompileControl(Some(control.clone())),
            )
            .unwrap_err();
            assert_eq!(error, ComparisonPrepareError::Control(control.clone()));
            assert_eq!(error.failure_fact(), control.failure_fact());
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&control)
            );
        }
        let kernel = KernelFailure::from_domain(LayoutResourceError::SourceModel);
        let error = ComparisonPrepareError::Kernel(kernel.clone());
        assert_eq!(error.failure_fact(), kernel.failure_fact());
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            error.source().unwrap().downcast_ref::<KernelFailure>(),
            Some(&kernel)
        );
    }
}

#[cfg(test)]
mod primitive_failure_tests {
    use super::*;
    use crate::{ArithmeticPrepareError as A, CastPrepareError as C, ComparisonPrepareError as B};

    #[test]
    fn primitive_capabilities_are_unsupported_with_the_original_owner_reasons() {
        for (actual, code) in [
            (B::Unsupported.failure_fact(), 602),
            (A::Unsupported.failure_fact(), 604),
            (C::Unsupported.failure_fact(), 606),
        ] {
            assert_eq!(actual, fact(FailureClass::Unsupported, code));
        }
        for (actual, code) in [
            (B::TypeMismatch.failure_fact(), 603),
            (A::TypeMismatch.failure_fact(), 605),
            (C::TypeMismatch.failure_fact(), 607),
        ] {
            assert_eq!(actual, fact(FailureClass::Invariant, code));
        }
    }

    #[test]
    fn primitive_wrappers_delegate_full_kernel_and_caller_control_facts() {
        let control = novarocks_type_contract::CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
        );
        for error in [
            KernelFailure::Capacity(crate::KernelCapacityRefusal::new(
                FailureReason::new(domain::MEMORY, NonZeroU16::new(8).unwrap()),
                16,
                8,
                16,
            )),
            KernelFailure::from(novarocks_type_contract::ValueTypeError::TooDeep),
            KernelFailure::CompileControl(control.clone()),
            KernelFailure::Cancelled,
            KernelFailure::ResourceExhausted,
            KernelFailure::InvalidProgram(crate::KernelDiagnostic::new("unsupported")),
            KernelFailure::Operational(crate::KernelDiagnostic::new("unsupported")),
        ] {
            let original = error.failure_fact();
            assert_eq!(B::Kernel(error.clone()).failure_fact(), original);
            assert_eq!(A::Kernel(error.clone()).failure_fact(), original);
            assert_eq!(C::Kernel(error).failure_fact(), original);
        }
        assert_eq!(
            B::Control(control.clone()).failure_fact(),
            control.failure_fact()
        );
        assert_eq!(
            A::Control(control.clone()).failure_fact(),
            control.failure_fact()
        );
        assert_eq!(
            C::Control(control.clone()).failure_fact(),
            control.failure_fact()
        );
    }
}
