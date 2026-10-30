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

//! Compiler-owned facts; diagnostic phase labels never choose a category.
use crate::{FragmentCompileError, FragmentCreationError, ProviderPreparationError};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};
use std::{error::Error, num::NonZeroU16};

#[repr(u16)]
enum Reason {
    Fragment = 2,
    Expression = 12,
    Channels = 20,
    Repeat = 21,
    Unpivot = 22,
}
fn fact(class: FailureClass, reason: Reason) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(
            domain::LOCAL_COMPILER,
            NonZeroU16::new(reason as u16).unwrap(),
        ),
    )
}
impl FailureProjection for FragmentCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::ArrayProjection(source) => source.failure_fact(),
            Self::ProjectMetadataRequest(source) => source.failure_fact(),
            Self::ProjectMetadataHost { error } | Self::Owner { error, .. } => error.failure_fact(),
            Self::Control(source) => source.failure_fact(),
            Self::Unsupported { .. } => FailureFact::new(
                FailureClass::Unsupported,
                novarocks_failure_contract::local_compiler_reason::UNSUPPORTED_FRAGMENT,
            ),
            Self::Invalid(_) => fact(FailureClass::Invariant, Reason::Fragment),
        }
    }
}
impl<H: FailureProjection> FailureProjection for FragmentCreationError<H> {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Creator(source) => source.failure_fact(),
            Self::Resource(source) => source.failure_fact(),
            Self::Compile(source) => source.failure_fact(),
        }
    }
}
impl<E: Error + FailureProjection + 'static> FailureProjection for ProviderPreparationError<E> {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Read { error, .. } | Self::Write { error, .. } => error.failure_fact(),
        }
    }
}
impl FailureProjection for crate::expressions::ExpressionLoweringError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Construction(source) => source.failure_fact(),
            Self::BTree(source) => source.failure_fact(),
            Self::Publication(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Reference(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Type(source) => source.failure_fact(),
            Self::Expressions(source) => source.failure_fact(),
            Self::Specialization(source) => source.failure_fact(),
            Self::Effects(source) => source.failure_fact(),
            Self::Arithmetic(source) => source.failure_fact(),
            Self::Cast(source) => source.failure_fact(),
            Self::Comparison(source) => source.failure_fact(),
            Self::UnsupportedExpression(_) => FailureFact::new(
                FailureClass::Unsupported,
                novarocks_failure_contract::local_compiler_reason::UNSUPPORTED_EXPRESSION,
            ),
            Self::UnsupportedCall(_) => FailureFact::new(
                FailureClass::Unsupported,
                novarocks_failure_contract::local_compiler_reason::UNSUPPORTED_CALL,
            ),
            Self::Invalid(_) => fact(FailureClass::Invariant, Reason::Expression),
        }
    }
}
impl FailureProjection for crate::channels::ChannelLoweringError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::ValueType(source) => source.failure_fact(),
            Self::Repeat(source) => source.failure_fact(),
            Self::Unpivot(source) => source.failure_fact(),
            Self::Join(source) => source.failure_fact(),
            Self::Invalid(_) => fact(FailureClass::Invariant, Reason::Channels),
        }
    }
}
impl FailureProjection for crate::repeat::RepeatLoweringError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::ValueType(source) => source.failure_fact(),
            Self::Layout(source) => source.failure_fact(),
            Self::Invalid(_) => fact(FailureClass::Invariant, Reason::Repeat),
        }
    }
}
impl FailureProjection for crate::unpivot::UnpivotLoweringError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::ValueType(source) => source.failure_fact(),
            Self::Layout(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Invalid(_) => fact(FailureClass::Invariant, Reason::Unpivot),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::SharedFailureCause;
    use novarocks_type_contract::{
        CompileControlError, owned_resources::layout::LayoutResourceError,
    };
    #[test]
    fn explicit_capabilities_keep_their_compiler_reasons_through_private_ownership() {
        use crate::expressions::ExpressionLoweringError;
        use novarocks_physical_plan::{ExprId, NodeId, PhysicalCallSite};
        let errors = [
            (
                FragmentCompileError::Unsupported {
                    node: Some(NodeId::new(7)),
                    feature: "FULL OUTER hash join",
                },
                1,
            ),
            (
                FragmentCompileError::from(ExpressionLoweringError::UnsupportedExpression(
                    ExprId::new(7),
                )),
                10,
            ),
            (
                FragmentCompileError::from(ExpressionLoweringError::UnsupportedCall(
                    PhysicalCallSite::Table {
                        node: NodeId::new(7),
                    },
                )),
                11,
            ),
        ];
        for (error, reason) in errors {
            let fact = error.failure_fact();
            assert_eq!(fact.class(), FailureClass::Unsupported);
            assert_eq!(fact.reason().domain(), domain::LOCAL_COMPILER.get());
            assert_eq!(fact.reason().code(), reason);
            if reason != 1 {
                assert!(
                    error
                        .source()
                        .unwrap()
                        .source()
                        .unwrap()
                        .is::<ExpressionLoweringError>()
                );
            }
        }
        let invalid = FragmentCompileError::Invalid("unsupported diagnostic");
        assert_eq!(invalid.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(invalid.failure_fact().reason().code(), 2);
    }

    #[test]
    fn existential_owner_keeps_original_geometry_and_does_not_classify_phase_text() {
        let source = LayoutResourceError::SourceModel;
        let error = FragmentCompileError::Owner {
            phase: "resource exhausted",
            error: SharedFailureCause::new(source),
        };
        assert_eq!(error.failure_fact(), source.failure_fact());
        assert!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<LayoutResourceError>()
        );
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
    #[test]
    fn creator_resource_and_control_keep_their_original_domain_facts() {
        let creator: FragmentCreationError<LayoutResourceError> =
            FragmentCreationError::Creator(LayoutResourceError::Arithmetic);
        let resource: FragmentCreationError<LayoutResourceError> =
            FragmentCreationError::Resource(LayoutResourceError::SourceModel);
        let stopped: FragmentCreationError<LayoutResourceError> =
            FragmentCreationError::Control(CompileControlError::Cancelled);
        assert_eq!(
            creator.failure_fact(),
            LayoutResourceError::Arithmetic.failure_fact()
        );
        assert_eq!(
            resource.failure_fact(),
            LayoutResourceError::SourceModel.failure_fact()
        );
        assert!(resource.source().unwrap().is::<LayoutResourceError>());
        assert_eq!(
            stopped.failure_fact(),
            CompileControlError::Cancelled.failure_fact()
        );
    }
}

#[cfg(test)]
mod constant_publication_source_tests {
    use super::*;
    use novarocks_functions::ConstantError;
    use std::{convert::Infallible, error::Error};
    #[test]
    fn values_parameter_and_constant_refusals_keep_the_actual_author_source() {
        let parameters = novarocks_type_contract::validate_arrow_carrier_parameters_observed::<
            ConstantError,
        >(&arrow_schema::DataType::FixedSizeBinary(-1), || Ok(()))
        .unwrap_err();
        for original in [
            parameters,
            ConstantError::Limit("original admitted constant extent"),
        ] {
            let expected = original.failure_fact();
            let compiled = FragmentCompileError::from(original);
            assert_eq!(compiled.failure_fact(), expected);
            let original = compiled
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<ConstantError>()
                .expect("actual constant author");
            assert_eq!(original.failure_fact(), expected);
        }
    }
    #[test]
    fn compiler_publication_carries_the_real_program_reserve_error() {
        let source = Vec::<u64>::new().try_reserve_exact(usize::MAX).unwrap_err();
        let source =
            novarocks_local_program::ProgramCreationFailure::<Infallible, Infallible>::Capacity(
                source,
            );
        let expected = source.failure_fact();
        let novarocks_local_program::ProgramCreationFailure::Capacity(source) = source else {
            unreachable!()
        };
        let compiled = crate::creation::program_publication_failure(source);
        assert_eq!(compiled.failure_fact(), expected);
        let program = compiled.source().unwrap().source().unwrap().downcast_ref::<novarocks_local_program::ProgramCreationFailure<Infallible, Infallible>>().expect("actual program publication owner");
        assert!(
            program
                .source()
                .unwrap()
                .is::<std::collections::TryReserveError>()
        );
    }
}
