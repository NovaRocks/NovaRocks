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

//! Type/construction facts are classified by their defining domain.

use crate::owned_resources::{
    layout::LayoutResourceError, metadata_materialization::MetadataMaterializationError,
};
use crate::{CompileControlError, MetadataRequestError, ValueTypeError};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};
use std::num::NonZeroU16;
fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(
            domain::TYPE_CONTRACT,
            NonZeroU16::new(code).expect("nonzero reason"),
        ),
    )
}
impl FailureProjection for CompileControlError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Cancelled => {
                FailureFact::stop_observation(fact(FailureClass::Control, 1).reason())
            }
            Self::DeadlineExceeded => {
                FailureFact::stop_observation(fact(FailureClass::Control, 2).reason())
            }
            Self::ResourceExhausted => fact(FailureClass::Resource, 3),
            Self::Source(source) => source.failure_fact(),
        }
    }
}
impl FailureProjection for LayoutResourceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::SourceModel => fact(FailureClass::Invariant, 10),
            Self::ArcHeader => fact(FailureClass::Resource, 11),
            Self::ArcBacking => fact(FailureClass::Resource, 12),
            Self::Arithmetic => fact(FailureClass::Resource, 13),
            Self::BytesShared => fact(FailureClass::Resource, 14),
        }
    }
}
impl FailureProjection for ValueTypeError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::InvalidType(_) => fact(FailureClass::Invariant, 20),
            Self::UnknownLogicalMetadata => fact(FailureClass::Invariant, 21),
            Self::InvalidLogicalCarrier(_) => fact(FailureClass::Invariant, 22),
            Self::TooDeep => fact(FailureClass::Resource, 23),
            Self::TooManyNodes => fact(FailureClass::Resource, 24),
        }
    }
}
impl FailureProjection for crate::owned_resources::hashmap::HashMapResourceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::SourceModel(_) => fact(FailureClass::Invariant, 32),
            Self::Arithmetic(_) => fact(FailureClass::Resource, 33),
        }
    }
}
impl FailureProjection for MetadataMaterializationError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Model(cause) => cause.failure_fact(),
            Self::BTree(cause) => cause.failure_fact(),
            Self::ValueType(cause) => cause.failure_fact(),
            Self::Arithmetic => fact(FailureClass::Resource, 34),
            Self::InsertCountExceededOriginalReservation => fact(FailureClass::Invariant, 30),
            Self::MissingOriginalFieldOrigin => fact(FailureClass::Invariant, 31),
        }
    }
}
impl FailureProjection for MetadataRequestError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(cause) => cause.failure_fact(),
            Self::Arithmetic => fact(FailureClass::Resource, 40),
            Self::SourceModel(_) => fact(FailureClass::Invariant, 41),
            Self::Metadata(cause) => cause.failure_fact(),
            Self::ValueType(cause) => cause.failure_fact(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metadata_btree_model_keeps_the_original_reason_and_discoverable_source() {
        use crate::owned_resources::{
            arrow_metadata::hashmap_conversion_requests, btree::BTreeResourceError,
        };
        use std::error::Error;
        for source in [
            hashmap_conversion_requests(usize::MAX).unwrap_err(),
            BTreeResourceError::SourceModel("original metadata model"),
        ] {
            let wrapped = MetadataMaterializationError::BTree(source);
            assert_eq!(wrapped.failure_fact(), source.failure_fact());
            assert_eq!(
                wrapped
                    .source()
                    .unwrap()
                    .downcast_ref::<BTreeResourceError>(),
                Some(&source)
            );
        }
        let mut sum = crate::owned_resources::metadata_request::MetadataRequestSum::default();
        let original = sum.facts();
        let error = sum.metadata_conversion(usize::MAX).unwrap_err();
        let cause = error
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<BTreeResourceError>()
            .expect("the original metadata numeric cause");
        assert_eq!(error.failure_fact(), cause.failure_fact());
        assert_eq!(sum.facts(), original);
    }
    #[test]
    fn layout_request_arithmetic_does_not_invent_value_representation_facts() {
        let arithmetic = LayoutResourceError::Arithmetic.failure_fact();
        assert_eq!(arithmetic.class(), FailureClass::Resource);
        assert_eq!(arithmetic.representation(), None);
        assert_eq!(
            LayoutResourceError::SourceModel.failure_fact().class(),
            FailureClass::Invariant
        );
    }
}

impl FailureProjection for crate::CarrierParameterError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Invalid(_) => fact(FailureClass::Invariant, 90),
            Self::Decimal(_) => fact(FailureClass::Invariant, 91),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for crate::ValueTypeError {}
impl novarocks_failure_contract::OuterFailureProjection for crate::CarrierParameterError {}

impl FailureProjection for crate::ExecutionValueTypeError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Logical(source) => source.failure_fact(),
            Self::Invalid(detail) => ValueTypeError::InvalidType(detail).failure_fact(),
            Self::Depth => ValueTypeError::TooDeep.failure_fact(),
            Self::Nodes => ValueTypeError::TooManyNodes.failure_fact(),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for crate::ExecutionValueTypeError {}
#[cfg(test)]
mod execution_value_tests {
    use super::*;
    #[test]
    fn actual_layout_profile_refusal_keeps_its_original_resource_reason() {
        use arrow_schema::{DataType, Field};
        let mut ty = DataType::Int32;
        for _ in 0..=crate::MAX_VALUE_TYPE_DEPTH {
            ty = DataType::List(std::sync::Arc::new(Field::new("item", ty, true)));
        }
        let error =
            crate::ValueLayout::try_new(&ty, crate::ValueTypeStorageLifetime::pure()).unwrap_err();
        assert!(matches!(error, crate::ExecutionValueTypeError::Depth));
        assert_eq!(error.failure_fact(), ValueTypeError::TooDeep.failure_fact());
    }
}

impl FailureProjection for crate::FunctionIdentityError {
    fn failure_fact(&self) -> FailureFact {
        fact(FailureClass::Invariant, 92)
    }
}

// Layout refusals are outer operation failures, never row-local data errors.
impl novarocks_failure_contract::OuterFailureProjection for LayoutResourceError {}

impl FailureProjection for crate::EffectContractError {
    fn failure_fact(&self) -> FailureFact {
        let code = match self {
            Self::KindMismatch => 110,
            Self::ControlMismatch => 111,
            Self::DuplicateEnvironmentKey => 112,
            Self::ProofScopeMismatch => 113,
            Self::CallIdentityMismatch => 114,
            Self::InvalidRefinement => 115,
            Self::InvalidEnvironmentReference => 116,
        };
        fact(FailureClass::Invariant, code)
    }
}

impl FailureProjection for crate::ExpressionControlFlowError {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Control(source) => return source.failure_fact(),
            Self::TooManyItems => (FailureClass::Resource, 120),
            Self::DuplicateIdentity => (FailureClass::Invariant, 121),
            Self::InvalidReference => (FailureClass::Invariant, 122),
            Self::InvalidGuard => (FailureClass::Invariant, 123),
            Self::InvalidControlShape => (FailureClass::Invariant, 124),
            Self::InvalidDemand => (FailureClass::Invariant, 125),
            Self::SharedUse => (FailureClass::Invariant, 126),
            Self::Cycle => (FailureClass::Invariant, 127),
            Self::TooDeep => (FailureClass::Resource, 128),
            Self::SourceModel(_) => (FailureClass::Invariant, 129),
        };
        let projected = fact(class, code);
        if matches!(self, Self::TooDeep) {
            projected.with_resource(novarocks_failure_contract::ResourceFacts {
                request_scope: Some(novarocks_failure_contract::ResourceScope::Request),
                constraint_scope: Some(novarocks_failure_contract::ResourceScope::Request),
                dimension: novarocks_failure_contract::ResourceDimension::Positions,
                requested: None,
                required_growth: None,
                limit: Some(crate::control_flow::MAX_CONTROL_DEPTH as u64),
                live: None,
                committed: None,
            })
        } else {
            projected
        }
    }
}
impl FailureProjection for crate::SemanticParameterError {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::DuplicateId(_) => (FailureClass::Invariant, 130),
            Self::DuplicateStatementStart => (FailureClass::Invariant, 131),
            Self::MissingId(_) => (FailureClass::Invariant, 132),
            Self::KeyMismatch(_) => (FailureClass::Invariant, 133),
            Self::TooManyParameters => (FailureClass::Resource, 134),
            Self::InvalidTimeZone => (FailureClass::Data, 135),
        };
        let projected = fact(class, code);
        if matches!(self, Self::TooManyParameters) {
            projected.with_resource(novarocks_failure_contract::ResourceFacts {
                request_scope: Some(novarocks_failure_contract::ResourceScope::Request),
                constraint_scope: Some(novarocks_failure_contract::ResourceScope::Request),
                dimension: novarocks_failure_contract::ResourceDimension::Items,
                requested: None,
                required_growth: None,
                limit: Some(crate::semantics::MAX_SEMANTIC_PARAMETERS as u64),
                live: None,
                committed: None,
            })
        } else {
            projected
        }
    }
}

impl FailureProjection for crate::owned_resources::btree::BTreeResourceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::SourceModel(_) => fact(FailureClass::Invariant, 136),
            Self::Arithmetic(_) => fact(FailureClass::Resource, 137),
        }
    }
}

impl FailureProjection for crate::ControlResourceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::SourceModel(_) => fact(FailureClass::Invariant, 42),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for crate::ControlResourceError {}
