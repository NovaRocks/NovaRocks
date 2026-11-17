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

//! Physical-program authors own their contract failures. Receivers delegate
//! these facts and retain this error, rather than inspecting its children.
use crate::*;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};
use std::num::NonZeroU16;

// Stable domain-owned reasons. Rust rejects accidental duplicate IDs; no
// source-token guard or cross-domain taxonomy is needed.
#[derive(Clone, Copy)]
#[repr(u16)]
enum PhysicalReason {
    IdentityZeroPlanVersion = 1,
    IdentityEmptyStableIdentity = 2,
    IdentityStableIdentityTooLong = 3,
    PredicateSourceTooDeep = 50,
    PredicateSourceInvalidFragment = 51,
    PredicateSourceInvalidSite = 52,
    PredicateSourceNotSearchPredicate = 53,
    PredicateSourceInvalidUse = 54,
    PredicateSourceInvalidScope = 55,
    PredicateSourceWrongDemand = 56,
    PredicateSourceNotBoolean = 57,
    PredicateSourceNotPositiveConjunction = 58,
    PredicateSourceWrongControl = 59,
    PredicateSourceWrongArguments = 60,
    PredicateSourceInvalidArgument = 61,
    PruningStructureTooLarge = 70,
    PruningStructureEmptySources = 71,
    PruningStructureInvalidFragment = 72,
    PruningStructureInvalidScan = 73,
    PruningStructureInvalidOccurrence = 74,
    PruningStructureWrongResponsibility = 75,
    PruningStructureWrongContext = 76,
    PruningStructureWrongSnapshot = 77,
    PruningStructureInvalidPath = 78,
    PruningStructureSharedLocalProducer = 79,
    PruningStructureDuplicateSource = 80,
    PruningStructureInvalidColumn = 81,
    PruningStructureInvalidTransport = 82,
    PruningStructureMissingDomainColumn = 83,
    FrozenPruningResourceSource = 90,
    FrozenPruningWrongFragment = 91,
    FrozenPruningDuplicateTarget = 92,
    FrozenPruningTooLarge = 93,
    FrozenPruningEmptySources = 94,
    FrozenCallResourceSource = 100,
    FrozenCallTooManyItems = 101,
    FrozenCallWrongFragment = 102,
    FrozenCallDuplicateSite = 103,
    FrozenCallInvalidSite = 104,
    FrozenCallMissingSite = 105,
    FrozenCallWrongContext = 106,
    FrozenCallSharedUse = 107,
    FrozenCallWrongProofScope = 109,
    FrozenCallWrongControl = 110,
    FrozenCallReplicaEquivalence = 111,
    ConstantReferenceUnusedPools = 123,
    ConstantReferenceInvalidConsumer = 124,
    CallRequestWrongFragment = 130,
    CallRequestDuplicateDefinition = 131,
    CallRequestMissingDefinition = 132,
    CallRequestExtraDefinition = 133,
    CallRequestInvalidArgumentCount = 134,
    CallRequestArgumentTypeMismatch = 135,
    FragmentPackageResourceSource = 140,
    FragmentPackageUnusedParameters = 141,
    ValidationEmpty = 3200,
    ValidationStructural = 3201,
    ValidationCapability = 3202,
    ValidationStructuralCapability = 3203,
    ValidationResource = 3204,
    ValidationStructuralResource = 3205,
    ValidationCapabilityResource = 3206,
    ValidationStructuralCapabilityResource = 3207,
}

fn fact(class: FailureClass, reason: PhysicalReason) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(
            domain::PHYSICAL_PLAN,
            NonZeroU16::new(reason as u16).unwrap(),
        ),
    )
}

impl FailureProjection for IdentityError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::ZeroPlanVersion => fact(
                FailureClass::Invariant,
                PhysicalReason::IdentityZeroPlanVersion,
            ),
            Self::EmptyStableIdentity { .. } => fact(
                FailureClass::Invariant,
                PhysicalReason::IdentityEmptyStableIdentity,
            ),
            Self::StableIdentityTooLong { .. } => fact(
                FailureClass::Resource,
                PhysicalReason::IdentityStableIdentityTooLong,
            ),
        }
    }
}

impl FailureProjection for PredicateSourceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::TooDeep => fact(
                FailureClass::Resource,
                PhysicalReason::PredicateSourceTooDeep,
            ),
            Self::InvalidFragment => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceInvalidFragment,
            ),
            Self::InvalidSite => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceInvalidSite,
            ),
            Self::NotSearchPredicate => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceNotSearchPredicate,
            ),
            Self::InvalidUse => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceInvalidUse,
            ),
            Self::InvalidScope => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceInvalidScope,
            ),
            Self::WrongDemand => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceWrongDemand,
            ),
            Self::NotBoolean => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceNotBoolean,
            ),
            Self::NotPositiveConjunction => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceNotPositiveConjunction,
            ),
            Self::WrongControl => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceWrongControl,
            ),
            Self::WrongArguments => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceWrongArguments,
            ),
            Self::InvalidArgument => fact(
                FailureClass::Invariant,
                PhysicalReason::PredicateSourceInvalidArgument,
            ),
        }
    }
}

impl FailureProjection for PruningStructureError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Source(source) => source.failure_fact(),
            Self::TooLarge => fact(
                FailureClass::Resource,
                PhysicalReason::PruningStructureTooLarge,
            ),
            Self::EmptySources => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureEmptySources,
            ),
            Self::InvalidFragment => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureInvalidFragment,
            ),
            Self::InvalidScan => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureInvalidScan,
            ),
            Self::InvalidOccurrence => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureInvalidOccurrence,
            ),
            Self::WrongResponsibility => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureWrongResponsibility,
            ),
            Self::WrongContext => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureWrongContext,
            ),
            Self::WrongSnapshot => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureWrongSnapshot,
            ),
            Self::InvalidPath => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureInvalidPath,
            ),
            Self::SharedLocalProducer => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureSharedLocalProducer,
            ),
            Self::DuplicateSource => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureDuplicateSource,
            ),
            Self::InvalidColumn => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureInvalidColumn,
            ),
            Self::InvalidTransport => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureInvalidTransport,
            ),
            Self::MissingDomainColumn => fact(
                FailureClass::Invariant,
                PhysicalReason::PruningStructureMissingDomainColumn,
            ),
        }
    }
}

impl FailureProjection for FrozenPruningError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Structure(source) => source.failure_fact(),
            Self::ResourceSource(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenPruningResourceSource,
            ),
            Self::WrongFragment => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenPruningWrongFragment,
            ),
            Self::DuplicateTarget => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenPruningDuplicateTarget,
            ),
            Self::TooLarge => fact(
                FailureClass::Resource,
                PhysicalReason::FrozenPruningTooLarge,
            ),
            Self::EmptySources => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenPruningEmptySources,
            ),
        }
    }
}

impl FailureProjection for FrozenCallError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::MissingLegacyMetadata(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
            Self::InvalidEffects(source) => source.failure_fact(),
            Self::ResourceSource(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallResourceSource,
            ),
            Self::TooManyItems => fact(
                FailureClass::Resource,
                PhysicalReason::FrozenCallTooManyItems,
            ),
            Self::WrongFragment => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallWrongFragment,
            ),
            Self::DuplicateSite => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallDuplicateSite,
            ),
            Self::InvalidSite => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallInvalidSite,
            ),
            Self::MissingSite(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallMissingSite,
            ),
            Self::WrongContext => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallWrongContext,
            ),
            Self::DuplicateIdentity => {
                fact(FailureClass::Invariant, PhysicalReason::FrozenCallSharedUse)
            }
            Self::WrongProofScope => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallWrongProofScope,
            ),
            Self::WrongControl => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallWrongControl,
            ),
            Self::ReplicaEquivalence(_) | Self::ReplicaExpression { .. } => fact(
                FailureClass::Invariant,
                PhysicalReason::FrozenCallReplicaEquivalence,
            ),
        }
    }
}

impl FailureProjection for ConstantReferenceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Table(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Structure(source) => source.failure_fact(),
            Self::UnusedPools => fact(
                FailureClass::Invariant,
                PhysicalReason::ConstantReferenceUnusedPools,
            ),
            Self::InvalidConsumer(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::ConstantReferenceInvalidConsumer,
            ),
        }
    }
}

impl FailureProjection for CallRequestError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Binding(source) => source.failure_fact(),
            Self::Calls(source) => source.failure_fact(),
            Self::Structure(source) => source.failure_fact(),
            Self::ConstructionResource(source) => source.failure_fact(),
            Self::ConstructionTree(source) => source.failure_fact(),
            Self::WrongFragment => fact(
                FailureClass::Invariant,
                PhysicalReason::CallRequestWrongFragment,
            ),
            Self::DuplicateDefinition(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::CallRequestDuplicateDefinition,
            ),
            Self::MissingDefinition(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::CallRequestMissingDefinition,
            ),
            Self::ExtraDefinition => fact(
                FailureClass::Invariant,
                PhysicalReason::CallRequestExtraDefinition,
            ),
            Self::InvalidArgumentCount(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::CallRequestInvalidArgumentCount,
            ),
            Self::ArgumentTypeMismatch(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::CallRequestArgumentTypeMismatch,
            ),
        }
    }
}

impl FailureProjection for FragmentStructureError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Structure(source) => source.failure_fact(),
        }
    }
}

impl FailureProjection for FragmentPackageError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Requests(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
            Self::Constant(source) => source.failure_fact(),
            Self::Structure(source) => source.failure_fact(),
            Self::Calls(source) => source.failure_fact(),
            Self::Pruning(source) => source.failure_fact(),
            Self::Parameter(source) => source.failure_fact(),
            Self::ResourceSource(_) => fact(
                FailureClass::Invariant,
                PhysicalReason::FragmentPackageResourceSource,
            ),
            Self::UnusedParameters => fact(
                FailureClass::Invariant,
                PhysicalReason::FragmentPackageUnusedParameters,
            ),
        }
    }
}

impl FailureProjection for ValidationErrors {
    fn failure_fact(&self) -> FailureFact {
        // Preserve the complete typed category set in this owner's stable
        // reason range, rather than choosing by text or discarding a second
        // admission constraint. A structural defect cannot be fixed by either
        // another target or a raised limit. This does not authorize recovery.
        let categories = u16::from(self.has(ValidationErrorCategory::StructuralInvariant))
            | (u16::from(self.has(ValidationErrorCategory::UnsupportedCapability)) << 1)
            | (u16::from(self.has(ValidationErrorCategory::ResourceLimit)) << 2);
        let class = if categories & 1 != 0 || categories == 0 {
            FailureClass::Invariant
        } else if categories & 4 != 0 {
            FailureClass::Resource
        } else {
            FailureClass::Dependency
        };
        let reason = match categories {
            0 => PhysicalReason::ValidationEmpty,
            1 => PhysicalReason::ValidationStructural,
            2 => PhysicalReason::ValidationCapability,
            3 => PhysicalReason::ValidationStructuralCapability,
            4 => PhysicalReason::ValidationResource,
            5 => PhysicalReason::ValidationStructuralResource,
            6 => PhysicalReason::ValidationCapabilityResource,
            7 => PhysicalReason::ValidationStructuralCapabilityResource,
            _ => unreachable!("the validation category set has exactly three bits"),
        };
        fact(class, reason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::FailureEvidence;
    use novarocks_type_contract::{
        CompileControlError, SemanticParameterError, owned_resources::btree::BTreeResourceError,
    };
    use std::error::Error;

    #[test]
    fn clipping_diagnostics_neither_invents_a_cause_nor_loses_observed_categories() {
        use crate::validation::{MAX_VALIDATION_ERRORS, ValidationContext};
        let mut collector = ValidationContext::new();
        for _ in 0..MAX_VALIDATION_ERRORS * 4 {
            collector.push(ValidationError::resource_limit(
                "wide",
                "actual declared bound",
            ));
        }
        let errors = ValidationErrors::from_collector(collector);
        assert_eq!(errors.errors().len(), MAX_VALIDATION_ERRORS);
        assert!(errors.diagnostics_truncated());
        assert!(errors.has(ValidationErrorCategory::ResourceLimit));
        assert!(!errors.has(ValidationErrorCategory::StructuralInvariant));
        assert!(!errors.is_producer_defect());
        assert_eq!(errors.failure_fact().class(), FailureClass::Resource);
        assert!(
            errors
                .errors()
                .iter()
                .all(|e| e.category() == ValidationErrorCategory::ResourceLimit)
        );

        let mut collector = ValidationContext::new();
        for _ in 0..MAX_VALIDATION_ERRORS {
            collector.push(ValidationError::resource_limit("wide", "bound"));
        }
        collector.push(ValidationError::new(
            "late",
            "actual invariant after the payload cap",
        ));
        let mixed = ValidationErrors::from_collector(collector);
        assert!(
            mixed
                .errors()
                .iter()
                .all(|e| e.category() == ValidationErrorCategory::ResourceLimit)
        );
        assert!(mixed.has(ValidationErrorCategory::StructuralInvariant));
        assert!(mixed.has(ValidationErrorCategory::ResourceLimit));
        assert_eq!(mixed.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(mixed.failure_fact().reason().code(), 3205);
    }

    #[test]
    fn physical_projection_delegates_original_tree_resource_and_control_sources() {
        let error = FragmentPackageError::Requests(CallRequestError::ConstructionTree(
            BTreeResourceError::Arithmetic("cancelled numeric overflow"),
        ));
        assert_eq!(
            error.failure_fact(),
            BTreeResourceError::Arithmetic("a different spelling").failure_fact()
        );
        assert!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<BTreeResourceError>()
        );
        let stopped = FragmentPackageError::Pruning(FrozenPruningError::Structure(
            PruningStructureError::Control(CompileControlError::Cancelled),
        ));
        assert_eq!(
            stopped.failure_fact().evidence(),
            FailureEvidence::StopObservation
        );
        assert!(
            stopped
                .source()
                .unwrap()
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<CompileControlError>()
        );
        let params = FragmentPackageError::Parameter(SemanticParameterError::TooManyParameters);
        assert_eq!(
            params.failure_fact(),
            SemanticParameterError::TooManyParameters.failure_fact()
        );
        assert!(params.failure_fact().resource().unwrap().limit.unwrap() > 0);
    }
    #[test]
    fn aggregate_projection_preserves_every_typed_category_without_retry_authority() {
        use crate::validation::ValidationContext;
        for mask in 1u16..8 {
            let mut collector = ValidationContext::new();
            if mask & 1 != 0 {
                collector.push(ValidationError::new("root", "resource exhausted"));
            }
            if mask & 2 != 0 {
                collector.push(ValidationError::unsupported_capability(
                    "root",
                    "internal invariant",
                ));
            }
            if mask & 4 != 0 {
                collector.push(ValidationError::resource_limit(
                    "root",
                    "unsupported capability",
                ));
            }
            let error = ValidationErrors::from_collector(collector);
            let projected = error.failure_fact();
            assert_eq!(projected.reason().domain(), domain::PHYSICAL_PLAN.get());
            assert_eq!(projected.reason().code(), 3200 + mask);
            assert_eq!(
                projected.class(),
                if mask & 1 != 0 {
                    FailureClass::Invariant
                } else if mask & 4 != 0 {
                    FailureClass::Resource
                } else {
                    FailureClass::Dependency
                }
            );
        }
    }
}
