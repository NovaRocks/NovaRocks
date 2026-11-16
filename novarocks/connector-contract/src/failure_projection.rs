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

//! Projection of this crate's frozen Connector contract failures.
use crate::*;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, ResourceDimension, ResourceFacts,
    ResourceScope, domain,
};
use std::num::NonZeroU16;
#[derive(Clone, Copy)]
#[repr(u16)]
enum ContractReason {
    ProviderDuplicateManifest = 140,
    ProviderEmptyManifestEntry = 141,
    ProviderDuplicateDefinition = 142,
    ProviderMissingDefinition = 143,
    ProviderUnexpectedDefinition = 144,
    ProviderCapabilityMismatch = 145,
    ProviderMissingProvider = 146,
    ProviderReadUnavailable = 147,
    ProviderWriteUnavailable = 148,
    CodecRevisionZero = 100,
    CodecProvider = 101,
    CodecCatalog = 102,
    CodecCategory = 103,
    CodecRevision = 104,
    RecipeBinding = 110,
    RecipeFacts = 111,
    RecipeSplits = 112,
    RecipeColumns = 113,
    RecipePayload = 114,
    RecipeBytes = 115,
    ScanEmpty = 120,
    ScanAssignments = 121,
    ScanColumns = 122,
    ScanVariable = 123,
    ScanDuplicateVariable = 124,
    ScanPredicateColumn = 125,
    ScanPredicateType = 126,
    ScanExpression = 127,
    ScanExpressionVariable = 128,
    ScanExpressionType = 129,
    ScanDuplicateFilter = 130,
    ScanFilterVariable = 131,
    ScanBytes = 133,
}
fn fact(reason: ContractReason, class: FailureClass) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::CONNECTOR, NonZeroU16::new(reason as u16).unwrap()),
    )
}
fn bound(reason: ContractReason, dimension: ResourceDimension, limit: usize) -> FailureFact {
    fact(reason, FailureClass::Resource).with_resource(ResourceFacts {
        request_scope: Some(ResourceScope::Request),
        constraint_scope: Some(ResourceScope::Request),
        dimension,
        requested: None,
        required_growth: None,
        limit: u64::try_from(limit).ok(),
        live: None,
        committed: None,
    })
}
impl FailureProjection for ConnectorCodecContractError {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::RevisionMustBeNonZero => ContractReason::CodecRevisionZero,
            Self::ProviderMismatch => ContractReason::CodecProvider,
            Self::CatalogMismatch => ContractReason::CodecCatalog,
            Self::CategoryMismatch => ContractReason::CodecCategory,
            Self::RevisionMismatch => ContractReason::CodecRevision,
        };
        fact(reason, FailureClass::Invariant)
    }
}
impl FailureProjection for ConnectorReadRelationRecipeError {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::Header(source) => return source.failure_fact(),
            Self::BindingMismatch => ContractReason::RecipeBinding,
            Self::PublicFactsMismatch => ContractReason::RecipeFacts,
            Self::InvalidSplitFacts => ContractReason::RecipeSplits,
            Self::TooManyColumns => {
                return bound(
                    ContractReason::RecipeColumns,
                    ResourceDimension::Items,
                    MAX_CONNECTOR_RECIPE_COLUMNS,
                );
            }
            Self::PayloadTooLarge => {
                return bound(
                    ContractReason::RecipePayload,
                    ResourceDimension::Bytes,
                    MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES,
                );
            }
            Self::RecipeTooLarge => {
                return bound(
                    ContractReason::RecipeBytes,
                    ResourceDimension::Bytes,
                    MAX_CONNECTOR_RECIPE_BYTES,
                );
            }
        };
        fact(reason, FailureClass::Invariant)
    }
}
impl FailureProjection for StaticConnectorScanError {
    fn failure_fact(&self) -> FailureFact {
        let reason = match self {
            Self::EmptyAssignments => ContractReason::ScanEmpty,
            Self::TooManyAssignments => {
                return bound(
                    ContractReason::ScanAssignments,
                    ResourceDimension::Items,
                    MAX_CONNECTOR_RECIPE_COLUMNS,
                );
            }
            Self::RecipeColumnMismatch => ContractReason::ScanColumns,
            Self::InvalidVariable => ContractReason::ScanVariable,
            Self::DuplicateVariable => ContractReason::ScanDuplicateVariable,
            Self::InvalidPredicateColumn => ContractReason::ScanPredicateColumn,
            Self::PredicateTypeMismatch => ContractReason::ScanPredicateType,
            Self::InvalidExpression => ContractReason::ScanExpression,
            Self::UnknownExpressionVariable => ContractReason::ScanExpressionVariable,
            Self::ExpressionTypeMismatch => ContractReason::ScanExpressionType,
            Self::DuplicateDynamicFilter => ContractReason::ScanDuplicateFilter,
            Self::UnknownDynamicFilterVariable => ContractReason::ScanFilterVariable,
            Self::TooManyRetainedBytes => {
                return bound(
                    ContractReason::ScanBytes,
                    ResourceDimension::Bytes,
                    MAX_STATIC_SCAN_RETAINED_BYTES,
                );
            }
        };
        fact(reason, FailureClass::Invariant)
    }
}

impl FailureProjection for crate::PureProviderCatalogError {
    fn failure_fact(&self) -> FailureFact {
        use crate::PureProviderCatalogError as E;
        match self {
            E::Control(source) => source.failure_fact(),
            E::DuplicateManifest(_) => fact(
                ContractReason::ProviderDuplicateManifest,
                FailureClass::Invariant,
            ),
            E::EmptyManifestEntry(_) => fact(
                ContractReason::ProviderEmptyManifestEntry,
                FailureClass::Invariant,
            ),
            E::DuplicateDefinition(_) => fact(
                ContractReason::ProviderDuplicateDefinition,
                FailureClass::Invariant,
            ),
            E::MissingDefinition(_) => fact(
                ContractReason::ProviderMissingDefinition,
                FailureClass::Invariant,
            ),
            E::UnexpectedDefinition(_) => fact(
                ContractReason::ProviderUnexpectedDefinition,
                FailureClass::Invariant,
            ),
            E::CapabilityMismatch(_) => fact(
                ContractReason::ProviderCapabilityMismatch,
                FailureClass::Invariant,
            ),
            E::MissingProvider(_) => fact(
                ContractReason::ProviderMissingProvider,
                FailureClass::Dependency,
            ),
            E::ReadUnavailable(_) => fact(
                ContractReason::ProviderReadUnavailable,
                FailureClass::Unsupported,
            ),
            E::WriteUnavailable(_) | E::WriteLayoutUnavailable(_) => fact(
                ContractReason::ProviderWriteUnavailable,
                FailureClass::Unsupported,
            ),
        }
    }
}
impl<E: std::error::Error + FailureProjection + 'static> FailureProjection
    for crate::PureProviderProgramError<E>
{
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Catalog(source) => source.failure_fact(),
            Self::Contract(source) => source.failure_fact(),
            Self::Provider(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
        }
    }
}
impl<E: std::error::Error + FailureProjection> FailureProjection
    for crate::PureProviderCompileError<E>
{
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Provider(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
        }
    }
}

#[cfg(test)]
mod pure_provider_projection_tests {
    use super::*;
    use std::{convert::Infallible, error::Error};
    #[test]
    fn absent_provider_remains_dependency_while_declared_capability_refusals_keep_their_reasons() {
        let provider = crate::ConnectorProviderId::parse("iceberg").unwrap();
        for (source, class, code) in [
            (
                crate::PureProviderCatalogError::MissingProvider(provider.clone()),
                FailureClass::Dependency,
                146,
            ),
            (
                crate::PureProviderCatalogError::ReadUnavailable(provider.clone()),
                FailureClass::Unsupported,
                147,
            ),
            (
                crate::PureProviderCatalogError::WriteUnavailable(provider),
                FailureClass::Unsupported,
                148,
            ),
        ] {
            let fact = source.failure_fact();
            assert_eq!(fact.class(), class);
            assert_eq!(fact.reason().code(), code);
            let wrapper = crate::PureProviderProgramError::<Infallible>::Catalog(source);
            assert_eq!(wrapper.failure_fact(), fact);
            assert!(
                wrapper
                    .source()
                    .unwrap()
                    .is::<crate::PureProviderCatalogError>()
            );
        }
    }

    #[test]
    fn installed_catalog_invariant_and_unavailable_provider_are_distinct_original_facts() {
        let provider = crate::ConnectorProviderId::parse("iceberg").unwrap();
        for source in [
            crate::PureProviderCatalogError::MissingDefinition(provider.clone()),
            crate::PureProviderCatalogError::MissingProvider(provider),
        ] {
            let expected = source.failure_fact();
            let invariant = matches!(
                source,
                crate::PureProviderCatalogError::MissingDefinition(_)
            );
            let error = crate::PureProviderProgramError::<Infallible>::Catalog(source);
            assert_eq!(error.failure_fact(), expected);
            assert_eq!(
                expected.class(),
                if invariant {
                    FailureClass::Invariant
                } else {
                    FailureClass::Dependency
                }
            );
            assert!(
                error
                    .source()
                    .unwrap()
                    .is::<crate::PureProviderCatalogError>()
            );
        }
        let original = novarocks_type_contract::CompileControlError::Cancelled;
        let error = crate::PureProviderCompileError::<Infallible>::Control(original.clone());
        assert_eq!(error.failure_fact(), original.failure_fact());
        assert_eq!(error.source().unwrap().downcast_ref(), Some(&original));
    }
}
