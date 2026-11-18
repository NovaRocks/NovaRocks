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

//! Immutable program authors own static validation reasons and support limits.
use crate::{SinkCompileError, StaticSinkError};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, OuterFailureProjection,
    ResourceDimension, ResourceFacts, domain,
};
use std::num::NonZeroU16;

fn fact(class: FailureClass, code: u16) -> FailureFact {
    FailureFact::new(
        class,
        FailureReason::new(domain::LOCAL_PROGRAM, NonZeroU16::new(code).unwrap()),
    )
}
fn limit(code: u16, dimension: ResourceDimension, maximum: usize) -> FailureFact {
    fact(FailureClass::Resource, code).with_resource(ResourceFacts {
        request_scope: None,
        constraint_scope: None,
        dimension,
        requested: None,
        required_growth: None,
        limit: u64::try_from(maximum).ok(),
        live: None,
        committed: None,
    })
}

impl FailureProjection for StaticSinkError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::InvalidDestinationNode => fact(FailureClass::Invariant, 20),
            Self::DuplicateOutputColumn => fact(FailureClass::Invariant, 21),
            Self::TooManyColumns => limit(
                22,
                ResourceDimension::Items,
                crate::sink::MAX_STATIC_SINK_COLUMNS,
            ),
            Self::TooManyExpressions => limit(
                23,
                ResourceDimension::Items,
                crate::sink::MAX_STATIC_SINK_EXPRESSIONS,
            ),
            Self::InvalidPartitionExpressions => fact(FailureClass::Invariant, 24),
            Self::InvalidExpression => fact(FailureClass::Invariant, 25),
            Self::EmptyBranches => fact(FailureClass::Invariant, 26),
            Self::TooManyBranches => limit(
                27,
                ResourceDimension::Items,
                crate::sink::MAX_STATIC_SINK_BRANCHES,
            ),
            Self::SplitArityMismatch => fact(FailureClass::Invariant, 28),
            Self::InvalidRouter => fact(FailureClass::Invariant, 29),
            Self::InvalidSummaryLayout => fact(FailureClass::Invariant, 30),
            Self::InvalidDeliveryRequirement => fact(FailureClass::Invariant, 31),
        }
    }
}

impl FailureProjection for SinkCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Sink(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
        }
    }
}

impl OuterFailureProjection for StaticSinkError {}

impl OuterFailureProjection for SinkCompileError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StaticStreamBranch;
    use novarocks_execution_contract::DataStreamPartitionType;
    use novarocks_types::SlotId;

    #[test]
    fn actual_sink_shape_and_support_limit_keep_distinct_facts() {
        let duplicate = StaticStreamBranch::try_new(
            1,
            DataStreamPartitionType::Unpartitioned,
            vec![],
            vec![SlotId::new(7), SlotId::new(7)],
            None,
            crate::StaticStreamContract {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                routing: crate::StaticStreamRouting::Ordinary,
            },
        )
        .unwrap_err();
        assert_eq!(duplicate, StaticSinkError::DuplicateOutputColumn);
        assert_eq!(duplicate.failure_fact().class(), FailureClass::Invariant);
        let oversized = StaticStreamBranch::try_new(
            1,
            DataStreamPartitionType::Unpartitioned,
            vec![],
            vec![SlotId::new(7); crate::sink::MAX_STATIC_SINK_COLUMNS + 1],
            None,
            crate::StaticStreamContract {
                role: crate::ProgramOutputRole::Relational,
                delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                routing: crate::StaticStreamRouting::Ordinary,
            },
        )
        .unwrap_err();
        assert_eq!(oversized, StaticSinkError::TooManyColumns);
        let facts = oversized.failure_fact();
        assert_eq!(facts.class(), FailureClass::Resource);
        assert_eq!(facts.representation(), None);
        let resource = facts.resource().unwrap();
        assert_eq!(
            resource.limit,
            Some(crate::sink::MAX_STATIC_SINK_COLUMNS as u64)
        );
        assert_eq!(resource.requested, None);
        assert_eq!(resource.committed, None);
        assert_eq!(SinkCompileError::Sink(oversized).failure_fact(), facts);
    }

    #[test]
    fn compile_wrappers_forward_original_control_without_a_static_reason() {
        for source in [
            novarocks_type_contract::CompileControlError::Cancelled,
            novarocks_type_contract::CompileControlError::DeadlineExceeded,
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        ] {
            assert_eq!(
                SinkCompileError::Control(source.clone()).failure_fact(),
                source.failure_fact()
            );
            struct Stop(novarocks_type_contract::CompileControlError);
            impl novarocks_type_contract::PureCompileControl for Stop {
                fn checkpoint(
                    &self,
                    _: novarocks_type_contract::CompilePhase,
                    _: u32,
                ) -> Result<(), novarocks_type_contract::CompileControlError> {
                    Err(self.0.clone())
                }
            }
            let error = crate::StaticSinkProgram::Result
                .validate_for_compile(&[], &Stop(source.clone()))
                .unwrap_err();
            assert_eq!(error, SinkCompileError::Control(source.clone()));
            assert_eq!(error.failure_fact(), source.failure_fact());
        }
    }
}

use crate::*;

#[derive(Clone, Copy)]
#[repr(u16)]
enum ProgramReason {
    FilterEmptyOrderedKeys = 410,
    FilterKindContractMismatch = 411,
    FilterReductionContractMismatch = 412,
    FilterBlockingOrderedConsumer = 413,
    AggregateMissing = 420,
    AggregateNonScalarResult = 421,
    AggregateArgumentCount = 422,
    LayoutArityMismatch = 200,
    LayoutDuplicateSlot = 201,
    LayoutUnknownSlot = 202,
    LayoutTooDeep = 203,
    LayoutTooManyMetadataNodes = 204,
    LayoutEncode = 205,
    LayoutMetadataOwnerConflict = 206,
    LayoutValueBindingMismatch = 207,
    LayoutTypedProjectionRequiresOwner = 208,
    LayoutAllocationModel = 209,
    StaticValuesSchemaMismatch = 220,
    StaticValuesTooManyBytes = 221,
    StaticValuesNoDynamicCell = 222,
    StaticValuesCellOutOfRange = 223,
    StaticValuesCellOrder = 224,
    StaticValuesColumnCoverage = 225,
    BindingRequirementsDuplicate = 230,
    LocalProgramEmpty = 240,
    LocalProgramTooManyNodes = 241,
    LocalProgramInvalidRoot = 242,
    LocalProgramInvalidChild = 243,
    LocalProgramUnreachableNode = 244,
    LocalProgramTooDeep = 245,
    LocalProgramExpandedLimit = 246,
    LocalProgramDuplicateNativeNode = 247,
    LocalProgramInvalidExpression = 248,
    LocalProgramInvalidNodeShape = 249,
    LocalProgramLayoutMismatch = 250,
    LocalProgramInvalidRequirement = 251,
    LocalProgramInvalidSink = 252,
    LocalProgramUnsupportedKernelAbi = 253,
    ProgramResolvedCallsTooManyItems = 280,
    ProgramResolvedCallsInvalidSite = 281,
    ProgramResolvedCallsDuplicateSite = 282,
    ProgramResolvedCallsMissingSite = 283,
    ProgramResolvedCallsWrongSource = 284,
    ProgramResolvedCallsWrongContext = 285,
    ProgramResolvedCallsWrongArguments = 289,
    ProgramResolvedCallsWrongLifecycle = 291,
    ProgramResolvedCallsWrongPhase = 292,
    ProgramResolvedCallsWrongOrder = 293,
    ProgramResolvedCallsWrongWindow = 294,
    ProgramResolvedCallsWrongStateFormat = 295,
    ProgramResolvedCallsTypeMismatch = 296,
    ProgramResolvedCallsInvalidType = 297,
    ProgramResolvedCallsDuplicateRelationCall = 298,
    ProgramChannelTypeTooManyChannels = 310,
    ProgramChannelTypeDuplicateSite = 311,
    ProgramChannelTypeMissingSite = 312,
    ProgramChannelTypeInvalidSite = 313,
    ProgramChannelTypeWrongShape = 314,
    ProgramChannelTypeWrongKind = 315,
    ProgramChannelTypeTypeMismatch = 316,
    ProvenanceEmpty = 340,
    ProvenanceDuplicateOperator = 341,
    ProvenanceInvalidLoweredNode = 342,
    ProvenanceDuplicateLoweredNode = 343,
    ProvenanceInvalidSource = 344,
    ProvenanceDuplicateSource = 345,
    ProvenanceInvalidOrigin = 346,
    ProvenanceDuplicateSplitPiece = 347,
    ProvenanceDuplicateDirectSource = 348,
    ProvenanceUncoveredLoweredNode = 349,
    ProvenanceMissingCostOwner = 350,
    ProvenanceIndirectCostOwner = 351,
    ProvenanceCostMetricMismatch = 352,
    CompiledOriginsInvalidLocalIdentity = 360,
    CompiledOriginsSourceMismatch = 361,
    ProviderLinkLegacyScan = 370,
    ProviderLinkMissingWriterRecipe = 371,
    ProviderLinkUnexpectedWriterRecipe = 372,
    ProviderLinkColumnCount = 373,
    ProviderLinkSchemaMetadata = 374,
    ProviderLinkFieldMismatch = 375,
    ProviderLinkMissingChannel = 376,
    ProviderLinkTypeMismatch = 377,
    ProviderLinkOrdinalOverflow = 378,
    LocalProgramCompileMissingSink = 390,
    LocalProgramCompileExchangeInputMismatch = 391,
    LocalProgramCompileDuplicateExchangeReceiver = 392,
    LocalProgramCompileScanInputMismatch = 393,
    LocalProgramCompileDuplicateScanNode = 394,
    LocalProgramCompileAggregateMismatch = 395,
    LocalProgramCompilePartialFinalization = 396,
}

impl FailureProjection for LayoutError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::ValueType(source) => source.failure_fact(),
            Self::Resource(source) => source.failure_fact(),
            Self::Encoding(source) => source.failure_fact(),
            Self::ArityMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::LayoutArityMismatch as u16,
            ),
            Self::DuplicateSlot => fact(
                FailureClass::Invariant,
                ProgramReason::LayoutDuplicateSlot as u16,
            ),
            Self::UnknownSlot => fact(
                FailureClass::Invariant,
                ProgramReason::LayoutUnknownSlot as u16,
            ),
            Self::TooDeep => limit(
                ProgramReason::LayoutTooDeep as u16,
                ResourceDimension::Items,
                crate::layout::MAX_SLOT_METADATA_DEPTH,
            ),
            Self::TooManyMetadataNodes => limit(
                ProgramReason::LayoutTooManyMetadataNodes as u16,
                ResourceDimension::Items,
                crate::layout::MAX_SLOT_METADATA_NODES,
            ),
            Self::Encode => fact(FailureClass::Invariant, ProgramReason::LayoutEncode as u16),
            Self::MetadataOwnerConflict => fact(
                FailureClass::Invariant,
                ProgramReason::LayoutMetadataOwnerConflict as u16,
            ),
            Self::ValueBindingMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::LayoutValueBindingMismatch as u16,
            ),
            Self::TypedProjectionRequiresOwner => fact(
                FailureClass::Invariant,
                ProgramReason::LayoutTypedProjectionRequiresOwner as u16,
            ),
            Self::AllocationModel => fact(
                FailureClass::Resource,
                ProgramReason::LayoutAllocationModel as u16,
            ),
        }
    }
}

impl FailureProjection for StaticValuesError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::SchemaMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::StaticValuesSchemaMismatch as u16,
            ),
            Self::TooManyBytes => limit(
                ProgramReason::StaticValuesTooManyBytes as u16,
                ResourceDimension::Bytes,
                crate::values::MAX_STATIC_VALUES_BACKING_BYTES,
            ),
            Self::NoDynamicCell => fact(
                FailureClass::Invariant,
                ProgramReason::StaticValuesNoDynamicCell as u16,
            ),
            Self::CellOutOfRange => fact(
                FailureClass::Invariant,
                ProgramReason::StaticValuesCellOutOfRange as u16,
            ),
            Self::CellOrder => fact(
                FailureClass::Invariant,
                ProgramReason::StaticValuesCellOrder as u16,
            ),
            Self::ColumnCoverage => fact(
                FailureClass::Invariant,
                ProgramReason::StaticValuesColumnCoverage as u16,
            ),
        }
    }
}

impl FailureProjection for BindingRequirementsError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Duplicate => fact(
                FailureClass::Invariant,
                ProgramReason::BindingRequirementsDuplicate as u16,
            ),
        }
    }
}

impl FailureProjection for LocalProgramError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Layout(source) => source.failure_fact(),
            Self::Sink(source) => source.failure_fact(),
            Self::Empty => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramEmpty as u16,
            ),
            Self::TooManyNodes => limit(
                ProgramReason::LocalProgramTooManyNodes as u16,
                ResourceDimension::Items,
                crate::MAX_PROGRAM_NODES,
            ),
            Self::InvalidRoot => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramInvalidRoot as u16,
            ),
            Self::InvalidChild => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramInvalidChild as u16,
            ),
            Self::UnreachableNode => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramUnreachableNode as u16,
            ),
            Self::TooDeep => limit(
                ProgramReason::LocalProgramTooDeep as u16,
                ResourceDimension::Items,
                crate::MAX_PROGRAM_NODE_DEPTH,
            ),
            Self::ExpandedLimit => limit(
                ProgramReason::LocalProgramExpandedLimit as u16,
                ResourceDimension::Items,
                crate::MAX_PROGRAM_EXPANDED_OCCURRENCES,
            ),
            Self::DuplicateNativeNode => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramDuplicateNativeNode as u16,
            ),
            Self::InvalidExpression => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramInvalidExpression as u16,
            ),
            Self::InvalidNodeShape => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramInvalidNodeShape as u16,
            ),
            Self::LayoutMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramLayoutMismatch as u16,
            ),
            Self::InvalidRequirement => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramInvalidRequirement as u16,
            ),
            Self::InvalidSink => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramInvalidSink as u16,
            ),
            Self::UnsupportedKernelAbi => fact(
                FailureClass::Dependency,
                ProgramReason::LocalProgramUnsupportedKernelAbi as u16,
            ),
        }
    }
}

impl FailureProjection for ProgramResolvedCallsError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::TooManyItems => fact(
                FailureClass::Resource,
                ProgramReason::ProgramResolvedCallsTooManyItems as u16,
            ),
            Self::InvalidSite => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsInvalidSite as u16,
            ),
            Self::DuplicateSite => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsDuplicateSite as u16,
            ),
            Self::MissingSite(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsMissingSite as u16,
            ),
            Self::WrongSource => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsWrongSource as u16,
            ),
            Self::WrongContext => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsWrongContext as u16,
            ),
            Self::DuplicateRelationCall => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsDuplicateRelationCall as u16,
            ),
            Self::WrongArguments => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsWrongArguments as u16,
            ),
            Self::WrongLifecycle => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsWrongLifecycle as u16,
            ),
            Self::WrongPhase => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsWrongPhase as u16,
            ),
            Self::WrongOrder => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsWrongOrder as u16,
            ),
            Self::WrongWindow => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsWrongWindow as u16,
            ),
            Self::WrongStateFormat => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsWrongStateFormat as u16,
            ),
            Self::TypeMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsTypeMismatch as u16,
            ),
            Self::InvalidType => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramResolvedCallsInvalidType as u16,
            ),
        }
    }
}

impl FailureProjection for ProgramChannelTypeError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::ValueType(source) => source.failure_fact(),
            Self::Kernel(source) => source.failure_fact(),
            Self::TooManyChannels => limit(
                ProgramReason::ProgramChannelTypeTooManyChannels as u16,
                ResourceDimension::Items,
                crate::MAX_PROGRAM_TYPED_CHANNELS,
            ),
            Self::DuplicateSite => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramChannelTypeDuplicateSite as u16,
            ),
            Self::MissingSite(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramChannelTypeMissingSite as u16,
            ),
            Self::InvalidSite => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramChannelTypeInvalidSite as u16,
            ),
            Self::WrongShape => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramChannelTypeWrongShape as u16,
            ),
            Self::WrongKind => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramChannelTypeWrongKind as u16,
            ),
            Self::TypeMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::ProgramChannelTypeTypeMismatch as u16,
            ),
        }
    }
}

impl FailureProjection for ProvenanceError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Empty => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceEmpty as u16,
            ),
            Self::DuplicateOperator => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceDuplicateOperator as u16,
            ),
            Self::InvalidLoweredNode => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceInvalidLoweredNode as u16,
            ),
            Self::DuplicateLoweredNode => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceDuplicateLoweredNode as u16,
            ),
            Self::InvalidSource => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceInvalidSource as u16,
            ),
            Self::DuplicateSource => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceDuplicateSource as u16,
            ),
            Self::InvalidOrigin => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceInvalidOrigin as u16,
            ),
            Self::DuplicateSplitPiece => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceDuplicateSplitPiece as u16,
            ),
            Self::DuplicateDirectSource => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceDuplicateDirectSource as u16,
            ),
            Self::UncoveredLoweredNode => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceUncoveredLoweredNode as u16,
            ),
            Self::MissingCostOwner => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceMissingCostOwner as u16,
            ),
            Self::IndirectCostOwner => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceIndirectCostOwner as u16,
            ),
            Self::CostMetricMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::ProvenanceCostMetricMismatch as u16,
            ),
        }
    }
}

impl FailureProjection for CompiledOriginsError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Provenance(source) => source.failure_fact(),
            Self::InvalidLocalIdentity => fact(
                FailureClass::Invariant,
                ProgramReason::CompiledOriginsInvalidLocalIdentity as u16,
            ),
            Self::SourceMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::CompiledOriginsSourceMismatch as u16,
            ),
        }
    }
}

impl FailureProjection for ProviderLinkError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::ValueType(source) => source.failure_fact(),
            Self::LegacyScan(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProviderLinkLegacyScan as u16,
            ),
            Self::MissingWriterRecipe(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProviderLinkMissingWriterRecipe as u16,
            ),
            Self::UnexpectedWriterRecipe(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProviderLinkUnexpectedWriterRecipe as u16,
            ),
            Self::ColumnCount(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProviderLinkColumnCount as u16,
            ),
            Self::SchemaMetadata(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProviderLinkSchemaMetadata as u16,
            ),
            Self::FieldMismatch { .. } => fact(
                FailureClass::Invariant,
                ProgramReason::ProviderLinkFieldMismatch as u16,
            ),
            Self::MissingChannel(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProviderLinkMissingChannel as u16,
            ),
            Self::TypeMismatch(_) => fact(
                FailureClass::Invariant,
                ProgramReason::ProviderLinkTypeMismatch as u16,
            ),
            Self::OrdinalOverflow(_) => fact(
                FailureClass::Resource,
                ProgramReason::ProviderLinkOrdinalOverflow as u16,
            ),
        }
    }
}

impl FailureProjection for LocalProgramCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Control(source) => source.failure_fact(),
            Self::Origins(source) => source.failure_fact(),
            Self::Provider(source) => source.failure_fact(),
            Self::Calls(source) => source.failure_fact(),
            Self::Channels(source) => source.failure_fact(),
            Self::Sink(source) => source.failure_fact(),
            Self::TooManyDefinitions => limit(
                300,
                ResourceDimension::Items,
                novarocks_expression::MAX_STATIC_EXPRESSIONS,
            ),
            Self::TooManyConstantBytes => limit(
                5,
                ResourceDimension::Bytes,
                novarocks_expression::MAX_STATIC_EXPRESSION_DYNAMIC_BYTES,
            ),
            Self::InvalidExpression => LocalProgramError::InvalidExpression.failure_fact(),
            Self::MissingSink => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramCompileMissingSink as u16,
            ),
            Self::ExchangeInputMismatch(_) => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramCompileExchangeInputMismatch as u16,
            ),
            Self::DuplicateExchangeReceiver(_) => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramCompileDuplicateExchangeReceiver as u16,
            ),
            Self::ScanInputMismatch(_) => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramCompileScanInputMismatch as u16,
            ),
            Self::DuplicateScanNode(_) => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramCompileDuplicateScanNode as u16,
            ),
            Self::AggregateMismatch(_) => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramCompileAggregateMismatch as u16,
            ),
            Self::PartialFinalization(_) => fact(
                FailureClass::Invariant,
                ProgramReason::LocalProgramCompilePartialFinalization as u16,
            ),
        }
    }
}

impl FailureProjection for LayoutCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Layout(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
        }
    }
}

impl FailureProjection for ValuesCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Values(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
            Self::Resource(source) => source.failure_fact(),
        }
    }
}

impl FailureProjection for BindingRequirementsCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Requirement(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
        }
    }
}

impl FailureProjection for ProgramCompileError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Program(source) => source.failure_fact(),
            Self::Control(source) => source.failure_fact(),
        }
    }
}

#[cfg(test)]
mod compiled_owner_tests {
    use super::*;
    use novarocks_type_contract::{
        CompileControlError, ExecutionValueTypeError, owned_resources::layout::LayoutResourceError,
    };
    use std::error::Error;
    #[test]
    fn actual_layout_conversion_keeps_type_depth_and_geometry_instead_of_mismatch() {
        let depth = LayoutCompileError::from(ExecutionValueTypeError::Depth);
        assert_eq!(
            depth.failure_fact(),
            ExecutionValueTypeError::Depth.failure_fact()
        );
        assert!(
            depth
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<ExecutionValueTypeError>()
        );
        for original in [
            LayoutResourceError::SourceModel,
            LayoutResourceError::Arithmetic,
            LayoutResourceError::ArcBacking,
        ] {
            let converted = LayoutCompileError::from(original);
            assert_eq!(converted.failure_fact(), original.failure_fact());
            assert!(
                converted
                    .source()
                    .unwrap()
                    .source()
                    .unwrap()
                    .is::<LayoutResourceError>()
            );
        }
    }
    #[test]
    fn program_compilation_preserves_control_and_configured_support_bound() {
        let stopped = LayoutCompileError::Control(CompileControlError::Cancelled);
        assert_eq!(
            stopped.failure_fact(),
            CompileControlError::Cancelled.failure_fact()
        );
        let wide = ProgramCompileError::Program(LocalProgramError::TooManyNodes);
        assert_eq!(wide.failure_fact().class(), FailureClass::Resource);
        assert_eq!(
            wide.failure_fact().resource().unwrap().limit,
            Some(crate::MAX_PROGRAM_NODES as u64)
        );
        assert!(wide.source().unwrap().is::<LocalProgramError>());
    }
}

impl FailureProjection for crate::StaticFilterError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::EmptyOrderedKeys => fact(
                FailureClass::Invariant,
                ProgramReason::FilterEmptyOrderedKeys as u16,
            ),
            Self::KindContractMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::FilterKindContractMismatch as u16,
            ),
            Self::ReductionContractMismatch => fact(
                FailureClass::Invariant,
                ProgramReason::FilterReductionContractMismatch as u16,
            ),
            Self::BlockingOrderedConsumer => fact(
                FailureClass::Invariant,
                ProgramReason::FilterBlockingOrderedConsumer as u16,
            ),
        }
    }
}

impl FailureProjection for crate::FrozenAggregateSelectionError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::MissingAggregate => fact(
                FailureClass::Invariant,
                ProgramReason::AggregateMissing as u16,
            ),
            Self::NonScalarResult => fact(
                FailureClass::Invariant,
                ProgramReason::AggregateNonScalarResult as u16,
            ),
            Self::InvalidLogicalArgumentCount => fact(
                FailureClass::Invariant,
                ProgramReason::AggregateArgumentCount as u16,
            ),
        }
    }
}
