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

#![cfg_attr(
    test,
    expect(
        clippy::disallowed_methods,
        reason = "Unit tests exercise thread/task mechanics under their original test owners; see ADR-0218."
    )
)]

//! Pure, task-independent facts used while compiling a local fragment program.
//!
//! This crate deliberately has no execution, transport, worker, connector SPI,
//! or async runtime dependency. A program's static representation moves here
//! as its node and expression types are separated from task-owned bindings.

mod ordinal;
pub use ordinal::{ProgramOrdinalFailure, checked_program_ordinal};
mod unit_inputs;
mod unit_refs;
pub use unit_refs::{ProgramRootRef, ProgramUnitId};
mod batch_sequence;
mod compiled;
mod compiled_origins;
mod contract;
mod creation;
mod failure_projection;
mod layout;
mod program;
mod provenance;
mod provider_links;
mod requirements;
mod resolved_calls;
mod runtime_filter;
mod sink;
mod typed_channels;
mod values;
pub use batch_sequence::{BatchSequenceCreationFailure, StaticBatchSequence};
mod writer_preparation;

pub use compiled::{
    CompiledAggregate, CompiledAggregateGrouping, CompiledExchangeInput, CompiledExchangeRouting,
    CompiledProgramFacts, CompiledScanInput, InputCompletion, LocalProgram,
    LocalProgramCompileError,
};
pub use compiled_origins::CompiledOriginsError;
pub use contract::{
    CompileProfile, FragmentProgramOptions, FragmentSinkAssignmentKind,
    FragmentSinkAssignmentRequirement, KernelAbiVersion, LayoutIdentity, RuntimeFilterContract,
    RuntimeFilterId, ScanAssignmentKind, ScanSourceContract,
};
pub use creation::ProgramCreationFailure;
pub use layout::{LayoutCompileError, LayoutCreationFailure, LayoutError, StaticLayout};
pub use novarocks_connector_contract::{
    ScanColumnId, StaticConnectorScan, StaticConnectorScanError, StaticScanAssignment,
    StaticScanDynamicFilter,
};
// The partition vocabulary of `StaticStreamBranch`, re-exported so a compiler
// can author a stream sink without a second execution-contract dependency.
pub use novarocks_execution_contract::{DataStreamPartitionType, ExchangeDeliveryRequirement};
mod frozen_aggregate;
pub use frozen_aggregate::{FrozenAggregateSelection, FrozenAggregateSelectionError};

pub use program::{
    AggregateTopNFilter, AnalyticOutputColumn, AssertRowsMode, ChangeEventOutputExpr,
    ChangeEventSpec, FilterConsumerAtExpr, FilterConsumerAtJoinKey, FilterProducerAtExpr,
    JoinDistributionMode, JoinType, LocalProgramError, LocalProgramGraph, LocalSortMode,
    MAX_PROGRAM_EXPANDED_OCCURRENCES, MAX_PROGRAM_NODES, MAX_RECURSIVE_PIPELINE_BUILD_DEPTH,
    NestedLoopJoinType, PartitionRank, ProgramAssertionApplicability, ProgramCompileError,
    ProgramContentApply, ProgramContentApplyTarget, ProgramJoinCondition, ProgramJoinKey,
    ProgramNode, ProgramNodeKind, ProgramPositionShardInput, ProgramScanSource,
    ProgramWriteLayoutEffects, ProjectExpressionSlot, ProjectExpressionSlots, RowAssertion,
    SetOpKind, SortExpression, StaticAggregateCall, StaticAggregateOrder,
    StaticAggregateTypeSignature, StaticWindowFunction, StaticWriterProjection,
    StreamingPreaggregationMode, TableFunctionOutputSlot, UnpivotConstant, UnpivotMapping,
    UnpivotPassthrough, WindowBoundary, WindowFrame, WindowFunctionKind, WindowType,
    WriterFinalAggregateCall, WriterFinalAggregatePlan, WriterGroupedUnpivotMapping,
    WriterGroupedUnpivotPlan, WriterPartialAggregateCall,
};
pub use provenance::*;
pub use provider_links::ProviderLinkError;
pub use requirements::{
    BindingRequirement, BindingRequirements, BindingRequirementsCompileError,
    BindingRequirementsError, ProgramNodeId, ScanSourceKind,
};
pub use resolved_calls::*;
pub use runtime_filter::{
    AggregateTopNFilters, FilterConsumerActivation, FilterLateApplyGranularity,
    FilterMembershipSchema, FilterMembershipSchemaError, FilterNullOrder, FilterNullSemantics,
    FilterOrderKey, FilterOrderKeys, FilterProducerKind, FilterReduction, FilterSortDirection,
    ScanFilterConsumers, StaticFilterConsumer, StaticFilterContract, StaticFilterError,
    StaticFilterProducer, encode_filter_key_type, is_aggregate_topn_key_carrier,
    is_aggregate_topn_key_type, is_filter_key_type, is_ordered_filter_flat_payload_type,
    is_semantic_filter_key_type, single_key_ordered_filter_digests_observed,
    temporal_order_key_type_bytes, visit_filter_key_type, visit_ordered_filter_key_type,
};
pub use sink::{
    MAX_STATIC_SINK_BRANCHES, MAX_STATIC_SINK_COLUMNS, MAX_STATIC_SINK_EXPRESSIONS,
    ProgramOutputRole, ProgramWriteLayoutPreference, SinkCompileError, StaticRouterRoute,
    StaticSinkError, StaticSinkProgram, StaticStreamBranch, StaticStreamContract,
    StaticStreamRouting, StaticSummaryAuthor, StaticSummaryBranch, validate_summary_layout,
};
pub use typed_channels::*;
pub use values::{
    StaticValueBatch, StaticValues, StaticValuesBacking, StaticValuesCell, StaticValuesError,
    ValuesCompileError, ValuesCreationFailure,
};
pub use writer_preparation::{
    PreparedWriterAggregateCall, PreparedWriterAggregateCalls, PreparedWriterCallsError,
};

// Shared semantic nodes retain their original creation responsibility.
pub use novarocks_type_contract::ValueTypeRef;

pub use novarocks_result_contract::{
    DeleteVerificationCounts, FinishCountMode, FinishInputs, LogicalChangeCounts, PerKind,
    SummaryField, SummaryKind, SummaryValue, SummaryWireType,
};

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod unit_properties;
