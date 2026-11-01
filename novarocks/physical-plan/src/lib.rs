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

//! Pure, immutable final physical-plan contract.
//!
//! This crate owns the semantic boundary between planning and execution. It
//! intentionally contains no SQL IR, generated wire DTO, application owner,
//! provider implementation, runtime object, or I/O capability.

mod builder;
mod call_requests;
mod constants;
mod definition_sources;
mod expression;
mod expression_site;
mod failure_projection;
mod frozen_calls;
mod frozen_pruning;
mod identity;
mod package;
mod physical_temporal_source;
mod plan;
mod predicate;
mod pruning_structure;
mod relation;
mod resource;
mod runtime_filter_binding;
mod structure_input;
mod validation;
pub mod wait_graph;
mod write_execution;

pub use builder::*;
pub use call_requests::*;
pub use constants::*;
pub use definition_sources::{FragmentDefinitionSource, visit_fragment_definitions_observed};
pub use expression::*;
pub use expression_site::*;
pub use frozen_calls::*;
pub use frozen_pruning::*;
pub use identity::*;
pub use novarocks_connector_contract::{ConnectorWriteRouteId, WriteTargetOrdinal};
pub use novarocks_constant_contract::{
    ConstantError, ConstantPolicy, ConstantPool, ConstantResourceFacts, ConstantValue,
};
pub use novarocks_function_contract::FunctionArgument as StaticFunctionArgument;
pub use novarocks_type_contract::FunctionValueType as ValueType;
pub use novarocks_type_contract::{
    AggregateStateFormatId, BucketLayoutAlgorithm, DecimalOverflowPolicy,
    FunctionArgumentEvaluation, FunctionArgumentType, FunctionFailureBehavior, FunctionId,
    FunctionIdentityError, FunctionIntrinsicRowError, FunctionKind, FunctionOverloadId,
    FunctionValueType, FunctionVolatility, OrderedComparisonAlgorithm, PartitionCountParameterId,
    PartitionCountParameterIdentityError, PartitionHashAlgorithm, PartitionSpaceId,
    PartitionSpaceIdentityError,
};
pub use package::*;
pub use physical_temporal_source::{
    NonCanonicalNativeV1FunctionName, TemporalSourceProjectionError, native_v1_function_name,
    temporal_source_definitions_observed,
};
pub use plan::*;
pub use predicate::*;
pub use pruning_structure::*;
pub use relation::*;
pub use resource::{
    MAX_ANNOTATION_BYTES, MAX_ANNOTATION_KEY_BYTES, MAX_ANNOTATION_VALUE_BYTES, MAX_ANNOTATIONS,
    MAX_DATA_TYPE_DEPTH, MAX_DATA_TYPE_FIELD_METADATA_BYTES, MAX_DATA_TYPE_FIELD_METADATA_ENTRIES,
    MAX_DATA_TYPE_FIELD_METADATA_KEY_BYTES, MAX_DATA_TYPE_FIELD_METADATA_VALUE_BYTES,
    MAX_DATA_TYPE_FIELD_NAME_BYTES, MAX_DATA_TYPE_NODES, MAX_FIXED_SIZE_LENGTH,
    MAX_FRAGMENT_DYNAMIC_BYTES, MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_PLAN_DERIVED_CUT_BYTES,
    MAX_PLAN_DERIVED_CUT_ITEMS, MAX_PLAN_DYNAMIC_BYTES, MAX_PLAN_DYNAMIC_ITEMS,
    MAX_TIMESTAMP_TIMEZONE_BYTES,
};
pub use runtime_filter_binding::*;
pub use structure_input::FragmentStructureInput;
pub use validation::*;
pub use write_execution::*;

/// Exact revision of the semantic plan contract implemented by this crate.
///
/// It is part of the repository's complete Native compatibility material.
pub const PLAN_CONTRACT_REVISION: u32 = 16;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod physical_temporal_source_tests;

pub use physical_temporal_source::{
    native_v1_emitted_constant_reference, regexp_count_pattern_source_observed,
    to_base64_byte_source_observed,
};
