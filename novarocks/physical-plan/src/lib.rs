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
mod failure_projection;
mod frozen_calls;
mod frozen_pruning;
mod identity;
mod join_condition;
mod node_roots;
mod package;

mod plan;
mod predicate;
mod pruning_structure;
mod relation;
mod resource;
mod runtime_filter_binding;
mod structure_input;
mod system_relation;
pub use system_relation::*;
mod validation;
pub mod wait_graph;
mod write_execution;

pub use builder::*;
pub use call_requests::*;
pub use constants::*;
pub use definition_sources::{FragmentDefinitionSource, visit_fragment_definitions_observed};
pub use expression::*;

pub use frozen_calls::*;
pub use frozen_pruning::*;
pub use identity::*;
pub use join_condition::*;
pub use novarocks_connector_contract::{ConnectorWriteRouteId, WriteTargetOrdinal};
pub use novarocks_constant_contract::{
    ConstantError, ConstantPolicy, ConstantPool, ConstantPools, ConstantReference,
    ConstantResourceFacts, ConstantTableError, ConstantValue,
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

/// Whether a complete grouping key can be compared and partitioned for a
/// `TopNReduction::GroupedStates` reduction.
///
/// Planning and validation use this same installation domain so an optional
/// reduction cannot make a supported query fail during preparation.
// Design: ADR-0236 (docs/adr/ADR-0236-installed-optimization-domains-and-partition-rank-retention.md)
pub fn grouped_state_topn_key_supported(
    comparator: OrderedComparisonAlgorithm,
    ty: &FunctionValueType,
) -> bool {
    comparator.supports_value_type(ty)
        && PartitionHashAlgorithm::NativeExchangeV1.supports_partition_key(ty.data_type())
}

/// Exact revision of the semantic plan contract implemented by this crate.
///
/// It is part of the repository's complete Native compatibility material.
/// Revision 34 declares each hash key match relation and retires the
/// decimal-to-double promotion parameter.
pub const PLAN_CONTRACT_REVISION: u32 = 34;

#[cfg(test)]
mod tests;

// Expression definitions are named directly from their neutral owner. Only
// ExprId/ValueId remain re-exported by the plan's identity vocabulary.
use novarocks_expression_contract::{
    BoundExprSet, BoundExprSetBuilder, BoundFunction, ExprKind, ExprNode, ExprPort, ExprRoot,
    MissingLegacyBindingMetadata, PhysicalCallRequest, RootOrdinal, RootScope, UnitLimits,
};

// Internal fixtures name the same neutral expression vocabulary directly.
#[cfg(test)]
use novarocks_expression_contract::{BinaryOperator, LegacyBindingMetadata, UnaryOperator};
