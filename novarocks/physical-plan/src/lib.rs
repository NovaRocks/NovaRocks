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

mod artifact;
mod builder;
mod expression;
mod identity;
mod plan;
mod relation;
mod resource;
mod validation;

pub use artifact::*;
pub use builder::*;
pub use expression::*;
pub use identity::*;
pub use novarocks_connector_contract::{ConnectorWriteRouteId, WriteTargetOrdinal};
pub use novarocks_type_contract::FunctionValueType as ValueType;
pub use novarocks_type_contract::{
    AggregateStateFormatId, BucketLayoutAlgorithm, FunctionArgumentEvaluation,
    FunctionArgumentType, FunctionFailureBehavior, FunctionId, FunctionIdentityError, FunctionKind,
    FunctionOverloadId, FunctionValueType, FunctionVolatility, OrderedComparisonAlgorithm,
    PartitionCountParameterId, PartitionCountParameterIdentityError, PartitionHashAlgorithm,
    PartitionSpaceId, PartitionSpaceIdentityError,
};
pub use plan::*;
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
pub use validation::*;

/// Exact revision of the semantic plan contract implemented by this crate.
///
/// It is part of the repository's complete Native compatibility material.
pub const PLAN_CONTRACT_REVISION: u32 = 3;

#[cfg(test)]
mod tests;
