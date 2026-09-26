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

//! Neutral, immutable connector vocabulary embedded in physical plans.
//!
//! This crate owns pure read values, predicates, identities, errors, and
//! bounded recipes. Provider services and Arrow runtime values remain outside
//! this boundary.

mod catalog;
mod codec;
mod error;
mod identity;
mod mutation;
mod predicate;
mod read;
mod recipe;
mod value;
mod write;

pub use catalog::{CATALOG_VERSION_BYTES, CatalogHandle, CatalogVersion};
pub use codec::{
    ConnectorCodecCategory, ConnectorCodecContractError, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorReadRelationPayload,
};
pub use error::{ConnectorError, ConnectorErrorKind, ConnectorTableObjectBindingFailure};
pub use identity::{
    ConnectorIdentityError, ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
};
pub use mutation::{ConnectorRowMutationEffect, ConnectorWriteRouteId};
pub use predicate::{
    Bound, ConnectorExpression, ConnectorFunctionName, Constraint, Domain,
    MAX_CONNECTOR_EXPRESSION_DEPTH, MAX_CONNECTOR_EXPRESSION_NODES, MAX_CONNECTOR_VALUE_BYTES,
    MAX_TUPLE_DOMAIN_COLUMNS, MAX_VALUE_SET_DISCRETE_VALUES, MAX_VALUE_SET_RANGES, Range,
    TupleDomain, ValueSet,
};
pub use read::{ConnectorReadBinding, ConnectorReadRelationKind, ConnectorReadWorkSource};
pub use recipe::{
    ConnectorReadRecipeSplit, ConnectorReadRecipeSplitDraft, ConnectorReadRecipeSplitFacts,
    ConnectorReadRecipeSplitKind, ConnectorReadRelationRecipe,
    ConnectorReadRelationRecipeCompileError, ConnectorReadRelationRecipeCompiler,
    ConnectorReadRelationRecipeDraft, ConnectorReadRelationRecipeError, ConnectorRecipeHostAddress,
    MAX_CONNECTOR_RECIPE_ADDRESSES, MAX_CONNECTOR_RECIPE_AFFINITY_BYTES,
    MAX_CONNECTOR_RECIPE_BYTES, MAX_CONNECTOR_RECIPE_COLUMNS, MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES,
    MAX_CONNECTOR_RECIPE_SPLIT_WEIGHT,
};
pub use value::{ConnectorValue, ConnectorValueType, MAX_CONNECTOR_DECIMAL_PRECISION};
pub use write::{ConnectorWriteFieldToken, MAX_CONNECTOR_WRITE_TARGETS, WriteTargetOrdinal};
