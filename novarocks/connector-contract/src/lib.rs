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
mod failure_projection;
mod identity;
mod metadata_materialization_copy;
mod mutation;
mod owned_copy;
mod predicate;
mod pure_catalogue;
mod pure_compile;
mod read;
mod read_facts;
mod read_program;
mod read_public;
mod recipe;
mod scan;
mod schema;
mod source_schema_facts;
mod type_projection;
mod value;
mod write;
mod write_input;
mod write_recipe;
mod write_schema;

pub use catalog::{CATALOG_VERSION_BYTES, CatalogHandle, CatalogVersion};
pub use codec::{
    ConnectorCodecCategory, ConnectorCodecContractError, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorReadRelationPayload,
};
pub use error::{ConnectorError, ConnectorErrorKind, ConnectorTableObjectBindingFailure};
pub use identity::{
    ConnectorIdentityError, ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
};
pub use mutation::{
    ConnectorMutationContributionGroupId, ConnectorRowMutationEffect, ConnectorWriteRouteId,
    PositionShardWindow,
};
pub use owned_copy::{WriterOwnedCreationFailure, WriterOwnedResourceFacts};
pub use predicate::{
    Bound, ConnectorExpression, ConnectorFunctionName, Constraint, Domain,
    MAX_CONNECTOR_EXPRESSION_DEPTH, MAX_CONNECTOR_EXPRESSION_NODES, MAX_CONNECTOR_VALUE_BYTES,
    MAX_TUPLE_DOMAIN_COLUMNS, MAX_VALUE_SET_DISCRETE_VALUES, MAX_VALUE_SET_RANGES, Range,
    TupleDomain, ValueSet,
};
pub use read::{ConnectorReadBinding, ConnectorReadRelationKind};
pub use read_facts::{
    ConnectorReadArtifactCoverage, ConnectorReadBucketLayout, ConnectorReadDistribution,
    ConnectorReadInputVersion, ConnectorReadMetadataKind, ConnectorReadMetadataRequest,
    ConnectorReadMetadataVersion, ConnectorReadNullOrdering, ConnectorReadOrderingKey,
    ConnectorReadPartitionCountDomain, ConnectorReadPartitionHash, ConnectorReadProperties,
    ConnectorReadSortDirection, ConnectorReadStaticFacts, MAX_READ_COVERAGE_EVIDENCE_BYTES,
    MAX_READ_INPUT_VERSION_BYTES, MAX_READ_PROPERTY_KEYS,
};
pub use read_program::{
    ConnectorReadProgramCompileError, ConnectorReadProgramCompiler, ConnectorReadProgramRecipe,
    FrozenConnectorRead,
};
pub use read_public::{ConnectorReadPublicFacts, TypedReadCreationFailure};
pub use recipe::{
    ConnectorReadRecipeSplit, ConnectorReadRecipeSplitDraft, ConnectorReadRecipeSplitFacts,
    ConnectorReadRecipeSplitKind, ConnectorReadRelationRecipe,
    ConnectorReadRelationRecipeCompileError, ConnectorReadRelationRecipeCompiler,
    ConnectorReadRelationRecipeDraft, ConnectorReadRelationRecipeError, ConnectorRecipeHostAddress,
    MAX_CONNECTOR_RECIPE_ADDRESSES, MAX_CONNECTOR_RECIPE_AFFINITY_BYTES,
    MAX_CONNECTOR_RECIPE_BYTES, MAX_CONNECTOR_RECIPE_COLUMNS, MAX_CONNECTOR_RECIPE_PAYLOAD_BYTES,
    MAX_CONNECTOR_RECIPE_SPLIT_WEIGHT, ReadRecipeOwnedError,
};
pub use source_schema_facts::{
    ConnectorReadExecutionSchema, ConnectorSourceFieldFacts, ConnectorSourceInitialDefault,
    ConnectorSourceSchemaFacts, ConnectorSourceSchemaShape,
};
pub use type_projection::{
    connector_type_accepts_arrow, connector_type_accepts_value_type, connector_type_for_arrow,
    connector_type_for_value_type,
};
pub use value::{ConnectorValue, ConnectorValueType, MAX_CONNECTOR_DECIMAL_PRECISION};
pub use write::{ConnectorWriteFieldToken, MAX_CONNECTOR_WRITE_TARGETS, WriteTargetOrdinal};
pub use write_input::{
    ConnectorWriteBinding, ConnectorWriteFieldBinding, ConnectorWriteFieldRef,
    ConnectorWriteInputRef, ConnectorWriteInputShape, write_field_matches_execution_type_observed,
};
pub use write_recipe::{
    ConnectorWriteRecipe, ConnectorWriteRecipeCompileError, ConnectorWriteRecipeCompiler,
    ConnectorWriteRecipeDraft, MAX_CONNECTOR_WRITE_INPUT_FIELDS, MAX_CONNECTOR_WRITER_HANDLE_BYTES,
};
pub use write_schema::*;

pub use scan::{
    ConnectorScan, FrozenConnectorScan, MAX_STATIC_SCAN_RETAINED_BYTES, ReadScanOwnedError,
    ScanColumnId, ScanConstructionInput, StaticConnectorScan, StaticConnectorScanError,
    StaticScanAssignment, StaticScanDynamicFilter,
};

pub use pure_compile::PureProviderCompileError;

pub use pure_catalogue::{
    PureProviderCatalogError, PureProviderManifestEntry, PureProviderProgramCatalog,
    PureProviderProgramDefinition, PureProviderProgramError,
};
