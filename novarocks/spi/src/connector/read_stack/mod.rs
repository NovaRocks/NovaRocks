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

//! The Trino-aligned connector read stack.
//!
//! This module owns the transport-neutral vocabulary every provider read uses:
//! typed values and predicates, handles, splits and their weights, lazy split
//! enumeration, page production, dynamic filters, and system relations.
//!
//! It deliberately contains no provider name, no generated wire DTO, no
//! opaque payload, and no downcast. Concrete provider facts that cross the
//! FE/BE boundary are carried by the central IDL's closed per-category
//! `oneof`s, validated by `novarocks-proto-codec`, and converted to concrete domain
//! types only inside the provider that produced them.
// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)

pub mod adapter;
pub mod dynamic_filter;
pub mod handle;
pub mod page_source;
pub mod predicate;
pub mod projection;
pub mod runtime;
pub mod session;
pub mod split;
pub mod split_source;
pub mod system_table;
pub mod value;

pub use dynamic_filter::{
    BoundsMatch, ColumnValueBounds, CompleteAllDynamicFilter, DynamicFilter, DynamicFilterSnapshot,
};
pub use handle::{
    ColumnHandle, ConnectorMergeTableHandle, ConnectorTableExecuteHandle,
    ConnectorTableFunctionHandle, ConnectorTableHandle, ConnectorTransactionHandle,
    MAX_SCHEMA_TABLE_NAME_BYTES, SchemaTableName,
};
pub use page_source::{
    ConnectorDataCacheOptions, ConnectorPageSource, ConnectorPageSourceProviderOptions,
    LazyBlockLoader, PageSourceFileMetrics, PageSourceMetrics, SourcePage,
};
pub use predicate::{
    Bound, ConnectorExpression, ConnectorFunctionName, Constraint, Domain,
    MAX_CONNECTOR_EXPRESSION_DEPTH, MAX_CONNECTOR_EXPRESSION_NODES, MAX_CONNECTOR_VALUE_BYTES,
    MAX_TUPLE_DOMAIN_COLUMNS, MAX_VALUE_SET_DISCRETE_VALUES, MAX_VALUE_SET_RANGES, Range,
    TupleDomain, ValueSet,
};
pub use projection::{
    Assignment, ConstraintApplicationResult, LimitApplicationResult, MAX_PROJECTION_ASSIGNMENTS,
    OrderedAssignments, ProjectionApplicationResult,
};
pub use runtime::{
    ConnectorReadAttemptAccessMint, ConnectorReadAttemptAccessReacquirer,
    ConnectorReadAttemptAccessSealer, ConnectorReadAttemptAccessSource,
    ConnectorReadAttemptRuntime, ConnectorReadBinding, ConnectorReadChangeWindow,
    ConnectorReadColumnBinding, ConnectorReadColumnHandle, ConnectorReadConstraint,
    ConnectorReadDynamicFilter, ConnectorReadDynamicFilterSnapshot, ConnectorReadFilterApplication,
    ConnectorReadFrozenRewriteGroup, ConnectorReadLimitApplication, ConnectorReadMetadata,
    ConnectorReadPageSourceProvider, ConnectorReadProviderFactory, ConnectorReadRegistrationLease,
    ConnectorReadRelation, ConnectorReadRelationKind, ConnectorReadRelationVersion,
    ConnectorReadRequestControl, ConnectorReadRequestControlFactory, ConnectorReadSplit,
    ConnectorReadSplitFacts, ConnectorReadSplitManager, ConnectorReadSplitSource,
    ConnectorReadSystemTablePlan, ConnectorReadSystemTableProvider,
    ConnectorReadTableExecuteProcedure, ConnectorReadTableHandle, ConnectorReadTransactionHandle,
    ConnectorReadWorkSource,
};
pub use session::{ConnectorSession, MAX_SESSION_PROPERTIES, SessionPropertyValue};
pub use split::{ConnectorSplit, HostAddress, STANDARD_SPLIT_WEIGHT_RAW, SplitWeight};
pub use split_source::{ConnectorSplitBatch, ConnectorSplitSource, SplitSourceProfile};
pub use system_table::{SystemTableColumn, SystemTableDistribution};
pub use value::{ConnectorValue, ConnectorValueType, MAX_CONNECTOR_DECIMAL_PRECISION};
