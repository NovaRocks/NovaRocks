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

mod context;
mod control;
mod distribution;
mod error;
mod execution;
mod handle;
mod identity;
mod metadata;
mod mutation;
mod predicate;
mod read;

pub mod conformance;

pub use context::{ConnectorCancellation, ConnectorRequestContext};
pub use control::{
    ConnectorControlBinding, ConnectorControlPlanningLease, ConnectorControlRegistry,
    ConnectorControlResolver, ConnectorExecutionDistribution, ConnectorScanPlanning,
};
pub use distribution::{
    ConnectorExecutionDeclaration, ConnectorInstanceIncarnation,
    MAX_CONNECTOR_INSTANCE_DECLARATION_PAYLOAD_BYTES,
};
pub use error::{ConnectorError, ConnectorErrorKind};
pub use execution::{
    ConnectorExecutionBinding, ConnectorExecutionBindingKey, ConnectorExecutionInstaller,
    ConnectorExecutionResolver, ConnectorReadExecution,
};
pub use handle::{
    ConnectorScanHandle, ConnectorSplit, ConnectorTableHandle, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
pub use identity::{ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId};
pub use metadata::{
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceIdentity,
    ConnectorNamespaceRequest, ConnectorTableIdentity, ConnectorTableMetadata,
    ConnectorTableRequest, ConnectorTableResolution,
};
pub use mutation::{
    ConnectorCatalogMutation, ConnectorCatalogMutationLease, ConnectorCatalogMutationOperation,
    ConnectorCatalogMutationReceipt, ConnectorCatalogMutationReconcileRequest,
    ConnectorCatalogMutationRequest, ConnectorCatalogMutationResolver, ConnectorColumnAggregation,
    ConnectorColumnDefinition, ConnectorColumnPath, ConnectorColumnPosition, ConnectorDataType,
    ConnectorDefaultValue, ConnectorDropTableDataDisposition, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ConnectorPartitionTransform,
    ConnectorPropertyChange, ConnectorRefAction, ConnectorRefKind,
    ConnectorRefreshPublicationGuard, ConnectorSchemaChange, ConnectorStructField,
    ConnectorTableKey, ConnectorTableKeyKind, ConnectorViewDefinition, ConnectorViewDialect,
    ConnectorViewIdentity, CreateOrReplacePolicy, CreatePolicy, DropPolicy, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
    MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};
pub use predicate::{
    ConnectorPredicateDisposition, ConnectorPredicateDispositionKind, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateColumn, ConnectorStaticPredicateDataType,
    ConnectorStaticPredicateId, ConnectorStaticPredicateKind, ConnectorStaticPredicateLiteral,
    MAX_CONNECTOR_STATIC_IN_LITERALS, MAX_CONNECTOR_STATIC_LITERAL_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATIC_PREDICATES, MAX_CONNECTOR_STATIC_VARIABLE_LITERAL_BYTES,
    normalize_predicate_dispositions, validate_static_predicates,
};
pub use read::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest,
    ConnectorOpenReaderRequest, ConnectorReadSelector, ConnectorReaderMetricsSnapshot,
    ConnectorScan, ConnectorSplitPlanningMetrics, ConnectorSplitPlanningRequest,
    ConnectorSplitPlanningResult,
};
