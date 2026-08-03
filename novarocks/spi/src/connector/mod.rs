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

mod cleanup_maintenance;
mod context;
mod control;
mod data_mutation;
mod distributed_rewrite;
mod distribution;
mod domain_facts;
mod error;
mod execution;
mod handle;
mod identity;
mod metadata;
mod metadata_maintenance;
mod mutation;
mod predicate;
mod read;
mod read_session;
mod scalar;
mod staged_create;
mod staged_publication_recovery;
mod statistics;
mod write;

pub mod conformance;

pub use cleanup_maintenance::{
    BatchReceipt, BatchReceiptSummary, CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION,
    CandidatePage, ConnectorCleanupCandidatePageRequest, ConnectorCleanupExecuteRequest,
    ConnectorCleanupFinalizeRequest, ConnectorCleanupMaintenance, ConnectorCleanupMaintenanceLease,
    ConnectorCleanupMaintenanceResolver, ConnectorCleanupOperation, ConnectorCleanupOperationId,
    ConnectorCleanupPlan, ConnectorCleanupPlanSummary, ConnectorCleanupPlanningRequest,
    ConnectorCleanupPrepareRequest, ConnectorCleanupReconcileRequest,
    MAX_CONNECTOR_CLEANUP_BATCH_OBJECTS, MAX_CONNECTOR_CLEANUP_BATCHES,
    MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_BYTES, MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS,
    MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES, PreparedBatch, REMOVE_UNREFERENCED_OBJECTS_KIND,
};
pub use context::{ConnectorCancellation, ConnectorRequestContext};
pub use control::{
    ConnectorControlBinding, ConnectorControlPlanningLease, ConnectorControlRegistry,
    ConnectorControlResolver, ConnectorExecutionDistribution, ConnectorScanPlanning,
};
pub use data_mutation::{
    CONNECTOR_DATA_MUTATION_CONTRACT_VERSION, CONNECTOR_DATA_MUTATION_DURABLE_WIRE_VERSION,
    ConnectorDataMutation, ConnectorDataMutationExecuteRequest, ConnectorDataMutationLease,
    ConnectorDataMutationOperation, ConnectorDataMutationPlan, ConnectorDataMutationPlanSummary,
    ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorDataMutationResolver,
    ConnectorDataMutationSourceScope, ConnectorDataMutationSourceScopeKind,
    MAX_CONNECTOR_DATA_MUTATION_FILE_LOCATION_BYTES, MAX_CONNECTOR_DATA_MUTATION_FILES,
    MAX_CONNECTOR_DATA_MUTATION_PARQUET_FOOTER_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_SOURCE_LOCATION_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_TARGET_REF_BYTES, MAX_CONNECTOR_DATA_MUTATION_TOTAL_FOOTER_BYTES,
    REGISTER_EXISTING_FILES_KIND, TRUNCATE_KIND,
};
pub use distributed_rewrite::{
    CONNECTOR_DISTRIBUTED_REWRITE_CONTRACT_VERSION, ConnectorDistributedRewrite,
    ConnectorDistributedRewriteAttemptCheckpoint, ConnectorDistributedRewriteAttemptDisposition,
    ConnectorDistributedRewriteCohortPlan, ConnectorDistributedRewriteLease,
    ConnectorDistributedRewriteOperation, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewritePlanSummary, ConnectorDistributedRewritePlanningRequest,
    ConnectorDistributedRewriteReceipt, ConnectorDistributedRewriteReceiptSummary,
    ConnectorDistributedRewriteResolver, MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS,
    MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES, REWRITE_DATA_FILES_KIND,
    REWRITE_POSITION_DELETES_KIND,
};
pub use distribution::{
    ConnectorExecutionDeclaration, ConnectorInstanceIncarnation,
    MAX_CONNECTOR_INSTANCE_DECLARATION_PAYLOAD_BYTES,
};
pub use domain_facts::{
    ConnectorAvailableScanUnitDomainFacts, ConnectorScanUnitColumn, ConnectorScanUnitColumnDomain,
    ConnectorScanUnitColumnFacts, ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsEvidence,
    ConnectorScanUnitFactsMissingReason, ConnectorScanUnitFactsSummary,
    MAX_CONNECTOR_SCAN_UNIT_FACT_COLUMNS, MAX_CONNECTOR_SCAN_UNIT_FACT_PAYLOAD_BYTES,
    MAX_CONNECTOR_SCAN_UNIT_FACT_VARIABLE_VALUE_BYTES,
};
pub use error::{ConnectorError, ConnectorErrorKind};
pub use execution::{
    ConnectorExecutionBinding, ConnectorExecutionBindingKey, ConnectorExecutionInstaller,
    ConnectorExecutionResolver, ConnectorPrepareSplitRequest, ConnectorPreparedScanUnit,
    ConnectorPreparedScanUnitDescriptor, ConnectorPreparedScanUnitSet, ConnectorReadExecution,
    MAX_CONNECTOR_PREPARED_SCAN_UNITS_PER_SPLIT,
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
pub use metadata_maintenance::{
    CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION, ConnectorMetadataMaintenance,
    ConnectorMetadataMaintenanceExecuteRequest, ConnectorMetadataMaintenanceLease,
    ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenancePlan,
    ConnectorMetadataMaintenancePlanSummary, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorMetadataMaintenanceReceipt, ConnectorMetadataMaintenanceReceiptSummary,
    ConnectorMetadataMaintenanceReconcileRequest, ConnectorMetadataMaintenanceResolver,
    EXPIRE_TABLE_VERSIONS_KIND, MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES,
    MAX_CONNECTOR_METADATA_MAINTENANCE_PATH_BYTES,
    MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES, REWRITE_METADATA_LAYOUT_KIND,
};
pub use mutation::{
    ConnectorCatalogMutation, ConnectorCatalogMutationLease, ConnectorCatalogMutationOperation,
    ConnectorCatalogMutationReceipt, ConnectorCatalogMutationReconcileRequest,
    ConnectorCatalogMutationRequest, ConnectorCatalogMutationResolver, ConnectorColumnAggregation,
    ConnectorColumnDefinition, ConnectorColumnPath, ConnectorColumnPosition,
    ConnectorCommittedVersion, ConnectorDataType, ConnectorDefaultValue,
    ConnectorDropTableDataDisposition, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorPartitionTransform, ConnectorPropertyChange,
    ConnectorRefAction, ConnectorRefKind, ConnectorRefreshPublicationGuard, ConnectorSchemaChange,
    ConnectorStructField, ConnectorTableKey, ConnectorTableKeyKind, ConnectorViewDefinition,
    ConnectorViewDialect, ConnectorViewIdentity, CreateOrReplacePolicy, CreatePolicy, DropPolicy,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};
pub use predicate::{
    ConnectorPredicateDisposition, ConnectorPredicateDispositionKind, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateColumn, ConnectorStaticPredicateId,
    ConnectorStaticPredicateKind, MAX_CONNECTOR_STATIC_IN_LITERALS,
    MAX_CONNECTOR_STATIC_LITERAL_PAYLOAD_BYTES, MAX_CONNECTOR_STATIC_PREDICATES,
    MAX_CONNECTOR_STATIC_VARIABLE_LITERAL_BYTES, normalize_predicate_dispositions,
    validate_static_predicates,
};
pub use read::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest,
    ConnectorOpenReaderRequest, ConnectorReadSelector, ConnectorReaderMetricsSnapshot,
    ConnectorScan, ConnectorSplitPlanningMetrics, ConnectorSplitPlanningRequest,
    ConnectorSplitPlanningResult,
};
pub use read_session::{
    ConnectorReadSession, ConnectorReadSessionFinalizationContext, ConnectorReadSessionLease,
    ConnectorReadSessionOutcome,
};
pub use scalar::{ConnectorScalarType, ConnectorScalarValue};
pub use staged_create::{
    CONNECTOR_STAGED_CREATE_CONTRACT_VERSION, ConnectorStagedCreate,
    ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
    ConnectorStagedCreateLease, ConnectorStagedCreateOperationId,
    ConnectorStagedCreatePrepareOutcome, ConnectorStagedCreatePrepareRequest,
    ConnectorStagedCreatePublishOutcome, ConnectorStagedCreatePublishRequest,
    ConnectorStagedCreateReceipt, ConnectorStagedCreateReceiptPhase,
    ConnectorStagedCreateReconcileOutcome, ConnectorStagedCreateReconcilePhase,
    ConnectorStagedCreateReconcileRequest, ConnectorStagedTableHandle,
    ConnectorStagedWritePlanningBinding, ConnectorStagedWritePlanningRequest,
};
pub use staged_publication_recovery::{
    ConnectorHistoricalPublicationAction, ConnectorStagedPublicationBaseFact,
    ConnectorStagedPublicationCleanupReceipt, ConnectorStagedPublicationCleanupRequest,
    ConnectorStagedPublicationDescriptor, ConnectorStagedPublicationDisposition,
    ConnectorStagedPublicationObservation, ConnectorStagedPublicationPhase,
    ConnectorStagedPublicationPhaseState, ConnectorStagedPublicationProof,
    ConnectorStagedPublicationRecovery, MAX_CONNECTOR_STAGED_PUBLICATION_BASE_FACTS,
    MAX_CONNECTOR_STAGED_PUBLICATION_COHORTS, MAX_CONNECTOR_STAGED_PUBLICATION_LINEAGE_FACTS,
    MAX_CONNECTOR_STAGED_PUBLICATION_PROOF_BYTES,
};
pub use statistics::{
    ConnectorStatistics, ConnectorStatisticsLease, ConnectorStatisticsResolver,
    MAX_CONNECTOR_STATISTICS_METRICS, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES, StatisticsAccuracy,
    StatisticsCollection, StatisticsCollectionPlan, StatisticsCollectionRequest,
    StatisticsCollectionResult, StatisticsCoverage, StatisticsDataVersion, StatisticsEvidence,
    StatisticsEvidenceRevision, StatisticsInterval, StatisticsMetric, StatisticsMetricError,
    StatisticsMetricErrorKind, StatisticsMetricRequest, StatisticsMetricState,
    StatisticsMetricValue, StatisticsMissing, StatisticsMissingKind, StatisticsProvenance,
    StatisticsPublishPreparationRequest, StatisticsPublishRequest, StatisticsReadRequest,
    StatisticsReader, StatisticsReceipt, StatisticsReconcileRequest, StatisticsScanColumn,
};
pub use write::{
    CONNECTOR_WRITE_CONTRACT_VERSION, ConnectorBatchWriter, ConnectorOpenWriterRequest,
    ConnectorSealedWriteCohortSet, ConnectorStagedReport, ConnectorStagedReportFrame,
    ConnectorStagedReportSummary, ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest,
    ConnectorWriteAttemptCompletion, ConnectorWriteCohortCompletion,
    ConnectorWriteCohortDescriptor, ConnectorWriteCohortId, ConnectorWriteCommitRequest,
    ConnectorWriteControl, ConnectorWriteExecution, ConnectorWriteExecutionId,
    ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationCompletion,
    ConnectorWriteOperationId, ConnectorWritePlan, ConnectorWritePlanningRequest,
    ConnectorWriteReceipt, ConnectorWriteReconcileRequest, ConnectorWriteResolver,
    ConnectorWriterHandle, ConnectorWriterIdentity, ConnectorWriterTerminalState,
    MAX_CONNECTOR_STAGED_REPORT_FRAME_BYTES, MAX_CONNECTOR_STAGED_REPORT_PARTS,
    MAX_CONNECTOR_STAGED_REPORT_PAYLOAD_BYTES, MAX_CONNECTOR_WRITE_COHORTS,
    MAX_CONNECTOR_WRITE_OPERATION_PAYLOAD_BYTES, MAX_CONNECTOR_WRITE_OPERATION_WRITERS,
    MAX_CONNECTOR_WRITE_RECEIPT_BYTES,
};
