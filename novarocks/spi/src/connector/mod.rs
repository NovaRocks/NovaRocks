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

mod catalog;
mod catalog_runtime;
mod cleanup_maintenance;
mod codec;
mod context;
mod control;
mod credential;
mod credential_lease;
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
mod mv_storage_observation;
mod predicate;
pub mod provider;
mod provider_binding;
mod publication;
mod read;
mod read_session;
mod resources;
mod row_mutation;
mod scalar;
mod staged_create;
mod statistics;
mod view_metadata;
mod write;

pub mod conformance;
pub mod read_stack;
pub mod write_stack;

pub use catalog::{
    CATALOG_VERSION_BYTES, CatalogHandle, CatalogProperties, CatalogProperty, CatalogProviderKind,
    CatalogVersion, ConnectorControlRuntimeId, MAX_CATALOG_PROPERTIES,
    MAX_CATALOG_PROPERTY_KEY_BYTES, MAX_CATALOG_PROPERTY_VALUE_BYTES, MAX_CATALOG_SET_BYTES,
    MAX_CATALOGS_PER_QUERY, MAX_PRUNE_CATALOG_SET_BYTES, MAX_REACHABLE_CATALOGS_PER_PRUNE,
};
pub use catalog_runtime::{CatalogRuntime, CatalogRuntimeMaterializer};
pub use cleanup_maintenance::{
    BatchReceipt, BatchReceiptSummary, CONNECTOR_CLEANUP_MAINTENANCE_CONTRACT_VERSION,
    CandidatePage, ConnectorCleanupCandidate, ConnectorCleanupCandidatePageRequest,
    ConnectorCleanupExecuteRequest, ConnectorCleanupFinalizeRequest, ConnectorCleanupMaintenance,
    ConnectorCleanupMaintenanceLease, ConnectorCleanupMaintenanceResolver,
    ConnectorCleanupOperation, ConnectorCleanupOperationId, ConnectorCleanupOwnedRefIdentity,
    ConnectorCleanupOwnedRefSelection, ConnectorCleanupPlan, ConnectorCleanupPlanSummary,
    ConnectorCleanupPlanningRequest, ConnectorCleanupPrepareRequest,
    MAX_CONNECTOR_CLEANUP_BATCH_OBJECTS, MAX_CONNECTOR_CLEANUP_BATCHES,
    MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_BYTES, MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS,
    MAX_CONNECTOR_CLEANUP_OWNED_REF_SELECTION_ITEMS, MAX_CONNECTOR_CLEANUP_PROVIDER_PAYLOAD_BYTES,
    PreparedBatch, REMOVE_UNREFERENCED_OBJECTS_KIND,
};
pub use codec::{
    ConnectorCodecCategory, ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision,
    ConnectorDecodeCheckpoint, ConnectorDecodeContext, ConnectorDecodeDepthGuard,
    ConnectorDecodeLedger, ConnectorDecodeLimits, ConnectorEncodedPayload, ConnectorEnvelopeHeader,
    ConnectorFieldPath, ConnectorFieldPathSegment, ConnectorPrivateDecoder,
    ConnectorPrivateEncoder, MAX_CONNECTOR_CODEC_ERROR_DETAIL_BYTES,
    MAX_CONNECTOR_CODEC_FIELD_NAME_BYTES, MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH,
};
pub use context::{
    ConnectorCancellation, ConnectorRequestContext, ConnectorRequestScope,
    ConnectorStorageResolver, ResolvedVendedS3Access, StorageAccessRequest,
};
pub use control::{
    ConnectorControlBinding, ConnectorControlCreation, ConnectorControlFactory,
    ConnectorControlFactoryRequest, ConnectorControlFactoryResolver, ConnectorControlPlanningLease,
    ConnectorControlRegistry, ConnectorControlResolver, ConnectorExecutionDistribution,
    ConnectorScanPlanning,
};
pub use credential::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose,
    CatalogNonSecretProperty, CatalogStorageAccessDomainInput, CatalogUncredentialedStorageKind,
    CredentialConsumerRole, MAX_CATALOG_CREDENTIAL_BINDINGS, MAX_CATALOG_NON_SECRET_PROPERTIES,
    MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES, MAX_STORAGE_CREDENTIAL_SCOPE_PREFIXES,
    StaticCredentialReference, StorageAccessDomainId, StorageCredentialScopePrefix,
    canonical_catalog_credential_binding_bytes, canonicalize_catalog_credential_bindings,
};
pub use credential_lease::{
    ConnectorVendedCredentialLeaseCollectionPort, ConnectorVendedCredentialLeaseSink,
    ConnectorVendedS3CredentialLeaseRefresher, CredentialLeaseDescriptor, CredentialLeaseId,
    CredentialLeaseProvider, CredentialLeaseSecretEnvelope, MAX_CREDENTIAL_LEASE_ID_BYTES,
    MAX_CREDENTIAL_LEASE_PREFIXES, MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES,
    MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES, MAX_CREDENTIAL_LEASES_PER_QUERY,
    VendedS3CredentialLeaseContribution, VendedS3CredentialLeaseEntry,
    VendedS3CredentialLeaseRefresh,
};
pub use data_mutation::{
    CONNECTOR_DATA_MUTATION_CONTRACT_VERSION, CONNECTOR_DATA_MUTATION_DURABLE_WIRE_VERSION,
    ConnectorDataMutation, ConnectorDataMutationAddFilesDomain,
    ConnectorDataMutationExecuteRequest, ConnectorDataMutationLease,
    ConnectorDataMutationOperation, ConnectorDataMutationPlan, ConnectorDataMutationPlanSummary,
    ConnectorDataMutationPlanningRequest, ConnectorDataMutationReceipt,
    ConnectorDataMutationReconcileRequest, ConnectorDataMutationResolver,
    ConnectorDataMutationSourceDomain, ConnectorDataMutationSourceScope,
    ConnectorDataMutationSourceScopeKind, MAX_CONNECTOR_DATA_MUTATION_FILE_LOCATION_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_FILES, MAX_CONNECTOR_DATA_MUTATION_PARQUET_FOOTER_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_SOURCE_LOCATION_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_TARGET_REF_BYTES, MAX_CONNECTOR_DATA_MUTATION_TOTAL_FOOTER_BYTES,
    REGISTER_EXISTING_FILES_KIND, TRUNCATE_KIND,
};
pub use distributed_rewrite::{
    CONNECTOR_DISTRIBUTED_REWRITE_CONTRACT_VERSION, ConnectorDistributedRewrite,
    ConnectorDistributedRewriteCohortPlan, ConnectorDistributedRewriteLease,
    ConnectorDistributedRewriteOperation, ConnectorDistributedRewritePlan,
    ConnectorDistributedRewritePlanSummary, ConnectorDistributedRewritePlanningRequest,
    ConnectorDistributedRewriteReceipt, ConnectorDistributedRewriteReceiptSummary,
    ConnectorDistributedRewriteResolver, ConnectorDistributedRewriteShape,
    ConnectorFrozenRewriteGroup, ConnectorRewriteCohortRead,
    MAX_CONNECTOR_DISTRIBUTED_REWRITE_COHORTS,
    MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES, REWRITE_DATA_FILES_KIND,
    REWRITE_POSITION_DELETES_KIND,
};
pub use distribution::ProviderBindingEpoch;
pub use domain_facts::{
    ConnectorAvailableScanUnitDomainFacts, ConnectorScanUnitColumn, ConnectorScanUnitColumnDomain,
    ConnectorScanUnitColumnFacts, ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsEvidence,
    ConnectorScanUnitFactsMissingReason, ConnectorScanUnitFactsSummary,
    MAX_CONNECTOR_SCAN_UNIT_FACT_COLUMNS, MAX_CONNECTOR_SCAN_UNIT_FACT_PAYLOAD_BYTES,
    MAX_CONNECTOR_SCAN_UNIT_FACT_VARIABLE_VALUE_BYTES,
};
pub use error::{ConnectorError, ConnectorErrorKind, ConnectorTableObjectBindingFailure};
pub use execution::{
    ConnectorPrepareSplitRequest, ConnectorPreparedScanUnit, ConnectorPreparedScanUnitDescriptor,
    ConnectorPreparedScanUnitSet, ConnectorReadExecution,
    MAX_CONNECTOR_PREPARED_SCAN_UNITS_PER_SPLIT,
};
pub use handle::{
    ConnectorPinnedFileSet, ConnectorScanHandle, ConnectorSplit, ConnectorTableHandle,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_PINNED_FILES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
pub use identity::{ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId};
pub use metadata::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, CONNECTOR_MV_APPLY_KEY_COLUMN_PROPERTY,
    CONNECTOR_MV_HIDDEN_COLUMNS_PROPERTY, ConnectorColumnDefault, ConnectorListNamespacesRequest,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceIdentity,
    ConnectorNamespaceRequest, ConnectorReadNamedReference, ConnectorReadReferenceFacts,
    ConnectorReadReferenceFactsRequest, ConnectorReadReferenceKind, ConnectorReadSnapshotLogEntry,
    ConnectorTableColumnPlanningFact, ConnectorTableColumnRole, ConnectorTableColumnSemanticKind,
    ConnectorTableColumnVisibility, ConnectorTableDefinitionColumn, ConnectorTableDefinitionFacts,
    ConnectorTableDefinitionStructField, ConnectorTableDefinitionType,
    ConnectorTableForeignKeyConstraint, ConnectorTableIdentity, ConnectorTableMetadata,
    ConnectorTableObjectBinding, ConnectorTableObjectCaptureRequest, ConnectorTableObjectId,
    ConnectorTableObjectRebindRequest, ConnectorTableObjectSelector, ConnectorTablePlanningFacts,
    ConnectorTableRequest, ConnectorTableResolution, ConnectorTableUniqueConstraint,
    MAX_CONNECTOR_COLUMN_DEFAULT_DEPTH, MAX_CONNECTOR_COLUMN_DEFAULT_NODES,
    MAX_CONNECTOR_TABLE_DEFINITION_COLUMNS, MAX_CONNECTOR_TABLE_DEFINITION_TYPE_DEPTH,
    MAX_CONNECTOR_TABLE_DEFINITION_TYPE_NODES, MAX_CONNECTOR_TABLE_OBJECT_ID_BYTES,
    MAX_CONNECTOR_TABLE_PLANNING_FACT_COLUMNS,
    MAX_CONNECTOR_TABLE_PLANNING_FACT_CONSTRAINT_COLUMNS,
    MAX_CONNECTOR_TABLE_PLANNING_FACT_FOREIGN_KEY_CONSTRAINTS,
    MAX_CONNECTOR_TABLE_PLANNING_FACT_UNIQUE_CONSTRAINTS,
};
pub use metadata_maintenance::{
    CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION, ConnectorMaxCompactableDataFiles,
    ConnectorMaxCompactableDataFilesRequest, ConnectorMetadataMaintenance,
    ConnectorMetadataMaintenanceExecuteRequest, ConnectorMetadataMaintenanceLease,
    ConnectorMetadataMaintenanceOperation, ConnectorMetadataMaintenancePlan,
    ConnectorMetadataMaintenancePlanSummary, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorMetadataMaintenanceReceipt, ConnectorMetadataMaintenanceReceiptSummary,
    ConnectorMetadataMaintenanceResolver, EXPIRE_TABLE_VERSIONS_KIND,
    MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES, MAX_CONNECTOR_METADATA_MAINTENANCE_PATH_BYTES,
    MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES, REWRITE_METADATA_LAYOUT_KIND,
};
pub use mutation::{
    ConnectorCatalogMutation, ConnectorCatalogMutationLease, ConnectorCatalogMutationOperation,
    ConnectorCatalogMutationReceipt, ConnectorCatalogMutationReconcileRequest,
    ConnectorCatalogMutationRequest, ConnectorCatalogMutationResolver, ConnectorColumnAggregation,
    ConnectorColumnDefinition, ConnectorColumnPath, ConnectorColumnPosition,
    ConnectorCommittedVersion, ConnectorDataType, ConnectorDefaultValue,
    ConnectorDropTableDataDisposition, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorMvMetadataOnlyBaseFact,
    ConnectorMvMetadataOnlyProvenance, ConnectorPartitionTransform, ConnectorPropertyAuthority,
    ConnectorPropertyChange, ConnectorRefAction, ConnectorRefKind,
    ConnectorRefreshPublicationGuard, ConnectorSchemaChange, ConnectorStructField,
    ConnectorTableKey, ConnectorTableKeyKind, ConnectorViewDefinition, ConnectorViewDialect,
    ConnectorViewIdentity, ConnectorViewSourceFormat, CreateOrReplacePolicy, CreatePolicy,
    DropPolicy, ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};
pub use mv_storage_observation::{
    MAX_MV_LAKE_BASES, MAX_MV_LAKE_DESCRIPTOR_BYTES, MAX_MV_OBSERVATION_FIELDS,
    MAX_MV_OBSERVATION_PARTITION_FIELDS, MAX_MV_OBSERVATION_REFS, MAX_MV_OBSERVATION_SNAPSHOTS,
    MvCreatedTargetObservation, MvLakeCatalogDiscovery, MvLakeCatalogIncompleteReason,
    MvLakeDescriptorProjection, MvLakePackageFailure, MvLakePackageObservation,
    MvLakePackageOutcome, MvLakePublicationObservation, MvLakeTargetSnapshotObservation,
    MvMaintenanceMetadataObservation, MvObservedField, MvObservedMaintenancePolicy,
    MvObservedPartitionField, MvObservedPartitionSpec, MvObservedPartitionTransform,
    MvObservedRefreshMarker, MvObservedSnapshot, MvPublishedBaseObservation,
    MvPublishedRefreshObservation, MvPublishedRefreshTechnique, MvRefreshBaseObservation,
    MvRefreshTargetObservation, MvSchemaValidationObservation, MvStorageObservationPort,
    UnavailableMvStorageObservationPort,
};
pub use predicate::{
    ConnectorPredicateDisposition, ConnectorPredicateDispositionKind, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateColumn, ConnectorStaticPredicateId,
    ConnectorStaticPredicateKind, MAX_CONNECTOR_STATIC_IN_LITERALS,
    MAX_CONNECTOR_STATIC_LITERAL_PAYLOAD_BYTES, MAX_CONNECTOR_STATIC_PREDICATES,
    MAX_CONNECTOR_STATIC_VARIABLE_LITERAL_BYTES, normalize_predicate_dispositions,
    validate_static_predicates,
};
pub use provider_binding::{
    ConnectorProviderBinding, ConnectorProviderBindingKey, ConnectorProviderBindingKind,
    ConnectorProviderBindingProvider,
};
pub use publication::{
    LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
    LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationStatementTag,
    LakePublicationTarget, LakePublicationTerminal,
};
pub use read::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorBeginScanRequest,
    ConnectorChangePartition, ConnectorChangePartitionField, ConnectorChangePartitionTransform,
    ConnectorChangePartitionValue, ConnectorChangeWindow, ConnectorChangeWindowAdmission,
    ConnectorChangeWindowFullRebuildReason, ConnectorChangeWindowPartitionImpact,
    ConnectorChangeWindowReplaceFailure, ConnectorOpenReaderRequest, ConnectorReadPurpose,
    ConnectorReadSelector, ConnectorReaderMetricsSnapshot, ConnectorReaderOptions, ConnectorScan,
    ConnectorScanAdmission, ConnectorScanSelection, ConnectorSplitPlanningMetrics,
    ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
};
pub use read_session::{
    ConnectorReadSession, ConnectorReadSessionFinalizationContext, ConnectorReadSessionLease,
    ConnectorReadSessionOutcome,
};
pub use resources::{
    ConnectorOutputMemoryToken, ConnectorRequestResources, ConnectorResourceCheckpoint,
    ConnectorResourceClass, ConnectorResourceLease, ConnectorResourceLedger,
    ConnectorResourceReservation,
};
pub use row_mutation::{
    CONNECTOR_ROW_MUTATION_CONTRACT_VERSION, ConnectorMutationEffectField,
    ConnectorMutationMatchContract, ConnectorMutationRouteInput, ConnectorMutationSourceField,
    ConnectorMutationTargetField, ConnectorRowMutationActivationRequest,
    ConnectorRowMutationCohortRecipe, ConnectorRowMutationCohortRecipeBody,
    ConnectorRowMutationEffect, ConnectorRowMutationExecutionPlan, ConnectorRowMutationIntent,
    ConnectorRowMutationPreparation, ConnectorRowMutationPreparationOutcome,
    ConnectorRowMutationPreparationRequest, ConnectorRowMutationRoute,
    ConnectorRowMutationScanBinding, ConnectorRowMutationSelection,
    ConnectorRowMutationSelectionOrdinal, ConnectorRowMutationSelectionView,
    ConnectorRowMutationStrategy, ConnectorWriteRouteId, MAX_CONNECTOR_ROW_MUTATION_ROUTES,
    MAX_CONNECTOR_ROW_MUTATION_SELECTION_BATCHES,
};
pub use scalar::{ConnectorScalarType, ConnectorScalarValue};
pub use staged_create::{
    CONNECTOR_CTAS_UNANCHORED_CLEANUP_CONTRACT_VERSION, CONNECTOR_STAGED_CREATE_CONTRACT_VERSION,
    ConnectorCtasUnanchoredCleanupOutcome, ConnectorCtasUnanchoredCleanupRequest,
    ConnectorCtasUnanchoredDiscoveryRequest, ConnectorCtasUnanchoredProvenance,
    ConnectorStagedCreate, ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
    ConnectorStagedCreateLease, ConnectorStagedCreateOperationId,
    ConnectorStagedCreatePrepareOutcome, ConnectorStagedCreatePrepareRequest,
    ConnectorStagedCreatePublicationAdjudicationOutcome,
    ConnectorStagedCreatePublicationAdjudicationRequest, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreatePublishRequest, ConnectorStagedCreateReceipt,
    ConnectorStagedCreateReceiptPhase, ConnectorStagedTableHandle,
    ConnectorStagedWritePlanningBinding, ConnectorStagedWritePlanningRequest,
    ConnectorStagedWriteProof, ConnectorUnanchoredCtasCleanup, ConnectorUnanchoredCtasCleanupLease,
};
pub use statistics::{
    ConnectorStatistics, ConnectorStatisticsLease, ConnectorStatisticsResolver,
    MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES, MAX_CONNECTOR_STATISTICS_ARTIFACTS,
    MAX_CONNECTOR_STATISTICS_COLUMNS, MAX_CONNECTOR_STATISTICS_METRICS,
    MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES, MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
    MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES, StatisticsArtifactDraft,
    StatisticsArtifactIdentity, StatisticsBasisRelation, StatisticsCollection,
    StatisticsCollectionSession, StatisticsCollectionStart, StatisticsCollectionStartRequest,
    StatisticsColumnSelection, StatisticsDataVersion, StatisticsEvidence,
    StatisticsEvidenceRevision, StatisticsInterval, StatisticsMetric, StatisticsMetricError,
    StatisticsMetricErrorKind, StatisticsMetricObservation, StatisticsMetricRequest,
    StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue, StatisticsMissing,
    StatisticsMissingKind, StatisticsNumericNature, StatisticsReadRequest, StatisticsReader,
    StatisticsReceipt, StatisticsRequiredAggregation, StatisticsRowCoverage, StatisticsScanColumn,
};
pub use view_metadata::{
    ConnectorListViewsRequest, ConnectorViewMetadata, ConnectorViewMetadataValue,
    ConnectorViewRequest,
};
pub use write::{
    ConnectorCommittedPartitionField, ConnectorCommittedPartitioning,
    ConnectorManagedDescriptorProperties, ConnectorManagedPartitionField,
    ConnectorManagedPartitionSpecObservation, ConnectorManagedPartitionSpecPreview,
    ConnectorManagedPartitionSpecPreviewRequest, ConnectorManagedPartitionSpecReplacement,
    ConnectorManagedPartitionSpecReplacementId, ConnectorManagedPartitionSpecReplacementTarget,
    ConnectorManagedPartitionTransform, ConnectorManagedPublicationEmptyInputDisposition,
    ConnectorManagedPublicationIntent, ConnectorManagedPublicationTarget,
    ConnectorManagedPublicationTechnique, ConnectorPreReadyWritePlanningProof,
    ConnectorPreReadyWritePlanningRequest, ConnectorSealedWriteCohortSet,
    ConnectorStagedPublicationBaseFact, ConnectorWriteAbortOutcome, ConnectorWriteActivationIntent,
    ConnectorWriteActivationRequest, ConnectorWriteActivationSource,
    ConnectorWriteAdmissionPurpose, ConnectorWriteBaseVersion, ConnectorWriteCohortDescriptor,
    ConnectorWriteCohortId, ConnectorWriteControl, ConnectorWriteFieldBinding,
    ConnectorWriteFieldRequest, ConnectorWriteFieldToken, ConnectorWriteInputRequest,
    ConnectorWriteInputShape, ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationId,
    ConnectorWritePreparation, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
    ConnectorWriteReceipt, ConnectorWriteTargetRef, DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_BYTES,
    DEFAULT_WRITE_COMMIT_EVIDENCE_MAX_ENTRIES, MAX_CONNECTOR_MANAGED_DESCRIPTOR_PROPERTIES,
    MAX_CONNECTOR_MANAGED_DESCRIPTOR_PROPERTY_BYTES, MAX_CONNECTOR_MANAGED_DESCRIPTOR_TOTAL_BYTES,
    MAX_CONNECTOR_MANAGED_PARTITION_FIELD_TEXT_BYTES, MAX_CONNECTOR_MANAGED_PARTITION_SPEC_FIELDS,
    MAX_CONNECTOR_MANAGED_PUBLICATION_TEXT_BYTES, MAX_CONNECTOR_STAGED_PUBLICATION_BASE_FACTS,
    MAX_CONNECTOR_WRITE_RECEIPT_BYTES, WriteCommitEvidenceLedger, WriteCommitEvidenceLimits,
    WriteCommitEvidenceUsage,
};
