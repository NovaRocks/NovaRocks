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

pub mod binding;
mod cancellation;
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
pub mod document_storage;
mod error;
mod execution;
mod handle;
mod identity;
mod io_admission;
mod listing;
mod maintenance_duration;
mod metadata;
mod metadata_maintenance;
mod mutation;
mod mv_storage_observation;
mod original_result_scope;
mod payload_retention;
mod predicate;
mod process_local;
mod property;
pub mod provider;
mod provider_binding;
mod publication;
mod read;
mod read_session;
mod resources;
mod row_mutation;
mod scalar;
mod scoped_begin;
mod semantic_revision;
mod staged_create;
mod statistics;
mod view_metadata;
mod write;

pub mod conformance;
pub mod read_stack;
pub mod write_stack;

pub use binding::{
    ConnectorControlReadBinding, ConnectorControlRoleBinding, ConnectorControlWriteBinding,
    ConnectorExecutionReadBinding, ConnectorExecutionRoleBinding, ConnectorExecutionWriteBinding,
    ConnectorMaterializationError, ConnectorMaterializationErrorClass,
    ConnectorMaterializationRetryDisposition, ConnectorPartitionKeyEvaluator,
    ConnectorReadAttemptAccess, ConnectorReadAttemptCapabilities,
    ConnectorReadAttemptCapabilitiesFuture, ConnectorWriteLayoutExecution, MaterializationContext,
    NormalizedCatalogProperties,
};
pub use cancellation::{ConnectorStopOwner, ConnectorStopView};
pub use catalog::{
    CATALOG_VERSION_BYTES, CatalogHandle, CatalogProperties, CatalogProperty, CatalogVersion,
    ConnectorControlRuntimeId, MAX_CATALOG_PROPERTIES, MAX_CATALOG_PROPERTY_KEY_BYTES,
    MAX_CATALOG_PROPERTY_VALUE_BYTES, MAX_CATALOG_SET_BYTES, MAX_CATALOGS_PER_QUERY,
    MAX_PRUNE_CATALOG_SET_BYTES, MAX_REACHABLE_CATALOGS_PER_PRUNE,
};
pub use catalog_runtime::{CatalogRuntime, CatalogRuntimeMaterializer};
pub use cleanup_maintenance::{
    ConnectorCleanupAsyncExecution, ConnectorCleanupAsyncObservation, ConnectorCleanupAsyncOutput,
    ConnectorCleanupAsyncSource, ConnectorCleanupAsyncStart, ConnectorCleanupMaintenance,
    ConnectorCleanupMaintenanceResolver, ConnectorCleanupMaintenanceSession,
    ConnectorCleanupOperationId, ConnectorOrphanCleanupMetrics,
};
pub use codec::{
    ConnectorCodecCategory, ConnectorCodecContractError, ConnectorCodecError,
    ConnectorCodecErrorKind, ConnectorCodecRevision, ConnectorDecodeCheckpoint,
    ConnectorDecodeContext, ConnectorDecodeDepthGuard, ConnectorDecodeLedger,
    ConnectorDecodeLimits, ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorFieldPath,
    ConnectorFieldPathSegment, ConnectorPrivateDecoder, ConnectorPrivateEncoder,
    ConnectorReadRelationPayload, ConnectorReadSplitCategory, ConnectorReadSplitPayload,
    ConnectorReadWireDecoder, ConnectorReadWireEncoder, ConnectorWriteFragmentWireDecoder,
    ConnectorWriteFragmentWireEncoder, ConnectorWriteHandleWireDecoder,
    ConnectorWriteHandleWireEncoder, MAX_CONNECTOR_CODEC_ERROR_DETAIL_BYTES,
    MAX_CONNECTOR_CODEC_FIELD_NAME_BYTES, MAX_CONNECTOR_CODEC_FIELD_PATH_DEPTH,
};
pub use context::{
    ConnectorAttemptContext, ConnectorOperationControl, ConnectorOperationControlView,
    ConnectorPlanningContext, ConnectorRangeScope, ConnectorRequestContext,
    ConnectorRequestInitiation, ConnectorRequestScope, ConnectorStorageResolver,
    ResolvedVendedS3Access, StorageAccessRequest, VendedS3SeedMaterial,
};
pub use control::{
    ConnectorControlBinding, ConnectorControlCreation, ConnectorControlFactory,
    ConnectorControlFactoryRequest, ConnectorControlFactoryResolver, ConnectorControlPlanningPin,
    ConnectorControlRegistry, ConnectorControlResolver, ConnectorExecutionDistribution,
    ConnectorScanPlanning,
};
pub use credential::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose,
    CatalogNonSecretProperty, CatalogStorageAccessDomainInput, CatalogUncredentialedStorageKind,
    CredentialConsumerRole, MAX_CATALOG_CREDENTIAL_BINDINGS, MAX_CATALOG_NON_SECRET_PROPERTIES,
    MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES, MAX_STORAGE_CREDENTIAL_SCOPE_PREFIXES,
    NodeLocalCredentialReference, StaticCredentialReference, StorageAccessDomainId,
    StorageCredentialScopePrefix, canonical_catalog_credential_binding_bytes,
    canonicalize_catalog_credential_bindings,
};
pub use credential_lease::{
    ConnectorVendedCredentialLeaseCollectionPort, ConnectorVendedCredentialLeaseSink,
    ConnectorVendedS3CredentialLeaseRefresher, CredentialAcquisitionOwnerId,
    CredentialLeaseDescriptor, CredentialLeaseId, CredentialLeaseProvider,
    CredentialLeaseSecretEnvelope, CredentialLoadTableDelegation, CredentialRenewalPath,
    CredentialRoutingContext, CredentialRoutingHeader, MAX_CREDENTIAL_IDENTIFIER_BYTES,
    MAX_CREDENTIAL_LEASE_ID_BYTES, MAX_CREDENTIAL_LEASE_PREFIXES,
    MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES, MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES,
    MAX_CREDENTIAL_LEASES_PER_QUERY, MAX_CREDENTIAL_NAMESPACE_LEVELS,
    MAX_CREDENTIAL_ROUTING_HEADER_BYTES, MAX_CREDENTIAL_ROUTING_HEADERS,
    MAX_CREDENTIALS_ENDPOINT_BYTES, VendedS3CredentialLeaseContribution,
    VendedS3CredentialLeaseEntry, VendedS3CredentialLeaseRefresh,
    VendedS3CredentialRefreshCallPolicy, VendedS3CredentialRefreshDispatch,
    VendedS3CredentialRefreshDispatchGuard, VendedS3InProcessAcquisition,
    VendedS3InProcessAcquisitionIdentity, VendedS3SeedGrantId,
};
pub use data_mutation::{
    CONNECTOR_DATA_MUTATION_CONTRACT_VERSION, CONNECTOR_DATA_MUTATION_DURABLE_WIRE_VERSION,
    ConnectorDataMutation, ConnectorDataMutationAddFilesDomain,
    ConnectorDataMutationExecuteRequest, ConnectorDataMutationOperation, ConnectorDataMutationPlan,
    ConnectorDataMutationPlanSummary, ConnectorDataMutationPlanningRequest,
    ConnectorDataMutationReceipt, ConnectorDataMutationReconcileRequest,
    ConnectorDataMutationResolver, ConnectorDataMutationSession, ConnectorDataMutationSourceDomain,
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
    ConnectorDistributedRewriteCohortPlan, ConnectorDistributedRewriteOperation,
    ConnectorDistributedRewritePlan, ConnectorDistributedRewritePlanSummary,
    ConnectorDistributedRewritePlanningRequest, ConnectorDistributedRewriteReceipt,
    ConnectorDistributedRewriteReceiptSummary, ConnectorDistributedRewriteResolver,
    ConnectorDistributedRewriteShape, ConnectorFrozenRewriteGroup, ConnectorRewriteCohortRead,
    ConnectorRewritePlanSource, ConnectorRewritePrepared, ConnectorRewriteSelection,
    ConnectorRewriteSourceRequest, ConnectorRewriteWriteBeginSource,
    MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES, REWRITE_DATA_FILES_KIND,
    REWRITE_POSITION_DELETES_KIND, RewriteCpuInput, RewriteCpuPort, RewritePlanCpuSource,
    SharedConnectorDistributedRewrite,
};
pub use distribution::ProviderBindingEpoch;
pub use document_storage::*;
pub use error::{
    ConnectorError, ConnectorErrorKind, ConnectorTableFormatVersionUnsupported,
    ConnectorTableObjectBindingFailure,
};
pub use execution::{ConnectorPrepareSplitRequest, MAX_CONNECTOR_PREPARED_SCAN_UNITS_PER_SPLIT};
pub use handle::{
    ConnectorPinnedFileSet, ConnectorScanHandle, ConnectorSplit, ConnectorTableHandle,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_PINNED_FILES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};
pub use identity::{
    ConnectorIdentityError, ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
};
pub use io_admission::{
    ConnectorIoAdmissionPort, ConnectorIoCleanupFuture, ConnectorIoPermit,
    ConnectorIoProgressSource, ConnectorIoRequest, ConnectorIoRequestSource,
    ConnectorIoStreamCustody, ConnectorIoWeakPhase, HdfsServiceOrigin, NetworkServiceOrigin,
    SelectedThriftServiceOrigin, SharedDependency, ThriftServiceOrigin,
};
pub use listing::{ConnectorListingCollector, ConnectorListingPageSize};
pub use maintenance_duration::{
    ConnectorMaintenanceDuration, ConnectorMaintenanceRetention,
    EXPIRE_SNAPSHOTS_MIN_RETENTION_SESSION_PROPERTY,
    REMOVE_ORPHAN_FILES_MIN_RETENTION_SESSION_PROPERTY,
};
pub use metadata::{
    CONNECTOR_FIELD_HIDDEN_FROM_SQL, CONNECTOR_MV_APPLY_KEY_COLUMN_PROPERTY,
    CONNECTOR_MV_HIDDEN_COLUMNS_PROPERTY, ConnectorColumnDefault, ConnectorListNamespacesRequest,
    ConnectorListTablesRequest, ConnectorMetadata, ConnectorNamespaceIdentity,
    ConnectorNamespaceRequest, ConnectorReadNamedReference, ConnectorReadReferenceFacts,
    ConnectorReadReferenceFactsRequest, ConnectorReadReferenceKind, ConnectorReadSnapshotLogEntry,
    ConnectorTableColumnPlanningFact, ConnectorTableColumnRole, ConnectorTableColumnSemanticKind,
    ConnectorTableColumnVisibility, ConnectorTableDefinitionColumn, ConnectorTableDefinitionFacts,
    ConnectorTableDefinitionStructField, ConnectorTableDefinitionType,
    ConnectorTableForeignKeyConstraint, ConnectorTableIdentity, ConnectorTableJobAdmissionRequest,
    ConnectorTableJobKind, ConnectorTableMetadata, ConnectorTableMetadataHeader,
    ConnectorTableObjectBinding, ConnectorTableObjectCaptureRequest, ConnectorTableObjectId,
    ConnectorTableObjectRebindRequest, ConnectorTableObjectSelector,
    ConnectorTableObjectSnapshotFuture, ConnectorTablePlanningFacts, ConnectorTablePreparation,
    ConnectorTablePreparationFuture, ConnectorTableRequest, ConnectorTableResolution,
    ConnectorTableUniqueConstraint, MAX_CONNECTOR_COLUMN_DEFAULT_DEPTH,
    MAX_CONNECTOR_COLUMN_DEFAULT_NODES, MAX_CONNECTOR_TABLE_DEFINITION_COLUMNS,
    MAX_CONNECTOR_TABLE_DEFINITION_TYPE_DEPTH, MAX_CONNECTOR_TABLE_DEFINITION_TYPE_NODES,
    MAX_CONNECTOR_TABLE_OBJECT_ID_BYTES, MAX_CONNECTOR_TABLE_PLANNING_FACT_COLUMNS,
    MAX_CONNECTOR_TABLE_PLANNING_FACT_CONSTRAINT_COLUMNS,
    MAX_CONNECTOR_TABLE_PLANNING_FACT_FOREIGN_KEY_CONSTRAINTS,
    MAX_CONNECTOR_TABLE_PLANNING_FACT_UNIQUE_CONSTRAINTS,
};
pub use metadata_maintenance::{
    CONNECTOR_METADATA_MAINTENANCE_CONTRACT_VERSION, ConnectorExpireSnapshotsExecute,
    ConnectorManifestRewriteMetrics, ConnectorMaxCompactableDataFiles,
    ConnectorMaxCompactableDataFilesExecution, ConnectorMaxCompactableDataFilesRequest,
    ConnectorMaxCompactableDataFilesSource, ConnectorMetadataCleanupResponsibility,
    ConnectorMetadataMaintenance, ConnectorMetadataMaintenanceExecuteRequest,
    ConnectorMetadataMaintenanceExecution, ConnectorMetadataMaintenanceExecutionSource,
    ConnectorMetadataMaintenanceFuture, ConnectorMetadataMaintenanceIoGeometry,
    ConnectorMetadataMaintenanceIoProfile, ConnectorMetadataMaintenanceOperation,
    ConnectorMetadataMaintenanceOutcome, ConnectorMetadataMaintenancePlan,
    ConnectorMetadataMaintenancePlanSummary, ConnectorMetadataMaintenancePlanningRequest,
    ConnectorMetadataMaintenanceReceipt, ConnectorMetadataMaintenanceReceiptSummary,
    ConnectorMetadataMaintenanceResolver, ConnectorMetadataMaintenanceSession,
    EXPIRE_TABLE_VERSIONS_KIND, MAX_CONNECTOR_METADATA_MAINTENANCE_MARKER_BYTES,
    MAX_CONNECTOR_METADATA_MAINTENANCE_PROVIDER_PAYLOAD_BYTES, REWRITE_METADATA_LAYOUT_KIND,
};
pub use mutation::{
    ConnectorCatalogMutation, ConnectorCatalogMutationNeededCommitted,
    ConnectorCatalogMutationObservation, ConnectorCatalogMutationOperation,
    ConnectorCatalogMutationReceipt, ConnectorCatalogMutationReceiving,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorCatalogMutationResolver, ConnectorCatalogMutationSession, ConnectorColumnDefinition,
    ConnectorColumnPath, ConnectorColumnPosition, ConnectorCommittedVersion, ConnectorDataType,
    ConnectorDefaultValue, ConnectorDropTableDataDisposition, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationOperationId, ConnectorPartitionTransform,
    ConnectorPropertyAuthority, ConnectorPropertyChange, ConnectorRefAction, ConnectorRefKind,
    ConnectorRefreshPublicationGuard, ConnectorSchemaChange, ConnectorStructField,
    ConnectorViewColumn, ConnectorViewDefinition, ConnectorViewDialect, ConnectorViewIdentity,
    CreateOrReplacePolicy, CreatePolicy, DropPolicy, ExternalMutationEffect,
    ExternalMutationEvidence, ExternalMutationFinalization, ExternalMutationOutcome,
    MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES,
};
pub use mv_storage_observation::{
    MAX_MV_OBSERVATION_FIELDS, MAX_MV_OBSERVATION_PARTITION_FIELDS, MAX_MV_OBSERVATION_REFS,
    MAX_MV_OBSERVATION_SNAPSHOTS, MvCreatedTargetObservation, MvExactPartitionField,
    MvExactPartitionTransform, MvMaintenanceMetadataObservation, MvObservedField,
    MvObservedMaintenancePolicy, MvObservedPartitionField, MvObservedPartitionSpec,
    MvObservedPartitionTransform, MvObservedRefreshMarker, MvObservedSnapshot,
    MvObservedSourceField, MvRefreshBaseObservation, MvRefreshTargetObservation,
    MvSchemaValidationObservation, MvSourceSchemaObservation, MvStorageObservationPort,
    UnavailableMvStorageObservationPort,
};
pub use original_result_scope::{
    ConnectorOriginalResultScope, ConnectorOriginalResultScopeCheck, OriginalResultCheckClass,
    OriginalResultCheckError,
};
pub use payload_retention::{ConnectorPayloadByteBudget, ConnectorPayloadRetentionGuard};
pub use predicate::{
    ConnectorPredicateDisposition, ConnectorPredicateDispositionKind, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateColumn, ConnectorStaticPredicateId,
    ConnectorStaticPredicateKind, MAX_CONNECTOR_STATIC_IN_LITERALS,
    MAX_CONNECTOR_STATIC_LITERAL_PAYLOAD_BYTES, MAX_CONNECTOR_STATIC_PREDICATES,
    MAX_CONNECTOR_STATIC_VARIABLE_LITERAL_BYTES, normalize_predicate_dispositions,
    validate_static_predicates,
};
pub use property::{
    DdlPropertyChange, DdlPropertyDeclaration, DdlPropertyDeclarationError, DdlPropertyType,
    DdlPropertyValue, InvalidTableProperty,
};
pub use provider::{
    CatalogActivityEvents, CatalogActivityPhase, CatalogActivityReader, CatalogActivitySnapshot,
    CatalogActivitySource, CatalogGenerationActivity, CatalogPhaseActivity, CatalogScopeIdentity,
    ProviderCatalogControlFactory, ProviderControlRoleFactory, ProviderExecutionRoleFactory,
    ProviderRoleDefinition, SealedProviderRoleRegistry,
};
pub use provider::{
    ProviderControlRoleFactory as ConnectorControlRoleBindingFactory,
    ProviderExecutionRoleFactory as ConnectorExecutionRoleBindingFactory,
};
pub use provider_binding::{ConnectorProviderBinding, ConnectorProviderBindingKey};
pub use publication::{
    LakePublicationDisposition, LakePublicationFamily, LakePublicationId,
    LakePublicationMarkerHeader, LakePublicationNextAction, LakePublicationStatementTag,
    LakePublicationTarget, LakePublicationTerminal,
};
pub use read::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorChangePartition,
    ConnectorChangePartitionField, ConnectorChangePartitionTransform,
    ConnectorChangePartitionValue, ConnectorChangeWindowAdmission,
    ConnectorChangeWindowFullRebuildReason, ConnectorChangeWindowPartitionImpact,
    ConnectorChangeWindowReplaceFailure, ConnectorReadPurpose, ConnectorReadSelector,
    ConnectorScan, ConnectorScanAdmission, ConnectorScanSelection, ConnectorSplitPlanningMetrics,
    ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
};
pub use read_session::{
    ConnectorReadSession, ConnectorReadSessionFinalizationContext, ConnectorReadSessionOutcome,
    SharedConnectorReadSession,
};
pub use resources::{
    ConnectorExecutionResources, ConnectorOutputMemoryToken, ConnectorReservationBackend,
    ConnectorResourceCheckpoint, ConnectorResourceClass, ConnectorResourceLedger,
    ConnectorResourceReservation,
};
pub use row_mutation::{
    CONNECTOR_ROW_MUTATION_CONTRACT_VERSION, ConnectorMutationEffectField,
    ConnectorMutationMatchContract, ConnectorMutationRouteInput, ConnectorMutationSourceField,
    ConnectorMutationTargetField, ConnectorRowMutationCurrent, ConnectorRowMutationEffect,
    ConnectorRowMutationIntent, ConnectorRowMutationPreparation,
    ConnectorRowMutationPreparationOutcome, ConnectorRowMutationPreparationRequest,
    ConnectorRowMutationStrategy, ConnectorWriteRouteId,
};
pub use scalar::{ConnectorScalarType, ConnectorScalarValue};
pub use scoped_begin::ConnectorScopedBeginCause;
pub use semantic_revision::{
    ConnectorCanonicalReadPoint, ConnectorExactSemanticRevision, ConnectorSemanticFact,
    MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES, MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES,
};
pub use staged_create::{
    CONNECTOR_CTAS_UNANCHORED_CLEANUP_CONTRACT_VERSION, CONNECTOR_STAGED_CREATE_CONTRACT_VERSION,
    ConnectorCtasUnanchoredCleanupOutcome, ConnectorCtasUnanchoredCleanupRequest,
    ConnectorCtasUnanchoredDiscoveryRequest, ConnectorCtasUnanchoredProvenance,
    ConnectorPreparedCreateDocumentTarget, ConnectorPreparedCreateFieldBinding,
    ConnectorStagedCreate, ConnectorStagedCreateAbortOutcome, ConnectorStagedCreateAbortRequest,
    ConnectorStagedCreateMode, ConnectorStagedCreateNeededCommitted,
    ConnectorStagedCreateOperationId, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePrepareRequest, ConnectorStagedCreatePublication,
    ConnectorStagedCreatePublicationAdjudicationOutcome,
    ConnectorStagedCreatePublicationAdjudicationRequest,
    ConnectorStagedCreatePublicationObservation, ConnectorStagedCreatePublicationPayload,
    ConnectorStagedCreatePublicationReceiving, ConnectorStagedCreatePublicationRecorder,
    ConnectorStagedCreatePublicationSource, ConnectorStagedCreatePublishOutcome,
    ConnectorStagedCreatePublishRequest, ConnectorStagedCreateReceipt,
    ConnectorStagedCreateReceiptPhase, ConnectorStagedCreateSession, ConnectorStagedCtasProperties,
    ConnectorStagedMvStorageProperties, ConnectorStagedTableHandle,
    ConnectorStagedWritePlanningBinding, ConnectorStagedWritePlanningRequest,
    ConnectorStagedWriteProof, ConnectorUnanchoredCtasCleanup,
    ConnectorUnanchoredCtasCleanupSession,
};
pub use statistics::{
    AsyncStatisticsCollectionSession, AsyncStatisticsCollectionStart, ConnectorStatistics,
    ConnectorStatisticsResolver, ConnectorStatisticsSession,
    MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES, MAX_CONNECTOR_STATISTICS_COLUMNS,
    MAX_CONNECTOR_STATISTICS_METRICS, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES, StatisticsArtifactDraft,
    StatisticsArtifactIdentity, StatisticsArtifactParts, StatisticsArtifactsCpuSource,
    StatisticsBasisRelation, StatisticsBindingCpuSource, StatisticsCollection,
    StatisticsCollectionSession, StatisticsCollectionStart, StatisticsCollectionStartRequest,
    StatisticsColumnSelection, StatisticsCpuInput, StatisticsCpuPort, StatisticsDataVersion,
    StatisticsEvidence, StatisticsEvidenceCpuSource, StatisticsEvidenceRevision,
    StatisticsInterval, StatisticsMetric, StatisticsMetricError, StatisticsMetricErrorKind,
    StatisticsMetricObservation, StatisticsMetricRequest, StatisticsMetricSource,
    StatisticsMetricState, StatisticsMetricValue, StatisticsMissing, StatisticsMissingKind,
    StatisticsNumericNature, StatisticsPublicationClose, StatisticsPublicationReady,
    StatisticsReadCpuSource, StatisticsReadRequest, StatisticsReader, StatisticsReceipt,
    StatisticsRequiredAggregation, StatisticsRowCoverage, StatisticsScanColumn,
    StatisticsSchemaCpuSource, StatisticsSessionFuture, StatisticsSummaryColumn,
    StatisticsSummaryColumnType, StatisticsSummaryReadOriginal, StatisticsSummaryReadResult,
    TableStatisticsSummary,
};
pub use view_metadata::{
    ConnectorListViewsRequest, ConnectorViewMetadata, ConnectorViewMetadataValue,
    ConnectorViewPreparation, ConnectorViewPreparationFuture, ConnectorViewRequest,
};
pub use write::{
    ConnectorCommittedPartitionField, ConnectorCommittedPartitioning,
    ConnectorManagedPartitionField, ConnectorManagedPartitionSpecObservation,
    ConnectorManagedPartitionSpecPreview, ConnectorManagedPartitionSpecPreviewRequest,
    ConnectorManagedPartitionSpecReplacement, ConnectorManagedPartitionSpecReplacementId,
    ConnectorManagedPartitionSpecReplacementTarget, ConnectorManagedPartitionTransform,
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
    ConnectorPreReadyWritePlanningProof, ConnectorPreReadyWritePlanningRequest, ConnectorWrite,
    ConnectorWriteAbortOutcome, ConnectorWriteActivationIntent, ConnectorWriteActivationRequest,
    ConnectorWriteActivationSource, ConnectorWriteAdmissionPurpose, ConnectorWriteBaseVersion,
    ConnectorWriteCohortId, ConnectorWriteControl, ConnectorWriteFieldBinding,
    ConnectorWriteFieldRequest, ConnectorWriteFieldToken, ConnectorWriteInputRequest,
    ConnectorWriteInputShape, ConnectorWriteIntent, ConnectorWriteOperationId,
    ConnectorWritePreparation, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
    ConnectorWriteReceipt, ConnectorWriteTargetRef,
    MAX_CONNECTOR_MANAGED_PARTITION_FIELD_TEXT_BYTES, MAX_CONNECTOR_MANAGED_PARTITION_SPEC_FIELDS,
    MAX_CONNECTOR_WRITE_RECEIPT_BYTES,
};

pub use process_local::ConnectorProcessLocalPayload;

mod provider_metadata_cpu;
pub use provider_metadata_cpu::*;
