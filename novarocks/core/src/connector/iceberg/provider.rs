// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Iceberg's provider-owned implementation of the connector metadata/read SPI.
//!
//! The JSON payloads below are deliberately private to this provider.  They
//! contain only catalog/table identity and a snapshot pin; core code transports
//! them as opaque bytes and never downcasts into Iceberg objects.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Instant;

use arrow::array::{Array, Int8Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use bytes::Bytes;
use novarocks_catalog::schema::SqlType;
use novarocks_connector_iceberg::iceberg::spec::{PrimitiveType, TableMetadata, Type};
use novarocks_connector_iceberg::iceberg::transaction::{ApplyTransactionAction, Transaction};
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorCatalogMutation,
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorColumnAggregation, ConnectorColumnDefinition, ConnectorCommittedVersion,
    ConnectorControlBinding, ConnectorDataType, ConnectorDefaultValue,
    ConnectorDropTableDataDisposition, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorListNamespacesRequest,
    ConnectorListTablesRequest, ConnectorListViewsRequest,
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationTechnique,
    ConnectorMetadata, ConnectorMutationEffectField, ConnectorMutationFailure,
    ConnectorMutationFailureKind, ConnectorMutationMatchContract, ConnectorMutationOperationId,
    ConnectorMutationRouteInput, ConnectorMutationSourceField, ConnectorMutationTargetField,
    ConnectorNamespaceRequest, ConnectorPartitionTransform, ConnectorPredicateDisposition,
    ConnectorPredicateDispositionKind, ConnectorPrepareSplitRequest, ConnectorProviderId,
    ConnectorReadNamedReference, ConnectorReadPurpose, ConnectorReadReferenceFacts,
    ConnectorReadReferenceFactsRequest, ConnectorReadReferenceKind, ConnectorReadSelector,
    ConnectorReadSnapshotLogEntry, ConnectorRefAction, ConnectorRefKind,
    ConnectorRefreshPublicationGuard, ConnectorRequestContext,
    ConnectorRowMutationActivationRequest, ConnectorRowMutationCohortRecipe,
    ConnectorRowMutationEffect, ConnectorRowMutationExecutionPlan, ConnectorRowMutationIntent,
    ConnectorRowMutationPreparation, ConnectorRowMutationPreparationOutcome,
    ConnectorRowMutationPreparationRequest, ConnectorRowMutationRoute,
    ConnectorRowMutationStrategy, ConnectorScalarType, ConnectorScalarValue, ConnectorScan,
    ConnectorScanHandle, ConnectorScanPlanning, ConnectorScanSelection,
    ConnectorSealedWriteCohortSet, ConnectorSplit, ConnectorSplitPlanningMetrics,
    ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
    ConnectorStagedPublicationBaseFact, ConnectorStagedPublicationCleanupReceipt,
    ConnectorStagedPublicationCleanupRequest, ConnectorStagedPublicationDescriptor,
    ConnectorStagedPublicationDisposition, ConnectorStagedPublicationObservation,
    ConnectorStagedPublicationProof, ConnectorStagedPublicationRecovery,
    ConnectorStaticComparisonOp, ConnectorStaticPredicate, ConnectorStaticPredicateKind,
    ConnectorStatistics, ConnectorTableColumnRole, ConnectorTableColumnSemanticKind,
    ConnectorTableColumnVisibility, ConnectorTableHandle, ConnectorTableMetadata,
    ConnectorTablePlanningFacts, ConnectorTableRequest, ConnectorTableResolution,
    ConnectorViewDefinition, ConnectorViewDialect, ConnectorViewIdentity, ConnectorViewMetadata,
    ConnectorViewMetadataValue, ConnectorViewRequest, ConnectorWriteActivationIntent,
    ConnectorWriteActivationRequest, ConnectorWriteActivationSource,
    ConnectorWriteAdmissionPurpose, ConnectorWriteBaseVersion, ConnectorWriteCohortDescriptor,
    ConnectorWriteCohortId, ConnectorWriteFieldBinding, ConnectorWriteFieldRequest,
    ConnectorWriteFieldToken, ConnectorWriteInputRequest, ConnectorWriteInputShape,
    ConnectorWriteIntent, ConnectorWriteLease, ConnectorWriteOperationId,
    ConnectorWritePreparation, ConnectorWritePreparationOutcome, ConnectorWritePreparationRequest,
    ConnectorWriteRouteId, ConnectorWriteTargetRef, CreateOrReplacePolicy, CreatePolicy,
    DropPolicy, ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome, StatisticsAccuracy, StatisticsCollection, StatisticsCollectionPlan,
    StatisticsCollectionRequest, StatisticsCoverage, StatisticsDataVersion, StatisticsEvidence,
    StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricState, StatisticsMetricValue,
    StatisticsMissing, StatisticsMissingKind, StatisticsProvenance,
    StatisticsPublishPreparationRequest, StatisticsPublishRequest, StatisticsReadRequest,
    StatisticsReader, StatisticsReceipt, StatisticsReconcileRequest, StatisticsScanColumn,
    normalize_predicate_dispositions, validate_static_predicates,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::catalog::IcebergCatalogEntry;
use super::catalog::registry::{
    IcebergCatalogRegistry, create_namespace, create_table, drop_namespace, drop_table,
    extract_data_files_with_stats_at, list_namespaces, list_tables, load_table, namespace_exists,
};
use super::catalog::views;
use super::cleanup_maintenance::IcebergCleanupMaintenanceAdapter;
use super::data_mutation::IcebergDataMutationAdapter;
use super::metadata_maintenance::IcebergMetadataMaintenanceAdapter;
use super::write_contract::{
    IcebergWriteFileCompression, IcebergWriteSinkMode, IcebergWriteSinkSpec,
    encode_data_sink_spec_handle_payload, encode_equality_delete_sink_spec_handle_payload,
    encode_position_delete_sink_handle_payload,
};
use super::write_control::{
    IcebergRowMutationActivationFactory, IcebergRowMutationPreparationFactory,
    IcebergWriteControlAdapter, IcebergWritePreparationFactory,
};
use super::write_service::{
    IcebergFirstRefreshWriteReportCommitter, IcebergMvPrimaryEmptyInputPolicy,
    IcebergWriteCohortContext, IcebergWriteControlService, IcebergWriteControlServiceContext,
    IcebergWriteReportCommitter, IcebergWriteServiceRegistry, RegisteredIcebergWriteControlBackend,
};
use crate::connector::backend::ResolvedTableStatisticsPin;
use crate::sql::optimizer::stats_input::{StatValue, StatsMissingReason};
use novarocks_connector_iceberg::delta::IcebergDeltaSplitPayload;
use novarocks_connector_iceberg::execution_declaration::IcebergInstanceDistribution;
use novarocks_connector_iceberg::file_reader::distributed_rewrite_reader::{
    ICEBERG_REWRITE_POSITION_SPLIT_V1, IcebergRewritePositionSplitPayloadV1,
};
use novarocks_connector_iceberg::file_reader::execution_payload::{
    ICEBERG_SPLIT_V5, IcebergFrozenScanUnitPayload, IcebergMetadataSplitPayloadV1,
    IcebergScanFactColumnV1, SplitPayload, canonical_split_name_mapping,
    materialize_local_scan_units, scan_fact_scalar_type,
};
use novarocks_connector_iceberg::reconcile_payload::{
    ICEBERG_MUTATION_EVIDENCE_VERSION, ICEBERG_STAGED_PUBLICATION_PROOF_VERSION,
    ICEBERG_STATISTICS_EVIDENCE_VERSION, IcebergMutationEvidenceTarget, IcebergMutationEvidenceV1,
    IcebergStagedPublicationProofV1, IcebergStatisticsEvidenceV1, decode_mutation_evidence,
    decode_staged_publication_proof, decode_statistics_evidence, encode_mutation_evidence,
    encode_staged_publication_proof, encode_statistics_evidence,
};
use novarocks_connector_iceberg::row_lineage_synth as iceberg_row_lineage;
use novarocks_connector_iceberg::scan_model::{
    IcebergDataFileBinding, IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat,
    IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
    IcebergPhysicalPredicateValue, IcebergTableInfo,
};
#[cfg(test)]
use novarocks_connector_iceberg::scan_model::{IcebergSchemaDef, IcebergSchemaFieldDef};
use novarocks_connector_iceberg::schema_facts::{iceberg_schema_def, row_lineage_enabled};
use novarocks_connector_iceberg::statistics_codec::{
    decode_provider_statistics, encode_provider_statistics, statistics_data_version,
    statistics_metric_column,
};
use novarocks_connector_iceberg::write_payload::{
    IcebergFirstRefreshWritePlanPayloadV2, IcebergWritePlanPayloadV1,
};

const PROVIDER_ID: &str = "iceberg";
const MAX_CACHED_SNAPSHOT_MEMBERSHIPS: usize = 64;
const ICEBERG_STATISTICS_OPERATION_KIND: &str = "statistics-publish";

/// Build the physical layout from the metadata serialized into the resolved
/// table handle.  In particular, this must not call `load_table`: durable
/// ANALYZE consumes the original snapshot/schema pin even if latest metadata
/// has advanced while a job was waiting for its worker lease.
fn statistics_scan_layout(
    table: &TablePayload,
    projection: &[usize],
) -> Result<Vec<StatisticsScanColumn>, ConnectorError> {
    let table_info = table.table_info.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg statistics collection requires a resolved base table payload",
        )
    })?;
    let serialized = table_info.serialized_metadata.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg statistics collection payload is missing serialized pinned metadata",
        )
    })?;
    let metadata: novarocks_connector_iceberg::iceberg::spec::TableMetadata =
        serde_json::from_str(serialized).map_err(|e| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode pinned Iceberg statistics metadata: {e}"),
            )
        })?;
    if metadata.current_schema_id() != table_info.schema_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg statistics collection metadata does not match its pinned schema ID",
        ));
    }
    let schema = novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema(
        metadata.current_schema(),
    )
    .map_err(|e| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("convert pinned Iceberg statistics schema to Arrow: {e}"),
        )
    })?;
    projection
        .iter()
        .map(|&ordinal| {
            let field = schema.fields().get(ordinal).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg statistics metric projection index {ordinal} is outside the pinned schema"
                    ),
                )
            })?;
            // Arrow field metadata can carry provider implementation details;
            // the cross-layer statistics layout intentionally retains only the
            // typed scan contract needed by the internal sink.
            let data_type = match table
                .logical_type_columns
                .get(&field.name().to_ascii_lowercase())
                .map(String::as_str)
            {
                Some("bitmap") | Some("hll") => DataType::Binary,
                _ => field.data_type().clone(),
            };
            StatisticsScanColumn::try_new(
                ordinal,
                Arc::<str>::from(field.name().as_str()),
                data_type,
                field.is_nullable(),
            )
        })
        .collect()
}

use novarocks_connector_iceberg::access_binding::IcebergReadBinding;

#[derive(Clone)]
pub(crate) struct IcebergControlProvider {
    descriptor: ConnectorInstanceDescriptor,
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
    binding_key: ConnectorExecutionBindingKey,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
    /// FE-local filesystem capability used only to refine already-authorized
    /// files into frozen row-group units. It is injected by composition;
    /// absent legacy construction fails closed rather than discovering a
    /// process-global Tokio runtime or credentials.
    planning_binding: Option<IcebergReadBinding>,
    snapshot_memberships: Arc<SnapshotMembershipCache>,
    recovery_cleanup_outcomes:
        Arc<Mutex<HashMap<ConnectorMutationOperationId, IcebergRecoveryCleanupRecord>>>,
}

#[derive(Clone)]
struct IcebergRecoveryCleanupRecord {
    outcome: ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>,
    proof: IcebergStagedPublicationProofV1,
    descriptor_digest: [u8; 32],
    observation_digest: [u8; 32],
}

impl IcebergControlProvider {
    /// Creates the FE-only control binding for a logical Iceberg catalog. The
    /// implementation remains in core until SPI-5, but its runtime capability
    /// aggregate no longer needs to cross into a BE process.
    pub(crate) fn new_control(
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
    ) -> Result<ConnectorControlBinding, ConnectorError> {
        Self::new_control_with_planning_binding(instance_id, registry, None)
    }

    pub(crate) fn new_control_with_planning_binding(
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
        planning_binding: Option<IcebergReadBinding>,
    ) -> Result<ConnectorControlBinding, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse(PROVIDER_ID)?,
            instance_id: instance_id.clone(),
        };
        let incarnation = ConnectorInstanceIncarnation::new();
        let (control_entry, services) = {
            let registry = registry.read().map_err(|error| {
                internal(format!("Iceberg catalog registry read lock: {error}"))
            })?;
            (
                registry.get(instance_id.as_str()).map_err(internal)?,
                registry.write_services(),
            )
        };
        let staged_create_supported = matches!(
            control_entry.kind,
            novarocks_connector_iceberg::catalog_config::IcebergCatalogKind::Rest
        );
        let write_key = ConnectorExecutionBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let provider = Arc::new(Self {
            descriptor: descriptor.clone(),
            instance_id,
            incarnation,
            binding_key: write_key.clone(),
            registry: Arc::clone(&registry),
            planning_binding,
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
            recovery_cleanup_outcomes: Arc::new(Mutex::new(HashMap::new())),
        });
        let write =
            Arc::new(
                IcebergWriteControlAdapter::new_with_preparation(
                    write_key.clone(),
                    Arc::new(
                        RegisteredIcebergWriteControlBackend::new(services.clone())
                            .with_control_registry(Arc::clone(&registry)),
                    ),
                    Arc::new(prepare_iceberg_write) as Arc<IcebergWritePreparationFactory>,
                )?
                .with_row_mutation_preparation(Arc::new(prepare_iceberg_row_mutation)
                    as Arc<IcebergRowMutationPreparationFactory>)
                .with_row_mutation_activation(Arc::new(activate_iceberg_row_mutation)
                    as Arc<IcebergRowMutationActivationFactory>),
            );
        let data_mutation = Arc::new(IcebergDataMutationAdapter::new_registered(
            write_key.clone(),
            descriptor.instance_id.clone(),
            Arc::clone(&registry),
        )?);
        let distributed_rewrite = Arc::new(
            super::distributed_rewrite::IcebergDistributedRewriteAdapter::new_registered(
                write_key.clone(),
                descriptor.instance_id.clone(),
                Arc::clone(&registry),
                services.clone(),
            )?,
        );
        let metadata_maintenance = Arc::new(IcebergMetadataMaintenanceAdapter::new_registered(
            write_key.clone(),
            descriptor.instance_id.clone(),
            Arc::clone(&registry),
        )?);
        let cleanup_maintenance = Arc::new(IcebergCleanupMaintenanceAdapter::new_registered(
            write_key,
            descriptor.instance_id.clone(),
            Arc::clone(&registry),
        )?);
        let staged_create: Option<Arc<dyn novarocks_spi::connector::ConnectorStagedCreate>> =
            staged_create_supported.then(|| {
                Arc::new(super::staged_create::IcebergStagedCreateAdapter::new(
                    descriptor.clone(),
                    incarnation,
                    control_entry,
                    services,
                )) as Arc<dyn novarocks_spi::connector::ConnectorStagedCreate>
            });
        ConnectorControlBinding::try_new_with_all_maintenance_capabilities_cleanup_and_staged_create(
            descriptor.clone(),
            incarnation,
            provider.clone(),
            provider.clone(),
            Arc::new(IcebergInstanceDistribution::new(descriptor, incarnation)),
            Some(provider.clone()),
            Some(data_mutation),
            Some(metadata_maintenance),
            Some(distributed_rewrite),
            Some(cleanup_maintenance),
            staged_create,
            Some(write),
            Some(provider.clone()),
        )?
        .try_with_staged_publication_recovery(Some(provider.clone()))?
        .try_with_view_metadata(Some(provider))
    }

    fn entry(&self, catalog: &str) -> Result<IcebergCatalogEntry, ConnectorError> {
        self.registry
            .read()
            .map_err(|error| internal(format!("iceberg catalog registry read lock: {error}")))?
            .get(catalog)
            .map_err(map_iceberg_error)
            .map_err(ConnectorError::with_retryable_before_progress)
    }

    fn validate_context(
        &self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn mutation_evidence(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        operation: &ConnectorCatalogMutationOperation,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        use novarocks_spi::connector::ConnectorRefAction;

        let table_before = |namespace: &str,
                            table: &str|
         -> Result<Option<(String, Option<String>)>, ConnectorError> {
            let entry = self.entry(self.instance_id.as_str())?;
            let Some(loaded) = load_existing_table_for_reconcile(&entry, namespace, table)? else {
                return Ok(None);
            };
            Ok(Some((
                loaded.table.metadata().uuid().to_string(),
                loaded.table.metadata_location().map(ToString::to_string),
            )))
        };

        let target = match operation {
            ConnectorCatalogMutationOperation::CreateNamespace { namespace, .. } => {
                IcebergMutationEvidenceTarget::Namespace {
                    namespace: namespace.namespace.to_string(),
                    should_exist: true,
                }
            }
            ConnectorCatalogMutationOperation::DropNamespace { namespace, .. } => {
                IcebergMutationEvidenceTarget::Namespace {
                    namespace: namespace.namespace.to_string(),
                    should_exist: false,
                }
            }
            ConnectorCatalogMutationOperation::CreateTable { table, .. } => {
                IcebergMutationEvidenceTarget::Table {
                    namespace: table.namespace.to_string(),
                    table: table.table.to_string(),
                    should_exist: true,
                    before_uuid: table_before(&table.namespace, &table.table)?
                        .map(|(uuid, _)| uuid),
                }
            }
            ConnectorCatalogMutationOperation::BootstrapEmptyTableSnapshot { .. } => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "empty-table bootstrap must build provider-specific evidence",
                ));
            }
            ConnectorCatalogMutationOperation::DropTable { table, .. } => {
                IcebergMutationEvidenceTarget::Table {
                    namespace: table.namespace.to_string(),
                    table: table.table.to_string(),
                    should_exist: false,
                    before_uuid: table_before(&table.namespace, &table.table)?
                        .map(|(uuid, _)| uuid),
                }
            }
            ConnectorCatalogMutationOperation::CreateView { view, .. } => {
                IcebergMutationEvidenceTarget::View {
                    namespace: view.namespace.to_string(),
                    view: view.view.to_string(),
                    should_exist: true,
                }
            }
            ConnectorCatalogMutationOperation::DropView { view, .. } => {
                IcebergMutationEvidenceTarget::View {
                    namespace: view.namespace.to_string(),
                    view: view.view.to_string(),
                    should_exist: false,
                }
            }
            ConnectorCatalogMutationOperation::AlterSchema { table, .. }
            | ConnectorCatalogMutationOperation::AlterPartitionSpec { table, .. }
            | ConnectorCatalogMutationOperation::AlterProperties { table, .. } => {
                let Some((table_uuid, before_metadata_location)) =
                    table_before(&table.namespace, &table.table)?
                else {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "Iceberg table does not exist for catalog mutation evidence",
                    ));
                };
                IcebergMutationEvidenceTarget::TableVersion {
                    namespace: table.namespace.to_string(),
                    table: table.table.to_string(),
                    table_uuid,
                    before_metadata_location,
                }
            }
            ConnectorCatalogMutationOperation::AlterRef { table, action } => {
                let Some((table_uuid, _)) = table_before(&table.namespace, &table.table)? else {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "Iceberg table does not exist for ref mutation evidence",
                    ));
                };
                let entry = self.entry(self.instance_id.as_str())?;
                let loaded = load_table(&entry, &table.namespace, &table.table)
                    .map_err(map_iceberg_error)?;
                let (ref_name, expected_snapshot_id) = match action {
                    ConnectorRefAction::Create {
                        name, snapshot_id, ..
                    } => (
                        name.to_string(),
                        snapshot_id.or_else(|| loaded.table.metadata().current_snapshot_id()),
                    ),
                    ConnectorRefAction::Drop { name, .. } => (name.to_string(), None),
                    ConnectorRefAction::FastForwardBranch {
                        target_branch,
                        committed_version,
                        ..
                    } => (
                        target_branch.to_string(),
                        Some(source_snapshot_id_from_committed_version(
                            committed_version,
                        )?),
                    ),
                };
                IcebergMutationEvidenceTarget::Ref {
                    namespace: table.namespace.to_string(),
                    table: table.table.to_string(),
                    table_uuid,
                    ref_name,
                    expected_snapshot_id,
                }
            }
        };
        let payload = encode_mutation_evidence(&IcebergMutationEvidenceV1 {
            version: ICEBERG_MUTATION_EVIDENCE_VERSION,
            target,
        })
        .map_err(internal)?;
        ExternalMutationEvidence::try_new(
            ICEBERG_MUTATION_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            operation_id,
            operation.kind(),
            Bytes::from(payload),
        )
    }

    fn guarded_fast_forward_evidence(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        table: &novarocks_spi::connector::ConnectorTableIdentity,
        source_branch: &str,
        target_branch: &str,
        source_snapshot_id: i64,
        expected_target_snapshot_id: Option<i64>,
        guard: &ConnectorRefreshPublicationGuard,
        loaded: &super::catalog::IcebergLoadedTable,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let target = IcebergMutationEvidenceTarget::GuardedFastForward {
            namespace: table.namespace.to_string(),
            table: table.table.to_string(),
            table_uuid: loaded.table.metadata().uuid().to_string(),
            before_metadata_location: loaded.table.metadata_location().map(ToString::to_string),
            source_branch: source_branch.to_string(),
            target_branch: target_branch.to_string(),
            source_snapshot_id,
            expected_target_snapshot_id,
            guard_digest: guard.digest(),
        };
        let payload = encode_mutation_evidence(&IcebergMutationEvidenceV1 {
            version: ICEBERG_MUTATION_EVIDENCE_VERSION,
            target,
        })
        .map_err(internal)?;
        ExternalMutationEvidence::try_new(
            ICEBERG_MUTATION_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            operation_id,
            "alter-ref",
            Bytes::from(payload),
        )
    }

    fn bootstrap_empty_table_snapshot_evidence(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        table: &novarocks_spi::connector::ConnectorTableIdentity,
        loaded: &super::catalog::IcebergLoadedTable,
        operation_marker: &str,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let target = IcebergMutationEvidenceTarget::BootstrapEmptyTableSnapshot {
            namespace: table.namespace.to_string(),
            table: table.table.to_string(),
            table_uuid: loaded.table.metadata().uuid().to_string(),
            operation_marker: operation_marker.to_string(),
        };
        let payload = encode_mutation_evidence(&IcebergMutationEvidenceV1 {
            version: ICEBERG_MUTATION_EVIDENCE_VERSION,
            target,
        })
        .map_err(internal)?;
        ExternalMutationEvidence::try_new(
            ICEBERG_MUTATION_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            operation_id,
            "bootstrap-empty-table-snapshot",
            Bytes::from(payload),
        )
    }

    fn execute_bootstrap_empty_table_snapshot(
        &self,
        request: &ConnectorCatalogMutationRequest,
        table: &novarocks_spi::connector::ConnectorTableIdentity,
        expected_current_snapshot: Option<i64>,
        properties: &[(Arc<str>, Arc<str>)],
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        const OPERATION_MARKER: &str = "novarocks.bootstrap.empty.operation-id";
        if table.instance_id != self.instance_id {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "bootstrap mutation belongs to another connector instance",
            )));
        }
        if expected_current_snapshot.is_some() {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "empty-table bootstrap requires expected_current_snapshot to be absent",
            )));
        }
        let operation_marker = hex::encode(request.operation_id.to_bytes());
        let mut snapshot_properties = BTreeMap::new();
        for (key, value) in properties {
            if key.is_empty()
                || value.len() > 4096
                || key.len() > 1024
                || key.as_ref() == OPERATION_MARKER
                || snapshot_properties
                    .insert(key.to_string(), value.to_string())
                    .is_some()
            {
                return Ok(known_uncommitted(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "invalid or duplicate empty-table bootstrap snapshot property",
                )));
            }
        }
        if snapshot_properties.is_empty()
            || snapshot_properties
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>()
                > novarocks_spi::connector::MAX_EXTERNAL_MUTATION_EVIDENCE_BYTES
        {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "empty-table bootstrap snapshot properties are empty or exceed the bounded limit",
            )));
        }
        snapshot_properties.insert(OPERATION_MARKER.to_string(), operation_marker.clone());
        let entry = match self.entry(self.instance_id.as_str()) {
            Ok(entry) => entry,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let loaded = match load_existing_table_for_reconcile(&entry, &table.namespace, &table.table)
        {
            Ok(Some(loaded)) => loaded,
            Ok(None) => {
                return Ok(known_uncommitted(ConnectorError::new(
                    ConnectorErrorKind::NotFound,
                    "Iceberg table does not exist for empty-table bootstrap",
                )));
            }
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let metadata = loaded.table.metadata();
        if let Some(snapshot) = metadata.current_snapshot() {
            let existing_marker = snapshot
                .summary()
                .additional_properties
                .get(OPERATION_MARKER)
                .map(String::as_str);
            if existing_marker == Some(operation_marker.as_str()) {
                return Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::NoOp,
                    receipt: self.receipt(
                        request.operation_id,
                        request.operation.kind(),
                        Some(provider_version(loaded.table.metadata_location())),
                    )?,
                    finalization: ExternalMutationFinalization::Complete,
                });
            }
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "empty-table bootstrap requires a target without a current snapshot",
            )));
        }
        let evidence = self.bootstrap_empty_table_snapshot_evidence(
            request.operation_id,
            table,
            &loaded,
            &operation_marker,
        )?;
        if let Err(error) = self.validate_context(&request.context) {
            return Ok(known_uncommitted(error));
        }
        let catalog = match super::catalog::registry::build_iceberg_catalog(&entry)
            .map_err(map_iceberg_error)
        {
            Ok(catalog) => catalog,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let ident = novarocks_connector_iceberg::iceberg::TableIdent::from_strs([
            table.namespace.as_ref(),
            table.table.as_ref(),
        ])
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("invalid bootstrap table identity: {error}"),
            )
        })?;
        let commit_result = super::catalog::registry::block_on_iceberg(async {
            let current = catalog.load_table(&ident).await?;
            if current.metadata().current_snapshot().is_some() {
                return Err(novarocks_connector_iceberg::iceberg::Error::new(
                    novarocks_connector_iceberg::iceberg::ErrorKind::PreconditionFailed,
                    "empty-table bootstrap target gained a snapshot before commit",
                ));
            }
            let tx = Transaction::new(&current);
            let action = tx
                .fast_append()
                .set_snapshot_properties(snapshot_properties.into_iter().collect())
                .set_commit_uuid(uuid::Uuid::new_v4());
            let tx = action.apply(tx)?;
            tx.commit(catalog.as_ref()).await
        })
        .map_err(map_iceberg_error)
        .and_then(|result| result.map_err(|error| map_iceberg_error(error.to_string())));
        let committed = match commit_result {
            Ok(committed) => committed,
            Err(error) if mutation_commit_may_be_unknown(error.kind()) => {
                return Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        mutation_failure_kind(error.kind()),
                        error.to_string(),
                    ),
                    evidence,
                });
            }
            Err(error) => return Ok(known_uncommitted(error)),
        };
        entry.invalidate_table_cache(&table.namespace, &table.table);
        let finalized = load_existing_table_for_reconcile(&entry, &table.namespace, &table.table);
        let finalization = match finalized {
            Ok(Some(current)) => match current.table.metadata().current_snapshot() {
                Some(snapshot)
                    if snapshot
                        .summary()
                        .additional_properties
                        .get(OPERATION_MARKER)
                        .map(String::as_str)
                        == Some(operation_marker.as_str())
                        && snapshot.parent_snapshot_id().is_none() =>
                {
                    ExternalMutationFinalization::Complete
                }
                _ => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg empty-table bootstrap postcondition does not match its operation marker",
                )),
            },
            Ok(None) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::NotFound,
                "Iceberg table disappeared after empty-table bootstrap",
            )),
            Err(error) => ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                mutation_failure_kind(error.kind()),
                error.to_string(),
            )),
        };
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: self.receipt(
                request.operation_id,
                request.operation.kind(),
                Some(provider_version(committed.metadata_location())),
            )?,
            finalization,
        })
    }

    fn execute_guarded_fast_forward(
        &self,
        request: &ConnectorCatalogMutationRequest,
        table: &novarocks_spi::connector::ConnectorTableIdentity,
        action: &novarocks_spi::connector::ConnectorRefAction,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        let novarocks_spi::connector::ConnectorRefAction::FastForwardBranch {
            source_branch,
            target_branch,
            committed_version,
            expected_target_snapshot_id,
            guard,
        } = action
        else {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "guarded fast-forward requires the internal publication action",
            ));
        };
        if table.instance_id != self.instance_id {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "ref mutation belongs to another connector instance",
            )));
        }
        let source_snapshot_id = match source_snapshot_id_from_committed_version(committed_version)
        {
            Ok(snapshot_id) => snapshot_id,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let prepared = (|| -> Result<_, ConnectorError> {
            if source_branch.eq_ignore_ascii_case("main")
                || !target_branch.eq_ignore_ascii_case("main")
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "MV guarded fast-forward must publish a named staging branch to main",
                ));
            }
            let entry = self.entry(self.instance_id.as_str())?;
            let loaded = load_existing_table_for_reconcile(&entry, &table.namespace, &table.table)?
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "Iceberg table does not exist for MV publication",
                    )
                })?;
            let evidence = self.guarded_fast_forward_evidence(
                request.operation_id,
                table,
                source_branch,
                target_branch,
                source_snapshot_id,
                *expected_target_snapshot_id,
                guard,
                &loaded,
            )?;
            let commit = build_guarded_fast_forward_commit(
                &table.namespace,
                &table.table,
                loaded.table.metadata(),
                source_branch,
                target_branch,
                source_snapshot_id,
                *expected_target_snapshot_id,
                guard,
            )?;
            Ok((
                entry,
                evidence,
                commit,
                provider_version(loaded.table.metadata_location()),
            ))
        })();
        let (entry, evidence, commit, before_version) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        if let Err(error) = self.validate_context(&request.context) {
            return Ok(known_uncommitted(error));
        }
        let catalog = match super::catalog::registry::build_iceberg_catalog(&entry)
            .map_err(map_iceberg_error)
        {
            Ok(catalog) => catalog,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        if let Err(error) =
            super::catalog::registry::block_on_iceberg(async { catalog.update_table(commit).await })
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        format!("commit guarded Iceberg MV publication: {error}"),
                    )
                })
        {
            return if mutation_commit_may_be_unknown(error.kind()) {
                Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        mutation_failure_kind(error.kind()),
                        error.to_string(),
                    ),
                    evidence,
                })
            } else {
                Ok(known_uncommitted(error))
            };
        }
        let receipt = self.receipt(
            request.operation_id,
            request.operation.kind(),
            Some(before_version),
        )?;
        match self.validate_context(&request.context).and_then(|()| {
            load_existing_table_for_reconcile(&entry, &table.namespace, &table.table)?.ok_or_else(
                || {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        "Iceberg table disappeared after guarded MV publication",
                    )
                },
            )
        }) {
            Ok(finalized)
                if finalized.table.metadata().current_snapshot_id() == Some(source_snapshot_id) =>
            {
                Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt,
                    finalization: ExternalMutationFinalization::Complete,
                })
            }
            Ok(_) => Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt,
                finalization: ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg main ref changed before guarded MV publication finalization",
                )),
            }),
            Err(error) => Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt,
                finalization: ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
                    mutation_failure_kind(error.kind()),
                    error.to_string(),
                )),
            }),
        }
    }

    fn receipt(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        operation_kind: &str,
        provider_version: Option<Bytes>,
    ) -> Result<ConnectorCatalogMutationReceipt, ConnectorError> {
        ConnectorCatalogMutationReceipt::try_new(
            self.descriptor.clone(),
            self.incarnation,
            operation_id,
            operation_kind,
            provider_version,
        )
    }

    fn statistics_evidence(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        table: &TablePayload,
        data_version: &StatisticsDataVersion,
        statistics_path: &str,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        let payload = encode_statistics_evidence(&IcebergStatisticsEvidenceV1 {
            version: ICEBERG_STATISTICS_EVIDENCE_VERSION,
            namespace: table.namespace.clone(),
            table: table.table.clone(),
            data_version: data_version.as_bytes().to_vec(),
            statistics_path: statistics_path.to_string(),
        })
        .map_err(internal)?;
        ExternalMutationEvidence::try_new(
            ICEBERG_STATISTICS_EVIDENCE_VERSION,
            self.descriptor.clone(),
            self.incarnation,
            operation_id,
            ICEBERG_STATISTICS_OPERATION_KIND,
            Bytes::from(payload),
        )
    }

    fn statistics_receipt(
        &self,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
        data_version: StatisticsDataVersion,
        evidence_revision: StatisticsEvidenceRevision,
        provider_payload: Bytes,
        effect: ExternalMutationEffect,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        Ok(ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt: StatisticsReceipt::try_new(
                self.descriptor.clone(),
                self.incarnation,
                operation_id,
                data_version,
                evidence_revision,
                provider_payload,
            )?,
            finalization: ExternalMutationFinalization::Complete,
        })
    }

    fn table_payload(&self, table: &ConnectorTableHandle) -> Result<TablePayload, ConnectorError> {
        ensure_owner(table.owner(), &self.instance_id)?;
        decode_payload(table.payload(), "table handle")
    }

    fn hydrate_frozen_rewrite_source(
        &self,
        entry: &IcebergCatalogEntry,
        table: &mut TablePayload,
    ) -> Result<(), ConnectorError> {
        let Some(frozen) = table.frozen_rewrite.as_ref() else {
            return Ok(());
        };
        if frozen.version != 1
            || !matches!(
                frozen.operation_kind.as_str(),
                novarocks_spi::connector::REWRITE_DATA_FILES_KIND
                    | novarocks_spi::connector::REWRITE_POSITION_DELETES_KIND
            )
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg frozen rewrite source is invalid",
            ));
        }
        let loaded =
            load_table(entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let group = super::distributed_rewrite::load_frozen_rewrite_group(
            loaded.table.file_io(),
            &frozen.group,
        )?;
        table.prepared_files = group.data_files.clone();
        table.explicit_files = Some(group.data_files);
        Ok(())
    }

    fn scan_payload(&self, scan: &ConnectorScanHandle) -> Result<ScanPayload, ConnectorError> {
        ensure_owner(scan.owner(), &self.instance_id)?;
        decode_payload(scan.payload(), "scan handle")
    }

    fn schema_for(
        &self,
        entry: &IcebergCatalogEntry,
        namespace: &str,
        table: &str,
        metadata_columns: &[String],
        projection: &[usize],
    ) -> Result<SchemaRef, ConnectorError> {
        let loaded = load_table(entry, namespace, table).map_err(map_iceberg_error)?;
        let storage_schema = novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema(
            loaded.table.metadata().current_schema(),
        )
        .map_err(|error| internal(format!("convert Iceberg schema to Arrow: {error}")))?;
        let mut storage_fields = storage_schema.fields().to_vec();
        storage_fields.extend(iceberg_metadata_arrow_fields(metadata_columns)?);
        let indexes = if projection.is_empty() {
            (0..storage_fields.len()).collect::<Vec<_>>()
        } else {
            projection.to_vec()
        };
        let fields = indexes
            .into_iter()
            .map(|index| {
                let storage_field = storage_fields.get(index).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg projection index {index} is outside the table schema"),
                    )
                })?;
                if is_iceberg_metadata_column(storage_field.name()) {
                    return Ok(storage_field.clone());
                }
                let logical_column = loaded
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(storage_field.name()))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            format!(
                                "Iceberg table schema field {} is missing its logical column definition",
                                storage_field.name()
                            ),
                        )
                    })?;
                Ok(Arc::new(
                    storage_field
                        .as_ref()
                        .clone()
                        .with_data_type(logical_column.data_type.clone()),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let columns = fields
            .iter()
            .map(|field| super::schema::IcebergArrowColumn {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            })
            .collect::<Vec<_>>();
        let schema = super::schema::build_projected_output_schema_from_scan_model(
            &iceberg_schema_def(loaded.table.metadata().current_schema()),
            &columns,
        )
        .map_err(|error| internal(format!("build Iceberg projected schema: {error}")))?;
        preserve_hidden_sql_field_metadata(schema, &fields)
    }

    fn frozen_data_rewrite_schema(
        &self,
        entry: &IcebergCatalogEntry,
        namespace: &str,
        table: &str,
        projection: &[usize],
    ) -> Result<SchemaRef, ConnectorError> {
        let loaded = load_table(entry, namespace, table).map_err(map_iceberg_error)?;
        let storage_schema = novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema(
            loaded.table.metadata().current_schema(),
        )
        .map_err(|error| internal(format!("convert Iceberg schema to Arrow: {error}")))?;
        let mut storage_fields = storage_schema.fields().to_vec();
        if row_lineage_enabled(loaded.table.metadata()) {
            storage_fields.extend([
                Arc::new(Field::new(
                    iceberg_row_lineage::ICEBERG_ROW_ID_COL,
                    DataType::Int64,
                    false,
                )),
                Arc::new(Field::new(
                    iceberg_row_lineage::ICEBERG_LAST_UPDATED_SEQ_COL,
                    DataType::Int64,
                    true,
                )),
            ]);
        }
        let indexes = if projection.is_empty() {
            (0..storage_fields.len()).collect::<Vec<_>>()
        } else {
            projection.to_vec()
        };
        let fields = indexes
            .into_iter()
            .map(|index| {
                let storage_field = storage_fields.get(index).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg rewrite projection index {index} is outside the table schema"),
                    )
                })?;
                if iceberg_row_lineage::is_iceberg_row_id(storage_field.name())
                    || iceberg_row_lineage::is_iceberg_last_updated_sequence_number(
                        storage_field.name(),
                    )
                {
                    return Ok(storage_field.clone());
                }
                let logical_column = loaded
                    .columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(storage_field.name()))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            format!(
                                "Iceberg table schema field {} is missing its logical column definition",
                                storage_field.name()
                            ),
                        )
                    })?;
                Ok(Arc::new(
                    storage_field
                        .as_ref()
                        .clone()
                        .with_data_type(logical_column.data_type.clone()),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    fn select_snapshot(
        &self,
        entry: &IcebergCatalogEntry,
        table: &TablePayload,
        selector: ConnectorReadSelector,
    ) -> Result<(Option<i64>, String), ConnectorError> {
        let loaded =
            load_table(entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let metadata = loaded.table.metadata();
        let snapshot_id = match selector {
            ConnectorReadSelector::Current => Ok(metadata.current_snapshot_id()),
            ConnectorReadSelector::SnapshotId(snapshot_id) => metadata
                .snapshot_by_id(snapshot_id)
                .map(|_| Some(snapshot_id))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        format!("Iceberg snapshot {snapshot_id} was not found"),
                    )
                }),
            ConnectorReadSelector::TimestampMicros(timestamp_micros) => metadata
                .snapshots()
                .filter(|snapshot| {
                    snapshot.timestamp_ms().saturating_mul(1_000) <= timestamp_micros
                })
                .max_by_key(|snapshot| snapshot.timestamp_ms())
                .map(|snapshot| Some(snapshot.snapshot_id()))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        format!("no Iceberg snapshot exists at timestamp {timestamp_micros}"),
                    )
                }),
        }?;
        Ok((snapshot_id, metadata.uuid().to_string()))
    }

    fn snapshot_membership(
        &self,
        entry: &IcebergCatalogEntry,
        namespace: &str,
        table: &str,
        table_uuid: &str,
        snapshot_id: i64,
    ) -> Result<Arc<HashSet<SnapshotFileIdentity>>, ConnectorError> {
        let key = SnapshotMembershipKey {
            namespace: namespace.to_string(),
            table: table.to_string(),
            table_uuid: table_uuid.to_string(),
            snapshot_id,
        };
        self.snapshot_memberships.get_or_try_init(key, || {
            let loaded = load_table(entry, namespace, table).map_err(map_iceberg_error)?;
            if loaded.table.metadata().uuid().to_string() != table_uuid {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg split belongs to a different table incarnation",
                ));
            }
            extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                .map_err(map_iceberg_error)
                .map(|files| {
                    Arc::new(
                        files
                            .into_iter()
                            .map(|file| SnapshotFileIdentity {
                                path: file.path,
                                size: file.size,
                                row_count: file.record_count,
                            })
                            .collect(),
                    )
                })
        })
    }
}

impl ConnectorCatalogMutation for IcebergControlProvider {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn execute(
        &self,
        request: ConnectorCatalogMutationRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        if let Err(error) = self.validate_context(&request.context) {
            return Ok(known_uncommitted(error));
        }
        if let ConnectorCatalogMutationOperation::AlterRef { table, action } = &request.operation
            && matches!(
                action,
                novarocks_spi::connector::ConnectorRefAction::FastForwardBranch { .. }
            )
        {
            return self.execute_guarded_fast_forward(&request, table, action);
        }
        if let ConnectorCatalogMutationOperation::BootstrapEmptyTableSnapshot {
            table,
            expected_current_snapshot,
            properties,
        } = &request.operation
        {
            return self.execute_bootstrap_empty_table_snapshot(
                &request,
                table,
                *expected_current_snapshot,
                properties,
            );
        }
        if let ConnectorCatalogMutationOperation::AlterProperties {
            table,
            changes,
            authority,
            expected_committed_partitioning: Some(expected),
        } = &request.operation
        {
            let evidence = match self.mutation_evidence(request.operation_id, &request.operation) {
                Ok(evidence) => evidence,
                Err(error) => return Ok(known_uncommitted(error)),
            };
            if table.instance_id != self.instance_id {
                return Ok(known_uncommitted(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "property mutation belongs to another connector instance",
                )));
            }
            let entry = match self.entry(self.instance_id.as_str()) {
                Ok(entry) => entry,
                Err(error) => return Ok(known_uncommitted(error)),
            };
            let operation = match lower_property_changes(changes) {
                Ok(operation) => operation,
                Err(error) => return Ok(known_uncommitted(error)),
            };
            return match super::catalog::schema_update::alter_table_properties_on_entry(
                &entry,
                &table.namespace,
                &table.table,
                &operation,
                *authority,
                Some(expected),
            ) {
                Ok(()) => Ok(ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: self.receipt(request.operation_id, request.operation.kind(), None)?,
                    finalization: ExternalMutationFinalization::Complete,
                }),
                Err(super::catalog::schema_update::AlterTablePropertiesOnEntryError::Conflict(
                    message,
                )) => Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        message,
                    ),
                }),
                Err(super::catalog::schema_update::AlterTablePropertiesOnEntryError::Other(
                    message,
                )) => {
                    let error = map_iceberg_error(message);
                    if mutation_commit_may_be_unknown(error.kind()) {
                        Ok(ExternalMutationOutcome::CommitUnknown {
                            failure: ConnectorMutationFailure::new(
                                mutation_failure_kind(error.kind()),
                                error.to_string(),
                            ),
                            evidence,
                        })
                    } else {
                        Ok(known_uncommitted(error))
                    }
                }
            };
        }
        let operation_kind = request.operation.kind();
        let evidence = match self.mutation_evidence(request.operation_id, &request.operation) {
            Ok(evidence) => evidence,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let result = (|| -> Result<ExternalMutationEffect, ConnectorError> {
            match request.operation {
                ConnectorCatalogMutationOperation::CreateNamespace { namespace, policy } => {
                    if namespace.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "namespace mutation belongs to another connector instance",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    match namespace_exists(&entry, &namespace.namespace) {
                        Ok(true) if policy == CreatePolicy::NoOpIfExists => {
                            Ok(ExternalMutationEffect::NoOp)
                        }
                        Ok(true) => Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "namespace already exists",
                        )),
                        Ok(false) => create_namespace(&entry, &namespace.namespace)
                            .map(|()| ExternalMutationEffect::Applied)
                            .map_err(map_iceberg_error),
                        Err(error) => Err(map_iceberg_error(error)),
                    }
                }
                ConnectorCatalogMutationOperation::DropNamespace { namespace, policy } => {
                    if namespace.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "namespace mutation belongs to another connector instance",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    match namespace_exists(&entry, &namespace.namespace) {
                        Ok(false) if policy == DropPolicy::NoOpIfMissing => {
                            Ok(ExternalMutationEffect::NoOp)
                        }
                        Ok(false) => Err(ConnectorError::new(
                            ConnectorErrorKind::NotFound,
                            "namespace does not exist",
                        )),
                        Ok(true) => drop_namespace(&entry, &namespace.namespace)
                            .map(|()| ExternalMutationEffect::Applied)
                            .map_err(map_iceberg_error),
                        Err(error) => Err(map_iceberg_error(error)),
                    }
                }
                ConnectorCatalogMutationOperation::CreateTable {
                    table,
                    columns,
                    key,
                    partitioning,
                    properties,
                    policy,
                } => {
                    if table.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "table mutation belongs to another connector instance",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    let existing =
                        load_existing_table_for_reconcile(&entry, &table.namespace, &table.table)
                            .map(|loaded| loaded.is_some());
                    match existing {
                        Ok(true) if policy == CreatePolicy::NoOpIfExists => {
                            Ok(ExternalMutationEffect::NoOp)
                        }
                        Ok(true) => Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "table already exists",
                        )),
                        Ok(false) => create_table(
                            &entry,
                            &table.namespace,
                            &table.table,
                            &columns
                                .iter()
                                .map(lower_column)
                                .collect::<Result<Vec<_>, _>>()?,
                            key.as_ref().map(lower_key).transpose()?.as_ref(),
                            &partitioning
                                .iter()
                                .map(lower_partition)
                                .collect::<Result<Vec<_>, _>>()?,
                            &properties
                                .iter()
                                .map(|(key, value)| (key.to_string(), value.to_string()))
                                .collect::<Vec<_>>(),
                        )
                        .map(|()| ExternalMutationEffect::Applied)
                        .map_err(map_iceberg_error),
                        Err(error) => Err(error),
                    }
                }
                ConnectorCatalogMutationOperation::DropTable {
                    table,
                    policy,
                    data_disposition,
                } => {
                    if table.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "table mutation belongs to another connector instance",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    let existing =
                        load_existing_table_for_reconcile(&entry, &table.namespace, &table.table)
                            .map(|loaded| loaded.is_some());
                    match existing {
                        Ok(false) if policy == DropPolicy::NoOpIfMissing => {
                            if data_disposition == ConnectorDropTableDataDisposition::Purge {
                                super::catalog::registry::purge_orphan_s3_table_prefix(
                                    &entry,
                                    &table.namespace,
                                    &table.table,
                                )
                                .map(|purged| {
                                    if purged {
                                        ExternalMutationEffect::Applied
                                    } else {
                                        ExternalMutationEffect::NoOp
                                    }
                                })
                                .map_err(map_iceberg_error)
                            } else {
                                Ok(ExternalMutationEffect::NoOp)
                            }
                        }
                        Ok(false) => Err(ConnectorError::new(
                            ConnectorErrorKind::NotFound,
                            "table does not exist",
                        )),
                        Ok(true) => drop_table(&entry, &table.namespace, &table.table)
                            .map(|()| ExternalMutationEffect::Applied)
                            .map_err(map_iceberg_error),
                        Err(error) => Err(error),
                    }
                }
                ConnectorCatalogMutationOperation::CreateView {
                    view,
                    columns,
                    definition,
                    comment,
                    properties,
                    policy,
                } => {
                    if view.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "view mutation belongs to another connector instance",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    let table_exists = list_tables(&entry, &view.namespace)
                        .map_err(map_iceberg_error)
                        .map(|tables| {
                            tables
                                .iter()
                                .any(|candidate| candidate.eq_ignore_ascii_case(&view.view))
                        });
                    match table_exists {
                        Ok(true) => {
                            return Err(ConnectorError::new(
                                ConnectorErrorKind::InvalidRequest,
                                "a table with the requested view name already exists",
                            ));
                        }
                        Ok(false) => {}
                        Err(error) => return Err(error),
                    }
                    match views::view_exists(&entry, &view.namespace, &view.view) {
                        Ok(true) if policy == CreateOrReplacePolicy::NoOpIfExists => {
                            Ok(ExternalMutationEffect::NoOp)
                        }
                        Ok(true) if policy == CreateOrReplacePolicy::FailIfExists => {
                            Err(ConnectorError::new(
                                ConnectorErrorKind::InvalidRequest,
                                "view already exists",
                            ))
                        }
                        Ok(existing) => views::create_view(
                            &entry,
                            &view.namespace,
                            &view.view,
                            &columns
                                .iter()
                                .map(lower_column)
                                .collect::<Result<Vec<_>, _>>()?,
                            &definition.sql,
                            comment.as_deref(),
                            existing && policy == CreateOrReplacePolicy::ReplaceIfExists,
                            &properties
                                .iter()
                                .map(|(key, value)| (key.to_string(), value.to_string()))
                                .collect::<Vec<_>>(),
                        )
                        .map(|()| ExternalMutationEffect::Applied)
                        .map_err(map_iceberg_error),
                        Err(error) => Err(map_iceberg_error(error)),
                    }
                }
                ConnectorCatalogMutationOperation::DropView { view, policy } => {
                    if view.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "view mutation belongs to another connector instance",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    match views::view_exists(&entry, &view.namespace, &view.view) {
                        Ok(false) if policy == DropPolicy::NoOpIfMissing => {
                            Ok(ExternalMutationEffect::NoOp)
                        }
                        Ok(false) => Err(ConnectorError::new(
                            ConnectorErrorKind::NotFound,
                            "view does not exist",
                        )),
                        Ok(true) => views::drop_view(&entry, &view.namespace, &view.view)
                            .map(|()| ExternalMutationEffect::Applied)
                            .map_err(map_iceberg_error),
                        Err(error) => Err(map_iceberg_error(error)),
                    }
                }
                ConnectorCatalogMutationOperation::AlterPartitionSpec { table, add, drop } => {
                    if table.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "partition mutation belongs to another connector instance",
                        ));
                    }
                    if add.len() + drop.len() != 1 {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "Iceberg partition mutation requires exactly one add or drop transform",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    let field = add
                        .first()
                        .or_else(|| drop.first())
                        .expect("validated non-empty partition mutation");
                    let stmt = if add.is_empty() {
                        crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::DropPartitionColumn {
                        table: crate::sql::parser::ast::ObjectName {
                            parts: vec![table.namespace.to_string(), table.table.to_string()],
                        },
                        field: lower_partition(field)?,
                    }
                    } else {
                        crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                            table: crate::sql::parser::ast::ObjectName {
                                parts: vec![table.namespace.to_string(), table.table.to_string()],
                            },
                            field: lower_partition(field)?,
                        }
                    };
                    super::catalog::registry::alter_partition_spec(
                        &entry,
                        &table.namespace,
                        &table.table,
                        stmt,
                    )
                    .map(|()| ExternalMutationEffect::Applied)
                    .map_err(map_iceberg_error)
                }
                ConnectorCatalogMutationOperation::AlterRef { table, action } => {
                    if table.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "ref mutation belongs to another connector instance",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    let loaded = load_table(&entry, &table.namespace, &table.table)
                        .map_err(map_iceberg_error)?;
                    let action = novarocks_connector_iceberg::commit::lower_ref_action(
                        action,
                        loaded.table.metadata(),
                        &table.namespace,
                        &table.table,
                        self.instance_id.as_str(),
                    )?;
                    let catalog = super::catalog::registry::build_iceberg_catalog(&entry)
                        .map_err(map_iceberg_error)?;
                    super::catalog::registry::block_on_iceberg(async {
                        novarocks_connector_iceberg::commit::execute_ref_action(
                            catalog.as_ref(),
                            &action,
                        )
                        .await
                    })
                    .map_err(|error| {
                        ConnectorError::new(
                            ConnectorErrorKind::Internal,
                            format!("execute Iceberg ref mutation runtime: {error}"),
                        )
                    })?
                    .map(|outcome| match outcome {
                        novarocks_connector_iceberg::commit::RefActionOutcome::Committed => {
                            ExternalMutationEffect::Applied
                        }
                        novarocks_connector_iceberg::commit::RefActionOutcome::NoOp => {
                            ExternalMutationEffect::NoOp
                        }
                    })
                    .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))
                }
                ConnectorCatalogMutationOperation::AlterProperties {
                    table,
                    changes,
                    authority,
                    expected_committed_partitioning: _,
                } => {
                    if table.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "property mutation belongs to another connector instance",
                        ));
                    }
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    let operation = lower_property_changes(&changes)?;
                    super::catalog::schema_update::alter_table_properties_on_entry(
                        &entry,
                        &table.namespace,
                        &table.table,
                        &operation,
                        authority,
                        None,
                    )
                    .map(|()| ExternalMutationEffect::Applied)
                    .map_err(|error| {
                        match error {
                        super::catalog::schema_update::AlterTablePropertiesOnEntryError::Conflict(
                            message,
                        )
                        | super::catalog::schema_update::AlterTablePropertiesOnEntryError::Other(
                            message,
                        ) => map_iceberg_error(message),
                    }
                    })
                }
                ConnectorCatalogMutationOperation::AlterSchema { table, changes } => {
                    if table.instance_id != self.instance_id {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "schema mutation belongs to another connector instance",
                        ));
                    }
                    let [change] = changes.as_slice() else {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::Unsupported,
                            "Iceberg schema mutation currently requires exactly one change",
                        ));
                    };
                    let entry = match self.entry(self.instance_id.as_str()) {
                        Ok(entry) => entry,
                        Err(error) => return Err(error),
                    };
                    let change = lower_schema_change(change)?;
                    super::catalog::schema_update::alter_table_schema_on_entry(
                        &entry,
                        &table.namespace,
                        &table.table,
                        &change,
                    )
                    .map(|()| ExternalMutationEffect::Applied)
                    .map_err(map_iceberg_error)
                }
                _ => Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    format!("Iceberg catalog mutation `{operation_kind}` is not implemented"),
                )),
            }
        })();
        match result {
            Ok(effect) => Ok(ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt: self.receipt(request.operation_id, operation_kind, None)?,
                finalization: ExternalMutationFinalization::Complete,
            }),
            Err(error)
                if mutation_commit_may_be_unknown(error.kind())
                    && !error.retryable_before_progress() =>
            {
                Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        mutation_failure_kind(error.kind()),
                        error.to_string(),
                    ),
                    evidence,
                })
            }
            Err(error) => Ok(known_uncommitted(error)),
        }
    }

    fn reconcile(
        &self,
        request: ConnectorCatalogMutationReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
        if let Err(error) = self.validate_context(&request.context) {
            return Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    mutation_failure_kind(error.kind()),
                    error.to_string(),
                ),
                evidence: request.evidence,
            });
        }
        let decoded =
            decode_mutation_evidence(request.evidence.provider_payload()).map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("decode Iceberg mutation evidence: {error}"),
                )
            })?;
        if decoded.version != ICEBERG_MUTATION_EVIDENCE_VERSION
            || request.evidence.schema_version() != ICEBERG_MUTATION_EVIDENCE_VERSION
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "unsupported Iceberg mutation evidence version",
            ));
        }
        let entry = match self.entry(self.instance_id.as_str()) {
            Ok(entry) => entry,
            Err(error) => {
                return Ok(ExternalMutationOutcome::CommitUnknown {
                    failure: ConnectorMutationFailure::new(
                        mutation_failure_kind(error.kind()),
                        error.to_string(),
                    ),
                    evidence: request.evidence,
                });
            }
        };
        reconcile_iceberg_mutation_evidence(self, &entry, decoded.target, request.evidence)
    }
}

impl ConnectorStagedPublicationRecovery for IcebergControlProvider {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    fn inspect(
        &self,
        descriptor: ConnectorStagedPublicationDescriptor,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorStagedPublicationObservation, ConnectorError> {
        self.validate_context(&context)?;
        descriptor.validate()?;
        if descriptor.table.instance_id != self.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "staged publication descriptor belongs to another Iceberg instance",
            ));
        }
        let entry = self.entry(self.instance_id.as_str())?;
        entry.invalidate_table_cache(&descriptor.table.namespace, &descriptor.table.table);
        let loaded = load_table(&entry, &descriptor.table.namespace, &descriptor.table.table)
            .map_err(map_iceberg_error)?;
        let metadata = loaded.table.metadata();
        let staging_snapshot_id = metadata
            .refs()
            .get(descriptor.staging_ref.as_ref())
            .map(|reference| reference.snapshot_id);
        let target_snapshot_id = if descriptor.target_ref.as_ref() == "main" {
            metadata.current_snapshot_id()
        } else {
            metadata
                .refs()
                .get(descriptor.target_ref.as_ref())
                .map(|reference| reference.snapshot_id)
        };
        let marker = crate::connector::iceberg::commit::MvRefreshSnapshotMarker {
            refresh_id: descriptor.refresh_id,
            mv_id: descriptor.mv_id,
            token: descriptor.marker_token.to_string(),
        };
        let marker_for = |snapshot_id: i64| {
            metadata
                .snapshot_by_id(snapshot_id)
                .is_some_and(|snapshot| {
                    crate::connector::iceberg::commit::snapshot_matches_refresh_marker(
                        snapshot, &marker,
                    )
                })
        };
        let main_ancestors = iceberg_main_ancestors(metadata);
        let disposition = match (staging_snapshot_id, target_snapshot_id) {
            (Some(staging), Some(target)) if staging == target => {
                if marker_for(staging) {
                    ConnectorStagedPublicationDisposition::CleanupPending
                } else {
                    ConnectorStagedPublicationDisposition::Ambiguous
                }
            }
            (Some(staging), _) if main_ancestors.contains(&staging) => {
                if marker_for(staging) {
                    ConnectorStagedPublicationDisposition::Superseded
                } else {
                    ConnectorStagedPublicationDisposition::Ambiguous
                }
            }
            (Some(staging), _) if marker_for(staging) => {
                ConnectorStagedPublicationDisposition::Staged
            }
            (Some(_), _) => ConnectorStagedPublicationDisposition::Ambiguous,
            (None, Some(target)) if marker_for(target) => {
                ConnectorStagedPublicationDisposition::Published
            }
            (None, _) => ConnectorStagedPublicationDisposition::KnownUncommitted,
        };
        let observed_snapshot = match disposition {
            ConnectorStagedPublicationDisposition::Published
            | ConnectorStagedPublicationDisposition::CleanupPending => target_snapshot_id,
            ConnectorStagedPublicationDisposition::Superseded
            | ConnectorStagedPublicationDisposition::Staged => staging_snapshot_id,
            ConnectorStagedPublicationDisposition::KnownUncommitted
            | ConnectorStagedPublicationDisposition::Ambiguous => None,
        };
        let (committed_version, resulting_row_count, bases, definition_fingerprint) = if matches!(
            disposition,
            ConnectorStagedPublicationDisposition::Published
                | ConnectorStagedPublicationDisposition::Superseded
                | ConnectorStagedPublicationDisposition::CleanupPending
        ) {
            let snapshot_id = observed_snapshot.ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "published MV recovery observation has no snapshot",
                )
            })?;
            let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "published MV recovery snapshot is missing",
                )
            })?;
            let provenance =
                novarocks_connector_iceberg::commit::MvProvenanceV1::from_snapshot_summary(
                    snapshot,
                )
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?
                .filter(|provenance| {
                    provenance.refresh_id == descriptor.refresh_id
                        && provenance.mv_id == descriptor.mv_id
                        && provenance.token == descriptor.marker_token.as_ref()
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "published MV recovery snapshot lacks matching provenance",
                    )
                })?;
            let total_records = snapshot
                .summary()
                .additional_properties
                .get("total-records")
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "published MV recovery snapshot lacks total-records",
                    )
                })?
                .parse::<u64>()
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        format!("published MV recovery has invalid total-records: {error}"),
                    )
                })?;
            if provenance.rows < 0
                || (provenance.rows != 0 && provenance.rows as u64 != total_records)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "published MV recovery provenance rows conflict with total-records",
                ));
            }
            let bases = provenance
                .bases
                .into_iter()
                .map(|base| ConnectorStagedPublicationBaseFact {
                    table: Arc::from(base.table_fqn),
                    uuid: Arc::from(base.uuid),
                    from_version: base.from_snapshot,
                    to_version: base.to_snapshot,
                })
                .collect::<Vec<_>>();
            let version = ConnectorCommittedVersion::try_new(
                Bytes::from(format!("iceberg/recovery/v1/{snapshot_id}")),
                Some(snapshot_id),
            )?;
            (
                Some(version),
                Some(total_records),
                bases,
                Some(Arc::from(provenance.definition_fingerprint)),
            )
        } else {
            (None, None, Vec::new(), None)
        };
        let proof = IcebergStagedPublicationProofV1 {
            version: ICEBERG_STAGED_PUBLICATION_PROOF_VERSION,
            descriptor_digest: descriptor.digest().to_vec(),
            namespace: descriptor.table.namespace.to_string(),
            table: descriptor.table.table.to_string(),
            table_uuid: metadata.uuid().to_string(),
            staging_ref: descriptor.staging_ref.to_string(),
            staging_snapshot_id,
            target_ref: descriptor.target_ref.to_string(),
            target_snapshot_id,
            refresh_id: descriptor.refresh_id,
            mv_id: descriptor.mv_id,
            marker_token: descriptor.marker_token.to_string(),
        };
        let proof = encode_staged_publication_proof(&proof)
            .map(Bytes::from)
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Internal, error.to_string())
            })?;
        ConnectorStagedPublicationObservation::try_new(
            disposition,
            committed_version,
            resulting_row_count,
            bases,
            definition_fingerprint,
            staging_snapshot_id,
            target_snapshot_id,
            staging_snapshot_id.is_some(),
            ConnectorStagedPublicationProof::try_new(proof)?,
        )
    }

    fn cleanup(
        &self,
        request: ConnectorStagedPublicationCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>, ConnectorError>
    {
        self.validate_context(&request.context)?;
        request.observation.validate()?;
        if let Some(outcome) = self
            .recovery_cleanup_outcomes
            .lock()
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("Iceberg recovery cleanup outcome lock: {error}"),
                )
            })?
            .get(&request.operation_id)
            .map(|record| record.outcome.clone())
        {
            return Ok(outcome);
        }
        let proof = decode_staged_publication_proof(request.observation.proof.payload()).map_err(
            |error| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("invalid Iceberg staged publication cleanup proof: {error}"),
                )
            },
        )?;
        if proof.version != ICEBERG_STAGED_PUBLICATION_PROOF_VERSION
            || proof.descriptor_digest.as_slice() != request.descriptor_digest
            || proof.staging_snapshot_id != request.observation.staging_snapshot_id
            || proof.staging_snapshot_id.is_none()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg staged publication cleanup proof conflicts with observation",
            ));
        }
        let table = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: self.instance_id.clone(),
            namespace: Arc::from(proof.namespace.clone()),
            table: Arc::from(proof.table.clone()),
        };
        let entry = self.entry(self.instance_id.as_str())?;
        entry.invalidate_table_cache(&table.namespace, &table.table);
        let loaded =
            load_table(&entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        if loaded.table.metadata().uuid().to_string() != proof.table_uuid {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg staged publication cleanup table UUID drifted",
            ));
        }
        if let Some(reference) = loaded.table.metadata().refs().get(&proof.staging_ref) {
            if reference.snapshot_id != proof.staging_snapshot_id.expect("checked above")
                || !reference.is_branch()
            {
                return Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "Iceberg staged publication ref drifted before cleanup",
                    ),
                });
            }
        } else {
            let outcome = ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::NoOp,
                receipt: ConnectorStagedPublicationCleanupReceipt {
                    descriptor_digest: request.descriptor_digest,
                    observation_digest: request.observation.digest(),
                },
                finalization: ExternalMutationFinalization::Complete,
            };
            self.recovery_cleanup_outcomes
                .lock()
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::Internal,
                        format!("Iceberg recovery cleanup outcome lock: {error}"),
                    )
                })?
                .insert(
                    request.operation_id,
                    IcebergRecoveryCleanupRecord {
                        outcome: outcome.clone(),
                        proof,
                        descriptor_digest: request.descriptor_digest,
                        observation_digest: request.observation.digest(),
                    },
                );
            return Ok(outcome);
        }
        let outcome = ConnectorCatalogMutation::execute(
            self,
            ConnectorCatalogMutationRequest {
                operation_id: request.operation_id,
                target: self.binding_key.clone(),
                operation: ConnectorCatalogMutationOperation::AlterRef {
                    table,
                    action: ConnectorRefAction::Drop {
                        kind: ConnectorRefKind::Branch,
                        name: Arc::from(proof.staging_ref.clone()),
                        policy: DropPolicy::NoOpIfMissing,
                    },
                },
                context: request.context,
            },
        )?;
        let outcome = match outcome {
            ExternalMutationOutcome::KnownCommitted {
                effect,
                finalization,
                ..
            } => ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt: ConnectorStagedPublicationCleanupReceipt {
                    descriptor_digest: request.descriptor_digest,
                    observation_digest: request.observation.digest(),
                },
                finalization,
            },
            ExternalMutationOutcome::KnownUncommitted { failure } => {
                ExternalMutationOutcome::KnownUncommitted { failure }
            }
            ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
                ExternalMutationOutcome::CommitUnknown { failure, evidence }
            }
        };
        self.recovery_cleanup_outcomes
            .lock()
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("Iceberg recovery cleanup outcome lock: {error}"),
                )
            })?
            .insert(
                request.operation_id,
                IcebergRecoveryCleanupRecord {
                    outcome: outcome.clone(),
                    proof,
                    descriptor_digest: request.descriptor_digest,
                    observation_digest: request.observation.digest(),
                },
            );
        Ok(outcome)
    }

    fn reconcile_cleanup(
        &self,
        operation_id: ConnectorMutationOperationId,
        evidence: ExternalMutationEvidence,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorStagedPublicationCleanupReceipt>, ConnectorError>
    {
        self.validate_context(&context)?;
        if evidence.operation_id() != operation_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg staged publication cleanup evidence operation does not match",
            ));
        }
        let record = self
            .recovery_cleanup_outcomes
            .lock()
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("Iceberg recovery cleanup outcome lock: {error}"),
                )
            })?
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    "Iceberg staged publication cleanup has no retained outcome to reconcile",
                )
            })?;
        if !matches!(
            record.outcome,
            ExternalMutationOutcome::CommitUnknown { .. }
        ) {
            return Ok(record.outcome);
        }
        let table = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: self.instance_id.clone(),
            namespace: Arc::from(record.proof.namespace.clone()),
            table: Arc::from(record.proof.table.clone()),
        };
        let entry = self.entry(self.instance_id.as_str())?;
        entry.invalidate_table_cache(&table.namespace, &table.table);
        let loaded =
            load_table(&entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let outcome = if loaded.table.metadata().uuid().to_string() != record.proof.table_uuid {
            ExternalMutationOutcome::KnownUncommitted {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "Iceberg staged publication cleanup table UUID drifted during reconciliation",
                ),
            }
        } else {
            match loaded
                .table
                .metadata()
                .refs()
                .get(&record.proof.staging_ref)
            {
                None => ExternalMutationOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    receipt: ConnectorStagedPublicationCleanupReceipt {
                        descriptor_digest: record.descriptor_digest,
                        observation_digest: record.observation_digest,
                    },
                    finalization: ExternalMutationFinalization::Complete,
                },
                Some(reference)
                    if reference.is_branch()
                        && Some(reference.snapshot_id) == record.proof.staging_snapshot_id =>
                {
                    ExternalMutationOutcome::KnownUncommitted {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            "Iceberg staged publication cleanup ref still points at the inspected snapshot",
                        ),
                    }
                }
                Some(_) => ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "Iceberg staged publication cleanup ref drifted during reconciliation",
                    ),
                },
            }
        };
        self.recovery_cleanup_outcomes
            .lock()
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("Iceberg recovery cleanup outcome lock: {error}"),
                )
            })?
            .insert(
                operation_id,
                IcebergRecoveryCleanupRecord {
                    outcome: outcome.clone(),
                    ..record
                },
            );
        Ok(outcome)
    }
}

fn iceberg_main_ancestors(
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
) -> Vec<i64> {
    let mut ancestors = Vec::new();
    let mut cursor = metadata.current_snapshot_id();
    while let Some(snapshot_id) = cursor {
        if ancestors.len()
            == novarocks_spi::connector::MAX_CONNECTOR_STAGED_PUBLICATION_LINEAGE_FACTS
        {
            break;
        }
        ancestors.push(snapshot_id);
        cursor = metadata
            .snapshot_by_id(snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }
    ancestors
}

fn known_uncommitted(
    error: ConnectorError,
) -> ExternalMutationOutcome<ConnectorCatalogMutationReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(
            mutation_failure_kind(error.kind()),
            error.to_string(),
        ),
    }
}

fn statistics_known_uncommitted(
    error: ConnectorError,
) -> ExternalMutationOutcome<StatisticsReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(
            mutation_failure_kind(error.kind()),
            error.to_string(),
        ),
    }
}

fn mutation_failure_kind(kind: ConnectorErrorKind) -> ConnectorMutationFailureKind {
    match kind {
        ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
        ConnectorErrorKind::PermissionDenied => ConnectorMutationFailureKind::PermissionDenied,
        ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => ConnectorMutationFailureKind::DeadlineExceeded,
        ConnectorErrorKind::ResourceExhausted => ConnectorMutationFailureKind::ResourceExhausted,
        ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
        ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
        ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
    }
}

fn mutation_commit_may_be_unknown(kind: ConnectorErrorKind) -> bool {
    matches!(
        kind,
        ConnectorErrorKind::Unavailable
            | ConnectorErrorKind::DeadlineExceeded
            | ConnectorErrorKind::Cancelled
            | ConnectorErrorKind::Internal
    )
}

fn provider_version(metadata_location: Option<&str>) -> Bytes {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.metadata-version.v1");
    if let Some(metadata_location) = metadata_location {
        hasher.update(metadata_location.as_bytes());
    }
    Bytes::copy_from_slice(&hasher.finalize())
}

fn snapshot_matches_publication_guard(
    snapshot: &novarocks_connector_iceberg::iceberg::spec::Snapshot,
    guard: &ConnectorRefreshPublicationGuard,
) -> bool {
    let properties = &snapshot.summary().additional_properties;
    properties
        .get(super::commit::MV_REFRESH_ID_PROP)
        .and_then(|value| value.parse::<i64>().ok())
        == Some(guard.refresh_id())
        && properties
            .get(super::commit::MV_ID_PROP)
            .and_then(|value| value.parse::<i64>().ok())
            == Some(guard.materialized_view_id())
        && properties
            .get(super::commit::MV_REFRESH_TOKEN_PROP)
            .map(String::as_str)
            == Some(guard.token())
}

/// Decode the provider-private write version only inside the Iceberg control
/// provider. The frontend persists this value opaquely and cannot substitute a
/// bare snapshot ID for it at publication time.
fn source_snapshot_id_from_committed_version(
    committed_version: &ConnectorCommittedVersion,
) -> Result<i64, ConnectorError> {
    committed_version.validate()?;
    let snapshot_id = committed_version.snapshot_id().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg guarded publication requires a snapshot-bearing committed version",
        )
    })?;
    let decoded =
        novarocks_connector_iceberg::write_codec::decode_write_receipt(committed_version.payload())
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("decode Iceberg committed version for guarded publication: {error}"),
                )
            })?;
    if decoded != snapshot_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg committed version snapshot fact does not match its opaque payload",
        ));
    }
    Ok(snapshot_id)
}

fn build_guarded_fast_forward_commit(
    namespace: &str,
    table: &str,
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    source_branch: &str,
    target_branch: &str,
    source_snapshot_id: i64,
    expected_target_snapshot_id: Option<i64>,
    guard: &ConnectorRefreshPublicationGuard,
) -> Result<novarocks_connector_iceberg::iceberg::TableCommit, ConnectorError> {
    let source = metadata.refs().get(source_branch).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::NotFound,
            "MV publication staging branch does not exist",
        )
    })?;
    if !source.is_branch() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "MV publication staging ref is not a branch",
        ));
    }
    if source.snapshot_id != source_snapshot_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "MV publication staging branch does not match its expected snapshot",
        ));
    }
    let snapshot = metadata.snapshot_by_id(source_snapshot_id).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::NotFound,
            "MV publication staging snapshot does not exist",
        )
    })?;
    if !snapshot_matches_publication_guard(snapshot, guard) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "MV publication staging snapshot marker mismatch",
        ));
    }
    let target_snapshot_id = if target_branch.eq_ignore_ascii_case("main") {
        metadata.current_snapshot_id()
    } else {
        let target = metadata.refs().get(target_branch).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "MV publication target branch does not exist",
            )
        })?;
        if !target.is_branch() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "MV publication target ref is not a branch",
            ));
        }
        Some(target.snapshot_id)
    };
    if target_snapshot_id != expected_target_snapshot_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "MV publication target branch does not match its expected snapshot",
        ));
    }
    let ident = novarocks_connector_iceberg::iceberg::TableIdent::from_strs([namespace, table])
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("invalid Iceberg MV publication table identity: {error}"),
            )
        })?;
    Ok(novarocks_connector_iceberg::iceberg::TableCommit::builder()
        .ident(ident)
        .updates(vec![
            novarocks_connector_iceberg::iceberg::TableUpdate::SetSnapshotRef {
                ref_name: target_branch.to_string(),
                reference: novarocks_connector_iceberg::iceberg::spec::SnapshotReference {
                    snapshot_id: source_snapshot_id,
                    retention:
                        novarocks_connector_iceberg::iceberg::spec::SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                },
            },
        ])
        .requirements(vec![
            novarocks_connector_iceberg::iceberg::TableRequirement::RefSnapshotIdMatch {
                r#ref: target_branch.to_string(),
                snapshot_id: expected_target_snapshot_id,
            },
            novarocks_connector_iceberg::iceberg::TableRequirement::RefSnapshotIdMatch {
                r#ref: source_branch.to_string(),
                snapshot_id: Some(source_snapshot_id),
            },
        ])
        .build())
}

fn load_existing_table_for_reconcile(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table: &str,
) -> Result<Option<super::catalog::IcebergLoadedTable>, ConnectorError> {
    entry.invalidate_table_cache(namespace, table);
    match load_table(entry, namespace, table).map_err(map_iceberg_error) {
        Ok(loaded) => Ok(Some(loaded)),
        Err(error) if error.kind() == ConnectorErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn reconcile_iceberg_mutation_evidence(
    provider: &IcebergControlProvider,
    entry: &IcebergCatalogEntry,
    target: IcebergMutationEvidenceTarget,
    evidence: ExternalMutationEvidence,
) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError> {
    let operation_id = evidence.operation_id();
    let operation_kind = evidence.operation_kind().to_string();
    let known_committed = |effect| {
        provider
            .receipt(operation_id, &operation_kind, None)
            .map(|receipt| ExternalMutationOutcome::KnownCommitted {
                effect,
                receipt,
                finalization: ExternalMutationFinalization::Complete,
            })
    };
    let unknown = |error: ConnectorError| ExternalMutationOutcome::CommitUnknown {
        failure: ConnectorMutationFailure::new(
            mutation_failure_kind(error.kind()),
            error.to_string(),
        ),
        evidence: evidence.clone(),
    };
    let ambiguous = |message: String| ExternalMutationOutcome::CommitUnknown {
        failure: ConnectorMutationFailure::new(ConnectorMutationFailureKind::Conflict, message),
        evidence: evidence.clone(),
    };
    let uncommitted = |message: String| ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(ConnectorMutationFailureKind::Conflict, message),
    };

    match target {
        IcebergMutationEvidenceTarget::Namespace {
            namespace,
            should_exist,
        } => match namespace_exists(entry, &namespace).map_err(map_iceberg_error) {
            // Namespaces do not have an immutable catalog identity.  A matching
            // presence bit could have been produced by a concurrent creator or
            // dropper, so an authoritative reread cannot attribute it to this
            // operation.
            Ok(exists) if exists == should_exist => Ok(ambiguous(format!(
                "authoritative namespace state for `{namespace}` matches but cannot be attributed to this mutation"
            ))),
            Ok(_) => Ok(uncommitted(format!(
                "authoritative namespace state for `{namespace}` does not match mutation"
            ))),
            Err(error) => Ok(unknown(error)),
        },
        IcebergMutationEvidenceTarget::Table {
            namespace,
            table,
            should_exist,
            before_uuid,
        } => match load_existing_table_for_reconcile(entry, &namespace, &table) {
            // A table created after an absent pre-state has no operation marker
            // in the catalog protocol.  Its existence alone could belong to a
            // concurrent creator.  Likewise, absence after a drop cannot prove
            // which actor removed the object.
            Ok(current) if should_exist && current.is_some() => Ok(ambiguous(format!(
                "authoritative table `{namespace}.{table}` exists but cannot be attributed to this create"
            ))),
            Ok(None) if !should_exist => Ok(ambiguous(format!(
                "authoritative table `{namespace}.{table}` is absent but cannot be attributed to this drop"
            ))),
            Ok(Some(current)) if !should_exist => {
                if before_uuid.as_deref()
                    == Some(current.table.metadata().uuid().to_string().as_str())
                {
                    Ok(uncommitted(format!(
                        "authoritative table `{namespace}.{table}` still has its pre-mutation identity"
                    )))
                } else {
                    Ok(ExternalMutationOutcome::CommitUnknown {
                        failure: ConnectorMutationFailure::new(
                            ConnectorMutationFailureKind::Conflict,
                            "table name was reused with a different identity during reconciliation",
                        ),
                        evidence,
                    })
                }
            }
            Ok(None) => Ok(uncommitted(format!(
                "authoritative table `{namespace}.{table}` is absent"
            ))),
            Ok(Some(_)) => Ok(uncommitted(format!(
                "authoritative table `{namespace}.{table}` does not match mutation"
            ))),
            Err(error) => Ok(unknown(error)),
        },
        IcebergMutationEvidenceTarget::View {
            namespace,
            view,
            should_exist,
        } => match views::view_exists(entry, &namespace, &view).map_err(map_iceberg_error) {
            // The current view adapter exposes only presence here.  Do not turn
            // a concurrent same-name view mutation into a false commit proof.
            Ok(exists) if exists == should_exist => Ok(ambiguous(format!(
                "authoritative view state for `{namespace}.{view}` matches but cannot be attributed to this mutation"
            ))),
            Ok(_) => Ok(uncommitted(format!(
                "authoritative view state for `{namespace}.{view}` does not match mutation"
            ))),
            Err(error) => Ok(unknown(error)),
        },
        IcebergMutationEvidenceTarget::TableVersion {
            namespace,
            table,
            table_uuid,
            before_metadata_location,
        } => match load_existing_table_for_reconcile(entry, &namespace, &table) {
            Ok(Some(current)) if current.table.metadata().uuid().to_string() == table_uuid => {
                if current.table.metadata_location().map(str::to_string) != before_metadata_location
                {
                    // A metadata-location advance proves that *some* commit
                    // happened, not that it contains this schema/partition/
                    // properties change.  The provider intentionally keeps the
                    // result uncertain until the evidence format carries a
                    // semantic postcondition digest for these operations.
                    Ok(ambiguous(format!(
                        "authoritative metadata version for `{namespace}.{table}` advanced without a matching semantic postcondition"
                    )))
                } else {
                    Ok(uncommitted(format!(
                        "authoritative metadata version for `{namespace}.{table}` did not advance"
                    )))
                }
            }
            Ok(Some(_)) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "table identity changed during mutation reconciliation",
                ),
                evidence,
            }),
            Ok(None) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "table disappeared during mutation reconciliation",
                ),
                evidence,
            }),
            Err(error) => Ok(unknown(error)),
        },
        IcebergMutationEvidenceTarget::BootstrapEmptyTableSnapshot {
            namespace,
            table,
            table_uuid,
            operation_marker,
        } => match load_existing_table_for_reconcile(entry, &namespace, &table) {
            Ok(Some(current)) if current.table.metadata().uuid().to_string() == table_uuid => {
                match current.table.metadata().current_snapshot() {
                    Some(snapshot)
                        if snapshot.parent_snapshot_id().is_none()
                            && snapshot
                                .summary()
                                .additional_properties
                                .get("novarocks.bootstrap.empty.operation-id")
                                .map(String::as_str)
                                == Some(operation_marker.as_str()) =>
                    {
                        known_committed(ExternalMutationEffect::Applied)
                    }
                    Some(_) => Ok(uncommitted(format!(
                        "authoritative empty-table bootstrap state for `{namespace}.{table}` does not match its operation marker"
                    ))),
                    None => Ok(uncommitted(format!(
                        "authoritative table `{namespace}.{table}` has no bootstrap snapshot"
                    ))),
                }
            }
            Ok(Some(_)) | Ok(None) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "table identity changed during empty-table bootstrap reconciliation",
                ),
                evidence,
            }),
            Err(error) => Ok(unknown(error)),
        },
        IcebergMutationEvidenceTarget::Ref {
            namespace,
            table,
            table_uuid,
            ref_name,
            expected_snapshot_id,
        } => match load_existing_table_for_reconcile(entry, &namespace, &table) {
            Ok(Some(current)) if current.table.metadata().uuid().to_string() == table_uuid => {
                let metadata = current.table.metadata();
                let actual_snapshot_id = if ref_name.eq_ignore_ascii_case("main") {
                    metadata.current_snapshot_id()
                } else {
                    metadata
                        .refs()
                        .get(&ref_name)
                        .map(|reference| reference.snapshot_id)
                };
                if actual_snapshot_id == expected_snapshot_id {
                    known_committed(ExternalMutationEffect::Applied)
                } else {
                    Ok(uncommitted(format!(
                        "authoritative ref `{ref_name}` does not match mutation target"
                    )))
                }
            }
            Ok(Some(_)) | Ok(None) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "table identity changed during ref reconciliation",
                ),
                evidence,
            }),
            Err(error) => Ok(unknown(error)),
        },
        IcebergMutationEvidenceTarget::GuardedFastForward {
            namespace,
            table,
            table_uuid,
            before_metadata_location,
            source_branch,
            target_branch,
            source_snapshot_id,
            expected_target_snapshot_id,
            guard_digest,
        } => match load_existing_table_for_reconcile(entry, &namespace, &table) {
            Ok(Some(current)) if current.table.metadata().uuid().to_string() == table_uuid => {
                let metadata = current.table.metadata();
                let target_snapshot_id = if target_branch.eq_ignore_ascii_case("main") {
                    metadata.current_snapshot_id()
                } else {
                    metadata
                        .refs()
                        .get(&target_branch)
                        .filter(|reference| reference.is_branch())
                        .map(|reference| reference.snapshot_id)
                };
                let source_is_matching_branch = matches!(
                    metadata.refs().get(&source_branch),
                    Some(reference) if reference.is_branch() && reference.snapshot_id == source_snapshot_id
                );
                let marker_matches = metadata
                    .snapshot_by_id(source_snapshot_id)
                    .and_then(|snapshot| publication_guard_digest_from_snapshot(snapshot).ok())
                    .is_some_and(|digest| digest == guard_digest);
                if target_snapshot_id == Some(source_snapshot_id) && marker_matches {
                    known_committed(ExternalMutationEffect::Applied)
                } else if target_snapshot_id == expected_target_snapshot_id
                    && current.table.metadata_location().map(str::to_string)
                        == before_metadata_location
                    && source_is_matching_branch
                {
                    Ok(uncommitted(format!(
                        "authoritative guarded MV publication state for `{namespace}.{table}` did not advance"
                    )))
                } else {
                    Ok(ambiguous(format!(
                        "authoritative guarded MV publication state for `{namespace}.{table}` diverged"
                    )))
                }
            }
            Ok(Some(_)) | Ok(None) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "table identity changed during guarded MV publication reconciliation",
                ),
                evidence,
            }),
            Err(error) => Ok(unknown(error)),
        },
    }
}

fn publication_guard_digest_from_snapshot(
    snapshot: &novarocks_connector_iceberg::iceberg::spec::Snapshot,
) -> Result<[u8; 32], ConnectorError> {
    let properties = &snapshot.summary().additional_properties;
    let refresh_id = properties
        .get(super::commit::MV_REFRESH_ID_PROP)
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "MV publication snapshot is missing a valid refresh id marker",
            )
        })?;
    let materialized_view_id = properties
        .get(super::commit::MV_ID_PROP)
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "MV publication snapshot is missing a valid materialized view id marker",
            )
        })?;
    let token = properties
        .get(super::commit::MV_REFRESH_TOKEN_PROP)
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "MV publication snapshot is missing its refresh token marker",
            )
        })?;
    Ok(
        ConnectorRefreshPublicationGuard::try_new(
            refresh_id,
            materialized_view_id,
            token.as_str(),
        )?
        .digest(),
    )
}

pub(crate) fn lower_column(
    column: &ConnectorColumnDefinition,
) -> Result<crate::sql::parser::ast::TableColumnDef, ConnectorError> {
    Ok(crate::sql::parser::ast::TableColumnDef {
        name: column.name.to_string(),
        data_type: lower_data_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: column.aggregation.map(|aggregation| match aggregation {
            ConnectorColumnAggregation::Sum => crate::sql::parser::ast::ColumnAggregation::Sum,
            ConnectorColumnAggregation::Min => crate::sql::parser::ast::ColumnAggregation::Min,
            ConnectorColumnAggregation::Max => crate::sql::parser::ast::ColumnAggregation::Max,
            ConnectorColumnAggregation::Replace => {
                crate::sql::parser::ast::ColumnAggregation::Replace
            }
            ConnectorColumnAggregation::ReplaceIfNotNull => {
                crate::sql::parser::ast::ColumnAggregation::ReplaceIfNotNull
            }
            ConnectorColumnAggregation::BitmapUnion => {
                crate::sql::parser::ast::ColumnAggregation::BitmapUnion
            }
            ConnectorColumnAggregation::HllUnion => {
                crate::sql::parser::ast::ColumnAggregation::HllUnion
            }
        }),
        default: column.default.as_ref().map(lower_default).transpose()?,
    })
}

fn lower_data_type(data_type: &ConnectorDataType) -> Result<SqlType, ConnectorError> {
    Ok(match data_type {
        ConnectorDataType::Boolean => SqlType::Boolean,
        ConnectorDataType::TinyInt => SqlType::TinyInt,
        ConnectorDataType::SmallInt => SqlType::SmallInt,
        ConnectorDataType::Int => SqlType::Int,
        ConnectorDataType::BigInt => SqlType::BigInt,
        ConnectorDataType::LargeInt => SqlType::LargeInt,
        ConnectorDataType::Float => SqlType::Float,
        ConnectorDataType::Double => SqlType::Double,
        ConnectorDataType::Decimal { precision, scale } => SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        ConnectorDataType::String => SqlType::String,
        ConnectorDataType::Binary => SqlType::Binary,
        ConnectorDataType::Json => SqlType::Json,
        ConnectorDataType::Bitmap => SqlType::Bitmap,
        ConnectorDataType::Hll => SqlType::Hll,
        ConnectorDataType::Date => SqlType::Date,
        ConnectorDataType::DateTime => SqlType::DateTime,
        ConnectorDataType::DateTimeNs => SqlType::DateTimeNs,
        ConnectorDataType::Time => SqlType::Time,
        ConnectorDataType::Array(element) => SqlType::Array(Box::new(lower_data_type(element)?)),
        ConnectorDataType::Map(key, value) => SqlType::Map(
            Box::new(lower_data_type(key)?),
            Box::new(lower_data_type(value)?),
        ),
        ConnectorDataType::Struct(fields) => SqlType::Struct(
            fields
                .iter()
                .map(|field| Ok((field.name.to_string(), lower_data_type(&field.data_type)?)))
                .collect::<Result<_, ConnectorError>>()?,
        ),
        ConnectorDataType::Variant => SqlType::Variant,
    })
}

fn lower_default(
    value: &ConnectorDefaultValue,
) -> Result<crate::sql::parser::ast::DefaultLiteral, ConnectorError> {
    Ok(match value {
        ConnectorDefaultValue::Null => crate::sql::parser::ast::DefaultLiteral::Null,
        ConnectorDefaultValue::Bool(value) => crate::sql::parser::ast::DefaultLiteral::Bool(*value),
        ConnectorDefaultValue::Int(value) => crate::sql::parser::ast::DefaultLiteral::Int(*value),
        ConnectorDefaultValue::Float(value) => {
            crate::sql::parser::ast::DefaultLiteral::Float(*value)
        }
        ConnectorDefaultValue::Decimal { unscaled, scale } => {
            crate::sql::parser::ast::DefaultLiteral::Decimal {
                unscaled: *unscaled,
                scale: *scale,
            }
        }
        ConnectorDefaultValue::String(value) => {
            crate::sql::parser::ast::DefaultLiteral::String(value.to_string())
        }
        ConnectorDefaultValue::Date(value) => crate::sql::parser::ast::DefaultLiteral::Date(*value),
        ConnectorDefaultValue::DateTime(value) => {
            crate::sql::parser::ast::DefaultLiteral::DateTime(*value)
        }
        ConnectorDefaultValue::Binary(value) => {
            crate::sql::parser::ast::DefaultLiteral::Binary(value.to_vec())
        }
    })
}

fn lower_key(
    key: &novarocks_spi::connector::ConnectorTableKey,
) -> Result<crate::sql::parser::ast::TableKeyDesc, ConnectorError> {
    Ok(crate::sql::parser::ast::TableKeyDesc {
        kind: match key.kind {
            novarocks_spi::connector::ConnectorTableKeyKind::Duplicate => {
                crate::sql::parser::ast::TableKeyKind::Duplicate
            }
            novarocks_spi::connector::ConnectorTableKeyKind::Unique => {
                crate::sql::parser::ast::TableKeyKind::Unique
            }
            novarocks_spi::connector::ConnectorTableKeyKind::Aggregate => {
                crate::sql::parser::ast::TableKeyKind::Aggregate
            }
            novarocks_spi::connector::ConnectorTableKeyKind::Primary => {
                crate::sql::parser::ast::TableKeyKind::Primary
            }
        },
        columns: key.columns.iter().map(ToString::to_string).collect(),
    })
}

pub(crate) fn lower_partition(
    transform: &ConnectorPartitionTransform,
) -> Result<crate::sql::parser::ast::IcebergPartitionFieldExpr, ConnectorError> {
    use crate::sql::parser::ast::IcebergPartitionFieldExpr;
    Ok(match transform {
        ConnectorPartitionTransform::Identity { column } => IcebergPartitionFieldExpr::Identity {
            column: column.to_string(),
        },
        ConnectorPartitionTransform::Year { column } => IcebergPartitionFieldExpr::Year {
            column: column.to_string(),
        },
        ConnectorPartitionTransform::Month { column } => IcebergPartitionFieldExpr::Month {
            column: column.to_string(),
        },
        ConnectorPartitionTransform::Day { column } => IcebergPartitionFieldExpr::Day {
            column: column.to_string(),
        },
        ConnectorPartitionTransform::Hour { column } => IcebergPartitionFieldExpr::Hour {
            column: column.to_string(),
        },
        ConnectorPartitionTransform::Bucket {
            column,
            num_buckets,
        } => IcebergPartitionFieldExpr::Bucket {
            column: column.to_string(),
            num_buckets: *num_buckets,
        },
        ConnectorPartitionTransform::Truncate { column, width } => {
            IcebergPartitionFieldExpr::Truncate {
                column: column.to_string(),
                width: *width,
            }
        }
        ConnectorPartitionTransform::Void { column } => IcebergPartitionFieldExpr::Void {
            column: column.to_string(),
        },
    })
}

fn lower_property_changes(
    changes: &[novarocks_spi::connector::ConnectorPropertyChange],
) -> Result<crate::engine::statement::PropertiesOp, ConnectorError> {
    use novarocks_spi::connector::ConnectorPropertyChange;

    let Some(first) = changes.first() else {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "property mutation must contain at least one change",
        ));
    };
    match first {
        ConnectorPropertyChange::Set { .. } => {
            let entries = changes
                .iter()
                .map(|change| match change {
                    ConnectorPropertyChange::Set { key, value } => {
                        Ok((key.to_string(), value.to_string()))
                    }
                    ConnectorPropertyChange::Unset { .. } => Err(ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "mixed property set/unset mutations are not supported",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(crate::engine::statement::PropertiesOp::Set { entries })
        }
        ConnectorPropertyChange::Unset { if_exists, .. } => {
            let keys = changes
                .iter()
                .map(|change| match change {
                    ConnectorPropertyChange::Unset {
                        key,
                        if_exists: change_if_exists,
                    } if change_if_exists == if_exists => Ok(key.to_string()),
                    ConnectorPropertyChange::Unset { .. } => Err(ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "property unset mutations must share one existence policy",
                    )),
                    ConnectorPropertyChange::Set { .. } => Err(ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "mixed property set/unset mutations are not supported",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(crate::engine::statement::PropertiesOp::Unset {
                keys,
                if_exists: *if_exists,
            })
        }
    }
}

fn lower_schema_change(
    change: &novarocks_spi::connector::ConnectorSchemaChange,
) -> Result<crate::engine::statement::IcebergSchemaChange, ConnectorError> {
    use crate::engine::statement::{AddPosition, ColumnPath, IcebergSchemaChange};
    use novarocks_spi::connector::{
        ConnectorColumnPath, ConnectorColumnPosition, ConnectorSchemaChange,
    };

    fn path(path: &ConnectorColumnPath, allow_empty: bool) -> Result<ColumnPath, ConnectorError> {
        if path.segments.is_empty() && !allow_empty {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "schema mutation column path must not be empty",
            ));
        }
        Ok(ColumnPath::from_segments(
            path.segments.iter().map(ToString::to_string).collect(),
        ))
    }
    fn position(position: &ConnectorColumnPosition) -> AddPosition {
        match position {
            ConnectorColumnPosition::Default => AddPosition::Default,
            ConnectorColumnPosition::First => AddPosition::First,
            ConnectorColumnPosition::After { column } => AddPosition::After(column.to_string()),
            ConnectorColumnPosition::Before { column } => AddPosition::Before(column.to_string()),
        }
    }

    Ok(match change {
        ConnectorSchemaChange::AddColumn {
            parent,
            column,
            position: column_position,
        } => {
            let column = lower_column(column)?;
            IcebergSchemaChange::AddColumn {
                parent: path(parent, true)?,
                name: column.name,
                data_type: column.data_type,
                default: column.default,
                position: position(column_position),
            }
        }
        ConnectorSchemaChange::DropColumn { path: column_path } => {
            IcebergSchemaChange::DropColumn {
                path: path(column_path, false)?,
            }
        }
        ConnectorSchemaChange::RenameColumn {
            path: column_path,
            to,
        } => IcebergSchemaChange::RenameColumn {
            path: path(column_path, false)?,
            new_name: to.to_string(),
        },
        ConnectorSchemaChange::ModifyColumn {
            path: column_path,
            data_type,
        } => IcebergSchemaChange::ModifyColumn {
            path: path(column_path, false)?,
            new_type: lower_data_type(data_type)?,
        },
        ConnectorSchemaChange::SetColumnNullability {
            path: column_path,
            nullable,
        } => IcebergSchemaChange::SetNullable {
            path: path(column_path, false)?,
            nullable: *nullable,
        },
        ConnectorSchemaChange::ReorderColumn {
            path: column_path,
            position: column_position,
        } => IcebergSchemaChange::Reorder {
            path: path(column_path, false)?,
            position: position(column_position),
        },
        ConnectorSchemaChange::SetColumnComment {
            path: column_path,
            comment,
        } => IcebergSchemaChange::UpdateComment {
            path: path(column_path, false)?,
            comment: comment.to_string(),
        },
    })
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SnapshotMembershipKey {
    namespace: String,
    table: String,
    table_uuid: String,
    snapshot_id: i64,
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct SnapshotFileIdentity {
    path: String,
    size: i64,
    row_count: Option<i64>,
}

impl From<&IcebergDataFileInfo> for SnapshotFileIdentity {
    fn from(file: &IcebergDataFileInfo) -> Self {
        Self {
            path: file.path.clone(),
            size: file.size,
            row_count: file.row_count,
        }
    }
}

type SnapshotMembership = Result<Arc<HashSet<SnapshotFileIdentity>>, ConnectorError>;
type SnapshotMembershipCell = Arc<OnceLock<SnapshotMembership>>;

struct SnapshotMembershipCache {
    capacity: usize,
    state: Mutex<SnapshotMembershipCacheState>,
}

#[derive(Default)]
struct SnapshotMembershipCacheState {
    entries: HashMap<SnapshotMembershipKey, SnapshotMembershipCell>,
    order: VecDeque<SnapshotMembershipKey>,
}

impl SnapshotMembershipCache {
    fn new(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "snapshot membership cache capacity must be nonzero"
        );
        Self {
            capacity,
            state: Mutex::new(SnapshotMembershipCacheState::default()),
        }
    }

    fn cell(&self, key: &SnapshotMembershipKey) -> Result<SnapshotMembershipCell, ConnectorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| internal(format!("snapshot membership cache lock: {error}")))?;
        if let Some(cell) = state.entries.get(key).cloned() {
            state.order.retain(|cached| cached != key);
            state.order.push_back(key.clone());
            return Ok(cell);
        }
        while state.entries.len() >= self.capacity {
            let Some(evicted) = state.order.pop_front() else {
                return Err(internal(
                    "snapshot membership cache lost its eviction order".to_string(),
                ));
            };
            state.entries.remove(&evicted);
        }
        let cell = Arc::new(OnceLock::new());
        state.entries.insert(key.clone(), Arc::clone(&cell));
        state.order.push_back(key.clone());
        Ok(cell)
    }

    fn get_or_try_init(
        &self,
        key: SnapshotMembershipKey,
        load: impl FnOnce() -> SnapshotMembership,
    ) -> SnapshotMembership {
        let cell = self.cell(&key)?;
        let result = cell.get_or_init(load).clone();
        if result.is_err()
            && let Ok(mut state) = self.state.lock()
            && state
                .entries
                .get(&key)
                .is_some_and(|cached| Arc::ptr_eq(cached, &cell))
        {
            state.entries.remove(&key);
            state.order.retain(|cached| cached != &key);
        }
        result
    }

    fn insert(
        &self,
        key: SnapshotMembershipKey,
        membership: Arc<HashSet<SnapshotFileIdentity>>,
    ) -> Result<(), ConnectorError> {
        let cell = self.cell(&key)?;
        let _ = cell.set(Ok(membership));
        Ok(())
    }
}

impl ConnectorMetadata for IcebergControlProvider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn list_namespaces(
        &self,
        request: ConnectorListNamespacesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorNamespaceIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        list_namespaces(&entry)
            .map_err(map_iceberg_error)?
            .into_iter()
            .map(|namespace| {
                Ok(novarocks_spi::connector::ConnectorNamespaceIdentity {
                    instance_id: self.instance_id.clone(),
                    namespace: Arc::from(namespace),
                })
            })
            .collect()
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.namespace.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        super::catalog::namespace_exists(&entry, &request.namespace.namespace)
            .map_err(map_iceberg_error)
    }

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.table.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        let tables = list_tables(&entry, &request.table.namespace).map_err(map_iceberg_error)?;
        Ok(tables
            .iter()
            .any(|table| table.eq_ignore_ascii_case(&request.table.table)))
    }

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.namespace.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        list_tables(&entry, &request.namespace.namespace)
            .map_err(map_iceberg_error)?
            .into_iter()
            .map(|table| {
                Ok(novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: self.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    table: Arc::from(table),
                })
            })
            .collect()
    }

    fn read_reference_facts(
        &self,
        request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.table.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        let loaded = load_table(&entry, &request.table.namespace, &request.table.table)
            .map_err(map_iceberg_error)?;
        iceberg_read_reference_facts(loaded.table.metadata(), &request.context)
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.table.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        let requested_table = request.table.table.to_string();
        let (table_name, metadata_table_type) =
            resolve_table_request(&requested_table, request.resolution)?;
        let loaded =
            load_table(&entry, &request.table.namespace, &table_name).map_err(map_iceberg_error)?;
        let version = Some(Bytes::copy_from_slice(
            &loaded.table.metadata().current_schema_id().to_le_bytes(),
        ));
        let statistics_data_version = statistics_data_version(
            &loaded.table.metadata().uuid().to_string(),
            loaded.table.metadata().current_snapshot_id(),
        )?;
        let definition_schema = loaded.table.metadata().current_schema().clone();
        let table_comment = loaded.table.metadata().properties().get("comment").cloned();
        let table = build_table_payload(
            self.instance_id.as_str(),
            &request.table.namespace,
            &table_name,
            loaded,
            metadata_table_type,
        )?;
        let schema = if table.metadata_table_type.is_some() {
            self.metadata_schema(&table, &[])?
        } else {
            self.schema_for(
                &entry,
                &request.table.namespace,
                &table_name,
                &table.metadata_columns,
                &[],
            )?
        };
        let planning_facts = novarocks_connector_iceberg::planning_facts::table_planning_facts(
            novarocks_connector_iceberg::planning_facts::IcebergTablePlanningFactsInput {
                schema: &schema,
                // Metadata tables project a synthetic Arrow schema that has no
                // Iceberg column behind it, so they expose no write defaults.
                iceberg_schema: table
                    .metadata_table_type
                    .is_none()
                    .then(|| definition_schema.as_ref()),
                metadata_columns: &table.metadata_columns,
                hidden_columns: &table.hidden_columns,
                logical_type_columns: &table.logical_type_columns,
                serialized_metadata: table
                    .table_info
                    .as_ref()
                    .and_then(|info| info.serialized_metadata.as_deref()),
                namespace: &request.table.namespace,
                instance_id: &self.instance_id,
                context: &request.context,
            },
        )?;
        let definition_facts = if table.metadata_table_type.is_some() {
            novarocks_spi::connector::ConnectorTableDefinitionFacts::empty()
        } else {
            novarocks_connector_iceberg::table_definition::table_definition_facts(
                &definition_schema,
                &schema,
                &planning_facts,
                table_comment.as_deref(),
                &request.context,
            )?
        };
        Ok(ConnectorTableMetadata {
            identity: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: self.instance_id.clone(),
                namespace: request.table.namespace,
                table: Arc::from(table_name),
            },
            schema,
            planning_facts,
            definition_facts,
            version,
            statistics_data_version: Some(statistics_data_version),
            table: ConnectorTableHandle::try_new(
                self.instance_id.clone(),
                encode_payload(
                    &table,
                    "table handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
        })
    }
}

impl ConnectorViewMetadata for IcebergControlProvider {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn view_exists(&self, request: ConnectorViewRequest) -> Result<bool, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.view.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        views::view_exists(&entry, &request.view.namespace, &request.view.view)
            .map_err(map_iceberg_error)
    }

    fn load_view(
        &self,
        request: ConnectorViewRequest,
    ) -> Result<ConnectorViewMetadataValue, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.view.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        let loaded = views::load_view(&entry, &request.view.namespace, &request.view.view)
            .map_err(map_iceberg_error)?;
        iceberg_view_metadata_value(
            self.instance_id.clone(),
            request.view.namespace,
            request.view.view,
            loaded,
            &request.context,
        )
    }

    fn list_views(
        &self,
        request: ConnectorListViewsRequest,
    ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
        self.validate_context(&request.context)?;
        ensure_owner(&request.namespace.instance_id, &self.instance_id)?;
        let entry = self.entry(self.instance_id.as_str())?;
        views::list_views(&entry, &request.namespace.namespace)
            .map_err(map_iceberg_error)?
            .into_iter()
            .map(|view| {
                Ok(ConnectorViewIdentity {
                    instance_id: self.instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    view: Arc::from(view),
                })
            })
            .collect()
    }
}

fn iceberg_read_reference_facts(
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
    ConnectorReadReferenceFacts::try_new(
        metadata
            .snapshots()
            .map(|snapshot| snapshot.snapshot_id())
            .collect(),
        metadata
            .history()
            .iter()
            .map(|entry| ConnectorReadSnapshotLogEntry {
                snapshot_id: entry.snapshot_id,
                timestamp_millis: entry.timestamp_ms(),
            })
            .collect(),
        metadata
            .refs()
            .iter()
            .map(|(name, reference)| ConnectorReadNamedReference {
                name: Arc::from(name.as_str()),
                kind: if reference.is_branch() {
                    ConnectorReadReferenceKind::Branch
                } else {
                    ConnectorReadReferenceKind::Tag
                },
                snapshot_id: reference.snapshot_id,
            })
            .collect(),
        metadata.current_snapshot_id(),
        context,
    )
}

fn iceberg_view_metadata_value(
    instance_id: ConnectorInstanceId,
    namespace: Arc<str>,
    view: Arc<str>,
    loaded: views::LoadedIcebergView,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ConnectorViewMetadataValue, ConnectorError> {
    if !loaded
        .dialect
        .eq_ignore_ascii_case(views::VIEW_DIALECT_STARROCKS)
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "Iceberg view {namespace}.{view} uses unsupported SQL dialect {}",
                loaded.dialect
            ),
        ));
    }
    ConnectorViewMetadataValue::try_new(
        ConnectorViewIdentity {
            instance_id,
            namespace,
            view,
        },
        ConnectorViewDefinition {
            dialect: ConnectorViewDialect::StarRocks,
            sql: Arc::from(loaded.sql),
        },
        Arc::from(loaded.default_namespace),
        loaded.column_names.into_iter().map(Arc::from).collect(),
        loaded.comment.map(Arc::from),
        loaded
            .properties
            .into_iter()
            .map(|(key, value)| (Arc::from(key), Arc::from(value)))
            .collect(),
        context,
    )
}

impl StatisticsReader for IcebergControlProvider {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn read_statistics(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        self.validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let table_info = table.table_info.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics require a resolved base table payload",
            )
        })?;
        let expected_data_version = statistics_data_version(
            table_info.table_uuid.as_deref().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg table payload is missing its table UUID",
                )
            })?,
            table_info.current_snapshot_id,
        )?;
        if request.data_version != expected_data_version {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics request data version does not match its resolved table pin",
            ));
        }

        let snapshot_id = table_info.current_snapshot_id.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table has no current snapshot for statistics",
            )
        })?;
        // A metadata-only `update_statistics` leaves the Iceberg snapshot (and
        // therefore the data-version) unchanged.  Its registered Puffin path
        // is nevertheless a new immutable evidence revision, so derive the
        // resolver-cache key from it rather than from the snapshot alone.
        let entry = self.entry(self.instance_id.as_str())?;
        let loaded =
            load_table(&entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let metadata = loaded.table.metadata();
        let current_data_version =
            statistics_data_version(&metadata.uuid().to_string(), metadata.current_snapshot_id())?;
        if current_data_version != expected_data_version {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg table changed while its statistics evidence was loading",
            ));
        }
        let statistics_path = metadata
            .statistics_for_snapshot(snapshot_id)
            .map(|statistics| statistics.statistics_path.as_str());
        let evidence_path = statistics_path.unwrap_or("none");
        let evidence_revision = StatisticsEvidenceRevision::try_new(Bytes::from(format!(
            "iceberg/v1/{}/{snapshot_id}/{evidence_path}",
            table_info
                .table_uuid
                .as_deref()
                .expect("table UUID checked above"),
        )))?;
        if let Some(statistics_path) = statistics_path {
            let artifact = super::catalog::registry::block_on_iceberg(
                novarocks_connector_iceberg::stats_assembler::read_provider_statistics_blob(
                    loaded.table.file_io(),
                    statistics_path,
                ),
            )
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    format!("read Iceberg provider statistics runtime: {error}"),
                )
            })?
            .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
            if let Some(artifact) = artifact {
                let metrics = decode_provider_statistics(
                    &artifact,
                    &expected_data_version,
                    &request.metrics,
                )?;
                return Ok(StatisticsEvidence {
                    data_version: expected_data_version,
                    evidence_revision,
                    coverage: StatisticsCoverage::Full,
                    accuracy: StatisticsAccuracy::Exact,
                    interval: None,
                    provenance: StatisticsProvenance::ProviderArtifact,
                    metrics,
                });
            }
        }

        let base = super::stats::read_pinned_table_statistics(
            &self.registry,
            self.instance_id.as_str(),
            &table.namespace,
            &table.table,
            Some(snapshot_id),
        )
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("read Iceberg statistics: {error:?}"),
            )
        })?;
        // A manifest summary is authoritative only when it describes every
        // live data file and no delete file can change the visible row set.
        // `read_pinned_table_statistics` deliberately exposes only data-file
        // metrics to the optimizer, so retain the raw file set here to prove
        // that this is an append-only snapshot before upgrading its evidence.
        let manifest_files = extract_data_files_with_stats_at(&loaded.table, snapshot_id)
            .map_err(map_iceberg_error)?;
        let manifest_is_complete = manifest_evidence_is_complete(&base.row_count, &manifest_files);

        let mut metrics = BTreeMap::new();
        for metric in request.metrics.metrics() {
            let state = match metric {
                StatisticsMetric::RowCount => metric_state_u64(&base.row_count),
                StatisticsMetric::NullCount { column } => {
                    manifest_null_count(&manifest_files, column)
                }
                StatisticsMetric::Minimum { column } => base
                    .columns
                    .get(column.as_ref())
                    .map(|statistics| metric_state_f64(&statistics.min_value))
                    .unwrap_or_else(|| missing_column(column)),
                StatisticsMetric::Maximum { column } => base
                    .columns
                    .get(column.as_ref())
                    .map(|statistics| metric_state_f64(&statistics.max_value))
                    .unwrap_or_else(|| missing_column(column)),
                StatisticsMetric::AverageSize { column } => base
                    .columns
                    .get(column.as_ref())
                    .map(|statistics| metric_state_f64(&statistics.average_row_size))
                    .unwrap_or_else(|| missing_column(column)),
                StatisticsMetric::ThetaNdv { column } => base
                    .columns
                    .get(column.as_ref())
                    .map(|statistics| metric_state_f64(&statistics.ndv))
                    .unwrap_or_else(|| missing_column(column)),
            };
            metrics.insert(metric.clone(), state);
        }
        Ok(StatisticsEvidence {
            data_version: expected_data_version,
            evidence_revision,
            // Do not infer exactness from a manifest in the presence of
            // deletes. For an append-only snapshot, the manifest list is the
            // complete visible data-file set and its record counts are exact.
            coverage: if manifest_is_complete {
                StatisticsCoverage::Full
            } else {
                StatisticsCoverage::Subset
            },
            accuracy: if manifest_is_complete {
                StatisticsAccuracy::Exact
            } else {
                StatisticsAccuracy::Approximate
            },
            interval: None,
            provenance: StatisticsProvenance::Manifest,
            metrics,
        })
    }
}

impl StatisticsCollection for IcebergControlProvider {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn prepare_collection(
        &self,
        request: StatisticsCollectionRequest,
    ) -> Result<StatisticsCollectionPlan, ConnectorError> {
        self.validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let table_info = table.table_info.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics collection requires a resolved base table payload",
            )
        })?;
        let expected_data_version = statistics_data_version(
            table_info.table_uuid.as_deref().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg table payload is missing its table UUID",
                )
            })?,
            table_info.current_snapshot_id,
        )?;
        if request.data_version != expected_data_version {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics collection request does not match its resolved table pin",
            ));
        }
        let mut scan_projection = request
            .metrics
            .metrics()
            .iter()
            .filter_map(statistics_metric_column)
            .map(|column| {
                table_info
                    .schema
                    .fields
                    .iter()
                    .position(|field| field.name.eq_ignore_ascii_case(column))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            format!(
                                "Iceberg statistics metric column `{column}` is absent from the resolved schema"
                            ),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        scan_projection.sort_unstable();
        scan_projection.dedup();
        // The opaque payload is the provider's resolved table envelope. Core
        // may compile normal distributed scans from it but cannot reinterpret
        // catalog credentials or re-resolve latest metadata.
        let provider_payload = request.table.payload().clone();
        let evidence_revision = StatisticsEvidenceRevision::try_new(Bytes::from(format!(
            "iceberg/v1/{}/{}/collection/{}",
            table_info
                .table_uuid
                .as_deref()
                .expect("table UUID checked above"),
            table_info.current_snapshot_id.unwrap_or_default(),
            uuid::Uuid::from_bytes(request.operation_id.to_bytes()),
        )))?;
        let scan_columns = statistics_scan_layout(&table, &scan_projection)?;
        StatisticsCollectionPlan::try_new(
            request.table,
            request.data_version,
            evidence_revision,
            request.metrics,
            scan_columns,
            provider_payload,
        )
    }

    fn prepare_publish(
        &self,
        request: StatisticsPublishPreparationRequest,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        self.validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let table_info = table.table_info.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics publication requires a resolved base table payload",
            )
        })?;
        let expected_data_version = statistics_data_version(
            table_info.table_uuid.as_deref().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg table payload is missing its table UUID",
                )
            })?,
            table_info.current_snapshot_id,
        )?;
        if request.result.evidence.data_version != expected_data_version {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics publication does not match its resolved table pin",
            ));
        }
        let entry = self.entry(self.instance_id.as_str())?;
        let loaded =
            load_table(&entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let metadata = loaded.table.metadata();
        let current_data_version =
            statistics_data_version(&metadata.uuid().to_string(), metadata.current_snapshot_id())?;
        if current_data_version != expected_data_version {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg table changed after statistics collection; recompute before publishing",
            ));
        }
        let snapshot_id = metadata.current_snapshot_id().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cannot publish Iceberg statistics for a table without a snapshot",
            )
        })?;
        let statistics_path =
            novarocks_connector_iceberg::stats_assembler::puffin_path_for_statistics_operation(
                metadata,
                snapshot_id,
                request.operation_id.to_bytes(),
            );
        self.statistics_evidence(
            request.operation_id,
            &table,
            &expected_data_version,
            &statistics_path,
        )
    }

    fn publish_statistics(
        &self,
        request: StatisticsPublishRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        self.validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let Some(table_info) = table.table_info.as_ref() else {
            return Ok(statistics_known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics publication requires a resolved base table payload",
            )));
        };
        let expected_data_version = statistics_data_version(
            table_info.table_uuid.as_deref().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg table payload is missing its table UUID",
                )
            })?,
            table_info.current_snapshot_id,
        )?;
        if request.result.evidence.data_version != expected_data_version {
            return Ok(statistics_known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics publication does not match its resolved table pin",
            )));
        }
        if request.result.evidence.coverage != StatisticsCoverage::Full
            || request.result.evidence.accuracy != StatisticsAccuracy::Exact
            || request.result.evidence.provenance != StatisticsProvenance::VisibleRows
        {
            return Ok(statistics_known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics publication requires Full Exact visible-row evidence",
            )));
        }
        let provider_statistics = encode_provider_statistics(&request.result.evidence)?;
        let (artifact_version, theta) =
            crate::query_execution::statistics::decode_visible_row_artifact(
                request.result.provider_payload(),
            )
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("decode Core statistics artifact: {error}"),
                )
            })?;
        if artifact_version != expected_data_version {
            return Ok(statistics_known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Core statistics artifact does not match its resolved table pin",
            )));
        }

        let entry = self.entry(self.instance_id.as_str())?;
        let loaded =
            load_table(&entry, &table.namespace, &table.table).map_err(map_iceberg_error)?;
        let metadata = loaded.table.metadata();
        let current_data_version =
            statistics_data_version(&metadata.uuid().to_string(), metadata.current_snapshot_id())?;
        if current_data_version != expected_data_version {
            return Ok(statistics_known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg table changed after statistics collection; recompute before publishing",
            )));
        }
        let snapshot_id = metadata.current_snapshot_id().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "cannot publish Iceberg statistics for a table without a snapshot",
            )
        })?;
        let sequence_number = metadata
            .current_snapshot()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "cannot publish Iceberg statistics for a table without a current snapshot",
                )
            })?
            .sequence_number();
        let field_ids = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.to_ascii_lowercase(), field.id))
            .collect::<HashMap<_, _>>();
        let mut sketches = HashMap::new();
        for (column, partial) in theta {
            let field_id = field_ids.get(&column.to_ascii_lowercase()).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("statistics artifact column `{column}` is absent from the pinned Iceberg schema"),
                )
            })?;
            let (lg_k, theta, hashes) = partial.compact_parts();
            let sketch =
                novarocks_connector_iceberg::theta_sketch::ThetaSketchHandle::from_compact_parts(
                    lg_k, theta, hashes,
                )
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
            sketches.insert(*field_id, sketch);
        }
        let statistics_path =
            novarocks_connector_iceberg::stats_assembler::puffin_path_for_statistics_operation(
                metadata,
                snapshot_id,
                request.operation_id.to_bytes(),
            );
        let expected_evidence = self.statistics_evidence(
            request.operation_id,
            &table,
            &expected_data_version,
            &statistics_path,
        )?;
        if request.evidence != expected_evidence {
            return Ok(statistics_known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg statistics publication evidence does not match its pinned operation",
            )));
        }
        let written = super::catalog::registry::block_on_iceberg(
            novarocks_connector_iceberg::stats_assembler::write_puffin_with_provider_statistics(
                loaded.table.file_io(),
                &statistics_path,
                snapshot_id,
                sequence_number,
                &sketches,
                Some(&provider_statistics),
            ),
        )
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("write Iceberg statistics runtime: {error}"),
            )
        })?
        .map_err(map_iceberg_error)?;
        let Some(statistics_file) = written else {
            return self.statistics_receipt(
                request.operation_id,
                expected_data_version,
                request.result.evidence.evidence_revision,
                Bytes::from(statistics_path),
                ExternalMutationEffect::NoOp,
            );
        };
        let catalog =
            super::catalog::registry::build_iceberg_catalog(&entry).map_err(map_iceberg_error)?;
        match super::catalog::registry::block_on_iceberg(
            novarocks_connector_iceberg::commit::statistics::commit_statistics_file(
                &loaded.table,
                catalog.as_ref(),
                statistics_file,
            ),
        ) {
            Ok(Ok(())) => {
                // A statistics-only Iceberg commit changes metadata but not
                // the snapshot pin.  Invalidate the catalog cache so a
                // subsequent SHOW/optimizer read observes the newly
                // registered Puffin rather than a stale metadata envelope.
                entry.invalidate_table_cache(&table.namespace, &table.table);
                self.statistics_receipt(
                    request.operation_id,
                    expected_data_version,
                    request.result.evidence.evidence_revision,
                    Bytes::from(statistics_path),
                    ExternalMutationEffect::Applied,
                )
            }
            Ok(Err(error)) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Internal,
                    format!("commit Iceberg statistics: {error}"),
                ),
                evidence: request.evidence,
            }),
            Err(error) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Internal,
                    format!("commit Iceberg statistics runtime: {error}"),
                ),
                evidence: request.evidence,
            }),
        }
    }

    fn reconcile_statistics(
        &self,
        request: StatisticsReconcileRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        self.validate_context(&request.context)?;
        let evidence =
            decode_statistics_evidence(request.evidence.provider_payload()).map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("decode Iceberg statistics evidence: {error}"),
                )
            })?;
        if request.evidence.schema_version() != ICEBERG_STATISTICS_EVIDENCE_VERSION
            || evidence.version != ICEBERG_STATISTICS_EVIDENCE_VERSION
            || request.evidence.operation_kind() != ICEBERG_STATISTICS_OPERATION_KIND
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "unsupported Iceberg statistics reconciliation evidence",
            ));
        }
        let expected_data_version =
            StatisticsDataVersion::try_new(Bytes::from(evidence.data_version))?;
        let entry = self.entry(self.instance_id.as_str())?;
        let loaded =
            load_existing_table_for_reconcile(&entry, &evidence.namespace, &evidence.table)?;
        let Some(loaded) = loaded else {
            return Ok(statistics_known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table disappeared before statistics publication reconciled",
            )));
        };
        let metadata = loaded.table.metadata();
        let current_data_version =
            statistics_data_version(&metadata.uuid().to_string(), metadata.current_snapshot_id())?;
        if current_data_version != expected_data_version {
            return Ok(statistics_known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg table version changed before statistics publication could be reconciled",
            )));
        }
        let path_is_registered = metadata
            .statistics_iter()
            .any(|file| file.statistics_path == evidence.statistics_path);
        if path_is_registered {
            return self.statistics_receipt(
                request.evidence.operation_id(),
                expected_data_version,
                StatisticsEvidenceRevision::try_new(Bytes::from(format!(
                    "iceberg/statistics/v1/{}",
                    evidence.statistics_path
                )))?,
                Bytes::from(evidence.statistics_path),
                ExternalMutationEffect::Applied,
            );
        }
        Ok(statistics_known_uncommitted(ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg statistics artifact is not registered in table metadata",
        )))
    }
}

impl ConnectorStatistics for IcebergControlProvider {
    fn collection(&self) -> Option<&dyn StatisticsCollection> {
        Some(self)
    }
}

fn metric_state_u64(value: &StatValue<u64>) -> StatisticsMetricState {
    match value {
        StatValue::Known { value, .. } => {
            StatisticsMetricState::Available(StatisticsMetricValue::U64(*value))
        }
        StatValue::Missing { reason } => metric_missing(reason),
    }
}

fn metric_state_f64(value: &StatValue<f64>) -> StatisticsMetricState {
    match value {
        StatValue::Known { value, .. } => {
            StatisticsMetricState::Available(StatisticsMetricValue::F64(*value))
        }
        StatValue::Missing { reason } => metric_missing(reason),
    }
}

/// Return a null count only when every live data file reports one. This keeps
/// the manifest fast path exact without reconstructing a count from a lossy
/// floating-point null fraction.
fn manifest_null_count(
    files: &[novarocks_connector_iceberg::manifest::DataFileWithStats],
    column: &str,
) -> StatisticsMetricState {
    let total = files.iter().try_fold(0_u64, |total, file| {
        let count = file
            .column_stats
            .as_ref()?
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(column))?
            .1
            .null_count?;
        let count = u64::try_from(count).ok()?;
        total.checked_add(count)
    });
    match total {
        Some(total) => StatisticsMetricState::Available(StatisticsMetricValue::U64(total)),
        None => StatisticsMetricState::Missing(StatisticsMissing {
            kind: StatisticsMissingKind::IncompleteEvidence,
            message: Arc::from(format!(
                "Iceberg manifest does not report an exact null count for `{column}`"
            )),
        }),
    }
}

fn manifest_evidence_is_complete(
    row_count: &StatValue<u64>,
    files: &[novarocks_connector_iceberg::manifest::DataFileWithStats],
) -> bool {
    matches!(row_count, StatValue::Known { .. })
        && files.iter().all(|file| file.delete_files.is_empty())
}

fn missing_column(column: &str) -> StatisticsMetricState {
    StatisticsMetricState::Missing(StatisticsMissing {
        kind: StatisticsMissingKind::NotCollected,
        message: Arc::from(format!(
            "statistics for column `{column}` are not collected"
        )),
    })
}

fn metric_missing(reason: &StatsMissingReason) -> StatisticsMetricState {
    let (kind, message) = match reason {
        StatsMissingReason::NoCurrentSnapshot | StatsMissingReason::NoDataFiles => (
            StatisticsMissingKind::NotAvailableForVersion,
            format!("{reason:?}"),
        ),
        StatsMissingReason::StatsFileMissing => {
            (StatisticsMissingKind::NotCollected, format!("{reason:?}"))
        }
        StatsMissingReason::ManifestMissingRowCount | StatsMissingReason::ColumnNotReported(_) => (
            StatisticsMissingKind::IncompleteEvidence,
            format!("{reason:?}"),
        ),
        StatsMissingReason::ConnectorUnsupported(_) => (
            StatisticsMissingKind::UnsupportedMetric,
            format!("{reason:?}"),
        ),
        StatsMissingReason::CatalogLoadError(_) => (
            StatisticsMissingKind::CorruptArtifact,
            format!("{reason:?}"),
        ),
    };
    StatisticsMetricState::Missing(StatisticsMissing {
        kind,
        message: Arc::from(message),
    })
}

impl ConnectorScanPlanning for IcebergControlProvider {
    fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    fn begin_scan(
        &self,
        table: &ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
    ) -> Result<ConnectorScan, ConnectorError> {
        self.validate_context(&request.context)?;
        validate_static_predicates(&request.static_predicates)?;
        let mut table = self.table_payload(table)?;
        let entry = self.entry(self.instance_id.as_str())?;
        self.hydrate_frozen_rewrite_source(&entry, &mut table)?;
        if let Some(target_kind) = match request.purpose {
            ConnectorReadPurpose::MvTargetState => Some("target-state"),
            ConnectorReadPurpose::MvTargetLocator => Some("target-locator"),
            ConnectorReadPurpose::Query => None,
        } {
            let files = table
                .explicit_files
                .as_deref()
                .unwrap_or(&table.prepared_files);
            if files.iter().any(|file| {
                file.delete_files
                    .iter()
                    .any(|delete| delete.file_content == IcebergDeleteFileContent::Equality)
            }) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!("Iceberg {target_kind} scan does not support equality deletes yet"),
                ));
            }
        }
        let output_schema = match table
            .frozen_rewrite
            .as_ref()
            .map(|frozen| frozen.operation_kind.as_str())
        {
            Some(novarocks_spi::connector::REWRITE_POSITION_DELETES_KIND) => {
                rewrite_position_output_schema()
            }
            Some(novarocks_spi::connector::REWRITE_DATA_FILES_KIND) => self
                .frozen_data_rewrite_schema(
                    &entry,
                    &table.namespace,
                    &table.table,
                    &request.projection,
                )?,
            _ if table.metadata_table_type.is_some() => {
                self.metadata_schema(&table, &request.projection)?
            }
            _ => self.schema_for(
                &entry,
                &table.namespace,
                &table.table,
                &table.metadata_columns,
                &request.projection,
            )?,
        };
        let selector = match request.selection {
            ConnectorScanSelection::Snapshot(selector) => selector,
            ConnectorScanSelection::ChangeWindow(_) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "legacy Core Iceberg control does not admit change-window scans",
                ));
            }
        };
        let (snapshot_id, table_uuid) = if table.explicit_files.is_some() {
            (None, None)
        } else if matches!(selector, ConnectorReadSelector::Current) {
            let table_info = table.table_info.as_ref().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg current scan is missing its resolved table pin",
                )
            })?;
            (
                table_info.current_snapshot_id,
                table_info.table_uuid.clone(),
            )
        } else {
            let (snapshot_id, table_uuid) = self.select_snapshot(&entry, &table, selector)?;
            (snapshot_id, Some(table_uuid))
        };
        let (physical_predicates, predicate_dispositions) =
            negotiate_static_predicates(&table, &request.static_predicates);
        let fact_columns = scan_fact_columns(&output_schema, &request.projection, &table)?;
        let payload = ScanPayload {
            table,
            snapshot_id,
            table_uuid,
            projection: request.projection,
            limit: request.limit,
            physical_predicates,
            fact_columns,
        };
        ConnectorScan::try_new_snapshot(
            self.binding_key.clone(),
            selector,
            ConnectorScanHandle::try_new(
                self.instance_id.clone(),
                encode_payload(
                    &payload,
                    "scan handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
            output_schema,
            predicate_dispositions,
        )
    }

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        self.validate_context(&request.context)?;
        let scan = self.scan_payload(scan)?;
        if scan.table.metadata_table_type.is_some() {
            return self.plan_metadata_splits(scan, request);
        }
        let files = match (&scan.table.explicit_files, scan.snapshot_id) {
            (Some(files), _) => files.clone(),
            (None, None) => Vec::new(),
            (None, Some(snapshot_id)) => {
                let entry = self.entry(self.instance_id.as_str())?;
                let loaded = load_table(&entry, &scan.table.namespace, &scan.table.table)
                    .map_err(map_iceberg_error)?;
                let table_uuid = scan.table_uuid.as_deref().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg snapshot scan is missing its table incarnation",
                    )
                })?;
                if loaded.table.metadata().uuid().to_string() != table_uuid {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg scan belongs to a different table incarnation",
                    ));
                }
                extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                    .map_err(map_iceberg_error)?
                    .into_iter()
                    .map(novarocks_connector_iceberg::manifest::data_file_with_stats_to_iceberg_data_file_info)
                    .collect()
            }
        };
        if scan.table.frozen_rewrite.as_ref().is_some_and(|frozen| {
            frozen.operation_kind == novarocks_spi::connector::REWRITE_POSITION_DELETES_KIND
        }) {
            return self.plan_frozen_rewrite_position_splits(scan, files, request);
        }
        super::planning::validate_planned_files(scan.table.table_info.as_ref(), &files)?;
        if let Some(snapshot_id) = scan.snapshot_id {
            let table_uuid = scan.table_uuid.as_deref().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg snapshot scan is missing its table incarnation",
                )
            })?;
            self.snapshot_memberships.insert(
                SnapshotMembershipKey {
                    namespace: scan.table.namespace.clone(),
                    table: scan.table.table.clone(),
                    table_uuid: table_uuid.to_string(),
                    snapshot_id,
                },
                Arc::new(
                    files
                        .iter()
                        .map(SnapshotFileIdentity::from)
                        .collect::<HashSet<_>>(),
                ),
            )?;
        }
        // Cache the entire pinned snapshot before applying optional pruning.
        // Delete applicability must never depend on a predicate-selected view.
        let candidate_units_considered = files.len() as u64;
        let mut pruning_counters = super::file_pruning::IcebergFilePruningCounters::default();
        let files = files
            .into_iter()
            .filter(|file| {
                super::file_pruning::file_may_satisfy_physical_predicates(
                    file,
                    &scan.physical_predicates,
                    &mut pruning_counters,
                )
            })
            .collect::<Vec<_>>();
        let mut remaining = scan.limit;
        let mut leaves = Vec::new();
        let name_mapping = split_name_mapping(&scan.table)?;
        for file in files {
            if let Some(remaining_rows) = remaining.as_mut() {
                if *remaining_rows == 0 {
                    break;
                }
                if let Some(row_count) = file.row_count.and_then(|count| u64::try_from(count).ok())
                {
                    *remaining_rows = remaining_rows.saturating_sub(row_count);
                }
            }
            let estimated_bytes = u64::try_from(file.size).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("Iceberg data file {} has a negative size", file.path),
                )
            })?;
            leaves.push(IcebergFrozenScanUnitPayload {
                data_file: file,
                row_groups: None,
                estimated_bytes: Some(estimated_bytes),
            });
        }
        // Freeze row-group membership at the FE while the catalog snapshot and
        // object-store capability are still pinned. BE preparation preserves a
        // non-empty `row_groups` selection verbatim and never re-plans it.
        let planning_binding = self.planning_binding.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "Iceberg control generation has no composed filesystem planning resources",
            )
        })?;
        let leaves = materialize_local_scan_units(
            &planning_binding,
            leaves,
            false,
            &ConnectorPrepareSplitRequest {
                context: request.context.clone(),
            },
        )?;
        let scan_units_planned = u64::try_from(leaves.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg composite scan unit count overflows u64",
            )
        })?;
        let total_leaf_bytes = leaves
            .iter()
            .try_fold(0_u64, |total, leaf| {
                total.checked_add(leaf.estimated_bytes.expect("Iceberg leaf cost is known"))
            })
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg composite split cost overflowed",
                )
            })?;
        let derived_target = total_leaf_bytes
            .checked_add(request.target_parallelism.get() as u64 - 1)
            .and_then(|bytes| bytes.checked_div(request.target_parallelism.get() as u64))
            .unwrap_or(u64::MAX)
            .max(1);
        let target_bytes = request
            .max_split_bytes
            .map(NonZeroU64::get)
            .unwrap_or(derived_target);
        let hard_limit = request.max_split_bytes.map(NonZeroU64::get);
        let mut splits = Vec::new();
        let mut total_payload_bytes = 0usize;
        let mut pending = Vec::new();
        let mut pending_bytes = 0_u64;
        for leaf in leaves {
            let leaf_bytes = leaf.estimated_bytes.expect("Iceberg leaf cost is known");
            if hard_limit.is_some_and(|limit| leaf_bytes > limit) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    format!(
                        "Iceberg physical leaf {} exceeds explicit split byte limit {target_bytes}",
                        leaf.data_file.path
                    ),
                ));
            }
            let exceeds_target = !pending.is_empty()
                && pending_bytes
                    .checked_add(leaf_bytes)
                    .is_none_or(|bytes| bytes > target_bytes);
            if exceeds_target
                || pending.len()
                    >= novarocks_spi::connector::MAX_CONNECTOR_PREPARED_SCAN_UNITS_PER_SPLIT
            {
                let estimated_bytes = pending_bytes;
                let payload = SplitPayload {
                    version: ICEBERG_SPLIT_V5,
                    owner_instance_id: self.instance_id.as_str().to_string(),
                    incarnation: self.incarnation.to_bytes(),
                    namespace: scan.table.namespace.clone(),
                    table: scan.table.table.clone(),
                    snapshot_id: scan.snapshot_id,
                    table_uuid: scan.table_uuid.clone(),
                    schema_id: scan.table.table_info.as_ref().map(|table| table.schema_id),
                    units: std::mem::take(&mut pending),
                    projection: scan.projection.clone(),
                    limit: scan.limit,
                    physical_predicates: scan.physical_predicates.clone(),
                    fact_columns: scan.fact_columns.clone(),
                    name_mapping: name_mapping.clone(),
                    delta: None,
                    distributed_rewrite_position: None,
                    metadata: None,
                };
                let index = splits.len();
                push_split_with_budget(
                    &mut splits,
                    &mut total_payload_bytes,
                    self.instance_id.clone(),
                    format!(
                        "{}-{index}",
                        scan.snapshot_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "explicit".to_string())
                    ),
                    &payload,
                    Some(estimated_bytes),
                    &request.context,
                )?;
                pending_bytes = 0;
            }
            pending_bytes = pending_bytes.checked_add(leaf_bytes).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg composite split cost overflowed",
                )
            })?;
            pending.push(leaf);
        }
        if !pending.is_empty() {
            let index = splits.len();
            let payload = SplitPayload {
                version: ICEBERG_SPLIT_V5,
                owner_instance_id: self.instance_id.as_str().to_string(),
                incarnation: self.incarnation.to_bytes(),
                namespace: scan.table.namespace.clone(),
                table: scan.table.table.clone(),
                snapshot_id: scan.snapshot_id,
                table_uuid: scan.table_uuid.clone(),
                schema_id: scan.table.table_info.as_ref().map(|table| table.schema_id),
                units: pending,
                projection: scan.projection.clone(),
                limit: scan.limit,
                physical_predicates: scan.physical_predicates.clone(),
                fact_columns: scan.fact_columns.clone(),
                name_mapping: name_mapping.clone(),
                delta: None,
                distributed_rewrite_position: None,
                metadata: None,
            };
            push_split_with_budget(
                &mut splits,
                &mut total_payload_bytes,
                self.instance_id.clone(),
                format!(
                    "{}-{index}",
                    scan.snapshot_id
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "explicit".to_string())
                ),
                &payload,
                Some(pending_bytes),
                &request.context,
            )?;
        }
        let composite_splits_planned = splits.len() as u64;
        ConnectorSplitPlanningResult::try_new(
            splits,
            ConnectorSplitPlanningMetrics {
                candidate_units_considered,
                candidate_units_pruned: u64::try_from(pruning_counters.files_pruned)
                    .unwrap_or(u64::MAX)
                    .min(candidate_units_considered),
                composite_splits_planned,
                scan_units_planned,
            },
        )
    }
}

impl IcebergControlProvider {
    fn metadata_schema(
        &self,
        table: &TablePayload,
        projection: &[usize],
    ) -> Result<SchemaRef, ConnectorError> {
        let metadata_table_type = table.metadata_table_type.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata schema requires a metadata table type",
            )
        })?;
        let table_info = table.table_info.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg metadata alias is missing its frozen table information",
            )
        })?;
        let columns =
            metadata_columns_for_table(sql_metadata_table_kind(metadata_table_type), table_info)
                .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
        let fields = columns
            .into_iter()
            .map(|column| Field::new(column.name, column.data_type, column.nullable))
            .collect::<Vec<_>>();
        let fields = if projection.is_empty() {
            fields
        } else {
            projection
                .iter()
                .map(|index| {
                    fields.get(*index).cloned().ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            format!(
                                "metadata projection index {index} is outside the visible schema"
                            ),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(Arc::new(Schema::new(fields)))
    }

    fn plan_metadata_splits(
        &self,
        scan: ScanPayload,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        let metadata_table_type = scan.table.metadata_table_type.clone().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata split planning requires a metadata table type",
            )
        })?;
        let table = scan.table.table_info.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg metadata split is missing frozen table information",
            )
        })?;
        let serialized_table = table.serialized_metadata.clone().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg metadata split is missing serialized table metadata",
            )
        })?;
        let serialized_payload = match &metadata_table_type {
            super::metadata::IcebergMetadataTableType::Files
            | super::metadata::IcebergMetadataTableType::Manifests
            | super::metadata::IcebergMetadataTableType::LogicalIcebergMetadata => {
                table.serialized_metadata_rows.clone().ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg metadata split is missing its frozen metadata rows",
                    )
                })?
            }
            super::metadata::IcebergMetadataTableType::Snapshots
            | super::metadata::IcebergMetadataTableType::History
            | super::metadata::IcebergMetadataTableType::Refs => String::new(),
            super::metadata::IcebergMetadataTableType::Partitions => {
                Self::metadata_payload(&metadata_table_type, table, &scan.table.prepared_files)
                    .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?
                    .unwrap_or_default()
            }
        };
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V5,
            owner_instance_id: self.instance_id.as_str().to_string(),
            incarnation: self.incarnation.to_bytes(),
            namespace: scan.table.namespace,
            table: scan.table.table,
            snapshot_id: scan.snapshot_id,
            table_uuid: scan.table_uuid,
            schema_id: table.schema_id.into(),
            units: Vec::new(),
            projection: scan.projection,
            limit: scan.limit,
            physical_predicates: Vec::new(),
            fact_columns: Vec::new(),
            name_mapping: None,
            delta: None,
            distributed_rewrite_position: None,
            metadata: Some(IcebergMetadataSplitPayloadV1 {
                metadata_table_type: super::metadata::provider_metadata_table_type(
                    metadata_table_type,
                ),
                serialized_table,
                serialized_payload,
            }),
        };
        let mut splits = Vec::new();
        let mut total_payload_bytes = 0;
        push_split_with_budget(
            &mut splits,
            &mut total_payload_bytes,
            self.instance_id.clone(),
            "iceberg-metadata-0".to_string(),
            &payload,
            None,
            &request.context,
        )?;
        ConnectorSplitPlanningResult::try_new(
            splits,
            ConnectorSplitPlanningMetrics {
                candidate_units_considered: 1,
                candidate_units_pruned: 0,
                composite_splits_planned: 1,
                scan_units_planned: 1,
            },
        )
    }

    fn metadata_payload(
        kind: &super::metadata::IcebergMetadataTableType,
        table: &IcebergTableInfo,
        files: &[IcebergDataFileInfo],
    ) -> Result<Option<String>, String> {
        match kind {
            super::metadata::IcebergMetadataTableType::Partitions => {
                let mut groups = std::collections::BTreeMap::<
                    (i32, String),
                    (
                        i64,
                        i64,
                        i64,
                        std::collections::BTreeSet<String>,
                        std::collections::BTreeSet<String>,
                    ),
                >::new();
                for file in files {
                    let spec_id = file.partition_spec_id.ok_or_else(|| {
                    format!(
                        "iceberg partitions metadata requires partition spec id for data file {}",
                        file.path
                    )
                })?;
                    let rows = file.row_count.ok_or_else(|| {
                        format!(
                            "iceberg partitions metadata requires record_count for data file {}",
                            file.path
                        )
                    })?;
                    let entry = groups
                        .entry((
                            spec_id,
                            file.partition_key
                                .clone()
                                .unwrap_or_else(|| "Struct([])".to_string()),
                        ))
                        .or_default();
                    entry.0 = entry.0.checked_add(rows).ok_or_else(|| {
                        "iceberg partitions metadata record_count overflow".to_string()
                    })?;
                    entry.1 = entry.1.checked_add(1).ok_or_else(|| {
                        "iceberg partitions metadata file_count overflow".to_string()
                    })?;
                    entry.2 = entry.2.checked_add(file.size).ok_or_else(|| {
                        "iceberg partitions metadata total_data_file_size_in_bytes overflow"
                            .to_string()
                    })?;
                    for delete in &file.delete_files {
                        match delete.file_content {
                            IcebergDeleteFileContent::Position => {
                                entry.3.insert(delete.path.clone());
                            }
                            IcebergDeleteFileContent::Equality => {
                                entry.4.insert(delete.path.clone());
                            }
                        }
                    }
                }
                let rows = groups.into_iter().map(|((spec_id, partition), (record_count, file_count, total_data_file_size_in_bytes, position_delete_files, equality_delete_files))| {
                Ok(serde_json::json!({
                    "spec_id": spec_id,
                    "partition": partition,
                    "record_count": record_count,
                    "file_count": file_count,
                    "total_data_file_size_in_bytes": total_data_file_size_in_bytes,
                    "position_delete_file_count": i64::try_from(position_delete_files.len()).map_err(|_| "iceberg partitions metadata position_delete_file_count overflow".to_string())?,
                    "equality_delete_file_count": i64::try_from(equality_delete_files.len()).map_err(|_| "iceberg partitions metadata equality_delete_file_count overflow".to_string())?,
                }))
            }).collect::<Result<Vec<_>, String>>()?;
                serde_json::to_string(&serde_json::json!({ "version": 1, "rows": rows }))
                    .map(Some)
                    .map_err(|error| {
                        format!("serialize iceberg partitions metadata payload failed: {error}")
                    })
            }
            super::metadata::IcebergMetadataTableType::Files
            | super::metadata::IcebergMetadataTableType::Manifests
            | super::metadata::IcebergMetadataTableType::LogicalIcebergMetadata => table
                .serialized_metadata_rows
                .clone()
                .map(Some)
                .ok_or_else(|| {
                    "iceberg metadata rows were not resolved at catalog lookup time".to_string()
                }),
            super::metadata::IcebergMetadataTableType::Snapshots
            | super::metadata::IcebergMetadataTableType::History
            | super::metadata::IcebergMetadataTableType::Refs => Ok(None),
        }
    }

    fn plan_frozen_rewrite_position_splits(
        &self,
        scan: ScanPayload,
        files: Vec<IcebergDataFileInfo>,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        let frozen = scan.table.frozen_rewrite.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg rewrite-position scan is missing its frozen source",
            )
        })?;
        let entry = self.entry(self.instance_id.as_str())?;
        let loaded = load_table(&entry, &scan.table.namespace, &scan.table.table)
            .map_err(map_iceberg_error)?;
        let group = super::distributed_rewrite::load_frozen_rewrite_group(
            loaded.table.file_io(),
            &frozen.group,
        )?;
        if group.data_files.len() != 1 || group.selected_position_delete_files.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg rewrite-position artifact group is invalid",
            ));
        }
        let file = group
            .data_files
            .into_iter()
            .next()
            .expect("checked exactly one file");
        if files.len() != 1 || files[0].path != file.path {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg rewrite-position scan does not match its frozen input",
            ));
        }
        let selected = file
            .delete_files
            .iter()
            .filter(|delete| group.selected_position_delete_files.contains(&delete.path))
            .cloned()
            .collect::<Vec<_>>();
        if selected.len() != group.selected_position_delete_files.len()
            || selected.iter().any(|delete| {
                !matches!(delete.file_content, IcebergDeleteFileContent::Position)
                    || !matches!(delete.file_format, IcebergDeleteFileFormat::Puffin)
                    || delete.content_offset.is_none()
                    || delete.content_size_in_bytes.is_none()
            })
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg rewrite-position artifact selects invalid Puffin files",
            ));
        }
        let estimated_bytes = u64::try_from(file.size).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg rewrite-position source has a negative size",
            )
        })?;
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V5,
            owner_instance_id: self.instance_id.as_str().to_string(),
            incarnation: self.incarnation.to_bytes(),
            namespace: scan.table.namespace,
            table: scan.table.table,
            snapshot_id: None,
            table_uuid: None,
            schema_id: None,
            units: vec![IcebergFrozenScanUnitPayload {
                estimated_bytes: Some(estimated_bytes),
                data_file: file,
                row_groups: None,
            }],
            projection: Vec::new(),
            limit: None,
            physical_predicates: Vec::new(),
            fact_columns: Vec::new(),
            name_mapping: None,
            delta: None,
            distributed_rewrite_position: Some(IcebergRewritePositionSplitPayloadV1 {
                version: ICEBERG_REWRITE_POSITION_SPLIT_V1,
                selected_delete_files: selected,
            }),
            metadata: None,
        };
        let mut splits = Vec::new();
        let mut payload_bytes = 0usize;
        push_split_with_budget(
            &mut splits,
            &mut payload_bytes,
            self.instance_id.clone(),
            format!("rewrite-position-{}", frozen.group.group_digest_hex),
            &payload,
            Some(estimated_bytes),
            &request.context,
        )?;
        ConnectorSplitPlanningResult::try_new(
            splits,
            ConnectorSplitPlanningMetrics {
                candidate_units_considered: 1,
                candidate_units_pruned: 0,
                composite_splits_planned: 1,
                scan_units_planned: 1,
            },
        )
    }
}

fn negotiate_static_predicates(
    table: &TablePayload,
    predicates: &[ConnectorStaticPredicate],
) -> (
    Vec<IcebergPhysicalPredicate>,
    Vec<ConnectorPredicateDisposition>,
) {
    // Metadata and delta scans deliberately keep their Core residuals in V1.
    // Ordinary table data scans are safe to use manifest statistics and
    // Parquet row-group metadata as a pruning-only optimization.
    let table_info = (table.metadata_table_type.is_none())
        .then_some(table.table_info.as_ref())
        .flatten();
    let mut physical_predicates = Vec::new();
    let mut dispositions = Vec::with_capacity(predicates.len());
    for predicate in predicates {
        let physical = table_info.and_then(|table_info| {
            let field = table_info
                .schema
                .fields
                .get(predicate.column.field_ordinal as usize)?;
            static_predicate_to_physical(predicate, field.field_id, &field.name)
        });
        let kind = if let Some(predicate) = physical {
            physical_predicates.push(predicate);
            ConnectorPredicateDispositionKind::PruningOnly
        } else {
            ConnectorPredicateDispositionKind::Unsupported
        };
        dispositions.push(ConnectorPredicateDisposition {
            predicate_id: predicate.id,
            kind,
        });
    }
    (physical_predicates, dispositions)
}

fn static_predicate_to_physical(
    predicate: &ConnectorStaticPredicate,
    field_id: i32,
    column: &str,
) -> Option<IcebergPhysicalPredicate> {
    use ConnectorScalarType::{Boolean, Date32, Int32, Int64};

    let value = |literal: &ConnectorScalarValue| match literal {
        ConnectorScalarValue::Boolean(value) if predicate.column.data_type == Boolean => {
            Some(IcebergPhysicalPredicateValue::Boolean(*value))
        }
        ConnectorScalarValue::Int32(value) if predicate.column.data_type == Int32 => {
            Some(IcebergPhysicalPredicateValue::Int32(*value))
        }
        ConnectorScalarValue::Int64(value) if predicate.column.data_type == Int64 => {
            Some(IcebergPhysicalPredicateValue::Int64(*value))
        }
        ConnectorScalarValue::Date32(value) if predicate.column.data_type == Date32 => {
            Some(IcebergPhysicalPredicateValue::Date32(*value))
        }
        _ => None,
    };
    let domain = match &predicate.kind {
        ConnectorStaticPredicateKind::Comparison { op, literal } => {
            let op = match op {
                ConnectorStaticComparisonOp::Eq => IcebergPhysicalPredicateOp::Eq,
                ConnectorStaticComparisonOp::Lt => IcebergPhysicalPredicateOp::Lt,
                ConnectorStaticComparisonOp::Le => IcebergPhysicalPredicateOp::Le,
                ConnectorStaticComparisonOp::Gt => IcebergPhysicalPredicateOp::Gt,
                ConnectorStaticComparisonOp::Ge => IcebergPhysicalPredicateOp::Ge,
                ConnectorStaticComparisonOp::Ne => return None,
                _ => return None,
            };
            IcebergPhysicalPredicateDomain::Range {
                op,
                value: value(literal)?,
            }
        }
        ConnectorStaticPredicateKind::In { literals } => {
            let values = literals.iter().map(value).collect::<Option<Vec<_>>>()?;
            if values.is_empty() {
                return None;
            }
            IcebergPhysicalPredicateDomain::DiscreteSet { values }
        }
        ConnectorStaticPredicateKind::IsNull | ConnectorStaticPredicateKind::IsNotNull => {
            return None;
        }
        _ => return None,
    };
    Some(IcebergPhysicalPredicate {
        field_id,
        column: column.to_string(),
        domain,
    })
}

fn resolve_table_request(
    requested_table: &str,
    resolution: ConnectorTableResolution,
) -> Result<(String, Option<super::IcebergMetadataTableType>), ConnectorError> {
    let alias = requested_table
        .rsplit_once('$')
        .and_then(|(table, suffix)| {
            super::IcebergMetadataTableType::parse(suffix)
                .ok()
                .map(|metadata_type| (table.to_string(), metadata_type))
        });
    match (resolution, alias) {
        (ConnectorTableResolution::StrictBaseTable, Some(_)) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "strict Iceberg table resolution does not accept metadata aliases",
        )),
        (ConnectorTableResolution::StrictBaseTable, None) => {
            Ok((requested_table.to_string(), None))
        }
        (ConnectorTableResolution::ProviderReadAlias, Some((table, metadata_type))) => {
            Ok((table, Some(metadata_type)))
        }
        (ConnectorTableResolution::ProviderReadAlias, None) => Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg provider read alias must use `<table>$<metadata-type>`",
        )),
    }
}

fn build_table_payload(
    catalog: &str,
    namespace: &str,
    table: &str,
    loaded: super::catalog::IcebergLoadedTable,
    metadata_table_type: Option<super::IcebergMetadataTableType>,
) -> Result<TablePayload, ConnectorError> {
    let logical_type_columns = loaded
        .logical_types
        .iter()
        .filter_map(|(column_name, logical_type)| match logical_type {
            SqlType::Bitmap => Some((column_name.to_lowercase(), "bitmap".to_string())),
            SqlType::Hll => Some((column_name.to_lowercase(), "hll".to_string())),
            _ => None,
        })
        .collect();
    let mut prepared_files = Vec::new();
    let metadata_table = loaded.table.clone();
    let metadata_file_io = metadata_table.file_io().clone();
    if matches!(
        metadata_table_type,
        Some(super::IcebergMetadataTableType::Partitions)
    ) {
        let snapshot_id = loaded.table.metadata().current_snapshot_id();
        let data_files = match snapshot_id {
            Some(snapshot_id) => extract_data_files_with_stats_at(&loaded.table, snapshot_id)
                .map_err(map_iceberg_error)?,
            None => Vec::new(),
        };
        prepared_files = data_files
            .iter()
            .cloned()
            .map(novarocks_connector_iceberg::manifest::data_file_with_stats_to_iceberg_data_file_info)
            .collect();
    }
    let mut table_info = novarocks_connector_iceberg::scan_model::IcebergTableInfo {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
        table_uuid: Some(metadata_table.metadata().uuid().to_string()),
        current_snapshot_id: metadata_table.metadata().current_snapshot_id(),
        schema_id: metadata_table.metadata().current_schema_id(),
        location: metadata_table.metadata().location().to_string(),
        schema: iceberg_schema_def(metadata_table.metadata().current_schema()),
        serialized_metadata: Some(
            serde_json::to_string(metadata_table.metadata())
                .map_err(|error| internal(format!("serialize Iceberg table metadata: {error}")))?,
        ),
        serialized_metadata_rows: None,
    };
    if matches!(
        metadata_table_type,
        Some(
            super::IcebergMetadataTableType::Files
                | super::IcebergMetadataTableType::Manifests
                | super::IcebergMetadataTableType::LogicalIcebergMetadata
        )
    ) {
        let rows = super::catalog::registry::block_on_iceberg(async {
            novarocks_connector_iceberg::metadata_read::read_metadata_table_rows(
                &metadata_table,
                &metadata_file_io,
                provider_metadata_table_type(
                    metadata_table_type
                        .as_ref()
                        .expect("metadata table type is present"),
                )?,
            )
            .await
        })
        .map_err(map_iceberg_error)?
        .map_err(map_iceberg_error)?;
        table_info.serialized_metadata_rows = Some(rows);
    }
    let hidden_columns = super::catalog::backend::hidden_internal_column_names_from_metadata(
        metadata_table.metadata(),
    );
    let metadata_columns = iceberg_metadata_column_names(metadata_table.metadata());
    Ok(TablePayload {
        namespace: namespace.to_string(),
        table: table.to_string(),
        table_info: Some(table_info),
        metadata_columns,
        metadata_table_type,
        prepared_files,
        explicit_files: None,
        logical_type_columns,
        hidden_columns,
        frozen_rewrite: None,
    })
}

fn provider_metadata_table_type(
    ty: &super::IcebergMetadataTableType,
) -> Result<novarocks_connector_iceberg::metadata_read::MetadataTableType, String> {
    match ty {
        super::IcebergMetadataTableType::Files => {
            Ok(novarocks_connector_iceberg::metadata_read::MetadataTableType::Files)
        }
        super::IcebergMetadataTableType::Manifests => {
            Ok(novarocks_connector_iceberg::metadata_read::MetadataTableType::Manifests)
        }
        super::IcebergMetadataTableType::LogicalIcebergMetadata => Ok(
            novarocks_connector_iceberg::metadata_read::MetadataTableType::LogicalIcebergMetadata,
        ),
        other => Err(format!("provider metadata walk does not support {other:?}")),
    }
}

/// Project only the SQL-visible Iceberg pseudo-column names into the provider
/// payload.  This is intentionally independent of a planner `TableDef`: the
/// connector owns its metadata envelope and application query planning later
/// assigns the request-local scan token.
fn iceberg_metadata_column_names(
    metadata: &novarocks_connector_iceberg::iceberg::spec::TableMetadata,
) -> Vec<String> {
    let mut names = vec!["_file".to_string(), "_pos".to_string()];
    if matches!(
        metadata.format_version(),
        novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3
    ) && !metadata
        .properties()
        .get("write.row-lineage")
        .is_some_and(|value| value.eq_ignore_ascii_case("false"))
    {
        names.push("_row_id".to_string());
        names.push("_last_updated_sequence_number".to_string());
    }
    names
}

fn iceberg_metadata_arrow_fields(names: &[String]) -> Result<Vec<Arc<Field>>, ConnectorError> {
    names
        .iter()
        .map(|name| {
            let (data_type, nullable) = match name.as_str() {
                "_file" => (DataType::Utf8, false),
                "_pos" => (DataType::Int64, false),
                "_row_id" => (DataType::Int64, false),
                "_last_updated_sequence_number" => (DataType::Int64, true),
                other => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        format!("unknown Iceberg metadata column `{other}`"),
                    ));
                }
            };
            Ok(Arc::new(
                Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
                    novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
                    "true".to_string(),
                )])),
            ))
        })
        .collect()
}

fn metadata_columns_for_table(
    kind: crate::sql::planner::table::SqlMetadataTableKind,
    table: &novarocks_connector_iceberg::scan_model::IcebergTableInfo,
) -> Result<Vec<crate::sql::analyzer::iceberg_metadata::MetadataColumn>, String> {
    let mut columns = crate::sql::analyzer::iceberg_metadata::metadata_table_schema(kind);
    if matches!(
        kind,
        crate::sql::planner::table::SqlMetadataTableKind::Files
            | crate::sql::planner::table::SqlMetadataTableKind::LogicalIcebergMetadata
    ) {
        let partition = partition_struct_type(table)?;
        let column = columns
            .iter_mut()
            .find(|column| column.name.eq_ignore_ascii_case("partition"))
            .ok_or_else(|| "Iceberg metadata schema is missing partition column".to_string())?;
        column.data_type = partition;
    }
    Ok(columns)
}

fn iceberg_type_to_arrow_type(ty: &Type) -> Result<DataType, String> {
    match ty {
        Type::Primitive(primitive) => Ok(match primitive {
            PrimitiveType::Boolean => DataType::Boolean,
            PrimitiveType::Int => DataType::Int32,
            PrimitiveType::Long => DataType::Int64,
            PrimitiveType::Float => DataType::Float32,
            PrimitiveType::Double => DataType::Float64,
            PrimitiveType::Decimal { precision, scale } => DataType::Decimal128(
                u8::try_from(*precision)
                    .map_err(|_| format!("iceberg decimal precision out of range: {precision}"))?,
                i8::try_from(*scale)
                    .map_err(|_| format!("iceberg decimal scale out of range: {scale}"))?,
            ),
            PrimitiveType::Date => DataType::Date32,
            PrimitiveType::Time => DataType::Time64(TimeUnit::Microsecond),
            PrimitiveType::Timestamp | PrimitiveType::Timestamptz => {
                DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            PrimitiveType::TimestampNs | PrimitiveType::TimestamptzNs => {
                DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            PrimitiveType::String | PrimitiveType::Uuid => DataType::Utf8,
            PrimitiveType::Fixed(width) => DataType::FixedSizeBinary(
                i32::try_from(*width)
                    .map_err(|_| format!("iceberg fixed width out of range: {width}"))?,
            ),
            PrimitiveType::Binary | PrimitiveType::Variant => DataType::Binary,
        }),
        other => Err(format!(
            "iceberg metadata partition field must be primitive, got {other:?}"
        )),
    }
}

fn partition_source_type(metadata: &TableMetadata, source_id: i32) -> Option<&Type> {
    metadata
        .current_schema()
        .field_by_id(source_id)
        .map(|field| field.field_type.as_ref())
        .or_else(|| {
            metadata.schemas_iter().find_map(|schema| {
                schema
                    .field_by_id(source_id)
                    .map(|field| field.field_type.as_ref())
            })
        })
}

fn partition_struct_type(
    table: &novarocks_connector_iceberg::scan_model::IcebergTableInfo,
) -> Result<DataType, String> {
    let serialized = table.serialized_metadata.as_deref().ok_or_else(|| {
        format!(
            "iceberg metadata table {}.{} requires serialized metadata to type partition struct",
            table.namespace, table.table
        )
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized).map_err(|error| {
        format!("parse iceberg table metadata for partition schema failed: {error}")
    })?;
    let mut specs = metadata.partition_specs_iter().cloned().collect::<Vec<_>>();
    specs.sort_by_key(|spec| spec.spec_id());
    let mut fields: Vec<Arc<Field>> = Vec::new();
    for spec in specs {
        for partition_field in spec.fields() {
            let source_type = partition_source_type(&metadata, partition_field.source_id)
                .ok_or_else(|| {
                    format!(
                        "iceberg partition field {} references missing source field id {}",
                        partition_field.name, partition_field.source_id
                    )
                })?;
            let result_type =
                partition_field
                    .transform
                    .result_type(source_type)
                    .map_err(|error| {
                        format!(
                            "infer iceberg partition field {} type: {error}",
                            partition_field.name
                        )
                    })?;
            let arrow_type = iceberg_type_to_arrow_type(&result_type)?;
            if let Some(existing) = fields
                .iter()
                .find(|field| field.name().eq_ignore_ascii_case(&partition_field.name))
            {
                if existing.data_type() != &arrow_type {
                    return Err(format!(
                        "iceberg partition field {} has incompatible types across specs: {:?} vs {:?}",
                        partition_field.name,
                        existing.data_type(),
                        arrow_type
                    ));
                }
                continue;
            }
            fields.push(Arc::new(Field::new(
                partition_field.name.clone(),
                arrow_type,
                true,
            )));
        }
    }
    Ok(DataType::Struct(Fields::from(fields)))
}

/// The scan-model projection builder preserves Iceberg field IDs, while the
/// connector owns whether a field is visible to SQL.  Carry provider-owned
/// hidden-field metadata through that projection so control consumers such as
/// ANALYZE do not treat row-identity fields as table columns.
fn preserve_hidden_sql_field_metadata(
    schema: SchemaRef,
    source_fields: &[Arc<Field>],
) -> Result<SchemaRef, ConnectorError> {
    if schema.fields().len() != source_fields.len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg projected schema lost a source field",
        ));
    }
    let fields = schema
        .fields()
        .iter()
        .zip(source_fields)
        .map(|(projected, source)| {
            let hidden = source
                .metadata()
                .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_some_and(|value| value.eq_ignore_ascii_case("true"));
            if !hidden {
                return projected.clone();
            }
            let mut metadata = projected.metadata().clone();
            metadata.insert(
                novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
                "true".to_string(),
            );
            Arc::new(projected.as_ref().clone().with_metadata(metadata))
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn is_iceberg_metadata_column(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "_file" | "_pos" | "_row_id" | "_last_updated_sequence_number"
    )
}

#[derive(Clone, Deserialize, Serialize)]
struct TablePayload {
    namespace: String,
    table: String,
    table_info: Option<novarocks_connector_iceberg::scan_model::IcebergTableInfo>,
    metadata_columns: Vec<String>,
    metadata_table_type: Option<super::IcebergMetadataTableType>,
    prepared_files: Vec<IcebergDataFileInfo>,
    explicit_files: Option<Vec<IcebergDataFileInfo>>,
    #[serde(default)]
    logical_type_columns: BTreeMap<String, String>,
    #[serde(default)]
    hidden_columns: Vec<String>,
    #[serde(default)]
    frozen_rewrite: Option<FrozenRewriteSourcePayloadV1>,
}

/// Sign the SQL-proposed Arrow input while the Iceberg provider still owns the
/// exact admitted table.  This is intentionally close to `TablePayload`: no
/// application layer can decode the handle, substitute a catalog field ID, or
/// recreate a preparation for another connector incarnation.
pub(crate) fn prepare_iceberg_write(
    request: ConnectorWritePreparationRequest,
    owner: &ConnectorExecutionBindingKey,
) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
    request.validate(owner)?;
    let payload: TablePayload =
        decode_payload(request.table.payload(), "admitted Iceberg write table")?;
    if payload.metadata_table_type.is_some() {
        return Ok(ConnectorWritePreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata tables cannot be write targets",
            ),
        ));
    }
    let table = payload.table_info.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg write table is missing its frozen table descriptor",
        )
    })?;
    let serialized_metadata = table.serialized_metadata.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg write table is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized_metadata).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode admitted Iceberg write metadata: {error}"),
        )
    })?;
    if matches!(request.purpose, ConnectorWriteAdmissionPurpose::OrdinaryDml)
        && metadata
            .properties()
            .contains_key(crate::mv::persistence::descriptor::MV_DESCRIPTOR_PACKAGE_ID_PROP)
    {
        return Ok(ConnectorWritePreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "table {}.{}.{} is a materialized view; use REFRESH MATERIALIZED VIEW to update it",
                    table.catalog, table.namespace, table.table
                ),
            ),
        ));
    }

    let input = bind_iceberg_write_input(&request, owner, &metadata)?;
    let target_snapshot_id =
        iceberg_write_target_snapshot_id(&metadata, request.target_ref.as_str())?;
    let table_uuid = table.table_uuid.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "admitted Iceberg write table is missing its table UUID",
        )
    })?;
    let snapshot = target_snapshot_id.map_or_else(|| "none".to_string(), |id| id.to_string());
    let base_version = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
        "iceberg/write-base/v1/{table_uuid}/{}/{snapshot}",
        request.target_ref.as_str()
    )))?;
    let preparation_payload = Bytes::from(format!(
        "iceberg/write-preparation/v1/{}/{}/{}/{snapshot}",
        owner.instance_id.as_str(),
        table_uuid,
        request.target_ref.as_str()
    ));
    Ok(ConnectorWritePreparationOutcome::Prepared(
        ConnectorWritePreparation::try_new(
            owner.clone(),
            request.table,
            request.target_ref,
            request.intent,
            base_version,
            input,
            preparation_payload,
        )?,
    ))
}

fn iceberg_write_target_snapshot_id(
    metadata: &TableMetadata,
    target_ref: &str,
) -> Result<Option<i64>, ConnectorError> {
    if target_ref == "main" {
        return Ok(metadata.current_snapshot_id());
    }
    novarocks_connector_iceberg::ref_snapshot::resolve_branch_head_snapshot_id(metadata, target_ref)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))
}

/// Provider-side row-mutation admission. This is intentionally independent of
/// `prepare_iceberg_write`: it chooses the table-format strategy and signs
/// identity facts before any staging service is registered.
pub(crate) fn prepare_iceberg_row_mutation(
    request: ConnectorRowMutationPreparationRequest,
    owner: &ConnectorExecutionBindingKey,
) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError> {
    request.validate(owner)?;
    let payload: TablePayload = decode_payload(
        request.table.payload(),
        "admitted Iceberg row-mutation table",
    )?;
    if payload.metadata_table_type.is_some() {
        return Ok(ConnectorRowMutationPreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg metadata tables cannot be row-mutation targets",
            ),
        ));
    }
    let table = payload.table_info.as_ref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg row-mutation table is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata =
        serde_json::from_str(table.serialized_metadata.as_deref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "admitted Iceberg row-mutation table has no serialized metadata",
            )
        })?)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode admitted Iceberg row-mutation metadata: {error}"),
            )
        })?;
    // The managed-materialized-view rejection deliberately does NOT live here.
    // Incremental MV refresh drives its own change-stream writes through this
    // same admission, so a check at this level cannot tell a user DML statement
    // apart from the MV machinery maintaining its own target. That rejection
    // stays at the SQL entry points, where `reject_if_iceberg_mv_table` already
    // makes it from neutral metadata under the same exact lease.

    // Writing to a non-main branch needs the v3 row-lineage semantics the
    // branch writer relies on.
    if request.target_ref.as_str() != "main"
        && metadata.format_version()
            != novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3
    {
        return Ok(ConnectorRowMutationPreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!(
                    "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                    table.table,
                    metadata.format_version() as u8,
                ),
            ),
        ));
    }
    // The strategy rule, the fail-fast write guards it runs first, and the
    // merge-on-read override for a MERGE that can delete all live in the
    // provider. A policy rejection here is a denial, not an internal fault.
    let strategy = match novarocks_connector_iceberg::commit::row_mutation_strategy_from_metadata(
        &metadata,
        &request.intent,
    ) {
        Ok(strategy) => strategy,
        Err(error) => {
            return Ok(ConnectorRowMutationPreparationOutcome::Denied(
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error),
            ));
        }
    };
    // A MERGE that can append has no target identity or before-image for its
    // Insert rows.  The signed match layout therefore declares precisely those
    // fields nullable; Delete/Replace validation still rejects null keys.
    let insert_eligible = request.intent.accepts(ConnectorRowMutationEffect::Insert);
    let identity_fields = iceberg_metadata_arrow_fields(&payload.metadata_columns)?
        .into_iter()
        .enumerate()
        .map(|(ordinal, field)| {
            let mut hasher = Sha256::new();
            hasher.update(b"novarocks.iceberg.row-mutation-identity.v1\0");
            hasher.update(owner.instance_id.as_str().as_bytes());
            hasher.update(owner.incarnation.to_bytes());
            hasher.update(request.table.payload());
            hasher.update((ordinal as u64).to_be_bytes());
            ConnectorMutationSourceField::new(
                ConnectorWriteFieldToken::from_bytes(hasher.finalize().into()),
                field
                    .as_ref()
                    .clone()
                    .with_nullable(field.is_nullable() || insert_eligible),
                ordinal as u32,
            )
        })
        .collect::<Vec<_>>();
    if identity_fields.is_empty() {
        return Ok(ConnectorRowMutationPreparationOutcome::Denied(
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg row-mutation target has no admitted identity fields",
            ),
        ));
    }
    let table_uuid = table.table_uuid.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "admitted Iceberg row-mutation table is missing table UUID",
        )
    })?;
    let target_snapshot_id =
        iceberg_write_target_snapshot_id(&metadata, request.target_ref.as_str())?;
    let snapshot = target_snapshot_id.map_or_else(|| "none".to_string(), |id| id.to_string());
    let base_version = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
        "iceberg/row-mutation-base/v1/{table_uuid}/{}/{snapshot}",
        request.target_ref.as_str()
    )))?;
    // The match layout is provider-signed rather than name-derived by SQL:
    // source identities precede target before/after values and the logical
    // effect field is last.  The source/target ordinals are the sole cross
    // layer binding; these familiar Iceberg names never become a Core rule.
    let requested_target_fields = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            ConnectorWriteFieldRequest::new(Field::new(
                &field.name,
                DataType::Null,
                !field.required,
            ))
        })
        .collect::<Vec<_>>();
    let target_schema = Schema::new(
        exact_requested_iceberg_write_fields(&metadata, &requested_target_fields)?
            .into_iter()
            .map(|field| Arc::new(field.field().clone()))
            .collect::<Vec<_>>(),
    );
    let target_start = u32::try_from(identity_fields.len()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg row-mutation identity layout exceeds u32 ordinals",
        )
    })?;
    let target_width = u32::try_from(target_schema.fields().len()).map_err(|_| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg row-mutation target layout exceeds u32 ordinals",
        )
    })?;
    let before_fields = target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ConnectorMutationTargetField::new(
                iceberg_row_mutation_field_token(owner, &request.table, b"before", ordinal),
                field
                    .as_ref()
                    .clone()
                    .with_nullable(field.is_nullable() || insert_eligible),
                target_start + u32::try_from(ordinal).expect("target ordinal fits u32"),
            )
        })
        .collect::<Vec<_>>();
    let after_fields = target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(ordinal, field)| {
            ConnectorMutationTargetField::new(
                iceberg_row_mutation_field_token(owner, &request.table, b"after", ordinal),
                field.as_ref().clone(),
                target_start
                    + target_width
                    + u32::try_from(ordinal).expect("target ordinal fits u32"),
            )
        })
        .collect::<Vec<_>>();
    let mut effect_hasher = Sha256::new();
    effect_hasher.update(b"novarocks.iceberg.row-mutation-effect.v1\0");
    effect_hasher.update(owner.instance_id.as_str().as_bytes());
    effect_hasher.update(request.table.payload());
    let effect_field = ConnectorMutationEffectField::try_new(
        ConnectorWriteFieldToken::from_bytes(effect_hasher.finalize().into()),
        Field::new("__row_mutation_effect", DataType::Int8, false),
        target_start
            .checked_add(target_width.checked_mul(2).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg row-mutation target layout overflowed",
                )
            })?)
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg row-mutation effect ordinal overflowed",
                )
            })?,
    )?;
    let uniqueness_tokens = identity_fields
        .iter()
        .map(ConnectorMutationSourceField::token)
        .collect();
    let contract = ConnectorMutationMatchContract::try_new(
        owner.clone(),
        request.table.clone(),
        base_version.clone(),
        identity_fields,
        before_fields,
        after_fields,
        uniqueness_tokens,
        effect_field,
    )?;
    let payload = Bytes::from(format!(
        "iceberg/row-mutation-preparation/v1/{}/{table_uuid}/{}/{snapshot}/{strategy:?}",
        owner.instance_id.as_str(),
        request.target_ref.as_str()
    ));
    Ok(ConnectorRowMutationPreparationOutcome::Prepared(
        ConnectorRowMutationPreparation::try_new(
            owner.clone(),
            request.operation_id,
            request.table,
            request.target_ref,
            request.intent,
            base_version,
            contract,
            strategy,
            // The application persists this in its durable DML journal. It is
            // the same ref-scoped resolution the SQL entry points used to run
            // against a concrete table handle: the current snapshot for main,
            // the branch head otherwise.
            target_snapshot_id,
            // The sequence number this mutation's rows will belong to. A
            // merge-on-read writer stamps it on every rewritten row, so it has
            // to be known before the commit exists.
            Some(metadata.last_sequence_number() + 1),
            payload,
        )?,
    ))
}

fn iceberg_row_mutation_field_token(
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
    role: &[u8],
    ordinal: usize,
) -> ConnectorWriteFieldToken {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-field.v1\0");
    hasher.update(owner.instance_id.as_str().as_bytes());
    hasher.update(owner.incarnation.to_bytes());
    hasher.update(table.payload());
    hasher.update((role.len() as u64).to_be_bytes());
    hasher.update(role);
    hasher.update((ordinal as u64).to_be_bytes());
    ConnectorWriteFieldToken::from_bytes(hasher.finalize().into())
}

/// Materialize the Provider-owned route graph after a durable operation has
/// retained the exact write lease.  This is deliberately not a call to
/// `prepare_write`: route preparation is derived from the sealed row-mutation
/// contract and every physical choice remains inside the Iceberg provider.
pub(crate) fn activate_iceberg_row_mutation(
    request: ConnectorRowMutationActivationRequest,
    owner: &ConnectorExecutionBindingKey,
) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
    request.validate(owner)?;
    if request.context().cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg row-mutation activation was cancelled before Provider planning",
        ));
    }
    if Instant::now() >= request.context().deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg row-mutation activation deadline elapsed before Provider planning",
        ));
    }
    let preparation = request.preparation().clone();
    match request {
        ConnectorRowMutationActivationRequest::Direct { .. } => {
            activate_iceberg_direct_row_mutation(&preparation)
        }
        ConnectorRowMutationActivationRequest::CopyOnWrite { .. } => Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "legacy Iceberg control bindings do not support copy-on-write activation",
        )),
    }
}

fn activate_iceberg_direct_row_mutation(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<ConnectorRowMutationExecutionPlan, ConnectorError> {
    let primary = ConnectorWriteCohortId::primary(preparation.operation_id());
    let mut routes = Vec::new();
    match preparation.strategy() {
        ConnectorRowMutationStrategy::PositionDelete
        | ConnectorRowMutationStrategy::DeletionVector => {
            let effects = admitted_effects(preparation, &[ConnectorRowMutationEffect::Delete]);
            if effects.is_empty() {
                return Err(invalid_iceberg_row_mutation_activation(
                    "Iceberg position-delete strategy cannot implement the admitted logical effects",
                ));
            }
            let input = iceberg_position_input(preparation)?;
            routes.push(iceberg_row_mutation_route(
                preparation,
                primary,
                b"direct-position-delete",
                effects,
                input,
                iceberg_position_partition_tokens(preparation)?,
            )?);
        }
        ConnectorRowMutationStrategy::EqualityDelete => {
            let effects = admitted_effects(preparation, &[ConnectorRowMutationEffect::Delete]);
            if effects.is_empty() {
                return Err(invalid_iceberg_row_mutation_activation(
                    "Iceberg equality-delete strategy cannot implement the admitted logical effects",
                ));
            }
            let input = ConnectorWriteInputShape::EqualityDelete {
                equality_fields: target_bindings(preparation.match_contract().before_fields()),
            };
            routes.push(iceberg_row_mutation_route(
                preparation,
                primary,
                b"direct-equality-delete",
                effects,
                input,
                Vec::new(),
            )?);
        }
        ConnectorRowMutationStrategy::MergeOnRead => {
            // A Replace reaches both routes.  The delete route consumes its
            // before-image identity while the data route consumes its
            // after-image values; neither route learns the other's physical
            // Iceberg policy.
            let delete_effects = admitted_effects(
                preparation,
                &[
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                ],
            );
            if !delete_effects.is_empty() {
                routes.push(iceberg_row_mutation_route(
                    preparation,
                    iceberg_row_mutation_direct_cohort(preparation, b"mor-delete")?,
                    b"mor-delete",
                    delete_effects,
                    iceberg_mor_delete_input(preparation)?,
                    iceberg_position_partition_tokens(preparation)?,
                )?);
            }
            let replacement_effects =
                admitted_effects(preparation, &[ConnectorRowMutationEffect::Replace]);
            if !replacement_effects.is_empty() {
                routes.push(iceberg_row_mutation_route(
                    preparation,
                    iceberg_row_mutation_direct_cohort(preparation, b"mor-replacement")?,
                    b"mor-replacement",
                    replacement_effects,
                    iceberg_cow_rewrite_input(preparation)?,
                    Vec::new(),
                )?);
            }
            let insert_effects =
                admitted_effects(preparation, &[ConnectorRowMutationEffect::Insert]);
            if !insert_effects.is_empty() {
                routes.push(iceberg_row_mutation_route(
                    preparation,
                    iceberg_row_mutation_direct_cohort(preparation, b"mor-insert")?,
                    b"mor-insert",
                    insert_effects,
                    ConnectorWriteInputShape::Data {
                        fields: target_bindings(preparation.match_contract().after_fields()),
                    },
                    Vec::new(),
                )?);
            }
        }
        ConnectorRowMutationStrategy::CopyOnWrite => {
            return Err(invalid_iceberg_row_mutation_activation(
                "Iceberg Copy-on-Write activation requires the bounded match selection",
            ));
        }
    }
    ConnectorRowMutationExecutionPlan::try_direct(preparation.clone(), routes)
}

fn iceberg_row_mutation_snapshot(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<i64, ConnectorError> {
    let payload = std::str::from_utf8(preparation.payload()).map_err(|_| {
        invalid_iceberg_row_mutation_activation("Iceberg row-mutation preparation is not UTF-8")
    })?;
    payload
        .rsplit('/')
        .nth(1)
        .filter(|snapshot| *snapshot != "none")
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation(
                "Iceberg Copy-on-Write preparation lacks a frozen snapshot",
            )
        })?
        .parse::<i64>()
        .map_err(|_| {
            invalid_iceberg_row_mutation_activation(
                "Iceberg Copy-on-Write preparation has an invalid frozen snapshot",
            )
        })
}

fn admitted_effects(
    preparation: &ConnectorRowMutationPreparation,
    candidates: &[ConnectorRowMutationEffect],
) -> Vec<ConnectorRowMutationEffect> {
    candidates
        .iter()
        .copied()
        .filter(|effect| preparation.intent().accepts(*effect))
        .collect()
}

fn iceberg_row_mutation_route(
    preparation: &ConnectorRowMutationPreparation,
    cohort_id: ConnectorWriteCohortId,
    route_kind: &[u8],
    effects: Vec<ConnectorRowMutationEffect>,
    input: ConnectorWriteInputShape,
    partition_fields: Vec<ConnectorWriteFieldToken>,
) -> Result<ConnectorRowMutationRoute, ConnectorError> {
    let route_preparation = ConnectorWritePreparation::try_new(
        preparation.owner().clone(),
        preparation.table().clone(),
        preparation.target_ref().clone(),
        ConnectorWriteIntent::RowDelta,
        preparation.base_version().clone(),
        input,
        iceberg_row_mutation_route_payload(preparation, route_kind),
    )?;
    iceberg_row_mutation_route_with_preparation(
        preparation,
        cohort_id,
        route_kind,
        effects,
        route_preparation,
        partition_fields,
    )
}

fn iceberg_row_mutation_route_with_preparation(
    preparation: &ConnectorRowMutationPreparation,
    cohort_id: ConnectorWriteCohortId,
    route_kind: &[u8],
    effects: Vec<ConnectorRowMutationEffect>,
    route_preparation: ConnectorWritePreparation,
    partition_fields: Vec<ConnectorWriteFieldToken>,
) -> Result<ConnectorRowMutationRoute, ConnectorError> {
    if effects.is_empty() {
        return Err(invalid_iceberg_row_mutation_activation(
            "Iceberg row-mutation route has no admitted logical effects",
        ));
    }
    let route_id = iceberg_row_mutation_route_id(preparation, cohort_id, route_kind);
    let input_ordinals = route_preparation
        .input()
        .fields()
        .into_iter()
        .map(|binding| {
            row_mutation_input_ordinal(preparation, binding.token())
                .map(|ordinal| ConnectorMutationRouteInput::new(binding.token(), ordinal))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ConnectorRowMutationRoute::try_new(
        route_id,
        cohort_id,
        effects,
        route_preparation.input().clone(),
        input_ordinals,
        partition_fields,
        route_preparation,
    )
}

fn iceberg_row_mutation_route_id(
    preparation: &ConnectorRowMutationPreparation,
    cohort_id: ConnectorWriteCohortId,
    route_kind: &[u8],
) -> ConnectorWriteRouteId {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-route.v1\0");
    hasher.update(preparation.operation_id().to_bytes());
    hasher.update(preparation.digest());
    hasher.update(cohort_id.to_bytes());
    hasher.update((route_kind.len() as u64).to_be_bytes());
    hasher.update(route_kind);
    ConnectorWriteRouteId::from_bytes(hasher.finalize().into())
}

fn iceberg_row_mutation_direct_cohort(
    preparation: &ConnectorRowMutationPreparation,
    route_kind: &[u8],
) -> Result<ConnectorWriteCohortId, ConnectorError> {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-direct-cohort.v1\0");
    hasher.update(preparation.digest());
    hasher.update((route_kind.len() as u64).to_be_bytes());
    hasher.update(route_kind);
    ConnectorWriteCohortId::derive(
        preparation.operation_id(),
        b"iceberg-row-mutation-direct",
        hasher.finalize().into(),
    )
}

fn iceberg_row_mutation_route_payload(
    preparation: &ConnectorRowMutationPreparation,
    route_kind: &[u8],
) -> Bytes {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.row-mutation-route-payload.v1\0");
    hasher.update(preparation.operation_id().to_bytes());
    hasher.update(preparation.digest());
    hasher.update((route_kind.len() as u64).to_be_bytes());
    hasher.update(route_kind);
    Bytes::from(format!(
        "iceberg/row-mutation-route/v1/{}",
        hex::encode(hasher.finalize())
    ))
}

/// Resolve the physical COW base solely inside the Iceberg adapter.  The
/// recipe deliberately contains only opaque cohort membership: a branch head
/// is a provider metadata fact, not a Core-carried execution route.
fn iceberg_row_mutation_base_snapshot_from_preparation(
    preparation: &ConnectorWritePreparation,
    target_ref: &str,
) -> Result<i64, ConnectorError> {
    let payload: TablePayload = decode_payload(
        preparation.table().payload(),
        "admitted Iceberg COW preparation table",
    )?;
    let table = payload.table_info.ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg COW preparation is missing its frozen table descriptor",
        )
    })?;
    let serialized = table.serialized_metadata.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg COW preparation is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode admitted Iceberg COW preparation metadata: {error}"),
        )
    })?;
    iceberg_ref_snapshot_from_metadata(&metadata, target_ref)
}

fn iceberg_ref_snapshot_from_metadata(
    metadata: &TableMetadata,
    target_ref: &str,
) -> Result<i64, ConnectorError> {
    if target_ref.eq_ignore_ascii_case("main") {
        return metadata.current_snapshot_id().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg COW preparation table has no current snapshot",
            )
        });
    }
    let reference = metadata.refs().get(target_ref).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::NotFound,
            format!("Iceberg COW target branch `{target_ref}` does not exist in frozen metadata"),
        )
    })?;
    if !reference.is_branch() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("Iceberg COW target ref `{target_ref}` is not a branch"),
        ));
    }
    Ok(reference.snapshot_id)
}

fn target_bindings(fields: &[ConnectorMutationTargetField]) -> Vec<ConnectorWriteFieldBinding> {
    fields
        .iter()
        .map(|field| ConnectorWriteFieldBinding::new(field.token(), field.field().clone()))
        .collect()
}

fn iceberg_cow_rewrite_input(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    let contract = preparation.match_contract();
    let lineage = [
        iceberg_row_lineage::ICEBERG_ROW_ID_COL,
        iceberg_row_lineage::ICEBERG_LAST_UPDATED_SEQ_COL,
    ]
    .into_iter()
    .map(|name| {
        contract
            .identity_fields()
            .iter()
            .find(|field| field.field().name().eq_ignore_ascii_case(name))
            .map(|field| ConnectorWriteFieldBinding::new(field.token(), field.field().clone()))
            .ok_or_else(|| {
                invalid_iceberg_row_mutation_activation(format!(
                    "Iceberg COW identity lacks `{name}`"
                ))
            })
    })
    .collect::<Result<Vec<_>, _>>()?;
    Ok(ConnectorWriteInputShape::RowLineage {
        data_fields: target_bindings(contract.after_fields()),
        row_identity_fields: lineage,
    })
}

fn iceberg_position_input(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    let contract = preparation.match_contract();
    let file = contract
        .identity_fields()
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case("_file"))
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation("Iceberg row identity lacks `_file`")
        })?;
    let pos = contract
        .identity_fields()
        .iter()
        .find(|field| field.field().name().eq_ignore_ascii_case("_pos"))
        .ok_or_else(|| {
            invalid_iceberg_row_mutation_activation("Iceberg row identity lacks `_pos`")
        })?;
    let partition_source_fields = iceberg_position_partition_bindings(preparation)?;
    Ok(match preparation.strategy() {
        ConnectorRowMutationStrategy::DeletionVector => ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![
                ConnectorWriteFieldBinding::new(
                    file.token(),
                    file.field().clone().with_nullable(false),
                ),
                ConnectorWriteFieldBinding::new(
                    pos.token(),
                    pos.field().clone().with_nullable(false),
                ),
            ],
            partition_source_fields,
        },
        _ => ConnectorWriteInputShape::PositionDelete {
            identity_fields: vec![
                ConnectorWriteFieldBinding::new(
                    file.token(),
                    file.field().clone().with_nullable(false),
                ),
                ConnectorWriteFieldBinding::new(
                    pos.token(),
                    pos.field().clone().with_nullable(false),
                ),
            ],
            partition_source_fields,
        },
    })
}

fn iceberg_mor_delete_input(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    // MOR is admitted only for Iceberg v3 row-lineage tables.  Its delete
    // half therefore uses the v3 deletion-vector writer, while its data half
    // remains an ordinary row-lineage append route.
    match iceberg_position_input(preparation)? {
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => Ok(ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        }),
        input @ ConnectorWriteInputShape::DeletionVector { .. } => Ok(input),
        _ => Err(invalid_iceberg_row_mutation_activation(
            "Iceberg MOR delete route did not derive a position identity input",
        )),
    }
}

fn iceberg_position_partition_bindings(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<Vec<ConnectorWriteFieldBinding>, ConnectorError> {
    let payload: TablePayload = decode_payload(
        preparation.table().payload(),
        "admitted Iceberg row-mutation table",
    )?;
    let table = payload.table_info.ok_or_else(|| {
        invalid_iceberg_row_mutation_activation(
            "admitted Iceberg row-mutation table is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata =
        serde_json::from_str(table.serialized_metadata.as_deref().ok_or_else(|| {
            invalid_iceberg_row_mutation_activation(
                "admitted Iceberg row-mutation table has no serialized metadata",
            )
        })?)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode admitted Iceberg row-mutation metadata: {error}"),
            )
        })?;
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|partition| {
            let source = metadata
                .current_schema()
                .field_by_id(partition.source_id)
                .ok_or_else(|| {
                    invalid_iceberg_row_mutation_activation(
                        "Iceberg partition source is absent from the frozen schema",
                    )
                })?;
            let field = preparation
                .match_contract()
                .before_fields()
                .iter()
                .find(|field| field.field().name().eq_ignore_ascii_case(&source.name))
                .ok_or_else(|| {
                    invalid_iceberg_row_mutation_activation(
                        "Iceberg match contract is missing a partition source before-field",
                    )
                })?;
            Ok(ConnectorWriteFieldBinding::new(
                field.token(),
                field.field().clone(),
            ))
        })
        .collect()
}

fn iceberg_position_partition_tokens(
    preparation: &ConnectorRowMutationPreparation,
) -> Result<Vec<ConnectorWriteFieldToken>, ConnectorError> {
    iceberg_position_partition_bindings(preparation).map(|bindings| {
        bindings
            .into_iter()
            .map(|binding| binding.token())
            .collect()
    })
}

fn row_mutation_input_ordinal(
    preparation: &ConnectorRowMutationPreparation,
    token: ConnectorWriteFieldToken,
) -> Result<u32, ConnectorError> {
    let contract = preparation.match_contract();
    if let Some(field) = contract
        .identity_fields()
        .iter()
        .find(|field| field.token() == token)
    {
        return Ok(field.source_ordinal());
    }
    if let Some(field) = contract
        .before_fields()
        .iter()
        .chain(contract.after_fields())
        .find(|field| field.token() == token)
    {
        return Ok(field.target_ordinal());
    }
    Err(invalid_iceberg_row_mutation_activation(
        "Iceberg route input token is foreign to its signed match contract",
    ))
}

fn invalid_iceberg_row_mutation_activation(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn bind_iceberg_write_input(
    request: &ConnectorWritePreparationRequest,
    owner: &ConnectorExecutionBindingKey,
    metadata: &TableMetadata,
) -> Result<ConnectorWriteInputShape, ConnectorError> {
    Ok(match &request.input {
        ConnectorWriteInputRequest::Data { fields } => ConnectorWriteInputShape::Data {
            fields: bind_iceberg_write_fields(
                &exact_iceberg_data_write_fields(metadata, fields)?,
                owner,
                &request.table,
                request.intent,
                1,
            )?,
        },
        ConnectorWriteInputRequest::RowLineage {
            data_fields,
            row_identity_fields,
        } => ConnectorWriteInputShape::RowLineage {
            data_fields: bind_iceberg_write_fields(
                &exact_iceberg_data_write_fields(metadata, data_fields)?,
                owner,
                &request.table,
                request.intent,
                2,
            )?,
            row_identity_fields: bind_iceberg_write_fields(
                row_identity_fields,
                owner,
                &request.table,
                request.intent,
                3,
            )?,
        },
        ConnectorWriteInputRequest::PositionDelete {
            identity_fields,
            partition_source_fields,
        } => {
            let partition_source_fields = if partition_source_fields.is_empty() {
                iceberg_position_delete_partition_field_requests(metadata)?
            } else {
                partition_source_fields.clone()
            };
            ConnectorWriteInputShape::PositionDelete {
                identity_fields: bind_iceberg_write_fields(
                    identity_fields,
                    owner,
                    &request.table,
                    request.intent,
                    4,
                )?,
                partition_source_fields: bind_iceberg_write_fields(
                    &partition_source_fields,
                    owner,
                    &request.table,
                    request.intent,
                    5,
                )?,
            }
        }
        ConnectorWriteInputRequest::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => {
            let partition_source_fields = if partition_source_fields.is_empty() {
                iceberg_position_delete_partition_field_requests(metadata)?
            } else {
                partition_source_fields.clone()
            };
            ConnectorWriteInputShape::DeletionVector {
                identity_fields: bind_iceberg_write_fields(
                    identity_fields,
                    owner,
                    &request.table,
                    request.intent,
                    6,
                )?,
                partition_source_fields: bind_iceberg_write_fields(
                    &partition_source_fields,
                    owner,
                    &request.table,
                    request.intent,
                    7,
                )?,
            }
        }
        ConnectorWriteInputRequest::EqualityDelete { equality_fields } => {
            ConnectorWriteInputShape::EqualityDelete {
                equality_fields: bind_iceberg_write_fields(
                    &exact_requested_iceberg_write_fields(metadata, equality_fields)?,
                    owner,
                    &request.table,
                    request.intent,
                    8,
                )?,
            }
        }
    })
}

/// Rebuild SQL-proposed data fields from the Provider-owned frozen Iceberg
/// schema before signing them. Arrow offset width is part of the execution
/// contract: Iceberg `binary` is `Binary`, while Iceberg `variant` is the
/// engine's encoded `LargeBinary` representation.
fn exact_iceberg_data_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    for request in requested {
        if metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .all(|field| !field.name.eq_ignore_ascii_case(request.field().name()))
        {
            return Err(invalid_iceberg_write_activation(format!(
                "Iceberg write input column `{}` is absent from the frozen target schema",
                request.field().name()
            )));
        }
    }
    let requested_all = metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            ConnectorWriteFieldRequest::new(Field::new(
                &field.name,
                DataType::Null,
                !field.required,
            ))
        })
        .collect::<Vec<_>>();
    exact_requested_iceberg_write_fields(metadata, &requested_all)
}

fn exact_requested_iceberg_write_fields(
    metadata: &TableMetadata,
    requested: &[ConnectorWriteFieldRequest],
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    let iceberg_schema = metadata.current_schema();
    let arrow_schema =
        novarocks_connector_iceberg::iceberg::arrow::schema_to_arrow_schema(iceberg_schema)
            .map_err(|error| {
                invalid_iceberg_write_activation(format!(
                    "convert frozen Iceberg write schema to Arrow: {error}"
                ))
            })?;
    requested
        .iter()
        .map(|request| {
            let requested_name = request.field().name();
            let (ordinal, iceberg_field) = iceberg_schema
                .as_struct()
                .fields()
                .iter()
                .enumerate()
                .find(|(_, field)| field.name.eq_ignore_ascii_case(requested_name))
                .ok_or_else(|| {
                    invalid_iceberg_write_activation(format!(
                        "Iceberg write input column `{requested_name}` is absent from the frozen target schema"
                    ))
                })?;
            let arrow_field = arrow_schema.field(ordinal);
            let data_type = match iceberg_field.field_type.as_ref() {
                Type::Primitive(PrimitiveType::Variant) => DataType::LargeBinary,
                Type::Primitive(PrimitiveType::Binary) => DataType::Binary,
                Type::Primitive(PrimitiveType::Timestamptz) => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
                Type::Primitive(PrimitiveType::TimestamptzNs) => {
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                }
                _ => arrow_field.data_type().clone(),
            };
            Ok(ConnectorWriteFieldRequest::new(Field::new(
                &iceberg_field.name,
                data_type,
                !iceberg_field.required,
            )))
        })
        .collect()
}

/// Position-delete and deletion-vector SQL only name the fixed row identity.
/// The exact partition-source projection is provider-owned and is added while
/// the frozen Iceberg metadata is still available.
fn iceberg_position_delete_partition_field_requests(
    metadata: &TableMetadata,
) -> Result<Vec<ConnectorWriteFieldRequest>, ConnectorError> {
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|partition| {
            let source = metadata
                .current_schema()
                .field_by_id(partition.source_id)
                .ok_or_else(|| {
                    invalid_iceberg_write_activation(format!(
                        "Iceberg position-delete partition source field id {} is absent from the frozen schema",
                        partition.source_id
                    ))
                })?;
            let data_type = iceberg_type_to_arrow_type(source.field_type.as_ref())
                .map_err(invalid_iceberg_write_activation)?;
            Ok(ConnectorWriteFieldRequest::new(Field::new(
                &source.name,
                data_type,
                !source.required,
            )))
        })
        .collect()
}

fn bind_iceberg_write_fields(
    fields: &[ConnectorWriteFieldRequest],
    owner: &ConnectorExecutionBindingKey,
    table: &ConnectorTableHandle,
    intent: novarocks_spi::connector::ConnectorWriteIntent,
    domain: u8,
) -> Result<Vec<ConnectorWriteFieldBinding>, ConnectorError> {
    fields
        .iter()
        .enumerate()
        .map(|(ordinal, request)| {
            let mut hasher = Sha256::new();
            hasher.update(b"novarocks.iceberg.write-field-token.v1\0");
            hasher.update(owner.instance_id.as_str().as_bytes());
            hasher.update(owner.incarnation.to_bytes());
            hasher.update(table.payload());
            hasher.update(format!("{intent:?}").as_bytes());
            hasher.update([domain]);
            hasher.update((ordinal as u64).to_be_bytes());
            hasher.update(format!("{:?}", request.field()).as_bytes());
            Ok(ConnectorWriteFieldBinding::new(
                ConnectorWriteFieldToken::from_bytes(hasher.finalize().into()),
                request.field().clone(),
            ))
        })
        .collect()
}

/// Reserve a provider-owned data writer service after the generic operation
/// has retained the exact lease and sealed its preparation.  SQL/Core pass
/// only the sealed preparation and their generic commit seam: this boundary
/// derives the private sink specification, writer payload, and control plan
/// payload from the admitted Iceberg table.
pub(crate) fn register_iceberg_data_write_service_from_preparation(
    services: IcebergWriteServiceRegistry,
    operation_id: ConnectorWriteOperationId,
    preparation: &ConnectorWritePreparation,
    target_ref: &str,
    entry: &IcebergCatalogEntry,
    commit_executor: Arc<dyn IcebergWriteReportCommitter>,
) -> Result<(), ConnectorError> {
    preparation.validate()?;
    let sink_spec = iceberg_data_sink_spec_from_preparation(preparation, entry)?;
    let writer_handle_payload = encode_data_sink_spec_handle_payload(&sink_spec)
        .map_err(|error| invalid_iceberg_write_activation(error))?;
    register_iceberg_write_service_from_preparation_payload(
        services,
        operation_id,
        preparation,
        target_ref,
        sink_spec,
        writer_handle_payload,
        commit_executor,
    )
}

/// Activate one managed MV publication entirely inside the legacy provider
/// generation. The application supplies only the provider-signed preparation
/// and neutral publication intent; catalog reload, exact-target validation,
/// writer service construction, provenance encoding and commit ownership stay
/// behind `ConnectorWriteControl::activate_write`.
pub(crate) fn activate_iceberg_managed_publication_write(
    registry: &Arc<RwLock<IcebergCatalogRegistry>>,
    services: IcebergWriteServiceRegistry,
    request: &ConnectorWriteActivationRequest,
) -> Result<(), ConnectorError> {
    let ConnectorWriteActivationIntent::ManagedPublication(intent) = &request.intent else {
        return Ok(());
    };
    let (selected_cohort, preparation, routed_preparations, source_digest) = match &request.source {
        ConnectorWriteActivationSource::Prepared(preparation) => (
            ConnectorWriteCohortId::primary(request.operation_id),
            preparation.clone(),
            None,
            preparation.digest(),
        ),
        ConnectorWriteActivationSource::RowMutation(plan) => {
            plan.validate()?;
            if plan.operation_id() != request.operation_id || plan.copy_on_write().is_some() {
                return Err(invalid_iceberg_write_activation(
                    "managed Iceberg publication requires one direct row-mutation plan",
                ));
            }
            let mut routes = plan
                .routes()
                .iter()
                .map(|route| (route.cohort_id(), route.preparation().clone()))
                .collect::<Vec<_>>();
            routes.sort_by_key(|(cohort_id, _)| *cohort_id);
            let (selected_cohort, preparation) = routes.first().cloned().ok_or_else(|| {
                invalid_iceberg_write_activation(
                    "managed Iceberg row-mutation plan has no writer routes",
                )
            })?;
            (selected_cohort, preparation, Some(routes), plan.digest())
        }
    };
    preparation.validate()?;
    let payload: TablePayload = decode_payload(
        preparation.table().payload(),
        "managed Iceberg publication target",
    )?;
    let target = payload.table_info.as_ref().ok_or_else(|| {
        invalid_iceberg_write_activation(
            "managed Iceberg publication target is missing its frozen descriptor",
        )
    })?;
    if target.catalog != preparation.owner().instance_id.as_str()
        || target.namespace != payload.namespace
        || target.table != payload.table
    {
        return Err(invalid_iceberg_write_activation(
            "managed Iceberg publication target identity drifted from its preparation",
        ));
    }
    let frozen_metadata: TableMetadata =
        serde_json::from_str(target.serialized_metadata.as_deref().ok_or_else(|| {
            invalid_iceberg_write_activation(
                "managed Iceberg publication target is missing frozen metadata",
            )
        })?)
        .map_err(|error| {
            invalid_iceberg_write_activation(format!(
                "decode managed Iceberg publication metadata: {error}"
            ))
        })?;
    let expected_snapshot_id =
        iceberg_write_target_snapshot_id(&frozen_metadata, preparation.target_ref().as_str())?;
    let entry = registry
        .read()
        .map_err(|error| {
            invalid_iceberg_write_activation(format!(
                "read Iceberg catalog registry for managed publication: {error}"
            ))
        })?
        .get(&target.catalog)
        .map_err(invalid_iceberg_write_activation)?;
    let (commit_executor, observed_snapshot_id) =
        super::write_commit::build_admitted_data_write_commit_executor(
            &entry,
            &target.namespace,
            &target.table,
            preparation.target_ref().as_str(),
            preparation.intent(),
            BTreeMap::new(),
        )
        .map_err(invalid_iceberg_write_activation)?;
    let observed_metadata = commit_executor.table.metadata();
    if observed_snapshot_id != expected_snapshot_id
        || observed_metadata.uuid() != frozen_metadata.uuid()
        || observed_metadata.current_schema_id() != frozen_metadata.current_schema_id()
        || observed_metadata.default_partition_spec_id()
            != frozen_metadata.default_partition_spec_id()
        || observed_metadata.location() != frozen_metadata.location()
    {
        return Err(invalid_iceberg_write_activation(
            "managed Iceberg publication target no longer matches its exact preparation",
        ));
    }
    let provenance = novarocks_connector_iceberg::commit::MvProvenanceV1 {
        provenance_version: novarocks_connector_iceberg::commit::MV_PROVENANCE_VERSION,
        refresh_id: intent.refresh_id(),
        mv_id: intent.materialization_id(),
        token: intent.marker().to_string(),
        technique: match intent.technique() {
            ConnectorManagedPublicationTechnique::Full => {
                novarocks_connector_iceberg::commit::RefreshTechnique::Full
            }
            ConnectorManagedPublicationTechnique::Incremental => {
                novarocks_connector_iceberg::commit::RefreshTechnique::Incremental
            }
        },
        bases: intent
            .bases()
            .iter()
            .map(|base| novarocks_connector_iceberg::commit::ProvenanceBase {
                table_fqn: base.table.to_string(),
                uuid: base.uuid.to_string(),
                from_snapshot: base.from_version,
                to_snapshot: base.to_version,
            })
            .collect(),
        definition_fingerprint: intent.definition_fingerprint().to_string(),
        rows: 0,
    };
    let plan = IcebergFirstRefreshWritePlanPayloadV2 {
        version: 2,
        target: format!("{}.{}.{}", target.catalog, target.namespace, target.table),
        target_ref: preparation.target_ref().as_str().to_string(),
        expected_snapshot_id,
        staging_path: commit_executor.collector.staging_dir.clone(),
        provenance_properties: provenance
            .to_summary_properties()
            .map_err(invalid_iceberg_write_activation)?,
    };
    let empty_input_policy = match intent.empty_input() {
        ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit => {
            IcebergMvPrimaryEmptyInputPolicy::AbortWithoutSnapshot
        }
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite => {
            IcebergMvPrimaryEmptyInputPolicy::CommitEmptyOverwrite
        }
    };
    let Some(routed_preparations) = routed_preparations else {
        return register_iceberg_first_refresh_write_service_from_preparation(
            services,
            request.operation_id,
            &preparation,
            plan,
            &entry,
            commit_executor,
            empty_input_policy,
        );
    };
    let mut writer_payloads = Vec::with_capacity(routed_preparations.len());
    for (_, route_preparation) in &routed_preparations {
        route_preparation.validate()?;
        if route_preparation.owner() != preparation.owner()
            || route_preparation.table() != preparation.table()
            || route_preparation.target_ref() != preparation.target_ref()
            || route_preparation.base_version() != preparation.base_version()
        {
            return Err(invalid_iceberg_write_activation(
                "managed Iceberg row-mutation route drifted from its exact target",
            ));
        }
        let (_, payload) =
            iceberg_route_writer_handle_payload(route_preparation, &entry, &commit_executor.table)?;
        writer_payloads.push(payload);
    }
    let committer: Arc<dyn IcebergWriteReportCommitter> =
        Arc::new(IcebergFirstRefreshWriteReportCommitter::new(
            Arc::clone(&commit_executor),
            plan.provenance_properties.clone(),
            empty_input_policy,
        )?);
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.managed-routed-publication-activation.v1\0");
    hasher.update(request.operation_id.to_bytes());
    hasher.update(source_digest);
    hasher.update(plan.encode()?.as_ref());
    let activation_digest: [u8; 32] = hasher.finalize().into();
    let preparation_digest = preparation.digest();
    services.register_lazy(request.operation_id, activation_digest, move || {
        let context = IcebergWriteControlServiceContext::new_with_ordered_route_handle_payloads(
            selected_cohort,
            writer_payloads.clone(),
            plan.clone(),
            preparation_digest,
            Arc::clone(&committer),
        )?;
        Ok(Arc::new(IcebergWriteControlService::new(context)))
    })
}

/// Reserve a first-refresh writer from a sealed preparation.  The refresh
/// adapter supplies its opaque commit seam, while this provider boundary owns
/// the Iceberg sink/handle and the durable payload used by the control service.
pub(crate) fn register_iceberg_first_refresh_write_service_from_preparation(
    services: IcebergWriteServiceRegistry,
    operation_id: ConnectorWriteOperationId,
    preparation: &ConnectorWritePreparation,
    payload: IcebergFirstRefreshWritePlanPayloadV2,
    entry: &IcebergCatalogEntry,
    commit_executor: Arc<super::write_commit::IcebergWriteCommitExecutor>,
    empty_input_policy: IcebergMvPrimaryEmptyInputPolicy,
) -> Result<(), ConnectorError> {
    preparation.validate()?;
    validate_preparation_target_ref(preparation, &payload.target_ref)?;
    let sink_spec = iceberg_data_sink_spec_from_preparation(preparation, entry)?;
    let writer_handle_payload = encode_data_sink_spec_handle_payload(&sink_spec)
        .map_err(|error| invalid_iceberg_write_activation(error))?;
    let expected_target = format!(
        "{}.{}.{}",
        sink_spec.iceberg.catalog, sink_spec.iceberg.namespace, sink_spec.iceberg.table
    );
    let observed_snapshot_id = if payload.target_ref == "main" {
        commit_executor
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id())
    } else {
        commit_executor
            .table
            .metadata()
            .refs()
            .get(&payload.target_ref)
            .map(|reference| reference.snapshot_id)
    };
    if payload.target != expected_target
        || payload.expected_snapshot_id != observed_snapshot_id
        || payload.staging_path != commit_executor.collector.staging_dir
    {
        return Err(invalid_iceberg_write_activation(
            "first-refresh payload drifted from the provider-admitted target",
        ));
    }
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.first-refresh-preparation-activation.v1\0");
    hasher.update(operation_id.to_bytes());
    hasher.update(preparation.digest());
    hasher.update(payload.encode()?.as_ref());
    let activation_digest: [u8; 32] = hasher.finalize().into();
    let preparation_digest = preparation.digest();
    let committer: Arc<dyn IcebergWriteReportCommitter> =
        Arc::new(IcebergFirstRefreshWriteReportCommitter::new(
            commit_executor,
            payload.provenance_properties.clone(),
            empty_input_policy,
        )?);
    services.register_lazy(operation_id, activation_digest, move || {
        let context =
            IcebergWriteControlServiceContext::new_with_first_refresh_preparation_handle_payload(
                writer_handle_payload.clone(),
                payload.clone(),
                preparation_digest,
                Arc::clone(&committer),
            )?;
        Ok(Arc::new(IcebergWriteControlService::new(context)))
    })
}

/// Reserve a row-level writer service from a sealed Iceberg preparation.
///
/// The caller provides the already-opened table only for provider-local
/// deletion-vector state inspection.  This function compares it with the
/// preparation's frozen table before any writer payload is created, then
/// derives all field IDs, partition transforms, position descriptors, and
/// previous DV facts on the provider side.  Core and SQL therefore retain no
/// Iceberg sink specification or writer payload.
pub(crate) fn register_iceberg_row_write_service_from_preparation(
    services: IcebergWriteServiceRegistry,
    operation_id: ConnectorWriteOperationId,
    preparation: &ConnectorWritePreparation,
    target_ref: &str,
    entry: &IcebergCatalogEntry,
    _table: &novarocks_connector_iceberg::iceberg::table::Table,
    commit_executor: Arc<dyn IcebergWriteReportCommitter>,
) -> Result<(), ConnectorError> {
    preparation.validate()?;
    let expected_snapshot_id =
        iceberg_row_mutation_base_snapshot_from_preparation(preparation, target_ref)?;
    let table_target: TablePayload = decode_payload(
        preparation.table().payload(),
        "admitted Iceberg row-write service table",
    )?;
    let table_info = table_target.table_info.ok_or_else(|| {
        invalid_iceberg_write_activation(
            "admitted Iceberg row-write service is missing its frozen table descriptor",
        )
    })?;
    // The generic carrier never asks Core to refresh or interpret Iceberg
    // metadata.  Re-open here, under the provider's own cache ownership, and
    // fail before staging if the branch head differs from the signed match.
    entry.invalidate_table_cache(&table_info.namespace, &table_info.table);
    let table =
        super::catalog::registry::load_table(entry, &table_info.namespace, &table_info.table)
            .map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::Unavailable,
                    format!("reload Iceberg row-mutation target at activation: {error}"),
                )
            })?
            .into_table();
    let actual_snapshot_id = iceberg_ref_snapshot_from_metadata(table.metadata(), target_ref)?;
    if actual_snapshot_id != expected_snapshot_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!(
                "Iceberg row-mutation target ref `{target_ref}` changed after preparation: expected snapshot {expected_snapshot_id}, got {actual_snapshot_id}",
            ),
        ));
    }
    validate_preparation_table_against_open_table(preparation, table.metadata())?;
    let (sink_spec, writer_handle_payload) =
        iceberg_route_writer_handle_payload(preparation, entry, &table)?;
    register_iceberg_write_service_from_preparation_payload(
        services,
        operation_id,
        preparation,
        target_ref,
        sink_spec,
        writer_handle_payload,
        commit_executor,
    )
}

fn iceberg_route_writer_handle_payload(
    preparation: &ConnectorWritePreparation,
    entry: &IcebergCatalogEntry,
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<(IcebergWriteSinkSpec, bytes::Bytes), ConnectorError> {
    match preparation.input() {
        ConnectorWriteInputShape::Data { .. } | ConnectorWriteInputShape::RowLineage { .. } => {
            let sink_spec = iceberg_data_sink_spec_from_preparation(preparation, entry)?;
            let payload = encode_data_sink_spec_handle_payload(&sink_spec)
                .map_err(invalid_iceberg_write_activation)?;
            Ok((sink_spec, payload))
        }
        ConnectorWriteInputShape::PositionDelete { .. }
        | ConnectorWriteInputShape::DeletionVector { .. }
        | ConnectorWriteInputShape::EqualityDelete { .. } => {
            let (mut sink_spec, equality_columns) =
                iceberg_row_write_sink_spec_from_preparation(preparation, entry, table.metadata())?;
            let snapshot_id = iceberg_row_mutation_base_snapshot_from_preparation(
                preparation,
                preparation.target_ref().as_str(),
            )?;
            let snapshot_id = Some(snapshot_id);
            sink_spec
                .set_planned_snapshot_id(snapshot_id)
                .map_err(invalid_iceberg_write_activation)?;
            let payload = match sink_spec.mode {
                IcebergWriteSinkMode::PositionDeletes => {
                    let position_index_storage =
                        super::change_stream_write::position_delete_index_storage_config(
                            entry,
                            table.metadata().location(),
                        )
                        .map_err(invalid_iceberg_write_activation)?;
                    let partitions = super::sink::build_position_delete_data_file_partition_index(
                        table.metadata(),
                        snapshot_id,
                        table.metadata().location(),
                        position_index_storage.as_ref(),
                    )
                    .map_err(invalid_iceberg_write_activation)?;
                    encode_position_delete_sink_handle_payload(
                        &sink_spec,
                        table.metadata(),
                        &partitions,
                    )
                    .map_err(invalid_iceberg_write_activation)?
                }
                IcebergWriteSinkMode::DeletionVectors => {
                    super::change_stream_write::frozen_deletion_vector_handle_payload(
                        &sink_spec,
                        table,
                        entry,
                        snapshot_id,
                    )
                    .map_err(invalid_iceberg_write_activation)?
                }
                IcebergWriteSinkMode::EqualityDeletes => {
                    encode_equality_delete_sink_spec_handle_payload(
                        &sink_spec,
                        equality_columns.as_deref().ok_or_else(|| {
                            invalid_iceberg_write_activation(
                                "equality-delete preparation is missing provider field bindings",
                            )
                        })?,
                    )
                    .map_err(invalid_iceberg_write_activation)?
                }
                IcebergWriteSinkMode::Data | IcebergWriteSinkMode::RowLineageData => {
                    unreachable!("row-level preparation produced a data sink")
                }
            };
            Ok((sink_spec, payload))
        }
    }
}

fn register_iceberg_write_service_from_preparation_payload(
    services: IcebergWriteServiceRegistry,
    operation_id: ConnectorWriteOperationId,
    preparation: &ConnectorWritePreparation,
    target_ref: &str,
    sink_spec: IcebergWriteSinkSpec,
    writer_handle_payload: Bytes,
    commit_executor: Arc<dyn IcebergWriteReportCommitter>,
) -> Result<(), ConnectorError> {
    validate_preparation_target_ref(preparation, target_ref)?;
    let plan_payload = IcebergWritePlanPayloadV1 {
        version: 1,
        target: format!(
            "{}.{}.{}",
            sink_spec.iceberg.catalog, sink_spec.iceberg.namespace, sink_spec.iceberg.table
        ),
        target_ref: target_ref.to_string(),
    };
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg.write-preparation-activation.v1\0");
    hasher.update(operation_id.to_bytes());
    hasher.update(preparation.digest());
    let activation_digest: [u8; 32] = hasher.finalize().into();
    let preparation_digest = preparation.digest();
    services.register_lazy(operation_id, activation_digest, move || {
        let context = IcebergWriteControlServiceContext::new_with_preparation_handle_payload(
            writer_handle_payload.clone(),
            plan_payload.clone(),
            preparation_digest,
            Arc::clone(&commit_executor),
        )?;
        Ok(Arc::new(IcebergWriteControlService::new(context)))
    })
}

fn validate_preparation_target_ref(
    preparation: &ConnectorWritePreparation,
    target_ref: &str,
) -> Result<(), ConnectorError> {
    if preparation.target_ref().as_str() != target_ref {
        return Err(invalid_iceberg_write_activation(format!(
            "Iceberg write preparation targets ref `{}`, but activation requested `{target_ref}`",
            preparation.target_ref().as_str()
        )));
    }
    Ok(())
}

/// Rehydrate a data-file sink entirely within the Iceberg provider.  The
/// preparation's tagged input shape selects the generic writer mode; all
/// catalog field IDs, partition transforms, storage, and compression facts
/// stay in this module.
pub(crate) fn iceberg_data_sink_spec_from_preparation(
    preparation: &ConnectorWritePreparation,
    entry: &IcebergCatalogEntry,
) -> Result<IcebergWriteSinkSpec, ConnectorError> {
    let payload: TablePayload = decode_payload(
        preparation.table().payload(),
        "admitted Iceberg write preparation table",
    )?;
    let mut iceberg = payload.table_info.ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg write preparation is missing its frozen table descriptor",
        )
    })?;
    let serialized = iceberg.serialized_metadata.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg write preparation is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode admitted Iceberg write preparation metadata: {error}"),
        )
    })?;
    iceberg.current_snapshot_id =
        iceberg_write_target_snapshot_id(&metadata, preparation.target_ref().as_str())?;
    let mode = match preparation.input() {
        ConnectorWriteInputShape::Data { .. } => IcebergWriteSinkMode::Data,
        ConnectorWriteInputShape::RowLineage { .. } => IcebergWriteSinkMode::RowLineageData,
        ConnectorWriteInputShape::PositionDelete { .. }
        | ConnectorWriteInputShape::DeletionVector { .. }
        | ConnectorWriteInputShape::EqualityDelete { .. } => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "row-level Iceberg writer activation remains owned by the SPI-5C1 row-DML adapter",
            ));
        }
    };
    if matches!(mode, IcebergWriteSinkMode::RowLineageData) {
        iceberg.schema.fields.extend([
            novarocks_connector_iceberg::scan_model::IcebergSchemaFieldDef {
                field_id: iceberg_row_lineage::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                name: iceberg_row_lineage::ICEBERG_ROW_ID_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
            novarocks_connector_iceberg::scan_model::IcebergSchemaFieldDef {
                field_id:
                    iceberg_row_lineage::ICEBERG_RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER,
                name: iceberg_row_lineage::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            },
        ]);
    }
    let target_columns = preparation
        .input()
        .fields()
        .into_iter()
        .map(|binding| novarocks_catalog::schema::ColumnDef {
            name: binding.field().name().to_string(),
            data_type: binding.field().data_type().clone(),
            nullable: binding.field().is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect();
    let table_location = metadata.location().to_string();
    let data_location = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", table_location.trim_end_matches('/')));
    Ok(IcebergWriteSinkSpec {
        mode,
        iceberg,
        target_columns,
        table_location,
        data_location,
        target_partition_spec_id: metadata.default_partition_spec_id(),
        cloud_properties: entry.cloud_properties_map(),
        file_format: "parquet".to_string(),
        compression: IcebergWriteFileCompression::Snappy,
        position_delete_output_descriptor: None,
    })
}

/// Derive an Iceberg row-level sink from the signed, tagged input shape.
/// Each shape is validated against the frozen metadata before the descriptor
/// is serialized into the writer handle.  In particular, generic field names
/// never become Iceberg field IDs without this provider-owned lookup.
pub(crate) fn iceberg_row_write_sink_spec_from_preparation(
    preparation: &ConnectorWritePreparation,
    entry: &IcebergCatalogEntry,
    open_metadata: &TableMetadata,
) -> Result<
    (
        IcebergWriteSinkSpec,
        Option<Vec<novarocks_connector_iceberg::commit::EqualityDeleteColumn>>,
    ),
    ConnectorError,
> {
    let payload: TablePayload = decode_payload(
        preparation.table().payload(),
        "admitted Iceberg row-write preparation table",
    )?;
    let mut iceberg = payload.table_info.ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg row-write preparation is missing its frozen table descriptor",
        )
    })?;
    let serialized = iceberg.serialized_metadata.as_deref().ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "admitted Iceberg row-write preparation is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode admitted Iceberg row-write preparation metadata: {error}"),
        )
    })?;
    validate_row_write_metadata_matches_open_table(&metadata, open_metadata)?;
    let prepared_target_snapshot =
        iceberg_write_target_snapshot_id(&metadata, preparation.target_ref().as_str())?;
    let open_target_snapshot =
        iceberg_write_target_snapshot_id(open_metadata, preparation.target_ref().as_str())?;
    if prepared_target_snapshot != open_target_snapshot {
        return Err(invalid_iceberg_write_activation(
            "opened Iceberg write target ref no longer matches the sealed preparation",
        ));
    }

    let (mode, target_columns, position_delete_output_descriptor, equality_columns) =
        match preparation.input() {
            ConnectorWriteInputShape::PositionDelete {
                identity_fields,
                partition_source_fields,
            } => {
                let (columns, descriptor) = position_delete_columns_and_descriptor(
                    &metadata,
                    identity_fields,
                    partition_source_fields,
                )?;
                (
                    IcebergWriteSinkMode::PositionDeletes,
                    columns,
                    Some(descriptor),
                    None,
                )
            }
            ConnectorWriteInputShape::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => {
                let (columns, descriptor) = position_delete_columns_and_descriptor(
                    &metadata,
                    identity_fields,
                    partition_source_fields,
                )?;
                (
                    IcebergWriteSinkMode::DeletionVectors,
                    columns,
                    Some(descriptor),
                    None,
                )
            }
            ConnectorWriteInputShape::EqualityDelete { equality_fields } => {
                let (columns, equality_columns) =
                    equality_delete_columns_from_preparation(&metadata, equality_fields)?;
                (
                    IcebergWriteSinkMode::EqualityDeletes,
                    columns,
                    None,
                    Some(equality_columns),
                )
            }
            ConnectorWriteInputShape::Data { .. } | ConnectorWriteInputShape::RowLineage { .. } => {
                return Err(invalid_iceberg_write_activation(
                    "row-level Iceberg writer activation requires PositionDelete, DeletionVector, or EqualityDelete input",
                ));
            }
        };

    iceberg.current_snapshot_id = prepared_target_snapshot;
    let table_location = metadata.location().to_string();
    let data_location = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", table_location.trim_end_matches('/')));
    Ok((
        IcebergWriteSinkSpec {
            mode,
            iceberg,
            target_columns,
            table_location,
            data_location,
            target_partition_spec_id: metadata.default_partition_spec_id(),
            cloud_properties: entry.cloud_properties_map(),
            file_format: "parquet".to_string(),
            compression: IcebergWriteFileCompression::Snappy,
            position_delete_output_descriptor,
        },
        equality_columns,
    ))
}

fn validate_preparation_table_against_open_table(
    preparation: &ConnectorWritePreparation,
    open_metadata: &TableMetadata,
) -> Result<(), ConnectorError> {
    let payload: TablePayload = decode_payload(
        preparation.table().payload(),
        "admitted Iceberg row-write preparation table",
    )?;
    let iceberg = payload.table_info.ok_or_else(|| {
        invalid_iceberg_write_activation(
            "admitted Iceberg row-write preparation is missing its frozen table descriptor",
        )
    })?;
    let serialized = iceberg.serialized_metadata.as_deref().ok_or_else(|| {
        invalid_iceberg_write_activation(
            "admitted Iceberg row-write preparation is missing frozen metadata",
        )
    })?;
    let metadata: TableMetadata = serde_json::from_str(serialized).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode admitted Iceberg row-write preparation metadata: {error}"),
        )
    })?;
    validate_row_write_metadata_matches_open_table(&metadata, open_metadata)
}

fn validate_row_write_metadata_matches_open_table(
    prepared: &TableMetadata,
    open: &TableMetadata,
) -> Result<(), ConnectorError> {
    if prepared.uuid() != open.uuid()
        || prepared.current_snapshot_id() != open.current_snapshot_id()
        || prepared.current_schema_id() != open.current_schema_id()
        || prepared.default_partition_spec_id() != open.default_partition_spec_id()
    {
        return Err(invalid_iceberg_write_activation(
            "opened Iceberg table no longer matches the sealed row-write preparation",
        ));
    }
    Ok(())
}

fn position_delete_columns_and_descriptor(
    metadata: &TableMetadata,
    identity_fields: &[ConnectorWriteFieldBinding],
    partition_source_fields: &[ConnectorWriteFieldBinding],
) -> Result<
    (
        Vec<novarocks_catalog::schema::ColumnDef>,
        novarocks_connector_iceberg::position_delete_descriptor::PositionDeleteDescriptorInput,
    ),
    ConnectorError,
> {
    use novarocks_connector_iceberg::position_delete_descriptor::{
        ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
        ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        PositionDeleteOutputField, PositionDeletePartitionSourceField,
    };

    if identity_fields.len() != 2 {
        return Err(invalid_iceberg_write_activation(
            "Iceberg row-level write preparation requires exactly file-path and position identity fields",
        ));
    }
    let file = identity_fields[0].field();
    let pos = identity_fields[1].field();
    if !file
        .name()
        .eq_ignore_ascii_case(iceberg_row_lineage::ICEBERG_FILE_PATH_COL)
        || file.data_type() != &DataType::Utf8
        || file.is_nullable()
        || !pos
            .name()
            .eq_ignore_ascii_case(iceberg_row_lineage::ICEBERG_ROW_POS_COL)
        || pos.data_type() != &DataType::Int64
        || pos.is_nullable()
    {
        return Err(invalid_iceberg_write_activation(
            "Iceberg row-level write identity must be non-null `_file` UTF-8 followed by non-null `_pos` INT64",
        ));
    }
    let partition_fields = metadata.default_partition_spec().fields();
    if partition_source_fields.len() != partition_fields.len() {
        return Err(invalid_iceberg_write_activation(format!(
            "Iceberg row-level write preparation has {} partition source fields but the frozen table requires {}",
            partition_source_fields.len(),
            partition_fields.len()
        )));
    }

    let mut target_columns = Vec::with_capacity(2 + partition_source_fields.len());
    target_columns.push(column_def_from_binding(&identity_fields[0]));
    target_columns.push(column_def_from_binding(&identity_fields[1]));
    let schema = metadata.current_schema();
    let mut descriptor_fields = Vec::with_capacity(partition_source_fields.len());
    for (index, (partition, binding)) in partition_fields
        .iter()
        .zip(partition_source_fields.iter())
        .enumerate()
    {
        let source = schema.field_by_id(partition.source_id).ok_or_else(|| {
            invalid_iceberg_write_activation(format!(
                "Iceberg row-level write partition source field id {} is absent from the frozen schema",
                partition.source_id
            ))
        })?;
        let field = binding.field();
        if !field.name().eq_ignore_ascii_case(&source.name) {
            return Err(invalid_iceberg_write_activation(format!(
                "Iceberg row-level write partition source `{}` does not match frozen source `{}`",
                field.name(),
                source.name
            )));
        }
        target_columns.push(column_def_from_binding(binding));
        descriptor_fields.push(PositionDeletePartitionSourceField {
            output_expr_index: index + 2,
            source_column_name: source.name.clone(),
            partition_field_name: partition.name.clone(),
            transform_expr: super::write_contract::transform_to_sink_string(&partition.transform),
            source_field_id: partition.source_id,
            data_type: field.data_type().clone(),
        });
    }
    Ok((
        target_columns,
        novarocks_connector_iceberg::position_delete_descriptor::PositionDeleteDescriptorInput {
            file_path: PositionDeleteOutputField {
                output_expr_index: 0,
                name: ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
                data_type: DataType::Utf8,
                field_id: ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            },
            pos: PositionDeleteOutputField {
                output_expr_index: 1,
                name: ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
                data_type: DataType::Int64,
                field_id: ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            },
            partition_source_fields: descriptor_fields,
            target_partition_spec_id: metadata.default_partition_spec_id(),
        },
    ))
}

fn equality_delete_columns_from_preparation(
    metadata: &TableMetadata,
    fields: &[ConnectorWriteFieldBinding],
) -> Result<
    (
        Vec<novarocks_catalog::schema::ColumnDef>,
        Vec<novarocks_connector_iceberg::commit::EqualityDeleteColumn>,
    ),
    ConnectorError,
> {
    if fields.is_empty() {
        return Err(invalid_iceberg_write_activation(
            "Iceberg equality-delete write preparation requires at least one equality field",
        ));
    }
    if !metadata.default_partition_spec().is_unpartitioned() {
        return Err(invalid_iceberg_write_activation(
            "Iceberg connector equality-delete writer supports only unpartitioned tables",
        ));
    }
    let schema = metadata.current_schema();
    let mut columns = Vec::with_capacity(fields.len());
    let mut equality_columns = Vec::with_capacity(fields.len());
    for binding in fields {
        let field = binding.field();
        let iceberg_field = schema
            .as_struct()
            .fields()
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(field.name()))
            .ok_or_else(|| {
                invalid_iceberg_write_activation(format!(
                    "Iceberg equality-delete field `{}` is absent from the frozen schema",
                    field.name()
                ))
            })?;
        columns.push(column_def_from_binding(binding));
        equality_columns.push(novarocks_connector_iceberg::commit::EqualityDeleteColumn {
            name: field.name().to_string(),
            field_id: iceberg_field.id,
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        });
    }
    Ok((columns, equality_columns))
}

fn column_def_from_binding(
    binding: &ConnectorWriteFieldBinding,
) -> novarocks_catalog::schema::ColumnDef {
    let field = binding.field();
    novarocks_catalog::schema::ColumnDef {
        name: field.name().to_string(),
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
        write_default: None,
        logical_type: None,
    }
}

fn invalid_iceberg_write_activation(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

/// Provider facts admitted for one query table.  This is deliberately an
/// application/provider envelope rather than a SQL table: it preserves the
/// exact Iceberg descriptor, prepared files, statistics identity, and control
/// lease until `engine::query_planning` assigns a request-local binding token.
///
/// No `TableDef` or SQL scan source is carried here.  The application catalog
/// materializer is the only code that may project this envelope into SQL facts.
#[derive(Clone)]
pub(crate) struct IcebergQueryTableMaterialization {
    pub(crate) table_name: String,
    pub(crate) schema_id: Option<i32>,
    pub(crate) columns: Vec<novarocks_catalog::schema::ColumnDef>,
    /// SQL-visible columns as row DML must write them, which differs from
    /// `columns` only where the provider declared a write-target Arrow type.
    pub(crate) dml_target_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    /// Names of the columns the provider's current default partition spec
    /// derives from. Row DML uses this as a membership set to reject assigning
    /// a partition column; it carries no transform.
    pub(crate) partition_source_columns: Vec<String>,
    pub(crate) iceberg_row_lineage_metadata_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    /// The sole read authority handed back to the application boundary.  The
    /// provider may still inspect its payload while deriving SQL-owned facts,
    /// but Core receives this handle as opaque input to generic scan planning.
    pub(crate) read_table: ConnectorTableHandle,
    pub(crate) read_schema: SchemaRef,
    pub(crate) read_selector: ConnectorReadSelector,
    /// SQL-owned optimizer facts projected while the provider still owns the
    /// frozen metadata.  Core receives this value without decoding the table
    /// handle or serialized Iceberg metadata.
    pub(crate) sql_ukfk_facts: crate::sql::planner::table::SqlUkFkTableFacts,
    /// SQL-owned terminal write facts projected while this provider still
    /// owns the frozen Iceberg metadata.  Core retains only the opaque table
    /// handle contained in this value.
    pub(crate) write_target_admission:
        Option<crate::engine::query_planning::bindings::QueryWriteTargetAdmission>,
    table: novarocks_connector_iceberg::scan_model::IcebergTableInfo,
    pub(crate) files: Vec<IcebergDataFileInfo>,
    pub(crate) binding: IcebergDataFileBinding,
    pub(crate) statistics_pin: Option<ResolvedTableStatisticsPin>,
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

impl IcebergQueryTableMaterialization {
    /// SQL/application identity facts projected while the provider still owns
    /// the concrete Iceberg table descriptor.
    pub(crate) fn table_uuid(&self) -> Option<&str> {
        self.table.table_uuid.as_deref()
    }

    pub(crate) fn current_snapshot_id(&self) -> Option<i64> {
        self.table.current_snapshot_id
    }

    /// Ask the exact generation that produced this materialization to sign a
    /// terminal write.  The opaque table handle is carried directly into the
    /// request; Core neither decodes it nor reacquires a current generation.
    pub(crate) fn prepare_write(
        &self,
        target_ref: &str,
        intent: ConnectorWriteIntent,
        purpose: ConnectorWriteAdmissionPurpose,
        input: ConnectorWriteInputRequest,
        context: ConnectorRequestContext,
    ) -> Result<(ConnectorWriteLease, ConnectorWritePreparation), String> {
        let lease = self
            .planning_lease
            .derive_write_lease()
            .map_err(|error| format!("derive Iceberg materialization write lease: {error}"))?;
        let outcome = lease
            .control()
            .prepare_write(ConnectorWritePreparationRequest {
                table: self.read_table.clone(),
                target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse(target_ref)
                    .map_err(|error| format!("validate Iceberg write target ref: {error}"))?,
                intent,
                purpose,
                input,
                context,
            })
            .map_err(|error| format!("prepare Iceberg materialization write: {error}"))?;
        match outcome {
            ConnectorWritePreparationOutcome::Prepared(preparation) => Ok((lease, preparation)),
            ConnectorWritePreparationOutcome::Denied(error) => Err(format!(
                "Iceberg materialization write admission denied: {error}"
            )),
        }
    }

    /// Pure row-mutation admission under the exact retained lease.  This is
    /// intentionally usable before the frontend persists its operation intent;
    /// activation is a separate post-intent step and has no staging side effect.
    pub(crate) fn prepare_row_mutation(
        &self,
        target_ref: &str,
        operation_id: ConnectorWriteOperationId,
        intent: ConnectorRowMutationIntent,
        context: ConnectorRequestContext,
    ) -> Result<(ConnectorWriteLease, ConnectorRowMutationPreparation), String> {
        let lease = self
            .planning_lease
            .derive_write_lease()
            .map_err(|error| format!("derive Iceberg row-mutation write lease: {error}"))?;
        let preparation = match lease
            .prepare_row_mutation(ConnectorRowMutationPreparationRequest {
                operation_id,
                table: self.read_table.clone(),
                target_ref: ConnectorWriteTargetRef::parse(target_ref).map_err(|error| {
                    format!("validate Iceberg row-mutation target ref: {error}")
                })?,
                intent,
                context: context.clone(),
            })
            .map_err(|error| format!("prepare Iceberg row mutation: {error}"))?
        {
            ConnectorRowMutationPreparationOutcome::Prepared(preparation) => preparation,
            ConnectorRowMutationPreparationOutcome::Denied(error) => {
                return Err(format!("Iceberg row-mutation admission denied: {error}"));
            }
        };
        Ok((lease, preparation))
    }

    /// Activate an admitted logical mutation only after the frontend has
    /// durably recorded the operation intent. The exact lease and signed
    /// preparation make a generation refresh or ordinary-write fallback
    /// impossible at this boundary.
    pub(crate) fn activate_direct_row_mutation(
        &self,
        lease: &ConnectorWriteLease,
        preparation: ConnectorRowMutationPreparation,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorRowMutationExecutionPlan, String> {
        lease
            .activate_row_mutation(ConnectorRowMutationActivationRequest::Direct {
                preparation,
                context,
            })
            .map_err(|error| format!("activate Iceberg row mutation: {error}"))
    }

    /// Freeze an already admitted Iceberg file set into a new provider-owned
    /// opaque handle.  Callers never decode or rebuild the handle: MV target
    /// partition filtering may choose a subset of files, but only this
    /// provider boundary is allowed to turn that selection into read
    /// authority for generic connector planning.
    pub(crate) fn with_frozen_files(
        &self,
        files: Vec<IcebergDataFileInfo>,
        selector: ConnectorReadSelector,
    ) -> Result<Self, String> {
        let mut payload: TablePayload =
            decode_payload(self.read_table.payload(), "Iceberg admitted table handle")
                .map_err(|error| error.to_string())?;
        if payload.metadata_table_type.is_some() {
            return Err(
                "Iceberg metadata aliases cannot be materialized as MV target files".to_string(),
            );
        }
        payload.explicit_files = Some(files.clone());
        payload.prepared_files = files;
        let table = ConnectorTableHandle::try_new(
            self.read_table.owner().clone(),
            encode_payload(
                &payload,
                "Iceberg frozen read table",
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        Ok(Self {
            table_name: self.table_name.clone(),
            schema_id: self.schema_id,
            columns: self.columns.clone(),
            dml_target_columns: self.dml_target_columns.clone(),
            partition_source_columns: self.partition_source_columns.clone(),
            iceberg_row_lineage_metadata_columns: self.iceberg_row_lineage_metadata_columns.clone(),
            read_table: table,
            read_schema: self.read_schema.clone(),
            read_selector: selector,
            sql_ukfk_facts: self.sql_ukfk_facts.clone(),
            write_target_admission: None,
            table: self.table.clone(),
            files: Vec::new(),
            binding: IcebergDataFileBinding::ExplicitFiles,
            statistics_pin: self.statistics_pin.clone(),
            planning_lease: self.planning_lease.clone(),
        })
    }

    /// Freeze an explicit read set at a known snapshot without exposing the
    /// provider table descriptor to the COW rewrite admission path.
    pub(crate) fn with_frozen_files_at_snapshot(
        &self,
        files: Vec<IcebergDataFileInfo>,
        snapshot_id: i64,
    ) -> Result<Self, String> {
        let mut snapshot_materialization = self.clone();
        snapshot_materialization.table.current_snapshot_id = Some(snapshot_id);
        snapshot_materialization
            .with_frozen_files(files, ConnectorReadSelector::SnapshotId(snapshot_id))
    }
}

/// Freeze the exact IMV target read while the provider owns the table handle.
///
/// Both lanes intentionally reuse the same full-table authority. Affected
/// partition filtering is a performance optimization and must not force Core
/// to recover provider file tuples or concrete target-table state.
pub(crate) fn freeze_mv_target_reads(
    materialization: &IcebergQueryTableMaterialization,
    selector: ConnectorReadSelector,
) -> Result<
    (
        IcebergQueryTableMaterialization,
        IcebergQueryTableMaterialization,
    ),
    String,
> {
    let mut exact = materialization.clone();
    exact.read_selector = selector;
    if let ConnectorReadSelector::SnapshotId(snapshot_id) = selector {
        exact.table.current_snapshot_id = Some(snapshot_id);
    }
    Ok((exact.clone(), exact))
}

pub(crate) fn filter_frozen_mv_target_state_files(
    files: Vec<IcebergDataFileInfo>,
    filter: &crate::mv::model::TargetPartitionFilter,
    contract: Option<&crate::mv::persistence::schema::MvPartitionContract>,
    node_id: i32,
) -> Result<Vec<IcebergDataFileInfo>, String> {
    let crate::mv::model::TargetPartitionFilter::AllowList(allow_list) = filter else {
        return Ok(files);
    };
    if allow_list.is_empty() {
        return Ok(Vec::new());
    }
    let contract = contract.ok_or_else(|| {
        format!(
            "Iceberg MV target-state scan node_id={node_id} requires an affected-partition allow-list but its frozen binding has no target partition contract"
        )
    })?;
    files
        .into_iter()
        .filter_map(|file| match mv_target_partition_key(contract, &file) {
            Ok(key) if allow_list.contains(&key) => Some(Ok(file)),
            Ok(_) => None,
            Err(error) => Some(Err(format!(
                "Iceberg MV target-state scan node_id={node_id} cannot map frozen target file {} partition: {error}",
                file.path
            ))),
        })
        .collect()
}

fn mv_target_partition_key(
    contract: &crate::mv::persistence::schema::MvPartitionContract,
    file: &IcebergDataFileInfo,
) -> Result<crate::mv::model::MvPartitionKey, String> {
    let spec_id = file
        .partition_spec_id
        .ok_or_else(|| format!("target file {} is missing partition spec id", file.path))?;
    let mut fields = Vec::with_capacity(contract.fields.len());
    for partition_field in &contract.fields {
        let transform = mv_target_transform_text(&partition_field.transform).ok_or_else(|| {
            format!(
                "MV partition field {} uses unsupported void transform",
                partition_field.partition_field_name
            )
        })?;
        let value = file
            .partition_values
            .iter()
            .find(|value| {
                value
                    .source_column
                    .eq_ignore_ascii_case(&partition_field.source_column_name)
                    && value.transform.eq_ignore_ascii_case(&transform)
            })
            .or_else(|| {
                file.partition_values.iter().find(|value| {
                    value
                        .field_name
                        .eq_ignore_ascii_case(&partition_field.partition_field_name)
                        && value.transform.eq_ignore_ascii_case(&transform)
                })
            })
            .ok_or_else(|| {
                format!(
                    "target file {} has no partition value for {} with transform {}",
                    file.path, partition_field.partition_field_name, transform
                )
            })?;
        fields.push(crate::mv::model::MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            mv_target_partition_value(value)?,
        ));
    }
    Ok(crate::mv::model::MvPartitionKey::new(spec_id, fields))
}

fn mv_target_partition_value(
    value: &novarocks_connector_iceberg::scan_model::IcebergPartitionFieldValue,
) -> Result<crate::mv::model::MvPartitionValue, String> {
    use novarocks_connector_iceberg::scan_model::IcebergPartitionValue;

    match &value.value {
        None => Ok(crate::mv::model::MvPartitionValue::Null),
        Some(IcebergPartitionValue::Boolean(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::Int32(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::Int64(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::Float(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::Double(value)) => Ok(
            crate::mv::model::MvPartitionValue::String(value.to_string()),
        ),
        Some(IcebergPartitionValue::String(value)) => {
            Ok(crate::mv::model::MvPartitionValue::String(value.clone()))
        }
        Some(IcebergPartitionValue::Binary(_)) => Err(format!(
            "target partition field {} has unsupported binary value",
            value.field_name
        )),
    }
}

fn mv_target_transform_text(
    transform: &crate::mv::persistence::schema::MvPartitionTransformContract,
) -> Option<String> {
    use crate::mv::persistence::schema::MvPartitionTransformContract;

    match transform {
        MvPartitionTransformContract::Identity => Some("identity".to_string()),
        MvPartitionTransformContract::Year => Some("year".to_string()),
        MvPartitionTransformContract::Month => Some("month".to_string()),
        MvPartitionTransformContract::Day => Some("day".to_string()),
        MvPartitionTransformContract::Hour => Some("hour".to_string()),
        MvPartitionTransformContract::Bucket { num_buckets } => {
            Some(format!("bucket({num_buckets})"))
        }
        MvPartitionTransformContract::Truncate { width } => Some(format!("truncate({width})")),
        MvPartitionTransformContract::Void => None,
    }
}

/// A bounded reference to an Iceberg-owned rewrite artifact.  It is the only
/// special table handle C1 needs: the detailed file list is rehydrated by the
/// FE provider, then reaches BEs through ordinary opaque Iceberg splits.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FrozenRewriteSourcePayloadV1 {
    version: u16,
    operation_kind: String,
    group: super::distributed_rewrite::IcebergRewriteGroupPayloadV1,
}

pub(crate) fn frozen_rewrite_source_table_handle(
    original: &ConnectorTableHandle,
    operation: &novarocks_spi::connector::ConnectorDistributedRewriteOperation,
    group: super::distributed_rewrite::IcebergRewriteGroupPayloadV1,
) -> Result<ConnectorTableHandle, ConnectorError> {
    let table: TablePayload = decode_payload(original.payload(), "Iceberg rewrite source table")?;
    if table.metadata_table_type.is_some() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg distributed rewrite requires a base table source",
        ));
    }
    let payload = TablePayload {
        namespace: table.namespace,
        table: table.table,
        table_info: None,
        metadata_columns: Vec::new(),
        metadata_table_type: None,
        prepared_files: Vec::new(),
        explicit_files: None,
        logical_type_columns: BTreeMap::new(),
        hidden_columns: Vec::new(),
        frozen_rewrite: Some(FrozenRewriteSourcePayloadV1 {
            version: 1,
            operation_kind: operation.kind().to_string(),
            group,
        }),
    };
    ConnectorTableHandle::try_new(
        original.owner().clone(),
        encode_payload(
            &payload,
            "Iceberg frozen rewrite source table",
            novarocks_spi::connector::MAX_CONNECTOR_DISTRIBUTED_REWRITE_PROVIDER_PAYLOAD_BYTES,
        )?,
    )
}

/// Convert an invisible staged-create table into the same opaque Iceberg table
/// carrier used by normal Provider-signed write preparation. The staged-create
/// adapter may prove ownership of the invisible table, but generic CTAS must
/// never pass the staged-create handle payload to the normal table decoder.
pub(crate) fn staged_iceberg_write_table_handle(
    owner: ConnectorInstanceId,
    table: &novarocks_connector_iceberg::iceberg::table::Table,
) -> Result<ConnectorTableHandle, ConnectorError> {
    let metadata = table.metadata();
    let ident = table.identifier();
    let table_info = novarocks_connector_iceberg::scan_model::IcebergTableInfo {
        catalog: owner.as_str().to_string(),
        namespace: ident.namespace.to_string(),
        table: ident.name.clone(),
        table_uuid: Some(metadata.uuid().to_string()),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        location: metadata.location().to_string(),
        schema: iceberg_schema_def(metadata.current_schema()),
        serialized_metadata: Some(serde_json::to_string(metadata).map_err(|error| {
            internal(format!("serialize staged Iceberg write metadata: {error}"))
        })?),
        serialized_metadata_rows: None,
    };
    let payload = TablePayload {
        namespace: ident.namespace.to_string(),
        table: ident.name.clone(),
        table_info: Some(table_info),
        metadata_columns: iceberg_metadata_column_names(metadata),
        metadata_table_type: None,
        prepared_files: Vec::new(),
        explicit_files: None,
        logical_type_columns: BTreeMap::new(),
        hidden_columns: super::catalog::backend::hidden_internal_column_names_from_metadata(
            metadata,
        ),
        frozen_rewrite: None,
    };
    ConnectorTableHandle::try_new(
        owner,
        encode_payload(
            &payload,
            "Iceberg staged write table",
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        )?,
    )
}

pub(crate) fn decode_data_mutation_table_target(
    handle: &novarocks_spi::connector::ConnectorTableHandle,
) -> Result<(String, String), ConnectorError> {
    let payload: TablePayload = decode_payload(handle.payload(), "data mutation table handle")?;
    if payload.metadata_table_type.is_some() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg data mutation requires a base table handle",
        ));
    }
    Ok((payload.namespace, payload.table))
}

#[derive(Clone, Deserialize, Serialize)]
struct ScanPayload {
    table: TablePayload,
    snapshot_id: Option<i64>,
    #[serde(default)]
    table_uuid: Option<String>,
    projection: Vec<usize>,
    limit: Option<u64>,
    #[serde(default)]
    physical_predicates: Vec<IcebergPhysicalPredicate>,
    #[serde(default)]
    fact_columns: Vec<IcebergScanFactColumnV1>,
}

fn scan_fact_columns(
    output_schema: &SchemaRef,
    projection: &[usize],
    table: &TablePayload,
) -> Result<Vec<IcebergScanFactColumnV1>, ConnectorError> {
    if table.metadata_table_type.is_some() || table.frozen_rewrite.is_some() {
        return Ok(Vec::new());
    }
    let Some(table_info) = table.table_info.as_ref() else {
        return Ok(Vec::new());
    };
    let indexes = if projection.is_empty() {
        (0..output_schema.fields().len()).collect::<Vec<_>>()
    } else {
        projection.to_vec()
    };
    if indexes.len() != output_schema.fields().len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg output schema does not match its frozen projection",
        ));
    }
    let mut columns = indexes
        .into_iter()
        .zip(output_schema.fields())
        .map(|(ordinal, field)| {
            if is_iceberg_metadata_column(field.name()) {
                return Ok(None);
            }
            let field_ordinal = u32::try_from(ordinal).map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg table-schema ordinal does not fit u32",
                )
            })?;
            let table_field = table_info.schema.fields.get(ordinal).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg projection index {ordinal} is outside the frozen table schema"
                    ),
                )
            })?;
            if !table_field.name.eq_ignore_ascii_case(field.name()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg frozen table schema disagrees with its output schema",
                ));
            }
            Ok(Some(IcebergScanFactColumnV1 {
                field_ordinal,
                field_id: table_field.field_id,
                canonical_name: table_field.name.to_ascii_lowercase(),
                scalar_type: scan_fact_scalar_type(field.data_type()),
                nullable: field.is_nullable(),
            }))
        })
        .collect::<Result<Vec<Option<_>>, _>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    // The projection preserves planner output order, which can differ from the
    // frozen table schema. Domain facts are sealed in table-schema ordinal
    // order so their constructor can reject duplicate or reordered entries.
    columns.sort_by_key(|column| column.field_ordinal);
    Ok(columns)
}

fn split_name_mapping(table: &TablePayload) -> Result<Option<String>, ConnectorError> {
    let Some(serialized_metadata) = table
        .table_info
        .as_ref()
        .and_then(|table| table.serialized_metadata.as_deref())
    else {
        return Ok(None);
    };
    let metadata: novarocks_connector_iceberg::iceberg::spec::TableMetadata =
        serde_json::from_str(serialized_metadata).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode pinned Iceberg metadata for name mapping: {error}"),
            )
        })?;
    let Some(mapping) = metadata
        .properties()
        .get(novarocks_connector_iceberg::iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
    else {
        return Ok(None);
    };
    canonical_split_name_mapping(mapping).map(Some)
}

fn rewrite_position_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_file", DataType::Utf8, false),
        Field::new("_pos", DataType::Int64, false),
    ]))
}

/// Resolve a base table into the application-owned admission envelope.  New
/// query planning callers must use this instead of receiving a legacy
/// `TableDef`: the concrete descriptor remains outside SQL until a
/// `SqlTableBindingId` has been allocated.
pub(crate) fn load_schema_materialization_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<IcebergQueryTableMaterialization, String> {
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let planning_lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    load_schema_materialization_from_exact_lease(planning_lease, context, namespace, table)
}

/// Materialize a base table from the caller's exact control generation.
///
/// This is an application-facing admission helper for flows which have already
/// frozen a `ConnectorControlPlanningLease`. It deliberately does not accept a
/// resolver or catalog identity, so it cannot reacquire a newer generation
/// while resolving schema facts. The returned envelope retains the supplied
/// lease through query-local binding and preparation.
pub(crate) fn load_schema_materialization_from_exact_lease(
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    namespace: &str,
    table: &str,
) -> Result<IcebergQueryTableMaterialization, String> {
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };

    let instance_id = planning_lease.binding().descriptor().instance_id.clone();
    let metadata = planning_lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context,
        })
        .map_err(|error| error.to_string())?;
    materialization_from_metadata(
        metadata,
        planning_lease,
        IcebergDataFileBinding::CurrentSnapshot,
    )
}

/// Resolve one fixed snapshot without manufacturing a synthetic catalog table.
/// The selector is carried as an application fact here and later projected to
/// `SqlTableVersionSelector::Snapshot` with the same request binding token.
pub(crate) fn load_time_travel_materialization_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    snapshot_id: i64,
) -> Result<IcebergQueryTableMaterialization, String> {
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let planning_lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    load_snapshot_materialization_from_exact_lease(
        planning_lease,
        context,
        namespace,
        table,
        snapshot_id,
    )
}

/// Freeze a snapshot selector against the supplied exact provider generation.
/// The provider retains the opaque table handle and resolves its own snapshot
/// files at scan planning; Core receives only the selector and SQL facts.
pub(crate) fn load_snapshot_materialization_from_exact_lease(
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    namespace: &str,
    table: &str,
    snapshot_id: i64,
) -> Result<IcebergQueryTableMaterialization, String> {
    let mut materialization =
        load_schema_materialization_from_exact_lease(planning_lease, context, namespace, table)?;
    materialization.table.current_snapshot_id = Some(snapshot_id);
    materialization.binding = IcebergDataFileBinding::ExplicitFiles;
    materialization.read_selector = ConnectorReadSelector::SnapshotId(snapshot_id);
    materialization.files.clear();
    Ok(materialization)
}

/// Resolve an Iceberg metadata alias into an admission envelope.  Metadata
/// rows and provider descriptors stay application-owned until the catalog
/// materializer creates the tokenized SQL metadata scan.
pub(crate) fn load_metadata_materialization_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    metadata_table_kind: crate::sql::planner::table::SqlMetadataTableKind,
) -> Result<IcebergQueryTableMaterialization, String> {
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };

    let metadata_table_type = iceberg_metadata_table_type_from_sql_kind(metadata_table_kind);
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let planning_lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let alias = format!("{table}${}", metadata_table_name(&metadata_table_type));
    let metadata = planning_lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(alias),
            },
            resolution: ConnectorTableResolution::ProviderReadAlias,
            context,
        })
        .map_err(|error| error.to_string())?;
    materialization_from_metadata(
        metadata,
        planning_lease,
        IcebergDataFileBinding::CurrentSnapshot,
    )
}

fn iceberg_metadata_table_type_from_sql_kind(
    kind: crate::sql::planner::table::SqlMetadataTableKind,
) -> super::IcebergMetadataTableType {
    match kind {
        crate::sql::planner::table::SqlMetadataTableKind::Snapshots => {
            super::IcebergMetadataTableType::Snapshots
        }
        crate::sql::planner::table::SqlMetadataTableKind::History => {
            super::IcebergMetadataTableType::History
        }
        crate::sql::planner::table::SqlMetadataTableKind::Refs => {
            super::IcebergMetadataTableType::Refs
        }
        crate::sql::planner::table::SqlMetadataTableKind::Files => {
            super::IcebergMetadataTableType::Files
        }
        crate::sql::planner::table::SqlMetadataTableKind::Manifests => {
            super::IcebergMetadataTableType::Manifests
        }
        crate::sql::planner::table::SqlMetadataTableKind::Partitions => {
            super::IcebergMetadataTableType::Partitions
        }
        crate::sql::planner::table::SqlMetadataTableKind::LogicalIcebergMetadata => {
            super::IcebergMetadataTableType::LogicalIcebergMetadata
        }
    }
}

fn materialization_from_metadata(
    metadata: ConnectorTableMetadata,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    binding: IcebergDataFileBinding,
) -> Result<IcebergQueryTableMaterialization, String> {
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    let statistics_pin = metadata
        .statistics_data_version
        .clone()
        .map(|data_version| ResolvedTableStatisticsPin {
            table: metadata.table.clone(),
            data_version,
        });
    // Snapshot selection is an application fact supplied by the caller which
    // froze it.  Base materialization is always current and never decodes the
    // opaque table handle to rediscover a provider-private snapshot id.
    let read_selector = ConnectorReadSelector::Current;
    let payload: TablePayload = decode_payload(metadata.table.payload(), "table handle")
        .map_err(|error| error.to_string())?;
    let table = payload
        .table_info
        .ok_or_else(|| "Iceberg SPI table metadata is missing its read descriptor".to_string())?;
    let sql_ukfk_facts =
        crate::sql::planner::table::SqlUkFkTableFacts::from_connector_planning_facts(
            &metadata.schema,
            &metadata.planning_facts,
        );
    let (columns, iceberg_row_lineage_metadata_columns) =
        columns_from_planning_facts(&metadata.schema, &metadata.planning_facts);
    let dml_target_columns =
        dml_target_columns_from_planning_facts(&metadata.schema, &metadata.planning_facts);
    let partition_source_columns = metadata
        .planning_facts
        .partition_source_column_ordinals()
        .iter()
        .filter_map(|ordinal| {
            metadata
                .schema
                .fields()
                .get(*ordinal as usize)
                .map(|field| field.name().to_string())
        })
        .collect();
    Ok(IcebergQueryTableMaterialization {
        table_name: payload.table,
        schema_id,
        columns,
        dml_target_columns,
        partition_source_columns,
        iceberg_row_lineage_metadata_columns,
        read_table: metadata.table,
        read_schema: metadata.schema,
        read_selector,
        sql_ukfk_facts,
        write_target_admission: None,
        table,
        files: payload.prepared_files,
        binding,
        statistics_pin,
        planning_lease,
    })
}

pub(crate) fn row_lineage_sink_spec_from_frozen_materialization(
    materialization: &IcebergQueryTableMaterialization,
    entry: &IcebergCatalogEntry,
) -> Result<IcebergWriteSinkSpec, String> {
    let serialized = materialization
        .table
        .serialized_metadata
        .as_deref()
        .ok_or_else(|| {
            "frozen Iceberg row-lineage target is missing serialized metadata".to_string()
        })?;
    let metadata: novarocks_connector_iceberg::iceberg::spec::TableMetadata =
        serde_json::from_str(serialized).map_err(|error| {
            format!("decode frozen Iceberg row-lineage target metadata: {error}")
        })?;
    let mut target_columns = materialization.columns.clone();
    target_columns.extend([
        novarocks_catalog::schema::ColumnDef {
            name: iceberg_row_lineage::ICEBERG_ROW_ID_COL.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        },
        novarocks_catalog::schema::ColumnDef {
            name: iceberg_row_lineage::ICEBERG_LAST_UPDATED_SEQ_COL.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        },
    ]);
    let table_location = metadata.location().to_string();
    let data_location = metadata
        .properties()
        .get("write.data.path")
        .cloned()
        .unwrap_or_else(|| format!("{}/data", table_location.trim_end_matches('/')));
    Ok(IcebergWriteSinkSpec {
        mode: IcebergWriteSinkMode::RowLineageData,
        iceberg: materialization.table.clone(),
        target_columns,
        table_location,
        data_location,
        target_partition_spec_id: metadata.default_partition_spec_id(),
        cloud_properties: entry.cloud_properties_map(),
        file_format: "parquet".to_string(),
        compression: IcebergWriteFileCompression::Snappy,
        position_delete_output_descriptor: None,
    })
}

/// Row-DML target columns, which are the SQL-visible columns with each
/// provider-declared write-target Arrow type applied.
///
/// The read schema and the write target disagree for Iceberg variant and binary
/// columns. That divergence is a known defect; this keeps it byte-identical to
/// what row DML produced when it decoded the Iceberg schema itself, while making
/// the provider the one that states it.
fn dml_target_columns_from_planning_facts(
    schema: &Schema,
    facts: &ConnectorTablePlanningFacts,
) -> Vec<novarocks_catalog::schema::ColumnDef> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(ordinal, field)| {
            let fact = facts.column_facts().get(ordinal);
            if matches!(
                fact.map(|fact| fact.visibility()),
                Some(ConnectorTableColumnVisibility::Hidden)
            ) || matches!(
                fact.map(|fact| fact.role()),
                Some(ConnectorTableColumnRole::RowLineageSystem)
            ) {
                return None;
            }
            let data_type = fact
                .and_then(|fact| fact.write_target_type())
                .cloned()
                .unwrap_or_else(|| field.data_type().clone());
            Some(novarocks_catalog::schema::ColumnDef {
                name: field.name().to_string(),
                data_type,
                nullable: field.is_nullable(),
                write_default: None,
                logical_type: None,
            })
        })
        .collect()
}

fn columns_from_planning_facts(
    schema: &Schema,
    facts: &ConnectorTablePlanningFacts,
) -> (
    Vec<novarocks_catalog::schema::ColumnDef>,
    Vec<novarocks_catalog::schema::ColumnDef>,
) {
    let mut columns = Vec::new();
    let mut system_columns = Vec::new();
    for (ordinal, field) in schema.fields().iter().enumerate() {
        let fact = facts.column_facts().get(ordinal);
        let logical_type = match fact.map(|fact| fact.semantic_kind()) {
            Some(ConnectorTableColumnSemanticKind::Bitmap) => Some(SqlType::Bitmap),
            Some(ConnectorTableColumnSemanticKind::Hll) => Some(SqlType::Hll),
            _ => None,
        };
        let column = novarocks_catalog::schema::ColumnDef {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type,
        };
        match fact.map(|fact| fact.role()) {
            Some(ConnectorTableColumnRole::RowLineageSystem) => system_columns.push(column),
            _ if matches!(
                fact.map(|fact| fact.visibility()),
                Some(ConnectorTableColumnVisibility::Hidden)
            ) => {}
            _ => columns.push(column),
        }
    }
    (columns, system_columns)
}

fn metadata_table_name(metadata_type: &super::IcebergMetadataTableType) -> &'static str {
    match metadata_type {
        super::IcebergMetadataTableType::Files => "FILES",
        super::IcebergMetadataTableType::Manifests => "MANIFESTS",
        super::IcebergMetadataTableType::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
        super::IcebergMetadataTableType::Snapshots => "SNAPSHOTS",
        super::IcebergMetadataTableType::History => "HISTORY",
        super::IcebergMetadataTableType::Refs => "REFS",
        super::IcebergMetadataTableType::Partitions => "PARTITIONS",
    }
}

fn sql_metadata_table_kind(
    metadata_type: &super::IcebergMetadataTableType,
) -> crate::sql::planner::table::SqlMetadataTableKind {
    match metadata_type {
        super::IcebergMetadataTableType::Files => {
            crate::sql::planner::table::SqlMetadataTableKind::Files
        }
        super::IcebergMetadataTableType::Manifests => {
            crate::sql::planner::table::SqlMetadataTableKind::Manifests
        }
        super::IcebergMetadataTableType::LogicalIcebergMetadata => {
            crate::sql::planner::table::SqlMetadataTableKind::LogicalIcebergMetadata
        }
        super::IcebergMetadataTableType::Snapshots => {
            crate::sql::planner::table::SqlMetadataTableKind::Snapshots
        }
        super::IcebergMetadataTableType::History => {
            crate::sql::planner::table::SqlMetadataTableKind::History
        }
        super::IcebergMetadataTableType::Refs => {
            crate::sql::planner::table::SqlMetadataTableKind::Refs
        }
        super::IcebergMetadataTableType::Partitions => {
            crate::sql::planner::table::SqlMetadataTableKind::Partitions
        }
    }
}

#[cfg(test)]
pub(crate) fn planned_split_data_file_for_test(
    split: &ConnectorSplit,
) -> Result<IcebergDataFileInfo, String> {
    decode_payload::<SplitPayload>(split.payload(), "test Iceberg split")
        .and_then(|payload| {
            payload
                .units
                .into_iter()
                .next()
                .map(|unit| unit.data_file)
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "test Iceberg split has no units",
                    )
                })
        })
        .map_err(|error| error.to_string())
}

/// Plans an Iceberg read against a control generation selected during query
/// metadata resolution.  Callers retain the returned lease in the prepared
/// execution binding through the backend ensure barrier; this path must not
/// acquire a newer control generation.
pub(crate) fn plan_native_iceberg_read_with_lease(
    lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &novarocks_connector_iceberg::scan_model::IcebergTableInfo,
    binding: novarocks_connector_iceberg::scan_model::IcebergDataFileBinding,
    explicit_files: &[IcebergDataFileInfo],
    projection: &[usize],
    static_predicates: Vec<ConnectorStaticPredicate>,
    target_parallelism: NonZeroUsize,
    max_split_bytes: Option<NonZeroU64>,
) -> Result<PlannedIcebergConnectorRead, String> {
    plan_native_iceberg_read_with_bound_lease(
        lease,
        context,
        table,
        binding,
        Some(explicit_files),
        projection,
        static_predicates,
        target_parallelism,
        max_split_bytes,
    )
}

fn plan_native_iceberg_read_with_bound_lease(
    lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &novarocks_connector_iceberg::scan_model::IcebergTableInfo,
    binding: novarocks_connector_iceberg::scan_model::IcebergDataFileBinding,
    explicit_files: Option<&[IcebergDataFileInfo]>,
    projection: &[usize],
    static_predicates: Vec<ConnectorStaticPredicate>,
    target_parallelism: NonZeroUsize,
    max_split_bytes: Option<NonZeroU64>,
) -> Result<PlannedIcebergConnectorRead, String> {
    let instance_id =
        ConnectorInstanceId::parse(&table.catalog).map_err(|error| error.to_string())?;
    let control_binding = lease.binding();
    if control_binding.descriptor().instance_id != instance_id {
        return Err(format!(
            "Iceberg planning lease owner '{:?}' does not match table catalog '{}'",
            control_binding.descriptor().instance_id,
            table.catalog
        ));
    }
    let declaration = control_binding
        .execution_declaration(&context)
        .map_err(|error| error.to_string())?;
    // A fixed snapshot can arrive without a materialized file list when it
    // was loaded through the schema metadata path (notably the synthetic
    // time-travel table used by an MV full refresh). Keep an explicit list
    // authoritative when present, but otherwise let the connector resolve
    // the requested snapshot instead of treating the empty list as an empty
    // table.
    let use_explicit_files = matches!(
        binding,
        novarocks_connector_iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles
    ) && explicit_files.is_some_and(|files| !files.is_empty());
    let table_handle = ConnectorTableHandle::try_new(
        instance_id.clone(),
        encode_payload(
            &TablePayload {
                namespace: table.namespace.clone(),
                table: table.table.clone(),
                table_info: Some(table.clone()),
                metadata_columns: Vec::new(),
                metadata_table_type: None,
                prepared_files: Vec::new(),
                explicit_files: use_explicit_files
                    .then(|| explicit_files.expect("non-empty explicit files").to_vec()),
                logical_type_columns: BTreeMap::new(),
                hidden_columns: Vec::new(),
                frozen_rewrite: None,
            },
            "table handle",
            context.max_handle_payload_bytes(),
        )
        .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let selection = ConnectorScanSelection::Snapshot(
        table
            .current_snapshot_id
            .filter(|_| {
                matches!(
                    binding,
                    novarocks_connector_iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles
                )
            })
            .map(ConnectorReadSelector::SnapshotId)
            .unwrap_or(ConnectorReadSelector::Current),
    );
    let scan = control_binding
        .planning()
        .begin_scan(
            &table_handle,
            novarocks_spi::connector::ConnectorBeginScanRequest {
                projection: projection.to_vec(),
                static_predicates: static_predicates.clone(),
                selection,
                purpose: ConnectorReadPurpose::Query,
                limit: None,
                batch: novarocks_spi::connector::ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(4096).expect("batch rows are nonzero"),
                    max_bytes: NonZeroUsize::new(
                        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                    )
                    .expect("batch bytes are nonzero"),
                },
                context: context.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
    scan.validate(
        &ConnectorExecutionBindingKey {
            instance_id: control_binding.descriptor().instance_id.clone(),
            incarnation: control_binding.incarnation(),
        },
        selection,
    )
    .map_err(|error| error.to_string())?;
    normalize_predicate_dispositions(&static_predicates, scan.predicate_dispositions())
        .map_err(|error| format!("Iceberg connector static predicate response: {error}"))?;
    let split_result = control_binding
        .planning()
        .plan_splits(
            scan.handle(),
            ConnectorSplitPlanningRequest {
                target_parallelism,
                max_split_bytes,
                context,
            },
        )
        .map_err(|error| error.to_string())?;
    let splits = split_result.splits;
    if splits
        .iter()
        .any(|split| split.owner() != &control_binding.descriptor().instance_id)
    {
        return Err("Iceberg connector planned a split for another instance".to_string());
    }
    Ok(PlannedIcebergConnectorRead {
        declaration,
        planning_lease: lease,
        scan,
        splits,
        planning_metrics: split_result.metrics,
        batch: novarocks_spi::connector::ConnectorBatchBudget {
            max_rows: NonZeroUsize::new(4096).expect("batch rows are nonzero"),
            max_bytes: NonZeroUsize::new(
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )
            .expect("batch bytes are nonzero"),
        },
    })
}

pub(crate) struct PlannedIcebergConnectorRead {
    pub(crate) declaration: ConnectorExecutionDeclaration,
    pub(crate) planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    pub(crate) scan: ConnectorScan,
    pub(crate) splits: Vec<ConnectorSplit>,
    pub(crate) planning_metrics: ConnectorSplitPlanningMetrics,
    pub(crate) batch: novarocks_spi::connector::ConnectorBatchBudget,
}

fn ensure_owner(
    owner: &ConnectorInstanceId,
    expected: &ConnectorInstanceId,
) -> Result<(), ConnectorError> {
    if owner == expected {
        Ok(())
    } else {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector handle belongs to a different instance",
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn push_split_with_budget(
    splits: &mut Vec<ConnectorSplit>,
    total_payload_bytes: &mut usize,
    owner: ConnectorInstanceId,
    split_id: String,
    payload: &SplitPayload,
    estimated_bytes: Option<u64>,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    // Encoding one candidate is bounded by the per-handle budget. Admit its
    // bytes against the aggregate budget before allocating/pushing the opaque
    // split, so a rejected high-cardinality plan never builds the full split
    // vector transiently.
    let payload = encode_payload(payload, "split", context.max_handle_payload_bytes())?;
    let next_total = total_payload_bytes
        .checked_add(payload.len())
        .filter(|total| *total <= context.max_total_payload_bytes())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg split payloads exceed the request budget",
            )
        })?;
    let split = ConnectorSplit::try_new(owner, split_id, payload, estimated_bytes)?;
    splits.push(split);
    *total_payload_bytes = next_total;
    Ok(())
}

fn encode_payload(
    payload: &impl Serialize,
    subject: &str,
    max_payload_bytes: usize,
) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(payload)
        .map_err(|error| internal(format!("serialize Iceberg {subject}: {error}")))
        .and_then(|payload| {
            if payload.len() > max_payload_bytes {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    format!("Iceberg {subject} exceeds the request payload budget"),
                ));
            }
            Ok(Bytes::from(payload))
        })
}

fn decode_payload<T: for<'de> Deserialize<'de>>(
    payload: &Bytes,
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("decode Iceberg {subject}: {error}"),
        )
    })
}

fn map_iceberg_error(error: String) -> ConnectorError {
    let normalized = error.to_ascii_lowercase();
    let kind = if normalized.contains("not found")
        || normalized.contains("does not exist")
        // `load_table` normalizes an absent table across the local/Hadoop and
        // REST paths to this stable text.  Mutation existence policies must
        // interpret it as absence, rather than turning CREATE IF NOT EXISTS
        // (and ordinary CREATE) into an internal error before dispatch.
        || normalized.contains("unknown table")
        || normalized.contains("unknown view")
        || normalized.contains("no metadata files")
        // The local Hadoop catalog uses this normalized absence error when
        // probing a table's metadata directory.  Catalog mutations perform
        // that probe before CREATE TABLE, so it must retain NotFound semantics
        // rather than being surfaced as an internal failure.
        || normalized.contains("unknown table:")
    {
        ConnectorErrorKind::NotFound
    } else if normalized.contains("format-version 3")
        || normalized.contains("nanosecond")
        || normalized.contains("invalid partition")
        || normalized.contains("variant columns cannot appear in the partition spec")
        || normalized.contains("unsupported iceberg type evolution")
        || normalized.contains("decimal scale change is not allowed")
        || normalized.contains("decimal precision must strictly increase")
        || normalized.contains("parent path must point to a struct")
        || (normalized.contains("iceberg column `") && normalized.contains("already exists"))
        || (normalized.contains("column path '") && normalized.contains("not found"))
        || normalized.contains("format-version is reserved")
        || normalized.contains("iceberg internal metadata key")
        || normalized.contains("novarocks.* namespace is reserved")
        || normalized.contains("variant columns cannot appear in the partition spec")
        // Partition-spec evolution rejections raised by
        // `build_evolved_partition_spec` before any commit is dispatched. These
        // reach this classifier now that ALTER TABLE ADD PARTITION COLUMN stopped
        // duplicating the check in the SQL layer; without them the same
        // rejections would be reported as commit-unknown rather than as the
        // validation errors they are.
        || normalized.contains("temporal partition transform requires")
        || normalized.contains("already exists in current default spec")
    {
        // These are local Iceberg semantic rejections, before any catalog
        // commit can have been dispatched.  Keeping them out of the unknown
        // outcome path preserves the SQL-level validation error instead of
        // replacing it with reconciliation evidence.
        ConnectorErrorKind::InvalidRequest
    } else {
        ConnectorErrorKind::Internal
    };
    ConnectorError::new(kind, error)
}

fn internal(message: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::num::NonZeroUsize;
    use std::path::Path;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;
    use novarocks_connector_iceberg::file_reader::execution_payload::{
        IcebergScanFactScalarTypeV1, iceberg_unit_domain_facts, validate_split_payload,
    };
    use novarocks_connector_iceberg::scan_model::{
        IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
    };
    use novarocks_spi::connector::{
        ConnectorScanUnitColumnDomain, ConnectorScanUnitColumnFacts, ConnectorScanUnitFactsEvidence,
    };

    fn explicit_test_read_binding(runtime: &tokio::runtime::Runtime) -> IcebergReadBinding {
        let handle = runtime.handle().clone();
        IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle)),
        )
    }

    #[test]
    fn unknown_table_catalog_error_is_not_found() {
        let error = map_iceberg_error("unknown table: analytics.orders".to_string());
        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    }

    #[test]
    fn spi5c1_row_write_input_keeps_identity_and_partition_tokens_separate() {
        let lease = fixture_planning_lease("iceberg");
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg").expect("fixture instance ID"),
            incarnation: lease.binding().incarnation(),
        };
        let metadata = lease
            .binding()
            .metadata()
            .load_table(ConnectorTableRequest {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: owner.instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("orders"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: crate::connector::test_request_context(),
            })
            .expect("fixture table admission");
        let request = ConnectorWritePreparationRequest {
            table: metadata.table,
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::main(),
            intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::PositionDelete {
                identity_fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("_file", DataType::Utf8, false)),
                    ConnectorWriteFieldRequest::new(Field::new("_pos", DataType::Int64, false)),
                ],
                partition_source_fields: vec![ConnectorWriteFieldRequest::new(Field::new(
                    "id",
                    DataType::Int32,
                    false,
                ))],
            },
            context: crate::connector::test_request_context(),
        };

        let frozen_metadata = super::super::test_metadata::metadata_with_two_snapshots();
        let shape = bind_iceberg_write_input(&request, &owner, &frozen_metadata)
            .expect("bind row-write input");
        let ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        } = shape
        else {
            panic!("expected position-delete input shape");
        };
        assert_eq!(identity_fields.len(), 2);
        assert_eq!(partition_source_fields.len(), 1);
        assert_ne!(
            identity_fields[0].token(),
            partition_source_fields[0].token()
        );
    }

    #[test]
    fn spi5c1_row_write_preparation_freezes_the_named_branch_snapshot() {
        use novarocks_connector_iceberg::iceberg::spec::{SnapshotReference, SnapshotRetention};

        let lease = fixture_planning_lease("iceberg");
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg").expect("fixture instance ID"),
            incarnation: lease.binding().incarnation(),
        };
        let admitted = lease
            .binding()
            .metadata()
            .load_table(ConnectorTableRequest {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: owner.instance_id.clone(),
                    namespace: Arc::from("db"),
                    table: Arc::from("orders"),
                },
                resolution: ConnectorTableResolution::StrictBaseTable,
                context: crate::connector::test_request_context(),
            })
            .expect("fixture table admission");
        let metadata = super::super::test_metadata::metadata_with_two_snapshots()
            .into_builder(None)
            .set_ref(
                "dev",
                SnapshotReference::new(1, SnapshotRetention::branch(None, None, None)),
            )
            .expect("set dev branch")
            .build()
            .expect("build metadata with dev branch")
            .metadata;
        let mut payload: TablePayload = decode_payload(
            admitted.table.payload(),
            "fixture admitted Iceberg write table",
        )
        .expect("decode fixture table payload");
        let table_info = payload.table_info.as_mut().expect("fixture table info");
        table_info.current_snapshot_id = metadata.current_snapshot_id();
        table_info.serialized_metadata =
            Some(serde_json::to_string(&metadata).expect("serialize branch metadata"));
        let table = ConnectorTableHandle::try_new(
            owner.instance_id.clone(),
            encode_payload(&payload, "fixture branch table", 1024 * 1024)
                .expect("encode branch table payload"),
        )
        .expect("branch table handle");
        let request = ConnectorWritePreparationRequest {
            table,
            target_ref: novarocks_spi::connector::ConnectorWriteTargetRef::parse("dev")
                .expect("dev target ref"),
            intent: novarocks_spi::connector::ConnectorWriteIntent::RowDelta,
            purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
            input: ConnectorWriteInputRequest::DeletionVector {
                identity_fields: vec![
                    ConnectorWriteFieldRequest::new(Field::new("_file", DataType::Utf8, false)),
                    ConnectorWriteFieldRequest::new(Field::new("_pos", DataType::Int64, false)),
                ],
                partition_source_fields: Vec::new(),
            },
            context: crate::connector::test_request_context(),
        };
        let ConnectorWritePreparationOutcome::Prepared(preparation) =
            prepare_iceberg_write(request, &owner).expect("prepare branch write")
        else {
            panic!("branch write admission must be prepared");
        };
        assert_eq!(preparation.target_ref().as_str(), "dev");

        let warehouse = tempfile::TempDir::new().expect("warehouse tempdir");
        let entry = super::super::catalog::registry::build_catalog_entry(
            "iceberg",
            &[
                ("type".to_string(), "iceberg".to_string()),
                (
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.path().display().to_string(),
                ),
            ],
        )
        .expect("fixture catalog entry");
        let (sink, _) =
            iceberg_row_write_sink_spec_from_preparation(&preparation, &entry, &metadata)
                .expect("derive branch DV sink");
        assert_eq!(sink.iceberg.current_snapshot_id, Some(1));

        let drifted = metadata
            .clone()
            .into_builder(None)
            .set_ref(
                "dev",
                SnapshotReference::new(2, SnapshotRetention::branch(None, None, None)),
            )
            .expect("move dev branch")
            .build()
            .expect("build drifted branch metadata")
            .metadata;
        let error = iceberg_row_write_sink_spec_from_preparation(&preparation, &entry, &drifted)
            .expect_err("branch drift must fail before writer activation");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn mv_write_admission_signs_exact_provider_binary_and_variant_layout() {
        use std::collections::HashMap;

        use novarocks_connector_iceberg::iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema as IcebergSchema,
            SortOrder, TableMetadataBuilder, Type,
        };

        let metadata = TableMetadataBuilder::new(
            IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(1, "state", Type::Primitive(PrimitiveType::Binary))
                        .into(),
                    NestedField::optional(2, "payload", Type::Primitive(PrimitiveType::Variant))
                        .into(),
                ])
                .build()
                .expect("provider schema"),
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///novarocks-test/provider-layout".to_string(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .expect("provider metadata builder")
        .build()
        .expect("provider metadata")
        .metadata;
        let fields = exact_iceberg_data_write_fields(
            &metadata,
            &[
                ConnectorWriteFieldRequest::new(Field::new("state", DataType::LargeBinary, false)),
                ConnectorWriteFieldRequest::new(Field::new("payload", DataType::Binary, true)),
            ],
        )
        .expect("provider-signed write layout");

        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].field().name(), "state");
        assert_eq!(fields[0].field().data_type(), &DataType::Binary);
        assert!(!fields[0].field().is_nullable());
        assert_eq!(fields[1].field().name(), "payload");
        assert_eq!(fields[1].field().data_type(), &DataType::LargeBinary);
        assert!(fields[1].field().is_nullable());
    }

    #[test]
    fn spi5b_unknown_view_catalog_error_is_not_found() {
        let error = map_iceberg_error("unknown view: analytics.v_orders".to_string());
        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    }

    #[test]
    fn spi5b_frozen_read_handle_keeps_an_explicit_empty_or_subset_file_set() {
        let lease = fixture_planning_lease("test_catalog");
        let owner = lease.binding().descriptor().instance_id.clone();
        let table = IcebergTableInfo {
            catalog: "test_catalog".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("uuid".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: "file:///tmp/orders".to_string(),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let read_table = ConnectorTableHandle::try_new(
            owner,
            encode_payload(
                &TablePayload {
                    namespace: "db".to_string(),
                    table: "orders".to_string(),
                    table_info: Some(table.clone()),
                    metadata_columns: Vec::new(),
                    metadata_table_type: None,
                    prepared_files: Vec::new(),
                    explicit_files: None,
                    logical_type_columns: BTreeMap::new(),
                    hidden_columns: Vec::new(),
                    frozen_rewrite: None,
                },
                "test table",
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )
            .expect("encode table"),
        )
        .expect("table handle");
        let materialization = IcebergQueryTableMaterialization {
            table_name: "orders".to_string(),
            schema_id: Some(1),
            columns: Vec::new(),
            dml_target_columns: Vec::new(),
            partition_source_columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            read_table,
            read_schema: Arc::new(Schema::empty()),
            read_selector: ConnectorReadSelector::Current,
            sql_ukfk_facts: crate::sql::planner::table::SqlUkFkTableFacts::default(),
            write_target_admission: None,
            table,
            files: Vec::new(),
            binding: IcebergDataFileBinding::CurrentSnapshot,
            statistics_pin: None,
            planning_lease: lease,
        };
        let frozen = materialization
            .with_frozen_files_at_snapshot(Vec::new(), 7)
            .expect("freeze empty target read");
        assert_eq!(frozen.read_selector, ConnectorReadSelector::SnapshotId(7));
        let payload: TablePayload = decode_payload(frozen.read_table.payload(), "frozen table")
            .expect("decode frozen provider handle");
        assert!(payload.explicit_files.is_some());
        assert_eq!(payload.explicit_files.as_ref().map(Vec::len), Some(0));
        assert!(payload.prepared_files.is_empty());

        let (full, affected) =
            freeze_mv_target_reads(&materialization, ConnectorReadSelector::SnapshotId(7))
                .expect("freeze exact MV target read lanes");
        assert_eq!(full.read_selector, ConnectorReadSelector::SnapshotId(7));
        assert_eq!(affected.read_selector, full.read_selector);
        assert_eq!(affected.read_table, full.read_table);
        assert_eq!(
            affected.planning_lease.binding().incarnation(),
            full.planning_lease.binding().incarnation()
        );
        let affected_payload: TablePayload =
            decode_payload(affected.read_table.payload(), "affected full-table lane")
                .expect("decode affected target read");
        assert!(affected_payload.explicit_files.is_none());
    }

    #[test]
    fn spi5b_row_lineage_read_fields_are_hidden_from_sql_targets() {
        let fields = iceberg_metadata_arrow_fields(&[
            "_file".to_string(),
            "_pos".to_string(),
            "_row_id".to_string(),
        ])
        .expect("known row-lineage fields");
        assert!(fields.iter().all(|field| {
            field
                .metadata()
                .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_some_and(|value| value == "true")
        }));
    }

    #[test]
    fn spi5b_projected_schema_retains_hidden_sql_field_metadata() {
        let source_fields = vec![
            Arc::new(Field::new("id", DataType::Int64, false)),
            Arc::new(
                Field::new("_file", DataType::Utf8, false).with_metadata(HashMap::from([(
                    novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL.to_string(),
                    "true".to_string(),
                )])),
            ),
        ];
        let projected = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_file", DataType::Utf8, false),
        ]));

        let schema = preserve_hidden_sql_field_metadata(projected, &source_fields)
            .expect("projected schema preserves connector visibility metadata");

        assert!(
            schema
                .field_with_name("id")
                .expect("id field")
                .metadata()
                .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .is_none()
        );
        assert_eq!(
            schema
                .field_with_name("_file")
                .expect("file field")
                .metadata()
                .get(novarocks_spi::connector::CONNECTOR_FIELD_HIDDEN_FROM_SQL)
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn variant_partition_validation_is_known_uncommitted() {
        let error = map_iceberg_error(
            "iceberg table column `v` is variant; variant columns cannot appear in the partition spec"
                .to_string(),
        );
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn local_schema_and_property_validation_is_known_uncommitted() {
        for (message, expected_kind) in [
            (
                "ADD COLUMN parent path must point to a STRUCT",
                ConnectorErrorKind::InvalidRequest,
            ),
            (
                "decimal precision must strictly increase",
                ConnectorErrorKind::InvalidRequest,
            ),
            (
                "Iceberg column `v2` already exists",
                ConnectorErrorKind::InvalidRequest,
            ),
            (
                "column path 'c1.v2.v5' not found",
                ConnectorErrorKind::NotFound,
            ),
            (
                "identifier-field-ids is an Iceberg internal metadata key",
                ConnectorErrorKind::InvalidRequest,
            ),
            (
                "novarocks.* namespace is reserved for engine-owned properties",
                ConnectorErrorKind::InvalidRequest,
            ),
        ] {
            let error = map_iceberg_error(message.to_string());
            assert_eq!(error.kind(), expected_kind, "{message}");
        }
    }

    struct NotCancelled;

    impl novarocks_spi::connector::ConnectorCancellation for NotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context_with_payload_budgets(
        max_handle_payload_bytes: usize,
        max_total_payload_bytes: usize,
    ) -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            Instant::now() + std::time::Duration::from_secs(30),
            Arc::new(NotCancelled),
            max_handle_payload_bytes,
            max_total_payload_bytes,
        )
        .expect("connector request context")
    }

    #[test]
    fn spi5b_reference_facts_preserve_snapshots_history_and_ref_kinds() {
        use novarocks_connector_iceberg::iceberg::spec::{SnapshotReference, SnapshotRetention};

        let metadata = super::super::test_metadata::metadata_with_two_snapshots()
            .into_builder(None)
            .set_ref(
                "release-1",
                SnapshotReference::new(
                    1,
                    SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                ),
            )
            .expect("set tag ref")
            .build()
            .expect("build tagged metadata")
            .metadata;
        let facts =
            iceberg_read_reference_facts(&metadata, &context_with_payload_budgets(1024, 4096))
                .expect("reference facts");

        assert_eq!(facts.snapshot_ids(), &[1, 2]);
        assert_eq!(facts.current_snapshot_id(), Some(2));
        assert_eq!(
            facts
                .snapshot_log()
                .iter()
                .map(|entry| (entry.snapshot_id, entry.timestamp_millis))
                .collect::<Vec<_>>(),
            vec![(1, 1_700_000_000_000), (2, 1_700_000_001_000)]
        );
        assert_eq!(
            facts
                .named_references()
                .iter()
                .map(|reference| (
                    reference.name.as_ref(),
                    reference.kind,
                    reference.snapshot_id
                ))
                .collect::<Vec<_>>(),
            vec![
                ("main", ConnectorReadReferenceKind::Branch, 2),
                ("release-1", ConnectorReadReferenceKind::Tag, 1),
            ]
        );
    }

    #[test]
    fn spi5b_view_metadata_rejects_non_starrocks_dialect() {
        let loaded = views::LoadedIcebergView {
            sql: "SELECT 1".to_string(),
            dialect: "spark".to_string(),
            default_namespace: "analytics".to_string(),
            column_names: vec!["one".to_string()],
            comment: None,
            properties: HashMap::new(),
        };
        let error = iceberg_view_metadata_value(
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            Arc::from("analytics"),
            Arc::from("v_one"),
            loaded,
            &context_with_payload_budgets(1024, 4096),
        )
        .expect_err("unsupported dialect");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn spi5b_view_metadata_preserves_sorted_properties() {
        let loaded = views::LoadedIcebergView {
            sql: "SELECT id FROM orders".to_string(),
            dialect: views::VIEW_DIALECT_STARROCKS.to_string(),
            default_namespace: "analytics".to_string(),
            column_names: vec!["id".to_string()],
            comment: Some("orders view".to_string()),
            properties: HashMap::from([
                ("z-key".to_string(), "z-value".to_string()),
                ("a-key".to_string(), "a-value".to_string()),
            ]),
        };
        let view = iceberg_view_metadata_value(
            ConnectorInstanceId::parse("ice").expect("instance ID"),
            Arc::from("analytics"),
            Arc::from("v_orders"),
            loaded,
            &context_with_payload_budgets(1024, 4096),
        )
        .expect("view metadata");
        assert_eq!(view.definition.dialect, ConnectorViewDialect::StarRocks);
        assert_eq!(
            view.properties
                .iter()
                .map(|(key, value)| (key.as_ref(), value.as_ref()))
                .collect::<Vec<_>>(),
            vec![("a-key", "a-value"), ("z-key", "z-value")]
        );
    }

    fn write_single_row_group_parquet(path: &Path, row_count: usize) -> i64 {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(
                0..i32::try_from(row_count).expect("row count fits i32"),
            ))],
        )
        .expect("single-row-group batch");
        let mut writer = ArrowWriter::try_new(
            File::create(path).expect("create single-row-group Parquet"),
            schema,
            None,
        )
        .expect("single-row-group writer");
        writer.write(&batch).expect("write single row group");
        writer.close().expect("close single-row-group Parquet");
        i64::try_from(std::fs::metadata(path).expect("Parquet metadata").len())
            .expect("Parquet size fits i64")
    }

    fn write_two_row_group_parquet(path: &Path) -> i64 {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let first = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("first row-group batch");
        let second = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![4, 5, 6, 7, 8]))],
        )
        .expect("second row-group batch");
        let mut writer = ArrowWriter::try_new(
            File::create(path).expect("create two-row-group Parquet"),
            schema,
            None,
        )
        .expect("two-row-group writer");
        writer.write(&first).expect("write first row group");
        writer.flush().expect("flush first row group");
        writer.write(&second).expect("write second row group");
        writer.close().expect("close two-row-group Parquet");
        i64::try_from(std::fs::metadata(path).expect("Parquet metadata").len())
            .expect("Parquet size fits i64")
    }

    fn local_planning_provider(
        warehouse: &Path,
    ) -> (tokio::runtime::Runtime, IcebergControlProvider) {
        let runtime = tokio::runtime::Runtime::new().expect("build explicit test runtime");
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let mut catalog_registry = IcebergCatalogRegistry::default();
        catalog_registry
            .create_catalog(
                instance_id.as_str(),
                &[(
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.to_string_lossy().into_owned(),
                )],
            )
            .expect("local catalog registration");
        let incarnation = ConnectorInstanceIncarnation::from_bytes([9; 16]);
        let provider = IcebergControlProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(PROVIDER_ID).expect("provider ID"),
                instance_id: instance_id.clone(),
            },
            binding_key: ConnectorExecutionBindingKey {
                instance_id: instance_id.clone(),
                incarnation,
            },
            instance_id,
            incarnation,
            registry: Arc::new(RwLock::new(catalog_registry)),
            planning_binding: Some(explicit_test_read_binding(&runtime)),
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
            recovery_cleanup_outcomes: Arc::new(Mutex::new(HashMap::new())),
        };
        (runtime, provider)
    }

    fn explicit_file_scan_handle(
        instance_id: ConnectorInstanceId,
        files: Vec<IcebergDataFileInfo>,
    ) -> ConnectorScanHandle {
        ConnectorScanHandle::try_new(
            instance_id,
            encode_payload(
                &ScanPayload {
                    table: TablePayload {
                        namespace: "db".to_string(),
                        table: "orders".to_string(),
                        table_info: None,
                        metadata_columns: Vec::new(),
                        metadata_table_type: None,
                        prepared_files: Vec::new(),
                        explicit_files: Some(files),
                        logical_type_columns: BTreeMap::new(),
                        hidden_columns: Vec::new(),
                        frozen_rewrite: None,
                    },
                    snapshot_id: None,
                    table_uuid: None,
                    projection: Vec::new(),
                    limit: None,
                    physical_predicates: Vec::new(),
                    fact_columns: Vec::new(),
                },
                "test scan handle",
                64 * 1024,
            )
            .expect("encode test scan handle"),
        )
        .expect("test scan handle")
    }

    fn local_planning_request() -> ConnectorSplitPlanningRequest {
        ConnectorSplitPlanningRequest {
            target_parallelism: NonZeroUsize::new(2).expect("parallelism"),
            max_split_bytes: None,
            context: context_with_payload_budgets(64 * 1024, 256 * 1024),
        }
    }

    #[test]
    fn composite_file_packing_is_deterministic_and_preserves_exact_costs() {
        let directory = tempfile::tempdir().expect("tempdir");
        let files = [("a.parquet", 4), ("b.parquet", 4), ("c.parquet", 4096)]
            .into_iter()
            .map(|(name, row_count)| {
                let path = directory.path().join(name);
                let size = write_single_row_group_parquet(&path, row_count);
                IcebergDataFileInfo::for_test(
                    path.to_string_lossy().as_ref(),
                    size,
                    row_count as i64,
                )
            })
            .collect::<Vec<_>>();
        assert!(
            files[0].size == files[1].size && files[2].size >= files[0].size * 2,
            "the large third leaf must exceed the derived soft target"
        );
        let expected_total = files.iter().map(|file| file.size as u64).sum::<u64>();
        let (_runtime, provider) = local_planning_provider(directory.path());
        let scan = explicit_file_scan_handle(provider.instance_id.clone(), files.clone());

        let first = ConnectorScanPlanning::plan_splits(&provider, &scan, local_planning_request())
            .expect("first local composite plan");
        let second = ConnectorScanPlanning::plan_splits(&provider, &scan, local_planning_request())
            .expect("second local composite plan");
        assert_eq!(
            first.splits.len(),
            2,
            "two small leaves fit before the large leaf opens its own split"
        );
        assert_eq!(
            first
                .splits
                .iter()
                .map(|split| split.payload())
                .collect::<Vec<_>>(),
            second
                .splits
                .iter()
                .map(|split| split.payload())
                .collect::<Vec<_>>(),
            "identical frozen leaves must produce byte-identical composite carriers"
        );

        let decoded = first
            .splits
            .iter()
            .map(|split| {
                let payload: SplitPayload =
                    decode_payload(split.payload(), "planned composite split").expect("payload");
                let unit_cost = payload
                    .units
                    .iter()
                    .map(|unit| unit.estimated_bytes.expect("known leaf cost"))
                    .sum::<u64>();
                assert_eq!(split.estimated_bytes(), Some(unit_cost));
                payload
            })
            .collect::<Vec<_>>();
        assert_eq!(
            decoded
                .iter()
                .map(|payload| payload.units.len())
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(
            decoded
                .iter()
                .flat_map(|payload| payload.units.iter())
                .map(|unit| unit.data_file.path.as_str())
                .collect::<Vec<_>>(),
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
            "packing must retain the stable leaf order inside and across splits"
        );
        assert_eq!(
            first
                .splits
                .iter()
                .map(|split| split.estimated_bytes().expect("known split cost"))
                .sum::<u64>(),
            expected_total
        );
    }

    #[test]
    fn parquet_row_group_materialization_selects_each_group_and_preserves_file_cost() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("multi.parquet");
        let file_size = write_two_row_group_parquet(&path);
        let request = ConnectorPrepareSplitRequest {
            context: context_with_payload_budgets(64 * 1024, 256 * 1024),
        };
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = explicit_test_read_binding(&runtime);
        let materialized = materialize_local_scan_units(
            &binding,
            vec![IcebergFrozenScanUnitPayload {
                data_file: IcebergDataFileInfo::for_test(
                    path.to_string_lossy().as_ref(),
                    file_size,
                    8,
                ),
                row_groups: None,
                estimated_bytes: Some(file_size as u64),
            }],
            false,
            &request,
        )
        .expect("materialize local row groups");

        assert_eq!(materialized.len(), 2);
        assert_eq!(
            materialized
                .iter()
                .map(|unit| unit.row_groups.as_deref())
                .collect::<Vec<_>>(),
            vec![Some(&[0][..]), Some(&[1][..])]
        );
        assert_eq!(
            materialized
                .iter()
                .map(|unit| unit.estimated_bytes.expect("known row-group cost"))
                .sum::<u64>(),
            file_size as u64,
            "row-group costs must exactly reconstruct the frozen file cost"
        );
    }

    #[test]
    fn parquet_footer_facts_are_sealed_for_exact_and_conservative_units() {
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("facts.parquet");
        let file_size = write_two_row_group_parquet(&path);
        let request = ConnectorPrepareSplitRequest {
            context: context_with_payload_budgets(64 * 1024, 256 * 1024),
        };
        let unit = IcebergFrozenScanUnitPayload {
            data_file: IcebergDataFileInfo::for_test(path.to_string_lossy().as_ref(), file_size, 8),
            row_groups: None,
            estimated_bytes: Some(file_size as u64),
        };
        let columns = vec![IcebergScanFactColumnV1 {
            field_ordinal: 0,
            field_id: 1,
            canonical_name: "id".to_string(),
            scalar_type: IcebergScanFactScalarTypeV1::Int32,
            nullable: false,
        }];
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let binding = explicit_test_read_binding(&runtime);
        let mut inspections = HashMap::new();
        let exact = iceberg_unit_domain_facts(
            &binding,
            &mut inspections,
            &unit,
            &columns,
            false,
            false,
            &request,
        )
        .expect("exact facts");
        let available = exact.available_facts().expect("available exact facts");
        assert_eq!(available.physical_row_count(), 8);
        assert_eq!(available.evidence(), ConnectorScanUnitFactsEvidence::Exact);
        assert!(matches!(
            available.columns(),
            [ConnectorScanUnitColumnFacts::Available {
                domain: ConnectorScanUnitColumnDomain::Range {
                    inclusive_min: ConnectorScalarValue::Int32(1),
                    inclusive_max: ConnectorScalarValue::Int32(8),
                    null_count: 0,
                },
                ..
            }]
        ));
        let conservative = iceberg_unit_domain_facts(
            &binding,
            &mut inspections,
            &unit,
            &columns,
            true,
            false,
            &request,
        )
        .expect("conservative facts");
        assert_eq!(
            conservative
                .available_facts()
                .expect("available facts")
                .evidence(),
            ConnectorScanUnitFactsEvidence::Conservative
        );
        assert_eq!(
            inspections.len(),
            1,
            "one footer snapshot per authorized file"
        );
    }

    #[test]
    fn local_unknown_table_error_is_not_found() {
        let error = map_iceberg_error("unknown table: analytics.orders".to_string());
        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    }

    #[test]
    fn local_schema_and_property_validation_errors_are_invalid_requests() {
        for message in [
            "ADD COLUMN parent path must point to a STRUCT (LIST element is not a STRUCT)",
            "decimal precision must strictly increase (current decimal(20,2), new decimal(20,2))",
            "ALTER TABLE TBLPROPERTIES rejected reserved key(s): `current-schema-id`: Iceberg internal metadata key, not user-settable",
            "ALTER TABLE TBLPROPERTIES rejected reserved key(s): `novarocks.x`: novarocks.* namespace is reserved for engine-owned properties",
        ] {
            assert_eq!(
                map_iceberg_error(message.to_string()).kind(),
                ConnectorErrorKind::InvalidRequest,
                "{message}"
            );
        }
    }

    fn data_file_with_column_null_count(
        column: &str,
        null_count: Option<i64>,
    ) -> novarocks_connector_iceberg::manifest::DataFileWithStats {
        novarocks_connector_iceberg::manifest::DataFileWithStats {
            path: "file:///tmp/table/data.parquet".to_string(),
            size: 12,
            record_count: Some(4),
            column_stats: Some(HashMap::from([(
                column.to_string(),
                novarocks_connector_iceberg::scan_model::IcebergColumnStats {
                    field_id: None,
                    null_count,
                    value_count: Some(4),
                    column_size: Some(32),
                    lower_bound: None,
                    upper_bound: None,
                },
            )])),
            partition_spec_id: None,
            partition_key: None,
            partition_values: None,
            manifest_path: None,
            partition_field_values: Vec::new(),
            first_row_id: None,
            data_sequence_number: None,
            delete_files: Vec::new(),
        }
    }

    #[test]
    fn manifest_null_count_requires_complete_per_file_evidence() {
        let files = vec![
            data_file_with_column_null_count("value", Some(1)),
            data_file_with_column_null_count("VALUE", Some(2)),
        ];
        assert!(matches!(
            manifest_null_count(&files, "value"),
            StatisticsMetricState::Available(StatisticsMetricValue::U64(3))
        ));

        let incomplete = vec![
            data_file_with_column_null_count("value", Some(1)),
            data_file_with_column_null_count("value", None),
        ];
        assert!(matches!(
            manifest_null_count(&incomplete, "value"),
            StatisticsMetricState::Missing(StatisticsMissing {
                kind: StatisticsMissingKind::IncompleteEvidence,
                ..
            })
        ));
    }

    #[test]
    fn manifest_evidence_requires_known_rows_and_no_delete_files() {
        let files = vec![data_file_with_column_null_count("value", Some(0))];
        assert!(manifest_evidence_is_complete(
            &StatValue::known(
                4,
                crate::sql::optimizer::statistics::Confidence::Exact,
                crate::sql::optimizer::stats_input::StatsSource::IcebergManifest,
            ),
            &files
        ));
        assert!(!manifest_evidence_is_complete(
            &StatValue::missing(StatsMissingReason::ManifestMissingRowCount),
            &files
        ));

        let mut files_with_delete = files;
        files_with_delete[0].delete_files.push(
            novarocks_connector_iceberg::scan_model::IcebergDeleteFileInfo {
                path: "file:///tmp/table/delete.parquet".to_string(),
                file_format:
                    novarocks_connector_iceberg::scan_model::IcebergDeleteFileFormat::Parquet,
                file_content:
                    novarocks_connector_iceberg::scan_model::IcebergDeleteFileContent::Position,
                length: Some(4),
                content_offset: None,
                content_size_in_bytes: None,
                sequence_number: None,
                partition_spec_id: None,
                partition_key: None,
                equality_column_names: Vec::new(),
                equality_field_ids: Vec::new(),
            },
        );
        assert!(!manifest_evidence_is_complete(
            &StatValue::known(
                4,
                crate::sql::optimizer::statistics::Confidence::Exact,
                crate::sql::optimizer::stats_input::StatsSource::IcebergManifest,
            ),
            &files_with_delete
        ));
    }

    fn mutation_provider_without_catalog() -> IcebergControlProvider {
        let instance_id = ConnectorInstanceId::parse("ice.test").expect("instance ID");
        let incarnation = ConnectorInstanceIncarnation::from_bytes([7; 16]);
        IcebergControlProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(PROVIDER_ID).expect("provider ID"),
                instance_id: instance_id.clone(),
            },
            binding_key: ConnectorExecutionBindingKey {
                instance_id: instance_id.clone(),
                incarnation,
            },
            instance_id,
            incarnation,
            registry: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            planning_binding: None,
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
            recovery_cleanup_outcomes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[test]
    fn dispatch_failure_is_known_uncommitted_before_catalog_commit() {
        let provider = mutation_provider_without_catalog();
        let operation_id = novarocks_spi::connector::ConnectorMutationOperationId::new();
        let outcome = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id,
                target: ConnectorExecutionBindingKey {
                    instance_id: provider.instance_id.clone(),
                    incarnation: provider.incarnation,
                },
                operation: ConnectorCatalogMutationOperation::CreateNamespace {
                    namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                        instance_id: provider.instance_id.clone(),
                        namespace: Arc::from("db"),
                    },
                    policy: CreatePolicy::FailIfExists,
                },
                context: context_with_payload_budgets(1024, 1024),
            })
            .expect("provider contract");
        assert!(matches!(
            outcome,
            ExternalMutationOutcome::KnownUncommitted { .. }
        ));
        let evidence = provider
            .mutation_evidence(
                operation_id,
                &ConnectorCatalogMutationOperation::CreateNamespace {
                    namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                        instance_id: provider.instance_id.clone(),
                        namespace: Arc::from("db"),
                    },
                    policy: CreatePolicy::FailIfExists,
                },
            )
            .expect("evidence does not require a catalog access");
        assert_eq!(evidence.schema_version(), ICEBERG_MUTATION_EVIDENCE_VERSION);
        assert_eq!(
            evidence.descriptor(),
            novarocks_spi::connector::ConnectorCatalogMutation::descriptor(&provider)
        );
        assert_eq!(
            evidence.incarnation(),
            novarocks_spi::connector::ConnectorCatalogMutation::incarnation(&provider)
        );
        assert_eq!(evidence.operation_id(), operation_id);
        assert_eq!(evidence.operation_kind(), "create-namespace");
        assert!(format!("{evidence:?}").contains("provider_payload_len"));
        assert!(!format!("{evidence:?}").contains("\"namespace\""));
    }

    #[test]
    fn legacy_guarded_properties_require_exact_committed_partitioning() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let (_runtime, provider) = local_planning_provider(warehouse.path());
        let target = ConnectorExecutionBindingKey {
            instance_id: provider.instance_id.clone(),
            incarnation: provider.incarnation,
        };
        let namespace = novarocks_spi::connector::ConnectorNamespaceIdentity {
            instance_id: provider.instance_id.clone(),
            namespace: Arc::from("guarded"),
        };
        let create_namespace = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: target.clone(),
                operation: ConnectorCatalogMutationOperation::CreateNamespace {
                    namespace,
                    policy: CreatePolicy::FailIfExists,
                },
                context: context_with_payload_budgets(1024, 4096),
            })
            .expect("create namespace");
        assert!(matches!(
            create_namespace,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let table = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id: provider.instance_id.clone(),
            namespace: Arc::from("guarded"),
            table: Arc::from("orders"),
        };
        let create_table = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: target.clone(),
                operation: ConnectorCatalogMutationOperation::CreateTable {
                    table: table.clone(),
                    columns: vec![ConnectorColumnDefinition {
                        name: Arc::from("id"),
                        data_type: ConnectorDataType::BigInt,
                        nullable: false,
                        aggregation: None,
                        default: None,
                    }],
                    key: None,
                    partitioning: vec![ConnectorPartitionTransform::Identity {
                        column: Arc::from("id"),
                    }],
                    properties: Vec::new(),
                    policy: CreatePolicy::FailIfExists,
                },
                context: context_with_payload_budgets(1024, 4096),
            })
            .expect("create table");
        assert!(matches!(
            create_table,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let entry = provider
            .entry(provider.instance_id.as_str())
            .expect("entry");
        let loaded = load_table(&entry, &table.namespace, &table.table).expect("load table");
        let current = super::super::catalog::schema_update::canonical_committed_partitioning(
            loaded.table.metadata(),
        )
        .expect("canonical partitioning");
        let success = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: target.clone(),
                operation: ConnectorCatalogMutationOperation::AlterProperties {
                    table: table.clone(),
                    changes: vec![novarocks_spi::connector::ConnectorPropertyChange::Set {
                        key: Arc::from("novarocks.mv.partition"),
                        value: Arc::from("exact"),
                    }],
                    authority: novarocks_spi::connector::ConnectorPropertyAuthority::EngineOwned,
                    expected_committed_partitioning: Some(current.clone()),
                },
                context: context_with_payload_budgets(1024, 4096),
            })
            .expect("guarded property success");
        assert!(matches!(
            success,
            ExternalMutationOutcome::KnownCommitted { .. }
        ));
        let empty = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target: target.clone(),
                operation: ConnectorCatalogMutationOperation::AlterProperties {
                    table: table.clone(),
                    changes: Vec::new(),
                    authority: novarocks_spi::connector::ConnectorPropertyAuthority::EngineOwned,
                    expected_committed_partitioning: Some(current.clone()),
                },
                context: context_with_payload_budgets(1024, 4096),
            })
            .expect("empty guarded property mutation");
        assert!(matches!(
            empty,
            ExternalMutationOutcome::KnownUncommitted { failure }
                if failure.kind() == ConnectorMutationFailureKind::InvalidRequest
        ));
        let first = current.fields().first().expect("partition field");
        let mut mismatched_fields = current.fields().to_vec();
        mismatched_fields[0] = novarocks_spi::connector::ConnectorCommittedPartitionField::try_new(
            first.partition_field_id(),
            format!("{}_changed", first.partition_field_name()),
            first.source_field_id(),
            first.source_column_name(),
            first.position(),
            first.transform(),
        )
        .expect("mismatched partition field");
        let mismatched = novarocks_spi::connector::ConnectorCommittedPartitioning::try_new(
            current.spec_id(),
            mismatched_fields,
        )
        .expect("mismatched canonical guard");
        let mismatch = provider
            .execute(ConnectorCatalogMutationRequest {
                operation_id: ConnectorMutationOperationId::new(),
                target,
                operation: ConnectorCatalogMutationOperation::AlterProperties {
                    table,
                    changes: vec![novarocks_spi::connector::ConnectorPropertyChange::Set {
                        key: Arc::from("novarocks.mv.partition"),
                        value: Arc::from("stale"),
                    }],
                    authority: novarocks_spi::connector::ConnectorPropertyAuthority::EngineOwned,
                    expected_committed_partitioning: Some(mismatched),
                },
                context: context_with_payload_budgets(1024, 4096),
            })
            .expect("guarded property mismatch");
        assert!(matches!(
            mismatch,
            ExternalMutationOutcome::KnownUncommitted { failure }
                if failure.kind() == ConnectorMutationFailureKind::Conflict
        ));
    }

    #[test]
    fn absent_table_error_is_classified_as_not_found_for_mutation_policies() {
        let error = map_iceberg_error("unknown table: analytics.orders".to_string());
        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
    }

    #[test]
    fn snapshot_membership_cache_is_bounded_and_reloads_evicted_snapshot() {
        fn key(snapshot_id: i64) -> SnapshotMembershipKey {
            SnapshotMembershipKey {
                namespace: "db".to_string(),
                table: "orders".to_string(),
                table_uuid: "table-uuid".to_string(),
                snapshot_id,
            }
        }

        let cache = SnapshotMembershipCache::new(1);
        let loads = std::sync::atomic::AtomicUsize::new(0);
        let membership = || {
            loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Arc::new(HashSet::new()))
        };

        cache
            .get_or_try_init(key(1), membership)
            .expect("load first snapshot");
        cache
            .get_or_try_init(key(1), membership)
            .expect("reuse first snapshot");
        assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 1);

        cache
            .get_or_try_init(key(2), membership)
            .expect("load second snapshot");
        cache
            .get_or_try_init(key(1), membership)
            .expect("reload evicted first snapshot");

        assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(
            cache.state.lock().expect("cache state").entries.len(),
            1,
            "cache must never retain more snapshots than its configured capacity"
        );
    }

    #[test]
    fn icebergs_static_predicates_are_pruning_only_and_resolve_field_ids() {
        let table = TablePayload {
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_info: Some(IcebergTableInfo {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                table_uuid: None,
                current_snapshot_id: Some(7),
                schema_id: 1,
                location: "s3://warehouse/db/orders".to_string(),
                schema: IcebergSchemaDef {
                    fields: vec![IcebergSchemaFieldDef {
                        field_id: 42,
                        name: "renamed_id".to_string(),
                        initial_default: None,
                        write_default: None,
                        initial_default_json: None,
                        write_default_json: None,
                        children: Vec::new(),
                    }],
                },
                serialized_metadata: None,
                serialized_metadata_rows: None,
            }),
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
            frozen_rewrite: None,
        };
        let supported = ConnectorStaticPredicate {
            id: novarocks_spi::connector::ConnectorStaticPredicateId(3),
            column: novarocks_spi::connector::ConnectorStaticPredicateColumn {
                field_ordinal: 0,
                data_type: ConnectorScalarType::Int32,
                nullable: false,
            },
            kind: ConnectorStaticPredicateKind::Comparison {
                op: ConnectorStaticComparisonOp::Ge,
                literal: ConnectorScalarValue::Int32(10),
            },
        };
        let unsupported = ConnectorStaticPredicate {
            id: novarocks_spi::connector::ConnectorStaticPredicateId(4),
            column: supported.column.clone(),
            kind: ConnectorStaticPredicateKind::Comparison {
                op: ConnectorStaticComparisonOp::Ne,
                literal: ConnectorScalarValue::Int32(11),
            },
        };

        let (physical, dispositions) =
            negotiate_static_predicates(&table, &[supported, unsupported]);
        assert_eq!(physical.len(), 1);
        assert_eq!(physical[0].field_id, 42);
        assert_eq!(physical[0].column, "renamed_id");
        assert!(matches!(
            physical[0].domain,
            IcebergPhysicalPredicateDomain::Range {
                op: IcebergPhysicalPredicateOp::Ge,
                value: IcebergPhysicalPredicateValue::Int32(10),
            }
        ));
        assert_eq!(
            dispositions
                .iter()
                .map(|disposition| disposition.kind)
                .collect::<Vec<_>>(),
            vec![
                ConnectorPredicateDispositionKind::PruningOnly,
                ConnectorPredicateDispositionKind::Unsupported,
            ]
        );
    }

    #[test]
    fn split_payload_does_not_repeat_serialized_table_metadata() {
        let table = TablePayload {
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_info: Some(IcebergTableInfo {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                table_uuid: None,
                current_snapshot_id: Some(7),
                schema_id: 1,
                location: "s3://warehouse/db/orders".to_string(),
                schema: IcebergSchemaDef { fields: Vec::new() },
                serialized_metadata: Some("x".repeat(256 * 1024)),
                serialized_metadata_rows: None,
            }),
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
            frozen_rewrite: None,
        };
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V5,
            owner_instance_id: "ice".to_string(),
            incarnation: [0; 16],
            namespace: table.namespace,
            table: table.table,
            snapshot_id: Some(7),
            table_uuid: Some("table-uuid".to_string()),
            schema_id: Some(1),
            units: vec![IcebergFrozenScanUnitPayload {
                data_file: IcebergDataFileInfo::for_test(
                    "s3://warehouse/db/orders/data-1.parquet",
                    1024,
                    10,
                ),
                row_groups: None,
                estimated_bytes: Some(1024),
            }],
            projection: vec![0],
            limit: None,
            physical_predicates: Vec::new(),
            fact_columns: Vec::new(),
            name_mapping: None,
            delta: None,
            distributed_rewrite_position: None,
            metadata: None,
        };

        let encoded = serde_json::to_vec(&payload).expect("encode split payload");
        assert!(
            encoded.len() < 4096,
            "split payload repeated table metadata: {} bytes per split",
            encoded.len()
        );
        assert!(
            encoded.len().saturating_mul(512)
                <= novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            "ordinary 512-split planning exceeds the total payload budget"
        );
    }

    #[test]
    fn split_v5_requires_canonical_name_mapping() {
        let mut payload = SplitPayload {
            version: ICEBERG_SPLIT_V5,
            owner_instance_id: "ice".to_string(),
            incarnation: [0; 16],
            namespace: "db".to_string(),
            table: "orders".to_string(),
            snapshot_id: Some(7),
            table_uuid: Some("table-uuid".to_string()),
            schema_id: Some(1),
            units: vec![IcebergFrozenScanUnitPayload {
                data_file: IcebergDataFileInfo::for_test(
                    "s3://warehouse/db/orders/data-1.parquet",
                    1024,
                    10,
                ),
                row_groups: None,
                estimated_bytes: Some(1024),
            }],
            projection: vec![0],
            limit: None,
            physical_predicates: Vec::new(),
            fact_columns: Vec::new(),
            name_mapping: Some(r#"[{"field-id":1,"names":["legacy_id"]}]"#.to_string()),
            delta: None,
            distributed_rewrite_position: None,
            metadata: None,
        };
        validate_split_payload(&payload).expect("canonical V5 mapping");
        assert!(payload.name_mapping.is_some());

        payload.name_mapping = Some(r#"[{"names":["legacy_id"],"field-id":1}]"#.to_string());
        let error =
            validate_split_payload(&payload).expect_err("non-canonical V5 mapping must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn aggregate_budget_rejects_candidate_before_split_is_pushed() {
        let owner = ConnectorInstanceId::parse("ice").expect("owner");
        let payload = |suffix: &str| SplitPayload {
            version: ICEBERG_SPLIT_V5,
            owner_instance_id: "ice".to_string(),
            incarnation: [0; 16],
            namespace: "db".to_string(),
            table: "orders".to_string(),
            snapshot_id: Some(7),
            table_uuid: Some("table-uuid".to_string()),
            schema_id: Some(1),
            units: vec![IcebergFrozenScanUnitPayload {
                data_file: IcebergDataFileInfo::for_test(
                    &format!("s3://warehouse/db/orders/{}-{suffix}.bin", "x".repeat(512)),
                    1024,
                    10,
                ),
                row_groups: None,
                estimated_bytes: Some(1024),
            }],
            projection: vec![0],
            limit: None,
            physical_predicates: Vec::new(),
            fact_columns: Vec::new(),
            name_mapping: None,
            delta: None,
            distributed_rewrite_position: None,
            metadata: None,
        };
        let first = payload("first");
        let second = payload("second");
        let first_len = serde_json::to_vec(&first).expect("encode first").len();
        let second_len = serde_json::to_vec(&second).expect("encode second").len();
        let context =
            context_with_payload_budgets(first_len.max(second_len), first_len + second_len - 1);
        let mut splits = Vec::new();
        let mut total = 0;

        push_split_with_budget(
            &mut splits,
            &mut total,
            owner.clone(),
            "first".to_string(),
            &first,
            Some(1024),
            &context,
        )
        .expect("first split fits");
        let admitted_total = total;
        let error = push_split_with_budget(
            &mut splits,
            &mut total,
            owner,
            "second".to_string(),
            &second,
            Some(1024),
            &context,
        )
        .expect_err("second split must exceed aggregate budget");

        assert_eq!(
            error.kind(),
            ConnectorErrorKind::ResourceExhausted,
            "unexpected planning error: {error}"
        );
        assert_eq!(splits.len(), 1, "rejected split must not be pushed");
        assert_eq!(total, admitted_total, "rejection must not consume budget");
    }

    #[test]
    fn plan_splits_enforces_aggregate_budget_incrementally() {
        let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
        let files = ["first", "second"]
            .into_iter()
            .map(|suffix| {
                IcebergDataFileInfo::for_test(
                    &format!(
                        "file:///tmp/novarocks-facts/{}-{suffix}.bin",
                        "x".repeat(512)
                    ),
                    1024,
                    10,
                )
            })
            .collect::<Vec<_>>();
        let split_payloads = files
            .iter()
            .cloned()
            .map(|data_file| SplitPayload {
                version: ICEBERG_SPLIT_V5,
                owner_instance_id: instance_id.as_str().to_string(),
                incarnation: [0; 16],
                namespace: "db".to_string(),
                table: "orders".to_string(),
                snapshot_id: None,
                table_uuid: None,
                schema_id: None,
                units: vec![IcebergFrozenScanUnitPayload {
                    data_file,
                    row_groups: None,
                    estimated_bytes: Some(1024),
                }],
                projection: vec![0],
                limit: None,
                physical_predicates: Vec::new(),
                fact_columns: Vec::new(),
                name_mapping: None,
                delta: None,
                distributed_rewrite_position: None,
                metadata: None,
            })
            .collect::<Vec<_>>();
        let lengths = split_payloads
            .iter()
            .map(|payload| serde_json::to_vec(payload).expect("encode split").len())
            .collect::<Vec<_>>();
        let context = context_with_payload_budgets(
            *lengths.iter().max().expect("split length"),
            lengths.iter().sum::<usize>() - 1,
        );
        let scan = ConnectorScanHandle::try_new(
            instance_id.clone(),
            encode_payload(
                &ScanPayload {
                    table: TablePayload {
                        namespace: "db".to_string(),
                        table: "orders".to_string(),
                        table_info: None,
                        metadata_columns: Vec::new(),
                        metadata_table_type: None,
                        prepared_files: Vec::new(),
                        explicit_files: Some(files),
                        logical_type_columns: BTreeMap::new(),
                        hidden_columns: Vec::new(),
                        frozen_rewrite: None,
                    },
                    snapshot_id: None,
                    table_uuid: None,
                    projection: vec![0],
                    limit: None,
                    physical_predicates: Vec::new(),
                    fact_columns: Vec::new(),
                },
                "scan handle",
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )
            .expect("scan payload"),
        )
        .expect("scan handle");
        let incarnation = ConnectorInstanceIncarnation::from_bytes([0; 16]);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let mut catalog_registry = IcebergCatalogRegistry::default();
        catalog_registry
            .create_catalog(
                instance_id.as_str(),
                &[(
                    "iceberg.catalog.warehouse".to_string(),
                    warehouse.path().to_string_lossy().into_owned(),
                )],
            )
            .expect("catalog registration");
        let provider = IcebergControlProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(PROVIDER_ID).expect("provider"),
                instance_id: instance_id.clone(),
            },
            binding_key: ConnectorExecutionBindingKey {
                instance_id: instance_id.clone(),
                incarnation,
            },
            instance_id,
            incarnation,
            registry: Arc::new(RwLock::new(catalog_registry)),
            planning_binding: Some(explicit_test_read_binding(&runtime)),
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
            recovery_cleanup_outcomes: Arc::new(Mutex::new(HashMap::new())),
        };

        let error = ConnectorScanPlanning::plan_splits(
            &provider,
            &scan,
            ConnectorSplitPlanningRequest {
                target_parallelism: std::num::NonZeroUsize::new(1).expect("parallelism"),
                max_split_bytes: None,
                context,
            },
        )
        .expect_err("aggregate split budget must reject the plan");

        assert_eq!(
            error.kind(),
            ConnectorErrorKind::ResourceExhausted,
            "unexpected planning error: {error}"
        );
    }

    fn frozen_target_data_file(path: &str) -> IcebergDataFileInfo {
        IcebergDataFileInfo {
            path: path.to_string(),
            size: 128,
            row_count: Some(10),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: Some("Struct([])".to_string()),
            first_row_id: None,
            data_sequence_number: Some(1),
            ivm_change_op: None,
            included_positions: None,
            delete_files: Vec::new(),
            manifest_path: None,
            partition_values: Vec::new(),
        }
    }

    fn frozen_target_identity_partition_file(path: &str, id: i32) -> IcebergDataFileInfo {
        use novarocks_connector_iceberg::scan_model::{
            IcebergPartitionFieldValue, IcebergPartitionValue,
        };

        let mut file = frozen_target_data_file(path);
        file.partition_key = Some(format!("Struct([{id}])"));
        file.partition_values = vec![IcebergPartitionFieldValue {
            source_column: "id".to_string(),
            field_name: "id".to_string(),
            transform: "identity".to_string(),
            value: Some(IcebergPartitionValue::Int32(id)),
        }];
        file
    }

    /// Relocated from `scan_preparation/tests/iceberg.rs`, which drove
    /// [`filter_frozen_mv_target_state_files`] directly without ever entering
    /// scan preparation. The filter is Iceberg-typed production code, so no
    /// neutral fact can express these cases; they belong beside it. These two
    /// remain its only direct coverage.
    #[test]
    fn frozen_mv_target_state_reads_only_allow_list_partitions() {
        use std::collections::BTreeSet;

        use crate::mv::model::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};
        use crate::mv::persistence::schema::{
            MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract,
        };

        let mut selected = frozen_target_identity_partition_file("s3://bucket/selected.parquet", 7);
        selected.partition_spec_id = Some(3);
        let mut skipped = frozen_target_identity_partition_file("s3://bucket/skipped.parquet", 9);
        skipped.partition_spec_id = Some(3);
        let allow_key = MvPartitionKey::new(
            3,
            vec![MvPartitionKeyField::new(
                "id".to_string(),
                MvPartitionValue::String("7".to_string()),
            )],
        );
        let contract = MvPartitionContract {
            target_spec_id: 3,
            fields: vec![MvPartitionFieldContract {
                partition_field_id: 100,
                partition_field_name: "id".to_string(),
                source_target_field_id: 1,
                source_column_name: "id".to_string(),
                transform: MvPartitionTransformContract::Identity,
            }],
        };

        let files = filter_frozen_mv_target_state_files(
            vec![selected, skipped],
            &crate::mv::model::TargetPartitionFilter::AllowList(BTreeSet::from([allow_key])),
            Some(&contract),
            42,
        )
        .expect("frozen target-state files should be deterministically pruned");

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "s3://bucket/selected.parquet");
    }

    #[test]
    fn frozen_mv_target_state_with_empty_allow_list_reads_nothing() {
        use std::collections::BTreeSet;

        let files = filter_frozen_mv_target_state_files(
            vec![frozen_target_data_file("s3://bucket/target.parquet")],
            &crate::mv::model::TargetPartitionFilter::AllowList(BTreeSet::new()),
            None,
            43,
        )
        .expect("an empty admitted allow-list is a zero-file scan");

        assert!(files.is_empty());
    }
}

#[cfg(test)]
pub(crate) fn register_planned_files_fixture(
    registry: &crate::connector::ConnectorRegistry,
    catalog: &str,
    files: Vec<IcebergDataFileInfo>,
    seen_projections: Option<Arc<std::sync::Mutex<Vec<Vec<usize>>>>>,
) {
    register_planned_table_files_fixture(
        registry,
        catalog,
        std::collections::HashMap::from([("*".to_string(), files)]),
        seen_projections,
    );
}

#[cfg(test)]
pub(crate) fn register_planned_table_files_fixture(
    registry: &crate::connector::ConnectorRegistry,
    catalog: &str,
    files_by_table: std::collections::HashMap<String, Vec<IcebergDataFileInfo>>,
    seen_projections: Option<Arc<std::sync::Mutex<Vec<Vec<usize>>>>>,
) {
    registry.register_fixture_control(planned_table_files_fixture_binding(
        catalog,
        files_by_table,
        seen_projections,
    ));
}

/// A real control lease for protocol encoders that do not invoke planning but
/// must still model the mandatory execution-lifetime generation fence.
#[cfg(test)]
pub(crate) fn fixture_planning_lease(
    catalog: &str,
) -> novarocks_spi::connector::ConnectorControlPlanningLease {
    novarocks_spi::connector::ConnectorControlPlanningLease::new(
        Arc::new(planned_table_files_fixture_binding(
            catalog,
            std::collections::HashMap::new(),
            None,
        )),
        || {},
    )
}

#[cfg(test)]
pub(crate) fn fixture_change_window_scan(
    catalog: &str,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> novarocks_spi::connector::ConnectorScan {
    fixture_change_window_scan_for_table(catalog, "orders", from_snapshot_id, to_snapshot_id)
}

#[cfg(test)]
pub(crate) fn fixture_change_window_scan_for_table(
    catalog: &str,
    table: &str,
    from_snapshot_id: i64,
    to_snapshot_id: i64,
) -> novarocks_spi::connector::ConnectorScan {
    let lease = fixture_planning_lease(catalog);
    let instance_id = ConnectorInstanceId::parse(catalog).expect("fixture connector instance ID");
    let context = crate::connector::test_request_context();
    let metadata = lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })
        .expect("fixture table metadata");
    let projection = (0..metadata.schema.fields().len()).collect();
    lease
        .binding()
        .planning()
        .begin_scan(
            &metadata.table,
            ConnectorBeginScanRequest {
                projection,
                static_predicates: Vec::new(),
                selection: ConnectorScanSelection::ChangeWindow(
                    novarocks_spi::connector::ConnectorChangeWindow::new(
                        from_snapshot_id,
                        to_snapshot_id,
                    ),
                ),
                purpose: ConnectorReadPurpose::Query,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: std::num::NonZeroUsize::new(4096).expect("nonzero rows"),
                    max_bytes: std::num::NonZeroUsize::new(context.max_handle_payload_bytes())
                        .expect("nonzero bytes"),
                },
                context,
            },
        )
        .expect("fixture change-window scan")
}

/// A provider-produced neutral admission fixture for Core protocol tests.
/// Tests may choose a synthetic scan shape, but they must not manufacture an
/// Iceberg data-file carrier or a provider table handle in Core.
#[cfg(test)]
pub(crate) fn fixture_query_scan_materialization(
    catalog: &str,
) -> crate::engine::query_planning::bindings::QueryScanMaterialization {
    let planning_lease = fixture_planning_lease(catalog);
    let instance_id = ConnectorInstanceId::parse(catalog).expect("fixture connector instance ID");
    let metadata = planning_lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: crate::connector::test_request_context(),
        })
        .expect("fixture connector read admission");
    crate::engine::query_planning::bindings::QueryScanMaterialization {
        table: metadata.table,
        schema: metadata.schema,
        selector: ConnectorReadSelector::Current,
        statistics_pin: None,
        planning_lease,
    }
}

#[cfg(test)]
pub(crate) fn register_planned_table_files_control_fixture(
    controls: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    catalog: &str,
    files_by_table: std::collections::HashMap<String, Vec<IcebergDataFileInfo>>,
) -> Result<(), ConnectorError> {
    let binding = planned_table_files_fixture_binding(catalog, files_by_table, None);
    let instance_id = binding.descriptor().instance_id.clone();
    match controls.retire_current(&instance_id) {
        Ok(()) => {}
        Err(error) if error.kind() == ConnectorErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    controls.register(binding)
}

#[cfg(test)]
fn planned_table_files_fixture_binding(
    catalog: &str,
    files_by_table: std::collections::HashMap<String, Vec<IcebergDataFileInfo>>,
    seen_projections: Option<Arc<std::sync::Mutex<Vec<Vec<usize>>>>>,
) -> ConnectorControlBinding {
    fn fixture_read_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("v", DataType::LargeBinary, false),
            Field::new("agg", DataType::Binary, true),
            Field::new("extra", DataType::Utf8, true),
            Field::new("__nova_join_row_key", DataType::Utf8, false),
            Field::new("_file", DataType::Utf8, false),
            Field::new("_pos", DataType::Int64, false),
            Field::new("_row_id", DataType::Int64, false),
            Field::new("_last_updated_sequence_number", DataType::Int64, true),
        ]))
    }

    fn fixture_read_schema_for_table(table: &str) -> SchemaRef {
        if table == "mv_branch_target" {
            return Arc::new(Schema::new(vec![
                Field::new("__branch_id__", DataType::Int32, false),
                Field::new("__nova_join_row_key", DataType::Utf8, false),
                Field::new("__nova_base_row_id", DataType::Int64, false),
                Field::new("_file", DataType::Utf8, false),
                Field::new("_pos", DataType::Int64, false),
                Field::new("_row_id", DataType::Int64, false),
                Field::new("_last_updated_sequence_number", DataType::Int64, true),
            ]));
        }
        if matches!(table, "l" | "r" | "mv") {
            return Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Int64, true),
                Field::new("__nova_join_row_key", DataType::Utf8, false),
                Field::new("_file", DataType::Utf8, false),
                Field::new("_pos", DataType::Int64, false),
                Field::new("_row_id", DataType::Int64, false),
                Field::new("_last_updated_sequence_number", DataType::Int64, true),
            ]));
        }
        fixture_read_schema()
    }

    fn fixture_table_info(catalog: &str, namespace: &str, table: &str) -> IcebergTableInfo {
        let fields = fixture_read_schema_for_table(table)
            .fields()
            .iter()
            .enumerate()
            .map(|(ordinal, field)| IcebergSchemaFieldDef {
                field_id: i32::try_from(ordinal + 1).expect("fixture schema field ID"),
                name: field.name().to_string(),
                initial_default: None,
                write_default: None,
                initial_default_json: None,
                write_default_json: None,
                children: Vec::new(),
            })
            .collect();
        IcebergTableInfo {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("fixture-{table}")),
            current_snapshot_id: Some(1),
            schema_id: 1,
            location: format!("s3://fixture/{namespace}/{table}"),
            schema: IcebergSchemaDef { fields },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    struct Fixture {
        instance_id: ConnectorInstanceId,
        incarnation: ConnectorInstanceIncarnation,
        files_by_table: std::collections::HashMap<String, Vec<IcebergDataFileInfo>>,
        seen_projections: Option<Arc<std::sync::Mutex<Vec<Vec<usize>>>>>,
    }

    impl ConnectorScanPlanning for Fixture {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn begin_scan(
            &self,
            table: &ConnectorTableHandle,
            request: ConnectorBeginScanRequest,
        ) -> Result<ConnectorScan, ConnectorError> {
            if request.context.cancellation().is_cancelled() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Cancelled,
                    "planned-files fixture observed caller cancellation",
                ));
            }
            let table: TablePayload = decode_payload(table.payload(), "fixture table handle")?;
            let schema = fixture_read_schema_for_table(&table.table);
            if let Some(target_kind) = match request.purpose {
                ConnectorReadPurpose::MvTargetState => Some("target-state"),
                ConnectorReadPurpose::MvTargetLocator => Some("target-locator"),
                ConnectorReadPurpose::Query => None,
            } {
                let files = table
                    .explicit_files
                    .as_deref()
                    .unwrap_or(&table.prepared_files);
                if files.iter().any(|file| {
                    file.delete_files
                        .iter()
                        .any(|delete| delete.file_content == IcebergDeleteFileContent::Equality)
                }) {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg {target_kind} scan does not support equality deletes yet"),
                    ));
                }
            }
            if let Some(seen) = &self.seen_projections {
                seen.lock()
                    .expect("fixture projection lock")
                    .push(request.projection.clone());
            }
            let (physical_predicates, predicate_dispositions) =
                negotiate_static_predicates(&table, &request.static_predicates);
            let projection = if request.projection.is_empty() {
                (0..schema.fields().len()).collect::<Vec<_>>()
            } else {
                request.projection.clone()
            };
            let fields = projection
                .iter()
                .map(|ordinal| {
                    schema.fields().get(*ordinal).cloned().ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            format!("fixture projection index {ordinal} is outside its schema"),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let owner = ConnectorExecutionBindingKey {
                instance_id: self.instance_id.clone(),
                incarnation: self.incarnation,
            };
            let scan_handle = ConnectorScanHandle::try_new(
                self.instance_id.clone(),
                encode_payload(
                    &ScanPayload {
                        table,
                        snapshot_id: None,
                        table_uuid: None,
                        projection: request.projection,
                        limit: request.limit,
                        physical_predicates,
                        fact_columns: Vec::new(),
                    },
                    "fixture scan handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?;
            let output_schema = Arc::new(Schema::new(fields));
            match request.selection {
                ConnectorScanSelection::Snapshot(selector) => ConnectorScan::try_new_snapshot(
                    owner,
                    selector,
                    scan_handle,
                    output_schema,
                    predicate_dispositions,
                ),
                ConnectorScanSelection::ChangeWindow(window) => {
                    ConnectorScan::try_new_change_window(
                        owner,
                        window,
                        novarocks_spi::connector::ConnectorChangeWindowAdmission::MetadataOnly,
                        scan_handle,
                        output_schema,
                        predicate_dispositions,
                        &request.context,
                    )
                }
            }
        }

        fn plan_splits(
            &self,
            scan: &ConnectorScanHandle,
            request: ConnectorSplitPlanningRequest,
        ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
            let scan: ScanPayload = decode_payload(scan.payload(), "fixture scan handle")?;
            let files = self
                .files_by_table
                .get(&scan.table.table)
                .or_else(|| self.files_by_table.get("*"))
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        format!("no planned files for fixture table {}", scan.table.table),
                    )
                })?;
            super::planning::validate_planned_files(scan.table.table_info.as_ref(), files)?;
            let candidate_units_considered = files.len() as u64;
            let mut pruning_counters = super::file_pruning::IcebergFilePruningCounters::default();
            let files = files
                .iter()
                .filter(|file| {
                    super::file_pruning::file_may_satisfy_physical_predicates(
                        file,
                        &scan.physical_predicates,
                        &mut pruning_counters,
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            let splits = files
                .into_iter()
                .enumerate()
                .map(|(index, data_file)| {
                    let estimated_bytes = u64::try_from(data_file.size).ok();
                    ConnectorSplit::try_new(
                        self.instance_id.clone(),
                        format!("fixture-{index}"),
                        encode_payload(
                            &SplitPayload {
                                version: ICEBERG_SPLIT_V5,
                                owner_instance_id: self.instance_id.as_str().to_string(),
                                incarnation: [0; 16],
                                namespace: scan.table.namespace.clone(),
                                table: scan.table.table.clone(),
                                snapshot_id: None,
                                table_uuid: None,
                                schema_id: None,
                                units: vec![IcebergFrozenScanUnitPayload {
                                    estimated_bytes,
                                    data_file,
                                    row_groups: None,
                                }],
                                projection: scan.projection.clone(),
                                limit: scan.limit,
                                physical_predicates: scan.physical_predicates.clone(),
                                fact_columns: scan.fact_columns.clone(),
                                name_mapping: None,
                                delta: None,
                                distributed_rewrite_position: None,
                                metadata: None,
                            },
                            "fixture split",
                            request.context.max_handle_payload_bytes(),
                        )?,
                        estimated_bytes,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let composite_splits_planned = splits.len() as u64;
            ConnectorSplitPlanningResult::try_new(
                splits,
                ConnectorSplitPlanningMetrics {
                    candidate_units_considered,
                    candidate_units_pruned: u64::try_from(pruning_counters.files_pruned)
                        .unwrap_or(u64::MAX)
                        .min(candidate_units_considered),
                    composite_splits_planned,
                    scan_units_planned: candidate_units_considered.saturating_sub(
                        u64::try_from(pruning_counters.files_pruned).unwrap_or(u64::MAX),
                    ),
                },
            )
        }
    }

    impl ConnectorMetadata for Fixture {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn namespace_exists(&self, _: ConnectorNamespaceRequest) -> Result<bool, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not implement metadata",
            ))
        }

        fn table_exists(&self, _: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not implement metadata",
            ))
        }

        fn list_tables(
            &self,
            _: ConnectorListTablesRequest,
        ) -> Result<Vec<novarocks_spi::connector::ConnectorTableIdentity>, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not implement metadata",
            ))
        }

        fn load_table(
            &self,
            request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            if request.table.instance_id != self.instance_id {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "planned-files fixture received a table for another connector",
                ));
            }
            let files = self
                .files_by_table
                .get(request.table.table.as_ref())
                .or_else(|| self.files_by_table.get("*"))
                .cloned()
                .unwrap_or_default();
            let payload = TablePayload {
                namespace: request.table.namespace.to_string(),
                table: request.table.table.to_string(),
                table_info: Some(fixture_table_info(
                    self.instance_id.as_str(),
                    request.table.namespace.as_ref(),
                    request.table.table.as_ref(),
                )),
                metadata_columns: Vec::new(),
                metadata_table_type: None,
                prepared_files: files,
                explicit_files: None,
                logical_type_columns: BTreeMap::new(),
                hidden_columns: Vec::new(),
                frozen_rewrite: None,
            };
            Ok(ConnectorTableMetadata {
                identity: request.table.clone(),
                schema: fixture_read_schema_for_table(request.table.table.as_ref()),
                planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts::empty(),
                definition_facts: novarocks_spi::connector::ConnectorTableDefinitionFacts::empty(),
                version: None,
                statistics_data_version: None,
                table: ConnectorTableHandle::try_new(
                    self.instance_id.clone(),
                    encode_payload(
                        &payload,
                        "planned-files fixture table handle",
                        request.context.max_handle_payload_bytes(),
                    )?,
                )?,
            })
        }
    }

    let instance_id = ConnectorInstanceId::parse(catalog).expect("fixture instance ID");
    let incarnation = ConnectorInstanceIncarnation::from_bytes([0; 16]);
    let read = Arc::new(Fixture {
        instance_id: instance_id.clone(),
        incarnation,
        files_by_table,
        seen_projections,
    });
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("fixture provider ID"),
        instance_id,
    };
    ConnectorControlBinding::try_new(
        descriptor.clone(),
        incarnation,
        read.clone(),
        read,
        Arc::new(IcebergInstanceDistribution::new(descriptor, incarnation)),
        None,
    )
    .expect("fixture connector control binding")
}
