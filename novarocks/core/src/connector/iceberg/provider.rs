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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use novarocks_catalog::schema::SqlType;
use novarocks_fs::{
    FileCancellation, FileError, FileErrorKind, FileIdentity, FileIoRuntime, FileReadContext,
    FileTaskSpawner, FsAccessHandle, FsAccessResolver, ParquetMetadataInspection,
    ParquetStatisticsSortOrder, ParquetStatisticsValue, TokioFileIoRuntime, TokioFileTaskSpawner,
    inspect_parquet_metadata,
};
use novarocks_spi::connector::{
    ConnectorBatchReader, ConnectorBeginScanRequest, ConnectorCatalogMutation,
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorColumnAggregation, ConnectorColumnDefinition, ConnectorCommittedVersion,
    ConnectorControlBinding, ConnectorDataType, ConnectorDefaultValue,
    ConnectorDropTableDataDisposition, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBinding, ConnectorExecutionBindingKey, ConnectorExecutionDeclaration,
    ConnectorExecutionDistribution, ConnectorExecutionInstaller, ConnectorInstanceDescriptor,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorListTablesRequest,
    ConnectorMetadata, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorNamespaceRequest, ConnectorOpenReaderRequest,
    ConnectorPartitionTransform, ConnectorPredicateDisposition, ConnectorPredicateDispositionKind,
    ConnectorPrepareSplitRequest, ConnectorPreparedScanUnit, ConnectorPreparedScanUnitDescriptor,
    ConnectorPreparedScanUnitSet, ConnectorProviderId, ConnectorReadExecution,
    ConnectorReadSelector, ConnectorRefAction, ConnectorRefKind, ConnectorRefreshPublicationGuard,
    ConnectorScalarType, ConnectorScalarValue, ConnectorScan, ConnectorScanHandle,
    ConnectorScanPlanning, ConnectorScanUnitColumn, ConnectorScanUnitColumnDomain,
    ConnectorScanUnitColumnFacts, ConnectorScanUnitDomainFacts, ConnectorScanUnitFactsEvidence,
    ConnectorScanUnitFactsMissingReason, ConnectorSplit, ConnectorSplitPlanningMetrics,
    ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult,
    ConnectorStagedPublicationBaseFact, ConnectorStagedPublicationCleanupReceipt,
    ConnectorStagedPublicationCleanupRequest, ConnectorStagedPublicationDescriptor,
    ConnectorStagedPublicationDisposition, ConnectorStagedPublicationObservation,
    ConnectorStagedPublicationProof, ConnectorStagedPublicationRecovery,
    ConnectorStaticComparisonOp, ConnectorStaticPredicate, ConnectorStaticPredicateKind,
    ConnectorStatistics, ConnectorTableHandle, ConnectorTableMetadata, ConnectorTableRequest,
    ConnectorTableResolution, CreateOrReplacePolicy, CreatePolicy, DropPolicy,
    ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
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
    extract_data_files_with_stats_at, list_tables, load_table, namespace_exists,
};
use super::catalog::views;
use super::cleanup_maintenance::IcebergCleanupMaintenanceAdapter;
use super::data_mutation::IcebergDataMutationAdapter;
use super::metadata_maintenance::IcebergMetadataMaintenanceAdapter;
use super::reader::IcebergBatchReader;
use super::scan_model::{
    IcebergDataFileInfo, IcebergDeleteFileContent, IcebergDeleteFileFormat, IcebergDeleteFileInfo,
    IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain, IcebergPhysicalPredicateOp,
    IcebergPhysicalPredicateValue,
};
use super::write_control::IcebergWriteControlAdapter;
use super::write_execution::IcebergDataWriteExecution;
use super::write_service::RegisteredIcebergWriteControlBackend;
use crate::connector::backend::ResolvedTableStatisticsPin;
use crate::sql::optimizer::stats_input::{StatValue, StatsMissingReason};

#[derive(Clone, Deserialize, Serialize)]
struct IcebergDeltaSplitPayload {
    source: super::delta::DeltaSourceFile,
    #[serde(default)]
    delete_side: Option<super::delta::DeltaScanDeleteSide>,
}

const PROVIDER_ID: &str = "iceberg";
const MAX_CACHED_SNAPSHOT_MEMBERSHIPS: usize = 64;
const ICEBERG_DECLARATION_V1: u16 = 1;
const DEFAULT_ACCESS_BINDING: &str = "default";
const ICEBERG_MUTATION_EVIDENCE_VERSION: u16 = 1;
const ICEBERG_STATISTICS_EVIDENCE_VERSION: u16 = 1;
const ICEBERG_PROVIDER_STATISTICS_VERSION: u16 = 1;
const ICEBERG_STATISTICS_OPERATION_KIND: &str = "statistics-publish";
const ICEBERG_STAGED_PUBLICATION_PROOF_VERSION: u16 = 1;
fn statistics_data_version(
    table_uuid: &str,
    snapshot_id: Option<i64>,
) -> Result<StatisticsDataVersion, ConnectorError> {
    StatisticsDataVersion::try_new(Bytes::from(format!(
        "iceberg/v1/{table_uuid}/{}",
        snapshot_id
            .map(|snapshot| snapshot.to_string())
            .unwrap_or_else(|| "empty".to_string())
    )))
}

fn statistics_metric_column(metric: &StatisticsMetric) -> Option<&str> {
    match metric {
        StatisticsMetric::RowCount => None,
        StatisticsMetric::NullCount { column }
        | StatisticsMetric::Minimum { column }
        | StatisticsMetric::Maximum { column }
        | StatisticsMetric::AverageSize { column }
        | StatisticsMetric::ThetaNdv { column } => Some(column),
    }
}

fn encode_provider_statistics(evidence: &StatisticsEvidence) -> Result<Vec<u8>, ConnectorError> {
    if evidence.coverage != StatisticsCoverage::Full
        || evidence.accuracy != StatisticsAccuracy::Exact
        || evidence.provenance != StatisticsProvenance::VisibleRows
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg provider statistics require Full Exact visible-row evidence",
        ));
    }
    let metrics = evidence
        .metrics
        .iter()
        .map(|(metric, state)| {
            let StatisticsMetricState::Available(value) = state else {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg provider statistics cannot persist unavailable metrics",
                ));
            };
            let value = match value {
                StatisticsMetricValue::U64(value) => IcebergStatisticValueV1::U64(*value),
                StatisticsMetricValue::I64(value) => IcebergStatisticValueV1::I64(*value),
                StatisticsMetricValue::F64(value) => IcebergStatisticValueV1::F64(*value),
                StatisticsMetricValue::Bytes(value) => {
                    IcebergStatisticValueV1::Bytes(value.to_vec())
                }
            };
            Ok(match metric {
                StatisticsMetric::RowCount => IcebergProviderStatisticV1::RowCount { value },
                StatisticsMetric::NullCount { column } => IcebergProviderStatisticV1::NullCount {
                    column: column.to_string(),
                    value,
                },
                StatisticsMetric::Minimum { column } => IcebergProviderStatisticV1::Minimum {
                    column: column.to_string(),
                    value,
                },
                StatisticsMetric::Maximum { column } => IcebergProviderStatisticV1::Maximum {
                    column: column.to_string(),
                    value,
                },
                StatisticsMetric::AverageSize { column } => {
                    IcebergProviderStatisticV1::AverageSize {
                        column: column.to_string(),
                        value,
                    }
                }
                StatisticsMetric::ThetaNdv { column } => IcebergProviderStatisticV1::ThetaNdv {
                    column: column.to_string(),
                    value,
                },
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_vec(&IcebergProviderStatisticsV1 {
        version: ICEBERG_PROVIDER_STATISTICS_VERSION,
        data_version: evidence.data_version.as_bytes().to_vec(),
        metrics,
    })
    .map_err(|error| internal(format!("encode Iceberg provider statistics: {error}")))
}

fn decode_provider_statistics(
    payload: &[u8],
    expected_data_version: &StatisticsDataVersion,
    requested: &novarocks_spi::connector::StatisticsMetricRequest,
) -> Result<BTreeMap<StatisticsMetric, StatisticsMetricState>, ConnectorError> {
    let artifact: IcebergProviderStatisticsV1 =
        serde_json::from_slice(payload).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode Iceberg provider statistics: {error}"),
            )
        })?;
    if artifact.version != ICEBERG_PROVIDER_STATISTICS_VERSION
        || artifact.data_version.as_slice() != expected_data_version.as_bytes().as_ref()
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg provider statistics do not match the pinned table version",
        ));
    }
    let mut available = BTreeMap::new();
    for metric in artifact.metrics {
        let (metric, value) = match metric {
            IcebergProviderStatisticV1::RowCount { value } => (StatisticsMetric::RowCount, value),
            IcebergProviderStatisticV1::NullCount { column, value } => (
                StatisticsMetric::NullCount {
                    column: Arc::from(column),
                },
                value,
            ),
            IcebergProviderStatisticV1::Minimum { column, value } => (
                StatisticsMetric::Minimum {
                    column: Arc::from(column),
                },
                value,
            ),
            IcebergProviderStatisticV1::Maximum { column, value } => (
                StatisticsMetric::Maximum {
                    column: Arc::from(column),
                },
                value,
            ),
            IcebergProviderStatisticV1::AverageSize { column, value } => (
                StatisticsMetric::AverageSize {
                    column: Arc::from(column),
                },
                value,
            ),
            IcebergProviderStatisticV1::ThetaNdv { column, value } => (
                StatisticsMetric::ThetaNdv {
                    column: Arc::from(column),
                },
                value,
            ),
        };
        let value = match value {
            IcebergStatisticValueV1::U64(value) => StatisticsMetricValue::U64(value),
            IcebergStatisticValueV1::I64(value) => StatisticsMetricValue::I64(value),
            IcebergStatisticValueV1::F64(value) if value.is_finite() => {
                StatisticsMetricValue::F64(value)
            }
            IcebergStatisticValueV1::F64(_) => {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg provider statistics contain a non-finite value",
                ));
            }
            IcebergStatisticValueV1::Bytes(value) => {
                StatisticsMetricValue::try_bytes(Bytes::from(value))?
            }
        };
        if available
            .insert(metric, StatisticsMetricState::Available(value))
            .is_some()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg provider statistics contain a duplicate metric",
            ));
        }
    }
    Ok(requested
        .metrics()
        .iter()
        .map(|metric| {
            let state = available.get(metric).cloned().unwrap_or_else(|| {
                StatisticsMetricState::Missing(StatisticsMissing {
                    kind: StatisticsMissingKind::NotCollected,
                    message: Arc::from(
                        "metric was not present in the published statistics artifact",
                    ),
                })
            });
            (metric.clone(), state)
        })
        .collect())
}

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
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(serialized).map_err(|e| {
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
    let schema =
        iceberg::arrow::schema_to_arrow_schema(metadata.current_schema()).map_err(|e| {
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

/// Provider-owned, secret-free declaration used to install an Iceberg read
/// instance into a BE.  Catalog clients and credentials deliberately do not
/// cross this boundary: the installer resolves the named binding from process
/// startup composition.
#[derive(Deserialize, Serialize)]
struct IcebergDeclarationV1 {
    version: u16,
    access_binding: String,
}

#[derive(Clone)]
struct IcebergInstanceDistribution {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorExecutionDistribution for IcebergInstanceDistribution {
    fn declaration(
        &self,
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionDeclaration, ConnectorError> {
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
        ConnectorExecutionDeclaration::try_new(
            self.descriptor.clone(),
            self.incarnation,
            encode_payload(
                &IcebergDeclarationV1 {
                    version: ICEBERG_DECLARATION_V1,
                    access_binding: DEFAULT_ACCESS_BINDING.to_string(),
                },
                "Iceberg execution declaration",
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )?,
        )
    }
}

/// Process-startup binding for a read-only Iceberg execution instance.
///
/// This type intentionally owns only a locally-composed access binding.  It
/// has no catalog registry or metadata capability, because BE execution must
/// consume the fully planned provider split rather than reconnecting a
/// catalog.
#[derive(Clone)]
pub(crate) struct IcebergReadBinding {
    access_binding: String,
    object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
    access_resolver: FsAccessResolver,
    file_runtime: Arc<dyn FileIoRuntime>,
    file_task_spawner: Arc<dyn FileTaskSpawner>,
}

impl std::fmt::Debug for IcebergReadBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergReadBinding")
            .field("access_binding", &self.access_binding)
            .field(
                "object_store_config",
                &self.object_store_config.as_ref().map(|_| "<redacted>"),
            )
            .finish_non_exhaustive()
    }
}

impl IcebergReadBinding {
    pub(crate) fn default_binding(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
    ) -> Result<Self, ConnectorError> {
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            static FALLBACK_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
            FALLBACK_RUNTIME
                .get_or_init(|| {
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .expect("Iceberg fallback Tokio runtime must initialize")
                })
                .handle()
                .clone()
        });
        Ok(Self::new(
            object_store_config,
            Arc::new(TokioFileIoRuntime::new(handle.clone())),
            Arc::new(TokioFileTaskSpawner::new(handle)),
        ))
    }

    pub(crate) fn new(
        object_store_config: Option<novarocks_fs::ObjectStoreConfig>,
        file_runtime: Arc<dyn FileIoRuntime>,
        file_task_spawner: Arc<dyn FileTaskSpawner>,
    ) -> Self {
        Self {
            access_binding: DEFAULT_ACCESS_BINDING.to_string(),
            object_store_config,
            access_resolver: FsAccessResolver::new(),
            file_runtime,
            file_task_spawner,
        }
    }

    pub(crate) fn resolve_access(&self, location: &str) -> Result<FsAccessHandle, ConnectorError> {
        self.access_resolver
            .resolve_location(location, self.object_store_config.as_ref())
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
    }

    pub(crate) fn resolve_access_for_locations<I, S>(
        &self,
        locations: I,
    ) -> Result<FsAccessHandle, ConnectorError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.access_resolver
            .resolve_locations(locations, self.object_store_config.as_ref())
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })
    }

    pub(crate) fn file_read_context(
        &self,
        cancellation: novarocks_fs::FileCancellation,
        deadline: std::time::Instant,
    ) -> Result<FileReadContext, ConnectorError> {
        Ok(FileReadContext {
            cancellation,
            deadline: Some(deadline),
            runtime: Arc::clone(&self.file_runtime),
            task_spawner: Arc::clone(&self.file_task_spawner),
        })
    }

    pub(crate) fn file_size(
        &self,
        path: &str,
        access: &FsAccessHandle,
        context: &FileReadContext,
    ) -> Result<u64, ConnectorError> {
        let file = access
            .bind_location(path, FileIdentity::new(path, 0, None))
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let cancellation = context.cancellation.clone();
        context
            .runtime
            .block_on_u64(Box::pin(async move { file.stat(&cancellation).await }))
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::Unavailable, error.to_string())
                    .with_retryable_before_progress()
            })
    }

    /// Recreate provider-private writer storage configuration from the BE
    /// startup binding. Credentials remain local to the exact incarnation.
    pub(crate) fn write_object_store_config(
        &self,
        data_location: &str,
    ) -> Result<Option<super::sink_plan::IcebergSinkObjectStoreConfig>, String> {
        let Some(bucket) =
            super::changes::expected_object_store_bucket_from_location(data_location)?
        else {
            return Ok(None);
        };
        let config = self.object_store_config.as_ref().ok_or_else(|| {
            format!(
                "Iceberg connector writer needs a startup object-store binding for bucket {bucket}"
            )
        })?;
        Ok(Some(super::sink_plan::IcebergSinkObjectStoreConfig {
            endpoint: config.endpoint.clone(),
            bucket,
            access_key_id: config.access_key_id.clone(),
            access_key_secret: config.access_key_secret.clone(),
            session_token: config.session_token.clone(),
            region: config.region.clone(),
            enable_path_style_access: config.enable_path_style_access,
            retry_max_times: config.retry_max_times,
            retry_min_delay_ms: config.retry_min_delay_ms,
            retry_max_delay_ms: config.retry_max_delay_ms,
            timeout_ms: config.timeout_ms,
            io_timeout_ms: config.io_timeout_ms,
        }))
    }
}

/// Startup-composed installer for Iceberg read-only instances.  The payload
/// identifies the binding but cannot override it with cloud properties.
pub(crate) struct IcebergConnectorInstaller {
    provider_id: ConnectorProviderId,
    binding: IcebergReadBinding,
}

impl IcebergConnectorInstaller {
    pub(crate) fn new(binding: IcebergReadBinding) -> Self {
        Self {
            provider_id: ConnectorProviderId::parse(PROVIDER_ID)
                .expect("static Iceberg provider ID is valid"),
            binding,
        }
    }
}

impl ConnectorExecutionInstaller for IcebergConnectorInstaller {
    fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    fn install(
        &self,
        declaration: &ConnectorExecutionDeclaration,
        _context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<ConnectorExecutionBinding, ConnectorError> {
        if declaration.descriptor().provider_id != self.provider_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg installer received a declaration for another provider",
            ));
        }
        let payload: IcebergDeclarationV1 =
            decode_payload(declaration.payload(), "Iceberg execution declaration")?;
        if payload.version != ICEBERG_DECLARATION_V1 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!(
                    "unsupported Iceberg execution declaration version {}",
                    payload.version
                ),
            ));
        }
        if payload.access_binding != self.binding.access_binding {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg declaration access binding does not match BE startup binding",
            ));
        }
        let key = declaration.binding_key();
        ConnectorExecutionBinding::try_new_capabilities(
            self.provider_id.clone(),
            key.clone(),
            Some(Arc::new(IcebergReadOnlyConnectorInstance {
                key: key.clone(),
                binding: self.binding.clone(),
            })),
            Some(Arc::new(IcebergDataWriteExecution::new(
                key,
                self.binding.clone(),
            ))),
        )
    }
}

/// BE-only instance installed through the binding control plane. Metadata and
/// planning are deliberately unsupported: it materializes only the frozen
/// membership carried by one FE split into sealed local units.
struct IcebergReadOnlyConnectorInstance {
    key: ConnectorExecutionBindingKey,
    binding: IcebergReadBinding,
}

impl IcebergReadOnlyConnectorInstance {
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

    fn prepare_split(
        &self,
        split: &ConnectorSplit,
        request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
        request.check_active()?;
        ensure_owner(split.owner(), &self.key.instance_id)?;
        let payload: SplitPayload = decode_payload(split.payload(), "Iceberg split")?;
        validate_split_payload(&payload)?;
        if payload.owner_instance_id != self.key.instance_id.as_str()
            || payload.incarnation != self.key.incarnation.to_bytes()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg split does not belong to this installed instance incarnation",
            ));
        }
        if payload.units.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg split has no frozen scan units",
            ));
        }
        if (payload.delta.is_some() || payload.distributed_rewrite_position.is_some())
            && payload.units.len() != 1
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg special scan split must carry exactly one frozen unit",
            ));
        }
        let special_unit =
            payload.delta.is_some() || payload.distributed_rewrite_position.is_some();
        let fact_columns = payload.fact_columns.clone();
        let facts_are_conservative =
            payload.limit.is_some() || !payload.physical_predicates.is_empty() || special_unit;
        let shared_payload = encode_payload(
            &IcebergPreparedSplitSharedPayload {
                version: ICEBERG_PREPARED_SPLIT_SHARED_V2,
                owner_instance_id: payload.owner_instance_id,
                incarnation: payload.incarnation,
                namespace: payload.namespace,
                table: payload.table,
                snapshot_id: payload.snapshot_id,
                table_uuid: payload.table_uuid,
                schema_id: payload.schema_id,
                projection: payload.projection,
                limit: payload.limit,
                physical_predicates: payload.physical_predicates,
                fact_columns: payload.fact_columns,
                name_mapping: payload.name_mapping,
                delta: payload.delta,
                distributed_rewrite_position: payload.distributed_rewrite_position,
            },
            "prepared split shared payload",
            request.context.max_handle_payload_bytes(),
        )?;
        let units =
            materialize_local_scan_units(&self.binding, payload.units, special_unit, &request)?;
        let unit_count = units.len();
        let leaf_kind = if units.iter().any(|unit| unit.row_groups.is_some()) {
            "row_group"
        } else {
            "file"
        };
        let mut inspections = HashMap::<String, ParquetMetadataInspection>::new();
        let descriptors = units
            .into_iter()
            .map(|unit| {
                request.check_active()?;
                let facts = iceberg_unit_domain_facts(
                    &self.binding,
                    &mut inspections,
                    &unit,
                    &fact_columns,
                    facts_are_conservative
                        || !unit.data_file.delete_files.is_empty()
                        || unit.data_file.included_positions.is_some(),
                    special_unit,
                    &request,
                )?;
                ConnectorPreparedScanUnitDescriptor::try_new(
                    encode_payload(
                        &IcebergPreparedUnitPayload {
                            version: ICEBERG_PREPARED_SCAN_UNIT_V1,
                            data_file: unit.data_file,
                            row_groups: unit.row_groups,
                        },
                        "prepared scan unit payload",
                        request.context.max_handle_payload_bytes(),
                    )?,
                    unit.estimated_bytes,
                    facts,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let prepared = ConnectorPreparedScanUnitSet::try_new_with_preparation_evidence(
            self.key.clone(),
            split,
            shared_payload,
            descriptors,
            Some(leaf_kind),
            &request,
        )?;
        Ok(prepared)
    }

    fn open_unit_reader(
        &self,
        unit: &ConnectorPreparedScanUnit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.validate_context(&request.context)?;
        if unit.binding_key() != &self.key {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg prepared scan unit belongs to another installed instance incarnation",
            ));
        }
        let shared: IcebergPreparedSplitSharedPayload = decode_payload(
            unit.shared_payload(),
            "Iceberg prepared split shared payload",
        )?;
        let prepared: IcebergPreparedUnitPayload =
            decode_payload(unit.payload(), "Iceberg prepared scan unit")?;
        validate_prepared_payload(&shared, &prepared)?;
        if shared.owner_instance_id != self.key.instance_id.as_str()
            || shared.incarnation != self.key.incarnation.to_bytes()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg prepared scan unit does not belong to this installed instance incarnation",
            ));
        }
        if let Some(delta) = shared.delta {
            return super::delta_reader::IcebergDeltaBatchReader::try_new(
                delta.source,
                delta.delete_side,
                self.binding.clone(),
                request,
            )
            .map(|reader| Box::new(reader) as Box<dyn ConnectorBatchReader>);
        }
        if let Some(rewrite_position) = shared.distributed_rewrite_position {
            return super::distributed_rewrite_reader::IcebergRewritePositionBatchReader::try_new(
                prepared.data_file,
                rewrite_position,
                self.binding.clone(),
                request,
            )
            .map(|reader| Box::new(reader) as Box<dyn ConnectorBatchReader>);
        }
        let file_context = self.binding.file_read_context(
            novarocks_fs::FileCancellation::new(),
            request.context.deadline(),
        )?;
        // A split is the authorization unit for its data file and every
        // provider-planned delete side file. Resolve one least-common access
        // root up front so partitioned data and sibling delete staging paths
        // remain inside the same explicit capability.
        let access = self.binding.resolve_access_for_locations(
            std::iter::once(prepared.data_file.path.as_str()).chain(
                prepared
                    .data_file
                    .delete_files
                    .iter()
                    .map(|delete| delete.path.as_str()),
            ),
        )?;
        IcebergBatchReader::try_new_with_name_mapping_and_row_groups(
            &prepared.data_file,
            &shared.physical_predicates,
            shared.name_mapping.as_deref(),
            prepared.row_groups.as_deref(),
            access,
            request,
            file_context,
        )
        .map(|reader| Box::new(reader) as Box<dyn ConnectorBatchReader>)
    }
}

impl ConnectorReadExecution for IcebergReadOnlyConnectorInstance {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn prepare_split(
        &self,
        split: &ConnectorSplit,
        request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
        self.prepare_split(split, request)
    }

    fn open_unit_reader(
        &self,
        unit: &ConnectorPreparedScanUnit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.open_unit_reader(unit, request)
    }
}

#[derive(Clone)]
pub(crate) struct IcebergControlProvider {
    descriptor: ConnectorInstanceDescriptor,
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
    binding_key: ConnectorExecutionBindingKey,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
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

/// Provider-private, bounded description of exactly the ref that an
/// inspection proved safe to clean up. It is only carried back as opaque SPI
/// proof; the frontend never interprets these fields.
#[derive(Clone, Deserialize, Serialize)]
struct IcebergStagedPublicationProofV1 {
    version: u16,
    descriptor_digest: Vec<u8>,
    namespace: String,
    table: String,
    table_uuid: String,
    staging_ref: String,
    staging_snapshot_id: Option<i64>,
    target_ref: String,
    target_snapshot_id: Option<i64>,
    refresh_id: i64,
    mv_id: i64,
    marker_token: String,
}

/// Provider-private, bounded evidence used only to decide an ambiguous
/// external catalog commit. It intentionally contains no catalog handle,
/// credentials, parser AST, or runtime object.
#[derive(Deserialize, Serialize)]
struct IcebergMutationEvidenceV1 {
    version: u16,
    target: IcebergMutationEvidenceTarget,
}

/// Minimal, secret-free proof needed to determine whether an uncertain Puffin
/// publication reached the catalog.  The operation-specific path is both the
/// idempotency key and the orphan-cleanup target; it is never a replacement
/// statistics authority.
#[derive(Deserialize, Serialize)]
struct IcebergStatisticsEvidenceV1 {
    version: u16,
    namespace: String,
    table: String,
    data_version: Vec<u8>,
    statistics_path: String,
}

/// Exact scalar evidence collected from visible rows and stored alongside the
/// standard Theta blobs.  This is provider-private persisted data: Core sees
/// only the typed `StatisticsEvidence` reconstructed from it.
#[derive(Deserialize, Serialize)]
struct IcebergProviderStatisticsV1 {
    version: u16,
    data_version: Vec<u8>,
    metrics: Vec<IcebergProviderStatisticV1>,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "metric", rename_all = "snake_case")]
enum IcebergProviderStatisticV1 {
    RowCount {
        value: IcebergStatisticValueV1,
    },
    NullCount {
        column: String,
        value: IcebergStatisticValueV1,
    },
    Minimum {
        column: String,
        value: IcebergStatisticValueV1,
    },
    Maximum {
        column: String,
        value: IcebergStatisticValueV1,
    },
    AverageSize {
        column: String,
        value: IcebergStatisticValueV1,
    },
    ThetaNdv {
        column: String,
        value: IcebergStatisticValueV1,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum IcebergStatisticValueV1 {
    U64(u64),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
}

#[derive(Deserialize, Serialize)]
enum IcebergMutationEvidenceTarget {
    Namespace {
        namespace: String,
        should_exist: bool,
    },
    Table {
        namespace: String,
        table: String,
        should_exist: bool,
        before_uuid: Option<String>,
    },
    View {
        namespace: String,
        view: String,
        should_exist: bool,
    },
    TableVersion {
        namespace: String,
        table: String,
        table_uuid: String,
        before_metadata_location: Option<String>,
    },
    BootstrapEmptyTableSnapshot {
        namespace: String,
        table: String,
        table_uuid: String,
        operation_marker: String,
    },
    Ref {
        namespace: String,
        table: String,
        table_uuid: String,
        ref_name: String,
        expected_snapshot_id: Option<i64>,
    },
    GuardedFastForward {
        namespace: String,
        table: String,
        table_uuid: String,
        before_metadata_location: Option<String>,
        source_branch: String,
        target_branch: String,
        source_snapshot_id: i64,
        expected_target_snapshot_id: Option<i64>,
        guard_digest: [u8; 32],
    },
}

impl IcebergControlProvider {
    /// Creates the FE-only control binding for a logical Iceberg catalog. The
    /// implementation remains in core until SPI-5, but its runtime capability
    /// aggregate no longer needs to cross into a BE process.
    pub(crate) fn new_control(
        instance_id: ConnectorInstanceId,
        registry: Arc<RwLock<IcebergCatalogRegistry>>,
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
            super::catalog::registry::IcebergCatalogKind::Rest
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
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
            recovery_cleanup_outcomes: Arc::new(Mutex::new(HashMap::new())),
        });
        let write = Arc::new(IcebergWriteControlAdapter::new(
            write_key.clone(),
            Arc::new(RegisteredIcebergWriteControlBackend::new(services.clone())),
        )?);
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
            Arc::new(IcebergInstanceDistribution {
                descriptor,
                incarnation,
            }),
            Some(provider.clone()),
            Some(data_mutation),
            Some(metadata_maintenance),
            Some(distributed_rewrite),
            Some(cleanup_maintenance),
            staged_create,
            Some(write),
            Some(provider.clone()),
        )?
        .try_with_staged_publication_recovery(Some(provider))
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
        let payload = serde_json::to_vec(&IcebergMutationEvidenceV1 {
            version: ICEBERG_MUTATION_EVIDENCE_VERSION,
            target,
        })
        .map_err(|error| internal(format!("encode Iceberg mutation evidence: {error}")))?;
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
        let payload = serde_json::to_vec(&IcebergMutationEvidenceV1 {
            version: ICEBERG_MUTATION_EVIDENCE_VERSION,
            target,
        })
        .map_err(|error| internal(format!("encode Iceberg mutation evidence: {error}")))?;
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
        let payload = serde_json::to_vec(&IcebergMutationEvidenceV1 {
            version: ICEBERG_MUTATION_EVIDENCE_VERSION,
            target,
        })
        .map_err(|error| internal(format!("encode Iceberg bootstrap evidence: {error}")))?;
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
        let ident =
            iceberg::TableIdent::from_strs([table.namespace.as_ref(), table.table.as_ref()])
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("invalid bootstrap table identity: {error}"),
                    )
                })?;
        let commit_result = super::catalog::registry::block_on_iceberg(async {
            let current = catalog.load_table(&ident).await?;
            if current.metadata().current_snapshot().is_some() {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::PreconditionFailed,
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
        let payload = serde_json::to_vec(&IcebergStatisticsEvidenceV1 {
            version: ICEBERG_STATISTICS_EVIDENCE_VERSION,
            namespace: table.namespace.clone(),
            table: table.table.clone(),
            data_version: data_version.as_bytes().to_vec(),
            statistics_path: statistics_path.to_string(),
        })
        .map_err(|error| internal(format!("encode Iceberg statistics evidence: {error}")))?;
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
        projection: &[usize],
    ) -> Result<SchemaRef, ConnectorError> {
        let loaded = load_table(entry, namespace, table).map_err(map_iceberg_error)?;
        let storage_schema =
            iceberg::arrow::schema_to_arrow_schema(loaded.table.metadata().current_schema())
                .map_err(|error| internal(format!("convert Iceberg schema to Arrow: {error}")))?;
        let indexes = if projection.is_empty() {
            (0..storage_schema.fields().len()).collect::<Vec<_>>()
        } else {
            projection.to_vec()
        };
        let fields = indexes
            .into_iter()
            .map(|index| {
                let storage_field = storage_schema.fields().get(index).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg projection index {index} is outside the table schema"),
                    )
                })?;
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

    fn frozen_data_rewrite_schema(
        &self,
        entry: &IcebergCatalogEntry,
        namespace: &str,
        table: &str,
        projection: &[usize],
    ) -> Result<SchemaRef, ConnectorError> {
        let loaded = load_table(entry, namespace, table).map_err(map_iceberg_error)?;
        let storage_schema =
            iceberg::arrow::schema_to_arrow_schema(loaded.table.metadata().current_schema())
                .map_err(|error| internal(format!("convert Iceberg schema to Arrow: {error}")))?;
        let mut storage_fields = storage_schema.fields().to_vec();
        if super::catalog::backend::row_lineage_enabled(loaded.table.metadata()) {
            storage_fields.extend([
                Arc::new(Field::new(
                    crate::exec::row_position::ICEBERG_ROW_ID_COL,
                    DataType::Int64,
                    false,
                )),
                Arc::new(Field::new(
                    crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
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
                if crate::exec::row_position::is_iceberg_row_id(storage_field.name())
                    || crate::exec::row_position::is_iceberg_last_updated_sequence_number(
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
                    let action = lower_ref_action(
                        action,
                        loaded.table.metadata(),
                        &table.namespace,
                        &table.table,
                        self.instance_id.as_str(),
                    )?;
                    let catalog = super::catalog::registry::build_iceberg_catalog(&entry)
                        .map_err(map_iceberg_error)?;
                    super::catalog::registry::block_on_iceberg(async {
                        super::commit::execute_ref_action(catalog.as_ref(), &action).await
                    })
                    .map_err(|error| {
                        ConnectorError::new(
                            ConnectorErrorKind::Internal,
                            format!("execute Iceberg ref mutation runtime: {error}"),
                        )
                    })?
                    .map(|outcome| match outcome {
                        super::commit::RefActionOutcome::Committed => {
                            ExternalMutationEffect::Applied
                        }
                        super::commit::RefActionOutcome::NoOp => ExternalMutationEffect::NoOp,
                    })
                    .map_err(|error| ConnectorError::new(ConnectorErrorKind::Internal, error))
                }
                ConnectorCatalogMutationOperation::AlterProperties { table, changes } => {
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
                    )
                    .map(|()| ExternalMutationEffect::Applied)
                    .map_err(map_iceberg_error)
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
        let decoded: IcebergMutationEvidenceV1 =
            serde_json::from_slice(request.evidence.provider_payload()).map_err(|error| {
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
            let provenance = crate::connector::iceberg::commit::mv_provenance::MvProvenanceV1::from_snapshot_summary(snapshot)
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
        let proof = serde_json::to_vec(&proof)
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
        let proof: IcebergStagedPublicationProofV1 =
            serde_json::from_slice(request.observation.proof.payload()).map_err(|error| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    format!("invalid Iceberg staged publication cleanup proof: {error}"),
                )
            })?;
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

fn iceberg_main_ancestors(metadata: &iceberg::spec::TableMetadata) -> Vec<i64> {
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
    snapshot: &iceberg::spec::Snapshot,
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
    let decoded = super::write_contract::decode_write_receipt(committed_version.payload())
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
    metadata: &iceberg::spec::TableMetadata,
    source_branch: &str,
    target_branch: &str,
    source_snapshot_id: i64,
    expected_target_snapshot_id: Option<i64>,
    guard: &ConnectorRefreshPublicationGuard,
) -> Result<iceberg::TableCommit, ConnectorError> {
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
    let ident = iceberg::TableIdent::from_strs([namespace, table]).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("invalid Iceberg MV publication table identity: {error}"),
        )
    })?;
    Ok(iceberg::TableCommit::builder()
        .ident(ident)
        .updates(vec![iceberg::TableUpdate::SetSnapshotRef {
            ref_name: target_branch.to_string(),
            reference: iceberg::spec::SnapshotReference {
                snapshot_id: source_snapshot_id,
                retention: iceberg::spec::SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        }])
        .requirements(vec![
            iceberg::TableRequirement::RefSnapshotIdMatch {
                r#ref: target_branch.to_string(),
                snapshot_id: expected_target_snapshot_id,
            },
            iceberg::TableRequirement::RefSnapshotIdMatch {
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
    snapshot: &iceberg::spec::Snapshot,
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

fn lower_ref_action(
    action: novarocks_spi::connector::ConnectorRefAction,
    metadata: &iceberg::spec::TableMetadata,
    namespace: &str,
    table: &str,
    catalog: &str,
) -> Result<super::commit::RefActionPlan, ConnectorError> {
    use novarocks_spi::connector::{ConnectorRefAction, ConnectorRefKind};

    fn assert_kind(
        metadata: &iceberg::spec::TableMetadata,
        name: &str,
        expected: ConnectorRefKind,
    ) -> Result<(), ConnectorError> {
        let Some(existing) = metadata.refs().get(name) else {
            return Ok(());
        };
        let actual = match existing.retention {
            iceberg::spec::SnapshotRetention::Branch { .. } => ConnectorRefKind::Branch,
            iceberg::spec::SnapshotRetention::Tag { .. } => ConnectorRefKind::Tag,
        };
        if actual == expected {
            return Ok(());
        }
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("Iceberg ref `{name}` has a different kind"),
        ))
    }

    let action = match action {
        ConnectorRefAction::Create {
            kind,
            name,
            snapshot_id,
            policy,
        } => {
            if name.eq_ignore_ascii_case("main") {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref `main` is reserved",
                ));
            }
            assert_kind(metadata, &name, kind)?;
            let snapshot_id = match snapshot_id.or_else(|| metadata.current_snapshot_id()) {
                Some(snapshot_id) if metadata.snapshot_by_id(snapshot_id).is_some() => snapshot_id,
                _ => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "Iceberg ref create requires an existing snapshot",
                    ));
                }
            };
            let (replace, if_not_exists) = match policy {
                CreateOrReplacePolicy::FailIfExists => (false, false),
                CreateOrReplacePolicy::NoOpIfExists => (false, true),
                CreateOrReplacePolicy::ReplaceIfExists => (true, false),
            };
            match kind {
                ConnectorRefKind::Branch => super::commit::RefAction::CreateBranch {
                    name: name.to_string(),
                    snapshot_id,
                    replace,
                    if_not_exists,
                },
                ConnectorRefKind::Tag => super::commit::RefAction::CreateTag {
                    name: name.to_string(),
                    snapshot_id,
                    replace,
                    if_not_exists,
                },
            }
        }
        ConnectorRefAction::Drop { kind, name, policy } => {
            if name.eq_ignore_ascii_case("main") {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref `main` is reserved",
                ));
            }
            assert_kind(metadata, &name, kind)?;
            let if_exists = policy == DropPolicy::NoOpIfMissing;
            match kind {
                ConnectorRefKind::Branch => super::commit::RefAction::DropBranch {
                    name: name.to_string(),
                    if_exists,
                },
                ConnectorRefKind::Tag => super::commit::RefAction::DropTag {
                    name: name.to_string(),
                    if_exists,
                },
            }
        }
        ConnectorRefAction::FastForwardBranch { .. } => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "guarded MV publication bypassed its provider commit path",
            ));
        }
    };
    Ok(super::commit::RefActionPlan {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
        action,
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
        let schema = self.schema_for(&entry, &request.table.namespace, &table_name, &[])?;
        let version = Some(Bytes::copy_from_slice(
            &loaded.table.metadata().current_schema_id().to_le_bytes(),
        ));
        let statistics_data_version = statistics_data_version(
            &loaded.table.metadata().uuid().to_string(),
            loaded.table.metadata().current_snapshot_id(),
        )?;
        let table = build_table_payload(
            &entry,
            self.instance_id.as_str(),
            &request.table.namespace,
            &table_name,
            loaded,
            metadata_table_type,
        )?;
        Ok(ConnectorTableMetadata {
            identity: novarocks_spi::connector::ConnectorTableIdentity {
                instance_id: self.instance_id.clone(),
                namespace: request.table.namespace,
                table: Arc::from(table_name),
            },
            schema,
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
                super::stats_assembler::read_provider_statistics_blob(
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
        let statistics_path = super::stats_assembler::puffin_path_for_statistics_operation(
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
                super::theta_sketch::ThetaSketchHandle::from_compact_parts(lg_k, theta, hashes)
                    .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))?;
            sketches.insert(*field_id, sketch);
        }
        let statistics_path = super::stats_assembler::puffin_path_for_statistics_operation(
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
            super::stats_assembler::write_puffin_with_provider_statistics(
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
            super::commit::statistics::commit_statistics_file(
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
        let evidence: IcebergStatisticsEvidenceV1 =
            serde_json::from_slice(request.evidence.provider_payload()).map_err(|error| {
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
    files: &[super::catalog::registry::DataFileWithStats],
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
    files: &[super::catalog::registry::DataFileWithStats],
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
            _ => self.schema_for(&entry, &table.namespace, &table.table, &request.projection)?,
        };
        let (snapshot_id, table_uuid) = if table.explicit_files.is_some() {
            (None, None)
        } else if matches!(request.selector, ConnectorReadSelector::Current) {
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
            let (snapshot_id, table_uuid) =
                self.select_snapshot(&entry, &table, request.selector)?;
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
        Ok(ConnectorScan {
            handle: ConnectorScanHandle::try_new(
                self.instance_id.clone(),
                encode_payload(
                    &payload,
                    "scan handle",
                    request.context.max_handle_payload_bytes(),
                )?,
            )?,
            output_schema,
            predicate_dispositions,
        })
    }

    fn plan_splits(
        &self,
        scan: &ConnectorScanHandle,
        request: ConnectorSplitPlanningRequest,
    ) -> Result<ConnectorSplitPlanningResult, ConnectorError> {
        self.validate_context(&request.context)?;
        let scan = self.scan_payload(scan)?;
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
                    .map(super::catalog::backend::data_file_with_stats_to_iceberg_data_file_info)
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
        let planning_binding = IcebergReadBinding::default_binding(
            self.entry(self.instance_id.as_str())?
                .object_store_config()
                .cloned(),
        )?;
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
    entry: &IcebergCatalogEntry,
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
    let mut table_def = if matches!(
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
            .map(super::catalog::backend::data_file_with_stats_to_iceberg_data_file_info)
            .collect();
        super::catalog::backend::build_iceberg_table_def_with_files(
            entry, catalog, namespace, table, loaded, data_files,
        )
        .map_err(map_iceberg_error)?
    } else {
        super::catalog::backend::build_iceberg_schema_table_def_from_loaded(
            entry, catalog, namespace, table, loaded,
        )
        .map_err(map_iceberg_error)?
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
            crate::connector::iceberg::metadata_read::read_metadata_table_rows(
                &metadata_table,
                &metadata_file_io,
                metadata_table_type
                    .clone()
                    .expect("metadata table type is present"),
            )
            .await
        })
        .map_err(map_iceberg_error)?
        .map_err(map_iceberg_error)?;
        if let crate::sql::planner::table::ScanSource::IcebergDataFiles { table, .. } =
            &mut table_def.source
        {
            table.serialized_metadata_rows = Some(rows);
        }
    }
    let hidden_columns = super::catalog::backend::hidden_internal_column_names_from_metadata(
        metadata_table.metadata(),
    );
    let metadata_columns = table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let (table_info, source_files) = match table_def.source {
        crate::sql::planner::table::ScanSource::IcebergDataFiles { table, files, .. } => {
            (table, files)
        }
        _ => {
            return Err(internal(
                "Iceberg metadata capability produced a non-Iceberg table source".to_string(),
            ));
        }
    };
    if prepared_files.is_empty() {
        prepared_files = source_files;
    }
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

#[derive(Clone, Deserialize, Serialize)]
struct TablePayload {
    namespace: String,
    table: String,
    table_info: Option<super::scan_model::IcebergTableInfo>,
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

#[derive(Deserialize, Serialize)]
struct SplitPayload {
    version: u16,
    owner_instance_id: String,
    incarnation: [u8; 16],
    namespace: String,
    table: String,
    snapshot_id: Option<i64>,
    #[serde(default)]
    table_uuid: Option<String>,
    #[serde(default)]
    schema_id: Option<i32>,
    units: Vec<IcebergFrozenScanUnitPayload>,
    projection: Vec<usize>,
    limit: Option<u64>,
    #[serde(default)]
    physical_predicates: Vec<IcebergPhysicalPredicate>,
    #[serde(default)]
    fact_columns: Vec<IcebergScanFactColumnV1>,
    #[serde(default)]
    name_mapping: Option<String>,
    #[serde(default)]
    delta: Option<IcebergDeltaSplitPayload>,
    #[serde(default)]
    distributed_rewrite_position: Option<IcebergRewritePositionSplitPayloadV1>,
}

/// FE-frozen physical leaf. The outer split is intentionally a composite
/// carrier; only this provider interprets the leaf membership.
#[derive(Deserialize, Serialize)]
struct IcebergFrozenScanUnitPayload {
    data_file: IcebergDataFileInfo,
    /// `Some` selects exactly those Parquet row groups. `None` denotes one
    /// whole-file leaf (ORC and special Iceberg roles).
    row_groups: Option<Vec<usize>>,
    estimated_bytes: Option<u64>,
}

#[derive(Deserialize, Serialize)]
struct IcebergPreparedSplitSharedPayload {
    version: u16,
    owner_instance_id: String,
    incarnation: [u8; 16],
    namespace: String,
    table: String,
    snapshot_id: Option<i64>,
    table_uuid: Option<String>,
    schema_id: Option<i32>,
    projection: Vec<usize>,
    limit: Option<u64>,
    physical_predicates: Vec<IcebergPhysicalPredicate>,
    #[serde(default)]
    fact_columns: Vec<IcebergScanFactColumnV1>,
    name_mapping: Option<String>,
    delta: Option<IcebergDeltaSplitPayload>,
    distributed_rewrite_position: Option<IcebergRewritePositionSplitPayloadV1>,
}

#[derive(Deserialize, Serialize)]
struct IcebergPreparedUnitPayload {
    version: u16,
    data_file: IcebergDataFileInfo,
    row_groups: Option<Vec<usize>>,
}

/// Provider-private description of one projected top-level Iceberg field.
/// It binds the table-schema ordinal to the field ID, canonical name, and
/// scalar vocabulary before a split crosses the FE/BE boundary.
#[derive(Clone, Deserialize, Serialize)]
struct IcebergScanFactColumnV1 {
    field_ordinal: u32,
    field_id: i32,
    canonical_name: String,
    scalar_type: IcebergScanFactScalarTypeV1,
    nullable: bool,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
enum IcebergScanFactScalarTypeV1 {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Date32,
    TimestampMicros,
    TimestampNanos,
    Utf8,
    Binary,
    Unsupported,
}

/// Provider-private maintenance split.  The C1 carrier only transports this
/// opaque payload; it never learns Puffin metadata or deletion-vector rows.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergRewritePositionSplitPayloadV1 {
    pub(crate) version: u16,
    pub(crate) selected_delete_files: Vec<IcebergDeleteFileInfo>,
}

const ICEBERG_REWRITE_POSITION_SPLIT_V1: u16 = 1;

const ICEBERG_SPLIT_V5: u16 = 5;
const ICEBERG_PREPARED_SPLIT_SHARED_V2: u16 = 2;
const ICEBERG_PREPARED_SCAN_UNIT_V1: u16 = 1;

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
        (0..table_info.schema.fields.len()).collect::<Vec<_>>()
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
            Ok(IcebergScanFactColumnV1 {
                field_ordinal,
                field_id: table_field.field_id,
                canonical_name: table_field.name.to_ascii_lowercase(),
                scalar_type: iceberg_scan_fact_scalar_type(field.data_type()),
                nullable: field.is_nullable(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The projection preserves planner output order, which can differ from the
    // frozen table schema. Domain facts are sealed in table-schema ordinal
    // order so their constructor can reject duplicate or reordered entries.
    columns.sort_by_key(|column| column.field_ordinal);
    Ok(columns)
}

fn iceberg_scan_fact_scalar_type(data_type: &DataType) -> IcebergScanFactScalarTypeV1 {
    match data_type {
        DataType::Boolean => IcebergScanFactScalarTypeV1::Boolean,
        DataType::Int8 => IcebergScanFactScalarTypeV1::Int8,
        DataType::Int16 => IcebergScanFactScalarTypeV1::Int16,
        DataType::Int32 => IcebergScanFactScalarTypeV1::Int32,
        DataType::Int64 => IcebergScanFactScalarTypeV1::Int64,
        DataType::Date32 => IcebergScanFactScalarTypeV1::Date32,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None) => {
            IcebergScanFactScalarTypeV1::TimestampMicros
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
            IcebergScanFactScalarTypeV1::TimestampNanos
        }
        DataType::Utf8 => IcebergScanFactScalarTypeV1::Utf8,
        DataType::Binary => IcebergScanFactScalarTypeV1::Binary,
        _ => IcebergScanFactScalarTypeV1::Unsupported,
    }
}

fn split_name_mapping(table: &TablePayload) -> Result<Option<String>, ConnectorError> {
    let Some(serialized_metadata) = table
        .table_info
        .as_ref()
        .and_then(|table| table.serialized_metadata.as_deref())
    else {
        return Ok(None);
    };
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(serialized_metadata)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!("decode pinned Iceberg metadata for name mapping: {error}"),
            )
        })?;
    let Some(mapping) = metadata
        .properties()
        .get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
    else {
        return Ok(None);
    };
    canonical_split_name_mapping(mapping).map(Some)
}

fn canonical_split_name_mapping(mapping: &str) -> Result<String, ConnectorError> {
    if mapping.len() > novarocks_spi::connector::MAX_CONNECTOR_DATA_MUTATION_PROVIDER_PAYLOAD_BYTES
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg name mapping exceeds the provider-private split bound",
        ));
    }
    super::catalog::add_files::canonical_name_mapping(mapping)
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::CorruptData, error))
}

fn validate_split_name_mapping(mapping: Option<&str>) -> Result<(), ConnectorError> {
    let Some(mapping) = mapping else {
        return Ok(());
    };
    let canonical = canonical_split_name_mapping(mapping)?;
    if canonical != mapping {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg split name mapping is not canonical",
        ));
    }
    Ok(())
}

fn validate_split_payload(payload: &SplitPayload) -> Result<(), ConnectorError> {
    if payload.version != ICEBERG_SPLIT_V5 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "unsupported Iceberg composite split version {}",
                payload.version
            ),
        ));
    }
    validate_split_name_mapping(payload.name_mapping.as_deref())?;
    if let Some(rewrite_position) = payload.distributed_rewrite_position.as_ref() {
        if rewrite_position.version != ICEBERG_REWRITE_POSITION_SPLIT_V1
            || rewrite_position.selected_delete_files.is_empty()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg rewrite-position split is invalid",
            ));
        }
    }
    Ok(())
}

fn validate_prepared_payload(
    shared: &IcebergPreparedSplitSharedPayload,
    unit: &IcebergPreparedUnitPayload,
) -> Result<(), ConnectorError> {
    if shared.version != ICEBERG_PREPARED_SPLIT_SHARED_V2
        || unit.version != ICEBERG_PREPARED_SCAN_UNIT_V1
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "unsupported Iceberg prepared scan unit payload version",
        ));
    }
    validate_split_name_mapping(shared.name_mapping.as_deref())?;
    if let Some(row_groups) = unit.row_groups.as_ref() {
        if row_groups.is_empty() || row_groups.windows(2).any(|window| window[0] >= window[1]) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg prepared scan unit row groups must be non-empty and strictly ordered",
            ));
        }
    }
    Ok(())
}

/// Materialize Parquet leaves locally from the exact FE-frozen file list.
/// This never consults catalog metadata: footer reads only refine the physical
/// membership of an already-authorized file, and all resulting unit costs add
/// back to the frozen file cost exactly.
fn materialize_local_scan_units(
    binding: &IcebergReadBinding,
    frozen_units: Vec<IcebergFrozenScanUnitPayload>,
    special_unit: bool,
    request: &ConnectorPrepareSplitRequest,
) -> Result<Vec<IcebergFrozenScanUnitPayload>, ConnectorError> {
    if special_unit {
        return Ok(frozen_units);
    }
    let mut materialized = Vec::with_capacity(frozen_units.len());
    for unit in frozen_units {
        request.check_active()?;
        if unit.row_groups.is_some() || !is_parquet_path(&unit.data_file.path) {
            materialized.push(unit);
            continue;
        }
        let file_size = u64::try_from(unit.data_file.size).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!(
                    "Iceberg data file {} has a negative size",
                    unit.data_file.path
                ),
            )
        })?;
        let access = binding.resolve_access(&unit.data_file.path)?;
        let file = access
            .bind_location(
                &unit.data_file.path,
                FileIdentity::new(&unit.data_file.path, file_size, None),
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let context =
            binding.file_read_context(FileCancellation::new(), request.context.deadline())?;
        let metadata =
            inspect_parquet_metadata(file, None, context).map_err(map_iceberg_footer_error)?;
        let layout = metadata.row_groups();
        request.check_active()?;
        if layout.is_empty() {
            materialized.push(unit);
            continue;
        }
        // A one-row-group Parquet file is already exactly one local unit.
        // Retain it as a whole-file unit so small-file packing remains visible
        // and the reader does not need a redundant row-group selector.
        if layout.len() == 1 {
            materialized.push(unit);
            continue;
        }
        let file_cost = unit.estimated_bytes.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg Parquet split unit must carry a known frozen cost",
            )
        })?;
        let costs = distribute_unit_cost(file_cost, &layout)?;
        for (row_group, estimated_bytes) in layout.into_iter().zip(costs) {
            materialized.push(IcebergFrozenScanUnitPayload {
                data_file: unit.data_file.clone(),
                row_groups: Some(vec![row_group.ordinal as usize]),
                estimated_bytes: Some(estimated_bytes),
            });
        }
    }
    Ok(materialized)
}

fn iceberg_unit_domain_facts(
    binding: &IcebergReadBinding,
    inspections: &mut HashMap<String, ParquetMetadataInspection>,
    unit: &IcebergFrozenScanUnitPayload,
    fact_columns: &[IcebergScanFactColumnV1],
    conservative: bool,
    special_unit: bool,
    request: &ConnectorPrepareSplitRequest,
) -> Result<ConnectorScanUnitDomainFacts, ConnectorError> {
    if special_unit || !is_parquet_path(&unit.data_file.path) {
        return Ok(ConnectorScanUnitDomainFacts::missing(
            ConnectorScanUnitFactsMissingReason::ProviderUnsupported,
        ));
    }
    if fact_columns.is_empty()
        || fact_columns
            .iter()
            .any(|column| matches!(column.scalar_type, IcebergScanFactScalarTypeV1::Unsupported))
    {
        return Ok(ConnectorScanUnitDomainFacts::missing(
            ConnectorScanUnitFactsMissingReason::DataTypeUnsupported,
        ));
    }
    if !inspections.contains_key(&unit.data_file.path) {
        let file_size = u64::try_from(unit.data_file.size).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                format!(
                    "Iceberg data file {} has a negative size",
                    unit.data_file.path
                ),
            )
        })?;
        let access = binding.resolve_access(&unit.data_file.path)?;
        let file = access
            .bind_location(
                &unit.data_file.path,
                FileIdentity::new(&unit.data_file.path, file_size, None),
            )
            .map_err(|error| {
                ConnectorError::new(ConnectorErrorKind::InvalidRequest, error.to_string())
            })?;
        let context =
            binding.file_read_context(FileCancellation::new(), request.context.deadline())?;
        let inspection =
            inspect_parquet_metadata(file, None, context).map_err(map_iceberg_footer_error)?;
        inspections.insert(unit.data_file.path.clone(), inspection);
    }
    let inspection = inspections
        .get(&unit.data_file.path)
        .expect("inserted authorized Parquet inspection");
    request.check_active()?;
    let selected = selected_parquet_row_groups(inspection, unit.row_groups.as_deref())?;
    let physical = map_iceberg_fact_columns(inspection, fact_columns)?;
    let physical_row_count = selected.iter().try_fold(0_u64, |total, row_group| {
        total.checked_add(row_group.row_count).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg selected Parquet row count overflows facts accounting",
            )
        })
    })?;
    let evidence = if conservative {
        ConnectorScanUnitFactsEvidence::Conservative
    } else {
        ConnectorScanUnitFactsEvidence::Exact
    };
    let columns = physical
        .iter()
        .map(|(column, physical_ordinal)| {
            iceberg_column_domain_facts(
                inspection,
                selected.as_slice(),
                *physical_ordinal,
                column,
                physical_row_count,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    ConnectorScanUnitDomainFacts::available(physical_row_count, evidence, columns)
}

fn selected_parquet_row_groups<'a>(
    inspection: &'a ParquetMetadataInspection,
    selected: Option<&[usize]>,
) -> Result<Vec<&'a novarocks_fs::ParquetRowGroupLayout>, ConnectorError> {
    match selected {
        None => Ok(inspection.row_groups().iter().collect()),
        Some(selected) => selected
            .iter()
            .map(|ordinal| {
                inspection.row_groups().get(*ordinal).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg prepared scan unit selects a Parquet row group outside the frozen footer",
                    )
                })
            })
            .collect(),
    }
}

fn map_iceberg_footer_error(error: FileError) -> ConnectorError {
    let kind = match error.kind() {
        FileErrorKind::Invalid => ConnectorErrorKind::InvalidRequest,
        FileErrorKind::Unsupported => ConnectorErrorKind::Unsupported,
        // A file that disappeared after the FE froze it contradicts the
        // sealed membership; do not turn that contradiction into fail-open.
        FileErrorKind::NotFound | FileErrorKind::Corrupt => ConnectorErrorKind::CorruptData,
        FileErrorKind::Permission => ConnectorErrorKind::PermissionDenied,
        FileErrorKind::ResourceExhausted => ConnectorErrorKind::ResourceExhausted,
        FileErrorKind::Transient => ConnectorErrorKind::Unavailable,
        FileErrorKind::DeadlineExceeded => ConnectorErrorKind::DeadlineExceeded,
        FileErrorKind::Cancelled => ConnectorErrorKind::Cancelled,
        FileErrorKind::Internal => ConnectorErrorKind::Internal,
    };
    ConnectorError::new(kind, error.to_string())
}

fn map_iceberg_fact_columns<'a>(
    inspection: &ParquetMetadataInspection,
    columns: &'a [IcebergScanFactColumnV1],
) -> Result<Vec<(&'a IcebergScanFactColumnV1, Option<u32>)>, ConnectorError> {
    let physical = inspection.physical_columns();
    let physical_columns_len = physical.len();
    let with_ids = physical
        .iter()
        .filter(|column| column.field_id().is_some())
        .count();
    if with_ids != 0 && with_ids != physical.len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg Parquet footer has mixed field-ID coverage",
        ));
    }
    columns
        .iter()
        .map(|column| {
            let matches = physical
                .iter()
                .filter(|physical| {
                    if with_ids == physical_columns_len {
                        physical.field_id() == Some(column.field_id)
                    } else {
                        physical.path().len() == 1
                            && physical.path()[0].eq_ignore_ascii_case(&column.canonical_name)
                    }
                })
                .collect::<Vec<_>>();
            if matches.len() > 1 {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg Parquet footer maps a frozen field to multiple physical leaves",
                ));
            }
            Ok((column, matches.first().map(|physical| physical.ordinal())))
        })
        .collect()
}

fn iceberg_column_domain_facts(
    inspection: &ParquetMetadataInspection,
    selected: &[&novarocks_fs::ParquetRowGroupLayout],
    physical_ordinal: Option<u32>,
    frozen: &IcebergScanFactColumnV1,
    physical_row_count: u64,
) -> Result<ConnectorScanUnitColumnFacts, ConnectorError> {
    let scalar_type = iceberg_fact_scalar_type(frozen.scalar_type).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "unsupported Iceberg facts scalar reached the Parquet mapper",
        )
    })?;
    let column = ConnectorScanUnitColumn::new(frozen.field_ordinal, scalar_type, frozen.nullable);
    let Some(physical_ordinal) = physical_ordinal else {
        return Ok(ConnectorScanUnitColumnFacts::missing(
            column,
            ConnectorScanUnitFactsMissingReason::ValueUnavailable,
        ));
    };
    if physical_row_count == 0 {
        return Ok(ConnectorScanUnitColumnFacts::missing(
            column,
            ConnectorScanUnitFactsMissingReason::ValueUnavailable,
        ));
    }
    let mut null_count = 0_u64;
    let mut min: Option<ConnectorScalarValue> = None;
    let mut max: Option<ConnectorScalarValue> = None;
    for row_group in selected {
        let Some(statistics) = inspection.column_statistics(row_group.ordinal, physical_ordinal)
        else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
            ));
        };
        let Some(row_group_nulls) = statistics.null_count() else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
            ));
        };
        null_count = null_count.checked_add(row_group_nulls).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg Parquet null count overflows facts accounting",
            )
        })?;
        if row_group_nulls == row_group.row_count {
            continue;
        }
        if !statistics.min_is_exact()
            || !statistics.max_is_exact()
            || statistics.min_max_deprecated()
            || !matches!(
                statistics.sort_order(),
                ParquetStatisticsSortOrder::Signed | ParquetStatisticsSortOrder::Unsigned
            )
        {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
            ));
        }
        let (Some(row_min), Some(row_max)) = (statistics.min(), statistics.max()) else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
            ));
        };
        let Some(row_min) = parquet_statistic_scalar(row_min, frozen.scalar_type) else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::DataTypeUnsupported,
            ));
        };
        let Some(row_max) = parquet_statistic_scalar(row_max, frozen.scalar_type) else {
            return Ok(ConnectorScanUnitColumnFacts::missing(
                column,
                ConnectorScanUnitFactsMissingReason::DataTypeUnsupported,
            ));
        };
        min = match min {
            Some(current)
                if current
                    .compare_same_type(&row_min)
                    .is_some_and(|order| order.is_gt()) =>
            {
                Some(row_min)
            }
            Some(current) => Some(current),
            None => Some(row_min),
        };
        max = match max {
            Some(current)
                if current
                    .compare_same_type(&row_max)
                    .is_some_and(|order| order.is_lt()) =>
            {
                Some(row_max)
            }
            Some(current) => Some(current),
            None => Some(row_max),
        };
    }
    if null_count > physical_row_count {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg Parquet null count exceeds selected physical rows",
        ));
    }
    if null_count == physical_row_count {
        return ConnectorScanUnitColumnDomain::try_all_null(column, null_count, physical_row_count);
    }
    match (min, max) {
        (Some(min), Some(max)) => ConnectorScanUnitColumnDomain::try_range(
            column,
            min,
            max,
            null_count,
            physical_row_count,
        ),
        _ => Ok(ConnectorScanUnitColumnFacts::missing(
            column,
            ConnectorScanUnitFactsMissingReason::PhysicalStatisticsAbsent,
        )),
    }
}

fn iceberg_fact_scalar_type(
    scalar_type: IcebergScanFactScalarTypeV1,
) -> Option<ConnectorScalarType> {
    Some(match scalar_type {
        IcebergScanFactScalarTypeV1::Boolean => ConnectorScalarType::Boolean,
        IcebergScanFactScalarTypeV1::Int8 => ConnectorScalarType::Int8,
        IcebergScanFactScalarTypeV1::Int16 => ConnectorScalarType::Int16,
        IcebergScanFactScalarTypeV1::Int32 => ConnectorScalarType::Int32,
        IcebergScanFactScalarTypeV1::Int64 => ConnectorScalarType::Int64,
        IcebergScanFactScalarTypeV1::Date32 => ConnectorScalarType::Date32,
        IcebergScanFactScalarTypeV1::TimestampMicros => ConnectorScalarType::TimestampMicros,
        IcebergScanFactScalarTypeV1::TimestampNanos => ConnectorScalarType::TimestampNanos,
        IcebergScanFactScalarTypeV1::Utf8 => ConnectorScalarType::Utf8,
        IcebergScanFactScalarTypeV1::Binary => ConnectorScalarType::Binary,
        IcebergScanFactScalarTypeV1::Unsupported => return None,
    })
}

fn parquet_statistic_scalar(
    value: &ParquetStatisticsValue,
    scalar_type: IcebergScanFactScalarTypeV1,
) -> Option<ConnectorScalarValue> {
    match (scalar_type, value) {
        (IcebergScanFactScalarTypeV1::Boolean, ParquetStatisticsValue::Boolean(value)) => {
            Some(ConnectorScalarValue::Boolean(*value))
        }
        (IcebergScanFactScalarTypeV1::Int32, ParquetStatisticsValue::Int32(value)) => {
            Some(ConnectorScalarValue::Int32(*value))
        }
        (IcebergScanFactScalarTypeV1::Date32, ParquetStatisticsValue::Int32(value)) => {
            Some(ConnectorScalarValue::Date32(*value))
        }
        (IcebergScanFactScalarTypeV1::Int64, ParquetStatisticsValue::Int64(value)) => {
            Some(ConnectorScalarValue::Int64(*value))
        }
        (IcebergScanFactScalarTypeV1::TimestampMicros, ParquetStatisticsValue::Int64(value)) => {
            Some(ConnectorScalarValue::TimestampMicros(*value))
        }
        (IcebergScanFactScalarTypeV1::TimestampNanos, ParquetStatisticsValue::Int64(value)) => {
            Some(ConnectorScalarValue::TimestampNanos(*value))
        }
        (IcebergScanFactScalarTypeV1::Utf8, ParquetStatisticsValue::ByteArray(value)) => {
            std::str::from_utf8(value)
                .ok()
                .map(|value| ConnectorScalarValue::Utf8(value.to_string()))
        }
        (IcebergScanFactScalarTypeV1::Binary, ParquetStatisticsValue::ByteArray(value)) => {
            Some(ConnectorScalarValue::Binary(value.clone()))
        }
        _ => None,
    }
}

fn distribute_unit_cost(
    total: u64,
    layout: &[novarocks_fs::ParquetRowGroupLayout],
) -> Result<Vec<u64>, ConnectorError> {
    let weight_total = layout.iter().try_fold(0_u64, |sum, row_group| {
        sum.checked_add(row_group.compressed_bytes)
    });
    let mut costs = Vec::with_capacity(layout.len());
    if let Some(weight_total) = weight_total.filter(|total| *total > 0) {
        let mut assigned = 0_u64;
        for (index, row_group) in layout.iter().enumerate() {
            let cost = if index + 1 == layout.len() {
                total.checked_sub(assigned).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg row-group cost accounting underflowed",
                    )
                })?
            } else {
                total
                    .checked_mul(row_group.compressed_bytes)
                    .and_then(|value| value.checked_div(weight_total))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::ResourceExhausted,
                            "Iceberg row-group cost accounting overflowed",
                        )
                    })?
            };
            assigned = assigned.checked_add(cost).ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg row-group cost accounting overflowed",
                )
            })?;
            costs.push(cost);
        }
    } else {
        let count = u64::try_from(layout.len()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg row-group count overflows u64",
            )
        })?;
        let base = total / count;
        let mut remainder = total % count;
        for _ in layout {
            let extra = u64::from(remainder > 0);
            remainder = remainder.saturating_sub(extra);
            costs.push(base + extra);
        }
    }
    Ok(costs)
}

fn is_parquet_path(path: &str) -> bool {
    let path = path.split('?').next().unwrap_or(path).to_ascii_lowercase();
    path.ends_with(".parquet") || path.ends_with(".parq")
}

fn rewrite_position_output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("_file", DataType::Utf8, false),
        Field::new("_pos", DataType::Int64, false),
    ]))
}

pub(crate) fn load_schema_table_def(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<
    (
        crate::sql::planner::table::TableDef,
        Option<i32>,
        Option<ResolvedTableStatisticsPin>,
    ),
    String,
> {
    let (table_def, schema_id, statistics_pin, _planning_lease) =
        load_schema_table_def_with_lease(controls, context, catalog, namespace, table)?;
    Ok((table_def, schema_id, statistics_pin))
}

/// Resolves an Iceberg schema while retaining the exact control generation
/// that served metadata. Query-scoped callers carry this lease through
/// statistics and split planning instead of resolving `latest` again.
pub(crate) fn load_schema_table_def_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<
    (
        crate::sql::planner::table::TableDef,
        Option<i32>,
        Option<ResolvedTableStatisticsPin>,
        novarocks_spi::connector::ConnectorControlPlanningLease,
    ),
    String,
> {
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };

    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let metadata = lease
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
    let payload: TablePayload = decode_payload(metadata.table.payload(), "table handle")
        .map_err(|error| error.to_string())?;
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    let table_info = payload
        .table_info
        .ok_or_else(|| "Iceberg SPI table metadata is missing its read descriptor".to_string())?;
    let columns = columns_from_metadata(&metadata.schema, &payload.logical_type_columns)
        .into_iter()
        .filter(|column| {
            !payload
                .hidden_columns
                .iter()
                .any(|hidden| column.name.eq_ignore_ascii_case(hidden))
        })
        .collect();
    let table_def = crate::sql::planner::table::TableDef {
        name: payload.table,
        columns,
        iceberg_row_lineage_metadata_columns: iceberg_metadata_columns(&payload.metadata_columns)?,
        source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: table_info,
            files: Vec::new(),
            cloud_properties: BTreeMap::new(),
            binding: super::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        },
    };
    let statistics_pin = metadata
        .statistics_data_version
        .clone()
        .map(|data_version| ResolvedTableStatisticsPin {
            table: metadata.table.clone(),
            data_version,
        });
    Ok((table_def, schema_id, statistics_pin, lease))
}

pub(crate) fn load_table_def_at(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    snapshot_id: Option<i64>,
    schema_only: bool,
) -> Result<
    (
        crate::sql::planner::table::TableDef,
        Option<i32>,
        Option<ResolvedTableStatisticsPin>,
    ),
    String,
> {
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };

    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let metadata = lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })
        .map_err(|error| error.to_string())?;
    let payload: TablePayload = decode_payload(metadata.table.payload(), "table handle")
        .map_err(|error| error.to_string())?;
    let schema_id = metadata.version.as_ref().and_then(|version| {
        <[u8; 4]>::try_from(version.as_ref())
            .ok()
            .map(i32::from_le_bytes)
    });
    let mut table_info = payload
        .table_info
        .ok_or_else(|| "Iceberg SPI table metadata is missing its read descriptor".to_string())?;
    let binding = if snapshot_id.is_some() {
        table_info.current_snapshot_id = snapshot_id;
        super::scan_model::IcebergDataFileBinding::ExplicitFiles
    } else {
        super::scan_model::IcebergDataFileBinding::CurrentSnapshot
    };
    let files = if schema_only {
        Vec::new()
    } else {
        plan_scan_files(
            controls,
            context,
            &table_info,
            binding,
            &payload.prepared_files,
            &(0..metadata.schema.fields().len()).collect::<Vec<_>>(),
        )?
    };
    let columns = columns_from_metadata(&metadata.schema, &payload.logical_type_columns)
        .into_iter()
        .filter(|column| {
            !payload
                .hidden_columns
                .iter()
                .any(|hidden| column.name.eq_ignore_ascii_case(hidden))
        })
        .collect();
    let table_def = crate::sql::planner::table::TableDef {
        name: payload.table,
        columns,
        iceberg_row_lineage_metadata_columns: iceberg_metadata_columns(&payload.metadata_columns)?,
        source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: table_info,
            files,
            cloud_properties: BTreeMap::new(),
            binding,
        },
    };
    let statistics_pin = metadata
        .statistics_data_version
        .clone()
        .map(|data_version| ResolvedTableStatisticsPin {
            table: metadata.table.clone(),
            data_version,
        });
    Ok((table_def, schema_id, statistics_pin))
}

/// Resolve a fixed Iceberg snapshot without publishing a synthetic table to
/// the process-wide local catalog. The returned planning lease is retained by
/// the query binding and later reused by statistics and split preparation.
pub(crate) fn load_time_travel_table_def_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    snapshot_id: i64,
) -> Result<
    (
        crate::sql::planner::table::TableDef,
        Option<ResolvedTableStatisticsPin>,
        novarocks_spi::connector::ConnectorControlPlanningLease,
    ),
    String,
> {
    let (mut table_def, _schema_id, statistics_pin, planning_lease) =
        load_schema_table_def_with_lease(controls, context, catalog, namespace, table)?;
    let crate::sql::planner::table::ScanSource::IcebergDataFiles {
        table,
        files,
        binding,
        ..
    } = &mut table_def.source
    else {
        return Err("Iceberg time travel metadata did not produce a file scan".to_string());
    };
    table.current_snapshot_id = Some(snapshot_id);
    files.clear();
    *binding = super::scan_model::IcebergDataFileBinding::ExplicitFiles;
    Ok((table_def, statistics_pin, planning_lease))
}

pub(crate) fn load_metadata_table_def(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    metadata_table_type: super::IcebergMetadataTableType,
) -> Result<crate::sql::planner::table::TableDef, String> {
    use novarocks_spi::connector::{
        ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    };
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let alias = format!("{table}${}", metadata_table_name(&metadata_table_type));
    let metadata = lease
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
    table_def_from_metadata(metadata)
}

fn table_def_from_metadata(
    metadata: ConnectorTableMetadata,
) -> Result<crate::sql::planner::table::TableDef, String> {
    let payload: TablePayload = decode_payload(metadata.table.payload(), "table handle")
        .map_err(|error| error.to_string())?;
    let table_info = payload
        .table_info
        .ok_or_else(|| "Iceberg SPI table metadata is missing its read descriptor".to_string())?;
    Ok(crate::sql::planner::table::TableDef {
        name: payload.table,
        columns: columns_from_metadata(&metadata.schema, &payload.logical_type_columns),
        iceberg_row_lineage_metadata_columns: iceberg_metadata_columns(&payload.metadata_columns)?,
        source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
            table: table_info,
            files: payload.prepared_files,
            cloud_properties: BTreeMap::new(),
            binding: super::scan_model::IcebergDataFileBinding::CurrentSnapshot,
        },
    })
}

fn columns_from_metadata(
    schema: &Schema,
    logical_type_columns: &BTreeMap<String, String>,
) -> Vec<novarocks_catalog::schema::ColumnDef> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let logical_type = match logical_type_columns
                .get(&field.name().to_lowercase())
                .map(String::as_str)
            {
                Some("bitmap") => Some(SqlType::Bitmap),
                Some("hll") => Some(SqlType::Hll),
                _ => None,
            };
            novarocks_catalog::schema::ColumnDef {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                write_default: None,
                logical_type,
            }
        })
        .collect()
}

fn iceberg_metadata_columns(
    names: &[String],
) -> Result<Vec<novarocks_catalog::schema::ColumnDef>, String> {
    names
        .iter()
        .map(|name| {
            let (data_type, nullable) = match name.as_str() {
                "_file" => (arrow::datatypes::DataType::Utf8, false),
                "_pos" | "_row_id" | "_last_updated_sequence_number" => {
                    (arrow::datatypes::DataType::Int64, false)
                }
                other => return Err(format!("unknown Iceberg metadata column `{other}`")),
            };
            Ok(novarocks_catalog::schema::ColumnDef {
                name: name.clone(),
                data_type,
                nullable,
                write_default: None,
                logical_type: None,
            })
        })
        .collect()
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

pub(crate) fn plan_scan_files(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
    explicit_files: &[IcebergDataFileInfo],
    projection: &[usize],
) -> Result<Vec<IcebergDataFileInfo>, String> {
    // Schema metadata does not carry data-file planning results.  A
    // time-travel table therefore has an ExplicitFiles binding with an empty
    // placeholder here; it must still ask the provider to enumerate the
    // table's already-pinned snapshot rather than treating that placeholder
    // as an explicitly empty scan.
    let file_override = (!matches!(
        binding,
        super::scan_model::IcebergDataFileBinding::ExplicitFiles
    ) || !explicit_files.is_empty())
    .then_some(explicit_files);
    let planned = plan_native_iceberg_read_with_file_override(
        controls,
        context,
        table,
        binding,
        file_override,
        projection,
        Vec::new(),
        NonZeroUsize::new(1).expect("metadata file enumeration parallelism is nonzero"),
        None,
    )?;
    planned
        .splits
        .iter()
        .map(|split| {
            decode_payload::<SplitPayload>(split.payload(), "split")
                .map(|payload| {
                    payload
                        .units
                        .into_iter()
                        .map(|unit| unit.data_file)
                        .collect::<Vec<_>>()
                })
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|groups| groups.into_iter().flatten().collect())
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

/// Fully plans an Iceberg read through its real connector instance.  The
/// returned scan handle and splits are provider-owned bytes; callers may only
/// schedule and carry them.  This is intentionally separate from range
/// planning so native execution never reconstructs an Iceberg file scan in
/// core.
pub(crate) fn plan_native_iceberg_read(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
    explicit_files: &[IcebergDataFileInfo],
    projection: &[usize],
    static_predicates: Vec<ConnectorStaticPredicate>,
    target_parallelism: NonZeroUsize,
    max_split_bytes: Option<NonZeroU64>,
) -> Result<PlannedIcebergConnectorRead, String> {
    plan_native_iceberg_read_with_file_override(
        controls,
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

/// Plans an Iceberg read against a control generation selected during query
/// metadata resolution.  Callers retain the returned lease in the prepared
/// execution binding through the backend ensure barrier; this path must not
/// acquire a newer control generation.
pub(crate) fn plan_native_iceberg_read_with_lease(
    lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
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

fn plan_native_iceberg_read_with_file_override(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
    explicit_files: Option<&[IcebergDataFileInfo]>,
    projection: &[usize],
    static_predicates: Vec<ConnectorStaticPredicate>,
    target_parallelism: NonZeroUsize,
    max_split_bytes: Option<NonZeroU64>,
) -> Result<PlannedIcebergConnectorRead, String> {
    let instance_id =
        ConnectorInstanceId::parse(&table.catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    plan_native_iceberg_read_with_bound_lease(
        lease,
        context,
        table,
        binding,
        explicit_files,
        projection,
        static_predicates,
        target_parallelism,
        max_split_bytes,
    )
}

fn plan_native_iceberg_read_with_bound_lease(
    lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    binding: super::scan_model::IcebergDataFileBinding,
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
        super::scan_model::IcebergDataFileBinding::ExplicitFiles
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
    let scan = control_binding
        .planning()
        .begin_scan(
            &table_handle,
            novarocks_spi::connector::ConnectorBeginScanRequest {
                projection: projection.to_vec(),
                static_predicates: static_predicates.clone(),
                selector: table
                    .current_snapshot_id
                    .filter(|_| {
                        matches!(
                            binding,
                            super::scan_model::IcebergDataFileBinding::ExplicitFiles
                        )
                    })
                    .map(ConnectorReadSelector::SnapshotId)
                    .unwrap_or(ConnectorReadSelector::Current),
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
    normalize_predicate_dispositions(&static_predicates, &scan.predicate_dispositions)
        .map_err(|error| format!("Iceberg connector static predicate response: {error}"))?;
    let split_result = control_binding
        .planning()
        .plan_splits(
            &scan.handle,
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

/// Plan snapshot-delta physical reads as ordinary opaque Iceberg connector
/// splits.  Delta retains its logical planner identity; no native carrier or
/// core scan operator receives a role-specific file/deletion payload.
pub(crate) fn plan_native_iceberg_delta_read(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    sources: &[super::delta::DeltaSourceFile],
    delete_side: Option<&super::delta::DeltaScanDeleteSide>,
    target_parallelism: NonZeroUsize,
    max_split_bytes: Option<NonZeroU64>,
) -> Result<PlannedIcebergConnectorRead, String> {
    let instance_id =
        ConnectorInstanceId::parse(&table.catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    plan_native_iceberg_delta_read_with_lease(
        lease,
        context,
        table,
        sources,
        delete_side,
        target_parallelism,
        max_split_bytes,
    )
}

/// Equivalent to [`plan_native_iceberg_delta_read`] but uses the exact
/// metadata lease already retained by the query binding store.
pub(crate) fn plan_native_iceberg_delta_read_with_lease(
    lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    sources: &[super::delta::DeltaSourceFile],
    delete_side: Option<&super::delta::DeltaScanDeleteSide>,
    target_parallelism: NonZeroUsize,
    max_split_bytes: Option<NonZeroU64>,
) -> Result<PlannedIcebergConnectorRead, String> {
    let mut planned = plan_native_iceberg_read_with_lease(
        lease,
        context.clone(),
        table,
        super::scan_model::IcebergDataFileBinding::ExplicitFiles,
        &[],
        &[],
        Vec::new(),
        target_parallelism,
        max_split_bytes,
    )?;
    let owner = planned.declaration.descriptor().instance_id.clone();
    let incarnation = planned.declaration.incarnation().to_bytes();
    let mut total_payload_bytes = 0usize;
    let mut splits = Vec::with_capacity(sources.len());
    for (index, source) in sources.iter().cloned().enumerate() {
        let data_file = IcebergDataFileInfo {
            path: source.path.clone(),
            size: source.size,
            row_count: None,
            column_stats: None,
            partition_spec_id: source.partition_spec_id,
            partition_key: source.partition_key.clone(),
            first_row_id: source.first_row_id,
            data_sequence_number: source.data_sequence_number,
            ivm_change_op: None,
            included_positions: None,
            delete_files: Vec::new(),
            manifest_path: None,
            partition_values: Vec::new(),
        };
        let estimated_bytes = u64::try_from(data_file.size)
            .map_err(|_| "Iceberg delta source has a negative size".to_string())?;
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V5,
            owner_instance_id: owner.as_str().to_string(),
            incarnation,
            namespace: table.namespace.clone(),
            table: table.table.clone(),
            snapshot_id: table.current_snapshot_id,
            table_uuid: table.table_uuid.clone(),
            schema_id: Some(table.schema_id),
            units: vec![IcebergFrozenScanUnitPayload {
                data_file,
                row_groups: None,
                estimated_bytes: Some(estimated_bytes),
            }],
            projection: Vec::new(),
            limit: None,
            physical_predicates: Vec::new(),
            fact_columns: Vec::new(),
            name_mapping: None,
            delta: Some(IcebergDeltaSplitPayload {
                source,
                delete_side: delete_side.cloned(),
            }),
            distributed_rewrite_position: None,
        };
        push_split_with_budget(
            &mut splits,
            &mut total_payload_bytes,
            owner.clone(),
            format!("delta-{index}"),
            &payload,
            Some(estimated_bytes),
            &context,
        )
        .map_err(|error| error.to_string())?;
    }
    planned.splits = splits;
    Ok(planned)
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
    use crate::connector::iceberg::scan_model::{
        IcebergSchemaDef, IcebergSchemaFieldDef, IcebergTableInfo,
    };

    #[test]
    fn unknown_table_catalog_error_is_not_found() {
        let error = map_iceberg_error("unknown table: analytics.orders".to_string());
        assert_eq!(error.kind(), ConnectorErrorKind::NotFound);
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

    fn local_planning_provider(warehouse: &Path) -> IcebergControlProvider {
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
            registry: Arc::new(RwLock::new(catalog_registry)),
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
            recovery_cleanup_outcomes: Arc::new(Mutex::new(HashMap::new())),
        }
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
        let provider = local_planning_provider(directory.path());
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
        let materialized = materialize_local_scan_units(
            &IcebergReadBinding::default_binding(None).expect("local binding"),
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
        let binding = IcebergReadBinding::default_binding(None).expect("local binding");
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
    fn row_group_cost_distribution_is_deterministic_for_weighted_and_zero_weight_layouts() {
        let weighted = vec![
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 0,
                compressed_bytes: 1,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 1,
                compressed_bytes: 3,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 2,
                compressed_bytes: 6,
                row_count: 1,
            },
        ];
        assert_eq!(
            distribute_unit_cost(101, &weighted).expect("weighted costs"),
            [10, 30, 61]
        );
        assert_eq!(
            distribute_unit_cost(101, &weighted).expect("repeated weighted costs"),
            [10, 30, 61],
            "weighted distribution must be repeatable"
        );

        let zero_weight = vec![
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 0,
                compressed_bytes: 0,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 1,
                compressed_bytes: 0,
                row_count: 1,
            },
            novarocks_fs::ParquetRowGroupLayout {
                ordinal: 2,
                compressed_bytes: 0,
                row_count: 1,
            },
        ];
        assert_eq!(
            distribute_unit_cost(8, &zero_weight).expect("zero-weight costs"),
            [3, 3, 2]
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
    ) -> crate::connector::iceberg::catalog::registry::DataFileWithStats {
        crate::connector::iceberg::catalog::registry::DataFileWithStats {
            path: "file:///tmp/table/data.parquet".to_string(),
            size: 12,
            record_count: Some(4),
            column_stats: Some(HashMap::from([(
                column.to_string(),
                crate::connector::iceberg::scan_model::IcebergColumnStats {
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
            crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
                path: "file:///tmp/table/delete.parquet".to_string(),
                file_format:
                    crate::connector::iceberg::scan_model::IcebergDeleteFileFormat::Parquet,
                file_content:
                    crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Position,
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

    #[test]
    fn provider_statistics_artifact_round_trips_requested_metrics() {
        let data_version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("version");
        let theta_metric = StatisticsMetric::ThetaNdv {
            column: Arc::from("k"),
        };
        let evidence = StatisticsEvidence {
            data_version: data_version.clone(),
            evidence_revision: StatisticsEvidenceRevision::try_new(Bytes::from_static(b"run-v1"))
                .expect("revision"),
            coverage: StatisticsCoverage::Full,
            accuracy: StatisticsAccuracy::Exact,
            interval: None,
            provenance: StatisticsProvenance::VisibleRows,
            metrics: BTreeMap::from([
                (
                    StatisticsMetric::RowCount,
                    StatisticsMetricState::Available(StatisticsMetricValue::U64(3)),
                ),
                (
                    theta_metric.clone(),
                    StatisticsMetricState::Available(StatisticsMetricValue::F64(3.0)),
                ),
            ]),
        };
        let payload = encode_provider_statistics(&evidence).expect("encode artifact");
        let requested = novarocks_spi::connector::StatisticsMetricRequest::try_new(vec![
            StatisticsMetric::RowCount,
            theta_metric.clone(),
            StatisticsMetric::NullCount {
                column: Arc::from("k"),
            },
        ])
        .expect("metric request");

        let decoded = decode_provider_statistics(&payload, &data_version, &requested)
            .expect("decode artifact");
        assert_eq!(
            decoded.get(&StatisticsMetric::RowCount),
            Some(&StatisticsMetricState::Available(
                StatisticsMetricValue::U64(3)
            ))
        );
        assert_eq!(
            decoded.get(&theta_metric),
            Some(&StatisticsMetricState::Available(
                StatisticsMetricValue::F64(3.0)
            ))
        );
        assert!(matches!(
            decoded.get(&StatisticsMetric::NullCount {
                column: Arc::from("k")
            }),
            Some(StatisticsMetricState::Missing(StatisticsMissing {
                kind: StatisticsMissingKind::NotCollected,
                ..
            }))
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
    struct Fixture {
        instance_id: ConnectorInstanceId,
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
            if let Some(seen) = &self.seen_projections {
                seen.lock()
                    .expect("fixture projection lock")
                    .push(request.projection.clone());
            }
            let (physical_predicates, predicate_dispositions) =
                negotiate_static_predicates(&table, &request.static_predicates);
            Ok(ConnectorScan {
                handle: ConnectorScanHandle::try_new(
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
                )?,
                output_schema: Arc::new(Schema::empty()),
                predicate_dispositions,
            })
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
            _: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "planned-files fixture does not implement metadata",
            ))
        }
    }

    let instance_id = ConnectorInstanceId::parse(catalog).expect("fixture instance ID");
    let read = Arc::new(Fixture {
        instance_id: instance_id.clone(),
        files_by_table,
        seen_projections,
    });
    let descriptor = ConnectorInstanceDescriptor {
        provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
            .expect("fixture provider ID"),
        instance_id,
    };
    let incarnation = ConnectorInstanceIncarnation::from_bytes([0; 16]);
    ConnectorControlBinding::try_new(
        descriptor.clone(),
        incarnation,
        read.clone(),
        read,
        Arc::new(IcebergInstanceDistribution {
            descriptor,
            incarnation,
        }),
        None,
    )
    .expect("fixture connector control binding")
}
