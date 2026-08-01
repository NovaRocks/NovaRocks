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
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Instant;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use bytes::Bytes;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use novarocks_catalog::schema::SqlType;
use novarocks_fs::{
    FileIdentity, FileIoRuntime, FileReadContext, FileTaskSpawner, FsAccessHandle,
    FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner,
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
    ConnectorNamespaceRequest, ConnectorOpenReaderRequest, ConnectorPartitionTransform,
    ConnectorPredicateDisposition, ConnectorPredicateDispositionKind, ConnectorProviderId,
    ConnectorReadExecution, ConnectorReadSelector, ConnectorRefreshPublicationGuard, ConnectorScan,
    ConnectorScanHandle, ConnectorScanPlanning, ConnectorSplit, ConnectorSplitPlanningMetrics,
    ConnectorSplitPlanningRequest, ConnectorSplitPlanningResult, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateDataType, ConnectorStaticPredicateKind,
    ConnectorStaticPredicateLiteral, ConnectorStatistics, ConnectorTableHandle,
    ConnectorTableMetadata, ConnectorTableRequest, ConnectorTableResolution, CreateOrReplacePolicy,
    CreatePolicy, DropPolicy, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, StatisticsAccuracy,
    StatisticsCollection, StatisticsCollectionPlan, StatisticsCollectionRequest,
    StatisticsCoverage, StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision,
    StatisticsMetric, StatisticsMetricState, StatisticsMetricValue, StatisticsMissing,
    StatisticsMissingKind, StatisticsProvenance, StatisticsPublishPreparationRequest,
    StatisticsPublishRequest, StatisticsReadRequest, StatisticsReader, StatisticsReceipt,
    StatisticsReconcileRequest, StatisticsScanColumn, normalize_predicate_dispositions,
    validate_static_predicates,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::catalog::IcebergCatalogEntry;
use super::catalog::registry::{
    IcebergCatalogRegistry, create_namespace, create_table, drop_namespace, drop_table,
    extract_data_files_with_stats_at, list_tables, load_table, namespace_exists,
};
use super::catalog::views;
use super::data_mutation::IcebergDataMutationAdapter;
use super::metadata_maintenance::IcebergMetadataMaintenanceAdapter;
use super::reader::IcebergBatchReader;
use super::scan_model::{
    IcebergDataFileInfo, IcebergPhysicalPredicate, IcebergPhysicalPredicateDomain,
    IcebergPhysicalPredicateOp, IcebergPhysicalPredicateValue,
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

/// BE-only instance installed through the binding control plane.  Metadata and
/// planning are deliberately unsupported; once the provider reader is wired,
/// `open_reader` will consume the opaque, fully planned Iceberg split.
struct IcebergReadOnlyConnectorInstance {
    key: ConnectorExecutionBindingKey,
    binding: IcebergReadBinding,
}

impl IcebergReadOnlyConnectorInstance {
    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        if request.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "connector request was cancelled",
            ));
        }
        if Instant::now() >= request.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "connector request deadline elapsed",
            ));
        }
        ensure_owner(split.owner(), &self.key.instance_id)?;
        let mut payload: SplitPayload = decode_payload(split.payload(), "Iceberg split")?;
        validate_and_normalize_split_payload(&mut payload)?;
        if payload.owner_instance_id != self.key.instance_id.as_str()
            || payload.incarnation != self.key.incarnation.to_bytes()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg split does not belong to this installed instance incarnation",
            ));
        }
        if let Some(delta) = payload.delta {
            return super::delta_reader::IcebergDeltaBatchReader::try_new(
                delta.source,
                delta.delete_side,
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
            std::iter::once(payload.data_file.path.as_str()).chain(
                payload
                    .data_file
                    .delete_files
                    .iter()
                    .map(|delete| delete.path.as_str()),
            ),
        )?;
        IcebergBatchReader::try_new_with_name_mapping(
            &payload.data_file,
            &payload.physical_predicates,
            payload.name_mapping.as_deref(),
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

    fn open_reader(
        &self,
        split: &ConnectorSplit,
        request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        self.open_reader(split, request)
    }
}

#[derive(Clone)]
pub(crate) struct IcebergControlProvider {
    descriptor: ConnectorInstanceDescriptor,
    instance_id: ConnectorInstanceId,
    incarnation: ConnectorInstanceIncarnation,
    registry: Arc<RwLock<IcebergCatalogRegistry>>,
    snapshot_memberships: Arc<SnapshotMembershipCache>,
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
        let provider = Arc::new(Self {
            descriptor: descriptor.clone(),
            instance_id,
            incarnation,
            registry: Arc::clone(&registry),
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
        });
        let write_key = ConnectorExecutionBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let services = registry
            .read()
            .map_err(|error| internal(format!("Iceberg catalog registry read lock: {error}")))?
            .write_services();
        let write = Arc::new(IcebergWriteControlAdapter::new(
            write_key.clone(),
            Arc::new(RegisteredIcebergWriteControlBackend::new(services)),
        )?);
        let data_mutation = Arc::new(IcebergDataMutationAdapter::new_registered(
            write_key.clone(),
            descriptor.instance_id.clone(),
            Arc::clone(&registry),
        )?);
        let metadata_maintenance = Arc::new(IcebergMetadataMaintenanceAdapter::new_registered(
            write_key,
            descriptor.instance_id.clone(),
            registry,
        )?);
        ConnectorControlBinding::try_new_with_all_capabilities_and_metadata_maintenance(
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
            Some(write),
            Some(provider),
        )
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

fn lower_column(
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

fn lower_partition(
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
        let table = self.table_payload(table)?;
        let entry = self.entry(self.instance_id.as_str())?;
        let output_schema =
            self.schema_for(&entry, &table.namespace, &table.table, &request.projection)?;
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
        let payload = ScanPayload {
            table,
            snapshot_id,
            table_uuid,
            projection: request.projection,
            limit: request.limit,
            physical_predicates,
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
        let mut splits = Vec::new();
        let mut total_payload_bytes = 0usize;
        let name_mapping = split_name_mapping(&scan.table)?;
        for (index, file) in files.into_iter().enumerate() {
            if let Some(remaining_rows) = remaining.as_mut() {
                if *remaining_rows == 0 {
                    break;
                }
                if let Some(row_count) = file.row_count.and_then(|count| u64::try_from(count).ok())
                {
                    *remaining_rows = remaining_rows.saturating_sub(row_count);
                }
            }
            let estimated_bytes = u64::try_from(file.size).ok();
            let payload = SplitPayload {
                version: ICEBERG_SPLIT_V3,
                owner_instance_id: self.instance_id.as_str().to_string(),
                incarnation: self.incarnation.to_bytes(),
                namespace: scan.table.namespace.clone(),
                table: scan.table.table.clone(),
                snapshot_id: scan.snapshot_id,
                table_uuid: scan.table_uuid.clone(),
                schema_id: scan.table.table_info.as_ref().map(|table| table.schema_id),
                data_file: file,
                projection: scan.projection.clone(),
                limit: scan.limit,
                physical_predicates: scan.physical_predicates.clone(),
                name_mapping: name_mapping.clone(),
                delta: None,
            };
            push_split_with_budget(
                &mut splits,
                &mut total_payload_bytes,
                self.instance_id.clone(),
                format!(
                    "{}-{index}",
                    scan.snapshot_id
                        .map(|snapshot_id| snapshot_id.to_string())
                        .unwrap_or_else(|| "explicit".to_string())
                ),
                &payload,
                estimated_bytes,
                &request.context,
            )?;
        }
        ConnectorSplitPlanningResult::try_new(
            splits,
            ConnectorSplitPlanningMetrics {
                candidate_units_considered,
                candidate_units_pruned: u64::try_from(pruning_counters.files_pruned)
                    .unwrap_or(u64::MAX)
                    .min(candidate_units_considered),
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
    use ConnectorStaticPredicateDataType::{Boolean, Date32, Int32, Int64};

    let value = |literal: &ConnectorStaticPredicateLiteral| match literal {
        ConnectorStaticPredicateLiteral::Boolean(value)
            if predicate.column.data_type == Boolean =>
        {
            Some(IcebergPhysicalPredicateValue::Boolean(*value))
        }
        ConnectorStaticPredicateLiteral::Int32(value) if predicate.column.data_type == Int32 => {
            Some(IcebergPhysicalPredicateValue::Int32(*value))
        }
        ConnectorStaticPredicateLiteral::Int64(value) if predicate.column.data_type == Int64 => {
            Some(IcebergPhysicalPredicateValue::Int64(*value))
        }
        ConnectorStaticPredicateLiteral::Date32(value) if predicate.column.data_type == Date32 => {
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
}

#[derive(Deserialize, Serialize)]
struct SplitPayload {
    #[serde(default = "default_iceberg_split_version")]
    version: u16,
    #[serde(default)]
    owner_instance_id: String,
    #[serde(default)]
    incarnation: [u8; 16],
    namespace: String,
    table: String,
    snapshot_id: Option<i64>,
    #[serde(default)]
    table_uuid: Option<String>,
    #[serde(default)]
    schema_id: Option<i32>,
    data_file: IcebergDataFileInfo,
    projection: Vec<usize>,
    limit: Option<u64>,
    #[serde(default)]
    physical_predicates: Vec<IcebergPhysicalPredicate>,
    #[serde(default)]
    name_mapping: Option<String>,
    #[serde(default)]
    delta: Option<IcebergDeltaSplitPayload>,
}

const ICEBERG_SPLIT_V1: u16 = 1;
const ICEBERG_SPLIT_V2: u16 = 2;
const ICEBERG_SPLIT_V3: u16 = 3;

fn default_iceberg_split_version() -> u16 {
    ICEBERG_SPLIT_V1
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

fn validate_and_normalize_split_payload(payload: &mut SplitPayload) -> Result<(), ConnectorError> {
    match payload.version {
        ICEBERG_SPLIT_V1 => {
            payload.physical_predicates.clear();
            payload.name_mapping = None;
        }
        ICEBERG_SPLIT_V2 => payload.name_mapping = None,
        ICEBERG_SPLIT_V3 => validate_split_name_mapping(payload.name_mapping.as_deref())?,
        version => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!("unsupported Iceberg split version {version}"),
            ));
        }
    }
    Ok(())
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
    )?;
    planned
        .splits
        .iter()
        .map(|split| {
            decode_payload::<SplitPayload>(split.payload(), "split")
                .map(|payload| payload.data_file)
                .map_err(|error| error.to_string())
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn planned_split_data_file_for_test(
    split: &ConnectorSplit,
) -> Result<IcebergDataFileInfo, String> {
    decode_payload::<SplitPayload>(split.payload(), "test Iceberg split")
        .map(|payload| payload.data_file)
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
) -> Result<PlannedIcebergConnectorRead, String> {
    plan_native_iceberg_read_with_file_override(
        controls,
        context,
        table,
        binding,
        Some(explicit_files),
        projection,
        static_predicates,
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
) -> Result<PlannedIcebergConnectorRead, String> {
    plan_native_iceberg_read_with_bound_lease(
        lease,
        context,
        table,
        binding,
        Some(explicit_files),
        projection,
        static_predicates,
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
) -> Result<PlannedIcebergConnectorRead, String> {
    use std::num::NonZeroUsize;

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
                target_parallelism: NonZeroUsize::new(1).expect("parallelism is nonzero"),
                max_split_bytes: None,
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
) -> Result<PlannedIcebergConnectorRead, String> {
    let instance_id =
        ConnectorInstanceId::parse(&table.catalog).map_err(|error| error.to_string())?;
    let lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    plan_native_iceberg_delta_read_with_lease(lease, context, table, sources, delete_side)
}

/// Equivalent to [`plan_native_iceberg_delta_read`] but uses the exact
/// metadata lease already retained by the query binding store.
pub(crate) fn plan_native_iceberg_delta_read_with_lease(
    lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &super::scan_model::IcebergTableInfo,
    sources: &[super::delta::DeltaSourceFile],
    delete_side: Option<&super::delta::DeltaScanDeleteSide>,
) -> Result<PlannedIcebergConnectorRead, String> {
    let mut planned = plan_native_iceberg_read_with_lease(
        lease,
        context.clone(),
        table,
        super::scan_model::IcebergDataFileBinding::ExplicitFiles,
        &[],
        &[],
        Vec::new(),
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
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V1,
            owner_instance_id: owner.as_str().to_string(),
            incarnation,
            namespace: table.namespace.clone(),
            table: table.table.clone(),
            snapshot_id: table.current_snapshot_id,
            table_uuid: table.table_uuid.clone(),
            schema_id: Some(table.schema_id),
            data_file,
            projection: Vec::new(),
            limit: None,
            physical_predicates: Vec::new(),
            name_mapping: None,
            delta: Some(IcebergDeltaSplitPayload {
                source,
                delete_side: delete_side.cloned(),
            }),
        };
        push_split_with_budget(
            &mut splits,
            &mut total_payload_bytes,
            owner.clone(),
            format!("delta-{index}"),
            &payload,
            u64::try_from(payload.data_file.size).ok(),
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
        IcebergControlProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(PROVIDER_ID).expect("provider ID"),
                instance_id: instance_id.clone(),
            },
            instance_id,
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
            registry: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
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
        };
        let supported = ConnectorStaticPredicate {
            id: novarocks_spi::connector::ConnectorStaticPredicateId(3),
            column: novarocks_spi::connector::ConnectorStaticPredicateColumn {
                field_ordinal: 0,
                data_type: ConnectorStaticPredicateDataType::Int32,
                nullable: false,
            },
            kind: ConnectorStaticPredicateKind::Comparison {
                op: ConnectorStaticComparisonOp::Ge,
                literal: ConnectorStaticPredicateLiteral::Int32(10),
            },
        };
        let unsupported = ConnectorStaticPredicate {
            id: novarocks_spi::connector::ConnectorStaticPredicateId(4),
            column: supported.column.clone(),
            kind: ConnectorStaticPredicateKind::Comparison {
                op: ConnectorStaticComparisonOp::Ne,
                literal: ConnectorStaticPredicateLiteral::Int32(11),
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
        };
        let payload = SplitPayload {
            version: ICEBERG_SPLIT_V1,
            owner_instance_id: "ice".to_string(),
            incarnation: [0; 16],
            namespace: table.namespace,
            table: table.table,
            snapshot_id: Some(7),
            table_uuid: Some("table-uuid".to_string()),
            schema_id: Some(1),
            data_file: IcebergDataFileInfo::for_test(
                "s3://warehouse/db/orders/data-1.parquet",
                1024,
                10,
            ),
            projection: vec![0],
            limit: None,
            physical_predicates: Vec::new(),
            name_mapping: None,
            delta: None,
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
    fn split_v3_preserves_only_canonical_name_mapping() {
        let mut payload = SplitPayload {
            version: ICEBERG_SPLIT_V3,
            owner_instance_id: "ice".to_string(),
            incarnation: [0; 16],
            namespace: "db".to_string(),
            table: "orders".to_string(),
            snapshot_id: Some(7),
            table_uuid: Some("table-uuid".to_string()),
            schema_id: Some(1),
            data_file: IcebergDataFileInfo::for_test(
                "s3://warehouse/db/orders/data-1.parquet",
                1024,
                10,
            ),
            projection: vec![0],
            limit: None,
            physical_predicates: Vec::new(),
            name_mapping: Some(r#"[{"field-id":1,"names":["legacy_id"]}]"#.to_string()),
            delta: None,
        };
        validate_and_normalize_split_payload(&mut payload).expect("canonical V3 mapping");
        assert!(payload.name_mapping.is_some());

        payload.name_mapping = Some(r#"[{"names":["legacy_id"],"field-id":1}]"#.to_string());
        let error = validate_and_normalize_split_payload(&mut payload)
            .expect_err("non-canonical V3 mapping must fail");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn legacy_split_versions_cannot_carry_name_mapping() {
        for version in [ICEBERG_SPLIT_V1, ICEBERG_SPLIT_V2] {
            let mut payload = SplitPayload {
                version,
                owner_instance_id: "ice".to_string(),
                incarnation: [0; 16],
                namespace: "db".to_string(),
                table: "orders".to_string(),
                snapshot_id: Some(7),
                table_uuid: Some("table-uuid".to_string()),
                schema_id: Some(1),
                data_file: IcebergDataFileInfo::for_test(
                    "s3://warehouse/db/orders/data-1.parquet",
                    1024,
                    10,
                ),
                projection: vec![0],
                limit: None,
                physical_predicates: vec![IcebergPhysicalPredicate {
                    field_id: 1,
                    column: "id".to_string(),
                    domain: IcebergPhysicalPredicateDomain::Range {
                        op: IcebergPhysicalPredicateOp::Ge,
                        value: IcebergPhysicalPredicateValue::Int32(0),
                    },
                }],
                name_mapping: Some(r#"[{"field-id":1,"names":["legacy_id"]}]"#.to_string()),
                delta: None,
            };
            validate_and_normalize_split_payload(&mut payload).expect("legacy split");
            assert!(payload.name_mapping.is_none());
            if version == ICEBERG_SPLIT_V1 {
                assert!(payload.physical_predicates.is_empty());
            }
        }
    }

    #[test]
    fn aggregate_budget_rejects_candidate_before_split_is_pushed() {
        let owner = ConnectorInstanceId::parse("ice").expect("owner");
        let payload = |suffix: &str| SplitPayload {
            version: ICEBERG_SPLIT_V1,
            owner_instance_id: "ice".to_string(),
            incarnation: [0; 16],
            namespace: "db".to_string(),
            table: "orders".to_string(),
            snapshot_id: Some(7),
            table_uuid: Some("table-uuid".to_string()),
            schema_id: Some(1),
            data_file: IcebergDataFileInfo::for_test(
                &format!(
                    "s3://warehouse/db/orders/{}-{suffix}.parquet",
                    "x".repeat(512)
                ),
                1024,
                10,
            ),
            projection: vec![0],
            limit: None,
            physical_predicates: Vec::new(),
            name_mapping: None,
            delta: None,
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

        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
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
                        "s3://warehouse/db/orders/{}-{suffix}.parquet",
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
                version: ICEBERG_SPLIT_V1,
                owner_instance_id: instance_id.as_str().to_string(),
                incarnation: [0; 16],
                namespace: "db".to_string(),
                table: "orders".to_string(),
                snapshot_id: None,
                table_uuid: None,
                schema_id: None,
                data_file,
                projection: vec![0],
                limit: None,
                physical_predicates: Vec::new(),
                name_mapping: None,
                delta: None,
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
                    },
                    snapshot_id: None,
                    table_uuid: None,
                    projection: vec![0],
                    limit: None,
                    physical_predicates: Vec::new(),
                },
                "scan handle",
                novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            )
            .expect("scan payload"),
        )
        .expect("scan handle");
        let provider = IcebergControlProvider {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse(PROVIDER_ID).expect("provider"),
                instance_id: instance_id.clone(),
            },
            instance_id,
            incarnation: ConnectorInstanceIncarnation::from_bytes([0; 16]),
            registry: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            snapshot_memberships: Arc::new(SnapshotMembershipCache::new(
                MAX_CACHED_SNAPSHOT_MEMBERSHIPS,
            )),
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

        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
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
                                version: ICEBERG_SPLIT_V2,
                                owner_instance_id: self.instance_id.as_str().to_string(),
                                incarnation: [0; 16],
                                namespace: scan.table.namespace.clone(),
                                table: scan.table.table.clone(),
                                snapshot_id: None,
                                table_uuid: None,
                                schema_id: None,
                                data_file,
                                projection: scan.projection.clone(),
                                limit: scan.limit,
                                physical_predicates: scan.physical_predicates.clone(),
                                name_mapping: None,
                                delta: None,
                            },
                            "fixture split",
                            request.context.max_handle_payload_bytes(),
                        )?,
                        estimated_bytes,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            ConnectorSplitPlanningResult::try_new(
                splits,
                ConnectorSplitPlanningMetrics {
                    candidate_units_considered,
                    candidate_units_pruned: u64::try_from(pruning_counters.files_pruned)
                        .unwrap_or(u64::MAX)
                        .min(candidate_units_considered),
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
