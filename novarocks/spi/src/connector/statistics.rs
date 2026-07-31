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

//! FE-only provider-neutral statistics contract.
//! Design: ADR-0022 (docs/adr/ADR-0022-connector-statistics-capability.md)

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorInstanceIncarnation, ConnectorMutationOperationId, ConnectorRequestContext,
    ConnectorTableHandle, ExternalMutationEvidence, ExternalMutationOutcome,
};

/// Maximum size of one provider-owned data-version, evidence-revision, plan,
/// result, or receipt payload. The values may cross a durable FE boundary but
/// are never executable code or BE runtime state.
pub const MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES: usize = 64 * 1024;

/// Maximum number of independently requested metrics in one provider call.
pub const MAX_CONNECTOR_STATISTICS_METRICS: usize = 1024;

/// Opaque provider token that pins a table's data state. It is deliberately
/// distinct from `ConnectorTableMetadata::version`, whose current providers
/// may use it for a schema-level version.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StatisticsDataVersion(Bytes);

impl StatisticsDataVersion {
    pub fn try_new(bytes: Bytes) -> Result<Self, ConnectorError> {
        bounded_payload(bytes, "statistics data version").map(Self)
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl fmt::Debug for StatisticsDataVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_debug(formatter, "StatisticsDataVersion", &self.0)
    }
}

/// Opaque provider token identifying one immutable statistics artifact/evidence
/// revision for a pinned data version.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StatisticsEvidenceRevision(Bytes);

impl StatisticsEvidenceRevision {
    pub fn try_new(bytes: Bytes) -> Result<Self, ConnectorError> {
        bounded_payload(bytes, "statistics evidence revision").map(Self)
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

impl fmt::Debug for StatisticsEvidenceRevision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        redacted_debug(formatter, "StatisticsEvidenceRevision", &self.0)
    }
}

/// Stable, typed metric selection. The field names identify logical table
/// columns; providers must resolve them against the schema pinned by
/// `StatisticsDataVersion`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StatisticsMetric {
    RowCount,
    NullCount { column: Arc<str> },
    Minimum { column: Arc<str> },
    Maximum { column: Arc<str> },
    AverageSize { column: Arc<str> },
    ThetaNdv { column: Arc<str> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsMetricRequest {
    metrics: Vec<StatisticsMetric>,
}

impl StatisticsMetricRequest {
    pub fn try_new(metrics: Vec<StatisticsMetric>) -> Result<Self, ConnectorError> {
        if metrics.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics metric request must not be empty",
            ));
        }
        if metrics.len() > MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "statistics metric request exceeds the metric limit",
            ));
        }
        for metric in &metrics {
            if let Some(column) = metric_column(metric)
                && column.is_empty()
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "statistics metric column must not be empty",
                ));
            }
        }
        Ok(Self { metrics })
    }

    pub fn metrics(&self) -> &[StatisticsMetric] {
        &self.metrics
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsCoverage {
    Full,
    Subset,
    Superset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsAccuracy {
    Exact,
    Approximate,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StatisticsInterval {
    pub lower: f64,
    pub upper: f64,
}

impl StatisticsInterval {
    pub fn try_new(lower: f64, upper: f64) -> Result<Self, ConnectorError> {
        if !lower.is_finite() || !upper.is_finite() || lower > upper {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics interval must be finite and ordered",
            ));
        }
        Ok(Self { lower, upper })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsProvenance {
    ProviderArtifact,
    Manifest,
    VisibleRows,
    Provider(Arc<str>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsMetricValue {
    U64(u64),
    I64(i64),
    F64(f64),
    Bytes(Bytes),
}

impl StatisticsMetricValue {
    pub fn try_bytes(bytes: Bytes) -> Result<Self, ConnectorError> {
        Ok(Self::Bytes(bounded_payload(
            bytes,
            "statistics metric value",
        )?))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsMissingKind {
    NotCollected,
    NotAvailableForVersion,
    UnsupportedMetric,
    IncompleteEvidence,
    CorruptArtifact,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsMissing {
    pub kind: StatisticsMissingKind,
    pub message: Arc<str>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsMetricErrorKind {
    Unavailable,
    PermissionDenied,
    CorruptData,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsMetricError {
    pub kind: StatisticsMetricErrorKind,
    pub message: Arc<str>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StatisticsMetricState {
    Available(StatisticsMetricValue),
    Missing(StatisticsMissing),
    Error(StatisticsMetricError),
}

/// One immutable statistics answer. It has no table client, runtime handle, or
/// executable artifact, so callers may cache it using the explicit version pair.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsEvidence {
    pub data_version: StatisticsDataVersion,
    pub evidence_revision: StatisticsEvidenceRevision,
    pub coverage: StatisticsCoverage,
    pub accuracy: StatisticsAccuracy,
    pub interval: Option<StatisticsInterval>,
    pub provenance: StatisticsProvenance,
    pub metrics: BTreeMap<StatisticsMetric, StatisticsMetricState>,
}

#[derive(Clone)]
pub struct StatisticsReadRequest {
    pub table: ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
    pub metrics: StatisticsMetricRequest,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct StatisticsCollectionRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub table: ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
    pub metrics: StatisticsMetricRequest,
    pub context: ConnectorRequestContext,
}

/// One provider-resolved physical input column for a statistics collection.
///
/// A durable ANALYZE job owns only an opaque table handle and data-version.
/// It therefore cannot ask Core to resolve the table name or schema again when
/// the worker eventually runs.  This compact layout supplies exactly the
/// scan-facing schema needed to compile the already-pinned projection.  It is
/// not catalog metadata: defaults, field metadata and connector credentials
/// are deliberately excluded.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsScanColumn {
    ordinal: usize,
    name: Arc<str>,
    data_type: DataType,
    nullable: bool,
}

impl StatisticsScanColumn {
    pub fn try_new(
        ordinal: usize,
        name: impl Into<Arc<str>>,
        data_type: DataType,
        nullable: bool,
    ) -> Result<Self, ConnectorError> {
        let name = name.into();
        if name.is_empty() || name.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics scan column name is empty or exceeds the payload limit",
            ));
        }
        Ok(Self {
            ordinal,
            name,
            data_type,
            nullable,
        })
    }

    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// Provider-neutral collection preparation result. `table` is the exact
/// already-resolved table handle pinned by `data_version`; Core must compile
/// ordinary distributed work from this handle and must never resolve latest
/// metadata a second time. `scan_columns` is resolved against that same
/// pinned schema.  It is intentionally a compact physical layout rather than
/// a Core-owned catalog lookup: providers own their handle/schema codecs while
/// Core owns only normal connector scan scheduling. The provider payload
/// remains opaque to the FE and Core.
#[derive(Clone)]
pub struct StatisticsCollectionPlan {
    table: ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
    evidence_revision: StatisticsEvidenceRevision,
    pub metrics: StatisticsMetricRequest,
    scan_columns: Vec<StatisticsScanColumn>,
    provider_payload: Bytes,
}

impl StatisticsCollectionPlan {
    pub fn try_new(
        table: ConnectorTableHandle,
        data_version: StatisticsDataVersion,
        evidence_revision: StatisticsEvidenceRevision,
        metrics: StatisticsMetricRequest,
        scan_columns: Vec<StatisticsScanColumn>,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        if scan_columns.len() > MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "statistics collection scan layout exceeds the metric limit",
            ));
        }
        if scan_columns
            .windows(2)
            .any(|pair| pair[0].ordinal() >= pair[1].ordinal())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics collection scan layout ordinals must be sorted and unique",
            ));
        }
        Ok(Self {
            table,
            data_version,
            evidence_revision,
            metrics,
            scan_columns,
            provider_payload: bounded_payload(provider_payload, "statistics collection plan")?,
        })
    }

    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }

    /// Provider-owned revision fixed at collection preparation.  Core carries
    /// it unchanged to the final evidence and must never synthesize a token.
    pub fn evidence_revision(&self) -> &StatisticsEvidenceRevision {
        &self.evidence_revision
    }

    /// Provider-resolved compact schema for the normal connector scan. An
    /// empty layout is valid for a row-count-only collection; the provider's
    /// scan implementation decides how to represent it.
    pub fn scan_columns(&self) -> &[StatisticsScanColumn] {
        &self.scan_columns
    }

    pub fn scan_projection(&self) -> Vec<usize> {
        self.scan_columns
            .iter()
            .map(StatisticsScanColumn::ordinal)
            .collect()
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsCollectionResult {
    pub evidence: StatisticsEvidence,
    provider_payload: Bytes,
}

impl StatisticsCollectionResult {
    pub fn try_new(
        evidence: StatisticsEvidence,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            evidence,
            provider_payload: bounded_payload(provider_payload, "statistics collection result")?,
        })
    }

    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
}

#[derive(Clone)]
pub struct StatisticsPublishRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub table: ConnectorTableHandle,
    pub result: StatisticsCollectionResult,
    pub context: ConnectorRequestContext,
    /// Evidence prepared before the durable caller enters PUBLISHING.  The
    /// provider must use exactly this operation-specific evidence if commit
    /// status becomes uncertain.
    pub evidence: ExternalMutationEvidence,
}

#[derive(Clone)]
pub struct StatisticsPublishPreparationRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub table: ConnectorTableHandle,
    pub result: StatisticsCollectionResult,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct StatisticsReconcileRequest {
    pub evidence: ExternalMutationEvidence,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Eq, PartialEq)]
pub struct StatisticsReceipt {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    operation_id: ConnectorMutationOperationId,
    data_version: StatisticsDataVersion,
    evidence_revision: StatisticsEvidenceRevision,
    provider_payload: Bytes,
}

impl StatisticsReceipt {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        operation_id: ConnectorMutationOperationId,
        data_version: StatisticsDataVersion,
        evidence_revision: StatisticsEvidenceRevision,
        provider_payload: Bytes,
    ) -> Result<Self, ConnectorError> {
        Ok(Self {
            descriptor,
            incarnation,
            operation_id,
            data_version,
            evidence_revision,
            provider_payload: bounded_payload(provider_payload, "statistics receipt")?,
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }
    pub const fn operation_id(&self) -> ConnectorMutationOperationId {
        self.operation_id
    }
    pub fn data_version(&self) -> &StatisticsDataVersion {
        &self.data_version
    }
    pub fn evidence_revision(&self) -> &StatisticsEvidenceRevision {
        &self.evidence_revision
    }
    pub fn provider_payload(&self) -> &Bytes {
        &self.provider_payload
    }
}

impl fmt::Debug for StatisticsReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatisticsReceipt")
            .field("descriptor", &self.descriptor)
            .field("incarnation", &self.incarnation)
            .field("operation_id", &self.operation_id)
            .field("data_version", &self.data_version)
            .field("evidence_revision", &self.evidence_revision)
            .field("provider_payload_len", &self.provider_payload.len())
            .finish()
    }
}

/// Read-only half of the FE-only statistics capability.
pub trait StatisticsReader: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ConnectorInstanceIncarnation;
    fn read_statistics(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError>;
}

/// Optional collection and publication half of a connector statistics
/// capability. Providers without this trait remain valid readers.
pub trait StatisticsCollection: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ConnectorInstanceIncarnation;
    fn prepare_collection(
        &self,
        request: StatisticsCollectionRequest,
    ) -> Result<StatisticsCollectionPlan, ConnectorError>;
    /// Computes deterministic reconciliation evidence before the caller makes
    /// the PUBLISHING state durable. This method must not perform an external
    /// catalog write.
    fn prepare_publish(
        &self,
        request: StatisticsPublishPreparationRequest,
    ) -> Result<ExternalMutationEvidence, ConnectorError>;
    fn publish_statistics(
        &self,
        request: StatisticsPublishRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError>;
    fn reconcile_statistics(
        &self,
        request: StatisticsReconcileRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError>;
}

/// Aggregate FE-only statistics capability. Its optional collection half is
/// intentionally not part of an execution binding.
pub trait ConnectorStatistics: StatisticsReader {
    fn collection(&self) -> Option<&dyn StatisticsCollection> {
        None
    }
}

/// Narrow consumer port. Consumers can hold one generation-fenced lease but
/// cannot inspect, register, or retire control generations.
pub trait ConnectorStatisticsResolver: Send + Sync {
    fn acquire_current_statistics(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<ConnectorStatisticsLease, ConnectorError>;
}

#[derive(Clone)]
pub struct ConnectorStatisticsLease {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    statistics: Arc<dyn ConnectorStatistics>,
    _release: Arc<StatisticsLeaseRelease>,
}

struct StatisticsLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorStatisticsLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        statistics: Arc<dyn ConnectorStatistics>,
        release: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<Self, ConnectorError> {
        validate_statistics_owner(&descriptor, incarnation, statistics.as_ref())?;
        Ok(Self {
            descriptor,
            incarnation,
            statistics,
            _release: Arc::new(StatisticsLeaseRelease {
                release: Mutex::new(Some(Box::new(release))),
            }),
        })
    }

    pub fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }
    pub fn supports_collection(&self) -> bool {
        self.statistics.collection().is_some()
    }
    pub fn read(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        self.validate_table(&request.table)?;
        let evidence = self.statistics.read_statistics(request)?;
        if evidence.data_version.as_bytes().is_empty()
            || evidence.evidence_revision.as_bytes().is_empty()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector statistics reader returned an empty version token",
            ));
        }
        Ok(evidence)
    }
    pub fn prepare_collection(
        &self,
        request: StatisticsCollectionRequest,
    ) -> Result<StatisticsCollectionPlan, ConnectorError> {
        self.validate_table(&request.table)?;
        let expected_table = request.table.clone();
        let expected_data_version = request.data_version.clone();
        let expected_metrics = request.metrics.clone();
        let collection = self.statistics.collection().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector statistics capability does not support collection",
            )
        })?;
        let plan = collection.prepare_collection(request)?;
        if plan.table() != &expected_table
            || plan.data_version != expected_data_version
            || plan.evidence_revision.as_bytes().is_empty()
            || plan.metrics != expected_metrics
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector statistics collection plan does not preserve its resolved table pin",
            ));
        }
        Ok(plan)
    }
    pub fn publish(
        &self,
        request: StatisticsPublishRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        self.validate_table(&request.table)?;
        let operation_id = request.operation_id;
        self.validate_evidence(&request.evidence)?;
        if request.evidence.operation_id() != operation_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics publication evidence operation ID does not match request",
            ));
        }
        let collection = self.collection()?;
        let outcome = collection.publish_statistics(request)?;
        self.validate_outcome(operation_id, &outcome)?;
        Ok(outcome)
    }
    pub fn prepare_publish(
        &self,
        request: StatisticsPublishPreparationRequest,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        self.validate_table(&request.table)?;
        let operation_id = request.operation_id;
        let evidence = self.collection()?.prepare_publish(request)?;
        self.validate_evidence(&evidence)?;
        if evidence.operation_id() != operation_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector statistics publication evidence changed the operation ID",
            ));
        }
        Ok(evidence)
    }
    pub fn reconcile(
        &self,
        request: StatisticsReconcileRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        self.validate_evidence(&request.evidence)?;
        let operation_id = request.evidence.operation_id();
        let outcome = self.collection()?.reconcile_statistics(request)?;
        self.validate_outcome(operation_id, &outcome)?;
        Ok(outcome)
    }

    fn collection(&self) -> Result<&dyn StatisticsCollection, ConnectorError> {
        self.statistics.collection().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector statistics capability does not support collection",
            )
        })
    }

    fn validate_table(&self, table: &ConnectorTableHandle) -> Result<(), ConnectorError> {
        if table.owner() != &self.descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics request does not match its lease instance",
            ));
        }
        Ok(())
    }

    fn validate_evidence(&self, evidence: &ExternalMutationEvidence) -> Result<(), ConnectorError> {
        if evidence.descriptor() != &self.descriptor || evidence.incarnation() != self.incarnation {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics reconcile evidence does not match its lease generation",
            ));
        }
        Ok(())
    }

    fn validate_outcome(
        &self,
        operation_id: ConnectorMutationOperationId,
        outcome: &ExternalMutationOutcome<StatisticsReceipt>,
    ) -> Result<(), ConnectorError> {
        match outcome {
            ExternalMutationOutcome::KnownCommitted { receipt, .. } => {
                if receipt.descriptor() != &self.descriptor
                    || receipt.incarnation() != self.incarnation
                    || receipt.operation_id() != operation_id
                {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "statistics receipt does not match its lease or operation",
                    ));
                }
            }
            ExternalMutationOutcome::CommitUnknown { evidence, .. } => {
                self.validate_evidence(evidence)?;
                if evidence.operation_id() != operation_id {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "statistics commit evidence does not match its operation",
                    ));
                }
            }
            ExternalMutationOutcome::KnownUncommitted { .. } => {}
        }
        Ok(())
    }
}

impl Drop for StatisticsLeaseRelease {
    fn drop(&mut self) {
        let Ok(mut release) = self.release.lock() else {
            return;
        };
        if let Some(release) = release.take() {
            release();
        }
    }
}

pub(crate) fn validate_statistics_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    statistics: &dyn ConnectorStatistics,
) -> Result<(), ConnectorError> {
    if statistics.descriptor() != descriptor || statistics.incarnation() != incarnation {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector statistics capability owner does not match its control binding generation",
        ));
    }
    if let Some(collection) = statistics.collection()
        && (collection.descriptor() != descriptor || collection.incarnation() != incarnation)
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "connector statistics collection owner does not match its control binding generation",
        ));
    }
    Ok(())
}

fn bounded_payload(bytes: Bytes, label: &str) -> Result<Bytes, ConnectorError> {
    if bytes.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("{label} exceeds 64 KiB"),
        ));
    }
    Ok(bytes)
}

fn metric_column(metric: &StatisticsMetric) -> Option<&str> {
    match metric {
        StatisticsMetric::RowCount => None,
        StatisticsMetric::NullCount { column }
        | StatisticsMetric::Minimum { column }
        | StatisticsMetric::Maximum { column }
        | StatisticsMetric::AverageSize { column }
        | StatisticsMetric::ThetaNdv { column } => Some(column),
    }
}

fn redacted_debug(
    formatter: &mut fmt::Formatter<'_>,
    name: &'static str,
    bytes: &Bytes,
) -> fmt::Result {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    formatter
        .debug_struct(name)
        .field("len", &bytes.len())
        .field("digest", &digest)
        .finish()
}
