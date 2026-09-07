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
//! Design: ADR-0136 (docs/adr/ADR-0136-ordinary-aggregate-statistics-dataflow.md)
//! Design: ADR-0080 (docs/adr/ADR-0080-statistics-evidence-four-dimension-model.md)

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorTableHandle,
    ExternalMutationOutcome, ProviderBindingEpoch,
};

/// Maximum size of one provider-owned data-version, evidence-revision, plan,
/// result, or receipt payload. The values may cross a durable FE boundary but
/// are never executable code or BE runtime state.
pub const MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES: usize = 64 * 1024;

/// Maximum number of columns addressed by one statistics operation.
pub const MAX_CONNECTOR_STATISTICS_COLUMNS: usize = 1024;

/// Maximum number of independently requested column metrics in one provider
/// read. Each bounded column can request every currently defined column metric;
/// a request may additionally contain the single table-level
/// [`StatisticsMetric::RowCount`] metric.
pub const MAX_CONNECTOR_STATISTICS_METRICS: usize = MAX_CONNECTOR_STATISTICS_COLUMNS * 5;

/// Maximum number of provider artifacts produced by one statistics session.
pub const MAX_CONNECTOR_STATISTICS_ARTIFACTS: usize = 4096;

/// One artifact body must fit an ordinary native result packet.
pub const MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Output budget for one Arrow batch carrying statistics artifact rows.
///
/// This must remain larger than one maximum-sized artifact plus its bounded
/// identity, properties, Arrow offsets, and IPC framing. Otherwise a valid
/// artifact could never make progress: Unpivot can split between rows, but it
/// cannot split one artifact body across batches.
pub const MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// Maximum retained artifact body bytes accumulated by one FE result decoder.
///
/// The current 1,024-column ANALYZE ceiling can legitimately produce 1,024
/// saturated default-lg-k Theta compact sketches (65,560 bytes each), which is
/// slightly larger than 64 MiB. Keep a finite headroom while ensuring the
/// declared column limit is actually executable.
pub const MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES: usize = 128 * 1024 * 1024;

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
        let column_metrics = metrics.iter().filter_map(metric_column);
        let column_metric_count = column_metrics.clone().count();
        if column_metric_count > MAX_CONNECTOR_STATISTICS_METRICS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "statistics metric request exceeds the column metric limit",
            ));
        }
        if metrics
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != metrics.len()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics metric request contains a duplicate metric",
            ));
        }
        if column_metrics
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            > MAX_CONNECTOR_STATISTICS_COLUMNS
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "statistics metric request exceeds the column limit",
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

/// Column selection supplied to a provider-owned collection session.
///
/// Default selection silently omits unsupported columns. Explicit selection
/// must reject an unsupported column before distributed work or object writes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsColumnSelection {
    Default,
    Explicit(Vec<Arc<str>>),
}

impl StatisticsColumnSelection {
    pub fn explicit(columns: Vec<Arc<str>>) -> Result<Self, ConnectorError> {
        if columns.len() > MAX_CONNECTOR_STATISTICS_COLUMNS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "statistics column selection exceeds the column limit",
            ));
        }
        let mut normalized = std::collections::BTreeSet::new();
        for column in &columns {
            if column.is_empty() || column.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "statistics column name is empty or exceeds the payload limit",
                ));
            }
            if !normalized.insert(column.to_ascii_lowercase()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "statistics column selection contains a duplicate column",
                ));
            }
        }
        Ok(Self::Explicit(columns))
    }
}

/// Provider-neutral identity of one artifact expected from ordinary execution.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StatisticsArtifactIdentity {
    input_fields: Vec<i32>,
    blob_type: Arc<str>,
}

impl StatisticsArtifactIdentity {
    pub fn try_new(
        input_fields: Vec<i32>,
        blob_type: impl Into<Arc<str>>,
    ) -> Result<Self, ConnectorError> {
        let blob_type = blob_type.into();
        if input_fields.is_empty()
            || input_fields.len() > MAX_CONNECTOR_STATISTICS_COLUMNS
            || input_fields.iter().any(|field_id| *field_id <= 0)
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics artifact input fields must be non-empty positive field IDs",
            ));
        }
        if input_fields
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != input_fields.len()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics artifact input fields must not repeat a field ID",
            ));
        }
        if blob_type.is_empty() || blob_type.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics artifact blob type is empty or exceeds the payload limit",
            ));
        }
        Ok(Self {
            input_fields,
            blob_type,
        })
    }

    pub fn input_fields(&self) -> &[i32] {
        &self.input_fields
    }

    pub fn blob_type(&self) -> &str {
        &self.blob_type
    }
}

/// Generic long-form artifact row produced after an ordinary aggregate and
/// Unpivot. This carrier contains no provider session or catalog authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsArtifactDraft {
    identity: StatisticsArtifactIdentity,
    body: Bytes,
    properties: BTreeMap<String, String>,
}

impl StatisticsArtifactDraft {
    pub fn try_new(
        input_fields: Vec<i32>,
        blob_type: impl Into<Arc<str>>,
        body: Bytes,
        properties: BTreeMap<String, String>,
    ) -> Result<Self, ConnectorError> {
        if body.len() > MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "statistics artifact body exceeds the packet limit",
            ));
        }
        let property_bytes = properties.iter().try_fold(0usize, |total, (key, value)| {
            total
                .checked_add(key.len())
                .and_then(|value_total| value_total.checked_add(value.len()))
        });
        if property_bytes.is_none_or(|bytes| bytes > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "statistics artifact properties exceed the payload limit",
            ));
        }
        if properties.keys().any(|key| key.is_empty()) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics artifact property name must not be empty",
            ));
        }
        Ok(Self {
            identity: StatisticsArtifactIdentity::try_new(input_fields, blob_type)?,
            body,
            properties,
        })
    }

    pub fn identity(&self) -> &StatisticsArtifactIdentity {
        &self.identity
    }

    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
    }

    pub fn into_parts(self) -> (StatisticsArtifactIdentity, Bytes, BTreeMap<String, String>) {
        (self.identity, self.body, self.properties)
    }
}

/// One ordinary global aggregate selected by the provider for a pinned field.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsRequiredAggregation {
    input: StatisticsScanColumn,
    function_name: Arc<str>,
    artifact: StatisticsArtifactIdentity,
}

impl StatisticsRequiredAggregation {
    pub fn try_new(
        input: StatisticsScanColumn,
        function_name: impl Into<Arc<str>>,
        artifact: StatisticsArtifactIdentity,
    ) -> Result<Self, ConnectorError> {
        let function_name = function_name.into();
        if function_name.is_empty() || function_name.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics aggregate function name is empty or exceeds the payload limit",
            ));
        }
        Ok(Self {
            input,
            function_name,
            artifact,
        })
    }

    pub fn input(&self) -> &StatisticsScanColumn {
        &self.input
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn artifact(&self) -> &StatisticsArtifactIdentity {
        &self.artifact
    }
}

#[derive(Clone)]
pub struct StatisticsCollectionStartRequest {
    pub operation_id: ConnectorMutationOperationId,
    pub table: ConnectorTableHandle,
    pub data_version: StatisticsDataVersion,
    pub selection: StatisticsColumnSelection,
    pub context: ConnectorRequestContext,
}

/// Provider planning facts separated from the FE-local publication authority.
/// The type is intentionally not cloneable or serializable.
pub struct StatisticsCollectionStart {
    table: ConnectorTableHandle,
    data_version: StatisticsDataVersion,
    read_version_ordinal: Option<i64>,
    required_aggregations: Vec<StatisticsRequiredAggregation>,
    session: Box<dyn StatisticsCollectionSession>,
}

impl StatisticsCollectionStart {
    pub fn try_new(
        table: ConnectorTableHandle,
        data_version: StatisticsDataVersion,
        read_version_ordinal: Option<i64>,
        required_aggregations: Vec<StatisticsRequiredAggregation>,
        session: Box<dyn StatisticsCollectionSession>,
    ) -> Result<Self, ConnectorError> {
        if required_aggregations.len() > MAX_CONNECTOR_STATISTICS_ARTIFACTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "statistics aggregation requirement count exceeds the metric limit",
            ));
        }
        let identities = required_aggregations
            .iter()
            .map(|requirement| requirement.artifact().clone())
            .collect::<std::collections::BTreeSet<_>>();
        if identities.len() != required_aggregations.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics aggregation requirements contain duplicate artifact identities",
            ));
        }
        if !required_aggregations.is_empty() && read_version_ordinal.is_none() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics aggregation requirements need an exact read version ordinal",
            ));
        }
        let planned = required_aggregations
            .iter()
            .map(|requirement| requirement.artifact())
            .collect::<Vec<_>>();
        let expected = session.expectations().iter().collect::<Vec<_>>();
        if planned != expected {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "statistics aggregation requirements do not exactly match the session expectations",
            ));
        }
        Ok(Self {
            table,
            data_version,
            read_version_ordinal,
            required_aggregations,
            session,
        })
    }

    pub fn table(&self) -> &ConnectorTableHandle {
        &self.table
    }

    pub fn data_version(&self) -> &StatisticsDataVersion {
        &self.data_version
    }

    pub const fn read_version_ordinal(&self) -> Option<i64> {
        self.read_version_ordinal
    }

    pub fn required_aggregations(&self) -> &[StatisticsRequiredAggregation] {
        &self.required_aggregations
    }

    pub fn into_parts(
        self,
    ) -> (
        ConnectorTableHandle,
        StatisticsDataVersion,
        Option<i64>,
        Vec<StatisticsRequiredAggregation>,
        Box<dyn StatisticsCollectionSession>,
    ) {
        (
            self.table,
            self.data_version,
            self.read_version_ordinal,
            self.required_aggregations,
            self.session,
        )
    }
}

/// FE-local, single-use publication authority. It has no serialization or
/// cloning surface, and `finish` consumes the authority even on failure.
pub trait StatisticsCollectionSession: Send {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn operation_id(&self) -> ConnectorMutationOperationId;
    fn expectations(&self) -> &[StatisticsArtifactIdentity];
    fn finish(
        self: Box<Self>,
        artifacts: Vec<StatisticsArtifactDraft>,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError>;
}

/// Collection-level fact: whether this measurement observed every visible row
/// of its basis. It is deliberately independent of how accurate the resulting
/// numbers are — a scan can cover every visible row and still report an
/// approximate sketch value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsRowCoverage {
    AllVisibleRows,
    PartialRows,
}

/// Per-metric fact: which kind of artifact the value was read from.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StatisticsMetricSource {
    CurrentManifest,
    ProviderArtifact,
    VisibleRowScan,
    Provider(Arc<str>),
}

/// Per-metric fact: how the value relates to the true value **on its own
/// basis**. It says nothing about how that basis relates to the queried
/// version; that is `StatisticsBasisRelation`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsNumericNature {
    Exact,
    UpperBound,
    LowerBound,
    /// Estimated in both directions. Sketch-derived values such as Theta NDV
    /// are always this, even when their basis is the queried version.
    TwoSidedApproximate,
}

/// Per-metric fact: how the metric's basis row set relates to the row set of
/// the queried version. It says nothing about whether the value is accurate on
/// that basis; that is `StatisticsNumericNature`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsBasisRelation {
    Identical,
    BasisIsSubset,
    BasisIsSuperset,
    /// The provider could not prove any of the above. Providers must return
    /// this rather than guessing.
    Incomparable,
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

/// One provided metric value together with the three per-metric facts that
/// describe it. Keeping them on the value rather than on the whole evidence is
/// what lets one answer carry an exact current row count, a directional bound,
/// and an approximate ancestor NDV without any of them polluting the others.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsMetricObservation {
    value: StatisticsMetricValue,
    basis_version: StatisticsDataVersion,
    source: StatisticsMetricSource,
    numeric_nature: StatisticsNumericNature,
    basis_relation: StatisticsBasisRelation,
    interval: Option<StatisticsInterval>,
}

impl StatisticsMetricObservation {
    pub fn new(
        value: StatisticsMetricValue,
        basis_version: StatisticsDataVersion,
        source: StatisticsMetricSource,
        numeric_nature: StatisticsNumericNature,
        basis_relation: StatisticsBasisRelation,
    ) -> Self {
        Self {
            value,
            basis_version,
            source,
            numeric_nature,
            basis_relation,
            interval: None,
        }
    }

    /// Attaches a confidence interval. No producer fills one today; the setter
    /// exists so a future estimator does not have to reshape the observation.
    pub fn with_interval(mut self, interval: StatisticsInterval) -> Self {
        self.interval = Some(interval);
        self
    }

    pub fn value(&self) -> &StatisticsMetricValue {
        &self.value
    }
    pub fn basis_version(&self) -> &StatisticsDataVersion {
        &self.basis_version
    }
    pub fn source(&self) -> &StatisticsMetricSource {
        &self.source
    }
    pub const fn numeric_nature(&self) -> StatisticsNumericNature {
        self.numeric_nature
    }
    pub const fn basis_relation(&self) -> StatisticsBasisRelation {
        self.basis_relation
    }
    pub const fn interval(&self) -> Option<StatisticsInterval> {
        self.interval
    }

    /// Whether this value describes the row set the caller is asking about.
    ///
    /// This is the admission question for a consumer, and `Identical` is
    /// exactly the claim it needs: the basis holds the same rows. That can be
    /// true of a *different* snapshot — a compaction rewrites files without
    /// changing which rows exist — so this deliberately does not also require
    /// the basis version to match. Requiring it would re-tie "same version" to
    /// "same rows", which is the conflation this model exists to remove.
    ///
    /// Numeric nature is likewise not part of it: a bound or a sketch estimate
    /// still describes the right rows, and how far to trust it is a separate,
    /// consumer-owned confidence decision.
    pub fn describes_queried_rows(&self) -> bool {
        self.basis_relation == StatisticsBasisRelation::Identical
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
    Available(StatisticsMetricObservation),
    Missing(StatisticsMissing),
    Error(StatisticsMetricError),
}

/// One immutable statistics answer. It has no table client, runtime handle, or
/// executable artifact, so callers may cache it using the explicit version pair.
///
/// Fields are private because two of its invariants cannot be restored after
/// the fact: a sketch-derived metric must never claim to be exact, and there
/// must be no evidence-level accuracy or provenance that can disagree with the
/// per-metric facts. `try_new` is the only way to build one.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsEvidence {
    data_version: StatisticsDataVersion,
    evidence_revision: StatisticsEvidenceRevision,
    row_coverage: StatisticsRowCoverage,
    metrics: BTreeMap<StatisticsMetric, StatisticsMetricState>,
}

impl StatisticsEvidence {
    pub fn try_new(
        data_version: StatisticsDataVersion,
        evidence_revision: StatisticsEvidenceRevision,
        row_coverage: StatisticsRowCoverage,
        metrics: BTreeMap<StatisticsMetric, StatisticsMetricState>,
    ) -> Result<Self, ConnectorError> {
        let evidence = Self {
            data_version,
            evidence_revision,
            row_coverage,
            metrics,
        };
        evidence.validate()?;
        Ok(evidence)
    }

    /// Rejects evidence whose per-metric facts contradict themselves.
    ///
    /// `try_new` is the only constructor and the fields are private, so no
    /// provider — in this crate or outside it — can produce evidence that
    /// skipped this check.
    fn validate(&self) -> Result<(), ConnectorError> {
        for (metric, state) in &self.metrics {
            let StatisticsMetricState::Available(observation) = state else {
                continue;
            };
            if matches!(metric, StatisticsMetric::ThetaNdv { .. })
                && observation.numeric_nature() != StatisticsNumericNature::TwoSidedApproximate
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Theta NDV is a two-sided estimate and cannot be reported as an exact or one-sided value",
                ));
            }
            // A value measured on the queried version itself cannot claim its
            // rows differ from that version's. The converse does not hold: a
            // compaction produces a new snapshot over the same logical rows, so
            // an older basis may legitimately be `Identical`.
            if *observation.basis_version() == self.data_version
                && observation.basis_relation() != StatisticsBasisRelation::Identical
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "statistics metric measured on the queried version cannot claim a different row set",
                ));
            }
        }
        Ok(())
    }

    pub fn data_version(&self) -> &StatisticsDataVersion {
        &self.data_version
    }
    pub fn evidence_revision(&self) -> &StatisticsEvidenceRevision {
        &self.evidence_revision
    }
    pub const fn row_coverage(&self) -> StatisticsRowCoverage {
        self.row_coverage
    }
    pub fn metrics(&self) -> &BTreeMap<StatisticsMetric, StatisticsMetricState> {
        &self.metrics
    }
    pub fn into_metrics(self) -> BTreeMap<StatisticsMetric, StatisticsMetricState> {
        self.metrics
    }
}

#[derive(Clone)]
pub struct StatisticsReadRequest {
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

#[derive(Clone, Eq, PartialEq)]
pub struct StatisticsReceipt {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    operation_id: ConnectorMutationOperationId,
    data_version: StatisticsDataVersion,
    evidence_revision: StatisticsEvidenceRevision,
    provider_payload: Bytes,
}

impl StatisticsReceipt {
    pub fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
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
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
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
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn read_statistics(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError>;
}

/// Optional collection and publication half of a connector statistics
/// capability. Providers without this trait remain valid readers.
pub trait StatisticsCollection: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn begin_collection(
        &self,
        request: StatisticsCollectionStartRequest,
    ) -> Result<StatisticsCollectionStart, ConnectorError>;
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
    incarnation: ProviderBindingEpoch,
    statistics: Arc<dyn ConnectorStatistics>,
    _release: Arc<StatisticsLeaseRelease>,
}

struct StatisticsLeaseRelease {
    release: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
}

impl ConnectorStatisticsLease {
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
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
    pub const fn incarnation(&self) -> ProviderBindingEpoch {
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
        if evidence.data_version().as_bytes().is_empty()
            || evidence.evidence_revision().as_bytes().is_empty()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector statistics reader returned an empty version token",
            ));
        }
        Ok(evidence)
    }
    pub fn begin_collection(
        self,
        request: StatisticsCollectionStartRequest,
    ) -> Result<StatisticsCollectionStart, ConnectorError> {
        self.validate_table(&request.table)?;
        let expected_table = request.table.clone();
        let expected_data_version = request.data_version.clone();
        let expected_operation_id = request.operation_id;
        let collection = self.collection()?;
        let start = collection.begin_collection(request)?;
        if start.table() != &expected_table || start.data_version() != &expected_data_version {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector statistics session did not preserve its resolved table pin",
            ));
        }
        if start.session.descriptor() != &self.descriptor
            || start.session.incarnation() != self.incarnation
            || start.session.operation_id() != expected_operation_id
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector statistics session does not match its lease generation or operation",
            ));
        }
        let StatisticsCollectionStart {
            table,
            data_version,
            read_version_ordinal,
            required_aggregations,
            session,
        } = start;
        let session = Box::new(LeaseBoundStatisticsSession {
            inner: session,
            _lease: self,
        });
        StatisticsCollectionStart::try_new(
            table,
            data_version,
            read_version_ordinal,
            required_aggregations,
            session,
        )
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
}

struct LeaseBoundStatisticsSession {
    inner: Box<dyn StatisticsCollectionSession>,
    _lease: ConnectorStatisticsLease,
}

impl StatisticsCollectionSession for LeaseBoundStatisticsSession {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.inner.descriptor()
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.inner.incarnation()
    }

    fn operation_id(&self) -> ConnectorMutationOperationId {
        self.inner.operation_id()
    }

    fn expectations(&self) -> &[StatisticsArtifactIdentity] {
        self.inner.expectations()
    }

    fn finish(
        self: Box<Self>,
        artifacts: Vec<StatisticsArtifactDraft>,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        let Self { inner, _lease } = *self;
        inner.finish(artifacts)
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
    incarnation: ProviderBindingEpoch,
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

#[cfg(test)]
mod tests {
    use super::*;

    struct NeverCancelled;

    impl crate::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct TestSession {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        operation_id: ConnectorMutationOperationId,
        expectations: Vec<StatisticsArtifactIdentity>,
    }

    impl StatisticsCollectionSession for TestSession {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn operation_id(&self) -> ConnectorMutationOperationId {
            self.operation_id
        }

        fn expectations(&self) -> &[StatisticsArtifactIdentity] {
            &self.expectations
        }

        fn finish(
            self: Box<Self>,
            _artifacts: Vec<StatisticsArtifactDraft>,
        ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
            unreachable!("contract construction tests never finish the session")
        }
    }

    fn test_descriptor() -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: crate::connector::ConnectorProviderId::parse("test").unwrap(),
            instance_id: ConnectorInstanceId::parse("test.instance").unwrap(),
        }
    }

    fn requirement(field_id: i32, ordinal: usize) -> StatisticsRequiredAggregation {
        StatisticsRequiredAggregation::try_new(
            StatisticsScanColumn::try_new(ordinal, format!("c{field_id}"), DataType::Int64, true)
                .unwrap(),
            "$test_stat",
            StatisticsArtifactIdentity::try_new(vec![field_id], "test/blob").unwrap(),
        )
        .unwrap()
    }

    fn test_session(expectations: Vec<StatisticsArtifactIdentity>) -> Box<TestSession> {
        Box::new(TestSession {
            descriptor: test_descriptor(),
            incarnation: ProviderBindingEpoch::new(),
            operation_id: ConnectorMutationOperationId::new(),
            expectations,
        })
    }

    struct WrongOperationStatistics {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
    }

    impl StatisticsReader for WrongOperationStatistics {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn read_statistics(
            &self,
            _request: StatisticsReadRequest,
        ) -> Result<StatisticsEvidence, ConnectorError> {
            unreachable!("not used")
        }
    }

    impl StatisticsCollection for WrongOperationStatistics {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }

        fn begin_collection(
            &self,
            request: StatisticsCollectionStartRequest,
        ) -> Result<StatisticsCollectionStart, ConnectorError> {
            StatisticsCollectionStart::try_new(
                request.table,
                request.data_version,
                None,
                Vec::new(),
                Box::new(TestSession {
                    descriptor: self.descriptor.clone(),
                    incarnation: self.incarnation,
                    operation_id: ConnectorMutationOperationId::new(),
                    expectations: Vec::new(),
                }),
            )
        }
    }

    impl ConnectorStatistics for WrongOperationStatistics {
        fn collection(&self) -> Option<&dyn StatisticsCollection> {
            Some(self)
        }
    }

    fn data_version(token: &'static [u8]) -> StatisticsDataVersion {
        StatisticsDataVersion::try_new(Bytes::from_static(token)).expect("data version")
    }

    fn revision() -> StatisticsEvidenceRevision {
        StatisticsEvidenceRevision::try_new(Bytes::from_static(b"rev-1")).expect("revision")
    }

    fn theta() -> StatisticsMetric {
        StatisticsMetric::ThetaNdv {
            column: Arc::from("k"),
        }
    }

    fn observation(
        basis: StatisticsDataVersion,
        nature: StatisticsNumericNature,
        relation: StatisticsBasisRelation,
    ) -> StatisticsMetricObservation {
        StatisticsMetricObservation::new(
            StatisticsMetricValue::F64(7.0),
            basis,
            StatisticsMetricSource::ProviderArtifact,
            nature,
            relation,
        )
    }

    fn evidence(
        metrics: BTreeMap<StatisticsMetric, StatisticsMetricState>,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        StatisticsEvidence::try_new(
            data_version(b"data-v1"),
            revision(),
            StatisticsRowCoverage::AllVisibleRows,
            metrics,
        )
    }

    #[test]
    fn collection_start_requires_exact_ordered_session_expectations() {
        let requirements = vec![requirement(1, 0), requirement(2, 1)];
        let reversed = requirements
            .iter()
            .rev()
            .map(|requirement| requirement.artifact().clone())
            .collect();
        let error = match StatisticsCollectionStart::try_new(
            ConnectorTableHandle::try_new(
                test_descriptor().instance_id,
                Bytes::from_static(b"table"),
            )
            .unwrap(),
            data_version(b"data-v1"),
            Some(7),
            requirements,
            test_session(reversed),
        ) {
            Ok(_) => panic!("planner order and session expectation order must be identical"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn artifact_identity_preserves_order_but_rejects_duplicate_fields() {
        let identity = StatisticsArtifactIdentity::try_new(vec![2, 1], "test/blob")
            .expect("ordered composite identity");
        assert_eq!(identity.input_fields(), &[2, 1]);

        let error = StatisticsArtifactIdentity::try_new(vec![1, 1], "test/blob")
            .expect_err("duplicate field identity must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn collection_start_rejects_duplicate_requirements_and_missing_read_pin() {
        let duplicate = vec![requirement(1, 0), requirement(1, 1)];
        let expectations = duplicate
            .iter()
            .map(|requirement| requirement.artifact().clone())
            .collect();
        let table = ConnectorTableHandle::try_new(
            test_descriptor().instance_id,
            Bytes::from_static(b"table"),
        )
        .unwrap();
        assert!(
            StatisticsCollectionStart::try_new(
                table.clone(),
                data_version(b"data-v1"),
                Some(7),
                duplicate,
                test_session(expectations),
            )
            .is_err()
        );

        let one = requirement(1, 0);
        let expectation = one.artifact().clone();
        assert!(
            StatisticsCollectionStart::try_new(
                table,
                data_version(b"data-v1"),
                None,
                vec![one],
                test_session(vec![expectation]),
            )
            .is_err()
        );
    }

    #[test]
    fn lease_rejects_a_session_for_a_different_operation_before_execution() {
        let descriptor = test_descriptor();
        let incarnation = ProviderBindingEpoch::new();
        let table = ConnectorTableHandle::try_new(
            descriptor.instance_id.clone(),
            Bytes::from_static(b"table"),
        )
        .unwrap();
        let lease = ConnectorStatisticsLease::new(
            descriptor.clone(),
            incarnation,
            Arc::new(WrongOperationStatistics {
                descriptor,
                incarnation,
            }),
            || {},
        )
        .expect("lease");
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(1),
            Arc::new(NeverCancelled),
            1024,
            1024,
        )
        .expect("context");
        let result = lease.begin_collection(StatisticsCollectionStartRequest {
            operation_id: ConnectorMutationOperationId::new(),
            table,
            data_version: data_version(b"data-v1"),
            selection: StatisticsColumnSelection::Default,
            context,
        });
        assert!(matches!(result, Err(error) if error.kind() == ConnectorErrorKind::CorruptData));
    }

    #[test]
    fn theta_ndv_cannot_be_reported_as_exact_even_on_the_queried_version() {
        for nature in [
            StatisticsNumericNature::Exact,
            StatisticsNumericNature::UpperBound,
            StatisticsNumericNature::LowerBound,
        ] {
            let error = evidence(BTreeMap::from([(
                theta(),
                StatisticsMetricState::Available(observation(
                    data_version(b"data-v1"),
                    nature,
                    StatisticsBasisRelation::Identical,
                )),
            )]))
            .expect_err("sketch-derived NDV must not claim a non-approximate nature");
            assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        }

        evidence(BTreeMap::from([(
            theta(),
            StatisticsMetricState::Available(observation(
                data_version(b"data-v1"),
                StatisticsNumericNature::TwoSidedApproximate,
                StatisticsBasisRelation::Identical,
            )),
        )]))
        .expect("two-sided approximate Theta NDV is the only admissible labelling");
    }

    #[test]
    fn a_metric_measured_on_the_queried_version_cannot_claim_different_rows() {
        let same_basis_claiming_divergence = evidence(BTreeMap::from([(
            StatisticsMetric::RowCount,
            StatisticsMetricState::Available(observation(
                data_version(b"data-v1"),
                StatisticsNumericNature::Exact,
                StatisticsBasisRelation::BasisIsSuperset,
            )),
        )]));
        assert!(same_basis_claiming_divergence.is_err());

        evidence(BTreeMap::from([(
            StatisticsMetric::RowCount,
            StatisticsMetricState::Available(observation(
                data_version(b"data-v0"),
                StatisticsNumericNature::Exact,
                StatisticsBasisRelation::BasisIsSuperset,
            )),
        )]))
        .expect("an older basis paired with a non-identical relation is the normal ancestor case");
    }

    /// A compaction writes new files over the same logical rows. Such a basis is
    /// an older version whose row set is nonetheless identical, so the model has
    /// to be able to say both things at once — tying `Identical` to version
    /// equality would make this case inexpressible.
    #[test]
    fn an_older_basis_may_still_hold_the_same_rows() {
        let rewritten = evidence(BTreeMap::from([(
            theta(),
            StatisticsMetricState::Available(observation(
                data_version(b"data-v0"),
                StatisticsNumericNature::TwoSidedApproximate,
                StatisticsBasisRelation::Identical,
            )),
        )]))
        .expect("a rewrite-only ancestor basis is identical in rows, not in version");

        let Some(StatisticsMetricState::Available(observation)) = rewritten.metrics().get(&theta())
        else {
            panic!("metric must be available");
        };
        assert_ne!(observation.basis_version(), rewritten.data_version());
        assert!(
            observation.describes_queried_rows(),
            "an identical row set is admissible however old the snapshot is"
        );
    }

    #[test]
    fn one_evidence_carries_exact_bounded_and_ancestor_metrics_without_cross_contamination() {
        let evidence = evidence(BTreeMap::from([
            (
                StatisticsMetric::RowCount,
                StatisticsMetricState::Available(observation(
                    data_version(b"data-v1"),
                    StatisticsNumericNature::Exact,
                    StatisticsBasisRelation::Identical,
                )),
            ),
            (
                StatisticsMetric::Maximum {
                    column: Arc::from("k"),
                },
                StatisticsMetricState::Available(observation(
                    data_version(b"data-v1"),
                    StatisticsNumericNature::UpperBound,
                    StatisticsBasisRelation::Identical,
                )),
            ),
            (
                theta(),
                StatisticsMetricState::Available(observation(
                    data_version(b"data-v0"),
                    StatisticsNumericNature::TwoSidedApproximate,
                    StatisticsBasisRelation::BasisIsSuperset,
                )),
            ),
        ]))
        .expect("mixed-provenance evidence is the point of the per-metric model");

        let metrics = evidence.metrics();
        let nature = |metric: &StatisticsMetric| match metrics.get(metric) {
            Some(StatisticsMetricState::Available(observation)) => observation.numeric_nature(),
            _ => panic!("metric must be available"),
        };
        assert_eq!(
            nature(&StatisticsMetric::RowCount),
            StatisticsNumericNature::Exact
        );
        assert_eq!(
            nature(&StatisticsMetric::Maximum {
                column: Arc::from("k")
            }),
            StatisticsNumericNature::UpperBound
        );
        assert_eq!(
            nature(&theta()),
            StatisticsNumericNature::TwoSidedApproximate
        );
        assert_eq!(
            evidence.row_coverage(),
            StatisticsRowCoverage::AllVisibleRows
        );
    }

    #[test]
    fn only_the_basis_decides_admission_never_the_numeric_nature() {
        let queried = data_version(b"data-v1");

        for nature in [
            StatisticsNumericNature::Exact,
            StatisticsNumericNature::UpperBound,
            StatisticsNumericNature::LowerBound,
            StatisticsNumericNature::TwoSidedApproximate,
        ] {
            assert!(
                observation(queried.clone(), nature, StatisticsBasisRelation::Identical)
                    .describes_queried_rows(),
                "{nature:?} measured on the queried version still describes it"
            );
        }

        for relation in [
            StatisticsBasisRelation::BasisIsSubset,
            StatisticsBasisRelation::BasisIsSuperset,
            StatisticsBasisRelation::Incomparable,
        ] {
            assert!(
                !observation(
                    data_version(b"data-v0"),
                    StatisticsNumericNature::Exact,
                    relation
                )
                .describes_queried_rows(),
                "an exact value on a {relation:?} basis still describes other rows"
            );
        }
    }
}
