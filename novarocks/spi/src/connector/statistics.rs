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

mod scoped_cpu;
pub use scoped_cpu::*;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorMetadataMaintenanceIoProfile, ConnectorMutationOperationId,
    ConnectorOriginalResultScope, ConnectorPayloadRetentionGuard, ConnectorRequestContext,
    ConnectorTableHandle, ConnectorTableRequest, ExternalMutationEffect,
    ExternalMutationFinalization, ExternalMutationOutcome, ProviderBindingEpoch,
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
#[derive(Clone, Debug)]
pub struct StatisticsArtifactIdentity {
    input_fields: Vec<i32>,
    blob_type: Arc<str>,
    retention: Option<ConnectorPayloadRetentionGuard>,
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
            retention: None,
        })
    }

    pub fn input_fields(&self) -> &[i32] {
        &self.input_fields
    }

    pub fn blob_type(&self) -> &str {
        &self.blob_type
    }
}

// Retention is a lifetime fact and has no semantic identity.
impl PartialEq for StatisticsArtifactIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.input_fields == other.input_fields && self.blob_type == other.blob_type
    }
}
impl Eq for StatisticsArtifactIdentity {}
impl PartialOrd for StatisticsArtifactIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for StatisticsArtifactIdentity {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.input_fields, &self.blob_type).cmp(&(&other.input_fields, &other.blob_type))
    }
}
impl std::hash::Hash for StatisticsArtifactIdentity {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.input_fields, state);
        std::hash::Hash::hash(&self.blob_type, state);
    }
}

/// Generic long-form artifact row produced after an ordinary aggregate and
/// Unpivot. This carrier contains no provider session or catalog authority.
#[derive(Clone, Debug)]
pub struct StatisticsArtifactDraft {
    identity: StatisticsArtifactIdentity,
    body: Bytes,
    properties: BTreeMap<String, String>,
    retention: Option<ConnectorPayloadRetentionGuard>,
}

impl StatisticsArtifactDraft {
    pub fn try_new(
        input_fields: Vec<i32>,
        blob_type: impl Into<Arc<str>>,
        body: Bytes,
        properties: BTreeMap<String, String>,
    ) -> Result<Self, ConnectorError> {
        validate_artifact_parts(body.len(), &properties)?;
        Ok(Self {
            identity: StatisticsArtifactIdentity::try_new(input_fields, blob_type)?,
            body,
            properties,
            retention: None,
        })
    }

    /// The inputs must already be covered by the caller's admitted holder.
    /// This factory retains that holder before validation allocates scratch;
    /// it neither admits an ungoverned graph nor acquires fresh capacity.
    pub fn try_new_with_guard(
        input_fields: Vec<i32>,
        blob_type: impl Into<Arc<str>>,
        body: Bytes,
        properties: BTreeMap<String, String>,
        guard: ConnectorPayloadRetentionGuard,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(input_fields, blob_type, body, properties)
            .map(|draft| draft.attach_guard(guard))
    }

    /// Ownership handoff for an already-covered graph, with no payload copy.
    /// Callers must hold their admission before constructing the original.
    pub fn attach_guard(mut self, guard: ConnectorPayloadRetentionGuard) -> Self {
        let guard = match self.retention.take() {
            Some(previous) => ConnectorPayloadRetentionGuard::new((previous, guard)),
            None => guard,
        };
        self.body = Bytes::from_owner(RetainedArtifactBody {
            body: self.body,
            _guard: guard.clone(),
        });
        self.identity.retention = Some(guard.clone());
        // Keep any previous holder through the body and identity when a
        // covered graph is handed between owners.
        self.retention = Some(guard);
        self
    }

    pub fn into_guarded_parts(self) -> StatisticsArtifactParts {
        StatisticsArtifactParts { draft: self }
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

    /// Transition-only extraction. Guarded consumers must use
    /// `into_guarded_parts` so property metadata retains its holder as well.
    pub fn into_parts(self) -> (StatisticsArtifactIdentity, Bytes, BTreeMap<String, String>) {
        (self.identity, self.body, self.properties)
    }
}

fn validate_artifact_parts(
    body_len: usize,
    properties: &BTreeMap<String, String>,
) -> Result<(), ConnectorError> {
    if body_len > MAX_CONNECTOR_STATISTICS_ARTIFACT_BODY_BYTES {
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
    Ok(())
}

impl PartialEq for StatisticsArtifactDraft {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity
            && self.body == other.body
            && self.properties == other.properties
    }
}
impl Eq for StatisticsArtifactDraft {}

// Drop the actual backing before its caller-provided capacity holder.
struct RetainedArtifactBody {
    body: Bytes,
    _guard: ConnectorPayloadRetentionGuard,
}
impl AsRef<[u8]> for RetainedArtifactBody {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}

/// Retained decomposition for provider normalization and assembly. Borrowing
/// metadata does not detach it; cloning this carrier retains the same holder.
#[derive(Clone, Debug)]
pub struct StatisticsArtifactParts {
    draft: StatisticsArtifactDraft,
}
impl StatisticsArtifactParts {
    pub fn identity(&self) -> &StatisticsArtifactIdentity {
        self.draft.identity()
    }
    pub fn body(&self) -> &Bytes {
        self.draft.body()
    }
    pub fn properties(&self) -> &BTreeMap<String, String> {
        self.draft.properties()
    }

    /// Mutate metadata while the carrier and its holder remain owned. The
    /// caller covers new growth with the same pre-admitted capacity.
    pub fn properties_mut(&mut self) -> &mut BTreeMap<String, String> {
        &mut self.draft.properties
    }

    /// Revalidate normalized metadata without replacing the retained identity
    /// or body. Error meanings and ordering match the ordinary constructor.
    pub fn try_into_draft(self) -> Result<StatisticsArtifactDraft, ConnectorError> {
        let draft = self.draft;
        validate_artifact_parts(draft.body.len(), &draft.properties)?;
        Ok(draft)
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
pub struct StatisticsCollectionStart<S: ?Sized = dyn StatisticsCollectionSession> {
    table: ConnectorTableHandle,
    data_version: StatisticsDataVersion,
    read_version_ordinal: Option<i64>,
    required_aggregations: Vec<StatisticsRequiredAggregation>,
    session: Box<S>,
}

impl StatisticsCollectionStart {
    pub fn try_new(
        table: ConnectorTableHandle,
        data_version: StatisticsDataVersion,
        read_version_ordinal: Option<i64>,
        required_aggregations: Vec<StatisticsRequiredAggregation>,
        session: Box<dyn StatisticsCollectionSession>,
    ) -> Result<Self, ConnectorError> {
        validate_collection_start(
            &required_aggregations,
            read_version_ordinal,
            session.expectations(),
        )?;
        Ok(Self {
            table,
            data_version,
            read_version_ordinal,
            required_aggregations,
            session,
        })
    }
}

impl StatisticsCollectionStart<dyn AsyncStatisticsCollectionSession> {
    pub fn try_new_async(
        table: ConnectorTableHandle,
        data_version: StatisticsDataVersion,
        read_version_ordinal: Option<i64>,
        required_aggregations: Vec<StatisticsRequiredAggregation>,
        session: Box<dyn AsyncStatisticsCollectionSession>,
    ) -> Result<Self, ConnectorError> {
        validate_collection_start(
            &required_aggregations,
            read_version_ordinal,
            session.expectations(),
        )?;
        Ok(Self {
            table,
            data_version,
            read_version_ordinal,
            required_aggregations,
            session,
        })
    }
}

impl<S: ?Sized> StatisticsCollectionStart<S> {
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
        Box<S>,
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

fn validate_collection_start(
    required_aggregations: &[StatisticsRequiredAggregation],
    read_version_ordinal: Option<i64>,
    expectations: &[StatisticsArtifactIdentity],
) -> Result<(), ConnectorError> {
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
    let expected = expectations.iter().collect::<Vec<_>>();
    if planned != expected {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "statistics aggregation requirements do not exactly match the session expectations",
        ));
    }
    Ok(())
}

/// Needed publication facts, authored before optional owned-object GC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsPublicationReady {
    Committed {
        effect: ExternalMutationEffect,
        receipt: StatisticsReceipt,
        finalization: ExternalMutationFinalization,
    },
    Settled(ExternalMutationOutcome<StatisticsReceipt>),
}

/// Closing observes the SAME operation. Unknown publication retains its
/// original evidence and never authorizes deletion or another dispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsPublicationClose {
    Observed {
        outcome: ExternalMutationOutcome<StatisticsReceipt>,
        /// Actual disposal evidence is separate from cumulative diagnostics.
        owned_objects_disposed: bool,
    },
    RetainedUnknown {
        failure: super::ConnectorMutationFailure,
        evidence: super::ExternalMutationEvidence,
    },
}

pub type StatisticsSessionFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, ConnectorError>> + Send + 'a>>;

fn async_collection_unsupported() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "connector statistics capability does not support supervised async collection",
    )
}

/// FE-local, single-use publication authority. It has no serialization or
/// cloning surface. Both `finish` and `abort` consume the authority, so a
/// caller must explicitly converge every session it starts rather than
/// treating `Drop` as provider cleanup.
pub trait StatisticsCollectionSession: Send {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn operation_id(&self) -> ConnectorMutationOperationId;
    fn expectations(&self) -> &[StatisticsArtifactIdentity];

    fn finish(
        self: Box<Self>,
        artifacts: Vec<StatisticsArtifactDraft>,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError>;

    /// Abandons a collection before publication and releases provider-local
    /// state such as an uncommitted transaction frontier.
    fn abort(self: Box<Self>) -> Result<(), ConnectorError>;
}

/// The supervised session exposes only borrowing operations. It cannot call
/// the legacy consuming finish/abort API after installing a publication source.
pub trait AsyncStatisticsCollectionSession: Send {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ProviderBindingEpoch;
    fn operation_id(&self) -> ConnectorMutationOperationId;
    fn expectations(&self) -> &[StatisticsArtifactIdentity];
    fn io_profile(&self) -> Result<ConnectorMetadataMaintenanceIoProfile, ConnectorError> {
        Err(async_collection_unsupported())
    }
    /// The caller retains both original slots through this borrowed future.
    /// Validation/refusal leaves the artifact slot unchanged. No publication
    /// is dispatched until finish_publication_async is polled.
    fn prepare_publication_async<'a>(
        &'a mut self,
        _artifacts: &'a mut Option<Vec<StatisticsArtifactDraft>>,
    ) -> StatisticsSessionFuture<'a, ()> {
        Box::pin(async { Err(async_collection_unsupported()) })
    }
    fn finish_publication_async(
        &mut self,
    ) -> StatisticsSessionFuture<'_, StatisticsPublicationReady> {
        Box::pin(async { Err(async_collection_unsupported()) })
    }
    /// Current is the same original action's existing-obligation context.
    /// A returned observation is not an assertion that retained cleanup exited.
    fn close_publication_async<'a>(
        &'a mut self,
        _current: &'a ConnectorRequestContext,
    ) -> StatisticsSessionFuture<'a, StatisticsPublicationClose> {
        Box::pin(async { Err(async_collection_unsupported()) })
    }
}

pub type AsyncStatisticsCollectionStart =
    StatisticsCollectionStart<dyn AsyncStatisticsCollectionSession>;

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
    /// The read as it stands after negotiation, when one has happened.
    ///
    /// Statistics for a table and statistics for a read of that table are
    /// different numbers: once a provider has taken on a predicate, the rows it
    /// will actually return are the pruned ones, and a caller reasoning about
    /// cost needs those rather than the whole table's. `None` asks about the
    /// table itself, which is all a caller can ask before any negotiation.
    ///
    /// A provider that cannot answer for a narrowed read answers for the table;
    /// that is a weaker answer, never a wrong one.
    pub narrowed_read: Option<crate::connector::read_stack::runtime::ConnectorReadTableHandle>,
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
/// not catalog metadata: defaults and connector credentials are excluded.
/// The complete admitted value domain and nested Arrow field facts are kept;
/// a carrier alone cannot declare numeric or opaque logical identity.
#[derive(Clone, Debug, PartialEq)]
pub struct StatisticsScanColumn {
    ordinal: usize,
    name: Arc<str>,
    value_type: novarocks_type_contract::FunctionValueType,
}

impl StatisticsScanColumn {
    pub fn try_new(
        ordinal: usize,
        name: impl Into<Arc<str>>,
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> Result<Self, ConnectorError> {
        let name = name.into();
        if name.is_empty() || name.len() > MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics scan column name is empty or exceeds the payload limit",
            ));
        }
        value_type.validate().map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("statistics scan column has invalid complete value type: {error}"),
            )
        })?;
        Ok(Self {
            ordinal,
            name,
            value_type,
        })
    }

    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn value_type(&self) -> &novarocks_type_contract::FunctionValueType {
        &self.value_type
    }

    pub fn data_type(&self) -> &DataType {
        self.value_type.data_type()
    }

    pub fn nullable(&self) -> bool {
        self.value_type.nullable()
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

    /// Observe the original pinned evidence without a synchronous runtime bridge.
    /// A scoped SQL read is constructed only by this exact lease's Provider.
    /// This does not delegate to the legacy synchronous or ungoverned reader.
    fn prepare_scoped_read(
        &self,
        _request: ConnectorTableRequest,
        _original: ConnectorOriginalResultScope,
    ) -> Result<Arc<dyn StatisticsReadOriginal>, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "connector has no scoped statistics read source",
        ))
    }

    fn read_statistics_async(
        &self,
        request: StatisticsReadRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<StatisticsEvidence, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let _ = request;
            Err(ConnectorError::new(
                super::ConnectorErrorKind::Unsupported,
                "connector statistics does not support asynchronous observation",
            ))
        })
    }
}

/// Optional collection and publication half of a connector statistics
/// capability. Providers without this trait remain valid readers.
pub trait StatisticsCollection: Send + Sync {
    /// The sealed production Provider manifest's concrete original phase
    /// Source. None supports no scoped phase; descriptor equality is not a
    /// factory qualification. This never registers a new executor or runner.
    fn scoped_cpu_source_type_id(&self) -> Option<std::any::TypeId> {
        None
    }

    fn io_profile(&self) -> Result<ConnectorMetadataMaintenanceIoProfile, ConnectorError> {
        Err(async_collection_unsupported())
    }
    fn begin_collection_async(
        &self,
        _request: StatisticsCollectionStartRequest,
        _cpu: Arc<dyn StatisticsCpuPort>,
    ) -> StatisticsSessionFuture<'_, AsyncStatisticsCollectionStart> {
        Box::pin(async { Err(async_collection_unsupported()) })
    }
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
    pub fn scoped_cpu_source_type_id(&self) -> Result<std::any::TypeId, ConnectorError> {
        let collection = self
            .statistics
            .collection()
            .ok_or_else(async_collection_unsupported)?;
        if collection.descriptor() != &self.descriptor
            || collection.incarnation() != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Statistics CPU factory has a foreign lease generation",
            ));
        }
        collection
            .scoped_cpu_source_type_id()
            .ok_or_else(async_collection_unsupported)
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
    pub fn prepare_scoped_read(
        &self,
        request: ConnectorTableRequest,
        original: ConnectorOriginalResultScope,
    ) -> Result<Arc<dyn StatisticsReadOriginal>, ConnectorError> {
        if request.table.instance_id != self.descriptor.instance_id {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics read table has a foreign lease",
            ));
        }
        let source = self.statistics.prepare_scoped_read(request, original)?;
        if source.descriptor() != &self.descriptor
            || source.incarnation() != self.incarnation
            || std::any::Any::type_id(source.as_ref()) != self.scoped_cpu_source_type_id()?
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "statistics read source has a foreign exact generation",
            ));
        }
        Ok(source)
    }
    pub fn collection_io_profile(
        &self,
    ) -> Result<ConnectorMetadataMaintenanceIoProfile, ConnectorError> {
        self.collection()?.io_profile()
    }
    /// This future borrows the caller's original lease. The returned session
    /// retains that same generation before the future returns.
    pub async fn begin_collection_async(
        &self,
        request: StatisticsCollectionStartRequest,
        cpu: Arc<dyn StatisticsCpuPort>,
    ) -> Result<AsyncStatisticsCollectionStart, ConnectorError> {
        self.validate_table(&request.table)?;
        let expected_table = request.table.clone();
        let expected_version = request.data_version.clone();
        let expected_operation = request.operation_id;
        let start = self
            .collection()?
            .begin_collection_async(request, cpu)
            .await?;
        if start.table() != &expected_table || start.data_version() != &expected_version {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector statistics session did not preserve its resolved table pin",
            ));
        }
        if start.session.descriptor() != &self.descriptor
            || start.session.incarnation() != self.incarnation
            || start.session.operation_id() != expected_operation
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
        // The actual Provider's scoped schema CPU source already validated
        // requirements/expectations. Only this private wrapper moves that same
        // validated collection; it never copies or rebuilds its facts.
        Ok(StatisticsCollectionStart {
            table,
            data_version,
            read_version_ordinal,
            required_aggregations,
            session: Box::new(LeaseBoundStatisticsSession {
                inner: session,
                _lease: self.clone(),
            }),
        })
    }
    pub fn session_backing_bytes() -> usize {
        std::mem::size_of::<LeaseBoundStatisticsSession<dyn AsyncStatisticsCollectionSession>>()
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
        self.bind_collection_start(
            start,
            expected_table,
            expected_data_version,
            expected_operation_id,
        )
    }
    fn bind_collection_start(
        self,
        start: StatisticsCollectionStart,
        expected_table: ConnectorTableHandle,
        expected_data_version: StatisticsDataVersion,
        expected_operation_id: ConnectorMutationOperationId,
    ) -> Result<StatisticsCollectionStart, ConnectorError> {
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

struct LeaseBoundStatisticsSession<S: ?Sized> {
    inner: Box<S>,
    _lease: ConnectorStatisticsLease,
}

impl AsyncStatisticsCollectionSession
    for LeaseBoundStatisticsSession<dyn AsyncStatisticsCollectionSession>
{
    fn io_profile(&self) -> Result<ConnectorMetadataMaintenanceIoProfile, ConnectorError> {
        self.inner.io_profile()
    }
    fn prepare_publication_async<'a>(
        &'a mut self,
        artifacts: &'a mut Option<Vec<StatisticsArtifactDraft>>,
    ) -> StatisticsSessionFuture<'a, ()> {
        self.inner.prepare_publication_async(artifacts)
    }
    fn finish_publication_async(
        &mut self,
    ) -> StatisticsSessionFuture<'_, StatisticsPublicationReady> {
        self.inner.finish_publication_async()
    }
    fn close_publication_async<'a>(
        &'a mut self,
        current: &'a ConnectorRequestContext,
    ) -> StatisticsSessionFuture<'a, StatisticsPublicationClose> {
        self.inner.close_publication_async(current)
    }
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
}

impl StatisticsCollectionSession for LeaseBoundStatisticsSession<dyn StatisticsCollectionSession> {
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

    fn abort(self: Box<Self>) -> Result<(), ConnectorError> {
        let Self { inner, _lease } = *self;
        inner.abort()
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

        fn abort(self: Box<Self>) -> Result<(), ConnectorError> {
            Ok(())
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
            StatisticsScanColumn::try_new(
                ordinal,
                format!("c{field_id}"),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
            )
            .unwrap(),
            "$test_stat",
            StatisticsArtifactIdentity::try_new(vec![field_id], "test/blob").unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn scan_columns_preserve_every_explicit_root_value_domain() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        for (logical_type, data_type) in [
            (ValueLogicalType::Physical, DataType::Int64),
            (ValueLogicalType::Json, DataType::Utf8),
            (ValueLogicalType::Variant, DataType::LargeBinary),
            (ValueLogicalType::Hll, DataType::Binary),
            (ValueLogicalType::Bitmap, DataType::LargeBinary),
            (ValueLogicalType::Object, DataType::Binary),
            (ValueLogicalType::Percentile, DataType::LargeBinary),
            (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(16)),
            (ValueLogicalType::Uuid, DataType::FixedSizeBinary(16)),
        ] {
            for nullable in [false, true] {
                let expected = FunctionValueType::try_with_logical_type(
                    data_type.clone(),
                    nullable,
                    logical_type,
                )
                .unwrap();
                let column = StatisticsScanColumn::try_new(7, "actual", expected.clone()).unwrap();
                assert_eq!(column.ordinal(), 7);
                assert_eq!(column.name(), "actual");
                assert_eq!(column.value_type(), &expected);
                assert_eq!(column.data_type(), expected.data_type());
                assert_eq!(column.nullable(), nullable);
            }
        }
        // Equal physical widths do not erase distinct authored domains.
        let plain = StatisticsScanColumn::try_new(
            0,
            "x",
            FunctionValueType::new(DataType::FixedSizeBinary(16), false),
        )
        .unwrap();
        let integer = StatisticsScanColumn::try_new(
            0,
            "x",
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        )
        .unwrap();
        assert_ne!(plain, integer);
    }

    #[test]
    #[allow(deprecated)]
    fn scan_columns_preserve_complete_nested_domains_separately_from_source_encoding_and_attributes()
     {
        use arrow::datatypes::Field;
        use novarocks_type_contract::{
            FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueField, ValueKind, ValueLayout,
            ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
        };
        let storage = ValueTypeStorageLifetime::pure();
        let payload =
            FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
                .unwrap();
        let codes = FunctionValueType::new(DataType::Utf8, true);
        let source = DataType::Struct(
            vec![
                Field::new("payload", DataType::Utf8, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([
                        (
                            NR_LOGICAL_TYPE_KEY.into(),
                            "unknown-source-attribute".into(),
                        ),
                        ("provider.field-id".into(), "71".into()),
                    ]),
                ),
                Field::new_dict(
                    "codes",
                    DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
                    true,
                    91,
                    true,
                ),
            ]
            .into(),
        );
        let semantic = ValueTypeRef::try_new(
            ValueKind::Struct(
                vec![
                    ValueField::new("payload", payload.value_type().clone(), storage.clone()),
                    ValueField::new("codes", codes.value_type().clone(), storage.clone()),
                ]
                .into_boxed_slice(),
            ),
            true,
            storage.clone(),
        )
        .unwrap();
        let expected = FunctionValueType::try_from_value(
            semantic,
            ValueLayout::try_new(&source, storage).unwrap(),
        )
        .unwrap();
        let column = StatisticsScanColumn::try_new(3, "nested", expected.clone()).unwrap();
        assert_eq!(column.value_type(), &expected);
        assert_eq!(
            column
                .value_type()
                .value_type()
                .child_type_at(0)
                .unwrap()
                .domain(),
            Some(ValueLogicalType::Json)
        );
        assert_eq!(
            column
                .value_type()
                .value_type()
                .child_type_at(1)
                .unwrap()
                .kind(),
            &ValueKind::String
        );
        let DataType::Struct(fields) = column.data_type() else {
            panic!("declared struct layout")
        };
        assert!(fields.iter().all(|field| field.metadata().is_empty()));
        assert_eq!(fields[1].data_type(), &DataType::Utf8);
    }

    #[test]
    fn scan_columns_require_checked_complete_declarations_before_publication() {
        use arrow::datatypes::Field;
        use novarocks_type_contract::{
            FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueKind, ValueLayout, ValueLogicalType,
            ValueTypeError, ValueTypeRef, ValueTypeStorageLifetime,
        };
        for logical_type in [
            ValueLogicalType::Json,
            ValueLogicalType::Variant,
            ValueLogicalType::Hll,
            ValueLogicalType::Bitmap,
            ValueLogicalType::Object,
            ValueLogicalType::Percentile,
            ValueLogicalType::LargeInt,
            ValueLogicalType::Uuid,
        ] {
            assert_eq!(
                FunctionValueType::try_with_logical_type(DataType::Int64, true, logical_type),
                Err(ValueTypeError::InvalidLogicalCarrier(logical_type))
            );
        }
        let storage = ValueTypeStorageLifetime::pure();
        let json =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        let semantic = ValueTypeRef::try_new(
            ValueKind::List(json.value_type().clone()),
            true,
            storage.clone(),
        )
        .unwrap();
        let incompatible = ValueLayout::try_new(
            &DataType::List(Arc::new(Field::new("child", DataType::Int64, true))),
            storage,
        )
        .unwrap();
        assert!(
            FunctionValueType::try_from_value(semantic, incompatible).is_err(),
            "an incompatible complete child declaration cannot be published"
        );

        let source = DataType::List(Arc::new(
            Field::new("child", DataType::Int64, true).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                NR_LOGICAL_TYPE_KEY.into(),
                "json".into(),
            )])),
        ));
        let physical = FunctionValueType::new(source, true);
        let column = StatisticsScanColumn::try_new(0, "nested", physical).unwrap();
        assert_eq!(
            column
                .value_type()
                .value_type()
                .child_type_at(0)
                .unwrap()
                .kind(),
            &ValueKind::Scalar(DataType::Int64),
            "source attributes cannot replace the explicit physical declaration"
        );
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
            crate::connector::ConnectorStopOwner::new().view(),
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

#[cfg(test)]
mod retention_tests {
    use super::*;

    #[test]
    fn identity_clone_alone_retains_the_admitted_metadata_holder() {
        let holder = Arc::new(());
        let weak = Arc::downgrade(&holder);
        let draft = StatisticsArtifactDraft::try_new_with_guard(
            vec![1],
            "theta",
            Bytes::new(),
            BTreeMap::new(),
            ConnectorPayloadRetentionGuard::new(holder),
        )
        .unwrap();
        let identity = draft.identity().clone();
        drop(draft);
        assert!(weak.upgrade().is_some());
        assert_eq!(identity.input_fields(), [1]);
        drop(identity);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn artifact_body_slice_and_metadata_identity_outlive_draft_with_same_holder() {
        let holder = Arc::new(());
        let weak = Arc::downgrade(&holder);
        let draft = StatisticsArtifactDraft::try_new_with_guard(
            vec![1],
            "theta",
            Bytes::from_static(b"body"),
            BTreeMap::new(),
            ConnectorPayloadRetentionGuard::new(holder),
        )
        .unwrap();
        let plain = StatisticsArtifactDraft::try_new(
            vec![1],
            "theta",
            Bytes::from_static(b"body"),
            BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(draft, plain);
        assert_eq!(
            draft.identity().cmp(plain.identity()),
            std::cmp::Ordering::Equal
        );
        let hash = |id: &StatisticsArtifactIdentity| {
            let mut state = std::collections::hash_map::DefaultHasher::new();
            std::hash::Hash::hash(id, &mut state);
            std::hash::Hasher::finish(&state)
        };
        assert_eq!(hash(draft.identity()), hash(plain.identity()));
        let body = draft.body().slice(1..3);
        let identity = draft.identity().clone();
        let mut parts = draft.into_guarded_parts();
        parts.properties_mut().insert("ndv".into(), "1".into());
        let normalized = parts.try_into_draft().unwrap();
        let metadata = normalized.clone().into_guarded_parts();
        drop(normalized);
        drop(identity);
        drop(body);
        assert!(weak.upgrade().is_some());
        assert_eq!(metadata.properties().get("ndv").unwrap(), "1");
        drop(metadata);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn last_body_slice_releases_backing_before_capacity_holder() {
        struct Payload {
            data: Vec<u8>,
            events: Arc<Mutex<Vec<&'static str>>>,
        }
        impl AsRef<[u8]> for Payload {
            fn as_ref(&self) -> &[u8] {
                &self.data
            }
        }
        impl Drop for Payload {
            fn drop(&mut self) {
                self.events.lock().unwrap().push("payload");
            }
        }
        struct Holder(Arc<Mutex<Vec<&'static str>>>);
        impl Drop for Holder {
            fn drop(&mut self) {
                self.0.lock().unwrap().push("holder");
            }
        }
        let events = Arc::new(Mutex::new(Vec::new()));
        let body = Bytes::from_owner(Payload {
            data: vec![1, 2, 3],
            events: events.clone(),
        });
        let draft = StatisticsArtifactDraft::try_new_with_guard(
            vec![1],
            "theta",
            body,
            BTreeMap::new(),
            ConnectorPayloadRetentionGuard::new(Holder(events.clone())),
        )
        .unwrap();
        let alias = draft.body().slice(1..2);
        drop(draft);
        assert!(events.lock().unwrap().is_empty());
        drop(alias);
        assert_eq!(*events.lock().unwrap(), ["payload", "holder"]);
    }

    #[test]
    fn guarded_normalization_revalidates_metadata_with_original_errors() {
        let draft = StatisticsArtifactDraft::try_new_with_guard(
            vec![1],
            "theta",
            Bytes::new(),
            BTreeMap::new(),
            ConnectorPayloadRetentionGuard::new(()),
        )
        .unwrap();
        let mut parts = draft.into_guarded_parts();
        parts.properties_mut().insert("".into(), "x".into());
        let error = parts.try_into_draft().unwrap_err();
        let plain = StatisticsArtifactDraft::try_new(
            vec![1],
            "theta",
            Bytes::new(),
            BTreeMap::from([(String::new(), "x".into())]),
        )
        .unwrap_err();
        assert_eq!(error.kind(), plain.kind());
        assert_eq!(error.to_string(), plain.to_string());
    }
}
