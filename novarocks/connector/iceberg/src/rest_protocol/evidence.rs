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

//! Dispatch observations and the D13 evidence classification.
//!
//! The observation types below are shared by the HTTP layer, which records
//! where one send reached, and by the pure classifier, which turns an
//! observation plus a decoded minimal view into evidence. Observation never
//! interprets status codes; classification never performs I/O.

use std::fmt;

use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationEffect,
};
use reqwest::header::HeaderName;
use uuid::Uuid;

use super::json::ScanError;
use super::minimal_view::{
    MutationView, NamespaceView, PropertyValue, RefCheck, TableResponseView, TableViewRequest,
    ViewMutationView, ViewRequestError,
};
use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome};
use crate::catalog::transaction::CommitProof;

/// The establishment step that failed while acquiring the session binding.
///
/// It belongs to exactly one establishment attempt: every participant of that
/// attempt observes the same step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BindingFailure {
    /// The token endpoint answered with a 4xx status.
    TokenRejected,
    /// The token exchange failed in any other way.
    TokenUnavailable,
    /// `/v1/config` failed, or upstream never reached `catalog_session`.
    ConfigFetch,
    /// The merged configuration carried server-sent authentication material
    /// or authentication-class headers.
    InvalidServerConfig,
}

/// Why a request was never written to the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotWrittenCause {
    /// No session binding could be acquired.
    Binding(BindingFailure),
    /// The request URL could not be formed.
    Url,
    /// The request body could not be serialized.
    Serialize,
    /// The HTTP request could not be built.
    Build,
    /// The client failed while connecting (DNS, TCP, TLS, proxy tunnel,
    /// connect timeout).
    Connect,
}

/// Why a written request produced no status line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NoStatusCause {
    /// The explicitly granted phase deadline elapsed before headers arrived.
    DeadlineExceeded,
    /// The read timeout elapsed before a status line arrived.
    ReadTimeout,
    /// The connection closed before a status line arrived.
    Closed,
    /// Any other post-write transport failure.
    Other,
}

/// Where one send of one request reached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DispatchStage {
    NotWritten(NotWrittenCause),
    WrittenNoStatus(NoStatusCause),
    /// A status line arrived. Only header names are recorded: header values
    /// may carry credentials.
    Status {
        code: u16,
        header_names: Vec<HeaderName>,
    },
}

// ---------------------------------------------------------------------------
// Classification (spec 5.7)
// ---------------------------------------------------------------------------

/// A NovaRocks-issued request that may change catalog state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MutationEndpoint {
    /// Table commit against an existing table.
    TableCommit,
    /// The final `assert-create` commit of a CTAS staged create.
    CtasFinalCommit,
    /// Create table (static access only).
    CreateTable,
    /// Stage create (`stage-create: true`).
    StageCreate,
    CreateNamespace,
    CreateView,
    ReplaceView,
    DropTable,
    DropNamespace,
    DropView,
}

/// The minimal view a success response of an endpoint decodes to.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResponseShape {
    Table,
    Namespace,
    View,
}

impl MutationEndpoint {
    pub(crate) const ALL: [Self; 10] = [
        Self::TableCommit,
        Self::CtasFinalCommit,
        Self::CreateTable,
        Self::StageCreate,
        Self::CreateNamespace,
        Self::CreateView,
        Self::ReplaceView,
        Self::DropTable,
        Self::DropNamespace,
        Self::DropView,
    ];

    /// The primary success status of this endpoint: 204 for drops, 200
    /// otherwise. A drop also succeeds with a 200 whose body is empty
    /// ([`Self::is_drop`], L24 as revised 2026-10-14): Java REST servers before
    /// 1.11 answer drops that way, while a 200 with any body -- a gateway page,
    /// say -- stays unknown.
    pub(crate) fn success_status(self) -> u16 {
        match self {
            Self::DropTable | Self::DropNamespace | Self::DropView => 204,
            _ => 200,
        }
    }

    pub(crate) fn is_drop(self) -> bool {
        matches!(self, Self::DropTable | Self::DropNamespace | Self::DropView)
    }

    /// The statuses whose body the dispatch hands to the classifier: the
    /// primary success status, and a drop's empty-body 200.
    pub(crate) fn success_statuses(self) -> &'static [u16] {
        if self.is_drop() { &[204, 200] } else { &[200] }
    }

    /// Whether a 409 means "already exists" rather than a conflict.
    pub(crate) fn creates(self) -> bool {
        matches!(
            self,
            Self::CtasFinalCommit
                | Self::CreateTable
                | Self::StageCreate
                | Self::CreateNamespace
                | Self::CreateView
        )
    }

    /// The minimal view of the success response; `None` for drops, whose 204
    /// has no body.
    pub(crate) fn response_shape(self) -> Option<ResponseShape> {
        match self {
            Self::TableCommit | Self::CtasFinalCommit | Self::CreateTable | Self::StageCreate => {
                Some(ResponseShape::Table)
            }
            Self::CreateNamespace => Some(ResponseShape::Namespace),
            Self::CreateView | Self::ReplaceView => Some(ResponseShape::View),
            Self::DropTable | Self::DropNamespace | Self::DropView => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::TableCommit => "table commit",
            Self::CtasFinalCommit => "CTAS final commit",
            Self::CreateTable => "create table",
            Self::StageCreate => "stage create",
            Self::CreateNamespace => "create namespace",
            Self::CreateView => "create view",
            Self::ReplaceView => "replace view",
            Self::DropTable => "drop table",
            Self::DropNamespace => "drop namespace",
            Self::DropView => "drop view",
        }
    }
}

/// The snapshot a snapshot-producing commit adds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExpectedSnapshot {
    snapshot_id: i64,
    target_ref: Option<String>,
}

impl ExpectedSnapshot {
    /// `target_ref` is the ref the commit moves, recorded for diagnosis only:
    /// a refreshed response may already show it at a later snapshot.
    pub(crate) fn new(snapshot_id: i64, target_ref: Option<String>) -> Self {
        Self {
            snapshot_id,
            target_ref,
        }
    }

    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExpectedIdentity {
    /// Create table, stage and create view have no identity known in advance.
    Unknown,
    Uuid(Uuid),
    Namespace(Vec<String>),
}

/// Why an attribution expectation cannot be formed. Raised before dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpectationError {
    View(ViewRequestError),
}

impl fmt::Display for ExpectationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::View(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ExpectationError {}

impl From<ViewRequestError> for ExpectationError {
    fn from(error: ViewRequestError) -> Self {
        Self::View(error)
    }
}

/// What a success response must show to be attributable to this request
/// (spec 5.7 items 3-6), derived from the request's shape before dispatch.
///
/// The endpoint is part of the expectation, so the classifier can never pair
/// an observation with another endpoint's checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AttributionExpectation {
    endpoint: MutationEndpoint,
    identity: ExpectedIdentity,
    snapshot: Option<ExpectedSnapshot>,
    /// CTAS supplementary expectations: NovaRocks-named properties.
    properties: Vec<(String, String)>,
    /// What the table-response scan must retain; `Some` for table shapes.
    view_request: Option<TableViewRequest>,
    retention: Option<RetentionExpectation>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct RetentionExpectation {
    removed_snapshots: Vec<i64>,
    refs: Vec<(String, Option<i64>)>,
}

impl AttributionExpectation {
    fn new(
        endpoint: MutationEndpoint,
        identity: ExpectedIdentity,
        snapshot: Option<ExpectedSnapshot>,
        properties: Vec<(String, String)>,
    ) -> Result<Self, ExpectationError> {
        let view_request = match endpoint.response_shape() {
            Some(ResponseShape::Table) => Some(TableViewRequest::new(
                snapshot.as_ref().map(ExpectedSnapshot::snapshot_id),
                snapshot.as_ref().and_then(|s| s.target_ref.clone()),
                properties
                    .iter()
                    .map(|(key, value)| (key.clone(), value.len()))
                    .collect(),
            )?),
            _ => None,
        };
        Ok(Self {
            endpoint,
            identity,
            snapshot,
            properties,
            view_request,
            retention: None,
        })
    }

    fn infallible(endpoint: MutationEndpoint, identity: ExpectedIdentity) -> Self {
        let view_request = matches!(endpoint.response_shape(), Some(ResponseShape::Table))
            .then(TableViewRequest::default);
        Self {
            endpoint,
            identity,
            snapshot: None,
            properties: Vec::new(),
            view_request,
            retention: None,
        }
    }

    /// A commit to an existing table. The frozen table UUID is mandatory
    /// (L34). `snapshot` is `None` for a metadata-only commit, which checks
    /// items 1-4 only (item 7).
    pub(crate) fn table_commit(
        table_uuid: Uuid,
        snapshot: Option<ExpectedSnapshot>,
    ) -> Result<Self, ExpectationError> {
        Self::new(
            MutationEndpoint::TableCommit,
            ExpectedIdentity::Uuid(table_uuid),
            snapshot,
            Vec::new(),
        )
    }

    pub(crate) fn ref_retention(
        table_uuid: Uuid,
        updates: &[crate::iceberg::TableUpdate],
    ) -> Result<Self, ExpectationError> {
        let mut snapshots = std::collections::BTreeSet::new();
        let mut refs = std::collections::BTreeMap::new();
        for update in updates {
            match update {
                crate::iceberg::TableUpdate::RemoveSnapshots { snapshot_ids } => {
                    snapshots.extend(snapshot_ids.iter().copied())
                }
                crate::iceberg::TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                } => {
                    refs.insert(ref_name.clone(), Some(reference.snapshot_id));
                }
                crate::iceberg::TableUpdate::RemoveSnapshotRef { ref_name } => {
                    refs.insert(ref_name.clone(), None);
                }
                _ => {}
            }
        }
        let removed_snapshots = snapshots.into_iter().collect::<Vec<_>>();
        let refs = refs.into_iter().collect::<Vec<_>>();
        let mut expected = Self::table_commit(table_uuid, None)?;
        expected.view_request = Some(TableViewRequest::default().with_retention(
            removed_snapshots.clone(),
            refs.iter().map(|(name, _)| name.clone()).collect(),
        )?);
        expected.retention = Some(RetentionExpectation {
            removed_snapshots,
            refs,
        });
        Ok(expected)
    }

    /// The CTAS final commit: the UUID the stage froze, the snapshot it adds
    /// when it carries data, and the publication marker and document
    /// properties CTAS already checks.
    pub(crate) fn ctas_final_commit(
        staged_uuid: Uuid,
        snapshot: Option<ExpectedSnapshot>,
        properties: Vec<(String, String)>,
    ) -> Result<Self, ExpectationError> {
        Self::new(
            MutationEndpoint::CtasFinalCommit,
            ExpectedIdentity::Uuid(staged_uuid),
            snapshot,
            properties,
        )
    }

    pub(crate) fn create_table() -> Self {
        Self::infallible(MutationEndpoint::CreateTable, ExpectedIdentity::Unknown)
    }

    pub(crate) fn stage_create() -> Self {
        Self::infallible(MutationEndpoint::StageCreate, ExpectedIdentity::Unknown)
    }

    pub(crate) fn create_namespace(namespace: Vec<String>) -> Self {
        Self::infallible(
            MutationEndpoint::CreateNamespace,
            ExpectedIdentity::Namespace(namespace),
        )
    }

    pub(crate) fn create_view() -> Self {
        Self::infallible(MutationEndpoint::CreateView, ExpectedIdentity::Unknown)
    }

    pub(crate) fn replace_view(view_uuid: Uuid) -> Self {
        Self::infallible(
            MutationEndpoint::ReplaceView,
            ExpectedIdentity::Uuid(view_uuid),
        )
    }

    pub(crate) fn drop_table() -> Self {
        Self::infallible(MutationEndpoint::DropTable, ExpectedIdentity::Unknown)
    }

    pub(crate) fn drop_namespace() -> Self {
        Self::infallible(MutationEndpoint::DropNamespace, ExpectedIdentity::Unknown)
    }

    pub(crate) fn drop_view() -> Self {
        Self::infallible(MutationEndpoint::DropView, ExpectedIdentity::Unknown)
    }

    pub(crate) fn endpoint(&self) -> MutationEndpoint {
        self.endpoint
    }

    /// The request a table-shaped success response is scanned with
    /// (`TableResponseSink::new`). `None` for the other shapes.
    pub(crate) fn table_view_request(&self) -> Option<&TableViewRequest> {
        self.view_request.as_ref()
    }

    fn expected_uuid(&self) -> Option<Uuid> {
        match self.identity {
            ExpectedIdentity::Uuid(uuid) => Some(uuid),
            _ => None,
        }
    }
}

/// How a post-status body read ended.
pub(crate) enum BodyRead<V> {
    /// The whole body was read and scanned into a complete, valid document.
    Complete(V),
    Failed(BodyReadFailure),
    /// The body was not read (drops, or a caller that never read it).
    NotRead,
    /// The whole body was read and it was empty (a drop answered 200).
    Empty,
}

/// Why a success body could not be read to the end as one valid document.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyReadFailure {
    ReadTimeout,
    Closed,
    Cancelled,
    DeadlineExceeded,
    /// Any other transport failure while reading.
    Transport,
    Scan(ScanError),
}

/// Why a request was not dispatched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotDispatchedCause {
    NotWritten(NotWrittenCause),
    /// A commit to an existing table carried no frozen table UUID (L34); the
    /// transitional adapter refuses it before dispatch.
    UnfrozenTableUuid,
}

/// Relation of the target ref to the expected snapshot, for diagnosis only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RefRelation {
    /// No snapshot or no target ref was expected.
    NotTracked,
    /// The response does not show the ref, or the ref has no snapshot.
    Absent,
    AtExpectedSnapshot,
    /// The ref is at another snapshot: a refreshed response, or a later
    /// commit moved it.
    AtOtherSnapshot,
}

/// Items 2-4 failed: the response is not this object's, or not readable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AnomalyCause {
    /// A body-bearing success status whose body was never read.
    BodyNotRead,
    /// A drop answered 200 with a non-empty body (L24).
    NonEmptyDropBody,
    Read(BodyReadFailure),
    /// The decoded view does not match the endpoint or the expectation it
    /// was scanned for.
    ViewMismatch,
    MissingMetadataLocation,
    /// A stage response carried a metadata location.
    UnexpectedMetadataLocation,
    MissingUuid,
    MalformedUuid,
    UuidMismatch,
    MissingNamespace,
    NamespaceMismatch,
}

/// Items 5-6 failed: a server defect or a refreshed response that later
/// commits rewrote; the two cannot be told apart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UnattributableCause {
    SnapshotAbsent,
    RetentionPostconditionMismatch,
    /// A NovaRocks-named property is absent or differs.
    PropertyMismatch {
        key: String,
    },
}

/// Why the outcome is unknown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum UnknownReason {
    ProtocolAnomaly(AnomalyCause),
    Unattributable(UnattributableCause),
    /// 400 or 404: the server may have failed after publishing.
    PostPublishPossible,
    /// Any other non-success status, including 3xx and non-success 2xx.
    Status,
    NoStatus(NoStatusCause),
}

/// Diagnosis of an unknown outcome. Holds only dispatch facts, header names
/// and NovaRocks-owned expectations: never response values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct UnknownDiagnosis {
    pub(crate) endpoint: MutationEndpoint,
    pub(crate) reason: UnknownReason,
    pub(crate) status: Option<u16>,
    pub(crate) header_names: Vec<HeaderName>,
    pub(crate) expected_uuid: Option<Uuid>,
    pub(crate) expected_snapshot: Option<i64>,
    pub(crate) ref_relation: RefRelation,
}

/// What an attributable success hands back.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct RestReceipt {
    pub(crate) endpoint: MutationEndpoint,
    /// The snapshot this request added, when it adds one.
    pub(crate) snapshot_id: Option<i64>,
    /// The table or view UUID the response carried (equal to the frozen one
    /// when one was expected).
    pub(crate) uuid: Option<Uuid>,
    /// Diagnosis and IRU-6a only: in a refreshed response it may belong to a
    /// later version.
    pub(crate) metadata_location: Option<String>,
    pub(crate) metadata_predecessor: crate::catalog::transaction::MetadataPredecessorObservation,
    pub(crate) ref_relation: RefRelation,
}

impl RestReceipt {
    fn bare(endpoint: MutationEndpoint) -> Self {
        Self {
            endpoint,
            snapshot_id: None,
            uuid: None,
            metadata_location: None,
            metadata_predecessor: Default::default(),
            ref_relation: RefRelation::NotTracked,
        }
    }
}

impl fmt::Debug for RestReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestReceipt")
            .field("endpoint", &self.endpoint)
            .field("snapshot_id", &self.snapshot_id)
            .field("uuid", &self.uuid)
            .field(
                "metadata_location_present",
                &self.metadata_location.is_some(),
            )
            .field("ref_relation", &self.ref_relation)
            .finish()
    }
}

/// The evidence of one REST mutation request (D13).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RestEvidence {
    NotDispatched {
        kind: ConnectorMutationFailureKind,
        cause: NotDispatchedCause,
    },
    /// 409, 401, 403 or 419: refused before publication.
    Rejected {
        status: u16,
        kind: ConnectorMutationFailureKind,
    },
    Applied {
        receipt: RestReceipt,
    },
    Unknown {
        diagnosis: UnknownDiagnosis,
    },
}

/// Classify one dispatch of one mutation request (spec 5.7).
///
/// `read` is consulted only when the status is the endpoint's success status
/// and the endpoint has a response body.
// Design: ADR-0175 (docs/adr/ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md)
pub(crate) fn classify(
    stage: &DispatchStage,
    read: BodyRead<MutationView>,
    expect: &AttributionExpectation,
) -> RestEvidence {
    let endpoint = expect.endpoint;
    match stage {
        DispatchStage::NotWritten(cause) => RestEvidence::NotDispatched {
            kind: not_written_kind(*cause),
            cause: NotDispatchedCause::NotWritten(*cause),
        },
        DispatchStage::WrittenNoStatus(cause) => unknown(
            expect,
            UnknownReason::NoStatus(*cause),
            None,
            &[],
            RefRelation::NotTracked,
        ),
        DispatchStage::Status { code, header_names } => {
            let code = *code;
            if code == endpoint.success_status() {
                return attribute(expect, code, header_names, read);
            }
            if code == 200 && endpoint.is_drop() {
                return match read {
                    BodyRead::Empty => RestEvidence::Applied {
                        receipt: RestReceipt::bare(endpoint),
                    },
                    _ => unknown(
                        expect,
                        UnknownReason::ProtocolAnomaly(AnomalyCause::NonEmptyDropBody),
                        Some(code),
                        header_names,
                        RefRelation::NotTracked,
                    ),
                };
            }
            let rejected = |kind| RestEvidence::Rejected { status: code, kind };
            match code {
                409 if endpoint.creates() => rejected(ConnectorMutationFailureKind::AlreadyExists),
                409 => rejected(ConnectorMutationFailureKind::Conflict),
                401 | 419 => rejected(ConnectorMutationFailureKind::Unauthenticated),
                403 => rejected(ConnectorMutationFailureKind::PermissionDenied),
                413 => rejected(ConnectorMutationFailureKind::ResourceExhausted),
                400 | 404 => unknown(
                    expect,
                    UnknownReason::PostPublishPossible,
                    Some(code),
                    header_names,
                    RefRelation::NotTracked,
                ),
                _ => unknown(
                    expect,
                    UnknownReason::Status,
                    Some(code),
                    header_names,
                    RefRelation::NotTracked,
                ),
            }
        }
    }
}

/// The failure kind of a request that was never written.
pub(crate) fn not_written_kind(cause: NotWrittenCause) -> ConnectorMutationFailureKind {
    match cause {
        NotWrittenCause::Binding(BindingFailure::TokenRejected) => {
            ConnectorMutationFailureKind::Unauthenticated
        }
        NotWrittenCause::Binding(
            BindingFailure::TokenUnavailable | BindingFailure::ConfigFetch,
        ) => ConnectorMutationFailureKind::Unavailable,
        NotWrittenCause::Binding(BindingFailure::InvalidServerConfig) => {
            ConnectorMutationFailureKind::Unsupported
        }
        NotWrittenCause::Url | NotWrittenCause::Serialize | NotWrittenCause::Build => {
            ConnectorMutationFailureKind::Internal
        }
        NotWrittenCause::Connect => ConnectorMutationFailureKind::Unavailable,
    }
}

fn unknown(
    expect: &AttributionExpectation,
    reason: UnknownReason,
    status: Option<u16>,
    header_names: &[HeaderName],
    ref_relation: RefRelation,
) -> RestEvidence {
    RestEvidence::Unknown {
        diagnosis: UnknownDiagnosis {
            endpoint: expect.endpoint,
            reason,
            status,
            header_names: header_names.to_vec(),
            expected_uuid: expect.expected_uuid(),
            expected_snapshot: expect.snapshot.as_ref().map(ExpectedSnapshot::snapshot_id),
            ref_relation,
        },
    }
}

/// A success status arrived: apply attribution items 1-7.
fn attribute(
    expect: &AttributionExpectation,
    code: u16,
    header_names: &[HeaderName],
    read: BodyRead<MutationView>,
) -> RestEvidence {
    // Item 1 holds by construction: the client never follows redirects.
    let Some(shape) = expect.endpoint.response_shape() else {
        // Drops: a 204 has no body; item 1 is the whole check.
        return RestEvidence::Applied {
            receipt: RestReceipt::bare(expect.endpoint),
        };
    };
    let anomaly = |cause, ref_relation| {
        unknown(
            expect,
            UnknownReason::ProtocolAnomaly(cause),
            Some(code),
            header_names,
            ref_relation,
        )
    };
    // Item 2: the whole body is one complete, valid document.
    let view = match read {
        BodyRead::Complete(view) => view,
        BodyRead::Failed(failure) => {
            return anomaly(AnomalyCause::Read(failure), RefRelation::NotTracked);
        }
        BodyRead::NotRead | BodyRead::Empty => {
            return anomaly(AnomalyCause::BodyNotRead, RefRelation::NotTracked);
        }
    };
    let checked = match (shape, &view) {
        (ResponseShape::Table, MutationView::Table(view)) => attribute_table(expect, view),
        (ResponseShape::Namespace, MutationView::Namespace(view)) => {
            attribute_namespace(expect, view)
        }
        (ResponseShape::View, MutationView::View(view)) => attribute_view(expect, view),
        _ => Err(Failed::Anomaly(AnomalyCause::ViewMismatch)),
    };
    match checked {
        Ok(receipt) => RestEvidence::Applied { receipt },
        Err(Failed::Anomaly(cause)) => anomaly(cause, RefRelation::NotTracked),
        Err(Failed::AnomalyWith(cause, relation)) => anomaly(cause, relation),
        Err(Failed::Unattributable(cause, relation)) => unknown(
            expect,
            UnknownReason::Unattributable(cause),
            Some(code),
            header_names,
            relation,
        ),
    }
}

enum Failed {
    Anomaly(AnomalyCause),
    AnomalyWith(AnomalyCause, RefRelation),
    Unattributable(UnattributableCause, RefRelation),
}

fn parse_uuid(text: Option<&str>) -> Result<Uuid, AnomalyCause> {
    let text = text.ok_or(AnomalyCause::MissingUuid)?;
    Uuid::parse_str(text).map_err(|_| AnomalyCause::MalformedUuid)
}

fn ref_relation(expected: Option<&ExpectedSnapshot>, observed: Option<&RefCheck>) -> RefRelation {
    match (expected, observed) {
        (Some(expected), Some(observed)) => match observed.snapshot_id {
            None => RefRelation::Absent,
            Some(id) if id == expected.snapshot_id => RefRelation::AtExpectedSnapshot,
            Some(_) => RefRelation::AtOtherSnapshot,
        },
        _ => RefRelation::NotTracked,
    }
}

fn attribute_table(
    expect: &AttributionExpectation,
    view: &TableResponseView,
) -> Result<RestReceipt, Failed> {
    let relation = ref_relation(expect.snapshot.as_ref(), view.target_ref.as_ref());
    let anomaly = |cause| Failed::AnomalyWith(cause, relation);
    // The view must have been scanned for exactly this expectation.
    let request = expect
        .view_request
        .as_ref()
        .ok_or(anomaly(AnomalyCause::ViewMismatch))?;
    let scanned_keys = view.properties.iter().map(|(key, _)| key);
    if view.snapshot.map(|check| check.snapshot_id) != request.snapshot()
        || view.target_ref.as_ref().map(|check| check.name.as_str()) != request.target_ref()
        || !scanned_keys.eq(request.property_keys().iter())
        || !view
            .retention_snapshots
            .iter()
            .map(|check| check.snapshot_id)
            .eq(request.removed_snapshots().iter().copied())
        || !view
            .retention_refs
            .iter()
            .map(|(name, _, _)| name)
            .eq(request.retention_refs().iter())
    {
        return Err(anomaly(AnomalyCause::ViewMismatch));
    }
    // Item 3: the shape decodes.
    if expect.endpoint == MutationEndpoint::StageCreate {
        if !view.metadata_location.is_unset() {
            return Err(anomaly(AnomalyCause::UnexpectedMetadataLocation));
        }
    } else if view.metadata_location.non_empty().is_none() {
        return Err(anomaly(AnomalyCause::MissingMetadataLocation));
    }
    let uuid = parse_uuid(view.table_uuid.as_deref()).map_err(anomaly)?;
    // Item 4: identity.
    if expect
        .expected_uuid()
        .is_some_and(|expected| expected != uuid)
    {
        return Err(anomaly(AnomalyCause::UuidMismatch));
    }
    // Item 5: this request's snapshot is in the returned metadata. Where the
    // target ref points does not decide: the response may be refreshed.
    if view.snapshot.is_some_and(|check| !check.present) {
        return Err(Failed::Unattributable(
            UnattributableCause::SnapshotAbsent,
            relation,
        ));
    }
    if let Some(retention) = &expect.retention
        && (view
            .retention_snapshots
            .iter()
            .any(|snapshot| snapshot.present)
            || retention.refs.iter().zip(&view.retention_refs).any(
                |((_, expected), (_, present, observed))| match expected {
                    Some(snapshot) => !*present || observed != &Some(*snapshot),
                    None => *present,
                },
            ))
    {
        return Err(Failed::Unattributable(
            UnattributableCause::RetentionPostconditionMismatch,
            relation,
        ));
    }
    // Item 6: CTAS supplementary expectations.
    for ((key, expected), (_, observed)) in expect.properties.iter().zip(&view.properties) {
        let matches = matches!(
            observed,
            PropertyValue::Text(actual) if actual == expected
        );
        if !matches {
            return Err(Failed::Unattributable(
                UnattributableCause::PropertyMismatch { key: key.clone() },
                relation,
            ));
        }
    }
    Ok(RestReceipt {
        endpoint: expect.endpoint,
        snapshot_id: expect.snapshot.as_ref().map(ExpectedSnapshot::snapshot_id),
        uuid: Some(uuid),
        metadata_location: view.metadata_location.non_empty().map(str::to_owned),
        metadata_predecessor: view.metadata_predecessor.clone(),
        ref_relation: relation,
    })
}

fn attribute_namespace(
    expect: &AttributionExpectation,
    view: &NamespaceView,
) -> Result<RestReceipt, Failed> {
    let observed = view
        .namespace
        .as_ref()
        .ok_or(Failed::Anomaly(AnomalyCause::MissingNamespace))?;
    match &expect.identity {
        ExpectedIdentity::Namespace(expected) if expected == observed => {
            Ok(RestReceipt::bare(expect.endpoint))
        }
        _ => Err(Failed::Anomaly(AnomalyCause::NamespaceMismatch)),
    }
}

fn attribute_view(
    expect: &AttributionExpectation,
    view: &ViewMutationView,
) -> Result<RestReceipt, Failed> {
    let location = view
        .metadata_location
        .non_empty()
        .ok_or(Failed::Anomaly(AnomalyCause::MissingMetadataLocation))?;
    let uuid = parse_uuid(view.view_uuid.as_deref()).map_err(Failed::Anomaly)?;
    if expect
        .expected_uuid()
        .is_some_and(|expected| expected != uuid)
    {
        return Err(Failed::Anomaly(AnomalyCause::UuidMismatch));
    }
    Ok(RestReceipt {
        uuid: Some(uuid),
        metadata_location: Some(location.to_owned()),
        ..RestReceipt::bare(expect.endpoint)
    })
}

// ---------------------------------------------------------------------------
// Messages and mapping to IRU-5 types
// ---------------------------------------------------------------------------

fn not_dispatched_message(cause: NotDispatchedCause) -> &'static str {
    match cause {
        NotDispatchedCause::NotWritten(NotWrittenCause::Binding(BindingFailure::TokenRejected)) => {
            "REST catalog request was not dispatched: the token endpoint rejected the token exchange"
        }
        NotDispatchedCause::NotWritten(NotWrittenCause::Binding(
            BindingFailure::TokenUnavailable,
        )) => "REST catalog request was not dispatched: the token exchange failed",
        NotDispatchedCause::NotWritten(NotWrittenCause::Binding(BindingFailure::ConfigFetch)) => {
            "REST catalog request was not dispatched: the catalog session could not be established"
        }
        NotDispatchedCause::NotWritten(NotWrittenCause::Binding(
            BindingFailure::InvalidServerConfig,
        )) => {
            "REST catalog request was not dispatched: the server configuration carries \
             authentication material; authentication is only accepted through the \
             CatalogControl or DataCredentialVending identity"
        }
        NotDispatchedCause::NotWritten(NotWrittenCause::Url) => {
            "REST catalog request was not dispatched: the request URL could not be formed"
        }
        NotDispatchedCause::NotWritten(NotWrittenCause::Serialize) => {
            "REST catalog request was not dispatched: the request body could not be serialized"
        }
        NotDispatchedCause::NotWritten(NotWrittenCause::Build) => {
            "REST catalog request was not dispatched: the request could not be built"
        }
        NotDispatchedCause::NotWritten(NotWrittenCause::Connect) => {
            "REST catalog request was not dispatched: the connection could not be established"
        }
        NotDispatchedCause::UnfrozenTableUuid => {
            "REST catalog request was not dispatched: a commit to an existing table must \
             freeze the table UUID"
        }
    }
}

impl RestEvidence {
    /// A fixed, server-text-free message for this evidence.
    pub(crate) fn message(&self) -> String {
        match self {
            Self::NotDispatched { cause, .. } => not_dispatched_message(*cause).to_owned(),
            Self::Rejected { status, .. } => {
                format!("REST catalog rejected the request with HTTP {status}")
            }
            Self::Applied { receipt } => {
                format!("REST catalog applied the {}", receipt.endpoint.label())
            }
            Self::Unknown { diagnosis } => {
                let target = diagnosis.endpoint.label();
                match (&diagnosis.reason, diagnosis.status) {
                    (UnknownReason::NoStatus(NoStatusCause::ReadTimeout), _) => format!(
                        "REST catalog {target} timed out after the request was written; \
                         the outcome is unknown"
                    ),
                    (UnknownReason::NoStatus(NoStatusCause::Closed), _) => format!(
                        "REST catalog connection closed after the {target} request was \
                         written; the outcome is unknown"
                    ),
                    (UnknownReason::NoStatus(NoStatusCause::Other), _) => format!(
                        "REST catalog {target} failed after the request was written; \
                         the outcome is unknown"
                    ),
                    (UnknownReason::PostPublishPossible, Some(status)) => format!(
                        "REST catalog answered the {target} with HTTP {status}; the server \
                         may have failed after publishing, so the outcome is unknown"
                    ),
                    (UnknownReason::ProtocolAnomaly(_), Some(status)) => format!(
                        "REST catalog answered the {target} with HTTP {status}, but the \
                         response is incomplete or does not belong to the target; the \
                         outcome is unknown"
                    ),
                    (UnknownReason::Unattributable(_), Some(status)) => format!(
                        "REST catalog answered the {target} with HTTP {status}, but the \
                         response does not show this request's change; the outcome is unknown"
                    ),
                    (_, Some(status)) => format!(
                        "REST catalog answered the {target} with HTTP {status}; the outcome \
                         is unknown"
                    ),
                    (_, None) => format!("REST catalog {target} outcome is unknown"),
                }
            }
        }
    }

    /// The neutral failure: `None` only for `Applied`. Unknown outcomes use
    /// `Unavailable`, as [`CatalogOutcome::unknown`] does.
    pub(crate) fn failure(&self) -> Option<ConnectorMutationFailure> {
        let kind = match self {
            Self::NotDispatched { kind, .. } | Self::Rejected { kind, .. } => *kind,
            Self::Unknown { .. } => ConnectorMutationFailureKind::Unavailable,
            Self::Applied { .. } => return None,
        };
        Some(ConnectorMutationFailure::new(kind, self.message()))
    }

    /// Map to the IRU-5 outcome. Applied yields the receipt "snapshot S +
    /// table UUID"; the metadata location stays out of the proof (L11).
    /// `evidence` carries today's adjudication facts for an unknown outcome.
    pub(crate) fn into_catalog_outcome(
        self,
        evidence: CatalogCommitEvidence,
    ) -> CatalogOutcome<CommitProof> {
        let message = self.message();
        match self {
            Self::Applied { receipt } => {
                let proof = CommitProof::applied(receipt.snapshot_id);
                let proof = match receipt.uuid {
                    Some(uuid) => proof.with_table_uuid(uuid.to_string()),
                    None => proof,
                };
                CatalogOutcome::committed(proof, ExternalMutationEffect::Applied)
            }
            Self::NotDispatched { kind, .. } | Self::Rejected { kind, .. } => {
                CatalogOutcome::uncommitted(kind, message)
            }
            Self::Unknown { .. } => CatalogOutcome::unknown(message, evidence),
        }
    }

    /// Map a stage-create evidence to its saga branch (L19).
    #[cfg(test)]
    pub(crate) fn into_stage_verdict(self) -> StageVerdict {
        match split(self) {
            Split::Applied(receipt) => StageVerdict::Staged { receipt },
            Split::Conflict(failure) => StageVerdict::Conflict(failure),
            Split::Uncommitted(failure) => StageVerdict::KnownUncommitted(failure),
            Split::Unknown(failure, diagnosis) => {
                StageVerdict::CommitUnknown { failure, diagnosis }
            }
        }
    }

    /// Map a CTAS final-commit evidence to its publication branch.
    #[cfg(test)]
    pub(crate) fn into_ctas_commit_verdict(self) -> CtasCommitVerdict {
        match split(self) {
            Split::Applied(receipt) => CtasCommitVerdict::Committed { receipt },
            Split::Conflict(failure) => CtasCommitVerdict::Conflict(failure),
            Split::Uncommitted(failure) => CtasCommitVerdict::KnownUncommitted(failure),
            Split::Unknown(failure, diagnosis) => {
                CtasCommitVerdict::CommitUnknown { failure, diagnosis }
            }
        }
    }
}

enum Split {
    Applied(RestReceipt),
    Conflict(ConnectorMutationFailure),
    Uncommitted(ConnectorMutationFailure),
    Unknown(ConnectorMutationFailure, UnknownDiagnosis),
}

fn split(evidence: RestEvidence) -> Split {
    let message = evidence.message();
    match evidence {
        RestEvidence::Applied { receipt } => Split::Applied(receipt),
        RestEvidence::Rejected { status: 409, kind } => {
            Split::Conflict(ConnectorMutationFailure::new(kind, message))
        }
        RestEvidence::Rejected { kind, .. } | RestEvidence::NotDispatched { kind, .. } => {
            Split::Uncommitted(ConnectorMutationFailure::new(kind, message))
        }
        RestEvidence::Unknown { diagnosis } => Split::Unknown(
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Unavailable, message),
            diagnosis,
        ),
    }
}

/// The saga branch of a stage create (L19): not dispatched and 401/403/419
/// are known uncommitted, 409 is a conflict (kind `AlreadyExists`), an
/// attributable success is staged, everything else -- 400 and 404 included --
/// is unknown.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) enum StageVerdict {
    Staged {
        receipt: RestReceipt,
    },
    Conflict(ConnectorMutationFailure),
    KnownUncommitted(ConnectorMutationFailure),
    CommitUnknown {
        failure: ConnectorMutationFailure,
        diagnosis: UnknownDiagnosis,
    },
}

/// The publication branch of the CTAS final commit.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(test)]
pub(crate) enum CtasCommitVerdict {
    Committed {
        receipt: RestReceipt,
    },
    /// 409; the kind is `AlreadyExists`: the create is not retried.
    Conflict(ConnectorMutationFailure),
    KnownUncommitted(ConnectorMutationFailure),
    CommitUnknown {
        failure: ConnectorMutationFailure,
        diagnosis: UnknownDiagnosis,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::rest_protocol::json::scan_complete;
    use crate::rest_protocol::minimal_view::{NamespaceSink, TableResponseSink, ViewMutationSink};

    const TABLE_UUID: &str = "9c12d441-03fe-4693-9a96-a0705ddf69c1";
    const OTHER_UUID: &str = "1b4e28ba-2fa1-11d2-883f-0016d3cca427";
    const S: i64 = 6;
    const LATER: i64 = 7;
    const MARKER: &str = "novarocks.ctas.operation-id";
    const SECRET: &str = "SECRET-token-from-server";

    use ConnectorMutationFailureKind as Kind;
    use MutationEndpoint as E;

    fn uuid(text: &str) -> Uuid {
        Uuid::parse_str(text).unwrap()
    }

    fn snapshot() -> Option<ExpectedSnapshot> {
        Some(ExpectedSnapshot::new(S, Some("main".to_owned())))
    }

    fn expectation(endpoint: MutationEndpoint) -> AttributionExpectation {
        match endpoint {
            E::TableCommit => {
                AttributionExpectation::table_commit(uuid(TABLE_UUID), snapshot()).unwrap()
            }
            E::CtasFinalCommit => AttributionExpectation::ctas_final_commit(
                uuid(TABLE_UUID),
                snapshot(),
                vec![(MARKER.to_owned(), "op-7".to_owned())],
            )
            .unwrap(),
            E::CreateTable => AttributionExpectation::create_table(),
            E::StageCreate => AttributionExpectation::stage_create(),
            E::CreateNamespace => {
                AttributionExpectation::create_namespace(vec!["db".to_owned(), "inner".to_owned()])
            }
            E::CreateView => AttributionExpectation::create_view(),
            E::ReplaceView => AttributionExpectation::replace_view(uuid(TABLE_UUID)),
            E::DropTable => AttributionExpectation::drop_table(),
            E::DropNamespace => AttributionExpectation::drop_namespace(),
            E::DropView => AttributionExpectation::drop_view(),
        }
    }

    /// A table response. `location` is the raw member text (or empty).
    fn table_body(
        location: &str,
        table_uuid: &str,
        snapshots: &[i64],
        main: Option<i64>,
    ) -> String {
        let snapshots = snapshots
            .iter()
            .map(|id| format!(r#"{{"snapshot-id":{id},"summary":{{"operation":"append"}}}}"#))
            .collect::<Vec<_>>()
            .join(",");
        let refs = main
            .map(|id| format!(r#""refs":{{"main":{{"snapshot-id":{id},"type":"branch"}}}},"#))
            .unwrap_or_default();
        format!(
            r#"{{{location}"metadata":{{"format-version":2,"table-uuid":"{table_uuid}",
               "snapshots":[{snapshots}],{refs}
               "properties":{{"{MARKER}":"op-7"}}}},"config":{{}}}}"#
        )
    }

    fn good_body(endpoint: MutationEndpoint) -> Option<String> {
        let located = r#""metadata-location":"s3://b/t/metadata/00002.json","#;
        match endpoint {
            E::TableCommit | E::CtasFinalCommit => {
                Some(table_body(located, TABLE_UUID, &[5, S], Some(S)))
            }
            E::CreateTable => Some(table_body(located, OTHER_UUID, &[], None)),
            E::StageCreate => Some(table_body("", OTHER_UUID, &[], None)),
            E::CreateNamespace => {
                Some(r#"{"namespace":["db","inner"],"properties":{}}"#.to_owned())
            }
            E::CreateView | E::ReplaceView => Some(format!(
                r#"{{"metadata-location":"s3://v/m.json","metadata":{{"view-uuid":"{TABLE_UUID}"}}}}"#
            )),
            E::DropTable | E::DropNamespace | E::DropView => None,
        }
    }

    /// Scan `body` with the sink the endpoint's success response uses.
    fn read(expect: &AttributionExpectation, body: &str) -> BodyRead<MutationView> {
        let bytes = body.as_bytes();
        let scanned = match expect.endpoint().response_shape() {
            Some(ResponseShape::Table) => scan_complete(
                TableResponseSink::new(expect.table_view_request().unwrap().clone()),
                bytes,
            )
            .map(MutationView::Table),
            Some(ResponseShape::Namespace) => {
                scan_complete(NamespaceSink::new(), bytes).map(MutationView::Namespace)
            }
            Some(ResponseShape::View) | None => {
                scan_complete(ViewMutationSink::new(), bytes).map(MutationView::View)
            }
        };
        match scanned {
            Ok(view) => BodyRead::Complete(view),
            Err(error) => BodyRead::Failed(BodyReadFailure::Scan(error)),
        }
    }

    fn status(code: u16) -> DispatchStage {
        DispatchStage::Status {
            code,
            header_names: vec![
                HeaderName::from_static("content-type"),
                HeaderName::from_static("x-request-id"),
            ],
        }
    }

    fn classify_body(
        expect: &AttributionExpectation,
        code: u16,
        body: Option<&str>,
    ) -> RestEvidence {
        let read = match body {
            Some(body) => read(expect, body),
            None => BodyRead::NotRead,
        };
        classify(&status(code), read, expect)
    }

    fn success(endpoint: MutationEndpoint) -> RestEvidence {
        let expect = expectation(endpoint);
        classify_body(
            &expect,
            endpoint.success_status(),
            good_body(endpoint).as_deref(),
        )
    }

    fn unknown_reason(evidence: &RestEvidence) -> &UnknownReason {
        match evidence {
            RestEvidence::Unknown { diagnosis } => &diagnosis.reason,
            other => panic!("expected unknown evidence, got {other:?}"),
        }
    }

    fn assert_anomaly(evidence: &RestEvidence, cause: AnomalyCause) {
        assert_eq!(
            unknown_reason(evidence),
            &UnknownReason::ProtocolAnomaly(cause),
            "{evidence:?}"
        );
    }

    #[test]
    fn success_statuses_follow_the_endpoint_table() {
        for endpoint in E::ALL {
            let expected = match endpoint {
                E::DropTable | E::DropNamespace | E::DropView => 204,
                _ => 200,
            };
            assert_eq!(endpoint.success_status(), expected, "{endpoint:?}");
        }
    }

    #[test]
    fn not_written_is_not_dispatched_on_every_endpoint() {
        let causes = [
            (
                NotWrittenCause::Binding(BindingFailure::TokenRejected),
                Kind::Unauthenticated,
            ),
            (
                NotWrittenCause::Binding(BindingFailure::TokenUnavailable),
                Kind::Unavailable,
            ),
            (
                NotWrittenCause::Binding(BindingFailure::ConfigFetch),
                Kind::Unavailable,
            ),
            (
                NotWrittenCause::Binding(BindingFailure::InvalidServerConfig),
                Kind::Unsupported,
            ),
            (NotWrittenCause::Url, Kind::Internal),
            (NotWrittenCause::Serialize, Kind::Internal),
            (NotWrittenCause::Build, Kind::Internal),
            (NotWrittenCause::Connect, Kind::Unavailable),
        ];
        for endpoint in E::ALL {
            for (cause, kind) in causes {
                let evidence = classify(
                    &DispatchStage::NotWritten(cause),
                    BodyRead::NotRead,
                    &expectation(endpoint),
                );
                assert_eq!(
                    evidence,
                    RestEvidence::NotDispatched {
                        kind,
                        cause: NotDispatchedCause::NotWritten(cause)
                    },
                    "{endpoint:?} {cause:?}"
                );
            }
        }
    }

    #[test]
    fn rejected_status_set_on_every_endpoint() {
        for endpoint in E::ALL {
            let expect = expectation(endpoint);
            let conflict = if matches!(
                endpoint,
                E::CtasFinalCommit
                    | E::CreateTable
                    | E::StageCreate
                    | E::CreateNamespace
                    | E::CreateView
            ) {
                Kind::AlreadyExists
            } else {
                Kind::Conflict
            };
            for (code, kind) in [
                (409, conflict),
                (401, Kind::Unauthenticated),
                (419, Kind::Unauthenticated),
                (403, Kind::PermissionDenied),
                (413, Kind::ResourceExhausted),
            ] {
                // A body is never consulted for a non-success status.
                let evidence = classify_body(&expect, code, Some("<html>"));
                assert_eq!(
                    evidence,
                    RestEvidence::Rejected { status: code, kind },
                    "{endpoint:?} {code}"
                );
                assert_eq!(
                    evidence.message(),
                    format!("REST catalog rejected the request with HTTP {code}")
                );
            }
        }
    }

    #[test]
    fn bad_request_and_not_found_may_follow_publication() {
        for endpoint in E::ALL {
            for code in [400, 404] {
                let evidence = classify_body(&expectation(endpoint), code, None);
                let RestEvidence::Unknown { diagnosis } = &evidence else {
                    panic!("{endpoint:?} {code}: {evidence:?}");
                };
                assert_eq!(diagnosis.reason, UnknownReason::PostPublishPossible);
                assert_eq!(diagnosis.status, Some(code));
                assert_eq!(diagnosis.header_names.len(), 2);
            }
        }
    }

    #[test]
    fn every_other_status_is_unknown() {
        let others = [
            500, 501, 502, 503, 504, 505, 408, 415, 429, 201, 202, 301, 302, 303, 307, 308,
        ];
        for endpoint in E::ALL {
            let expect = expectation(endpoint);
            for code in others {
                let evidence = classify_body(&expect, code, good_body(endpoint).as_deref());
                assert_eq!(
                    unknown_reason(&evidence),
                    &UnknownReason::Status,
                    "{endpoint:?} {code}"
                );
            }
            // A table commit's 204 and a drop's 200 (HTML or otherwise) are
            // not their success status.
            let wrong_success = if endpoint.success_status() == 204 {
                200
            } else {
                204
            };
            for body in [
                None,
                Some("<html>ok</html>"),
                good_body(E::TableCommit).as_deref(),
            ] {
                let evidence = classify_body(&expect, wrong_success, body);
                // A drop's 200 is judged by its body (L24 as revised): any
                // body it was not shown as empty is a protocol anomaly.
                let expected = if endpoint.is_drop() {
                    UnknownReason::ProtocolAnomaly(AnomalyCause::NonEmptyDropBody)
                } else {
                    UnknownReason::Status
                };
                assert_eq!(unknown_reason(&evidence), &expected, "{endpoint:?}");
            }
        }
    }

    #[test]
    fn a_drop_answered_with_an_empty_200_is_applied() {
        for endpoint in E::ALL.into_iter().filter(|endpoint| endpoint.is_drop()) {
            let expect = expectation(endpoint);
            let evidence = classify(&status(200), BodyRead::Empty, &expect);
            assert!(
                matches!(evidence, RestEvidence::Applied { .. }),
                "{endpoint:?}: {evidence:?}"
            );
        }
    }

    #[test]
    fn written_without_status_is_unknown() {
        for endpoint in E::ALL {
            for cause in [
                NoStatusCause::ReadTimeout,
                NoStatusCause::Closed,
                NoStatusCause::Other,
            ] {
                let evidence = classify(
                    &DispatchStage::WrittenNoStatus(cause),
                    BodyRead::NotRead,
                    &expectation(endpoint),
                );
                let RestEvidence::Unknown { diagnosis } = &evidence else {
                    panic!("{evidence:?}");
                };
                assert_eq!(diagnosis.reason, UnknownReason::NoStatus(cause));
                assert_eq!(diagnosis.status, None);
            }
        }
    }

    #[test]
    fn attributable_success_is_applied_on_every_endpoint() {
        for endpoint in E::ALL {
            let RestEvidence::Applied { receipt } = success(endpoint) else {
                panic!("{endpoint:?}: {:?}", success(endpoint));
            };
            assert_eq!(receipt.endpoint, endpoint);
            match endpoint {
                E::TableCommit | E::CtasFinalCommit => {
                    assert_eq!(receipt.snapshot_id, Some(S));
                    assert_eq!(receipt.uuid, Some(uuid(TABLE_UUID)));
                    assert_eq!(receipt.ref_relation, RefRelation::AtExpectedSnapshot);
                    assert!(receipt.metadata_location.is_some());
                }
                E::CreateTable => assert_eq!(receipt.uuid, Some(uuid(OTHER_UUID))),
                E::StageCreate => {
                    assert_eq!(receipt.uuid, Some(uuid(OTHER_UUID)));
                    assert_eq!(receipt.metadata_location, None);
                }
                E::CreateView | E::ReplaceView => {
                    assert_eq!(receipt.uuid, Some(uuid(TABLE_UUID)))
                }
                _ => assert_eq!(receipt.uuid, None),
            }
        }
    }

    #[test]
    fn incomplete_documents_are_protocol_anomalies() {
        for endpoint in E::ALL {
            let Some(good) = good_body(endpoint) else {
                continue;
            };
            let expect = expectation(endpoint);
            let code = endpoint.success_status();
            let html = classify_body(&expect, code, Some("<html><body>gateway</body></html>"));
            assert!(matches!(
                unknown_reason(&html),
                UnknownReason::ProtocolAnomaly(AnomalyCause::Read(BodyReadFailure::Scan(
                    ScanError::Syntax { .. }
                )))
            ));
            let truncated = classify_body(&expect, code, Some(&good[..good.len() - 1]));
            assert_anomaly(
                &truncated,
                AnomalyCause::Read(BodyReadFailure::Scan(ScanError::Truncated)),
            );
            let trailing = classify_body(&expect, code, Some(&format!("{good} {{}}")));
            assert!(matches!(
                unknown_reason(&trailing),
                UnknownReason::ProtocolAnomaly(AnomalyCause::Read(BodyReadFailure::Scan(
                    ScanError::TrailingContent { .. }
                )))
            ));
            for failure in [
                BodyReadFailure::ReadTimeout,
                BodyReadFailure::Closed,
                BodyReadFailure::Cancelled,
                BodyReadFailure::DeadlineExceeded,
                BodyReadFailure::Transport,
            ] {
                let evidence = classify(&status(code), BodyRead::Failed(failure), &expect);
                assert_anomaly(&evidence, AnomalyCause::Read(failure));
            }
            assert_anomaly(
                &classify(&status(code), BodyRead::NotRead, &expect),
                AnomalyCause::BodyNotRead,
            );
        }
    }

    #[test]
    fn missing_or_unexpected_metadata_location() {
        for endpoint in [E::TableCommit, E::CtasFinalCommit, E::CreateTable] {
            let expect = expectation(endpoint);
            for location in [
                "",
                r#""metadata-location":null,"#,
                r#""metadata-location":"","#,
            ] {
                let body = table_body(location, TABLE_UUID, &[S], Some(S));
                assert_anomaly(
                    &classify_body(&expect, 200, Some(&body)),
                    AnomalyCause::MissingMetadataLocation,
                );
            }
        }
        let stage = expectation(E::StageCreate);
        for location in [
            "",
            r#""metadata-location":null,"#,
            r#""metadata-location":"","#,
        ] {
            let body = table_body(location, OTHER_UUID, &[], None);
            assert!(matches!(
                classify_body(&stage, 200, Some(&body)),
                RestEvidence::Applied { .. }
            ));
        }
        let body = table_body(r#""metadata-location":"m","#, OTHER_UUID, &[], None);
        assert_anomaly(
            &classify_body(&stage, 200, Some(&body)),
            AnomalyCause::UnexpectedMetadataLocation,
        );
        for endpoint in [E::CreateView, E::ReplaceView] {
            let body = format!(r#"{{"metadata":{{"view-uuid":"{TABLE_UUID}"}}}}"#);
            assert_anomaly(
                &classify_body(&expectation(endpoint), 200, Some(&body)),
                AnomalyCause::MissingMetadataLocation,
            );
        }
    }

    #[test]
    fn identity_must_match_the_frozen_identity() {
        let located = r#""metadata-location":"m","#;
        for endpoint in [E::TableCommit, E::CtasFinalCommit] {
            let expect = expectation(endpoint);
            let mismatch = table_body(located, OTHER_UUID, &[S], Some(S));
            assert_anomaly(
                &classify_body(&expect, 200, Some(&mismatch)),
                AnomalyCause::UuidMismatch,
            );
            let malformed = table_body(located, "not-a-uuid", &[S], Some(S));
            assert_anomaly(
                &classify_body(&expect, 200, Some(&malformed)),
                AnomalyCause::MalformedUuid,
            );
            let missing =
                r#"{"metadata-location":"m","metadata":{"snapshots":[{"snapshot-id":6}]}}"#;
            assert_anomaly(
                &classify_body(&expect, 200, Some(missing)),
                AnomalyCause::MissingUuid,
            );
        }
        // Uppercase and hyphenless spellings name the same UUID.
        let upper = table_body(located, &TABLE_UUID.to_uppercase(), &[S], Some(S));
        assert!(matches!(
            classify_body(&expectation(E::TableCommit), 200, Some(&upper)),
            RestEvidence::Applied { .. }
        ));
        // Create and stage have no prior identity to compare.
        for endpoint in [E::CreateTable, E::StageCreate] {
            let location = if endpoint == E::StageCreate {
                ""
            } else {
                located
            };
            let body = table_body(location, TABLE_UUID, &[], None);
            assert!(matches!(
                classify_body(&expectation(endpoint), 200, Some(&body)),
                RestEvidence::Applied { .. }
            ));
        }
        let replace = expectation(E::ReplaceView);
        let other =
            format!(r#"{{"metadata-location":"v","metadata":{{"view-uuid":"{OTHER_UUID}"}}}}"#);
        assert_anomaly(
            &classify_body(&replace, 200, Some(&other)),
            AnomalyCause::UuidMismatch,
        );
        assert!(matches!(
            classify_body(&expectation(E::CreateView), 200, Some(&other)),
            RestEvidence::Applied { .. }
        ));
        let namespace = expectation(E::CreateNamespace);
        assert_anomaly(
            &classify_body(&namespace, 200, Some(r#"{"namespace":["db"]}"#)),
            AnomalyCause::NamespaceMismatch,
        );
        assert_anomaly(
            &classify_body(&namespace, 200, Some(r#"{"properties":{}}"#)),
            AnomalyCause::MissingNamespace,
        );
        let duplicate = format!(
            r#"{{"metadata-location":"m","metadata":{{"table-uuid":"{TABLE_UUID}","table-uuid":"{TABLE_UUID}"}}}}"#
        );
        assert_anomaly(
            &classify_body(&expectation(E::TableCommit), 200, Some(&duplicate)),
            AnomalyCause::Read(BodyReadFailure::Scan(ScanError::DuplicateField(
                "metadata.table-uuid",
            ))),
        );
    }

    #[test]
    fn missing_snapshot_or_property_is_unattributable() {
        let located = r#""metadata-location":"m","#;
        for endpoint in [E::TableCommit, E::CtasFinalCommit] {
            let expect = expectation(endpoint);
            let body = table_body(located, TABLE_UUID, &[5], Some(5));
            let evidence = classify_body(&expect, 200, Some(&body));
            let RestEvidence::Unknown { diagnosis } = &evidence else {
                panic!("{evidence:?}");
            };
            assert_eq!(
                diagnosis.reason,
                UnknownReason::Unattributable(UnattributableCause::SnapshotAbsent)
            );
            assert_eq!(diagnosis.ref_relation, RefRelation::AtOtherSnapshot);
            assert_eq!(diagnosis.expected_snapshot, Some(S));
            assert_eq!(diagnosis.expected_uuid, Some(uuid(TABLE_UUID)));
        }
        let ctas = expectation(E::CtasFinalCommit);
        let wrong_marker = table_body(located, TABLE_UUID, &[S], Some(S)).replace("op-7", "op-8");
        let property_mismatch =
            UnknownReason::Unattributable(UnattributableCause::PropertyMismatch {
                key: MARKER.to_owned(),
            });
        assert_eq!(
            unknown_reason(&classify_body(&ctas, 200, Some(&wrong_marker))),
            &property_mismatch
        );
        let no_marker = table_body(located, TABLE_UUID, &[S], Some(S)).replace(MARKER, "other");
        assert_eq!(
            unknown_reason(&classify_body(&ctas, 200, Some(&no_marker))),
            &property_mismatch
        );
        // A CTAS without data adds no snapshot; only the marker is checked.
        let no_data = AttributionExpectation::ctas_final_commit(
            uuid(TABLE_UUID),
            None,
            vec![(MARKER.to_owned(), "op-7".to_owned())],
        )
        .unwrap();
        let body = table_body(located, TABLE_UUID, &[], None);
        assert!(matches!(
            classify_body(&no_data, 200, Some(&body)),
            RestEvidence::Applied { .. }
        ));
    }

    #[test]
    fn metadata_only_commit_checks_identity_only() {
        let expect = AttributionExpectation::table_commit(uuid(TABLE_UUID), None).unwrap();
        let located = r#""metadata-location":"m","#;
        let body = table_body(located, TABLE_UUID, &[1, 2], Some(2));
        let RestEvidence::Applied { receipt } = classify_body(&expect, 200, Some(&body)) else {
            panic!("metadata-only commit must be applied");
        };
        assert_eq!(receipt.snapshot_id, None);
        assert_eq!(receipt.ref_relation, RefRelation::NotTracked);
        let other = table_body(located, OTHER_UUID, &[1, 2], Some(2));
        assert_anomaly(
            &classify_body(&expect, 200, Some(&other)),
            AnomalyCause::UuidMismatch,
        );
    }

    #[test]
    fn refreshed_response_is_applied() {
        let located = r#""metadata-location":"s3://b/t/metadata/00003.json","#;
        for endpoint in [E::TableCommit, E::CtasFinalCommit] {
            let expect = expectation(endpoint);
            let refreshed = table_body(located, TABLE_UUID, &[5, S, LATER], Some(LATER));
            let RestEvidence::Applied { receipt } = classify_body(&expect, 200, Some(&refreshed))
            else {
                panic!("a refreshed response must be applied");
            };
            assert_eq!(receipt.snapshot_id, Some(S));
            assert_eq!(receipt.ref_relation, RefRelation::AtOtherSnapshot);
            let no_ref = table_body(located, TABLE_UUID, &[S], None);
            let RestEvidence::Applied { receipt } = classify_body(&expect, 200, Some(&no_ref))
            else {
                panic!("the ref never decides attribution");
            };
            assert_eq!(receipt.ref_relation, RefRelation::Absent);
        }
    }

    #[test]
    fn a_view_of_another_shape_or_request_is_an_anomaly() {
        let expect = expectation(E::TableCommit);
        let namespace = BodyRead::Complete(MutationView::Namespace(NamespaceView {
            namespace: Some(vec!["db".to_owned()]),
        }));
        assert_anomaly(
            &classify(&status(200), namespace, &expect),
            AnomalyCause::ViewMismatch,
        );
        // Scanned without asking for S: the view cannot prove item 5.
        let body = table_body(r#""metadata-location":"m","#, TABLE_UUID, &[S], Some(S));
        let view = scan_complete(
            TableResponseSink::new(TableViewRequest::default()),
            body.as_bytes(),
        )
        .unwrap();
        assert_anomaly(
            &classify(
                &status(200),
                BodyRead::Complete(MutationView::Table(view)),
                &expect,
            ),
            AnomalyCause::ViewMismatch,
        );
    }

    #[test]
    fn catalog_outcome_mapping() {
        let evidence = || CatalogCommitEvidence::for_target("db.t");
        let CatalogOutcome::KnownCommitted {
            effect, receipt, ..
        } = success(E::TableCommit).into_catalog_outcome(evidence())
        else {
            panic!("applied must be committed");
        };
        assert_eq!(effect, ExternalMutationEffect::Applied);
        assert_eq!(receipt.snapshot_id, Some(S));
        assert_eq!(receipt.table_uuid.as_deref(), Some(TABLE_UUID));
        assert_eq!(receipt.metadata_location, None);

        let expect = expectation(E::TableCommit);
        let not_written = classify(
            &DispatchStage::NotWritten(NotWrittenCause::Connect),
            BodyRead::NotRead,
            &expect,
        );
        assert!(matches!(
            not_written.into_catalog_outcome(evidence()),
            CatalogOutcome::KnownUncommitted { failure } if failure.kind() == Kind::Unavailable
        ));
        let rejected = classify_body(&expect, 403, None);
        let CatalogOutcome::KnownUncommitted { failure } =
            rejected.into_catalog_outcome(evidence())
        else {
            panic!("403 must be uncommitted");
        };
        assert_eq!(failure.kind(), Kind::PermissionDenied);
        assert_eq!(
            failure.message(),
            "REST catalog rejected the request with HTTP 403"
        );
        let conflict = classify_body(&expect, 409, None).into_catalog_outcome(evidence());
        assert!(matches!(
            conflict,
            CatalogOutcome::KnownUncommitted { failure } if failure.kind() == Kind::Conflict
        ));
        for code in [404, 500, 302] {
            let outcome = classify_body(&expect, code, None).into_catalog_outcome(evidence());
            let CatalogOutcome::CommitUnknown {
                failure,
                evidence: carried,
            } = outcome
            else {
                panic!("{code} must be unknown");
            };
            assert_eq!(carried.target, Some(Arc::from("db.t")));
            assert!(failure.message().contains(&format!("HTTP {code}")));
        }
    }

    #[test]
    fn stage_verdicts_follow_l19() {
        let expect = expectation(E::StageCreate);
        let verdict = |evidence: RestEvidence| evidence.into_stage_verdict();
        assert!(matches!(
            verdict(success(E::StageCreate)),
            StageVerdict::Staged { .. }
        ));
        assert!(matches!(
            verdict(classify_body(&expect, 409, None)),
            StageVerdict::Conflict(failure) if failure.kind() == Kind::AlreadyExists
        ));
        for (code, kind) in [
            (401, Kind::Unauthenticated),
            (419, Kind::Unauthenticated),
            (403, Kind::PermissionDenied),
            (413, Kind::ResourceExhausted),
        ] {
            assert!(matches!(
                verdict(classify_body(&expect, code, None)),
                StageVerdict::KnownUncommitted(failure) if failure.kind() == kind
            ));
        }
        assert!(matches!(
            verdict(classify(
                &DispatchStage::NotWritten(NotWrittenCause::Binding(BindingFailure::TokenRejected)),
                BodyRead::NotRead,
                &expect,
            )),
            StageVerdict::KnownUncommitted(failure) if failure.kind() == Kind::Unauthenticated
        ));
        for code in [400, 404, 500, 503, 307] {
            assert!(matches!(
                verdict(classify_body(&expect, code, None)),
                StageVerdict::CommitUnknown { .. }
            ));
        }
        assert!(matches!(
            verdict(classify_body(&expect, 200, Some("<html>"))),
            StageVerdict::CommitUnknown { .. }
        ));
        assert!(matches!(
            verdict(classify(
                &DispatchStage::WrittenNoStatus(NoStatusCause::Closed),
                BodyRead::NotRead,
                &expect,
            )),
            StageVerdict::CommitUnknown { .. }
        ));
    }

    #[test]
    fn ctas_commit_verdicts() {
        let expect = expectation(E::CtasFinalCommit);
        let verdict = |evidence: RestEvidence| evidence.into_ctas_commit_verdict();
        let CtasCommitVerdict::Committed { receipt } = verdict(success(E::CtasFinalCommit)) else {
            panic!("attributable CTAS commit must be committed");
        };
        assert_eq!(receipt.uuid, Some(uuid(TABLE_UUID)));
        assert!(matches!(
            verdict(classify_body(&expect, 409, None)),
            CtasCommitVerdict::Conflict(failure) if failure.kind() == Kind::AlreadyExists
        ));
        assert!(matches!(
            verdict(classify_body(&expect, 403, None)),
            CtasCommitVerdict::KnownUncommitted(failure) if failure.kind() == Kind::PermissionDenied
        ));
        assert!(matches!(
            verdict(classify_body(&expect, 413, None)),
            CtasCommitVerdict::KnownUncommitted(failure) if failure.kind() == Kind::ResourceExhausted
        ));
        for code in [400, 404, 502] {
            assert!(matches!(
                verdict(classify_body(&expect, code, None)),
                CtasCommitVerdict::CommitUnknown { .. }
            ));
        }
        assert!(matches!(
            verdict(classify_body(&expect, 200, Some("<html>"))),
            CtasCommitVerdict::CommitUnknown { .. }
        ));
    }

    #[test]
    fn messages_and_diagnoses_carry_no_response_text() {
        let expect = expectation(E::TableCommit);
        let bodies = [
            format!("<html>{SECRET}</html>"),
            table_body(
                &format!(r#""metadata-location":"s3://{SECRET}/m","#),
                OTHER_UUID,
                &[S],
                Some(S),
            ),
            table_body(r#""metadata-location":"m","#, TABLE_UUID, &[5], Some(5))
                .replace("op-7", SECRET),
        ];
        for body in &bodies {
            let evidence = classify_body(&expect, 200, Some(body));
            assert!(matches!(evidence, RestEvidence::Unknown { .. }));
            let failure = evidence.failure().unwrap();
            for text in [
                format!("{evidence:?}"),
                evidence.message(),
                format!("{failure:?}"),
            ] {
                assert!(!text.contains(SECRET), "{text}");
                assert!(!text.contains(OTHER_UUID), "{text}");
            }
        }
        // An applied receipt's Debug does not print the returned location.
        let located = format!(r#""metadata-location":"s3://{SECRET}/m","#);
        let applied = classify_body(
            &expect,
            200,
            Some(&table_body(&located, TABLE_UUID, &[S], Some(S))),
        );
        let RestEvidence::Applied { receipt } = &applied else {
            panic!("{applied:?}");
        };
        assert!(
            receipt
                .metadata_location
                .as_deref()
                .is_some_and(|l| l.contains(SECRET))
        );
        assert!(!format!("{applied:?}").contains(SECRET));
        assert_eq!(
            classify_body(&expect, 401, None).message(),
            "REST catalog rejected the request with HTTP 401"
        );
    }

    #[test]
    fn expectations_retain_long_values_and_keep_ref_validation() {
        // This UTF8 value exceeds the former 64KiB quota; its bytes author
        // response retention, without becoming a replacement product limit.
        let expected = "原文".repeat(16 * 1024);
        assert!(expected.len() > 64 * 1024);
        let expect = AttributionExpectation::ctas_final_commit(
            uuid(TABLE_UUID),
            None,
            vec![(MARKER.to_owned(), expected.clone())],
        )
        .unwrap();
        for extra in [0, 1, 10_000] {
            let value = format!("{expected}{}", "x".repeat(extra));
            let body = table_body(
                r#""metadata-location":"s3://warehouse/db/t/metadata/2.json","#,
                TABLE_UUID,
                &[],
                None,
            )
            .replace("\"op-7\"", &serde_json::to_string(&value).unwrap());
            let evidence = classify_body(&expect, 200, Some(&body));
            if extra == 0 {
                assert!(matches!(evidence, RestEvidence::Applied { .. }));
            } else {
                assert_eq!(
                    unknown_reason(&evidence),
                    &UnknownReason::Unattributable(UnattributableCause::PropertyMismatch {
                        key: MARKER.to_owned(),
                    })
                );
            }
        }
        assert_eq!(
            AttributionExpectation::table_commit(
                uuid(TABLE_UUID),
                Some(ExpectedSnapshot::new(S, Some("r".repeat(2048)))),
            ),
            Err(ExpectationError::View(ViewRequestError::RefNameTooLong))
        );
        assert!(expectation(E::DropTable).table_view_request().is_none());
        assert!(expectation(E::StageCreate).table_view_request().is_some());
    }
    #[test]
    fn ref_retention_success_checks_every_requested_postcondition() {
        use crate::iceberg::TableUpdate;
        use crate::iceberg::spec::{SnapshotReference, SnapshotRetention};
        let expect = AttributionExpectation::ref_retention(
            uuid(TABLE_UUID),
            &[
                TableUpdate::RemoveSnapshots {
                    snapshot_ids: vec![1, 2],
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: "kept".into(),
                    reference: SnapshotReference::new(
                        3,
                        SnapshotRetention::branch(None, None, None),
                    ),
                },
                TableUpdate::SetSnapshotRef {
                    ref_name: "tag".into(),
                    reference: SnapshotReference::new(
                        4,
                        SnapshotRetention::Tag {
                            max_ref_age_ms: None,
                        },
                    ),
                },
                TableUpdate::RemoveSnapshotRef {
                    ref_name: "removed".into(),
                },
            ],
        )
        .unwrap();
        let response = serde_json::json!({"metadata-location":"s3://warehouse/t/metadata/v2.json","metadata":{"table-uuid":TABLE_UUID,"snapshots":[{"snapshot-id":3},{"snapshot-id":4},{"snapshot-id":999}],"refs":{"kept":{"snapshot-id":3},"tag":{"snapshot-id":4},"unrequested":{"snapshot-id":999}}}});
        assert!(matches!(
            classify(&status(200), read(&expect, &response.to_string()), &expect),
            RestEvidence::Applied { .. }
        ));
        for changed in 0..5 {
            let mut invalid = response.clone();
            match changed {
                0 => invalid["metadata"]["snapshots"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!({"snapshot-id":1})),
                1 => invalid["metadata"]["snapshots"]
                    .as_array_mut()
                    .unwrap()
                    .push(serde_json::json!({"snapshot-id":2})),
                2 => invalid["metadata"]["refs"]["kept"]["snapshot-id"] = 99.into(),
                3 => {
                    invalid["metadata"]["refs"]
                        .as_object_mut()
                        .unwrap()
                        .remove("tag");
                }
                4 => invalid["metadata"]["refs"]["removed"] = serde_json::json!({}),
                _ => unreachable!(),
            }
            let evidence = classify(&status(200), read(&expect, &invalid.to_string()), &expect);
            assert_eq!(
                unknown_reason(&evidence),
                &UnknownReason::Unattributable(UnattributableCause::RetentionPostconditionMismatch),
                "case {changed}: {evidence:?}"
            );
        }
        for malformed in [
            serde_json::json!({}),
            serde_json::json!({"snapshot-id":null}),
            serde_json::json!({"snapshot-id":"1"}),
        ] {
            let mut invalid = response.clone();
            invalid["metadata"]["snapshots"]
                .as_array_mut()
                .unwrap()
                .push(malformed);
            assert!(
                matches!(
                    classify(&status(200), read(&expect, &invalid.to_string()), &expect),
                    RestEvidence::Unknown { .. }
                ),
                "a malformed snapshot cannot prove requested IDs absent"
            );
        }
        let duplicate_ref = format!(
            r#"{{"metadata-location":"s3://w/m","metadata":{{"table-uuid":"{TABLE_UUID}","refs":{{"kept":{{"snapshot-id":3}},"kept":{{"snapshot-id":3}}}}}}}}"#
        );
        assert!(matches!(
            classify(&status(200), read(&expect, &duplicate_ref), &expect),
            RestEvidence::Unknown { .. }
        ));
    }
}
