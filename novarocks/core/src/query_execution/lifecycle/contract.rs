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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use super::identity::AttemptId;
use super::identity::QueryExecutionId;
use super::manifest::{
    ExchangeRouteManifest, ParticipantBackendIdentity, ParticipantManifest,
    ParticipantManifestDigest, ParticipantQueryOptions, ParticipantRole, QueryControlEndpoint,
    RuntimeFilterContribution,
};
use super::stage::{
    QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartOutcome,
    QueryStartRequest, StageDigest, StageDigestVersion, StageFragment,
};
use super::terminal::{QueryTerminalSnapshot, QueryTerminalSnapshotDigest};
use crate::common::types::UniqueId;
use crate::proto::{common, filter, novarocks};
use crate::runtime::profile::RuntimeProfileTree;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryLifecycleErrorCode {
    InvalidManifest,
    Conflict,
    StaleBackend,
    Capacity,
    Terminated,
    Transport,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryLifecycleError {
    code: QueryLifecycleErrorCode,
    detail: String,
}

impl QueryLifecycleError {
    pub fn new(code: QueryLifecycleErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub(crate) fn invalid_manifest(detail: impl Into<String>) -> Self {
        Self::new(QueryLifecycleErrorCode::InvalidManifest, detail)
    }

    pub const fn code(&self) -> QueryLifecycleErrorCode {
        self.code
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for QueryLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for QueryLifecycleError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentLiveObservation {
    execution_id: QueryExecutionId,
    init_digest: ParticipantManifestDigest,
    backend: ParticipantBackendIdentity,
    fragment_instance_id: UniqueId,
    sequence: u64,
    input_rows: u64,
    output_rows: u64,
    elapsed_ms: u64,
    profile: Option<RuntimeProfileTree>,
}

impl FragmentLiveObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: QueryExecutionId,
        init_digest: ParticipantManifestDigest,
        backend: ParticipantBackendIdentity,
        fragment_instance_id: UniqueId,
        sequence: u64,
        input_rows: u64,
        output_rows: u64,
        elapsed_ms: u64,
        profile: Option<RuntimeProfileTree>,
    ) -> Result<Self, QueryLifecycleError> {
        if fragment_instance_id.hi == 0 && fragment_instance_id.lo == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "fragment observation instance id must be nonzero",
            ));
        }
        if sequence == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "fragment observation sequence must be nonzero",
            ));
        }
        Ok(Self {
            execution_id,
            init_digest,
            backend,
            fragment_instance_id,
            sequence,
            input_rows,
            output_rows,
            elapsed_ms,
            profile,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        &self.backend
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub const fn input_rows(&self) -> u64 {
        self.input_rows
    }

    pub const fn output_rows(&self) -> u64 {
        self.output_rows
    }

    pub const fn elapsed_ms(&self) -> u64 {
        self.elapsed_ms
    }

    pub const fn profile(&self) -> Option<&RuntimeProfileTree> {
        self.profile.as_ref()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryControlEvent {
    ControlReady,
    HeartbeatAck {
        sequence: u64,
    },
    LocalFailure {
        code: String,
        detail: String,
    },
    LocalDrained,
    TerminalSnapshot {
        snapshot: QueryTerminalSnapshot,
    },
    TerminationAccepted {
        reason: QueryTerminationReason,
    },
    FragmentObservation {
        observation: FragmentLiveObservation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryControlCommand {
    Heartbeat { sequence: u64, sent_mono_ns: u64 },
    Abort { reason: String },
    Finalize,
    TerminalAck { ack: QueryTerminalAck },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryTerminalAck {
    execution_id: QueryExecutionId,
    init_digest: ParticipantManifestDigest,
    version: u32,
    digest: QueryTerminalSnapshotDigest,
}

impl QueryTerminalAck {
    pub const fn new(
        execution_id: QueryExecutionId,
        init_digest: ParticipantManifestDigest,
        version: u32,
        digest: QueryTerminalSnapshotDigest,
    ) -> Self {
        Self {
            execution_id,
            init_digest,
            version,
            digest,
        }
    }

    pub const fn from_snapshot(snapshot: &QueryTerminalSnapshot) -> Self {
        Self::new(
            snapshot.execution_id(),
            snapshot.init_digest(),
            snapshot.version(),
            snapshot.digest(),
        )
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }
    pub const fn init_digest(&self) -> ParticipantManifestDigest {
        self.init_digest
    }
    pub const fn version(&self) -> u32 {
        self.version
    }
    pub const fn digest(&self) -> QueryTerminalSnapshotDigest {
        self.digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTerminalReportOutcome {
    Accepted,
    AlreadyAccepted,
    RejectedConflict,
    RejectedGone,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminalReportAck {
    outcome: QueryTerminalReportOutcome,
    detail: String,
}

impl QueryTerminalReportAck {
    pub fn new(outcome: QueryTerminalReportOutcome, detail: impl Into<String>) -> Self {
        Self {
            outcome,
            detail: detail.into(),
        }
    }
    pub const fn outcome(&self) -> QueryTerminalReportOutcome {
        self.outcome
    }
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

/// FE-owned ingress for immutable terminal snapshots.  It is intentionally
/// distinct from FE-to-BE lifecycle RPCs.
pub trait QueryTerminalIngress: Send + Sync + 'static {
    fn report_query_terminal(
        &self,
        snapshot: QueryTerminalSnapshot,
    ) -> Result<QueryTerminalReportAck, QueryLifecycleError>;
}

/// BE-owned fallback transport.  Delivery never reconnects or recreates the
/// control session; it only reports the already frozen snapshot.
pub trait QueryTerminalFallbackTransport: Send + Sync + 'static {
    fn report_query_terminal(
        &self,
        endpoint: &QueryControlEndpoint,
        snapshot: QueryTerminalSnapshot,
        timeout: Duration,
    ) -> Result<QueryTerminalReportAck, QueryLifecycleTransportError>;
}

pub trait QueryLifecycleTransport: Send + Sync + 'static {
    fn init_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryInitRequest,
        timeout: Duration,
    ) -> Result<QueryInitAck, QueryLifecycleTransportError>;

    fn attach_control(
        &self,
        target: QueryLifecycleTarget,
        attach: QueryControlAttach,
        timeout: Duration,
    ) -> Result<Arc<dyn QueryControlSession>, QueryLifecycleTransportError>;

    /// Atomically stage the complete participant-local fragment batch.
    ///
    /// Implementations that have not completed the QLC-3 cutover return an
    /// explicit unavailable error rather than falling back to per-fragment startup.
    fn stage_fragments(
        &self,
        _target: QueryLifecycleTarget,
        _request: &QueryStageRequest,
        _timeout: Duration,
    ) -> Result<QueryStageAck, QueryLifecycleTransportError> {
        Err(QueryLifecycleTransportError::new(
            QueryLifecycleTransportErrorKind::Unavailable,
            "StageFragments is not supported by this lifecycle transport",
        ))
    }

    /// Releases the already prepared participant-local start gate.
    fn start_prepared_query(
        &self,
        _target: QueryLifecycleTarget,
        _request: &QueryStartRequest,
        _timeout: Duration,
    ) -> Result<QueryStartAck, QueryLifecycleTransportError> {
        Err(QueryLifecycleTransportError::new(
            QueryLifecycleTransportErrorKind::Unavailable,
            "StartPreparedQuery is not supported by this lifecycle transport",
        ))
    }

    fn abort_query(
        &self,
        target: QueryLifecycleTarget,
        request: QueryAbortRequest,
        timeout: Duration,
    ) -> Result<QueryTerminationAck, QueryLifecycleTransportError>;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryLifecycleTarget {
    backend_idx: usize,
    endpoint: SocketAddr,
    start_epoch: u64,
}

impl QueryLifecycleTarget {
    pub const fn new(backend_idx: usize, endpoint: SocketAddr, start_epoch: u64) -> Self {
        Self {
            backend_idx,
            endpoint,
            start_epoch,
        }
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(self) -> SocketAddr {
        self.endpoint
    }

    pub const fn start_epoch(self) -> u64 {
        self.start_epoch
    }
}

pub trait QueryControlSession: Send + Sync + 'static {
    fn send(&self, command: QueryControlCommand) -> Result<(), QueryLifecycleTransportError>;

    fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<QueryControlEvent, QueryLifecycleTransportError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryLifecycleTransportErrorKind {
    DeadlineExceeded,
    StreamClosed,
    Backpressure,
    InvalidResponse,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryLifecycleTransportError {
    kind: QueryLifecycleTransportErrorKind,
    detail: String,
}

impl QueryLifecycleTransportError {
    pub fn new(kind: QueryLifecycleTransportErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
        }
    }

    pub const fn kind(&self) -> QueryLifecycleTransportErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }

    pub const fn is_unknown_init_outcome(&self) -> bool {
        matches!(
            self.kind,
            QueryLifecycleTransportErrorKind::DeadlineExceeded
                | QueryLifecycleTransportErrorKind::StreamClosed
        )
    }

    pub const fn is_unknown_stage_or_start_outcome(&self) -> bool {
        matches!(
            self.kind,
            QueryLifecycleTransportErrorKind::DeadlineExceeded
                | QueryLifecycleTransportErrorKind::StreamClosed
        )
    }
}

impl std::fmt::Display for QueryLifecycleTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.detail)
    }
}

impl std::error::Error for QueryLifecycleTransportError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTerminationReason {
    CoordinatorAbort,
    CoordinatorFinalize,
    CoordinatorStreamLost,
    CoordinatorHeartbeatTimeout,
    LocalFailure,
    PreStartTimeout,
}

pub trait BackendQueryControl: Send + Sync + 'static {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError>;

    fn abort(&self, reason: String) -> Result<(), QueryLifecycleError>;

    fn finalize(&self) -> Result<(), QueryLifecycleError>;

    fn terminal_ack(&self, _ack: QueryTerminalAck) -> Result<(), QueryLifecycleError> {
        Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::Terminated,
            "query terminal acknowledgement is not supported by this lifecycle owner",
        ))
    }

    fn coordinator_lost(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError>;
}

pub struct QueryControlAttachment {
    pub control: Arc<dyn BackendQueryControl>,
    pub events: tokio::sync::mpsc::Receiver<QueryControlEvent>,
    /// A single-slot, replaceable telemetry view. Correctness events remain on
    /// `events` so a congested profiler/progress producer cannot delay an ACK,
    /// drain barrier, or terminal snapshot.
    pub observations: tokio::sync::watch::Receiver<Option<FragmentLiveObservation>>,
}

#[derive(Clone, Debug)]
pub struct QueryInitRequest {
    manifest: ParticipantManifest,
    digest: ParticipantManifestDigest,
}

impl QueryInitRequest {
    pub fn new(
        manifest: ParticipantManifest,
        digest: ParticipantManifestDigest,
    ) -> Result<Self, QueryLifecycleError> {
        if manifest.digest() != digest {
            return Err(QueryLifecycleError::invalid_manifest(
                "participant manifest digest does not match canonical projection",
            ));
        }
        Ok(Self { manifest, digest })
    }

    pub fn from_manifest(manifest: ParticipantManifest) -> Self {
        let digest = manifest.digest();
        Self { manifest, digest }
    }

    pub const fn manifest(&self) -> &ParticipantManifest {
        &self.manifest
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub fn into_parts(self) -> (ParticipantManifest, ParticipantManifestDigest) {
        (self.manifest, self.digest)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryInitOutcome {
    Applied,
    AlreadyApplied,
    RejectedConflict,
    RejectedStaleBackend,
    RejectedCapacity,
    RejectedInvalidManifest,
    RejectedTerminated,
}

impl QueryInitOutcome {
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Applied | Self::AlreadyApplied)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryInitAck {
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
    outcome: QueryInitOutcome,
}

impl QueryInitAck {
    pub const fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        outcome: QueryInitOutcome,
    ) -> Self {
        Self {
            execution_id,
            digest,
            outcome,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub const fn outcome(&self) -> QueryInitOutcome {
        self.outcome
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryControlAttach {
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
    frontend_owner_epoch: u64,
}

impl QueryControlAttach {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        frontend_owner_epoch: u64,
    ) -> Result<Self, QueryLifecycleError> {
        if frontend_owner_epoch == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "frontend owner epoch must be nonzero",
            ));
        }
        Ok(Self {
            execution_id,
            digest,
            frontend_owner_epoch,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub const fn frontend_owner_epoch(&self) -> u64 {
        self.frontend_owner_epoch
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAbortRequest {
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
    reason: String,
}

impl QueryAbortRequest {
    pub fn new(
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
        reason: impl Into<String>,
    ) -> Result<Self, QueryLifecycleError> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(QueryLifecycleError::invalid_manifest(
                "abort reason must not be empty",
            ));
        }
        Ok(Self {
            execution_id,
            digest,
            reason,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryTerminationAck {
    execution_id: QueryExecutionId,
    accepted_reason: QueryTerminationReason,
}

impl QueryTerminationAck {
    pub const fn new(
        execution_id: QueryExecutionId,
        accepted_reason: QueryTerminationReason,
    ) -> Self {
        Self {
            execution_id,
            accepted_reason,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn accepted_reason(&self) -> QueryTerminationReason {
        self.accepted_reason
    }
}

pub trait QueryLifecycleIngress: Send + Sync + 'static {
    fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError>;

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck;

    /// Atomically records the participant-local stage contract.  Fragment
    /// materialization remains a backend concern; this contract boundary only
    /// returns a typed outcome so an ambiguous RPC retry can be idempotent.
    fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        QueryStageAck::new(
            request.execution_id(),
            request.digest_version(),
            request.digest(),
            QueryStageOutcome::RejectedInvalidState,
            "StageFragments is not supported by this lifecycle ingress",
        )
    }

    /// Releases one previously staged query bundle.  A duplicate request with
    /// the same digest must not cause a second release.
    fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        QueryStartAck::new(
            request.execution_id(),
            request.digest_version(),
            request.digest(),
            QueryStartOutcome::RejectedNotStaged,
            "StartPreparedQuery is not supported by this lifecycle ingress",
        )
    }

    fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError>;

    fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError>;
}

pub fn encode_query_init_request(
    request: &QueryInitRequest,
) -> Result<novarocks::InitQueryRequest, QueryLifecycleError> {
    Ok(novarocks::InitQueryRequest {
        manifest: Some(encode_participant_manifest(request.manifest())?),
        init_digest: request.digest().as_bytes().to_vec(),
    })
}

pub fn decode_query_init_request(
    request: &novarocks::InitQueryRequest,
) -> Result<QueryInitRequest, QueryLifecycleError> {
    let manifest = request
        .manifest
        .as_ref()
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("participant manifest is required"))
        .and_then(decode_participant_manifest)?;
    let digest = ParticipantManifestDigest::try_from_slice(&request.init_digest)?;
    QueryInitRequest::new(manifest, digest)
}

pub fn encode_query_init_response(response: &QueryInitAck) -> novarocks::InitQueryResponse {
    novarocks::InitQueryResponse {
        execution_id: Some(encode_execution_id(response.execution_id())),
        init_digest: response.digest().as_bytes().to_vec(),
        outcome: encode_init_outcome(response.outcome()),
    }
}

pub fn decode_query_init_response(
    response: &novarocks::InitQueryResponse,
) -> Result<QueryInitAck, QueryLifecycleError> {
    Ok(QueryInitAck::new(
        decode_required_execution_id(response.execution_id.as_ref())?,
        ParticipantManifestDigest::try_from_slice(&response.init_digest)?,
        decode_init_outcome(response.outcome)?,
    ))
}

pub fn encode_query_stage_request(request: &QueryStageRequest) -> novarocks::StageFragmentsRequest {
    novarocks::StageFragmentsRequest {
        execution_id: Some(encode_execution_id(request.execution_id())),
        init_digest: request.init_digest().as_bytes().to_vec(),
        stage_digest_version: request.digest_version().get(),
        stage_digest: request.digest().as_bytes().to_vec(),
        fragments: request
            .fragments()
            .iter()
            .map(|fragment| novarocks::StageFragment {
                plan: Some(fragment.plan().clone()),
                instance_params: Some(fragment.instance_params().clone()),
            })
            .collect(),
    }
}

pub fn decode_query_stage_request(
    request: &novarocks::StageFragmentsRequest,
) -> Result<QueryStageRequest, QueryLifecycleError> {
    let fragments = request
        .fragments
        .iter()
        .map(|fragment| {
            let plan = fragment.plan.clone().ok_or_else(|| {
                QueryLifecycleError::invalid_manifest("stage fragment plan is required")
            })?;
            let instance_params = fragment.instance_params.clone().ok_or_else(|| {
                QueryLifecycleError::invalid_manifest("stage fragment instance params are required")
            })?;
            StageFragment::new(plan, instance_params)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let decoded = QueryStageRequest::new(
        decode_required_execution_id(request.execution_id.as_ref())?,
        ParticipantManifestDigest::try_from_slice(&request.init_digest)?,
        StageDigestVersion::try_from_wire(request.stage_digest_version)?,
        StageDigest::try_from_slice(&request.stage_digest)?,
        fragments,
    )?;
    let recomputed = StageDigest::compute_v1(
        decoded.execution_id(),
        decoded.init_digest(),
        decoded.fragments(),
    )?;
    if recomputed != decoded.digest() {
        return Err(QueryLifecycleError::invalid_manifest(
            "stage digest does not match decoded stage fragment batch",
        ));
    }
    Ok(decoded)
}

pub fn encode_query_stage_response(response: &QueryStageAck) -> novarocks::StageFragmentsResponse {
    novarocks::StageFragmentsResponse {
        execution_id: Some(encode_execution_id(response.execution_id())),
        stage_digest_version: response.digest_version().get(),
        stage_digest: response.digest().as_bytes().to_vec(),
        outcome: encode_stage_outcome(response.outcome()),
        detail: response.detail().to_string(),
    }
}

pub fn decode_query_stage_response(
    response: &novarocks::StageFragmentsResponse,
) -> Result<QueryStageAck, QueryLifecycleError> {
    Ok(QueryStageAck::new(
        decode_required_execution_id(response.execution_id.as_ref())?,
        StageDigestVersion::try_from_wire(response.stage_digest_version)?,
        StageDigest::try_from_slice(&response.stage_digest)?,
        decode_stage_outcome(response.outcome)?,
        response.detail.clone(),
    ))
}

pub fn encode_query_start_request(
    request: &QueryStartRequest,
) -> novarocks::StartPreparedQueryRequest {
    novarocks::StartPreparedQueryRequest {
        execution_id: Some(encode_execution_id(request.execution_id())),
        stage_digest_version: request.digest_version().get(),
        stage_digest: request.digest().as_bytes().to_vec(),
    }
}

pub fn decode_query_start_request(
    request: &novarocks::StartPreparedQueryRequest,
) -> Result<QueryStartRequest, QueryLifecycleError> {
    Ok(QueryStartRequest::new(
        decode_required_execution_id(request.execution_id.as_ref())?,
        StageDigestVersion::try_from_wire(request.stage_digest_version)?,
        StageDigest::try_from_slice(&request.stage_digest)?,
    ))
}

pub fn encode_query_start_response(
    response: &QueryStartAck,
) -> novarocks::StartPreparedQueryResponse {
    novarocks::StartPreparedQueryResponse {
        execution_id: Some(encode_execution_id(response.execution_id())),
        stage_digest_version: response.digest_version().get(),
        stage_digest: response.digest().as_bytes().to_vec(),
        outcome: encode_start_outcome(response.outcome()),
        detail: response.detail().to_string(),
    }
}

pub fn decode_query_start_response(
    response: &novarocks::StartPreparedQueryResponse,
) -> Result<QueryStartAck, QueryLifecycleError> {
    Ok(QueryStartAck::new(
        decode_required_execution_id(response.execution_id.as_ref())?,
        StageDigestVersion::try_from_wire(response.stage_digest_version)?,
        StageDigest::try_from_slice(&response.stage_digest)?,
        decode_start_outcome(response.outcome)?,
        response.detail.clone(),
    ))
}

pub fn encode_abort_query_request(request: &QueryAbortRequest) -> novarocks::AbortQueryRequest {
    novarocks::AbortQueryRequest {
        execution_id: Some(encode_execution_id(request.execution_id())),
        init_digest: request.digest().as_bytes().to_vec(),
        reason: request.reason().to_string(),
    }
}

pub fn decode_abort_query_request(
    request: &novarocks::AbortQueryRequest,
) -> Result<QueryAbortRequest, QueryLifecycleError> {
    QueryAbortRequest::new(
        decode_required_execution_id(request.execution_id.as_ref())?,
        ParticipantManifestDigest::try_from_slice(&request.init_digest)?,
        request.reason.clone(),
    )
}

pub fn encode_abort_query_response(
    response: &QueryTerminationAck,
) -> novarocks::AbortQueryResponse {
    novarocks::AbortQueryResponse {
        execution_id: Some(encode_execution_id(response.execution_id())),
        accepted_reason: encode_termination_reason(response.accepted_reason()),
    }
}

pub fn decode_abort_query_response(
    response: &novarocks::AbortQueryResponse,
) -> Result<QueryTerminationAck, QueryLifecycleError> {
    Ok(QueryTerminationAck::new(
        decode_required_execution_id(response.execution_id.as_ref())?,
        decode_termination_reason(response.accepted_reason)?,
    ))
}

pub fn encode_query_control_attach(attach: &QueryControlAttach) -> novarocks::QueryControlRequest {
    novarocks::QueryControlRequest {
        command: Some(novarocks::query_control_request::Command::Attach(
            novarocks::QueryControlAttach {
                execution_id: Some(encode_execution_id(attach.execution_id())),
                init_digest: attach.digest().as_bytes().to_vec(),
                frontend_owner_epoch: attach.frontend_owner_epoch(),
            },
        )),
    }
}

pub fn decode_query_control_attach(
    request: &novarocks::QueryControlRequest,
) -> Result<QueryControlAttach, QueryLifecycleError> {
    let Some(novarocks::query_control_request::Command::Attach(attach)) = request.command.as_ref()
    else {
        return Err(QueryLifecycleError::invalid_manifest(
            "query control request must contain attach",
        ));
    };
    QueryControlAttach::new(
        decode_required_execution_id(attach.execution_id.as_ref())?,
        ParticipantManifestDigest::try_from_slice(&attach.init_digest)?,
        attach.frontend_owner_epoch,
    )
}

pub fn encode_query_control_command(
    command: &QueryControlCommand,
) -> novarocks::QueryControlRequest {
    let command = match command {
        QueryControlCommand::Heartbeat {
            sequence,
            sent_mono_ns,
        } => {
            novarocks::query_control_request::Command::Heartbeat(novarocks::QueryControlHeartbeat {
                sequence: *sequence,
                sent_mono_ns: *sent_mono_ns,
            })
        }
        QueryControlCommand::Abort { reason } => {
            novarocks::query_control_request::Command::Abort(novarocks::QueryControlAbort {
                reason: reason.clone(),
            })
        }
        QueryControlCommand::Finalize => {
            novarocks::query_control_request::Command::Finalize(novarocks::QueryControlFinalize {})
        }
        QueryControlCommand::TerminalAck { ack } => {
            novarocks::query_control_request::Command::TerminalAck(
                novarocks::QueryControlTerminalAck {
                    execution_id: Some(encode_execution_id(ack.execution_id())),
                    init_digest: ack.init_digest().as_bytes().to_vec(),
                    snapshot_version: ack.version(),
                    snapshot_digest: ack.digest().as_bytes().to_vec(),
                },
            )
        }
    };
    novarocks::QueryControlRequest {
        command: Some(command),
    }
}

pub fn decode_query_control_command(
    request: &novarocks::QueryControlRequest,
) -> Result<QueryControlCommand, QueryLifecycleError> {
    match request.command.as_ref() {
        Some(novarocks::query_control_request::Command::Heartbeat(heartbeat)) => {
            Ok(QueryControlCommand::Heartbeat {
                sequence: heartbeat.sequence,
                sent_mono_ns: heartbeat.sent_mono_ns,
            })
        }
        Some(novarocks::query_control_request::Command::Abort(abort))
            if !abort.reason.trim().is_empty() =>
        {
            Ok(QueryControlCommand::Abort {
                reason: abort.reason.clone(),
            })
        }
        Some(novarocks::query_control_request::Command::Finalize(_)) => {
            Ok(QueryControlCommand::Finalize)
        }
        Some(novarocks::query_control_request::Command::TerminalAck(ack)) => {
            Ok(QueryControlCommand::TerminalAck {
                ack: QueryTerminalAck::new(
                    decode_required_execution_id(ack.execution_id.as_ref())?,
                    ParticipantManifestDigest::try_from_slice(&ack.init_digest)?,
                    ack.snapshot_version,
                    QueryTerminalSnapshotDigest::try_from_slice(&ack.snapshot_digest)?,
                ),
            })
        }
        Some(novarocks::query_control_request::Command::Abort(_)) => Err(
            QueryLifecycleError::invalid_manifest("query control abort reason must not be empty"),
        ),
        Some(novarocks::query_control_request::Command::Attach(_)) => Err(
            QueryLifecycleError::invalid_manifest("attach is not a query control command"),
        ),
        None => Err(QueryLifecycleError::invalid_manifest(
            "query control command is required",
        )),
    }
}

pub fn encode_query_control_event(event: &QueryControlEvent) -> novarocks::QueryControlResponse {
    let event = match event {
        QueryControlEvent::ControlReady => {
            novarocks::query_control_response::Event::ControlReady(novarocks::QueryControlReady {})
        }
        QueryControlEvent::HeartbeatAck { sequence } => {
            novarocks::query_control_response::Event::HeartbeatAck(
                novarocks::QueryControlHeartbeatAck {
                    sequence: *sequence,
                },
            )
        }
        QueryControlEvent::LocalFailure { code, detail } => {
            novarocks::query_control_response::Event::LocalFailure(
                novarocks::QueryControlLocalFailure {
                    code: code.clone(),
                    detail: detail.clone(),
                },
            )
        }
        QueryControlEvent::LocalDrained => novarocks::query_control_response::Event::LocalDrained(
            novarocks::QueryControlLocalDrained {},
        ),
        QueryControlEvent::TerminalSnapshot { snapshot } => {
            novarocks::query_control_response::Event::TerminalSnapshot(
                encode_query_terminal_snapshot(snapshot),
            )
        }
        QueryControlEvent::TerminationAccepted { reason } => {
            novarocks::query_control_response::Event::TerminationAccepted(
                novarocks::QueryControlTerminationAccepted {
                    reason: encode_termination_reason(*reason),
                },
            )
        }
        QueryControlEvent::FragmentObservation { observation } => {
            novarocks::query_control_response::Event::FragmentObservation(
                encode_fragment_live_observation(observation),
            )
        }
    };
    novarocks::QueryControlResponse { event: Some(event) }
}

pub fn decode_query_control_event(
    response: &novarocks::QueryControlResponse,
) -> Result<QueryControlEvent, QueryLifecycleError> {
    match response.event.as_ref() {
        Some(novarocks::query_control_response::Event::ControlReady(_)) => {
            Ok(QueryControlEvent::ControlReady)
        }
        Some(novarocks::query_control_response::Event::HeartbeatAck(ack)) => {
            Ok(QueryControlEvent::HeartbeatAck {
                sequence: ack.sequence,
            })
        }
        Some(novarocks::query_control_response::Event::LocalFailure(failure))
            if !failure.code.trim().is_empty() && !failure.detail.trim().is_empty() =>
        {
            Ok(QueryControlEvent::LocalFailure {
                code: failure.code.clone(),
                detail: failure.detail.clone(),
            })
        }
        Some(novarocks::query_control_response::Event::TerminationAccepted(accepted)) => {
            Ok(QueryControlEvent::TerminationAccepted {
                reason: decode_termination_reason(accepted.reason)?,
            })
        }
        Some(novarocks::query_control_response::Event::LocalDrained(_)) => {
            Ok(QueryControlEvent::LocalDrained)
        }
        Some(novarocks::query_control_response::Event::TerminalSnapshot(snapshot)) => {
            Ok(QueryControlEvent::TerminalSnapshot {
                snapshot: decode_query_terminal_snapshot(snapshot)?,
            })
        }
        Some(novarocks::query_control_response::Event::FragmentObservation(observation)) => {
            Ok(QueryControlEvent::FragmentObservation {
                observation: decode_fragment_live_observation(observation)?,
            })
        }
        Some(novarocks::query_control_response::Event::LocalFailure(_)) => {
            Err(QueryLifecycleError::invalid_manifest(
                "local failure code and detail must not be empty",
            ))
        }
        None => Err(QueryLifecycleError::invalid_manifest(
            "query control event is required",
        )),
    }
}

pub fn encode_fragment_live_observation(
    observation: &FragmentLiveObservation,
) -> novarocks::FragmentLiveObservation {
    novarocks::FragmentLiveObservation {
        execution_id: Some(encode_execution_id(observation.execution_id())),
        init_digest: observation.init_digest().as_bytes().to_vec(),
        backend: Some(encode_backend_identity(observation.backend())),
        fragment_instance_id: Some(encode_unique_id(observation.fragment_instance_id())),
        sequence: observation.sequence(),
        input_rows: observation.input_rows(),
        output_rows: observation.output_rows(),
        elapsed_ms: observation.elapsed_ms(),
        profile: observation.profile().map(RuntimeProfileTree::to_proto),
    }
}

pub fn decode_fragment_live_observation(
    observation: &novarocks::FragmentLiveObservation,
) -> Result<FragmentLiveObservation, QueryLifecycleError> {
    let profile = observation
        .profile
        .as_ref()
        .map(RuntimeProfileTree::from_proto)
        .transpose()
        .map_err(QueryLifecycleError::invalid_manifest)?;
    FragmentLiveObservation::new(
        decode_required_execution_id(observation.execution_id.as_ref())?,
        ParticipantManifestDigest::try_from_slice(&observation.init_digest)?,
        decode_backend_identity(observation.backend.as_ref().ok_or_else(|| {
            QueryLifecycleError::invalid_manifest(
                "fragment observation backend identity is required",
            )
        })?)?,
        decode_unique_id(observation.fragment_instance_id.as_ref().ok_or_else(|| {
            QueryLifecycleError::invalid_manifest("fragment observation instance id is required")
        })?)?,
        observation.sequence,
        observation.input_rows,
        observation.output_rows,
        observation.elapsed_ms,
        profile,
    )
}

pub fn encode_query_terminal_snapshot(
    snapshot: &QueryTerminalSnapshot,
) -> novarocks::QueryTerminalSnapshot {
    use crate::runtime::sink_commit::{SinkLoadStats, TabletCommitInfo, TabletFailInfo};
    let fragments = snapshot
        .fragments()
        .iter()
        .map(|fragment| {
            let (outcome, error_code, error_detail) = match fragment.outcome() {
                super::terminal::FragmentTerminalOutcome::Succeeded => {
                    (1, String::new(), String::new())
                }
                super::terminal::FragmentTerminalOutcome::Failed { code, detail } => {
                    (2, code.clone(), detail.clone())
                }
                super::terminal::FragmentTerminalOutcome::Cancelled { detail } => {
                    (3, "CANCELLED".to_string(), detail.clone())
                }
                super::terminal::FragmentTerminalOutcome::IncompleteDrain { detail } => {
                    (4, "INCOMPLETE_DRAIN".to_string(), detail.clone())
                }
            };
            novarocks::QueryTerminalFragmentSnapshot {
                fragment_instance_id: Some(common::UniqueId {
                    hi: fragment.fragment_instance_id().hi,
                    lo: fragment.fragment_instance_id().lo,
                }),
                backend_num: fragment.backend_num(),
                outcome,
                error_code,
                error_detail,
                connector_staged_report_frames: fragment
                    .sink()
                    .connector_staged_report_frames
                    .iter()
                    .map(crate::query_execution::write::encode_connector_staged_report_frame)
                    .collect(),
                tablet_commit_infos: fragment
                    .sink()
                    .tablet_commit_infos
                    .iter()
                    .map(|value| novarocks::QueryTerminalTabletInfo {
                        tablet_id: value.tablet_id,
                        backend_id: value.backend_id,
                    })
                    .collect(),
                tablet_fail_infos: fragment
                    .sink()
                    .tablet_fail_infos
                    .iter()
                    .map(|value| novarocks::QueryTerminalTabletInfo {
                        tablet_id: value.tablet_id,
                        backend_id: value.backend_id,
                    })
                    .collect(),
                load_stats: Some(novarocks::QueryTerminalLoadStats {
                    loaded_rows: fragment.sink().load_stats.loaded_rows,
                    loaded_bytes: fragment.sink().load_stats.loaded_bytes,
                    filtered_rows: fragment.sink().load_stats.filtered_rows,
                }),
                profile: fragment.profile().map(|profile| profile.to_proto()),
                statistics_payload: fragment.statistics_payload().to_vec(),
            }
        })
        .collect();
    novarocks::QueryTerminalSnapshot {
        version: snapshot.version(),
        execution_id: Some(encode_execution_id(snapshot.execution_id())),
        backend: Some(encode_backend_identity(snapshot.backend())),
        init_digest: snapshot.init_digest().as_bytes().to_vec(),
        digest: snapshot.digest().as_bytes().to_vec(),
        fragments,
    }
}

pub fn decode_query_terminal_snapshot(
    value: &novarocks::QueryTerminalSnapshot,
) -> Result<QueryTerminalSnapshot, QueryLifecycleError> {
    use crate::runtime::sink_commit::{
        SinkCommitReportSnapshot, SinkLoadStats, TabletCommitInfo, TabletFailInfo,
    };
    let fragments = value
        .fragments
        .iter()
        .map(|fragment| {
            let id = fragment.fragment_instance_id.as_ref().ok_or_else(|| {
                QueryLifecycleError::invalid_manifest("terminal fragment instance id is required")
            })?;
            let outcome = match fragment.outcome {
                1 => super::terminal::FragmentTerminalOutcome::Succeeded,
                2 if !fragment.error_code.trim().is_empty() => {
                    super::terminal::FragmentTerminalOutcome::Failed {
                        code: fragment.error_code.clone(),
                        detail: fragment.error_detail.clone(),
                    }
                }
                3 => super::terminal::FragmentTerminalOutcome::Cancelled {
                    detail: fragment.error_detail.clone(),
                },
                4 => super::terminal::FragmentTerminalOutcome::IncompleteDrain {
                    detail: fragment.error_detail.clone(),
                },
                _ => {
                    return Err(QueryLifecycleError::invalid_manifest(
                        "invalid terminal fragment outcome",
                    ));
                }
            };
            let stats = fragment.load_stats.as_ref().ok_or_else(|| {
                QueryLifecycleError::invalid_manifest("terminal fragment load stats are required")
            })?;
            let sink = SinkCommitReportSnapshot {
                connector_staged_report_frames: fragment
                    .connector_staged_report_frames
                    .iter()
                    .map(crate::query_execution::write::decode_connector_staged_report_frame)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| QueryLifecycleError::invalid_manifest(error.message()))?,
                tablet_commit_infos: fragment
                    .tablet_commit_infos
                    .iter()
                    .map(|value| TabletCommitInfo {
                        tablet_id: value.tablet_id,
                        backend_id: value.backend_id,
                    })
                    .collect(),
                tablet_fail_infos: fragment
                    .tablet_fail_infos
                    .iter()
                    .map(|value| TabletFailInfo {
                        tablet_id: value.tablet_id,
                        backend_id: value.backend_id,
                    })
                    .collect(),
                load_stats: SinkLoadStats {
                    loaded_rows: stats.loaded_rows,
                    loaded_bytes: stats.loaded_bytes,
                    filtered_rows: stats.filtered_rows,
                },
            };
            let profile = fragment
                .profile
                .as_ref()
                .map(crate::runtime::profile::RuntimeProfileTree::from_proto)
                .transpose()
                .map_err(QueryLifecycleError::invalid_manifest)?;
            super::terminal::FragmentTerminalSnapshot::new(
                crate::common::types::UniqueId {
                    hi: id.hi,
                    lo: id.lo,
                },
                fragment.backend_num,
                outcome,
                sink,
                profile,
            )
            .and_then(|snapshot| {
                snapshot.with_statistics_payload(fragment.statistics_payload.clone())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot = QueryTerminalSnapshot::new(
        decode_required_execution_id(value.execution_id.as_ref())?,
        decode_backend_identity(value.backend.as_ref().ok_or_else(|| {
            QueryLifecycleError::invalid_manifest("terminal backend identity is required")
        })?)?,
        ParticipantManifestDigest::try_from_slice(&value.init_digest)?,
        fragments,
    )?;
    if value.version != snapshot.version()
        || QueryTerminalSnapshotDigest::try_from_slice(&value.digest)? != snapshot.digest()
    {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::Conflict,
            "terminal snapshot wire content has invalid version or digest",
        ));
    }
    Ok(snapshot)
}

fn encode_participant_manifest(
    manifest: &ParticipantManifest,
) -> Result<novarocks::ParticipantManifest, QueryLifecycleError> {
    Ok(novarocks::ParticipantManifest {
        execution_id: Some(encode_execution_id(manifest.execution_id())),
        backend: Some(encode_backend_identity(manifest.backend())),
        participant_roles: manifest
            .roles()
            .iter()
            .map(|role| match role {
                ParticipantRole::FragmentExecutor => 1,
                ParticipantRole::RuntimeFilterService => 2,
            })
            .collect(),
        expected_fragment_instance_ids: manifest
            .expected_fragment_instance_ids()
            .iter()
            .copied()
            .map(encode_unique_id)
            .collect(),
        query_options: Some(
            crate::protocol::native::encode::instance::encode_query_options(
                manifest.query_options().native(),
            ),
        ),
        query_deadline_unix_ms: manifest.query_deadline_unix_ms(),
        exchange_routes: manifest
            .exchange_routes()
            .iter()
            .map(encode_exchange_route)
            .collect(),
        runtime_filter: manifest
            .runtime_filter()
            .map(|contribution| {
                encode_runtime_filter_contribution(manifest.execution_id(), contribution)
            })
            .transpose()?,
        pre_start_timeout_ms: u64::try_from(manifest.pre_start_timeout().as_millis())
            .expect("validated pre-start timeout fits in u64 milliseconds"),
        report_endpoint: Some(encode_endpoint(manifest.report_endpoint())),
    })
}

fn decode_participant_manifest(
    manifest: &novarocks::ParticipantManifest,
) -> Result<ParticipantManifest, QueryLifecycleError> {
    let execution_id = decode_required_execution_id(manifest.execution_id.as_ref())?;
    let backend = manifest
        .backend
        .as_ref()
        .ok_or_else(|| {
            QueryLifecycleError::invalid_manifest("participant backend identity is required")
        })
        .and_then(decode_backend_identity)?;
    let roles = manifest
        .participant_roles
        .iter()
        .copied()
        .map(decode_participant_role)
        .collect::<Result<Vec<_>, _>>()?;
    let expected_fragment_instance_ids = manifest
        .expected_fragment_instance_ids
        .iter()
        .map(decode_unique_id)
        .collect::<Result<Vec<_>, _>>()?;
    let query_options = manifest
        .query_options
        .as_ref()
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("query options are required"))
        .and_then(|wire| {
            crate::protocol::native::query_options_contract::decode_query_options(wire)
                .map(ParticipantQueryOptions::new)
                .map_err(|error| QueryLifecycleError::invalid_manifest(error.to_string()))
        })?;
    let exchange_routes = manifest
        .exchange_routes
        .iter()
        .map(decode_exchange_route)
        .collect::<Result<Vec<_>, _>>()?;
    let runtime_filter = manifest
        .runtime_filter
        .as_ref()
        .map(|contribution| decode_runtime_filter_contribution(execution_id, contribution))
        .transpose()?;
    let report_endpoint = manifest
        .report_endpoint
        .as_ref()
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("report endpoint is required"))
        .and_then(decode_endpoint)?;

    ParticipantManifest::new(
        execution_id,
        backend,
        roles,
        expected_fragment_instance_ids,
        query_options,
        manifest.query_deadline_unix_ms,
        exchange_routes,
        runtime_filter,
        Duration::from_millis(manifest.pre_start_timeout_ms),
        report_endpoint,
    )
}

fn encode_execution_id(execution_id: QueryExecutionId) -> novarocks::QueryExecutionId {
    novarocks::QueryExecutionId {
        query_id: Some(common::UniqueId {
            hi: execution_id.query_id().high(),
            lo: execution_id.query_id().low(),
        }),
        attempt_id: execution_id.attempt_id().get(),
    }
}

fn decode_required_execution_id(
    execution_id: Option<&novarocks::QueryExecutionId>,
) -> Result<QueryExecutionId, QueryLifecycleError> {
    let execution_id = execution_id
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("query execution id is required"))?;
    let query_id = execution_id
        .query_id
        .as_ref()
        .ok_or_else(|| QueryLifecycleError::invalid_manifest("query id is required"))?;
    QueryExecutionId::new(
        crate::query_execution::contract::QueryId::new(query_id.hi, query_id.lo),
        AttemptId::new(execution_id.attempt_id)?,
    )
}

fn encode_unique_id(id: UniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: id.hi,
        lo: id.lo,
    }
}

fn decode_unique_id(id: &common::UniqueId) -> Result<UniqueId, QueryLifecycleError> {
    if id.hi == 0 && id.lo == 0 {
        return Err(QueryLifecycleError::invalid_manifest(
            "unique id must be nonzero",
        ));
    }
    Ok(UniqueId {
        hi: id.hi,
        lo: id.lo,
    })
}

fn encode_endpoint(endpoint: &QueryControlEndpoint) -> novarocks::QueryControlEndpoint {
    novarocks::QueryControlEndpoint {
        host: endpoint.host().to_string(),
        port: u32::from(endpoint.port()),
    }
}

fn decode_endpoint(
    endpoint: &novarocks::QueryControlEndpoint,
) -> Result<QueryControlEndpoint, QueryLifecycleError> {
    let port = u16::try_from(endpoint.port).map_err(|_| {
        QueryLifecycleError::invalid_manifest("query control endpoint port exceeds u16 range")
    })?;
    QueryControlEndpoint::new(endpoint.host.clone(), port)
}

fn encode_backend_identity(
    backend: &ParticipantBackendIdentity,
) -> novarocks::ParticipantBackendIdentity {
    novarocks::ParticipantBackendIdentity {
        backend_id: backend.backend_id(),
        endpoint: Some(encode_endpoint(backend.endpoint())),
        start_epoch: backend.start_epoch(),
    }
}

fn decode_backend_identity(
    backend: &novarocks::ParticipantBackendIdentity,
) -> Result<ParticipantBackendIdentity, QueryLifecycleError> {
    let endpoint = backend
        .endpoint
        .as_ref()
        .ok_or_else(|| {
            QueryLifecycleError::invalid_manifest("participant backend endpoint is required")
        })
        .and_then(decode_endpoint)?;
    ParticipantBackendIdentity::new(backend.backend_id, endpoint, backend.start_epoch)
}

fn decode_participant_role(role: i32) -> Result<ParticipantRole, QueryLifecycleError> {
    match role {
        1 => Ok(ParticipantRole::FragmentExecutor),
        2 => Ok(ParticipantRole::RuntimeFilterService),
        value => Err(QueryLifecycleError::invalid_manifest(format!(
            "unknown participant role {value}"
        ))),
    }
}

fn encode_exchange_route(route: &ExchangeRouteManifest) -> novarocks::ExchangeRouteManifest {
    novarocks::ExchangeRouteManifest {
        source_fragment_instance_id: Some(encode_unique_id(route.source_fragment_instance_id())),
        destination_fragment_instance_id: Some(encode_unique_id(
            route.destination_fragment_instance_id(),
        )),
        destination_node_id: route.destination_node_id(),
        sender_ordinal: route.sender_ordinal(),
        sender_count: route.sender_count(),
    }
}

fn decode_exchange_route(
    route: &novarocks::ExchangeRouteManifest,
) -> Result<ExchangeRouteManifest, QueryLifecycleError> {
    let source = route.source_fragment_instance_id.as_ref().ok_or_else(|| {
        QueryLifecycleError::invalid_manifest(
            "exchange route source fragment instance id is required",
        )
    })?;
    let destination = route
        .destination_fragment_instance_id
        .as_ref()
        .ok_or_else(|| {
            QueryLifecycleError::invalid_manifest(
                "exchange route destination fragment instance id is required",
            )
        })?;
    ExchangeRouteManifest::new(
        decode_unique_id(source)?,
        decode_unique_id(destination)?,
        route.destination_node_id,
        route.sender_ordinal,
        route.sender_count,
    )
}

fn encode_runtime_filter_contribution(
    execution_id: QueryExecutionId,
    contribution: &RuntimeFilterContribution,
) -> Result<novarocks::RuntimeFilterContribution, QueryLifecycleError> {
    let envelope = crate::protocol::native::encode_participant_install(
        execution_id.query_id().into_unique_id(),
        contribution.lifecycle(),
        contribution.install(),
    )
    .map_err(|error| QueryLifecycleError::invalid_manifest(error.to_string()))?;
    Ok(novarocks::RuntimeFilterContribution {
        participant_id: contribution.participant_id(),
        lifecycle: envelope.lifecycle,
        install: envelope.install,
        contribution_digest: contribution.digest().to_vec(),
    })
}

fn decode_runtime_filter_contribution(
    execution_id: QueryExecutionId,
    contribution: &novarocks::RuntimeFilterContribution,
) -> Result<RuntimeFilterContribution, QueryLifecycleError> {
    let digest: [u8; 32] = contribution
        .contribution_digest
        .as_slice()
        .try_into()
        .map_err(|_| {
            QueryLifecycleError::invalid_manifest(
                "runtime filter contribution digest must be 32 bytes",
            )
        })?;
    let envelope = filter::InstallRuntimeFilterDeploymentRequest {
        query_id: Some(common::UniqueId {
            hi: execution_id.query_id().high(),
            lo: execution_id.query_id().low(),
        }),
        deployment_epoch: execution_id.attempt_id().get(),
        participant_id: contribution.participant_id,
        lifecycle: contribution.lifecycle.clone(),
        install: contribution.install.clone(),
    };
    let decoded = crate::protocol::native::decode_participant_install(&envelope)
        .map_err(|error| QueryLifecycleError::invalid_manifest(error.to_string()))?;
    let canonical_digest = RuntimeFilterContribution::canonical_digest(
        execution_id,
        decoded.lifecycle,
        &decoded.install,
    )?;
    if digest != canonical_digest {
        return Err(QueryLifecycleError::invalid_manifest(
            "runtime filter contribution digest does not match canonical payload",
        ));
    }
    RuntimeFilterContribution::new(
        contribution.participant_id,
        decoded.lifecycle,
        decoded.install,
        canonical_digest,
    )
}

fn encode_init_outcome(outcome: QueryInitOutcome) -> i32 {
    match outcome {
        QueryInitOutcome::Applied => 1,
        QueryInitOutcome::AlreadyApplied => 2,
        QueryInitOutcome::RejectedConflict => 3,
        QueryInitOutcome::RejectedStaleBackend => 4,
        QueryInitOutcome::RejectedCapacity => 5,
        QueryInitOutcome::RejectedInvalidManifest => 6,
        QueryInitOutcome::RejectedTerminated => 7,
    }
}

fn decode_init_outcome(outcome: i32) -> Result<QueryInitOutcome, QueryLifecycleError> {
    match outcome {
        1 => Ok(QueryInitOutcome::Applied),
        2 => Ok(QueryInitOutcome::AlreadyApplied),
        3 => Ok(QueryInitOutcome::RejectedConflict),
        4 => Ok(QueryInitOutcome::RejectedStaleBackend),
        5 => Ok(QueryInitOutcome::RejectedCapacity),
        6 => Ok(QueryInitOutcome::RejectedInvalidManifest),
        7 => Ok(QueryInitOutcome::RejectedTerminated),
        value => Err(QueryLifecycleError::invalid_manifest(format!(
            "unknown query init outcome {value}"
        ))),
    }
}

fn encode_stage_outcome(outcome: QueryStageOutcome) -> i32 {
    match outcome {
        QueryStageOutcome::Applied => 1,
        QueryStageOutcome::AlreadyApplied => 2,
        QueryStageOutcome::RejectedConflict => 3,
        QueryStageOutcome::RejectedInvalidState => 4,
        QueryStageOutcome::RejectedInvalidBatch => 5,
        QueryStageOutcome::RejectedCapacity => 6,
        QueryStageOutcome::RejectedTerminated => 7,
        QueryStageOutcome::RejectedLocalFailure => 8,
    }
}

fn decode_stage_outcome(outcome: i32) -> Result<QueryStageOutcome, QueryLifecycleError> {
    match outcome {
        1 => Ok(QueryStageOutcome::Applied),
        2 => Ok(QueryStageOutcome::AlreadyApplied),
        3 => Ok(QueryStageOutcome::RejectedConflict),
        4 => Ok(QueryStageOutcome::RejectedInvalidState),
        5 => Ok(QueryStageOutcome::RejectedInvalidBatch),
        6 => Ok(QueryStageOutcome::RejectedCapacity),
        7 => Ok(QueryStageOutcome::RejectedTerminated),
        8 => Ok(QueryStageOutcome::RejectedLocalFailure),
        value => Err(QueryLifecycleError::invalid_manifest(format!(
            "unknown stage fragments outcome {value}"
        ))),
    }
}

fn encode_start_outcome(outcome: QueryStartOutcome) -> i32 {
    match outcome {
        QueryStartOutcome::Applied => 1,
        QueryStartOutcome::AlreadyStarted => 2,
        QueryStartOutcome::RejectedNotStaged => 3,
        QueryStartOutcome::RejectedConflict => 4,
        QueryStartOutcome::RejectedTerminated => 5,
    }
}

fn decode_start_outcome(outcome: i32) -> Result<QueryStartOutcome, QueryLifecycleError> {
    match outcome {
        1 => Ok(QueryStartOutcome::Applied),
        2 => Ok(QueryStartOutcome::AlreadyStarted),
        3 => Ok(QueryStartOutcome::RejectedNotStaged),
        4 => Ok(QueryStartOutcome::RejectedConflict),
        5 => Ok(QueryStartOutcome::RejectedTerminated),
        value => Err(QueryLifecycleError::invalid_manifest(format!(
            "unknown start prepared query outcome {value}"
        ))),
    }
}

fn encode_termination_reason(reason: QueryTerminationReason) -> i32 {
    match reason {
        QueryTerminationReason::CoordinatorAbort => 1,
        QueryTerminationReason::CoordinatorFinalize => 2,
        QueryTerminationReason::CoordinatorStreamLost => 3,
        QueryTerminationReason::CoordinatorHeartbeatTimeout => 4,
        QueryTerminationReason::LocalFailure => 5,
        QueryTerminationReason::PreStartTimeout => 6,
    }
}

fn decode_termination_reason(reason: i32) -> Result<QueryTerminationReason, QueryLifecycleError> {
    match reason {
        1 => Ok(QueryTerminationReason::CoordinatorAbort),
        2 => Ok(QueryTerminationReason::CoordinatorFinalize),
        3 => Ok(QueryTerminationReason::CoordinatorStreamLost),
        4 => Ok(QueryTerminationReason::CoordinatorHeartbeatTimeout),
        5 => Ok(QueryTerminationReason::LocalFailure),
        6 => Ok(QueryTerminationReason::PreStartTimeout),
        value => Err(QueryLifecycleError::invalid_manifest(format!(
            "unknown query termination reason {value}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use super::{
        FragmentLiveObservation, QueryInitRequest, decode_fragment_live_observation,
        decode_query_control_event, decode_query_init_request, decode_query_stage_request,
        decode_query_stage_response, decode_query_start_request, decode_query_start_response,
        decode_query_terminal_snapshot, encode_fragment_live_observation,
        encode_query_control_event, encode_query_init_request, encode_query_stage_request,
        encode_query_stage_response, encode_query_start_request, encode_query_start_response,
        encode_query_terminal_snapshot,
    };
    use crate::exec::spill::{SpillConfig, SpillMode};
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::identity::{AttemptId, QueryExecutionId};
    use crate::query_execution::lifecycle::manifest::{
        ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions, ParticipantRole,
        QueryControlEndpoint, RuntimeFilterContribution,
    };
    use crate::query_execution::lifecycle::{
        QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartOutcome,
        QueryStartRequest, StageDigest, StageDigestVersion, StageFragment,
    };
    use crate::runtime::profile::{ProfileUnit, RuntimeProfile};
    use crate::runtime::query_options::{QueryCacheOptions, QueryOptions};
    use crate::runtime::sink_commit::SinkCommitReportSnapshot;
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::install::{
        RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::routing::RuntimeFilterRoutingShard;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 42),
            AttemptId::new(7).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    #[test]
    fn terminal_snapshot_wire_round_trips_digest_with_profile_and_sink_facts() {
        let profile = RuntimeProfile::new("terminal-profile");
        profile.counter_set("RowsRead", ProfileUnit::Unit, 7);
        let sink = SinkCommitReportSnapshot::default();
        let fragment = crate::query_execution::lifecycle::FragmentTerminalSnapshot::new(
            crate::common::types::UniqueId { hi: 7, lo: 9 },
            3,
            crate::query_execution::lifecycle::FragmentTerminalOutcome::Succeeded,
            sink,
            Some(profile.to_native_tree()),
        )
        .expect("terminal fragment");
        let snapshot = crate::query_execution::lifecycle::QueryTerminalSnapshot::new(
            execution_id(),
            ParticipantBackendIdentity::new(
                3,
                QueryControlEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
                11,
            )
            .expect("backend identity"),
            crate::query_execution::lifecycle::ParticipantManifestDigest::new([9; 32]),
            vec![fragment],
        )
        .expect("terminal snapshot");

        let decoded = decode_query_terminal_snapshot(&encode_query_terminal_snapshot(&snapshot))
            .expect("terminal snapshot wire round trip");
        assert_eq!(decoded.digest(), snapshot.digest());
        assert_eq!(decoded.canonical_bytes(), snapshot.canonical_bytes());
    }

    fn observation_backend() -> ParticipantBackendIdentity {
        ParticipantBackendIdentity::new(
            3,
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("endpoint"),
            11,
        )
        .expect("backend identity")
    }

    #[test]
    fn fragment_live_observation_wire_round_trips_full_snapshot() {
        let profile = RuntimeProfile::new("live-profile");
        profile.counter_set("RowsRead", ProfileUnit::Unit, 7);
        let observation = FragmentLiveObservation::new(
            execution_id(),
            crate::query_execution::lifecycle::ParticipantManifestDigest::new([9; 32]),
            observation_backend(),
            crate::common::types::UniqueId { hi: 7, lo: 9 },
            1,
            11,
            7,
            5,
            Some(profile.to_native_tree()),
        )
        .expect("valid observation");

        let decoded =
            decode_fragment_live_observation(&encode_fragment_live_observation(&observation))
                .expect("observation wire round trip");
        assert_eq!(decoded, observation);

        let event = super::QueryControlEvent::FragmentObservation { observation };
        assert_eq!(
            decode_query_control_event(&encode_query_control_event(&event))
                .expect("event wire round trip"),
            event
        );
    }

    #[test]
    fn fragment_live_observation_accepts_progress_without_profile() {
        let observation = FragmentLiveObservation::new(
            execution_id(),
            crate::query_execution::lifecycle::ParticipantManifestDigest::new([8; 32]),
            observation_backend(),
            crate::common::types::UniqueId { hi: 1, lo: 2 },
            u64::MAX,
            1,
            2,
            3,
            None,
        )
        .expect("valid profile-less observation");

        assert_eq!(
            decode_fragment_live_observation(&encode_fragment_live_observation(&observation))
                .expect("observation decodes"),
            observation
        );
    }

    #[test]
    fn fragment_live_observation_rejects_invalid_identity_and_sequence() {
        let digest = crate::query_execution::lifecycle::ParticipantManifestDigest::new([7; 32]);
        assert!(
            FragmentLiveObservation::new(
                execution_id(),
                digest,
                observation_backend(),
                crate::common::types::UniqueId { hi: 0, lo: 0 },
                1,
                0,
                0,
                0,
                None,
            )
            .is_err()
        );
        assert!(
            FragmentLiveObservation::new(
                execution_id(),
                digest,
                observation_backend(),
                crate::common::types::UniqueId { hi: 1, lo: 2 },
                0,
                0,
                0,
                0,
                None,
            )
            .is_err()
        );

        let invalid_profile = crate::proto::novarocks::FragmentLiveObservation {
            execution_id: Some(super::encode_execution_id(execution_id())),
            init_digest: digest.as_bytes().to_vec(),
            backend: Some(super::encode_backend_identity(&observation_backend())),
            fragment_instance_id: Some(super::encode_unique_id(crate::common::types::UniqueId {
                hi: 1,
                lo: 2,
            })),
            sequence: 1,
            profile: Some(crate::proto::novarocks::RuntimeProfileTree::default()),
            ..Default::default()
        };
        assert!(decode_fragment_live_observation(&invalid_profile).is_err());
    }

    fn service_only_request() -> QueryInitRequest {
        let participant = RuntimeFilterParticipantId::new(3);
        let epoch = DeploymentEpoch::new(7);
        let install = RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(epoch, participant, BTreeMap::new()),
            RuntimeFilterRoutingShard::new(epoch, participant, BTreeMap::new())
                .expect("empty routing shard is structurally valid"),
        );
        let lifecycle = crate::protocol::native::RuntimeFilterQueryLifecycleOptions {
            delivery_expire: Duration::from_secs(5),
            query_expire: Duration::from_secs(30),
            transport_retry_interval: Duration::from_millis(200),
            transport_max_attempts: 3,
            transport_deadline: Duration::from_secs(2),
            transport_max_pending_entries: 1024,
            transport_max_pending_bytes: 1 << 20,
        };
        let contribution =
            RuntimeFilterContribution::from_compiled(execution_id(), 3, lifecycle, install)
                .expect("valid contribution");
        let options = QueryOptions {
            batch_size: Some(4096),
            query_timeout: Some(120),
            query_delivery_timeout: Some(60),
            enable_profile: true,
            runtime_profile_report_interval: Some(10),
            pipeline_dop: Some(4),
            exec_mem_limit: Some(1 << 30),
            connector_io_tasks_per_scan_operator: Some(8),
            orc_use_column_names: true,
            enable_file_metacache: true,
            enable_file_pagecache: true,
            enable_parquet_reader_page_index: true,
            runtime_filter_scan_wait_time_ms: Some(250),
            runtime_filter_wait_timeout_ms: Some(500),
            allow_throw_exception: true,
            group_concat_max_len: Some(1024),
            enable_join_runtime_bitset_filter: Some(true),
            global_runtime_filter_build_max_size: Some(1 << 20),
            cache: QueryCacheOptions {
                enable_scan_datacache: true,
                enable_populate_datacache: true,
                enable_datacache_async_populate_mode: true,
                enable_datacache_io_adaptor: true,
                enable_cache_select: true,
                datacache_evict_probability: Some(10),
                datacache_priority: Some(2),
                datacache_ttl_seconds: Some(300),
                datacache_sharing_work_period: Some(30),
            },
            spill: Some(SpillConfig {
                enable_spill: true,
                spill_mode: SpillMode::Force,
                spill_mem_limit_threshold: Some(0.75),
                spill_operator_min_bytes: Some(1024),
                spill_operator_max_bytes: Some(8192),
                spill_encode_level: Some(3),
                enable_spill_buffer_read: Some(true),
                max_spill_read_buffer_bytes_per_driver: Some(16384),
                spill_mem_table_size: Some(512),
                spill_mem_table_num: Some(2),
            }),
            ..Default::default()
        };
        let manifest = ParticipantManifest::new(
            execution_id(),
            ParticipantBackendIdentity::new(
                2,
                QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
                11,
            )
            .expect("valid backend"),
            [ParticipantRole::RuntimeFilterService],
            [],
            ParticipantQueryOptions::new(options),
            10_000,
            [],
            Some(contribution),
            Duration::from_secs(30),
            QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
        )
        .expect("valid service-only manifest");
        QueryInitRequest::from_manifest(manifest)
    }

    #[test]
    fn proto_query_lifecycle_round_trips_all_query_options() {
        let request = service_only_request();
        let wire = encode_query_init_request(&request).expect("request encodes");
        let decoded = decode_query_init_request(&wire).expect("request decodes");

        assert_eq!(decoded.manifest(), request.manifest());
        assert_eq!(decoded.digest(), request.digest());
        let options = decoded.manifest().query_options().native();
        assert!(options.orc_use_column_names);
        assert!(options.enable_file_metacache);
        assert!(options.enable_file_pagecache);
        assert!(options.enable_parquet_reader_page_index);
    }

    #[test]
    fn proto_query_lifecycle_rejects_runtime_filter_payload_digest_mismatch() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        let lifecycle = wire
            .manifest
            .as_mut()
            .expect("manifest")
            .runtime_filter
            .as_mut()
            .expect("runtime filter contribution")
            .lifecycle
            .as_mut()
            .expect("runtime filter lifecycle");
        lifecycle.delivery_expire_ms += 1;

        let error = decode_query_init_request(&wire)
            .expect_err("mutated runtime filter payload must not retain the original digest");

        assert_eq!(
            error.code(),
            super::QueryLifecycleErrorCode::InvalidManifest
        );
        assert_eq!(
            error.detail(),
            "runtime filter contribution digest does not match canonical payload"
        );
    }

    #[test]
    fn proto_query_lifecycle_rejects_unknown_role() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        wire.manifest.as_mut().expect("manifest").participant_roles = vec![99];

        assert!(decode_query_init_request(&wire).is_err());
    }

    #[test]
    fn proto_query_lifecycle_rejects_missing_execution_id() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        wire.manifest.as_mut().expect("manifest").execution_id = None;

        assert!(decode_query_init_request(&wire).is_err());
    }

    #[test]
    fn proto_query_lifecycle_rejects_wrong_digest_length() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        wire.init_digest.pop();

        assert!(decode_query_init_request(&wire).is_err());
    }

    #[test]
    fn proto_query_lifecycle_rejects_zero_attempt() {
        let mut wire = encode_query_init_request(&service_only_request()).expect("request encodes");
        wire.manifest
            .as_mut()
            .expect("manifest")
            .execution_id
            .as_mut()
            .expect("execution id")
            .attempt_id = 0;

        assert!(decode_query_init_request(&wire).is_err());
    }

    fn stage_fragment(lo: i64) -> StageFragment {
        StageFragment::new(
            crate::proto::plan::PlanFragment::default(),
            crate::proto::novarocks::InstanceParams {
                fragment_instance_id: Some(crate::proto::common::UniqueId { hi: 17, lo }),
                ..Default::default()
            },
        )
        .expect("valid stage fragment")
    }

    #[test]
    fn proto_stage_and_start_round_trip_typed_contracts() {
        let fragments = vec![stage_fragment(9), stage_fragment(2)];
        let digest = StageDigest::compute_v1(
            execution_id(),
            crate::query_execution::lifecycle::ParticipantManifestDigest::new([4; 32]),
            &fragments,
        )
        .expect("stage digest");
        let stage = QueryStageRequest::new(
            execution_id(),
            crate::query_execution::lifecycle::ParticipantManifestDigest::new([4; 32]),
            StageDigestVersion::V1,
            digest,
            fragments,
        )
        .expect("valid stage request");
        let decoded_stage = decode_query_stage_request(&encode_query_stage_request(&stage))
            .expect("stage request round trips");
        assert_eq!(decoded_stage, stage);

        let stage_ack = QueryStageAck::new(
            execution_id(),
            StageDigestVersion::V1,
            StageDigest::new([5; 32]),
            QueryStageOutcome::AlreadyApplied,
            "replayed",
        );
        assert_eq!(
            decode_query_stage_response(&encode_query_stage_response(&stage_ack))
                .expect("stage ack round trips"),
            stage_ack
        );

        let start = QueryStartRequest::new(
            execution_id(),
            StageDigestVersion::V1,
            StageDigest::new([5; 32]),
        );
        assert_eq!(
            decode_query_start_request(&encode_query_start_request(&start))
                .expect("start request round trips"),
            start
        );
        let start_ack = QueryStartAck::new(
            execution_id(),
            StageDigestVersion::V1,
            StageDigest::new([5; 32]),
            QueryStartOutcome::AlreadyStarted,
            "replayed",
        );
        assert_eq!(
            decode_query_start_response(&encode_query_start_response(&start_ack))
                .expect("start ack round trips"),
            start_ack
        );
    }

    #[test]
    fn proto_stage_rejects_unknown_version_and_incomplete_fragment() {
        let fragments = vec![stage_fragment(2)];
        let digest = StageDigest::compute_v1(
            execution_id(),
            crate::query_execution::lifecycle::ParticipantManifestDigest::new([4; 32]),
            &fragments,
        )
        .expect("stage digest");
        let request = QueryStageRequest::new(
            execution_id(),
            crate::query_execution::lifecycle::ParticipantManifestDigest::new([4; 32]),
            StageDigestVersion::V1,
            digest,
            fragments,
        )
        .expect("valid stage request");
        let mut wire = encode_query_stage_request(&request);
        wire.stage_digest_version = 99;
        assert!(decode_query_stage_request(&wire).is_err());

        let mut wire = encode_query_stage_request(&request);
        wire.fragments[0].plan = None;
        assert!(decode_query_stage_request(&wire).is_err());

        let mut wire = encode_query_stage_request(&request);
        wire.stage_digest[0] ^= 0xff;
        assert!(decode_query_stage_request(&wire).is_err());
    }
}
