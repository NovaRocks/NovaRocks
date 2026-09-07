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

//! Backend-local native query-lifecycle role contracts.
//!
//! These traits describe BE-owned control and fallback behavior.  The neutral
//! values they carry are deliberately separate from this role-local surface.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use novarocks_proto_codec::lifecycle::{
    CredentialLeaseSecretEnvelope, FragmentLiveObservation, ParticipantTerminalOutcome,
    QueryAbortRequest, QueryControlAttach, QueryControlEndpoint, QueryControlEvent, QueryInitAck,
    QueryInitRequest, QueryStageAck, QueryStageOutcome, QueryStageRequest, QueryStartAck,
    QueryStartOutcome, QueryStartRequest, QueryTerminalAck, QueryTerminalReportAck,
    QueryTerminationAck, QueryTerminationReason, StageDigest,
};
use novarocks_spi::connector::{CatalogHandle, CredentialLeaseId};
use novarocks_types::BackendProcessId;

use crate::rpc::data_plane_handlers::{ExchangeRouteClaim, ExchangeRouteQuery};

/// Backend-local lifecycle failure categories.
///
/// Protocol owns only structural contract validation. Registry state,
/// liveness, transport, and admission failures remain Backend concerns and
/// keep the established native status mapping at the RPC boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryLifecycleErrorCode {
    InvalidManifest,
    Conflict,
    #[allow(
        dead_code,
        reason = "Retained for backend lifecycle owners that report stale membership after test-only control paths."
    )]
    StaleBackend,
    Capacity,
    Terminated,
    #[allow(
        dead_code,
        reason = "Retained for backend lifecycle owners that surface transport failures after test-only control paths."
    )]
    Transport,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryLifecycleError {
    code: QueryLifecycleErrorCode,
    detail: String,
}

impl QueryLifecycleError {
    pub(crate) fn new(code: QueryLifecycleErrorCode, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    pub(crate) fn invalid_manifest(detail: impl Into<String>) -> Self {
        Self::new(QueryLifecycleErrorCode::InvalidManifest, detail)
    }

    pub(crate) const fn code(&self) -> QueryLifecycleErrorCode {
        self.code
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl std::fmt::Display for QueryLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.detail)
    }
}

impl std::error::Error for QueryLifecycleError {}

impl From<novarocks_proto_codec::ProtocolError> for QueryLifecycleError {
    fn from(error: novarocks_proto_codec::ProtocolError) -> Self {
        Self::invalid_manifest(error.detail())
    }
}

pub(crate) trait BackendQueryControl: Send + Sync + 'static {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError>;

    fn abort(&self, reason: String) -> Result<(), QueryLifecycleError>;

    fn finalize(&self) -> Result<(), QueryLifecycleError>;

    fn terminal_ack(&self, _ack: QueryTerminalAck) -> Result<(), QueryLifecycleError> {
        Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::Terminated,
            "query terminal acknowledgement is not supported by this lifecycle owner",
        ))
    }

    fn credential_lease_prepare(
        &self,
        _envelope: CredentialLeaseSecretEnvelope,
    ) -> Result<(), QueryLifecycleError> {
        Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::Terminated,
            "credential lease prepare is not supported by this lifecycle owner",
        ))
    }

    fn credential_lease_commit(
        &self,
        _lease_id: CredentialLeaseId,
        _epoch: u64,
    ) -> Result<(), QueryLifecycleError> {
        Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::Terminated,
            "credential lease commit is not supported by this lifecycle owner",
        ))
    }

    fn coordinator_lost(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError>;
}

pub(crate) struct QueryControlAttachment {
    pub(crate) control: Arc<dyn BackendQueryControl>,
    pub(crate) events: tokio::sync::mpsc::Receiver<QueryControlEvent>,
    /// Independent bounded best-effort feedback. The RPC mux drains normal
    /// lifecycle correctness traffic before this receiver.
    pub(crate) runtime_filter_feedback: tokio::sync::mpsc::Receiver<QueryControlEvent>,
    /// A single-slot, replaceable telemetry view. Correctness events remain on
    /// `events` so a congested profiler/progress producer cannot delay an ACK,
    /// drain barrier, or terminal snapshot.
    #[allow(
        dead_code,
        reason = "The attachment preserves the telemetry receiver for native control-stream consumers outside this target."
    )]
    pub(crate) observations: tokio::sync::watch::Receiver<Option<FragmentLiveObservation>>,
}

/// Derives the stage identity for ingress implementations that reject the
/// request outright. The request carries the fragments, so the identity is
/// recoverable without the sender restating it.
fn stage_digest_of(request: &QueryStageRequest) -> StageDigest {
    StageDigest::compute(request.participant(), &request.fragments())
        .expect("validated QueryStageRequest always derives a stage digest")
}

pub(crate) trait QueryLifecycleIngress: Send + Sync + 'static {
    /// Immutable process identity generated by this BE during startup.
    fn backend_process_id(&self) -> BackendProcessId;

    /// A draining backend remains reachable for existing lifecycle control,
    /// but must report itself ineligible for new admission.
    fn is_draining(&self) -> bool {
        false
    }

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck;

    /// Invoked only by the BE listener after a concrete TLS connection has
    /// completed its handshake. Production overrides this to accept lease
    /// material; ordinary ingress implementations remain fail-closed.
    fn init_query_tls(&self, request: QueryInitRequest) -> QueryInitAck {
        self.init_query(request)
    }

    /// Reconciles retained catalog runtimes against one complete FE
    /// reachability snapshot. The lifecycle owner keeps the only catalog
    /// manager, so the RPC adapter cannot manufacture a parallel registry.
    fn prune_catalogs(&self, _reachable: BTreeSet<CatalogHandle>) -> CatalogPruneOutcome {
        CatalogPruneOutcome::Rejected {
            safe_detail: "catalog reachability pruning is not supported by this lifecycle ingress"
                .to_string(),
        }
    }

    /// Claims one native exchange frame against an active participant
    /// manifest. The data-plane never obtains authority from a receiver key.
    ///
    /// An ingress with no lifecycle state holds no destination at all, so it
    /// disclaims rather than refusing: a refusal here would be this owner
    /// speaking about another owner's destinations.
    fn claim_exchange_route(&self, _query: ExchangeRouteQuery) -> ExchangeRouteClaim {
        ExchangeRouteClaim::NotHeld
    }

    /// Atomically records the participant-local stage contract. Fragment
    /// materialization remains a backend concern; this contract boundary only
    /// returns a typed outcome so an ambiguous RPC retry can be idempotent.
    fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        QueryStageAck::new(
            request.execution_id(),
            stage_digest_of(&request),
            QueryStageOutcome::RejectedInvalidState,
            "StageFragments is not supported by this lifecycle ingress",
        )
        .expect("existing validated Stage request has a valid acknowledgement projection")
    }

    /// Releases one previously staged query bundle. A duplicate request with
    /// the same digest must not cause a second release.
    fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        QueryStartAck::new(
            request.execution_id(),
            request.digest(),
            QueryStartOutcome::RejectedNotStaged,
            "StartPreparedQuery is not supported by this lifecycle ingress",
        )
        .expect("existing validated Start request has a valid acknowledgement projection")
    }

    /// Admits one runtime split assignment for an already staged task. The
    /// default refuses rather than silently dropping an assignment, so an
    /// ingress that does not own task queues cannot look like it accepted one.
    fn task_update(
        &self,
        request: super::task_update::TaskUpdateRequest,
    ) -> super::task_update::TaskUpdateAck {
        let _ = request;
        super::task_update::TaskUpdateAck::rejected(
            super::task_update::TaskUpdateRejectionReason::NotAdmitted,
            "TaskUpdate is not supported by this lifecycle ingress",
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

/// Closed, credential-free result of reconciling catalog reachability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CatalogPruneOutcome {
    Accepted,
    Rejected { safe_detail: String },
}

/// BE-local failure category for reporting an already frozen terminal outcome
/// through the fallback transport. It is intentionally independent from the
/// Frontend-owned lifecycle transport error because fallback delivery is a BE
/// role concern and must not introduce a Backend-to-Frontend dependency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryTerminalFallbackTransportError {
    detail: String,
}

impl QueryTerminalFallbackTransportError {
    pub(crate) fn unavailable(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for QueryTerminalFallbackTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Unavailable: {}", self.detail)
    }
}

impl std::error::Error for QueryTerminalFallbackTransportError {}

/// BE-owned fallback transport. Delivery never reconnects or recreates the
/// control session; it only reports the already frozen outcome.
pub(crate) trait QueryTerminalFallbackTransport: Send + Sync + 'static {
    fn report_query_terminal(
        &self,
        endpoint: &QueryControlEndpoint,
        outcome: ParticipantTerminalOutcome,
        timeout: Duration,
    ) -> Result<QueryTerminalReportAck, QueryTerminalFallbackTransportError>;
}
