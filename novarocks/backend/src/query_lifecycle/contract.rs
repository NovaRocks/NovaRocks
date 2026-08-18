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

use std::sync::Arc;
use std::time::Duration;

use novarocks::query_lifecycle::{QueryLifecycleError, QueryLifecycleErrorCode};
use novarocks_protocol::lifecycle::{
    FragmentLiveObservation, ParticipantTerminalOutcome, QueryAbortRequest, QueryControlAttach,
    QueryControlEndpoint, QueryControlEvent, QueryInitAck, QueryInitRequest, QueryStageAck,
    QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartOutcome, QueryStartRequest,
    QueryTerminalAck, QueryTerminalReportAck, QueryTerminationAck, QueryTerminationReason,
};

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

    fn coordinator_lost(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError>;
}

pub(crate) struct QueryControlAttachment {
    pub(crate) control: Arc<dyn BackendQueryControl>,
    pub(crate) events: tokio::sync::mpsc::Receiver<QueryControlEvent>,
    /// A single-slot, replaceable telemetry view. Correctness events remain on
    /// `events` so a congested profiler/progress producer cannot delay an ACK,
    /// drain barrier, or terminal snapshot.
    pub(crate) observations: tokio::sync::watch::Receiver<Option<FragmentLiveObservation>>,
}

pub(crate) trait QueryLifecycleIngress: Send + Sync + 'static {
    fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError>;

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck;

    /// Atomically records the participant-local stage contract. Fragment
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
        .expect("existing validated Stage request has a valid acknowledgement projection")
    }

    /// Releases one previously staged query bundle. A duplicate request with
    /// the same digest must not cause a second release.
    fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        QueryStartAck::new(
            request.execution_id(),
            request.digest_version(),
            request.digest(),
            QueryStartOutcome::RejectedNotStaged,
            "StartPreparedQuery is not supported by this lifecycle ingress",
        )
        .expect("existing validated Start request has a valid acknowledgement projection")
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
