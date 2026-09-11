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

//! Frontend-owned session cancellation control contract.

use std::sync::Arc;
use tokio::time::Instant;

use crate::client_connection::ClientConnectionToken;
use crate::common::query_cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};
use novarocks_workload_control::{
    BusinessPermit, CancellationReason, CancellationView, RootAdmissionHandle,
    WorkCancellationRequestOutcome, WorkClass, WorkError, WorkOwner, WorkRequest, WorkScope,
    WorkSuccessSealer,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionIdentity {
    connection: ClientConnectionToken,
    principal: Arc<str>,
}

impl SessionIdentity {
    pub fn new(connection: ClientConnectionToken, principal: impl Into<Arc<str>>) -> Self {
        Self {
            connection,
            principal: principal.into(),
        }
    }

    pub const fn connection_id(&self) -> u32 {
        self.connection.connection_id()
    }

    pub const fn connection_token(&self) -> ClientConnectionToken {
        self.connection
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionToken {
    connection_id: u32,
    session_epoch: u64,
}

impl SessionToken {
    pub const fn new(connection_id: u32, session_epoch: u64) -> Self {
        Self {
            connection_id,
            session_epoch,
        }
    }

    pub const fn connection_id(self) -> u32 {
        self.connection_id
    }

    pub const fn session_epoch(self) -> u64 {
        self.session_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StatementToken {
    session: SessionToken,
    generation: u64,
}

impl StatementToken {
    pub const fn new(session: SessionToken, generation: u64) -> Self {
        Self {
            session,
            generation,
        }
    }

    pub const fn session(self) -> SessionToken {
        self.session
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone)]
pub struct StatementRegistration {
    token: StatementToken,
    cancellation: QueryCancellationView,
}

impl StatementRegistration {
    pub fn new(token: StatementToken, cancellation: QueryCancellationView) -> Self {
        Self {
            token,
            cancellation,
        }
    }

    pub const fn token(&self) -> StatementToken {
        self.token
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }
}

#[derive(Clone)]
pub struct GovernedStatementCancellation {
    requester: novarocks_workload_control::WorkCancellationRequester,
    success_sealer: WorkSuccessSealer,
    view: CancellationView,
    first_query_reason: Arc<std::sync::Mutex<Option<QueryCancellationReason>>>,
}

impl GovernedStatementCancellation {
    fn new(owner: &WorkOwner) -> Result<Self, WorkError> {
        Ok(Self {
            requester: owner.cancellation_requester(),
            success_sealer: owner.success_sealer(),
            view: owner.scope().cancellation()?,
            first_query_reason: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    pub fn requester(&self) -> &novarocks_workload_control::WorkCancellationRequester {
        &self.requester
    }

    pub fn view(&self) -> &CancellationView {
        &self.view
    }

    pub fn success_sealer(&self) -> &WorkSuccessSealer {
        &self.success_sealer
    }

    pub(crate) fn remember_query_reason(&self, reason: QueryCancellationReason) {
        let mut first = self
            .first_query_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if first.is_none() {
            *first = Some(reason);
        }
    }

    pub(crate) fn first_query_reason(&self) -> Option<QueryCancellationReason> {
        self.first_query_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[derive(Clone)]
pub struct GovernedStatementRegistration {
    token: StatementToken,
    cancellation: CancellationView,
}

impl GovernedStatementRegistration {
    pub(crate) fn new(token: StatementToken, cancellation: CancellationView) -> Self {
        Self {
            token,
            cancellation,
        }
    }

    pub const fn token(&self) -> StatementToken {
        self.token
    }

    pub fn cancellation(&self) -> &CancellationView {
        &self.cancellation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryControlError {
    ConnectionIdInUse,
    UnknownSession,
    StaleSession,
    StatementBusy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCancelOutcome {
    Requested,
    AlreadyRequested(QueryCancellationReason),
    Failed(WorkError),
    NoActiveStatement,
    UnknownSession,
    PermissionDenied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionKillAuthorization {
    Authorized(ClientConnectionToken),
    UnknownSession,
    PermissionDenied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatementFinishOutcome {
    Completed,
    Cancelled(QueryCancellationReason),
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GovernedStatementFinishOutcome {
    Completed,
    ProtocolFailed,
    Cancelled(CancellationReason),
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GovernedStatementVisibilitySealOutcome {
    Sealed,
    Cancelled(CancellationReason),
    Stale,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GovernedQueryStatementBeginError {
    Admission(WorkError),
    QueryControl(QueryControlError),
}

impl std::fmt::Display for GovernedQueryStatementBeginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(error) => write!(f, "admit governed query statement: {error}"),
            Self::QueryControl(error) => {
                write!(f, "register governed query statement generation: {error:?}")
            }
        }
    }
}

impl std::error::Error for GovernedQueryStatementBeginError {}

// Design: ADR-0010 (docs/adr/ADR-0010-explicit-query-cancellation-surface.md)
pub trait QueryControlPort: Send + Sync + 'static {
    fn register_session(
        &self,
        identity: SessionIdentity,
    ) -> Result<SessionToken, QueryControlError>;
    fn unregister_session(&self, token: SessionToken);
    fn begin_statement(
        &self,
        session: SessionToken,
    ) -> Result<StatementRegistration, QueryControlError>;
    fn begin_statement_with_cancellation(
        &self,
        session: SessionToken,
        _cancellation: QueryCancellationSource,
    ) -> Result<StatementRegistration, QueryControlError> {
        self.begin_statement(session)
    }
    fn begin_statement_with_governed_cancellation(
        &self,
        session: SessionToken,
        cancellation: GovernedStatementCancellation,
    ) -> Result<GovernedStatementRegistration, QueryControlError>;
    fn finish_statement(&self, statement: StatementToken) -> StatementFinishOutcome;
    fn finish_governed_statement(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementFinishOutcome;
    fn seal_governed_statement_visibility(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementVisibilitySealOutcome;
    fn fail_governed_statement(&self, statement: StatementToken) -> GovernedStatementFinishOutcome;
    fn cancel_governed_statement(
        &self,
        statement: StatementToken,
        reason: CancellationReason,
    ) -> Result<Option<WorkCancellationRequestOutcome>, WorkError>;
    fn cancel_session_statement(
        &self,
        session: SessionToken,
        reason: QueryCancellationReason,
    ) -> QueryCancelOutcome;
    fn kill_query(&self, requester: SessionToken, target_connection_id: u32) -> QueryCancelOutcome;
    fn authorize_connection_kill(
        &self,
        requester: SessionToken,
        target_connection_id: u32,
    ) -> ConnectionKillAuthorization;
    fn cancel_all(&self, reason: QueryCancellationReason);
}

#[derive(Clone)]
pub struct QueryControlService {
    port: Arc<dyn QueryControlPort>,
}

impl QueryControlService {
    pub fn new(port: Arc<dyn QueryControlPort>) -> Self {
        Self { port }
    }

    pub fn register_session(
        &self,
        identity: SessionIdentity,
    ) -> Result<QuerySessionLease, QueryControlError> {
        let token = self.port.register_session(identity)?;
        Ok(QuerySessionLease {
            service: self.clone(),
            token,
            released: false,
        })
    }

    pub fn begin_statement(
        &self,
        session: SessionToken,
    ) -> Result<ActiveStatementLease, QueryControlError> {
        self.begin_statement_with_cancellation(session, QueryCancellationSource::new())
    }

    pub fn begin_statement_with_cancellation(
        &self,
        session: SessionToken,
        cancellation: QueryCancellationSource,
    ) -> Result<ActiveStatementLease, QueryControlError> {
        let registration = self
            .port
            .begin_statement_with_cancellation(session, cancellation)?;
        Ok(ActiveStatementLease {
            service: self.clone(),
            registration,
            finished: false,
        })
    }

    /// Admit one query business responsibility and bind the active statement
    /// generation to that same workload cancellation authority.
    pub fn begin_governed_query_statement(
        &self,
        session: SessionToken,
        admission: &RootAdmissionHandle,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        let mut request = WorkRequest::new(WorkClass::Query);
        request.deadline = deadline;
        let root = admission
            .try_begin_root(request)
            .map_err(GovernedQueryStatementBeginError::Admission)?;
        let cancellation = match GovernedStatementCancellation::new(&root.owner) {
            Ok(cancellation) => cancellation,
            Err(error) => {
                root.owner.complete();
                root.business.release();
                return Err(GovernedQueryStatementBeginError::Admission(error));
            }
        };
        let registration = match self
            .port
            .begin_statement_with_governed_cancellation(session, cancellation)
        {
            Ok(registration) => registration,
            Err(error) => {
                root.owner.complete();
                root.business.release();
                return Err(GovernedQueryStatementBeginError::QueryControl(error));
            }
        };
        Ok(GovernedQueryStatementOwner {
            service: self.clone(),
            registration,
            scope: root.owner.scope(),
            execution_owner: Some(root.owner),
            business: Some(root.business),
            timeout_ms,
            success_visibility_sealed: false,
            finished: false,
        })
    }

    pub fn cancel_session_statement(
        &self,
        session: SessionToken,
        reason: QueryCancellationReason,
    ) -> QueryCancelOutcome {
        self.port.cancel_session_statement(session, reason)
    }

    pub fn kill_query(
        &self,
        requester: SessionToken,
        target_connection_id: u32,
    ) -> QueryCancelOutcome {
        self.port.kill_query(requester, target_connection_id)
    }

    pub fn authorize_connection_kill(
        &self,
        requester: SessionToken,
        target_connection_id: u32,
    ) -> ConnectionKillAuthorization {
        self.port
            .authorize_connection_kill(requester, target_connection_id)
    }

    pub fn cancel_all(&self, reason: QueryCancellationReason) {
        self.port.cancel_all(reason);
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution contract and lifecycle integration."
    )]
    pub(crate) fn for_test() -> Self {
        Self::new(Arc::new(TestQueryControlPort))
    }
}

pub struct QuerySessionLease {
    service: QueryControlService,
    token: SessionToken,
    released: bool,
}

impl QuerySessionLease {
    pub const fn token(&self) -> SessionToken {
        self.token
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            self.service.port.unregister_session(self.token);
            self.released = true;
        }
    }
}

impl Drop for QuerySessionLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

pub struct ActiveStatementLease {
    service: QueryControlService,
    registration: StatementRegistration,
    finished: bool,
}

impl ActiveStatementLease {
    pub const fn token(&self) -> StatementToken {
        self.registration.token()
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        self.registration.cancellation()
    }

    pub fn finish(&mut self) -> StatementFinishOutcome {
        if self.finished {
            return StatementFinishOutcome::Stale;
        }
        self.finished = true;
        self.service
            .port
            .finish_statement(self.registration.token())
    }
}

impl Drop for ActiveStatementLease {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

/// Move-only owner spanning query admission, async execution start, and the
/// final protocol outcome. The business permit and statement generation remain
/// live after the execution owner is handed to the query application.
pub struct GovernedQueryStatementOwner {
    service: QueryControlService,
    registration: GovernedStatementRegistration,
    scope: WorkScope,
    execution_owner: Option<WorkOwner>,
    business: Option<BusinessPermit>,
    timeout_ms: Option<u64>,
    success_visibility_sealed: bool,
    finished: bool,
}

impl GovernedQueryStatementOwner {
    pub const fn token(&self) -> StatementToken {
        self.registration.token()
    }

    pub fn scope(&self) -> &WorkScope {
        &self.scope
    }

    pub fn cancellation(&self) -> &CancellationView {
        self.registration.cancellation()
    }

    pub const fn timeout_ms(&self) -> Option<u64> {
        self.timeout_ms
    }

    /// Transfer the unique root owner into `QueryExecutionClient::start`.
    /// The statement generation and business permit stay with this protocol owner.
    pub fn take_execution_owner(&mut self) -> Option<WorkOwner> {
        self.execution_owner.take()
    }

    /// Atomically wins the terminal success boundary against every later
    /// cancellation request while retaining business and statement ownership.
    pub fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        if self.finished {
            return GovernedStatementVisibilitySealOutcome::Stale;
        }
        if self.success_visibility_sealed {
            return GovernedStatementVisibilitySealOutcome::Sealed;
        }
        let outcome = self
            .service
            .port
            .seal_governed_statement_visibility(self.registration.token());
        if outcome == GovernedStatementVisibilitySealOutcome::Sealed {
            self.success_visibility_sealed = true;
        }
        outcome
    }

    pub fn finish(mut self) -> GovernedStatementFinishOutcome {
        self.finish_inner()
    }

    pub fn fail(mut self, reason: CancellationReason) -> GovernedStatementFinishOutcome {
        if self.success_visibility_sealed {
            return self.protocol_fail_inner();
        } else {
            let _ = self
                .service
                .port
                .cancel_governed_statement(self.registration.token(), reason);
        }
        self.finish_inner()
    }

    /// Settle a protocol or encoding failure without inventing a client
    /// cancellation reason. This remains valid after success visibility was
    /// sealed: the statement is no longer cancellable, but EOF failure still
    /// cannot become a successful result.
    pub fn protocol_fail(mut self) -> GovernedStatementFinishOutcome {
        self.protocol_fail_inner()
    }

    fn protocol_fail_inner(&mut self) -> GovernedStatementFinishOutcome {
        if self.finished {
            return GovernedStatementFinishOutcome::Stale;
        }
        self.finished = true;
        if let Some(owner) = self.execution_owner.take() {
            owner.complete();
        }
        let outcome = self
            .service
            .port
            .fail_governed_statement(self.registration.token());
        self.business.take();
        outcome
    }

    fn finish_inner(&mut self) -> GovernedStatementFinishOutcome {
        if self.finished {
            return GovernedStatementFinishOutcome::Stale;
        }
        self.finished = true;
        if let Some(owner) = self.execution_owner.take() {
            owner.complete();
        }
        let outcome = self
            .service
            .port
            .finish_governed_statement(self.registration.token());
        self.business.take();
        outcome
    }
}

impl Drop for GovernedQueryStatementOwner {
    fn drop(&mut self) {
        if !self.finished {
            if self.success_visibility_sealed {
                self.finished = true;
                if let Some(owner) = self.execution_owner.take() {
                    owner.complete();
                }
                let _ = self
                    .service
                    .port
                    .fail_governed_statement(self.registration.token());
                self.business.take();
            } else {
                let _ = self.service.port.cancel_governed_statement(
                    self.registration.token(),
                    CancellationReason::OwnerDropped,
                );
                let _ = self.finish_inner();
            }
        }
    }
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged query-execution contract and lifecycle integration."
)]
struct TestQueryControlPort;

#[cfg(test)]
impl QueryControlPort for TestQueryControlPort {
    fn register_session(
        &self,
        _identity: SessionIdentity,
    ) -> Result<SessionToken, QueryControlError> {
        Err(QueryControlError::UnknownSession)
    }

    fn unregister_session(&self, _token: SessionToken) {}

    fn begin_statement(
        &self,
        _session: SessionToken,
    ) -> Result<StatementRegistration, QueryControlError> {
        Err(QueryControlError::UnknownSession)
    }

    fn finish_statement(&self, _statement: StatementToken) -> StatementFinishOutcome {
        StatementFinishOutcome::Stale
    }

    fn begin_statement_with_governed_cancellation(
        &self,
        _session: SessionToken,
        _cancellation: GovernedStatementCancellation,
    ) -> Result<GovernedStatementRegistration, QueryControlError> {
        Err(QueryControlError::UnknownSession)
    }

    fn finish_governed_statement(
        &self,
        _statement: StatementToken,
    ) -> GovernedStatementFinishOutcome {
        GovernedStatementFinishOutcome::Stale
    }

    fn seal_governed_statement_visibility(
        &self,
        _statement: StatementToken,
    ) -> GovernedStatementVisibilitySealOutcome {
        GovernedStatementVisibilitySealOutcome::Stale
    }

    fn fail_governed_statement(
        &self,
        _statement: StatementToken,
    ) -> GovernedStatementFinishOutcome {
        GovernedStatementFinishOutcome::Stale
    }

    fn cancel_governed_statement(
        &self,
        _statement: StatementToken,
        _reason: CancellationReason,
    ) -> Result<Option<WorkCancellationRequestOutcome>, WorkError> {
        Ok(None)
    }

    fn cancel_session_statement(
        &self,
        _session: SessionToken,
        _reason: QueryCancellationReason,
    ) -> QueryCancelOutcome {
        QueryCancelOutcome::UnknownSession
    }

    fn kill_query(
        &self,
        _requester: SessionToken,
        _target_connection_id: u32,
    ) -> QueryCancelOutcome {
        QueryCancelOutcome::UnknownSession
    }

    fn authorize_connection_kill(
        &self,
        _requester: SessionToken,
        _target_connection_id: u32,
    ) -> ConnectionKillAuthorization {
        ConnectionKillAuthorization::UnknownSession
    }

    fn cancel_all(&self, _reason: QueryCancellationReason) {}
}
