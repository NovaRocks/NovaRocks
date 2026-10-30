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

//! Query-application session cancellation control contract.

use std::sync::{Arc, Weak};

mod background_source;
mod statement_action;
pub use background_source::{
    BackgroundActionCustody, BackgroundActionReady, BackgroundActionReceipt,
    BackgroundActionResultView, BackgroundActionSourceGeometry, GovernedBackgroundAction,
};
pub(crate) use statement_action::StatementProgressCursor;
pub use statement_action::{
    BackgroundActionResultGuard, GovernedStatementAction, GovernedStatementResidual,
    StatementActionAttempt, StatementActionFailure, StatementActionReady, StatementActionReceipt,
    StatementActionValue, StatementActionWaitError, StatementActionWaiter,
    StatementCommandCompletion, StatementProgressGeometry,
};
#[doc(hidden)]
pub use statement_action::{StatementActionSource, StatementProgressSlot};
use std::time::Duration;
use tokio::time::Instant;

use crate::cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};
use crate::client_connection::ClientConnectionToken;
use novarocks_workload_control::{
    BusinessPermit, CancellationReason, CancellationView, QueryConcurrencyPermit, ResultClosingCut,
    ResultWindowAlias, ResultWindowClass, ResultWindowGrant, RootAdmissionHandle,
    WorkCancellationRequestOutcome, WorkClass, WorkError, WorkOwner, WorkRequest, WorkScope,
    WorkSuccessSealer,
};

/// Query-application identity, never reused during this control owner's lifetime.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClientSessionKey(std::num::NonZeroU64);

impl ClientSessionKey {
    pub const fn new(value: u64) -> Option<Self> {
        match std::num::NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ClientQueryId(pub(crate) Box<str>);
impl ClientQueryId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl std::borrow::Borrow<str> for ClientQueryId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
impl std::fmt::Display for ClientQueryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionOrigin {
    Connection(ClientConnectionToken),
    ClientStatement(ClientQueryId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionIdentity {
    origin: SessionOrigin,
    principal: Arc<str>,
}

impl SessionIdentity {
    pub fn new(connection: ClientConnectionToken, principal: impl Into<Arc<str>>) -> Self {
        Self {
            origin: SessionOrigin::Connection(connection),
            principal: principal.into(),
        }
    }
    pub fn client_statement(query: ClientQueryId, principal: impl Into<Arc<str>>) -> Self {
        Self {
            origin: SessionOrigin::ClientStatement(query),
            principal: principal.into(),
        }
    }
    pub fn origin(&self) -> &SessionOrigin {
        &self.origin
    }
    pub fn principal(&self) -> &str {
        &self.principal
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionToken {
    key: ClientSessionKey,
}
impl SessionToken {
    pub const fn new(key: ClientSessionKey) -> Self {
        Self { key }
    }
    pub const fn key(self) -> ClientSessionKey {
        self.key
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

    pub fn remember_query_reason(&self, reason: QueryCancellationReason) {
        let mut first = self
            .first_query_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if first.is_none() {
            *first = Some(reason);
        }
    }

    pub fn first_query_reason(&self) -> Option<QueryCancellationReason> {
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
    pub fn new(token: StatementToken, cancellation: CancellationView) -> Self {
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
    SessionIdentityExhausted,
    ConnectionIdInUse,
    UnknownSession,
    StaleSession,
    StatementBusy,
    StatementActionCapacity,
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
    Failed,
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
/// One session as `SHOW PROCESSLIST` sees it.
///
/// This is a read-only projection of the control registry, minted under its
/// lock and handed out by value: a reader must never be able to reach live
/// cancellation state through it.
///
/// Only what the registry actually owns appears here. A client's host address
/// belongs to the MySQL listener and a session's current database is
/// session-local SQL state; neither is registry state, so neither is
/// reconstructed from a guess.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionProcess {
    pub connection_id: u32,
    pub principal: Arc<str>,
    /// How long the session has been in its current command.
    pub elapsed: Duration,
    /// The statement text while one is running. `None` means the session is
    /// idle, which is what makes this the `Command` column too.
    pub statement: Option<Arc<str>>,
}

impl SessionProcess {
    /// The MySQL `Command` column: a session either has a statement or does not.
    pub const fn command(&self) -> &'static str {
        if self.statement.is_some() {
            "Query"
        } else {
            "Sleep"
        }
    }
}

pub trait QueryControlPort: Send + Sync + 'static {
    fn mint_client_query_id(&self) -> ClientQueryId;
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
        statement_text: Option<Arc<str>>,
    ) -> Result<GovernedStatementRegistration, QueryControlError>;
    /// Every registered session, ordered by connection id.
    fn list_processes(&self) -> Vec<SessionProcess>;
    fn finish_statement(&self, statement: StatementToken) -> StatementFinishOutcome;
    fn finish_governed_statement(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementFinishOutcome;
    fn seal_governed_statement_visibility(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementVisibilitySealOutcome;
    /// Internal same-source dispatch for the original SQL action's poll loan.
    #[doc(hidden)]
    fn seal_governed_statement_action_visibility(
        &self,
        request: &StatementActionVisibilityRequest<'_>,
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

    /// Composition installs one original supervisor before publishing actions.
    fn attach_statement_progress(
        &self,
        _slot: &Arc<StatementProgressSlot>,
        _source_slots: std::num::NonZeroUsize,
    ) -> Result<(), QueryControlError> {
        Err(QueryControlError::UnknownSession)
    }
    fn statement_progress_slot(&self) -> Option<Arc<StatementProgressSlot>> {
        None
    }
    fn publish_statement_action_in(
        &self,
        _owner: &mut Option<GovernedQueryStatementOwner>,
        _action: &mut Option<Box<dyn GovernedStatementAction>>,
        _source: &Arc<StatementActionSource>,
    ) -> Result<(), QueryControlError> {
        Err(QueryControlError::UnknownSession)
    }
    fn next_statement_action(
        &self,
        _after: Option<usize>,
    ) -> Option<(usize, Option<Arc<StatementActionSource>>)> {
        None
    }
    fn retire_statement_action_source(&self, _source: &Arc<StatementActionSource>) -> bool {
        false
    }
    fn detach_statement_action(
        &self,
        _token: StatementToken,
        _source: &Arc<StatementActionSource>,
    ) -> bool {
        false
    }
    fn settle_statement_action_silent(
        &self,
        _token: StatementToken,
        _source: &Arc<StatementActionSource>,
    ) -> bool {
        false
    }
    fn close_statement_actions(&self) {}
    fn has_statement_actions(&self) -> bool {
        false
    }
    fn statement_action_failure(&self) -> Option<Arc<StatementActionFailure>> {
        None
    }
}

#[derive(Clone)]
pub struct QueryControlService {
    port: Arc<dyn QueryControlPort>,
}

impl QueryControlService {
    pub fn new(port: Arc<dyn QueryControlPort>) -> Self {
        Self { port }
    }

    pub fn mint_client_query_id(&self) -> ClientQueryId {
        self.port.mint_client_query_id()
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
        statement_text: Option<Arc<str>>,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        self.begin_governed_statement(
            session,
            admission,
            WorkClass::Query,
            deadline,
            timeout_ms,
            statement_text,
        )
    }

    /// Register protocol cancellation before awaiting the warehouse query
    /// queue. The returned owner holds the sole permit through the logical
    /// query's terminal protocol outcome; preparation completion alone cannot
    /// release it.
    pub async fn begin_queued_governed_query_statement(
        &self,
        session: SessionToken,
        admission: &RootAdmissionHandle,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement_text: Option<Arc<str>>,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        self.begin_queued(
            session,
            admission,
            deadline,
            timeout_ms,
            statement_text,
            None,
        )
        .await
    }

    /// Acquire the declared result custody with the query permit. Client and
    /// local results retain their original window. Internal statements retain
    /// the Root's actual byte budget; only a later computation takes a position.
    pub async fn begin_queued_governed_query_statement_with_result(
        &self,
        session: SessionToken,
        admission: &RootAdmissionHandle,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement_text: Option<Arc<str>>,
        class: ResultWindowClass,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        self.begin_queued(
            session,
            admission,
            deadline,
            timeout_ms,
            statement_text,
            Some(class),
        )
        .await
    }

    async fn begin_queued(
        &self,
        session: SessionToken,
        admission: &RootAdmissionHandle,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement_text: Option<Arc<str>>,
        window: Option<ResultWindowClass>,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        let mut request = WorkRequest::new(WorkClass::Query);
        request.deadline = deadline;
        let root = admission
            .begin_query_root(request)
            .map_err(GovernedQueryStatementBeginError::Admission)?;
        let cancellation = match GovernedStatementCancellation::new(&root.owner) {
            Ok(cancellation) => cancellation,
            Err(error) => {
                root.owner.complete_after_terminal_cancel_settled();
                return Err(GovernedQueryStatementBeginError::Admission(error));
            }
        };
        let registration = match self.port.begin_statement_with_governed_cancellation(
            session,
            cancellation,
            statement_text,
        ) {
            Ok(registration) => registration,
            Err(error) => {
                root.owner.complete_after_terminal_cancel_settled();
                return Err(GovernedQueryStatementBeginError::QueryControl(error));
            }
        };
        let scope = root.owner.scope();
        let admitted = match window {
            None => scope.admit_query().map(|admission| {
                Box::pin(async move { admission.await.map(|permit| (permit, None)) })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send>>
            }),
            Some(ResultWindowClass::Internal) => ResultWindowAlias::from_root_owner(&root.owner)
                .and_then(|retention| {
                    scope.admit_query().map(|admission| {
                        Box::pin(async move {
                            admission.await.map(|permit| {
                                (
                                    permit,
                                    Some(StatementResultCapacity::RootRetention(retention)),
                                )
                            })
                        })
                            as std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send>>
                    })
                }),
            Some(class) => scope.admit_query_with_result(class).map(|admission| {
                Box::pin(async move {
                    admission.await.map(|(permit, window)| {
                        (permit, Some(StatementResultCapacity::Window(window)))
                    })
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = _> + Send>>
            }),
        };
        let query_admission = match admitted {
            Ok(admission) => admission,
            Err(error) => {
                root.owner.complete_after_terminal_cancel_settled();
                let _ = self.port.fail_governed_statement(registration.token());
                return Err(GovernedQueryStatementBeginError::Admission(error));
            }
        };
        let (permit, result_window) = match query_admission.await {
            Ok(admitted) => admitted,
            Err(error) => {
                if matches!(error, WorkError::Cancelled(_)) {
                    root.owner.complete_after_terminal_cancel_settled();
                    let _ = self.port.finish_governed_statement(registration.token());
                } else {
                    root.owner.complete_after_terminal_cancel_settled();
                    let _ = self.port.fail_governed_statement(registration.token());
                }
                return Err(GovernedQueryStatementBeginError::Admission(error));
            }
        };
        Ok(GovernedQueryStatementOwner {
            service: GovernedControlLink::Strong(self.clone()),
            registration,
            scope,
            execution_owner: Some(root.owner),
            business: None,
            query_concurrency: Some(permit),
            result_window,
            accepted_delivery_cut: None,
            timeout_ms,
            success_visibility_sealed: false,
            finished: false,
        })
    }

    /// Admit one protocol-visible statement under its actual workload class.
    /// The returned owner remains the sole holder of the statement generation,
    /// business permit, cancellation authority, and final protocol outcome.
    /// Every registered session as `SHOW PROCESSLIST` sees it.
    pub fn list_processes(&self) -> Vec<SessionProcess> {
        self.port.list_processes()
    }

    pub fn begin_governed_statement(
        &self,
        session: SessionToken,
        admission: &RootAdmissionHandle,
        class: WorkClass,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement_text: Option<Arc<str>>,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        self.begin_nonqueued(
            session,
            admission,
            class,
            deadline,
            timeout_ms,
            statement_text,
            None,
        )
    }

    /// Acquire declared result custody with nonqueued business admission.
    /// Internal statement custody does not take a computation position.
    pub fn begin_governed_statement_with_result(
        &self,
        session: SessionToken,
        admission: &RootAdmissionHandle,
        class: WorkClass,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement_text: Option<Arc<str>>,
        window: ResultWindowClass,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        self.begin_nonqueued(
            session,
            admission,
            class,
            deadline,
            timeout_ms,
            statement_text,
            Some(window),
        )
    }

    fn begin_nonqueued(
        &self,
        session: SessionToken,
        admission: &RootAdmissionHandle,
        class: WorkClass,
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
        statement_text: Option<Arc<str>>,
        window: Option<ResultWindowClass>,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        let mut request = WorkRequest::new(class);
        request.deadline = deadline;
        let (root, result_window) = match window {
            None => admission.try_begin_root(request).map(|root| (root, None)),
            Some(ResultWindowClass::Internal) => {
                admission.try_begin_root(request).and_then(|root| {
                    match ResultWindowAlias::from_root_owner(&root.owner) {
                        Ok(retention) => Ok((
                            root,
                            Some(StatementResultCapacity::RootRetention(retention)),
                        )),
                        Err(error) => {
                            root.owner.complete_after_terminal_cancel_settled();
                            root.business.release();
                            Err(error)
                        }
                    }
                })
            }
            Some(window) => admission
                .try_begin_root_with_result(request, window)
                .map(|(root, window)| (root, Some(StatementResultCapacity::Window(window)))),
        }
        .map_err(GovernedQueryStatementBeginError::Admission)?;
        let cancellation = match GovernedStatementCancellation::new(&root.owner) {
            Ok(cancellation) => cancellation,
            Err(error) => {
                root.owner.complete_after_terminal_cancel_settled();
                root.business.release();
                return Err(GovernedQueryStatementBeginError::Admission(error));
            }
        };
        let registration = match self.port.begin_statement_with_governed_cancellation(
            session,
            cancellation,
            statement_text,
        ) {
            Ok(registration) => registration,
            Err(error) => {
                root.owner.complete_after_terminal_cancel_settled();
                root.business.release();
                return Err(GovernedQueryStatementBeginError::QueryControl(error));
            }
        };
        Ok(GovernedQueryStatementOwner {
            service: GovernedControlLink::Strong(self.clone()),
            registration,
            scope: root.owner.scope(),
            execution_owner: Some(root.owner),
            business: Some(root.business),
            query_concurrency: None,
            result_window,
            accepted_delivery_cut: None,
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
enum GovernedControlLink {
    Strong(QueryControlService),
    Stable(Weak<dyn QueryControlPort>),
}
impl GovernedControlLink {
    fn port(&self) -> Arc<dyn QueryControlPort> {
        match self {
            Self::Strong(service) => Arc::clone(&service.port),
            Self::Stable(port) => port
                .upgrade()
                .expect("original statement supervisor retains query control"),
        }
    }
}

/// A synchronous request borrowed only from the original SQL poll loan.
/// Read-only access serves the trusted control implementation; neither an
/// observer's Source alias nor a statement token can construct this request.
#[doc(hidden)]
pub struct StatementActionVisibilityRequest<'a> {
    statement: StatementToken,
    source: &'a Arc<StatementActionSource>,
}
impl StatementActionVisibilityRequest<'_> {
    pub fn statement(&self) -> StatementToken {
        self.statement
    }

    pub fn source(&self) -> &Arc<StatementActionSource> {
        self.source
    }
}

// This synchronous loan borrows the original owner's visibility fields. Its
// registered form also borrows the exact Source; a scope or token alone cannot
// construct it, and neither a Background owner nor a public view supplies it.
struct StatementVisibilityLoan<'a> {
    service: &'a GovernedControlLink,
    token: StatementToken,
    source: Option<&'a Arc<StatementActionSource>>,
    finished: &'a bool,
    accepted_delivery_cut: &'a Option<ResultClosingCut>,
    cancellation: &'a CancellationView,
    success_visibility_sealed: &'a mut bool,
}
impl StatementVisibilityLoan<'_> {
    fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        if *self.finished {
            return GovernedStatementVisibilitySealOutcome::Stale;
        }
        match self.accepted_delivery_cut {
            Some(ResultClosingCut::OriginatingFailure) => {
                return GovernedStatementVisibilitySealOutcome::Failed;
            }
            Some(ResultClosingCut::AcceptedCancellation) => {
                return GovernedStatementVisibilitySealOutcome::Cancelled(
                    self.cancellation
                        .reason()
                        .expect("accepted cancellation cut retains its reason"),
                );
            }
            None => {}
        }
        if *self.success_visibility_sealed {
            return GovernedStatementVisibilitySealOutcome::Sealed;
        }
        let outcome = match self.source {
            Some(source) => self
                .service
                .port()
                .seal_governed_statement_action_visibility(&StatementActionVisibilityRequest {
                    statement: self.token,
                    source,
                }),
            None => self
                .service
                .port()
                .seal_governed_statement_visibility(self.token),
        };
        if outcome == GovernedStatementVisibilitySealOutcome::Sealed {
            *self.success_visibility_sealed = true;
        }
        outcome
    }
}

/// A synchronous borrow of one original action's existing root slots.
/// This view owns no Work, result alias, registration, permit or authority;
/// constructing it neither admits work nor manufactures a background root.
/// The source author supplies its exact existing scope, owner slot and facts.
pub struct GovernedActionContext<'a> {
    scope: &'a WorkScope,
    execution_owner: &'a mut Option<WorkOwner>,
    cancellation: &'a CancellationView,
    timeout_ms: Option<u64>,
    result_window: Option<&'a ResultWindowAlias>,
    query_permit: Option<&'a mut Option<QueryConcurrencyPermit>>,
    visibility: Option<StatementVisibilityLoan<'a>>,
}

impl<'a> GovernedActionContext<'a> {
    pub fn new(
        scope: &'a WorkScope,
        execution_owner: &'a mut Option<WorkOwner>,
        cancellation: &'a CancellationView,
        timeout_ms: Option<u64>,
        result_window: Option<&'a ResultWindowAlias>,
    ) -> Self {
        Self {
            scope,
            execution_owner,
            cancellation,
            timeout_ms,
            result_window,
            query_permit: None,
            visibility: None,
        }
    }

    /// Seal only this original registered SQL action's visibility boundary.
    /// A plain view or Background action has no statement visibility loan.
    pub fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        match self.visibility.as_mut() {
            Some(visibility) => visibility.seal_success_visibility(),
            None => GovernedStatementVisibilitySealOutcome::Stale,
        }
    }

    fn with_query_permit(mut self, permit: &'a mut Option<QueryConcurrencyPermit>) -> Self {
        self.query_permit = Some(permit);
        self
    }

    /// A synchronous handoff from this exact SQL source after its active
    /// preparation has actually exited. A plain observer/context has no grant.
    pub fn handoff_query_permit_in(
        &mut self,
        receiver: &WorkOwner,
        destination: &mut Option<QueryConcurrencyPermit>,
    ) -> Result<novarocks_workload_control::WorkloadProgressNotification, WorkError> {
        QueryConcurrencyPermit::handoff_in(
            self.query_permit
                .as_deref_mut()
                .ok_or(WorkError::NotReady)?,
            self.scope,
            receiver,
            destination,
        )
    }

    pub fn scope(&self) -> &WorkScope {
        self.scope
    }

    pub fn cancellation(&self) -> &CancellationView {
        self.cancellation
    }

    pub const fn timeout_ms(&self) -> Option<u64> {
        self.timeout_ms
    }

    /// Retain only the same original window when the actual result needs it.
    /// A missing alias remains missing; this view cannot acquire a new window.
    pub fn result_window_alias(&self) -> Option<ResultWindowAlias> {
        self.result_window.cloned()
    }

    pub fn execution_owner_slot(&mut self) -> &mut Option<WorkOwner> {
        self.execution_owner
    }
}

enum StatementResultCapacity {
    Window(ResultWindowGrant),
    RootRetention(ResultWindowAlias),
}
impl StatementResultCapacity {
    fn retain_alias(&self) -> ResultWindowAlias {
        match self {
            Self::Window(window) => window.retain_alias(),
            Self::RootRetention(retention) => retention.clone(),
        }
    }
}

pub struct GovernedQueryStatementOwner {
    service: GovernedControlLink,
    registration: GovernedStatementRegistration,
    scope: WorkScope,
    execution_owner: Option<WorkOwner>,
    business: Option<BusinessPermit>,
    query_concurrency: Option<QueryConcurrencyPermit>,
    /// The original result policy. Root data retention carries the configured
    /// byte budget without occupying the later computation's position.
    result_window: Option<StatementResultCapacity>,
    accepted_delivery_cut: Option<ResultClosingCut>,
    timeout_ms: Option<u64>,
    success_visibility_sealed: bool,
    finished: bool,
}

impl GovernedQueryStatementOwner {
    /// Borrow protocol-owned facts for one synchronous action poll. The
    /// caller's original window alias remains outside this non-owning view.
    pub fn action_context<'a>(
        &'a mut self,
        result_window: Option<&'a ResultWindowAlias>,
    ) -> GovernedActionContext<'a> {
        GovernedActionContext::new(
            &self.scope,
            &mut self.execution_owner,
            self.registration.cancellation(),
            self.timeout_ms,
            result_window,
        )
        .with_query_permit(&mut self.query_concurrency)
    }

    // Only the SQL ActionBody in its original poll loan supplies this Source.
    fn registered_action_context<'a>(
        &'a mut self,
        result_window: Option<&'a ResultWindowAlias>,
        source: &'a Arc<StatementActionSource>,
    ) -> GovernedActionContext<'a> {
        GovernedActionContext {
            scope: &self.scope,
            execution_owner: &mut self.execution_owner,
            cancellation: self.registration.cancellation(),
            timeout_ms: self.timeout_ms,
            result_window,
            query_permit: Some(&mut self.query_concurrency),
            visibility: Some(StatementVisibilityLoan {
                service: &self.service,
                token: self.registration.token(),
                source: Some(source),
                finished: &self.finished,
                accepted_delivery_cut: &self.accepted_delivery_cut,
                cancellation: self.registration.cancellation(),
                success_visibility_sealed: &mut self.success_visibility_sealed,
            }),
        }
    }

    fn park_control_link(&mut self) {
        let port = self.service.port();
        self.service = GovernedControlLink::Stable(Arc::downgrade(&port));
    }
    fn restore_control_link(&mut self, service: &QueryControlService) {
        let original = self.service.port();
        assert!(
            Arc::ptr_eq(&original, &service.port),
            "same statement control authority"
        );
        self.service = GovernedControlLink::Strong(service.clone());
    }

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

    /// Retain the statement's original result author. Internal retention alone
    /// cannot create computation, manifest, or Native action authority.
    pub fn result_window_alias(&self) -> Option<ResultWindowAlias> {
        self.result_window
            .as_ref()
            .map(StatementResultCapacity::retain_alias)
    }

    pub(crate) fn release_transferred_result_window(&mut self) {
        // Fetch and transport aliases retain their own short-tail position.
        // The protocol has already moved its owned bytes into closing capacity.
        self.result_window.take();
    }

    pub(crate) fn execution_owner_matches_scope(&self) -> bool {
        // Both fields are minted together by the same statement constructor.
        // The WorkOwner is move-only; this read grants no capacity or locator.
        self.execution_owner
            .as_ref()
            .is_some_and(|owner| owner.is_for_scope(&self.scope))
    }

    /// Borrow the original stable slot for a synchronous, in-place QA start.
    /// A refusal must leave this slot intact; a successful start installs the
    /// same owner in QA before returning any notification tail.
    pub fn execution_owner_slot(&mut self) -> &mut Option<WorkOwner> {
        &mut self.execution_owner
    }

    /// Transfer the unique root owner into `QueryExecutionClient::start`.
    /// The statement generation and business permit stay with this protocol owner.
    pub fn take_execution_owner(&mut self) -> Option<WorkOwner> {
        self.execution_owner.take()
    }

    /// Returns the root responsibility after a synchronous executor has
    /// produced a protocol-visible result. The result owner must keep it live
    /// until the protocol reaches a terminal outcome, because that delivery
    /// may still acquire data credits under the same scope.
    pub fn restore_execution_owner(&mut self, owner: WorkOwner) {
        assert!(
            self.execution_owner.is_none(),
            "governed statement must not retain two execution owners"
        );
        self.execution_owner = Some(owner);
    }

    /// Releases only the execution root after synchronous work has stopped.
    /// The statement generation and business permit intentionally remain with
    /// this owner until the protocol terminal outcome is known. The protocol
    /// keeps the cancellation view; no execution dispatcher remains to consume
    /// a Cancel control after this local work has stopped.
    pub fn complete_execution(&mut self) {
        if let Some(owner) = self.execution_owner.take() {
            owner.complete_after_terminal_cancel_settled();
        }
    }

    fn release_terminal_concurrency(&mut self) {
        self.query_concurrency.take();
        self.business.take();
    }

    /// Accept a cancellation delivery cut after publication/fetch dispatch has
    /// stopped. Only the computation permit is returned here. The statement
    /// generation, business responsibility and external effects stay owned
    /// through the protocol's independent closing/actual exit boundary.
    pub fn accept_cancel_delivery_cut(&mut self) -> Result<bool, WorkError> {
        if self.finished || self.success_visibility_sealed || self.cancellation().reason().is_none()
        {
            return Err(WorkError::Conflict);
        }
        if self.accepted_delivery_cut.is_some() {
            return Ok(false);
        }
        self.accepted_delivery_cut = Some(ResultClosingCut::AcceptedCancellation);
        Ok(self.query_concurrency.take().is_some())
    }

    /// The originating failure owner uses this only after its failure cut has
    /// stopped new delivery work. It does not invent client cancellation or
    /// settle business/external effects, even after success visibility sealed.
    pub fn accept_failed_delivery_cut(&mut self) -> Result<bool, WorkError> {
        if self.finished {
            return Err(WorkError::Released);
        }
        if self.accepted_delivery_cut.is_some() {
            return Ok(false);
        }
        self.accepted_delivery_cut = Some(ResultClosingCut::OriginatingFailure);
        Ok(self.query_concurrency.take().is_some())
    }
    pub(crate) fn accepted_delivery_cut(&self) -> Option<ResultClosingCut> {
        self.accepted_delivery_cut
    }

    /// Atomically wins the terminal success boundary against every later
    /// cancellation request while retaining business and statement ownership.
    pub fn seal_success_visibility(&mut self) -> GovernedStatementVisibilitySealOutcome {
        StatementVisibilityLoan {
            service: &self.service,
            token: self.registration.token(),
            source: None,
            finished: &self.finished,
            accepted_delivery_cut: &self.accepted_delivery_cut,
            cancellation: self.registration.cancellation(),
            success_visibility_sealed: &mut self.success_visibility_sealed,
        }
        .seal_success_visibility()
    }

    pub fn finish(mut self) -> GovernedStatementFinishOutcome {
        self.finish_inner()
    }

    /// Finish a cancelled read before its root owner has been handed to the
    /// logical-execution supervisor. At this point no attempt, output, or
    /// external-effect owner exists, so the known cancellation control reaches
    /// its terminal boundary with the local read start failure.
    pub fn finish_unstarted_read_after_cancellation(mut self) -> GovernedStatementFinishOutcome {
        self.finish_inner()
    }

    pub fn fail(mut self, reason: CancellationReason) -> GovernedStatementFinishOutcome {
        if self.success_visibility_sealed {
            return self.protocol_fail_inner();
        } else {
            let _ = self
                .service
                .port()
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

    /// Only the same stable Source uses this after its actual action/result
    /// exit. Keep the original registry entry until every callback here returns.
    fn cleanup_abandoned_action_in_place(&mut self) {
        assert!(!self.finished, "one original statement cleanup attempt");
        self.finished = true;
        self.complete_local_owner_after_terminal();
        self.release_terminal_concurrency();
        // This grant may notify WC on actual last exit too. It is part of this
        // borrowed cleanup, never an implicit owner Drop after Source removal.
        self.result_window.take();
    }

    fn protocol_fail_inner(&mut self) -> GovernedStatementFinishOutcome {
        if self.finished {
            return GovernedStatementFinishOutcome::Stale;
        }
        self.finished = true;
        self.complete_local_owner_after_terminal();
        self.release_terminal_concurrency();

        self.service
            .port()
            .fail_governed_statement(self.registration.token())
    }

    fn finish_inner(&mut self) -> GovernedStatementFinishOutcome {
        if self.accepted_delivery_cut == Some(ResultClosingCut::OriginatingFailure) {
            return self.protocol_fail_inner();
        }
        if self.finished {
            return GovernedStatementFinishOutcome::Stale;
        }
        self.finished = true;
        self.complete_local_owner_after_terminal();
        self.release_terminal_concurrency();

        self.service
            .port()
            .finish_governed_statement(self.registration.token())
    }

    // A root still held here never reached the logical execution supervisor.
    // This terminal path has no actor left to consume its Cancel control;
    // the cancellation reason remains visible through the statement token.
    fn complete_local_owner_after_terminal(&mut self) {
        if let Some(owner) = self.execution_owner.take() {
            owner.complete_after_terminal_cancel_settled();
        }
    }
}

impl Drop for GovernedQueryStatementOwner {
    fn drop(&mut self) {
        if !self.finished {
            if self.success_visibility_sealed {
                self.finished = true;
                self.complete_local_owner_after_terminal();
                self.release_terminal_concurrency();
                let _ = self
                    .service
                    .port()
                    .fail_governed_statement(self.registration.token());
            } else {
                let _ = self.service.port().cancel_governed_statement(
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
    fn mint_client_query_id(&self) -> ClientQueryId {
        panic!("test control does not mint client queries")
    }
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
        _statement_text: Option<Arc<str>>,
    ) -> Result<GovernedStatementRegistration, QueryControlError> {
        Err(QueryControlError::UnknownSession)
    }

    fn list_processes(&self) -> Vec<SessionProcess> {
        Vec::new()
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

    fn seal_governed_statement_action_visibility(
        &self,
        _request: &StatementActionVisibilityRequest<'_>,
    ) -> GovernedStatementVisibilitySealOutcome {
        // This fixture does not publish or supervise SQL Actions.
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
