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

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    time::Duration,
};

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
    StatementCommandCompletion, StatementCommandRowCount, StatementCompletionProjection,
    StatementProgressGeometry,
};
#[doc(hidden)]
pub use statement_action::{StatementActionSource, StatementProgressSlot};
use tokio::time::Instant;

use crate::cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView, governed_timeout_ms,
};
use crate::query_control::{
    ClientQueryCancellation, ClientQueryClaim, ClientQueryColumns, ClientQueryConclusion,
    ClientQueryEvent, ClientQueryHistoryStatus, ClientQueryLifecyclePort, ClientQueryObservation,
    ClientQueryRegistration, ClientQueryStart, ClientQueryText, ClientQueryTime,
    KillClientQueryError, ManagementCancellationReceipt,
};
use novarocks_workload_control::{
    BusinessPermit, CancellationReason, CancellationView, PendingQueryRoot, QueryConcurrencyPermit,
    ResultClosingCut, ResultWindowAlias, ResultWindowClass, ResultWindowGrant, RootAdmissionHandle,
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
    /// Validate the identity shape minted by Query Application at external boundaries.
    pub fn parse(value: &str) -> Result<Self, String> {
        let valid = value.len() == 27
            && value.bytes().enumerate().all(|(index, byte)| match index {
                8 | 15 | 21 => byte == b'_',
                22..=26 => byte.is_ascii_lowercase() || byte.is_ascii_digit(),
                _ => byte.is_ascii_digit(),
            });
        if !valid {
            return Err("invalid client query id shape".into());
        }
        Ok(Self(value.into()))
    }
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
    ClientStatement(ClientQueryId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionIdentity {
    origin: SessionOrigin,
    principal: Arc<str>,
}

impl SessionIdentity {
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

    pub(crate) fn principal_handle(&self) -> Arc<str> {
        Arc::clone(&self.principal)
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
    ClientQueryIdInUse,
    ClientQueryCancelled,
    ClientQueryLifecycleUnavailable,
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

pub trait QueryControlPort: Send + Sync + 'static {
    #[doc(hidden)]
    fn client_query_lifecycle(&self) -> Option<&dyn ClientQueryLifecyclePort> {
        None
    }

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

/// Read-only reporting to the original statement generation. A compiler
/// continuation can hold this without retaining its controlling Action.
pub struct ClientQueryTiming {
    port: Weak<dyn QueryControlPort>,
    statement: StatementToken,
}

impl ClientQueryTiming {
    pub fn record(&self, event: ClientQueryEvent, time: ClientQueryTime) {
        if let Some(port) = self.port.upgrade()
            && let Some(lifecycle) = port.client_query_lifecycle()
        {
            lifecycle.record_client_query_event(self.statement, event, time);
        }
    }

    pub fn start_control(self) {
        self.record(ClientQueryEvent::ControlStarted, ClientQueryTime::now());
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
        Ok(self.lease_session(token))
    }

    pub fn register_claimed_session(
        &self,
        identity: SessionIdentity,
        claim: ClientQueryClaim,
    ) -> Result<QuerySessionLease, QueryControlError> {
        let token = self
            .client_query_lifecycle()?
            .register_claimed_session(identity, claim)?;
        Ok(self.lease_session(token))
    }

    fn lease_session(&self, token: SessionToken) -> QuerySessionLease {
        QuerySessionLease {
            service: self.clone(),
            token,
            released: false,
        }
    }

    fn client_query_lifecycle(&self) -> Result<&dyn ClientQueryLifecyclePort, QueryControlError> {
        self.port
            .client_query_lifecycle()
            .ok_or(QueryControlError::ClientQueryLifecycleUnavailable)
    }

    pub fn register_client_query(
        &self,
        id: ClientQueryId,
        principal: Arc<str>,
        source: Option<Arc<str>>,
        text: Option<&ClientQueryText>,
        time: ClientQueryTime,
    ) -> Result<ClientQueryRegistration, QueryControlError> {
        self.client_query_lifecycle()?
            .register_client_query(id, principal, source, text, time)
    }

    pub fn withdraw_client_query(&self, registration: &ClientQueryRegistration) -> bool {
        self.client_query_lifecycle()
            .is_ok_and(|lifecycle| lifecycle.withdraw_client_query(registration))
    }

    pub fn claim_client_query(
        &self,
        registration: &ClientQueryRegistration,
        time: ClientQueryTime,
    ) -> ClientQueryStart {
        self.client_query_lifecycle()
            .map_or(ClientQueryStart::Unavailable, |lifecycle| {
                lifecycle.claim_client_query(registration, time)
            })
    }

    pub fn touch_client_query(
        &self,
        registration: &ClientQueryRegistration,
        time: ClientQueryTime,
    ) {
        if let Ok(lifecycle) = self.client_query_lifecycle() {
            lifecycle.touch_client_query(registration, time);
        }
    }

    pub fn request_unstarted_client_cancel(
        &self,
        registration: &ClientQueryRegistration,
        reason: QueryCancellationReason,
    ) -> bool {
        self.client_query_lifecycle()
            .is_ok_and(|lifecycle| lifecycle.request_unstarted_client_cancel(registration, reason))
    }

    pub fn client_query_cancellation(
        &self,
        registration: &ClientQueryRegistration,
    ) -> Option<ClientQueryCancellation> {
        self.client_query_lifecycle()
            .ok()?
            .client_query_cancellation(registration)
    }

    pub fn settle_client_query(
        &self,
        registration: &ClientQueryRegistration,
        proposed: ClientQueryConclusion,
        time: ClientQueryTime,
    ) -> Option<ClientQueryConclusion> {
        self.client_query_lifecycle()
            .ok()?
            .settle_client_query(registration, proposed, time)
    }

    pub fn record_client_query_event(
        &self,
        statement: StatementToken,
        event: ClientQueryEvent,
        time: ClientQueryTime,
    ) -> bool {
        self.client_query_lifecycle()
            .is_ok_and(|lifecycle| lifecycle.record_client_query_event(statement, event, time))
    }

    pub fn kill_client_query(
        &self,
        requester: SessionToken,
        target: &str,
        message: Option<&str>,
        now: ClientQueryTime,
    ) -> Result<Arc<ManagementCancellationReceipt>, KillClientQueryError> {
        self.client_query_lifecycle()
            .map_err(|_| KillClientQueryError::UnknownRequester)?
            .kill_client_query(requester, target, message, now)
    }

    pub fn observe_client_query(
        &self,
        principal: &str,
        id: &str,
        columns: ClientQueryColumns,
        now: ClientQueryTime,
    ) -> Option<ClientQueryObservation> {
        self.client_query_lifecycle()
            .ok()?
            .observe_client_query(principal, id, columns, now)
    }

    pub fn next_client_query(
        &self,
        principal: &str,
        after: Option<&str>,
        columns: ClientQueryColumns,
        now: ClientQueryTime,
    ) -> Option<ClientQueryObservation> {
        self.client_query_lifecycle()
            .ok()?
            .next_client_query(principal, after, columns, now)
    }

    pub fn subscribe_client_query_history(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<Option<std::time::Instant>>> {
        Some(
            self.client_query_lifecycle()
                .ok()?
                .subscribe_client_query_history(),
        )
    }

    pub fn client_query_history_status(&self) -> Option<ClientQueryHistoryStatus> {
        Some(
            self.client_query_lifecycle()
                .ok()?
                .client_query_history_status(),
        )
    }

    pub fn evict_client_query_history(&self, now: std::time::Instant) -> ClientQueryHistoryStatus {
        self.client_query_lifecycle().map_or_else(
            |_| ClientQueryHistoryStatus::default(),
            |lifecycle| lifecycle.evict_client_query_history(now),
        )
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
        let (root, registration) = self.register_query_root(root, session, statement_text)?;
        let scope = root.owner.scope();
        let admitted = (|| {
            let retention = if window == Some(ResultWindowClass::Internal) {
                Some(StatementResultCapacity::RootRetention(
                    ResultWindowAlias::from_root_owner(&root.owner)?,
                ))
            } else {
                None
            };
            Ok::<_, WorkError>((queue_original_query(&scope, window)?, retention))
        })();
        let (queue, retention) = match admitted {
            Ok(value) => value,
            Err(error) => {
                root.owner.complete_after_terminal_cancel_settled();
                let _ = self.port.fail_governed_statement(registration.token());
                return Err(GovernedQueryStatementBeginError::Admission(error));
            }
        };
        let grant = match queue.await {
            Ok(grant) => grant,
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
        let mut owner = self.own_registered_query(root, registration, timeout_ms);
        owner.result_window = grant
            .window
            .map(StatementResultCapacity::Window)
            .or(retention);
        owner.query_concurrency = Some(grant.permit);
        self.record_client_query_event(
            owner.token(),
            ClientQueryEvent::Admitted,
            ClientQueryTime::now(),
        );
        Ok(owner)
    }

    /// Register the original query before the property preparation Action
    /// starts. Its Root may provide scalar storage while it is not queued.
    pub fn begin_governed_query_session_preparation(
        &self,
        session: SessionToken,
        admission: &RootAdmissionHandle,
        clock: QuerySessionDeadline,
        statement_text: Arc<str>,
        class: ResultWindowClass,
    ) -> Result<GovernedQueryStatementOwner, GovernedQueryStatementBeginError> {
        if class == ResultWindowClass::Closing {
            return Err(GovernedQueryStatementBeginError::Admission(
                WorkError::Conflict,
            ));
        }
        let (root, timeout_ms) = match clock {
            QuerySessionDeadline::Inherit {
                deadline,
                timeout_ms,
            } => {
                let mut request = WorkRequest::new(WorkClass::Query);
                request.deadline = deadline;
                (admission.begin_query_root(request), timeout_ms)
            }
            QuerySessionDeadline::ResolveFromProperties => {
                (admission.begin_query_root_with_deferred_deadline(), None)
            }
        };
        let root = root.map_err(GovernedQueryStatementBeginError::Admission)?;
        let (root, registration) = self.register_query_root(root, session, Some(statement_text))?;
        let mut owner = self.own_registered_query(root, registration, timeout_ms);
        let retention = match ResultWindowAlias::from_root_owner(
            owner
                .execution_owner
                .as_ref()
                .expect("the original prepared query Root"),
        ) {
            Ok(retention) => retention,
            Err(error) => return Err(owner.refuse_unpublished_queue(error)),
        };
        owner.result_window = Some(StatementResultCapacity::RootRetention(retention));
        owner.session_query_admission = Some(SessionQueryAdmissionSlots {
            class,
            bind_timeout: matches!(clock, QuerySessionDeadline::ResolveFromProperties),
            queue_started: false,
        });
        Ok(owner)
    }

    fn register_query_root(
        &self,
        root: PendingQueryRoot,
        session: SessionToken,
        statement_text: Option<Arc<str>>,
    ) -> Result<(PendingQueryRoot, GovernedStatementRegistration), GovernedQueryStatementBeginError>
    {
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
        Ok((root, registration))
    }

    fn own_registered_query(
        &self,
        root: PendingQueryRoot,
        registration: GovernedStatementRegistration,
        timeout_ms: Option<u64>,
    ) -> GovernedQueryStatementOwner {
        GovernedQueryStatementOwner {
            service: GovernedControlLink::Strong(self.clone()),
            registration,
            scope: root.owner.scope(),
            execution_owner: Some(root.owner),
            business: None,
            query_concurrency: None,
            result_window: None,
            session_query_admission: None,
            accepted_delivery_cut: None,
            timeout_ms,
            control_start_pending: false,
            success_visibility_sealed: false,
            finished: false,
        }
    }

    /// Admit one protocol-visible statement under its actual workload class.
    /// The returned owner remains the sole holder of the statement generation,
    /// business permit, cancellation authority, and final protocol outcome.
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
        self.record_client_query_event(
            registration.token(),
            ClientQueryEvent::Admitted,
            ClientQueryTime::now(),
        );
        Ok(GovernedQueryStatementOwner {
            service: GovernedControlLink::Strong(self.clone()),
            registration,
            scope: root.owner.scope(),
            execution_owner: Some(root.owner),
            business: Some(root.business),
            query_concurrency: None,
            result_window,
            session_query_admission: None,
            accepted_delivery_cut: None,
            timeout_ms,
            control_start_pending: class == WorkClass::Management,
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

/// The clock selected by the original statement route. A plain EXPLAIN
/// keeps its outer admitted deadline while using the scoped plan settings.
#[derive(Clone, Copy)]
pub enum QuerySessionDeadline {
    Inherit {
        deadline: Option<Instant>,
        timeout_ms: Option<u64>,
    },
    ResolveFromProperties,
}

#[derive(Debug)]
pub enum SessionQueryAdmissionError {
    BindDeadline(WorkError),
    Queue(WorkError),
}

pub type SessionQueryAdmission =
    Pin<Box<dyn Future<Output = Result<QueuedQueryGrant, WorkError>> + Send>>;

/// The original queue return. Callers can only install it on the matching
/// statement before that Action derives its planning/runtime capabilities.
pub struct QueuedQueryGrant {
    permit: QueryConcurrencyPermit,
    window: Option<ResultWindowGrant>,
}

fn queue_original_query(
    scope: &WorkScope,
    class: Option<ResultWindowClass>,
) -> Result<SessionQueryAdmission, WorkError> {
    match class {
        None | Some(ResultWindowClass::Internal) => {
            let pending = scope.admit_query()?;
            Ok(Box::pin(async move {
                pending.await.map(|permit| QueuedQueryGrant {
                    permit,
                    window: None,
                })
            }))
        }
        Some(class) => {
            let pending = scope.admit_query_with_result(class)?;
            Ok(Box::pin(async move {
                pending.await.map(|(permit, window)| QueuedQueryGrant {
                    permit,
                    window: Some(window),
                })
            }))
        }
    }
}

struct SessionQueryAdmissionSlots {
    class: ResultWindowClass,
    bind_timeout: bool,
    queue_started: bool,
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
    query_result: Option<&'a mut Option<StatementResultCapacity>>,
    session_query_admission: Option<&'a mut Option<SessionQueryAdmissionSlots>>,
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
            query_result: None,
            session_query_admission: None,
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

    pub fn record_client_query_event(
        &self,
        event: ClientQueryEvent,
        time: ClientQueryTime,
    ) -> bool {
        self.visibility.as_ref().is_some_and(|visibility| {
            visibility
                .service
                .port()
                .client_query_lifecycle()
                .is_some_and(|lifecycle| {
                    lifecycle.record_client_query_event(visibility.token, event, time)
                })
        })
    }

    pub fn client_query_timing(&self) -> Option<ClientQueryTiming> {
        self.visibility
            .as_ref()
            .map(|visibility| ClientQueryTiming {
                port: Arc::downgrade(&visibility.service.port()),
                statement: visibility.token,
            })
    }

    fn with_query_permit(mut self, permit: &'a mut Option<QueryConcurrencyPermit>) -> Self {
        self.query_permit = Some(permit);
        self
    }

    /// Fix the original statement clock after the required CPU turn has
    /// exited, then queue the same Root. The Action retains this future.
    pub fn queue_after_session_preparation(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<SessionQueryAdmission, SessionQueryAdmissionError> {
        let slots = self
            .session_query_admission
            .as_deref_mut()
            .and_then(Option::as_mut)
            .ok_or(SessionQueryAdmissionError::Queue(WorkError::Conflict))?;
        if slots.queue_started {
            return Err(SessionQueryAdmissionError::Queue(WorkError::Conflict));
        }
        if slots.bind_timeout {
            let owner =
                self.execution_owner
                    .as_ref()
                    .ok_or(SessionQueryAdmissionError::BindDeadline(
                        WorkError::Released,
                    ))?;
            owner
                .bind_deferred_root_timeout(Instant::now(), timeout)
                .map_err(SessionQueryAdmissionError::BindDeadline)?;
        }
        slots.queue_started = true;
        queue_original_query(self.scope, Some(slots.class))
            .map_err(SessionQueryAdmissionError::Queue)
    }

    /// Accept ownership only from this Action's original Root admission.
    /// Validation leaves the original grant slot untouched on refusal.
    pub fn accept_session_query_grant_in(
        &mut self,
        grant: &mut Option<QueuedQueryGrant>,
    ) -> Result<(), WorkError> {
        self.scope.check()?;
        let slots = self
            .session_query_admission
            .as_deref_mut()
            .ok_or(WorkError::Conflict)?;
        let pending = slots
            .as_ref()
            .filter(|pending| pending.queue_started)
            .ok_or(WorkError::Conflict)?;
        let result = self
            .query_result
            .as_deref_mut()
            .ok_or(WorkError::Conflict)?;
        let permit = self
            .query_permit
            .as_deref_mut()
            .ok_or(WorkError::Conflict)?;
        let grant_ref = grant.as_ref().ok_or(WorkError::Conflict)?;
        if permit.is_some()
            || !grant_ref.permit.is_for_scope(self.scope)
            || !matches!(result, Some(StatementResultCapacity::RootRetention(_)))
        {
            return Err(WorkError::Conflict);
        }
        match (pending.class, grant_ref.window.as_ref()) {
            (ResultWindowClass::Internal, None) => {}
            (class, Some(window)) if class == window.class() && window.is_for_scope(self.scope) => {
            }
            _ => return Err(WorkError::Conflict),
        }
        let grant = grant.take().expect("matched the original queue grant");
        *permit = Some(grant.permit);
        let retired = grant
            .window
            .and_then(|window| result.replace(StatementResultCapacity::Window(window)));
        *slots = None;
        drop(retired);
        self.record_client_query_event(ClientQueryEvent::Admitted, ClientQueryTime::now());
        Ok(())
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

    pub fn timeout_ms(&self) -> Option<u64> {
        governed_timeout_ms(self.cancellation, self.timeout_ms)
    }

    /// Retain only the same original window when the actual result needs it.
    /// A missing alias remains missing; this view cannot acquire a new window.
    pub fn result_window_alias(&self) -> Option<ResultWindowAlias> {
        match &self.query_result {
            Some(slot) => slot.as_ref().map(StatementResultCapacity::retain_alias),
            None => self.result_window.cloned(),
        }
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
    session_query_admission: Option<SessionQueryAdmissionSlots>,
    accepted_delivery_cut: Option<ResultClosingCut>,
    timeout_ms: Option<u64>,
    control_start_pending: bool,
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
            query_result: Some(&mut self.result_window),
            session_query_admission: Some(&mut self.session_query_admission),
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

    /// Observe query planning without transferring the command control-start observation.
    pub fn client_query_timing(&self) -> ClientQueryTiming {
        ClientQueryTiming {
            port: Arc::downgrade(&self.service.port()),
            statement: self.token(),
        }
    }

    /// Transfer the one timing observation to the original command execution edge.
    pub fn take_control_timing(&mut self) -> Option<ClientQueryTiming> {
        std::mem::take(&mut self.control_start_pending).then(|| self.client_query_timing())
    }

    pub fn observe_control_start(&mut self) {
        if let Some(timing) = self.take_control_timing()
            && self.scope.check().is_ok()
        {
            timing.start_control();
        }
    }

    pub fn scope(&self) -> &WorkScope {
        &self.scope
    }

    pub fn cancellation(&self) -> &CancellationView {
        self.registration.cancellation()
    }

    pub fn timeout_ms(&self) -> Option<u64> {
        governed_timeout_ms(self.cancellation(), self.timeout_ms)
    }

    fn refuse_unpublished_queue(mut self, error: WorkError) -> GovernedQueryStatementBeginError {
        self.finished = true;
        self.complete_local_owner_after_terminal();
        let _ = self
            .service
            .port()
            .fail_governed_statement(self.registration.token());
        GovernedQueryStatementBeginError::Admission(error)
    }

    /// Retain the statement's original result author. Internal retention alone
    /// cannot create computation, manifest, or Native action authority.
    pub fn result_window_alias(&self) -> Option<ResultWindowAlias> {
        self.result_window
            .as_ref()
            .map(StatementResultCapacity::retain_alias)
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

    fn cancel_all(&self, _reason: QueryCancellationReason) {}
}
