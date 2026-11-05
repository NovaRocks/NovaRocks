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

//! Query-application-owned session and statement cancellation state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::cancellation::{QueryCancellationReason, QueryCancellationSource};
use crate::session_control::{
    ClientQueryId, ClientSessionKey, GovernedStatementCancellation, GovernedStatementFinishOutcome,
    GovernedStatementRegistration, GovernedStatementVisibilitySealOutcome, QueryCancelOutcome,
    QueryControlError, QueryControlPort, QueryControlService, SessionIdentity, SessionOrigin,
    SessionToken, StatementFinishOutcome, StatementRegistration, StatementToken,
};
use novarocks_workload_control::{
    CancellationReason, WorkCancellationRequestOutcome, WorkError, WorkSuccessSealOutcome,
};

// Design: ADR-0102 (docs/adr/ADR-0102-mysql-kill-connection-lifecycle-ownership.md)
#[cfg(novarocks_dev_controls)]
pub type ClientStatementRegistrationObserver =
    Arc<dyn Fn(&ClientQueryId, SessionToken) + Send + Sync>;

pub struct QueryApplicationControl {
    query_ids: crate::client_query_id::ClientQueryIdGenerator,
    #[cfg(novarocks_dev_controls)]
    client_statement_observer: std::sync::OnceLock<ClientStatementRegistrationObserver>,
    state: Mutex<QueryControlState>,
}

#[derive(Default)]
struct QueryControlState {
    next_session_key: u64,
    statement_progress: std::sync::Weak<crate::session_control::StatementProgressSlot>,
    statement_actions_open: bool,
    statement_progress_installed: bool,
    statement_sources: Box<[Option<Arc<crate::session_control::StatementActionSource>>]>,
    sessions: BTreeMap<ClientSessionKey, SessionEntry>,
    by_client_query: BTreeMap<ClientQueryId, ClientSessionKey>,
}

impl QueryControlState {
    fn remove_session(&mut self, key: ClientSessionKey) {
        if let Some(entry) = self.sessions.remove(&key) {
            let SessionOrigin::ClientStatement(query) = entry.origin;
            self.by_client_query.remove(&query);
        }
    }
}

struct SessionEntry {
    origin: SessionOrigin,
    next_statement_generation: u64,
    active: Option<ActiveStatement>,
    unregister_pending: bool,
}

struct ActiveStatement {
    generation: u64,
    action: Option<Arc<crate::session_control::StatementActionSource>>,
    cancellation: ActiveStatementCancellation,
    success_visibility_sealed: bool,
}

enum ActiveStatementCancellation {
    Legacy(QueryCancellationSource),
    Governed(GovernedStatementCancellation),
}

impl ActiveStatementCancellation {
    fn request(&self, reason: QueryCancellationReason) -> QueryCancelOutcome {
        match self {
            Self::Legacy(cancellation) => match cancellation.request(reason) {
                crate::cancellation::QueryCancellationRequestResult::Requested => {
                    QueryCancelOutcome::Requested
                }
                crate::cancellation::QueryCancellationRequestResult::AlreadyRequested(reason) => {
                    QueryCancelOutcome::AlreadyRequested(reason)
                }
            },
            Self::Governed(cancellation) => {
                let workload_reason = workload_cancellation_reason(&reason);
                match cancellation
                    .requester()
                    .request_with_outcome(workload_reason)
                {
                    Ok(WorkCancellationRequestOutcome::Requested) => {
                        cancellation.remember_query_reason(reason);
                        QueryCancelOutcome::Requested
                    }
                    Ok(WorkCancellationRequestOutcome::AlreadyRequested(existing)) => {
                        let first = cancellation.first_query_reason();
                        QueryCancelOutcome::AlreadyRequested(query_cancellation_reason(
                            existing,
                            first.as_ref().unwrap_or(&reason),
                        ))
                    }
                    Ok(WorkCancellationRequestOutcome::SuccessSealed) => {
                        QueryCancelOutcome::NoActiveStatement
                    }
                    Err(error) => QueryCancelOutcome::Failed(error),
                }
            }
        }
    }
}

fn workload_cancellation_reason(reason: &QueryCancellationReason) -> CancellationReason {
    match reason {
        QueryCancellationReason::ExecutionCancellationRequested => CancellationReason::Requested,
        QueryCancellationReason::ExecutionOwnerDropped => CancellationReason::OwnerDropped,
        QueryCancellationReason::ExplicitKill { requester_session } => {
            CancellationReason::ExplicitKill {
                requester_session: requester_session.get(),
            }
        }
        QueryCancellationReason::ExplicitKillConnection { requester_session } => {
            CancellationReason::ExplicitKillConnection {
                requester_session: requester_session.get(),
            }
        }
        QueryCancellationReason::ClientCancelRequested => CancellationReason::ClientCancelRequested,
        QueryCancellationReason::ClientDisconnected => CancellationReason::ClientDisconnected,
        QueryCancellationReason::DeadlineExceeded { .. } => CancellationReason::DeadlineExceeded,
        QueryCancellationReason::FrontendDrainDeadlineExceeded { .. } => {
            CancellationReason::FrontendDrainDeadlineExceeded
        }
        QueryCancellationReason::ServerShutdown => CancellationReason::ServerShutdown,
    }
}

fn query_cancellation_reason(
    reason: CancellationReason,
    requested: &QueryCancellationReason,
) -> QueryCancellationReason {
    match reason {
        CancellationReason::ExplicitKill { requester_session } => {
            QueryCancellationReason::ExplicitKill {
                requester_session: ClientSessionKey::new(requester_session)
                    .expect("registered requester session is nonzero"),
            }
        }
        CancellationReason::ExplicitKillConnection { requester_session } => {
            QueryCancellationReason::ExplicitKillConnection {
                requester_session: ClientSessionKey::new(requester_session)
                    .expect("registered requester session is nonzero"),
            }
        }
        CancellationReason::ClientCancelRequested => QueryCancellationReason::ClientCancelRequested,
        CancellationReason::ClientDisconnected => QueryCancellationReason::ClientDisconnected,
        CancellationReason::DeadlineExceeded => match requested {
            QueryCancellationReason::DeadlineExceeded { timeout_ms } => {
                QueryCancellationReason::DeadlineExceeded {
                    timeout_ms: *timeout_ms,
                }
            }
            _ => QueryCancellationReason::DeadlineExceeded { timeout_ms: 0 },
        },
        CancellationReason::FrontendDrainDeadlineExceeded => match requested {
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms } => {
                QueryCancellationReason::FrontendDrainDeadlineExceeded {
                    timeout_ms: *timeout_ms,
                }
            }
            _ => QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 0 },
        },
        CancellationReason::ServerShutdown => QueryCancellationReason::ServerShutdown,
        CancellationReason::Requested => QueryCancellationReason::ExecutionCancellationRequested,
        CancellationReason::OwnerDropped => QueryCancellationReason::ExecutionOwnerDropped,
    }
}

/// Actual existing session-registry values, including its inline action slot.
/// The caller supplies the real client/session bound and BTree allocator fee.
#[derive(Clone, Copy, Debug)]
pub struct QueryControlSessionGeometry {
    pub owner: std::alloc::Layout,
    pub record: std::alloc::Layout,
}

#[cfg(any(test, novarocks_dev_controls))]
impl Default for QueryApplicationControl {
    fn default() -> Self {
        Self::new(novarocks_types::FrontendProcessId::new_v7())
    }
}
impl QueryApplicationControl {
    pub fn new(process: novarocks_types::FrontendProcessId) -> Self {
        Self {
            state: Mutex::new(QueryControlState::default()),
            query_ids: crate::client_query_id::ClientQueryIdGenerator::new(process),
            #[cfg(novarocks_dev_controls)]
            client_statement_observer: std::sync::OnceLock::new(),
        }
    }
    /// Read the active session for this exact client statement without retaining it.
    pub fn current_session_for_client_query(&self, query: &str) -> Option<SessionToken> {
        let state = self.lock();
        let key = state.by_client_query.get(query)?;
        state
            .sessions
            .get(key)
            .filter(|entry| !entry.unregister_pending)?;
        Some(SessionToken::new(*key))
    }

    #[cfg(novarocks_dev_controls)]
    pub fn install_client_statement_observer(
        &self,
        observer: ClientStatementRegistrationObserver,
    ) -> Result<(), String> {
        self.client_statement_observer
            .set(observer)
            .map_err(|_| "client statement observer was already installed".into())
    }

    pub fn session_geometry() -> QueryControlSessionGeometry {
        QueryControlSessionGeometry {
            owner: std::alloc::Layout::new::<Self>(),
            record: std::alloc::Layout::new::<(ClientSessionKey, SessionEntry)>(),
        }
    }
    #[cfg(any(test, novarocks_dev_controls))]
    pub fn service() -> QueryControlService {
        QueryControlService::new(Arc::new(Self::default()))
    }

    fn seal_statement_visibility(
        &self,
        statement: StatementToken,
        source: Option<&Arc<crate::session_control::StatementActionSource>>,
    ) -> GovernedStatementVisibilitySealOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().key()) else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        let Some(active) = entry.active.as_mut() else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return GovernedStatementVisibilitySealOutcome::Stale;
        }
        match (&active.action, source) {
            (None, None) => {}
            (Some(active), Some(source)) if Arc::ptr_eq(active, source) => {}
            _ => return GovernedStatementVisibilitySealOutcome::Stale,
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        if active.success_visibility_sealed {
            return GovernedStatementVisibilitySealOutcome::Sealed;
        }
        match cancellation.success_sealer().seal() {
            Ok(WorkSuccessSealOutcome::Sealed | WorkSuccessSealOutcome::AlreadySealed) => {
                active.success_visibility_sealed = true;
                GovernedStatementVisibilitySealOutcome::Sealed
            }
            Ok(WorkSuccessSealOutcome::Cancelled(reason)) => {
                GovernedStatementVisibilitySealOutcome::Cancelled(reason)
            }
            Err(_) => GovernedStatementVisibilitySealOutcome::Stale,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueryControlState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl QueryControlPort for QueryApplicationControl {
    fn mint_client_query_id(&self) -> ClientQueryId {
        self.query_ids.mint()
    }
    fn attach_statement_progress(
        &self,
        slot: &Arc<crate::session_control::StatementProgressSlot>,
        source_slots: std::num::NonZeroUsize,
    ) -> Result<(), QueryControlError> {
        // One serial pre-Ready allocation, paid by the same profile's N slots.
        // Allocate before locking; publication never grows this original array.
        let mut rows = Vec::new();
        rows.try_reserve_exact(source_slots.get())
            .map_err(|_| QueryControlError::StatementActionCapacity)?;
        rows.resize_with(source_slots.get(), || None);
        let rows = rows.into_boxed_slice();
        let mut state = self.lock();
        if state.statement_progress_installed
            || state.sessions.values().any(|entry| {
                entry
                    .active
                    .as_ref()
                    .is_some_and(|active| active.action.is_some())
            })
        {
            return Err(QueryControlError::StatementBusy);
        }
        state.statement_sources = rows;
        state.statement_progress = Arc::downgrade(slot);
        state.statement_actions_open = true;
        state.statement_progress_installed = true;
        Ok(())
    }
    fn statement_progress_slot(
        &self,
    ) -> Option<Arc<crate::session_control::StatementProgressSlot>> {
        self.lock().statement_progress.upgrade()
    }
    fn publish_statement_action_in(
        &self,
        owner: &mut Option<crate::session_control::GovernedQueryStatementOwner>,
        action: &mut Option<Box<dyn crate::session_control::GovernedStatementAction>>,
        source: &Arc<crate::session_control::StatementActionSource>,
    ) -> Result<(), QueryControlError> {
        let token = owner
            .as_ref()
            .ok_or(QueryControlError::StatementBusy)?
            .token();
        if action.is_none() {
            return Err(QueryControlError::StatementBusy);
        }
        let original = owner.as_ref().expect("checked original owner");
        if !original.execution_owner_matches_scope() {
            return Err(QueryControlError::StatementBusy);
        }
        let mut state = self.lock();
        if !state.statement_actions_open {
            return Err(QueryControlError::StatementBusy);
        }
        let row = state
            .statement_sources
            .iter()
            .position(Option::is_none)
            .ok_or(QueryControlError::StatementActionCapacity)?;
        let entry = state
            .sessions
            .get_mut(&token.session().key())
            .ok_or(QueryControlError::UnknownSession)?;
        let active = entry
            .active
            .as_mut()
            .filter(|active| active.generation == token.generation())
            .ok_or(QueryControlError::StatementBusy)?;
        if active.action.is_some() || active.success_visibility_sealed {
            return Err(QueryControlError::StatementBusy);
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return Err(QueryControlError::StatementBusy);
        };
        source.install(owner, action, cancellation);
        active.action = Some(Arc::clone(source));
        state.statement_sources[row] = Some(Arc::clone(source));
        Ok(())
    }
    fn next_statement_action(
        &self,
        after: Option<usize>,
    ) -> Option<(
        usize,
        Option<Arc<crate::session_control::StatementActionSource>>,
    )> {
        let state = self.lock();
        let next = match after {
            Some(previous) => previous.checked_add(1)?,
            None => 0,
        };
        // Empty original rows consume the same finite turn budget as occupied
        // ones. Do not scan an arbitrary N-row suffix under the registry lock
        // inside a single source step.
        state
            .statement_sources
            .get(next)
            .map(|source| (next, source.as_ref().map(Arc::clone)))
    }
    fn retire_statement_action_source(
        &self,
        source: &Arc<crate::session_control::StatementActionSource>,
    ) -> bool {
        let retired = {
            let mut state = self.lock();
            if !source.can_retire() {
                return false;
            }
            let Some(row) = state.statement_sources.iter_mut().find(|row| {
                row.as_ref()
                    .is_some_and(|original| Arc::ptr_eq(original, source))
            }) else {
                return false;
            };
            row.take()
        };
        // The polling caller still owns the same Source; no foreign Drop under State.
        drop(retired);
        true
    }
    fn detach_statement_action(
        &self,
        token: StatementToken,
        source: &Arc<crate::session_control::StatementActionSource>,
    ) -> bool {
        let removed = {
            let mut state = self.lock();
            let Some(entry) = state.sessions.get_mut(&token.session().key()) else {
                return false;
            };
            let Some(active) = entry
                .active
                .as_mut()
                .filter(|active| active.generation == token.generation())
            else {
                return false;
            };
            if !active
                .action
                .as_ref()
                .is_some_and(|original| Arc::ptr_eq(original, source))
            {
                return false;
            }
            active.action.take()
        };
        drop(removed);
        true
    }
    fn settle_statement_action_silent(
        &self,
        token: StatementToken,
        source: &Arc<crate::session_control::StatementActionSource>,
    ) -> bool {
        let retired = {
            let mut state = self.lock();
            let Some(entry) = state.sessions.get_mut(&token.session().key()) else {
                return false;
            };
            let Some(active) = entry
                .active
                .as_ref()
                .filter(|active| active.generation == token.generation())
            else {
                return false;
            };
            if !active
                .action
                .as_ref()
                .is_some_and(|original| Arc::ptr_eq(original, source))
                || !source.is_settled()
            {
                return false;
            }
            let unregister_pending = entry.unregister_pending;
            let retired = entry.active.take();
            if unregister_pending {
                state.remove_session(token.session().key());
            }
            retired
        };
        // Source/cancellation remain held by the original caller. The metadata
        // record's actual Drop is also outside the QueryControl lock.
        drop(retired);
        true
    }
    fn close_statement_actions(&self) {
        let mut state = self.lock();
        state.statement_actions_open = false;
        for source in state.statement_sources.iter().flatten() {
            source.stop();
        }
    }
    fn statement_action_failure(
        &self,
    ) -> Option<Arc<crate::session_control::StatementActionFailure>> {
        self.lock()
            .statement_sources
            .iter()
            .flatten()
            .find_map(|source| source.failure())
    }
    fn has_statement_actions(&self) -> bool {
        self.lock().statement_sources.iter().any(Option::is_some)
    }

    fn register_session(
        &self,
        identity: SessionIdentity,
    ) -> Result<SessionToken, QueryControlError> {
        let mut state = self.lock();
        let SessionOrigin::ClientStatement(query) = identity.origin();
        if state.by_client_query.contains_key(query) {
            return Err(QueryControlError::ClientQueryIdInUse);
        }
        let client_query = query.clone();
        state.next_session_key = state
            .next_session_key
            .checked_add(1)
            .ok_or(QueryControlError::SessionIdentityExhausted)?;
        let key =
            ClientSessionKey::new(state.next_session_key).expect("incremented nonzero session key");
        let token = SessionToken::new(key);
        state.by_client_query.insert(client_query.clone(), key);
        state.sessions.insert(
            key,
            SessionEntry {
                origin: identity.origin().clone(),
                next_statement_generation: 0,
                active: None,
                unregister_pending: false,
            },
        );
        // Never call the consumer while holding the registry lock: the observer
        // has its own journal and input freezing must precede statement admission.
        drop(state);
        #[cfg(novarocks_dev_controls)]
        if let Some(observer) = self.client_statement_observer.get() {
            observer(&client_query, token);
        }
        Ok(token)
    }

    fn unregister_session(&self, token: SessionToken) {
        let mut state = self.lock();
        if state.sessions.contains_key(&token.key()) {
            let entry = state.sessions.get_mut(&token.key()).unwrap();
            if let Some(active) = entry.active.as_ref() {
                entry.unregister_pending = true;
                if !active.success_visibility_sealed
                    && let QueryCancelOutcome::Failed(error) = active
                        .cancellation
                        .request(QueryCancellationReason::ClientDisconnected)
                {
                    tracing::error!(
                        session_key = token.key().get(),
                        error = %error,
                        "retain query-control session after workload cancellation failed"
                    );
                }
                return;
            }
            state.remove_session(token.key());
        }
    }

    fn begin_statement(
        &self,
        session: SessionToken,
    ) -> Result<StatementRegistration, QueryControlError> {
        self.begin_statement_with_cancellation(session, QueryCancellationSource::new())
    }

    fn begin_statement_with_cancellation(
        &self,
        session: SessionToken,
        cancellation: QueryCancellationSource,
    ) -> Result<StatementRegistration, QueryControlError> {
        let mut state = self.lock();
        let entry = state
            .sessions
            .get_mut(&session.key())
            .ok_or(QueryControlError::UnknownSession)?;
        if entry.unregister_pending {
            return Err(QueryControlError::UnknownSession);
        }
        if entry.active.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        entry.next_statement_generation = entry.next_statement_generation.wrapping_add(1);
        if entry.next_statement_generation == 0 {
            entry.next_statement_generation = 1;
        }
        let registration = StatementRegistration::new(
            StatementToken::new(session, entry.next_statement_generation),
            cancellation.view(),
        );
        entry.active = Some(ActiveStatement {
            action: None,
            generation: entry.next_statement_generation,
            cancellation: ActiveStatementCancellation::Legacy(cancellation),
            success_visibility_sealed: false,
        });
        Ok(registration)
    }

    fn begin_statement_with_governed_cancellation(
        &self,
        session: SessionToken,
        cancellation: GovernedStatementCancellation,
        _statement_text: Option<Arc<str>>,
    ) -> Result<GovernedStatementRegistration, QueryControlError> {
        let mut state = self.lock();
        let entry = state
            .sessions
            .get_mut(&session.key())
            .ok_or(QueryControlError::UnknownSession)?;
        if entry.unregister_pending {
            return Err(QueryControlError::UnknownSession);
        }
        if entry.active.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        entry.next_statement_generation = entry.next_statement_generation.wrapping_add(1);
        if entry.next_statement_generation == 0 {
            entry.next_statement_generation = 1;
        }
        let registration = GovernedStatementRegistration::new(
            StatementToken::new(session, entry.next_statement_generation),
            cancellation.view().clone(),
        );
        entry.active = Some(ActiveStatement {
            action: None,
            generation: entry.next_statement_generation,
            cancellation: ActiveStatementCancellation::Governed(cancellation),
            success_visibility_sealed: false,
        });
        Ok(registration)
    }

    fn finish_statement(&self, statement: StatementToken) -> StatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().key()) else {
            return StatementFinishOutcome::Stale;
        };
        let Some(active) = entry.active.as_ref() else {
            return StatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return StatementFinishOutcome::Stale;
        }
        let ActiveStatementCancellation::Legacy(cancellation) = &active.cancellation else {
            return StatementFinishOutcome::Stale;
        };
        let reason = if active.success_visibility_sealed {
            None
        } else {
            cancellation.view().reason()
        };
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        let outcome = match reason {
            Some(reason) => StatementFinishOutcome::Cancelled(reason),
            None => StatementFinishOutcome::Completed,
        };
        if unregister_pending {
            state.remove_session(statement.session().key());
        }
        outcome
    }

    fn seal_governed_statement_visibility(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementVisibilitySealOutcome {
        self.seal_statement_visibility(statement, None)
    }

    fn seal_governed_statement_action_visibility(
        &self,
        request: &crate::session_control::StatementActionVisibilityRequest<'_>,
    ) -> GovernedStatementVisibilitySealOutcome {
        self.seal_statement_visibility(request.statement(), Some(request.source()))
    }

    fn finish_governed_statement(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().key()) else {
            return GovernedStatementFinishOutcome::Stale;
        };
        let Some(active) = entry.active.as_ref() else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return GovernedStatementFinishOutcome::Stale;
        }
        if active.action.is_some() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return GovernedStatementFinishOutcome::Stale;
        };
        let reason = if active.success_visibility_sealed {
            None
        } else {
            cancellation.view().reason()
        };
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        let outcome = match reason {
            Some(reason) => GovernedStatementFinishOutcome::Cancelled(reason),
            None => GovernedStatementFinishOutcome::Completed,
        };
        if unregister_pending {
            state.remove_session(statement.session().key());
        }
        outcome
    }

    fn fail_governed_statement(&self, statement: StatementToken) -> GovernedStatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().key()) else {
            return GovernedStatementFinishOutcome::Stale;
        };
        let Some(active) = entry.active.as_ref() else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation()
            || !matches!(
                active.cancellation,
                ActiveStatementCancellation::Governed(_)
            )
        {
            return GovernedStatementFinishOutcome::Stale;
        }
        if active.action.is_some() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        if unregister_pending {
            state.remove_session(statement.session().key());
        }
        GovernedStatementFinishOutcome::ProtocolFailed
    }

    fn cancel_governed_statement(
        &self,
        statement: StatementToken,
        reason: CancellationReason,
    ) -> Result<Option<WorkCancellationRequestOutcome>, WorkError> {
        let state = self.lock();
        let Some(entry) = state.sessions.get(&statement.session().key()) else {
            return Ok(None);
        };
        let Some(active) = entry.active.as_ref() else {
            return Ok(None);
        };
        if active.generation != statement.generation() {
            return Ok(None);
        }
        if active.success_visibility_sealed {
            return Ok(None);
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return Ok(None);
        };
        cancellation
            .requester()
            .request_with_outcome(reason)
            .map(Some)
    }

    fn cancel_session_statement(
        &self,
        session: SessionToken,
        reason: QueryCancellationReason,
    ) -> QueryCancelOutcome {
        let state = self.lock();
        let Some(entry) = state.sessions.get(&session.key()) else {
            return QueryCancelOutcome::UnknownSession;
        };
        let Some(active) = entry.active.as_ref() else {
            return QueryCancelOutcome::NoActiveStatement;
        };
        if active.success_visibility_sealed {
            return QueryCancelOutcome::NoActiveStatement;
        }
        active.cancellation.request(reason)
    }

    fn cancel_all(&self, reason: QueryCancellationReason) {
        let state = self.lock();
        for entry in state.sessions.values() {
            if let Some(active) = entry.active.as_ref() {
                if active.success_visibility_sealed {
                    continue;
                }
                if let QueryCancelOutcome::Failed(error) =
                    active.cancellation.request(reason.clone())
                {
                    tracing::error!(
                        error = %error,
                        "failed to request governed statement cancellation during cancel-all"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_control::{GovernedQueryStatementBeginError, QueryControlPort};
    use novarocks_workload_control::{ResourceConfig, WorkloadConfig, WorkloadControl};
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    fn register(control: &QueryApplicationControl, principal: &str) -> SessionToken {
        control
            .register_session(SessionIdentity::client_statement(
                control.mint_client_query_id(),
                principal,
            ))
            .expect("register original statement session")
    }

    #[test]
    fn statement_sessions_have_independent_nonreused_keys_and_no_connection_position() {
        let control = QueryApplicationControl::default();
        let peer = register(&control, "root");
        let query_id = control.mint_client_query_id();
        let client = control
            .register_session(SessionIdentity::client_statement(query_id, "root"))
            .unwrap();
        let statement = control.begin_statement(client).unwrap();
        assert_ne!(client.key(), peer.key());
        assert_eq!(control.lock().by_client_query.len(), 2);
        control.unregister_session(client);
        assert_eq!(
            control.finish_statement(statement.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ClientDisconnected)
        );
        let successor = control
            .register_session(SessionIdentity::client_statement(
                control.mint_client_query_id(),
                "root",
            ))
            .unwrap();
        assert!(successor.key() > client.key());
        control.cancel_all(QueryCancellationReason::ServerShutdown);
        control.unregister_session(successor);
        control.unregister_session(peer);
        assert!(control.lock().sessions.is_empty());
    }

    #[test]
    fn client_query_session_lookup_tracks_exact_registration_and_removal() {
        let control = QueryApplicationControl::default();
        let query = control.mint_client_query_id();
        assert!(
            control
                .current_session_for_client_query(query.as_str())
                .is_none()
        );
        let token = control
            .register_session(SessionIdentity::client_statement(query.clone(), "root"))
            .unwrap();
        assert_eq!(
            control.current_session_for_client_query(query.as_str()),
            Some(token)
        );
        assert_eq!(
            control.register_session(SessionIdentity::client_statement(query.clone(), "root")),
            Err(QueryControlError::ClientQueryIdInUse)
        );
        let statement = control.begin_statement(token).unwrap();
        control.unregister_session(token);
        assert!(
            control
                .current_session_for_client_query(query.as_str())
                .is_none()
        );
        control.finish_statement(statement.token());
        assert!(control.lock().by_client_query.is_empty());
        assert!(control.lock().sessions.is_empty());
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn client_query_registration_notifies_before_first_statement_without_registry_lock() {
        let control = Arc::new(QueryApplicationControl::default());
        let original = Arc::downgrade(&control);
        let observed = Arc::new(Mutex::new(None));
        let slot = observed.clone();
        control
            .install_client_statement_observer(Arc::new(move |query, session| {
                let control = original.upgrade().unwrap();
                assert_eq!(
                    control.current_session_for_client_query(query.as_str()),
                    Some(session)
                );
                assert!(control.lock().sessions[&session.key()].active.is_none());
                *slot.lock().unwrap() = Some((query.clone(), session));
            }))
            .unwrap();
        let query = control.mint_client_query_id();
        let token = control
            .register_session(SessionIdentity::client_statement(query.clone(), "root"))
            .unwrap();
        assert_eq!(*observed.lock().unwrap(), Some((query, token)));
        control.unregister_session(token);
    }

    #[test]
    fn session_key_exhaustion_refuses_without_reusing_a_retired_key() {
        let control = QueryApplicationControl::default();
        control.lock().next_session_key = u64::MAX;
        assert_eq!(
            control.register_session(SessionIdentity::client_statement(
                control.mint_client_query_id(),
                "root"
            )),
            Err(QueryControlError::SessionIdentityExhausted)
        );
        assert!(control.lock().sessions.is_empty());
    }

    #[test]
    fn stale_session_and_statement_cannot_remove_successor() {
        let control = QueryApplicationControl::default();
        let query = control.mint_client_query_id();
        let first = control
            .register_session(SessionIdentity::client_statement(query.clone(), "root"))
            .unwrap();
        let old = control.begin_statement(first).expect("begin old");
        control.unregister_session(first);
        assert_eq!(
            control.register_session(SessionIdentity::client_statement(query, "root")),
            Err(QueryControlError::ClientQueryIdInUse)
        );
        assert_eq!(
            control.finish_statement(old.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ClientDisconnected)
        );
        let second = register(&control, "root");
        let current = control.begin_statement(second).expect("begin current");
        assert_eq!(
            control.finish_statement(old.token()),
            StatementFinishOutcome::Stale
        );
        assert_eq!(
            control.cancel_session_statement(second, QueryCancellationReason::ClientDisconnected),
            QueryCancelOutcome::Requested
        );
        assert!(current.cancellation().is_cancelled());
    }

    #[test]
    fn repeated_client_cancel_preserves_first_reason() {
        let control = QueryApplicationControl::default();
        let target = register(&control, "root");
        let active = control.begin_statement(target).expect("begin target");
        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ClientCancelRequested),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ClientCancelRequested),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::ClientCancelRequested)
        );
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ClientCancelRequested)
        );
    }

    #[test]
    fn finish_and_cancel_are_linearized_and_busy_session_rejects_successor() {
        let control = QueryApplicationControl::default();
        let target = register(&control, "root");
        let active = control.begin_statement(target).expect("begin target");

        assert!(matches!(
            control.begin_statement(target),
            Err(QueryControlError::StatementBusy)
        ));
        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ClientCancelRequested),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ClientCancelRequested)
        );
        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ClientCancelRequested),
            QueryCancelOutcome::NoActiveStatement
        );
        assert!(
            control.begin_statement(target).is_ok(),
            "only the matching active generation is released"
        );
    }

    #[test]
    fn caller_owned_cancellation_source_controls_the_registered_statement() {
        let control = QueryApplicationControl::default();
        let session = register(&control, "root");
        let source = QueryCancellationSource::new();
        let active = control
            .begin_statement_with_cancellation(session, source.clone())
            .expect("begin statement with lifecycle source");

        assert_eq!(
            source.request(QueryCancellationReason::FrontendDrainDeadlineExceeded {
                timeout_ms: 300_000,
            }),
            crate::cancellation::QueryCancellationRequestResult::Requested
        );
        assert!(active.cancellation().is_cancelled());
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(
                QueryCancellationReason::FrontendDrainDeadlineExceeded {
                    timeout_ms: 300_000,
                }
            )
        );
    }

    fn governed_control() -> (
        Arc<QueryApplicationControl>,
        QueryControlService,
        WorkloadControl,
    ) {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload authority");
        workload.mark_ready().expect("workload authority ready");
        (control, service, workload)
    }

    #[test]
    fn nonqueued_result_admission_rolls_back_registration_failure_and_retains_aliases() {
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass, WorkClass};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1, 2, 1, 1],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        workload.mark_ready().unwrap();
        let session = register(&control, "root");
        let other = register(&control, "root");
        let begin = |session| {
            service.begin_governed_statement_with_result(
                session,
                &workload.root_admission(),
                WorkClass::Management,
                None,
                None,
                None,
                ResultWindowClass::Local,
            )
        };
        let statement = begin(session).unwrap();
        assert!(matches!(
            begin(session),
            Err(GovernedQueryStatementBeginError::QueryControl(_))
        ));
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert_eq!(workload.snapshot().businesses, 1);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let second = begin(other).unwrap();
        let third = register(&control, "root");
        assert!(matches!(
            begin(third),
            Err(GovernedQueryStatementBeginError::Admission(_))
        ));
        assert_eq!(workload.snapshot().root_responsibilities, 2);
        assert_eq!(workload.snapshot().businesses, 2);
        assert_eq!(workload.snapshot().admitted_queries, 0);
        let alias = statement.result_window_alias().unwrap();
        drop(statement);
        drop(second);
        assert_eq!(workload.snapshot().businesses, 0);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        drop(alias);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        // The failed capacity admission did not start a protocol generation.
        drop(begin(third).unwrap());
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    // A statement's retained data is not an executing computation. Keep the
    // real Internal position occupied by an independent producer to prove both
    // statement admission paths do not wait for or consume that position.
    #[tokio::test]
    async fn internal_statement_custody_does_not_take_another_compute_position() {
        use futures::FutureExt;
        use novarocks_workload_control::{
            ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
        };
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new_counted(WorkloadConfig {
            query_concurrency_limit: 1,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1; 4],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                all_objects_bytes: [64; 4],
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        workload.mark_ready().unwrap();
        let (busy, physical) = workload
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let session = register(&control, "root");
        let queued = service
            .begin_queued_governed_query_statement_with_result(
                session,
                &workload.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Internal,
            )
            .now_or_never()
            .expect("Root retention must not wait for a compute position")
            .unwrap();
        assert_eq!(workload.snapshot().admitted_queries, 1);
        let other = register(&control, "root");
        let nonqueued = service
            .begin_governed_statement_with_result(
                other,
                &workload.root_admission(),
                WorkClass::Management,
                None,
                None,
                None,
                ResultWindowClass::Internal,
            )
            .unwrap();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        let data = queued.result_window_alias().unwrap();
        assert!(data.is_root_backed_internal());
        assert!(data.internal_manifest_binding().unwrap().is_none());
        assert!(data.native_action_authority().is_err());
        let receipt = data.reserve_payload_bytes(40).unwrap();
        let backing = vec![0u8; 40];
        assert!(data.reserve_payload_bytes(25).is_err());
        drop(queued);
        drop(nonqueued);
        assert_eq!(workload.snapshot().admitted_queries, 0);
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(physical);
        busy.business.release();
        busy.owner.complete();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        drop(data);
        drop(backing);
        drop(receipt);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[tokio::test]
    async fn result_statement_takes_its_window_with_the_permit_and_keeps_it_for_aliases() {
        use novarocks_workload_control::{ResultCapacityConfig, ResultWindowClass};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new(
            WorkloadConfig {
                query_concurrency_limit: 1,
                ..WorkloadConfig::default()
            },
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1; 4],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        workload.mark_ready().unwrap();
        let session = register(&control, "root");
        let mut statement = service
            .begin_queued_governed_query_statement_with_result(
                session,
                &workload.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Client,
            )
            .await
            .unwrap();
        assert_eq!(workload.snapshot().admitted_queries, 1);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        let alias = statement
            .result_window_alias()
            .expect("a result statement owns its window");
        assert_eq!(alias.class(), ResultWindowClass::Client);
        // An accepted cancel cut returns only the computation permit.
        assert_eq!(
            control
                .cancel_session_statement(session, QueryCancellationReason::ClientCancelRequested),
            QueryCancelOutcome::Requested
        );
        assert!(statement.accept_cancel_delivery_cut().unwrap());
        assert_eq!(workload.snapshot().admitted_queries, 0);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        // The owner's exit does not free the position while an alias lives.
        drop(statement);
        assert_eq!(capacity.snapshot().held_positions, [1, 0, 0, 0]);
        drop(alias);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        // A plain statement takes no window.
        let plain = service
            .begin_queued_governed_query_statement(
                session,
                &workload.root_admission(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(plain.result_window_alias().is_none());
    }

    #[test]
    fn failed_delivery_cut_retains_business_and_does_not_fabricate_cancel() {
        for previously_sealed in [false, true] {
            for finish_path in 0..3 {
                let (control, service, workload) = governed_control();
                let session = register(&control, "root");
                let mut statement = service
                    .begin_governed_query_statement(
                        session,
                        &workload.root_admission(),
                        None,
                        None,
                        None,
                    )
                    .unwrap();
                if previously_sealed {
                    assert_eq!(
                        statement.seal_success_visibility(),
                        GovernedStatementVisibilitySealOutcome::Sealed
                    );
                }
                assert_eq!(workload.snapshot().businesses, 1);
                // This entry owns a business permit rather than a compute
                // permit. The failure cut retains that business owner.
                assert!(!statement.accept_failed_delivery_cut().unwrap());
                assert_eq!(workload.snapshot().businesses, 1);
                assert!(statement.cancellation().reason().is_none());
                assert_eq!(
                    statement.seal_success_visibility(),
                    GovernedStatementVisibilitySealOutcome::Failed
                );
                assert!(matches!(
                    service.begin_statement(session),
                    Err(QueryControlError::StatementBusy)
                ));
                let outcome = match finish_path {
                    0 => statement.finish(),
                    1 => statement.finish_unstarted_read_after_cancellation(),
                    _ => statement.protocol_fail(),
                };
                assert_eq!(outcome, GovernedStatementFinishOutcome::ProtocolFailed);
                assert_eq!(workload.snapshot().businesses, 0);
            }
        }
    }

    #[tokio::test]
    async fn queued_governed_query_registers_cancellation_before_its_concurrency_grant() {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let config = WorkloadConfig {
            query_concurrency_limit: 1,
            ..WorkloadConfig::default()
        };
        let workload = WorkloadControl::try_new(
            config,
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload authority");
        workload.mark_ready().expect("workload authority ready");
        let first_session = register(&control, "root");
        let second_session = register(&control, "root");

        let mut first = service
            .begin_queued_governed_query_statement(
                first_session,
                &workload.root_admission(),
                None,
                None,
                None,
            )
            .await
            .expect("first query grant");
        let query_admission = workload.root_admission();
        let mut second = Box::pin(service.begin_queued_governed_query_statement(
            second_session,
            &query_admission,
            None,
            None,
            None,
        ));
        assert!(matches!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        assert_eq!(workload.snapshot().admitted_queries, 1);
        assert_eq!(workload.snapshot().admission_records, 1);

        assert_eq!(
            control.cancel_session_statement(
                second_session,
                QueryCancellationReason::ClientCancelRequested,
            ),
            QueryCancelOutcome::Requested
        );
        assert!(matches!(
            second.await,
            Err(GovernedQueryStatementBeginError::Admission(
                WorkError::Cancelled(CancellationReason::ClientCancelRequested)
            ))
        ));
        assert_eq!(workload.snapshot().admitted_queries, 1);
        assert_eq!(workload.snapshot().admission_records, 0);

        first
            .take_execution_owner()
            .expect("first execution owner")
            .complete();
        assert_eq!(first.finish(), GovernedStatementFinishOutcome::Completed);
        assert_eq!(workload.snapshot().admitted_queries, 0);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn governed_client_cancel_requests_the_registered_work_scope() {
        let (control, service, workload) = governed_control();
        let target = register(&control, "root");
        let mut statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        let owner = statement
            .take_execution_owner()
            .expect("root owner transfers once");

        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ClientCancelRequested),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::ClientCancelRequested)
        );
        owner.complete();
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::ClientCancelRequested)
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_late_client_cancel_reports_the_existing_owner_drop_reason() {
        let (control, service, workload) = governed_control();
        let target = register(&control, "root");
        let statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None, None)
            .expect("governed query statement");

        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ExecutionOwnerDropped,),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ClientCancelRequested),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::ExecutionOwnerDropped,)
        );
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::OwnerDropped)
        );
    }

    #[test]
    fn governed_disconnect_and_timeout_use_the_same_workload_authority() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        let owner = statement
            .take_execution_owner()
            .expect("root owner transfers once");

        assert_eq!(
            control.cancel_session_statement(
                session,
                QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
            ),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.cancel_session_statement(session, QueryCancellationReason::ClientDisconnected,),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::DeadlineExceeded {
                timeout_ms: 50,
            })
        );
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::DeadlineExceeded)
        );
        owner.complete();
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::DeadlineExceeded)
        );

        let session_lease = service
            .register_session(SessionIdentity::client_statement(
                service.mint_client_query_id(),
                "root",
            ))
            .expect("register second session");
        let session = session_lease.token();
        let statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("second governed query statement");
        drop(session_lease);
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::ClientDisconnected)
        );
    }

    #[test]
    fn cancelled_unstarted_read_settles_its_known_control() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");

        assert_eq!(
            control.cancel_session_statement(
                session,
                QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
            ),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            statement.finish_unstarted_read_after_cancellation(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::DeadlineExceeded)
        );
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert!(
            workload.next_control().is_none(),
            "an unstarted read has no logical-execution owner left to acknowledge cancellation"
        );
    }

    #[test]
    fn cancelled_local_query_terminals_do_not_leave_ready_control() {
        for terminal in 0..3 {
            let (control, service, workload) = governed_control();
            let session = register(&control, "root");
            let statement = service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .expect("governed query statement");
            assert_eq!(
                control.cancel_session_statement(
                    session,
                    QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
                ),
                QueryCancelOutcome::Requested
            );

            match terminal {
                0 => {
                    assert_eq!(
                        statement.finish(),
                        GovernedStatementFinishOutcome::Cancelled(
                            CancellationReason::DeadlineExceeded
                        )
                    );
                }
                1 => {
                    assert_eq!(
                        statement.protocol_fail(),
                        GovernedStatementFinishOutcome::ProtocolFailed
                    );
                }
                _ => drop(statement),
            }
            assert_eq!(workload.snapshot().root_responsibilities, 0);
            assert!(workload.next_control().is_none());
        }
    }

    #[test]
    fn cancelled_synchronous_execution_settles_control_before_protocol_finish() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        assert_eq!(
            control.cancel_session_statement(
                session,
                QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
            ),
            QueryCancelOutcome::Requested
        );

        statement.complete_execution();
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert!(workload.next_control().is_none());
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::DeadlineExceeded)
        );
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn governed_generation_and_business_permit_span_execution_start_to_protocol_finish() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let mut first = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("first governed query statement");
        let first_generation = first.token().generation();
        let owner = first
            .take_execution_owner()
            .expect("execution start consumes the root owner");
        owner.complete();

        assert_eq!(workload.snapshot().businesses, 1);
        assert!(matches!(
            service.begin_governed_query_statement(
                session,
                &workload.root_admission(),
                None,
                None,
                None
            ),
            Err(GovernedQueryStatementBeginError::QueryControl(
                QueryControlError::StatementBusy
            ))
        ));
        assert_eq!(
            workload.snapshot().businesses,
            1,
            "rejected successor admission rolls back its root and business permit"
        );

        assert_eq!(first.finish(), GovernedStatementFinishOutcome::Completed);
        assert_eq!(workload.snapshot().businesses, 0);
        let second = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("protocol completion releases the next generation");
        assert!(second.token().generation() > first_generation);
    }

    #[test]
    fn governed_success_seal_wins_against_late_client_cancel_until_protocol_finish() {
        let (control, service, workload) = governed_control();
        let target = register(&control, "root");
        let mut statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(workload.snapshot().businesses, 1);
        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ClientCancelRequested),
            QueryCancelOutcome::NoActiveStatement
        );
        assert_eq!(statement.cancellation().reason(), None);
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_eof_failure_after_success_seal_is_protocol_failed() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_internal_protocol_failure_does_not_fabricate_cancellation() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        let cancellation = statement.cancellation().clone();
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(cancellation.reason(), None);
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn unregister_is_deferred_until_the_active_generation_settles() {
        let (control, service, workload) = governed_control();
        let query = control.mint_client_query_id();
        let session = service
            .register_session(SessionIdentity::client_statement(query.clone(), "root"))
            .expect("register session");
        let token = session.token();
        let statement = service
            .begin_governed_query_statement(token, &workload.root_admission(), None, None, None)
            .expect("begin governed statement");

        drop(session);
        assert!(
            matches!(
                service.register_session(SessionIdentity::client_statement(query.clone(), "root")),
                Err(QueryControlError::ClientQueryIdInUse)
            ),
            "the registry retains the deferred unregister owner"
        );
        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        let successor = service
            .register_session(SessionIdentity::client_statement(
                control.mint_client_query_id(),
                "root",
            ))
            .expect("settlement completes deferred unregister");
        assert_ne!(successor.token().key(), token.key());
        drop(successor);
        assert!(!control.lock().sessions.contains_key(&token.key()));
    }

    #[tokio::test]
    async fn governed_root_deadline_is_observed_without_a_legacy_cancellation_source() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(10);
        let statement = service
            .begin_governed_query_statement(
                session,
                &workload.root_admission(),
                Some(deadline),
                Some(1),
                None,
            )
            .expect("deadline-bound governed query statement");

        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                statement.cancellation().cancelled(),
            )
            .await
            .expect("deadline cancellation arrives"),
            CancellationReason::DeadlineExceeded
        );
    }

    #[test]
    fn dropping_protocol_owner_after_execution_handoff_cancels_the_same_root() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .expect("governed query statement");
        let scope = statement.scope().clone();
        let owner = statement
            .take_execution_owner()
            .expect("execution owner transfers once");

        drop(statement);

        assert_eq!(
            scope.cancellation().expect("root remains owned").reason(),
            Some(CancellationReason::OwnerDropped)
        );
        assert_eq!(workload.snapshot().businesses, 0);
        owner.complete();
    }
    struct OriginalStatementGate {
        gate: futures::channel::oneshot::Receiver<()>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl crate::session_control::GovernedStatementAction for OriginalStatementGate {
        fn poll_action(
            &mut self,
            _owner: &mut crate::session_control::GovernedActionContext<'_>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<
            Result<
                crate::session_control::StatementActionValue,
                crate::session_error::QueryServiceError,
            >,
        > {
            use std::future::Future;
            std::pin::Pin::new(&mut self.gate).poll(cx).map(|ready| {
                ready.expect("original release sender");
                Ok(crate::session_control::StatementActionValue::Ok(
                    crate::session_control::StatementCommandCompletion::empty(),
                ))
            })
        }
    }
    impl Drop for OriginalStatementGate {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    fn drive_original_statement_actions(slot: &crate::session_control::StatementProgressSlot) {
        let mut cursor = crate::session_control::StatementProgressCursor::default();
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        // Drive the real finite scanner with its persistent cursor across
        // turns. The test waker is inert; only real source notifications mark
        // another sweep. Empty rows still consume the production turn budget.
        for _ in 0..10000 {
            if slot
                .poll_turn(
                    &mut cursor,
                    std::num::NonZeroUsize::new(7).unwrap(),
                    &mut cx,
                )
                .is_pending()
            {
                return;
            }
        }
        panic!("original statement scanner did not reach idle");
    }
    #[test]
    fn ordinary_action_views_cannot_seal_statement_visibility() {
        use crate::session_control::GovernedActionContext;
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None, None)
            .unwrap();
        let scope = statement.scope().clone();
        let cancellation = statement.cancellation().clone();
        let window = statement.result_window_alias();
        assert_eq!(
            statement
                .action_context(window.as_ref())
                .seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Stale
        );
        let mut no_owner = None;
        assert_eq!(
            GovernedActionContext::new(&scope, &mut no_owner, &cancellation, None, None)
                .seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Stale,
            "a real same-Root scope is not a registered SQL visibility loan"
        );
        assert!(cancellation.reason().is_none());
        assert_eq!(
            statement.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed,
            "the original unregistered protocol owner retains its existing seal"
        );
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[tokio::test]
    async fn registered_action_visibility_loan_checks_source_and_preserves_cancel_order() {
        use crate::session_control::{
            GovernedActionContext, GovernedStatementAction, StatementActionValue,
        };
        use crate::session_error::{QueryServiceError, QueryServiceErrorKind};
        use std::future::Future;
        use std::task::{Context, Poll};
        struct SealAction {
            gate: futures::channel::oneshot::Receiver<()>,
            outcomes: Arc<Mutex<Vec<GovernedStatementVisibilitySealOutcome>>>,
        }
        impl GovernedStatementAction for SealAction {
            fn poll_action(
                &mut self,
                owner: &mut GovernedActionContext<'_>,
                cx: &mut Context<'_>,
            ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
                match std::pin::Pin::new(&mut self.gate).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(ready) => ready.expect("original seal gate"),
                }
                let outcome = owner.seal_success_visibility();
                assert_eq!(owner.seal_success_visibility(), outcome);
                self.outcomes.lock().unwrap().push(outcome.clone());
                Poll::Ready(match outcome {
                    GovernedStatementVisibilitySealOutcome::Sealed => Ok(StatementActionValue::Ok(
                        crate::session_control::StatementCommandCompletion::empty(),
                    )),
                    GovernedStatementVisibilitySealOutcome::Cancelled(_) => {
                        Err(QueryServiceError::new(
                            QueryServiceErrorKind::Interrupted,
                            "test action stopped before visibility",
                        ))
                    }
                    _ => panic!("same registered action has its original visibility loan"),
                })
            }
        }
        for cancel_first in [false, true] {
            let (control, service, workload) = governed_control();
            let session = register(&control, "root");
            let foreign_session = register(&control, "root");
            let slot = crate::session_control::StatementProgressSlot::empty();
            slot.install(
                service.clone(),
                std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
            )
            .unwrap();
            let mut owner = Some(
                service
                    .begin_governed_query_statement(
                        session,
                        &workload.root_admission(),
                        None,
                        None,
                        None,
                    )
                    .unwrap(),
            );
            let token = owner.as_ref().unwrap().token();
            let scope = owner.as_ref().unwrap().scope().clone();
            let cancellation = owner.as_ref().unwrap().cancellation().clone();
            let (release, gate) = futures::channel::oneshot::channel();
            let outcomes = Arc::new(Mutex::new(Vec::new()));
            let mut action: Option<Box<dyn GovernedStatementAction>> = Some(Box::new(SealAction {
                gate,
                outcomes: Arc::clone(&outcomes),
            }));
            let mut receiver = None;
            service
                .publish_statement_action_in(
                    &mut owner,
                    &mut action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Empty,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut receiver,
                )
                .unwrap()
                .finish()
                .unwrap();
            drive_original_statement_actions(&slot);
            assert!(outcomes.lock().unwrap().is_empty());
            let source = control.next_statement_action(None).unwrap().1.unwrap();
            assert!(Arc::ptr_eq(
                &source,
                control.lock().sessions[&session.key()]
                    .active
                    .as_ref()
                    .unwrap()
                    .action
                    .as_ref()
                    .unwrap(),
            ));
            assert_eq!(
                control.seal_governed_statement_visibility(token),
                GovernedStatementVisibilitySealOutcome::Stale,
                "the old protocol ingress cannot seal a registered action"
            );
            let mut foreign_owner = Some(
                service
                    .begin_governed_query_statement(
                        foreign_session,
                        &workload.root_admission(),
                        None,
                        None,
                        None,
                    )
                    .unwrap(),
            );
            let foreign_token = foreign_owner.as_ref().unwrap().token();
            assert_eq!(
                control.seal_statement_visibility(foreign_token, Some(&source)),
                GovernedStatementVisibilitySealOutcome::Stale,
                "an unbound statement cannot borrow another Source's qualification"
            );
            let (foreign_release, foreign_gate) = futures::channel::oneshot::channel();
            let foreign_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut foreign_action: Option<Box<dyn GovernedStatementAction>> =
                Some(Box::new(OriginalStatementGate {
                    gate: foreign_gate,
                    drops: Arc::clone(&foreign_drops),
                }));
            let mut foreign_receiver = None;
            service
                .publish_statement_action_in(
                    &mut foreign_owner,
                    &mut foreign_action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Empty,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut foreign_receiver,
                )
                .unwrap()
                .finish()
                .unwrap();
            let foreign_source = control.lock().sessions[&foreign_session.key()]
                .active
                .as_ref()
                .unwrap()
                .action
                .as_ref()
                .unwrap()
                .clone();
            assert_eq!(
                control.seal_statement_visibility(token, Some(&foreign_source)),
                GovernedStatementVisibilitySealOutcome::Stale,
                "only the registry's exact original Source can seal"
            );
            for stale in [
                StatementToken::new(
                    SessionToken::new(ClientSessionKey::new(session.key().get() + 1).unwrap()),
                    token.generation(),
                ),
                StatementToken::new(session, token.generation() + 1),
            ] {
                assert_eq!(
                    control.seal_statement_visibility(stale, Some(&source)),
                    GovernedStatementVisibilitySealOutcome::Stale
                );
            }
            assert!(
                !control.lock().sessions[&session.key()]
                    .active
                    .as_ref()
                    .unwrap()
                    .success_visibility_sealed
            );
            assert!(cancellation.reason().is_none());
            drive_original_statement_actions(&slot);
            assert!(outcomes.lock().unwrap().is_empty());
            assert_eq!(workload.snapshot().root_responsibilities, 2);
            if cancel_first {
                assert_eq!(
                    control
                        .cancel_governed_statement(token, CancellationReason::Requested)
                        .unwrap(),
                    Some(WorkCancellationRequestOutcome::Requested)
                );
            }
            release.send(()).unwrap();
            drive_original_statement_actions(&slot);
            let expected = if cancel_first {
                GovernedStatementVisibilitySealOutcome::Cancelled(CancellationReason::Requested)
            } else {
                GovernedStatementVisibilitySealOutcome::Sealed
            };
            assert_eq!(*outcomes.lock().unwrap(), vec![expected.clone()]);
            if !cancel_first {
                assert_eq!(
                    control.cancel_session_statement(
                        session,
                        QueryCancellationReason::ClientCancelRequested
                    ),
                    QueryCancelOutcome::NoActiveStatement
                );
                assert!(cancellation.reason().is_none());
            }
            let receipt = receiver.take().unwrap().await.unwrap();
            let mut ready = None;
            receipt
                .take_ready_in(&mut ready)
                .unwrap_or_else(|_| panic!("same original action receiver"))
                .finish()
                .unwrap();
            let mut ready = ready.take().unwrap();
            assert_eq!(ready.owner.token(), token);
            assert_eq!(ready.owner.scope().id(), scope.id());
            assert_eq!(ready.owner.seal_success_visibility(), expected);
            assert_eq!(ready.result.is_err(), cancel_first);
            assert_eq!(
                ready.owner.finish(),
                if cancel_first {
                    GovernedStatementFinishOutcome::Cancelled(CancellationReason::Requested)
                } else {
                    GovernedStatementFinishOutcome::Completed
                }
            );
            foreign_release.send(()).unwrap();
            drive_original_statement_actions(&slot);
            let mut ready = None;
            foreign_receiver
                .take()
                .unwrap()
                .await
                .unwrap()
                .take_ready_in(&mut ready)
                .unwrap_or_else(|_| panic!("foreign original receiver"))
                .finish()
                .unwrap();
            let ready = ready.take().unwrap();
            assert!(ready.result.is_ok());
            assert_eq!(
                ready.owner.finish(),
                GovernedStatementFinishOutcome::Completed
            );
            assert_eq!(foreign_drops.load(std::sync::atomic::Ordering::SeqCst), 1);
            drive_original_statement_actions(&slot);
            assert_eq!(workload.snapshot().root_responsibilities, 0);
            assert!(!slot.has_sources());
        }
    }

    #[tokio::test]
    async fn original_statement_source_keeps_owner_until_actual_action_drop_and_protocol_take() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let token = owner.as_ref().unwrap().token();
        let (release, gate) = futures::channel::oneshot::channel();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalStatementGate {
                gate,
                drops: Arc::clone(&drops),
            }));
        let mut receiver = None;
        let publication = service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::Empty,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut receiver,
            )
            .unwrap();
        assert!(owner.is_none() && action.is_none());
        publication.finish().unwrap();
        assert_eq!(
            control.finish_governed_statement(token),
            GovernedStatementFinishOutcome::Stale,
            "the old numeric finish route cannot destroy the source"
        );
        let mut wait = Box::pin(receiver.take().unwrap());
        use futures::FutureExt;
        assert!(wait.as_mut().now_or_never().is_none());
        drive_original_statement_actions(&slot);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        release.send(()).unwrap();
        drive_original_statement_actions(&slot);
        assert_eq!(
            drops.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "actual action Drop precedes the protocol handoff"
        );
        let receipt = wait.await.unwrap();
        let mut stable_ready = None;
        receipt
            .take_ready_in(&mut stable_ready)
            .unwrap_or_else(|_| panic!("same original stable receiver"))
            .finish()
            .unwrap();
        let ready = stable_ready.take().unwrap();
        assert!(ready.result.is_ok());
        assert_eq!(ready.owner.token(), token);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        ready.owner.finish();
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }
    #[test]
    fn abandoning_the_waiter_never_drops_the_original_inflight_statement() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let (release, gate) = futures::channel::oneshot::channel();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalStatementGate {
                gate,
                drops: Arc::clone(&drops),
            }));
        let mut receiver = None;
        service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::Empty,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut receiver,
            )
            .unwrap()
            .finish()
            .unwrap();
        drop(receiver.take());
        drive_original_statement_actions(&slot);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        assert!(matches!(
            service.begin_governed_query_statement(
                session,
                &workload.root_admission(),
                None,
                None,
                None
            ),
            Err(GovernedQueryStatementBeginError::QueryControl(
                QueryControlError::StatementBusy
            ))
        ));
        release.send(()).unwrap();
        drive_original_statement_actions(&slot);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert!(!slot.has_sources());
    }
    #[test]
    fn original_statement_poll_panic_retains_same_cause_and_owner_without_repoll() {
        use crate::api::StatementExternalEffectDisposition;
        use std::future::Future;
        type Observer =
            Arc<std::sync::Mutex<Option<crate::session_control::StatementActionWaiter>>>;
        struct Cause(Arc<std::sync::atomic::AtomicUsize>);
        impl Drop for Cause {
            fn drop(&mut self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        struct PanicAction {
            polls: Arc<std::sync::atomic::AtomicUsize>,
            cause: Option<Cause>,
            observer: Observer,
        }
        impl crate::session_control::GovernedStatementAction for PanicAction {
            fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
                let mut observer = self.observer.lock().unwrap();
                let observer = observer.as_mut().expect("same UI observer");
                let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
                assert!(
                    std::pin::Pin::new(observer).poll(&mut cx).is_pending(),
                    "the first failure is not exposed before the original fact snapshot"
                );
                Some(StatementExternalEffectDisposition::KnownCommitted)
            }

            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<
                    crate::session_control::StatementActionValue,
                    crate::session_error::QueryServiceError,
                >,
            > {
                self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::panic::panic_any(self.cause.take().unwrap())
            }
        }
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observer: Observer = Arc::new(std::sync::Mutex::new(None));
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(PanicAction {
                polls: Arc::clone(&polls),
                cause: Some(Cause(Arc::clone(&drops))),
                observer: Arc::clone(&observer),
            }));
        let mut receiver = None;
        service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::Empty,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut receiver,
            )
            .unwrap()
            .finish()
            .unwrap();
        *observer.lock().unwrap() = receiver.take();
        drive_original_statement_actions(&slot);
        let failure = slot.failure().expect("same original cause");
        assert!(failure.panic_payload().unwrap().is::<Cause>());
        assert_eq!(
            failure.external_effect_disposition(),
            Some(StatementExternalEffectDisposition::KnownCommitted)
        );
        let mut waiter = observer.lock().unwrap().take().unwrap();
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        match std::pin::Pin::new(&mut waiter).poll(&mut cx) {
            std::task::Poll::Ready(Err(
                crate::session_control::StatementActionWaitError::Failed(packet),
            )) => {
                assert!(Arc::ptr_eq(&failure, &packet));
                assert_eq!(
                    packet.external_effect_disposition(),
                    Some(StatementExternalEffectDisposition::KnownCommitted)
                );
            }
            _ => panic!("the original observer receives the complete stable failure"),
        }
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        drop(waiter);
        drive_original_statement_actions(&slot);
        assert_eq!(
            polls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a poisoned source is never repolled"
        );
        let source = control.next_statement_action(None).unwrap().1.unwrap();
        drop(failure);
        source.dispose_poisoned_for_test(&service);
        drop(source);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn original_statement_retains_typed_local_health_on_pending_and_ready_without_repoll() {
        use crate::session_control::{GovernedStatementAction, StatementActionValue};
        use std::sync::atomic::{AtomicUsize, Ordering};
        #[derive(Debug)]
        struct OriginalHealthCause(Arc<AtomicUsize>);
        impl std::fmt::Display for OriginalHealthCause {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("original local source exit is unproven")
            }
        }
        impl std::error::Error for OriginalHealthCause {}
        impl Drop for OriginalHealthCause {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct HealthAction {
            failure: crate::api::StatementNativeHealthFailure,
            ready: bool,
            polls: Arc<AtomicUsize>,
            drops: Arc<AtomicUsize>,
        }
        impl GovernedStatementAction for HealthAction {
            fn local_health_failure(&self) -> Option<crate::api::StatementNativeHealthFailure> {
                Some(self.failure.clone())
            }
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<StatementActionValue, crate::session_error::QueryServiceError>,
            > {
                self.polls.fetch_add(1, Ordering::SeqCst);
                if self.ready {
                    std::task::Poll::Ready(Ok(StatementActionValue::Ok(
                        crate::session_control::StatementCommandCompletion::empty(),
                    )))
                } else {
                    std::task::Poll::Pending
                }
            }
        }
        impl Drop for HealthAction {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        for ready in [false, true] {
            let (control, service, workload) = governed_control();
            let session = register(&control, "root");
            let slot = crate::session_control::StatementProgressSlot::empty();
            slot.install(
                service.clone(),
                std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
            )
            .unwrap();
            let polls = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let cause_drops = Arc::new(AtomicUsize::new(0));
            let mut owner = Some(
                service
                    .begin_governed_query_statement(
                        session,
                        &workload.root_admission(),
                        None,
                        None,
                        None,
                    )
                    .unwrap(),
            );
            let mut action: Option<Box<dyn GovernedStatementAction>> =
                Some(Box::new(HealthAction {
                    failure: crate::api::StatementNativeHealthFailure::actual_exit_unproven(
                        "original local failure",
                        Arc::new(OriginalHealthCause(Arc::clone(&cause_drops))),
                    ),
                    ready,
                    polls: Arc::clone(&polls),
                    drops: Arc::clone(&drops),
                }));
            let mut observer = None;
            service
                .publish_statement_action_in(
                    &mut owner,
                    &mut action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Empty,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut observer,
                )
                .unwrap()
                .finish()
                .unwrap();
            drive_original_statement_actions(&slot);
            let failure = slot.failure().expect("same typed health packet");
            assert!(failure.panic_payload().is_none());
            assert!(
                failure
                    .health()
                    .unwrap()
                    .cause()
                    .is::<OriginalHealthCause>()
            );
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert_eq!(workload.snapshot().root_responsibilities, 1);
            let mut observer = observer.take().unwrap();
            let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
            use std::future::Future;
            match std::pin::Pin::new(&mut observer).poll(&mut cx) {
                std::task::Poll::Ready(Err(
                    crate::session_control::StatementActionWaitError::Failed(packet),
                )) => {
                    assert!(Arc::ptr_eq(&failure, &packet));
                    assert!(packet.health().is_some());
                }
                _ => panic!("typed health must not publish the safe Ready value"),
            }
            drop(observer);
            drive_original_statement_actions(&slot);
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            let source = control.next_statement_action(None).unwrap().1.unwrap();
            drop(failure);
            source.dispose_poisoned_for_test(&service);
            drop(source);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
            assert_eq!(workload.snapshot().root_responsibilities, 0);
        }
    }

    #[test]
    fn ordinary_statement_service_error_is_not_a_local_health_failure() {
        struct DomainError;
        impl crate::session_control::GovernedStatementAction for DomainError {
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<
                    crate::session_control::StatementActionValue,
                    crate::session_error::QueryServiceError,
                >,
            > {
                std::task::Poll::Ready(Err(crate::session_error::QueryServiceError::new(
                    crate::session_error::QueryServiceErrorKind::InvalidValue,
                    "original business rejection",
                )))
            }
        }
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(DomainError));
        let mut observer = None;
        service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::Empty,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut observer,
            )
            .unwrap()
            .finish()
            .unwrap();
        drive_original_statement_actions(&slot);
        assert!(slot.failure().is_none());
        let mut observer = observer.take().unwrap();
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        use std::future::Future;
        let std::task::Poll::Ready(Ok(receipt)) = std::pin::Pin::new(&mut observer).poll(&mut cx)
        else {
            panic!("ordinary domain result retains the normal receipt protocol");
        };
        let mut ready = None;
        receipt
            .take_ready_in(&mut ready)
            .unwrap_or_else(|_| panic!("original receipt destination must be empty"))
            .finish()
            .unwrap();
        let ready = ready.take().unwrap();
        let error = ready.result.err().expect("original service error");
        assert_eq!(
            error.kind(),
            crate::session_error::QueryServiceErrorKind::InvalidValue
        );
        ready.owner.finish();
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }

    #[test]
    fn original_statement_action_drop_panic_preserves_publication_and_raw_cause() {
        use crate::api::StatementExternalEffectDisposition;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Cause(Arc<AtomicUsize>);
        impl Drop for Cause {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct DropPanicAction {
            disposition: StatementExternalEffectDisposition,
            polls: Arc<AtomicUsize>,
            cause: Option<Cause>,
        }
        impl crate::session_control::GovernedStatementAction for DropPanicAction {
            fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
                Some(self.disposition)
            }
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<
                    crate::session_control::StatementActionValue,
                    crate::session_error::QueryServiceError,
                >,
            > {
                self.polls.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Ready(Ok(crate::session_control::StatementActionValue::Ok(
                    crate::session_control::StatementCommandCompletion::empty(),
                )))
            }
        }
        impl Drop for DropPanicAction {
            fn drop(&mut self) {
                if let Some(cause) = self.cause.take() {
                    std::panic::panic_any(cause);
                }
            }
        }
        for disposition in [
            StatementExternalEffectDisposition::KnownCommitted,
            StatementExternalEffectDisposition::CommitUnknown,
            StatementExternalEffectDisposition::KnownUncommitted,
        ] {
            let (control, service, workload) = governed_control();
            let session = register(&control, "root");
            let slot = crate::session_control::StatementProgressSlot::empty();
            slot.install(
                service.clone(),
                std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
            )
            .unwrap();
            let polls = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let mut owner = Some(
                service
                    .begin_governed_query_statement(
                        session,
                        &workload.root_admission(),
                        None,
                        None,
                        None,
                    )
                    .unwrap(),
            );
            let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
                Some(Box::new(DropPanicAction {
                    disposition,
                    polls: Arc::clone(&polls),
                    cause: Some(Cause(Arc::clone(&drops))),
                }));
            let mut observer = None;
            service
                .publish_statement_action_in(
                    &mut owner,
                    &mut action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Empty,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut observer,
                )
                .unwrap()
                .finish()
                .unwrap();
            drive_original_statement_actions(&slot);
            let failure = slot.failure().expect("same original destructor cause");
            assert!(failure.panic_payload().unwrap().is::<Cause>());
            assert_eq!(failure.external_effect_disposition(), Some(disposition));
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert_eq!(workload.snapshot().root_responsibilities, 1);
            drop(observer.take());
            drive_original_statement_actions(&slot);
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            let source = control.next_statement_action(None).unwrap().1.unwrap();
            drop(failure);
            source.dispose_poisoned_for_test(&service);
            drop(source);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(workload.snapshot().root_responsibilities, 0);
        }
    }

    #[test]
    fn original_statement_stopped_observer_keeps_publication_after_actual_settlement() {
        use crate::api::StatementExternalEffectDisposition;
        use std::future::Future;
        struct PublishedAction(StatementExternalEffectDisposition);
        impl crate::session_control::GovernedStatementAction for PublishedAction {
            fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
                Some(self.0)
            }
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<
                    crate::session_control::StatementActionValue,
                    crate::session_error::QueryServiceError,
                >,
            > {
                std::task::Poll::Ready(Ok(crate::session_control::StatementActionValue::Ok(
                    crate::session_control::StatementCommandCompletion::empty(),
                )))
            }
        }
        for disposition in [
            StatementExternalEffectDisposition::KnownCommitted,
            StatementExternalEffectDisposition::CommitUnknown,
            StatementExternalEffectDisposition::KnownUncommitted,
        ] {
            let (control, service, workload) = governed_control();
            let session = register(&control, "root");
            let slot = crate::session_control::StatementProgressSlot::empty();
            slot.install(
                service.clone(),
                std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
            )
            .unwrap();
            let mut owner = Some(
                service
                    .begin_governed_query_statement(
                        session,
                        &workload.root_admission(),
                        None,
                        None,
                        None,
                    )
                    .unwrap(),
            );
            let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
                Some(Box::new(PublishedAction(disposition)));
            let mut observer = None;
            service
                .publish_statement_action_in(
                    &mut owner,
                    &mut action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Empty,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut observer,
                )
                .unwrap()
                .finish()
                .unwrap();
            slot.close();
            drive_original_statement_actions(&slot);
            assert!(!slot.has_sources());
            assert_eq!(workload.snapshot().root_responsibilities, 0);
            let mut observer = observer.take().unwrap();
            let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
            match std::pin::Pin::new(&mut observer).poll(&mut cx) {
                std::task::Poll::Ready(Err(
                    crate::session_control::StatementActionWaitError::Stopped { external_effect },
                )) => assert_eq!(external_effect, Some(disposition)),
                _ => panic!("actual source settlement keeps its original publication fact"),
            }
        }
    }

    #[test]
    fn stopped_statement_publication_returns_the_same_two_original_sources() {
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let token = owner.as_ref().unwrap().token();
        let (_release, gate) = futures::channel::oneshot::channel();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalStatementGate {
                gate,
                drops: Arc::clone(&drops),
            }));
        let mut receiver = None;
        slot.close();
        let refusal = service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::Empty,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut receiver,
            )
            .unwrap();
        assert_eq!(refusal.result(), &Err(QueryControlError::StatementBusy));
        assert_eq!(owner.as_ref().unwrap().token(), token);
        assert!(action.is_some() && receiver.is_none());
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(refusal.finish(), Err(QueryControlError::StatementBusy));
        drop(action);
        owner.take().unwrap().protocol_fail();
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }
    #[test]
    fn abandoned_statement_cleanup_wake_panic_keeps_the_same_source_and_owner() {
        use std::future::Future;
        use std::sync::atomic::{AtomicUsize, Ordering};
        type OriginalWait = std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;
        struct CleanupCause(Arc<AtomicUsize>);
        impl Drop for CleanupCause {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct PanicOnce {
            cause: Mutex<Option<CleanupCause>>,
        }
        impl std::task::Wake for PanicOnce {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                let cause = self.cause.lock().unwrap().take();
                if let Some(cause) = cause {
                    std::panic::panic_any(cause);
                }
            }
        }
        struct ArmActualCleanupWake {
            workload: Arc<WorkloadControl>,
            wait: Arc<Mutex<Option<OriginalWait>>>,
            wake: Arc<PanicOnce>,
        }
        impl crate::session_control::GovernedStatementAction for ArmActualCleanupWake {
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<
                    crate::session_control::StatementActionValue,
                    crate::session_error::QueryServiceError,
                >,
            > {
                std::task::Poll::Ready(Ok(crate::session_control::StatementActionValue::Ok(
                    crate::session_control::StatementCommandCompletion::empty(),
                )))
            }
        }
        impl Drop for ArmActualCleanupWake {
            fn drop(&mut self) {
                // Arm the REAL authority waiter after action exit and before
                // the original root/permit cleanup. No injected cleanup hook.
                let workload = Arc::clone(&self.workload);
                let marker = workload.progress_revision();
                let mut waiting: OriginalWait = Box::pin(async move {
                    workload.wait_progress(marker).await;
                });
                let waker = std::task::Waker::from(Arc::clone(&self.wake));
                assert!(
                    waiting
                        .as_mut()
                        .poll(&mut std::task::Context::from_waker(&waker))
                        .is_pending()
                );
                *self.wait.lock().unwrap() = Some(waiting);
            }
        }
        let (control, service, workload) = governed_control();
        let workload = Arc::new(workload);
        let session = register(&control, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let wake = Arc::new(PanicOnce {
            cause: Mutex::new(Some(CleanupCause(Arc::clone(&drops)))),
        });
        let wait = Arc::new(Mutex::new(None));
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let token = owner.as_ref().unwrap().token();
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(ArmActualCleanupWake {
                workload: Arc::clone(&workload),
                wait: Arc::clone(&wait),
                wake,
            }));
        let mut receiver = None;
        service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::Empty,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut receiver,
            )
            .unwrap()
            .finish()
            .unwrap();
        drop(receiver.take());
        // Settle the original cancellation delivery so the waiter below arms
        // for the true completion callback rather than an older ready intent.
        while let Some(permit) = workload.next_control() {
            permit.acknowledge();
        }
        drive_original_statement_actions(&slot);
        let failure = slot.failure().expect("actual cleanup wake preserved");
        assert!(failure.panic_payload().unwrap().is::<CleanupCause>());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(
            slot.has_sources(),
            "callback panic cannot detach the sole stable receiver"
        );
        assert_eq!(
            control.finish_governed_statement(token),
            GovernedStatementFinishOutcome::Stale
        );
        let source = control.next_statement_action(None).unwrap().1.unwrap();
        assert!(
            !source.is_settled(),
            "partial cleanup is not a terminal receipt"
        );
        assert!(matches!(
            service.begin_governed_query_statement(
                session,
                &workload.root_admission(),
                None,
                None,
                None
            ),
            Err(GovernedQueryStatementBeginError::QueryControl(
                QueryControlError::StatementBusy
            ))
        ));
        let waiting = wait.lock().unwrap().take();
        drop(waiting);
        drop(failure);
        source.dispose_poisoned_for_test(&service);
        drop(source);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert_eq!(workload.snapshot().businesses, 0);
    }
    struct OriginalLocalExplainGate {
        gate: futures::channel::oneshot::Receiver<()>,
        dropped: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl crate::session_control::GovernedStatementAction for OriginalLocalExplainGate {
        fn poll_action(
            &mut self,
            owner: &mut crate::session_control::GovernedActionContext<'_>,
            cx: &mut Context<'_>,
        ) -> Poll<
            Result<
                crate::session_control::StatementActionValue,
                crate::session_error::QueryServiceError,
            >,
        > {
            if std::pin::Pin::new(&mut self.gate).poll(cx).is_pending() {
                return Poll::Pending;
            }
            let window = owner.result_window_alias().expect("same Local window");
            let value = crate::api::LocalResultProducer::try_new(
                owner.scope(),
                window,
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                    novarocks_result_contract::TrinoJsonFacts {
                        parametric_datetime: true,
                        variant: false,
                        variant_binary: false,
                    },
                ),
            )
            .and_then(|producer| {
                producer.produce(|| {
                    crate::api::build_string_query_result("Explain String", vec!["ready".into()])
                })
            })
            .map(crate::session_control::StatementActionValue::Local)
            .map_err(|error| {
                crate::session_error::QueryServiceError::new(
                    crate::session_error::QueryServiceErrorKind::Internal,
                    error,
                )
            });
            Poll::Ready(value)
        }
    }
    impl Drop for OriginalLocalExplainGate {
        fn drop(&mut self) {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    async fn original_local_statement(
        control: &Arc<QueryApplicationControl>,
        service: &QueryControlService,
        workload: &WorkloadControl,
        _id: u32,
    ) -> crate::session_control::GovernedQueryStatementOwner {
        let session = register(control, "root");
        service
            .begin_queued_governed_query_statement_with_result(
                session,
                &workload.root_admission(),
                None,
                None,
                None,
                novarocks_workload_control::ResultWindowClass::Local,
            )
            .await
            .unwrap()
    }
    fn local_workload() -> (
        WorkloadControl,
        novarocks_workload_control::ResultCapacityHandle,
    ) {
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        (workload, capacity)
    }
    #[tokio::test]
    async fn abandoned_explain_dispatch_retains_local_window_until_actual_action_exit() {
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let (workload, capacity) = local_workload();
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let mut owner = Some(original_local_statement(&control, &service, &workload, 7).await);
        let (release, gate) = futures::channel::oneshot::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalLocalExplainGate {
                gate,
                dropped: Arc::clone(&dropped),
            }));
        let mut wait = None;
        service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::ResultSet,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut wait,
            )
            .unwrap()
            .finish()
            .unwrap();
        drive_original_statement_actions(&slot);
        drop(wait.take());
        drive_original_statement_actions(&slot);
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
        release.send(()).unwrap();
        drive_original_statement_actions(&slot);
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
    }
    #[tokio::test]
    async fn local_ready_is_stable_before_tail_panic_and_protocol_transfer_is_pure() {
        use crate::session_control::{StatementActionReady, StatementActionValue};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let (workload, capacity) = local_workload();
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let mut owner = Some(original_local_statement(&control, &service, &workload, 7).await);
        let token = owner.as_ref().unwrap().token();
        let (release, gate) = futures::channel::oneshot::channel();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut action: Option<Box<dyn crate::session_control::GovernedStatementAction>> =
            Some(Box::new(OriginalLocalExplainGate { gate, dropped }));
        let mut wait = None;
        service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::ResultSet,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut wait,
            )
            .unwrap()
            .finish()
            .unwrap();
        release.send(()).unwrap();
        drive_original_statement_actions(&slot);
        let receipt = wait.take().unwrap().await.unwrap();
        let receiver = Arc::new(Mutex::new(None::<StatementActionReady>));
        struct PanicReceiverWake(Arc<Mutex<Option<StatementActionReady>>>);
        impl std::task::Wake for PanicReceiverWake {
            fn wake(self: Arc<Self>) {
                assert!(
                    self.0.try_lock().is_ok(),
                    "receiver notification is outside its lock"
                );
                std::panic::panic_any(17u32);
            }
        }
        let waker = std::task::Waker::from(Arc::new(PanicReceiverWake(Arc::clone(&receiver))));
        let mut cx = Context::from_waker(&waker);
        let mut cursor = crate::session_control::StatementProgressCursor::default();
        let _ = slot.poll_turn(
            &mut cursor,
            std::num::NonZeroUsize::new(7).unwrap(),
            &mut cx,
        );
        let tail = {
            let mut target = receiver.lock().unwrap();
            receipt
                .take_ready_in(&mut target)
                .unwrap_or_else(|_| panic!("same empty receiver"))
        };
        let cause = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tail.finish()));
        assert!(matches!(cause,Err(ref payload) if payload.downcast_ref::<u32>()==Some(&17)));
        assert_eq!(
            receiver.lock().unwrap().as_ref().unwrap().owner.token(),
            token
        );
        assert!(matches!(
            receiver.lock().unwrap().as_ref().unwrap().result,
            Ok(StatementActionValue::Local(_))
        ));
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let mut output = None;
        {
            let mut source = receiver.lock().unwrap();
            StatementActionReady::transfer_protocol_in(&mut source, &mut output).unwrap();
        }
        let crate::protocol_delivery::QuerySessionOutput::GovernedQuery(result) = output.unwrap()
        else {
            panic!("same Local graph")
        };
        let (local, mut protocol) = result.into_parts();
        assert_eq!(local.row_count(), 1);
        drop(local);
        assert_eq!(
            protocol.complete(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
    #[tokio::test]
    async fn local_protocol_refusal_preserves_foreign_graph_and_original_owner() {
        use crate::session_control::{StatementActionReady, StatementActionValue};
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let (workload, capacity) = local_workload();
        let owner = original_local_statement(&control, &service, &workload, 7).await;
        let foreign = original_local_statement(&control, &service, &workload, 8).await;
        let local = crate::api::LocalResultProducer::try_new(
            foreign.scope(),
            foreign.result_window_alias().unwrap(),
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: false,
                    variant_binary: false,
                },
            ),
        )
        .unwrap()
        .produce(|| crate::api::build_string_query_result("Explain String", vec!["foreign".into()]))
        .unwrap();
        let token = owner.token();
        let mut source = Some(StatementActionReady::new(
            Ok(StatementActionValue::Local(local)),
            owner,
        ));
        let mut target = None;
        assert!(StatementActionReady::transfer_protocol_in(&mut source, &mut target).is_err());
        assert!(target.is_none());
        assert_eq!(capacity.snapshot().held_positions, [0, 2, 0, 0]);
        assert_eq!(source.as_ref().unwrap().owner.token(), token);
        let ready = source.take().unwrap();
        drop(ready.result);
        ready.owner.finish();
        foreign.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn original_residual_outlives_business_and_session_without_holding_result_window() {
        use crate::session_control::{
            GovernedStatementAction, GovernedStatementResidual, StatementActionValue,
        };
        use novarocks_workload_control::{
            ResultCapacityConfig, ResultWindowAlias, ResultWindowClass, WorkClass,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Tail {
            gate: futures::channel::oneshot::Receiver<()>,
            dropped: Arc<AtomicUsize>,
            control: std::sync::Weak<QueryApplicationControl>,
        }
        impl GovernedStatementResidual for Tail {
            fn poll_retirement(
                &mut self,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
                assert!(
                    self.control
                        .upgrade()
                        .expect("original control owner")
                        .state
                        .try_lock()
                        .is_ok(),
                    "retirement poll outside registry lock"
                );
                use std::future::Future;
                std::pin::Pin::new(&mut self.gate).poll(cx).map(|result| {
                    result.expect("actual retirement sender");
                    Ok(())
                })
            }
        }
        impl Drop for Tail {
            fn drop(&mut self) {
                assert!(
                    self.control
                        .upgrade()
                        .expect("original control owner")
                        .state
                        .try_lock()
                        .is_ok(),
                    "retirement Drop outside registry lock"
                );
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Action {
            window: Option<ResultWindowAlias>,
            tail: Option<Box<dyn GovernedStatementResidual>>,
            dropped: Arc<AtomicUsize>,
        }
        impl GovernedStatementAction for Action {
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<StatementActionValue, crate::session_error::QueryServiceError>,
            > {
                std::task::Poll::Ready(Ok(StatementActionValue::Ok(
                    crate::session_control::StatementCommandCompletion::empty(),
                )))
            }
            fn handoff_residual_in(
                &mut self,
                target: &mut Option<Box<dyn GovernedStatementResidual>>,
            ) -> Result<(), crate::api::StatementNativeHealthFailure> {
                assert!(target.is_none());
                *target = self.tail.take();
                Ok(())
            }
        }
        impl Drop for Action {
            fn drop(&mut self) {
                drop(self.window.take());
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let config = WorkloadConfig {
            root_limit: 2,
            business_limit: 2,
            query_concurrency_limit: 1,
            scope_records_limit: 2,
            ..WorkloadConfig::default()
        };
        let source_slots = std::num::NonZeroUsize::new(config.scope_records_limit).unwrap();
        let workload = WorkloadControl::try_new(
            config,
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig {
                positions: [1; 4],
                client_compute_positions: 1,
                client_short_tail_positions: 0,
                supported_cancel_burst: 0,
                sustained_cancels_per_second: 0,
                ..ResultCapacityConfig::V1
            })
            .unwrap();
        workload.mark_ready().unwrap();
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(service.clone(), source_slots).unwrap();
        let session = register(&control, "root");
        let action_drops = Arc::new(AtomicUsize::new(0));
        let tail_drops = Arc::new(AtomicUsize::new(0));
        let mut releases = Vec::new();
        for index in 0..2 {
            let mut owner = Some(
                service
                    .begin_governed_statement_with_result(
                        session,
                        &workload.root_admission(),
                        WorkClass::Management,
                        None,
                        None,
                        None,
                        ResultWindowClass::Local,
                    )
                    .expect("same session can start its next real statement"),
            );
            let (release, gate) = futures::channel::oneshot::channel();
            releases.push(release);
            let mut action: Option<Box<dyn GovernedStatementAction>> = Some(Box::new(Action {
                window: owner.as_ref().unwrap().result_window_alias(),
                tail: Some(Box::new(Tail {
                    gate,
                    dropped: tail_drops.clone(),
                    control: Arc::downgrade(&control),
                })),
                dropped: action_drops.clone(),
            }));
            let mut waiter = None;
            service
                .publish_statement_action_in(
                    &mut owner,
                    &mut action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Empty,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut waiter,
                )
                .unwrap()
                .finish()
                .unwrap();
            drive_original_statement_actions(&slot);
            assert_eq!(action_drops.load(Ordering::SeqCst), index + 1);
            assert_eq!(tail_drops.load(Ordering::SeqCst), 0);
            let receipt = waiter.take().unwrap().await.unwrap();
            let mut ready = None;
            receipt
                .take_ready_in(&mut ready)
                .unwrap_or_else(|_| panic!("same stable business receiver"))
                .finish()
                .unwrap();
            let ready = ready.take().unwrap();
            assert!(ready.result.is_ok());
            ready.owner.finish();
            assert_eq!(
                capacity.snapshot().held_positions,
                [0; 4],
                "residual does not retain the result window"
            );
            assert_eq!(
                workload.snapshot().root_responsibilities,
                index + 1,
                "same root remains actual residual owner"
            );
            drive_original_statement_actions(&slot);
            assert!(slot.has_sources());
        }
        assert_eq!(
            control.lock().statement_sources.iter().flatten().count(),
            source_slots.get()
        );
        assert!(
            matches!(
                service.begin_governed_statement_with_result(
                    session,
                    &workload.root_admission(),
                    WorkClass::Management,
                    None,
                    None,
                    None,
                    ResultWindowClass::Local,
                ),
                Err(GovernedQueryStatementBeginError::Admission(_))
            ),
            "actual original N is exhausted, not silently enlarged"
        );
        control.unregister_session(session);
        assert!(
            slot.has_sources(),
            "actual registry rows survive Session removal"
        );
        slot.close();
        drive_original_statement_actions(&slot);
        assert_eq!(tail_drops.load(Ordering::SeqCst), 0);
        assert_eq!(workload.snapshot().root_responsibilities, 2);
        for release in releases {
            release.send(()).unwrap();
        }
        drive_original_statement_actions(&slot);
        assert_eq!(tail_drops.load(Ordering::SeqCst), 2);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert!(
            !slot.has_sources(),
            "same original root sees actual retirement"
        );
        workload.close_admission();
        assert!(
            workload.shutdown().is_ok(),
            "same WC must actually drain after both real tails"
        );
    }

    #[test]
    fn retirement_poll_panic_preserves_original_root_and_cause_without_repoll() {
        use crate::session_control::{
            GovernedStatementAction, GovernedStatementResidual, StatementActionValue,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Cause(Arc<AtomicUsize>);
        impl Drop for Cause {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Tail {
            cause: Option<Cause>,
            polls: Arc<AtomicUsize>,
            drops: Arc<AtomicUsize>,
        }
        impl GovernedStatementResidual for Tail {
            fn poll_retirement(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
                self.polls.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(self.cause.take().expect("original opaque cause"));
            }
        }
        impl Drop for Tail {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Action(Option<Box<dyn GovernedStatementResidual>>);
        impl GovernedStatementAction for Action {
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<StatementActionValue, crate::session_error::QueryServiceError>,
            > {
                std::task::Poll::Ready(Ok(StatementActionValue::Ok(
                    crate::session_control::StatementCommandCompletion::empty(),
                )))
            }
            fn handoff_residual_in(
                &mut self,
                target: &mut Option<Box<dyn GovernedStatementResidual>>,
            ) -> Result<(), crate::api::StatementNativeHealthFailure> {
                assert!(target.is_none());
                *target = self.0.take();
                Ok(())
            }
        }
        let (control, service, workload) = governed_control();
        let session = register(&control, "root");
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let mut owner = Some(
            service
                .begin_governed_query_statement(
                    session,
                    &workload.root_admission(),
                    None,
                    None,
                    None,
                )
                .unwrap(),
        );
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let cause_drops = Arc::new(AtomicUsize::new(0));
        let mut action: Option<Box<dyn GovernedStatementAction>> =
            Some(Box::new(Action(Some(Box::new(Tail {
                cause: Some(Cause(cause_drops.clone())),
                polls: polls.clone(),
                drops: drops.clone(),
            })))));
        let mut waiter = None;
        service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::Empty,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut waiter,
            )
            .unwrap()
            .finish()
            .unwrap();
        // No UI take can supply the first residual progress notification.
        drop(waiter.take());
        drive_original_statement_actions(&slot);
        // Original business F closed and the abandoned protocol actually settled.
        // Its accepted residual must wake/register under the same root itself.
        drive_original_statement_actions(&slot);
        let packet = slot
            .failure()
            .expect("original retirement panic remains health custody");
        assert!(packet.panic_payload().unwrap().is::<Cause>());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
        assert_eq!(workload.snapshot().root_responsibilities, 1);
        drop(waiter.take());
        drive_original_statement_actions(&slot);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let source = control.next_statement_action(None).unwrap().1.unwrap();
        source.dispose_poisoned_for_test(&service);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(workload.snapshot().root_responsibilities, 0);
        assert!(!slot.has_sources());
        assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
        drop(packet);
        assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
        drop(source);
        workload.close_admission();
        assert!(
            workload.shutdown().is_ok(),
            "explicit actual test disposition must drain the same WC"
        );
    }

    #[tokio::test]
    async fn retirement_panic_preserves_late_business_ready_and_abandoned_protocol_exit() {
        use crate::session_control::{
            GovernedStatementAction, GovernedStatementResidual, StatementActionValue,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Cause {
            drops: Arc<AtomicUsize>,
            identity: [u8; 16],
        }
        impl Drop for Cause {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Tail {
            cause: Option<Cause>,
            polls: Arc<AtomicUsize>,
            drops: Arc<AtomicUsize>,
        }
        impl GovernedStatementResidual for Tail {
            fn poll_retirement(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
                self.polls.fetch_add(1, Ordering::SeqCst);
                std::panic::panic_any(self.cause.take().expect("original retirement cause"));
            }
        }
        impl Drop for Tail {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Action(Option<Box<dyn GovernedStatementResidual>>);
        impl GovernedStatementAction for Action {
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<StatementActionValue, crate::session_error::QueryServiceError>,
            > {
                std::task::Poll::Ready(Ok(StatementActionValue::Ok(
                    crate::session_control::StatementCommandCompletion::empty(),
                )))
            }
            fn handoff_residual_in(
                &mut self,
                target: &mut Option<Box<dyn GovernedStatementResidual>>,
            ) -> Result<(), crate::api::StatementNativeHealthFailure> {
                assert!(target.is_none());
                *target = self.0.take();
                Ok(())
            }
        }
        // Both real observation paths delay the UI until retirement has failed.
        // The abandoned path must still settle the original protocol/window.
        for deliver_business in [true, false] {
            let control = Arc::new(QueryApplicationControl::default());
            let service = QueryControlService::new(control.clone());
            let (workload, capacity) = local_workload();
            let slot = crate::session_control::StatementProgressSlot::empty();
            slot.install(
                service.clone(),
                std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
            )
            .unwrap();
            let mut owner = Some(original_local_statement(&control, &service, &workload, 7).await);
            let session = owner.as_ref().unwrap().token().session();
            let polls = Arc::new(AtomicUsize::new(0));
            let tail_drops = Arc::new(AtomicUsize::new(0));
            let cause_drops = Arc::new(AtomicUsize::new(0));
            let mut action: Option<Box<dyn GovernedStatementAction>> =
                Some(Box::new(Action(Some(Box::new(Tail {
                    cause: Some(Cause {
                        drops: cause_drops.clone(),
                        identity: [73; 16],
                    }),
                    polls: polls.clone(),
                    drops: tail_drops.clone(),
                })))));
            let mut waiter = None;
            service
                .publish_statement_action_in(
                    &mut owner,
                    &mut action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Empty,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut waiter,
                )
                .unwrap()
                .finish()
                .unwrap();
            drive_original_statement_actions(&slot);
            drive_original_statement_actions(&slot);
            let packet = slot
                .failure()
                .expect("same source retains retirement health");
            assert_eq!(
                packet
                    .panic_payload()
                    .unwrap()
                    .downcast_ref::<Cause>()
                    .unwrap()
                    .identity,
                [73; 16],
            );
            assert_eq!(polls.load(Ordering::SeqCst), 1);
            assert_eq!(tail_drops.load(Ordering::SeqCst), 0);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
            let source = control.next_statement_action(None).unwrap().1.unwrap();
            if deliver_business {
                let receipt = waiter
                    .take()
                    .unwrap()
                    .await
                    .unwrap_or_else(|_| panic!("retirement cannot veto completed business"));
                let mut ready = None;
                receipt
                    .take_ready_in(&mut ready)
                    .unwrap_or_else(|_| panic!("same completed business installs once"))
                    .finish()
                    .unwrap();
                let ready = ready.take().unwrap();
                assert!(
                    matches!(ready.result, Ok(StatementActionValue::Ok(completion)) if completion == crate::session_control::StatementCommandCompletion::empty())
                );
                ready.owner.finish();
            } else {
                drop(waiter.take());
                drive_original_statement_actions(&slot);
                assert!(
                    source.is_settled(),
                    "retirement health does not block protocol cleanup"
                );
            }
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            assert_eq!(workload.snapshot().root_responsibilities, 1);
            assert!(
                slot.has_sources(),
                "retirement failure is not source retirement"
            );
            drop(waiter.take());
            control.unregister_session(session);
            drive_original_statement_actions(&slot);
            assert_eq!(
                polls.load(Ordering::SeqCst),
                1,
                "poisoned tail is never repolled"
            );
            assert_eq!(tail_drops.load(Ordering::SeqCst), 0);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            assert_eq!(workload.snapshot().root_responsibilities, 1);
            assert!(Arc::ptr_eq(&packet, &slot.failure().unwrap()));
            source.dispose_poisoned_for_test(&service);
            assert_eq!(tail_drops.load(Ordering::SeqCst), 1);
            assert_eq!(workload.snapshot().root_responsibilities, 0);
            assert!(!slot.has_sources());
            assert_eq!(cause_drops.load(Ordering::SeqCst), 0);
            drop(packet);
            assert_eq!(cause_drops.load(Ordering::SeqCst), 1);
            drop(source);
            workload.close_admission();
            assert!(
                workload.shutdown().is_ok(),
                "explicit same-owner disposition drains WC"
            );
        }
    }
    #[tokio::test]
    async fn completion_projection_failure_keeps_original_commit_fact_and_owner() {
        use crate::api::StatementExternalEffectDisposition;
        use crate::session_control::{
            GovernedStatementAction, StatementActionReady, StatementActionValue,
            StatementCommandCompletion,
        };
        struct CommittedCount;
        impl GovernedStatementAction for CommittedCount {
            fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
                Some(StatementExternalEffectDisposition::KnownCommitted)
            }
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<StatementActionValue, crate::session_error::QueryServiceError>,
            > {
                std::task::Poll::Ready(Ok(StatementActionValue::Ok(
                    StatementCommandCompletion::new(u64::MAX),
                )))
            }
        }
        let control = Arc::new(QueryApplicationControl::default());
        let service = QueryControlService::new(control.clone());
        let (workload, capacity) = local_workload();
        let slot = crate::session_control::StatementProgressSlot::empty();
        slot.install(
            service.clone(),
            std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
        )
        .unwrap();
        let mut owner = Some(original_local_statement(&control, &service, &workload, 7).await);
        let original_token = owner.as_ref().unwrap().token();
        let mut action: Option<Box<dyn GovernedStatementAction>> = Some(Box::new(CommittedCount));
        let mut wait = None;
        service
            .publish_statement_action_in(
                &mut owner,
                &mut action,
                crate::session_control::StatementCompletionProjection::new(
                    crate::session::StatementOutputShape::Rows,
                    novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                        novarocks_result_contract::TrinoJsonFacts {
                            parametric_datetime: true,
                            variant: false,
                            variant_binary: false,
                        },
                    ),
                ),
                &mut wait,
            )
            .unwrap()
            .finish()
            .unwrap();
        drive_original_statement_actions(&slot);
        let receipt = wait.take().unwrap().await.unwrap();
        let mut ready = None;
        receipt
            .take_ready_in(&mut ready)
            .unwrap_or_else(|_| panic!("original ready transfer"))
            .finish()
            .unwrap();
        ready.as_mut().unwrap().prepare_completion(
            crate::session::StatementOutputShape::Rows,
            novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    variant: false,
                    variant_binary: false,
                },
            ),
        );
        assert_eq!(ready.as_ref().unwrap().owner.token(), original_token);
        let error = ready
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .err()
            .expect("bigint overflow refused");
        assert_eq!(
            error.external_effect_disposition(),
            Some(StatementExternalEffectDisposition::KnownCommitted)
        );
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        let mut output = None;
        StatementActionReady::transfer_protocol_in(&mut ready, &mut output).unwrap();
        let crate::protocol_delivery::QuerySessionOutput::GovernedError(error) = output.unwrap()
        else {
            panic!("original governed error")
        };
        let (error, mut protocol) = error.into_parts();
        assert_eq!(
            error.external_effect_disposition(),
            Some(StatementExternalEffectDisposition::KnownCommitted)
        );
        protocol.fail();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
    #[tokio::test]
    async fn post_transfer_failures_retain_original_effect_cause_and_last_window_alias() {
        use crate::api::StatementExternalEffectDisposition;
        use crate::session_control::{
            GovernedStatementAction, StatementActionReady, StatementActionValue,
            StatementCommandCompletion,
        };
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        struct OriginalCompletion(
            StatementCommandCompletion,
            StatementExternalEffectDisposition,
        );
        impl GovernedStatementAction for OriginalCompletion {
            fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
                Some(self.1)
            }
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<StatementActionValue, crate::session_error::QueryServiceError>,
            > {
                std::task::Poll::Ready(Ok(StatementActionValue::Ok(self.0)))
            }
        }
        for (completion, effect, cancel) in [
            (
                StatementCommandCompletion::new(0),
                StatementExternalEffectDisposition::KnownCommitted,
                false,
            ),
            (
                StatementCommandCompletion::no_count(),
                StatementExternalEffectDisposition::CommitUnknown,
                true,
            ),
        ] {
            let control = Arc::new(QueryApplicationControl::default());
            let service = QueryControlService::new(control.clone());
            let (workload, capacity) = local_workload();
            let slot = crate::session_control::StatementProgressSlot::empty();
            slot.install(
                service.clone(),
                std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
            )
            .unwrap();
            let mut owner = Some(original_local_statement(&control, &service, &workload, 7).await);
            let token = owner.as_ref().unwrap().token();
            let mut action: Option<Box<dyn GovernedStatementAction>> =
                Some(Box::new(OriginalCompletion(completion, effect)));
            let mut wait = None;
            service
                .publish_statement_action_in(
                    &mut owner,
                    &mut action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Rows,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut wait,
                )
                .unwrap()
                .finish()
                .unwrap();
            drive_original_statement_actions(&slot);
            let receipt = wait.take().unwrap().await.unwrap();
            let mut ready = None;
            receipt
                .take_ready_in(&mut ready)
                .unwrap_or_else(|_| panic!("same original source"))
                .finish()
                .unwrap();
            ready.as_mut().unwrap().prepare_completion(
                crate::session::StatementOutputShape::Rows,
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                    novarocks_result_contract::TrinoJsonFacts {
                        parametric_datetime: true,
                        variant: false,
                        variant_binary: false,
                    },
                ),
            );
            assert!(ready.as_ref().unwrap().result.is_ok());
            let mut output = None;
            StatementActionReady::transfer_protocol_in(&mut ready, &mut output).unwrap();
            let output = output.unwrap();
            assert_eq!(output.external_effect_disposition(), Some(effect));
            let crate::protocol_delivery::QuerySessionOutput::GovernedCompletion(result) = output
            else {
                panic!("original completion carrier")
            };
            let (actual, local, mut protocol) = result.into_parts_with_rows();
            assert_eq!(actual, completion);
            assert_eq!(protocol.statement_token(), Some(token));
            assert_eq!(protocol.external_effect_disposition(), Some(effect));
            let window = protocol.result_window_alias().unwrap();
            let mut cursor = local.unwrap().into_cursor();
            let physical = cursor.retain_physical_guard();
            let source = if cancel {
                let cancellation = protocol.cancellation();
                let _ = protocol.client_cancel_requested();
                assert!(matches!(
                    cancellation.reason(),
                    Some(crate::cancellation::QueryCancellationReason::ClientCancelRequested)
                ));
                cursor.cancel();
                let failure = cursor
                    .step(&mut [0; 64])
                    .expect_err("actual canceled local cursor");
                assert_eq!(
                    failure.kind,
                    novarocks_result_render::RenderErrorKind::Cancelled
                );
                SharedFailureCause::new(failure)
            } else {
                let failure = window
                    .check_backing_total(u64::MAX)
                    .expect_err("actual original backing envelope refuses overflow");
                assert_eq!(
                    failure,
                    novarocks_workload_control::WorkError::Capacity("result backing envelope")
                );
                let _ = protocol.fail();
                SharedFailureCause::new(failure)
            };
            let error = crate::session_error::QueryServiceError::from_failure_cause(source.clone())
                .attach_external_effect_disposition(
                    protocol.external_effect_disposition().unwrap(),
                );
            assert_eq!(error.failure_cause(), Some(&source));
            assert_eq!(
                error.execution_failure().unwrap().fact(),
                source.failure_fact()
            );
            assert_eq!(error.external_effect_disposition(), Some(effect));
            assert_eq!(protocol.external_effect_disposition(), Some(effect));
            assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
            drop(window);
            drop(cursor);
            drop(protocol);
            assert_eq!(
                capacity.snapshot().held_positions,
                [0, 1, 0, 0],
                "last real physical alias still owns the original window"
            );
            drop(physical);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            assert_eq!(workload.snapshot().root_responsibilities, 0);
        }
    }

    #[tokio::test]
    async fn no_count_completion_keeps_original_root_until_projection_after_residual_exit() {
        use crate::api::StatementExternalEffectDisposition;
        use crate::session_control::{
            GovernedStatementAction, GovernedStatementResidual, StatementActionReady,
            StatementActionValue, StatementCommandCompletion,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct OriginalTail {
            gate: futures::channel::oneshot::Receiver<()>,
            drops: Arc<AtomicUsize>,
        }
        impl GovernedStatementResidual for OriginalTail {
            fn poll_retirement(
                &mut self,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
                use std::future::Future;
                std::pin::Pin::new(&mut self.gate).poll(cx).map(|result| {
                    result.expect("same original residual exit sender");
                    Ok(())
                })
            }
        }
        impl Drop for OriginalTail {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct OriginalNoCount(Option<Box<dyn GovernedStatementResidual>>);
        impl GovernedStatementAction for OriginalNoCount {
            fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
                Some(StatementExternalEffectDisposition::KnownCommitted)
            }
            fn poll_action(
                &mut self,
                _: &mut crate::session_control::GovernedActionContext<'_>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<
                Result<StatementActionValue, crate::session_error::QueryServiceError>,
            > {
                std::task::Poll::Ready(Ok(StatementActionValue::Ok(
                    StatementCommandCompletion::no_count(),
                )))
            }
            fn handoff_residual_in(
                &mut self,
                target: &mut Option<Box<dyn GovernedStatementResidual>>,
            ) -> Result<(), crate::api::StatementNativeHealthFailure> {
                assert!(target.is_none());
                *target = self.0.take();
                Ok(())
            }
        }

        for abandon_before_take in [false, true] {
            let control = Arc::new(QueryApplicationControl::default());
            let service = QueryControlService::new(control.clone());
            let (workload, capacity) = local_workload();
            let slot = crate::session_control::StatementProgressSlot::empty();
            slot.install(
                service.clone(),
                std::num::NonZeroUsize::new(WorkloadConfig::default().scope_records_limit).unwrap(),
            )
            .unwrap();
            let mut owner = Some(original_local_statement(&control, &service, &workload, 7).await);
            let root = owner.as_ref().unwrap().scope().clone();
            let last_alias = owner.as_ref().unwrap().result_window_alias().unwrap();
            let (exit, gate) = futures::channel::oneshot::channel();
            let drops = Arc::new(AtomicUsize::new(0));
            let mut action: Option<Box<dyn GovernedStatementAction>> =
                Some(Box::new(OriginalNoCount(Some(Box::new(OriginalTail {
                    gate,
                    drops: drops.clone(),
                })))));
            let mut waiter = None;
            service
                .publish_statement_action_in(
                    &mut owner,
                    &mut action,
                    crate::session_control::StatementCompletionProjection::new(
                        crate::session::StatementOutputShape::Rows,
                        novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                            novarocks_result_contract::TrinoJsonFacts {
                                parametric_datetime: true,
                                variant: false,
                                variant_binary: false,
                            },
                        ),
                    ),
                    &mut waiter,
                )
                .unwrap()
                .finish()
                .unwrap();
            drive_original_statement_actions(&slot);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            assert!(root.check().is_ok());
            assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);

            // Close the actual original residual, then drive its real retirement
            // before the client takes Ready or constructs its Trino no-count graph.
            exit.send(()).unwrap();
            drive_original_statement_actions(&slot);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
            let before_projection = root.check();
            if abandon_before_take {
                // The closed graph remains in the same Source when the client
                // never takes Ready. Actual abandon disposes it before settlement.
                drop(waiter.take());
                drive_original_statement_actions(&slot);
                assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
                drop(last_alias);
                drive_original_statement_actions(&slot);
                assert_eq!(capacity.snapshot().held_positions, [0; 4]);
                assert_eq!(workload.snapshot().root_responsibilities, 0);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                continue;
            }
            let receipt = waiter.take().unwrap().await.unwrap();
            let mut ready = None;
            receipt
                .take_ready_in(&mut ready)
                .unwrap_or_else(|_| panic!("same original business result and owner"))
                .finish()
                .unwrap();
            assert_eq!(ready.as_ref().unwrap().owner.scope().id(), root.id());
            assert!(matches!(
                ready.as_ref().unwrap().result,
                Ok(StatementActionValue::Ok(StatementCommandCompletion::Rows(
                    crate::session_control::StatementCommandRowCount::NoCount
                )))
            ));
            ready.as_mut().unwrap().prepare_completion(
                crate::session::StatementOutputShape::Rows,
                novarocks_result_contract::ClientPresentationFamily::TrinoJson(
                    novarocks_result_contract::TrinoJsonFacts {
                        parametric_datetime: true,
                        variant: false,
                        variant_binary: false,
                    },
                ),
            );
            let projection_error = ready.as_ref().unwrap().result.as_ref().err().cloned();
            let mut output = None;
            StatementActionReady::transfer_protocol_in(&mut ready, &mut output).unwrap();
            match output.take().unwrap() {
                crate::protocol_delivery::QuerySessionOutput::GovernedCompletion(result) => {
                    let (completion, local, mut protocol) = result.into_parts_with_rows();
                    assert_eq!(completion, StatementCommandCompletion::no_count());
                    assert_eq!(
                        protocol.external_effect_disposition(),
                        Some(StatementExternalEffectDisposition::KnownCommitted)
                    );
                    let mut cursor = local
                        .expect("Trino no-count owns its original NULL row")
                        .into_cursor();
                    let physical = cursor.retain_physical_guard();
                    let mut rendered = Vec::new();
                    let mut rows = 0;
                    loop {
                        let mut bytes = [0; 64];
                        let turn = cursor.step(&mut bytes).unwrap();
                        rendered.extend_from_slice(&bytes[..turn.emitted_bytes]);
                        rows += turn.completed_rows;
                        if turn.status == novarocks_result_render::RenderTurnStatus::InputComplete {
                            break;
                        }
                    }
                    assert_eq!(rows, 1);
                    let payload = b"[null]";
                    let mut expected = (payload.len() as u32).to_le_bytes().to_vec();
                    expected.extend_from_slice(payload);
                    assert_eq!(rendered, expected);
                    drop(cursor);
                    protocol.complete();
                    drop(protocol);
                    assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
                    drop(physical);
                }
                crate::protocol_delivery::QuerySessionOutput::GovernedError(result) => {
                    // Dispose the real failed protocol before the final regression
                    // assertion, so the RED run leaves no original responsibility.
                    let (error, mut protocol) = result.into_parts();
                    assert_eq!(
                        error.external_effect_disposition(),
                        Some(StatementExternalEffectDisposition::KnownCommitted)
                    );
                    protocol.fail();
                }
                _ => panic!("same original command completion carrier"),
            }
            drop(last_alias);
            drive_original_statement_actions(&slot);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            assert_eq!(workload.snapshot().root_responsibilities, 0);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(
                projection_error.is_none(),
                "original Root state before projection: {before_projection:?}; completion: {projection_error:?}"
            );
        }
    }
}
