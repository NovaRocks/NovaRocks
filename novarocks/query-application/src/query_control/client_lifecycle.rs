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

//! Client query admission, management cancellation and protocol observation.

use super::{ActiveStatementCancellation, QueryApplicationControl};
use crate::cancellation::QueryCancellationReason;
use crate::session_control::{
    ClientQueryId, QueryCancelOutcome, QueryControlError, SessionIdentity, SessionToken,
    StatementToken,
};
use novarocks_workload_control::WorkError;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime};

mod history;
use history::{HistoryCharge, RecentClientQueries};

/// The original UTF-8 text allocation, shared by the protocol request and any
/// authorized observer. Retention follows the backing through its last alias.
#[derive(Clone)]
pub struct ClientQueryText(Arc<TextBacking>);

struct TextBacking {
    value: String,
    history_charge: OnceLock<HistoryCharge>,
    original_observation: OnceLock<OriginalTextObservation>,
}

impl ClientQueryText {
    pub fn new(value: String) -> Self {
        Self(Arc::new(TextBacking {
            value,
            history_charge: OnceLock::new(),
            original_observation: OnceLock::new(),
        }))
    }

    pub fn as_str(&self) -> &str {
        &self.0.value
    }

    /// Track this one original allocation through its actual last alias.
    /// Re-observing the same backing adds neither capacity nor another charge.
    pub fn observe_original_backing(&self, tracker: &ClientQueryTextTracker) {
        if let Some(bytes) = self.retained_bytes() {
            self.0
                .original_observation
                .get_or_init(|| tracker.observe(bytes));
        }
    }

    fn retained_bytes(&self) -> Option<usize> {
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        let header = arc_layout(std::alloc::Layout::new::<TextBacking>()).ok()?;
        header.size().checked_add(self.0.value.capacity())
    }
}

impl std::fmt::Debug for ClientQueryText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientQueryText")
            .field("length", &self.0.value.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ClientQueryTextBackingStatus {
    pub requested_bytes: usize,
    pub backings: usize,
}

/// Listener-local counters. Removing a protocol entry does not remove its
/// accounting while history or an authorized reader owns the same allocation.
#[derive(Clone, Default)]
pub struct ClientQueryTextTracker(Arc<Mutex<ClientQueryTextBackingStatus>>);

impl ClientQueryTextTracker {
    pub fn status(&self) -> ClientQueryTextBackingStatus {
        *self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn observe(&self, bytes: usize) -> OriginalTextObservation {
        let mut status = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        status.requested_bytes = status
            .requested_bytes
            .checked_add(bytes)
            .expect("nonaliasing observed allocations fit the process address space");
        status.backings += 1;
        OriginalTextObservation {
            tracker: self.clone(),
            bytes,
        }
    }
}

struct OriginalTextObservation {
    tracker: ClientQueryTextTracker,
    bytes: usize,
}

impl Drop for OriginalTextObservation {
    fn drop(&mut self) {
        let mut status = self
            .tracker
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        status.backings -= 1;
        status.requested_bytes -= self.bytes;
    }
}

/// A wall-clock observation paired with the same event's monotonic instant.
#[derive(Clone, Copy, Debug)]
pub struct ClientQueryTime {
    monotonic: Instant,
    wall_clock: SystemTime,
}

impl ClientQueryTime {
    pub fn now() -> Self {
        Self::new(Instant::now(), SystemTime::now())
    }

    pub fn new(monotonic: Instant, wall_clock: SystemTime) -> Self {
        Self {
            monotonic,
            wall_clock,
        }
    }
}

/// The typed protocol error identity selected by the same original projection
/// that supplies the terminal page. History does not retain the page or error body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientQueryFailure {
    pub code: i32,
    pub name: &'static str,
    pub error_type: &'static str,
}

pub const ADMINISTRATIVELY_KILLED: ClientQueryFailure = ClientQueryFailure {
    code: 38,
    name: "ADMINISTRATIVELY_KILLED",
    error_type: "USER_ERROR",
};
pub const USER_CANCELED: ClientQueryFailure = ClientQueryFailure {
    code: 3,
    name: "USER_CANCELED",
    error_type: "USER_ERROR",
};
pub const ABANDONED_QUERY: ClientQueryFailure = ClientQueryFailure {
    code: 2,
    name: "ABANDONED_QUERY",
    error_type: "USER_ERROR",
};

fn protocol_cancellation_error(reason: &QueryCancellationReason) -> Option<ClientQueryFailure> {
    match reason {
        QueryCancellationReason::ClientCancelRequested => Some(USER_CANCELED),
        QueryCancellationReason::ClientDisconnected => Some(ABANDONED_QUERY),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientQueryConclusion {
    Finished,
    Failed(ClientQueryFailure),
    AdministrativelyKilled(Arc<ManagementCancellationReceipt>),
}

impl ClientQueryConclusion {
    pub fn failure(&self) -> Option<ClientQueryFailure> {
        match self {
            Self::Finished => None,
            Self::Failed(error) => Some(*error),
            Self::AdministrativelyKilled(_) => Some(ADMINISTRATIVELY_KILLED),
        }
    }
}

/// Which original startup/statement boundary decided the accepted kill. A
/// claimed target keeps the same identity until the precise generation binds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagementCancellationCut {
    Registered,
    Claimed,
    Statement(StatementToken),
}

#[derive(Debug, Eq, PartialEq)]
pub struct ManagementCancellationReceipt {
    query: ClientQueryId,
    requester: SessionToken,
    user: Arc<str>,
    message: Option<Box<str>>,
    cut: ManagementCancellationCut,
}

impl ManagementCancellationReceipt {
    pub fn query_id(&self) -> &ClientQueryId {
        &self.query
    }

    pub fn requester(&self) -> SessionToken {
        self.requester
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    pub fn cut(&self) -> ManagementCancellationCut {
        self.cut
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientQueryCancellation {
    Management(Arc<ManagementCancellationReceipt>),
    Protocol(QueryCancellationReason),
}

/// Identity belongs to the registration, and remains valid independently of
/// the history horizon while the original protocol entry holds a reference.
#[derive(Clone)]
pub struct ClientQueryRegistration(Arc<ClientRegistration>);

struct ClientRegistration {
    brand: Weak<()>,
    id: ClientQueryId,
    cancellation: OnceLock<ClientQueryCancellation>,
    cancellation_requested: tokio::sync::Notify,
    conclusion: OnceLock<ClientQueryConclusion>,
}

impl ClientQueryRegistration {
    fn new(brand: &Arc<()>, id: ClientQueryId) -> Self {
        Self(Arc::new(ClientRegistration {
            brand: Arc::downgrade(brand),
            id,
            cancellation: OnceLock::new(),
            cancellation_requested: tokio::sync::Notify::new(),
            conclusion: OnceLock::new(),
        }))
    }

    pub fn id(&self) -> &ClientQueryId {
        &self.0.id
    }

    pub async fn cancellation_requested(&self) {
        loop {
            let changed = self.0.cancellation_requested.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.0.cancellation.get().is_some() {
                return;
            }
            changed.await;
        }
    }

    fn publish_cancellation(&self, cancellation: ClientQueryCancellation) {
        self.0
            .cancellation
            .set(cancellation)
            .expect("single cancellation winner");
        self.0.cancellation_requested.notify_waiters();
    }
}

/// The one successful claim may be handed to Session registration only once.
#[must_use]
pub struct ClientQueryClaim(ClientQueryRegistration);

pub enum ClientQueryStart {
    Claimed(ClientQueryClaim),
    Cancelled,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientQueryState {
    Queued,
    Running,
    Finishing,
    Finished,
    Failed,
}

impl ClientQueryState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::Finishing => "FINISHING",
            Self::Finished => "FINISHED",
            Self::Failed => "FAILED",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ClientQueryColumns {
    pub user: bool,
    pub source: bool,
    pub query: bool,
}

#[derive(Debug)]
pub struct ClientQueryObservation {
    pub query_id: ClientQueryText,
    pub state: ClientQueryState,
    pub user: Option<ClientQueryText>,
    pub source: Option<ClientQueryText>,
    pub query: Option<ClientQueryText>,
    pub queued: Duration,
    pub analysis: Option<Duration>,
    pub planning: Option<Duration>,
    pub created: SystemTime,
    pub claimed: Option<SystemTime>,
    pub started: Option<SystemTime>,
    pub last_heartbeat: Option<SystemTime>,
    pub end: Option<SystemTime>,
    pub error: Option<ClientQueryFailure>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientQueryEvent {
    Admitted,
    AnalysisStarted,
    AnalysisFinished,
    PlanningStarted,
    PlanningFinished,
    ControlStarted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientQueryHistoryConfig {
    pub max_bytes: usize,
    pub max_age: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ClientQueryHistoryStatus {
    pub retained_bytes: usize,
    pub rows: usize,
    pub oldest_finished: Option<Instant>,
    pub next_expiry: Option<Instant>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KillClientQueryError {
    InvalidId,
    UnknownRequester,
    PermissionDenied,
    NotFound,
    NotRunning,
    Work(WorkError),
}

#[doc(hidden)]
pub trait ClientQueryLifecyclePort: Send + Sync {
    fn register_client_query(
        &self,
        id: ClientQueryId,
        principal: Arc<str>,
        source: Option<Arc<str>>,
        text: Option<&ClientQueryText>,
        time: ClientQueryTime,
    ) -> Result<ClientQueryRegistration, QueryControlError>;
    fn withdraw_client_query(&self, registration: &ClientQueryRegistration) -> bool;
    fn claim_client_query(
        &self,
        registration: &ClientQueryRegistration,
        time: ClientQueryTime,
    ) -> ClientQueryStart;
    fn register_claimed_session(
        &self,
        identity: SessionIdentity,
        claim: ClientQueryClaim,
    ) -> Result<SessionToken, QueryControlError>;
    fn touch_client_query(&self, registration: &ClientQueryRegistration, time: ClientQueryTime);
    fn request_unstarted_client_cancel(
        &self,
        registration: &ClientQueryRegistration,
        reason: QueryCancellationReason,
    ) -> bool;
    fn client_query_cancellation(
        &self,
        registration: &ClientQueryRegistration,
    ) -> Option<ClientQueryCancellation>;
    fn settle_client_query(
        &self,
        registration: &ClientQueryRegistration,
        proposed: ClientQueryConclusion,
        time: ClientQueryTime,
    ) -> Option<ClientQueryConclusion>;
    fn record_client_query_event(
        &self,
        statement: StatementToken,
        event: ClientQueryEvent,
        time: ClientQueryTime,
    ) -> bool;
    fn kill_client_query(
        &self,
        requester: SessionToken,
        target: &str,
        message: Option<&str>,
        now: ClientQueryTime,
    ) -> Result<Arc<ManagementCancellationReceipt>, KillClientQueryError>;
    fn observe_client_query(
        &self,
        principal: &str,
        id: &str,
        columns: ClientQueryColumns,
        now: ClientQueryTime,
    ) -> Option<ClientQueryObservation>;
    fn next_client_query(
        &self,
        principal: &str,
        after: Option<&str>,
        columns: ClientQueryColumns,
        now: ClientQueryTime,
    ) -> Option<ClientQueryObservation>;
    fn subscribe_client_query_history(&self) -> tokio::sync::watch::Receiver<Option<Instant>>;
    fn client_query_history_status(&self) -> ClientQueryHistoryStatus;
    fn evict_client_query_history(&self, now: Instant) -> ClientQueryHistoryStatus;
}

#[derive(Default)]
struct EventTimes {
    admitted: Option<ClientQueryTime>,
    analysis_started: Option<ClientQueryTime>,
    analysis_finished: Option<ClientQueryTime>,
    planning_started: Option<ClientQueryTime>,
    planning_finished: Option<ClientQueryTime>,
    control_started: Option<ClientQueryTime>,
}

impl EventTimes {
    fn record(&mut self, event: ClientQueryEvent, time: ClientQueryTime) -> bool {
        let target = match event {
            ClientQueryEvent::Admitted => &mut self.admitted,
            ClientQueryEvent::AnalysisStarted => &mut self.analysis_started,
            ClientQueryEvent::AnalysisFinished if self.analysis_started.is_some() => {
                &mut self.analysis_finished
            }
            ClientQueryEvent::PlanningStarted => &mut self.planning_started,
            ClientQueryEvent::PlanningFinished if self.planning_started.is_some() => {
                &mut self.planning_finished
            }
            ClientQueryEvent::ControlStarted => &mut self.control_started,
            _ => return false,
        };
        if target.is_some() {
            return false;
        }
        *target = Some(time);
        true
    }
}

#[derive(Clone, Copy)]
struct ObservationTimes {
    queued: Duration,
    analysis: Option<Duration>,
    planning: Option<Duration>,
    created: SystemTime,
    claimed: Option<SystemTime>,
    started: Option<SystemTime>,
    last_heartbeat: Option<SystemTime>,
}

impl ObservationTimes {
    fn snapshot(query: &LiveQuery, now: ClientQueryTime) -> Self {
        let times = &query.events;
        let elapsed = |start: ClientQueryTime, end: Option<ClientQueryTime>| {
            end.unwrap_or(now)
                .monotonic
                .saturating_duration_since(start.monotonic)
        };
        Self {
            queued: elapsed(query.created, times.admitted),
            analysis: times
                .analysis_started
                .map(|start| elapsed(start, times.analysis_finished)),
            planning: times
                .planning_started
                .map(|start| elapsed(start, times.planning_finished)),
            created: query.created.wall_clock,
            claimed: query.claimed.map(|time| time.wall_clock),
            started: times
                .planning_started
                .or(times.control_started)
                .map(|time| time.wall_clock),
            last_heartbeat: query.last_heartbeat.map(|time| time.wall_clock),
        }
    }

    fn observation(
        self,
        query_id: ClientQueryText,
        state: ClientQueryState,
        user: Option<ClientQueryText>,
        source: Option<ClientQueryText>,
        query: Option<ClientQueryText>,
        end: Option<SystemTime>,
        error: Option<ClientQueryFailure>,
    ) -> ClientQueryObservation {
        ClientQueryObservation {
            query_id,
            state,
            user,
            source,
            query,
            end,
            error,
            queued: self.queued,
            analysis: self.analysis,
            planning: self.planning,
            created: self.created,
            claimed: self.claimed,
            started: self.started,
            last_heartbeat: self.last_heartbeat,
        }
    }
}

struct LiveQuery {
    registration: ClientQueryRegistration,
    id: ClientQueryText,
    user: ClientQueryText,
    source: Option<ClientQueryText>,
    text: Option<Weak<TextBacking>>,
    created: ClientQueryTime,
    claimed: Option<ClientQueryTime>,
    session: Option<SessionToken>,
    statement: Option<StatementToken>,
    success_sealed: bool,
    last_heartbeat: Option<ClientQueryTime>,
    events: EventTimes,
}

impl LiveQuery {
    fn observe(&self, columns: ClientQueryColumns, now: ClientQueryTime) -> ClientQueryObservation {
        let state = if self.success_sealed {
            ClientQueryState::Finishing
        } else if self.events.admitted.is_some() {
            ClientQueryState::Running
        } else {
            ClientQueryState::Queued
        };
        let text = columns
            .query
            .then(|| self.text.as_ref()?.upgrade().map(ClientQueryText))
            .flatten();
        ObservationTimes::snapshot(self, now).observation(
            self.id.clone(),
            state,
            columns.user.then(|| self.user.clone()),
            self.source.as_ref().filter(|_| columns.source).cloned(),
            text,
            None,
            None,
        )
    }
}

pub(super) struct ClientQueries {
    brand: Arc<()>,
    active: BTreeMap<ClientQueryId, LiveQuery>,
    history: RecentClientQueries,
}

impl Default for ClientQueries {
    fn default() -> Self {
        Self::new(None)
    }
}

impl ClientQueries {
    pub(super) fn new(config: Option<ClientQueryHistoryConfig>) -> Self {
        Self {
            brand: Arc::new(()),
            active: BTreeMap::new(),
            history: RecentClientQueries::new(config),
        }
    }

    fn belongs_to_owner(&self, registration: &ClientQueryRegistration) -> bool {
        std::ptr::eq(registration.0.brand.as_ptr(), Arc::as_ptr(&self.brand))
    }

    fn live_for(&mut self, registration: &ClientQueryRegistration) -> Option<&mut LiveQuery> {
        self.active
            .get_mut(registration.id())
            .filter(|active| Arc::ptr_eq(&active.registration.0, &registration.0))
    }

    pub(super) fn bind_claim(
        &mut self,
        identity: &SessionIdentity,
        claim: &ClientQueryClaim,
    ) -> Result<(), QueryControlError> {
        let crate::session_control::SessionOrigin::ClientStatement(query) = identity.origin();
        let live = self
            .live_for(&claim.0)
            .ok_or(QueryControlError::StaleSession)?;
        if claim.0.id() != query
            || live.user.as_str() != identity.principal()
            || live.session.is_some()
            || live.claimed.is_none()
        {
            return Err(QueryControlError::StaleSession);
        }
        if live.registration.0.cancellation.get().is_some() {
            return Err(QueryControlError::ClientQueryCancelled);
        }
        Ok(())
    }

    pub(super) fn install_session(&mut self, claim: ClientQueryClaim, session: SessionToken) {
        // The same control lock has held the verified original claim since
        // before Session registration associated the ID and its principal.
        self.live_for(&claim.0)
            .expect("verified client query claim")
            .session = Some(session);
    }

    pub(super) fn reserve_unclaimed_identity(&self, query: &ClientQueryId) -> bool {
        self.active.contains_key(query) || self.history.get(query.as_str()).is_some()
    }

    pub(super) fn install_statement(
        &mut self,
        query: &ClientQueryId,
        token: StatementToken,
    ) -> Result<(), QueryControlError> {
        let live = self
            .active
            .get_mut(query)
            .ok_or(QueryControlError::StaleSession)?;
        if live.session != Some(token.session()) || live.statement.is_some() {
            return Err(QueryControlError::StaleSession);
        }
        if live.registration.0.cancellation.get().is_some() {
            return Err(QueryControlError::ClientQueryCancelled);
        }
        live.statement = Some(token);
        Ok(())
    }

    pub(super) fn seal(&mut self, query: &ClientQueryId, statement: StatementToken) {
        if let Some(live) = self.active.get_mut(query)
            && live.statement == Some(statement)
        {
            live.success_sealed = true;
        }
    }
}

impl ClientQueryLifecyclePort for QueryApplicationControl {
    fn register_client_query(
        &self,
        id: ClientQueryId,
        principal: Arc<str>,
        source: Option<Arc<str>>,
        text: Option<&ClientQueryText>,
        time: ClientQueryTime,
    ) -> Result<ClientQueryRegistration, QueryControlError> {
        let mut state = self.lock();
        if state.by_client_query.contains_key(&id)
            || state.client_queries.reserve_unclaimed_identity(&id)
        {
            return Err(QueryControlError::ClientQueryIdInUse);
        }
        let queries = &mut state.client_queries;
        let registration = ClientQueryRegistration::new(&queries.brand, id.clone());
        let live = LiveQuery {
            registration: registration.clone(),
            id: ClientQueryText::new(id.as_str().to_owned()),
            user: ClientQueryText::new(principal.to_string()),
            source: source.map(|source| ClientQueryText::new(source.to_string())),
            text: text.map(|text| Arc::downgrade(&text.0)),
            created: time,
            claimed: None,
            session: None,
            statement: None,
            success_sealed: false,
            last_heartbeat: Some(time),
            events: EventTimes::default(),
        };
        queries.active.insert(id, live);
        Ok(registration)
    }

    fn withdraw_client_query(&self, registration: &ClientQueryRegistration) -> bool {
        let mut state = self.lock();
        let queries = &mut state.client_queries;
        if queries.live_for(registration).is_some_and(|query| {
            query.claimed.is_none() && query.registration.0.cancellation.get().is_none()
        }) {
            queries.active.remove(registration.id());
            true
        } else {
            false
        }
    }

    fn claim_client_query(
        &self,
        registration: &ClientQueryRegistration,
        time: ClientQueryTime,
    ) -> ClientQueryStart {
        let mut state = self.lock();
        let Some(query) = state.client_queries.live_for(registration) else {
            return ClientQueryStart::Unavailable;
        };
        if registration.0.cancellation.get().is_some() {
            return ClientQueryStart::Cancelled;
        }
        if query.claimed.is_some() {
            return ClientQueryStart::Unavailable;
        }
        query.claimed = Some(time);
        ClientQueryStart::Claimed(ClientQueryClaim(registration.clone()))
    }

    fn register_claimed_session(
        &self,
        identity: SessionIdentity,
        claim: ClientQueryClaim,
    ) -> Result<SessionToken, QueryControlError> {
        self.register_client_session(identity, Some(claim))
    }

    fn touch_client_query(&self, registration: &ClientQueryRegistration, time: ClientQueryTime) {
        let mut state = self.lock();
        if let Some(query) = state.client_queries.live_for(registration)
            && query
                .last_heartbeat
                .is_none_or(|previous| previous.monotonic <= time.monotonic)
        {
            query.last_heartbeat = Some(time);
        }
    }

    fn request_unstarted_client_cancel(
        &self,
        registration: &ClientQueryRegistration,
        reason: QueryCancellationReason,
    ) -> bool {
        let mut state = self.lock();
        let Some(query) = state.client_queries.live_for(registration) else {
            return false;
        };
        if query.statement.is_some()
            || registration.0.cancellation.get().is_some()
            || protocol_cancellation_error(&reason).is_none()
        {
            return false;
        }
        registration.publish_cancellation(ClientQueryCancellation::Protocol(reason));
        true
    }

    fn client_query_cancellation(
        &self,
        registration: &ClientQueryRegistration,
    ) -> Option<ClientQueryCancellation> {
        // Acquire the same authority after a Work waiter wakes. A winning Work
        // cancellation and the complete management receipt linearize here.
        let state = self.lock();
        if state.client_queries.belongs_to_owner(registration) {
            registration.0.cancellation.get().cloned()
        } else {
            None
        }
    }

    fn settle_client_query(
        &self,
        registration: &ClientQueryRegistration,
        proposed: ClientQueryConclusion,
        time: ClientQueryTime,
    ) -> Option<ClientQueryConclusion> {
        let mut state = self.lock();
        let queries = &mut state.client_queries;
        if !queries.belongs_to_owner(registration) {
            return None;
        }
        if let Some(original) = registration.0.conclusion.get() {
            return Some(original.clone());
        }
        queries.live_for(registration)?;

        let conclusion = match registration.0.cancellation.get() {
            Some(ClientQueryCancellation::Management(receipt)) => {
                ClientQueryConclusion::AdministrativelyKilled(Arc::clone(receipt))
            }
            Some(ClientQueryCancellation::Protocol(reason)) => ClientQueryConclusion::Failed(
                protocol_cancellation_error(reason)
                    .expect("original unstarted protocol cancellation"),
            ),
            _ if matches!(proposed, ClientQueryConclusion::AdministrativelyKilled(_)) => {
                return None;
            }
            _ => proposed,
        };
        let live = queries
            .active
            .remove(registration.id())
            .expect("verified original registration");
        registration
            .0
            .conclusion
            .set(conclusion.clone())
            .expect("single protocol conclusion");
        queries.history.retain(live, conclusion.failure(), time);
        Some(conclusion)
    }

    fn record_client_query_event(
        &self,
        statement: StatementToken,
        event: ClientQueryEvent,
        time: ClientQueryTime,
    ) -> bool {
        let mut state = self.lock();
        let state = &mut *state;
        let Some(session) = state.sessions.get(&statement.session().key()) else {
            return false;
        };
        if session
            .active
            .as_ref()
            .is_none_or(|active| active.generation != statement.generation())
        {
            return false;
        }
        let crate::session_control::SessionOrigin::ClientStatement(query) = &session.origin;
        let Some(query) = state.client_queries.active.get_mut(query) else {
            return false;
        };
        query.statement == Some(statement) && query.events.record(event, time)
    }

    fn kill_client_query(
        &self,
        requester: SessionToken,
        target: &str,
        message: Option<&str>,
        now: ClientQueryTime,
    ) -> Result<Arc<ManagementCancellationReceipt>, KillClientQueryError> {
        if target.is_empty()
            || !target
                .bytes()
                .all(|byte| byte == b'_' || byte.is_ascii_lowercase() || byte.is_ascii_digit())
        {
            return Err(KillClientQueryError::InvalidId);
        }
        let state = self.lock();
        let Some(requester_entry) = state.sessions.get(&requester.key()) else {
            return Err(KillClientQueryError::UnknownRequester);
        };
        if requester_entry.unregister_pending || requester_entry.active.is_none() {
            return Err(KillClientQueryError::UnknownRequester);
        }
        let user = Arc::clone(&requester_entry.principal);
        let queries = &state.client_queries;
        let Some(live) = queries.active.get(target) else {
            let historical = queries
                .history
                .get_live(target, now.monotonic)
                .ok_or(KillClientQueryError::NotFound)?;
            return if historical.user.as_str() == user.as_ref() {
                Err(KillClientQueryError::NotRunning)
            } else {
                Err(KillClientQueryError::PermissionDenied)
            };
        };
        if live.user.as_str() != user.as_ref() {
            return Err(KillClientQueryError::PermissionDenied);
        }
        if live.success_sealed || live.registration.0.cancellation.get().is_some() {
            return Err(KillClientQueryError::NotRunning);
        }
        let cut = if let Some(statement) = live.statement {
            let active = state
                .sessions
                .get(&statement.session().key())
                .and_then(|session| session.active.as_ref())
                .filter(|active| {
                    active.generation == statement.generation() && !active.success_visibility_sealed
                })
                .ok_or(KillClientQueryError::NotRunning)?;
            if !matches!(
                active.cancellation,
                ActiveStatementCancellation::Governed(_)
            ) {
                return Err(KillClientQueryError::NotRunning);
            }
            match active
                .cancellation
                .request(QueryCancellationReason::ExplicitKill {
                    requester_session: requester.key(),
                }) {
                QueryCancelOutcome::Requested => {}
                QueryCancelOutcome::Failed(error) => return Err(KillClientQueryError::Work(error)),
                _ => return Err(KillClientQueryError::NotRunning),
            }
            ManagementCancellationCut::Statement(statement)
        } else if live.claimed.is_some() {
            ManagementCancellationCut::Claimed
        } else {
            ManagementCancellationCut::Registered
        };
        let receipt = Arc::new(ManagementCancellationReceipt {
            requester,
            user,
            query: live.registration.id().clone(),
            message: message.map(Box::from),
            cut,
        });
        live.registration
            .publish_cancellation(ClientQueryCancellation::Management(Arc::clone(&receipt)));
        Ok(receipt)
    }

    fn observe_client_query(
        &self,
        principal: &str,
        id: &str,
        columns: ClientQueryColumns,
        now: ClientQueryTime,
    ) -> Option<ClientQueryObservation> {
        let state = self.lock();
        let queries = &state.client_queries;
        if let Some(live) = queries.active.get(id) {
            return (live.created.monotonic <= now.monotonic && live.user.as_str() == principal)
                .then(|| live.observe(columns, now));
        }
        queries
            .history
            .get_live(id, now.monotonic)
            .filter(|row| row.user.as_str() == principal)
            .map(|row| row.observe(columns))
    }

    fn next_client_query(
        &self,
        principal: &str,
        after: Option<&str>,
        columns: ClientQueryColumns,
        now: ClientQueryTime,
    ) -> Option<ClientQueryObservation> {
        let state = self.lock();
        let queries = &state.client_queries;
        let range = (
            after.map_or(Bound::Unbounded, Bound::Excluded),
            Bound::Unbounded,
        );
        let live = queries.active.range::<str, _>(range).find(|(_, query)| {
            query.created.monotonic <= now.monotonic && query.user.as_str() == principal
        });
        let recent = queries.history.next(principal, after, now.monotonic);
        match (live, recent) {
            (Some((key, live)), Some(row)) if key.as_str() < row.id.as_str() => {
                Some(live.observe(columns, now))
            }
            (_, Some(row)) => Some(row.observe(columns)),
            (Some((_, live)), None) => Some(live.observe(columns, now)),
            _ => None,
        }
    }

    fn subscribe_client_query_history(&self) -> tokio::sync::watch::Receiver<Option<Instant>> {
        self.lock().client_queries.history.subscribe()
    }

    fn client_query_history_status(&self) -> ClientQueryHistoryStatus {
        self.lock().client_queries.history.status()
    }

    fn evict_client_query_history(&self, now: Instant) -> ClientQueryHistoryStatus {
        let mut state = self.lock();
        state.client_queries.history.evict_expired(now);
        state.client_queries.history.status()
    }
}

#[cfg(test)]
mod tests;
