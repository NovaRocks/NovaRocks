// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Process-owned logical-execution runtime registration.
//!
//! Reservation precedes actor spawn, and installation cannot fail after spawn.
//! The registry retains both actor ingress and its join handle until a real
//! join produces the exact move-only receipt required for retirement.

pub(super) mod health;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Poll;
use std::time::Instant;

use super::original_exit::{OriginalAssociation, OriginalLogicalExit};
use novarocks_execution_contract::QueryContextRef;
use novarocks_types::identity::{QueryExecutionId, QueryId};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};

use crate::coordination::{
    AttemptInstantiationPermit, LogicalExecutionActor, LogicalExecutionActorConfig,
    LogicalExecutionActorError, LogicalExecutionOutputTransfer, RegistryClose,
    RegistryContextConvergence, assert_shutdown_complete, spawn_logical_execution_actor,
};
use novarocks_coordination_contract::LogicalExecutionActorId;

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

pub struct LogicalExecutionRuntimeRegistry {
    handle: LogicalExecutionRuntimeRegistryHandle,
    shutdown_complete: bool,
    #[cfg(test)]
    injected_shutdown_error: Option<LogicalExecutionRuntimeRegistryError>,
}

#[derive(Clone)]
pub struct LogicalExecutionRuntimeRegistryHandle {
    inner: Arc<RegistryInner>,
}

impl fmt::Debug for LogicalExecutionRuntimeRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionRuntimeRegistry")
            .field("registry_id", &self.handle.inner.id)
            .field("entries", &self.handle.lock().entries.len())
            .finish()
    }
}

impl fmt::Debug for LogicalExecutionRuntimeRegistryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionRuntimeRegistryHandle")
            .field("registry_id", &self.inner.id)
            .finish_non_exhaustive()
    }
}

impl std::ops::Deref for LogicalExecutionRuntimeRegistry {
    type Target = LogicalExecutionRuntimeRegistryHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for LogicalExecutionRuntimeRegistry {
    fn drop(&mut self) {
        assert_shutdown_complete(
            self.shutdown_complete,
            "logical execution runtime registry owner",
        );
    }
}

struct RegistryInner {
    id: u64,
    runtime: Handle,
    state: Mutex<RegistryState>,
    progress: Arc<Notify>,
    original_exit_ready: AtomicBool,
    health: health::LogicalExecutionHealth,
    active_native: super::NativeActiveCustodyRegistry,
}

struct RegistryState {
    accepting: bool,
    next_generation: u64,
    entries: BTreeMap<QueryId, RegistryEntry>,
}

impl Default for RegistryState {
    fn default() -> Self {
        Self {
            accepting: true,
            next_generation: 0,
            entries: BTreeMap::new(),
        }
    }
}

/// Inline facts for one actual original registry record, not a map quote.
/// Installed and Joined are alternate phases of this same enum. The health
/// field is diagnostic detail already included in record_layout(), not an
/// additional allocation or an amount to add to that record a second time.
/// Independent consumer/cause pointees and BTree backing are not quoted here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalExecutionRegistryEntryGeometry {
    record: std::alloc::Layout,
    internal_health_field: std::alloc::Layout,
    actor_join_backing: std::alloc::Layout,
    actor_join_failure: std::alloc::Layout,
    actor_join_wake: std::alloc::Layout,
    close_wait_frame: std::alloc::Layout,
    native_active_slot: std::alloc::Layout,
    native_consumer_task_frame: std::alloc::Layout,
    native_tail_wake: std::alloc::Layout,
}
impl LogicalExecutionRegistryEntryGeometry {
    pub const fn record_layout(self) -> std::alloc::Layout {
        self.record
    }
    pub const fn internal_health_field_layout(self) -> std::alloc::Layout {
        self.internal_health_field
    }
    pub const fn actor_join_backing_layout(self) -> std::alloc::Layout {
        self.actor_join_backing
    }
    pub const fn actor_join_failure_layout(self) -> std::alloc::Layout {
        self.actor_join_failure
    }
    pub const fn actor_join_wake_layout(self) -> std::alloc::Layout {
        self.actor_join_wake
    }
    pub const fn close_wait_frame_layout(self) -> std::alloc::Layout {
        self.close_wait_frame
    }
    pub const fn native_consumer_task_frame_layout(self) -> std::alloc::Layout {
        self.native_consumer_task_frame
    }
    pub const fn native_tail_wake_layout(self) -> std::alloc::Layout {
        self.native_tail_wake
    }
    pub const fn native_active_slot_layout(self) -> std::alloc::Layout {
        self.native_active_slot
    }

    /// The caller supplies its actual bounded original entry inventory.
    /// This neither establishes that bound nor pays for map nodes/pointees.
    pub fn inline_bytes_for_entries(
        self,
        entries: usize,
    ) -> Result<usize, novarocks_type_contract::owned_resources::layout::LayoutResourceError> {
        self.record.size().checked_mul(entries).ok_or(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    }
}
impl LogicalExecutionRuntimeRegistry {
    /// No registry, actor, consumer or allocation is created by this getter.
    pub fn entry_geometry() -> LogicalExecutionRegistryEntryGeometry {
        LogicalExecutionRegistryEntryGeometry {
            record: std::alloc::Layout::new::<RegistryEntry>(),
            internal_health_field: std::alloc::Layout::new::<Option<InternalConsumerHealth>>(),
            actor_join_backing: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<OriginalActorJoin>(),
            )
            .expect("fixed original join layout"),
            actor_join_wake: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<OriginalJoinWake>(),
            )
            .expect("fixed original join wake layout"),
            actor_join_failure: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<JoinError>(),
            )
            .expect("fixed original join failure layout"),
            close_wait_frame: original_close_wait_layout(original_close_wait),
            native_active_slot: super::NativeActiveCustodyRegistry::slot_layout(),
            native_consumer_task_frame:
                super::NativeActiveCustodyRegistry::consumer_task_frame_layout(),
            native_tail_wake: super::NativeActiveCustodyRegistry::tail_wake_layout(),
        }
    }
}

// Original actor join custody. The synchronous lease moves the SAME handle
// only while polling outside both mutexes; Pending/unwind restores it.
struct OriginalActorJoin {
    state: Mutex<OriginalActorJoinState>,
    wake: Arc<OriginalJoinWake>,
}
struct OriginalJoinWake {
    registry: std::sync::Weak<RegistryInner>,
    forward: futures::task::AtomicWaker,
}
impl std::task::Wake for OriginalJoinWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(registry) = self.registry.upgrade() {
            registry.original_exit_ready.store(true, Ordering::Release);
            registry.progress.notify_waiters();
        }
        self.forward.wake();
    }
}
#[derive(Default)]
pub(super) struct OriginalExitCursor {
    key: Option<QueryId>,
    active: bool,
}
struct OriginalActorJoinState {
    handle: Option<JoinHandle<()>>,
    completed: bool,
    unwound: bool,
    waiting: bool,
}
struct OriginalActorJoinLease {
    owner: Arc<OriginalActorJoin>,
    handle: Option<JoinHandle<()>>,
    completed: bool,
}
impl OriginalActorJoin {
    fn new(handle: JoinHandle<()>, registry: std::sync::Weak<RegistryInner>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(OriginalActorJoinState {
                handle: Some(handle),
                completed: false,
                unwound: false,
                waiting: false,
            }),
            wake: Arc::new(OriginalJoinWake {
                registry,
                forward: futures::task::AtomicWaker::new(),
            }),
        })
    }
    fn is_finished(&self) -> bool {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.completed || s.handle.as_ref().is_some_and(JoinHandle::is_finished)
    }
    #[cfg(test)]
    fn abort(self: &Arc<Self>) {
        let handle = {
            self.state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .handle
                .take()
        };
        if let Some(handle) = handle {
            let lease = OriginalActorJoinLease {
                owner: Arc::clone(self),
                handle: Some(handle),
                completed: false,
            };
            lease.handle.as_ref().unwrap().abort();
            drop(lease);
        }
    }
    fn poll(
        self: &Arc<Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<(Result<(), JoinError>, OriginalActorJoinLease)> {
        self.wake.forward.register(cx.waker());
        let handle = {
            let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if s.completed || s.unwound {
                return Poll::Pending;
            }
            let Some(handle) = s.handle.take() else {
                s.waiting = true;
                return Poll::Pending;
            };
            handle
        };
        let mut lease = OriginalActorJoinLease {
            owner: Arc::clone(self),
            handle: Some(handle),
            completed: false,
        };
        let wake = std::task::Waker::from(Arc::clone(&self.wake));
        let mut join_context = std::task::Context::from_waker(&wake);
        match Pin::new(lease.handle.as_mut().unwrap()).poll(&mut join_context) {
            Poll::Pending => {
                drop(lease);
                Poll::Pending
            }
            Poll::Ready(result) => {
                lease.completed = true;
                Poll::Ready((result, lease))
            }
        }
    }
}
impl Drop for OriginalActorJoinLease {
    fn drop(&mut self) {
        let wake = {
            let mut s = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
            if self.completed {
                s.completed = true;
            } else {
                assert!(s.handle.is_none());
                s.handle = self.handle.take();
                if std::thread::panicking() {
                    s.unwound = true;
                }
            }
            std::mem::take(&mut s.waiting)
        };
        // Completed handle destruction is AFTER stable original result install.
        drop(self.handle.take());
        if wake && !std::thread::panicking() {
            self.owner.wake.forward.wake();
        }
    }
}
fn original_close_wait_layout<F: Future<Output = ()>>(
    _: fn(tokio::sync::watch::Receiver<bool>) -> F,
) -> std::alloc::Layout {
    std::alloc::Layout::new::<F>()
}
async fn original_close_wait(mut closed: tokio::sync::watch::Receiver<bool>) {
    while !*closed.borrow_and_update() {
        if closed.changed().await.is_err() {
            break;
        }
    }
}

enum RegistryEntry {
    Reserved(ReservedEntry),
    Installed(InstalledEntry),
    Joined(JoinedEntry),
}

struct ReservedEntry {
    generation: u64,
    initial_execution: QueryExecutionId,
    contexts: BTreeSet<QueryContextRef>,
    original_exit: Option<Arc<OriginalLogicalExit>>,
    spawn_started: bool,
}

struct InstalledEntry {
    generation: u64,
    initial_execution: QueryExecutionId,
    actor: LogicalExecutionActor,
    join: Arc<OriginalActorJoin>,
    original_exit: Option<Arc<OriginalLogicalExit>>,
    original_close: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ingress_closed: bool,
    internal_health: Option<InternalConsumerHealth>,
}

// The original registration retains its same consumer and complete failure
// packet. This fixed field is not a second record or a new task/identity.
struct InternalConsumerHealth {
    consumer: Arc<dyn crate::api::InternalComputationConsumer>,
    failure: crate::api::StatementNativeHealthFailure,
}
struct JoinedEntry {
    generation: u64,
    initial_execution: QueryExecutionId,
    actor_id: LogicalExecutionActorId,
    outcome: LogicalExecutionJoinOutcome,
    original_result: Result<(), Arc<JoinError>>,
    original_exit: Option<Arc<OriginalLogicalExit>>,
    internal_health: Option<InternalConsumerHealth>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalExecutionRuntimeRegistryError {
    AlreadyRegistered,
    RegistryClosing,
    RegistryNotEmpty,
    UnknownRegistration,
    ForeignRegistry,
    StaleGeneration,
    InvalidContextSet,
    ContextNotRegistered,
    ConfigDoesNotMatchReservation,
    Actor(LogicalExecutionActorError),
    JoinNotReady,
    JoinAlreadyCompleted,
    InternalConsumerHealthUnresolved,
    OriginalExitFailureUnresolved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalExecutionRuntimeShutdownError {
    DeadlineExceeded { remaining_entries: usize },
    Registry(LogicalExecutionRuntimeRegistryError),
}

impl fmt::Display for LogicalExecutionRuntimeShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeadlineExceeded { remaining_entries } => write!(
                formatter,
                "logical execution runtime shutdown deadline exceeded with {remaining_entries} entries remaining"
            ),
            Self::Registry(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for LogicalExecutionRuntimeShutdownError {}

impl From<LogicalExecutionRuntimeRegistryError> for LogicalExecutionRuntimeShutdownError {
    fn from(value: LogicalExecutionRuntimeRegistryError) -> Self {
        Self::Registry(value)
    }
}

impl fmt::Display for LogicalExecutionRuntimeRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Actor(error) => write!(formatter, "logical execution actor failed: {error}"),
            other => write!(
                formatter,
                "logical execution runtime registry error: {other:?}"
            ),
        }
    }
}

impl std::error::Error for LogicalExecutionRuntimeRegistryError {}

impl novarocks_execution_contract::failure::FailureProjection
    for LogicalExecutionRuntimeRegistryError
{
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureProjection, FailureReason, domain,
        };
        let (class, code, observed_stop) = match self {
            Self::AlreadyRegistered => (FailureClass::Invariant, 61, false),
            Self::RegistryClosing => (FailureClass::Control, 62, true),
            Self::RegistryNotEmpty => (FailureClass::Control, 63, false),
            Self::UnknownRegistration => (FailureClass::Control, 64, true),
            Self::ForeignRegistry => (FailureClass::Invariant, 65, false),
            Self::StaleGeneration => (FailureClass::Control, 66, true),
            Self::InvalidContextSet => (FailureClass::Invariant, 67, false),
            Self::ContextNotRegistered => (FailureClass::Invariant, 68, false),
            Self::ConfigDoesNotMatchReservation => (FailureClass::Invariant, 69, false),
            Self::JoinNotReady => (FailureClass::Control, 70, false),
            Self::JoinAlreadyCompleted => (FailureClass::Control, 71, true),
            Self::InternalConsumerHealthUnresolved => {
                return crate::execution_error::QueryFailureReason::InternalConsumerPanicked
                    .failure_fact();
            }
            Self::OriginalExitFailureUnresolved => {
                return crate::execution_error::QueryFailureReason::LogicalFailureUnspecified
                    .failure_fact();
            }
            Self::Actor(error) => return FailureProjection::failure_fact(error),
        };
        let reason = FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap());
        if observed_stop {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}

impl LogicalExecutionRuntimeRegistryError {
    /// Bounded diagnostic authored here; no foreign Display becomes safe detail.
    pub fn safe_detail(&self) -> novarocks_execution_contract::SafeDetail {
        let message = match self {
            Self::AlreadyRegistered => "logical query already has a runtime registration",
            Self::RegistryClosing => "logical execution runtime registry is closing",
            Self::RegistryNotEmpty => "logical execution runtime still has registered work",
            Self::UnknownRegistration => {
                "logical execution runtime registration is no longer present"
            }
            Self::ForeignRegistry => {
                "logical execution registration belongs to a different registry"
            }
            Self::StaleGeneration => {
                "logical execution registration generation is no longer current"
            }
            Self::InvalidContextSet => {
                "runtime reservation has duplicate or foreign execution contexts"
            }
            Self::ContextNotRegistered => "context convergence names an unregistered query context",
            Self::ConfigDoesNotMatchReservation => {
                "actor configuration does not match its runtime reservation"
            }
            Self::JoinNotReady => "logical execution runtime join is not ready",
            Self::JoinAlreadyCompleted => "logical execution runtime join already completed",
            Self::InternalConsumerHealthUnresolved => {
                "logical execution retains an unresolved internal consumer failure"
            }
            Self::OriginalExitFailureUnresolved => {
                "logical execution retains its original unhandled exit cause"
            }
            Self::Actor(error) => return error.safe_detail(),
        };
        novarocks_execution_contract::SafeDetail::truncating(message)
    }
}

impl From<LogicalExecutionActorError> for LogicalExecutionRuntimeRegistryError {
    fn from(value: LogicalExecutionActorError) -> Self {
        Self::Actor(value)
    }
}

pub(crate) struct LogicalExecutionRuntimeReservation {
    registry: LogicalExecutionRuntimeRegistryHandle,
    initial_execution: QueryExecutionId,
    generation: u64,
    active: bool,
}

impl fmt::Debug for LogicalExecutionRuntimeReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogicalExecutionRuntimeReservation")
            .field("registry_id", &self.registry.inner.id)
            .field("initial_execution", &self.initial_execution)
            .field("generation", &self.generation)
            .finish()
    }
}

impl Drop for LogicalExecutionRuntimeReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let query = self.initial_execution.query_id();
        let mut state = self.registry.lock();
        let removable = matches!(state.entries.get(&query),
            Some(RegistryEntry::Reserved(entry)) if entry.generation == self.generation
                && !(entry.spawn_started && entry.original_exit.is_some()));
        let removed = if removable {
            state.entries.remove(&query)
        } else {
            None
        };
        if let Some(RegistryEntry::Reserved(entry)) = &removed
            && let Some(source) = &entry.original_exit
        {
            source.unreserve_silent(OriginalAssociation {
                registry: self.registry.inner.id,
                initial: self.initial_execution,
                generation: self.generation,
            });
        }
        drop(state);
        if let Some(RegistryEntry::Reserved(entry)) = &removed
            && let Some(source) = &entry.original_exit
        {
            source.notify();
        }
        drop(removed);
        self.registry.inner.progress.notify_one();
    }
}

#[must_use = "the installed logical execution must be handed to its runtime consumer"]
pub(crate) struct InstalledLogicalExecution {
    registration: LogicalExecutionRegistration,
    initial_attempt: AttemptInstantiationPermit,
    output: LogicalExecutionOutputTransfer,
}

impl InstalledLogicalExecution {
    pub(crate) fn into_parts(
        self,
    ) -> (
        LogicalExecutionRegistration,
        AttemptInstantiationPermit,
        LogicalExecutionOutputTransfer,
    ) {
        (self.registration, self.initial_attempt, self.output)
    }
}

#[derive(Debug)]
pub(crate) struct LogicalExecutionRegistration {
    registry_id: u64,
    initial_execution: QueryExecutionId,
    generation: u64,
}

impl LogicalExecutionRegistration {
    pub const fn initial_execution(&self) -> QueryExecutionId {
        self.initial_execution
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalExecutionJoinReadiness {
    WaitingForConvergence,
    Ready,
}

#[derive(Debug)]
#[must_use = "an observed actor join must be consumed by exact registry retirement"]
pub(crate) struct LogicalExecutionJoinReceipt {
    registry_id: u64,
    initial_execution: QueryExecutionId,
    generation: u64,
    actor_id: LogicalExecutionActorId,
    outcome: LogicalExecutionJoinOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalExecutionJoinOutcome {
    Completed,
    Cancelled,
    Panicked,
}

impl LogicalExecutionJoinReceipt {
    #[cfg(test)]
    pub const fn outcome(&self) -> LogicalExecutionJoinOutcome {
        self.outcome
    }
}

impl LogicalExecutionRuntimeRegistry {
    pub fn new(runtime: Handle) -> Self {
        let id = NEXT_REGISTRY_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .expect("logical execution runtime registry identity exhausted");
        let health_changed = Arc::new(Notify::new());
        let health = health::LogicalExecutionHealth::new(Arc::clone(&health_changed));
        let progress = Arc::new(Notify::new());
        Self {
            handle: LogicalExecutionRuntimeRegistryHandle {
                inner: Arc::new(RegistryInner {
                    id,
                    runtime,
                    state: Mutex::new(RegistryState::default()),
                    progress: Arc::clone(&progress),
                    original_exit_ready: AtomicBool::new(false),
                    active_native: super::NativeActiveCustodyRegistry::new(
                        health.clone(),
                        progress,
                    ),
                    health,
                }),
            },
            shutdown_complete: false,
            #[cfg(test)]
            injected_shutdown_error: None,
        }
    }

    pub fn handle(&self) -> LogicalExecutionRuntimeRegistryHandle {
        self.handle.clone()
    }

    /// Closes reservation admission and drives every installed actor through
    /// its normal convergence, join, and exact retirement path until the
    /// absolute deadline.
    ///
    /// This method borrows the unique owner so cancelling the wait or reaching
    /// the deadline cannot detach an actor task or lose its join handle. On an
    /// error the caller must retain this owner and may retry with a later
    /// deadline after additional context-convergence facts arrive.
    pub async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), LogicalExecutionRuntimeShutdownError> {
        self.handle.begin_shutdown();
        if self.shutdown_complete {
            return Ok(());
        }

        // This broadcast is synchronous and independent of actor mailbox
        // capacity, so even an already-expired deadline records shutdown on
        // every actor that was installed when shutdown began.
        self.handle.request_close_for_installed();
        #[cfg(test)]
        if let Some(error) = self.injected_shutdown_error.take() {
            if self.handle.entry_count() == 0 {
                self.shutdown_complete = true;
            }
            return Err(error.into());
        }
        let drain = self.handle.drain_actors_for_shutdown();
        match tokio::time::timeout_at(deadline.into(), drain).await {
            Ok(Ok(())) => {
                debug_assert!(self.handle.lock().entries.is_empty());
                self.shutdown_complete = true;
                Ok(())
            }
            Ok(Err(error)) => {
                // An invariant failure after the Registry has already retired
                // every entry is a terminal cleanup diagnostic, not evidence
                // that runtime ownership remains live. Remember convergence
                // before returning the diagnostic so the process owner can
                // accumulate it and continue releasing later owners. When an
                // entry remains, retain this exact owner for a later retry.
                if self.handle.entry_count() == 0 {
                    self.shutdown_complete = true;
                }
                Err(error.into())
            }
            Err(_) => Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded {
                remaining_entries: self.handle.entry_count(),
            }),
        }
    }

    /// Explicitly releases process-local supervision when the owning FE
    /// process is committed to exit after bounded graceful shutdown failed.
    ///
    /// This is deliberately separate from ordinary shutdown: dropping actor
    /// joins is sound only because no process-local execution can survive the
    /// process boundary. Callers that continue serving must retain this owner
    /// and retry [`Self::shutdown_until`] instead.
    pub fn abandon_for_process_exit(&mut self) {
        self.handle.begin_shutdown();
        self.handle.request_close_for_installed();
        // Detach the original health and entries at one cut. External Drops
        // may publish later health, so no destructor runs under this lock.
        let (entries, health) = {
            let mut state = self.handle.lock();
            (
                std::mem::take(&mut state.entries),
                self.handle.inner.health.take_for_process_exit(),
            )
        };
        drop(entries);
        self.handle.inner.active_native.abandon_for_process_exit();
        drop(health);
        self.handle.inner.progress.notify_waiters();
        self.shutdown_complete = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_shutdown_error_once(
        &mut self,
        error: LogicalExecutionRuntimeRegistryError,
    ) {
        assert!(self.injected_shutdown_error.replace(error).is_none());
    }

    /// Closes reservation admission and succeeds only after every installed
    /// runtime has produced and retired its exact join receipt.
    pub fn shutdown(
        mut self,
    ) -> Result<
        (),
        (
            LogicalExecutionRuntimeRegistryError,
            LogicalExecutionRuntimeRegistry,
        ),
    > {
        self.handle.begin_shutdown();
        if self.handle.entry_count() != 0 {
            return Err((LogicalExecutionRuntimeRegistryError::RegistryNotEmpty, self));
        }
        self.shutdown_complete = true;
        Ok(())
    }
}

impl LogicalExecutionRuntimeRegistryHandle {
    fn begin_shutdown(&self) {
        self.lock().accepting = false;
    }

    pub(crate) async fn process_health_changed(&self) {
        self.inner.health.changed().await;
    }

    pub(crate) fn progress_root_ack_tails(&self) {
        self.inner.active_native.poll_root_ack_tails();
    }

    pub(crate) async fn root_ack_tail_changed(&self) {
        self.inner.active_native.root_ack_tail_changed().await;
    }

    pub(crate) fn root_ack_tail_count(&self) -> usize {
        self.inner.active_native.root_ack_tail_count()
    }

    pub(crate) fn process_health(&self) -> Option<crate::api::QueryExecutionError> {
        use novarocks_execution_contract::failure::FailureProjection as _;
        self.inner.health.health().or_else(|| {
            let state = self.lock();
            let retained = state.entries.values().any(|entry| match entry {
                RegistryEntry::Installed(entry) => entry.internal_health.is_some(),
                RegistryEntry::Joined(entry) => entry.internal_health.is_some(),
                RegistryEntry::Reserved(_) => false,
            });
            retained.then(|| {
                crate::api::QueryExecutionError::from_execution(
                    crate::execution_error::ExecutionFailureInput::new(
                        LogicalExecutionRuntimeRegistryError::InternalConsumerHealthUnresolved
                            .failure_fact(),
                        novarocks_execution_contract::SafeDetail::truncating(
                            "Original Internal CPU failure remains in custody",
                        ),
                        crate::execution_error::ExecutionFailureSite::Coordinator,
                    ),
                )
            })
        })
    }

    pub(crate) async fn wait_registration_consumers(
        &self,
        registration: &LogicalExecutionRegistration,
    ) {
        loop {
            let changed = self.inner.progress.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .inner
                .active_native
                .consumers_pending(registration.initial_execution, registration.generation)
            {
                return;
            }
            changed.await;
        }
    }

    /// Preserve the same NoRecovery consumer in its exact original record.
    /// All receiver inputs are borrowed until the empty stable field is known.
    pub(crate) fn retain_internal_consumer_health(
        &self,
        registration: &LogicalExecutionRegistration,
        consumer: &Arc<dyn crate::api::InternalComputationConsumer>,
        failure: &crate::api::StatementNativeHealthFailure,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        let mut state = self.lock();
        let target = match state
            .entries
            .get_mut(&registration.initial_execution.query_id())
        {
            Some(RegistryEntry::Installed(entry))
                if entry.generation == registration.generation
                    && entry.initial_execution == registration.initial_execution =>
            {
                &mut entry.internal_health
            }
            Some(RegistryEntry::Joined(entry))
                if entry.generation == registration.generation
                    && entry.initial_execution == registration.initial_execution =>
            {
                &mut entry.internal_health
            }
            Some(_) => return Err(LogicalExecutionRuntimeRegistryError::StaleGeneration),
            None => return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
        };
        if let Some(original) = target {
            if !Arc::ptr_eq(&original.consumer, consumer)
                || !std::ptr::eq(original.failure.cause(), failure.cause())
            {
                return Err(LogicalExecutionRuntimeRegistryError::InternalConsumerHealthUnresolved);
            }
        } else {
            *target = Some(InternalConsumerHealth {
                consumer: Arc::clone(consumer),
                failure: failure.clone(),
            });
        }
        state.accepting = false;
        drop(state);
        self.inner.progress.notify_one();
        Ok(())
    }

    /// Reserve the exact original registration/attempt custody before Native
    /// activation. This grants no new work, actor, permit, or dispatch authority.
    // Design: ADR-0187 (docs/adr/ADR-0187-receiver-owned-steps-and-single-task-conclusion.md)
    pub(crate) fn reserve_active_native_owner(
        &self,
        registration: &LogicalExecutionRegistration,
        execution: QueryExecutionId,
        contexts: &[QueryContextRef],
    ) -> Result<super::NativeActiveCustodyReservation, LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        // Serialize the original custody insertion with begin_shutdown's
        // admission cut. No Native activation can acquire a slot after close
        // has observed that this exact registration has no retained owner.
        // Lock order is Registry state -> Native custody; neither invokes
        // foreign code or awaits while held.
        let state = self.lock();
        if !state.accepting {
            return Err(LogicalExecutionRuntimeRegistryError::RegistryClosing);
        }
        let Some(RegistryEntry::Installed(entry)) = state
            .entries
            .get(&registration.initial_execution.query_id())
        else {
            return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration);
        };
        validate_generation(entry.generation, registration.generation)?;
        if execution.query_id() != registration.initial_execution.query_id() {
            return Err(LogicalExecutionRuntimeRegistryError::InvalidContextSet);
        }
        let reserved = self.inner.active_native.reserve_with_original_exit(
            super::NativeActiveCustodyKey {
                initial: registration.initial_execution,
                generation: registration.generation,
                execution,
            },
            contexts,
            self.clone(),
            entry
                .original_exit
                .as_ref()
                .map_or_else(std::sync::Weak::new, Arc::downgrade),
        );
        if reserved.is_some()
            && let Some(source) = &entry.original_exit
        {
            source.native_reserved_silent();
        }
        drop(state);
        reserved.ok_or(LogicalExecutionRuntimeRegistryError::InvalidContextSet)
    }

    pub(crate) async fn record_native_custody_closure(
        &self,
        key: super::NativeActiveCustodyKey,
        closure: &crate::api::NativeActiveConvergenceClosure,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        let registration = LogicalExecutionRegistration {
            registry_id: self.inner.id,
            initial_execution: key.initial,
            generation: key.generation,
        };
        let mut first_error = None;
        match closure {
            crate::api::NativeActiveConvergenceClosure::Evidence(
                crate::api::NativeAttemptConvergence::AllWorkersStoppedAndContextsFenced,
            ) => {
                let contexts = self
                    .inner
                    .active_native
                    .contexts(key)
                    .ok_or(LogicalExecutionRuntimeRegistryError::UnknownRegistration)?;
                for context in contexts {
                    if let Err(error) = self
                        .observe_worker_stopped_and_context_fenced(&registration, context)
                        .await
                    {
                        first_error.get_or_insert(error);
                    }
                }
            }
            crate::api::NativeActiveConvergenceClosure::Evidence(
                crate::api::NativeAttemptConvergence::Contexts(facts),
            ) => {
                for fact in facts {
                    let result = match fact.kind() {
                        crate::api::NativeContextConvergenceKind::NeverEstablished => {
                            self.observe_context_never_established(&registration, fact.context())
                                .await
                        }
                        crate::api::NativeContextConvergenceKind::WorkerStoppedAndContextFenced => {
                            self.observe_worker_stopped_and_context_fenced(
                                &registration,
                                fact.context(),
                            )
                            .await
                        }
                        crate::api::NativeContextConvergenceKind::WorkerProcessReplaced => {
                            self.observe_worker_process_replaced(&registration, fact.context())
                                .await
                        }
                    };
                    if let Err(error) = result {
                        first_error.get_or_insert(error);
                    }
                }
            }
            crate::api::NativeActiveConvergenceClosure::RemoteTrackingEnded { exact_contexts } => {
                for &context in exact_contexts {
                    if let Err(error) = self
                        .end_remote_context_tracking(&registration, context)
                        .await
                    {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
        if first_error.is_none() {
            let state = self.lock();
            if let Some(RegistryEntry::Installed(entry)) =
                state.entries.get(&key.initial.query_id())
            {
                validate_generation(entry.generation, key.generation)?;
                if let Some(source) = &entry.original_exit {
                    source.native_closure_silent();
                }
            }
        }
        if first_error.is_none() {
            self.mark_original_exit_ready();
        }
        first_error.map_or(Ok(()), Err)
    }

    async fn wait_active_native_custody(&self, registration: &LogicalExecutionRegistration) {
        loop {
            let changed = self.inner.progress.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .inner
                .active_native
                .blocks_registration(registration.initial_execution, registration.generation)
            {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn active_native_registration_health(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Option<crate::api::QueryExecutionError> {
        self.inner
            .active_native
            .registration_health(registration.initial_execution, registration.generation)
    }

    /// Forward the original whole packet to the existing process health owner.
    /// No field projection, formatting or replacement owner is introduced here.
    pub(super) fn publish_original_exit_health_silent(
        &self,
        source: &Arc<OriginalLogicalExit>,
    ) -> Option<health::LogicalExecutionHealthNotification> {
        if source.health_published() {
            return None;
        }
        let mut custody: Option<Box<dyn std::any::Any + Send>> = Some(Box::new(
            super::original_exit::LogicalExitHealthCustody::new(Arc::clone(source)),
        ));
        let notice = self.inner.health.record_supervision_panic_in(&mut custody);
        if custody.is_none() {
            source.mark_health_published_silent();
        } else {
            let mut rejected = custody
                .take()
                .unwrap()
                .downcast::<super::original_exit::LogicalExitHealthCustody>()
                .unwrap_or_else(|_| unreachable!("original health wrapper type"));
            rejected.disarm();
            drop(rejected);
        }
        notice
    }

    pub(crate) fn record_process_failure(&self, failure: crate::api::StatementNativeHealthFailure) {
        self.inner.health.record_unproven_exit(failure);
    }

    pub(crate) fn record_native_supervision_panic(&self, original: Box<dyn std::any::Any + Send>) {
        self.inner.health.record_supervision_panic(original);
    }

    pub(crate) fn record_native_supervision_join_failure(&self, error: tokio::task::JoinError) {
        if error.is_panic() {
            self.record_native_supervision_panic(error.into_panic());
        } else {
            self.inner.health.record_supervision_join_failure(error);
        }
    }

    #[cfg(test)]
    pub(crate) fn active_native_custody_count(&self) -> usize {
        self.inner.active_native.len()
    }

    fn entry_count(&self) -> usize {
        self.lock().entries.len() + self.inner.active_native.len()
    }

    fn request_close_for_installed(&self) {
        let actors = {
            let state = self.lock();
            state
                .entries
                .values()
                .filter_map(|entry| match entry {
                    RegistryEntry::Installed(entry) => Some((
                        entry.initial_execution,
                        entry.generation,
                        entry.actor.clone(),
                    )),
                    RegistryEntry::Reserved(_) | RegistryEntry::Joined(_) => None,
                })
                .collect::<Vec<_>>()
        };
        for (initial_execution, generation, actor) in actors {
            // Closing actor ingress can complete its original WorkOwner even
            // before its Registry join is polled. The same Native custody must
            // therefore gate the close request itself, not only join().
            if !self
                .inner
                .active_native
                .blocks_registration(initial_execution, generation)
            {
                drop(actor.request_registry_close_for_join(RegistryClose::Shutdown));
            }
        }
    }

    async fn drain_actors_for_shutdown(&self) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        loop {
            let progress = self.inner.progress.notified();
            tokio::pin!(progress);
            progress.as_mut().enable();
            let (registrations, joined) = {
                let state = self.lock();
                if state.entries.is_empty() {
                    return Ok(());
                }
                let joined = state
                    .entries
                    .values()
                    .filter_map(|entry| match entry {
                        RegistryEntry::Joined(entry) => Some(LogicalExecutionJoinReceipt {
                            registry_id: self.inner.id,
                            initial_execution: entry.initial_execution,
                            generation: entry.generation,
                            actor_id: entry.actor_id,
                            outcome: entry.outcome,
                        }),
                        RegistryEntry::Reserved(_) | RegistryEntry::Installed(_) => None,
                    })
                    .collect::<Vec<_>>();
                let registrations = state
                    .entries
                    .values()
                    .filter_map(|entry| match entry {
                        RegistryEntry::Installed(entry) => Some(LogicalExecutionRegistration {
                            registry_id: self.inner.id,
                            initial_execution: entry.initial_execution,
                            generation: entry.generation,
                        }),
                        RegistryEntry::Reserved(_) | RegistryEntry::Joined(_) => None,
                    })
                    .collect::<Vec<_>>();
                (registrations, joined)
            };
            // The root may already have observed the SAME original join but
            // retained its entry after health refused retirement. Reuse that
            // receipt and its original retirement author; waiting for another
            // join notification would otherwise wait forever on a Joined row.
            for receipt in joined {
                self.retire(receipt).map_err(|(error, _)| error)?;
            }
            if registrations.is_empty() && self.entry_count() == 0 {
                return Ok(());
            }

            // The original progress waiter was enabled before the snapshot.
            // A normal custody removal wakes this same loop, including when
            // the initial shutdown broadcast had to skip that registration.
            let registrations = registrations
                .into_iter()
                .filter(|registration| {
                    !self.inner.active_native.blocks_registration(
                        registration.initial_execution,
                        registration.generation,
                    )
                })
                .collect::<Vec<_>>();
            if registrations.is_empty() {
                progress.await;
                continue;
            }

            let mut close_waiters = Vec::with_capacity(registrations.len());
            for registration in registrations {
                let actor = self.actor(&registration)?;
                let closed = actor.request_registry_close_for_join(RegistryClose::Shutdown);
                close_waiters.push((registration, closed));
            }

            for (mut registration, mut closed) in close_waiters {
                while !*closed.borrow_and_update() {
                    if closed.changed().await.is_err() {
                        break;
                    }
                }
                self.record_ingress_closed(&registration)?;
                let receipt = self.join(&mut registration).await?;
                self.retire(receipt).map_err(|(error, _)| error)?;
            }
        }
    }

    fn record_ingress_closed(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        let mut state = self.lock();
        match state
            .entries
            .get_mut(&registration.initial_execution.query_id())
        {
            Some(RegistryEntry::Installed(entry)) => {
                validate_generation(entry.generation, registration.generation)?;
                entry.ingress_closed = true;
                Ok(())
            }
            Some(RegistryEntry::Joined(entry)) => {
                validate_generation(entry.generation, registration.generation)?;
                Err(LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted)
            }
            Some(RegistryEntry::Reserved(entry)) => {
                validate_generation(entry.generation, registration.generation)?;
                Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration)
            }
            None => Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
        }
    }

    pub(crate) fn reserve(
        &self,
        initial_execution: QueryExecutionId,
        frozen_contexts: Vec<QueryContextRef>,
    ) -> Result<LogicalExecutionRuntimeReservation, LogicalExecutionRuntimeRegistryError> {
        self.reserve_with_original_exit(initial_execution, frozen_contexts, None)
    }

    pub(super) fn reserve_with_original_exit(
        &self,
        initial_execution: QueryExecutionId,
        frozen_contexts: Vec<QueryContextRef>,
        original_exit: Option<Arc<OriginalLogicalExit>>,
    ) -> Result<LogicalExecutionRuntimeReservation, LogicalExecutionRuntimeRegistryError> {
        let contexts: BTreeSet<_> = frozen_contexts.iter().copied().collect();
        if contexts.len() != frozen_contexts.len()
            || contexts
                .iter()
                .any(|context| context.query_execution_id() != initial_execution)
        {
            return Err(LogicalExecutionRuntimeRegistryError::InvalidContextSet);
        }
        let query = initial_execution.query_id();
        let mut state = self.lock();
        if !state.accepting {
            return Err(LogicalExecutionRuntimeRegistryError::RegistryClosing);
        }
        if state.entries.contains_key(&query) {
            return Err(LogicalExecutionRuntimeRegistryError::AlreadyRegistered);
        }
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .expect("logical execution runtime registration generation exhausted");
        let generation = state.next_generation;
        if let Some(source) = &original_exit {
            source.reserve_silent(OriginalAssociation {
                registry: self.inner.id,
                initial: initial_execution,
                generation,
            });
        }
        state.entries.insert(
            query,
            RegistryEntry::Reserved(ReservedEntry {
                generation,
                initial_execution,
                contexts,
                original_exit,
                spawn_started: false,
            }),
        );
        Ok(LogicalExecutionRuntimeReservation {
            registry: self.clone(),
            initial_execution,
            generation,
            active: true,
        })
    }

    pub(crate) fn actor(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<LogicalExecutionActor, LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        let state = self.lock();
        match state
            .entries
            .get(&registration.initial_execution.query_id())
        {
            Some(RegistryEntry::Installed(entry)) => {
                validate_generation(entry.generation, registration.generation)?;
                Ok(entry.actor.clone())
            }
            Some(RegistryEntry::Joined(entry)) => {
                validate_generation(entry.generation, registration.generation)?;
                Err(LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted)
            }
            Some(RegistryEntry::Reserved(entry)) => {
                validate_generation(entry.generation, registration.generation)?;
                Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration)
            }
            None => Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
        }
    }

    pub(crate) async fn observe_worker_stopped_and_context_fenced(
        &self,
        registration: &LogicalExecutionRegistration,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.observe_context_convergence(
            registration,
            context,
            RegistryContextConvergence::WorkerStoppedAndContextFenced,
        )
        .await
    }

    pub(crate) async fn observe_context_never_established(
        &self,
        registration: &LogicalExecutionRegistration,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.observe_context_convergence(
            registration,
            context,
            RegistryContextConvergence::NeverEstablished,
        )
        .await
    }

    pub(crate) async fn observe_worker_process_replaced(
        &self,
        registration: &LogicalExecutionRegistration,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.observe_context_convergence(
            registration,
            context,
            RegistryContextConvergence::WorkerProcessReplaced,
        )
        .await
    }

    /// Ends the FE's bounded observation of this context without asserting
    /// that the Worker stopped or released its resources. The actor records a
    /// distinct retirement reason and returns only its governance tracking.
    pub(crate) async fn end_remote_context_tracking(
        &self,
        registration: &LogicalExecutionRegistration,
        context: QueryContextRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.observe_context_convergence(
            registration,
            context,
            RegistryContextConvergence::RemoteTrackingEnded,
        )
        .await
    }

    pub(crate) async fn join_readiness(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<LogicalExecutionJoinReadiness, LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        if self
            .inner
            .active_native
            .blocks_registration(registration.initial_execution, registration.generation)
        {
            return Ok(LogicalExecutionJoinReadiness::WaitingForConvergence);
        }
        {
            let state = self.lock();
            if let Some(RegistryEntry::Installed(entry)) = state
                .entries
                .get(&registration.initial_execution.query_id())
            {
                validate_generation(entry.generation, registration.generation)?;
                if entry.join.is_finished() {
                    return Ok(LogicalExecutionJoinReadiness::Ready);
                }
            }
        }
        let actor = self.actor(registration)?;
        Ok(if actor.registry_join_readiness(false).await? {
            LogicalExecutionJoinReadiness::Ready
        } else {
            LogicalExecutionJoinReadiness::WaitingForConvergence
        })
    }

    pub(crate) async fn close_ingress_for_join(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        let actor = {
            let mut state = self.lock();
            match state
                .entries
                .get_mut(&registration.initial_execution.query_id())
            {
                Some(RegistryEntry::Installed(entry)) => {
                    validate_generation(entry.generation, registration.generation)?;
                    if entry.ingress_closed || entry.join.is_finished() {
                        entry.ingress_closed = true;
                        return Ok(());
                    }
                    entry.actor.clone()
                }
                Some(RegistryEntry::Joined(entry)) => {
                    validate_generation(entry.generation, registration.generation)?;
                    return Err(LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted);
                }
                _ => return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
            }
        };
        if actor.registry_join_readiness(true).await? {
            let mut state = self.lock();
            match state
                .entries
                .get_mut(&registration.initial_execution.query_id())
            {
                Some(RegistryEntry::Installed(entry)) => {
                    validate_generation(entry.generation, registration.generation)?;
                    entry.ingress_closed = true;
                    Ok(())
                }
                _ => Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
            }
        } else {
            Err(LogicalExecutionRuntimeRegistryError::JoinNotReady)
        }
    }

    /// Waits by polling the Registry-owned join handle in place. Cancelling
    /// this future leaves the handle in the installed entry for a later wait.
    pub(crate) async fn join(
        &self,
        registration: &mut LogicalExecutionRegistration,
    ) -> Result<LogicalExecutionJoinReceipt, LogicalExecutionRuntimeRegistryError> {
        self.wait_active_native_custody(registration).await;
        self.close_ingress_for_join(registration).await?;
        let query = registration.initial_execution.query_id();
        let generation = registration.generation;
        let joined = loop {
            // Both the original role and shutdown may observe this SAME join.
            // Its completion event wakes each original waiter; no last-waker
            // overwrite can strand the role future in the fixed bank.
            let changed = self.inner.progress.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            tokio::select! {
                biased;
                result = std::future::poll_fn(|context| self.poll_original_join(query, generation, context)) => break result?,
                _ = &mut changed => {},
            }
        };
        Ok(LogicalExecutionJoinReceipt {
            registry_id: self.inner.id,
            initial_execution: joined.0,
            generation: joined.1,
            actor_id: joined.2,
            outcome: joined.3,
        })
    }

    fn poll_original_join(
        &self,
        query: QueryId,
        generation: u64,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<
        Result<
            (
                QueryExecutionId,
                u64,
                LogicalExecutionActorId,
                LogicalExecutionJoinOutcome,
            ),
            LogicalExecutionRuntimeRegistryError,
        >,
    > {
        let join = {
            let state = self.lock();
            match state.entries.get(&query) {
                Some(RegistryEntry::Installed(entry)) => {
                    validate_generation(entry.generation, generation)?;
                    Arc::clone(&entry.join)
                }
                Some(RegistryEntry::Joined(entry)) => {
                    validate_generation(entry.generation, generation)?;
                    return Poll::Ready(Err(
                        LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted,
                    ));
                }
                _ => {
                    return Poll::Ready(Err(
                        LogicalExecutionRuntimeRegistryError::UnknownRegistration,
                    ));
                }
            }
        };
        let (result, completed) = std::task::ready!(join.poll(cx));
        let result = result.map_err(Arc::new);
        let outcome = match &result {
            Ok(()) => LogicalExecutionJoinOutcome::Completed,
            Err(error) if error.is_cancelled() => LogicalExecutionJoinOutcome::Cancelled,
            Err(_) => LogicalExecutionJoinOutcome::Panicked,
        };
        let (proof, removed, source) = {
            let mut state = self.lock();
            let Some(RegistryEntry::Installed(entry)) = state.entries.get_mut(&query) else {
                unreachable!("actual join lease keeps its original installed entry");
            };
            assert_eq!(entry.generation, generation);
            let source = entry.original_exit.clone();
            let proof = (
                entry.initial_execution,
                entry.generation,
                entry.actor.id(),
                outcome,
            );
            if let Some(source) = &source {
                source.joined_silent(
                    OriginalAssociation {
                        registry: self.inner.id,
                        initial: entry.initial_execution,
                        generation,
                    },
                    result.clone(),
                );
            }
            let joined = JoinedEntry {
                generation,
                initial_execution: entry.initial_execution,
                actor_id: entry.actor.id(),
                outcome,
                original_result: result,
                original_exit: source.clone(),
                internal_health: entry.internal_health.take(),
            };
            let removed = state.entries.insert(query, RegistryEntry::Joined(joined));
            (proof, removed, source)
        };
        // Original JoinError is stable in Joined/source before any destructor
        // or notification can run. This is also shutdown's sole join author.
        drop(completed);
        drop(removed);
        if let Some(source) = source {
            if source.has_failure() {
                let notice = self.publish_original_exit_health_silent(&source);
                if let Some(notice) = notice {
                    notice.notify();
                }
            }
            source.notify();
        }
        self.inner.progress.notify_waiters();
        Poll::Ready(Ok(proof))
    }

    pub(super) fn original_exit_pending(&self) -> bool {
        self.lock().entries.values().any(|entry| match entry {
            RegistryEntry::Reserved(entry) => entry
                .original_exit
                .as_ref()
                .is_some_and(|s| s.pending_actor()),
            RegistryEntry::Installed(entry) => entry
                .original_exit
                .as_ref()
                .is_some_and(|s| s.pending_actor()),
            RegistryEntry::Joined(_) => false,
        })
    }

    pub(super) fn mark_original_exit_ready(&self) {
        self.inner
            .original_exit_ready
            .store(true, Ordering::Release);
        self.inner.progress.notify_one();
    }
    pub(super) async fn original_exit_changed(&self) {
        self.inner.progress.notified().await;
    }

    /// The original map and service cursor only; no new task or completion map.
    pub(super) fn poll_original_exit_turn(
        &self,
        cursor: &mut OriginalExitCursor,
        budget: std::num::NonZeroUsize,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<()> {
        use std::ops::Bound::{Excluded, Unbounded};
        if !cursor.active {
            if !self.inner.original_exit_ready.swap(false, Ordering::AcqRel) {
                return Poll::Pending;
            }
            cursor.active = true;
            cursor.key = None;
        }
        for _ in 0..budget.get() {
            let selected = {
                let state = self.lock();
                state
                    .entries
                    .range((cursor.key.map_or(Unbounded, Excluded), Unbounded))
                    .next()
                    .map(|(&query, entry)| {
                        (
                            query,
                            match entry {
                                RegistryEntry::Installed(entry)
                                    if entry
                                        .original_exit
                                        .as_ref()
                                        .is_some_and(|s| s.pending_actor()) =>
                                {
                                    Some((entry.generation, entry.initial_execution))
                                }
                                _ => None,
                            },
                        )
                    })
            };
            let Some((query, selected)) = selected else {
                cursor.key = None;
                cursor.active = false;
                return Poll::Pending;
            };
            cursor.key = Some(query);
            let Some((generation, initial)) = selected else {
                continue;
            };
            if self
                .inner
                .active_native
                .blocks_registration(initial, generation)
            {
                continue;
            }
            let (actor, close, join) = {
                let mut state = self.lock();
                let Some(RegistryEntry::Installed(entry)) = state.entries.get_mut(&query) else {
                    continue;
                };
                (
                    entry.actor.clone(),
                    entry.original_close.take(),
                    Arc::clone(&entry.join),
                )
            };
            let mut close = close.unwrap_or_else(|| {
                let closed = actor.request_registry_close_for_join(RegistryClose::Retire);
                Box::pin(original_close_wait(closed))
            });
            join.wake.forward.register(cx.waker());
            let close_wake = std::task::Waker::from(Arc::clone(&join.wake));
            let mut close_context = std::task::Context::from_waker(&close_wake);
            match close.as_mut().poll(&mut close_context) {
                Poll::Pending => {
                    let mut state = self.lock();
                    if let Some(RegistryEntry::Installed(entry)) = state.entries.get_mut(&query) {
                        assert!(entry.original_close.is_none());
                        entry.original_close = Some(close);
                    }
                }
                Poll::Ready(()) => {
                    drop(close);
                    {
                        let mut state = self.lock();
                        if let Some(RegistryEntry::Installed(entry)) = state.entries.get_mut(&query)
                        {
                            entry.ingress_closed = true;
                        }
                    }
                    match self.poll_original_join(query, generation, cx) {
                        Poll::Ready(Ok(proof)) => {
                            let receipt = LogicalExecutionJoinReceipt {
                                registry_id: self.inner.id,
                                initial_execution: proof.0,
                                generation: proof.1,
                                actor_id: proof.2,
                                outcome: proof.3,
                            };
                            let _retained = self.retire(receipt);
                            return Poll::Ready(());
                        }
                        Poll::Ready(Err(_)) | Poll::Pending => {}
                    }
                }
            }
            drop(actor);
        }
        Poll::Ready(())
    }

    /// Requests actor ingress closure, waits for the exact closure fact, then
    /// consumes the Registry-owned join and retirement receipts. Cancelling
    /// this future leaves both the actor task and join handle in the Registry.
    pub(crate) async fn join_and_retire(
        &self,
        mut registration: LogicalExecutionRegistration,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        // The close request itself may complete the original actor WorkOwner.
        // Preserve its same Native custody before closing ingress, as on the
        // shutdown path; waiting only inside join() would already be too late.
        self.wait_active_native_custody(&registration).await;
        let actor = self.actor(&registration)?;
        let mut closed = actor.request_registry_close_for_join(RegistryClose::Retire);
        while !*closed.borrow_and_update() {
            if closed.changed().await.is_err() {
                break;
            }
        }
        self.record_ingress_closed(&registration)?;
        let receipt = self.join(&mut registration).await?;
        self.retire(receipt).map_err(|(error, _receipt)| error)
    }

    pub(crate) fn retire(
        &self,
        receipt: LogicalExecutionJoinReceipt,
    ) -> Result<
        (),
        (
            LogicalExecutionRuntimeRegistryError,
            LogicalExecutionJoinReceipt,
        ),
    > {
        let fail = |error, receipt| Err((error, receipt));
        if receipt.registry_id != self.inner.id {
            return fail(
                LogicalExecutionRuntimeRegistryError::ForeignRegistry,
                receipt,
            );
        }
        let query = receipt.initial_execution.query_id();
        let mut state = self.lock();
        match state.entries.get(&query) {
            Some(RegistryEntry::Joined(entry))
                if entry.generation == receipt.generation
                    && entry.initial_execution == receipt.initial_execution
                    && entry.actor_id == receipt.actor_id
                    && entry.outcome == receipt.outcome
                    && entry.outcome
                        == match &entry.original_result {
                            Ok(()) => LogicalExecutionJoinOutcome::Completed,
                            Err(error) if error.is_cancelled() => {
                                LogicalExecutionJoinOutcome::Cancelled
                            }
                            Err(_) => LogicalExecutionJoinOutcome::Panicked,
                        } =>
            {
                if entry.internal_health.is_some() {
                    return fail(
                        LogicalExecutionRuntimeRegistryError::InternalConsumerHealthUnresolved,
                        receipt,
                    );
                }
                if entry
                    .original_exit
                    .as_ref()
                    .is_some_and(|source| source.has_unhandled_failure())
                {
                    return fail(
                        LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved,
                        receipt,
                    );
                }
                let removed = state.entries.remove(&query);
                drop(state);
                drop(removed);
                self.inner.progress.notify_one();
                Ok(())
            }
            Some(RegistryEntry::Joined(_)) => fail(
                LogicalExecutionRuntimeRegistryError::StaleGeneration,
                receipt,
            ),
            Some(RegistryEntry::Installed(_) | RegistryEntry::Reserved(_)) => {
                fail(LogicalExecutionRuntimeRegistryError::JoinNotReady, receipt)
            }
            None => fail(
                LogicalExecutionRuntimeRegistryError::UnknownRegistration,
                receipt,
            ),
        }
    }

    async fn observe_context_convergence(
        &self,
        registration: &LogicalExecutionRegistration,
        context: QueryContextRef,
        convergence: RegistryContextConvergence,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        let actor = {
            self.validate_registry(registration)?;
            let state = self.lock();
            match state
                .entries
                .get(&registration.initial_execution.query_id())
            {
                Some(RegistryEntry::Installed(entry)) => {
                    validate_generation(entry.generation, registration.generation)?;
                    // The reservation freezes only the initial attempt's
                    // contexts. Replacement attempts keep the same logical
                    // query identity but may select a different exact context
                    // set. The actor owns every current and residual attempt
                    // ledger and performs the final exact-context check.
                    if context.query_execution_id().query_id()
                        != registration.initial_execution.query_id()
                    {
                        return Err(LogicalExecutionRuntimeRegistryError::ContextNotRegistered);
                    }
                    entry.actor.clone()
                }
                _ => return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
            }
        };
        actor
            .observe_registry_context_convergence(context, convergence)
            .await
            .map_err(Into::into)
    }

    fn validate_registry(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        if registration.registry_id == self.inner.id {
            Ok(())
        } else {
            Err(LogicalExecutionRuntimeRegistryError::ForeignRegistry)
        }
    }

    fn lock(&self) -> MutexGuard<'_, RegistryState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl LogicalExecutionRuntimeReservation {
    pub(crate) fn spawn_and_install(
        mut self,
        config: LogicalExecutionActorConfig,
    ) -> Result<InstalledLogicalExecution, LogicalExecutionRuntimeRegistryError> {
        let query = self.initial_execution.query_id();
        {
            let state = self.registry.lock();
            let Some(RegistryEntry::Reserved(entry)) = state.entries.get(&query) else {
                return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration);
            };
            if entry.generation != self.generation {
                return Err(LogicalExecutionRuntimeRegistryError::StaleGeneration);
            }
            let config_contexts: BTreeSet<_> = config
                .required_establish_contexts()
                .iter()
                .copied()
                .collect();
            if config.initial_execution() != entry.initial_execution
                || config_contexts != entry.contexts
            {
                return Err(LogicalExecutionRuntimeRegistryError::ConfigDoesNotMatchReservation);
            }
        }
        {
            let mut state = self.registry.lock();
            if let Some(RegistryEntry::Reserved(entry)) = state.entries.get_mut(&query) {
                entry.spawn_started = true;
            }
        }
        let spawned = match spawn_logical_execution_actor(&self.registry.inner.runtime, config) {
            Ok(spawned) => spawned,
            Err(error) => {
                let mut state = self.registry.lock();
                if let Some(RegistryEntry::Reserved(entry)) = state.entries.get_mut(&query) {
                    entry.spawn_started = false;
                }
                return Err(error.into());
            }
        };
        let (owner, initial_attempt, output) = spawned.into_parts();
        let (actor, join) = owner.into_registry_parts();
        let registration = LogicalExecutionRegistration {
            registry_id: self.registry.inner.id,
            initial_execution: self.initial_execution,
            generation: self.generation,
        };
        {
            let mut state = self.registry.lock();
            let reserved = match state.entries.remove(&query) {
                Some(RegistryEntry::Reserved(reserved))
                    if reserved.generation == self.generation =>
                {
                    reserved
                }
                _ => unreachable!("live reservation must retain its exact registry slot"),
            };
            if let Some(source) = &reserved.original_exit {
                source.installed_silent(OriginalAssociation {
                    registry: self.registry.inner.id,
                    initial: self.initial_execution,
                    generation: self.generation,
                });
            }
            state.entries.insert(
                query,
                RegistryEntry::Installed(InstalledEntry {
                    generation: reserved.generation,
                    initial_execution: reserved.initial_execution,
                    actor,
                    join: OriginalActorJoin::new(join, Arc::downgrade(&self.registry.inner)),
                    original_exit: reserved.original_exit,
                    original_close: None,
                    ingress_closed: false,
                    internal_health: None,
                }),
            );
        }
        self.registry.inner.progress.notify_one();
        self.active = false;
        Ok(InstalledLogicalExecution {
            registration,
            initial_attempt,
            output,
        })
    }
}

fn validate_generation(
    actual: u64,
    supplied: u64,
) -> Result<(), LogicalExecutionRuntimeRegistryError> {
    if actual == supplied {
        Ok(())
    } else {
        Err(LogicalExecutionRuntimeRegistryError::StaleGeneration)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use novarocks_types::identity::{AttemptId, BackendProcessId, FrontendProcessId};
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };

    use super::*;
    use crate::coordination::LogicalConclusion;
    use novarocks_coordination_contract::ExecutionEffect;

    fn execution(tag: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(91, tag as i64), AttemptId::new(1).unwrap()).unwrap()
    }

    fn config(
        execution: QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> LogicalExecutionActorConfig {
        config_observing_cancellation(execution, contexts).0
    }

    /// Also returns a view of the work cancellation the actor owns.
    fn config_observing_cancellation(
        execution: QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> (
        LogicalExecutionActorConfig,
        novarocks_workload_control::CancellationView,
    ) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 12,
                per_scope_bytes: (1 << 20) - (1 << 12),
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let stage = root.owner.scope().try_acquire(Stage::Execution).unwrap();
        let cancellation = root.owner.scope().cancellation().unwrap();
        drop(root.business);
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(4).unwrap(),
            contexts,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            root.owner,
            stage,
        )
        .unwrap();
        (config, cancellation)
    }

    #[derive(Debug)]
    struct DropTrackedActiveOwner(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for DropTrackedActiveOwner {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl crate::api::ActiveNativeAttemptOwner for DropTrackedActiveOwner {
        fn run<'a>(
            &'a mut self,
            _drive: &'a crate::coordination::NativeAttemptDrive,
            _cancellation: novarocks_workload_control::CancellationView,
        ) -> crate::api::NativeAttemptRunFuture<'a> {
            Box::pin(async { panic!("custody fixture is not dispatched") })
        }
        fn converge<'a>(
            &'a mut self,
            _request: crate::api::NativeActiveConvergenceRequest,
        ) -> crate::api::NativeActiveAttemptConvergenceFuture<'a> {
            Box::pin(async { panic!("custody fixture returns original receipts explicitly") })
        }
    }

    fn native_test_context(execution: QueryExecutionId) -> QueryContextRef {
        QueryContextRef::new(
            execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    #[derive(Debug)]
    struct NeverEstablishedAbortPort {
        capacity: tokio::sync::watch::Sender<u64>,
    }
    impl crate::coordination::AbortQueryContextEffectPort for NeverEstablishedAbortPort {
        fn subscribe_capacity(&self) -> tokio::sync::watch::Receiver<u64> {
            self.capacity.subscribe()
        }
        fn try_reserve(
            &self,
            identity: crate::coordination::AbortQueryContextIssueIdentity,
        ) -> crate::coordination::AbortQueryContextEffectAdmission {
            panic!("a never-established custody fixture must not issue Abort: {identity:?}")
        }
    }
    fn native_custody_config(
        execution: QueryExecutionId,
        context: QueryContextRef,
    ) -> LogicalExecutionActorConfig {
        let (capacity, _) = tokio::sync::watch::channel(0);
        // A real nonempty context manifest requires an Abort port before actor
        // installation. These fixtures issue no admission or Establish, so any
        // actual Abort request is a fixture violation, never a fabricated ACK.
        config(execution, vec![context]).with_abort_query_context_effect_port(
            Arc::new(NeverEstablishedAbortPort { capacity }),
            NonZeroUsize::new(2).unwrap(),
        )
    }

    #[tokio::test]
    async fn unpolled_active_residual_returns_same_box_without_blocking_another_registration() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let exact = execution(71);
        let context = native_test_context(exact);
        let installed = registry
            .reserve(exact, vec![context])
            .unwrap()
            .spawn_and_install(native_custody_config(exact, context))
            .unwrap();
        let (registration, initial, output) = installed.into_parts();
        let _output = output.into_output();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let owner = registry
            .reserve_active_native_owner(&registration, exact, &[context])
            .unwrap()
            .bind(Box::new(DropTrackedActiveOwner(Arc::clone(&drops))));
        let actor = registry.actor(&registration).unwrap();
        actor.initialization_failed(initial).await.unwrap();
        // A queued residual can be dropped before its first poll. Its original
        // Box must still be held by the pre-reserved original runtime record.
        let unpolled = async move {
            let _same_owner = owner;
            std::future::pending::<()>().await;
        };
        drop(unpolled);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(registry.active_native_custody_count(), 1);
        assert!(
            registry
                .active_native_registration_health(&registration)
                .is_some()
        );
        assert_eq!(
            registry.join_readiness(&registration).await.unwrap(),
            LogicalExecutionJoinReadiness::WaitingForConvergence
        );

        // Process health reports the problem; it does not become a global
        // barrier that prevents a different original actor from retiring.
        let other = execution(72);
        let installed = registry
            .reserve(other, Vec::new())
            .unwrap()
            .spawn_and_install(config(other, Vec::new()))
            .unwrap();
        let (other_registration, initial, output) = installed.into_parts();
        let _other_output = output.into_output();
        let actor = registry.actor(&other_registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();
        actor.complete_attempt(running).await.unwrap();
        assert!(
            registry
                .active_native_registration_health(&other_registration)
                .is_none()
        );
        registry.join_and_retire(other_registration).await.unwrap();
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        registry.abandon_for_process_exit();
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn original_active_custody_retires_only_after_exact_actor_observations() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let exact = execution(73);
        let context = native_test_context(exact);
        let installed = registry
            .reserve(exact, vec![context])
            .unwrap()
            .spawn_and_install(native_custody_config(exact, context))
            .unwrap();
        let (registration, initial, output) = installed.into_parts();
        let _output = output.into_output();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut owner = registry
            .reserve_active_native_owner(&registration, exact, &[context])
            .unwrap()
            .bind(Box::new(DropTrackedActiveOwner(Arc::clone(&drops))));
        registry
            .actor(&registration)
            .unwrap()
            .initialization_failed(initial)
            .await
            .unwrap();
        owner
            .record_and_close(crate::api::NativeActiveConvergenceClosure::Evidence(
                crate::api::NativeAttemptConvergence::contexts(vec![
                    crate::api::NativeContextConvergence::never_established(context),
                ]),
            ))
            .await
            .unwrap();
        assert_eq!(registry.active_native_custody_count(), 0);
        registry.join_and_retire(registration).await.unwrap();
        drop(owner);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn duplicate_active_closure_keeps_original_box_and_custody_until_process_exit() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let exact = execution(74);
        let context = native_test_context(exact);
        let installed = registry
            .reserve(exact, vec![context])
            .unwrap()
            .spawn_and_install(native_custody_config(exact, context))
            .unwrap();
        let (registration, initial, output) = installed.into_parts();
        let _output = output.into_output();
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut owner = registry
            .reserve_active_native_owner(&registration, exact, &[context])
            .unwrap()
            .bind(Box::new(DropTrackedActiveOwner(Arc::clone(&drops))));
        registry
            .actor(&registration)
            .unwrap()
            .initialization_failed(initial)
            .await
            .unwrap();
        let fact = crate::api::NativeContextConvergence::never_established(context);
        assert!(
            owner
                .record_and_close(crate::api::NativeActiveConvergenceClosure::Evidence(
                    crate::api::NativeAttemptConvergence::contexts(vec![fact, fact]),
                ))
                .await
                .is_err()
        );
        drop(owner);
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(registry.active_native_custody_count(), 1);
        registry.abandon_for_process_exit();
        assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reservation_installs_actor_before_exposing_attempt_and_output() {
        let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(1);
        let installed = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(execution, Vec::new()))
            .unwrap();
        let (mut registration, initial, output) = installed.into_parts();
        assert_eq!(registration.initial_execution(), execution);
        assert!(matches!(
            output.into_output(),
            crate::api::ExecutionOutput::Completion
        ));

        let actor = registry.actor(&registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();
        assert_eq!(
            actor.complete_attempt(running).await.unwrap(),
            LogicalConclusion::Succeeded
        );
        assert_eq!(
            registry.join_readiness(&registration).await.unwrap(),
            LogicalExecutionJoinReadiness::Ready
        );

        registry
            .close_ingress_for_join(&registration)
            .await
            .unwrap();
        let unpolled_join = registry.join(&mut registration);
        drop(unpolled_join);
        let receipt = registry.join(&mut registration).await.unwrap();
        registry.retire(receipt).unwrap();
        // Query identity allocation owns process-period uniqueness. The
        // runtime registry does not retain an unbounded tombstone per query;
        // its generation still prevents a stale registration from reaching a
        // later occupant of the same map key.
        let reused_query =
            QueryExecutionId::new(execution.query_id(), AttemptId::new(2).unwrap()).unwrap();
        let (mut reused_registration, reused_initial, reused_output) = registry
            .reserve(reused_query, Vec::new())
            .unwrap()
            .spawn_and_install(config(reused_query, Vec::new()))
            .unwrap()
            .into_parts();
        drop(reused_output);
        assert_eq!(
            registry.actor(&registration).unwrap_err(),
            LogicalExecutionRuntimeRegistryError::StaleGeneration
        );
        let reused_actor = registry.actor(&reused_registration).unwrap();
        let reused_running = reused_actor.activate(reused_initial.ready()).await.unwrap();
        reused_actor.complete_attempt(reused_running).await.unwrap();
        let receipt = registry.join(&mut reused_registration).await.unwrap();
        registry.retire(receipt).unwrap();
        registry.shutdown().unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_polled_join_wait_keeps_the_join_handle_registered() {
        let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(11);
        let (mut registration, initial, _output) = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(execution, Vec::new()))
            .unwrap()
            .into_parts();
        let actor = registry.actor(&registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();

        {
            let mut state = registry.lock();
            let Some(RegistryEntry::Installed(entry)) =
                state.entries.get_mut(&execution.query_id())
            else {
                panic!("installed runtime must remain registered");
            };
            // Isolate the join-wait cancellation property from the actor's
            // readiness gate: the production close path sets this only after
            // the actor accepted its close command.
            entry.ingress_closed = true;
        }
        let mut pending_join = Box::pin(registry.join(&mut registration));
        std::future::poll_fn(|context| {
            assert!(matches!(pending_join.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        drop(pending_join);
        {
            let mut state = registry.lock();
            let Some(RegistryEntry::Installed(entry)) =
                state.entries.get_mut(&execution.query_id())
            else {
                panic!("cancelled wait must not remove the installed runtime");
            };
            assert!(!entry.join.is_finished());
            entry.ingress_closed = false;
        }

        actor.complete_attempt(running).await.unwrap();
        let receipt = registry.join(&mut registration).await.unwrap();
        assert_eq!(receipt.outcome(), LogicalExecutionJoinOutcome::Completed);
        registry.retire(receipt).unwrap();
        registry.shutdown().unwrap();
    }

    #[test]
    fn dropped_reservation_releases_the_exact_query_slot() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let registry = LogicalExecutionRuntimeRegistry::new(runtime.handle().clone());
        let execution = execution(2);
        drop(registry.reserve(execution, Vec::new()).unwrap());
        assert!(registry.reserve(execution, Vec::new()).is_ok());
        registry.shutdown().unwrap();
    }

    #[test]
    fn process_shutdown_closes_reservations_and_retains_the_owner_until_empty() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let registry = LogicalExecutionRuntimeRegistry::new(runtime.handle().clone());
        let reserved_execution = execution(12);
        let reservation = registry.reserve(reserved_execution, Vec::new()).unwrap();
        let (error, registry) = registry.shutdown().unwrap_err();
        assert_eq!(
            error,
            LogicalExecutionRuntimeRegistryError::RegistryNotEmpty
        );
        assert_eq!(
            registry.reserve(execution(13), Vec::new()).unwrap_err(),
            LogicalExecutionRuntimeRegistryError::RegistryClosing
        );
        drop(reservation);
        registry.shutdown().unwrap();
    }

    /// An owner abandoned by an unwind must not replace the panic that
    /// abandoned it. Without the guard in its destructor this test does not
    /// fail: it aborts the whole binary and takes every other test's result
    /// with it.
    #[tokio::test]
    async fn an_owner_abandoned_by_an_unwind_reports_the_original_panic() {
        let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());

        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _abandoned = registry;
            panic!("the assertion that actually failed");
        }))
        .expect_err("the closure panics");

        assert_eq!(
            *payload
                .downcast_ref::<&str>()
                .expect("the original panic payload survives the unwind"),
            "the assertion that actually failed"
        );
    }

    #[tokio::test]
    async fn terminal_shutdown_error_after_empty_registry_records_convergence() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        registry.inject_shutdown_error_once(LogicalExecutionRuntimeRegistryError::JoinNotReady);

        assert_eq!(
            registry
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await,
            Err(LogicalExecutionRuntimeShutdownError::Registry(
                LogicalExecutionRuntimeRegistryError::JoinNotReady,
            ))
        );
        assert!(registry.shutdown_complete);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the converged owner must not replay a terminal diagnostic");
    }

    #[tokio::test]
    async fn bounded_shutdown_joins_and_retires_completed_actors() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(15);
        let (registration, initial, _output) = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(execution, Vec::new()))
            .unwrap()
            .into_parts();
        let actor = registry.actor(&registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();
        actor.complete_attempt(running).await.unwrap();

        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(registry.entry_count(), 0);
    }

    #[tokio::test]
    async fn retiring_a_concluded_actor_leaves_its_work_uncancelled() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(31);
        let (config, cancellation) = config_observing_cancellation(execution, Vec::new());
        let (registration, initial, _output) = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config)
            .unwrap()
            .into_parts();
        let actor = registry.actor(&registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();
        assert_eq!(
            actor.complete_attempt(running).await.unwrap(),
            LogicalConclusion::Succeeded
        );

        // Retirement follows the client-visible conclusion; closing the
        // actor's ingress to join it must not revise that outcome.
        registry
            .handle()
            .join_and_retire(registration)
            .await
            .unwrap();
        assert_eq!(cancellation.reason(), None);
        assert_eq!(registry.entry_count(), 0);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_the_work_of_an_actor_still_running() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(32);
        let (config, cancellation) = config_observing_cancellation(execution, Vec::new());
        let (registration, initial, _output) = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config)
            .unwrap()
            .into_parts();
        let actor = registry.actor(&registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();

        let shutdown = registry.shutdown_until(Instant::now()).await;
        assert!(actor.registry_close_was_requested());
        let reason = tokio::time::timeout(Duration::from_secs(5), cancellation.cancelled())
            .await
            .expect("shutdown cancels running work");
        assert_eq!(reason, CancellationReason::ServerShutdown);
        drop(running);
        if shutdown.is_err() {
            registry
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn bounded_shutdown_timeout_retains_owner_for_exact_retry() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let reservation = registry.reserve(execution(16), Vec::new()).unwrap();

        assert_eq!(
            registry.shutdown_until(Instant::now()).await,
            Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded {
                remaining_entries: 1,
            })
        );
        assert_eq!(
            registry.reserve(execution(17), Vec::new()).unwrap_err(),
            LogicalExecutionRuntimeRegistryError::RegistryClosing
        );

        drop(reservation);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn expired_deadline_still_broadcasts_close_to_installed_actor() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let initial_execution = execution(22);
        let (registration, initial, _output) = registry
            .reserve(initial_execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(initial_execution, Vec::new()))
            .unwrap()
            .into_parts();
        let actor = registry.actor(&registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();

        let shutdown = registry.shutdown_until(Instant::now()).await;
        assert!(matches!(
            shutdown,
            Ok(())
                | Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded {
                    remaining_entries: 1,
                })
        ));
        assert!(actor.registry_close_was_requested());

        let mut ingress_closed = actor.registry_ingress_closed();
        while !*ingress_closed.borrow_and_update() {
            ingress_closed.changed().await.unwrap();
        }
        drop(running);
        if shutdown.is_err() {
            registry
                .shutdown_until(Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn bounded_shutdown_wakes_when_an_outstanding_reservation_is_dropped() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let reservation = registry.reserve(execution(19), Vec::new()).unwrap();
        let mut shutdown =
            Box::pin(registry.shutdown_until(Instant::now() + Duration::from_secs(60)));
        std::future::poll_fn(|context| {
            assert!(matches!(shutdown.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;

        drop(reservation);
        shutdown.await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_bounded_shutdown_wait_keeps_registry_join_ownership() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(18);
        let (registration, initial, _output) = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(execution, Vec::new()))
            .unwrap()
            .into_parts();
        let actor = registry.actor(&registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();

        let mut shutdown =
            Box::pin(registry.shutdown_until(Instant::now() + Duration::from_secs(60)));
        std::future::poll_fn(|context| {
            assert!(matches!(shutdown.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        drop(shutdown);

        assert_eq!(registry.entry_count(), 1);
        let mut ingress_closed = actor.registry_ingress_closed();
        while !*ingress_closed.borrow_and_update() {
            ingress_closed.changed().await.unwrap();
        }
        drop(running);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn saturated_actor_mailbox_does_not_block_close_broadcast_to_other_actors() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let first_execution = execution(20);
        let (first_registration, first_initial, _first_output) = registry
            .reserve(first_execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(first_execution, Vec::new()))
            .unwrap()
            .into_parts();
        let first_actor = registry.actor(&first_registration).unwrap();
        let first_running = first_actor.activate(first_initial.ready()).await.unwrap();

        let second_execution = execution(21);
        let (second_registration, second_initial, _second_output) = registry
            .reserve(second_execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(second_execution, Vec::new()))
            .unwrap()
            .into_parts();
        let second_actor = registry.actor(&second_registration).unwrap();
        let second_running = second_actor.activate(second_initial.ready()).await.unwrap();

        let mut queued_snapshots = (0..4)
            .map(|_| Box::pin(first_actor.snapshot()))
            .collect::<Vec<_>>();
        for snapshot in &mut queued_snapshots {
            std::future::poll_fn(|context| {
                assert!(matches!(snapshot.as_mut().poll(context), Poll::Pending));
                Poll::Ready(())
            })
            .await;
        }

        let mut shutdown =
            Box::pin(registry.shutdown_until(Instant::now() + Duration::from_secs(60)));
        std::future::poll_fn(|context| {
            assert!(matches!(shutdown.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        assert!(first_actor.registry_close_was_requested());
        assert!(second_actor.registry_close_was_requested());
        drop(shutdown);
        drop(queued_snapshots);

        for actor in [&first_actor, &second_actor] {
            let mut ingress_closed = actor.registry_ingress_closed();
            while !*ingress_closed.borrow_and_update() {
                ingress_closed.changed().await.unwrap();
            }
        }
        drop(first_running);
        drop(second_running);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn actor_rejects_an_unregistered_context_after_logical_identity_validation() {
        let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(3);
        let (mut registration, initial, _output) = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(execution, Vec::new()))
            .unwrap()
            .into_parts();
        let foreign_execution =
            QueryExecutionId::new(QueryId::new(91, 99), AttemptId::new(1).unwrap()).unwrap();
        let foreign_logical_context = QueryContextRef::new(
            foreign_execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        assert_eq!(
            registry
                .observe_worker_process_replaced(&registration, foreign_logical_context)
                .await,
            Err(LogicalExecutionRuntimeRegistryError::ContextNotRegistered)
        );
        let foreign = QueryContextRef::new(
            execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        assert_eq!(
            registry
                .observe_worker_process_replaced(&registration, foreign)
                .await,
            Err(LogicalExecutionRuntimeRegistryError::Actor(
                LogicalExecutionActorError::StandDown(
                    crate::coordination::ContextStandDownError::StandDownNotStarted,
                ),
            ))
        );
        drop(initial);
        loop {
            if registry.join_readiness(&registration).await.unwrap()
                == LogicalExecutionJoinReadiness::Ready
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let receipt = registry.join(&mut registration).await.unwrap();
        registry.retire(receipt).unwrap();
        registry.shutdown().unwrap();
    }

    #[tokio::test]
    async fn failed_retirement_returns_the_join_receipt_for_exact_retry() {
        let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let foreign_registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(4);
        let (mut registration, initial, _output) = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(execution, Vec::new()))
            .unwrap()
            .into_parts();
        let actor = registry.actor(&registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();
        actor.complete_attempt(running).await.unwrap();
        let receipt = registry.join(&mut registration).await.unwrap();
        let (error, receipt) = foreign_registry.retire(receipt).unwrap_err();
        assert_eq!(error, LogicalExecutionRuntimeRegistryError::ForeignRegistry);
        registry.retire(receipt).unwrap();
        foreign_registry.shutdown().unwrap();
        registry.shutdown().unwrap();
    }

    #[tokio::test]
    async fn cancelled_actor_join_still_produces_a_retirable_receipt() {
        let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let execution = execution(14);
        let (mut registration, initial, _output) = registry
            .reserve(execution, Vec::new())
            .unwrap()
            .spawn_and_install(config(execution, Vec::new()))
            .unwrap()
            .into_parts();
        drop(initial);
        let join = {
            let mut state = registry.lock();
            let Some(RegistryEntry::Installed(entry)) =
                state.entries.get_mut(&execution.query_id())
            else {
                panic!("installed runtime must remain registered");
            };
            entry.ingress_closed = true;
            Arc::clone(&entry.join)
        };
        join.abort();
        drop(join);

        let receipt = registry.join(&mut registration).await.unwrap();
        assert_eq!(receipt.outcome(), LogicalExecutionJoinOutcome::Cancelled);
        registry.retire(receipt).unwrap();
        registry.shutdown().unwrap();
    }

    #[tokio::test]
    async fn shutdown_keeps_unknown_native_actor_work_while_a_healthy_registration_really_joins() {
        use std::future::Future;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let exact = execution(75);
        let context = native_test_context(exact);
        let installed = registry
            .reserve(exact, vec![context])
            .unwrap()
            .spawn_and_install(native_custody_config(exact, context))
            .unwrap();
        let (registration, initial, output) = installed.into_parts();
        let _output = output.into_output();
        let drops = Arc::new(AtomicUsize::new(0));
        let original = registry
            .reserve_active_native_owner(&registration, exact, &[context])
            .unwrap()
            .bind(Box::new(DropTrackedActiveOwner(Arc::clone(&drops))));
        registry
            .actor(&registration)
            .unwrap()
            .initialization_failed(initial)
            .await
            .unwrap();
        // This real enclosing-drop seam returns the same original Box to its
        // exact pre-reserved health slot; no foreign exit receipt is invented.
        drop(original);

        let healthy = execution(76);
        let installed = registry
            .reserve(healthy, Vec::new())
            .unwrap()
            .spawn_and_install(config(healthy, Vec::new()))
            .unwrap();
        let (healthy_registration, initial, output) = installed.into_parts();
        let _healthy_output = output.into_output();
        let actor = registry.actor(&healthy_registration).unwrap();
        let running = actor.activate(initial.ready()).await.unwrap();
        actor.complete_attempt(running).await.unwrap();
        let handle = registry.handle();
        let mut shutdown =
            Box::pin(registry.shutdown_until(Instant::now() + Duration::from_secs(1)));
        // Poll the real Registry shutdown to actual healthy retirement. The
        // one-second test cap is not a receipt; only removal after join is.
        let healthy_retired = std::future::poll_fn(|cx| {
            if shutdown.as_mut().poll(cx).is_ready() {
                return Poll::Ready(false);
            }
            if !handle.lock().entries.contains_key(&healthy.query_id()) {
                Poll::Ready(true)
            } else {
                Poll::Pending
            }
        })
        .await;
        drop(shutdown);
        let unknown_actor_still_live = match handle.lock().entries.get(&exact.query_id()) {
            Some(RegistryEntry::Installed(entry)) => !entry.join.is_finished(),
            _ => false,
        };
        let original_held = drops.load(Ordering::SeqCst) == 0;
        let one_unknown = registry.active_native_custody_count() == 1;
        let healthy_health_absent = registry
            .active_native_registration_health(&healthy_registration)
            .is_none();
        registry.abandon_for_process_exit();
        assert!(
            healthy_retired,
            "one unknown Native registration cannot strand a healthy sibling join"
        );
        assert!(
            unknown_actor_still_live,
            "RegistryClose must not finish the original actor WorkOwner before Native exit"
        );
        assert!(original_held);
        assert!(one_unknown);
        assert!(healthy_health_absent);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "only explicit process-exit abandonment retires the unknown Box"
        );
    }

    #[tokio::test]
    async fn the_same_shutdown_wait_closes_an_initially_skipped_actor_after_original_native_receipt()
     {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let exact = execution(77);
        let context = native_test_context(exact);
        let installed = registry
            .reserve(exact, vec![context])
            .unwrap()
            .spawn_and_install(native_custody_config(exact, context))
            .unwrap();
        let (registration, initial, output) = installed.into_parts();
        let _output = output.into_output();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut original = registry
            .reserve_active_native_owner(&registration, exact, &[context])
            .unwrap()
            .bind(Box::new(DropTrackedActiveOwner(Arc::clone(&drops))));
        registry
            .actor(&registration)
            .unwrap()
            .initialization_failed(initial)
            .await
            .unwrap();
        let handle = registry.handle();
        let mut shutdown =
            Box::pin(registry.shutdown_until(Instant::now() + Duration::from_secs(1)));
        let initially_pending = std::future::poll_fn(|cx| {
            Poll::Ready(std::future::Future::poll(shutdown.as_mut(), cx).is_pending())
        })
        .await;
        tokio::task::yield_now().await;
        let actor_kept_before_receipt = match handle.lock().entries.get(&exact.query_id()) {
            Some(RegistryEntry::Installed(entry)) => !entry.join.is_finished(),
            _ => false,
        };
        // The original prepared fixture never dispatched admission/Establish.
        // This exact author receipt settles its same pre-reserved Native slot.
        original
            .record_and_close(crate::api::NativeActiveConvergenceClosure::Evidence(
                crate::api::NativeAttemptConvergence::contexts(vec![
                    crate::api::NativeContextConvergence::never_established(context),
                ]),
            ))
            .await
            .unwrap();
        // No second shutdown call, new service or polling timer is involved:
        // Native slot removal wakes the original registered progress waiter.
        let result = shutdown.await;
        let retired = !handle.lock().entries.contains_key(&exact.query_id());
        drop(original);
        let owner_dropped = drops.load(Ordering::SeqCst);
        if result.is_err() {
            registry.abandon_for_process_exit();
        }
        assert!(initially_pending);
        assert!(actor_kept_before_receipt);
        result.expect("the same shutdown wait must resume after normal custody removal");
        assert!(retired);
        assert_eq!(owner_dropped, 1);
    }
    #[tokio::test]
    async fn retire_wait_keeps_original_actor_until_the_same_native_receipt() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let exact = execution(78);
        let context = native_test_context(exact);
        let installed = registry
            .reserve(exact, vec![context])
            .unwrap()
            .spawn_and_install(native_custody_config(exact, context))
            .unwrap();
        let (registration, initial, output) = installed.into_parts();
        let _output = output.into_output();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut original = registry
            .reserve_active_native_owner(&registration, exact, &[context])
            .unwrap()
            .bind(Box::new(DropTrackedActiveOwner(Arc::clone(&drops))));
        registry
            .actor(&registration)
            .unwrap()
            .initialization_failed(initial)
            .await
            .unwrap();
        let handle = registry.handle();
        let mut shutdown = Box::pin(handle.join_and_retire(registration));
        let initially_pending = std::future::poll_fn(|cx| {
            Poll::Ready(std::future::Future::poll(shutdown.as_mut(), cx).is_pending())
        })
        .await;
        tokio::task::yield_now().await;
        let actor_kept_before_receipt = match handle.lock().entries.get(&exact.query_id()) {
            Some(RegistryEntry::Installed(entry)) => !entry.join.is_finished(),
            _ => false,
        };
        // The original prepared fixture never dispatched admission/Establish.
        // This exact author receipt settles its same pre-reserved Native slot.
        original
            .record_and_close(crate::api::NativeActiveConvergenceClosure::Evidence(
                crate::api::NativeAttemptConvergence::contexts(vec![
                    crate::api::NativeContextConvergence::never_established(context),
                ]),
            ))
            .await
            .unwrap();
        // The same pending Retire wait observes the original custody receipt;
        // no close request was allowed to race ahead of that receipt.
        let result = shutdown.await;
        let retired = !handle.lock().entries.contains_key(&exact.query_id());
        drop(original);
        let owner_dropped = drops.load(Ordering::SeqCst);
        if result.is_err() {
            registry.abandon_for_process_exit();
        }
        assert!(initially_pending);
        assert!(actor_kept_before_receipt);
        result.expect("Retire must resume only after original Native custody closes");
        assert!(retired);
        assert_eq!(owner_dropped, 1);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_cut_refuses_late_native_custody_before_activation() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let exact = execution(79);
        let context = native_test_context(exact);
        let installed = registry
            .reserve(exact, vec![context])
            .unwrap()
            .spawn_and_install(native_custody_config(exact, context))
            .unwrap();
        let (registration, initial, output) = installed.into_parts();
        let _output = output.into_output();
        let handle = registry.handle();
        handle.begin_shutdown();
        let refused = handle.reserve_active_native_owner(&registration, exact, &[context]);
        assert!(matches!(
            refused,
            Err(LogicalExecutionRuntimeRegistryError::RegistryClosing)
        ));
        assert_eq!(registry.active_native_custody_count(), 0);
        // This original fixture never dispatched Establish: the rejected
        // reservation grants no Native activation or remote responsibility.
        registry
            .actor(&registration)
            .unwrap()
            .initialization_failed(initial)
            .await
            .unwrap();
        handle
            .observe_context_never_established(&registration, context)
            .await
            .unwrap();
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(handle.lock().entries.is_empty());
        assert_eq!(registry.active_native_custody_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn original_worker_cause_survives_joined_registration_until_explicit_disposition() {
        use crate::cpu::{QueryBlockingExecutorConfig, QueryBlockingExecutorOwner};
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct OriginalPanic(Arc<AtomicUsize>);
        impl Drop for OriginalPanic {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::AcqRel);
            }
        }
        #[derive(Debug)]
        struct OriginalConsumer(crate::api::StatementNativeHealthFailure);
        impl crate::api::InternalComputationConsumer for OriginalConsumer {
            fn begin_local_close(&self) {}
            fn poll_local_closed(
                &self,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Result<(), crate::api::StatementNativeHealthFailure>> {
                // This original fixture already represents an unresolved local
                // source; a close observer cannot turn it into a no-issue exit.
                std::task::Poll::Ready(Err(self.0.clone()))
            }

            fn consume(
                &self,
                _: crate::api::InternalConsumerActivation,
            ) -> crate::api::InternalConsumerFuture {
                Box::pin(async {
                    panic!("health custody fixture must not dispatch a second consumer")
                })
            }
            fn local_health_failure(&self) -> Option<crate::api::StatementNativeHealthFailure> {
                Some(self.0.clone())
            }
        }
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let exact = execution(95);
        let installed = registry
            .reserve(exact, Vec::new())
            .unwrap()
            .spawn_and_install(config(exact, Vec::new()))
            .unwrap();
        let (mut registration, initial, output) = installed.into_parts();
        let _output = output.into_output();
        registry
            .actor(&registration)
            .unwrap()
            .initialization_failed(initial)
            .await
            .unwrap();
        let mut workers = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        ))
        .unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let original_drops = Arc::clone(&drops);
        let cause = workers
            .executor()
            .execute_preserving::<(), _>(move || {
                std::panic::panic_any(OriginalPanic(original_drops));
            })
            .await
            .unwrap_err();
        let original_type = cause.original_payload_is::<OriginalPanic>();
        let alias = cause.clone();
        let exact_alias = alias.shares_original_with(&cause);
        drop(alias);
        let health = crate::api::StatementNativeHealthFailure::actual_exit_unproven(
            "Original Internal CPU failure remains in custody",
            Arc::new(cause),
        );
        let consumer: Arc<dyn crate::api::InternalComputationConsumer> =
            Arc::new(OriginalConsumer(health.clone()));
        let retained = registry.retain_internal_consumer_health(&registration, &consumer, &health);
        drop(consumer);
        drop(health);
        let before_join = drops.load(Ordering::Acquire);
        let receipt = registry.join(&mut registration).await.unwrap();
        let (blocked, receipt) = match registry.retire(receipt) {
            Err((error, original)) => (
                error == LogicalExecutionRuntimeRegistryError::InternalConsumerHealthUnresolved,
                Some(original),
            ),
            Ok(()) => (false, None),
        };
        let safe_health = registry.process_health().is_some();
        let after_join = drops.load(Ordering::Acquire);
        // Test-only explicit disposition takes the original fixed field; no
        // production recovery/clear API is introduced by this candidate.
        let disposition = {
            let mut state = registry.lock();
            match state.entries.get_mut(&exact.query_id()) {
                Some(RegistryEntry::Joined(entry)) => entry.internal_health.take(),
                _ => None,
            }
        };
        let held_by_receiver = drops.load(Ordering::Acquire) == 0 && disposition.is_some();
        drop(disposition);
        let disposition_exited = drops.load(Ordering::Acquire);
        let retired = receipt
            .map(|original| registry.retire(original).is_ok())
            .unwrap_or(false);
        let worker_exit = workers
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await;
        let registry_exit = registry
            .shutdown_until(Instant::now() + Duration::from_secs(5))
            .await;
        if registry_exit.is_err() {
            registry.abandon_for_process_exit();
        }
        assert!(retained.is_ok() && original_type && exact_alias);
        assert!(blocked && safe_health && held_by_receiver && retired);
        assert_eq!((before_join, after_join, disposition_exited), (0, 0, 1));
        assert!(worker_exit.is_ok() && registry_exit.is_ok());
    }

    #[test]
    fn registry_health_geometry_is_one_original_phase_record() {
        let geometry = LogicalExecutionRuntimeRegistry::entry_geometry();
        assert_eq!(
            geometry.record_layout(),
            std::alloc::Layout::new::<RegistryEntry>()
        );
        assert_eq!(
            geometry.internal_health_field_layout(),
            std::alloc::Layout::new::<Option<InternalConsumerHealth>>()
        );
        assert!(geometry.record_layout().size() >= std::mem::size_of::<InstalledEntry>());
        assert!(geometry.record_layout().size() >= std::mem::size_of::<JoinedEntry>());
        assert_eq!(
            geometry.inline_bytes_for_entries(1),
            Ok(geometry.record_layout().size())
        );
        assert_eq!(
            geometry.inline_bytes_for_entries(usize::MAX),
            Err(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,)
        );
    }
}

#[cfg(test)]
#[path = "original_exit_registry_tests.rs"]
mod original_exit_registry_tests;

#[cfg(test)]
mod original_health_migration_tests {
    use super::*;

    #[tokio::test]
    async fn process_health_survives_runtime_shutdown_until_its_last_original_handle_exits() {
        let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let handle = registry.handle();
        let cause: Arc<dyn std::error::Error + Send + Sync> =
            Arc::new(std::io::Error::other("original process failure"));
        let original = Arc::downgrade(&cause);
        handle.record_process_failure(
            crate::api::StatementNativeHealthFailure::actual_exit_unproven(
                "original local closure is unproven",
                cause,
            ),
        );
        let health = handle.inner.health.clone();
        assert_eq!(
            health.health().unwrap().message(),
            "original local closure is unproven"
        );
        drop(health);
        assert_eq!(
            handle.process_health().unwrap().message(),
            "original local closure is unproven"
        );
        assert!(original.upgrade().is_some());
        assert!(registry.shutdown().is_ok());
        assert!(
            original.upgrade().is_some(),
            "the same process handle still owns health"
        );
        drop(handle);
        assert!(original.upgrade().is_none());
    }
}

#[cfg(test)]
mod process_exit_health_tests {
    use super::*;

    struct CleanupHealth {
        handle: LogicalExecutionRuntimeRegistryHandle,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for CleanupHealth {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
            self.handle.record_process_failure(
                crate::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "original cleanup failed during process exit",
                    Arc::new(std::io::Error::other("original cleanup cause")),
                ),
            );
        }
    }

    #[tokio::test]
    async fn process_exit_drops_detached_health_outside_locks_and_keeps_later_cleanup_failure() {
        let mut registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
        let handle = registry.handle();
        let dropped = Arc::new(AtomicBool::new(false));
        handle.record_native_supervision_panic(Box::new(CleanupHealth {
            handle: handle.clone(),
            dropped: Arc::clone(&dropped),
        }));
        registry.abandon_for_process_exit();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(
            handle.process_health().unwrap().message(),
            "original cleanup failed during process exit",
        );
        assert_eq!(handle.entry_count(), 0);
    }
}
