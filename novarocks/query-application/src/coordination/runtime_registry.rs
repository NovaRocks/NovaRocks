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
//! Reservation precedes installation in the FE runtime. The same registration
//! retains original ingress, actual runtime exit and Native return custody.
//! Cancellation of a wait never replaces these original retirement facts.

pub(super) mod health;
mod runtime_installed;
use crate::api::logical_runtime::{
    InstalledLogicalExecution as InstalledRuntime, LogicalExit, LogicalSourceKind,
};
#[cfg(test)]
use crate::api::logical_runtime::{LogicalHostFact, LogicalJoinReadiness};
use runtime_installed::{RuntimeCarrier, RuntimeInstalledEntry};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Poll;
use std::time::Instant;

use super::original_exit::{OriginalAssociation, OriginalLogicalExit};
use novarocks_execution_contract::ParticipationRef;
use novarocks_types::identity::{QueryExecutionId, QueryId};
use tokio::sync::Notify;

use crate::coordination::assert_shutdown_complete;
use novarocks_coordination_contract::LogicalExecutionActorId;
use novarocks_logical_execution_protocol::input::RegistryClose;

#[cfg(test)]
use novarocks_logical_execution_protocol::input::RegistryParticipationConvergence;

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

#[derive(Default)]
pub(super) struct OriginalExitCursor {
    key: Option<QueryId>,
    active: bool,
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
    native_active_slot: std::alloc::Layout,
}
impl LogicalExecutionRegistryEntryGeometry {
    pub const fn record_layout(self) -> std::alloc::Layout {
        self.record
    }
    pub const fn internal_health_field_layout(self) -> std::alloc::Layout {
        self.internal_health_field
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
impl Default for LogicalExecutionRuntimeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalExecutionRuntimeRegistry {
    /// No registry, actor, consumer or allocation is created by this getter.
    pub fn entry_geometry() -> LogicalExecutionRegistryEntryGeometry {
        LogicalExecutionRegistryEntryGeometry {
            record: std::alloc::Layout::new::<RegistryEntry>(),
            internal_health_field: std::alloc::Layout::new::<Option<InternalConsumerHealth>>(),
            native_active_slot: super::NativeActiveCustodyRegistry::slot_layout(),
        }
    }
}

enum RegistryEntry {
    Reserved(ReservedEntry),
    RuntimeInstalled(RuntimeInstalledEntry),
    Joined(JoinedEntry),
}

struct ReservedEntry {
    generation: u64,
    participations: BTreeSet<ParticipationRef>,
    original_exit: Option<Arc<OriginalLogicalExit>>,
    spawn_started: bool,
}

// The original registration retains its same consumer and complete failure
// packet. This fixed field is not a second record or a new task/identity.
struct InternalConsumerHealth {
    _consumer: Arc<dyn crate::api::InternalComputationConsumer>,
    _failure: crate::api::StatementNativeHealthFailure,
}
struct JoinedEntry {
    generation: u64,
    initial_execution: QueryExecutionId,
    actor_id: LogicalExecutionActorId,
    outcome: LogicalExecutionJoinOutcome,
    original_result: LogicalExit,
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
    InvalidParticipationSet,
    ParticipationNotRegistered,
    ConfigDoesNotMatchReservation,
    JoinNotReady,
    JoinAlreadyCompleted,
    InternalConsumerHealthUnresolved,
    OriginalExitFailureUnresolved,
    RuntimeSourceClosed,
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
        write!(
            formatter,
            "logical execution runtime registry error: {self:?}"
        )
    }
}

impl std::error::Error for LogicalExecutionRuntimeRegistryError {}

impl novarocks_execution_contract::failure::FailureProjection
    for LogicalExecutionRuntimeRegistryError
{
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let (class, code, observed_stop) = match self {
            Self::AlreadyRegistered => (FailureClass::Invariant, 61, false),
            Self::RegistryClosing => (FailureClass::Control, 62, true),
            Self::RegistryNotEmpty => (FailureClass::Control, 63, false),
            Self::UnknownRegistration => (FailureClass::Control, 64, true),
            Self::ForeignRegistry => (FailureClass::Invariant, 65, false),
            Self::StaleGeneration => (FailureClass::Control, 66, true),
            Self::InvalidParticipationSet => (FailureClass::Invariant, 67, false),
            Self::ParticipationNotRegistered => (FailureClass::Invariant, 68, false),
            Self::ConfigDoesNotMatchReservation => (FailureClass::Invariant, 69, false),
            Self::JoinNotReady => (FailureClass::Control, 70, false),
            Self::JoinAlreadyCompleted => (FailureClass::Control, 71, true),
            Self::RuntimeSourceClosed => (FailureClass::Control, 92, true),
            Self::InternalConsumerHealthUnresolved => {
                return crate::execution_error::QueryFailureReason::InternalConsumerPanicked
                    .failure_fact();
            }
            Self::OriginalExitFailureUnresolved => {
                return crate::execution_error::QueryFailureReason::LogicalFailureUnspecified
                    .failure_fact();
            }
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
            Self::InvalidParticipationSet => {
                "runtime reservation has duplicate or foreign execution participations"
            }
            Self::ParticipationNotRegistered => {
                "participation convergence names an unregistered participation"
            }
            Self::ConfigDoesNotMatchReservation => {
                "actor configuration does not match its runtime reservation"
            }
            Self::JoinNotReady => "logical execution runtime join is not ready",
            Self::JoinAlreadyCompleted => "logical execution runtime join already completed",
            Self::RuntimeSourceClosed => "original logical runtime Registry source closed",
            Self::InternalConsumerHealthUnresolved => {
                "logical execution retains an unresolved internal consumer failure"
            }
            Self::OriginalExitFailureUnresolved => {
                "logical execution retains its original unhandled exit cause"
            }
        };
        novarocks_execution_contract::SafeDetail::truncating(message)
    }
}

pub(crate) struct LogicalExecutionRuntimeReservation {
    registry: LogicalExecutionRuntimeRegistryHandle,
    initial_execution: QueryExecutionId,
    generation: u64,
    active: bool,
    runtime_registration: Option<Arc<LogicalExecutionRegistration>>,
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
    RuntimeOwnerFailed,
}

impl LogicalExecutionJoinReceipt {
    #[cfg(test)]
    pub const fn outcome(&self) -> LogicalExecutionJoinOutcome {
        self.outcome
    }
}

impl LogicalExecutionRuntimeRegistry {
    pub fn new() -> Self {
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
    /// deadline after additional participation-convergence facts arrive.
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
        let drain = self.handle.drain_registrations_for_shutdown();
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

    pub(crate) fn process_health(&self) -> Option<crate::api::QueryExecutionError> {
        use novarocks_execution_contract::failure::FailureProjection as _;
        self.inner.health.health().or_else(|| {
            let state = self.lock();
            let retained = state.entries.values().any(|entry| match entry {
                RegistryEntry::RuntimeInstalled(entry) => entry.internal_health.is_some(),
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

    /// Preserve the same NoRecovery consumer in its exact original record.
    /// All receiver inputs are borrowed until the empty stable field is known.
    #[cfg(test)]
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
            Some(RegistryEntry::RuntimeInstalled(entry))
                if entry.registration.generation == registration.generation
                    && entry.registration.initial_execution == registration.initial_execution =>
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
            if !Arc::ptr_eq(&original._consumer, consumer)
                || !std::ptr::eq(original._failure.cause(), failure.cause())
            {
                return Err(LogicalExecutionRuntimeRegistryError::InternalConsumerHealthUnresolved);
            }
        } else {
            *target = Some(InternalConsumerHealth {
                _consumer: Arc::clone(consumer),
                _failure: failure.clone(),
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
        participations: &[ParticipationRef],
    ) -> Result<super::NativeActivationReservationObligation, LogicalExecutionRuntimeRegistryError>
    {
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
        let (generation, original_exit) = match state
            .entries
            .get(&registration.initial_execution.query_id())
        {
            Some(RegistryEntry::RuntimeInstalled(entry)) => {
                (entry.registration.generation, entry.original_exit.as_ref())
            }
            _ => return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
        };
        validate_generation(generation, registration.generation)?;
        if execution.query_id() != registration.initial_execution.query_id() {
            return Err(LogicalExecutionRuntimeRegistryError::InvalidParticipationSet);
        }
        let reserved = self.inner.active_native.reserve_with_original_exit(
            super::NativeActiveCustodyKey {
                initial: registration.initial_execution,
                generation: registration.generation,
                execution,
            },
            participations,
            original_exit.map_or_else(std::sync::Weak::new, Arc::downgrade),
        );
        if reserved.is_some()
            && let Some(source) = original_exit
        {
            source.native_reserved_silent();
        }
        drop(state);
        reserved.ok_or(LogicalExecutionRuntimeRegistryError::InvalidParticipationSet)
    }

    /// Cleanup-only recovery of the same successful activation output. Existing
    /// custody remains claimable during Closing; no new slot or activation is
    /// admitted and no original native-return count is incremented.
    pub(in crate::coordination) fn take_pending_native_for_cleanup(
        &self,
        registration: &LogicalExecutionRegistration,
        original_exit: &Arc<OriginalLogicalExit>,
        original_body: &Arc<tokio::sync::Mutex<super::original_exit::OriginalNativePreparation>>,
        pending: &mut Option<super::native_active_custody::PendingNativeActivationObligation>,
    ) -> Result<
        (
            super::NativeActivationReturnObligation,
            Option<crate::api::NativeRowsAttemptRuntime>,
        ),
        super::PendingNativeCleanupRefusal,
    > {
        use super::PendingNativeCleanupRefusal as Refusal;
        self.validate_registry(registration)
            .map_err(Refusal::Registry)?;
        let state = self.lock();
        let (initial, generation, source) = match state
            .entries
            .get(&registration.initial_execution.query_id())
        {
            Some(RegistryEntry::RuntimeInstalled(entry)) => (
                entry.registration.initial_execution,
                entry.registration.generation,
                entry.original_exit.as_ref(),
            ),
            Some(RegistryEntry::Joined(entry)) => (
                entry.initial_execution,
                entry.generation,
                entry.original_exit.as_ref(),
            ),
            _ => {
                return Err(Refusal::Registry(
                    LogicalExecutionRuntimeRegistryError::UnknownRegistration,
                ));
            }
        };
        validate_generation(generation, registration.generation).map_err(Refusal::Registry)?;
        if initial != registration.initial_execution
            || !source.is_some_and(|source| Arc::ptr_eq(source, original_exit))
            || !Arc::ptr_eq(&original_exit.native, original_body)
        {
            return Err(Refusal::ForeignSource);
        }
        // Lock order stays Registry -> original custody map -> original owner.
        // The caller keeps its unique original-body loan through this transfer.
        self.inner.active_native.take_staged_for_cleanup(
            initial,
            generation,
            original_exit,
            pending,
        )
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

    #[cfg(test)]
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
        if source.health_published() || !source.has_unhandled_failure() {
            return None;
        }
        let mut custody: Option<Box<dyn std::any::Any + Send>> = Some(Box::new(
            super::original_exit::LogicalExitHealthCustody::new(Arc::clone(source)),
        ));
        let notice = if let Some(error) = source.owner_failure() {
            self.inner
                .health
                .record_owner_failure_in(&mut custody, error)
        } else {
            self.inner.health.record_supervision_panic_in(&mut custody)
        };
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

    #[cfg(test)]
    pub(crate) fn active_native_custody_count(&self) -> usize {
        self.inner.active_native.len()
    }

    fn entry_count(&self) -> usize {
        self.lock().entries.len() + self.inner.active_native.len()
    }

    fn request_close_for_installed(&self) {
        let runtime = {
            let state = self.lock();
            state
                .entries
                .values()
                .filter_map(|entry| match entry {
                    RegistryEntry::RuntimeInstalled(entry) => Some(entry.carrier.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        for carrier in runtime {
            carrier.installed.request_close(
                novarocks_logical_execution_protocol::input::RegistryClose::Shutdown,
            );
        }
    }

    async fn drain_registrations_for_shutdown(
        &self,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
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
                        RegistryEntry::Reserved(_) | RegistryEntry::RuntimeInstalled(_) => None,
                    })
                    .collect::<Vec<_>>();
                let registrations = state
                    .entries
                    .values()
                    .filter_map(|entry| match entry {
                        RegistryEntry::RuntimeInstalled(entry) => {
                            Some(LogicalExecutionRegistration {
                                registry_id: self.inner.id,
                                initial_execution: entry.registration.initial_execution,
                                generation: entry.registration.generation,
                            })
                        }
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

            for mut registration in registrations {
                self.close_ingress_for_join(&registration).await?;
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
            Some(RegistryEntry::RuntimeInstalled(entry)) => {
                validate_generation(entry.registration.generation, registration.generation)?;
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

    #[cfg(test)]
    pub(crate) fn reserve(
        &self,
        initial_execution: QueryExecutionId,
        frozen_participations: Vec<ParticipationRef>,
    ) -> Result<LogicalExecutionRuntimeReservation, LogicalExecutionRuntimeRegistryError> {
        self.reserve_with_original_exit(initial_execution, frozen_participations, None)
    }

    pub(super) fn reserve_with_original_exit(
        &self,
        initial_execution: QueryExecutionId,
        frozen_participations: Vec<ParticipationRef>,
        original_exit: Option<Arc<OriginalLogicalExit>>,
    ) -> Result<LogicalExecutionRuntimeReservation, LogicalExecutionRuntimeRegistryError> {
        let participations: BTreeSet<_> = frozen_participations.iter().copied().collect();
        if participations.len() != frozen_participations.len()
            || participations
                .iter()
                .any(|participation| participation.query_execution_id() != initial_execution)
        {
            return Err(LogicalExecutionRuntimeRegistryError::InvalidParticipationSet);
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
                participations,
                original_exit,
                spawn_started: false,
            }),
        );
        Ok(LogicalExecutionRuntimeReservation {
            registry: self.clone(),
            initial_execution,
            generation,
            active: true,
            runtime_registration: None,
        })
    }

    #[cfg(test)]
    pub(crate) async fn observe_participation_never_established(
        &self,
        registration: &LogicalExecutionRegistration,
        participation: ParticipationRef,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.observe_participation_convergence(
            registration,
            participation,
            RegistryParticipationConvergence::NeverEstablished,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn join_readiness(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<LogicalExecutionJoinReadiness, LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        let Some(carrier) = self.runtime_carrier(registration)? else {
            let state = self.lock();
            return match state
                .entries
                .get(&registration.initial_execution.query_id())
            {
                Some(RegistryEntry::Joined(entry)) => {
                    validate_generation(entry.generation, registration.generation)?;
                    Err(LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted)
                }
                _ => Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
            };
        };
        carrier
            .readiness(false)
            .await
            .map(|value| match value {
                LogicalJoinReadiness::Ready
                    if self.inner.active_native.blocks_registration(
                        registration.initial_execution,
                        registration.generation,
                    ) =>
                {
                    LogicalExecutionJoinReadiness::WaitingForConvergence
                }
                LogicalJoinReadiness::Ready => LogicalExecutionJoinReadiness::Ready,
                LogicalJoinReadiness::WaitingForConvergence => {
                    LogicalExecutionJoinReadiness::WaitingForConvergence
                }
            })
            .map_err(|_| LogicalExecutionRuntimeRegistryError::RuntimeSourceClosed)
    }

    /// Close only this original runtime ingress after its actual logical
    /// owner failed. This never claims the Join or waits for the caller's own
    /// actor exit; ordinary join/retirement still require the real HostExit.
    pub(in crate::coordination) async fn close_runtime_ingress_after_owner_failure(
        &self,
        registration: &LogicalExecutionRegistration,
        original: &Arc<OriginalLogicalExit>,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        if original.owner_failure().is_none() {
            return Err(LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved);
        }
        let carrier = {
            let state = self.lock();
            let Some(RegistryEntry::RuntimeInstalled(entry)) = state
                .entries
                .get(&registration.initial_execution.query_id())
            else {
                return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration);
            };
            validate_generation(entry.registration.generation, registration.generation)?;
            if !entry
                .original_exit
                .as_ref()
                .is_some_and(|source| Arc::ptr_eq(source, original))
            {
                return Err(LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved);
            }
            Arc::clone(&entry.carrier)
        };
        carrier
            .close_ingress_after_owner_failure()
            .await
            .map_err(|_| LogicalExecutionRuntimeRegistryError::RuntimeSourceClosed)?;
        let mut state = self.lock();
        let Some(RegistryEntry::RuntimeInstalled(entry)) = state
            .entries
            .get_mut(&registration.initial_execution.query_id())
        else {
            return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration);
        };
        validate_generation(entry.registration.generation, registration.generation)?;
        if !Arc::ptr_eq(&entry.carrier, &carrier)
            || !entry
                .original_exit
                .as_ref()
                .is_some_and(|source| Arc::ptr_eq(source, original))
            || !carrier.source_idle()
        {
            return Err(LogicalExecutionRuntimeRegistryError::RuntimeSourceClosed);
        }
        entry.ingress_closed = true;
        drop(state);
        self.inner.progress.notify_waiters();
        Ok(())
    }

    pub(crate) async fn close_ingress_for_join(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        let Some(carrier) = self.runtime_carrier(registration)? else {
            let state = self.lock();
            return match state
                .entries
                .get(&registration.initial_execution.query_id())
            {
                Some(RegistryEntry::Joined(entry)) => {
                    validate_generation(entry.generation, registration.generation)?;
                    Err(LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted)
                }
                _ => Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
            };
        };
        self.claim_runtime_join(registration)?;
        {
            let state = self.lock();
            if matches!(state.entries.get(&registration.initial_execution.query_id()),
                Some(RegistryEntry::RuntimeInstalled(entry)) if entry.ingress_closed)
            {
                return Ok(());
            }
        }
        self.wait_active_native_custody(registration).await;
        carrier.installed.request_close(RegistryClose::Retire);
        carrier.wait_ingress().await;
        self.record_ingress_closed(registration)
    }

    /// Waits on the same installed runtime carrier. Cancelling this future
    /// leaves its actual exit and the exclusive retirement claim in custody.
    pub(crate) async fn join(
        &self,
        registration: &mut LogicalExecutionRegistration,
    ) -> Result<LogicalExecutionJoinReceipt, LogicalExecutionRuntimeRegistryError> {
        self.claim_runtime_join(registration)?;
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
        if let Some(carrier) = match self.runtime_carrier_for_query(query, generation) {
            Ok(carrier) => carrier,
            Err(error) => return Poll::Ready(Err(error)),
        } {
            return self.poll_runtime_join(query, generation, carrier, false, cx);
        }
        let state = self.lock();
        match state.entries.get(&query) {
            Some(RegistryEntry::Joined(entry)) => {
                validate_generation(entry.generation, generation)?;
                Poll::Ready(Err(
                    LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted,
                ))
            }
            _ => Poll::Ready(Err(
                LogicalExecutionRuntimeRegistryError::UnknownRegistration,
            )),
        }
    }

    pub(super) fn original_exit_pending(&self) -> bool {
        self.lock().entries.values().any(|entry| match entry {
            RegistryEntry::Reserved(entry) => entry
                .original_exit
                .as_ref()
                .is_some_and(|s| s.pending_actor()),
            // Runtime actor custody belongs to this Registry. Its install-only
            // role ending cannot keep the old supervisor role loop alive;
            // process shutdown drains the same runtime entries afterwards.
            RegistryEntry::RuntimeInstalled(_) => false,
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
                                RegistryEntry::RuntimeInstalled(entry) => Some((
                                    entry.registration.generation,
                                    entry.registration.initial_execution,
                                )),
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
            if let Ok(Some(carrier)) = self.runtime_carrier_for_query(query, generation) {
                carrier.poll_source(cx);
                // The installed handle keeps this observer registered for
                // protocol-ready progress as well as final Scope removal.
                let actually_exited = carrier.poll_exit(cx).is_ready();
                if carrier.explicit_join_claimed() {
                    // The explicit borrower owns the original receipt. Service
                    // progress may stabilize its actual exit but cannot steal it.
                    continue;
                }
                if !actually_exited {
                    let may_close = !self
                        .inner
                        .active_native
                        .blocks_registration(initial, generation);
                    match carrier.poll_natural_retirement(cx, may_close) {
                        Poll::Pending | Poll::Ready(Err(_)) => continue,
                        Poll::Ready(Ok(())) => {}
                    }
                }
                if let Poll::Ready(Ok(proof)) =
                    self.poll_runtime_join(query, generation, carrier, true, cx)
                {
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
                continue;
            }
        }
        Poll::Ready(())
    }

    /// Requests original ingress closure, waits for actual runtime exit and
    /// consumes the exact retirement receipt. Cancelling this borrower leaves
    /// the same carrier, source and exclusive claim in the Registry.
    #[cfg(test)]
    pub(crate) async fn join_and_retire(
        &self,
        mut registration: LogicalExecutionRegistration,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.claim_runtime_join(&registration)?;
        self.wait_active_native_custody(&registration).await;
        self.close_ingress_for_join(&registration).await?;
        let receipt = self.join(&mut registration).await?;
        self.retire(receipt).map_err(|(error, _)| error)
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
                    && entry.outcome == original_exit_outcome(&entry.original_result) =>
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
            Some(RegistryEntry::RuntimeInstalled(_) | RegistryEntry::Reserved(_)) => {
                fail(LogicalExecutionRuntimeRegistryError::JoinNotReady, receipt)
            }
            None => fail(
                LogicalExecutionRuntimeRegistryError::UnknownRegistration,
                receipt,
            ),
        }
    }

    #[cfg(test)]
    async fn observe_participation_convergence(
        &self,
        registration: &LogicalExecutionRegistration,
        participation: ParticipationRef,
        convergence: RegistryParticipationConvergence,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        if let Some(carrier) = self.runtime_carrier(registration)? {
            let activation = {
                let state = self.lock();
                let Some(RegistryEntry::RuntimeInstalled(entry)) = state
                    .entries
                    .get(&registration.initial_execution.query_id())
                else {
                    return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration);
                };
                *entry
                    .activations
                    .get(&participation.query_execution_id())
                    .ok_or(LogicalExecutionRuntimeRegistryError::ParticipationNotRegistered)?
            };
            let reply = carrier.enqueue_fact(LogicalHostFact::RegistryParticipation {
                activation,
                participation,
                convergence,
            });
            return carrier
                .wait_reply(&reply)
                .await
                .map(|_| ())
                .map_err(|_| LogicalExecutionRuntimeRegistryError::RuntimeSourceClosed);
        }

        Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration)
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

fn original_exit_outcome(result: &LogicalExit) -> LogicalExecutionJoinOutcome {
    match result {
        LogicalExit::Completed => LogicalExecutionJoinOutcome::Completed,
        LogicalExit::OwnerFailed(_) => LogicalExecutionJoinOutcome::RuntimeOwnerFailed,
    }
}

/// Passive proof that this exact installed carrier was committed to its
/// original Registry entry. Only this Registry constructs it. It owns no Work,
/// capacity, Scope or checked obligation, and promises no future liveness.
pub struct LogicalRegistryCommit {
    original: std::sync::Weak<dyn InstalledRuntime>,
}
impl LogicalRegistryCommit {
    pub fn matches(&self, original: &dyn InstalledRuntime) -> bool {
        self.original.strong_count() != 0
            && std::ptr::addr_eq(
                self.original.as_ptr(),
                original as *const dyn InstalledRuntime,
            )
    }
}
// The returned commit value retains the SAME aliases until its caller returns.
// Their presence is custody, not another registration or exit authority.
pub(crate) struct RuntimeInstalledLogicalExecution {
    pub _registration: Arc<LogicalExecutionRegistration>,
    pub _installed: Arc<dyn InstalledRuntime>,
}

/// An unsuccessful commit keeps the exact reserved registration and any
/// already installed original Scope under its installer, never a fake join.
pub(crate) struct RuntimeInstallRefusal {
    pub error: LogicalExecutionRuntimeRegistryError,
    pub reservation: LogicalExecutionRuntimeReservation,
    pub installed: Arc<dyn InstalledRuntime>,
}
impl fmt::Debug for RuntimeInstallRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeInstallRefusal")
            .field("error", &self.error)
            .field("reservation", &self.reservation)
            .finish_non_exhaustive()
    }
}

impl LogicalExecutionRuntimeReservation {
    /// Mint the original registration allocation once, before building Native
    /// executors which must retain this SAME allocation across installation.
    pub(crate) fn runtime_registration(&mut self) -> Arc<LogicalExecutionRegistration> {
        self.runtime_registration
            .get_or_insert_with(|| {
                Arc::new(LogicalExecutionRegistration {
                    registry_id: self.registry.inner.id,
                    initial_execution: self.initial_execution,
                    generation: self.generation,
                })
            })
            .clone()
    }

    pub(crate) fn actor_instance(&self) -> LogicalExecutionActorId {
        LogicalExecutionActorId::from_raw(self.generation)
    }

    /// Commit a previously installed FE carrier to its reserved original slot.
    /// The installer retains its executors on any refusal and owns cleanup.
    pub(crate) fn install_runtime(
        mut self,
        start: &novarocks_logical_execution_protocol::start::LogicalStart,
        installed: Arc<dyn InstalledRuntime>,
    ) -> Result<RuntimeInstalledLogicalExecution, RuntimeInstallRefusal> {
        let invalid = {
            let state = self.registry.lock();
            match state.entries.get(&self.initial_execution.query_id()) {
                Some(RegistryEntry::Reserved(entry)) if entry.generation == self.generation => {
                    start.validate().is_err()
                        || start.first_attempt.activation.execution() != self.initial_execution
                        || start.actor_instance != self.actor_instance()
                        || start.first_attempt.participations.len() != entry.participations.len()
                        || start
                            .first_attempt
                            .participations
                            .iter()
                            .any(|p| !entry.participations.contains(p))
                }
                _ => true,
            }
        };
        if invalid {
            return Err(RuntimeInstallRefusal {
                error: LogicalExecutionRuntimeRegistryError::ConfigDoesNotMatchReservation,
                reservation: self,
                installed,
            });
        }
        let source = match installed.source(LogicalSourceKind::Registry) {
            Ok(source) => source,
            Err(_) => {
                return Err(RuntimeInstallRefusal {
                    error: LogicalExecutionRuntimeRegistryError::RuntimeSourceClosed,
                    reservation: self,
                    installed,
                });
            }
        };
        let registration = self.runtime_registration();
        let carrier = RuntimeCarrier::new(
            installed.clone(),
            source,
            Arc::downgrade(&self.registry.inner),
        );
        let query = self.initial_execution.query_id();
        let closing = {
            let mut state = self.registry.lock();
            let reserved = match state.entries.remove(&query) {
                Some(RegistryEntry::Reserved(entry)) if entry.generation == self.generation => {
                    entry
                }
                _ => unreachable!("the unique original reservation retains its exact slot"),
            };
            if let Some(original) = &reserved.original_exit {
                original.runtime_installed_silent(OriginalAssociation {
                    registry: self.registry.inner.id,
                    initial: self.initial_execution,
                    generation: self.generation,
                });
            }
            let closing = !state.accepting;
            state.entries.insert(
                query,
                RegistryEntry::RuntimeInstalled(RuntimeInstalledEntry {
                    registration: registration.clone(),
                    actor_id: start.actor_instance,
                    carrier: carrier.clone(),
                    original_exit: reserved.original_exit,
                    ingress_closed: false,
                    internal_health: None,
                    activations: BTreeMap::from([(
                        start.first_attempt.activation.execution(),
                        start.first_attempt.activation,
                    )]),
                }),
            );
            closing
        };
        self.active = false;
        self.registry.inner.progress.notify_waiters();
        if closing {
            installed.request_close(
                novarocks_logical_execution_protocol::input::RegistryClose::Shutdown,
            );
        }
        // All original association/map metadata is committed before this
        // foreign notification, and no Registry lock is held across it.
        installed.registry_committed(LogicalRegistryCommit {
            original: Arc::downgrade(&installed),
        });
        Ok(RuntimeInstalledLogicalExecution {
            _registration: registration,
            _installed: installed,
        })
    }
}

impl LogicalExecutionRuntimeRegistryHandle {
    fn claim_runtime_join(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        let state = self.lock();
        if let Some(RegistryEntry::RuntimeInstalled(entry)) = state
            .entries
            .get(&registration.initial_execution.query_id())
        {
            validate_generation(entry.registration.generation, registration.generation)?;
            entry.carrier.claim_explicit_join();
        }
        Ok(())
    }

    fn runtime_carrier(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<Option<Arc<RuntimeCarrier>>, LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        self.runtime_carrier_for_query(
            registration.initial_execution.query_id(),
            registration.generation,
        )
    }
    fn runtime_carrier_for_query(
        &self,
        query: QueryId,
        generation: u64,
    ) -> Result<Option<Arc<RuntimeCarrier>>, LogicalExecutionRuntimeRegistryError> {
        let state = self.lock();
        match state.entries.get(&query) {
            Some(RegistryEntry::RuntimeInstalled(entry)) => {
                validate_generation(entry.registration.generation, generation)?;
                Ok(Some(entry.carrier.clone()))
            }
            Some(RegistryEntry::Reserved(entry)) => {
                validate_generation(entry.generation, generation)?;
                Ok(None)
            }
            Some(RegistryEntry::Joined(entry)) => {
                validate_generation(entry.generation, generation)?;
                Ok(None)
            }
            None => Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration),
        }
    }

    /// Installation/activation author records the exact activation once. This
    /// does not freeze a successor to the initial backend participation count.
    pub(crate) fn register_runtime_activation(
        &self,
        registration: &LogicalExecutionRegistration,
        activation: novarocks_coordination_contract::AttemptActivationIdentity,
    ) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
        let mut state = self.lock();
        let Some(RegistryEntry::RuntimeInstalled(entry)) = state
            .entries
            .get_mut(&registration.initial_execution.query_id())
        else {
            return Err(LogicalExecutionRuntimeRegistryError::UnknownRegistration);
        };
        validate_generation(entry.registration.generation, registration.generation)?;
        if activation.actor() != entry.actor_id
            || activation.execution().query_id() != registration.initial_execution.query_id()
        {
            return Err(LogicalExecutionRuntimeRegistryError::ParticipationNotRegistered);
        }
        match entry.activations.entry(activation.execution()) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(activation);
                Ok(())
            }
            std::collections::btree_map::Entry::Occupied(slot) if *slot.get() == activation => {
                Ok(())
            }
            _ => Err(LogicalExecutionRuntimeRegistryError::StaleGeneration),
        }
    }

    fn poll_runtime_join(
        &self,
        query: QueryId,
        generation: u64,
        carrier: Arc<RuntimeCarrier>,
        natural: bool,
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
        carrier.poll_source(cx);
        let result = std::task::ready!(carrier.poll_exit(cx));
        let initial = {
            let state = self.lock();
            let Some(RegistryEntry::RuntimeInstalled(entry)) = state.entries.get(&query) else {
                return Poll::Ready(Err(
                    LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted,
                ));
            };
            validate_generation(entry.registration.generation, generation)?;
            entry.registration.initial_execution
        };
        if self
            .inner
            .active_native
            .blocks_registration(initial, generation)
            || !carrier.source_idle()
        {
            return Poll::Pending;
        }
        let original_result = result.clone();
        let outcome = original_exit_outcome(&original_result);
        let (proof, removed, source) = {
            let mut state = self.lock();
            let Some(RegistryEntry::RuntimeInstalled(entry)) = state.entries.get_mut(&query) else {
                return Poll::Ready(Err(
                    LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted,
                ));
            };
            validate_generation(entry.registration.generation, generation)?;
            if natural && carrier.explicit_join_claimed() {
                return Poll::Pending;
            }
            let source = entry.original_exit.clone();
            let proof = (initial, generation, entry.actor_id, outcome);
            if let Some(source) = &source {
                source.runtime_joined_silent(
                    OriginalAssociation {
                        registry: self.inner.id,
                        initial,
                        generation,
                    },
                    result,
                );
            }
            let joined = JoinedEntry {
                generation,
                initial_execution: initial,
                actor_id: entry.actor_id,
                outcome,
                original_result,
                original_exit: source.clone(),
                internal_health: entry.internal_health.take(),
            };
            let removed = state.entries.insert(query, RegistryEntry::Joined(joined));
            (proof, removed, source)
        };
        drop(removed);
        if let Some(source) = source {
            // The install-only role's earlier completion deferred this body.
            // Actual runtime Actor/Scope exit now permits its original tail.
            source.retire_native_body_silent();
            if source.has_failure()
                && let Some(notice) = self.publish_original_exit_health_silent(&source)
            {
                notice.notify();
            }
            source.notify();
        }
        self.inner.progress.notify_waiters();
        Poll::Ready(Ok(proof))
    }
}

#[cfg(test)]
mod tests {
    //! Registry/Native-return component contracts only. The shared runtime
    //! helper explicitly supplies a passive exit; no test here proves AP
    //! success or a real FA Scope exit. Those obligations remain in the
    //! accompanying migration manifest and actual installed-source tests.
    use super::runtime_tests::{
        install_native_component_runtime, install_native_component_runtime_for_original,
    };
    use super::*;
    use crate::coordination::native_active_custody::{
        NativeActivationReturnObligation, PendingNativeCleanupRefusal,
    };
    use novarocks_coordination_contract::AttemptActivationIdentity;
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_types::identity::{AttemptId, BackendProcessId, FrontendProcessId};
    use novarocks_types::{StageId, TaskId};
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Wake, Waker};
    use std::time::Duration;

    fn execution(tag: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(91, tag as i64), AttemptId::new(1).unwrap()).unwrap()
    }
    fn participation(execution: QueryExecutionId) -> ParticipationRef {
        ParticipationRef::new(
            execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }
    fn root(participation: ParticipationRef) -> TaskIdentity {
        TaskIdentity::new(
            participation.query_execution_id(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            participation.backend_process_id(),
        )
    }
    fn borrowed(original: &LogicalExecutionRegistration) -> LogicalExecutionRegistration {
        LogicalExecutionRegistration {
            registry_id: original.registry_id,
            initial_execution: original.initial_execution,
            generation: original.generation,
        }
    }
    fn original_return(
        registry: &LogicalExecutionRuntimeRegistryHandle,
        registration: &LogicalExecutionRegistration,
        activation: AttemptActivationIdentity,
        participation: ParticipationRef,
    ) -> NativeActivationReturnObligation {
        let mut reservation = registry
            .reserve_active_native_owner(registration, activation.execution(), &[participation])
            .unwrap();
        let mut pending = None;
        reservation.receive_activated_in(
            &mut pending,
            crate::api::ActivatedNativeAttempt::completion(activation),
        );
        reservation
            .stage_activated_in(activation, root(participation), &mut pending)
            .unwrap();
        let (returned, rows) = reservation.finish_activated_in(&mut pending).unwrap();
        assert!(pending.is_none() && rows.is_none());
        returned
    }
    #[derive(Default)]
    struct WakeFlag(AtomicBool);
    impl Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test]
    async fn pending_cleanup_reclaims_same_staged_return_during_close_without_new_reservation() {
        let mut registry = LogicalExecutionRuntimeRegistry::new();
        let exact = execution(982);
        let member = participation(exact);
        let source = OriginalLogicalExit::new();
        let observer =
            super::super::original_exit::LogicalExecutionExitObserver::new(source.clone());
        let handle = registry.handle();
        let (registration, activation, release) = install_native_component_runtime_for_original(
            &handle,
            exact,
            vec![member],
            Some(source.clone()),
        );
        let reservation = handle
            .reserve_active_native_owner(&registration, exact, &[member])
            .unwrap();
        let mut body = source.native.lock().await;
        reservation.receive_activated_in(
            &mut body.pending_activated,
            crate::api::ActivatedNativeAttempt::completion(activation),
        );
        reservation
            .stage_activated_in(activation, root(member), &mut body.pending_activated)
            .unwrap();
        let address = body
            .pending_activated
            .as_ref()
            .unwrap()
            .staged_slot_address()
            .unwrap();
        drop(reservation);
        assert_eq!(handle.active_native_custody_count(), 1);
        assert_eq!(observer.view().unwrap().pending_native_returns(), 1);
        let foreign = OriginalLogicalExit::new();
        for wrong_body in [false, true] {
            let (original, native) = if wrong_body {
                (&source, &foreign.native)
            } else {
                (&foreign, &source.native)
            };
            assert!(matches!(
                handle.take_pending_native_for_cleanup(
                    &registration,
                    original,
                    native,
                    &mut body.pending_activated,
                ),
                Err(PendingNativeCleanupRefusal::ForeignSource)
            ));
            assert_eq!(
                body.pending_activated
                    .as_ref()
                    .unwrap()
                    .staged_slot_address(),
                Some(address)
            );
        }
        handle.begin_shutdown();
        assert!(matches!(
            handle.reserve_active_native_owner(&registration, exact, &[member]),
            Err(LogicalExecutionRuntimeRegistryError::RegistryClosing)
        ));
        let (mut returned, rows) = handle
            .take_pending_native_for_cleanup(
                &registration,
                &source,
                &source.native,
                &mut body.pending_activated,
            )
            .unwrap();
        assert!(rows.is_none() && body.pending_activated.is_none());
        assert_eq!(handle.active_native_custody_count(), 1);
        assert_eq!(
            observer.view().unwrap().pending_native_returns(),
            1,
            "adoption reserves no second slot"
        );
        assert!(matches!(
            handle.take_pending_native_for_cleanup(
                &registration,
                &source,
                &source.native,
                &mut body.pending_activated,
            ),
            Err(PendingNativeCleanupRefusal::NotStaged)
        ));
        drop(body);
        // This task-free completion fixture models the exact original return
        // author after its independently supplied local-exit qualification.
        // It does not synthesize a Native owner, future, AP or Scope proof.
        let changed = returned.retire_local();
        changed.notify_waiters();
        assert_eq!(handle.active_native_custody_count(), 0);
        assert_eq!(observer.view().unwrap().pending_native_returns(), 0);
        drop(returned);
        release();
        handle
            .join_and_retire(borrowed(&registration))
            .await
            .unwrap();
        assert_eq!(handle.entry_count(), 0);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn same_shutdown_wait_resumes_when_original_native_return_notifies_after_metadata() {
        let mut registry = LogicalExecutionRuntimeRegistry::new();
        let exact = execution(983);
        let member = participation(exact);
        let source = OriginalLogicalExit::new();
        let observer =
            super::super::original_exit::LogicalExecutionExitObserver::new(source.clone());
        let handle = registry.handle();
        let (registration, activation, release) = install_native_component_runtime_for_original(
            &handle,
            exact,
            vec![member],
            Some(source.clone()),
        );
        let mut returned = original_return(&handle, &registration, activation, member);
        release(); // Explicit scripted runtime exit, not Native closure.
        let flag = Arc::new(WakeFlag::default());
        let wake = Waker::from(flag.clone());
        let mut cx = Context::from_waker(&wake);
        let mut wait = Box::pin(registry.shutdown_until(Instant::now() + Duration::from_secs(60)));
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        assert_eq!(handle.active_native_custody_count(), 1);
        assert!(handle.lock().entries.contains_key(&exact.query_id()));
        flag.0.store(false, Ordering::Release);
        let progress = returned.retire_local();
        assert_eq!(observer.view().unwrap().pending_native_returns(), 0);
        assert_eq!(handle.active_native_custody_count(), 0);
        progress.notify_waiters();
        assert!(
            flag.0.load(Ordering::Acquire),
            "same pending shutdown receives original progress"
        );
        wait.await.unwrap();
        drop(returned);
        assert_eq!(handle.entry_count(), 0);
    }

    #[tokio::test]
    async fn same_explicit_retirement_wait_keeps_registration_until_original_native_return() {
        let mut registry = LogicalExecutionRuntimeRegistry::new();
        let exact = execution(984);
        let member = participation(exact);
        let source = OriginalLogicalExit::new();
        let observer =
            super::super::original_exit::LogicalExecutionExitObserver::new(source.clone());
        let handle = registry.handle();
        let (registration, activation, release) = install_native_component_runtime_for_original(
            &handle,
            exact,
            vec![member],
            Some(source.clone()),
        );
        let mut returned = original_return(&handle, &registration, activation, member);
        release();
        let mut wait = Box::pin(handle.join_and_retire(borrowed(&registration)));
        assert!(futures::poll!(wait.as_mut()).is_pending());
        assert_eq!(handle.active_native_custody_count(), 1);
        assert!(handle.lock().entries.contains_key(&exact.query_id()));
        let progress = returned.retire_local();
        progress.notify_waiters();
        wait.await.unwrap();
        drop(returned);
        assert_eq!(observer.view().unwrap().pending_native_returns(), 0);
        assert_eq!(handle.entry_count(), 0);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn unknown_native_return_keeps_exact_registration_while_healthy_runtime_retires() {
        let mut registry = LogicalExecutionRuntimeRegistry::new();
        let handle = registry.handle();
        let exact = execution(985);
        let member = participation(exact);
        let (unknown, activation, release_unknown) =
            install_native_component_runtime(&handle, exact, vec![member]);
        let returned = original_return(&handle, &unknown, activation, member);
        drop(returned); // Explicitly unresolved return, never a positive close.
        release_unknown();
        let (healthy, _, release_healthy) =
            install_native_component_runtime(&handle, execution(986), vec![]);
        release_healthy();
        // Both exits above are explicit scripted InstalledRuntime inputs.
        // Only this SAME real Registry sweep may retire healthy B: an explicit
        // join_and_retire(B) would bypass its skip-unknown scheduling contract.
        let mut shutdown =
            Box::pin(registry.shutdown_until(Instant::now() + Duration::from_secs(5)));
        let healthy_retired_by_same_sweep = std::future::poll_fn(|cx| {
            if shutdown.as_mut().poll(cx).is_ready() {
                return Poll::Ready(false);
            }
            if !handle
                .lock()
                .entries
                .contains_key(&execution(986).query_id())
            {
                Poll::Ready(true)
            } else {
                Poll::Pending
            }
        })
        .await;
        let same_wait_still_pending =
            healthy_retired_by_same_sweep && futures::poll!(shutdown.as_mut()).is_pending();
        // End only the borrowed wait. Keep the exact Registry and original
        // unproven return; neither its timer nor B's exit proves A's exit.
        drop(shutdown);
        let exact_a_kept = matches!(
            handle.lock().entries.get(&exact.query_id()),
            Some(RegistryEntry::RuntimeInstalled(entry))
                if Arc::ptr_eq(&entry.registration, &unknown)
        );
        let original_return_still_blocks = handle
            .inner
            .active_native
            .blocks_registration(exact, unknown.generation);
        let one_return = handle.active_native_custody_count() == 1;
        let a_health_kept = handle.active_native_registration_health(&unknown).is_some();
        let b_health_absent = handle.active_native_registration_health(&healthy).is_none();
        let original_entries_and_return = handle.entry_count() == 2;
        // This component has no physical Native/AP resource. Only its explicit
        // process-exit disposition releases the unresolved passive return slot.
        registry.abandon_for_process_exit();
        assert!(healthy_retired_by_same_sweep && same_wait_still_pending);
        assert!(exact_a_kept && original_return_still_blocks && one_return);
        assert!(a_health_kept && b_health_absent && original_entries_and_return);
        assert_eq!(handle.entry_count(), 0);
    }

    #[tokio::test]
    async fn shutdown_cut_refuses_late_native_return_reservation_without_activation() {
        let mut registry = LogicalExecutionRuntimeRegistry::new();
        let handle = registry.handle();
        let exact = execution(987);
        let member = participation(exact);
        let source = OriginalLogicalExit::new();
        let observer =
            super::super::original_exit::LogicalExecutionExitObserver::new(source.clone());
        let (registration, _, release) = install_native_component_runtime_for_original(
            &handle,
            exact,
            vec![member],
            Some(source.clone()),
        );
        handle.begin_shutdown();
        assert!(matches!(
            handle.reserve_active_native_owner(&registration, exact, &[member]),
            Err(LogicalExecutionRuntimeRegistryError::RegistryClosing)
        ));
        assert_eq!(handle.active_native_custody_count(), 0);
        assert_eq!(observer.view().unwrap().pending_native_returns(), 0);
        release();
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(handle.entry_count(), 0);
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
        // The registry seam reports an explicit scripted runtime exit. The
        // blocking worker and its opaque panic payload below are genuine.
        let mut registry = LogicalExecutionRuntimeRegistry::new();
        let exact = execution(95);
        let (registration, _, release) =
            install_native_component_runtime(&registry.handle(), exact, Vec::new());
        let mut registration = borrowed(&registration);
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
        release();
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
        assert!(geometry.record_layout().size() >= std::mem::size_of::<RuntimeInstalledEntry>());
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
mod original_health_migration_tests {
    use super::*;

    #[tokio::test]
    async fn process_health_survives_runtime_shutdown_until_its_last_original_handle_exits() {
        let registry = LogicalExecutionRuntimeRegistry::new();
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
        let mut registry = LogicalExecutionRuntimeRegistry::new();
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

#[cfg(test)]
#[path = "runtime_registry/runtime_tests.rs"]
pub(in crate::coordination) mod runtime_tests;
