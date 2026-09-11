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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::Poll;
use std::time::Instant;

use novarocks_execution_contract::QueryContextRef;
use novarocks_types::identity::{QueryExecutionId, QueryId};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::coordination::{
    AttemptInstantiationPermit, LogicalExecutionActor, LogicalExecutionActorConfig,
    LogicalExecutionActorError, LogicalExecutionActorId, LogicalExecutionOutputTransfer,
    RegistryContextConvergence, spawn_logical_execution_actor,
};

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

pub struct LogicalExecutionRuntimeRegistry {
    handle: LogicalExecutionRuntimeRegistryHandle,
    shutdown_complete: bool,
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
        assert!(
            self.shutdown_complete,
            "logical execution runtime registry owner dropped without explicit shutdown"
        );
    }
}

struct RegistryInner {
    id: u64,
    runtime: Handle,
    state: Mutex<RegistryState>,
    progress: Notify,
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

enum RegistryEntry {
    Reserved(ReservedEntry),
    Installed(InstalledEntry),
    Joined(JoinedEntry),
}

struct ReservedEntry {
    generation: u64,
    initial_execution: QueryExecutionId,
    contexts: BTreeSet<QueryContextRef>,
}

struct InstalledEntry {
    generation: u64,
    initial_execution: QueryExecutionId,
    actor: LogicalExecutionActor,
    join: JoinHandle<()>,
    ingress_closed: bool,
}

#[derive(Clone, Copy)]
struct JoinedEntry {
    generation: u64,
    initial_execution: QueryExecutionId,
    actor_id: LogicalExecutionActorId,
    outcome: LogicalExecutionJoinOutcome,
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
        let removed = if matches!(
            state.entries.get(&query),
            Some(RegistryEntry::Reserved(entry)) if entry.generation == self.generation
        ) {
            state.entries.remove(&query);
            true
        } else {
            false
        };
        drop(state);
        if removed {
            self.registry.inner.progress.notify_one();
        }
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

    pub const fn generation(&self) -> u64 {
        self.generation
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
        Self {
            handle: LogicalExecutionRuntimeRegistryHandle {
                inner: Arc::new(RegistryInner {
                    id,
                    runtime,
                    state: Mutex::new(RegistryState::default()),
                    progress: Notify::new(),
                }),
            },
            shutdown_complete: false,
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
        let drain = self.handle.drain_for_shutdown();
        match tokio::time::timeout_at(deadline.into(), drain).await {
            Ok(Ok(())) => {
                debug_assert!(self.handle.lock().entries.is_empty());
                self.shutdown_complete = true;
                Ok(())
            }
            Ok(Err(error)) => Err(error.into()),
            Err(_) => Err(LogicalExecutionRuntimeShutdownError::DeadlineExceeded {
                remaining_entries: self.handle.entry_count(),
            }),
        }
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
        if !self.handle.lock().entries.is_empty() {
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

    fn entry_count(&self) -> usize {
        self.lock().entries.len()
    }

    fn request_close_for_installed(&self) {
        let actors = {
            let state = self.lock();
            state
                .entries
                .values()
                .filter_map(|entry| match entry {
                    RegistryEntry::Installed(entry) => Some(entry.actor.clone()),
                    RegistryEntry::Reserved(_) | RegistryEntry::Joined(_) => None,
                })
                .collect::<Vec<_>>()
        };
        for actor in actors {
            drop(actor.request_registry_close_for_join());
        }
    }

    async fn drain_for_shutdown(&self) -> Result<(), LogicalExecutionRuntimeRegistryError> {
        loop {
            let progress = self.inner.progress.notified();
            tokio::pin!(progress);
            progress.as_mut().enable();
            let registrations = {
                let state = self.lock();
                if state.entries.is_empty() {
                    return Ok(());
                }
                state
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
                    .collect::<Vec<_>>()
            };

            if registrations.is_empty() {
                progress.await;
                continue;
            }

            let mut close_waiters = Vec::with_capacity(registrations.len());
            for registration in registrations {
                let actor = self.actor(&registration)?;
                let closed = actor.request_registry_close_for_join();
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
        state.entries.insert(
            query,
            RegistryEntry::Reserved(ReservedEntry {
                generation,
                initial_execution,
                contexts,
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

    pub(crate) async fn join_readiness(
        &self,
        registration: &LogicalExecutionRegistration,
    ) -> Result<LogicalExecutionJoinReadiness, LogicalExecutionRuntimeRegistryError> {
        self.validate_registry(registration)?;
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
        self.close_ingress_for_join(registration).await?;
        let query = registration.initial_execution.query_id();
        let generation = registration.generation;
        let joined = std::future::poll_fn(|context| {
            let mut state = self.lock();
            let result = match state.entries.get_mut(&query) {
                Some(RegistryEntry::Installed(entry)) => {
                    if entry.generation != generation {
                        return Poll::Ready(Err(
                            LogicalExecutionRuntimeRegistryError::StaleGeneration,
                        ));
                    }
                    match Pin::new(&mut entry.join).poll(context) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => {
                            let joined = JoinedEntry {
                                generation: entry.generation,
                                initial_execution: entry.initial_execution,
                                actor_id: entry.actor.id(),
                                outcome: LogicalExecutionJoinOutcome::Completed,
                            };
                            (result, joined)
                        }
                    }
                }
                Some(RegistryEntry::Joined(entry)) => {
                    validate_generation(entry.generation, generation)?;
                    return Poll::Ready(Err(
                        LogicalExecutionRuntimeRegistryError::JoinAlreadyCompleted,
                    ));
                }
                Some(RegistryEntry::Reserved(entry)) => {
                    validate_generation(entry.generation, generation)?;
                    return Poll::Ready(Err(
                        LogicalExecutionRuntimeRegistryError::UnknownRegistration,
                    ));
                }
                None => {
                    return Poll::Ready(Err(
                        LogicalExecutionRuntimeRegistryError::UnknownRegistration,
                    ));
                }
            };
            let (result, mut joined) = result;
            joined.outcome = match result {
                Ok(()) => LogicalExecutionJoinOutcome::Completed,
                Err(error) if error.is_cancelled() => LogicalExecutionJoinOutcome::Cancelled,
                Err(_) => LogicalExecutionJoinOutcome::Panicked,
            };
            state.entries.insert(query, RegistryEntry::Joined(joined));
            Poll::Ready(Ok(joined))
        })
        .await?;
        Ok(LogicalExecutionJoinReceipt {
            registry_id: self.inner.id,
            initial_execution: joined.initial_execution,
            generation: joined.generation,
            actor_id: joined.actor_id,
            outcome: joined.outcome,
        })
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
                    && entry.outcome == receipt.outcome =>
            {
                state.entries.remove(&query);
                drop(state);
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
        let spawned = spawn_logical_execution_actor(&self.registry.inner.runtime, config)?;
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
            state.entries.insert(
                query,
                RegistryEntry::Installed(InstalledEntry {
                    generation: reserved.generation,
                    initial_execution: reserved.initial_execution,
                    actor,
                    join,
                    ingress_closed: false,
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
        ResourceConfig, Stage, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;
    use crate::coordination::{ExecutionEffect, LogicalConclusion};

    fn execution(tag: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(91, tag as i64), AttemptId::new(1).unwrap()).unwrap()
    }

    fn config(
        execution: QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> LogicalExecutionActorConfig {
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
        drop(root.business);
        LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(4).unwrap(),
            contexts,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            root.owner,
            stage,
        )
        .unwrap()
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
        drop(registration);

        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(registry.entry_count(), 0);
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
        drop(registration);
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
        drop(registration);
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
        drop(first_registration);
        drop(second_registration);
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
        {
            let mut state = registry.lock();
            let Some(RegistryEntry::Installed(entry)) =
                state.entries.get_mut(&execution.query_id())
            else {
                panic!("installed runtime must remain registered");
            };
            entry.ingress_closed = true;
            entry.join.abort();
        }

        let receipt = registry.join(&mut registration).await.unwrap();
        assert_eq!(receipt.outcome(), LogicalExecutionJoinOutcome::Cancelled);
        registry.retire(receipt).unwrap();
        registry.shutdown().unwrap();
    }
}
